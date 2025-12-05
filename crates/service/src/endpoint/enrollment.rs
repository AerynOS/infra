//! Enroll with remote services to provision authorization

use std::time::Duration;

use http::Uri;
use serde::{Deserialize, Serialize};
use service_client::EndpointServiceClient;
use service_core::Token;
use service_grpc::endpoint::{EnrollmentRequest, Issuer as ProtoIssuer, Role as ProtoRole};
use thiserror::Error;
use tokio::time;
use tracing::{debug, error, info, info_span};

use crate::{
    Account, Database, Endpoint, State, account, client,
    crypto::{EncodedPublicKey, KeyPair, PublicKey},
    database,
    endpoint::{self, Role},
    error,
    token::{self, VerifiedToken},
};

/// An issuer of enrollment requests
#[derive(Debug, Clone)]
pub struct Issuer {
    /// [`KeyPair`] for creating / validating tokens
    pub key_pair: KeyPair,
    /// [`Uri`] the issuer can be reached at
    pub host_address: Uri,
    /// Endpoint role
    pub role: Role,
    /// Endpoint description
    pub description: String,
    /// Admin name
    pub admin_name: String,
    /// Admin email
    pub admin_email: String,
}

impl From<Issuer> for ProtoIssuer {
    fn from(issuer: Issuer) -> Self {
        let Issuer {
            key_pair,
            host_address,
            role,
            admin_name,
            admin_email,
            description,
        } = issuer;

        ProtoIssuer {
            public_key: key_pair.public_key().encode().to_string(),
            url: host_address.to_string(),
            role: ProtoRole::from(role) as i32,
            admin_name,
            admin_email,
            description,
        }
    }
}

/// The remote details of an enrollment request
#[derive(Debug, Clone)]
pub struct Remote {
    /// [`PublicKey`] of the remote endpoint
    pub public_key: PublicKey,
    /// [`Uri`] the remote endpoint can be reached at
    pub host_address: Uri,
    /// Remote endpoint role
    pub role: Role,
    /// Bearer token assigned to us by the remote endpoint
    pub bearer_token: VerifiedToken,
    /// Remote description
    pub description: String,
}

/// A received enrollment request
#[derive(Debug, Clone)]
pub struct Received {
    /// UUID to assign the endpoint of this request
    pub endpoint: endpoint::Id,
    /// UUID to assign the service account of this request
    pub account: account::Id,
    /// Remote details of the enrollment request
    pub remote: Remote,
}

/// The target of a [`Sent`] enrollment
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Target {
    /// [`Uri`] the target endpoint can be reached at
    #[serde(with = "http_serde::uri")]
    pub host_address: Uri,
    /// [`PublicKey`] of the target endpoint
    pub public_key: PublicKey,
    /// Target endpoint role
    pub role: Role,
}

/// Send auto-enrollment to the list of targets if the endpoint isn't already configured
pub(crate) async fn auto_enroll(target: &Target, ourself: Issuer, state: &State) -> Result<(), Error> {
    let mut conn = state.service_db.acquire().await?;

    let endpoints = Endpoint::list(conn.as_mut()).await.map_err(Error::ListEndpoints)?;

    let mut enrolled = false;

    let span = info_span!(
        "auto_enrollment",
        url = %target.host_address,
        public_key = %target.public_key,
        role = %target.role,
    );
    let _guard = span.enter();

    if let Some(endpoint) = endpoints.iter().find(|e| e.host_address == target.host_address) {
        let account = Account::get(conn.as_mut(), endpoint.account)
            .await
            .map_err(Error::ReadAccount)?;

        if account.public_key == target.public_key.encode() {
            enrolled = true;

            debug!("Endpoint already enrolled");
        }
    }

    if !enrolled {
        debug!("Enrolling with target service");

        loop {
            if let Err(e) = enroll(state, target.clone(), ourself.clone()).await {
                error!(error = %error::chain(e), "Enrollment request failed, retrying in 30s...");

                time::sleep(Duration::from_secs(30)).await;

                continue;
            }

            break;
        }
    }

    Ok(())
}

#[tracing::instrument(
    name = "send_enrollment",
    skip_all,
    fields(
        public_key = %target.public_key,
        url = %target.host_address,
        role = %target.role,
    )
)]
/// Enroll with the [`Target`]
async fn enroll(state: &State, target: Target, ourself: Issuer) -> Result<(), Error> {
    let endpoint = endpoint::Id::generate();
    let account = account::Id::generate();

    debug!(%endpoint, %account, "Generated endpoint & account IDs for enrollment request");

    let bearer_token = endpoint::create_token(token::Purpose::Authorization, endpoint, account, target.role, &ourself)
        .map_err(Error::SignToken)?;

    let mut client = EndpointServiceClient::connect(target.host_address.clone()).await?;

    let resp = client
        .enroll(EnrollmentRequest {
            issuer: Some(ourself.into()),
            issue_token: bearer_token.encoded.clone(),
            role: ProtoRole::from(target.role) as i32,
        })
        .await?
        .into_inner();

    info!(
        %endpoint,
        %account,
        public_key = %target.public_key,
        url = %target.host_address,
        role = %target.role,
        "Enrollment request sent"
    );

    let issuer = resp.issuer.as_ref().ok_or(Error::MalformedRequest)?;
    let issuer_role = Role::from_proto(issuer.role()).ok_or(Error::UnknownRole(issuer.role))?;

    let public_key = EncodedPublicKey::decode(&issuer.public_key).map_err(|_| Error::InvalidPublicKey)?;
    let verified_token =
        Token::verify(&resp.issue_token, &public_key, &token::Validation::new()).map_err(Error::VerifyToken)?;

    if !matches!(verified_token.decoded.payload.purpose, token::Purpose::Authorization) {
        return Err(Error::RequireBearerToken);
    }
    if issuer_role != target.role {
        return Err(Error::RoleMismatch {
            expected: target.role,
            provided: issuer_role,
        });
    }

    if public_key != target.public_key {
        return Err(Error::PublicKeyMismatch {
            expected: target.public_key.encode(),
            actual: public_key.encode(),
        });
    }

    let username = format!("@{account}");

    let mut tx = state.service_db.begin().await?;

    Account {
        id: account,
        kind: account::Kind::Service,
        username: username.clone(),
        email: None,
        name: None,
        public_key: target.public_key.encode(),
    }
    .save(&mut tx)
    .await
    .map_err(Error::CreateServiceAccount)?;

    info!(username, "Created a new service account");

    Endpoint {
        id: endpoint,
        host_address: target.host_address.clone(),
        status: endpoint::Status::Operational,
        error: None,
        account,
        remote_account: verified_token.decoded.payload.account_id.into(),
        role: issuer_role,
        description: Some(issuer.description.clone()),
    }
    .save(&mut tx)
    .await
    .map_err(Error::CreateEndpoint)?;

    endpoint::Tokens {
        bearer_token: Some(verified_token.encoded),
        access_token: None,
    }
    .save(&mut tx, endpoint)
    .await
    .map_err(Error::SetEndpointAccountToken)?;

    info!("Created a new endpoint for the service account");

    account::Token::set(&mut tx, account, &bearer_token.encoded, bearer_token.expires())
        .await
        .map_err(Error::SetAccountToken)?;

    info!(
        expiration = %bearer_token.expires(),
        "Bearer token saved",
    );

    tx.commit().await?;

    info!("Endpoint is enrolled and operational");

    Ok(())
}

impl Received {
    /// Accept the received enrollment
    #[tracing::instrument(
        name = "accept_enrollment",
        skip_all,
        fields(
            endpoint = %self.endpoint,
            account = %self.account,
            public_key = %self.remote.public_key,
            url = %self.remote.host_address,
            role = %self.remote.role,
        )
    )]
    pub(crate) async fn accept(self, db: &Database, ourself: Issuer) -> Result<EnrollmentRequest, Error> {
        let account_id = self.account;
        let username = format!("@{account_id}");

        let mut tx = db.begin().await?;

        Account::service(account_id, self.remote.public_key.encode())
            .save(&mut tx)
            .await
            .map_err(Error::CreateServiceAccount)?;

        info!(username, "Created a new service account");

        let endpoint_id = self.endpoint;

        let mut endpoint = Endpoint {
            id: endpoint_id,
            host_address: self.remote.host_address.clone(),
            status: endpoint::Status::AwaitingAcceptance,
            error: None,
            account: account_id,
            remote_account: self.remote.bearer_token.decoded.payload.account_id.into(),
            role: self.remote.role,
            description: Some(self.remote.description),
        };
        endpoint.save(&mut tx).await.map_err(Error::CreateEndpoint)?;

        endpoint::Tokens {
            bearer_token: Some(self.remote.bearer_token.encoded.clone()),
            access_token: None,
        }
        .save(&mut tx, endpoint.id)
        .await
        .map_err(Error::SetEndpointAccountToken)?;

        info!("Created a new endpoint for the service account");

        let bearer_token = endpoint::create_token(
            token::Purpose::Authorization,
            endpoint_id,
            account_id,
            self.remote.role,
            &ourself,
        )
        .map_err(Error::SignToken)?;

        account::Token::set(&mut tx, account_id, &bearer_token.encoded, bearer_token.expires())
            .await
            .map_err(Error::SetAccountToken)?;

        info!(
            expiration = %bearer_token.expires(),
            "Bearer token created",
        );

        endpoint.status = endpoint::Status::Operational;
        endpoint.save(&mut tx).await.map_err(Error::UpdateEndpointStatus)?;

        tx.commit().await?;

        info!("Accepted endpoint now operational");

        Ok(EnrollmentRequest {
            issuer: Some(ourself.into()),
            issue_token: bearer_token.encoded,
            role: ProtoRole::from(self.remote.role) as i32,
        })
    }
}

/// An enrollment error
#[derive(Debug, Error)]
pub(crate) enum Error {
    /// Reading an [`Account`] failed
    #[error("read account")]
    ReadAccount(#[source] account::Error),
    /// Creating a service [`Account`] failed
    #[error("create service account")]
    CreateServiceAccount(#[source] account::Error),
    /// Listing endpoints failed
    #[error("list endpoints")]
    ListEndpoints(#[source] database::Error),
    /// Creating an [`Endpoint`] failed
    #[error("create endpoint")]
    CreateEndpoint(#[source] database::Error),
    /// Setting the account token given by an endpoint failed
    #[error("set endpoint account token")]
    SetEndpointAccountToken(#[source] database::Error),
    /// Setting the account token given to an endpoint failed
    #[error("set account token")]
    SetAccountToken(#[source] account::Error),
    /// Updating the endpoint status failed
    #[error("update endpoint status")]
    UpdateEndpointStatus(#[source] database::Error),
    /// Invalid public key
    #[error("Invalid public key")]
    InvalidPublicKey,
    /// Public key doesn't match expected value
    #[error("public key mismatch, expected {expected} got {actual}")]
    PublicKeyMismatch {
        /// The expected key
        expected: EncodedPublicKey,
        /// The actual key
        actual: EncodedPublicKey,
    },
    /// Token signing failed
    #[error("sign token")]
    SignToken(#[source] token::Error),
    /// Token verification failed
    #[error("verify token")]
    VerifyToken(#[source] token::Error),
    /// Role of target service doesn't match expected role
    #[error("Role mismatch, expected {expected:?} provided {provided:?}")]
    RoleMismatch { expected: Role, provided: Role },
    /// Enrollment requires a bearer token
    #[error("Requires a bearer token")]
    RequireBearerToken,
    /// Client error
    #[error("client")]
    Client(#[from] client::Error),
    /// Database error
    #[error("database")]
    Database(#[from] database::Error),
    /// Grpc transport error
    #[error("grpc transport")]
    GrpcTransport(#[from] tonic::transport::Error),
    /// Grpc request
    #[error("grpc request")]
    GrpcRequest(#[from] tonic::Status),
    /// Malformed request
    #[error("Malformed request")]
    MalformedRequest,
    /// Unknown role
    #[error("Unknown role: {0}")]
    UnknownRole(i32),
}
