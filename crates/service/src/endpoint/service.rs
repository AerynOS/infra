use std::{sync::Arc, time::Duration};

use http::Uri;
use service_core::{
    Token,
    crypto::{EncodedPublicKey, PublicKey},
    token::{self, VerifiedToken},
};
use service_grpc::endpoint::{
    EnrollmentRequest,
    endpoint_service_server::{EndpointService, EndpointServiceServer},
};
use thiserror::Error;
use tonic::async_trait;
use tracing::{debug, error, info};

use crate::{
    Database, account,
    endpoint::{
        self, Role,
        enrollment::{self, Issuer},
    },
    error,
    grpc::handle,
    sync::SharedMap,
};

pub type Server = EndpointServiceServer<Service>;

pub struct Service {
    state: Arc<State>,
}

pub fn service(role: Role, config: &crate::Config, state: &crate::State) -> Server {
    Server::new(Service {
        state: Arc::new(State {
            issuer: config.issuer(role, state.key_pair.clone()),
            db: state.service_db.clone(),
            pending_sent: state.pending_sent.clone(),
            upstream: config.upstream,
        }),
    })
}

/// State for endpoint handlers
#[derive(Debug, Clone)]
struct State {
    /// Issuer details of this service
    issuer: Issuer,
    /// Shared database of this service
    db: Database,
    /// Pending enrollment requests that are awaiting confirmation
    ///
    /// Only applicable for hub service
    pending_sent: SharedMap<endpoint::Id, enrollment::Sent>,
    /// Upstream hub to auto-accept enrollment with
    ///
    /// Only applicable for non-hub services
    upstream: Option<PublicKey>,
}

impl State {
    fn role(&self) -> Role {
        self.issuer.role
    }
}

#[async_trait]
impl EndpointService for Service {
    async fn enroll(
        &self,
        request: tonic::Request<EnrollmentRequest>,
    ) -> std::result::Result<tonic::Response<()>, tonic::Status> {
        let state = self.state.clone();

        handle(request, async move |req| enroll(state, req).await).await
    }

    async fn accept(
        &self,
        request: tonic::Request<EnrollmentRequest>,
    ) -> std::result::Result<tonic::Response<()>, tonic::Status> {
        let state = self.state.clone();

        handle(request, async move |req| accept(state, req).await).await
    }

    async fn decline(&self, request: tonic::Request<()>) -> std::result::Result<tonic::Response<()>, tonic::Status> {
        let state = self.state.clone();

        handle(request, async move |req| decline(state, req).await).await
    }
}

#[tracing::instrument(skip_all)]
async fn enroll(state: Arc<State>, request: tonic::Request<EnrollmentRequest>) -> Result<(), Error> {
    let upstream = *state.upstream.as_ref().ok_or(Error::UpstreamNotSet)?;

    let request = request.into_inner();
    let issuer = request.issuer.as_ref().ok_or(Error::MalformedRequest)?;
    let issue_token = request.issue_token.clone();
    let issuer_role = Role::from_proto(issuer.role()).ok_or(Error::UnknownRole(request.role))?;
    let role = Role::from_proto(request.role()).ok_or(Error::UnknownRole(request.role))?;

    let public_key = EncodedPublicKey::decode(&issuer.public_key).map_err(|_| Error::InvalidPublicKey)?;

    if public_key != upstream {
        return Err(Error::UpstreamMismatch {
            expected: upstream,
            provided: public_key,
        });
    }

    let verified_token =
        Token::verify(&issue_token, &public_key, &token::Validation::new()).map_err(Error::VerifyToken)?;

    if !matches!(verified_token.decoded.payload.purpose, token::Purpose::Authorization) {
        return Err(Error::RequireBearerToken);
    }
    if role != state.role() {
        return Err(Error::RoleMismatch {
            expected: state.role(),
            provided: role,
        });
    }

    info!(
        public_key = issuer.public_key,
        url = issuer.url,
        role = %issuer.role,
        "Enrollment requested"
    );

    let endpoint = endpoint::Id::generate();
    let account = account::Id::generate();

    debug!(%endpoint, %account, "Generated endpoint & account IDs for enrollment request");

    let recieved = enrollment::Received {
        endpoint,
        account,
        remote: enrollment::Remote {
            host_address: issuer.url.parse::<Uri>()?,
            public_key,
            role: issuer_role,
            bearer_token: verified_token,
        },
    };

    // Return from handler and accept in background
    //
    // D infra expects this operation returns before we
    // respond w/ acceptance
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(1)).await;

        if let Err(e) = recieved.accept(&state.db, state.issuer.clone()).await {
            error!(error=%error::chain(e), "Auto accept failed")
        };
    });

    Ok(())
}

#[tracing::instrument(skip_all)]
async fn accept(state: Arc<State>, request: tonic::Request<EnrollmentRequest>) -> Result<(), Error> {
    let token = request
        .extensions()
        .get::<VerifiedToken>()
        .cloned()
        .ok_or(Error::MissingRequestToken)?;

    let request = request.into_inner();
    let issuer = request.issuer.as_ref().ok_or(Error::MalformedRequest)?;
    let issuer_role = Role::from_proto(issuer.role()).ok_or(Error::UnknownRole(request.role))?;
    let role = Role::from_proto(request.role()).ok_or(Error::UnknownRole(request.role))?;

    let public_key = EncodedPublicKey::decode(&issuer.public_key).map_err(|_| Error::InvalidPublicKey)?;
    let verified_token =
        Token::verify(&request.issue_token, &public_key, &token::Validation::new()).map_err(Error::VerifyToken)?;

    if !matches!(verified_token.decoded.payload.purpose, token::Purpose::Authorization) {
        return Err(Error::RequireBearerToken);
    }
    if role != state.role() {
        return Err(Error::RoleMismatch {
            expected: state.role(),
            provided: role,
        });
    }

    let endpoint = token
        .decoded
        .payload
        .sub
        .parse::<endpoint::Id>()
        .map_err(Error::InvalidEndpoint)?;

    info!(
        %endpoint,
        public_key = issuer.public_key,
        url = issuer.url,
        role = %issuer.role,
        "Enrollment accepted"
    );

    state
        .pending_sent
        .remove(&endpoint)
        .await
        .ok_or(Error::MissingPendingEnrollment(endpoint))?
        .accepted(
            &state.db,
            enrollment::Remote {
                host_address: issuer.url.parse::<Uri>()?,
                public_key,
                role: issuer_role,
                bearer_token: verified_token,
            },
        )
        .await?;

    Ok(())
}

#[tracing::instrument(skip_all)]
async fn decline(state: Arc<State>, request: tonic::Request<()>) -> Result<(), Error> {
    let token = request
        .extensions()
        .get::<VerifiedToken>()
        .cloned()
        .ok_or(Error::MissingRequestToken)?;

    let endpoint = token
        .decoded
        .payload
        .sub
        .parse::<endpoint::Id>()
        .map_err(Error::InvalidEndpoint)?;

    if let Some(enrollment) = state.pending_sent.remove(&endpoint).await {
        info!(
            %endpoint,
            public_key = %enrollment.target.public_key,
            url = %enrollment.target.host_address,
            role = %enrollment.target.role,
            "Enrollment declined"
        );
    }

    Ok(())
}

#[derive(Debug, Error)]
#[allow(clippy::large_enum_variant)]
enum Error {
    #[error("Malformed request")]
    MalformedRequest,
    #[error("Token missing from request")]
    MissingRequestToken,
    #[error("Requires a bearer token")]
    RequireBearerToken,
    #[error("Invalid public key")]
    InvalidPublicKey,
    #[error("Upstream public key not set for auto-enrollment")]
    UpstreamNotSet,
    #[error("Upstream public key mismatch, expected: {expected} provided {provided}")]
    UpstreamMismatch { expected: PublicKey, provided: PublicKey },
    #[error("Uknown role: {0}")]
    UnknownRole(i32),
    #[error("Role mismatch, expected {expected:?} provided {provided:?}")]
    RoleMismatch { expected: Role, provided: Role },
    #[error("Pending enrollment missing for endpoint {0}")]
    MissingPendingEnrollment(endpoint::Id),
    #[error("invalid uri")]
    InvalidUrl(#[from] http::uri::InvalidUri),
    #[error("invalid endpoint")]
    InvalidEndpoint(#[source] uuid::Error),
    #[error("verify token")]
    VerifyToken(#[source] token::Error),
    #[error("enrollment")]
    Enrollment(#[from] enrollment::Error),
}

impl From<Error> for tonic::Status {
    fn from(error: Error) -> Self {
        match error {
            Error::MissingRequestToken => tonic::Status::unauthenticated(""),
            Error::Enrollment(_) | Error::UpstreamNotSet => tonic::Status::internal(""),
            Error::InvalidPublicKey
            | Error::InvalidUrl(_)
            | Error::InvalidEndpoint(_)
            | Error::RequireBearerToken
            | Error::VerifyToken(_)
            | Error::RoleMismatch { .. }
            | Error::MissingPendingEnrollment(_)
            | Error::UpstreamMismatch { .. }
            | Error::MalformedRequest
            | Error::UnknownRole(_) => tonic::Status::invalid_argument(""),
        }
    }
}
