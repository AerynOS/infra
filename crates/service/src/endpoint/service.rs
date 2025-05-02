use std::sync::Arc;

use http::Uri;
use service_core::{
    Token,
    crypto::{EncodedPublicKey, PublicKey},
    token,
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
    grpc::handle,
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
            downstreams: config.downstreams.clone(),
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
    /// Downstream services to auto-accept enrollment with
    downstreams: Vec<PublicKey>,
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
    ) -> std::result::Result<tonic::Response<EnrollmentRequest>, tonic::Status> {
        let state = self.state.clone();

        handle(request, async move |req| enroll(state, req).await).await
    }
}

#[tracing::instrument(skip_all)]
async fn enroll(state: Arc<State>, request: tonic::Request<EnrollmentRequest>) -> Result<EnrollmentRequest, Error> {
    let request = request.into_inner();
    let issuer = request.issuer.as_ref().ok_or(Error::MalformedRequest)?;
    let issue_token = request.issue_token.clone();
    let issuer_role = Role::from_proto(issuer.role()).ok_or(Error::UnknownRole(request.role))?;
    let role = Role::from_proto(request.role()).ok_or(Error::UnknownRole(request.role))?;

    let public_key = EncodedPublicKey::decode(&issuer.public_key).map_err(|_| Error::InvalidPublicKey)?;

    if !state.downstreams.contains(&public_key) {
        return Err(Error::DownstreamMismatch { provided: public_key });
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

    let received = enrollment::Received {
        endpoint,
        account,
        remote: enrollment::Remote {
            host_address: issuer.url.parse::<Uri>()?,
            public_key,
            role: issuer_role,
            bearer_token: verified_token,
        },
    };

    Ok(received.accept(&state.db, state.issuer.clone()).await?)
}

#[derive(Debug, Error)]
#[allow(clippy::large_enum_variant)]
enum Error {
    #[error("Malformed request")]
    MalformedRequest,
    #[error("Requires a bearer token")]
    RequireBearerToken,
    #[error("Invalid public key")]
    InvalidPublicKey,
    #[error("Public key not defined in downstreams: {provided}")]
    DownstreamMismatch { provided: PublicKey },
    #[error("Unknown role: {0}")]
    UnknownRole(i32),
    #[error("Role mismatch, expected {expected:?} provided {provided:?}")]
    RoleMismatch { expected: Role, provided: Role },
    #[error("invalid uri")]
    InvalidUrl(#[from] http::uri::InvalidUri),
    #[error("verify token")]
    VerifyToken(#[source] token::Error),
    #[error("enrollment")]
    Enrollment(#[from] enrollment::Error),
}

impl From<Error> for tonic::Status {
    fn from(error: Error) -> Self {
        match error {
            Error::Enrollment(_) => tonic::Status::internal(""),
            Error::InvalidPublicKey
            | Error::InvalidUrl(_)
            | Error::RequireBearerToken
            | Error::VerifyToken(_)
            | Error::RoleMismatch { .. }
            | Error::DownstreamMismatch { .. }
            | Error::MalformedRequest
            | Error::UnknownRole(_) => tonic::Status::invalid_argument(""),
        }
    }
}
