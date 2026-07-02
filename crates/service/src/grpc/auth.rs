//! Grpc service enabling authentication
use std::sync::Arc;

use base64::Engine;
use chrono::{DateTime, Utc};
use futures_util::{
    Stream, StreamExt, future,
    stream::{self, BoxStream},
};
use rand::Rng;
use service_core::{
    Service, Session, Token,
    auth::{self, AuthorizedServices},
    crypto::{self, EncodedPublicKey, EncodedSignature, KeyPair},
    session::{self, ActiveSessions},
    token,
};
use service_grpc::proto::auth::{
    AccountCredentials, Credentials, TokenResponse,
    auth_service_server::{AuthService as GrpcAuthService, AuthServiceServer},
    authenticate_stream, credentials,
};
use thiserror::Error;
use tonic::async_trait;
use tracing::{debug, info_span};
use tracing_futures::Instrument;

use crate::{Account, Database, account, database, grpc};

/// Serves the grpc auth service
pub type ServiceServer = AuthServiceServer<AuthService>;

/// Auth service implementation
pub struct AuthService {
    state: Arc<State>,
}

/// Returns a server that can serves the grpc auth service
pub fn service(
    service: Service,
    db: Database,
    key_pair: KeyPair,
    active_sessions: ActiveSessions,
    authorized_services: AuthorizedServices,
) -> ServiceServer {
    ServiceServer::new(AuthService {
        state: Arc::new(State {
            service,
            db,
            key_pair,
            active_sessions,
            authorized_services,
        }),
    })
}

#[derive(Debug)]
struct State {
    service: Service,
    db: Database,
    key_pair: KeyPair,
    active_sessions: ActiveSessions,
    authorized_services: AuthorizedServices,
}

#[async_trait]
impl GrpcAuthService for AuthService {
    type AuthenticateStream = BoxStream<'static, Result<authenticate_stream::Outgoing, tonic::Status>>;

    async fn authenticate(
        &self,
        request: tonic::Request<tonic::Streaming<authenticate_stream::Incoming>>,
    ) -> Result<tonic::Response<Self::AuthenticateStream>, tonic::Status> {
        let state = self.state.clone();

        grpc::handle_server_streaming(request, |request| authenticate(state, request))
    }

    async fn refresh_token(
        &self,
        request: tonic::Request<()>,
    ) -> Result<tonic::Response<TokenResponse>, tonic::Status> {
        let state = self.state.clone();

        grpc::handle(request, async move |request| refresh_token(state, request).await).await
    }
}

fn authenticate(
    state: Arc<State>,
    request: tonic::Request<tonic::Streaming<authenticate_stream::Incoming>>,
) -> impl Stream<Item = Result<authenticate_stream::Outgoing, Error>> + 'static {
    #[allow(clippy::large_enum_variant)]
    enum Progress {
        Idle {
            state: Arc<State>,
        },
        ChallengeSent {
            state: Arc<State>,
            client: session::Client,
            challenge: String,
        },
        Finished,
    }

    let progress = Progress::Idle { state: state.clone() };

    // Send public key as first message to enable client side
    // mutual auth flow
    stream::once(future::ready(Ok(authenticate_stream::Outgoing {
        body: Some(authenticate_stream::outgoing::Body::PublicKey(
            state.key_pair.public_key().to_string(),
        )),
    })))
    // Handle stateful stream w/ client
    .chain(stream::try_unfold(
        (request.into_inner(), progress),
        |(mut incoming, progress)| async move {
            let Some(request) = incoming.next().await else {
                return Ok(None);
            };

            let body = request?.body.ok_or(Error::MalformedRequest)?;

            match (progress, body) {
                (
                    Progress::Idle { state },
                    authenticate_stream::incoming::Body::Credentials(Credentials { credentials }),
                ) => {
                    let credentials = credentials.ok_or(Error::MalformedRequest)?;

                    let client = match credentials {
                        credentials::Credentials::Account(AccountCredentials { username, public_key }) => {
                            let mut conn = state.db.acquire().await?;

                            let public_key =
                                EncodedPublicKey::decode(&public_key).map_err(Error::MalformedPublicKey)?;
                            let encoded_public_key = public_key.encode();

                            let account =
                                Account::lookup_with_credentials(conn.as_mut(), &username, &encoded_public_key)
                                    .await
                                    .map_err(|error| {
                                        Error::AccountLookup(username.clone(), encoded_public_key, error)
                                    })?;

                            session::Client::Account {
                                account_id: account.id,
                                account_kind: account.kind,
                                public_key,
                            }
                        }
                        credentials::Credentials::Service(credentials) => {
                            let service = credentials.service().to_core().ok_or(Error::MalformedRequest)?;

                            let public_key =
                                EncodedPublicKey::decode(&credentials.public_key).map_err(Error::MalformedPublicKey)?;

                            let authorized_service = match state.authorized_services.get(&public_key) {
                                Some(authorized_service) if authorized_service.service == service => {
                                    authorized_service.clone()
                                }
                                _ => return Err(Error::NonAuthorizedService(service, public_key.encode())),
                            };

                            session::Client::Service {
                                service_id: authorized_service.id,
                                service: authorized_service.service,
                                public_key,
                            }
                        }
                    };

                    let mut rand = rand::thread_rng();
                    let mut challenge = String::default();

                    base64::prelude::BASE64_URL_SAFE_NO_PAD.encode_string(rand.r#gen::<[u8; 16]>(), &mut challenge);

                    debug!(%client, "Authenticate challenge created");

                    Ok(Some((
                        authenticate_stream::Outgoing {
                            body: Some(authenticate_stream::outgoing::Body::Challenge(challenge.clone())),
                        },
                        (
                            incoming,
                            Progress::ChallengeSent {
                                state,
                                client,
                                challenge,
                            },
                        ),
                    )))
                }
                // If we receive a challenge request, we must respond to it so the client
                // can verify our public key
                (Progress::Idle { state }, authenticate_stream::incoming::Body::Challenge(challenge)) => {
                    let signature = base64::prelude::BASE64_URL_SAFE_NO_PAD
                        .encode(state.key_pair.sign(challenge.as_bytes()).to_bytes());

                    Ok(Some((
                        authenticate_stream::Outgoing {
                            body: Some(authenticate_stream::outgoing::Body::Signature(signature)),
                        },
                        (incoming, Progress::Idle { state }),
                    )))
                }
                (
                    Progress::ChallengeSent {
                        state,
                        client,
                        challenge,
                    },
                    authenticate_stream::incoming::Body::Signature(signature),
                ) => {
                    let signature = EncodedSignature::decode(&signature).map_err(Error::MalformedSignature)?;

                    client
                        .public_key()
                        .verify(challenge.as_bytes(), &signature)
                        .map_err(Error::InvalidSignature)?;

                    // Create a new session & provision tokens
                    let session_id = session::Id::generate();

                    let (bearer_token, expires_on) = create_token(
                        &state.key_pair,
                        state.service,
                        session_id,
                        &client,
                        token::Purpose::Authorization,
                    )?;
                    let (access_token, _) = create_token(
                        &state.key_pair,
                        state.service,
                        session_id,
                        &client,
                        token::Purpose::Authentication,
                    )?;

                    debug!(%client, %session_id, "Authentication successful");

                    state.active_sessions.add(Session {
                        id: session_id,
                        client,
                        expires: expires_on,
                    });

                    Ok(Some((
                        authenticate_stream::Outgoing {
                            body: Some(authenticate_stream::outgoing::Body::Tokens(TokenResponse {
                                bearer_token,
                                access_token,
                            })),
                        },
                        (incoming, Progress::Finished),
                    )))
                }
                _ => Err(Error::MalformedRequest),
            }
        },
    ))
    .instrument(info_span!("authenticate"))
}

#[tracing::instrument(skip_all)]
async fn refresh_token(state: Arc<State>, request: tonic::Request<()>) -> Result<TokenResponse, Error> {
    // We've already validated we have non-expired bearer token

    // Assuming we still have a non-revoked active session,
    // we can safely refresh / extend it
    let session = request
        .extensions()
        .get::<Session>()
        .cloned()
        .ok_or(Error::NoActiveSessionDuringRefresh)?;

    // Looks good! Let's issue a new pair

    let (bearer_token, expires_on) = create_token(
        &state.key_pair,
        state.service,
        session.id,
        &session.client,
        token::Purpose::Authorization,
    )?;
    let (access_token, _) = create_token(
        &state.key_pair,
        state.service,
        session.id,
        &session.client,
        token::Purpose::Authentication,
    )?;

    // Extend the session off the new expiration time
    state.active_sessions.extend(&session.id, expires_on);

    debug!(
        session_id = %session.id,
        client = %session.client,
        "Refresh token successful",
    );

    Ok(TokenResponse {
        bearer_token,
        access_token,
    })
}

fn create_token(
    key_pair: &KeyPair,
    ourself: Service,
    session_id: session::Id,
    client: &session::Client,
    purpose: token::Purpose,
) -> Result<(String, DateTime<Utc>), Error> {
    let now = Utc::now();
    let expires_on = now + purpose.duration();

    let auth_role = client.role();

    let token = Token::new(token::Payload {
        exp: expires_on.timestamp(),
        iat: now.timestamp(),
        iss: ourself.name().to_owned(),
        jti: None,
        purpose,
        permissions: auth_role.iter().flat_map(auth::Role::permissions).collect(),
        kind: token::Kind::Session { session_id },
    })
    .sign(key_pair)
    .map_err(Error::SignToken)?;

    Ok((token, expires_on))
}

/// Auth error
#[derive(Debug, Error)]
pub enum Error {
    /// No active session found during token refresh
    #[error("No active session found during token refresh")]
    NoActiveSessionDuringRefresh,
    /// Malformed request
    #[error("Malformed request")]
    MalformedRequest,
    /// Malformed public key
    #[error("malformed public key")]
    MalformedPublicKey(#[source] crypto::Error),
    /// Malformed signature
    #[error("malformed signature")]
    MalformedSignature(#[source] crypto::Error),
    /// Signature verification
    #[error("signature verification")]
    InvalidSignature(#[source] crypto::Error),
    /// Failed to save new account token
    #[error("saving new account token")]
    SaveAccountToken(#[source] account::Error),
    /// Failed to read account token
    #[error("reading account token")]
    ReadAccountToken(#[source] account::Error),
    /// Failed to read account
    #[error("reading account")]
    ReadAccount(#[source] account::Error),
    /// Account lookup failed
    #[error("account lookup for username {0}, public_key {1}")]
    AccountLookup(String, EncodedPublicKey, #[source] account::Error),
    /// Service is not authorized
    #[error("service {0} is not authorized with public_key {1}")]
    NonAuthorizedService(Service, EncodedPublicKey),
    /// Failed to sign token
    #[error("sign token")]
    SignToken(#[source] token::Error),
    /// Request error
    #[error(transparent)]
    Request(#[from] tonic::Status),
    /// Database error
    #[error(transparent)]
    Database(#[from] database::Error),
}

impl From<Error> for tonic::Status {
    fn from(error: Error) -> Self {
        match error {
            Error::NoActiveSessionDuringRefresh
            | Error::InvalidSignature(_)
            | Error::AccountLookup(..)
            | Error::NonAuthorizedService(..) => tonic::Status::unauthenticated(""),
            Error::MalformedRequest
            | Error::SignToken(_)
            | Error::SaveAccountToken(_)
            | Error::ReadAccountToken(_)
            | Error::ReadAccount(_)
            | Error::Database(_) => tonic::Status::internal(""),
            Error::MalformedPublicKey(_) | Error::MalformedSignature(_) => tonic::Status::invalid_argument(""),
            Error::Request(status) => status,
        }
    }
}
