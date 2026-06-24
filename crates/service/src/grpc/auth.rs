//! Grpc service enabling authentication
use std::{collections::HashMap, sync::Arc};

use base64::Engine;
use chrono::{DateTime, Utc};
use futures_util::{
    Stream, StreamExt, future,
    stream::{self, BoxStream},
};
use rand::Rng;
use service_core::{
    Service, Token,
    auth::{self, AuthorizedService, AuthorizedServices},
    crypto::{self, EncodedPublicKey, EncodedSignature, KeyPair},
    token::{self, VerifiedToken},
};
use service_grpc::proto::auth::{
    AccountCredentials, Credentials, TokenResponse,
    auth_service_server::{AuthService as GrpcAuthService, AuthServiceServer},
    authenticate_stream, credentials,
};
use thiserror::Error;
use tokio::sync::Mutex;
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
    authorized_services: AuthorizedServices,
) -> ServiceServer {
    ServiceServer::new(AuthService {
        state: Arc::new(State {
            service,
            db,
            key_pair,
            authorized_services,
            service_bearer_tokens: Default::default(),
        }),
    })
}

#[derive(Debug)]
struct State {
    service: Service,
    db: Database,
    key_pair: KeyPair,
    authorized_services: AuthorizedServices,
    /// Runtime lifetime bearer tokens for authorized services
    service_bearer_tokens: Mutex<HashMap<AuthorizedService, String>>,
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
            client: auth::Client,
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

                            auth::Client::Account {
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

                            auth::Client::Service {
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

                    let (bearer_token, expires_on) =
                        create_token(&state.key_pair, state.service, &client, token::Purpose::Authorization)?;
                    let (access_token, _) =
                        create_token(&state.key_pair, state.service, &client, token::Purpose::Authentication)?;

                    match &client {
                        auth::Client::Account { account_id, .. } => {
                            let mut tx = state.db.begin().await?;

                            account::Token::set(&mut tx, *account_id, &bearer_token, expires_on)
                                .await
                                .map_err(Error::SaveAccountToken)?;

                            tx.commit().await?;
                        }
                        auth::Client::Service {
                            service_id, service, ..
                        } => {
                            state.service_bearer_tokens.lock().await.insert(
                                AuthorizedService {
                                    id: service_id.clone(),
                                    service: *service,
                                },
                                bearer_token.clone(),
                            );
                        }
                    }

                    debug!(%client, "Authenticate successful");

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
    let request_token = request
        .extensions()
        .get::<VerifiedToken>()
        .cloned()
        .ok_or(Error::MissingRequestToken)?;

    let client = request_token.decoded.payload.client;

    let bearer_token = match &client {
        auth::Client::Account { account_id, .. } => {
            let mut conn = state.db.acquire().await?;

            account::Token::get(conn.as_mut(), *account_id)
                .await
                .map_err(Error::ReadAccountToken)?
                .encoded
        }
        auth::Client::Service {
            service_id, service, ..
        } => {
            let service = AuthorizedService {
                id: service_id.clone(),
                service: *service,
            };

            state
                .service_bearer_tokens
                .lock()
                .await
                .get(&service)
                .ok_or(Error::NoIssuedServiceBearerToken)?
                .clone()
        }
    };

    if request_token.encoded != bearer_token {
        return Err(Error::NotCurrentBearerToken);
    }

    // We've already validated it's not expired in auth middleware
    // Looks good! Let's issue a new pair

    let (bearer_token, expires_on) =
        create_token(&state.key_pair, state.service, &client, token::Purpose::Authorization)?;
    let (access_token, _) = create_token(&state.key_pair, state.service, &client, token::Purpose::Authentication)?;

    match &client {
        auth::Client::Account { account_id, .. } => {
            let mut tx = state.db.begin().await?;

            account::Token::set(&mut tx, *account_id, &bearer_token, expires_on)
                .await
                .map_err(Error::SaveAccountToken)?;

            tx.commit().await?;
        }
        auth::Client::Service {
            service_id, service, ..
        } => {
            state.service_bearer_tokens.lock().await.insert(
                AuthorizedService {
                    id: service_id.clone(),
                    service: *service,
                },
                bearer_token.clone(),
            );
        }
    }

    debug!(
        %client,
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
    client: &auth::Client,
    purpose: token::Purpose,
) -> Result<(String, DateTime<Utc>), Error> {
    let now = Utc::now();
    let expires_on = now + purpose.duration();

    let auth_role = client.role();

    let token = Token::new(token::Payload {
        exp: expires_on.timestamp(),
        iat: now.timestamp(),
        iss: ourself.name().to_owned(),
        client: client.clone(),
        jti: None,
        purpose,
        permissions: auth_role.iter().flat_map(auth::Role::permissions).collect(),
    })
    .sign(key_pair)
    .map_err(Error::SignToken)?;

    Ok((token, expires_on))
}

/// Auth error
#[derive(Debug, Error)]
pub enum Error {
    /// Service bearer token not issued
    #[error("Service bearer token not issued")]
    NoIssuedServiceBearerToken,
    /// Request token doesn't match current bearer token
    #[error("Request token doesn't match current bearer token")]
    NotCurrentBearerToken,
    /// Token missing from request
    #[error("Token missing from request")]
    MissingRequestToken,
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
            Error::InvalidSignature(_)
            | Error::AccountLookup(..)
            | Error::NoIssuedServiceBearerToken
            | Error::NotCurrentBearerToken
            | Error::NonAuthorizedService(..) => tonic::Status::unauthenticated(""),
            Error::MissingRequestToken
            | Error::MalformedRequest
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
