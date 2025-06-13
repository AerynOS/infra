use std::sync::Arc;

use base64::Engine;
use chrono::{DateTime, Utc};
use futures_util::{
    Stream, StreamExt,
    stream::{self, BoxStream},
};
use rand::Rng;
use service_core::{
    Token, auth,
    crypto::{self, EncodedPublicKey, EncodedSignature, KeyPair, PublicKey},
    token::{self, VerifiedToken},
};
use service_grpc::account::{
    AuthenticateRequest, AuthenticateResponse, Credentials, TokenResponse,
    account_service_server::{AccountService, AccountServiceServer},
    authenticate_request, authenticate_response,
};
use thiserror::Error;
use tonic::async_trait;
use tracing::{debug, error, info_span};
use tracing_futures::Instrument;

use crate::{Account, Database, Endpoint, account, database, endpoint::Role, grpc};

pub type Server = AccountServiceServer<Service>;

pub struct Service {
    state: Arc<State>,
}

pub fn service(role: Role, state: &crate::State) -> Server {
    Server::new(Service {
        state: Arc::new(State {
            role,
            db: state.service_db.clone(),
            key_pair: state.key_pair.clone(),
        }),
    })
}

#[derive(Debug, Clone)]
struct State {
    role: Role,
    db: Database,
    key_pair: KeyPair,
}

#[async_trait]
impl AccountService for Service {
    type AuthenticateStream = BoxStream<'static, Result<AuthenticateResponse, tonic::Status>>;

    async fn authenticate(
        &self,
        request: tonic::Request<tonic::Streaming<AuthenticateRequest>>,
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
    request: tonic::Request<tonic::Streaming<AuthenticateRequest>>,
) -> impl Stream<Item = Result<AuthenticateResponse, Error>> + 'static {
    #[allow(clippy::large_enum_variant)]
    enum State {
        Idle {
            db: Database,
            key_pair: KeyPair,
            role: Role,
        },
        ChallengeSent {
            db: Database,
            key_pair: KeyPair,
            role: Role,
            account: Account,
            endpoint: Option<Endpoint>,
            public_key: PublicKey,
            challenge: String,
        },
        Finished,
    }

    let state = State::Idle {
        db: state.db.clone(),
        key_pair: state.key_pair.clone(),
        role: state.role,
    };

    stream::try_unfold((request.into_inner(), state), |(mut incoming, state)| async move {
        let Some(request) = incoming.next().await else {
            return Ok(None);
        };

        let body = request?.body.ok_or(Error::MalformedRequest)?;

        match (state, body) {
            (
                State::Idle { db, key_pair, role },
                authenticate_request::Body::Credentials(Credentials { username, public_key }),
            ) => {
                let public_key = EncodedPublicKey::decode(&public_key).map_err(Error::MalformedPublicKey)?;
                let encoded_public_key = public_key.encode();

                let mut conn = db.acquire().await?;

                let account = Account::lookup_with_credentials(conn.as_mut(), &username, &encoded_public_key)
                    .await
                    .map_err(|error| Error::AccountLookup(username.clone(), encoded_public_key.clone(), error))?;
                let endpoint = Endpoint::get_by_account_id(conn.as_mut(), account.id).await?;

                let mut rand = rand::thread_rng();
                let mut challenge = String::default();

                base64::prelude::BASE64_URL_SAFE_NO_PAD.encode_string(rand.r#gen::<[u8; 16]>(), &mut challenge);

                debug!(account = %account.id, "Authenticate challenge created");

                Ok(Some((
                    AuthenticateResponse {
                        body: Some(authenticate_response::Body::Challenge(challenge.clone())),
                    },
                    (
                        incoming,
                        State::ChallengeSent {
                            db,
                            key_pair,
                            role,
                            account,
                            endpoint,
                            public_key,
                            challenge,
                        },
                    ),
                )))
            }
            (
                State::ChallengeSent {
                    db,
                    key_pair,
                    role,
                    account,
                    endpoint,
                    public_key,
                    challenge,
                },
                authenticate_request::Body::Signature(signature),
            ) => {
                let signature = EncodedSignature::decode(&signature).map_err(Error::MalformedSignature)?;

                public_key
                    .verify(challenge.as_bytes(), &signature)
                    .map_err(Error::InvalidSignature)?;

                let (bearer_token, expires_on) = create_token(
                    &key_pair,
                    &account,
                    endpoint.as_ref(),
                    role,
                    token::Purpose::Authorization,
                )?;
                let (access_token, _) = create_token(
                    &key_pair,
                    &account,
                    endpoint.as_ref(),
                    role,
                    token::Purpose::Authentication,
                )?;

                let mut tx = db.begin().await?;

                account::Token::set(&mut tx, account.id, &bearer_token, expires_on)
                    .await
                    .map_err(Error::SaveAccountToken)?;

                tx.commit().await?;

                debug!(
                    account = %account.id,
                    "Authenticate successful",
                );

                Ok(Some((
                    AuthenticateResponse {
                        body: Some(authenticate_response::Body::Tokens(TokenResponse {
                            bearer_token,
                            access_token,
                        })),
                    },
                    (incoming, State::Finished),
                )))
            }
            _ => Err(Error::MalformedRequest),
        }
    })
    .instrument(info_span!("authenticate"))
}

#[tracing::instrument(skip_all)]
async fn refresh_token(state: Arc<State>, request: tonic::Request<()>) -> Result<TokenResponse, Error> {
    let request_token = request
        .extensions()
        .get::<VerifiedToken>()
        .cloned()
        .ok_or(Error::MissingRequestToken)?;

    let token::Payload { account_id, .. } = request_token.decoded.payload;

    let account_id = account::Id::from(account_id);

    let mut tx = state.db.begin().await?;

    let account = Account::get(tx.as_mut(), account_id)
        .await
        .map_err(Error::ReadAccount)?;
    let endpoint = Endpoint::get_by_account_id(tx.as_mut(), account.id).await?;

    // Confirm this is their current account token
    let current_token = account::Token::get(tx.as_mut(), account_id)
        .await
        .map_err(Error::ReadAccountToken)?;

    if current_token.encoded != request_token.encoded {
        return Err(Error::NotCurrentAccountToken);
    }

    // We've already validated it's not expired in auth middleware
    // Looks good! Let's issue a new pair

    let (bearer_token, expires_on) = create_token(
        &state.key_pair,
        &account,
        endpoint.as_ref(),
        state.role,
        token::Purpose::Authorization,
    )?;
    let (access_token, _) = create_token(
        &state.key_pair,
        &account,
        endpoint.as_ref(),
        state.role,
        token::Purpose::Authentication,
    )?;

    // Update their account token to the newly issued one
    account::Token::set(&mut tx, account_id, &bearer_token, expires_on)
        .await
        .map_err(Error::SaveAccountToken)?;

    tx.commit().await?;

    debug!(
        account = %account.id,
        "Refresh token successful",
    );

    Ok(TokenResponse {
        bearer_token,
        access_token,
    })
}

fn create_token(
    key_pair: &KeyPair,
    account: &Account,
    endpoint: Option<&Endpoint>,
    role: Role,
    purpose: token::Purpose,
) -> Result<(String, DateTime<Utc>), Error> {
    let now = Utc::now();
    let expires_on = now + purpose.duration();

    let (aud, sub) = if let Some(endpoint) = endpoint {
        (endpoint.role.to_string(), endpoint.id.to_string())
    } else {
        (account.id.to_string(), account.id.to_string())
    };
    let auth_role = if let Some(endpoint) = endpoint {
        match endpoint.role {
            Role::Builder => Some(auth::Role::Builder),
            Role::RepositoryManager => Some(auth::Role::RepositoryManager),
            Role::Hub => Some(auth::Role::Hub),
        }
    } else if matches!(account.kind, account::Kind::Admin) {
        Some(auth::Role::Admin)
    } else {
        None
    };

    let token = Token::new(token::Payload {
        aud,
        exp: expires_on.timestamp(),
        iat: now.timestamp(),
        iss: role.service_name().to_string(),
        sub,
        jti: None,
        purpose,
        account_id: account.id.into(),
        account_type: account.kind,
        permissions: auth_role.iter().flat_map(auth::Role::permissions).collect(),
    })
    .sign(key_pair)
    .map_err(Error::SignToken)?;

    Ok((token, expires_on))
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("Account not issued an account token")]
    NoIssuedAccountToken,
    #[error("Request token doesn't match current account token")]
    NotCurrentAccountToken,
    #[error("Token missing from request")]
    MissingRequestToken,
    #[error("Malformed request")]
    MalformedRequest,
    #[error("malformed public key")]
    MalformedPublicKey(#[source] crypto::Error),
    #[error("malformed signature")]
    MalformedSignature(#[source] crypto::Error),
    #[error("signature verification")]
    InvalidSignature(#[source] crypto::Error),
    #[error("saving new account token")]
    SaveAccountToken(#[source] account::Error),
    #[error("reading account token")]
    ReadAccountToken(#[source] account::Error),
    #[error("reading account")]
    ReadAccount(#[source] account::Error),
    #[error("account lookup for username {0}, public_key {1}")]
    AccountLookup(String, EncodedPublicKey, #[source] account::Error),
    #[error("sign token")]
    SignToken(#[source] token::Error),
    #[error(transparent)]
    Request(#[from] tonic::Status),
    #[error(transparent)]
    Database(#[from] database::Error),
}

impl From<Error> for tonic::Status {
    fn from(error: Error) -> Self {
        match error {
            Error::NoIssuedAccountToken | Error::NotCurrentAccountToken => tonic::Status::unauthenticated(""),
            Error::MissingRequestToken
            | Error::MalformedRequest
            | Error::SignToken(_)
            | Error::SaveAccountToken(_)
            | Error::ReadAccountToken(_)
            | Error::ReadAccount(_)
            | Error::Database(_) => tonic::Status::internal(""),
            Error::MalformedPublicKey(_) | Error::MalformedSignature(_) => tonic::Status::invalid_argument(""),
            Error::InvalidSignature(_) | Error::AccountLookup(..) => tonic::Status::unauthenticated(""),
            Error::Request(status) => status,
        }
    }
}
