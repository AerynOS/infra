use std::convert::Infallible;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;
use std::{io, mem};

use async_trait::async_trait;
use base64::Engine;
use futures_util::future::BoxFuture;
use futures_util::{FutureExt, StreamExt};
use http::{HeaderValue, Request, Response, Uri};
use serde::Deserialize;
use service_core::auth;
use service_core::crypto::KeyPair;
use service_core::token::VerifiedToken;
use service_grpc::Method;
use service_grpc::account::{
    AuthenticateRequest, Credentials as GrpcCredentials, authenticate_request, authenticate_response,
};
use thiserror::Error;
use tokio::fs;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::body::Body;
use tonic::metadata::MetadataValue;
use tonic::service::Interceptor;
use tonic::transport::{self, Certificate, Channel, ClientTlsConfig, Identity};
use tower::Service;
use tracing::error;

pub use service_grpc::account::account_service_client::AccountServiceClient;
pub use service_grpc::endpoint::endpoint_service_client::EndpointServiceClient;
pub use service_grpc::summit::summit_service_client::SummitServiceClient;
pub use service_grpc::vessel::vessel_service_client::VesselServiceClient;

const TOKEN_VALIDITY: Duration = Duration::from_secs(15 * 60);

/// Tls configuration
#[derive(Debug, Deserialize, Default)]
pub struct TlsConfig {
    /// Path to PEM encoded CA certificate
    pub ca: Option<PathBuf>,
    /// Path to PEM encoded identity certificate
    pub certificate: Option<PathBuf>,
    /// Path to PEM encoded identity key
    pub key: Option<PathBuf>,
}

impl TlsConfig {
    async fn load(&self) -> Result<ClientTlsConfig, ConnectError> {
        let mut config = ClientTlsConfig::new().with_enabled_roots();

        let load = async |path: &Path| {
            fs::read(path)
                .await
                .map_err(|err| ConnectError::LoadFile(err, path.to_owned()))
        };

        if let Some(ca) = &self.ca {
            config = config.ca_certificate(Certificate::from_pem(load(ca).await?));
        }
        if let Some(cert) = &self.certificate {
            let key = if let Some(key) = &self.key {
                load(key).await?
            } else {
                vec![]
            };

            let identity = Identity::from_pem(Certificate::from_pem(load(cert).await?), key);

            config = config.identity(identity);
        }

        Ok(config)
    }
}

/// Extension trait for connecting a client
/// with an [`AuthProvider`]
#[async_trait]
pub trait AuthClient<A>: Sized {
    async fn connect_with_auth(uri: Uri, tls_config: Option<TlsConfig>, provider: A) -> Result<Self, ConnectError>
    where
        A: AuthProvider + 'static;
}

/// Extension trait for connecting a client with a token
#[async_trait]
pub trait TokenClient: Sized {
    async fn connect_with_token(uri: Uri, tls_config: Option<TlsConfig>, token: &str) -> Result<Self, ConnectError>;
}

macro_rules! service_client {
    ($name:ident) => {
        #[async_trait::async_trait]
        impl<A> AuthClient<A> for $name<AuthService<A>> {
            async fn connect_with_auth(
                uri: Uri,
                tls_config: Option<TlsConfig>,
                provider: A,
            ) -> Result<$name<AuthService<A>>, ConnectError>
            where
                A: AuthProvider + 'static,
            {
                let mut endpoint = ::tonic::transport::Endpoint::new(uri)?
                    .http2_keep_alive_interval(Duration::from_secs(60))
                    .keep_alive_timeout(Duration::from_secs(20));

                if let Some(config) = tls_config {
                    endpoint = endpoint.tls_config(config.load().await?)?;
                }

                let channel = endpoint.connect().await?;

                Ok(Self::new(AuthService { channel, provider }))
            }
        }

        #[async_trait::async_trait]
        impl TokenClient
            for $name<::tonic::service::interceptor::InterceptedService<::tonic::transport::Channel, TokenInterceptor>>
        {
            async fn connect_with_token(
                uri: Uri,
                tls_config: Option<TlsConfig>,
                token: &str,
            ) -> Result<
                $name<::tonic::service::interceptor::InterceptedService<::tonic::transport::Channel, TokenInterceptor>>,
                ConnectError,
            > {
                let mut endpoint = ::tonic::transport::Endpoint::new(uri)?
                    .http2_keep_alive_interval(Duration::from_secs(60))
                    .keep_alive_timeout(Duration::from_secs(20));

                if let Some(config) = tls_config {
                    endpoint = endpoint.tls_config(config.load().await?)?;
                }

                let channel = endpoint.connect().await?;

                Ok($name::with_interceptor(
                    channel,
                    TokenInterceptor {
                        token: token.to_owned(),
                    },
                ))
            }
        }
    };
}

/// Service middleware for injecting & refreshing auth tokens
pub struct AuthService<A> {
    channel: Channel,
    provider: A,
}

impl<A> Service<Request<Body>> for AuthService<A>
where
    A: AuthProvider,
{
    type Response = Response<Body>;
    type Error = Error<A::Error>;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
        self.channel.poll_ready(cx).map_err(Error::Transport)
    }

    fn call(&mut self, mut req: Request<Body>) -> Self::Future {
        let cloned = self.channel.clone();
        let mut channel = mem::replace(&mut self.channel, cloned);
        let provider = self.provider.clone();

        async move {
            let method = Method::from_path(req.uri().path());
            let flags = method.map(Method::flags).unwrap_or_default();

            let mut token = None;

            if flags.intersects(auth::Flags::ACCESS_TOKEN | auth::Flags::BEARER_TOKEN) {
                let tokens = provider.tokens().await.map_err(Error::AuthProvider)?;

                let mut encoded_bearer = tokens.bearer_token.as_ref().map(|t| t.encoded.clone());
                let mut encoded_access = tokens.access_token.as_ref().map(|t| t.encoded.clone());

                // If provider supports persisting refresh tokens, ensure they're refreshed
                if A::REFRESH_ENABLED {
                    let refresh_bearer = tokens
                        .bearer_token
                        .as_ref()
                        .is_none_or(|bearer| bearer.decoded.is_expired_in(TOKEN_VALIDITY));
                    let refresh_access = tokens
                        .access_token
                        .as_ref()
                        .is_none_or(|access| access.decoded.is_expired_in(TOKEN_VALIDITY));

                    if refresh_bearer {
                        if let Some(credentials) = provider.credentials() {
                            match authenticate(channel.clone(), credentials).await {
                                Ok(refreshed) => {
                                    provider
                                        .tokens_refreshed(&refreshed)
                                        .await
                                        .map_err(Error::AuthProvider)?;

                                    encoded_bearer = Some(refreshed.bearer_token);
                                    encoded_access = Some(refreshed.access_token);
                                }
                                Err(error) => provider
                                    .token_refresh_failed(&error)
                                    .await
                                    .map_err(Error::AuthProvider)?,
                            }
                        }
                    } else if refresh_access && let Some(bearer) = &tokens.bearer_token {
                        match refresh_tokens(channel.clone(), &bearer.encoded).await {
                            Ok(refreshed) => {
                                provider
                                    .tokens_refreshed(&refreshed)
                                    .await
                                    .map_err(Error::AuthProvider)?;

                                encoded_bearer = Some(refreshed.bearer_token);
                                encoded_access = Some(refreshed.access_token);
                            }
                            Err(error) => provider
                                .token_refresh_failed(&error)
                                .await
                                .map_err(Error::AuthProvider)?,
                        }
                    }
                }

                if flags.contains(auth::Flags::BEARER_TOKEN) {
                    token = encoded_bearer;
                } else {
                    token = encoded_access;
                }
            }

            if let Some(token) = token {
                req.headers_mut().insert(
                    "authorization",
                    HeaderValue::from_str(&format!("Bearer {token}")).expect("JWT token"),
                );
            }

            channel.call(req).await.map_err(Error::Transport)
        }
        .boxed()
    }
}

service_client!(AccountServiceClient);
service_client!(EndpointServiceClient);
service_client!(SummitServiceClient);
service_client!(VesselServiceClient);

/// Tokens needed to make authenticated requests
#[derive(Debug, Clone, Default)]
pub struct VerifiedTokens {
    /// A bearer token
    pub bearer_token: Option<VerifiedToken>,
    /// An access token
    pub access_token: Option<VerifiedToken>,
}

/// Refreshed tokens
#[derive(Debug, Clone, Default)]
pub struct RefreshedTokens {
    /// A bearer token
    pub bearer_token: String,
    /// An access token
    pub access_token: String,
}

/// Credentials used to authenticate
#[derive(Debug, Clone)]
pub struct Credentials {
    /// Account username
    pub username: String,
    /// Account keypair
    pub key_pair: KeyPair,
}

/// A provider of tokens and possibly credentials & persistence
/// to enable automatic token refreshing
#[async_trait]
pub trait AuthProvider: Clone + Send + Sync + 'static {
    /// An auth provider error
    type Error: std::error::Error + Send + Sync;

    /// Can this provider persist refreshed tokens?
    ///
    /// Must be set true for client to call [`AuthProvider::tokens_refreshed`]
    /// after an expired token is refreshed.
    const REFRESH_ENABLED: bool = false;

    /// Returns credentials used for authenticating if tokens
    /// are expired
    fn credentials(&self) -> Option<Credentials> {
        None
    }

    /// Returns current tokens from this provider
    async fn tokens(&self) -> Result<VerifiedTokens, Self::Error>;

    /// Called when client refreshes tokens, allowing provider to persist the tokens.
    async fn tokens_refreshed(&self, _tokens: &RefreshedTokens) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Called when client fails to refresh a token
    async fn token_refresh_failed(&self, _error: &Error<Self::Error>) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// Auth with credentials and no refresh persistence
#[derive(Debug, Clone)]
pub struct CredentialsAuth(Credentials);

impl CredentialsAuth {
    pub fn new(username: String, key_pair: KeyPair) -> Self {
        Self(Credentials { username, key_pair })
    }
}

#[async_trait]
impl AuthProvider for CredentialsAuth {
    type Error = Infallible;

    const REFRESH_ENABLED: bool = true;

    fn credentials(&self) -> Option<Credentials> {
        Some(self.0.clone())
    }

    async fn tokens(&self) -> Result<VerifiedTokens, Self::Error> {
        Ok(VerifiedTokens::default())
    }
}

#[derive(Debug, Error)]
pub enum ConnectError {
    #[error("failed to load {1:?}")]
    LoadFile(#[source] io::Error, PathBuf),
    #[error(transparent)]
    Transport(#[from] transport::Error),
}

/// A client error
#[derive(Debug, Error)]
pub enum Error<E = Infallible>
where
    E: std::error::Error,
{
    /// Missing bearer token
    #[error("Missing bearer token")]
    MissingBearerToken,
    /// Missing access token
    #[error("Missing access token")]
    MissingAccessToken,
    /// Failed to refresh bearer token
    #[error("Failed to refresh bearer token")]
    RefreshBearerTokenFailed,
    /// Failed to refresh access token
    #[error("Failed to refresh access token")]
    RefreshAccessTokenFailed,
    /// Grpc stream unexpectedly closed
    #[error("GRPC stream unexpectedly closed")]
    StreamClosed,
    /// Malformed request
    #[error("Malformed request")]
    MalformedRequest,
    /// Auth provider error
    #[error("auth provider")]
    AuthProvider(#[source] E),
    /// Failure during refresh token request
    #[error("Refresh token request failed with status {0}")]
    RefreshTokenRequest(tonic::Status),
    /// Failure during authentication request
    #[error("Authenticate request failed with status {0}")]
    AuthenticateRequest(tonic::Status),
    /// Transport error
    #[error("transport")]
    Transport(#[from] transport::Error),
}

async fn refresh_tokens<A>(channel: Channel, bearer_token: &str) -> Result<RefreshedTokens, Error<A>>
where
    A: std::error::Error,
{
    let mut account_client = AccountServiceClient::with_interceptor(channel, |mut req: tonic::Request<()>| {
        req.metadata_mut().insert(
            "authorization",
            MetadataValue::from_str(&format!("Bearer {bearer_token}")).expect("JWT bearer token"),
        );

        Ok(req)
    });

    let tokens = account_client
        .refresh_token(())
        .await
        .map_err(Error::RefreshTokenRequest)?
        .into_inner();

    Ok(RefreshedTokens {
        bearer_token: tokens.bearer_token,
        access_token: tokens.access_token,
    })
}

async fn authenticate<A>(channel: Channel, credentials: Credentials) -> Result<RefreshedTokens, Error<A>>
where
    A: std::error::Error,
{
    let mut account_client = AccountServiceClient::new(channel);

    let (request_tx, request_rx) = mpsc::channel(1);
    let (challenge_tx, mut challenge_rx) = mpsc::channel::<String>(1);

    let request = ReceiverStream::new(request_rx);

    tokio::spawn(async move {
        let _ = request_tx
            .send(AuthenticateRequest {
                body: Some(authenticate_request::Body::Credentials(GrpcCredentials {
                    username: credentials.username,
                    public_key: credentials.key_pair.public_key().encode().to_string(),
                })),
            })
            .await;

        let Some(challenge) = challenge_rx.recv().await else {
            return;
        };

        let signature =
            base64::prelude::BASE64_URL_SAFE_NO_PAD.encode(credentials.key_pair.sign(challenge.as_bytes()).to_bytes());

        let _ = request_tx
            .send(AuthenticateRequest {
                body: Some(authenticate_request::Body::Signature(signature)),
            })
            .await;
    });

    let mut resp = account_client
        .authenticate(request)
        .await
        .map_err(Error::AuthenticateRequest)?
        .into_inner();

    let Some(authenticate_response::Body::Challenge(challenge)) = resp
        .next()
        .await
        .ok_or(Error::StreamClosed)?
        .map_err(Error::AuthenticateRequest)?
        .body
    else {
        return Err(Error::MalformedRequest);
    };

    let _ = challenge_tx.send(challenge).await;

    let Some(authenticate_response::Body::Tokens(tokens)) = resp
        .next()
        .await
        .ok_or(Error::StreamClosed)?
        .map_err(Error::AuthenticateRequest)?
        .body
    else {
        return Err(Error::MalformedRequest);
    };

    Ok(RefreshedTokens {
        bearer_token: tokens.bearer_token,
        access_token: tokens.access_token,
    })
}

pub struct TokenInterceptor {
    token: String,
}

impl Interceptor for TokenInterceptor {
    fn call(&mut self, mut req: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
        if let Ok(value) = MetadataValue::from_str(&format!("Bearer {}", self.token))
            .inspect_err(|err| error!(error = %err, "failed to convert token to header value"))
        {
            req.metadata_mut().insert("authorization", value);
        }

        Ok(req)
    }
}
