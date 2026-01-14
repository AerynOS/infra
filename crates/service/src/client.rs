//! Make requests to service APIs

use async_trait::async_trait;
use thiserror::Error;
use tracing::{error, info};

use crate::crypto::KeyPair;
use crate::{Account, Database, Endpoint, Token, account, crypto, database, endpoint, token};

pub use service_client::*;

/// Auth credentials are stored in [`Database`] for a configured endpoint
/// and updated when refresh tokens are fetched
#[derive(Debug, Clone)]
pub struct EndpointAuth {
    endpoint: endpoint::Id,
    remote_account: account::Id,
    db: Database,
    key_pair: KeyPair,
}

impl EndpointAuth {
    /// Create a new [`EndpointAuth`] for the provided [`Endpoint`]
    pub fn new(endpoint: &Endpoint, db: Database, key_pair: KeyPair) -> Self {
        Self {
            endpoint: endpoint.id,
            remote_account: endpoint.remote_account,
            db,
            key_pair,
        }
    }
}

#[async_trait]
impl AuthProvider for EndpointAuth {
    type Error = EndpointAuthError;

    const REFRESH_ENABLED: bool = true;

    fn credentials(&self) -> Option<Credentials> {
        Some(Credentials {
            username: format!("@{}", self.remote_account),
            key_pair: self.key_pair.clone(),
        })
    }

    async fn tokens(&self) -> Result<VerifiedTokens, EndpointAuthError> {
        let mut conn = self.db.acquire().await?;

        let endpoint = Endpoint::get(conn.as_mut(), self.endpoint).await?;
        let account = Account::get(conn.as_mut(), endpoint.account).await?;
        let public_key = account.public_key.decoded()?;

        let tokens = endpoint::Tokens::get(conn.as_mut(), self.endpoint).await?;

        Ok(VerifiedTokens {
            bearer_token: tokens
                .bearer_token
                .as_deref()
                .map(|token| Token::verify(token, &public_key, &token::Validation::new()))
                .transpose()?,
            access_token: tokens
                .access_token
                .as_deref()
                .map(|token| Token::verify(token, &public_key, &token::Validation::new()))
                .transpose()?,
        })
    }

    #[tracing::instrument(
        skip_all,
        fields(
            endpoint = %self.endpoint,
        )
    )]
    async fn tokens_refreshed(&self, tokens: &RefreshedTokens) -> Result<(), Self::Error> {
        let mut tx = self.db.begin().await?;

        let mut endpoint = Endpoint::get(tx.as_mut(), self.endpoint).await?;
        let account = Account::get(tx.as_mut(), endpoint.account).await?;

        let public_key = account.public_key.decoded()?;

        let bearer_token = Token::verify(&tokens.bearer_token, &public_key, &token::Validation::new());
        let access_token = Token::verify(&tokens.access_token, &public_key, &token::Validation::new());

        match (bearer_token, access_token) {
            (Ok(bearer_token), Ok(access_token)) => {
                endpoint.status = endpoint::Status::Operational;
                endpoint.error = None;

                endpoint::Tokens {
                    bearer_token: Some(bearer_token.encoded),
                    access_token: Some(access_token.encoded),
                }
                .save(&mut tx, self.endpoint)
                .await?;
                endpoint.save(&mut tx).await?;

                tx.commit().await?;

                info!("Token refreshed, endpoint operational");

                Ok(())
            }
            (Err(token::Error::InvalidSignature), Err(_)) => {
                endpoint.status = endpoint::Status::Forbidden;
                endpoint.error = Some("Invalid signature".to_owned());

                error!("Invalid signature");

                endpoint::Tokens {
                    bearer_token: None,
                    access_token: None,
                }
                .save(&mut tx, self.endpoint)
                .await?;
                endpoint.save(&mut tx).await?;

                tx.commit().await?;

                Err(EndpointAuthError::InvalidToken)
            }
            _ => {
                endpoint.status = endpoint::Status::Forbidden;
                endpoint.error = Some("Invalid token".to_owned());

                error!("Invalid token");

                endpoint::Tokens {
                    bearer_token: None,
                    access_token: None,
                }
                .save(&mut tx, self.endpoint)
                .await?;
                endpoint.save(&mut tx).await?;

                tx.commit().await?;

                Err(EndpointAuthError::InvalidToken)
            }
        }
    }

    #[tracing::instrument(
        skip_all,
        fields(
            endpoint = %self.endpoint,
        )
    )]
    async fn token_refresh_failed(&self, error: &Error<EndpointAuthError>) -> Result<(), Self::Error> {
        let mut tx = self.db.begin().await?;

        let mut endpoint = Endpoint::get(tx.as_mut(), self.endpoint).await?;

        endpoint.status = endpoint::Status::Unreachable;
        endpoint.error = Some("Failed to refresh tokens".to_owned());
        error!(%error, "Failed to refresh tokens");

        endpoint::Tokens {
            bearer_token: None,
            access_token: None,
        }
        .save(&mut tx, self.endpoint)
        .await?;
        endpoint.save(&mut tx).await?;

        tx.commit().await?;

        Ok(())
    }
}

/// An endpoint auth provider error
#[derive(Debug, Error)]
pub enum EndpointAuthError {
    /// Invalid token
    #[error("Invalid token")]
    InvalidToken,
    /// Account error
    #[error("account")]
    Account(#[from] account::Error),
    /// Crypto error
    #[error("crypto")]
    Crypto(#[from] crypto::Error),
    /// Database error
    #[error("database")]
    Database(#[from] database::Error),
    /// Error decoding token
    #[error("decode token")]
    DecodeToken(#[from] token::Error),
}
