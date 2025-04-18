//! Json Web Token (JWT)
use std::time::SystemTime;

use chrono::{DateTime, Duration, Utc};
use jsonwebtoken::{DecodingKey, EncodingKey, Header};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::{
    account,
    crypto::{self, KeyPair, PublicKey},
};

/// A decoded Json Web Token (JWT)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Token {
    header: Header,
    /// Payload of the token
    pub payload: Payload,
}

impl Token {
    /// Creates a new token from the provided [`Payload`]
    pub fn new(payload: Payload) -> Self {
        Self {
            header: Header::new(jsonwebtoken::Algorithm::EdDSA),
            payload,
        }
    }

    /// Verify and return a decoded token
    pub fn verify(token: &str, public_key: &PublicKey, validation: &Validation) -> Result<VerifiedToken, Error> {
        let decoded = jsonwebtoken::decode::<Payload>(
            token,
            // This actually takes the compressed bytes and not
            // the der encoded pkcs#8 format bytes, such as
            // on the sign / encoding side. Fails otherwise.
            &DecodingKey::from_ed_der(public_key.as_ref()),
            &validation.0,
        )
        .map_err(Error::decode)?;

        Ok(VerifiedToken {
            encoded: token.to_string(),
            decoded: Token {
                header: decoded.header,
                payload: decoded.claims,
            },
        })
    }

    /// Sign and return an encoded token
    pub fn sign(&self, key_pair: &KeyPair) -> Result<String, Error> {
        jsonwebtoken::encode(
            &self.header,
            &self.payload,
            &EncodingKey::from_ed_der(key_pair.der()?.as_bytes()),
        )
        .map_err(Error::SignToken)
    }

    /// Returns true if the token is expired from [`SystemTime::now`]
    pub fn is_expired(&self) -> bool {
        let start = SystemTime::now();
        let now = start
            .duration_since(std::time::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs();

        self.payload.exp as u64 <= now
    }

    /// Returns true if the token is expired in [`Duration`] from now
    pub fn is_expired_in(&self, duration: std::time::Duration) -> bool {
        let start = SystemTime::now();
        let now = (start
            .duration_since(std::time::UNIX_EPOCH)
            .expect("Time went backwards")
            + duration)
            .as_secs();

        self.payload.exp as u64 <= now
    }

    /// Refresh this token with a new expiration & issue time
    pub fn refresh(&self) -> Self {
        let now = Utc::now();
        let expires_on = now + self.payload.purpose.duration();

        Self {
            payload: Payload {
                exp: expires_on.timestamp(),
                iat: now.timestamp(),
                ..self.payload.clone()
            },
            ..self.clone()
        }
    }

    /// Change the purpose of this token
    pub fn with_purpose(self, purpose: Purpose) -> Self {
        Self {
            header: self.header,
            payload: Payload {
                purpose,
                ..self.payload
            },
        }
    }
}

/// A token that's been verified via [`Token::verify`]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedToken {
    /// Encoded token string
    pub encoded: String,
    /// Decoded token
    pub decoded: Token,
}

impl VerifiedToken {
    /// Returns the datetime the token expires
    pub fn expires(&self) -> DateTime<Utc> {
        DateTime::from_timestamp(self.decoded.payload.exp, 0).unwrap_or(DateTime::UNIX_EPOCH)
    }
}

/// Validation rules to use when running [`Token::verify`]
#[derive(Debug, Clone)]
pub struct Validation(jsonwebtoken::Validation);

impl Default for Validation {
    fn default() -> Self {
        let mut validation = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::EdDSA);
        // Expiration is evaluated in the authentication layer
        validation.validate_exp = false;
        validation.validate_aud = false;
        validation.required_spec_claims = ["aud", "exp", "iss", "sub"].into_iter().map(String::from).collect();

        Self(validation)
    }
}

impl Validation {
    /// Create a default validation that verifies signature
    pub fn new() -> Self {
        Self::default()
    }

    /// Validation will check that the `aud` field is is equal to
    /// the provided value
    pub fn aud(mut self, aud: impl ToString) -> Self {
        self.0.validate_aud = true;
        self.0.aud = Some([aud.to_string()].into_iter().collect());
        self
    }

    /// Validation will check that the `iss` field is is equal to
    /// the provided value
    pub fn iss(mut self, iss: impl ToString) -> Self {
        self.0.iss = Some([iss.to_string()].into_iter().collect());
        self
    }

    /// Validation will check that the `sub` field is is equal to
    /// the provided value
    #[allow(clippy::should_implement_trait)]
    pub fn sub(mut self, sub: impl ToString) -> Self {
        self.0.sub = Some(sub.to_string());
        self
    }
}

/// Payload of a [`Token`] which defines various claims
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Payload {
    /// Audience - Recipient for which the JWT is intended
    pub aud: String,
    /// Expiration - Time after which the JWT expires
    pub exp: i64,
    /// Issued at - Time at which the JWT was issued; can be used to determine age of the JWT
    pub iat: i64,
    /// Issuer - Issuer of the JWT
    pub iss: String,
    /// Subject - Subject of the JWT (the user)
    pub sub: String,
    /// Token purpose
    pub purpose: Purpose,
    /// Account id of the holder
    #[serde(rename = "uid")]
    pub account_id: Uuid,
    /// Account type of the holder
    #[serde(rename = "act")]
    pub account_type: account::Kind,
}

/// Purpose of the token
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, strum::Display)]
#[strum(serialize_all = "lowercase")]
pub enum Purpose {
    /// Bearer
    #[serde(rename = "authorize")]
    Authorization,
    /// Access
    #[serde(rename = "authenticate")]
    Authentication,
}

impl Purpose {
    /// Duration used for the expiration of a token with this purpose
    pub fn duration(&self) -> Duration {
        match self {
            Purpose::Authorization => Duration::days(7),
            Purpose::Authentication => Duration::hours(1),
        }
    }
}

/// A token error
#[derive(Debug, Error)]
pub enum Error {
    /// Token signature invalid
    #[error("Invalid signature")]
    InvalidSignature,
    /// Decoding token failed
    #[error("decode token")]
    DecodeToken(#[source] jsonwebtoken::errors::Error),
    /// Signing token failed
    #[error("sign token")]
    SignToken(#[source] jsonwebtoken::errors::Error),
    /// A crypto error
    #[error(transparent)]
    Crypto(#[from] crypto::Error),
}

impl Error {
    fn decode(error: jsonwebtoken::errors::Error) -> Self {
        match error.kind() {
            jsonwebtoken::errors::ErrorKind::InvalidSignature => Self::InvalidSignature,
            _ => Self::decode(error),
        }
    }
}

#[cfg(test)]
mod test {
    use chrono::{Duration, Utc};
    use jsonwebtoken::Algorithm;

    use super::*;

    #[test]
    fn roundtrip() {
        let keypair = KeyPair::generate();

        let now = Utc::now();
        let one_hour = now + Duration::seconds(60 * 60);

        let token = Token {
            header: Header::new(Algorithm::EdDSA),
            payload: Payload {
                aud: "test".into(),
                exp: one_hour.timestamp(),
                iat: now.timestamp(),
                iss: "test".into(),
                sub: "test".into(),
                purpose: Purpose::Authorization,
                account_id: Uuid::new_v4(),
                account_type: account::Kind::Admin,
            },
        };

        // Round trip
        let encoded = token.sign(&keypair).unwrap();
        let verified = Token::verify(&encoded, &keypair.public_key(), &Validation::new()).unwrap();

        assert_eq!(token, verified.decoded);
    }
}
