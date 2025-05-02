//! Shared service configuration

use std::{io, path::Path};

use http::Uri;
use serde::Deserialize;
use tokio::fs;

use crate::{
    account::Admin,
    crypto::{KeyPair, PublicKey},
    endpoint::{
        Role,
        enrollment::{self, Issuer},
    },
    tracing,
};

/// Service configuration
#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    /// [`Uri`] this service's grpc is reachable from
    #[serde(with = "http_serde::uri")]
    pub grpc_address: Uri,
    /// Description of this service
    pub description: String,
    /// Admin details of this service
    pub admin: Admin,
    /// Tracing configuration
    #[serde(default)]
    pub tracing: tracing::Config,
    /// Upstream (hub) service to send enrollment to
    ///
    /// Only applicable for non-hub services
    pub upstream: Option<enrollment::Target>,
    /// Downstream services to auto-accept enrollment with
    ///
    /// Only applicable for hub service
    #[serde(default)]
    pub downstreams: Vec<PublicKey>,
}

impl Config {
    /// Load configuration from the provided `path`
    pub async fn load(path: impl AsRef<Path>) -> Result<Self, Error> {
        let content = fs::read_to_string(path).await?;
        let config = toml::from_str(&content)?;
        Ok(config)
    }
}

impl Config {
    /// Construct [`Issuer`] details based on this [`Config`] and
    /// the provided [`Role`] and [`KeyPair`]
    pub fn issuer(&self, role: Role, key_pair: KeyPair) -> Issuer {
        Issuer {
            key_pair,
            host_address: self.grpc_address.clone(),
            role,
            admin_name: self.admin.name.clone(),
            admin_email: self.admin.email.clone(),
            description: self.description.clone(),
        }
    }
}

/// A config error
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Loading the config failed
    #[error("load config")]
    Load(#[from] io::Error),
    /// Decoding the config failed
    #[error("decode config")]
    Decode(#[from] toml::de::Error),
}
