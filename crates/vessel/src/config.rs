use std::path::Path;

use color_eyre::eyre::{Context, Result};
use http::Uri;
use serde::Deserialize;
use service::{
    account::Admin,
    crypto::KeyPair,
    endpoint::{
        Role,
        enrollment::{self, Issuer},
    },
    tracing,
};
use tokio::fs;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    #[serde(with = "http_serde::uri")]
    pub grpc_address: Uri,
    pub description: String,
    pub admin: Admin,
    #[serde(default)]
    pub tracing: tracing::Config,
    pub upstream: enrollment::Target,
}

impl Config {
    pub async fn load(path: impl AsRef<Path>) -> Result<Self> {
        let content = fs::read_to_string(path).await.context("read config")?;
        let config = toml::from_str(&content).context("deserialize config")?;
        Ok(config)
    }

    pub fn issuer(&self, key_pair: KeyPair) -> Issuer {
        Issuer {
            key_pair,
            host_address: self.grpc_address.clone(),
            role: Role::RepositoryManager,
            admin_name: self.admin.name.clone(),
            admin_email: self.admin.email.clone(),
            description: self.description.clone(),
        }
    }
}
