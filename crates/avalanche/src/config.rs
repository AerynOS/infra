use std::path::Path;

use color_eyre::eyre::{Context, Result};
use http::Uri;
use serde::Deserialize;
use service::{account::Admin, crypto::PublicKey, tracing};
use tokio::fs;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub admin: Admin,
    #[serde(default)]
    pub tracing: tracing::Config,
    #[serde(rename = "summit", alias = "hub", default)]
    pub upstreams: Vec<SummitConfig>,
}

impl Config {
    pub async fn load(path: impl AsRef<Path>) -> Result<Self> {
        let content = fs::read_to_string(path).await.context("read config")?;
        let config = toml::from_str(&content).context("deserialize config")?;
        Ok(config)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct SummitConfig {
    /// Host address of the upstream summit hub to connect to
    #[serde(with = "http_serde::uri")]
    pub host_address: Uri,
    /// Public key of the summit instance
    ///
    /// Avalanche will verify this public key during authentication
    pub public_key: PublicKey,
}
