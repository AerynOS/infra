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
    pub description: String,
    pub admin: Admin,
    #[serde(default)]
    pub tracing: tracing::Config,
    #[serde(rename = "hub", default)]
    pub hubs: Vec<enrollment::HubTarget>,
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
            // Client only, we have no address to connect back to
            host_address: Uri::from_static("avalanche://client.only"),
            role: Role::Builder,
            admin_name: self.admin.name.clone(),
            admin_email: self.admin.email.clone(),
            description: self.description.clone(),
        }
    }
}

#[cfg(test)]
mod test {
    use http::Uri;

    #[test]
    fn client_only_address() {
        Uri::from_static("avalanche://client.only");
    }
}
