use std::{iter, path::Path};

use color_eyre::eyre::{Context, Result};
use http::Uri;
use serde::{Deserialize, Serialize};
use service::{
    account::Admin,
    crypto::{KeyPair, PublicKey},
    endpoint::{self, enrollment::Issuer},
    tracing,
};
use tokio::fs;

use crate::builder;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    #[serde(with = "http_serde::uri")]
    pub grpc_address: Uri,
    pub description: String,
    pub admin: Admin,
    #[serde(default)]
    pub tracing: tracing::Config,
    pub repository_manager: RepositoryManagerConfig,
    #[serde(rename = "builder", default)]
    pub builders: Vec<builder::Config>,
    #[serde(default)]
    pub build_sizes: BuildSizesConfig,
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
            role: endpoint::Role::Hub,
            admin_name: self.admin.name.clone(),
            admin_email: self.admin.email.clone(),
            description: self.description.clone(),
        }
    }

    pub fn downstreams(&self) -> Vec<PublicKey> {
        iter::once(self.repository_manager.public_key)
            .chain(self.builders.iter().map(|config| config.public_key))
            .collect()
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct RepositoryManagerConfig {
    pub public_key: PublicKey,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, strum::Display)]
#[serde(rename_all = "kebab-case")]
#[strum(serialize_all = "kebab-case")]
pub enum Size {
    Small,
    Medium,
    Large,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct BuildSizesConfig {
    #[serde(default)]
    large: Vec<String>,
    #[serde(default)]
    medium: Vec<String>,
}

impl BuildSizesConfig {
    pub fn get(&self, source_id: &String) -> Size {
        if self.large.contains(source_id) {
            Size::Large
        } else if self.medium.contains(source_id) {
            Size::Medium
        } else {
            Size::Small
        }
    }
}
