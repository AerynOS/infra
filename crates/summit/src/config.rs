use std::{iter, path::Path};

use color_eyre::eyre::{Context, Result};
use serde::{Deserialize, Serialize};
use service::{
    Service,
    account::Admin,
    auth::{AuthorizedService, AuthorizedServices},
    tracing,
};
use tokio::fs;

use crate::{builder, repository_manager};

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub admin: Admin,
    #[serde(default)]
    pub tracing: tracing::Config,
    #[serde(alias = "vessel")]
    pub repository_manager: repository_manager::Config,
    #[serde(rename = "builder", alias = "avalanche", default)]
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

    pub fn authorized_services(&self) -> AuthorizedServices {
        iter::once((
            self.repository_manager.public_key,
            AuthorizedService {
                id: self.repository_manager.id.clone(),
                service: Service::Vessel,
            },
        ))
        .chain(self.builders.iter().map(|config| {
            (
                config.public_key,
                AuthorizedService {
                    id: config.id.clone(),
                    service: Service::Avalanche,
                },
            )
        }))
        .collect()
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, strum::Display, strum::EnumString,
)]
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
