use color_eyre::eyre::{self, Context};
use http::Uri;
use moss::repository::format;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Index {
    #[serde(rename = "uri", with = "http_serde::uri")]
    DirectIndex(Uri),
    #[serde(untagged)]
    RootIndex(RootIndex),
}

impl Index {
    pub fn as_moss_repo_source(&self, arch: &str) -> eyre::Result<moss::repository::Source> {
        match self {
            Index::DirectIndex(uri) => Ok(moss::repository::Source::DirectIndex(
                uri.to_string().parse().context("invalid url")?,
            )),
            Index::RootIndex(root_index) => {
                Ok(moss::repository::Source::RootIndex(moss::repository::RootIndexSource {
                    base_uri: root_index.base_uri.to_string().parse().context("invalid url")?,
                    channel: root_index.channel.clone(),
                    version: root_index.version.clone(),
                    arch: arch.to_owned(),
                }))
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RootIndex {
    #[serde(with = "http_serde::uri")]
    pub base_uri: Uri,
    #[serde(default = "default_channel")]
    pub channel: format::Identifier,
    pub version: format::ScopedIdentifier,
}

fn default_channel() -> format::Identifier {
    moss::repository::DEFAULT_CHANNEL.try_into().expect("valid identifier")
}
