use serde::{Deserialize, Serialize};

/// Type of account
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, strum::Display, strum::EnumString)]
#[serde(rename_all = "kebab-case")]
#[strum(serialize_all = "kebab-case")]
pub enum Kind {
    /// Standard account
    Standard,
    /// Bot account
    Bot,
    /// Service account (endpoint)
    Service,
    /// Admin account
    Admin,
}
