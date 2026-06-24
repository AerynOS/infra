//! Account primitives
use std::str::FromStr;

use derive_more::{AsRef, Display, From, Into};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Unique identifier of an [`Account`]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, From, Into, Display, AsRef)]
pub struct Id(Uuid);

impl Id {
    /// Generate a new [`Id`]
    pub fn generate() -> Self {
        Self(Uuid::new_v4())
    }
}

impl FromStr for Id {
    type Err = uuid::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value.parse::<Uuid>().map(Id)
    }
}

impl<'a> TryFrom<&'a str> for Id {
    type Error = uuid::Error;

    fn try_from(value: &'a str) -> Result<Self, Self::Error> {
        value.parse::<Uuid>().map(Id)
    }
}

impl From<Id> for String {
    fn from(id: Id) -> Self {
        id.to_string()
    }
}

/// Type of account
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, strum::Display, strum::EnumString)]
#[serde(rename_all = "kebab-case")]
#[strum(serialize_all = "kebab-case")]
pub enum Kind {
    /// Standard account
    Standard,
    /// Bot account
    Bot,
    /// Admin account
    Admin,
}
