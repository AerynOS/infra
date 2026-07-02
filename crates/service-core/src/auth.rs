//! Authentication primitives
use std::collections::{HashMap, HashSet};

use bitflags::bitflags;
use serde::{Deserialize, Serialize};

use crate::{Service, crypto::PublicKey};

bitflags! {
    /// Authorization flags that describe the account making the request
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
    pub struct Flags : u16 {
        /// Missing or invalid token
        const NO_AUTH = 0;
        /// Bearer token purpose
        const BEARER_TOKEN = 1 << 0;
        /// Access token purpose
        const ACCESS_TOKEN = 1 << 1;
        /// Token is expired
        const EXPIRED = 1 << 2;
        /// Token is not expired
        const NOT_EXPIRED = 1 << 3;
        /// Valid session
        const VALID_SESSION = 1 << 4;

        /// A valid session w/ non-expired access token
        const SESSION_ACCESS =
            Self::ACCESS_TOKEN.bits()
            | Self::NOT_EXPIRED.bits()
            | Self::VALID_SESSION.bits();

        /// A valid session w/ non-expired refresh / bearer token
        const SESSION_REFRESH =
            Self::BEARER_TOKEN.bits()
            | Self::NOT_EXPIRED.bits()
            | Self::VALID_SESSION.bits();
    }
}

/// Convert [`Flags`] to an array of flag names
pub fn flag_names(flags: Flags) -> Vec<String> {
    flags.iter_names().map(|(name, _)| name.to_owned()).collect()
}

/// RBAC role
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Role {
    /// Admin role
    Admin,
    /// Hub role
    Hub,
    /// Repository Manager role
    RepositoryManager,
    /// Builder role
    Builder,
}

impl Role {
    /// Permissions granted to the role
    pub fn permissions(&self) -> HashSet<Permission> {
        use Permission::*;

        match self {
            Role::Admin => [
                RetryTask,
                CancelTask,
                Refresh,
                Pause,
                Resume,
                UpdateStream,
                AddTag,
                RemoveTag,
                UpgradeFormat,
            ]
            .into_iter()
            .collect(),
            Role::Hub => [].into_iter().collect(),
            Role::RepositoryManager => [ConnectRepositoryManagerStream].into_iter().collect(),
            Role::Builder => [ConnectBuilderStream].into_iter().collect(),
        }
    }
}

/// RBAC permission
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, strum::Display)]
#[serde(rename_all = "kebab-case")]
#[strum(serialize_all = "kebab-case")]
pub enum Permission {
    /// Connect to summit as a builder
    ConnectBuilderStream,
    /// Connect to summit as a repository manager
    ConnectRepositoryManagerStream,
    /// Upload a package to vessel
    UploadPackage,
    /// Retry a task
    RetryTask,
    /// Cancel a task
    CancelTask,
    /// Force refresh all projects
    Refresh,
    /// Pause updating all projects
    Pause,
    /// Resume updating all projects
    Resume,
    /// Update a channel stream
    UpdateStream,
    /// Add a channel tag
    AddTag,
    /// Remove a channel tag
    RemoveTag,
    /// Upgrade repository format
    UpgradeFormat,
}

/// A unique service which is allowed to authorize
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AuthorizedService {
    /// Unique identifier of service
    pub id: String,
    /// Service
    pub service: Service,
}

/// A map of services allowed to authorize by public key
///
/// [`PublicKey`] must be unique for each [`AuthorizedService`].
///
/// If the same [`PublicKey`] is added, it will overwrite any
/// previous service entry.
pub type AuthorizedServices = HashMap<PublicKey, AuthorizedService>;

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_required_flags() {
        const REQUIRED_FLAGS: Flags = Flags::SESSION_ACCESS;

        let valid = [
            Flags::NOT_EXPIRED | Flags::ACCESS_TOKEN | Flags::VALID_SESSION,
            Flags::all(),
        ];

        let not_valid = [
            Flags::empty(),
            Flags::NO_AUTH,
            Flags::SESSION_REFRESH,
            Flags::all() & !Flags::VALID_SESSION,
        ];

        for actual in valid {
            assert!(actual.contains(REQUIRED_FLAGS));
        }

        for actual in not_valid {
            assert!(!actual.contains(REQUIRED_FLAGS));
        }
    }
}
