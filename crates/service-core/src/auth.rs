//! Authentication
use std::collections::HashSet;

use bitflags::bitflags;
use serde::{Deserialize, Serialize};

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
        /// Service account type
        const SERVICE_ACCOUNT = 1 << 2;
        /// Bot account type
        const BOT_ACCOUNT = 1 << 3;
        /// User account type
        const USER_ACCOUNT = 1 << 4;
        /// Admin account type
        const ADMIN_ACCOUNT = 1 << 5;
        /// Token is expired
        const EXPIRED = 1 << 6;
        /// Token is not expired
        const NOT_EXPIRED = 1 << 7;
    }
}

/// Convert [`Flags`] to an array of flag names
pub fn flag_names(flags: Flags) -> Vec<String> {
    flags.iter_names().map(|(name, _)| name.to_string()).collect()
}

/// RBAC role
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Role {
    Admin,
    Hub,
    RepositoryManager,
    Builder,
}

impl Role {
    /// Permissions granted to the role
    pub fn permissions(&self) -> HashSet<Permission> {
        use Permission::*;

        match self {
            Role::Admin => [RetryTask, FailTask, Refresh].into_iter().collect(),
            Role::Hub => [RequestUploadToken].into_iter().collect(),
            Role::RepositoryManager => [ReportImportStatus].into_iter().collect(),
            Role::Builder => [ConnectBuilderStream].into_iter().collect(),
        }
    }
}

/// RBAC permission
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, strum::Display)]
#[serde(rename_all = "kebab-case")]
#[strum(serialize_all = "kebab-case")]
pub enum Permission {
    /// Request a one time vessel upload token
    RequestUploadToken,
    /// Upload a package to vessel
    UploadPackage,
    /// Report import status to summit
    ReportImportStatus,
    /// Connect to summit as a builder
    ConnectBuilderStream,
    /// Retry a task
    RetryTask,
    /// Fail a task
    FailTask,
    /// Force refresh all projects
    Refresh,
}
