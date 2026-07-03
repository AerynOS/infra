//! Session management

use std::{
    collections::HashMap,
    sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard},
    time::SystemTime,
};

use chrono::{DateTime, Utc};
use derive_more::{AsRef, Display, From, Into};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{Service, account, auth, crypto::PublicKey};

/// Unique [`Session`] identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, From, Into, Display, AsRef)]
pub struct Id(Uuid);

impl Id {
    /// Generate a new, random session id
    pub fn generate() -> Id {
        Id(Uuid::new_v4())
    }
}

/// A session
#[derive(Debug, Clone)]
pub struct Session {
    /// Unique session identifier
    pub id: Id,
    /// The session client
    pub client: Client,
    /// When this session expires
    ///
    /// The lifetime of a session is inherently tied
    /// to the expiration of the refresh token issued
    /// for this session.
    ///
    /// Expired sessions should never hit any API method
    /// since we enforce token expiration validation,
    /// except for authentication routes.
    ///
    /// This is exists so we can actively prune expired
    /// sessions from memory.
    pub expires: DateTime<Utc>,
}

impl Session {
    /// Returns true if the session is expired from [`SystemTime::now`]
    pub fn is_expired(&self) -> bool {
        let start = SystemTime::now();
        let now = start
            .duration_since(std::time::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs();

        self.expires.timestamp() as u64 <= now
    }
}

/// The client of a [`Session`]
#[derive(Debug, Clone, PartialEq, Eq, strum::Display)]
pub enum Client {
    /// Account client
    #[strum(serialize = "account(id={account_id}, kind={account_kind}, public_key={public_key})")]
    Account {
        /// Account id
        account_id: account::Id,
        /// Account kind
        account_kind: account::Kind,
        /// Public key of account used for this session
        public_key: PublicKey,
    },
    /// Service client
    #[strum(serialize = "service(id={service_id}, service={service}, public_key={public_key})")]
    Service {
        /// Service id
        service_id: String,
        /// Service
        service: Service,
        /// Public key of service
        public_key: PublicKey,
    },
}

impl Client {
    /// The client's [`PublicKey`] for this session
    pub fn public_key(&self) -> PublicKey {
        match self {
            Client::Account { public_key, .. } => *public_key,
            Client::Service { public_key, .. } => *public_key,
        }
    }

    /// The client's [`Role`], if any
    pub fn role(&self) -> Option<auth::Role> {
        match self {
            Client::Account { account_kind, .. } => {
                matches!(account_kind, account::Kind::Admin).then_some(auth::Role::Admin)
            }
            Client::Service { service, .. } => Some(match service {
                Service::Summit => auth::Role::Hub,
                Service::Avalanche => auth::Role::Builder,
                Service::Vessel => auth::Role::RepositoryManager,
            }),
        }
    }
}

/// A map of active sessions
#[derive(Debug, Clone, Default)]
pub struct ActiveSessions(Arc<RwLock<HashMap<Id, Session>>>);

impl ActiveSessions {
    fn read(&self) -> RwLockReadGuard<'_, HashMap<Id, Session>> {
        self.0.read().unwrap_or_else(|err| err.into_inner())
    }

    fn write(&self) -> RwLockWriteGuard<'_, HashMap<Id, Session>> {
        self.0.write().unwrap_or_else(|err| err.into_inner())
    }

    /// Returns the related [`ActiveSession`] if it is still active
    pub fn get(&self, id: &Id) -> Option<Session> {
        self.read().get(id).cloned()
    }

    /// Adds a new [`Session`]
    pub fn add(&self, session: Session) {
        self.write().insert(session.id, session);
    }

    /// Revokes an active session
    pub fn revoke(&self, id: &Id) {
        self.write().remove(id);
    }

    /// Extends an active session
    pub fn extend(&self, id: &Id, expires: DateTime<Utc>) {
        if let Some(session) = self.write().get_mut(id) {
            session.expires = expires;
        }
    }
}
