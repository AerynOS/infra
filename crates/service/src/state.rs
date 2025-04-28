//! Shared service state
use std::{io, path::PathBuf};

use thiserror::Error;
use tokio::fs;
use tracing::debug;

use crate::{
    Database,
    crypto::{self, KeyPair},
    database,
};

/// Service state
#[derive(Debug, Clone)]
pub struct State {
    /// Root directory
    pub root: PathBuf,
    /// State directory
    pub state_dir: PathBuf,
    /// Cache directory
    pub cache_dir: PathBuf,
    /// Database directory
    pub db_dir: PathBuf,
    /// Service database
    pub service_db: Database,
    /// Key pair used by the service
    pub key_pair: KeyPair,
}

impl State {
    /// Load state from the provided path. If no keypair and/or database exist, they will be created.
    #[tracing::instrument(name = "load_state", skip_all)]
    pub async fn load(root: impl Into<PathBuf>) -> Result<Self, Error> {
        let root = root.into();

        let state_dir = root.join("state");
        let cache_dir = state_dir.join("cache");
        let db_dir = state_dir.join("db");

        if !db_dir.exists() {
            fs::create_dir_all(&db_dir).await.map_err(Error::CreateDbDir)?;
        }

        let service_db_path = db_dir.join("service");
        let service_db = Database::new(&service_db_path).await?;
        debug!(path = ?service_db_path, "Database opened");

        let key_path = state_dir.join(".privkey");
        let key_pair = if !key_path.exists() {
            let key_pair = KeyPair::generate();
            debug!(key_pair = %key_pair.public_key(), "Keypair generated");

            fs::write(&key_path, &key_pair.to_bytes())
                .await
                .map_err(Error::SavePrivateKey)?;

            key_pair
        } else {
            let bytes = fs::read(&key_path).await.map_err(Error::LoadPrivateKey)?;

            let key_pair = KeyPair::try_from_bytes(&bytes).map_err(Error::DecodePrivateKey)?;
            debug!(key_pair = %key_pair.public_key(), "Keypair loaded");

            key_pair
        };

        Ok(Self {
            root,
            state_dir,
            cache_dir,
            db_dir,
            service_db,
            key_pair,
        })
    }

    /// Run the provided migrations against the service database
    pub async fn with_migrations(mut self, migrator: database::Migrator) -> Result<Self, Error> {
        self.service_db = self.service_db.with_migrations(migrator).await?;
        Ok(self)
    }
}

/// A state error
#[derive(Debug, Error)]
pub enum Error {
    /// Error creating db directory
    #[error("create db directory")]
    CreateDbDir(#[source] io::Error),
    /// Loading database failed
    #[error("load database")]
    LoadDatabase(#[from] database::Error),
    /// Saving private key failed
    #[error("save private key")]
    SavePrivateKey(#[source] io::Error),
    /// Loading private key failed
    #[error("load private key")]
    LoadPrivateKey(#[source] io::Error),
    /// Decoding private key failed
    #[error("decode private key")]
    DecodePrivateKey(#[source] crypto::Error),
}
