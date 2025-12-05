use std::path::{Path, PathBuf};

use color_eyre::eyre::{Context, Result};
use moss::db::meta;

#[derive(Clone)]
pub struct State {
    pub meta_db: meta::Database,
    pub service: service::State,
}

impl State {
    pub async fn load(root: impl AsRef<Path>) -> Result<Self> {
        let service = service::State::load(root).await.context("load service state")?;

        let meta_db = meta::Database::new(service.db_dir.join("meta").to_string_lossy().as_ref())
            .context("failed to open meta database")?;

        Ok(Self { meta_db, service })
    }

    pub fn service_db(&self) -> &service::Database {
        &self.service.service_db
    }

    pub fn public_dir(&self) -> PathBuf {
        self.service.state_dir.join("public")
    }

    pub fn work_dir(&self) -> PathBuf {
        self.service.state_dir.join("work")
    }
}
