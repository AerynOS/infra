use std::{path::Path, sync::Arc};

use arc_swap::ArcSwap;
use color_eyre::eyre::{Context, Result};

use crate::{builder, queue};

pub struct State {
    pub service: service::State,
    /// Cached view of builders, used to render it on the frontend
    pub builders: ArcSwap<Vec<builder::Info>>,
    /// Cached json view of the queue, used to render it on the frontend
    pub queue_json_view: ArcSwap<queue::JsonView>,
}

impl State {
    pub async fn load(root: impl AsRef<Path>) -> Result<Arc<State>> {
        let service = service::State::load(root).await.context("load service state")?;

        service
            .service_db
            .migrate(sqlx::migrate!("./migrations"))
            .await
            .context("apply database migrations")?;

        Ok(Arc::new(Self {
            service,
            builders: Default::default(),
            queue_json_view: Default::default(),
        }))
    }

    pub fn service_db(&self) -> &service::Database {
        &self.service.service_db
    }

    pub fn cache_dir(&self) -> &Path {
        &self.service.cache_dir
    }

    pub fn state_dir(&self) -> &Path {
        &self.service.state_dir
    }
}
