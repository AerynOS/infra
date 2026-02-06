use std::{path::Path, sync::Arc};

use arc_swap::ArcSwap;
use color_eyre::eyre::{Context, Result};
use tokio::sync::broadcast;

use crate::{builder, queue, route};

pub struct State {
    pub service: service::State,
    /// Cached view of builders, used to render it on the frontend
    pub builders: ArcSwap<Vec<builder::Info>>,
    /// Cached json view of the queue, used to render it on the frontend
    pub queue_json_view: ArcSwap<queue::JsonView>,
    /// Events that get forwarded by SSE connected streams
    pub sse_receiver: broadcast::Receiver<route::SseEvent>,
}

impl State {
    pub async fn load(
        root: impl AsRef<Path>,
        sse_receiver: broadcast::Receiver<route::SseEvent>,
    ) -> Result<Arc<State>> {
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
            sse_receiver,
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
