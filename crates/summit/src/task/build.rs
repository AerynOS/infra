use std::path::PathBuf;

use color_eyre::eyre::{Context, OptionExt, Result};
use futures_util::StreamExt;
use moss::request;
use service::client::{AuthClient, AvalancheServiceClient, EndpointAuth};
use service::grpc::collectable::{self, Collectable};
use service::grpc::{avalanche::BuildRequest, remote::Remote as ProtoRemote};
use service::{Endpoint, State, endpoint::builder};
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tracing::{info, warn};

use crate::task;

use super::{Id, Queued, Status};

pub use self::failed::failed;
pub use self::succeeded::succeeded;

pub mod failed;
pub mod succeeded;

#[tracing::instrument(
    name = "build_task",
    skip_all,
    fields(
        builder = %builder.id,
        task = %queued.task.id,
        build = %queued.task.build_id
    )
)]
pub async fn build(state: &State, builder: &mut Endpoint, queued: &Queued) -> Result<()> {
    let mut client = AvalancheServiceClient::connect_with_auth(
        builder.host_address.clone(),
        EndpointAuth::new(builder, state.service_db.clone(), state.key_pair.clone()),
    )
    .await
    .context("connect avalanche client")?;

    client
        .build(BuildRequest {
            task_id: i64::from(queued.task.id) as u64,
            uri: queued.origin_uri.to_string(),
            commit_ref: queued.commit_ref.clone(),
            relative_path: queued
                .meta
                .uri
                .as_ref()
                .ok_or_eyre("missing relative path on metadata")?
                .to_string(),
            build_architecture: queued.task.arch.clone(),
            remotes: queued
                .remotes
                .iter()
                .enumerate()
                .map(|(idx, uri)| ProtoRemote {
                    index_uri: uri.to_string(),
                    name: format!("repo{idx}"),
                    priority: idx as u64 * 10,
                })
                .collect(),
        })
        .await
        .context("send build request")?;

    let mut tx = state.service_db.begin().await.context("begin db tx")?;

    builder.set_work_status(builder::WorkStatus::Running);
    builder.save(&mut tx).await?;

    task::set_status(&mut tx, queued.task.id, Status::Building)
        .await
        .context("set status")?;
    task::set_allocated_builder(&mut tx, queued.task.id, &builder.id.to_string())
        .await
        .context("set builder")?;

    tx.commit().await.context("commit tx")?;

    info!("Task sent for build");

    Ok(())
}

async fn stash_log(state: &State, task_id: Id, collectables: &[Collectable]) -> Result<Option<PathBuf>> {
    let Some(log) = collectables.iter().find(|c| matches!(c.kind(), collectable::Kind::Log)) else {
        warn!("Missing log from builder");
        return Ok(None);
    };

    let uri = log.uri.parse::<http::Uri>().context("invalid log uri")?;
    let (_, name) = uri.path().rsplit_once("/").ok_or_eyre("missing log path in uri")?;

    let parent = state.state_dir.join("logs").join(task_id.to_string());
    let path = parent.join(name);
    let relative_path = path
        .strip_prefix(&state.state_dir)
        .expect("descendent of state dir")
        .to_owned();

    let _ = fs::create_dir_all(&parent).await;

    let mut file = fs::File::create(&path).await.context("create log file")?;

    let mut stream = request::get(uri.to_string().parse().expect("valid uri to url conversion"))
        .await
        .context("request log file")?;

    while let Some(result) = stream.next().await {
        let mut chunk = result.context("request log file")?;

        file.write_all_buf(&mut chunk).await.context("write bytes")?;
    }

    file.flush().await.context("flush")?;

    Ok(Some(relative_path))
}
