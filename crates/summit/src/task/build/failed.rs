use color_eyre::eyre::{Context, Result};
use service::{
    Endpoint, State,
    endpoint::{self, builder},
    grpc::collectable::Collectable,
};
use tracing::{error, info};

use super::stash_log;
use crate::task;

#[tracing::instrument(
    name = "build_failed",
    skip_all,
    fields(
        task = %task_id,
        builder = %builder,
    )
)]
pub async fn failed(
    state: &State,
    task_id: task::Id,
    builder: endpoint::Id,
    collectables: Vec<Collectable>,
) -> Result<()> {
    let log = stash_log(state, task_id, &collectables)
        .await
        .inspect_err(|error| error!(%error,"Failed to download log file"))
        .ok()
        .flatten();

    let mut tx = state.service_db.begin().await.context("begin db tx")?;

    let mut endpoint = Endpoint::get(tx.as_mut(), builder).await.context("get endpoint")?;
    endpoint.set_work_status(builder::WorkStatus::Idle);
    endpoint.save(&mut tx).await.context("save endpoint status")?;

    task::set_status(&mut tx, task_id, task::Status::Failed)
        .await
        .context("set status")?;

    if let Some(log_path) = log {
        task::set_log_path(&mut tx, task_id, &log_path)
            .await
            .context("set log path")?;
    }

    tx.commit().await.context("commit tx")?;

    info!("Task marked as failed");

    Ok(())
}
