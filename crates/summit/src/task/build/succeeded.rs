use color_eyre::eyre::{Context, OptionExt, Result};
use service::{
    Endpoint, State,
    client::{AuthClient, EndpointAuth, VesselServiceClient},
    endpoint::{self, builder},
    grpc::{
        collectable::{self, Collectable},
        vessel::ImportRequest,
    },
};
use tracing::{error, info};

use super::stash_log;
use crate::task;

#[tracing::instrument(
    name = "build_succeeded",
    skip_all,
    fields(
        task = %task_id,
        builder = %builder,
    )
)]
pub async fn succeeded(
    state: &State,
    task_id: task::Id,
    builder: endpoint::Id,
    collectables: Vec<Collectable>,
) -> Result<bool> {
    let mut tx = state.service_db.begin().await.context("begin db tx")?;

    let mut endpoint = Endpoint::get(tx.as_mut(), builder).await.context("get endpoint")?;
    endpoint.set_work_status(builder::WorkStatus::Idle);
    endpoint.save(&mut tx).await.context("save endpoint status")?;

    let task = task::query(tx.as_mut(), task::query::Params::default().id(task_id))
        .await
        .context("get task")?
        .tasks
        .into_iter()
        .next()
        .ok_or_eyre("task is missing")?;

    let vessel = Endpoint::list(tx.as_mut())
        .await
        .context("list endpoints")?
        .into_iter()
        .find(|endpoint| {
            matches!(endpoint.status, endpoint::Status::Operational)
                && matches!(endpoint.kind, endpoint::Kind::RepositoryManager)
        })
        .ok_or_eyre("no operational vessel instance")?;

    tx.commit().await?;

    // Reject tasks that somehow already failed
    if matches!(task.status, task::Status::Failed) {
        error!("Blocking inclusion of previously failed build");
        return Ok(false);
    }

    let log = stash_log(state, task_id, &collectables)
        .await
        .inspect_err(|error| error!(%error,"Failed to download log file"))
        .ok()
        .flatten();

    let mut client = VesselServiceClient::connect_with_auth(
        vessel.host_address.clone(),
        EndpointAuth::new(&vessel, state.service_db.clone(), state.key_pair.clone()),
    )
    .await
    .context("connect vessel client")?;

    let result = client
        .import(ImportRequest {
            task_id: i64::from(task_id) as u64,
            collectables: collectables
                .into_iter()
                .filter(|c| matches!(c.kind(), collectable::Kind::Package))
                .collect(),
        })
        .await
        .context("send import request");

    let (status, failed) = match result {
        Ok(_) => {
            info!("Task sent for publishing");
            (task::Status::Publishing, false)
        }
        Err(error) => {
            error!(%error,"Request failed to import binaries");
            (task::Status::Failed, true)
        }
    };

    let mut tx = state.service_db.begin().await.context("begin db tx")?;

    task::set_status(&mut tx, task_id, status).await.context("set status")?;

    if let Some(log_path) = log {
        task::set_log_path(&mut tx, task_id, &log_path)
            .await
            .context("set log path")?;
    }

    tx.commit().await.context("commit tx")?;

    Ok(failed)
}
