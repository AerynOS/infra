use std::time::Duration;

use chrono::Utc;
use color_eyre::eyre::{Context, Result};
use tokio::fs;
use tracing::info;

use crate::{
    Package, State,
    channel::{self, db},
    package,
};

#[tracing::instrument(skip_all, fields(%channel))]
pub async fn prune(state: &State, channel: &str) -> Result<()> {
    info!("Prune started");

    prune_stale_versions(state, channel).await?;
    prune_orphaned_packages(state, channel).await?;

    info!("Prune finished");

    Ok(())
}

#[tracing::instrument(skip_all)]
async fn prune_stale_versions(state: &State, channel: &str) -> Result<()> {
    // TODO: Configurable
    const STALE_AFTER: Duration = Duration::from_secs(60 * 60 * 24 * 14);

    let created_before = Utc::now() - chrono::Duration::from_std(STALE_AFTER).expect("within i64");

    info!(%created_before, "Checking for stale history");

    let stale_history = db::list_stale_history(
        state.service_db().acquire().await.context("acquire db conn")?.as_mut(),
        channel,
        created_before,
    )
    .await
    .context("list stale history")?;

    if stale_history.is_empty() {
        info!("No stale history");
        return Ok(());
    }

    for history in &stale_history {
        // Only history versions are returned from list stale history
        let channel::Version::History { identifier } = &history.version else {
            continue;
        };

        // Remove from filesystem
        //
        // Ignore error since DB operation can't be atomic w/ FS operation
        // and the committed DB operation is the source of truth. If a DB commit
        // fails _after_ the FS operation, we can safely retry and the
        // folder will already be deleted, which is fine.
        let _ = fs::remove_dir_all(state.public_dir().join(channel).join(identifier.relative_base_dir())).await;

        // Remove from DB
        //
        // Commits the removal of this history from state
        {
            let mut tx = state.service_db().begin().await.context("begin db tx")?;

            db::delete_history(&mut tx, channel, history)
                .await
                .context("delete db history")?;

            tx.commit().await.context("commit db tx")?;
        }

        info!(version = %history.version, "History deleted");
    }

    info!("num_deleted" = stale_history.len(), "Stale history deleted");

    Ok(())
}

#[tracing::instrument(skip_all)]
async fn prune_orphaned_packages(state: &State, channel: &str) -> Result<()> {
    info!("Checking for orphaned stones");

    // All stones on the filesystem
    let mut stones = vec![];

    let pool_dir = state.public_dir().join(channel).join("pool");
    let legacy_pool_dir = state.public_dir().join(channel).join("legacy/pool");

    for path in [&pool_dir, &legacy_pool_dir] {
        if fs::try_exists(path).await? {
            stones.extend(package::enumerate(path).await.context("enumerate stones")?);
        }
    }

    // Package id is the sha256 of the file on disk. We use this to detect
    // orphaned stones that aren't part of any existing index.
    let index_hashes = db::unique_package_ids(
        state
            .service_db()
            .acquire()
            .await
            .context("acquire database connection")?
            .as_mut(),
        channel,
    )
    .await
    .context("list unique package ids from db")?;

    let orphaned_stones = stones
        .into_iter()
        .filter(|stone| !index_hashes.contains(&stone.sha256sum))
        .collect::<Vec<_>>();

    if orphaned_stones.is_empty() {
        info!("No orphaned stones on disk");
        return Ok(());
    }

    let num_stones = orphaned_stones.len();

    remove_orphaned_packages(state, orphaned_stones)
        .await
        .context("remove orphaned packages")?;

    info!(num_stones, "All orphaned stones removed");

    Ok(())
}

async fn remove_orphaned_packages(state: &State, packages: Vec<Package>) -> Result<()> {
    use rayon::prelude::*;

    let state = state.clone();

    tokio::task::spawn_blocking(move || {
        packages
            .into_par_iter()
            .try_for_each_with(state, |state, stone| remove_orphaned_package(state, stone))
    })
    .await
    .context("join handle")?
}

fn remove_orphaned_package(state: &State, stone: Package) -> Result<()> {
    use std::fs;

    fs::remove_file(&stone.path).context(format!("remove orphaned stone {:?}", stone.path))?;

    state
        .meta_db
        .remove(&stone.sha256sum.clone().into())
        .context("remove stone from metadb")?;

    info!(path = ?stone.relative_path, "Orphaned stone removed");

    Ok(())
}
