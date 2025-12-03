use std::{collections::HashSet, time::Duration};

use chrono::Utc;
use color_eyre::eyre::{Context, Result};
use tokio::fs;
use tracing::info;

use crate::{State, channel::version, package};

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

    let mut tx = state.service_db().begin().await.context("begin db tx")?;

    let created_before = Utc::now() - chrono::Duration::from_std(STALE_AFTER).expect("within i64");

    info!(%created_before, "Checking for stale history");

    let deleted = version::delete_stale_history(&mut tx, channel, created_before)
        .await
        .context("delete stale history")?;

    if deleted.is_empty() {
        info!("No stale history");
        return Ok(());
    }

    for version in &deleted {
        // Remove from filesystem
        let _ = fs::remove_dir_all(state.public_dir().join(channel).join(version.relative_base_dir())).await;

        info!(%version, "History deleted");
    }

    info!("num_deleted" = deleted.len(), "Stale history deleted");

    tx.commit().await.context("commit db tx")?;

    Ok(())
}

#[tracing::instrument(skip_all)]
async fn prune_orphaned_packages(state: &State, channel: &str) -> Result<()> {
    info!("Checking for orphaned stones");

    let pool_dir = state.public_dir().join(channel).join("pool");

    // All stones on the filesystem
    let stones = package::async_enumerate(&pool_dir).await.context("enumerate stones")?;

    // Packages in any version
    let indexed_packages = version::all_entries(
        state
            .service_db()
            .acquire()
            .await
            .context("acquire database connection")?
            .as_mut(),
        channel,
    )
    .await
    .context("list entries from collection db")?;

    // Package id is the sha256 of the file on disk. We use this to detect
    // orphaned packages that aren't part of the existing index
    let index_hashes = indexed_packages
        .into_iter()
        .map(|entry| entry.package_id)
        .collect::<HashSet<_>>();

    let orphaned_stones = stones
        .into_iter()
        .filter(|stone| !index_hashes.contains(&stone.sha256sum))
        .collect::<Vec<_>>();

    if orphaned_stones.is_empty() {
        info!("No orphaned stones on disk");
        return Ok(());
    }

    for stone in &orphaned_stones {
        let relative_path = stone.path.strip_prefix(&pool_dir).expect("lives in pool dir");

        fs::remove_file(&stone.path)
            .await
            .context(format!("remove orphaned stone {:?}", stone.path))?;

        state
            .meta_db
            .remove(&stone.sha256sum.clone().into())
            .context("remove stone from metadb")?;

        info!(path = ?relative_path, "Orphaned stone removed");
    }

    info!("num_stones" = orphaned_stones.len(), "All orphaned stones removed");

    Ok(())
}
