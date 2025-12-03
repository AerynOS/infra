use std::collections::HashSet;

use color_eyre::eyre::{Context, Result};
use tracing::info;

use crate::{State, channel::version, package};

#[tracing::instrument(skip_all, fields(%channel))]
pub async fn prune(state: &State, channel: &str) -> Result<()> {
    info!("Prune started");

    prune_orphaned_packages(state, channel).await?;

    info!("Prune finished");

    Ok(())
}

#[tracing::instrument(skip_all)]
async fn prune_orphaned_packages(state: &State, channel: &str) -> Result<()> {
    info!("Checking for orphaned stones");

    let pool_dir = state.public_dir().join(channel).join("pool");

    // All stones on the filesystem
    let stones = package::async_enumerate(&pool_dir).await.context("enumerate stones")?;

    // Packages in any version
    let indexed_packages = version::list_all(
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
