use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    time::Duration,
};

use chrono::Utc;
use color_eyre::eyre::{Context, Result};
use futures_util::{TryStreamExt, future};
use tokio::fs;
use tracing::info;

use crate::{
    State,
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
    let mut on_disk_stones = BTreeSet::new();

    let base_dir = state.public_dir().join(channel);
    let pool_dir = base_dir.join("pool");
    let legacy_pool_dir = base_dir.join("legacy/pool");

    for path in [&pool_dir, &legacy_pool_dir] {
        if fs::try_exists(path).await? {
            on_disk_stones.extend(package::enumerate_paths(path).await.context("enumerate stones")?);
        }
    }

    // All packages linked to from an active history & their resolved path
    //
    // Ensure we remove all orphaned DB packages caused from removed histories
    // since the last time we ran this function
    let (db_package_paths, db_package_ids) = {
        let mut tx = state.service_db().begin().await.context("begin database tx")?;

        db::delete_orphaned_packages(&mut tx, channel)
            .await
            .context("delete db orphaned packages")?;

        let (paths, package_ids) = db::all_entries(tx.as_mut(), channel)
            .try_fold((BTreeSet::new(), BTreeSet::new()), |(mut paths, mut ids), entry| {
                paths.insert(base_dir.join(entry.relative_path()));
                ids.insert(moss::package::Id::from(entry.package_id));
                future::ready(Ok((paths, ids)))
            })
            .await
            .context("list db all entries")?;

        tx.commit().await.context("commit db tx")?;

        (paths, package_ids)
    };

    // Any on-disk stone that isn't used by any remaining channel history
    let orphaned_stones = on_disk_stones
        .into_iter()
        .filter(|path| !db_package_paths.contains(path))
        .collect::<Vec<_>>();

    if orphaned_stones.is_empty() {
        info!("No orphaned stones on disk");
        return Ok(());
    }

    let num_stones = orphaned_stones.len();

    remove_orphaned_stones(orphaned_stones)
        .await
        .context("remove orphaned stones")?;

    remove_orphaned_meta_db_packages(state, db_package_ids)
        .await
        .context("remove orphaned meta db packages")?;

    info!(num_stones, "All orphaned stones removed");

    Ok(())
}

async fn remove_orphaned_stones(stones: Vec<PathBuf>) -> Result<()> {
    use rayon::prelude::*;

    tokio::task::spawn_blocking(move || {
        stones
            .into_par_iter()
            .try_for_each(|path| remove_orphaned_stone(&path).context(format!("remove orphaned package {path:?}")))
    })
    .await
    .context("join handle")?
}

fn remove_orphaned_stone(path: &Path) -> Result<()> {
    use std::fs;

    fs::remove_file(path).context("remove stone")?;

    info!(?path, "Orphaned stone removed");

    Ok(())
}

async fn remove_orphaned_meta_db_packages(state: &State, db_package_ids: BTreeSet<moss::package::Id>) -> Result<()> {
    let state = state.clone();

    tokio::task::spawn_blocking(move || {
        let meta_db_ids = state.meta_db.package_ids().context("list meta db package ids")?;

        let orphaned_meta_db_ids = meta_db_ids.difference(&db_package_ids);

        state
            .meta_db
            .batch_remove(orphaned_meta_db_ids)
            .context("remove orphaned meta db package ids")?;

        Ok(())
    })
    .await
    .context("join handle")?
}
