use std::{collections::HashSet, path::Path};

use color_eyre::eyre::{self, Context, Result};
use moss::{db::meta, package, repository::Format};
use tokio::fs;
use tracing::{debug, info};

use crate::{
    State,
    channel::{self, DEFAULT_CHANNEL},
};

#[tracing::instrument(name = "migrations", skip_all)]
pub async fn run_all(state: &State) -> Result<()> {
    state
        .service_db()
        .migrate(sqlx::migrate!("./migrations"))
        .await
        .context("apply database migrations")?;

    migrate_collection_model(state, &state.meta_db)
        .await
        .context("migrate channel versions")?;

    migrate_format_legacy(state).await.context("migrate format v0")?;

    Ok(())
}

#[tracing::instrument(name = "", skip_all, fields(migration = "collection-model-to-channel-version-model"))]
async fn migrate_collection_model(state: &State, meta_db: &meta::Database) -> Result<()> {
    let mut tx = state.service_db().begin().await.context("begin db tx")?;

    let (count,) = sqlx::query_as::<_, (i64,)>(
        "
        SELECT COUNT(*)
        FROM sqlite_master
        WHERE type='table'
          AND name='pending_migration_collection';
        ",
    )
    .fetch_one(tx.as_mut())
    .await?;

    if count == 0 {
        return Ok(());
    }

    info!("Migrating collection model to new channel version model");

    let package_ids = sqlx::query_as::<_, (String,)>(
        "
        SELECT package_id FROM pending_migration_collection; 
        ",
    )
    .fetch_all(tx.as_mut())
    .await?
    .into_iter()
    .map(|(package,)| package::Id::from(package))
    .collect::<HashSet<_>>();

    let entries = tokio::task::spawn_blocking({
        let meta_db = meta_db.clone();

        move || {
            let mut entries = Vec::with_capacity(package_ids.len());

            for id in package_ids {
                let meta = meta_db
                    .get(&id)
                    .with_context(|| format!("lookup metadata for package {id}"))?;

                entries.push(channel::version::Entry::new(&id, &meta, Format::Legacy));
            }

            eyre::Ok(entries)
        }
    })
    .await
    .context("spawn blocking")?
    .context("lookup package metadata")?;

    channel::db::record_history(&mut tx, DEFAULT_CHANNEL, &entries, &Format::Legacy)
        .await
        .context("record entries")?;

    sqlx::query(
        "
        DROP TABLE pending_migration_collection;
        ",
    )
    .execute(tx.as_mut())
    .await?;

    // Remove old volatile folder
    let _ = fs::remove_dir_all(state.public_dir().join("volatile")).await;

    // Create new `main` folder
    let _ = fs::create_dir_all(state.public_dir().join(DEFAULT_CHANNEL)).await;

    // Move `pool` under `main`
    let _ = fs::rename(
        state.public_dir().join("pool"),
        state.public_dir().join(DEFAULT_CHANNEL).join("pool"),
    )
    .await;

    tx.commit().await.context("commit db tx")?;

    info!("Migration complete");

    Ok(())
}

/// Migrates from our `legacy` on-disk format to the new `v0`+ format. This doesn't
/// handle migrating `volatile` to the new `v0` format, as that is handled by an
/// explicit admin command and until that is issued, all newly recorded history
/// is still `legacy` format, just now placed under a `legacy/` folder.
///
/// This migration will only run if the current on-disk format has a `volatile` index
/// written out with the `legacy` on disk-format, meaning it shouldn't run on fresh
/// instances of `vessel` or instances that have already run this migration.
#[tracing::instrument(name = "", skip_all, fields(migration = "format-legacy"))]
async fn migrate_format_legacy(state: &State) -> Result<()> {
    let channel_dir = state.public_dir().join(DEFAULT_CHANNEL);
    // Symlink file for volatile stream from on-disk legacy format
    let volatile_link = channel_dir.join("stream/volatile");
    // New v0+ format aware index file
    let root_index = channel_dir.join("moss-root-index.json");
    // New legacy/ folder to house older on-disk legacy format
    let legacy_dir = channel_dir.join("legacy");

    // If root index already exists, then we're already operating at v0+
    if fs::try_exists(&root_index).await? {
        debug!("Root index already exists");
        return Ok(());
    }
    // If legacy dir already exists, then we already ran this migration
    if fs::try_exists(&legacy_dir).await? {
        debug!("Legacy dir already exists");
        return Ok(());
    }
    // If legacy volatile link doesn't exist, there is nothing to migrate
    // to the new v0+ on-disk format legacy/ folder
    if !fs::try_exists(&volatile_link).await? {
        debug!("No volatile index exists on this channel");
        return Ok(());
    }

    // We haven't migrated to `<pool>/legacy` folder yet, do that now
    info!("Migrating to legacy/ folder");

    fs::create_dir_all(&legacy_dir).await?;

    // History dir isn't migrated, but all other dirs are
    for dir in ["pool", "tag", "stream"] {
        let source_dir = channel_dir.join(dir);

        if fs::try_exists(&source_dir).await? {
            let dest_dir = legacy_dir.join(dir);

            fs::rename(&source_dir, &dest_dir).await?;

            info!(?source_dir, ?dest_dir, "Moved folder beneath legacy/");
        }
    }

    // Update all tag / stream symlinks since `history` is
    // now an extra level up
    //
    // Before: stream/volatile -> ../history/<ident>
    // After: legacy/stream/volatile -> ../../history/<ident>
    for dir in ["tag", "stream"] {
        let version_dir = legacy_dir.join(dir);

        if !fs::try_exists(&version_dir).await? {
            continue;
        }

        let mut content = fs::read_dir(&version_dir).await?;

        // Each symlink under legacy/(stream|tag)/ links to
        // a history dir, which we need to add an additional
        // leading `..` to the link
        while let Some(entry) = content.next_entry().await? {
            let link = entry.path();

            if let Ok(old_source) = fs::read_link(&link).await {
                let new_source = Path::new("..").join(&old_source);

                fs::remove_file(&link).await?;

                fs::symlink(&new_source, &link)
                    .await
                    .context(format!("link from {new_source:?} to {link:?}"))?;

                info!(?link, ?old_source, ?new_source, "Updated version symlink");
            }
        }
    }

    info!("Migration complete");

    Ok(())
}
