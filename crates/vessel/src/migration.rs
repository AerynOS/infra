use std::collections::HashSet;

use color_eyre::eyre::{self, Context, Result};
use moss::{db::meta, package};
use service::{State, database::Transaction};
use tokio::fs;
use tracing::info;

use crate::channel::{self, DEFAULT_CHANNEL};

#[tracing::instrument(name = "migrations", skip_all)]
pub async fn run_all(state: &State) -> Result<()> {
    let mut tx = state.service_db.begin().await.context("begin db tx")?;

    let meta_db = meta::Database::new(state.db_dir.join("meta").to_string_lossy().as_ref())
        .context("failed to open meta database")?;

    migrate_collection_model(state, &meta_db, &mut tx)
        .await
        .context("migrate channel versions")?;

    tx.commit().await.context("commit db tx")?;

    Ok(())
}

#[tracing::instrument(name = "", skip_all, fields(migration = "collection-model-to-channel-version-model"))]
async fn migrate_collection_model(state: &State, meta_db: &meta::Database, tx: &mut Transaction) -> Result<()> {
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

                entries.push(channel::Entry::new(&id, &meta));
            }

            eyre::Ok(entries)
        }
    })
    .await
    .context("spawn blocking")?
    .context("lookup package metadata")?;

    channel::record(tx, DEFAULT_CHANNEL, &entries)
        .await
        .context("record entries")?;

    sqlx::query(
        "
        DROP TABLE pending_migration_collection;
        ",
    )
    .execute(tx.as_mut())
    .await?;

    fs::remove_dir_all(state.state_dir.join("public/volatile"))
        .await
        .context("remove old volatile index directory")?;

    let _ = fs::create_dir_all(state.state_dir.join("public").join(DEFAULT_CHANNEL)).await;

    fs::rename(
        state.state_dir.join("public/pool"),
        state.state_dir.join("public").join(DEFAULT_CHANNEL).join("pool"),
    )
    .await
    .context("move pool under default channel")?;

    info!("Migration complete");

    Ok(())
}
