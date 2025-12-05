use std::collections::HashSet;

use color_eyre::eyre::{self, Context, Result};
use moss::{db::meta, package};
use tokio::fs;
use tracing::info;

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

                entries.push(channel::version::Entry::new(&id, &meta));
            }

            eyre::Ok(entries)
        }
    })
    .await
    .context("spawn blocking")?
    .context("lookup package metadata")?;

    channel::db::record_history(&mut tx, DEFAULT_CHANNEL, &entries)
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
