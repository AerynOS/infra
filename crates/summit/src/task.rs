use std::path::Path;

use chrono::{DateTime, Utc};
use color_eyre::eyre::{Context, Result};
use derive_more::derive::{Display, From, Into};
use http::Uri;
use moss::{
    db::meta,
    dependency,
    package::{self, Meta},
};
use serde::{Deserialize, Serialize};
use service::database::Transaction;
use sqlx::{SqliteConnection, prelude::FromRow};
use strum::IntoEnumIterator;
use tokio::task::spawn_blocking;
use tracing::{debug, warn};

use crate::{Manager, Project, Repository, profile, project, repository, task};

pub use self::build::build;
pub use self::create::create;
pub use self::query::query;

pub mod build;
pub mod create;
pub mod query;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, From, Into, Display, FromRow)]
pub struct Id(i64);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Task {
    pub id: Id,
    pub project_id: project::Id,
    pub profile_id: profile::Id,
    pub repository_id: repository::Id,
    pub slug: String,
    pub package_id: package::Id,
    pub arch: String,
    pub build_id: String,
    pub description: String,
    pub commit_ref: String,
    pub source_path: String,
    pub status: Status,
    pub allocated_builder: Option<String>,
    pub log_path: Option<String>,
    pub blocked_by: Vec<String>,
    pub started: DateTime<Utc>,
    pub updated: DateTime<Utc>,
    pub ended: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, strum::Display, strum::EnumString, strum::EnumIter)]
#[strum(serialize_all = "kebab-case")]
pub enum Status {
    /// Freshly created task
    New,
    /// Failed execution or evaluation
    Failed,
    /// This task is now building
    Building,
    /// Now publishing to Vessel
    Publishing,
    /// Job successfully completed!
    Completed,
    /// This build must remain blocked until its block
    /// criteria have been met, i.e. the dependent that
    /// caused the failure has been fixed.
    Blocked,
}

impl Status {
    pub fn is_open(&self) -> bool {
        !matches!(self, Status::Completed | Status::Failed)
    }

    pub fn open() -> impl Iterator<Item = Status> {
        Status::iter().filter(Status::is_open)
    }
}

#[derive(Debug, Clone)]
pub struct Queued {
    pub task: Task,
    pub meta: Meta,
    pub commit_ref: String,
    pub origin_uri: Uri,
    pub index_uri: Uri,
    pub remotes: Vec<Uri>,
    pub dependencies: Vec<task::Id>,
}

#[tracing::instrument(name = "create_missing_tasks", skip_all)]
pub async fn create_missing(
    conn: &mut SqliteConnection,
    manager: &Manager,
    project: &Project,
    repo: &Repository,
    repo_db: &meta::Database,
) -> Result<()> {
    for profile in &project.profiles {
        let profile_db = manager.profile_db(&profile.id).context("missing profile db")?;

        let packages = spawn_blocking({
            let repo_db = repo_db.clone();
            move || repo_db.query(None)
        })
        .await
        .context("join handle")?
        .context("list source repo packages")?;

        for (_, meta) in packages {
            'providers: for name in meta
                .providers
                .iter()
                .filter(|p| p.kind == dependency::Kind::PackageName)
                .cloned()
            {
                let corresponding = task::spawn_blocking({
                    let profile_db = profile_db.clone();
                    move || profile_db.query(Some(meta::Filter::Provider(name)))
                })
                .await
                .context("join handle")?
                .context("list package dependents")?;

                let slug = || format!("~/{}/{}/{}", project.slug, repo.name, meta.name);
                let version = |meta: &Meta| format!("{}-{}", meta.version_identifier, meta.source_release);

                let latest = corresponding.iter().max_by(|(_, a), (_, b)| {
                    a.version_identifier
                        .cmp(&b.version_identifier)
                        .then(a.source_release.cmp(&b.source_release))
                });

                if let Some((_, published)) = latest {
                    if published.source_release >= meta.source_release {
                        warn!(
                            slug = slug(),
                            published = version(published),
                            recipe = version(&meta),
                            "Newer package already in index"
                        );

                        continue 'providers;
                    } else {
                        debug!(
                            slug = slug(),
                            published = version(published),
                            recipe = version(&meta),
                            "Adding newer package as task"
                        );

                        create(
                            conn,
                            project,
                            profile,
                            repo,
                            &meta,
                            format!(
                                "Update {} from {} to {}",
                                meta.source_id,
                                version(published),
                                version(&meta)
                            ),
                        )
                        .await
                        .context("create task")?;
                    }
                } else {
                    debug!(
                        slug = slug(),
                        version = version(&meta),
                        "Adding missing package as task"
                    );

                    create(
                        conn,
                        project,
                        profile,
                        repo,
                        &meta,
                        format!("Initial build of {} ({})", meta.source_id, version(&meta)),
                    )
                    .await
                    .context("create task")?;

                    break 'providers;
                };
            }
        }
    }

    Ok(())
}

pub async fn set_status(tx: &mut Transaction, task_id: task::Id, status: Status) -> Result<()> {
    let ended = if !status.is_open() { ", ended = unixepoch()" } else { "" };

    let query = format!(
        "
        UPDATE task
        SET
          status = ?,
          updated = unixepoch(){ended}
        WHERE task_id = ?;
        ",
    );

    sqlx::query(&query)
        .bind(status.to_string())
        .bind(i64::from(task_id))
        .execute(tx.as_mut())
        .await
        .context("update task")?;

    Ok(())
}

pub async fn set_log_path(tx: &mut Transaction, task_id: task::Id, log_path: &Path) -> Result<()> {
    sqlx::query(
        "
        UPDATE task
        SET
          log_path = ?,
          updated = unixepoch()
        WHERE task_id = ?;
        ",
    )
    .bind(log_path.display().to_string())
    .bind(i64::from(task_id))
    .execute(tx.as_mut())
    .await
    .context("update task")?;

    Ok(())
}

pub async fn set_allocated_builder(tx: &mut Transaction, task_id: task::Id, allocated_builder: &str) -> Result<()> {
    sqlx::query(
        "
        UPDATE task
        SET
          allocated_builder = ?,
          updated = unixepoch()
        WHERE task_id = ?;
        ",
    )
    .bind(allocated_builder)
    .bind(i64::from(task_id))
    .execute(tx.as_mut())
    .await
    .context("update task")?;

    Ok(())
}

pub async fn block(tx: &mut Transaction, task: Id, blocker: &str) -> Result<()> {
    set_status(tx, task, Status::Blocked).await?;

    let _ = sqlx::query(
        "
        INSERT INTO task_blockers (task_id, blocker)
        VALUES (?,?);
        ",
    )
    .bind(i64::from(task))
    .bind(blocker)
    .execute(tx.as_mut())
    .await?;

    Ok(())
}

pub async fn unblock(tx: &mut Transaction, task: Id, blocker: &str) -> Result<usize> {
    let _ = sqlx::query(
        "
        DELETE FROM task_blockers
        WHERE task_id = ? AND blocker = ?;
        ",
    )
    .bind(i64::from(task))
    .bind(blocker)
    .execute(tx.as_mut())
    .await?;

    let (remaining,) = sqlx::query_as::<_, (u32,)>(
        "
        SELECT COUNT(*)
        FROM task_blockers
        WHERE task_id = ?;
        ",
    )
    .bind(i64::from(task))
    .fetch_one(tx.as_mut())
    .await?;

    set_status(tx, task, if remaining > 0 { Status::Blocked } else { Status::New }).await?;

    Ok(remaining as usize)
}
