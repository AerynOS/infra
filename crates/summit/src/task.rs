use std::path::Path;

use chrono::{DateTime, Utc};
use color_eyre::eyre::{Context, Result};
use derive_more::derive::{Display, From, Into};
use http::Uri;
use moss::{db::meta, dependency, package::Meta};
use serde::{Deserialize, Serialize};
use service::database::Transaction;
use service::endpoint;
use sqlx::prelude::FromRow;
use strum::IntoEnumIterator;
use tokio::task::spawn_blocking;
use tracing::{Span, debug, warn};
use uuid::Uuid;

use crate::{Manager, Project, Repository, profile, project, repository, task};

pub use self::create::create;
pub use self::query::query;

pub mod create;
pub mod query;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, From, Into, Display, FromRow)]
pub struct Id(i64);

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Task {
    pub id: Id,
    pub project_id: project::Id,
    pub profile_id: profile::Id,
    pub repository_id: repository::Id,
    pub slug: String,
    pub package_id: String,
    pub arch: String,
    pub build_id: String,
    pub description: String,
    pub commit_ref: String,
    pub source_path: String,
    pub status: Status,
    pub allocated_builder: Option<endpoint::Id>,
    pub log_path: Option<String>,
    pub blocked_by: Vec<String>,
    pub added: DateTime<Utc>,
    pub started: Option<DateTime<Utc>>,
    pub updated: DateTime<Utc>,
    pub ended: Option<DateTime<Utc>>,
    pub duration: Option<i64>,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, strum::Display, strum::EnumString, strum::EnumIter,
)]
#[serde(rename_all = "kebab-case")]
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
    /// Task was superseded by a newer task.
    /// If this task was previously blocked, it must be
    /// removed from the task_blockers table, but only after the
    /// superseding task has been marked blocked by the blocker
    /// in a new row in the task_blockers table.
    Superseded,
    /// Job successfully completed!
    Completed,
    /// This build must remain blocked until its block
    /// criteria have been met, i.e. the dependent that
    /// caused the failure has been fixed.
    Blocked,
}

impl Status {
    pub fn is_open(&self) -> bool {
        !matches!(self, Status::Completed | Status::Failed | Status::Superseded)
    }

    pub fn is_in_progress(&self) -> bool {
        matches!(self, Status::Building | Status::Publishing)
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

#[tracing::instrument(name = "create_missing_tasks", skip_all, fields(repository = %repo.name, profile))]
pub async fn create_missing(
    tx: &mut Transaction,
    manager: &Manager,
    project: &Project,
    repo: &Repository,
    repo_db: &meta::Database,
) -> Result<()> {
    let span = Span::current();

    for profile in &project.profiles {
        span.record("profile", &profile.name);

        if !matches!(profile.status, profile::Status::Indexed) {
            warn!(
                status = %profile.status,
                "Profile not fully indexed, skipping task creation"
            );
            continue;
        }

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

                let latest = corresponding
                    .iter()
                    .max_by(|(_, a), (_, b)| a.source_release.cmp(&b.source_release));

                if let Some((_, published)) = latest {
                    // distinguishing between > and == is the kind thing to do in logs
                    if published.source_release > meta.source_release {
                        warn!(
                            slug = slug(),
                            published = version(published),
                            recipe = version(&meta),
                            "Newer package version already present in index"
                        );
                        continue 'providers;
                    } else if published.source_release == meta.source_release {
                        warn!(
                            slug = slug(),
                            published = version(published),
                            recipe = version(&meta),
                            "Current package version already present in index"
                        );
                        continue 'providers;

                    // published.source_release > meta.source_release below
                    // so we need to create a new task for the newer package
                    } else {
                        debug!(
                            slug = slug(),
                            published = version(published),
                            recipe = version(&meta),
                            "Adding newer package as task"
                        );

                        create(
                            tx,
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

                        break 'providers;
                    }
                } else {
                    debug!(
                        slug = slug(),
                        version = version(&meta),
                        "Adding missing package as task"
                    );

                    create(
                        tx,
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

/// Set the status of a task_id in the db
pub async fn set_status(tx: &mut Transaction, task_id: task::Id, status: Status) -> Result<()> {
    let ended = if !status.is_open() { ", ended = unixepoch()" } else { "" };

    let started = if status == Status::Building {
        ", started = unixepoch()"
    } else if status == Status::New {
        ", started = null"
    } else {
        ""
    };

    let query = format!(
        "
        UPDATE task
        SET
          status = ?,
          updated = unixepoch(){ended}{started}
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

/// Set the path to the task logfile in the filesystem
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
    .context("set log_path for task_id={task_id:?}")?;

    debug!("set log_path for task_id={task_id:?}");

    Ok(())
}

/// Allocate a builder for a task
pub async fn set_allocated_builder(
    tx: &mut Transaction,
    task_id: task::Id,
    builder: Option<endpoint::Id>,
) -> Result<()> {
    sqlx::query(
        "
        UPDATE task
        SET
          allocated_builder = ?,
          updated = unixepoch()
        WHERE task_id = ?;
        ",
    )
    .bind(builder.map(Uuid::from))
    .bind(i64::from(task_id))
    .execute(tx.as_mut())
    .await
    .context("update allocated_builder for task_id={task_id:?}")?;

    debug!("update allocated_builder for task_id={task_id:?}");

    Ok(())
}

/// block a task Id with a blocker string and insert it into the task_blockers table
pub async fn block(tx: &mut Transaction, task_id: Id, blocker: &str) -> Result<()> {
    set_status(tx, task_id, Status::Blocked).await?;

    let _ = sqlx::query(
        "
        INSERT INTO task_blockers (task_id, blocker)
        VALUES (?,?)
        ON CONFLICT DO NOTHING;
        ",
    )
    .bind(i64::from(task_id))
    .bind(blocker)
    .execute(tx.as_mut())
    .await
    .context("add blocker={blocker:?} for task_id={task_id:?}")?;

    debug!("add blocker={blocker:?} for task_id={task_id:?}");

    Ok(())
}

/// Unblock a task Id previously blocked by blocker string from the task_blockers table
pub async fn unblock(tx: &mut Transaction, task_id: Id, blocker: &str) -> Result<usize> {
    let _ = sqlx::query(
        "
        DELETE FROM task_blockers
        WHERE task_id = ? AND blocker = ?;
        ",
    )
    .bind(i64::from(task_id))
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
    .bind(i64::from(task_id))
    .fetch_one(tx.as_mut())
    .await?;

    debug!("remove blocker={blocker:?} for task_id={task_id:?} ({remaining:?} blockers remain)");

    if remaining > 0 {
        set_status(tx, task_id, Status::Blocked).await?;
        debug!("task_id={task_id:?} remains blocked by {remaining:?} blockers");
    } else {
        set_status(tx, task_id, Status::New).await?;
        debug!("task_id={task_id:?} is now unblocked ({remaining:?} blockers remain)");
    }

    Ok(remaining as usize)
}
