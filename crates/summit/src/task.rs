use std::path::Path;

use chrono::{DateTime, Utc};
use color_eyre::eyre::{self, Context, Result};
use derive_more::derive::{Display, From, Into};
use http::Uri;
use moss::{db::meta, dependency, package::Meta};
use serde::{Deserialize, Serialize};
use service::database::Transaction;
use service::endpoint;
use sqlx::prelude::FromRow;
use strum::IntoEnumIterator;
use tokio::task::spawn_blocking;
use tracing::{Instrument, debug, warn};
use uuid::Uuid;

use crate::{Manager, Profile, Project, Repository, builder, profile, project, repository};

pub use self::query::query;

pub mod create;
pub mod query;

use self::create::create;

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

    /// Path to the compressed log file.
    ///
    /// Relative to the state directory.
    pub log_path: Option<String>,

    /// Failed dependencies that stop this task from getting executed.
    ///
    /// Items are blocker IDs consisting of source ID, arch, project ID and repo ID.
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
    /// Cancelled execution
    Cancelled,
    /// This task is now building
    Building,
    /// Now publishing to Vessel
    Publishing,
    /// Task was superseded by a newer task.
    ///
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
        !matches!(
            self,
            Status::Completed | Status::Failed | Status::Cancelled | Status::Superseded
        )
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
    pub dependencies: Vec<Id>,
}

#[tracing::instrument(name = " fix_and_create_tasks", skip_all, fields(repository = %repo.name))]
pub async fn fix_and_create(
    tx: &mut Transaction,
    manager: &Manager,
    project: &Project,
    repo: &Repository,
    repo_db: &meta::Database,
) -> Result<()> {
    // Move tasks stuck in building back to new prior to
    // adding new tasks so if they have been superseded, they
    // can be marked as such
    fix_stuck_building(tx, manager, project, repo)
        .await
        .context("fix stuck building tasks")?;

    for task in &collect_missing(manager, project, repo, repo_db).await? {
        // FIXME: do a batch insert?
        create(tx, project, repo, task).await.context("create task")?;
    }

    // Move tasks no longer in the recipe repo to failed after
    // adding new tasks so that superseded tasks are marked as such
    // and only truly orphaned tasks remain
    fix_orphaned(tx, project, repo, repo_db)
        .await
        .context("fix orphaned tasks")?;

    Ok(())
}

#[tracing::instrument(
    name = "fix_stuck_building_tasks",
    skip_all,
    fields(repository = %repo.name, project = %project.name)
)]
async fn fix_stuck_building(
    tx: &mut Transaction,
    manager: &Manager,
    project: &Project,
    repo: &Repository,
) -> Result<()> {
    const DISCONNECTED_TIMEOUT: chrono::Duration = chrono::Duration::minutes(60);

    let building_tasks = query(tx.as_mut(), query::Params::default().statuses(Some(Status::Building)))
        .await
        .context("list building tasks")?
        .tasks;

    for task in building_tasks {
        let requeue = if let Some(endpoint) = task.allocated_builder {
            match manager.builder_status(&endpoint) {
                Some(builder::Status::Building { task: id }) => {
                    if task.id == id {
                        false
                    } else {
                        warn!(
                            task = %task.id,
                            build = task.build_id,
                            builder = %endpoint,
                            "Builder is building another task, requeuing stuck build"
                        );

                        true
                    }
                }
                Some(builder::Status::Idle) => {
                    warn!(
                        task = %task.id,
                        build = task.build_id,
                        builder = %endpoint,
                        "Builder is idle, requeuing stuck build"
                    );

                    true
                }
                Some(builder::Status::Disconnected) | None => {
                    let build_duration = task.started.map(|started| Utc::now() - started).unwrap_or_default();

                    if build_duration > DISCONNECTED_TIMEOUT {
                        warn!(
                            task = %task.id,
                            build = task.build_id,
                            builder = %endpoint,
                            "Builder is disconnected & build is older than {} minutes, requeuing stuck build",
                            DISCONNECTED_TIMEOUT.num_minutes()
                        );

                        true
                    } else {
                        false
                    }
                }
            }
        } else {
            warn!(
                task = %task.id,
                build = task.build_id,
                "Building task has no set allocated builder, requeuing"
            );

            true
        };

        if requeue {
            set_status(tx, task.id, Status::New).await.context("set task status")?;
        }
    }

    Ok(())
}

#[tracing::instrument(
    name = "fix_orphaned_tasks",
    skip_all,
    fields(repository = %repo.name, project = %project.name)
)]
async fn fix_orphaned(
    tx: &mut Transaction,
    project: &Project,
    repo: &Repository,
    repo_db: &meta::Database,
) -> Result<()> {
    let pending_tasks = query(tx.as_mut(), query::Params::default().statuses(Some(Status::New)))
        .await
        .context("list new tasks")?
        .tasks;

    for task in pending_tasks {
        let is_missing = spawn_blocking({
            let repo_db = repo_db.clone();
            let package_id = task.package_id.into();

            move || repo_db.get(&package_id)
        })
        .await
        .context("join handle")?
        .is_err();

        if is_missing {
            warn!(
                task = %task.id,
                build = task.build_id,
                "Task no longer exists in repository, marking as failed"
            );

            set_status(tx, task.id, Status::Failed)
                .await
                .context("set task status")?;
        }
    }

    Ok(())
}

/// Set the status of a task_id in the db
pub async fn set_status(tx: &mut Transaction, task_id: Id, status: Status) -> Result<()> {
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
pub async fn set_log_path(tx: &mut Transaction, task_id: Id, log_path: &Path) -> Result<()> {
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

    debug!(%task_id, "will use log_path='{log_path:?}' for output");

    Ok(())
}

/// Allocate a builder for a task
pub async fn set_allocated_builder(tx: &mut Transaction, task_id: Id, builder: Option<endpoint::Id>) -> Result<()> {
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

    debug!(%task_id, "update allocated_builder");

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

    let remaining: u32 = sqlx::query_scalar(
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
        debug!(%task_id, "remains blocked by {remaining:?} blockers");
    } else {
        set_status(tx, task_id, Status::New).await?;
        debug!(%task_id, "is now unblocked ({remaining:?} blockers remain)");
    }

    Ok(remaining as usize)
}

struct MissingTask<'a> {
    kind: MissingTaskKind,
    profile: &'a Profile,
    meta: Meta,
}

impl<'a> MissingTask<'a> {
    fn description(&self) -> String {
        let source_id = &self.meta.source_id;
        let version = version(&self.meta);

        match &self.kind {
            MissingTaskKind::Initial => format!("Initial build of {source_id} ({version})"),
            MissingTaskKind::Update { published } => format!("Update {source_id} from {published} to {version}"),
        }
    }
}

enum MissingTaskKind {
    Initial,
    Update { published: String },
}

async fn collect_missing<'a>(
    manager: &Manager,
    project: &'a Project,
    repo: &Repository,
    repo_db: &meta::Database,
) -> Result<Vec<MissingTask<'a>>> {
    let mut missing_tasks = Vec::new();

    for profile in &project.profiles {
        let span = tracing::info_span!("collect_missing_tasks_for_profile", profile = profile.name);
        async {
            if !matches!(profile.status, profile::Status::Indexed) {
                warn!(
                    status = %profile.status,
                    "Profile not fully indexed, skipping task creation"
                );
                return eyre::Ok(()); // from the async block
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
                for name in meta
                    .providers
                    .iter()
                    .filter(|p| p.kind == dependency::Kind::PackageName)
                    .cloned()
                {
                    let corresponding = spawn_blocking({
                        let profile_db = profile_db.clone();
                        move || profile_db.query(Some(meta::Filter::Provider(name)))
                    })
                    .await
                    .context("join handle")?
                    .context("list package dependents")?;

                    let slug = || format!("~/{}/{}/{}", project.slug, repo.name, meta.name);

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
                                "Newer package release already present in index"
                            );
                            continue;
                        } else if published.source_release == meta.source_release {
                            warn!(
                                slug = slug(),
                                published = version(published),
                                recipe = version(&meta),
                                "Current package release already present in index"
                            );
                            continue;

                        // published.source_release > meta.source_release below
                        // so we need to create a new task for the newer package
                        } else {
                            let published = version(published);
                            debug!(
                                slug = slug(),
                                published,
                                recipe = version(&meta),
                                "Adding newer package release as task"
                            );

                            missing_tasks.push(MissingTask {
                                kind: MissingTaskKind::Update { published },
                                profile,
                                meta,
                            });

                            break;
                        }
                    } else {
                        debug!(
                            slug = slug(),
                            version = version(&meta),
                            "Adding missing package release as task"
                        );

                        missing_tasks.push(MissingTask {
                            kind: MissingTaskKind::Initial,
                            profile,
                            meta,
                        });

                        break;
                    };
                }
            }

            eyre::Ok(())
        }
        .instrument(span)
        .await?;
    }

    Ok(missing_tasks)
}

fn version(meta: &Meta) -> String {
    format!("{}-{}", meta.version_identifier, meta.source_release)
}
