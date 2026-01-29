use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use color_eyre::eyre::{self, Context, OptionExt, Result};
use derive_more::derive::{Display, From, Into};
use http::Uri;
use moss::{db::meta, dependency, package::Meta};
use serde::{Deserialize, Serialize};
use service::database::Transaction;
use service::endpoint;
use sqlx::{SqliteConnection, prelude::FromRow};
use strum::IntoEnumIterator;
use tokio::task::spawn_blocking;
use tracing::{Instrument, Span, debug, error, info, trace, warn};
use uuid::Uuid;

use crate::{Manager, Profile, Project, Repository, builder, config::Size, profile, project, repository};

pub use self::query::{Query, query};

pub mod create;
pub mod query;

use self::create::create;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, From, Into, Display, FromRow)]
pub struct Id(i64);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
    pub blocked_by: Vec<Blocker>,

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
    /// This build must remain blocked until its block
    /// criteria have been met, i.e. the dependent that
    /// caused the failure has been fixed.
    Blocked,
    /// This task is now building
    Building,
    /// Now publishing to Vessel
    Publishing,
    /// Job successfully completed!
    Completed,
    /// Failed execution or evaluation
    Failed,
    /// Task was superseded by a newer task.
    ///
    /// If this task was previously blocked, it must be
    /// removed from the task_blockers table, but only after the
    /// superseding task has been marked blocked by the blocker
    /// in a new row in the task_blockers table.
    Superseded,
    /// Cancelled execution
    Cancelled,
}

impl Status {
    pub fn is_closed(&self) -> bool {
        matches!(
            self,
            Status::Completed | Status::Failed | Status::Cancelled | Status::Superseded
        )
    }

    pub fn is_open(&self) -> bool {
        !self.is_closed()
    }

    pub fn is_in_progress(&self) -> bool {
        matches!(self, Status::Building | Status::Publishing)
    }

    pub fn open() -> impl Iterator<Item = Status> {
        Status::iter().filter(Status::is_open)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, strum::Display)]
#[strum(serialize_all = "kebab-case")]
pub enum Transition {
    /// Task is blocked by a specific blocked
    ///
    /// Must transition from open but not-inprogress statuses
    Blocked { blocker: Blocker },
    /// Task is unblocked by a specific blocker
    ///
    /// Must transition from `blocked`
    Unblocked { blocker: Blocker },
    /// Task has been superseded
    ///
    /// Must transition from open but not-inprogress statuses
    Superseded { by: Id },
    /// Task is building
    ///
    /// Must transition from `new`
    Building { builder: endpoint::Id },
    /// Task is publishing
    ///
    /// Must transition from `building`
    /// where we have stashed the build logs
    Publishing {
        builder: endpoint::Id,
        stashed_build_logs: PathBuf,
    },
    /// Task is complete
    ///
    /// Must transition from `publishing`
    Complete,
    /// Task is failed
    ///
    /// Can transition from any open status
    // TODO: Add failure reason / record in DB / display on UI
    Failed {
        // If caused by a build failure, we will have stashed
        // logs from the failed build
        stashed_build_logs: Option<PathBuf>,
    },
    /// Task has been cancelled
    ///
    /// Only allowed from an "in-progress" status
    /// like `building` & `publishing`
    Cancelled,
    /// Task needs to be retried
    ///
    /// Only allowed from a non-`complete` closed status
    Retry,
    /// Task is being requeued
    ///
    /// Only allowed from `building` status
    Requeue,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, derive_more::AsRef, derive_more::Display)]
pub struct Blocker(String);

/// A queue of tasks
pub trait TaskQueue: Send + Sync + 'static {
    /// Get a queued task from the queue
    fn get(&self, task_id: Id) -> Option<&Queued>;

    /// Get all tasks dependent on the provided `task_id`
    fn dependents(&self, task_id: Id) -> Vec<Id>;
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
    pub size: Size,
}

impl Queued {
    /// Returns the associated [`Blocker`] of this task
    pub fn blocker(&self) -> Blocker {
        Blocker(format!(
            "{}_{}@{}/{}",
            self.meta.source_id, self.task.arch, self.task.project_id, self.task.repository_id
        ))
    }
}

pub async fn get(conn: &mut SqliteConnection, id: Id) -> Result<Task> {
    query(conn, query::Params::default().id(id))
        .await
        .context("query for task")?
        .tasks
        .into_iter()
        .next()
        .ok_or_eyre(format!("task doesn't exist: {id}"))
}

/// Transition the task from one status to another
#[tracing::instrument(skip_all, fields(transition_task_id = %id, %transition, transition_build_id, transition_prev_status))]
pub async fn transition(
    tx: &mut Transaction,
    id: Id,
    mut transition: Transition,
    queue: &impl TaskQueue,
) -> Result<bool> {
    let span = Span::current();

    // Get current view on task
    let task = get(tx.as_mut(), id).await.context("get task")?;

    span.record("transition_build_id", &task.build_id);
    span.record("transition_prev_status", task.status.to_string());

    // Ensure this transition is valid and if not,
    // we mark the task as failed since our state
    // machine is somehow borked & needs to be fixed
    let is_valid_transition = match &transition {
        // We can only block things still waiting to get built
        Transition::Blocked { .. } => task.status.is_open() && !task.status.is_in_progress(),
        // We can only unblock things that are blocked
        Transition::Unblocked { blocker } => {
            // Ensure it's actually blocked by this
            if matches!(task.status, Status::Blocked) && !task.blocked_by.contains(blocker) {
                error!(
                    %blocker,
                    "Attempting unblock, but task is not blocked by this blocker"
                );

                false
            } else {
                matches!(task.status, Status::Blocked)
            }
        }
        // We can only supersede things still waiting to get built
        Transition::Superseded { .. } => task.status.is_open() && !task.status.is_in_progress(),
        // Must transition from a new unblocked task
        Transition::Building { .. } => matches!(task.status, Status::New),
        // Must transition from a building task
        Transition::Publishing { builder, .. } => {
            // Ensure is transitioning on the correct builder
            if matches!(task.status, Status::Building) && task.allocated_builder.is_none_or(|b| b != *builder) {
                error!(
                    requested_builder = %builder,
                    allocated_builder = task.allocated_builder.as_ref().map(ToString::to_string),
                    "Attempting to transition from building to publishing, but received request from non-allocated builder"
                );

                false
            } else {
                matches!(task.status, Status::Building)
            }
        }
        // Must transition from a publishing task
        Transition::Complete => matches!(task.status, Status::Publishing),
        // Only open tasks can enter a failure state
        Transition::Failed { .. } => task.status.is_open(),
        // We can only cancel things that are currently in-progress
        Transition::Cancelled => task.status.is_in_progress(),
        // We can only retry things that are closed but not already completed
        Transition::Retry => task.status.is_closed() && !matches!(task.status, Status::Completed),
        // We can only requeue something that is currently being built
        Transition::Requeue => matches!(task.status, Status::Building),
    };

    if !is_valid_transition {
        // Trying to cancel / fail / complete / supersede an already
        // cancelled / failed / completed / superseded task
        //
        // We don't want to override that status to failed but instead
        // log the error & return
        if task.status.is_closed() {
            error!("Invalid transition on a closed task");

            return Ok(false);
        }
        // Otherwise something is broken in our state machine for progressing
        // and open task forward. Fail it so it & log the issue. This will
        // require manual intervention & investigation to fix the broken flow.
        else {
            error!("Invalid transition on an open task, marking as failed");

            transition = Transition::Failed {
                stashed_build_logs: None,
            };
        }
    }

    match transition {
        Transition::Blocked { blocker } => {
            add_blocker(tx, task.id, &blocker).await.context("add blocker")?;

            // Only update status if not already blocked
            if !matches!(task.status, Status::Blocked) {
                set_status(tx, task.id, Status::Blocked).await.context("set status")?;
            }

            warn!(blocked_by = %blocker, "Task blocked");
        }
        Transition::Unblocked { blocker } => {
            let remaining = task.blocked_by.iter().filter(|b| **b != blocker).count();

            remove_blocker(tx, task.id, &blocker).await.context("remove blocker")?;

            if remaining > 0 {
                info!(
                    unblocked_by = %blocker,
                    %remaining,
                    "Task unblocked with remaining blockers"
                );
            } else {
                set_status(tx, task.id, Status::New).await.context("set status")?;

                info!(unblocked_by = %blocker, "Task fully unblocked");
            }
        }
        Transition::Superseded { by } => {
            // `superseded` can be reached from `blocked` so remove any blockers
            // for this task if they exist since they're no longer relevant
            if !task.blocked_by.is_empty() {
                clear_blockers(tx, task.id).await.context("clear blockers")?;
            }

            // No need to add blockers on dependents since the new incoming
            // task replaces this one & those tasks will depend on it

            set_status(tx, task.id, Status::Superseded)
                .await
                .context("set status")?;

            info!(superseded_by = %by, "Task marked as superseded");
        }
        Transition::Building { builder } => {
            set_allocated_builder(tx, task.id, Some(builder))
                .await
                .context("set status")?;
            set_status(tx, task.id, Status::Building).await.context("set status")?;

            info!(%builder, "Task marked as building");
        }
        Transition::Publishing { stashed_build_logs, .. } => {
            set_log_path(tx, task.id, Some(&stashed_build_logs))
                .await
                .context("set status")?;
            set_status(tx, task.id, Status::Publishing)
                .await
                .context("set status")?;

            info!("Task marked as publishing");
        }
        Transition::Complete => {
            // Unblock any queued items blocked by this now that it's complete
            Box::pin(unblock_all(tx, task.id, queue))
                .await
                .context("unblock all tasks")?;

            set_status(tx, task.id, Status::Completed).await.context("set status")?;

            info!("Task marked as complete");
        }
        Transition::Failed { stashed_build_logs } => {
            // Block all queued tasks dependent on this one since it failed
            // to build. A new task needs to be submitted that succeeds to
            // unblock these.
            if task.status.is_open() {
                Box::pin(block_all(tx, task.id, queue))
                    .await
                    .context("block all tasks")?;
            }

            // `failed` can be reached from `blocked` so remove any blockers
            // for this task if they exist since they're no longer relevant
            if !task.blocked_by.is_empty() {
                clear_blockers(tx, task.id).await.context("clear blockers")?;
            }

            // If this failed from within a build, we will have logs to stash
            if let Some(logs) = stashed_build_logs {
                set_log_path(tx, task.id, Some(&logs)).await.context("set status")?;
            }

            set_status(tx, task.id, Status::Failed).await.context("set status")?;

            warn!("Task marked as failed");
        }
        Transition::Cancelled => {
            // Block all queued tasks dependent on this one since it was cancelled.
            // Cancelling is typically required for stuck builds, so this is akin
            // to "failing" and should block dependent tasks
            Box::pin(block_all(tx, task.id, queue))
                .await
                .context("block all tasks")?;

            set_status(tx, task.id, Status::Cancelled).await.context("set status")?;

            info!("Task marked as cancelled");
        }
        Transition::Retry => {
            // Clear all previously associated data from the last time it was built
            // and set as New to kick off the full task lifecycle again
            set_allocated_builder(tx, task.id, None).await.context("set status")?;
            set_log_path(tx, task.id, None).await.context("set status")?;
            set_status(tx, task.id, Status::New).await.context("set status")?;

            info!("Task marked as new for retry");
        }
        Transition::Requeue => {
            // Clear all previously associated data from the last time it was built
            // and set as New to kick off the full task lifecycle again
            set_allocated_builder(tx, task.id, None).await.context("set status")?;
            set_log_path(tx, task.id, None).await.context("set status")?;
            set_status(tx, task.id, Status::New).await.context("set status")?;

            info!("Task requeued");
        }
    }

    Ok(is_valid_transition)
}

#[tracing::instrument(name = "fix_and_create_tasks", skip_all, fields(repository = %repo.name))]
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
        create(tx, project, repo, task, &manager.queue)
            .await
            .context("create task")?;
    }

    // Move tasks no longer in the recipe repo to failed after
    // adding new tasks so that superseded tasks are marked as such
    // and only truly orphaned tasks remain
    fix_orphaned(tx, project, repo, repo_db, &manager.queue)
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
                            task_id = %task.id,
                            build_id = task.build_id,
                            builder = %endpoint,
                            "Builder is building another task, requeuing stuck build"
                        );

                        true
                    }
                }
                Some(builder::Status::Busy) => {
                    warn!(
                        task_id = %task.id,
                        build_id = task.build_id,
                        builder = %endpoint,
                        "Builder is busy with something else, requeuing stuck build"
                    );

                    true
                }
                Some(builder::Status::Idle) => {
                    warn!(
                        task_id = %task.id,
                        build_id = task.build_id,
                        builder = %endpoint,
                        "Builder is idle, requeuing stuck build"
                    );

                    true
                }
                Some(builder::Status::Disconnected) | None => {
                    let build_duration = task.started.map(|started| Utc::now() - started).unwrap_or_default();

                    if build_duration > DISCONNECTED_TIMEOUT {
                        warn!(
                            task_id = %task.id,
                            build_id = task.build_id,
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
                task_id = %task.id,
                build_id = task.build_id,
                "Building task has no set allocated builder, requeuing"
            );

            true
        };

        if requeue {
            transition(tx, task.id, Transition::Requeue, &manager.queue)
                .await
                .context("requeue task")?;
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
    queue: &impl TaskQueue,
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
                task_id = %task.id,
                build_id = task.build_id,
                "Task no longer exists in repository, marking as failed"
            );

            transition(
                tx,
                task.id,
                Transition::Failed {
                    stashed_build_logs: None,
                },
                queue,
            )
            .await
            .context("transition task to failed")?;
        }
    }

    Ok(())
}

/// Set the status of a task_id in the db
async fn set_status(tx: &mut Transaction, task_id: Id, status: Status) -> Result<()> {
    let ended = if !status.is_open() {
        ", ended = unixepoch()"
    }
    // Retry
    else if status == Status::New {
        ", ended = null"
    } else {
        ""
    };

    let started = if status == Status::Building {
        ", started = unixepoch()"
    }
    // Retry
    else if status == Status::New {
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
async fn set_log_path(tx: &mut Transaction, task_id: Id, log_path: Option<&Path>) -> Result<()> {
    sqlx::query(
        "
        UPDATE task
        SET
          log_path = ?,
          updated = unixepoch()
        WHERE task_id = ?;
        ",
    )
    .bind(log_path.map(|p| p.display().to_string()))
    .bind(i64::from(task_id))
    .execute(tx.as_mut())
    .await
    .context("set log_path for task_id={task_id:?}")?;

    debug!(%task_id, "will use log_path='{log_path:?}' for output");

    Ok(())
}

/// Allocate a builder for a task
async fn set_allocated_builder(tx: &mut Transaction, task_id: Id, builder: Option<endpoint::Id>) -> Result<()> {
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

async fn add_blocker(tx: &mut Transaction, task_id: Id, blocker: &Blocker) -> Result<()> {
    let _ = sqlx::query(
        "
        INSERT INTO task_blockers (task_id, blocker)
        VALUES (?,?)
        ON CONFLICT DO NOTHING;
        ",
    )
    .bind(i64::from(task_id))
    .bind(blocker.as_ref())
    .execute(tx.as_mut())
    .await?;

    Ok(())
}

async fn remove_blocker(tx: &mut Transaction, task_id: Id, blocker: &Blocker) -> Result<()> {
    let _ = sqlx::query(
        "
        DELETE FROM task_blockers
        WHERE task_id = ? AND blocker = ?;
        ",
    )
    .bind(i64::from(task_id))
    .bind(blocker.as_ref())
    .execute(tx.as_mut())
    .await?;

    Ok(())
}

async fn clear_blockers(tx: &mut Transaction, task_id: Id) -> Result<()> {
    let _ = sqlx::query(
        "
        DELETE FROM task_blockers
        WHERE task_id = ?;
        ",
    )
    .bind(i64::from(task_id))
    .execute(tx.as_mut())
    .await?;

    Ok(())
}

/// Returns the task blocked by this [`Blocker`]
pub async fn blocked(conn: &mut SqliteConnection, blocker: &Blocker) -> Result<Vec<Id>> {
    let tasks = sqlx::query_as(
        "
        SELECT
          task_id
        FROM
          task_blockers
        WHERE
          blocker = ?;
        ",
    )
    .bind(&blocker.0)
    .fetch_all(conn)
    .await?;

    Ok(tasks)
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
                            trace!(
                                slug = slug(),
                                published = version(published),
                                recipe = version(&meta),
                                "Newer package release already present in index"
                            );
                            continue;
                        } else if published.source_release == meta.source_release {
                            trace!(
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

#[tracing::instrument(skip_all)]
async fn block_all(tx: &mut Transaction, task_id: Id, queue: &impl TaskQueue) -> Result<()> {
    let task_blocker = queue.get(task_id).ok_or_eyre("task missing from queue")?.blocker();

    for dependent in queue.dependents(task_id) {
        transition(
            tx,
            dependent,
            Transition::Blocked {
                blocker: task_blocker.clone(),
            },
            queue,
        )
        .await
        .context("block task")?;
    }

    Ok(())
}

#[tracing::instrument(skip_all)]
async fn unblock_all(tx: &mut Transaction, task_id: Id, queue: &impl TaskQueue) -> Result<()> {
    let task_blocker = queue.get(task_id).ok_or_eyre("task missing from queue")?.blocker();

    let all_blocked = blocked(tx.as_mut(), &task_blocker).await.context("get blocked tasks")?;

    for blocked in all_blocked {
        transition(
            tx,
            blocked,
            Transition::Unblocked {
                blocker: task_blocker.clone(),
            },
            queue,
        )
        .await
        .context("unblock task")?;
    }

    Ok(())
}
