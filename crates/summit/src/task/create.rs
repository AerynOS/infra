use std::collections::HashSet;

use color_eyre::eyre::{Context, OptionExt, Result};
use service::database::Transaction;
use strum::IntoEnumIterator;
use tracing::{Span, info, warn};

use super::{MissingTask, Status, TaskQueue, query, version};
use crate::{Project, Repository, task};

#[tracing::instrument(name = "create_task", skip_all, fields(slug, build_id, version))]
pub(super) async fn create(
    tx: &mut Transaction,
    project: &Project,
    repository: &Repository,
    task @ MissingTask { profile, meta, .. }: &MissingTask<'_>,
    queue: &impl TaskQueue,
) -> Result<()> {
    let build_id = format!(
        "{} / {} / {}-{}-{}_{}-{}",
        project.slug,
        repository.name,
        meta.source_id,
        meta.version_identifier,
        meta.source_release,
        meta.build_release,
        profile.arch
    );
    let slug = format!("~/{}/{}/{}", project.slug, repository.name, meta.name);

    let span = Span::current();
    span.record("build_id", &build_id);
    span.record("slug", &slug);
    span.record("version", version(meta));

    let exists: Option<i64> = sqlx::query_scalar(
        "
        SELECT task_id
        FROM task
        WHERE build_id = ?
        ",
    )
    .bind(&build_id)
    .fetch_optional(tx.as_mut())
    .await
    .context("lookup existing task")?;

    // Task already created, do nothing
    if exists.is_some() {
        warn!("Task already created, skipping");
        return Ok(());
    }

    let source_path = meta.uri.clone().ok_or_eyre("missing relative recipe path")?;

    let superseded_tasks = query(
        tx.as_mut(),
        query::Params::default()
            .statuses(Status::iter().filter(|status| status.is_open() && !status.is_in_progress()))
            .source_path(source_path.clone()),
    )
    .await
    .context("find superseded tasks")?
    .tasks;

    let task_id: task::Id = sqlx::query_scalar::<_, i64>(
        "
        INSERT INTO task
        (
          project_id,
          profile_id,
          repository_id,
          slug,
          package_id,
          arch,
          build_id,
          description,
          commit_ref,
          source_path,
          status
        )
        VALUES (?,?,?,?,?,?,?,?,?,?,?)
        RETURNING task_id;
        ",
    )
    .bind(i64::from(project.id))
    .bind(i64::from(profile.id))
    .bind(i64::from(repository.id))
    .bind(slug)
    .bind(meta.id().to_string())
    .bind(&profile.arch)
    .bind(&build_id)
    .bind(task.description())
    .bind(repository.commit_ref.as_deref().ok_or_eyre("missing repo commit ref")?)
    .bind(source_path)
    .bind(Status::New.to_string())
    .fetch_one(tx.as_mut())
    .await
    .context("insert task")?
    .into();

    // Mark all superseded tasks as superseded and
    // migrate their blockers to the new task, if any
    if !superseded_tasks.is_empty() {
        let mut blockers = HashSet::new();

        for superseded_task in superseded_tasks {
            info!(
                %task_id,
                superceded_build_id = superseded_task.build_id,
                "Task superseded by newer build"
            );

            task::transition(
                tx,
                superseded_task.id,
                task::Transition::Superceded { by: task_id },
                queue,
            )
            .await
            .context("transition task superceded")?;

            blockers.extend(superseded_task.blocked_by);
        }

        if !blockers.is_empty() {
            let num_blockers = blockers.len();

            for blocker in blockers {
                task::transition(tx, task_id, task::Transition::Blocked { blocker }, queue)
                    .await
                    .context("transition task to blocked")?;
            }

            info!(
                %task_id,
                num_blockers,
                "Task set as blocked due to superseded tasks being blocked"
            );
        }
    }

    info!(%task_id, "Task created");

    Ok(())
}
