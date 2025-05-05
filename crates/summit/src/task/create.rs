use color_eyre::eyre::{Context, OptionExt, Result};
use moss::package::Meta;
use service::database::Transaction;
use strum::IntoEnumIterator;
use tracing::{Span, info, warn};

use super::{Status, query, set_status};
use crate::{Profile, Project, Repository};

#[tracing::instrument(name = "create_task", skip_all, fields(slug, build_id, version))]
pub async fn create(
    tx: &mut Transaction,
    project: &Project,
    profile: &Profile,
    repository: &Repository,
    meta: &Meta,
    description: String,
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
    span.record(
        "version",
        format!("{}-{}", meta.version_identifier, meta.source_release),
    );

    let exists = sqlx::query_as::<_, (i64,)>(
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

    let superseded = query(
        tx.as_mut(),
        query::Params::default()
            .statuses(Status::iter().filter(|status| match status {
                Status::New | Status::Blocked => true,
                // Tasks already building / publishing shouldn't get cancelled & should have their
                // lifecycle finished since they're already in-flight
                Status::Building | Status::Publishing | Status::Completed | Status::Failed | Status::Superseded => {
                    false
                }
            }))
            .source_path(source_path.clone()),
    )
    .await
    .context("find superseded tasks")?
    .tasks;

    for task in superseded {
        info!(old = task.build_id, new = build_id, "Task superceded by newer build");

        set_status(tx, task.id, Status::Superseded)
            .await
            .context("set task as superseded")?;
    }

    sqlx::query(
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
        VALUES (?,?,?,?,?,?,?,?,?,?,?);
        ",
    )
    .bind(i64::from(project.id))
    .bind(i64::from(profile.id))
    .bind(i64::from(repository.id))
    .bind(slug)
    .bind(meta.id().to_string())
    .bind(&profile.arch)
    .bind(build_id)
    .bind(description)
    .bind(repository.commit_ref.as_deref().ok_or_eyre("missing repo commit ref")?)
    .bind(source_path)
    .bind(Status::New.to_string())
    .execute(tx.as_mut())
    .await
    .context("insert task")?;

    info!("Task created");

    Ok(())
}
