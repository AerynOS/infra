use std::path::Path;

use color_eyre::eyre::{Context, Result};
use service::{State, git};
use sqlx::SqliteConnection;
use tokio::fs;
use tracing::{debug, info, warn};

use super::{Repository, Status, set_commit_ref, set_status};

#[tracing::instrument(name = "refresh_repository", skip_all, fields(repository = %repo.name))]
pub async fn refresh(conn: &mut SqliteConnection, state: &State, mut repo: Repository) -> Result<(Repository, bool)> {
    debug!("Refreshing repository");

    let current_ref = match repo.status {
        Status::Fresh => clone_git(&mut *conn, state, &mut repo).await.context("clone git")?,
        // We must have failed previously while cloning or updating. Try again from a fresh clone
        status @ (Status::Cloning | Status::Updating) => {
            warn!(%status, "Previous refresh failed, re-cloning...");
            clone_git(&mut *conn, state, &mut repo).await.context("update git")?
        }
        _ => update_git(&mut *conn, state, &mut repo).await.context("update git")?,
    };

    if Some(&current_ref) != repo.commit_ref.as_ref() {
        let old_ref = repo.commit_ref.clone();

        set_commit_ref(&mut *conn, &mut repo, &current_ref)
            .await
            .context("set commit ref")?;

        info!(old_ref, new_ref = current_ref, "Repository updated");

        Ok((repo, true))
    } else {
        debug!("No change in repository");

        Ok((repo, false))
    }
}

async fn clone_git(conn: &mut SqliteConnection, state: &State, repo: &mut Repository) -> Result<String> {
    debug!("Cloning repository");

    let repo_dir = state.cache_dir.join("repository").join(repo.id.to_string());
    let clone_dir = repo_dir.join("clone");

    let _ = fs::remove_dir_all(&repo_dir).await;
    fs::create_dir_all(&repo_dir).await.context("create repo cache dir")?;

    set_status(conn, repo, Status::Cloning)
        .await
        .context("set status to cloning")?;

    git::mirror(&repo.origin_uri, &clone_dir)
        .await
        .context("clone mirror for repository")?;

    set_status(conn, repo, Status::Idle)
        .await
        .context("set status to cloning")?;

    latest_commit(&clone_dir, repo.branch.as_deref())
        .await
        .context("get latest commit")
}

async fn update_git(conn: &mut SqliteConnection, state: &State, repo: &mut Repository) -> Result<String> {
    debug!("Updating repository");

    let clone_dir = state
        .cache_dir
        .join("repository")
        .join(repo.id.to_string())
        .join("clone");

    set_status(conn, repo, Status::Updating)
        .await
        .context("set status to updating")?;

    git::remote_update(&clone_dir)
        .await
        .context("clone mirror for repository")?;

    set_status(conn, repo, Status::Idle)
        .await
        .context("set status to cloning")?;

    latest_commit(&clone_dir, repo.branch.as_deref())
        .await
        .context("get latest commit")
}

async fn latest_commit(path: &Path, branch: Option<&str>) -> Result<String> {
    Ok(git::rev_parse(path, branch.unwrap_or("HEAD")).await?)
}
