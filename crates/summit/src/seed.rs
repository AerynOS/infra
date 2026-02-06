use std::path::Path;

use color_eyre::eyre::{Context, Result};
use http::Uri;
use serde::Deserialize;
use service::database::Transaction;
use tokio::fs;
use tracing::{Span, debug, info};

use crate::{State, profile, project, repository};

#[tracing::instrument(
    skip_all,
    fields(
        path = path.as_ref().display().to_string(),
        project,
    )
)]
pub async fn seed(state: &State, path: impl AsRef<Path>) -> Result<()> {
    let mut tx = state.service_db().begin().await.context("begin db tx")?;

    let existing_projects = project::list(tx.as_mut()).await.context("list projects")?;

    let content = fs::read_to_string(path).await.context("read seed content")?;
    let seed: Seed = toml::from_str(&content).context("decode seed from toml")?;

    for seed in seed.projects {
        let span = Span::current();
        span.record("project", &seed.name);

        let project = if let Some(project) = existing_projects.iter().find(|p| p.slug == seed.slug).cloned() {
            debug!("Project already exists");
            project
        } else {
            let project = project::create(&mut tx, seed.name, seed.slug, seed.summary)
                .await
                .context("create project")?;

            info!("Project created");

            project
        };

        for seed in seed.profiles {
            seed_profile(&mut tx, &project, seed).await.context("seed profile")?;
        }

        for seed in seed.repositories {
            seed_repository(&mut tx, &project, seed)
                .await
                .context("seed repository")?;
        }
    }

    tx.commit().await.context("commit tx")?;

    Ok(())
}

#[tracing::instrument(name = "profile", skip_all, fields(profile = seed.name))]
async fn seed_profile(tx: &mut Transaction, project: &crate::Project, seed: Profile) -> Result<()> {
    let profile = if let Some(profile) = project.profiles.iter().find(|p| p.name == seed.name).cloned() {
        debug!("Profile already exists");

        profile
    } else {
        let profile = profile::create(tx, project.id, seed.name, seed.arch, seed.index_uri)
            .await
            .context("create profile")?;

        info!("Profile created");

        profile
    };

    for seed in seed.remotes {
        seed_remote(tx, &profile, seed).await.context("seed remote")?;
    }

    Ok(())
}

#[tracing::instrument(name = "remote", skip_all, fields(remote = seed.name))]
async fn seed_remote(tx: &mut Transaction, profile: &crate::Profile, seed: Remote) -> Result<()> {
    if profile.remotes.iter().any(|r| r.name == seed.name) {
        debug!("Remote already exists");
    } else {
        profile::remote::create(tx, profile.id, seed.uri, seed.name, seed.priority)
            .await
            .context("create remote")?;

        info!("Remote created");
    }

    Ok(())
}

#[tracing::instrument(name = "repository", skip_all, fields(repository = seed.name))]
async fn seed_repository(tx: &mut Transaction, project: &crate::Project, seed: Repository) -> Result<()> {
    if project.repositories.iter().any(|p| p.name == seed.name) {
        debug!("Repository already exists");
    } else {
        repository::create(tx, project.id, seed.name, seed.summary, seed.uri, seed.branch)
            .await
            .context("create repository")?;

        info!("Repository created");
    }

    Ok(())
}

#[derive(Debug, Deserialize)]
pub struct Seed {
    #[serde(rename = "project")]
    projects: Vec<Project>,
}

#[derive(Debug, Deserialize)]
pub struct Project {
    pub name: String,
    pub slug: String,
    pub summary: String,
    #[serde(rename = "profile")]
    pub profiles: Vec<Profile>,
    #[serde(rename = "repository")]
    pub repositories: Vec<Repository>,
}

#[derive(Debug, Deserialize)]
pub struct Profile {
    pub name: String,
    pub arch: String,
    #[serde(with = "http_serde::uri")]
    pub index_uri: Uri,
    #[serde(default, rename = "remote")]
    pub remotes: Vec<Remote>,
}

#[derive(Debug, Deserialize)]
pub struct Remote {
    pub name: String,
    #[serde(with = "http_serde::uri")]
    pub uri: Uri,
    pub priority: u64,
}

#[derive(Debug, Deserialize)]
pub struct Repository {
    pub name: String,
    pub summary: String,
    #[serde(with = "http_serde::uri")]
    pub uri: Uri,
    pub branch: Option<String>,
}
