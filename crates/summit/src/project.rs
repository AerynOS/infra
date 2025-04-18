use derive_more::derive::{Display, From, Into};
use http::Uri;
use serde::{Deserialize, Serialize};
use service::database::Transaction;
use sqlx::{FromRow, SqliteConnection};

use crate::{
    Profile, Repository,
    profile::{self, Remote},
    repository,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, From, Into, Display, FromRow)]
pub struct Id(i64);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Project {
    pub id: Id,
    pub name: String,
    pub slug: String,
    pub summary: String,
    pub profiles: Vec<Profile>,
    pub repositories: Vec<Repository>,
}

pub async fn create(tx: &mut Transaction, name: String, slug: String, summary: String) -> Result<Project, sqlx::Error> {
    let (id,): (i64,) = sqlx::query_as(
        "
        INSERT INTO project
        (
          name,
          slug,
          summary
        )
        VALUES (?,?,?)
        RETURNING project_id;
        ",
    )
    .bind(&name)
    .bind(&slug)
    .bind(&summary)
    .fetch_one(tx.as_mut())
    .await?;

    Ok(Project {
        id: Id(id),
        name,
        slug,
        summary,
        profiles: vec![],
        repositories: vec![],
    })
}

pub async fn list(conn: &mut SqliteConnection) -> Result<Vec<Project>, sqlx::Error> {
    #[derive(FromRow)]
    struct ProjectRow {
        #[sqlx(rename = "project_id", try_from = "i64")]
        id: Id,
        name: String,
        slug: String,
        summary: String,
    }

    #[derive(FromRow)]
    struct ProfileRow {
        #[sqlx(rename = "profile_id", try_from = "i64")]
        id: profile::Id,
        name: String,
        arch: String,
        #[sqlx(try_from = "String")]
        index_uri: Uri,
        #[sqlx(try_from = "i64")]
        project_id: Id,
    }

    #[derive(FromRow)]
    struct ProfileRemoteRow {
        #[sqlx(rename = "profile_id", try_from = "i64")]
        profile_id: profile::Id,
        #[sqlx(try_from = "String")]
        index_uri: Uri,
        name: String,
        priority: i64,
    }

    #[derive(FromRow)]
    struct RepositoryRow {
        #[sqlx(rename = "repository_id", try_from = "i64")]
        id: repository::Id,
        name: String,
        summary: String,
        description: Option<String>,
        commit_ref: Option<String>,
        #[sqlx(try_from = "String")]
        origin_uri: Uri,
        branch: Option<String>,
        #[sqlx(try_from = "&'a str")]
        status: repository::Status,
        #[sqlx(try_from = "i64")]
        project_id: Id,
    }

    let rows = sqlx::query_as::<_, ProjectRow>(
        "
        SELECT
          project_id,
          name,
          slug,
          summary
        FROM
          project;
        ",
    )
    .fetch_all(&mut *conn)
    .await?;

    let mut projects = rows
        .into_iter()
        .map(|row| Project {
            id: row.id,
            name: row.name,
            slug: row.slug,
            summary: row.summary,
            profiles: vec![],
            repositories: vec![],
        })
        .collect::<Vec<_>>();

    for row in sqlx::query_as::<_, ProfileRow>(
        "
        SELECT
          profile_id,
          name,
          arch,
          index_uri,
          project_id
        FROM
          profile;
        ",
    )
    .fetch_all(&mut *conn)
    .await?
    {
        if let Some(project) = projects.iter_mut().find(|p| p.id == row.project_id) {
            project.profiles.push(Profile {
                id: row.id,
                name: row.name,
                arch: row.arch,
                index_uri: row.index_uri,
                remotes: vec![],
            });
        }
    }

    for row in sqlx::query_as::<_, ProfileRemoteRow>(
        "
        SELECT
          profile_id,
          index_uri,
          name,
          priority
        FROM
          profile_remote;
        ",
    )
    .fetch_all(&mut *conn)
    .await?
    {
        if let Some(profile) = projects
            .iter_mut()
            .find_map(|p| p.profiles.iter_mut().find(|p| p.id == row.profile_id))
        {
            profile.remotes.push(Remote {
                index_uri: row.index_uri,
                name: row.name,
                priority: row.priority as u64,
            });
        }
    }

    for row in sqlx::query_as::<_, RepositoryRow>(
        "
        SELECT
          repository_id,
          name,
          summary,
          description,
          commit_ref,
          origin_uri,
          branch,
          status,
          project_id
        FROM
          repository;
        ",
    )
    .fetch_all(conn)
    .await?
    {
        if let Some(project) = projects.iter_mut().find(|p| p.id == row.project_id) {
            project.repositories.push(Repository {
                id: row.id,
                name: row.name,
                summary: row.summary,
                description: row.description,
                commit_ref: row.commit_ref,
                origin_uri: row.origin_uri,
                branch: row.branch,
                status: row.status,
            });
        }
    }

    Ok(projects)
}
