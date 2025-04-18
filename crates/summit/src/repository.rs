use derive_more::derive::{Display, From, Into};
use http::Uri;
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, SqliteConnection};

pub use self::create::create;
pub use self::refresh::refresh;
pub use self::reindex::reindex;

mod create;
mod refresh;
mod reindex;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, From, Into, Display, FromRow)]
pub struct Id(i64);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Repository {
    pub id: Id,
    pub name: String,
    pub summary: String,
    pub description: Option<String>,
    pub commit_ref: Option<String>,
    #[serde(with = "http_serde::uri")]
    pub origin_uri: Uri,
    pub branch: Option<String>,
    pub status: Status,
}

/// Status of the repository
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, strum::Display, strum::EnumString)]
#[serde(rename_all = "kebab-case")]
#[strum(serialize_all = "kebab-case")]
pub enum Status {
    /// Never cloned before
    Fresh,
    /// Cloning for the first time
    Cloning,
    /// Updating git ref
    Updating,
    /// Indexing for updates
    Indexing,
    /// Doing nothing
    Idle,
}

pub async fn set_status(conn: &mut SqliteConnection, repo: &mut Repository, status: Status) -> Result<(), sqlx::Error> {
    sqlx::query(
        "
        UPDATE repository
        SET status = ?
        WHERE repository_id = ?;
        ",
    )
    .bind(status.to_string())
    .bind(i64::from(repo.id))
    .execute(&mut *conn)
    .await?;

    repo.status = status;

    Ok(())
}

pub async fn set_commit_ref(
    conn: &mut SqliteConnection,
    repo: &mut Repository,
    commit_ref: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "
        UPDATE repository
        SET commit_ref = ?
        WHERE repository_id = ?;
        ",
    )
    .bind(commit_ref)
    .bind(i64::from(repo.id))
    .execute(&mut *conn)
    .await?;

    repo.commit_ref = Some(commit_ref.to_owned());

    Ok(())
}

pub async fn set_description(
    conn: &mut SqliteConnection,
    repo: &mut Repository,
    description: String,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "
        UPDATE repository
        SET description = ?
        WHERE repository_id = ?;
        ",
    )
    .bind(&description)
    .bind(i64::from(repo.id))
    .execute(&mut *conn)
    .await?;

    repo.description = Some(description);

    Ok(())
}
