use derive_more::derive::{Display, From, Into};
use http::Uri;
use serde::{Deserialize, Serialize};
use service::database::Transaction;
use sqlx::{FromRow, SqliteConnection};

use crate::project;

pub use self::refresh::refresh;
pub use self::remote::Remote;

pub mod refresh;
pub mod remote;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, From, Into, Display, FromRow)]
pub struct Id(i64);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Profile {
    pub id: Id,
    pub name: String,
    pub arch: String,
    #[serde(with = "http_serde::uri")]
    pub index_uri: Uri,
    pub status: Status,
    pub remotes: Vec<Remote>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, strum::Display, strum::EnumString, Default)]
#[serde(rename_all = "kebab-case")]
#[strum(serialize_all = "kebab-case")]
pub enum Status {
    /// Not yet indexed
    #[default]
    Fresh,
    /// Refreshing latest index file
    Refreshing,
    /// Fully indexed
    Indexed,
}

pub async fn create(
    tx: &mut Transaction,
    project: project::Id,
    name: String,
    arch: String,
    index_uri: Uri,
) -> Result<Profile, sqlx::Error> {
    let id: i64 = sqlx::query_scalar(
        "
        INSERT INTO profile
        (
          name,
          arch,
          index_uri,
          status,
          project_id
        )
        VALUES (?,?,?,?,?)
        RETURNING profile_id;
        ",
    )
    .bind(&name)
    .bind(&arch)
    .bind(index_uri.to_string())
    .bind(Status::Fresh.to_string())
    .bind(i64::from(project))
    .fetch_one(tx.as_mut())
    .await?;

    Ok(Profile {
        id: Id(id),
        name,
        arch,
        index_uri,
        status: Status::Fresh,
        remotes: vec![],
    })
}

pub async fn set_status(conn: &mut SqliteConnection, profile: &mut Profile, status: Status) -> Result<(), sqlx::Error> {
    sqlx::query(
        "
        UPDATE profile
        SET status = ?
        WHERE profile_id = ?;
        ",
    )
    .bind(status.to_string())
    .bind(i64::from(profile.id))
    .execute(&mut *conn)
    .await?;

    profile.status = status;

    Ok(())
}
