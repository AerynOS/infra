use derive_more::derive::{Display, From, Into};
use http::Uri;
use serde::{Deserialize, Serialize};
use service::database::Transaction;
use sqlx::FromRow;

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
    pub remotes: Vec<Remote>,
}

pub async fn create(
    tx: &mut Transaction,
    project: project::Id,
    name: String,
    arch: String,
    index_uri: Uri,
) -> Result<Profile, sqlx::Error> {
    let (id,): (i64,) = sqlx::query_as(
        "
        INSERT INTO profile
        (
          name,
          arch,
          index_uri,
          project_id
        )
        VALUES (?,?,?,?)
        RETURNING profile_id;
        ",
    )
    .bind(&name)
    .bind(&arch)
    .bind(index_uri.to_string())
    .bind(i64::from(project))
    .fetch_one(tx.as_mut())
    .await?;

    Ok(Profile {
        id: Id(id),
        name,
        arch,
        index_uri,
        remotes: vec![],
    })
}
