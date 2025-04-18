use http::Uri;
use serde::Serialize;
use service::database::Transaction;

use crate::profile;

#[derive(Debug, Clone, Serialize)]
pub struct Remote {
    #[serde(with = "http_serde::uri")]
    pub index_uri: Uri,
    pub name: String,
    pub priority: u64,
}

pub async fn create(
    tx: &mut Transaction,
    profile: profile::Id,
    index_uri: Uri,
    name: String,
    priority: u64,
) -> Result<Remote, sqlx::Error> {
    sqlx::query(
        "
        INSERT INTO profile_remote
        (
          profile_id,
          index_uri,
          name,
          priority
        )
        VALUES (?,?,?,?);
        ",
    )
    .bind(i64::from(profile))
    .bind(index_uri.to_string())
    .bind(&name)
    .bind(priority as i64)
    .execute(tx.as_mut())
    .await?;

    Ok(Remote {
        index_uri,
        name,
        priority,
    })
}
