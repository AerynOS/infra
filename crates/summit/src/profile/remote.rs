use serde::{Deserialize, Serialize};
use service::database::Transaction;
use sqlx::types::Json;

use crate::profile::{self, Index};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Remote {
    pub index: Index,
    pub name: String,
    pub priority: u64,
}

pub async fn create(
    tx: &mut Transaction,
    profile: profile::Id,
    index: Index,
    name: String,
    priority: u64,
) -> Result<Remote, sqlx::Error> {
    sqlx::query(
        "
        INSERT INTO profile_remote
        (
          profile_id,
          index_source,
          name,
          priority
        )
        VALUES (?,?,?,?);
        ",
    )
    .bind(i64::from(profile))
    .bind(Json(&index))
    .bind(&name)
    .bind(priority as i64)
    .execute(tx.as_mut())
    .await?;

    Ok(Remote { index, name, priority })
}
