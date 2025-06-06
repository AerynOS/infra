use http::Uri;
use service::database::Transaction;

use crate::project;

use super::{Id, Repository, Status};

pub async fn create(
    tx: &mut Transaction,
    project: project::Id,
    name: String,
    summary: String,
    origin_uri: Uri,
    branch: Option<String>,
) -> Result<Repository, sqlx::Error> {
    let id: i64 = sqlx::query_scalar(
        "
        INSERT INTO repository
        (
          name,
          summary,
          origin_uri,
          branch,
          status,
          project_id
        )
        VALUES (?,?,?,?,?,?)
        RETURNING repository_id;
        ",
    )
    .bind(&name)
    .bind(&summary)
    .bind(origin_uri.to_string())
    .bind(branch.as_deref())
    .bind(Status::Fresh.to_string())
    .bind(i64::from(project))
    .fetch_one(tx.as_mut())
    .await?;

    Ok(Repository {
        id: Id(id),
        name,
        summary,
        description: None,
        commit_ref: None,
        origin_uri,
        branch,
        status: Status::Fresh,
    })
}
