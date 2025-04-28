use chrono::{DateTime, Utc};
use color_eyre::eyre::{Context, Result};
use itertools::Itertools;
use serde::Serialize;
use sqlx::{Sqlite, SqliteConnection, prelude::FromRow, query::QueryAs, sqlite::SqliteArguments};
use uuid::Uuid;

use crate::{profile, project, repository};

use super::{Id, Status, Task};

#[derive(Debug, Default)]
pub struct Params {
    id: Option<Id>,
    statuses: Option<Vec<Status>>,
    offset: Option<i64>,
    limit: Option<u32>,
}

impl Params {
    pub fn id(self, id: Id) -> Self {
        Self { id: Some(id), ..self }
    }

    pub fn statuses(self, statuses: impl IntoIterator<Item = Status>) -> Self {
        Self {
            statuses: Some(statuses.into_iter().collect()),
            ..self
        }
    }

    pub fn offset(self, offset: i64) -> Self {
        Self {
            offset: Some(offset),
            ..self
        }
    }

    pub fn limit(self, limit: u32) -> Self {
        Self {
            limit: Some(limit),
            ..self
        }
    }

    fn where_clause(&self) -> String {
        if self.id.is_some() || self.statuses.is_some() {
            let conditions = self
                .id
                .map(|_| "task_id = ?".to_string())
                .into_iter()
                .chain(self.statuses.as_ref().map(|statuses| {
                    let binds = ",?".repeat(statuses.len()).chars().skip(1).collect::<String>();

                    format!("status IN ({binds})")
                }))
                .join(" AND ");

            format!("WHERE {conditions}")
        } else {
            String::default()
        }
    }

    fn limit_offset_clause(&self) -> &'static str {
        match (self.limit, self.offset) {
            (None, None) => "",
            (None, Some(_)) => "OFFSET ?",
            (Some(_), None) => "LIMIT ?",
            (Some(_), Some(_)) => "LIMIT ?\nOFFSET ?",
        }
    }

    fn bind_where<'a, O>(
        &self,
        mut query: QueryAs<'a, Sqlite, O, SqliteArguments<'a>>,
    ) -> QueryAs<'a, Sqlite, O, SqliteArguments<'a>> {
        if let Some(id) = self.id {
            query = query.bind(i64::from(id));
        }
        if let Some(statuses) = self.statuses.as_ref() {
            for status in statuses {
                query = query.bind(status.to_string());
            }
        }
        query
    }

    fn bind_limit_offset<'a, O>(
        &self,
        mut query: QueryAs<'a, Sqlite, O, SqliteArguments<'a>>,
    ) -> QueryAs<'a, Sqlite, O, SqliteArguments<'a>> {
        if let Some(limit) = self.limit {
            query = query.bind(limit);
        }
        if let Some(offset) = self.offset {
            query = query.bind(offset);
        }
        query
    }
}

#[derive(Debug, Serialize)]
pub struct Query {
    pub tasks: Vec<Task>,
    pub count: usize,
    pub total: usize,
}

pub async fn query(conn: &mut SqliteConnection, params: Params) -> Result<Query> {
    #[derive(FromRow)]
    struct Row {
        #[sqlx(rename = "task_id", try_from = "i64")]
        id: Id,
        #[sqlx(try_from = "i64")]
        project_id: project::Id,
        #[sqlx(try_from = "i64")]
        profile_id: profile::Id,
        #[sqlx(try_from = "i64")]
        repository_id: repository::Id,
        slug: String,
        package_id: String,
        arch: String,
        build_id: String,
        description: String,
        commit_ref: String,
        source_path: String,
        #[sqlx(try_from = "&'a str")]
        status: Status,
        allocated_builder: Option<Uuid>,
        log_path: Option<String>,
        started: DateTime<Utc>,
        updated: DateTime<Utc>,
        ended: Option<DateTime<Utc>>,
    }

    let where_clause = params.where_clause();
    let limit_offset_clause = params.limit_offset_clause();

    let query_str = format!(
        "
        SELECT
          task_id,
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
          status,
          allocated_builder,
          log_path,
          started,
          updated,
          ended
        FROM task
        {where_clause}
        ORDER BY started DESC, task_id DESC
        {limit_offset_clause}
        ",
    );

    let mut query = sqlx::query_as::<_, Row>(&query_str);
    query = params.bind_where(query);
    query = params.bind_limit_offset(query);

    let rows = query.fetch_all(&mut *conn).await.context("fetch tasks")?;

    let query_str = format!(
        "
        SELECT COUNT(*)
        FROM task
        {where_clause}
        "
    );

    let mut query = sqlx::query_as::<_, (i64,)>(&query_str);
    query = params.bind_where(query);

    let (total,) = query.fetch_one(&mut *conn).await.context("fetch tasks count")?;

    let mut tasks = rows
        .into_iter()
        .map(|row| Task {
            id: row.id,
            project_id: row.project_id,
            profile_id: row.profile_id,
            repository_id: row.repository_id,
            slug: row.slug,
            package_id: row.package_id,
            arch: row.arch,
            build_id: row.build_id,
            description: row.description,
            commit_ref: row.commit_ref,
            source_path: row.source_path,
            status: row.status,
            allocated_builder: row.allocated_builder.map(From::from),
            log_path: row.log_path,
            started: row.started,
            updated: row.updated,
            ended: row.ended,
            // Fetched next
            blocked_by: vec![],
        })
        .collect::<Vec<_>>();

    // max number of sqlite params
    for chunk in tasks.chunks_mut(32766) {
        let binds = ",?".repeat(chunk.len()).chars().skip(1).collect::<String>();

        let query_str = format!(
            "
            SELECT
              task_id,
              blocker
            FROM task_blockers
            WHERE task_id IN ({binds});
            ",
        );

        let mut query = sqlx::query_as::<_, (i64, String)>(&query_str);

        for task in chunk.iter() {
            query = query.bind(i64::from(task.id));
        }

        let rows = query.fetch_all(&mut *conn).await.context("fetch task blockers")?;

        for (id, blocker) in rows {
            if let Some(task) = chunk.iter_mut().find(|t| t.id == Id::from(id)) {
                task.blocked_by.push(blocker);
            }
        }
    }

    Ok(Query {
        count: tasks.len(),
        tasks,
        total: total as usize,
    })
}
