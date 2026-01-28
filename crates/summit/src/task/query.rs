use std::sync::LazyLock;

use chrono::{DateTime, TimeZone as _, Utc};
use color_eyre::eyre::{Context, Result};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use sqlx::{Sqlite, SqliteConnection, prelude::FromRow, query::QueryAs, sqlite::SqliteArguments};
use uuid::Uuid;

use crate::{profile, project, repository, use_mock_data};

use super::{Blocker, Id, Status, Task};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, strum::Display, strum::EnumString)]
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "lowercase")]
pub enum SortField {
    Ended,
    Build,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, strum::Display, strum::EnumString)]
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "UPPERCASE")]
pub enum SortOrder {
    Asc,
    Desc,
}

#[derive(Debug, Default)]
pub struct Params {
    id: Option<Id>,
    statuses: Option<Vec<Status>>,
    source_path: Option<String>,
    offset: Option<i64>,
    limit: Option<u32>,
    sort_field: Option<SortField>,
    sort_order: Option<SortOrder>,
    search_query: Option<String>,
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

    pub fn source_path(self, source_path: impl ToString) -> Self {
        Self {
            source_path: Some(source_path.to_string()),
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

    pub fn sort(self, field: SortField, order: SortOrder) -> Self {
        Self {
            sort_field: Some(field),
            sort_order: Some(order),
            ..self
        }
    }

    pub fn search_tasks(self, search: String) -> Self {
        Self {
            search_query: Some(search),
            ..self
        }
    }

    fn where_clause(&self) -> String {
        if self.id.is_some() || self.statuses.is_some() || self.search_query.is_some() {
            let conditions = self
                .id
                .map(|_| "task_id = ?".to_owned())
                .into_iter()
                .chain(self.statuses.as_ref().map(|statuses| {
                    let binds = ",?".repeat(statuses.len()).chars().skip(1).collect::<String>();

                    format!("status IN ({binds})")
                }))
                .chain(self.source_path.is_some().then(|| "source_path = ?".to_owned()))
                .chain(
                    self.search_query
                        .is_some()
                        .then(|| "description LIKE ? COLLATE NOCASE".to_owned()),
                )
                .join(" AND ");

            format!("WHERE {conditions}")
        } else {
            String::default()
        }
    }

    fn sort_order_clause(&self) -> &'static str {
        match (self.sort_field, self.sort_order) {
            (None, _) => "ORDER BY added DESC, task_id DESC",
            (Some(SortField::Ended), Some(SortOrder::Asc)) => {
                "ORDER BY
                (ended IS NULL),
                ended ASC,
                added DESC,
                task_id DESC"
            }
            (Some(SortField::Ended), Some(SortOrder::Desc)) => {
                "ORDER BY
                (ended IS NULL),
                ended DESC,
                added DESC,
                task_id DESC"
            }
            (Some(SortField::Build), Some(SortOrder::Asc)) => {
                "ORDER BY
                    (CASE
                        WHEN started IS NULL OR ended IS NULL THEN 1
                        ELSE 0
                    END),
                    (CASE
                        WHEN started IS NOT NULL AND ended IS NOT NULL
                        THEN ended - started
                        ELSE NULL
                    END) ASC,
                    added DESC,
                    task_id DESC"
            }
            (Some(SortField::Build), Some(SortOrder::Desc)) => {
                "ORDER BY
                    (CASE
                        WHEN started IS NULL OR ended IS NULL THEN 1
                        ELSE 0
                    END),
                    (CASE
                        WHEN started IS NOT NULL AND ended IS NOT NULL
                        THEN ended - started
                        ELSE NULL
                    END) DESC,
                    added DESC,
                    task_id DESC"
            }
            _ => "ORDER BY added DESC, task_id DESC",
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
        if let Some(source_path) = self.source_path.clone() {
            query = query.bind(source_path);
        }
        if let Some(search_query) = self.search_query.clone() {
            let pattern = format!("%{search_query}%");
            query = query.bind(pattern);
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
    if use_mock_data() {
        return Ok(query_mock_data(params));
    }

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
        added: DateTime<Utc>,
        started: Option<DateTime<Utc>>,
        updated: DateTime<Utc>,
        ended: Option<DateTime<Utc>>,
    }

    let where_clause = params.where_clause();
    let limit_offset_clause = params.limit_offset_clause();
    let sort_order_clause = params.sort_order_clause();

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
          added,
          started,
          updated,
          ended
        FROM task
        {where_clause}
        {sort_order_clause}
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
            added: row.added,
            started: row.started,
            updated: row.updated,
            ended: row.ended,
            duration: match (row.started, row.ended) {
                (Some(start), Some(end)) => Some((end - start).num_seconds()),
                _ => None,
            },
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
                task.blocked_by.push(Blocker(blocker));
            }
        }
    }

    Ok(Query {
        count: tasks.len(),
        tasks,
        total: total as usize,
    })
}

pub fn query_mock_data(params: Params) -> Query {
    static MOCK_TASKS: LazyLock<Vec<Task>> = LazyLock::new(|| {
        let a_start = Utc.with_ymd_and_hms(2025, 5, 15, 22, 10, 32).unwrap();
        let a_end = Utc.with_ymd_and_hms(2025, 5, 15, 22, 16, 12).unwrap();

        vec![Task {
            id: 1.into(),
            project_id: 1.into(),
            profile_id: 1.into(),
            repository_id: 1.into(),
            slug: "pkg-a".to_owned(),
            package_id: "a".to_owned(),
            arch: "x86_64".to_owned(),
            build_id: "build-id-a".to_owned(),
            description: "dummy package a".to_owned(),
            commit_ref: "abcdefg".to_owned(),
            source_path: "idk/man".to_owned(),
            status: Status::Completed,
            allocated_builder: None,
            log_path: None,
            blocked_by: vec![],
            added: Utc.with_ymd_and_hms(2025, 5, 15, 22, 0, 0).unwrap(),
            started: Some(a_start),
            updated: a_end,
            ended: Some(a_end),
            duration: Some((a_end - a_start).num_seconds()),
        }]
    });

    let matched_tasks: Vec<_> = MOCK_TASKS
        .iter()
        .filter(|task| {
            params.id.is_none_or(|id| id == task.id)
                && params
                    .statuses
                    .as_deref()
                    .is_none_or(|statuses| statuses.contains(&task.status))
                && params
                    .source_path
                    .as_deref()
                    .is_none_or(|source_path| source_path == task.source_path)
        })
        .collect();

    let total = matched_tasks.len();

    let tasks: Vec<_> = match (params.offset, params.limit) {
        (Some(offset), Some(limit)) => matched_tasks
            .into_iter()
            .skip(offset as usize)
            .take(limit as usize)
            .cloned()
            .collect(),
        (Some(offset), None) => matched_tasks.into_iter().skip(offset as usize).cloned().collect(),
        (None, Some(limit)) => matched_tasks.into_iter().take(limit as usize).cloned().collect(),
        (None, None) => matched_tasks.into_iter().cloned().collect(),
    };

    Query {
        count: tasks.len(),
        tasks,
        total,
    }
}
