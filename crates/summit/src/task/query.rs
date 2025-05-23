use std::sync::LazyLock;

use chrono::{DateTime, TimeZone as _, Utc};
use color_eyre::eyre::{Context, Result};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use sqlx::{Sqlite, SqliteConnection, prelude::FromRow, query::QueryAs, sqlite::SqliteArguments};
use uuid::Uuid;

use crate::{profile, project, repository, use_mock_data};

use super::{Id, Status, Task};

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

    fn where_clause(&self) -> String {
        if self.id.is_some() || self.statuses.is_some() {
            let conditions = self
                .id
                .map(|_| "t.task_id = ?".to_owned())
                .into_iter()
                .chain(self.statuses.as_ref().map(|statuses| {
                    let binds = ",?".repeat(statuses.len()).chars().skip(1).collect::<String>();

                    format!("t.status IN ({binds})")
                }))
                .chain(self.source_path.is_some().then(|| "t.source_path = ?".to_owned()))
                .join(" AND ");

            format!("WHERE {conditions}")
        } else {
            String::default()
        }
    }

    fn sort_order_clause(&self) -> &'static str {
        match (self.sort_field, self.sort_order) {
            (None, _) => "ORDER BY t.added DESC, t.task_id DESC",
            (Some(SortField::Ended), Some(SortOrder::Asc)) => {
                "ORDER BY
                (t.ended IS NULL),
                t.ended ASC,
                t.added DESC,
                t.task_id DESC"
            }
            (Some(SortField::Ended), Some(SortOrder::Desc)) => {
                "ORDER BY
                (t.ended IS NULL),
                t.ended DESC,
                t.added DESC,
                t.task_id DESC"
            }
            (Some(SortField::Build), Some(SortOrder::Asc)) => {
                "ORDER BY
                    (CASE
                        WHEN t.started IS NULL OR t.ended IS NULL THEN 1
                        ELSE 0
                    END),
                    (CASE
                        WHEN t.started IS NOT NULL AND t.ended IS NOT NULL
                        THEN t.ended - t.started
                        ELSE NULL
                    END) ASC,
                    t.added DESC,
                    t.task_id DESC"
            }
            (Some(SortField::Build), Some(SortOrder::Desc)) => {
                "ORDER BY
                    (CASE
                        WHEN t.started IS NULL OR t.ended IS NULL THEN 1
                        ELSE 0
                    END),
                    (CASE
                        WHEN t.started IS NOT NULL AND t.ended IS NOT NULL
                        THEN t.ended - t.started
                        ELSE NULL
                    END) DESC,
                    t.added DESC,
                    t.task_id DESC"
            }
            _ => "ORDER BY t.added DESC, t.task_id DESC",
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
        blockers: Option<String>,
    }

    let where_clause = params.where_clause();
    let limit_offset_clause = params.limit_offset_clause();
    let sort_order_clause = params.sort_order_clause();

    let query_str = format!(
        "
        SELECT
          t.task_id,
          t.project_id,
          t.profile_id,
          t.repository_id,
          t.slug,
          t.package_id,
          t.arch,
          t.build_id,
          t.description,
          t.commit_ref,
          t.source_path,
          t.status,
          t.allocated_builder,
          t.log_path,
          t.added,
          t.started,
          t.updated,
          t.ended,
          GROUP_CONCAT(tb.blocker, '||') AS blockers
        FROM task t
        LEFT JOIN task_blockers tb ON t.task_id = tb.task_id
        {where_clause}
        GROUP BY t.task_id
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
        FROM task t
        {where_clause}
        "
    );

    let mut query = sqlx::query_as::<_, (i64,)>(&query_str);
    query = params.bind_where(query);

    let (total,) = query.fetch_one(&mut *conn).await.context("fetch tasks count")?;

    let tasks = rows
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
            blocked_by: row
                .blockers
                .as_deref()
                .unwrap_or("")
                .split("||")
                .map(|s| s.to_string())
                .collect(),
        })
        .collect::<Vec<_>>();

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

        vec![
            Task {
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
            },
            Task {
                id: 2.into(),
                project_id: 2.into(),
                profile_id: 2.into(),
                repository_id: 2.into(),
                slug: "pkg-b".to_owned(),
                package_id: "b".to_owned(),
                arch: "x86_64".to_owned(),
                build_id: "build-id-b".to_owned(),
                description: "dummy package b".to_owned(),
                commit_ref: "abcdefg".to_owned(),
                source_path: "idk/man".to_owned(),
                status: Status::Blocked,
                allocated_builder: None,
                log_path: None,
                blocked_by: vec![
                    "autocc_x86_64@1/1".to_string(),
                    "polkit_x86_64@1/1".to_string(),
                    "pulseaudio_x86_64@1/1".to_string(),
                ],
                added: Utc.with_ymd_and_hms(2025, 5, 15, 22, 0, 0).unwrap(),
                started: None,
                updated: a_end,
                ended: None,
                duration: None,
            },
        ]
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
