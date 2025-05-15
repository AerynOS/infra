use std::{cmp::Ordering, iter, num::NonZeroU32};

use axum::{
    extract::{Query, State},
    response::{IntoResponse, Response},
};
use color_eyre::eyre::{self, Context};
use http::StatusCode;
use serde::Deserialize;
use service::Endpoint;
use snafu::Snafu;
use strum::IntoEnumIterator;

use crate::{project, task, templates::render_html};

pub async fn index(State(state): State<service::State>) -> Result<impl IntoResponse, Error> {
    let mut conn = state.service_db.acquire().await.context("acquire db conn")?;
    let params = task::query::Params::default().offset(0).limit(10);
    let task_query = task::query(&mut conn, params).await.context("query tasks")?;

    Ok(render_html(
        "index.html.jinja",
        minijinja::context! {
            tasks => task_query.tasks,
        },
    ))
}

#[derive(Debug, Deserialize)]
pub struct TasksQuery {
    pub page: Option<NonZeroU32>,
    pub per_page: Option<u32>,
    pub status: Option<task::Status>,
    pub sort: Option<String>,
    pub order: Option<String>,
}

pub async fn tasks(
    State(state): State<service::State>,
    Query(query): Query<TasksQuery>,
) -> Result<impl IntoResponse, Error> {
    const DEFAULT_LIMIT: u32 = 25;
    const MAX_LIMIT: u32 = 100;

    let mut conn = state.service_db.acquire().await.context("acquire db conn")?;

    let page = query.page.map(u32::from).unwrap_or(1);

    let limit = query.per_page.unwrap_or(DEFAULT_LIMIT).min(MAX_LIMIT);
    let offset = (page - 1) as i64 * limit as i64;

    let mut params = task::query::Params::default().offset(offset).limit(limit);

    let selected_status = query.status;
    let selected_sort = query.sort;
    let selected_order = query.order;

    if let Some(status) = selected_status {
        params = params.statuses(iter::once(status));
    }

    let projects = project::list(&mut conn).await.context("list projects")?;
    let endpoints = Endpoint::list(&mut *conn).await.context("list endpoints")?;
    let mut task_query = task::query(&mut conn, params).await.context("query tasks")?;

    let statuses = task::Status::iter().collect::<Vec<_>>();
    let total_pages = (task_query.total / limit as usize + 1) as u32;

    let side_window = 3;
    let start = page.saturating_sub(side_window).max(1);
    let end = (page + side_window).min(total_pages);
    let pages_to_show = (start..=end).collect::<Vec<u32>>();

    if let (Some(sort), Some(order)) = (&selected_sort, &selected_order) {
        let is_asc = order == "asc";

        match sort.as_str() {
            "ended" => sort_tasks_by_ended(&mut task_query, is_asc),
            "build" => sort_tasks_by_build_time(&mut task_query, is_asc),
            _ => {}
        }
    }

    Ok(render_html(
        "tasks.html.jinja",
        minijinja::context! {
            projects,
            endpoints,
            statuses,
            selected_status,
            page,
            total_pages,
            limit,
            pages_to_show,
            tasks => task_query.tasks,
            total => task_query.total,
        },
    ))
}

pub async fn fallback() -> impl IntoResponse {
    render_html("404.html.jinja", ())
}

fn sort_tasks_by_ended(task_query: &mut task::query::Query, is_asc: bool) {
    task_query.tasks.sort_by(|a, b| match (a.ended, b.ended) {
        (Some(a_time), Some(b_time)) => {
            if is_asc {
                a_time.cmp(&b_time)
            } else {
                b_time.cmp(&a_time)
            }
        }
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    });
}

fn sort_tasks_by_build_time(task_query: &mut task::query::Query, is_asc: bool) {
    task_query.tasks.sort_by(|a, b| match (a.duration, b.duration) {
        (Some(a_duration), Some(b_duration)) => {
            if is_asc {
                a_duration.cmp(&b_duration)
            } else {
                b_duration.cmp(&a_duration)
            }
        }
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    });
}

#[derive(Debug, Snafu)]
#[snafu(transparent)]
pub struct Error {
    source: eyre::Report,
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        // TODO: Error page template?
        (StatusCode::INTERNAL_SERVER_ERROR, "Internal Server Error").into_response()
    }
}
