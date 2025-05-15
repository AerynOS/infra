use std::{iter, num::NonZeroU32};

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
    pub sort: Option<task::query::SortField>,
    pub order: Option<task::query::SortOrder>,
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

    if let (Some(sort_field), Some(sort_order)) = (selected_sort, selected_order) {
        params = params.sort(sort_field, sort_order);
    }

    let projects = project::list(&mut conn).await.context("list projects")?;
    let endpoints = Endpoint::list(&mut *conn).await.context("list endpoints")?;
    let task_query = task::query(&mut conn, params).await.context("query tasks")?;

    let statuses = task::Status::iter().collect::<Vec<_>>();
    let total_pages = (task_query.total / limit as usize + 1) as u32;

    let side_window = 3;
    let start = page.saturating_sub(side_window).max(1);
    let end = (page + side_window).min(total_pages);
    let pages_to_show = (start..=end).collect::<Vec<u32>>();

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
