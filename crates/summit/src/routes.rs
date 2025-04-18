use std::num::NonZeroU32;

use axum::{
    extract::{Query, State},
    response::{IntoResponse, Response},
};
use color_eyre::eyre::{self, Context};
use http::StatusCode;
use serde::Deserialize;
use snafu::Snafu;

use crate::{project, task, templates::render_html};

pub async fn index() -> impl IntoResponse {
    render_html("index.html.jinja", ())
}

#[derive(Debug, Deserialize)]
pub struct TasksQuery {
    pub page: Option<NonZeroU32>,
    pub per_page: Option<u32>,
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

    let projects = project::list(&mut conn).await.context("list projects")?;
    let task_query = task::query(&mut conn, task::query::Params::default().offset(offset).limit(limit))
        .await
        .context("query tasks")?;

    let total_pages = task_query.total / limit as usize + 1;

    Ok(render_html(
        "tasks.html.jinja",
        minijinja::context! {
            projects,
            page,
            total_pages,
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
