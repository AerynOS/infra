use std::{convert::Infallible, iter, num::NonZeroU32, sync::Arc, time::Duration};

use axum::{
    extract::{self, Query},
    response::{IntoResponse, Response, Sse, sse},
};
use color_eyre::eyre::{self, Context, OptionExt, eyre};
use futures_util::{future, stream};
use http::{StatusCode, Uri, header};
use serde::Deserialize;
use service::Endpoint;
use snafu::Snafu;
use strum::IntoEnumIterator;
use tokio::sync::{broadcast, mpsc};
use tokio_stream::StreamExt;
use tracing::{error, info, warn};

use crate::{State, manager, project, task, template};

pub async fn index(extract::State(state): extract::State<Arc<State>>) -> Result<impl IntoResponse, Error> {
    let mut conn = state.service.service_db.acquire().await.context("acquire db conn")?;

    let task::Query { tasks, .. } = task::query(
        &mut conn,
        task::query::Params::default()
            .limit(10)
            .sort(task::query::SortField::Updated, task::query::SortOrder::Desc),
    )
    .await
    .context("query tasks")?;

    let task::Query {
        tasks: building_tasks, ..
    } = task::query(
        &mut conn,
        task::query::Params::default().statuses(Some(task::Status::Building)),
    )
    .await
    .context("query tasks")?;

    let endpoints = Endpoint::list(&mut *conn).await.context("list endpoints")?;

    let _builders_guard = state.builders.load();
    let builders = _builders_guard.as_slice();

    Ok(template::render_response(
        "index.html.jinja",
        minijinja::context! {
            endpoints,
            tasks,
            building_tasks,
            builders,
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
    pub search_tasks: Option<String>,
}

pub async fn tasks(
    extract::State(state): extract::State<Arc<State>>,
    Query(query): Query<TasksQuery>,
) -> Result<impl IntoResponse, Error> {
    Ok(template::render_response(
        "tasks.html.jinja",
        tasks_context(&state, &query).await?,
    ))
}

async fn tasks_context(state: &State, query: &TasksQuery) -> Result<minijinja::Value, Error> {
    const DEFAULT_LIMIT: u32 = 25;
    const MAX_LIMIT: u32 = 100;

    let mut conn = state.service.service_db.acquire().await.context("acquire db conn")?;

    let page = query.page.map(u32::from).unwrap_or(1);

    let limit = query.per_page.unwrap_or(DEFAULT_LIMIT).min(MAX_LIMIT);
    let offset = (page - 1) as i64 * limit as i64;

    let mut params = task::query::Params::default().offset(offset).limit(limit);

    let selected_status = query.status;
    let selected_sort = query.sort;
    let selected_order = query.order;
    let search_tasks = &query.search_tasks;

    if let Some(status) = selected_status {
        params = params.statuses(iter::once(status));
    }

    if let (Some(sort_field), Some(sort_order)) = (selected_sort, selected_order) {
        params = params.sort(sort_field, sort_order);
    }

    if let Some(search) = search_tasks.clone() {
        let truncated = search.chars().take(50).collect();
        params = params.search_tasks(truncated);
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

    Ok(minijinja::context! {
        projects,
        endpoints,
        statuses,
        selected_status,
        selected_sort,
        selected_order,
        page,
        total_pages,
        limit,
        search_tasks,
        pages_to_show,
        tasks => task_query.tasks,
        total => task_query.total,
    })
}

pub async fn queue(extract::State(state): extract::State<Arc<State>>) -> impl IntoResponse {
    let data = state.queue_json_view.load();

    template::render_response(
        "queue.html.jinja",
        minijinja::context! {
            nodes => &data.nodes,
            links => &data.links,
        },
    )
}

pub async fn sse(
    extract::State(state): extract::State<Arc<State>>,
    req: extract::Request,
) -> Result<impl IntoResponse, Error> {
    let referer = req
        .headers()
        .get(header::REFERER)
        .ok_or_eyre("missing referer")?
        .to_str()
        .context("non-string referer header")?;
    let referer_uri = referer.parse::<Uri>().context("parse referer header as URI")?;

    enum Page {
        Index,
        Tasks(TasksQuery),
        Queue,
    }

    struct State {
        receiver: broadcast::Receiver<SseEvent>,
        page: Page,
        app_state: Arc<crate::State>,
    }

    let page = match referer_uri.path().trim_end_matches('/') {
        // Index
        "" => Page::Index,
        "/tasks" => {
            let Query(query) = Query::<TasksQuery>::try_from_uri(&referer_uri).context("")?;

            Page::Tasks(query)
        }
        "/queue" => Page::Queue,
        path => return Err(eyre!("invalid referer path for sse request: {path}").into()),
    };

    let receiver = state.sse_receiver.resubscribe();

    Ok(Sse::new(
        stream::once(future::ready(Result::<_, Error>::Ok(sse::Event::default()))).chain(stream::unfold(
            State {
                receiver,
                page,
                app_state: state.clone(),
            },
            |mut state| async move {
                loop {
                    match state.receiver.recv().await {
                        Ok(event) => match event {
                            // Filter for relevant events based on the clients page
                            SseEvent::IndexPageBuilders(event) | SseEvent::IndexPageRecentEvents(event)
                                if matches!(state.page, Page::Index) =>
                            {
                                return Some((Ok(event), state));
                            }
                            SseEvent::IndexPageBuilders(_) | SseEvent::IndexPageRecentEvents(_) => continue,
                            SseEvent::QueuePageQueueJson(event) if matches!(state.page, Page::Queue) => {
                                return Some((Ok(event), state));
                            }
                            SseEvent::QueuePageQueueJson(_) => continue,
                            SseEvent::TasksPageTasks => {
                                if let Page::Tasks(query) = &state.page {
                                    let result = tasks_context(&state.app_state, query)
                                        .await
                                        .context("make tasks jinja context")
                                        .and_then(|value| {
                                            template::render("tasks-table.html.jinja", value)
                                                .context("render tasks-table jinja template")
                                        });

                                    match result {
                                        Ok(template) => {
                                            let event = sse::Event::default().event("tasks").data(template);

                                            return Some((Ok(event), state));
                                        }
                                        Err(err) => {
                                            error!(
                                                error = format!("{err:#}"),
                                                "Failed to render tasks table template in sse stream"
                                            );
                                            continue;
                                        }
                                    }
                                } else {
                                    continue;
                                }
                            }
                        },
                        Err(broadcast::error::RecvError::Lagged(skipped_messages)) => {
                            warn!(%skipped_messages, "Sse receiver is lagged");
                            continue;
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            return None;
                        }
                    }
                }
            },
        )),
    )
    // This is the default keep-alive, but adding it explicitly so its
    // obvious what is being used & we can update easily if needed
    .keep_alive(sse::KeepAlive::new().interval(Duration::from_secs(15))))
}

#[derive(Clone)]
pub enum SseEvent {
    // All streams get the same data so we can precompute it
    IndexPageBuilders(sse::Event),
    IndexPageRecentEvents(sse::Event),
    QueuePageQueueJson(sse::Event),

    // All streams have a unique view of tasks based on params
    // & will need to be computed per stream
    TasksPageTasks,
}

/// Worker task which translates backend events into SSE updates & publishes
/// them to all connected SSE streams
pub async fn sse_worker(
    state: Arc<State>,
    mut manager_events: mpsc::Receiver<manager::Event>,
    sse_sender: broadcast::Sender<SseEvent>,
) -> Result<(), Infallible> {
    let mut worker = async move || -> Result<(), Error> {
        while let Some(event) = manager_events.recv().await {
            let mut sse_events = vec![];

            match event {
                manager::Event::QueueRecomputed => {
                    sse_events.push(SseEvent::QueuePageQueueJson(
                        sse::Event::default()
                            .event("queue-json")
                            .json_data(&*state.queue_json_view.load())
                            .context("serialize queue json view")?,
                    ));
                }
                manager::Event::TasksUpdated => {
                    sse_events.push(SseEvent::TasksPageTasks);

                    let mut conn = state.service.service_db.acquire().await.context("acquire db conn")?;

                    let task::Query { tasks, .. } = task::query(
                        &mut conn,
                        task::query::Params::default()
                            .limit(10)
                            .sort(task::query::SortField::Updated, task::query::SortOrder::Desc),
                    )
                    .await
                    .context("query tasks")?;

                    let endpoints = Endpoint::list(&mut *conn).await.context("list endpoints")?;

                    let template = template::render(
                        "index-recent-events.html.jinja",
                        minijinja::context! {
                            tasks,
                            endpoints,
                        },
                    )
                    .context("render index-recent-events jinja template")?;

                    sse_events.push(SseEvent::IndexPageRecentEvents(
                        sse::Event::default().event("recent-events").data(template),
                    ));
                }
                manager::Event::BuildersUpdated => {
                    let mut conn = state.service.service_db.acquire().await.context("acquire db conn")?;

                    let _builders_guard = state.builders.load();
                    let builders = _builders_guard.as_slice();

                    let task::Query {
                        tasks: building_tasks, ..
                    } = task::query(
                        &mut conn,
                        task::query::Params::default().statuses(Some(task::Status::Building)),
                    )
                    .await
                    .context("query tasks")?;

                    let template = template::render(
                        "index-builders.html.jinja",
                        minijinja::context! {
                            builders,
                            building_tasks,
                        },
                    )
                    .context("render index-builders jinja template")?;

                    sse_events.push(SseEvent::IndexPageBuilders(
                        sse::Event::default().event("builders").data(template),
                    ));
                }
            }

            for event in sse_events {
                // We always have a receiver channel in State, so this
                // would only error out during app shutdown
                if sse_sender.send(event).is_err() {
                    break;
                }
            }
        }

        Ok(())
    };

    while let Err(err) = worker().await {
        error!(error = format!("{err:#}"), "Sse worker thread failed with error");
    }

    info!("Sse worker exiting");

    Ok(())
}

pub async fn fallback() -> impl IntoResponse {
    template::render_response("404.html.jinja", ())
}

#[derive(Debug, Snafu)]
#[snafu(transparent)]
pub struct Error {
    source: eyre::Report,
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        // TODO: Add this as response extension & add proper error handling
        // middleware that can extract the error & log it w/ the request data
        error!(error = format!("{self:#}"), "Request failed");

        // TODO: Error page template?
        (StatusCode::INTERNAL_SERVER_ERROR, "Internal Server Error").into_response()
    }
}
