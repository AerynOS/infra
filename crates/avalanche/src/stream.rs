use std::{convert::Infallible, sync::Arc, time::Duration};

use color_eyre::eyre::{Context, OptionExt, Result};
use futures_util::TryStreamExt;
use service::{
    Endpoint, State,
    client::{AuthClient, EndpointAuth, SummitServiceClient},
    endpoint::Role,
    error,
    grpc::{
        self,
        collectable::Collectable,
        summit::{
            BuilderBusy, BuilderFinished, BuilderLog, BuilderStatus, BuilderStreamIncoming, BuilderStreamIncomingEvent,
            BuilderUpload, builder_stream_incoming_event, builder_stream_outgoing_event,
        },
    },
    tracing::{OpenTelemetryContext, OpenTelemetrySpanExt},
};
use tokio::{
    select,
    sync::{Mutex, mpsc},
    time,
};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{Instrument, Span, debug, error, info, info_span, warn};

use crate::{build, upload};

#[derive(Clone)]
pub struct Handle {
    sender: mpsc::Sender<BuilderStreamIncoming>,
}

impl Handle {
    pub async fn build_started(&self, task_id: u64) {
        let _ = self
            .sender
            .send(BuilderStreamIncoming {
                event: Some(BuilderStreamIncomingEvent {
                    event: Some(builder_stream_incoming_event::Event::BuildStarted(task_id)),
                }),
                tracing_context: Some(grpc::inject_tracing_context()),
            })
            .await;
    }

    pub async fn build_log(&self, chunk: Vec<u8>) {
        let _ = self
            .sender
            .send(BuilderStreamIncoming {
                event: Some(BuilderStreamIncomingEvent {
                    event: Some(builder_stream_incoming_event::Event::BuildLog(BuilderLog { chunk })),
                }),
                tracing_context: Some(grpc::inject_tracing_context()),
            })
            .await;
    }

    pub async fn build_succeeded(&self, task_id: u64, collectables: Vec<Collectable>) {
        let _ = self
            .sender
            .send(BuilderStreamIncoming {
                event: Some(BuilderStreamIncomingEvent {
                    event: Some(builder_stream_incoming_event::Event::BuildSucceeded(BuilderFinished {
                        task_id,
                        collectables,
                    })),
                }),
                tracing_context: Some(grpc::inject_tracing_context()),
            })
            .await;
    }

    pub async fn build_failed(&self, task_id: u64) {
        let _ = self
            .sender
            .send(BuilderStreamIncoming {
                event: Some(BuilderStreamIncomingEvent {
                    event: Some(builder_stream_incoming_event::Event::BuildFailed(task_id)),
                }),
                tracing_context: Some(grpc::inject_tracing_context()),
            })
            .await;
    }
}

pub async fn run(state: State) -> Result<(), Infallible> {
    loop {
        debug!("Attempting to connect to summit");

        if let Err(e) = connect(&state).await {
            let error = error::chain(&*e);
            error!(%error, "Stream error");

            // TODO: Exponential backoff due to spurious / network errors
            time::sleep(Duration::from_secs(10)).await;
        }
    }
}

async fn connect(state: &State) -> Result<()> {
    let endpoint = Endpoint::list(&mut *state.service_db.acquire().await.context("acquire db conn")?)
        .await
        .context("list endpoints")?
        .into_iter()
        .find(|e| e.role == Role::Hub)
        .ok_or_eyre("no enrolled hub")?;

    let mut client = SummitServiceClient::connect_with_auth(
        endpoint.host_address.clone(),
        EndpointAuth::new(&endpoint, state.service_db.clone(), state.key_pair.clone()),
    )
    .await
    .context("connect summit client")?;

    let (sender, receiver) = mpsc::channel(1);

    let resp = client
        .builder(ReceiverStream::new(receiver))
        .await
        .context("connect summit builder stream")?;

    info!("Connected to summit");

    let mut stream = resp.into_inner();
    let mut interval = time::interval(Duration::from_secs(60));

    let building = Arc::new(Mutex::new(None));

    loop {
        select! {
            _ = interval.tick() => {
                let building = *building.lock().await;

                let _ = sender
                    .send(BuilderStreamIncoming {
                        event: Some(BuilderStreamIncomingEvent {
                            event: Some(builder_stream_incoming_event::Event::Status(BuilderStatus { building })),
                        }),
                        tracing_context: Some(grpc::inject_tracing_context()),
                    })
                    .await;

                debug!(building, "Status reported");
            },
            result = stream.try_next() => {
                if let Some(message) = result.context("stream grpc error")? {
                    let context = message.tracing_context.as_ref().map(grpc::extract_tracing_context).unwrap_or(OpenTelemetryContext::current());
                    let span = info_span!(parent: None, "stream_event");
                    span.set_parent(context);

                    let event = message.event.and_then(|event| event.event).ok_or_eyre("missing stream event")?;

                    handle_stream_event(event, &building, &sender, state).instrument(span).await?;
                } else {
                    break;
                }

            }
        }
    }

    Ok(())
}

async fn handle_stream_event(
    event: builder_stream_outgoing_event::Event,
    building: &Arc<Mutex<Option<u64>>>,
    sender: &mpsc::Sender<BuilderStreamIncoming>,
    state: &State,
) -> Result<()> {
    let span = Span::current();

    match event {
        builder_stream_outgoing_event::Event::Build(request) => {
            let mut build_guard = building.lock().await;

            if let Some(in_progress) = *build_guard {
                warn!("Build already in progress, ignoring");
                let _ = sender
                    .send(BuilderStreamIncoming {
                        event: Some(BuilderStreamIncomingEvent {
                            event: Some(builder_stream_incoming_event::Event::Busy(BuilderBusy {
                                requested_task_id: request.task_id,
                                in_progress_task_id: in_progress,
                            })),
                        }),
                        tracing_context: Some(grpc::inject_tracing_context()),
                    })
                    .await;
                return Ok(());
            }

            *build_guard = Some(request.task_id);

            drop(build_guard);

            tokio::spawn({
                let state = state.clone();
                let handle = Handle { sender: sender.clone() };
                let building = building.clone();

                async move {
                    build(request, state, handle).await;
                    building.lock().await.take();
                }
                .instrument(span)
            });
        }
        builder_stream_outgoing_event::Event::Upload(BuilderUpload { build, token, uri }) => {
            let build = build.ok_or_eyre("missing build message")?;

            tokio::spawn({
                let state = state.clone();

                async move {
                    if let Err(e) = upload(state, build, token, &uri).await {
                        let error = error::chain(&*e);
                        error!(uri, %error, "Failed to upload packages to vessel");
                    }
                }
                .instrument(span)
            });
        }
    }

    Ok(())
}
