use std::{convert::Infallible, sync::Arc, time::Duration};

use color_eyre::eyre::{Context, OptionExt, Result};
use futures_util::TryStreamExt;
use service::{
    Endpoint, State,
    client::{AuthClient, EndpointAuth, SummitServiceClient},
    endpoint::Role,
    error,
    grpc::{
        collectable::Collectable,
        summit::{
            BuilderBuild, BuilderBusy, BuilderFinished, BuilderLog, BuilderStatus, BuilderStreamIncoming,
            BuilderUpload, builder_stream_incoming, builder_stream_outgoing,
        },
    },
};
use tokio::{
    select,
    sync::{Mutex, broadcast, mpsc},
    time,
};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, error, info, warn};

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
                event: Some(builder_stream_incoming::Event::BuildStarted(task_id)),
            })
            .await;
    }

    pub async fn build_log(&self, chunk: Vec<u8>) {
        let _ = self
            .sender
            .send(BuilderStreamIncoming {
                event: Some(builder_stream_incoming::Event::BuildLog(BuilderLog { chunk })),
            })
            .await;
    }

    pub async fn build_succeeded(&self, task_id: u64, collectables: Vec<Collectable>) {
        let _ = self
            .sender
            .send(BuilderStreamIncoming {
                event: Some(builder_stream_incoming::Event::BuildSucceeded(BuilderFinished {
                    task_id,
                    collectables,
                })),
            })
            .await;
    }

    pub async fn build_failed(&self, task_id: u64) {
        let _ = self
            .sender
            .send(BuilderStreamIncoming {
                event: Some(builder_stream_incoming::Event::BuildFailed(task_id)),
            })
            .await;
    }
}

pub async fn run(state: State) -> Result<(), Infallible> {
    let building = Arc::new(Mutex::new(None));

    loop {
        debug!("Attempting to connect to summit");

        if let Err(e) = connect(&state, building.clone()).await {
            let error = error::chain(&*e);
            error!(%error, "Stream error");

            // TODO: Exponential backoff due to spurious / network errors
            time::sleep(Duration::from_secs(10)).await;
        }
    }
}

async fn connect(state: &State, building: Arc<Mutex<Option<u64>>>) -> Result<()> {
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
    let (cancel_sender, cancel_receiver) = broadcast::channel::<()>(1);

    loop {
        select! {
            _ = interval.tick() => {
                let building = *building.lock().await;

                let _ = sender
                    .send(BuilderStreamIncoming {
                        event: Some(builder_stream_incoming::Event::Status(BuilderStatus { building })),
                    })
                    .await;

                debug!(building, "Status reported");
            },
            result = stream.try_next() => {
                if let Some(message) = result.context("stream grpc error")? {
                    let event = message.event.ok_or_eyre("missing stream event")?;

                    match event {
                        builder_stream_outgoing::Event::Build(request) => {
                            build_requested(state, &building, &sender, request, &cancel_receiver).await;
                        }
                        builder_stream_outgoing::Event::Upload(BuilderUpload { build, token, uri }) => {
                            let build = build.ok_or_eyre("missing build message")?;

                            tokio::spawn({
                                let state = state.clone();

                                async move {
                                    if let Err(e) = upload(state, build, token, &uri).await {
                                        let error = error::chain(&*e);
                                        error!(uri, %error, "Failed to upload packages to vessel");
                                    }
                                }
                            });
                        }
                        builder_stream_outgoing::Event::CancelBuild(()) => {
                            let _  = cancel_sender.send(());
                        }
                    }
                } else {
                    break;
                }

            }
        }
    }

    Ok(())
}

async fn build_requested(
    state: &State,
    building: &Arc<Mutex<Option<u64>>>,
    sender: &mpsc::Sender<BuilderStreamIncoming>,
    request: BuilderBuild,
    cancel_receiver: &broadcast::Receiver<()>,
) {
    let mut build_guard = building.lock().await;

    if let Some(in_progress) = *build_guard {
        warn!("Build already in progress, ignoring");

        let _ = sender
            .send(BuilderStreamIncoming {
                event: Some(builder_stream_incoming::Event::Busy(BuilderBusy {
                    requested_task_id: request.task_id,
                    in_progress_task_id: in_progress,
                })),
            })
            .await;

        return;
    }

    *build_guard = Some(request.task_id);

    drop(build_guard);

    tokio::spawn({
        let state = state.clone();
        let sender = sender.clone();
        let handle = Handle { sender: sender.clone() };
        let building = building.clone();
        let mut cancel_receiver = cancel_receiver.resubscribe();

        async move {
            select! {
                biased;

                _ = cancel_receiver.recv() => {
                    info!("Build cancelled");

                    let _ = sender
                        .send(BuilderStreamIncoming {
                            event: Some(builder_stream_incoming::Event::Status(
                                BuilderStatus { building: None })
                            ),
                        })
                        .await;
                }

                _ = build(request, state, handle) => {}
            }

            building.lock().await.take();
        }
    });
}
