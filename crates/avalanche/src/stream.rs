use std::{convert::Infallible, sync::Arc, time::Duration};

use color_eyre::eyre::{Context, OptionExt, Result};
use futures_util::{TryStreamExt, future::join_all};
use service::{
    Endpoint, State,
    client::{AuthClient, EndpointAuth, SummitServiceClient},
    endpoint::{self, Role, enrollment::HubTarget},
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
    sync::{Mutex, broadcast, mpsc, watch},
    time,
};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, error, info, warn};

use crate::{build, config::Config, upload};

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

#[derive(Debug, Clone, Copy)]
struct Building {
    hub_id: endpoint::Id,
    task_id: u64,
}

pub async fn run(state: State, config: Config) -> Result<(), Infallible> {
    let building = Arc::new(Mutex::new(None));

    let (build_changed_tx, _rx) = watch::channel(None);

    join_all(
        config
            .hubs
            .iter()
            .map(|hub| connect(&state, hub, building.clone(), build_changed_tx.clone())),
    )
    .await;

    Ok(())
}

#[tracing::instrument(
    name = "stream",
    skip_all,
    fields(
        host_address = %target.host_address,
        public_key = %target.public_key
    ),
)]
async fn connect(
    state: &State,
    target: &HubTarget,
    building: Arc<Mutex<Option<Building>>>,
    build_changed_tx: watch::Sender<Option<Building>>,
) {
    loop {
        debug!("Attempting to connect to summit");

        if let Err(e) = connect_inner(state, target, building.clone(), build_changed_tx.clone()).await {
            let error = error::chain(&*e);
            error!(%error, "Stream error");

            // TODO: Exponential backoff due to spurious / network errors
            time::sleep(Duration::from_secs(10)).await;
        }
    }
}

async fn connect_inner(
    state: &State,
    target: &HubTarget,
    building: Arc<Mutex<Option<Building>>>,
    build_changed_tx: watch::Sender<Option<Building>>,
) -> Result<()> {
    let endpoint = Endpoint::list(&mut *state.service_db.acquire().await.context("acquire db conn")?)
        .await
        .context("list endpoints")?
        .into_iter()
        .find(|e| {
            e.role == Role::Hub && e.status == endpoint::Status::Operational && e.host_address == target.host_address
        })
        .ok_or_eyre("no enrolled hub")?;

    let mut client = SummitServiceClient::connect_with_auth(
        endpoint.host_address.clone(),
        None,
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
    let mut build_changed_rx = build_changed_tx.subscribe();

    let (cancel_sender, cancel_receiver) = broadcast::channel::<()>(1);

    loop {
        select! {
            _ = interval.tick() => {
                let building = *building.lock().await;

                // Only send task id if its being built for this summit instance
                let task_id = building.filter(|b| b.hub_id == endpoint.id).map(|b| b.task_id);
                let building = building.is_some();

                let _ = sender
                    .send(BuilderStreamIncoming {
                        event: Some(builder_stream_incoming::Event::Status(BuilderStatus { building, task_id })),
                    })
                    .await;

                debug!(building, task_id, "Status reported");
            },
            _ = build_changed_rx.changed() => {
                let building = *build_changed_rx.borrow_and_update();

                // Only send task id if its being built for this summit instance
                let task_id = building.filter(|b| b.hub_id == endpoint.id).map(|b| b.task_id);
                let building = building.is_some();

                let _ = sender
                    .send(BuilderStreamIncoming {
                        event: Some(builder_stream_incoming::Event::Status(BuilderStatus { building, task_id })),
                    })
                    .await;

                debug!(building, task_id, "Build status change reported");
            }
            result = stream.try_next() => {
                if let Some(message) = result.context("stream grpc error")? {
                    let event = message.event.ok_or_eyre("missing stream event")?;

                    match event {
                        builder_stream_outgoing::Event::Build(request) => {
                            build_requested(state, endpoint.id, &building, &sender, request, &cancel_receiver, &build_changed_tx).await;
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
    hub_id: endpoint::Id,
    building: &Arc<Mutex<Option<Building>>>,
    sender: &mpsc::Sender<BuilderStreamIncoming>,
    request: BuilderBuild,
    cancel_receiver: &broadcast::Receiver<()>,
    build_changed_tx: &watch::Sender<Option<Building>>,
) {
    let mut build_guard = building.lock().await;

    if let Some(in_progress) = *build_guard {
        warn!("Build already in progress, ignoring");

        let in_progress_task_id = (in_progress.hub_id == hub_id).then_some(in_progress.task_id);

        let _ = sender
            .send(BuilderStreamIncoming {
                event: Some(builder_stream_incoming::Event::Busy(BuilderBusy {
                    requested_task_id: request.task_id,
                    in_progress_task_id,
                })),
            })
            .await;

        return;
    }

    *build_guard = Some(Building {
        hub_id,
        task_id: request.task_id,
    });
    let _ = build_changed_tx.send(*build_guard);

    drop(build_guard);

    tokio::spawn({
        let state = state.clone();
        let sender = sender.clone();
        let handle = Handle { sender: sender.clone() };
        let building = building.clone();
        let build_changed_tx = build_changed_tx.clone();
        let mut cancel_receiver = cancel_receiver.resubscribe();

        async move {
            select! {
                biased;

                _ = cancel_receiver.recv() => {
                    info!("Build cancelled");

                    let _ = sender
                        .send(BuilderStreamIncoming {
                            event: Some(builder_stream_incoming::Event::Status(
                                BuilderStatus { building: false, task_id: None })
                            ),
                        })
                        .await;
                }

                _ = build(request, state, handle) => {}
            }

            building.lock().await.take();
            let _ = build_changed_tx.send(None);
        }
    });
}
