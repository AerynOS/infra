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
    sync::{Mutex, mpsc, watch},
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

    let (notify_build_change, _) = watch::channel(());

    join_all(
        config
            .hubs
            .iter()
            .map(|hub| connect(&state, hub, building.clone(), notify_build_change.clone())),
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
    notify_build_change: watch::Sender<()>,
) {
    loop {
        debug!("Attempting to connect to summit");

        if let Err(e) = connect_inner(state, target, building.clone(), notify_build_change.clone()).await {
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
    notify_build_change: watch::Sender<()>,
) -> Result<()> {
    let endpoint = Endpoint::list(&mut *state.service_db.acquire().await.context("acquire db conn")?)
        .await
        .context("list endpoints")?
        .into_iter()
        .find(|e| e.role == Role::Hub && e.host_address == target.host_address)
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

    let (notify_build_cancelled, _) = watch::channel(());

    let mut stream = resp.into_inner();
    let mut interval = time::interval(Duration::from_secs(60));
    let mut build_changed_receiver = notify_build_change.subscribe();

    loop {
        select! {
            _ = interval.tick() => {
                let building = *building.lock().await;

                // Only send task id if its being built for this summit instance
                let task_id = building.and_then(|b| (b.hub_id == endpoint.id).then_some(b.task_id));
                let building = building.is_some();

                let _ = sender
                    .send(BuilderStreamIncoming {
                        event: Some(builder_stream_incoming::Event::Status(BuilderStatus { building, task_id })),
                    })
                    .await;

                debug!(building, task_id, "Status reported");
            },
            _ = build_changed_receiver.changed() => {
                let building = *building.lock().await;

                // Only send task id if its being built for this summit instance
                let task_id = building.and_then(|b| (b.hub_id == endpoint.id).then_some(b.task_id));
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
                            let build_changed_receiver = notify_build_cancelled.subscribe();

                            build_requested(
                                state,
                                endpoint.id,
                                &building,
                                &sender,
                                request,
                                build_changed_receiver,
                                &notify_build_change
                            ).await;
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
                            notify_build_cancelled.send_replace(());
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
    mut cancelled_receiver: watch::Receiver<()>,
    notify_build_changed: &watch::Sender<()>,
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
    notify_build_changed.send_replace(());

    drop(build_guard);

    tokio::spawn({
        let state = state.clone();
        let sender = sender.clone();
        let handle = Handle { sender: sender.clone() };
        let building = building.clone();
        let notify_build_changed = notify_build_changed.clone();

        async move {
            select! {
                biased;

                _ = cancelled_receiver.changed() => {
                    info!("Build cancelled");
                }

                _ = build(request, state, handle) => {}
            }

            building.lock().await.take();
            notify_build_changed.send_replace(());
        }
    });
}
