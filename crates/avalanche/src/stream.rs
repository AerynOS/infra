use std::{convert::Infallible, sync::Arc, time::Duration};

use color_eyre::eyre::{Context, OptionExt, Result};
use futures_util::{TryStreamExt, future::join_all};
use service::{
    Service, State,
    client::{AuthClient, Credentials, CredentialsAuth, InMemoryTokenStorage, SummitServiceClient},
    crypto::PublicKey,
    error,
    grpc::proto::{common::Collectable, summit::builder_stream},
};
use tokio::{
    select,
    sync::{Mutex, mpsc, watch},
    time,
};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, error, info, warn};

use crate::{
    build,
    config::{Config, SummitConfig},
    upload,
};

#[derive(Clone)]
pub struct Handle {
    sender: mpsc::Sender<builder_stream::Incoming>,
}

impl Handle {
    pub async fn build_started(&self, task_id: u64) {
        let _ = self
            .sender
            .send(builder_stream::Incoming {
                event: Some(builder_stream::incoming::Event::BuildStarted(task_id)),
            })
            .await;
    }

    pub async fn build_log(&self, chunk: Vec<u8>) {
        let _ = self
            .sender
            .send(builder_stream::Incoming {
                event: Some(builder_stream::incoming::Event::BuildLog(builder_stream::BuildLog {
                    chunk,
                })),
            })
            .await;
    }

    pub async fn build_succeeded(&self, task_id: u64, collectables: Vec<Collectable>) {
        let _ = self
            .sender
            .send(builder_stream::Incoming {
                event: Some(builder_stream::incoming::Event::BuildSucceeded(
                    builder_stream::BuildFinished { task_id, collectables },
                )),
            })
            .await;
    }

    pub async fn build_failed(&self, task_id: u64) {
        let _ = self
            .sender
            .send(builder_stream::Incoming {
                event: Some(builder_stream::incoming::Event::BuildFailed(task_id)),
            })
            .await;
    }
}

#[derive(Debug, Clone, Copy)]
struct Building {
    // Summit instance this build is related to. Since avalanche
    // can connect to multiple summit, we need to track who this
    // build is for.
    summit_public: PublicKey,
    task_id: u64,
}

pub async fn run(state: State, config: Config) -> Result<(), Infallible> {
    let building = Arc::new(Mutex::new(None));

    let (notify_build_change, _) = watch::channel(());

    join_all(
        config
            .upstreams
            .iter()
            .map(|summit| connect(&state, summit, building.clone(), notify_build_change.clone())),
    )
    .await;

    Ok(())
}

#[tracing::instrument(
    name = "stream",
    skip_all,
    fields(
        host_address = %summit.host_address,
        public_key = %summit.public_key,
    ),
)]
async fn connect(
    state: &State,
    summit: &SummitConfig,
    building: Arc<Mutex<Option<Building>>>,
    notify_build_change: watch::Sender<()>,
) {
    // Create auth credentials up here so it'll live longer than our `connect_inner` loop
    // and we can reuse access tokens across broken connections / reconnect loop
    let auth = CredentialsAuth::with_in_memory_storage(Credentials::Service {
        service: Service::Avalanche,
        key_pair: state.key_pair.clone(),
    })
    // Ensure the configured summit is who they say they are
    //
    // TLS should cover this, but this is an extra protection especially
    // if TLS isn't enabled on the summit grpc server
    .verify_server(summit.public_key);

    loop {
        debug!("Attempting to connect to summit");

        if let Err(e) = connect_inner(
            state,
            summit,
            auth.clone(),
            building.clone(),
            notify_build_change.clone(),
        )
        .await
        {
            let error = error::chain(&*e);
            error!(%error, "Stream error");

            // TODO: Exponential backoff due to spurious / network errors
            time::sleep(Duration::from_secs(10)).await;
        }
    }
}

async fn connect_inner(
    state: &State,
    summit: &SummitConfig,
    auth: CredentialsAuth<InMemoryTokenStorage>,
    building: Arc<Mutex<Option<Building>>>,
    notify_build_change: watch::Sender<()>,
) -> Result<()> {
    let mut client = SummitServiceClient::connect_with_auth(summit.host_address.clone(), None, auth)
        .await
        .context("connect summit client")?;

    let (sender, receiver) = mpsc::channel(1);

    let resp = client
        .builder(ReceiverStream::new(receiver))
        .await
        .context("connect summit builder stream")?;

    info!("Connected to summit");

    let summit_public = summit.public_key;
    let (notify_build_cancelled, _) = watch::channel(());

    let mut stream = resp.into_inner();
    let mut interval = time::interval(Duration::from_secs(60));
    let mut build_changed_receiver = notify_build_change.subscribe();

    loop {
        select! {
            _ = interval.tick() => {
                let building = *building.lock().await;

                // Only send task id if its being built for this summit instance
                let task_id = building.and_then(|b| (b.summit_public == summit_public).then_some(b.task_id));
                let building = building.is_some();

                let _ = sender
                    .send(builder_stream::Incoming {
                        event: Some(builder_stream::incoming::Event::Status(builder_stream::BuilderStatus { building, task_id })),
                    })
                    .await;

                debug!(building, task_id, "Status reported");
            },
            _ = build_changed_receiver.changed() => {
                let building = *building.lock().await;

                // Only send task id if its being built for this summit instance
                let task_id = building.and_then(|b| (b.summit_public == summit_public).then_some(b.task_id));
                let building = building.is_some();

                let _ = sender
                    .send(builder_stream::Incoming {
                        event: Some(builder_stream::incoming::Event::Status(builder_stream::BuilderStatus { building, task_id })),
                    })
                    .await;

                debug!(building, task_id, "Build status change reported");
            }
            result = stream.try_next() => {
                if let Some(message) = result.context("stream grpc error")? {
                    let event = message.event.ok_or_eyre("missing stream event")?;

                    match event {
                        builder_stream::outgoing::Event::StartBuild(request) => {
                            let build_changed_receiver = notify_build_cancelled.subscribe();

                            build_requested(
                                state,
                                summit_public,
                                &building,
                                &sender,
                                request,
                                build_changed_receiver,
                                &notify_build_change
                            ).await;
                        }
                        builder_stream::outgoing::Event::UploadBuild(builder_stream::UploadBuild { build, token, uri }) => {
                            let build = build.ok_or_eyre("missing build message")?;
                            let task_id = build.task_id;

                            tokio::spawn({
                                let state = state.clone();
                                let sender = sender.clone();

                                async move {
                                    if let Err(e) = upload(state, build, token, &uri).await {
                                        let error = error::chain(&*e);
                                        error!(uri, task_id, %error, "Failed to upload packages to vessel");

                                        let _ = sender
                                            .send(builder_stream::Incoming {
                                                event: Some(builder_stream::incoming::Event::UploadFailed(task_id)),
                                            })
                                            .await;
                                    }
                                }
                            });
                        }
                        builder_stream::outgoing::Event::CancelBuild(()) => {
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
    summit_public: PublicKey,
    building: &Arc<Mutex<Option<Building>>>,
    sender: &mpsc::Sender<builder_stream::Incoming>,
    request: builder_stream::StartBuild,
    mut cancelled_receiver: watch::Receiver<()>,
    notify_build_changed: &watch::Sender<()>,
) {
    let mut build_guard = building.lock().await;

    if let Some(in_progress) = *build_guard {
        warn!("Build already in progress, ignoring");

        let in_progress_task_id = (in_progress.summit_public == summit_public).then_some(in_progress.task_id);

        let _ = sender
            .send(builder_stream::Incoming {
                event: Some(builder_stream::incoming::Event::Busy(builder_stream::BuilderBusy {
                    requested_task_id: request.task_id,
                    in_progress_task_id,
                })),
            })
            .await;

        return;
    }

    *build_guard = Some(Building {
        summit_public,
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
