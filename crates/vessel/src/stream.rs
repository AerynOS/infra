use std::{convert::Infallible, time::Duration};

use color_eyre::eyre::{Context, OptionExt as _, Result};
use service::{
    Service,
    client::{AuthClient as _, Credentials, CredentialsAuth, InMemoryTokenStorage, SummitServiceClient},
    error,
    grpc::proto::summit::repository_manager_stream,
};
use tokio::{select, sync::mpsc, time};
use tokio_stream::{StreamExt, wrappers::ReceiverStream};
use tracing::{debug, error, info};

use crate::{Config, State, upload, worker};

pub async fn run(state: State, config: Config, worker_events: worker::EventReceiver) -> Result<(), Infallible> {
    connect(&state, &config, worker_events).await;
    Ok(())
}

#[tracing::instrument(
    name = "stream",
    skip_all,
    fields(
        host_address = %config.summit.host_address,
        public_key = %config.summit.public_key,
    ),
)]
async fn connect(state: &State, config: &Config, mut worker_events: worker::EventReceiver) {
    // Create auth credentials up here so it'll live longer than our `connect_inner` loop
    // and we can reuse access tokens across broken connections / reconnect loop
    let auth = CredentialsAuth::with_in_memory_storage(Credentials::Service {
        service: Service::Vessel,
        key_pair: state.service.key_pair.clone(),
    })
    // Ensure the configured summit is who they say they are
    //
    // TLS should cover this, but this is an extra protection especially
    // if TLS isn't enabled on the summit grpc server
    .verify_server(config.summit.public_key);

    loop {
        debug!("Attempting to connect to summit");

        if let Err(e) = connect_inner(state, config, auth.clone(), &mut worker_events).await {
            let error = error::chain(&*e);
            error!(%error, "Stream error");

            // TODO: Exponential backoff due to spurious / network errors
            time::sleep(Duration::from_secs(10)).await;
        }
    }
}

async fn connect_inner(
    state: &State,
    config: &Config,
    auth: CredentialsAuth<InMemoryTokenStorage>,
    worker_events: &mut worker::EventReceiver,
) -> Result<()> {
    let mut client = SummitServiceClient::connect_with_auth(config.summit.host_address.clone(), None, auth)
        .await
        .context("connect summit client")?;

    let (sender, receiver) = mpsc::channel(1);

    let resp = client
        .repository_manager(ReceiverStream::new(receiver))
        .await
        .context("connect summit repository manager stream")?;

    info!("Connected to summit");

    // Send details upon connecting
    let _ = sender
        .send(repository_manager_stream::Incoming {
            event: Some(repository_manager_stream::incoming::Event::Details(
                repository_manager_stream::Details {
                    grpc_uri: config.grpc_address.to_string(),
                },
            )),
        })
        .await;

    let mut stream = resp.into_inner();
    let mut interval = time::interval(Duration::from_secs(60));

    loop {
        select! {
            // TODO: Status reporting: disk info, etc
            _ = interval.tick() => {},
            event = worker_events.recv() => {
                if let Some(event) = event {
                    match event {
                        worker::Event::ImportSucceeded { task_id } => {
                            let _ = sender.send(repository_manager_stream::Incoming {
                                event: Some(repository_manager_stream::incoming::Event::ImportSucceeded(
                                    task_id
                                ))
                            })
                            .await;

                            debug!(%task_id, "Import succeeded reported");
                        },
                        worker::Event::ImportFailed { task_id } => {
                            let _ = sender.send(repository_manager_stream::Incoming {
                                event: Some(repository_manager_stream::incoming::Event::ImportFailed(
                                    task_id
                                ))
                            })
                            .await;

                            debug!(%task_id, "Import failed reported");
                        },
                    }
                }
            }
            result = stream.try_next() => {
                if let Some(message) = result.context("stream grpc error")? {
                    let event = message.event.ok_or_eyre("missing stream event")?;

                    match event {
                        repository_manager_stream::outgoing::Event::RequestUploadToken(request) => {
                            let token = upload::upload_token(state, &request).context("create upload token")?;

                            let _ = sender.send(repository_manager_stream::Incoming {
                                event: Some(repository_manager_stream::incoming::Event::UploadToken(
                                    repository_manager_stream::UploadToken {
                                        task_id: request.task_id,
                                        collectables: request.collectables,
                                        token
                                    }
                                ))
                            })
                            .await;

                            info!(
                                task_id = request.task_id,
                                builder_id = request.builder_id,
                                "Upload token issued for builder"
                            );
                        },
                    }
                } else {
                    break;
                }

            }
        }
    }

    Ok(())
}
