use std::{convert::Infallible, time::Duration};

use color_eyre::eyre::{Context, OptionExt as _, Result, bail, eyre};
use service::{
    Service,
    client::{AuthClient as _, Credentials, CredentialsAuth, InMemoryTokenStorage, SummitServiceClient},
    error,
    grpc::proto::{
        summit::repository_manager_stream,
        vessel::{command, upgrade_format},
    },
};
use tokio::{select, sync::mpsc, time};
use tokio_stream::{StreamExt, wrappers::ReceiverStream};
use tracing::{debug, error, info};

use crate::{Config, State, channel, upload, worker};

pub async fn run(
    state: State,
    config: Config,
    worker_sender: worker::Sender,
    worker_events: worker::EventReceiver,
) -> Result<(), Infallible> {
    connect(&state, &config, worker_sender, worker_events).await;
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
async fn connect(
    state: &State,
    config: &Config,
    mut worker_sender: worker::Sender,
    mut worker_events: worker::EventReceiver,
) {
    loop {
        debug!("Attempting to connect to summit");

        if let Err(e) = connect_inner(state, config, &mut worker_sender, &mut worker_events).await {
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
    worker_sender: &mut worker::Sender,
    worker_events: &mut worker::EventReceiver,
) -> Result<()> {
    let mut client = SummitServiceClient::connect_with_auth(
        config.summit.host_address.clone(),
        None,
        CredentialsAuth::with_in_memory_storage(Credentials::Service {
            service: Service::Vessel,
            key_pair: state.service.key_pair.clone(),
        })
        // Ensure the configured summit is who they say they are
        //
        // TLS should cover this, but this is an extra protection especially
        // if TLS isn't enabled on the summit grpc server
        .verify_server(config.summit.public_key),
    )
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
                        worker::Event::CommandFinished { request_id, result } => {
                            let _ = sender.send(repository_manager_stream::Incoming {
                                event: Some(repository_manager_stream::incoming::Event::Command(
                                    command::Response {
                                        request_id: request_id.clone(),
                                        success: result.is_ok(),
                                        error: result.err().map(|err| format!("{err:#}")),
                                    }
                                ))
                            })
                            .await;

                            debug!(%request_id, "Command status reported");
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
                                "Upload token issued for task"
                            );
                        },
                        repository_manager_stream::outgoing::Event::Command(request) => {
                            let request_id = request.request_id.clone();

                            match parse_channel_command(request) {
                                Ok((channel, command)) => {
                                    info!(
                                        %request_id,
                                        %channel,
                                        %command,
                                        "Command received"
                                    );

                                    let _ = worker_sender.send(worker::Message::ChannelCommand {
                                        request_id,
                                        channel,
                                        command,
                                    });
                                }
                                Err(err) => {
                                    error!(
                                        %request_id,
                                        error = format!("{err:#}"),
                                        "Failed to parse incoming command"
                                    );

                                    let _ = sender.send(repository_manager_stream::Incoming {
                                        event: Some(repository_manager_stream::incoming::Event::Command(
                                            command::Response {
                                                request_id,
                                                success: false,
                                                error: Some(format!("{err:#}"))
                                            }
                                        ))
                                    })
                                    .await;
                                }
                            }
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

fn parse_channel_command(request: command::Request) -> Result<(String, channel::Command)> {
    match request
        .command
        .and_then(|inner| inner.command)
        .ok_or_eyre("malformed grpc request")?
    {
        command::inner::Command::UpdateStream(command) => Ok((
            command.channel.clone(),
            channel::Command::UpdateStream {
                stream: match command.stream() {
                    service::grpc::proto::vessel::Stream::Volatile => channel::version::Stream::Volatile,
                    service::grpc::proto::vessel::Stream::Unstable => channel::version::Stream::Unstable,
                    _ => bail!("unknown stream"),
                },
                version: channel::Version::try_from(command.version)
                    .map_err(|err| eyre!("failed to parse version: {err}"))?,
            },
        )),
        command::inner::Command::AddTag(command) => Ok((
            command.channel,
            channel::Command::AddTag {
                tag: channel::version::Identifier::new(&command.tag)?.into(),
                history: channel::version::Identifier::new(&command.history)?.into(),
            },
        )),
        command::inner::Command::RemoveTag(command) => Ok((
            command.channel,
            channel::Command::RemoveTag {
                tag: channel::version::Identifier::new(&command.tag)?.into(),
            },
        )),
        command::inner::Command::UpgradeFormat(command) => Ok((
            command.channel,
            channel::Command::FormatUpgrade(
                command
                    .format
                    .and_then(|f| match f.format? {
                        upgrade_format::Format::Legacy(request) => Some(channel::FormatUpgrade::Legacy {
                            tag_name: request.tag_name,
                        }),
                    })
                    .ok_or_eyre("missing format")?,
            ),
        )),
    }
}
