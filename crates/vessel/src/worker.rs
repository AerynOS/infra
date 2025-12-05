use std::{convert::Infallible, future::Future, time::Duration};

use color_eyre::eyre::{Context, Result};
use service::{
    Endpoint,
    client::{AuthClient, EndpointAuth, SummitServiceClient},
    grpc::summit::ImportRequest,
};
use tokio::{
    sync::{mpsc, oneshot},
    time::{self, Instant},
};
use tracing::{Instrument, error, info, info_span};

use crate::{
    Package, State,
    channel::{self, DEFAULT_CHANNEL},
};

pub type Sender = mpsc::UnboundedSender<Message>;

/// Prune 1x per day
const PRUNE_INTERVAL: Duration = Duration::from_secs(60 * 60 * 24);

#[derive(Debug, strum::Display)]
#[strum(serialize_all = "kebab-case")]
#[allow(clippy::large_enum_variant)]
pub enum Message {
    PackagesUploaded {
        task_id: u64,
        endpoint: Endpoint,
        packages: Vec<Package>,
    },
    Prune(Instant),
    #[strum(serialize = "channel-command-{command}")]
    ChannelCommand {
        channel: String,
        command: channel::Command,
        response: oneshot::Sender<Result<()>>,
    },
}

pub async fn run(state: State) -> Result<(Sender, impl Future<Output = Result<(), Infallible>> + use<>)> {
    let (sender, mut receiver) = mpsc::unbounded_channel::<Message>();

    tokio::spawn(prune_interval_task(sender.clone()));

    let task = async move {
        while let Some(message) = receiver.recv().await {
            let kind = message.to_string();

            if let Err(e) = handle_message(&state, message).await {
                let error = service::error::chain(e.as_ref() as &dyn std::error::Error);
                error!(message = kind, %error, "Error handling message");
            }
        }

        info!("Worker exiting");

        Ok(())
    };

    Ok((sender, task))
}

/// Fires off [`Message::Prune`] every [`PRUNE_INTERVAL`]
async fn prune_interval_task(sender: Sender) -> Result<(), Infallible> {
    // Will fire immediately on launch & then every interval defined
    let mut interval = time::interval(PRUNE_INTERVAL);

    loop {
        let _ = sender.send(Message::Prune(interval.tick().await));
    }
}

async fn handle_message(state: &State, message: Message) -> Result<()> {
    match message {
        Message::PackagesUploaded {
            task_id,
            endpoint,
            packages,
        } => {
            let channel = DEFAULT_CHANNEL;

            let span = info_span!(
                "packages_uploaded",
                task_id,
                endpoint = %endpoint.id,
                num_packages = packages.len(),
                %channel,
            );

            async move {
                let mut client = SummitServiceClient::connect_with_auth(
                    endpoint.host_address.clone(),
                    EndpointAuth::new(&endpoint, state.service_db().clone(), state.service.key_pair.clone()),
                )
                .await
                .context("connect summit client")?;

                match channel::import_packages(state, channel, packages, true).await {
                    Ok(()) => {
                        info!("All packages imported");

                        client
                            .import_succeeded(ImportRequest { task_id })
                            .await
                            .context("send import succeeded request")?;
                    }
                    Err(e) => {
                        let error = service::error::chain(e.as_ref() as &dyn std::error::Error);
                        error!(%error, "Failed to import packages");

                        client
                            .import_failed(ImportRequest { task_id })
                            .await
                            .context("send import failed request")?;
                    }
                }

                Ok(())
            }
            .instrument(span)
            .await
        }
        Message::Prune(_) => channel::prune(state, DEFAULT_CHANNEL).await,
        Message::ChannelCommand {
            channel,
            command,
            response,
        } => {
            let result = channel::handle_command(state, &channel, command).await;

            if let Err(error) = &result {
                let error = service::error::chain(error.as_ref() as &dyn std::error::Error);
                error!(%error, "Failed to handle command");
            }

            let _ = response.send(result);

            Ok(())
        }
    }
}
