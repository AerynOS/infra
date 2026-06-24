use std::{convert::Infallible, future::Future, time::Duration};

use color_eyre::eyre::Result;
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
pub type EventReceiver = mpsc::UnboundedReceiver<Event>;

/// Prune 1x per day
const PRUNE_INTERVAL: Duration = Duration::from_secs(60 * 60 * 24);

#[derive(Debug, strum::Display)]
#[strum(serialize_all = "kebab-case")]
#[allow(clippy::large_enum_variant)]
pub enum Message {
    PackagesUploaded {
        task_id: u64,
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

#[derive(Debug, strum::Display)]
#[strum(serialize_all = "kebab-case")]
pub enum Event {
    ImportSucceeded { task_id: u64 },
    ImportFailed { task_id: u64 },
}

pub async fn run(
    state: State,
) -> Result<(
    Sender,
    EventReceiver,
    impl Future<Output = Result<(), Infallible>> + use<>,
)> {
    let (sender, mut receiver) = mpsc::unbounded_channel::<Message>();
    let (event_sender, event_receiver) = mpsc::unbounded_channel::<Event>();

    tokio::spawn(prune_interval_task(sender.clone()));

    let task = async move {
        while let Some(message) = receiver.recv().await {
            let kind = message.to_string();

            if let Err(e) = handle_message(&state, message, &event_sender).await {
                let error = service::error::chain(e.as_ref() as &dyn std::error::Error);
                error!(message = kind, %error, "Error handling message");
            }
        }

        info!("Worker exiting");

        Ok(())
    };

    Ok((sender, event_receiver, task))
}

/// Fires off [`Message::Prune`] every [`PRUNE_INTERVAL`]
async fn prune_interval_task(sender: Sender) -> Result<(), Infallible> {
    // Will fire immediately on launch & then every interval defined
    let mut interval = time::interval(PRUNE_INTERVAL);

    loop {
        let _ = sender.send(Message::Prune(interval.tick().await));
    }
}

async fn handle_message(state: &State, message: Message, events: &mpsc::UnboundedSender<Event>) -> Result<()> {
    match message {
        Message::PackagesUploaded { task_id, packages } => {
            let channel = DEFAULT_CHANNEL;

            let span = info_span!(
                "packages_uploaded",
                task_id,
                num_packages = packages.len(),
                %channel,
            );

            async move {
                match channel::import_packages(state, channel, packages, true).await {
                    Ok(()) => {
                        info!("All packages imported");

                        let _ = events.send(Event::ImportSucceeded { task_id });
                    }
                    Err(e) => {
                        let error = service::error::chain(e.as_ref() as &dyn std::error::Error);
                        error!(%error, "Failed to import packages");

                        let _ = events.send(Event::ImportFailed { task_id });
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
