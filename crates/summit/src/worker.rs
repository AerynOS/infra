use std::{convert::Infallible, future::Future, path::PathBuf, time::Duration};

use color_eyre::{Result, eyre::Context};
use service::{crypto::EncodedPublicKey, endpoint, signal};
use tokio::{
    sync::mpsc,
    time::{self, Instant},
};
use tracing::{debug, error, info, warn};

use crate::{Config, Manager, builder, task};

/// The interval at which a timer fires off an event to check the configured git recipe repo branch
const TIMER_INTERVAL: Duration = Duration::from_secs(30);

pub type Sender = mpsc::UnboundedSender<Message>;

#[derive(Debug, strum::Display)]
#[strum(serialize_all = "kebab-case")]
pub enum Message {
    AllocateBuilds,
    ImportSucceeded { task_id: task::Id },
    ImportFailed { task_id: task::Id },
    RetryTask { task_id: task::Id },
    CancelTask { task_id: task::Id },
    Timer(Instant),
    ForceRefresh,
    Pause,
    Resume,
    Builder(endpoint::Id, EncodedPublicKey, builder::Message),
    ConfigReloaded(Box<Config>),
}

pub async fn run(
    mut manager: Manager,
    config_path: PathBuf,
) -> Result<(Sender, impl Future<Output = Result<(), Infallible>> + use<>)> {
    let (sender, mut receiver) = mpsc::unbounded_channel::<Message>();

    let task = {
        let sender = sender.clone();

        tokio::spawn(timer_task(sender.clone()));
        tokio::spawn(reload_config_on_sighup_task(sender.clone(), config_path));

        async move {
            let mut paused = false;

            loop {
                if receiver.is_closed() && receiver.is_empty() {
                    break;
                }

                // Pull out all immediately available messages or wait
                // for at least 1 to come in
                let num_to_recv = receiver.len().max(1);
                let mut messages = Vec::with_capacity(num_to_recv);
                receiver.recv_many(&mut messages, num_to_recv).await;

                for message in dedupe_messages(messages) {
                    let kind = message.to_string();

                    if let Err(e) = handle_message(&sender, &mut manager, &mut paused, message).await {
                        let error = service::error::chain(e.as_ref() as &dyn std::error::Error);
                        error!(message = kind, %error, "Error handling message");
                    }

                    // Ensure cached build info is always up-to-date after
                    // any state change so frontend can access this asynchronously
                    manager.refresh_cached_builder_info();
                }
            }

            info!("Worker exiting");

            Ok(())
        }
    };

    let _ = sender.send(Message::AllocateBuilds);

    Ok((sender, task))
}

async fn timer_task(sender: Sender) {
    let mut interval = time::interval(TIMER_INTERVAL);
    interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

    loop {
        let _ = sender.send(Message::Timer(interval.tick().await));
    }
}

async fn reload_config_on_sighup_task(sender: Sender, config_path: PathBuf) {
    loop {
        if let Err(err) = signal::capture(Some(signal::Kind::hangup())).await {
            let error = service::error::chain(&err);
            error!(%error, "Error during SIGHUP capture");
            break;
        };

        info!("SIGHUP received, reloading config...");

        match Config::load(&config_path).await {
            Ok(config) => {
                let _ = sender.send(Message::ConfigReloaded(Box::new(config)));
            }
            Err(err) => {
                let error = service::error::chain(&*err);
                error!(%error, "Failed to reload config");
            }
        }
    }
}

async fn handle_message(sender: &Sender, manager: &mut Manager, paused: &mut bool, message: Message) -> Result<()> {
    match message {
        Message::AllocateBuilds => allocate_builds(manager, *paused).await,
        Message::ImportSucceeded { task_id } => import_succeeded(sender, manager, task_id).await,
        Message::ImportFailed { task_id } => import_failed(sender, manager, task_id).await,
        Message::RetryTask { task_id } => retry_task(sender, manager, task_id).await,
        Message::CancelTask { task_id } => cancel_task(sender, manager, task_id).await,
        Message::Timer(_) => timer(sender, manager, *paused).await,
        Message::ForceRefresh => force_refresh(sender, manager, *paused).await,
        Message::Pause => pause(paused).await,
        Message::Resume => resume(sender, paused).await,
        Message::Builder(endpoint, public_key, message) => {
            let allocate_builds = manager
                .update_builder(endpoint, public_key, message)
                .await
                .context("update builder")?;

            if allocate_builds {
                let _ = sender.send(Message::AllocateBuilds);
            }

            Ok(())
        }
        Message::ConfigReloaded(config) => {
            manager.config_reloaded(*config);

            Ok(())
        }
    }
}

#[tracing::instrument(skip_all)]
async fn allocate_builds(manager: &mut Manager, paused: bool) -> Result<()> {
    if paused {
        warn!("Cannot allocate builds while paused");
        return Ok(());
    }

    debug!("Allocating builds");

    manager.allocate_builds().await.context("allocate builds")
}

#[tracing::instrument(skip_all)]
async fn import_succeeded(sender: &Sender, manager: &mut Manager, task_id: task::Id) -> Result<()> {
    debug!("Import succeeded");

    manager
        .import_succeeded(task_id)
        .await
        .context("manager import failed")?;

    let _ = sender.send(Message::AllocateBuilds);

    Ok(())
}

#[tracing::instrument(skip_all)]
async fn import_failed(sender: &Sender, manager: &mut Manager, task_id: task::Id) -> Result<()> {
    debug!("Import failed");

    manager.import_failed(task_id).await.context("manager import failed")?;

    let _ = sender.send(Message::AllocateBuilds);

    Ok(())
}

#[tracing::instrument(skip_all)]
async fn retry_task(sender: &Sender, manager: &mut Manager, task_id: task::Id) -> Result<()> {
    debug!("Retry task");

    manager.retry_task(task_id).await.context("manager retry task")?;

    let _ = sender.send(Message::AllocateBuilds);

    Ok(())
}

#[tracing::instrument(skip_all)]
async fn cancel_task(sender: &Sender, manager: &mut Manager, task_id: task::Id) -> Result<()> {
    debug!("Cancel task");

    manager.cancel_task(task_id).await.context("manager cancel task")?;

    let _ = sender.send(Message::AllocateBuilds);

    Ok(())
}

#[tracing::instrument(skip_all, fields(project))]
async fn timer(sender: &Sender, manager: &Manager, paused: bool) -> Result<()> {
    if paused {
        info!("Paused");
        return Ok(());
    }

    debug!("Timer triggered");

    let have_changes = manager.refresh(false).await.context("refresh")?;

    if have_changes {
        let _ = sender.send(Message::AllocateBuilds);
    }

    Ok(())
}

#[tracing::instrument(skip_all, fields(project))]
async fn force_refresh(sender: &Sender, manager: &Manager, paused: bool) -> Result<()> {
    if paused {
        warn!("Cannot force refresh while paused");
        return Ok(());
    }

    info!("Force refresh");

    let have_changes = manager.refresh(true).await.context("refresh")?;

    if have_changes {
        let _ = sender.send(Message::AllocateBuilds);
    }

    Ok(())
}

#[tracing::instrument(skip_all, fields(project))]
async fn pause(paused: &mut bool) -> Result<()> {
    info!("Pause");

    *paused = true;

    Ok(())
}

#[tracing::instrument(skip_all, fields(project))]
async fn resume(sender: &Sender, paused: &mut bool) -> Result<()> {
    info!("Resume");

    *paused = false;

    // Always allocate builds after resuming
    let _ = sender.send(Message::AllocateBuilds);

    Ok(())
}

/// Remove sequential duplicate messages that can be collapsed to a single message
fn dedupe_messages(messages: Vec<Message>) -> Vec<Message> {
    fn is_dupe(a: &Message, b: &Message) -> bool {
        use Message::*;

        matches!(
            (a, b),
            (AllocateBuilds, AllocateBuilds) | (ForceRefresh, ForceRefresh) | (Pause, Pause) | (Resume, Resume)
        )
    }

    let num_messages = messages.len();

    messages
        .into_iter()
        .fold(Vec::with_capacity(num_messages), |mut acc, message| {
            if acc.last().is_some_and(|m| is_dupe(m, &message)) {
                return acc;
            }

            acc.push(message);
            acc
        })
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    #[allow(clippy::needless_range_loop)]
    fn test_dedupe_messages() {
        use Message::*;

        let messages = vec![
            AllocateBuilds,
            Pause,
            AllocateBuilds,
            AllocateBuilds,
            AllocateBuilds,
            Resume,
            Resume,
            Pause,
            Pause,
            Resume,
            AllocateBuilds,
        ];

        let deduped = dedupe_messages(messages);

        // Message doesn't impl Eq
        assert_eq!(deduped.len(), 7);

        for i in 0..7 {
            let actual = &deduped[i];

            match i {
                0 => assert!(matches!(actual, AllocateBuilds)),
                1 => assert!(matches!(actual, Pause)),
                2 => assert!(matches!(actual, AllocateBuilds)),
                3 => assert!(matches!(actual, Resume)),
                4 => assert!(matches!(actual, Pause)),
                5 => assert!(matches!(actual, Resume)),
                6 => assert!(matches!(actual, AllocateBuilds)),
                _ => unreachable!(),
            }
        }
    }
}
