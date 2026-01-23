use std::{convert::Infallible, future::Future, path::PathBuf, time::Duration};

use color_eyre::{Result, eyre::Context};
use service::{endpoint, signal};
use tokio::{
    sync::{mpsc, oneshot},
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
    Builder(endpoint::Id, builder::Message),
    ListBuilders(oneshot::Sender<Vec<builder::Info>>),
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

            while let Some(message) = receiver.recv().await {
                let kind = message.to_string();

                if let Err(e) = handle_message(&sender, &mut manager, &mut paused, message).await {
                    let error = service::error::chain(e.as_ref() as &dyn std::error::Error);
                    error!(message = kind, %error, "Error handling message");
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
        Message::Builder(endpoint, message) => {
            let allocate_builds = manager
                .update_builder(endpoint, message)
                .await
                .context("update builder")?;

            if allocate_builds {
                let _ = sender.send(Message::AllocateBuilds);
            }

            Ok(())
        }
        Message::ListBuilders(sender) => {
            let builders = manager.builders().await.context("list builders")?;

            let _ = sender.send(builders);

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
