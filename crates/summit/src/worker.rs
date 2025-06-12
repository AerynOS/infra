use std::{convert::Infallible, future::Future, time::Duration};

use color_eyre::{Result, eyre::Context};
use service::endpoint;
use tokio::{
    sync::mpsc,
    time::{self, Instant},
};
use tracing::{Instrument, Span, debug, error, info};

use crate::{Manager, builder, task};

const TIMER_INTERVAL: Duration = Duration::from_secs(30);

pub type Sender = mpsc::UnboundedSender<Message>;

#[derive(Debug, strum::Display)]
#[strum(serialize_all = "kebab-case")]
pub enum Message {
    AllocateBuilds,
    ImportSucceeded { task_id: task::Id, span: Span },
    ImportFailed { task_id: task::Id, span: Span },
    RetryTask { task_id: task::Id },
    FailTask { task_id: task::Id },
    Timer(Instant),
    ForceRefresh,
    Builder(endpoint::Id, builder::Message),
}

pub async fn run(mut manager: Manager) -> Result<(Sender, impl Future<Output = Result<(), Infallible>> + use<>)> {
    let (sender, mut receiver) = mpsc::unbounded_channel::<Message>();

    let task = {
        let sender = sender.clone();

        tokio::spawn(timer_task(sender.clone()));

        async move {
            while let Some(message) = receiver.recv().await {
                let kind = message.to_string();

                if let Err(e) = handle_message(&sender, &mut manager, message).await {
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

async fn timer_task(sender: Sender) -> Result<(), Infallible> {
    let mut interval = time::interval(TIMER_INTERVAL);
    interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

    loop {
        let _ = sender.send(Message::Timer(interval.tick().await));
    }
}

async fn handle_message(sender: &Sender, manager: &mut Manager, message: Message) -> Result<()> {
    match message {
        Message::AllocateBuilds => allocate_builds(manager).await,
        Message::ImportSucceeded { task_id, span } => import_succeeded(sender, manager, task_id).instrument(span).await,
        Message::ImportFailed { task_id, span } => import_failed(sender, manager, task_id).instrument(span).await,
        Message::RetryTask { task_id } => retry_task(sender, manager, task_id).await,
        Message::FailTask { task_id } => fail_task(sender, manager, task_id).await,
        Message::Timer(_) => timer(sender, manager).await,
        Message::ForceRefresh => force_refresh(sender, manager).await,
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
    }
}

#[tracing::instrument(skip_all)]
async fn allocate_builds(manager: &mut Manager) -> Result<()> {
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
async fn fail_task(sender: &Sender, manager: &mut Manager, task_id: task::Id) -> Result<()> {
    debug!("Fail task");

    manager.fail_task(task_id).await.context("manager fail task")?;

    let _ = sender.send(Message::AllocateBuilds);

    Ok(())
}

#[tracing::instrument(skip_all, fields(project))]
async fn timer(sender: &Sender, manager: &Manager) -> Result<()> {
    debug!("Timer triggered");

    let have_changes = manager.refresh(false).await.context("refresh")?;

    if have_changes {
        let _ = sender.send(Message::AllocateBuilds);
    }

    Ok(())
}

#[tracing::instrument(skip_all, fields(project))]
async fn force_refresh(sender: &Sender, manager: &Manager) -> Result<()> {
    info!("Force refresh");

    let have_changes = manager.refresh(true).await.context("refresh")?;

    if have_changes {
        let _ = sender.send(Message::AllocateBuilds);
    }

    Ok(())
}
