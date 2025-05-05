use std::{convert::Infallible, future::Future, time::Duration};

use color_eyre::{
    Result,
    eyre::{Context, Report},
};
use service::{endpoint, error};
use tokio::{
    sync::mpsc,
    time::{self, Instant},
};
use tracing::{debug, error, info};

use crate::{Manager, builder, profile, repository, task};

const TIMER_INTERVAL: Duration = Duration::from_secs(30);

pub type Sender = mpsc::UnboundedSender<Message>;

#[derive(Debug, strum::Display)]
#[strum(serialize_all = "kebab-case")]
pub enum Message {
    AllocateBuilds,
    ImportSucceeded { task_id: task::Id },
    ImportFailed { task_id: task::Id },
    Retry { task_id: task::Id },
    Timer(Instant),
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
        Message::ImportSucceeded { task_id } => import_succeeded(sender, manager, task_id).await,
        Message::ImportFailed { task_id } => import_failed(sender, manager, task_id).await,
        Message::Retry { task_id } => retry(sender, manager, task_id).await,
        Message::Timer(_) => timer(sender, manager).await,
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
async fn retry(sender: &Sender, manager: &mut Manager, task_id: task::Id) -> Result<()> {
    debug!("Retry");

    manager.retry(task_id).await.context("manager retry task")?;

    let _ = sender.send(Message::AllocateBuilds);

    Ok(())
}

#[tracing::instrument(skip_all, fields(project))]
async fn timer(sender: &Sender, manager: &Manager) -> Result<()> {
    debug!("Timer triggered");

    let mut have_changes = false;

    let mut conn = manager.acquire().await.context("acquire db conn")?;

    for mut project in manager.projects().await.context("list projects")? {
        let mut profile_refreshed = false;

        // Ensure un-indexed profiles get refreshed, such as if they failed
        // to refresh previously from spurious errors
        for profile in &mut project.profiles {
            let mut refresh = async || {
                if !matches!(profile.status, profile::Status::Indexed) {
                    debug!(
                        profile = %profile.name,
                        status = %profile.status,
                        "Profile not fully indexed, refreshing..."
                    );

                    let profile_db = manager.profile_db(&profile.id).cloned()?;

                    profile::refresh(&manager.state, profile, profile_db)
                        .await
                        .context("refres profile")?;

                    Result::<_, Report>::Ok(true)
                } else {
                    Ok(false)
                }
            };

            match refresh().await {
                Ok(refreshed) => {
                    if refreshed {
                        profile_refreshed = true;
                    }
                }
                Err(err) => {
                    error!(
                        profile = %profile.name,
                        error = error::chain(&*err),
                        "Failed to refresh profile"
                    );
                }
            }
        }

        // Refresh each repo & if newer, reindex. If reindexed, try adding missing tasks
        for repo in &project.repositories {
            let mut refresh = async |profile_refreshed: bool| {
                let (mut repo, repo_changed) = repository::refresh(&mut conn, &manager.state, repo.clone())
                    .await
                    .context("refresh repository")?;

                let repo_db = manager.repository_db(&repo.id)?.clone();

                if repo_changed {
                    repo = repository::reindex(&mut conn, &manager.state, repo, repo_db.clone())
                        .await
                        .context("reindex repository")?;
                }

                if repo_changed || profile_refreshed {
                    task::create_missing(&mut conn, manager, &project, &repo, &repo_db)
                        .await
                        .context("create missing tasks")?;

                    Result::<_, Report>::Ok(true)
                } else {
                    Ok(false)
                }
            };

            match refresh(profile_refreshed).await {
                Ok(tasks_created) => {
                    if tasks_created {
                        have_changes = true;
                    }
                }
                Err(err) => {
                    error!(error = error::chain(&*err), "Failed to refresh repository");
                }
            }
        }
    }

    // If tasks were added, allocate new builds
    if have_changes {
        let _ = sender.send(Message::AllocateBuilds);
    }

    Ok(())
}
