use std::{path::PathBuf, time::Duration};

use chrono::{DateTime, Utc};
use color_eyre::eyre::{Context, OptionExt, Result};
use http::Uri;
use serde::{Deserialize, Serialize};
use service::{
    crypto::PublicKey,
    database,
    grpc::proto::{
        self,
        common::{Collectable, Remote, collectable},
        summit::builder_stream,
    },
};
use tokio::{sync::mpsc, time::Instant};
use tracing::{debug, info, warn};

use crate::{State, Task, config::Size, profile, task};

const REQUESTED_BUILD_ACK_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug)]
pub enum Message {
    Connected(Handle),
    Disconnected,
    Status {
        now: DateTime<Utc>,
        building: bool,
        task_id: Option<task::Id>,
    },
    BuildSucceeded {
        task_id: task::Id,
        collectables: Vec<Collectable>,
        /// Path to the log file (uncompressed).
        log_path: PathBuf,
    },
    BuildFailed {
        task_id: task::Id,
        /// Path to the log file (uncompressed).
        log_path: Option<PathBuf>,
    },
    Busy {
        requested: task::Id,
        in_progress: Option<task::Id>,
    },
    UploadFailed {
        task_id: task::Id,
    },
}

#[derive(Debug)]
pub enum Event {
    StatusChanged(Status),
    BuildSucceeded {
        task_id: task::Id,
        collectables: Vec<Collectable>,
        /// Path to the log file (uncompressed).
        log_path: PathBuf,
    },
    BuildFailed {
        task_id: task::Id,
        /// Path to the log file (uncompressed).
        log_path: Option<PathBuf>,
    },
    BuildRejected {
        task_id: task::Id,
    },
    UploadFailed {
        task_id: task::Id,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Info {
    pub last_seen: Option<DateTime<Utc>>,
    pub status: StatusKind,
    pub building: Option<task::Id>,
    pub size: Size,
    pub description: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    /// Unique id of the builder for use as an internal identifier
    /// for things like logs
    pub id: String,
    pub public_key: PublicKey,
    pub description: String,
    pub size: Size,
}

/// Initializes a builder for each incoming config
///
/// Ensures [`Config`] is sync'd to backing database which is used to
/// populate builder info on related build tasks. If a builder / config
/// is removed in the future, the old DB entry will continue to provide
/// builder description resolution for older tasks.
pub async fn init_all(state: &State, configs: &[Config]) -> Result<Vec<Builder>> {
    let mut builders = vec![];

    let mut tx = state.service_db().begin().await.context("begin db tx")?;

    for config in configs {
        // Persist config to ensure it's always up-to-date
        persist_config(&mut tx, config)
            .await
            .context("persist builder config")?;

        builders.push(Builder::new(config.clone()));
    }

    tx.commit().await?;

    Ok(builders)
}

#[derive(Debug)]
pub struct Builder {
    pub config: Config,
    connection: Option<Connection>,
}

impl Builder {
    pub fn new(config: Config) -> Self {
        Self {
            config: config.clone(),
            connection: None,
        }
    }

    pub fn info(&self) -> Info {
        let status = StatusKind::from(self.status());

        let (last_seen, building) = match self.connection {
            Some(Connection {
                last_status_update,
                status: Connected::Idle | Connected::Busy,
                ..
            }) => (last_status_update, None),
            Some(Connection {
                last_status_update,
                status: Connected::BuildRequested { task, .. } | Connected::Building { task },
                ..
            }) => (last_status_update, Some(task)),
            None => (None, None),
        };

        Info {
            size: self.config.size,
            description: self.config.description.clone(),
            last_seen,
            status,
            building,
        }
    }

    pub fn is_idle(&self) -> bool {
        matches!(self.status(), Status::Idle)
    }

    pub fn is_connected(&self) -> bool {
        self.connection.is_some()
    }

    pub fn status(&self) -> Status {
        match self.connection {
            Some(Connection {
                status: Connected::Idle,
                ..
            }) => Status::Idle,
            Some(Connection {
                status: Connected::Busy,
                ..
            }) => Status::Busy,
            Some(Connection {
                status: Connected::BuildRequested { task, .. } | Connected::Building { task },
                ..
            }) => Status::Building { task },
            None => Status::Disconnected,
        }
    }

    #[tracing::instrument(
        name = "cancel_build",
        skip_all,
        fields(
            builder = %self.config.id,
            task_id = %task.id,
            build_id = task.build_id
        )
    )]
    pub async fn cancel_build(&self, task: &Task) -> Result<()> {
        match self.connection.as_ref() {
            None => warn!("Builder is disconnected, cannot cancel build"),
            Some(Connection {
                status: Connected::Idle,
                ..
            }) => warn!("Builder is idle, nothing to cancel"),
            Some(Connection {
                status: Connected::Busy,
                ..
            }) => warn!("Builder is building something else"),
            Some(Connection {
                status: Connected::Building { task: id },
                ..
            }) if task.id != *id => warn!("Builder is building something else"),
            Some(Connection { handle, .. }) => {
                handle
                    .sender
                    .send(builder_stream::Outgoing {
                        event: Some(builder_stream::outgoing::Event::CancelBuild(())),
                    })
                    .await
                    .context("send builder stream message")?;

                // Don't update builder status here, status ping
                // will update it once the builder has finished
                // actually cancelling the task & is reporting
                // as idle

                info!("Cancel request submitted to builder");
            }
        }

        Ok(())
    }

    #[tracing::instrument(
        name = "build",
        skip_all,
        fields(
            builder = %self.config.id,
            task_id = %queued.task.id,
            build_id = %queued.task.build_id,
            task_size = %queued.size,
            builder_size = %self.config.size,
        )
    )]
    pub async fn build(&mut self, queued: &task::Queued) -> Result<()> {
        let connection = self.connection.as_mut().ok_or_eyre("builder not connected")?;

        connection
            .handle
            .sender
            .send(builder_stream::Outgoing {
                event: Some(builder_stream::outgoing::Event::StartBuild(
                    builder_stream::StartBuild {
                        task_id: i64::from(queued.task.id) as u64,
                        uri: queued.origin_uri.to_string(),
                        commit_ref: queued.commit_ref.clone(),
                        relative_path: queued
                            .meta
                            .uri
                            .as_ref()
                            .ok_or_eyre("missing relative path on metadata")?
                            .to_string(),
                        build_architecture: queued.task.arch.clone(),
                        remotes: queued
                            .remotes
                            .iter()
                            .enumerate()
                            .map(|(idx, index)| Remote {
                                #[allow(deprecated)]
                                index_uri: String::default(),
                                name: format!("repo{idx}"),
                                priority: idx as u64 * 10,
                                index: Some(proto::common::Index {
                                    index: Some(match index {
                                        profile::Index::DirectIndex(uri) => {
                                            proto::common::index::Index::DirectIndex(uri.to_string())
                                        }
                                        profile::Index::RootIndex(root_index) => {
                                            proto::common::index::Index::RootIndex(proto::common::index::RootIndex {
                                                base_uri: root_index.base_uri.to_string(),
                                                channel: root_index.channel.to_string(),
                                                version: root_index.version.to_string(),
                                            })
                                        }
                                    }),
                                }),
                            })
                            .collect(),
                    },
                )),
            })
            .await
            .context("send builder stream message")?;

        connection.status = Connected::BuildRequested {
            at: Instant::now(),
            task: queued.task.id,
        };

        info!("Task sent for build");

        Ok(())
    }

    #[tracing::instrument(name = "update_builder", skip_all, fields(builder = %self.config.id))]
    pub async fn update(&mut self, message: Message) -> Option<Event> {
        match message {
            Message::Connected(handle) => {
                info!("Builder connected");

                self.connection = Some(Connection {
                    handle,
                    last_status_update: None,
                    status: Connected::Idle,
                });
            }
            Message::Disconnected => {
                warn!("Builder disconnected");

                self.connection = None;
            }
            Message::Status { now, building, task_id } => {
                let mut first_update = false;
                let previous_status = self.status();

                if let Some(connection) = self.connection.as_mut() {
                    if connection.last_status_update.is_none() {
                        first_update = true;
                    }

                    connection.last_status_update = Some(now);

                    connection.status = match (building, task_id) {
                        (true, Some(task)) => Connected::Building { task },
                        (true, None) => Connected::Busy,
                        (false, _) => {
                            // Allow up to N duration to receive an ACK on a requested
                            // build before considering it unprocessed builder side &
                            // syncing back to reported status. This is to ensure we
                            // don't process `idle` messages sent by the builder between
                            // us assigning the build & the builder actually receiving it
                            if let Connected::BuildRequested {
                                task,
                                at: build_requested_at,
                            } = connection.status
                                && build_requested_at.duration_since(Instant::now()) < REQUESTED_BUILD_ACK_TIMEOUT
                            {
                                debug!(
                                    task_id = %task,
                                    "Ignoring builder idle status race condition while waiting for requested build ack",
                                );

                                return None;
                            } else {
                                Connected::Idle
                            }
                        }
                    };

                    debug!(status = %connection.status, "Builder status updated");
                }

                let new_status = self.status();

                if first_update || new_status != previous_status {
                    return Some(Event::StatusChanged(new_status));
                }
            }
            Message::BuildSucceeded {
                task_id,
                collectables,
                log_path,
            } => {
                info!(%task_id, "Build succeeded");

                return Some(Event::BuildSucceeded {
                    task_id,
                    collectables,
                    log_path,
                });
            }
            Message::BuildFailed { task_id, log_path } => {
                info!(%task_id, "Build failed");

                return Some(Event::BuildFailed { task_id, log_path });
            }
            Message::Busy { requested, in_progress } => {
                info!(task_id = %requested, "Builder is busy & rejected build");

                // Connection status was out of sync, ensure its updated
                // so we don't try to requeue another build on this busy builder
                if let Some(connection) = self.connection.as_mut() {
                    connection.status = match in_progress {
                        Some(task) => Connected::Building { task },
                        None => Connected::Busy,
                    };
                }

                return Some(Event::BuildRejected { task_id: requested });
            }
            Message::UploadFailed { task_id } => {
                info!(%task_id, "Upload failed");

                return Some(Event::UploadFailed { task_id });
            }
        }

        None
    }

    pub async fn upload_build(
        &self,
        state: &State,
        task_id: task::Id,
        collectables: Vec<Collectable>,
        token: String,
        vessel_uri: &Uri,
    ) -> Result<()> {
        let connection = self.connection.as_ref().ok_or_eyre("builder not connected")?;

        let mut conn = state.service_db().acquire().await.context("acquire db connection")?;

        let task = task::get(conn.as_mut(), task_id).await.context("get task")?;

        connection
            .handle
            .sender
            .send(builder_stream::Outgoing {
                event: Some(builder_stream::outgoing::Event::UploadBuild(
                    builder_stream::UploadBuild {
                        build: Some(builder_stream::BuildFinished {
                            task_id: i64::from(task_id) as u64,
                            collectables: collectables
                                .into_iter()
                                .filter(|c| matches!(c.kind(), collectable::Kind::Package))
                                .collect(),
                        }),
                        token,
                        uri: vessel_uri.to_string(),
                    },
                )),
            })
            .await
            .context("send builder upload message")?;

        info!(%task_id, build_id = %task.build_id, "Upload request sent to builder");

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct Handle {
    sender: mpsc::Sender<builder_stream::Outgoing>,
}

impl From<mpsc::Sender<builder_stream::Outgoing>> for Handle {
    fn from(sender: mpsc::Sender<builder_stream::Outgoing>) -> Self {
        Self { sender }
    }
}

#[derive(Debug)]
struct Connection {
    handle: Handle,
    last_status_update: Option<DateTime<Utc>>,
    status: Connected,
}

#[derive(Debug, Clone, Copy, strum::Display)]
enum Connected {
    Idle,
    Busy,
    BuildRequested {
        at: Instant,
        task: task::Id,
    },
    #[strum(serialize = "Building {task}")]
    Building {
        task: task::Id,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, strum::EnumDiscriminants)]
#[strum_discriminants(name(StatusKind), derive(Serialize, Deserialize))]
pub enum Status {
    Disconnected,
    Idle,
    Busy,
    Building { task: task::Id },
}

async fn persist_config(tx: &mut database::Transaction, config: &Config) -> Result<(), database::Error> {
    sqlx::query(
        "
        INSERT INTO builder (
          public_key,
          description,
          size
        )
        VALUES (?,?,?)
        ON CONFLICT(public_key) DO UPDATE SET
          description=excluded.description,
          size=excluded.size
        ",
    )
    .bind(config.public_key.to_string())
    .bind(&config.description)
    .bind(config.size.to_string())
    .execute(tx.as_mut())
    .await?;

    Ok(())
}
