use std::{path::PathBuf, time::Duration};

use chrono::{DateTime, Utc};
use color_eyre::eyre::{Context, OptionExt, Result};
use serde::{Deserialize, Serialize};
use service::{
    Endpoint,
    client::{AuthClient, EndpointAuth, VesselServiceClient},
    crypto::PublicKey,
    endpoint,
    grpc::{
        collectable::{self, Collectable},
        remote::Remote,
        summit::{BuilderBuild, BuilderFinished, BuilderStreamOutgoing, BuilderUpload, builder_stream_outgoing},
        vessel::UploadTokenRequest,
    },
};
use tokio::{sync::mpsc, time::Instant};
use tracing::{debug, info, warn};

use crate::{State, Task, config::Size, task};

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
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Info {
    pub endpoint: endpoint::Id,
    pub last_seen: Option<DateTime<Utc>>,
    pub status: StatusKind,
    pub building: Option<task::Id>,
    pub size: Size,
    pub description: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Key {
    pub index: usize,
    pub endpoint: endpoint::Id,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub public_key: PublicKey,
    pub size: Size,
}

#[derive(Debug)]
pub struct Builder {
    pub endpoint: endpoint::Id,
    pub size: Size,
    description: Option<String>,
    connection: Option<Connection>,
}

impl Builder {
    pub fn new(endpoint: &Endpoint, config: &Config) -> Self {
        Self {
            endpoint: endpoint.id,
            size: config.size,
            description: endpoint.description.clone(),
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
            endpoint: self.endpoint,
            size: self.size,
            last_seen,
            status,
            building,
            description: self.description.clone(),
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
            builder = %self.endpoint,
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
                    .send(BuilderStreamOutgoing {
                        event: Some(builder_stream_outgoing::Event::CancelBuild(())),
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
            builder = %self.endpoint,
            task_id = %queued.task.id,
            build_id = %queued.task.build_id,
            task_size = %queued.size,
            builder_size = %self.size,
        )
    )]
    pub async fn build(&mut self, queued: &task::Queued) -> Result<()> {
        let connection = self.connection.as_mut().ok_or_eyre("builder not connected")?;

        connection
            .handle
            .sender
            .send(BuilderStreamOutgoing {
                event: Some(builder_stream_outgoing::Event::Build(BuilderBuild {
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
                        .map(|(idx, uri)| Remote {
                            index_uri: uri.to_string(),
                            name: format!("repo{idx}"),
                            priority: idx as u64 * 10,
                        })
                        .collect(),
                })),
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

    #[tracing::instrument(name = "update_builder", skip_all, fields(builder = %self.endpoint))]
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
        }

        None
    }

    pub async fn request_upload(&self, state: &State, task_id: task::Id, collectables: Vec<Collectable>) -> Result<()> {
        let connection = self.connection.as_ref().ok_or_eyre("builder not connected")?;

        let mut conn = state.service_db().acquire().await.context("acquire db connection")?;

        let task = task::get(conn.as_mut(), task_id).await.context("get task")?;

        let vessel = Endpoint::list(conn.as_mut())
            .await
            .context("list endpoints")?
            .into_iter()
            .find(|endpoint| matches!(endpoint.role, endpoint::Role::RepositoryManager))
            .ok_or_eyre("no vessel instance")?;

        drop(conn);

        let mut client = VesselServiceClient::connect_with_auth(
            vessel.host_address.clone(),
            None,
            EndpointAuth::new(&vessel, state.service_db().clone(), state.service.key_pair.clone()),
        )
        .await
        .context("connect vessel client")?;

        let response = client
            .upload_token(UploadTokenRequest {
                task_id: i64::from(task_id) as u64,
                collectables: collectables
                    .iter()
                    .filter(|c| matches!(c.kind(), collectable::Kind::Package))
                    .cloned()
                    .collect(),
            })
            .await
            .context("send upload token request")?;

        connection
            .handle
            .sender
            .send(BuilderStreamOutgoing {
                event: Some(builder_stream_outgoing::Event::Upload(BuilderUpload {
                    build: Some(BuilderFinished {
                        task_id: i64::from(task_id) as u64,
                        collectables: collectables
                            .into_iter()
                            .filter(|c| matches!(c.kind(), collectable::Kind::Package))
                            .collect(),
                    }),
                    token: response.into_inner().token,
                    uri: vessel.host_address.to_string(),
                })),
            })
            .await
            .context("send builder upload message")?;

        info!(%task_id, build_id = %task.build_id, "Upload token sent to builder");

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct Handle {
    sender: mpsc::Sender<BuilderStreamOutgoing>,
}

impl From<mpsc::Sender<BuilderStreamOutgoing>> for Handle {
    fn from(sender: mpsc::Sender<BuilderStreamOutgoing>) -> Self {
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
