use std::path::PathBuf;

use chrono::{DateTime, Utc};
use color_eyre::eyre::{Context, OptionExt, Result};
use serde::{Deserialize, Serialize};
use service::{
    Endpoint, State,
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
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::{Task, config::Size, task};

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
    Idle,
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
    connection: Option<Connection>,
}

impl Builder {
    pub fn new(endpoint: endpoint::Id, config: &Config) -> Self {
        Self {
            endpoint,
            size: config.size,
            connection: None,
        }
    }

    pub fn info(&self) -> Info {
        let status = StatusKind::from(self.status());

        let (last_seen, building) = match self.connection {
            Some(Connection {
                last_seen,
                status: Connected::Idle | Connected::Busy,
                ..
            }) => (Some(last_seen), None),
            Some(Connection {
                last_seen,
                status: Connected::Building { task },
                ..
            }) => (Some(last_seen), Some(task)),
            None => (None, None),
        };

        Info {
            endpoint: self.endpoint,
            size: self.size,
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
                status: Connected::Building { task },
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
    pub async fn cancel_build(&mut self, task: &Task) -> Result<()> {
        match self.connection.as_mut() {
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

        connection.status = Connected::Building { task: queued.task.id };

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
                    last_seen: Utc::now(),
                    status: Connected::Idle,
                });
            }
            Message::Disconnected => {
                warn!("Builder disconnected");

                self.connection = None;
            }
            Message::Status { now, building, task_id } => {
                let mut event = None;

                if let Some(connection) = self.connection.as_mut() {
                    connection.last_seen = now;

                    // TODO: Invalidate tasks which still show as building
                    // on this builder that aren't this one
                    connection.status = match (building, task_id) {
                        (true, Some(task)) => Connected::Building { task },
                        (true, None) => Connected::Busy,
                        (false, _) => {
                            event = Some(Event::Idle);

                            Connected::Idle
                        }
                    };

                    debug!(status = %connection.status, "Builder status updated");
                }

                return event;
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

        let mut conn = state.service_db.acquire().await.context("acquire db connection")?;

        let task = task::get(conn.as_mut(), task_id).await.context("get task")?;

        let vessel = Endpoint::list(conn.as_mut())
            .await
            .context("list endpoints")?
            .into_iter()
            .find(|endpoint| {
                matches!(endpoint.status, endpoint::Status::Operational)
                    && matches!(endpoint.role, endpoint::Role::RepositoryManager)
            })
            .ok_or_eyre("no operational vessel instance")?;

        drop(conn);

        let mut client = VesselServiceClient::connect_with_auth(
            vessel.host_address.clone(),
            None,
            EndpointAuth::new(&vessel, state.service_db.clone(), state.key_pair.clone()),
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
    pub handle: Handle,
    pub last_seen: DateTime<Utc>,
    pub status: Connected,
}

#[derive(Debug, strum::Display)]
enum Connected {
    Idle,
    Busy,
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
