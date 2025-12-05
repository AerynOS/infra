use std::path::PathBuf;

use chrono::{DateTime, Utc};
use color_eyre::eyre::{Context, OptionExt, Report, Result};
use serde::{Deserialize, Serialize};
use service::{
    Endpoint, State,
    client::{AuthClient, EndpointAuth, VesselServiceClient},
    database::Transaction,
    endpoint, error,
    grpc::{
        collectable::{self, Collectable},
        remote::Remote,
        summit::{BuilderBuild, BuilderFinished, BuilderStreamOutgoing, BuilderUpload, builder_stream_outgoing},
        vessel::UploadTokenRequest,
    },
};
use tokio::{sync::mpsc, task::spawn_blocking};
use tracing::{error, info, warn};

use crate::{Task, task};

#[derive(Debug)]
pub enum Message {
    Connected(Handle),
    Disconnected,
    Status {
        now: DateTime<Utc>,
        building: Option<task::Id>,
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
        in_progress: task::Id,
    },
}

#[derive(Debug)]
pub enum Event {
    Connected,
    BuildFailed { task_id: task::Id },
    BuildRequeued,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Info {
    pub endpoint: endpoint::Id,
    pub last_seen: Option<DateTime<Utc>>,
    pub status: StatusKind,
    pub building: Option<task::Id>,
}

impl Info {
    pub fn disconnected(endpoint: endpoint::Id) -> Self {
        Self {
            endpoint,
            last_seen: None,
            status: StatusKind::Disconnected,
            building: None,
        }
    }
}

#[derive(Debug)]
pub struct Builder {
    pub endpoint: endpoint::Id,
    connection: Option<Connection>,
}

impl Builder {
    pub fn new(endpoint: endpoint::Id) -> Self {
        Self {
            endpoint,
            connection: None,
        }
    }

    pub fn info(&self) -> Info {
        let status = StatusKind::from(self.status());

        let (last_seen, building) = match self.connection {
            Some(Connection {
                last_seen,
                status: Connected::Idle,
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
            last_seen,
            status,
            building,
        }
    }

    pub fn is_idle(&self) -> bool {
        matches!(self.status(), Status::Idle)
    }

    pub fn status(&self) -> Status {
        match self.connection {
            Some(Connection {
                status: Connected::Idle,
                ..
            }) => Status::Idle,
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
            task = %task.id,
            build = task.build_id
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
            task = %queued.task.id,
            build = %queued.task.build_id
        )
    )]
    pub async fn build(&mut self, state: &State, queued: &task::Queued) -> Result<()> {
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

        let mut tx = state.service_db.begin().await.context("begin db tx")?;

        task::set_status(&mut tx, queued.task.id, task::Status::Building)
            .await
            .context("set status")?;
        task::set_allocated_builder(&mut tx, queued.task.id, Some(self.endpoint))
            .await
            .context("set builder")?;

        tx.commit().await.context("commit tx")?;

        info!("Task sent for build");

        Ok(())
    }

    #[tracing::instrument(name = "update_builder", skip_all, fields(builder = %self.endpoint))]
    pub async fn update(&mut self, state: &State, message: Message) -> Result<Option<Event>> {
        match message {
            Message::Connected(handle) => {
                info!("Builder connected");

                self.connection = Some(Connection {
                    handle,
                    last_seen: Utc::now(),
                    status: Connected::Idle,
                });

                return Ok(Some(Event::Connected));
            }
            Message::Disconnected => {
                warn!("Builder disconnected");

                self.connection = None;
            }
            Message::Status { now, building } => {
                if let Some(connection) = self.connection.as_mut() {
                    connection.last_seen = now;

                    // TODO: Invalidate tasks which still show as building
                    // on this builder that aren't this one
                    connection.status = match building {
                        Some(task) => Connected::Building { task },
                        None => Connected::Idle,
                    };
                }
            }
            Message::BuildSucceeded {
                task_id,
                collectables,
                log_path,
            } => {
                let connection = self.connection.as_mut().ok_or_eyre("builder not connected")?;
                connection.status = Connected::Idle;

                let mut conn = state.service_db.acquire().await.context("acquire db conn")?;

                let task = task::query(&mut conn, task::query::Params::default().id(task_id))
                    .await
                    .context("get task")?
                    .tasks
                    .into_iter()
                    .next()
                    .ok_or_eyre("task is missing")?;

                let vessel = Endpoint::list(&mut *conn)
                    .await
                    .context("list endpoints")?
                    .into_iter()
                    .find(|endpoint| {
                        matches!(endpoint.status, endpoint::Status::Operational)
                            && matches!(endpoint.role, endpoint::Role::RepositoryManager)
                    })
                    .ok_or_eyre("no operational vessel instance")?;

                // Reject tasks that somehow already failed
                if matches!(task.status, task::Status::Failed) {
                    error!(task = %task_id, "Blocking inclusion of previously failed build");
                    return Ok(None);
                }

                drop(conn);

                let mut client = VesselServiceClient::connect_with_auth(
                    vessel.host_address.clone(),
                    None,
                    EndpointAuth::new(&vessel, state.service_db.clone(), state.key_pair.clone()),
                )
                .await
                .context("connect vessel client")?;

                let result = client
                    .upload_token(UploadTokenRequest {
                        task_id: i64::from(task_id) as u64,
                        collectables: collectables
                            .iter()
                            .filter(|c| matches!(c.kind(), collectable::Kind::Package))
                            .cloned()
                            .collect(),
                    })
                    .await
                    .context("send import request");

                let (status, failed) = match result {
                    Ok(response) => {
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
                            .context("send builder stream message")?;

                        info!(task = %task_id, "Upload token sent to builder");

                        (task::Status::Publishing, false)
                    }
                    Err(error) => {
                        error!(
                            error = error::chain(&*error),
                            task = %task_id,
                            "Request failed to get upload token"
                        );

                        (task::Status::Failed, true)
                    }
                };

                let mut tx = state.service_db.begin().await.context("begin db tx")?;

                stash_log(&mut tx, state, task_id, log_path)
                    .await
                    .context("stash build log")?;

                task::set_status(&mut tx, task_id, status).await.context("set status")?;

                tx.commit().await.context("commit tx")?;

                if failed {
                    return Ok(Some(Event::BuildFailed { task_id }));
                }
            }
            Message::BuildFailed { task_id, log_path } => {
                let connection = self.connection.as_mut().ok_or_eyre("builder not connected")?;
                connection.status = Connected::Idle;

                let mut tx = state.service_db.begin().await.context("begin db tx")?;

                if let Some(path) = log_path {
                    stash_log(&mut tx, state, task_id, path)
                        .await
                        .context("stash build log")?;
                }

                task::set_status(&mut tx, task_id, task::Status::Failed)
                    .await
                    .context("set status")?;

                tx.commit().await.context("commit tx")?;

                info!(task = %task_id, "Build marked as failed");

                return Ok(Some(Event::BuildFailed { task_id }));
            }
            Message::Busy { requested, in_progress } => {
                let connection = self.connection.as_mut().ok_or_eyre("builder not connected")?;
                connection.status = Connected::Building { task: in_progress };

                let mut tx = state.service_db.begin().await.context("begin db tx")?;

                task::set_status(&mut tx, requested, task::Status::New)
                    .await
                    .context("set status")?;
                task::set_allocated_builder(&mut tx, requested, None)
                    .await
                    .context("set builder")?;

                tx.commit().await.context("commit tx")?;

                info!(task = %requested, "Build requeued, builder is busy");

                return Ok(Some(Event::BuildRequeued));
            }
        }

        Ok(None)
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

#[derive(Debug)]
enum Connected {
    Idle,
    #[allow(dead_code)]
    Building {
        task: task::Id,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, strum::EnumDiscriminants)]
#[strum_discriminants(name(StatusKind), derive(Serialize, Deserialize))]
pub enum Status {
    Disconnected,
    Idle,
    Building { task: task::Id },
}

async fn stash_log(tx: &mut Transaction, state: &State, task_id: task::Id, path: PathBuf) -> Result<()> {
    let compressed_path = spawn_blocking(move || {
        use flate2::write::GzEncoder;
        use std::fs::{self, File};
        use std::io::{self, Write};

        let mut plain_file = File::open(&path).context("open plain file")?;

        let gz_path = PathBuf::from(format!("{}.gz", path.display()));
        let mut gz_file = File::create(&gz_path).context("create compressed file")?;

        let mut encoder = GzEncoder::new(&mut gz_file, flate2::Compression::new(9));

        io::copy(&mut plain_file, &mut encoder)?;

        encoder.finish()?;
        gz_file.flush()?;

        fs::remove_file(path).context("remove plain file")?;

        Result::<_, Report>::Ok(gz_path)
    })
    .await
    .context("join handle")?
    .context("compress log file")?;

    let relative_path = compressed_path
        .strip_prefix(&state.state_dir)
        .context("log is descendent of state dir")?;

    task::set_log_path(tx, task_id, relative_path)
        .await
        .context("set log path")?;

    Ok(())
}
