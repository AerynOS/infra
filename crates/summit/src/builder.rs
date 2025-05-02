use std::path::PathBuf;

use color_eyre::eyre::{Context, OptionExt, Report, Result};
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
use tokio::{sync::mpsc, task::spawn_blocking, time::Instant};
use tracing::{error, info, warn};

use crate::task;

#[derive(Debug)]
pub enum Message {
    Connected(Handle),
    Disconnected,
    Status {
        now: Instant,
        building: Option<task::Id>,
    },
    BuildSucceeded {
        task_id: task::Id,
        collectables: Vec<Collectable>,
        log_path: PathBuf,
    },
    BuildFailed {
        task_id: task::Id,
        log_path: Option<PathBuf>,
    },
}

#[derive(Debug)]
pub enum Event {
    Connected,
    BuildFailed { task_id: task::Id },
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
                status: Connected::Building { .. },
                ..
            }) => Status::Building,
            None => Status::Disconnected,
        }
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

        connection.status = Connected::Building {
            task_id: queued.task.id,
        };

        let mut tx = state.service_db.begin().await.context("begin db tx")?;

        task::set_status(&mut tx, queued.task.id, task::Status::Building)
            .await
            .context("set status")?;
        task::set_allocated_builder(&mut tx, queued.task.id, &self.endpoint)
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
                    last_seen: Instant::now(),
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
                        Some(task_id) => Connected::Building { task_id },
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
                    error!("Blocking inclusion of previously failed build");
                    return Ok(None);
                }

                drop(conn);

                let mut client = VesselServiceClient::connect_with_auth(
                    vessel.host_address.clone(),
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

                        info!("Upload token sent to builder");

                        (task::Status::Publishing, false)
                    }
                    Err(error) => {
                        error!(error = error::chain(&*error), "Request failed to get upload token");

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

                info!("Build marked as failed");

                return Ok(Some(Event::BuildFailed { task_id }));
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
    pub last_seen: Instant,
    pub status: Connected,
}

#[derive(Debug)]
enum Connected {
    Idle,
    Building { task_id: task::Id },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Status {
    Disconnected,
    Idle,
    Building,
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
