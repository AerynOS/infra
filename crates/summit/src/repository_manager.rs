use std::collections::HashMap;

use color_eyre::eyre::{Context as _, OptionExt as _, Result};
use http::Uri;
use serde::Deserialize;
use service::{
    crypto::PublicKey,
    grpc::proto::{
        common::Collectable,
        summit::repository_manager_stream,
        vessel::command::{self, inner::Command},
    },
};
use tokio::sync::{mpsc, oneshot};
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::task;

#[derive(Debug)]
pub enum Message {
    Connected {
        handle: Handle,
        grpc_uri: Uri,
    },
    Disconnected,
    UploadToken {
        task_id: task::Id,
        collectables: Vec<Collectable>,
        token: String,
    },
    ImportSucceeded {
        task_id: task::Id,
    },
    ImportFailed {
        task_id: task::Id,
    },
    Command {
        command: Command,
        resp: oneshot::Sender<command::Response>,
    },
    CommandResponse(command::Response),
}

#[derive(Debug)]
pub enum Event {
    UploadToken {
        task_id: task::Id,
        collectables: Vec<Collectable>,
        token: String,
        vessel_uri: Uri,
    },
    ImportSucceeded {
        task_id: task::Id,
    },
    ImportFailed {
        task_id: task::Id,
    },
}

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    /// Unique id of the repository manager for use as an internal identifier
    /// for things like logs
    pub id: String,
    pub public_key: PublicKey,
    pub description: String,
}

#[derive(Debug)]
pub struct RepositoryManager {
    pub config: Config,
    connection: Option<Connection>,
    pending_commands: HashMap<String, oneshot::Sender<command::Response>>,
}

impl RepositoryManager {
    pub fn new(config: Config) -> Self {
        Self {
            config,
            connection: None,
            pending_commands: HashMap::new(),
        }
    }

    #[tracing::instrument(skip_all, fields(%task_id))]
    pub async fn request_upload_token(&self, task_id: task::Id, collectables: Vec<Collectable>) -> Result<()> {
        let connection = self
            .connection
            .as_ref()
            .ok_or_eyre("repository manager not connected")?;

        connection
            .handle
            .sender
            .send(repository_manager_stream::Outgoing {
                event: Some(repository_manager_stream::outgoing::Event::RequestUploadToken(
                    repository_manager_stream::RequestUploadToken {
                        task_id: i64::from(task_id) as u64,
                        collectables,
                    },
                )),
            })
            .await
            .context("send upload token request")?;

        info!("Upload token request sent");

        Ok(())
    }

    #[tracing::instrument(name = "update_repository_manager", skip_all, fields(repository_manager = %self.config.id))]
    pub async fn update(&mut self, message: Message) -> Option<Event> {
        match message {
            Message::Connected { handle, grpc_uri } => {
                info!("Repository manager connected");

                self.connection = Some(Connection { handle, grpc_uri });

                None
            }
            Message::Disconnected => {
                info!("Repository manager disconnected");

                self.connection = None;

                None
            }
            Message::UploadToken {
                task_id,
                collectables,
                token,
            } => {
                info!(%task_id, "Upload token issued");

                match &self.connection {
                    Some(connection) => Some(Event::UploadToken {
                        task_id,
                        collectables,
                        token,
                        vessel_uri: connection.grpc_uri.clone(),
                    }),
                    None => {
                        error!("Repository manager disconnected after issuing upload token");
                        Some(Event::ImportFailed { task_id })
                    }
                }
            }
            Message::ImportSucceeded { task_id } => {
                info!(%task_id, "Import succeeded");
                Some(Event::ImportSucceeded { task_id })
            }
            Message::ImportFailed { task_id } => {
                info!(%task_id, "Import failed");
                Some(Event::ImportFailed { task_id })
            }
            Message::Command { command, resp } => {
                let request_id = Uuid::new_v4().to_string();

                match &self.connection {
                    Some(connection) => {
                        if connection
                            .handle
                            .sender
                            .send(repository_manager_stream::Outgoing {
                                event: Some(repository_manager_stream::outgoing::Event::Command(command::Request {
                                    request_id: request_id.clone(),
                                    command: Some(command::Inner { command: Some(command) }),
                                })),
                            })
                            .await
                            .is_err()
                        {
                            let _ = resp.send(command::Response {
                                request_id,
                                success: false,
                                error: Some("internal error".to_owned()),
                            });

                            return None;
                        }

                        self.pending_commands.insert(request_id, resp);

                        None
                    }
                    None => {
                        warn!("Repository manager disconnected while forwarding command");

                        let _ = resp.send(command::Response {
                            request_id,
                            success: false,
                            error: Some("disconnected".to_owned()),
                        });

                        None
                    }
                }
            }
            Message::CommandResponse(resp) => match self.pending_commands.remove(&resp.request_id) {
                Some(sender) => {
                    let _ = sender.send(resp);

                    None
                }
                None => {
                    warn!("Pending command missing after response received");

                    None
                }
            },
        }
    }
}

#[derive(Debug, Clone)]
pub struct Handle {
    sender: mpsc::Sender<repository_manager_stream::Outgoing>,
}

impl From<mpsc::Sender<repository_manager_stream::Outgoing>> for Handle {
    fn from(sender: mpsc::Sender<repository_manager_stream::Outgoing>) -> Self {
        Self { sender }
    }
}

#[derive(Debug)]
struct Connection {
    handle: Handle,
    grpc_uri: Uri,
}
