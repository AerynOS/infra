use color_eyre::eyre::{Context as _, OptionExt as _, Result};
use http::Uri;
use serde::Deserialize;
use service::{
    crypto::PublicKey,
    grpc::proto::{common::Collectable, summit::repository_manager_stream},
};
use tokio::sync::mpsc;
use tracing::{error, info};

use crate::{Builder, task};

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
pub struct RepositoryMananger {
    pub config: Config,
    connection: Option<Connection>,
}

impl RepositoryMananger {
    pub fn new(config: Config) -> Self {
        Self {
            config,
            connection: None,
        }
    }

    #[tracing::instrument(skip_all, fields(%task_id, builder = builder.config.id))]
    pub async fn request_upload_token(
        &self,
        task_id: task::Id,
        collectables: Vec<Collectable>,
        builder: &Builder,
    ) -> Result<()> {
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
                        builder_id: builder.config.id.clone(),
                        builder_public_key: builder.config.public_key.to_string(),
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
