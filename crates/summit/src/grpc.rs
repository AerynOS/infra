use std::{io, sync::Arc};

use async_trait::async_trait;
use chrono::Utc;
use http::Extensions;
use service::{
    Service, auth,
    crypto::PublicKey,
    grpc::{
        self,
        proto::summit::{
            CancelRequest, RetryRequest, builder_stream, repository_manager_stream,
            summit_service_server::{SummitService as GrpcSummitService, SummitServiceServer},
        },
    },
    token::VerifiedToken,
};
use snafu::{ResultExt, Snafu};
use tokio::{
    fs::{self, File},
    io::AsyncWriteExt,
    sync::mpsc,
};
use tokio_stream::{StreamExt, wrappers::ReceiverStream};
use tracing::{Instrument, Span, info};

use crate::{Config, builder, repository_manager, task, worker};

pub use service::grpc::auth::service as auth_service;

pub fn summit_service(
    state: &crate::State,
    config: Config,
    worker: worker::Sender,
) -> SummitServiceServer<SummitService> {
    SummitServiceServer::new(SummitService {
        state: Arc::new(State {
            worker,
            service: state.service.clone(),
            config,
        }),
    })
}

#[derive(Clone)]
pub struct SummitService {
    state: Arc<State>,
}

#[derive(Clone)]
struct State {
    worker: worker::Sender,
    service: service::State,
    config: Config,
}

#[async_trait]
impl GrpcSummitService for SummitService {
    type BuilderStream = ReceiverStream<Result<builder_stream::Outgoing, tonic::Status>>;
    type RepositoryManagerStream = ReceiverStream<Result<repository_manager_stream::Outgoing, tonic::Status>>;

    async fn retry(&self, request: tonic::Request<RetryRequest>) -> Result<tonic::Response<()>, tonic::Status> {
        let state = self.state.clone();

        grpc::handle(request, async move |request| retry(state, request).await).await
    }

    async fn cancel(&self, request: tonic::Request<CancelRequest>) -> Result<tonic::Response<()>, tonic::Status> {
        let state = self.state.clone();

        grpc::handle(request, async move |request| cancel(state, request).await).await
    }

    async fn refresh(&self, request: tonic::Request<()>) -> Result<tonic::Response<()>, tonic::Status> {
        let state = self.state.clone();

        grpc::handle(request, async move |request| refresh(state, request).await).await
    }

    async fn pause(&self, request: tonic::Request<()>) -> Result<tonic::Response<()>, tonic::Status> {
        let state = self.state.clone();

        grpc::handle(request, async move |request| pause(state, request).await).await
    }

    async fn resume(&self, request: tonic::Request<()>) -> Result<tonic::Response<()>, tonic::Status> {
        let state = self.state.clone();

        grpc::handle(request, async move |request| resume(state, request).await).await
    }

    async fn builder(
        &self,
        request: tonic::Request<tonic::Streaming<builder_stream::Incoming>>,
    ) -> Result<tonic::Response<Self::BuilderStream>, tonic::Status> {
        let state = self.state.clone();

        grpc::handle_streaming(request, move |extensions, stream, sender| {
            builder(state, extensions, stream, sender)
        })
    }

    async fn repository_manager(
        &self,
        request: tonic::Request<tonic::Streaming<repository_manager_stream::Incoming>>,
    ) -> Result<tonic::Response<Self::RepositoryManagerStream>, tonic::Status> {
        let state = self.state.clone();

        grpc::handle_streaming(request, move |extensions, stream, sender| {
            repository_manager(state, extensions, stream, sender)
        })
    }
}

#[tracing::instrument(
    skip_all,
    fields(
        task_id = %request.get_ref().task_id,
    )
)]
async fn retry(state: Arc<State>, request: tonic::Request<RetryRequest>) -> Result<(), Error> {
    let token = request
        .extensions()
        .get::<VerifiedToken>()
        .cloned()
        .ok_or(Error::MissingRequestToken)?;

    info!(
        client = %token.decoded.payload.client,
        "Retry task"
    );

    let _ = state.worker.send(worker::Message::RetryTask {
        task_id: (request.into_inner().task_id as i64).into(),
    });

    Ok(())
}

#[tracing::instrument(
    skip_all,
    fields(
        task_id = %request.get_ref().task_id,
    )
)]
async fn cancel(state: Arc<State>, request: tonic::Request<CancelRequest>) -> Result<(), Error> {
    let token = request
        .extensions()
        .get::<VerifiedToken>()
        .cloned()
        .ok_or(Error::MissingRequestToken)?;

    info!(
        client = %token.decoded.payload.client,
        "Cancel task"
    );

    let _ = state.worker.send(worker::Message::CancelTask {
        task_id: (request.into_inner().task_id as i64).into(),
    });

    Ok(())
}

#[tracing::instrument(skip_all)]
async fn refresh(state: Arc<State>, request: tonic::Request<()>) -> Result<(), Error> {
    let token = request
        .extensions()
        .get::<VerifiedToken>()
        .cloned()
        .ok_or(Error::MissingRequestToken)?;

    info!(
        client = %token.decoded.payload.client,
        "Refresh"
    );

    let _ = state.worker.send(worker::Message::ForceRefresh);

    Ok(())
}

#[tracing::instrument(skip_all)]
async fn pause(state: Arc<State>, request: tonic::Request<()>) -> Result<(), Error> {
    let token = request
        .extensions()
        .get::<VerifiedToken>()
        .cloned()
        .ok_or(Error::MissingRequestToken)?;

    info!(
        client = %token.decoded.payload.client,
        "Pause"
    );

    let _ = state.worker.send(worker::Message::Pause);

    Ok(())
}

#[tracing::instrument(skip_all)]
async fn resume(state: Arc<State>, request: tonic::Request<()>) -> Result<(), Error> {
    let token = request
        .extensions()
        .get::<VerifiedToken>()
        .cloned()
        .ok_or(Error::MissingRequestToken)?;

    info!(
        client = %token.decoded.payload.client,
        "Resume"
    );

    let _ = state.worker.send(worker::Message::Resume);

    Ok(())
}

#[tracing::instrument(skip_all, fields(builder_id, public_key))]
async fn builder(
    state: Arc<State>,
    extensions: Extensions,
    mut stream: tonic::Streaming<builder_stream::Incoming>,
    sender: mpsc::Sender<builder_stream::Outgoing>,
) -> Result<(), Error> {
    let span = Span::current();

    let token = extensions
        .get::<VerifiedToken>()
        .cloned()
        .ok_or(Error::MissingRequestToken)?;

    let (builder_id, public_key) = match token.decoded.payload.client {
        auth::Client::Service {
            service_id: id,
            public_key,
            service: Service::Avalanche,
        } => (id, public_key),
        client => return Err(Error::InvalidTokenClient { client }),
    };

    // Verify this is an actively configured builder. This will reject
    // previously issued JWT if we drop that builder / restart summit
    if !state
        .config
        .builders
        .iter()
        .any(|config| config.id == builder_id && config.public_key == public_key)
    {
        return Err(Error::UnconfiguredBuilder { builder_id, public_key });
    }

    span.record("builder_id", builder_id.clone());
    span.record("public_key", public_key.to_string());

    let handle = builder::Handle::from(sender);

    let _ = state.worker.send(worker::Message::Builder(
        builder_id.clone(),
        public_key,
        builder::Message::Connected(handle),
    ));

    let mut inner = async || {
        let mut log_file = None;

        while let Some(message) = stream.next().await {
            let Some(event) = message.context(BuilderStreamSnafu)?.event else {
                continue;
            };

            match event {
                builder_stream::incoming::Event::Status(builder_stream::BuilderStatus { building, task_id }) => {
                    let _ = state.worker.send(worker::Message::Builder(
                        builder_id.clone(),
                        public_key,
                        builder::Message::Status {
                            now: Utc::now(),
                            building,
                            task_id: task_id.map(|id| (id as i64).into()),
                        },
                    ));
                }
                builder_stream::incoming::Event::BuildStarted(task_id) => {
                    let parent = state.service.state_dir.join("logs").join(task_id.to_string());

                    let _ = fs::remove_dir_all(&parent).await;
                    let _ = fs::create_dir_all(&parent).await;

                    let path = parent.join("build.log");

                    let file = File::create(&path).await.context(CreateLogFileSnafu { task_id })?;

                    log_file = Some((task_id, path, file));
                }
                builder_stream::incoming::Event::BuildLog(builder_stream::BuildLog { chunk }) => {
                    if let Some((task_id, _, file)) = log_file.as_mut() {
                        file.write_all(&chunk)
                            .await
                            .context(WriteLogFileSnafu { task_id: *task_id })?;
                    }
                }
                builder_stream::incoming::Event::BuildSucceeded(builder_stream::BuildFinished {
                    task_id,
                    collectables,
                }) => {
                    let (_, log_path, mut log_file) = log_file.take().ok_or(Error::TakeLogFile { task_id })?;

                    log_file.flush().await.context(WriteLogFileSnafu { task_id })?;

                    let _ = state.worker.send(worker::Message::Builder(
                        builder_id.clone(),
                        public_key,
                        builder::Message::BuildSucceeded {
                            task_id: (task_id as i64).into(),
                            collectables,
                            log_path,
                        },
                    ));
                }
                builder_stream::incoming::Event::BuildFailed(task_id) => {
                    let log_path = if let Some((_, path, mut file)) = log_file.take() {
                        file.flush().await.context(WriteLogFileSnafu { task_id })?;

                        Some(path)
                    } else {
                        None
                    };

                    let _ = state.worker.send(worker::Message::Builder(
                        builder_id.clone(),
                        public_key,
                        builder::Message::BuildFailed {
                            task_id: (task_id as i64).into(),
                            log_path,
                        },
                    ));
                }
                builder_stream::incoming::Event::Busy(builder_stream::BuilderBusy {
                    requested_task_id,
                    in_progress_task_id,
                }) => {
                    let _ = state.worker.send(worker::Message::Builder(
                        builder_id.clone(),
                        public_key,
                        builder::Message::Busy {
                            requested: (requested_task_id as i64).into(),
                            in_progress: in_progress_task_id.map(|id| (id as i64).into()),
                        },
                    ));
                }
                builder_stream::incoming::Event::UploadFailed(task_id) => {
                    let _ = state.worker.send(worker::Message::Builder(
                        builder_id.clone(),
                        public_key,
                        builder::Message::UploadFailed {
                            task_id: (task_id as i64).into(),
                        },
                    ));
                }
            }
        }

        Ok(())
    };

    let result = inner().instrument(span).await;

    let _ = state.worker.send(worker::Message::Builder(
        builder_id,
        public_key,
        builder::Message::Disconnected,
    ));

    result
}

#[tracing::instrument(skip_all, fields(repository_manager_id, public_key))]
async fn repository_manager(
    state: Arc<State>,
    extensions: Extensions,
    mut stream: tonic::Streaming<repository_manager_stream::Incoming>,
    sender: mpsc::Sender<repository_manager_stream::Outgoing>,
) -> Result<(), Error> {
    let span = Span::current();

    let token = extensions
        .get::<VerifiedToken>()
        .cloned()
        .ok_or(Error::MissingRequestToken)?;

    let (repository_manager_id, public_key) = match token.decoded.payload.client {
        auth::Client::Service {
            service_id: id,
            public_key,
            service: Service::Vessel,
        } => (id, public_key),
        client => return Err(Error::InvalidTokenClient { client }),
    };

    // Verify this is the actively configured repo manager. This will reject
    // previously issued JWT if we change it / restart summit
    if state.config.repository_manager.id != repository_manager_id
        || state.config.repository_manager.public_key != public_key
    {
        return Err(Error::UnconfiguredRepositoryManager {
            repository_manager_id,
            public_key,
        });
    }

    span.record("repository_manager_id", repository_manager_id.clone());
    span.record("public_key", public_key.to_string());

    let handle = repository_manager::Handle::from(sender);

    let mut inner = async || {
        while let Some(message) = stream.next().await {
            let Some(event) = message.context(RepositoryManagerStreamSnafu)?.event else {
                continue;
            };

            match event {
                // Occurs once at the beginning of the connection
                repository_manager_stream::incoming::Event::Details(details) => {
                    let _ = state.worker.send(worker::Message::RepositoryManager(
                        repository_manager_id.clone(),
                        public_key,
                        repository_manager::Message::Connected {
                            handle: handle.clone(),
                            grpc_uri: details.grpc_uri.parse().context(ParseVesselGrpcUriSnafu)?,
                        },
                    ));
                }
                repository_manager_stream::incoming::Event::UploadToken(repository_manager_stream::UploadToken {
                    task_id,
                    collectables,
                    token,
                }) => {
                    let _ = state.worker.send(worker::Message::RepositoryManager(
                        repository_manager_id.clone(),
                        public_key,
                        repository_manager::Message::UploadToken {
                            task_id: task::Id::from(task_id as i64),
                            collectables,
                            token,
                        },
                    ));
                }
                repository_manager_stream::incoming::Event::ImportSucceeded(task_id) => {
                    let _ = state.worker.send(worker::Message::RepositoryManager(
                        repository_manager_id.clone(),
                        public_key,
                        repository_manager::Message::ImportSucceeded {
                            task_id: task::Id::from(task_id as i64),
                        },
                    ));
                }
                repository_manager_stream::incoming::Event::ImportFailed(task_id) => {
                    let _ = state.worker.send(worker::Message::RepositoryManager(
                        repository_manager_id.clone(),
                        public_key,
                        repository_manager::Message::ImportFailed {
                            task_id: task::Id::from(task_id as i64),
                        },
                    ));
                }
            }
        }

        Ok(())
    };

    let result = inner().instrument(span).await;

    let _ = state.worker.send(worker::Message::RepositoryManager(
        repository_manager_id,
        public_key,
        repository_manager::Message::Disconnected,
    ));

    result
}

#[derive(Debug, Snafu)]
enum Error {
    #[snafu(display("Token missing from request"))]
    MissingRequestToken,
    #[snafu(display("Invalid token client: {client}"))]
    InvalidTokenClient { client: auth::Client },
    #[snafu(display("Unconfigured builder {builder_id} with public key {public_key}"))]
    UnconfiguredBuilder { builder_id: String, public_key: PublicKey },
    #[snafu(display("Unconfigured repository manager {repository_manager_id} with public key {public_key}"))]
    UnconfiguredRepositoryManager {
        repository_manager_id: String,
        public_key: PublicKey,
    },
    #[snafu(display("Builder stream response"))]
    BuilderStream { source: tonic::Status },
    #[snafu(display("Repository manager stream response"))]
    RepositoryManagerStream { source: tonic::Status },
    #[snafu(display("Create log file for task {task_id}"))]
    CreateLogFile { source: io::Error, task_id: u64 },
    #[snafu(display("Write to log file for task {task_id}"))]
    WriteLogFile { source: io::Error, task_id: u64 },
    #[snafu(display("Missing log file for finished task {task_id}"))]
    TakeLogFile { task_id: u64 },
    #[snafu(display("Failed to parse vessel grpc uri"))]
    ParseVesselGrpcUri { source: http::uri::InvalidUri },
}

impl From<Error> for tonic::Status {
    fn from(error: Error) -> Self {
        match error {
            Error::MissingRequestToken
            | Error::InvalidTokenClient { .. }
            | Error::UnconfiguredBuilder { .. }
            | Error::UnconfiguredRepositoryManager { .. } => tonic::Status::unauthenticated(""),
            Error::BuilderStream { source } => source,
            Error::CreateLogFile { .. }
            | Error::WriteLogFile { .. }
            | Error::TakeLogFile { .. }
            | Error::RepositoryManagerStream { .. }
            | Error::ParseVesselGrpcUri { .. } => tonic::Status::internal(""),
        }
    }
}
