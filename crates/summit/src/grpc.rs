use std::{io, sync::Arc};

use async_trait::async_trait;
use http::Extensions;
use service::{
    endpoint,
    grpc::{
        self,
        summit::{
            BuilderBusy, BuilderFinished, BuilderLog, BuilderStatus, BuilderStreamIncoming, BuilderStreamOutgoing,
            FailRequest, ImportRequest, RetryRequest, builder_stream_incoming_event,
            summit_service_server::{SummitService, SummitServiceServer},
        },
    },
    token::VerifiedToken,
    tracing::{OpenTelemetryContext, OpenTelemetrySpanExt},
};
use snafu::{ResultExt, Snafu};
use tokio::{
    fs::{self, File},
    io::AsyncWriteExt,
    sync::mpsc,
    time::Instant,
};
use tokio_stream::{StreamExt, wrappers::ReceiverStream};
use tracing::{Instrument, Span, info, info_span, warn};

use crate::{builder, worker};

pub type Server = SummitServiceServer<Service>;

pub fn service(state: crate::State, worker: worker::Sender) -> Server {
    Server::new(Service {
        state: Arc::new(State { worker, service: state }),
    })
}

#[derive(Clone)]
pub struct Service {
    state: Arc<State>,
}

#[derive(Clone)]
struct State {
    worker: worker::Sender,
    service: service::State,
}

#[async_trait]
impl SummitService for Service {
    type BuilderStream = ReceiverStream<Result<BuilderStreamOutgoing, tonic::Status>>;

    async fn import_succeeded(
        &self,
        request: tonic::Request<ImportRequest>,
    ) -> Result<tonic::Response<()>, tonic::Status> {
        let state = self.state.clone();

        grpc::handle(request, async move |request| import_succeeded(state, request).await).await
    }

    async fn import_failed(
        &self,
        request: tonic::Request<ImportRequest>,
    ) -> Result<tonic::Response<()>, tonic::Status> {
        let state = self.state.clone();

        grpc::handle(request, async move |request| import_failed(state, request).await).await
    }

    async fn retry(&self, request: tonic::Request<RetryRequest>) -> Result<tonic::Response<()>, tonic::Status> {
        let state = self.state.clone();

        grpc::handle(request, async move |request| retry(state, request).await).await
    }

    async fn fail(&self, request: tonic::Request<FailRequest>) -> Result<tonic::Response<()>, tonic::Status> {
        let state = self.state.clone();

        grpc::handle(request, async move |request| fail(state, request).await).await
    }

    async fn refresh(&self, request: tonic::Request<()>) -> Result<tonic::Response<()>, tonic::Status> {
        let state = self.state.clone();

        grpc::handle(request, async move |request| refresh(state, request).await).await
    }

    async fn builder(
        &self,
        request: tonic::Request<tonic::Streaming<BuilderStreamIncoming>>,
    ) -> std::result::Result<tonic::Response<Self::BuilderStream>, tonic::Status> {
        let state = self.state.clone();

        grpc::handle_streaming(request, move |extensions, stream, sender| {
            builder(state, extensions, stream, sender)
        })
    }
}

#[tracing::instrument(
    skip_all,
    fields(
        task_id = %request.get_ref().task_id,
    )
)]
async fn import_succeeded(state: Arc<State>, request: tonic::Request<ImportRequest>) -> Result<(), Error> {
    let token = request
        .extensions()
        .get::<VerifiedToken>()
        .cloned()
        .ok_or(Error::MissingRequestToken)?;

    let endpoint_id = token
        .decoded
        .payload
        .sub
        .parse::<endpoint::Id>()
        .context(InvalidEndpointSnafu)?;

    info!(
        endpoint = %endpoint_id,
        "Import succeeded"
    );

    let _ = state.worker.send(worker::Message::ImportSucceeded {
        task_id: (request.into_inner().task_id as i64).into(),
        span: Span::current(),
    });

    Ok(())
}

#[tracing::instrument(
    skip_all,
    fields(
        task_id = %request.get_ref().task_id,
    )
)]
async fn import_failed(state: Arc<State>, request: tonic::Request<ImportRequest>) -> Result<(), Error> {
    let token = request
        .extensions()
        .get::<VerifiedToken>()
        .cloned()
        .ok_or(Error::MissingRequestToken)?;

    let endpoint_id = token
        .decoded
        .payload
        .sub
        .parse::<endpoint::Id>()
        .context(InvalidEndpointSnafu)?;

    warn!(
        endpoint = %endpoint_id,
        "Import failed"
    );

    let _ = state.worker.send(worker::Message::ImportFailed {
        task_id: (request.into_inner().task_id as i64).into(),
        span: Span::current(),
    });

    Ok(())
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

    let account_id = token.decoded.payload.account_id;

    info!(
        account = %account_id,
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
async fn fail(state: Arc<State>, request: tonic::Request<FailRequest>) -> Result<(), Error> {
    let token = request
        .extensions()
        .get::<VerifiedToken>()
        .cloned()
        .ok_or(Error::MissingRequestToken)?;

    let account_id = token.decoded.payload.account_id;

    info!(
        account = %account_id,
        "Fail task"
    );

    let _ = state.worker.send(worker::Message::FailTask {
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

    let account_id = token.decoded.payload.account_id;

    info!(
        account = %account_id,
        "Refresh"
    );

    let _ = state.worker.send(worker::Message::ForceRefresh);

    Ok(())
}

#[tracing::instrument(skip_all, fields(endpoint))]
async fn builder(
    state: Arc<State>,
    extensions: Extensions,
    mut stream: tonic::Streaming<BuilderStreamIncoming>,
    sender: mpsc::Sender<BuilderStreamOutgoing>,
) -> Result<(), Error> {
    let span = Span::current();

    let token = extensions
        .get::<VerifiedToken>()
        .cloned()
        .ok_or(Error::MissingRequestToken)?;

    let endpoint_id = token
        .decoded
        .payload
        .sub
        .parse::<endpoint::Id>()
        .context(InvalidEndpointSnafu)?;

    span.record("endpoint", endpoint_id.to_string());

    let handle = builder::Handle::from(sender);

    let _ = state.worker.send(worker::Message::Builder(
        endpoint_id,
        builder::Message::Connected(handle),
    ));

    let mut inner = async || {
        let mut log_file = None;

        while let Some(result) = stream.next().await {
            let message = result.context(BuilderStreamSnafu)?;

            let Some(event) = message.event.and_then(|e| e.event) else {
                continue;
            };

            let context = message
                .tracing_context
                .as_ref()
                .map(grpc::extract_tracing_context)
                .unwrap_or(OpenTelemetryContext::current());
            let span = info_span!(parent: None, "stream_event");
            span.set_parent(context);

            let cloned_span = span.clone();

            async {
                match event {
                    builder_stream_incoming_event::Event::Status(BuilderStatus { building }) => {
                        let _ = state.worker.send(worker::Message::Builder(
                            endpoint_id,
                            builder::Message::Status {
                                now: Instant::now(),
                                building: building.map(|id| (id as i64).into()),
                            },
                        ));
                    }
                    builder_stream_incoming_event::Event::BuildStarted(task_id) => {
                        let parent = state.service.state_dir.join("logs").join(task_id.to_string());

                        let _ = fs::remove_dir_all(&parent).await;
                        let _ = fs::create_dir_all(&parent).await;

                        let path = parent.join("build.log");

                        let file = File::create(&path).await.context(CreateLogFileSnafu { task_id })?;

                        log_file = Some((task_id, path, file));
                    }
                    builder_stream_incoming_event::Event::BuildLog(BuilderLog { chunk }) => {
                        // Disable tracing for this as its very noisy & wasteful
                        let span = Span::none();

                        async {
                            if let Some((task_id, _, file)) = log_file.as_mut() {
                                file.write_all(&chunk)
                                    .await
                                    .context(WriteLogFileSnafu { task_id: *task_id })?;
                            }

                            Ok(())
                        }
                        .instrument(span)
                        .await?;
                    }
                    builder_stream_incoming_event::Event::BuildSucceeded(BuilderFinished { task_id, collectables }) => {
                        let (_, log_path, mut log_file) = log_file.take().ok_or(Error::TakeLogFile { task_id })?;

                        log_file.flush().await.context(WriteLogFileSnafu { task_id })?;

                        let _ = state.worker.send(worker::Message::Builder(
                            endpoint_id,
                            builder::Message::BuildSucceeded {
                                task_id: (task_id as i64).into(),
                                collectables,
                                log_path,
                                span,
                            },
                        ));
                    }
                    builder_stream_incoming_event::Event::BuildFailed(task_id) => {
                        let log_path = if let Some((_, path, mut file)) = log_file.take() {
                            file.flush().await.context(WriteLogFileSnafu { task_id })?;

                            Some(path)
                        } else {
                            None
                        };

                        let _ = state.worker.send(worker::Message::Builder(
                            endpoint_id,
                            builder::Message::BuildFailed {
                                task_id: (task_id as i64).into(),
                                log_path,
                                span,
                            },
                        ));
                    }
                    builder_stream_incoming_event::Event::Busy(BuilderBusy {
                        requested_task_id,
                        in_progress_task_id,
                    }) => {
                        let _ = state.worker.send(worker::Message::Builder(
                            endpoint_id,
                            builder::Message::Busy {
                                requested: (requested_task_id as i64).into(),
                                in_progress: (in_progress_task_id as i64).into(),
                                span,
                            },
                        ));
                    }
                }

                Ok(())
            }
            .instrument(cloned_span)
            .await?;
        }

        Ok(())
    };

    match inner().instrument(span).await {
        Ok(()) => Ok(()),
        Err(e) => {
            let _ = state
                .worker
                .send(worker::Message::Builder(endpoint_id, builder::Message::Disconnected));
            Err(e)
        }
    }
}

#[derive(Debug, Snafu)]
enum Error {
    #[snafu(display("Token missing from request"))]
    MissingRequestToken,
    #[snafu(display("Invalid endpoint"))]
    InvalidEndpoint { source: uuid::Error },
    #[snafu(display("Builder stream response"))]
    BuilderStream { source: tonic::Status },
    #[snafu(display("Create log file for task {task_id}"))]
    CreateLogFile { source: io::Error, task_id: u64 },
    #[snafu(display("Write to log file for task {task_id}"))]
    WriteLogFile { source: io::Error, task_id: u64 },
    #[snafu(display("Missing log file for finished task {task_id}"))]
    TakeLogFile { task_id: u64 },
}

impl From<Error> for tonic::Status {
    fn from(error: Error) -> Self {
        match error {
            Error::MissingRequestToken => tonic::Status::unauthenticated(""),
            Error::InvalidEndpoint { .. } => tonic::Status::invalid_argument(""),
            Error::BuilderStream { source } => source,
            Error::CreateLogFile { .. } | Error::WriteLogFile { .. } | Error::TakeLogFile { .. } => {
                tonic::Status::internal("")
            }
        }
    }
}
