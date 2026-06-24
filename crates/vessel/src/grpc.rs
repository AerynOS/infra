use std::sync::Arc;

use async_trait::async_trait;
use color_eyre::eyre::{OptionExt as _, Report, bail, eyre};
use futures_util::TryStreamExt;
use prost::Message;
use service::{
    Service, crypto,
    grpc::{
        self,
        proto::{
            self,
            summit::builder_stream::BuildFinished,
            vessel::{
                AddTagRequest, CommandResponse, RemoveTagRequest, UpdateStreamRequest, UpgradeFormatRequest,
                UploadRequest, upgrade_format,
                vessel_service_server::{VesselService as GrpcVesselService, VesselServiceServer},
            },
        },
    },
    token::VerifiedToken,
};
use snafu::{OptionExt, ResultExt, Snafu};
use tokio::sync::{mpsc, oneshot};
use tracing::{Span, error, info, warn};

use crate::{
    channel::{self, FormatUpgrade},
    upload, worker,
};

pub fn vessel_service(state: service::State, worker: worker::Sender) -> VesselServiceServer<VesselService> {
    VesselServiceServer::new(VesselService {
        state: Arc::new(State { service: state, worker }),
    })
}

#[derive(Clone)]
pub struct VesselService {
    state: Arc<State>,
}

#[derive(Clone)]
struct State {
    service: service::State,
    worker: worker::Sender,
}

#[async_trait]
impl GrpcVesselService for VesselService {
    async fn upload(
        &self,
        request: tonic::Request<tonic::Streaming<UploadRequest>>,
    ) -> Result<tonic::Response<()>, tonic::Status> {
        let state = self.state.clone();

        grpc::handle(request, async move |request| upload(state, request).await).await
    }

    async fn update_stream(
        &self,
        request: tonic::Request<UpdateStreamRequest>,
    ) -> Result<tonic::Response<CommandResponse>, tonic::Status> {
        let state = self.state.clone();

        grpc::handle(request, async move |request| update_stream(state, request).await).await
    }

    async fn add_tag(
        &self,
        request: tonic::Request<AddTagRequest>,
    ) -> Result<tonic::Response<CommandResponse>, tonic::Status> {
        let state = self.state.clone();

        grpc::handle(request, async move |request| add_tag(state, request).await).await
    }

    async fn remove_tag(
        &self,
        request: tonic::Request<RemoveTagRequest>,
    ) -> Result<tonic::Response<CommandResponse>, tonic::Status> {
        let state = self.state.clone();

        grpc::handle(request, async move |request| remove_tag(state, request).await).await
    }

    async fn upgrade_format(
        &self,
        request: tonic::Request<UpgradeFormatRequest>,
    ) -> Result<tonic::Response<CommandResponse>, tonic::Status> {
        let state = self.state.clone();

        grpc::handle(request, async move |request| upgrade_format(state, request).await).await
    }
}

#[tracing::instrument(skip_all, fields(task_id, builder_id))]
async fn upload(state: Arc<State>, request: tonic::Request<tonic::Streaming<UploadRequest>>) -> Result<(), Error> {
    let token = request
        .extensions()
        .get::<VerifiedToken>()
        .cloned()
        .context(MissingRequestTokenSnafu)?;

    let mut stream = request.into_inner().into_stream();

    // First "chunk" will be the proto encoded UploadTokenRequest
    // so we can validate this token matches & we can see what
    // collectables will be uploaded
    let header_chunk = stream.try_next().await?.context(MissingUploadTokenRequestSnafu)?.chunk;
    // Second chunk is a signature over the first chunk using the declared builders
    // public key signing key. This is used to confirm the upload token granted for this upload
    // is actually used by the builder to make this request.
    let signature_chunk = stream
        .try_next()
        .await?
        .context(MissingUploadSignatureRequestSnafu)?
        .chunk;

    let header = BuildFinished::decode(&*header_chunk).context(DecodeUploadTokenRequestSnafu)?;

    let span = Span::current();
    span.record("task_id", header.task_id);

    let hash = upload::upload_request_hash(&header);

    if token.decoded.payload.jti.is_none_or(|h| h != hash) {
        return Err(Error::InvalidUploadToken);
    }

    // Validate this request came from the authenticated builder
    //
    // This token isn't directly granted to the builder, but is more of a
    // "step-up" token passed to the builder via summit. This is a sanity
    // check that summit or something else isn't using the token and its
    // the intended builder.
    match token.decoded.payload.client {
        service::auth::Client::Service {
            service_id: builder_id,
            service: Service::Avalanche,
            public_key: builder_public_key,
        } => {
            let signature = crypto::signature_from_bytes(&signature_chunk).context(ParseUploadSignatureSnafu)?;

            builder_public_key
                .verify(&header_chunk, &signature)
                .context(UploadSignatureVerificationSnafu)?;

            span.record("builder_id", builder_id);
        }
        _ => return Err(Error::InvalidUploadToken),
    }

    info!(num_packages = header.collectables.len(), "Upload requested");

    let packages = upload::save_packages(&state.service, &header.collectables, stream)
        .await
        .context(SavePackagesSnafu)?;

    if packages.is_empty() {
        warn!("No packages sent");
        return Ok(());
    }

    info!(num_packages = packages.len(), "Upload finished");

    state
        .worker
        .send(worker::Message::PackagesUploaded {
            task_id: header.task_id,
            packages,
        })
        .context(SendWorkerSnafu)?;

    Ok(())
}

#[tracing::instrument(
    skip_all,
    fields(
        channel = %request.get_ref().channel,
        stream = ?request.get_ref().stream(),
        version = %request.get_ref().version,
    )
)]
async fn update_stream(
    state: Arc<State>,
    request: tonic::Request<UpdateStreamRequest>,
) -> Result<CommandResponse, Error> {
    let token = request
        .extensions()
        .get::<VerifiedToken>()
        .cloned()
        .ok_or(Error::MissingRequestToken)?;

    info!(
        client = %token.decoded.payload.client,
        "Update stream request received"
    );

    let body = request.into_inner();

    let (sender, receiver) = oneshot::channel();

    let validate = || -> Result<_, Report> {
        let stream = match body.stream() {
            proto::vessel::Stream::Unknown => bail!("unknown stream"),
            proto::vessel::Stream::Volatile => channel::version::Stream::Volatile,
            proto::vessel::Stream::Unstable => channel::version::Stream::Unstable,
        };
        let version =
            channel::Version::try_from(body.version).map_err(|err| eyre!("failed to parse version: {err}"))?;

        Ok(worker::Message::ChannelCommand {
            channel: body.channel,
            command: channel::Command::UpdateStream { stream, version },
            response: sender,
        })
    };

    match validate() {
        Ok(command) => {
            let _ = state.worker.send(command);
        }
        Err(error) => {
            return Ok(CommandResponse {
                success: false,
                error: Some(format!("{error:#}")),
            });
        }
    }

    match receiver.await? {
        Ok(_) => Ok(CommandResponse {
            success: true,
            error: None,
        }),
        Err(error) => Ok(CommandResponse {
            success: false,
            error: Some(format!("{error:#}")),
        }),
    }
}

#[tracing::instrument(
    skip_all,
    fields(
        channel = %request.get_ref().channel,
        tag = ?request.get_ref().tag,
        history = %request.get_ref().history,
    )
)]
async fn add_tag(state: Arc<State>, request: tonic::Request<AddTagRequest>) -> Result<CommandResponse, Error> {
    let token = request
        .extensions()
        .get::<VerifiedToken>()
        .cloned()
        .ok_or(Error::MissingRequestToken)?;

    info!(
        client = %token.decoded.payload.client,
        "Add tag request received"
    );

    let body = request.into_inner();

    let (sender, receiver) = oneshot::channel();

    let validate = || -> Result<_, Report> {
        let tag = channel::version::Identifier::new(&body.tag)?.into();
        let history = channel::version::Identifier::new(&body.history)?.into();

        Ok(worker::Message::ChannelCommand {
            channel: body.channel,
            command: channel::Command::AddTag { tag, history },
            response: sender,
        })
    };

    match validate() {
        Ok(command) => {
            let _ = state.worker.send(command);
        }
        Err(error) => {
            return Ok(CommandResponse {
                success: false,
                error: Some(format!("{error:#}")),
            });
        }
    }

    match receiver.await? {
        Ok(_) => Ok(CommandResponse {
            success: true,
            error: None,
        }),
        Err(error) => Ok(CommandResponse {
            success: false,
            error: Some(format!("{error:#}")),
        }),
    }
}

#[tracing::instrument(
    skip_all,
    fields(
        channel = %request.get_ref().channel,
        tag = ?request.get_ref().tag,
    )
)]
async fn remove_tag(state: Arc<State>, request: tonic::Request<RemoveTagRequest>) -> Result<CommandResponse, Error> {
    let token = request
        .extensions()
        .get::<VerifiedToken>()
        .cloned()
        .ok_or(Error::MissingRequestToken)?;

    info!(
        client = %token.decoded.payload.client,
        "Remove tag request received"
    );

    let body = request.into_inner();

    let (sender, receiver) = oneshot::channel();

    let validate = || -> Result<_, Report> {
        let tag = channel::version::Identifier::new(&body.tag)?.into();

        Ok(worker::Message::ChannelCommand {
            channel: body.channel,
            command: channel::Command::RemoveTag { tag },
            response: sender,
        })
    };

    match validate() {
        Ok(command) => {
            let _ = state.worker.send(command);
        }
        Err(error) => {
            return Ok(CommandResponse {
                success: false,
                error: Some(format!("{error:#}")),
            });
        }
    }

    match receiver.await? {
        Ok(_) => Ok(CommandResponse {
            success: true,
            error: None,
        }),
        Err(error) => Ok(CommandResponse {
            success: false,
            error: Some(format!("{error:#}")),
        }),
    }
}

#[tracing::instrument(
    skip_all,
    fields(
        channel = %request.get_ref().channel,
        format,
    )
)]
async fn upgrade_format(
    state: Arc<State>,
    request: tonic::Request<UpgradeFormatRequest>,
) -> Result<CommandResponse, Error> {
    let body = request.into_inner();

    let format_upgrade = body.format.and_then(|f| match f.format? {
        upgrade_format::Format::Legacy(request) => Some(FormatUpgrade::Legacy {
            tag_name: request.tag_name,
        }),
    });

    if let Some(format) = format_upgrade.as_ref() {
        Span::current().record("format", format.to_string());
    }

    info!("Upgrade format request received");

    let (sender, receiver) = oneshot::channel();

    let validate = || -> Result<_, Report> {
        let format_upgrade = format_upgrade.ok_or_eyre("missing format")?;

        Ok(worker::Message::ChannelCommand {
            channel: body.channel,
            command: channel::Command::FormatUpgrade(format_upgrade),
            response: sender,
        })
    };

    match validate() {
        Ok(command) => {
            let _ = state.worker.send(command);
        }
        Err(err) => {
            error!(error = format!("{err:#}"), "Upgrade format request validation failed");

            return Ok(CommandResponse {
                success: false,
                error: Some(format!("{err:#}")),
            });
        }
    }

    match receiver.await? {
        Ok(_) => Ok(CommandResponse {
            success: true,
            error: None,
        }),
        Err(err) => {
            error!(error = format!("{err:#}"), "Upgrade format command failed");

            Ok(CommandResponse {
                success: false,
                error: Some(format!("{err:#}")),
            })
        }
    }
}

#[derive(Debug, Snafu)]
enum Error {
    #[snafu(display("Token missing from request"))]
    MissingRequestToken,
    #[snafu(display("Invalid upload token"))]
    InvalidUploadToken,
    #[snafu(display("Failed to send task to worker"))]
    SendWorker {
        source: mpsc::error::SendError<worker::Message>,
    },
    #[snafu(context(false), display("Grpc request error"))]
    GrpcRequest { source: tonic::Status },
    #[snafu(display("Missing upload token body from upload stream"))]
    MissingUploadTokenRequest,
    #[snafu(display("Missing upload signature from upload stream"))]
    MissingUploadSignatureRequest,
    #[snafu(display("Failed to decode upload token body"))]
    DecodeUploadTokenRequest { source: prost::DecodeError },
    #[snafu(transparent)]
    OneshotRecv { source: oneshot::error::RecvError },
    #[snafu(display("Failed to save packages"))]
    SavePackages { source: upload::Error },
    #[snafu(display("Failed to parse upload signature"))]
    ParseUploadSignature { source: crypto::Error },
    #[snafu(display("Upload signature verification failed"))]
    UploadSignatureVerification { source: crypto::Error },
}

impl From<Error> for tonic::Status {
    fn from(error: Error) -> Self {
        match error {
            Error::MissingRequestToken => tonic::Status::unauthenticated(""),
            Error::InvalidUploadToken => tonic::Status::permission_denied(""),
            Error::MissingUploadTokenRequest
            | Error::MissingUploadSignatureRequest
            | Error::DecodeUploadTokenRequest { .. }
            | Error::ParseUploadSignature { .. }
            | Error::UploadSignatureVerification { .. } => tonic::Status::invalid_argument(""),
            Error::SendWorker { .. } | Error::OneshotRecv { .. } => tonic::Status::internal(""),
            Error::GrpcRequest { source } => source,
            Error::SavePackages { source } => match source {
                upload::Error::Sha256Mismatch { .. }
                | upload::Error::InvalidSha256Length { .. }
                | upload::Error::InvalidCollectableKind { .. } => tonic::Status::invalid_argument(""),
                upload::Error::SignUploadToken { .. }
                | upload::Error::ParseBuilderPublicKey { .. }
                | upload::Error::CreateDownloadDir { .. }
                | upload::Error::WriteDownloadFile { .. }
                | upload::Error::UnexpectedEndOfUpload
                | upload::Error::CreateDownloadFile { .. }
                | upload::Error::StreamBytes { .. } => tonic::Status::internal(""),
            },
        }
    }
}
