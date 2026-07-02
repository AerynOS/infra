use std::sync::Arc;

use async_trait::async_trait;
use futures_util::TryStreamExt;
use prost::Message;
use service::{
    grpc::{
        self,
        proto::{
            summit::builder_stream::BuildFinished,
            vessel::{
                UploadRequest,
                vessel_service_server::{VesselService as GrpcVesselService, VesselServiceServer},
            },
        },
    },
    token::VerifiedToken,
};
use snafu::{OptionExt, ResultExt, Snafu};
use tokio::sync::mpsc;
use tracing::{Span, info, warn};

use crate::{upload, worker};

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
}

#[tracing::instrument(skip_all, fields(task_id))]
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

    let header = BuildFinished::decode(&*header_chunk).context(DecodeUploadTokenRequestSnafu)?;

    let span = Span::current();
    span.record("task_id", header.task_id);

    let hash = upload::upload_request_hash(&header);

    if token.decoded.payload.jti.is_none_or(|h| h != hash) {
        return Err(Error::InvalidUploadToken);
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
    #[snafu(display("Failed to decode upload token body"))]
    DecodeUploadTokenRequest { source: prost::DecodeError },
    #[snafu(display("Failed to save packages"))]
    SavePackages { source: upload::Error },
}

impl From<Error> for tonic::Status {
    fn from(error: Error) -> Self {
        match error {
            Error::MissingRequestToken => tonic::Status::unauthenticated(""),
            Error::InvalidUploadToken => tonic::Status::permission_denied(""),
            Error::MissingUploadTokenRequest | Error::DecodeUploadTokenRequest { .. } => {
                tonic::Status::invalid_argument("")
            }
            Error::SendWorker { .. } => tonic::Status::internal(""),
            Error::GrpcRequest { source } => source,
            Error::SavePackages { source } => match source {
                upload::Error::Sha256Mismatch { .. }
                | upload::Error::InvalidSha256Length { .. }
                | upload::Error::InvalidCollectableKind { .. } => tonic::Status::invalid_argument(""),
                upload::Error::SignUploadToken { .. }
                | upload::Error::CreateDownloadDir { .. }
                | upload::Error::WriteDownloadFile { .. }
                | upload::Error::UnexpectedEndOfUpload
                | upload::Error::CreateDownloadFile { .. }
                | upload::Error::StreamBytes { .. } => tonic::Status::internal(""),
            },
        }
    }
}
