use std::{
    io,
    path::{Path, PathBuf},
    sync::Arc,
};

use async_trait::async_trait;
use futures_util::TryStreamExt;
use prost::Message;
use service::{
    Database, Endpoint, Token,
    auth::Permission,
    database, endpoint,
    grpc::{
        self, collectable,
        vessel::{
            UploadRequest, UploadTokenRequest, UploadTokenResponse,
            vessel_service_server::{VesselService, VesselServiceServer},
        },
    },
    token::{self, VerifiedToken},
};
use sha2::{Digest, Sha256};
use snafu::{OptionExt, ResultExt, Snafu, ensure};
use sqlx::types::chrono::Utc;
use tokio::{
    fs::{self, File},
    io::AsyncWriteExt,
    sync::mpsc,
};
use tracing::{Span, debug, info, warn};

use crate::worker;

pub type Server = VesselServiceServer<Service>;

pub fn service(db: Database, state: service::State, worker: worker::Sender) -> Server {
    Server::new(Service {
        state: Arc::new(State {
            db,
            service: state,
            worker,
        }),
    })
}

#[derive(Clone)]
pub struct Service {
    state: Arc<State>,
}

#[derive(Clone)]
struct State {
    db: Database,
    service: service::State,
    worker: worker::Sender,
}

#[async_trait]
impl VesselService for Service {
    async fn upload_token(
        &self,
        request: tonic::Request<UploadTokenRequest>,
    ) -> Result<tonic::Response<UploadTokenResponse>, tonic::Status> {
        let state = self.state.clone();

        grpc::handle(request, async move |request| upload_token(state, request).await).await
    }

    async fn upload(
        &self,
        request: tonic::Request<tonic::Streaming<UploadRequest>>,
    ) -> std::result::Result<tonic::Response<()>, tonic::Status> {
        let state = self.state.clone();

        grpc::handle(request, async move |request| upload(state, request).await).await
    }
}

#[tracing::instrument(
    skip_all,
    fields(
        task_id = %request.get_ref().task_id,
    )
)]
async fn upload_token(
    state: Arc<State>,
    request: tonic::Request<UploadTokenRequest>,
) -> Result<UploadTokenResponse, Error> {
    let token = request
        .extensions()
        .get::<VerifiedToken>()
        .cloned()
        .context(MissingRequestTokenSnafu)?;

    let hash = grpc::upload_request_hash(&request.into_inner());

    let now = Utc::now();

    // Create a single use upload token for vessel
    // derived from summits token
    let upload_token = Token::new(token::Payload {
        iat: now.timestamp(),
        exp: (now + chrono::Duration::minutes(5)).timestamp(),
        jti: Some(hash),
        permissions: [Permission::UploadPackage].into_iter().collect(),
        ..token.decoded.payload
    })
    .sign(&state.service.key_pair)
    .context(SignUploadTokenSnafu)?;

    Ok(UploadTokenResponse { token: upload_token })
}

#[tracing::instrument(skip_all, fields(task_id))]
async fn upload(state: Arc<State>, request: tonic::Request<tonic::Streaming<UploadRequest>>) -> Result<(), Error> {
    let token = request
        .extensions()
        .get::<VerifiedToken>()
        .cloned()
        .context(MissingRequestTokenSnafu)?;

    let endpoint_id = token
        .decoded
        .payload
        .sub
        .parse::<endpoint::Id>()
        .context(InvalidEndpointSnafu)?;
    let endpoint = Endpoint::get(state.db.acquire().await.context(DatabaseSnafu)?.as_mut(), endpoint_id)
        .await
        .context(LoadEndpointSnafu)?;

    let mut stream = request.into_inner().into_stream();

    // First "chunk" will be the proto encoded UploadTokenRequest
    // so we can validate this token matches & we can see what
    // collectables will be uploaded
    let chunk = stream.try_next().await?.context(MissingUploadTokenRequestSnafu)?;

    let body = UploadTokenRequest::decode(&*chunk.chunk).context(DecodeUploadTokenRequestSnafu)?;

    let span = Span::current();
    span.record("task_id", body.task_id);

    let hash = grpc::upload_request_hash(&body);

    if token.decoded.payload.jti.is_none_or(|h| h != hash) {
        return Err(Error::InvalidUploadToken);
    }

    info!(num_packages = body.collectables.len(), "Upload requested");

    let mut bytes = vec![];
    let mut packages = vec![];

    // Save each package
    for collectable in body.collectables {
        ensure!(
            collectable.kind() == collectable::Kind::Package,
            InvalidCollectableKindSnafu {
                kind: collectable.kind()
            }
        );

        let path = download_path(&state.service.state_dir, &collectable.sha256sum).await?;
        let mut file = File::create(&path).await.context(CreateDownloadFileSnafu)?;
        let mut sha2 = Sha256::default();

        let mut remaining = collectable.size as usize;

        while remaining > 0 {
            let rest = bytes.split_off(bytes.len().min(remaining));

            remaining -= bytes.len();

            sha2.update(&bytes);
            file.write_all(&bytes).await.context(WriteDownloadFileSnafu)?;

            bytes = rest;

            if let Some(req) = stream.try_next().await? {
                bytes.extend(req.chunk);
            } else {
                ensure!(remaining == 0, UnexpectedEndOfUploadSnafu);
            }
        }

        file.flush().await.context(WriteDownloadFileSnafu)?;

        let hash = hex::encode(sha2.finalize());

        ensure!(
            hash == collectable.sha256sum,
            Sha256MismatchSnafu {
                expected: collectable.sha256sum,
                actual: hash
            }
        );

        debug!(name = collectable.name, "Package saved");

        packages.push(worker::Package {
            name: collectable.name,
            path,
            sha256sum: hash,
        });
    }

    if packages.is_empty() {
        warn!("No packages sent");
        return Ok(());
    }

    info!(num_packages = packages.len(), "Upload finished");

    state
        .worker
        .send(worker::Message::PackagesUploaded {
            task_id: body.task_id,
            endpoint,
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
    #[snafu(display("Invalid endpoint"))]
    InvalidEndpoint { source: uuid::Error },
    #[snafu(display("Failed to load endpoint"))]
    LoadEndpoint { source: database::Error },
    #[snafu(display("Failed to send task to worker"))]
    SendWorker {
        source: mpsc::error::SendError<worker::Message>,
    },
    #[snafu(display("Database error"))]
    Database { source: database::Error },
    #[snafu(display("Failed to sign upload token"))]
    SignUploadToken { source: token::Error },
    #[snafu(display("Cannot import collectable type {kind:?}"))]
    InvalidCollectableKind { kind: collectable::Kind },
    #[snafu(context(false), display("Grpc request error"))]
    GrpcRequest { source: tonic::Status },
    #[snafu(display("Invalid sha256 length of collectable: {size}"))]
    InvalidSha256Length { size: usize },
    #[snafu(display("Failed to create download directory"))]
    CreateDownloadDir { source: io::Error },
    #[snafu(display("Missing upload token body from upload stream"))]
    MissingUploadTokenRequest,
    #[snafu(display("Failed to decode upload token body"))]
    DecodeUploadTokenRequest { source: prost::DecodeError },
    #[snafu(display("Failed to create download file"))]
    CreateDownloadFile { source: io::Error },
    #[snafu(display("Failed writing to download file"))]
    WriteDownloadFile { source: io::Error },
    #[snafu(display("Unexpected end of stream while downloading packages"))]
    UnexpectedEndOfUpload,
    #[snafu(display("Sha256 mismatch of uploaded package, expected {expected} got {actual}"))]
    Sha256Mismatch { expected: String, actual: String },
}

impl From<Error> for tonic::Status {
    fn from(error: Error) -> Self {
        match error {
            Error::MissingRequestToken => tonic::Status::unauthenticated(""),
            Error::InvalidUploadToken => tonic::Status::permission_denied(""),
            Error::InvalidEndpoint { .. }
            | Error::InvalidCollectableKind { .. }
            | Error::InvalidSha256Length { .. }
            | Error::MissingUploadTokenRequest
            | Error::DecodeUploadTokenRequest { .. }
            | Error::UnexpectedEndOfUpload
            | Error::Sha256Mismatch { .. } => tonic::Status::invalid_argument(""),
            Error::LoadEndpoint { .. }
            | Error::SendWorker { .. }
            | Error::Database { .. }
            | Error::SignUploadToken { .. }
            | Error::CreateDownloadDir { .. }
            | Error::CreateDownloadFile { .. }
            | Error::WriteDownloadFile { .. } => tonic::Status::internal(""),
            Error::GrpcRequest { source } => source,
        }
    }
}

async fn download_path(state_dir: &Path, hash: &str) -> Result<PathBuf, Error> {
    ensure!(hash.len() >= 5, InvalidSha256LengthSnafu { size: hash.len() });

    let dir = state_dir.join("staging").join(&hash[..5]).join(&hash[hash.len() - 5..]);

    if !fs::try_exists(&dir).await.unwrap_or_default() {
        fs::create_dir_all(&dir).await.context(CreateDownloadDirSnafu)?;
    }

    Ok(dir.join(hash))
}
