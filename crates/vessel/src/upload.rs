use std::{
    io,
    path::{Path, PathBuf},
};

use chrono::Utc;
use futures_util::{Stream, TryStreamExt};
use service::{
    Service, Token, auth, crypto,
    grpc::proto::{
        common::{Collectable, collectable},
        summit::{builder_stream::BuildFinished, repository_manager_stream::RequestUploadToken},
        vessel::UploadRequest,
    },
    token,
};
use sha2::{Digest, Sha256};
use snafu::{ResultExt as _, Snafu, ensure};
use tokio::{
    fs::{self, File},
    io::AsyncWriteExt,
};
use tracing::debug;

use crate::{Package, State};

/// Create a short lived upload token scoped to the requested upload
pub fn upload_token(state: &State, request: &RequestUploadToken) -> Result<String, Error> {
    let now = Utc::now();

    // Hash of the request allows us to validate the token
    // against the incoming request to ensure they match
    let hash = upload_request_hash(&BuildFinished {
        task_id: request.task_id,
        collectables: request.collectables.clone(),
    });

    Token::new(token::Payload {
        iat: now.timestamp(),
        // Short expiration since this is intended to be a single use token
        exp: (now + chrono::Duration::minutes(5)).timestamp(),
        jti: Some(hash),
        permissions: [auth::Permission::UploadPackage].into_iter().collect(),
        iss: Service::Vessel.name().to_owned(),
        client: auth::Client::Service {
            service_id: request.builder_id.clone(),
            service: Service::Avalanche,
            public_key: request
                .builder_public_key
                .clone()
                .try_into()
                .context(ParseBuilderPublicKeySnafu)?,
        },
        purpose: token::Purpose::Authentication,
    })
    .sign(&state.service.key_pair)
    .context(SignUploadTokenSnafu)
}

/// Save the defined collectables from the incoming stream of byte chunks
/// to the filesystem
pub async fn save_packages(
    state: &service::State,
    collectables: &[Collectable],
    mut stream: impl Stream<Item = Result<UploadRequest, tonic::Status>> + Unpin,
) -> Result<Vec<Package>, Error> {
    let mut bytes = vec![];
    let mut packages = vec![];

    // Save each package
    for collectable in collectables {
        ensure!(
            collectable.kind() == collectable::Kind::Package,
            InvalidCollectableKindSnafu {
                kind: collectable.kind()
            }
        );

        let path = download_path(&state.state_dir, &collectable.sha256sum).await?;
        let mut file = File::create(&path).await.context(CreateDownloadFileSnafu)?;
        let mut sha2 = Sha256::default();

        let mut remaining = collectable.size as usize;

        while remaining > 0 {
            let rest = bytes.split_off(bytes.len().min(remaining));

            remaining -= bytes.len();

            sha2.update(&bytes);
            file.write_all(&bytes).await.context(WriteDownloadFileSnafu)?;

            bytes = rest;

            if let Some(req) = stream.try_next().await.context(StreamBytesSnafu)? {
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
                expected: collectable.sha256sum.clone(),
                actual: hash
            }
        );

        debug!(name = collectable.name, "Package saved");

        packages.push(Package {
            name: collectable.name.clone(),
            relative_path: path
                .strip_prefix(&state.state_dir)
                .expect("lives under state dir")
                .to_owned(),
            path,
            sha256sum: hash,
        });
    }

    Ok(packages)
}

pub fn upload_request_hash(request: &BuildFinished) -> String {
    use sha2::{Digest, Sha256};

    let mut sha2 = Sha256::default();
    sha2.update(request.task_id.to_be_bytes());

    for collectable in &request.collectables {
        sha2.update(collectable.kind.to_be_bytes());
        sha2.update(collectable.sha256sum.as_bytes());
        sha2.update(collectable.size.to_be_bytes());
    }

    hex::encode(sha2.finalize())
}

async fn download_path(state_dir: &Path, hash: &str) -> Result<PathBuf, Error> {
    ensure!(hash.len() >= 5, InvalidSha256LengthSnafu { size: hash.len() });

    let dir = state_dir.join("staging").join(&hash[..5]).join(&hash[hash.len() - 5..]);

    if !fs::try_exists(&dir).await.unwrap_or_default() {
        fs::create_dir_all(&dir).await.context(CreateDownloadDirSnafu)?;
    }

    Ok(dir.join(hash))
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to sign upload token"))]
    SignUploadToken { source: token::Error },
    #[snafu(display("Failed to parse builder public key"))]
    ParseBuilderPublicKey { source: crypto::Error },
    #[snafu(display("Cannot import collectable type {kind:?}"))]
    InvalidCollectableKind { kind: collectable::Kind },
    #[snafu(display("Failed to create download directory"))]
    CreateDownloadDir { source: io::Error },
    #[snafu(display("Failed writing to download file"))]
    WriteDownloadFile { source: io::Error },
    #[snafu(display("Unexpected end of stream while downloading packages"))]
    UnexpectedEndOfUpload,
    #[snafu(display("Sha256 mismatch of uploaded package, expected {expected} got {actual}"))]
    Sha256Mismatch { expected: String, actual: String },
    #[snafu(display("Invalid sha256 length of collectable: {size}"))]
    InvalidSha256Length { size: usize },
    #[snafu(display("Failed to create download file"))]
    CreateDownloadFile { source: io::Error },
    #[snafu(display("Failed to read stream bytes"))]
    StreamBytes { source: tonic::Status },
}
