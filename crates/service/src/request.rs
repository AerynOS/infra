//! Download local or remote files
use std::{io, path::Path};

use futures_util::StreamExt;
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::{fs::File, io::AsyncWriteExt};
use url::Url;

/// Downloads the file at [`Url`] to destination [`Path`] and validates it matches
/// the provided sha256sum
pub async fn download_and_verify(url: Url, dest: impl AsRef<Path>, sha256sum: &str) -> Result<(), Error> {
    let mut stream = moss::request::stream(url).await?;

    let mut file = File::create(dest).await.map_err(Error::CreateFile)?;
    let mut hasher = Sha256::default();

    while let Some(bytes) = stream.next().await {
        let mut bytes = bytes?;

        hasher.update(bytes.as_ref());

        file.write_all_buf(&mut bytes).await.map_err(Error::Write)?;
    }

    file.flush().await.map_err(Error::Write)?;

    let hash = hex::encode(hasher.finalize());

    if hash != sha256sum {
        return Err(Error::Sha256Mismatch {
            expected: sha256sum.to_owned(),
            actual: hash,
        });
    }

    Ok(())
}

/// Request error
#[derive(Debug, Error)]
pub enum Error {
    /// Error fetching remote file
    #[error("fetch")]
    Fetch(#[source] reqwest::Error),
    /// Error reading local file
    #[error("read")]
    Read(#[source] io::Error),
    /// Error writing to file
    #[error("write")]
    Write(#[source] io::Error),
    /// Error creating file
    #[error("create file")]
    CreateFile(#[source] io::Error),
    /// Sha256 mismatch
    #[error("invalid sha256, expected {expected} actual {actual}")]
    Sha256Mismatch {
        /// Expected hash
        expected: String,
        /// Actual hash
        actual: String,
    },
}

impl From<moss::request::Error> for Error {
    fn from(error: moss::request::Error) -> Self {
        match error {
            moss::request::Error::Fetch(e) => Error::Fetch(e),
            moss::request::Error::Read(e) => Error::Read(e),
        }
    }
}
