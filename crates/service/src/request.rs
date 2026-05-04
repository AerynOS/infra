//! Download local or remote files
use std::path::Path;

use thiserror::Error;
use url::Url;

/// Downloads the file at [`Url`] to destination [`Path`] and validates it matches
/// the provided sha256sum
pub async fn download_and_verify(url: Url, dest: impl AsRef<Path>, sha256sum: &str) -> Result<(), Error> {
    let hash = moss::request::download_with_sha256(url, dest.as_ref()).await?;

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
    /// Error downloading remote file
    #[error("download")]
    Download(#[from] moss::request::Error),
    /// Sha256 mismatch
    #[error("invalid sha256, expected {expected} actual {actual}")]
    Sha256Mismatch {
        /// Expected hash
        expected: String,
        /// Actual hash
        actual: String,
    },
}
