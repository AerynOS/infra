use std::ffi::OsStr;
use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};

use color_eyre::eyre::{Context, OptionExt, Result};
use natural_sort_rs::NaturalSortable;
use rayon::iter::{ParallelBridge, ParallelIterator};
use sha2::{Digest, Sha256};

#[derive(Debug)]
pub struct Package {
    pub name: String,
    pub path: PathBuf,
    pub sha256sum: String,
}

/// Enumerates all stone packages under `dir`
pub async fn async_enumerate(dir: &Path) -> Result<Vec<Package>> {
    let dir = dir.to_owned();

    tokio::task::spawn_blocking(move || enumerate(&dir))
        .await
        .context("spawn blocking")?
}

/// Enumerates all stone packages under `dir`
pub fn enumerate(dir: &Path) -> Result<Vec<Package>> {
    fn recurse(dir: &Path) -> Result<Vec<Package>> {
        let contents = fs::read_dir(dir).context("read directory")?;

        // Enumerate in parallel to saturate hashing on all worker threads
        contents
            .par_bridge()
            .try_fold(Vec::new, |mut acc, entry| {
                let entry = entry.context("read directory entry")?;
                let path = entry.path();
                let meta = entry.metadata().context("read directory entry metadata")?;

                if meta.is_file() && path.extension() == Some(OsStr::new("stone")) {
                    let name = path
                        .file_name()
                        .and_then(|n| n.to_str())
                        .ok_or_eyre("missing file name")?
                        .to_owned();

                    let mut hasher = Sha256::default();

                    io::copy(&mut File::open(&path).context("open file")?, &mut hasher).context("hash file")?;

                    let sha256sum = hex::encode(hasher.finalize());

                    acc.push(Package { name, path, sha256sum });
                } else if meta.is_dir() {
                    acc.extend(enumerate(&path)?);
                }

                Ok(acc)
            })
            .try_reduce(Vec::new, |mut acc, items| {
                acc.extend(items);
                Ok(acc)
            })
    }

    let mut packages = recurse(dir)?;

    // this is where we human sort the packages in ascending order to enable a directory to have
    // multiple stones with the same recipe origin but different versions, source-releases
    // and build-releases.
    // The to_string_lossy() call is necessary because Rust cannot guarantee that the OS formats
    // PathBuf filenames in valid utf-8.
    packages.sort_by(|a, b| (a.path.to_string_lossy()).natural_cmp(&b.path.to_string_lossy()));

    Ok(packages)
}
