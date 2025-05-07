use std::path::Path;

use color_eyre::eyre::{Context, OptionExt, Result};
use futures_util::{TryStreamExt, stream::select};
use http::Uri;
use itertools::Itertools;
use service::{
    State, error, git,
    grpc::{
        collectable::{self, Collectable},
        remote::Remote,
        summit::BuilderBuild,
    },
};
use sha2::{Digest, Sha256};
use tokio::{
    fs,
    io::BufReader,
    process::{ChildStderr, ChildStdout},
};
use tokio_util::io::ReaderStream;
use tracing::{error, info};

use crate::stream;

#[tracing::instrument(
    skip_all,
    fields(
        task_id = request.task_id,
    )
)]
pub async fn build(request: BuilderBuild, state: State, stream: stream::Handle) {
    info!("Starting build");

    let task_id = request.task_id;

    match run(request, &state, stream.clone()).await {
        Ok(collectables) => {
            info!("Build succeeded");

            stream.build_succeeded(task_id, collectables).await;
        }
        Err(e) => {
            let error = error::chain(e.as_ref() as &dyn std::error::Error);
            error!(%error, "Build failed");

            stream.build_failed(task_id).await;
        }
    }
}

async fn run(request: BuilderBuild, state: &State, stream: stream::Handle) -> Result<Vec<Collectable>> {
    let uri = request.uri.parse::<Uri>().context("invalid upstream URI")?;

    let mirror_dir = state.cache_dir.join(
        uri.path()
            .strip_prefix("/")
            .ok_or_eyre("path should always have leading slash")?,
    );

    if let Some(parent) = mirror_dir.parent() {
        ensure_dir_exists(parent).await.context("create mirror parent dir")?;
    }

    let work_dir = state.state_dir.join("work");
    recreate_dir(&work_dir).await.context("recreate work dir")?;

    let worktree_dir = work_dir.join("source");
    ensure_dir_exists(&worktree_dir).await.context("create worktree dir")?;

    let asset_dir = state.root.join("assets").join(request.task_id.to_string());
    recreate_dir(&asset_dir).await.context("recreate asset dir")?;

    if mirror_dir.exists() {
        info!(%uri, "Updating mirror of recipe repo");

        git::remote_update(&mirror_dir).await?;
    } else {
        info!(%uri, "Creating mirror of recipe repo");

        git::mirror(&uri, &mirror_dir).await?;
    }

    info!(commit_ref = request.commit_ref, "Checking out commit ref to worktree");
    git::checkout_worktree(&mirror_dir, &worktree_dir, &request.commit_ref)
        .await
        .context("checkout commit as worktree")?;

    create_boulder_config(&work_dir, &request.remotes)
        .await
        .context("create boulder config")?;

    build_recipe(
        request.task_id,
        &work_dir,
        &asset_dir,
        &worktree_dir,
        &request.relative_path,
        stream,
    )
    .await
    .context("build recipe")?;

    let collectables = scan_collectables(&asset_dir).await.context("scan collectables")?;

    info!("Removing worktree");
    git::remove_worktree(&mirror_dir, &worktree_dir)
        .await
        .context("remove worktree")?;

    Ok(collectables)
}

async fn ensure_dir_exists(path: &Path) -> Result<()> {
    Ok(fs::create_dir_all(path).await?)
}

async fn recreate_dir(path: &Path) -> Result<()> {
    if path.exists() {
        fs::remove_dir_all(path).await?;
    }

    Ok(fs::create_dir_all(path).await?)
}

async fn create_boulder_config(work_dir: &Path, remotes: &[Remote]) -> Result<()> {
    info!("Creating boulder config");

    let remotes = remotes
        .iter()
        .map(|remote| {
            format!(
                "
        {}:
            uri: \"{}\"
            description: \"Remotely configured repository\"
            priority: {}
                ",
                remote.name, remote.index_uri, remote.priority,
            )
        })
        .join("\n");

    let config = format!(
        "
avalanche:
    repositories:
{remotes}
        "
    );

    let config_dir = work_dir.join("etc/boulder/profile.d");
    ensure_dir_exists(&config_dir)
        .await
        .context("create boulder config dir")?;

    fs::write(config_dir.join("avalanche.yaml"), config)
        .await
        .context("write boulder config")?;

    Ok(())
}

async fn build_recipe(
    task_id: u64,
    work_dir: &Path,
    asset_dir: &Path,
    worktree_dir: &Path,
    relative_path: &str,
    stream: stream::Handle,
) -> Result<()> {
    info!("Building recipe");

    let write_logs = |stdout: ChildStdout, stderr: ChildStderr| async move {
        let mut stdout = ReaderStream::new(BufReader::new(stdout));
        let mut stderr = ReaderStream::new(BufReader::new(stderr));

        stream.build_started(task_id).await;

        while let Ok(Some(bytes)) = select(&mut stdout, &mut stderr).try_next().await {
            stream.build_log(bytes.into()).await;
        }
    };

    // By running with nice like this, we ensure that avalanche can run on
    // systems that also host other services (or are workstations).
    // "The range of the nice value is +19 (low priority) to -20 (high
    //  priority).  Attempts to set a nice value outside the range are
    //  clamped to the range." (from `man 2 nice`)
    service::process::piped(
        "nice",
        |process| {
            process
                .args(["-n19", "boulder", "build", "-p", "avalanche", "--update", "-o"])
                .arg(asset_dir)
                .arg("--config-dir")
                .arg(work_dir.join("etc/boulder"))
                .arg("--")
                .arg(relative_path)
                .current_dir(worktree_dir)
        },
        write_logs,
    )
    .await?;

    Ok(())
}

async fn scan_collectables(asset_dir: &Path) -> Result<Vec<Collectable>> {
    let mut collectables = vec![];

    let mut contents = fs::read_dir(asset_dir).await.context("read asset dir")?;

    while let Some(entry) = contents.next_entry().await.context("get next assets dir entry")? {
        let path = entry.path();

        let Some(file_name) = path.file_name().and_then(|name| name.to_str()).map(ToOwned::to_owned) else {
            continue;
        };

        let mut kind = collectable::Kind::Unknown;

        if file_name.ends_with(".bin") {
            kind = collectable::Kind::BinaryManifest;
        } else if file_name.ends_with(".jsonc") {
            kind = collectable::Kind::JsonManifest;
        } else if file_name.ends_with(".stone") {
            kind = collectable::Kind::Package;
        }

        let (size, sha256sum) = tokio::task::spawn_blocking(move || stat(&path))
            .await
            .context("spawn blocking")?
            .context("stat file")?;

        collectables.push(Collectable {
            kind: kind as i32,
            name: file_name.to_owned(),
            size,
            sha256sum,
        })
    }

    Ok(collectables)
}

fn stat(file: &Path) -> Result<(u64, String)> {
    use std::fs::File;
    use std::io;

    let file = File::open(file).context("open file")?;
    let mut hasher = Sha256::default();

    let size = io::copy(&mut &file, &mut hasher)?;

    Ok((size, hex::encode(hasher.finalize())))
}
