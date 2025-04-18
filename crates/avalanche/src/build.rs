use std::path::Path;

use color_eyre::eyre::{self, Context, OptionExt, Report, Result};
use http::Uri;
use itertools::Itertools;
use service::{
    Endpoint, State,
    client::{AuthClient, EndpointAuth, SummitServiceClient},
    error, git,
    grpc::{
        avalanche::BuildRequest,
        collectable::{self, Collectable},
        remote::Remote,
        summit,
    },
};
use sha2::{Digest, Sha256};
use tokio::fs::{self, File};
use tracing::{error, info};

use crate::Config;

#[tracing::instrument(
    skip_all,
    fields(
        task_id = request.task_id,
        endpoint = %endpoint.id,
    )
)]
pub async fn build(request: BuildRequest, endpoint: Endpoint, state: State, config: Config) {
    info!("Starting build");

    let run = async || {
        let mut client = SummitServiceClient::connect_with_auth(
            endpoint.host_address.clone(),
            EndpointAuth::new(&endpoint, state.service_db.clone(), state.key_pair.clone()),
        )
        .await?;

        let task_id = request.task_id;

        match run(request, endpoint, state, config).await {
            Ok((None, collectables)) => {
                info!("Build succeeded");

                client
                    .build_succeeded(summit::BuildRequest { task_id, collectables })
                    .await?;
            }
            Ok((Some(e), collectables)) => {
                let error = error::chain(e.as_ref() as &dyn std::error::Error);
                error!(%error, "Build failed");

                client
                    .build_failed(summit::BuildRequest { task_id, collectables })
                    .await?;
            }
            Err(e) => {
                let error = error::chain(e.as_ref() as &dyn std::error::Error);
                error!(%error, "Build failed");

                client
                    .build_failed(summit::BuildRequest {
                        task_id,
                        collectables: vec![],
                    })
                    .await?;
            }
        }

        Result::<_, eyre::Report>::Ok(())
    };

    if let Err(e) = run().await {
        let error = error::chain(&*e);
        error!(%error, "Failed to send build status response");
    }
}

async fn run(
    request: BuildRequest,
    _endpoint: Endpoint,
    state: State,
    config: Config,
) -> Result<(Option<Report>, Vec<Collectable>)> {
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

    let log_file = asset_dir.join("build.log");

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

    let error = build_recipe(&work_dir, &asset_dir, &worktree_dir, &request.relative_path, &log_file)
        .await
        .err();

    tokio::task::spawn_blocking(move || compress_file(&log_file))
        .await
        .context("spawn blocking")?
        .context("compress log file")?;

    let collectables = scan_collectables(
        request.task_id,
        config.http_address.as_ref().ok_or_eyre("missing configured http uri")?,
        &asset_dir,
    )
    .await
    .context("scan collectables")?;

    info!("Removing worktree");
    git::remove_worktree(&mirror_dir, &worktree_dir)
        .await
        .context("remove worktree")?;

    Ok((error, collectables))
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
    work_dir: &Path,
    asset_dir: &Path,
    worktree_dir: &Path,
    relative_path: &str,
    log_path: &Path,
) -> Result<()> {
    let log_file = File::create(log_path)
        .await
        .context("create log file")?
        .into_std()
        .await;

    info!("Building recipe");

    let stdout = log_file.try_clone()?;
    let stderr = log_file;

    service::process::execute("sudo", |process| {
        process
            .args(["nice", "-n20", "boulder", "build", "-p", "avalanche", "--update", "-o"])
            .arg(asset_dir)
            .arg("--config-dir")
            .arg(work_dir.join("etc/boulder"))
            .arg("--")
            .arg(relative_path)
            .current_dir(worktree_dir)
            .stdout(stdout)
            .stderr(stderr)
    })
    .await?;

    Ok(())
}

fn compress_file(file: &Path) -> Result<()> {
    use flate2::write::GzEncoder;
    use std::fs::{self, File};
    use std::io::{self, Write};

    let mut plain_file = File::open(file).context("open plain file")?;
    let mut gz_file = File::create(format!("{}.gz", file.display())).context("create compressed file")?;

    let mut encoder = GzEncoder::new(&mut gz_file, flate2::Compression::new(9));

    io::copy(&mut plain_file, &mut encoder)?;

    encoder.finish()?;
    gz_file.flush()?;

    fs::remove_file(file).context("remove plain file")?;

    Ok(())
}

async fn scan_collectables(build_id: u64, host_address: &Uri, asset_dir: &Path) -> Result<Vec<Collectable>> {
    let mut collectables = vec![];

    let mut contents = fs::read_dir(asset_dir).await.context("read asset dir")?;

    while let Some(entry) = contents.next_entry().await.context("get next assets dir entry")? {
        let path = entry.path();

        let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };

        let mut kind = collectable::Kind::Unknown;

        if file_name.ends_with(".bin") {
            kind = collectable::Kind::BinaryManifest;
        } else if file_name.ends_with(".jsonc") {
            kind = collectable::Kind::JsonManifest;
        } else if file_name.ends_with(".log.gz") {
            kind = collectable::Kind::Log;
        } else if file_name.ends_with(".stone") {
            kind = collectable::Kind::Package;
        }

        let uri = format!("{host_address}assets/{build_id}/{file_name}")
            .parse()
            .context("invalid asset URI")?;

        let sha256sum = tokio::task::spawn_blocking(move || compute_sha256(&path))
            .await
            .context("spawn blocking")?
            .context("compute asset sha256")?;

        collectables.push(Collectable {
            kind: kind as i32,
            uri,
            sha256sum,
        })
    }

    Ok(collectables)
}

fn compute_sha256(file: &Path) -> Result<String> {
    use std::fs::File;
    use std::io;

    let file = File::open(file).context("open file")?;
    let mut hasher = Sha256::default();

    io::copy(&mut &file, &mut hasher)?;

    Ok(hex::encode(hasher.finalize()))
}
