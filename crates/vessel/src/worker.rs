use std::{
    convert::Infallible,
    ffi::OsStr,
    future::Future,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
};

use color_eyre::eyre::{self, Context, OptionExt, Result, eyre};
use moss::db::meta;
use natural_sort_rs::NaturalSortable;
use service::{
    Endpoint,
    client::{AuthClient, EndpointAuth, SummitServiceClient},
    crypto::KeyPair,
    database,
    grpc::summit::ImportRequest,
};
use sha2::{Digest, Sha256};
use tokio::{sync::mpsc, time::Instant};
use tracing::{Instrument, debug, error, info, info_span, warn};

use crate::collection;

pub type Sender = mpsc::UnboundedSender<Message>;

#[derive(Debug, strum::Display)]
#[strum(serialize_all = "kebab-case")]
pub enum Message {
    PackagesUploaded {
        task_id: u64,
        endpoint: Endpoint,
        packages: Vec<Package>,
    },
}

#[derive(Debug)]
pub struct Package {
    pub name: String,
    pub path: PathBuf,
    pub sha256sum: String,
}

pub async fn run(state: State) -> Result<(Sender, impl Future<Output = Result<(), Infallible>> + use<>)> {
    let (sender, mut receiver) = mpsc::unbounded_channel::<Message>();

    let task = async move {
        while let Some(message) = receiver.recv().await {
            let kind = message.to_string();

            if let Err(e) = handle_message(&state, message).await {
                let error = service::error::chain(e.as_ref() as &dyn std::error::Error);
                error!(message = kind, %error, "Error handling message");
            }
        }

        info!("Worker exiting");

        Ok(())
    };

    Ok((sender, task))
}

#[derive(Debug, Clone)]
pub struct State {
    state_dir: PathBuf,
    service_db: service::Database,
    meta_db: meta::Database,
    key_pair: KeyPair,
}

impl State {
    pub async fn new(service_state: &service::State) -> Result<Self> {
        let meta_db = meta::Database::new(service_state.db_dir.join("meta").to_string_lossy().as_ref())
            .context("failed to open meta database")?;

        Ok(Self {
            state_dir: service_state.state_dir.clone(),
            service_db: service_state.service_db.clone(),
            meta_db,
            key_pair: service_state.key_pair.clone(),
        })
    }
}

async fn handle_message(state: &State, message: Message) -> Result<()> {
    match message {
        Message::PackagesUploaded {
            task_id,
            endpoint,
            packages,
        } => {
            let span = info_span!(
                "import_packages",
                task_id,
                endpoint = %endpoint.id,
                num_packages = packages.len(),
            );

            async move {
                let mut client = SummitServiceClient::connect_with_auth(
                    endpoint.host_address.clone(),
                    EndpointAuth::new(&endpoint, state.service_db.clone(), state.key_pair.clone()),
                )
                .await
                .context("connect summit client")?;

                match import_packages(state, packages, true).await {
                    Ok(()) => {
                        info!("All packages imported");

                        client
                            .import_succeeded(ImportRequest { task_id })
                            .await
                            .context("send import succeeded request")?;
                    }
                    Err(e) => {
                        let error = service::error::chain(e.as_ref() as &dyn std::error::Error);
                        error!(%error, "Failed to import packages");

                        client
                            .import_failed(ImportRequest { task_id })
                            .await
                            .context("send import failed request")?;
                    }
                }

                Ok(())
            }
            .instrument(span)
            .await
        }
    }
}

#[tracing::instrument(skip_all, fields(?directory))]
pub async fn import_directory(state: &State, directory: PathBuf) -> Result<()> {
    info!("Import started");

    let stones = tokio::task::spawn_blocking(move || enumerate_stones(&directory))
        .await
        .context("spawn blocking")?
        .context("enumerate stones")?;

    let num_stones = stones.len();

    if num_stones > 0 {
        import_packages(state, stones, false).await.context("import packages")?;

        info!(num_stones, "All stones imported");
    } else {
        info!("No stones to import");
    }

    Ok(())
}

async fn import_packages(state: &State, packages: Vec<Package>, destructive_move: bool) -> Result<()> {
    // Stone is read in blocking manner
    let tx = tokio::task::spawn_blocking({
        let span = tracing::Span::current();
        let state = state.clone();

        // Rollback any collection DB inserts if we encounter any failures
        let mut tx = state.service_db.begin().await.context("start db tx")?;

        move || {
            span.in_scope(|| {
                for package in packages {
                    import_package(&state, &mut tx, &package, destructive_move)?;
                }

                Result::<_, eyre::Report>::Ok(tx)
            })
        }
    })
    .await
    .context("spawn blocking")?
    .context("import package")?;

    // No failures, commit it all to collection DB
    tx.commit().await.context("commit collection db tx")?;

    reindex(state).await.context("reindex")?;

    prune_orphaned_packages(state)
        .await
        .context("prune orphaned packages")?;

    Ok(())
}

fn import_package(
    state: &State,
    tx: &mut database::Transaction,
    package: &Package,
    destructive_move: bool,
) -> Result<()> {
    use std::fs::{self, File};

    debug!("Attempting to open {:?}", &package.path);

    let mut file = File::open(&package.path).context("open staged stone")?;
    let file_size = file.metadata().context("read file metadata")?.size();

    let mut reader = stone::read(&mut file).context("create stone reader")?;

    let stone::Header::V1(header) = reader.header;

    if !matches!(header.file_type, stone::header::v1::FileType::Binary) {
        return Err(eyre!("{:?}: Invalid archive, expected binary stone", &file));
    }

    let payloads = reader
        .payloads()
        .context("get stone payload reader")?
        .collect::<Result<Vec<_>, _>>()
        .context("read stone payloads")?;

    let meta_payload = payloads
        .iter()
        .find_map(stone::read::PayloadKind::meta)
        .ok_or(eyre!("{:?}: Invalid archive, missing meta payload", &file))?;

    let mut meta = moss::package::Meta::from_stone_payload(&meta_payload.body)
        .context("convert meta payload into moss package metadata")?;

    let name = meta.name.clone();
    let source_id = meta.source_id.clone();

    meta.hash = Some(package.sha256sum.clone());
    meta.download_size = Some(file_size);

    let id = moss::package::Id::from(package.sha256sum.clone());

    let pool_dir = relative_pool_dir(&source_id)?;
    let file_name = &package.name;
    let target_path = pool_dir.join(file_name);
    let full_path = state.state_dir.join("public").join(&target_path);

    meta.uri = Some(target_path.to_string_lossy().to_string());

    if let Some(parent) = full_path.parent() {
        fs::create_dir_all(parent).context("create pool directory")?;
    }

    let existing = tokio::runtime::Handle::current()
        .block_on(collection::lookup(tx.as_mut(), name.as_ref()))
        .context("lookup existing collection entry")?;

    match existing {
        Some(e) if e.source_release as u64 > meta.source_release => {
            return Err(eyre!(
                "{:?}: Newer candidate (rel: {}) exists already",
                &file,
                e.source_release
            ));
        }
        Some(e) if e.source_release as u64 == meta.source_release && e.build_release as u64 > meta.build_release => {
            return Err(eyre!("{:?}: Bump release number to {}", &file, e.source_release + 1));
        }
        Some(e) if e.source_release as u64 == meta.source_release => {
            warn!("{:?}: Cannot include build with identical release field", &file);
            return Ok(());
        }
        _ => {}
    }

    if destructive_move {
        fs::rename(&package.path, &full_path).context("rename download to pool")?;
    } else {
        hardlink_or_copy(&package.path, &full_path).context("link or copy download to pool")?;
    }

    // Adding meta records is idempotent as we delete / insert so
    // it doesn't matter we are adding them outside a TX if we encounter
    // an error
    state
        .meta_db
        .add(id.clone(), meta.clone())
        .context("add package to meta db")?;

    // Will only be added once TX is committed / all packages
    // are succsefully handled
    tokio::runtime::Handle::current()
        .block_on(collection::record(tx, collection::Entry::new(id, meta)))
        .context("record collection entry")?;

    info!(file_name, source_id, "Package imported");

    Ok(())
}

fn relative_pool_dir(source_id: &str) -> Result<PathBuf> {
    let lower = source_id.to_lowercase();

    if lower.is_empty() {
        return Err(eyre!("Invalid archive, package name is empty"));
    }

    let mut portion = &lower[0..1];

    if lower.len() > 4 && lower.starts_with("lib") {
        portion = &lower[0..4];
    }

    Ok(Path::new("pool").join(portion).join(lower))
}

fn hardlink_or_copy(from: &Path, to: &Path) -> Result<()> {
    use std::fs;

    // Attempt hard link
    let link_result = fs::hard_link(from, to);

    // Copy instead
    if link_result.is_err() {
        fs::copy(from, to)?;
    }

    Ok(())
}

#[tracing::instrument(skip_all)]
pub async fn reindex(state: &State) -> Result<()> {
    let mut entries = collection::list(
        state
            .service_db
            .acquire()
            .await
            .context("acquire database connection")?
            .as_mut(),
    )
    .await
    .context("list entries from collection db")?;
    entries.sort_by(|a, b| a.source_id.cmp(&b.source_id).then_with(|| a.name.cmp(&b.name)));

    let now = Instant::now();

    // Write stone is blocking
    tokio::task::spawn_blocking({
        let span = tracing::Span::current();
        let state = state.clone();

        move || {
            span.in_scope(|| {
                use std::fs::{self, File};

                let temp_dir = state.state_dir.join("work/volatile/x86_64");
                let temp_path = temp_dir.join("stone.index");

                // TODO: Replace w/ configurable index path
                let dir = state.state_dir.join("public/volatile/x86_64");
                let path = dir.join("stone.index");

                if !temp_dir.exists() {
                    fs::create_dir_all(&temp_dir).context("create volatile directory")?;
                }

                if !dir.exists() {
                    fs::create_dir_all(&dir).context("create volatile directory")?;
                }

                info!(?temp_path, ?path, "Indexing");

                let mut file = File::create(&temp_path).context("create index file")?;
                let mut writer = stone::Writer::new(&mut file, stone::header::v1::FileType::Repository)
                    .context("create stone writer")?;

                for entry in entries {
                    let mut meta = state
                        .meta_db
                        .get(&entry.package_id.clone().into())
                        .context("get package from meta db")?;

                    // TODO: Replace hardcoded relative path
                    // once we have non-hardcoded index path
                    meta.uri = Some(format!(
                        "../../{}",
                        meta.uri
                            .ok_or(eyre!("Package {} is missing URI in metadata", &entry.package_id))?,
                    ));

                    writer
                        .add_payload(meta.to_stone_payload().as_slice())
                        .context("add meta payload")?;
                }

                writer.finalize().context("finalize stone index")?;

                fs::rename(&temp_path, &path).context(format!("rename {temp_path:?} to {path:?}"))?;

                Result::<_, eyre::Report>::Ok(())
            })
        }
    })
    .await
    .context("spawn blocking")??;

    let elapsed = format!("{}ms", now.elapsed().as_millis());

    info!(elapsed, "Index complete");

    Ok(())
}

#[tracing::instrument(skip_all)]
async fn prune_orphaned_packages(state: &State) -> Result<()> {
    use tokio::fs;

    let pool_dir = state.state_dir.join("public/pool");

    // All stones on the filesystem
    let stones = tokio::task::spawn_blocking({
        let pool_dir = pool_dir.clone();
        move || enumerate_stones(&pool_dir)
    })
    .await
    .context("spawn blocking")?
    .context("enumerate stones")?;

    // Packages in the current index
    let indexed_packages = collection::list(
        state
            .service_db
            .acquire()
            .await
            .context("acquire database connection")?
            .as_mut(),
    )
    .await
    .context("list entries from collection db")?;

    // Package id is the sha256 of the file on disk. We use this to detect
    // orphaned packages that aren't part of the existing index
    let index_hashes = indexed_packages
        .into_iter()
        .map(|entry| entry.package_id)
        .collect::<Vec<_>>();

    let orphaned_stones = stones
        .into_iter()
        .filter(|stone| !index_hashes.contains(&stone.sha256sum))
        .collect::<Vec<_>>();

    if orphaned_stones.is_empty() {
        info!("No orphaned stones on disk");
        return Ok(());
    }

    for stone in &orphaned_stones {
        let relative_path = stone.path.strip_prefix(&pool_dir).expect("lives in pool dir");

        fs::remove_file(&stone.path)
            .await
            .context(format!("remove orphaned stone {:?}", stone.path))?;

        info!(path = ?relative_path, "Orphaned stone removed");
    }

    info!("num_stones" = orphaned_stones.len(), "All orphaned stones removed");

    Ok(())
}

fn enumerate_stones(dir: &Path) -> Result<Vec<Package>> {
    use std::fs::{self, File};
    use std::io;

    let contents = fs::read_dir(dir).context("read directory")?;

    let mut packages = vec![];

    for entry in contents {
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

            packages.push(Package { name, path, sha256sum });
        } else if meta.is_dir() {
            packages.extend(enumerate_stones(&path)?);
        }
    }

    // this is where we human sort the packages in ascending order to enable a directory to have
    // multiple stones with the same recipe origin but different versions, source-releases
    // and build-releases.
    // The to_string_lossy() call is necessary because Rust cannot guarantee that the OS formats
    // PathBuf filenames in valid utf-8.
    packages.sort_by(|a, b| (a.path.to_string_lossy()).natural_cmp(&b.path.to_string_lossy()));
    Ok(packages)
}
