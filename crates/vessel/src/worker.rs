use std::{
    convert::Infallible,
    ffi::OsStr,
    future::Future,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
};

use color_eyre::eyre::{self, eyre, Context, Result};
use futures::{stream, StreamExt, TryStreamExt};
use moss::db::meta;
use service::{api, database, request, Endpoint};
use sha2::{Digest, Sha256};
use tokio::{fs, sync::mpsc, time::Instant};
use tracing::{error, info, info_span, Instrument};
use url::Url;

use crate::collection;

pub type Sender = mpsc::UnboundedSender<Message>;

#[derive(Debug, strum::Display)]
#[strum(serialize_all = "kebab-case")]
pub enum Message {
    ImportPackages {
        task_id: u64,
        endpoint: Endpoint,
        packages: Vec<Package>,
    },
    ImportDirectory(PathBuf),
}

#[derive(Debug)]
pub struct Package {
    pub url: Url,
    pub sha256sum: String,
}

pub async fn run(service_state: &service::State) -> Result<(Sender, impl Future<Output = Result<(), Infallible>>)> {
    let state = State::new(service_state).await.context("construct state")?;

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
struct State {
    state_dir: PathBuf,
    service_db: service::Database,
    meta_db: meta::Database,
}

impl State {
    async fn new(service_state: &service::State) -> Result<Self> {
        let meta_db = meta::Database::new(service_state.db_dir.join("meta").to_string_lossy().as_ref())
            .context("failed to open meta database")?;

        Ok(Self {
            state_dir: service_state.state_dir.clone(),
            service_db: service_state.service_db.clone(),
            meta_db,
        })
    }
}

async fn handle_message(state: &State, message: Message) -> Result<()> {
    match message {
        Message::ImportPackages {
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
                let client = service::Client::new(endpoint.host_address.clone())
                    .with_endpoint_auth(endpoint.id, state.service_db.clone());

                match import_packages(state, packages).await {
                    Ok(()) => {
                        info!("All packages imported");

                        client
                            .send::<api::v1::summit::ImportSucceeded>(&api::v1::summit::ImportBody { task_id })
                            .await
                            .context("send import succeeded request")?;
                    }
                    Err(e) => {
                        let error = service::error::chain(e.as_ref() as &dyn std::error::Error);
                        error!(%error, "Failed to import packages");

                        client
                            .send::<api::v1::summit::ImportFailed>(&api::v1::summit::ImportBody { task_id })
                            .await
                            .context("send import failed request")?;
                    }
                }

                Ok(())
            }
            .instrument(span)
            .await
        }
        Message::ImportDirectory(directory) => {
            let span = info_span!("import_directory", directory = directory.to_string_lossy().to_string());

            async move {
                info!("Import started");

                let stones = tokio::task::spawn_blocking(move || enumerate_stones(&directory))
                    .await
                    .context("spawn blocking")?
                    .context("enumerate stones")?;

                let num_stones = stones.len();

                if num_stones > 0 {
                    import_packages(state, stones).await.context("import packages")?;

                    info!(num_stones, "All stones imported");
                } else {
                    info!("No stones to import");
                }

                Ok(())
            }
            .instrument(span)
            .await
        }
    }
}

async fn import_packages(state: &State, packages: Vec<Package>) -> Result<()> {
    let downloads = stream::iter(packages.into_iter())
        .map(|package| download_package(&state.state_dir, package))
        .buffer_unordered(moss::environment::MAX_NETWORK_CONCURRENCY)
        .try_collect::<Vec<(Package, PathBuf)>>()
        .await
        .context("download package")?;

    // Stone is read in blocking manner
    let tx = tokio::task::spawn_blocking({
        let span = tracing::Span::current();
        let state = state.clone();

        // Rollback any collection DB inserts if we encounter any failures
        let mut tx = state.service_db.begin().await.context("start db tx")?;

        move || {
            span.in_scope(|| {
                for (package, path) in downloads {
                    import_package(&state, &mut tx, &package, &path, true)?;
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

    Ok(())
}

fn import_package(
    state: &State,
    tx: &mut database::Transaction,
    package: &Package,
    download_path: &Path,
    destructive_move: bool,
) -> Result<()> {
    use std::fs::{self, File};

    let mut file = File::open(download_path).context("open staged stone")?;
    let file_size = file.metadata().context("read file metadata")?.size();

    let mut reader = stone::read(&mut file).context("create stone reader")?;

    let stone::Header::V1(header) = reader.header;

    if !matches!(header.file_type, stone::header::v1::FileType::Binary) {
        return Err(eyre!("Invalid archive, expected binary stone"));
    }

    let payloads = reader
        .payloads()
        .context("get stone payload reader")?
        .collect::<Result<Vec<_>, _>>()
        .context("read stone payloads")?;

    let meta_payload = payloads
        .iter()
        .find_map(stone::read::PayloadKind::meta)
        .ok_or(eyre!("Invalid archive, missing meta payload"))?;

    let mut meta = moss::package::Meta::from_stone_payload(&meta_payload.body)
        .context("convert meta payload into moss package metadata")?;

    let name = meta.name.clone();
    let source_id = meta.source_id.clone();

    meta.hash = Some(package.sha256sum.clone());
    meta.download_size = Some(file_size);

    let id = moss::package::Id::from(package.sha256sum.clone());

    let pool_dir = relative_pool_dir(&source_id)?;
    let file_name = Path::new(package.url.path())
        .file_name()
        .ok_or(eyre!("Invalid archive, no file name in URI"))?;
    let target_path = pool_dir.join(file_name);
    let full_path = state.state_dir.join("public").join(&target_path);

    meta.uri = Some(target_path.to_string_lossy().to_string());

    if let Some(parent) = full_path.parent() {
        fs::create_dir_all(parent).context("create pool directory")?;
    }

    let existing = tokio::runtime::Handle::current()
        .block_on(collection::lookup(tx.as_mut(), name.as_ref()))
        .context("lookup existing collection record")?;

    match existing {
        Some(e) if e.source_release as u64 > meta.source_release => {
            return Err(eyre!("Newer candidate (rel: {}) exists already", e.source_release));
        }
        Some(e) if e.source_release as u64 == meta.source_release && e.build_release as u64 > meta.build_release => {
            return Err(eyre!("Bump release number to {}", e.source_release + 1));
        }
        Some(e) if e.source_release as u64 == meta.source_release => {
            return Err(eyre!("Cannot include build with identical release field"));
        }
        _ => {}
    }

    if destructive_move {
        fs::rename(download_path, &full_path).context("rename download to pool")?;
    } else {
        hardlink_or_copy(download_path, &full_path).context("link or copy download to pool")?;
    }

    // Adding meta records is idempotent as we delete / insert so
    // it doesn't matter we are adding them outside a TX if we encounter
    // and error
    state
        .meta_db
        .add(id.clone(), meta.clone())
        .context("add package to meta db")?;

    // Will only be added once TX is committed / all packages
    // are succsefully handled
    tokio::runtime::Handle::current()
        .block_on(collection::record(tx, collection::Record::new(id, meta)))
        // English why you be like this
        .context("record collection record")?;

    info!(file_name = file_name.to_str(), source_id, "Package imported");

    Ok(())
}

async fn download_package(state_dir: &Path, package: Package) -> Result<(Package, PathBuf)> {
    let path = download_path(state_dir, &package.sha256sum).await?;

    request::download_and_verify(package.url.clone(), &path, &package.sha256sum).await?;

    Ok((package, path))
}

async fn download_path(state_dir: &Path, hash: &str) -> Result<PathBuf> {
    if hash.len() < 5 {
        return Err(eyre!("Invalid SHA256 hash length"));
    }

    let dir = state_dir.join("staging").join(&hash[..5]).join(&hash[hash.len() - 5..]);

    if !dir.exists() {
        fs::create_dir_all(&dir)
            .await
            .context("create download parent directory")?;
    }

    Ok(dir.join(hash))
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

async fn reindex(state: &State) -> Result<()> {
    let mut records = collection::list(
        state
            .service_db
            .acquire()
            .await
            .context("acquire database connection")?
            .as_mut(),
    )
    .await
    .context("list records from collection db")?;
    records.sort_by(|a, b| a.source_id.cmp(&b.source_id).then_with(|| a.name.cmp(&b.name)));

    let now = Instant::now();

    // Write stone is blocking
    tokio::task::spawn_blocking({
        let span = tracing::Span::current();
        let state = state.clone();

        move || {
            span.in_scope(|| {
                use std::fs::{self, File};

                // TODO: Replace w/ configurable index path
                let dir = state.state_dir.join("public/volatile/x86_64");
                let path = dir.join("stone.index");

                if !dir.exists() {
                    fs::create_dir_all(&dir).context("create volatile directory")?;
                }

                info!(?path, "Indexing");

                let mut file = File::create(path).context("create index file")?;
                let mut writer = stone::Writer::new(&mut file, stone::header::v1::FileType::Repository)
                    .context("create stone writer")?;

                for record in records {
                    let mut meta = state
                        .meta_db
                        .get(&record.package_id.clone().into())
                        .context("get package from meta db")?;

                    // TODO: Replace hardcoded relative path
                    // once we have non-hardcoded index path
                    meta.uri = Some(format!(
                        "../../{}",
                        meta.uri
                            .ok_or(eyre!("Package {} is missing URI in metadata", &record.package_id))?,
                    ));

                    writer
                        .add_payload(meta.to_stone_payload().as_slice())
                        .context("add meta payload")?;
                }

                writer.finalize().context("finalize stone index")?;

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

fn enumerate_stones(dir: &Path) -> Result<Vec<Package>> {
    use std::fs::{self, File};
    use std::io;

    let contents = fs::read_dir(dir).context("read directory")?;

    let mut files = vec![];

    for entry in contents {
        let entry = entry.context("read directory entry")?;
        let path = entry.path();
        let meta = entry.metadata().context("read directory entry metadata")?;

        if meta.is_file() && path.extension() == Some(OsStr::new("stone")) {
            let url = format!("file://{}", path.to_string_lossy())
                .parse()
                .context("invalid file uri")?;

            let mut hasher = Sha256::default();

            io::copy(&mut File::open(&path).context("open file")?, &mut hasher).context("hash file")?;

            let sha256sum = hex::encode(hasher.finalize());

            files.push(Package { url, sha256sum });
        } else if meta.is_dir() {
            files.extend(enumerate_stones(&path)?);
        }
    }

    Ok(files)
}
