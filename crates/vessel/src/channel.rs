use std::collections::HashMap;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

use color_eyre::eyre::{self, Context, OptionExt, Result, bail, ensure, eyre};
use itertools::Itertools;
use service::database::Transaction;
use stone::{StoneDecodedPayload, StoneHeader, StoneHeaderV1FileType, StoneWriter};
use tokio::time::Instant;
use tracing::{debug, info, info_span, warn};

use crate::{Package, State, package};

pub use self::prune::prune;
pub use self::version::Version;

pub mod db;
mod prune;
pub mod version;

// TODO: Hardcoded for now but eventually will
// be provided by summit & vessel can handle many
// channels
pub const DEFAULT_CHANNEL: &str = "main";

#[derive(Debug, strum::Display)]
#[strum(serialize_all = "kebab-case")]
pub enum Command {
    UpdateStream {
        stream: version::Stream,
        version: Version,
    },
    AddTag {
        tag: version::TagIdentifier,
        history: version::HistoryIdentifier,
    },
    RemoveTag {
        tag: version::TagIdentifier,
    },
}

#[tracing::instrument(skip_all, fields(%channel, %command))]
pub async fn handle_command(state: &State, channel: &str, command: Command) -> Result<()> {
    let mut tx = state.service_db().begin().await.context("begin db tx")?;

    match command {
        Command::UpdateStream { stream, version } => {
            let stream_version = stream.version();

            ensure!(version != stream_version, "cannot update stream to itself");

            // Resolve the provided version to it's underlying history so we
            // can link against that
            let history = if let Version::History { identifier } = version {
                identifier
            } else {
                let version = db::find_related_history(tx.as_mut(), channel, &version)
                    .await
                    .context("find related history")?
                    .ok_or_eyre(format!("{version} doesn't exist"))?
                    .version;

                let Version::History { identifier } = version else {
                    bail!("non-history version returned from `db::find_related_history`");
                };

                identifier
            };

            // Update database
            db::link_version_to_history(&mut tx, channel, &stream_version, &history)
                .await
                .context("link version to history in db")?;

            // Update filesystem
            symlink_version_to_history(state, channel, &stream_version, &history)
                .await
                .context("symlink stream to history folder")?;

            info!(%stream, %history, "Stream updated");
        }
        Command::AddTag { tag, history } => {
            let tag = Version::Tag { identifier: tag };

            // Ensure tag doesn't already exist
            if db::find_related_history(tx.as_mut(), channel, &tag)
                .await
                .context("find related history")?
                .is_some()
            {
                bail!("{tag} already exists");
            }

            // Update database
            db::link_version_to_history(&mut tx, channel, &tag, &history)
                .await
                .context("link version to history in db")?;

            // Update filesystem
            symlink_version_to_history(state, channel, &tag, &history)
                .await
                .context("symlink stream to history folder")?;

            info!(%tag, %history, "Tag added");
        }
        Command::RemoveTag { tag: tag_identifier } => {
            let tag = Version::Tag {
                identifier: tag_identifier.clone(),
            };

            // Update database
            db::delete_tag(&mut tx, channel, &tag_identifier)
                .await
                .context("delete tag in db")?;

            // Update filesystem
            remove_version_symlink(state, channel, &tag)
                .await
                .context("remove symlink to history folder")?;

            info!(%tag, "Tag removed");
        }
    }

    tx.commit().await.context("commit db tx")?;

    Ok(())
}

/// Enumerates an external directory of stones & calls [`import_packages`] on them
/// without `destructive_move`
#[tracing::instrument(skip_all, fields(%channel, ?directory))]
pub async fn import_directory(state: &State, channel: &str, directory: PathBuf) -> Result<()> {
    info!("Import started");

    let stones = tokio::task::spawn_blocking(move || package::enumerate(&directory))
        .await
        .context("spawn blocking")?
        .context("enumerate stones")?;

    let num_stones = stones.len();

    if num_stones > 0 {
        import_packages(state, channel, stones, false)
            .await
            .context("import packages")?;

        info!(num_stones, "All stones imported");
    } else {
        info!("No stones to import");
    }

    Ok(())
}

/// Import packages to the provided channel, recording a new `history` index with the new packages
/// and linking `volatile` to this latest `history`
///
/// If `destructive_move` if `true`, packages will be _moved_ otherwise they will be hardlinked or copied
/// to the channel's `pool`
#[tracing::instrument(skip_all, fields(%channel, %destructive_move, num_packages = packages.len()))]
pub async fn import_packages(
    state: &State,
    channel: &str,
    packages: Vec<Package>,
    destructive_move: bool,
) -> Result<()> {
    // Stone is read in blocking manner
    let (mut tx, mut entries) = tokio::task::spawn_blocking({
        let span = tracing::Span::current();
        let state = state.clone();
        let channel = channel.to_owned();

        // Rollback any collection DB inserts if we encounter any failures
        let mut tx = state.service_db().begin().await.context("start db tx")?;

        move || {
            span.in_scope(|| {
                let entries = packages
                    .iter()
                    .map(|package| {
                        import_package(&state, &mut tx, package, destructive_move, &channel)
                            .context(format!("import package {}", package.name))
                    })
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter()
                    .flatten()
                    .collect::<Vec<_>>();

                eyre::Ok((tx, entries))
            })
        }
    })
    .await
    .context("spawn blocking")?
    .context("import package")?;

    entries.sort_by(|a, b| {
        a.name
            .cmp(&b.name)
            .then_with(|| a.source_release.cmp(&b.source_release))
    });

    // Record entries as new version
    let new_version = db::record_history(&mut tx, channel, &entries).await?;

    // No failures, commit it all to collection DB
    tx.commit().await.context("commit collection db tx")?;

    // Reindex the new history version
    reindex(state, channel, &new_version).await.context("reindex history")?;

    Ok(())
}

fn import_package(
    state: &State,
    tx: &mut Transaction,
    package: &Package,
    destructive_move: bool,
    channel: &str,
) -> Result<Option<version::Entry>> {
    use std::fs::{self, File};

    debug!("Attempting to open {:?}", &package.path);

    let mut file = File::open(&package.path).context("open staged stone")?;
    let file_size = file.metadata().context("read file metadata")?.size();

    let mut reader = stone::read(&mut file).context("create stone reader")?;

    let StoneHeader::V1(header) = reader.header;

    ensure!(
        matches!(header.file_type, StoneHeaderV1FileType::Binary),
        "{file:?}: Invalid archive, expected binary stone",
    );

    let payloads = reader
        .payloads()
        .context("get stone payload reader")?
        .collect::<Result<Vec<_>, _>>()
        .context("read stone payloads")?;

    let meta_payload = payloads
        .iter()
        .find_map(StoneDecodedPayload::meta)
        .ok_or(eyre!("{:?}: Invalid archive, missing meta payload", &file))?;

    let mut meta = moss::package::Meta::from_stone_payload(&meta_payload.body)
        .context("convert meta payload into moss package metadata")?;

    let name = meta.name.clone();
    let source_id = meta.source_id.clone();

    ensure!(!source_id.is_empty(), "Invalid archive, source id is empty");

    meta.hash = Some(package.sha256sum.clone());
    meta.download_size = Some(file_size);

    let id = moss::package::Id::from(package.sha256sum.clone());
    let entry = version::Entry::new(&id, &meta);

    let filename = entry.filename();
    let full_path = state.public_dir().join(channel).join(entry.relative_path());

    if let Some(parent) = full_path.parent() {
        fs::create_dir_all(parent).context("create pool directory")?;
    }

    let existing = tokio::runtime::Handle::current()
        .block_on(db::lookup_entry(
            tx.as_mut(),
            channel,
            &Version::Volatile,
            name.as_str(),
            &meta.architecture,
        ))
        .context("lookup existing collection entry")?;

    match existing {
        Some(e) if e.source_release as u64 > meta.source_release => {
            bail!("{file:?}: Newer candidate (rel: {}) exists already", e.source_release);
        }
        Some(e) if e.source_release as u64 == meta.source_release && e.build_release as u64 > meta.build_release => {
            bail!("{file:?}: Bump release number to {}", e.source_release + 1);
        }
        Some(e) if e.source_release as u64 == meta.source_release => {
            warn!("{file:?}: Cannot include build with identical release field");
            return Ok(None);
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

    info!(filename, source_id, "Package imported");

    Ok(Some(entry))
}

/// Reindex the latest `history` version & relink `volatile` to it
#[tracing::instrument(skip_all, fields(%channel))]
pub async fn reindex_latest(state: &State, channel: &str) -> Result<()> {
    match db::find_related_history(
        state.service_db().acquire().await.context("acquire db conn")?.as_mut(),
        channel,
        &Version::Volatile,
    )
    .await
    .context("find latest volatile history")?
    {
        Some(history) => {
            reindex(state, channel, &history.version).await?;
        }
        None => {
            import_packages(state, channel, vec![], false).await?;
        }
    }

    Ok(())
}

#[tracing::instrument(skip_all, fields(%channel, %version))]
async fn reindex(state: &State, channel: &str, version: &Version) -> Result<()> {
    let Version::History { identifier } = version else {
        bail!("reindex called on non-history version");
    };

    let mut entries = db::entries(
        state
            .service_db()
            .acquire()
            .await
            .context("acquire database connection")?
            .as_mut(),
        channel,
        version,
    )
    .await
    .context("list entries from collection db")?;
    entries.sort_by(|a, b| a.source_id.cmp(&b.source_id).then_with(|| a.name.cmp(&b.name)));

    let now = Instant::now();

    // Write stone is blocking
    tokio::task::spawn_blocking({
        let span = tracing::Span::current();
        let state = state.clone();
        let version = version.clone();
        let channel = channel.to_owned();

        move || {
            let index_arch = |arch: &str, entries: &[&version::Entry]| {
                let span = info_span!(parent: &span, "index", %arch);

                span.in_scope(|| {
                    use std::fs::{self, File};

                    let temp_path = state
                        .work_dir()
                        .join(&channel)
                        .join(version.relative_index_path("x86_64"));
                    let temp_dir = temp_path.parent().unwrap();

                    // TODO: Replace w/ configurable index path
                    let path = state
                        .public_dir()
                        .join(&channel)
                        .join(version.relative_index_path("x86_64"));
                    let dir = path.parent().unwrap();

                    if !temp_dir.exists() {
                        fs::create_dir_all(temp_dir).context("create volatile directory")?;
                    }

                    if !dir.exists() {
                        fs::create_dir_all(dir).context("create volatile directory")?;
                    }

                    info!(?temp_path, ?path, "Indexing");

                    let mut file = File::create(&temp_path).context("create index file")?;
                    let mut writer = StoneWriter::new(&mut file, StoneHeaderV1FileType::Repository)
                        .context("create stone writer")?;

                    for entry in entries {
                        let mut meta = state
                            .meta_db
                            .get(&entry.package_id.clone().into())
                            .context("get package from meta db")?;

                        meta.uri = Some(version.relative_index_to_entry_path(entry));

                        writer
                            .add_payload(meta.to_stone_payload().as_slice())
                            .context("add meta payload")?;
                    }

                    writer.finalize().context("finalize stone index")?;

                    fs::rename(&temp_path, &path).context(format!("rename {temp_path:?} to {path:?}"))?;

                    eyre::Ok(())
                })
            };

            // Ensure x86_64 is always created, even if empty
            let mut entries_by_arch = HashMap::<&str, Vec<&version::Entry>>::from_iter([("x86_64", vec![])]);

            entries_by_arch.extend(
                entries
                    .iter()
                    .map(|entry| (entry.arch.as_str(), entry))
                    .into_group_map(),
            );

            for (arch, entries) in entries_by_arch {
                index_arch(arch, &entries)?;
            }

            eyre::Ok(())
        }
    })
    .await
    .context("spawn blocking")??;

    // Symlink volatile to new history version
    symlink_version_to_history(state, channel, &Version::Volatile, identifier)
        .await
        .context("symlink volatile")?;

    let elapsed = format!("{}ms", now.elapsed().as_millis());

    info!(elapsed, "Index complete");

    Ok(())
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

async fn symlink_version_to_history(
    state: &State,
    channel: &str,
    version: &Version,
    history: &version::HistoryIdentifier,
) -> Result<()> {
    use tokio::fs;

    let history = Version::History {
        identifier: history.clone(),
    };

    let source = Path::new("..").join(history.relative_base_dir());

    let dest = state.public_dir().join(channel).join(version.relative_base_dir());
    let dest_parent = dest.parent().unwrap();

    if !dest_parent.exists() {
        let _ = fs::create_dir_all(dest_parent).await;
    } else {
        let _ = fs::remove_file(&dest).await;
    }

    fs::symlink(source, dest)
        .await
        .context("link from {source:?} to {dest:?}")?;

    Ok(())
}

async fn remove_version_symlink(state: &State, channel: &str, version: &Version) -> Result<()> {
    use tokio::fs;

    let path = state.public_dir().join(channel).join(version.relative_base_dir());

    fs::remove_file(&path).await?;

    Ok(())
}
