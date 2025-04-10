use std::path::{Path, PathBuf};

use color_eyre::eyre::{Context, OptionExt, Result};
use moss::{db::meta, package::Meta};
use service::{State, git};
use sqlx::SqliteConnection;
use tokio::{fs, task};
use tracing::{info, trace};

use super::{Repository, Status, set_description, set_status};

#[tracing::instrument(name = "reindex_repository", skip_all, fields(commit_ref = repo.commit_ref))]
pub async fn reindex(
    conn: &mut SqliteConnection,
    state: &State,
    mut repo: Repository,
    db: meta::Database,
) -> Result<Repository> {
    info!("Reindexing repository");

    set_status(conn, &mut repo, Status::Indexing)
        .await
        .context("set status to indexing")?;

    let repo_dir = state.cache_dir.join("repository").join(repo.id.to_string());
    let clone_dir = repo_dir.join("clone");
    let work_dir = repo_dir.join("work");

    checkout_commit(&repo, &clone_dir, &work_dir)
        .await
        .context("checkout commit")?;

    update_readme(conn, &mut repo, &work_dir)
        .await
        .context("update readme")?;

    let num_indexed = task::spawn_blocking(move || update_manifests(work_dir, db))
        .await
        .context("join handle")?
        .context("update manifests")?;

    set_status(conn, &mut repo, Status::Idle)
        .await
        .context("set status to idle")?;

    info!(num_indexed, "Indexing finished");

    Ok(repo)
}

async fn checkout_commit(repo: &Repository, clone_dir: &Path, work_dir: &Path) -> Result<()> {
    let _ = fs::remove_dir_all(&work_dir).await;

    git::checkout_worktree(
        clone_dir,
        work_dir,
        repo.commit_ref.as_ref().ok_or_eyre("no commit checked out")?,
    )
    .await
    .context("git checkout worktree")?;

    Ok(())
}

async fn update_readme(conn: &mut SqliteConnection, repo: &mut Repository, work_dir: &Path) -> Result<()> {
    let path = work_dir.join("README.md");

    if !fs::try_exists(&path).await.unwrap_or_default() {
        return Ok(());
    }

    let content = fs::read_to_string(&path).await.context("read README.md")?;

    set_description(conn, repo, content).await.context("set description")?;

    Ok(())
}

fn update_manifests(work_dir: PathBuf, db: meta::Database) -> Result<usize> {
    let manifests = enumerate_manifests(&work_dir).context("enumerate manifests")?;

    db.wipe().context("wipe meta db")?;

    for manifest in &manifests {
        install_manifest(&db, &work_dir, manifest).context("install manifest")?;
    }

    Ok(manifests.len())
}

fn enumerate_manifests(dir: &Path) -> Result<Vec<PathBuf>> {
    use std::fs;

    let contents = fs::read_dir(dir).context("read dir")?;

    let mut manifests = vec![];

    for result in contents {
        let entry = result.context("read dir entry")?;
        let meta = entry.metadata().context("entry metadata")?;
        let path = entry.path();
        let name = path.file_name().and_then(|s| s.to_str()).unwrap_or_default();

        if meta.is_file() && name.starts_with("manifest.") && name.ends_with(".bin") {
            manifests.push(path);
        } else if meta.is_dir() {
            manifests.extend(enumerate_manifests(&path)?);
        }
    }

    Ok(manifests)
}

#[tracing::instrument(skip_all, fields(manifest = %manifest.display()))]
fn install_manifest(db: &meta::Database, work_dir: &Path, manifest: &Path) -> Result<()> {
    use std::fs;

    let parent = manifest.parent().ok_or_eyre("manifest path has no parent")?;
    let recipe_path = parent.join("stone.yaml");
    let relative_path = recipe_path
        .strip_prefix(work_dir)
        // Impossible
        .context("manifest not a descendent of work dir")?;

    let hash = compute_hash(&recipe_path).context("compute sha256 of recipe")?;

    let recipe = stone_recipe::from_str(&fs::read_to_string(&recipe_path).context("read recipe file")?)
        .context("parse recipe file")?;
    let manifest = fs::File::open(manifest).context("open manifest reader")?;

    let mut reader = stone::read(&manifest).context("read stone header")?;

    let payloads = reader
        .payloads()
        .context("read stone payloads")?
        .collect::<Result<Vec<_>, _>>()
        .context("read stone payloads")?;

    let mut meta_payloads = payloads.iter().filter_map(stone::read::PayloadKind::meta);

    // Seed metadata from the first payload
    let first = meta_payloads.next().ok_or_eyre("missing meta payload in manifest")?;

    let mut meta = Meta::from_stone_payload(&first.body).context("convert meta payload to metadata")?;

    // Overwrite from root package since we don't know if root package
    // was first meta payload that seeded this
    meta.summary = recipe.package.summary.clone().unwrap_or_default();
    meta.description = recipe.package.description.clone().unwrap_or_default();
    meta.homepage = recipe.source.homepage.clone();
    // Source id should be the same for all packages / use as the name
    meta.name = meta.source_id.clone().into();
    meta.hash = Some(hash);
    meta.uri = Some(relative_path.display().to_string());

    // Extend deps & such from addtl. packages
    for payload in meta_payloads {
        let addtl = Meta::from_stone_payload(&payload.body).context("convert meta payload to metadata")?;

        meta.licenses.extend(addtl.licenses);
        meta.dependencies.extend(addtl.dependencies);
        meta.providers.extend(addtl.providers);
    }

    db.add(meta.id().into(), meta).context("add meta to db")?;

    trace!("Manifest installed");

    Ok(())
}

fn compute_hash(path: &Path) -> Result<String> {
    use sha2::{Digest, Sha256};
    use std::{fs, io};

    let mut hasher = Sha256::default();

    io::copy(&mut fs::File::open(path)?, &mut hasher)?;

    Ok(hex::encode(hasher.finalize()))
}
