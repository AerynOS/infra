use std::path::Path;

use color_eyre::eyre::{Context, OptionExt, Result};
use futures_util::StreamExt;
use http::Uri;
use moss::{
    db::meta,
    package::{self, Meta},
    request,
};
use service::State;
use stone::StoneDecodedPayload;
use tokio::{
    fs::{self, File},
    io::AsyncWriteExt,
    task,
};
use tracing::{Span, info};

use super::{Profile, Status, set_status};

#[tracing::instrument(name = "refresh_profile", skip_all, fields(profile = profile.name))]
pub async fn refresh(state: &State, profile: &mut Profile, db: meta::Database) -> Result<()> {
    let profile_dir = state.cache_dir.join("profile").join(profile.id.to_string());
    let index_path = profile_dir.join("index");

    set_status(
        &mut *state.service_db.acquire().await.context("acquire db conn")?,
        profile,
        Status::Refreshing,
    )
    .await
    .context("set profile status")?;

    if !fs::try_exists(&profile_dir).await.unwrap_or_default() {
        fs::create_dir_all(&profile_dir)
            .await
            .context("create profile cache dir")?;
    }

    fetch_index(&profile.index_uri, &index_path)
        .await
        .context("fetch index file")?;

    task::spawn_blocking(move || update_db(db, &index_path))
        .await
        .context("join handle")?
        .context("update index db")?;

    set_status(
        &mut *state.service_db.acquire().await.context("acquire db conn")?,
        profile,
        Status::Indexed,
    )
    .await
    .context("set profile status")?;

    info!("Profile refreshed");

    Ok(())
}

async fn fetch_index(uri: &Uri, index_path: &Path) -> Result<()> {
    let mut stream = request::get(uri.to_string().parse().context("invalid url")?)
        .await
        .context("request index file")?;

    let mut out = File::create(index_path).await?;

    while let Some(chunk) = stream.next().await {
        out.write_all(&chunk.context("download index file")?)
            .await
            .context("write index file")?;
    }

    out.flush().await.context("flush index file")?;

    Ok(())
}

fn update_db(db: meta::Database, index_path: &Path) -> Result<()> {
    use std::fs::File;

    db.wipe()?;

    let mut file = File::open(index_path).context("open index file")?;
    let mut reader = stone::read(&mut file).context("read stone header")?;

    let payloads = reader
        .payloads()
        .context("read stone payloads")?
        .collect::<Result<Vec<_>, _>>()
        .context("read stone payloads")?;

    let packages = payloads
        .into_iter()
        .filter_map(|payload| {
            if let StoneDecodedPayload::Meta(meta) = payload {
                Some(meta)
            } else {
                None
            }
        })
        .map(|payload| {
            let meta = Meta::from_stone_payload(&payload.body).context("convert meta payload")?;

            let span = Span::current();
            span.record("package", meta.name.as_str());

            // Create id from hash of meta
            let hash = meta.hash.clone().ok_or_eyre("missing package hash")?;
            let id = package::Id::from(hash);

            Ok((id, meta))
        })
        .collect::<Result<Vec<_>>>()?;

    db.batch_add(packages).context("batch add index meta to db")?;

    Ok(())
}
