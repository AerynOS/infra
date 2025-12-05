use bytes::BytesMut;
use color_eyre::eyre::{Context, Result, ensure};
use prost::Message;
use service::{
    State,
    client::{TokenClient, VesselServiceClient},
    error,
    grpc::{collectable, summit::BuilderFinished, vessel::UploadRequest},
};
use tokio::{
    fs::{self, File},
    sync::mpsc,
};
use tokio_stream::{StreamExt, wrappers::ReceiverStream};
use tokio_util::io::ReaderStream;
use tracing::{debug, error, info, warn};

#[tracing::instrument(
    skip_all,
    fields(
        task_id = build.task_id,
        upstream = uri,
    )
)]
pub async fn upload(state: State, build: BuilderFinished, token: String, uri: &str) -> Result<()> {
    info!("Upload started");

    let asset_dir = state.root.join("assets").join(build.task_id.to_string());

    let mut header = BytesMut::new();
    build.encode(&mut header).context("encode header")?;

    let mut client = VesselServiceClient::connect_with_token(uri.parse().context("invalid vessel uri")?, None, &token)
        .await
        .context("connect vessel client")?;

    let (sender, receiver) = mpsc::channel(1);

    tokio::spawn(async move {
        if let Err(e) = client.upload(ReceiverStream::new(receiver)).await {
            error!(error = error::chain(e), "Upload stream failed");
        }
    });

    sender
        .send(UploadRequest { chunk: header.into() })
        .await
        .context("send header")?;

    for collectable in &build.collectables {
        ensure!(
            collectable.kind() == collectable::Kind::Package,
            "Cannot upload collectable type {:?}",
            collectable.kind()
        );

        let path = asset_dir.join(&collectable.name);
        let mut stream = ReaderStream::new(File::open(&path).await.context("open collectable")?);

        while let Some(chunk) = stream.try_next().await.context("read file")? {
            sender
                .send(UploadRequest { chunk: chunk.into() })
                .await
                .context("send chunk")?;
        }

        debug!(
            name = %collectable.name,
            size = collectable.size,
            "Uploaded package"
        );
    }

    // Prune assets now that they're uploaded
    let _ = fs::remove_dir_all(&asset_dir)
        .await
        .inspect_err(|err| warn!(error = error::chain(err), "Failed to delete asset dir {asset_dir:?}"));

    info!(num_packages = build.collectables.len(), "Upload finished");

    Ok(())
}
