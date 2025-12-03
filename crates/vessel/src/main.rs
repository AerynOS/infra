use std::{net::IpAddr, path::PathBuf};

use channel::DEFAULT_CHANNEL;
use clap::Parser;
use color_eyre::eyre::Context;
use service::{Server, endpoint::Role};

pub use self::package::Package;
pub use self::state::State;

mod channel;
mod grpc;
mod migration;
mod package;
mod state;
mod worker;

pub type Result<T, E = color_eyre::eyre::Error> = std::result::Result<T, E>;
pub type Config = service::Config;

#[tokio::main]
async fn main() -> Result<()> {
    let Args {
        host,
        grpc_port,
        config,
        root,
        import,
    } = Args::parse();

    let config = Config::load(config.unwrap_or_else(|| root.join("config.toml"))).await?;

    service::tracing::init(&config.tracing);

    let state = State::load(root).await.context("load state")?;

    migration::run_all(&state).await.context("run migrations")?;

    if let Some(directory) = import {
        channel::import_directory(&state, DEFAULT_CHANNEL, directory)
            .await
            .context("import")?;
    } else {
        channel::reindex_latest(&state, DEFAULT_CHANNEL)
            .await
            .context("reindex")?;
    }

    let (worker_sender, worker_task) = worker::run(state.clone()).await?;

    Server::new(Role::RepositoryManager, &config, &state.service)
        .with_grpc((host, grpc_port))
        .merge_grpc(grpc::service(state.service.clone(), worker_sender))
        .with_task("worker", worker_task)
        .start()
        .await?;

    Ok(())
}

#[derive(Debug, Parser)]
struct Args {
    #[arg(default_value = "127.0.0.1")]
    host: IpAddr,
    #[arg(long = "grpc", default_value = "5001")]
    grpc_port: u16,
    #[arg(long, short)]
    config: Option<PathBuf>,
    #[arg(long, short, default_value = ".")]
    root: PathBuf,
    #[arg(long)]
    import: Option<PathBuf>,
}
