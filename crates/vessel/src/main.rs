use std::{net::IpAddr, path::PathBuf};

use clap::Parser;
use service::{Server, State, endpoint::Role};

pub type Result<T, E = color_eyre::eyre::Error> = std::result::Result<T, E>;
pub type Config = service::Config;

mod collection;
mod grpc;
mod worker;

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

    let state = State::load(root)
        .await?
        .with_migrations(sqlx::migrate!("./migrations"))
        .await?;

    let (worker_sender, worker_task) = worker::run(&state).await?;

    if let Some(directory) = import {
        let _ = worker_sender.send(worker::Message::ImportDirectory(directory));
    }

    Server::new(Role::RepositoryManager, &config, &state)
        .with_grpc((host, grpc_port))
        .merge_grpc(grpc::service(state.service_db.clone(), state.clone(), worker_sender))
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
