use std::{net::IpAddr, path::PathBuf};

use channel::DEFAULT_CHANNEL;
use clap::Parser;
use color_eyre::eyre::Context;
use service::{Server, Service, buildinfo, error};
use tracing::{error, info};

pub use self::config::Config;
pub use self::package::Package;
pub use self::state::State;

mod channel;
mod config;
mod grpc;
mod migration;
mod package;
mod state;
mod stream;
mod upload;
mod worker;

pub type Result<T, E = color_eyre::eyre::Error> = std::result::Result<T, E>;

#[tokio::main]
async fn main() -> Result<()> {
    let Args {
        host,
        grpc_port,
        config,
        root,
        import,
        version,
    } = Args::parse();

    if version {
        println!("vessel {}", buildinfo::get_full_version());
        return Ok(());
    }

    let config = Config::load(config.unwrap_or_else(|| root.join("config.toml"))).await?;

    service::tracing::init(&config.tracing);

    info!(
        version = buildinfo::get_version(),
        git_ref = buildinfo::get_git_full_hash(),
        git_dirty = !buildinfo::get_git_dirty().is_empty(),
        build_time = buildinfo::get_build_time(),
        "vessel started"
    );

    let state = State::load(root).await.context("load state")?;

    migration::run_all(&state).await.context("run migrations")?;

    if let Some(directory) = import
        && let Err(err) = channel::import_directory(&state, DEFAULT_CHANNEL, directory).await
    {
        error!(error = error::chain(&*err), "Failed to import directory");
    }

    channel::reindex_latest(&state, DEFAULT_CHANNEL)
        .await
        .context("reindex")?;

    let (worker_sender, worker_events, worker_task) = worker::run(state.clone()).await?;

    Server::new(Service::Vessel, &state.service, config.admin.clone())
        .with_task("worker", worker_task)
        .with_task("stream", stream::run(state.clone(), config.clone(), worker_events))
        .with_grpc((host, grpc_port), |routes| {
            routes.add_service(grpc::vessel_service(state.service.clone(), worker_sender));
        })
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
    /// Print version and exit
    #[arg(long)]
    version: bool,
}

#[cfg(test)]
pub mod test {
    pub async fn database() -> service::Database {
        let db = service::Database::in_memory().await.unwrap();
        db.migrate(sqlx::migrate!("./migrations")).await.unwrap();
        db
    }

    #[tokio::test]
    async fn test_migrations() {
        database().await;
    }
}
