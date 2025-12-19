use std::{net::IpAddr, path::PathBuf};

use channel::DEFAULT_CHANNEL;
use clap::Parser;
use color_eyre::eyre::Context;
use service::{Server, endpoint::Role};

pub use self::config::Config;
pub use self::package::Package;
pub use self::state::State;

mod channel;
mod config;
mod grpc;
mod migration;
mod package;
mod state;
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
    } = Args::parse();

    let config = Config::load(config.unwrap_or_else(|| root.join("config.toml"))).await?;

    service::tracing::init(&config.tracing);

    let state = State::load(root).await.context("load state")?;
    let issuer = config.issuer(state.service.key_pair.clone());

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

    Server::new(Role::RepositoryManager, &state.service, config.admin.clone())
        .with_task("worker", worker_task)
        .with_auto_enroll(issuer, config.upstream)
        .with_grpc((host, grpc_port), |routes| {
            routes.add_service(grpc::service(state.service.clone(), worker_sender));
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
}

#[cfg(test)]
pub mod test {
    pub async fn database() -> service::Database {
        let db = service::Database::in_memory().await.unwrap();
        db.migrate(sqlx::migrate!("./migrations")).await.unwrap();
        db
    }
}
