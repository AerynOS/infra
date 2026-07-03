use std::path::PathBuf;

use clap::Parser;
use service::{Server, Service, State, buildinfo};
use tracing::info;

use self::build::build;
use self::config::Config;
use self::upload::upload;

pub type Result<T, E = color_eyre::eyre::Error> = std::result::Result<T, E>;

mod build;
mod config;
mod stream;
mod upload;

#[tokio::main]
async fn main() -> Result<()> {
    let Args { config, root, version } = Args::parse();

    if version {
        println!("avalanche {}", buildinfo::full_version());
        return Ok(());
    }

    let config = Config::load(config.unwrap_or_else(|| root.join("config.toml"))).await?;

    service::tracing::init(&config.tracing);

    info!(
        version = buildinfo::version(),
        git_ref = buildinfo::git_sha(),
        git_dirty = buildinfo::git_dirty(),
        build_time = buildinfo::build_time(),
        "avalanche started"
    );

    let state = State::load(root).await?;

    Server::new(Service::Avalanche, &state, config.admin.clone())
        .with_task("stream", stream::run(state.clone(), config.clone()))
        .start()
        .await?;

    Ok(())
}

#[derive(Debug, Parser)]
struct Args {
    #[arg(long, short)]
    config: Option<PathBuf>,
    #[arg(long, short, default_value = ".")]
    root: PathBuf,
    /// Print version and exit
    #[arg(long)]
    version: bool,
}
