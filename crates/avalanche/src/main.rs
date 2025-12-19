use std::path::PathBuf;

use clap::Parser;
use service::{Server, State, endpoint::Role};

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
    let Args { config, root } = Args::parse();

    let config = Config::load(config.unwrap_or_else(|| root.join("config.toml"))).await?;

    service::tracing::init(&config.tracing);

    let state = State::load(root).await?;
    let issuer = config.issuer(state.key_pair.clone());

    Server::new(Role::Builder, &state, config.admin.clone())
        .with_task("stream", stream::run(state.clone()))
        .with_auto_enroll(issuer, config.upstream)
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
}
