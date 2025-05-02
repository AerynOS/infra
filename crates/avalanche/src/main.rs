use std::path::PathBuf;

use clap::Parser;
use service::{Server, State, endpoint::Role};

pub type Result<T, E = color_eyre::eyre::Error> = std::result::Result<T, E>;
pub type Config = service::Config;

use self::build::build;
use self::upload::upload;

mod build;
mod stream;
mod upload;

#[tokio::main]
async fn main() -> Result<()> {
    let Args { config, root } = Args::parse();

    let config = Config::load(config.unwrap_or_else(|| root.join("config.toml"))).await?;

    service::tracing::init(&config.tracing);

    let state = State::load(root).await?;

    Server::new(Role::Builder, &config, &state)
        .with_task("stream", stream::run(state.clone()))
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
