use std::{net::IpAddr, path::PathBuf};

use clap::Parser;
use service::{Server, State, endpoint::Role};

pub type Result<T, E = color_eyre::eyre::Error> = std::result::Result<T, E>;
pub type Config = service::Config;

use self::build::build;

mod build;
mod grpc;

#[tokio::main]
async fn main() -> Result<()> {
    let Args {
        host,
        http_port,
        grpc_port,
        config,
        root,
    } = Args::parse();

    let config = Config::load(config.unwrap_or_else(|| root.join("config.toml"))).await?;

    service::tracing::init(&config.tracing);

    let state = State::load(root).await?;

    Server::new(Role::Builder, &config, &state)
        .with_grpc((host, grpc_port))
        .merge_grpc(grpc::service(state.clone(), config.clone()))
        .with_http((host, http_port))
        .merge_http(axum::Router::new().nest_service(
            "/assets",
            tower_http::services::ServeDir::new("assets").precompressed_gzip(),
        ))
        .start()
        .await?;

    Ok(())
}

#[derive(Debug, Parser)]
struct Args {
    #[arg(default_value = "127.0.0.1")]
    host: IpAddr,
    #[arg(long = "http", default_value = "5000")]
    http_port: u16,
    #[arg(long = "grpc", default_value = "5001")]
    grpc_port: u16,
    #[arg(long, short)]
    config: Option<PathBuf>,
    #[arg(long, short, default_value = ".")]
    root: PathBuf,
}
