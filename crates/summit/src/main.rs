use std::path::Path;
use std::{net::IpAddr, path::PathBuf};

use axum::routing::get;
use clap::Parser;
use color_eyre::eyre::Context;
use service::{Server, State, endpoint::Role};
use tower_http::services::ServeDir;

pub use self::manager::Manager;
pub use self::profile::Profile;
pub use self::project::Project;
pub use self::queue::Queue;
pub use self::repository::Repository;
pub use self::seed::seed;
pub use self::task::Task;

pub type Result<T, E = color_eyre::eyre::Error> = std::result::Result<T, E>;
pub type Config = service::Config;

mod grpc;
mod manager;
mod profile;
mod project;
mod queue;
mod repository;
mod routes;
mod seed;
mod task;
mod templates;
mod worker;

#[tokio::main]
async fn main() -> Result<()> {
    let Args {
        host,
        http_port,
        grpc_port,
        config,
        root,
        seed_from,
        static_dir,
    } = Args::parse();

    let config = Config::load(config.unwrap_or_else(|| root.join("config.toml"))).await?;

    service::tracing::init(&config.tracing);

    let state = State::load(root)
        .await?
        .with_migrations(sqlx::migrate!("./migrations"))
        .await?;

    if let Some(from_path) = seed_from {
        seed(&state, from_path).await.context("seeding")?;
    }

    let manager = Manager::load(state.clone()).await.context("load manager")?;

    let (worker_sender, worker_task) = worker::run(manager).await?;

    let serve_static = ServeDir::new(static_dir.as_deref().unwrap_or(Path::new("static")));

    Server::new(Role::Hub, &config, &state)
        .with_http((host, http_port))
        .merge_http(
            axum::Router::new()
                .route("/", get(routes::index))
                .route("/tasks", get(routes::tasks))
                .nest_service("/static", serve_static)
                .fallback(get(routes::fallback))
                .with_state(state.clone()),
        )
        .with_grpc((host, grpc_port))
        .merge_grpc(grpc::service(worker_sender.clone()))
        .with_task("worker", worker_task)
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
    #[arg(long = "seed")]
    seed_from: Option<PathBuf>,
    #[arg(long = "static")]
    static_dir: Option<PathBuf>,
}
