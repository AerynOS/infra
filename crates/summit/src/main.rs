use std::path::Path;
use std::{net::IpAddr, path::PathBuf};

use axum::{
    extract::Request,
    middleware::{self, Next},
    routing::get,
};
use clap::Parser;
use color_eyre::eyre::Context;
use service::{Server, State, endpoint::Role};
use tower_http::{services::ServeDir, set_header::SetResponseHeader};

pub use self::builder::Builder;
pub use self::manager::Manager;
pub use self::profile::Profile;
pub use self::project::Project;
pub use self::queue::Queue;
pub use self::repository::Repository;
pub use self::seed::seed;
pub use self::task::Task;

pub type Result<T, E = color_eyre::eyre::Error> = std::result::Result<T, E>;
pub type Config = service::Config;

mod builder;
mod grpc;
mod manager;
mod profile;
mod project;
mod queue;
mod repository;
mod route;
mod seed;
mod task;
mod template;
mod worker;

tokio::task_local! {
    static USE_MOCK_DATA: bool;
}

fn use_mock_data() -> bool {
    USE_MOCK_DATA.try_with(|b| *b).unwrap_or(false)
}

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
        use_mock_data,
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
    let serve_logs = SetResponseHeader::overriding(
        ServeDir::new(state.state_dir.join("logs")).precompressed_gzip(),
        http::header::CONTENT_TYPE,
        const { http::HeaderValue::from_static("text/plain; charset=utf-8") },
    );

    Server::new(Role::Hub, &config, &state)
        .with_http((host, http_port))
        .merge_http(
            axum::Router::new()
                .route("/", get(route::index))
                .route("/tasks", get(route::tasks))
                .route_layer(middleware::from_fn(move |request: Request, next: Next| {
                    USE_MOCK_DATA.scope(use_mock_data, next.run(request))
                }))
                .nest_service("/static", serve_static)
                .nest_service("/logs", serve_logs)
                .fallback(get(route::fallback))
                .with_state(route::state(state.clone(), worker_sender.clone())),
        )
        .with_grpc((host, grpc_port))
        .merge_grpc(grpc::service(state.clone(), worker_sender.clone()))
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
    #[arg(long)]
    use_mock_data: bool,
}
