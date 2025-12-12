//! Batteries included server that provides common service APIs
//! over grpc, with the ability to handle additional consumer
//! defined http & grpc APIs
use std::{
    convert::Infallible,
    future::{self, IntoFuture},
    net::IpAddr,
    time::Duration,
};

use axum::response::IntoResponse;
use thiserror::Error;
use tonic::server::NamedService;
use tracing::{error, info};

use crate::{
    Config, State, account,
    endpoint::{self, Role, enrollment},
    error, middleware, signal, task, token,
};

pub use crate::task::CancellationToken;

/// Batteries included server which provides http & grpc serving w/ common
/// middlewares and the ability to spawn & manage tasks.
pub struct Server<'a> {
    http_router: axum::Router,
    http_addr: Option<(IpAddr, u16)>,
    grpc_router: tonic::service::Routes,
    grpc_addr: Option<(IpAddr, u16)>,
    config: &'a Config,
    state: &'a State,
    role: Role,
    extract_token: middleware::ExtractToken,
    signals: Vec<signal::Kind>,
    runner: task::Runner,
}

impl<'a> Server<'a> {
    /// Create a new [`Server`]
    pub fn new(role: Role, config: &'a Config, state: &'a State) -> Self {
        let http_router = axum::Router::new();

        let mut grpc_router = tonic::service::Routes::default().add_service(account::service(role, state));

        if matches!(role, Role::Hub) {
            grpc_router = grpc_router.add_service(endpoint::service(role, config, state));
        }

        Self {
            http_router,
            http_addr: None,
            grpc_router,
            grpc_addr: None,
            config,
            state,
            role,
            extract_token: middleware::ExtractToken {
                pub_key: state.key_pair.public_key(),
                validation: token::Validation::new().iss(role.service_name()),
            },
            signals: vec![signal::Kind::terminate(), signal::Kind::interrupt()],
            runner: task::Runner::new(),
        }
    }
}

impl Server<'_> {
    /// Enable & specify the http binding address
    pub fn with_http(self, addr: (IpAddr, u16)) -> Self {
        Self {
            http_addr: Some(addr),
            ..self
        }
    }

    /// Enable & specify the grpc binding address
    pub fn with_grpc(self, addr: (IpAddr, u16)) -> Self {
        Self {
            grpc_addr: Some(addr),
            ..self
        }
    }

    /// Override the default graceful shutdown duration (5s)
    pub fn with_graceful_shutdown(self, duration: Duration) -> Self {
        Self {
            runner: self.runner.with_graceful_shutdown(duration),
            ..self
        }
    }

    /// Add a task which is killed immediately upon shutdown sequence
    pub fn with_task<F, E>(self, name: &'static str, task: F) -> Self
    where
        F: IntoFuture<Output = Result<(), E>>,
        F::IntoFuture: Send + 'static,
        E: std::error::Error + Send + 'static,
    {
        Self {
            runner: self.runner.with_task(name, task),
            ..self
        }
    }

    /// Add a task which can monitor shutdown sequence via [`CancellationToken`].
    /// The task is given graceful shutdown duration to cleanup & exit before being
    /// forcefully killed.
    pub fn with_cancellation_task<F, E>(self, name: &'static str, f: impl FnOnce(CancellationToken) -> F) -> Self
    where
        F: IntoFuture<Output = Result<(), E>>,
        F::IntoFuture: Send + 'static,
        E: std::error::Error + Send + 'static,
    {
        Self {
            runner: self.runner.with_cancellation_task(name, f),
            ..self
        }
    }

    /// Merges a tonic grpc service with the server
    pub fn merge_grpc<S>(self, service: S) -> Self
    where
        S: tower::Service<http::Request<tonic::body::Body>, Error = Infallible>
            + NamedService
            + Clone
            + Send
            + Sync
            + 'static,
        S::Response: IntoResponse,
        S::Future: Send + 'static,
    {
        Self {
            grpc_router: self.grpc_router.add_service(service),
            ..self
        }
    }

    /// Merges an http [`axum::Router`] with the server
    pub fn merge_http(self, router: impl Into<axum::Router>) -> Self {
        Self {
            http_router: self.http_router.merge(router),
            ..self
        }
    }

    /// Start the server and perform the following:
    ///
    /// - Sync the defined [`Config::admin`] to the service [`Database`] to ensure
    ///   it's credentials can authenticate and hit all admin endpoints.
    /// - Start the underlying http & grpc servers, if configured & any configured tasks
    /// - Send auto-enrollment for [`Config::upstream`] target defined when not [`Role::Hub`]
    ///
    /// [`Database`]: crate::Database
    pub async fn start(self) -> Result<(), Error> {
        account::sync_admin(&self.state.service_db, self.config.admin.clone()).await?;

        let mut runner = self.runner.with_task("signal capture", signal::capture(self.signals));

        if self.role != Role::Hub {
            runner = runner.with_task(
                "auto enroll",
                auto_enroll(self.role, self.config.clone(), self.state.clone()),
            );
        }

        if let Some(addr) = self.http_addr {
            let router = self
                .http_router
                .layer(self.extract_token.clone())
                .layer(middleware::Log);

            runner = runner.with_task("http server", async move {
                let (host, port) = addr;

                let listener = tokio::net::TcpListener::bind(addr).await?;

                info!("listening http on {host}:{port}");

                axum::serve(listener, router).await
            });
        }

        if let Some(addr) = self.grpc_addr {
            let server = tonic::transport::Server::builder()
                // https://grpc.io/docs/guides/keepalive/#keepalive-configuration-specification
                .http2_keepalive_interval(Some(Duration::from_secs(60 * 60 * 2)))
                .http2_keepalive_timeout(Some(Duration::from_secs(20)))
                .layer(middleware::Log)
                .layer(middleware::GrpcMethod)
                .layer(self.extract_token)
                .add_routes(self.grpc_router);

            runner = runner.with_task("grpc server", async move {
                let (host, port) = addr;

                info!("listening grpc on {host}:{port}");

                server.serve(addr.into()).await
            });
        }

        runner.run().await;

        Ok(())
    }
}

/// A server error
#[derive(Debug, Error)]
pub enum Error {
    /// Syncing admin account failed
    #[error("sync admin account")]
    SyncAdmin(#[from] account::Error),
}

async fn auto_enroll(role: Role, config: Config, state: State) -> Result<(), Infallible> {
    if role != Role::Hub
        && let Some(target) = &config.upstream
        && let Err(e) = enrollment::auto_enroll(target, config.issuer(role, state.key_pair.clone()), &state).await
    {
        error!(error = %error::chain(e), "Auto enrollment failed");
    }

    future::pending::<Result<(), Infallible>>().await
}
