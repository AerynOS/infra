//! Batteries included server that provides common service APIs
//! over grpc, with the ability to handle additional consumer
//! defined http & grpc APIs
use std::{
    convert::Infallible,
    future::{self, IntoFuture},
    net::IpAddr,
    time::Duration,
};

use thiserror::Error;
use tracing::{error, info};

use crate::{
    State,
    account::{self, Admin},
    endpoint::{Role, enrollment},
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
    state: &'a State,
    role: Role,
    admin: Admin,
    extract_token: middleware::ExtractToken,
    signals: Vec<signal::Kind>,
    runner: task::Runner,
}

impl<'a> Server<'a> {
    /// Create a new [`Server`] with the configured [`Role`]
    pub fn new(role: Role, state: &'a State, admin: Admin) -> Self {
        let extract_token = middleware::ExtractToken {
            public_key: state.key_pair.public_key(),
            validation: token::Validation::new().iss(role.service_name()),
        };

        Self {
            http_router: axum::Router::new(),
            http_addr: None,
            grpc_router: tonic::service::Routes::default(),
            grpc_addr: None,
            role,
            state,
            admin,
            extract_token,
            signals: vec![signal::Kind::terminate(), signal::Kind::interrupt()],
            runner: task::Runner::new(),
        }
    }
}

impl Server<'_> {
    /// Enable http with the provided binding address & routes
    pub fn with_http(self, addr: (IpAddr, u16), router: axum::Router) -> Self {
        Self {
            http_addr: Some(addr),
            http_router: router,
            ..self
        }
    }

    /// Enable grpc with the provided binding address & routes
    ///
    /// Account service is always added, allowing authentication w/ the service
    pub fn with_grpc(self, addr: (IpAddr, u16), f: impl FnOnce(&mut tonic::service::RoutesBuilder)) -> Self {
        let mut routes = tonic::service::Routes::builder();

        f(&mut routes);

        Self {
            grpc_addr: Some(addr),
            grpc_router: routes.routes(),
            ..self
        }
    }

    /// Add task to auto-enroll
    pub fn with_auto_enroll(self, issuer: enrollment::Issuer, target: enrollment::Target) -> Self {
        debug_assert!(!matches!(self.role, Role::Hub));

        let db = self.state.service_db.clone();

        self.with_task("auto enroll", async move {
            if let Err(e) = enrollment::auto_enroll(&db, issuer, &target).await {
                error!(error = %error::chain(e), "Auto enrollment failed");
            }

            future::pending::<Result<(), Infallible>>().await
        })
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

    /// Start the server and perform the following:
    ///
    /// - Sync the defined [`Admin`] to the service [`Database`] to ensure
    ///   it's credentials can authenticate and hit all admin endpoints.
    /// - Start the underlying http & grpc servers, if configured & any configured tasks
    ///
    /// [`Database`]: crate::Database
    pub async fn start(self) -> Result<(), Error> {
        account::sync_admin(&self.state.service_db, &self.admin).await?;

        let mut runner = self.runner.with_task("signal capture", signal::capture(self.signals));

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
                .add_routes(self.grpc_router.add_service(account::service(
                    self.role,
                    self.state.service_db.clone(),
                    self.state.key_pair.clone(),
                )));

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
