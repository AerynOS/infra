use std::sync::{
    Arc,
    atomic::{self, AtomicBool},
};

use async_trait::async_trait;
use service::{Endpoint, database, endpoint, token::VerifiedToken};
use snafu::{ResultExt, Snafu, ensure};
use tracing::info;

use crate::Config;

static BUILD_IN_PROGRESS: AtomicBool = AtomicBool::new(false);

pub type Server = AvalancheServiceServer<Service>;

pub fn service(state: service::State, config: service::Config) -> Server {
    Server::new(Service {
        state: Arc::new(State { service: state, config }),
    })
}

#[derive(Clone)]
pub struct Service {
    state: Arc<State>,
}

#[derive(Clone)]
struct State {
    service: service::State,
    config: Config,
}

#[async_trait]
impl AvalancheService for Service {
    async fn build(&self, request: tonic::Request<BuildRequest>) -> Result<tonic::Response<()>, tonic::Status> {
        let state = self.state.clone();

        grpc::handle(request, async move |request| build(state, request).await).await
    }
}

#[tracing::instrument(
    skip_all,
    fields(
        task_id = %request.get_ref().task_id,
    )
)]
async fn build(state: Arc<State>, request: tonic::Request<BuildRequest>) -> Result<(), Error> {
    let token = request
        .extensions()
        .get::<VerifiedToken>()
        .cloned()
        .ok_or(Error::MissingRequestToken)?;

    let endpoint_id = token
        .decoded
        .payload
        .sub
        .parse::<endpoint::Id>()
        .context(InvalidEndpointSnafu)?;
    let endpoint = Endpoint::get(
        state
            .service
            .service_db
            .acquire()
            .await
            .context(DatabaseSnafu)?
            .as_mut(),
        endpoint_id,
    )
    .await
    .context(LoadEndpointSnafu)?;

    let build = request.into_inner();

    ensure!(!build.remotes.is_empty(), MissingRemotesSnafu);

    info!(
        endpoint = %endpoint.id,
        "Build request received"
    );

    // Atomically guarantee another build isn't in progress
    if BUILD_IN_PROGRESS
        .compare_exchange(false, true, atomic::Ordering::SeqCst, atomic::Ordering::Relaxed)
        .is_err()
    {
        return Err(Error::BuildInProgress);
    }

    // Build time!
    tokio::spawn(async move {
        crate::build(build, endpoint, state.service.clone(), state.config.clone()).await;
        BUILD_IN_PROGRESS.store(false, atomic::Ordering::Relaxed);
    });

    Ok(())
}

#[derive(Debug, Snafu)]
enum Error {
    #[snafu(display("Token missing from request"))]
    MissingRequestToken,
    #[snafu(display("Missing remotes"))]
    MissingRemotes,
    #[snafu(display("Another build is already in progress"))]
    BuildInProgress,
    #[snafu(display("Invalid endpoint"))]
    InvalidEndpoint { source: uuid::Error },
    #[snafu(display("Failed to load endpoint"))]
    LoadEndpoint { source: database::Error },
    #[snafu(display("Database error"))]
    Database { source: database::Error },
}

impl From<Error> for tonic::Status {
    fn from(error: Error) -> Self {
        match error {
            Error::MissingRequestToken => tonic::Status::unauthenticated(""),
            Error::MissingRemotes | Error::InvalidEndpoint { .. } => tonic::Status::invalid_argument(""),
            Error::LoadEndpoint { .. } | Error::Database { .. } => tonic::Status::internal(""),
            Error::BuildInProgress => tonic::Status::resource_exhausted(""),
        }
    }
}
