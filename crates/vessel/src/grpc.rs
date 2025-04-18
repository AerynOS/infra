use std::sync::Arc;

use async_trait::async_trait;
use service::{
    Database, Endpoint, database, endpoint,
    grpc::{
        self, collectable,
        vessel::{
            ImportRequest,
            vessel_service_server::{VesselService, VesselServiceServer},
        },
    },
    token::VerifiedToken,
};
use snafu::{ResultExt, Snafu};
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::worker;

pub type Server = VesselServiceServer<Service>;

pub fn service(db: Database, worker: worker::Sender) -> Server {
    Server::new(Service {
        state: Arc::new(State { db, worker }),
    })
}

#[derive(Clone)]
pub struct Service {
    state: Arc<State>,
}

#[derive(Clone)]
struct State {
    db: Database,
    worker: worker::Sender,
}

#[async_trait]
impl VesselService for Service {
    async fn import(
        &self,
        request: tonic::Request<ImportRequest>,
    ) -> std::result::Result<tonic::Response<()>, tonic::Status> {
        let state = self.state.clone();

        grpc::handle(request, async move |request| import_packages(state, request).await).await
    }
}

#[tracing::instrument(
    skip_all,
    fields(
        task_id = %request.get_ref().task_id,
        num_collectables = request.get_ref().collectables.len()
    )
)]
async fn import_packages(state: Arc<State>, request: tonic::Request<ImportRequest>) -> Result<(), Error> {
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
    let endpoint = Endpoint::get(state.db.acquire().await.context(DatabaseSnafu)?.as_mut(), endpoint_id)
        .await
        .context(LoadEndpointSnafu)?;

    let body = request.into_inner();

    let packages = body
        .collectables
        .into_iter()
        .filter_map(|c| {
            matches!(c.kind(), collectable::Kind::Package).then_some(c.uri.parse().map(|url| worker::Package {
                url,
                sha256sum: c.sha256sum,
            }))
        })
        .collect::<Result<Vec<_>, _>>()
        .context(InvalidUrlSnafu)?;

    if packages.is_empty() {
        warn!(endpoint = %endpoint.id, "No packages to import");
        return Ok(());
    }

    info!(
        endpoint = %endpoint.id,
        num_packages = packages.len(),
        "Import packages"
    );

    state
        .worker
        .send(worker::Message::ImportPackages {
            task_id: body.task_id,
            endpoint,
            packages,
        })
        .context(SendWorkerSnafu)?;

    Ok(())
}

#[derive(Debug, Snafu)]
enum Error {
    #[snafu(display("Token missing from request"))]
    MissingRequestToken,
    #[snafu(display("Invalid endpoint"))]
    InvalidEndpoint { source: uuid::Error },
    #[snafu(display("Invalid url"))]
    InvalidUrl { source: url::ParseError },
    #[snafu(display("Failed to load endpoint"))]
    LoadEndpoint { source: database::Error },
    #[snafu(display("Failed to send task to worker"))]
    SendWorker {
        source: mpsc::error::SendError<worker::Message>,
    },
    #[snafu(display("Database error"))]
    Database { source: database::Error },
}

impl From<Error> for tonic::Status {
    fn from(error: Error) -> Self {
        match error {
            Error::MissingRequestToken => tonic::Status::unauthenticated(""),
            Error::InvalidEndpoint { .. } | Error::InvalidUrl { .. } => tonic::Status::invalid_argument(""),
            Error::LoadEndpoint { .. } | Error::SendWorker { .. } | Error::Database { .. } => {
                tonic::Status::internal("")
            }
        }
    }
}
