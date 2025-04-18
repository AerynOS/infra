use std::sync::Arc;

use async_trait::async_trait;
use service::{
    endpoint,
    grpc::{
        self,
        summit::{
            BuildRequest, ImportRequest, RetryRequest,
            summit_service_server::{SummitService, SummitServiceServer},
        },
    },
    token::VerifiedToken,
};
use snafu::{ResultExt, Snafu};
use tracing::{info, warn};

use crate::worker;

pub type Server = SummitServiceServer<Service>;

pub fn service(worker: worker::Sender) -> Server {
    Server::new(Service {
        state: Arc::new(State { worker }),
    })
}

#[derive(Clone)]
pub struct Service {
    state: Arc<State>,
}

#[derive(Clone)]
struct State {
    worker: worker::Sender,
}

#[async_trait]
impl SummitService for Service {
    async fn build_succeeded(
        &self,
        request: tonic::Request<BuildRequest>,
    ) -> Result<tonic::Response<()>, tonic::Status> {
        let state = self.state.clone();

        grpc::handle(request, async move |request| build_succeeded(state, request).await).await
    }

    async fn build_failed(&self, request: tonic::Request<BuildRequest>) -> Result<tonic::Response<()>, tonic::Status> {
        let state = self.state.clone();

        grpc::handle(request, async move |request| build_failed(state, request).await).await
    }

    async fn import_succeeded(
        &self,
        request: tonic::Request<ImportRequest>,
    ) -> Result<tonic::Response<()>, tonic::Status> {
        let state = self.state.clone();

        grpc::handle(request, async move |request| import_succeeded(state, request).await).await
    }

    async fn import_failed(
        &self,
        request: tonic::Request<ImportRequest>,
    ) -> Result<tonic::Response<()>, tonic::Status> {
        let state = self.state.clone();

        grpc::handle(request, async move |request| import_failed(state, request).await).await
    }

    async fn refresh(&self, request: tonic::Request<()>) -> Result<tonic::Response<()>, tonic::Status> {
        let state = self.state.clone();

        grpc::handle(request, async move |request| refresh(state, request).await).await
    }

    async fn retry(&self, request: tonic::Request<RetryRequest>) -> Result<tonic::Response<()>, tonic::Status> {
        let state = self.state.clone();

        grpc::handle(request, async move |request| retry(state, request).await).await
    }
}

#[tracing::instrument(
    skip_all,
    fields(
        task_id = %request.get_ref().task_id,
    )
)]
async fn build_succeeded(state: Arc<State>, request: tonic::Request<BuildRequest>) -> Result<(), Error> {
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

    info!(
        endpoint = %endpoint_id,
        "Build succeeded"
    );

    let build = request.into_inner();

    let _ = state.worker.send(worker::Message::BuildSucceeded {
        task_id: (build.task_id as i64).into(),
        builder: endpoint_id,
        collectables: build.collectables,
    });

    Ok(())
}

#[tracing::instrument(
    skip_all,
    fields(
        task_id = %request.get_ref().task_id,
    )
)]
async fn build_failed(state: Arc<State>, request: tonic::Request<BuildRequest>) -> Result<(), Error> {
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

    warn!(
        endpoint = %endpoint_id,
        "Build failed"
    );

    let build = request.into_inner();

    let _ = state.worker.send(worker::Message::BuildFailed {
        task_id: (build.task_id as i64).into(),
        builder: endpoint_id,
        collectables: build.collectables,
    });

    Ok(())
}

#[tracing::instrument(
    skip_all,
    fields(
        task_id = %request.get_ref().task_id,
    )
)]
async fn import_succeeded(state: Arc<State>, request: tonic::Request<ImportRequest>) -> Result<(), Error> {
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

    info!(
        endpoint = %endpoint_id,
        "Import succeeded"
    );

    let _ = state.worker.send(worker::Message::ImportSucceeded {
        task_id: (request.into_inner().task_id as i64).into(),
    });

    Ok(())
}

#[tracing::instrument(
    skip_all,
    fields(
        task_id = %request.get_ref().task_id,
    )
)]
async fn import_failed(state: Arc<State>, request: tonic::Request<ImportRequest>) -> Result<(), Error> {
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

    warn!(
        endpoint = %endpoint_id,
        "Import failed"
    );

    let _ = state.worker.send(worker::Message::ImportFailed {
        task_id: (request.into_inner().task_id as i64).into(),
    });

    Ok(())
}

#[tracing::instrument(skip_all)]
async fn refresh(state: Arc<State>, request: tonic::Request<()>) -> Result<(), Error> {
    let token = request
        .extensions()
        .get::<VerifiedToken>()
        .cloned()
        .ok_or(Error::MissingRequestToken)?;

    let account_id = token.decoded.payload.account_id;

    info!(
        account = %account_id,
        "Refresh"
    );

    let _ = state.worker.send(worker::Message::Refresh);

    Ok(())
}

#[tracing::instrument(
    skip_all,
    fields(
        task_id = %request.get_ref().task_id,
    )
)]
async fn retry(state: Arc<State>, request: tonic::Request<RetryRequest>) -> Result<(), Error> {
    let token = request
        .extensions()
        .get::<VerifiedToken>()
        .cloned()
        .ok_or(Error::MissingRequestToken)?;

    let account_id = token.decoded.payload.account_id;

    info!(
        account = %account_id,
        "Retry"
    );

    let _ = state.worker.send(worker::Message::Retry {
        task_id: (request.into_inner().task_id as i64).into(),
    });

    Ok(())
}

#[derive(Debug, Snafu)]
enum Error {
    #[snafu(display("Token missing from request"))]
    MissingRequestToken,
    #[snafu(display("Invalid endpoint"))]
    InvalidEndpoint { source: uuid::Error },
}

impl From<Error> for tonic::Status {
    fn from(error: Error) -> Self {
        match error {
            Error::MissingRequestToken => tonic::Status::unauthenticated(""),
            Error::InvalidEndpoint { .. } => tonic::Status::invalid_argument(""),
        }
    }
}
