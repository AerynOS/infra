//! Grpc types

use std::collections::HashSet;

use futures_util::{Stream, StreamExt, stream::BoxStream};
use service_core::auth::{Flags, Permission, flag_names};
use service_grpc::vessel::UploadTokenRequest;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{error, warn};

use crate::error;

pub use service_grpc::*;

/// Verify the incoming request with the supplied [`auth::Flags`]
/// and if authorized, respond to the request with the supplied handler
pub async fn handle<T, U, E>(
    request: tonic::Request<T>,
    handler: impl AsyncFnOnce(tonic::Request<T>) -> Result<U, E>,
) -> Result<tonic::Response<U>, tonic::Status>
where
    E: std::error::Error + Into<tonic::Status>,
{
    verify_auth(&request)?;

    match handler(request).await {
        Ok(data) => Ok(tonic::Response::new(data)),
        Err(err) => {
            let error = error::chain(&err);
            error!(%error, "Handler error");

            Err(err.into())
        }
    }
}

/// Verify the incoming request with the supplied [`auth::Flags`]
/// and if authorized, respond to the request with the supplied handler
pub fn handle_server_streaming<S, T, U, E>(
    request: tonic::Request<T>,
    handler: impl FnOnce(tonic::Request<T>) -> S,
) -> Result<tonic::Response<BoxStream<'static, Result<U, tonic::Status>>>, tonic::Status>
where
    E: std::error::Error + Into<tonic::Status>,
    S: Stream<Item = Result<U, E>> + Send + 'static,
{
    verify_auth(&request)?;

    Ok(tonic::Response::new(
        handler(request)
            .map(|result| match result {
                Ok(data) => Ok(data),
                Err(err) => {
                    let error = error::chain(&err);
                    error!(%error, "Handler error");

                    Err(err.into())
                }
            })
            .boxed(),
    ))
}

/// Verify the incoming request with the supplied [`auth::Flags`]
/// and if authorized, establish a bi-directional stream that is handled
/// by handler
pub fn handle_streaming<T, U, E, F>(
    request: tonic::Request<tonic::Streaming<T>>,
    handler: impl FnOnce(tonic::Extensions, tonic::Streaming<T>, mpsc::Sender<U>) -> F + Send + Sync + 'static,
) -> Result<tonic::Response<ReceiverStream<Result<U, tonic::Status>>>, tonic::Status>
where
    T: 'static,
    U: Send + 'static,
    E: std::error::Error + Into<tonic::Status> + Send,
    F: Future<Output = Result<(), E>> + Send + 'static,
{
    verify_auth(&request)?;

    let extensions = request.extensions().clone();
    let stream = request.into_inner();

    let (inner_sender, mut inner_receiver) = mpsc::channel(1);
    let (sender, receiver) = mpsc::channel(1);

    tokio::spawn({
        let sender = sender.clone();
        async move {
            while let Some(inner) = inner_receiver.recv().await {
                let _ = sender.send(Ok(inner)).await;
            }
        }
    });
    tokio::spawn(async move {
        if let Err(err) = handler(extensions, stream, inner_sender).await {
            let error = error::chain(&err);
            error!(%error, "Handler error");

            let _ = sender.send(Err(err.into())).await;
        }
    });

    Ok(tonic::Response::new(ReceiverStream::new(receiver)))
}

fn verify_auth<T>(request: &tonic::Request<T>) -> Result<(), tonic::Status> {
    let Some(method) = request.extensions().get::<Method>() else {
        return Err(tonic::Status::unimplemented(""));
    };

    let required_flags = method.flags();
    let token_flags = request.extensions().get::<Flags>().copied().unwrap_or_default();

    let required_flag_names = flag_names(required_flags);
    let token_flag_names = flag_names(token_flags);

    let required_perm = method.permission();
    let token_perms = request
        .extensions()
        .get::<HashSet<Permission>>()
        .cloned()
        .unwrap_or_default();

    let has_required_perm = required_perm.is_none_or(|perm| token_perms.contains(&perm));

    // If token flags wholly contains all required flags &
    // user has the needed permission, then user is properly authorized
    if token_flags.contains(required_flags) && has_required_perm {
        Ok(())
    } else if token_flags == Flags::NO_AUTH {
        warn!(expected = ?required_flag_names, received = ?token_flag_names, "unauthenticated");
        Err(tonic::Status::unauthenticated(""))
    } else {
        warn!(
            expected = ?required_flag_names,
            received = ?token_flag_names,
            permissions = ?token_perms,
            "permission denied"
        );
        Err(tonic::Status::permission_denied(""))
    }
}

/// Hashes the [`UploadRequest`] for use in
/// deriving a single use token
pub fn upload_request_hash(request: &UploadTokenRequest) -> String {
    use sha2::{Digest, Sha256};

    let mut sha2 = Sha256::default();
    sha2.update(request.task_id.to_be_bytes());

    for collectable in &request.collectables {
        sha2.update(collectable.kind.to_be_bytes());
        sha2.update(collectable.sha256sum.as_bytes());
        sha2.update(collectable.size.to_be_bytes());
    }

    hex::encode(sha2.finalize())
}
