//! Grpc types

use futures_util::{Stream, StreamExt, stream::BoxStream};
use service_core::auth::{Flags, flag_names};
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
pub fn handle_streaming<S, T, U, E>(
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

fn verify_auth<T>(request: &tonic::Request<T>) -> Result<(), tonic::Status> {
    let Some(method) = request.extensions().get::<Method>() else {
        return Err(tonic::Status::unimplemented(""));
    };

    let required_flags = method.flags();
    let token_flags = request.extensions().get::<Flags>().copied().unwrap_or_default();

    let required_flag_names = flag_names(required_flags);
    let token_flag_names = flag_names(token_flags);

    // If token flags wholly contains all required flags,
    // then user is properly authorized
    if token_flags.contains(required_flags) {
        Ok(())
    } else if token_flags == Flags::NO_AUTH {
        warn!(expected = ?required_flag_names, received = ?token_flag_names, "unauthenticated");
        Err(tonic::Status::unauthenticated(""))
    } else {
        warn!(expected = ?required_flag_names, received = ?token_flag_names, "permission denied");
        Err(tonic::Status::permission_denied(""))
    }
}
