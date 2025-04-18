//! Log the request and if applicable, error

use futures_util::{FutureExt, future::BoxFuture};
use tracing::{Instrument, debug, info_span};

/// Logging middleware which logs the request and if applicable, error
#[derive(Debug, Clone, Copy)]
pub struct Log;

impl<S> tower::Layer<S> for Log {
    type Service = Service<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Service { inner }
    }
}

/// Tower service of the [`Log`] layer
#[derive(Debug, Clone)]
pub struct Service<S> {
    inner: S,
}

impl<S, ReqBody, ResBody> tower::Service<http::Request<ReqBody>> for Service<S>
where
    S: tower::Service<http::Request<ReqBody>, Response = http::Response<ResBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    ReqBody: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
        tower::Service::poll_ready(&mut self.inner, cx)
    }

    fn call(&mut self, req: http::Request<ReqBody>) -> Self::Future {
        // This is necessary because tonic internally uses `tower::buffer::Buffer`.
        // See https://github.com/tower-rs/tower/issues/547#issuecomment-767629149
        // for details on why this is necessary
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);

        let path = req.uri().path().to_string();

        async move {
            debug!("Request received");

            match inner.call(req).await {
                Ok(resp) => {
                    let (parts, body) = resp.into_parts();

                    let resp = http::Response::from_parts(parts, body);

                    debug!(status = %resp.status(), "Sending response");

                    Ok(resp)
                }
                Err(e) => Err(e),
            }
        }
        .instrument(info_span!("request", path))
        .boxed()
    }
}
