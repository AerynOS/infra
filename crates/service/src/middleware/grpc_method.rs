//! Adds [`grpc::Method`] as an extension

use crate::grpc;

/// Adds [`grpc::Method`] as an extension
#[derive(Debug, Clone, Copy)]
pub struct GrpcMethod;

impl<S> tower::Layer<S> for GrpcMethod {
    type Service = Service<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Service { inner }
    }
}

/// Tower service of the [`GrpcMethod`] layer
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
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
        tower::Service::poll_ready(&mut self.inner, cx)
    }

    fn call(&mut self, mut req: http::Request<ReqBody>) -> Self::Future {
        if let Some(method) = grpc::Method::from_path(req.uri().path()) {
            req.extensions_mut().insert(method);
        }
        self.inner.call(req)
    }
}
