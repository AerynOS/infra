//! Active session lookup middleware

use service_core::{
    auth,
    session::ActiveSessions,
    token::{self, VerifiedToken},
};

/// Checks the incoming session to see if it is still active and
/// if so, inserts it as a request extension.
///
/// Must be layered after the [`ExtractToken`] layer so session
/// can be identified from the valid token.
///
/// [`ExtractToken`]: super::ExtractToken
#[derive(Debug, Clone)]
pub struct ExtractActiveSession {
    /// In-memory map of active sessions
    pub active_sessions: ActiveSessions,
}

impl<S> tower::Layer<S> for ExtractActiveSession {
    type Service = Service<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Service {
            inner,
            active_sessions: self.active_sessions.clone(),
        }
    }
}

/// Tower service of the [`ExtractActiveSession`] layer
#[derive(Debug, Clone)]
pub struct Service<S> {
    inner: S,
    active_sessions: ActiveSessions,
}

impl<S, ReqBody, ResBody> tower::Service<http::Request<ReqBody>> for Service<S>
where
    S: tower::Service<http::Request<ReqBody>, Response = http::Response<ResBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
        tower::Service::poll_ready(&mut self.inner, cx)
    }

    fn call(&mut self, mut req: http::Request<ReqBody>) -> Self::Future {
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);

        if let Some(token) = req.extensions().get::<VerifiedToken>()
            && let token::Kind::Session { session_id } = token.decoded.payload.kind
            && let Some(session) = self.active_sessions.get(&session_id)
        {
            // TODO: Add background job to clean these up automatically
            if session.is_expired() {
                self.active_sessions.revoke(&session.id);
            } else {
                if let Some(flags) = req.extensions_mut().get_mut::<auth::Flags>() {
                    *flags |= auth::Flags::VALID_SESSION;
                }

                req.extensions_mut().insert(session);
            }
        };

        inner.call(req)
    }
}
