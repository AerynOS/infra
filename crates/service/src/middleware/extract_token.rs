//! Parse the authorization token from incoming requests, validate it and provide
//! the verified token & flags as extensions to downstream middleware / handlers

use tracing::{debug, warn};

use crate::{
    Token, account,
    auth::{Flags, flag_names},
    crypto::PublicKey,
    token::{self, Validation, VerifiedToken},
};

/// Middleware to extract auth token and decorate request with [`Flags`],
/// allowing downstream handlers to assess permissions.
///
/// If an auth token is on the request and verified using [`Validation`],
/// [`VerifiedToken`] will be added as an extension.
#[derive(Debug, Clone)]
pub struct ExtractToken {
    /// Public key used to verify the [`Token`] signature
    pub pub_key: PublicKey,
    /// Validation rules used when calling [`Token::verify`]
    pub validation: Validation,
}

impl<S> tower::Layer<S> for ExtractToken {
    type Service = Service<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Service {
            inner,
            pub_key: self.pub_key,
            validation: self.validation.clone(),
        }
    }
}

/// Tower service of the [`ExtractToken`] layer
#[derive(Debug, Clone)]
pub struct Service<S> {
    inner: S,
    pub_key: PublicKey,
    validation: Validation,
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

        let token_maybe = extract_token(&req, &self.pub_key, &self.validation);

        let mut flags = Flags::default();

        if let Some(token) = token_maybe {
            req.extensions_mut().insert(token.clone());

            match token.decoded.payload.purpose {
                token::Purpose::Authorization => flags |= Flags::BEARER_TOKEN,
                token::Purpose::Authentication => flags |= Flags::ACCESS_TOKEN,
            }

            match token.decoded.payload.account_type {
                account::Kind::Admin => flags |= Flags::ADMIN_ACCOUNT,
                account::Kind::Standard => flags |= Flags::USER_ACCOUNT,
                account::Kind::Bot => flags |= Flags::BOT_ACCOUNT,
                account::Kind::Service => flags |= Flags::SERVICE_ACCOUNT,
            }

            if token.decoded.is_expired() {
                flags |= Flags::EXPIRED
            } else {
                flags |= Flags::NOT_EXPIRED
            }

            let token_flags = flag_names(flags);
            let token_purpose = Some(token.decoded.payload.purpose.to_string());
            let account = Some(token.decoded.payload.account_id.to_string());
            let account_type = Some(token.decoded.payload.account_type.to_string());

            debug!(?token_flags, token_purpose, account, account_type, "Auth parsed");
        }

        req.extensions_mut().insert(flags);

        inner.call(req)
    }
}

fn extract_token<Body>(
    req: &http::Request<Body>,
    pub_key: &PublicKey,
    validation: &Validation,
) -> Option<VerifiedToken> {
    let header = req.headers().get("authorization")?;
    let token_str = header.to_str().ok()?.strip_prefix("Bearer ")?;

    match Token::verify(token_str, pub_key, validation) {
        Ok(token) => Some(token),
        Err(error) => {
            warn!(%error, "Invalid authorization token");
            None
        }
    }
}
