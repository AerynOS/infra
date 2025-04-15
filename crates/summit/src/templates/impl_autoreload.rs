use std::{
    sync::Arc,
    task::{Context, Poll},
};

use camino::Utf8Path;
use http::StatusCode;
use minijinja::path_loader;
use minijinja_autoreload::AutoReloader;
use tokio::task::futures::TaskLocalFuture;

use super::TemplateResponse;

tokio::task_local! {
    static AUTO_RELOADER: Arc<minijinja_autoreload::AutoReloader>;
}

#[derive(Clone)]
pub struct TemplateContextLayer {
    auto_reloader: Arc<AutoReloader>,
}

impl TemplateContextLayer {
    pub(super) fn new() -> Self {
        let cargo_manifest_dir = std::env::var("CARGO_MANIFEST_DIR").expect(
            "templates-autoreload Cargo feature is only meant for development, \
             but CARGO_MANIFEST_DIR is not set",
        );
        let template_dir = Utf8Path::new(&cargo_manifest_dir).join("templates");

        let auto_reloader = Arc::new(AutoReloader::new(move |notifier| {
            let mut env = minijinja::Environment::new();
            env.set_loader(path_loader(template_dir.as_str()));

            notifier.watch_path(&template_dir, true);
            notifier.set_fast_reload(true);

            Ok(env)
        }));

        Self { auto_reloader }
    }
}

impl<S> tower::Layer<S> for TemplateContextLayer {
    type Service = TemplateContextService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        TemplateContextService {
            auto_reloader: self.auto_reloader.clone(),
            inner,
        }
    }
}

#[derive(Clone)]
pub struct TemplateContextService<S> {
    auto_reloader: Arc<AutoReloader>,
    inner: S,
}

impl<B, S> tower::Service<http::Request<B>> for TemplateContextService<S>
where
    S: tower::Service<http::Request<B>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = TaskLocalFuture<Arc<AutoReloader>, S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<B>) -> Self::Future {
        AUTO_RELOADER.scope(self.auto_reloader.clone(), self.inner.call(req))
    }
}

#[allow(clippy::result_large_err)]
pub(super) fn with_environment(f: impl FnOnce(&minijinja::Environment<'_>) -> TemplateResponse) -> TemplateResponse {
    AUTO_RELOADER.with(|reloader| {
        let env = reloader.acquire_env().map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to reload MiniJinja templates: {e}"),
            )
        })?;

        f(&env)
    })
}
