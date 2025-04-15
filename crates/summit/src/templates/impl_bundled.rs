use std::{
    sync::Arc,
    task::{Context, Poll},
};

use include_dir::{Dir, DirEntry, include_dir};
use tokio::task::futures::TaskLocalFuture;
use tracing::error;

use super::TemplateResponse;

tokio::task_local! {
    static MINIJINJA_ENV: Arc<minijinja::Environment<'_>>;
}

#[derive(Clone)]
pub struct TemplateContextLayer {
    minijinja_env: Arc<minijinja::Environment<'static>>,
}

impl TemplateContextLayer {
    pub(super) fn new() -> Self {
        static TEMPLATES: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR/templates");

        let mut env = minijinja::Environment::new();
        add_templates(&TEMPLATES, &mut env);

        Self {
            minijinja_env: Arc::new(env),
        }
    }
}

fn add_templates<'a>(dir: &Dir<'a>, env: &mut minijinja::Environment<'a>) {
    for entry in dir.entries() {
        let file = match entry {
            DirEntry::Dir(dir) => {
                add_templates(dir, env);
                return;
            }
            DirEntry::File(file) => file,
        };

        let Some(tpl_name) = file.path().as_os_str().to_str() else {
            error!("non-utf8 template path: `{}`", file.path().display());
            continue;
        };
        let Some(tpl_source) = file.contents_utf8() else {
            error!(template = tpl_name, "template contains non-utf8 bytes");
            continue;
        };

        if let Err(e) = env.add_template(tpl_name, tpl_source) {
            error!(
                template = tpl_name,
                error = &e as &dyn std::error::Error,
                "failed to add template"
            );
            continue;
        }
    }
}

impl<S> tower::Layer<S> for TemplateContextLayer {
    type Service = TemplateContextService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        TemplateContextService {
            minijinja_env: self.minijinja_env.clone(),
            inner,
        }
    }
}

#[derive(Clone)]
pub struct TemplateContextService<S> {
    minijinja_env: Arc<minijinja::Environment<'static>>,
    inner: S,
}

impl<B, S> tower::Service<http::Request<B>> for TemplateContextService<S>
where
    S: tower::Service<http::Request<B>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = TaskLocalFuture<Arc<minijinja::Environment<'static>>, S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<B>) -> Self::Future {
        MINIJINJA_ENV.scope(self.minijinja_env.clone(), self.inner.call(req))
    }
}

#[allow(clippy::result_large_err)]
pub(super) fn with_environment(f: impl FnOnce(&minijinja::Environment<'_>) -> TemplateResponse) -> TemplateResponse {
    MINIJINJA_ENV.with(|env| f(env))
}
