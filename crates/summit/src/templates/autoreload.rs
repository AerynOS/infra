use std::sync::LazyLock;

use camino::Utf8Path;
use http::StatusCode;
use minijinja::path_loader;
use minijinja_autoreload::AutoReloader;

use super::{Response, env};

static AUTO_RELOADER: LazyLock<minijinja_autoreload::AutoReloader> = LazyLock::new(|| {
    let cargo_manifest_dir = std::env::var("CARGO_MANIFEST_DIR").expect(
        "templates-autoreload Cargo feature is only meant for development, \
             but CARGO_MANIFEST_DIR is not set",
    );
    let template_dir = Utf8Path::new(&cargo_manifest_dir).join("templates");

    AutoReloader::new(move |notifier| {
        let mut env = env();
        env.set_loader(path_loader(template_dir.as_str()));

        notifier.watch_path(&template_dir, true);
        notifier.set_fast_reload(true);

        Ok(env)
    })
});

#[allow(clippy::result_large_err)]
pub(super) fn with_environment(f: impl FnOnce(&minijinja::Environment<'_>) -> Response) -> Response {
    let env = AUTO_RELOADER.acquire_env().map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to reload MiniJinja templates: {e}"),
        )
    })?;

    f(&env)
}
