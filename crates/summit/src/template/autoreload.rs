use std::sync::LazyLock;

use camino::Utf8Path;
use minijinja::path_loader;
use minijinja_autoreload::AutoReloader;

use super::env;

static AUTO_RELOADER: LazyLock<AutoReloader> = LazyLock::new(|| {
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
pub(super) fn with_environment<T>(f: impl FnOnce(&minijinja::Environment<'_>) -> T) -> T {
    let env = AUTO_RELOADER.acquire_env().expect("env built successfully");

    f(&env)
}
