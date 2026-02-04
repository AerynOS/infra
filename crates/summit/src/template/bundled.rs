use std::sync::LazyLock;

use include_dir::{Dir, DirEntry, include_dir};
use tracing::error;

use super::env;

static ENV: LazyLock<minijinja::Environment<'_>> = LazyLock::new(|| {
    static TEMPLATES: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR/templates");

    let mut env = env();
    add_templates(&TEMPLATES, &mut env);

    env
});

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

#[allow(clippy::result_large_err)]
pub(super) fn with_environment<T>(f: impl FnOnce(&minijinja::Environment<'_>) -> T) -> T {
    f(&ENV)
}
