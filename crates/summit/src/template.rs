use axum::response::Html;
use http::StatusCode;
use minijinja::Environment;
use serde::Serialize;

mod filter;
mod function;
#[cfg_attr(
    all(feature = "templates-bundled", not(feature = "templates-autoreload")),
    path = "template/bundled.rs"
)]
#[cfg_attr(feature = "templates-autoreload", path = "template/autoreload.rs")]
mod implementation;

pub type Response = axum::response::Result<Html<String>>;

fn env() -> Environment<'static> {
    let mut env = Environment::new();
    env.add_filter("repository", filter::repository);
    env.add_filter("profile", filter::profile);
    env.add_filter("endpoint", filter::endpoint);
    env.add_filter("builder", filter::builder);
    env.add_filter("task", filter::task);
    env.add_filter("format_duration", filter::format_duration);
    env.add_function("build_task_query_url", function::build_task_query_url);

    env
}

#[allow(clippy::result_large_err)]
pub fn render<S>(name: &str, ctx: S) -> Response
where
    S: Serialize,
{
    let do_render = |env: &Environment<'_>| {
        let Ok(template) = env.get_template(name) else {
            return Err((StatusCode::INTERNAL_SERVER_ERROR, "Couldn't find MiniJinja template").into());
        };

        let rendered = template.render(ctx).map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to render MiniJinja template: {e}"),
            )
        })?;

        Ok(Html(rendered))
    };

    implementation::with_environment(do_render)
}
