use axum::response::Html;
use http::StatusCode;
use serde::Serialize;

#[cfg_attr(
    all(feature = "templates-bundled", not(feature = "templates-autoreload")),
    path = "templates/bundled.rs"
)]
#[cfg_attr(feature = "templates-autoreload", path = "templates/autoreload.rs")]
mod implementation;

pub type Response = axum::response::Result<Html<String>>;

#[allow(clippy::result_large_err)]
pub fn render_html<S>(name: &str, ctx: S) -> Response
where
    S: Serialize,
{
    let do_render = |env: &minijinja::Environment<'_>| {
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
