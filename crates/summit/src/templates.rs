use axum::response::Html;
use http::StatusCode;
use minijinja::{Environment, Value, value::ViaDeserialize};
use serde::Serialize;

use crate::Project;

#[cfg_attr(
    all(feature = "templates-bundled", not(feature = "templates-autoreload")),
    path = "templates/bundled.rs"
)]
#[cfg_attr(feature = "templates-autoreload", path = "templates/autoreload.rs")]
mod implementation;

pub type Response = axum::response::Result<Html<String>>;

fn env() -> Environment<'static> {
    let mut env = Environment::new();
    env.add_filter("repository", repository_filter);
    env.add_filter("profile", profile_filter);
    env.add_filter("format_duration", format_duration_filter);

    env
}

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

fn repository_filter(projects: ViaDeserialize<Vec<Project>>, id: i64) -> Option<Value> {
    projects
        .iter()
        .find_map(|p| p.repositories.iter().find(|r| r.id == id.into()))
        .map(Value::from_serialize)
}

fn profile_filter(projects: ViaDeserialize<Vec<Project>>, id: i64) -> Option<Value> {
    projects
        .iter()
        .find_map(|p| p.profiles.iter().find(|p| p.id == id.into()))
        .map(Value::from_serialize)
}

fn format_duration_filter(value: Value) -> Result<Value, minijinja::Error> {
    let secs = match value.as_i64() {
        Some(s) => s,
        None => return Ok(Value::from("")),
    };
    let hours = secs / 3600;
    let minutes = (secs % 3600) / 60;
    let seconds = secs % 60;

    Ok(Value::from(format!("{:02}h {:02}m {:02}s", hours, minutes, seconds)))
}
