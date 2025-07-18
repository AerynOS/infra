use minijinja::{Value, value::ViaDeserialize};
use service::{Endpoint, endpoint};

use crate::{Project, Task, builder, task};

pub fn repository(projects: ViaDeserialize<Vec<Project>>, id: i64) -> Option<Value> {
    projects
        .iter()
        .find_map(|p| p.repositories.iter().find(|r| r.id == id.into()))
        .map(Value::from_serialize)
}

pub fn profile(projects: ViaDeserialize<Vec<Project>>, id: i64) -> Option<Value> {
    projects
        .iter()
        .find_map(|p| p.profiles.iter().find(|p| p.id == id.into()))
        .map(Value::from_serialize)
}

pub fn endpoint(endpoints: ViaDeserialize<Vec<Endpoint>>, id: ViaDeserialize<Option<endpoint::Id>>) -> Option<Value> {
    let id = id.0?;

    endpoints.iter().find(|e| e.id == id).map(Value::from_serialize)
}

pub fn builder(
    builders: ViaDeserialize<Vec<builder::Info>>,
    id: ViaDeserialize<Option<endpoint::Id>>,
) -> Option<Value> {
    let id = id.0?;

    builders.iter().find(|b| b.endpoint == id).map(Value::from_serialize)
}

pub fn task(tasks: ViaDeserialize<Vec<Task>>, id: ViaDeserialize<Option<task::Id>>) -> Option<Value> {
    let id = id.0?;

    tasks.iter().find(|t| t.id == id).map(Value::from_serialize)
}

pub fn format_duration(value: Value) -> Result<Value, minijinja::Error> {
    let secs = match value.as_i64() {
        Some(s) => s,
        None => return Ok(Value::from("")),
    };
    let hours = secs / 3600;
    let minutes = (secs % 3600) / 60;
    let seconds = secs % 60;

    Ok(Value::from(format!("{hours:02}h {minutes:02}m {seconds:02}s")))
}
