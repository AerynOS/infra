use minijinja::{Value, value::ViaDeserialize};

use crate::{Project, Task, task};

pub fn profile(projects: ViaDeserialize<Vec<Project>>, id: i64) -> Option<Value> {
    projects
        .iter()
        .find_map(|p| p.profiles.iter().find(|p| p.id == id.into()))
        .map(Value::from_serialize)
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
