use urlencoding::encode;

pub fn build_task_query_url(
    status: Option<&str>,
    sort: Option<&str>,
    order: Option<&str>,
    search_tasks: Option<&str>,
) -> String {
    let mut parts = vec![];

    if let Some(s) = status
        && !s.is_empty()
    {
        parts.push(format!("status={}", encode(s)));
    }
    if let Some(s) = sort
        && !s.is_empty()
    {
        parts.push(format!("sort={}", encode(s)));
    }
    if let Some(s) = order
        && !s.is_empty()
    {
        parts.push(format!("order={}", encode(s)));
    }
    if let Some(s) = search_tasks
        && !s.is_empty()
    {
        parts.push(format!("search_tasks={}", encode(s)));
    }

    if parts.is_empty() {
        "".to_owned()
    } else {
        parts.join("&").to_string()
    }
}
