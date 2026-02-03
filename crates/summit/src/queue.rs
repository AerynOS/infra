use std::collections::{BTreeMap, HashMap};

use color_eyre::eyre::{self, Context, OptionExt, Result};
use dag::Dag;
use futures_util::{StreamExt, TryStreamExt, stream};
use itertools::Itertools;
use moss::db::meta;
use service::error;
use sqlx::SqliteConnection;
use tokio::task::spawn_blocking;
use tracing::{debug, error, warn};

use crate::{
    Project,
    config::BuildSizesConfig,
    repository,
    task::{self, TaskQueue},
};

#[derive(Default)]
pub struct Queue(Vec<task::Queued>);

impl Queue {
    #[tracing::instrument(name = "recompute_queue", skip_all)]
    pub async fn recompute(
        &mut self,
        conn: &mut SqliteConnection,
        projects: &[Project],
        repo_dbs: &HashMap<repository::Id, meta::Database>,
        build_sizes: &BuildSizesConfig,
    ) -> Result<()> {
        let open_tasks = task::query(conn, task::query::Params::default().statuses(task::Status::open()))
            .await
            .context("list open tasks")?
            .tasks;

        let mapped_tasks = stream::iter(open_tasks)
            .then(|task| async {
                let project = projects
                    .iter()
                    .find(|p| p.id == task.project_id)
                    .ok_or_eyre("task has missing project")?;
                let profile = project
                    .profiles
                    .iter()
                    .find(|p| p.id == task.profile_id)
                    .ok_or_eyre("task has missing profile")?;
                let repo = project
                    .repositories
                    .iter()
                    .find(|r| r.id == task.repository_id)
                    .ok_or_eyre("task has missing repo")?;
                let db = repo_dbs.get(&repo.id).ok_or_eyre("repo has missing meta db")?.clone();

                let package_id = task.package_id.clone().into();

                let meta = match spawn_blocking(move || db.get(&package_id))
                    .await
                    .context("join handle")?
                    .context("find meta in repo db for task")
                {
                    Ok(meta) => meta,
                    Err(err) => {
                        // TODO: Mark these tasks as failed / some other terminal status?
                        //       status::Zombie maybe?
                        warn!(
                            error = error::chain(&*err),
                            task_id = %task.id,
                            build_id = %task.build_id,
                            profile = %profile.name,
                            repository = %repo.name,
                            "Task metadata missing, omitting from queue"
                        );
                        return Ok(None);
                    }
                };

                let size = build_sizes.get(&meta.source_id);

                let remotes = profile
                    .remotes
                    .iter()
                    .sorted_by_key(|r| r.priority)
                    .map(|r| &r.index_uri)
                    .chain(Some(&profile.index_uri))
                    .cloned()
                    .collect();

                Result::<_, eyre::Report>::Ok(Some((
                    task.id,
                    task::Queued {
                        task,
                        meta,
                        commit_ref: repo.commit_ref.clone().ok_or_eyre("missing repo commit ref")?,
                        origin_uri: repo.origin_uri.clone(),
                        index_uri: profile.index_uri.clone(),
                        remotes,
                        dependencies: vec![],
                        size,
                    },
                )))
            })
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .flatten()
            .collect::<BTreeMap<_, _>>();

        let mut dag = Dag::<task::Id>::new();

        for current in mapped_tasks.values() {
            let current_node = dag.add_node_or_get_index(&current.task.id);

            // No deps to link
            if current.meta.dependencies.is_empty() {
                continue;
            }

            // All tasks that provide something that satisfies
            // a dep of this task == a dependency of this task
            mapped_tasks
                .values()
                // Intersections == dependency
                //
                // NOTE: I tried using a BTreeSet here but it slowed this function
                // down from millis on 1500 items to seconds.
                .filter(|a| {
                    a.meta.providers.iter().any(|provider| {
                        current
                            .meta
                            .dependencies
                            .iter()
                            .any(|dep| dep.kind == provider.kind && dep.name == provider.name)
                    })
                })
                // Ensure tasks share the same arch & index
                .filter(|a| {
                    current.task.id != a.task.id
                        && current.task.arch == a.task.arch
                        && current.remotes.contains(&a.index_uri)
                })
                // Connect each dependency to this task
                .for_each(|dependency| {
                    let dep_node = dag.add_node_or_get_index(&dependency.task.id);

                    // We want to iterate the graph by dependencies
                    // so we add the direction of the edge from
                    // the dependent to the dependency
                    if !dag.add_edge(current_node, dep_node) {
                        // Cyclic connection
                        error!(
                            dependency = %dependency.task.build_id,
                            dependent = %current.task.build_id,
                            "Cyclic connection in queue DAG"
                        );
                    }
                });
        }

        self.0 = mapped_tasks
            .into_values()
            .map(|mut queued| {
                queued.dependencies = dag
                    // DFS traverses deps with how we connected edges above
                    .dfs(dag.get_index(&queued.task.id).expect("topo derived from dag"))
                    // DFS always starts on current node, skip it
                    .skip(1)
                    .copied()
                    .collect();
                queued
            })
            .collect();

        debug!(num_tasks = self.0.len(), "Queue recomputed");

        Ok(())
    }

    pub fn available(&self) -> impl Iterator<Item = &task::Queued> {
        self.0
            .iter()
            .filter(|queued| queued.dependencies.is_empty() && matches!(queued.task.status, task::Status::New))
    }
}

impl TaskQueue for Queue {
    fn get(&self, task_id: task::Id) -> Option<&task::Queued> {
        self.0.iter().find(|queued| queued.task.id == task_id)
    }

    fn dependents(&self, task_id: task::Id) -> Vec<task::Id> {
        self.0
            .iter()
            .filter(move |queued| queued.dependencies.contains(&task_id))
            .map(|queued| queued.task.id)
            .collect()
    }
}
