use std::collections::HashMap;

use color_eyre::eyre::{self, Context, OptionExt, Result};
use dag::Dag;
use futures_util::{StreamExt, TryStreamExt, stream};
use itertools::Itertools;
use moss::db::meta;
use service::{database::Transaction, error};
use sqlx::SqliteConnection;
use tokio::task::spawn_blocking;
use tracing::{debug, info, warn};

use crate::{Project, repository, task};

#[derive(Default)]
pub struct Queue(Vec<task::Queued>);

impl Queue {
    #[tracing::instrument(name = "recompute_queue", skip_all)]
    pub async fn recompute(
        &mut self,
        conn: &mut SqliteConnection,
        projects: &[Project],
        repo_dbs: &HashMap<repository::Id, meta::Database>,
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
                            task = %task.id,
                            build_id = %task.build_id,
                            profile = %profile.name,
                            repository = %repo.name,
                            "Task metadata missing, omitting from queue"
                        );
                        return Ok(None);
                    }
                };

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
                    },
                )))
            })
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .flatten()
            .collect::<HashMap<_, _>>();

        let mut dag = Dag::<task::Id>::new();

        for current in mapped_tasks.values() {
            let current_node = dag.add_node_or_get_index(&current.task.id);

            // All other tasks which share the same arch & index
            let common_tasks = mapped_tasks
                .values()
                .filter(|a| {
                    current.task.id != a.task.id
                        && current.task.arch == a.task.arch
                        && current.remotes.contains(&a.index_uri)
                })
                .collect::<Vec<_>>();

            // for dep in current
            for dep in &current.meta.dependencies {
                common_tasks
                    .iter()
                    .filter(|a| {
                        a.meta
                            .providers
                            .iter()
                            .any(|p| p.kind == dep.kind && p.name == dep.name)
                    })
                    .for_each(|provider| {
                        let provider_node = dag.add_node_or_get_index(&provider.task.id);

                        dag.add_edge(provider_node, current_node);
                    });
            }
        }

        let mut topo = dag
            .topo()
            .map(|id| mapped_tasks.get(id).cloned().expect("dag populated from mapped tasks"))
            .collect::<Vec<_>>();

        // Transpose the graph so we can search for
        // all dependencies of a package
        let transposed = dag.transpose();

        for queued in topo.iter_mut() {
            queued.dependencies = transposed
                .dfs(dag.get_index(&queued.task.id).expect("topo derived from dag"))
                // DFS always starts on current node, skip it
                .skip(1)
                .copied()
                .collect();
        }

        self.0 = topo;

        debug!(num_tasks = self.0.len(), "Queue recomputed");

        Ok(())
    }

    #[tracing::instrument(name = "queue_add_blockers", skip_all, fields(%task_id))]
    pub async fn add_blockers(&mut self, tx: &mut Transaction, task_id: task::Id) -> Result<()> {
        let idx = self
            .0
            .iter()
            .position(|queued| queued.task.id == task_id)
            .ok_or_eyre("task_id={task_id:?} is missing")?;
        let removed = self.0.remove(idx);

        let blocker_id = format!(
            "{}_{}@{}/{}",
            removed.meta.source_id, removed.task.arch, removed.task.project_id, removed.task.repository_id
        );

        let mut num_blocked = 0;

        for blocked in self.0.iter().filter(|queued| queued.dependencies.contains(&task_id)) {
            task::block(tx, blocked.task.id, &blocker_id)
                .await
                .context("add task blocker")?;

            num_blocked += 1;

            warn!(task_id = %blocked.task.id, blocker = %removed.task.id, "Task blocked");
        }

        if num_blocked == 0 {
            debug!(%task_id, "No dependents to block");
        }

        Ok(())
    }

    #[tracing::instrument(name = "queue_remove_blockers", skip_all, fields(%task_id))]
    pub async fn remove_blockers(&mut self, tx: &mut Transaction, task_id: task::Id) -> Result<()> {
        let idx = self
            .0
            .iter()
            .position(|queued| queued.task.id == task_id)
            .ok_or_eyre("task is missing")?;
        let removed = self.0.remove(idx);

        let blocker_id = format!(
            "{}_{}@{}/{}",
            removed.meta.source_id, removed.task.arch, removed.task.project_id, removed.task.repository_id
        );

        for blocked in self.0.iter().filter(|queued| queued.dependencies.contains(&task_id)) {
            let remaining = task::unblock(tx, blocked.task.id, &blocker_id)
                .await
                .context("unblock task")?;

            if remaining > 0 {
                info!(task_id = %blocked.task.id, "Task still blocked ({remaining:?} remaining blockers)");
            } else {
                info!(task_id = %blocked.task.id, "Task unblocked");
            }
        }

        Ok(())
    }

    pub fn available(&self) -> impl Iterator<Item = &task::Queued> {
        self.0
            .iter()
            .filter(|queued| queued.dependencies.is_empty() && matches!(queued.task.status, task::Status::New))
    }
}
