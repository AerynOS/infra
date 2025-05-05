use std::collections::{HashMap, VecDeque};

use color_eyre::eyre::{self, Context, OptionExt, Report, Result};
use moss::db::meta;
use service::{State, database::Transaction, endpoint, error};
use sqlx::{Sqlite, pool::PoolConnection};
use tokio::task::spawn_blocking;
use tracing::{Span, debug, error, info, warn};

use crate::{Builder, Project, Queue, builder, profile, project, repository, task};

pub struct Manager {
    pub state: State,
    queue: Queue,
    builders: HashMap<endpoint::Id, Builder>,
    profile_dbs: HashMap<profile::Id, meta::Database>,
    repository_dbs: HashMap<repository::Id, meta::Database>,
}

impl Manager {
    #[tracing::instrument(name = "load_manager", skip_all)]
    pub async fn load(state: State) -> Result<Self> {
        let projects = project::list(&mut *state.service_db.acquire().await?).await?;

        let span = Span::current();

        // Moss DB implementations are blocking
        let (state, profile_dbs, repository_dbs) = spawn_blocking(move || {
            let _enter = span.enter();

            let profile_dbs = projects
                .iter()
                .flat_map(|project| {
                    project
                        .profiles
                        .iter()
                        .map(|profile| Ok((profile.id, connect_profile_db(&state, &profile.id)?)))
                })
                .collect::<Result<HashMap<_, _>, eyre::Error>>()?;

            let repository_dbs = projects
                .iter()
                .flat_map(|project| {
                    project
                        .repositories
                        .iter()
                        .map(|repo| Ok((repo.id, connect_repository_db(&state, &repo.id)?)))
                })
                .collect::<Result<HashMap<_, _>, eyre::Error>>()?;

            info!(num_projects = projects.len(), "Projects loaded");

            Result::<_, eyre::Error>::Ok((state, profile_dbs, repository_dbs))
        })
        .await
        .context("join handle")??;

        let manager = Self {
            state,
            queue: Queue::default(),
            builders: HashMap::default(),
            profile_dbs,
            repository_dbs,
        };

        manager.refresh(true).await.context("refresh")?;

        Ok(manager)
    }

    pub async fn refresh(&self, force: bool) -> Result<bool> {
        let mut have_changes = false;

        let mut conn = self.acquire().await.context("acquire db conn")?;

        for mut project in self.projects().await.context("list projects")? {
            let mut profile_refreshed = false;

            // Ensure un-indexed profiles get refreshed, such as if they failed
            // to refresh previously from spurious errors
            for profile in &mut project.profiles {
                let mut refresh = async || {
                    if force || !matches!(profile.status, profile::Status::Indexed) {
                        if !matches!(profile.status, profile::Status::Indexed) {
                            debug!(
                                profile = %profile.name,
                                status = %profile.status,
                                "Profile not fully indexed, refreshing..."
                            );
                        }

                        let profile_db = self.profile_db(&profile.id).cloned()?;

                        profile::refresh(&self.state, profile, profile_db)
                            .await
                            .context("refres profile")?;

                        Result::<_, Report>::Ok(true)
                    } else {
                        Ok(false)
                    }
                };

                match refresh().await {
                    Ok(refreshed) => {
                        if refreshed {
                            profile_refreshed = true;
                        }
                    }
                    Err(err) => {
                        error!(
                            profile = %profile.name,
                            error = error::chain(&*err),
                            "Failed to refresh profile"
                        );
                    }
                }
            }

            // Refresh each repo & if newer, reindex. If reindexed, try adding missing tasks
            for repo in &project.repositories {
                let mut refresh = async |profile_refreshed: bool| {
                    let (mut repo, repo_changed) = repository::refresh(&mut conn, &self.state, repo.clone())
                        .await
                        .context("refresh repository")?;

                    let repo_db = self.repository_db(&repo.id)?.clone();

                    if repo_changed {
                        repo = repository::reindex(&mut conn, &self.state, repo, repo_db.clone())
                            .await
                            .context("reindex repository")?;
                    }

                    if force || repo_changed || profile_refreshed {
                        task::create_missing(&mut conn, self, &project, &repo, &repo_db)
                            .await
                            .context("create missing tasks")?;

                        Result::<_, Report>::Ok(true)
                    } else {
                        Ok(false)
                    }
                };

                match refresh(profile_refreshed).await {
                    Ok(tasks_created) => {
                        if tasks_created {
                            have_changes = true;
                        }
                    }
                    Err(err) => {
                        error!(error = error::chain(&*err), "Failed to refresh repository");
                    }
                }
            }
        }

        Ok(have_changes)
    }

    pub async fn begin(&self) -> Result<Transaction> {
        Ok(self.state.service_db.begin().await?)
    }

    pub async fn acquire(&self) -> Result<PoolConnection<Sqlite>> {
        Ok(self.state.service_db.acquire().await?)
    }

    pub async fn projects(&self) -> Result<Vec<Project>> {
        Ok(project::list(&mut *self.state.service_db.acquire().await?).await?)
    }

    pub fn profile_db(&self, profile: &profile::Id) -> Result<&meta::Database> {
        self.profile_dbs.get(profile).ok_or_eyre("missing profile")
    }

    pub fn repository_db(&self, repo: &repository::Id) -> Result<&meta::Database> {
        self.repository_dbs.get(repo).ok_or_eyre("missing repository")
    }

    pub async fn allocate_builds(&mut self) -> Result<()> {
        let mut conn = self.acquire().await.context("acquire db conn")?;

        let projects = project::list(&mut conn).await.context("list projects")?;

        self.queue
            .recompute(&mut conn, &projects, &self.repository_dbs)
            .await
            .context("recompute queue")?;

        let mut available = self.queue.available().collect::<VecDeque<_>>();
        let mut next_task = available.pop_front();

        if next_task.is_none() {
            debug!("No tasks available");
            return Ok(());
        }

        let builders = self
            .builders
            .values_mut()
            .filter(|builder| builder.is_idle())
            .collect::<Vec<_>>();

        if builders.is_empty() {
            debug!("No builders available");
            return Ok(());
        }

        // We will use a TX within each build to atomically
        // update all the things together, so we don't need
        // this connection anymore
        drop(conn);

        for builder in builders {
            if let Some(task) = next_task {
                match builder.build(&self.state, task).await {
                    Ok(_) => {
                        next_task = available.pop_front();
                    }
                    Err(_) => {
                        warn!(builder = %builder.endpoint, "Failed to send build, trying next builder");
                    }
                }
            } else {
                // Nothing else to build
                break;
            }
        }

        Ok(())
    }

    pub async fn import_succeeded(&mut self, task_id: task::Id) -> Result<()> {
        let mut tx = self.begin().await.context("begin db tx")?;

        task::set_status(&mut tx, task_id, task::Status::Completed)
            .await
            .context("set task as import failed")?;

        let mut projects = project::list(tx.as_mut()).await.context("list projects")?;

        let task = task::query(tx.as_mut(), task::query::Params::default().id(task_id))
            .await
            .context("query task")?
            .tasks
            .into_iter()
            .next()
            .ok_or_eyre("task is missing")?;

        self.queue
            .task_completed(&mut tx, task_id)
            .await
            .context("add queue blockers")?;

        tx.commit().await.context("commit db tx")?;

        let profile = projects
            .iter_mut()
            .find_map(|p| p.profiles.iter_mut().find(|p| p.id == task.profile_id))
            .ok_or_eyre("missing profile")?;
        let profile_db = self
            .profile_dbs
            .get(&task.profile_id)
            .ok_or_eyre("missing profile db")?
            .clone();
        profile::refresh(&self.state, profile, profile_db)
            .await
            .context("refresh profile")?;

        Ok(())
    }

    pub async fn import_failed(&mut self, task_id: task::Id) -> Result<()> {
        let mut tx = self.begin().await.context("begin db tx")?;

        task::set_status(&mut tx, task_id, task::Status::Failed)
            .await
            .context("set task as import failed")?;

        self.queue
            .task_failed(&mut tx, task_id)
            .await
            .context("add queue blockers")?;

        tx.commit().await.context("commit db tx")?;

        Ok(())
    }

    pub async fn retry(&mut self, task_id: task::Id) -> Result<()> {
        let mut tx = self.begin().await.context("begin db tx")?;

        let task = task::query(tx.as_mut(), task::query::Params::default().id(task_id))
            .await
            .context("get task")?
            .tasks
            .into_iter()
            .next()
            .ok_or_eyre("task is missing")?;

        if matches!(task.status, task::Status::New) {
            warn!(status = %task.status, "Task is already in new status and won't be retried");
            return Ok(());
        }

        task::set_status(&mut tx, task_id, task::Status::New)
            .await
            .context("set task as import failed")?;

        tx.commit().await.context("commit db tx")?;

        info!("Task marked for retry");

        Ok(())
    }

    pub async fn update_builder(&mut self, endpoint: endpoint::Id, message: builder::Message) -> Result<bool> {
        let builder = self.builders.entry(endpoint).or_insert_with(|| Builder::new(endpoint));

        if let Some(event) = builder.update(&self.state, message).await? {
            match event {
                builder::Event::Connected => {
                    return Ok(true);
                }
                builder::Event::BuildFailed { task_id } => {
                    let mut tx = self.begin().await.context("begin db tx")?;

                    self.queue
                        .task_failed(&mut tx, task_id)
                        .await
                        .context("add queue blockers")?;

                    tx.commit().await.context("commit db tx")?;

                    return Ok(true);
                }
            }
        }

        Ok(false)
    }
}

fn connect_profile_db(state: &State, profile: &profile::Id) -> Result<meta::Database> {
    use std::fs;

    let parent = state.db_dir.join("profile");

    fs::create_dir_all(&parent).context("create profile db directory")?;

    let db =
        meta::Database::new(parent.join(profile.to_string()).to_string_lossy().as_ref()).context("open profile db")?;

    Ok(db)
}

fn connect_repository_db(state: &State, repository: &repository::Id) -> Result<meta::Database> {
    use std::fs;

    let parent = state.db_dir.join("repository");

    fs::create_dir_all(&parent).context("create repository db directory")?;

    let db = meta::Database::new(parent.join(repository.to_string()).to_string_lossy().as_ref())
        .context("open repository db")?;

    Ok(db)
}
