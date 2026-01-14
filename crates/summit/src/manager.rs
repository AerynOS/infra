use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};

use color_eyre::eyre::{self, Context, OptionExt, Report, Result, bail};
use itertools::Itertools;
use moss::db::meta;
use service::{Account, Endpoint, State, database::Transaction, endpoint, error};
use sqlx::{Sqlite, pool::PoolConnection};
use tokio::task::spawn_blocking;
use tracing::{Span, debug, error, info, warn};

use crate::{Builder, Config, Project, Queue, builder, config::Size, profile, project, repository, task};

pub struct Manager {
    pub state: State,
    pub paused: bool,
    queue: Queue,
    builders: BTreeMap<builder::Key, Builder>,
    profile_dbs: HashMap<profile::Id, meta::Database>,
    repository_dbs: HashMap<repository::Id, meta::Database>,
    config: Config,
}

impl Manager {
    #[tracing::instrument(name = "load_manager", skip_all)]
    pub async fn load(config: Config, state: State) -> Result<Self> {
        let mut conn = state.service_db.acquire().await?;

        let projects = project::list(conn.as_mut()).await?;

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

        let mut builders = BTreeMap::default();

        for endpoint in Endpoint::list(conn.as_mut()).await.context("list endpoints")? {
            if endpoint.role == endpoint::Role::Builder {
                let account = Account::get(conn.as_mut(), endpoint.account)
                    .await
                    .context(format!("lookup account: {}", endpoint.account))?;

                if let Some((index, config)) = config
                    .builders
                    .iter()
                    .enumerate()
                    .find(|(_, config)| config.public_key.encode() == account.public_key)
                {
                    builders.insert(
                        builder::Key {
                            index,
                            endpoint: endpoint.id,
                        },
                        Builder::new(endpoint.id, config),
                    );
                }
            }
        }

        let manager = Self {
            state,
            paused: false,
            queue: Queue::default(),
            builders,
            profile_dbs,
            repository_dbs,
            config,
        };

        manager.refresh(true).await.context("refresh")?;

        Ok(manager)
    }

    pub async fn refresh(&self, force: bool) -> Result<bool> {
        let mut have_changes = false;

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
                            .context("refresh profile")?;

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
                let refresh = async |profile_refreshed: bool| {
                    let (mut repo, repo_changed) = repository::refresh(
                        &mut *self.acquire().await.context("acquire db conn")?,
                        &self.state,
                        repo.clone(),
                    )
                    .await
                    .context("refresh repository")?;

                    let repo_db = self.repository_db(&repo.id)?.clone();

                    if repo_changed {
                        repo = repository::reindex(
                            &mut *self.acquire().await.context("acquire db conn")?,
                            &self.state,
                            repo,
                            repo_db.clone(),
                        )
                        .await
                        .context("reindex repository")?;
                    }

                    if force || repo_changed || profile_refreshed {
                        let mut tx = self.begin().await.context("begin db tx")?;

                        task::fix_and_create(&mut tx, self, &project, &repo, &repo_db)
                            .await
                            .context("create missing tasks")?;

                        tx.commit().await.context("commit tx")?;

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
            .recompute(&mut conn, &projects, &self.repository_dbs, &self.config.build_sizes)
            .await
            .context("recompute queue")?;

        let mut available_tasks = self
            .queue
            .available()
            // Sort by largest to smallest
            .sorted_by(|a, b| a.size.cmp(&b.size).reverse())
            .collect::<VecDeque<_>>();

        if available_tasks.is_empty() {
            debug!("No tasks available");
            return Ok(());
        }

        let max_connected_builder_size = self
            .builders
            .values()
            .filter(|builder| builder.is_connected())
            .map(|builder| builder.size)
            .max()
            .unwrap_or(Size::Small);

        let mut idle_builders = self
            .builders
            .values_mut()
            .filter(|builder| builder.is_idle())
            .collect::<Vec<_>>();

        if idle_builders.is_empty() {
            debug!("No builders available");
            return Ok(());
        }

        // We will use a TX within each build to atomically
        // update all the things together, so we don't need
        // this connection anymore
        drop(conn);

        // Tasks are processed from highest to lowest build size, so
        // we just need to ensure we queue up against the largest available
        // builder or wait for it if its not available. If no builder is connected
        // at that size, we allow larger builds to queue on the next smaller connected builder.
        'tasks: while let Some(task) = available_tasks.pop_front() {
            loop {
                if idle_builders.is_empty() {
                    break 'tasks;
                }

                let available_sizes = idle_builders.iter().map(|b| b.size).collect::<BTreeSet<_>>();

                // Attempt to find the smallest builder that fits (>=) this task,
                // otherwise find the largest builder that is smaller (<) than the task
                let best_fit_builder_size = available_sizes
                    .iter()
                    .find(|size| **size >= task.size)
                    .or_else(|| available_sizes.iter().rev().find(|size| **size < task.size))
                    .copied()
                    .expect("non empty");

                // Builder can build this size
                // OR no builder is connected for this size but this is the next biggest size available
                //
                // This allows us to "wait" for an applicable sized builder, if available, instead of queuing
                // it onto a builder too small
                if best_fit_builder_size >= task.size
                    || (max_connected_builder_size < task.size && best_fit_builder_size == max_connected_builder_size)
                {
                    let builder_idx = idle_builders
                        .iter()
                        .position(|b| b.size == best_fit_builder_size)
                        .expect("non empty");
                    let builder = idle_builders.remove(builder_idx);

                    if task.size > best_fit_builder_size {
                        debug!(
                            builder = %builder.endpoint,
                            task = %task.task.id,
                            build = %task.task.build_id,
                            task_size = %task.size,
                            builder_size = %best_fit_builder_size,
                            %max_connected_builder_size,
                            "No builder is connected to handle this task size. Queueing it on the next largest builder."
                        );
                    }

                    match builder.build(&self.state, task).await {
                        Ok(_) => {
                            continue 'tasks;
                        }
                        Err(_) => {
                            warn!(builder = %builder.endpoint, "Failed to send build, trying next builder");
                        }
                    }
                } else {
                    debug!(
                        task = %task.task.id,
                        build = %task.task.build_id,
                        task_size = %task.size,
                        %max_connected_builder_size,
                        "Waiting on a builder large enough to queue up this task"
                    );

                    continue 'tasks;
                }
            }
        }

        Ok(())
    }

    pub async fn import_succeeded(&mut self, task_id: task::Id) -> Result<()> {
        let mut tx = self.begin().await.context("begin db tx")?;

        task::set_status(&mut tx, task_id, task::Status::Completed)
            .await
            .context("set task_id={task_id:?} as import completed")?;

        let mut projects = project::list(tx.as_mut()).await.context("list projects")?;

        let task = task::query(tx.as_mut(), task::query::Params::default().id(task_id))
            .await
            .context("query task")?
            .tasks
            .into_iter()
            .next()
            .ok_or_eyre("task is missing")?;

        self.queue
            .remove_blockers(&mut tx, task_id)
            .await
            .context("remove queue blockers for task_id={task_id:?}")?;

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
            .context("set task_id={task_id:?} as import failed")?;

        self.queue
            .add_blockers(&mut tx, task_id)
            .await
            .context("add queue blockers for task_id={task_id}")?;

        tx.commit().await.context("commit db tx")?;

        Ok(())
    }

    pub async fn retry_task(&mut self, task_id: task::Id) -> Result<()> {
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
        task::set_allocated_builder(&mut tx, task_id, None)
            .await
            .context("set builder")?;

        tx.commit().await.context("commit db tx")?;

        info!(%task_id, "task marked for retry");

        Ok(())
    }

    pub async fn cancel_task(&mut self, task_id: task::Id) -> Result<()> {
        let mut tx = self.begin().await.context("begin db tx")?;

        let task = task::query(tx.as_mut(), task::query::Params::default().id(task_id))
            .await
            .context("get task")?
            .tasks
            .into_iter()
            .next()
            .ok_or_eyre("task is missing")?;

        if !task.status.is_in_progress() {
            warn!(%task_id, status = %task.status, "Task isn't in progress and won't be cancelled");
            return Ok(());
        }

        if let Some(endpoint) = task.allocated_builder.filter(|_| task.status == task::Status::Building) {
            if let Some(builder) = self
                .builders
                .iter_mut()
                .find_map(|(key, value)| (key.endpoint == endpoint).then_some(value))
            {
                builder.cancel_build(&task).await.context("cancel build")?;
            } else {
                warn!(
                    builder = %endpoint,
                    task = %task.id,
                    build = task.build_id,
                    "Builder not connected while cancelling task"
                );
            }
        }

        task::set_status(&mut tx, task_id, task::Status::Cancelled)
            .await
            .context("set task_id={task_id:?} as cancelled")?;

        self.queue
            .add_blockers(&mut tx, task_id)
            .await
            .context("add queue blockers for task_id={task_id:?}")?;

        tx.commit().await.context("commit db tx")?;

        info!(%task_id, "Task successfully cancelled");

        Ok(())
    }

    pub async fn update_builder(&mut self, endpoint: endpoint::Id, message: builder::Message) -> Result<bool> {
        let builder = if let Some(builder) = self
            .builders
            .iter_mut()
            .find_map(|(key, value)| (key.endpoint == endpoint).then_some(value))
        {
            builder
        }
        // First time connecting after enrollment
        else {
            let mut conn = self.state.service_db.acquire().await?;
            let endpoint = Endpoint::get(conn.as_mut(), endpoint).await?;
            let account = Account::get(conn.as_mut(), endpoint.account).await?;

            if let Some((index, config)) = self
                .config
                .builders
                .iter()
                .enumerate()
                .find(|(_, config)| config.public_key.encode() == account.public_key)
            {
                let key = builder::Key {
                    index,
                    endpoint: endpoint.id,
                };

                self.builders.insert(key, Builder::new(endpoint.id, config));
                self.builders.get_mut(&key).unwrap()
            } else {
                bail!("builder not defined in config: {}", endpoint.id);
            }
        };

        if let Some(event) = builder.update(&self.state, message).await? {
            match event {
                builder::Event::Connected => {
                    return Ok(true);
                }
                builder::Event::Idle => {
                    return Ok(true);
                }
                builder::Event::BuildFailed { task_id } => {
                    let mut tx = self.begin().await.context("begin db tx")?;

                    self.queue
                        .add_blockers(&mut tx, task_id)
                        .await
                        .context("add queue blockers")?;

                    tx.commit().await.context("commit db tx")?;

                    return Ok(true);
                }
                builder::Event::BuildRequeued => {
                    return Ok(true);
                }
            }
        }

        Ok(false)
    }

    pub fn builder_status(&self, endpoint: &endpoint::Id) -> Option<builder::Status> {
        self.builders
            .iter()
            .find_map(|(key, value)| (key.endpoint == *endpoint).then_some(value))
            .map(Builder::status)
    }

    pub async fn builders(&self) -> Result<Vec<builder::Info>> {
        Ok(self.builders.values().map(Builder::info).collect())
    }

    #[tracing::instrument(skip_all)]
    pub fn config_reloaded(&mut self, config: Config) {
        // TODO: Do we want to refresh other stuff at runtime, like
        // allowed builders / etc? If so we will need to ensure we
        // can invalidate active connections / etc.
        self.config.build_sizes = config.build_sizes;

        info!("`build_sizes` reloaded");
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
