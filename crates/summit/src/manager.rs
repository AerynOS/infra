use std::{
    collections::{BTreeMap, BTreeSet, HashMap, VecDeque, btree_map},
    path::PathBuf,
    sync::Arc,
};

use color_eyre::eyre::{self, Context, OptionExt, Report, Result};
use itertools::Itertools;
use moss::db::meta;
use service::{Account, Endpoint, crypto::EncodedPublicKey, database::Transaction, endpoint, error};
use sqlx::{Sqlite, pool::PoolConnection};
use tokio::{sync::mpsc, task::spawn_blocking};
use tracing::{Span, debug, error, info, warn};

use crate::{Builder, Config, Project, Queue, State, builder, config::Size, profile, project, repository, task};

#[derive(Debug, Clone)]
pub enum Event {
    QueueRecomputed,
    TasksUpdated,
    BuildersUpdated,
}

pub struct Manager {
    pub state: Arc<State>,
    pub paused: bool,
    pub queue: Queue,
    builders: BTreeMap<builder::Key, Builder>,
    profile_dbs: HashMap<profile::Id, meta::Database>,
    repository_dbs: HashMap<repository::Id, meta::Database>,
    config: Config,
    event_sender: mpsc::Sender<Event>,
}

impl Manager {
    #[tracing::instrument(name = "load_manager", skip_all)]
    pub async fn load(config: Config, state: Arc<State>, event_sender: mpsc::Sender<Event>) -> Result<Self> {
        let mut conn = state.service_db().acquire().await?;

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
                        Builder::new(&endpoint, config),
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
            event_sender,
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

                        let tasks_updated = task::fix_and_create(&mut tx, self, &project, &repo, &repo_db)
                            .await
                            .context("create missing tasks")?;

                        tx.commit().await.context("commit tx")?;

                        Result::<_, Report>::Ok(tasks_updated)
                    } else {
                        Ok(false)
                    }
                };

                match refresh(profile_refreshed).await {
                    Ok(tasks_updated) => {
                        if tasks_updated {
                            let _ = self.event_sender.try_send(Event::TasksUpdated);

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
        Ok(self.state.service_db().begin().await?)
    }

    pub async fn acquire(&self) -> Result<PoolConnection<Sqlite>> {
        Ok(self.state.service_db().acquire().await?)
    }

    pub async fn projects(&self) -> Result<Vec<Project>> {
        Ok(project::list(&mut *self.state.service_db().acquire().await?).await?)
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

        let json_view = self
            .queue
            .recompute(&mut conn, &projects, &self.repository_dbs, &self.config.build_sizes)
            .await
            .context("recompute queue")?;

        self.state.queue_json_view.store(json_view);

        // Send event w/out backpressure (drop event otherwise)
        let _ = self.event_sender.try_send(Event::QueueRecomputed);

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

        let mut tasks_updated = false;

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
                            task_id = %task.task.id,
                            build_id = %task.task.build_id,
                            task_size = %task.size,
                            builder_size = %best_fit_builder_size,
                            %max_connected_builder_size,
                            "No builder is connected to handle this task size. Queueing it on the next largest builder."
                        );
                    }

                    match builder.build(task).await {
                        Ok(_) => {
                            let mut tx = self.state.service_db().begin().await?;

                            task::transition(
                                &mut tx,
                                task.task.id,
                                task::Transition::Building {
                                    builder: builder.endpoint,
                                },
                                &self.queue,
                            )
                            .await
                            .context(format!("transition task {} to building", task.task.id))?;

                            tx.commit().await?;

                            tasks_updated = true;

                            continue 'tasks;
                        }
                        Err(err) => {
                            warn!(
                                builder = %builder.endpoint,
                                error = error::chain(&*err),
                                "Failed to send build, trying next builder"
                            );
                        }
                    }
                } else {
                    debug!(
                        task_id = %task.task.id,
                        build_id = %task.task.build_id,
                        task_size = %task.size,
                        %max_connected_builder_size,
                        "Waiting on a builder large enough to queue up this task"
                    );

                    continue 'tasks;
                }
            }
        }

        if tasks_updated {
            let _ = self.event_sender.try_send(Event::TasksUpdated);
        }

        Ok(())
    }

    pub async fn import_succeeded(&mut self, task_id: task::Id) -> Result<()> {
        let mut tx = self.begin().await.context("begin db tx")?;

        let mut projects = project::list(tx.as_mut()).await.context("list projects")?;

        let task = task::get(tx.as_mut(), task_id).await.context("get task")?;

        // Transition the task to complete
        task::transition(&mut tx, task.id, task::Transition::Complete, &self.queue)
            .await
            .context(format!("transition task {} to complete", task.id))?;

        tx.commit().await.context("commit db tx")?;

        let _ = self.event_sender.try_send(Event::TasksUpdated);

        // Refresh profile since this task is now indexed by it
        {
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
        }

        Ok(())
    }

    pub async fn import_failed(&mut self, task_id: task::Id) -> Result<()> {
        let mut tx = self.begin().await.context("begin db tx")?;

        // Transition to failed
        task::transition(
            &mut tx,
            task_id,
            task::Transition::Failed {
                stashed_build_logs: None,
            },
            &self.queue,
        )
        .await
        .context(format!("transition task {task_id} to failed"))?;

        tx.commit().await.context("commit db tx")?;

        let _ = self.event_sender.try_send(Event::TasksUpdated);

        Ok(())
    }

    pub async fn retry_task(&mut self, task_id: task::Id) -> Result<()> {
        let mut tx = self.begin().await.context("begin db tx")?;

        // Transition to retried
        task::transition(&mut tx, task_id, task::Transition::Retry, &self.queue)
            .await
            .context(format!("transition task {task_id} to retried"))?;

        tx.commit().await.context("commit db tx")?;

        let _ = self.event_sender.try_send(Event::TasksUpdated);

        Ok(())
    }

    pub async fn cancel_task(&mut self, task_id: task::Id) -> Result<()> {
        let mut tx = self.begin().await.context("begin db tx")?;

        let task = task::get(tx.as_mut(), task_id).await.context("get task")?;

        // Send cancellation to builder, if connected & building
        if let Some(endpoint) = task.allocated_builder.filter(|_| task.status == task::Status::Building) {
            if let Some(builder) = self
                .builders
                .iter_mut()
                .find_map(|(key, value)| (key.endpoint == endpoint).then_some(value))
            {
                if let Err(err) = builder.cancel_build(&task).await {
                    error!(
                        builder = %endpoint,
                        task_id = %task.id,
                        build_id = task.build_id,
                        error = error::chain(&*err),
                        "Failed to send cancellation request to builder"
                    );
                }
            } else {
                warn!(
                    builder = %endpoint,
                    task_id = %task.id,
                    build_id = task.build_id,
                    "Builder not connected while cancelling task"
                );
            }
        }

        // Transition to cancelled
        task::transition(&mut tx, task_id, task::Transition::Cancelled, &self.queue)
            .await
            .context(format!("transition task {task_id} to cancelled"))?;

        tx.commit().await.context("commit db tx")?;

        let _ = self.event_sender.try_send(Event::TasksUpdated);

        info!(%task_id, "Task successfully cancelled");

        Ok(())
    }

    pub async fn update_builder(
        &mut self,
        endpoint: endpoint::Id,
        public_key: EncodedPublicKey,
        message: builder::Message,
    ) -> Result<bool> {
        let builder = get_or_init_builder(
            &mut self.builders,
            &self.config,
            self.state.service_db(),
            endpoint,
            public_key,
        )
        .await
        .context("init builder")?;

        let event = builder.update(message).await;

        let _ = self.event_sender.try_send(Event::BuildersUpdated);

        if let Some(event) = event {
            match event {
                builder::Event::StatusChanged(status) => {
                    let mut reallocate_builds = false;
                    let mut tasks_updated = false;
                    let mut building = None;

                    match status {
                        builder::Status::Idle => {
                            reallocate_builds = true;
                        }
                        builder::Status::Busy => {}
                        builder::Status::Building { task } => {
                            building = Some(task);
                        }
                        builder::Status::Disconnected => {}
                    }

                    // Check for stuck / stale builds on this builder that dont
                    // match the current status and rebuild them.
                    {
                        let mut tx = self.state.service_db().begin().await?;

                        let query = task::query(
                            tx.as_mut(),
                            task::query::Params::default().statuses([task::Status::Building]),
                        )
                        .await
                        .context("query tasks")?;

                        for task in query.tasks {
                            // Skip tasks not on this builder
                            if task.allocated_builder.is_none_or(|id| id != builder.endpoint) {
                                continue;
                            }

                            // Skip the task currently being built
                            if building.is_some_and(|id| id == task.id) {
                                continue;
                            }

                            warn!(
                                task_id = %task.id,
                                build_id = %task.build_id,
                                builder = %builder.endpoint,
                                "Task stuck as building, but builder is no longer building it. Requeuing the task."
                            );

                            // This task isn't being built anymore but got stuck as building
                            // (builder crashed / shut down / etc), so requeue it
                            task::transition(&mut tx, task.id, task::Transition::Requeue, &self.queue)
                                .await
                                .context(format!("transition task {} to requeued", task.id))?;

                            // Pick this task back up on another builder
                            reallocate_builds = true;
                            tasks_updated = true;
                        }

                        tx.commit().await?;
                    }

                    if tasks_updated {
                        let _ = self.event_sender.try_send(Event::TasksUpdated);
                    }

                    return Ok(reallocate_builds);
                }
                builder::Event::BuildSucceeded {
                    task_id,
                    collectables,
                    log_path,
                } => {
                    let stashed_build_logs = stash_log(&self.state, log_path).await.context("stash logs")?;

                    let mut tx = self.state.service_db().begin().await?;

                    task::transition(
                        &mut tx,
                        task_id,
                        task::Transition::Publishing {
                            builder: builder.endpoint,
                            stashed_build_logs,
                        },
                        &self.queue,
                    )
                    .await
                    .context(format!("transition task {task_id} to publishing"))?;

                    tx.commit().await?;

                    if let Err(err) = builder.request_upload(&self.state, task_id, collectables).await {
                        error!(
                            %task_id,
                            builder = %builder.endpoint,
                            error = error::chain(&*err),
                            "Failed to send upload request to builder"
                        );

                        let mut tx = self.state.service_db().begin().await?;

                        task::transition(
                            &mut tx,
                            task_id,
                            task::Transition::Failed {
                                stashed_build_logs: None,
                            },
                            &self.queue,
                        )
                        .await
                        .context(format!("transition task {task_id} to failed"))?;

                        tx.commit().await?;
                    }

                    let _ = self.event_sender.try_send(Event::TasksUpdated);
                }
                builder::Event::BuildFailed { task_id, log_path } => {
                    let mut tx = self.state.service_db().begin().await?;

                    let stashed_build_logs = if let Some(path) = log_path {
                        Some(stash_log(&self.state, path).await.context("stash logs")?)
                    } else {
                        None
                    };

                    task::transition(
                        &mut tx,
                        task_id,
                        task::Transition::Failed { stashed_build_logs },
                        &self.queue,
                    )
                    .await
                    .context(format!("transition task {task_id} to failed"))?;

                    tx.commit().await?;

                    let _ = self.event_sender.try_send(Event::TasksUpdated);
                }
                builder::Event::BuildRejected { task_id } => {
                    let mut tx = self.state.service_db().begin().await?;

                    task::transition(&mut tx, task_id, task::Transition::Requeue, &self.queue)
                        .await
                        .context(format!("transition task {task_id} to requeued"))?;

                    tx.commit().await?;

                    let _ = self.event_sender.try_send(Event::TasksUpdated);

                    // Task needs to be queued up on a new builder
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

    pub fn refresh_cached_builder_info(&self) {
        self.state
            .builders
            .store(Arc::new(self.builders.values().map(Builder::info).collect()));
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

    let parent = state.service.db_dir.join("profile");

    fs::create_dir_all(&parent).context("create profile db directory")?;

    let db =
        meta::Database::new(parent.join(profile.to_string()).to_string_lossy().as_ref()).context("open profile db")?;

    Ok(db)
}

fn connect_repository_db(state: &State, repository: &repository::Id) -> Result<meta::Database> {
    use std::fs;

    let parent = state.service.db_dir.join("repository");

    fs::create_dir_all(&parent).context("create repository db directory")?;

    let db = meta::Database::new(parent.join(repository.to_string()).to_string_lossy().as_ref())
        .context("open repository db")?;

    Ok(db)
}

async fn stash_log(state: &State, path: PathBuf) -> Result<PathBuf> {
    let compressed_path = spawn_blocking(move || {
        use flate2::write::GzEncoder;
        use std::fs::{self, File};
        use std::io::{self, Write};

        let mut plain_file = File::open(&path).context("open plain file")?;

        let gz_path = PathBuf::from(format!("{}.gz", path.display()));
        let mut gz_file = File::create(&gz_path).context("create compressed file")?;

        let mut encoder = GzEncoder::new(&mut gz_file, flate2::Compression::new(9));

        io::copy(&mut plain_file, &mut encoder)?;

        encoder.finish()?;
        gz_file.flush()?;

        fs::remove_file(path).context("remove plain file")?;

        Result::<_, Report>::Ok(gz_path)
    })
    .await
    .context("join handle")?
    .context("compress log file")?;

    let relative_path = compressed_path
        .strip_prefix(state.state_dir())
        .context("log is descendent of state dir")?;

    Ok(relative_path.to_owned())
}

async fn get_or_init_builder<'a>(
    builders: &'a mut BTreeMap<builder::Key, Builder>,
    config: &Config,
    service_db: &service::Database,
    endpoint_id: endpoint::Id,
    public_key: EncodedPublicKey,
) -> Result<&'a mut Builder> {
    let (index, builder_config) = config
        .builders
        .iter()
        .enumerate()
        .find(|(_, config)| config.public_key.encode() == public_key)
        .ok_or_eyre(format!("builder not defined in config: {public_key}"))?;

    let key = builder::Key {
        index,
        endpoint: endpoint_id,
    };

    match builders.entry(key) {
        btree_map::Entry::Vacant(entry) => {
            let endpoint = Endpoint::get(service_db.acquire().await?.as_mut(), endpoint_id).await?;

            Ok(entry.insert(Builder::new(&endpoint, builder_config)))
        }
        btree_map::Entry::Occupied(entry) => Ok(entry.into_mut()),
    }
}
