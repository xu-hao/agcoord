use crate::error::{AppError, Result};
use crate::host;
use crate::platform::{
    OwnerLock, process_group_exists, same_worker_process, signal_process_group,
    worker_identity_conflicts,
};
use crate::store::{
    Paths, RunRecord, allocations, blocked_by, connect, initialize_native, load_run, load_runs,
    maintain_child_cpu_leases, map_database_error, now, validate_child_cpu_leases,
};
use crate::worker::{NativeWorker, PendingWorker, WorkerFault, WorkerSetup};
use crate::{cgroup, project_quota, resources};
use rusqlite::{Connection, params};
use serde_json::{Map, Value, json};
use signal_hook::consts::signal::{SIGINT, SIGTERM};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::OpenOptions;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

const OWNER_LOCK_STARTUP_RETRY: Duration = Duration::from_millis(250);

const POLL_INTERVAL: Duration = Duration::from_millis(25);
const CANCEL_GRACE: Duration = Duration::from_secs(2);

#[derive(Debug)]
pub struct ServeOptions {
    pub state_dir: PathBuf,
    pub capacities: BTreeMap<String, u64>,
    pub idle_timeout: Option<Duration>,
    pub host_preflight: Option<host::PreflightOptions>,
    pub crash_after: Option<String>,
    pub worker_fault: Option<WorkerFault>,
    pub cgroup_fixture: Option<PathBuf>,
    pub project_quota_fixture: Option<PathBuf>,
}

pub struct Broker {
    paths: Paths,
    capacities: BTreeMap<String, u64>,
    idle_timeout: Option<Duration>,
    managed_host: bool,
    crash_after: Option<String>,
    worker_fault: Option<WorkerFault>,
    resource_capabilities: Value,
    cgroup_backend: Option<cgroup::CgroupBackend>,
    project_quota_backend: Option<project_quota::ProjectQuotaBackend>,
    _owner: OwnerLock,
    children: HashMap<String, NativeWorker>,
    cancellation_started: HashMap<String, Instant>,
    group_drain_started: HashMap<String, Instant>,
    last_repository: Option<String>,
    stopped: Arc<AtomicBool>,
    idle_since: Option<Instant>,
}

impl Broker {
    fn worker_command(&self, run: &RunRecord) -> Result<Vec<String>> {
        if run.kind != "land" {
            return Ok(run.command.clone());
        }
        let python = run
            .environment
            .get("_AGCOORD_LAND_PYTHON")
            .filter(|path| Path::new(path).is_absolute())
            .ok_or_else(|| {
                AppError::new(
                    "broker-worker-start-failed",
                    "land worker Python is missing or no longer an absolute file",
                )
            })?;
        let adapter = run.publication_adapter.as_deref().ok_or_else(|| {
            AppError::new("broker-row-invalid", "land run has no publication adapter")
        })?;
        let request = run.publication_request.as_ref().ok_or_else(|| {
            AppError::new("broker-row-invalid", "land run has no publication request")
        })?;
        let head = run
            .head_sha
            .as_deref()
            .ok_or_else(|| AppError::new("broker-row-invalid", "land run has no exact head"))?;
        let mut command = vec![
            python.clone(),
            "-m".to_owned(),
            "agcoord.land".to_owned(),
            "--run-id".to_owned(),
            run.run_id.clone(),
            "--state-dir".to_owned(),
            self.paths.state_dir.to_string_lossy().into_owned(),
        ];
        command.extend([
            "--checkout".to_owned(),
            run.checkout.to_string_lossy().into_owned(),
            "--branch".to_owned(),
            run.branch.clone(),
            "--head-sha".to_owned(),
            head.to_owned(),
            "--adapter".to_owned(),
            adapter.to_owned(),
            "--request-json".to_owned(),
            serde_json::to_string(request).map_err(|error| {
                AppError::new(
                    "broker-row-invalid",
                    format!("cannot encode land publication request: {error}"),
                )
            })?,
            "--".to_owned(),
        ]);
        command.extend(run.command.clone());
        Ok(command)
    }
    pub fn start(options: ServeOptions) -> Result<Self> {
        let managed_host = options.host_preflight.is_some();
        if let Some(preflight) = &options.host_preflight {
            host::preflight(preflight)?;
        }
        let paths = Paths::new(&options.state_dir);
        paths.prepare()?;
        let paths = paths.configured()?;
        let resource_configuration = resources::load_configuration(&options.state_dir)?;
        let capacities = if options.capacities.is_empty() {
            resource_configuration.capacities.clone()
        } else {
            options.capacities.clone()
        };
        if capacities.is_empty()
            || capacities.values().any(|units| *units == 0)
            || !capacities.contains_key("jobs")
        {
            return Err(AppError::new(
                "broker-config-invalid",
                "capacities require a positive jobs entry",
            ));
        }
        let mut capability_map = Map::new();
        let referenced_backends: BTreeSet<String> = resource_configuration
            .bindings
            .values()
            .filter_map(|binding| binding.backend.clone())
            .collect();
        if options.cgroup_fixture.is_some()
            && !referenced_backends.contains(resources::CGROUP_BACKEND)
        {
            return Err(AppError::new(
                "broker-config-invalid",
                "cgroup fixture requires at least one cgroup-v2 binding",
            ));
        }
        if options.project_quota_fixture.is_some()
            && !referenced_backends.contains(project_quota::PROJECT_QUOTA_BACKEND)
        {
            return Err(AppError::new(
                "broker-config-invalid",
                "project quota fixture requires at least one project-quota binding",
            ));
        }
        let mut cgroup_backend = if referenced_backends.contains(resources::CGROUP_BACKEND) {
            Some(
                cgroup::CgroupBackend::new(
                    &resource_configuration,
                    &options.state_dir,
                    options.cgroup_fixture.as_deref(),
                )
                .map_err(|error| {
                    AppError::new(
                        "broker-config-invalid",
                        format!("cannot initialize cgroup-v2 backend: {}", error.code),
                    )
                })?,
            )
        } else {
            None
        };
        let project_quota_backend =
            if referenced_backends.contains(project_quota::PROJECT_QUOTA_BACKEND) {
                Some(
                    project_quota::ProjectQuotaBackend::new(
                        &paths.state_dir,
                        options.project_quota_fixture.as_deref(),
                    )
                    .map_err(|error| {
                        AppError::new(
                            "broker-config-invalid",
                            format!("cannot initialize project-quota backend: {}", error.code),
                        )
                    })?,
                )
            } else {
                None
            };
        for backend in &referenced_backends {
            let capability = if backend == resources::CGROUP_BACKEND {
                cgroup_backend.as_mut().unwrap().capability()
            } else if backend == project_quota::PROJECT_QUOTA_BACKEND {
                project_quota_backend.as_ref().unwrap().capability()
            } else {
                resources::Capability::unavailable("backend-unavailable")
            };
            capability_map.insert(backend.clone(), capability.to_value());
        }
        let resource_capabilities = Value::Object(capability_map);
        let mut owner =
            OwnerLock::acquire_with_retry(&options.state_dir, OWNER_LOCK_STARTUP_RETRY)?;
        if options.crash_after.as_deref() == Some("owner-lock") {
            std::process::exit(86);
        }
        let connection = initialize_native(&paths, &capacities)?;
        let runs = load_runs(&connection)?;
        validate_child_cpu_leases(&connection)?;
        maintain_child_cpu_leases(&connection)?;
        if cgroup_backend.is_none()
            && runs
                .iter()
                .any(|run| run.resource_state.contains_key(resources::CGROUP_BACKEND))
        {
            return Err(AppError::new(
                "broker-row-invalid",
                "stored cgroup recovery state has no configured native backend",
            ));
        }
        if project_quota_backend.is_none()
            && runs.iter().any(|run| {
                run.resource_state
                    .contains_key(project_quota::PROJECT_QUOTA_BACKEND)
            })
        {
            return Err(AppError::new(
                "broker-row-invalid",
                "stored project quota recovery state has no configured native backend",
            ));
        }
        if let Some(backend) = &cgroup_backend {
            for run in &runs {
                let Some(record) = run.resource_state.get(resources::CGROUP_BACKEND) else {
                    continue;
                };
                let request = Self::cgroup_request(run, &record.resources)?;
                backend
                    .validate_recovery(&request, &record.handle)
                    .map_err(|error| {
                        AppError::new(
                            "broker-row-invalid",
                            format!(
                                "run {} has invalid cgroup recovery state: {}",
                                run.run_id, error.code
                            ),
                        )
                    })?;
            }
        }
        if let Some(backend) = &project_quota_backend {
            for run in &runs {
                let Some(record) = run.resource_state.get(project_quota::PROJECT_QUOTA_BACKEND)
                else {
                    continue;
                };
                let request = Self::project_quota_request(run, &record.resources)?;
                backend
                    .validate_recovery(&request, &record.handle)
                    .map_err(|error| {
                        AppError::new(
                            "broker-row-invalid",
                            format!(
                                "run {} has invalid project quota recovery state: {}",
                                run.run_id, error.code
                            ),
                        )
                    })?;
            }
        }
        let started_at = now(&connection)?;
        drop(connection);
        let capacities_json = serde_json::to_string(&capacities).map_err(|error| {
            AppError::new(
                "broker-config-invalid",
                format!("cannot encode capacities: {error}"),
            )
        })?;
        let bindings_json =
            serde_json::to_string(&resources::bindings_value(&resource_configuration.bindings))
                .map_err(|error| {
                    AppError::new(
                        "broker-config-invalid",
                        format!("cannot encode resource bindings: {error}"),
                    )
                })?;
        let capabilities_json = serde_json::to_string(&resource_capabilities).map_err(|error| {
            AppError::new(
                "broker-config-invalid",
                format!("cannot encode resource capabilities: {error}"),
            )
        })?;
        let stopped = Arc::new(AtomicBool::new(false));
        signal_hook::flag::register(SIGTERM, Arc::clone(&stopped)).map_err(|error| {
            AppError::new(
                "broker-signal-handler-failed",
                format!("cannot install SIGTERM handler: {error}"),
            )
        })?;
        signal_hook::flag::register(SIGINT, Arc::clone(&stopped)).map_err(|error| {
            AppError::new(
                "broker-signal-handler-failed",
                format!("cannot install SIGINT handler: {error}"),
            )
        })?;
        owner.publish(&format!(
            "pid={}\nprotocol=5\nimplementation=rust-native\nversion={}\nbuild={}\ncapacities={}\nresource_bindings={}\nresource_capabilities={}\nstarted_at={}\n",
            std::process::id(),
            env!("CARGO_PKG_VERSION"),
            env!("AGCOORD_BUILD_ID"),
            capacities_json,
            bindings_json,
            capabilities_json,
            started_at,
        ))?;
        Ok(Self {
            paths,
            capacities,
            idle_timeout: options.idle_timeout,
            managed_host,
            crash_after: options.crash_after,
            worker_fault: options.worker_fault,
            resource_capabilities,
            cgroup_backend,
            project_quota_backend,
            _owner: owner,
            children: HashMap::new(),
            cancellation_started: HashMap::new(),
            group_drain_started: HashMap::new(),
            last_repository: None,
            stopped,
            idle_since: None,
        })
    }

    fn crash(&self, point: &str) {
        if self.crash_after.as_deref() == Some(point) {
            std::process::exit(86);
        }
    }

    pub fn serve(mut self) -> Result<()> {
        loop {
            if self.stopped.load(Ordering::Relaxed) {
                self.graceful_shutdown()?;
                return Ok(());
            }
            match self.pump_once() {
                Ok(()) => {}
                Err(error) if error.code == "broker-database-busy" => {
                    thread::sleep(POLL_INTERVAL);
                    continue;
                }
                Err(error) => return Err(error),
            }
            if self.stopped.load(Ordering::Relaxed) {
                self.graceful_shutdown()?;
                return Ok(());
            }
            let connection = connect(&self.paths)?;
            let has_live: bool = connection
                .query_row(
                    "SELECT EXISTS(
                        SELECT 1 FROM runs WHERE status IN ('queued', 'running')
                    )",
                    [],
                    |row| row.get(0),
                )
                .map_err(map_database_error)?;
            drop(connection);
            if has_live {
                self.idle_since = None;
            } else {
                if let Some(idle_timeout) = self.idle_timeout {
                    let since = self.idle_since.get_or_insert_with(Instant::now);
                    if since.elapsed() >= idle_timeout {
                        return Ok(());
                    }
                }
            }
            thread::sleep(POLL_INTERVAL);
        }
    }

    fn validate_active(&self, active: &[RunRecord]) -> Result<()> {
        for (name, units) in allocations(active) {
            if units > self.capacities.get(&name).copied().unwrap_or(0) {
                return Err(AppError::new(
                    "broker-active-state-invalid",
                    format!("active allocation for {name} exceeds configured capacity"),
                ));
            }
        }
        let mut repositories: BTreeMap<&str, Vec<&RunRecord>> = BTreeMap::new();
        for run in active {
            repositories
                .entry(&run.repository_id)
                .or_default()
                .push(run);
        }
        for (repository, runs) in repositories {
            if runs.iter().any(|run| run.barrier) && runs.len() != 1 {
                return Err(AppError::new(
                    "broker-active-state-invalid",
                    format!("repository {repository} has overlapping barrier work"),
                ));
            }
        }
        Ok(())
    }

    fn pump_once(&mut self) -> Result<()> {
        let connection = connect(&self.paths)?;
        maintain_child_cpu_leases(&connection)?;
        let runs = load_runs(&connection)?;
        let active: Vec<_> = runs
            .iter()
            .filter(|run| run.status == "running")
            .cloned()
            .collect();
        self.validate_active(&active)?;
        for run in active {
            self.observe(&connection, &run)?;
        }
        maintain_child_cpu_leases(&connection)?;
        drop(connection);

        loop {
            let connection = connect(&self.paths)?;
            let runs = load_runs(&connection)?;
            let active: Vec<_> = runs
                .iter()
                .filter(|run| run.status == "running")
                .cloned()
                .collect();
            let queued: Vec<_> = runs
                .iter()
                .filter(|run| run.status == "queued")
                .cloned()
                .collect();
            self.validate_active(&active)?;
            let Some(next) = self.next_admissible(&active, &queued) else {
                return Ok(());
            };
            drop(connection);
            self.start_worker(&next)?;
            self.last_repository = Some(next.repository_id);
        }
    }

    fn next_admissible(&self, active: &[RunRecord], queued: &[RunRecord]) -> Option<RunRecord> {
        let mut heads = Vec::new();
        let mut seen = BTreeSet::new();
        for run in queued {
            if seen.insert(run.repository_id.clone()) {
                heads.push(run.clone());
            }
        }
        if let Some(last) = &self.last_repository {
            let (after, before): (Vec<_>, Vec<_>) =
                heads.into_iter().partition(|run| run.repository_id > *last);
            heads = after.into_iter().chain(before).collect();
        }
        heads
            .into_iter()
            .find(|run| blocked_by(run, active, queued, &self.capacities).is_empty())
    }

    fn observe(&mut self, connection: &Connection, run: &RunRecord) -> Result<()> {
        let cgroup_managed = run.resource_state.contains_key(resources::CGROUP_BACKEND);
        let resource_managed = !run.resource_state.is_empty();
        if resource_managed {
            let refreshed = load_run(connection, &run.run_id)?;
            self.capture_resource_usage(connection, &refreshed)?;
        }
        if self.children.contains_key(&run.run_id) {
            if run.cancel_requested {
                if resource_managed {
                    let refreshed = load_run(connection, &run.run_id)?;
                    let _ = self.cancel_resources(connection, &refreshed)?;
                }
                if !cgroup_managed {
                    Self::signal_cancel(&mut self.cancellation_started, run)?;
                }
            }
            let child = self.children.get_mut(&run.run_id).unwrap();
            let Some(exit) = child.try_wait().map_err(|error| {
                AppError::new(
                    "broker-worker-observation-failed",
                    format!("cannot observe worker {}: {error}", run.run_id),
                )
            })?
            else {
                return Ok(());
            };
            let exit_status = exit;
            if cgroup_managed {
                let refreshed = load_run(connection, &run.run_id)?;
                self.finish_and_cleanup_resources(connection, &refreshed)?;
            } else if !self.drain_finished_process_group(run)? {
                return Ok(());
            } else if resource_managed {
                let refreshed = load_run(connection, &run.run_id)?;
                self.finish_and_cleanup_resources(connection, &refreshed)?;
            }
            let run = load_run(connection, &run.run_id)?;
            let observed_exit = if run.kind == "land" {
                run.reported_exit_status.unwrap_or(exit_status)
            } else {
                exit_status
            };
            let (status, selected_exit, failure_reason) = if run.cancel_requested {
                ("cancelled", 130, None)
            } else if observed_exit == 0 {
                ("passed", 0, None)
            } else if Self::has_resource_observation(&run, "memory-oom") {
                ("failed", observed_exit, Some("memory-oom"))
            } else {
                ("failed", observed_exit, None)
            };
            self.finish_run(connection, &run, status, selected_exit, failure_reason)?;
            self.crash("terminal-commit");
            self.children.remove(&run.run_id);
            self.cancellation_started.remove(&run.run_id);
            self.group_drain_started.remove(&run.run_id);
            self.crash("worker-cleanup");
            return Ok(());
        }

        if same_worker_process(run.worker_pid, run.worker_start_token.as_deref()) {
            if run.cancel_requested {
                if resource_managed {
                    let refreshed = load_run(connection, &run.run_id)?;
                    let _ = self.cancel_resources(connection, &refreshed)?;
                }
                if !cgroup_managed {
                    Self::signal_cancel(&mut self.cancellation_started, run)?;
                }
            }
            return Ok(());
        }
        if cgroup_managed {
            let refreshed = load_run(connection, &run.run_id)?;
            self.finish_and_cleanup_resources(connection, &refreshed)?;
        } else if !worker_identity_conflicts(run.worker_pid, run.worker_start_token.as_deref())
            && !self.drain_finished_process_group(run)?
        {
            return Ok(());
        } else if resource_managed {
            let refreshed = load_run(connection, &run.run_id)?;
            self.finish_and_cleanup_resources(connection, &refreshed)?;
        }
        let run = load_run(connection, &run.run_id)?;
        let (status, exit_status, failure_reason) = if run.cancel_requested {
            ("cancelled", 130, None)
        } else if run.kind == "land" && run.reported_exit_status == Some(0) {
            ("passed", 0, None)
        } else if let ("land", Some(exit_status)) = (run.kind.as_str(), run.reported_exit_status) {
            ("failed", exit_status, None)
        } else {
            ("interrupted", 125, Some("worker-result-lost"))
        };
        self.finish_run(connection, &run, status, exit_status, failure_reason)?;
        self.cancellation_started.remove(&run.run_id);
        self.group_drain_started.remove(&run.run_id);
        Ok(())
    }

    fn drain_finished_process_group(&mut self, run: &RunRecord) -> Result<bool> {
        let Some(process_group) = run.worker_pid else {
            return Ok(true);
        };
        if !process_group_exists(process_group) {
            self.group_drain_started.remove(&run.run_id);
            return Ok(true);
        }
        let started = if run.cancel_requested {
            self.cancellation_started
                .entry(run.run_id.clone())
                .or_insert_with(Instant::now)
        } else {
            self.group_drain_started
                .entry(run.run_id.clone())
                .or_insert_with(Instant::now)
        };
        let signal = if started.elapsed() >= CANCEL_GRACE {
            libc::SIGKILL
        } else {
            libc::SIGTERM
        };
        signal_process_group(process_group, signal)?;
        if process_group_exists(process_group) {
            Ok(false)
        } else {
            self.group_drain_started.remove(&run.run_id);
            Ok(true)
        }
    }

    fn signal_cancel(
        cancellation_started: &mut HashMap<String, Instant>,
        run: &RunRecord,
    ) -> Result<()> {
        let Some(pid) = run.worker_pid else {
            return Ok(());
        };
        let started = cancellation_started
            .entry(run.run_id.clone())
            .or_insert_with(Instant::now);
        let signal = if started.elapsed() >= CANCEL_GRACE {
            libc::SIGKILL
        } else {
            libc::SIGTERM
        };
        signal_process_group(pid, signal)
    }

    fn finish_run(
        &self,
        connection: &Connection,
        run: &RunRecord,
        status: &str,
        exit_status: i64,
        failure_reason: Option<&str>,
    ) -> Result<()> {
        connection
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(map_database_error)?;
        let current = load_run(connection, &run.run_id)?;
        if current.status != "running"
            || current.worker_pid != run.worker_pid
            || current.worker_start_token != run.worker_start_token
        {
            return Err(AppError::new(
                "broker-worker-identity-mismatch",
                format!("run {} changed while its worker was observed", run.run_id),
            ));
        }
        let (status, exit_status, failure_reason) = if current.cancel_requested {
            ("cancelled", 130, None)
        } else {
            (status, exit_status, failure_reason)
        };
        connection
            .execute(
                "UPDATE runs SET status = ?1, phase = 'complete', finished_at = ?2,
                 exit_status = ?3, failure_reason = ?4, environment_json = '{}'
                 WHERE run_id = ?5",
                params![
                    status,
                    now(connection)?,
                    exit_status,
                    failure_reason,
                    run.run_id
                ],
            )
            .map_err(map_database_error)?;
        connection
            .execute_batch("COMMIT")
            .map_err(map_database_error)
    }

    fn run_bindings(run: &RunRecord) -> Result<BTreeMap<String, resources::Binding>> {
        let bindings = resources::parse_bindings(Some(&run.resource_contract)).map_err(|_| {
            AppError::new(
                "broker-row-invalid",
                format!("run {} has an invalid stored resource contract", run.run_id),
            )
        })?;
        if bindings.keys().collect::<BTreeSet<_>>() != run.resources.keys().collect::<BTreeSet<_>>()
        {
            return Err(AppError::new(
                "broker-row-invalid",
                format!(
                    "run {} resource contract does not match its request",
                    run.run_id
                ),
            ));
        }
        Ok(bindings)
    }

    fn cgroup_request(run: &RunRecord, names: &[String]) -> Result<cgroup::CgroupRequest> {
        let bindings = Self::run_bindings(run)?;
        let selected_resources = names
            .iter()
            .map(|name| {
                run.resources
                    .get(name)
                    .copied()
                    .map(|units| (name.clone(), units))
                    .ok_or_else(|| {
                        AppError::new(
                            "broker-row-invalid",
                            format!("run {} has inconsistent resource state", run.run_id),
                        )
                    })
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        let request = cgroup::CgroupRequest::new(&run.run_id, &selected_resources, &bindings);
        if request.names() != names {
            return Err(AppError::new(
                "broker-row-invalid",
                format!("run {} has inconsistent cgroup state", run.run_id),
            ));
        }
        Ok(request)
    }

    fn project_quota_request(
        run: &RunRecord,
        names: &[String],
    ) -> Result<project_quota::QuotaRequest> {
        let bindings = Self::run_bindings(run)?;
        let selected_resources = names
            .iter()
            .map(|name| {
                run.resources
                    .get(name)
                    .copied()
                    .map(|units| (name.clone(), units))
                    .ok_or_else(|| {
                        AppError::new(
                            "broker-row-invalid",
                            format!("run {} has inconsistent resource state", run.run_id),
                        )
                    })
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        let request = project_quota::QuotaRequest::new(&run.run_id, &selected_resources, &bindings);
        if request.names() != names {
            return Err(AppError::new(
                "broker-row-invalid",
                format!("run {} has inconsistent project quota state", run.run_id),
            ));
        }
        Ok(request)
    }

    fn quota_result<T>(
        result: std::result::Result<T, project_quota::QuotaError>,
    ) -> std::result::Result<T, cgroup::CgroupError> {
        result.map_err(|error| cgroup::CgroupError { code: error.code })
    }

    fn prepare_worker_setup(
        &mut self,
        connection: &Connection,
        run: &RunRecord,
    ) -> Result<(WorkerSetup, bool)> {
        let mut setup = WorkerSetup {
            apparmor_admitted: self.managed_host,
            ..WorkerSetup::default()
        };
        if let Some(record) = run.resource_state.get(resources::CGROUP_BACKEND) {
            let request = Self::cgroup_request(run, &record.resources)?;
            let backend = self.cgroup_backend.as_ref().ok_or_else(|| {
                AppError::new("broker-config-invalid", "cgroup-v2 backend is unavailable")
            })?;
            setup.isolate_cgroup = !backend.fixture();
            match backend.tmpfs_setup(&request, &record.handle) {
                Ok(tmpfs) => setup.tmpfs = tmpfs,
                Err(error) => {
                    let bindings = Self::run_bindings(run)?;
                    let affected: Vec<_> = record
                        .resources
                        .iter()
                        .filter(|name| matches!(bindings[*name].kind.as_str(), "tmpfs" | "inodes"))
                        .cloned()
                        .collect();
                    if affected.is_empty() {
                        return Err(AppError::new(
                            "broker-row-invalid",
                            format!("run {} failed unrelated worker setup", run.run_id),
                        ));
                    }
                    let mut receipt = run.resource_receipt.clone();
                    let required_failure = affected.iter().any(|name| bindings[name].required());
                    for name in affected {
                        Self::append_resource_event(
                            connection,
                            &mut receipt,
                            resources::CGROUP_BACKEND,
                            &name,
                            "attach",
                            if bindings[&name].required() {
                                "failed"
                            } else {
                                "unapplied"
                            },
                            &error.code,
                        )?;
                    }
                    Self::save_resource_records(connection, run, &receipt, &run.resource_state)?;
                    if required_failure {
                        return Ok((setup, false));
                    }
                }
            }
        }

        if let Some(record) = run.resource_state.get(project_quota::PROJECT_QUOTA_BACKEND) {
            let request = Self::project_quota_request(run, &record.resources)?;
            let result = self
                .project_quota_backend
                .as_ref()
                .ok_or_else(|| project_quota::QuotaError {
                    code: "backend-unavailable".to_owned(),
                })
                .and_then(|backend| backend.scratch_path(&request, &record.handle));
            match result {
                Ok(path) => setup.project_quota = Some(path),
                Err(error) => {
                    let bindings = Self::run_bindings(run)?;
                    let required_failure = record
                        .resources
                        .iter()
                        .any(|name| bindings[name].required());
                    let mut receipt = run.resource_receipt.clone();
                    for name in &record.resources {
                        Self::append_resource_event(
                            connection,
                            &mut receipt,
                            project_quota::PROJECT_QUOTA_BACKEND,
                            name,
                            "attach",
                            if bindings[name].required() {
                                "failed"
                            } else {
                                "unapplied"
                            },
                            &error.code,
                        )?;
                    }
                    let mut selected = BTreeMap::from([(
                        project_quota::PROJECT_QUOTA_BACKEND.to_owned(),
                        record.clone(),
                    )]);
                    self.cleanup_resource_records(connection, run, &mut receipt, &mut selected)?;
                    let cleanup_failed = Self::resource_event_exists(
                        &receipt,
                        project_quota::PROJECT_QUOTA_BACKEND,
                        None,
                        Some("cleanup"),
                        "cleanup-failed",
                    ) || receipt
                        .get("events")
                        .and_then(Value::as_array)
                        .is_some_and(|events| {
                            events.iter().any(|event| {
                                event["backend"] == project_quota::PROJECT_QUOTA_BACKEND
                                    && event["stage"] == "cleanup"
                                    && event["status"] == "failed"
                            })
                        });
                    let mut state = run.resource_state.clone();
                    state.remove(project_quota::PROJECT_QUOTA_BACKEND);
                    Self::save_resource_records(connection, run, &receipt, &state)?;
                    if required_failure || cleanup_failed {
                        return Ok((setup, false));
                    }
                }
            }
        }
        Ok((setup, true))
    }

    fn record_worker_setup(
        connection: &Connection,
        run: &RunRecord,
        setup: &WorkerSetup,
    ) -> Result<()> {
        if setup.tmpfs.is_none() && setup.project_quota.is_none() {
            return Ok(());
        }
        let bindings = Self::run_bindings(run)?;
        let mut receipt = run.resource_receipt.clone();
        if let Some(tmpfs) = &setup.tmpfs {
            let record = run
                .resource_state
                .get(resources::CGROUP_BACKEND)
                .ok_or_else(|| {
                    AppError::new(
                        "broker-row-invalid",
                        format!("run {} lost its cgroup resource state", run.run_id),
                    )
                })?;
            let applied_names: Vec<_> = record
                .resources
                .iter()
                .filter(|name| matches!(bindings[*name].kind.as_str(), "tmpfs" | "inodes"))
                .cloned()
                .collect();
            let applied = receipt
                .get_mut("applied")
                .and_then(Value::as_object_mut)
                .ok_or_else(|| {
                    AppError::new(
                        "broker-row-invalid",
                        format!("run {} has an invalid resource receipt", run.run_id),
                    )
                })?;
            for name in &applied_names {
                let units = match bindings[name].kind.as_str() {
                    "tmpfs" => tmpfs.size,
                    "inodes" => tmpfs.inodes,
                    _ => unreachable!(),
                };
                if units == 0 || units > run.resources[name] {
                    return Err(AppError::new(
                        "broker-row-invalid",
                        format!("run {} received an invalid tmpfs setup", run.run_id),
                    ));
                }
                applied.insert(name.clone(), json!(units));
            }
            for name in applied_names {
                Self::append_resource_event(
                    connection,
                    &mut receipt,
                    resources::CGROUP_BACKEND,
                    &name,
                    "attach",
                    "applied",
                    "tmpfs-mounted",
                )?;
            }
        }
        if setup.project_quota.is_some() {
            let record = run
                .resource_state
                .get(project_quota::PROJECT_QUOTA_BACKEND)
                .ok_or_else(|| {
                    AppError::new(
                        "broker-row-invalid",
                        format!("run {} lost its project quota state", run.run_id),
                    )
                })?;
            {
                let applied = receipt
                    .get_mut("applied")
                    .and_then(Value::as_object_mut)
                    .ok_or_else(|| {
                        AppError::new(
                            "broker-row-invalid",
                            format!("run {} has an invalid resource receipt", run.run_id),
                        )
                    })?;
                for name in &record.resources {
                    applied.insert(name.clone(), json!(run.resources[name]));
                }
            }
            for name in &record.resources {
                Self::append_resource_event(
                    connection,
                    &mut receipt,
                    project_quota::PROJECT_QUOTA_BACKEND,
                    name,
                    "attach",
                    "applied",
                    "quota-ready",
                )?;
            }
        }
        if setup.tmpfs.is_some() && setup.project_quota.is_some() {
            return Err(AppError::new(
                "broker-row-invalid",
                format!("run {} combined two scratch providers", run.run_id),
            ));
        }
        Self::save_resource_records(connection, run, &receipt, &run.resource_state)
    }

    fn record_worker_setup_failure(
        connection: &Connection,
        run: &RunRecord,
        code: &str,
    ) -> Result<bool> {
        if !resources::code_valid(code) {
            return Err(AppError::new(
                "broker-row-invalid",
                format!("run {} returned an invalid setup refusal", run.run_id),
            ));
        }
        let bindings = Self::run_bindings(run)?;
        let namespace_failure = code.starts_with("namespace-")
            || matches!(
                code,
                "controller-files-exposed" | "tmpfs-namespace-required"
            );
        let mut receipt = run.resource_receipt.clone();
        let mut required_failure = false;
        let mut recorded = false;
        if let Some(record) = run.resource_state.get(resources::CGROUP_BACKEND) {
            let affected: Vec<_> = record
                .resources
                .iter()
                .filter(|name| {
                    namespace_failure || matches!(bindings[*name].kind.as_str(), "tmpfs" | "inodes")
                })
                .cloned()
                .collect();
            required_failure |=
                namespace_failure || affected.iter().any(|name| bindings[name].required());
            if namespace_failure {
                let applied = receipt
                    .get_mut("applied")
                    .and_then(Value::as_object_mut)
                    .ok_or_else(|| {
                        AppError::new(
                            "broker-row-invalid",
                            format!("run {} has an invalid resource receipt", run.run_id),
                        )
                    })?;
                for name in &affected {
                    applied.remove(name);
                }
            }
            for name in affected {
                Self::append_resource_event(
                    connection,
                    &mut receipt,
                    resources::CGROUP_BACKEND,
                    &name,
                    "attach",
                    if bindings[&name].required() {
                        "failed"
                    } else {
                        "unapplied"
                    },
                    code,
                )?;
                recorded = true;
            }
        }
        if let Some(record) = run.resource_state.get(project_quota::PROJECT_QUOTA_BACKEND) {
            let applied = receipt
                .get_mut("applied")
                .and_then(Value::as_object_mut)
                .ok_or_else(|| {
                    AppError::new(
                        "broker-row-invalid",
                        format!("run {} has an invalid resource receipt", run.run_id),
                    )
                })?;
            for name in &record.resources {
                applied.remove(name);
            }
            for name in &record.resources {
                Self::append_resource_event(
                    connection,
                    &mut receipt,
                    project_quota::PROJECT_QUOTA_BACKEND,
                    name,
                    "attach",
                    if bindings[name].required() {
                        "failed"
                    } else {
                        "unapplied"
                    },
                    code,
                )?;
                recorded = true;
            }
            // Quota administration power must never be lent to user code, even when
            // the capacity bindings themselves are best effort.
            required_failure = true;
        }
        if !recorded {
            return Err(AppError::new(
                "broker-row-invalid",
                format!("run {} returned an unrelated setup refusal", run.run_id),
            ));
        }
        Self::save_resource_records(connection, run, &receipt, &run.resource_state)?;
        Ok(required_failure)
    }

    fn append_resource_event(
        connection: &Connection,
        receipt: &mut Value,
        backend: &str,
        resource: &str,
        stage: &str,
        status: &str,
        code: &str,
    ) -> Result<()> {
        let events = receipt
            .get_mut("events")
            .and_then(Value::as_array_mut)
            .ok_or_else(|| {
                AppError::new("broker-row-invalid", "resource receipt events are invalid")
            })?;
        events.push(json!({
            "at": now(connection)?,
            "backend": backend,
            "resource": resource,
            "stage": stage,
            "status": status,
            "code": code,
        }));
        Ok(())
    }

    fn resource_event_exists(
        receipt: &Value,
        backend: &str,
        resource: Option<&str>,
        stage: Option<&str>,
        code: &str,
    ) -> bool {
        receipt
            .get("events")
            .and_then(Value::as_array)
            .is_some_and(|events| {
                events.iter().any(|event| {
                    event.get("backend").and_then(Value::as_str) == Some(backend)
                        && resource.is_none_or(|name| {
                            event.get("resource").and_then(Value::as_str) == Some(name)
                        })
                        && stage.is_none_or(|name| {
                            event.get("stage").and_then(Value::as_str) == Some(name)
                        })
                        && event.get("code").and_then(Value::as_str) == Some(code)
                })
            })
    }

    fn has_resource_observation(run: &RunRecord, code: &str) -> bool {
        run.resource_receipt
            .get("events")
            .and_then(Value::as_array)
            .is_some_and(|events| {
                events
                    .iter()
                    .any(|event| event.get("code").and_then(Value::as_str) == Some(code))
            })
    }

    fn merge_measurement(
        connection: &Connection,
        run: &RunRecord,
        receipt: &mut Value,
        backend: &str,
        stage: &str,
        measurement: cgroup::Measurement,
    ) -> Result<bool> {
        if measurement
            .peak
            .keys()
            .any(|name| !run.resources.contains_key(name))
            || measurement.observations.iter().any(|observation| {
                !run.resources.contains_key(&observation.resource)
                    || !resources::code_valid(&observation.code)
            })
        {
            return Err(AppError::new(
                "broker-row-invalid",
                format!("run {} received invalid resource measurements", run.run_id),
            ));
        }
        let applied_names = receipt
            .get("applied")
            .and_then(Value::as_object)
            .ok_or_else(|| {
                AppError::new(
                    "broker-row-invalid",
                    format!("run {} has an invalid resource receipt", run.run_id),
                )
            })?
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>();
        let measurement = cgroup::Measurement {
            peak: measurement
                .peak
                .into_iter()
                .filter(|(name, _units)| applied_names.contains(name))
                .collect(),
            observations: measurement
                .observations
                .into_iter()
                .filter(|observation| applied_names.contains(&observation.resource))
                .collect(),
        };
        let mut changed = false;
        {
            let peak = receipt
                .get_mut("peak")
                .and_then(Value::as_object_mut)
                .ok_or_else(|| {
                    AppError::new(
                        "broker-row-invalid",
                        format!("run {} has an invalid resource receipt", run.run_id),
                    )
                })?;
            for (name, units) in measurement.peak {
                let previous = peak.get(&name).and_then(Value::as_u64);
                if previous.is_none_or(|previous| units > previous) {
                    peak.insert(name, json!(units));
                    changed = true;
                }
            }
        }
        for observation in measurement.observations {
            if Self::resource_event_exists(
                receipt,
                backend,
                Some(&observation.resource),
                None,
                &observation.code,
            ) {
                continue;
            }
            Self::append_resource_event(
                connection,
                receipt,
                backend,
                &observation.resource,
                stage,
                "recorded",
                &observation.code,
            )?;
            changed = true;
        }
        Ok(changed)
    }

    fn save_resource_records(
        connection: &Connection,
        run: &RunRecord,
        receipt: &Value,
        state: &BTreeMap<String, resources::BackendState>,
    ) -> Result<()> {
        if !resources::receipt_valid(receipt, &run.resources) {
            return Err(AppError::new(
                "broker-row-invalid",
                format!("run {} produced an invalid resource receipt", run.run_id),
            ));
        }
        connection
            .execute(
                "UPDATE runs SET resource_receipt_json = ?1, resource_state_json = ?2
                 WHERE run_id = ?3",
                params![
                    serde_json::to_string(receipt).unwrap(),
                    serde_json::to_string(&resources::backend_state_value(state)).unwrap(),
                    run.run_id,
                ],
            )
            .map_err(map_database_error)?;
        Ok(())
    }

    fn cleanup_resource_records(
        &mut self,
        connection: &Connection,
        run: &RunRecord,
        receipt: &mut Value,
        state: &mut BTreeMap<String, resources::BackendState>,
    ) -> Result<()> {
        let backends: Vec<_> = state.keys().cloned().collect();
        for backend_name in backends {
            let record = state.get(&backend_name).unwrap().clone();
            let result = if backend_name == resources::CGROUP_BACKEND {
                let request = Self::cgroup_request(run, &record.resources)?;
                self.cgroup_backend
                    .as_mut()
                    .ok_or_else(|| cgroup::CgroupError {
                        code: "backend-unavailable".to_owned(),
                    })
                    .and_then(|backend| backend.cleanup(&request, &record.handle))
            } else if backend_name == project_quota::PROJECT_QUOTA_BACKEND {
                let request = Self::project_quota_request(run, &record.resources)?;
                self.project_quota_backend
                    .as_mut()
                    .ok_or_else(|| cgroup::CgroupError {
                        code: "backend-unavailable".to_owned(),
                    })
                    .and_then(|backend| {
                        Self::quota_result(backend.cleanup(&request, &record.handle))
                    })
            } else {
                Err(cgroup::CgroupError {
                    code: "backend-unavailable".to_owned(),
                })
            };
            let (status, code) = match result {
                Ok(()) => ("recorded", "cleaned"),
                Err(ref error) => ("failed", error.code.as_str()),
            };
            for name in &record.resources {
                Self::append_resource_event(
                    connection,
                    receipt,
                    &backend_name,
                    name,
                    "cleanup",
                    status,
                    code,
                )?;
            }
            state.remove(&backend_name);
        }
        Ok(())
    }

    fn prepare_resources(&mut self, connection: &Connection, run: &RunRecord) -> Result<bool> {
        let mut receipt = run.resource_receipt.clone();
        let mut state = run.resource_state.clone();
        if !state.is_empty() {
            return Err(AppError::new(
                "broker-row-invalid",
                format!("run {} already has prepared resource state", run.run_id),
            ));
        }
        let bindings = Self::run_bindings(run)?;
        let capabilities = self.resource_capabilities.as_object().ok_or_else(|| {
            AppError::new("broker-config-invalid", "resource capabilities invalid")
        })?;
        let mut eligible: BTreeMap<String, Vec<String>> = BTreeMap::new();
        let mut required_failure = false;
        for name in run.resources.keys() {
            let binding = bindings.get(name).unwrap();
            if !binding.enforced() {
                continue;
            }
            let backend = binding.backend.as_deref().unwrap();
            let Some(issue) = resources::capability_issue(binding, capabilities.get(backend))
            else {
                eligible
                    .entry(backend.to_owned())
                    .or_default()
                    .push(name.clone());
                continue;
            };
            let status = if binding.required() {
                required_failure = true;
                "failed"
            } else {
                "unapplied"
            };
            Self::append_resource_event(
                connection,
                &mut receipt,
                backend,
                name,
                "probe",
                status,
                &issue,
            )?;
        }

        if !required_failure {
            for (backend_name, names) in eligible {
                let result = if backend_name == resources::CGROUP_BACKEND {
                    let request = Self::cgroup_request(run, &names)?;
                    self.cgroup_backend
                        .as_mut()
                        .ok_or_else(|| cgroup::CgroupError {
                            code: "backend-unavailable".to_owned(),
                        })
                        .and_then(|backend| backend.prepare(&request))
                } else if backend_name == project_quota::PROJECT_QUOTA_BACKEND {
                    let request = Self::project_quota_request(run, &names)?;
                    self.project_quota_backend
                        .as_mut()
                        .ok_or_else(|| cgroup::CgroupError {
                            code: "backend-unavailable".to_owned(),
                        })
                        .and_then(|backend| Self::quota_result(backend.prepare(&request)))
                } else {
                    Err(cgroup::CgroupError {
                        code: "backend-unavailable".to_owned(),
                    })
                };
                match result {
                    Ok(handle) => {
                        state.insert(
                            backend_name.clone(),
                            resources::BackendState {
                                handle,
                                resources: names.clone(),
                                finished: false,
                                cancelled: false,
                            },
                        );
                        for name in &names {
                            Self::append_resource_event(
                                connection,
                                &mut receipt,
                                &backend_name,
                                name,
                                "prepare",
                                "recorded",
                                "prepared",
                            )?;
                        }
                    }
                    Err(error) => {
                        let code = if resources::code_valid(&error.code) {
                            error.code
                        } else {
                            "prepare-failed".to_owned()
                        };
                        for name in &names {
                            let binding = bindings.get(name).unwrap();
                            let status = if binding.required() {
                                required_failure = true;
                                "failed"
                            } else {
                                "unapplied"
                            };
                            Self::append_resource_event(
                                connection,
                                &mut receipt,
                                &backend_name,
                                name,
                                "prepare",
                                status,
                                &code,
                            )?;
                        }
                    }
                }
            }
        }
        if required_failure {
            self.cleanup_resource_records(connection, run, &mut receipt, &mut state)?;
            Self::save_resource_records(connection, run, &receipt, &state)?;
            return Ok(false);
        }
        Self::save_resource_records(connection, run, &receipt, &state)?;
        Ok(true)
    }

    fn cancel_resources(&mut self, connection: &Connection, run: &RunRecord) -> Result<bool> {
        let mut receipt = run.resource_receipt.clone();
        let mut state = run.resource_state.clone();
        let mut changed = false;
        for backend_name in state.keys().cloned().collect::<Vec<_>>() {
            let record = state.get(&backend_name).unwrap().clone();
            if record.cancelled {
                continue;
            }
            let result = if backend_name == resources::CGROUP_BACKEND {
                let request = Self::cgroup_request(run, &record.resources)?;
                self.cgroup_backend
                    .as_mut()
                    .ok_or_else(|| cgroup::CgroupError {
                        code: "backend-unavailable".to_owned(),
                    })
                    .and_then(|backend| backend.cancel(&request, &record.handle))
            } else if backend_name == project_quota::PROJECT_QUOTA_BACKEND {
                let request = Self::project_quota_request(run, &record.resources)?;
                self.project_quota_backend
                    .as_ref()
                    .ok_or_else(|| cgroup::CgroupError {
                        code: "backend-unavailable".to_owned(),
                    })
                    .and_then(|backend| {
                        Self::quota_result(backend.cancel(&request, &record.handle))
                    })
            } else {
                Err(cgroup::CgroupError {
                    code: "backend-unavailable".to_owned(),
                })
            };
            let (status, code) = match result {
                Ok(()) => ("recorded", "cancelled"),
                Err(ref error) => ("failed", error.code.as_str()),
            };
            for name in &record.resources {
                Self::append_resource_event(
                    connection,
                    &mut receipt,
                    &backend_name,
                    name,
                    "cancel",
                    status,
                    code,
                )?;
            }
            state.get_mut(&backend_name).unwrap().cancelled = true;
            changed = true;
        }
        if changed {
            Self::save_resource_records(connection, run, &receipt, &state)?;
        }
        Ok(!state.is_empty())
    }

    fn capture_resource_usage(&mut self, connection: &Connection, run: &RunRecord) -> Result<()> {
        let mut receipt = run.resource_receipt.clone();
        let state = run.resource_state.clone();
        let mut changed = false;
        for (backend_name, record) in &state {
            if record.finished {
                continue;
            }
            let result = if backend_name == resources::CGROUP_BACKEND {
                let request = Self::cgroup_request(run, &record.resources)?;
                self.cgroup_backend
                    .as_mut()
                    .ok_or_else(|| cgroup::CgroupError {
                        code: "backend-unavailable".to_owned(),
                    })
                    .and_then(|backend| backend.usage(&request, &record.handle))
            } else if backend_name == project_quota::PROJECT_QUOTA_BACKEND {
                let request = Self::project_quota_request(run, &record.resources)?;
                self.project_quota_backend
                    .as_ref()
                    .ok_or_else(|| cgroup::CgroupError {
                        code: "backend-unavailable".to_owned(),
                    })
                    .and_then(|backend| Self::quota_result(backend.usage(&request, &record.handle)))
            } else {
                Err(cgroup::CgroupError {
                    code: "backend-unavailable".to_owned(),
                })
            };
            match result {
                Ok(measurement) => {
                    changed |= Self::merge_measurement(
                        connection,
                        run,
                        &mut receipt,
                        backend_name,
                        "usage",
                        measurement,
                    )?;
                }
                Err(error) => {
                    let code = if resources::code_valid(&error.code) {
                        error.code
                    } else {
                        "usage-failed".to_owned()
                    };
                    if Self::resource_event_exists(
                        &receipt,
                        backend_name,
                        None,
                        Some("usage"),
                        &code,
                    ) {
                        continue;
                    }
                    for name in &record.resources {
                        Self::append_resource_event(
                            connection,
                            &mut receipt,
                            backend_name,
                            name,
                            "usage",
                            "failed",
                            &code,
                        )?;
                    }
                    changed = true;
                }
            }
        }
        if changed {
            Self::save_resource_records(connection, run, &receipt, &state)?;
        }
        Ok(())
    }

    fn attach_resources(
        &mut self,
        connection: &Connection,
        run: &RunRecord,
        worker_pid: u32,
    ) -> Result<bool> {
        let mut receipt = run.resource_receipt.clone();
        let mut state = run.resource_state.clone();
        let bindings = Self::run_bindings(run)?;
        let mut required_failure = false;
        let mut failed = BTreeSet::new();
        for backend_name in state.keys().cloned().collect::<Vec<_>>() {
            let record = state.get(&backend_name).unwrap().clone();
            let result = if backend_name == resources::CGROUP_BACKEND {
                let request = Self::cgroup_request(run, &record.resources)?;
                self.cgroup_backend
                    .as_mut()
                    .ok_or_else(|| cgroup::CgroupError {
                        code: "backend-unavailable".to_owned(),
                    })
                    .and_then(|backend| backend.attach(&request, &record.handle, worker_pid))
            } else if backend_name == project_quota::PROJECT_QUOTA_BACKEND {
                let request = Self::project_quota_request(run, &record.resources)?;
                self.project_quota_backend
                    .as_ref()
                    .ok_or_else(|| cgroup::CgroupError {
                        code: "backend-unavailable".to_owned(),
                    })
                    .and_then(|backend| {
                        Self::quota_result(backend.attach(&request, &record.handle, worker_pid))
                    })
            } else {
                Err(cgroup::CgroupError {
                    code: "backend-unavailable".to_owned(),
                })
            };
            match result {
                Ok(()) => {
                    let applied_names: Vec<_> = record
                        .resources
                        .iter()
                        .filter(|name| {
                            backend_name != project_quota::PROJECT_QUOTA_BACKEND
                                && !matches!(bindings[*name].kind.as_str(), "inodes" | "tmpfs")
                        })
                        .cloned()
                        .collect();
                    {
                        let applied = receipt
                            .get_mut("applied")
                            .and_then(Value::as_object_mut)
                            .ok_or_else(|| {
                                AppError::new(
                                    "broker-row-invalid",
                                    format!("run {} has an invalid resource receipt", run.run_id),
                                )
                            })?;
                        for name in &applied_names {
                            applied.insert(name.clone(), json!(run.resources[name]));
                        }
                    }
                    for name in &applied_names {
                        Self::append_resource_event(
                            connection,
                            &mut receipt,
                            &backend_name,
                            name,
                            "attach",
                            "applied",
                            "applied",
                        )?;
                    }
                }
                Err(error) => {
                    let code = if resources::code_valid(&error.code) {
                        error.code
                    } else {
                        "attach-failed".to_owned()
                    };
                    for name in &record.resources {
                        let binding = bindings.get(name).unwrap();
                        let status = if binding.required() {
                            required_failure = true;
                            "failed"
                        } else {
                            "unapplied"
                        };
                        Self::append_resource_event(
                            connection,
                            &mut receipt,
                            &backend_name,
                            name,
                            "attach",
                            status,
                            &code,
                        )?;
                    }
                    failed.insert(backend_name);
                }
            }
        }
        if !failed.is_empty() {
            // Attachment can fail after the kernel accepted the PID. Kill through the
            // owned leaf before discarding any private handle.
            let temporary = RunRecord {
                resource_receipt: receipt.clone(),
                resource_state: state.clone(),
                ..run.clone()
            };
            let _ = self.cancel_resources(connection, &temporary)?;
            let refreshed = load_run(connection, &run.run_id)?;
            receipt = refreshed.resource_receipt;
            state = refreshed.resource_state;
            self.cleanup_resource_records(connection, run, &mut receipt, &mut state)?;
        }
        Self::save_resource_records(connection, run, &receipt, &state)?;
        Ok(!required_failure)
    }

    fn finish_and_cleanup_resources(
        &mut self,
        connection: &Connection,
        run: &RunRecord,
    ) -> Result<()> {
        let mut receipt = run.resource_receipt.clone();
        let mut state = run.resource_state.clone();
        for backend_name in state.keys().cloned().collect::<Vec<_>>() {
            let record = state.get(&backend_name).unwrap().clone();
            if record.finished {
                continue;
            }
            let result = if backend_name == resources::CGROUP_BACKEND {
                let request = Self::cgroup_request(run, &record.resources)?;
                self.cgroup_backend
                    .as_mut()
                    .ok_or_else(|| cgroup::CgroupError {
                        code: "backend-unavailable".to_owned(),
                    })
                    .and_then(|backend| backend.finish(&request, &record.handle))
            } else if backend_name == project_quota::PROJECT_QUOTA_BACKEND {
                let request = Self::project_quota_request(run, &record.resources)?;
                self.project_quota_backend
                    .as_ref()
                    .ok_or_else(|| cgroup::CgroupError {
                        code: "backend-unavailable".to_owned(),
                    })
                    .and_then(|backend| {
                        Self::quota_result(backend.finish(&request, &record.handle))
                    })
            } else {
                Err(cgroup::CgroupError {
                    code: "backend-unavailable".to_owned(),
                })
            };
            let (status, code, measurement) = match result {
                Ok(measurement) => ("recorded", "finished".to_owned(), Some(measurement)),
                Err(error) => ("failed", error.code, None),
            };
            if let Some(measurement) = measurement {
                let _ = Self::merge_measurement(
                    connection,
                    run,
                    &mut receipt,
                    &backend_name,
                    "finish",
                    measurement,
                )?;
            }
            for name in &record.resources {
                Self::append_resource_event(
                    connection,
                    &mut receipt,
                    &backend_name,
                    name,
                    "finish",
                    status,
                    &code,
                )?;
            }
            state.get_mut(&backend_name).unwrap().finished = true;
        }
        Self::save_resource_records(connection, run, &receipt, &state)?;
        let refreshed = load_run(connection, &run.run_id)?;
        receipt = refreshed.resource_receipt.clone();
        state = refreshed.resource_state.clone();
        self.cleanup_resource_records(connection, &refreshed, &mut receipt, &mut state)?;
        Self::save_resource_records(connection, &refreshed, &receipt, &state)
    }

    fn start_worker(&mut self, queued: &RunRecord) -> Result<()> {
        let connection = connect(&self.paths)?;
        connection
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(map_database_error)?;
        let current = load_run(&connection, &queued.run_id)?;
        if current.status != "queued" {
            connection
                .execute_batch("ROLLBACK")
                .map_err(map_database_error)?;
            return Ok(());
        }
        let phase = if current.kind == "land" {
            "preflight"
        } else {
            "running"
        };
        connection
            .execute(
                "UPDATE runs SET status = 'running', phase = ?1, started_at = ?2
                 WHERE run_id = ?3 AND status = 'queued'",
                params![phase, now(&connection)?, current.run_id],
            )
            .map_err(map_database_error)?;
        connection
            .execute_batch("COMMIT")
            .map_err(map_database_error)?;
        self.crash("admission-commit");

        let current = load_run(&connection, &queued.run_id)?;
        if current.cancel_requested {
            self.finish_run(&connection, &current, "cancelled", 130, None)?;
            return Ok(());
        }
        if !self.prepare_resources(&connection, &current)? {
            let refreshed = load_run(&connection, &current.run_id)?;
            self.finish_run(
                &connection,
                &refreshed,
                "failed",
                125,
                Some("resource-enforcement-failed"),
            )?;
            return Ok(());
        }
        let current = load_run(&connection, &current.run_id)?;
        let (worker_setup, setup_ready) = self.prepare_worker_setup(&connection, &current)?;
        if !setup_ready {
            let refreshed = load_run(&connection, &current.run_id)?;
            self.finish_and_cleanup_resources(&connection, &refreshed)?;
            let refreshed = load_run(&connection, &current.run_id)?;
            self.finish_run(
                &connection,
                &refreshed,
                "failed",
                125,
                Some("resource-enforcement-failed"),
            )?;
            return Ok(());
        }
        let log_path = self.paths.logs.join(format!("{}.log", current.run_id));
        let output = OpenOptions::new()
            .create(true)
            .append(true)
            .mode(0o600)
            .open(&log_path)
            .map_err(|error| {
                AppError::new(
                    "broker-worker-start-failed",
                    format!("cannot open worker log: {error}"),
                )
            })?;
        let mut environment = current.environment.clone();
        environment.retain(|name, _value| !name.starts_with("_AGCOORD_"));
        environment
            .entry("PATH".to_owned())
            .or_insert_with(|| "/usr/bin:/bin".to_owned());
        environment.insert("AGCOORD_RUN_ID".to_owned(), current.run_id.clone());
        environment.insert("AGCOORD_RUN_KIND".to_owned(), current.kind.clone());
        environment.insert(
            "AGCOORD_STATE_DIR".to_owned(),
            self.paths.state_dir.to_string_lossy().into_owned(),
        );
        if let Some(tmpfs) = &worker_setup.tmpfs {
            let target = tmpfs.target.to_string_lossy().into_owned();
            environment.insert("TMPDIR".to_owned(), target.clone());
            environment.insert("TMP".to_owned(), target.clone());
            environment.insert("TEMP".to_owned(), target);
        }
        if let Some(project_quota) = &worker_setup.project_quota {
            let target = project_quota.to_string_lossy().into_owned();
            environment.insert("TMPDIR".to_owned(), target.clone());
            environment.insert("TMP".to_owned(), target.clone());
            environment.insert("TEMP".to_owned(), target);
        }
        let worker_command = self.worker_command(&current)?;
        let mut pending = match PendingWorker::spawn(
            &worker_command,
            &environment,
            &current.checkout,
            &output,
            self.worker_fault,
            worker_setup.clone(),
        ) {
            Ok(worker) => worker,
            Err(error) => {
                let start_failure = error.code == "broker-worker-start-failed";
                let exit_status = if start_failure { 127 } else { 125 };
                let failure_reason = if start_failure {
                    "worker-start-failed"
                } else {
                    error.code
                };
                let refreshed = load_run(&connection, &current.run_id)?;
                if !refreshed.resource_state.is_empty() {
                    self.finish_and_cleanup_resources(&connection, &refreshed)?;
                }
                let refreshed = load_run(&connection, &current.run_id)?;
                self.finish_run(
                    &connection,
                    &refreshed,
                    "failed",
                    exit_status,
                    Some(failure_reason),
                )?;
                return Ok(());
            }
        };
        let pid = pending.pid();
        let token = pending.start_token().to_owned();
        loop {
            match connection
                .execute_batch("BEGIN IMMEDIATE")
                .map_err(map_database_error)
            {
                Ok(()) => break,
                Err(error) if error.code == "broker-database-busy" => {
                    thread::sleep(POLL_INTERVAL);
                }
                Err(error) => return Err(error),
            }
        }
        let identity_commit = (|| {
            let refreshed = load_run(&connection, &current.run_id)?;
            if refreshed.status != "running" || refreshed.worker_pid.is_some() {
                return Err(AppError::new(
                    "broker-worker-identity-mismatch",
                    format!(
                        "run {} changed before worker identity commit",
                        current.run_id
                    ),
                ));
            }
            connection
                .execute(
                    "UPDATE runs SET worker_pid = ?1, worker_start_token = ?2,
                     environment_json = '{}' WHERE run_id = ?3",
                    params![i64::from(pid), token, current.run_id],
                )
                .map_err(map_database_error)?;
            connection
                .execute_batch("COMMIT")
                .map_err(map_database_error)
        })();
        if let Err(error) = identity_commit {
            let _ = connection.execute_batch("ROLLBACK");
            return Err(error);
        }
        self.crash("worker-identity-commit");
        let attached = load_run(&connection, &current.run_id)?;
        if !self.attach_resources(&connection, &attached, pid)? {
            drop(pending);
            let refreshed = load_run(&connection, &current.run_id)?;
            self.finish_run(
                &connection,
                &refreshed,
                "failed",
                125,
                Some("resource-enforcement-failed"),
            )?;
            return Ok(());
        }
        let setup_failure = match pending.verify_setup() {
            Ok(code) => code,
            Err(error) => {
                let refreshed = load_run(&connection, &current.run_id)?;
                let quota_failure = refreshed
                    .resource_state
                    .contains_key(project_quota::PROJECT_QUOTA_BACKEND);
                if quota_failure {
                    let _ = Self::record_worker_setup_failure(&connection, &refreshed, error.code)?;
                }
                let refreshed = load_run(&connection, &current.run_id)?;
                let _ = self.cancel_resources(&connection, &refreshed)?;
                drop(pending);
                let refreshed = load_run(&connection, &current.run_id)?;
                self.finish_and_cleanup_resources(&connection, &refreshed)?;
                let refreshed = load_run(&connection, &current.run_id)?;
                self.finish_run(
                    &connection,
                    &refreshed,
                    "failed",
                    125,
                    Some(if quota_failure {
                        "resource-enforcement-failed"
                    } else {
                        error.code
                    }),
                )?;
                return Ok(());
            }
        };
        let refreshed = load_run(&connection, &current.run_id)?;
        if let Some(code) = setup_failure {
            if Self::record_worker_setup_failure(&connection, &refreshed, code)? {
                let refreshed = load_run(&connection, &current.run_id)?;
                let _ = self.cancel_resources(&connection, &refreshed)?;
                drop(pending);
                let refreshed = load_run(&connection, &current.run_id)?;
                self.finish_and_cleanup_resources(&connection, &refreshed)?;
                let refreshed = load_run(&connection, &current.run_id)?;
                self.finish_run(
                    &connection,
                    &refreshed,
                    "failed",
                    125,
                    Some("resource-enforcement-failed"),
                )?;
                return Ok(());
            }
        } else {
            Self::record_worker_setup(&connection, &refreshed, &worker_setup)?;
        }
        self.crash("worker-setup-commit");
        let refreshed = load_run(&connection, &current.run_id)?;
        if refreshed.cancel_requested {
            let _ = self.cancel_resources(&connection, &refreshed)?;
            drop(pending);
            let refreshed = load_run(&connection, &current.run_id)?;
            self.finish_and_cleanup_resources(&connection, &refreshed)?;
            let refreshed = load_run(&connection, &current.run_id)?;
            self.finish_run(&connection, &refreshed, "cancelled", 130, None)?;
            return Ok(());
        }
        let child = match pending.release() {
            Ok(child) => child,
            Err(error) => {
                let refreshed = load_run(&connection, &current.run_id)?;
                let _ = self.cancel_resources(&connection, &refreshed)?;
                let refreshed = load_run(&connection, &current.run_id)?;
                self.finish_and_cleanup_resources(&connection, &refreshed)?;
                let refreshed = load_run(&connection, &current.run_id)?;
                self.finish_run(&connection, &refreshed, "failed", 125, Some(error.code))?;
                return Ok(());
            }
        };
        self.crash("worker-release");
        self.children.insert(current.run_id, child);
        Ok(())
    }

    fn graceful_shutdown(&mut self) -> Result<()> {
        loop {
            let connection = connect(&self.paths)?;
            let request_cancellation = (|| {
                connection
                    .execute_batch("BEGIN IMMEDIATE")
                    .map_err(map_database_error)?;
                let timestamp = now(&connection)?;
                connection
                    .execute(
                        "UPDATE runs SET cancel_requested = 1, cancel_requested_at = ?1
                         WHERE status = 'running'
                           AND kind != 'merge'
                           AND NOT (kind = 'land' AND phase = 'publishing')",
                        params![timestamp],
                    )
                    .map_err(map_database_error)?;
                connection
                    .execute_batch("COMMIT")
                    .map_err(map_database_error)
            })();
            match request_cancellation {
                Ok(()) => break,
                Err(error) if error.code == "broker-database-busy" => {
                    thread::sleep(POLL_INTERVAL);
                }
                Err(error) => return Err(error),
            }
        }
        loop {
            match self.observe_active_once() {
                Ok(0) => return Ok(()),
                Ok(_) => {}
                Err(error) if error.code == "broker-database-busy" => {}
                Err(error) => return Err(error),
            }
            thread::sleep(POLL_INTERVAL);
        }
    }

    fn observe_active_once(&mut self) -> Result<i64> {
        let connection = connect(&self.paths)?;
        let runs = load_runs(&connection)?;
        let active: Vec<_> = runs
            .iter()
            .filter(|run| run.status == "running")
            .cloned()
            .collect();
        self.validate_active(&active)?;
        for run in &active {
            self.observe(&connection, run)?;
        }
        connection
            .query_row(
                "SELECT COUNT(*) FROM runs WHERE status = 'running'",
                [],
                |row| row.get(0),
            )
            .map_err(map_database_error)
    }
}
