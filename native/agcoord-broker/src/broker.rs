use crate::error::{AppError, Result};
use crate::platform::{
    OwnerLock, process_start_token, same_process, shell_exit_status, signal_process_group,
};
use crate::store::{
    Paths, RunRecord, allocations, blocked_by, connect, initialize_native, load_run, load_runs,
    map_database_error, now,
};
use rusqlite::{Connection, params};
use signal_hook::consts::signal::{SIGINT, SIGTERM};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::OpenOptions;
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::process::CommandExt;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

const POLL_INTERVAL: Duration = Duration::from_millis(25);
const CANCEL_GRACE: Duration = Duration::from_secs(2);

#[derive(Debug)]
pub struct ServeOptions {
    pub state_dir: PathBuf,
    pub capacities: BTreeMap<String, u64>,
    pub idle_timeout: Duration,
    pub crash_after: Option<String>,
}

pub struct Broker {
    paths: Paths,
    capacities: BTreeMap<String, u64>,
    idle_timeout: Duration,
    crash_after: Option<String>,
    _owner: OwnerLock,
    children: HashMap<String, Child>,
    cancellation_started: HashMap<String, Instant>,
    last_repository: Option<String>,
    stopped: Arc<AtomicBool>,
    idle_since: Option<Instant>,
}

impl Broker {
    pub fn start(options: ServeOptions) -> Result<Self> {
        if options.capacities.is_empty()
            || options.capacities.values().any(|units| *units == 0)
            || !options.capacities.contains_key("jobs")
        {
            return Err(AppError::new(
                "broker-config-invalid",
                "capacities require a positive jobs entry",
            ));
        }
        let paths = Paths::new(&options.state_dir);
        paths.prepare()?;
        let paths = paths.configured()?;
        let mut owner = OwnerLock::acquire(&options.state_dir)?;
        if options.crash_after.as_deref() == Some("owner-lock") {
            std::process::exit(86);
        }
        let connection = initialize_native(&paths, &options.capacities)?;
        load_runs(&connection)?;
        let started_at = now(&connection)?;
        drop(connection);
        let capacities_json = serde_json::to_string(&options.capacities).map_err(|error| {
            AppError::new(
                "broker-config-invalid",
                format!("cannot encode capacities: {error}"),
            )
        })?;
        owner.publish(&format!(
            "pid={}\nprotocol=5\nimplementation=rust-native\nversion={}\nbuild={}\ncapacities={}\nresource_bindings={{}}\nresource_capabilities={{}}\nstarted_at={}\n",
            std::process::id(),
            env!("CARGO_PKG_VERSION"),
            env!("AGCOORD_BUILD_ID"),
            capacities_json,
            started_at,
        ))?;
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
        Ok(Self {
            paths,
            capacities: options.capacities,
            idle_timeout: options.idle_timeout,
            crash_after: options.crash_after,
            _owner: owner,
            children: HashMap::new(),
            cancellation_started: HashMap::new(),
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
                let since = self.idle_since.get_or_insert_with(Instant::now);
                if since.elapsed() >= self.idle_timeout {
                    return Ok(());
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
        if let Some(child) = self.children.get_mut(&run.run_id) {
            if run.cancel_requested {
                Self::signal_cancel(&mut self.cancellation_started, run)?;
            }
            let Some(exit) = child.try_wait().map_err(|error| {
                AppError::new(
                    "broker-worker-observation-failed",
                    format!("cannot observe worker {}: {error}", run.run_id),
                )
            })?
            else {
                return Ok(());
            };
            let exit_status = shell_exit_status(exit);
            let (status, selected_exit, failure_reason) = if run.cancel_requested {
                ("cancelled", 130, None)
            } else if exit_status == 0 {
                ("passed", 0, None)
            } else {
                ("failed", exit_status, None)
            };
            self.finish_run(connection, run, status, selected_exit, failure_reason)?;
            self.crash("terminal-commit");
            self.children.remove(&run.run_id);
            self.cancellation_started.remove(&run.run_id);
            self.crash("worker-cleanup");
            return Ok(());
        }

        if same_process(run.worker_pid, run.worker_start_token.as_deref()) {
            if run.cancel_requested {
                Self::signal_cancel(&mut self.cancellation_started, run)?;
            }
            return Ok(());
        }
        let (status, exit_status, failure_reason) = if run.cancel_requested {
            ("cancelled", 130, None)
        } else {
            ("interrupted", 125, Some("worker-result-lost"))
        };
        self.finish_run(connection, run, status, exit_status, failure_reason)?;
        self.cancellation_started.remove(&run.run_id);
        Ok(())
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
        let error_output = output.try_clone().map_err(|error| {
            AppError::new(
                "broker-worker-start-failed",
                format!("cannot duplicate worker log: {error}"),
            )
        })?;
        let mut command = Command::new(&current.command[0]);
        command
            .args(&current.command[1..])
            .current_dir(&current.checkout)
            .env_clear()
            .envs(&current.environment)
            .env(
                "PATH",
                current
                    .environment
                    .get("PATH")
                    .map(String::as_str)
                    .unwrap_or("/usr/bin:/bin"),
            )
            .env("AGCOORD_RUN_ID", &current.run_id)
            .env("AGCOORD_RUN_KIND", &current.kind)
            .env("AGCOORD_STATE_DIR", &self.paths.state_dir)
            .stdin(Stdio::null())
            .stdout(Stdio::from(output))
            .stderr(Stdio::from(error_output))
            .process_group(0);
        let mut child = match command.spawn() {
            Ok(child) => child,
            Err(error) => {
                self.finish_run(
                    &connection,
                    &current,
                    "failed",
                    127,
                    Some("worker-start-failed"),
                )?;
                let _ = error;
                return Ok(());
            }
        };
        let pid = child.id();
        let mut token = process_start_token(pid);
        for _ in 0..20 {
            if token.is_some() {
                break;
            }
            thread::sleep(Duration::from_millis(5));
            token = process_start_token(pid);
        }
        let Some(token) = token else {
            let _ = signal_process_group(pid, libc::SIGKILL);
            let _ = child.wait();
            self.finish_run(
                &connection,
                &current,
                "failed",
                125,
                Some("worker-identity-unavailable"),
            )?;
            return Ok(());
        };
        loop {
            match connection
                .execute_batch("BEGIN IMMEDIATE")
                .map_err(map_database_error)
            {
                Ok(()) => break,
                Err(error) if error.code == "broker-database-busy" => {
                    thread::sleep(POLL_INTERVAL);
                }
                Err(error) => {
                    let _ = signal_process_group(pid, libc::SIGKILL);
                    let _ = child.wait();
                    return Err(error);
                }
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
                    "UPDATE runs SET worker_pid = ?1, worker_start_token = ?2 WHERE run_id = ?3",
                    params![i64::from(pid), token, current.run_id],
                )
                .map_err(map_database_error)?;
            connection
                .execute_batch("COMMIT")
                .map_err(map_database_error)
        })();
        if let Err(error) = identity_commit {
            let _ = connection.execute_batch("ROLLBACK");
            let _ = signal_process_group(pid, libc::SIGKILL);
            let _ = child.wait();
            return Err(error);
        }
        self.crash("worker-identity-commit");
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
