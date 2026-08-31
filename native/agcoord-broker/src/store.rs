use crate::error::{AppError, Result};
use crate::platform::{
    OwnerLock, live_owner_metadata, prepare_private_directory, same_process, sync_file,
};
use rusqlite::{Connection, ErrorCode, OpenFlags, OptionalExtension, Row, params};
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::time::Duration;

pub const PROTOCOL: u64 = 5;
pub const SCHEMA_FINGERPRINT: &str = "agcoord-spool-v5";
const DATABASE_TIMEOUT: Duration = Duration::from_secs(10);
const RECENT_LIMIT: usize = 50;

const RUNS_SCHEMA: &str = r#"
CREATE TABLE runs (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL CHECK (
        status IN ('queued', 'running', 'passed', 'failed', 'cancelled', 'interrupted')
    ),
    kind TEXT NOT NULL CHECK (kind IN ('check', 'full', 'merge', 'land')),
    phase TEXT NOT NULL CHECK (
        phase IN ('queued', 'running', 'preflight', 'gating', 'publishing', 'complete')
    ),
    label TEXT NOT NULL,
    agent TEXT NOT NULL,
    repository_id TEXT NOT NULL,
    repository TEXT NOT NULL,
    worktree_id TEXT NOT NULL,
    checkout TEXT NOT NULL,
    branch TEXT NOT NULL,
    head_sha TEXT,
    barrier INTEGER NOT NULL CHECK (barrier IN (0, 1)),
    resources_json TEXT NOT NULL,
    resource_contract_json TEXT NOT NULL,
    resource_receipt_json TEXT NOT NULL,
    resource_state_json TEXT NOT NULL,
    gate_run_id TEXT,
    publication_adapter TEXT,
    publication_request TEXT,
    failure_reason TEXT,
    gate_exit_status INTEGER,
    reported_exit_status INTEGER,
    caller_pid INTEGER NOT NULL,
    command_json TEXT NOT NULL,
    environment_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT,
    exit_status INTEGER,
    worker_pid INTEGER,
    worker_start_token TEXT,
    cancel_requested INTEGER NOT NULL DEFAULT 0,
    cancel_requested_at TEXT
);
CREATE INDEX runs_status_sequence ON runs(status, sequence);
CREATE INDEX runs_repository_sequence ON runs(repository_id, sequence);
CREATE TABLE coordinator_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
"#;

const LEASE_SCHEMA: &str = r#"
CREATE TABLE child_cpu_leases (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    lease_id TEXT NOT NULL UNIQUE,
    run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
    status TEXT NOT NULL CHECK (status IN ('waiting', 'active', 'released', 'cancelled')),
    requested INTEGER NOT NULL CHECK (requested > 0),
    minimum INTEGER NOT NULL CHECK (minimum > 0 AND minimum <= requested),
    granted INTEGER NOT NULL DEFAULT 0 CHECK (granted >= 0 AND granted <= requested),
    owner_pid INTEGER NOT NULL CHECK (owner_pid > 0),
    owner_start_token TEXT NOT NULL,
    bypass_count INTEGER NOT NULL DEFAULT 0 CHECK (bypass_count >= 0),
    created_at TEXT NOT NULL,
    acquired_at TEXT,
    finished_at TEXT
);
CREATE INDEX child_cpu_leases_run_sequence ON child_cpu_leases(run_id, sequence);
CREATE INDEX child_cpu_leases_status_sequence ON child_cpu_leases(status, sequence);
"#;

const RUNS_V2_SCHEMA: &str = r#"
CREATE TABLE runs_v2 (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL CHECK (
        status IN ('queued', 'running', 'passed', 'failed', 'cancelled', 'interrupted')
    ),
    kind TEXT NOT NULL CHECK (kind IN ('check', 'full', 'merge', 'land')),
    phase TEXT NOT NULL CHECK (
        phase IN ('queued', 'running', 'preflight', 'gating', 'publishing', 'complete')
    ),
    label TEXT NOT NULL,
    agent TEXT NOT NULL,
    repository_id TEXT NOT NULL,
    repository TEXT NOT NULL,
    worktree_id TEXT NOT NULL,
    checkout TEXT NOT NULL,
    branch TEXT NOT NULL,
    head_sha TEXT,
    barrier INTEGER NOT NULL CHECK (barrier IN (0, 1)),
    resources_json TEXT NOT NULL,
    gate_run_id TEXT,
    publication_adapter TEXT,
    publication_request TEXT,
    failure_reason TEXT,
    gate_exit_status INTEGER,
    reported_exit_status INTEGER,
    caller_pid INTEGER NOT NULL,
    command_json TEXT NOT NULL,
    environment_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT,
    exit_status INTEGER,
    worker_pid INTEGER,
    worker_start_token TEXT,
    cancel_requested INTEGER NOT NULL DEFAULT 0,
    cancel_requested_at TEXT
);
"#;

#[derive(Clone, Debug)]
pub struct Paths {
    pub state_dir: PathBuf,
    pub database: PathBuf,
    pub logs: PathBuf,
    database_timeout: Duration,
}

impl Paths {
    pub fn new(state_dir: &Path) -> Self {
        Self {
            state_dir: state_dir.to_owned(),
            database: state_dir.join("queue.sqlite3"),
            logs: state_dir.join("logs"),
            database_timeout: DATABASE_TIMEOUT,
        }
    }

    pub fn configured(mut self) -> Result<Self> {
        self.database_timeout = database_timeout(&self)?;
        Ok(self)
    }

    pub fn prepare(&self) -> Result<()> {
        prepare_private_directory(&self.state_dir)?;
        prepare_private_directory(&self.logs)
    }
}

#[derive(Clone, Debug)]
pub struct OwnerInfo {
    pub pid: u32,
    pub capacities: BTreeMap<String, u64>,
}

#[derive(Clone, Debug)]
pub struct SubmitRequest {
    pub run_id: String,
    pub kind: String,
    pub label: String,
    pub agent: String,
    pub repository_id: String,
    pub repository: String,
    pub worktree_id: String,
    pub checkout: PathBuf,
    pub branch: String,
    pub head_sha: Option<String>,
    pub resources: BTreeMap<String, u64>,
    pub gate_run_id: Option<String>,
    pub command: Vec<String>,
    pub environment: BTreeMap<String, String>,
}

#[derive(Clone, Debug)]
pub struct PhaseRequest {
    pub run_id: String,
    pub worker_pid: u32,
    pub worker_start_token: String,
    pub checkout: PathBuf,
    pub head_sha: String,
    pub phase: String,
    pub gate_exit_status: Option<i64>,
}

#[derive(Clone, Debug)]
pub struct RunRecord {
    pub sequence: i64,
    pub run_id: String,
    pub status: String,
    pub kind: String,
    pub phase: String,
    pub label: String,
    pub agent: String,
    pub repository_id: String,
    pub repository: String,
    pub worktree_id: String,
    pub checkout: PathBuf,
    pub branch: String,
    pub head_sha: Option<String>,
    pub barrier: bool,
    pub resources: BTreeMap<String, u64>,
    pub resource_contract: Value,
    pub resource_receipt: Value,
    pub gate_run_id: Option<String>,
    pub publication_adapter: Option<String>,
    pub publication_request: Option<Value>,
    pub failure_reason: Option<String>,
    pub gate_exit_status: Option<i64>,
    pub caller_pid: i64,
    pub command: Vec<String>,
    pub environment: BTreeMap<String, String>,
    pub created_at: String,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    pub exit_status: Option<i64>,
    pub worker_pid: Option<u32>,
    pub worker_start_token: Option<String>,
    pub cancel_requested: bool,
}

pub fn map_database_error(error: rusqlite::Error) -> AppError {
    let code = error.sqlite_error_code();
    if matches!(
        code,
        Some(ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked)
    ) {
        AppError::new(
            "broker-database-busy",
            "coordinator database is busy; retry the operation",
        )
    } else {
        AppError::new(
            "broker-database-error",
            format!("coordinator database operation failed: {error}"),
        )
    }
}

pub fn connect(paths: &Paths) -> Result<Connection> {
    let connection = Connection::open(&paths.database).map_err(map_database_error)?;
    connection
        .busy_timeout(paths.database_timeout)
        .map_err(map_database_error)?;
    connection
        .pragma_update(None, "foreign_keys", "ON")
        .map_err(map_database_error)?;
    Ok(connection)
}

fn database_timeout(paths: &Paths) -> Result<Duration> {
    let path = paths.state_dir.join("config.json");
    let text = match fs::read_to_string(&path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(DATABASE_TIMEOUT),
        Err(error) => {
            return Err(AppError::new(
                "broker-config-invalid",
                format!("cannot read broker configuration: {error}"),
            ));
        }
    };
    let document: Value = serde_json::from_str(&text).map_err(|_| {
        AppError::new(
            "broker-config-invalid",
            "broker configuration is not valid JSON",
        )
    })?;
    let object = document.as_object().ok_or_else(|| {
        AppError::new(
            "broker-config-invalid",
            "broker configuration must be one JSON object",
        )
    })?;
    let allowed = [
        "capacities",
        "bindings",
        "cgroup_root",
        "cgroup_io",
        "database_timeout",
    ];
    if let Some(unknown) = object.keys().find(|key| !allowed.contains(&key.as_str())) {
        return Err(AppError::new(
            "broker-config-invalid",
            format!("broker configuration has unknown key {unknown}"),
        ));
    }
    for section in ["capacities", "bindings"] {
        if object
            .get(section)
            .is_some_and(|value| !value.is_object() && !value.is_null())
        {
            return Err(AppError::new(
                "broker-config-invalid",
                format!("broker configuration section {section} must be a JSON object"),
            ));
        }
    }
    if let Some(root) = object.get("cgroup_root")
        && !root.is_null()
        && root.as_str().is_none_or(|value| value.trim().is_empty())
    {
        return Err(AppError::new(
            "broker-config-invalid",
            "broker configuration cgroup_root must be a non-empty string",
        ));
    }
    if let Some(io) = object.get("cgroup_io")
        && !io.is_null()
    {
        let io = io.as_object().ok_or_else(|| {
            AppError::new(
                "broker-config-invalid",
                "broker configuration cgroup_io must contain exactly paths",
            )
        })?;
        if io.len() != 1 || !io.contains_key("paths") {
            return Err(AppError::new(
                "broker-config-invalid",
                "broker configuration cgroup_io must contain exactly paths",
            ));
        }
        let paths = io["paths"]
            .as_array()
            .filter(|paths| !paths.is_empty())
            .ok_or_else(|| {
                AppError::new(
                    "broker-config-invalid",
                    "broker configuration cgroup_io paths must be a non-empty list",
                )
            })?;
        let mut identities = BTreeSet::new();
        for path in paths {
            let path = path
                .as_str()
                .filter(|path| !path.is_empty() && !path.contains('\0'))
                .map(Path::new)
                .filter(|path| path.is_absolute())
                .ok_or_else(|| {
                    AppError::new(
                        "broker-config-invalid",
                        "broker configuration cgroup_io paths must be absolute strings",
                    )
                })?;
            if !identities.insert(path.to_owned()) {
                return Err(AppError::new(
                    "broker-config-invalid",
                    "broker configuration cgroup_io paths must be unique",
                ));
            }
        }
    }
    let Some(value) = object.get("database_timeout") else {
        return Ok(DATABASE_TIMEOUT);
    };
    let seconds = value
        .as_f64()
        .filter(|seconds| seconds.is_finite() && *seconds > 0.0 && *seconds <= 2_147_483.647)
        .ok_or_else(|| {
            AppError::new(
                "broker-config-invalid",
                "database_timeout must be a positive finite number no greater than 2147483.647",
            )
        })?;
    Ok(Duration::from_secs_f64(seconds))
}

pub fn now(connection: &Connection) -> Result<String> {
    connection
        .query_row("SELECT strftime('%Y-%m-%dT%H:%M:%fZ', 'now')", [], |row| {
            row.get(0)
        })
        .map_err(map_database_error)
}

fn protocol(connection: &Connection) -> Result<u64> {
    let value: Option<String> = connection
        .query_row(
            "SELECT value FROM coordinator_meta WHERE key = 'protocol'",
            [],
            |row| row.get(0),
        )
        .optional()
        .map_err(|_| {
            AppError::new(
                "broker-schema-invalid",
                "coordinator database has no readable protocol metadata",
            )
        })?;
    value
        .ok_or_else(|| {
            AppError::new(
                "broker-schema-invalid",
                "coordinator database has no protocol value",
            )
        })?
        .parse()
        .map_err(|_| {
            AppError::new(
                "broker-schema-invalid",
                "coordinator database protocol is not an integer",
            )
        })
}

fn metadata(connection: &Connection, key: &str) -> Result<Option<String>> {
    connection
        .query_row(
            "SELECT value FROM coordinator_meta WHERE key = ?1",
            params![key],
            |row| row.get(0),
        )
        .optional()
        .map_err(map_database_error)
}

fn set_metadata(connection: &Connection, key: &str, value: &str) -> Result<()> {
    connection
        .execute(
            "INSERT INTO coordinator_meta(key, value) VALUES (?1, ?2)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![key, value],
        )
        .map_err(map_database_error)?;
    Ok(())
}

fn table_names(connection: &Connection) -> Result<BTreeSet<String>> {
    let mut statement = connection
        .prepare(
            "SELECT name FROM sqlite_master WHERE type = 'table'
             AND name IN ('runs', 'coordinator_meta', 'child_cpu_leases')",
        )
        .map_err(map_database_error)?;
    let rows = statement
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(map_database_error)?;
    rows.collect::<std::result::Result<BTreeSet<_>, _>>()
        .map_err(map_database_error)
}

fn validate_schema(connection: &Connection) -> Result<()> {
    if table_names(connection)?
        != BTreeSet::from([
            "child_cpu_leases".to_owned(),
            "coordinator_meta".to_owned(),
            "runs".to_owned(),
        ])
    {
        return Err(AppError::new(
            "broker-schema-invalid",
            "coordinator database is partial or missing required tables",
        ));
    }
    let mut statement = connection
        .prepare("PRAGMA table_info(runs)")
        .map_err(map_database_error)?;
    let columns = statement
        .query_map([], |row| row.get::<_, String>(1))
        .map_err(map_database_error)?
        .collect::<std::result::Result<BTreeSet<_>, _>>()
        .map_err(map_database_error)?;
    let required: BTreeSet<String> = [
        "sequence",
        "run_id",
        "status",
        "kind",
        "phase",
        "label",
        "agent",
        "repository_id",
        "repository",
        "worktree_id",
        "checkout",
        "branch",
        "head_sha",
        "barrier",
        "resources_json",
        "resource_contract_json",
        "resource_receipt_json",
        "resource_state_json",
        "gate_run_id",
        "publication_adapter",
        "publication_request",
        "failure_reason",
        "gate_exit_status",
        "reported_exit_status",
        "caller_pid",
        "command_json",
        "environment_json",
        "created_at",
        "started_at",
        "finished_at",
        "exit_status",
        "worker_pid",
        "worker_start_token",
        "cancel_requested",
        "cancel_requested_at",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect();
    let missing: Vec<_> = required.difference(&columns).cloned().collect();
    if !missing.is_empty() {
        return Err(AppError::new(
            "broker-schema-invalid",
            format!(
                "coordinator database is missing columns: {}",
                missing.join(", ")
            ),
        ));
    }
    Ok(())
}

pub fn initialize_native(paths: &Paths, capacities: &BTreeMap<String, u64>) -> Result<Connection> {
    paths.prepare()?;
    let mut connection = connect(paths)?;
    let tables = table_names(&connection)?;
    if tables.is_empty() {
        let transaction = connection.transaction().map_err(map_database_error)?;
        transaction
            .execute_batch(RUNS_SCHEMA)
            .map_err(map_database_error)?;
        transaction
            .execute_batch(LEASE_SCHEMA)
            .map_err(map_database_error)?;
        set_metadata(&transaction, "protocol", &PROTOCOL.to_string())?;
        set_metadata(&transaction, "owner_implementation", "rust-native")?;
        set_metadata(&transaction, "schema_fingerprint", SCHEMA_FINGERPRINT)?;
        set_metadata(&transaction, "native_gate_floor", "1")?;
        transaction.commit().map_err(map_database_error)?;
    } else {
        let selected_protocol = protocol(&connection)?;
        if selected_protocol != PROTOCOL {
            return Err(AppError::new(
                "broker-protocol-unsupported",
                format!(
                    "native broker requires protocol {PROTOCOL}; state uses {selected_protocol}"
                ),
            ));
        }
        validate_schema(&connection)?;
        if metadata(&connection, "owner_implementation")?.as_deref() != Some("rust-native")
            || metadata(&connection, "schema_fingerprint")?.as_deref() != Some(SCHEMA_FINGERPRINT)
        {
            return Err(AppError::new(
                "broker-schema-invalid",
                "protocol-5 owner metadata or schema fingerprint is invalid",
            ));
        }
        // Validate all client-authored rows before journal or activity metadata can change.
        load_runs(&connection)?;
    }
    connection
        .pragma_update(None, "journal_mode", "WAL")
        .map_err(map_database_error)?;
    let selected: String = connection
        .pragma_query_value(None, "journal_mode", |row| row.get(0))
        .map_err(map_database_error)?;
    if !selected.eq_ignore_ascii_case("wal") {
        return Err(AppError::new(
            "broker-wal-unavailable",
            "coordinator database refused WAL journal mode",
        ));
    }
    set_metadata(
        &connection,
        "capacities_json",
        &serde_json::to_string(capacities).map_err(|error| {
            AppError::new(
                "broker-config-invalid",
                format!("invalid capacities: {error}"),
            )
        })?,
    )?;
    set_metadata(&connection, "last_activity", &now(&connection)?)?;
    fs::set_permissions(&paths.database, fs::Permissions::from_mode(0o600)).map_err(|error| {
        AppError::new(
            "broker-state-invalid",
            format!("cannot protect coordinator database: {error}"),
        )
    })?;
    Ok(connection)
}

pub fn open_protocol5(paths: &Paths) -> Result<Connection> {
    if !paths.database.is_file() {
        return Err(AppError::new(
            "broker-state-missing",
            "coordinator database does not exist",
        ));
    }
    let connection = connect(paths)?;
    let selected = protocol(&connection)?;
    if selected != PROTOCOL {
        return Err(AppError::new(
            "broker-protocol-unsupported",
            format!("native client requires protocol {PROTOCOL}; state uses {selected}"),
        ));
    }
    validate_schema(&connection)?;
    Ok(connection)
}

fn parse_capacities(value: &str) -> Result<BTreeMap<String, u64>> {
    let capacities: BTreeMap<String, u64> = serde_json::from_str(value).map_err(|_| {
        AppError::new(
            "broker-owner-metadata-invalid",
            "live owner capacities are not valid JSON",
        )
    })?;
    if capacities.is_empty()
        || capacities
            .iter()
            .any(|(name, units)| !identifier_valid(name) || *units == 0 || *units > i64::MAX as u64)
    {
        return Err(AppError::new(
            "broker-owner-metadata-invalid",
            "live owner capacities are empty or non-positive",
        ));
    }
    Ok(capacities)
}

pub fn owner_info(paths: &Paths) -> Result<OwnerInfo> {
    let raw = live_owner_metadata(&paths.state_dir)?.ok_or_else(|| {
        AppError::new(
            "broker-not-running",
            "no native broker owns this state directory",
        )
    })?;
    let mut fields = BTreeMap::new();
    for line in raw.lines() {
        let (key, value) = line.split_once('=').ok_or_else(|| {
            AppError::new(
                "broker-owner-metadata-invalid",
                "live owner metadata contains an invalid line",
            )
        })?;
        if fields.insert(key, value).is_some() {
            return Err(AppError::new(
                "broker-owner-metadata-invalid",
                "live owner metadata contains a duplicate key",
            ));
        }
    }
    if fields.get("protocol") != Some(&"5") || fields.get("implementation") != Some(&"rust-native")
    {
        return Err(AppError::new(
            "broker-protocol-mismatch",
            "live owner is not the protocol-5 native broker",
        ));
    }
    let pid = fields
        .get("pid")
        .and_then(|value| value.parse::<u32>().ok())
        .filter(|pid| *pid > 0)
        .ok_or_else(|| {
            AppError::new("broker-owner-metadata-invalid", "live owner PID is invalid")
        })?;
    let capacities = parse_capacities(fields.get("capacities").ok_or_else(|| {
        AppError::new(
            "broker-owner-metadata-invalid",
            "live owner capacities are missing",
        )
    })?)?;
    Ok(OwnerInfo { pid, capacities })
}

fn parse_json<T: serde::de::DeserializeOwned>(
    value: String,
    run_id: &str,
    subject: &str,
) -> Result<T> {
    serde_json::from_str(&value).map_err(|_| {
        AppError::new(
            "broker-row-invalid",
            format!("run {run_id} has invalid stored {subject}"),
        )
    })
}

fn run_from_row(row: &Row<'_>) -> Result<RunRecord> {
    let run_id: String = row.get("run_id").map_err(map_database_error)?;
    let resources: BTreeMap<String, u64> = parse_json(
        row.get("resources_json").map_err(map_database_error)?,
        &run_id,
        "resources",
    )?;
    if resources.is_empty()
        || resources
            .iter()
            .any(|(name, units)| !identifier_valid(name) || *units == 0 || *units > i64::MAX as u64)
    {
        return Err(AppError::new(
            "broker-row-invalid",
            format!("run {run_id} has empty or non-positive resources"),
        ));
    }
    let command: Vec<String> = parse_json(
        row.get("command_json").map_err(map_database_error)?,
        &run_id,
        "command",
    )?;
    if command.is_empty()
        || command[0].is_empty()
        || command.iter().any(|argument| argument.contains('\0'))
    {
        return Err(AppError::new(
            "broker-row-invalid",
            format!("run {run_id} has an empty or invalid command"),
        ));
    }
    let environment: BTreeMap<String, String> = parse_json(
        row.get("environment_json").map_err(map_database_error)?,
        &run_id,
        "environment",
    )?;
    if environment.iter().any(|(name, value)| {
        name.is_empty()
            || name.contains('=')
            || name.contains('\0')
            || value.contains('\0')
            || name == "AGCOORD_RUN_ID"
    }) {
        return Err(AppError::new(
            "broker-row-invalid",
            format!("run {run_id} has an invalid stored environment"),
        ));
    }
    let publication_request = row
        .get::<_, Option<String>>("publication_request")
        .map_err(map_database_error)?
        .map(|value| parse_json(value, &run_id, "publication request"))
        .transpose()?;
    let worker_pid = row
        .get::<_, Option<i64>>("worker_pid")
        .map_err(map_database_error)?
        .map(|pid| {
            u32::try_from(pid).map_err(|_| {
                AppError::new(
                    "broker-row-invalid",
                    format!("run {run_id} has an invalid worker PID"),
                )
            })
        })
        .transpose()?;
    Ok(RunRecord {
        sequence: row.get("sequence").map_err(map_database_error)?,
        run_id: run_id.clone(),
        status: row.get("status").map_err(map_database_error)?,
        kind: row.get("kind").map_err(map_database_error)?,
        phase: row.get("phase").map_err(map_database_error)?,
        label: row.get("label").map_err(map_database_error)?,
        agent: row.get("agent").map_err(map_database_error)?,
        repository_id: row.get("repository_id").map_err(map_database_error)?,
        repository: row.get("repository").map_err(map_database_error)?,
        worktree_id: row.get("worktree_id").map_err(map_database_error)?,
        checkout: PathBuf::from(
            row.get::<_, String>("checkout")
                .map_err(map_database_error)?,
        ),
        branch: row.get("branch").map_err(map_database_error)?,
        head_sha: row.get("head_sha").map_err(map_database_error)?,
        barrier: row.get::<_, i64>("barrier").map_err(map_database_error)? != 0,
        resources,
        resource_contract: parse_json(
            row.get("resource_contract_json")
                .map_err(map_database_error)?,
            &run_id,
            "resource contract",
        )?,
        resource_receipt: parse_json(
            row.get("resource_receipt_json")
                .map_err(map_database_error)?,
            &run_id,
            "resource receipt",
        )?,
        gate_run_id: row.get("gate_run_id").map_err(map_database_error)?,
        publication_adapter: row.get("publication_adapter").map_err(map_database_error)?,
        publication_request,
        failure_reason: row.get("failure_reason").map_err(map_database_error)?,
        gate_exit_status: row.get("gate_exit_status").map_err(map_database_error)?,
        caller_pid: row.get("caller_pid").map_err(map_database_error)?,
        command,
        environment,
        created_at: row.get("created_at").map_err(map_database_error)?,
        started_at: row.get("started_at").map_err(map_database_error)?,
        finished_at: row.get("finished_at").map_err(map_database_error)?,
        exit_status: row.get("exit_status").map_err(map_database_error)?,
        worker_pid,
        worker_start_token: row.get("worker_start_token").map_err(map_database_error)?,
        cancel_requested: row
            .get::<_, i64>("cancel_requested")
            .map_err(map_database_error)?
            != 0,
    })
}

pub fn load_runs(connection: &Connection) -> Result<Vec<RunRecord>> {
    let mut statement = connection
        .prepare("SELECT * FROM runs ORDER BY sequence")
        .map_err(map_database_error)?;
    let mut rows = statement.query([]).map_err(map_database_error)?;
    let mut selected = Vec::new();
    while let Some(row) = rows.next().map_err(map_database_error)? {
        selected.push(run_from_row(row)?);
    }
    Ok(selected)
}

pub fn load_run(connection: &Connection, run_id: &str) -> Result<RunRecord> {
    let mut statement = connection
        .prepare("SELECT * FROM runs WHERE run_id = ?1")
        .map_err(map_database_error)?;
    let mut rows = statement
        .query(params![run_id])
        .map_err(map_database_error)?;
    let row = rows.next().map_err(map_database_error)?.ok_or_else(|| {
        AppError::new(
            "broker-run-unknown",
            format!("unknown coordinator run {run_id}"),
        )
    })?;
    run_from_row(row)
}

pub fn allocations(active: &[RunRecord]) -> BTreeMap<String, u64> {
    let mut selected = BTreeMap::new();
    for run in active {
        for (name, units) in &run.resources {
            let allocated = selected.entry(name.clone()).or_insert(0_u64);
            *allocated = allocated.saturating_add(*units);
        }
    }
    selected
}

pub fn blocked_by(
    run: &RunRecord,
    active: &[RunRecord],
    queued: &[RunRecord],
    capacities: &BTreeMap<String, u64>,
) -> Vec<String> {
    if run.status != "queued" {
        return Vec::new();
    }
    let same_active: Vec<_> = active
        .iter()
        .filter(|candidate| candidate.repository_id == run.repository_id)
        .collect();
    let earlier: Vec<_> = queued
        .iter()
        .filter(|candidate| {
            candidate.repository_id == run.repository_id && candidate.sequence < run.sequence
        })
        .collect();
    let mut reasons = Vec::new();
    if run.barrier {
        reasons.extend(same_active.iter().map(|candidate| {
            format!(
                "repository:{}:active:{}",
                run.repository_id, candidate.run_id
            )
        }));
        if let Some(candidate) = earlier.first() {
            reasons.push(format!(
                "repository:{}:fifo:{}",
                run.repository_id, candidate.run_id
            ));
        }
    } else if let Some(candidate) = same_active
        .iter()
        .copied()
        .chain(earlier.iter().copied())
        .find(|candidate| candidate.barrier)
    {
        reasons.push(format!(
            "repository:{}:barrier:{}",
            run.repository_id, candidate.run_id
        ));
    }
    let used = allocations(active);
    for (name, units) in &run.resources {
        if used.get(name).copied().unwrap_or(0) + units > capacities.get(name).copied().unwrap_or(0)
        {
            reasons.push(format!("resource:{name}"));
        }
    }
    reasons
}

pub fn public_run(
    paths: &Paths,
    run: &RunRecord,
    position: Option<usize>,
    blockers: Vec<String>,
) -> Value {
    let log_bytes = fs::metadata(paths.logs.join(format!("{}.log", run.run_id)))
        .map(|metadata| metadata.len())
        .unwrap_or(0);
    let publication = run.publication_adapter.as_ref().map(|adapter| {
        json!({
            "adapter": adapter,
            "request": run.publication_request.clone().unwrap_or(Value::Null),
        })
    });
    json!({
        "run_id": run.run_id,
        "sequence": run.sequence,
        "status": run.status,
        "kind": run.kind,
        "phase": run.phase,
        "label": run.label,
        "agent": run.agent,
        "repository_id": run.repository_id,
        "repository": run.repository,
        "worktree_id": run.worktree_id,
        "checkout": run.checkout,
        "branch": run.branch,
        "head_sha": run.head_sha,
        "barrier": run.barrier,
        "resources": run.resources,
        "resource_contract": run.resource_contract,
        "resource_receipt": run.resource_receipt,
        "blocked_by": blockers,
        "gate_run_id": run.gate_run_id,
        "publication": publication,
        "failure_reason": run.failure_reason,
        "gate_exit_status": run.gate_exit_status,
        "caller_pid": run.caller_pid,
        "command": run.command,
        "created_at": run.created_at,
        "started_at": run.started_at,
        "finished_at": run.finished_at,
        "exit_status": run.exit_status,
        "worker_pid": run.worker_pid,
        "cancel_requested": run.cancel_requested,
        "log_bytes": log_bytes,
        "position": position,
    })
}

pub fn snapshot(paths: &Paths) -> Result<Value> {
    let owner = owner_info(paths)?;
    let connection = open_protocol5(paths)?;
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
    let terminal: Vec<_> = runs
        .iter()
        .filter(|run| is_terminal(&run.status))
        .cloned()
        .collect();
    let mut selected_allocations = BTreeMap::new();
    let used = allocations(&active);
    for name in owner.capacities.keys() {
        selected_allocations.insert(name.clone(), used.get(name).copied().unwrap_or(0));
    }
    let queued_public: Vec<_> = queued
        .iter()
        .enumerate()
        .map(|(index, run)| {
            public_run(
                paths,
                run,
                Some(index + 1),
                blocked_by(run, &active, &queued, &owner.capacities),
            )
        })
        .collect();
    let recent: Vec<_> = terminal
        .iter()
        .rev()
        .take(RECENT_LIMIT)
        .map(|run| public_run(paths, run, None, Vec::new()))
        .collect();
    Ok(json!({
        "protocol": PROTOCOL,
        "broker_pid": owner.pid,
        "captured_at": now(&connection)?,
        "capacities": owner.capacities,
        "allocations": selected_allocations,
        "resource_bindings": {},
        "resource_capabilities": {},
        "active": active.iter().map(|run| public_run(paths, run, None, Vec::new())).collect::<Vec<_>>(),
        "queued": queued_public,
        "recent": recent,
    }))
}

pub fn status(paths: &Paths, run_id: &str) -> Result<Value> {
    let connection = open_protocol5(paths)?;
    let runs = load_runs(&connection)?;
    let run = runs
        .iter()
        .find(|run| run.run_id == run_id)
        .ok_or_else(|| {
            AppError::new(
                "broker-run-unknown",
                format!("unknown coordinator run {run_id}"),
            )
        })?;
    let active: Vec<_> = runs
        .iter()
        .filter(|candidate| candidate.status == "running")
        .cloned()
        .collect();
    let queued: Vec<_> = runs
        .iter()
        .filter(|candidate| candidate.status == "queued")
        .cloned()
        .collect();
    let position = queued
        .iter()
        .position(|candidate| candidate.run_id == run.run_id)
        .map(|index| index + 1);
    let capacities = live_owner_metadata(&paths.state_dir)?
        .map(|_| owner_info(paths).map(|owner| owner.capacities))
        .transpose()?
        .or_else(|| {
            metadata(&connection, "capacities_json")
                .ok()
                .flatten()
                .and_then(|value| serde_json::from_str(&value).ok())
        })
        .unwrap_or_default();
    Ok(public_run(
        paths,
        run,
        position,
        blocked_by(run, &active, &queued, &capacities),
    ))
}

fn identifier_valid(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-_.:".contains(&byte))
}

fn validate_identifier(value: &str, subject: &str) -> Result<()> {
    if !identifier_valid(value) {
        return Err(AppError::new(
            "broker-submission-invalid",
            format!("{subject} has an invalid value"),
        ));
    }
    Ok(())
}

fn validate_submit(request: &SubmitRequest, owner: &OwnerInfo) -> Result<()> {
    validate_identifier(&request.run_id, "run ID")?;
    validate_identifier(&request.repository_id, "repository ID")?;
    validate_identifier(&request.worktree_id, "worktree ID")?;
    if !matches!(request.kind.as_str(), "check" | "full" | "merge" | "land") {
        return Err(AppError::new(
            "broker-submission-invalid",
            "run kind is invalid",
        ));
    }
    if request.label.trim().is_empty()
        || request.repository.trim().is_empty()
        || request.branch.trim().is_empty()
        || request.command.is_empty()
        || request.resources.is_empty()
    {
        return Err(AppError::new(
            "broker-submission-invalid",
            "submission has an empty required field",
        ));
    }
    if request.command[0].is_empty()
        || request
            .command
            .iter()
            .any(|argument| argument.contains('\0'))
        || request.environment.iter().any(|(name, value)| {
            name.is_empty()
                || name.contains('=')
                || name.contains('\0')
                || value.contains('\0')
                || name == "AGCOORD_RUN_ID"
        })
    {
        return Err(AppError::new(
            "broker-submission-invalid",
            "submission command or environment is invalid",
        ));
    }
    if !request.checkout.is_absolute() || !request.checkout.is_dir() {
        return Err(AppError::new(
            "broker-submission-invalid",
            "checkout must be an existing absolute directory",
        ));
    }
    if request.kind != "check"
        && !request.head_sha.as_ref().is_some_and(|head| {
            head.len() == 40
                && head
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        })
    {
        return Err(AppError::new(
            "broker-submission-invalid",
            "barrier submissions require a lowercase 40-character head",
        ));
    }
    for (name, units) in &request.resources {
        validate_identifier(name, "resource name")?;
        if *units == 0
            || *units > i64::MAX as u64
            || *units > owner.capacities.get(name).copied().unwrap_or(0)
        {
            return Err(AppError::new(
                "broker-resource-unavailable",
                format!("resource {name} exceeds the live broker capacity"),
            ));
        }
    }
    Ok(())
}

fn resource_contract(resources: &BTreeMap<String, u64>) -> Value {
    let mut contract = Map::new();
    for name in resources.keys() {
        contract.insert(
            name.clone(),
            json!({
                "backend": null,
                "kind": "generic",
                "mode": "admission-only",
                "unit": "admission-unit",
            }),
        );
    }
    Value::Object(contract)
}

pub fn submit(paths: &Paths, request: &SubmitRequest) -> Result<Value> {
    let owner = owner_info(paths)?;
    validate_submit(request, &owner)?;
    let connection = open_protocol5(paths)?;
    connection
        .execute_batch("BEGIN IMMEDIATE")
        .map_err(map_database_error)?;
    if connection
        .query_row(
            "SELECT 1 FROM runs WHERE run_id = ?1",
            params![request.run_id],
            |_| Ok(()),
        )
        .optional()
        .map_err(map_database_error)?
        .is_some()
    {
        return Err(AppError::new(
            "broker-run-exists",
            format!("run {} already exists", request.run_id),
        ));
    }
    if request.kind == "merge" {
        let gate_run_id = request.gate_run_id.as_deref().ok_or_else(|| {
            AppError::new(
                "broker-gate-required",
                "merge submission requires an exact full-gate receipt",
            )
        })?;
        let gate = load_run(&connection, gate_run_id)?;
        let floor: i64 = metadata(&connection, "native_gate_floor")?
            .ok_or_else(|| {
                AppError::new(
                    "broker-schema-invalid",
                    "native gate generation metadata is missing",
                )
            })?
            .parse()
            .map_err(|_| {
                AppError::new(
                    "broker-schema-invalid",
                    "native gate generation metadata is invalid",
                )
            })?;
        if gate.sequence < floor {
            return Err(AppError::new(
                "stale-gate-verdict",
                "gate verdict predates the active native protocol generation",
            ));
        }
        if gate.kind != "full"
            || gate.status != "passed"
            || gate.exit_status != Some(0)
            || gate.repository_id != request.repository_id
            || gate.branch != request.branch
            || gate.head_sha != request.head_sha
        {
            return Err(AppError::new(
                "broker-gate-mismatch",
                "gate receipt does not match this repository, branch, and head",
            ));
        }
    }
    let timestamp = now(&connection)?;
    let contract = resource_contract(&request.resources);
    let receipt = json!({
        "requested": request.resources,
        "applied": {},
        "peak": {},
        "events": [],
    });
    connection
        .execute(
            "INSERT INTO runs (
                run_id, status, kind, phase, label, agent, repository_id, repository,
                worktree_id, checkout, branch, head_sha, barrier, resources_json,
                resource_contract_json, resource_receipt_json, resource_state_json,
                gate_run_id, caller_pid, command_json, environment_json, created_at
             ) VALUES (
                ?1, 'queued', ?2, 'queued', ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10,
                ?11, ?12, ?13, ?14, '{}', ?15, ?16, ?17, ?18, ?19
             )",
            params![
                request.run_id,
                request.kind,
                request.label.trim(),
                request.agent,
                request.repository_id,
                request.repository,
                request.worktree_id,
                request.checkout.to_string_lossy(),
                request.branch,
                request.head_sha,
                i64::from(matches!(request.kind.as_str(), "full" | "merge" | "land")),
                serde_json::to_string(&request.resources).unwrap(),
                serde_json::to_string(&contract).unwrap(),
                serde_json::to_string(&receipt).unwrap(),
                request.gate_run_id,
                i64::from(std::process::id()),
                serde_json::to_string(&request.command).unwrap(),
                serde_json::to_string(&request.environment).unwrap(),
                timestamp,
            ],
        )
        .map_err(map_database_error)?;
    set_metadata(&connection, "last_activity", &now(&connection)?)?;
    connection
        .execute_batch("COMMIT")
        .map_err(map_database_error)?;
    Ok(json!({"run_id": request.run_id}))
}

pub fn cancel(paths: &Paths, run_id: &str, crash_after_commit: bool) -> Result<Value> {
    let _owner = owner_info(paths)?;
    let connection = open_protocol5(paths)?;
    connection
        .execute_batch("BEGIN IMMEDIATE")
        .map_err(map_database_error)?;
    let run = load_run(&connection, run_id)?;
    if is_terminal(&run.status) {
        return Err(AppError::new(
            "broker-run-terminal",
            format!("run {run_id} is already {}", run.status),
        ));
    }
    if run.status == "running"
        && (run.kind == "merge" || (run.kind == "land" && run.phase == "publishing"))
    {
        return Err(AppError::new(
            "broker-publication-authoritative",
            "publication is authoritative and cannot be cancelled",
        ));
    }
    let timestamp = now(&connection)?;
    if run.status == "queued" {
        connection
            .execute(
                "UPDATE runs SET status = 'cancelled', phase = 'complete', finished_at = ?1,
                 exit_status = 130, cancel_requested = 1, cancel_requested_at = ?1,
                 environment_json = '{}' WHERE run_id = ?2",
                params![timestamp, run_id],
            )
            .map_err(map_database_error)?;
    } else {
        connection
            .execute(
                "UPDATE runs SET cancel_requested = 1, cancel_requested_at = ?1
                 WHERE run_id = ?2",
                params![timestamp, run_id],
            )
            .map_err(map_database_error)?;
    }
    connection
        .execute_batch("COMMIT")
        .map_err(map_database_error)?;
    if crash_after_commit {
        std::process::exit(86);
    }
    status(paths, run_id)
}

pub fn advance_land_phase(paths: &Paths, request: &PhaseRequest) -> Result<Value> {
    let _owner = owner_info(paths)?;
    let connection = open_protocol5(paths)?;
    connection
        .execute_batch("BEGIN IMMEDIATE")
        .map_err(map_database_error)?;
    let run = load_run(&connection, &request.run_id)?;
    if run.kind != "land" || run.status != "running" {
        return Err(AppError::new(
            "broker-land-phase-invalid",
            "only a running land may report a publication phase",
        ));
    }
    if run.worker_pid != Some(request.worker_pid)
        || run.worker_start_token.as_deref() != Some(&request.worker_start_token)
        || !same_process(Some(request.worker_pid), Some(&request.worker_start_token))
        || run.checkout != request.checkout
        || run.head_sha.as_deref() != Some(&request.head_sha)
    {
        return Err(AppError::new(
            "broker-land-identity-mismatch",
            "land phase reporter does not match the admitted worker, checkout, and head",
        ));
    }
    if run.cancel_requested {
        return Err(AppError::new(
            "broker-land-cancelled",
            "land cancellation committed before publication became authoritative",
        ));
    }
    let order = |phase: &str| match phase {
        "preflight" => Some(0),
        "gating" => Some(1),
        "publishing" => Some(2),
        _ => None,
    };
    let selected_gate_status = request.gate_exit_status.or(run.gate_exit_status);
    let valid = order(&run.phase)
        .is_some_and(|current| order(&request.phase).is_some_and(|next| next >= current))
        && request
            .gate_exit_status
            .is_none_or(|status| (0..=255).contains(&status))
        && !(run.gate_exit_status.is_some()
            && request.gate_exit_status.is_some()
            && run.gate_exit_status != request.gate_exit_status)
        && !(request.phase == "preflight" && selected_gate_status.is_some())
        && !(request.phase == "publishing" && selected_gate_status != Some(0));
    if !valid {
        return Err(AppError::new(
            "broker-land-phase-invalid",
            "land phase transition or gate exit status is invalid",
        ));
    }
    connection
        .execute(
            "UPDATE runs SET phase = ?1, gate_exit_status = ?2 WHERE run_id = ?3",
            params![request.phase, selected_gate_status, request.run_id,],
        )
        .map_err(map_database_error)?;
    connection
        .execute_batch("COMMIT")
        .map_err(map_database_error)?;
    status(paths, &request.run_id)
}

pub fn is_terminal(status: &str) -> bool {
    matches!(status, "passed" | "failed" | "cancelled" | "interrupted")
}

fn live_run_ids(connection: &Connection) -> Result<Vec<String>> {
    let mut statement = connection
        .prepare("SELECT run_id FROM runs WHERE status IN ('queued', 'running') ORDER BY sequence")
        .map_err(map_database_error)?;
    statement
        .query_map([], |row| row.get(0))
        .map_err(map_database_error)?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(map_database_error)
}

fn backup_database(paths: &Paths, connection: &Connection, from_protocol: u64) -> Result<PathBuf> {
    let (busy, log_frames, checkpointed): (i64, i64, i64) = connection
        .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .map_err(|error| {
            AppError::new(
                "broker-migration-backup-failed",
                format!("cannot checkpoint migration source: {error}"),
            )
        })?;
    if busy != 0 || (log_frames >= 0 && checkpointed != log_frames) {
        return Err(AppError::new(
            "broker-migration-backup-failed",
            "cannot checkpoint every migration source WAL frame",
        ));
    }
    let mut backup = paths
        .state_dir
        .join(format!("queue.sqlite3.protocol{from_protocol}.bak"));
    for generation in 0..10_000_u64 {
        if !backup.exists() {
            break;
        }
        verify_migration_backup(&backup, from_protocol)?;
        backup = paths.state_dir.join(format!(
            "queue.sqlite3.protocol{from_protocol}.{}.bak",
            generation + 1
        ));
    }
    if backup.exists() {
        return Err(AppError::new(
            "broker-migration-backup-failed",
            "cannot allocate a fresh migration backup name",
        ));
    }
    connection
        .execute("VACUUM INTO ?1", params![backup.to_string_lossy()])
        .map_err(|error| {
            AppError::new(
                "broker-migration-backup-failed",
                format!("cannot create migration backup: {error}"),
            )
        })?;
    fs::set_permissions(&backup, fs::Permissions::from_mode(0o600)).map_err(|error| {
        AppError::new(
            "broker-migration-backup-failed",
            format!("cannot protect migration backup: {error}"),
        )
    })?;
    sync_file(&backup)?;
    sync_file(&paths.state_dir)?;
    verify_migration_backup(&backup, from_protocol)?;
    Ok(backup)
}

fn verify_migration_backup(path: &Path, expected_protocol: u64) -> Result<()> {
    let invalid = || {
        AppError::new(
            "broker-migration-backup-invalid",
            "migration backup is missing, public, corrupt, or has the wrong protocol",
        )
    };
    let details = fs::symlink_metadata(path).map_err(|_| invalid())?;
    // SAFETY: geteuid takes no arguments and has no preconditions.
    if details.file_type().is_symlink()
        || !details.is_file()
        || details.uid() != unsafe { libc::geteuid() }
        || details.mode() & 0o777 != 0o600
    {
        return Err(invalid());
    }
    let backup = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(|_| invalid())?;
    if protocol(&backup).map_err(|_| invalid())? != expected_protocol {
        return Err(invalid());
    }
    let integrity: String = backup
        .query_row("PRAGMA quick_check", [], |row| row.get(0))
        .map_err(|_| invalid())?;
    if integrity != "ok" {
        return Err(invalid());
    }
    if expected_protocol == 4 {
        validate_schema(&backup).map_err(|_| invalid())?;
        load_runs(&backup).map_err(|_| invalid())?;
        if !live_run_ids(&backup).map_err(|_| invalid())?.is_empty() {
            return Err(invalid());
        }
    }
    Ok(())
}

fn migrate_protocol_1_to_2(connection: &Connection) -> Result<()> {
    connection
        .execute_batch(RUNS_V2_SCHEMA)
        .map_err(map_database_error)?;
    connection
        .execute_batch(
            "INSERT INTO runs_v2 (
                sequence, run_id, status, kind, phase, label, agent,
                repository_id, repository, worktree_id, checkout, branch,
                head_sha, barrier, resources_json, gate_run_id,
                publication_adapter, publication_request, failure_reason,
                gate_exit_status, reported_exit_status, caller_pid,
                command_json, environment_json, created_at, started_at,
                finished_at, exit_status, worker_pid, worker_start_token,
                cancel_requested, cancel_requested_at
             )
             SELECT sequence, run_id, status, kind, 'complete', label, agent,
                    repository_id, repository, worktree_id, checkout, branch,
                    head_sha, barrier, resources_json, gate_run_id,
                    publication_adapter, publication_request, failure_reason,
                    NULL, NULL, caller_pid, command_json, environment_json,
                    created_at, started_at, finished_at, exit_status, worker_pid,
                    worker_start_token, cancel_requested, cancel_requested_at
             FROM runs ORDER BY sequence;
             DROP TABLE runs;
             ALTER TABLE runs_v2 RENAME TO runs;
             CREATE INDEX runs_status_sequence ON runs(status, sequence);
             CREATE INDEX runs_repository_sequence ON runs(repository_id, sequence);",
        )
        .map_err(map_database_error)?;
    set_metadata(connection, "protocol", "2")
}

fn migrate_protocol_2_to_3(connection: &Connection) -> Result<()> {
    connection
        .execute_batch(
            "ALTER TABLE runs ADD COLUMN resource_contract_json
                 TEXT NOT NULL DEFAULT '{}';
             ALTER TABLE runs ADD COLUMN resource_receipt_json
                 TEXT NOT NULL DEFAULT '{}';
             ALTER TABLE runs ADD COLUMN resource_state_json
                 TEXT NOT NULL DEFAULT '{}';",
        )
        .map_err(map_database_error)?;
    let rows = {
        let mut statement = connection
            .prepare("SELECT run_id, resources_json FROM runs ORDER BY sequence")
            .map_err(map_database_error)?;
        statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(map_database_error)?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(map_database_error)?
    };
    for (run_id, encoded) in rows {
        let resources: BTreeMap<String, u64> = serde_json::from_str(&encoded).map_err(|_| {
            AppError::new(
                "broker-migration-row-invalid",
                format!("cannot migrate run {run_id}: invalid stored resources"),
            )
        })?;
        if resources.is_empty()
            || resources.iter().any(|(name, units)| {
                !identifier_valid(name) || *units == 0 || *units > i64::MAX as u64
            })
        {
            return Err(AppError::new(
                "broker-migration-row-invalid",
                format!("cannot migrate run {run_id}: invalid stored resources"),
            ));
        }
        let receipt = json!({
            "requested": resources,
            "applied": {},
            "peak": {},
            "events": [],
        });
        connection
            .execute(
                "UPDATE runs SET resource_contract_json = ?1,
                    resource_receipt_json = ?2, resource_state_json = '{}'
                 WHERE run_id = ?3",
                params![
                    serde_json::to_string(&resource_contract(&resources)).unwrap(),
                    serde_json::to_string(&receipt).unwrap(),
                    run_id,
                ],
            )
            .map_err(map_database_error)?;
    }
    set_metadata(connection, "protocol", "3")
}

fn migrate_protocol_3_to_4(connection: &Connection) -> Result<()> {
    connection
        .execute_batch(LEASE_SCHEMA)
        .map_err(map_database_error)?;
    set_metadata(connection, "protocol", "4")
}

pub fn migrate(state_dir: &Path) -> Result<Value> {
    prepare_private_directory(state_dir)?;
    let paths = Paths::new(state_dir).configured()?;
    if !paths.database.is_file() {
        return Err(AppError::new(
            "broker-state-missing",
            "no coordinator database exists to migrate",
        ));
    }
    let _owner = OwnerLock::acquire(state_dir)?;
    let connection = connect(&paths)?;
    let from_protocol = protocol(&connection)?;
    if from_protocol == PROTOCOL {
        validate_schema(&connection)?;
        load_runs(&connection)?;
        if metadata(&connection, "owner_implementation")?.as_deref() != Some("rust-native")
            || metadata(&connection, "schema_fingerprint")?.as_deref() != Some(SCHEMA_FINGERPRINT)
        {
            return Err(AppError::new(
                "broker-schema-invalid",
                "protocol-5 owner metadata or schema fingerprint is invalid",
            ));
        }
        return Ok(json!({
            "changed": false,
            "from_protocol": PROTOCOL,
            "to_protocol": PROTOCOL,
        }));
    }
    if !(1..=4).contains(&from_protocol) {
        return Err(AppError::new(
            "broker-protocol-unsupported",
            format!("no migration from protocol {from_protocol} to {PROTOCOL} is defined"),
        ));
    }
    let live = live_run_ids(&connection)?;
    if !live.is_empty() {
        return Err(AppError::new(
            "broker-migration-live-runs",
            format!("cannot migrate with live runs: {}", live.join(", ")),
        ));
    }
    let source_backup = backup_database(&paths, &connection, from_protocol)?;
    connection
        .execute_batch("BEGIN IMMEDIATE")
        .map_err(map_database_error)?;
    if protocol(&connection)? != from_protocol || !live_run_ids(&connection)?.is_empty() {
        return Err(AppError::new(
            "broker-migration-state-changed",
            "coordinator state changed while its migration backup was created",
        ));
    }
    let mut current_protocol = from_protocol;
    if current_protocol == 1 {
        migrate_protocol_1_to_2(&connection)?;
        current_protocol = 2;
    }
    if current_protocol == 2 {
        migrate_protocol_2_to_3(&connection)?;
        current_protocol = 3;
    }
    if current_protocol == 3 {
        migrate_protocol_3_to_4(&connection)?;
        current_protocol = 4;
    }
    debug_assert_eq!(current_protocol, 4);
    validate_schema(&connection)?;
    load_runs(&connection)?;
    connection
        .execute_batch("COMMIT")
        .map_err(map_database_error)?;

    let backup = if from_protocol == 4 {
        source_backup
    } else {
        backup_database(&paths, &connection, 4)?
    };
    connection
        .execute_batch("BEGIN IMMEDIATE")
        .map_err(map_database_error)?;
    if protocol(&connection)? != 4 || !live_run_ids(&connection)?.is_empty() {
        return Err(AppError::new(
            "broker-migration-state-changed",
            "coordinator state changed before native ownership was committed",
        ));
    }
    validate_schema(&connection)?;
    load_runs(&connection)?;
    let gate_floor: i64 = connection
        .query_row(
            "SELECT COALESCE(MAX(sequence), 0) + 1 FROM runs",
            [],
            |row| row.get(0),
        )
        .map_err(map_database_error)?;
    set_metadata(&connection, "owner_implementation", "rust-native")?;
    set_metadata(&connection, "schema_fingerprint", SCHEMA_FINGERPRINT)?;
    set_metadata(&connection, "native_gate_floor", &gate_floor.to_string())?;
    set_metadata(&connection, "migration_from", &from_protocol.to_string())?;
    set_metadata(
        &connection,
        "migration_backup",
        backup.file_name().unwrap().to_string_lossy().as_ref(),
    )?;
    set_metadata(&connection, "protocol", &PROTOCOL.to_string())?;
    connection
        .execute_batch("COMMIT")
        .map_err(map_database_error)?;
    connection
        .pragma_update(None, "journal_mode", "WAL")
        .map_err(map_database_error)?;
    let verified = open_protocol5(&paths)?;
    load_runs(&verified)?;
    Ok(json!({
        "changed": true,
        "from_protocol": from_protocol,
        "to_protocol": PROTOCOL,
    }))
}

pub fn rollback(state_dir: &Path) -> Result<Value> {
    prepare_private_directory(state_dir)?;
    let paths = Paths::new(state_dir).configured()?;
    let _owner = OwnerLock::acquire(state_dir)?;
    let connection = connect(&paths)?;
    let from_protocol = protocol(&connection)?;
    if from_protocol == 4 {
        validate_schema(&connection)?;
        load_runs(&connection)?;
        return Ok(json!({"changed": false, "from_protocol": 4, "to_protocol": 4}));
    }
    if from_protocol != PROTOCOL {
        return Err(AppError::new(
            "broker-protocol-unsupported",
            "rollback requires protocol 5",
        ));
    }
    let live = live_run_ids(&connection)?;
    if !live.is_empty() {
        return Err(AppError::new(
            "broker-migration-live-runs",
            format!("cannot roll back with live runs: {}", live.join(", ")),
        ));
    }
    let backup_name = metadata(&connection, "migration_backup")?.ok_or_else(|| {
        AppError::new(
            "broker-migration-backup-invalid",
            "protocol-5 state has no migration backup record",
        )
    })?;
    let backup = paths.state_dir.join(backup_name);
    if !backup.is_file() {
        return Err(AppError::new(
            "broker-migration-backup-invalid",
            "protocol-4 migration backup is missing or invalid",
        ));
    }
    let backup_connection = Connection::open(&backup).map_err(map_database_error)?;
    if protocol(&backup_connection)? != 4 {
        return Err(AppError::new(
            "broker-migration-backup-invalid",
            "protocol-4 migration backup is missing or invalid",
        ));
    }
    validate_schema(&backup_connection).map_err(|_| {
        AppError::new(
            "broker-migration-backup-invalid",
            "protocol-4 migration backup has an invalid schema",
        )
    })?;
    load_runs(&backup_connection).map_err(|_| {
        AppError::new(
            "broker-migration-backup-invalid",
            "protocol-4 migration backup has invalid history",
        )
    })?;
    if !live_run_ids(&backup_connection)?.is_empty() {
        return Err(AppError::new(
            "broker-migration-backup-invalid",
            "protocol-4 migration backup contains live runs",
        ));
    }
    drop(backup_connection);
    validate_schema(&connection)?;
    load_runs(&connection)?;
    connection
        .execute(
            "ATTACH DATABASE ?1 AS rollback_backup",
            params![backup.to_string_lossy()],
        )
        .map_err(map_database_error)?;
    connection
        .execute_batch("BEGIN IMMEDIATE")
        .map_err(map_database_error)?;
    if protocol(&connection)? != PROTOCOL || !live_run_ids(&connection)?.is_empty() {
        return Err(AppError::new(
            "broker-migration-state-changed",
            "coordinator state changed before rollback acquired the database",
        ));
    }
    let cutoff: i64 = connection
        .query_row("SELECT COALESCE(MAX(sequence), 0) FROM runs", [], |row| {
            row.get(0)
        })
        .map_err(map_database_error)?;
    connection
        .execute_batch(
            "CREATE TEMP TABLE native_terminal_runs AS
                 SELECT * FROM runs
                 WHERE status IN ('passed', 'failed', 'cancelled', 'interrupted');
             CREATE TEMP TABLE native_terminal_leases AS
                 SELECT child_cpu_leases.* FROM child_cpu_leases
                 JOIN runs USING (run_id)
                 WHERE runs.status IN ('passed', 'failed', 'cancelled', 'interrupted');
             DELETE FROM child_cpu_leases;
             DELETE FROM runs;
             DELETE FROM coordinator_meta;
             INSERT INTO coordinator_meta SELECT * FROM rollback_backup.coordinator_meta;
             INSERT INTO runs SELECT * FROM rollback_backup.runs ORDER BY sequence;
             INSERT OR IGNORE INTO runs SELECT * FROM native_terminal_runs ORDER BY sequence;
             INSERT INTO child_cpu_leases
                 SELECT * FROM rollback_backup.child_cpu_leases ORDER BY sequence;
             INSERT OR IGNORE INTO child_cpu_leases
                 SELECT * FROM native_terminal_leases ORDER BY sequence;",
        )
        .map_err(map_database_error)?;
    set_metadata(
        &connection,
        "invalid_gate_through_sequence",
        &cutoff.to_string(),
    )?;
    set_metadata(&connection, "protocol", "4")?;
    connection
        .execute_batch("COMMIT")
        .map_err(map_database_error)?;
    connection
        .execute_batch("DETACH DATABASE rollback_backup")
        .map_err(map_database_error)?;
    validate_schema(&connection)?;
    load_runs(&connection)?;
    Ok(json!({"changed": true, "from_protocol": PROTOCOL, "to_protocol": 4}))
}
