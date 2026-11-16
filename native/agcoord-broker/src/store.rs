use crate::error::{AppError, Result};
use crate::overflow::Pool;
use crate::platform::{
    OwnerLock, direct_child_of, is_descendant_process, live_owner_metadata,
    prepare_private_directory, same_process, same_worker_process, sync_file,
};
use crate::resources::{
    BackendState, Binding, initial_receipt, parse_backend_state, parse_bindings,
    parse_bindings_json, parse_capabilities_json, receipt_valid, resource_contract,
};
use rusqlite::{Connection, ErrorCode, OpenFlags, OptionalExtension, Row, params};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom};
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::time::Duration;

pub const PROTOCOL: u64 = 5;
pub const SCHEMA_FINGERPRINT: &str = "agcoord-spool-v5";
pub const MAINTENANCE_REFUSAL: &str = "agcoord-maintenance-draining";
const DATABASE_TIMEOUT: Duration = Duration::from_secs(10);
const LAND_GATE_REUSE_MAX_AGE: Duration = Duration::from_secs(3600);
const STOP_GRACE: Duration = Duration::from_secs(600);
const RECENT_LIMIT: usize = 50;

#[derive(Debug, Default, Clone)]
pub struct SnapshotFilter {
    pub repository: Option<String>,
    pub agent: Option<String>,
    pub kind: Option<String>,
    pub pool: Option<String>,
    pub limit: Option<usize>,
}

fn matches_snapshot_filter(
    run: &RunRecord,
    filter: &SnapshotFilter,
    placed: &BTreeMap<String, String>,
) -> bool {
    if let Some(expected_repo) = &filter.repository {
        let repo_matched = run.repository == *expected_repo
            || run.repository_id == *expected_repo
            || run.repository.trim_end_matches("/.git") == expected_repo.trim_end_matches("/.git");
        if !repo_matched {
            return false;
        }
    }
    if let Some(expected_agent) = &filter.agent
        && run.agent != *expected_agent
    {
        return false;
    }
    if let Some(expected_kind) = &filter.kind
        && run.kind != *expected_kind
    {
        return false;
    }
    if let Some(expected_pool) = &filter.pool
        && placed
            .get(&run.run_id)
            .map(String::as_str)
            .unwrap_or("local")
            != expected_pool
    {
        return false;
    }
    true
}
const MAX_LOG_BYTES: usize = 64 * 1024;
const MAX_MAINTENANCE_REASON: usize = 256;
const MAINTENANCE_TRIGGERS: [&str; 3] = [
    "agcoord_maintenance_reject_runs",
    "agcoord_maintenance_reject_activity_insert",
    "agcoord_maintenance_reject_activity_update",
];
const MAINTENANCE_KEYS: [&str; 4] = [
    "maintenance_state",
    "maintenance_id",
    "maintenance_reason",
    "maintenance_started_at",
];

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
    pub resource_bindings: BTreeMap<String, Binding>,
    pub resource_capabilities: Value,
    pub local_runtime: crate::overflow::Runtime,
    pub overflow_pools: Vec<Pool>,
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
    pub publication_adapter: Option<String>,
    pub publication_request: Option<Value>,
    pub caller_pid: u32,
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
    pub new_head_sha: Option<String>,
    pub phase: String,
    pub gate_exit_status: Option<i64>,
}

#[derive(Clone, Debug)]
pub struct ReuseRequest {
    pub run_id: String,
    pub worker_pid: u32,
    pub worker_start_token: String,
    pub checkout: PathBuf,
    pub head_sha: String,
}

#[derive(Clone, Debug)]
pub struct AdmissionRequest {
    pub run_id: String,
    pub kind: String,
    pub worker_pid: u32,
    pub worker_start_token: String,
    pub checkout: PathBuf,
    pub head_sha: String,
}

#[derive(Clone, Debug)]
pub struct LandResultRequest {
    pub run_id: String,
    pub worker_pid: u32,
    pub worker_start_token: String,
    pub exit_status: i64,
}

#[derive(Clone, Debug)]
pub struct BatchClaimRequest {
    pub run_id: String,
    pub worker_pid: u32,
    pub worker_start_token: String,
    pub checkout: PathBuf,
    pub head_sha: String,
    pub member_run_ids: Vec<String>,
    pub target_branch: Option<String>,
}

#[derive(Clone, Debug)]
pub struct BatchStatusRequest {
    pub run_id: String,
    pub worker_pid: u32,
    pub worker_start_token: String,
    pub checkout: PathBuf,
    pub head_sha: String,
}

#[derive(Clone, Debug)]
pub struct BatchSliceRequest {
    pub identity: BatchStatusRequest,
    pub gating_head_sha: String,
    pub member_run_ids: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DroppedBatchMember {
    pub run_id: String,
    pub reason: String,
}

impl DroppedBatchMember {
    pub fn to_json_value(&self) -> Value {
        json!({
            "run_id": self.run_id,
            "reason": self.reason,
        })
    }
}

#[derive(Clone, Debug)]
pub struct BatchStepPayload {
    pub step: Option<u64>,
    pub member_run_ids: Option<Vec<String>>,
    pub gated_member_run_ids: Option<Vec<String>>,
    pub dropped: Option<Vec<DroppedBatchMember>>,
    pub base_sha: Option<String>,
    pub gating_head_sha: Option<String>,
    pub gate_reused_from: Option<String>,
    pub gate_exit_status: Option<i64>,
    pub outcome: String,
    pub split: Option<[u64; 2]>,
    pub published_sha: Option<String>,
}

impl BatchStepPayload {
    pub fn from_json_str(raw: &str) -> Result<Self> {
        let value: Value = serde_json::from_str(raw)
            .map_err(|error| AppError::usage(format!("--step-json is invalid: {error}")))?;
        let Some(object) = value.as_object() else {
            return Err(AppError::usage("--step-json must be a JSON object"));
        };
        const ALLOWED: &[&str] = &[
            "step",
            "member_run_ids",
            "gated_member_run_ids",
            "dropped",
            "base_sha",
            "gating_head_sha",
            "gate_reused_from",
            "gate_exit_status",
            "outcome",
            "split",
            "published_sha",
        ];
        for key in object.keys() {
            if !ALLOWED.contains(&key.as_str()) {
                return Err(AppError::usage(format!(
                    "--step-json contains unknown field {key}"
                )));
            }
        }
        let parse_opt_str = |field: &str| -> Result<Option<String>> {
            match object.get(field) {
                None | Some(Value::Null) => Ok(None),
                Some(Value::String(s)) => Ok(Some(s.clone())),
                _ => Err(AppError::usage(format!(
                    "--step-json field {field} must be a string or null"
                ))),
            }
        };
        let parse_opt_str_list = |field: &str| -> Result<Option<Vec<String>>> {
            match object.get(field) {
                None | Some(Value::Null) => Ok(None),
                Some(Value::Array(items)) => {
                    let mut out = Vec::with_capacity(items.len());
                    for item in items {
                        let Some(s) = item.as_str() else {
                            return Err(AppError::usage(format!(
                                "--step-json field {field} must contain only strings"
                            )));
                        };
                        out.push(s.to_owned());
                    }
                    Ok(Some(out))
                }
                _ => Err(AppError::usage(format!(
                    "--step-json field {field} must be a string array or null"
                ))),
            }
        };
        let step = match object.get("step") {
            None | Some(Value::Null) => None,
            Some(v) => Some(
                v.as_u64()
                    .ok_or_else(|| AppError::usage("--step-json field step must be a u64"))?,
            ),
        };
        let dropped = match object.get("dropped") {
            None | Some(Value::Null) => None,
            Some(Value::Array(items)) => {
                let mut out = Vec::with_capacity(items.len());
                for item in items {
                    let Some(obj) = item.as_object() else {
                        return Err(AppError::usage(
                            "--step-json field dropped must contain objects",
                        ));
                    };
                    if obj.len() != 2
                        || !obj.contains_key("run_id")
                        || !obj.contains_key("reason")
                    {
                        return Err(AppError::usage(
                            "--step-json dropped items must have exact keys run_id and reason",
                        ));
                    }
                    let Some(run_id) = obj.get("run_id").and_then(Value::as_str) else {
                        return Err(AppError::usage(
                            "--step-json dropped.run_id must be a string",
                        ));
                    };
                    let Some(reason) = obj.get("reason").and_then(Value::as_str) else {
                        return Err(AppError::usage(
                            "--step-json dropped.reason must be a string",
                        ));
                    };
                    out.push(DroppedBatchMember {
                        run_id: run_id.to_owned(),
                        reason: reason.to_owned(),
                    });
                }
                Some(out)
            }
            _ => {
                return Err(AppError::usage(
                    "--step-json field dropped must be an array or null",
                ))
            }
        };
        let gate_exit_status = match object.get("gate_exit_status") {
            None | Some(Value::Null) => None,
            Some(v) => Some(v.as_i64().ok_or_else(|| {
                AppError::usage("--step-json field gate_exit_status must be an integer")
            })?),
        };
        let outcome = object
            .get("outcome")
            .and_then(Value::as_str)
            .ok_or_else(|| AppError::usage("--step-json field outcome is required"))?
            .to_owned();
        let split = match object.get("split") {
            None | Some(Value::Null) => None,
            Some(Value::Array(items)) if items.len() == 2 => {
                let l = items[0]
                    .as_u64()
                    .ok_or_else(|| AppError::usage("--step-json split[0] must be a u64"))?;
                let r = items[1]
                    .as_u64()
                    .ok_or_else(|| AppError::usage("--step-json split[1] must be a u64"))?;
                Some([l, r])
            }
            _ => {
                return Err(AppError::usage(
                    "--step-json field split must be a 2-element u64 array or null",
                ))
            }
        };
        Ok(Self {
            step,
            member_run_ids: parse_opt_str_list("member_run_ids")?,
            gated_member_run_ids: parse_opt_str_list("gated_member_run_ids")?,
            dropped,
            base_sha: parse_opt_str("base_sha")?,
            gating_head_sha: parse_opt_str("gating_head_sha")?,
            gate_reused_from: parse_opt_str("gate_reused_from")?,
            gate_exit_status,
            outcome,
            split,
            published_sha: parse_opt_str("published_sha")?,
        })
    }
}

#[derive(Clone, Debug)]
pub struct BatchStepRequest {
    pub identity: BatchStatusRequest,
    pub payload: BatchStepPayload,
}

#[derive(Clone, Debug)]
pub struct BatchMemberPhaseRequest {
    pub run_id: String,
    pub worker_pid: u32,
    pub worker_start_token: String,
    pub checkout: PathBuf,
    pub head_sha: String,
    pub member_run_id: String,
    pub phase: String,
    pub new_head_sha: Option<String>,
    pub gate_exit_status: Option<i64>,
}

#[derive(Clone, Debug)]
pub struct BatchMemberFinishRequest {
    pub run_id: String,
    pub worker_pid: u32,
    pub worker_start_token: String,
    pub checkout: PathBuf,
    pub head_sha: String,
    pub member_run_id: String,
    pub exit_status: i64,
    pub failure_reason: Option<String>,
    pub gate_exit_status: Option<i64>,
}

#[derive(Clone, Debug)]
pub struct BatchMemberReuseRequest {
    pub run_id: String,
    pub worker_pid: u32,
    pub worker_start_token: String,
    pub checkout: PathBuf,
    pub head_sha: String,
    pub member_run_id: String,
}

#[derive(Clone, Debug)]
pub struct ChildLeaseRequest {
    pub lease_id: String,
    pub run_id: String,
    pub requested: u64,
    pub minimum: u64,
    pub owner_pid: u32,
    pub owner_start_token: String,
}

#[derive(Clone, Debug)]
pub struct ChildLeaseOwnerRequest {
    pub lease_id: String,
    pub owner_pid: u32,
    pub owner_start_token: String,
}

#[derive(Clone, Debug)]
pub struct MaintenanceRecord {
    pub state: String,
    pub drain_id: String,
    pub reason: String,
    pub started_at: String,
}

#[derive(Clone, Debug)]
struct ChildLeaseRecord {
    sequence: i64,
    lease_id: String,
    run_id: String,
    status: String,
    requested: u64,
    minimum: u64,
    granted: u64,
    owner_pid: u32,
    owner_start_token: String,
    bypass_count: u64,
    created_at: String,
    acquired_at: Option<String>,
    finished_at: Option<String>,
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
    pub resource_state: BTreeMap<String, BackendState>,
    pub gate_run_id: Option<String>,
    pub publication_adapter: Option<String>,
    pub publication_request: Option<Value>,
    pub failure_reason: Option<String>,
    pub gate_exit_status: Option<i64>,
    pub reported_exit_status: Option<i64>,
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
    if error.to_string().contains(MAINTENANCE_REFUSAL) {
        return AppError::new(
            "broker-draining",
            "coordinator is draining; new submissions are refused",
        );
    }
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
        "capabilities",
        "bindings",
        "cgroup_root",
        "cgroup_io",
        "database_timeout",
        "land_gate_reuse_max_age",
        "stop_grace",
        "native_broker",
        "overflow_pools",
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
    if let Some(native) = object.get("native_broker") {
        let native = native.as_object().ok_or_else(|| {
            AppError::new(
                "broker-config-invalid",
                "broker configuration native_broker must be a JSON object",
            )
        })?;
        if native.keys().any(|key| {
            !matches!(
                key.as_str(),
                "path" | "allow_development" | "managed_service"
            )
        }) {
            return Err(AppError::new(
                "broker-config-invalid",
                "broker configuration native_broker has an unknown key",
            ));
        }
        let selected = native
            .get("path")
            .and_then(Value::as_str)
            .filter(|path| !path.is_empty() && !path.contains('\0'))
            .map(Path::new)
            .filter(|path| path.is_absolute());
        if selected.is_none() {
            return Err(AppError::new(
                "broker-config-invalid",
                "broker configuration native_broker path must be absolute",
            ));
        }
        if native
            .get("allow_development")
            .is_some_and(|value| !value.is_boolean())
        {
            return Err(AppError::new(
                "broker-config-invalid",
                "broker configuration native_broker allow_development must be boolean",
            ));
        }
        if native
            .get("managed_service")
            .is_some_and(|value| !value.is_boolean())
        {
            return Err(AppError::new(
                "broker-config-invalid",
                "broker configuration native_broker managed_service must be boolean",
            ));
        }
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
    if let Some(value) = object.get("land_gate_reuse_max_age")
        && value
            .as_f64()
            .filter(|seconds| seconds.is_finite() && *seconds >= 0.0 && *seconds <= 2_147_483.647)
            .is_none()
    {
        return Err(AppError::new(
            "broker-config-invalid",
            "land_gate_reuse_max_age must be a finite number of seconds from 0 to 2147483.647",
        ));
    }
    if let Some(value) = object.get("stop_grace")
        && value
            .as_f64()
            .filter(|seconds| seconds.is_finite() && *seconds >= 0.0 && *seconds <= 2_147_483.647)
            .is_none()
    {
        return Err(AppError::new(
            "broker-config-invalid",
            "stop_grace must be a finite number of seconds from 0 to 2147483.647",
        ));
    }
    crate::overflow::parse_capabilities_field(object.get("capabilities"), "broker configuration")?;
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

/// How long a gracefully stopping broker lets running jobs finish; zero interrupts them at once.
pub fn configured_stop_grace(paths: &Paths) -> Result<Duration> {
    let path = paths.state_dir.join("config.json");
    let text = match fs::read_to_string(&path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(STOP_GRACE),
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
    let Some(value) = document.get("stop_grace") else {
        return Ok(STOP_GRACE);
    };
    let seconds = value
        .as_f64()
        .filter(|seconds| seconds.is_finite() && *seconds >= 0.0 && *seconds <= 2_147_483.647)
        .ok_or_else(|| {
            AppError::new(
                "broker-config-invalid",
                "stop_grace must be a finite number of seconds from 0 to 2147483.647",
            )
        })?;
    Ok(Duration::from_secs_f64(seconds))
}

/// How long a passed `full` receipt may authorize a land gate; zero disables reuse.
fn land_gate_reuse_max_age(paths: &Paths) -> Result<Duration> {
    let path = paths.state_dir.join("config.json");
    let text = match fs::read_to_string(&path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(LAND_GATE_REUSE_MAX_AGE);
        }
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
    let Some(value) = document.get("land_gate_reuse_max_age") else {
        return Ok(LAND_GATE_REUSE_MAX_AGE);
    };
    let seconds = value
        .as_f64()
        .filter(|seconds| seconds.is_finite() && *seconds >= 0.0 && *seconds <= 2_147_483.647)
        .ok_or_else(|| {
            AppError::new(
                "broker-config-invalid",
                "land_gate_reuse_max_age must be a finite number of seconds from 0 to 2147483.647",
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
        .map_err(|error| {
            // Contention is transient and retryable; only an unreadable table is a schema fault.
            if matches!(
                error.sqlite_error_code(),
                Some(ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked)
            ) {
                map_database_error(error)
            } else {
                AppError::new(
                    "broker-schema-invalid",
                    "coordinator database has no readable protocol metadata",
                )
            }
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

pub(crate) fn metadata(connection: &Connection, key: &str) -> Result<Option<String>> {
    connection
        .query_row(
            "SELECT value FROM coordinator_meta WHERE key = ?1",
            params![key],
            |row| row.get(0),
        )
        .optional()
        .map_err(map_database_error)
}

pub(crate) fn set_metadata(connection: &Connection, key: &str, value: &str) -> Result<()> {
    connection
        .execute(
            "INSERT INTO coordinator_meta(key, value) VALUES (?1, ?2)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![key, value],
        )
        .map_err(map_database_error)?;
    Ok(())
}

pub(crate) fn drain_id_valid(value: &str) -> bool {
    value.len() == 18
        && value.starts_with("drain-")
        && value[6..]
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

pub(crate) fn maintenance_time_valid(value: &str) -> bool {
    let bytes = value.as_bytes();
    let zone_start = if bytes.ends_with(b"Z") {
        bytes.len().checked_sub(1)
    } else if bytes.ends_with(b"+00:00") {
        bytes.len().checked_sub(6)
    } else {
        None
    };
    let Some(zone_start) = zone_start else {
        return false;
    };
    if zone_start < 19
        || bytes.get(4) != Some(&b'-')
        || bytes.get(7) != Some(&b'-')
        || bytes.get(10) != Some(&b'T')
        || bytes.get(13) != Some(&b':')
        || bytes.get(16) != Some(&b':')
    {
        return false;
    }
    let digits = [0..4, 5..7, 8..10, 11..13, 14..16, 17..19];
    if digits.iter().any(|range| {
        bytes
            .get(range.clone())
            .is_none_or(|part| !part.iter().all(u8::is_ascii_digit))
    }) {
        return false;
    }
    let fraction = &bytes[19..zone_start];
    if !fraction.is_empty()
        && (fraction.len() < 2
            || fraction.len() > 7
            || fraction[0] != b'.'
            || !fraction[1..].iter().all(u8::is_ascii_digit))
    {
        return false;
    }
    let parse = |range: std::ops::Range<usize>| {
        std::str::from_utf8(&bytes[range]).ok()?.parse::<u32>().ok()
    };
    let (Some(year), Some(month), Some(day), Some(hour), Some(minute), Some(second)) = (
        parse(0..4),
        parse(5..7),
        parse(8..10),
        parse(11..13),
        parse(14..16),
        parse(17..19),
    ) else {
        return false;
    };
    let leap = year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400));
    let days = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap => 29,
        2 => 28,
        _ => return false,
    };
    year > 0 && day > 0 && day <= days && hour < 24 && minute < 60 && second < 60
}

pub fn maintenance_record(connection: &Connection) -> Result<Option<MaintenanceRecord>> {
    let mut values = BTreeMap::new();
    let mut guards = BTreeSet::new();
    let mut statement = connection
        .prepare(
            "SELECT 'metadata', key, value FROM coordinator_meta
             WHERE key IN (
                'maintenance_state', 'maintenance_id',
                'maintenance_reason', 'maintenance_started_at'
             )
             UNION ALL
             SELECT 'trigger', name, NULL FROM sqlite_master
             WHERE type = 'trigger'
               AND name IN (
                  'agcoord_maintenance_reject_runs',
                  'agcoord_maintenance_reject_activity_insert',
                  'agcoord_maintenance_reject_activity_update'
               )",
        )
        .map_err(map_database_error)?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
            ))
        })
        .map_err(map_database_error)?;
    for row in rows {
        let (kind, name, value) = row.map_err(map_database_error)?;
        match (kind.as_str(), value) {
            ("metadata", Some(value)) => {
                values.insert(name, value);
            }
            ("trigger", None) => {
                guards.insert(name);
            }
            _ => {
                return Err(AppError::new(
                    "broker-maintenance-invalid",
                    "coordinator maintenance query returned an invalid row",
                ));
            }
        }
    }
    if values.is_empty() {
        if !guards.is_empty() {
            return Err(AppError::new(
                "broker-maintenance-invalid",
                "coordinator maintenance submission guards have no marker",
            ));
        }
        return Ok(None);
    }
    if values.len() != MAINTENANCE_KEYS.len()
        || MAINTENANCE_KEYS
            .iter()
            .any(|key| !values.contains_key(*key))
    {
        return Err(AppError::new(
            "broker-maintenance-invalid",
            "coordinator maintenance metadata is incomplete",
        ));
    }
    let state = values.remove("maintenance_state").unwrap();
    let drain_id = values.remove("maintenance_id").unwrap();
    let reason = values.remove("maintenance_reason").unwrap();
    let started_at = values.remove("maintenance_started_at").unwrap();
    if !matches!(state.as_str(), "draining" | "drained" | "recovering")
        || !(if state == "recovering" {
            crate::recovery::valid_id(&drain_id)
        } else {
            drain_id_valid(&drain_id)
        })
        || reason.is_empty()
        || reason.chars().count() > MAX_MAINTENANCE_REASON
        || reason.contains('\0')
        || !maintenance_time_valid(&started_at)
    {
        return Err(AppError::new(
            "broker-maintenance-invalid",
            "coordinator maintenance metadata is invalid",
        ));
    }
    if guards
        != MAINTENANCE_TRIGGERS
            .into_iter()
            .map(str::to_owned)
            .collect()
    {
        return Err(AppError::new(
            "broker-maintenance-invalid",
            "coordinator maintenance submission guards are missing",
        ));
    }
    Ok(Some(MaintenanceRecord {
        state,
        drain_id,
        reason,
        started_at,
    }))
}

pub fn mark_maintenance_drained(connection: &Connection) -> Result<()> {
    let record = maintenance_record(connection)?
        .ok_or_else(|| AppError::new("broker-not-draining", "coordinator is not draining"))?;
    if record.state == "recovering" {
        return Err(AppError::new(
            "host-recovery-required",
            "guarded host recovery cannot drain or resume",
        ));
    }
    if record.state != "drained" {
        set_metadata(connection, "maintenance_state", "drained")?;
    }
    Ok(())
}

pub(crate) fn install_maintenance_guards(connection: &Connection) -> Result<()> {
    connection
        .execute_batch(&format!(
            r#"
            CREATE TRIGGER {}
            BEFORE INSERT ON runs
            WHEN EXISTS (
                SELECT 1 FROM coordinator_meta
                WHERE key = 'maintenance_state'
                  AND value IN ('draining', 'drained', 'recovering')
            )
            BEGIN
                SELECT RAISE(ABORT, '{}');
            END;
            CREATE TRIGGER {}
            BEFORE INSERT ON coordinator_meta
            WHEN NEW.key = 'last_activity'
              AND EXISTS (
                  SELECT 1 FROM coordinator_meta
                  WHERE key = 'maintenance_state'
                    AND value = 'drained'
              )
            BEGIN
                SELECT RAISE(ABORT, '{}');
            END;
            CREATE TRIGGER {}
            BEFORE UPDATE OF value ON coordinator_meta
            WHEN OLD.key = 'last_activity'
              AND EXISTS (
                  SELECT 1 FROM coordinator_meta
                  WHERE key = 'maintenance_state'
                    AND value = 'drained'
              )
            BEGIN
                SELECT RAISE(ABORT, '{}');
            END;
            "#,
            MAINTENANCE_TRIGGERS[0],
            MAINTENANCE_REFUSAL,
            MAINTENANCE_TRIGGERS[1],
            MAINTENANCE_REFUSAL,
            MAINTENANCE_TRIGGERS[2],
            MAINTENANCE_REFUSAL,
        ))
        .map_err(map_database_error)
}

pub(crate) fn remove_maintenance_guards(connection: &Connection) -> Result<()> {
    for name in MAINTENANCE_TRIGGERS {
        connection
            .execute_batch(&format!("DROP TRIGGER IF EXISTS {name}"))
            .map_err(map_database_error)?;
    }
    Ok(())
}

fn maintenance_owner_pid(state_dir: &Path) -> Result<Option<u32>> {
    let Some(raw) = live_owner_metadata(state_dir)? else {
        return Ok(None);
    };
    raw.lines()
        .find_map(|line| line.strip_prefix("pid="))
        .and_then(|value| value.parse::<u32>().ok())
        .filter(|pid| *pid > 0)
        .map(Some)
        .ok_or_else(|| {
            AppError::new(
                "broker-owner-metadata-invalid",
                "live coordinator owner has no valid PID",
            )
        })
}

fn maintenance_value(
    record: &MaintenanceRecord,
    protocol: u64,
    live: i64,
    broker_pid: Option<u32>,
) -> Value {
    json!({
        "state": record.state,
        "drain_id": record.drain_id,
        "reason": record.reason,
        "started_at": record.started_at,
        "protocol": protocol,
        "live": live,
        "broker_pid": broker_pid,
    })
}

pub fn begin_drain(state_dir: &Path, drain_id: &str, reason: &str) -> Result<Value> {
    if !drain_id_valid(drain_id) {
        return Err(AppError::new(
            "broker-drain-id-invalid",
            "maintenance drain ID is invalid",
        ));
    }
    let reason = reason.trim();
    if reason.is_empty() || reason.chars().count() > MAX_MAINTENANCE_REASON || reason.contains('\0')
    {
        return Err(AppError::new(
            "broker-drain-reason-invalid",
            format!("maintenance reason must be 1 to {MAX_MAINTENANCE_REASON} characters"),
        ));
    }
    let paths = Paths::new(state_dir).configured()?;
    if !paths.database.is_file() {
        return Err(AppError::new(
            "broker-state-missing",
            "coordinator database does not exist",
        ));
    }
    let connection = connect(&paths)?;
    connection
        .execute_batch("BEGIN IMMEDIATE")
        .map_err(map_database_error)?;
    let selected = protocol(&connection)?;
    if !matches!(selected, 4 | PROTOCOL) {
        return Err(AppError::new(
            "broker-protocol-unsupported",
            format!("durable draining does not support protocol {selected}"),
        ));
    }
    if maintenance_record(&connection)?.is_some_and(|m| m.state == "recovering") {
        return Err(AppError::new(
            "host-recovery-required",
            "guarded recovery cannot be replaced by a normal drain",
        ));
    }
    if maintenance_record(&connection)?.is_none() {
        connection
            .execute(
                "INSERT INTO coordinator_meta(key, value)
                 VALUES ('last_activity', CAST(strftime('%s', 'now') AS TEXT))
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                [],
            )
            .map_err(map_database_error)?;
        install_maintenance_guards(&connection)?;
        let started_at = now(&connection)?;
        for (key, value) in [
            ("maintenance_state", "draining"),
            ("maintenance_id", drain_id),
            ("maintenance_reason", reason),
            ("maintenance_started_at", started_at.as_str()),
        ] {
            set_metadata(&connection, key, value)?;
        }
    }
    connection
        .execute_batch("COMMIT")
        .map_err(map_database_error)?;
    drain_status(state_dir)
}

pub fn drain_status(state_dir: &Path) -> Result<Value> {
    let paths = Paths::new(state_dir).configured()?;
    if !paths.database.is_file() {
        return Err(AppError::new(
            "broker-state-missing",
            "coordinator database does not exist",
        ));
    }
    let connection = connect(&paths)?;
    connection
        .execute_batch("BEGIN IMMEDIATE")
        .map_err(map_database_error)?;
    let selected = protocol(&connection)?;
    if !matches!(selected, 4 | PROTOCOL) {
        return Err(AppError::new(
            "broker-protocol-unsupported",
            format!("durable draining does not support protocol {selected}"),
        ));
    }
    let mut record = maintenance_record(&connection)?
        .ok_or_else(|| AppError::new("broker-not-draining", "coordinator is not draining"))?;
    let live: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM runs WHERE status IN ('queued', 'running')",
            [],
            |row| row.get(0),
        )
        .map_err(map_database_error)?;
    let broker_pid = maintenance_owner_pid(state_dir)?;
    if record.state == "draining" && live == 0 && broker_pid.is_none() {
        set_metadata(&connection, "maintenance_state", "drained")?;
        record.state = "drained".to_owned();
    }
    connection
        .execute_batch("COMMIT")
        .map_err(map_database_error)?;
    Ok(maintenance_value(&record, selected, live, broker_pid))
}

pub fn resume(state_dir: &Path, drain_id: &str) -> Result<Value> {
    if crate::recovery::valid_id(drain_id) {
        return Err(AppError::new(
            "host-recovery-required",
            "complete verified host recovery instead of resume",
        ));
    }
    if !drain_id_valid(drain_id) {
        return Err(AppError::new(
            "broker-drain-id-invalid",
            "maintenance drain ID is invalid",
        ));
    }
    let owner = match OwnerLock::acquire(state_dir) {
        Ok(owner) => Some(owner),
        Err(error) if error.code == "broker-already-owned" => None,
        Err(error) => return Err(error),
    };
    let paths = Paths::new(state_dir).configured()?;
    let connection = connect(&paths)?;
    connection
        .execute_batch("BEGIN IMMEDIATE")
        .map_err(map_database_error)?;
    let record = maintenance_record(&connection)?
        .ok_or_else(|| AppError::new("broker-not-draining", "coordinator is not draining"))?;
    if record.state == "recovering" {
        return Err(AppError::new(
            "host-recovery-required",
            "complete verified host recovery instead of resume",
        ));
    }
    if record.drain_id != drain_id {
        return Err(AppError::new(
            "broker-drain-id-mismatch",
            "maintenance drain ID does not match",
        ));
    }
    if record.state != "draining" {
        if owner.is_none() {
            return Err(AppError::new(
                "broker-resume-owner-live",
                "cannot resume while a broker or maintenance operation owns the queue",
            ));
        }
        let live: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM runs WHERE status IN ('queued', 'running')",
                [],
                |row| row.get(0),
            )
            .map_err(map_database_error)?;
        if live != 0 {
            return Err(AppError::new(
                "broker-drain-live-work",
                format!("cannot resume while {live} queued or running row(s) remain"),
            ));
        }
    }
    remove_maintenance_guards(&connection)?;
    connection
        .execute(
            "DELETE FROM coordinator_meta WHERE key IN (
                'maintenance_state', 'maintenance_id',
                'maintenance_reason', 'maintenance_started_at'
             )",
            [],
        )
        .map_err(map_database_error)?;
    connection
        .execute_batch("COMMIT")
        .map_err(map_database_error)?;
    drop(owner);
    Ok(json!({"state": "open", "drain_id": drain_id, "resumed": true}))
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
    crate::overflow::ensure_schema(&connection)?;
    ensure_land_batch_schema(&connection)?;
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
    if maintenance_record(&connection)?.is_none() {
        set_metadata(&connection, "last_activity", &now(&connection)?)?;
    }
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
    ensure_land_batch_schema(&connection)?;
    maintenance_record(&connection)?;
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
    let resource_bindings =
        parse_bindings_json(fields.get("resource_bindings").ok_or_else(|| {
            AppError::new(
                "broker-owner-metadata-invalid",
                "live owner resource bindings are missing",
            )
        })?)?;
    let resource_capabilities =
        parse_capabilities_json(fields.get("resource_capabilities").ok_or_else(|| {
            AppError::new(
                "broker-owner-metadata-invalid",
                "live owner resource capabilities are missing",
            )
        })?)?;
    let local_runtime = match fields.get("local_runtime") {
        Some(value) => crate::overflow::parse_local_runtime_json(value, &capacities)?,
        None => crate::overflow::Runtime::local(capacities.clone(), BTreeSet::new()),
    };
    // An owner without overflow pools, or one that predates them, publishes none.
    let overflow_pools = fields
        .get("overflow_pools")
        .map(|value| crate::overflow::parse_pools_json(value))
        .transpose()?
        .unwrap_or_default();
    Ok(OwnerInfo {
        pid,
        capacities,
        resource_bindings,
        resource_capabilities,
        local_runtime,
        overflow_pools,
    })
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
    let resource_receipt: Value = parse_json(
        row.get("resource_receipt_json")
            .map_err(map_database_error)?,
        &run_id,
        "resource receipt",
    )?;
    if !receipt_valid(&resource_receipt, &resources) {
        return Err(AppError::new(
            "broker-row-invalid",
            format!("run {run_id} has invalid stored resource receipt"),
        ));
    }
    let resource_contract: Value = parse_json(
        row.get("resource_contract_json")
            .map_err(map_database_error)?,
        &run_id,
        "resource contract",
    )?;
    let contract_bindings = parse_bindings(Some(&resource_contract)).map_err(|_| {
        AppError::new(
            "broker-row-invalid",
            format!("run {run_id} has invalid stored resource contract"),
        )
    })?;
    if contract_bindings.keys().collect::<BTreeSet<_>>()
        != resources.keys().collect::<BTreeSet<_>>()
    {
        return Err(AppError::new(
            "broker-row-invalid",
            format!("run {run_id} resource contract does not match its request"),
        ));
    }
    let resource_state = {
        let value: Value = parse_json(
            row.get("resource_state_json").map_err(map_database_error)?,
            &run_id,
            "resource state",
        )?;
        parse_backend_state(&value).ok_or_else(|| {
            AppError::new(
                "broker-row-invalid",
                format!("run {run_id} has invalid stored resource state"),
            )
        })?
    };
    if resource_state.iter().any(|(backend, state)| {
        state.resources.iter().any(|name| {
            contract_bindings
                .get(name)
                .and_then(|binding| binding.backend.as_deref())
                != Some(backend)
        })
    }) {
        return Err(AppError::new(
            "broker-row-invalid",
            format!("run {run_id} resource state does not match its contract"),
        ));
    }
    let receipt_resources_match = |section: &str| {
        resource_receipt
            .get(section)
            .and_then(Value::as_object)
            .is_some_and(|values| {
                values
                    .keys()
                    .all(|name| contract_bindings.get(name).is_some_and(Binding::enforced))
            })
    };
    let receipt_events_match = resource_receipt
        .get("events")
        .and_then(Value::as_array)
        .is_some_and(|events| {
            events.iter().all(|event| {
                event
                    .get("resource")
                    .and_then(Value::as_str)
                    .and_then(|name| contract_bindings.get(name))
                    .and_then(|binding| binding.backend.as_deref())
                    == event.get("backend").and_then(Value::as_str)
            })
        });
    if !receipt_resources_match("applied")
        || !receipt_resources_match("peak")
        || !receipt_events_match
    {
        return Err(AppError::new(
            "broker-row-invalid",
            format!("run {run_id} resource receipt does not match its contract"),
        ));
    }
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
        resource_contract,
        resource_receipt,
        resource_state,
        gate_run_id: row.get("gate_run_id").map_err(map_database_error)?,
        publication_adapter: row.get("publication_adapter").map_err(map_database_error)?,
        publication_request,
        failure_reason: row.get("failure_reason").map_err(map_database_error)?,
        gate_exit_status: row.get("gate_exit_status").map_err(map_database_error)?,
        reported_exit_status: row
            .get("reported_exit_status")
            .map_err(map_database_error)?,
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

pub struct LiveSchedulerState {
    pub active: Vec<RunRecord>,
    pub queued: Vec<RunRecord>,
    pub placed: BTreeMap<String, String>,
    pub batch_followers: BTreeMap<String, String>,
    pub batch_owners: BTreeMap<String, String>,
}

/// Load live runs, pool placements, and active land-batch membership from one snapshot.
pub fn load_live_scheduler_state(connection: &Connection) -> Result<LiveSchedulerState> {
    ensure_land_batch_schema(connection)?;
    connection
        .execute_batch("BEGIN DEFERRED")
        .map_err(map_database_error)?;
    let loaded = (|| {
        let runs = load_live_runs(connection)?;
        let active: Vec<_> = runs
            .iter()
            .filter(|run| run.status == "running")
            .cloned()
            .collect();
        let mut queued: Vec<_> = runs
            .iter()
            .filter(|run| run.status == "queued")
            .cloned()
            .collect();
        crate::recovery::filter_queued(connection, &mut queued)?;
        let placed = crate::overflow::live_placements(connection)?;
        let batch_followers = active_batch_followers(connection)?;
        let batch_owners = active_batch_owners(connection)?;
        Ok(LiveSchedulerState {
            active,
            queued,
            placed,
            batch_followers,
            batch_owners,
        })
    })();
    match loaded {
        Ok(state) => {
            connection
                .execute_batch("COMMIT")
                .map_err(map_database_error)?;
            Ok(state)
        }
        Err(error) => {
            let _ = connection.execute_batch("ROLLBACK");
            Err(error)
        }
    }
}

/// Load queued and running rows in submission order.
///
/// Scheduling, observation, and a queued row's position depend only on live work, so these
/// paths read it through the status index instead of decoding terminal history.
pub fn load_live_runs(connection: &Connection) -> Result<Vec<RunRecord>> {
    let mut statement = connection
        .prepare("SELECT * FROM runs WHERE status IN ('queued', 'running') ORDER BY sequence")
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

/// The summed claims of the running rows that hold this machine's own capacity.
pub fn local_allocations(
    active: &[RunRecord],
    placed: &BTreeMap<String, String>,
    batch_followers: &BTreeMap<String, String>,
) -> BTreeMap<String, u64> {
    let local: Vec<_> = active
        .iter()
        .filter(|run| {
            !placed.contains_key(&run.run_id) && !batch_followers.contains_key(&run.run_id)
        })
        .cloned()
        .collect();
    allocations(&local)
}

pub fn blocked_by(
    run: &RunRecord,
    active: &[RunRecord],
    queued: &[RunRecord],
    local_runtime: &crate::overflow::Runtime,
    placed: &BTreeMap<String, String>,
    batch_followers: &BTreeMap<String, String>,
) -> Vec<String> {
    if run.status != "queued" {
        return Vec::new();
    }
    // Pool placements keep their repository exclusions but consume no local capacity.
    let used = local_allocations(active, placed, batch_followers);
    let front = front_row(active, queued, local_runtime, &used)
        .filter(|front| run.sequence > front.sequence);
    row_blockers(run, active, queued, local_runtime, &used, front)
}

/// The oldest locally feasible queued row blocked only by raw resource shortages.
pub fn front_row<'a>(
    active: &[RunRecord],
    queued: &'a [RunRecord],
    local_runtime: &crate::overflow::Runtime,
    used: &BTreeMap<String, u64>,
) -> Option<&'a RunRecord> {
    queued
        .iter()
        .filter(|candidate| {
            // A claim accepted only for overflow must never reserve this machine.
            let Ok((allowed_runtimes, required_capabilities)) =
                crate::overflow::parse_placement_constraints(&candidate.environment)
            else {
                return false;
            };
            allowed_runtimes
                .as_ref()
                .is_none_or(|allowed| allowed.contains(crate::overflow::LOCAL_RUNTIME))
                && local_runtime.satisfies(&candidate.resources, &required_capabilities)
        })
        .filter(|candidate| {
            // Select from raw blockers, before any reservation is applied. A row waiting
            // on its repository, barrier, or worktree reserves nothing.
            let raw = row_blockers(candidate, active, queued, local_runtime, used, None);
            !raw.is_empty() && raw.iter().all(|reason| reason.starts_with("resource:"))
        })
        .min_by_key(|candidate| candidate.sequence)
}

/// Without a front row, these are the raw repository and local-resource blockers.
fn row_blockers(
    run: &RunRecord,
    active: &[RunRecord],
    queued: &[RunRecord],
    local_runtime: &crate::overflow::Runtime,
    used: &BTreeMap<String, u64>,
    front: Option<&RunRecord>,
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
    // Exact-head gates for a branch share a coordinator-owned checkout.  Reserve it
    // from admission through completion, including while the launcher prepares it.
    if matches!(run.kind.as_str(), "full" | "land") {
        let same_gate_tree = |candidate: &&RunRecord| {
            matches!(candidate.kind.as_str(), "full" | "land") && candidate.branch == run.branch
        };
        if let Some(candidate) = same_active.iter().copied().find(same_gate_tree) {
            reasons.push(format!("gate-tree:active:{}", candidate.run_id));
        }
        if let Some(candidate) = earlier.iter().copied().find(same_gate_tree) {
            reasons.push(format!("gate-tree:fifo:{}", candidate.run_id));
        }
    }
    // A barrier (land or retained merge) excludes every other barrier in its lane and
    // every job that shares its worktree.  Ordinary work in another worktree of the same
    // repository only competes for capacity.
    let conflicts_with_barrier =
        |candidate: &RunRecord| candidate.barrier || candidate.worktree_id == run.worktree_id;
    if run.barrier {
        reasons.extend(
            same_active
                .iter()
                .filter(|candidate| conflicts_with_barrier(candidate))
                .map(|candidate| {
                    format!(
                        "repository:{}:active:{}",
                        run.repository_id, candidate.run_id
                    )
                }),
        );
        if let Some(candidate) = earlier
            .iter()
            .find(|candidate| conflicts_with_barrier(candidate))
        {
            reasons.push(format!(
                "repository:{}:fifo:{}",
                run.repository_id, candidate.run_id
            ));
        }
    } else if let Some(candidate) = same_active
        .iter()
        .copied()
        .chain(earlier.iter().copied())
        .find(|candidate| candidate.barrier && candidate.worktree_id == run.worktree_id)
    {
        reasons.push(format!(
            "repository:{}:barrier:{}",
            run.repository_id, candidate.run_id
        ));
    }
    if let Ok((allowed_runtimes, required_capabilities)) =
        crate::overflow::parse_placement_constraints(&run.environment)
    {
        if allowed_runtimes
            .as_ref()
            .is_some_and(|allowed| !allowed.contains(crate::overflow::LOCAL_RUNTIME))
        {
            reasons.push("runtime:local".to_owned());
        }
        for capability in &required_capabilities {
            if !local_runtime.capabilities.contains(capability) {
                reasons.push(format!("capability:{capability}"));
            }
        }
    }
    let capacities = &local_runtime.capacities;
    for (name, units) in &run.resources {
        if used.get(name).copied().unwrap_or(0) + units > capacities.get(name).copied().unwrap_or(0)
        {
            reasons.push(format!("resource:{name}"));
        } else if let Some(front) = front {
            let front_units = front.resources.get(name).copied().unwrap_or(0);
            if front_units > 0
                && used
                    .get(name)
                    .copied()
                    .unwrap_or(0)
                    .saturating_add(front_units)
                    .saturating_add(*units)
                    > capacities.get(name).copied().unwrap_or(0)
            {
                reasons.push(format!("reservation:{}:{name}", front.run_id));
            }
        }
    }
    reasons
}

pub fn public_run(
    paths: &Paths,
    run: &RunRecord,
    position: Option<usize>,
    blockers: Vec<String>,
    placed: &BTreeMap<String, String>,
    batch: Value,
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
        "pool": placed.get(&run.run_id),
        "gate_run_id": run.gate_run_id,
        "publication": publication,
        "batch": batch,
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

pub fn snapshot(paths: &Paths, filter: &SnapshotFilter) -> Result<Value> {
    let connection = open_protocol5(paths)?;
    let maintenance = maintenance_record(&connection)?;
    let owner = if live_owner_metadata(&paths.state_dir)?.is_some() {
        Some(owner_info(paths)?)
    } else {
        None
    };
    if owner.is_none() && maintenance.is_none() {
        return Err(AppError::new(
            "broker-not-running",
            "no native broker owns this state directory",
        ));
    }
    let runs = load_runs(&connection)?;
    let placed = crate::overflow::all_placements(&connection)?;
    let mut known_pools = BTreeSet::new();
    let mut known_repositories = BTreeMap::new();
    let mut known_agents = BTreeSet::new();
    let mut known_kinds = BTreeSet::new();
    for run in &runs {
        known_pools.insert(
            placed
                .get(&run.run_id)
                .map(String::as_str)
                .unwrap_or("local")
                .to_owned(),
        );
        known_repositories.insert(run.repository_id.clone(), run.repository.clone());
        if !run.agent.is_empty() {
            known_agents.insert(run.agent.clone());
        }
        if !run.kind.is_empty() {
            known_kinds.insert(run.kind.clone());
        }
    }
    let all_active: Vec<_> = runs
        .iter()
        .filter(|run| run.status == "running")
        .cloned()
        .collect();
    let all_queued: Vec<_> = runs
        .iter()
        .filter(|run| run.status == "queued")
        .cloned()
        .collect();
    let active: Vec<_> = all_active
        .iter()
        .filter(|run| matches_snapshot_filter(run, filter, &placed))
        .cloned()
        .collect();
    let queued: Vec<_> = all_queued
        .iter()
        .filter(|run| matches_snapshot_filter(run, filter, &placed))
        .cloned()
        .collect();
    let mut terminal: Vec<_> = runs
        .iter()
        .filter(|run| is_terminal(&run.status) && matches_snapshot_filter(run, filter, &placed))
        .cloned()
        .collect();
    terminal.sort_by(|run_a, run_b| {
        let finish_a = run_a.finished_at.as_deref().unwrap_or(&run_a.created_at);
        let finish_b = run_b.finished_at.as_deref().unwrap_or(&run_b.created_at);
        finish_b
            .cmp(finish_a)
            .then_with(|| run_b.sequence.cmp(&run_a.sequence))
    });
    let capacities = if let Some(owner) = &owner {
        owner.capacities.clone()
    } else {
        match metadata(&connection, "capacities_json")? {
            Some(value) => parse_capacities(&value).map_err(|_| {
                AppError::new(
                    "broker-schema-invalid",
                    "drained coordinator capacities are invalid",
                )
            })?,
            None => crate::resources::load_configuration(&paths.state_dir)?.capacities,
        }
    };
    let resource_bindings = if let Some(owner) = &owner {
        crate::resources::bindings_value(&owner.resource_bindings)
    } else {
        let configuration = crate::resources::load_configuration(&paths.state_dir)?;
        crate::resources::bindings_value(&configuration.bindings)
    };
    let resource_capabilities = owner
        .as_ref()
        .map(|owner| owner.resource_capabilities.clone())
        .unwrap_or_else(|| json!({}));
    let local_runtime = match &owner {
        Some(owner) => owner.local_runtime.clone(),
        None => crate::overflow::Runtime::local(
            capacities.clone(),
            crate::overflow::load_local_capabilities(&paths.state_dir)?,
        ),
    };
    let overflow_pools = match &owner {
        Some(owner) => owner.overflow_pools.clone(),
        None => crate::overflow::load_pools(&paths.state_dir)?,
    };
    let live_placed = crate::overflow::live_placements(&connection)?;
    let batch_followers = active_batch_followers(&connection)?;
    let mut selected_allocations = BTreeMap::new();
    let used = local_allocations(&all_active, &live_placed, &batch_followers);
    for name in capacities.keys() {
        selected_allocations.insert(name.clone(), used.get(name).copied().unwrap_or(0));
    }
    let limit = filter.limit.unwrap_or(RECENT_LIMIT);
    let recent_runs: Vec<_> = terminal.iter().take(limit).collect();
    let mut batch_cache: BTreeMap<i64, (String, Value)> = BTreeMap::new();
    let mut active_public = Vec::with_capacity(active.len());
    for run in &active {
        let batch = batch_for_run_cached(&connection, run, &mut batch_cache)?;
        active_public.push(public_run(paths, run, None, Vec::new(), &placed, batch));
    }
    let mut queued_public = Vec::with_capacity(queued.len());
    for (index, run) in queued.iter().enumerate() {
        let batch = batch_for_run_cached(&connection, run, &mut batch_cache)?;
        queued_public.push(public_run(
            paths,
            run,
            Some(index + 1),
            blocked_by(
                run,
                &all_active,
                &all_queued,
                &local_runtime,
                &live_placed,
                &batch_followers,
            ),
            &placed,
            batch,
        ));
    }
    let mut recent = Vec::with_capacity(recent_runs.len());
    for run in recent_runs {
        let batch = batch_for_run_cached(&connection, run, &mut batch_cache)?;
        recent.push(public_run(paths, run, None, Vec::new(), &placed, batch));
    }
    Ok(json!({
        "protocol": PROTOCOL,
        "broker_pid": owner.as_ref().map(|owner| owner.pid),
        "captured_at": now(&connection)?,
        "capacities": capacities,
        "allocations": selected_allocations,
        "resource_bindings": resource_bindings,
        "resource_capabilities": resource_capabilities,
        "overflow_pools": crate::overflow::public_pools(
            &connection,
            &overflow_pools,
            &all_active,
            &live_placed,
        )?,
        "maintenance": maintenance.as_ref().map(|record| maintenance_value(
            record,
            PROTOCOL,
            (all_active.len() + all_queued.len()) as i64,
            owner.as_ref().map(|owner| owner.pid),
        )),
        "available_filters": json!({
            "repositories": known_repositories,
            "agents": known_agents,
            "kinds": known_kinds,
            "pools": known_pools,
        }),
        "active": active_public,
        "queued": queued_public,
        "recent": recent,
    }))
}

pub fn status(paths: &Paths, run_id: &str) -> Result<Value> {
    let connection = open_protocol5(paths)?;
    // One read transaction ranks the row against the live work of the same instant.
    connection
        .execute_batch("BEGIN")
        .map_err(map_database_error)?;
    let runs = load_live_runs(&connection)?;
    let run = match runs.iter().find(|run| run.run_id == run_id) {
        Some(run) => run.clone(),
        None => load_run(&connection, run_id)?,
    };
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
    let live_owner = live_owner_metadata(&paths.state_dir)?
        .map(|_| owner_info(paths))
        .transpose()?;
    let local_runtime = match live_owner {
        Some(owner) => owner.local_runtime,
        None => {
            let capacities = metadata(&connection, "capacities_json")
                .ok()
                .flatten()
                .and_then(|value| serde_json::from_str(&value).ok())
                .unwrap_or_default();
            let capabilities =
                crate::overflow::load_local_capabilities(&paths.state_dir).unwrap_or_default();
            crate::overflow::Runtime::local(capacities, capabilities)
        }
    };
    let live_placed = crate::overflow::live_placements(&connection)?;
    let placed = crate::overflow::all_placements(&connection)?;
    let batch_followers = active_batch_followers(&connection)?;
    let mut batch_cache = BTreeMap::new();
    let batch = batch_for_run_cached(&connection, &run, &mut batch_cache)?;
    let mut result = public_run(
        paths,
        &run,
        position,
        blocked_by(
            &run,
            &active,
            &queued,
            &local_runtime,
            &live_placed,
            &batch_followers,
        ),
        &placed,
        batch,
    );
    result["batch_slice"] = batch_slice(&connection, &run.run_id)?;
    Ok(result)
}

pub fn log(paths: &Paths, run_id: &str, offset: u64, limit: usize) -> Result<Value> {
    if limit == 0 || limit > MAX_LOG_BYTES {
        return Err(AppError::new(
            "broker-log-range-invalid",
            format!("gate log limit must be between 1 and {MAX_LOG_BYTES}"),
        ));
    }
    let connection = open_protocol5(paths)?;
    load_run(&connection, run_id)?;
    let path = paths.logs.join(format!("{run_id}.log"));
    let size = match fs::metadata(&path) {
        Ok(metadata) => metadata.len(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
        Err(error) => {
            return Err(AppError::new(
                "broker-log-read-failed",
                format!("cannot inspect gate log for {run_id}: {error}"),
            ));
        }
    };
    if offset > size {
        return Err(AppError::new(
            "broker-log-range-invalid",
            format!("gate log offset {offset} is past its {size}-byte end"),
        ));
    }
    let mut data = Vec::new();
    if size > 0 {
        let mut file = File::open(&path).map_err(|error| {
            AppError::new(
                "broker-log-read-failed",
                format!("cannot open gate log for {run_id}: {error}"),
            )
        })?;
        file.seek(SeekFrom::Start(offset)).map_err(|error| {
            AppError::new(
                "broker-log-read-failed",
                format!("cannot seek gate log for {run_id}: {error}"),
            )
        })?;
        file.take(limit as u64)
            .read_to_end(&mut data)
            .map_err(|error| {
                AppError::new(
                    "broker-log-read-failed",
                    format!("cannot read gate log for {run_id}: {error}"),
                )
            })?;
    }
    let next_offset = offset + data.len() as u64;
    Ok(json!({
        "run_id": run_id,
        "offset": offset,
        "next_offset": next_offset,
        "text": String::from_utf8_lossy(&data),
        "eof": next_offset >= size,
    }))
}

fn child_lease_from_row(row: &Row<'_>) -> Result<ChildLeaseRecord> {
    let lease_id: String = row.get("lease_id").map_err(map_database_error)?;
    let positive = |value: i64, subject: &str| {
        u64::try_from(value)
            .ok()
            .filter(|selected| *selected > 0)
            .ok_or_else(|| {
                AppError::new(
                    "broker-lease-row-invalid",
                    format!("child CPU lease {lease_id} has invalid {subject}"),
                )
            })
    };
    let non_negative = |value: i64, subject: &str| {
        u64::try_from(value).map_err(|_| {
            AppError::new(
                "broker-lease-row-invalid",
                format!("child CPU lease {lease_id} has invalid {subject}"),
            )
        })
    };
    let owner_pid_value: i64 = row.get("owner_pid").map_err(map_database_error)?;
    let owner_pid = u32::try_from(owner_pid_value)
        .ok()
        .filter(|pid| *pid > 0)
        .ok_or_else(|| {
            AppError::new(
                "broker-lease-row-invalid",
                format!("child CPU lease {lease_id} has invalid owner PID"),
            )
        })?;
    let record = ChildLeaseRecord {
        sequence: row.get("sequence").map_err(map_database_error)?,
        lease_id: lease_id.clone(),
        run_id: row.get("run_id").map_err(map_database_error)?,
        status: row.get("status").map_err(map_database_error)?,
        requested: positive(row.get("requested").map_err(map_database_error)?, "request")?,
        minimum: positive(row.get("minimum").map_err(map_database_error)?, "minimum")?,
        granted: non_negative(row.get("granted").map_err(map_database_error)?, "grant")?,
        owner_pid,
        owner_start_token: row.get("owner_start_token").map_err(map_database_error)?,
        bypass_count: non_negative(
            row.get("bypass_count").map_err(map_database_error)?,
            "bypass count",
        )?,
        created_at: row.get("created_at").map_err(map_database_error)?,
        acquired_at: row.get("acquired_at").map_err(map_database_error)?,
        finished_at: row.get("finished_at").map_err(map_database_error)?,
    };
    let coherent = record.sequence > 0
        && identifier_valid(&record.lease_id)
        && identifier_valid(&record.run_id)
        && !record.owner_start_token.is_empty()
        && record.minimum <= record.requested
        && record.granted <= record.requested
        && record.bypass_count <= 1
        && match record.status.as_str() {
            "waiting" => {
                record.granted == 0 && record.acquired_at.is_none() && record.finished_at.is_none()
            }
            "active" => {
                record.granted >= record.minimum
                    && record.acquired_at.is_some()
                    && record.finished_at.is_none()
            }
            "released" => record.finished_at.is_some(),
            "cancelled" => record.finished_at.is_some(),
            _ => false,
        };
    if !coherent {
        return Err(AppError::new(
            "broker-lease-row-invalid",
            format!(
                "child CPU lease {} is structurally invalid",
                record.lease_id
            ),
        ));
    }
    Ok(record)
}

fn load_child_leases(connection: &Connection) -> Result<Vec<ChildLeaseRecord>> {
    let mut statement = connection
        .prepare("SELECT * FROM child_cpu_leases ORDER BY sequence")
        .map_err(map_database_error)?;
    let mut rows = statement.query([]).map_err(map_database_error)?;
    let mut selected = Vec::new();
    while let Some(row) = rows.next().map_err(map_database_error)? {
        selected.push(child_lease_from_row(row)?);
    }
    Ok(selected)
}

fn load_live_child_leases(connection: &Connection) -> Result<Vec<ChildLeaseRecord>> {
    let mut statement = connection
        .prepare(
            "SELECT * FROM child_cpu_leases
             WHERE status IN ('waiting', 'active') ORDER BY sequence",
        )
        .map_err(map_database_error)?;
    let mut rows = statement.query([]).map_err(map_database_error)?;
    let mut selected = Vec::new();
    while let Some(row) = rows.next().map_err(map_database_error)? {
        selected.push(child_lease_from_row(row)?);
    }
    Ok(selected)
}

fn load_child_lease(connection: &Connection, lease_id: &str) -> Result<ChildLeaseRecord> {
    let mut statement = connection
        .prepare("SELECT * FROM child_cpu_leases WHERE lease_id = ?1")
        .map_err(map_database_error)?;
    let mut rows = statement
        .query(params![lease_id])
        .map_err(map_database_error)?;
    let row = rows.next().map_err(map_database_error)?.ok_or_else(|| {
        AppError::new(
            "broker-lease-unknown",
            format!("unknown child CPU lease {lease_id:?}"),
        )
    })?;
    child_lease_from_row(row)
}

fn child_lease_position(connection: &Connection, lease: &ChildLeaseRecord) -> Result<Option<u64>> {
    if lease.status != "waiting" {
        return Ok(None);
    }
    let count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM child_cpu_leases
             WHERE run_id = ?1 AND status = 'waiting' AND sequence <= ?2",
            params![lease.run_id, lease.sequence],
            |row| row.get(0),
        )
        .map_err(map_database_error)?;
    u64::try_from(count).map(Some).map_err(|_| {
        AppError::new(
            "broker-lease-row-invalid",
            "child CPU lease queue position is invalid",
        )
    })
}

fn public_child_lease(lease: &ChildLeaseRecord, position: Option<u64>) -> Value {
    json!({
        "lease_id": lease.lease_id,
        "run_id": lease.run_id,
        "status": lease.status,
        "requested": lease.requested,
        "minimum": lease.minimum,
        "granted": lease.granted,
        "full": lease.granted > 0 && lease.granted == lease.requested,
        "owner_pid": lease.owner_pid,
        "created_at": lease.created_at,
        "acquired_at": lease.acquired_at,
        "finished_at": lease.finished_at,
        "position": position,
    })
}

pub fn validate_child_cpu_leases(connection: &Connection) -> Result<()> {
    let leases = load_child_leases(connection)?;
    let mut active_by_run: BTreeMap<String, u64> = BTreeMap::new();
    for lease in &leases {
        let parent = load_run(connection, &lease.run_id)?;
        let budget = parent.resources.get("cpu").copied().ok_or_else(|| {
            AppError::new(
                "broker-lease-row-invalid",
                format!(
                    "child CPU lease {} has a parent without a CPU budget",
                    lease.lease_id
                ),
            )
        })?;
        if lease.minimum > budget {
            return Err(AppError::new(
                "broker-lease-row-invalid",
                format!(
                    "child CPU lease {} exceeds its parent budget",
                    lease.lease_id
                ),
            ));
        }
        if lease.status == "active" {
            let used = active_by_run.entry(lease.run_id.clone()).or_default();
            *used = used.checked_add(lease.granted).ok_or_else(|| {
                AppError::new(
                    "broker-lease-row-invalid",
                    "child CPU lease allocation overflowed",
                )
            })?;
            if *used > budget {
                return Err(AppError::new(
                    "broker-lease-row-invalid",
                    format!(
                        "active child CPU leases exceed parent {} budget",
                        lease.run_id
                    ),
                ));
            }
        }
    }
    Ok(())
}

pub fn request_child_cpu_lease(paths: &Paths, request: &ChildLeaseRequest) -> Result<Value> {
    let _owner = owner_info(paths)?;
    if !identifier_valid(&request.lease_id)
        || !identifier_valid(&request.run_id)
        || request.requested == 0
        || request.requested > i64::MAX as u64
        || request.minimum == 0
        || request.minimum > request.requested
        || request.owner_start_token.is_empty()
    {
        return Err(AppError::new(
            "broker-lease-request-invalid",
            "child CPU lease request is invalid",
        ));
    }
    let connection = open_protocol5(paths)?;
    connection
        .execute_batch("BEGIN IMMEDIATE")
        .map_err(map_database_error)?;
    let parent = load_run(&connection, &request.run_id)?;
    let admitted = parent.status == "running"
        && !parent.cancel_requested
        && parent.worker_pid.is_some()
        && parent.worker_start_token.is_some()
        && same_worker_process(parent.worker_pid, parent.worker_start_token.as_deref())
        && same_process(request.owner_pid, &request.owner_start_token)
        && is_descendant_process(
            request.owner_pid,
            &request.owner_start_token,
            parent.worker_pid.unwrap(),
            parent.worker_start_token.as_deref().unwrap(),
        );
    if !admitted {
        return Err(AppError::new(
            "broker-lease-parent-invalid",
            format!(
                "caller is not a live descendant of admitted run {}",
                request.run_id
            ),
        ));
    }
    let budget = parent.resources.get("cpu").copied().ok_or_else(|| {
        AppError::new(
            "broker-lease-budget-missing",
            format!("parent run {} has no 'cpu' resource budget", request.run_id),
        )
    })?;
    if request.minimum > budget {
        return Err(AppError::new(
            "broker-lease-budget-exceeded",
            format!(
                "child CPU lease minimum {} exceeds parent budget {budget}",
                request.minimum
            ),
        ));
    }
    connection
        .execute(
            "INSERT INTO child_cpu_leases (
                lease_id, run_id, status, requested, minimum, owner_pid,
                owner_start_token, created_at
             ) VALUES (?1, ?2, 'waiting', ?3, ?4, ?5, ?6, ?7)",
            params![
                request.lease_id,
                request.run_id,
                request.requested as i64,
                request.minimum as i64,
                i64::from(request.owner_pid),
                request.owner_start_token,
                now(&connection)?,
            ],
        )
        .map_err(|error| {
            if matches!(error, rusqlite::Error::SqliteFailure(_, _)) {
                AppError::new(
                    "broker-lease-exists",
                    format!("child CPU lease {} already exists", request.lease_id),
                )
            } else {
                map_database_error(error)
            }
        })?;
    set_metadata(&connection, "last_activity", &now(&connection)?)?;
    connection
        .execute_batch("COMMIT")
        .map_err(map_database_error)?;
    let lease = load_child_lease(&connection, &request.lease_id)?;
    let position = child_lease_position(&connection, &lease)?;
    Ok(public_child_lease(&lease, position))
}

pub fn child_cpu_lease_status(paths: &Paths, lease_id: &str) -> Result<Value> {
    let _owner = owner_info(paths)?;
    let connection = open_protocol5(paths)?;
    let lease = load_child_lease(&connection, lease_id)?;
    let position = child_lease_position(&connection, &lease)?;
    Ok(public_child_lease(&lease, position))
}

pub fn child_cpu_leases(
    paths: &Paths,
    run_id: Option<&str>,
    include_terminal: bool,
) -> Result<Value> {
    let _owner = owner_info(paths)?;
    let connection = open_protocol5(paths)?;
    if let Some(run_id) = run_id {
        load_run(&connection, run_id)?;
    }
    let leases = load_child_leases(&connection)?;
    let mut waiting_positions: BTreeMap<String, u64> = BTreeMap::new();
    let selected = leases
        .iter()
        .filter(|lease| run_id.is_none_or(|selected| lease.run_id == selected))
        .filter(|lease| include_terminal || matches!(lease.status.as_str(), "waiting" | "active"))
        .map(|lease| {
            let position = if lease.status == "waiting" {
                let value = waiting_positions.entry(lease.run_id.clone()).or_default();
                *value += 1;
                Some(*value)
            } else {
                None
            };
            public_child_lease(lease, position)
        })
        .collect::<Vec<_>>();
    Ok(Value::Array(selected))
}

pub fn finish_child_cpu_lease(
    paths: &Paths,
    request: &ChildLeaseOwnerRequest,
    status: &str,
) -> Result<Value> {
    if !matches!(status, "released" | "cancelled") {
        return Err(AppError::new(
            "broker-lease-request-invalid",
            "child CPU lease terminal status is invalid",
        ));
    }
    let _owner = owner_info(paths)?;
    let connection = open_protocol5(paths)?;
    connection
        .execute_batch("BEGIN IMMEDIATE")
        .map_err(map_database_error)?;
    let lease = load_child_lease(&connection, &request.lease_id)?;
    if lease.owner_pid != request.owner_pid
        || lease.owner_start_token != request.owner_start_token
        || !same_process(request.owner_pid, &request.owner_start_token)
    {
        return Err(AppError::new(
            "broker-lease-owner-mismatch",
            format!("caller does not own child CPU lease {}", request.lease_id),
        ));
    }
    if matches!(lease.status.as_str(), "waiting" | "active") {
        connection
            .execute(
                "UPDATE child_cpu_leases SET status = ?1, finished_at = ?2
                 WHERE lease_id = ?3 AND status IN ('waiting', 'active')",
                params![status, now(&connection)?, request.lease_id],
            )
            .map_err(map_database_error)?;
    }
    set_metadata(&connection, "last_activity", &now(&connection)?)?;
    connection
        .execute_batch("COMMIT")
        .map_err(map_database_error)?;
    let lease = load_child_lease(&connection, &request.lease_id)?;
    Ok(public_child_lease(&lease, None))
}

pub fn maintain_child_cpu_leases(connection: &Connection) -> Result<()> {
    // Every step below acts on a waiting or active lease. Without one, skip the write lock
    // instead of taking it on every scheduling tick; a lease requested after this check is
    // maintained on the next tick.
    let live: bool = connection
        .query_row(
            "SELECT EXISTS(
                SELECT 1 FROM child_cpu_leases WHERE status IN ('waiting', 'active')
            )",
            [],
            |row| row.get(0),
        )
        .map_err(map_database_error)?;
    if !live {
        return Ok(());
    }
    connection
        .execute_batch("BEGIN IMMEDIATE")
        .map_err(map_database_error)?;
    let result = (|| {
        let timestamp = now(connection)?;
        let leases = load_live_child_leases(connection)?;
        for lease in &leases {
            let parent = load_run(connection, &lease.run_id)?;
            let valid = parent.status == "running"
                && !parent.cancel_requested
                && parent.worker_pid.is_some()
                && parent.worker_start_token.is_some()
                && same_worker_process(parent.worker_pid, parent.worker_start_token.as_deref())
                && same_process(lease.owner_pid, &lease.owner_start_token)
                && is_descendant_process(
                    lease.owner_pid,
                    &lease.owner_start_token,
                    parent.worker_pid.unwrap(),
                    parent.worker_start_token.as_deref().unwrap(),
                );
            if !valid {
                connection
                    .execute(
                        "UPDATE child_cpu_leases SET status = 'cancelled', finished_at = ?1
                         WHERE lease_id = ?2 AND status IN ('waiting', 'active')",
                        params![timestamp, lease.lease_id],
                    )
                    .map_err(map_database_error)?;
            }
        }

        let leases = load_live_child_leases(connection)?;
        let run_ids = leases
            .iter()
            .filter(|lease| matches!(lease.status.as_str(), "waiting" | "active"))
            .map(|lease| lease.run_id.clone())
            .collect::<BTreeSet<_>>();
        for run_id in run_ids {
            let parent = load_run(connection, &run_id)?;
            if parent.status != "running" || parent.cancel_requested {
                continue;
            }
            let budget = parent.resources.get("cpu").copied().ok_or_else(|| {
                AppError::new(
                    "broker-lease-row-invalid",
                    format!("parent run {run_id} has live child leases without a CPU budget"),
                )
            })?;
            let active = load_live_child_leases(connection)?
                .into_iter()
                .filter(|lease| lease.run_id == run_id && lease.status == "active")
                .collect::<Vec<_>>();
            let used = active.iter().try_fold(0_u64, |total, lease| {
                total.checked_add(lease.granted).ok_or_else(|| {
                    AppError::new(
                        "broker-lease-row-invalid",
                        "child CPU lease allocation overflowed",
                    )
                })
            })?;
            if used > budget {
                return Err(AppError::new(
                    "broker-lease-row-invalid",
                    format!("child CPU leases for {run_id} exceed the parent budget"),
                ));
            }
            let mut available = budget - used;
            let mut waiting = load_live_child_leases(connection)?
                .into_iter()
                .filter(|lease| lease.run_id == run_id && lease.status == "waiting")
                .collect::<Vec<_>>();
            while available > 0 && !waiting.is_empty() {
                let selected_index = if waiting[0].minimum <= available {
                    Some(0)
                } else if waiting[0].bypass_count >= 1 {
                    None
                } else {
                    waiting
                        .iter()
                        .enumerate()
                        .skip(1)
                        .find(|(_index, lease)| lease.minimum <= available)
                        .map(|(index, _lease)| index)
                };
                let Some(selected_index) = selected_index else {
                    break;
                };
                if selected_index > 0 {
                    connection
                        .execute(
                            "UPDATE child_cpu_leases SET bypass_count = bypass_count + 1
                             WHERE lease_id = ?1",
                            params![waiting[0].lease_id],
                        )
                        .map_err(map_database_error)?;
                    waiting[0].bypass_count += 1;
                }
                let selected = waiting.remove(selected_index);
                let granted = selected.requested.min(available);
                if granted < selected.minimum {
                    return Err(AppError::new(
                        "broker-lease-row-invalid",
                        "child CPU lease scheduler selected an impossible grant",
                    ));
                }
                connection
                    .execute(
                        "UPDATE child_cpu_leases SET status = 'active', granted = ?1,
                         acquired_at = ?2 WHERE lease_id = ?3 AND status = 'waiting'",
                        params![granted as i64, timestamp, selected.lease_id],
                    )
                    .map_err(map_database_error)?;
                available -= granted;
            }
        }
        Ok(())
    })();
    match result {
        Ok(()) => connection
            .execute_batch("COMMIT")
            .map_err(map_database_error),
        Err(error) => {
            let _ = connection.execute_batch("ROLLBACK");
            Err(error)
        }
    }
}

pub(crate) fn commit_sha_valid(value: &str) -> bool {
    value.len() == 40
        && value
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
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

/// Whether a row may ever be placed in an overflow pool: the submitter allowed it, it is
/// not a retained merge.  Enforcing its claims becomes the pool executor's duty.
pub(crate) fn overflow_candidate(kind: &str, environment: &BTreeMap<String, String>) -> bool {
    crate::overflow::requested(environment) && kind != "merge"
}

pub(crate) fn validate_submit(request: &SubmitRequest, owner: &OwnerInfo) -> Result<()> {
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
    let publication_valid = match request.kind.as_str() {
        "merge" | "land" => {
            request.publication_adapter.as_deref() == Some("github")
                && request
                    .publication_request
                    .as_ref()
                    .and_then(Value::as_u64)
                    .is_some_and(|number| number > 0 && number <= i64::MAX as u64)
        }
        _ => request.publication_adapter.is_none() && request.publication_request.is_none(),
    };
    if !publication_valid {
        return Err(AppError::new(
            "broker-submission-invalid",
            "publication metadata does not match the run kind",
        ));
    }
    if request.kind != "merge" && request.gate_run_id.is_some() {
        return Err(AppError::new(
            "broker-submission-invalid",
            "only a merge submission may name a full-gate receipt",
        ));
    }
    if request.kind == "land" {
        let worker = request.environment.get("_AGCOORD_LAND_PYTHON");
        if worker.is_none_or(|path| !Path::new(path).is_absolute() || !Path::new(path).is_file()) {
            return Err(AppError::new(
                "broker-submission-invalid",
                "land submission requires an absolute existing Python worker",
            ));
        }
        if request
            .environment
            .get("_AGCOORD_LAND_TARGET_SYNC")
            .is_some_and(|value| !matches!(value.as_str(), "0" | "1"))
        {
            return Err(AppError::new(
                "broker-submission-invalid",
                "land target-sync setting must be 0 or 1",
            ));
        }
        if request
            .environment
            .get("_AGCOORD_LAND_REUSE_FULL")
            .is_some_and(|value| !matches!(value.as_str(), "0" | "1"))
        {
            return Err(AppError::new(
                "broker-submission-invalid",
                "land gate-reuse setting must be 0 or 1",
            ));
        }
        if request
            .environment
            .get("_AGCOORD_LAND_AVOID")
            .is_some_and(|value| !value.split(',').all(commit_sha_valid))
        {
            return Err(AppError::new(
                "broker-submission-invalid",
                "land avoided commits must be full lowercase hexadecimal SHAs",
            ));
        }
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
            "full, merge, and land submissions require a lowercase 40-character head",
        ));
    }
    if request
        .environment
        .get(crate::overflow::ELIGIBLE)
        .is_some_and(|value| value != "1")
    {
        return Err(AppError::new(
            "broker-submission-invalid",
            "overflow eligibility must be 1 when present",
        ));
    }
    let (allowed_runtimes, required_capabilities) =
        crate::overflow::parse_placement_constraints(&request.environment)?;
    if let Some(allowed) = &allowed_runtimes {
        for runtime_name in allowed {
            let known = runtime_name == crate::overflow::LOCAL_RUNTIME
                || owner
                    .overflow_pools
                    .iter()
                    .any(|pool| &pool.name == runtime_name);
            if !known {
                return Err(AppError::new(
                    "broker-submission-invalid",
                    format!("unknown runtime {runtime_name}"),
                ));
            }
        }
        if !allowed.contains(crate::overflow::LOCAL_RUNTIME)
            && !overflow_candidate(&request.kind, &request.environment)
        {
            return Err(AppError::new(
                "broker-submission-invalid",
                "selecting only overflow runtimes requires overflow eligibility",
            ));
        }
    }
    for (name, units) in &request.resources {
        validate_identifier(name, "resource name")?;
        if *units == 0 || *units > i64::MAX as u64 {
            return Err(AppError::new(
                "broker-resource-unavailable",
                format!("resource {name} exceeds the live broker capacity"),
            ));
        }
    }
    let mut permitted_runtimes = Vec::new();
    if allowed_runtimes
        .as_ref()
        .is_none_or(|allowed| allowed.contains(crate::overflow::LOCAL_RUNTIME))
    {
        permitted_runtimes.push(owner.local_runtime.clone());
    }
    if overflow_candidate(&request.kind, &request.environment) {
        for pool in &owner.overflow_pools {
            if allowed_runtimes
                .as_ref()
                .is_none_or(|allowed| allowed.contains(&pool.name))
            {
                permitted_runtimes.push(pool.runtime());
            }
        }
    }
    if !permitted_runtimes
        .iter()
        .any(|runtime| runtime.satisfies(&request.resources, &required_capabilities))
    {
        for (name, units) in &request.resources {
            let resource_fits_any = permitted_runtimes.iter().any(|runtime| {
                runtime.capacities.get(name).is_some_and(|cap| units <= cap)
                    && runtime
                        .max_claim
                        .get(name)
                        .is_none_or(|limit| units <= limit)
            });
            if !resource_fits_any {
                return Err(AppError::new(
                    "broker-resource-unavailable",
                    format!("resource {name} exceeds the live broker capacity"),
                ));
            }
        }
        for capability in &required_capabilities {
            let cap_fits_any = permitted_runtimes
                .iter()
                .any(|runtime| runtime.capabilities.contains(capability));
            if !cap_fits_any {
                return Err(AppError::new(
                    "broker-resource-unavailable",
                    format!("capability {capability} is unavailable on every allowed runtime"),
                ));
            }
        }
        return Err(AppError::new(
            "broker-resource-unavailable",
            "no single allowed runtime satisfies the requested resources and capabilities together",
        ));
    }
    Ok(())
}

fn insert_request(
    connection: &Connection,
    request: &SubmitRequest,
    owner: &OwnerInfo,
    gate_run_id: Option<String>,
) -> Result<()> {
    let timestamp = now(connection)?;
    let contract = resource_contract(&request.resources, &owner.resource_bindings)?;
    let receipt = initial_receipt(&request.resources);
    connection
        .execute(
            "INSERT INTO runs (
                run_id, status, kind, phase, label, agent, repository_id, repository,
                worktree_id, checkout, branch, head_sha, barrier, resources_json,
                resource_contract_json, resource_receipt_json, resource_state_json,
                gate_run_id, publication_adapter, publication_request, caller_pid,
                command_json, environment_json, created_at
             ) VALUES (
                ?1, 'queued', ?2, 'queued', ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10,
                ?11, ?12, ?13, ?14, '{}', ?15, ?16, ?17, ?18, ?19, ?20, ?21
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
                i64::from(matches!(request.kind.as_str(), "merge" | "land")),
                serde_json::to_string(&request.resources).unwrap(),
                serde_json::to_string(&contract).unwrap(),
                serde_json::to_string(&receipt).unwrap(),
                gate_run_id,
                request.publication_adapter,
                request
                    .publication_request
                    .as_ref()
                    .map(|value| serde_json::to_string(value).unwrap()),
                i64::from(request.caller_pid),
                serde_json::to_string(&request.command).unwrap(),
                serde_json::to_string(&request.environment).unwrap(),
                timestamp,
            ],
        )
        .map_err(map_database_error)?;
    Ok(())
}

pub(crate) fn insert_check(
    connection: &Connection,
    request: &SubmitRequest,
    owner: &OwnerInfo,
) -> Result<()> {
    if request.kind != "check" {
        return Err(AppError::new(
            "broker-submission-invalid",
            "recovery proof must be a check",
        ));
    }
    insert_request(connection, request, owner, None)
}

pub fn submit(paths: &Paths, request: &SubmitRequest) -> Result<Value> {
    let owner = owner_info(paths)?;
    validate_submit(request, &owner)?;
    let connection = open_protocol5(paths)?;
    connection
        .execute_batch("BEGIN IMMEDIATE")
        .map_err(map_database_error)?;
    if maintenance_record(&connection)?.is_some() {
        return Err(AppError::new(
            "broker-draining",
            "coordinator is draining; new submissions are refused",
        ));
    }
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
    let mut selected_gate_run_id = request.gate_run_id.clone();
    if request.kind == "merge" {
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
        if selected_gate_run_id.is_none() {
            selected_gate_run_id = connection
                .query_row(
                    "SELECT run_id FROM runs
                     WHERE kind = 'full' AND status = 'passed' AND exit_status = 0
                       AND repository_id = ?1 AND branch = ?2 AND head_sha = ?3
                       AND sequence >= ?4
                     ORDER BY sequence DESC LIMIT 1",
                    params![
                        request.repository_id,
                        request.branch,
                        request.head_sha,
                        floor,
                    ],
                    |row| row.get(0),
                )
                .optional()
                .map_err(map_database_error)?;
        }
        let gate_run_id = selected_gate_run_id.as_deref().ok_or_else(|| {
            AppError::new(
                "broker-gate-required",
                "no passed full-gate receipt matches this repository, branch, and head",
            )
        })?;
        let gate = load_run(&connection, gate_run_id)?;
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
    insert_request(&connection, request, &owner, selected_gate_run_id)?;
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

pub fn clear(paths: &Paths) -> Result<Value> {
    let _owner = owner_info(paths)?;
    let connection = open_protocol5(paths)?;
    connection
        .execute_batch("BEGIN IMMEDIATE")
        .map_err(map_database_error)?;
    if maintenance_record(&connection)?.is_some() {
        return Err(AppError::new(
            "broker-draining",
            "cannot clear history while the coordinator is draining",
        ));
    }
    let live: Vec<String> = {
        let mut statement = connection
            .prepare(
                "SELECT run_id FROM runs WHERE status IN ('queued', 'running') ORDER BY sequence",
            )
            .map_err(map_database_error)?;
        statement
            .query_map([], |row| row.get(0))
            .map_err(map_database_error)?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(map_database_error)?
    };
    if !live.is_empty() {
        return Err(AppError::new(
            "broker-clear-live-work",
            format!(
                "cannot clear history while work is queued or running: {}",
                live.join(", ")
            ),
        ));
    }
    let terminal: Vec<String> = {
        let mut statement = connection
            .prepare(
                "SELECT run_id FROM runs WHERE status IN
                 ('passed', 'failed', 'cancelled', 'interrupted') ORDER BY sequence",
            )
            .map_err(map_database_error)?;
        statement
            .query_map([], |row| row.get(0))
            .map_err(map_database_error)?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(map_database_error)?
    };
    for run_id in &terminal {
        let path = paths.logs.join(format!("{run_id}.log"));
        if let Err(error) = fs::remove_file(&path)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            return Err(AppError::new(
                "broker-clear-failed",
                format!("cannot remove gate log for {run_id}: {error}"),
            ));
        }
    }
    connection
        .execute(
            "DELETE FROM runs WHERE status IN
             ('passed', 'failed', 'cancelled', 'interrupted')",
            [],
        )
        .map_err(map_database_error)?;
    set_metadata(&connection, "last_activity", &now(&connection)?)?;
    connection
        .execute_batch("COMMIT")
        .map_err(map_database_error)?;
    Ok(json!({"cleared": terminal.len()}))
}

/// Whether the run declared a tmpfs or project-quota scratch provider, in which case the
/// recorded worker is the coordinator's launcher and the command is its direct child.
fn scratch_policy_declared(run: &RunRecord) -> bool {
    run.resource_contract.as_object().is_some_and(|entries| {
        entries.values().any(|entry| {
            entry.get("kind").and_then(Value::as_str) == Some("tmpfs")
                || entry.get("backend").and_then(Value::as_str)
                    == Some(crate::project_quota::PROJECT_QUOTA_BACKEND)
        })
    })
}

/// Whether `pid`, proven by `token`, is this run's admitted worker as the command sees it:
/// the recorded worker itself, or — under a scratch policy — the live direct child of the
/// live recorded launcher.
fn admitted_worker_presents(run: &RunRecord, pid: u32, token: &str) -> bool {
    if run.worker_pid == Some(pid) {
        return run.worker_start_token.as_deref() == Some(token)
            && same_worker_process(Some(pid), Some(token));
    }
    let (Some(recorded), Some(recorded_token)) =
        (run.worker_pid, run.worker_start_token.as_deref())
    else {
        return false;
    };
    scratch_policy_declared(run) && direct_child_of(pid, token, recorded, recorded_token)
}

pub fn verify_admission(paths: &Paths, request: &AdmissionRequest) -> Result<Value> {
    let _owner = owner_info(paths)?;
    if !matches!(request.kind.as_str(), "full" | "merge" | "land") {
        return Err(AppError::new(
            "broker-admission-invalid",
            "broker admission kind must be full, merge, or land",
        ));
    }
    let connection = open_protocol5(paths)?;
    let run = load_run(&connection, &request.run_id)?;
    let slice_head: Option<String> = if run.kind == "land" {
        connection
            .query_row(
                "SELECT gating_head_sha FROM land_batch_slices WHERE leader_run_id = ?1",
                params![run.run_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(map_database_error)?
    } else {
        None
    };
    let matches = run.status == "running"
        && run.kind == request.kind
        && run.checkout == request.checkout
        && slice_head.as_deref().or(run.head_sha.as_deref()) == Some(&request.head_sha)
        && admitted_worker_presents(&run, request.worker_pid, &request.worker_start_token);
    if !matches {
        return Err(AppError::new(
            "broker-admission-mismatch",
            format!("run {:?} has no exact broker admission", request.run_id),
        ));
    }
    Ok(json!({"verified": true}))
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
    if !admitted_worker_presents(&run, request.worker_pid, &request.worker_start_token)
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
    apply_land_phase_update(
        &connection,
        &run,
        &request.phase,
        &request.head_sha,
        request.new_head_sha.as_ref(),
        request.gate_exit_status,
    )?;
    connection
        .execute_batch("COMMIT")
        .map_err(map_database_error)?;
    status(paths, &request.run_id)
}

/// Authorize one land gate from a passed `full` receipt for the very same exact head.
///
/// The broker, not the worker, decides whether a receipt exists and records it, so a gate can
/// never be skipped on a claim the durable record does not support.  A candidate must be a
/// passed `full` for the same repository, the same exact head the land reached after preflight,
/// and the same gate command; `check` rows are never eligible because they assert no clean head.
/// On a hit the row moves to `gating` with a zero gate status and names its source in
/// `gate_run_id`; on a miss nothing changes and the worker runs the gate.
pub fn reuse_gate(paths: &Paths, request: &ReuseRequest) -> Result<Value> {
    let _owner = owner_info(paths)?;
    let connection = open_protocol5(paths)?;
    connection
        .execute_batch("BEGIN IMMEDIATE")
        .map_err(map_database_error)?;
    let run = load_run(&connection, &request.run_id)?;
    if !admitted_worker_presents(&run, request.worker_pid, &request.worker_start_token)
        || run.checkout != request.checkout
        || run.head_sha.as_deref() != Some(&request.head_sha)
    {
        return Err(AppError::new(
            "broker-land-identity-mismatch",
            "gate reuse requester does not match the admitted worker, checkout, and head",
        ));
    }
    let in_active_batch = land_batch_schema_present(&connection)?
        && connection
            .query_row(
                "SELECT 1 FROM land_batch_members WHERE member_run_id = ?1 AND active = 1",
                params![run.run_id],
                |_| Ok(()),
            )
            .optional()
            .map_err(map_database_error)?
            .is_some();
    let outcome = try_reuse_gate_for_run(paths, &connection, &run, in_active_batch)?;
    if outcome["reused"] == true {
        connection
            .execute_batch("COMMIT")
            .map_err(map_database_error)?;
    }
    Ok(outcome)
}

pub fn report_land_result(paths: &Paths, request: &LandResultRequest) -> Result<Value> {
    if !(0..=255).contains(&request.exit_status) {
        return Err(AppError::new(
            "broker-land-result-invalid",
            "land result must be an exit status from 0 to 255",
        ));
    }
    let _owner = owner_info(paths)?;
    let connection = open_protocol5(paths)?;
    connection
        .execute_batch("BEGIN IMMEDIATE")
        .map_err(map_database_error)?;
    let run = load_run(&connection, &request.run_id)?;
    let identity_matches =
        admitted_worker_presents(&run, request.worker_pid, &request.worker_start_token);
    let phase_valid = (run.phase == "gating" && run.gate_exit_status.is_some())
        || (run.phase == "publishing" && run.gate_exit_status == Some(0))
        || run.phase == "preflight";
    if run.kind != "land"
        || run.status != "running"
        || !identity_matches
        || run.reported_exit_status.is_some()
        || !phase_valid
    {
        return Err(AppError::new(
            "broker-land-result-invalid",
            "land result does not match one unreported admitted worker",
        ));
    }
    connection
        .execute(
            "UPDATE runs SET reported_exit_status = ?1 WHERE run_id = ?2",
            params![request.exit_status, request.run_id],
        )
        .map_err(map_database_error)?;
    connection
        .execute_batch("COMMIT")
        .map_err(map_database_error)?;
    Ok(json!({"reported": true}))
}

fn land_batch_schema_present(connection: &Connection) -> Result<bool> {
    let count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master
             WHERE type = 'table'
               AND name IN (
                   'land_batch_members',
                   'land_batch_slices',
                   'land_batches',
                   'land_batch_entries',
                   'land_batch_steps'
               )",
            [],
            |row| row.get(0),
        )
        .map_err(map_database_error)?;
    Ok(count == 5)
}

/// Ensure the coordinator-private tables tracking active and historical land batches exist.
pub fn ensure_land_batch_schema(connection: &Connection) -> Result<()> {
    if land_batch_schema_present(connection)? {
        return Ok(());
    }
    connection
        .execute_batch(
            "CREATE TABLE IF NOT EXISTS land_batch_members (
                leader_run_id TEXT NOT NULL,
                member_run_id TEXT NOT NULL PRIMARY KEY,
                position INTEGER NOT NULL,
                active INTEGER NOT NULL DEFAULT 1,
                original_checkout TEXT NOT NULL,
                original_environment_json TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS land_batch_slices (
                leader_run_id TEXT PRIMARY KEY,
                gating_head_sha TEXT NOT NULL,
                slice_member_ids_json TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS land_batches (
                batch_id INTEGER PRIMARY KEY AUTOINCREMENT,
                leader_run_id TEXT NOT NULL,
                target_branch TEXT,
                created_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS land_batches_leader
                ON land_batches(leader_run_id, batch_id);
            CREATE TABLE IF NOT EXISTS land_batch_entries (
                batch_id INTEGER NOT NULL REFERENCES land_batches(batch_id) ON DELETE CASCADE,
                position INTEGER NOT NULL,
                member_run_id TEXT NOT NULL,
                branch TEXT NOT NULL,
                head_sha TEXT NOT NULL,
                publication_adapter TEXT,
                publication_request TEXT,
                requeued INTEGER NOT NULL DEFAULT 0,
                final_status TEXT,
                final_failure_reason TEXT,
                PRIMARY KEY (batch_id, position),
                UNIQUE (batch_id, member_run_id)
            );
            CREATE INDEX IF NOT EXISTS land_batch_entries_member
                ON land_batch_entries(member_run_id, batch_id);
            CREATE TABLE IF NOT EXISTS land_batch_steps (
                batch_id INTEGER NOT NULL REFERENCES land_batches(batch_id) ON DELETE CASCADE,
                step INTEGER NOT NULL,
                member_run_ids_json TEXT NOT NULL,
                gated_member_ids_json TEXT NOT NULL,
                dropped_json TEXT NOT NULL DEFAULT '[]',
                base_sha TEXT,
                gating_head_sha TEXT,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                gate_reused_from TEXT,
                gate_exit_status INTEGER,
                outcome TEXT NOT NULL,
                split_left_count INTEGER,
                split_right_count INTEGER,
                published_sha TEXT,
                PRIMARY KEY (batch_id, step)
            );",
        )
        .map_err(map_database_error)
}

fn load_batch_view(connection: &Connection, batch_id: i64) -> Result<(String, Value)> {
    let (leader_run_id, target_branch): (String, Option<String>) = connection
        .query_row(
            "SELECT leader_run_id, target_branch FROM land_batches WHERE batch_id = ?1",
            params![batch_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(map_database_error)?;
    let mut member_stmt = connection
        .prepare(
            "SELECT e.position, e.member_run_id, e.branch, e.head_sha,
                    e.publication_adapter, e.publication_request,
                    e.requeued, e.final_status, e.final_failure_reason,
                    r.status, r.failure_reason
             FROM land_batch_entries e
             LEFT JOIN runs r ON r.run_id = e.member_run_id
             WHERE e.batch_id = ?1
             ORDER BY e.position",
        )
        .map_err(map_database_error)?;
    let mut member_rows = member_stmt.query(params![batch_id]).map_err(map_database_error)?;
    let mut members = Vec::new();
    while let Some(row) = member_rows.next().map_err(map_database_error)? {
        let position: i64 = row.get(0).map_err(map_database_error)?;
        let member_run_id: String = row.get(1).map_err(map_database_error)?;
        let branch: String = row.get(2).map_err(map_database_error)?;
        let head_sha: String = row.get(3).map_err(map_database_error)?;
        let pub_adapter: Option<String> = row.get(4).map_err(map_database_error)?;
        let pub_request_raw: Option<String> = row.get(5).map_err(map_database_error)?;
        let requeued: i64 = row.get(6).map_err(map_database_error)?;
        let final_status: Option<String> = row.get(7).map_err(map_database_error)?;
        let final_failure: Option<String> = row.get(8).map_err(map_database_error)?;
        let live_status: Option<String> = row.get(9).map_err(map_database_error)?;
        let live_failure: Option<String> = row.get(10).map_err(map_database_error)?;
        let pub_request: Value = match pub_request_raw {
            Some(raw) => parse_json(raw, &member_run_id, "batch publication request")?,
            None => Value::Null,
        };
        let publication = pub_adapter.map(|adapter| {
            json!({
                "adapter": adapter,
                "request": pub_request,
            })
        });
        let (status, failure_reason) = if requeued != 0 {
            ("requeued".to_owned(), None)
        } else if let Some(status) = final_status {
            (status, final_failure)
        } else {
            (
                live_status.unwrap_or_else(|| "unknown".to_owned()),
                live_failure,
            )
        };
        members.push(json!({
            "position": position,
            "run_id": member_run_id,
            "branch": branch,
            "head_sha": head_sha,
            "publication": publication,
            "status": status,
            "failure_reason": failure_reason,
        }));
    }

    let mut step_stmt = connection
        .prepare(
            "SELECT step, member_run_ids_json, gated_member_ids_json, dropped_json,
                    base_sha, gating_head_sha, started_at, finished_at,
                    gate_reused_from, gate_exit_status, outcome,
                    split_left_count, split_right_count, published_sha
             FROM land_batch_steps
             WHERE batch_id = ?1
             ORDER BY step",
        )
        .map_err(map_database_error)?;
    let mut step_rows = step_stmt.query(params![batch_id]).map_err(map_database_error)?;
    let mut steps = Vec::new();
    while let Some(row) = step_rows.next().map_err(map_database_error)? {
        let step: i64 = row.get(0).map_err(map_database_error)?;
        let member_ids_raw: String = row.get(1).map_err(map_database_error)?;
        let gated_ids_raw: String = row.get(2).map_err(map_database_error)?;
        let dropped_raw: String = row.get(3).map_err(map_database_error)?;
        let base_sha: Option<String> = row.get(4).map_err(map_database_error)?;
        let gating_head_sha: Option<String> = row.get(5).map_err(map_database_error)?;
        let started_at: String = row.get(6).map_err(map_database_error)?;
        let finished_at: Option<String> = row.get(7).map_err(map_database_error)?;
        let gate_reused_from: Option<String> = row.get(8).map_err(map_database_error)?;
        let gate_exit_status: Option<i64> = row.get(9).map_err(map_database_error)?;
        let outcome: String = row.get(10).map_err(map_database_error)?;
        let split_left: Option<i64> = row.get(11).map_err(map_database_error)?;
        let split_right: Option<i64> = row.get(12).map_err(map_database_error)?;
        let published_sha: Option<String> = row.get(13).map_err(map_database_error)?;
        let member_run_ids: Vec<String> =
            parse_json(member_ids_raw, &leader_run_id, "batch step members")?;
        let gated_member_run_ids: Vec<String> =
            parse_json(gated_ids_raw, &leader_run_id, "batch step gated members")?;
        let dropped: Vec<Value> =
            parse_json(dropped_raw, &leader_run_id, "batch step dropped members")?;
        let split = match (split_left, split_right) {
            (Some(l), Some(r)) => json!([l, r]),
            _ => Value::Null,
        };
        steps.push(json!({
            "step": step,
            "member_run_ids": member_run_ids,
            "gated_member_run_ids": gated_member_run_ids,
            "dropped": dropped,
            "base_sha": base_sha,
            "gating_head_sha": gating_head_sha,
            "started_at": started_at,
            "finished_at": finished_at,
            "gate_reused_from": gate_reused_from,
            "gate_exit_status": gate_exit_status,
            "outcome": outcome,
            "split": split,
            "published_sha": published_sha,
        }));
    }

    let base = json!({
        "leader_run_id": leader_run_id,
        "target_branch": target_branch,
        "members": members,
        "steps": steps,
    });
    Ok((leader_run_id, base))
}

fn batch_for_run_cached(
    connection: &Connection,
    run: &RunRecord,
    cache: &mut BTreeMap<i64, (String, Value)>,
) -> Result<Value> {
    if run.kind != "land" {
        return Ok(Value::Null);
    }
    let batch_id: Option<i64> = connection
        .query_row(
            "SELECT batch_id FROM land_batch_entries
             WHERE member_run_id = ?1 AND requeued = 0
             ORDER BY batch_id DESC LIMIT 1",
            params![run.run_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(map_database_error)?;
    let Some(batch_id) = batch_id else {
        return Ok(Value::Null);
    };
    if !cache.contains_key(&batch_id) {
        let loaded = load_batch_view(connection, batch_id)?;
        cache.insert(batch_id, loaded);
    }
    let (leader_run_id, base) = cache.get(&batch_id).unwrap();
    let mut payload = base.clone();
    payload["role"] = json!(if run.run_id == *leader_run_id {
        "leader"
    } else {
        "follower"
    });
    Ok(payload)
}

fn batch_slice(connection: &Connection, run_id: &str) -> Result<Value> {
    let slice: Option<(String, String)> = connection.query_row(
        "SELECT gating_head_sha, slice_member_ids_json FROM land_batch_slices WHERE leader_run_id = ?1",
        params![run_id], |row| Ok((row.get(0)?, row.get(1)?)),
    ).optional().map_err(map_database_error)?;
    let Some((head, raw)) = slice else {
        return Ok(Value::Null);
    };
    let ids: Vec<String> = serde_json::from_str(&raw)
        .map_err(|_| AppError::new("broker-row-invalid", "invalid batch slice members"))?;
    let mut members = Vec::new();
    for id in ids {
        let run = load_run(connection, &id)?;
        let original_checkout: String = connection.query_row(
            "SELECT original_checkout FROM land_batch_members WHERE leader_run_id = ?1 AND member_run_id = ?2",
            params![run_id, id], |row| row.get(0),
        ).map_err(map_database_error)?;
        let publication = run
            .publication_adapter
            .as_ref()
            .map(|adapter| json!({"adapter": adapter, "request": run.publication_request}));
        members.push(
            json!({"run_id": run.run_id, "branch": run.branch, "head_sha": run.head_sha,
            "original_checkout": original_checkout, "publication": publication}),
        );
    }
    Ok(json!({"gating_head_sha": head, "members": members}))
}

/// Active follower -> leader mapping for claimed land-batch members (excludes the leader).
pub fn active_batch_followers(connection: &Connection) -> Result<BTreeMap<String, String>> {
    ensure_land_batch_schema(connection)?;
    let mut statement = connection
        .prepare(
            "SELECT member_run_id, leader_run_id
             FROM land_batch_members
             WHERE active = 1 AND member_run_id != leader_run_id",
        )
        .map_err(map_database_error)?;
    let mut rows = statement.query([]).map_err(map_database_error)?;
    let mut selected = BTreeMap::new();
    while let Some(row) = rows.next().map_err(map_database_error)? {
        selected.insert(
            row.get::<_, String>(0).map_err(map_database_error)?,
            row.get::<_, String>(1).map_err(map_database_error)?,
        );
    }
    Ok(selected)
}

/// Active member -> leader mapping for all active members of any running land batch.
pub fn active_batch_owners(connection: &Connection) -> Result<BTreeMap<String, String>> {
    ensure_land_batch_schema(connection)?;
    let mut statement = connection
        .prepare(
            "SELECT member_run_id, leader_run_id
             FROM land_batch_members
             WHERE active = 1",
        )
        .map_err(map_database_error)?;
    let mut rows = statement.query([]).map_err(map_database_error)?;
    let mut selected = BTreeMap::new();
    while let Some(row) = rows.next().map_err(map_database_error)? {
        selected.insert(
            row.get::<_, String>(0).map_err(map_database_error)?,
            row.get::<_, String>(1).map_err(map_database_error)?,
        );
    }
    Ok(selected)
}

pub fn publication_failure_reason(run: &RunRecord, exit_status: i64) -> Option<&'static str> {
    if !matches!(run.kind.as_str(), "merge" | "land") {
        return None;
    }
    if run.kind == "land"
        && run.phase == "gating"
        && run.gate_exit_status.is_some_and(|status| status != 0)
    {
        return Some("gate-failed");
    }
    Some(publication_exit_reason(exit_status))
}

fn publication_exit_reason(exit_status: i64) -> &'static str {
    match exit_status {
        75 => "stale-main",
        76 => "head-changed",
        77 => "pr-not-ready",
        78 => "publish-failed",
        80 => "avoided-commit",
        _ => "merge-error",
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParsedLandOptions {
    pub target_sync: bool,
    pub reuse_full: bool,
    pub avoid: Vec<String>,
}

pub fn parse_land_options(environment: &BTreeMap<String, String>) -> Result<ParsedLandOptions> {
    let target_sync = match environment
        .get("_AGCOORD_LAND_TARGET_SYNC")
        .map(String::as_str)
    {
        None | Some("1") => true,
        Some("0") => false,
        Some(_) => {
            return Err(AppError::new(
                "broker-row-invalid",
                "land run has an invalid target-sync setting",
            ));
        }
    };
    let reuse_full = match environment
        .get("_AGCOORD_LAND_REUSE_FULL")
        .map(String::as_str)
    {
        None | Some("0") => false,
        Some("1") => true,
        Some(_) => {
            return Err(AppError::new(
                "broker-row-invalid",
                "land run has an invalid gate-reuse setting",
            ));
        }
    };
    let mut avoid = Vec::new();
    if let Some(avoided) = environment.get("_AGCOORD_LAND_AVOID") {
        for sha in avoided.split(',') {
            if !commit_sha_valid(sha) {
                return Err(AppError::new(
                    "broker-row-invalid",
                    "land run names an invalid avoided commit",
                ));
            }
            avoid.push(sha.to_owned());
        }
    }
    Ok(ParsedLandOptions {
        target_sync,
        reuse_full,
        avoid,
    })
}

pub fn normalize_gate_command(command: &[String]) -> Vec<String> {
    command
        .iter()
        .map(|arg| {
            let mut value = arg.as_str();
            while let Some(rest) = value.strip_prefix("./") {
                if rest.is_empty() {
                    break;
                }
                value = rest;
            }
            value.to_owned()
        })
        .collect()
}

fn resource_envelope_compatible(candidate: &RunRecord, leader: &RunRecord) -> bool {
    if !candidate
        .resources
        .iter()
        .all(|(name, units)| leader.resources.get(name).is_some_and(|cap| units <= cap))
    {
        return false;
    }
    let Some(candidate_contract) = candidate.resource_contract.as_object() else {
        return false;
    };
    let Some(leader_contract) = leader.resource_contract.as_object() else {
        return false;
    };
    candidate_contract
        .iter()
        .all(|(name, value)| leader_contract.get(name) == Some(value))
}

struct BatchSelectionContext<'a> {
    leader: &'a RunRecord,
    leader_command: Vec<String>,
    same_active: Vec<&'a RunRecord>,
    repo_queued: Vec<&'a RunRecord>,
    target_runtime: &'a crate::overflow::Runtime,
}

impl<'a> BatchSelectionContext<'a> {
    fn from_live(
        leader: &'a RunRecord,
        live: &'a [RunRecord],
        target_runtime: &'a crate::overflow::Runtime,
    ) -> Self {
        Self {
            leader,
            leader_command: normalize_gate_command(&leader.command),
            same_active: live
                .iter()
                .filter(|run| run.status == "running" && run.repository_id == leader.repository_id)
                .collect(),
            repo_queued: live
                .iter()
                .filter(|run| run.status == "queued" && run.repository_id == leader.repository_id)
                .collect(),
            target_runtime,
        }
    }

    fn candidate_eligible(
        &self,
        candidate: &RunRecord,
        seen_branches: &BTreeSet<String>,
        seen_requests: &[Option<Value>],
    ) -> bool {
        let on_overflow_pool = self.target_runtime.name != crate::overflow::LOCAL_RUNTIME;
        let Ok((allowed_runtimes, required_capabilities)) =
            crate::overflow::parse_placement_constraints(&candidate.environment)
        else {
            return false;
        };
        if candidate.run_id == self.leader.run_id
            || candidate.kind != "land"
            || candidate.status != "queued"
            || candidate.cancel_requested
            || candidate.repository_id != self.leader.repository_id
            || candidate.publication_adapter != self.leader.publication_adapter
            || seen_branches.contains(&candidate.branch)
            || seen_requests.contains(&candidate.publication_request)
            || normalize_gate_command(&candidate.command) != self.leader_command
            || !resource_envelope_compatible(candidate, self.leader)
            || (on_overflow_pool && !overflow_candidate(&candidate.kind, &candidate.environment))
            || allowed_runtimes
                .as_ref()
                .is_some_and(|allowed| !allowed.contains(&self.target_runtime.name))
            || !self
                .target_runtime
                .satisfies(&candidate.resources, &required_capabilities)
        {
            return false;
        }
        let active_conflict = self.same_active.iter().any(|run| {
            run.run_id != self.leader.run_id
                && (run.barrier
                    || run.worktree_id == candidate.worktree_id
                    || (matches!(run.kind.as_str(), "full" | "land")
                        && run.branch == candidate.branch))
        });
        if active_conflict {
            return false;
        }
        let earlier_blocker = self.repo_queued.iter().any(|run| {
            run.sequence < candidate.sequence
                && run.run_id != self.leader.run_id
                && !seen_branches.contains(&run.branch)
                && (run.worktree_id == candidate.worktree_id
                    || (matches!(run.kind.as_str(), "full" | "land")
                        && run.branch == candidate.branch))
        });
        !earlier_blocker
    }
}

fn member_descriptor(run: &RunRecord, is_leader: bool) -> Result<Value> {
    let options = parse_land_options(&run.environment)?;
    Ok(json!({
        "run_id": run.run_id,
        "sequence": run.sequence,
        "branch": run.branch,
        "head_sha": run.head_sha,
        "worktree_id": run.worktree_id,
        "publication_adapter": run.publication_adapter,
        "publication_request": run.publication_request,
        "gate_run_id": run.gate_run_id,
        "is_leader": is_leader,
        "environment_options": {
            "target_sync": options.target_sync,
            "reuse_full": options.reuse_full,
            "avoid": options.avoid,
        },
    }))
}

pub fn select_land_batch_candidates(
    connection: &Connection,
    leader: &RunRecord,
    target_runtime: &crate::overflow::Runtime,
) -> Result<Vec<Value>> {
    if leader.kind != "land" {
        return Ok(Vec::new());
    }
    let live = load_live_runs(connection)?;
    let context = BatchSelectionContext::from_live(leader, &live, target_runtime);
    let mut seen_branches = BTreeSet::new();
    seen_branches.insert(leader.branch.clone());
    let mut seen_requests = vec![leader.publication_request.clone()];
    let mut selected = Vec::new();
    for candidate in &context.repo_queued {
        if context.candidate_eligible(candidate, &seen_branches, &seen_requests) {
            seen_branches.insert(candidate.branch.clone());
            seen_requests.push(candidate.publication_request.clone());
            selected.push(member_descriptor(candidate, false)?);
        }
    }
    Ok(selected)
}

fn load_verified_batch_leader(
    connection: &Connection,
    run_id: &str,
    worker_pid: u32,
    worker_start_token: &str,
    checkout: &Path,
    head_sha: &str,
    require_not_cancelled: bool,
) -> Result<RunRecord> {
    let leader = load_run(connection, run_id)?;
    if leader.kind != "land" || leader.status != "running" {
        return Err(AppError::new(
            "broker-land-phase-invalid",
            "only a running land worker may coordinate a land batch",
        ));
    }
    if !admitted_worker_presents(&leader, worker_pid, worker_start_token)
        || leader.checkout != checkout
        || leader.head_sha.as_deref() != Some(head_sha)
    {
        return Err(AppError::new(
            "broker-land-identity-mismatch",
            "batch caller does not match the admitted land worker, checkout, and head",
        ));
    }
    if require_not_cancelled && leader.cancel_requested {
        return Err(AppError::new(
            "broker-land-cancelled",
            "land cancellation committed",
        ));
    }
    Ok(leader)
}

pub fn set_batch_slice(paths: &Paths, request: &BatchSliceRequest) -> Result<Value> {
    let _owner = owner_info(paths)?;
    let connection = open_protocol5(paths)?;
    connection
        .execute_batch("BEGIN IMMEDIATE")
        .map_err(map_database_error)?;
    let identity = &request.identity;
    load_verified_batch_leader(
        &connection,
        &identity.run_id,
        identity.worker_pid,
        &identity.worker_start_token,
        &identity.checkout,
        &identity.head_sha,
        true,
    )?;
    let sha = &request.gating_head_sha;
    if sha.len() != 40
        || !sha
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(AppError::new(
            "broker-batch-slice-invalid",
            "batch slice head must be 40 lowercase hexadecimal characters",
        ));
    }
    let mut seen = BTreeSet::new();
    if request.member_run_ids.is_empty() {
        return Err(AppError::new(
            "broker-batch-slice-invalid",
            "batch slice must contain members",
        ));
    }
    for id in &request.member_run_ids {
        let active: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM land_batch_members JOIN runs ON runs.run_id = land_batch_members.member_run_id WHERE leader_run_id = ?1 AND member_run_id = ?2 AND active = 1 AND runs.status = 'running')",
            params![identity.run_id, id], |row| row.get(0),
        ).map_err(map_database_error)?;
        if !active || !seen.insert(id) {
            return Err(AppError::new(
                "broker-batch-slice-invalid",
                "batch slice members must be distinct running batch members",
            ));
        }
    }
    let raw = serde_json::to_string(&request.member_run_ids)
        .map_err(|_| AppError::new("broker-batch-slice-invalid", "invalid members"))?;
    connection.execute("INSERT OR REPLACE INTO land_batch_slices (leader_run_id, gating_head_sha, slice_member_ids_json) VALUES (?1, ?2, ?3)", params![identity.run_id, sha, raw]).map_err(map_database_error)?;
    connection
        .execute_batch("COMMIT")
        .map_err(map_database_error)?;
    Ok(json!({"leader_run_id": identity.run_id}))
}

pub fn clear_batch_slice(paths: &Paths, request: &BatchStatusRequest) -> Result<Value> {
    let _owner = owner_info(paths)?;
    let connection = open_protocol5(paths)?;
    connection
        .execute_batch("BEGIN IMMEDIATE")
        .map_err(map_database_error)?;
    load_verified_batch_leader(
        &connection,
        &request.run_id,
        request.worker_pid,
        &request.worker_start_token,
        &request.checkout,
        &request.head_sha,
        false,
    )?;
    connection
        .execute(
            "DELETE FROM land_batch_slices WHERE leader_run_id = ?1",
            params![request.run_id],
        )
        .map_err(map_database_error)?;
    connection
        .execute_batch("COMMIT")
        .map_err(map_database_error)?;
    Ok(json!({"leader_run_id": request.run_id}))
}

fn batch_step_outcome_valid(value: &str) -> bool {
    matches!(
        value,
        "running"
            | "published"
            | "bisected"
            | "gate-failed"
            | "publish-failed"
            | "merge-conflict"
            | "merge-error"
            | "stale-main"
            | "head-changed"
            | "pr-not-ready"
            | "avoided-commit"
            | "preflight-failed"
            | "cancelled"
            | "interrupted"
    )
}

pub fn record_batch_step(paths: &Paths, request: &BatchStepRequest) -> Result<Value> {
    let _owner = owner_info(paths)?;
    let connection = open_protocol5(paths)?;
    ensure_land_batch_schema(&connection)?;
    connection
        .execute_batch("BEGIN IMMEDIATE")
        .map_err(map_database_error)?;
    let identity = &request.identity;
    let payload = &request.payload;
    load_verified_batch_leader(
        &connection,
        &identity.run_id,
        identity.worker_pid,
        &identity.worker_start_token,
        &identity.checkout,
        &identity.head_sha,
        payload.outcome == "running",
    )?;
    if !batch_step_outcome_valid(&payload.outcome) {
        return Err(AppError::new(
            "broker-batch-step-invalid",
            format!("invalid batch step outcome {:?}", payload.outcome),
        ));
    }
    for sha in [
        payload.base_sha.as_deref(),
        payload.gating_head_sha.as_deref(),
        payload.published_sha.as_deref(),
    ]
    .into_iter()
    .flatten()
    {
        if !commit_sha_valid(sha) {
            return Err(AppError::new(
                "broker-batch-step-invalid",
                "batch step commit SHA must be 40 lowercase hexadecimal characters",
            ));
        }
    }
    if let Some(reused) = &payload.gate_reused_from
        && !identifier_valid(reused)
    {
        return Err(AppError::new(
            "broker-batch-step-invalid",
            "batch step gate_reused_from has an invalid run identifier",
        ));
    }
    if payload
        .gate_exit_status
        .is_some_and(|code| !(0..=255).contains(&code))
    {
        return Err(AppError::new(
            "broker-batch-step-invalid",
            "batch step gate_exit_status must be from 0 to 255",
        ));
    }
    if let Some([left, right]) = payload.split
        && (left == 0 || right == 0 || payload.outcome != "bisected")
    {
        return Err(AppError::new(
            "broker-batch-step-invalid",
            "batch step split counts require a bisected outcome and positive halves",
        ));
    }
    if (payload.outcome == "published") != payload.published_sha.is_some() {
        return Err(AppError::new(
            "broker-batch-step-invalid",
            "published batch steps must carry published_sha and non-published steps must omit it",
        ));
    }
    let batch_id: i64 = connection
        .query_row(
            "SELECT batch_id FROM land_batches WHERE leader_run_id = ?1 ORDER BY batch_id DESC LIMIT 1",
            params![identity.run_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(map_database_error)?
        .ok_or_else(|| {
            AppError::new(
                "broker-batch-step-invalid",
                "leader has no recorded land batch",
            )
        })?;
    let batch_members: BTreeSet<String> = {
        let mut stmt = connection
            .prepare("SELECT member_run_id FROM land_batch_entries WHERE batch_id = ?1")
            .map_err(map_database_error)?;
        stmt.query_map(params![batch_id], |row| row.get::<_, String>(0))
            .map_err(map_database_error)?
            .collect::<std::result::Result<BTreeSet<_>, _>>()
            .map_err(map_database_error)?
    };
    if let Some(dropped) = &payload.dropped {
        for item in dropped {
            if !batch_members.contains(&item.run_id)
                || item.reason.trim().is_empty()
                || item.reason.contains('\0')
            {
                return Err(AppError::new(
                    "broker-batch-step-invalid",
                    "dropped batch member must belong to the batch and carry a non-empty reason",
                ));
            }
        }
    }

    let recorded_step = match payload.step {
        None => {
            let member_ids = payload.member_run_ids.as_ref().ok_or_else(|| {
                AppError::new(
                    "broker-batch-step-invalid",
                    "starting a batch step requires member_run_ids",
                )
            })?;
            if member_ids.is_empty() {
                return Err(AppError::new(
                    "broker-batch-step-invalid",
                    "batch step member_run_ids cannot be empty",
                ));
            }
            let mut seen_attempted = BTreeSet::new();
            for id in member_ids {
                if !batch_members.contains(id) || !seen_attempted.insert(id.clone()) {
                    return Err(AppError::new(
                        "broker-batch-step-invalid",
                        "batch step member_run_ids must be distinct members of the batch",
                    ));
                }
            }
            let gated_ids = payload
                .gated_member_run_ids
                .clone()
                .unwrap_or_else(|| member_ids.clone());
            let mut seen_gated = BTreeSet::new();
            for id in &gated_ids {
                if !seen_attempted.contains(id) || !seen_gated.insert(id.clone()) {
                    return Err(AppError::new(
                        "broker-batch-step-invalid",
                        "batch step gated_member_run_ids must be distinct members of member_run_ids",
                    ));
                }
            }
            let dropped_values: Vec<Value> = payload
                .dropped
                .as_deref()
                .unwrap_or(&[])
                .iter()
                .map(DroppedBatchMember::to_json_value)
                .collect();
            let next_step: i64 = connection
                .query_row(
                    "SELECT COALESCE(MAX(step), 0) + 1 FROM land_batch_steps WHERE batch_id = ?1",
                    params![batch_id],
                    |row| row.get(0),
                )
                .map_err(map_database_error)?;
            let started_at = now(&connection)?;
            let finished_at = (payload.outcome != "running").then(|| started_at.clone());
            let (split_left, split_right) = match payload.split {
                Some([l, r]) => (Some(l as i64), Some(r as i64)),
                None => (None, None),
            };
            connection
                .execute(
                    "INSERT INTO land_batch_steps (
                        batch_id, step, member_run_ids_json, gated_member_ids_json, dropped_json,
                        base_sha, gating_head_sha, started_at, finished_at,
                        gate_reused_from, gate_exit_status, outcome,
                        split_left_count, split_right_count, published_sha
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
                    params![
                        batch_id,
                        next_step,
                        serde_json::to_string(member_ids).unwrap(),
                        serde_json::to_string(&gated_ids).unwrap(),
                        serde_json::to_string(&dropped_values).unwrap(),
                        payload.base_sha,
                        payload.gating_head_sha,
                        started_at,
                        finished_at,
                        payload.gate_reused_from,
                        payload.gate_exit_status,
                        payload.outcome,
                        split_left,
                        split_right,
                        payload.published_sha,
                    ],
                )
                .map_err(map_database_error)?;
            next_step as u64
        }
        Some(step_num) => {
            if step_num == 0 || payload.outcome == "running" {
                return Err(AppError::new(
                    "broker-batch-step-invalid",
                    "updating an existing batch step requires a positive step and terminal outcome",
                ));
            }
            let current_outcome: Option<String> = connection
                .query_row(
                    "SELECT outcome FROM land_batch_steps WHERE batch_id = ?1 AND step = ?2",
                    params![batch_id, step_num as i64],
                    |row| row.get(0),
                )
                .optional()
                .map_err(map_database_error)?;
            if current_outcome.as_deref() != Some("running") {
                return Err(AppError::new(
                    "broker-batch-step-invalid",
                    format!("batch step {step_num} is not currently running"),
                ));
            }
            let finished_at = now(&connection)?;
            let (split_left, split_right) = match payload.split {
                Some([l, r]) => (Some(l as i64), Some(r as i64)),
                None => (None, None),
            };
            let dropped_update = payload.dropped.as_ref().map(|items| {
                let values: Vec<Value> = items.iter().map(DroppedBatchMember::to_json_value).collect();
                serde_json::to_string(&values).unwrap()
            });
            connection
                .execute(
                    "UPDATE land_batch_steps
                     SET finished_at = ?1,
                         gate_reused_from = COALESCE(?2, gate_reused_from),
                         gate_exit_status = COALESCE(?3, gate_exit_status),
                         outcome = ?4,
                         split_left_count = ?5,
                         split_right_count = ?6,
                         published_sha = ?7,
                         dropped_json = COALESCE(?8, dropped_json)
                     WHERE batch_id = ?9 AND step = ?10",
                    params![
                        finished_at,
                        payload.gate_reused_from,
                        payload.gate_exit_status,
                        payload.outcome,
                        split_left,
                        split_right,
                        payload.published_sha,
                        dropped_update,
                        batch_id,
                        step_num as i64,
                    ],
                )
                .map_err(map_database_error)?;
            step_num
        }
    };
    connection
        .execute_batch("COMMIT")
        .map_err(map_database_error)?;
    Ok(json!({
        "leader_run_id": identity.run_id,
        "step": recorded_step,
    }))
}

fn apply_land_phase_update(
    connection: &Connection,
    run: &RunRecord,
    phase: &str,
    head_sha: &str,
    new_head_sha: Option<&String>,
    gate_exit_status: Option<i64>,
) -> Result<()> {
    let order = |value: &str| match value {
        "preflight" => Some(0),
        "gating" => Some(1),
        "publishing" => Some(2),
        _ => None,
    };
    let selected_gate_status = gate_exit_status.or(run.gate_exit_status);
    let new_head_valid = new_head_sha.is_none_or(|head| {
        head.len() == 40
            && head
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            && head != head_sha
            && run.phase == "preflight"
            && phase == "preflight"
            && selected_gate_status.is_none()
    });
    let valid = order(&run.phase)
        .is_some_and(|current| order(phase).is_some_and(|next| next >= current))
        && gate_exit_status.is_none_or(|status| (0..=255).contains(&status))
        && !(run.gate_exit_status.is_some()
            && gate_exit_status.is_some()
            && run.gate_exit_status != gate_exit_status)
        && !(phase == "preflight" && selected_gate_status.is_some())
        && !(phase == "publishing" && selected_gate_status != Some(0))
        && new_head_valid;
    if !valid {
        return Err(AppError::new(
            "broker-land-phase-invalid",
            "land phase transition or gate exit status is invalid",
        ));
    }
    connection
        .execute(
            "UPDATE runs SET phase = ?1, gate_exit_status = ?2, head_sha = ?3
             WHERE run_id = ?4",
            params![
                phase,
                selected_gate_status,
                new_head_sha.map(String::as_str).unwrap_or(head_sha),
                run.run_id,
            ],
        )
        .map_err(map_database_error)?;
    Ok(())
}

pub fn claim_batch(paths: &Paths, request: &BatchClaimRequest) -> Result<Value> {
    let owner = owner_info(paths)?;
    let connection = open_protocol5(paths)?;
    ensure_land_batch_schema(&connection)?;
    connection
        .execute_batch("BEGIN IMMEDIATE")
        .map_err(map_database_error)?;
    let leader = load_verified_batch_leader(
        &connection,
        &request.run_id,
        request.worker_pid,
        &request.worker_start_token,
        &request.checkout,
        &request.head_sha,
        true,
    )?;
    let live = load_live_runs(&connection)?;
    let placements = crate::overflow::live_placements(&connection)?;
    let target_runtime = match placements.get(&leader.run_id) {
        Some(pool_name) => owner
            .overflow_pools
            .iter()
            .find(|pool| &pool.name == pool_name)
            .map(Pool::runtime)
            .ok_or_else(|| {
                AppError::new(
                    "broker-row-invalid",
                    format!(
                        "running land leader {} is placed on unknown pool {pool_name}",
                        leader.run_id
                    ),
                )
            })?,
        None => owner.local_runtime.clone(),
    };
    if let Some(target) = &request.target_branch
        && (target.trim().is_empty() || target.contains('\0'))
    {
        return Err(AppError::new(
            "broker-submission-invalid",
            "batch target branch must be non-empty without NUL",
        ));
    }
    let context = BatchSelectionContext::from_live(&leader, &live, &target_runtime);
    let started_at = now(&connection)?;
    let leader_env_json: String = connection
        .query_row(
            "SELECT environment_json FROM runs WHERE run_id = ?1",
            params![leader.run_id],
            |row| row.get(0),
        )
        .map_err(map_database_error)?;

    let mut seen_branches = BTreeSet::new();
    seen_branches.insert(leader.branch.clone());
    let mut seen_requests = vec![leader.publication_request.clone()];
    let mut members = vec![member_descriptor(&leader, true)?];
    let mut eligible_candidates = Vec::new();
    let leader_checkout_str = leader.checkout.to_string_lossy().into_owned();

    for member_id in &request.member_run_ids {
        let Some(candidate) = context
            .repo_queued
            .iter()
            .copied()
            .find(|run| run.run_id == *member_id)
        else {
            continue;
        };
        if !context.candidate_eligible(candidate, &seen_branches, &seen_requests) {
            continue;
        }
        let (orig_checkout, orig_env): (String, String) = connection
            .query_row(
                "SELECT checkout, environment_json FROM runs WHERE run_id = ?1",
                params![candidate.run_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(map_database_error)?;
        seen_branches.insert(candidate.branch.clone());
        seen_requests.push(candidate.publication_request.clone());
        members.push(member_descriptor(candidate, false)?);
        eligible_candidates.push((candidate, orig_checkout, orig_env));
    }

    if !eligible_candidates.is_empty() {
        connection
            .execute(
                "INSERT INTO land_batch_members
                 (leader_run_id, member_run_id, position, active, original_checkout, original_environment_json)
                 VALUES (?1, ?2, 0, 1, ?3, ?4)
                 ON CONFLICT(member_run_id) DO UPDATE SET active = 1",
                params![
                    leader.run_id,
                    leader.run_id,
                    &leader_checkout_str,
                    leader_env_json,
                ],
            )
            .map_err(map_database_error)?;
        connection
            .execute(
                "INSERT INTO land_batches (leader_run_id, target_branch, created_at)
                 VALUES (?1, ?2, ?3)",
                params![leader.run_id, request.target_branch, started_at],
            )
            .map_err(map_database_error)?;
        let batch_id = connection.last_insert_rowid();
        let insert_entry = |position: i64, run: &RunRecord| -> Result<()> {
            let head_sha = run.head_sha.as_deref().ok_or_else(|| {
                AppError::new(
                    "broker-row-invalid",
                    format!("land batch member {} has no head SHA", run.run_id),
                )
            })?;
            let pub_req = run
                .publication_request
                .as_ref()
                .map(serde_json::to_string)
                .transpose()
                .map_err(|error| {
                    AppError::new(
                        "broker-row-invalid",
                        format!("cannot encode batch publication request: {error}"),
                    )
                })?;
            connection
                .execute(
                    "INSERT INTO land_batch_entries (
                        batch_id, position, member_run_id, branch, head_sha,
                        publication_adapter, publication_request
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                    params![
                        batch_id,
                        position,
                        run.run_id,
                        run.branch,
                        head_sha,
                        run.publication_adapter,
                        pub_req,
                    ],
                )
                .map_err(map_database_error)?;
            Ok(())
        };
        insert_entry(0, &leader)?;
        for (idx, (candidate, orig_checkout, orig_env)) in
            eligible_candidates.into_iter().enumerate()
        {
            let position = (idx + 1) as i64;
            connection
                .execute(
                    "UPDATE runs
                     SET status = 'running', phase = 'preflight', started_at = ?1,
                         environment_json = '{}', checkout = ?2
                     WHERE run_id = ?3 AND status = 'queued'",
                    params![started_at, &leader_checkout_str, candidate.run_id],
                )
                .map_err(map_database_error)?;
            connection
                .execute(
                    "INSERT OR REPLACE INTO land_batch_members
                     (leader_run_id, member_run_id, position, active, original_checkout, original_environment_json)
                     VALUES (?1, ?2, ?3, 1, ?4, ?5)",
                    params![
                        leader.run_id,
                        candidate.run_id,
                        position,
                        orig_checkout,
                        orig_env,
                    ],
                )
                .map_err(map_database_error)?;
            insert_entry(position, candidate)?;
        }
    }
    connection
        .execute_batch("COMMIT")
        .map_err(map_database_error)?;
    Ok(json!({
        "leader_run_id": leader.run_id,
        "members": members,
    }))
}

pub fn batch_status(paths: &Paths, request: &BatchStatusRequest) -> Result<Value> {
    let _owner = owner_info(paths)?;
    let connection = open_protocol5(paths)?;
    ensure_land_batch_schema(&connection)?;
    let leader = load_verified_batch_leader(
        &connection,
        &request.run_id,
        request.worker_pid,
        &request.worker_start_token,
        &request.checkout,
        &request.head_sha,
        false,
    )?;
    let mut statement = connection
        .prepare(
            "SELECT runs.run_id, runs.status, runs.phase, runs.cancel_requested, land_batch_members.active
             FROM land_batch_members
             JOIN runs ON runs.run_id = land_batch_members.member_run_id
             WHERE land_batch_members.leader_run_id = ?1
             ORDER BY land_batch_members.position",
        )
        .map_err(map_database_error)?;
    let mut rows = statement
        .query(params![leader.run_id])
        .map_err(map_database_error)?;
    let mut members = Vec::new();
    while let Some(row) = rows.next().map_err(map_database_error)? {
        members.push(json!({
            "run_id": row.get::<_, String>(0).map_err(map_database_error)?,
            "status": row.get::<_, String>(1).map_err(map_database_error)?,
            "phase": row.get::<_, String>(2).map_err(map_database_error)?,
            "cancel_requested": row.get::<_, i64>(3).map_err(map_database_error)? != 0,
            "active": row.get::<_, i64>(4).map_err(map_database_error)? != 0,
        }));
    }
    Ok(json!({
        "leader_run_id": leader.run_id,
        "cancel_requested": leader.cancel_requested,
        "members": members,
    }))
}

fn verify_active_batch_member(
    connection: &Connection,
    leader_run_id: &str,
    member_run_id: &str,
) -> Result<RunRecord> {
    let active_for_leader: Option<i64> = connection
        .query_row(
            "SELECT active FROM land_batch_members
             WHERE leader_run_id = ?1 AND member_run_id = ?2",
            params![leader_run_id, member_run_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(map_database_error)?;
    if active_for_leader != Some(1) {
        return Err(AppError::new(
            "broker-land-phase-invalid",
            format!("run {member_run_id} is not an active member of batch {leader_run_id}"),
        ));
    }
    let member = load_run(connection, member_run_id)?;
    if member.kind != "land" || member.status != "running" {
        return Err(AppError::new(
            "broker-land-phase-invalid",
            format!("batch member {member_run_id} is not running"),
        ));
    }
    Ok(member)
}

pub fn advance_batch_member_phase(
    paths: &Paths,
    request: &BatchMemberPhaseRequest,
) -> Result<Value> {
    let _owner = owner_info(paths)?;
    let connection = open_protocol5(paths)?;
    ensure_land_batch_schema(&connection)?;
    connection
        .execute_batch("BEGIN IMMEDIATE")
        .map_err(map_database_error)?;
    let _leader = load_verified_batch_leader(
        &connection,
        &request.run_id,
        request.worker_pid,
        &request.worker_start_token,
        &request.checkout,
        &request.head_sha,
        true,
    )?;
    let member = verify_active_batch_member(&connection, &request.run_id, &request.member_run_id)?;
    if member.cancel_requested {
        return Err(AppError::new(
            "broker-land-cancelled",
            "batch member cancellation committed before publication became authoritative",
        ));
    }
    let member_head = member.head_sha.clone().ok_or_else(|| {
        AppError::new(
            "broker-row-invalid",
            format!("batch member {} has no head SHA", member.run_id),
        )
    })?;
    apply_land_phase_update(
        &connection,
        &member,
        &request.phase,
        &member_head,
        request.new_head_sha.as_ref(),
        request.gate_exit_status,
    )?;
    connection
        .execute_batch("COMMIT")
        .map_err(map_database_error)?;
    status(paths, &request.member_run_id)
}

fn try_reuse_gate_for_run(
    paths: &Paths,
    connection: &Connection,
    run: &RunRecord,
    allow_batch_gating: bool,
) -> Result<Value> {
    let max_age = land_gate_reuse_max_age(paths)?;
    let phase_ok = run.phase == "preflight" || (allow_batch_gating && run.phase == "gating");
    if run.kind != "land" || run.status != "running" || !phase_ok {
        return Err(AppError::new(
            "broker-gate-reuse-invalid",
            "only a running land still in preflight may reuse a gate",
        ));
    }
    if run.cancel_requested {
        return Err(AppError::new(
            "broker-land-cancelled",
            "land cancellation committed before a gate could be reused",
        ));
    }
    if run.gate_exit_status.is_some() || run.gate_run_id.is_some() {
        return Err(AppError::new(
            "broker-gate-reuse-invalid",
            "land already carries a gate verdict",
        ));
    }
    if max_age.is_zero() {
        return Ok(json!({"reused": false, "run_id": Value::Null, "reason": "disabled"}));
    }
    let Some(head_sha) = run.head_sha.as_deref() else {
        return Ok(json!({"reused": false, "run_id": Value::Null, "reason": "no-receipt"}));
    };
    let command_json = serde_json::to_string(&run.command).map_err(|error| {
        AppError::new(
            "broker-row-invalid",
            format!("cannot encode land gate command: {error}"),
        )
    })?;
    let horizon = format!("-{} seconds", max_age.as_secs_f64());
    let candidate = connection
        .query_row(
            "SELECT run_id, finished_at FROM runs
             WHERE kind = 'full' AND status = 'passed'
               AND repository_id = ?1 AND worktree_id = ?2
               AND head_sha = ?3 AND command_json = ?4
               AND finished_at IS NOT NULL
               AND finished_at >= strftime('%Y-%m-%dT%H:%M:%fZ', 'now', ?5)
             ORDER BY sequence DESC LIMIT 1",
            params![
                run.repository_id,
                run.worktree_id,
                head_sha,
                command_json,
                horizon
            ],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()
        .map_err(map_database_error)?;
    let Some((source, finished_at)) = candidate else {
        return Ok(json!({"reused": false, "run_id": Value::Null, "reason": "no-receipt"}));
    };
    connection
        .execute(
            "UPDATE runs SET phase = 'gating', gate_exit_status = 0, gate_run_id = ?1
             WHERE run_id = ?2",
            params![source, run.run_id],
        )
        .map_err(map_database_error)?;
    Ok(json!({
        "reused": true,
        "run_id": source,
        "finished_at": finished_at,
    }))
}

pub fn reuse_batch_member_gate(paths: &Paths, request: &BatchMemberReuseRequest) -> Result<Value> {
    let _owner = owner_info(paths)?;
    let connection = open_protocol5(paths)?;
    ensure_land_batch_schema(&connection)?;
    connection
        .execute_batch("BEGIN IMMEDIATE")
        .map_err(map_database_error)?;
    let _leader = load_verified_batch_leader(
        &connection,
        &request.run_id,
        request.worker_pid,
        &request.worker_start_token,
        &request.checkout,
        &request.head_sha,
        true,
    )?;
    let member = verify_active_batch_member(&connection, &request.run_id, &request.member_run_id)?;
    let outcome = try_reuse_gate_for_run(paths, &connection, &member, true)?;
    connection
        .execute_batch("COMMIT")
        .map_err(map_database_error)?;
    Ok(outcome)
}

pub fn copy_leader_log_to_member(paths: &Paths, leader_run_id: &str, member_run_id: &str) {
    if leader_run_id == member_run_id {
        return;
    }
    let source = paths.logs.join(format!("{leader_run_id}.log"));
    let target = paths.logs.join(format!("{member_run_id}.log"));
    if let Ok(bytes) = fs::read(&source) {
        let _ = fs::write(&target, bytes);
    }
}

pub fn finish_batch_member(paths: &Paths, request: &BatchMemberFinishRequest) -> Result<Value> {
    if !(0..=255).contains(&request.exit_status) {
        return Err(AppError::new(
            "broker-land-result-invalid",
            "batch member exit status must be from 0 to 255",
        ));
    }
    if request.member_run_id == request.run_id {
        return Err(AppError::new(
            "broker-land-result-invalid",
            "batch leader reports its own result via report",
        ));
    }
    let _owner = owner_info(paths)?;
    let connection = open_protocol5(paths)?;
    ensure_land_batch_schema(&connection)?;
    connection
        .execute_batch("BEGIN IMMEDIATE")
        .map_err(map_database_error)?;
    let _leader = load_verified_batch_leader(
        &connection,
        &request.run_id,
        request.worker_pid,
        &request.worker_start_token,
        &request.checkout,
        &request.head_sha,
        false,
    )?;
    let member = verify_active_batch_member(&connection, &request.run_id, &request.member_run_id)?;
    let effective_gate = request.gate_exit_status.or(member.gate_exit_status);
    let (member_status, exit_status, failure_reason): (&str, i64, Option<String>) =
        if member.cancel_requested {
            ("cancelled", 130, None)
        } else if request.exit_status == 0 {
            ("passed", 0, None)
        } else {
            let inferred = if let Some(explicit) = &request.failure_reason {
                explicit.clone()
            } else if effective_gate.is_some_and(|code| code != 0) && member.phase == "gating" {
                "gate-failed".to_owned()
            } else {
                publication_exit_reason(request.exit_status).to_owned()
            };
            ("failed", request.exit_status, Some(inferred))
        };
    connection
        .execute(
            "UPDATE runs
             SET status = ?1, phase = 'complete', finished_at = ?2,
                 exit_status = ?3, reported_exit_status = ?3,
                 failure_reason = ?4, gate_exit_status = COALESCE(?5, gate_exit_status),
                 environment_json = '{}'
             WHERE run_id = ?6",
            params![
                member_status,
                now(&connection)?,
                exit_status,
                failure_reason,
                request.gate_exit_status,
                member.run_id,
            ],
        )
        .map_err(map_database_error)?;
    connection
        .execute(
            "UPDATE land_batch_members SET active = 0 WHERE member_run_id = ?1",
            params![member.run_id],
        )
        .map_err(map_database_error)?;
    connection
        .execute(
            "UPDATE land_batch_entries
             SET final_status = ?1, final_failure_reason = ?2
             WHERE batch_id = (
                 SELECT batch_id FROM land_batches
                 WHERE leader_run_id = ?3
                 ORDER BY batch_id DESC LIMIT 1
             ) AND member_run_id = ?4",
            params![member_status, failure_reason, request.run_id, member.run_id],
        )
        .map_err(map_database_error)?;
    connection
        .execute_batch("COMMIT")
        .map_err(map_database_error)?;
    copy_leader_log_to_member(paths, &request.run_id, &member.run_id);
    status(paths, &request.member_run_id)
}

/// Finalize or re-queue any still-active batch followers inside the leader's finish transaction.
pub fn reconcile_batch_on_leader_exit(
    paths: &Paths,
    connection: &Connection,
    leader: &RunRecord,
    leader_status: &str,
    leader_exit_status: i64,
    leader_failure_reason: Option<&str>,
) -> Result<Vec<String>> {
    ensure_land_batch_schema(connection)?;
    let finished_at = now(connection)?;
    let active_batch_id: Option<i64> = connection
        .query_row(
            "SELECT batch_id FROM land_batches
             WHERE leader_run_id = ?1
             ORDER BY batch_id DESC LIMIT 1",
            params![leader.run_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(map_database_error)?;
    if let Some(batch_id) = active_batch_id {
        connection
            .execute(
                "UPDATE land_batch_entries
                 SET final_status = ?1, final_failure_reason = ?2
                 WHERE batch_id = ?3 AND member_run_id = ?4",
                params![leader_status, leader_failure_reason, batch_id, leader.run_id],
            )
            .map_err(map_database_error)?;
        let aborted_step_outcome = if leader.cancel_requested || leader_status == "cancelled" {
            "cancelled"
        } else if leader_status == "interrupted" {
            "interrupted"
        } else {
            leader_failure_reason.unwrap_or("merge-error")
        };
        connection
            .execute(
                "UPDATE land_batch_steps
                 SET finished_at = ?1, outcome = ?2
                 WHERE batch_id = ?3 AND outcome = 'running'",
                params![finished_at, aborted_step_outcome, batch_id],
            )
            .map_err(map_database_error)?;
    }
    connection
        .execute(
            "DELETE FROM land_batch_slices WHERE leader_run_id = ?1",
            params![leader.run_id],
        )
        .map_err(map_database_error)?;
    let mut statement = connection
        .prepare(
            "SELECT member_run_id, original_checkout, original_environment_json
             FROM land_batch_members
             WHERE leader_run_id = ?1 AND active = 1",
        )
        .map_err(map_database_error)?;
    let rows = statement
        .query_map(params![leader.run_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .map_err(map_database_error)?;
    let entries: Vec<(String, String, String)> = rows
        .collect::<std::result::Result<_, _>>()
        .map_err(map_database_error)?;
    connection
        .execute(
            "DELETE FROM land_batch_members WHERE leader_run_id = ?1 AND member_run_id = ?1",
            params![leader.run_id],
        )
        .map_err(map_database_error)?;
    if entries.is_empty() {
        return Ok(Vec::new());
    }
    let mut finalized_for_log_copy = Vec::new();
    let leader_requeue =
        leader.cancel_requested || matches!(leader_status, "cancelled" | "interrupted");
    for (member_id, orig_checkout, orig_env) in entries {
        if member_id == leader.run_id {
            continue;
        }
        let Ok(member) = load_run(connection, &member_id) else {
            continue;
        };
        if member.status != "running" {
            connection
                .execute(
                    "UPDATE land_batch_members SET active = 0 WHERE member_run_id = ?1",
                    params![member_id],
                )
                .map_err(map_database_error)?;
            if let Some(batch_id) = active_batch_id {
                connection
                    .execute(
                        "UPDATE land_batch_entries
                         SET final_status = COALESCE(final_status, ?1),
                             final_failure_reason = COALESCE(final_failure_reason, ?2)
                         WHERE batch_id = ?3 AND member_run_id = ?4",
                        params![member.status, member.failure_reason, batch_id, member_id],
                    )
                    .map_err(map_database_error)?;
            }
            continue;
        }
        if leader_requeue && !member.cancel_requested {
            connection
                .execute(
                    "UPDATE runs
                     SET status = 'queued', phase = 'queued', started_at = NULL,
                         finished_at = NULL, exit_status = NULL, reported_exit_status = NULL,
                         failure_reason = NULL, gate_exit_status = NULL, gate_run_id = NULL,
                         checkout = ?1, environment_json = ?2
                     WHERE run_id = ?3",
                    params![orig_checkout, orig_env, member_id],
                )
                .map_err(map_database_error)?;
            connection
                .execute(
                    "DELETE FROM land_batch_members WHERE member_run_id = ?1",
                    params![member_id],
                )
                .map_err(map_database_error)?;
            if let Some(batch_id) = active_batch_id {
                connection
                    .execute(
                        "UPDATE land_batch_entries
                         SET requeued = 1, final_status = 'requeued', final_failure_reason = NULL
                         WHERE batch_id = ?1 AND member_run_id = ?2",
                        params![batch_id, member_id],
                    )
                    .map_err(map_database_error)?;
            }
        } else {
            let (status, exit_status, failure_reason) = if member.cancel_requested {
                ("cancelled", 130, None)
            } else {
                (
                    "failed",
                    leader_exit_status,
                    Some(leader_failure_reason.unwrap_or("batch-leader-failed")),
                )
            };
            connection
                .execute(
                    "UPDATE runs
                     SET status = ?1, phase = 'complete', finished_at = ?2,
                         exit_status = ?3, failure_reason = ?4, environment_json = '{}'
                     WHERE run_id = ?5",
                    params![status, finished_at, exit_status, failure_reason, member_id],
                )
                .map_err(map_database_error)?;
            connection
                .execute(
                    "UPDATE land_batch_members SET active = 0 WHERE member_run_id = ?1",
                    params![member_id],
                )
                .map_err(map_database_error)?;
            if let Some(batch_id) = active_batch_id {
                connection
                    .execute(
                        "UPDATE land_batch_entries
                         SET final_status = ?1, final_failure_reason = ?2
                         WHERE batch_id = ?3 AND member_run_id = ?4",
                        params![status, failure_reason, batch_id, member_id],
                    )
                    .map_err(map_database_error)?;
            }
            finalized_for_log_copy.push(member_id);
        }
    }
    let _ = paths;
    Ok(finalized_for_log_copy)
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
                    serde_json::to_string(
                        &resource_contract(&resources, &BTreeMap::new()).unwrap(),
                    )
                    .unwrap(),
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
    let source_maintenance = maintenance_record(&connection)?;
    if from_protocol < 4 && source_maintenance.is_some() {
        return Err(AppError::new(
            "broker-maintenance-invalid",
            "durable maintenance metadata requires protocol 4 or 5",
        ));
    }
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
    maintenance_record(&connection)?;
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
    let maintenance = maintenance_record(&connection)?;
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
    remove_maintenance_guards(&connection)?;
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
    connection
        .execute(
            "DELETE FROM coordinator_meta WHERE key IN (
                'maintenance_state', 'maintenance_id',
                'maintenance_reason', 'maintenance_started_at'
             )",
            [],
        )
        .map_err(map_database_error)?;
    if let Some(maintenance) = &maintenance {
        for (key, value) in [
            ("maintenance_state", maintenance.state.as_str()),
            ("maintenance_id", maintenance.drain_id.as_str()),
            ("maintenance_reason", maintenance.reason.as_str()),
            ("maintenance_started_at", maintenance.started_at.as_str()),
        ] {
            set_metadata(&connection, key, value)?;
        }
        install_maintenance_guards(&connection)?;
    }
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
    maintenance_record(&connection)?;
    Ok(json!({"changed": true, "from_protocol": PROTOCOL, "to_protocol": 4}))
}
