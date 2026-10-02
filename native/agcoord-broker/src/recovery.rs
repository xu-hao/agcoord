//! Owner-locked host recovery and the durable verification-only admission guard.
use crate::broker::Broker;
use crate::cgroup::{CgroupBackend, sha256_prefix};
use crate::error::{AppError, Result};
use crate::resources;
use crate::store::{self, Paths, SubmitRequest, load_live_runs, load_run, map_database_error};
use rusqlite::{Connection, params};
use serde_json::{Value, json};
use std::collections::BTreeSet;
use std::fs;
use std::io::Read;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

const KEY: &str = "host_recovery";
const MAX_ATTEMPTS: u64 = 3;

fn refused(message: impl Into<String>) -> AppError {
    AppError::new("host-recovery-invalid", message)
}

pub fn valid_id(value: &str) -> bool {
    value.len() == 21
        && value.starts_with("recovery-")
        && value[9..]
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn identity() -> Value {
    serde_json::from_str(&crate::identity_json()).expect("static native identity")
}

fn probe_digest(path: &Path) -> Result<String> {
    let details = fs::symlink_metadata(path)
        .map_err(|e| refused(format!("cannot inspect proof helper: {e}")))?;
    if !path.is_absolute()
        || !details.is_file()
        || details.file_type().is_symlink()
        || details.permissions().mode() & 0o022 != 0
        || details.len() > 1024 * 1024
        || details.permissions().mode() & 0o111 == 0
    {
        return Err(refused(
            "proof helper must be a bounded, non-writable, regular executable",
        ));
    }
    let bytes = fs::read(path).map_err(|e| refused(format!("cannot read proof helper: {e}")))?;
    Ok(sha256_prefix(&bytes, 32))
}

fn original_drain_valid(value: &Value) -> bool {
    if value.is_null() {
        return true;
    }
    let keys = ["state", "drain_id", "reason", "started_at"];
    value.as_object().is_some_and(|o| {
        o.keys().map(String::as_str).collect::<BTreeSet<_>>() == keys.into_iter().collect()
    }) && matches!(value["state"].as_str(), Some("draining" | "drained"))
        && value["drain_id"]
            .as_str()
            .is_some_and(store::drain_id_valid)
        && value["reason"]
            .as_str()
            .is_some_and(|r| !r.is_empty() && r.chars().count() <= 256 && !r.contains('\0'))
        && value["started_at"]
            .as_str()
            .is_some_and(store::maintenance_time_valid)
}

fn validate_record(value: &Value, completed: bool) -> Result<()> {
    let mut keys = BTreeSet::from([
        "format",
        "recovery_id",
        "identity",
        "probe",
        "probe_sha256",
        "proof_run_id",
        "attempts",
        "phase",
        "incident",
        "original_drain",
    ]);
    if completed {
        keys.insert("completion");
    }
    if value
        .as_object()
        .is_none_or(|o| o.keys().map(String::as_str).collect::<BTreeSet<_>>() != keys)
        || value["format"] != 2
        || !value["recovery_id"].as_str().is_some_and(valid_id)
        || value["identity"] != identity()
        || !value["probe"]
            .as_str()
            .is_some_and(|p| Path::new(p).is_absolute())
        || !value["probe_sha256"].as_str().is_some_and(|s| {
            s.len() == 64
                && s.bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        })
        || !(value["proof_run_id"].is_null()
            || value["proof_run_id"]
                .as_str()
                .is_some_and(|s| !s.is_empty()))
        || value["attempts"].as_u64().is_none_or(|n| n > MAX_ATTEMPTS)
        || !(if completed {
            value["phase"] == "complete"
        } else {
            matches!(
                value["phase"].as_str(),
                Some("guarded" | "verifying" | "parked")
            )
        })
        || !(value["incident"].is_null()
            || value["incident"]
                .as_str()
                .is_some_and(|s| !s.is_empty() && s.len() <= 256))
        || !original_drain_valid(&value["original_drain"])
    {
        return Err(refused(
            "recovery record or replacement identity does not match",
        ));
    }
    if completed {
        let receipt = &value["completion"];
        let keys = BTreeSet::from(["state", "recovery_id", "proof_run_id", "drain_id"]);
        let drain = &value["original_drain"];
        if receipt
            .as_object()
            .is_none_or(|o| o.keys().map(String::as_str).collect::<BTreeSet<_>>() != keys)
            || receipt["recovery_id"] != value["recovery_id"]
            || receipt["proof_run_id"] != value["proof_run_id"]
            || !(if drain.is_null() {
                receipt["state"] == "open" && receipt["drain_id"].is_null()
            } else {
                matches!(receipt["state"].as_str(), Some("draining" | "drained"))
                    && receipt["drain_id"] == drain["drain_id"]
            })
        {
            return Err(refused(
                "completed recovery receipt does not match its retained drain",
            ));
        }
    }
    Ok(())
}

pub fn record(connection: &Connection) -> Result<Option<Value>> {
    // Read the marker, its guards, and its target from one snapshot even when completion
    // concurrently restores the drain or removes them.
    let _snapshot = if connection.is_autocommit() {
        Some(
            connection
                .unchecked_transaction()
                .map_err(map_database_error)?,
        )
    } else {
        None
    };
    let raw = store::metadata(connection, KEY)?;
    let maintenance = store::maintenance_record(connection)?;
    let recovering = maintenance
        .as_ref()
        .is_some_and(|m| m.state == "recovering");
    let Some(raw) = raw else {
        if recovering {
            return Err(refused("recovery guard has no recovery record"));
        }
        return Ok(None);
    };
    let value: Value = serde_json::from_str(&raw).map_err(|_| refused("invalid recovery JSON"))?;
    validate_record(&value, false)?;
    if !recovering || value["recovery_id"] != maintenance.unwrap().drain_id {
        return Err(refused(
            "recovery record does not match its maintenance guard",
        ));
    }
    Ok(Some(value))
}

fn save(connection: &Connection, record: &Value) -> Result<()> {
    store::set_metadata(connection, KEY, &record.to_string())
}

fn required(connection: &Connection, recovery_id: &str) -> Result<Value> {
    let value = record(connection)?.ok_or_else(|| refused("no guarded recovery is active"))?;
    if value["recovery_id"] != recovery_id {
        return Err(refused("recovery ID does not match"));
    }
    Ok(value)
}

fn receipt(value: &Value) -> Value {
    let mut result = value.clone();
    result["state"] = json!("recovering");
    result["protocol"] = json!(store::PROTOCOL);
    result
}

fn process_fields(pid: u32) -> Result<Option<(String, u32)>> {
    let raw = match fs::read_to_string(format!("/proc/{pid}/stat")) {
        Ok(raw) => raw,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(AppError::new(
                "host-recovery-worker-live",
                format!("worker identity unavailable: {error}"),
            ));
        }
    };
    let fields: Vec<_> = raw
        .rsplit_once(')')
        .ok_or_else(|| refused("malformed process identity"))?
        .1
        .split_whitespace()
        .collect();
    let state = fields
        .first()
        .ok_or_else(|| refused("missing process state"))?
        .to_string();
    let group = fields
        .get(2)
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| refused("missing process group"))?;
    Ok(Some((state, group)))
}

fn prove_workers_gone(paths: &Paths, connection: &Connection) -> Result<()> {
    let running: Vec<_> = load_live_runs(connection)?
        .into_iter()
        .filter(|r| r.status == "running")
        .collect();
    let mut groups = BTreeSet::new();
    for run in &running {
        let pid = run
            .worker_pid
            .filter(|pid| *pid > 0)
            .ok_or_else(|| refused("running row has no worker identity"))?;
        if run.worker_start_token.as_deref().is_none_or(str::is_empty) {
            return Err(refused("running row has no worker start token"));
        }
        if process_fields(pid)?.is_some_and(|(s, _)| !matches!(s.as_str(), "Z" | "X")) {
            return Err(AppError::new(
                "host-recovery-worker-live",
                format!("worker for {} is present or ambiguous", run.run_id),
            ));
        }
        groups.insert(pid);
    }
    if !groups.is_empty() {
        for entry in fs::read_dir("/proc")
            .map_err(|e| refused(format!("cannot inspect process groups: {e}")))?
        {
            let entry = entry.map_err(|e| refused(format!("cannot inspect process entry: {e}")))?;
            let Some(pid) = entry
                .file_name()
                .to_str()
                .and_then(|s| s.parse::<u32>().ok())
            else {
                continue;
            };
            if process_fields(pid)?.is_some_and(|(state, group)| {
                groups.contains(&group) && !matches!(state.as_str(), "Z" | "X")
            }) {
                return Err(AppError::new(
                    "host-recovery-worker-live",
                    "a retained worker process group is still populated",
                ));
            }
        }
    }
    if running
        .iter()
        .any(|r| r.resource_state.contains_key(resources::CGROUP_BACKEND))
    {
        let configuration = resources::load_configuration(&paths.state_dir)?;
        let backend = CgroupBackend::new(&configuration, &paths.state_dir, None)
            .map_err(|e| refused(format!("cannot inspect retained cgroup state: {}", e.code)))?;
        for run in &running {
            if let Some(state) = run.resource_state.get(resources::CGROUP_BACKEND) {
                let request = Broker::cgroup_request(run, &state.resources)?;
                backend
                    .prove_recovery_empty(&request, &state.handle)
                    .map_err(|e| {
                        AppError::new(
                            "host-recovery-worker-live",
                            format!("retained cgroup is populated or unverifiable: {}", e.code),
                        )
                    })?;
            }
        }
    }
    Ok(())
}

/// Called only while the host maintenance holder owns the spool lock.
pub fn prepare(paths: &Paths, probe: &Path) -> Result<Value> {
    let connection = store::open_protocol5(paths)?;
    connection
        .execute_batch("BEGIN IMMEDIATE")
        .map_err(map_database_error)?;
    prove_workers_gone(paths, &connection)?;
    let digest = probe_digest(probe)?;
    if let Some(value) = record(&connection)? {
        if value["probe"] != probe.to_string_lossy().as_ref() || value["probe_sha256"] != digest {
            return Err(refused(
                "retry must use the same proof helper and replacement identity",
            ));
        }
        connection
            .execute_batch("COMMIT")
            .map_err(map_database_error)?;
        return Ok(receipt(&value));
    }
    let original_drain = match store::maintenance_record(&connection)? {
        Some(drain) => {
            if drain.state == "drained" && !load_live_runs(&connection)?.is_empty() {
                return Err(refused("drained maintenance still contains accepted work"));
            }
            json!({"state":drain.state,"drain_id":drain.drain_id,
                "reason":drain.reason,"started_at":drain.started_at})
        }
        None => Value::Null,
    };
    let mut random = [0u8; 6];
    fs::File::open("/dev/urandom")
        .and_then(|mut f| f.read_exact(&mut random))
        .map_err(|e| refused(format!("cannot create recovery identity: {e}")))?;
    let recovery_id = format!(
        "recovery-{}",
        random
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>()
    );
    let value = json!({"format":2,"recovery_id":recovery_id,"identity":identity(),"probe":probe,"probe_sha256":digest,"proof_run_id":null,"attempts":0,"phase":"guarded","incident":null,"original_drain":original_drain});
    if original_drain.is_null() {
        store::install_maintenance_guards(&connection)?;
    }
    for (key, value) in [
        ("maintenance_state", "recovering".to_owned()),
        ("maintenance_id", recovery_id),
        (
            "maintenance_reason",
            "guarded native host recovery".to_owned(),
        ),
        ("maintenance_started_at", store::now(&connection)?),
    ] {
        store::set_metadata(&connection, key, &value)?;
    }
    save(&connection, &value)?;
    connection
        .execute_batch("COMMIT")
        .map_err(map_database_error)?;
    Ok(receipt(&value))
}

pub fn status(paths: &Paths) -> Result<Value> {
    let connection = store::open_protocol5(paths)?;
    record(&connection)?
        .map(|v| receipt(&v))
        .ok_or_else(|| refused("no guarded recovery is active"))
}

/// Ordinary jobs never become eligible while any recovery record is active.
pub fn filter_queued(connection: &Connection, queued: &mut Vec<store::RunRecord>) -> Result<()> {
    let Some(value) = record(connection)? else {
        return Ok(());
    };
    if value["phase"] != "verifying" {
        queued.clear();
        return Ok(());
    }
    if probe_digest(Path::new(value["probe"].as_str().unwrap()))? != value["probe_sha256"] {
        return Err(refused("proof helper changed before admission"));
    }
    queued.retain(|run| value["proof_run_id"] == run.run_id);
    Ok(())
}

pub fn submit_proof(paths: &Paths, recovery_id: &str, request: &SubmitRequest) -> Result<Value> {
    let owner = store::owner_info(paths)?;
    store::validate_submit(request, &owner)?;
    let connection = store::open_protocol5(paths)?;
    connection
        .execute_batch("BEGIN IMMEDIATE")
        .map_err(map_database_error)?;
    let mut value = required(&connection, recovery_id)?;
    if request.kind != "check"
        || request.resources.len() != 2
        || request.resources.get("cpu") != Some(&1)
        || request.resources.get("jobs") != Some(&1)
        || request.command.len() != 1
        || request.command[0] != value["probe"]
        || request.gate_run_id.is_some()
        || request.publication_adapter.is_some()
        || request.publication_request.is_some()
        || probe_digest(Path::new(&request.command[0]))? != value["probe_sha256"]
    {
        return Err(refused(
            "only the exact cpu=1 enforcement helper may run during recovery",
        ));
    }
    if let Some(id) = value["proof_run_id"].as_str() {
        let prior = load_run(&connection, id)?;
        if value["phase"] == "verifying"
            && matches!(prior.status.as_str(), "queued" | "running" | "passed")
        {
            return Ok(json!({"run_id":id}));
        }
    }
    if value["attempts"].as_u64().unwrap() >= MAX_ATTEMPTS {
        value["phase"] = json!("parked");
        value["incident"] = json!("proof-attempts-exhausted");
        save(&connection, &value)?;
        connection
            .execute_batch("COMMIT")
            .map_err(map_database_error)?;
        return Err(AppError::new(
            "host-recovery-exhausted",
            "three enforcement attempts exhausted; recovery remains parked",
        ));
    }
    if load_live_runs(&connection)?
        .iter()
        .any(|r| r.status == "running")
    {
        return Err(AppError::new(
            "host-recovery-settling",
            "dead worker settlement has not completed",
        ));
    }
    // Trigger replacement and insertion share one transaction; no other writer observes a gap.
    store::remove_maintenance_guards(&connection)?;
    store::insert_check(&connection, request, &owner)?;
    store::install_maintenance_guards(&connection)?;
    value["proof_run_id"] = json!(request.run_id);
    value["attempts"] = json!(value["attempts"].as_u64().unwrap() + 1);
    value["phase"] = json!("verifying");
    value["incident"] = Value::Null;
    save(&connection, &value)?;
    connection
        .execute_batch("COMMIT")
        .map_err(map_database_error)?;
    Ok(json!({"run_id":request.run_id}))
}

pub fn complete(paths: &Paths, recovery_id: &str) -> Result<Value> {
    let connection = store::open_protocol5(paths)?;
    connection
        .execute_batch("BEGIN IMMEDIATE")
        .map_err(map_database_error)?;
    if record(&connection)?.is_none() {
        let raw = store::metadata(&connection, "host_recovery_last")?
            .ok_or_else(|| refused("no matching completed recovery exists"))?;
        let last: Value =
            serde_json::from_str(&raw).map_err(|_| refused("invalid completed recovery record"))?;
        validate_record(&last, true)?;
        if last["recovery_id"] != recovery_id {
            return Err(refused("completed recovery identity does not match"));
        }
        let proof_id = last["proof_run_id"]
            .as_str()
            .ok_or_else(|| refused("completed recovery has no proof"))?;
        let proof = load_run(&connection, proof_id)?;
        if proof.status != "passed"
            || proof.exit_status != Some(0)
            || proof.resource_receipt["requested"]["cpu"] != 1
            || proof.resource_receipt["applied"]["cpu"] != 1
            || proof.resource_receipt["peak"]["cpu"]
                .as_u64()
                .is_none_or(|n| n < 1)
        {
            return Err(refused("completed recovery proof is invalid"));
        }
        return Ok(last["completion"].clone());
    }
    store::owner_info(paths)?;
    let mut value = required(&connection, recovery_id)?;
    let proof_id = value["proof_run_id"]
        .as_str()
        .ok_or_else(|| refused("no enforcement proof exists"))?;
    let proof = load_run(&connection, proof_id)?;
    if value["phase"] != "verifying"
        || proof.status != "passed"
        || proof.exit_status != Some(0)
        || proof.resource_receipt["requested"]["cpu"] != 1
        || proof.resource_receipt["applied"]["cpu"] != 1
        || proof.resource_receipt["peak"]["cpu"]
            .as_u64()
            .is_none_or(|n| n < 1)
        || probe_digest(Path::new(value["probe"].as_str().unwrap()))? != value["probe_sha256"]
        || load_live_runs(&connection)?
            .iter()
            .any(|r| r.status == "running")
    {
        return Err(AppError::new(
            "host-recovery-proof-failed",
            "a passed enforced cpu=1 receipt is required; recovery remains guarded",
        ));
    }
    let drain = &value["original_drain"];
    let completed = if drain.is_null() {
        store::remove_maintenance_guards(&connection)?;
        connection.execute("DELETE FROM coordinator_meta WHERE key IN ('maintenance_state','maintenance_id','maintenance_reason','maintenance_started_at')", []).map_err(map_database_error)?;
        json!({"state":"open","recovery_id":recovery_id,"proof_run_id":proof.run_id,"drain_id":null})
    } else {
        let state = if load_live_runs(&connection)?.is_empty() {
            "drained"
        } else {
            "draining"
        };
        for (key, retained) in [
            ("maintenance_state", state),
            ("maintenance_id", drain["drain_id"].as_str().unwrap()),
            ("maintenance_reason", drain["reason"].as_str().unwrap()),
            (
                "maintenance_started_at",
                drain["started_at"].as_str().unwrap(),
            ),
        ] {
            store::set_metadata(&connection, key, retained)?;
        }
        json!({"state":state,"recovery_id":recovery_id,"proof_run_id":proof.run_id,"drain_id":drain["drain_id"]})
    };
    value["phase"] = json!("complete");
    value["completion"] = completed.clone();
    store::set_metadata(&connection, "host_recovery_last", &value.to_string())?;
    connection
        .execute(
            "DELETE FROM coordinator_meta WHERE key = 'host_recovery'",
            [],
        )
        .map_err(map_database_error)?;
    connection
        .execute_batch("COMMIT")
        .map_err(map_database_error)?;
    Ok(completed)
}

pub fn park(paths: &Paths, recovery_id: &str, reason: &str) -> Result<Value> {
    if reason.is_empty() || reason.len() > 256 || reason.contains('\0') {
        return Err(refused("invalid incident reason"));
    }
    let connection = store::open_protocol5(paths)?;
    connection
        .execute_batch("BEGIN IMMEDIATE")
        .map_err(map_database_error)?;
    let mut value = required(&connection, recovery_id)?;
    if let Some(id) = value["proof_run_id"].as_str() {
        connection.execute("UPDATE runs SET cancel_requested=1, cancel_requested_at=?1 WHERE run_id=?2 AND status='running'", params![store::now(&connection)?,id]).map_err(map_database_error)?;
        connection.execute("UPDATE runs SET status='cancelled', phase='complete', exit_status=130, finished_at=?1, failure_reason='host-recovery-parked' WHERE run_id=?2 AND status='queued'", params![store::now(&connection)?,id]).map_err(map_database_error)?;
    }
    value["phase"] = json!("parked");
    value["incident"] = json!(reason);
    save(&connection, &value)?;
    connection
        .execute_batch("COMMIT")
        .map_err(map_database_error)?;
    Ok(receipt(&value))
}
