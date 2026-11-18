//! Overflow pools: named capacities on another machine, reached through an executor.
//!
//! A pool is provider-neutral.  The broker only knows its capacities, how long an eligible
//! row waits before it may be placed there, the hard runtime of a placed run, an optional
//! daily CPU-hour budget, and the executor command that carries the row's command to the
//! other machine.  The executor runs as the row's ordinary local worker, so supervision,
//! cancellation, logs, and the exit status keep their existing contracts.

use crate::error::{AppError, Result};
use crate::resources::name_valid;
use crate::store::{RunRecord, map_database_error};
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

/// The reserved submission entry that marks a row as allowed to leave this machine.
pub const ELIGIBLE: &str = "_AGCOORD_OVERFLOW";
/// The reserved submission entry carrying required runtime capabilities as a JSON array.
pub const CAPABILITIES_ENV: &str = "_AGCOORD_CAPABILITIES";
/// The reserved submission entry carrying allowed runtime names as a JSON array.
pub const RUNTIMES_ENV: &str = "_AGCOORD_RUNTIMES";
/// Executor placement variable set to `"1"` on privileged pools and `"0"` on unprivileged pools.
pub const OVERFLOW_PRIVILEGED_ENV: &str = "AGCOORD_OVERFLOW_PRIVILEGED";
/// Reserved runtime name for the native host execution target.
pub const LOCAL_RUNTIME: &str = "local";
/// Standard capability indicating user-namespace / unprivileged sandbox creation support.
pub const USERNS_CAPABILITY: &str = "userns";
/// The resource whose claim a budget counts.
pub const BUDGET_RESOURCE: &str = "cpu";
pub const RUNTIME_EXCEEDED: &str = "overflow-runtime-exceeded";
pub const RUNTIME_EXCEEDED_EXIT: i64 = 124;
pub const DELEGATED_CODE: &str = "overflow-delegated";
/// The exit status with which an executor reports that it could not obtain a verdict,
/// such as a lost connection or a preempted machine.
pub const EXECUTOR_FAILED_EXIT: i64 = 125;
pub const EXECUTOR_FAILED: &str = "overflow-executor-failed";

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS overflow_placements (
    run_id TEXT PRIMARY KEY,
    pool TEXT NOT NULL,
    budget_day TEXT NOT NULL,
    cpu_seconds INTEGER NOT NULL CHECK (cpu_seconds >= 0),
    settled INTEGER NOT NULL DEFAULT 0 CHECK (settled IN (0, 1)),
    placed_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS overflow_placements_budget
    ON overflow_placements(pool, budget_day);
CREATE INDEX IF NOT EXISTS overflow_placements_live
    ON overflow_placements(settled);
";

/// How many budget days of settled placements are kept as accounting history.
const RETAINED_DAYS: i64 = 3;

#[derive(Clone, Debug, PartialEq)]
pub struct Budget {
    /// The daily allowance in CPU-seconds.
    pub cpu_seconds: u64,
    /// Minutes after 00:00 UTC at which a new budget day begins.
    pub rollover_minutes: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Runtime {
    pub name: String,
    pub capacities: BTreeMap<String, u64>,
    pub max_claim: BTreeMap<String, u64>,
    pub privileged: bool,
    pub capabilities: BTreeSet<String>,
}

impl Runtime {
    pub fn local(capacities: BTreeMap<String, u64>, capabilities: BTreeSet<String>) -> Self {
        Self {
            name: LOCAL_RUNTIME.to_owned(),
            capacities,
            max_claim: BTreeMap::new(),
            privileged: false,
            capabilities,
        }
    }

    pub fn can_ever_fit(&self, resources: &BTreeMap<String, u64>) -> bool {
        resources.iter().all(|(name, units)| {
            self.capacities.get(name).is_some_and(|cap| units <= cap)
                && self.max_claim.get(name).is_none_or(|limit| units <= limit)
        })
    }

    pub fn satisfies(
        &self,
        resources: &BTreeMap<String, u64>,
        required_capabilities: &BTreeSet<String>,
    ) -> bool {
        self.can_ever_fit(resources) && required_capabilities.is_subset(&self.capabilities)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Pool {
    pub name: String,
    pub capacities: BTreeMap<String, u64>,
    /// The largest single claim the pool's target can run, per resource.
    pub max_claim: BTreeMap<String, u64>,
    pub executor: Vec<String>,
    pub wait_threshold: u64,
    pub max_runtime: u64,
    pub budget: Option<Budget>,
    pub cpu_hour_price: Option<f64>,
    pub privileged: bool,
    pub capabilities: BTreeSet<String>,
}

fn config_error(message: impl Into<String>) -> AppError {
    AppError::new("broker-config-invalid", message)
}

pub fn parse_capabilities_field(value: Option<&Value>, subject: &str) -> Result<BTreeSet<String>> {
    let Some(value) = value else {
        return Ok(BTreeSet::new());
    };
    let entries = value
        .as_array()
        .ok_or_else(|| config_error(format!("{subject} capabilities must be an array")))?;
    let mut selected = BTreeSet::new();
    for entry in entries {
        let capability = entry
            .as_str()
            .filter(|capability| name_valid(capability))
            .ok_or_else(|| {
                config_error(format!("{subject} contains an invalid capability name"))
            })?;
        if !selected.insert(capability.to_owned()) {
            return Err(config_error(format!(
                "{subject} capability {capability} is configured twice"
            )));
        }
    }
    Ok(selected)
}

pub fn load_local_capabilities(state_dir: &Path) -> Result<BTreeSet<String>> {
    let text = match fs::read_to_string(state_dir.join("config.json")) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(BTreeSet::new());
        }
        Err(error) => {
            return Err(config_error(format!(
                "cannot read broker configuration: {error}"
            )));
        }
    };
    let document: Value = serde_json::from_str(&text)
        .map_err(|_| config_error("broker configuration is not valid JSON"))?;
    parse_capabilities_field(document.get("capabilities"), "broker configuration")
}

pub fn local_runtime_json(runtime: &Runtime) -> String {
    json!({
        "privileged": runtime.privileged,
        "capabilities": runtime.capabilities,
    })
    .to_string()
}

pub fn parse_local_runtime_json(
    value: &str,
    capacities: &BTreeMap<String, u64>,
) -> Result<Runtime> {
    let invalid = || {
        AppError::new(
            "broker-owner-metadata-invalid",
            "live owner local runtime metadata is invalid",
        )
    };
    let parsed: Value = serde_json::from_str(value).map_err(|_| invalid())?;
    let object = parsed.as_object().ok_or_else(invalid)?;
    if object
        .keys()
        .any(|key| !matches!(key.as_str(), "privileged" | "capabilities"))
    {
        return Err(invalid());
    }
    let privileged = object
        .get("privileged")
        .and_then(Value::as_bool)
        .ok_or_else(invalid)?;
    if privileged {
        return Err(invalid());
    }
    let capabilities = parse_capabilities_field(object.get("capabilities"), "local runtime")
        .map_err(|_| invalid())?;
    Ok(Runtime::local(capacities.clone(), capabilities))
}

pub fn parse_placement_constraints(
    environment: &BTreeMap<String, String>,
) -> Result<(Option<BTreeSet<String>>, BTreeSet<String>)> {
    let submission_error =
        |message: &str| AppError::new("broker-submission-invalid", message.to_owned());
    let parse_name_list = |raw: &str,
                           subject: &str,
                           allow_empty: bool|
     -> Result<BTreeSet<String>> {
        let value: Value = serde_json::from_str(raw)
            .map_err(|_| submission_error(&format!("{subject} must be valid JSON")))?;
        let entries = value
            .as_array()
            .ok_or_else(|| submission_error(&format!("{subject} must be a JSON array")))?;
        if !allow_empty && entries.is_empty() {
            return Err(submission_error(&format!("{subject} cannot be empty")));
        }
        let mut selected = BTreeSet::new();
        for entry in entries {
            let name = entry
                .as_str()
                .filter(|name| name_valid(name))
                .ok_or_else(|| submission_error(&format!("{subject} contains an invalid name")))?;
            selected.insert(name.to_owned());
        }
        Ok(selected)
    };
    let allowed_runtimes = environment
        .get(RUNTIMES_ENV)
        .map(|raw| parse_name_list(raw, "runtime selector", false))
        .transpose()?;
    let required_capabilities = environment
        .get(CAPABILITIES_ENV)
        .map(|raw| parse_name_list(raw, "capability selector", true))
        .transpose()?
        .unwrap_or_default();
    Ok((allowed_runtimes, required_capabilities))
}

/// Probe whether admitted local jobs on this host can create an unprivileged user namespace
/// (either via `/usr/bin/bwrap` under the admitted AppArmor stack or direct `CLONE_NEWUSER`).
pub fn probe_local_userns() -> bool {
    static PROBED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *PROBED.get_or_init(run_local_userns_probe)
}

fn run_local_userns_probe() -> bool {
    const APPARMOR_CURRENT: &str = "/proc/self/attr/current";
    const APPARMOR_BROKER: &str = "agcoord-broker (enforce)";
    const APPARMOR_ADMITTED: &str = "agcoord-admitted (enforce)";
    const APPARMOR_CHANGE_ADMITTED: &[u8] = b"changeprofile agcoord-admitted";
    const LINUX_CAPABILITY_VERSION_3: u32 = 0x2008_0522;
    const PR_SET_NO_NEW_PRIVS: libc::c_int = 38;
    const PR_CAP_AMBIENT: libc::c_int = 47;
    const PR_CAP_AMBIENT_CLEAR_ALL: libc::c_ulong = 4;

    #[repr(C)]
    struct CapabilityHeader {
        version: u32,
        pid: i32,
    }

    #[repr(C)]
    #[derive(Clone, Copy)]
    struct CapabilityData {
        effective: u32,
        permitted: u32,
        inheritable: u32,
    }

    // SAFETY: `fork` runs during single-threaded broker startup; the child performs only
    // bounded kernel probes and terminates via `_exit`.
    let pid = unsafe { libc::fork() };
    if pid < 0 {
        return false;
    }
    if pid == 0 {
        let code = (|| -> i32 {
            // SAFETY: `/dev/null` is a static C string and `dup2`/`close`/`close_range`
            // operate only on this forked child's file descriptor table.
            unsafe {
                let devnull = libc::open(c"/dev/null".as_ptr(), libc::O_RDWR | libc::O_CLOEXEC);
                if devnull >= 0 {
                    for target in [0, 1, 2] {
                        let _ = libc::dup2(devnull, target);
                    }
                    if devnull > 2 {
                        let _ = libc::close(devnull);
                    }
                }
                let _ = libc::syscall(libc::SYS_close_range, 3_u32, u32::MAX, 0);
            }

            let was_broker_profile = fs::read_to_string(APPARMOR_CURRENT)
                .is_ok_and(|current| current.trim() == APPARMOR_BROKER);
            if was_broker_profile {
                use std::io::Write;
                use std::os::unix::fs::OpenOptionsExt;
                let changed = fs::OpenOptions::new()
                    .write(true)
                    .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
                    .open(APPARMOR_CURRENT)
                    .and_then(|mut file| file.write_all(APPARMOR_CHANGE_ADMITTED));
                if changed.is_err() {
                    return 1;
                }
                let verified = fs::read_to_string(APPARMOR_CURRENT)
                    .is_ok_and(|current| current.trim() == APPARMOR_ADMITTED);
                if !verified {
                    return 1;
                }
            }

            let header = CapabilityHeader {
                version: LINUX_CAPABILITY_VERSION_3,
                pid: 0,
            };
            let data = [CapabilityData {
                effective: 0,
                permitted: 0,
                inheritable: 0,
            }; 2];
            // SAFETY: `prctl` and `capset` receive valid scalar arguments and stack-allocated
            // version-3 capability structures matching admitted worker privilege drop.
            let dropped = unsafe {
                libc::prctl(
                    PR_CAP_AMBIENT,
                    PR_CAP_AMBIENT_CLEAR_ALL,
                    0_u64,
                    0_u64,
                    0_u64,
                ) == 0
                    && libc::syscall(
                        libc::SYS_capset,
                        &header as *const CapabilityHeader,
                        data.as_ptr(),
                    ) == 0
                    && libc::prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) == 0
            };
            if !dropped {
                return 1;
            }

            // SAFETY: `access` inspects static NUL-terminated paths.
            let bwrap_available =
                unsafe { libc::access(c"/usr/bin/bwrap".as_ptr(), libc::X_OK) == 0 };
            if bwrap_available {
                // SAFETY: `access` inspects static NUL-terminated candidate paths.
                let true_bin =
                    if unsafe { libc::access(c"/usr/bin/true".as_ptr(), libc::X_OK) } == 0 {
                        c"/usr/bin/true"
                    } else {
                        c"/bin/true"
                    };
                let args = [
                    c"/usr/bin/bwrap".as_ptr(),
                    c"--unshare-user".as_ptr(),
                    c"--ro-bind".as_ptr(),
                    c"/".as_ptr(),
                    c"/".as_ptr(),
                    true_bin.as_ptr(),
                    std::ptr::null(),
                ];
                // SAFETY: `args` is a NUL-terminated array of valid C string pointers.
                unsafe {
                    libc::execv(c"/usr/bin/bwrap".as_ptr(), args.as_ptr());
                }
                return 1;
            }

            if was_broker_profile {
                return 1;
            }

            // SAFETY: `geteuid`, `getegid`, and `unshare(CLONE_NEWUSER)` take no pointers.
            let (uid, gid, unshared) = unsafe {
                (
                    libc::geteuid(),
                    libc::getegid(),
                    libc::unshare(libc::CLONE_NEWUSER),
                )
            };
            if unshared != 0 {
                return 1;
            }
            let _ = fs::write("/proc/self/setgroups", b"deny");
            if fs::write("/proc/self/uid_map", format!("0 {uid} 1\n")).is_err()
                || fs::write("/proc/self/gid_map", format!("0 {gid} 1\n")).is_err()
            {
                return 1;
            }
            0
        })();
        // SAFETY: `_exit` terminates the forked probe child immediately.
        unsafe { libc::_exit(code) };
    }

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    let mut status: libc::c_int = 0;
    loop {
        // SAFETY: `status` is a valid out-pointer for `waitpid` on our child `pid`.
        let waited = unsafe { libc::waitpid(pid, &mut status, libc::WNOHANG) };
        if waited == pid {
            return libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0;
        }
        if waited < 0 {
            if std::io::Error::last_os_error().kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            return false;
        }
        if std::time::Instant::now() >= deadline {
            // SAFETY: `pid` is our still-unreaped child process.
            unsafe {
                let _ = libc::kill(pid, libc::SIGKILL);
                let _ = libc::waitpid(pid, &mut status, 0);
            }
            return false;
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
}

fn rollover_text(minutes: u32) -> String {
    format!("{:02}:{:02}", minutes / 60, minutes % 60)
}

fn parse_rollover(value: &str) -> Option<u32> {
    let (hours, minutes) = value.split_once(':')?;
    if hours.len() != 2 || minutes.len() != 2 {
        return None;
    }
    let hours: u32 = hours.parse().ok()?;
    let minutes: u32 = minutes.parse().ok()?;
    (hours < 24 && minutes < 60).then_some(hours * 60 + minutes)
}

impl Pool {
    fn parse(value: &Value) -> Result<Self> {
        let object = value
            .as_object()
            .ok_or_else(|| config_error("an overflow pool must be an object"))?;
        let allowed = [
            "name",
            "capacities",
            "max_claim",
            "executor",
            "wait_threshold",
            "max_runtime",
            "budget",
            "cpu_hour_price",
            "privileged",
            "capabilities",
        ];
        if let Some(unknown) = object.keys().find(|key| !allowed.contains(&key.as_str())) {
            return Err(config_error(format!(
                "overflow pool has unknown key {unknown}"
            )));
        }
        let name = object
            .get("name")
            .and_then(Value::as_str)
            .filter(|name| name_valid(name))
            .ok_or_else(|| config_error("overflow pool name is missing or invalid"))?
            .to_owned();
        if name == LOCAL_RUNTIME {
            return Err(config_error(
                "overflow pool name local is reserved for the native host runtime",
            ));
        }
        let capacities = object
            .get("capacities")
            .and_then(Value::as_object)
            .filter(|capacities| !capacities.is_empty())
            .ok_or_else(|| {
                config_error(format!("overflow pool {name} requires a capacities object"))
            })?
            .iter()
            .map(|(resource, units)| {
                let units = units
                    .as_u64()
                    .filter(|units| *units > 0 && *units <= i64::MAX as u64)
                    .filter(|_| name_valid(resource))
                    .ok_or_else(|| {
                        config_error(format!(
                            "overflow pool {name} capacity {resource} is invalid"
                        ))
                    })?;
                Ok((resource.clone(), units))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        let max_claim = match object.get("max_claim") {
            None | Some(Value::Null) => BTreeMap::new(),
            Some(value) => value
                .as_object()
                .ok_or_else(|| {
                    config_error(format!("overflow pool {name} max_claim must be an object"))
                })?
                .iter()
                .map(|(resource, units)| {
                    let units = units
                        .as_u64()
                        .filter(|units| *units > 0)
                        .filter(|units| capacities.get(resource).is_some_and(|cap| units <= cap))
                        .ok_or_else(|| {
                            config_error(format!(
                                "overflow pool {name} max_claim {resource} must be a positive \
                                 amount no larger than the pool's capacity"
                            ))
                        })?;
                    Ok((resource.clone(), units))
                })
                .collect::<Result<BTreeMap<_, _>>>()?,
        };
        let executor = object
            .get("executor")
            .and_then(Value::as_array)
            .filter(|executor| !executor.is_empty())
            .and_then(|executor| {
                executor
                    .iter()
                    .map(|argument| {
                        argument
                            .as_str()
                            .filter(|argument| !argument.contains('\0'))
                            .map(str::to_owned)
                    })
                    .collect::<Option<Vec<_>>>()
            })
            .filter(|executor| !executor[0].is_empty())
            .ok_or_else(|| {
                config_error(format!(
                    "overflow pool {name} requires a non-empty executor command"
                ))
            })?;
        let wait_threshold = match object.get("wait_threshold") {
            None => 0,
            Some(value) => value
                .as_u64()
                .filter(|seconds| *seconds <= i64::MAX as u64)
                .ok_or_else(|| {
                    config_error(format!(
                        "overflow pool {name} wait_threshold must be whole seconds"
                    ))
                })?,
        };
        let max_runtime = object
            .get("max_runtime")
            .and_then(Value::as_u64)
            .filter(|seconds| *seconds > 0 && *seconds <= u64::from(u32::MAX))
            .ok_or_else(|| {
                config_error(format!(
                    "overflow pool {name} requires max_runtime in positive whole seconds"
                ))
            })?;
        let budget = match object.get("budget") {
            None | Some(Value::Null) => None,
            Some(value) => {
                let section = value.as_object().ok_or_else(|| {
                    config_error(format!("overflow pool {name} budget must be an object"))
                })?;
                if let Some(unknown) = section
                    .keys()
                    .find(|key| !["cpu_hours_per_day", "rollover_utc"].contains(&key.as_str()))
                {
                    return Err(config_error(format!(
                        "overflow pool {name} budget has unknown key {unknown}"
                    )));
                }
                let hours = section
                    .get("cpu_hours_per_day")
                    .and_then(Value::as_f64)
                    .filter(|hours| hours.is_finite() && *hours > 0.0 && *hours <= 1.0e9)
                    .ok_or_else(|| {
                        config_error(format!(
                            "overflow pool {name} budget requires positive cpu_hours_per_day"
                        ))
                    })?;
                let rollover_minutes = match section.get("rollover_utc") {
                    None => 0,
                    Some(value) => value.as_str().and_then(parse_rollover).ok_or_else(|| {
                        config_error(format!(
                            "overflow pool {name} rollover_utc must be HH:MM in UTC"
                        ))
                    })?,
                };
                Some(Budget {
                    cpu_seconds: (hours * 3600.0).round() as u64,
                    rollover_minutes,
                })
            }
        };
        let cpu_hour_price = match object.get("cpu_hour_price") {
            None | Some(Value::Null) => None,
            Some(value) => Some(
                value
                    .as_f64()
                    .filter(|price| price.is_finite() && *price >= 0.0)
                    .ok_or_else(|| {
                        config_error(format!(
                            "overflow pool {name} cpu_hour_price must be a non-negative number"
                        ))
                    })?,
            ),
        };
        let privileged = match object.get("privileged") {
            None => false,
            Some(value) => value.as_bool().ok_or_else(|| {
                config_error(format!(
                    "overflow pool {name} privileged must be true or false"
                ))
            })?,
        };
        let mut capabilities =
            parse_capabilities_field(object.get("capabilities"), &format!("overflow pool {name}"))?;
        if privileged {
            capabilities.insert(USERNS_CAPABILITY.to_owned());
        }
        Ok(Self {
            name,
            capacities,
            max_claim,
            executor,
            wait_threshold,
            max_runtime,
            budget,
            cpu_hour_price,
            privileged,
            capabilities,
        })
    }

    pub fn runtime(&self) -> Runtime {
        Runtime {
            name: self.name.clone(),
            capacities: self.capacities.clone(),
            max_claim: self.max_claim.clone(),
            privileged: self.privileged,
            capabilities: self.capabilities.clone(),
        }
    }

    /// The configuration form, which is also what the owner publishes for clients.
    pub fn to_value(&self) -> Value {
        let mut value = Map::new();
        value.insert("name".to_owned(), json!(self.name));
        value.insert("capacities".to_owned(), json!(self.capacities));
        value.insert("max_claim".to_owned(), json!(self.max_claim));
        value.insert("executor".to_owned(), json!(self.executor));
        value.insert("wait_threshold".to_owned(), json!(self.wait_threshold));
        value.insert("max_runtime".to_owned(), json!(self.max_runtime));
        value.insert(
            "budget".to_owned(),
            self.budget.as_ref().map_or(Value::Null, |budget| {
                json!({
                    "cpu_hours_per_day": budget.cpu_seconds as f64 / 3600.0,
                    "rollover_utc": rollover_text(budget.rollover_minutes),
                })
            }),
        );
        value.insert("cpu_hour_price".to_owned(), json!(self.cpu_hour_price));
        value.insert("privileged".to_owned(), json!(self.privileged));
        value.insert("capabilities".to_owned(), json!(self.capabilities));
        Value::Object(value)
    }

    /// Whether the pool offers every claimed resource in the claimed amount at all, as one
    /// job.
    pub fn can_ever_fit(&self, resources: &BTreeMap<String, u64>) -> bool {
        resources.iter().all(|(name, units)| {
            self.capacities.get(name).is_some_and(|cap| units <= cap)
                && self.max_claim.get(name).is_none_or(|limit| units <= limit)
        })
    }

    /// Whether the claim fits beside the pool's running rows right now.
    pub fn fits(&self, resources: &BTreeMap<String, u64>, used: &BTreeMap<String, u64>) -> bool {
        self.can_ever_fit(resources)
            && resources.iter().all(|(name, units)| {
                self.capacities.get(name).is_some_and(|capacity| {
                    used.get(name).copied().unwrap_or(0).saturating_add(*units) <= *capacity
                })
            })
    }

    /// The worst-case CPU-seconds a placed row can consume before its runtime limit.
    pub fn reservation(&self, resources: &BTreeMap<String, u64>) -> u64 {
        resources
            .get(BUDGET_RESOURCE)
            .copied()
            .unwrap_or(0)
            .saturating_mul(self.max_runtime)
    }
}

pub fn parse_pools(value: Option<&Value>) -> Result<Vec<Pool>> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    let entries = value
        .as_array()
        .ok_or_else(|| config_error("broker configuration overflow_pools must be an array"))?;
    let mut pools = Vec::new();
    let mut names = BTreeSet::new();
    for entry in entries {
        let pool = Pool::parse(entry)?;
        if !names.insert(pool.name.clone()) {
            return Err(config_error(format!(
                "overflow pool {} is configured twice",
                pool.name
            )));
        }
        pools.push(pool);
    }
    Ok(pools)
}

pub fn load_pools(state_dir: &Path) -> Result<Vec<Pool>> {
    let text = match fs::read_to_string(state_dir.join("config.json")) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => {
            return Err(config_error(format!(
                "cannot read broker configuration: {error}"
            )));
        }
    };
    let document: Value = serde_json::from_str(&text)
        .map_err(|_| config_error("broker configuration is not valid JSON"))?;
    parse_pools(document.get("overflow_pools"))
}

pub fn pools_json(pools: &[Pool]) -> String {
    Value::Array(pools.iter().map(Pool::to_value).collect()).to_string()
}

pub fn parse_pools_json(value: &str) -> Result<Vec<Pool>> {
    let invalid = || {
        AppError::new(
            "broker-owner-metadata-invalid",
            "live owner overflow pools are invalid",
        )
    };
    let value: Value = serde_json::from_str(value).map_err(|_| invalid())?;
    parse_pools(Some(&value)).map_err(|_| invalid())
}

/// Whether a placed run's executor reported that it could not obtain a verdict.  A land
/// reports its gate's status; every other kind reports the executor's own exit status.
pub fn executor_failed(run: &RunRecord, exit_status: i64) -> bool {
    if run.kind == "land" {
        run.gate_exit_status == Some(EXECUTOR_FAILED_EXIT)
    } else {
        exit_status == EXECUTOR_FAILED_EXIT
    }
}

/// Whether the submitter allowed this row to run on another machine.
pub fn requested(environment: &BTreeMap<String, String>) -> bool {
    environment.get(ELIGIBLE).is_some_and(|value| value == "1")
}

pub fn ensure_schema(connection: &Connection) -> Result<()> {
    connection.execute_batch(SCHEMA).map_err(map_database_error)
}

fn schema_present(connection: &Connection) -> Result<bool> {
    connection
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'overflow_placements'",
            [],
            |_| Ok(()),
        )
        .optional()
        .map(|found| found.is_some())
        .map_err(map_database_error)
}

fn placements(connection: &Connection, live_only: bool) -> Result<BTreeMap<String, String>> {
    if !schema_present(connection)? {
        return Ok(BTreeMap::new());
    }
    let query = if live_only {
        "SELECT run_id, pool FROM overflow_placements WHERE settled = 0"
    } else {
        "SELECT run_id, pool FROM overflow_placements"
    };
    let mut statement = connection.prepare(query).map_err(map_database_error)?;
    let rows = statement
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .map_err(map_database_error)?;
    rows.collect::<std::result::Result<BTreeMap<_, _>, _>>()
        .map_err(map_database_error)
}

/// The pool of every run that still holds pool capacity, by run ID.
pub fn live_placements(connection: &Connection) -> Result<BTreeMap<String, String>> {
    placements(connection, true)
}

/// The pool of every retained placement, including finished runs.
pub fn all_placements(connection: &Connection) -> Result<BTreeMap<String, String>> {
    placements(connection, false)
}

/// The summed claims of the running rows placed in one pool.
pub fn pool_allocations(
    active: &[RunRecord],
    placed: &BTreeMap<String, String>,
    pool: &str,
) -> BTreeMap<String, u64> {
    let mut used = BTreeMap::new();
    for run in active {
        if placed.get(&run.run_id).map(String::as_str) != Some(pool) {
            continue;
        }
        for (name, units) in &run.resources {
            let entry = used.entry(name.clone()).or_insert(0_u64);
            *entry = entry.saturating_add(*units);
        }
    }
    used
}

fn budget_day(connection: &Connection, budget: Option<&Budget>) -> Result<String> {
    let minutes = budget.map_or(0, |budget| budget.rollover_minutes);
    connection
        .query_row(
            "SELECT strftime('%Y-%m-%d', 'now', ?1)",
            params![format!("-{minutes} minutes")],
            |row| row.get(0),
        )
        .map_err(map_database_error)
}

/// The CPU-seconds charged to the pool's current budget day: reservations of running
/// rows plus the settled use of finished ones.
pub fn budget_used(connection: &Connection, pool: &Pool) -> Result<u64> {
    if !schema_present(connection)? {
        return Ok(0);
    }
    let day = budget_day(connection, pool.budget.as_ref())?;
    let used: i64 = connection
        .query_row(
            "SELECT COALESCE(SUM(cpu_seconds), 0) FROM overflow_placements
             WHERE pool = ?1 AND budget_day = ?2",
            params![pool.name, day],
            |row| row.get(0),
        )
        .map_err(map_database_error)?;
    Ok(used.max(0) as u64)
}

/// Whether the pool's remaining daily budget covers this claim's worst case.
pub fn budget_allows(
    connection: &Connection,
    pool: &Pool,
    resources: &BTreeMap<String, u64>,
) -> Result<bool> {
    let Some(budget) = &pool.budget else {
        return Ok(true);
    };
    let used = budget_used(connection, pool)?;
    Ok(used.saturating_add(pool.reservation(resources)) <= budget.cpu_seconds)
}

/// Whole seconds a row has waited since the stored instant `since`: its submission, or the
/// moment it last entered the queue.
pub fn waited_seconds(connection: &Connection, since: &str) -> Result<u64> {
    let waited: Option<f64> = connection
        .query_row(
            "SELECT (julianday('now') - julianday(?1)) * 86400.0",
            params![since],
            |row| row.get(0),
        )
        .map_err(map_database_error)?;
    Ok(waited.unwrap_or(0.0).max(0.0) as u64)
}

/// Record a placement and its worst-case reservation.  The caller holds the admission
/// transaction, so the reservation commits with the row's transition to running.
pub fn place(connection: &Connection, run: &RunRecord, pool: &Pool) -> Result<()> {
    let day = budget_day(connection, pool.budget.as_ref())?;
    connection
        .execute(
            "DELETE FROM overflow_placements
             WHERE settled = 1 AND julianday(placed_at) < julianday('now') - ?1",
            params![RETAINED_DAYS],
        )
        .map_err(map_database_error)?;
    connection
        .execute(
            "INSERT INTO overflow_placements (run_id, pool, budget_day, cpu_seconds, placed_at)
             VALUES (?1, ?2, ?3, ?4, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))",
            params![
                run.run_id,
                pool.name,
                day,
                pool.reservation(&run.resources).min(i64::MAX as u64) as i64,
            ],
        )
        .map_err(map_database_error)?;
    Ok(())
}

/// Release a finished run's pool capacity and return the unused part of its reservation:
/// the charge becomes the claimed CPUs times the seconds the run actually held them.
pub fn settle(connection: &Connection, run: &RunRecord) -> Result<()> {
    let cpus = run.resources.get(BUDGET_RESOURCE).copied().unwrap_or(0);
    connection
        .execute(
            "UPDATE overflow_placements
             SET settled = 1,
                 cpu_seconds = MIN(
                     cpu_seconds,
                     ?2 * (CAST((julianday('now') - julianday(placed_at)) * 86400.0 AS INTEGER) + 1)
                 )
             WHERE run_id = ?1 AND settled = 0",
            params![run.run_id, cpus.min(i64::MAX as u64) as i64],
        )
        .map_err(map_database_error)?;
    Ok(())
}

/// Whether a placed run has held its pool capacity longer than the pool allows.
pub fn runtime_exceeded(connection: &Connection, run_id: &str, pool: &Pool) -> Result<bool> {
    let elapsed: Option<f64> = connection
        .query_row(
            "SELECT (julianday('now') - julianday(placed_at)) * 86400.0
             FROM overflow_placements WHERE run_id = ?1 AND settled = 0",
            params![run_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(map_database_error)?;
    Ok(elapsed.is_some_and(|elapsed| elapsed >= pool.max_runtime as f64))
}

/// The public record of each pool: its configuration, what its running rows hold, and
/// the state of its budget day.
pub fn public_pools(
    connection: &Connection,
    pools: &[Pool],
    active: &[RunRecord],
    placed: &BTreeMap<String, String>,
) -> Result<Value> {
    let mut selected = Vec::new();
    for pool in pools {
        let used = pool_allocations(active, placed, &pool.name);
        let allocations: BTreeMap<_, _> = pool
            .capacities
            .keys()
            .map(|name| (name.clone(), used.get(name).copied().unwrap_or(0)))
            .collect();
        let mut value = pool.to_value();
        let object = value.as_object_mut().unwrap();
        object.insert("allocations".to_owned(), json!(allocations));
        if let Some(budget) = &pool.budget {
            let used_seconds = budget_used(connection, pool)?;
            let section = object
                .get_mut("budget")
                .and_then(Value::as_object_mut)
                .unwrap();
            section.insert(
                "used_cpu_hours".to_owned(),
                json!(used_seconds as f64 / 3600.0),
            );
            section.insert(
                "remaining_cpu_hours".to_owned(),
                json!(budget.cpu_seconds.saturating_sub(used_seconds) as f64 / 3600.0),
            );
        }
        selected.push(value);
    }
    Ok(Value::Array(selected))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rollover_accepts_only_utc_clock_times() {
        assert_eq!(parse_rollover("00:00"), Some(0));
        assert_eq!(parse_rollover("23:59"), Some(23 * 60 + 59));
        assert_eq!(parse_rollover("24:00"), None);
        assert_eq!(parse_rollover("7:30"), None);
        assert_eq!(parse_rollover("07:60"), None);
    }

    #[test]
    fn published_pools_parse_back_to_the_same_configuration() {
        let pools = parse_pools(Some(&json!([{
            "name": "cloud",
            "capacities": {"jobs": 4, "cpu": 64},
            "max_claim": {"cpu": 32},
            "executor": ["run-elsewhere", "--zone", "a"],
            "wait_threshold": 300,
            "max_runtime": 3600,
            "budget": {"cpu_hours_per_day": 100, "rollover_utc": "08:30"},
            "cpu_hour_price": 0.034,
        }])))
        .unwrap();
        assert_eq!(parse_pools_json(&pools_json(&pools)).unwrap(), pools);
        assert_eq!(pools[0].budget.as_ref().unwrap().cpu_seconds, 360_000);
        assert_eq!(
            pools[0].reservation(&BTreeMap::from([("cpu".to_owned(), 16)])),
            57_600
        );
        assert!(!pools[0].privileged);
        assert!(pools[0].capabilities.is_empty());
    }

    #[test]
    fn privileged_pools_imply_userns_and_local_name_is_reserved() {
        let pools = parse_pools(Some(&json!([
            {
                "name": "batch-priv",
                "capacities": {"jobs": 2, "cpu": 16},
                "executor": ["agc", "overflow-gcp-batch"],
                "max_runtime": 1800,
                "privileged": true,
                "capabilities": ["ssd", "userns"],
            }
        ])))
        .unwrap();
        assert!(pools[0].privileged);
        assert_eq!(
            pools[0].capabilities,
            BTreeSet::from(["ssd".to_owned(), "userns".to_owned()])
        );
        assert_eq!(parse_pools_json(&pools_json(&pools)).unwrap(), pools);

        let reserved = parse_pools(Some(&json!([{
            "name": "local",
            "capacities": {"jobs": 1, "cpu": 2},
            "executor": ["true"],
            "max_runtime": 60,
        }])))
        .unwrap_err();
        assert_eq!(reserved.code, "broker-config-invalid");

        for bad in [
            json!({
                "name": "bad-priv",
                "capacities": {"jobs": 1},
                "executor": ["true"],
                "max_runtime": 60,
                "privileged": 1,
            }),
            json!({
                "name": "bad-caps",
                "capacities": {"jobs": 1},
                "executor": ["true"],
                "max_runtime": 60,
                "capabilities": ["dup", "dup"],
            }),
            json!({
                "name": "bad-cap-name",
                "capacities": {"jobs": 1},
                "executor": ["true"],
                "max_runtime": 60,
                "capabilities": ["not valid"],
            }),
        ] {
            assert_eq!(
                parse_pools(Some(&json!([bad]))).unwrap_err().code,
                "broker-config-invalid"
            );
        }
    }
}
