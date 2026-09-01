use crate::error::{AppError, Result};
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

pub const CGROUP_BACKEND: &str = "cgroup-v2";
pub const RESOURCE_OPERATIONS: [&str; 6] =
    ["prepare", "attach", "usage", "finish", "cancel", "cleanup"];

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Binding {
    pub backend: Option<String>,
    pub kind: String,
    pub mode: String,
    pub unit: String,
}

impl Binding {
    pub fn admission() -> Self {
        Self {
            backend: None,
            kind: "generic".to_owned(),
            mode: "admission-only".to_owned(),
            unit: "admission-unit".to_owned(),
        }
    }

    pub fn enforced(&self) -> bool {
        self.mode != "admission-only"
    }

    pub fn required(&self) -> bool {
        self.mode == "required"
    }

    pub fn to_value(&self) -> Value {
        json!({
            "backend": self.backend,
            "kind": self.kind,
            "mode": self.mode,
            "unit": self.unit,
        })
    }
}

#[derive(Clone, Debug)]
pub struct ResourceConfiguration {
    pub capacities: BTreeMap<String, u64>,
    pub bindings: BTreeMap<String, Binding>,
    pub cgroup_root: Option<PathBuf>,
    pub cgroup_io_paths: Vec<PathBuf>,
}

#[derive(Clone, Debug)]
pub struct Capability {
    pub available: bool,
    pub kinds: BTreeSet<String>,
    pub units: BTreeSet<String>,
    pub operations: BTreeSet<String>,
    pub reason: Option<String>,
}

#[derive(Clone, Debug)]
pub struct BackendState {
    pub handle: Value,
    pub resources: Vec<String>,
    pub finished: bool,
    pub cancelled: bool,
}

pub fn parse_backend_state(value: &Value) -> Option<BTreeMap<String, BackendState>> {
    let object = value.as_object()?;
    let expected = BTreeSet::from(["cancelled", "finished", "handle", "resources"]);
    let mut selected = BTreeMap::new();
    let mut claimed = BTreeSet::new();
    for (backend, raw) in object {
        if !name_valid(backend) {
            return None;
        }
        let raw = raw.as_object()?;
        if raw.keys().map(String::as_str).collect::<BTreeSet<_>>() != expected {
            return None;
        }
        let handle = raw.get("handle")?.as_object()?;
        let mut names = raw
            .get("resources")?
            .as_array()?
            .iter()
            .map(|name| {
                name.as_str()
                    .filter(|name| name_valid(name))
                    .map(str::to_owned)
            })
            .collect::<Option<Vec<_>>>()?;
        names.sort();
        if names.is_empty()
            || names.windows(2).any(|pair| pair[0] == pair[1])
            || names.iter().any(|name| !claimed.insert(name.clone()))
        {
            return None;
        }
        selected.insert(
            backend.clone(),
            BackendState {
                handle: Value::Object(handle.clone()),
                resources: names,
                finished: raw.get("finished")?.as_bool()?,
                cancelled: raw.get("cancelled")?.as_bool()?,
            },
        );
    }
    Some(selected)
}

pub fn backend_state_value(state: &BTreeMap<String, BackendState>) -> Value {
    Value::Object(
        state
            .iter()
            .map(|(backend, record)| {
                (
                    backend.clone(),
                    json!({
                        "handle": record.handle,
                        "resources": record.resources,
                        "finished": record.finished,
                        "cancelled": record.cancelled,
                    }),
                )
            })
            .collect(),
    )
}

impl Capability {
    pub fn unavailable(reason: &str) -> Self {
        Self {
            available: false,
            kinds: BTreeSet::new(),
            units: BTreeSet::new(),
            operations: BTreeSet::new(),
            reason: Some(reason.to_owned()),
        }
    }

    pub fn to_value(&self) -> Value {
        json!({
            "available": self.available,
            "kinds": self.kinds,
            "units": self.units,
            "operations": self.operations,
            "reason": self.reason,
        })
    }
}

fn config_error(message: impl Into<String>) -> AppError {
    AppError::new("broker-config-invalid", message)
}

pub fn name_valid(value: &str) -> bool {
    let bytes = value.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 64
        && bytes[0].is_ascii_lowercase()
        && bytes[1..].iter().all(|byte| {
            byte.is_ascii_lowercase()
                || byte.is_ascii_digit()
                || matches!(byte, b'_' | b'.' | b':' | b'-')
        })
}

pub fn code_valid(value: &str) -> bool {
    let bytes = value.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 64
        && bytes[0].is_ascii_lowercase()
        && bytes[1..]
            .iter()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'-')
}

fn valid_kind_unit(kind: &str, unit: &str) -> bool {
    match kind {
        "generic" => unit == "admission-unit",
        "cpu" => unit == "logical-cpu",
        "memory" | "memory-high" | "swap" | "tmpfs" | "storage" => unit == "bytes",
        "io-bandwidth" => matches!(
            unit,
            "bytes-per-second" | "read-bytes-per-second" | "write-bytes-per-second"
        ),
        "io-operations" => matches!(
            unit,
            "operations-per-second" | "read-operations-per-second" | "write-operations-per-second"
        ),
        "io-weight" => unit == "weight",
        "inodes" => unit == "inodes",
        "processes" => unit == "processes",
        _ => false,
    }
}

pub fn parse_bindings(value: Option<&Value>) -> Result<BTreeMap<String, Binding>> {
    let Some(value) = value else {
        return Ok(BTreeMap::new());
    };
    let bindings = value
        .as_object()
        .ok_or_else(|| config_error("broker configuration bindings must be a JSON object"))?;
    let mut selected = BTreeMap::new();
    for (name, raw) in bindings {
        if !name_valid(name) {
            return Err(config_error("resource binding name is invalid"));
        }
        let raw = raw.as_object().ok_or_else(|| {
            config_error(format!("resource binding {name} must be a JSON object"))
        })?;
        let expected = BTreeSet::from(["backend", "kind", "mode", "unit"]);
        if raw.keys().map(String::as_str).collect::<BTreeSet<_>>() != expected {
            return Err(config_error(format!(
                "resource binding {name} must contain exactly backend, kind, mode, and unit"
            )));
        }
        let kind = raw["kind"]
            .as_str()
            .ok_or_else(|| config_error(format!("resource binding {name} kind is invalid")))?;
        let unit = raw["unit"]
            .as_str()
            .ok_or_else(|| config_error(format!("resource binding {name} unit is invalid")))?;
        if !valid_kind_unit(kind, unit) {
            return Err(config_error(format!(
                "resource binding {name} has an unsupported kind and unit"
            )));
        }
        let mode = raw["mode"]
            .as_str()
            .filter(|mode| matches!(*mode, "admission-only" | "best-effort" | "required"))
            .ok_or_else(|| config_error(format!("resource binding {name} mode is invalid")))?;
        let backend = match raw.get("backend") {
            Some(Value::Null) => None,
            Some(Value::String(backend)) if name_valid(backend) => Some(backend.clone()),
            _ => {
                return Err(config_error(format!(
                    "resource binding {name} backend is invalid"
                )));
            }
        };
        if (mode == "admission-only") != backend.is_none() {
            return Err(config_error(format!(
                "resource binding {name} backend does not match its mode"
            )));
        }
        selected.insert(
            name.clone(),
            Binding {
                backend,
                kind: kind.to_owned(),
                mode: mode.to_owned(),
                unit: unit.to_owned(),
            },
        );
    }
    Ok(selected)
}

pub fn bindings_value(bindings: &BTreeMap<String, Binding>) -> Value {
    Value::Object(
        bindings
            .iter()
            .map(|(name, binding)| (name.clone(), binding.to_value()))
            .collect(),
    )
}

pub fn parse_bindings_json(value: &str) -> Result<BTreeMap<String, Binding>> {
    let value: Value = serde_json::from_str(value).map_err(|_| {
        AppError::new(
            "broker-owner-metadata-invalid",
            "live owner resource bindings are not valid JSON",
        )
    })?;
    parse_bindings(Some(&value)).map_err(|_| {
        AppError::new(
            "broker-owner-metadata-invalid",
            "live owner resource bindings are invalid",
        )
    })
}

pub fn load_configuration(state_dir: &Path) -> Result<ResourceConfiguration> {
    let path = state_dir.join("config.json");
    let text = match fs::read_to_string(&path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(ResourceConfiguration {
                capacities: BTreeMap::from([("jobs".to_owned(), 2)]),
                bindings: BTreeMap::new(),
                cgroup_root: None,
                cgroup_io_paths: Vec::new(),
            });
        }
        Err(error) => {
            return Err(config_error(format!(
                "cannot read broker configuration: {error}"
            )));
        }
    };
    let document: Value = serde_json::from_str(&text)
        .map_err(|_| config_error("broker configuration is not valid JSON"))?;
    let object = document
        .as_object()
        .ok_or_else(|| config_error("broker configuration must be one JSON object"))?;
    let mut capacities = object
        .get("capacities")
        .map(|value| {
            value
                .as_object()
                .ok_or_else(|| config_error("broker configuration capacities must be an object"))?
                .iter()
                .map(|(name, units)| {
                    if !name_valid(name) {
                        return Err(config_error("capacity name is invalid"));
                    }
                    let units = units
                        .as_u64()
                        .filter(|units| *units > 0 && *units <= i64::MAX as u64)
                        .ok_or_else(|| config_error(format!("capacity {name} is invalid")))?;
                    Ok((name.clone(), units))
                })
                .collect::<Result<BTreeMap<_, _>>>()
        })
        .transpose()?
        .unwrap_or_default();
    capacities.entry("jobs".to_owned()).or_insert(2);
    let bindings = parse_bindings(object.get("bindings"))?;
    let cgroup_root = object
        .get("cgroup_root")
        .and_then(Value::as_str)
        .map(PathBuf::from)
        .map(|root| {
            std::path::absolute(root)
                .map_err(|_| config_error("cannot resolve configured cgroup root"))
        })
        .transpose()?;
    let cgroup_io_paths = object
        .get("cgroup_io")
        .and_then(Value::as_object)
        .and_then(|section| section.get("paths"))
        .and_then(Value::as_array)
        .map(|paths| {
            paths
                .iter()
                .filter_map(Value::as_str)
                .map(PathBuf::from)
                .collect()
        })
        .unwrap_or_default();
    Ok(ResourceConfiguration {
        capacities,
        bindings,
        cgroup_root,
        cgroup_io_paths,
    })
}

pub fn resource_contract(
    resources: &BTreeMap<String, u64>,
    bindings: &BTreeMap<String, Binding>,
) -> Result<Value> {
    let mut contract = Map::new();
    let mut scratch_kinds = BTreeSet::new();
    for name in resources.keys() {
        let binding = bindings
            .get(name)
            .cloned()
            .unwrap_or_else(Binding::admission);
        if binding.enforced() && matches!(binding.kind.as_str(), "storage" | "tmpfs") {
            scratch_kinds.insert(binding.kind.clone());
        }
        contract.insert(name.clone(), binding.to_value());
    }
    if scratch_kinds.len() > 1 {
        return Err(AppError::new(
            "broker-submission-invalid",
            "one run cannot combine persistent-storage and tmpfs scratch providers",
        ));
    }
    Ok(Value::Object(contract))
}

pub fn initial_receipt(resources: &BTreeMap<String, u64>) -> Value {
    json!({
        "requested": resources,
        "applied": {},
        "peak": {},
        "events": [],
    })
}

pub fn receipt_valid(value: &Value, resources: &BTreeMap<String, u64>) -> bool {
    let Some(receipt) = value.as_object() else {
        return false;
    };
    if receipt.keys().map(String::as_str).collect::<BTreeSet<_>>()
        != BTreeSet::from(["applied", "events", "peak", "requested"])
    {
        return false;
    }
    let mapping = |name: &str, allow_zero: bool| -> Option<BTreeMap<String, u64>> {
        receipt
            .get(name)?
            .as_object()?
            .iter()
            .map(|(resource, raw)| {
                raw.as_u64()
                    .filter(|units| allow_zero || *units > 0)
                    .map(|units| (resource.clone(), units))
            })
            .collect()
    };
    let Some(requested) = mapping("requested", false) else {
        return false;
    };
    let Some(applied) = mapping("applied", false) else {
        return false;
    };
    let Some(peak) = mapping("peak", true) else {
        return false;
    };
    if &requested != resources
        || applied
            .iter()
            .any(|(name, units)| requested.get(name).is_none_or(|limit| units > limit))
        || peak.keys().any(|name| !requested.contains_key(name))
    {
        return false;
    }
    let stages = BTreeSet::from([
        "probe", "prepare", "attach", "usage", "finish", "cancel", "cleanup",
    ]);
    let statuses = BTreeSet::from(["applied", "recorded", "unapplied", "failed"]);
    receipt
        .get("events")
        .and_then(Value::as_array)
        .is_some_and(|events| {
            events.iter().all(|event| {
                let Some(event) = event.as_object() else {
                    return false;
                };
                event.keys().map(String::as_str).collect::<BTreeSet<_>>()
                    == BTreeSet::from(["at", "backend", "code", "resource", "stage", "status"])
                    && event
                        .get("at")
                        .and_then(Value::as_str)
                        .is_some_and(|value| !value.is_empty())
                    && event
                        .get("backend")
                        .and_then(Value::as_str)
                        .is_some_and(name_valid)
                    && event
                        .get("resource")
                        .and_then(Value::as_str)
                        .is_some_and(|name| requested.contains_key(name))
                    && event
                        .get("stage")
                        .and_then(Value::as_str)
                        .is_some_and(|stage| stages.contains(stage))
                    && event
                        .get("status")
                        .and_then(Value::as_str)
                        .is_some_and(|status| statuses.contains(status))
                    && event
                        .get("code")
                        .and_then(Value::as_str)
                        .is_some_and(code_valid)
            })
        })
}

pub fn parse_capabilities_json(value: &str) -> Result<Value> {
    let value: Value = serde_json::from_str(value).map_err(|_| {
        AppError::new(
            "broker-owner-metadata-invalid",
            "live owner resource capabilities are not valid JSON",
        )
    })?;
    if !value.is_object() {
        return Err(AppError::new(
            "broker-owner-metadata-invalid",
            "live owner resource capabilities are invalid",
        ));
    }
    Ok(value)
}

pub fn capability_issue(binding: &Binding, capability: Option<&Value>) -> Option<String> {
    let Some(capability) = capability.and_then(Value::as_object) else {
        return Some("backend-unavailable".to_owned());
    };
    if capability.get("available").and_then(Value::as_bool) != Some(true) {
        return capability
            .get("reason")
            .and_then(Value::as_str)
            .filter(|reason| code_valid(reason))
            .map(str::to_owned)
            .or_else(|| Some("backend-unavailable".to_owned()));
    }
    let contains = |field: &str, expected: &str| {
        capability
            .get(field)
            .and_then(Value::as_array)
            .is_some_and(|values| values.iter().any(|value| value.as_str() == Some(expected)))
    };
    if !contains("kinds", &binding.kind) {
        return Some("kind-unsupported".to_owned());
    }
    if !contains("units", &binding.unit) {
        return Some("unit-unsupported".to_owned());
    }
    let lifecycle = RESOURCE_OPERATIONS
        .iter()
        .all(|operation| contains("operations", operation));
    (!lifecycle).then(|| "lifecycle-unsupported".to_owned())
}
