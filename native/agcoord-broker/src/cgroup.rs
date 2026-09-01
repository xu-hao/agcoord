use crate::resources::{Binding, Capability, RESOURCE_OPERATIONS, ResourceConfiguration};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::CString;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};

const CPU_PERIOD_USEC: u64 = 100_000;
const EMPTY_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Debug)]
pub struct CgroupError {
    pub code: String,
}

impl CgroupError {
    pub(crate) fn new(code: &str) -> Self {
        Self {
            code: code.to_owned(),
        }
    }
}

pub(crate) type CgroupResult<T> = std::result::Result<T, CgroupError>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Identity {
    device: u64,
    inode: u64,
}

#[derive(Clone, Debug)]
struct Probe {
    available: bool,
    reason: Option<String>,
    controllers: BTreeSet<String>,
}

#[derive(Clone, Debug)]
pub struct CgroupRequest {
    pub run_id: String,
    pub resources: BTreeMap<String, u64>,
    pub bindings: BTreeMap<String, Binding>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Observation {
    pub resource: String,
    pub code: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Measurement {
    pub peak: BTreeMap<String, u64>,
    pub observations: Vec<Observation>,
}

impl CgroupRequest {
    pub fn new(
        run_id: &str,
        resources: &BTreeMap<String, u64>,
        bindings: &BTreeMap<String, Binding>,
    ) -> Self {
        let selected_bindings: BTreeMap<_, _> = resources
            .iter()
            .filter_map(|(name, _units)| {
                bindings
                    .get(name)
                    .filter(|binding| binding.backend.as_deref() == Some("cgroup-v2"))
                    .map(|binding| (name.clone(), binding.clone()))
            })
            .collect();
        let selected_resources = selected_bindings
            .keys()
            .filter_map(|name| resources.get(name).map(|units| (name.clone(), *units)))
            .collect();
        Self {
            run_id: run_id.to_owned(),
            resources: selected_resources,
            bindings: selected_bindings,
        }
    }

    pub fn names(&self) -> Vec<String> {
        self.resources.keys().cloned().collect()
    }
}

#[derive(Clone, Debug)]
struct CgroupMount {
    path: PathBuf,
    options: BTreeSet<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct IoDevice {
    number: String,
    filesystem: String,
}

#[derive(Clone, Debug)]
struct IoMount {
    path: PathBuf,
    root: PathBuf,
    filesystem: String,
    source: PathBuf,
    device: String,
    options: BTreeSet<String>,
}

#[derive(Clone, Debug)]
struct IoSample {
    at: Instant,
    counters: BTreeMap<String, u64>,
    peaks: BTreeMap<String, u64>,
}

#[derive(Clone, Debug)]
struct TmpfsPolicy {
    size_name: String,
    inode_name: String,
    size: u64,
    inodes: u64,
}

#[derive(Clone, Debug)]
pub struct TmpfsSetup {
    pub target: PathBuf,
    pub size: u64,
    pub inodes: u64,
    pub report: PathBuf,
    pub token: String,
    pub emulate: bool,
}

#[derive(Clone, Copy, Debug)]
pub struct TmpfsBaseline {
    pub blocks: u64,
    pub blocks_free: u64,
    pub fragment_size: u64,
    pub files: u64,
    pub files_free: u64,
}

pub(crate) fn sha256_prefix(value: &[u8], bytes: usize) -> String {
    const INITIAL: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
        0x5be0cd19,
    ];
    const ROUND: [u32; 64] = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
        0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
        0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
        0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
        0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
        0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
        0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
        0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
        0xc67178f2,
    ];
    let bit_length = (value.len() as u64).wrapping_mul(8);
    let mut padded = value.to_vec();
    padded.push(0x80);
    while padded.len() % 64 != 56 {
        padded.push(0);
    }
    padded.extend_from_slice(&bit_length.to_be_bytes());
    let mut hash = INITIAL;
    for chunk in padded.chunks_exact(64) {
        let mut schedule = [0_u32; 64];
        for (index, word) in chunk.chunks_exact(4).enumerate() {
            schedule[index] = u32::from_be_bytes(word.try_into().unwrap());
        }
        for index in 16..64 {
            let first = schedule[index - 15].rotate_right(7)
                ^ schedule[index - 15].rotate_right(18)
                ^ (schedule[index - 15] >> 3);
            let second = schedule[index - 2].rotate_right(17)
                ^ schedule[index - 2].rotate_right(19)
                ^ (schedule[index - 2] >> 10);
            schedule[index] = schedule[index - 16]
                .wrapping_add(first)
                .wrapping_add(schedule[index - 7])
                .wrapping_add(second);
        }
        let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut h] = hash;
        for index in 0..64 {
            let choose = (e & f) ^ ((!e) & g);
            let majority = (a & b) ^ (a & c) ^ (b & c);
            let first = h
                .wrapping_add(e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25))
                .wrapping_add(choose)
                .wrapping_add(ROUND[index])
                .wrapping_add(schedule[index]);
            let second = (a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22))
                .wrapping_add(majority);
            h = g;
            g = f;
            f = e;
            e = d.wrapping_add(first);
            d = c;
            c = b;
            b = a;
            a = first.wrapping_add(second);
        }
        for (selected, value) in hash.iter_mut().zip([a, b, c, d, e, f, g, h]) {
            *selected = selected.wrapping_add(value);
        }
    }
    let digest: Vec<u8> = hash.into_iter().flat_map(u32::to_be_bytes).collect();
    digest[..bytes]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn random_hex(bytes: usize) -> CgroupResult<String> {
    let mut value = vec![0_u8; bytes];
    let mut offset = 0;
    while offset < value.len() {
        // SAFETY: the pointer names the unwritten suffix of an owned byte buffer.
        let written = unsafe {
            libc::getrandom(value[offset..].as_mut_ptr().cast(), value.len() - offset, 0)
        };
        if written > 0 {
            offset += written as usize;
            continue;
        }
        if written < 0 && io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
            continue;
        }
        return Err(CgroupError::new("randomness-unavailable"));
    }
    Ok(value.iter().map(|byte| format!("{byte:02x}")).collect())
}

fn identity(path: &Path) -> CgroupResult<Option<Identity>> {
    let details = match fs::symlink_metadata(path) {
        Ok(details) => details,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(CgroupError::new("cgroup-path-unreadable")),
    };
    if details.file_type().is_symlink() || !details.file_type().is_dir() {
        return Err(CgroupError::new("cgroup-path-invalid"));
    }
    Ok(Some(Identity {
        device: details.dev(),
        inode: details.ino(),
    }))
}

fn process_table() -> BTreeMap<u32, (u32, String)> {
    let mut selected = BTreeMap::new();
    let Ok(entries) = fs::read_dir("/proc") else {
        return selected;
    };
    for entry in entries.flatten() {
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|value| value.parse::<u32>().ok())
        else {
            continue;
        };
        let Ok(stat) = fs::read_to_string(entry.path().join("stat")) else {
            continue;
        };
        let Some(closing) = stat.rfind(')') else {
            continue;
        };
        let fields: Vec<_> = stat[closing + 2..].split_whitespace().collect();
        if fields.len() <= 19 || matches!(fields[0], "Z" | "X") {
            continue;
        }
        let Some(parent) = fields[1].parse::<u32>().ok() else {
            continue;
        };
        selected.insert(pid, (parent, fields[19].to_owned()));
    }
    selected
}

#[derive(Debug)]
struct FixtureSystem {
    root: PathBuf,
    controllers: BTreeSet<String>,
    leaf_faults: BTreeMap<String, Option<String>>,
}

impl FixtureSystem {
    fn new(root: &Path) -> CgroupResult<Self> {
        let controllers = ["cpu", "io", "memory", "pids"]
            .into_iter()
            .map(str::to_owned)
            .collect();
        let fault_path = root.join(".agcoord-fixture-leaf-faults.json");
        let leaf_faults = match fs::read_to_string(&fault_path) {
            Ok(raw) => {
                let value: Value = serde_json::from_str(&raw)
                    .map_err(|_| CgroupError::new("fixture-fault-invalid"))?;
                value
                    .as_object()
                    .ok_or_else(|| CgroupError::new("fixture-fault-invalid"))?
                    .iter()
                    .map(|(name, value)| {
                        let path = Path::new(name);
                        if name.is_empty()
                            || path.components().count() != 1
                            || matches!(name.as_str(), "." | "..")
                            || name.starts_with(".agcoord-")
                        {
                            return Err(CgroupError::new("fixture-fault-invalid"));
                        }
                        match value {
                            Value::Null => Ok((name.clone(), None)),
                            Value::String(contents) => Ok((name.clone(), Some(contents.clone()))),
                            _ => Err(CgroupError::new("fixture-fault-invalid")),
                        }
                    })
                    .collect::<CgroupResult<BTreeMap<_, _>>>()?
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => BTreeMap::new(),
            Err(_) => return Err(CgroupError::new("fixture-fault-invalid")),
        };
        let selected = Self {
            root: root.to_owned(),
            controllers,
            leaf_faults,
        };
        selected.initialize_group(root)?;
        Ok(selected)
    }

    fn members_path(path: &Path) -> PathBuf {
        path.join(".agcoord-members.json")
    }

    fn initialize_group(&self, path: &Path) -> CgroupResult<()> {
        fs::create_dir_all(path).map_err(|_| CgroupError::new("create-failed"))?;
        let defaults = [
            ("cgroup.procs", ""),
            ("cgroup.events", "populated 0\n"),
            ("cgroup.kill", ""),
            ("cgroup.controllers", "cpu io memory pids\n"),
            ("cgroup.subtree_control", ""),
            ("cpu.max", "max 100000\n"),
            (
                "cpu.stat",
                "usage_usec 0\nnr_throttled 0\nthrottled_usec 0\n",
            ),
            ("pids.max", "max\n"),
            ("pids.current", "0\n"),
            ("pids.peak", "0\n"),
            ("pids.events", "max 0\n"),
            ("memory.high", "max\n"),
            ("memory.max", "max\n"),
            ("memory.oom.group", "0\n"),
            ("memory.current", "0\n"),
            ("memory.peak", "0\n"),
            (
                "memory.events",
                "high 0\nmax 0\noom 0\noom_kill 0\noom_group_kill 0\n",
            ),
            (
                "memory.pressure",
                "some avg10=0.00 avg60=0.00 avg300=0.00 total=0\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=0\n",
            ),
            ("memory.swap.max", "max\n"),
            ("memory.swap.current", "0\n"),
            ("memory.swap.peak", "0\n"),
            ("memory.swap.events", "high 0\nmax 0\nfail 0\n"),
            ("io.max", ""),
            ("io.weight", "default 100\n"),
            ("io.stat", ""),
        ];
        for (name, value) in defaults {
            let target = path.join(name);
            if !target.exists() {
                fs::write(target, value).map_err(|_| CgroupError::new("create-failed"))?;
            }
        }
        if !Self::members_path(path).exists() {
            fs::write(Self::members_path(path), "{}")
                .map_err(|_| CgroupError::new("create-failed"))?;
        }
        if path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with("run-"))
        {
            for (name, contents) in &self.leaf_faults {
                let target = path.join(name);
                if let Some(contents) = contents {
                    fs::write(target, contents)
                        .map_err(|_| CgroupError::new("fixture-fault-invalid"))?;
                } else if let Err(error) = fs::remove_file(target)
                    && error.kind() != io::ErrorKind::NotFound
                {
                    return Err(CgroupError::new("fixture-fault-invalid"));
                }
            }
        }
        Ok(())
    }

    fn read_members(&self, path: &Path) -> CgroupResult<BTreeMap<u32, String>> {
        let raw = fs::read_to_string(Self::members_path(path))
            .map_err(|_| CgroupError::new("membership-unreadable"))?;
        let stored: BTreeMap<String, String> =
            serde_json::from_str(&raw).map_err(|_| CgroupError::new("membership-invalid"))?;
        stored
            .into_iter()
            .map(|(pid, token)| {
                pid.parse::<u32>()
                    .map(|pid| (pid, token))
                    .map_err(|_| CgroupError::new("membership-invalid"))
            })
            .collect()
    }

    fn write_members(&self, path: &Path, members: &BTreeMap<u32, String>) -> CgroupResult<()> {
        let stored: BTreeMap<_, _> = members
            .iter()
            .map(|(pid, token)| (pid.to_string(), token.clone()))
            .collect();
        fs::write(
            Self::members_path(path),
            serde_json::to_vec(&stored).unwrap(),
        )
        .map_err(|_| CgroupError::new("membership-unreadable"))?;
        fs::write(
            path.join("cgroup.procs"),
            members
                .keys()
                .map(|pid| format!("{pid}\n"))
                .collect::<String>(),
        )
        .map_err(|_| CgroupError::new("membership-unreadable"))?;
        fs::write(
            path.join("cgroup.events"),
            format!("populated {}\n", u8::from(!members.is_empty())),
        )
        .map_err(|_| CgroupError::new("events-unreadable"))?;
        fs::write(path.join("pids.current"), format!("{}\n", members.len()))
            .map_err(|_| CgroupError::new("controller-read-failed"))?;
        Ok(())
    }

    fn members(&self, path: &Path) -> CgroupResult<BTreeSet<u32>> {
        let table = process_table();
        let mut recorded = self.read_members(path)?;
        recorded.retain(|pid, token| {
            table
                .get(pid)
                .is_some_and(|(_parent, observed)| observed == token)
        });
        loop {
            let mut changed = false;
            for (pid, (parent, token)) in &table {
                if !recorded.contains_key(pid) && recorded.contains_key(parent) {
                    recorded.insert(*pid, token.clone());
                    changed = true;
                }
            }
            if !changed {
                break;
            }
        }
        self.write_members(path, &recorded)?;
        Ok(recorded.keys().copied().collect())
    }

    fn attach(&self, path: &Path, pid: u32) -> CgroupResult<()> {
        let table = process_table();
        let token = table
            .get(&pid)
            .map(|(_parent, token)| token.clone())
            .ok_or_else(|| CgroupError::new("attach-failed"))?;
        let mut members = self.read_members(path)?;
        members.insert(pid, token);
        self.write_members(path, &members)
    }

    fn kill(&self, path: &Path) -> CgroupResult<()> {
        let deadline = Instant::now() + EMPTY_TIMEOUT;
        loop {
            let members = self.members(path)?;
            if members.is_empty() {
                return Ok(());
            }
            for pid in members {
                // SAFETY: fixture membership retains the matching Linux start token and
                // targets only the test-owned process identity recorded for this leaf.
                unsafe {
                    libc::kill(pid as i32, libc::SIGKILL);
                }
            }
            if Instant::now() >= deadline {
                return Err(CgroupError::new("leaf-populated"));
            }
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn remove_group(&self, path: &Path) -> CgroupResult<()> {
        if !self.members(path)?.is_empty() {
            return Err(CgroupError::new("leaf-populated"));
        }
        for entry in fs::read_dir(path).map_err(|_| CgroupError::new("cleanup-failed"))? {
            let entry = entry.map_err(|_| CgroupError::new("cleanup-failed"))?;
            if entry
                .file_type()
                .map_err(|_| CgroupError::new("cleanup-failed"))?
                .is_dir()
            {
                return Err(CgroupError::new("leaf-populated"));
            }
            fs::remove_file(entry.path()).map_err(|_| CgroupError::new("cleanup-failed"))?;
        }
        fs::remove_dir(path).map_err(|_| CgroupError::new("cleanup-failed"))
    }

    fn clear_root(&self) -> CgroupResult<()> {
        for entry in fs::read_dir(&self.root).map_err(|_| CgroupError::new("cleanup-failed"))? {
            let entry = entry.map_err(|_| CgroupError::new("cleanup-failed"))?;
            if entry
                .file_type()
                .map_err(|_| CgroupError::new("cleanup-failed"))?
                .is_dir()
            {
                return Err(CgroupError::new("owner-populated"));
            }
            fs::remove_file(entry.path()).map_err(|_| CgroupError::new("cleanup-failed"))?;
        }
        Ok(())
    }

    fn write_control(&self, path: &Path, name: &str, value: &str) -> CgroupResult<()> {
        let rendered = if matches!(name, "io.max" | "io.weight") {
            let key = value.split_whitespace().next().unwrap_or("");
            let previous = fs::read_to_string(path.join(name)).unwrap_or_default();
            let mut lines: Vec<_> = previous
                .lines()
                .filter(|line| line.split_whitespace().next() != Some(key))
                .map(str::to_owned)
                .collect();
            lines.push(value.trim().to_owned());
            format!("{}\n", lines.join("\n"))
        } else {
            format!("{}\n", value.trim_end())
        };
        fs::write(path.join(name), rendered)
            .map_err(|_| CgroupError::new("controller-write-failed"))
    }
}

#[derive(Debug)]
enum CgroupSystem {
    Linux,
    Fixture(FixtureSystem),
}

impl CgroupSystem {
    fn fixture(&self) -> bool {
        matches!(self, Self::Fixture(_))
    }

    fn identity(&self, path: &Path) -> CgroupResult<Option<Identity>> {
        identity(path)
    }

    fn prepare_root(&self, path: &Path) -> CgroupResult<()> {
        if let Self::Fixture(system) = self {
            system.initialize_group(path)?;
        }
        Ok(())
    }

    fn cleanup_root(&self) -> CgroupResult<()> {
        if let Self::Fixture(system) = self {
            system.clear_root()?;
        }
        Ok(())
    }

    fn create_group(&self, parent: &Path, name: &str) -> CgroupResult<Identity> {
        let path = parent.join(name);
        fs::create_dir(&path).map_err(|error| {
            CgroupError::new(if error.kind() == io::ErrorKind::AlreadyExists {
                "path-collision"
            } else {
                "create-failed"
            })
        })?;
        if let Self::Fixture(system) = self {
            system.initialize_group(&path)?;
        }
        self.identity(&path)?
            .ok_or_else(|| CgroupError::new("create-failed"))
    }

    fn enable_controllers(&self, path: &Path, controllers: &BTreeSet<String>) -> CgroupResult<()> {
        if controllers.is_empty() {
            return Ok(());
        }
        let available: BTreeSet<_> = self
            .read_raw(path, "cgroup.controllers")?
            .split_whitespace()
            .map(str::to_owned)
            .collect();
        if !controllers.is_subset(&available) {
            return Err(CgroupError::new("controller-unavailable"));
        }
        let mut enabled: BTreeSet<_> = self
            .read_raw(path, "cgroup.subtree_control")?
            .split_whitespace()
            .map(|value| value.trim_start_matches('+').to_owned())
            .collect();
        let missing: Vec<_> = controllers.difference(&enabled).cloned().collect();
        if !missing.is_empty() {
            if self.fixture() {
                enabled.extend(missing);
                fs::write(
                    path.join("cgroup.subtree_control"),
                    enabled
                        .iter()
                        .map(|name| format!("{name}\n"))
                        .collect::<String>(),
                )
                .map_err(|_| CgroupError::new("controller-enable-failed"))?;
            } else {
                write_kernel_file(
                    &path.join("cgroup.subtree_control"),
                    &(missing
                        .iter()
                        .map(|name| format!("+{name}"))
                        .collect::<Vec<_>>()
                        .join(" ")
                        + "\n"),
                )
                .map_err(|_| CgroupError::new("controller-enable-failed"))?;
            }
        }
        let observed: BTreeSet<_> = self
            .read_raw(path, "cgroup.subtree_control")?
            .split_whitespace()
            .map(|value| value.trim_start_matches('+').to_owned())
            .collect();
        if !controllers.is_subset(&observed) {
            return Err(CgroupError::new("controller-enable-unverified"));
        }
        Ok(())
    }

    fn write(&self, path: &Path, name: &str, value: &str) -> CgroupResult<()> {
        if let Self::Fixture(system) = self {
            system.write_control(path, name, value)
        } else {
            write_kernel_file(&path.join(name), &format!("{}\n", value.trim_end()))
                .map_err(|_| CgroupError::new("controller-write-failed"))
        }
    }

    fn read_raw(&self, path: &Path, name: &str) -> CgroupResult<String> {
        fs::read_to_string(path.join(name)).map_err(|error| {
            CgroupError::new(if error.kind() == io::ErrorKind::NotFound {
                "controller-file-missing"
            } else {
                "controller-read-failed"
            })
        })
    }

    fn attach(&self, path: &Path, pid: u32) -> CgroupResult<()> {
        if let Self::Fixture(system) = self {
            system.attach(path, pid)
        } else {
            write_kernel_file(&path.join("cgroup.procs"), &format!("{pid}\n"))
                .map_err(|_| CgroupError::new("attach-failed"))
        }
    }

    fn members(&self, path: &Path) -> CgroupResult<BTreeSet<u32>> {
        if let Self::Fixture(system) = self {
            return system.members(path);
        }
        self.read_raw(path, "cgroup.procs")?
            .lines()
            .filter(|line| !line.is_empty())
            .map(|line| {
                line.parse::<u32>()
                    .map_err(|_| CgroupError::new("membership-invalid"))
            })
            .collect()
    }

    fn populated(&self, path: &Path) -> CgroupResult<bool> {
        if let Self::Fixture(system) = self {
            return Ok(!system.members(path)?.is_empty());
        }
        let events = self.read_raw(path, "cgroup.events")?;
        events
            .lines()
            .filter_map(|line| line.split_once(' '))
            .find(|(name, _value)| *name == "populated")
            .map(|(_name, value)| value == "1")
            .ok_or_else(|| CgroupError::new("events-invalid"))
    }

    fn kill(&self, path: &Path) -> CgroupResult<()> {
        if let Self::Fixture(system) = self {
            return system.kill(path);
        }
        write_kernel_file(&path.join("cgroup.kill"), "1\n")
            .map_err(|_| CgroupError::new("kill-failed"))
    }

    fn remove_group(&self, path: &Path) -> CgroupResult<()> {
        if let Self::Fixture(system) = self {
            system.remove_group(path)
        } else {
            fs::remove_dir(path).map_err(|_| CgroupError::new("cleanup-failed"))
        }
    }

    fn swap_total_bytes(&self) -> CgroupResult<u64> {
        if self.fixture() {
            return Ok(1024 * 1024 * 1024);
        }
        let raw = fs::read_to_string("/proc/meminfo")
            .map_err(|_| CgroupError::new("swap-state-unavailable"))?;
        let matches: Vec<_> = raw
            .lines()
            .filter(|line| line.starts_with("SwapTotal:"))
            .collect();
        if matches.len() != 1 {
            return Err(CgroupError::new("swap-state-unavailable"));
        }
        let fields: Vec<_> = matches[0].split_whitespace().collect();
        if fields.len() != 3
            || fields[0] != "SwapTotal:"
            || fields[2] != "kB"
            || !fields[1].bytes().all(|byte| byte.is_ascii_digit())
        {
            return Err(CgroupError::new("swap-state-unavailable"));
        }
        fields[1]
            .parse::<u64>()
            .ok()
            .and_then(|value| value.checked_mul(1024))
            .ok_or_else(|| CgroupError::new("swap-state-unavailable"))
    }
}

fn write_kernel_file(path: &Path, value: &str) -> io::Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
        .open(path)?;
    file.write_all(value.as_bytes())
}

fn decode_mount_path(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut selected = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'\\'
            && index + 3 < bytes.len()
            && bytes[index + 1..=index + 3]
                .iter()
                .all(|byte| matches!(byte, b'0'..=b'7'))
        {
            let decoded = (bytes[index + 1] - b'0') * 64
                + (bytes[index + 2] - b'0') * 8
                + (bytes[index + 3] - b'0');
            selected.push(decoded);
            index += 4;
        } else {
            selected.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8_lossy(&selected).into_owned()
}

fn cgroup_mounts() -> std::io::Result<Vec<CgroupMount>> {
    let mountinfo = fs::read_to_string("/proc/self/mountinfo")?;
    let mut mounts = Vec::new();
    for line in mountinfo.lines() {
        let Some((left, right)) = line.split_once(" - ") else {
            continue;
        };
        let fields: Vec<_> = left.split_whitespace().collect();
        let right_fields: Vec<_> = right.split_whitespace().collect();
        if fields.len() < 6 || right_fields.len() < 3 || right_fields[0] != "cgroup2" {
            continue;
        }
        let path = PathBuf::from(decode_mount_path(fields[4]));
        if !path.is_absolute() {
            continue;
        }
        let options = fields[5]
            .split(',')
            .chain(right_fields[2].split(','))
            .map(str::to_owned)
            .collect();
        mounts.push(CgroupMount { path, options });
    }
    Ok(mounts)
}

fn device_number_valid(value: &str) -> bool {
    let Some((major, minor)) = value.split_once(':') else {
        return false;
    };
    !major.is_empty()
        && !minor.is_empty()
        && major.bytes().all(|byte| byte.is_ascii_digit())
        && minor.bytes().all(|byte| byte.is_ascii_digit())
        && !minor.contains(':')
}

fn supported_io_filesystem(value: &str) -> bool {
    matches!(value, "ext2" | "ext4" | "f2fs" | "xfs")
}

fn linux_device_parts(device: u64) -> (u64, u64) {
    let major = ((device >> 8) & 0xfff) | ((device >> 32) & 0xffff_f000);
    let minor = (device & 0xff) | ((device >> 12) & 0xffff_ff00);
    (major, minor)
}

fn io_mounts() -> CgroupResult<Vec<IoMount>> {
    let raw = fs::read_to_string("/proc/self/mountinfo")
        .map_err(|_| CgroupError::new("io-mountinfo-unavailable"))?;
    let mut selected = Vec::new();
    for line in raw.lines() {
        let Some((left, right)) = line.split_once(" - ") else {
            continue;
        };
        let fields: Vec<_> = left.split_whitespace().collect();
        let right: Vec<_> = right.split_whitespace().collect();
        if fields.len() < 6 || right.len() < 3 || !device_number_valid(fields[2]) {
            continue;
        }
        let path = PathBuf::from(decode_mount_path(fields[4]));
        let root = PathBuf::from(decode_mount_path(fields[3]));
        if !path.is_absolute() {
            continue;
        }
        selected.push(IoMount {
            path,
            root,
            filesystem: right[0].to_owned(),
            source: PathBuf::from(decode_mount_path(right[1])),
            device: fields[2].to_owned(),
            options: fields[5]
                .split(',')
                .chain(right[2].split(','))
                .map(str::to_owned)
                .collect(),
        });
    }
    Ok(selected)
}

fn covering_cgroup_mounts(mut mounts: Vec<CgroupMount>) -> Vec<CgroupMount> {
    mounts.sort_by_key(|mount| mount.path.components().count());
    let mut selected: Vec<CgroupMount> = Vec::new();
    for mount in mounts {
        if selected
            .iter()
            .any(|parent| mount.path.starts_with(&parent.path))
        {
            continue;
        }
        selected.push(mount);
    }
    selected
}

fn namespace_mapping(path: &Path, value: &str) -> CgroupResult<()> {
    write_kernel_file(path, value).map_err(|_| CgroupError::new("namespace-mapping-failed"))
}

pub fn isolate_current_cgroup() -> CgroupResult<()> {
    let mounts = cgroup_mounts()
        .map(covering_cgroup_mounts)
        .map_err(|_| CgroupError::new("mountinfo-unreadable"))?;
    if mounts.is_empty()
        || mounts
            .iter()
            .any(|mount| !mount.options.contains("nsdelegate"))
    {
        return Err(CgroupError::new("namespace-delegation-unavailable"));
    }
    let uid = unsafe { libc::getuid() };
    let gid = unsafe { libc::getgid() };
    // SAFETY: unshare receives only the documented namespace bit mask.
    if unsafe { libc::unshare(libc::CLONE_NEWUSER | libc::CLONE_NEWCGROUP | libc::CLONE_NEWNS) }
        != 0
    {
        return Err(CgroupError::new("namespace-isolation-unavailable"));
    }
    if Path::new("/proc/self/setgroups").exists() {
        namespace_mapping(Path::new("/proc/self/setgroups"), "deny\n")?;
    }
    namespace_mapping(Path::new("/proc/self/uid_map"), &format!("{uid} {uid} 1\n"))?;
    namespace_mapping(Path::new("/proc/self/gid_map"), &format!("{gid} {gid} 1\n"))?;
    let root = CString::new("/").unwrap();
    // SAFETY: all string arguments are live NUL-terminated buffers; null source,
    // filesystem and data are valid for a propagation-only mount operation.
    if unsafe {
        libc::mount(
            std::ptr::null(),
            root.as_ptr(),
            std::ptr::null(),
            (libc::MS_REC | libc::MS_PRIVATE) as libc::c_ulong,
            std::ptr::null(),
        )
    } != 0
    {
        eprintln!(
            "native cgroup namespace propagation mount failed: {}",
            io::Error::last_os_error()
        );
        return Err(CgroupError::new("namespace-mount-failed"));
    }
    let source = CString::new("none").unwrap();
    let filesystem = CString::new("cgroup2").unwrap();
    for mount in &mounts {
        let target = CString::new(mount.path.as_os_str().as_bytes())
            .map_err(|_| CgroupError::new("namespace-mount-failed"))?;
        // SAFETY: the three path buffers are NUL terminated and live through mount(2).
        if unsafe {
            libc::mount(
                source.as_ptr(),
                target.as_ptr(),
                filesystem.as_ptr(),
                (libc::MS_NOSUID | libc::MS_NODEV | libc::MS_NOEXEC) as libc::c_ulong,
                std::ptr::null(),
            )
        } != 0
        {
            eprintln!(
                "native cgroup2 namespace mount at {} failed: {}",
                mount.path.display(),
                io::Error::last_os_error()
            );
            return Err(CgroupError::new("namespace-mount-failed"));
        }
    }
    let cgroups = fs::read_to_string("/proc/self/cgroup")
        .map_err(|_| CgroupError::new("namespace-verification-failed"))?;
    if !cgroups.lines().any(|line| line == "0::/")
        || mounts
            .iter()
            .any(|mount| !mount.path.join("cgroup.events").is_file())
    {
        return Err(CgroupError::new("namespace-verification-failed"));
    }
    for mount in &mounts {
        match write_kernel_file(&mount.path.join("cgroup.kill"), "1\n") {
            Ok(()) => return Err(CgroupError::new("controller-files-exposed")),
            Err(error)
                if matches!(
                    error.raw_os_error(),
                    Some(libc::EACCES | libc::EPERM | libc::EROFS)
                ) => {}
            Err(_) => return Err(CgroupError::new("namespace-verification-failed")),
        }
    }
    Ok(())
}

fn current_statvfs(target: &Path) -> CgroupResult<TmpfsBaseline> {
    let target = CString::new(target.as_os_str().as_bytes())
        .map_err(|_| CgroupError::new("tmpfs-target-invalid"))?;
    // SAFETY: the structure is plain old data and statvfs initializes it on success.
    let mut details: libc::statvfs = unsafe { std::mem::zeroed() };
    // SAFETY: target and the writable result structure live through statvfs(3).
    if unsafe { libc::statvfs(target.as_ptr(), &mut details) } != 0 {
        return Err(CgroupError::new("tmpfs-mount-unverified"));
    }
    Ok(TmpfsBaseline {
        blocks: details.f_blocks,
        blocks_free: details.f_bfree,
        fragment_size: details.f_frsize,
        files: details.f_files,
        files_free: details.f_ffree,
    })
}

fn fixture_directory_usage(target: &Path) -> CgroupResult<(u64, u64)> {
    let mut bytes = 0_u64;
    let mut inodes = 0_u64;
    for entry in fs::read_dir(target).map_err(|_| CgroupError::new("tmpfs-stat-invalid"))? {
        let entry = entry.map_err(|_| CgroupError::new("tmpfs-stat-invalid"))?;
        let details = fs::symlink_metadata(entry.path())
            .map_err(|_| CgroupError::new("tmpfs-stat-invalid"))?;
        inodes = inodes
            .checked_add(1)
            .ok_or_else(|| CgroupError::new("tmpfs-stat-invalid"))?;
        if details.file_type().is_dir() {
            let (nested_bytes, nested_inodes) = fixture_directory_usage(&entry.path())?;
            bytes = bytes
                .checked_add(nested_bytes)
                .ok_or_else(|| CgroupError::new("tmpfs-stat-invalid"))?;
            inodes = inodes
                .checked_add(nested_inodes)
                .ok_or_else(|| CgroupError::new("tmpfs-stat-invalid"))?;
        } else if details.file_type().is_file() {
            bytes = bytes
                .checked_add(details.len())
                .ok_or_else(|| CgroupError::new("tmpfs-stat-invalid"))?;
        }
    }
    Ok((bytes, inodes))
}

pub fn mount_current_tmpfs(setup: &TmpfsSetup) -> CgroupResult<TmpfsBaseline> {
    if setup.emulate {
        return Ok(TmpfsBaseline {
            blocks: setup.size,
            blocks_free: setup.size,
            fragment_size: 1,
            files: setup.inodes,
            files_free: setup.inodes,
        });
    }
    let details = fs::symlink_metadata(&setup.target)
        .map_err(|_| CgroupError::new("tmpfs-target-invalid"))?;
    if !setup.target.is_absolute()
        || setup.target.canonicalize().ok().as_deref() != Some(&setup.target)
        || details.file_type().is_symlink()
        || !details.file_type().is_dir()
        || details.uid() != unsafe { libc::getuid() }
        || details.mode() & 0o077 != 0
    {
        return Err(CgroupError::new("tmpfs-target-invalid"));
    }
    let source = CString::new("agcoord-tmpfs").unwrap();
    let target = CString::new(setup.target.as_os_str().as_bytes())
        .map_err(|_| CgroupError::new("tmpfs-target-invalid"))?;
    let filesystem = CString::new("tmpfs").unwrap();
    let options = CString::new(format!(
        "size={},nr_inodes={},mode=700,uid={},gid={}",
        setup.size,
        setup.inodes,
        unsafe { libc::getuid() },
        unsafe { libc::getgid() },
    ))
    .unwrap();
    // SAFETY: all buffers are NUL terminated and live through mount(2).
    if unsafe {
        libc::mount(
            source.as_ptr(),
            target.as_ptr(),
            filesystem.as_ptr(),
            (libc::MS_NOSUID | libc::MS_NODEV | libc::MS_NOEXEC) as libc::c_ulong,
            options.as_ptr().cast(),
        )
    } != 0
    {
        return Err(CgroupError::new("tmpfs-mount-unavailable"));
    }
    let verification = (|| {
        let mountinfo = fs::read_to_string("/proc/self/mountinfo")
            .map_err(|_| CgroupError::new("tmpfs-mount-unverified"))?;
        let mounted = mountinfo
            .lines()
            .filter_map(|line| line.split_once(" - "))
            .filter(|(left, _right)| {
                left.split_whitespace().nth(4).is_some_and(|path| {
                    let decoded = decode_mount_path(path);
                    Path::new(&decoded) == setup.target
                })
            })
            .map(|(left, right)| {
                let filesystem = right.split_whitespace().next().unwrap_or("");
                let options: BTreeSet<_> = left
                    .split_whitespace()
                    .nth(5)
                    .unwrap_or("")
                    .split(',')
                    .chain(right.split_whitespace().nth(2).unwrap_or("").split(','))
                    .collect();
                (filesystem.to_owned(), options)
            })
            .collect::<Vec<_>>();
        if mounted.len() != 1
            || mounted[0].0 != "tmpfs"
            || !["nodev", "noexec", "nosuid"]
                .iter()
                .all(|option| mounted[0].1.contains(option))
        {
            return Err(CgroupError::new("tmpfs-mount-unverified"));
        }
        let details =
            fs::metadata(&setup.target).map_err(|_| CgroupError::new("tmpfs-mount-unverified"))?;
        if !details.is_dir()
            || details.uid() != unsafe { libc::getuid() }
            || details.gid() != unsafe { libc::getgid() }
            || details.mode() & 0o777 != 0o700
        {
            return Err(CgroupError::new("tmpfs-mount-unverified"));
        }
        let usage = current_statvfs(&setup.target)?;
        if u128::from(usage.blocks) * u128::from(usage.fragment_size) > u128::from(setup.size) {
            return Err(CgroupError::new("tmpfs-size-unverified"));
        }
        if usage.files > setup.inodes {
            return Err(CgroupError::new("tmpfs-inodes-unverified"));
        }
        Ok(usage)
    })();
    if verification.is_err() {
        // SAFETY: target is the exact private mount created above.
        let _ = unsafe { libc::umount2(target.as_ptr(), libc::MNT_DETACH) };
    }
    verification
}

pub fn unmount_current_tmpfs(setup: &TmpfsSetup) -> CgroupResult<()> {
    if setup.emulate {
        return Ok(());
    }
    let target = CString::new(setup.target.as_os_str().as_bytes())
        .map_err(|_| CgroupError::new("tmpfs-unmount-failed"))?;
    // SAFETY: target is the exact private mount from the validated setup.
    if unsafe { libc::umount2(target.as_ptr(), libc::MNT_DETACH) } != 0 {
        return Err(CgroupError::new("tmpfs-unmount-failed"));
    }
    Ok(())
}

pub fn tmpfs_stat(setup: &TmpfsSetup) -> CgroupResult<TmpfsBaseline> {
    current_statvfs(&setup.target)
}

fn controller_name_valid(value: &str) -> bool {
    let bytes = value.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 32
        && bytes[0].is_ascii_lowercase()
        && bytes[1..].iter().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-')
        })
}

fn probe_pipe() -> io::Result<(libc::c_int, libc::c_int)> {
    let mut descriptors = [-1; 2];
    // SAFETY: descriptors points to two writable integers and the flags are documented.
    if unsafe { libc::pipe2(descriptors.as_mut_ptr(), libc::O_CLOEXEC) } == 0 {
        Ok((descriptors[0], descriptors[1]))
    } else {
        Err(io::Error::last_os_error())
    }
}

fn close_probe_descriptor(descriptor: libc::c_int) {
    if descriptor >= 0 {
        // SAFETY: the descriptor is one exact probe-pipe endpoint owned by this process.
        let _ = unsafe { libc::close(descriptor) };
    }
}

fn wait_probe_child(child: libc::pid_t) -> Option<libc::c_int> {
    let mut status = 0;
    loop {
        // SAFETY: child is the exact fork result and status is writable.
        let observed = unsafe { libc::waitpid(child, &mut status, 0) };
        if observed == child {
            return Some(status);
        }
        if observed < 0 && io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
            continue;
        }
        return None;
    }
}

fn probe_isolation(probe_path: &Path) -> Option<String> {
    let Ok((release_read, release_write)) = probe_pipe() else {
        return Some("namespace-isolation-unavailable".to_owned());
    };
    let Ok((result_read, result_write)) = probe_pipe() else {
        close_probe_descriptor(release_read);
        close_probe_descriptor(release_write);
        return Some("namespace-isolation-unavailable".to_owned());
    };
    // SAFETY: the broker is single-threaded during capability probing and the child
    // performs only the bounded probe below before calling _exit(2).
    let child = unsafe { libc::fork() };
    if child < 0 {
        for descriptor in [release_read, release_write, result_read, result_write] {
            close_probe_descriptor(descriptor);
        }
        return Some("namespace-isolation-unavailable".to_owned());
    }
    if child == 0 {
        close_probe_descriptor(release_write);
        close_probe_descriptor(result_read);
        let mut release = [0_u8; 1];
        // SAFETY: release is writable for one byte and release_read is the child endpoint.
        let released = unsafe { libc::read(release_read, release.as_mut_ptr().cast(), 1) } == 1
            && release[0] == b'1';
        let code = if !released {
            "namespace-isolation-failed".to_owned()
        } else {
            isolate_current_cgroup()
                .err()
                .map_or_else(|| "ok".to_owned(), |error| error.code)
        };
        // SAFETY: the ASCII code buffer is valid for its exact length.
        let _ = unsafe { libc::write(result_write, code.as_ptr().cast(), code.len()) };
        close_probe_descriptor(release_read);
        close_probe_descriptor(result_write);
        // SAFETY: a fork child must not unwind through copied broker state.
        unsafe { libc::_exit(i32::from(code != "ok")) }
    }

    close_probe_descriptor(release_read);
    close_probe_descriptor(result_write);
    let mut waited = false;
    let reason = (|| {
        let system = CgroupSystem::Linux;
        if system.attach(probe_path, child as u32).is_err() {
            return Some("attach-failed".to_owned());
        }
        match system.members(probe_path) {
            Ok(members) if members.contains(&(child as u32)) => {}
            _ => return Some("attach-unverified".to_owned()),
        }
        // SAFETY: release_write is the parent endpoint and points at one live child.
        if unsafe { libc::write(release_write, b"1".as_ptr().cast(), 1) } != 1 {
            return Some("namespace-isolation-unavailable".to_owned());
        }
        close_probe_descriptor(release_write);
        let mut poll = libc::pollfd {
            fd: result_read,
            events: libc::POLLIN | libc::POLLHUP,
            revents: 0,
        };
        // SAFETY: poll points to one initialized entry for the bounded timeout.
        let ready = unsafe { libc::poll(&mut poll, 1, 5_000) };
        if ready == 0 {
            return Some("namespace-isolation-timeout".to_owned());
        }
        if ready < 0 {
            return Some("namespace-isolation-unavailable".to_owned());
        }
        let mut payload = [0_u8; 128];
        // SAFETY: payload is writable for its full length and result_read is the endpoint.
        let length = unsafe { libc::read(result_read, payload.as_mut_ptr().cast(), payload.len()) };
        if length <= 0 {
            if let Some(status) = wait_probe_child(child) {
                waited = true;
                if libc::WIFSIGNALED(status) {
                    return Some("controller-files-exposed".to_owned());
                }
            }
            return Some("namespace-isolation-failed".to_owned());
        }
        let Some(status) = wait_probe_child(child) else {
            return Some("namespace-isolation-failed".to_owned());
        };
        waited = true;
        if libc::WIFSIGNALED(status) {
            return Some("controller-files-exposed".to_owned());
        }
        let code = std::str::from_utf8(&payload[..length as usize]).unwrap_or("");
        if libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0 && code == "ok" {
            None
        } else if crate::resources::code_valid(code) {
            Some(code.to_owned())
        } else {
            Some("namespace-isolation-failed".to_owned())
        }
    })();
    close_probe_descriptor(release_write);
    close_probe_descriptor(result_read);
    if !waited {
        // SAFETY: child is the unreaped exact probe child.
        let _ = unsafe { libc::kill(child, libc::SIGKILL) };
        let _ = wait_probe_child(child);
    }
    reason
}

fn basic_probe(root: Option<&Path>) -> (Option<String>, BTreeSet<String>) {
    let Some(root) = root else {
        return (Some("delegation-unconfigured".to_owned()), BTreeSet::new());
    };
    let details = match fs::symlink_metadata(root) {
        Ok(details) => details,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return (Some("root-missing".to_owned()), BTreeSet::new());
        }
        Err(_) => return (Some("root-unreadable".to_owned()), BTreeSet::new()),
    };
    if details.file_type().is_symlink() || !details.file_type().is_dir() {
        return (Some("root-invalid".to_owned()), BTreeSet::new());
    }
    if root.canonicalize().ok().as_deref() != Some(root) {
        return (Some("root-invalid".to_owned()), BTreeSet::new());
    }
    let mounts = match cgroup_mounts() {
        Ok(mounts) => mounts,
        Err(_) => return (Some("mountinfo-unreadable".to_owned()), BTreeSet::new()),
    };
    let mount = mounts
        .iter()
        .filter(|mount| root.starts_with(&mount.path))
        .max_by_key(|mount| mount.path.components().count());
    let Some(mount) = mount else {
        return (Some("not-cgroup-v2".to_owned()), BTreeSet::new());
    };
    if mount.options.contains("ro") {
        return (Some("delegation-read-only".to_owned()), BTreeSet::new());
    }
    if !mount.options.contains("nsdelegate") {
        return (
            Some("namespace-delegation-unavailable".to_owned()),
            BTreeSet::new(),
        );
    }
    if !["cgroup.procs", "cgroup.events"]
        .iter()
        .all(|name| root.join(name).is_file())
    {
        return (Some("delegation-invalid".to_owned()), BTreeSet::new());
    }
    let controllers = match fs::read_to_string(root.join("cgroup.controllers")) {
        Ok(value) => value
            .split_whitespace()
            .map(str::to_owned)
            .collect::<BTreeSet<_>>(),
        Err(_) => return (Some("controllers-unreadable".to_owned()), BTreeSet::new()),
    };
    if controllers.iter().any(|name| !controller_name_valid(name)) {
        return (Some("controllers-invalid".to_owned()), BTreeSet::new());
    }
    let probe_name = match random_hex(6) {
        Ok(token) => format!(".agcoord-probe-{token}"),
        Err(error) => return (Some(error.code), BTreeSet::new()),
    };
    let probe_path = root.join(probe_name);
    let mut created = false;
    let mut reason = match fs::create_dir(&probe_path) {
        Ok(()) => {
            created = true;
            if !["cgroup.procs", "cgroup.events"]
                .iter()
                .all(|name| probe_path.join(name).is_file())
            {
                Some("delegation-invalid".to_owned())
            } else if !probe_path.join("cgroup.kill").is_file() {
                Some("kill-unsupported".to_owned())
            } else {
                probe_isolation(&probe_path)
            }
        }
        Err(error) if error.kind() == io::ErrorKind::PermissionDenied => {
            Some("delegation-undelegated".to_owned())
        }
        Err(error) if error.raw_os_error() == Some(libc::EROFS) => {
            Some("delegation-read-only".to_owned())
        }
        Err(_) => Some("delegation-unavailable".to_owned()),
    };
    if created {
        if reason.is_none() {
            let system = CgroupSystem::Linux;
            if system.kill(&probe_path).is_err() || system.populated(&probe_path).unwrap_or(true) {
                reason = Some("kill-failed".to_owned());
            }
        }
        if fs::remove_dir(&probe_path).is_err() {
            reason = Some("probe-cleanup-failed".to_owned());
        }
    }
    if reason.is_some() {
        (reason, BTreeSet::new())
    } else {
        (None, controllers)
    }
}

fn capability_for_probe(result: Probe) -> Capability {
    if !result.available {
        return Capability::unavailable(result.reason.as_deref().unwrap_or("probe-invalid"));
    }
    let controllers = result.controllers;
    let mut kinds = BTreeSet::from(["generic".to_owned()]);
    let mut units = BTreeSet::from(["admission-unit".to_owned()]);
    if controllers.contains("cpu") {
        kinds.insert("cpu".to_owned());
        units.insert("logical-cpu".to_owned());
    }
    if controllers.contains("pids") {
        kinds.insert("processes".to_owned());
        units.insert("processes".to_owned());
    }
    if controllers.contains("memory") {
        kinds.extend(
            ["inodes", "memory", "memory-high", "swap", "tmpfs"]
                .into_iter()
                .map(str::to_owned),
        );
        units.extend(["bytes", "inodes"].into_iter().map(str::to_owned));
    }
    if controllers.contains("io") {
        kinds.extend(
            ["io-bandwidth", "io-operations", "io-weight"]
                .into_iter()
                .map(str::to_owned),
        );
        units.extend(
            [
                "bytes-per-second",
                "operations-per-second",
                "read-bytes-per-second",
                "read-operations-per-second",
                "weight",
                "write-bytes-per-second",
                "write-operations-per-second",
            ]
            .into_iter()
            .map(str::to_owned),
        );
    }
    Capability {
        available: true,
        kinds,
        units,
        operations: RESOURCE_OPERATIONS.into_iter().map(str::to_owned).collect(),
        reason: None,
    }
}

pub struct CgroupBackend {
    root: Option<PathBuf>,
    metadata_dir: PathBuf,
    owner_name: String,
    system: CgroupSystem,
    probe: Option<Probe>,
    io_paths: Vec<PathBuf>,
    cpu_samples: BTreeMap<String, (Instant, u64, u64)>,
    pids_peaks: BTreeMap<String, u64>,
    memory_peaks: BTreeMap<String, u64>,
    swap_peaks: BTreeMap<String, u64>,
    io_samples: BTreeMap<String, IoSample>,
}

impl CgroupBackend {
    pub fn new(
        configuration: &ResourceConfiguration,
        state_dir: &Path,
        fixture: Option<&Path>,
    ) -> CgroupResult<Self> {
        let system = if let Some(fixture) = fixture {
            if configuration.cgroup_root.as_deref() != Some(fixture) {
                return Err(CgroupError::new("fixture-root-mismatch"));
            }
            CgroupSystem::Fixture(FixtureSystem::new(fixture)?)
        } else {
            CgroupSystem::Linux
        };
        let state_dir =
            std::path::absolute(state_dir).map_err(|_| CgroupError::new("metadata-invalid"))?;
        let owner_hash = sha256_prefix(
            format!("{}:{}", unsafe { libc::geteuid() }, state_dir.display()).as_bytes(),
            8,
        );
        Ok(Self {
            root: configuration.cgroup_root.clone(),
            metadata_dir: state_dir.join("cgroup-v2"),
            owner_name: format!("agcoord-u{}-{owner_hash}", unsafe { libc::geteuid() }),
            system,
            probe: None,
            io_paths: configuration.cgroup_io_paths.clone(),
            cpu_samples: BTreeMap::new(),
            pids_peaks: BTreeMap::new(),
            memory_peaks: BTreeMap::new(),
            swap_peaks: BTreeMap::new(),
            io_samples: BTreeMap::new(),
        })
    }

    pub fn fixture(&self) -> bool {
        self.system.fixture()
    }

    fn probe_result(&mut self) -> Probe {
        if let Some(result) = &self.probe {
            return result.clone();
        }
        let result = match &self.system {
            CgroupSystem::Fixture(system) => Probe {
                available: true,
                reason: None,
                controllers: system.controllers.clone(),
            },
            CgroupSystem::Linux => {
                let (reason, controllers) = basic_probe(self.root.as_deref());
                Probe {
                    available: reason.is_none(),
                    reason,
                    controllers,
                }
            }
        };
        self.probe = Some(result.clone());
        result
    }

    pub fn capability(&mut self) -> Capability {
        capability_for_probe(self.probe_result())
    }

    fn require_available(&mut self) -> CgroupResult<()> {
        let result = self.probe_result();
        if result.available {
            Ok(())
        } else {
            Err(CgroupError::new(
                result.reason.as_deref().unwrap_or("probe-invalid"),
            ))
        }
    }

    fn prepare_metadata(&self) -> CgroupResult<()> {
        fs::create_dir_all(&self.metadata_dir).map_err(|_| CgroupError::new("metadata-invalid"))?;
        let details = fs::symlink_metadata(&self.metadata_dir)
            .map_err(|_| CgroupError::new("metadata-invalid"))?;
        if details.file_type().is_symlink()
            || !details.file_type().is_dir()
            || details.uid() != unsafe { libc::geteuid() }
        {
            return Err(CgroupError::new("metadata-invalid"));
        }
        if details.mode() & 0o077 != 0 {
            fs::set_permissions(&self.metadata_dir, fs::Permissions::from_mode(0o700))
                .map_err(|_| CgroupError::new("metadata-invalid"))?;
        }
        Ok(())
    }

    fn write_json(&self, path: &Path, value: &Value) -> CgroupResult<()> {
        let suffix = random_hex(8)?;
        let temporary = path.with_file_name(format!(
            ".{}.{}.tmp",
            path.file_name().unwrap().to_string_lossy(),
            suffix
        ));
        let result = (|| {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
                .open(&temporary)
                .map_err(|_| CgroupError::new("metadata-invalid"))?;
            let payload =
                serde_json::to_vec(value).map_err(|_| CgroupError::new("metadata-invalid"))?;
            file.write_all(&payload)
                .and_then(|()| file.sync_all())
                .map_err(|_| CgroupError::new("metadata-invalid"))?;
            fs::rename(&temporary, path).map_err(|_| CgroupError::new("metadata-invalid"))?;
            File::open(path.parent().unwrap())
                .and_then(|directory| directory.sync_all())
                .map_err(|_| CgroupError::new("metadata-invalid"))?;
            Ok(())
        })();
        let _ = fs::remove_file(&temporary);
        result
    }

    fn read_json(&self, path: &Path) -> CgroupResult<Value> {
        let details = fs::symlink_metadata(path).map_err(|error| {
            CgroupError::new(if error.kind() == io::ErrorKind::NotFound {
                "metadata-missing"
            } else {
                "metadata-invalid"
            })
        })?;
        if details.file_type().is_symlink()
            || !details.file_type().is_file()
            || details.uid() != unsafe { libc::geteuid() }
            || details.len() > 64 * 1024
        {
            return Err(CgroupError::new("metadata-invalid"));
        }
        serde_json::from_slice(&fs::read(path).map_err(|_| CgroupError::new("metadata-invalid"))?)
            .map_err(|_| CgroupError::new("metadata-invalid"))
    }

    fn owner_record_path(&self) -> PathBuf {
        self.metadata_dir.join("owner.json")
    }

    fn manifest_path(&self, run_id: &str) -> PathBuf {
        self.metadata_dir
            .join(format!("run-{}.json", sha256_prefix(run_id.as_bytes(), 16)))
    }

    fn tmpfs_report_path(&self, run_id: &str) -> PathBuf {
        self.metadata_dir.join(format!(
            "tmpfs-{}.json",
            sha256_prefix(run_id.as_bytes(), 16)
        ))
    }

    fn tmpfs_target_path(&self, run_id: &str, handle: &Value) -> CgroupResult<PathBuf> {
        let token = handle
            .get("token")
            .and_then(Value::as_str)
            .ok_or_else(|| CgroupError::new("handle-invalid"))?;
        if token.len() != 32
            || !token
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(CgroupError::new("handle-invalid"));
        }
        Ok(self.metadata_dir.join(format!(
            "scratch-{}-{}",
            sha256_prefix(run_id.as_bytes(), 16),
            &token[..12]
        )))
    }

    fn owner_record(&self, root_identity: Identity, owner_identity: Identity) -> Value {
        json!({
            "version": 1,
            "root": self.root.as_ref().unwrap(),
            "root_device": root_identity.device,
            "root_inode": root_identity.inode,
            "owner": self.owner_name,
            "owner_device": owner_identity.device,
            "owner_inode": owner_identity.inode,
        })
    }

    fn value_u64(value: &Value, name: &str) -> CgroupResult<u64> {
        value
            .get(name)
            .and_then(Value::as_u64)
            .ok_or_else(|| CgroupError::new("metadata-invalid"))
    }

    fn validate_owner_record(&self, value: &Value) -> CgroupResult<(PathBuf, Option<Identity>)> {
        let object = value
            .as_object()
            .ok_or_else(|| CgroupError::new("owner-metadata-invalid"))?;
        let expected: BTreeSet<_> = [
            "version",
            "root",
            "root_device",
            "root_inode",
            "owner",
            "owner_device",
            "owner_inode",
        ]
        .into_iter()
        .collect();
        if object.keys().map(String::as_str).collect::<BTreeSet<_>>() != expected
            || value.get("version").and_then(Value::as_u64) != Some(1)
            || value.get("root").and_then(Value::as_str)
                != self.root.as_ref().and_then(|path| path.to_str())
            || value.get("owner").and_then(Value::as_str) != Some(&self.owner_name)
        {
            return Err(CgroupError::new("owner-metadata-invalid"));
        }
        let root = self.root.as_ref().unwrap();
        let expected_root = Identity {
            device: Self::value_u64(value, "root_device")?,
            inode: Self::value_u64(value, "root_inode")?,
        };
        if self.system.identity(root)? != Some(expected_root) {
            return Err(CgroupError::new("root-reused"));
        }
        let owner_path = root.join(&self.owner_name);
        let owner_identity = self.system.identity(&owner_path)?;
        let expected_owner = Identity {
            device: Self::value_u64(value, "owner_device")?,
            inode: Self::value_u64(value, "owner_inode")?,
        };
        if owner_identity.is_some_and(|identity| identity != expected_owner) {
            return Err(CgroupError::new("owner-reused"));
        }
        Ok((owner_path, owner_identity))
    }

    fn ensure_owner(&self) -> CgroupResult<(PathBuf, Identity)> {
        let root = self
            .root
            .as_ref()
            .ok_or_else(|| CgroupError::new("root-missing"))?;
        let root_identity = self
            .system
            .identity(root)?
            .ok_or_else(|| CgroupError::new("root-missing"))?;
        let record = self.owner_record_path();
        if record.exists() {
            let (owner_path, owner_identity) =
                self.validate_owner_record(&self.read_json(&record)?)?;
            if let Some(owner_identity) = owner_identity {
                return Ok((owner_path, owner_identity));
            }
            if fs::read_dir(&self.metadata_dir)
                .map_err(|_| CgroupError::new("metadata-invalid"))?
                .flatten()
                .any(|entry| entry.file_name().to_string_lossy().starts_with("run-"))
            {
                return Err(CgroupError::new("owner-missing"));
            }
            let identity = self.system.create_group(root, &self.owner_name)?;
            self.write_json(&record, &self.owner_record(root_identity, identity))?;
            return Ok((owner_path, identity));
        }
        let owner_path = root.join(&self.owner_name);
        if self.system.identity(&owner_path)?.is_some() {
            return Err(CgroupError::new("owner-collision"));
        }
        let owner_identity = self.system.create_group(root, &self.owner_name)?;
        if let Err(error) =
            self.write_json(&record, &self.owner_record(root_identity, owner_identity))
        {
            let _ = self.system.remove_group(&owner_path);
            return Err(error);
        }
        Ok((owner_path, owner_identity))
    }

    fn controller_resources(request: &CgroupRequest) -> CgroupResult<BTreeMap<String, String>> {
        let mut selected = BTreeMap::new();
        for (name, binding) in &request.bindings {
            let control = match (binding.kind.as_str(), binding.unit.as_str()) {
                ("cpu", "logical-cpu") => Some("cpu"),
                ("processes", "processes") => Some("pids"),
                ("memory", "bytes") => Some("memory.max"),
                ("memory-high", "bytes") => Some("memory.high"),
                ("swap", "bytes") => Some("memory.swap.max"),
                ("tmpfs", "bytes") => Some("tmpfs.size"),
                ("inodes", "inodes") => Some("tmpfs.nr_inodes"),
                ("generic", "admission-unit") => None,
                _ if matches!(
                    binding.kind.as_str(),
                    "io-bandwidth" | "io-operations" | "io-weight"
                ) =>
                {
                    None
                }
                _ => return Err(CgroupError::new("request-unsupported")),
            };
            if let Some(control) = control
                && selected.insert(control.to_owned(), name.clone()).is_some()
            {
                return Err(CgroupError::new("controller-ambiguous"));
            }
        }
        Ok(selected)
    }

    fn io_resources(request: &CgroupRequest) -> CgroupResult<BTreeMap<String, String>> {
        let mut selected = BTreeMap::new();
        for (name, binding) in &request.bindings {
            let controls: &[&str] = match (binding.kind.as_str(), binding.unit.as_str()) {
                ("io-bandwidth", "bytes-per-second") => &["rbps", "wbps"],
                ("io-bandwidth", "read-bytes-per-second") => &["rbps"],
                ("io-bandwidth", "write-bytes-per-second") => &["wbps"],
                ("io-operations", "operations-per-second") => &["riops", "wiops"],
                ("io-operations", "read-operations-per-second") => &["riops"],
                ("io-operations", "write-operations-per-second") => &["wiops"],
                ("io-weight", "weight") => &["weight"],
                _ => continue,
            };
            for control in controls {
                if selected
                    .insert((*control).to_owned(), name.clone())
                    .is_some()
                {
                    return Err(CgroupError::new("controller-ambiguous"));
                }
            }
        }
        Ok(selected)
    }

    fn resolve_io_devices(&self, request: &CgroupRequest) -> CgroupResult<Vec<IoDevice>> {
        if Self::io_resources(request)?.is_empty() {
            return Ok(Vec::new());
        }
        if self.io_paths.is_empty() {
            return Err(CgroupError::new("io-path-unconfigured"));
        }
        if self.system.fixture() {
            let mut devices = Vec::new();
            for (index, path) in self.io_paths.iter().enumerate() {
                let details = fs::symlink_metadata(path)
                    .map_err(|_| CgroupError::new("io-path-unavailable"))?;
                if details.file_type().is_symlink()
                    || !details.file_type().is_dir()
                    || path.canonicalize().ok().as_deref() != Some(path)
                {
                    return Err(CgroupError::new("io-path-invalid"));
                }
                devices.push(IoDevice {
                    number: format!("7:{}", 31 + index),
                    filesystem: "ext4".to_owned(),
                });
            }
            devices.sort_by(|first, second| first.number.cmp(&second.number));
            devices.dedup_by(|first, second| first.number == second.number);
            return Ok(devices);
        }

        let mounts = io_mounts()?;
        let mut selected = BTreeMap::new();
        for path in &self.io_paths {
            let details =
                fs::symlink_metadata(path).map_err(|_| CgroupError::new("io-path-unavailable"))?;
            let target = path
                .canonicalize()
                .map_err(|_| CgroupError::new("io-path-unavailable"))?;
            if details.file_type().is_symlink() || !details.file_type().is_dir() || &target != path
            {
                return Err(CgroupError::new("io-path-invalid"));
            }
            let candidates: Vec<_> = mounts
                .iter()
                .filter(|mount| target.starts_with(&mount.path))
                .collect();
            let Some(depth) = candidates
                .iter()
                .map(|mount| mount.path.components().count())
                .max()
            else {
                return Err(CgroupError::new("io-mount-unavailable"));
            };
            let effective: Vec<_> = candidates
                .into_iter()
                .filter(|mount| mount.path.components().count() == depth)
                .collect();
            if effective.len() != 1 {
                return Err(CgroupError::new("io-mount-ambiguous"));
            }
            let mount = effective[0];
            if !supported_io_filesystem(&mount.filesystem) {
                return Err(CgroupError::new("io-filesystem-unsupported"));
            }
            if mount.root != Path::new("/") {
                return Err(CgroupError::new("io-mount-ambiguous"));
            }
            if mount.options.contains("ro") || !mount.options.contains("rw") {
                return Err(CgroupError::new("io-path-read-only"));
            }
            let source = mount
                .source
                .canonicalize()
                .map_err(|_| CgroupError::new("io-device-unavailable"))?;
            let source_details =
                fs::metadata(source).map_err(|_| CgroupError::new("io-device-unavailable"))?;
            if !source_details.file_type().is_block_device() {
                return Err(CgroupError::new("io-device-ambiguous"));
            }
            let (major, minor) = mount
                .device
                .split_once(':')
                .and_then(|(major, minor)| Some((major.parse().ok()?, minor.parse().ok()?)))
                .ok_or_else(|| CgroupError::new("io-device-ambiguous"))?;
            if linux_device_parts(details.dev()) != (major, minor)
                || linux_device_parts(source_details.rdev()) != (major, minor)
            {
                return Err(CgroupError::new("io-device-ambiguous"));
            }
            let topology = Path::new("/sys/dev/block")
                .join(&mount.device)
                .canonicalize()
                .map_err(|_| CgroupError::new("io-device-unavailable"))?;
            let slaves = topology.join("slaves");
            let layered = topology.join("dm").exists()
                || topology.join("md").exists()
                || topology.join("partition").exists()
                || (slaves.is_dir()
                    && fs::read_dir(slaves)
                        .map_err(|_| CgroupError::new("io-device-unavailable"))?
                        .next()
                        .is_some());
            if layered {
                return Err(CgroupError::new("io-device-ambiguous"));
            }
            let device = IoDevice {
                number: mount.device.clone(),
                filesystem: mount.filesystem.clone(),
            };
            if selected
                .insert(device.number.clone(), device.clone())
                .is_some_and(|previous| previous != device)
            {
                return Err(CgroupError::new("io-device-ambiguous"));
            }
        }
        if selected.is_empty() {
            return Err(CgroupError::new("io-device-response-invalid"));
        }
        Ok(selected.into_values().collect())
    }

    fn requested_controllers(request: &CgroupRequest) -> CgroupResult<BTreeSet<String>> {
        let resources = Self::controller_resources(request)?;
        let mut controllers = BTreeSet::new();
        for control in resources.keys() {
            if control == "cpu" {
                controllers.insert("cpu".to_owned());
            } else if control == "pids" {
                controllers.insert("pids".to_owned());
            } else if control.starts_with("memory") || control.starts_with("tmpfs") {
                controllers.insert("memory".to_owned());
            }
        }
        if request.bindings.values().any(|binding| {
            matches!(
                binding.kind.as_str(),
                "io-bandwidth" | "io-operations" | "io-weight"
            )
        }) {
            controllers.insert("io".to_owned());
        }
        Ok(controllers)
    }

    fn controller_settings(
        &self,
        request: &CgroupRequest,
    ) -> CgroupResult<BTreeMap<String, String>> {
        let resources = Self::controller_resources(request)?;
        let _ = Self::tmpfs_policy(request, &resources)?;
        let mut settings = BTreeMap::new();
        if let Some(name) = resources.get("cpu") {
            let quota = request.resources[name]
                .checked_mul(CPU_PERIOD_USEC)
                .ok_or_else(|| CgroupError::new("cpu-limit-impossible"))?;
            settings.insert("cpu.max".to_owned(), format!("{quota} {CPU_PERIOD_USEC}"));
        }
        if let Some(name) = resources.get("pids") {
            settings.insert("pids.max".to_owned(), request.resources[name].to_string());
        }
        let hard = resources
            .get("memory.max")
            .map(|name| request.resources[name]);
        let high = resources
            .get("memory.high")
            .map(|name| request.resources[name]);
        let swap = resources
            .get("memory.swap.max")
            .map(|name| request.resources[name]);
        if hard.is_some() || high.is_some() || swap.is_some() {
            if hard.zip(high).is_some_and(|(hard, high)| high > hard) {
                return Err(CgroupError::new("memory-limit-impossible"));
            }
            if swap.is_some() && self.system.swap_total_bytes()? == 0 {
                return Err(CgroupError::new("swap-disabled"));
            }
            settings.insert(
                "memory.high".to_owned(),
                high.map_or_else(|| "max".to_owned(), |value| value.to_string()),
            );
            settings.insert(
                "memory.max".to_owned(),
                hard.map_or_else(|| "max".to_owned(), |value| value.to_string()),
            );
            settings.insert(
                "memory.swap.max".to_owned(),
                swap.map_or_else(
                    || if hard.is_some() { "0" } else { "max" }.to_owned(),
                    |value| value.to_string(),
                ),
            );
            settings.insert(
                "memory.oom.group".to_owned(),
                if hard.is_some() { "1" } else { "0" }.to_owned(),
            );
        }
        Ok(settings)
    }

    fn tmpfs_policy(
        request: &CgroupRequest,
        resources: &BTreeMap<String, String>,
    ) -> CgroupResult<Option<TmpfsPolicy>> {
        let size_name = resources.get("tmpfs.size");
        let inode_name = resources.get("tmpfs.nr_inodes");
        if size_name.is_none() && inode_name.is_none() {
            return Ok(None);
        }
        let (Some(size_name), Some(inode_name)) = (size_name, inode_name) else {
            return Err(CgroupError::new("tmpfs-policy-incomplete"));
        };
        let memory_name = resources
            .get("memory.max")
            .filter(|name| request.bindings[*name].required())
            .ok_or_else(|| CgroupError::new("tmpfs-memory-required"))?;
        // SAFETY: sysconf with _SC_PAGESIZE has no pointer arguments or side effects.
        let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        if page_size <= 0 {
            return Err(CgroupError::new("tmpfs-size-impossible"));
        }
        let page_size = page_size as u64;
        let requested = request.resources[size_name];
        let size = requested - requested % page_size;
        if size == 0 {
            return Err(CgroupError::new("tmpfs-size-impossible"));
        }
        if size > request.resources[memory_name] {
            return Err(CgroupError::new("tmpfs-memory-impossible"));
        }
        Ok(Some(TmpfsPolicy {
            size_name: size_name.clone(),
            inode_name: inode_name.clone(),
            size,
            inodes: request.resources[inode_name],
        }))
    }

    fn io_max_values(raw: &str) -> CgroupResult<BTreeMap<String, BTreeMap<String, String>>> {
        let mut selected = BTreeMap::new();
        for line in raw.lines() {
            let mut fields = line.split_whitespace();
            let device = fields.next().unwrap_or("");
            if !device_number_valid(device) || selected.contains_key(device) {
                return Err(CgroupError::new("io-controller-value-invalid"));
            }
            let mut values = BTreeMap::new();
            for field in fields {
                let Some((name, value)) = field.split_once('=') else {
                    return Err(CgroupError::new("io-controller-value-invalid"));
                };
                if !matches!(name, "rbps" | "wbps" | "riops" | "wiops")
                    || values.contains_key(name)
                    || (value != "max"
                        && (value.is_empty()
                            || !value.bytes().all(|byte| byte.is_ascii_digit())
                            || value
                                .parse::<u64>()
                                .ok()
                                .filter(|value| *value > 0)
                                .is_none()))
                {
                    return Err(CgroupError::new("io-controller-value-invalid"));
                }
                values.insert(name.to_owned(), value.to_owned());
            }
            if values.is_empty() {
                return Err(CgroupError::new("io-controller-value-invalid"));
            }
            selected.insert(device.to_owned(), values);
        }
        Ok(selected)
    }

    fn io_weight_values(raw: &str) -> CgroupResult<(u64, BTreeMap<String, u64>)> {
        let mut default = None;
        let mut selected = BTreeMap::new();
        for line in raw.lines() {
            let fields: Vec<_> = line.split_whitespace().collect();
            let value = fields
                .get(1)
                .filter(|_| fields.len() == 2)
                .and_then(|value| value.parse::<u64>().ok())
                .filter(|value| (1..=10_000).contains(value))
                .ok_or_else(|| CgroupError::new("io-controller-value-invalid"))?;
            if fields[0] == "default" {
                if default.replace(value).is_some() {
                    return Err(CgroupError::new("io-controller-value-invalid"));
                }
            } else if !device_number_valid(fields[0])
                || selected.insert(fields[0].to_owned(), value).is_some()
            {
                return Err(CgroupError::new("io-controller-value-invalid"));
            }
        }
        Ok((
            default.ok_or_else(|| CgroupError::new("io-controller-value-invalid"))?,
            selected,
        ))
    }

    fn configure_io(
        &self,
        request: &CgroupRequest,
        leaf: &Path,
        devices: &[IoDevice],
    ) -> CgroupResult<()> {
        let resources = Self::io_resources(request)?;
        if resources.is_empty() {
            return Ok(());
        }
        if devices.is_empty() {
            return Err(CgroupError::new("io-device-missing"));
        }
        let limits: BTreeMap<_, _> = resources
            .iter()
            .filter(|(control, _name)| {
                matches!(control.as_str(), "rbps" | "wbps" | "riops" | "wiops")
            })
            .map(|(control, name)| (control.clone(), request.resources[name]))
            .collect();
        if !limits.is_empty() {
            for device in devices {
                let value = std::iter::once(device.number.clone())
                    .chain(
                        limits
                            .iter()
                            .map(|(control, units)| format!("{control}={units}")),
                    )
                    .collect::<Vec<_>>()
                    .join(" ");
                self.system.write(leaf, "io.max", &value)?;
            }
            let observed = Self::io_max_values(&self.system.read_raw(leaf, "io.max")?)?;
            for device in devices {
                if limits.iter().any(|(control, units)| {
                    observed
                        .get(&device.number)
                        .and_then(|values| values.get(control))
                        .and_then(|value| value.parse::<u64>().ok())
                        != Some(*units)
                }) {
                    return Err(CgroupError::new("controller-value-unverified"));
                }
            }
        }
        if let Some(name) = resources.get("weight") {
            let weight = request.resources[name];
            if !(1..=10_000).contains(&weight) {
                return Err(CgroupError::new("io-weight-invalid"));
            }
            for device in devices {
                self.system
                    .write(leaf, "io.weight", &format!("{} {weight}", device.number))?;
            }
            let (_default, observed) =
                Self::io_weight_values(&self.system.read_raw(leaf, "io.weight")?)?;
            if devices
                .iter()
                .any(|device| observed.get(&device.number) != Some(&weight))
            {
                return Err(CgroupError::new("controller-value-unverified"));
            }
        }
        Ok(())
    }

    fn configure(
        &self,
        request: &CgroupRequest,
        leaf: &Path,
        devices: &[IoDevice],
    ) -> CgroupResult<()> {
        for (name, expected) in self.controller_settings(request)? {
            self.system.write(leaf, &name, &expected)?;
            let observed = self
                .system
                .read_raw(leaf, &name)?
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ");
            if observed != expected {
                return Err(CgroupError::new("controller-value-unverified"));
            }
        }
        self.configure_io(request, leaf, devices)
    }

    fn clear_samples(&mut self, run_id: &str) {
        self.cpu_samples.remove(run_id);
        self.pids_peaks.remove(run_id);
        self.memory_peaks.remove(run_id);
        self.swap_peaks.remove(run_id);
        self.io_samples.remove(run_id);
    }

    fn start_cpu_sample(&mut self, request: &CgroupRequest, leaf: &Path) -> CgroupResult<()> {
        let resources = Self::controller_resources(request)?;
        if !resources.contains_key("cpu") {
            return Ok(());
        }
        let values = self.flat_values(leaf, "cpu.stat", "cpu-stat-invalid")?;
        let usage = values
            .get("usage_usec")
            .copied()
            .ok_or_else(|| CgroupError::new("cpu-stat-invalid"))?;
        self.cpu_samples
            .insert(request.run_id.clone(), (Instant::now(), usage, 0));
        Ok(())
    }

    fn start_memory_sample(&mut self, request: &CgroupRequest, leaf: &Path) -> CgroupResult<()> {
        let resources = Self::controller_resources(request)?;
        if resources.contains_key("memory.max") || resources.contains_key("memory.high") {
            let current = self.single_value(leaf, "memory.current", "memory-current-invalid")?;
            let peak = self.optional_peak(leaf, "memory.peak", current, "memory-peak-invalid")?;
            let _ = self.flat_values(leaf, "memory.events", "memory-events-invalid")?;
            if resources.contains_key("memory.high") {
                let _ = Self::pressure_totals(&self.system.read_raw(leaf, "memory.pressure")?)?;
            }
            self.memory_peaks
                .insert(request.run_id.clone(), current.max(peak));
        }
        if resources.contains_key("memory.swap.max") {
            let current = self.single_value(leaf, "memory.swap.current", "swap-current-invalid")?;
            let peak =
                self.optional_peak(leaf, "memory.swap.peak", current, "swap-peak-invalid")?;
            let _ = self.flat_values(leaf, "memory.swap.events", "swap-events-invalid")?;
            self.swap_peaks
                .insert(request.run_id.clone(), current.max(peak));
        }
        Ok(())
    }

    fn handle(
        &self,
        owner_identity: Identity,
        leaf: &str,
        leaf_identity: Identity,
        token: &str,
        devices: &[IoDevice],
    ) -> Value {
        let mut selected = json!({
            "version": if devices.is_empty() { 1 } else { 2 },
            "owner": self.owner_name,
            "owner_device": owner_identity.device,
            "owner_inode": owner_identity.inode,
            "leaf": leaf,
            "leaf_device": leaf_identity.device,
            "leaf_inode": leaf_identity.inode,
            "token": token,
        });
        if !devices.is_empty() {
            selected.as_object_mut().unwrap().insert(
                "io_devices".to_owned(),
                Value::Array(
                    devices
                        .iter()
                        .map(|device| {
                            json!({
                                "device": device.number,
                                "filesystem": device.filesystem,
                            })
                        })
                        .collect(),
                ),
            );
        }
        selected
    }

    fn manifest(&self, request: &CgroupRequest, handle: &Value) -> Value {
        json!({"version": 1, "run_id": request.run_id, "handle": handle})
    }

    fn validate_handle(&self, value: &Value) -> CgroupResult<()> {
        let object = value
            .as_object()
            .ok_or_else(|| CgroupError::new("handle-invalid"))?;
        let version = value.get("version").and_then(Value::as_u64);
        let mut expected: BTreeSet<_> = [
            "version",
            "owner",
            "owner_device",
            "owner_inode",
            "leaf",
            "leaf_device",
            "leaf_inode",
            "token",
        ]
        .into_iter()
        .collect();
        if version == Some(2) {
            expected.insert("io_devices");
        }
        let leaf = value.get("leaf").and_then(Value::as_str).unwrap_or("");
        let token = value.get("token").and_then(Value::as_str).unwrap_or("");
        if object.keys().map(String::as_str).collect::<BTreeSet<_>>() != expected
            || !matches!(version, Some(1 | 2))
            || value.get("owner").and_then(Value::as_str) != Some(&self.owner_name)
            || Self::value_u64(value, "owner_device").is_err()
            || Self::value_u64(value, "owner_inode").is_err()
            || Self::value_u64(value, "leaf_device").is_err()
            || Self::value_u64(value, "leaf_inode").is_err()
            || !leaf.is_ascii()
            || !leaf.starts_with("run-")
            || leaf.len() != 33
            || !leaf[4..20]
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            || leaf.as_bytes()[20] != b'-'
            || !leaf[21..]
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            || token.len() != 32
            || !token
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(CgroupError::new("handle-invalid"));
        }
        if version == Some(2) {
            let _ = Self::io_devices_from_handle(value)?;
        }
        Ok(())
    }

    fn io_devices_from_handle(value: &Value) -> CgroupResult<Vec<IoDevice>> {
        if value.get("version").and_then(Value::as_u64) == Some(1) {
            return Ok(Vec::new());
        }
        let mut selected = value
            .get("io_devices")
            .and_then(Value::as_array)
            .filter(|devices| !devices.is_empty())
            .ok_or_else(|| CgroupError::new("handle-invalid"))?
            .iter()
            .map(|raw| {
                let raw = raw
                    .as_object()
                    .ok_or_else(|| CgroupError::new("handle-invalid"))?;
                if raw.keys().map(String::as_str).collect::<BTreeSet<_>>()
                    != BTreeSet::from(["device", "filesystem"])
                {
                    return Err(CgroupError::new("handle-invalid"));
                }
                let number = raw
                    .get("device")
                    .and_then(Value::as_str)
                    .filter(|value| device_number_valid(value))
                    .ok_or_else(|| CgroupError::new("handle-invalid"))?;
                let filesystem = raw
                    .get("filesystem")
                    .and_then(Value::as_str)
                    .filter(|value| supported_io_filesystem(value))
                    .ok_or_else(|| CgroupError::new("handle-invalid"))?;
                Ok(IoDevice {
                    number: number.to_owned(),
                    filesystem: filesystem.to_owned(),
                })
            })
            .collect::<CgroupResult<Vec<_>>>()?;
        let original = selected.clone();
        selected.sort_by(|first, second| first.number.cmp(&second.number));
        if selected != original
            || selected
                .windows(2)
                .any(|devices| devices[0].number == devices[1].number)
        {
            return Err(CgroupError::new("handle-invalid"));
        }
        Ok(selected)
    }

    fn read_manifest(&self, request: &CgroupRequest) -> CgroupResult<Value> {
        let value = self
            .read_json(&self.manifest_path(&request.run_id))
            .map_err(|error| {
                if error.code == "metadata-missing" {
                    CgroupError::new("manifest-missing")
                } else {
                    error
                }
            })?;
        let object = value
            .as_object()
            .ok_or_else(|| CgroupError::new("manifest-invalid"))?;
        let expected: BTreeSet<_> = ["version", "run_id", "handle"].into_iter().collect();
        if object.keys().map(String::as_str).collect::<BTreeSet<_>>() != expected
            || value.get("version").and_then(Value::as_u64) != Some(1)
        {
            return Err(CgroupError::new("manifest-invalid"));
        }
        if value.get("run_id").and_then(Value::as_str) != Some(&request.run_id) {
            return Err(CgroupError::new("manifest-collision"));
        }
        let handle = value
            .get("handle")
            .cloned()
            .ok_or_else(|| CgroupError::new("manifest-invalid"))?;
        self.validate_handle(&handle)?;
        Ok(handle)
    }

    fn resolve(
        &self,
        request: &CgroupRequest,
        handle: &Value,
        allow_missing: bool,
    ) -> CgroupResult<Option<PathBuf>> {
        self.validate_handle(handle)?;
        if self.read_manifest(request)? != *handle {
            return Err(CgroupError::new("handle-mismatch"));
        }
        let (owner, owner_identity) =
            self.validate_owner_record(&self.read_json(&self.owner_record_path())?)?;
        let expected_owner = Identity {
            device: Self::value_u64(handle, "owner_device")?,
            inode: Self::value_u64(handle, "owner_inode")?,
        };
        match owner_identity {
            None if allow_missing => return Ok(None),
            None => return Err(CgroupError::new("owner-missing")),
            Some(observed) if observed != expected_owner => {
                return Err(CgroupError::new("owner-reused"));
            }
            Some(_) => {}
        }
        let leaf = owner.join(handle.get("leaf").and_then(Value::as_str).unwrap());
        let observed = self.system.identity(&leaf)?;
        let expected_leaf = Identity {
            device: Self::value_u64(handle, "leaf_device")?,
            inode: Self::value_u64(handle, "leaf_inode")?,
        };
        match observed {
            None if allow_missing => return Ok(None),
            None => return Err(CgroupError::new("leaf-missing")),
            Some(observed) if observed != expected_leaf => {
                return Err(CgroupError::new("leaf-reused"));
            }
            Some(_) => {}
        }
        Ok(Some(leaf))
    }

    pub fn prepare(&mut self, request: &CgroupRequest) -> CgroupResult<Value> {
        if request.resources.is_empty() || request.resources.len() != request.bindings.len() {
            return Err(CgroupError::new("request-invalid"));
        }
        self.require_available()?;
        let io_devices = self.resolve_io_devices(request)?;
        self.prepare_metadata()?;
        self.system.prepare_root(
            self.root
                .as_deref()
                .ok_or_else(|| CgroupError::new("root-missing"))?,
        )?;
        let (owner, owner_identity) = self.ensure_owner()?;
        let controller_setup = (|| {
            let controllers = Self::requested_controllers(request)?;
            self.system
                .enable_controllers(self.root.as_ref().unwrap(), &controllers)?;
            self.system.enable_controllers(&owner, &controllers)
        })();
        if let Err(error) = controller_setup {
            self.cleanup_owner()?;
            return Err(error);
        }
        let manifest_path = self.manifest_path(&request.run_id);
        if manifest_path.exists() {
            let handle = self.read_manifest(request)?;
            if Self::io_devices_from_handle(&handle)? != io_devices {
                return Err(CgroupError::new("io-device-changed"));
            }
            if let Some(leaf) = self.resolve(request, &handle, true)? {
                if self.system.populated(&leaf)? {
                    return Err(CgroupError::new("leaf-populated"));
                }
                let configured = self
                    .configure(request, &leaf, &io_devices)
                    .and_then(|()| self.start_cpu_sample(request, &leaf))
                    .and_then(|()| self.start_memory_sample(request, &leaf))
                    .and_then(|()| self.start_io_sample(request, &leaf, &io_devices));
                if let Err(error) = configured {
                    self.clear_samples(&request.run_id);
                    let _ = self.system.remove_group(&leaf);
                    let _ = fs::remove_file(&manifest_path);
                    let _ = self.cleanup_owner();
                    return Err(error);
                }
                return Ok(handle);
            }
            fs::remove_file(&manifest_path).map_err(|_| CgroupError::new("metadata-invalid"))?;
        }
        for _attempt in 0..16 {
            let token = random_hex(16)?;
            let leaf_name = format!(
                "run-{}-{}",
                sha256_prefix(request.run_id.as_bytes(), 8),
                &token[..12]
            );
            let leaf_identity = match self.system.create_group(&owner, &leaf_name) {
                Ok(identity) => identity,
                Err(error) if error.code == "path-collision" => continue,
                Err(error) => return Err(error),
            };
            let leaf = owner.join(&leaf_name);
            let handle = self.handle(
                owner_identity,
                &leaf_name,
                leaf_identity,
                &token,
                &io_devices,
            );
            let prepared = self
                .configure(request, &leaf, &io_devices)
                .and_then(|()| self.start_cpu_sample(request, &leaf))
                .and_then(|()| self.start_memory_sample(request, &leaf))
                .and_then(|()| self.start_io_sample(request, &leaf, &io_devices))
                .and_then(|()| self.write_json(&manifest_path, &self.manifest(request, &handle)));
            if let Err(error) = prepared {
                self.clear_samples(&request.run_id);
                let _ = self.system.remove_group(&leaf);
                let _ = self.cleanup_owner();
                return Err(error);
            }
            return Ok(handle);
        }
        Err(CgroupError::new("leaf-collision"))
    }

    pub fn attach(
        &mut self,
        request: &CgroupRequest,
        handle: &Value,
        worker_pid: u32,
    ) -> CgroupResult<()> {
        self.validate_handle(handle)?;
        if Self::io_devices_from_handle(handle)? != self.resolve_io_devices(request)? {
            return Err(CgroupError::new("io-device-changed"));
        }
        let leaf = self
            .resolve(request, handle, false)?
            .ok_or_else(|| CgroupError::new("leaf-missing"))?;
        self.system.attach(&leaf, worker_pid)?;
        if !self.system.members(&leaf)?.contains(&worker_pid) {
            return Err(CgroupError::new("attach-unverified"));
        }
        Ok(())
    }

    pub fn validate_recovery(&self, request: &CgroupRequest, handle: &Value) -> CgroupResult<()> {
        self.validate_handle(handle)?;
        if Self::io_devices_from_handle(handle)? != self.resolve_io_devices(request)? {
            return Err(CgroupError::new("io-device-changed"));
        }
        let _ = self.resolve(request, handle, true)?;
        Ok(())
    }

    pub fn tmpfs_setup(
        &self,
        request: &CgroupRequest,
        handle: &Value,
    ) -> CgroupResult<Option<TmpfsSetup>> {
        let resources = Self::controller_resources(request)?;
        let Some(policy) = Self::tmpfs_policy(request, &resources)? else {
            return Ok(None);
        };
        let _ = self
            .resolve(request, handle, false)?
            .ok_or_else(|| CgroupError::new("leaf-missing"))?;
        let target = self.tmpfs_target_path(&request.run_id, handle)?;
        fs::create_dir(&target).map_err(|error| {
            CgroupError::new(if error.kind() == io::ErrorKind::AlreadyExists {
                "tmpfs-target-collision"
            } else {
                "tmpfs-target-unavailable"
            })
        })?;
        if fs::set_permissions(&target, fs::Permissions::from_mode(0o700)).is_err() {
            let _ = fs::remove_dir(&target);
            return Err(CgroupError::new("tmpfs-target-unavailable"));
        }
        let details =
            fs::symlink_metadata(&target).map_err(|_| CgroupError::new("tmpfs-target-invalid"))?;
        if !target.is_absolute()
            || target.canonicalize().ok().as_deref() != Some(target.as_path())
            || details.file_type().is_symlink()
            || !details.file_type().is_dir()
            || details.uid() != unsafe { libc::geteuid() }
            || details.mode() & 0o077 != 0
        {
            let _ = fs::remove_dir(&target);
            return Err(CgroupError::new("tmpfs-target-invalid"));
        }
        let token = handle
            .get("token")
            .and_then(Value::as_str)
            .ok_or_else(|| CgroupError::new("handle-invalid"))?;
        Ok(Some(TmpfsSetup {
            target,
            size: policy.size,
            inodes: policy.inodes,
            report: self.tmpfs_report_path(&request.run_id),
            token: token.to_owned(),
            emulate: self.system.fixture(),
        }))
    }

    fn flat_values(
        &self,
        leaf: &Path,
        file: &str,
        code: &str,
    ) -> CgroupResult<BTreeMap<String, u64>> {
        let raw = self.system.read_raw(leaf, file)?;
        let mut selected = BTreeMap::new();
        for line in raw.lines() {
            let fields: Vec<_> = line.split_whitespace().collect();
            if fields.len() != 2
                || !controller_name_valid(fields[0])
                || fields[1].is_empty()
                || !fields[1].bytes().all(|byte| byte.is_ascii_digit())
                || selected
                    .insert(
                        fields[0].to_owned(),
                        fields[1]
                            .parse::<u64>()
                            .map_err(|_| CgroupError::new(code))?,
                    )
                    .is_some()
            {
                return Err(CgroupError::new(code));
            }
        }
        if selected.is_empty() {
            return Err(CgroupError::new(code));
        }
        Ok(selected)
    }

    fn single_value(&self, leaf: &Path, file: &str, code: &str) -> CgroupResult<u64> {
        let raw = self.system.read_raw(leaf, file)?;
        let value = raw.trim();
        if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(CgroupError::new(code));
        }
        value.parse().map_err(|_| CgroupError::new(code))
    }

    fn optional_peak(
        &self,
        leaf: &Path,
        file: &str,
        current: u64,
        code: &str,
    ) -> CgroupResult<u64> {
        match self.single_value(leaf, file, code) {
            Ok(value) => Ok(value),
            Err(error) if error.code == "controller-file-missing" => Ok(current),
            Err(error) => Err(error),
        }
    }

    fn pressure_totals(raw: &str) -> CgroupResult<BTreeMap<String, u64>> {
        let mut selected = BTreeMap::new();
        for line in raw.lines() {
            let mut fields = line.split_whitespace();
            let category = fields.next().unwrap_or("");
            if !matches!(category, "some" | "full") || selected.contains_key(category) {
                return Err(CgroupError::new("memory-pressure-invalid"));
            }
            let mut values = BTreeMap::new();
            for field in fields {
                let Some((name, value)) = field.split_once('=') else {
                    return Err(CgroupError::new("memory-pressure-invalid"));
                };
                if !matches!(name, "avg10" | "avg60" | "avg300" | "total")
                    || values.insert(name, value).is_some()
                    || value.is_empty()
                    || !value
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || byte == b'.')
                    || value.bytes().filter(|byte| *byte == b'.').count() > 1
                {
                    return Err(CgroupError::new("memory-pressure-invalid"));
                }
            }
            if values.keys().copied().collect::<BTreeSet<_>>()
                != BTreeSet::from(["avg10", "avg300", "avg60", "total"])
                || !values["total"].bytes().all(|byte| byte.is_ascii_digit())
            {
                return Err(CgroupError::new("memory-pressure-invalid"));
            }
            selected.insert(
                category.to_owned(),
                values["total"]
                    .parse()
                    .map_err(|_| CgroupError::new("memory-pressure-invalid"))?,
            );
        }
        if selected.keys().map(String::as_str).collect::<BTreeSet<_>>()
            != BTreeSet::from(["full", "some"])
        {
            return Err(CgroupError::new("memory-pressure-invalid"));
        }
        Ok(selected)
    }

    fn io_stat_values(raw: &str) -> CgroupResult<BTreeMap<String, BTreeMap<String, u64>>> {
        let mut selected = BTreeMap::new();
        for line in raw.lines() {
            let mut fields = line.split_whitespace();
            let device = fields.next().unwrap_or("");
            if !device_number_valid(device) || selected.contains_key(device) {
                return Err(CgroupError::new("io-stat-invalid"));
            }
            let mut values = BTreeMap::new();
            for field in fields {
                let Some((name, value)) = field.split_once('=') else {
                    return Err(CgroupError::new("io-stat-invalid"));
                };
                if !controller_name_valid(name)
                    || value.is_empty()
                    || !value.bytes().all(|byte| byte.is_ascii_digit())
                    || values
                        .insert(
                            name.to_owned(),
                            value
                                .parse::<u64>()
                                .map_err(|_| CgroupError::new("io-stat-invalid"))?,
                        )
                        .is_some()
                {
                    return Err(CgroupError::new("io-stat-invalid"));
                }
            }
            if !["rbytes", "wbytes", "rios", "wios"]
                .iter()
                .all(|name| values.contains_key(*name))
            {
                return Err(CgroupError::new("io-stat-invalid"));
            }
            selected.insert(device.to_owned(), values);
        }
        Ok(selected)
    }

    fn io_counter_totals(
        &self,
        leaf: &Path,
        devices: &[IoDevice],
    ) -> CgroupResult<BTreeMap<String, u64>> {
        let values = Self::io_stat_values(&self.system.read_raw(leaf, "io.stat")?)?;
        let counters = [
            ("rbps", "rbytes"),
            ("wbps", "wbytes"),
            ("riops", "rios"),
            ("wiops", "wios"),
        ];
        counters
            .into_iter()
            .map(|(control, counter)| {
                let total = devices.iter().try_fold(0_u64, |total, device| {
                    total
                        .checked_add(
                            values
                                .get(&device.number)
                                .and_then(|values| values.get(counter))
                                .copied()
                                .unwrap_or(0),
                        )
                        .ok_or_else(|| CgroupError::new("io-stat-invalid"))
                })?;
                Ok((control.to_owned(), total))
            })
            .collect()
    }

    fn measure_io(
        &mut self,
        request: &CgroupRequest,
        leaf: &Path,
        devices: &[IoDevice],
    ) -> CgroupResult<BTreeMap<String, u64>> {
        let measured: BTreeMap<_, _> = Self::io_resources(request)?
            .into_iter()
            .filter(|(control, _name)| {
                matches!(control.as_str(), "rbps" | "wbps" | "riops" | "wiops")
            })
            .collect();
        if measured.is_empty() {
            return Ok(BTreeMap::new());
        }
        let counters = self.io_counter_totals(leaf, devices)?;
        let current = Instant::now();
        let Some(sample) = self.io_samples.get_mut(&request.run_id) else {
            let peaks: BTreeMap<String, u64> =
                measured.values().map(|name| (name.clone(), 0)).collect();
            self.io_samples.insert(
                request.run_id.clone(),
                IoSample {
                    at: current,
                    counters,
                    peaks: peaks.clone(),
                },
            );
            return Ok(peaks);
        };
        if measured.iter().any(|(control, _name)| {
            counters[control] < sample.counters.get(control).copied().unwrap_or(0)
        }) {
            return Err(CgroupError::new("io-stat-invalid"));
        }
        let elapsed = current.duration_since(sample.at).as_nanos();
        if elapsed > 0 {
            let mut rates = BTreeMap::new();
            for control in measured.keys() {
                let delta = u128::from(
                    counters[control] - sample.counters.get(control).copied().unwrap_or(0),
                );
                let rate = delta
                    .saturating_mul(1_000_000_000)
                    .saturating_add(elapsed - 1)
                    / elapsed;
                rates.insert(control.clone(), rate.min(u128::from(u64::MAX)) as u64);
            }
            for name in measured.values().cloned().collect::<BTreeSet<_>>() {
                let observed = measured
                    .iter()
                    .filter(|(_control, resource)| *resource == &name)
                    .map(|(control, _resource)| rates[control])
                    .max()
                    .unwrap_or(0);
                sample
                    .peaks
                    .entry(name)
                    .and_modify(|peak| *peak = (*peak).max(observed))
                    .or_insert(observed);
            }
        }
        sample.at = current;
        sample.counters = counters;
        Ok(sample.peaks.clone())
    }

    fn start_io_sample(
        &mut self,
        request: &CgroupRequest,
        leaf: &Path,
        devices: &[IoDevice],
    ) -> CgroupResult<()> {
        self.io_samples.remove(&request.run_id);
        let _ = self.measure_io(request, leaf, devices)?;
        Ok(())
    }

    fn measurement(
        &mut self,
        request: &CgroupRequest,
        leaf: &Path,
        handle: &Value,
    ) -> CgroupResult<Measurement> {
        let resources = Self::controller_resources(request)?;
        let mut measured = Measurement::default();

        if let Some(name) = resources.get("cpu") {
            let stats = self.flat_values(leaf, "cpu.stat", "cpu-stat-invalid")?;
            let usage = stats
                .get("usage_usec")
                .copied()
                .ok_or_else(|| CgroupError::new("cpu-stat-invalid"))?;
            let current = Instant::now();
            let mut peak = 0;
            if let Some((previous, previous_usage, previous_peak)) =
                self.cpu_samples.get(&request.run_id).copied()
            {
                if usage < previous_usage {
                    return Err(CgroupError::new("cpu-stat-invalid"));
                }
                peak = previous_peak;
                let elapsed = current.duration_since(previous).as_micros();
                if elapsed > 0 {
                    let used = u128::from(usage - previous_usage);
                    let concurrency = used.div_ceil(elapsed);
                    peak = peak.max(concurrency.min(u128::from(u64::MAX)) as u64);
                }
            }
            self.cpu_samples
                .insert(request.run_id.clone(), (current, usage, peak));
            measured.peak.insert(name.clone(), peak);
            if stats.get("nr_throttled").copied().unwrap_or(0) > 0
                || stats.get("throttled_usec").copied().unwrap_or(0) > 0
            {
                measured.observations.push(Observation {
                    resource: name.clone(),
                    code: "cpu-throttled".to_owned(),
                });
            }
        }

        if let Some(name) = resources.get("pids") {
            let current = self.single_value(leaf, "pids.current", "pids-current-invalid")?;
            let reported = self.optional_peak(leaf, "pids.peak", current, "pids-peak-invalid")?;
            let peak = current
                .max(reported)
                .max(self.pids_peaks.get(&request.run_id).copied().unwrap_or(0));
            self.pids_peaks.insert(request.run_id.clone(), peak);
            measured.peak.insert(name.clone(), peak);
            if self
                .flat_values(leaf, "pids.events", "pids-events-invalid")?
                .get("max")
                .copied()
                .unwrap_or(0)
                > 0
            {
                measured.observations.push(Observation {
                    resource: name.clone(),
                    code: "pids-limit-hit".to_owned(),
                });
            }
        }

        let hard_name = resources.get("memory.max");
        let high_name = resources.get("memory.high");
        if hard_name.is_some() || high_name.is_some() {
            let current = self.single_value(leaf, "memory.current", "memory-current-invalid")?;
            let reported =
                self.optional_peak(leaf, "memory.peak", current, "memory-peak-invalid")?;
            let peak = current
                .max(reported)
                .max(self.memory_peaks.get(&request.run_id).copied().unwrap_or(0));
            self.memory_peaks.insert(request.run_id.clone(), peak);
            for name in [hard_name, high_name].into_iter().flatten() {
                measured.peak.insert(name.clone(), peak);
            }
            let events = self.flat_values(leaf, "memory.events", "memory-events-invalid")?;
            if let Some(name) = hard_name {
                if events.get("max").copied().unwrap_or(0) > 0 {
                    measured.observations.push(Observation {
                        resource: name.clone(),
                        code: "memory-max-hit".to_owned(),
                    });
                }
                if events.get("oom_kill").copied().unwrap_or(0) > 0
                    || events.get("oom_group_kill").copied().unwrap_or(0) > 0
                {
                    measured.observations.push(Observation {
                        resource: name.clone(),
                        code: "memory-oom".to_owned(),
                    });
                }
            }
            if let Some(name) = high_name {
                if events.get("high").copied().unwrap_or(0) > 0 {
                    measured.observations.push(Observation {
                        resource: name.clone(),
                        code: "memory-high-throttled".to_owned(),
                    });
                }
                let pressure =
                    Self::pressure_totals(&self.system.read_raw(leaf, "memory.pressure")?)?;
                if pressure.values().any(|value| *value > 0) {
                    measured.observations.push(Observation {
                        resource: name.clone(),
                        code: "memory-pressure".to_owned(),
                    });
                }
            }
        }

        if let Some(name) = resources.get("memory.swap.max") {
            let current = self.single_value(leaf, "memory.swap.current", "swap-current-invalid")?;
            let reported =
                self.optional_peak(leaf, "memory.swap.peak", current, "swap-peak-invalid")?;
            let peak = current
                .max(reported)
                .max(self.swap_peaks.get(&request.run_id).copied().unwrap_or(0));
            self.swap_peaks.insert(request.run_id.clone(), peak);
            measured.peak.insert(name.clone(), peak);
            let events = self.flat_values(leaf, "memory.swap.events", "swap-events-invalid")?;
            if events.get("max").copied().unwrap_or(0) > 0
                || events.get("fail").copied().unwrap_or(0) > 0
            {
                measured.observations.push(Observation {
                    resource: name.clone(),
                    code: "swap-limit-hit".to_owned(),
                });
            }
        }
        if let Some(policy) = Self::tmpfs_policy(request, &resources)? {
            if self.system.fixture() {
                let target = self.tmpfs_target_path(&request.run_id, handle)?;
                if target.exists() {
                    let (bytes, inodes) = fixture_directory_usage(&target)?;
                    measured
                        .peak
                        .insert(policy.size_name.clone(), bytes.min(policy.size));
                    measured
                        .peak
                        .insert(policy.inode_name.clone(), inodes.min(policy.inodes));
                    if bytes >= policy.size {
                        measured.observations.push(Observation {
                            resource: policy.size_name,
                            code: "tmpfs-byte-limit-hit".to_owned(),
                        });
                    }
                    if inodes >= policy.inodes {
                        measured.observations.push(Observation {
                            resource: policy.inode_name,
                            code: "tmpfs-inode-limit-hit".to_owned(),
                        });
                    }
                }
            } else {
                let report_path = self.tmpfs_report_path(&request.run_id);
                if report_path.exists() {
                    let report = self.read_json(&report_path)?;
                    let object = report
                        .as_object()
                        .ok_or_else(|| CgroupError::new("tmpfs-report-invalid"))?;
                    let expected = BTreeSet::from([
                        "byte_limit_hit",
                        "inode_limit_hit",
                        "peak_bytes",
                        "peak_inodes",
                        "terminal_bytes",
                        "terminal_inodes",
                        "token",
                        "version",
                    ]);
                    let numeric = [
                        "peak_bytes",
                        "peak_inodes",
                        "terminal_bytes",
                        "terminal_inodes",
                    ];
                    if object.keys().map(String::as_str).collect::<BTreeSet<_>>() != expected
                        || report.get("version").and_then(Value::as_u64) != Some(1)
                        || report.get("token").and_then(Value::as_str)
                            != handle.get("token").and_then(Value::as_str)
                        || numeric
                            .iter()
                            .any(|name| report.get(*name).and_then(Value::as_u64).is_none())
                        || report
                            .get("byte_limit_hit")
                            .and_then(Value::as_bool)
                            .is_none()
                        || report
                            .get("inode_limit_hit")
                            .and_then(Value::as_bool)
                            .is_none()
                        || report["terminal_bytes"].as_u64().unwrap()
                            > report["peak_bytes"].as_u64().unwrap()
                        || report["terminal_inodes"].as_u64().unwrap()
                            > report["peak_inodes"].as_u64().unwrap()
                        || report["peak_bytes"].as_u64().unwrap() > policy.size
                        || report["peak_inodes"].as_u64().unwrap() > policy.inodes
                    {
                        return Err(CgroupError::new("tmpfs-report-invalid"));
                    }
                    measured.peak.insert(
                        policy.size_name.clone(),
                        report["peak_bytes"].as_u64().unwrap(),
                    );
                    measured.peak.insert(
                        policy.inode_name.clone(),
                        report["peak_inodes"].as_u64().unwrap(),
                    );
                    if report["byte_limit_hit"] == Value::Bool(true) {
                        measured.observations.push(Observation {
                            resource: policy.size_name,
                            code: "tmpfs-byte-limit-hit".to_owned(),
                        });
                    }
                    if report["inode_limit_hit"] == Value::Bool(true) {
                        measured.observations.push(Observation {
                            resource: policy.inode_name,
                            code: "tmpfs-inode-limit-hit".to_owned(),
                        });
                    }
                }
            }
        }
        let devices = Self::io_devices_from_handle(handle)?;
        measured
            .peak
            .extend(self.measure_io(request, leaf, &devices)?);
        Ok(measured)
    }

    fn kill_and_wait(&self, leaf: &Path) -> CgroupResult<()> {
        if self.system.populated(leaf)? {
            self.system.kill(leaf)?;
        }
        let deadline = Instant::now() + EMPTY_TIMEOUT;
        while self.system.populated(leaf)? {
            if Instant::now() >= deadline {
                return Err(CgroupError::new("leaf-populated"));
            }
            thread::sleep(Duration::from_millis(20));
        }
        Ok(())
    }

    pub fn cancel(&mut self, request: &CgroupRequest, handle: &Value) -> CgroupResult<()> {
        if let Some(leaf) = self.resolve(request, handle, true)? {
            self.kill_and_wait(&leaf)?;
        }
        Ok(())
    }

    pub fn finish(&mut self, request: &CgroupRequest, handle: &Value) -> CgroupResult<Measurement> {
        let Some(leaf) = self.resolve(request, handle, true)? else {
            return Ok(Measurement::default());
        };
        self.kill_and_wait(&leaf)?;
        self.measurement(request, &leaf, handle)
    }

    fn cleanup_owner(&self) -> CgroupResult<()> {
        if !self.metadata_dir.exists()
            || fs::read_dir(&self.metadata_dir)
                .map_err(|_| CgroupError::new("metadata-invalid"))?
                .flatten()
                .any(|entry| entry.file_name().to_string_lossy().starts_with("run-"))
        {
            return Ok(());
        }
        let record = self.owner_record_path();
        if !record.exists() {
            return Ok(());
        }
        let (owner, identity) = self.validate_owner_record(&self.read_json(&record)?)?;
        let Some(_identity) = identity else {
            fs::remove_file(record).map_err(|_| CgroupError::new("metadata-invalid"))?;
            return Ok(());
        };
        if self.system.populated(&owner)? {
            return Ok(());
        }
        self.system.remove_group(&owner)?;
        fs::remove_file(record).map_err(|_| CgroupError::new("metadata-invalid"))?;
        self.system.cleanup_root()
    }

    pub fn cleanup(&mut self, request: &CgroupRequest, handle: &Value) -> CgroupResult<()> {
        if self.read_manifest(request)? != *handle {
            return Err(CgroupError::new("handle-mismatch"));
        }
        if let Some(leaf) = self.resolve(request, handle, true)? {
            if self.system.populated(&leaf)? {
                return Err(CgroupError::new("leaf-populated"));
            }
            self.system.remove_group(&leaf)?;
        }
        self.clear_samples(&request.run_id);
        let report = self.tmpfs_report_path(&request.run_id);
        if report.exists() {
            fs::remove_file(report).map_err(|_| CgroupError::new("metadata-invalid"))?;
        }
        let resources = Self::controller_resources(request)?;
        if Self::tmpfs_policy(request, &resources)?.is_some() {
            let target = self.tmpfs_target_path(&request.run_id, handle)?;
            if target.exists() {
                let details = fs::symlink_metadata(&target)
                    .map_err(|_| CgroupError::new("tmpfs-target-invalid"))?;
                if details.file_type().is_symlink()
                    || !details.file_type().is_dir()
                    || details.uid() != unsafe { libc::geteuid() }
                    || target.canonicalize().ok().as_deref() != Some(&target)
                {
                    return Err(CgroupError::new("tmpfs-target-invalid"));
                }
                fs::remove_dir_all(&target)
                    .map_err(|_| CgroupError::new("tmpfs-cleanup-failed"))?;
            }
        }
        fs::remove_file(self.manifest_path(&request.run_id))
            .map_err(|_| CgroupError::new("metadata-invalid"))?;
        self.cleanup_owner()
    }

    pub fn usage(&mut self, request: &CgroupRequest, handle: &Value) -> CgroupResult<Measurement> {
        let Some(leaf) = self.resolve(request, handle, false)? else {
            return Err(CgroupError::new("leaf-missing"));
        };
        self.measurement(request, &leaf, handle)
    }
}
