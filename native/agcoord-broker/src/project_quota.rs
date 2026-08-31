use crate::cgroup::{Measurement, Observation, sha256_prefix};
use crate::resources::{Binding, Capability, RESOURCE_OPERATIONS};
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::CString;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::os::unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};

pub const PROJECT_QUOTA_BACKEND: &str = "project-quota";
const QUOTA_BLOCK_BYTES: u64 = 1024;
const MAX_METADATA_BYTES: u64 = 64 * 1024;
const MIN_PROJECT_ID: u64 = 1_000_000_000;
const MAX_PROJECT_ID: u64 = 2_000_000_000;
const PROJECT_ATTEMPTS: usize = 64;
const FS_XFLAG_PROJINHERIT: u32 = 0x0000_0200;
const CLEANUP_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Clone, Debug)]
pub struct QuotaError {
    pub code: String,
}

impl QuotaError {
    fn new(code: &str) -> Self {
        Self {
            code: code.to_owned(),
        }
    }
}

type QuotaResult<T> = std::result::Result<T, QuotaError>;

#[derive(Clone, Debug)]
pub struct QuotaRequest {
    pub run_id: String,
    pub resources: BTreeMap<String, u64>,
    pub bindings: BTreeMap<String, Binding>,
}

impl QuotaRequest {
    pub fn new(
        run_id: &str,
        resources: &BTreeMap<String, u64>,
        bindings: &BTreeMap<String, Binding>,
    ) -> Self {
        let selected_bindings: BTreeMap<_, _> = resources
            .keys()
            .filter_map(|name| {
                bindings
                    .get(name)
                    .filter(|binding| binding.backend.as_deref() == Some(PROJECT_QUOTA_BACKEND))
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

#[derive(Clone, Debug, Eq, PartialEq)]
struct Policy {
    storage_name: String,
    inode_name: String,
    hard_bytes: u64,
    hard_inodes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct QuotaMount {
    path: PathBuf,
    source: PathBuf,
    filesystem: String,
    device: String,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct Attributes {
    project_id: u64,
    inherit: bool,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct Usage {
    hard_bytes: u64,
    hard_inodes: u64,
    used_bytes: u64,
    used_inodes: u64,
}

#[derive(Clone, Debug)]
enum QuotaSystem {
    Real,
    Fixture { root: PathBuf },
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
struct Fsxattr {
    xflags: u32,
    extsize: u32,
    nextents: u32,
    project_id: u32,
    cowextsize: u32,
    padding: [u8; 8],
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
struct Dqblk {
    block_hard: u64,
    block_soft: u64,
    current_space: u64,
    inode_hard: u64,
    inode_soft: u64,
    current_inodes: u64,
    block_time: u64,
    inode_time: u64,
    valid: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
struct XfsDiskQuota {
    version: i8,
    flags: i8,
    fieldmask: u16,
    project_id: u32,
    block_hard: u64,
    block_soft: u64,
    inode_hard: u64,
    inode_soft: u64,
    block_count: u64,
    inode_count: u64,
    inode_timer: i32,
    block_timer: i32,
    inode_warns: u16,
    block_warns: u16,
    inode_timer_hi: i8,
    block_timer_hi: i8,
    realtime_timer_hi: i8,
    padding2: i8,
    realtime_hard: u64,
    realtime_soft: u64,
    realtime_count: u64,
    realtime_timer: i32,
    realtime_warns: u16,
    padding3: i16,
    padding4: [u8; 8],
}

const fn ioc(direction: u64, kind: u64, number: u64, size: u64) -> libc::Ioctl {
    ((direction << 30) | (size << 16) | (kind << 8) | number) as libc::Ioctl
}

const FS_IOC_FSGETXATTR: libc::Ioctl =
    ioc(2, b'X' as u64, 31, std::mem::size_of::<Fsxattr>() as u64);
const FS_IOC_FSSETXATTR: libc::Ioctl =
    ioc(1, b'X' as u64, 32, std::mem::size_of::<Fsxattr>() as u64);
const Q_GETQUOTA: i32 = 0x800007;
const Q_SETQUOTA: i32 = 0x800008;
const PRJQUOTA: i32 = 2;
const QIF_BLIMITS: u32 = 1 << 0;
const QIF_ILIMITS: u32 = 1 << 2;
const Q_XGETQUOTA: i32 = (b'X' as i32) << 8 | 3;
const Q_XSETQLIM: i32 = (b'X' as i32) << 8 | 4;
const FS_PROJ_QUOTA: i8 = 1 << 1;
const FS_DQ_ISOFT: u16 = 1 << 0;
const FS_DQ_IHARD: u16 = 1 << 1;
const FS_DQ_BSOFT: u16 = 1 << 2;
const FS_DQ_BHARD: u16 = 1 << 3;

fn qcmd(command: i32) -> i32 {
    (command << 8) | PRJQUOTA
}

fn operation_error(error: &io::Error, attributes: bool) -> QuotaError {
    let code = match error.raw_os_error() {
        Some(libc::EPERM) | Some(libc::EACCES) => "quota-privilege-unavailable",
        Some(libc::EINVAL) | Some(libc::ENOTTY) | Some(libc::EOPNOTSUPP) if attributes => {
            "quota-project-attributes-unavailable"
        }
        Some(libc::EINVAL) | Some(libc::ENOSYS) | Some(libc::EOPNOTSUPP) | Some(libc::ESRCH) => {
            "quota-enforcement-unavailable"
        }
        _ => "quota-operation-failed",
    };
    QuotaError::new(code)
}

fn random_hex(bytes: usize) -> QuotaResult<String> {
    let mut value = vec![0_u8; bytes];
    let mut offset = 0;
    while offset < value.len() {
        // SAFETY: the pointer names the unwritten suffix of an owned byte buffer.
        let written = unsafe {
            libc::getrandom(value[offset..].as_mut_ptr().cast(), value.len() - offset, 0)
        };
        if written > 0 {
            offset += written as usize;
        } else if written < 0 && io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
            continue;
        } else {
            return Err(QuotaError::new("randomness-unavailable"));
        }
    }
    Ok(value.iter().map(|byte| format!("{byte:02x}")).collect())
}

fn random_project_id() -> QuotaResult<u64> {
    let token = random_hex(8)?;
    let value =
        u64::from_str_radix(&token, 16).map_err(|_| QuotaError::new("randomness-unavailable"))?;
    Ok(MIN_PROJECT_ID + value % (MAX_PROJECT_ID - MIN_PROJECT_ID))
}

fn device_number(details: &fs::Metadata) -> String {
    format!(
        "{}:{}",
        libc::major(details.dev()),
        libc::minor(details.dev())
    )
}

fn token_valid(value: &str) -> bool {
    value.len() == 32
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn device_valid(value: &str) -> bool {
    value.split_once(':').is_some_and(|(major, minor)| {
        !major.is_empty()
            && !minor.is_empty()
            && major.bytes().all(|byte| byte.is_ascii_digit())
            && minor.bytes().all(|byte| byte.is_ascii_digit())
    })
}

fn decode_mount_path(value: &str) -> QuotaResult<PathBuf> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'\\' {
            if index + 3 >= bytes.len()
                || !bytes[index + 1..=index + 3]
                    .iter()
                    .all(|byte| matches!(byte, b'0'..=b'7'))
            {
                return Err(QuotaError::new("quota-mountinfo-unavailable"));
            }
            let value =
                (bytes[index + 1] - b'0') * 64 + (bytes[index + 2] - b'0') * 8 + bytes[index + 3]
                    - b'0';
            decoded.push(value);
            index += 4;
        } else {
            decoded.push(bytes[index]);
            index += 1;
        }
    }
    Ok(PathBuf::from(std::ffi::OsString::from_vec(decoded)))
}

fn effective_initial_capability(number: u32) -> bool {
    let Ok(status) = fs::read_to_string("/proc/self/status") else {
        return false;
    };
    let Ok(mapping) = fs::read_to_string("/proc/self/uid_map") else {
        return false;
    };
    if mapping.split_whitespace().collect::<Vec<_>>() != ["0", "0", "4294967295"] {
        return false;
    }
    status.lines().find_map(|line| {
        line.strip_prefix("CapEff:")
            .and_then(|value| u64::from_str_radix(value.trim(), 16).ok())
            .map(|value| value & (1_u64 << number) != 0)
    }) == Some(true)
}

impl QuotaSystem {
    fn fixture(&self) -> bool {
        matches!(self, Self::Fixture { .. })
    }

    fn probe(&self, path: &Path) -> QuotaResult<QuotaMount> {
        match self {
            Self::Fixture { root } => {
                let root = fs::canonicalize(root)
                    .map_err(|_| QuotaError::new("quota-root-unavailable"))?;
                let target = fs::canonicalize(path)
                    .map_err(|_| QuotaError::new("quota-root-unavailable"))?;
                if !target.starts_with(&root) {
                    return Err(QuotaError::new("quota-mount-unavailable"));
                }
                let details =
                    fs::metadata(&root).map_err(|_| QuotaError::new("quota-root-unavailable"))?;
                if !details.is_dir() {
                    return Err(QuotaError::new("quota-root-unavailable"));
                }
                Ok(QuotaMount {
                    path: root.clone(),
                    source: root,
                    filesystem: "ext4".to_owned(),
                    device: device_number(&details),
                })
            }
            Self::Real => Self::probe_real(path),
        }
    }

    fn probe_real(path: &Path) -> QuotaResult<QuotaMount> {
        if !cfg!(target_os = "linux") {
            return Err(QuotaError::new("quota-platform-unsupported"));
        }
        if !effective_initial_capability(21) {
            return Err(QuotaError::new("quota-privilege-unavailable"));
        }
        let target =
            fs::canonicalize(path).map_err(|_| QuotaError::new("quota-root-unavailable"))?;
        let contents = fs::read_to_string("/proc/self/mountinfo")
            .map_err(|_| QuotaError::new("quota-mountinfo-unavailable"))?;
        let mut candidates = Vec::new();
        for line in contents.lines() {
            let Some((left, right)) = line.split_once(" - ") else {
                continue;
            };
            let left: Vec<_> = left.split_whitespace().collect();
            let right: Vec<_> = right.split_whitespace().collect();
            if left.len() < 6 || right.len() < 3 || !device_valid(left[2]) {
                continue;
            }
            let mount_path = decode_mount_path(left[4])?;
            if !mount_path.is_absolute() || !target.starts_with(&mount_path) {
                continue;
            }
            let source = decode_mount_path(right[1])?;
            let options: BTreeSet<_> = left[5]
                .split(',')
                .chain(right[2].split(','))
                .map(str::to_owned)
                .collect();
            candidates.push((
                mount_path,
                source,
                right[0].to_owned(),
                left[2].to_owned(),
                options,
            ));
        }
        let Some((mount_path, source, filesystem, device, options)) = candidates
            .into_iter()
            .max_by_key(|(path, ..)| path.components().count())
        else {
            return Err(QuotaError::new("quota-mount-unavailable"));
        };
        if !matches!(filesystem.as_str(), "ext4" | "xfs") {
            return Err(QuotaError::new("quota-filesystem-unsupported"));
        }
        if options.contains("ro") || !options.contains("rw") {
            return Err(QuotaError::new("quota-filesystem-read-only"));
        }
        if (!options.contains("prjquota") && !options.contains("pquota"))
            || options.contains("noquota")
            || options.contains("pqnoenforce")
        {
            return Err(QuotaError::new("quota-enforcement-unavailable"));
        }
        let source =
            fs::canonicalize(source).map_err(|_| QuotaError::new("quota-device-unavailable"))?;
        let details =
            fs::metadata(&source).map_err(|_| QuotaError::new("quota-device-unavailable"))?;
        if !details.file_type().is_block_device() {
            return Err(QuotaError::new("quota-device-unsupported"));
        }
        let source_device = format!(
            "{}:{}",
            libc::major(details.rdev()),
            libc::minor(details.rdev())
        );
        if source_device != device
            || Path::new("/sys/dev/block")
                .join(&device)
                .join("dm")
                .exists()
        {
            return Err(QuotaError::new("quota-device-unsupported"));
        }
        let mount = QuotaMount {
            path: mount_path,
            source,
            filesystem,
            device,
        };
        let _ = Self::real_get_attributes(&target)?;
        let _ = Self::real_get_quota(&mount, 0)?;
        Ok(mount)
    }

    fn open_directory(path: &Path) -> QuotaResult<File> {
        OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW)
            .open(path)
            .map_err(|error| operation_error(&error, true))
    }

    fn real_get_attributes(path: &Path) -> QuotaResult<Attributes> {
        let file = Self::open_directory(path)?;
        let mut raw = Fsxattr::default();
        // SAFETY: the descriptor names an opened directory and raw is writable for its full size.
        if unsafe { libc::ioctl(file.as_raw_fd(), FS_IOC_FSGETXATTR, &mut raw) } != 0 {
            return Err(operation_error(&io::Error::last_os_error(), true));
        }
        Ok(Attributes {
            project_id: u64::from(raw.project_id),
            inherit: raw.xflags & FS_XFLAG_PROJINHERIT != 0,
        })
    }

    fn real_set_attributes(path: &Path, selected: Attributes) -> QuotaResult<()> {
        let file = Self::open_directory(path)?;
        let mut raw = Fsxattr::default();
        if unsafe { libc::ioctl(file.as_raw_fd(), FS_IOC_FSGETXATTR, &mut raw) } != 0 {
            return Err(operation_error(&io::Error::last_os_error(), true));
        }
        raw.project_id = u32::try_from(selected.project_id)
            .map_err(|_| QuotaError::new("quota-project-id-invalid"))?;
        if selected.inherit {
            raw.xflags |= FS_XFLAG_PROJINHERIT;
        } else {
            raw.xflags &= !FS_XFLAG_PROJINHERIT;
        }
        if unsafe { libc::ioctl(file.as_raw_fd(), FS_IOC_FSSETXATTR, &raw) } != 0 {
            return Err(operation_error(&io::Error::last_os_error(), true));
        }
        if Self::real_get_attributes(path)? != selected {
            return Err(QuotaError::new("quota-project-attributes-unverified"));
        }
        Ok(())
    }

    fn quotactl<T>(
        command: i32,
        mount: &QuotaMount,
        project_id: u64,
        value: &mut T,
    ) -> QuotaResult<()> {
        let source = CString::new(mount.source.as_os_str().as_bytes())
            .map_err(|_| QuotaError::new("quota-device-unavailable"))?;
        let project_id =
            i32::try_from(project_id).map_err(|_| QuotaError::new("quota-project-id-invalid"))?;
        // SAFETY: quotactl receives a NUL-terminated source and a live typed payload.
        let result = unsafe {
            libc::syscall(
                libc::SYS_quotactl,
                qcmd(command),
                source.as_ptr(),
                project_id,
                value as *mut T,
            )
        };
        if result == 0 {
            Ok(())
        } else {
            Err(operation_error(&io::Error::last_os_error(), false))
        }
    }

    fn real_get_quota(mount: &QuotaMount, project_id: u64) -> QuotaResult<Usage> {
        if mount.filesystem == "xfs" {
            let mut raw = XfsDiskQuota {
                version: 1,
                ..XfsDiskQuota::default()
            };
            Self::quotactl(Q_XGETQUOTA, mount, project_id, &mut raw)?;
            if raw.version != 1 || raw.flags & FS_PROJ_QUOTA == 0 {
                return Err(QuotaError::new("quota-response-invalid"));
            }
            return Ok(Usage {
                hard_bytes: raw.block_hard.saturating_mul(512),
                hard_inodes: raw.inode_hard,
                used_bytes: raw.block_count.saturating_mul(512),
                used_inodes: raw.inode_count,
            });
        }
        let mut raw = Dqblk::default();
        Self::quotactl(Q_GETQUOTA, mount, project_id, &mut raw)?;
        Ok(Usage {
            hard_bytes: raw.block_hard.saturating_mul(QUOTA_BLOCK_BYTES),
            hard_inodes: raw.inode_hard,
            used_bytes: raw.current_space,
            used_inodes: raw.current_inodes,
        })
    }

    fn real_set_quota(
        mount: &QuotaMount,
        project_id: u64,
        hard_bytes: u64,
        hard_inodes: u64,
    ) -> QuotaResult<()> {
        if !hard_bytes.is_multiple_of(QUOTA_BLOCK_BYTES) {
            return Err(QuotaError::new("quota-byte-alignment-invalid"));
        }
        if mount.filesystem == "xfs" {
            let mut raw = XfsDiskQuota {
                version: 1,
                flags: FS_PROJ_QUOTA,
                fieldmask: FS_DQ_ISOFT | FS_DQ_IHARD | FS_DQ_BSOFT | FS_DQ_BHARD,
                project_id: u32::try_from(project_id)
                    .map_err(|_| QuotaError::new("quota-project-id-invalid"))?,
                block_hard: hard_bytes / 512,
                inode_hard: hard_inodes,
                ..XfsDiskQuota::default()
            };
            return Self::quotactl(Q_XSETQLIM, mount, project_id, &mut raw);
        }
        let mut raw = Dqblk {
            block_hard: hard_bytes / QUOTA_BLOCK_BYTES,
            inode_hard: hard_inodes,
            valid: QIF_BLIMITS | QIF_ILIMITS,
            ..Dqblk::default()
        };
        Self::quotactl(Q_SETQUOTA, mount, project_id, &mut raw)
    }

    fn registry_path(root: &Path) -> PathBuf {
        root.join(".agcoord-project-quota-fixture.json")
    }

    fn read_registry(root: &Path) -> QuotaResult<Value> {
        let path = Self::registry_path(root);
        let value = match fs::read(&path) {
            Ok(raw) => serde_json::from_slice(&raw)
                .map_err(|_| QuotaError::new("quota-metadata-invalid"))?,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                json!({"version": 1, "projects": {}})
            }
            Err(_) => return Err(QuotaError::new("quota-metadata-invalid")),
        };
        if value.get("version").and_then(Value::as_u64) != Some(1)
            || !value.get("projects").is_some_and(Value::is_object)
        {
            return Err(QuotaError::new("quota-metadata-invalid"));
        }
        Ok(value)
    }

    fn write_registry(root: &Path, value: &Value) -> QuotaResult<()> {
        write_json_atomic(&Self::registry_path(root), value)
    }

    fn fixture_record(registry: &Value, project_id: u64) -> Option<&Value> {
        registry
            .get("projects")?
            .as_object()?
            .get(&project_id.to_string())
    }

    fn fixture_usage(root: &Path, project_id: u64) -> QuotaResult<Usage> {
        let registry = Self::read_registry(root)?;
        let Some(record) = Self::fixture_record(&registry, project_id) else {
            return Ok(Usage::default());
        };
        let hard_bytes = record
            .get("hard_bytes")
            .and_then(Value::as_u64)
            .ok_or_else(|| QuotaError::new("quota-metadata-invalid"))?;
        let hard_inodes = record
            .get("hard_inodes")
            .and_then(Value::as_u64)
            .ok_or_else(|| QuotaError::new("quota-metadata-invalid"))?;
        let (used_bytes, used_inodes) = match record.get("path").and_then(Value::as_str) {
            Some(path) => {
                let path = Path::new(path);
                match fs::symlink_metadata(path) {
                    Ok(details)
                        if record.get("path_device").and_then(Value::as_u64)
                            == Some(details.dev())
                            && record.get("path_inode").and_then(Value::as_u64)
                                == Some(details.ino()) =>
                    {
                        scan_usage(path, details.dev())?
                    }
                    Ok(_) => return Err(QuotaError::new("quota-tree-reused")),
                    Err(error) if error.kind() == io::ErrorKind::NotFound => (0, 0),
                    Err(_) => return Err(QuotaError::new("quota-tree-unavailable")),
                }
            }
            None => (0, 0),
        };
        Ok(Usage {
            hard_bytes,
            hard_inodes,
            used_bytes,
            used_inodes,
        })
    }

    fn get_attributes(&self, path: &Path) -> QuotaResult<Attributes> {
        match self {
            Self::Real => Self::real_get_attributes(path),
            Self::Fixture { root } => {
                let details = fs::symlink_metadata(path)
                    .map_err(|_| QuotaError::new("quota-tree-unavailable"))?;
                let registry = Self::read_registry(root)?;
                for (project_id, record) in registry["projects"].as_object().unwrap() {
                    if record.get("path").and_then(Value::as_str)
                        == Some(path.to_string_lossy().as_ref())
                        && record.get("path_device").and_then(Value::as_u64) == Some(details.dev())
                        && record.get("path_inode").and_then(Value::as_u64) == Some(details.ino())
                    {
                        return Ok(Attributes {
                            project_id: project_id
                                .parse()
                                .map_err(|_| QuotaError::new("quota-metadata-invalid"))?,
                            inherit: record
                                .get("inherit")
                                .and_then(Value::as_bool)
                                .unwrap_or(false),
                        });
                    }
                }
                Ok(Attributes::default())
            }
        }
    }

    fn set_attributes(&self, path: &Path, selected: Attributes) -> QuotaResult<()> {
        match self {
            Self::Real => Self::real_set_attributes(path, selected),
            Self::Fixture { root } => {
                let details = fs::symlink_metadata(path)
                    .map_err(|_| QuotaError::new("quota-tree-unavailable"))?;
                let mut registry = Self::read_registry(root)?;
                let projects = registry["projects"].as_object_mut().unwrap();
                if selected.project_id == 0 {
                    for record in projects.values_mut() {
                        if record.get("path").and_then(Value::as_str)
                            == Some(path.to_string_lossy().as_ref())
                        {
                            record["path"] = Value::Null;
                            record["path_device"] = Value::Null;
                            record["path_inode"] = Value::Null;
                            record["inherit"] = json!(false);
                        }
                    }
                } else {
                    let record = projects
                        .get_mut(&selected.project_id.to_string())
                        .ok_or_else(|| QuotaError::new("quota-project-collision"))?;
                    record["path"] = json!(path.to_string_lossy());
                    record["path_device"] = json!(details.dev());
                    record["path_inode"] = json!(details.ino());
                    record["inherit"] = json!(selected.inherit);
                }
                Self::write_registry(root, &registry)?;
                if self.get_attributes(path)? != selected {
                    return Err(QuotaError::new("quota-project-attributes-unverified"));
                }
                Ok(())
            }
        }
    }

    fn get_quota(&self, mount: &QuotaMount, project_id: u64) -> QuotaResult<Usage> {
        match self {
            Self::Real => Self::real_get_quota(mount, project_id),
            Self::Fixture { root } => Self::fixture_usage(root, project_id),
        }
    }

    fn set_quota(
        &self,
        mount: &QuotaMount,
        project_id: u64,
        hard_bytes: u64,
        hard_inodes: u64,
    ) -> QuotaResult<()> {
        match self {
            Self::Real => Self::real_set_quota(mount, project_id, hard_bytes, hard_inodes),
            Self::Fixture { root } => {
                let mut registry = Self::read_registry(root)?;
                let projects = registry["projects"].as_object_mut().unwrap();
                if hard_bytes == 0 && hard_inodes == 0 {
                    projects.remove(&project_id.to_string());
                } else {
                    projects.insert(
                        project_id.to_string(),
                        json!({
                            "hard_bytes": hard_bytes,
                            "hard_inodes": hard_inodes,
                            "path": null,
                            "path_device": null,
                            "path_inode": null,
                            "inherit": false,
                            "mount_device": mount.device,
                        }),
                    );
                }
                Self::write_registry(root, &registry)
            }
        }
    }

    fn sync(&self, path: &Path) -> QuotaResult<()> {
        if self.fixture() {
            return Ok(());
        }
        let file = Self::open_directory(path)?;
        // SAFETY: syncfs accepts the live directory descriptor and no pointers.
        if unsafe { libc::syncfs(file.as_raw_fd()) } != 0 {
            return Err(operation_error(&io::Error::last_os_error(), false));
        }
        Ok(())
    }
}

fn scan_usage(path: &Path, expected_device: u64) -> QuotaResult<(u64, u64)> {
    let details =
        fs::symlink_metadata(path).map_err(|_| QuotaError::new("quota-tree-unavailable"))?;
    if details.dev() != expected_device {
        return Err(QuotaError::new("quota-tree-boundary-changed"));
    }
    let mut bytes = details.blocks().saturating_mul(512);
    let mut inodes = 1_u64;
    if details.is_dir() && !details.file_type().is_symlink() {
        let entries = fs::read_dir(path).map_err(|_| QuotaError::new("quota-tree-unavailable"))?;
        for entry in entries {
            let entry = entry.map_err(|_| QuotaError::new("quota-tree-unavailable"))?;
            let (child_bytes, child_inodes) = scan_usage(&entry.path(), expected_device)?;
            bytes = bytes.saturating_add(child_bytes);
            inodes = inodes.saturating_add(child_inodes);
        }
    }
    Ok((bytes, inodes))
}

fn write_json_atomic(path: &Path, value: &Value) -> QuotaResult<()> {
    let parent = path
        .parent()
        .ok_or_else(|| QuotaError::new("quota-metadata-invalid"))?;
    let name = path
        .file_name()
        .ok_or_else(|| QuotaError::new("quota-metadata-invalid"))?;
    let temporary = parent.join(format!(
        ".{}.{}.tmp",
        name.to_string_lossy(),
        random_hex(8)?
    ));
    let result = (|| {
        let mut output = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
            .open(&temporary)
            .map_err(|_| QuotaError::new("quota-metadata-unavailable"))?;
        let payload =
            serde_json::to_vec(value).map_err(|_| QuotaError::new("quota-metadata-invalid"))?;
        output
            .write_all(&payload)
            .and_then(|()| output.sync_all())
            .map_err(|_| QuotaError::new("quota-metadata-unavailable"))?;
        fs::rename(&temporary, path).map_err(|_| QuotaError::new("quota-metadata-unavailable"))?;
        File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|_| QuotaError::new("quota-metadata-unavailable"))
    })();
    let _ = fs::remove_file(&temporary);
    result
}

struct Locks {
    mount: File,
    local: File,
}

impl Drop for Locks {
    fn drop(&mut self) {
        // SAFETY: both descriptors remain live until after these unlock operations.
        unsafe {
            libc::flock(self.local.as_raw_fd(), libc::LOCK_UN);
            libc::flock(self.mount.as_raw_fd(), libc::LOCK_UN);
        }
    }
}

pub struct ProjectQuotaBackend {
    state_dir: PathBuf,
    storage_parent: PathBuf,
    metadata_dir: PathBuf,
    runs_dir: PathBuf,
    lock_path: PathBuf,
    system: QuotaSystem,
}

impl ProjectQuotaBackend {
    pub fn new(state_dir: &Path, fixture: Option<&Path>) -> QuotaResult<Self> {
        let state_dir = std::path::absolute(state_dir)
            .map_err(|_| QuotaError::new("quota-root-unavailable"))?;
        let storage_parent = state_dir
            .parent()
            .ok_or_else(|| QuotaError::new("quota-root-unavailable"))?
            .to_path_buf();
        let system = match fixture {
            Some(root) => QuotaSystem::Fixture {
                root: std::path::absolute(root)
                    .map_err(|_| QuotaError::new("quota-root-unavailable"))?,
            },
            None => QuotaSystem::Real,
        };
        Ok(Self {
            metadata_dir: state_dir.join(PROJECT_QUOTA_BACKEND),
            runs_dir: state_dir.join(PROJECT_QUOTA_BACKEND).join("runs"),
            lock_path: state_dir.join(PROJECT_QUOTA_BACKEND).join("owner.lock"),
            state_dir,
            storage_parent,
            system,
        })
    }

    pub fn capability(&self) -> Capability {
        match self.system.probe(&self.storage_parent) {
            Ok(_) => Capability {
                available: true,
                kinds: BTreeSet::from(["inodes".to_owned(), "storage".to_owned()]),
                units: BTreeSet::from(["bytes".to_owned(), "inodes".to_owned()]),
                operations: RESOURCE_OPERATIONS
                    .iter()
                    .map(|value| (*value).to_owned())
                    .collect(),
                reason: None,
            },
            Err(error) => Capability::unavailable(&error.code),
        }
    }

    fn policy(request: &QuotaRequest) -> QuotaResult<Policy> {
        let mut storage = Vec::new();
        let mut inodes = Vec::new();
        for (name, binding) in &request.bindings {
            if binding.backend.as_deref() != Some(PROJECT_QUOTA_BACKEND) {
                return Err(QuotaError::new("quota-request-invalid"));
            }
            match (binding.kind.as_str(), binding.unit.as_str()) {
                ("storage", "bytes") => storage.push(name.clone()),
                ("inodes", "inodes") => inodes.push(name.clone()),
                _ => return Err(QuotaError::new("quota-request-unsupported")),
            }
        }
        if storage.len() != 1 || inodes.len() != 1 {
            return Err(QuotaError::new("quota-policy-incomplete"));
        }
        if request.bindings[&storage[0]].mode != request.bindings[&inodes[0]].mode {
            return Err(QuotaError::new("quota-mode-mismatch"));
        }
        let hard_bytes = request.resources[&storage[0]];
        if hard_bytes < QUOTA_BLOCK_BYTES || !hard_bytes.is_multiple_of(QUOTA_BLOCK_BYTES) {
            return Err(QuotaError::new("quota-byte-alignment-invalid"));
        }
        Ok(Policy {
            storage_name: storage[0].clone(),
            inode_name: inodes[0].clone(),
            hard_bytes,
            hard_inodes: request.resources[&inodes[0]],
        })
    }

    fn prepare_paths(&self, mount: &QuotaMount) -> QuotaResult<()> {
        for path in [&self.state_dir, &self.metadata_dir, &self.runs_dir] {
            fs::create_dir_all(path).map_err(|_| QuotaError::new("quota-metadata-unavailable"))?;
            let details = fs::symlink_metadata(path)
                .map_err(|_| QuotaError::new("quota-metadata-unavailable"))?;
            if details.file_type().is_symlink()
                || !details.is_dir()
                || details.uid() != unsafe { libc::geteuid() }
                || fs::canonicalize(path).ok().as_deref() != Some(path.as_path())
            {
                return Err(QuotaError::new("quota-metadata-invalid"));
            }
            if details.mode() & 0o077 != 0 {
                fs::set_permissions(path, fs::Permissions::from_mode(0o700))
                    .map_err(|_| QuotaError::new("quota-metadata-unavailable"))?;
            }
        }
        if self.system.probe(&self.runs_dir)? != *mount {
            return Err(QuotaError::new("quota-mount-changed"));
        }
        Ok(())
    }

    fn lock(&self, mount: &QuotaMount) -> QuotaResult<Locks> {
        let mount_file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_CLOEXEC)
            .open(&mount.path)
            .map_err(|_| QuotaError::new("quota-lock-unavailable"))?;
        // SAFETY: flock operates on this live directory descriptor.
        if unsafe { libc::flock(mount_file.as_raw_fd(), libc::LOCK_EX) } != 0 {
            return Err(QuotaError::new("quota-lock-unavailable"));
        }
        let local = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .mode(0o600)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
            .open(&self.lock_path)
            .map_err(|_| QuotaError::new("quota-lock-unavailable"))?;
        local
            .set_permissions(fs::Permissions::from_mode(0o600))
            .map_err(|_| QuotaError::new("quota-lock-unavailable"))?;
        if unsafe { libc::flock(local.as_raw_fd(), libc::LOCK_EX) } != 0 {
            return Err(QuotaError::new("quota-lock-unavailable"));
        }
        Ok(Locks {
            mount: mount_file,
            local,
        })
    }

    fn manifest_path(&self, run_id: &str) -> PathBuf {
        self.metadata_dir
            .join(format!("run-{}.json", sha256_prefix(run_id.as_bytes(), 16)))
    }

    fn request_record(request: &QuotaRequest) -> Value {
        json!({
            "resources": request.resources,
            "bindings": request.bindings.iter().map(|(name, binding)| (name.clone(), binding.to_value())).collect::<Map<_, _>>(),
        })
    }

    fn mount_record(mount: &QuotaMount) -> Value {
        json!({
            "path": mount.path,
            "source": mount.source,
            "filesystem": mount.filesystem,
            "device": mount.device,
        })
    }

    fn read_manifest(path: &Path) -> QuotaResult<Value> {
        let details = fs::symlink_metadata(path).map_err(|error| {
            if error.kind() == io::ErrorKind::NotFound {
                QuotaError::new("quota-manifest-missing")
            } else {
                QuotaError::new("quota-metadata-invalid")
            }
        })?;
        if details.file_type().is_symlink()
            || !details.is_file()
            || details.uid() != unsafe { libc::geteuid() }
            || details.len() > MAX_METADATA_BYTES
        {
            return Err(QuotaError::new("quota-metadata-invalid"));
        }
        let mut input = File::open(path).map_err(|_| QuotaError::new("quota-metadata-invalid"))?;
        let mut raw = Vec::new();
        input
            .read_to_end(&mut raw)
            .map_err(|_| QuotaError::new("quota-metadata-invalid"))?;
        serde_json::from_slice(&raw).map_err(|_| QuotaError::new("quota-metadata-invalid"))
    }

    fn manifest(
        &self,
        request: &QuotaRequest,
        policy: &Policy,
        mount: &QuotaMount,
        token: &str,
        project_id: u64,
    ) -> Value {
        let run_hash = sha256_prefix(request.run_id.as_bytes(), 8);
        json!({
            "version": 1,
            "request": Self::request_record(request),
            "phase": "allocating",
            "token": token,
            "project_id": project_id,
            "path": self.runs_dir.join(format!("run-{run_hash}-{}", &token[..12])),
            "path_device": null,
            "path_inode": null,
            "original_project": null,
            "original_inherit": null,
            "mount": Self::mount_record(mount),
            "hard_bytes": policy.hard_bytes,
            "hard_inodes": policy.hard_inodes,
        })
    }

    fn validate_manifest(
        &self,
        request: &QuotaRequest,
        raw: &Value,
        mount: &QuotaMount,
    ) -> QuotaResult<Value> {
        let policy = Self::policy(request)?;
        let object = raw
            .as_object()
            .ok_or_else(|| QuotaError::new("quota-metadata-invalid"))?;
        let expected = BTreeSet::from([
            "version",
            "request",
            "phase",
            "token",
            "project_id",
            "path",
            "path_device",
            "path_inode",
            "original_project",
            "original_inherit",
            "mount",
            "hard_bytes",
            "hard_inodes",
        ]);
        if object.keys().map(String::as_str).collect::<BTreeSet<_>>() != expected
            || raw.get("version").and_then(Value::as_u64) != Some(1)
            || raw.get("request") != Some(&Self::request_record(request))
            || !matches!(
                raw.get("phase").and_then(Value::as_str),
                Some("allocating" | "allocated" | "ready" | "cleaning")
            )
            || !raw
                .get("token")
                .and_then(Value::as_str)
                .is_some_and(token_valid)
            || !raw
                .get("project_id")
                .and_then(Value::as_u64)
                .is_some_and(|value| (MIN_PROJECT_ID..MAX_PROJECT_ID).contains(&value))
            || raw.get("mount") != Some(&Self::mount_record(mount))
            || raw.get("hard_bytes").and_then(Value::as_u64) != Some(policy.hard_bytes)
            || raw.get("hard_inodes").and_then(Value::as_u64) != Some(policy.hard_inodes)
        {
            return Err(QuotaError::new("quota-metadata-invalid"));
        }
        let token = raw["token"].as_str().unwrap();
        let path = raw["path"]
            .as_str()
            .map(PathBuf::from)
            .ok_or_else(|| QuotaError::new("quota-metadata-invalid"))?;
        if !path.is_absolute()
            || path.parent() != Some(&self.runs_dir)
            || !path
                .file_name()
                .is_some_and(|name| name.to_string_lossy().ends_with(&token[..12]))
        {
            return Err(QuotaError::new("quota-metadata-invalid"));
        }
        let identity = ["path_device", "path_inode", "original_project"];
        if raw["path_device"].is_null() {
            if identity.iter().any(|name| !raw[*name].is_null())
                || !raw["original_inherit"].is_null()
            {
                return Err(QuotaError::new("quota-metadata-invalid"));
            }
        } else if identity.iter().any(|name| raw[*name].as_u64().is_none())
            || raw["original_inherit"].as_bool().is_none()
        {
            return Err(QuotaError::new("quota-metadata-invalid"));
        }
        Ok(raw.clone())
    }

    fn handle(manifest: &Value) -> Value {
        json!({
            "version": 1,
            "token": manifest["token"],
            "project_id": manifest["project_id"],
            "path": manifest["path"],
            "path_device": manifest["path_device"],
            "path_inode": manifest["path_inode"],
            "filesystem": manifest["mount"]["filesystem"],
            "mount_device": manifest["mount"]["device"],
            "hard_bytes": manifest["hard_bytes"],
            "hard_inodes": manifest["hard_inodes"],
        })
    }

    fn validate_handle(&self, raw: &Value) -> QuotaResult<Value> {
        let object = raw
            .as_object()
            .ok_or_else(|| QuotaError::new("quota-handle-invalid"))?;
        let expected = BTreeSet::from([
            "version",
            "token",
            "project_id",
            "path",
            "path_device",
            "path_inode",
            "filesystem",
            "mount_device",
            "hard_bytes",
            "hard_inodes",
        ]);
        if object.keys().map(String::as_str).collect::<BTreeSet<_>>() != expected
            || raw["version"].as_u64() != Some(1)
            || !raw["token"].as_str().is_some_and(token_valid)
            || !raw["project_id"]
                .as_u64()
                .is_some_and(|value| (MIN_PROJECT_ID..MAX_PROJECT_ID).contains(&value))
            || !matches!(raw["filesystem"].as_str(), Some("ext4" | "xfs"))
            || !raw["mount_device"].as_str().is_some_and(device_valid)
            || ["path_device", "path_inode", "hard_bytes", "hard_inodes"]
                .iter()
                .any(|name| raw[*name].as_u64().is_none_or(|value| value == 0))
        {
            return Err(QuotaError::new("quota-handle-invalid"));
        }
        let path = raw["path"]
            .as_str()
            .map(PathBuf::from)
            .ok_or_else(|| QuotaError::new("quota-handle-invalid"))?;
        let token = raw["token"].as_str().unwrap();
        if !path.is_absolute()
            || path.parent() != Some(&self.runs_dir)
            || !path
                .file_name()
                .is_some_and(|name| name.to_string_lossy().ends_with(&token[..12]))
        {
            return Err(QuotaError::new("quota-handle-invalid"));
        }
        Ok(raw.clone())
    }

    fn validate_tree(&self, manifest: &Value) -> QuotaResult<fs::Metadata> {
        let path = Path::new(manifest["path"].as_str().unwrap());
        let details =
            fs::symlink_metadata(path).map_err(|_| QuotaError::new("quota-tree-missing"))?;
        if details.file_type().is_symlink()
            || !details.is_dir()
            || details.uid() != unsafe { libc::geteuid() }
            || manifest["path_device"].as_u64() != Some(details.dev())
            || manifest["path_inode"].as_u64() != Some(details.ino())
            || fs::canonicalize(path).ok().as_deref() != Some(path)
        {
            return Err(QuotaError::new("quota-tree-reused"));
        }
        if details.mode() & 0o077 != 0 {
            return Err(QuotaError::new("quota-tree-permissions-changed"));
        }
        Ok(details)
    }

    fn allocate_project(&self, mount: &QuotaMount) -> QuotaResult<u64> {
        let mut occupied = BTreeSet::new();
        for entry in fs::read_dir(&self.metadata_dir)
            .map_err(|_| QuotaError::new("quota-metadata-unavailable"))?
        {
            let entry = entry.map_err(|_| QuotaError::new("quota-metadata-invalid"))?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if !name.starts_with("run-") || !name.ends_with(".json") {
                continue;
            }
            let raw = Self::read_manifest(&entry.path())?;
            let project_id = raw["project_id"]
                .as_u64()
                .ok_or_else(|| QuotaError::new("quota-metadata-invalid"))?;
            occupied.insert(project_id);
        }
        for _ in 0..PROJECT_ATTEMPTS {
            let candidate = random_project_id()?;
            if !occupied.contains(&candidate)
                && self.system.get_quota(mount, candidate)? == Usage::default()
            {
                return Ok(candidate);
            }
        }
        Err(QuotaError::new("quota-project-id-exhausted"))
    }

    fn complete_manifest(
        &self,
        manifest_path: &Path,
        mut manifest: Value,
        mount: &QuotaMount,
    ) -> QuotaResult<Value> {
        let path = PathBuf::from(manifest["path"].as_str().unwrap());
        if manifest["path_device"].is_null() {
            match fs::create_dir(&path) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                    let details = fs::symlink_metadata(&path)
                        .map_err(|_| QuotaError::new("quota-tree-collision"))?;
                    if details.file_type().is_symlink()
                        || !details.is_dir()
                        || details.uid() != unsafe { libc::geteuid() }
                        || fs::read_dir(&path)
                            .ok()
                            .and_then(|mut entries| entries.next())
                            .is_some()
                        || fs::canonicalize(&path).ok().as_deref() != Some(path.as_path())
                    {
                        return Err(QuotaError::new("quota-tree-collision"));
                    }
                }
                Err(_) => return Err(QuotaError::new("quota-tree-collision")),
            }
            fs::set_permissions(&path, fs::Permissions::from_mode(0o700))
                .map_err(|_| QuotaError::new("quota-tree-unavailable"))?;
            let details = fs::symlink_metadata(&path)
                .map_err(|_| QuotaError::new("quota-tree-unavailable"))?;
            let original = self.system.get_attributes(&path)?;
            manifest["phase"] = json!("allocated");
            manifest["path_device"] = json!(details.dev());
            manifest["path_inode"] = json!(details.ino());
            manifest["original_project"] = json!(original.project_id);
            manifest["original_inherit"] = json!(original.inherit);
            write_json_atomic(manifest_path, &manifest)?;
        }
        self.validate_tree(&manifest)?;
        let selected = Attributes {
            project_id: manifest["project_id"].as_u64().unwrap(),
            inherit: true,
        };
        let desired = Usage {
            hard_bytes: manifest["hard_bytes"].as_u64().unwrap(),
            hard_inodes: manifest["hard_inodes"].as_u64().unwrap(),
            ..Usage::default()
        };
        let quota = self.system.get_quota(mount, selected.project_id)?;
        if quota == Usage::default() {
            self.system.set_quota(
                mount,
                selected.project_id,
                desired.hard_bytes,
                desired.hard_inodes,
            )?;
        } else if quota.hard_bytes != desired.hard_bytes
            || quota.hard_inodes != desired.hard_inodes
            || quota.used_bytes > quota.hard_bytes
            || quota.used_inodes > quota.hard_inodes
        {
            return Err(QuotaError::new("quota-project-collision"));
        }
        let attributes = self.system.get_attributes(&path)?;
        let original = Attributes {
            project_id: manifest["original_project"].as_u64().unwrap(),
            inherit: manifest["original_inherit"].as_bool().unwrap(),
        };
        if attributes == original {
            self.system.set_attributes(&path, selected)?;
        } else if attributes != selected {
            return Err(QuotaError::new("quota-tree-attributes-changed"));
        }
        self.system.sync(&path)?;
        let quota = self.system.get_quota(mount, selected.project_id)?;
        if quota.hard_bytes != desired.hard_bytes
            || quota.hard_inodes != desired.hard_inodes
            || quota.used_bytes > quota.hard_bytes
            || quota.used_inodes > quota.hard_inodes
            || self.system.get_attributes(&path)? != selected
        {
            return Err(QuotaError::new("quota-enforcement-unverified"));
        }
        manifest["phase"] = json!("ready");
        write_json_atomic(manifest_path, &manifest)?;
        Ok(Self::handle(&manifest))
    }

    fn rollback(
        &self,
        manifest_path: &Path,
        manifest: &Value,
        mount: &QuotaMount,
    ) -> QuotaResult<()> {
        let path = Path::new(
            manifest["path"]
                .as_str()
                .ok_or_else(|| QuotaError::new("quota-rollback-refused"))?,
        );
        if let Ok(details) = fs::symlink_metadata(path) {
            if manifest["path_device"].as_u64().is_some()
                && (manifest["path_device"].as_u64() != Some(details.dev())
                    || manifest["path_inode"].as_u64() != Some(details.ino()))
            {
                return Err(QuotaError::new("quota-rollback-refused"));
            }
            make_deletable(path, details.dev())?;
            fs::remove_dir_all(path).map_err(|_| QuotaError::new("quota-rollback-refused"))?;
        }
        let project_id = manifest["project_id"]
            .as_u64()
            .ok_or_else(|| QuotaError::new("quota-rollback-refused"))?;
        let quota = self.system.get_quota(mount, project_id)?;
        if quota.used_bytes != 0 || quota.used_inodes != 0 {
            return Err(QuotaError::new("quota-rollback-refused"));
        }
        if quota.hard_bytes == manifest["hard_bytes"].as_u64().unwrap_or(0)
            && quota.hard_inodes == manifest["hard_inodes"].as_u64().unwrap_or(0)
        {
            self.system.set_quota(mount, project_id, 0, 0)?;
        } else if quota.hard_bytes != 0 || quota.hard_inodes != 0 {
            return Err(QuotaError::new("quota-rollback-refused"));
        }
        if self.system.get_quota(mount, project_id)? != Usage::default() {
            return Err(QuotaError::new("quota-rollback-refused"));
        }
        match fs::remove_file(manifest_path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(_) => Err(QuotaError::new("quota-rollback-refused")),
        }
    }

    pub fn prepare(&mut self, request: &QuotaRequest) -> QuotaResult<Value> {
        let policy = Self::policy(request)?;
        let mount = self.system.probe(&self.storage_parent)?;
        self.prepare_paths(&mount)?;
        let _locks = self.lock(&mount)?;
        let manifest_path = self.manifest_path(&request.run_id);
        let manifest = match Self::read_manifest(&manifest_path) {
            Ok(raw) => self.validate_manifest(request, &raw, &mount)?,
            Err(error) if error.code == "quota-manifest-missing" => {
                let project_id = self.allocate_project(&mount)?;
                let manifest =
                    self.manifest(request, &policy, &mount, &random_hex(16)?, project_id);
                write_json_atomic(&manifest_path, &manifest)?;
                manifest
            }
            Err(error) => return Err(error),
        };
        match self.complete_manifest(&manifest_path, manifest, &mount) {
            Ok(handle) => Ok(handle),
            Err(error) => {
                if let Ok(current) = Self::read_manifest(&manifest_path) {
                    let _ = self.rollback(&manifest_path, &current, &mount);
                }
                Err(error)
            }
        }
    }

    fn resolve(
        &self,
        request: &QuotaRequest,
        raw_handle: &Value,
        require_path: bool,
    ) -> QuotaResult<(Value, Option<Value>, QuotaMount)> {
        let _ = Self::policy(request)?;
        let handle = self.validate_handle(raw_handle)?;
        let mount = self.system.probe(&self.storage_parent)?;
        self.prepare_paths(&mount)?;
        if handle["filesystem"].as_str() != Some(&mount.filesystem)
            || handle["mount_device"].as_str() != Some(&mount.device)
        {
            return Err(QuotaError::new("quota-mount-changed"));
        }
        let manifest_path = self.manifest_path(&request.run_id);
        let manifest = match Self::read_manifest(&manifest_path) {
            Ok(raw) => Some(self.validate_manifest(request, &raw, &mount)?),
            Err(error) if error.code == "quota-manifest-missing" => {
                let path = Path::new(handle["path"].as_str().unwrap());
                if require_path || fs::symlink_metadata(path).is_ok() {
                    return Err(QuotaError::new("quota-manifest-missing"));
                }
                None
            }
            Err(error) => return Err(error),
        };
        if let Some(manifest) = &manifest {
            if Self::handle(manifest) != handle {
                return Err(QuotaError::new("quota-handle-mismatch"));
            }
            if require_path {
                self.validate_tree(manifest)?;
            }
        }
        Ok((handle, manifest, mount))
    }

    fn verify_enforcement(&self, handle: &Value, mount: &QuotaMount) -> QuotaResult<Usage> {
        let path = Path::new(handle["path"].as_str().unwrap());
        let selected = Attributes {
            project_id: handle["project_id"].as_u64().unwrap(),
            inherit: true,
        };
        if self.system.get_attributes(path)? != selected {
            return Err(QuotaError::new("quota-tree-attributes-changed"));
        }
        let quota = self.system.get_quota(mount, selected.project_id)?;
        if quota.hard_bytes != handle["hard_bytes"].as_u64().unwrap()
            || quota.hard_inodes != handle["hard_inodes"].as_u64().unwrap()
            || quota.used_bytes > quota.hard_bytes
            || quota.used_inodes > quota.hard_inodes
        {
            return Err(QuotaError::new("quota-enforcement-changed"));
        }
        Ok(quota)
    }

    pub fn scratch_path(&self, request: &QuotaRequest, handle: &Value) -> QuotaResult<PathBuf> {
        let (handle, manifest, mount) = self.resolve(request, handle, true)?;
        if manifest.as_ref().and_then(|value| value["phase"].as_str()) != Some("ready") {
            return Err(QuotaError::new("quota-tree-not-ready"));
        }
        let _ = self.verify_enforcement(&handle, &mount)?;
        Ok(PathBuf::from(handle["path"].as_str().unwrap()))
    }

    pub fn attach(
        &self,
        request: &QuotaRequest,
        handle: &Value,
        worker_pid: u32,
    ) -> QuotaResult<()> {
        let _ = Self::policy(request)?;
        let _ = self.validate_handle(handle)?;
        if worker_pid == 0 {
            return Err(QuotaError::new("quota-worker-invalid"));
        }
        Ok(())
    }

    fn measurement(&self, request: &QuotaRequest, handle: &Value) -> QuotaResult<Measurement> {
        let policy = Self::policy(request)?;
        let (handle, _, mount) = self.resolve(request, handle, true)?;
        let usage = self.verify_enforcement(&handle, &mount)?;
        let mut observations = Vec::new();
        if usage.used_bytes >= usage.hard_bytes {
            observations.push(Observation {
                resource: policy.storage_name.clone(),
                code: "storage-byte-limit-hit".to_owned(),
            });
        }
        if usage.used_inodes >= usage.hard_inodes {
            observations.push(Observation {
                resource: policy.inode_name.clone(),
                code: "storage-inode-limit-hit".to_owned(),
            });
        }
        Ok(Measurement {
            peak: BTreeMap::from([
                (policy.storage_name, usage.used_bytes),
                (policy.inode_name, usage.used_inodes),
            ]),
            observations,
        })
    }

    pub fn usage(&self, request: &QuotaRequest, handle: &Value) -> QuotaResult<Measurement> {
        self.measurement(request, handle)
    }

    pub fn finish(&self, request: &QuotaRequest, handle: &Value) -> QuotaResult<Measurement> {
        self.measurement(request, handle)
    }

    pub fn cancel(&self, request: &QuotaRequest, handle: &Value) -> QuotaResult<()> {
        let _ = self.resolve(request, handle, true)?;
        Ok(())
    }

    pub fn validate_recovery(&self, request: &QuotaRequest, handle: &Value) -> QuotaResult<()> {
        let (handle, manifest, mount) = self.resolve(request, handle, false)?;
        if manifest.is_some() {
            let _ = self.verify_enforcement(&handle, &mount)?;
        }
        Ok(())
    }

    pub fn cleanup(&mut self, request: &QuotaRequest, raw_handle: &Value) -> QuotaResult<()> {
        let mount = self.system.probe(&self.storage_parent)?;
        self.prepare_paths(&mount)?;
        let _locks = self.lock(&mount)?;
        let (handle, mut manifest, mount) = self.resolve(request, raw_handle, false)?;
        let path = PathBuf::from(handle["path"].as_str().unwrap());
        if let Some(value) = manifest.as_mut() {
            value["phase"] = json!("cleaning");
            write_json_atomic(&self.manifest_path(&request.run_id), value)?;
        }
        match fs::symlink_metadata(&path) {
            Ok(details) => {
                let manifest = manifest
                    .as_ref()
                    .ok_or_else(|| QuotaError::new("quota-manifest-missing"))?;
                self.validate_tree(manifest)?;
                let _ = self.verify_enforcement(&handle, &mount)?;
                make_deletable(&path, details.dev())?;
                fs::remove_dir_all(&path)
                    .map_err(|_| QuotaError::new("quota-tree-cleanup-failed"))?;
                self.system.sync(&mount.path)?;
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(_) => return Err(QuotaError::new("quota-tree-unavailable")),
        }
        let project_id = handle["project_id"].as_u64().unwrap();
        let deadline = Instant::now() + CLEANUP_TIMEOUT;
        let quota = loop {
            let quota = self.system.get_quota(&mount, project_id)?;
            if quota.used_bytes == 0 && quota.used_inodes == 0 {
                break quota;
            }
            if Instant::now() >= deadline {
                return Err(QuotaError::new("quota-usage-still-live"));
            }
            thread::sleep(Duration::from_millis(20));
        };
        if quota.hard_bytes == handle["hard_bytes"].as_u64().unwrap()
            && quota.hard_inodes == handle["hard_inodes"].as_u64().unwrap()
        {
            self.system.set_quota(&mount, project_id, 0, 0)?;
        } else if quota.hard_bytes != 0 || quota.hard_inodes != 0 {
            return Err(QuotaError::new("quota-enforcement-changed"));
        }
        if self.system.get_quota(&mount, project_id)? != Usage::default() {
            return Err(QuotaError::new("quota-cleanup-unverified"));
        }
        match fs::remove_file(self.manifest_path(&request.run_id)) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(_) => return Err(QuotaError::new("quota-metadata-unavailable")),
        }
        Ok(())
    }
}

fn make_deletable(path: &Path, expected_device: u64) -> QuotaResult<()> {
    let details =
        fs::symlink_metadata(path).map_err(|_| QuotaError::new("quota-tree-cleanup-failed"))?;
    if details.dev() != expected_device {
        return Err(QuotaError::new("quota-tree-boundary-changed"));
    }
    if details.is_dir() && !details.file_type().is_symlink() {
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))
            .map_err(|_| QuotaError::new("quota-tree-cleanup-failed"))?;
        for entry in fs::read_dir(path).map_err(|_| QuotaError::new("quota-tree-cleanup-failed"))? {
            let entry = entry.map_err(|_| QuotaError::new("quota-tree-cleanup-failed"))?;
            make_deletable(&entry.path(), expected_device)?;
        }
    }
    Ok(())
}
