use crate::cgroup::{
    self, TmpfsBaseline, TmpfsSetup, isolate_current_cgroup, mount_current_tmpfs,
    unmount_current_tmpfs,
};
use crate::error::{AppError, Result};
use crate::platform::{process_start_token, same_worker_process, signal_process_group};
use serde_json::json;
use std::collections::BTreeMap;
use std::ffi::CString;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::os::fd::{AsRawFd, FromRawFd, RawFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::ptr;
use std::thread;
use std::time::{Duration, Instant};

const TOKEN_BYTES: usize = 32;
const HELLO_BYTES: usize = 44;
const CONTROL_BYTES: usize = 37;
const SETUP_BYTES: usize = 37;
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
const ABORT_GRACE: Duration = Duration::from_millis(500);
const HELLO_MAGIC: &[u8; 4] = b"AGH1";
const CONTROL_MAGIC: &[u8; 4] = b"AGC1";
const SETUP_MAGIC: &[u8; 4] = b"AGS1";
const INITIAL_RELEASE: u8 = 1;
const FINAL_RELEASE: u8 = 2;
const LINUX_CAPABILITY_VERSION_3: u32 = 0x2008_0522;
const PR_SET_NO_NEW_PRIVS: libc::c_int = 38;
const PR_CAP_AMBIENT: libc::c_int = 47;
const PR_CAP_AMBIENT_CLEAR_ALL: libc::c_ulong = 4;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkerFault {
    LauncherDeath,
    HelloToken,
    ReplayedToken,
    SubstitutedChannel,
    SetupToken,
    PrivilegeVerification,
    RetainedDescriptor,
    FinalToken,
    TmpfsMountUnavailable,
    NamespaceIsolationUnavailable,
}

impl WorkerFault {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "launcher-death" => Some(Self::LauncherDeath),
            "hello-token" => Some(Self::HelloToken),
            "replayed-token" => Some(Self::ReplayedToken),
            "substituted-channel" => Some(Self::SubstitutedChannel),
            "setup-token" => Some(Self::SetupToken),
            "privilege-verification" => Some(Self::PrivilegeVerification),
            "retained-descriptor" => Some(Self::RetainedDescriptor),
            "final-token" => Some(Self::FinalToken),
            "tmpfs-mount-unavailable" => Some(Self::TmpfsMountUnavailable),
            "namespace-isolation-unavailable" => Some(Self::NamespaceIsolationUnavailable),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
enum SetupCode {
    Ok = 0,
    PrivilegeDropFailed = 1,
    PrivilegeDropUnverified = 2,
    DescriptorLeak = 3,
    NamespaceDelegationUnavailable = 4,
    NamespaceIsolationUnavailable = 5,
    NamespaceMappingFailed = 6,
    NamespaceMountFailed = 7,
    NamespaceVerificationFailed = 8,
    ControllerFilesExposed = 9,
    TmpfsNamespaceRequired = 10,
    TmpfsTargetInvalid = 11,
    TmpfsMountUnavailable = 12,
    TmpfsMountUnverified = 13,
    TmpfsSizeUnverified = 14,
    TmpfsInodesUnverified = 15,
    TmpfsSetupFailed = 16,
    TmpfsReportFailed = 17,
}

impl SetupCode {
    fn from_byte(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::Ok),
            1 => Some(Self::PrivilegeDropFailed),
            2 => Some(Self::PrivilegeDropUnverified),
            3 => Some(Self::DescriptorLeak),
            4 => Some(Self::NamespaceDelegationUnavailable),
            5 => Some(Self::NamespaceIsolationUnavailable),
            6 => Some(Self::NamespaceMappingFailed),
            7 => Some(Self::NamespaceMountFailed),
            8 => Some(Self::NamespaceVerificationFailed),
            9 => Some(Self::ControllerFilesExposed),
            10 => Some(Self::TmpfsNamespaceRequired),
            11 => Some(Self::TmpfsTargetInvalid),
            12 => Some(Self::TmpfsMountUnavailable),
            13 => Some(Self::TmpfsMountUnverified),
            14 => Some(Self::TmpfsSizeUnverified),
            15 => Some(Self::TmpfsInodesUnverified),
            16 => Some(Self::TmpfsSetupFailed),
            17 => Some(Self::TmpfsReportFailed),
            _ => None,
        }
    }

    fn error(self) -> Option<AppError> {
        match self {
            Self::Ok => None,
            Self::PrivilegeDropFailed => Some(AppError::new(
                "worker-privilege-drop-failed",
                "native worker could not clear its privilege state",
            )),
            Self::PrivilegeDropUnverified => Some(AppError::new(
                "worker-privilege-drop-unverified",
                "native worker privilege state could not be verified",
            )),
            Self::DescriptorLeak => Some(AppError::new(
                "worker-descriptor-leak",
                "native worker retained an internal descriptor before exec",
            )),
            code => Some(AppError::new(
                code.stable_code().unwrap(),
                "native worker resource setup could not be verified",
            )),
        }
    }

    fn stable_code(self) -> Option<&'static str> {
        match self {
            Self::Ok => None,
            Self::PrivilegeDropFailed => Some("worker-privilege-drop-failed"),
            Self::PrivilegeDropUnverified => Some("worker-privilege-drop-unverified"),
            Self::DescriptorLeak => Some("worker-descriptor-leak"),
            Self::NamespaceDelegationUnavailable => Some("namespace-delegation-unavailable"),
            Self::NamespaceIsolationUnavailable => Some("namespace-isolation-unavailable"),
            Self::NamespaceMappingFailed => Some("namespace-mapping-failed"),
            Self::NamespaceMountFailed => Some("namespace-mount-failed"),
            Self::NamespaceVerificationFailed => Some("namespace-verification-failed"),
            Self::ControllerFilesExposed => Some("controller-files-exposed"),
            Self::TmpfsNamespaceRequired => Some("tmpfs-namespace-required"),
            Self::TmpfsTargetInvalid => Some("tmpfs-target-invalid"),
            Self::TmpfsMountUnavailable => Some("tmpfs-mount-unavailable"),
            Self::TmpfsMountUnverified => Some("tmpfs-mount-unverified"),
            Self::TmpfsSizeUnverified => Some("tmpfs-size-unverified"),
            Self::TmpfsInodesUnverified => Some("tmpfs-inodes-unverified"),
            Self::TmpfsSetupFailed => Some("tmpfs-setup-failed"),
            Self::TmpfsReportFailed => Some("tmpfs-report-failed"),
        }
    }

    fn from_resource_error(code: &str) -> Self {
        if code.starts_with("namespace-cgroup2-mount-failed-errno-") {
            return Self::NamespaceMountFailed;
        }
        match code {
            "namespace-delegation-unavailable" => Self::NamespaceDelegationUnavailable,
            "namespace-isolation-unavailable" => Self::NamespaceIsolationUnavailable,
            "namespace-mapping-failed" => Self::NamespaceMappingFailed,
            "namespace-mount-failed"
            | "namespace-propagation-mount-failed"
            | "namespace-cgroup2-mount-failed" => Self::NamespaceMountFailed,
            "namespace-verification-failed" => Self::NamespaceVerificationFailed,
            "controller-files-exposed" => Self::ControllerFilesExposed,
            "tmpfs-namespace-required" => Self::TmpfsNamespaceRequired,
            "tmpfs-target-invalid" => Self::TmpfsTargetInvalid,
            "tmpfs-mount-unavailable" => Self::TmpfsMountUnavailable,
            "tmpfs-mount-unverified" => Self::TmpfsMountUnverified,
            "tmpfs-size-unverified" => Self::TmpfsSizeUnverified,
            "tmpfs-inodes-unverified" => Self::TmpfsInodesUnverified,
            _ => Self::TmpfsSetupFailed,
        }
    }

    fn resource_failure(self) -> Option<&'static str> {
        match self {
            Self::Ok
            | Self::PrivilegeDropFailed
            | Self::PrivilegeDropUnverified
            | Self::DescriptorLeak => None,
            code => code.stable_code(),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct WorkerSetup {
    pub isolate_cgroup: bool,
    pub tmpfs: Option<TmpfsSetup>,
    pub project_quota: Option<PathBuf>,
}

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

struct ExecPlan {
    executable: CString,
    _arguments: Vec<CString>,
    argument_pointers: Vec<*const libc::c_char>,
    _environment: Vec<CString>,
    environment_pointers: Vec<*const libc::c_char>,
    checkout: CString,
    log_fd: RawFd,
    fault: Option<WorkerFault>,
    setup: WorkerSetup,
}

impl ExecPlan {
    fn new(
        command: &[String],
        environment: &BTreeMap<String, String>,
        checkout: &Path,
        log: &File,
        fault: Option<WorkerFault>,
        setup: WorkerSetup,
    ) -> Result<Self> {
        let executable = resolve_executable(&command[0], environment, checkout)?;
        let arguments = command
            .iter()
            .map(|argument| {
                CString::new(argument.as_bytes()).map_err(|_| {
                    AppError::new(
                        "broker-worker-start-failed",
                        "worker command contains an invalid NUL byte",
                    )
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let environment = environment
            .iter()
            .map(|(name, value)| {
                CString::new(format!("{name}={value}").into_bytes()).map_err(|_| {
                    AppError::new(
                        "broker-worker-start-failed",
                        "worker environment contains an invalid NUL byte",
                    )
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let checkout = CString::new(checkout.as_os_str().as_bytes()).map_err(|_| {
            AppError::new(
                "broker-worker-start-failed",
                "worker checkout contains an invalid NUL byte",
            )
        })?;
        let mut argument_pointers: Vec<_> = arguments.iter().map(|value| value.as_ptr()).collect();
        argument_pointers.push(ptr::null());
        let mut environment_pointers: Vec<_> =
            environment.iter().map(|value| value.as_ptr()).collect();
        environment_pointers.push(ptr::null());
        Ok(Self {
            executable,
            _arguments: arguments,
            argument_pointers,
            _environment: environment,
            environment_pointers,
            checkout,
            log_fd: log.as_raw_fd(),
            fault,
            setup,
        })
    }
}

fn resolve_executable(
    command: &str,
    environment: &BTreeMap<String, String>,
    checkout: &Path,
) -> Result<CString> {
    if command.as_bytes().contains(&b'/') {
        let selected = PathBuf::from(command);
        let candidate = if selected.is_absolute() {
            selected
        } else {
            checkout.join(selected)
        };
        let valid = fs::metadata(&candidate)
            .is_ok_and(|metadata| metadata.is_file() && metadata.permissions().mode() & 0o111 != 0);
        if !valid {
            return Err(AppError::new(
                "broker-worker-start-failed",
                "worker executable path is unavailable or not executable",
            ));
        }
        return CString::new(candidate.as_os_str().as_bytes()).map_err(|_| {
            AppError::new(
                "broker-worker-start-failed",
                "worker executable contains an invalid NUL byte",
            )
        });
    }
    let path = environment
        .get("PATH")
        .map(String::as_str)
        .unwrap_or("/usr/bin:/bin");
    for component in path.split(':') {
        let base = if component.is_empty() {
            checkout.to_owned()
        } else {
            let selected = PathBuf::from(component);
            if selected.is_absolute() {
                selected
            } else {
                checkout.join(selected)
            }
        };
        let candidate = base.join(command);
        let Ok(metadata) = fs::metadata(&candidate) else {
            continue;
        };
        if metadata.is_file() && metadata.permissions().mode() & 0o111 != 0 {
            return CString::new(candidate.as_os_str().as_bytes()).map_err(|_| {
                AppError::new(
                    "broker-worker-start-failed",
                    "resolved worker executable contains an invalid NUL byte",
                )
            });
        }
    }
    Err(AppError::new(
        "broker-worker-start-failed",
        "worker executable is unavailable on the submitted PATH",
    ))
}

fn create_pipe() -> io::Result<(RawFd, RawFd)> {
    let mut descriptors = [-1, -1];
    // SAFETY: `descriptors` points to two writable integers and pipe2 initializes both
    // on success. O_CLOEXEC makes every inherited endpoint fail closed at exec.
    let result = unsafe { libc::pipe2(descriptors.as_mut_ptr(), libc::O_CLOEXEC) };
    if result == 0 {
        Ok((descriptors[0], descriptors[1]))
    } else {
        Err(io::Error::last_os_error())
    }
}

fn random_token() -> Result<[u8; TOKEN_BYTES]> {
    let mut token = [0_u8; TOKEN_BYTES];
    let mut written = 0;
    while written < token.len() {
        // SAFETY: the pointer names the unwritten suffix of the fixed token buffer.
        let result = unsafe {
            libc::getrandom(
                token[written..].as_mut_ptr().cast(),
                token.len() - written,
                0,
            )
        };
        if result > 0 {
            written += result as usize;
            continue;
        }
        if result < 0 && io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
            continue;
        }
        return Err(AppError::new(
            "broker-worker-start-failed",
            "kernel randomness is unavailable for the worker handshake",
        ));
    }
    Ok(token)
}

fn altered(mut token: [u8; TOKEN_BYTES]) -> [u8; TOKEN_BYTES] {
    token[0] ^= 0xff;
    token
}

fn hello_message(token: &[u8; TOKEN_BYTES], pid: u32, process_group: u32) -> [u8; HELLO_BYTES] {
    let mut message = [0_u8; HELLO_BYTES];
    message[..4].copy_from_slice(HELLO_MAGIC);
    message[4..36].copy_from_slice(token);
    message[36..40].copy_from_slice(&pid.to_be_bytes());
    message[40..44].copy_from_slice(&process_group.to_be_bytes());
    message
}

fn control_message(token: &[u8; TOKEN_BYTES], stage: u8) -> [u8; CONTROL_BYTES] {
    let mut message = [0_u8; CONTROL_BYTES];
    message[..4].copy_from_slice(CONTROL_MAGIC);
    message[4] = stage;
    message[5..].copy_from_slice(token);
    message
}

fn setup_message(token: &[u8; TOKEN_BYTES], code: SetupCode) -> [u8; SETUP_BYTES] {
    let mut message = [0_u8; SETUP_BYTES];
    message[..4].copy_from_slice(SETUP_MAGIC);
    message[4..36].copy_from_slice(token);
    message[36] = code as u8;
    message
}

fn write_all_fd(descriptor: RawFd, mut payload: &[u8]) -> bool {
    while !payload.is_empty() {
        // SAFETY: the descriptor is inherited by this process and the slice points to
        // initialized bytes for the duration of the syscall.
        let written = unsafe { libc::write(descriptor, payload.as_ptr().cast(), payload.len()) };
        if written > 0 {
            payload = &payload[written as usize..];
            continue;
        }
        if written < 0 && io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
            continue;
        }
        return false;
    }
    true
}

fn read_exact_fd(descriptor: RawFd, mut payload: &mut [u8]) -> bool {
    while !payload.is_empty() {
        // SAFETY: the descriptor is inherited by this process and the mutable slice is
        // valid for the exact requested byte count.
        let read = unsafe { libc::read(descriptor, payload.as_mut_ptr().cast(), payload.len()) };
        if read > 0 {
            let (_, rest) = payload.split_at_mut(read as usize);
            payload = rest;
            continue;
        }
        if read < 0 && io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
            continue;
        }
        return false;
    }
    true
}

fn read_exact_timeout(descriptor: RawFd, payload: &mut [u8]) -> io::Result<()> {
    let deadline = Instant::now() + HANDSHAKE_TIMEOUT;
    let mut offset = 0;
    while offset < payload.len() {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "worker handshake timed out",
            ));
        }
        let timeout = i32::try_from(remaining.as_millis().max(1)).unwrap_or(i32::MAX);
        let mut poll = libc::pollfd {
            fd: descriptor,
            events: libc::POLLIN | libc::POLLHUP,
            revents: 0,
        };
        // SAFETY: `poll` points to one initialized pollfd for the duration of the call.
        let ready = unsafe { libc::poll(&mut poll, 1, timeout) };
        if ready == 0 {
            continue;
        }
        if ready < 0 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(error);
        }
        // SAFETY: the descriptor and unwritten suffix are valid for this syscall.
        let read = unsafe {
            libc::read(
                descriptor,
                payload[offset..].as_mut_ptr().cast(),
                payload.len() - offset,
            )
        };
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "worker handshake channel closed",
            ));
        }
        if read < 0 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(error);
        }
        offset += read as usize;
    }
    Ok(())
}

fn fallback_close_except(keep: &[RawFd]) {
    let descriptors: Vec<_> = fs::read_dir("/proc/self/fd")
        .ok()
        .into_iter()
        .flatten()
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| entry.file_name().to_string_lossy().parse::<RawFd>().ok())
        .collect();
    for descriptor in descriptors {
        if descriptor >= 3 && !keep.contains(&descriptor) {
            // SAFETY: closing an enumerated descriptor is idempotent from this child;
            // failures simply mean it was already closed.
            let _ = unsafe { libc::close(descriptor) };
        }
    }
}

fn close_range(first: u32, last: u32) -> io::Result<()> {
    if first > last {
        return Ok(());
    }
    // SAFETY: close_range accepts only numeric descriptor bounds and no pointers.
    let result = unsafe { libc::syscall(libc::SYS_close_range, first, last, 0) };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

fn close_all_except(keep: &[RawFd]) {
    let mut selected: Vec<u32> = keep
        .iter()
        .copied()
        .filter(|descriptor| *descriptor >= 3)
        .map(|descriptor| descriptor as u32)
        .collect();
    selected.sort_unstable();
    selected.dedup();
    let mut first = 3_u32;
    for descriptor in &selected {
        if close_range(first, descriptor.saturating_sub(1)).is_err() {
            fallback_close_except(keep);
            return;
        }
        first = descriptor.saturating_add(1);
    }
    if close_range(first, u32::MAX).is_err() {
        fallback_close_except(keep);
    }
}

fn descriptors_are_exactly(allowed: &[RawFd]) -> bool {
    let descriptors: Vec<_> = match fs::read_dir("/proc/self/fd") {
        Ok(entries) => entries
            .filter_map(|entry| entry.ok())
            .filter_map(|entry| entry.file_name().to_string_lossy().parse::<RawFd>().ok())
            .collect(),
        Err(_) => return false,
    };
    descriptors.into_iter().all(|descriptor| {
        if descriptor < 3 || allowed.contains(&descriptor) {
            return true;
        }
        // SAFETY: F_GETFD probes only this numeric descriptor. EBADF means the
        // transient /proc directory descriptor has already closed.
        (unsafe { libc::fcntl(descriptor, libc::F_GETFD) }) == -1
            && io::Error::last_os_error().raw_os_error() == Some(libc::EBADF)
    })
}

fn drop_privileges() -> SetupCode {
    // SAFETY: prctl is called with the documented scalar operations and zeroed unused
    // arguments; capset receives two initialized version-3 capability words.
    let ambient = unsafe {
        libc::prctl(
            PR_CAP_AMBIENT,
            PR_CAP_AMBIENT_CLEAR_ALL,
            0_u64,
            0_u64,
            0_u64,
        )
    };
    let header = CapabilityHeader {
        version: LINUX_CAPABILITY_VERSION_3,
        pid: 0,
    };
    let data = [
        CapabilityData {
            effective: 0,
            permitted: 0,
            inheritable: 0,
        },
        CapabilityData {
            effective: 0,
            permitted: 0,
            inheritable: 0,
        },
    ];
    // SAFETY: the header and two capability data elements live through the syscall.
    let capabilities = unsafe {
        libc::syscall(
            libc::SYS_capset,
            &header as *const CapabilityHeader,
            data.as_ptr(),
        )
    };
    // SAFETY: this is the documented irreversible no_new_privs operation.
    let no_new_privileges = unsafe { libc::prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) };
    if ambient != 0 || capabilities != 0 || no_new_privileges != 0 {
        return SetupCode::PrivilegeDropFailed;
    }
    let Ok(status) = fs::read_to_string("/proc/self/status") else {
        return SetupCode::PrivilegeDropUnverified;
    };
    let mut values = BTreeMap::new();
    for line in status.lines() {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        if matches!(
            name,
            "CapEff" | "CapPrm" | "CapInh" | "CapAmb" | "NoNewPrivs"
        ) {
            values.insert(name, value.trim());
        }
    }
    let capabilities_clear = ["CapEff", "CapPrm", "CapInh", "CapAmb"]
        .into_iter()
        .all(|name| {
            values
                .get(name)
                .and_then(|value| u64::from_str_radix(value, 16).ok())
                == Some(0)
        });
    if capabilities_clear && values.get("NoNewPrivs") == Some(&"1") {
        SetupCode::Ok
    } else {
        SetupCode::PrivilegeDropUnverified
    }
}

fn write_tmpfs_report(
    setup: &TmpfsSetup,
    peak_bytes: u64,
    peak_inodes: u64,
    terminal_bytes: u64,
    terminal_inodes: u64,
    byte_limit_hit: bool,
    inode_limit_hit: bool,
) -> io::Result<()> {
    let file_name = setup
        .report
        .file_name()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "report has no name"))?;
    let temporary =
        setup
            .report
            .with_file_name(format!(".{}.{}.tmp", file_name.to_string_lossy(), unsafe {
                libc::getpid()
            }));
    let result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
            .open(&temporary)?;
        let payload = serde_json::to_vec(&json!({
            "version": 1,
            "token": setup.token,
            "peak_bytes": peak_bytes,
            "peak_inodes": peak_inodes,
            "terminal_bytes": terminal_bytes,
            "terminal_inodes": terminal_inodes,
            "byte_limit_hit": byte_limit_hit,
            "inode_limit_hit": inode_limit_hit,
        }))?;
        file.write_all(&payload)?;
        file.sync_all()?;
        fs::rename(&temporary, &setup.report)?;
        Ok(())
    })();
    let _ = fs::remove_file(temporary);
    result
}

fn emulated_tmpfs_usage(path: &Path) -> io::Result<(u64, u64)> {
    let mut bytes = 0_u64;
    let mut inodes = 0_u64;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let details = fs::symlink_metadata(entry.path())?;
        inodes = inodes
            .checked_add(1)
            .ok_or_else(|| io::Error::other("inode count overflow"))?;
        if details.is_dir() {
            let (nested_bytes, nested_inodes) = emulated_tmpfs_usage(&entry.path())?;
            bytes = bytes
                .checked_add(nested_bytes)
                .ok_or_else(|| io::Error::other("byte count overflow"))?;
            inodes = inodes
                .checked_add(nested_inodes)
                .ok_or_else(|| io::Error::other("inode count overflow"))?;
        } else if details.is_file() {
            bytes = bytes
                .checked_add(details.len())
                .ok_or_else(|| io::Error::other("byte count overflow"))?;
        }
    }
    Ok((bytes, inodes))
}

fn tmpfs_sample(setup: &TmpfsSetup, baseline: TmpfsBaseline) -> io::Result<(u64, u64, bool, bool)> {
    if setup.emulate {
        let (bytes, inodes) = emulated_tmpfs_usage(&setup.target)?;
        return Ok((bytes, inodes, bytes >= setup.size, inodes >= setup.inodes));
    }
    let usage = cgroup::tmpfs_stat(setup).map_err(|error| io::Error::other(error.code))?;
    let used_bytes = usage
        .blocks
        .saturating_sub(usage.blocks_free)
        .saturating_mul(usage.fragment_size);
    let baseline_inodes = baseline.files.saturating_sub(baseline.files_free);
    let used_inodes = usage
        .files
        .saturating_sub(usage.files_free)
        .saturating_sub(baseline_inodes);
    Ok((
        used_bytes,
        used_inodes,
        usage.blocks_free == 0,
        usage.files_free == 0,
    ))
}

fn cleanup_emulated_tmpfs(setup: &TmpfsSetup) {
    if !setup.emulate {
        return;
    }
    let Ok(entries) = fs::read_dir(&setup.target) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if fs::symlink_metadata(&path).is_ok_and(|details| details.is_dir()) {
            let _ = fs::remove_dir_all(path);
        } else {
            let _ = fs::remove_file(path);
        }
    }
}

fn exec_plan(plan: &ExecPlan) -> ! {
    close_all_except(&[]);
    if !descriptors_are_exactly(&[]) {
        unsafe { libc::_exit(125) }
    }
    // SAFETY: checkout, argv, envp and executable are immutable NUL-terminated buffers
    // built before fork. execve replaces only this child and never returns on success.
    unsafe {
        if libc::chdir(plan.checkout.as_ptr()) != 0 {
            libc::_exit(127);
        }
        libc::execve(
            plan.executable.as_ptr(),
            plan.argument_pointers.as_ptr(),
            plan.environment_pointers.as_ptr(),
        );
        libc::_exit(127);
    }
}

fn supervise_tmpfs(plan: &ExecPlan, setup: &TmpfsSetup, baseline: TmpfsBaseline) -> ! {
    // SAFETY: the launcher is single-threaded and the child immediately execs or exits.
    let child = unsafe { libc::fork() };
    if child < 0 {
        unsafe { libc::_exit(125) }
    }
    if child == 0 {
        exec_plan(plan);
    }
    let mut peak_bytes = 0;
    let mut peak_inodes = 0;
    let mut byte_limit_hit = false;
    let mut inode_limit_hit = false;
    let status = loop {
        let mut status = 0;
        // SAFETY: child is the supervisor's exact fork child and status is writable.
        let observed = unsafe { libc::waitpid(child, &mut status, libc::WNOHANG) };
        let finished = observed == child;
        if observed < 0 {
            unsafe { libc::_exit(125) }
        }
        if let Ok((bytes, inodes, bytes_full, inodes_full)) = tmpfs_sample(setup, baseline) {
            peak_bytes = peak_bytes.max(bytes);
            peak_inodes = peak_inodes.max(inodes);
            byte_limit_hit |= bytes_full;
            inode_limit_hit |= inodes_full;
            let _ = write_tmpfs_report(
                setup,
                peak_bytes,
                peak_inodes,
                bytes,
                inodes,
                byte_limit_hit,
                inode_limit_hit,
            );
        }
        if finished {
            break status;
        }
        thread::sleep(Duration::from_millis(50));
    };
    cleanup_emulated_tmpfs(setup);
    if unmount_current_tmpfs(setup).is_err() {
        unsafe { libc::_exit(125) }
    }
    if libc::WIFEXITED(status) {
        unsafe { libc::_exit(libc::WEXITSTATUS(status)) }
    }
    if libc::WIFSIGNALED(status) {
        let signal = libc::WTERMSIG(status);
        unsafe {
            libc::signal(signal, libc::SIG_DFL);
            libc::kill(libc::getpid(), signal);
            libc::_exit(128 + signal);
        }
    }
    unsafe { libc::_exit(125) }
}

fn reset_worker_signals() {
    // SAFETY: restoring default scalar signal dispositions in the single-threaded fork
    // child prevents the broker's flag handlers from swallowing cancellation.
    unsafe {
        libc::signal(libc::SIGINT, libc::SIG_DFL);
        libc::signal(libc::SIGTERM, libc::SIG_DFL);
        libc::signal(libc::SIGPIPE, libc::SIG_DFL);
    }
}

fn child_stdio(log_fd: RawFd) -> bool {
    let null = b"/dev/null\0";
    // SAFETY: the path is a static NUL-terminated string.
    let input = unsafe { libc::open(null.as_ptr().cast(), libc::O_RDONLY | libc::O_CLOEXEC) };
    if input < 0 {
        return false;
    }
    // SAFETY: dup2 targets the standard descriptors and the source descriptors are live
    // inherited files. Each result is checked before continuing.
    let stdin_result = unsafe { libc::dup2(input, libc::STDIN_FILENO) };
    let stdout_result = unsafe { libc::dup2(log_fd, libc::STDOUT_FILENO) };
    let stderr_result = unsafe { libc::dup2(log_fd, libc::STDERR_FILENO) };
    if input > libc::STDERR_FILENO {
        // SAFETY: this closes only the child copy of the temporary descriptor.
        let _ = unsafe { libc::close(input) };
    }
    stdin_result >= 0 && stdout_result >= 0 && stderr_result >= 0
}

fn child_main(
    plan: &ExecPlan,
    control_read: RawFd,
    setup_write: RawFd,
    token: [u8; TOKEN_BYTES],
) -> ! {
    reset_worker_signals();
    // SAFETY: zero selects the calling child and its own PID as a new process group.
    if unsafe { libc::setpgid(0, 0) } != 0 || !child_stdio(plan.log_fd) {
        // SAFETY: the fork child must never unwind through copied broker state.
        unsafe { libc::_exit(125) }
    }
    close_all_except(&[control_read, setup_write]);
    if plan.fault == Some(WorkerFault::LauncherDeath) {
        // SAFETY: bounded debug fault exits before any release is observed.
        unsafe { libc::_exit(125) }
    }
    let pid = unsafe { libc::getpid() } as u32;
    let process_group = unsafe { libc::getpgrp() } as u32;
    let hello_token = if plan.fault == Some(WorkerFault::HelloToken) {
        altered(token)
    } else {
        token
    };
    if !write_all_fd(
        setup_write,
        &hello_message(&hello_token, pid, process_group),
    ) {
        unsafe { libc::_exit(125) }
    }
    let mut initial = [0_u8; CONTROL_BYTES];
    if !read_exact_fd(control_read, &mut initial)
        || initial[..4] != CONTROL_MAGIC[..]
        || initial[4] != INITIAL_RELEASE
        || initial[5..] != token[..]
    {
        unsafe { libc::_exit(125) }
    }

    let mut tmpfs_baseline = None;
    let mut code = SetupCode::Ok;
    if plan.fault == Some(WorkerFault::NamespaceIsolationUnavailable) {
        code = SetupCode::NamespaceIsolationUnavailable;
    } else if plan.setup.isolate_cgroup
        && let Err(error) = isolate_current_cgroup()
    {
        code = SetupCode::from_resource_error(&error.code);
    }
    if code == SetupCode::Ok
        && let Some(setup) = &plan.setup.tmpfs
    {
        if !setup.emulate && !plan.setup.isolate_cgroup {
            code = SetupCode::TmpfsNamespaceRequired;
        } else if plan.fault == Some(WorkerFault::TmpfsMountUnavailable) {
            code = SetupCode::TmpfsMountUnavailable;
        } else {
            match mount_current_tmpfs(setup) {
                Ok(baseline) => {
                    if write_tmpfs_report(setup, 0, 0, 0, 0, false, false).is_err() {
                        let _ = unmount_current_tmpfs(setup);
                        code = SetupCode::TmpfsReportFailed;
                    } else {
                        tmpfs_baseline = Some(baseline);
                    }
                }
                Err(error) => code = SetupCode::from_resource_error(&error.code),
            }
        }
    }
    let privilege_code = drop_privileges();
    if privilege_code != SetupCode::Ok {
        code = privilege_code;
    }
    if plan.fault == Some(WorkerFault::PrivilegeVerification) {
        code = SetupCode::PrivilegeDropUnverified;
    }
    if plan.fault == Some(WorkerFault::RetainedDescriptor) {
        let null = b"/dev/null\0";
        unsafe {
            libc::open(null.as_ptr().cast(), libc::O_RDONLY | libc::O_CLOEXEC);
        }
    }
    if !descriptors_are_exactly(&[control_read, setup_write]) {
        code = SetupCode::DescriptorLeak;
    }
    let setup_token = if plan.fault == Some(WorkerFault::SetupToken) {
        altered(token)
    } else {
        token
    };
    if !write_all_fd(setup_write, &setup_message(&setup_token, code)) {
        if let (Some(setup), Some(_baseline)) = (&plan.setup.tmpfs, tmpfs_baseline) {
            let _ = unmount_current_tmpfs(setup);
        }
        unsafe { libc::_exit(125) }
    }
    let mut final_release = [0_u8; CONTROL_BYTES];
    if !read_exact_fd(control_read, &mut final_release)
        || final_release[..4] != CONTROL_MAGIC[..]
        || final_release[4] != FINAL_RELEASE
        || final_release[5..] != token[..]
    {
        if let (Some(setup), Some(_baseline)) = (&plan.setup.tmpfs, tmpfs_baseline) {
            let _ = unmount_current_tmpfs(setup);
        }
        unsafe { libc::_exit(125) }
    }
    if code != SetupCode::Ok {
        if let (Some(setup), Some(_baseline)) = (&plan.setup.tmpfs, tmpfs_baseline) {
            let _ = unmount_current_tmpfs(setup);
        }
        exec_plan(plan);
    }
    if let (Some(setup), Some(baseline)) = (&plan.setup.tmpfs, tmpfs_baseline)
        && !setup.emulate
    {
        supervise_tmpfs(plan, setup, baseline);
    }
    exec_plan(plan);
}

fn wait_status(status: libc::c_int) -> i64 {
    if libc::WIFEXITED(status) {
        i64::from(libc::WEXITSTATUS(status))
    } else if libc::WIFSIGNALED(status) {
        i64::from(128 + libc::WTERMSIG(status))
    } else {
        255
    }
}

#[derive(Debug)]
pub struct NativeWorker {
    pid: u32,
    exit_status: Option<i64>,
}

impl NativeWorker {
    pub fn pid(&self) -> u32 {
        self.pid
    }

    pub fn try_wait(&mut self) -> io::Result<Option<i64>> {
        if self.exit_status.is_some() {
            return Ok(self.exit_status);
        }
        let mut status = 0;
        // SAFETY: waitpid receives this broker's exact fork child PID and a writable
        // status word. WNOHANG keeps the scheduler pump nonblocking.
        let observed = unsafe { libc::waitpid(self.pid as i32, &mut status, libc::WNOHANG) };
        if observed == 0 {
            return Ok(None);
        }
        if observed == self.pid as i32 {
            self.exit_status = Some(wait_status(status));
            return Ok(self.exit_status);
        }
        Err(io::Error::last_os_error())
    }
}

pub struct PendingWorker {
    worker: Option<NativeWorker>,
    control: Option<File>,
    setup: Option<File>,
    token: [u8; TOKEN_BYTES],
    start_token: String,
    fault: Option<WorkerFault>,
    setup_verified: bool,
}

impl PendingWorker {
    pub fn spawn(
        command: &[String],
        environment: &BTreeMap<String, String>,
        checkout: &Path,
        log: &File,
        fault: Option<WorkerFault>,
        setup: WorkerSetup,
    ) -> Result<Self> {
        let plan = ExecPlan::new(command, environment, checkout, log, fault, setup)?;
        let token = random_token()?;
        let (control_read, control_write) = create_pipe().map_err(|_| {
            AppError::new(
                "broker-worker-start-failed",
                "cannot create the private worker release channel",
            )
        })?;
        let (setup_read, setup_write) = match create_pipe() {
            Ok(pipe) => pipe,
            Err(_) => {
                unsafe {
                    libc::close(control_read);
                    libc::close(control_write);
                }
                return Err(AppError::new(
                    "broker-worker-start-failed",
                    "cannot create the private worker setup channel",
                ));
            }
        };
        // SAFETY: the broker is single-threaded and the child immediately restricts its
        // descriptor set, performs only its fixed launcher path, and execs or _exit(2)s.
        let forked = unsafe { libc::fork() };
        if forked < 0 {
            unsafe {
                libc::close(control_read);
                libc::close(control_write);
                libc::close(setup_read);
                libc::close(setup_write);
            }
            return Err(AppError::new(
                "broker-worker-start-failed",
                "cannot fork the native worker launcher",
            ));
        }
        if forked == 0 {
            unsafe {
                libc::close(control_write);
                libc::close(setup_read);
            }
            child_main(&plan, control_read, setup_write, token);
        }
        unsafe {
            libc::close(control_read);
            libc::close(setup_write);
        }
        let pid = u32::try_from(forked).map_err(|_| {
            AppError::new(
                "broker-worker-identity-invalid",
                "native worker PID is out of range",
            )
        })?;
        let mut pending = Self {
            worker: Some(NativeWorker {
                pid,
                exit_status: None,
            }),
            control: Some(unsafe { File::from_raw_fd(control_write) }),
            setup: Some(unsafe { File::from_raw_fd(setup_read) }),
            token,
            start_token: String::new(),
            fault,
            setup_verified: false,
        };
        let mut hello = [0_u8; HELLO_BYTES];
        read_exact_timeout(pending.setup.as_ref().unwrap().as_raw_fd(), &mut hello).map_err(
            |_| {
                AppError::new(
                    "broker-worker-handshake-failed",
                    "native worker did not complete its private-channel hello",
                )
            },
        )?;
        if hello != hello_message(&token, pid, pid) {
            return Err(AppError::new(
                "broker-worker-handshake-failed",
                "native worker hello did not match its private channel and identity",
            ));
        }
        let deadline = Instant::now() + Duration::from_millis(250);
        let start_token = loop {
            if let Some(selected) = process_start_token(pid)
                && same_worker_process(Some(pid), Some(&selected))
            {
                break selected;
            }
            if Instant::now() >= deadline {
                return Err(AppError::new(
                    "broker-worker-identity-invalid",
                    "native worker process identity could not be verified",
                ));
            }
            thread::sleep(Duration::from_millis(5));
        };
        pending.start_token = start_token;
        Ok(pending)
    }

    pub fn pid(&self) -> u32 {
        self.worker.as_ref().unwrap().pid()
    }

    pub fn start_token(&self) -> &str {
        &self.start_token
    }

    pub fn verify_setup(&mut self) -> Result<Option<&'static str>> {
        if self.fault == Some(WorkerFault::SubstitutedChannel) {
            if let Ok((substitute_read, substitute_write)) = create_pipe() {
                let mut substitute = unsafe { File::from_raw_fd(substitute_write) };
                let _ = substitute.write_all(&control_message(&self.token, INITIAL_RELEASE));
                drop(substitute);
                unsafe {
                    libc::close(substitute_read);
                }
            }
            return Err(AppError::new(
                "broker-worker-handshake-failed",
                "native worker release used a substituted private channel",
            ));
        }
        let selected = if self.fault == Some(WorkerFault::ReplayedToken) {
            altered(self.token)
        } else {
            self.token
        };
        self.control
            .as_mut()
            .unwrap()
            .write_all(&control_message(&selected, INITIAL_RELEASE))
            .map_err(|_| {
                AppError::new(
                    "broker-worker-handshake-failed",
                    "native worker release channel closed before setup",
                )
            })?;
        let mut setup = [0_u8; SETUP_BYTES];
        read_exact_timeout(self.setup.as_ref().unwrap().as_raw_fd(), &mut setup).map_err(|_| {
            AppError::new(
                "broker-worker-handshake-failed",
                "native worker setup channel closed before verification",
            )
        })?;
        if setup[..4] != SETUP_MAGIC[..] || setup[4..36] != self.token[..] {
            return Err(AppError::new(
                "broker-worker-handshake-failed",
                "native worker setup result did not match its private token",
            ));
        }
        let code = SetupCode::from_byte(setup[36]).ok_or_else(|| {
            AppError::new(
                "broker-worker-handshake-failed",
                "native worker returned an unknown setup result",
            )
        })?;
        if let Some(resource_failure) = code.resource_failure() {
            self.setup_verified = true;
            return Ok(Some(resource_failure));
        }
        if let Some(error) = code.error() {
            return Err(error);
        }
        self.setup_verified = true;
        Ok(None)
    }

    pub fn release(mut self) -> Result<NativeWorker> {
        if !self.setup_verified {
            return Err(AppError::new(
                "broker-worker-handshake-failed",
                "native worker cannot be released before verified setup",
            ));
        }
        let selected = if self.fault == Some(WorkerFault::FinalToken) {
            altered(self.token)
        } else {
            self.token
        };
        self.control
            .as_mut()
            .unwrap()
            .write_all(&control_message(&selected, FINAL_RELEASE))
            .map_err(|_| {
                AppError::new(
                    "broker-worker-handshake-failed",
                    "native worker final release channel closed",
                )
            })?;
        drop(self.control.take());
        drop(self.setup.take());
        Ok(self.worker.take().unwrap())
    }
}

impl Drop for PendingWorker {
    fn drop(&mut self) {
        drop(self.control.take());
        drop(self.setup.take());
        let Some(mut worker) = self.worker.take() else {
            return;
        };
        let deadline = Instant::now() + ABORT_GRACE;
        while Instant::now() < deadline {
            if worker.try_wait().ok().flatten().is_some() {
                return;
            }
            thread::sleep(Duration::from_millis(5));
        }
        let _ = signal_process_group(worker.pid(), libc::SIGKILL);
        // The child can fail before becoming a process-group leader, so target its PID
        // as a final bounded cleanup of this exact fork child.
        unsafe {
            libc::kill(worker.pid() as i32, libc::SIGKILL);
        }
        let deadline = Instant::now() + ABORT_GRACE;
        while Instant::now() < deadline {
            if worker.try_wait().ok().flatten().is_some() {
                return;
            }
            thread::sleep(Duration::from_millis(5));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::SetupCode;

    #[test]
    fn cgroup_mount_errno_remains_a_stable_worker_mount_failure() {
        assert_eq!(
            SetupCode::from_resource_error("namespace-cgroup2-mount-failed-errno-13"),
            SetupCode::NamespaceMountFailed,
        );
    }
}
