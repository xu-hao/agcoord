use crate::error::{AppError, Result};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::Path;

pub struct OwnerLock {
    file: File,
}

impl OwnerLock {
    pub fn acquire(state_dir: &Path) -> Result<Self> {
        let path = state_dir.join("broker.lock");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(&path)
            .map_err(|error| {
                AppError::new(
                    "broker-owner-lock-unavailable",
                    format!("cannot open broker ownership file: {error}"),
                )
            })?;
        file.set_permissions(fs::Permissions::from_mode(0o600))
            .map_err(|error| {
                AppError::new(
                    "broker-owner-lock-unavailable",
                    format!("cannot protect broker ownership file: {error}"),
                )
            })?;
        // SAFETY: flock receives a live descriptor owned by `file`; the descriptor remains
        // open for the lifetime of this guard and no pointer crosses the FFI boundary.
        let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if result != 0 {
            let error = std::io::Error::last_os_error();
            if error.raw_os_error() == Some(libc::EWOULDBLOCK) {
                return Err(AppError::new(
                    "broker-already-owned",
                    "another native or Python broker already owns this state directory",
                ));
            }
            return Err(AppError::new(
                "broker-owner-lock-unavailable",
                format!("cannot lock broker ownership file: {error}"),
            ));
        }
        Ok(Self { file })
    }

    pub fn publish(&mut self, metadata: &str) -> Result<()> {
        self.file.set_len(0).map_err(|error| {
            AppError::new(
                "broker-owner-metadata-failed",
                format!("cannot clear broker ownership metadata: {error}"),
            )
        })?;
        self.file.seek(SeekFrom::Start(0)).map_err(|error| {
            AppError::new(
                "broker-owner-metadata-failed",
                format!("cannot seek broker ownership metadata: {error}"),
            )
        })?;
        self.file.write_all(metadata.as_bytes()).map_err(|error| {
            AppError::new(
                "broker-owner-metadata-failed",
                format!("cannot write broker ownership metadata: {error}"),
            )
        })?;
        self.file.sync_all().map_err(|error| {
            AppError::new(
                "broker-owner-metadata-failed",
                format!("cannot sync broker ownership metadata: {error}"),
            )
        })
    }
}

impl Drop for OwnerLock {
    fn drop(&mut self) {
        // SAFETY: the descriptor is valid until `self.file` is dropped immediately after
        // this method. Unlock failure cannot be recovered during destruction.
        let _ = unsafe { libc::flock(self.file.as_raw_fd(), libc::LOCK_UN) };
    }
}

pub fn live_owner_metadata(state_dir: &Path) -> Result<Option<String>> {
    let path = state_dir.join("broker.lock");
    let mut file = match OpenOptions::new().read(true).write(true).open(&path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(AppError::new(
                "broker-owner-lock-unavailable",
                format!("cannot read broker ownership file: {error}"),
            ));
        }
    };
    // SAFETY: flock receives a live descriptor; this function keeps it open until the lock
    // probe and metadata read have finished.
    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if result == 0 {
        // SAFETY: see the acquisition above; this releases only the probe lock.
        let _ = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_UN) };
        return Ok(None);
    }
    let error = std::io::Error::last_os_error();
    if error.raw_os_error() != Some(libc::EWOULDBLOCK) {
        return Err(AppError::new(
            "broker-owner-lock-unavailable",
            format!("cannot probe broker ownership file: {error}"),
        ));
    }
    let mut metadata = String::new();
    file.read_to_string(&mut metadata).map_err(|error| {
        AppError::new(
            "broker-owner-metadata-invalid",
            format!("cannot read live broker ownership metadata: {error}"),
        )
    })?;
    if metadata.len() > 1024 * 1024 {
        return Err(AppError::new(
            "broker-owner-metadata-invalid",
            "live broker ownership metadata is oversized",
        ));
    }
    Ok(Some(metadata))
}

pub fn prepare_private_directory(path: &Path) -> Result<()> {
    if !path.exists() {
        fs::create_dir_all(path).map_err(|error| {
            AppError::new(
                "broker-state-invalid",
                format!("cannot create state directory: {error}"),
            )
        })?;
    }
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        AppError::new(
            "broker-state-invalid",
            format!("cannot inspect state directory: {error}"),
        )
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(AppError::new(
            "broker-state-invalid",
            "state path must be a real directory",
        ));
    }
    // SAFETY: geteuid takes no arguments and has no preconditions.
    if metadata.uid() != unsafe { libc::geteuid() } {
        return Err(AppError::new(
            "broker-state-invalid",
            "state directory belongs to another user",
        ));
    }
    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).map_err(|error| {
        AppError::new(
            "broker-state-invalid",
            format!("cannot protect state directory: {error}"),
        )
    })
}

fn process_identity(pid: u32) -> Option<(String, u32, String)> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let closing = stat.rfind(')')?;
    let fields: Vec<_> = stat.get(closing + 2..)?.split_whitespace().collect();
    Some((
        fields.first()?.to_string(),
        fields.get(2)?.parse().ok()?,
        fields.get(19)?.to_string(),
    ))
}

pub fn process_start_token(pid: u32) -> Option<String> {
    process_identity(pid).map(|(_state, _process_group, token)| token)
}

pub fn same_worker_process(pid: Option<u32>, token: Option<&str>) -> bool {
    match (pid, token) {
        (Some(pid), Some(token)) => {
            process_identity(pid).is_some_and(|(state, process_group, observed)| {
                !matches!(state.as_str(), "Z" | "X") && process_group == pid && observed == token
            })
        }
        _ => false,
    }
}

pub fn worker_identity_conflicts(pid: Option<u32>, token: Option<&str>) -> bool {
    match (pid, token) {
        (Some(pid), Some(token)) => {
            process_identity(pid).is_some_and(|(_state, process_group, observed)| {
                process_group != pid || observed != token
            })
        }
        _ => false,
    }
}

pub fn process_group_exists(process_group: u32) -> bool {
    let Ok(process_group) = i32::try_from(process_group) else {
        return false;
    };
    // SAFETY: signal zero performs only an existence/permission probe for this numeric
    // process group and cannot deliver a signal.
    let result = unsafe { libc::kill(-process_group, 0) };
    if result == 0 {
        return true;
    }
    std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

pub fn signal_process_group(pid: u32, signal: i32) -> Result<()> {
    let process_group = i32::try_from(pid).map_err(|_| {
        AppError::new(
            "broker-worker-identity-invalid",
            "worker PID is out of range",
        )
    })?;
    // SAFETY: kill receives a numeric process-group ID and signal. Negative PID targets
    // exactly the worker-owned process group established at spawn.
    let result = unsafe { libc::kill(-process_group, signal) };
    if result == 0 {
        return Ok(());
    }
    let error = std::io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::ESRCH) {
        return Ok(());
    }
    Err(AppError::new(
        "broker-worker-signal-failed",
        format!("cannot signal worker process group: {error}"),
    ))
}

pub fn sync_file(path: &Path) -> Result<()> {
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|error| {
            AppError::new(
                "broker-migration-backup-failed",
                format!("cannot sync migration backup: {error}"),
            )
        })
}
