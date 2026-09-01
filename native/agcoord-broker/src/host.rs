use crate::cgroup::sha256_prefix;
use crate::error::{AppError, Result};
use crate::store::PROTOCOL;
use serde_json::{Value, json};
use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

const INSTALLED_EXECUTABLE: &str = "/usr/libexec/agcoord/agcoord-broker";
const APPARMOR_PROFILE: &str = "agcoord-broker";
const APPARMOR_CLIENT_PROFILE: &str = "agcoord-broker-client";
const SERVICE_SUBGROUP: &str = "supervisor";
const IMPLEMENTATION: &str = "rust-native";
const BUILD: &str = env!("AGCOORD_BUILD_ID");
const TARGET: &str = env!("AGCOORD_TARGET");

#[derive(Clone, Debug)]
pub struct PreflightOptions {
    pub state_dir: PathBuf,
    pub fixture_root: Option<PathBuf>,
    pub executable: Option<PathBuf>,
}

impl PreflightOptions {
    pub fn production(state_dir: PathBuf) -> Self {
        Self {
            state_dir,
            fixture_root: None,
            executable: None,
        }
    }
}

fn refusal(code: &'static str, message: impl Into<String>) -> AppError {
    AppError::new(code, message)
}

fn fixture_path(root: Option<&Path>, path: &Path) -> PathBuf {
    root.map_or_else(
        || path.to_path_buf(),
        |root| root.join(path.strip_prefix("/").unwrap_or(path)),
    )
}

fn read_trimmed(root: Option<&Path>, path: &str, code: &'static str) -> Result<String> {
    fs::read_to_string(fixture_path(root, Path::new(path)))
        .map(|value| value.trim().to_owned())
        .map_err(|error| refusal(code, format!("cannot read {path}: {error}")))
}

fn valid_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn selected_executable(options: &PreflightOptions) -> Result<PathBuf> {
    if options.fixture_root.is_some() {
        return options.executable.clone().ok_or_else(|| {
            refusal(
                "host-executable-invalid",
                "fixture preflight requires an explicit executable",
            )
        });
    }
    if options.executable.is_some() {
        return Err(refusal(
            "host-executable-invalid",
            "production preflight does not accept an executable override",
        ));
    }
    let executable = fs::read_link("/proc/self/exe").map_err(|error| {
        refusal(
            "host-executable-invalid",
            format!("cannot resolve the running native executable: {error}"),
        )
    })?;
    if executable != Path::new(INSTALLED_EXECUTABLE) {
        return Err(refusal(
            "host-executable-path-mismatch",
            format!("managed broker must run from {INSTALLED_EXECUTABLE}"),
        ));
    }
    Ok(executable)
}

fn configured_host_paths(state_dir: &Path) -> Result<(PathBuf, PathBuf, bool, bool)> {
    let path = state_dir.join("config.json");
    let raw = fs::read_to_string(&path).map_err(|error| {
        refusal(
            "host-config-invalid",
            format!("cannot read managed broker configuration {path:?}: {error}"),
        )
    })?;
    let document: Value = serde_json::from_str(&raw).map_err(|_| {
        refusal(
            "host-config-invalid",
            "managed broker configuration is not JSON",
        )
    })?;
    let object = document.as_object().ok_or_else(|| {
        refusal(
            "host-config-invalid",
            "managed broker configuration must be one JSON object",
        )
    })?;
    let cgroup_root = object
        .get("cgroup_root")
        .and_then(Value::as_str)
        .map(PathBuf::from)
        .filter(|path| path.is_absolute())
        .ok_or_else(|| {
            refusal(
                "host-config-invalid",
                "managed broker configuration requires an absolute cgroup_root",
            )
        })?;
    let native = object
        .get("native_broker")
        .and_then(Value::as_object)
        .ok_or_else(|| {
            refusal(
                "host-config-invalid",
                "managed broker configuration requires native_broker",
            )
        })?;
    let executable = native
        .get("path")
        .and_then(Value::as_str)
        .map(PathBuf::from)
        .filter(|path| path.is_absolute())
        .ok_or_else(|| {
            refusal(
                "host-config-invalid",
                "managed broker configuration requires an absolute native_broker.path",
            )
        })?;
    let allow_development = native
        .get("allow_development")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let managed_service = native
        .get("managed_service")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    Ok((cgroup_root, executable, allow_development, managed_service))
}

fn verify_executable(options: &PreflightOptions, executable: &Path) -> Result<String> {
    let details = executable.symlink_metadata().map_err(|error| {
        refusal(
            "host-executable-invalid",
            format!("cannot inspect managed broker executable: {error}"),
        )
    })?;
    let mode = details.permissions().mode();
    let expected_owner = if options.fixture_root.is_some() {
        // SAFETY: geteuid has no preconditions and only reads process credentials.
        unsafe { libc::geteuid() }
    } else {
        0
    };
    if !details.file_type().is_file()
        || details.file_type().is_symlink()
        || details.uid() != expected_owner
        || mode & 0o022 != 0
        || mode & 0o111 == 0
    {
        return Err(refusal(
            "host-executable-invalid",
            "managed broker executable owner, type, or mode is unsafe",
        ));
    }
    if options.fixture_root.is_none()
        && (BUILD == "development"
            || TARGET != "x86_64-unknown-linux-musl"
            || !BUILD.strip_prefix("sha256:").is_some_and(valid_digest))
    {
        return Err(refusal(
            "host-executable-identity-mismatch",
            "managed broker requires the audited musl release identity",
        ));
    }
    let bytes = fs::read(executable).map_err(|error| {
        refusal(
            "host-executable-invalid",
            format!("cannot read managed broker executable: {error}"),
        )
    })?;
    let digest = sha256_prefix(&bytes, 32);
    let sidecar = executable.with_file_name("agcoord-broker.sha256");
    let checksum = fs::read_to_string(&sidecar).map_err(|error| {
        refusal(
            "host-executable-digest-mismatch",
            format!("cannot read installed broker checksum: {error}"),
        )
    })?;
    let fields = checksum
        .trim_end_matches('\n')
        .split_whitespace()
        .collect::<Vec<_>>();
    if fields.as_slice() != [digest.as_str(), "agcoord-broker"] {
        return Err(refusal(
            "host-executable-digest-mismatch",
            "installed broker checksum does not match the executable",
        ));
    }
    Ok(digest)
}

fn cgroup2_mount(raw: &str) -> Result<PathBuf> {
    let mut selected = Vec::new();
    for line in raw.lines() {
        let Some((left, right)) = line.split_once(" - ") else {
            continue;
        };
        let left = left.split_whitespace().collect::<Vec<_>>();
        let right = right.split_whitespace().collect::<Vec<_>>();
        let mount_options = left.get(5).copied().unwrap_or_default();
        let super_options = right.get(2).copied().unwrap_or_default();
        let options = mount_options
            .split(',')
            .chain(super_options.split(','))
            .collect::<BTreeSet<_>>();
        if right.first() == Some(&"cgroup2")
            && left.len() >= 6
            && options.contains("nsdelegate")
            && options.contains("rw")
        {
            selected.push(PathBuf::from(left[4]));
        }
    }
    if selected.len() != 1 || !selected[0].is_absolute() {
        return Err(refusal(
            "host-cgroup-mount-invalid",
            "managed broker requires one nsdelegate cgroup v2 mount",
        ));
    }
    Ok(selected.remove(0))
}

fn unified_cgroup(raw: &str) -> Result<PathBuf> {
    let mut selected = raw
        .lines()
        .filter_map(|line| line.strip_prefix("0::"))
        .map(PathBuf::from)
        .collect::<Vec<_>>();
    if selected.len() != 1 || !selected[0].is_absolute() {
        return Err(refusal(
            "host-service-cgroup-mismatch",
            "managed broker has no unique unified cgroup",
        ));
    }
    Ok(selected.remove(0))
}

fn verify_service_cgroup(options: &PreflightOptions, cgroup_root: &Path) -> Result<Vec<String>> {
    let fixture = options.fixture_root.as_deref();
    let mountinfo = read_trimmed(fixture, "/proc/self/mountinfo", "host-cgroup-mount-invalid")?;
    let mount = cgroup2_mount(&mountinfo)?;
    let relative_root = cgroup_root.strip_prefix(&mount).map_err(|_| {
        refusal(
            "host-service-cgroup-mismatch",
            "configured cgroup_root is outside the unified hierarchy",
        )
    })?;
    let current = unified_cgroup(&read_trimmed(
        fixture,
        "/proc/self/cgroup",
        "host-service-cgroup-mismatch",
    )?)?;
    let expected = Path::new("/").join(relative_root).join(SERVICE_SUBGROUP);
    if current != expected
        || cgroup_root.file_name().and_then(|name| name.to_str()) != Some("agcoord-broker.service")
    {
        return Err(refusal(
            "host-service-cgroup-mismatch",
            "managed broker is outside agcoord-broker.service/supervisor",
        ));
    }
    let physical_root = fixture_path(fixture, cgroup_root);
    let details = physical_root.symlink_metadata().map_err(|_| {
        refusal(
            "host-service-cgroup-mismatch",
            "configured service cgroup does not exist",
        )
    })?;
    // SAFETY: geteuid has no preconditions and only reads process credentials.
    let expected_owner = unsafe { libc::geteuid() };
    let subgroup = physical_root.join(SERVICE_SUBGROUP);
    let subgroup_details = subgroup.symlink_metadata().map_err(|_| {
        refusal(
            "host-service-cgroup-mismatch",
            "managed service supervisor subgroup does not exist",
        )
    })?;
    if !details.file_type().is_dir()
        || details.file_type().is_symlink()
        || details.uid() != expected_owner
        || details.permissions().mode() & 0o002 != 0
        || !subgroup_details.file_type().is_dir()
        || subgroup_details.file_type().is_symlink()
        || subgroup_details.uid() != expected_owner
        || subgroup_details.permissions().mode() & 0o002 != 0
    {
        return Err(refusal(
            "host-service-cgroup-mismatch",
            "configured service cgroup is not a delegated directory",
        ));
    }
    if fixture.is_none() {
        let members = fs::read_to_string(physical_root.join(SERVICE_SUBGROUP).join("cgroup.procs"))
            .map_err(|_| {
                refusal(
                    "host-service-cgroup-mismatch",
                    "cannot read managed service membership",
                )
            })?;
        if !members
            .split_whitespace()
            .any(|value| value.parse::<u32>().ok() == Some(std::process::id()))
        {
            return Err(refusal(
                "host-service-cgroup-mismatch",
                "managed broker is not a member of the supervisor subgroup",
            ));
        }
    }
    let controllers = fs::read_to_string(physical_root.join("cgroup.controllers"))
        .map_err(|_| {
            refusal(
                "host-cgroup-controllers-unavailable",
                "cannot read delegated cgroup controllers",
            )
        })?
        .split_whitespace()
        .map(str::to_owned)
        .collect::<BTreeSet<_>>();
    if !["cpu", "memory", "pids"]
        .iter()
        .all(|controller| controllers.contains(*controller))
        || controllers.iter().any(|name| {
            name.is_empty()
                || !name
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte == b'_')
        })
    {
        return Err(refusal(
            "host-cgroup-controllers-unavailable",
            "delegated cgroup controllers must include cpu, memory, and pids",
        ));
    }
    Ok(controllers.into_iter().collect())
}

pub fn preflight(options: &PreflightOptions) -> Result<Value> {
    if options.fixture_root.is_some() && !cfg!(debug_assertions) {
        return Err(refusal(
            "host-fixture-forbidden",
            "host fixture options are unavailable in release builds",
        ));
    }
    let executable = selected_executable(options)?;
    let (cgroup_root, configured_executable, allow_development, managed_service) =
        configured_host_paths(&options.state_dir)?;
    if configured_executable != executable
        || !managed_service
        || (options.fixture_root.is_some() && !allow_development)
        || (options.fixture_root.is_none() && allow_development)
    {
        return Err(refusal(
            "host-executable-path-mismatch",
            "managed configuration does not select this executable and trust policy",
        ));
    }
    let digest = verify_executable(options, &executable)?;
    let fixture = options.fixture_root.as_deref();
    let profile = read_trimmed(
        fixture,
        "/proc/self/attr/current",
        "host-apparmor-profile-mismatch",
    )?;
    if profile != format!("{APPARMOR_PROFILE} (enforce)") {
        return Err(refusal(
            "host-apparmor-profile-mismatch",
            "managed broker is not in the enforced agcoord-broker profile",
        ));
    }
    let restriction = read_trimmed(
        fixture,
        "/proc/sys/kernel/apparmor_restrict_unprivileged_userns",
        "host-apparmor-restriction-disabled",
    )?;
    if restriction != "1" {
        return Err(refusal(
            "host-apparmor-restriction-disabled",
            "Ubuntu unprivileged-user-namespace restriction is not enabled",
        ));
    }
    let controllers = verify_service_cgroup(options, &cgroup_root)?;
    Ok(json!({
        "ready": true,
        "protocol": PROTOCOL,
        "implementation": IMPLEMENTATION,
        "build": BUILD,
        "target": TARGET,
        "sha256": digest,
        "profile": APPARMOR_PROFILE,
        "service_subgroup": SERVICE_SUBGROUP,
        "controllers": controllers,
    }))
}

pub fn client_preflight() -> Result<Value> {
    let profile = fs::read_to_string("/proc/self/attr/current")
        .map(|value| value.trim().to_owned())
        .map_err(|error| {
            refusal(
                "host-client-profile-mismatch",
                format!("cannot read the client AppArmor profile: {error}"),
            )
        })?;
    if profile != format!("{APPARMOR_CLIENT_PROFILE} (enforce)") {
        return Err(refusal(
            "host-client-profile-mismatch",
            "broker client command is not in the enforced restricted client profile",
        ));
    }

    // SAFETY: this single-threaded diagnostic forks only to make one namespace syscall and
    // immediately exits without allocating, locking, or invoking user-controlled code.
    let child = unsafe { libc::fork() };
    if child < 0 {
        return Err(refusal(
            "host-client-userns-check-failed",
            "cannot fork the restricted user-namespace probe",
        ));
    }
    if child == 0 {
        // SAFETY: unshare receives one documented namespace flag; _exit avoids post-fork state.
        let result = unsafe { libc::unshare(libc::CLONE_NEWUSER) };
        let code = if result == 0 {
            90
        } else {
            match std::io::Error::last_os_error().raw_os_error() {
                Some(code) if code == libc::EPERM || code == libc::EACCES => 0,
                _ => 91,
            }
        };
        // SAFETY: _exit terminates only the forked diagnostic process.
        unsafe { libc::_exit(code) };
    }
    let mut status = 0;
    loop {
        // SAFETY: child is the exact positive PID returned by fork and status is writable.
        let waited = unsafe { libc::waitpid(child, &mut status, 0) };
        if waited == child {
            break;
        }
        if waited < 0 && std::io::Error::last_os_error().kind() == std::io::ErrorKind::Interrupted {
            continue;
        }
        return Err(refusal(
            "host-client-userns-check-failed",
            "cannot observe the restricted user-namespace probe",
        ));
    }
    if libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0 {
        return Ok(json!({
            "ready": true,
            "profile": APPARMOR_CLIENT_PROFILE,
            "user_namespace_denied": true,
        }));
    }
    if libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 90 {
        return Err(refusal(
            "host-client-userns-permitted",
            "restricted broker client execution can create a user namespace",
        ));
    }
    Err(refusal(
        "host-client-userns-check-failed",
        "restricted user-namespace probe failed for an unexpected reason",
    ))
}

fn validate_state_directory(state_dir: &Path) -> Result<fs::Metadata> {
    let details = state_dir.symlink_metadata().map_err(|error| {
        refusal(
            "host-drain-state-invalid",
            format!("cannot inspect the state directory: {error}"),
        )
    })?;
    // SAFETY: geteuid has no preconditions and only reads process credentials.
    let effective_user = unsafe { libc::geteuid() };
    if !details.file_type().is_dir()
        || details.file_type().is_symlink()
        || details.permissions().mode() & 0o077 != 0
        || (effective_user != 0 && details.uid() != effective_user)
    {
        return Err(refusal(
            "host-drain-state-invalid",
            "state directory owner, type, or mode is unsafe for host maintenance",
        ));
    }
    Ok(details)
}

fn validate_lock_file(lock: &File, state: &fs::Metadata) -> Result<()> {
    let details = lock.metadata().map_err(|error| {
        refusal(
            "host-drain-lock-invalid",
            format!("cannot inspect the broker ownership lock: {error}"),
        )
    })?;
    if !details.file_type().is_file()
        || details.uid() != state.uid()
        || details.gid() != state.gid()
        || details.nlink() != 1
        || details.permissions().mode() & 0o7777 != 0o600
    {
        return Err(refusal(
            "host-drain-lock-invalid",
            "broker ownership lock owner, type, link count, or mode is unsafe",
        ));
    }
    Ok(())
}

fn lock_exclusively(lock: &File) -> Result<()> {
    // SAFETY: lock owns a live descriptor and flock receives documented flags only.
    if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0 {
        return Ok(());
    }
    let error = std::io::Error::last_os_error();
    if error
        .raw_os_error()
        .is_some_and(|code| code == libc::EAGAIN || code == libc::EWOULDBLOCK)
    {
        return Err(refusal(
            "host-drain-owner-live",
            "a broker still owns the state directory; drain and stop it first",
        ));
    }
    Err(refusal(
        "host-drain-state-invalid",
        format!("cannot acquire the broker ownership lock: {error}"),
    ))
}

fn existing_maintenance_lock(state_dir: &Path, state: &fs::Metadata) -> Result<Option<File>> {
    let lock_path = state_dir.join("broker.lock");
    let lock = match OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
        .open(&lock_path)
    {
        Ok(lock) => lock,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(refusal(
                "host-drain-lock-invalid",
                format!("cannot safely open the broker ownership lock: {error}"),
            ));
        }
    };
    validate_lock_file(&lock, state)?;
    lock_exclusively(&lock)?;
    Ok(Some(lock))
}

fn maintenance_lock(state_dir: &Path, state: &fs::Metadata) -> Result<File> {
    let lock_path = state_dir.join("broker.lock");
    let lock = match OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
        .open(&lock_path)
    {
        Ok(lock) => {
            // A root installer creates the lock for the unprivileged state-directory owner.
            let created = lock.metadata().map_err(|error| {
                refusal(
                    "host-drain-lock-invalid",
                    format!("cannot inspect the new broker ownership lock: {error}"),
                )
            })?;
            if created.uid() != state.uid() || created.gid() != state.gid() {
                // SAFETY: the descriptor is newly created, live, and owned by this process.
                if unsafe { libc::fchown(lock.as_raw_fd(), state.uid(), state.gid()) } != 0 {
                    return Err(refusal(
                        "host-drain-lock-invalid",
                        format!(
                            "cannot assign the broker ownership lock: {}",
                            std::io::Error::last_os_error()
                        ),
                    ));
                }
            }
            lock.set_permissions(fs::Permissions::from_mode(0o600))
                .map_err(|error| {
                    refusal(
                        "host-drain-lock-invalid",
                        format!("cannot secure the broker ownership lock: {error}"),
                    )
                })?;
            lock
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
            .open(&lock_path)
            .map_err(|error| {
                refusal(
                    "host-drain-lock-invalid",
                    format!("cannot safely open the broker ownership lock: {error}"),
                )
            })?,
        Err(error) => {
            return Err(refusal(
                "host-drain-lock-invalid",
                format!("cannot create the broker ownership lock: {error}"),
            ));
        }
    };
    validate_lock_file(&lock, state)?;
    lock_exclusively(&lock)?;
    Ok(lock)
}

fn drained_database(state_dir: &Path) -> Result<Value> {
    let database_path = state_dir.join("queue.sqlite3");
    if !database_path.is_file() {
        return Ok(json!({"drained": true, "live": 0, "protocol": Value::Null}));
    }
    let connection = rusqlite::Connection::open_with_flags(
        &database_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|error| {
        refusal(
            "host-drain-state-invalid",
            format!("cannot open the queue read-only: {error}"),
        )
    })?;
    let protocol_value: String = connection
        .query_row(
            "SELECT value FROM coordinator_meta WHERE key = 'protocol'",
            [],
            |row| row.get(0),
        )
        .map_err(|_| {
            refusal(
                "host-drain-state-invalid",
                "queue protocol metadata is missing or invalid",
            )
        })?;
    let protocol = protocol_value
        .parse::<u64>()
        .ok()
        .filter(|protocol| (1..=PROTOCOL).contains(protocol))
        .ok_or_else(|| {
            refusal(
                "host-drain-protocol-mismatch",
                format!("host activation does not support spool protocol {protocol_value}"),
            )
        })?;
    let live: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM runs WHERE status IN ('queued', 'running')",
            [],
            |row| row.get(0),
        )
        .map_err(|_| refusal("host-drain-state-invalid", "cannot count live queue rows"))?;
    if live < 0 {
        return Err(refusal(
            "host-drain-state-invalid",
            "queue returned an invalid live-row count",
        ));
    }
    if live != 0 {
        return Err(refusal(
            "host-drain-live-work",
            format!("{live} queued or running row(s) remain; drain or cancel them first"),
        ));
    }
    Ok(json!({"drained": true, "live": live, "protocol": protocol}))
}

pub fn drain_check(state_dir: &Path) -> Result<Value> {
    let state = validate_state_directory(state_dir)?;
    let _lock = existing_maintenance_lock(state_dir, &state)?;
    drained_database(state_dir)
}

pub fn drain_hold(state_dir: &Path) -> Result<()> {
    let state = validate_state_directory(state_dir)?;
    let _lock = maintenance_lock(state_dir, &state)?;
    let result = drained_database(state_dir)?;
    let mut output = std::io::stdout().lock();
    writeln!(output, "{result}").map_err(|error| {
        refusal(
            "host-drain-hold-failed",
            format!("cannot report the acquired maintenance lock: {error}"),
        )
    })?;
    output.flush().map_err(|error| {
        refusal(
            "host-drain-hold-failed",
            format!("cannot flush the maintenance-lock report: {error}"),
        )
    })?;
    drop(output);

    let mut input = std::io::stdin().lock();
    let mut buffer = [0_u8; 256];
    loop {
        match input.read(&mut buffer) {
            Ok(0) => return Ok(()),
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) => {
                return Err(refusal(
                    "host-drain-hold-failed",
                    format!("maintenance-lock control pipe failed: {error}"),
                ));
            }
        }
    }
}
