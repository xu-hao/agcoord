use crate::error::{AppError, Result};
use serde_json::{Value, json};
use std::ffi::CString;
use std::fs;
use std::io::ErrorKind;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::time::Duration;

pub const WIRE_VERSION: u64 = 1;
pub const SOCKET_NAME: &str = "broker.sock";
pub const MAX_FRAME_BYTES: usize = 1024 * 1024 + 4096;
const PACKET_CHUNK_BYTES: usize = 32 * 1024;
const RECV_PACKET_BYTES: usize = 64 * 1024;
const CLIENT_IO_TIMEOUT: Duration = Duration::from_secs(2);
const LISTEN_BACKLOG: libc::c_int = 64;

pub struct SocketListener {
    fd: OwnedFd,
    path: PathBuf,
}

impl SocketListener {
    pub fn bind(state_dir: &Path) -> Result<Self> {
        let path = state_dir.join(SOCKET_NAME);
        let raw_bytes = path.as_os_str().as_bytes();
        if raw_bytes.is_empty() || raw_bytes.contains(&0) {
            return Err(AppError::new(
                "broker-socket-failed",
                format!("invalid broker socket path: {}", path.display()),
            ));
        }
        let c_path = CString::new(raw_bytes).map_err(|_| {
            AppError::new(
                "broker-socket-failed",
                format!("invalid broker socket path: {}", path.display()),
            )
        })?;
        // SAFETY: `sockaddr_un` is a plain C struct; zero-initialization is valid.
        let mut addr: libc::sockaddr_un = unsafe { std::mem::zeroed() };
        addr.sun_family = libc::AF_UNIX as libc::sa_family_t;
        let mut _long_dir_fd: Option<OwnedFd> = None;
        let bind_c_path = if c_path.as_bytes_with_nul().len() <= addr.sun_path.len() {
            c_path
        } else {
            let dir_c = CString::new(state_dir.as_os_str().as_bytes()).map_err(|_| {
                AppError::new(
                    "broker-socket-failed",
                    format!("invalid state directory path: {}", state_dir.display()),
                )
            })?;
            // SAFETY: `dir_c` is NUL-terminated; O_DIRECTORY ensures a real directory descriptor.
            let raw_dir = unsafe {
                libc::open(
                    dir_c.as_ptr(),
                    libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
                )
            };
            if raw_dir < 0 {
                let error = std::io::Error::last_os_error();
                return Err(AppError::new(
                    "broker-socket-failed",
                    format!(
                        "cannot open state directory for long socket path {}: {error}",
                        state_dir.display()
                    ),
                ));
            }
            // SAFETY: `raw_dir` is a valid newly opened directory descriptor checked `>= 0`.
            let dir_fd = unsafe { OwnedFd::from_raw_fd(raw_dir) };
            let proc_path = format!("/proc/self/fd/{}/{SOCKET_NAME}", dir_fd.as_raw_fd());
            _long_dir_fd = Some(dir_fd);
            CString::new(proc_path).unwrap()
        };
        let path_bytes = bind_c_path.as_bytes_with_nul();
        if path_bytes.len() > addr.sun_path.len() {
            return Err(AppError::new(
                "broker-socket-failed",
                format!(
                    "broker socket path exceeds {} bytes: {}",
                    addr.sun_path.len() - 1,
                    path.display()
                ),
            ));
        }

        match fs::symlink_metadata(&path) {
            Ok(metadata) => {
                if metadata.file_type().is_socket() {
                    if let Err(error) = fs::remove_file(&path)
                        && error.kind() != ErrorKind::NotFound
                    {
                        return Err(AppError::new(
                            "broker-socket-failed",
                            format!(
                                "cannot remove stale broker socket {}: {error}",
                                path.display()
                            ),
                        ));
                    }
                } else {
                    return Err(AppError::new(
                        "broker-socket-failed",
                        format!("broker socket path is obstructed: {}", path.display()),
                    ));
                }
            }
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => {
                return Err(AppError::new(
                    "broker-socket-failed",
                    format!(
                        "cannot inspect broker socket path {}: {error}",
                        path.display()
                    ),
                ));
            }
        }

        // SAFETY: `socket` takes only integer constants and returns `-1` on failure.
        let raw_fd = unsafe {
            libc::socket(
                libc::AF_UNIX,
                libc::SOCK_SEQPACKET | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
                0,
            )
        };
        if raw_fd < 0 {
            let error = std::io::Error::last_os_error();
            return Err(AppError::new(
                "broker-socket-failed",
                format!("cannot create broker socket: {error}"),
            ));
        }
        // SAFETY: `raw_fd` is a newly created, owned socket file descriptor checked `>= 0`.
        let fd = unsafe { OwnedFd::from_raw_fd(raw_fd) };

        for (slot, byte) in addr.sun_path.iter_mut().zip(path_bytes.iter()) {
            *slot = *byte as libc::c_char;
        }
        let addr_len = std::mem::size_of::<libc::sockaddr_un>() as libc::socklen_t;
        // SAFETY: `fd` is a valid AF_UNIX socket descriptor and `addr` is initialized with `path_bytes`.
        let bind_rc = unsafe {
            libc::bind(
                fd.as_raw_fd(),
                (&raw const addr).cast::<libc::sockaddr>(),
                addr_len,
            )
        };
        if bind_rc < 0 {
            let error = std::io::Error::last_os_error();
            return Err(AppError::new(
                "broker-socket-failed",
                format!("cannot bind broker socket {}: {error}", path.display()),
            ));
        }

        // Construct `Self` as soon as the socket inode exists so any later failure still unlinks it.
        let listener = Self { fd, path };
        fs::set_permissions(&listener.path, fs::Permissions::from_mode(0o600)).map_err(
            |error| {
                AppError::new(
                    "broker-socket-failed",
                    format!(
                        "cannot protect broker socket {}: {error}",
                        listener.path.display()
                    ),
                )
            },
        )?;
        // SAFETY: `listener.fd` is a bound AF_UNIX SOCK_SEQPACKET socket descriptor.
        if unsafe { libc::listen(listener.fd.as_raw_fd(), LISTEN_BACKLOG) } < 0 {
            let error = std::io::Error::last_os_error();
            return Err(AppError::new(
                "broker-socket-failed",
                format!(
                    "cannot listen on broker socket {}: {error}",
                    listener.path.display()
                ),
            ));
        }
        Ok(listener)
    }

    /// Wait up to `timeout` for a new client connection, returning `true` when ready.
    /// Signals (`EINTR`) wake early with `Ok(false)` so the broker checks its stop flag promptly.
    pub fn poll_or_sleep(&self, timeout: Duration) -> Result<bool> {
        let mut pfd = libc::pollfd {
            fd: self.fd.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        let timeout_ms = i32::try_from(timeout.as_millis()).unwrap_or(i32::MAX);
        // SAFETY: `pfd` points to one valid `pollfd` covering `self.fd` for this call.
        let rc = unsafe { libc::poll(&raw mut pfd, 1, timeout_ms) };
        if rc > 0 {
            return Ok(true);
        }
        if rc == 0 {
            return Ok(false);
        }
        let error = std::io::Error::last_os_error();
        if error.raw_os_error() == Some(libc::EINTR) {
            return Ok(false);
        }
        Err(AppError::new(
            "broker-socket-failed",
            format!("cannot poll broker socket: {error}"),
        ))
    }

    pub fn serve_pending(&self, state_dir: &Path, max_requests: usize) -> usize {
        let mut served = 0;
        for _ in 0..max_requests {
            // SAFETY: `self.fd` is a non-blocking listening socket; null peer pointers are allowed.
            let raw_client = unsafe {
                libc::accept4(
                    self.fd.as_raw_fd(),
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                    libc::SOCK_CLOEXEC,
                )
            };
            if raw_client < 0 {
                let error = std::io::Error::last_os_error();
                if error.kind() == ErrorKind::Interrupted {
                    continue;
                }
                break;
            }
            // SAFETY: `raw_client` is a newly accepted socket descriptor checked `>= 0`.
            let client = unsafe { OwnedFd::from_raw_fd(raw_client) };
            handle_client(client, state_dir);
            served += 1;
        }
        served
    }
}

impl Drop for SocketListener {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

pub fn current_euid() -> libc::uid_t {
    // SAFETY: `geteuid` takes no arguments and always succeeds.
    unsafe { libc::geteuid() }
}

pub fn peer_uid(fd: RawFd) -> Result<libc::uid_t> {
    // SAFETY: `ucred` is plain C data; zero-initialization before `getsockopt` is valid.
    let mut cred: libc::ucred = unsafe { std::mem::zeroed() };
    let mut len = std::mem::size_of::<libc::ucred>() as libc::socklen_t;
    // SAFETY: `cred` and `len` match `SO_PEERCRED` expectations for Unix sockets on Linux.
    let rc = unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_PEERCRED,
            (&raw mut cred).cast::<libc::c_void>(),
            &raw mut len,
        )
    };
    if rc < 0 || len as usize != std::mem::size_of::<libc::ucred>() {
        let error = std::io::Error::last_os_error();
        return Err(AppError::new(
            "broker-socket-unauthorized",
            format!("cannot read peer credentials: {error}"),
        ));
    }
    Ok(cred.uid)
}

pub fn authorize_peer_uid(actual_uid: libc::uid_t, expected_uid: libc::uid_t) -> Result<()> {
    if actual_uid != expected_uid {
        return Err(AppError::new(
            "broker-socket-unauthorized",
            format!(
                "socket client uid {actual_uid} does not match broker owner uid {expected_uid}"
            ),
        ));
    }
    Ok(())
}

fn set_socket_timeout(fd: RawFd, optname: libc::c_int, timeout: Duration) {
    let tv = libc::timeval {
        tv_sec: timeout.as_secs() as libc::time_t,
        tv_usec: timeout.subsec_micros() as libc::suseconds_t,
    };
    // SAFETY: `tv` matches `timeval` expected by `SO_RCVTIMEO`/`SO_SNDTIMEO`.
    unsafe {
        let _ = libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            optname,
            (&raw const tv).cast::<libc::c_void>(),
            std::mem::size_of::<libc::timeval>() as libc::socklen_t,
        );
    }
}

fn handle_client(client: OwnedFd, state_dir: &Path) {
    let fd = client.as_raw_fd();
    set_socket_timeout(fd, libc::SO_RCVTIMEO, CLIENT_IO_TIMEOUT);
    set_socket_timeout(fd, libc::SO_SNDTIMEO, CLIENT_IO_TIMEOUT);

    let uid = match peer_uid(fd) {
        Ok(uid) => uid,
        Err(error) => {
            let _ = write_error(fd, &error);
            return;
        }
    };
    if let Err(error) = authorize_peer_uid(uid, current_euid()) {
        let _ = write_error(fd, &error);
        return;
    }

    let request_bytes = match read_frame(fd) {
        Ok(bytes) => bytes,
        Err(error) => {
            let _ = write_error(fd, &error);
            return;
        }
    };

    match process_request_bytes(&request_bytes, state_dir) {
        Ok(value) => {
            let payload = serde_json::to_vec(&json!({
                "ok": true,
                "wire": WIRE_VERSION,
                "result": value,
            }))
            .unwrap();
            let _ = write_frame(fd, &payload);
        }
        Err(error) => {
            let _ = write_error(fd, &error);
        }
    }
}

fn write_error(fd: RawFd, error: &AppError) -> Result<()> {
    let payload = serde_json::to_vec(&json!({
        "ok": false,
        "wire": WIRE_VERSION,
        "code": error.code,
        "message": error.message,
        "exit_status": error.exit_status,
    }))
    .unwrap();
    write_frame(fd, &payload)
}

fn process_request_bytes(bytes: &[u8], expected_state_dir: &Path) -> Result<Value> {
    let value: Value = serde_json::from_slice(bytes).map_err(|error| {
        AppError::usage(format!("socket request must be valid UTF-8 JSON: {error}"))
    })?;
    let object = value
        .as_object()
        .ok_or_else(|| AppError::usage("socket request must be a JSON object"))?;
    let wire = object.get("wire").and_then(Value::as_u64);
    if wire != Some(WIRE_VERSION) {
        return Err(AppError::new(
            "broker-wire-unsupported",
            format!(
                "unsupported broker socket wire version: {:?}",
                object.get("wire")
            ),
        ));
    }
    let command = object
        .get("command")
        .and_then(Value::as_str)
        .ok_or_else(|| AppError::usage("socket request command must be a string"))?;
    let raw_args = object
        .get("arguments")
        .and_then(Value::as_array)
        .ok_or_else(|| AppError::usage("socket request arguments must be an array"))?;
    let mut arguments = Vec::with_capacity(raw_args.len());
    for entry in raw_args {
        let arg = entry
            .as_str()
            .ok_or_else(|| AppError::usage("socket request arguments must be strings"))?;
        arguments.push(arg.to_owned());
    }
    verify_state_dir(expected_state_dir, &arguments)?;
    crate::dispatch_rpc_command(command, &arguments)
}

fn verify_state_dir(expected_state_dir: &Path, arguments: &[String]) -> Result<()> {
    let mut index = 0;
    let mut requested: Option<PathBuf> = None;
    while index < arguments.len() {
        if arguments[index] == "--" {
            break;
        }
        if arguments[index] == "--state-dir" {
            let value = arguments
                .get(index + 1)
                .ok_or_else(|| AppError::usage("--state-dir requires a value"))?;
            if requested.is_some() {
                return Err(AppError::usage("duplicate --state-dir"));
            }
            requested = Some(PathBuf::from(value));
            index += 2;
            continue;
        }
        index += 1;
    }
    let requested = requested.ok_or_else(|| AppError::usage("--state-dir is required"))?;
    let expected =
        fs::canonicalize(expected_state_dir).unwrap_or_else(|_| expected_state_dir.to_path_buf());
    let actual = fs::canonicalize(&requested).map_err(|_| {
        AppError::new(
            "broker-state-mismatch",
            format!(
                "socket request state directory does not match this broker: {}",
                requested.display()
            ),
        )
    })?;
    if actual != expected {
        return Err(AppError::new(
            "broker-state-mismatch",
            format!(
                "socket request state directory {} does not match {}",
                requested.display(),
                expected_state_dir.display()
            ),
        ));
    }
    Ok(())
}

/// Read one 4-byte length-prefixed frame reassembled from `SOCK_SEQPACKET` chunks.
pub fn read_frame(fd: RawFd) -> Result<Vec<u8>> {
    let mut buffer = Vec::new();
    let mut packet = vec![0u8; RECV_PACKET_BYTES];
    let mut expected_len: Option<usize> = None;

    loop {
        if let Some(total) = expected_len
            && buffer.len() >= 4 + total
        {
            if buffer.len() > 4 + total {
                return Err(AppError::usage("socket frame contains trailing bytes"));
            }
            return Ok(buffer.split_off(4));
        }
        // SAFETY: `packet` provides a valid writable buffer of `packet.len()` bytes.
        let n = unsafe {
            libc::recv(
                fd,
                packet.as_mut_ptr().cast::<libc::c_void>(),
                packet.len(),
                libc::MSG_TRUNC,
            )
        };
        if n < 0 {
            let error = std::io::Error::last_os_error();
            if error.raw_os_error() == Some(libc::EINTR) {
                continue;
            }
            return Err(AppError::new(
                "broker-socket-failed",
                format!("cannot read broker socket frame: {error}"),
            ));
        }
        if n == 0 {
            return Err(AppError::new(
                "broker-socket-failed",
                "unexpected EOF reading broker socket frame",
            ));
        }
        let n = n as usize;
        if n > packet.len() {
            return Err(AppError::new(
                "broker-socket-failed",
                format!("socket packet exceeded {RECV_PACKET_BYTES} byte limit"),
            ));
        }
        if buffer.len() + n > 4 + MAX_FRAME_BYTES {
            return Err(AppError::new(
                "broker-socket-failed",
                "socket frame exceeds maximum byte limit",
            ));
        }
        buffer.extend_from_slice(&packet[..n]);
        if expected_len.is_none() && buffer.len() >= 4 {
            let total = u32::from_be_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]) as usize;
            if total > MAX_FRAME_BYTES {
                return Err(AppError::new(
                    "broker-socket-failed",
                    "declared socket frame exceeds maximum byte limit",
                ));
            }
            expected_len = Some(total);
        }
    }
}

/// Write one 4-byte length-prefixed frame split into bounded `SOCK_SEQPACKET` chunks.
pub fn write_frame(fd: RawFd, payload: &[u8]) -> Result<()> {
    if payload.len() > MAX_FRAME_BYTES {
        return Err(AppError::new(
            "broker-socket-failed",
            "socket response exceeds maximum byte limit",
        ));
    }
    let header = (payload.len() as u32).to_be_bytes();
    let mut framed = Vec::with_capacity(4 + payload.len());
    framed.extend_from_slice(&header);
    framed.extend_from_slice(payload);

    for chunk in framed.chunks(PACKET_CHUNK_BYTES) {
        loop {
            // SAFETY: `chunk` points to `chunk.len()` valid readable bytes; `MSG_NOSIGNAL` prevents SIGPIPE.
            let sent = unsafe {
                libc::send(
                    fd,
                    chunk.as_ptr().cast::<libc::c_void>(),
                    chunk.len(),
                    libc::MSG_NOSIGNAL,
                )
            };
            if sent < 0 {
                let error = std::io::Error::last_os_error();
                if error.raw_os_error() == Some(libc::EINTR) {
                    continue;
                }
                return Err(AppError::new(
                    "broker-socket-failed",
                    format!("cannot write broker socket frame: {error}"),
                ));
            }
            if sent as usize != chunk.len() {
                return Err(AppError::new(
                    "broker-socket-failed",
                    "short write on SOCK_SEQPACKET socket",
                ));
            }
            break;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    struct TempDir(PathBuf);

    impl TempDir {
        fn new(label: &str) -> Self {
            let unique = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "agcoord-socket-test-{label}-{}-{unique}",
                std::process::id()
            ));
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn authorize_peer_uid_accepts_owner_and_rejects_foreign_uid() {
        authorize_peer_uid(1000, 1000).unwrap();
        let error = authorize_peer_uid(1001, 1000).unwrap_err();
        assert_eq!(error.code, "broker-socket-unauthorized");
    }

    #[test]
    fn framed_seqpacket_roundtrip_handles_payloads_above_socket_buffer() {
        let mut raw = [0 as libc::c_int; 2];
        // SAFETY: `raw` is a two-element output buffer for `socketpair`.
        assert_eq!(
            unsafe {
                libc::socketpair(
                    libc::AF_UNIX,
                    libc::SOCK_SEQPACKET | libc::SOCK_CLOEXEC,
                    0,
                    raw.as_mut_ptr(),
                )
            },
            0
        );
        // SAFETY: both descriptors returned by `socketpair` are newly opened and valid.
        let (left, right) = unsafe { (OwnedFd::from_raw_fd(raw[0]), OwnedFd::from_raw_fd(raw[1])) };
        let payload = vec![b'x'; 512 * 1024 + 17];
        let expected = payload.clone();
        let writer = std::thread::spawn(move || write_frame(left.as_raw_fd(), &payload));
        let received = read_frame(right.as_raw_fd()).unwrap();
        writer.join().unwrap().unwrap();
        assert_eq!(received, expected);
    }

    #[test]
    fn listener_unlinks_stale_socket_and_rejects_obstruction() {
        let dir = TempDir::new("bind");
        let sock_path = dir.0.join(SOCKET_NAME);
        // Create a stale closed Unix socket inode at broker.sock.
        drop(std::os::unix::net::UnixListener::bind(&sock_path).unwrap());
        assert!(
            fs::symlink_metadata(&sock_path)
                .unwrap()
                .file_type()
                .is_socket()
        );

        {
            let listener = SocketListener::bind(&dir.0).unwrap();
            let mode = fs::symlink_metadata(&sock_path)
                .unwrap()
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(mode, 0o600);
            drop(listener);
        }
        assert!(!sock_path.exists());

        // Now obstruct broker.sock with a directory and verify clean refusal.
        fs::create_dir(&sock_path).unwrap();
        let error = match SocketListener::bind(&dir.0) {
            Ok(_) => panic!("directory obstruction should fail"),
            Err(error) => error,
        };
        assert_eq!(error.code, "broker-socket-failed");

        // Regular file (non-socket) should also fail rather than unlinking unrelated state.
        fs::remove_dir(&sock_path).unwrap();
        fs::write(&sock_path, b"not-a-socket").unwrap();
        let error = match SocketListener::bind(&dir.0) {
            Ok(_) => panic!("regular file obstruction should fail"),
            Err(error) => error,
        };
        assert_eq!(error.code, "broker-socket-failed");
    }

    #[test]
    fn listener_binds_long_state_dir_via_dirfd() {
        let dir = TempDir::new("long-dir");
        let deep = dir.0.join("nested-directory-component-padding-to-exceed-sun-path-length-limit-on-linux-unix-sockets-safely");
        fs::create_dir_all(&deep).unwrap();
        let sock_path = deep.join(SOCKET_NAME);
        assert!(sock_path.as_os_str().as_bytes().len() > 107);

        let listener = SocketListener::bind(&deep).unwrap();
        let mode = fs::symlink_metadata(&sock_path)
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600);
        drop(listener);
        assert!(!sock_path.exists());
    }

    #[test]
    fn process_request_bytes_rejects_state_dir_mismatch() {
        let dir_a = TempDir::new("state-a");
        let dir_b = TempDir::new("state-b");
        let raw = serde_json::to_vec(&json!({
            "wire": WIRE_VERSION,
            "command": "inspect",
            "arguments": ["--state-dir", dir_b.0.to_string_lossy()],
        }))
        .unwrap();
        let error = process_request_bytes(&raw, &dir_a.0).unwrap_err();
        assert_eq!(error.code, "broker-state-mismatch");
    }

    #[test]
    fn process_request_bytes_rejects_unsupported_wire_version() {
        let dir = TempDir::new("wire");
        let raw = serde_json::to_vec(&json!({
            "wire": WIRE_VERSION + 1,
            "command": "inspect",
            "arguments": ["--state-dir", dir.0.to_string_lossy()],
        }))
        .unwrap();
        let error = process_request_bytes(&raw, &dir.0).unwrap_err();
        assert_eq!(error.code, "broker-wire-unsupported");
    }
}
