use serde_json::Value;
use std::fs;
use std::io::{BufRead, BufReader};
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::thread;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

const BROKER: &str = env!("CARGO_BIN_EXE_agcoord-broker");

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new(name: &str) -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "agcoord-native-host-{name}-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn write(path: &Path, value: &str) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, value).unwrap();
}

fn sha256(path: &Path) -> String {
    let output = Command::new("sha256sum").arg(path).output().unwrap();
    assert!(output.status.success());
    String::from_utf8(output.stdout)
        .unwrap()
        .split_whitespace()
        .next()
        .unwrap()
        .to_owned()
}

fn repository_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf()
}

struct HostFixture {
    _temporary: TestDirectory,
    root: PathBuf,
    state: PathBuf,
    executable: PathBuf,
    profile: PathBuf,
    restriction: PathBuf,
    cgroup: PathBuf,
    mountinfo: PathBuf,
    controllers: PathBuf,
    service_root: PathBuf,
}

impl HostFixture {
    fn new() -> Self {
        let temporary = TestDirectory::new("preflight");
        let root = temporary.path().join("fixture");
        let state = temporary.path().join("state");
        let executable = temporary.path().join("usr/libexec/agcoord/agcoord-broker");
        fs::create_dir_all(executable.parent().unwrap()).unwrap();
        fs::copy(BROKER, &executable).unwrap();
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o755)).unwrap();
        write(
            &executable.with_file_name("agcoord-broker.sha256"),
            &format!("{}  agcoord-broker\n", sha256(&executable)),
        );

        let service =
            "/user.slice/user-1000.slice/user@1000.service/app.slice/agcoord-broker.service";
        let virtual_root = format!("/sys/fs/cgroup{service}");
        let physical_root = root.join(virtual_root.trim_start_matches('/'));
        fs::create_dir_all(physical_root.join("supervisor")).unwrap();
        let controllers = physical_root.join("cgroup.controllers");
        write(&controllers, "cpu memory pids\n");
        write(&physical_root.join("cgroup.procs"), "\n");
        write(
            &physical_root.join("supervisor/cgroup.procs"),
            &format!("{}\n", std::process::id()),
        );

        let profile = root.join("proc/self/attr/current");
        let restriction = root.join("proc/sys/kernel/apparmor_restrict_unprivileged_userns");
        let cgroup = root.join("proc/self/cgroup");
        write(&profile, "agcoord-broker (enforce)\n");
        write(&restriction, "1\n");
        write(&cgroup, &format!("0::{service}/supervisor\n"));
        let mountinfo = root.join("proc/self/mountinfo");
        write(
            &mountinfo,
            "31 23 0:28 / /sys/fs/cgroup rw,nosuid,nodev,noexec,relatime,nsdelegate - cgroup2 cgroup rw,nsdelegate\n",
        );
        fs::create_dir_all(&state).unwrap();
        write(
            &state.join("config.json"),
            &format!(
                concat!(
                    "{{\"capacities\":{{\"jobs\":2,\"cpu\":2}},",
                    "\"cgroup_root\":\"{}\",",
                    "\"native_broker\":{{\"path\":\"{}\",",
                    "\"allow_development\":true,\"managed_service\":true}}}}"
                ),
                virtual_root,
                executable.display()
            ),
        );
        Self {
            _temporary: temporary,
            root,
            state,
            executable,
            profile,
            restriction,
            cgroup,
            mountinfo,
            controllers,
            service_root: physical_root,
        }
    }

    fn run(&self) -> Output {
        Command::new(BROKER)
            .args([
                "host-preflight",
                "--state-dir",
                self.state.to_str().unwrap(),
                "--fixture-root",
                self.root.to_str().unwrap(),
                "--executable",
                self.executable.to_str().unwrap(),
            ])
            .output()
            .unwrap()
    }
}

fn refusal(output: &Output) -> Value {
    assert!(!output.status.success());
    serde_json::from_slice(&output.stderr).unwrap()
}

#[test]
fn managed_host_preflight_verifies_binary_profile_restriction_and_service_cgroup() {
    let fixture = HostFixture::new();
    let output = fixture.run();
    assert!(
        output.status.success(),
        "preflight failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let result: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["ready"], true);
    assert_eq!(result["protocol"], 5);
    assert_eq!(result["profile"], "agcoord-broker");
    assert_eq!(result["service_subgroup"], "supervisor");
    assert_eq!(
        result["controllers"],
        serde_json::json!(["cpu", "memory", "pids"])
    );
    assert_eq!(result["sha256"].as_str().unwrap().len(), 64);
}

#[test]
fn managed_host_preflight_fails_closed_for_each_trust_boundary() {
    let fixture = HostFixture::new();

    write(&fixture.profile, "unconfined\n");
    assert_eq!(
        refusal(&fixture.run())["code"],
        "host-apparmor-profile-mismatch"
    );
    write(&fixture.profile, "agcoord-broker (enforce)\n");

    write(&fixture.restriction, "0\n");
    assert_eq!(
        refusal(&fixture.run())["code"],
        "host-apparmor-restriction-disabled"
    );
    write(&fixture.restriction, "1\n");

    write(
        &fixture.mountinfo,
        "31 23 0:28 / /sys/fs/cgroup ro,nosuid,nodev,noexec,relatime,nsdelegate - cgroup2 cgroup ro,nsdelegate\n",
    );
    assert_eq!(refusal(&fixture.run())["code"], "host-cgroup-mount-invalid");
    write(
        &fixture.mountinfo,
        "31 23 0:28 / /sys/fs/cgroup rw,nosuid,nodev,noexec,relatime,nsdelegate - cgroup2 cgroup rw,nsdelegate\n",
    );

    write(&fixture.controllers, "memory pids\n");
    assert_eq!(
        refusal(&fixture.run())["code"],
        "host-cgroup-controllers-unavailable"
    );
    write(&fixture.controllers, "cpu memory pids\n");

    fs::set_permissions(&fixture.service_root, fs::Permissions::from_mode(0o777)).unwrap();
    assert_eq!(
        refusal(&fixture.run())["code"],
        "host-service-cgroup-mismatch"
    );
    fs::set_permissions(&fixture.service_root, fs::Permissions::from_mode(0o755)).unwrap();

    write(
        &fixture.cgroup,
        "0::/user.slice/not-the-service/supervisor\n",
    );
    assert_eq!(
        refusal(&fixture.run())["code"],
        "host-service-cgroup-mismatch"
    );
    write(
        &fixture.cgroup,
        "0::/user.slice/user-1000.slice/user@1000.service/app.slice/agcoord-broker.service/supervisor\n",
    );

    write(
        &fixture.executable.with_file_name("agcoord-broker.sha256"),
        &format!("{}  agcoord-broker\n", "0".repeat(64)),
    );
    assert_eq!(
        refusal(&fixture.run())["code"],
        "host-executable-digest-mismatch"
    );

    fs::set_permissions(&fixture.executable, fs::Permissions::from_mode(0o777)).unwrap();
    assert_eq!(refusal(&fixture.run())["code"], "host-executable-invalid");
}

#[test]
fn host_client_preflight_refuses_outside_the_restricted_transition() {
    let output = Command::new(BROKER)
        .arg("host-client-preflight")
        .output()
        .unwrap();
    assert_eq!(refusal(&output)["code"], "host-client-profile-mismatch");
}

#[test]
fn maintenance_holder_creates_the_owner_lock_and_excludes_a_broker_until_release() {
    let temporary = TestDirectory::new("maintenance-hold");
    let state = temporary.path().join("state");
    fs::create_dir(&state).unwrap();
    fs::set_permissions(&state, fs::Permissions::from_mode(0o700)).unwrap();
    let mut holder = Command::new(BROKER)
        .args(["host-drain-hold", "--state-dir", state.to_str().unwrap()])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut ready = String::new();
    BufReader::new(holder.stdout.take().unwrap())
        .read_line(&mut ready)
        .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&ready).unwrap(),
        serde_json::json!({"drained": true, "live": 0, "protocol": null})
    );
    let lock = fs::metadata(state.join("broker.lock")).unwrap();
    assert_eq!(lock.permissions().mode() & 0o777, 0o600);
    assert_eq!(lock.uid(), fs::metadata(&state).unwrap().uid());

    let refused = Command::new(BROKER)
        .args([
            "serve",
            "--state-dir",
            state.to_str().unwrap(),
            "--capacity",
            "jobs=1",
        ])
        .output()
        .unwrap();
    assert_eq!(refusal(&refused)["code"], "broker-already-owned");

    drop(holder.stdin.take());
    let status = holder.wait().unwrap();
    assert!(status.success());
    let drained = Command::new(BROKER)
        .args(["host-drain-check", "--state-dir", state.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(drained.status.success());
}

#[test]
fn managed_service_uses_configured_capacity_and_has_no_idle_shutdown() {
    let fixture = HostFixture::new();
    let mut broker = Command::new(BROKER)
        .args([
            "serve",
            "--state-dir",
            fixture.state.to_str().unwrap(),
            "--host-managed",
            "--host-fixture-root",
            fixture.root.to_str().unwrap(),
            "--host-executable",
            fixture.executable.to_str().unwrap(),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let lock = fixture.state.join("broker.lock");
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let owner = loop {
        if let Ok(owner) = fs::read_to_string(&lock)
            && owner.contains("protocol=5\n")
        {
            break owner;
        }
        if let Some(status) = broker.try_wait().unwrap() {
            let stderr = broker
                .stderr
                .take()
                .map(|stderr| std::io::read_to_string(stderr).unwrap())
                .unwrap_or_default();
            panic!("managed broker exited as {status}: {stderr}");
        }
        assert!(
            std::time::Instant::now() < deadline,
            "managed broker did not start"
        );
        thread::sleep(Duration::from_millis(20));
    };
    assert!(owner.contains("capacities={\"cpu\":2,\"jobs\":2}\n"));
    let live_drain = Command::new(BROKER)
        .args([
            "host-drain-check",
            "--state-dir",
            fixture.state.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert_eq!(refusal(&live_drain)["code"], "host-drain-owner-live");
    thread::sleep(Duration::from_millis(250));
    assert!(broker.try_wait().unwrap().is_none());
    let _ = broker.kill();
    let _ = broker.wait();
    let drained = Command::new(BROKER)
        .args([
            "host-drain-check",
            "--state-dir",
            fixture.state.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        drained.status.success(),
        "{}",
        String::from_utf8_lossy(&drained.stderr)
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&drained.stdout).unwrap(),
        serde_json::json!({"drained": true, "live": 0, "protocol": 5})
    );

    let connection = rusqlite::Connection::open(fixture.state.join("queue.sqlite3")).unwrap();
    connection
        .execute(
            "UPDATE coordinator_meta SET value = '4' WHERE key = 'protocol'",
            [],
        )
        .unwrap();
    drop(connection);
    let legacy = Command::new(BROKER)
        .args([
            "host-drain-check",
            "--state-dir",
            fixture.state.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        legacy.status.success(),
        "{}",
        String::from_utf8_lossy(&legacy.stderr)
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&legacy.stdout).unwrap()["protocol"],
        4
    );
}

#[test]
fn host_bundle_is_reproducible_and_rejects_tampering() {
    let temporary = TestDirectory::new("package");
    let repository = repository_root();
    let output = temporary.path().join("dist");
    let first = Command::new(repository.join("scripts/build-native-host-package"))
        .args([BROKER, output.to_str().unwrap()])
        .env("AGCOORD_HOST_ALLOW_DEVELOPMENT", "1")
        .output()
        .unwrap();
    assert!(
        first.status.success(),
        "host package failed: {}",
        String::from_utf8_lossy(&first.stderr)
    );
    let package = output.join("agcoord-native-host-x86_64-linux.tar.gz");
    let first_digest = sha256(&package);
    for helper in [
        "install-native-host",
        "check-native-host-package",
        "test-native-host-enforcement",
    ] {
        let path = output.join(helper);
        assert!(path.is_file());
        assert_ne!(fs::metadata(path).unwrap().permissions().mode() & 0o111, 0);
    }

    let second = Command::new(repository.join("scripts/build-native-host-package"))
        .args([BROKER, output.to_str().unwrap()])
        .env("AGCOORD_HOST_ALLOW_DEVELOPMENT", "1")
        .output()
        .unwrap();
    assert!(second.status.success());
    assert_eq!(sha256(&package), first_digest);

    let mut bytes = fs::read(&package).unwrap();
    bytes[32] ^= 1;
    fs::write(&package, bytes).unwrap();
    let refused = Command::new(repository.join("scripts/check-native-host-package"))
        .arg(&package)
        .output()
        .unwrap();
    assert!(!refused.status.success());
}

#[test]
fn host_installer_stages_without_live_changes_and_activates_only_after_drain() {
    let temporary = TestDirectory::new("install");
    let repository = repository_root();
    let output = temporary.path().join("dist");
    let built = Command::new(repository.join("scripts/build-native-host-package"))
        .args([BROKER, output.to_str().unwrap()])
        .env("AGCOORD_HOST_ALLOW_DEVELOPMENT", "1")
        .output()
        .unwrap();
    assert!(
        built.status.success(),
        "host package failed: {}",
        String::from_utf8_lossy(&built.stderr)
    );
    let package = output.join("agcoord-native-host-x86_64-linux.tar.gz");
    let image = temporary.path().join("image");
    let installer = repository.join("scripts/install-native-host");
    let staged = Command::new(&installer)
        .args([
            "stage",
            package.to_str().unwrap(),
            "--root",
            image.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        staged.status.success(),
        "host staging failed: {}",
        String::from_utf8_lossy(&staged.stderr)
    );
    let installed = image.join("usr/libexec/agcoord/agcoord-broker");
    assert!(!installed.exists(), "staging changed the live binary");

    let fixture = HostFixture::new();
    let mut broker = Command::new(BROKER)
        .args([
            "serve",
            "--state-dir",
            fixture.state.to_str().unwrap(),
            "--host-managed",
            "--host-fixture-root",
            fixture.root.to_str().unwrap(),
            "--host-executable",
            fixture.executable.to_str().unwrap(),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let ready = Command::new(BROKER)
            .args(["snapshot", "--state-dir", fixture.state.to_str().unwrap()])
            .output()
            .unwrap();
        if ready.status.success() {
            break;
        }
        assert!(
            broker.try_wait().unwrap().is_none(),
            "managed broker exited early"
        );
        assert!(
            std::time::Instant::now() < deadline,
            "managed broker did not become queryable: {}",
            String::from_utf8_lossy(&ready.stderr)
        );
        thread::sleep(Duration::from_millis(20));
    }

    let refused = Command::new(&installer)
        .args([
            "activate",
            fixture.state.to_str().unwrap(),
            "--root",
            image.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(!refused.status.success());
    assert!(String::from_utf8_lossy(&refused.stderr).contains("still owns"));
    assert!(
        !installed.exists(),
        "refused activation changed the live binary"
    );

    broker.kill().unwrap();
    broker.wait().unwrap();
    let activated = Command::new(&installer)
        .args([
            "activate",
            fixture.state.to_str().unwrap(),
            "--root",
            image.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        activated.status.success(),
        "host activation failed: {}",
        String::from_utf8_lossy(&activated.stderr)
    );
    assert!(String::from_utf8_lossy(&activated.stdout).contains("without restarting"));
    assert_eq!(
        fs::metadata(&installed).unwrap().permissions().mode() & 0o777,
        0o755
    );
    assert_eq!(
        sha256(&installed),
        fs::read_to_string(installed.with_file_name("agcoord-broker.sha256"))
            .unwrap()
            .split_whitespace()
            .next()
            .unwrap()
    );

    let fresh_image = temporary.path().join("fresh-image");
    let fresh_state = temporary.path().join("fresh-state");
    fs::create_dir(&fresh_state).unwrap();
    fs::set_permissions(&fresh_state, fs::Permissions::from_mode(0o700)).unwrap();
    let fresh_stage = Command::new(&installer)
        .args([
            "stage",
            package.to_str().unwrap(),
            "--root",
            fresh_image.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(fresh_stage.status.success());
    let fresh_activation = Command::new(&installer)
        .args([
            "activate",
            fresh_state.to_str().unwrap(),
            "--root",
            fresh_image.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        fresh_activation.status.success(),
        "fresh host activation failed: {}",
        String::from_utf8_lossy(&fresh_activation.stderr)
    );
    let fresh_lock = fs::metadata(fresh_state.join("broker.lock")).unwrap();
    assert_eq!(fresh_lock.permissions().mode() & 0o777, 0o600);
    assert_eq!(fresh_lock.uid(), fs::metadata(&fresh_state).unwrap().uid());
}
