use serde_json::{Value, json};
use std::fs::{self, File};
use std::io::Read;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::{Child, Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const BROKER: &str = env!("CARGO_BIN_EXE_agcoord-broker");

struct OwnedProcess(Child);

impl OwnedProcess {
    fn kill(&mut self) {
        if self.0.try_wait().unwrap().is_none() {
            self.0.kill().unwrap();
        }
        self.0.wait().unwrap();
    }
}

impl Drop for OwnedProcess {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            let _ = self.0.kill();
        }
        let _ = self.0.wait();
    }
}

struct Fixture {
    root: PathBuf,
    state: PathBuf,
    worker: Option<(u32, String)>,
}

impl Fixture {
    fn new() -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "agcoord-host-recovery-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir(&root).unwrap();
        let probe = root.join("enforcement-probe");
        fs::write(&probe, "#!/bin/sh\nexit 0\n").unwrap();
        fs::set_permissions(&probe, fs::Permissions::from_mode(0o700)).unwrap();
        Self {
            state: root.join("state"),
            root,
            worker: None,
        }
    }

    fn command(&self, operation: &str) -> Command {
        let mut command = Command::new(BROKER);
        command.arg(operation).arg("--state-dir").arg(&self.state);
        if operation == "host-recover-hold" {
            command
                .arg("--probe")
                .arg(self.root.join("enforcement-probe"));
        }
        command
    }

    fn start(&self) -> OwnedProcess {
        let mut process = OwnedProcess(
            self.command("serve")
                .args(["--capacity", "jobs=1", "--idle-timeout", "30"])
                .stdout(Stdio::null())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap(),
        );
        wait_for(|| {
            if let Some(status) = process.0.try_wait().unwrap() {
                let mut stderr = String::new();
                process
                    .0
                    .stderr
                    .take()
                    .unwrap()
                    .read_to_string(&mut stderr)
                    .unwrap();
                panic!("broker exited with {status}: {stderr}");
            }
            let output = self.command("snapshot").output().unwrap();
            output.status.success()
                && serde_json::from_slice::<Value>(&output.stdout).unwrap()["broker_pid"]
                    == json!(process.0.id())
        });
        process
    }

    fn submit(&self, id: &str, arguments: &[&str]) -> Output {
        self.command("submit")
            .args([
                "--run-id",
                id,
                "--kind",
                "check",
                "--label",
                id,
                "--repository-id",
                "recovery-test-repository",
                "--repository",
                "recovery-test-repository",
                "--worktree-id",
                "recovery-test-worktree",
                "--checkout",
                self.root.to_str().unwrap(),
                "--branch",
                "recovery-test",
                "--head",
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "--resource",
                "jobs=1",
                "--caller-pid",
                &std::process::id().to_string(),
                "--",
            ])
            .args(arguments)
            .output()
            .unwrap()
    }

    fn status(&self, id: &str) -> Value {
        parsed(
            self.command("status")
                .args(["--run-id", id])
                .output()
                .unwrap(),
        )
    }

    fn queue_behind_worker(&mut self) -> OwnedProcess {
        let broker = self.start();
        parsed(self.submit("retained-running", &["/bin/sleep", "300"]));
        wait_for(|| {
            let row = self.status("retained-running");
            if let Some(pid) = row["worker_pid"].as_u64()
                && let Some((state, token)) = process_identity(pid as u32)
                && state != "Z"
            {
                self.worker = Some((pid as u32, token));
                true
            } else {
                false
            }
        });
        parsed(self.submit(
            "retained-queued",
            &[
                "/usr/bin/touch",
                self.root.join("executed").to_str().unwrap(),
            ],
        ));
        assert_eq!(self.status("retained-queued")["status"], "queued");
        broker
    }

    fn stop_worker(&mut self) {
        let Some((pid, token)) = self.worker.take() else {
            return;
        };
        if process_identity(pid).is_some_and(|(_, current)| current == token) {
            // SAFETY: this process group belongs to the worker created by this fixture,
            // and the recorded leader's start token still matches before signalling it.
            unsafe { libc::kill(-(pid as libc::pid_t), libc::SIGKILL) };
        }
        wait_for(|| process_identity(pid).is_none_or(|(state, _)| state == "Z"));
    }

    fn hold(&self) -> (OwnedProcess, Value) {
        let output = self.root.join("holder-output");
        let error = self.root.join("holder-error");
        let mut holder = OwnedProcess(
            self.command("host-recover-hold")
                .stdin(Stdio::piped())
                .stdout(File::create(&output).unwrap())
                .stderr(File::create(&error).unwrap())
                .spawn()
                .unwrap(),
        );
        let mut receipt = None;
        wait_for(|| {
            if let Some(status) = holder.0.try_wait().unwrap() {
                panic!(
                    "recovery holder exited with {status}: {}",
                    fs::read_to_string(&error).unwrap()
                );
            }
            receipt = serde_json::from_slice::<Value>(&fs::read(&output).unwrap()).ok();
            receipt.is_some()
        });
        (holder, receipt.unwrap())
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.stop_worker();
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn process_identity(pid: u32) -> Option<(String, String)> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let fields: Vec<_> = stat[stat.rfind(')')? + 2..].split_whitespace().collect();
    Some((fields.first()?.to_string(), fields.get(19)?.to_string()))
}

fn wait_for(mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !condition() {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for test state"
        );
        thread::sleep(Duration::from_millis(20));
    }
}

fn parsed(output: Output) -> Value {
    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

fn refused(output: Output) -> Value {
    assert!(!output.status.success(), "operation unexpectedly succeeded");
    serde_json::from_slice(&output.stderr).unwrap()
}

#[test]
fn recovery_preserves_queued_work_and_guards_admission_across_restarts() {
    let mut fixture = Fixture::new();
    let mut old = fixture.queue_behind_worker();
    let queued = fixture.status("retained-queued");
    old.kill();
    fixture.stop_worker();
    assert_eq!(fixture.status("retained-running")["status"], "running");

    let (mut holder, receipt) = fixture.hold();
    assert_eq!(receipt["state"], "recovering");
    let recovery_id = receipt["recovery_id"].as_str().unwrap();
    let suffix = recovery_id.strip_prefix("recovery-").unwrap();
    assert_eq!(suffix.len(), 12);
    assert!(suffix.bytes().all(|byte| byte.is_ascii_hexdigit()));
    assert_eq!(fixture.status("retained-queued"), queued);
    let owner = refused(fixture.command("serve").output().unwrap());
    assert_eq!(owner["code"], "broker-already-owned");
    drop(holder.0.stdin.take());
    assert!(holder.0.wait().unwrap().success());

    for _ in 0..2 {
        let mut replacement = fixture.start();
        wait_for(|| fixture.status("retained-running")["status"] == "interrupted");
        assert_eq!(
            refused(fixture.submit("must-not-be-accepted", &["/bin/true"]))["code"],
            "broker-draining"
        );
        // Observe multiple scheduling ticks while the otherwise runnable row is guarded.
        thread::sleep(Duration::from_millis(300));
        assert_eq!(fixture.status("retained-queued"), queued);
        assert!(!fixture.root.join("executed").exists());
        replacement.kill();
    }

    let (mut resumed_holder, resumed_receipt) = fixture.hold();
    assert_eq!(resumed_receipt["recovery_id"], receipt["recovery_id"]);
    drop(resumed_holder.0.stdin.take());
    assert!(resumed_holder.0.wait().unwrap().success());
}

#[test]
fn recovery_refuses_a_live_owner_and_a_live_retained_worker() {
    let mut fixture = Fixture::new();
    let mut broker = fixture.queue_behind_worker();
    let running = fixture.status("retained-running");
    let queued = fixture.status("retained-queued");

    assert_eq!(
        refused(fixture.command("host-recover-hold").output().unwrap())["code"],
        "host-drain-owner-live"
    );
    assert_eq!(
        fixture.status("retained-running")["worker_pid"],
        running["worker_pid"]
    );
    broker.kill();
    assert!(
        process_identity(fixture.worker.as_ref().unwrap().0).is_some_and(|(state, _)| state != "Z")
    );
    assert_eq!(
        refused(fixture.command("host-recover-hold").output().unwrap())["code"],
        "host-recovery-worker-live"
    );
    assert_eq!(fixture.status("retained-running")["status"], "running");
    assert_eq!(fixture.status("retained-queued"), queued);
    assert!(!fixture.root.join("executed").exists());
}
