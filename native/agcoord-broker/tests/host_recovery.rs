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
    cgroup_fixture: Option<PathBuf>,
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
            cgroup_fixture: None,
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
        let mut command = self.command("serve");
        if let Some(root) = &self.cgroup_fixture {
            command.arg("--cgroup-fixture").arg(root);
        }
        let mut process = OwnedProcess(
            command
                .args([
                    "--capacity",
                    "jobs=1",
                    "--capacity",
                    "cpu=1",
                    "--idle-timeout",
                    "30",
                ])
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
        self.submit_request(self.command("submit"), id, arguments)
    }

    fn proof(&self, recovery_id: &str, run_id: &str) -> Output {
        let mut command = self.command("host-recover-proof");
        command.args(["--recovery-id", recovery_id, "--resource", "cpu=1"]);
        self.submit_request(
            command,
            run_id,
            &[self.root.join("enforcement-probe").to_str().unwrap()],
        )
    }

    fn submit_request(&self, mut command: Command, id: &str, arguments: &[&str]) -> Output {
        command
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

    fn configure_cpu_fixture(&mut self) {
        let root = self.root.join("delegated");
        fs::create_dir(&root).unwrap();
        fs::create_dir_all(&self.state).unwrap();
        fs::set_permissions(&self.state, fs::Permissions::from_mode(0o700)).unwrap();
        fs::write(self.state.join("config.json"), serde_json::to_vec(&json!({
            "bindings": {"cpu": {"backend": "cgroup-v2", "kind": "cpu", "mode": "required", "unit": "logical-cpu"}},
            "cgroup_root": root,
        })).unwrap()).unwrap();
        fs::set_permissions(
            self.state.join("config.json"),
            fs::Permissions::from_mode(0o600),
        )
        .unwrap();
        fs::write(
            self.root.join("enforcement-probe"),
            "#!/bin/sh\nwhile [ ! -e proof-release ]; do sleep 0.02; done\n",
        )
        .unwrap();
        self.cgroup_fixture = Some(root);
    }

    fn finish_enforced_fixture_proof(&mut self, recovery_id: &str) -> Value {
        parsed(self.proof(recovery_id, "fixture-enforcement-proof"));
        wait_for(|| {
            let row = self.status("fixture-enforcement-proof");
            if let Some(pid) = row["worker_pid"].as_u64()
                && let Some((_, token)) = process_identity(pid as u32)
            {
                self.worker = Some((pid as u32, token));
                row["resource_receipt"]["applied"]["cpu"] == 1
            } else {
                false
            }
        });
        let root = self.cgroup_fixture.as_ref().unwrap();
        let owner = fs::read_dir(root)
            .unwrap()
            .flatten()
            .map(|e| e.path())
            .find(|p| {
                p.file_name()
                    .unwrap()
                    .to_string_lossy()
                    .starts_with("agcoord-u")
            })
            .unwrap();
        let leaf = fs::read_dir(owner)
            .unwrap()
            .flatten()
            .map(|e| e.path())
            .find(|p| p.file_name().unwrap().to_string_lossy().starts_with("run-"))
            .unwrap();
        // This is the existing deterministic kernel seam, not a fabricated run receipt.
        fs::write(
            leaf.join("cpu.stat"),
            "usage_usec 100\nnr_throttled 0\nthrottled_usec 0\n",
        )
        .unwrap();
        fs::write(self.root.join("proof-release"), "release").unwrap();
        wait_for(|| self.status("fixture-enforcement-proof")["status"] == "passed");
        self.stop_worker();
        let proof = self.status("fixture-enforcement-proof");
        assert_eq!(proof["resource_receipt"]["applied"]["cpu"], 1);
        assert!(proof["resource_receipt"]["peak"]["cpu"].as_u64().unwrap() >= 1);
        proof
    }

    fn recovery_status(&self) -> Value {
        parsed(self.command("host-recover-status").output().unwrap())
    }

    fn guarded_replacement(&mut self) -> (OwnedProcess, String) {
        let mut old = self.queue_behind_worker();
        old.kill();
        self.stop_worker();
        let (mut holder, receipt) = self.hold();
        drop(holder.0.stdin.take());
        assert!(holder.0.wait().unwrap().success());
        let replacement = self.start();
        wait_for(|| self.status("retained-running")["status"] == "interrupted");
        (
            replacement,
            receipt["recovery_id"].as_str().unwrap().to_owned(),
        )
    }

    fn assert_queue_guarded(&self) {
        assert_eq!(self.status("retained-queued")["status"], "queued");
        assert!(!self.root.join("executed").exists());
        assert_eq!(
            refused(self.submit("ordinary-new-job", &["/bin/true"]))["code"],
            "broker-draining"
        );
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

fn assert_retained_queue(actual: &Value, original: &Value) {
    assert_eq!(actual["status"], "queued");
    for field in [
        "run_id",
        "sequence",
        "kind",
        "label",
        "repository_id",
        "worktree_id",
        "checkout",
        "branch",
        "head_sha",
        "resources",
        "command",
        "created_at",
        "started_at",
        "finished_at",
        "worker_pid",
        "cancel_requested",
    ] {
        assert_eq!(actual[field], original[field], "retained field {field}");
    }
    assert_eq!(actual["run_id"], "retained-queued");
    assert!(
        actual["command"]
            .as_array()
            .is_some_and(|args| !args.is_empty())
    );
    assert!(actual["created_at"].is_string());
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
    assert_retained_queue(&fixture.status("retained-queued"), &queued);
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
        assert_retained_queue(&fixture.status("retained-queued"), &queued);
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
    assert_retained_queue(&fixture.status("retained-queued"), &queued);
    assert!(!fixture.root.join("executed").exists());
}

#[test]
fn a_successful_unenforced_proof_cannot_complete_or_resume_recovery() {
    let mut fixture = Fixture::new();
    let (_replacement, id) = fixture.guarded_replacement();
    assert_eq!(fixture.recovery_status()["recovery_id"], id);
    parsed(fixture.proof(&id, "unenforced-proof"));
    wait_for(|| fixture.status("unenforced-proof")["status"] == "passed");
    let proof = fixture.status("unenforced-proof");
    assert_eq!(proof["resource_receipt"]["requested"]["cpu"], 1);
    assert!(proof["resource_receipt"]["applied"]["cpu"].is_null());
    assert_eq!(
        refused(
            fixture
                .command("host-recover-complete")
                .args(["--recovery-id", &id])
                .output()
                .unwrap()
        )["code"],
        "host-recovery-proof-failed"
    );
    refused(
        fixture
            .command("resume")
            .args(["--drain-id", &id])
            .output()
            .unwrap(),
    );
    assert_eq!(fixture.recovery_status()["recovery_id"], id);
    fixture.assert_queue_guarded();
}

#[test]
fn mismatched_recovery_identity_and_changed_helper_preserve_the_guard() {
    let mut fixture = Fixture::new();
    let (_replacement, id) = fixture.guarded_replacement();
    let wrong = if id == "recovery-000000000000" {
        "recovery-111111111111"
    } else {
        "recovery-000000000000"
    };
    assert_eq!(
        refused(fixture.proof(wrong, "wrong-id-proof"))["code"],
        "host-recovery-invalid"
    );
    assert_eq!(
        refused(
            fixture
                .command("host-recover-complete")
                .args(["--recovery-id", wrong])
                .output()
                .unwrap()
        )["code"],
        "host-recovery-invalid"
    );
    assert_eq!(
        refused(
            fixture
                .command("host-recover-park")
                .args(["--recovery-id", wrong, "--reason", "test-wrong-identity"])
                .output()
                .unwrap()
        )["code"],
        "host-recovery-invalid"
    );
    let mut command = fixture.command("host-recover-proof");
    command.args(["--recovery-id", &id, "--resource", "cpu=1"]);
    assert_eq!(
        refused(fixture.submit_request(command, "other-command-proof", &["/bin/true"]))["code"],
        "host-recovery-invalid"
    );
    fs::write(
        fixture.root.join("enforcement-probe"),
        "#!/bin/sh\nexit 7\n",
    )
    .unwrap();
    assert_eq!(
        refused(fixture.proof(&id, "changed-helper-proof"))["code"],
        "host-recovery-invalid"
    );
    assert_eq!(fixture.recovery_status()["attempts"], 0);
    assert_eq!(fixture.recovery_status()["recovery_id"], id);
    fixture.assert_queue_guarded();
}

#[test]
fn failed_proofs_retry_with_the_same_identity_and_park_at_the_attempt_bound() {
    let mut fixture = Fixture::new();
    fs::write(
        fixture.root.join("enforcement-probe"),
        "#!/bin/sh\nexit 7\n",
    )
    .unwrap();
    let (mut replacement, id) = fixture.guarded_replacement();
    for attempt in 1..=3 {
        let run_id = format!("failed-proof-{attempt}");
        parsed(fixture.proof(&id, &run_id));
        wait_for(|| fixture.status(&run_id)["status"] == "failed");
        assert_eq!(fixture.status(&run_id)["exit_status"], 7);
        assert_eq!(fixture.recovery_status()["attempts"], attempt);
        assert_eq!(fixture.recovery_status()["recovery_id"], id);
        fixture.assert_queue_guarded();
        if attempt == 1 {
            replacement.kill();
            replacement = fixture.start();
        }
    }
    assert_eq!(
        refused(fixture.proof(&id, "excess-proof"))["code"],
        "host-recovery-exhausted"
    );
    assert_eq!(fixture.recovery_status()["phase"], "parked");
    assert_eq!(fixture.recovery_status()["attempts"], 3);
    fixture.assert_queue_guarded();
}

#[test]
fn parking_cancels_only_the_active_proof_and_allows_a_bounded_retry() {
    let mut fixture = Fixture::new();
    fs::write(
        fixture.root.join("enforcement-probe"),
        "#!/bin/sh\nwhile [ ! -f release-proof ]; do sleep 0.02; done\n",
    )
    .unwrap();
    let (_replacement, id) = fixture.guarded_replacement();
    parsed(fixture.proof(&id, "parked-proof"));
    wait_for(|| {
        let row = fixture.status("parked-proof");
        if row["status"] != "running" {
            return false;
        }
        let Some(pid) = row["worker_pid"].as_u64() else {
            return false;
        };
        let Some((_, token)) = process_identity(pid as u32) else {
            return false;
        };
        fixture.worker = Some((pid as u32, token));
        true
    });
    let parked = parsed(
        fixture
            .command("host-recover-park")
            .args(["--recovery-id", &id, "--reason", "test-enforcement-timeout"])
            .output()
            .unwrap(),
    );
    assert_eq!(parked["phase"], "parked");
    assert_eq!(parked["incident"], "test-enforcement-timeout");
    wait_for(|| fixture.status("parked-proof")["status"] == "cancelled");
    fixture.stop_worker();
    fixture.assert_queue_guarded();
    fs::write(fixture.root.join("release-proof"), "release").unwrap();
    parsed(fixture.proof(&id, "retry-after-park"));
    wait_for(|| fixture.status("retry-after-park")["status"] == "passed");
    assert_eq!(fixture.recovery_status()["recovery_id"], id);
    assert_eq!(fixture.recovery_status()["attempts"], 2);
    assert_eq!(fixture.status("parked-proof")["status"], "cancelled");
    fixture.assert_queue_guarded();
}

#[test]
fn stranded_normal_drain_can_enter_recovery_without_releasing_accepted_work() {
    let mut fixture = Fixture::new();
    let mut old = fixture.queue_behind_worker();
    let queued = fixture.status("retained-queued");
    let drain = parsed(
        fixture
            .command("drain")
            .args([
                "--drain-id",
                "drain-0123456789ab",
                "--reason",
                "owned host maintenance",
            ])
            .output()
            .unwrap(),
    );
    assert_eq!(drain["state"], "draining");
    assert_eq!(drain["live"], 2);
    old.kill();
    fixture.stop_worker();
    assert_eq!(
        parsed(fixture.command("drain-status").output().unwrap())["state"],
        "draining"
    );

    let (mut holder, receipt) = fixture.hold();
    assert_eq!(receipt["state"], "recovering");
    assert_retained_queue(&fixture.status("retained-queued"), &queued);
    drop(holder.0.stdin.take());
    assert!(holder.0.wait().unwrap().success());

    let _replacement = fixture.start();
    wait_for(|| fixture.status("retained-running")["status"] == "interrupted");
    assert_eq!(
        fixture.recovery_status()["recovery_id"],
        receipt["recovery_id"]
    );
    assert_retained_queue(&fixture.status("retained-queued"), &queued);
    fixture.assert_queue_guarded();
}

#[test]
fn verified_recovery_restores_original_drain_and_replays_without_an_owner() {
    for retain_queue in [true, false] {
        let mut fixture = Fixture::new();
        fixture.configure_cpu_fixture();
        let mut old = fixture.queue_behind_worker();
        // The stranded worker claims only jobs; CPU enforcement starts with the proof.
        assert!(fixture.status("retained-running")["resource_receipt"]["applied"]["cpu"].is_null());
        if !retain_queue {
            parsed(
                fixture
                    .command("cancel")
                    .args(["--run-id", "retained-queued"])
                    .output()
                    .unwrap(),
            );
        }
        let drain = parsed(
            fixture
                .command("drain")
                .args([
                    "--drain-id",
                    "drain-0123456789ab",
                    "--reason",
                    "owned maintenance intent",
                ])
                .output()
                .unwrap(),
        );
        old.kill();
        fixture.stop_worker();
        let (mut holder, guard) = fixture.hold();
        assert_eq!(guard["format"], 2);
        assert_eq!(
            guard["original_drain"],
            json!({
                "state": drain["state"], "drain_id": drain["drain_id"],
                "reason": drain["reason"], "started_at": drain["started_at"],
            })
        );
        let recovery_id = guard["recovery_id"].as_str().unwrap();
        drop(holder.0.stdin.take());
        assert!(holder.0.wait().unwrap().success());
        let mut replacement = fixture.start();
        wait_for(|| fixture.status("retained-running")["status"] == "interrupted");
        let proof = fixture.finish_enforced_fixture_proof(recovery_id);
        if retain_queue {
            fixture.assert_queue_guarded();
        }
        for id in ["drain-ffffffffffff", recovery_id] {
            refused(
                fixture
                    .command("resume")
                    .args(["--drain-id", id])
                    .output()
                    .unwrap(),
            );
        }
        let completed = parsed(
            fixture
                .command("host-recover-complete")
                .args(["--recovery-id", recovery_id])
                .output()
                .unwrap(),
        );
        assert_eq!(
            completed,
            json!({
                "state": if retain_queue { "draining" } else { "drained" },
                "recovery_id": recovery_id, "proof_run_id": proof["run_id"],
                "drain_id": drain["drain_id"],
            })
        );
        if retain_queue {
            wait_for(|| fixture.status("retained-queued")["status"] == "passed");
            assert!(fixture.root.join("executed").exists());
        }
        wait_for(|| replacement.0.try_wait().unwrap().is_some());
        let restored = parsed(fixture.command("drain-status").output().unwrap());
        assert_eq!(restored["state"], "drained");
        assert_eq!(restored["live"], 0);
        assert!(restored["broker_pid"].is_null());
        for field in ["drain_id", "reason", "started_at"] {
            assert_eq!(restored[field], drain[field]);
        }
        assert_eq!(
            parsed(
                fixture
                    .command("host-recover-complete")
                    .args(["--recovery-id", recovery_id])
                    .output()
                    .unwrap()
            ),
            completed
        );
        refused(fixture.submit("new-during-restored-drain", &["/bin/true"]));
        for id in ["drain-ffffffffffff", recovery_id] {
            refused(
                fixture
                    .command("resume")
                    .args(["--drain-id", id])
                    .output()
                    .unwrap(),
            );
        }
        let resumed = parsed(
            fixture
                .command("resume")
                .args(["--drain-id", drain["drain_id"].as_str().unwrap()])
                .output()
                .unwrap(),
        );
        assert_eq!(resumed["state"], "open");
        assert_eq!(
            parsed(
                fixture
                    .command("host-recover-complete")
                    .args(["--recovery-id", recovery_id])
                    .output()
                    .unwrap()
            ),
            completed
        );
    }
}

#[test]
fn malformed_original_drain_cannot_be_read_or_reused_as_a_recovery_guard() {
    for invalid in [
        json!({"state": "draining"}),
        json!({"state": "open", "drain_id": "drain-0123456789ab", "reason": "owned", "started_at": "2026-10-02T00:00:00Z"}),
        json!({"state": "draining", "drain_id": "recovery-0123456789ab", "reason": "owned", "started_at": "2026-10-02T00:00:00Z"}),
    ] {
        let mut fixture = Fixture::new();
        let mut old = fixture.queue_behind_worker();
        old.kill();
        fixture.stop_worker();
        let (mut holder, _) = fixture.hold();
        drop(holder.0.stdin.take());
        assert!(holder.0.wait().unwrap().success());
        // Corrupt only this fixture's persisted boundary to exercise fail-closed reads.
        let connection = rusqlite::Connection::open(fixture.state.join("queue.sqlite3")).unwrap();
        let raw: String = connection
            .query_row(
                "SELECT value FROM coordinator_meta WHERE key='host_recovery'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let mut guard: Value = serde_json::from_str(&raw).unwrap();
        guard["original_drain"] = invalid;
        connection
            .execute(
                "UPDATE coordinator_meta SET value=?1 WHERE key='host_recovery'",
                [guard.to_string()],
            )
            .unwrap();
        drop(connection);
        assert_eq!(
            refused(fixture.command("host-recover-status").output().unwrap())["code"],
            "host-recovery-invalid"
        );
        assert_eq!(
            refused(fixture.command("host-recover-hold").output().unwrap())["code"],
            "host-recovery-invalid"
        );
        assert_eq!(fixture.status("retained-queued")["status"], "queued");
        assert!(!fixture.root.join("executed").exists());
    }
}
