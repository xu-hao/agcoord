use rusqlite::{Connection, params};
use serde_json::{Value, json};
use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const BROKER: &str = env!("CARGO_BIN_EXE_agcoord-broker");
const MIB: u64 = 1024 * 1024;

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new(name: &str) -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "agcoord-native-{name}-{}-{nonce}",
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

struct MountGuard(PathBuf);

impl Drop for MountGuard {
    fn drop(&mut self) {
        let _ = Command::new("umount").arg(&self.0).status();
    }
}

struct RunningBroker {
    child: Option<Child>,
}

impl RunningBroker {
    fn start(state_dir: &Path, capacities: &[(&str, u64)]) -> Self {
        Self::start_with_options(state_dir, capacities, &[])
    }

    fn start_with_worker_fault(state_dir: &Path, capacities: &[(&str, u64)], fault: &str) -> Self {
        Self::start_with_options(state_dir, capacities, &["--worker-fault", fault])
    }

    fn start_with_options(state_dir: &Path, capacities: &[(&str, u64)], options: &[&str]) -> Self {
        let mut command = Command::new(BROKER);
        command
            .arg("serve")
            .arg("--state-dir")
            .arg(state_dir)
            .arg("--idle-timeout")
            .arg("30");
        for (name, units) in capacities {
            command.arg("--capacity").arg(format!("{name}={units}"));
        }
        command.args(options);
        let mut child = command
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("start native broker");
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            assert!(
                child.try_wait().unwrap().is_none(),
                "native broker exited early"
            );
            if snapshot(state_dir).is_some() {
                break;
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!("native broker did not expose a snapshot");
            }
            thread::sleep(Duration::from_millis(20));
        }
        Self { child: Some(child) }
    }

    fn kill(&mut self) {
        let mut child = self.child.take().unwrap();
        child.kill().unwrap();
        child.wait().unwrap();
    }

    fn terminate(&mut self) -> std::process::ExitStatus {
        let child = self.child.as_mut().unwrap();
        if let Some(status) = child.try_wait().unwrap() {
            self.child = None;
            return status;
        }
        let status = Command::new("/bin/kill")
            .args(["-TERM", &child.id().to_string()])
            .status()
            .unwrap();
        assert!(status.success());
        let status = child.wait().unwrap();
        self.child = None;
        status
    }

    fn is_running(&mut self) -> bool {
        self.child
            .as_mut()
            .is_some_and(|child| child.try_wait().unwrap().is_none())
    }

    fn signal_terminate(&mut self) {
        let child = self.child.as_ref().unwrap();
        assert!(
            Command::new("/bin/kill")
                .args(["-TERM", &child.id().to_string()])
                .status()
                .unwrap()
                .success()
        );
    }

    fn wait(&mut self) -> std::process::ExitStatus {
        let status = self.child.as_mut().unwrap().wait().unwrap();
        self.child = None;
        status
    }
}

fn start_crashing_broker(state_dir: &Path, crash_after: &str) -> Child {
    let mut child = Command::new(BROKER)
        .args([
            "serve",
            "--state-dir",
            state_argument(state_dir),
            "--capacity",
            "jobs=1",
            "--idle-timeout",
            "30",
            "--crash-after",
            crash_after,
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        assert!(
            child.try_wait().unwrap().is_none(),
            "crash broker exited early"
        );
        if snapshot(state_dir).is_some() {
            return child;
        }
        assert!(Instant::now() < deadline, "crash broker did not start");
        thread::sleep(Duration::from_millis(20));
    }
}

impl Drop for RunningBroker {
    fn drop(&mut self) {
        let Some(mut child) = self.child.take() else {
            return;
        };
        if child.try_wait().ok().flatten().is_some() {
            return;
        }
        let _ = Command::new("/bin/kill")
            .args(["-TERM", &child.id().to_string()])
            .status();
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline {
            if child.try_wait().ok().flatten().is_some() {
                return;
            }
            thread::sleep(Duration::from_millis(20));
        }
        let _ = child.kill();
        let _ = child.wait();
    }
}

fn wait_for(timeout: Duration, mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if condition() {
            return;
        }
        thread::sleep(Duration::from_millis(20));
    }
    panic!("condition was not satisfied within {timeout:?}");
}

fn wait_for_nonempty_file(path: &Path, timeout: Duration) -> Vec<u8> {
    let mut contents = None;
    wait_for(timeout, || match fs::read(path) {
        Ok(value) if !value.is_empty() => {
            contents = Some(value);
            true
        }
        _ => false,
    });
    contents.unwrap()
}

fn run(arguments: &[&str]) -> Output {
    Command::new(BROKER)
        .args(arguments)
        .output()
        .expect("run native broker command")
}

fn json_output(arguments: &[&str]) -> Value {
    let result = run(arguments);
    assert!(
        result.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
    serde_json::from_slice(&result.stdout).unwrap()
}

fn state_argument(state_dir: &Path) -> &str {
    state_dir.to_str().unwrap()
}

fn real_cgroup_root() -> Option<PathBuf> {
    std::env::var_os("AGCOORD_TEST_CGROUP_ROOT")
        .map(fs::canonicalize)
        .transpose()
        .unwrap()
}

fn assert_no_cgroup_owner(root: &Path) {
    assert!(
        !fs::read_dir(root)
            .unwrap()
            .flatten()
            .any(|entry| entry.file_name().to_string_lossy().starts_with("agcoord-u"))
    );
}

fn snapshot(state_dir: &Path) -> Option<Value> {
    let result = run(&["snapshot", "--state-dir", state_argument(state_dir)]);
    result
        .status
        .success()
        .then(|| serde_json::from_slice(&result.stdout).unwrap())
}

fn status(state_dir: &Path, run_id: &str) -> Value {
    json_output(&[
        "status",
        "--state-dir",
        state_argument(state_dir),
        "--run-id",
        run_id,
    ])
}

fn process_start_token(pid: u64) -> String {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat")).unwrap();
    let closing = stat.rfind(')').unwrap();
    stat[closing + 2..]
        .split_whitespace()
        .nth(19)
        .unwrap()
        .to_owned()
}

fn process_state(pid: u64) -> Option<String> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let closing = stat.rfind(')')?;
    stat[closing + 2..]
        .split_whitespace()
        .next()
        .map(ToOwned::to_owned)
}

struct ProcessGuard {
    pid: u32,
    token: String,
    armed: bool,
}

impl ProcessGuard {
    fn new(pid: u32) -> Self {
        Self {
            pid,
            token: process_start_token(u64::from(pid)),
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for ProcessGuard {
    fn drop(&mut self) {
        if self.armed
            && fs::read_to_string(format!("/proc/{}/stat", self.pid))
                .ok()
                .and_then(|stat| {
                    let closing = stat.rfind(')')?;
                    stat[closing + 2..]
                        .split_whitespace()
                        .nth(19)
                        .map(ToOwned::to_owned)
                })
                .as_deref()
                == Some(&self.token)
        {
            let _ = Command::new("/bin/kill")
                .args(["-KILL", &self.pid.to_string()])
                .status();
        }
    }
}

struct ChildGuard(Child);

impl ChildGuard {
    fn is_running(&mut self) -> bool {
        self.0.try_wait().unwrap().is_none()
    }

    fn id(&self) -> u32 {
        self.0.id()
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            let _ = self.0.kill();
        }
        let _ = self.0.wait();
    }
}

fn advance_land_phase(state_dir: &Path, run_id: &str, row: &Value, phase: &str) -> Output {
    let worker_pid = row["worker_pid"].as_u64().unwrap();
    let token = process_start_token(worker_pid);
    let pid = worker_pid.to_string();
    let checkout = row["checkout"].as_str().unwrap();
    let head = row["head_sha"].as_str().unwrap();
    let mut arguments = vec![
        "phase",
        "--state-dir",
        state_argument(state_dir),
        "--run-id",
        run_id,
        "--worker-pid",
        &pid,
        "--worker-start-token",
        &token,
        "--checkout",
        checkout,
        "--head",
        head,
        "--phase",
        phase,
    ];
    if phase == "publishing" {
        arguments.extend(["--gate-exit-status", "0"]);
    }
    run(&arguments)
}

fn wait_status(state_dir: &Path, run_id: &str, expected: &str) -> Value {
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut observed = Value::Null;
    while Instant::now() < deadline {
        observed = status(state_dir, run_id);
        if observed["status"] == expected {
            return observed;
        }
        thread::sleep(Duration::from_millis(20));
    }
    panic!("run {run_id} did not reach {expected}; last status: {observed}");
}

struct Submission<'a> {
    run_id: &'a str,
    kind: &'a str,
    repository: &'a str,
    checkout: &'a Path,
    command: Vec<String>,
    gate_run_id: Option<&'a str>,
}

fn submit(state_dir: &Path, submission: &Submission<'_>) -> Output {
    submit_with_environment(state_dir, submission, &[])
}

fn submit_with_environment(
    state_dir: &Path,
    submission: &Submission<'_>,
    environment: &[(&str, &str)],
) -> Output {
    submit_with_resources_and_environment(state_dir, submission, &[], environment)
}

fn submit_with_resources(
    state_dir: &Path,
    submission: &Submission<'_>,
    resources: &[(&str, u64)],
) -> Output {
    submit_with_resources_and_environment(state_dir, submission, resources, &[])
}

fn submit_with_resources_and_environment(
    state_dir: &Path,
    submission: &Submission<'_>,
    resources: &[(&str, u64)],
    environment: &[(&str, &str)],
) -> Output {
    let mut arguments = vec![
        "submit".to_owned(),
        "--state-dir".to_owned(),
        state_argument(state_dir).to_owned(),
        "--run-id".to_owned(),
        submission.run_id.to_owned(),
        "--kind".to_owned(),
        submission.kind.to_owned(),
        "--label".to_owned(),
        format!("{} label", submission.run_id),
        "--repository-id".to_owned(),
        submission.repository.to_owned(),
        "--repository".to_owned(),
        submission.repository.to_owned(),
        "--worktree-id".to_owned(),
        format!("worktree-{}", submission.repository),
        "--checkout".to_owned(),
        submission.checkout.to_str().unwrap().to_owned(),
        "--branch".to_owned(),
        "ticket".to_owned(),
        "--head".to_owned(),
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
        "--resource".to_owned(),
        "jobs=1".to_owned(),
        "--caller-pid".to_owned(),
        std::process::id().to_string(),
    ];
    for (name, units) in resources {
        arguments.extend(["--resource".to_owned(), format!("{name}={units}")]);
    }
    if let Some(gate_run_id) = submission.gate_run_id {
        arguments.extend(["--gate-run-id".to_owned(), gate_run_id.to_owned()]);
    }
    if matches!(submission.kind, "merge" | "land") {
        arguments.extend([
            "--publication-adapter".to_owned(),
            "github".to_owned(),
            "--publication-request-json".to_owned(),
            "1".to_owned(),
        ]);
    }
    if submission.kind == "land" {
        let passthrough = submission.checkout.join("agcoord-test-land-python");
        fs::write(
            &passthrough,
            "#!/bin/sh\nwhile [ \"$1\" != -- ]; do shift; done\nshift\nexec \"$@\"\n",
        )
        .unwrap();
        fs::set_permissions(&passthrough, fs::Permissions::from_mode(0o755)).unwrap();
        arguments.extend([
            "--env".to_owned(),
            format!("_AGCOORD_LAND_PYTHON={}", passthrough.to_string_lossy()),
        ]);
    }
    for (name, value) in environment {
        arguments.extend(["--env".to_owned(), format!("{name}={value}")]);
    }
    arguments.push("--".to_owned());
    arguments.extend(submission.command.clone());
    Command::new(BROKER).args(arguments).output().unwrap()
}

fn submit_ok(state_dir: &Path, submission: &Submission<'_>) {
    let result = submit(state_dir, submission);
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&result.stdout).unwrap(),
        json!({"run_id": submission.run_id})
    );
}

fn blocking_command(entered: &Path, release: &Path, started: Option<&Path>) -> Vec<String> {
    let mut script = "touch \"$1\"; ".to_owned();
    if started.is_some() {
        script.push_str("printf 'started\\n' >>\"$3\"; ");
    }
    script.push_str(
        "i=0; while [ ! -e \"$2\" ] && [ \"$i\" -lt 500 ]; do sleep 0.02; i=$((i + 1)); done",
    );
    let mut command = vec![
        "/bin/sh".to_owned(),
        "-c".to_owned(),
        script,
        "agcoord-test".to_owned(),
        entered.to_str().unwrap().to_owned(),
        release.to_str().unwrap().to_owned(),
    ];
    if let Some(started) = started {
        command.push(started.to_str().unwrap().to_owned());
    }
    command
}

fn touch_command(path: &Path) -> Vec<String> {
    vec![
        "/usr/bin/touch".to_owned(),
        path.to_str().unwrap().to_owned(),
    ]
}

fn append_command(path: &Path, value: &str) -> Vec<String> {
    vec![
        "/bin/sh".to_owned(),
        "-c".to_owned(),
        "printf '%s\\n' \"$1\" >>\"$2\"".to_owned(),
        "agcoord-test".to_owned(),
        value.to_owned(),
        path.to_str().unwrap().to_owned(),
    ]
}

fn write_project_quota_config(state: &Path, mode: &str) {
    fs::write(
        state.join("config.json"),
        serde_json::to_vec(&json!({
            "bindings": {
                "disk": {
                    "backend": "project-quota",
                    "kind": "storage",
                    "mode": mode,
                    "unit": "bytes"
                },
                "disk_inodes": {
                    "backend": "project-quota",
                    "kind": "inodes",
                    "mode": mode,
                    "unit": "inodes"
                }
            }
        }))
        .unwrap(),
    )
    .unwrap();
}

fn create_legacy_database(state: &Path, checkout: &Path, selected_protocol: u64) {
    fs::create_dir(state).unwrap();
    let database = state.join("queue.sqlite3");
    let phase_column = if selected_protocol >= 2 {
        "phase TEXT NOT NULL,"
    } else {
        ""
    };
    let gate_columns = if selected_protocol >= 2 {
        "gate_exit_status INTEGER, reported_exit_status INTEGER,"
    } else {
        ""
    };
    let resource_columns = if selected_protocol >= 3 {
        "resource_contract_json TEXT NOT NULL, resource_receipt_json TEXT NOT NULL, resource_state_json TEXT NOT NULL,"
    } else {
        ""
    };
    let schema = format!(
        "CREATE TABLE coordinator_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
         INSERT INTO coordinator_meta(key, value) VALUES ('protocol', '{selected_protocol}');
         CREATE TABLE runs (
            sequence INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL UNIQUE,
            status TEXT NOT NULL,
            kind TEXT NOT NULL,
            {phase_column}
            label TEXT NOT NULL,
            agent TEXT NOT NULL,
            repository_id TEXT NOT NULL,
            repository TEXT NOT NULL,
            worktree_id TEXT NOT NULL,
            checkout TEXT NOT NULL,
            branch TEXT NOT NULL,
            head_sha TEXT,
            barrier INTEGER NOT NULL,
            resources_json TEXT NOT NULL,
            {resource_columns}
            gate_run_id TEXT,
            publication_adapter TEXT,
            publication_request TEXT,
            failure_reason TEXT,
            {gate_columns}
            caller_pid INTEGER NOT NULL,
            command_json TEXT NOT NULL,
            environment_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            started_at TEXT,
            finished_at TEXT,
            exit_status INTEGER,
            worker_pid INTEGER,
            worker_start_token TEXT,
            cancel_requested INTEGER NOT NULL DEFAULT 0,
            cancel_requested_at TEXT
         );"
    );
    let db = Connection::open(&database).unwrap();
    db.execute_batch(&schema).unwrap();
    let phase_name = if selected_protocol >= 2 {
        ", phase"
    } else {
        ""
    };
    let phase_value = if selected_protocol >= 2 {
        ", 'complete'"
    } else {
        ""
    };
    let resource_names = if selected_protocol >= 3 {
        ", resource_contract_json, resource_receipt_json, resource_state_json"
    } else {
        ""
    };
    let resource_values = if selected_protocol >= 3 {
        ", '{\"jobs\":{\"backend\":null,\"kind\":\"generic\",\"mode\":\"admission-only\",\"unit\":\"admission-unit\"}}', '{\"requested\":{\"jobs\":1},\"applied\":{},\"peak\":{},\"events\":[]}', '{}'"
    } else {
        ""
    };
    db.execute(
        &format!(
            "INSERT INTO runs (
                run_id, status, kind, label, agent, repository_id, repository,
                worktree_id, checkout, branch, head_sha, barrier, resources_json,
                caller_pid, command_json, environment_json, created_at, started_at,
                finished_at, exit_status{phase_name}{resource_names}
             ) VALUES (
                'legacy-full', 'passed', 'full', 'legacy full', 'legacy-agent',
                'repo-a', 'repo-a', 'worktree-a', ?1, 'ticket',
                'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 1, '{{\"jobs\":1}}',
                42, '[\"true\"]', '{{}}', '2026-08-31T00:00:00Z',
                '2026-08-31T00:00:01Z', '2026-08-31T00:00:02Z', 0
                {phase_value}{resource_values}
             )"
        ),
        params![checkout.to_str().unwrap()],
    )
    .unwrap();
}

#[test]
fn repository_barriers_and_machine_capacity_preserve_order() {
    let temporary = TestDirectory::new("barriers");
    let state = temporary.path().join("state");
    let checkout = temporary.path().join("checkout");
    fs::create_dir(&checkout).unwrap();
    let mut broker = RunningBroker::start(&state, &[("jobs", 2)]);
    let check_entered = temporary.path().join("check-entered");
    let check_release = temporary.path().join("check-release");
    let full_entered = temporary.path().join("full-entered");
    let full_release = temporary.path().join("full-release");
    let after = temporary.path().join("after");
    let other = temporary.path().join("other");

    submit_ok(
        &state,
        &Submission {
            run_id: "check-first",
            kind: "check",
            repository: "repo-a",
            checkout: &checkout,
            command: blocking_command(&check_entered, &check_release, None),
            gate_run_id: None,
        },
    );
    wait_for(Duration::from_secs(5), || check_entered.exists());
    submit_ok(
        &state,
        &Submission {
            run_id: "full-barrier",
            kind: "full",
            repository: "repo-a",
            checkout: &checkout,
            command: blocking_command(&full_entered, &full_release, None),
            gate_run_id: None,
        },
    );
    submit_ok(
        &state,
        &Submission {
            run_id: "check-after",
            kind: "check",
            repository: "repo-a",
            checkout: &checkout,
            command: touch_command(&after),
            gate_run_id: None,
        },
    );
    submit_ok(
        &state,
        &Submission {
            run_id: "check-other",
            kind: "check",
            repository: "repo-b",
            checkout: &checkout,
            command: touch_command(&other),
            gate_run_id: None,
        },
    );

    wait_status(&state, "check-other", "passed");
    assert!(!full_entered.exists());
    assert!(!after.exists());
    let barrier = status(&state, "full-barrier");
    assert!(
        barrier["blocked_by"][0]
            .as_str()
            .unwrap()
            .contains("check-first")
    );
    assert!(
        status(&state, "check-after")["blocked_by"][0]
            .as_str()
            .unwrap()
            .contains("full-barrier")
    );

    fs::write(&check_release, "release").unwrap();
    wait_for(Duration::from_secs(5), || full_entered.exists());
    assert!(!after.exists());
    fs::write(&full_release, "release").unwrap();
    wait_status(&state, "full-barrier", "passed");
    wait_status(&state, "check-after", "passed");
    assert!(after.exists());
    assert_eq!(snapshot(&state).unwrap()["allocations"], json!({"jobs": 0}));
    assert!(broker.terminate().success());
}

#[test]
fn repository_round_robin_preserves_queue_order_within_each_rotation() {
    let temporary = TestDirectory::new("round-robin");
    let state = temporary.path().join("state");
    let checkout = temporary.path().join("checkout");
    fs::create_dir(&checkout).unwrap();
    let entered = temporary.path().join("entered");
    let release = temporary.path().join("release");
    let order = temporary.path().join("order");
    let mut broker = RunningBroker::start(&state, &[("jobs", 1)]);
    submit_ok(
        &state,
        &Submission {
            run_id: "rotation-anchor",
            kind: "check",
            repository: "repo-b",
            checkout: &checkout,
            command: blocking_command(&entered, &release, None),
            gate_run_id: None,
        },
    );
    wait_for(Duration::from_secs(5), || entered.exists());
    for (run_id, repository, value) in [
        ("rotation-z", "repo-z", "z"),
        ("rotation-c", "repo-c", "c"),
        ("rotation-a", "repo-a", "a"),
    ] {
        submit_ok(
            &state,
            &Submission {
                run_id,
                kind: "check",
                repository,
                checkout: &checkout,
                command: append_command(&order, value),
                gate_run_id: None,
            },
        );
    }
    fs::write(&release, "release").unwrap();
    wait_status(&state, "rotation-z", "passed");
    wait_status(&state, "rotation-c", "passed");
    wait_status(&state, "rotation-a", "passed");
    assert_eq!(fs::read_to_string(&order).unwrap(), "z\nc\na\n");
    assert!(broker.terminate().success());
}

#[test]
fn queued_and_running_cancellation_are_durable() {
    let temporary = TestDirectory::new("cancel");
    let state = temporary.path().join("state");
    let checkout = temporary.path().join("checkout");
    fs::create_dir(&checkout).unwrap();
    let mut broker = RunningBroker::start(&state, &[("jobs", 1)]);
    let entered = temporary.path().join("entered");
    let release = temporary.path().join("release");

    submit_ok(
        &state,
        &Submission {
            run_id: "running-cancel",
            kind: "check",
            repository: "repo-a",
            checkout: &checkout,
            command: blocking_command(&entered, &release, None),
            gate_run_id: None,
        },
    );
    wait_for(Duration::from_secs(5), || entered.exists());
    submit_ok(
        &state,
        &Submission {
            run_id: "queued-cancel",
            kind: "check",
            repository: "repo-b",
            checkout: &checkout,
            command: touch_command(&temporary.path().join("never")),
            gate_run_id: None,
        },
    );

    let queued = json_output(&[
        "cancel",
        "--state-dir",
        state_argument(&state),
        "--run-id",
        "queued-cancel",
    ]);
    assert_eq!(queued["status"], "cancelled");
    assert_eq!(queued["exit_status"], 130);
    let running = json_output(&[
        "cancel",
        "--state-dir",
        state_argument(&state),
        "--run-id",
        "running-cancel",
    ]);
    assert_eq!(running["status"], "running");
    assert_eq!(running["cancel_requested"], true);
    let finished = wait_status(&state, "running-cancel", "cancelled");
    assert_eq!(finished["exit_status"], 130);
    assert!(broker.terminate().success());
}

#[test]
fn committed_cancellation_wins_a_waiting_terminal_transition() {
    let temporary = TestDirectory::new("cancel-terminal-race");
    let state = temporary.path().join("state");
    let checkout = temporary.path().join("checkout");
    fs::create_dir(&state).unwrap();
    fs::write(state.join("config.json"), r#"{"database_timeout":1}"#).unwrap();
    fs::create_dir(&checkout).unwrap();
    let entered = temporary.path().join("entered");
    let release = temporary.path().join("release");
    let mut broker = RunningBroker::start(&state, &[("jobs", 1)]);
    submit_ok(
        &state,
        &Submission {
            run_id: "cancel-terminal-race",
            kind: "check",
            repository: "repo-a",
            checkout: &checkout,
            command: blocking_command(&entered, &release, None),
            gate_run_id: None,
        },
    );
    wait_for(Duration::from_secs(5), || entered.exists());

    let locker = Connection::open(state.join("queue.sqlite3")).unwrap();
    locker.execute_batch("BEGIN IMMEDIATE").unwrap();
    locker
        .execute(
            "UPDATE runs SET cancel_requested = 1,
                cancel_requested_at = '2026-08-31T00:00:00Z'
             WHERE run_id = 'cancel-terminal-race'",
            [],
        )
        .unwrap();
    fs::write(&release, "release").unwrap();
    thread::sleep(Duration::from_millis(100));
    locker.execute_batch("COMMIT").unwrap();

    let finished = wait_status(&state, "cancel-terminal-race", "cancelled");
    assert_eq!(finished["exit_status"], 130);
    assert!(broker.terminate().success());
}

#[test]
fn ownership_and_protocol_refusals_are_stable_and_non_mutating() {
    let temporary = TestDirectory::new("ownership");
    let state = temporary.path().join("state");
    let mut broker = RunningBroker::start(&state, &[("jobs", 1)]);

    let contender = run(&[
        "serve",
        "--state-dir",
        state_argument(&state),
        "--capacity",
        "jobs=1",
    ]);
    assert!(!contender.status.success());
    assert_eq!(
        serde_json::from_slice::<Value>(&contender.stderr).unwrap()["code"],
        "broker-already-owned"
    );
    assert!(broker.terminate().success());

    let database = state.join("queue.sqlite3");
    let db = Connection::open(&database).unwrap();
    db.execute(
        "UPDATE coordinator_meta SET value = '99' WHERE key = 'protocol'",
        [],
    )
    .unwrap();
    drop(db);
    let unsupported = run(&[
        "serve",
        "--state-dir",
        state_argument(&state),
        "--capacity",
        "jobs=1",
    ]);
    assert!(!unsupported.status.success());
    assert_eq!(
        serde_json::from_slice::<Value>(&unsupported.stderr).unwrap()["code"],
        "broker-protocol-unsupported"
    );
    let db = Connection::open(&database).unwrap();
    assert_eq!(
        db.query_row(
            "SELECT value FROM coordinator_meta WHERE key = 'protocol'",
            [],
            |row| row.get::<_, String>(0)
        )
        .unwrap(),
        "99"
    );
}

#[test]
fn replacement_adopts_a_live_worker_without_duplicate_execution() {
    let temporary = TestDirectory::new("adoption");
    let state = temporary.path().join("state");
    let checkout = temporary.path().join("checkout");
    fs::create_dir(&checkout).unwrap();
    let entered = temporary.path().join("entered");
    let release = temporary.path().join("release");
    let starts = temporary.path().join("starts");
    let mut first = RunningBroker::start(&state, &[("jobs", 1)]);
    submit_ok(
        &state,
        &Submission {
            run_id: "adopted-check",
            kind: "check",
            repository: "repo-a",
            checkout: &checkout,
            command: blocking_command(&entered, &release, Some(&starts)),
            gate_run_id: None,
        },
    );
    wait_for(Duration::from_secs(5), || entered.exists());
    let worker_pid = status(&state, "adopted-check")["worker_pid"].clone();
    first.kill();

    let mut second = RunningBroker::start(&state, &[("jobs", 1)]);
    assert_eq!(status(&state, "adopted-check")["worker_pid"], worker_pid);
    assert_eq!(fs::read_to_string(&starts).unwrap().lines().count(), 1);
    fs::write(&release, "release").unwrap();
    let interrupted = wait_status(&state, "adopted-check", "interrupted");
    assert_eq!(interrupted["failure_reason"], "worker-result-lost");
    assert_eq!(fs::read_to_string(&starts).unwrap().lines().count(), 1);
    assert!(second.terminate().success());
}

#[test]
fn graceful_sigterm_cancels_and_drains_active_work() {
    let temporary = TestDirectory::new("shutdown");
    let state = temporary.path().join("state");
    let checkout = temporary.path().join("checkout");
    fs::create_dir(&checkout).unwrap();
    let entered = temporary.path().join("entered");
    let release = temporary.path().join("release");
    let mut broker = RunningBroker::start(&state, &[("jobs", 1)]);
    submit_ok(
        &state,
        &Submission {
            run_id: "shutdown-check",
            kind: "check",
            repository: "repo-a",
            checkout: &checkout,
            command: blocking_command(&entered, &release, None),
            gate_run_id: None,
        },
    );
    wait_for(Duration::from_secs(5), || entered.exists());
    assert!(broker.terminate().success());
    let row = status(&state, "shutdown-check");
    assert_eq!(row["status"], "cancelled");
    assert_eq!(row["exit_status"], 130);
}

#[test]
fn graceful_sigterm_leaves_queued_work_for_the_next_owner() {
    let temporary = TestDirectory::new("shutdown-queued");
    let state = temporary.path().join("state");
    let checkout = temporary.path().join("checkout");
    fs::create_dir(&checkout).unwrap();
    let entered = temporary.path().join("entered");
    let release = temporary.path().join("release");
    let queued_marker = temporary.path().join("queued-ran");
    let mut broker = RunningBroker::start(&state, &[("jobs", 1)]);
    submit_ok(
        &state,
        &Submission {
            run_id: "shutdown-active",
            kind: "check",
            repository: "repo-a",
            checkout: &checkout,
            command: blocking_command(&entered, &release, None),
            gate_run_id: None,
        },
    );
    wait_for(Duration::from_secs(5), || entered.exists());
    submit_ok(
        &state,
        &Submission {
            run_id: "shutdown-queued",
            kind: "check",
            repository: "repo-b",
            checkout: &checkout,
            command: touch_command(&queued_marker),
            gate_run_id: None,
        },
    );

    assert!(broker.terminate().success());
    assert_eq!(status(&state, "shutdown-active")["status"], "cancelled");
    assert_eq!(status(&state, "shutdown-queued")["status"], "queued");
    assert!(!queued_marker.exists());
}

#[test]
fn graceful_sigterm_retries_a_contended_cancellation_commit() {
    let temporary = TestDirectory::new("shutdown-contention");
    let state = temporary.path().join("state");
    let checkout = temporary.path().join("checkout");
    fs::create_dir(&state).unwrap();
    fs::write(state.join("config.json"), r#"{"database_timeout":0.05}"#).unwrap();
    fs::create_dir(&checkout).unwrap();
    let entered = temporary.path().join("entered");
    let release = temporary.path().join("release");
    let mut broker = RunningBroker::start(&state, &[("jobs", 1)]);
    submit_ok(
        &state,
        &Submission {
            run_id: "shutdown-contended",
            kind: "check",
            repository: "repo-a",
            checkout: &checkout,
            command: blocking_command(&entered, &release, None),
            gate_run_id: None,
        },
    );
    wait_for(Duration::from_secs(5), || entered.exists());
    let locker = Connection::open(state.join("queue.sqlite3")).unwrap();
    locker.execute_batch("BEGIN IMMEDIATE").unwrap();
    broker.signal_terminate();
    thread::sleep(Duration::from_millis(150));
    let retained_ownership = broker.is_running();
    locker.execute_batch("ROLLBACK").unwrap();
    fs::write(&release, "release").unwrap();
    assert!(
        retained_ownership,
        "broker abandoned a clean shutdown on contention"
    );
    assert!(broker.wait().success());
    assert_eq!(status(&state, "shutdown-contended")["status"], "cancelled");
}

#[test]
fn one_worker_start_failure_does_not_stop_the_broker() {
    let temporary = TestDirectory::new("spawn-failure");
    let state = temporary.path().join("state");
    let checkout = temporary.path().join("checkout");
    fs::create_dir(&checkout).unwrap();
    let succeeded = temporary.path().join("succeeded");
    let mut broker = RunningBroker::start(&state, &[("jobs", 1)]);
    submit_ok(
        &state,
        &Submission {
            run_id: "bad-executable",
            kind: "check",
            repository: "repo-a",
            checkout: &checkout,
            command: vec!["/definitely/not/an/executable".to_owned()],
            gate_run_id: None,
        },
    );
    submit_ok(
        &state,
        &Submission {
            run_id: "good-executable",
            kind: "check",
            repository: "repo-b",
            checkout: &checkout,
            command: touch_command(&succeeded),
            gate_run_id: None,
        },
    );

    let failed = wait_status(&state, "bad-executable", "failed");
    assert_eq!(failed["failure_reason"], "worker-start-failed");
    wait_status(&state, "good-executable", "passed");
    assert!(succeeded.exists());
    assert!(broker.terminate().success());
}

#[test]
fn transient_database_contention_keeps_the_same_owner_and_honors_timeout() {
    let temporary = TestDirectory::new("database-contention");
    let state = temporary.path().join("state");
    let checkout = temporary.path().join("checkout");
    fs::create_dir(&state).unwrap();
    fs::write(state.join("config.json"), r#"{"database_timeout":0.05}"#).unwrap();
    fs::create_dir(&checkout).unwrap();
    let entered = temporary.path().join("entered");
    let release = temporary.path().join("release");
    let after = temporary.path().join("after");
    let mut broker = RunningBroker::start(&state, &[("jobs", 1)]);
    let owner_pid = snapshot(&state).unwrap()["broker_pid"].clone();
    submit_ok(
        &state,
        &Submission {
            run_id: "contention-active",
            kind: "check",
            repository: "repo-a",
            checkout: &checkout,
            command: blocking_command(&entered, &release, None),
            gate_run_id: None,
        },
    );
    wait_for(Duration::from_secs(5), || entered.exists());
    submit_ok(
        &state,
        &Submission {
            run_id: "contention-after",
            kind: "check",
            repository: "repo-b",
            checkout: &checkout,
            command: touch_command(&after),
            gate_run_id: None,
        },
    );

    let locker = Connection::open(state.join("queue.sqlite3")).unwrap();
    locker.execute_batch("BEGIN IMMEDIATE").unwrap();
    fs::write(&release, "release").unwrap();
    thread::sleep(Duration::from_millis(180));
    let started = Instant::now();
    let refused = run(&[
        "cancel",
        "--state-dir",
        state_argument(&state),
        "--run-id",
        "contention-after",
    ]);
    assert!(!refused.status.success());
    assert_eq!(
        serde_json::from_slice::<Value>(&refused.stderr).unwrap()["code"],
        "broker-database-busy"
    );
    assert!(started.elapsed() < Duration::from_secs(1));
    locker.execute_batch("ROLLBACK").unwrap();

    wait_status(&state, "contention-active", "passed");
    wait_status(&state, "contention-after", "passed");
    assert!(after.exists());
    assert_eq!(snapshot(&state).unwrap()["broker_pid"], owner_pid);
    assert!(broker.terminate().success());
}

#[test]
fn identity_commit_contention_never_orphans_a_started_worker() {
    let temporary = TestDirectory::new("identity-contention");
    let state = temporary.path().join("state");
    let checkout = temporary.path().join("checkout");
    fs::create_dir(&state).unwrap();
    fs::write(state.join("config.json"), r#"{"database_timeout":0.05}"#).unwrap();
    fs::create_dir(&checkout).unwrap();
    let marker = temporary.path().join("worker-ran");
    let mut broker = RunningBroker::start(&state, &[("jobs", 1)]);
    let fifo = state.join("logs/identity-contention.log");
    assert!(
        Command::new("/usr/bin/mkfifo")
            .arg(&fifo)
            .status()
            .unwrap()
            .success()
    );
    submit_ok(
        &state,
        &Submission {
            run_id: "identity-contention",
            kind: "check",
            repository: "repo-a",
            checkout: &checkout,
            command: touch_command(&marker),
            gate_run_id: None,
        },
    );
    wait_for(Duration::from_secs(5), || {
        let row = status(&state, "identity-contention");
        row["status"] == "running" && row["worker_pid"].is_null()
    });

    let locker = Connection::open(state.join("queue.sqlite3")).unwrap();
    locker.execute_batch("BEGIN IMMEDIATE").unwrap();
    let _fifo_reader = OpenOptions::new().read(true).open(&fifo).unwrap();
    thread::sleep(Duration::from_millis(150));
    assert!(
        !marker.exists(),
        "worker ran before its durable identity commit completed"
    );
    locker.execute_batch("ROLLBACK").unwrap();

    let completed = wait_status(&state, "identity-contention", "passed");
    assert!(marker.exists());
    assert!(completed["worker_pid"].is_number());
    assert!(broker.terminate().success());
}

#[test]
fn native_owner_refuses_invalid_sections_in_the_shared_broker_config() {
    let temporary = TestDirectory::new("invalid-config");
    for (index, document) in [
        r#"{"capacities":"jobs=2"}"#,
        r#"{"bindings":[]}"#,
        r#"{"cgroup_root":""}"#,
        r#"{"cgroup_io":{"paths":[]}}"#,
        r#"{"cgroup_io":{"paths":["relative"]}}"#,
    ]
    .into_iter()
    .enumerate()
    {
        let state = temporary.path().join(format!("state-{index}"));
        fs::create_dir(&state).unwrap();
        fs::write(state.join("config.json"), document).unwrap();
        let refused = run(&[
            "serve",
            "--state-dir",
            state_argument(&state),
            "--capacity",
            "jobs=1",
            "--idle-timeout",
            "0.1",
        ]);
        assert!(
            !refused.status.success(),
            "invalid config was accepted: {document}"
        );
        assert_eq!(
            serde_json::from_slice::<Value>(&refused.stderr).unwrap()["code"],
            "broker-config-invalid"
        );
        assert!(!state.join("queue.sqlite3").exists());
    }
}

#[test]
fn required_cgroup_binding_refuses_before_user_code_when_delegation_is_unavailable() {
    let temporary = TestDirectory::new("required-cgroup-unavailable");
    let state = temporary.path().join("state");
    let root = temporary.path().join("not-a-cgroup");
    let checkout = temporary.path().join("checkout");
    let marker = temporary.path().join("must-not-run");
    fs::create_dir(&state).unwrap();
    fs::create_dir(&root).unwrap();
    fs::create_dir(&checkout).unwrap();
    fs::write(
        state.join("config.json"),
        serde_json::to_vec(&json!({
            "bindings": {
                "cpu": {
                    "backend": "cgroup-v2",
                    "kind": "cpu",
                    "mode": "required",
                    "unit": "logical-cpu"
                }
            },
            "cgroup_root": root,
        }))
        .unwrap(),
    )
    .unwrap();
    let mut broker = RunningBroker::start(&state, &[("jobs", 1), ("cpu", 1)]);

    let machine = snapshot(&state).unwrap();
    assert_eq!(
        machine["resource_bindings"]["cpu"],
        json!({
            "backend": "cgroup-v2",
            "kind": "cpu",
            "mode": "required",
            "unit": "logical-cpu",
        })
    );
    assert_eq!(
        machine["resource_capabilities"]["cgroup-v2"]["reason"],
        "not-cgroup-v2"
    );

    let submission = Submission {
        run_id: "required-cgroup-unavailable",
        kind: "check",
        repository: "repo-a",
        checkout: &checkout,
        command: touch_command(&marker),
        gate_run_id: None,
    };
    let submitted = submit_with_resources(&state, &submission, &[("cpu", 1)]);
    assert!(submitted.status.success());
    let failed = wait_status(&state, submission.run_id, "failed");
    assert_eq!(failed["exit_status"], 125);
    assert_eq!(failed["failure_reason"], "resource-enforcement-failed");
    assert_eq!(failed["resource_receipt"]["applied"], json!({}));
    assert!(
        failed["resource_receipt"]["events"]
            .as_array()
            .unwrap()
            .iter()
            .any(|event| event["resource"] == "cpu"
                && event["stage"] == "probe"
                && event["status"] == "failed"
                && event["code"] == "not-cgroup-v2")
    );
    assert!(!marker.exists());
    assert!(broker.terminate().success());
}

#[test]
fn best_effort_cgroup_binding_runs_unenforced_when_delegation_is_unavailable() {
    let temporary = TestDirectory::new("best-effort-cgroup-unavailable");
    let state = temporary.path().join("state");
    let root = temporary.path().join("not-a-cgroup");
    let checkout = temporary.path().join("checkout");
    let marker = temporary.path().join("ran-unenforced");
    for path in [&state, &root, &checkout] {
        fs::create_dir(path).unwrap();
    }
    fs::write(
        state.join("config.json"),
        serde_json::to_vec(&json!({
            "bindings": {
                "cpu": {"backend":"cgroup-v2", "kind":"cpu", "mode":"best-effort", "unit":"logical-cpu"}
            },
            "cgroup_root": root,
        }))
        .unwrap(),
    )
    .unwrap();
    let mut broker = RunningBroker::start(&state, &[("jobs", 1), ("cpu", 1)]);
    let submission = Submission {
        run_id: "best-effort-cgroup-unavailable",
        kind: "check",
        repository: "repo-a",
        checkout: &checkout,
        command: touch_command(&marker),
        gate_run_id: None,
    };
    assert!(
        submit_with_resources(&state, &submission, &[("cpu", 1)])
            .status
            .success()
    );
    let passed = wait_status(&state, submission.run_id, "passed");
    assert!(marker.exists());
    assert_eq!(passed["resource_receipt"]["applied"], json!({}));
    assert!(
        passed["resource_receipt"]["events"]
            .as_array()
            .unwrap()
            .iter()
            .any(|event| event["resource"] == "cpu"
                && event["stage"] == "probe"
                && event["status"] == "unapplied"
                && event["code"] == "not-cgroup-v2")
    );
    assert!(broker.terminate().success());
}

#[test]
fn project_quota_fixture_provisions_private_scratch_and_retains_usage() {
    let temporary = TestDirectory::new("project-quota-fixture");
    let fixture = temporary.path().join("quota-filesystem");
    let state = fixture.join("state");
    let checkout = temporary.path().join("checkout");
    let report = temporary.path().join("report.json");
    for path in [&fixture, &state, &checkout] {
        fs::create_dir(path).unwrap();
    }
    fs::write(
        state.join("config.json"),
        serde_json::to_vec(&json!({
            "bindings": {
                "disk": {
                    "backend": "project-quota",
                    "kind": "storage",
                    "mode": "required",
                    "unit": "bytes"
                },
                "disk_inodes": {
                    "backend": "project-quota",
                    "kind": "inodes",
                    "mode": "required",
                    "unit": "inodes"
                }
            }
        }))
        .unwrap(),
    )
    .unwrap();
    let mut broker = RunningBroker::start_with_options(
        &state,
        &[("jobs", 1), ("disk", 8 * 1024 * 1024), ("disk_inodes", 64)],
        &["--project-quota-fixture", fixture.to_str().unwrap()],
    );
    let machine = snapshot(&state).unwrap();
    assert_eq!(
        machine["resource_capabilities"]["project-quota"]["available"],
        true
    );

    let command = vec![
        "/usr/bin/python3".to_owned(),
        "-c".to_owned(),
        "import ctypes,json,os,pathlib,sys; target=pathlib.Path(os.environ['TMPDIR']); (target/'payload').write_bytes(b'x'*8192); status={line.split(':',1)[0]:line.split(':',1)[1].strip() for line in pathlib.Path('/proc/self/status').read_text().splitlines() if ':' in line}; libc=ctypes.CDLL(None,use_errno=True); ctypes.set_errno(0); reacquire=libc.prctl(47,2,21,0,0); pathlib.Path(sys.argv[1]).write_text(json.dumps({'target':str(target),'mode':target.stat().st_mode & 0o777,'caps':[status[name] for name in ('CapEff','CapPrm','CapInh','CapAmb')],'no_new_privs':status['NoNewPrivs'],'reacquire':[reacquire,ctypes.get_errno()]}))".to_owned(),
        report.to_str().unwrap().to_owned(),
    ];
    let submission = Submission {
        run_id: "project-quota-fixture",
        kind: "check",
        repository: "repo-a",
        checkout: &checkout,
        command,
        gate_run_id: None,
    };
    assert!(
        submit_with_resources(
            &state,
            &submission,
            &[("disk", 8 * 1024 * 1024), ("disk_inodes", 64)],
        )
        .status
        .success()
    );
    let passed = wait_status(&state, submission.run_id, "passed");
    let observed: Value = serde_json::from_slice(&fs::read(&report).unwrap()).unwrap();
    let target = PathBuf::from(observed["target"].as_str().unwrap());
    assert_eq!(observed["mode"], 0o700);
    assert_eq!(
        observed["caps"],
        json!([
            "0000000000000000",
            "0000000000000000",
            "0000000000000000",
            "0000000000000000"
        ])
    );
    assert_eq!(observed["no_new_privs"], "1");
    assert_eq!(observed["reacquire"], json!([-1, libc::EPERM]));
    assert_eq!(
        passed["resource_receipt"]["applied"],
        json!({"disk": 8 * 1024 * 1024, "disk_inodes": 64})
    );
    assert!(passed["resource_receipt"]["peak"]["disk"].as_u64().unwrap() >= 8192);
    assert!(
        passed["resource_receipt"]["peak"]["disk_inodes"]
            .as_u64()
            .unwrap()
            >= 2
    );
    assert!(!target.exists());
    assert!(
        fs::read_dir(state.join("project-quota").join("runs"))
            .unwrap()
            .next()
            .is_none()
    );
    assert!(broker.terminate().success());
}

#[test]
fn project_quota_fixture_refuses_incomplete_misaligned_or_mixed_policies() {
    let temporary = TestDirectory::new("project-quota-policy-refusals");
    let checkout = temporary.path().join("checkout");
    fs::create_dir(&checkout).unwrap();
    let cases = [
        (
            "missing-inodes",
            json!({
                "disk": {"backend":"project-quota", "kind":"storage", "mode":"required", "unit":"bytes"},
                "disk_inodes": {"backend":"project-quota", "kind":"inodes", "mode":"required", "unit":"inodes"}
            }),
            vec![("disk", 8 * 1024 * 1024)],
            "quota-policy-incomplete",
        ),
        (
            "misaligned-bytes",
            json!({
                "disk": {"backend":"project-quota", "kind":"storage", "mode":"required", "unit":"bytes"},
                "disk_inodes": {"backend":"project-quota", "kind":"inodes", "mode":"required", "unit":"inodes"}
            }),
            vec![("disk", 8 * 1024 * 1024 + 1), ("disk_inodes", 64)],
            "quota-byte-alignment-invalid",
        ),
        (
            "mixed-modes",
            json!({
                "disk": {"backend":"project-quota", "kind":"storage", "mode":"required", "unit":"bytes"},
                "disk_inodes": {"backend":"project-quota", "kind":"inodes", "mode":"best-effort", "unit":"inodes"}
            }),
            vec![("disk", 8 * 1024 * 1024), ("disk_inodes", 64)],
            "quota-mode-mismatch",
        ),
    ];
    for (name, bindings, requested, expected_code) in cases {
        let fixture = temporary.path().join(format!("fixture-{name}"));
        let state = fixture.join("state");
        fs::create_dir_all(&state).unwrap();
        fs::write(
            state.join("config.json"),
            serde_json::to_vec(&json!({"bindings": bindings})).unwrap(),
        )
        .unwrap();
        let mut capacities = vec![
            ("jobs", 1),
            ("disk", 16 * 1024 * 1024),
            ("disk_inodes", 128),
        ];
        if name == "missing-inodes" {
            capacities.retain(|(resource, _)| *resource != "disk_inodes");
        }
        let mut broker = RunningBroker::start_with_options(
            &state,
            &capacities,
            &["--project-quota-fixture", fixture.to_str().unwrap()],
        );
        let marker = temporary.path().join(format!("must-not-run-{name}"));
        let submission = Submission {
            run_id: name,
            kind: "check",
            repository: "repo-a",
            checkout: &checkout,
            command: touch_command(&marker),
            gate_run_id: None,
        };
        assert!(
            submit_with_resources(&state, &submission, &requested)
                .status
                .success()
        );
        let failed = wait_status(&state, name, "failed");
        assert_eq!(failed["failure_reason"], "resource-enforcement-failed");
        assert!(
            failed["resource_receipt"]["events"]
                .as_array()
                .unwrap()
                .iter()
                .any(|event| event["stage"] == "prepare" && event["code"] == expected_code),
            "{name}: {failed}"
        );
        assert!(!marker.exists());
        assert!(broker.terminate().success());
    }
}

#[test]
fn unavailable_project_quota_obeys_required_or_best_effort_fallback() {
    let temporary = TestDirectory::new("project-quota-unavailable");
    let checkout = temporary.path().join("checkout");
    fs::create_dir(&checkout).unwrap();
    for (mode, expected_status, ran) in [
        ("required", "failed", false),
        ("best-effort", "passed", true),
    ] {
        let fixture = temporary.path().join(format!("missing-{mode}"));
        let state = temporary.path().join(format!("state-{mode}"));
        fs::create_dir(&state).unwrap();
        write_project_quota_config(&state, mode);
        let mut broker = RunningBroker::start_with_options(
            &state,
            &[("jobs", 1), ("disk", 8 * 1024 * 1024), ("disk_inodes", 64)],
            &["--project-quota-fixture", fixture.to_str().unwrap()],
        );
        assert_eq!(
            snapshot(&state).unwrap()["resource_capabilities"]["project-quota"]["reason"],
            "quota-root-unavailable"
        );
        let marker = temporary.path().join(format!("ran-{mode}"));
        let submission = Submission {
            run_id: mode,
            kind: "check",
            repository: "repo-a",
            checkout: &checkout,
            command: touch_command(&marker),
            gate_run_id: None,
        };
        assert!(
            submit_with_resources(
                &state,
                &submission,
                &[("disk", 8 * 1024 * 1024), ("disk_inodes", 64)],
            )
            .status
            .success()
        );
        let finished = wait_status(&state, mode, expected_status);
        assert_eq!(marker.exists(), ran);
        assert_eq!(
            finished["resource_receipt"]["events"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|event| event["code"] == "quota-root-unavailable")
                .count(),
            2
        );
        assert!(broker.terminate().success());
    }
}

#[test]
fn project_quota_worker_privilege_refusal_always_fails_closed() {
    let temporary = TestDirectory::new("project-quota-privilege-refusal");
    let checkout = temporary.path().join("checkout");
    fs::create_dir(&checkout).unwrap();
    for mode in ["required", "best-effort"] {
        let fixture = temporary.path().join(format!("fixture-{mode}"));
        let state = fixture.join("state");
        fs::create_dir_all(&state).unwrap();
        write_project_quota_config(&state, mode);
        let mut broker = RunningBroker::start_with_options(
            &state,
            &[("jobs", 1), ("disk", 8 * 1024 * 1024), ("disk_inodes", 64)],
            &[
                "--project-quota-fixture",
                fixture.to_str().unwrap(),
                "--worker-fault",
                "privilege-verification",
            ],
        );
        let marker = temporary.path().join(format!("must-not-run-{mode}"));
        let submission = Submission {
            run_id: mode,
            kind: "check",
            repository: "repo-a",
            checkout: &checkout,
            command: touch_command(&marker),
            gate_run_id: None,
        };
        assert!(
            submit_with_resources(
                &state,
                &submission,
                &[("disk", 8 * 1024 * 1024), ("disk_inodes", 64)],
            )
            .status
            .success()
        );
        let failed = wait_status(&state, mode, "failed");
        assert_eq!(failed["exit_status"], 125);
        assert_eq!(failed["failure_reason"], "resource-enforcement-failed");
        assert!(!marker.exists());
        assert_eq!(
            failed["resource_receipt"]["events"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|event| event["code"] == "worker-privilege-drop-unverified")
                .count(),
            2
        );
        assert!(broker.terminate().success());
    }
}

#[test]
fn project_quota_fixture_keeps_parallel_allocations_distinct() {
    let temporary = TestDirectory::new("project-quota-parallel");
    let fixture = temporary.path().join("fixture");
    let state = fixture.join("state");
    let checkout = temporary.path().join("checkout");
    fs::create_dir_all(&state).unwrap();
    fs::create_dir(&checkout).unwrap();
    write_project_quota_config(&state, "required");
    let mut broker = RunningBroker::start_with_options(
        &state,
        &[("jobs", 2), ("disk", 8 * 1024 * 1024), ("disk_inodes", 128)],
        &["--project-quota-fixture", fixture.to_str().unwrap()],
    );
    let script = "import os,pathlib,sys,time; target=pathlib.Path(os.environ['TMPDIR']); (target/'payload').write_bytes(b'x'*4096); pathlib.Path(sys.argv[1]).write_text(str(target)); release=pathlib.Path(sys.argv[2]);\nwhile not release.exists(): time.sleep(.01)";
    let mut submissions = Vec::new();
    for (name, repository) in [
        ("quota-parallel-a", "repo-a"),
        ("quota-parallel-b", "repo-b"),
    ] {
        let report = temporary.path().join(format!("{name}.path"));
        let release = temporary.path().join(format!("{name}.release"));
        let submission = Submission {
            run_id: name,
            kind: "check",
            repository,
            checkout: &checkout,
            command: vec![
                "/usr/bin/python3".to_owned(),
                "-c".to_owned(),
                script.to_owned(),
                report.to_str().unwrap().to_owned(),
                release.to_str().unwrap().to_owned(),
            ],
            gate_run_id: None,
        };
        assert!(
            submit_with_resources(
                &state,
                &submission,
                &[("disk", 4 * 1024 * 1024), ("disk_inodes", 32)],
            )
            .status
            .success()
        );
        submissions.push((name, report, release));
    }
    let targets: Vec<_> = submissions
        .iter()
        .map(|(_, report, _)| {
            PathBuf::from(
                String::from_utf8(wait_for_nonempty_file(report, Duration::from_secs(5))).unwrap(),
            )
        })
        .collect();
    assert_ne!(targets[0], targets[1]);
    assert!(targets.iter().all(|target| target.is_dir()));
    for (_, _, release) in &submissions {
        fs::write(release, "release").unwrap();
    }
    for (name, _, _) in &submissions {
        let passed = wait_status(&state, name, "passed");
        assert_eq!(
            passed["resource_receipt"]["applied"],
            json!({"disk": 4 * 1024 * 1024, "disk_inodes": 32})
        );
    }
    assert!(targets.iter().all(|target| !target.exists()));
    let registry: Value = serde_json::from_slice(
        &fs::read(fixture.join(".agcoord-project-quota-fixture.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(registry["projects"], json!({}));
    assert!(broker.terminate().success());
}

#[test]
fn project_quota_cancellation_retains_terminal_usage_before_cleanup() {
    let temporary = TestDirectory::new("project-quota-cancel");
    let fixture = temporary.path().join("fixture");
    let state = fixture.join("state");
    let checkout = temporary.path().join("checkout");
    let report = temporary.path().join("target.path");
    fs::create_dir_all(&state).unwrap();
    fs::create_dir(&checkout).unwrap();
    write_project_quota_config(&state, "required");
    let mut broker = RunningBroker::start_with_options(
        &state,
        &[("jobs", 1), ("disk", 8 * 1024 * 1024), ("disk_inodes", 64)],
        &["--project-quota-fixture", fixture.to_str().unwrap()],
    );
    let submission = Submission {
        run_id: "project-quota-cancel",
        kind: "check",
        repository: "repo-a",
        checkout: &checkout,
        command: vec![
            "/usr/bin/python3".to_owned(),
            "-c".to_owned(),
            "import os,pathlib,sys,time; target=pathlib.Path(os.environ['TMPDIR']); (target/'payload').write_bytes(b'x'*8192); pathlib.Path(sys.argv[1]).write_text(str(target));\nwhile True: time.sleep(1)".to_owned(),
            report.to_str().unwrap().to_owned(),
        ],
        gate_run_id: None,
    };
    assert!(
        submit_with_resources(
            &state,
            &submission,
            &[("disk", 8 * 1024 * 1024), ("disk_inodes", 64)],
        )
        .status
        .success()
    );
    let target = PathBuf::from(
        String::from_utf8(wait_for_nonempty_file(&report, Duration::from_secs(5))).unwrap(),
    );
    assert!(
        run(&[
            "cancel",
            "--state-dir",
            state_argument(&state),
            "--run-id",
            submission.run_id,
        ])
        .status
        .success()
    );
    let cancelled = wait_status(&state, submission.run_id, "cancelled");
    assert!(
        cancelled["resource_receipt"]["peak"]["disk"]
            .as_u64()
            .unwrap()
            >= 8192
    );
    assert!(
        cancelled["resource_receipt"]["peak"]["disk_inodes"]
            .as_u64()
            .unwrap()
            >= 2
    );
    let codes: BTreeSet<_> = cancelled["resource_receipt"]["events"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|event| event["code"].as_str())
        .collect();
    assert!(codes.contains("cancelled"));
    assert!(codes.contains("finished"));
    assert!(codes.contains("cleaned"));
    assert!(!target.exists());
    assert!(broker.terminate().success());
}

#[test]
fn replacement_recovers_live_project_quota_state_and_cleans_owned_data() {
    let temporary = TestDirectory::new("project-quota-recovery");
    let fixture = temporary.path().join("fixture");
    let state = fixture.join("state");
    let checkout = temporary.path().join("checkout");
    let entered = temporary.path().join("entered.path");
    let release = temporary.path().join("release");
    fs::create_dir_all(&state).unwrap();
    fs::create_dir(&checkout).unwrap();
    write_project_quota_config(&state, "required");
    let fixture_argument = fixture.to_str().unwrap();
    let mut crashing = RunningBroker::start_with_options(
        &state,
        &[("jobs", 1), ("disk", 8 * 1024 * 1024), ("disk_inodes", 64)],
        &[
            "--project-quota-fixture",
            fixture_argument,
            "--crash-after",
            "worker-release",
        ],
    );
    let submission = Submission {
        run_id: "project-quota-recovery",
        kind: "check",
        repository: "repo-a",
        checkout: &checkout,
        command: vec![
            "/usr/bin/python3".to_owned(),
            "-c".to_owned(),
            "import os,pathlib,sys,time; target=pathlib.Path(os.environ['TMPDIR']); (target/'survives').write_bytes(b'x'*8192); pathlib.Path(sys.argv[1]).write_text(str(target)); release=pathlib.Path(sys.argv[2]);\nwhile not release.exists(): time.sleep(.01)".to_owned(),
            entered.to_str().unwrap().to_owned(),
            release.to_str().unwrap().to_owned(),
        ],
        gate_run_id: None,
    };
    assert!(
        submit_with_resources(
            &state,
            &submission,
            &[("disk", 8 * 1024 * 1024), ("disk_inodes", 64)],
        )
        .status
        .success()
    );
    assert_eq!(crashing.wait().code(), Some(86));
    let target = PathBuf::from(
        String::from_utf8(wait_for_nonempty_file(&entered, Duration::from_secs(5))).unwrap(),
    );
    let original_pid = status(&state, submission.run_id)["worker_pid"].clone();
    let mut replacement = RunningBroker::start_with_options(
        &state,
        &[("jobs", 1), ("disk", 8 * 1024 * 1024), ("disk_inodes", 64)],
        &["--project-quota-fixture", fixture_argument],
    );
    assert_eq!(
        status(&state, submission.run_id)["worker_pid"],
        original_pid
    );
    fs::write(&release, "release").unwrap();
    let interrupted = wait_status(&state, submission.run_id, "interrupted");
    assert_eq!(interrupted["failure_reason"], "worker-result-lost");
    assert!(
        interrupted["resource_receipt"]["peak"]["disk"]
            .as_u64()
            .unwrap()
            >= 8192
    );
    let codes: BTreeSet<_> = interrupted["resource_receipt"]["events"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|event| event["code"].as_str())
        .collect();
    assert!(codes.contains("finished"));
    assert!(codes.contains("cleaned"));
    assert!(!target.exists());
    assert!(replacement.terminate().success());
}

#[test]
fn project_quota_identity_and_setup_crashes_reclaim_without_execution() {
    let temporary = TestDirectory::new("project-quota-pre-release-crashes");
    let checkout = temporary.path().join("checkout");
    fs::create_dir(&checkout).unwrap();
    for crash_point in ["worker-identity-commit", "worker-setup-commit"] {
        let fixture = temporary.path().join(format!("fixture-{crash_point}"));
        let state = fixture.join("state");
        fs::create_dir_all(&state).unwrap();
        write_project_quota_config(&state, "required");
        let fixture_argument = fixture.to_str().unwrap();
        let marker = temporary.path().join(format!("must-not-run-{crash_point}"));
        let mut crashing = RunningBroker::start_with_options(
            &state,
            &[("jobs", 1), ("disk", 8 * 1024 * 1024), ("disk_inodes", 64)],
            &[
                "--project-quota-fixture",
                fixture_argument,
                "--crash-after",
                crash_point,
            ],
        );
        let submission = Submission {
            run_id: crash_point,
            kind: "check",
            repository: "repo-a",
            checkout: &checkout,
            command: touch_command(&marker),
            gate_run_id: None,
        };
        assert!(
            submit_with_resources(
                &state,
                &submission,
                &[("disk", 8 * 1024 * 1024), ("disk_inodes", 64)],
            )
            .status
            .success()
        );
        assert_eq!(crashing.wait().code(), Some(86));
        thread::sleep(Duration::from_millis(100));
        assert!(!marker.exists());
        let mut replacement = RunningBroker::start_with_options(
            &state,
            &[("jobs", 1), ("disk", 8 * 1024 * 1024), ("disk_inodes", 64)],
            &["--project-quota-fixture", fixture_argument],
        );
        let interrupted = wait_status(&state, crash_point, "interrupted");
        assert_eq!(interrupted["failure_reason"], "worker-result-lost");
        assert!(!marker.exists());
        assert!(
            fs::read_dir(state.join("project-quota").join("runs"))
                .unwrap()
                .next()
                .is_none()
        );
        let registry: Value = serde_json::from_slice(
            &fs::read(fixture.join(".agcoord-project-quota-fixture.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(registry["projects"], json!({}));
        assert!(replacement.terminate().success());
    }
}

#[test]
fn replacement_refuses_a_changed_project_quota_handle_without_mutation() {
    let temporary = TestDirectory::new("project-quota-handle-mismatch");
    let fixture = temporary.path().join("fixture");
    let state = fixture.join("state");
    let checkout = temporary.path().join("checkout");
    let marker = temporary.path().join("must-not-run");
    fs::create_dir_all(&state).unwrap();
    fs::create_dir(&checkout).unwrap();
    write_project_quota_config(&state, "required");
    let fixture_argument = fixture.to_str().unwrap();
    let mut crashing = RunningBroker::start_with_options(
        &state,
        &[("jobs", 1), ("disk", 8 * 1024 * 1024), ("disk_inodes", 64)],
        &[
            "--project-quota-fixture",
            fixture_argument,
            "--crash-after",
            "worker-identity-commit",
        ],
    );
    let submission = Submission {
        run_id: "project-quota-handle-mismatch",
        kind: "check",
        repository: "repo-a",
        checkout: &checkout,
        command: touch_command(&marker),
        gate_run_id: None,
    };
    assert!(
        submit_with_resources(
            &state,
            &submission,
            &[("disk", 8 * 1024 * 1024), ("disk_inodes", 64)],
        )
        .status
        .success()
    );
    assert_eq!(crashing.wait().code(), Some(86));
    let database = state.join("queue.sqlite3");
    let db = Connection::open(&database).unwrap();
    let original_state: String = db
        .query_row(
            "SELECT resource_state_json FROM runs WHERE run_id = ?1",
            params![submission.run_id],
            |row| row.get(0),
        )
        .unwrap();
    let mut changed: Value = serde_json::from_str(&original_state).unwrap();
    changed["project-quota"]["handle"]["token"] = json!("00000000000000000000000000000000");
    let changed_state = serde_json::to_string(&changed).unwrap();
    db.execute(
        "UPDATE runs SET resource_state_json = ?1 WHERE run_id = ?2",
        params![changed_state, submission.run_id],
    )
    .unwrap();
    drop(db);
    let manifest = fs::read_dir(state.join("project-quota"))
        .unwrap()
        .flatten()
        .map(|entry| entry.path())
        .find(|path| {
            path.file_name()
                .is_some_and(|name| name.to_string_lossy().starts_with("run-"))
        })
        .unwrap();
    let manifest_before = fs::read(&manifest).unwrap();
    let registry_path = fixture.join(".agcoord-project-quota-fixture.json");
    let registry_before = fs::read(&registry_path).unwrap();

    let refused = run(&[
        "serve",
        "--state-dir",
        state_argument(&state),
        "--capacity",
        "jobs=1",
        "--capacity",
        "disk=8388608",
        "--capacity",
        "disk_inodes=64",
        "--project-quota-fixture",
        fixture_argument,
    ]);
    assert!(!refused.status.success());
    assert_eq!(
        serde_json::from_slice::<Value>(&refused.stderr).unwrap()["code"],
        "broker-row-invalid"
    );
    assert_eq!(fs::read(&manifest).unwrap(), manifest_before);
    assert_eq!(fs::read(&registry_path).unwrap(), registry_before);
    let db = Connection::open(&database).unwrap();
    assert_eq!(
        db.query_row(
            "SELECT resource_state_json FROM runs WHERE run_id = ?1",
            params![submission.run_id],
            |row| row.get::<_, String>(0),
        )
        .unwrap(),
        changed_state
    );
    db.execute(
        "UPDATE runs SET resource_state_json = ?1 WHERE run_id = ?2",
        params![original_state, submission.run_id],
    )
    .unwrap();
    drop(db);
    let mut replacement = RunningBroker::start_with_options(
        &state,
        &[("jobs", 1), ("disk", 8 * 1024 * 1024), ("disk_inodes", 64)],
        &["--project-quota-fixture", fixture_argument],
    );
    wait_status(&state, submission.run_id, "interrupted");
    assert!(!marker.exists());
    assert!(!manifest.exists());
    assert!(replacement.terminate().success());
}

#[test]
fn project_quota_cleanup_refuses_a_replaced_tree_without_removing_it() {
    let temporary = TestDirectory::new("project-quota-tree-reuse");
    let fixture = temporary.path().join("fixture");
    let state = fixture.join("state");
    let checkout = temporary.path().join("checkout");
    let entered = temporary.path().join("entered.path");
    let release = temporary.path().join("release");
    fs::create_dir_all(&state).unwrap();
    fs::create_dir(&checkout).unwrap();
    write_project_quota_config(&state, "required");
    let mut broker = RunningBroker::start_with_options(
        &state,
        &[("jobs", 1), ("disk", 8 * 1024 * 1024), ("disk_inodes", 64)],
        &["--project-quota-fixture", fixture.to_str().unwrap()],
    );
    let submission = Submission {
        run_id: "project-quota-tree-reuse",
        kind: "check",
        repository: "repo-a",
        checkout: &checkout,
        command: vec![
            "/usr/bin/python3".to_owned(),
            "-c".to_owned(),
            "import os,pathlib,sys,time; target=pathlib.Path(os.environ['TMPDIR']); (target/'owned').write_text('owned'); pathlib.Path(sys.argv[1]).write_text(str(target)); release=pathlib.Path(sys.argv[2]);\nwhile not release.exists(): time.sleep(.01)".to_owned(),
            entered.to_str().unwrap().to_owned(),
            release.to_str().unwrap().to_owned(),
        ],
        gate_run_id: None,
    };
    assert!(
        submit_with_resources(
            &state,
            &submission,
            &[("disk", 8 * 1024 * 1024), ("disk_inodes", 64)],
        )
        .status
        .success()
    );
    let target = PathBuf::from(
        String::from_utf8(wait_for_nonempty_file(&entered, Duration::from_secs(5))).unwrap(),
    );
    fs::remove_dir_all(&target).unwrap();
    fs::create_dir(&target).unwrap();
    fs::set_permissions(&target, fs::Permissions::from_mode(0o700)).unwrap();
    let replacement = target.join("replacement");
    fs::write(&replacement, "must survive").unwrap();
    fs::write(&release, "release").unwrap();
    let passed = wait_status(&state, submission.run_id, "passed");
    assert!(target.is_dir());
    assert_eq!(fs::read_to_string(&replacement).unwrap(), "must survive");
    assert!(
        passed["resource_receipt"]["events"]
            .as_array()
            .unwrap()
            .iter()
            .any(|event| event["code"] == "quota-tree-reused" && event["status"] == "failed")
    );
    assert!(broker.terminate().success());
}

#[test]
fn real_ext4_project_quota_enforces_bytes_inodes_and_parallel_identity() {
    if std::env::var_os("AGCOORD_TEST_PROJECT_QUOTA").as_deref() != Some(std::ffi::OsStr::new("1"))
    {
        return;
    }
    for command in ["mkfs.ext4", "mount", "umount"] {
        assert!(
            Command::new("/usr/bin/env")
                .args(["sh", "-c", &format!("command -v {command}")])
                .stdout(Stdio::null())
                .status()
                .unwrap()
                .success(),
            "{command} is required for the real quota test"
        );
    }
    let temporary = TestDirectory::new("real-project-quota");
    let image = temporary.path().join("project-quota.ext4");
    let mountpoint = temporary.path().join("mounted");
    let checkout = temporary.path().join("checkout");
    fs::create_dir(&mountpoint).unwrap();
    fs::create_dir(&checkout).unwrap();
    File::create(&image)
        .unwrap()
        .set_len(128 * 1024 * 1024)
        .unwrap();
    assert!(
        Command::new("mkfs.ext4")
            .args(["-q", "-F", "-O", "quota,project", "-Q", "prjquota"])
            .arg(&image)
            .status()
            .unwrap()
            .success()
    );
    assert!(
        Command::new("mount")
            .args(["-o", "loop,prjquota"])
            .arg(&image)
            .arg(&mountpoint)
            .status()
            .unwrap()
            .success()
    );
    let _mount = MountGuard(mountpoint.clone());
    let state = mountpoint.join("state");
    fs::create_dir(&state).unwrap();
    write_project_quota_config(&state, "required");
    let mut broker = RunningBroker::start(
        &state,
        &[
            ("jobs", 2),
            ("disk", 16 * 1024 * 1024),
            ("disk_inodes", 256),
        ],
    );
    assert_eq!(
        snapshot(&state).unwrap()["resource_capabilities"]["project-quota"]["available"],
        true
    );

    let byte_report = temporary.path().join("byte-report.json");
    let byte_submission = Submission {
        run_id: "real-project-quota-bytes",
        kind: "check",
        repository: "repo-a",
        checkout: &checkout,
        command: vec![
            "/usr/bin/python3".to_owned(),
            "-c".to_owned(),
            r#"import errno,json,os,pathlib,sys
target=pathlib.Path(os.environ['TMPDIR'])
fd=os.open(target/'payload', os.O_WRONLY|os.O_CREAT|os.O_EXCL, 0o600)
written=0
limited=False
try:
    while True:
        try: written += os.write(fd, b'x'*4096)
        except OSError as exc:
            if exc.errno not in (errno.EDQUOT, errno.ENOSPC): raise
            limited=True
            break
finally: os.close(fd)
pathlib.Path(sys.argv[1]).write_text(json.dumps({'limited':limited,'target':str(target),'written':written}))"#.to_owned(),
            byte_report.to_str().unwrap().to_owned(),
        ],
        gate_run_id: None,
    };
    assert!(
        submit_with_resources(
            &state,
            &byte_submission,
            &[("disk", 2 * 1024 * 1024), ("disk_inodes", 128)],
        )
        .status
        .success()
    );
    let byte_finished = wait_status(&state, byte_submission.run_id, "passed");
    let byte_observed: Value = serde_json::from_slice(&fs::read(&byte_report).unwrap()).unwrap();
    assert_eq!(byte_observed["limited"], true);
    assert!(byte_observed["written"].as_u64().unwrap() > 0);
    assert!(byte_observed["written"].as_u64().unwrap() <= 2 * 1024 * 1024);
    assert!(!Path::new(byte_observed["target"].as_str().unwrap()).exists());
    assert!(
        byte_finished["resource_receipt"]["events"]
            .as_array()
            .unwrap()
            .iter()
            .any(|event| event["code"] == "storage-byte-limit-hit")
    );

    let inode_report = temporary.path().join("inode-report.json");
    let inode_submission = Submission {
        run_id: "real-project-quota-inodes",
        kind: "check",
        repository: "repo-a",
        checkout: &checkout,
        command: vec![
            "/usr/bin/python3".to_owned(),
            "-c".to_owned(),
            r#"import errno,json,os,pathlib,sys
target=pathlib.Path(os.environ['TMPDIR'])
created=0
limited=False
while True:
    try:
        (target/f'item-{created}').touch(exist_ok=False)
        created += 1
    except OSError as exc:
        if exc.errno not in (errno.EDQUOT, errno.ENOSPC): raise
        limited=True
        break
pathlib.Path(sys.argv[1]).write_text(json.dumps({'created':created,'limited':limited,'target':str(target)}))"#.to_owned(),
            inode_report.to_str().unwrap().to_owned(),
        ],
        gate_run_id: None,
    };
    assert!(
        submit_with_resources(
            &state,
            &inode_submission,
            &[("disk", 8 * 1024 * 1024), ("disk_inodes", 16)],
        )
        .status
        .success()
    );
    let inode_finished = wait_status(&state, inode_submission.run_id, "passed");
    let inode_observed: Value = serde_json::from_slice(&fs::read(&inode_report).unwrap()).unwrap();
    assert_eq!(inode_observed["limited"], true);
    assert!(inode_observed["created"].as_u64().unwrap() > 0);
    assert!(inode_observed["created"].as_u64().unwrap() < 16);
    assert!(!Path::new(inode_observed["target"].as_str().unwrap()).exists());
    assert!(
        inode_finished["resource_receipt"]["events"]
            .as_array()
            .unwrap()
            .iter()
            .any(|event| event["code"] == "storage-inode-limit-hit")
    );

    let parallel_script = "import fcntl,json,os,pathlib,struct,sys,time; target=pathlib.Path(os.environ['TMPDIR']); (target/'payload').write_bytes(b'x'*(1024*1024)); raw=bytearray(28); descriptor=os.open(target,os.O_RDONLY|os.O_DIRECTORY); fcntl.ioctl(descriptor,(2<<30)|(28<<16)|(ord('X')<<8)|31,raw,True); os.close(descriptor); project_id=struct.unpack_from('I',raw,12)[0]; pathlib.Path(sys.argv[1]).write_text(json.dumps({'target':str(target),'project_id':project_id})); release=pathlib.Path(sys.argv[2]);\nwhile not release.exists(): time.sleep(.01)";
    let mut parallel = Vec::new();
    for (name, repository) in [("real-quota-a", "repo-a"), ("real-quota-b", "repo-b")] {
        let report = temporary.path().join(format!("{name}.path"));
        let release = temporary.path().join(format!("{name}.release"));
        let submission = Submission {
            run_id: name,
            kind: "check",
            repository,
            checkout: &checkout,
            command: vec![
                "/usr/bin/python3".to_owned(),
                "-c".to_owned(),
                parallel_script.to_owned(),
                report.to_str().unwrap().to_owned(),
                release.to_str().unwrap().to_owned(),
            ],
            gate_run_id: None,
        };
        assert!(
            submit_with_resources(
                &state,
                &submission,
                &[("disk", 4 * 1024 * 1024), ("disk_inodes", 64)],
            )
            .status
            .success()
        );
        parallel.push((name, report, release));
    }
    let observations: Vec<Value> = parallel
        .iter()
        .map(|(_, report, _)| {
            serde_json::from_slice(&wait_for_nonempty_file(report, Duration::from_secs(10)))
                .unwrap()
        })
        .collect();
    let targets: Vec<_> = observations
        .iter()
        .map(|observed| PathBuf::from(observed["target"].as_str().unwrap()))
        .collect();
    assert_ne!(targets[0], targets[1]);
    assert_ne!(observations[0]["project_id"], observations[1]["project_id"]);
    for (_, _, release) in &parallel {
        fs::write(release, "release").unwrap();
    }
    for (name, _, _) in &parallel {
        wait_status(&state, name, "passed");
    }
    assert!(targets.iter().all(|target| !target.exists()));
    assert!(broker.terminate().success());

    for crash_point in ["worker-identity-commit", "worker-setup-commit"] {
        let marker = temporary.path().join(format!("{crash_point}-must-not-run"));
        let mut crashing = RunningBroker::start_with_options(
            &state,
            &[("jobs", 1), ("disk", 8 * MIB), ("disk_inodes", 64)],
            &["--crash-after", crash_point],
        );
        let submission = Submission {
            run_id: crash_point,
            kind: "check",
            repository: "repo-a",
            checkout: &checkout,
            command: touch_command(&marker),
            gate_run_id: None,
        };
        assert!(
            submit_with_resources(
                &state,
                &submission,
                &[("disk", 8 * MIB), ("disk_inodes", 64)],
            )
            .status
            .success()
        );
        assert_eq!(crashing.wait().code(), Some(86));
        thread::sleep(Duration::from_millis(100));
        assert!(!marker.exists());
        let mut replacement = RunningBroker::start(
            &state,
            &[("jobs", 1), ("disk", 8 * MIB), ("disk_inodes", 64)],
        );
        let interrupted = wait_status(&state, crash_point, "interrupted");
        assert_eq!(interrupted["failure_reason"], "worker-result-lost");
        assert!(!marker.exists());
        assert!(
            fs::read_dir(state.join("project-quota").join("runs"))
                .unwrap()
                .next()
                .is_none()
        );
        assert!(replacement.terminate().success());
    }

    let crash_entered = temporary.path().join("crash-entered.path");
    let crash_release = temporary.path().join("crash-release");
    let mut crashing = RunningBroker::start_with_options(
        &state,
        &[("jobs", 1), ("disk", 8 * MIB), ("disk_inodes", 64)],
        &["--crash-after", "worker-release"],
    );
    let crash_submission = Submission {
        run_id: "real-project-quota-recovery",
        kind: "check",
        repository: "repo-a",
        checkout: &checkout,
        command: vec![
            "/usr/bin/python3".to_owned(),
            "-c".to_owned(),
            "import os,pathlib,sys,time; target=pathlib.Path(os.environ['TMPDIR']); (target/'survives').write_bytes(b'x'*8192); pathlib.Path(sys.argv[1]).write_text(str(target)); release=pathlib.Path(sys.argv[2]);\nwhile not release.exists(): time.sleep(.01)".to_owned(),
            crash_entered.to_str().unwrap().to_owned(),
            crash_release.to_str().unwrap().to_owned(),
        ],
        gate_run_id: None,
    };
    assert!(
        submit_with_resources(
            &state,
            &crash_submission,
            &[("disk", 8 * MIB), ("disk_inodes", 64)],
        )
        .status
        .success()
    );
    assert_eq!(crashing.wait().code(), Some(86));
    let crash_target = PathBuf::from(
        String::from_utf8(wait_for_nonempty_file(
            &crash_entered,
            Duration::from_secs(5),
        ))
        .unwrap(),
    );
    let original_pid = status(&state, crash_submission.run_id)["worker_pid"].clone();
    let mut replacement = RunningBroker::start(
        &state,
        &[("jobs", 1), ("disk", 8 * MIB), ("disk_inodes", 64)],
    );
    assert_eq!(
        status(&state, crash_submission.run_id)["worker_pid"],
        original_pid
    );
    fs::write(&crash_release, "release").unwrap();
    let interrupted = wait_status(&state, crash_submission.run_id, "interrupted");
    assert_eq!(interrupted["failure_reason"], "worker-result-lost");
    assert!(
        interrupted["resource_receipt"]["peak"]["disk"]
            .as_u64()
            .unwrap()
            >= 8192
    );
    assert!(!crash_target.exists());
    assert!(replacement.terminate().success());
}

#[test]
fn cgroup_fixture_refuses_impossible_and_ambiguous_resource_contracts_stably() {
    let temporary = TestDirectory::new("cgroup-refusal-codes");
    let checkout = temporary.path().join("checkout");
    fs::create_dir(&checkout).unwrap();
    let cases = vec![
        (
            "memory-order",
            json!({
                "ram": {"backend":"cgroup-v2", "kind":"memory", "mode":"required", "unit":"bytes"},
                "soft": {"backend":"cgroup-v2", "kind":"memory-high", "mode":"required", "unit":"bytes"}
            }),
            vec![("ram", 4096), ("soft", 8192)],
            "memory-limit-impossible",
        ),
        (
            "tmpfs-incomplete",
            json!({
                "ram": {"backend":"cgroup-v2", "kind":"memory", "mode":"required", "unit":"bytes"},
                "scratch": {"backend":"cgroup-v2", "kind":"tmpfs", "mode":"required", "unit":"bytes"}
            }),
            vec![("ram", 16 * 1024 * 1024), ("scratch", 4 * 1024 * 1024)],
            "tmpfs-policy-incomplete",
        ),
        (
            "tmpfs-subpage",
            json!({
                "ram": {"backend":"cgroup-v2", "kind":"memory", "mode":"required", "unit":"bytes"},
                "scratch": {"backend":"cgroup-v2", "kind":"tmpfs", "mode":"required", "unit":"bytes"},
                "inodes": {"backend":"cgroup-v2", "kind":"inodes", "mode":"required", "unit":"inodes"}
            }),
            vec![("ram", 16 * 1024 * 1024), ("scratch", 1), ("inodes", 8)],
            "tmpfs-size-impossible",
        ),
        (
            "tmpfs-memory",
            json!({
                "ram": {"backend":"cgroup-v2", "kind":"memory", "mode":"required", "unit":"bytes"},
                "scratch": {"backend":"cgroup-v2", "kind":"tmpfs", "mode":"required", "unit":"bytes"},
                "inodes": {"backend":"cgroup-v2", "kind":"inodes", "mode":"required", "unit":"inodes"}
            }),
            vec![
                ("ram", 4 * 1024 * 1024),
                ("scratch", 8 * 1024 * 1024),
                ("inodes", 8),
            ],
            "tmpfs-memory-impossible",
        ),
        (
            "io-unconfigured",
            json!({
                "bandwidth": {"backend":"cgroup-v2", "kind":"io-bandwidth", "mode":"required", "unit":"bytes-per-second"}
            }),
            vec![("bandwidth", 1_000_000)],
            "io-path-unconfigured",
        ),
        (
            "controller-ambiguous",
            json!({
                "ram_a": {"backend":"cgroup-v2", "kind":"memory", "mode":"required", "unit":"bytes"},
                "ram_b": {"backend":"cgroup-v2", "kind":"memory", "mode":"required", "unit":"bytes"}
            }),
            vec![("ram_a", 4096), ("ram_b", 4096)],
            "controller-ambiguous",
        ),
    ];
    for (name, bindings, requested, expected) in cases {
        let state = temporary.path().join(format!("state-{name}"));
        let root = temporary.path().join(format!("root-{name}"));
        let marker = temporary.path().join(format!("must-not-run-{name}"));
        fs::create_dir(&state).unwrap();
        fs::create_dir(&root).unwrap();
        fs::write(
            state.join("config.json"),
            serde_json::to_vec(&json!({"bindings": bindings, "cgroup_root": &root})).unwrap(),
        )
        .unwrap();
        let mut capacities = vec![("jobs", 1)];
        capacities.extend(requested.iter().copied());
        let mut broker = RunningBroker::start_with_options(
            &state,
            &capacities,
            &["--cgroup-fixture", root.to_str().unwrap()],
        );
        let submission = Submission {
            run_id: name,
            kind: "check",
            repository: "repo-a",
            checkout: &checkout,
            command: touch_command(&marker),
            gate_run_id: None,
        };
        assert!(
            submit_with_resources(&state, &submission, &requested)
                .status
                .success()
        );
        let failed = wait_status(&state, name, "failed");
        assert_eq!(failed["failure_reason"], "resource-enforcement-failed");
        assert!(
            failed["resource_receipt"]["events"]
                .as_array()
                .unwrap()
                .iter()
                .any(|event| event["code"] == expected),
            "missing {expected} for {name}"
        );
        assert!(!marker.exists());
        assert!(
            !fs::read_dir(&root)
                .unwrap()
                .flatten()
                .any(|entry| entry.file_type().unwrap().is_dir())
        );
        assert!(broker.terminate().success());
    }
}

#[test]
fn cgroup_fixture_refuses_unreadable_initial_metrics_before_user_code() {
    let temporary = TestDirectory::new("cgroup-initial-metric-refusals");
    let checkout = temporary.path().join("checkout");
    fs::create_dir(&checkout).unwrap();

    let cases = [
        (
            "cpu-stat",
            json!({
                "cpu": {"backend":"cgroup-v2", "kind":"cpu", "mode":"required", "unit":"logical-cpu"}
            }),
            vec![("cpu", 1)],
            None,
            json!({"cpu.stat": "broken\n"}),
            "cpu-stat-invalid",
        ),
        (
            "memory-pressure",
            json!({
                "ram-soft": {"backend":"cgroup-v2", "kind":"memory-high", "mode":"required", "unit":"bytes"}
            }),
            vec![("ram-soft", 64 * 1024 * 1024)],
            None,
            json!({"memory.pressure": "broken\n"}),
            "memory-pressure-invalid",
        ),
        (
            "io-stat",
            json!({
                "read-rate": {"backend":"cgroup-v2", "kind":"io-bandwidth", "mode":"required", "unit":"read-bytes-per-second"}
            }),
            vec![("read-rate", 1024 * 1024)],
            Some("scratch"),
            json!({"io.stat": "broken\n"}),
            "io-stat-invalid",
        ),
        (
            "missing-memory-current",
            json!({
                "ram": {"backend":"cgroup-v2", "kind":"memory", "mode":"required", "unit":"bytes"}
            }),
            vec![("ram", 64 * 1024 * 1024)],
            None,
            json!({"memory.current": null}),
            "controller-file-missing",
        ),
    ];

    for (name, bindings, requested, io_directory, faults, code) in cases {
        let state = temporary.path().join(format!("state-{name}"));
        let root = temporary.path().join(format!("delegated-{name}"));
        let marker = temporary.path().join(format!("must-not-run-{name}"));
        fs::create_dir(&state).unwrap();
        fs::create_dir(&root).unwrap();
        let io_path = io_directory.map(|directory| {
            let path = temporary.path().join(format!("{directory}-{name}"));
            fs::create_dir(&path).unwrap();
            path
        });
        let mut configuration = json!({"bindings": bindings, "cgroup_root": root});
        if let Some(io_path) = &io_path {
            configuration["cgroup_io"] = json!({"paths": [io_path]});
        }
        fs::write(
            state.join("config.json"),
            serde_json::to_vec(&configuration).unwrap(),
        )
        .unwrap();
        fs::write(
            root.join(".agcoord-fixture-leaf-faults.json"),
            serde_json::to_vec(&faults).unwrap(),
        )
        .unwrap();
        let mut capacities = vec![("jobs", 1)];
        capacities.extend(requested.iter().copied());
        let mut broker = RunningBroker::start_with_options(
            &state,
            &capacities,
            &["--cgroup-fixture", root.to_str().unwrap()],
        );
        let submission = Submission {
            run_id: name,
            kind: "check",
            repository: "repo-a",
            checkout: &checkout,
            command: touch_command(&marker),
            gate_run_id: None,
        };
        assert!(
            submit_with_resources(&state, &submission, &requested)
                .status
                .success()
        );
        let failed = wait_status(&state, submission.run_id, "failed");
        assert_eq!(failed["exit_status"], 125);
        assert_eq!(failed["failure_reason"], "resource-enforcement-failed");
        assert!(
            failed["resource_receipt"]["events"]
                .as_array()
                .unwrap()
                .iter()
                .any(|event| event["stage"] == "prepare"
                    && event["status"] == "failed"
                    && event["code"] == code),
            "{name}: {failed}"
        );
        assert!(!marker.exists());
        assert!(
            !fs::read_dir(&root)
                .unwrap()
                .any(|entry| entry.unwrap().path().is_dir())
        );
        assert!(broker.terminate().success());
    }
}

#[test]
fn typed_cgroup_fixture_applies_controls_before_release_and_cleans_owned_leaf() {
    let temporary = TestDirectory::new("typed-cgroup-fixture");
    let state = temporary.path().join("state");
    let root = temporary.path().join("delegated");
    let checkout = temporary.path().join("checkout");
    let entered = temporary.path().join("entered");
    let release = temporary.path().join("release");
    fs::create_dir(&state).unwrap();
    fs::create_dir(&root).unwrap();
    fs::create_dir(&checkout).unwrap();
    fs::write(
        state.join("config.json"),
        serde_json::to_vec(&json!({
            "bindings": {
                "cpu": {
                    "backend": "cgroup-v2",
                    "kind": "cpu",
                    "mode": "required",
                    "unit": "logical-cpu"
                },
                "pids": {
                    "backend": "cgroup-v2",
                    "kind": "processes",
                    "mode": "required",
                    "unit": "processes"
                }
            },
            "cgroup_root": root,
        }))
        .unwrap(),
    )
    .unwrap();
    let root_argument = root.to_str().unwrap();
    let mut broker = RunningBroker::start_with_options(
        &state,
        &[("jobs", 1), ("cpu", 2), ("pids", 8)],
        &["--cgroup-fixture", root_argument],
    );
    let machine = snapshot(&state).unwrap();
    assert_eq!(
        machine["resource_capabilities"]["cgroup-v2"]["available"],
        true
    );

    let submission = Submission {
        run_id: "typed-cgroup-fixture",
        kind: "check",
        repository: "repo-a",
        checkout: &checkout,
        command: blocking_command(&entered, &release, None),
        gate_run_id: None,
    };
    let submitted = submit_with_resources(&state, &submission, &[("cpu", 2), ("pids", 8)]);
    assert!(submitted.status.success());
    wait_for(Duration::from_secs(5), || entered.exists());
    let running = status(&state, submission.run_id);
    assert_eq!(
        running["resource_receipt"]["applied"],
        json!({"cpu": 2, "pids": 8})
    );
    let worker_pid = running["worker_pid"].as_u64().unwrap();
    let owner = fs::read_dir(&root)
        .unwrap()
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .find(|path| {
            path.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("agcoord-u")
        })
        .unwrap();
    let leaf = fs::read_dir(&owner)
        .unwrap()
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .find(|path| {
            path.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("run-")
        })
        .unwrap();
    assert_eq!(
        fs::read_to_string(leaf.join("cpu.max")).unwrap(),
        "200000 100000\n"
    );
    assert_eq!(fs::read_to_string(leaf.join("pids.max")).unwrap(), "8\n");
    assert!(
        fs::read_to_string(leaf.join("cgroup.procs"))
            .unwrap()
            .lines()
            .any(|line| line == worker_pid.to_string())
    );

    fs::write(&release, "release").unwrap();
    let passed = wait_status(&state, submission.run_id, "passed");
    let codes: BTreeSet<_> = passed["resource_receipt"]["events"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|event| event["code"].as_str())
        .collect();
    assert!(
        ["prepared", "applied", "cleaned"]
            .into_iter()
            .all(|code| codes.contains(code))
    );
    assert!(fs::read_dir(&root).unwrap().next().is_none());
    assert!(broker.terminate().success());
}

#[test]
fn cgroup_fixture_cancellation_kills_descendants_and_cleans_the_exact_leaf() {
    let temporary = TestDirectory::new("cgroup-tree-cancel");
    let state = temporary.path().join("state");
    let root = temporary.path().join("delegated");
    let checkout = temporary.path().join("checkout");
    let child_pid = temporary.path().join("child.pid");
    for path in [&state, &root, &checkout] {
        fs::create_dir(path).unwrap();
    }
    fs::write(
        state.join("config.json"),
        serde_json::to_vec(&json!({
            "bindings": {
                "cgroup_slot": {"backend":"cgroup-v2", "kind":"generic", "mode":"required", "unit":"admission-unit"}
            },
            "cgroup_root": root,
        }))
        .unwrap(),
    )
    .unwrap();
    let mut broker = RunningBroker::start_with_options(
        &state,
        &[("jobs", 1), ("cgroup_slot", 1)],
        &["--cgroup-fixture", root.to_str().unwrap()],
    );
    let script = r#"
import os
from pathlib import Path
import sys
import time

first = os.fork()
if first == 0:
    os.setsid()
    detached = os.fork()
    if detached == 0:
        target = Path(sys.argv[1])
        pending = target.with_name(target.name + '.pending')
        pending.write_text(str(os.getpid()), encoding='ascii')
        pending.replace(target)
        while True:
            time.sleep(1)
    time.sleep(0.5)
    os._exit(0)
while True:
    time.sleep(1)
"#;
    let submission = Submission {
        run_id: "cgroup-tree-cancel",
        kind: "check",
        repository: "repo-a",
        checkout: &checkout,
        command: vec![
            "/usr/bin/python3".to_owned(),
            "-c".to_owned(),
            script.to_owned(),
            child_pid.to_string_lossy().into_owned(),
        ],
        gate_run_id: None,
    };
    assert!(
        submit_with_resources(&state, &submission, &[("cgroup_slot", 1)])
            .status
            .success()
    );
    wait_for(Duration::from_secs(5), || child_pid.exists());
    let descendant: u32 = fs::read_to_string(&child_pid)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    let mut guard = ProcessGuard::new(descendant);
    assert!(
        run(&[
            "cancel",
            "--state-dir",
            state_argument(&state),
            "--run-id",
            submission.run_id,
        ])
        .status
        .success()
    );
    let cancelled = wait_status(&state, submission.run_id, "cancelled");
    wait_for(Duration::from_secs(5), || {
        process_state(u64::from(descendant)).is_none_or(|status| status == "Z")
    });
    guard.disarm();
    let codes: BTreeSet<_> = cancelled["resource_receipt"]["events"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|event| event["code"].as_str())
        .collect();
    assert!(
        ["cancelled", "cleaned"]
            .into_iter()
            .all(|code| codes.contains(code))
    );
    assert!(fs::read_dir(&root).unwrap().next().is_none());
    assert!(broker.terminate().success());
}

#[test]
fn replacement_recovers_a_live_cgroup_worker_and_cleans_its_durable_leaf() {
    let temporary = TestDirectory::new("cgroup-crash-recovery");
    let state = temporary.path().join("state");
    let root = temporary.path().join("delegated");
    let checkout = temporary.path().join("checkout");
    let entered = temporary.path().join("entered");
    let release = temporary.path().join("release");
    for path in [&state, &root, &checkout] {
        fs::create_dir(path).unwrap();
    }
    fs::write(
        state.join("config.json"),
        serde_json::to_vec(&json!({
            "bindings": {
                "cgroup_slot": {"backend":"cgroup-v2", "kind":"generic", "mode":"required", "unit":"admission-unit"}
            },
            "cgroup_root": root,
        }))
        .unwrap(),
    )
    .unwrap();
    let root_argument = root.to_str().unwrap();
    let mut crashing = RunningBroker::start_with_options(
        &state,
        &[("jobs", 1), ("cgroup_slot", 1)],
        &[
            "--cgroup-fixture",
            root_argument,
            "--crash-after",
            "worker-release",
        ],
    );
    let submission = Submission {
        run_id: "cgroup-crash-recovery",
        kind: "check",
        repository: "repo-a",
        checkout: &checkout,
        command: blocking_command(&entered, &release, None),
        gate_run_id: None,
    };
    assert!(
        submit_with_resources(&state, &submission, &[("cgroup_slot", 1)])
            .status
            .success()
    );
    assert_eq!(crashing.wait().code(), Some(86));
    wait_for(Duration::from_secs(5), || entered.exists());
    let original_pid = status(&state, submission.run_id)["worker_pid"].clone();
    let mut replacement = RunningBroker::start_with_options(
        &state,
        &[("jobs", 1), ("cgroup_slot", 1)],
        &["--cgroup-fixture", root_argument],
    );
    assert_eq!(
        status(&state, submission.run_id)["worker_pid"],
        original_pid
    );
    fs::write(&release, "release").unwrap();
    let interrupted = wait_status(&state, submission.run_id, "interrupted");
    assert_eq!(interrupted["failure_reason"], "worker-result-lost");
    let codes: BTreeSet<_> = interrupted["resource_receipt"]["events"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|event| event["code"].as_str())
        .collect();
    assert!(
        ["finished", "cleaned"]
            .into_iter()
            .all(|code| codes.contains(code))
    );
    assert!(fs::read_dir(&root).unwrap().next().is_none());
    assert!(replacement.terminate().success());
}

#[test]
fn replacement_refuses_a_cgroup_handle_that_no_longer_matches_its_manifest() {
    let temporary = TestDirectory::new("cgroup-corrupt-recovery-handle");
    let state = temporary.path().join("state");
    let root = temporary.path().join("delegated");
    let checkout = temporary.path().join("checkout");
    let marker = temporary.path().join("must-not-run");
    for path in [&state, &root, &checkout] {
        fs::create_dir(path).unwrap();
    }
    fs::write(
        state.join("config.json"),
        serde_json::to_vec(&json!({
            "bindings": {
                "cgroup_slot": {"backend":"cgroup-v2", "kind":"generic", "mode":"required", "unit":"admission-unit"}
            },
            "cgroup_root": root,
        }))
        .unwrap(),
    )
    .unwrap();
    let root_argument = root.to_str().unwrap();
    let mut crashing = RunningBroker::start_with_options(
        &state,
        &[("jobs", 1), ("cgroup_slot", 1)],
        &[
            "--cgroup-fixture",
            root_argument,
            "--crash-after",
            "worker-identity-commit",
        ],
    );
    let submission = Submission {
        run_id: "cgroup-corrupt-recovery-handle",
        kind: "check",
        repository: "repo-a",
        checkout: &checkout,
        command: touch_command(&marker),
        gate_run_id: None,
    };
    assert!(
        submit_with_resources(&state, &submission, &[("cgroup_slot", 1)])
            .status
            .success()
    );
    assert_eq!(crashing.wait().code(), Some(86));
    let database = state.join("queue.sqlite3");
    let db = Connection::open(&database).unwrap();
    let original: String = db
        .query_row(
            "SELECT resource_state_json FROM runs WHERE run_id = 'cgroup-corrupt-recovery-handle'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let mut corrupted: Value = serde_json::from_str(&original).unwrap();
    corrupted["cgroup-v2"]["handle"]["token"] = "00000000000000000000000000000000".into();
    let corrupted = serde_json::to_string(&corrupted).unwrap();
    db.execute(
        "UPDATE runs SET resource_state_json = ?1 WHERE run_id = 'cgroup-corrupt-recovery-handle'",
        params![corrupted],
    )
    .unwrap();
    drop(db);

    let refused = run(&[
        "serve",
        "--state-dir",
        state_argument(&state),
        "--capacity",
        "jobs=1",
        "--capacity",
        "cgroup_slot=1",
        "--cgroup-fixture",
        root_argument,
        "--idle-timeout",
        "0.05",
    ]);
    assert!(!refused.status.success());
    assert_eq!(
        serde_json::from_slice::<Value>(&refused.stderr).unwrap()["code"],
        "broker-row-invalid"
    );
    let db = Connection::open(&database).unwrap();
    assert_eq!(
        db.query_row(
            "SELECT resource_state_json FROM runs WHERE run_id = 'cgroup-corrupt-recovery-handle'",
            [],
            |row| row.get::<_, String>(0),
        )
        .unwrap(),
        corrupted
    );
    let mut non_ascii_leaf: Value = serde_json::from_str(&original).unwrap();
    non_ascii_leaf["cgroup-v2"]["handle"]["leaf"] = "run-aaaaaaaaaaaaaaaéaaaaaaaaaaaa".into();
    let non_ascii_leaf = serde_json::to_string(&non_ascii_leaf).unwrap();
    db.execute(
        "UPDATE runs SET resource_state_json = ?1 WHERE run_id = 'cgroup-corrupt-recovery-handle'",
        params![non_ascii_leaf],
    )
    .unwrap();
    drop(db);

    let refused = run(&[
        "serve",
        "--state-dir",
        state_argument(&state),
        "--capacity",
        "jobs=1",
        "--capacity",
        "cgroup_slot=1",
        "--cgroup-fixture",
        root_argument,
        "--idle-timeout",
        "0.05",
    ]);
    assert!(!refused.status.success());
    assert_eq!(
        serde_json::from_slice::<Value>(&refused.stderr).unwrap()["code"],
        "broker-row-invalid"
    );
    let db = Connection::open(&database).unwrap();
    assert_eq!(
        db.query_row(
            "SELECT resource_state_json FROM runs WHERE run_id = 'cgroup-corrupt-recovery-handle'",
            [],
            |row| row.get::<_, String>(0),
        )
        .unwrap(),
        non_ascii_leaf
    );
    db.execute(
        "UPDATE runs SET resource_state_json = ?1 WHERE run_id = 'cgroup-corrupt-recovery-handle'",
        params![original],
    )
    .unwrap();
    drop(db);

    let mut replacement = RunningBroker::start_with_options(
        &state,
        &[("jobs", 1), ("cgroup_slot", 1)],
        &["--cgroup-fixture", root_argument],
    );
    wait_status(&state, submission.run_id, "interrupted");
    assert!(!marker.exists());
    assert!(fs::read_dir(&root).unwrap().next().is_none());
    assert!(replacement.terminate().success());
}

#[test]
fn real_cgroup_delegation_hides_parent_controls_from_the_worker() {
    let Some(root) = real_cgroup_root() else {
        return;
    };
    let temporary = TestDirectory::new("real-cgroup-namespace");
    let state = temporary.path().join("state");
    let checkout = temporary.path().join("checkout");
    let report = temporary.path().join("namespace.json");
    fs::create_dir(&state).unwrap();
    fs::create_dir(&checkout).unwrap();
    fs::write(
        state.join("config.json"),
        serde_json::to_vec(&json!({
            "bindings": {
                "cgroup_slot": {"backend":"cgroup-v2", "kind":"generic", "mode":"required", "unit":"admission-unit"}
            },
            "cgroup_root": root,
        }))
        .unwrap(),
    )
    .unwrap();
    let mut broker = RunningBroker::start(&state, &[("jobs", 1), ("cgroup_slot", 1)]);
    assert_eq!(
        snapshot(&state).unwrap()["resource_capabilities"]["cgroup-v2"]["available"],
        true
    );
    let script = r#"
import errno
import json
import os
from pathlib import Path
import sys

mounts = []
for line in Path('/proc/self/mountinfo').read_text(encoding='ascii').splitlines():
    left, separator, right = line.partition(' - ')
    if separator and right.split()[0] == 'cgroup2':
        mounts.append(Path(left.split()[4]))
if not mounts:
    raise AssertionError('isolated worker has no cgroup2 mount')
protected = []
visible_parents = []
for mount in mounts:
    visible_parents.extend(path.name for path in mount.iterdir() if path.name.startswith('agcoord-u'))
    try:
        descriptor = os.open(mount / 'cgroup.kill', os.O_WRONLY | os.O_CLOEXEC)
        try:
            os.write(descriptor, b'1\n')
        finally:
            os.close(descriptor)
    except OSError as exc:
        if exc.errno not in {errno.EACCES, errno.EPERM, errno.EROFS}:
            raise
        protected.append(str(mount))
    else:
        raise AssertionError('worker could rewrite its cgroup namespace root')
Path(sys.argv[1]).write_text(json.dumps({
    'cgroup': Path('/proc/self/cgroup').read_text(encoding='ascii'),
    'mounts': [str(path) for path in mounts],
    'protected': protected,
    'visible_parents': visible_parents,
}), encoding='ascii')
"#;
    let submission = Submission {
        run_id: "real-cgroup-namespace",
        kind: "check",
        repository: "repo-a",
        checkout: &checkout,
        command: vec![
            "/usr/bin/python3".to_owned(),
            "-c".to_owned(),
            script.to_owned(),
            report.to_string_lossy().into_owned(),
        ],
        gate_run_id: None,
    };
    assert!(
        submit_with_resources(&state, &submission, &[("cgroup_slot", 1)])
            .status
            .success()
    );
    let passed = wait_status(&state, submission.run_id, "passed");
    assert_eq!(
        passed["resource_receipt"]["applied"],
        json!({"cgroup_slot": 1})
    );
    let observed: Value = serde_json::from_slice(&fs::read(&report).unwrap()).unwrap();
    assert_eq!(observed["cgroup"], "0::/\n");
    assert_eq!(observed["visible_parents"], json!([]));
    assert_eq!(
        observed["protected"].as_array().unwrap().len(),
        observed["mounts"].as_array().unwrap().len()
    );
    assert_no_cgroup_owner(&root);
    assert!(broker.terminate().success());
}

#[test]
fn real_cgroup_cpu_and_pid_limits_cover_the_complete_process_tree() {
    let Some(root) = real_cgroup_root() else {
        return;
    };
    let temporary = TestDirectory::new("real-cgroup-compute");
    let state = temporary.path().join("state");
    let checkout = temporary.path().join("checkout");
    let report = temporary.path().join("compute.json");
    fs::create_dir(&state).unwrap();
    fs::create_dir(&checkout).unwrap();
    fs::write(
        state.join("config.json"),
        serde_json::to_vec(&json!({
            "bindings": {
                "cpu": {"backend":"cgroup-v2", "kind":"cpu", "mode":"required", "unit":"logical-cpu"},
                "pids": {"backend":"cgroup-v2", "kind":"processes", "mode":"required", "unit":"processes"}
            },
            "cgroup_root": root,
        }))
        .unwrap(),
    )
    .unwrap();
    let mut broker = RunningBroker::start(&state, &[("jobs", 1), ("cpu", 1), ("pids", 8)]);
    let script = r#"
import errno
import json
import os
from pathlib import Path
import subprocess
import sys
import time

busy = '''
import time
end = time.monotonic() + 1.0
value = 1
while time.monotonic() < end:
    value = (value * 48271) % 2147483647
'''
before = os.times()
started = time.monotonic()
workers = [subprocess.Popen([sys.executable, '-c', busy]) for _ in range(4)]
for worker in workers:
    worker.wait()
wall = time.monotonic() - started
after = os.times()
child_cpu = after.children_user + after.children_system - before.children_user - before.children_system
sleepers = []
exhausted = False
for _attempt in range(32):
    try:
        sleepers.append(subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)']))
    except OSError as exc:
        if exc.errno != errno.EAGAIN:
            raise
        exhausted = True
        break
try:
    Path(sys.argv[1]).write_text(json.dumps({
        'affinity': len(os.sched_getaffinity(0)),
        'child_cpu': child_cpu,
        'exhausted': exhausted,
        'sleepers': len(sleepers),
        'wall': wall,
    }), encoding='ascii')
finally:
    for sleeper in sleepers:
        sleeper.terminate()
    for sleeper in sleepers:
        sleeper.wait()
"#;
    let submission = Submission {
        run_id: "real-cgroup-compute",
        kind: "check",
        repository: "repo-a",
        checkout: &checkout,
        command: vec![
            "/usr/bin/python3".to_owned(),
            "-c".to_owned(),
            script.to_owned(),
            report.to_string_lossy().into_owned(),
        ],
        gate_run_id: None,
    };
    assert!(
        submit_with_resources(&state, &submission, &[("cpu", 1), ("pids", 8)])
            .status
            .success()
    );
    let passed = wait_status(&state, submission.run_id, "passed");
    let observed: Value = serde_json::from_slice(&fs::read(&report).unwrap()).unwrap();
    assert!(
        observed["child_cpu"].as_f64().unwrap() <= observed["wall"].as_f64().unwrap() * 1.5 + 0.1
    );
    assert_eq!(observed["exhausted"], true);
    assert!(observed["sleepers"].as_u64().unwrap() <= 7);
    let codes: BTreeSet<_> = passed["resource_receipt"]["events"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|event| event["code"].as_str())
        .collect();
    assert!(codes.contains("pids-limit-hit"));
    if observed["affinity"].as_u64().unwrap() > 1 {
        assert!(codes.contains("cpu-throttled"));
    }
    assert!((1..=8).contains(&passed["resource_receipt"]["peak"]["pids"].as_u64().unwrap()));
    assert_no_cgroup_owner(&root);
    assert!(broker.terminate().success());
}

#[test]
fn real_cgroup_memory_pressure_oom_and_swap_contracts_are_local() {
    let Some(root) = real_cgroup_root() else {
        return;
    };
    const MIB: u64 = 1024 * 1024;
    let temporary = TestDirectory::new("real-cgroup-memory");
    let state = temporary.path().join("state");
    let checkout = temporary.path().join("checkout");
    let swap_report = temporary.path().join("swap-limit");
    fs::create_dir(&state).unwrap();
    fs::create_dir(&checkout).unwrap();
    fs::write(
        state.join("config.json"),
        serde_json::to_vec(&json!({
            "bindings": {
                "ram": {"backend":"cgroup-v2", "kind":"memory", "mode":"required", "unit":"bytes"},
                "pressure": {"backend":"cgroup-v2", "kind":"memory-high", "mode":"required", "unit":"bytes"},
                "swap": {"backend":"cgroup-v2", "kind":"swap", "mode":"required", "unit":"bytes"}
            },
            "cgroup_root": root,
        }))
        .unwrap(),
    )
    .unwrap();
    let mut broker = RunningBroker::start(
        &state,
        &[
            ("jobs", 1),
            ("ram", 128 * MIB),
            ("pressure", 32 * MIB),
            ("swap", 16 * MIB),
        ],
    );
    let oom = Submission {
        run_id: "real-cgroup-oom",
        kind: "check",
        repository: "repo-a",
        checkout: &checkout,
        command: vec![
            "/usr/bin/python3".to_owned(),
            "-c".to_owned(),
            "blocks = []\nwhile True:\n block = bytearray(4 * 1024 * 1024)\n for offset in range(0, len(block), 4096): block[offset] = 1\n blocks.append(block)".to_owned(),
        ],
        gate_run_id: None,
    };
    assert!(
        submit_with_resources(&state, &oom, &[("ram", 64 * MIB)])
            .status
            .success()
    );
    let failed = wait_status(&state, oom.run_id, "failed");
    assert_eq!(failed["failure_reason"], "memory-oom");
    assert!(
        failed["resource_receipt"]["events"]
            .as_array()
            .unwrap()
            .iter()
            .any(|event| event["code"] == "memory-oom")
    );

    let pressure = Submission {
        run_id: "real-cgroup-pressure",
        kind: "check",
        repository: "repo-a",
        checkout: &checkout,
        command: vec![
            "/usr/bin/python3".to_owned(),
            "-c".to_owned(),
            "payload = bytearray(48 * 1024 * 1024)\nfor offset in range(0, len(payload), 4096): payload[offset] = 1".to_owned(),
        ],
        gate_run_id: None,
    };
    assert!(
        submit_with_resources(
            &state,
            &pressure,
            &[("pressure", 32 * MIB), ("ram", 128 * MIB)],
        )
        .status
        .success()
    );
    let pressured = wait_status(&state, pressure.run_id, "passed");
    let pressure_codes: BTreeSet<_> = pressured["resource_receipt"]["events"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|event| event["code"].as_str())
        .collect();
    assert!(
        ["memory-high-throttled", "memory-pressure"]
            .into_iter()
            .all(|code| pressure_codes.contains(code))
    );
    assert!(!pressure_codes.contains("memory-oom"));

    let swap = Submission {
        run_id: "real-cgroup-swap",
        kind: "check",
        repository: "repo-a",
        checkout: &checkout,
        command: vec![
            "/usr/bin/python3".to_owned(),
            "-c".to_owned(),
            "from pathlib import Path\nimport sys\nfor line in Path('/proc/self/mountinfo').read_text(encoding='ascii').splitlines():\n left, separator, right = line.partition(' - ')\n if separator and right.split()[0] == 'cgroup2':\n  Path(sys.argv[1]).write_text((Path(left.split()[4]) / 'memory.swap.max').read_text(encoding='ascii').strip(), encoding='ascii')\n  break\nelse:\n raise AssertionError('missing cgroup2 mount')".to_owned(),
            swap_report.to_string_lossy().into_owned(),
        ],
        gate_run_id: None,
    };
    assert!(
        submit_with_resources(&state, &swap, &[("swap", 16 * MIB)])
            .status
            .success()
    );
    let swap_total = fs::read_to_string("/proc/meminfo")
        .unwrap()
        .lines()
        .find(|line| line.starts_with("SwapTotal:"))
        .unwrap()
        .split_whitespace()
        .nth(1)
        .unwrap()
        .parse::<u64>()
        .unwrap();
    if swap_total == 0 {
        let refused = wait_status(&state, swap.run_id, "failed");
        assert_eq!(refused["failure_reason"], "resource-enforcement-failed");
        assert!(
            refused["resource_receipt"]["events"]
                .as_array()
                .unwrap()
                .iter()
                .any(|event| event["code"] == "swap-disabled")
        );
        assert!(!swap_report.exists());
    } else {
        let passed = wait_status(&state, swap.run_id, "passed");
        assert_eq!(passed["resource_receipt"]["applied"]["swap"], 16 * MIB);
        assert_eq!(
            fs::read_to_string(&swap_report).unwrap(),
            (16 * MIB).to_string()
        );
    }
    assert_no_cgroup_owner(&root);
    assert!(broker.terminate().success());
}

#[test]
fn cgroup_fixture_reports_conservative_compute_and_memory_peaks_and_events() {
    let temporary = TestDirectory::new("cgroup-compute-memory-receipt");
    let state = temporary.path().join("state");
    let root = temporary.path().join("delegated");
    let checkout = temporary.path().join("checkout");
    let entered = temporary.path().join("entered");
    let release = temporary.path().join("release");
    fs::create_dir(&state).unwrap();
    fs::create_dir(&root).unwrap();
    fs::create_dir(&checkout).unwrap();
    fs::write(
        state.join("config.json"),
        serde_json::to_vec(&json!({
            "bindings": {
                "cpu": {"backend":"cgroup-v2", "kind":"cpu", "mode":"required", "unit":"logical-cpu"},
                "pids": {"backend":"cgroup-v2", "kind":"processes", "mode":"required", "unit":"processes"},
                "ram": {"backend":"cgroup-v2", "kind":"memory", "mode":"required", "unit":"bytes"},
                "ram_soft": {"backend":"cgroup-v2", "kind":"memory-high", "mode":"required", "unit":"bytes"},
                "swap": {"backend":"cgroup-v2", "kind":"swap", "mode":"required", "unit":"bytes"}
            },
            "cgroup_root": root,
        }))
        .unwrap(),
    )
    .unwrap();
    let mut broker = RunningBroker::start_with_options(
        &state,
        &[
            ("jobs", 1),
            ("cpu", 2),
            ("pids", 8),
            ("ram", 4096),
            ("ram_soft", 2048),
            ("swap", 1024),
        ],
        &["--cgroup-fixture", root.to_str().unwrap()],
    );
    let submission = Submission {
        run_id: "cgroup-compute-memory-receipt",
        kind: "check",
        repository: "repo-a",
        checkout: &checkout,
        command: blocking_command(&entered, &release, None),
        gate_run_id: None,
    };
    assert!(
        submit_with_resources(
            &state,
            &submission,
            &[
                ("cpu", 2),
                ("pids", 8),
                ("ram", 4096),
                ("ram_soft", 2048),
                ("swap", 1024)
            ],
        )
        .status
        .success()
    );
    wait_for(Duration::from_secs(5), || entered.exists());
    let owner = fs::read_dir(&root)
        .unwrap()
        .flatten()
        .map(|entry| entry.path())
        .find(|path| path.is_dir())
        .unwrap();
    let leaf = fs::read_dir(&owner)
        .unwrap()
        .flatten()
        .map(|entry| entry.path())
        .find(|path| path.is_dir())
        .unwrap();
    fs::write(
        leaf.join("cpu.stat"),
        "usage_usec 100\nnr_throttled 1\nthrottled_usec 20\n",
    )
    .unwrap();
    fs::write(leaf.join("pids.current"), "3\n").unwrap();
    fs::write(leaf.join("pids.peak"), "5\n").unwrap();
    fs::write(leaf.join("pids.events"), "max 1\n").unwrap();
    fs::write(leaf.join("memory.current"), "100\n").unwrap();
    fs::write(leaf.join("memory.peak"), "200\n").unwrap();
    fs::write(
        leaf.join("memory.events"),
        "high 1\nmax 1\noom 1\noom_kill 1\noom_group_kill 0\n",
    )
    .unwrap();
    fs::write(
        leaf.join("memory.pressure"),
        "some avg10=0.01 avg60=0.00 avg300=0.00 total=5\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=0\n",
    )
    .unwrap();
    fs::write(leaf.join("memory.swap.current"), "20\n").unwrap();
    fs::write(leaf.join("memory.swap.peak"), "30\n").unwrap();
    fs::write(leaf.join("memory.swap.events"), "high 0\nmax 0\nfail 1\n").unwrap();

    wait_for(Duration::from_secs(5), || {
        let row = status(&state, submission.run_id);
        row["resource_receipt"]["peak"]["pids"] == 5
            && row["resource_receipt"]["peak"]["ram"] == 200
            && row["resource_receipt"]["peak"]["swap"] == 30
    });
    let sampled = status(&state, submission.run_id);
    let events = sampled["resource_receipt"]["events"].as_array().unwrap();
    for code in [
        "cpu-throttled",
        "pids-limit-hit",
        "memory-max-hit",
        "memory-oom",
        "memory-high-throttled",
        "memory-pressure",
        "swap-limit-hit",
    ] {
        assert_eq!(
            events.iter().filter(|event| event["code"] == code).count(),
            1,
            "event {code} was missing or duplicated"
        );
    }

    fs::write(leaf.join("memory.current"), "300\n").unwrap();
    fs::write(leaf.join("memory.peak"), "400\n").unwrap();
    fs::write(&release, "release").unwrap();
    let passed = wait_status(&state, submission.run_id, "passed");
    assert_eq!(passed["resource_receipt"]["peak"]["ram"], 400);
    assert!(broker.terminate().success());
}

#[test]
fn cgroup_fixture_applies_and_measures_directional_io_controls() {
    let temporary = TestDirectory::new("cgroup-io-receipt");
    let state = temporary.path().join("state");
    let root = temporary.path().join("delegated");
    let scratch = temporary.path().join("scratch");
    let checkout = temporary.path().join("checkout");
    let entered = temporary.path().join("entered");
    let release = temporary.path().join("release");
    for path in [&state, &root, &scratch, &checkout] {
        fs::create_dir(path).unwrap();
    }
    fs::write(
        state.join("config.json"),
        serde_json::to_vec(&json!({
            "bindings": {
                "read_bps": {"backend":"cgroup-v2", "kind":"io-bandwidth", "mode":"required", "unit":"read-bytes-per-second"},
                "write_bps": {"backend":"cgroup-v2", "kind":"io-bandwidth", "mode":"required", "unit":"write-bytes-per-second"},
                "read_iops": {"backend":"cgroup-v2", "kind":"io-operations", "mode":"required", "unit":"read-operations-per-second"},
                "write_iops": {"backend":"cgroup-v2", "kind":"io-operations", "mode":"required", "unit":"write-operations-per-second"},
                "weight": {"backend":"cgroup-v2", "kind":"io-weight", "mode":"required", "unit":"weight"}
            },
            "cgroup_root": root,
            "cgroup_io": {"paths": [scratch]},
        }))
        .unwrap(),
    )
    .unwrap();
    let requested = [
        ("read_bps", 8_000_000),
        ("write_bps", 6_000_000),
        ("read_iops", 80),
        ("write_iops", 60),
        ("weight", 250),
    ];
    let mut capacities = vec![("jobs", 1)];
    capacities.extend(requested);
    let mut broker = RunningBroker::start_with_options(
        &state,
        &capacities,
        &["--cgroup-fixture", root.to_str().unwrap()],
    );
    let submission = Submission {
        run_id: "cgroup-io-receipt",
        kind: "check",
        repository: "repo-a",
        checkout: &checkout,
        command: blocking_command(&entered, &release, None),
        gate_run_id: None,
    };
    assert!(
        submit_with_resources(&state, &submission, &requested)
            .status
            .success()
    );
    wait_for(Duration::from_secs(5), || entered.exists());
    let owner = fs::read_dir(&root)
        .unwrap()
        .flatten()
        .map(|entry| entry.path())
        .find(|path| path.is_dir())
        .unwrap();
    let leaf = fs::read_dir(&owner)
        .unwrap()
        .flatten()
        .map(|entry| entry.path())
        .find(|path| path.is_dir())
        .unwrap();
    let io_max = fs::read_to_string(leaf.join("io.max")).unwrap();
    for setting in ["rbps=8000000", "wbps=6000000", "riops=80", "wiops=60"] {
        assert!(io_max.contains(setting), "missing {setting} in {io_max:?}");
    }
    assert!(
        fs::read_to_string(leaf.join("io.weight"))
            .unwrap()
            .lines()
            .any(|line| line == "7:31 250")
    );
    fs::write(
        leaf.join("io.stat"),
        "7:31 rbytes=10000000 wbytes=8000000 rios=100 wios=80 dbytes=0 dios=0\n",
    )
    .unwrap();
    wait_for(Duration::from_secs(5), || {
        let receipt = status(&state, submission.run_id)["resource_receipt"].clone();
        ["read_bps", "write_bps", "read_iops", "write_iops"]
            .into_iter()
            .all(|name| {
                receipt["peak"][name]
                    .as_u64()
                    .is_some_and(|units| units > 0)
            })
    });
    let running = status(&state, submission.run_id);
    assert_eq!(
        running["resource_receipt"]["applied"],
        json!({
            "read_bps": 8_000_000,
            "write_bps": 6_000_000,
            "read_iops": 80,
            "write_iops": 60,
            "weight": 250,
        })
    );
    fs::write(&release, "release").unwrap();
    assert_eq!(
        wait_status(&state, submission.run_id, "passed")["status"],
        "passed"
    );
    assert!(broker.terminate().success());
}

#[test]
fn cgroup_fixture_applies_and_measures_bidirectional_io_units() {
    let temporary = TestDirectory::new("cgroup-io-combined");
    let state = temporary.path().join("state");
    let root = temporary.path().join("delegated");
    let scratch = temporary.path().join("scratch");
    let checkout = temporary.path().join("checkout");
    let entered = temporary.path().join("entered");
    let release = temporary.path().join("release");
    for path in [&state, &root, &scratch, &checkout] {
        fs::create_dir(path).unwrap();
    }
    fs::write(
        state.join("config.json"),
        serde_json::to_vec(&json!({
            "bindings": {
                "bandwidth": {"backend":"cgroup-v2", "kind":"io-bandwidth", "mode":"required", "unit":"bytes-per-second"},
                "operations": {"backend":"cgroup-v2", "kind":"io-operations", "mode":"required", "unit":"operations-per-second"}
            },
            "cgroup_root": root,
            "cgroup_io": {"paths": [scratch]},
        }))
        .unwrap(),
    )
    .unwrap();
    let requested = [("bandwidth", 5_000_000), ("operations", 50)];
    let mut broker = RunningBroker::start_with_options(
        &state,
        &[("jobs", 1), requested[0], requested[1]],
        &["--cgroup-fixture", root.to_str().unwrap()],
    );
    let submission = Submission {
        run_id: "cgroup-io-combined",
        kind: "check",
        repository: "repo-a",
        checkout: &checkout,
        command: blocking_command(&entered, &release, None),
        gate_run_id: None,
    };
    assert!(
        submit_with_resources(&state, &submission, &requested)
            .status
            .success()
    );
    wait_for(Duration::from_secs(5), || entered.exists());
    let owner = fs::read_dir(&root)
        .unwrap()
        .flatten()
        .map(|entry| entry.path())
        .find(|path| path.is_dir())
        .unwrap();
    let leaf = fs::read_dir(owner)
        .unwrap()
        .flatten()
        .map(|entry| entry.path())
        .find(|path| path.is_dir())
        .unwrap();
    let limits = fs::read_to_string(leaf.join("io.max")).unwrap();
    for expected in ["rbps=5000000", "wbps=5000000", "riops=50", "wiops=50"] {
        assert!(
            limits.contains(expected),
            "missing {expected} in {limits:?}"
        );
    }
    fs::write(
        leaf.join("io.stat"),
        "7:31 rbytes=9000000 wbytes=7000000 rios=90 wios=70 dbytes=0 dios=0\n",
    )
    .unwrap();
    wait_for(Duration::from_secs(5), || {
        let receipt = status(&state, submission.run_id)["resource_receipt"].clone();
        ["bandwidth", "operations"].into_iter().all(|name| {
            receipt["peak"][name]
                .as_u64()
                .is_some_and(|value| value > 0)
        })
    });
    fs::write(&release, "release").unwrap();
    let passed = wait_status(&state, submission.run_id, "passed");
    assert_eq!(
        passed["resource_receipt"]["applied"],
        json!({"bandwidth": 5_000_000, "operations": 50})
    );
    assert!(broker.terminate().success());
}

#[test]
fn invalid_cgroup_tmpfs_policy_refuses_before_user_code() {
    let temporary = TestDirectory::new("cgroup-tmpfs-policy");
    let state = temporary.path().join("state");
    let root = temporary.path().join("delegated");
    let checkout = temporary.path().join("checkout");
    let marker = temporary.path().join("must-not-run");
    for path in [&state, &root, &checkout] {
        fs::create_dir(path).unwrap();
    }
    fs::write(
        state.join("config.json"),
        serde_json::to_vec(&json!({
            "bindings": {
                "scratch": {"backend":"cgroup-v2", "kind":"tmpfs", "mode":"required", "unit":"bytes"},
                "scratch_inodes": {"backend":"cgroup-v2", "kind":"inodes", "mode":"required", "unit":"inodes"}
            },
            "cgroup_root": root,
        }))
        .unwrap(),
    )
    .unwrap();
    let mut broker = RunningBroker::start_with_options(
        &state,
        &[
            ("jobs", 1),
            ("scratch", 16 * 1024 * 1024),
            ("scratch_inodes", 128),
        ],
        &["--cgroup-fixture", root.to_str().unwrap()],
    );
    let submission = Submission {
        run_id: "cgroup-tmpfs-policy",
        kind: "check",
        repository: "repo-a",
        checkout: &checkout,
        command: touch_command(&marker),
        gate_run_id: None,
    };
    assert!(
        submit_with_resources(
            &state,
            &submission,
            &[("scratch", 16 * 1024 * 1024), ("scratch_inodes", 128)],
        )
        .status
        .success()
    );
    let failed = wait_status(&state, submission.run_id, "failed");
    assert_eq!(failed["exit_status"], 125);
    assert_eq!(failed["failure_reason"], "resource-enforcement-failed");
    assert!(
        failed["resource_receipt"]["events"]
            .as_array()
            .unwrap()
            .iter()
            .any(|event| event["code"] == "tmpfs-memory-required")
    );
    assert!(!marker.exists());
    assert!(broker.terminate().success());
}

#[test]
fn cgroup_fixture_provisions_private_tmpfs_and_retains_usage_receipt() {
    let temporary = TestDirectory::new("cgroup-tmpfs-setup");
    let state = temporary.path().join("state");
    let root = temporary.path().join("delegated");
    let checkout = temporary.path().join("checkout");
    let observed_target = temporary.path().join("observed-target");
    for path in [&state, &root, &checkout] {
        fs::create_dir(path).unwrap();
    }
    fs::write(
        state.join("config.json"),
        serde_json::to_vec(&json!({
            "bindings": {
                "ram": {"backend":"cgroup-v2", "kind":"memory", "mode":"required", "unit":"bytes"},
                "scratch": {"backend":"cgroup-v2", "kind":"tmpfs", "mode":"required", "unit":"bytes"},
                "scratch_inodes": {"backend":"cgroup-v2", "kind":"inodes", "mode":"required", "unit":"inodes"}
            },
            "cgroup_root": root,
        }))
        .unwrap(),
    )
    .unwrap();
    let requested = [
        ("ram", 64 * 1024 * 1024),
        ("scratch", 16 * 1024 * 1024),
        ("scratch_inodes", 128),
    ];
    let mut capacities = vec![("jobs", 1)];
    capacities.extend(requested);
    let mut broker = RunningBroker::start_with_options(
        &state,
        &capacities,
        &["--cgroup-fixture", root.to_str().unwrap()],
    );
    let submission = Submission {
        run_id: "cgroup-tmpfs-setup",
        kind: "check",
        repository: "repo-a",
        checkout: &checkout,
        command: vec![
            "/usr/bin/python3".to_owned(),
            "-c".to_owned(),
            "import os,pathlib,sys; assert os.environ['TMPDIR'] == os.environ['TMP'] == os.environ['TEMP']; target = pathlib.Path(os.environ['TMPDIR']); pathlib.Path(sys.argv[1]).write_text(str(target), encoding='ascii'); (target / 'payload').write_bytes(b'x' * 8192); (target / 'extra').touch()".to_owned(),
            observed_target.to_string_lossy().into_owned(),
        ],
        gate_run_id: None,
    };
    assert!(
        submit_with_resources(&state, &submission, &requested)
            .status
            .success()
    );
    let passed = wait_status(&state, submission.run_id, "passed");
    assert_eq!(
        passed["resource_receipt"]["applied"],
        json!({
            "ram": 64 * 1024 * 1024,
            "scratch": 16 * 1024 * 1024,
            "scratch_inodes": 128,
        })
    );
    assert!(
        passed["resource_receipt"]["peak"]["scratch"]
            .as_u64()
            .unwrap()
            >= 8192
    );
    assert!(
        passed["resource_receipt"]["peak"]["scratch_inodes"]
            .as_u64()
            .unwrap()
            >= 2
    );
    assert!(
        passed["resource_receipt"]["events"]
            .as_array()
            .unwrap()
            .iter()
            .any(|event| event["code"] == "tmpfs-mounted")
    );
    let target = PathBuf::from(fs::read_to_string(&observed_target).unwrap());
    assert!(!target.exists());
    assert!(broker.terminate().success());
}

#[test]
fn cgroup_tmpfs_mount_failure_obeys_required_or_disk_fallback_mode() {
    let temporary = TestDirectory::new("cgroup-tmpfs-worker-failure");
    let checkout = temporary.path().join("checkout");
    fs::create_dir(&checkout).unwrap();

    for (mode, expected_status, expected_event_status, user_code_runs) in [
        ("required", "failed", "failed", false),
        ("best-effort", "passed", "unapplied", true),
    ] {
        let state = temporary.path().join(format!("state-{mode}"));
        let root = temporary.path().join(format!("delegated-{mode}"));
        let marker = temporary.path().join(format!("user-code-{mode}"));
        fs::create_dir(&state).unwrap();
        fs::create_dir(&root).unwrap();
        fs::write(
            state.join("config.json"),
            serde_json::to_vec(&json!({
                "bindings": {
                    "ram": {"backend":"cgroup-v2", "kind":"memory", "mode":"required", "unit":"bytes"},
                    "scratch": {"backend":"cgroup-v2", "kind":"tmpfs", "mode":mode, "unit":"bytes"},
                    "scratch_inodes": {"backend":"cgroup-v2", "kind":"inodes", "mode":mode, "unit":"inodes"}
                },
                "cgroup_root": root,
            }))
            .unwrap(),
        )
        .unwrap();
        let requested = [
            ("ram", 64 * 1024 * 1024),
            ("scratch", 16 * 1024 * 1024),
            ("scratch_inodes", 128),
        ];
        let mut capacities = vec![("jobs", 1)];
        capacities.extend(requested);
        let mut broker = RunningBroker::start_with_options(
            &state,
            &capacities,
            &[
                "--cgroup-fixture",
                root.to_str().unwrap(),
                "--worker-fault",
                "tmpfs-mount-unavailable",
            ],
        );
        let submission = Submission {
            run_id: mode,
            kind: "check",
            repository: "repo-a",
            checkout: &checkout,
            command: touch_command(&marker),
            gate_run_id: None,
        };
        assert!(
            submit_with_resources(&state, &submission, &requested)
                .status
                .success()
        );
        let finished = wait_status(&state, submission.run_id, expected_status);
        assert_eq!(marker.exists(), user_code_runs);
        assert_eq!(
            finished["failure_reason"],
            if mode == "required" {
                json!("resource-enforcement-failed")
            } else {
                Value::Null
            }
        );
        assert_eq!(
            finished["resource_receipt"]["applied"],
            json!({"ram": 64 * 1024 * 1024})
        );
        for name in ["scratch", "scratch_inodes"] {
            assert!(
                finished["resource_receipt"]["events"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|event| event["resource"] == name
                        && event["stage"] == "attach"
                        && event["status"] == expected_event_status
                        && event["code"] == "tmpfs-mount-unavailable")
            );
        }
        assert!(
            !fs::read_dir(&root)
                .unwrap()
                .any(|entry| entry.unwrap().path().is_dir())
        );
        assert!(broker.terminate().success());
    }
}

#[test]
fn cgroup_namespace_setup_failure_is_never_released_to_user_code() {
    let temporary = TestDirectory::new("cgroup-namespace-worker-failure");
    let state = temporary.path().join("state");
    let root = temporary.path().join("delegated");
    let checkout = temporary.path().join("checkout");
    let marker = temporary.path().join("must-not-run");
    for path in [&state, &root, &checkout] {
        fs::create_dir(path).unwrap();
    }
    fs::write(
        state.join("config.json"),
        serde_json::to_vec(&json!({
            "bindings": {
                "cgroup_slot": {"backend":"cgroup-v2", "kind":"generic", "mode":"best-effort", "unit":"admission-unit"}
            },
            "cgroup_root": root,
        }))
        .unwrap(),
    )
    .unwrap();
    let mut broker = RunningBroker::start_with_options(
        &state,
        &[("jobs", 1), ("cgroup_slot", 1)],
        &[
            "--cgroup-fixture",
            root.to_str().unwrap(),
            "--worker-fault",
            "namespace-isolation-unavailable",
        ],
    );
    let submission = Submission {
        run_id: "cgroup-namespace-worker-failure",
        kind: "check",
        repository: "repo-a",
        checkout: &checkout,
        command: touch_command(&marker),
        gate_run_id: None,
    };
    assert!(
        submit_with_resources(&state, &submission, &[("cgroup_slot", 1)])
            .status
            .success()
    );
    let failed = wait_status(&state, submission.run_id, "failed");
    assert_eq!(failed["exit_status"], 125);
    assert_eq!(failed["failure_reason"], "resource-enforcement-failed");
    assert_eq!(failed["resource_receipt"]["applied"], json!({}));
    assert!(
        failed["resource_receipt"]["events"]
            .as_array()
            .unwrap()
            .iter()
            .any(|event| event["resource"] == "cgroup_slot"
                && event["stage"] == "attach"
                && event["status"] == "unapplied"
                && event["code"] == "namespace-isolation-unavailable")
    );
    assert!(!marker.exists());
    assert!(
        !fs::read_dir(&root)
            .unwrap()
            .any(|entry| entry.unwrap().path().is_dir())
    );
    assert!(broker.terminate().success());
}

#[test]
fn real_cgroup_tmpfs_enforces_bytes_inodes_and_private_teardown() {
    let Some(root) = real_cgroup_root() else {
        return;
    };
    const MIB: u64 = 1024 * 1024;
    let temporary = TestDirectory::new("real-cgroup-tmpfs");
    let state = temporary.path().join("state");
    let checkout = temporary.path().join("checkout");
    let report = temporary.path().join("limits.json");
    fs::create_dir(&state).unwrap();
    fs::create_dir(&checkout).unwrap();
    fs::write(
        state.join("config.json"),
        serde_json::to_vec(&json!({
            "bindings": {
                "ram": {"backend":"cgroup-v2", "kind":"memory", "mode":"required", "unit":"bytes"},
                "scratch": {"backend":"cgroup-v2", "kind":"tmpfs", "mode":"required", "unit":"bytes"},
                "scratch_inodes": {"backend":"cgroup-v2", "kind":"inodes", "mode":"required", "unit":"inodes"}
            },
            "cgroup_root": root,
        }))
        .unwrap(),
    )
    .unwrap();
    let mut broker = RunningBroker::start(
        &state,
        &[
            ("jobs", 1),
            ("ram", 128 * MIB),
            ("scratch", 64 * MIB),
            ("scratch_inodes", 2048),
        ],
    );
    let script = r#"
import errno
import json
import os
from pathlib import Path
import sys
import time

target = Path(os.environ['TMPDIR'])
mount = None
for line in Path('/proc/self/mountinfo').read_text(encoding='ascii').splitlines():
    left, separator, right = line.partition(' - ')
    if separator and left.split()[4] == str(target):
        mount = {
            'filesystem': right.split()[0],
            'options': sorted(set(left.split()[5].split(',') + right.split()[2].split(','))),
        }
        break
if mount is None:
    raise AssertionError('TMPDIR is not a mount')
created = []
inode_exhausted = False
for index in range(256):
    try:
        path = target / f'inode-{index}'
        path.touch(exist_ok=False)
        created.append(path)
    except OSError as exc:
        if exc.errno != errno.ENOSPC:
            raise
        inode_exhausted = True
        break
time.sleep(0.2)
for path in created:
    path.unlink()
written = 0
byte_exhausted = False
descriptor = os.open(target / 'payload', os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
try:
    block = b'x' * (1024 * 1024)
    while True:
        try:
            written += os.write(descriptor, block)
        except OSError as exc:
            if exc.errno != errno.ENOSPC:
                raise
            byte_exhausted = True
            break
finally:
    os.close(descriptor)
time.sleep(0.2)
Path(sys.argv[1]).write_text(json.dumps({
    'byte_exhausted': byte_exhausted,
    'created': len(created),
    'inode_exhausted': inode_exhausted,
    'mount': mount,
    'target': str(target),
    'written': written,
}), encoding='ascii')
"#;
    let submission = Submission {
        run_id: "real-cgroup-tmpfs",
        kind: "check",
        repository: "repo-a",
        checkout: &checkout,
        command: vec![
            "/usr/bin/python3".to_owned(),
            "-c".to_owned(),
            script.to_owned(),
            report.to_string_lossy().into_owned(),
        ],
        gate_run_id: None,
    };
    let requested = [
        ("ram", 128 * MIB),
        ("scratch", 16 * MIB),
        ("scratch_inodes", 32),
    ];
    assert!(
        submit_with_resources(&state, &submission, &requested)
            .status
            .success()
    );
    let passed = wait_status(&state, submission.run_id, "passed");
    let observed: Value = serde_json::from_slice(&fs::read(&report).unwrap()).unwrap();
    assert_eq!(observed["byte_exhausted"], true);
    assert_eq!(observed["inode_exhausted"], true);
    assert!((1..32).contains(&observed["created"].as_u64().unwrap()));
    assert!((1..=16 * MIB).contains(&observed["written"].as_u64().unwrap()));
    assert_eq!(observed["mount"]["filesystem"], "tmpfs");
    for option in ["nodev", "noexec", "nosuid"] {
        assert!(
            observed["mount"]["options"]
                .as_array()
                .unwrap()
                .iter()
                .any(|value| value == option)
        );
    }
    assert!(!PathBuf::from(observed["target"].as_str().unwrap()).exists());
    let codes: BTreeSet<_> = passed["resource_receipt"]["events"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|event| event["code"].as_str())
        .collect();
    assert!(
        [
            "tmpfs-byte-limit-hit",
            "tmpfs-inode-limit-hit",
            "tmpfs-mounted"
        ]
        .into_iter()
        .all(|code| codes.contains(code))
    );
    assert!(
        passed["resource_receipt"]["peak"]["ram"].as_u64().unwrap()
            >= passed["resource_receipt"]["peak"]["scratch"]
                .as_u64()
                .unwrap()
    );
    assert_no_cgroup_owner(&root);
    assert!(broker.terminate().success());
}

#[test]
fn real_cgroup_io_covers_combined_directional_and_weight_units() {
    let Some(root) = real_cgroup_root() else {
        return;
    };
    if std::env::var("AGCOORD_TEST_CGROUP_IO").as_deref() != Ok("1") {
        return;
    }
    const MIB: u64 = 1024 * 1024;
    for command in ["dd", "mkfs.ext4", "mount", "umount"] {
        assert!(Command::new(command).arg("--help").output().is_ok());
    }

    let temporary = TestDirectory::new("real-cgroup-io");
    let state = temporary.path().join("state");
    let checkout = temporary.path().join("checkout");
    let image = temporary.path().join("block-io.ext4");
    let mountpoint = temporary.path().join("mounted");
    fs::create_dir(&state).unwrap();
    fs::create_dir(&checkout).unwrap();
    fs::create_dir(&mountpoint).unwrap();
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&image)
        .unwrap()
        .set_len(128 * MIB)
        .unwrap();
    assert!(
        Command::new("mkfs.ext4")
            .args(["-q", "-F"])
            .arg(&image)
            .status()
            .unwrap()
            .success()
    );
    assert!(
        Command::new("mount")
            .args(["-o", "loop"])
            .arg(&image)
            .arg(&mountpoint)
            .status()
            .unwrap()
            .success()
    );
    let _mount = MountGuard(mountpoint.clone());
    fs::set_permissions(&mountpoint, fs::Permissions::from_mode(0o777)).unwrap();
    let input = mountpoint.join("input");
    fs::write(&input, vec![b'x'; 4 * MIB as usize]).unwrap();
    assert!(Command::new("sync").status().unwrap().success());

    fs::write(
        state.join("config.json"),
        serde_json::to_vec(&json!({
            "bindings": {
                "bandwidth": {"backend":"cgroup-v2", "kind":"io-bandwidth", "mode":"required", "unit":"bytes-per-second"},
                "iops": {"backend":"cgroup-v2", "kind":"io-operations", "mode":"required", "unit":"operations-per-second"},
                "read_bps": {"backend":"cgroup-v2", "kind":"io-bandwidth", "mode":"required", "unit":"read-bytes-per-second"},
                "write_bps": {"backend":"cgroup-v2", "kind":"io-bandwidth", "mode":"required", "unit":"write-bytes-per-second"},
                "read_iops": {"backend":"cgroup-v2", "kind":"io-operations", "mode":"required", "unit":"read-operations-per-second"},
                "write_iops": {"backend":"cgroup-v2", "kind":"io-operations", "mode":"required", "unit":"write-operations-per-second"},
                "io_weight": {"backend":"cgroup-v2", "kind":"io-weight", "mode":"required", "unit":"weight"}
            },
            "cgroup_root": root,
            "cgroup_io": {"paths": [mountpoint]},
        }))
        .unwrap(),
    )
    .unwrap();
    let capacities = [
        ("jobs", 1),
        ("bandwidth", 8 * MIB),
        ("iops", 128),
        ("read_bps", 10 * MIB),
        ("write_bps", 8 * MIB),
        ("read_iops", 160),
        ("write_iops", 128),
        ("io_weight", 250),
    ];
    let mut broker = RunningBroker::start(&state, &capacities);
    assert_eq!(
        snapshot(&state).unwrap()["resource_capabilities"]["cgroup-v2"]["available"],
        true
    );

    let workload = r#"
set -eu
touch "$1"
while [ ! -e "$2" ]; do sleep 0.01; done
dd if="$IO_SCRATCH/input" of=/dev/null bs=65536 count=64 iflag=direct status=none
dd if=/dev/zero of="$IO_SCRATCH/$3" bs=65536 count=64 oflag=direct conv=fsync status=none
"#;
    let find_leaf = || {
        let owners: Vec<_> = fs::read_dir(&root)
            .unwrap()
            .flatten()
            .filter(|entry| {
                entry.path().is_dir()
                    && entry.file_name().to_string_lossy().starts_with("agcoord-u")
            })
            .map(|entry| entry.path())
            .collect();
        assert_eq!(owners.len(), 1);
        let leaves: Vec<_> = fs::read_dir(&owners[0])
            .unwrap()
            .flatten()
            .filter(|entry| {
                entry.path().is_dir() && entry.file_name().to_string_lossy().starts_with("run-")
            })
            .map(|entry| entry.path())
            .collect();
        assert_eq!(leaves.len(), 1);
        leaves[0].clone()
    };

    let combined_entered = temporary.path().join("combined-entered");
    let combined_release = temporary.path().join("combined-release");
    let combined = Submission {
        run_id: "real-cgroup-io-combined",
        kind: "check",
        repository: "repo-a",
        checkout: &checkout,
        command: vec![
            "/bin/sh".to_owned(),
            "-c".to_owned(),
            workload.to_owned(),
            "agcoord-io".to_owned(),
            combined_entered.to_string_lossy().into_owned(),
            combined_release.to_string_lossy().into_owned(),
            "combined-output".to_owned(),
        ],
        gate_run_id: None,
    };
    let combined_resources = [("bandwidth", 8 * MIB), ("iops", 128), ("io_weight", 250)];
    assert!(
        submit_with_resources_and_environment(
            &state,
            &combined,
            &combined_resources,
            &[("IO_SCRATCH", mountpoint.to_str().unwrap())],
        )
        .status
        .success()
    );
    wait_for(Duration::from_secs(5), || combined_entered.exists());
    let combined_leaf = find_leaf();
    let combined_limits = fs::read_to_string(combined_leaf.join("io.max")).unwrap();
    assert!(combined_limits.contains(&format!("rbps={} ", 8 * MIB)));
    assert!(combined_limits.contains(&format!("wbps={} ", 8 * MIB)));
    assert!(combined_limits.contains("riops=128"));
    assert!(combined_limits.contains("wiops=128"));
    assert!(
        fs::read_to_string(combined_leaf.join("io.weight"))
            .unwrap()
            .lines()
            .any(|line| line.ends_with(" 250"))
    );
    fs::write(&combined_release, "release").unwrap();
    let combined_finished = wait_status(&state, combined.run_id, "passed");
    assert_eq!(
        combined_finished["resource_receipt"]["applied"],
        json!({"bandwidth": 8 * MIB, "iops": 128, "io_weight": 250})
    );
    assert!(
        combined_finished["resource_receipt"]["peak"]["bandwidth"]
            .as_u64()
            .unwrap()
            > 0
    );
    assert!(
        combined_finished["resource_receipt"]["peak"]["iops"]
            .as_u64()
            .unwrap()
            > 0
    );

    let directional_entered = temporary.path().join("directional-entered");
    let directional_release = temporary.path().join("directional-release");
    let directional = Submission {
        run_id: "real-cgroup-io-directional",
        kind: "check",
        repository: "repo-a",
        checkout: &checkout,
        command: vec![
            "/bin/sh".to_owned(),
            "-c".to_owned(),
            workload.to_owned(),
            "agcoord-io".to_owned(),
            directional_entered.to_string_lossy().into_owned(),
            directional_release.to_string_lossy().into_owned(),
            "directional-output".to_owned(),
        ],
        gate_run_id: None,
    };
    let directional_resources = [
        ("read_bps", 10 * MIB),
        ("write_bps", 8 * MIB),
        ("read_iops", 160),
        ("write_iops", 128),
        ("io_weight", 250),
    ];
    assert!(
        submit_with_resources_and_environment(
            &state,
            &directional,
            &directional_resources,
            &[("IO_SCRATCH", mountpoint.to_str().unwrap())],
        )
        .status
        .success()
    );
    wait_for(Duration::from_secs(5), || directional_entered.exists());
    let directional_leaf = find_leaf();
    let directional_limits = fs::read_to_string(directional_leaf.join("io.max")).unwrap();
    assert!(directional_limits.contains(&format!("rbps={} ", 10 * MIB)));
    assert!(directional_limits.contains(&format!("wbps={} ", 8 * MIB)));
    assert!(directional_limits.contains("riops=160"));
    assert!(directional_limits.contains("wiops=128"));
    fs::write(&directional_release, "release").unwrap();
    let directional_finished = wait_status(&state, directional.run_id, "passed");
    assert_eq!(
        directional_finished["resource_receipt"]["applied"],
        json!({
            "read_bps": 10 * MIB,
            "write_bps": 8 * MIB,
            "read_iops": 160,
            "write_iops": 128,
            "io_weight": 250,
        })
    );
    for name in ["read_bps", "write_bps", "read_iops", "write_iops"] {
        assert!(
            directional_finished["resource_receipt"]["peak"][name]
                .as_u64()
                .unwrap()
                > 0
        );
    }
    assert_no_cgroup_owner(&root);
    assert!(broker.terminate().success());
}

#[test]
fn running_owner_retains_the_configuration_loaded_at_startup() {
    let temporary = TestDirectory::new("config-snapshot");
    let state = temporary.path().join("state");
    let checkout = temporary.path().join("checkout");
    fs::create_dir(&state).unwrap();
    fs::write(state.join("config.json"), r#"{"database_timeout":0.05}"#).unwrap();
    fs::create_dir(&checkout).unwrap();
    let entered = temporary.path().join("entered");
    let release = temporary.path().join("release");
    let mut broker = RunningBroker::start(&state, &[("jobs", 1)]);
    submit_ok(
        &state,
        &Submission {
            run_id: "config-snapshot",
            kind: "check",
            repository: "repo-a",
            checkout: &checkout,
            command: blocking_command(&entered, &release, None),
            gate_run_id: None,
        },
    );
    wait_for(Duration::from_secs(5), || entered.exists());
    fs::write(
        state.join("config.json"),
        r#"{"database_timeout":"broken"}"#,
    )
    .unwrap();
    fs::write(&release, "release").unwrap();
    let database = state.join("queue.sqlite3");
    wait_for(Duration::from_secs(2), || {
        assert!(broker.is_running(), "broker re-read changed configuration");
        Connection::open(&database)
            .unwrap()
            .query_row(
                "SELECT status FROM runs WHERE run_id = 'config-snapshot'",
                [],
                |row| row.get::<_, String>(0),
            )
            .unwrap()
            == "passed"
    });
    assert!(broker.terminate().success());
}

#[test]
fn migration_refuses_live_and_preserves_terminal_project_quota_identity() {
    let temporary = TestDirectory::new("project-quota-migration");
    let state = temporary.path().join("state");
    let checkout = temporary.path().join("checkout");
    fs::create_dir(&checkout).unwrap();
    let mut bootstrap = RunningBroker::start(&state, &[("jobs", 1)]);
    submit_ok(
        &state,
        &Submission {
            run_id: "legacy-full",
            kind: "full",
            repository: "repo-a",
            checkout: &checkout,
            command: vec!["/usr/bin/true".to_owned()],
            gate_run_id: None,
        },
    );
    wait_status(&state, "legacy-full", "passed");
    assert!(bootstrap.terminate().success());
    let resources = json!({"disk": 8 * MIB, "disk_inodes": 64, "jobs": 1});
    let contract = json!({
        "disk": {"backend":"project-quota", "kind":"storage", "mode":"required", "unit":"bytes"},
        "disk_inodes": {"backend":"project-quota", "kind":"inodes", "mode":"required", "unit":"inodes"},
        "jobs": {"backend":null, "kind":"generic", "mode":"admission-only", "unit":"admission-unit"}
    });
    let receipt = json!({
        "requested": resources,
        "applied": {"disk": 8 * MIB, "disk_inodes": 64},
        "peak": {"disk": 4096, "disk_inodes": 2},
        "events": [
            {"at":"2026-08-31T00:00:01Z", "backend":"project-quota", "resource":"disk", "stage":"attach", "status":"applied", "code":"quota-ready"},
            {"at":"2026-08-31T00:00:01Z", "backend":"project-quota", "resource":"disk_inodes", "stage":"attach", "status":"applied", "code":"quota-ready"}
        ]
    });
    let resource_state = json!({
        "project-quota": {
            "handle": {
                "version": 1,
                "token": "0123456789abcdef0123456789abcdef",
                "project_id": 1500000042_u64,
                "path": "/managed/project-quota/run-identity-0123456789ab",
                "path_device": 41,
                "path_inode": 42,
                "filesystem": "ext4",
                "mount_device": "8:30",
                "hard_bytes": 8 * MIB,
                "hard_inodes": 64
            },
            "resources": ["disk", "disk_inodes"],
            "finished": false,
            "cancelled": false
        }
    });
    let encoded = (
        serde_json::to_string(&resources).unwrap(),
        serde_json::to_string(&contract).unwrap(),
        serde_json::to_string(&receipt).unwrap(),
        serde_json::to_string(&resource_state).unwrap(),
    );
    let database = state.join("queue.sqlite3");
    let db = Connection::open(&database).unwrap();
    db.execute("DELETE FROM coordinator_meta WHERE key != 'protocol'", [])
        .unwrap();
    db.execute(
        "UPDATE coordinator_meta SET value='4' WHERE key='protocol'",
        [],
    )
    .unwrap();
    db.execute(
        "UPDATE runs SET status='running', phase='running', finished_at=NULL,
             exit_status=NULL, resources_json=?1, resource_contract_json=?2,
             resource_receipt_json=?3, resource_state_json=?4
         WHERE run_id='legacy-full'",
        params![encoded.0, encoded.1, encoded.2, encoded.3],
    )
    .unwrap();
    drop(db);

    let refused = run(&["migrate", "--state-dir", state_argument(&state)]);
    assert!(!refused.status.success());
    assert_eq!(
        serde_json::from_slice::<Value>(&refused.stderr).unwrap()["code"],
        "broker-migration-live-runs"
    );
    let db = Connection::open(&database).unwrap();
    assert_eq!(
        db.query_row(
            "SELECT resource_state_json FROM runs WHERE run_id='legacy-full'",
            [],
            |row| row.get::<_, String>(0),
        )
        .unwrap(),
        encoded.3
    );
    db.execute(
        "UPDATE runs SET status='interrupted', phase='complete',
             finished_at='2026-08-31T00:00:02Z', exit_status=125
         WHERE run_id='legacy-full'",
        [],
    )
    .unwrap();
    drop(db);
    let migrated = json_output(&["migrate", "--state-dir", state_argument(&state)]);
    assert_eq!(migrated["from_protocol"], 4);
    assert_eq!(migrated["to_protocol"], 5);
    let db = Connection::open(&database).unwrap();
    let preserved: (String, String, String) = db
        .query_row(
            "SELECT resource_contract_json, resource_receipt_json, resource_state_json
             FROM runs WHERE run_id='legacy-full'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(preserved, (encoded.1, encoded.2, encoded.3));
}

#[test]
fn corrupt_resource_receipt_fails_stably_without_rewriting_state() {
    let temporary = TestDirectory::new("resource-receipt-corruption");
    let state = temporary.path().join("state");
    let checkout = temporary.path().join("checkout");
    fs::create_dir(&checkout).unwrap();
    let mut broker = RunningBroker::start(&state, &[("jobs", 1)]);
    submit_ok(
        &state,
        &Submission {
            run_id: "corrupt-resource-receipt",
            kind: "check",
            repository: "repo-a",
            checkout: &checkout,
            command: vec!["/usr/bin/true".to_owned()],
            gate_run_id: None,
        },
    );
    wait_status(&state, "corrupt-resource-receipt", "passed");
    assert!(broker.terminate().success());

    let database = state.join("queue.sqlite3");
    let db = Connection::open(&database).unwrap();
    let invalid_receipt =
        r#"{"requested":{"jobs":1},"applied":{"unknown":1},"peak":{},"events":[]}"#;
    db.execute(
        "UPDATE runs SET resource_receipt_json = ?1 WHERE run_id = 'corrupt-resource-receipt'",
        params![invalid_receipt],
    )
    .unwrap();
    let metadata_before: Vec<(String, String)> = db
        .prepare("SELECT key, value FROM coordinator_meta ORDER BY key")
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .collect::<std::result::Result<_, _>>()
        .unwrap();
    drop(db);

    for operation in ["migrate", "serve"] {
        let result = if operation == "migrate" {
            run(&["migrate", "--state-dir", state_argument(&state)])
        } else {
            run(&[
                "serve",
                "--state-dir",
                state_argument(&state),
                "--capacity",
                "jobs=1",
            ])
        };
        assert!(!result.status.success());
        assert_eq!(
            serde_json::from_slice::<Value>(&result.stderr).unwrap()["code"],
            "broker-row-invalid"
        );
    }

    let db = Connection::open(&database).unwrap();
    assert_eq!(
        db.query_row(
            "SELECT resource_receipt_json FROM runs WHERE run_id = 'corrupt-resource-receipt'",
            [],
            |row| row.get::<_, String>(0),
        )
        .unwrap(),
        invalid_receipt
    );
    let metadata_after: Vec<(String, String)> = db
        .prepare("SELECT key, value FROM coordinator_meta ORDER BY key")
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .collect::<std::result::Result<_, _>>()
        .unwrap();
    assert_eq!(metadata_after, metadata_before);
}

#[test]
fn corrupt_resource_contract_or_state_fails_before_owner_mutation() {
    let temporary = TestDirectory::new("resource-record-corruption");
    let state = temporary.path().join("state");
    let checkout = temporary.path().join("checkout");
    fs::create_dir(&checkout).unwrap();
    let mut broker = RunningBroker::start(&state, &[("jobs", 1)]);
    submit_ok(
        &state,
        &Submission {
            run_id: "corrupt-resource-record",
            kind: "check",
            repository: "repo-a",
            checkout: &checkout,
            command: vec!["/usr/bin/true".to_owned()],
            gate_run_id: None,
        },
    );
    wait_status(&state, "corrupt-resource-record", "passed");
    assert!(broker.terminate().success());

    let database = state.join("queue.sqlite3");
    for (column, invalid) in [
        (
            "resource_contract_json",
            r#"{"unknown":{"backend":null,"kind":"generic","mode":"admission-only","unit":"admission-unit"}}"#,
        ),
        (
            "resource_state_json",
            r#"{"cgroup-v2":{"handle":{},"resources":["jobs"],"finished":false,"cancelled":false}}"#,
        ),
    ] {
        let db = Connection::open(&database).unwrap();
        let original: String = db
            .query_row(
                &format!("SELECT {column} FROM runs WHERE run_id = 'corrupt-resource-record'"),
                [],
                |row| row.get(0),
            )
            .unwrap();
        db.execute(
            &format!("UPDATE runs SET {column} = ?1 WHERE run_id = 'corrupt-resource-record'"),
            params![invalid],
        )
        .unwrap();
        let metadata_before: Vec<(String, String)> = db
            .prepare("SELECT key, value FROM coordinator_meta ORDER BY key")
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<std::result::Result<_, _>>()
            .unwrap();
        drop(db);

        for result in [
            run(&["migrate", "--state-dir", state_argument(&state)]),
            run(&[
                "serve",
                "--state-dir",
                state_argument(&state),
                "--capacity",
                "jobs=1",
                "--idle-timeout",
                "0.05",
            ]),
        ] {
            assert!(!result.status.success(), "{column} corruption was accepted");
            assert_eq!(
                serde_json::from_slice::<Value>(&result.stderr).unwrap()["code"],
                "broker-row-invalid"
            );
        }

        let db = Connection::open(&database).unwrap();
        assert_eq!(
            db.query_row(
                &format!("SELECT {column} FROM runs WHERE run_id = 'corrupt-resource-record'"),
                [],
                |row| row.get::<_, String>(0),
            )
            .unwrap(),
            invalid
        );
        let metadata_after: Vec<(String, String)> = db
            .prepare("SELECT key, value FROM coordinator_meta ORDER BY key")
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<std::result::Result<_, _>>()
            .unwrap();
        assert_eq!(metadata_after, metadata_before);
        db.execute(
            &format!("UPDATE runs SET {column} = ?1 WHERE run_id = 'corrupt-resource-record'"),
            params![original],
        )
        .unwrap();
    }
}

#[test]
fn corrupt_schema_or_row_fails_stably_without_rewriting_state() {
    let temporary = TestDirectory::new("corruption");
    let state = temporary.path().join("state");
    let checkout = temporary.path().join("checkout");
    fs::create_dir(&checkout).unwrap();
    let mut broker = RunningBroker::start(&state, &[("jobs", 1)]);
    submit_ok(
        &state,
        &Submission {
            run_id: "corrupt-row",
            kind: "check",
            repository: "repo-a",
            checkout: &checkout,
            command: vec!["/usr/bin/true".to_owned()],
            gate_run_id: None,
        },
    );
    wait_status(&state, "corrupt-row", "passed");
    assert!(broker.terminate().success());
    let database = state.join("queue.sqlite3");
    let db = Connection::open(&database).unwrap();
    db.execute(
        "UPDATE runs SET resources_json = 'not-json' WHERE run_id = 'corrupt-row'",
        [],
    )
    .unwrap();
    let metadata_before: Vec<(String, String)> = db
        .prepare("SELECT key, value FROM coordinator_meta ORDER BY key")
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .collect::<std::result::Result<_, _>>()
        .unwrap();
    drop(db);

    let corrupt_migration = run(&["migrate", "--state-dir", state_argument(&state)]);
    assert!(!corrupt_migration.status.success());
    assert_eq!(
        serde_json::from_slice::<Value>(&corrupt_migration.stderr).unwrap()["code"],
        "broker-row-invalid"
    );
    let corrupt = run(&[
        "serve",
        "--state-dir",
        state_argument(&state),
        "--capacity",
        "jobs=1",
    ]);
    assert!(!corrupt.status.success());
    assert_eq!(
        serde_json::from_slice::<Value>(&corrupt.stderr).unwrap()["code"],
        "broker-row-invalid"
    );
    let db = Connection::open(&database).unwrap();
    assert_eq!(
        db.query_row(
            "SELECT resources_json FROM runs WHERE run_id = 'corrupt-row'",
            [],
            |row| row.get::<_, String>(0),
        )
        .unwrap(),
        "not-json"
    );
    let metadata_after: Vec<(String, String)> = db
        .prepare("SELECT key, value FROM coordinator_meta ORDER BY key")
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .collect::<std::result::Result<_, _>>()
        .unwrap();
    assert_eq!(metadata_after, metadata_before);
    db.execute("DROP TABLE child_cpu_leases", []).unwrap();
    drop(db);

    let partial = run(&[
        "serve",
        "--state-dir",
        state_argument(&state),
        "--capacity",
        "jobs=1",
    ]);
    assert!(!partial.status.success());
    assert_eq!(
        serde_json::from_slice::<Value>(&partial.stderr).unwrap()["code"],
        "broker-schema-invalid"
    );
    let db = Connection::open(&database).unwrap();
    assert_eq!(
        db.query_row(
            "SELECT value FROM coordinator_meta WHERE key = 'protocol'",
            [],
            |row| row.get::<_, String>(0),
        )
        .unwrap(),
        "5"
    );
}

#[test]
fn stored_nul_values_are_rejected_before_worker_syscalls() {
    let temporary = TestDirectory::new("stored-nul");
    let state = temporary.path().join("state");
    let checkout = temporary.path().join("checkout");
    fs::create_dir(&checkout).unwrap();
    let mut broker = RunningBroker::start(&state, &[("jobs", 1)]);
    submit_ok(
        &state,
        &Submission {
            run_id: "nul-row",
            kind: "check",
            repository: "repo-a",
            checkout: &checkout,
            command: vec!["/usr/bin/true".to_owned()],
            gate_run_id: None,
        },
    );
    wait_status(&state, "nul-row", "passed");
    assert!(broker.terminate().success());
    let database = state.join("queue.sqlite3");
    let db = Connection::open(&database).unwrap();
    db.execute(
        r#"UPDATE runs SET environment_json = '{"BAD":"before\u0000after"}'
           WHERE run_id = 'nul-row'"#,
        [],
    )
    .unwrap();
    drop(db);

    let refused = run(&[
        "serve",
        "--state-dir",
        state_argument(&state),
        "--capacity",
        "jobs=1",
        "--idle-timeout",
        "0.1",
    ]);
    assert!(!refused.status.success());
    assert_eq!(
        serde_json::from_slice::<Value>(&refused.stderr).unwrap()["code"],
        "broker-row-invalid"
    );
    let db = Connection::open(&database).unwrap();
    assert_eq!(
        db.query_row(
            "SELECT environment_json FROM runs WHERE run_id = 'nul-row'",
            [],
            |row| row.get::<_, String>(0),
        )
        .unwrap(),
        r#"{"BAD":"before\u0000after"}"#
    );
}

#[test]
fn ownership_and_admission_commit_crashes_recover_without_duplicate_work() {
    let temporary = TestDirectory::new("crash-ownership-admission");
    let owner_state = temporary.path().join("owner-state");
    let owner_crash = run(&[
        "serve",
        "--state-dir",
        state_argument(&owner_state),
        "--capacity",
        "jobs=1",
        "--crash-after",
        "owner-lock",
    ]);
    assert_eq!(owner_crash.status.code(), Some(86));
    let mut owner_replacement = RunningBroker::start(&owner_state, &[("jobs", 1)]);
    assert!(owner_replacement.terminate().success());

    let state = temporary.path().join("admission-state");
    let checkout = temporary.path().join("checkout");
    fs::create_dir(&checkout).unwrap();
    let marker = temporary.path().join("must-not-run");
    let mut crashing = start_crashing_broker(&state, "admission-commit");
    submit_ok(
        &state,
        &Submission {
            run_id: "admission-crash",
            kind: "check",
            repository: "repo-a",
            checkout: &checkout,
            command: touch_command(&marker),
            gate_run_id: None,
        },
    );
    assert_eq!(crashing.wait().unwrap().code(), Some(86));
    assert_eq!(status(&state, "admission-crash")["status"], "running");
    assert!(!marker.exists());
    let mut replacement = RunningBroker::start(&state, &[("jobs", 1)]);
    let recovered = wait_status(&state, "admission-crash", "interrupted");
    assert_eq!(recovered["failure_reason"], "worker-result-lost");
    assert!(!marker.exists());
    assert!(replacement.terminate().success());
}

#[test]
fn worker_identity_and_terminal_commit_crashes_recover_durably() {
    let temporary = TestDirectory::new("crash-worker-terminal");
    let state = temporary.path().join("worker-state");
    let checkout = temporary.path().join("checkout");
    fs::create_dir(&checkout).unwrap();
    let marker = temporary.path().join("must-not-run-before-release");
    let mut crashing = start_crashing_broker(&state, "worker-identity-commit");
    submit_ok(
        &state,
        &Submission {
            run_id: "identity-crash",
            kind: "check",
            repository: "repo-a",
            checkout: &checkout,
            command: touch_command(&marker),
            gate_run_id: None,
        },
    );
    assert_eq!(crashing.wait().unwrap().code(), Some(86));
    thread::sleep(Duration::from_millis(250));
    assert!(
        !marker.exists(),
        "the admitted command ran before the broker released its launcher"
    );
    let original_pid = status(&state, "identity-crash")["worker_pid"].clone();
    let mut replacement = RunningBroker::start(&state, &[("jobs", 1)]);
    assert_eq!(status(&state, "identity-crash")["worker_pid"], original_pid);
    let recovered = wait_status(&state, "identity-crash", "interrupted");
    assert_eq!(recovered["failure_reason"], "worker-result-lost");
    assert!(!marker.exists());
    assert!(replacement.terminate().success());

    let terminal_state = temporary.path().join("terminal-state");
    let mut terminal_crash = start_crashing_broker(&terminal_state, "terminal-commit");
    submit_ok(
        &terminal_state,
        &Submission {
            run_id: "terminal-crash",
            kind: "check",
            repository: "repo-b",
            checkout: &checkout,
            command: vec!["/usr/bin/true".to_owned()],
            gate_run_id: None,
        },
    );
    assert_eq!(terminal_crash.wait().unwrap().code(), Some(86));
    assert_eq!(
        status(&terminal_state, "terminal-crash")["status"],
        "passed"
    );
    let mut terminal_replacement = RunningBroker::start(&terminal_state, &[("jobs", 1)]);
    assert_eq!(
        status(&terminal_state, "terminal-crash")["status"],
        "passed"
    );
    assert!(terminal_replacement.terminate().success());

    let cleanup_state = temporary.path().join("cleanup-state");
    let mut cleanup_crash = start_crashing_broker(&cleanup_state, "worker-cleanup");
    submit_ok(
        &cleanup_state,
        &Submission {
            run_id: "cleanup-crash",
            kind: "check",
            repository: "repo-c",
            checkout: &checkout,
            command: vec!["/usr/bin/true".to_owned()],
            gate_run_id: None,
        },
    );
    assert_eq!(cleanup_crash.wait().unwrap().code(), Some(86));
    assert_eq!(status(&cleanup_state, "cleanup-crash")["status"], "passed");
    let mut cleanup_replacement = RunningBroker::start(&cleanup_state, &[("jobs", 1)]);
    assert_eq!(status(&cleanup_state, "cleanup-crash")["status"], "passed");
    assert!(cleanup_replacement.terminate().success());
}

#[test]
fn setup_and_final_release_crashes_preserve_exactly_once_execution() {
    let temporary = TestDirectory::new("crash-worker-release");
    let checkout = temporary.path().join("checkout");
    fs::create_dir(&checkout).unwrap();

    let setup_state = temporary.path().join("setup-state");
    let setup_marker = temporary.path().join("setup-must-not-run");
    let mut setup_crash = start_crashing_broker(&setup_state, "worker-setup-commit");
    submit_ok(
        &setup_state,
        &Submission {
            run_id: "setup-crash",
            kind: "check",
            repository: "repo-a",
            checkout: &checkout,
            command: touch_command(&setup_marker),
            gate_run_id: None,
        },
    );
    assert_eq!(setup_crash.wait().unwrap().code(), Some(86));
    thread::sleep(Duration::from_millis(250));
    assert!(!setup_marker.exists());
    let mut setup_replacement = RunningBroker::start(&setup_state, &[("jobs", 1)]);
    wait_status(&setup_state, "setup-crash", "interrupted");
    assert!(!setup_marker.exists());
    assert!(setup_replacement.terminate().success());

    let release_state = temporary.path().join("release-state");
    let entered = temporary.path().join("entered");
    let release = temporary.path().join("release");
    let starts = temporary.path().join("starts");
    let mut release_crash = start_crashing_broker(&release_state, "worker-release");
    submit_ok(
        &release_state,
        &Submission {
            run_id: "release-crash",
            kind: "check",
            repository: "repo-b",
            checkout: &checkout,
            command: blocking_command(&entered, &release, Some(&starts)),
            gate_run_id: None,
        },
    );
    assert_eq!(release_crash.wait().unwrap().code(), Some(86));
    wait_for(Duration::from_secs(5), || entered.exists());
    let original_pid = status(&release_state, "release-crash")["worker_pid"].clone();
    let mut release_replacement = RunningBroker::start(&release_state, &[("jobs", 1)]);
    assert_eq!(
        status(&release_state, "release-crash")["worker_pid"],
        original_pid
    );
    assert_eq!(fs::read_to_string(&starts).unwrap().lines().count(), 1);
    fs::write(&release, "release").unwrap();
    wait_status(&release_state, "release-crash", "interrupted");
    assert_eq!(fs::read_to_string(&starts).unwrap().lines().count(), 1);
    assert!(release_replacement.terminate().success());
}

#[test]
fn admitted_command_enters_with_no_privilege_or_inherited_broker_descriptors() {
    let temporary = TestDirectory::new("worker-privilege");
    let state = temporary.path().join("state");
    let checkout = temporary.path().join("checkout");
    fs::create_dir(&checkout).unwrap();
    let report = temporary.path().join("privilege.json");
    let script = r#"
import fcntl
import json
import os
import pathlib
import sys
import time

opened = []
for descriptor in range(3, 256):
    try:
        fcntl.fcntl(descriptor, fcntl.F_GETFD)
    except OSError:
        continue
    opened.append(descriptor)
status = {}
for line in pathlib.Path('/proc/self/status').read_text(encoding='ascii').splitlines():
    name, separator, value = line.partition(':')
    if separator and name in {'CapEff', 'CapPrm', 'CapInh', 'CapAmb', 'NoNewPrivs'}:
        status[name] = value.strip()
pathlib.Path(sys.argv[1]).write_text(
    json.dumps({
        'status': status,
        'open_fds': opened,
        'internal': sorted(name for name in os.environ if name.startswith('_AGCOORD_')),
        'safe': os.environ.get('SAFE_VISIBLE'),
    }),
    encoding='ascii',
)
while not pathlib.Path(sys.argv[2]).exists():
    time.sleep(0.01)
"#;
    let release = temporary.path().join("release");
    let mut broker = RunningBroker::start(&state, &[("jobs", 1)]);
    let submission = Submission {
        run_id: "worker-privilege",
        kind: "check",
        repository: "repo-a",
        checkout: &checkout,
        command: vec![
            "/usr/bin/python3".to_owned(),
            "-c".to_owned(),
            script.to_owned(),
            report.to_str().unwrap().to_owned(),
            release.to_str().unwrap().to_owned(),
        ],
        gate_run_id: None,
    };
    let submitted = submit_with_environment(
        &state,
        &submission,
        &[
            ("_AGCOORD_FORGED", "must-not-escape"),
            ("SAFE_VISIBLE", "kept"),
        ],
    );
    assert!(submitted.status.success());
    wait_for(Duration::from_secs(5), || report.exists());
    let database = Connection::open(state.join("queue.sqlite3")).unwrap();
    let durable_environment: String = database
        .query_row(
            "SELECT environment_json FROM runs WHERE run_id = 'worker-privilege'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(durable_environment, "{}");
    drop(database);
    let report: Value = serde_json::from_str(&fs::read_to_string(&report).unwrap()).unwrap();
    assert_eq!(report["open_fds"], json!([]));
    assert_eq!(report["internal"], json!([]));
    assert_eq!(report["safe"], "kept");
    assert_eq!(report["status"]["CapEff"], "0000000000000000");
    assert_eq!(report["status"]["CapPrm"], "0000000000000000");
    assert_eq!(report["status"]["CapInh"], "0000000000000000");
    assert_eq!(report["status"]["CapAmb"], "0000000000000000");
    assert_eq!(report["status"]["NoNewPrivs"], "1");
    fs::write(&release, "release").unwrap();
    wait_status(&state, "worker-privilege", "passed");
    assert!(broker.terminate().success());
}

#[test]
fn forged_replayed_or_broken_worker_handshakes_fail_closed() {
    let temporary = TestDirectory::new("worker-handshake-faults");
    let checkout = temporary.path().join("checkout");
    fs::create_dir(&checkout).unwrap();
    for fault in [
        "launcher-death",
        "hello-token",
        "replayed-token",
        "substituted-channel",
        "setup-token",
        "privilege-verification",
        "retained-descriptor",
        "final-token",
    ] {
        let state = temporary.path().join(format!("state-{fault}"));
        let marker = temporary.path().join(format!("ran-{fault}"));
        let mut broker = RunningBroker::start_with_worker_fault(&state, &[("jobs", 1)], fault);
        submit_ok(
            &state,
            &Submission {
                run_id: "faulted-worker",
                kind: "check",
                repository: "repo-a",
                checkout: &checkout,
                command: touch_command(&marker),
                gate_run_id: None,
            },
        );
        let failed = wait_status(&state, "faulted-worker", "failed");
        assert_eq!(failed["exit_status"], 125, "fault {fault}");
        assert!(!marker.exists(), "fault {fault} released user code");
        assert!(
            broker.terminate().success(),
            "fault {fault} killed the broker"
        );
    }
}

#[test]
fn cancellation_drains_the_complete_owned_process_group_before_terminal_state() {
    let temporary = TestDirectory::new("worker-tree-cancel");
    let state = temporary.path().join("state");
    let checkout = temporary.path().join("checkout");
    fs::create_dir(&checkout).unwrap();
    let child_pid = temporary.path().join("child.pid");
    let entered = temporary.path().join("entered");
    let script = r#"
/bin/sh -c 'trap "" TERM; i=0; while [ "$i" -lt 300 ]; do sleep 0.1; i=$((i + 1)); done' agcoord-descendant &
printf '%s\n' "$!" >"$1"
touch "$2"
i=0
while [ "$i" -lt 300 ]; do sleep 0.1; i=$((i + 1)); done
"#;
    let mut broker = RunningBroker::start(&state, &[("jobs", 1)]);
    submit_ok(
        &state,
        &Submission {
            run_id: "worker-tree-cancel",
            kind: "check",
            repository: "repo-a",
            checkout: &checkout,
            command: vec![
                "/bin/sh".to_owned(),
                "-c".to_owned(),
                script.to_owned(),
                "agcoord-tree".to_owned(),
                child_pid.to_str().unwrap().to_owned(),
                entered.to_str().unwrap().to_owned(),
            ],
            gate_run_id: None,
        },
    );
    wait_for(Duration::from_secs(5), || entered.exists());
    let descendant: u32 = fs::read_to_string(&child_pid)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    let mut guard = ProcessGuard::new(descendant);
    let cancellation = run(&[
        "cancel",
        "--state-dir",
        state_argument(&state),
        "--run-id",
        "worker-tree-cancel",
    ]);
    assert!(cancellation.status.success());
    wait_status(&state, "worker-tree-cancel", "cancelled");
    wait_for(Duration::from_secs(5), || {
        process_state(u64::from(descendant)).is_none_or(|state| state == "Z")
    });
    guard.disarm();
    assert!(broker.terminate().success());
}

#[test]
fn stale_worker_identity_never_signals_an_unrelated_reused_process_group() {
    let temporary = TestDirectory::new("worker-pid-reuse");
    let state = temporary.path().join("state");
    let checkout = temporary.path().join("checkout");
    fs::create_dir(&checkout).unwrap();
    let entered = temporary.path().join("entered");
    let release = temporary.path().join("release");
    let marker = temporary.path().join("must-not-run");
    let mut bootstrap = RunningBroker::start(&state, &[("jobs", 1)]);
    submit_ok(
        &state,
        &Submission {
            run_id: "capacity-blocker",
            kind: "check",
            repository: "repo-a",
            checkout: &checkout,
            command: blocking_command(&entered, &release, None),
            gate_run_id: None,
        },
    );
    wait_for(Duration::from_secs(5), || entered.exists());
    submit_ok(
        &state,
        &Submission {
            run_id: "stale-worker",
            kind: "check",
            repository: "repo-b",
            checkout: &checkout,
            command: touch_command(&marker),
            gate_run_id: None,
        },
    );
    assert_eq!(status(&state, "stale-worker")["status"], "queued");
    assert!(bootstrap.terminate().success());

    let mut unrelated_command = Command::new("/bin/sleep");
    unrelated_command.arg("30").process_group(0);
    let mut unrelated = ChildGuard(unrelated_command.spawn().unwrap());
    let stale_token = process_start_token(u64::from(unrelated.id()))
        .parse::<u64>()
        .unwrap()
        .saturating_add(1)
        .to_string();
    let database = state.join("queue.sqlite3");
    let db = Connection::open(&database).unwrap();
    db.execute(
        "UPDATE runs SET status = 'running', phase = 'running',
         started_at = '2026-08-31T00:00:00Z', worker_pid = ?1,
         worker_start_token = ?2 WHERE run_id = 'stale-worker'",
        params![i64::from(unrelated.id()), stale_token],
    )
    .unwrap();
    drop(db);

    let mut replacement = RunningBroker::start(&state, &[("jobs", 1)]);
    let recovered = wait_status(&state, "stale-worker", "interrupted");
    assert_eq!(recovered["failure_reason"], "worker-result-lost");
    assert!(
        unrelated.is_running(),
        "the broker signalled a process group whose start token did not match"
    );
    assert!(!marker.exists());
    assert!(replacement.terminate().success());
}

#[test]
fn cancellation_commit_crash_preserves_the_request_for_the_owner() {
    let temporary = TestDirectory::new("crash-cancel");
    let state = temporary.path().join("state");
    let checkout = temporary.path().join("checkout");
    fs::create_dir(&checkout).unwrap();
    let entered = temporary.path().join("entered");
    let release = temporary.path().join("release");
    let mut broker = RunningBroker::start(&state, &[("jobs", 1)]);
    submit_ok(
        &state,
        &Submission {
            run_id: "cancel-crash",
            kind: "check",
            repository: "repo-a",
            checkout: &checkout,
            command: blocking_command(&entered, &release, None),
            gate_run_id: None,
        },
    );
    wait_for(Duration::from_secs(5), || entered.exists());
    let cancellation = run(&[
        "cancel",
        "--state-dir",
        state_argument(&state),
        "--run-id",
        "cancel-crash",
        "--crash-after",
        "cancel-commit",
    ]);
    assert_eq!(cancellation.status.code(), Some(86));
    let finished = wait_status(&state, "cancel-crash", "cancelled");
    assert_eq!(finished["exit_status"], 130);
    assert!(broker.terminate().success());
}

#[test]
fn land_phase_commit_is_the_atomic_cancellation_authority_boundary() {
    let temporary = TestDirectory::new("land-phase");
    let state = temporary.path().join("state");
    let checkout = temporary.path().join("checkout");
    fs::create_dir(&checkout).unwrap();
    let entered = temporary.path().join("entered");
    let release = temporary.path().join("release");
    let mut broker = RunningBroker::start(&state, &[("jobs", 1)]);
    submit_ok(
        &state,
        &Submission {
            run_id: "authoritative-land",
            kind: "land",
            repository: "repo-a",
            checkout: &checkout,
            command: blocking_command(&entered, &release, None),
            gate_run_id: None,
        },
    );
    wait_for(Duration::from_secs(5), || entered.exists());
    let preflight = status(&state, "authoritative-land");
    assert_eq!(preflight["phase"], "preflight");

    let gating = advance_land_phase(&state, "authoritative-land", &preflight, "gating");
    assert!(gating.status.success());
    let gating_row: Value = serde_json::from_slice(&gating.stdout).unwrap();
    assert_eq!(gating_row["phase"], "gating");
    let backwards = advance_land_phase(&state, "authoritative-land", &gating_row, "preflight");
    assert!(!backwards.status.success());
    assert_eq!(
        serde_json::from_slice::<Value>(&backwards.stderr).unwrap()["code"],
        "broker-land-phase-invalid"
    );
    let publishing = advance_land_phase(&state, "authoritative-land", &gating_row, "publishing");
    assert!(publishing.status.success());
    let publishing_row: Value = serde_json::from_slice(&publishing.stdout).unwrap();
    assert_eq!(publishing_row["phase"], "publishing");
    assert_eq!(publishing_row["gate_exit_status"], 0);

    let cancellation = run(&[
        "cancel",
        "--state-dir",
        state_argument(&state),
        "--run-id",
        "authoritative-land",
    ]);
    assert!(!cancellation.status.success());
    assert_eq!(
        serde_json::from_slice::<Value>(&cancellation.stderr).unwrap()["code"],
        "broker-publication-authoritative"
    );
    fs::write(&release, "release").unwrap();
    let finished = wait_status(&state, "authoritative-land", "passed");
    assert_eq!(finished["phase"], "complete");
    assert_eq!(finished["gate_exit_status"], 0);
    assert!(broker.terminate().success());
}

#[test]
fn a_dead_land_worker_identity_cannot_advance_publication() {
    let temporary = TestDirectory::new("dead-land-worker");
    let state = temporary.path().join("state");
    let checkout = temporary.path().join("checkout");
    fs::create_dir(&checkout).unwrap();
    let entered = temporary.path().join("entered");
    let release = temporary.path().join("release");
    let mut broker = RunningBroker::start(&state, &[("jobs", 1)]);
    submit_ok(
        &state,
        &Submission {
            run_id: "dead-land-worker",
            kind: "land",
            repository: "repo-a",
            checkout: &checkout,
            command: blocking_command(&entered, &release, None),
            gate_run_id: None,
        },
    );
    wait_for(Duration::from_secs(5), || entered.exists());
    let row = status(&state, "dead-land-worker");
    let worker_pid = row["worker_pid"].as_u64().unwrap();
    let worker_token = process_start_token(worker_pid);
    let broker_pid = broker.child.as_ref().unwrap().id().to_string();
    assert!(
        Command::new("/bin/kill")
            .args(["-STOP", &broker_pid])
            .status()
            .unwrap()
            .success()
    );
    assert!(
        Command::new("/bin/kill")
            .args(["-KILL", "--", &format!("-{worker_pid}")])
            .status()
            .unwrap()
            .success()
    );
    wait_for(Duration::from_secs(5), || {
        process_state(worker_pid).is_none_or(|state| state == "Z")
    });

    let worker_pid = worker_pid.to_string();
    let refused = run(&[
        "phase",
        "--state-dir",
        state_argument(&state),
        "--run-id",
        "dead-land-worker",
        "--worker-pid",
        &worker_pid,
        "--worker-start-token",
        &worker_token,
        "--checkout",
        row["checkout"].as_str().unwrap(),
        "--head",
        row["head_sha"].as_str().unwrap(),
        "--phase",
        "gating",
    ]);
    assert!(!refused.status.success());
    assert_eq!(
        serde_json::from_slice::<Value>(&refused.stderr).unwrap()["code"],
        "broker-land-identity-mismatch"
    );

    assert!(
        Command::new("/bin/kill")
            .args(["-CONT", &broker_pid])
            .status()
            .unwrap()
            .success()
    );
    wait_status(&state, "dead-land-worker", "failed");
    assert!(broker.terminate().success());
}

#[test]
fn migration_and_rollback_preserve_history_and_reject_stale_gates() {
    let temporary = TestDirectory::new("migration");
    let state = temporary.path().join("state");
    let checkout = temporary.path().join("checkout");
    fs::create_dir(&checkout).unwrap();
    let mut bootstrap = RunningBroker::start(&state, &[("jobs", 1)]);
    assert!(bootstrap.terminate().success());

    let database = state.join("queue.sqlite3");
    let db = Connection::open(&database).unwrap();
    db.execute("DELETE FROM coordinator_meta WHERE key != 'protocol'", [])
        .unwrap();
    db.execute(
        "UPDATE coordinator_meta SET value = '4' WHERE key = 'protocol'",
        [],
    )
    .unwrap();
    db.execute(
        "INSERT INTO runs (
            run_id, status, kind, phase, label, agent, repository_id, repository,
            worktree_id, checkout, branch, head_sha, barrier, resources_json,
            resource_contract_json, resource_receipt_json, resource_state_json,
            caller_pid, command_json, environment_json, created_at, started_at,
            finished_at, exit_status
         ) VALUES (
            'legacy-full', 'passed', 'full', 'complete', 'legacy gate', 'agent',
            'repo-a', 'repo-a', 'worktree-a', ?1, 'ticket',
            'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 1, '{\"jobs\":1}',
            '{\"jobs\":{\"backend\":null,\"kind\":\"generic\",\"mode\":\"admission-only\",\"unit\":\"admission-unit\"}}',
            '{\"requested\":{\"jobs\":1},\"applied\":{},\"peak\":{},\"events\":[]}',
            '{}', 42, '[\"true\"]', '{}', '2026-08-31T00:00:00Z',
            '2026-08-31T00:00:01Z', '2026-08-31T00:00:02Z', 0
         )",
        params![checkout.to_str().unwrap()],
    )
    .unwrap();
    drop(db);

    let migrated = json_output(&["migrate", "--state-dir", state_argument(&state)]);
    assert_eq!(
        migrated,
        json!({"changed": true, "from_protocol": 4, "to_protocol": 5})
    );
    assert!(state.join("queue.sqlite3.protocol4.bak").is_file());
    let mut broker = RunningBroker::start(&state, &[("jobs", 1)]);
    assert_eq!(status(&state, "legacy-full")["status"], "passed");

    let stale = submit(
        &state,
        &Submission {
            run_id: "stale-merge",
            kind: "merge",
            repository: "repo-a",
            checkout: &checkout,
            command: vec!["/usr/bin/true".to_owned()],
            gate_run_id: Some("legacy-full"),
        },
    );
    assert!(!stale.status.success());
    assert_eq!(
        serde_json::from_slice::<Value>(&stale.stderr).unwrap()["code"],
        "stale-gate-verdict"
    );
    submit_ok(
        &state,
        &Submission {
            run_id: "native-full",
            kind: "full",
            repository: "repo-b",
            checkout: &checkout,
            command: vec!["/usr/bin/true".to_owned()],
            gate_run_id: None,
        },
    );
    let native_full = wait_status(&state, "native-full", "passed");
    assert!(broker.terminate().success());
    let db = Connection::open(&database).unwrap();
    db.execute(
        "UPDATE runs SET label = 'tampered after backup' WHERE run_id = 'legacy-full'",
        [],
    )
    .unwrap();
    drop(db);

    let rolled_back = json_output(&["rollback", "--state-dir", state_argument(&state)]);
    assert_eq!(
        rolled_back,
        json!({"changed": true, "from_protocol": 5, "to_protocol": 4})
    );
    let db = Connection::open(&database).unwrap();
    assert_eq!(
        db.query_row(
            "SELECT label FROM runs WHERE run_id = 'legacy-full'",
            [],
            |row| row.get::<_, String>(0)
        )
        .unwrap(),
        "legacy gate"
    );
    assert_eq!(
        db.query_row(
            "SELECT status FROM runs WHERE run_id = 'native-full'",
            [],
            |row| row.get::<_, String>(0)
        )
        .unwrap(),
        "passed"
    );
    assert!(
        db.query_row(
            "SELECT value FROM coordinator_meta
             WHERE key = 'invalid_gate_through_sequence'",
            [],
            |row| row.get::<_, String>(0)
        )
        .unwrap()
        .parse::<i64>()
        .unwrap()
            >= native_full["sequence"].as_i64().unwrap()
    );
    assert_eq!(
        db.query_row(
            "SELECT value FROM coordinator_meta WHERE key = 'protocol'",
            [],
            |row| row.get::<_, String>(0)
        )
        .unwrap(),
        "4"
    );
}

#[test]
fn every_legacy_protocol_migrates_through_protocol_four_without_inventing_evidence() {
    for legacy_protocol in 1..=3 {
        let temporary = TestDirectory::new(&format!("migration-{legacy_protocol}"));
        let state = temporary.path().join("state");
        let checkout = temporary.path().join("checkout");
        fs::create_dir(&checkout).unwrap();
        create_legacy_database(&state, &checkout, legacy_protocol);

        let migrated = json_output(&["migrate", "--state-dir", state_argument(&state)]);
        assert_eq!(
            migrated,
            json!({
                "changed": true,
                "from_protocol": legacy_protocol,
                "to_protocol": 5,
            })
        );
        assert!(state.join("queue.sqlite3.protocol4.bak").is_file());
        let mut broker = RunningBroker::start(&state, &[("jobs", 1)]);
        let historical = status(&state, "legacy-full");
        assert_eq!(historical["status"], "passed");
        assert_eq!(historical["phase"], "complete");
        assert_eq!(historical["gate_exit_status"], Value::Null);
        assert_eq!(
            historical["resource_contract"],
            json!({
                "jobs": {
                    "backend": null,
                    "kind": "generic",
                    "mode": "admission-only",
                    "unit": "admission-unit",
                }
            })
        );
        assert_eq!(
            historical["resource_receipt"],
            json!({
                "requested": {"jobs": 1},
                "applied": {},
                "peak": {},
                "events": [],
            })
        );
        assert!(broker.terminate().success());

        assert_eq!(
            json_output(&["rollback", "--state-dir", state_argument(&state)]),
            json!({"changed": true, "from_protocol": 5, "to_protocol": 4})
        );
        let db = Connection::open(state.join("queue.sqlite3")).unwrap();
        assert_eq!(
            db.query_row(
                "SELECT value FROM coordinator_meta WHERE key = 'protocol'",
                [],
                |row| row.get::<_, String>(0),
            )
            .unwrap(),
            "4"
        );
        assert_eq!(
            db.query_row(
                "SELECT status FROM runs WHERE run_id = 'legacy-full'",
                [],
                |row| row.get::<_, String>(0),
            )
            .unwrap(),
            "passed"
        );
    }
}

#[test]
fn migration_refuses_an_existing_corrupt_backup_before_protocol_transition() {
    let temporary = TestDirectory::new("migration-corrupt-backup");
    let state = temporary.path().join("state");
    let checkout = temporary.path().join("checkout");
    fs::create_dir(&checkout).unwrap();
    let mut broker = RunningBroker::start(&state, &[("jobs", 1)]);
    submit_ok(
        &state,
        &Submission {
            run_id: "backup-history",
            kind: "full",
            repository: "repo-a",
            checkout: &checkout,
            command: vec!["/usr/bin/true".to_owned()],
            gate_run_id: None,
        },
    );
    wait_status(&state, "backup-history", "passed");
    assert!(broker.terminate().success());

    let database = state.join("queue.sqlite3");
    let backup = state.join("queue.sqlite3.protocol4.bak");
    let db = Connection::open(&database).unwrap();
    db.execute(
        "UPDATE coordinator_meta SET value = '4' WHERE key = 'protocol'",
        [],
    )
    .unwrap();
    db.execute(
        "DELETE FROM coordinator_meta WHERE key IN
         ('owner_implementation', 'schema_fingerprint', 'native_gate_floor')",
        [],
    )
    .unwrap();
    db.execute("VACUUM INTO ?1", params![backup.to_str().unwrap()])
        .unwrap();
    drop(db);
    let backup_db = Connection::open(&backup).unwrap();
    backup_db
        .execute(
            "UPDATE runs SET resources_json = 'not-json' WHERE run_id = 'backup-history'",
            [],
        )
        .unwrap();
    drop(backup_db);

    let refused = run(&["migrate", "--state-dir", state_argument(&state)]);
    assert!(!refused.status.success());
    assert_eq!(
        serde_json::from_slice::<Value>(&refused.stderr).unwrap()["code"],
        "broker-migration-backup-invalid"
    );
    let db = Connection::open(&database).unwrap();
    assert_eq!(
        db.query_row(
            "SELECT value FROM coordinator_meta WHERE key = 'protocol'",
            [],
            |row| row.get::<_, String>(0),
        )
        .unwrap(),
        "4"
    );
}

#[test]
fn each_migration_cycle_uses_a_fresh_protocol_four_rollback_baseline() {
    let temporary = TestDirectory::new("migration-fresh-backup");
    let state = temporary.path().join("state");
    let checkout = temporary.path().join("checkout");
    fs::create_dir(&checkout).unwrap();
    create_legacy_database(&state, &checkout, 3);

    json_output(&["migrate", "--state-dir", state_argument(&state)]);
    json_output(&["rollback", "--state-dir", state_argument(&state)]);
    let database = state.join("queue.sqlite3");
    let db = Connection::open(&database).unwrap();
    db.execute("DELETE FROM runs WHERE run_id = 'legacy-full'", [])
        .unwrap();
    drop(db);

    json_output(&["migrate", "--state-dir", state_argument(&state)]);
    json_output(&["rollback", "--state-dir", state_argument(&state)]);
    let db = Connection::open(&database).unwrap();
    assert_eq!(
        db.query_row(
            "SELECT COUNT(*) FROM runs WHERE run_id = 'legacy-full'",
            [],
            |row| row.get::<_, i64>(0),
        )
        .unwrap(),
        0
    );
    let backup_count = fs::read_dir(&state)
        .unwrap()
        .filter_map(std::result::Result::ok)
        .filter(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .starts_with("queue.sqlite3.protocol4")
        })
        .count();
    assert!(backup_count >= 2);
}

#[test]
fn migration_requires_a_complete_wal_checkpoint_before_backup() {
    let temporary = TestDirectory::new("migration-checkpoint");
    let state = temporary.path().join("state");
    fs::create_dir(&state).unwrap();
    fs::write(state.join("config.json"), r#"{"database_timeout":0.05}"#).unwrap();
    let mut broker = RunningBroker::start(&state, &[("jobs", 1)]);
    assert!(broker.terminate().success());
    let database = state.join("queue.sqlite3");
    let setup = Connection::open(&database).unwrap();
    setup
        .execute(
            "UPDATE coordinator_meta SET value = '4' WHERE key = 'protocol'",
            [],
        )
        .unwrap();
    setup
        .execute(
            "DELETE FROM coordinator_meta WHERE key IN
             ('owner_implementation', 'schema_fingerprint', 'native_gate_floor')",
            [],
        )
        .unwrap();
    drop(setup);

    let reader = Connection::open(&database).unwrap();
    reader.execute_batch("BEGIN").unwrap();
    reader
        .query_row("SELECT COUNT(*) FROM runs", [], |row| row.get::<_, i64>(0))
        .unwrap();
    let writer = Connection::open(&database).unwrap();
    writer
        .execute(
            "INSERT INTO coordinator_meta(key, value) VALUES ('checkpoint-fixture', 'new')",
            [],
        )
        .unwrap();
    drop(writer);

    let refused = run(&["migrate", "--state-dir", state_argument(&state)]);
    reader.execute_batch("ROLLBACK").unwrap();
    assert!(!refused.status.success());
    assert_eq!(
        serde_json::from_slice::<Value>(&refused.stderr).unwrap()["code"],
        "broker-migration-backup-failed"
    );
    let db = Connection::open(&database).unwrap();
    assert_eq!(
        db.query_row(
            "SELECT value FROM coordinator_meta WHERE key = 'protocol'",
            [],
            |row| row.get::<_, String>(0),
        )
        .unwrap(),
        "4"
    );
}
