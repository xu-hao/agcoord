use rusqlite::{Connection, params};
use serde_json::{Value, json};
use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const BROKER: &str = env!("CARGO_BIN_EXE_agcoord-broker");

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

struct RunningBroker {
    child: Option<Child>,
}

impl RunningBroker {
    fn start(state_dir: &Path, capacities: &[(&str, u64)]) -> Self {
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
    let mut selected = None;
    wait_for(Duration::from_secs(10), || {
        let row = status(state_dir, run_id);
        if row["status"] == expected {
            selected = Some(row);
            true
        } else {
            false
        }
    });
    selected.unwrap()
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
    ];
    if let Some(gate_run_id) = submission.gate_run_id {
        arguments.extend(["--gate-run-id".to_owned(), gate_run_id.to_owned()]);
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
    wait_for(Duration::from_secs(5), || marker.exists());
    thread::sleep(Duration::from_millis(150));
    locker.execute_batch("ROLLBACK").unwrap();

    let completed = wait_status(&state, "identity-contention", "passed");
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
    let entered = temporary.path().join("entered");
    let release = temporary.path().join("release");
    let starts = temporary.path().join("starts");
    let mut crashing = start_crashing_broker(&state, "worker-identity-commit");
    submit_ok(
        &state,
        &Submission {
            run_id: "identity-crash",
            kind: "check",
            repository: "repo-a",
            checkout: &checkout,
            command: blocking_command(&entered, &release, Some(&starts)),
            gate_run_id: None,
        },
    );
    assert_eq!(crashing.wait().unwrap().code(), Some(86));
    wait_for(Duration::from_secs(5), || entered.exists());
    let original_pid = status(&state, "identity-crash")["worker_pid"].clone();
    let mut replacement = RunningBroker::start(&state, &[("jobs", 1)]);
    assert_eq!(status(&state, "identity-crash")["worker_pid"], original_pid);
    assert_eq!(fs::read_to_string(&starts).unwrap().lines().count(), 1);
    fs::write(&release, "release").unwrap();
    let recovered = wait_status(&state, "identity-crash", "interrupted");
    assert_eq!(recovered["failure_reason"], "worker-result-lost");
    assert_eq!(fs::read_to_string(&starts).unwrap().lines().count(), 1);
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
