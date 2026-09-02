use serde_json::json;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
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
            "agcoord-native-client-{name}-{}-{nonce}",
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

struct BrokerGuard(u32);

impl Drop for BrokerGuard {
    fn drop(&mut self) {
        if !Path::new(&format!("/proc/{}", self.0)).exists() {
            return;
        }
        let _ = Command::new("/bin/kill")
            .args(["-TERM", &self.0.to_string()])
            .status();
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if !Path::new(&format!("/proc/{}", self.0)).exists() {
                return;
            }
            thread::sleep(Duration::from_millis(20));
        }
        let _ = Command::new("/bin/kill")
            .args(["-KILL", &self.0.to_string()])
            .status();
    }
}

struct StateOwnerGuard(PathBuf);

impl Drop for StateOwnerGuard {
    fn drop(&mut self) {
        if let Some((guard, _owner)) = owner_guard(&self.0) {
            drop(guard);
        }
    }
}

struct ReleaseGuard(PathBuf);

impl Drop for ReleaseGuard {
    fn drop(&mut self) {
        let _ = fs::write(&self.0, "release\n");
    }
}

fn repository_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf()
}

fn git(checkout: &Path, arguments: &[&str]) {
    let result = Command::new("git")
        .args(arguments)
        .current_dir(checkout)
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "git {:?} failed: {}",
        arguments,
        String::from_utf8_lossy(&result.stderr)
    );
}

fn git_output(checkout: &Path, arguments: &[&str]) -> String {
    let result = Command::new("git")
        .args(arguments)
        .current_dir(checkout)
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "git {:?} failed: {}",
        arguments,
        String::from_utf8_lossy(&result.stderr)
    );
    String::from_utf8(result.stdout).unwrap().trim().to_owned()
}

fn initialize_checkout(path: &Path) {
    fs::create_dir(path).unwrap();
    git(path, &["init", "-q"]);
    git(path, &["config", "user.name", "Native Client Test"]);
    git(
        path,
        &["config", "user.email", "native-client@example.invalid"],
    );
    fs::write(path.join("tracked.txt"), "tracked\n").unwrap();
    git(path, &["add", "tracked.txt"]);
    git(path, &["commit", "-q", "-m", "initial"]);
}

fn python_cli(arguments: &[String]) -> Output {
    python_cli_with_env(arguments, &[])
}

fn python_cli_with_env(arguments: &[String], environment: &[(&str, &str)]) -> Output {
    let mut command = Command::new("python3");
    command
        .args(["-m", "agcoord"])
        .args(arguments)
        .env("PYTHONPATH", repository_root().join("src"))
        .env_remove("AGCOORD_RUN_ID")
        .env_remove("AGCOORD_RUN_KIND")
        .env_remove("AGCOORD_STATE_DIR");
    for (name, value) in environment {
        command.env(name, value);
    }
    command.output().unwrap()
}

fn python_command(arguments: &[String]) -> Output {
    Command::new("python3")
        .args(arguments)
        .env("PYTHONPATH", repository_root().join("src"))
        .env_remove("AGCOORD_RUN_ID")
        .env_remove("AGCOORD_RUN_KIND")
        .env_remove("AGCOORD_STATE_DIR")
        .output()
        .unwrap()
}

fn installed_broker(temporary: &TestDirectory) -> PathBuf {
    let selected = temporary.path().join("agcoord-broker");
    fs::copy(BROKER, &selected).unwrap();
    fs::set_permissions(&selected, fs::Permissions::from_mode(0o755)).unwrap();
    selected
}

fn write_config(state: &Path, broker: &Path, capacities: serde_json::Value) {
    fs::create_dir_all(state).unwrap();
    fs::write(
        state.join("config.json"),
        serde_json::to_vec(&json!({
            "capacities": capacities,
            "native_broker": {
                "path": broker,
                "allow_development": true
            }
        }))
        .unwrap(),
    )
    .unwrap();
}

fn assert_success(result: &Output, subject: &str) {
    assert!(
        result.status.success(),
        "{subject} failed:\nstdout={}\nstderr={}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
}

fn parse_json_output(result: &Output) -> serde_json::Value {
    serde_json::from_slice(&result.stdout).unwrap_or_else(|error| {
        panic!(
            "invalid JSON output ({error}): {}",
            String::from_utf8_lossy(&result.stdout)
        )
    })
}

fn owner_guard(state: &Path) -> Option<(BrokerGuard, String)> {
    let raw = fs::read_to_string(state.join("broker.lock")).ok()?;
    let pid = raw
        .lines()
        .find_map(|line| line.strip_prefix("pid="))?
        .parse()
        .ok()?;
    Some((BrokerGuard(pid), raw))
}

#[test]
fn python_cli_autostarts_the_selected_native_broker_and_runs_a_check() {
    let temporary = TestDirectory::new("autostart");
    let state = temporary.path().join("state");
    let checkout = temporary.path().join("checkout");
    let marker = temporary.path().join("ran.txt");
    let selected_broker = installed_broker(&temporary);
    initialize_checkout(&checkout);
    write_config(&state, &selected_broker, json!({"jobs": 1}));

    let result = python_cli(&[
        "--state-dir".to_owned(),
        state.to_string_lossy().into_owned(),
        "run".to_owned(),
        "--checkout".to_owned(),
        checkout.to_string_lossy().into_owned(),
        "--resource".to_owned(),
        "jobs=1".to_owned(),
        "--".to_owned(),
        "/bin/sh".to_owned(),
        "-c".to_owned(),
        "printf 'native client\\n' >\"$1\"".to_owned(),
        "agcoord-test".to_owned(),
        marker.to_string_lossy().into_owned(),
    ]);
    let owner = owner_guard(&state);
    assert!(
        result.status.success(),
        "client failed:\nstdout={}\nstderr={}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
    assert_eq!(fs::read_to_string(&marker).unwrap(), "native client\n");
    let (_guard, owner) = owner.expect("client did not start a broker");
    assert!(owner.contains("protocol=5\n"));
    assert!(owner.contains("implementation=rust-native\n"));
}

#[test]
fn python_public_commands_keep_the_protocol_five_json_contract() {
    let temporary = TestDirectory::new("commands");
    let state = temporary.path().join("state");
    let checkout = temporary.path().join("checkout");
    let selected_broker = installed_broker(&temporary);
    initialize_checkout(&checkout);
    write_config(&state, &selected_broker, json!({"jobs": 1}));

    let check = python_cli(&[
        "--json".to_owned(),
        "--state-dir".to_owned(),
        state.to_string_lossy().into_owned(),
        "run".to_owned(),
        "--label".to_owned(),
        "native command contract".to_owned(),
        "--checkout".to_owned(),
        checkout.to_string_lossy().into_owned(),
        "--".to_owned(),
        "/bin/sh".to_owned(),
        "-c".to_owned(),
        "printf 'native transcript\\n'".to_owned(),
    ]);
    assert_success(&check, "native check");
    let check_row = parse_json_output(&check);
    assert_eq!(check_row["status"], "passed");
    assert_eq!(check_row["kind"], "check");
    assert!(check_row["caller_pid"].as_u64().is_some());
    let run_id = check_row["run_id"].as_str().unwrap().to_owned();

    let shown = python_cli(&[
        "--state-dir".to_owned(),
        state.to_string_lossy().into_owned(),
        "show".to_owned(),
        run_id.clone(),
    ]);
    assert_success(&shown, "native show");
    assert_eq!(parse_json_output(&shown)["run_id"], run_id);

    let log = python_cli(&[
        "--json".to_owned(),
        "--state-dir".to_owned(),
        state.to_string_lossy().into_owned(),
        "log".to_owned(),
        run_id.clone(),
    ]);
    assert_success(&log, "native log");
    let log_page = parse_json_output(&log);
    assert_eq!(log_page["text"], "native transcript\n");
    assert_eq!(log_page["eof"], true);

    let full = python_cli(&[
        "--json".to_owned(),
        "--state-dir".to_owned(),
        state.to_string_lossy().into_owned(),
        "full".to_owned(),
        "--checkout".to_owned(),
        checkout.to_string_lossy().into_owned(),
        "--".to_owned(),
        "/bin/true".to_owned(),
    ]);
    assert_success(&full, "native full");
    assert_eq!(parse_json_output(&full)["status"], "passed");

    let listed = python_cli(&[
        "--json".to_owned(),
        "--state-dir".to_owned(),
        state.to_string_lossy().into_owned(),
        "list".to_owned(),
    ]);
    assert_success(&listed, "native list");
    let snapshot = parse_json_output(&listed);
    assert_eq!(snapshot["protocol"], 5);
    assert_eq!(snapshot["active"], json!([]));
    assert_eq!(snapshot["queued"], json!([]));
    assert_eq!(snapshot["recent"].as_array().unwrap().len(), 2);

    let cleared = python_cli(&[
        "--json".to_owned(),
        "--state-dir".to_owned(),
        state.to_string_lossy().into_owned(),
        "clear".to_owned(),
    ]);
    assert_success(&cleared, "native clear");
    assert_eq!(parse_json_output(&cleared), json!({"cleared": 2}));

    let (_guard, owner) = owner_guard(&state).expect("native owner disappeared");
    assert!(owner.contains("protocol=5\n"));
}

#[test]
fn native_broker_reclaims_a_crashed_child_cpu_controller_and_returns_tokens() {
    let temporary = TestDirectory::new("leases");
    let state = temporary.path().join("state");
    let checkout = temporary.path().join("checkout");
    let report = temporary.path().join("lease-report.json");
    let marker = temporary.path().join("crashed-controller-acquired");
    let selected_broker = installed_broker(&temporary);
    initialize_checkout(&checkout);
    write_config(&state, &selected_broker, json!({"jobs": 1, "cpu": 2}));

    let source = r#"
import json
import os
from pathlib import Path
import subprocess
import sys

from agcoord.queue import CoordinatorClient

state, report, marker = sys.argv[1:]
child_source = r'''
import os
from pathlib import Path
from agcoord.queue import CoordinatorClient

client = CoordinatorClient(state_dir=os.environ["AGCOORD_STATE_DIR"], autostart=False)
lease = client.acquire_child_cpu_lease(2, timeout=5)
Path(os.environ["LEASE_MARKER"]).touch()
os._exit(17)
'''
environment = dict(os.environ)
environment["LEASE_MARKER"] = marker
controller = subprocess.Popen([sys.executable, "-c", child_source], env=environment)
if controller.wait(timeout=10) != 17:
    raise AssertionError("crash controller returned an unexpected status")
if not Path(marker).exists():
    raise AssertionError("crash controller never acquired its lease")

client = CoordinatorClient(state_dir=state, autostart=False)
with client.acquire_child_cpu_lease(2, timeout=5) as recovered:
    grant = {"granted": recovered.granted, "full": recovered.full}
leases = client.child_cpu_leases(os.environ["AGCOORD_RUN_ID"], include_terminal=True)
Path(report).write_text(json.dumps({
    "grant": grant,
    "statuses": sorted(lease["status"] for lease in leases),
}), encoding="utf-8")
"#;
    let result = python_cli(&[
        "--json".to_owned(),
        "--state-dir".to_owned(),
        state.to_string_lossy().into_owned(),
        "run".to_owned(),
        "--checkout".to_owned(),
        checkout.to_string_lossy().into_owned(),
        "--resource".to_owned(),
        "cpu=2".to_owned(),
        "--".to_owned(),
        "python3".to_owned(),
        "-c".to_owned(),
        source.to_owned(),
        state.to_string_lossy().into_owned(),
        report.to_string_lossy().into_owned(),
        marker.to_string_lossy().into_owned(),
    ]);
    assert_success(&result, "native child CPU lease run");
    assert_eq!(parse_json_output(&result)["status"], "passed");
    let observed: serde_json::Value = serde_json::from_slice(&fs::read(&report).unwrap()).unwrap();
    assert_eq!(observed["grant"], json!({"granted": 2, "full": true}));
    assert_eq!(observed["statuses"], json!(["cancelled", "released"]));

    let (_guard, _owner) = owner_guard(&state).expect("native owner disappeared");
}

#[test]
fn atomic_land_keeps_gate_and_publication_inside_one_native_admission() {
    let temporary = TestDirectory::new("land");
    let state = temporary.path().join("state");
    let checkout = temporary.path().join("checkout");
    let target_checkout = temporary.path().join("target-checkout");
    let remote = temporary.path().join("origin.git");
    let bin = temporary.path().join("bin");
    let events = temporary.path().join("events.txt");
    let selected_broker = installed_broker(&temporary);
    fs::create_dir(&bin).unwrap();
    git(
        temporary.path(),
        &["init", "--bare", remote.to_str().unwrap()],
    );
    initialize_checkout(&checkout);
    git(
        &checkout,
        &["remote", "add", "origin", remote.to_str().unwrap()],
    );
    git(&checkout, &["branch", "-M", "main"]);
    git(&checkout, &["push", "-u", "origin", "main"]);
    git(&checkout, &["switch", "-c", "feature/native-land"]);
    fs::write(checkout.join("tracked.txt"), "candidate\n").unwrap();
    git(&checkout, &["add", "tracked.txt"]);
    git(&checkout, &["commit", "-q", "-m", "candidate"]);
    git(&checkout, &["push", "-u", "origin", "feature/native-land"]);
    let head = git_output(&checkout, &["rev-parse", "HEAD"]);
    git(
        temporary.path(),
        &[
            "clone",
            "--branch",
            "main",
            remote.to_str().unwrap(),
            target_checkout.to_str().unwrap(),
        ],
    );
    git(
        &target_checkout,
        &["config", "user.name", "AGCoord target test"],
    );
    git(
        &target_checkout,
        &["config", "user.email", "target@example.invalid"],
    );
    fs::write(target_checkout.join("target.txt"), "advanced\n").unwrap();
    git(&target_checkout, &["add", "target.txt"]);
    git(&target_checkout, &["commit", "-q", "-m", "advance target"]);
    let advanced_main = git_output(&target_checkout, &["rev-parse", "HEAD"]);
    git(&target_checkout, &["push", "origin", "main"]);
    write_config(&state, &selected_broker, json!({"jobs": 1}));

    let gh = bin.join("gh");
    fs::write(
        &gh,
        r#"#!/usr/bin/env python3
import json
import os
from pathlib import Path
import subprocess
import sys

arguments = sys.argv[1:]
raw_input = sys.stdin.read()
payload = json.loads(raw_input) if raw_input else None

if arguments[:2] == ["pr", "view"]:
    observed = subprocess.run(
        ["git", "ls-remote", "origin", f"refs/heads/{os.environ['AGCOORD_TEST_BRANCH']}"],
        check=True,
        text=True,
        capture_output=True,
    ).stdout.strip()
    print(json.dumps({
        "number": int(arguments[2]),
        "state": "OPEN",
        "isDraft": False,
        "baseRefName": "main",
        "headRefName": os.environ["AGCOORD_TEST_BRANCH"],
        "headRefOid": observed.split()[0],
        "isCrossRepository": False,
        "title": "Native atomic land",
        "headRepositoryOwner": {"login": "native-test"},
    }))
elif arguments[:2] == ["repo", "view"]:
    print(json.dumps({"id": "R_native_test", "nameWithOwner": "test/repository"}))
elif arguments[:3] == ["api", "--method", "POST"]:
    print(json.dumps({
        "sha": "c" * 40,
        "tree": {"sha": payload["tree"]},
        "parents": [{"sha": parent} for parent in payload["parents"]],
    }))
elif arguments[:2] == ["api", "graphql"]:
    with Path(os.environ["AGCOORD_TEST_EVENTS"]).open("a", encoding="utf-8") as stream:
        stream.write("publish\n")
    mutation = payload["variables"]["input"]["clientMutationId"]
    print(json.dumps({"data": {"updateRefs": {"clientMutationId": mutation}}}))
elif arguments[:1] == ["api"] and "/compare/" in arguments[1]:
    print(json.dumps({"merge_base_commit": {"sha": "0" * 40}}))
else:
    print(f"unexpected gh arguments: {arguments!r}", file=sys.stderr)
    raise SystemExit(93)
"#,
    )
    .unwrap();
    fs::set_permissions(&gh, fs::Permissions::from_mode(0o755)).unwrap();
    let path = format!(
        "{}:{}",
        bin.to_string_lossy(),
        std::env::var("PATH").unwrap_or_else(|_| "/usr/bin:/bin".to_owned())
    );
    let gate = format!("printf 'gate\\n' >>'{}'", events.to_string_lossy());
    let result = python_cli_with_env(
        &[
            "--json".to_owned(),
            "--state-dir".to_owned(),
            state.to_string_lossy().into_owned(),
            "land".to_owned(),
            "123".to_owned(),
            "--checkout".to_owned(),
            checkout.to_string_lossy().into_owned(),
            "--label".to_owned(),
            "native atomic land".to_owned(),
            "--".to_owned(),
            "/bin/sh".to_owned(),
            "-c".to_owned(),
            gate,
        ],
        &[
            ("PATH", &path),
            ("AGCOORD_TEST_BRANCH", "feature/native-land"),
            ("AGCOORD_TEST_HEAD", &head),
            ("AGCOORD_TEST_EVENTS", events.to_str().unwrap()),
        ],
    );
    assert_success(&result, "native atomic land");
    let row = parse_json_output(&result);
    assert_eq!(row["status"], "passed");
    assert_eq!(row["kind"], "land");
    assert_eq!(row["phase"], "complete");
    assert_eq!(row["gate_exit_status"], 0);
    let synchronized_head = row["head_sha"].as_str().unwrap();
    assert_ne!(synchronized_head, head);
    assert_eq!(
        git_output(&checkout, &["rev-parse", "HEAD"]),
        synchronized_head
    );
    assert_eq!(
        git_output(&checkout, &["show", "-s", "--format=%P", synchronized_head])
            .split_whitespace()
            .collect::<Vec<_>>(),
        vec![head.as_str(), advanced_main.as_str()]
    );
    assert_eq!(
        git_output(
            &checkout,
            &["ls-remote", "origin", "refs/heads/feature/native-land"]
        )
        .split_whitespace()
        .next()
        .unwrap(),
        synchronized_head
    );
    assert_eq!(
        row["publication"],
        json!({"adapter": "github", "request": 123})
    );
    assert_eq!(
        row["command"],
        json!([
            "/bin/sh",
            "-c",
            format!("printf 'gate\\n' >>'{}'", events.to_string_lossy())
        ])
    );
    assert_eq!(fs::read_to_string(&events).unwrap(), "gate\npublish\n");

    let (_guard, _owner) = owner_guard(&state).expect("native owner disappeared");
}

#[test]
fn native_cancellation_reclaims_an_active_child_cpu_lease() {
    let temporary = TestDirectory::new("cancel-lease");
    let state = temporary.path().join("state");
    let checkout = temporary.path().join("checkout");
    let ready = temporary.path().join("ready");
    let worker = temporary.path().join("worker.py");
    let selected_broker = installed_broker(&temporary);
    initialize_checkout(&checkout);
    write_config(&state, &selected_broker, json!({"jobs": 1, "cpu": 2}));
    fs::write(
        &worker,
        r#"import os
from pathlib import Path
import time
from agcoord.queue import CoordinatorClient

client = CoordinatorClient(state_dir=os.environ["AGCOORD_STATE_DIR"], autostart=False)
client.acquire_child_cpu_lease(2, timeout=5)
Path(os.environ["LEASE_READY"]).touch()
while True:
    time.sleep(1)
"#,
    )
    .unwrap();
    let submitter = r#"
import os
import sys
from agcoord.queue import CoordinatorClient

state, checkout, worker, ready = sys.argv[1:]
environment = dict(os.environ)
environment["LEASE_READY"] = ready
run_id = CoordinatorClient(state_dir=state, autostart=True).submit(
    [sys.executable, worker],
    checkout=checkout,
    resources={"cpu": 2},
    environment=environment,
)
print(run_id)
"#;
    let submitted = python_command(&[
        "-c".to_owned(),
        submitter.to_owned(),
        state.to_string_lossy().into_owned(),
        checkout.to_string_lossy().into_owned(),
        worker.to_string_lossy().into_owned(),
        ready.to_string_lossy().into_owned(),
    ]);
    assert_success(&submitted, "native cancellable submission");
    let run_id = String::from_utf8(submitted.stdout)
        .unwrap()
        .trim()
        .to_owned();
    assert!(run_id.starts_with("check-"));
    let deadline = Instant::now() + Duration::from_secs(10);
    while !ready.exists() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(20));
    }
    assert!(ready.exists(), "child lease worker never became ready");

    let cancelled = python_cli(&[
        "--json".to_owned(),
        "--state-dir".to_owned(),
        state.to_string_lossy().into_owned(),
        "cancel".to_owned(),
        run_id.clone(),
    ]);
    assert_success(&cancelled, "native cancellation");
    assert_eq!(parse_json_output(&cancelled)["cancel_requested"], true);

    let deadline = Instant::now() + Duration::from_secs(10);
    let terminal = loop {
        let shown = python_cli(&[
            "--state-dir".to_owned(),
            state.to_string_lossy().into_owned(),
            "show".to_owned(),
            run_id.clone(),
        ]);
        assert_success(&shown, "native cancellation status");
        let row = parse_json_output(&shown);
        if row["status"] == "cancelled" {
            break row;
        }
        assert!(
            Instant::now() < deadline,
            "cancelled run did not become terminal: {row}"
        );
        thread::sleep(Duration::from_millis(20));
    };
    assert_eq!(terminal["exit_status"], 130);

    let leases = python_command(&[
        "-c".to_owned(),
        "import json,sys; from agcoord.queue import CoordinatorClient; print(json.dumps(CoordinatorClient(state_dir=sys.argv[1], autostart=False).child_cpu_leases(sys.argv[2], include_terminal=True)))".to_owned(),
        state.to_string_lossy().into_owned(),
        run_id,
    ]);
    assert_success(&leases, "native terminal lease query");
    let lease_rows = parse_json_output(&leases);
    assert_eq!(lease_rows.as_array().unwrap().len(), 1);
    assert_eq!(lease_rows[0]["status"], "cancelled");

    let (_guard, _owner) = owner_guard(&state).expect("native owner disappeared");
}

#[test]
fn concurrent_first_clients_converge_on_one_native_owner() {
    let temporary = TestDirectory::new("autostart-race");
    let state = temporary.path().join("state");
    let checkout = temporary.path().join("checkout");
    let selected_broker = installed_broker(&temporary);
    initialize_checkout(&checkout);
    write_config(&state, &selected_broker, json!({"jobs": 2}));

    let state_text = state.to_string_lossy().into_owned();
    let checkout_text = checkout.to_string_lossy().into_owned();
    let clients = (0..2)
        .map(|index| {
            let state = state_text.clone();
            let checkout = checkout_text.clone();
            thread::spawn(move || {
                python_cli(&[
                    "--json".to_owned(),
                    "--state-dir".to_owned(),
                    state,
                    "run".to_owned(),
                    "--label".to_owned(),
                    format!("racing client {index}"),
                    "--checkout".to_owned(),
                    checkout,
                    "--".to_owned(),
                    "/bin/true".to_owned(),
                ])
            })
        })
        .collect::<Vec<_>>();
    for (index, client) in clients.into_iter().enumerate() {
        let result = client.join().unwrap();
        assert_success(&result, &format!("racing client {index}"));
        assert_eq!(parse_json_output(&result)["status"], "passed");
    }
    let (_guard, owner) = owner_guard(&state).expect("racing clients left no owner");
    assert_eq!(
        owner
            .lines()
            .filter(|line| line.starts_with("pid="))
            .count(),
        1
    );
    let listed = python_cli(&[
        "--json".to_owned(),
        "--state-dir".to_owned(),
        state.to_string_lossy().into_owned(),
        "list".to_owned(),
    ]);
    assert_success(&listed, "post-race list");
    assert_eq!(
        parse_json_output(&listed)["recent"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
}

#[test]
fn durable_drain_rejects_native_submissions_then_resumes_by_exact_id() {
    let temporary = TestDirectory::new("durable-drain");
    let state = temporary.path().join("state");
    let checkout = temporary.path().join("checkout");
    let ready = temporary.path().join("ready");
    let release = temporary.path().join("release");
    let selected_broker = installed_broker(&temporary);
    initialize_checkout(&checkout);
    write_config(&state, &selected_broker, json!({"jobs": 1}));

    let state_for_run = state.clone();
    let checkout_for_run = checkout.clone();
    let ready_for_run = ready.clone();
    let release_for_run = release.clone();
    let active = thread::spawn(move || {
        python_cli(&[
            "--json".to_owned(),
            "--state-dir".to_owned(),
            state_for_run.to_string_lossy().into_owned(),
            "run".to_owned(),
            "--label".to_owned(),
            "native drain survivor".to_owned(),
            "--checkout".to_owned(),
            checkout_for_run.to_string_lossy().into_owned(),
            "--".to_owned(),
            "/bin/sh".to_owned(),
            "-c".to_owned(),
            "touch \"$1\"; while [ ! -e \"$2\" ]; do sleep 0.02; done".to_owned(),
            "agcoord-drain".to_owned(),
            ready_for_run.to_string_lossy().into_owned(),
            release_for_run.to_string_lossy().into_owned(),
        ])
    });
    let deadline = Instant::now() + Duration::from_secs(10);
    while !ready.exists() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(20));
    }
    assert!(ready.exists(), "native pre-drain worker never became ready");

    let caller_pid = std::process::id().to_string();
    let queued = Command::new(&selected_broker)
        .args([
            "submit",
            "--state-dir",
            state.to_str().unwrap(),
            "--run-id",
            "check-native-drain-cancel",
            "--kind",
            "check",
            "--label",
            "native drain cancellation",
            "--repository-id",
            "repository",
            "--repository",
            checkout.to_str().unwrap(),
            "--worktree-id",
            "worktree",
            "--checkout",
            checkout.to_str().unwrap(),
            "--branch",
            "main",
            "--resource",
            "jobs=1",
            "--caller-pid",
            &caller_pid,
            "--",
            "/bin/false",
        ])
        .output()
        .unwrap();
    assert_success(&queued, "queued native drain cancellation target");

    let draining = python_cli(&[
        "--json".to_owned(),
        "--state-dir".to_owned(),
        state.to_string_lossy().into_owned(),
        "drain".to_owned(),
        "--reason".to_owned(),
        "native host upgrade".to_owned(),
        "--no-wait".to_owned(),
    ]);
    assert_success(&draining, "native drain request");
    let draining = parse_json_output(&draining);
    assert_eq!(draining["state"], "draining");
    assert_eq!(draining["live"], 2);
    assert_eq!(draining["reason"], "native host upgrade");
    let drain_id = draining["drain_id"].as_str().unwrap().to_owned();

    let cancelled = python_cli(&[
        "--json".to_owned(),
        "--state-dir".to_owned(),
        state.to_string_lossy().into_owned(),
        "cancel".to_owned(),
        "check-native-drain-cancel".to_owned(),
    ]);
    assert_success(&cancelled, "native cancellation during drain");
    let cancelled = parse_json_output(&cancelled);
    assert_eq!(cancelled["status"], "cancelled");
    assert_eq!(cancelled["exit_status"], 130);

    let refused = python_cli(&[
        "--json".to_owned(),
        "--state-dir".to_owned(),
        state.to_string_lossy().into_owned(),
        "run".to_owned(),
        "--checkout".to_owned(),
        checkout.to_string_lossy().into_owned(),
        "--".to_owned(),
        "/bin/true".to_owned(),
    ]);
    assert!(!refused.status.success());
    let refusal: serde_json::Value = serde_json::from_slice(&refused.stderr).unwrap();
    assert_eq!(refusal["code"], "broker-draining");
    assert!(refusal["message"].as_str().unwrap().contains("draining"));

    let direct_refusal = Command::new(&selected_broker)
        .args([
            "submit",
            "--state-dir",
            state.to_str().unwrap(),
            "--run-id",
            "check-direct-drain",
            "--kind",
            "check",
            "--label",
            "direct drain refusal",
            "--repository-id",
            "repository",
            "--repository",
            checkout.to_str().unwrap(),
            "--worktree-id",
            "worktree",
            "--checkout",
            checkout.to_str().unwrap(),
            "--branch",
            "main",
            "--resource",
            "jobs=1",
            "--caller-pid",
            &caller_pid,
            "--",
            "/bin/true",
        ])
        .output()
        .unwrap();
    assert!(!direct_refusal.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&direct_refusal.stderr).unwrap()["code"],
        "broker-draining"
    );

    fs::write(&release, "release\n").unwrap();
    let active = active.join().unwrap();
    assert_success(&active, "native pre-drain worker");
    assert_eq!(parse_json_output(&active)["status"], "passed");

    let deadline = Instant::now() + Duration::from_secs(10);
    let drained = loop {
        let status = python_cli(&[
            "--json".to_owned(),
            "--state-dir".to_owned(),
            state.to_string_lossy().into_owned(),
            "drain".to_owned(),
            "--no-wait".to_owned(),
        ]);
        assert_success(&status, "native drain status");
        let status = parse_json_output(&status);
        if status["state"] == "drained" {
            break status;
        }
        assert!(
            Instant::now() < deadline,
            "native broker did not drain: {status}"
        );
        thread::sleep(Duration::from_millis(20));
    };
    assert_eq!(drained["drain_id"], drain_id);
    assert_eq!(drained["live"], 0);
    assert!(drained["broker_pid"].is_null());

    let wrong_resume = python_cli(&[
        "--state-dir".to_owned(),
        state.to_string_lossy().into_owned(),
        "resume".to_owned(),
        "drain-ffffffffffff".to_owned(),
    ]);
    assert!(!wrong_resume.status.success());
    assert!(String::from_utf8_lossy(&wrong_resume.stderr).contains("does not match"));

    let resumed = python_cli(&[
        "--json".to_owned(),
        "--state-dir".to_owned(),
        state.to_string_lossy().into_owned(),
        "resume".to_owned(),
        drain_id.clone(),
    ]);
    assert_success(&resumed, "native resume");
    assert_eq!(
        parse_json_output(&resumed),
        json!({"state": "open", "drain_id": drain_id, "resumed": true})
    );

    let after = python_cli(&[
        "--json".to_owned(),
        "--state-dir".to_owned(),
        state.to_string_lossy().into_owned(),
        "run".to_owned(),
        "--checkout".to_owned(),
        checkout.to_string_lossy().into_owned(),
        "--".to_owned(),
        "/bin/true".to_owned(),
    ]);
    assert_success(&after, "post-resume native run");
    assert_eq!(parse_json_output(&after)["status"], "passed");
    let (_guard, _owner) = owner_guard(&state).expect("resumed broker disappeared");
}

#[test]
fn draining_native_spool_recovers_after_broker_crash() {
    let temporary = TestDirectory::new("drain-recovery");
    let state = temporary.path().join("state");
    let checkout = temporary.path().join("checkout");
    let ready = temporary.path().join("ready");
    let release = temporary.path().join("release");
    let selected_broker = installed_broker(&temporary);
    initialize_checkout(&checkout);
    write_config(&state, &selected_broker, json!({"jobs": 1}));
    let _cleanup = StateOwnerGuard(state.clone());
    let _release_cleanup = ReleaseGuard(release.clone());

    let state_for_run = state.clone();
    let checkout_for_run = checkout.clone();
    let ready_for_run = ready.clone();
    let release_for_run = release.clone();
    let active = thread::spawn(move || {
        python_cli(&[
            "--json".to_owned(),
            "--state-dir".to_owned(),
            state_for_run.to_string_lossy().into_owned(),
            "run".to_owned(),
            "--label".to_owned(),
            "native drain crash survivor".to_owned(),
            "--checkout".to_owned(),
            checkout_for_run.to_string_lossy().into_owned(),
            "--".to_owned(),
            "/bin/sh".to_owned(),
            "-c".to_owned(),
            "touch \"$1\"; while [ ! -e \"$2\" ]; do sleep 0.02; done".to_owned(),
            "agcoord-drain-recovery".to_owned(),
            ready_for_run.to_string_lossy().into_owned(),
            release_for_run.to_string_lossy().into_owned(),
        ])
    });
    let deadline = Instant::now() + Duration::from_secs(10);
    while !ready.exists() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(20));
    }
    assert!(ready.exists(), "native recovery worker never became ready");

    let draining = python_cli(&[
        "--json".to_owned(),
        "--state-dir".to_owned(),
        state.to_string_lossy().into_owned(),
        "drain".to_owned(),
        "--no-wait".to_owned(),
    ]);
    assert_success(&draining, "native recovery drain");
    let drain_id = parse_json_output(&draining)["drain_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let owner = fs::read_to_string(state.join("broker.lock")).unwrap();
    let crashed_pid = owner
        .lines()
        .find_map(|line| line.strip_prefix("pid="))
        .unwrap()
        .to_owned();
    assert!(
        Command::new("/bin/kill")
            .args(["-KILL", &crashed_pid])
            .status()
            .unwrap()
            .success()
    );
    let deadline = Instant::now() + Duration::from_secs(5);
    while Path::new(&format!("/proc/{crashed_pid}")).exists() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(20));
    }
    assert!(
        !Path::new(&format!("/proc/{crashed_pid}")).exists(),
        "crashed native broker remained live"
    );

    fs::write(&release, "release\n").unwrap();
    let active = active.join().unwrap();
    assert!(!active.status.success());
    let recovered = parse_json_output(&active);
    assert_eq!(recovered["status"], "interrupted");
    assert_eq!(recovered["failure_reason"], "worker-result-lost");

    let deadline = Instant::now() + Duration::from_secs(10);
    let drained = loop {
        let status = python_cli(&[
            "--json".to_owned(),
            "--state-dir".to_owned(),
            state.to_string_lossy().into_owned(),
            "drain".to_owned(),
            "--no-wait".to_owned(),
        ]);
        assert_success(&status, "recovered drain status");
        let status = parse_json_output(&status);
        if status["state"] == "drained" {
            break status;
        }
        assert!(
            Instant::now() < deadline,
            "replacement broker did not drain"
        );
        thread::sleep(Duration::from_millis(20));
    };
    assert_eq!(drained["drain_id"], drain_id);
    assert!(drained["broker_pid"].is_null());

    let resumed = python_cli(&[
        "--json".to_owned(),
        "--state-dir".to_owned(),
        state.to_string_lossy().into_owned(),
        "resume".to_owned(),
        drain_id,
    ]);
    assert_success(&resumed, "recovered drain resume");
}

#[test]
fn a_stale_selected_binary_cannot_replace_or_command_the_live_owner() {
    let temporary = TestDirectory::new("stale-binary");
    let state = temporary.path().join("state");
    let checkout = temporary.path().join("checkout");
    let selected_broker = installed_broker(&temporary);
    initialize_checkout(&checkout);
    write_config(&state, &selected_broker, json!({"jobs": 1}));
    let started = python_cli(&[
        "--json".to_owned(),
        "--state-dir".to_owned(),
        state.to_string_lossy().into_owned(),
        "run".to_owned(),
        "--checkout".to_owned(),
        checkout.to_string_lossy().into_owned(),
        "--".to_owned(),
        "/bin/true".to_owned(),
    ]);
    assert_success(&started, "owner bootstrap");
    let (guard, original_owner) = owner_guard(&state).expect("native owner disappeared");

    let stale = temporary.path().join("stale-broker");
    fs::write(
        &stale,
        r#"#!/bin/sh
if [ "$1" = identity ] && [ "$2" = --json ]; then
  printf '%s\n' '{"name":"agcoord-broker","version":"0.3.0","protocol":5,"implementation":"rust-native","build":"sha256:0000000000000000000000000000000000000000000000000000000000000000","target":"x86_64-unknown-linux-musl","sqlite":"3"}'
  exit 0
fi
exit 97
"#,
    )
    .unwrap();
    fs::set_permissions(&stale, fs::Permissions::from_mode(0o755)).unwrap();
    write_config(&state, &stale, json!({"jobs": 1}));
    let refused = python_cli(&[
        "--json".to_owned(),
        "--state-dir".to_owned(),
        state.to_string_lossy().into_owned(),
        "list".to_owned(),
    ]);
    assert!(!refused.status.success());
    let error = String::from_utf8_lossy(&refused.stderr);
    assert!(
        error.contains("does not match the selected executable"),
        "{error}"
    );
    assert!(error.contains("build"), "{error}");
    assert_eq!(
        fs::read_to_string(state.join("broker.lock")).unwrap(),
        original_owner
    );
    drop(guard);
}

#[test]
fn python_migrate_routes_an_idle_protocol_four_spool_through_the_native_binary() {
    let temporary = TestDirectory::new("migrate");
    let state = temporary.path().join("state");
    let checkout = temporary.path().join("checkout");
    let selected_broker = installed_broker(&temporary);
    initialize_checkout(&checkout);
    write_config(&state, &selected_broker, json!({"jobs": 1}));
    let legacy = python_command(&[
        "-c".to_owned(),
        "import sys; from agcoord.queue import CoordinatorBroker; CoordinatorBroker(sys.argv[1], idle_timeout=None)".to_owned(),
        state.to_string_lossy().into_owned(),
    ]);
    assert_success(&legacy, "protocol-four fixture");

    let migrated = python_cli(&[
        "--json".to_owned(),
        "--state-dir".to_owned(),
        state.to_string_lossy().into_owned(),
        "migrate".to_owned(),
    ]);
    assert_success(&migrated, "native migration");
    assert_eq!(
        parse_json_output(&migrated),
        json!({"changed": true, "from_protocol": 4, "to_protocol": 5})
    );

    let check = python_cli(&[
        "--json".to_owned(),
        "--state-dir".to_owned(),
        state.to_string_lossy().into_owned(),
        "run".to_owned(),
        "--checkout".to_owned(),
        checkout.to_string_lossy().into_owned(),
        "--".to_owned(),
        "/bin/true".to_owned(),
    ]);
    assert_success(&check, "post-migration native check");
    assert_eq!(parse_json_output(&check)["status"], "passed");
    let (_guard, owner) = owner_guard(&state).expect("migrated spool has no native owner");
    assert!(owner.contains("protocol=5\n"));
}

#[test]
fn protocol_four_drain_survives_native_migration_until_exact_resume() {
    let temporary = TestDirectory::new("drained-migrate");
    let state = temporary.path().join("state");
    let checkout = temporary.path().join("checkout");
    let selected_broker = installed_broker(&temporary);
    initialize_checkout(&checkout);
    write_config(&state, &selected_broker, json!({"jobs": 1}));
    let legacy = python_command(&[
        "-c".to_owned(),
        "import sys; from agcoord.queue import CoordinatorBroker; CoordinatorBroker(sys.argv[1], idle_timeout=None)".to_owned(),
        state.to_string_lossy().into_owned(),
    ]);
    assert_success(&legacy, "protocol-four fixture");
    let legacy_history = python_command(&[
        "-c".to_owned(),
        r#"
import sys
import threading
import time
from agcoord.queue import CoordinatorBroker

broker = CoordinatorBroker(sys.argv[1], idle_timeout=None)
thread = threading.Thread(target=broker.serve_forever)
thread.start()
assert broker.ready.wait(5)
run_id = broker.submit(["/bin/true"], checkout=sys.argv[2])
deadline = time.monotonic() + 5
while broker.status(run_id)["status"] not in {"passed", "failed", "cancelled", "interrupted"}:
    assert time.monotonic() < deadline
    time.sleep(0.02)
assert broker.status(run_id)["status"] == "passed"
broker.close()
thread.join(5)
assert not thread.is_alive()
"#
        .to_owned(),
        state.to_string_lossy().into_owned(),
        checkout.to_string_lossy().into_owned(),
    ]);
    assert_success(&legacy_history, "protocol-four terminal history");

    let drained = python_cli(&[
        "--json".to_owned(),
        "--state-dir".to_owned(),
        state.to_string_lossy().into_owned(),
        "drain".to_owned(),
        "--reason".to_owned(),
        "native migration".to_owned(),
    ]);
    assert_success(&drained, "protocol-four drain");
    let drained = parse_json_output(&drained);
    assert_eq!(drained["state"], "drained");
    assert_eq!(drained["protocol"], 4);
    let drain_id = drained["drain_id"].as_str().unwrap().to_owned();

    let migrated = python_cli(&[
        "--json".to_owned(),
        "--state-dir".to_owned(),
        state.to_string_lossy().into_owned(),
        "migrate".to_owned(),
    ]);
    assert_success(&migrated, "drained native migration");
    assert_eq!(
        parse_json_output(&migrated),
        json!({"changed": true, "from_protocol": 4, "to_protocol": 5})
    );

    let listed = python_cli(&[
        "--json".to_owned(),
        "--state-dir".to_owned(),
        state.to_string_lossy().into_owned(),
        "list".to_owned(),
    ]);
    assert_success(&listed, "migrated drain observation");
    let snapshot = parse_json_output(&listed);
    assert_eq!(snapshot["protocol"], 5);
    assert_eq!(snapshot["maintenance"]["state"], "drained");
    assert_eq!(snapshot["maintenance"]["drain_id"], drain_id);
    assert!(snapshot["broker_pid"].is_null());

    let rolled_back = Command::new(&selected_broker)
        .args(["rollback", "--state-dir", state.to_str().unwrap()])
        .output()
        .unwrap();
    assert_success(&rolled_back, "drained native rollback");
    assert_eq!(
        parse_json_output(&rolled_back),
        json!({"changed": true, "from_protocol": 5, "to_protocol": 4})
    );
    let legacy_status = python_cli(&[
        "--json".to_owned(),
        "--state-dir".to_owned(),
        state.to_string_lossy().into_owned(),
        "drain".to_owned(),
        "--no-wait".to_owned(),
    ]);
    assert_success(&legacy_status, "rolled-back drain observation");
    let legacy_status = parse_json_output(&legacy_status);
    assert_eq!(legacy_status["protocol"], 4);
    assert_eq!(legacy_status["drain_id"], drain_id);

    let remigrated = python_cli(&[
        "--json".to_owned(),
        "--state-dir".to_owned(),
        state.to_string_lossy().into_owned(),
        "migrate".to_owned(),
    ]);
    assert_success(&remigrated, "drained native remigration");
    assert_eq!(
        parse_json_output(&remigrated),
        json!({"changed": true, "from_protocol": 4, "to_protocol": 5})
    );

    let refused = python_cli(&[
        "--state-dir".to_owned(),
        state.to_string_lossy().into_owned(),
        "run".to_owned(),
        "--checkout".to_owned(),
        checkout.to_string_lossy().into_owned(),
        "--".to_owned(),
        "/bin/true".to_owned(),
    ]);
    assert!(!refused.status.success());
    assert!(String::from_utf8_lossy(&refused.stderr).contains("drained"));

    let resumed = python_cli(&[
        "--json".to_owned(),
        "--state-dir".to_owned(),
        state.to_string_lossy().into_owned(),
        "resume".to_owned(),
        drain_id,
    ]);
    assert_success(&resumed, "post-migration resume");

    let check = python_cli(&[
        "--json".to_owned(),
        "--state-dir".to_owned(),
        state.to_string_lossy().into_owned(),
        "run".to_owned(),
        "--checkout".to_owned(),
        checkout.to_string_lossy().into_owned(),
        "--".to_owned(),
        "/bin/true".to_owned(),
    ]);
    assert_success(&check, "post-resume native check");
    let (_guard, owner) = owner_guard(&state).expect("resumed spool has no native owner");
    assert!(owner.contains("protocol=5\n"));
}

#[test]
fn migration_runbook_rehearses_backup_rollback_and_final_native_ownership() {
    let temporary = TestDirectory::new("migration-runbook");
    let selected_broker = installed_broker(&temporary);
    let executable = python_command(&[
        "-c".to_owned(),
        "import sys; print(sys.executable)".to_owned(),
    ]);
    assert_success(&executable, "Python executable discovery");
    let python = String::from_utf8(executable.stdout).unwrap();
    let python = python.trim();
    let agc = temporary.path().join("agc");
    fs::write(
        &agc,
        format!("#!{python}\nfrom agcoord.cli import main\nraise SystemExit(main())\n"),
    )
    .unwrap();
    fs::set_permissions(&agc, fs::Permissions::from_mode(0o755)).unwrap();

    let result = Command::new(repository_root().join("scripts/rehearse-native-migration"))
        .args([
            "--python",
            python,
            "--agc",
            agc.to_str().unwrap(),
            "--broker",
            selected_broker.to_str().unwrap(),
        ])
        .env("PYTHONPATH", repository_root().join("src"))
        .env_remove("AGCOORD_RUN_ID")
        .env_remove("AGCOORD_RUN_KIND")
        .env_remove("AGCOORD_STATE_DIR")
        .output()
        .unwrap();
    assert_success(&result, "native migration runbook rehearsal");
    let receipt = parse_json_output(&result);
    assert_eq!(receipt["final_protocol"], 5);
    assert_eq!(receipt["rollback_protocol"], 4);
    assert_eq!(receipt["broker_version"], env!("CARGO_PKG_VERSION"));
    assert_eq!(receipt["broker_build"], "development");
    assert!(
        receipt["backup_sha256"]
            .as_str()
            .is_some_and(|value| value.len() == 64)
    );
    assert!(
        receipt["legacy_run_id"]
            .as_str()
            .unwrap()
            .starts_with("check-")
    );
    assert!(
        receipt["native_run_id"]
            .as_str()
            .unwrap()
            .starts_with("check-")
    );
}

#[test]
fn real_textual_refresh_renders_a_protocol_five_snapshot() {
    let temporary = TestDirectory::new("tui");
    let state = temporary.path().join("state");
    let checkout = temporary.path().join("checkout");
    let selected_broker = installed_broker(&temporary);
    initialize_checkout(&checkout);
    write_config(&state, &selected_broker, json!({"jobs": 1}));
    let check = python_cli(&[
        "--json".to_owned(),
        "--state-dir".to_owned(),
        state.to_string_lossy().into_owned(),
        "run".to_owned(),
        "--label".to_owned(),
        "native TUI row".to_owned(),
        "--checkout".to_owned(),
        checkout.to_string_lossy().into_owned(),
        "--".to_owned(),
        "/bin/true".to_owned(),
    ]);
    assert_success(&check, "native TUI fixture");
    let run_id = parse_json_output(&check)["run_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let script = r##"
import asyncio
import json
import sys

from textual.widgets import DataTable, Static
from agcoord.queue import CoordinatorClient
from agcoord.tui import build_app

async def inspect():
    state, expected = sys.argv[1:]
    app = build_app(
        lambda: CoordinatorClient(state_dir=state, autostart=False),
        refresh_interval=60,
    )
    async with app.run_test(size=(100, 30)) as pilot:
        for _ in range(20):
            await pilot.pause()
            if list(app.workers):
                await app.workers.wait_for_complete()
            table = app.query_one("#gates", DataTable)
            if table.row_count:
                break
        table = app.query_one("#gates", DataTable)
        cells = [str(value) for value in table.get_row_at(0)]
        status = str(app.query_one("#gate-status", Static).content)
        print(json.dumps({
            "headers": [str(column.label) for column in table.columns.values()],
            "cells": cells,
            "status": status,
            "horizontal_scroll": table.max_scroll_x,
        }))
        if expected not in cells:
            raise AssertionError((expected, cells))

asyncio.run(inspect())
"##;
    let rendered = python_command(&[
        "-c".to_owned(),
        script.to_owned(),
        state.to_string_lossy().into_owned(),
        run_id,
    ]);
    assert_success(&rendered, "native Textual refresh");
    let observed = parse_json_output(&rendered);
    assert_eq!(
        observed["headers"],
        json!([
            "STATE", "KIND", "REPO", "RUN", "BRANCH", "LABEL", "AGE", "DUR"
        ])
    );
    assert_eq!(observed["horizontal_scroll"], 0);
    assert!(!observed["status"].as_str().unwrap().contains("unavailable"));
    let (_guard, _owner) = owner_guard(&state).expect("native owner disappeared");
}

#[test]
fn pytest_xdist_uses_one_native_parent_lease_and_returns_it_on_failure() {
    let temporary = TestDirectory::new("xdist");
    let state = temporary.path().join("state");
    let checkout = temporary.path().join("checkout");
    let reports = temporary.path().join("workers");
    let selected_broker = installed_broker(&temporary);
    initialize_checkout(&checkout);
    fs::create_dir(&reports).unwrap();
    fs::write(
        checkout.join("conftest.py"),
        r#"import os
from pathlib import Path

def pytest_sessionstart(session):
    worker = os.environ.get("PYTEST_XDIST_WORKER")
    if worker:
        (Path(os.environ["XDIST_REPORT"]) / worker).touch()
"#,
    )
    .unwrap();
    fs::write(
        checkout.join("test_native_xdist.py"),
        r#"import os

def test_one():
    assert os.environ.get("XDIST_FAIL") != "1"

def test_two():
    assert True
"#,
    )
    .unwrap();
    git(&checkout, &["add", "."]);
    git(&checkout, &["commit", "-q", "-m", "xdist fixture"]);
    write_config(&state, &selected_broker, json!({"jobs": 1, "cpu": 2}));

    let run = |failure: bool, report: &Path| {
        fs::create_dir(report).unwrap();
        python_cli_with_env(
            &[
                "--json".to_owned(),
                "--state-dir".to_owned(),
                state.to_string_lossy().into_owned(),
                "run".to_owned(),
                "--checkout".to_owned(),
                checkout.to_string_lossy().into_owned(),
                "--resource".to_owned(),
                "cpu=2".to_owned(),
                "--".to_owned(),
                "python3".to_owned(),
                "-m".to_owned(),
                "pytest".to_owned(),
                "-q".to_owned(),
                "-p".to_owned(),
                "no:cacheprovider".to_owned(),
                "-n".to_owned(),
                "auto".to_owned(),
            ],
            &[
                ("PYTEST_XDIST_AUTO_NUM_WORKERS", "4"),
                ("XDIST_REPORT", report.to_str().unwrap()),
                ("XDIST_FAIL", if failure { "1" } else { "0" }),
            ],
        )
    };

    let success_report = reports.join("success");
    let success = run(false, &success_report);
    assert_success(&success, "successful native xdist run");
    let success_row = parse_json_output(&success);
    assert_eq!(success_row["status"], "passed");
    assert_eq!(
        fs::read_dir(&success_report)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect::<std::collections::BTreeSet<_>>(),
        std::collections::BTreeSet::from(["gw0".to_owned(), "gw1".to_owned()])
    );

    let failure_report = reports.join("failure");
    let failure = run(true, &failure_report);
    assert_eq!(failure.status.code(), Some(1));
    let failure_row = parse_json_output(&failure);
    assert_eq!(failure_row["status"], "failed");

    for row in [&success_row, &failure_row] {
        let run_id = row["run_id"].as_str().unwrap();
        let leases = python_command(&[
            "-c".to_owned(),
            "import json,sys; from agcoord.queue import CoordinatorClient; print(json.dumps(CoordinatorClient(state_dir=sys.argv[1], autostart=False).child_cpu_leases(sys.argv[2], include_terminal=True)))".to_owned(),
            state.to_string_lossy().into_owned(),
            run_id.to_owned(),
        ]);
        assert_success(&leases, "native xdist lease query");
        let leases = parse_json_output(&leases);
        assert_eq!(leases.as_array().unwrap().len(), 1);
        assert_eq!(leases[0]["run_id"], run_id);
        assert_eq!(leases[0]["status"], "released");
        assert_eq!(leases[0]["requested"], 2);
        assert_eq!(leases[0]["minimum"], 1);
        assert_eq!(leases[0]["granted"], 2);
        assert_eq!(leases[0]["full"], true);
    }
    let (_guard, _owner) = owner_guard(&state).expect("native owner disappeared");
}
