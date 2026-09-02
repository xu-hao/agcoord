"""Behavioral contract for the machine-local multi-repository coordinator."""

from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import signal
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time

import pytest

from agcoord.config import config_path
from agcoord.queue import (
    CoordinatorBroker,
    CoordinatorClient,
    CoordinatorError,
    PROTOCOL,
    migrate_queue,
    state_dir_for,
)
from agcoord.resources import ResourceMeasurement, ResourceObservation

from conftest import (
    RunningCoordinator,
    caller_environment,
    wait_for,
    write_broker_config,
)


ROW_KEYS = {
    "run_id",
    "sequence",
    "status",
    "kind",
    "label",
    "agent",
    "repository_id",
    "repository",
    "worktree_id",
    "checkout",
    "branch",
    "head_sha",
    "barrier",
    "resources",
    "resource_contract",
    "resource_receipt",
    "blocked_by",
    "gate_run_id",
    "publication",
    "failure_reason",
    "phase",
    "gate_exit_status",
    "caller_pid",
    "command",
    "created_at",
    "started_at",
    "finished_at",
    "exit_status",
    "worker_pid",
    "cancel_requested",
    "log_bytes",
    "position",
}

SNAPSHOT_KEYS = {
    "protocol",
    "broker_pid",
    "captured_at",
    "capacities",
    "allocations",
    "resource_bindings",
    "resource_capabilities",
    "maintenance",
    "active",
    "queued",
    "recent",
}

GIT = shutil.which("git") or "git"


def _python(source: str, *arguments: str | Path) -> list[str]:
    return [sys.executable, "-u", "-c", source, *(str(value) for value in arguments)]


def _git(repository: Path, *arguments: str) -> str:
    completed = subprocess.run(
        [GIT, "-C", str(repository), *arguments],
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr
    return completed.stdout.strip()


def _repository(path: Path, *, content: str = "base\n") -> Path:
    path.mkdir(parents=True)
    _git(path, "init", "-b", "main")
    _git(path, "config", "user.name", "AGCoord test")
    _git(path, "config", "user.email", "agcoord@example.invalid")
    (path / "tracked.txt").write_text(content, encoding="utf-8")
    _git(path, "add", "tracked.txt")
    _git(path, "commit", "-m", "initial")
    return path


def _head(repository: Path) -> str:
    return _git(repository, "rev-parse", "HEAD")


def _submit(
    client: CoordinatorClient,
    command: list[str],
    checkout: Path,
    *,
    kind: str = "check",
    label: str = "test job",
    resources: dict[str, int] | None = None,
    agent: str | None = "pytest",
) -> str:
    return client.submit(
        command,
        checkout=str(checkout),
        kind=kind,
        label=label,
        resources=resources or {"jobs": 1},
        agent=agent,
        caller_pid=os.getpid(),
        environment=caller_environment(),
    )


def _row(client: CoordinatorClient, run_id: str, *statuses: str) -> dict[str, object]:
    expected = set(statuses)
    return wait_for(
        lambda: (
            row
            if (row := client.status(run_id))["status"] in expected
            else None
        ),
        f"{run_id} never reached {sorted(expected)}",
    )


def _blocking_command(entered: Path, release: Path, name: str) -> list[str]:
    return _python(
        """
from pathlib import Path
import sys
import time

entered, release, name = sys.argv[1:]
Path(entered).write_text(name, encoding="utf-8")
while not Path(release).exists():
    time.sleep(0.01)
print(name, flush=True)
""",
        entered,
        release,
        name,
    )


def _publication_repository(path: Path) -> tuple[Path, Path, str, str]:
    remote = path.parent / f"{path.name}-origin.git"
    subprocess.run(
        [GIT, "init", "--bare", str(remote)],
        check=True,
        capture_output=True,
        text=True,
    )
    checkout = _repository(path)
    _git(checkout, "remote", "add", "origin", str(remote))
    _git(checkout, "push", "-u", "origin", "main")
    branch = "feature/atomic-land"
    _git(checkout, "switch", "-c", branch)
    (checkout / "tracked.txt").write_text("candidate\n", encoding="utf-8")
    _git(checkout, "add", "tracked.txt")
    _git(checkout, "commit", "-m", "candidate")
    head_sha = _head(checkout)
    _git(checkout, "push", "-u", "origin", branch)
    return checkout, remote, branch, head_sha


def _install_land_gh(tmp_path: Path) -> Path:
    bin_dir = tmp_path / "fake-gh-bin"
    bin_dir.mkdir()
    executable = bin_dir / "gh"
    executable.write_text(
        f"""#!{sys.executable}
import json
import os
from pathlib import Path
import subprocess
import sys
import time

arguments = sys.argv[1:]
raw_input = sys.stdin.read()
payload = json.loads(raw_input) if raw_input else None

if arguments[:2] == ["pr", "view"]:
    preflight_entered = os.environ.get("AGCOORD_TEST_PREFLIGHT_ENTERED")
    if preflight_entered:
        Path(preflight_entered).touch()
    preflight_release = os.environ.get("AGCOORD_TEST_PREFLIGHT_RELEASE")
    while preflight_release and not Path(preflight_release).exists():
        time.sleep(0.01)
    head = os.environ["AGCOORD_TEST_HEAD"]
    if os.environ.get("AGCOORD_TEST_DYNAMIC_HEAD") == "1":
        observed = subprocess.run(
            ["git", "ls-remote", "origin", f"refs/heads/{{os.environ['AGCOORD_TEST_BRANCH']}}"],
            check=True,
            text=True,
            capture_output=True,
        ).stdout.strip()
        head = observed.split()[0]
    print(json.dumps({{
        "number": int(arguments[2]),
        "state": "OPEN",
        "isDraft": False,
        "baseRefName": os.environ.get("AGCOORD_TEST_BASE", "main"),
        "headRefName": os.environ["AGCOORD_TEST_BRANCH"],
        "headRefOid": head,
        "isCrossRepository": False,
        "title": "Atomic land test",
        "headRepositoryOwner": {{"login": "pytest"}},
    }}))
elif arguments[:2] == ["repo", "view"]:
    print(json.dumps({{"id": "R_agcoord_test", "nameWithOwner": "test/repository"}}))
elif arguments[:3] == ["api", "--method", "POST"]:
    print(json.dumps({{
        "sha": "c" * 40,
        "tree": {{"sha": payload["tree"]}},
        "parents": [{{"sha": parent}} for parent in payload["parents"]],
    }}))
elif arguments[:2] == ["api", "graphql"]:
    event_log = os.environ.get("AGCOORD_TEST_EVENT_LOG")
    tag = os.environ.get("AGCOORD_TEST_TAG", "land")
    if event_log:
        with Path(event_log).open("a", encoding="utf-8") as stream:
            stream.write(f"publish:{{tag}}\\n")
    entered = os.environ.get("AGCOORD_TEST_PUBLISH_ENTERED")
    if entered:
        Path(entered).touch()
    release = os.environ.get("AGCOORD_TEST_PUBLISH_RELEASE")
    while release and not Path(release).exists():
        time.sleep(0.01)
    mutation = payload["variables"]["input"]["clientMutationId"]
    print(json.dumps({{"data": {{"updateRefs": {{"clientMutationId": mutation}}}}}}))
elif arguments[:1] == ["api"] and "/compare/" in arguments[1]:
    merge_base = (
        "0" * 40
        if os.environ.get("AGCOORD_TEST_DYNAMIC_HEAD") == "1"
        else os.environ["AGCOORD_TEST_HEAD"]
    )
    print(json.dumps({{
        "merge_base_commit": {{"sha": merge_base}},
    }}))
else:
    print(f"unexpected gh arguments: {{arguments!r}}", file=sys.stderr)
    raise SystemExit(93)
""",
        encoding="utf-8",
    )
    executable.chmod(0o755)
    return bin_dir


def _land_environment(
    bin_dir: Path,
    *,
    branch: str,
    head_sha: str,
    tag: str,
    event_log: Path,
    publish_entered: Path | None = None,
    publish_release: Path | None = None,
    preflight_entered: Path | None = None,
    preflight_release: Path | None = None,
    dynamic_head: bool = False,
) -> dict[str, str]:
    environment = caller_environment()
    environment.update(
        {
            "PATH": f"{bin_dir}{os.pathsep}{environment['PATH']}",
            "AGCOORD_TEST_BRANCH": branch,
            "AGCOORD_TEST_HEAD": head_sha,
            "AGCOORD_TEST_TAG": tag,
            "AGCOORD_TEST_EVENT_LOG": str(event_log),
        }
    )
    if publish_entered is not None:
        environment["AGCOORD_TEST_PUBLISH_ENTERED"] = str(publish_entered)
    if publish_release is not None:
        environment["AGCOORD_TEST_PUBLISH_RELEASE"] = str(publish_release)
    if preflight_entered is not None:
        environment["AGCOORD_TEST_PREFLIGHT_ENTERED"] = str(preflight_entered)
    if preflight_release is not None:
        environment["AGCOORD_TEST_PREFLIGHT_RELEASE"] = str(preflight_release)
    if dynamic_head:
        environment["AGCOORD_TEST_DYNAMIC_HEAD"] = "1"
    return environment


def _land_gate_command(
    event_log: Path,
    tag: str,
    *,
    entered: Path | None = None,
    release: Path | None = None,
) -> list[str]:
    return _python(
        """
from pathlib import Path
import sys
import time

event_log, tag, entered, release = sys.argv[1:]
with Path(event_log).open("a", encoding="utf-8") as stream:
    stream.write(f"gate:{tag}\\n")
print(f"gate transcript: {tag}", flush=True)
if entered:
    Path(entered).touch()
while release and not Path(release).exists():
    time.sleep(0.01)
""",
        event_log,
        tag,
        entered or "",
        release or "",
    )


def test_default_state_is_user_scoped_and_overrideable(monkeypatch, tmp_path: Path):
    xdg_state = tmp_path / "xdg"
    explicit = tmp_path / "explicit"
    monkeypatch.setenv("XDG_STATE_HOME", str(xdg_state))
    monkeypatch.delenv("AGCOORD_STATE_DIR", raising=False)
    assert state_dir_for() == (xdg_state / "agcoord").resolve()

    monkeypatch.setenv("AGCOORD_STATE_DIR", str(explicit))
    assert state_dir_for() == explicit.resolve()


def test_capacities_come_from_the_state_directory_configuration_file(tmp_path: Path):
    state_dir = tmp_path / "state"
    write_broker_config(state_dir, capacities={"jobs": 3, "cpu": 8, "browser": 1})
    running = RunningCoordinator(state_dir, capacities=None)
    client = running.start()
    try:
        assert client.snapshot()["capacities"] == {"jobs": 3, "cpu": 8, "browser": 1}
    finally:
        running.stop()


def test_absent_configuration_file_defaults_to_two_job_slots(tmp_path: Path):
    running = RunningCoordinator(tmp_path / "state", capacities=None)
    client = running.start()
    try:
        assert client.snapshot()["capacities"] == {"jobs": 2}
    finally:
        running.stop()


def test_fresh_database_uses_wal_and_the_configured_lock_timeout(tmp_path: Path):
    state_dir = tmp_path / "state"
    repository = _repository(tmp_path / "repository")
    write_broker_config(
        state_dir,
        capacities={"jobs": 1},
        database_timeout=0.05,
    )
    broker = CoordinatorBroker(state_dir, idle_timeout=None)
    locker = sqlite3.connect(broker.paths.database)
    try:
        assert locker.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        locker.execute("BEGIN IMMEDIATE")
        started = time.monotonic()
        with pytest.raises(sqlite3.OperationalError, match="locked"):
            broker.submit(
                _python("print('must not run')"),
                checkout=str(repository),
                resources={"jobs": 1},
                caller_pid=os.getpid(),
                environment=caller_environment(),
            )
        assert time.monotonic() - started < 1
    finally:
        locker.rollback()
        locker.close()
        broker.close()


def test_wal_writer_contention_does_not_stop_or_cancel_the_live_broker(
    tmp_path: Path,
):
    state_dir = tmp_path / "state"
    write_broker_config(
        state_dir,
        capacities={"jobs": 1},
        database_timeout=0.05,
    )
    running = RunningCoordinator(
        state_dir,
        capacities=None,
        idle_timeout=60,
    )
    client = running.start()
    repository = _repository(tmp_path / "repository")
    entered = tmp_path / "entered"
    release = tmp_path / "release"
    locker: sqlite3.Connection | None = None
    try:
        run_id = _submit(
            client,
            _blocking_command(entered, release, "locked full"),
            repository,
            kind="full",
        )
        wait_for(entered.exists, "the lock-test full gate never started")

        locker = sqlite3.connect(running.broker.paths.database)
        locker.execute("BEGIN EXCLUSIVE")
        locker.execute(
            "UPDATE coordinator_meta SET value = value WHERE key = 'protocol'"
        )
        release.touch()
        wait_for(
            lambda: "database is locked"
            in running.broker.paths.daemon_log.read_text(encoding="utf-8"),
            "the broker never encountered the held writer lock",
        )

        assert running.thread.is_alive()
        assert running.errors == []
        locked = client.status(run_id)
        assert locked["status"] == "running"
        assert locked["cancel_requested"] is False

        locker.rollback()
        locker.close()
        locker = None
        finished = _row(client, run_id, "passed")
        assert finished["cancel_requested"] is False
    finally:
        if locker is not None:
            locker.rollback()
            locker.close()
        release.touch()
        running.stop()


def test_idle_health_check_retries_a_transient_legacy_journal_lock(
    tmp_path: Path,
):
    state_dir = tmp_path / "state"
    write_broker_config(
        state_dir,
        capacities={"jobs": 1},
        database_timeout=0.05,
    )
    running = RunningCoordinator(
        state_dir,
        capacities=None,
        idle_timeout=60,
    )
    repository = _repository(tmp_path / "repository")
    entered = tmp_path / "entered"
    release = tmp_path / "release"
    locker: sqlite3.Connection | None = None
    try:
        with sqlite3.connect(running.broker.paths.database) as setup:
            assert setup.execute("PRAGMA journal_mode = DELETE").fetchone()[0] == "delete"

        running.thread.start()
        wait_for(
            lambda: running.broker.ready.is_set() or running.errors,
            "the legacy-journal broker never acquired ownership",
        )
        assert running.errors == []
        run_id = running.broker.submit(
            _blocking_command(entered, release, "legacy lock full"),
            checkout=str(repository),
            kind="full",
            resources={"jobs": 1},
            caller_pid=os.getpid(),
            environment=caller_environment(),
        )
        wait_for(entered.exists, "the legacy-lock full gate never started")

        locker = sqlite3.connect(running.broker.paths.database)
        locker.execute("BEGIN EXCLUSIVE")
        locker.execute(
            "UPDATE coordinator_meta SET value = value WHERE key = 'protocol'"
        )
        wait_for(
            lambda: (
                "idle check database contention"
                in running.broker.paths.daemon_log.read_text(encoding="utf-8")
            ),
            "the idle health check never observed the held database lock",
        )
        assert running.thread.is_alive()
        assert running.errors == []

        locker.rollback()
        locker.close()
        locker = None
        client = CoordinatorClient(state_dir=state_dir, autostart=False)
        preserved = client.status(run_id)
        assert preserved["status"] == "running"
        assert preserved["cancel_requested"] is False

        release.touch()
        assert _row(client, run_id, "passed")["cancel_requested"] is False
    finally:
        if locker is not None:
            locker.rollback()
            locker.close()
        release.touch()
        running.stop()


def test_capacity_environment_variable_no_longer_configures_a_broker(
    monkeypatch,
    tmp_path: Path,
):
    monkeypatch.setenv("AGCOORD_CAPACITIES", '{"jobs": 9, "cpu": 9}')
    running = RunningCoordinator(tmp_path / "state", capacities=None)
    client = running.start()
    try:
        assert client.snapshot()["capacities"] == {"jobs": 2}
    finally:
        running.stop()


@pytest.mark.parametrize(
    "document",
    [
        "{not json",
        "[]",
        '{"capacities": {"jobs": 2}, "unknown": 1}',
        '{"capacities": []}',
        '{"cgroup_root": ""}',
        '{"database_timeout": 0}',
        '{"database_timeout": -1}',
        '{"database_timeout": true}',
        '{"database_timeout": "10"}',
        '{"database_timeout": 2147483.648}',
    ],
)
def test_malformed_configuration_file_is_rejected_when_the_broker_loads(
    tmp_path: Path,
    document: str,
):
    state_dir = tmp_path / "state"
    state_dir.mkdir(parents=True)
    config_path(state_dir).write_text(document, encoding="utf-8")
    with pytest.raises(CoordinatorError, match="configuration"):
        CoordinatorBroker(state_dir, idle_timeout=None)


def test_resource_bindings_from_the_configuration_file_freeze_in_broker_metadata(
    tmp_path: Path,
):
    state_dir = tmp_path / "state"
    configuration = write_broker_config(
        state_dir,
        capacities={"jobs": 1, "memory": 1024},
        bindings={
            "memory": {
                "backend": "cgroup-v2",
                "kind": "memory",
                "mode": "best-effort",
                "unit": "bytes",
            }
        },
    )
    running = RunningCoordinator(state_dir, capacities=None)
    client = running.start()
    repository = _repository(tmp_path / "repository")
    try:
        snapshot = client.snapshot()
        assert snapshot["resource_bindings"]["memory"] == {
            "backend": "cgroup-v2",
            "kind": "memory",
            "mode": "best-effort",
            "unit": "bytes",
        }
        assert snapshot["resource_capabilities"] == {
            "cgroup-v2": {
                "available": False,
                "kinds": [],
                "operations": [],
                "reason": "delegation-unconfigured",
                "units": [],
            }
        }
        configuration.write_text(json.dumps({"bindings": {}}), encoding="utf-8")
        run_id = _submit(
            client,
            _python("print('frozen binding')"),
            repository,
            resources={"memory": 512},
        )
        row = _row(client, run_id, "passed")
        assert row["resource_contract"]["memory"] == snapshot["resource_bindings"]["memory"]
        assert row["resource_receipt"]["events"][0]["code"] == "delegation-unconfigured"
    finally:
        running.stop()


def test_resource_binding_rejects_ambiguous_kind_unit_pairs(tmp_path: Path):
    with pytest.raises(CoordinatorError, match="unit|kind|cpu|bytes"):
        CoordinatorBroker(
            tmp_path / "state",
            capacities={"jobs": 1, "cpu": 1},
            resource_bindings={
                "cpu": {
                    "backend": "test",
                    "kind": "cpu",
                    "mode": "required",
                    "unit": "bytes",
                }
            },
            idle_timeout=None,
        )


@pytest.mark.parametrize("legacy_protocol", [1, 2])
def test_legacy_history_requires_explicit_migration_without_inventing_enforcement(
    tmp_path: Path,
    legacy_protocol: int,
):
    state_dir = tmp_path / "legacy-state"
    state_dir.mkdir(mode=0o700)
    database = state_dir / "queue.sqlite3"
    phase_column = "phase TEXT NOT NULL," if legacy_protocol == 2 else ""
    gate_columns = (
        "gate_exit_status INTEGER, reported_exit_status INTEGER,"
        if legacy_protocol == 2
        else ""
    )
    phase_name = ", phase" if legacy_protocol == 2 else ""
    phase_value = ", 'complete'" if legacy_protocol == 2 else ""
    with sqlite3.connect(database) as db:
        db.executescript(
            f"""
            CREATE TABLE coordinator_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            INSERT INTO coordinator_meta(key, value) VALUES ('protocol', '{legacy_protocol}');
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
            );
            INSERT INTO runs (
                run_id, status, kind, label, agent, repository_id, repository,
                worktree_id, checkout, branch, head_sha, barrier, resources_json,
                caller_pid, command_json, environment_json, created_at, started_at,
                finished_at, exit_status{phase_name}
            ) VALUES (
                'full-legacy', 'passed', 'full', 'legacy full', 'legacy-agent',
                'repo-legacy', '/repos/legacy.git', 'worktree-legacy',
                '/worktrees/legacy', 'main',
                'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 1, '{{"jobs":1}}',
                42, '["true"]', '{{}}', '2026-08-30T12:00:00+00:00',
                '2026-08-30T12:00:01+00:00', '2026-08-30T12:00:02+00:00', 0
                {phase_value}
            );
            """
        )
    database.chmod(0o600)

    with pytest.raises(CoordinatorError, match="migrate|protocol"):
        CoordinatorBroker(
            state_dir=state_dir,
            capacities={"jobs": 1},
            idle_timeout=None,
        )

    assert migrate_queue(state_dir=state_dir) == {
        "changed": True,
        "from_protocol": legacy_protocol,
        "to_protocol": PROTOCOL,
    }
    with sqlite3.connect(database) as db:
        assert db.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
    running = RunningCoordinator(state_dir, capacities={"jobs": 1})
    client = running.start()
    try:
        legacy = client.status("full-legacy")
        assert set(legacy) == ROW_KEYS
        assert legacy["kind"] == "full"
        assert legacy["phase"] == "complete"
        assert legacy["gate_exit_status"] is None
        assert legacy["publication"] is None
        assert legacy["resource_contract"] == {
            "jobs": {
                "backend": None,
                "kind": "generic",
                "mode": "admission-only",
                "unit": "admission-unit",
            }
        }
        assert legacy["resource_receipt"] == {
            "requested": {"jobs": 1},
            "applied": {},
            "peak": {},
            "events": [],
        }
    finally:
        running.stop()


def test_submit_and_snapshot_have_the_strict_generic_schema(coordinator, tmp_path: Path):
    _broker, client = coordinator
    repository = _repository(tmp_path / "repository")
    run_id = client.submit(
        _python("print('hello from agcoord')"),
        checkout=str(repository),
        label="small check",
        agent="pytest",
        caller_pid=os.getpid(),
        environment=caller_environment(),
    )
    row = _row(client, run_id, "passed")
    snapshot = client.snapshot()

    assert set(row) == ROW_KEYS
    assert set(snapshot) == SNAPSHOT_KEYS
    assert snapshot["protocol"] == PROTOCOL
    assert snapshot["capacities"] == {"jobs": 2}
    assert snapshot["allocations"] == {"jobs": 0}
    assert row["kind"] == "check"
    assert row["label"] == "small check"
    assert row["agent"] == "pytest"
    assert row["resources"] == {"jobs": 1}
    assert row["resource_contract"] == {
        "jobs": {
            "backend": None,
            "kind": "generic",
            "mode": "admission-only",
            "unit": "admission-unit",
        }
    }
    assert row["resource_receipt"] == {
        "requested": {"jobs": 1},
        "applied": {},
        "peak": {},
        "events": [],
    }
    assert snapshot["resource_bindings"] == {}
    assert snapshot["resource_capabilities"] == {}
    assert row["barrier"] is False
    assert row["head_sha"] is None
    assert row["gate_run_id"] is None
    assert row["publication"] is None
    assert row["failure_reason"] is None
    assert row["phase"] == "complete"
    assert row["gate_exit_status"] is None
    assert isinstance(row["repository_id"], str) and row["repository_id"]
    assert isinstance(row["worktree_id"], str) and row["worktree_id"]
    assert row["checkout"] == str(repository.resolve())


def test_unnamed_agent_is_stable_while_explicit_identity_and_caller_pid_are_kept(
    coordinator,
    tmp_path: Path,
    monkeypatch,
):
    _broker, client = coordinator
    repository = _repository(tmp_path / "repository")
    monkeypatch.delenv("AGCOORD_AGENT", raising=False)

    first_id = client.submit(
        _python("print('first unnamed')"),
        checkout=str(repository),
        caller_pid=4101,
        environment=caller_environment(),
    )
    second_id = client.submit(
        _python("print('second unnamed')"),
        checkout=str(repository),
        caller_pid=4102,
        environment=caller_environment(),
    )
    monkeypatch.setenv("AGCOORD_AGENT", "environment-agent")
    environment_id = client.submit(
        _python("print('environment identity')"),
        checkout=str(repository),
        caller_pid=4103,
        environment=caller_environment(),
    )
    explicit_id = client.submit(
        _python("print('explicit identity')"),
        checkout=str(repository),
        agent="explicit-agent",
        caller_pid=4104,
        environment=caller_environment(),
    )

    rows = [
        _row(client, run_id, "passed")
        for run_id in (first_id, second_id, environment_id, explicit_id)
    ]
    assert [row["agent"] for row in rows] == [
        "unnamed",
        "unnamed",
        "environment-agent",
        "explicit-agent",
    ]
    assert [row["caller_pid"] for row in rows] == [4101, 4102, 4103, 4104]


RESOURCE_BINDING = {
    "cpu": {
        "kind": "cpu",
        "unit": "logical-cpu",
        "mode": "required",
        "backend": "test",
    }
}


class RecordingResourceBackend:
    def __init__(self, *, units: tuple[str, ...] = ("logical-cpu",)) -> None:
        self.units = units
        self.calls: list[tuple[str, object]] = []

    def probe(self) -> dict[str, object]:
        self.calls.append(("probe", None))
        return {
            "available": True,
            "kinds": ["cpu"],
            "units": list(self.units),
            "operations": [
                "prepare",
                "attach",
                "usage",
                "finish",
                "cancel",
                "cleanup",
            ],
            "reason": None,
        }

    def prepare(self, request) -> dict[str, object]:
        self.calls.append(("prepare", request))
        return {"token": request.run_id}

    def attach(self, request, state: dict[str, object], worker_pid: int) -> None:
        self.calls.append(("attach", (request, dict(state), worker_pid)))

    def usage(self, request, state: dict[str, object]) -> dict[str, int]:
        self.calls.append(("usage", (request, dict(state))))
        return {name: 1 for name in request.resources}

    def finish(self, request, state: dict[str, object]) -> dict[str, int]:
        self.calls.append(("finish", (request, dict(state))))
        return {name: 1 for name in request.resources}

    def cancel(self, request, state: dict[str, object]) -> None:
        self.calls.append(("cancel", (request, dict(state))))

    def cleanup(self, request, state: dict[str, object]) -> None:
        self.calls.append(("cleanup", (request, dict(state))))


class ObservingResourceBackend(RecordingResourceBackend):
    def _measurement(self, request) -> ResourceMeasurement:
        return ResourceMeasurement(
            {name: 1 for name in request.resources},
            tuple(
                ResourceObservation(name, "cpu-throttled")
                for name in request.resources
            ),
        )

    def usage(self, request, state: dict[str, object]) -> ResourceMeasurement:
        self.calls.append(("usage", (request, dict(state))))
        return self._measurement(request)

    def finish(self, request, state: dict[str, object]) -> ResourceMeasurement:
        self.calls.append(("finish", (request, dict(state))))
        return self._measurement(request)


def test_unbound_resource_names_keep_admission_only_meaning(tmp_path: Path):
    running = RunningCoordinator(
        tmp_path / "state",
        capacities={"jobs": 1, "browser": 1},
    )
    client = running.start()
    repository = _repository(tmp_path / "repository")
    try:
        run_id = _submit(
            client,
            _python("print('generic')"),
            repository,
            resources={"browser": 1},
        )
        row = _row(client, run_id, "passed")
        assert row["resource_contract"]["browser"] == {
            "backend": None,
            "kind": "generic",
            "mode": "admission-only",
            "unit": "admission-unit",
        }
        assert row["resource_receipt"] == {
            "requested": {"browser": 1, "jobs": 1},
            "applied": {},
            "peak": {},
            "events": [],
        }
    finally:
        running.stop()


def test_required_unavailable_resource_backend_refuses_before_user_code(tmp_path: Path):
    running = RunningCoordinator(
        tmp_path / "state",
        capacities={"jobs": 1, "cpu": 1},
        resource_bindings=RESOURCE_BINDING,
    )
    client = running.start()
    repository = _repository(tmp_path / "repository")
    marker = tmp_path / "user-code-ran"
    try:
        run_id = _submit(
            client,
            _python("from pathlib import Path; Path(__import__('sys').argv[1]).touch()", marker),
            repository,
            resources={"cpu": 1},
        )
        row = _row(client, run_id, "failed")
        assert not marker.exists()
        assert row["exit_status"] == 125
        assert row["failure_reason"] == "resource-enforcement-failed"
        assert row["resource_receipt"]["applied"] == {}
        assert {
            (event["resource"], event["stage"], event["status"], event["code"])
            for event in row["resource_receipt"]["events"]
        } == {("cpu", "probe", "failed", "backend-unavailable")}
    finally:
        running.stop()


def test_best_effort_unavailable_resource_is_visible_without_claiming_application(
    tmp_path: Path,
):
    binding = {
        "cpu": {
            **RESOURCE_BINDING["cpu"],
            "mode": "best-effort",
        }
    }
    running = RunningCoordinator(
        tmp_path / "state",
        capacities={"jobs": 1, "cpu": 1},
        resource_bindings=binding,
    )
    client = running.start()
    repository = _repository(tmp_path / "repository")
    try:
        run_id = _submit(client, _python("print('ran')"), repository, resources={"cpu": 1})
        row = _row(client, run_id, "passed")
        assert row["resource_receipt"]["applied"] == {}
        event = row["resource_receipt"]["events"][0]
        assert (event["resource"], event["status"], event["code"]) == (
            "cpu",
            "unapplied",
            "backend-unavailable",
        )
    finally:
        running.stop()


def test_backend_lifecycle_applies_and_measures_a_typed_resource(tmp_path: Path):
    backend = RecordingResourceBackend()
    running = RunningCoordinator(
        tmp_path / "state",
        capacities={"jobs": 1, "cpu": 1},
        resource_bindings=RESOURCE_BINDING,
        resource_backends={"test": backend},
    )
    client = running.start()
    repository = _repository(tmp_path / "repository")
    try:
        run_id = _submit(client, _python("print('enforced')"), repository, resources={"cpu": 1})
        row = _row(client, run_id, "passed")
        assert row["resource_contract"]["cpu"] == RESOURCE_BINDING["cpu"]
        assert row["resource_receipt"]["applied"] == {"cpu": 1}
        assert row["resource_receipt"]["peak"] == {"cpu": 1}
        stages = [name for name, _detail in backend.calls]
        assert stages[0] == "probe"
        assert stages.index("prepare") < stages.index("attach")
        assert stages.index("attach") < stages.index("finish")
        assert stages.index("finish") < stages.index("cleanup")
    finally:
        running.stop()


def test_backend_observations_are_sanitized_and_recorded_once(tmp_path: Path):
    backend = ObservingResourceBackend()
    running = RunningCoordinator(
        tmp_path / "state",
        capacities={"jobs": 1, "cpu": 1},
        resource_bindings=RESOURCE_BINDING,
        resource_backends={"test": backend},
    )
    client = running.start()
    repository = _repository(tmp_path / "repository")
    entered = tmp_path / "entered"
    release = tmp_path / "release"
    try:
        run_id = _submit(
            client,
            _blocking_command(entered, release, "observed resource"),
            repository,
            resources={"cpu": 1},
        )
        wait_for(entered.exists, "the observed resource worker did not start")

        def observation_recorded() -> bool:
            return any(
                event["code"] == "cpu-throttled"
                for event in client.status(run_id)["resource_receipt"]["events"]
            )

        wait_for(observation_recorded, "the backend observation was not recorded")
        release.touch()
        row = _row(client, run_id, "passed")
        observations = [
            event
            for event in row["resource_receipt"]["events"]
            if event["code"] == "cpu-throttled"
        ]
        assert len(observations) == 1
        assert observations[0]["status"] == "recorded"
        assert row["resource_receipt"]["peak"] == {"cpu": 1}
    finally:
        release.touch()
        running.stop()


def test_typed_resource_receipt_survives_an_idle_broker_restart(tmp_path: Path):
    state_dir = tmp_path / "state"
    repository = _repository(tmp_path / "repository")
    first_backend = RecordingResourceBackend()
    first = RunningCoordinator(
        state_dir,
        capacities={"jobs": 1, "cpu": 1},
        resource_bindings=RESOURCE_BINDING,
        resource_backends={"test": first_backend},
    )
    first_client = first.start()
    try:
        run_id = _submit(
            first_client,
            _python("print('durable receipt')"),
            repository,
            resources={"cpu": 1},
        )
        original = _row(first_client, run_id, "passed")
    finally:
        first.stop()

    replacement = RunningCoordinator(
        state_dir,
        capacities={"jobs": 1, "cpu": 1},
        resource_bindings=RESOURCE_BINDING,
        resource_backends={"test": RecordingResourceBackend()},
    )
    replacement_client = replacement.start()
    try:
        restored = replacement_client.status(run_id)
        assert restored["resource_contract"] == original["resource_contract"]
        assert restored["resource_receipt"] == original["resource_receipt"]
    finally:
        replacement.stop()


def test_backend_lifecycle_receives_cancellation_before_cleanup(tmp_path: Path):
    backend = RecordingResourceBackend()
    running = RunningCoordinator(
        tmp_path / "state",
        capacities={"jobs": 1, "cpu": 1},
        resource_bindings=RESOURCE_BINDING,
        resource_backends={"test": backend},
    )
    client = running.start()
    repository = _repository(tmp_path / "repository")
    entered = tmp_path / "entered"
    release = tmp_path / "release"
    try:
        run_id = _submit(
            client,
            _blocking_command(entered, release, "enforced blocker"),
            repository,
            resources={"cpu": 1},
        )
        wait_for(entered.exists, "the enforced blocker never started")
        client.cancel(run_id)
        row = _row(client, run_id, "cancelled")
        stages = [name for name, _detail in backend.calls]
        assert stages.index("attach") < stages.index("cancel")
        assert stages.index("cancel") < stages.index("finish")
        assert stages.index("finish") < stages.index("cleanup")
        assert any(
            event["stage"] == "cancel" and event["code"] == "cancelled"
            for event in row["resource_receipt"]["events"]
        )
    finally:
        release.touch()
        running.stop()


def test_backend_exception_paths_are_not_exposed_in_public_receipts(tmp_path: Path):
    class FailingBackend(RecordingResourceBackend):
        def prepare(self, request):
            raise RuntimeError("/sys/fs/cgroup/private-machine-path")

    running = RunningCoordinator(
        tmp_path / "state",
        capacities={"jobs": 1, "cpu": 1},
        resource_bindings=RESOURCE_BINDING,
        resource_backends={"test": FailingBackend()},
    )
    client = running.start()
    repository = _repository(tmp_path / "repository")
    try:
        run_id = _submit(client, _python("print('must not run')"), repository, resources={"cpu": 1})
        row = _row(client, run_id, "failed")
        public_json = json.dumps(row, sort_keys=True)
        assert "private-machine-path" not in public_json
        assert "prepare-failed" in public_json
    finally:
        running.stop()


def test_required_binding_fails_when_backend_does_not_support_its_unit(tmp_path: Path):
    backend = RecordingResourceBackend(units=("bytes",))
    running = RunningCoordinator(
        tmp_path / "state",
        capacities={"jobs": 1, "cpu": 1},
        resource_bindings=RESOURCE_BINDING,
        resource_backends={"test": backend},
    )
    client = running.start()
    repository = _repository(tmp_path / "repository")
    marker = tmp_path / "user-code-ran"
    try:
        run_id = _submit(
            client,
            _python("from pathlib import Path; Path(__import__('sys').argv[1]).touch()", marker),
            repository,
            resources={"cpu": 1},
        )
        row = _row(client, run_id, "failed")
        assert not marker.exists()
        assert row["resource_receipt"]["events"][0]["code"] == "unit-unsupported"
        assert all(name not in {"prepare", "attach"} for name, _detail in backend.calls)
    finally:
        running.stop()


def test_discovered_repository_names_are_readable_for_remote_and_local_checkouts(
    coordinator,
    tmp_path: Path,
):
    _broker, client = coordinator
    remote = _repository(tmp_path / "remote-checkout")
    _git(remote, "remote", "add", "origin", "git@github.com:example/widgets.git")
    local = _repository(tmp_path / "local-widgets")

    remote_id = _submit(
        client,
        _python("print('remote')"),
        remote,
        label="remote",
    )
    local_id = _submit(
        client,
        _python("print('local')"),
        local,
        label="local",
    )
    remote_row = _row(client, remote_id, "passed")
    local_row = _row(client, local_id, "passed")

    assert remote_row["repository"] == "github.com/example/widgets"
    assert local_row["repository"] == str((local / ".git").resolve())
    assert remote_row["repository_id"].startswith("repo-")
    assert local_row["repository_id"].startswith("repo-")


def test_unrelated_repositories_share_state_but_keep_stable_distinct_identities(
    coordinator,
    tmp_path: Path,
):
    broker, client = coordinator
    first = _repository(tmp_path / "first")
    linked = tmp_path / "first-linked"
    _git(first, "worktree", "add", "-b", "linked", str(linked))
    second = _repository(tmp_path / "second", content="other\n")
    second_client = CoordinatorClient(
        state_dir=broker.paths.state_dir,
        autostart=False,
    )

    run_ids = [
        _submit(
            client,
            _python("print('first')"),
            first,
            label="first",
            agent="agent-a",
        ),
        _submit(
            second_client,
            _python("print('linked')"),
            linked,
            label="linked",
            agent="agent-b",
        ),
        _submit(
            second_client,
            _python("print('second')"),
            second,
            label="second",
            agent="agent-b",
        ),
    ]
    first_row, linked_row, second_row = [
        _row(client, run_id, "passed") for run_id in run_ids
    ]

    assert first_row["repository_id"] == linked_row["repository_id"]
    assert first_row["repository"] == linked_row["repository"]
    assert first_row["worktree_id"] != linked_row["worktree_id"]
    assert first_row["repository_id"] != second_row["repository_id"]
    assert first_row["repository"] != second_row["repository"]
    assert [first_row["agent"], linked_row["agent"], second_row["agent"]] == [
        "agent-a",
        "agent-b",
        "agent-b",
    ]
    assert len(client.snapshot()["recent"]) == 3


def test_explicit_repository_identity_joins_distinct_git_worktrees_to_one_lane(
    coordinator,
    tmp_path: Path,
):
    _broker, client = coordinator
    first = _repository(tmp_path / "checkout-a")
    second = _repository(tmp_path / "checkout-b", content="second\n")
    run_ids = [
        client.submit(
            _python("print('a')"),
            checkout=str(first),
            repository="example/widgets",
            branch="feature/a",
            resources={"jobs": 1},
            caller_pid=os.getpid(),
            environment=caller_environment(),
        ),
        client.submit(
            _python("print('b')"),
            checkout=str(second),
            repository="example/widgets",
            branch="feature/b",
            resources={"jobs": 1},
            caller_pid=os.getpid(),
            environment=caller_environment(),
        ),
    ]
    first_row, second_row = [_row(client, run_id, "passed") for run_id in run_ids]

    assert first_row["repository"] == second_row["repository"] == "example/widgets"
    assert first_row["repository_id"] == second_row["repository_id"]
    assert first_row["worktree_id"] != second_row["worktree_id"]


@pytest.mark.parametrize("resources", [{"cpu": 3}, {"cpu": 0}, {"cpu": -1}])
def test_impossible_or_nonpositive_resource_requests_are_never_queued(
    tmp_path: Path,
    resources: dict[str, int],
):
    running = RunningCoordinator(
        tmp_path / "state",
        capacities={"jobs": 2, "cpu": 2},
    )
    client = running.start()
    repository = _repository(tmp_path / "repository")
    try:
        with pytest.raises(CoordinatorError, match="resource|capacity|positive|cpu"):
            client.submit(
                _python("raise SystemExit('must not run')"),
                checkout=str(repository),
                resources=resources,
                caller_pid=os.getpid(),
                environment=caller_environment(),
            )
        snapshot = client.snapshot()
        assert snapshot["active"] == []
        assert snapshot["queued"] == []
        assert snapshot["recent"] == []
    finally:
        running.stop()


def test_resources_allow_cross_repo_overlap_but_full_is_a_lane_barrier(tmp_path: Path):
    running = RunningCoordinator(
        tmp_path / "state",
        capacities={"jobs": 4, "cpu": 2},
    )
    client = running.start()
    first = _repository(tmp_path / "first")
    second = _repository(tmp_path / "second")
    entered_first = tmp_path / "entered-first"
    release_first = tmp_path / "release-first"
    entered_full = tmp_path / "entered-full"
    release_full = tmp_path / "release-full"
    entered_later = tmp_path / "entered-later"
    release_later = tmp_path / "release-later"
    entered_other = tmp_path / "entered-other"
    release_other = tmp_path / "release-other"

    try:
        first_id = _submit(
            client,
            _blocking_command(entered_first, release_first, "first check"),
            first,
            resources={"cpu": 1},
        )
        full_id = _submit(
            client,
            _blocking_command(entered_full, release_full, "full barrier"),
            first,
            kind="full",
            label="full gate",
            resources={"cpu": 1},
        )
        later_id = _submit(
            client,
            _blocking_command(entered_later, release_later, "later check"),
            first,
            resources={"cpu": 1},
        )
        other_id = _submit(
            client,
            _blocking_command(entered_other, release_other, "other repository"),
            second,
            resources={"cpu": 1},
        )

        wait_for(entered_first.exists, "the first lane job did not start")
        wait_for(entered_other.exists, "compatible cross-repository work did not overlap")
        assert client.snapshot()["allocations"] == {"jobs": 2, "cpu": 2}
        assert client.status(full_id)["status"] == "queued"
        assert client.status(full_id)["barrier"] is True
        assert any(first_id in blocker for blocker in client.status(full_id)["blocked_by"])
        assert client.status(later_id)["status"] == "queued"
        assert any(full_id in blocker for blocker in client.status(later_id)["blocked_by"])
        assert not entered_later.exists()

        release_first.touch()
        _row(client, first_id, "passed")
        wait_for(entered_full.exists, "the full barrier did not start after earlier lane work")
        assert client.status(later_id)["status"] == "queued"
        assert not entered_later.exists()
        assert client.status(other_id)["status"] == "running"

        release_full.touch()
        _row(client, full_id, "passed")
        wait_for(entered_later.exists, "later lane work did not start after the full barrier")
        release_later.touch()
        release_other.touch()
        assert _row(client, later_id, "passed")["repository_id"] == client.status(full_id)[
            "repository_id"
        ]
        _row(client, other_id, "passed")
    finally:
        for path in (release_first, release_full, release_later, release_other):
            path.touch()
        running.stop()


def test_full_derives_a_clean_exact_head_and_dirty_checkout_accepts_no_row(
    coordinator,
    tmp_path: Path,
):
    _broker, client = coordinator
    repository = _repository(tmp_path / "repository")
    receipt_id = _submit(
        client,
        _python("print('full passed')"),
        repository,
        kind="full",
        label="release gate",
    )
    receipt = _row(client, receipt_id, "passed")

    assert receipt["kind"] == "full"
    assert receipt["barrier"] is True
    assert receipt["head_sha"] == _head(repository)
    assert receipt["branch"] == "main"
    assert receipt["phase"] == "complete"
    assert receipt["gate_exit_status"] is None
    assert len(receipt["head_sha"]) == 40

    (repository / "untracked.txt").write_text("dirty\n", encoding="utf-8")
    with pytest.raises(CoordinatorError, match="clean|dirty|receipt"):
        _submit(
            client,
            _python("raise SystemExit('must not run')"),
            repository,
            kind="full",
        )
    rows = [
        *client.snapshot()["active"],
        *client.snapshot()["queued"],
        *client.snapshot()["recent"],
    ]
    assert [row["run_id"] for row in rows] == [receipt_id]


def test_land_submission_is_one_exact_head_publication_barrier(
    coordinator,
    tmp_path: Path,
):
    _broker, client = coordinator
    repository = _repository(tmp_path / "repository")
    entered = tmp_path / "entered"
    release = tmp_path / "release"
    blocker_id = _submit(
        client,
        _blocking_command(entered, release, "earlier lane work"),
        repository,
    )
    wait_for(entered.exists, "the earlier repository job did not start")
    gate_command = _python("print('exact land gate')")
    environment = caller_environment()
    environment["AGCOORD_TEST_LAND_ENV"] = "captured-by-request"

    try:
        land_id = client.submit_land(
            "github",
            123,
            gate_command,
            checkout=str(repository),
            label="gate and publish PR 123",
            resources={"jobs": 1},
            agent="pytest-land",
            head_sha=_head(repository),
            caller_pid=os.getpid(),
            environment=environment,
        )
        land = client.status(land_id)

        assert set(land) == ROW_KEYS
        assert land["run_id"].startswith("land-")
        assert land["status"] == "queued"
        assert land["phase"] == "queued"
        assert land["kind"] == "land"
        assert land["barrier"] is True
        assert land["publication"] == {"adapter": "github", "request": 123}
        assert land["gate_run_id"] is None
        assert land["head_sha"] == _head(repository)
        assert land["checkout"] == str(repository.resolve())
        assert land["command"] == gate_command
        assert land["resources"] == {"jobs": 1}
        assert land["gate_exit_status"] is None
        assert land["failure_reason"] is None
        assert any(blocker_id in blocker for blocker in land["blocked_by"])

        cancelled = client.cancel(land_id)
        assert cancelled["status"] == "cancelled"
        assert cancelled["phase"] == "complete"
        assert cancelled["gate_exit_status"] is None
    finally:
        release.touch()
        _row(client, blocker_id, "passed")


def test_land_target_sync_updates_the_durable_head_before_the_gate(
    coordinator,
    tmp_path: Path,
):
    _broker, client = coordinator
    checkout, remote, branch, old_head = _publication_repository(
        tmp_path / "repository"
    )
    target_checkout = tmp_path / "target-checkout"
    subprocess.run(
        [GIT, "clone", "--branch", "main", str(remote), str(target_checkout)],
        check=True,
        capture_output=True,
        text=True,
    )
    _git(target_checkout, "config", "user.name", "AGCoord target test")
    _git(target_checkout, "config", "user.email", "target@example.invalid")
    (target_checkout / "target.txt").write_text("advanced\n", encoding="utf-8")
    _git(target_checkout, "add", "target.txt")
    _git(target_checkout, "commit", "-m", "advance target")
    advanced_main = _head(target_checkout)
    _git(target_checkout, "push", "origin", "main")

    bin_dir = _install_land_gh(tmp_path)
    events = tmp_path / "events"
    gate_report = tmp_path / "gate-report.json"
    environment = _land_environment(
        bin_dir,
        branch=branch,
        head_sha=old_head,
        tag="target-sync",
        event_log=events,
        dynamic_head=True,
    )
    gate = _python(
        """
import json
import os
from pathlib import Path
import subprocess

from agcoord.queue import CoordinatorClient

head = subprocess.run(
    ["git", "rev-parse", "HEAD"],
    check=True,
    text=True,
    capture_output=True,
).stdout.strip()
row = CoordinatorClient(
    state_dir=os.environ["AGCOORD_STATE_DIR"],
    checkout=Path.cwd(),
    autostart=False,
).status(os.environ["AGCOORD_RUN_ID"])
Path(os.environ["AGCOORD_TEST_GATE_REPORT"]).write_text(
    json.dumps({"checkout_head": head, "durable_head": row["head_sha"]}),
    encoding="utf-8",
)
"""
    )
    environment["AGCOORD_TEST_GATE_REPORT"] = str(gate_report)

    land_id = client.submit_land(
        "github",
        123,
        gate,
        checkout=str(checkout),
        resources={"jobs": 1},
        caller_pid=os.getpid(),
        environment=environment,
    )
    receipt = _row(client, land_id, "passed")
    observed = json.loads(gate_report.read_text(encoding="utf-8"))
    merge_head = observed["checkout_head"]

    assert merge_head != old_head
    assert observed["durable_head"] == merge_head
    assert receipt["head_sha"] == merge_head
    assert _head(checkout) == merge_head
    assert _git(
        checkout,
        "show",
        "-s",
        "--format=%P",
        merge_head,
    ).split() == [old_head, advanced_main]
    remote_head = _git(
        checkout,
        "ls-remote",
        "origin",
        f"refs/heads/{branch}",
    ).split()[0]
    assert remote_head == merge_head
    assert events.read_text(encoding="utf-8").splitlines() == [
        "publish:target-sync"
    ]


def test_land_target_sync_opt_out_retains_the_stale_target_refusal(
    coordinator,
    tmp_path: Path,
):
    _broker, client = coordinator
    checkout, remote, branch, old_head = _publication_repository(
        tmp_path / "repository"
    )
    target_checkout = tmp_path / "target-checkout"
    subprocess.run(
        [GIT, "clone", "--branch", "main", str(remote), str(target_checkout)],
        check=True,
        capture_output=True,
        text=True,
    )
    _git(target_checkout, "config", "user.name", "AGCoord target test")
    _git(target_checkout, "config", "user.email", "target@example.invalid")
    (target_checkout / "target.txt").write_text("advanced\n", encoding="utf-8")
    _git(target_checkout, "add", "target.txt")
    _git(target_checkout, "commit", "-m", "advance target")
    advanced_main = _head(target_checkout)
    _git(target_checkout, "push", "origin", "main")

    bin_dir = _install_land_gh(tmp_path)
    events = tmp_path / "events"
    gate_marker = tmp_path / "gate-ran"
    environment = _land_environment(
        bin_dir,
        branch=branch,
        head_sha=old_head,
        tag="target-sync-opt-out",
        event_log=events,
        dynamic_head=True,
    )

    land_id = client.submit_land(
        "github",
        123,
        _python(
            "from pathlib import Path; import sys; Path(sys.argv[1]).touch()",
            gate_marker,
        ),
        checkout=str(checkout),
        resources={"jobs": 1},
        caller_pid=os.getpid(),
        environment=environment,
        synchronize_target=False,
    )
    receipt = _row(client, land_id, "failed")

    assert receipt["exit_status"] == 75
    assert receipt["failure_reason"] == "stale-main"
    assert receipt["head_sha"] == old_head
    assert not gate_marker.exists()
    assert not events.exists()
    assert _head(checkout) == old_head
    assert _git(
        checkout,
        "ls-remote",
        "origin",
        f"refs/heads/{branch}",
    ).split()[0] == old_head
    assert _git(
        checkout,
        "ls-remote",
        "origin",
        "refs/heads/main",
    ).split()[0] == advanced_main


def test_land_rejects_dirty_or_nested_requests_without_accepting_a_row(
    coordinator,
    tmp_path: Path,
):
    _broker, client = coordinator
    checkout, _remote, _branch, head_sha = _publication_repository(
        tmp_path / "repository"
    )
    (checkout / "untracked.txt").write_text("dirty\n", encoding="utf-8")

    with pytest.raises(CoordinatorError, match="clean|dirty|head"):
        client.submit_land(
            "github",
            123,
            _python("raise SystemExit('must not run')"),
            checkout=str(checkout),
            head_sha=head_sha,
            caller_pid=os.getpid(),
            environment=caller_environment(),
        )
    (checkout / "untracked.txt").unlink()
    nested = caller_environment()
    nested["AGCOORD_RUN_ID"] = "land-parent"
    with pytest.raises(CoordinatorError, match="nested|AGCOORD_RUN_ID|coordinated"):
        client.submit_land(
            "github",
            123,
            _python("raise SystemExit('must not run')"),
            checkout=str(checkout),
            head_sha=head_sha,
            caller_pid=os.getpid(),
            environment=nested,
        )

    snapshot = client.snapshot()
    assert snapshot["active"] == []
    assert snapshot["queued"] == []
    assert snapshot["recent"] == []


def test_land_holds_lane_resources_through_publication_fifo(
    tmp_path: Path,
):
    running = RunningCoordinator(
        tmp_path / "state",
        capacities={"jobs": 2, "cpu": 1},
    )
    client = running.start()
    checkout, _remote, branch, head_sha = _publication_repository(
        tmp_path / "repository"
    )
    bin_dir = _install_land_gh(tmp_path)
    events = tmp_path / "events"
    first_publishing = tmp_path / "first-publishing"
    release_first = tmp_path / "release-first-publication"
    try:
        first_id = client.submit_land(
            "github",
            101,
            _land_gate_command(events, "first"),
            checkout=str(checkout),
            label="first atomic land",
            resources={"cpu": 1},
            caller_pid=os.getpid(),
            environment=_land_environment(
                bin_dir,
                branch=branch,
                head_sha=head_sha,
                tag="first",
                event_log=events,
                publish_entered=first_publishing,
                publish_release=release_first,
            ),
        )
        second_id = client.submit_land(
            "github",
            102,
            _land_gate_command(events, "second"),
            checkout=str(checkout),
            label="second atomic land",
            resources={"cpu": 1},
            caller_pid=os.getpid(),
            environment=_land_environment(
                bin_dir,
                branch=branch,
                head_sha=head_sha,
                tag="second",
                event_log=events,
            ),
        )

        wait_for(first_publishing.exists, "the first land never reached publication")
        first = client.status(first_id)
        second = client.status(second_id)
        assert first["status"] == "running"
        assert first["phase"] == "publishing"
        assert first["gate_exit_status"] == 0
        assert second["status"] == "queued"
        assert any(first_id in blocker for blocker in second["blocked_by"])
        assert client.snapshot()["allocations"] == {"jobs": 1, "cpu": 1}
        assert events.read_text(encoding="utf-8").splitlines() == [
            "gate:first",
            "publish:first",
        ]

        with pytest.raises(CoordinatorError, match="publishing|cannot be cancelled"):
            client.cancel(first_id)
        assert client.status(first_id)["cancel_requested"] is False

        release_first.touch()
        first = _row(client, first_id, "passed")
        second = _row(client, second_id, "passed")

        assert first["phase"] == second["phase"] == "complete"
        assert first["gate_exit_status"] == second["gate_exit_status"] == 0
        assert events.read_text(encoding="utf-8").splitlines() == [
            "gate:first",
            "publish:first",
            "gate:second",
            "publish:second",
        ]
        assert len(
            [
                row
                for row in client.snapshot()["recent"]
                if row["run_id"] in {first_id, second_id}
            ]
        ) == 2
        transcript = client.log(first_id)["text"]
        assert "gate transcript: first" in transcript
        assert "gate passed" in transcript.lower()
        assert "landed" in transcript.lower()
    finally:
        release_first.touch()
        running.stop()


def test_land_cancellation_during_gating_publishes_nothing(
    tmp_path: Path,
):
    running = RunningCoordinator(tmp_path / "state", capacities={"jobs": 1})
    client = running.start()
    checkout, _remote, branch, head_sha = _publication_repository(
        tmp_path / "repository"
    )
    bin_dir = _install_land_gh(tmp_path)
    events = tmp_path / "events"
    gate_entered = tmp_path / "gate-entered"
    gate_release = tmp_path / "gate-release"

    try:
        run_id = client.submit_land(
            "github",
            103,
            _land_gate_command(
                events,
                "cancelled",
                entered=gate_entered,
                release=gate_release,
            ),
            checkout=str(checkout),
            resources={"jobs": 1},
            caller_pid=os.getpid(),
            environment=_land_environment(
                bin_dir,
                branch=branch,
                head_sha=head_sha,
                tag="cancelled",
                event_log=events,
            ),
        )
        wait_for(gate_entered.exists, "the cancellable land gate never started")
        gating = client.status(run_id)
        assert gating["status"] == "running"
        assert gating["phase"] == "gating"
        assert gating["gate_exit_status"] is None

        requested = client.cancel(run_id)
        assert requested["cancel_requested"] is True
        cancelled = _row(client, run_id, "cancelled")
        assert cancelled["phase"] == "complete"
        assert cancelled["exit_status"] == 130
        assert cancelled["gate_exit_status"] is None
        assert events.read_text(encoding="utf-8").splitlines() == ["gate:cancelled"]
        assert "publish:cancelled" not in client.log(run_id)["text"]
    finally:
        gate_release.touch()
        running.stop()


def test_land_cancellation_during_preflight_never_starts_the_gate(
    tmp_path: Path,
):
    running = RunningCoordinator(tmp_path / "state", capacities={"jobs": 1})
    client = running.start()
    checkout, _remote, branch, head_sha = _publication_repository(
        tmp_path / "repository"
    )
    bin_dir = _install_land_gh(tmp_path)
    events = tmp_path / "events"
    preflight_entered = tmp_path / "preflight-entered"
    preflight_release = tmp_path / "preflight-release"

    try:
        run_id = client.submit_land(
            "github",
            105,
            _land_gate_command(events, "preflight-cancelled"),
            checkout=str(checkout),
            resources={"jobs": 1},
            caller_pid=os.getpid(),
            environment=_land_environment(
                bin_dir,
                branch=branch,
                head_sha=head_sha,
                tag="preflight-cancelled",
                event_log=events,
                preflight_entered=preflight_entered,
                preflight_release=preflight_release,
            ),
        )
        wait_for(preflight_entered.exists, "the cancellable preflight never started")
        preflight = client.status(run_id)
        assert preflight["status"] == "running"
        assert preflight["phase"] == "preflight"

        assert client.cancel(run_id)["cancel_requested"] is True
        cancelled = _row(client, run_id, "cancelled")
        assert cancelled["phase"] == "complete"
        assert cancelled["gate_exit_status"] is None
        assert not events.exists()
    finally:
        preflight_release.touch()
        running.stop()


def test_replacement_broker_finishes_one_recovered_land_without_rerunning_gate(
    tmp_path: Path,
):
    state_dir = tmp_path / "state"
    checkout, _remote, branch, head_sha = _publication_repository(
        tmp_path / "repository"
    )
    bin_dir = _install_land_gh(tmp_path)
    events = tmp_path / "events"
    gate_entered = tmp_path / "gate-entered"
    gate_release = tmp_path / "gate-release"
    owner = subprocess.Popen(
        _python(
            """
import sys
from agcoord.queue import CoordinatorBroker

broker = CoordinatorBroker(
    state_dir=sys.argv[1],
    capacities={"jobs": 1},
    idle_timeout=None,
)
broker.serve_forever()
""",
            state_dir,
        ),
        env=caller_environment(),
    )
    client = CoordinatorClient(state_dir=state_dir, autostart=False)
    replacement: RunningCoordinator | None = None
    worker_pid: int | None = None

    try:
        snapshot = wait_for(
            lambda: client.snapshot(),
            "the original land broker never acquired ownership",
        )
        assert snapshot["broker_pid"] == owner.pid
        run_id = client.submit_land(
            "github",
            104,
            _land_gate_command(
                events,
                "recovered",
                entered=gate_entered,
                release=gate_release,
            ),
            checkout=str(checkout),
            resources={"jobs": 1},
            caller_pid=os.getpid(),
            environment=_land_environment(
                bin_dir,
                branch=branch,
                head_sha=head_sha,
                tag="recovered",
                event_log=events,
            ),
        )
        wait_for(gate_entered.exists, "the recoverable land gate never started")
        live = client.status(run_id)
        assert live["phase"] == "gating"
        worker_pid = live["worker_pid"]
        assert isinstance(worker_pid, int)
        os.kill(owner.pid, signal.SIGKILL)
        owner.wait(timeout=5)
        replacement = RunningCoordinator(state_dir, capacities={"jobs": 1})
        recovered_client = replacement.start()
        recovered = recovered_client.status(run_id)
        assert recovered["status"] == "running"
        assert recovered["worker_pid"] == worker_pid
        assert recovered_client.snapshot()["allocations"] == {"jobs": 1}

        gate_release.touch()
        finished = _row(recovered_client, run_id, "passed")
        assert finished["phase"] == "complete"
        assert finished["gate_exit_status"] == 0
        assert finished["exit_status"] == 0
        assert finished["failure_reason"] is None
        assert events.read_text(encoding="utf-8").splitlines() == [
            "gate:recovered",
            "publish:recovered",
        ]
    finally:
        gate_release.touch()
        if owner.poll() is None:
            owner.terminate()
            owner.wait(timeout=5)
        if replacement is not None:
            replacement.stop()
        elif worker_pid is not None:
            try:
                os.killpg(worker_pid, signal.SIGKILL)
            except ProcessLookupError:
                pass


def test_replacement_broker_preserves_a_full_worker_after_the_owner_crashes(
    tmp_path: Path,
):
    state_dir = tmp_path / "state"
    repository = _repository(tmp_path / "repository")
    entered = tmp_path / "entered"
    release = tmp_path / "release"
    crash = tmp_path / "crash"
    owner = subprocess.Popen(
        _python(
            """
import sys
from pathlib import Path

from agcoord.queue import CoordinatorBroker

class CrashingBroker(CoordinatorBroker):
    def _should_idle_exit(self):
        if Path(sys.argv[2]).exists():
            raise RuntimeError("injected broker failure")
        return False

broker = CrashingBroker(
    state_dir=sys.argv[1],
    capacities={"jobs": 1},
    idle_timeout=None,
)
broker.serve_forever()
""",
            state_dir,
            crash,
        ),
        env=caller_environment(),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    client = CoordinatorClient(state_dir=state_dir, autostart=False)
    replacement: RunningCoordinator | None = None
    worker_pid: int | None = None

    try:
        snapshot = wait_for(
            lambda: client.snapshot(),
            "the original full broker never acquired ownership",
        )
        assert snapshot["broker_pid"] == owner.pid
        run_id = _submit(
            client,
            _blocking_command(entered, release, "recovered full"),
            repository,
            kind="full",
            label="recoverable full",
        )
        wait_for(entered.exists, "the recoverable full gate never started")
        live = client.status(run_id)
        worker_pid = live["worker_pid"]
        assert isinstance(worker_pid, int)

        crash.touch()
        assert owner.wait(timeout=5) != 0

        replacement = RunningCoordinator(state_dir, capacities={"jobs": 1})
        recovered_client = replacement.start()
        recovered = recovered_client.status(run_id)
        assert recovered["status"] == "running"
        assert recovered["worker_pid"] == worker_pid
        assert recovered["cancel_requested"] is False
        assert recovered_client.snapshot()["allocations"] == {"jobs": 1}

        release.touch()
        finished = _row(recovered_client, run_id, "interrupted")
        assert finished["exit_status"] is None
        assert finished["worker_pid"] == worker_pid
        assert "recovered full" in recovered_client.log(run_id)["text"]
    finally:
        release.touch()
        if owner.poll() is None:
            owner.terminate()
            owner.wait(timeout=5)
        if replacement is not None:
            replacement.stop()
        elif worker_pid is not None:
            try:
                os.killpg(worker_pid, signal.SIGKILL)
            except ProcessLookupError:
                pass


def test_merge_submission_consumes_only_an_exact_repository_receipt(
    coordinator,
    tmp_path: Path,
):
    _broker, client = coordinator
    repository = _repository(tmp_path / "repository")
    receipt_id = _submit(
        client,
        _python("print('receipt')"),
        repository,
        kind="full",
    )
    _row(client, receipt_id, "passed")
    entered = tmp_path / "entered"
    release = tmp_path / "release"
    blocker_id = _submit(
        client,
        _blocking_command(entered, release, "lane blocker"),
        repository,
    )
    wait_for(entered.exists, "the lane blocker did not start")

    try:
        merge_id = client.submit_merge(
            "github",
            123,
            checkout=str(repository),
            gate_run_id=receipt_id,
            resources={"jobs": 1},
            agent="pytest",
            caller_pid=os.getpid(),
            environment=caller_environment(),
        )
        merge = client.status(merge_id)
        assert set(merge) == ROW_KEYS
        assert merge["kind"] == "merge"
        assert merge["barrier"] is True
        assert merge["status"] == "queued"
        assert merge["head_sha"] == _head(repository)
        assert merge["gate_run_id"] == receipt_id
        assert merge["publication"] == {"adapter": "github", "request": 123}
        assert merge["failure_reason"] is None
        assert merge["phase"] == "queued"
        assert merge["gate_exit_status"] is None
        assert client.cancel(merge_id)["status"] == "cancelled"

        other = _repository(tmp_path / "other")
        with pytest.raises(CoordinatorError, match="receipt|repository|head"):
            client.submit_merge(
                "github",
                124,
                checkout=str(other),
                gate_run_id=receipt_id,
                caller_pid=os.getpid(),
                environment=caller_environment(),
            )
    finally:
        release.touch()
        _row(client, blocker_id, "passed")


def test_rollback_cutoff_prevents_reusing_any_pre_rollback_gate(
    coordinator,
    tmp_path: Path,
):
    broker, client = coordinator
    repository = _repository(tmp_path / "repository")
    receipt_id = _submit(
        client,
        _python("print('receipt before native migration')"),
        repository,
        kind="full",
    )
    receipt = _row(client, receipt_id, "passed")
    with sqlite3.connect(broker.paths.database) as db:
        db.execute(
            "INSERT INTO coordinator_meta(key, value) VALUES "
            "('invalid_gate_through_sequence', ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (str(receipt["sequence"]),),
        )

    with pytest.raises(CoordinatorError, match="stale|rollback|new full"):
        client.submit_merge(
            "github",
            123,
            checkout=str(repository),
            gate_run_id=receipt_id,
            caller_pid=os.getpid(),
            environment=caller_environment(),
        )
    with pytest.raises(CoordinatorError, match="full-gate|rollback|new full"):
        client.submit_merge(
            "github",
            124,
            checkout=str(repository),
            caller_pid=os.getpid(),
            environment=caller_environment(),
        )

    fresh_id = _submit(
        client,
        _python("print('receipt after rollback')"),
        repository,
        kind="full",
    )
    fresh = _row(client, fresh_id, "passed")
    assert fresh["sequence"] > receipt["sequence"]
    entered = tmp_path / "entered"
    release = tmp_path / "release"
    blocker_id = _submit(
        client,
        _blocking_command(entered, release, "post-rollback lane blocker"),
        repository,
    )
    wait_for(entered.exists, "the post-rollback lane blocker did not start")
    try:
        merge_id = client.submit_merge(
            "github",
            125,
            checkout=str(repository),
            gate_run_id=fresh_id,
            caller_pid=os.getpid(),
            environment=caller_environment(),
        )
        assert client.status(merge_id)["status"] == "queued"
        assert client.cancel(merge_id)["status"] == "cancelled"
    finally:
        release.touch()
        _row(client, blocker_id, "passed")


def test_nested_coordinated_submission_is_rejected_without_a_row(
    coordinator,
    tmp_path: Path,
):
    _broker, client = coordinator
    repository = _repository(tmp_path / "repository")
    environment = caller_environment()
    environment["AGCOORD_RUN_ID"] = "check-parent"

    with pytest.raises(CoordinatorError, match="nested|AGCOORD_RUN_ID|coordinated"):
        client.submit(
            _python("raise SystemExit('must not run')"),
            checkout=str(repository),
            environment=environment,
        )
    snapshot = client.snapshot()
    assert snapshot["active"] == []
    assert snapshot["queued"] == []
    assert snapshot["recent"] == []


def _unreachable_native_state(state_dir: Path) -> None:
    """Configure a spool whose only possible autostart is a distinct, observable refusal."""
    state_dir.mkdir(mode=0o700)
    config_path(state_dir).write_text(
        json.dumps(
            {
                "capacities": {"jobs": 1},
                "native_broker": {
                    "path": str(state_dir / "missing-broker"),
                    "allow_development": True,
                },
            }
        ),
        encoding="utf-8",
    )
    config_path(state_dir).chmod(0o600)


def _submit_through(client: CoordinatorClient, entry: str, repository: Path, environment):
    command = _python("raise SystemExit('must not run')")
    if entry == "submit":
        return client.submit(
            command,
            checkout=str(repository),
            kind="full",
            environment=environment,
        )
    if entry == "submit_merge":
        return client.submit_merge(
            "github",
            123,
            checkout=str(repository),
            environment=environment,
        )
    return client.submit_land(
        "github",
        123,
        command,
        checkout=str(repository),
        environment=environment,
    )


@pytest.mark.parametrize("entry", ["submit", "submit_merge", "submit_land"])
def test_nested_submission_is_refused_before_any_broker_can_start(
    tmp_path: Path,
    entry: str,
):
    state_dir = tmp_path / "state"
    _unreachable_native_state(state_dir)
    repository = _repository(tmp_path / "repository")
    environment = caller_environment()
    environment["AGCOORD_RUN_ID"] = "check-parent"
    client = CoordinatorClient(state_dir=state_dir)

    with pytest.raises(CoordinatorError, match="cannot submit another coordinated job"):
        _submit_through(client, entry, repository, environment)

    assert not (state_dir / "broker.lock").exists()
    assert not (state_dir / "queue.sqlite3").exists()
    assert not (state_dir / "missing-broker").exists()


@pytest.mark.parametrize("entry", ["submit", "submit_merge", "submit_land"])
def test_dirty_checkout_refusal_precedes_nesting_and_any_broker_start(
    tmp_path: Path,
    entry: str,
):
    state_dir = tmp_path / "state"
    _unreachable_native_state(state_dir)
    repository = _repository(tmp_path / "repository")
    (repository / "untracked.txt").write_text("dirty\n", encoding="utf-8")
    environment = caller_environment()
    environment["AGCOORD_RUN_ID"] = "check-parent"
    client = CoordinatorClient(state_dir=state_dir)

    with pytest.raises(CoordinatorError, match="checkout is dirty"):
        _submit_through(client, entry, repository, environment)

    assert not (state_dir / "broker.lock").exists()
    assert not (state_dir / "queue.sqlite3").exists()
@pytest.mark.parametrize("kind", ["check", "full"])
def test_worker_receives_exact_immutable_admission_context(
    tmp_path: Path,
    kind: str,
):
    running = RunningCoordinator(tmp_path / "explicit-state", capacities={"jobs": 1})
    client = running.start()
    repository = _repository(tmp_path / "repository")
    report = tmp_path / f"{kind}-admission.json"
    environment = caller_environment()
    environment.update(
        {
            "AGCOORD_RUN_KIND": "caller-spoofed-kind",
            "AGCOORD_STATE_DIR": str(tmp_path / "caller-spoofed-state"),
        }
    )

    try:
        run_id = client.submit(
            _python(
                """
import json
import os
from pathlib import Path
import sys

Path(sys.argv[1]).write_text(json.dumps({
    name: os.environ[name]
    for name in ("AGCOORD_RUN_ID", "AGCOORD_RUN_KIND", "AGCOORD_STATE_DIR")
}), encoding="utf-8")
""",
                report,
            ),
            checkout=str(repository),
            kind=kind,
            resources={"jobs": 1},
            caller_pid=os.getpid(),
            environment=environment,
        )

        assert _row(client, run_id, "passed")["exit_status"] == 0
        assert json.loads(report.read_text(encoding="utf-8")) == {
            "AGCOORD_RUN_ID": run_id,
            "AGCOORD_RUN_KIND": kind,
            "AGCOORD_STATE_DIR": str(running.broker.paths.state_dir),
        }
    finally:
        running.stop()


def test_land_gate_child_uses_admission_context_to_verify_its_parent(
    tmp_path: Path,
):
    running = RunningCoordinator(tmp_path / "explicit-state", capacities={"jobs": 1})
    client = running.start()
    checkout, _remote, branch, head_sha = _publication_repository(
        tmp_path / "repository"
    )
    bin_dir = _install_land_gh(tmp_path)
    events = tmp_path / "events"
    report = tmp_path / "land-admission.json"
    wrong_state = tmp_path / "wrong-state"
    environment = _land_environment(
        bin_dir,
        branch=branch,
        head_sha=head_sha,
        tag="admission-context",
        event_log=events,
    )
    environment.update(
        {
            "AGCOORD_RUN_KIND": "caller-spoofed-kind",
            "AGCOORD_STATE_DIR": str(tmp_path / "caller-spoofed-state"),
        }
    )

    try:
        run_id = client.submit_land(
            "github",
            106,
            _python(
                """
import json
import os
from pathlib import Path
import sys

from agcoord.queue import CoordinatorClient, CoordinatorError

report = Path(sys.argv[1])
head_sha = sys.argv[2]
wrong_state = sys.argv[3]
run_id = os.environ["AGCOORD_RUN_ID"]
kind = os.environ["AGCOORD_RUN_KIND"]
state_dir = os.environ["AGCOORD_STATE_DIR"]
checkout = str(Path.cwd())
parent_pid = os.getppid()

def accepted(*, selected_state, selected_kind, selected_head, selected_pid):
    try:
        CoordinatorClient(
            state_dir=selected_state,
            checkout=checkout,
            autostart=False,
        ).verify_admission(
            run_id,
            kind=selected_kind,
            checkout=checkout,
            head_sha=selected_head,
            worker_pid=selected_pid,
        )
    except CoordinatorError:
        return False
    return True

report.write_text(json.dumps({
    "context": {
        "AGCOORD_RUN_ID": run_id,
        "AGCOORD_RUN_KIND": kind,
        "AGCOORD_STATE_DIR": state_dir,
    },
    "valid": accepted(
        selected_state=state_dir,
        selected_kind=kind,
        selected_head=head_sha,
        selected_pid=parent_pid,
    ),
    "wrong_state": accepted(
        selected_state=wrong_state,
        selected_kind=kind,
        selected_head=head_sha,
        selected_pid=parent_pid,
    ),
    "wrong_kind": accepted(
        selected_state=state_dir,
        selected_kind="full",
        selected_head=head_sha,
        selected_pid=parent_pid,
    ),
    "wrong_pid": accepted(
        selected_state=state_dir,
        selected_kind=kind,
        selected_head=head_sha,
        selected_pid=os.getpid(),
    ),
    "wrong_head": accepted(
        selected_state=state_dir,
        selected_kind=kind,
        selected_head="0" * 40,
        selected_pid=parent_pid,
    ),
}), encoding="utf-8")
""",
                report,
                head_sha,
                wrong_state,
            ),
            checkout=str(checkout),
            resources={"jobs": 1},
            caller_pid=os.getpid(),
            environment=environment,
        )

        assert _row(client, run_id, "passed")["exit_status"] == 0
        observed = json.loads(report.read_text(encoding="utf-8"))
        assert observed["context"] == {
            "AGCOORD_RUN_ID": run_id,
            "AGCOORD_RUN_KIND": "land",
            "AGCOORD_STATE_DIR": str(running.broker.paths.state_dir),
        }
        assert observed["valid"] is True
        assert observed["wrong_state"] is False
        assert observed["wrong_kind"] is False
        assert observed["wrong_pid"] is False
        assert observed["wrong_head"] is False
    finally:
        running.stop()


def test_first_client_refuses_a_missing_native_broker_without_python_fallback(
    tmp_path: Path,
):
    state_dir = tmp_path / "detached-state"
    repository = _repository(tmp_path / "repository")
    missing = tmp_path / "not-installed" / "agcoord-broker"
    write_broker_config(
        state_dir,
        capacities={"jobs": 1},
        native_broker={"path": str(missing)},
    )
    client = CoordinatorClient(state_dir=state_dir, autostart=True)

    with pytest.raises(
        CoordinatorError,
        match="native broker executable does not exist|install the host package",
    ):
        client.submit(
            [sys.executable, "-c", "raise AssertionError('Python fallback ran')"],
            checkout=str(repository),
            environment=caller_environment(),
        )

    assert not (state_dir / "broker.lock").exists()
    assert not (state_dir / "queue.sqlite3").exists()


def test_drain_rejects_legacy_inserts_and_preserves_existing_work_until_resume(
    tmp_path: Path,
):
    state_dir = tmp_path / "state"
    running = RunningCoordinator(
        state_dir,
        capacities={"jobs": 1},
        idle_timeout=None,
    )
    client = running.start()
    repository = _repository(tmp_path / "repository")
    entered = tmp_path / "entered"
    release = tmp_path / "release"

    try:
        run_id = _submit(
            client,
            _blocking_command(entered, release, "drain survivor"),
            repository,
        )
        wait_for(entered.exists, "the pre-drain job did not start")
        cancelled_id = _submit(
            client,
            _python("raise AssertionError('cancelled work ran')"),
            repository,
        )
        assert client.status(cancelled_id)["status"] == "queued"

        receipt = client.drain(reason="native host upgrade", wait=False)
        assert receipt == {
            "state": "draining",
            "drain_id": receipt["drain_id"],
            "reason": "native host upgrade",
            "started_at": receipt["started_at"],
            "protocol": PROTOCOL,
            "live": 2,
            "broker_pid": os.getpid(),
        }
        assert receipt["drain_id"].startswith("drain-")

        with pytest.raises(CoordinatorError, match="draining|drain") as refused:
            _submit(client, _python("print('must not run')"), repository)
        assert refused.value.code == "broker-draining"

        with sqlite3.connect(state_dir / "queue.sqlite3") as legacy:
            with pytest.raises(
                sqlite3.IntegrityError,
                match="agcoord-maintenance-draining",
            ):
                legacy.execute(
                    "INSERT INTO runs(run_id) VALUES ('check-legacy-race')"
                )

        cancelled = client.cancel(cancelled_id)
        assert cancelled["status"] == "cancelled"
        assert cancelled["exit_status"] == 130
        release.touch()
        assert _row(client, run_id, "passed")["exit_status"] == 0
        wait_for(
            lambda: not running.thread.is_alive(),
            "the drained broker did not yield ownership",
        )

        drained = client.drain_status()
        assert drained["state"] == "drained"
        assert drained["drain_id"] == receipt["drain_id"]
        assert drained["live"] == 0
        assert drained["broker_pid"] is None
        with pytest.raises(
            CoordinatorError,
            match="draining|drained|drain",
        ) as refused:
            _submit(client, _python("print('still must not run')"), repository)
        assert refused.value.code == "broker-draining"

        with pytest.raises(CoordinatorError, match="drain ID|identifier|mismatch"):
            client.resume("drain-ffffffffffff")
        assert client.resume(receipt["drain_id"]) == {
            "state": "open",
            "drain_id": receipt["drain_id"],
            "resumed": True,
        }

        replacement = RunningCoordinator(
            state_dir,
            capacities={"jobs": 1},
            idle_timeout=None,
        )
        replacement_client = replacement.start()
        try:
            resumed_id = _submit(
                replacement_client,
                _python("print('resumed')"),
                repository,
            )
            assert _row(replacement_client, resumed_id, "passed")["exit_status"] == 0
            assert replacement_client.snapshot()["maintenance"] is None
        finally:
            replacement.stop()
    finally:
        release.touch()
        running.stop()


def test_drain_safely_yields_an_idle_legacy_owner_without_an_idle_timeout(
    tmp_path: Path,
):
    state_dir = tmp_path / "state"
    initialized = CoordinatorBroker(state_dir, idle_timeout=None)
    initialized.close()
    ready = tmp_path / "legacy-owner-ready"
    script = """
import fcntl
import json
import os
from pathlib import Path
import sys
import time

lock_path = Path(sys.argv[1])
ready = Path(sys.argv[2])
descriptor = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
fcntl.flock(descriptor, fcntl.LOCK_EX)
metadata = (
    f"pid={os.getpid()}\\n"
    "protocol=4\\n"
    f"capacities={json.dumps({'jobs': 1}, separators=(',', ':'))}\\n"
    "resource_bindings={}\\n"
    "resource_capabilities={}\\n"
    "started_at=2026-09-02T00:00:00+00:00\\n"
)
os.ftruncate(descriptor, 0)
os.write(descriptor, metadata.encode())
os.fsync(descriptor)
ready.touch()
while True:
    time.sleep(1)
"""
    legacy = subprocess.Popen(
        [sys.executable, "-c", script, str(state_dir / "broker.lock"), str(ready)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        wait_for(ready.exists, "the legacy owner did not acquire its lock")
        client = CoordinatorClient(state_dir=state_dir, autostart=False)
        receipt = client.drain(reason="legacy owner handoff", poll_interval=0.02)
        assert receipt["state"] == "drained"
        assert receipt["live"] == 0
        assert receipt["broker_pid"] is None
        assert legacy.wait(timeout=5) == -signal.SIGTERM
        assert client.resume(receipt["drain_id"])["state"] == "open"
    finally:
        if legacy.poll() is None:
            legacy.terminate()
            legacy.wait(timeout=5)


@pytest.mark.parametrize(
    "started_at",
    [
        "2026-13-02T03:30:00+00:00",
        "2026-09-02T03:30:00+01:00",
    ],
)
def test_drain_status_fails_closed_for_an_invalid_durable_start_time(
    tmp_path: Path,
    started_at: str,
):
    state_dir = tmp_path / "state"
    CoordinatorBroker(state_dir, idle_timeout=None).close()
    client = CoordinatorClient(state_dir=state_dir, autostart=False)
    receipt = client.drain(reason="marker validation")

    with sqlite3.connect(state_dir / "queue.sqlite3") as database:
        database.execute(
            "UPDATE coordinator_meta SET value = ? "
            "WHERE key = 'maintenance_started_at'",
            (started_at,),
        )

    with pytest.raises(CoordinatorError, match="start time is invalid"):
        client.drain_status()
    assert receipt["state"] == "drained"


def test_drain_preserves_an_authoritative_land_publication(tmp_path: Path):
    state_dir = tmp_path / "state"
    running = RunningCoordinator(
        state_dir,
        capacities={"jobs": 1},
        idle_timeout=None,
    )
    client = running.start()
    checkout, _remote, branch, head = _publication_repository(
        tmp_path / "repository"
    )
    bin_dir = _install_land_gh(tmp_path)
    events = tmp_path / "events"
    publish_entered = tmp_path / "publish-entered"
    publish_release = tmp_path / "publish-release"
    environment = _land_environment(
        bin_dir,
        branch=branch,
        head_sha=head,
        tag="drain-survivor",
        event_log=events,
        publish_entered=publish_entered,
        publish_release=publish_release,
    )

    try:
        land_id = client.submit_land(
            "github",
            123,
            _land_gate_command(events, "drain-survivor"),
            checkout=str(checkout),
            resources={"jobs": 1},
            caller_pid=os.getpid(),
            environment=environment,
        )
        wait_for(publish_entered.exists, "land never entered authoritative publication")
        assert client.status(land_id)["phase"] == "publishing"

        receipt = client.drain(reason="publish-safe maintenance", wait=False)
        assert receipt["state"] == "draining"
        assert receipt["live"] == 1
        with pytest.raises(CoordinatorError, match="authoritative|cannot be cancelled"):
            client.cancel(land_id)
        with pytest.raises(CoordinatorError, match="draining|drain"):
            _submit(client, _python("print('must not run')"), checkout)

        publish_release.touch()
        row = _row(client, land_id, "passed")
        assert row["phase"] == "complete"
        assert row["gate_exit_status"] == 0
        wait_for(
            lambda: not running.thread.is_alive(),
            "broker did not yield after the land publication completed",
        )
        drained = client.drain_status()
        assert drained["state"] == "drained"
        assert drained["drain_id"] == receipt["drain_id"]
        assert events.read_text(encoding="utf-8") == (
            "gate:drain-survivor\npublish:drain-survivor\n"
        )
    finally:
        publish_release.touch()
        running.stop()


def test_drain_and_concurrent_submissions_have_one_atomic_order(tmp_path: Path):
    state_dir = tmp_path / "state"
    running = RunningCoordinator(
        state_dir,
        capacities={"jobs": 4},
        idle_timeout=None,
    )
    client = running.start()
    repository = _repository(tmp_path / "repository")
    contestants = 16
    start = threading.Barrier(contestants + 1)
    accepted: list[str] = []
    refused: list[str] = []
    unexpected: list[BaseException] = []
    lock = threading.Lock()

    def submit(index: int) -> None:
        contender = CoordinatorClient(state_dir=state_dir, autostart=False)
        start.wait()
        try:
            run_id = _submit(
                contender,
                _python(f"print('race {index}')"),
                repository,
            )
        except CoordinatorError as exc:
            with lock:
                refused.append(str(exc))
        except BaseException as exc:  # pragma: no cover - asserted below
            with lock:
                unexpected.append(exc)
        else:
            with lock:
                accepted.append(run_id)

    threads = [threading.Thread(target=submit, args=(index,)) for index in range(contestants)]
    receipt: dict[str, object] | None = None
    try:
        for thread in threads:
            thread.start()
        start.wait()
        receipt = client.drain(reason="linearization test", wait=False)
        for thread in threads:
            thread.join(timeout=10)

        assert all(not thread.is_alive() for thread in threads)
        assert unexpected == []
        assert len(accepted) + len(refused) == contestants
        assert all("drain" in error for error in refused), refused
        for run_id in accepted:
            assert _row(client, run_id, "passed")["exit_status"] == 0
        wait_for(
            lambda: not running.thread.is_alive(),
            "broker did not finish the pre-drain side of the submission race",
        )
        drained = client.drain_status()
        assert drained["state"] == "drained"
        assert drained["drain_id"] == receipt["drain_id"]
        rows = client.snapshot()["recent"]
        assert {row["run_id"] for row in rows} == set(accepted)
    finally:
        for thread in threads:
            if thread.is_alive():
                thread.join(timeout=2)
        if receipt is not None and not running.thread.is_alive():
            try:
                client.resume(receipt["drain_id"])
            except CoordinatorError:
                pass
        running.stop()


@pytest.mark.parametrize("kind", ["check", "full"])
def test_workers_without_scratch_do_not_receive_or_inherit_temp_paths(
    tmp_path: Path,
    kind: str,
):
    running = RunningCoordinator(tmp_path / "state", capacities={"jobs": 1})
    client = running.start()
    repository = _repository(tmp_path / "repository")
    report = tmp_path / "temp-report.json"
    release = tmp_path / "release"
    environment = caller_environment()
    environment.update({"TMPDIR": "/caller/tmp", "TMP": "/caller/tmp", "TEMP": "/caller/tmp"})

    try:
        run_id = client.submit(
            _python(
                """
import json
import os
from pathlib import Path
import sys
import time

report, release = map(Path, sys.argv[1:])
report.write_text(json.dumps({
    "variables": {name: os.environ.get(name) for name in ("TMPDIR", "TMP", "TEMP")},
}), encoding="utf-8")
while not release.exists():
    time.sleep(0.01)
""",
                report,
                release,
            ),
            checkout=str(repository),
            kind=kind,
            resources={"jobs": 1},
            caller_pid=os.getpid(),
            environment=environment,
        )
        wait_for(report.exists, "the worker did not report its temporary root")
        observed = json.loads(report.read_text(encoding="utf-8"))
        assert observed["variables"] == {"TMPDIR": None, "TMP": None, "TEMP": None}
        assert not (running.broker.paths.worker_tmp / run_id).exists()

        release.touch()
        _row(client, run_id, "passed")
    finally:
        release.touch()
        running.stop()


def test_land_without_scratch_does_not_receive_or_inherit_temp_paths(tmp_path: Path):
    running = RunningCoordinator(tmp_path / "state", capacities={"jobs": 1})
    client = running.start()
    checkout, _remote, branch, head_sha = _publication_repository(
        tmp_path / "repository"
    )
    bin_dir = _install_land_gh(tmp_path)
    report = tmp_path / "land-temp-report.json"
    events = tmp_path / "events"
    environment = _land_environment(
        bin_dir,
        branch=branch,
        head_sha=head_sha,
        tag="no-scratch",
        event_log=events,
    )
    environment.update(
        {"TMPDIR": "/caller/tmp", "TMP": "/caller/tmp", "TEMP": "/caller/tmp"}
    )

    try:
        run_id = client.submit_land(
            "github",
            124,
            _python(
                """
import json
import os
from pathlib import Path
import sys

Path(sys.argv[1]).write_text(json.dumps({
    name: os.environ.get(name) for name in ("TMPDIR", "TMP", "TEMP")
}), encoding="utf-8")
""",
                report,
            ),
            checkout=str(checkout),
            resources={"jobs": 1},
            caller_pid=os.getpid(),
            environment=environment,
        )
        _row(client, run_id, "passed")
        assert json.loads(report.read_text(encoding="utf-8")) == {
            "TMPDIR": None,
            "TMP": None,
            "TEMP": None,
        }
        assert not (running.broker.paths.worker_tmp / run_id).exists()
    finally:
        running.stop()


def test_clear_refuses_live_work_then_removes_only_terminal_history_and_logs(
    tmp_path: Path,
):
    state_dir = tmp_path / "state"
    running = RunningCoordinator(state_dir, capacities={"jobs": 1})
    client = running.start()
    repository = _repository(tmp_path / "repository")
    entered = tmp_path / "entered"
    release = tmp_path / "release"

    try:
        run_id = _submit(
            client,
            _blocking_command(entered, release, "clear blocker"),
            repository,
        )
        wait_for(entered.exists, "the clear blocker did not start")
        with pytest.raises(CoordinatorError, match="queued|running|active"):
            client.clear()
        assert client.status(run_id)["status"] == "running"

        release.touch()
        _row(client, run_id, "passed")
        assert list((state_dir / "logs").glob("*.log"))
        client.clear()

        snapshot = client.snapshot()
        assert snapshot["active"] == []
        assert snapshot["queued"] == []
        assert snapshot["recent"] == []
        assert not list((state_dir / "logs").glob("*.log"))
        assert (state_dir / "queue.sqlite3").exists()
        assert (state_dir / "broker.lock").exists()
        with pytest.raises(CoordinatorError, match="unknown|not found"):
            client.status(run_id)
    finally:
        release.touch()
        running.stop()
