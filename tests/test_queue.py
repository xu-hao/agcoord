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

import pytest

from agcoord.queue import (
    CoordinatorBroker,
    CoordinatorClient,
    CoordinatorError,
    PROTOCOL,
    migrate_queue,
    state_dir_for,
)

from conftest import RunningCoordinator, caller_environment, wait_for


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
    print(json.dumps({{
        "number": int(arguments[2]),
        "state": "OPEN",
        "isDraft": False,
        "baseRefName": os.environ.get("AGCOORD_TEST_BASE", "main"),
        "headRefName": os.environ["AGCOORD_TEST_BRANCH"],
        "headRefOid": os.environ["AGCOORD_TEST_HEAD"],
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
    print(json.dumps({{
        "merge_base_commit": {{"sha": os.environ["AGCOORD_TEST_HEAD"]}},
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
    return environment


def _land_gate_command(
    event_log: Path,
    tag: str,
    *,
    entered: Path | None = None,
    release: Path | None = None,
    temp_report: Path | None = None,
) -> list[str]:
    return _python(
        """
from pathlib import Path
import os
import sys
import tempfile
import time

event_log, tag, entered, release, temp_report = sys.argv[1:]
with Path(event_log).open("a", encoding="utf-8") as stream:
    stream.write(f"gate:{tag}\\n")
print(f"gate transcript: {tag}", flush=True)
if temp_report:
    root = Path(tempfile.gettempdir())
    Path(temp_report).write_text(str(root), encoding="utf-8")
    protected = root / "protected" / "nested"
    protected.mkdir(parents=True)
    (protected / "payload").write_text("scratch", encoding="utf-8")
    os.chmod(protected, 0)
    os.chmod(protected.parent, 0)
if entered:
    Path(entered).touch()
while release and not Path(release).exists():
    time.sleep(0.01)
""",
        event_log,
        tag,
        entered or "",
        release or "",
        temp_report or "",
    )


def test_default_state_is_user_scoped_and_overrideable(monkeypatch, tmp_path: Path):
    xdg_state = tmp_path / "xdg"
    explicit = tmp_path / "explicit"
    monkeypatch.setenv("XDG_STATE_HOME", str(xdg_state))
    monkeypatch.delenv("AGCOORD_STATE_DIR", raising=False)
    assert state_dir_for() == (xdg_state / "agcoord").resolve()

    monkeypatch.setenv("AGCOORD_STATE_DIR", str(explicit))
    assert state_dir_for() == explicit.resolve()


@pytest.mark.parametrize(
    ("configured", "expected"),
    [
        ('{"jobs": 3, "cpu": 8, "browser": 1}', {"jobs": 3, "cpu": 8, "browser": 1}),
        ("jobs=3,cpu=8,browser=1", {"jobs": 3, "cpu": 8, "browser": 1}),
    ],
)
def test_capacity_environment_accepts_json_or_name_unit_pairs(
    monkeypatch,
    tmp_path: Path,
    configured: str,
    expected: dict[str, int],
):
    monkeypatch.setenv("AGCOORD_CAPACITIES", configured)
    running = RunningCoordinator(tmp_path / "state", capacities=None)
    client = running.start()
    try:
        assert client.snapshot()["capacities"] == expected
    finally:
        running.stop()


def test_absent_capacity_configuration_defaults_to_two_job_slots(monkeypatch, tmp_path: Path):
    monkeypatch.delenv("AGCOORD_CAPACITIES", raising=False)
    running = RunningCoordinator(tmp_path / "state", capacities=None)
    client = running.start()
    try:
        assert client.snapshot()["capacities"] == {"jobs": 2}
    finally:
        running.stop()


def test_protocol_one_history_requires_explicit_migration_for_land_fields(
    tmp_path: Path,
):
    state_dir = tmp_path / "legacy-state"
    state_dir.mkdir(mode=0o700)
    database = state_dir / "queue.sqlite3"
    with sqlite3.connect(database) as db:
        db.executescript(
            """
            CREATE TABLE coordinator_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            INSERT INTO coordinator_meta(key, value) VALUES ('protocol', '1');
            CREATE TABLE runs (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL UNIQUE,
                status TEXT NOT NULL,
                kind TEXT NOT NULL,
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
                finished_at, exit_status
            ) VALUES (
                'full-legacy', 'passed', 'full', 'legacy full', 'legacy-agent',
                'repo-legacy', '/repos/legacy.git', 'worktree-legacy',
                '/worktrees/legacy', 'main',
                'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 1, '{"jobs":1}',
                42, '["true"]', '{}', '2026-08-30T12:00:00+00:00',
                '2026-08-30T12:00:01+00:00', '2026-08-30T12:00:02+00:00', 0
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
        "from_protocol": 1,
        "to_protocol": PROTOCOL,
    }
    running = RunningCoordinator(state_dir, capacities={"jobs": 1})
    client = running.start()
    try:
        legacy = client.status("full-legacy")
        assert set(legacy) == ROW_KEYS
        assert legacy["kind"] == "full"
        assert legacy["phase"] == "complete"
        assert legacy["gate_exit_status"] is None
        assert legacy["publication"] is None
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


def test_land_holds_lane_resources_and_scratch_through_publication_fifo(
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
    scratch_report = tmp_path / "scratch-root"

    try:
        first_id = client.submit_land(
            "github",
            101,
            _land_gate_command(
                events,
                "first",
                temp_report=scratch_report,
            ),
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
        scratch_root = Path(scratch_report.read_text(encoding="utf-8"))
        assert first["status"] == "running"
        assert first["phase"] == "publishing"
        assert first["gate_exit_status"] == 0
        assert second["status"] == "queued"
        assert any(first_id in blocker for blocker in second["blocked_by"])
        assert client.snapshot()["allocations"] == {"jobs": 1, "cpu": 1}
        assert scratch_root.exists()
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
        assert not scratch_root.exists()
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
    scratch_report = tmp_path / "scratch-root"
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
                temp_report=scratch_report,
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
        scratch_root = Path(scratch_report.read_text(encoding="utf-8"))
        assert scratch_root.exists()

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
        assert not scratch_root.exists()
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


def test_first_client_starts_a_detached_broker_and_later_shell_recovers_the_job(
    tmp_path: Path,
):
    state_dir = tmp_path / "detached-state"
    repository = _repository(tmp_path / "repository")
    entered = tmp_path / "entered"
    release = tmp_path / "release"
    submitter = _python(
        """
import os
import sys

from agcoord.queue import CoordinatorClient

state_dir, checkout, entered, release = sys.argv[1:]
environment = dict(os.environ)
environment.pop("AGCOORD_RUN_ID", None)
client = CoordinatorClient(state_dir=state_dir, autostart=True)
worker_source = (
    "from pathlib import Path\\n"
    "import sys\\n"
    "import time\\n"
    "entered, release = map(Path, sys.argv[1:])\\n"
    "entered.touch()\\n"
    "while not release.exists():\\n"
    "    time.sleep(0.01)\\n"
)
run_id = client.submit(
    [sys.executable, "-u", "-c", worker_source, entered, release],
    checkout=checkout,
    label="detached worker",
    caller_pid=os.getpid(),
    environment=environment,
)
print(os.getpid(), run_id, flush=True)
""",
        state_dir,
        repository,
        entered,
        release,
    )
    client = CoordinatorClient(state_dir=state_dir, autostart=False)
    broker_pid: int | None = None
    try:
        completed = subprocess.run(submitter, check=False, capture_output=True, text=True)
        assert completed.returncode == 0, completed.stderr
        caller_pid_text, run_id = completed.stdout.strip().splitlines()[-1].split()
        caller_pid = int(caller_pid_text)
        wait_for(entered.exists, "the detached worker did not survive its caller")
        snapshot = client.snapshot()
        broker_pid = snapshot["broker_pid"]
        assert isinstance(broker_pid, int)
        assert broker_pid != caller_pid
        assert client.status(run_id)["status"] == "running"

        release.touch()
        assert _row(client, run_id, "passed")["exit_status"] == 0
    finally:
        release.touch()
        if broker_pid is None:
            try:
                observed_pid = client.snapshot()["broker_pid"]
                broker_pid = observed_pid if isinstance(observed_pid, int) else None
            except (CoordinatorError, FileNotFoundError, KeyError):
                broker_pid = None
        if broker_pid is not None:
            try:
                os.kill(broker_pid, signal.SIGTERM)
            except ProcessLookupError:
                broker_pid = None

            if broker_pid is not None:
                def stopped() -> bool:
                    try:
                        os.kill(broker_pid, 0)
                    except ProcessLookupError:
                        return True
                    return False

                wait_for(stopped, "the test-owned detached broker did not stop")


@pytest.mark.parametrize("kind", ["check", "full"])
def test_every_worker_uses_private_system_temp_until_terminal_reaping(
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
import stat
import sys
import tempfile
import time

report, release = map(Path, sys.argv[1:])
root = Path(tempfile.gettempdir())
report.write_text(json.dumps({
    "root": str(root),
    "mode": stat.S_IMODE(root.stat().st_mode),
    "variables": {name: os.environ[name] for name in ("TMPDIR", "TMP", "TEMP")},
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
        run_root = Path(observed["root"]).resolve()
        system_temp = Path("/tmp").resolve()
        assert run_root.is_relative_to(system_temp)
        assert running.broker.paths.state_dir.resolve() not in (run_root, *run_root.parents)
        assert run_root.name == run_id
        assert observed["mode"] == 0o700
        assert set(observed["variables"].values()) == {str(run_root)}
        assert run_root.exists()

        release.touch()
        _row(client, run_id, "passed")
        assert not run_root.exists()
    finally:
        release.touch()
        running.stop()


def test_terminal_cleanup_reclaims_worker_created_mode_zero_trees(
    coordinator,
    tmp_path: Path,
):
    _broker, client = coordinator
    repository = _repository(tmp_path / "repository")
    report = tmp_path / "temp-root"
    run_id = _submit(
        client,
        _python(
            """
from pathlib import Path
import os
import sys
import tempfile

report = Path(sys.argv[1])
root = Path(tempfile.gettempdir())
protected = root / "protected" / "nested"
protected.mkdir(parents=True)
(protected / "payload").write_bytes(b"x" * 1024)
report.write_text(str(root), encoding="utf-8")
os.chmod(protected, 0)
os.chmod(protected.parent, 0)
""",
            report,
        ),
        repository,
    )
    passed = _row(client, run_id, "passed")
    run_root = Path(report.read_text(encoding="utf-8"))

    assert passed["status"] == "passed"
    assert not run_root.exists()


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
