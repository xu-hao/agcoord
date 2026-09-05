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
    LAND_AVOID_ENV,
    CoordinatorClient,
    CoordinatorError,
    NATIVE_PROTOCOL,
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
    assert snapshot["protocol"] == NATIVE_PROTOCOL
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


def test_full_is_not_a_lane_barrier_and_lane_work_packs_by_resources(tmp_path: Path):
    running = RunningCoordinator(
        tmp_path / "state",
        capacities={"jobs": 4, "cpu": 3},
    )
    client = running.start()
    first = _repository(tmp_path / "first")
    second = _repository(tmp_path / "second")
    entered_first = tmp_path / "entered-first"
    release_first = tmp_path / "release-first"
    entered_full = tmp_path / "entered-full"
    release_full = tmp_path / "release-full"
    entered_large = tmp_path / "entered-large"
    release_large = tmp_path / "release-large"
    entered_small = tmp_path / "entered-small"
    release_small = tmp_path / "release-small"
    entered_other = tmp_path / "entered-other"
    release_other = tmp_path / "release-other"
    releases = (
        release_first, release_full, release_large, release_small, release_other,
    )

    try:
        first_id = _submit(
            client,
            _blocking_command(entered_first, release_first, "first check"),
            first,
            resources={"cpu": 1},
        )
        wait_for(entered_first.exists, "the first lane job did not start")
        full_id = _submit(
            client,
            _blocking_command(entered_full, release_full, "full receipt"),
            first,
            kind="full",
            label="full gate",
            resources={"cpu": 1},
        )
        # A full is ordinary lane work: it overlaps the running check in its own worktree.
        wait_for(entered_full.exists, "the full did not overlap earlier lane work")
        assert client.status(full_id)["status"] == "running"
        assert client.status(full_id)["barrier"] is False

        large_id = _submit(
            client,
            _blocking_command(entered_large, release_large, "large check"),
            first,
            resources={"cpu": 2},
        )
        small_id = _submit(
            client,
            _blocking_command(entered_small, release_small, "small check"),
            first,
            resources={"cpu": 1},
        )

        # The large request waits for capacity while the smaller lane request behind it
        # packs into the free CPU.
        wait_for(entered_small.exists, "later lane work did not pack around a blocked request")
        other_id = _submit(
            client,
            _blocking_command(entered_other, release_other, "other repository"),
            second,
            resources={"cpu": 1},
        )
        # Unrelated repository work waits only for capacity.
        assert client.status(large_id)["status"] == "queued"
        assert client.status(large_id)["blocked_by"] == ["resource:cpu"]
        assert client.status(other_id)["status"] == "queued"
        assert client.status(other_id)["blocked_by"] == ["resource:cpu"]
        assert not entered_large.exists()
        assert client.snapshot()["allocations"] == {"jobs": 3, "cpu": 3}

        release_first.touch()
        _row(client, first_id, "passed")
        wait_for(entered_other.exists, "cross-repository work did not use the freed CPU")
        assert client.status(large_id)["status"] == "queued"
        assert not entered_large.exists()

        release_full.touch()
        release_small.touch()
        _row(client, full_id, "passed")
        _row(client, small_id, "passed")
        wait_for(entered_large.exists, "the large request never fit")
        release_large.touch()
        release_other.touch()
        assert _row(client, large_id, "passed")["repository_id"] == client.status(full_id)[
            "repository_id"
        ]
        _row(client, other_id, "passed")
        assert client.snapshot()["allocations"] == {"jobs": 0, "cpu": 0}
    finally:
        for path in releases:
            path.touch()
        running.stop()


def test_land_barrier_excludes_only_lane_work_in_its_own_worktree(tmp_path: Path):
    running = RunningCoordinator(
        tmp_path / "state",
        capacities={"jobs": 4, "cpu": 4},
    )
    client = running.start()
    checkout, remote, branch, head_sha = _publication_repository(tmp_path / "repository")
    other_worktree = tmp_path / "other-worktree"
    subprocess.run(
        [GIT, "clone", "--quiet", "--branch", branch, str(remote), str(other_worktree)],
        check=True,
        capture_output=True,
        text=True,
    )
    _git(other_worktree, "config", "user.name", "AGCoord test")
    _git(other_worktree, "config", "user.email", "agcoord@example.invalid")
    assert _head(other_worktree) == head_sha
    bin_dir = _install_land_gh(tmp_path)
    events = tmp_path / "events"
    first_gate_entered = tmp_path / "first-gate-entered"
    first_gate_release = tmp_path / "first-gate-release"
    second_gate_entered = tmp_path / "second-gate-entered"
    second_gate_release = tmp_path / "second-gate-release"
    entered_same = tmp_path / "entered-same"
    release_same = tmp_path / "release-same"
    entered_other = tmp_path / "entered-other"
    release_other = tmp_path / "release-other"
    releases = (first_gate_release, second_gate_release, release_same, release_other)

    try:
        first_id = client.submit_land(
            "github",
            101,
            _land_gate_command(
                events, "first", entered=first_gate_entered, release=first_gate_release
            ),
            checkout=str(checkout),
            label="first land",
            resources={"cpu": 1},
            caller_pid=os.getpid(),
            environment=_land_environment(
                bin_dir,
                branch=branch,
                head_sha=head_sha,
                tag="first",
                event_log=events,
            ),
        )
        wait_for(first_gate_entered.exists, "the first land never reached its gate")
        assert client.status(first_id)["barrier"] is True

        same_id = _submit(
            client,
            _blocking_command(entered_same, release_same, "same worktree check"),
            checkout,
            resources={"cpu": 1},
        )
        other_id = _submit(
            client,
            _blocking_command(entered_other, release_other, "other worktree check"),
            other_worktree,
            resources={"cpu": 1},
        )
        full_id = _submit(
            client,
            _python("print('full receipt')"),
            other_worktree,
            kind="full",
            label="other worktree full",
            resources={"cpu": 1},
        )
        second_id = client.submit_land(
            "github",
            102,
            _land_gate_command(
                events, "second", entered=second_gate_entered, release=second_gate_release
            ),
            checkout=str(other_worktree),
            label="second land",
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
        rows = {run_id: client.status(run_id) for run_id in (first_id, same_id, other_id)}
        assert rows[first_id]["repository_id"] == rows[other_id]["repository_id"]
        assert rows[first_id]["worktree_id"] == rows[same_id]["worktree_id"]
        assert rows[first_id]["worktree_id"] != rows[other_id]["worktree_id"]

        # Work in another worktree of the same repository passes the running land, even
        # though a same-worktree check queued ahead of it; that check and the second land
        # stay behind the barrier.
        wait_for(entered_other.exists, "other-worktree work did not overlap the land")
        full = _row(client, full_id, "passed")
        assert full["barrier"] is False
        assert client.status(first_id)["status"] == "running"
        assert not entered_same.exists()
        same = client.status(same_id)
        assert same["status"] == "queued"
        assert same["blocked_by"] == [f"repository:{same['repository_id']}:barrier:{first_id}"]
        # The second land waits for the running land and for the check that shares its
        # own worktree, but not for the same-worktree check queued behind the first land.
        second = client.status(second_id)
        assert second["status"] == "queued"
        assert second["blocked_by"] == [
            f"repository:{second['repository_id']}:active:{first_id}",
            f"repository:{second['repository_id']}:active:{other_id}",
        ]
        assert client.snapshot()["allocations"] == {"jobs": 2, "cpu": 2}

        release_other.touch()
        _row(client, other_id, "passed")
        second = client.status(second_id)
        assert second["status"] == "queued"
        assert second["blocked_by"] == [
            f"repository:{second['repository_id']}:active:{first_id}"
        ]

        # Once the first land publishes, the same-worktree check and the second land (in
        # the other worktree) overlap; two lands never do.
        first_gate_release.touch()
        first = _row(client, first_id, "passed")
        assert first["phase"] == "complete"
        wait_for(entered_same.exists, "the same-worktree check never started after the land")
        wait_for(second_gate_entered.exists, "the second land never overlapped other-worktree work")
        assert client.status(same_id)["status"] == "running"
        assert client.status(second_id)["status"] == "running"
        assert client.snapshot()["allocations"] == {"jobs": 2, "cpu": 2}

        release_same.touch()
        second_gate_release.touch()
        _row(client, same_id, "passed")
        second = _row(client, second_id, "passed")
        assert second["phase"] == "complete"
        assert events.read_text(encoding="utf-8").splitlines() == [
            "gate:first",
            "publish:first",
            "gate:second",
            "publish:second",
        ]
    finally:
        for path in releases:
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
    assert receipt["barrier"] is False
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


def test_land_refuses_a_commit_avoided_for_one_landing_before_any_push(
    coordinator,
    tmp_path: Path,
):
    _running, client = coordinator
    checkout, _remote, branch, head = _publication_repository(tmp_path / "repository")
    bin_dir = _install_land_gh(tmp_path)
    events = tmp_path / "events"
    gate_marker = tmp_path / "gate-ran"
    environment = _land_environment(
        bin_dir,
        branch=branch,
        head_sha=head,
        tag="avoid-request",
        event_log=events,
    )

    land_id = client.submit_land(
        "github",
        125,
        _python(
            "from pathlib import Path; import sys; Path(sys.argv[1]).touch()",
            gate_marker,
        ),
        checkout=str(checkout),
        resources={"jobs": 1},
        caller_pid=os.getpid(),
        environment=environment,
        avoid_commits=[head],
    )
    receipt = _row(client, land_id, "failed")

    assert receipt["exit_status"] == 80
    assert receipt["failure_reason"] == "avoided-commit"
    assert not gate_marker.exists()
    assert not events.exists()
    assert _git(
        checkout,
        "ls-remote",
        "origin",
        f"refs/heads/{branch}",
    ).split()[0] == head


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
    original = RunningCoordinator(state_dir, capacities={"jobs": 1})
    client = original.start()
    replacement: RunningCoordinator | None = None
    worker_pid: int | None = None

    try:
        assert client.snapshot()["broker_pid"] == original.pid
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
        original.kill()
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
        if original.is_running():
            original.kill()
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
    original = RunningCoordinator(state_dir, capacities={"jobs": 1})
    client = original.start()
    replacement: RunningCoordinator | None = None
    worker_pid: int | None = None

    try:
        assert client.snapshot()["broker_pid"] == original.pid
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

        original.kill()

        replacement = RunningCoordinator(state_dir, capacities={"jobs": 1})
        recovered_client = replacement.start()
        recovered = recovered_client.status(run_id)
        assert recovered["status"] == "running"
        assert recovered["worker_pid"] == worker_pid
        assert recovered["cancel_requested"] is False
        assert recovered_client.snapshot()["allocations"] == {"jobs": 1}

        release.touch()
        finished = _row(recovered_client, run_id, "interrupted")
        assert finished["exit_status"] == 125
        assert finished["failure_reason"] == "worker-result-lost"
        assert finished["worker_pid"] == worker_pid
        assert "recovered full" in recovered_client.log(run_id)["text"]
    finally:
        release.touch()
        if original.is_running():
            original.kill()
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


@pytest.mark.parametrize("entry", ["submit", "submit_merge", "submit_land"])
def test_submission_outside_a_git_repository_explains_the_rule_before_any_broker_can_start(
    tmp_path: Path,
    entry: str,
):
    state_dir = tmp_path / "state"
    _unreachable_native_state(state_dir)
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    client = CoordinatorClient(state_dir=state_dir)

    with pytest.raises(CoordinatorError) as refused:
        _submit_through(client, entry, scratch, caller_environment())

    message = str(refused.value)
    assert scratch.name in message
    assert "is not inside a Git repository; agc schedules work per repository and worktree" in message
    assert "run it from a checkout or pass --checkout PATH" in message
    assert "(git: fatal: not a git repository" in message
    assert "Stopping at filesystem boundary" not in message
    assert not (state_dir / "broker.lock").exists()
    assert not (state_dir / "queue.sqlite3").exists()
    assert not (state_dir / "missing-broker").exists()
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
            "AGCOORD_STATE_DIR": str(running.paths.state_dir),
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
            "AGCOORD_STATE_DIR": str(running.paths.state_dir),
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
        assert not (running.paths.worker_tmp / run_id).exists()

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
        assert not (running.paths.worker_tmp / run_id).exists()
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


def test_land_avoid_commits_are_validated_before_any_broker_can_start(tmp_path: Path):
    state_dir = tmp_path / "state"
    _unreachable_native_state(state_dir)
    repository = _repository(tmp_path / "repository")
    client = CoordinatorClient(state_dir=state_dir)

    with pytest.raises(CoordinatorError, match="40 hexadecimal|hexadecimal"):
        client.submit_land(
            "github",
            123,
            _python("raise SystemExit('must not run')"),
            checkout=str(repository),
            avoid_commits=["not-a-sha"],
        )
    with pytest.raises(CoordinatorError, match="sequence"):
        client.submit_land(
            "github",
            123,
            _python("raise SystemExit('must not run')"),
            checkout=str(repository),
            avoid_commits="a" * 40,
        )
    assert not (state_dir / "broker.lock").exists()
    assert not (state_dir / "queue.sqlite3").exists()


def test_land_reserves_the_avoid_environment_name_for_the_coordinator(
    coordinator,
    tmp_path: Path,
):
    _broker, client = coordinator
    repository = _repository(tmp_path / "repository")
    environment = caller_environment()
    environment[LAND_AVOID_ENV] = "a" * 40

    with pytest.raises(CoordinatorError, match=f"reserved {LAND_AVOID_ENV}"):
        client.submit_land(
            "github",
            123,
            _python("raise SystemExit('must not run')"),
            checkout=str(repository),
            environment=environment,
        )
    snapshot = client.snapshot()
    assert snapshot["active"] == [] and snapshot["queued"] == [] and snapshot["recent"] == []


def _protocol_spool(state_dir: Path):
    """Create a protocol-5 spool database the way the broker leaves it, with no owner."""
    from agcoord.queue import queue_paths

    state_dir.mkdir(mode=0o700)
    paths = queue_paths(state_dir=state_dir)
    database = sqlite3.connect(paths.database)
    try:
        database.execute("PRAGMA journal_mode=WAL")
        database.execute(
            "CREATE TABLE coordinator_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
        )
        database.execute("INSERT INTO coordinator_meta VALUES ('protocol', '5')")
        database.commit()
    finally:
        database.close()
    return paths


def _exclusive_holder(database: Path) -> sqlite3.Connection:
    """Hold the database exclusively, which makes every other reader see `database is locked`."""
    holder = sqlite3.connect(database, isolation_level=None, check_same_thread=False)
    holder.execute("PRAGMA locking_mode=EXCLUSIVE")
    holder.execute("BEGIN")
    holder.execute("SELECT count(*) FROM coordinator_meta").fetchone()
    return holder


def test_spool_protocol_waits_through_a_transient_lock_instead_of_aborting(tmp_path: Path):
    from agcoord import queue as queue_module

    paths = _protocol_spool(tmp_path / "state")
    holder = _exclusive_holder(paths.database)
    released = threading.Event()

    def release() -> None:
        time.sleep(0.5)
        holder.execute("COMMIT")
        holder.close()
        released.set()

    thread = threading.Thread(target=release)
    thread.start()
    started = time.monotonic()
    try:
        assert queue_module._spool_protocol(paths, timeout=5.0) == 5
    finally:
        thread.join(timeout=10)
    assert released.is_set()
    assert time.monotonic() - started < 5.0


def test_spool_protocol_reports_a_persistent_lock_only_after_the_timeout(tmp_path: Path):
    from agcoord import queue as queue_module

    paths = _protocol_spool(tmp_path / "state")
    holder = _exclusive_holder(paths.database)
    try:
        started = time.monotonic()
        with pytest.raises(CoordinatorError, match="database is locked"):
            queue_module._spool_protocol(paths, timeout=0.3)
        assert time.monotonic() - started >= 0.3
    finally:
        holder.execute("COMMIT")
        holder.close()
