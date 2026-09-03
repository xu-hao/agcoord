"""Behavioral coverage for child CPU leases inside one admitted worker tree."""

from __future__ import annotations

import json
import os
from pathlib import Path
import signal
import sqlite3
import subprocess
import sys

import pytest

from agcoord.queue import (
    CoordinatorBroker,
    CoordinatorClient,
    CoordinatorError,
    PROTOCOL,
    migrate_queue,
)

from conftest import RunningCoordinator, RunningReferenceBroker, caller_environment, wait_for


def _git(path: Path, *arguments: str) -> str:
    completed = subprocess.run(
        ["git", "-C", str(path), *arguments],
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def _repository(path: Path) -> Path:
    path.mkdir()
    _git(path, "init", "-b", "main")
    _git(path, "config", "user.name", "Lease Tests")
    _git(path, "config", "user.email", "leases@example.invalid")
    (path / "tracked.txt").write_text("tracked\n", encoding="utf-8")
    _git(path, "add", "tracked.txt")
    _git(path, "commit", "-m", "initial")
    return path


def _terminal(client: CoordinatorClient, run_id: str) -> dict[str, object] | None:
    row = client.status(run_id)
    return row if row["status"] not in {"queued", "running"} else None


def test_protocol_three_history_requires_migration_before_child_leases(
    tmp_path: Path,
):
    state_dir = tmp_path / "state"
    repository = _repository(tmp_path / "repository")
    original = RunningReferenceBroker(
        state_dir,
        capacities={"jobs": 1, "cpu": 1},
    )
    original_client = original.start()
    historical_run = original_client.submit(
        [sys.executable, "-c", "print('historical protocol-three run')"],
        checkout=str(repository),
        caller_pid=os.getpid(),
        environment=caller_environment(),
    )
    historical = wait_for(
        lambda: _terminal(original_client, historical_run),
        "the history fixture did not finish",
    )
    assert historical["status"] == "passed"
    original.stop()

    database = state_dir / "queue.sqlite3"
    with sqlite3.connect(database) as db:
        db.execute("DROP TABLE child_cpu_leases")
        db.execute(
            "UPDATE coordinator_meta SET value = '3' WHERE key = 'protocol'"
        )

    with pytest.raises(CoordinatorError, match="migrate|protocol"):
        CoordinatorBroker(
            state_dir,
            capacities={"jobs": 1, "cpu": 1},
            idle_timeout=None,
        )

    assert migrate_queue(state_dir=state_dir) == {
        "changed": True,
        "from_protocol": 3,
        "to_protocol": PROTOCOL,
    }

    migrated = RunningReferenceBroker(
        state_dir,
        capacities={"jobs": 1, "cpu": 1},
    )
    client = migrated.start()
    report = tmp_path / "migrated-lease.json"
    try:
        assert client.status(historical_run)["status"] == "passed"
        leased_run = client.submit(
            [
                sys.executable,
                "-u",
                "-c",
                """
import json
import os
from pathlib import Path

from agcoord import ChildCpuLease, CoordinatorClient

client = CoordinatorClient(
    state_dir=os.environ["AGCOORD_STATE_DIR"],
    autostart=False,
)
with client.acquire_child_cpu_lease(1, timeout=5) as lease:
    Path(os.environ["LEASE_REPORT"]).write_text(
        json.dumps({"granted": lease.granted, "full": lease.full}),
        encoding="utf-8",
    )
""",
            ],
            checkout=str(repository),
            resources={"cpu": 1},
            caller_pid=os.getpid(),
            environment={
                **caller_environment(),
                "LEASE_REPORT": str(report),
            },
        )
        finished = wait_for(
            lambda: _terminal(client, leased_run),
            "the migrated spool could not grant a child CPU lease",
        )
        assert finished["status"] == "passed"
        assert json.loads(report.read_text(encoding="utf-8")) == {
            "granted": 1,
            "full": True,
        }
        assert client.child_cpu_leases(
            leased_run,
            include_terminal=True,
        )[0]["status"] == "released"
    finally:
        migrated.stop()


_CONTROLLER_SOURCE = """
import json
import os
from pathlib import Path
import time

from agcoord.queue import CoordinatorClient

client = CoordinatorClient(
    state_dir=os.environ["AGCOORD_STATE_DIR"],
    autostart=False,
)
lease = client.acquire_child_cpu_lease(
    int(os.environ["LEASE_REQUESTED"]),
    minimum=int(os.environ["LEASE_MINIMUM"]),
    timeout=10,
)
with lease:
    Path(os.environ["LEASE_REPORT"]).write_text(json.dumps({
        "lease_id": lease.lease_id,
        "requested": lease.requested,
        "minimum": lease.minimum,
        "granted": lease.granted,
        "full": lease.full,
    }), encoding="utf-8")
    if os.environ.get("LEASE_CRASH") == "1":
        os._exit(17)
    release = Path(os.environ["LEASE_RELEASE"])
    while not release.exists():
        time.sleep(0.01)
"""


def _wait_for_report(
    client: CoordinatorClient,
    run_id: str,
    report: Path,
    failure: str,
) -> None:
    def observed() -> bool:
        if report.exists():
            return True
        terminal = _terminal(client, run_id)
        if terminal is not None:
            pytest.fail(f"{failure}: {terminal}")
        return False

    wait_for(observed, failure)


def test_admitted_child_holds_and_releases_parent_cpu_tokens(tmp_path: Path):
    running = RunningCoordinator(
        tmp_path / "state",
        capacities={"jobs": 1, "cpu": 2},
    )
    client = running.start()
    repository = _repository(tmp_path / "repository")
    report = tmp_path / "lease.json"
    release = tmp_path / "release"

    try:
        run_id = client.submit(
            [
                sys.executable,
                "-u",
                "-c",
                """
import json
import os
from pathlib import Path
import time

from agcoord import ChildCpuLease, CoordinatorClient

report = Path(os.environ["LEASE_REPORT"])
release = Path(os.environ["LEASE_RELEASE"])
client = CoordinatorClient(
    state_dir=os.environ["AGCOORD_STATE_DIR"],
    autostart=False,
)
with client.acquire_child_cpu_lease(2, timeout=5) as lease:
    assert isinstance(lease, ChildCpuLease)
    report.write_text(json.dumps({
        "lease_id": lease.lease_id,
        "run_id": lease.run_id,
        "requested": lease.requested,
        "granted": lease.granted,
        "full": lease.full,
    }), encoding="utf-8")
    while not release.exists():
        time.sleep(0.01)
""",
            ],
            checkout=str(repository),
            resources={"cpu": 2},
            caller_pid=os.getpid(),
            environment={
                **caller_environment(),
                "LEASE_REPORT": str(report),
                "LEASE_RELEASE": str(release),
            },
        )

        def reported() -> bool:
            if report.exists():
                return True
            terminal = _terminal(client, run_id)
            if terminal is not None:
                pytest.fail(f"lease worker ended before reporting: {terminal}")
            return False

        wait_for(reported, "the admitted child never acquired its CPU lease")
        observed = json.loads(report.read_text(encoding="utf-8"))
        assert observed == {
            "lease_id": observed["lease_id"],
            "run_id": run_id,
            "requested": 2,
            "granted": 2,
            "full": True,
        }
        leases = client.child_cpu_leases(run_id)
        assert len(leases) == 1
        assert set(leases[0]) == {
            "lease_id",
            "run_id",
            "status",
            "requested",
            "minimum",
            "granted",
            "full",
            "owner_pid",
            "created_at",
            "acquired_at",
            "finished_at",
            "position",
        }
        assert leases[0] == {
            "lease_id": observed["lease_id"],
            "run_id": run_id,
            "status": "active",
            "requested": 2,
            "minimum": 2,
            "granted": 2,
            "full": True,
            "owner_pid": leases[0]["owner_pid"],
            "created_at": leases[0]["created_at"],
            "acquired_at": leases[0]["acquired_at"],
            "finished_at": None,
            "position": None,
        }
        snapshot = client.snapshot()
        assert snapshot["allocations"] == {"cpu": 2, "jobs": 1}
        assert [row["run_id"] for row in snapshot["active"]] == [run_id]
        assert observed["lease_id"] not in {
            row["run_id"]
            for section in ("active", "queued", "recent")
            for row in snapshot[section]
        }

        release.touch()
        finished = wait_for(
            lambda: _terminal(client, run_id),
            "the lease worker did not finish",
        )
        assert finished["status"] == "passed"
        assert client.child_cpu_leases(run_id) == []
    finally:
        release.touch()
        running.stop()


def test_partial_grant_and_impossible_exact_request_are_explicit(tmp_path: Path):
    running = RunningCoordinator(
        tmp_path / "state",
        capacities={"jobs": 1, "cpu": 2},
    )
    client = running.start()
    repository = _repository(tmp_path / "repository")
    report = tmp_path / "result.json"

    try:
        run_id = client.submit(
            [
                sys.executable,
                "-u",
                "-c",
                """
import json
import os
from pathlib import Path

from agcoord.queue import CoordinatorClient, CoordinatorError

client = CoordinatorClient(
    state_dir=os.environ["AGCOORD_STATE_DIR"],
    autostart=False,
)
with client.acquire_child_cpu_lease(4, minimum=1, timeout=5) as lease:
    partial = {"granted": lease.granted, "full": lease.full}
try:
    client.acquire_child_cpu_lease(3, timeout=0)
except CoordinatorError as exc:
    refusal = str(exc)
else:
    raise AssertionError("impossible exact request was accepted")
Path(os.environ["LEASE_REPORT"]).write_text(json.dumps({
    "partial": partial,
    "refusal": refusal,
}), encoding="utf-8")
""",
            ],
            checkout=str(repository),
            resources={"cpu": 2},
            caller_pid=os.getpid(),
            environment={
                **caller_environment(),
                "LEASE_REPORT": str(report),
            },
        )
        finished = wait_for(
            lambda: _terminal(client, run_id),
            "the partial-grant worker did not finish",
        )
        assert finished["status"] == "passed", client.log(run_id)["text"]
        observed = json.loads(report.read_text(encoding="utf-8"))
        assert observed["partial"] == {"granted": 2, "full": False}
        assert "minimum 3 exceeds parent budget 2" in observed["refusal"]
    finally:
        running.stop()


def test_explicit_lease_cancellation_wakes_a_blocked_caller(tmp_path: Path):
    running = RunningCoordinator(
        tmp_path / "state",
        capacities={"jobs": 1, "cpu": 1},
    )
    client = running.start()
    repository = _repository(tmp_path / "repository")
    report = tmp_path / "cancelled.json"

    try:
        run_id = client.submit(
            [
                sys.executable,
                "-u",
                "-c",
                """
import json
import os
from pathlib import Path
import threading
import time

from agcoord import CoordinatorClient, CoordinatorError

client = CoordinatorClient(
    state_dir=os.environ["AGCOORD_STATE_DIR"],
    autostart=False,
)
holder = client.acquire_child_cpu_lease(1, timeout=5)
result = {}

def wait_for_token():
    try:
        client.acquire_child_cpu_lease(1, timeout=5)
    except CoordinatorError as exc:
        result["error"] = str(exc)
    else:
        result["error"] = "acquisition unexpectedly succeeded"

waiter = threading.Thread(target=wait_for_token)
waiter.start()
while True:
    leases = client.child_cpu_leases(os.environ["AGCOORD_RUN_ID"])
    waiting = [lease for lease in leases if lease["status"] == "waiting"]
    if waiting:
        break
    time.sleep(0.01)
client.cancel_child_cpu_lease(waiting[0]["lease_id"])
waiter.join(timeout=5)
if waiter.is_alive():
    raise AssertionError("cancelled child CPU lease caller remained blocked")
holder.release()
Path(os.environ["LEASE_REPORT"]).write_text(
    json.dumps(result),
    encoding="utf-8",
)
""",
            ],
            checkout=str(repository),
            resources={"cpu": 1},
            caller_pid=os.getpid(),
            environment={
                **caller_environment(),
                "LEASE_REPORT": str(report),
            },
        )
        finished = wait_for(
            lambda: _terminal(client, run_id),
            "the explicitly cancelled lease caller did not wake",
        )
        assert finished["status"] == "passed", client.log(run_id)["text"]
        assert "ended as cancelled before grant" in json.loads(
            report.read_text(encoding="utf-8")
        )["error"]
    finally:
        running.stop()


def test_spoofed_context_outside_worker_tree_cannot_acquire(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    running = RunningCoordinator(
        tmp_path / "state",
        capacities={"jobs": 1, "cpu": 1},
    )
    client = running.start()
    repository = _repository(tmp_path / "repository")
    entered = tmp_path / "entered"
    release = tmp_path / "release"

    try:
        run_id = client.submit(
            [
                sys.executable,
                "-u",
                "-c",
                """
import os
from pathlib import Path
import time

Path(os.environ["LEASE_ENTERED"]).touch()
release = Path(os.environ["LEASE_RELEASE"])
while not release.exists():
    time.sleep(0.01)
""",
            ],
            checkout=str(repository),
            resources={"cpu": 1},
            caller_pid=os.getpid(),
            environment={
                **caller_environment(),
                "LEASE_ENTERED": str(entered),
                "LEASE_RELEASE": str(release),
            },
        )
        wait_for(entered.exists, "the parent worker did not start")
        monkeypatch.setenv("AGCOORD_RUN_ID", run_id)
        monkeypatch.setenv("AGCOORD_STATE_DIR", str(running.paths.state_dir))

        with pytest.raises(CoordinatorError, match="descendant|admitted"):
            client.acquire_child_cpu_lease(1, timeout=0)
        assert client.child_cpu_leases(run_id, include_terminal=True) == []

        release.touch()
        assert wait_for(
            lambda: _terminal(client, run_id),
            "the parent worker did not finish",
        )["status"] == "passed"
    finally:
        release.touch()
        running.stop()


def test_fifo_bounded_bypass_keeps_small_work_moving_without_starving_large(
    tmp_path: Path,
):
    running = RunningCoordinator(
        tmp_path / "state",
        capacities={"jobs": 1, "cpu": 4},
    )
    client = running.start()
    repository = _repository(tmp_path / "repository")
    root = tmp_path / "coordination"
    root.mkdir()
    orchestrator = f"""
import os
from pathlib import Path
import subprocess
import sys
import time

from agcoord.queue import CoordinatorClient

root = Path(os.environ["LEASE_ROOT"])
source = {_CONTROLLER_SOURCE!r}
client = CoordinatorClient(state_dir=os.environ["AGCOORD_STATE_DIR"], autostart=False)

def launch(name, requested):
    environment = dict(os.environ)
    environment.update({{
        "LEASE_REQUESTED": str(requested),
        "LEASE_MINIMUM": str(requested),
        "LEASE_REPORT": str(root / f"{{name}}.json"),
        "LEASE_RELEASE": str(root / f"{{name}}.release"),
    }})
    return subprocess.Popen([sys.executable, "-u", "-c", source], env=environment)

holder = launch("holder", 3)
while not (root / "holder.json").exists():
    time.sleep(0.01)
large = launch("large", 4)
while not any(
    lease["requested"] == 4 and lease["status"] == "waiting"
    for lease in client.child_cpu_leases(os.environ["AGCOORD_RUN_ID"])
):
    time.sleep(0.01)
small = launch("small", 1)
while not (root / "small.json").exists():
    time.sleep(0.01)
(root / "ready").touch()
statuses = [holder.wait(), small.wait(), large.wait()]
if statuses != [0, 0, 0]:
    raise SystemExit(f"controller statuses: {{statuses}}")
"""

    try:
        run_id = client.submit(
            [sys.executable, "-u", "-c", orchestrator],
            checkout=str(repository),
            resources={"cpu": 4},
            caller_pid=os.getpid(),
            environment={
                **caller_environment(),
                "LEASE_ROOT": str(root),
            },
        )
        _wait_for_report(client, run_id, root / "ready", "lease contenders never became ready")
        live = client.child_cpu_leases(run_id)
        assert sum(
            lease["granted"] for lease in live if lease["status"] == "active"
        ) == 4
        assert sorted(
            (lease["requested"], lease["status"], lease["granted"])
            for lease in live
        ) == [
            (1, "active", 1),
            (3, "active", 3),
            (4, "waiting", 0),
        ]

        (root / "small.release").touch()
        wait_for(
            lambda: not any(
                lease["requested"] == 1 and lease["status"] == "active"
                for lease in client.child_cpu_leases(run_id)
            ),
            "the small lease was not returned",
        )
        assert any(
            lease["requested"] == 4 and lease["status"] == "waiting"
            for lease in client.child_cpu_leases(run_id)
        )

        (root / "holder.release").touch()
        _wait_for_report(client, run_id, root / "large.json", "the large lease starved")
        large = json.loads((root / "large.json").read_text(encoding="utf-8"))
        assert large["granted"] == 4
        assert large["full"] is True

        (root / "large.release").touch()
        finished = wait_for(
            lambda: _terminal(client, run_id),
            "the fairness worker did not finish",
        )
        assert finished["status"] == "passed", client.log(run_id)["text"]
    finally:
        for name in ("small", "holder", "large"):
            (root / f"{name}.release").touch()
        running.stop()


def test_crashed_controller_returns_tokens_to_waiter(tmp_path: Path):
    running = RunningCoordinator(
        tmp_path / "state",
        capacities={"jobs": 1, "cpu": 2},
    )
    client = running.start()
    repository = _repository(tmp_path / "repository")
    root = tmp_path / "coordination"
    root.mkdir()
    orchestrator = f"""
import os
from pathlib import Path
import subprocess
import sys
import time

root = Path(os.environ["LEASE_ROOT"])
source = {_CONTROLLER_SOURCE!r}

def launch(name, crash=False):
    environment = dict(os.environ)
    environment.update({{
        "LEASE_REQUESTED": "2",
        "LEASE_MINIMUM": "2",
        "LEASE_REPORT": str(root / f"{{name}}.json"),
        "LEASE_RELEASE": str(root / f"{{name}}.release"),
        "LEASE_CRASH": "1" if crash else "0",
    }})
    return subprocess.Popen([sys.executable, "-u", "-c", source], env=environment)

crasher = launch("crasher", crash=True)
while not (root / "crasher.json").exists():
    time.sleep(0.01)
waiter = launch("waiter")
crasher.wait()
while not (root / "waiter.json").exists():
    time.sleep(0.01)
(root / "ready").touch()
if waiter.wait() != 0:
    raise SystemExit("waiter failed")
"""

    try:
        run_id = client.submit(
            [sys.executable, "-u", "-c", orchestrator],
            checkout=str(repository),
            resources={"cpu": 2},
            caller_pid=os.getpid(),
            environment={
                **caller_environment(),
                "LEASE_ROOT": str(root),
            },
        )
        _wait_for_report(client, run_id, root / "ready", "the crash waiter was not admitted")
        leases = client.child_cpu_leases(run_id, include_terminal=True)
        assert any(
            lease["requested"] == 2 and lease["status"] == "cancelled"
            for lease in leases
        )
        assert any(
            lease["requested"] == 2 and lease["status"] == "active"
            for lease in leases
        )
        (root / "waiter.release").touch()
        finished = wait_for(
            lambda: _terminal(client, run_id),
            "the crash-recovery worker did not finish",
        )
        assert finished["status"] == "passed", client.log(run_id)["text"]
    finally:
        (root / "waiter.release").touch()
        running.stop()


def test_parent_cancellation_wakes_waiters_and_cancels_all_leases(tmp_path: Path):
    running = RunningCoordinator(
        tmp_path / "state",
        capacities={"jobs": 1, "cpu": 2},
    )
    client = running.start()
    repository = _repository(tmp_path / "repository")
    root = tmp_path / "coordination"
    root.mkdir()
    orchestrator = f"""
import os
from pathlib import Path
import subprocess
import sys
import time

from agcoord.queue import CoordinatorClient

root = Path(os.environ["LEASE_ROOT"])
source = {_CONTROLLER_SOURCE!r}
client = CoordinatorClient(state_dir=os.environ["AGCOORD_STATE_DIR"], autostart=False)

def launch(name, requested):
    environment = dict(os.environ)
    environment.update({{
        "LEASE_REQUESTED": str(requested),
        "LEASE_MINIMUM": str(requested),
        "LEASE_REPORT": str(root / f"{{name}}.json"),
        "LEASE_RELEASE": str(root / f"{{name}}.release"),
    }})
    return subprocess.Popen([sys.executable, "-u", "-c", source], env=environment)

launch("holder", 2)
while not (root / "holder.json").exists():
    time.sleep(0.01)
launch("waiter", 1)
while not any(
    lease["requested"] == 1 and lease["status"] == "waiting"
    for lease in client.child_cpu_leases(os.environ["AGCOORD_RUN_ID"])
):
    time.sleep(0.01)
(root / "ready").touch()
while True:
    time.sleep(1)
"""

    try:
        run_id = client.submit(
            [sys.executable, "-u", "-c", orchestrator],
            checkout=str(repository),
            resources={"cpu": 2},
            caller_pid=os.getpid(),
            environment={
                **caller_environment(),
                "LEASE_ROOT": str(root),
            },
        )
        _wait_for_report(client, run_id, root / "ready", "the cancellable waiters were not ready")
        client.cancel(run_id)
        finished = wait_for(
            lambda: _terminal(client, run_id),
            "parent cancellation did not finish",
        )
        assert finished["status"] == "cancelled"
        leases = client.child_cpu_leases(run_id, include_terminal=True)
        assert len(leases) == 2
        assert {lease["status"] for lease in leases} == {"cancelled"}
    finally:
        for name in ("holder", "waiter"):
            (root / f"{name}.release").touch()
        running.stop()


def test_replacement_broker_preserves_live_lease_without_minting_tokens(tmp_path: Path):
    state_dir = tmp_path / "state"
    repository = _repository(tmp_path / "repository")
    report = tmp_path / "lease.json"
    release = tmp_path / "release"
    original = RunningCoordinator(state_dir, capacities={"jobs": 1, "cpu": 2})
    client = original.start()
    replacement: RunningCoordinator | None = None
    worker_pid: int | None = None

    try:
        run_id = client.submit(
            [
                sys.executable,
                "-u",
                "-c",
                _CONTROLLER_SOURCE,
            ],
            checkout=str(repository),
            resources={"cpu": 2},
            caller_pid=os.getpid(),
            environment={
                **caller_environment(),
                "LEASE_REQUESTED": "2",
                "LEASE_MINIMUM": "2",
                "LEASE_REPORT": str(report),
                "LEASE_RELEASE": str(release),
            },
        )
        _wait_for_report(client, run_id, report, "the recoverable lease was not acquired")
        worker_pid = client.status(run_id)["worker_pid"]
        before = client.child_cpu_leases(run_id)
        assert len(before) == 1 and before[0]["granted"] == 2

        original.kill()
        replacement = RunningCoordinator(
            state_dir,
            capacities={"jobs": 1, "cpu": 2},
        )
        recovered_client = replacement.start()
        after = recovered_client.child_cpu_leases(run_id)
        assert after == before
        assert sum(lease["granted"] for lease in after) == 2

        release.touch()
        finished = wait_for(
            lambda: _terminal(recovered_client, run_id),
            "the recovered lease worker did not finish",
        )
        assert finished["status"] == "interrupted"
        assert recovered_client.child_cpu_leases(run_id) == []
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
