"""Behavioral contract for the optional pytest-xdist CPU lease adapter."""

from __future__ import annotations

import os
from pathlib import Path
import signal
import subprocess
import sys

import pytest

from agcoord.queue import CoordinatorClient

from conftest import RunningCoordinator, caller_environment, wait_for


def _git(path: Path, *arguments: str) -> str:
    completed = subprocess.run(
        ["git", "-C", str(path), *arguments],
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def _pytest_repository(
    path: Path,
    *,
    blocking: bool = False,
    crashing_worker: bool = False,
) -> Path:
    path.mkdir()
    _git(path, "init", "-b", "main")
    _git(path, "config", "user.name", "AGCoord xdist tests")
    _git(path, "config", "user.email", "xdist@example.invalid")
    (path / "conftest.py").write_text(
        """
import os
from pathlib import Path

def pytest_sessionstart(session):
    worker = os.environ.get("PYTEST_XDIST_WORKER")
    if worker:
        (Path(os.environ["XDIST_REPORT"]) / f"{worker}-{os.getpid()}").touch()
""",
        encoding="utf-8",
    )
    if blocking:
        test_source = """
import os
from pathlib import Path
import time

def wait_for_release():
    release = Path(os.environ["XDIST_HOLD"])
    deadline = time.monotonic() + 20
    while not release.exists():
        if time.monotonic() >= deadline:
            raise AssertionError("xdist fixture was never released")
        time.sleep(0.01)

def test_one():
    wait_for_release()

def test_two():
    wait_for_release()
"""
    elif crashing_worker:
        test_source = """
import os
from pathlib import Path

def test_00_crash_one_worker():
    marker = Path(os.environ["XDIST_CRASH_MARKER"])
    if not marker.exists():
        marker.touch()
        os._exit(17)

def test_10_replacement_worker_finishes_remaining_work():
    assert True
"""
    else:
        test_source = """
def test_one():
    assert True

def test_two():
    assert True
"""
    (path / "test_sample.py").write_text(test_source, encoding="utf-8")
    _git(path, "add", ".")
    _git(path, "commit", "-m", "pytest fixture")
    return path


def _terminal(client: CoordinatorClient, run_id: str) -> dict[str, object] | None:
    row = client.status(run_id)
    return row if row["status"] not in {"queued", "running"} else None


def _workers(report: Path) -> set[str]:
    return {path.name.split("-", 1)[0] for path in report.iterdir()}


def _controller_command(*arguments: str) -> list[str]:
    return [
        sys.executable,
        "-m",
        "pytest",
        "-q",
        "-p",
        "no:cacheprovider",
        *arguments,
    ]


def _outside_pytest(
    command: list[str],
    *,
    cwd: Path,
    environment: dict[str, str],
) -> subprocess.CompletedProcess[str]:
    process = subprocess.Popen(
        command,
        cwd=cwd,
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    try:
        stdout, stderr = process.communicate(timeout=20)
    except BaseException:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
            process.communicate()
        raise
    return subprocess.CompletedProcess(
        process.args,
        process.returncode,
        stdout,
        stderr,
    )


@pytest.mark.parametrize(
    ("automatic_mode", "maximum", "expected_workers"),
    [("auto", None, 2), ("logical", None, 2), ("auto", 1, 1)],
)
def test_automatic_mode_uses_one_parent_lease_instead_of_gate_wide_environment(
    tmp_path: Path,
    automatic_mode: str,
    maximum: int | None,
    expected_workers: int,
):
    running = RunningCoordinator(
        tmp_path / "state",
        capacities={"jobs": 1, "cpu": 2},
    )
    client = running.start()
    repository = _pytest_repository(tmp_path / "repository")
    report = tmp_path / "workers"
    report.mkdir()
    arguments = ["-n", automatic_mode]
    if maximum is not None:
        arguments.extend(["--maxprocesses", str(maximum)])

    try:
        run_id = client.submit(
            _controller_command(*arguments),
            checkout=str(repository),
            resources={"cpu": 2},
            caller_pid=os.getpid(),
            environment={
                **caller_environment(),
                "PYTEST_XDIST_AUTO_NUM_WORKERS": "3",
                "XDIST_REPORT": str(report),
            },
        )
        finished = wait_for(
            lambda: _terminal(client, run_id),
            "the admitted pytest-xdist controller did not finish",
            timeout=20,
        )
        assert finished["status"] == "passed", client.log(run_id)["text"]
        assert _workers(report) == {
            f"gw{index}" for index in range(expected_workers)
        }
        leases = client.child_cpu_leases(run_id, include_terminal=True)
        assert len(leases) == 1
        assert leases[0]["requested"] == expected_workers
        assert leases[0]["granted"] == expected_workers
        assert leases[0]["status"] == "released"
    finally:
        running.stop()


def test_explicit_worker_count_is_exact_and_an_impossible_count_fails_clearly(
    tmp_path: Path,
):
    running = RunningCoordinator(
        tmp_path / "state",
        capacities={"jobs": 1, "cpu": 2},
    )
    client = running.start()
    repository = _pytest_repository(tmp_path / "repository")
    exact_report = tmp_path / "exact-workers"
    impossible_report = tmp_path / "impossible-workers"
    exact_report.mkdir()
    impossible_report.mkdir()

    try:
        exact_id = client.submit(
            _controller_command("-n", "2"),
            checkout=str(repository),
            resources={"cpu": 2},
            caller_pid=os.getpid(),
            environment={
                **caller_environment(),
                "XDIST_REPORT": str(exact_report),
            },
        )
        exact = wait_for(
            lambda: _terminal(client, exact_id),
            "the explicit xdist controller did not finish",
            timeout=20,
        )
        assert exact["status"] == "passed", client.log(exact_id)["text"]
        assert _workers(exact_report) == {"gw0", "gw1"}
        assert [
            (lease["requested"], lease["minimum"], lease["granted"], lease["status"])
            for lease in client.child_cpu_leases(exact_id, include_terminal=True)
        ] == [(2, 2, 2, "released")]

        impossible_id = client.submit(
            _controller_command("-n", "3"),
            checkout=str(repository),
            resources={"cpu": 2},
            caller_pid=os.getpid(),
            environment={
                **caller_environment(),
                "XDIST_REPORT": str(impossible_report),
            },
        )
        impossible = wait_for(
            lambda: _terminal(client, impossible_id),
            "the impossible explicit xdist request did not finish",
            timeout=20,
        )
        assert impossible["status"] == "failed"
        assert _workers(impossible_report) == set()
        assert client.child_cpu_leases(impossible_id, include_terminal=True) == []
        transcript = client.log(impossible_id)["text"]
        assert "could not acquire 3 CPU worker token(s)" in transcript
        assert "minimum 3 exceeds parent budget 2" in transcript
    finally:
        running.stop()


@pytest.mark.parametrize("arguments", [(), ("-n", "0")])
def test_plain_pytest_and_n_zero_stay_serial_without_a_lease(
    tmp_path: Path,
    arguments: tuple[str, ...],
):
    running = RunningCoordinator(
        tmp_path / "state",
        capacities={"jobs": 1, "cpu": 2},
    )
    client = running.start()
    repository = _pytest_repository(tmp_path / "repository")
    report = tmp_path / "workers"
    report.mkdir()

    try:
        run_id = client.submit(
            _controller_command(*arguments),
            checkout=str(repository),
            resources={"cpu": 2},
            caller_pid=os.getpid(),
            environment={
                **caller_environment(),
                "XDIST_REPORT": str(report),
            },
        )
        finished = wait_for(
            lambda: _terminal(client, run_id),
            "the serial pytest controller did not finish",
            timeout=20,
        )
        assert finished["status"] == "passed", client.log(run_id)["text"]
        assert _workers(report) == set()
        assert client.child_cpu_leases(run_id, include_terminal=True) == []
    finally:
        running.stop()


def test_outside_an_admitted_run_preserves_upstream_xdist_auto_behavior(
    tmp_path: Path,
):
    repository = _pytest_repository(tmp_path / "repository")
    report = tmp_path / "workers"
    report.mkdir()
    environment = {
        **caller_environment(),
        "PYTEST_XDIST_AUTO_NUM_WORKERS": "2",
        "XDIST_REPORT": str(report),
    }

    completed = _outside_pytest(
        _controller_command("-n", "auto"),
        cwd=repository,
        environment=environment,
    )

    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert _workers(report) == {"gw0", "gw1"}


def test_adapter_entry_point_is_inert_when_xdist_is_not_loaded(tmp_path: Path):
    repository = _pytest_repository(tmp_path / "repository")
    report = tmp_path / "workers"
    report.mkdir()
    environment = {
        **caller_environment(),
        "XDIST_REPORT": str(report),
    }

    completed = _outside_pytest(
        _controller_command(
            "-p",
            "no:xdist",
            "-p",
            "no:xdist.looponfail",
        ),
        cwd=repository,
        environment=environment,
    )

    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert _workers(report) == set()


def test_two_controllers_contend_fairly_without_exceeding_the_parent_budget(
    tmp_path: Path,
):
    running = RunningCoordinator(
        tmp_path / "state",
        capacities={"jobs": 1, "cpu": 2},
    )
    client = running.start()
    repository = _pytest_repository(tmp_path / "repository", blocking=True)
    root = tmp_path / "controllers"
    root.mkdir()
    for name in ("first", "second"):
        (root / f"{name}-workers").mkdir()

    orchestrator = """
import os
from pathlib import Path
import subprocess
import sys
import time

root = Path(os.environ["XDIST_ROOT"])
command = [
    sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", "-n", "auto"
]

def launch(name):
    environment = dict(os.environ)
    environment.update({
        "PYTEST_XDIST_AUTO_NUM_WORKERS": "3",
        "XDIST_REPORT": str(root / f"{name}-workers"),
        "XDIST_HOLD": str(root / f"{name}.hold"),
    })
    return subprocess.Popen(command, env=environment)

def wait_for_workers(process, name):
    report = root / f"{name}-workers"
    deadline = time.monotonic() + 20
    while len(list(report.iterdir())) < 2:
        status = process.poll()
        if status is not None:
            raise SystemExit(f"{name} controller exited early with {status}")
        if time.monotonic() >= deadline:
            raise SystemExit(f"{name} controller never started its workers")
        time.sleep(0.01)

first = launch("first")
wait_for_workers(first, "first")
second = launch("second")
(root / "ready").touch()
statuses = [first.wait(), second.wait()]
if statuses != [0, 0]:
    raise SystemExit(f"xdist controller statuses: {statuses}")
"""

    try:
        run_id = client.submit(
            [sys.executable, "-u", "-c", orchestrator],
            checkout=str(repository),
            resources={"cpu": 2},
            caller_pid=os.getpid(),
            environment={
                **caller_environment(),
                "XDIST_ROOT": str(root),
            },
        )
        wait_for((root / "ready").exists, "the two xdist controllers did not start")

        def contended() -> list[dict[str, object]] | None:
            leases = client.child_cpu_leases(run_id)
            statuses = sorted(lease["status"] for lease in leases)
            return leases if statuses == ["active", "waiting"] else None

        leases = wait_for(
            contended,
            "the second xdist controller did not wait fairly",
        )
        assert sum(
            lease["granted"] for lease in leases if lease["status"] == "active"
        ) == 2
        snapshot = client.snapshot()
        assert snapshot["allocations"] == {"cpu": 2, "jobs": 1}
        assert [row["run_id"] for row in snapshot["active"]] == [run_id]
        assert _workers(root / "first-workers") == {"gw0", "gw1"}
        assert _workers(root / "second-workers") == set()

        (root / "first.hold").touch()
        wait_for(
            lambda: _workers(root / "second-workers") == {"gw0", "gw1"},
            "the waiting xdist controller did not acquire returned tokens",
            timeout=20,
        )
        second_live = client.child_cpu_leases(run_id)
        assert sum(
            lease["granted"]
            for lease in second_live
            if lease["status"] == "active"
        ) == 2

        (root / "second.hold").touch()
        finished = wait_for(
            lambda: _terminal(client, run_id),
            "the contending xdist controllers did not finish",
            timeout=20,
        )
        assert finished["status"] == "passed", client.log(run_id)["text"]
        terminal = client.child_cpu_leases(run_id, include_terminal=True)
        assert len(terminal) == 2
        assert {lease["status"] for lease in terminal} == {"released"}
    finally:
        (root / "first.hold").touch()
        (root / "second.hold").touch()
        running.stop()


def test_xdist_worker_restart_does_not_acquire_a_second_controller_lease(
    tmp_path: Path,
):
    running = RunningCoordinator(
        tmp_path / "state",
        capacities={"jobs": 1, "cpu": 1},
    )
    client = running.start()
    repository = _pytest_repository(
        tmp_path / "repository",
        crashing_worker=True,
    )
    report = tmp_path / "workers"
    report.mkdir()
    marker = tmp_path / "crashed"

    try:
        run_id = client.submit(
            _controller_command("-n", "1", "--max-worker-restart", "1"),
            checkout=str(repository),
            resources={"cpu": 1},
            caller_pid=os.getpid(),
            environment={
                **caller_environment(),
                "XDIST_CRASH_MARKER": str(marker),
                "XDIST_REPORT": str(report),
            },
        )
        finished = wait_for(
            lambda: _terminal(client, run_id),
            "pytest-xdist did not finish after restarting its crashed worker",
            timeout=20,
        )
        assert finished["status"] == "failed"
        assert len(list(report.iterdir())) >= 2
        leases = client.child_cpu_leases(run_id, include_terminal=True)
        assert len(leases) == 1
        assert leases[0]["requested"] == 1
        assert leases[0]["status"] == "released"
    finally:
        running.stop()


@pytest.mark.parametrize(
    ("termination", "expected_status"),
    [("cancel", "cancelled"), ("crash", "failed")],
)
def test_controller_cancellation_or_crash_reclaims_the_complete_lease(
    tmp_path: Path,
    termination: str,
    expected_status: str,
):
    running = RunningCoordinator(
        tmp_path / "state",
        capacities={"jobs": 1, "cpu": 2},
    )
    client = running.start()
    repository = _pytest_repository(tmp_path / "repository", blocking=True)
    report = tmp_path / "workers"
    report.mkdir()
    hold = tmp_path / "hold"

    try:
        run_id = client.submit(
            _controller_command("-n", "2"),
            checkout=str(repository),
            resources={"cpu": 2},
            caller_pid=os.getpid(),
            environment={
                **caller_environment(),
                "XDIST_HOLD": str(hold),
                "XDIST_REPORT": str(report),
            },
        )
        wait_for(
            lambda: _workers(report) == {"gw0", "gw1"},
            "the terminable xdist controller did not start two workers",
            timeout=20,
        )
        active = client.child_cpu_leases(run_id)
        assert len(active) == 1 and active[0]["granted"] == 2

        if termination == "cancel":
            client.cancel(run_id)
        else:
            worker_pid = client.status(run_id)["worker_pid"]
            assert isinstance(worker_pid, int)
            os.kill(worker_pid, signal.SIGKILL)

        finished = wait_for(
            lambda: _terminal(client, run_id),
            "the terminated xdist controller did not become terminal",
            timeout=20,
        )
        assert finished["status"] == expected_status
        leases = client.child_cpu_leases(run_id, include_terminal=True)
        assert len(leases) == 1
        assert leases[0]["granted"] == 2
        assert leases[0]["status"] == "cancelled"
    finally:
        hold.touch()
        running.stop()
