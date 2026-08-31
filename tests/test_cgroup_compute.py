"""Behavioral CPU-bandwidth and PID contracts for the cgroup v2 backend."""

from __future__ import annotations

import json
import os
from pathlib import Path
import sys

import pytest

from agcoord.cgroup import CgroupProbe, CgroupV2Backend

from conftest import RunningCoordinator, caller_environment, wait_for
from test_cgroup import FakeCgroupV2System, _repository, _terminal


COMPUTE_BINDINGS = {
    "cpu": {
        "backend": "cgroup-v2",
        "kind": "cpu",
        "mode": "required",
        "unit": "logical-cpu",
    },
    "pids": {
        "backend": "cgroup-v2",
        "kind": "processes",
        "mode": "required",
        "unit": "processes",
    },
}


class ControllerFakeCgroupV2System(FakeCgroupV2System):
    """Cgroup seam with deterministic controller files and a deterministic clock."""

    def __init__(
        self,
        root: Path,
        *,
        controllers: set[str],
        fail_writes: set[str] | None = None,
    ) -> None:
        super().__init__(root)
        self.controllers = frozenset(controllers)
        self.fail_writes = set() if fail_writes is None else set(fail_writes)
        self.enabled: dict[Path, set[str]] = {root: set()}
        self.files: dict[Path, dict[str, str]] = {root: {}}
        self.now_ns = 0
        self.terminal_on_empty: set[Path] = set()

    def probe(self, root: Path) -> CgroupProbe:
        assert root == self.root
        return CgroupProbe(True, None, self.controllers)

    def monotonic_ns(self) -> int:
        return self.now_ns

    def create_group(self, parent: Path, name: str):
        identity = super().create_group(parent, name)
        path = parent / name
        self.enabled[path] = set()
        self.files[path] = {
            "cpu.max": "max 100000\n",
            "cpu.stat": (
                "usage_usec 0\nuser_usec 0\nsystem_usec 0\n"
                "core_sched.force_idle_usec 0\n"
                "nr_periods 0\nnr_throttled 0\nthrottled_usec 0\n"
            ),
            "cpu.weight": "100\n",
            "pids.current": "0\n",
            "pids.events": "max 0\n",
            "pids.max": "max\n",
            "pids.peak": "0\n",
        }
        return identity

    def enable_controllers(self, path: Path, controllers: set[str]) -> None:
        if not controllers <= self.controllers:
            raise OSError("controller unavailable")
        self.enabled[path].update(controllers)

    def write_file(self, path: Path, name: str, value: str) -> None:
        if name in self.fail_writes:
            raise OSError(f"injected {name} write failure")
        self.files[path][name] = f"{value.rstrip()}\n"

    def read_file(self, path: Path, name: str) -> str:
        return self.files[path][name]

    def remove_group(self, path: Path) -> None:
        super().remove_group(path)
        self.enabled.pop(path, None)
        self.files.pop(path, None)

    def set_terminal_metrics(self, leaf: Path) -> None:
        self.now_ns = 100_000_000
        self.files[leaf].update(
            {
                "cpu.stat": (
                    "usage_usec 180000\nuser_usec 150000\nsystem_usec 30000\n"
                    "core_sched.force_idle_usec 0\n"
                    "nr_periods 2\nnr_throttled 1\nthrottled_usec 20000\n"
                ),
                "pids.current": "3\n",
                "pids.events": "max 1\n",
                "pids.peak": "4\n",
            }
        )

    def populated(self, path: Path) -> bool:
        populated = super().populated(path)
        if not populated and path in self.terminal_on_empty:
            self.terminal_on_empty.remove(path)
            self.set_terminal_metrics(path)
        return populated


def test_cpu_and_pid_limits_apply_before_user_code_and_report_terminal_usage(
    tmp_path: Path,
):
    root = tmp_path / "delegated"
    system = ControllerFakeCgroupV2System(root, controllers={"cpu", "pids"})
    state_dir = tmp_path / "state"
    backend = CgroupV2Backend(root, state_dir=state_dir, system=system)
    running = RunningCoordinator(
        state_dir,
        capacities={"jobs": 1, "cpu": 2, "pids": 5},
        resource_bindings=COMPUTE_BINDINGS,
        resource_backends={"cgroup-v2": backend},
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
from pathlib import Path
import os
import time

Path(os.environ["COMPUTE_ENTERED"]).touch()
while not Path(os.environ["COMPUTE_RELEASE"]).exists():
    time.sleep(0.01)
""",
            ],
            checkout=str(repository),
            resources={"cpu": 2, "pids": 5},
            caller_pid=os.getpid(),
            environment={
                **caller_environment(),
                "COMPUTE_ENTERED": str(entered),
                "COMPUTE_RELEASE": str(release),
            },
        )
        observed = wait_for(
            lambda: entered.exists() or _terminal(client, run_id),
            "the typed compute run neither started nor refused",
        )
        assert entered.exists(), observed
        leaves = [path for path in system.groups() if path.name.startswith("run-")]
        assert len(leaves) == 1
        leaf = leaves[0]
        owner = leaf.parent
        assert system.enabled[root] == {"cpu", "pids"}
        assert system.enabled[owner] == {"cpu", "pids"}
        assert system.files[leaf]["cpu.max"] == "200000 100000\n"
        assert system.files[leaf]["pids.max"] == "5\n"
        assert system.files[leaf]["cpu.weight"] == "100\n"
        assert "cpuset.cpus" not in system.files[leaf]

        system.set_terminal_metrics(leaf)

        def measured() -> bool:
            receipt = client.status(run_id)["resource_receipt"]
            codes = {event["code"] for event in receipt["events"]}
            return receipt["peak"] == {"cpu": 2, "pids": 4} and {
                "cpu-throttled",
                "pids-limit-hit",
            } <= codes

        wait_for(measured, "the controller metrics were not recorded durably")
        release.touch()
        finished = wait_for(
            lambda: _terminal(client, run_id),
            "the typed compute run did not finish",
        )
        assert finished["status"] == "passed"
        assert finished["resource_receipt"]["applied"] == {"cpu": 2, "pids": 5}
        assert finished["resource_receipt"]["peak"] == {"cpu": 2, "pids": 4}
        codes = [
            event["code"]
            for event in finished["resource_receipt"]["events"]
        ]
        assert codes.count("cpu-throttled") == 1
        assert codes.count("pids-limit-hit") == 1
        assert system.groups() == set()
    finally:
        release.touch()
        running.stop()
        system.close()


def test_sibling_runs_keep_independent_limits_metrics_and_violations(tmp_path: Path):
    root = tmp_path / "delegated"
    system = ControllerFakeCgroupV2System(root, controllers={"cpu", "pids"})
    state_dir = tmp_path / "state"
    backend = CgroupV2Backend(root, state_dir=state_dir, system=system)
    running = RunningCoordinator(
        state_dir,
        capacities={"jobs": 2, "cpu": 3, "pids": 10},
        resource_bindings=COMPUTE_BINDINGS,
        resource_backends={"cgroup-v2": backend},
    )
    client = running.start()
    first_repository = _repository(tmp_path / "first-repository")
    second_repository = _repository(tmp_path / "second-repository")
    first_entered = tmp_path / "first-entered"
    second_entered = tmp_path / "second-entered"
    first_release = tmp_path / "first-release"
    second_release = tmp_path / "second-release"

    command = [
        sys.executable,
        "-u",
        "-c",
        """
from pathlib import Path
import os
import time

Path(os.environ["COMPUTE_ENTERED"]).touch()
while not Path(os.environ["COMPUTE_RELEASE"]).exists():
    time.sleep(0.01)
""",
    ]
    try:
        first_id = client.submit(
            command,
            checkout=str(first_repository),
            resources={"cpu": 1, "pids": 4},
            caller_pid=os.getpid(),
            environment={
                **caller_environment(),
                "COMPUTE_ENTERED": str(first_entered),
                "COMPUTE_RELEASE": str(first_release),
            },
        )
        second_id = client.submit(
            command,
            checkout=str(second_repository),
            resources={"cpu": 2, "pids": 6},
            caller_pid=os.getpid(),
            environment={
                **caller_environment(),
                "COMPUTE_ENTERED": str(second_entered),
                "COMPUTE_RELEASE": str(second_release),
            },
        )
        wait_for(
            lambda: first_entered.exists() and second_entered.exists(),
            "the sibling compute runs did not overlap",
        )
        leaves = [path for path in system.groups() if path.name.startswith("run-")]
        assert len(leaves) == 2

        def leaf_for(run_id: str) -> Path:
            worker_pid = client.status(run_id)["worker_pid"]
            assert isinstance(worker_pid, int)
            return next(path for path in leaves if worker_pid in system.members(path))

        first_leaf = leaf_for(first_id)
        second_leaf = leaf_for(second_id)
        assert first_leaf != second_leaf
        assert system.files[first_leaf]["cpu.max"] == "100000 100000\n"
        assert system.files[first_leaf]["pids.max"] == "4\n"
        assert system.files[second_leaf]["cpu.max"] == "200000 100000\n"
        assert system.files[second_leaf]["pids.max"] == "6\n"

        system.set_terminal_metrics(first_leaf)
        system.files[second_leaf].update(
            {
                "cpu.stat": (
                    "usage_usec 50000\nuser_usec 40000\nsystem_usec 10000\n"
                    "core_sched.force_idle_usec 0\n"
                    "nr_periods 1\nnr_throttled 0\nthrottled_usec 0\n"
                ),
                "pids.current": "2\n",
                "pids.events": "max 0\n",
                "pids.peak": "2\n",
            }
        )

        def independently_measured() -> bool:
            first = client.status(first_id)["resource_receipt"]
            second = client.status(second_id)["resource_receipt"]
            first_codes = {event["code"] for event in first["events"]}
            second_codes = {event["code"] for event in second["events"]}
            return (
                first["peak"] == {"cpu": 2, "pids": 4}
                and second["peak"] == {"cpu": 1, "pids": 2}
                and {"cpu-throttled", "pids-limit-hit"} <= first_codes
                and "cpu-throttled" not in second_codes
                and "pids-limit-hit" not in second_codes
            )

        wait_for(independently_measured, "sibling cgroup metrics were not independent")
        first_release.touch()
        second_release.touch()
        assert wait_for(lambda: _terminal(client, first_id), "first sibling did not finish")[
            "status"
        ] == "passed"
        assert wait_for(lambda: _terminal(client, second_id), "second sibling did not finish")[
            "status"
        ] == "passed"
        assert system.groups() == set()
    finally:
        first_release.touch()
        second_release.touch()
        running.stop()
        system.close()


@pytest.mark.skipif(
    not os.environ.get("AGCOORD_TEST_CGROUP_ROOT"),
    reason="set AGCOORD_TEST_CGROUP_ROOT to an exclusive writable delegation",
)
def test_real_cpu_bandwidth_and_pid_limit_cover_the_complete_process_tree(
    tmp_path: Path,
):
    root = Path(os.environ["AGCOORD_TEST_CGROUP_ROOT"]).resolve(strict=True)
    state_dir = tmp_path / "state"
    backend = CgroupV2Backend(root, state_dir=state_dir)
    capability = backend.probe()
    assert capability["available"] is True
    assert {"cpu", "processes"} <= set(capability["kinds"])
    running = RunningCoordinator(
        state_dir,
        capacities={"jobs": 1, "cpu": 1, "pids": 8},
        resource_bindings=COMPUTE_BINDINGS,
        resource_backends={"cgroup-v2": backend},
    )
    client = running.start()
    repository = _repository(tmp_path / "repository")
    report = tmp_path / "real-compute.json"

    try:
        run_id = client.submit(
            [
                sys.executable,
                "-u",
                "-c",
                """
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
before_times = os.times()
started_at = time.monotonic()
workers = [subprocess.Popen([sys.executable, "-c", busy]) for _ in range(4)]
for worker in workers:
    worker.wait()
wall_seconds = time.monotonic() - started_at
after_times = os.times()
child_cpu_seconds = (
    after_times.children_user
    + after_times.children_system
    - before_times.children_user
    - before_times.children_system
)

sleepers = []
exhausted = False
for _attempt in range(32):
    try:
        sleepers.append(
            subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])
        )
    except OSError as exc:
        if exc.errno != errno.EAGAIN:
            raise
        exhausted = True
        break
try:
    Path(sys.argv[1]).write_text(
        json.dumps({
            "affinity_count": len(os.sched_getaffinity(0)),
            "child_cpu_seconds": child_cpu_seconds,
            "exhausted": exhausted,
            "sleepers": len(sleepers),
            "wall_seconds": wall_seconds,
        }),
        encoding="utf-8",
    )
finally:
    for sleeper in sleepers:
        sleeper.terminate()
    for sleeper in sleepers:
        sleeper.wait()
""",
                str(report),
            ],
            checkout=str(repository),
            resources={"cpu": 1, "pids": 8},
            caller_pid=os.getpid(),
            environment=caller_environment(),
        )
        finished = wait_for(
            lambda: _terminal(client, run_id),
            "the real compute controller run did not finish",
            timeout=30,
        )
        assert finished["status"] == "passed"
        observed = json.loads(report.read_text(encoding="utf-8"))
        assert observed["child_cpu_seconds"] <= observed["wall_seconds"] * 1.5 + 0.1
        assert observed["exhausted"] is True
        assert observed["sleepers"] <= 7
        codes = {
            event["code"]
            for event in finished["resource_receipt"]["events"]
        }
        assert "pids-limit-hit" in codes
        if observed["affinity_count"] > 1:
            assert "cpu-throttled" in codes
        assert 1 <= finished["resource_receipt"]["peak"]["pids"] <= 8
        assert not (root / backend.owner_name).exists()
    finally:
        running.stop()


def test_missing_required_cpu_controller_refuses_before_user_code(tmp_path: Path):
    root = tmp_path / "delegated"
    system = ControllerFakeCgroupV2System(root, controllers={"pids"})
    state_dir = tmp_path / "state"
    backend = CgroupV2Backend(root, state_dir=state_dir, system=system)
    running = RunningCoordinator(
        state_dir,
        capacities={"jobs": 1, "cpu": 1},
        resource_bindings={"cpu": COMPUTE_BINDINGS["cpu"]},
        resource_backends={"cgroup-v2": backend},
    )
    client = running.start()
    repository = _repository(tmp_path / "repository")
    marker = tmp_path / "user-code-ran"

    try:
        run_id = client.submit(
            [sys.executable, "-c", "from pathlib import Path; Path(__import__('sys').argv[1]).touch()", str(marker)],
            checkout=str(repository),
            resources={"cpu": 1},
            caller_pid=os.getpid(),
            environment=caller_environment(),
        )
        finished = wait_for(
            lambda: _terminal(client, run_id),
            "the missing CPU controller did not refuse the run",
        )
        assert finished["status"] == "failed"
        assert finished["exit_status"] == 125
        assert finished["failure_reason"] == "resource-enforcement-failed"
        assert not marker.exists()
        assert {
            (event["resource"], event["stage"], event["code"])
            for event in finished["resource_receipt"]["events"]
        } == {("cpu", "probe", "kind-unsupported")}
        assert system.groups() == set()
    finally:
        running.stop()
        system.close()


@pytest.mark.parametrize(
    ("mode", "status", "exit_status", "ran"),
    [
        ("required", "failed", 125, False),
        ("best-effort", "passed", 0, True),
    ],
)
def test_controller_write_failure_obeys_mode_and_cleans_partial_groups(
    tmp_path: Path,
    mode: str,
    status: str,
    exit_status: int,
    ran: bool,
):
    root = tmp_path / "delegated"
    system = ControllerFakeCgroupV2System(
        root,
        controllers={"cpu"},
        fail_writes={"cpu.max"},
    )
    state_dir = tmp_path / "state"
    backend = CgroupV2Backend(root, state_dir=state_dir, system=system)
    binding = {"cpu": {**COMPUTE_BINDINGS["cpu"], "mode": mode}}
    running = RunningCoordinator(
        state_dir,
        capacities={"jobs": 1, "cpu": 1},
        resource_bindings=binding,
        resource_backends={"cgroup-v2": backend},
    )
    client = running.start()
    repository = _repository(tmp_path / "repository")
    marker = tmp_path / "user-code-ran"

    try:
        run_id = client.submit(
            [sys.executable, "-c", "from pathlib import Path; Path(__import__('sys').argv[1]).touch()", str(marker)],
            checkout=str(repository),
            resources={"cpu": 1},
            caller_pid=os.getpid(),
            environment=caller_environment(),
        )
        finished = wait_for(
            lambda: _terminal(client, run_id),
            "the controller write failure did not finish",
        )
        assert finished["status"] == status
        assert finished["exit_status"] == exit_status
        assert marker.exists() is ran
        event = next(
            event
            for event in finished["resource_receipt"]["events"]
            if event["stage"] == "prepare"
        )
        assert event["status"] == ("failed" if mode == "required" else "unapplied")
        assert event["code"] == "prepare-failed"
        assert finished["resource_receipt"]["applied"] == {}
        assert system.groups() == set()
    finally:
        running.stop()
        system.close()


@pytest.mark.parametrize(
    ("cancel", "expected_status"),
    [(False, "passed"), (True, "cancelled")],
)
def test_terminal_paths_capture_pid_peak_and_limit_event_before_cleanup(
    tmp_path: Path,
    cancel: bool,
    expected_status: str,
):
    root = tmp_path / "delegated"
    system = ControllerFakeCgroupV2System(root, controllers={"pids"})
    state_dir = tmp_path / "state"
    backend = CgroupV2Backend(root, state_dir=state_dir, system=system)
    running = RunningCoordinator(
        state_dir,
        capacities={"jobs": 1, "pids": 5},
        resource_bindings={"pids": COMPUTE_BINDINGS["pids"]},
        resource_backends={"cgroup-v2": backend},
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
from pathlib import Path
import os
import time

Path(os.environ["COMPUTE_ENTERED"]).touch()
while not Path(os.environ["COMPUTE_RELEASE"]).exists():
    time.sleep(0.01)
""",
            ],
            checkout=str(repository),
            resources={"pids": 5},
            caller_pid=os.getpid(),
            environment={
                **caller_environment(),
                "COMPUTE_ENTERED": str(entered),
                "COMPUTE_RELEASE": str(release),
            },
        )
        wait_for(entered.exists, "the terminal-metric worker did not start")
        leaf = next(path for path in system.groups() if path.name.startswith("run-"))
        system.terminal_on_empty.add(leaf)
        if cancel:
            client.cancel(run_id)
        else:
            release.touch()
        finished = wait_for(
            lambda: _terminal(client, run_id),
            "the terminal-metric worker did not finish",
        )
        assert finished["status"] == expected_status
        assert finished["resource_receipt"]["peak"] == {"pids": 4}
        violation = next(
            event
            for event in finished["resource_receipt"]["events"]
            if event["code"] == "pids-limit-hit"
        )
        assert violation["stage"] == "finish"
        assert system.groups() == set()
    finally:
        release.touch()
        running.stop()
        system.close()
