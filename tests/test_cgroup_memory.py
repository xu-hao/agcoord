"""Behavioral memory, swap, pressure, and OOM contracts for cgroup v2."""

from __future__ import annotations

import os
from pathlib import Path
import signal
import sys

import pytest

from agcoord.cgroup import CgroupProbe, CgroupV2Backend
from agcoord.resources import ResourceMeasurement, ResourceRequest

from conftest import RunningReferenceBroker, caller_environment, wait_for
from test_cgroup import _repository, _terminal
from test_cgroup_compute import ControllerFakeCgroupV2System


MIB = 1024 * 1024
MEMORY_BINDINGS = {
    "ram": {
        "backend": "cgroup-v2",
        "kind": "memory",
        "mode": "required",
        "unit": "bytes",
    },
    "pressure": {
        "backend": "cgroup-v2",
        "kind": "memory-high",
        "mode": "required",
        "unit": "bytes",
    },
    "swap": {
        "backend": "cgroup-v2",
        "kind": "swap",
        "mode": "required",
        "unit": "bytes",
    },
}


class MemoryFakeCgroupV2System(ControllerFakeCgroupV2System):
    """Deterministic memory-controller files layered on the process-tree fake."""

    def __init__(
        self,
        root: Path,
        *,
        controllers: set[str] | None = None,
        swap_total: int = 8 * MIB,
    ) -> None:
        super().__init__(
            root,
            controllers={"memory"} if controllers is None else controllers,
        )
        self.swap_total = swap_total

    def create_group(self, parent: Path, name: str):
        identity = super().create_group(parent, name)
        path = parent / name
        self.files[path].update(
            {
                "memory.current": "0\n",
                "memory.events": (
                    "low 0\nhigh 0\nmax 0\noom 0\n"
                    "oom_kill 0\noom_group_kill 0\n"
                ),
                "memory.high": "max\n",
                "memory.max": "max\n",
                "memory.oom.group": "0\n",
                "memory.peak": "0\n",
                "memory.pressure": (
                    "some avg10=0.00 avg60=0.00 avg300=0.00 total=0\n"
                    "full avg10=0.00 avg60=0.00 avg300=0.00 total=0\n"
                ),
                "memory.swap.current": "0\n",
                "memory.swap.events": "high 0\nmax 0\nfail 0\n",
                "memory.swap.max": "max\n",
                "memory.swap.peak": "0\n",
            }
        )
        return identity

    def swap_total_bytes(self) -> int:
        return self.swap_total

    def set_memory_metrics(
        self,
        leaf: Path,
        *,
        high: int = 3,
        maximum: int = 0,
        oom: bool = False,
        swap_limit: bool = True,
    ) -> None:
        self.files[leaf].update(
            {
                "memory.current": str(71 * MIB) + "\n",
                "memory.peak": str(93 * MIB) + "\n",
                "memory.events": (
                    f"low 0\nhigh {high}\nmax {maximum}\n"
                    f"oom {int(oom)}\noom_kill {int(oom)}\n"
                    f"oom_group_kill {int(oom)}\n"
                ),
                "memory.pressure": (
                    "some avg10=1.25 avg60=0.50 avg300=0.10 total=900\n"
                    "full avg10=0.25 avg60=0.10 avg300=0.00 total=100\n"
                ),
                "memory.swap.current": str(5 * MIB) + "\n",
                "memory.swap.peak": str(7 * MIB) + "\n",
                "memory.swap.events": (
                    f"high 0\nmax {int(swap_limit)}\nfail {int(swap_limit)}\n"
                ),
            }
        )

    def set_terminal_metrics(self, leaf: Path) -> None:
        self.set_memory_metrics(leaf, maximum=2, oom=True)


def _memory_leaf(system: MemoryFakeCgroupV2System) -> Path:
    leaves = [path for path in system.groups() if path.name.startswith("run-")]
    assert len(leaves) == 1
    return leaves[0]


def test_memory_limits_apply_before_user_code_and_preserve_pressure_evidence(
    tmp_path: Path,
):
    root = tmp_path / "delegated"
    system = MemoryFakeCgroupV2System(root)
    state_dir = tmp_path / "state"
    backend = CgroupV2Backend(root, state_dir=state_dir, system=system)
    running = RunningReferenceBroker(
        state_dir,
        capacities={"jobs": 1, "pressure": 64 * MIB, "ram": 96 * MIB, "swap": 8 * MIB},
        resource_bindings=MEMORY_BINDINGS,
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

Path(os.environ["MEMORY_ENTERED"]).touch()
while not Path(os.environ["MEMORY_RELEASE"]).exists():
    time.sleep(0.01)
""",
            ],
            checkout=str(repository),
            resources={"pressure": 64 * MIB, "ram": 96 * MIB, "swap": 8 * MIB},
            caller_pid=os.getpid(),
            environment={
                **caller_environment(),
                "MEMORY_ENTERED": str(entered),
                "MEMORY_RELEASE": str(release),
            },
        )
        wait_for(entered.exists, "the memory-controlled worker never entered user code")
        leaf = _memory_leaf(system)
        assert system.enabled[root] == {"memory"}
        assert system.enabled[leaf.parent] == {"memory"}
        assert system.files[leaf]["memory.high"] == f"{64 * MIB}\n"
        assert system.files[leaf]["memory.max"] == f"{96 * MIB}\n"
        assert system.files[leaf]["memory.swap.max"] == f"{8 * MIB}\n"
        assert system.files[leaf]["memory.oom.group"] == "1\n"

        system.set_memory_metrics(leaf)

        def measured() -> bool:
            receipt = client.status(run_id)["resource_receipt"]
            codes = {event["code"] for event in receipt["events"]}
            return receipt["peak"] == {
                "pressure": 93 * MIB,
                "ram": 93 * MIB,
                "swap": 7 * MIB,
            } and {
                "memory-high-throttled",
                "memory-pressure",
                "swap-limit-hit",
            } <= codes

        wait_for(measured, "memory pressure and peak evidence was not retained")
        release.touch()
        finished = wait_for(
            lambda: _terminal(client, run_id),
            "the memory-controlled worker did not finish",
        )
        assert finished["status"] == "passed"
        assert finished["failure_reason"] is None
        codes = [event["code"] for event in finished["resource_receipt"]["events"]]
        assert codes.count("memory-high-throttled") == 1
        assert codes.count("memory-pressure") == 1
        assert codes.count("swap-limit-hit") == 1
        assert "memory-oom" not in codes
        assert system.groups() == set()
    finally:
        release.touch()
        running.stop()
        system.close()


@pytest.mark.parametrize(
    ("cancel", "expected_status", "expected_reason"),
    [(False, "failed", "memory-oom"), (True, "cancelled", None)],
)
def test_local_oom_is_a_stable_reason_but_cancellation_wins_terminal_status(
    tmp_path: Path,
    cancel: bool,
    expected_status: str,
    expected_reason: str | None,
):
    root = tmp_path / "delegated"
    system = MemoryFakeCgroupV2System(root)
    state_dir = tmp_path / "state"
    backend = CgroupV2Backend(root, state_dir=state_dir, system=system)
    running = RunningReferenceBroker(
        state_dir,
        capacities={"jobs": 1, "ram": 64 * MIB},
        resource_bindings={"ram": MEMORY_BINDINGS["ram"]},
        resource_backends={"cgroup-v2": backend},
    )
    client = running.start()
    repository = _repository(tmp_path / "repository")
    entered = tmp_path / "entered"
    die = tmp_path / "die"

    try:
        run_id = client.submit(
            [
                sys.executable,
                "-u",
                "-c",
                """
from pathlib import Path
import os
import signal
import time

Path(os.environ["MEMORY_ENTERED"]).touch()
while not Path(os.environ["MEMORY_DIE"]).exists():
    time.sleep(0.01)
os.kill(os.getpid(), signal.SIGKILL)
""",
            ],
            checkout=str(repository),
            resources={"ram": 64 * MIB},
            caller_pid=os.getpid(),
            environment={
                **caller_environment(),
                "MEMORY_DIE": str(die),
                "MEMORY_ENTERED": str(entered),
            },
        )
        wait_for(entered.exists, "the OOM fixture did not start")
        leaf = _memory_leaf(system)
        assert system.files[leaf]["memory.high"] == "max\n"
        assert system.files[leaf]["memory.swap.max"] == "0\n"
        assert system.files[leaf]["memory.oom.group"] == "1\n"
        system.terminal_on_empty.add(leaf)
        if cancel:
            client.cancel(run_id)
        else:
            die.touch()
        finished = wait_for(
            lambda: _terminal(client, run_id),
            "the OOM fixture did not reach a terminal state",
        )
        assert finished["status"] == expected_status
        assert finished["failure_reason"] == expected_reason
        assert "memory-oom" in {
            event["code"] for event in finished["resource_receipt"]["events"]
        }
        assert finished["resource_receipt"]["peak"]["ram"] == 93 * MIB
        assert system.groups() == set()
        assert client.snapshot()["broker_pid"] == os.getpid()
    finally:
        die.touch()
        running.stop()
        system.close()


def test_high_only_policy_keeps_hard_memory_and_swap_unbounded(tmp_path: Path):
    root = tmp_path / "delegated"
    system = MemoryFakeCgroupV2System(root)
    state_dir = tmp_path / "state"
    request = ResourceRequest.build(
        "check-memory-high-defaults",
        "cgroup-v2",
        {"pressure": 32 * MIB},
        {"pressure": MEMORY_BINDINGS["pressure"]},
    )
    backend = CgroupV2Backend(root, state_dir=state_dir, system=system)
    handle = backend.prepare(request)
    leaf = root / str(handle["owner"]) / str(handle["leaf"])

    try:
        assert system.files[leaf]["memory.high"] == f"{32 * MIB}\n"
        assert system.files[leaf]["memory.max"] == "max\n"
        assert system.files[leaf]["memory.swap.max"] == "max\n"
        assert system.files[leaf]["memory.oom.group"] == "0\n"
        backend.finish(request, handle)
        backend.cleanup(request, handle)
        assert system.groups() == set()
    finally:
        if system.groups():
            backend.cleanup(request, handle)
        system.close()


@pytest.mark.parametrize(
    ("controllers", "swap_total", "resources", "expected_stage", "expected_code"),
    [
        (set(), 8 * MIB, {"ram": 64 * MIB}, "probe", "kind-unsupported"),
        ({"memory"}, 0, {"swap": 4 * MIB}, "prepare", "swap-disabled"),
        (
            {"memory"},
            8 * MIB,
            {"pressure": 80 * MIB, "ram": 64 * MIB},
            "prepare",
            "memory-limit-impossible",
        ),
    ],
)
def test_unavailable_or_impossible_memory_policies_refuse_before_user_code(
    tmp_path: Path,
    controllers: set[str],
    swap_total: int,
    resources: dict[str, int],
    expected_stage: str,
    expected_code: str,
):
    root = tmp_path / "delegated"
    system = MemoryFakeCgroupV2System(
        root,
        controllers=controllers,
        swap_total=swap_total,
    )
    state_dir = tmp_path / "state"
    backend = CgroupV2Backend(root, state_dir=state_dir, system=system)
    bindings = {name: MEMORY_BINDINGS[name] for name in resources}
    running = RunningReferenceBroker(
        state_dir,
        capacities={"jobs": 1, **resources},
        resource_bindings=bindings,
        resource_backends={"cgroup-v2": backend},
    )
    client = running.start()
    repository = _repository(tmp_path / "repository")
    marker = tmp_path / "user-code-ran"

    try:
        run_id = client.submit(
            [sys.executable, "-c", "from pathlib import Path; Path(__import__('sys').argv[1]).touch()", str(marker)],
            checkout=str(repository),
            resources=resources,
            caller_pid=os.getpid(),
            environment=caller_environment(),
        )
        finished = wait_for(
            lambda: _terminal(client, run_id),
            "the invalid memory policy did not refuse the run",
        )
        assert finished["status"] == "failed"
        assert finished["exit_status"] == 125
        assert finished["failure_reason"] == "resource-enforcement-failed"
        assert not marker.exists()
        assert {
            (event["stage"], event["code"])
            for event in finished["resource_receipt"]["events"]
            if event["status"] == "failed"
        } == {(expected_stage, expected_code)}
        assert system.groups() == set()
    finally:
        running.stop()
        system.close()


def test_replacement_backend_reads_final_memory_counters_from_owned_leaf(
    tmp_path: Path,
):
    root = tmp_path / "delegated"
    system = MemoryFakeCgroupV2System(root)
    state_dir = tmp_path / "state"
    request = ResourceRequest.build(
        "check-memory-restart",
        "cgroup-v2",
        {"pressure": 64 * MIB, "ram": 96 * MIB, "swap": 8 * MIB},
        MEMORY_BINDINGS,
    )
    original = CgroupV2Backend(root, state_dir=state_dir, system=system)
    handle = original.prepare(request)
    leaf = root / str(handle["owner"]) / str(handle["leaf"])
    system.set_memory_metrics(leaf, maximum=2, oom=True)

    try:
        replacement = CgroupV2Backend(root, state_dir=state_dir, system=system)
        measured = replacement.finish(request, handle)
        assert isinstance(measured, ResourceMeasurement)
        assert measured.peak == {
            "pressure": 93 * MIB,
            "ram": 93 * MIB,
            "swap": 7 * MIB,
        }
        assert {observation.code for observation in measured.observations} == {
            "memory-high-throttled",
            "memory-max-hit",
            "memory-oom",
            "memory-pressure",
            "swap-limit-hit",
        }
        replacement.cleanup(request, handle)
        assert system.groups() == set()
    finally:
        if system.groups():
            original.cleanup(request, handle)
        system.close()


@pytest.mark.skipif(
    not os.environ.get("AGCOORD_TEST_CGROUP_ROOT"),
    reason="set AGCOORD_TEST_CGROUP_ROOT to an exclusive writable delegation",
)
def test_real_hard_oom_is_local_and_high_limit_is_pressure_not_oom(tmp_path: Path):
    root = Path(os.environ["AGCOORD_TEST_CGROUP_ROOT"]).resolve(strict=True)
    state_dir = tmp_path / "state"
    backend = CgroupV2Backend(root, state_dir=state_dir)
    capability = backend.probe()
    assert capability["available"] is True
    assert {"memory", "memory-high", "swap"} <= set(capability["kinds"])
    running = RunningReferenceBroker(
        state_dir,
        capacities={"jobs": 2, "pressure": 32 * MIB, "ram": 128 * MIB},
        resource_bindings={
            "pressure": MEMORY_BINDINGS["pressure"],
            "ram": MEMORY_BINDINGS["ram"],
        },
        resource_backends={"cgroup-v2": backend},
    )
    client = running.start()
    repository = _repository(tmp_path / "repository")
    sibling_entered = tmp_path / "sibling-entered"
    sibling_release = tmp_path / "sibling-release"

    try:
        sibling_id = client.submit(
            [
                sys.executable,
                "-u",
                "-c",
                """
from pathlib import Path
import os
import time

payload = bytearray(8 * 1024 * 1024)
for offset in range(0, len(payload), 4096):
    payload[offset] = 1
Path(os.environ["SIBLING_ENTERED"]).touch()
while not Path(os.environ["SIBLING_RELEASE"]).exists():
    time.sleep(0.01)
""",
            ],
            checkout=str(repository),
            resources={"ram": 64 * MIB},
            caller_pid=os.getpid(),
            environment={
                **caller_environment(),
                "SIBLING_ENTERED": str(sibling_entered),
                "SIBLING_RELEASE": str(sibling_release),
            },
        )
        wait_for(sibling_entered.exists, "the protected sibling did not start")
        oom_id = client.submit(
            [
                sys.executable,
                "-u",
                "-c",
                """
blocks = []
while True:
    block = bytearray(4 * 1024 * 1024)
    for offset in range(0, len(block), 4096):
        block[offset] = 1
    blocks.append(block)
""",
            ],
            checkout=str(repository),
            resources={"ram": 64 * MIB},
            caller_pid=os.getpid(),
            environment=caller_environment(),
        )
        oom = wait_for(
            lambda: _terminal(client, oom_id),
            "the real allocation never reached its hard memory limit",
            timeout=30,
        )
        assert oom["status"] == "failed"
        assert oom["failure_reason"] == "memory-oom"
        assert "memory-oom" in {
            event["code"] for event in oom["resource_receipt"]["events"]
        }
        assert client.status(sibling_id)["status"] == "running"
        assert client.snapshot()["broker_pid"] == os.getpid()
        sibling_release.touch()
        assert wait_for(
            lambda: _terminal(client, sibling_id),
            "the sibling did not finish after the local OOM",
        )["status"] == "passed"

        pressure_id = client.submit(
            [
                sys.executable,
                "-u",
                "-c",
                """
payload = bytearray(48 * 1024 * 1024)
for offset in range(0, len(payload), 4096):
    payload[offset] = 1
""",
            ],
            checkout=str(repository),
            resources={"pressure": 32 * MIB, "ram": 128 * MIB},
            caller_pid=os.getpid(),
            environment=caller_environment(),
        )
        pressured = wait_for(
            lambda: _terminal(client, pressure_id),
            "the real high-boundary run did not finish",
            timeout=30,
        )
        assert pressured["status"] == "passed"
        pressure_codes = {
            event["code"] for event in pressured["resource_receipt"]["events"]
        }
        assert {"memory-high-throttled", "memory-pressure"} <= pressure_codes
        assert "memory-oom" not in pressure_codes
        assert not (root / backend.owner_name).exists()
    finally:
        sibling_release.touch()
        running.stop()
