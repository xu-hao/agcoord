"""Behavioral bounded-tmpfs contracts for the cgroup namespace backend."""

from __future__ import annotations

import json
import os
from pathlib import Path
import sys

import pytest

from agcoord import queue as queue_module
from agcoord.cgroup import CgroupV2Backend
from agcoord.resources import ResourceMeasurement, ResourceRequest

from conftest import RunningCoordinator, caller_environment, wait_for
from test_cgroup import _repository, _terminal
from test_cgroup_memory import MIB, MEMORY_BINDINGS, MemoryFakeCgroupV2System


TMPFS_BINDINGS = {
    "ram": MEMORY_BINDINGS["ram"],
    "scratch": {
        "backend": "cgroup-v2",
        "kind": "tmpfs",
        "mode": "required",
        "unit": "bytes",
    },
    "scratch_inodes": {
        "backend": "cgroup-v2",
        "kind": "inodes",
        "mode": "required",
        "unit": "inodes",
    },
}

_FAKE_TMPFS_LAUNCHER = r"""
import json
import os
from pathlib import Path
import sys

release_fd = int(sys.argv[1])
setup_fd = int(sys.argv[2])
if os.read(release_fd, 1) != b"1":
    raise SystemExit(125)
spec = json.loads(os.environ.pop("_AGCOORD_TMPFS_SETUP"))
report = {
    "version": 1,
    "token": spec["token"],
    "peak_bytes": 6 * 1024 * 1024,
    "peak_inodes": 7,
    "terminal_bytes": 4 * 1024 * 1024,
    "terminal_inodes": 5,
    "byte_limit_hit": True,
    "inode_limit_hit": True,
}
Path(spec["report"]).write_text(json.dumps(report), encoding="ascii")
os.write(setup_fd, b"ok")
os.close(setup_fd)
if os.read(release_fd, 1) != b"1":
    raise SystemExit(125)
os.close(release_fd)
os.execvpe(sys.argv[3], sys.argv[3:], os.environ)
"""

_FAKE_REFUSING_TMPFS_LAUNCHER = r"""
import os
import sys

release_fd = int(sys.argv[1])
setup_fd = int(sys.argv[2])
if os.read(release_fd, 1) != b"1":
    raise SystemExit(125)
os.environ.pop("_AGCOORD_TMPFS_SETUP")
os.write(setup_fd, b"tmpfs-mount-unavailable")
os.close(setup_fd)
continue_run = os.read(release_fd, 1) == b"1"
os.close(release_fd)
if not continue_run:
    raise SystemExit(125)
os.execvpe(sys.argv[3], sys.argv[3:], os.environ)
"""


def test_tmpfs_policy_is_prepared_inside_the_hard_memory_envelope(tmp_path: Path):
    root = tmp_path / "delegated"
    system = MemoryFakeCgroupV2System(root)
    state_dir = tmp_path / "state"
    backend = CgroupV2Backend(root, state_dir=state_dir, system=system)
    request = ResourceRequest.build(
        "check-tmpfs-policy",
        "cgroup-v2",
        {"ram": 64 * MIB, "scratch": 16 * MIB, "scratch_inodes": 128},
        TMPFS_BINDINGS,
    )

    try:
        handle = backend.prepare(request)
        target = tmp_path / "worker-tmp"
        target.mkdir(mode=0o700)
        setup = backend.tmpfs_setup(request, handle, target)
        assert setup is not None
        assert setup["size"] == 16 * MIB
        assert setup["inodes"] == 128
        assert setup["target"] == str(target)
        leaf = root / str(handle["owner"]) / str(handle["leaf"])
        assert system.files[leaf]["memory.max"] == f"{64 * MIB}\n"
        assert system.files[leaf]["memory.swap.max"] == "0\n"
        backend.finish(request, handle)
        backend.cleanup(request, handle)
        assert system.groups() == set()
    finally:
        system.close()


@pytest.mark.parametrize(
    ("resources", "expected_code"),
    [
        ({"scratch": 16 * MIB, "scratch_inodes": 128}, "tmpfs-memory-required"),
        ({"ram": 64 * MIB, "scratch": 16 * MIB}, "tmpfs-policy-incomplete"),
        (
            {"ram": 8 * MIB, "scratch": 16 * MIB, "scratch_inodes": 128},
            "tmpfs-memory-impossible",
        ),
    ],
)
def test_invalid_tmpfs_policy_refuses_before_user_code(
    tmp_path: Path,
    resources: dict[str, int],
    expected_code: str,
):
    root = tmp_path / "delegated"
    system = MemoryFakeCgroupV2System(root)
    state_dir = tmp_path / "state"
    backend = CgroupV2Backend(root, state_dir=state_dir, system=system)
    bindings = {name: TMPFS_BINDINGS[name] for name in resources}
    running = RunningCoordinator(
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
            "the invalid tmpfs policy did not reach a terminal state",
        )
        assert finished["status"] == "failed"
        assert finished["exit_status"] == 125
        assert finished["failure_reason"] == "resource-enforcement-failed"
        assert not marker.exists()
        assert expected_code in {
            event["code"] for event in finished["resource_receipt"]["events"]
        }
        assert system.groups() == set()
    finally:
        running.stop()
        system.close()


def test_successful_launcher_setup_is_durable_before_user_code_and_reports_usage(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr(queue_module, "_WORKER_LAUNCHER", _FAKE_TMPFS_LAUNCHER)
    root = tmp_path / "delegated"
    system = MemoryFakeCgroupV2System(root)
    state_dir = tmp_path / "state"
    backend = CgroupV2Backend(root, state_dir=state_dir, system=system)
    running = RunningCoordinator(
        state_dir,
        capacities={
            "jobs": 1,
            "ram": 64 * MIB,
            "scratch": 16 * MIB,
            "scratch_inodes": 128,
        },
        resource_bindings=TMPFS_BINDINGS,
        resource_backends={"cgroup-v2": backend},
    )
    client = running.start()
    repository = _repository(tmp_path / "repository")
    report = tmp_path / "user-report.json"

    try:
        run_id = client.submit(
            [
                sys.executable,
                "-c",
                """
import json
import os
from pathlib import Path

target = Path(os.environ["TMPDIR"])
Path(__import__("sys").argv[1]).write_text(json.dumps({
    "target": str(target),
    "variables": [os.environ[name] for name in ("TMPDIR", "TMP", "TEMP")],
    "setup_hidden": "_AGCOORD_TMPFS_SETUP" not in os.environ,
}), encoding="utf-8")
""",
                str(report),
            ],
            checkout=str(repository),
            resources={
                "ram": 64 * MIB,
                "scratch": 16 * MIB,
                "scratch_inodes": 128,
            },
            caller_pid=os.getpid(),
            environment=caller_environment(),
        )
        finished = wait_for(
            lambda: _terminal(client, run_id),
            "the acknowledged tmpfs fixture did not finish",
        )
        assert finished["status"] == "passed"
        assert finished["resource_receipt"]["applied"] == {
            "ram": 64 * MIB,
            "scratch": 16 * MIB,
            "scratch_inodes": 128,
        }
        assert finished["resource_receipt"]["peak"] == {
            "ram": 0,
            "scratch": 6 * MIB,
            "scratch_inodes": 7,
        }
        codes = {event["code"] for event in finished["resource_receipt"]["events"]}
        assert {"tmpfs-mounted", "tmpfs-byte-limit-hit", "tmpfs-inode-limit-hit"} <= codes
        observed = json.loads(report.read_text(encoding="utf-8"))
        assert len(set(observed["variables"])) == 1
        assert observed["variables"] == [observed["target"]] * 3
        assert observed["setup_hidden"] is True
        assert not Path(observed["target"]).exists()
        assert not list((state_dir / "cgroup-v2").glob("tmpfs-*.json"))
        assert system.groups() == set()
    finally:
        running.stop()
        system.close()


@pytest.mark.parametrize(
    ("mode", "expected_status", "expected_exit", "ran"),
    [
        ("required", "failed", 125, False),
        ("best-effort", "passed", 0, True),
    ],
)
def test_mount_capability_failure_obeys_required_or_disk_fallback_mode(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
    expected_status: str,
    expected_exit: int,
    ran: bool,
):
    monkeypatch.setattr(
        queue_module,
        "_WORKER_LAUNCHER",
        _FAKE_REFUSING_TMPFS_LAUNCHER,
    )
    root = tmp_path / "delegated"
    system = MemoryFakeCgroupV2System(root)
    state_dir = tmp_path / "state"
    backend = CgroupV2Backend(root, state_dir=state_dir, system=system)
    bindings = {
        name: (
            {**binding, "mode": mode}
            if name in {"scratch", "scratch_inodes"}
            else binding
        )
        for name, binding in TMPFS_BINDINGS.items()
    }
    running = RunningCoordinator(
        state_dir,
        capacities={
            "jobs": 1,
            "ram": 64 * MIB,
            "scratch": 16 * MIB,
            "scratch_inodes": 128,
        },
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
            resources={
                "ram": 64 * MIB,
                "scratch": 16 * MIB,
                "scratch_inodes": 128,
            },
            caller_pid=os.getpid(),
            environment=caller_environment(),
        )
        finished = wait_for(
            lambda: _terminal(client, run_id),
            "the refused tmpfs setup did not finish",
        )
        assert finished["status"] == expected_status
        assert finished["exit_status"] == expected_exit
        assert marker.exists() is ran
        tmpfs_events = [
            event
            for event in finished["resource_receipt"]["events"]
            if event["resource"] in {"scratch", "scratch_inodes"}
            and event["code"] == "tmpfs-mount-unavailable"
        ]
        assert {event["status"] for event in tmpfs_events} == {
            "failed" if mode == "required" else "unapplied"
        }
        assert finished["resource_receipt"]["applied"] == {"ram": 64 * MIB}
        assert system.groups() == set()
    finally:
        running.stop()
        system.close()


def test_replacement_backend_retains_tmpfs_report_until_owned_cleanup(tmp_path: Path):
    root = tmp_path / "delegated"
    system = MemoryFakeCgroupV2System(root)
    state_dir = tmp_path / "state"
    request = ResourceRequest.build(
        "check-tmpfs-restart",
        "cgroup-v2",
        {"ram": 64 * MIB, "scratch": 16 * MIB, "scratch_inodes": 128},
        TMPFS_BINDINGS,
    )
    original = CgroupV2Backend(root, state_dir=state_dir, system=system)
    handle = original.prepare(request)
    target = tmp_path / "worker-tmp"
    target.mkdir(mode=0o700)
    setup = original.tmpfs_setup(request, handle, target)
    assert setup is not None
    Path(str(setup["report"])).write_text(
        json.dumps(
            {
                "version": 1,
                "token": setup["token"],
                "peak_bytes": 9 * MIB,
                "peak_inodes": 11,
                "terminal_bytes": 8 * MIB,
                "terminal_inodes": 10,
                "byte_limit_hit": False,
                "inode_limit_hit": True,
            }
        ),
        encoding="utf-8",
    )

    try:
        replacement = CgroupV2Backend(root, state_dir=state_dir, system=system)
        measured = replacement.finish(request, handle)
        assert isinstance(measured, ResourceMeasurement)
        assert measured.peak == {
            "ram": 0,
            "scratch": 9 * MIB,
            "scratch_inodes": 11,
        }
        assert {item.code for item in measured.observations} == {
            "tmpfs-inode-limit-hit"
        }
        replacement.cleanup(request, handle)
        assert not Path(str(setup["report"])).exists()
        assert system.groups() == set()
    finally:
        if system.groups():
            original.cleanup(request, handle)
        system.close()


def test_cancellation_retains_last_tmpfs_report_and_removes_owned_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr(queue_module, "_WORKER_LAUNCHER", _FAKE_TMPFS_LAUNCHER)
    root = tmp_path / "delegated"
    system = MemoryFakeCgroupV2System(root)
    state_dir = tmp_path / "state"
    backend = CgroupV2Backend(root, state_dir=state_dir, system=system)
    running = RunningCoordinator(
        state_dir,
        capacities={
            "jobs": 1,
            "ram": 64 * MIB,
            "scratch": 16 * MIB,
            "scratch_inodes": 128,
        },
        resource_bindings=TMPFS_BINDINGS,
        resource_backends={"cgroup-v2": backend},
    )
    client = running.start()
    repository = _repository(tmp_path / "repository")
    entered = tmp_path / "entered"

    try:
        run_id = client.submit(
            [
                sys.executable,
                "-u",
                "-c",
                """
from pathlib import Path
import os
import sys
import time

Path(sys.argv[1]).write_text(os.environ["TMPDIR"], encoding="utf-8")
while True:
    time.sleep(1)
""",
                str(entered),
            ],
            checkout=str(repository),
            resources={
                "ram": 64 * MIB,
                "scratch": 16 * MIB,
                "scratch_inodes": 128,
            },
            caller_pid=os.getpid(),
            environment=caller_environment(),
        )
        wait_for(entered.exists, "the cancellable tmpfs fixture did not start")
        target = Path(entered.read_text(encoding="utf-8"))
        client.cancel(run_id)
        finished = wait_for(
            lambda: _terminal(client, run_id),
            "the cancellable tmpfs fixture did not finish",
        )
        assert finished["status"] == "cancelled"
        assert finished["failure_reason"] is None
        assert finished["resource_receipt"]["peak"]["scratch"] == 6 * MIB
        assert finished["resource_receipt"]["peak"]["scratch_inodes"] == 7
        assert not target.exists()
        assert not list((state_dir / "cgroup-v2").glob("tmpfs-*.json"))
        assert system.groups() == set()
    finally:
        running.stop()
        system.close()


@pytest.mark.skipif(
    not os.environ.get("AGCOORD_TEST_CGROUP_ROOT"),
    reason="set AGCOORD_TEST_CGROUP_ROOT to an exclusive writable delegation",
)
def test_real_tmpfs_enforces_bytes_inodes_and_the_shared_memory_envelope(
    tmp_path: Path,
):
    root = Path(os.environ["AGCOORD_TEST_CGROUP_ROOT"]).resolve(strict=True)
    state_dir = tmp_path / "state"
    backend = CgroupV2Backend(root, state_dir=state_dir)
    capability = backend.probe()
    assert capability["available"] is True
    assert {"inodes", "memory", "tmpfs"} <= set(capability["kinds"])
    running = RunningCoordinator(
        state_dir,
        capacities={
            "jobs": 1,
            "ram": 128 * MIB,
            "scratch": 64 * MIB,
            "scratch_inodes": 2048,
        },
        resource_bindings=TMPFS_BINDINGS,
        resource_backends={"cgroup-v2": backend},
    )
    client = running.start()
    repository = _repository(tmp_path / "repository")
    report = tmp_path / "limits.json"

    try:
        limited_id = client.submit(
            [
                sys.executable,
                "-u",
                "-c",
                """
import errno
import json
import os
from pathlib import Path
import sys
import time

target = Path(os.environ["TMPDIR"])
mount = None
for line in Path("/proc/self/mountinfo").read_text(encoding="utf-8").splitlines():
    left, separator, right = line.partition(" - ")
    if separator and left.split()[4] == str(target):
        mount = {
            "filesystem": right.split()[0],
            "options": sorted(set(left.split()[5].split(",") + right.split()[2].split(","))),
        }
        break
if mount is None:
    raise AssertionError("TMPDIR is not a mount")

created = []
inode_exhausted = False
for index in range(256):
    try:
        path = target / f"inode-{index}"
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
descriptor = os.open(target / "payload", os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
try:
    block = b"x" * (1024 * 1024)
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
    "byte_exhausted": byte_exhausted,
    "created": len(created),
    "inode_exhausted": inode_exhausted,
    "mount": mount,
    "target": str(target),
    "written": written,
}), encoding="utf-8")
""",
                str(report),
            ],
            checkout=str(repository),
            resources={
                "ram": 128 * MIB,
                "scratch": 16 * MIB,
                "scratch_inodes": 32,
            },
            caller_pid=os.getpid(),
            environment=caller_environment(),
        )
        limited = wait_for(
            lambda: _terminal(client, limited_id),
            "the real bounded tmpfs run did not finish",
            timeout=30,
        )
        assert limited["status"] == "passed"
        observed = json.loads(report.read_text(encoding="utf-8"))
        assert observed["byte_exhausted"] is True
        assert observed["inode_exhausted"] is True
        assert 0 < observed["created"] < 32
        assert 0 < observed["written"] <= 16 * MIB
        assert observed["mount"]["filesystem"] == "tmpfs"
        assert {"nodev", "noexec", "nosuid"} <= set(
            observed["mount"]["options"]
        )
        assert not Path(observed["target"]).exists()
        codes = {event["code"] for event in limited["resource_receipt"]["events"]}
        assert {
            "tmpfs-byte-limit-hit",
            "tmpfs-inode-limit-hit",
            "tmpfs-mounted",
        } <= codes
        assert limited["resource_receipt"]["peak"]["ram"] >= limited[
            "resource_receipt"
        ]["peak"]["scratch"]

        oom_id = client.submit(
            [
                sys.executable,
                "-u",
                "-c",
                """
import os
from pathlib import Path

descriptor = os.open(Path(os.environ["TMPDIR"]) / "oom", os.O_WRONLY | os.O_CREAT, 0o600)
block = b"x" * (1024 * 1024)
while True:
    os.write(descriptor, block)
""",
            ],
            checkout=str(repository),
            resources={
                "ram": 64 * MIB,
                "scratch": 64 * MIB,
                "scratch_inodes": 2048,
            },
            caller_pid=os.getpid(),
            environment=caller_environment(),
        )
        oom = wait_for(
            lambda: _terminal(client, oom_id),
            "tmpfs writes did not reach the shared hard memory envelope",
            timeout=30,
        )
        assert oom["status"] == "failed"
        assert oom["failure_reason"] == "memory-oom"
        assert "memory-oom" in {
            event["code"] for event in oom["resource_receipt"]["events"]
        }
        assert not (root / backend.owner_name).exists()
    finally:
        running.stop()


@pytest.mark.skipif(
    not os.environ.get("AGCOORD_TEST_CGROUP_ROOT"),
    reason="set AGCOORD_TEST_CGROUP_ROOT to an exclusive writable delegation",
)
def test_real_parallel_tmpfs_mounts_are_private_and_cancel_cleanly(tmp_path: Path):
    root = Path(os.environ["AGCOORD_TEST_CGROUP_ROOT"]).resolve(strict=True)
    state_dir = tmp_path / "state"
    backend = CgroupV2Backend(root, state_dir=state_dir)
    running = RunningCoordinator(
        state_dir,
        capacities={
            "jobs": 2,
            "ram": 128 * MIB,
            "scratch": 32 * MIB,
            "scratch_inodes": 256,
        },
        resource_bindings=TMPFS_BINDINGS,
        resource_backends={"cgroup-v2": backend},
    )
    client = running.start()
    repository = _repository(tmp_path / "repository")

    def submit(name: str) -> tuple[str, Path, Path, Path, Path]:
        announced = tmp_path / f"{name}-announced.json"
        peer = tmp_path / f"{name}-peer"
        checked = tmp_path / f"{name}-checked.json"
        release = tmp_path / f"{name}-release"
        run_id = client.submit(
            [
                sys.executable,
                "-u",
                "-c",
                """
import json
import os
from pathlib import Path
import sys
import time

announced, peer, checked, release, secret = map(Path, sys.argv[1:])
target = Path(os.environ["TMPDIR"])
(target / "secret").write_text(str(secret), encoding="utf-8")
announced.write_text(json.dumps({"target": str(target)}), encoding="utf-8")
while not peer.exists():
    time.sleep(0.01)
other = Path(peer.read_text(encoding="utf-8"))
checked.write_text(json.dumps({
    "other_secret_visible": (other / "secret").exists(),
    "own_secret": (target / "secret").read_text(encoding="utf-8"),
}), encoding="utf-8")
while not release.exists():
    time.sleep(0.01)
""",
                str(announced),
                str(peer),
                str(checked),
                str(release),
                name,
            ],
            checkout=str(repository),
            resources={
                "ram": 64 * MIB,
                "scratch": 16 * MIB,
                "scratch_inodes": 128,
            },
            caller_pid=os.getpid(),
            environment=caller_environment(),
        )
        return run_id, announced, peer, checked, release

    first = submit("first")
    second = submit("second")
    try:
        wait_for(first[1].exists, "the first private tmpfs did not start")
        wait_for(second[1].exists, "the second private tmpfs did not start")
        first_target = Path(json.loads(first[1].read_text(encoding="utf-8"))["target"])
        second_target = Path(json.loads(second[1].read_text(encoding="utf-8"))["target"])
        assert first_target != second_target
        first[2].write_text(str(second_target), encoding="utf-8")
        second[2].write_text(str(first_target), encoding="utf-8")
        wait_for(first[3].exists, "the first run did not check mount privacy")
        wait_for(second[3].exists, "the second run did not check mount privacy")
        assert json.loads(first[3].read_text(encoding="utf-8")) == {
            "other_secret_visible": False,
            "own_secret": "first",
        }
        assert json.loads(second[3].read_text(encoding="utf-8")) == {
            "other_secret_visible": False,
            "own_secret": "second",
        }

        client.cancel(first[0])
        second[4].touch()
        assert wait_for(
            lambda: _terminal(client, first[0]),
            "the cancelled private tmpfs did not finish",
        )["status"] == "cancelled"
        assert wait_for(
            lambda: _terminal(client, second[0]),
            "the normal private tmpfs did not finish",
        )["status"] == "passed"
        assert not first_target.exists()
        assert not second_target.exists()
        assert not (root / backend.owner_name).exists()
    finally:
        first[4].touch()
        second[4].touch()
        running.stop()
