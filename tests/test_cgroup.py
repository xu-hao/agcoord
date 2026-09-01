"""Behavioral contract for delegated cgroup v2 run lifecycle ownership."""

from __future__ import annotations

import errno
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import threading
import time
from uuid import uuid4

import pytest

from agcoord import cgroup as cgroup_module
from agcoord.cgroup import (
    CGROUP_ISOLATE_ENV,
    CgroupIdentity,
    CgroupOwnershipError,
    CgroupProbe,
    CgroupV2Backend,
    LinuxCgroupV2System,
)
from agcoord.queue import CoordinatorClient
from agcoord.resources import ResourceRequest

from conftest import RunningCoordinator, caller_environment, wait_for


LIFECYCLE_BINDING = {
    "cgroup-slot": {
        "backend": "cgroup-v2",
        "kind": "generic",
        "mode": "required",
        "unit": "admission-unit",
    }
}


def test_namespace_cgroup_mount_falls_back_to_its_attached_leaf_on_ebusy(
    monkeypatch: pytest.MonkeyPatch,
):
    calls: list[tuple[str, tuple[object, ...]]] = []

    def call_libc(name: str, *arguments: object) -> None:
        calls.append((name, arguments))
        if len(calls) == 1:
            raise OSError(errno.EBUSY, os.strerror(errno.EBUSY))

    monkeypatch.setattr(cgroup_module, "_call_libc", call_libc)
    mount = cgroup_module.CgroupMount(
        path=Path("/sys/fs/cgroup"),
        root=Path("/user.slice"),
        options=frozenset({"rw", "nsdelegate"}),
    )

    cgroup_module._mount_isolated_cgroup_view(
        mount,
        Path("/user.slice/runner.scope/agcoord-leaf"),
    )

    assert calls == [
        (
            "mount",
            (
                b"none",
                b"/sys/fs/cgroup",
                b"cgroup2",
                cgroup_module._MS_NOSUID
                | cgroup_module._MS_NODEV
                | cgroup_module._MS_NOEXEC,
                None,
            ),
        ),
        (
            "mount",
            (
                b"/sys/fs/cgroup/runner.scope/agcoord-leaf",
                b"/sys/fs/cgroup",
                None,
                cgroup_module._MS_BIND,
                None,
            ),
        ),
    ]


def _process_identity(pid: int) -> tuple[int, str] | None:
    try:
        raw = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
        fields = raw[raw.rfind(")") + 2 :].split()
        if fields[0] == "Z":
            return None
        return int(fields[1]), fields[19]
    except (FileNotFoundError, IndexError, ProcessLookupError, ValueError):
        return None


class FakeCgroupV2System:
    """Deterministic cgroup kernel model backed by real subprocess identities."""

    def __init__(
        self,
        root: Path,
        *,
        reason: str | None = None,
        attach_marker: Path | None = None,
    ) -> None:
        self.root = root
        self.root.mkdir()
        self.reason = reason
        self.attach_marker = attach_marker
        self._members: dict[Path, dict[int, str]] = {}
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._monitor = threading.Thread(target=self._monitor_members, daemon=True)
        self._monitor.start()

    def close(self) -> None:
        self._stop.set()
        self._monitor.join(timeout=5)
        assert not self._monitor.is_alive()

    def probe(self, root: Path) -> CgroupProbe:
        assert root == self.root
        if self.reason is not None:
            return CgroupProbe(
                available=False,
                reason=self.reason,
                controllers=frozenset(),
            )
        return CgroupProbe(
            available=True,
            reason=None,
            controllers=frozenset({"cpu", "io", "memory", "pids"}),
        )

    def identity(self, path: Path) -> CgroupIdentity | None:
        try:
            details = path.lstat()
        except FileNotFoundError:
            return None
        if path.is_symlink() or not path.is_dir():
            raise RuntimeError("fake cgroup path is not a real directory")
        return CgroupIdentity(device=details.st_dev, inode=details.st_ino)

    def create_group(self, parent: Path, name: str) -> CgroupIdentity:
        path = parent / name
        path.mkdir()
        with self._lock:
            self._members[path] = {}
        identity = self.identity(path)
        assert identity is not None
        return identity

    def attach(self, path: Path, pid: int) -> None:
        identity = _process_identity(pid)
        if identity is None:
            raise ProcessLookupError(pid)
        with self._lock:
            self._members[path][pid] = identity[1]
            self._discover_descendants_locked()
        if self.attach_marker is not None:
            self.attach_marker.touch()

    def members(self, path: Path) -> set[int]:
        with self._lock:
            self._discover_descendants_locked()
            return set(self._members.get(path, {}))

    def populated(self, path: Path) -> bool:
        return bool(self.members(path))

    def kill(self, path: Path) -> None:
        deadline = time.monotonic() + 5
        while True:
            members = self.members(path)
            if not members:
                return
            for pid in members:
                try:
                    os.kill(pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            if time.monotonic() >= deadline:
                raise TimeoutError("fake cgroup did not become empty")
            time.sleep(0.01)

    def remove_group(self, path: Path) -> None:
        if self.populated(path):
            raise OSError(errno.EBUSY, "fake cgroup is populated", str(path))
        path.rmdir()
        with self._lock:
            self._members.pop(path, None)

    def groups(self) -> set[Path]:
        with self._lock:
            return set(self._members)

    def replace_with_foreign_group(self, path: Path) -> Path:
        retained = path.with_name(f"{path.name}-original")
        path.rename(retained)
        with self._lock:
            self._members[retained] = self._members.pop(path)
        path.mkdir()
        with self._lock:
            self._members[path] = {}
        return retained

    def _monitor_members(self) -> None:
        while not self._stop.wait(0.005):
            with self._lock:
                self._discover_descendants_locked()

    def _discover_descendants_locked(self) -> None:
        processes: dict[int, tuple[int, str]] = {}
        for entry in Path("/proc").iterdir():
            if not entry.name.isdigit():
                continue
            identity = _process_identity(int(entry.name))
            if identity is not None:
                processes[int(entry.name)] = identity
        for path, recorded in self._members.items():
            live = {
                pid: token
                for pid, token in recorded.items()
                if pid in processes and processes[pid][1] == token
            }
            changed = True
            while changed:
                changed = False
                for pid, (parent, token) in processes.items():
                    if pid not in live and parent in live:
                        live[pid] = token
                        changed = True
            self._members[path] = live


class PersistentFakeCgroupV2System:
    """File-backed fake shared by brokers on opposite sides of a hard restart."""

    _MEMBERS = ".members.json"

    def __init__(self, root: Path) -> None:
        self.root = root
        self.root.mkdir(exist_ok=True)

    def probe(self, root: Path) -> CgroupProbe:
        assert root == self.root
        return CgroupProbe(
            available=True,
            reason=None,
            controllers=frozenset({"cpu", "io", "memory", "pids"}),
        )

    def identity(self, path: Path) -> CgroupIdentity | None:
        try:
            details = path.lstat()
        except FileNotFoundError:
            return None
        if path.is_symlink() or not path.is_dir():
            raise RuntimeError("persistent fake cgroup path is invalid")
        return CgroupIdentity(details.st_dev, details.st_ino)

    def create_group(self, parent: Path, name: str) -> CgroupIdentity:
        path = parent / name
        path.mkdir()
        self._write_members(path, {})
        identity = self.identity(path)
        assert identity is not None
        return identity

    def _member_path(self, path: Path) -> Path:
        return path / self._MEMBERS

    def _read_members(self, path: Path) -> dict[int, str]:
        try:
            raw = json.loads(self._member_path(path).read_text(encoding="utf-8"))
        except FileNotFoundError:
            return {}
        return {int(pid): token for pid, token in raw.items()}

    def _write_members(self, path: Path, members: dict[int, str]) -> None:
        target = self._member_path(path)
        temporary = target.with_suffix(f".{uuid4().hex}.tmp")
        temporary.write_text(
            json.dumps({str(pid): token for pid, token in members.items()}),
            encoding="utf-8",
        )
        temporary.replace(target)

    def attach(self, path: Path, pid: int) -> None:
        identity = _process_identity(pid)
        if identity is None:
            raise ProcessLookupError(pid)
        self._write_members(path, {pid: identity[1]})

    def members(self, path: Path) -> set[int]:
        live = {
            pid: token
            for pid, token in self._read_members(path).items()
            if (identity := _process_identity(pid)) is not None and identity[1] == token
        }
        self._write_members(path, live)
        return set(live)

    def populated(self, path: Path) -> bool:
        return bool(self.members(path))

    def kill(self, path: Path) -> None:
        for pid in self.members(path):
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass

    def remove_group(self, path: Path) -> None:
        if self.populated(path):
            raise OSError(errno.EBUSY, "persistent fake cgroup is populated", str(path))
        self._member_path(path).unlink(missing_ok=True)
        path.rmdir()

    def groups(self) -> set[Path]:
        return {
            path
            for path in self.root.rglob("*")
            if path.is_dir()
        }


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
    _git(path, "config", "user.name", "Cgroup Tests")
    _git(path, "config", "user.email", "cgroup@example.invalid")
    (path / "tracked.txt").write_text("tracked\n", encoding="utf-8")
    _git(path, "add", "tracked.txt")
    _git(path, "commit", "-m", "initial")
    return path


def _terminal(client: CoordinatorClient, run_id: str) -> dict[str, object] | None:
    row = client.status(run_id)
    return row if row["status"] not in {"queued", "running"} else None


def _request(run_id: str) -> ResourceRequest:
    return ResourceRequest.build(
        run_id,
        "cgroup-v2",
        {"cgroup-slot": 1},
        LIFECYCLE_BINDING,
    )


def test_blocked_launcher_joins_owned_leaf_before_command_and_descendant_start(
    tmp_path: Path,
):
    root = tmp_path / "delegated"
    marker = tmp_path / "attached"
    system = FakeCgroupV2System(root, attach_marker=marker)
    state_dir = tmp_path / "state"
    backend = CgroupV2Backend(
        root,
        state_dir=state_dir,
        system=system,
        empty_timeout=2,
    )
    running = RunningCoordinator(
        state_dir,
        capacities={"jobs": 1, "cgroup-slot": 1},
        resource_bindings=LIFECYCLE_BINDING,
        resource_backends={"cgroup-v2": backend},
    )
    client = running.start()
    repository = _repository(tmp_path / "repository")
    report = tmp_path / "pids.json"
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
import subprocess
import sys
import time

if not Path(os.environ["CGROUP_ATTACHED"]).exists():
    raise AssertionError("user code ran before cgroup attachment")
child = subprocess.Popen([
    sys.executable,
    "-u",
    "-c",
    "import time; time.sleep(30)",
])
Path(os.environ["CGROUP_REPORT"]).write_text(
    json.dumps({
        "command": os.getpid(),
        "child": child.pid,
        "isolation_flag": os.environ.get("_AGCOORD_CGROUP_ISOLATE"),
    }),
    encoding="utf-8",
)
while not Path(os.environ["CGROUP_RELEASE"]).exists():
    time.sleep(0.01)
child.terminate()
child.wait()
""",
            ],
            checkout=str(repository),
            resources={"cgroup-slot": 1},
            caller_pid=os.getpid(),
            environment={
                **caller_environment(),
                CGROUP_ISOLATE_ENV: "caller-forged",
                "CGROUP_ATTACHED": str(marker),
                "CGROUP_REPORT": str(report),
                "CGROUP_RELEASE": str(release),
            },
        )
        wait_for(report.exists, "the cgroup-attached command never reported")
        pids = json.loads(report.read_text(encoding="utf-8"))
        assert pids["isolation_flag"] is None

        def inherited() -> bool:
            leaves = [path for path in system.groups() if path.name.startswith("run-")]
            return len(leaves) == 1 and {pids["command"], pids["child"]} <= system.members(
                leaves[0]
            )

        wait_for(inherited, "the later descendant did not inherit the run cgroup")
        release.touch()
        finished = wait_for(
            lambda: _terminal(client, run_id),
            "the cgroup-attached command did not finish",
        )
        assert finished["status"] == "passed"
        assert finished["resource_receipt"]["applied"] == {"cgroup-slot": 1}
        assert system.groups() == set()
    finally:
        release.touch()
        running.stop()
        system.close()


def test_double_forked_descendant_cannot_survive_cgroup_cancellation(
    tmp_path: Path,
):
    root = tmp_path / "delegated"
    system = FakeCgroupV2System(root)
    state_dir = tmp_path / "state"
    backend = CgroupV2Backend(
        root,
        state_dir=state_dir,
        system=system,
        empty_timeout=2,
    )
    running = RunningCoordinator(
        state_dir,
        capacities={"jobs": 1, "cgroup-slot": 1},
        resource_bindings=LIFECYCLE_BINDING,
        resource_backends={"cgroup-v2": backend},
    )
    client = running.start()
    repository = _repository(tmp_path / "repository")
    report = tmp_path / "detached-pid"

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

first = os.fork()
if first == 0:
    os.setsid()
    detached = os.fork()
    if detached == 0:
        report = Path(os.environ["DETACHED_REPORT"])
        report.touch()
        time.sleep(0.2)
        report.write_text(
            str(os.getpid()),
            encoding="utf-8",
        )
        while True:
            time.sleep(1)
    time.sleep(0.5)
    os._exit(0)
while True:
    time.sleep(1)
""",
            ],
            checkout=str(repository),
            resources={"cgroup-slot": 1},
            caller_pid=os.getpid(),
            environment={
                **caller_environment(),
                "DETACHED_REPORT": str(report),
            },
        )
        def reported_pid() -> int | None:
            value = report.read_text(encoding="utf-8").strip()
            return int(value) if value.isascii() and value.isdecimal() else None

        detached_pid = wait_for(
            reported_pid,
            "the double-forked descendant never reported a complete PID",
        )

        def inherited() -> bool:
            leaves = [path for path in system.groups() if path.name.startswith("run-")]
            return len(leaves) == 1 and detached_pid in system.members(leaves[0])

        wait_for(inherited, "the fake kernel did not observe double-fork inheritance")
        client.cancel(run_id)
        finished = wait_for(
            lambda: _terminal(client, run_id),
            "cgroup cancellation did not finish",
        )
        assert finished["status"] == "cancelled"
        wait_for(
            lambda: _process_identity(detached_pid) is None,
            "the double-forked descendant survived cgroup.kill",
        )
        assert system.groups() == set()
    finally:
        running.stop()
        system.close()


def test_durable_handle_recovers_an_empty_leaf_and_partial_cleanup(
    tmp_path: Path,
):
    root = tmp_path / "delegated"
    system = FakeCgroupV2System(root)
    state_dir = tmp_path / "state"
    request = _request("check-recoverable")
    first = CgroupV2Backend(root, state_dir=state_dir, system=system)
    handle = first.prepare(request)
    leaf = root / str(handle["owner"]) / str(handle["leaf"])

    try:
        replacement = CgroupV2Backend(root, state_dir=state_dir, system=system)
        assert replacement.prepare(request) == handle
        assert [path for path in system.groups() if path.name.startswith("run-")] == [
            leaf
        ]

        system.remove_group(leaf)
        replacement.cleanup(request, handle)
        assert system.groups() == set()
        assert list((state_dir / "cgroup-v2").glob("run-*.json")) == []
    finally:
        system.close()


def test_cleanup_refuses_a_reused_foreign_leaf_without_removing_it(tmp_path: Path):
    root = tmp_path / "delegated"
    system = FakeCgroupV2System(root)
    state_dir = tmp_path / "state"
    backend = CgroupV2Backend(root, state_dir=state_dir, system=system)
    request = _request("check-foreign")
    handle = backend.prepare(request)
    leaf = root / str(handle["owner"]) / str(handle["leaf"])
    original = system.replace_with_foreign_group(leaf)

    try:
        with pytest.raises(CgroupOwnershipError, match="reused|identity") as refused:
            backend.cleanup(request, handle)
        assert refused.value.code == "leaf-reused"
        assert leaf.is_dir()
        assert original.is_dir()
    finally:
        for path in (leaf, original, original.parent):
            if path.exists():
                system.remove_group(path)
        system.close()


def test_replacement_broker_adopts_live_cgroup_without_duplicate_work(
    tmp_path: Path,
):
    state_dir = tmp_path / "state"
    root = tmp_path / "delegated"
    repository = _repository(tmp_path / "repository")
    started = tmp_path / "started"
    release = tmp_path / "release"
    owner = subprocess.Popen(
        [
            sys.executable,
            "-u",
            "-c",
            """
import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd() / "tests"))
from agcoord.cgroup import CgroupV2Backend
from agcoord.queue import CoordinatorBroker
from test_cgroup import LIFECYCLE_BINDING, PersistentFakeCgroupV2System

state_dir = Path(sys.argv[1])
root = Path(sys.argv[2])
system = PersistentFakeCgroupV2System(root)
backend = CgroupV2Backend(root, state_dir=state_dir, system=system)
CoordinatorBroker(
    state_dir=state_dir,
    capacities={"jobs": 1, "cgroup-slot": 1},
    resource_bindings=LIFECYCLE_BINDING,
    resource_backends={"cgroup-v2": backend},
    idle_timeout=None,
).serve_forever()
""",
            str(state_dir),
            str(root),
        ],
        cwd=Path.cwd(),
        env=caller_environment(),
    )
    client = CoordinatorClient(state_dir=state_dir, autostart=False)
    replacement: RunningCoordinator | None = None
    worker_pid: int | None = None

    try:
        snapshot = wait_for(
            client.snapshot,
            "the original cgroup broker never acquired ownership",
        )
        assert snapshot["broker_pid"] == owner.pid
        run_id = client.submit(
            [
                sys.executable,
                "-u",
                "-c",
                """
from pathlib import Path
import os
import time

started = Path(os.environ["CGROUP_STARTED"])
with started.open("a", encoding="utf-8") as output:
    output.write(f"{os.getpid()}\\n")
while not Path(os.environ["CGROUP_RELEASE"]).exists():
    time.sleep(0.01)
""",
            ],
            checkout=str(repository),
            resources={"cgroup-slot": 1},
            caller_pid=os.getpid(),
            environment={
                **caller_environment(),
                "CGROUP_STARTED": str(started),
                "CGROUP_RELEASE": str(release),
            },
        )
        wait_for(started.exists, "the recoverable cgroup worker never started")
        live = client.status(run_id)
        worker_pid = live["worker_pid"]
        assert isinstance(worker_pid, int)
        assert started.read_text(encoding="utf-8").splitlines() == [str(worker_pid)]
        before = PersistentFakeCgroupV2System(root).groups()
        leaves = {path for path in before if path.name.startswith("run-")}
        assert len(leaves) == 1

        os.kill(owner.pid, signal.SIGKILL)
        owner.wait(timeout=5)
        replacement_system = PersistentFakeCgroupV2System(root)
        replacement_backend = CgroupV2Backend(
            root,
            state_dir=state_dir,
            system=replacement_system,
        )
        replacement = RunningCoordinator(
            state_dir,
            capacities={"jobs": 1, "cgroup-slot": 1},
            resource_bindings=LIFECYCLE_BINDING,
            resource_backends={"cgroup-v2": replacement_backend},
        )
        recovered_client = replacement.start()
        recovered = recovered_client.status(run_id)
        assert recovered["status"] == "running"
        assert recovered["worker_pid"] == worker_pid
        assert recovered_client.snapshot()["allocations"] == {
            "cgroup-slot": 1,
            "jobs": 1,
        }
        assert replacement_system.groups() == before
        assert started.read_text(encoding="utf-8").splitlines() == [str(worker_pid)]

        release.touch()
        finished = wait_for(
            lambda: _terminal(recovered_client, run_id),
            "the replacement broker did not finish the adopted cgroup worker",
        )
        assert finished["status"] == "interrupted"
        assert replacement_system.groups() == set()
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


@pytest.mark.parametrize(
    "reason",
    [
        "root-missing",
        "not-cgroup-v2",
        "delegation-read-only",
        "delegation-undelegated",
        "namespace-isolation-unavailable",
    ],
)
@pytest.mark.parametrize("mode", ["required", "best-effort"])
def test_unavailable_delegation_obeys_required_or_best_effort_mode(
    tmp_path: Path,
    reason: str,
    mode: str,
):
    root = tmp_path / "delegated"
    system = FakeCgroupV2System(root, reason=reason)
    state_dir = tmp_path / "state"
    backend = CgroupV2Backend(root, state_dir=state_dir, system=system)
    binding = {
        "cgroup-slot": {
            **LIFECYCLE_BINDING["cgroup-slot"],
            "mode": mode,
        }
    }
    running = RunningCoordinator(
        state_dir,
        capacities={"jobs": 1, "cgroup-slot": 1},
        resource_bindings=binding,
        resource_backends={"cgroup-v2": backend},
    )
    client = running.start()
    repository = _repository(tmp_path / "repository")
    marker = tmp_path / "user-code-ran"

    try:
        run_id = client.submit(
            [
                sys.executable,
                "-c",
                "from pathlib import Path; Path(__import__('sys').argv[1]).touch()",
                str(marker),
            ],
            checkout=str(repository),
            resources={"cgroup-slot": 1},
            caller_pid=os.getpid(),
            environment=caller_environment(),
        )
        finished = wait_for(
            lambda: _terminal(client, run_id),
            "the unavailable cgroup run did not finish",
        )
        event = finished["resource_receipt"]["events"][0]
        assert event["code"] == reason
        if mode == "required":
            assert finished["status"] == "failed"
            assert finished["exit_status"] == 125
            assert finished["failure_reason"] == "resource-enforcement-failed"
            assert event["status"] == "failed"
            assert not marker.exists()
        else:
            assert finished["status"] == "passed"
            assert event["status"] == "unapplied"
            assert marker.exists()
        assert system.groups() == set()
    finally:
        running.stop()
        system.close()


def test_linux_probe_reports_an_explicit_missing_root_without_mutation(tmp_path: Path):
    missing = tmp_path / "missing-delegation"
    backend = CgroupV2Backend(missing, state_dir=tmp_path / "state")

    assert backend.probe() == {
        "available": False,
        "kinds": [],
        "units": [],
        "operations": [],
        "reason": "root-missing",
    }
    assert not missing.exists()


@pytest.mark.parametrize(
    ("filesystem", "options", "expected"),
    [
        ("cgroup", "rw", "not-cgroup-v2"),
        ("cgroup2", "ro,nsdelegate", "delegation-read-only"),
        ("cgroup2", "rw", "namespace-delegation-unavailable"),
    ],
)
def test_linux_probe_distinguishes_v1_read_only_and_unsafe_namespace_mounts(
    tmp_path: Path,
    filesystem: str,
    options: str,
    expected: str,
):
    root = tmp_path / "delegated"
    root.mkdir()
    mountinfo = tmp_path / "mountinfo"
    mountinfo.write_text(
        f"36 29 0:32 / {root} {options} - {filesystem} cgroup {options}\n",
        encoding="utf-8",
    )
    backend = CgroupV2Backend(
        root,
        state_dir=tmp_path / "state",
        system=LinuxCgroupV2System(mountinfo=mountinfo),
    )

    assert backend.probe()["reason"] == expected
    assert list(root.iterdir()) == []


def test_linux_probe_reports_an_undelegated_writable_v2_mount(tmp_path: Path):
    root = tmp_path / "delegated"
    root.mkdir()
    for name, value in (
        ("cgroup.procs", ""),
        ("cgroup.events", "populated 0\n"),
        ("cgroup.controllers", "cpu memory\n"),
    ):
        (root / name).write_text(value, encoding="utf-8")
    mountinfo = tmp_path / "mountinfo"
    mountinfo.write_text(
        f"36 29 0:32 / {root} rw,nsdelegate - cgroup2 cgroup rw,nsdelegate\n",
        encoding="utf-8",
    )
    root.chmod(0o500)
    backend = CgroupV2Backend(
        root,
        state_dir=tmp_path / "state",
        system=LinuxCgroupV2System(mountinfo=mountinfo),
    )

    try:
        assert backend.probe()["reason"] == "delegation-undelegated"
    finally:
        root.chmod(0o700)
    assert {path.name for path in root.iterdir()} == {
        "cgroup.controllers",
        "cgroup.events",
        "cgroup.procs",
    }


@pytest.mark.skipif(
    not os.environ.get("AGCOORD_TEST_CGROUP_ROOT"),
    reason="set AGCOORD_TEST_CGROUP_ROOT to an exclusive writable delegation",
)
def test_real_delegation_hides_controller_files_from_the_worker(tmp_path: Path):
    root = Path(os.environ["AGCOORD_TEST_CGROUP_ROOT"]).resolve(strict=True)
    state_dir = tmp_path / "state"
    backend = CgroupV2Backend(root, state_dir=state_dir)
    assert backend.probe()["available"] is True
    running = RunningCoordinator(
        state_dir,
        capacities={"jobs": 1, "cgroup-slot": 1},
        resource_bindings=LIFECYCLE_BINDING,
        resource_backends={"cgroup-v2": backend},
    )
    client = running.start()
    repository = _repository(tmp_path / "repository")
    report = tmp_path / "namespace.json"

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
import sys

mounts = []
for line in Path("/proc/self/mountinfo").read_text(encoding="utf-8").splitlines():
    left, separator, right = line.partition(" - ")
    if separator and right.split()[0] == "cgroup2":
        mounts.append(Path(left.split()[4]))
if not mounts:
    raise AssertionError("isolated worker has no cgroup2 mount")
for mount in mounts:
    try:
        descriptor = os.open(mount / "cgroup.kill", os.O_WRONLY | os.O_CLOEXEC)
        try:
            os.write(descriptor, b"1\\n")
        finally:
            os.close(descriptor)
    except OSError as exc:
        if exc.errno not in {errno.EACCES, errno.EPERM, errno.EROFS}:
            raise
    else:
        raise AssertionError("worker could rewrite its cgroup namespace root")
Path(sys.argv[1]).write_text(
    json.dumps({
        "cgroup": Path("/proc/self/cgroup").read_text(encoding="ascii"),
        "mounts": [str(path) for path in mounts],
    }),
    encoding="utf-8",
)
""",
                str(report),
            ],
            checkout=str(repository),
            resources={"cgroup-slot": 1},
            caller_pid=os.getpid(),
            environment=caller_environment(),
        )
        finished = wait_for(
            lambda: _terminal(client, run_id),
            "the real cgroup namespace worker did not finish",
        )
        assert finished["status"] == "passed"
        observed = json.loads(report.read_text(encoding="utf-8"))
        assert "0::/\n" in observed["cgroup"]
        assert not (root / backend.owner_name).exists()
    finally:
        running.stop()
