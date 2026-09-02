"""Behavioral contracts for persistent project-quota scratch volumes."""

from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

import pytest

from agcoord import queue as queue_module
from agcoord import worker as worker_module
from agcoord.project_quota import (
    PROJECT_QUOTA_BACKEND,
    ProjectAttributes,
    ProjectQuotaBackend,
    ProjectQuotaError,
    ProjectQuotaMount,
    ProjectQuotaUsage,
)
from agcoord.resources import (
    ResourceContractError,
    ResourceMeasurement,
    ResourceRequest,
    resource_contract,
)

from conftest import RunningCoordinator, caller_environment, wait_for
from test_cgroup import _repository, _terminal


MIB = 1024 * 1024
QUOTA_BINDINGS = {
    "disk": {
        "backend": PROJECT_QUOTA_BACKEND,
        "kind": "storage",
        "mode": "required",
        "unit": "bytes",
    },
    "disk_inodes": {
        "backend": PROJECT_QUOTA_BACKEND,
        "kind": "inodes",
        "mode": "required",
        "unit": "inodes",
    },
}

_REFUSING_PRIVILEGE_LAUNCHER = r"""
import os
import sys

release_fd = int(sys.argv[1])
setup_fd = int(sys.argv[2])
if os.read(release_fd, 1) != b"1":
    raise SystemExit(125)
os.environ.pop("_AGCOORD_PROJECT_QUOTA_DROP_ADMIN")
os.write(setup_fd, b"worker-privilege-drop-failed")
os.close(setup_fd)
assert os.read(release_fd, 1) == b"0"
os.close(release_fd)
raise SystemExit(125)
"""


class _SimulatedBrokerCrash(BaseException):
    pass


class _ExecReached(BaseException):
    pass


class MemoryProjectQuotaSystem:
    """Filesystem/quota seam with real directories and deterministic kernel state."""

    def __init__(
        self,
        root: Path,
        *,
        available: bool = True,
        reason: str = "quota-privilege-unavailable",
    ) -> None:
        self.root = root.resolve()
        self.available = available
        self.reason = reason
        self.mount = ProjectQuotaMount(
            path=self.root,
            source=Path("/dev/fake-quota"),
            filesystem="ext4",
            device="8:30",
        )
        self.attributes: dict[Path, ProjectAttributes] = {}
        self.quotas: dict[int, ProjectQuotaUsage] = {}
        self.forced_usage: dict[int, tuple[int, int]] = {}
        self.fail_quota_sets = 0
        self.crash_quota_sets = 0

    def probe(self, _path: Path) -> ProjectQuotaMount:
        if not self.available:
            raise ProjectQuotaError(self.reason, "project quotas are unavailable")
        return self.mount

    def get_attributes(self, path: Path) -> ProjectAttributes:
        return self.attributes.get(
            path.resolve(),
            ProjectAttributes(project_id=0, inherit=False),
        )

    def set_attributes(
        self,
        path: Path,
        *,
        project_id: int,
        inherit: bool,
    ) -> None:
        self.attributes[path.resolve()] = ProjectAttributes(project_id, inherit)

    def get_quota(
        self,
        _mount: ProjectQuotaMount,
        project_id: int,
    ) -> ProjectQuotaUsage:
        current = self.quotas.get(project_id, ProjectQuotaUsage(0, 0, 0, 0))
        used_bytes, used_inodes = self.forced_usage.get(project_id, (0, 0))
        owned_roots = [
            path
            for path, attributes in self.attributes.items()
            if attributes.project_id == project_id and path.exists()
        ]
        if owned_roots:
            used_inodes = max(used_inodes, 1)
        else:
            used_bytes = 0
            used_inodes = 0
        return ProjectQuotaUsage(
            hard_bytes=current.hard_bytes,
            hard_inodes=current.hard_inodes,
            used_bytes=used_bytes,
            used_inodes=used_inodes,
        )

    def set_quota(
        self,
        _mount: ProjectQuotaMount,
        project_id: int,
        *,
        hard_bytes: int,
        hard_inodes: int,
    ) -> None:
        if self.crash_quota_sets:
            self.crash_quota_sets -= 1
            raise _SimulatedBrokerCrash()
        if self.fail_quota_sets:
            self.fail_quota_sets -= 1
            raise ProjectQuotaError(
                "quota-operation-failed",
                "injected quota update failure",
            )
        current = self.get_quota(self.mount, project_id)
        self.quotas[project_id] = ProjectQuotaUsage(
            hard_bytes=hard_bytes,
            hard_inodes=hard_inodes,
            used_bytes=current.used_bytes,
            used_inodes=current.used_inodes,
        )

    def sync(self, _path: Path) -> None:
        return None


def _request(run_id: str = "check-project-quota") -> ResourceRequest:
    return ResourceRequest.build(
        run_id,
        PROJECT_QUOTA_BACKEND,
        {"disk": 8 * MIB, "disk_inodes": 64},
        QUOTA_BINDINGS,
    )


def test_project_quota_prepares_private_tree_and_retains_terminal_usage(
    tmp_path: Path,
):
    state_dir = tmp_path / "state"
    system = MemoryProjectQuotaSystem(tmp_path)
    backend = ProjectQuotaBackend(
        state_dir,
        system=system,
        project_id_source=iter((1_500_000_001,)).__next__,
    )
    request = _request()

    handle = backend.prepare(request)
    target = backend.scratch_path(request, handle)
    assert target.is_dir()
    assert target.stat().st_mode & 0o777 == 0o700
    assert system.get_attributes(target) == ProjectAttributes(
        project_id=1_500_000_001,
        inherit=True,
    )
    assert system.get_quota(system.mount, 1_500_000_001) == ProjectQuotaUsage(
        hard_bytes=8 * MIB,
        hard_inodes=64,
        used_bytes=0,
        used_inodes=1,
    )

    system.forced_usage[1_500_000_001] = (8 * MIB, 64)
    measured = backend.finish(request, handle)
    assert isinstance(measured, ResourceMeasurement)
    assert measured.peak == {"disk": 8 * MIB, "disk_inodes": 64}
    assert {(item.resource, item.code) for item in measured.observations} == {
        ("disk", "storage-byte-limit-hit"),
        ("disk_inodes", "storage-inode-limit-hit"),
    }

    backend.cleanup(request, handle)
    assert not target.exists()
    assert system.get_quota(system.mount, 1_500_000_001) == ProjectQuotaUsage(
        0,
        0,
        0,
        0,
    )
    assert not list((state_dir / PROJECT_QUOTA_BACKEND).glob("run-*.json"))


@pytest.mark.parametrize(
    ("resources", "expected_code"),
    [
        ({"disk": 8 * MIB}, "quota-policy-incomplete"),
        ({"disk_inodes": 64}, "quota-policy-incomplete"),
        (
            {"disk": 8 * MIB + 1, "disk_inodes": 64},
            "quota-byte-alignment-invalid",
        ),
    ],
)
def test_invalid_project_quota_policy_refuses_without_allocating_scratch(
    tmp_path: Path,
    resources: dict[str, int],
    expected_code: str,
):
    state_dir = tmp_path / "state"
    system = MemoryProjectQuotaSystem(tmp_path)
    backend = ProjectQuotaBackend(state_dir, system=system)
    bindings = {name: QUOTA_BINDINGS[name] for name in resources}
    request = ResourceRequest.build(
        "check-invalid-project-quota",
        PROJECT_QUOTA_BACKEND,
        resources,
        bindings,
    )

    with pytest.raises(ProjectQuotaError) as raised:
        backend.prepare(request)
    assert raised.value.code == expected_code
    assert not (state_dir / PROJECT_QUOTA_BACKEND / "runs").exists()


def test_project_id_collision_is_skipped_before_a_tree_is_assigned(tmp_path: Path):
    system = MemoryProjectQuotaSystem(tmp_path)
    system.quotas[1_500_000_001] = ProjectQuotaUsage(
        hard_bytes=16 * MIB,
        hard_inodes=128,
        used_bytes=4 * MIB,
        used_inodes=4,
    )
    candidates = iter((1_500_000_001, 1_500_000_002))
    backend = ProjectQuotaBackend(
        tmp_path / "state",
        system=system,
        project_id_source=candidates.__next__,
    )
    request = _request("check-project-collision")

    handle = backend.prepare(request)
    assert handle["project_id"] == 1_500_000_002
    backend.finish(request, handle)
    backend.cleanup(request, handle)
    assert system.quotas[1_500_000_001].hard_bytes == 16 * MIB


def test_interrupted_allocation_is_resumed_from_its_durable_manifest(tmp_path: Path):
    state_dir = tmp_path / "state"
    system = MemoryProjectQuotaSystem(tmp_path)
    system.crash_quota_sets = 1
    request = _request("check-project-allocation-recovery")
    original = ProjectQuotaBackend(
        state_dir,
        system=system,
        project_id_source=iter((1_500_000_006,)).__next__,
    )

    with pytest.raises(_SimulatedBrokerCrash):
        original.prepare(request)
    assert list((state_dir / PROJECT_QUOTA_BACKEND).glob("run-*.json"))

    replacement = ProjectQuotaBackend(state_dir, system=system)
    handle = replacement.prepare(request)
    assert handle["project_id"] == 1_500_000_006
    target = replacement.scratch_path(request, handle)
    assert target.is_dir()
    replacement.finish(request, handle)
    replacement.cleanup(request, handle)
    assert not target.exists()


def test_caught_prepare_error_rolls_back_tree_quota_and_manifest(tmp_path: Path):
    state_dir = tmp_path / "state"
    system = MemoryProjectQuotaSystem(tmp_path)
    system.fail_quota_sets = 1
    backend = ProjectQuotaBackend(
        state_dir,
        system=system,
        project_id_source=iter((1_500_000_008,)).__next__,
    )

    with pytest.raises(ProjectQuotaError) as raised:
        backend.prepare(_request("check-project-rollback"))
    assert raised.value.code == "quota-operation-failed"
    assert not list((state_dir / PROJECT_QUOTA_BACKEND).glob("run-*.json"))
    assert not list((state_dir / PROJECT_QUOTA_BACKEND / "runs").iterdir())
    assert system.get_quota(system.mount, 1_500_000_008) == ProjectQuotaUsage(
        0,
        0,
        0,
        0,
    )


def test_coordinator_exposes_only_the_quota_tree_and_cleans_it_after_descendants(
    tmp_path: Path,
):
    state_dir = tmp_path / "state"
    system = MemoryProjectQuotaSystem(tmp_path)
    backend = ProjectQuotaBackend(
        state_dir,
        system=system,
        project_id_source=iter((1_500_000_003,)).__next__,
    )
    running = RunningCoordinator(
        state_dir,
        capacities={"jobs": 1, "disk": 8 * MIB, "disk_inodes": 64},
        resource_bindings=QUOTA_BINDINGS,
        resource_backends={PROJECT_QUOTA_BACKEND: backend},
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
import sys

target = Path(os.environ["TMPDIR"])
(target / "artifact").write_bytes(b"x" * 4096)
Path(sys.argv[1]).write_text(json.dumps({
    "target": str(target),
    "variables": [os.environ[name] for name in ("TMPDIR", "TMP", "TEMP")],
    "status": {
        line.split(":", 1)[0]: line.split(":", 1)[1].strip()
        for line in Path("/proc/self/status").read_text(encoding="ascii").splitlines()
        if line.startswith(("CapEff:", "CapPrm:", "CapInh:", "CapAmb:", "NoNewPrivs:"))
    },
}), encoding="utf-8")
""",
                str(report),
            ],
            checkout=str(repository),
            resources={"disk": 8 * MIB, "disk_inodes": 64},
            caller_pid=os.getpid(),
            environment=caller_environment(),
        )
        finished = wait_for(
            lambda: _terminal(client, run_id),
            "the project-quota run did not finish",
        )
        observed = json.loads(report.read_text(encoding="utf-8"))
        target = Path(observed["target"])
        assert finished["status"] == "passed"
        assert finished["resource_receipt"]["applied"] == {
            "disk": 8 * MIB,
            "disk_inodes": 64,
        }
        assert len(set(observed["variables"])) == 1
        assert observed["variables"] == [str(target)] * 3
        assert {
            observed["status"][name]
            for name in ("CapEff", "CapPrm", "CapInh", "CapAmb")
        } == {"0000000000000000"}
        assert observed["status"]["NoNewPrivs"] == "1"
        assert state_dir / PROJECT_QUOTA_BACKEND / "runs" in target.parents
        assert not target.exists()
        assert not list(
            (state_dir / PROJECT_QUOTA_BACKEND).glob("run-*.json")
        )
    finally:
        running.stop()


class _RefusingScratchBackend(ProjectQuotaBackend):
    def scratch_path(
        self,
        request: ResourceRequest,
        state: dict[str, object],
    ) -> Path:
        raise ProjectQuotaError(
            "quota-tree-unavailable",
            "injected post-allocation refusal",
        )


@pytest.mark.parametrize(
    ("mode", "expected_status", "ran"),
    [("required", "failed", False), ("best-effort", "passed", True)],
)
def test_post_allocation_refusal_fails_required_or_runs_without_scratch(
    tmp_path: Path,
    mode: str,
    expected_status: str,
    ran: bool,
):
    state_dir = tmp_path / "state"
    system = MemoryProjectQuotaSystem(tmp_path)
    backend = _RefusingScratchBackend(
        state_dir,
        system=system,
        project_id_source=iter((1_500_000_007,)).__next__,
    )
    bindings = {
        name: {**binding, "mode": mode}
        for name, binding in QUOTA_BINDINGS.items()
    }
    running = RunningCoordinator(
        state_dir,
        capacities={"jobs": 1, "disk": 8 * MIB, "disk_inodes": 64},
        resource_bindings=bindings,
        resource_backends={PROJECT_QUOTA_BACKEND: backend},
    )
    client = running.start()
    repository = _repository(tmp_path / "repository")
    report = tmp_path / "ran.json"

    try:
        run_id = client.submit(
            [
                sys.executable,
                "-c",
                """
import json
import os
from pathlib import Path
import sys
Path(sys.argv[1]).write_text(json.dumps({"tmp": os.environ.get("TMPDIR")}))
""",
                str(report),
            ],
            checkout=str(repository),
            resources={"disk": 8 * MIB, "disk_inodes": 64},
            caller_pid=os.getpid(),
            environment=caller_environment(),
        )
        finished = wait_for(
            lambda: _terminal(client, run_id),
            "the post-allocation quota refusal did not finish",
        )
        assert finished["status"] == expected_status
        assert report.exists() is ran
        failures = [
            event
            for event in finished["resource_receipt"]["events"]
            if event["code"] == "quota-tree-unavailable"
        ]
        assert len(failures) == 2
        assert {event["status"] for event in failures} == {
            "failed" if mode == "required" else "unapplied"
        }
        if ran:
            assert json.loads(report.read_text(encoding="utf-8")) == {"tmp": None}
    finally:
        running.stop()


@pytest.mark.parametrize("mode", ["required", "best-effort"])
def test_worker_privilege_drop_refusal_never_releases_user_code(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
):
    monkeypatch.setattr(
        queue_module,
        "_WORKER_LAUNCHER",
        _REFUSING_PRIVILEGE_LAUNCHER,
    )
    state_dir = tmp_path / "state"
    system = MemoryProjectQuotaSystem(tmp_path)
    backend = ProjectQuotaBackend(
        state_dir,
        system=system,
        project_id_source=iter((1_500_000_010,)).__next__,
    )
    bindings = {
        name: {**binding, "mode": mode}
        for name, binding in QUOTA_BINDINGS.items()
    }
    running = RunningCoordinator(
        state_dir,
        capacities={"jobs": 1, "disk": 8 * MIB, "disk_inodes": 64},
        resource_bindings=bindings,
        resource_backends={PROJECT_QUOTA_BACKEND: backend},
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
            resources={"disk": 8 * MIB, "disk_inodes": 64},
            caller_pid=os.getpid(),
            environment=caller_environment(),
        )
        finished = wait_for(
            lambda: _terminal(client, run_id),
            "the privilege-drop refusal did not finish",
        )
        assert finished["status"] == "failed"
        assert finished["exit_status"] == 125
        assert finished["failure_reason"] == "resource-enforcement-failed"
        assert not marker.exists()
        failures = [
            event
            for event in finished["resource_receipt"]["events"]
            if event["code"] == "worker-privilege-drop-failed"
        ]
        assert len(failures) == 2
        assert {event["status"] for event in failures} == {
            "failed" if mode == "required" else "unapplied"
        }
        assert not list((state_dir / PROJECT_QUOTA_BACKEND / "runs").iterdir())
    finally:
        running.stop()


def test_worker_finishes_cgroup_namespace_setup_before_dropping_capabilities(
    monkeypatch: pytest.MonkeyPatch,
):
    release_read, release_write = os.pipe()
    setup_read, setup_write = os.pipe()
    observed: list[str] = []

    def isolate() -> None:
        observed.append("isolated")

    def drop() -> None:
        assert observed == ["isolated"]
        observed.append("dropped")

    def execute(_command: list[str], _environment: dict[str, str]) -> None:
        assert observed == ["isolated", "dropped"]
        raise _ExecReached()

    monkeypatch.setattr(worker_module, "isolate_current_cgroup", isolate)
    monkeypatch.setattr(worker_module, "_drop_initial_admin_capabilities", drop)
    monkeypatch.setattr(worker_module, "_exec", execute)
    monkeypatch.setenv("_AGCOORD_CGROUP_ISOLATE", "1")
    monkeypatch.setenv("_AGCOORD_PROJECT_QUOTA_DROP_ADMIN", "1")
    monkeypatch.setattr(
        sys,
        "argv",
        ["agcoord-worker", str(release_read), str(setup_write), "command"],
    )
    os.write(release_write, b"11")
    os.close(release_write)

    try:
        with pytest.raises(_ExecReached):
            worker_module.launcher_main()
        assert os.read(setup_read, 16) == b"ok"
        assert observed == ["isolated", "dropped"]
    finally:
        os.close(setup_read)


def test_persistent_and_tmpfs_scratch_providers_are_rejected_together():
    bindings = {
        **QUOTA_BINDINGS,
        "ram_scratch": {
            "backend": "cgroup-v2",
            "kind": "tmpfs",
            "mode": "required",
            "unit": "bytes",
        },
    }
    with pytest.raises(ResourceContractError, match="cannot combine"):
        resource_contract(
            {"disk": 8 * MIB, "disk_inodes": 64, "ram_scratch": 4 * MIB},
            bindings,
        )


@pytest.mark.parametrize(
    ("mode", "expected_status", "expected_exit", "ran"),
    [
        ("required", "failed", 125, False),
        ("best-effort", "passed", 0, True),
    ],
)
def test_unavailable_quota_backend_obeys_required_or_no_scratch_mode(
    tmp_path: Path,
    mode: str,
    expected_status: str,
    expected_exit: int,
    ran: bool,
):
    state_dir = tmp_path / "state"
    system = MemoryProjectQuotaSystem(tmp_path, available=False)
    backend = ProjectQuotaBackend(state_dir, system=system)
    bindings = {
        name: {**binding, "mode": mode}
        for name, binding in QUOTA_BINDINGS.items()
    }
    running = RunningCoordinator(
        state_dir,
        capacities={"jobs": 1, "disk": 8 * MIB, "disk_inodes": 64},
        resource_bindings=bindings,
        resource_backends={PROJECT_QUOTA_BACKEND: backend},
    )
    client = running.start()
    repository = _repository(tmp_path / "repository")
    report = tmp_path / "user-code.json"

    try:
        run_id = client.submit(
            [
                sys.executable,
                "-c",
                "import json,os,pathlib,sys; pathlib.Path(sys.argv[1]).write_text(json.dumps({name: os.environ.get(name) for name in ('TMPDIR','TMP','TEMP')}))",
                str(report),
            ],
            checkout=str(repository),
            resources={"disk": 8 * MIB, "disk_inodes": 64},
            caller_pid=os.getpid(),
            environment=caller_environment(),
        )
        finished = wait_for(
            lambda: _terminal(client, run_id),
            "the unavailable project-quota run did not finish",
        )
        assert finished["status"] == expected_status
        assert finished["exit_status"] == expected_exit
        assert report.exists() is ran
        if ran:
            assert json.loads(report.read_text(encoding="utf-8")) == {
                "TMPDIR": None,
                "TMP": None,
                "TEMP": None,
            }
        events = [
            event
            for event in finished["resource_receipt"]["events"]
            if event["backend"] == PROJECT_QUOTA_BACKEND
        ]
        assert {event["code"] for event in events} == {
            "quota-privilege-unavailable"
        }
        assert {event["status"] for event in events} == {
            "failed" if mode == "required" else "unapplied"
        }
    finally:
        running.stop()


def test_replacement_backend_adopts_live_quota_identity_and_cleans_owned_data(
    tmp_path: Path,
):
    state_dir = tmp_path / "state"
    system = MemoryProjectQuotaSystem(tmp_path)
    request = _request("check-project-quota-recovery")
    original = ProjectQuotaBackend(
        state_dir,
        system=system,
        project_id_source=iter((1_500_000_004,)).__next__,
    )
    handle = original.prepare(request)
    target = original.scratch_path(request, handle)
    (target / "survives-broker").write_text("owned\n", encoding="utf-8")
    system.forced_usage[1_500_000_004] = (4096, 2)

    replacement = ProjectQuotaBackend(state_dir, system=system)
    measured = replacement.finish(request, handle)
    assert isinstance(measured, ResourceMeasurement)
    assert measured.peak == {"disk": 4096, "disk_inodes": 2}
    replacement.cleanup(request, handle)
    assert not target.exists()
    assert system.get_quota(system.mount, 1_500_000_004) == ProjectQuotaUsage(
        0,
        0,
        0,
        0,
    )


def test_cancellation_records_terminal_quota_usage_before_owned_cleanup(
    tmp_path: Path,
):
    state_dir = tmp_path / "state"
    system = MemoryProjectQuotaSystem(tmp_path)
    backend = ProjectQuotaBackend(
        state_dir,
        system=system,
        project_id_source=iter((1_500_000_009,)).__next__,
    )
    running = RunningCoordinator(
        state_dir,
        capacities={"jobs": 1, "disk": 8 * MIB, "disk_inodes": 64},
        resource_bindings=QUOTA_BINDINGS,
        resource_backends={PROJECT_QUOTA_BACKEND: backend},
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
import os
from pathlib import Path
import sys
import time

target = Path(os.environ["TMPDIR"])
(target / "artifact").write_bytes(b"x" * 4096)
Path(sys.argv[1]).write_text(str(target), encoding="utf-8")
while True:
    time.sleep(1)
""",
                str(entered),
            ],
            checkout=str(repository),
            resources={"disk": 8 * MIB, "disk_inodes": 64},
            caller_pid=os.getpid(),
            environment=caller_environment(),
        )
        wait_for(entered.exists, "the cancellable quota run did not start")
        target = Path(entered.read_text(encoding="utf-8"))
        system.forced_usage[1_500_000_009] = (4096, 2)
        client.cancel(run_id)
        finished = wait_for(
            lambda: _terminal(client, run_id),
            "the cancellable quota run did not finish",
        )
        assert finished["status"] == "cancelled"
        assert finished["resource_receipt"]["peak"] == {
            "disk": 4096,
            "disk_inodes": 2,
        }
        assert not target.exists()
        assert system.get_quota(system.mount, 1_500_000_009) == ProjectQuotaUsage(
            0,
            0,
            0,
            0,
        )
    finally:
        running.stop()


def test_changed_quota_tree_identity_is_refused_without_removing_replacement(
    tmp_path: Path,
):
    state_dir = tmp_path / "state"
    system = MemoryProjectQuotaSystem(tmp_path)
    backend = ProjectQuotaBackend(
        state_dir,
        system=system,
        project_id_source=iter((1_500_000_005,)).__next__,
    )
    request = _request("check-project-quota-reused")
    handle = backend.prepare(request)
    target = backend.scratch_path(request, handle)
    replacement = target.with_name(f"{target.name}-replacement")
    replacement.mkdir(mode=0o700)
    replacement_inode = replacement.stat().st_ino
    assert replacement_inode != target.stat().st_ino
    target.rmdir()
    replacement.rename(target)

    with pytest.raises(ProjectQuotaError) as raised:
        backend.cleanup(request, handle)
    assert raised.value.code == "quota-tree-reused"
    assert target.exists()
    assert target.stat().st_ino == replacement_inode


def test_dangling_symlink_replacement_is_refused_without_reclaiming_identity(
    tmp_path: Path,
):
    state_dir = tmp_path / "state"
    system = MemoryProjectQuotaSystem(tmp_path)
    backend = ProjectQuotaBackend(
        state_dir,
        system=system,
        project_id_source=iter((1_500_000_011,)).__next__,
    )
    request = _request("check-project-quota-dangling-replacement")
    handle = backend.prepare(request)
    target = backend.scratch_path(request, handle)
    target.rmdir()
    target.symlink_to(tmp_path / "missing-external-target", target_is_directory=True)

    with pytest.raises(ProjectQuotaError) as raised:
        backend.cleanup(request, handle)
    assert raised.value.code == "quota-tree-reused"
    assert target.is_symlink()
    assert system.get_quota(system.mount, 1_500_000_011).hard_bytes == 8 * MIB
    assert list((state_dir / PROJECT_QUOTA_BACKEND).glob("run-*.json"))


def test_cleanup_recovers_owned_directories_after_user_revokes_modes(tmp_path: Path):
    state_dir = tmp_path / "state"
    system = MemoryProjectQuotaSystem(tmp_path)
    backend = ProjectQuotaBackend(
        state_dir,
        system=system,
        project_id_source=iter((1_500_000_012,)).__next__,
    )
    request = _request("check-project-quota-revoked-modes")
    handle = backend.prepare(request)
    target = backend.scratch_path(request, handle)
    nested = target / "locked"
    nested.mkdir()
    (nested / "artifact").write_text("owned\n", encoding="utf-8")
    nested.chmod(0)
    target.chmod(0)

    backend.cleanup(request, handle)
    assert not target.exists()
    assert system.get_quota(system.mount, 1_500_000_012) == ProjectQuotaUsage(
        0,
        0,
        0,
        0,
    )


@pytest.mark.skipif(
    os.environ.get("AGCOORD_TEST_PROJECT_QUOTA") != "1",
    reason="set AGCOORD_TEST_PROJECT_QUOTA=1 as init-namespace root",
)
def test_real_ext4_project_quotas_enforce_bytes_inodes_and_parallel_identity(
    tmp_path: Path,
):
    for command in ("mkfs.ext4", "mount", "umount"):
        assert shutil.which(command), f"{command} is required for the real quota test"
    image = tmp_path / "project-quota.ext4"
    mountpoint = tmp_path / "mounted"
    mountpoint.mkdir()
    image.touch()
    os.truncate(image, 128 * MIB)
    subprocess.run(
        [
            "mkfs.ext4",
            "-q",
            "-F",
            "-O",
            "quota,project",
            "-Q",
            "prjquota",
            str(image),
        ],
        check=True,
    )
    mounted = False
    running: RunningCoordinator | None = None
    try:
        subprocess.run(
            ["mount", "-o", "loop,prjquota", str(image), str(mountpoint)],
            check=True,
        )
        mounted = True
        state_dir = mountpoint / "state"
        backend = ProjectQuotaBackend(state_dir)
        assert backend.probe() == {
            "available": True,
            "kinds": ["inodes", "storage"],
            "units": ["bytes", "inodes"],
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
        running = RunningCoordinator(
            state_dir,
            capacities={
                "jobs": 2,
                "disk": 16 * MIB,
                "disk_inodes": 256,
            },
            resource_bindings=QUOTA_BINDINGS,
            resource_backends={PROJECT_QUOTA_BACKEND: backend},
        )
        client = running.start()
        repository = _repository(tmp_path / "repository")

        byte_report = tmp_path / "byte-report.json"
        byte_id = client.submit(
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

target = Path(os.environ["TMPDIR"])
descriptor = os.open(target / "payload", os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
written = 0
limited = False
try:
    block = b"x" * 4096
    while True:
        try:
            written += os.write(descriptor, block)
        except OSError as exc:
            if exc.errno not in {errno.EDQUOT, errno.ENOSPC}:
                raise
            limited = True
            break
finally:
    os.close(descriptor)
Path(sys.argv[1]).write_text(json.dumps({
    "limited": limited,
    "target": str(target),
    "written": written,
}), encoding="utf-8")
""",
                str(byte_report),
            ],
            checkout=str(repository),
            resources={"disk": 2 * MIB, "disk_inodes": 128},
            caller_pid=os.getpid(),
            environment=caller_environment(),
        )
        byte_finished = wait_for(
            lambda: _terminal(client, byte_id),
            "the real project byte limit did not finish",
            timeout=30,
        )
        byte_observed = json.loads(byte_report.read_text(encoding="utf-8"))
        assert byte_finished["status"] == "passed"
        assert byte_observed["limited"] is True
        assert 0 < byte_observed["written"] <= 2 * MIB
        assert not Path(byte_observed["target"]).exists()
        assert "storage-byte-limit-hit" in {
            event["code"]
            for event in byte_finished["resource_receipt"]["events"]
        }

        inode_report = tmp_path / "inode-report.json"
        inode_id = client.submit(
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

target = Path(os.environ["TMPDIR"])
created = 0
limited = False
while True:
    try:
        (target / f"item-{created}").touch(exist_ok=False)
        created += 1
    except OSError as exc:
        if exc.errno not in {errno.EDQUOT, errno.ENOSPC}:
            raise
        limited = True
        break
Path(sys.argv[1]).write_text(json.dumps({
    "created": created,
    "limited": limited,
    "target": str(target),
}), encoding="utf-8")
""",
                str(inode_report),
            ],
            checkout=str(repository),
            resources={"disk": 8 * MIB, "disk_inodes": 16},
            caller_pid=os.getpid(),
            environment=caller_environment(),
        )
        inode_finished = wait_for(
            lambda: _terminal(client, inode_id),
            "the real project inode limit did not finish",
            timeout=30,
        )
        inode_observed = json.loads(inode_report.read_text(encoding="utf-8"))
        assert inode_finished["status"] == "passed"
        assert inode_observed["limited"] is True
        assert 0 < inode_observed["created"] < 16
        assert not Path(inode_observed["target"]).exists()
        assert "storage-inode-limit-hit" in {
            event["code"]
            for event in inode_finished["resource_receipt"]["events"]
        }

        def submit_parallel(name: str, fill: bool) -> tuple[str, Path, Path]:
            ready = tmp_path / f"{name}-ready.json"
            release = tmp_path / f"{name}-release"
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
import time

ready, release = map(Path, sys.argv[1:3])
fill = sys.argv[3] == "fill"
target = Path(os.environ["TMPDIR"])
descriptor = os.open(target / "parallel", os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
written = 0
limited = False
try:
    block = b"x" * 4096
    while written < (1024 * 1024 if not fill else 8 * 1024 * 1024):
        try:
            written += os.write(descriptor, block)
        except OSError as exc:
            if exc.errno not in {errno.EDQUOT, errno.ENOSPC}:
                raise
            limited = True
            break
finally:
    os.close(descriptor)
ready.write_text(json.dumps({
    "limited": limited,
    "target": str(target),
    "written": written,
}), encoding="utf-8")
while not release.exists():
    time.sleep(0.01)
""",
                    str(ready),
                    str(release),
                    "fill" if fill else "small",
                ],
                checkout=str(repository),
                resources={"disk": 4 * MIB, "disk_inodes": 64},
                caller_pid=os.getpid(),
                environment=caller_environment(),
            )
            return run_id, ready, release

        first = submit_parallel("first", True)
        second = submit_parallel("second", False)
        wait_for(first[1].exists, "the first parallel quota tree did not start")
        wait_for(second[1].exists, "the second parallel quota tree did not start")
        first_observed = json.loads(first[1].read_text(encoding="utf-8"))
        second_observed = json.loads(second[1].read_text(encoding="utf-8"))
        assert first_observed["limited"] is True
        assert second_observed["limited"] is False
        assert second_observed["written"] == MIB
        assert first_observed["target"] != second_observed["target"]
        first[2].touch()
        second[2].touch()
        assert wait_for(
            lambda: _terminal(client, first[0]),
            "the first parallel quota run did not finish",
        )["status"] == "passed"
        assert wait_for(
            lambda: _terminal(client, second[0]),
            "the second parallel quota run did not finish",
        )["status"] == "passed"
        assert not Path(first_observed["target"]).exists()
        assert not Path(second_observed["target"]).exists()
    finally:
        if running is not None:
            running.stop()
        if mounted:
            subprocess.run(["umount", str(mountpoint)], check=True)
