"""Behavioral per-device bandwidth, IOPS, and I/O weight contracts."""

from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

import pytest

from agcoord.cgroup import (
    CgroupV2Backend,
    CgroupV2Error,
    IoDevice,
    LinuxIoDeviceResolver,
)
from agcoord.config import BrokerConfigError, parse_broker_config
from agcoord.resources import ResourceMeasurement, ResourceRequest

from conftest import RunningReferenceBroker, caller_environment, wait_for
from test_cgroup import _repository, _terminal
from test_cgroup_compute import ControllerFakeCgroupV2System


MIB = 1024 * 1024
IO_BINDINGS = {
    "read_bps": {
        "backend": "cgroup-v2",
        "kind": "io-bandwidth",
        "mode": "required",
        "unit": "read-bytes-per-second",
    },
    "write_bps": {
        "backend": "cgroup-v2",
        "kind": "io-bandwidth",
        "mode": "required",
        "unit": "write-bytes-per-second",
    },
    "read_iops": {
        "backend": "cgroup-v2",
        "kind": "io-operations",
        "mode": "required",
        "unit": "read-operations-per-second",
    },
    "write_iops": {
        "backend": "cgroup-v2",
        "kind": "io-operations",
        "mode": "required",
        "unit": "write-operations-per-second",
    },
    "io_weight": {
        "backend": "cgroup-v2",
        "kind": "io-weight",
        "mode": "required",
        "unit": "weight",
    },
}


class StaticIoResolver:
    def __init__(
        self,
        devices: tuple[IoDevice, ...] = (IoDevice("7:31", "ext4"),),
        *,
        error: CgroupV2Error | None = None,
    ) -> None:
        self.devices = devices
        self.error = error
        self.calls: list[tuple[Path, ...]] = []

    def resolve(self, paths: tuple[Path, ...]) -> tuple[IoDevice, ...]:
        self.calls.append(paths)
        if self.error is not None:
            raise self.error
        return self.devices


class IoControllerFakeCgroupV2System(ControllerFakeCgroupV2System):
    """Keyed io.max/io.weight files plus deterministic io.stat counters."""

    def __init__(self, root: Path, *, controllers: set[str] | None = None) -> None:
        super().__init__(
            root,
            controllers={"io"} if controllers is None else controllers,
        )
        self.io_limits: dict[Path, dict[str, dict[str, str]]] = {root: {}}
        self.io_weights: dict[Path, dict[str, int]] = {root: {}}
        self.terminal_io: dict[Path, tuple[int, dict[str, dict[str, int]]]] = {}

    def create_group(self, parent: Path, name: str):
        identity = super().create_group(parent, name)
        path = parent / name
        self.io_limits[path] = {}
        self.io_weights[path] = {}
        self._render_io(path)
        return identity

    def _render_io(self, path: Path) -> None:
        self.files[path]["io.max"] = "".join(
            f"{device} "
            + " ".join(
                f"{key}={values.get(key, 'max')}"
                for key in ("rbps", "wbps", "riops", "wiops")
            )
            + "\n"
            for device, values in sorted(self.io_limits[path].items())
        )
        self.files[path]["io.weight"] = "default 100\n" + "".join(
            f"{device} {weight}\n"
            for device, weight in sorted(self.io_weights[path].items())
        )
        self.files[path].setdefault("io.stat", "")

    def write_file(self, path: Path, name: str, value: str) -> None:
        fields = value.split()
        if name == "io.max":
            device, *settings = fields
            selected = self.io_limits[path].setdefault(device, {})
            for setting in settings:
                key, raw = setting.split("=", 1)
                selected[key] = raw
            self._render_io(path)
            return
        if name == "io.weight":
            device, raw = fields
            self.io_weights[path][device] = int(raw)
            self._render_io(path)
            return
        super().write_file(path, name, value)

    def set_io_stats(
        self,
        path: Path,
        stats: dict[str, dict[str, int]],
    ) -> None:
        self.files[path]["io.stat"] = "".join(
            f"{device} "
            + " ".join(f"{key}={value}" for key, value in values.items())
            + "\n"
            for device, values in sorted(stats.items())
        )

    def populated(self, path: Path) -> bool:
        populated = super().populated(path)
        if not populated and path in self.terminal_io:
            now_ns, stats = self.terminal_io.pop(path)
            self.now_ns = now_ns
            self.set_io_stats(path, stats)
        return populated

    def remove_group(self, path: Path) -> None:
        super().remove_group(path)
        self.io_limits.pop(path, None)
        self.io_weights.pop(path, None)
        self.terminal_io.pop(path, None)


class IgnoringIoControllerFakeCgroupV2System(IoControllerFakeCgroupV2System):
    def __init__(self, root: Path, *, ignored: str) -> None:
        super().__init__(root)
        self.ignored = ignored

    def write_file(self, path: Path, name: str, value: str) -> None:
        if name == self.ignored:
            return
        super().write_file(path, name, value)


def _waiting_command(entered: Path, release: Path) -> list[str]:
    return [
        sys.executable,
        "-u",
        "-c",
        """
from pathlib import Path
import sys
import time

entered, release = map(Path, sys.argv[1:3])
entered.touch()
while not release.exists():
    time.sleep(0.01)
""",
        str(entered),
        str(release),
    ]


def test_directional_limits_and_weight_apply_before_code_and_meter_rates(
    tmp_path: Path,
):
    root = tmp_path / "delegated"
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    system = IoControllerFakeCgroupV2System(root)
    resolver = StaticIoResolver()
    state_dir = tmp_path / "state"
    backend = CgroupV2Backend(
        root,
        state_dir=state_dir,
        system=system,
        io_paths=[scratch],
        io_resolver=resolver,
    )
    requested = {
        "read_bps": 8 * MIB,
        "write_bps": 6 * MIB,
        "read_iops": 80,
        "write_iops": 60,
        "io_weight": 250,
    }
    running = RunningReferenceBroker(
        state_dir,
        capacities={"jobs": 1, **requested},
        resource_bindings=IO_BINDINGS,
        resource_backends={"cgroup-v2": backend},
    )
    client = running.start()
    repository = _repository(tmp_path / "repository")
    entered = tmp_path / "entered"
    release = tmp_path / "release"

    try:
        run_id = client.submit(
            _waiting_command(entered, release),
            checkout=str(repository),
            resources=requested,
            caller_pid=os.getpid(),
            environment=caller_environment(),
        )
        wait_for(entered.exists, "the I/O-limited command did not start")
        leaves = [path for path in system.groups() if path.name.startswith("run-")]
        assert len(leaves) == 1
        leaf = leaves[0]
        owner = leaf.parent
        assert system.enabled[root] == {"io"}
        assert system.enabled[owner] == {"io"}
        assert system.io_limits[root] == {}
        assert system.io_limits[owner] == {}
        assert system.io_limits[leaf] == {
            "7:31": {
                "rbps": str(8 * MIB),
                "wbps": str(6 * MIB),
                "riops": "80",
                "wiops": "60",
            }
        }
        assert system.io_weights[root] == {}
        assert system.io_weights[owner] == {}
        assert system.io_weights[leaf] == {"7:31": 250}
        assert resolver.calls == [(scratch,), (scratch,)]

        system.now_ns = 2_000_000_000
        system.set_io_stats(
            leaf,
            {
                "7:31": {
                    "rbytes": 10 * MIB,
                    "wbytes": 8 * MIB,
                    "rios": 100,
                    "wios": 80,
                    "dbytes": 0,
                    "dios": 0,
                }
            },
        )

        def measured() -> bool:
            return client.status(run_id)["resource_receipt"]["peak"] == {
                "read_bps": 5 * MIB,
                "write_bps": 4 * MIB,
                "read_iops": 50,
                "write_iops": 40,
            }

        wait_for(measured, "io.stat rates were not retained in the receipt")
        release.touch()
        finished = wait_for(
            lambda: _terminal(client, run_id),
            "the I/O-limited command did not finish",
        )
        assert finished["status"] == "passed"
        assert finished["resource_receipt"]["applied"] == requested
        assert measured()
        assert system.groups() == set()
    finally:
        release.touch()
        running.stop()
        system.close()


def test_sibling_io_limits_do_not_mutate_parent_or_each_other(tmp_path: Path):
    root = tmp_path / "delegated"
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    system = IoControllerFakeCgroupV2System(root)
    backend = CgroupV2Backend(
        root,
        state_dir=tmp_path / "state",
        system=system,
        io_paths=[scratch],
        io_resolver=StaticIoResolver(),
    )
    binding = {"write_bps": IO_BINDINGS["write_bps"]}
    running = RunningReferenceBroker(
        tmp_path / "state",
        capacities={"jobs": 2, "write_bps": 12 * MIB},
        resource_bindings=binding,
        resource_backends={"cgroup-v2": backend},
    )
    client = running.start()
    first_repository = _repository(tmp_path / "first-repository")
    second_repository = _repository(tmp_path / "second-repository")
    first_entered = tmp_path / "first-entered"
    second_entered = tmp_path / "second-entered"
    first_release = tmp_path / "first-release"
    second_release = tmp_path / "second-release"

    try:
        first_id = client.submit(
            _waiting_command(first_entered, first_release),
            checkout=str(first_repository),
            resources={"write_bps": 4 * MIB},
            caller_pid=os.getpid(),
            environment=caller_environment(),
        )
        second_id = client.submit(
            _waiting_command(second_entered, second_release),
            checkout=str(second_repository),
            resources={"write_bps": 8 * MIB},
            caller_pid=os.getpid(),
            environment=caller_environment(),
        )
        wait_for(
            lambda: first_entered.exists() and second_entered.exists(),
            "the sibling I/O runs did not overlap",
        )
        leaves = [path for path in system.groups() if path.name.startswith("run-")]
        assert len(leaves) == 2

        def leaf_for(run_id: str) -> Path:
            worker_pid = client.status(run_id)["worker_pid"]
            assert isinstance(worker_pid, int)
            return next(path for path in leaves if worker_pid in system.members(path))

        first_leaf = leaf_for(first_id)
        second_leaf = leaf_for(second_id)
        assert system.io_limits[first_leaf]["7:31"]["wbps"] == str(4 * MIB)
        assert system.io_limits[second_leaf]["7:31"]["wbps"] == str(8 * MIB)
        assert system.io_limits[root] == {}
        assert system.io_limits[first_leaf.parent] == {}
        first_release.touch()
        second_release.touch()
        assert wait_for(lambda: _terminal(client, first_id), "first I/O run stuck")[
            "status"
        ] == "passed"
        assert wait_for(lambda: _terminal(client, second_id), "second I/O run stuck")[
            "status"
        ] == "passed"
    finally:
        first_release.touch()
        second_release.touch()
        running.stop()
        system.close()


@pytest.mark.parametrize(
    ("cancel", "expected_status"),
    [(False, "passed"), (True, "cancelled")],
)
def test_final_io_statistics_survive_completion_and_leaf_cleanup(
    tmp_path: Path,
    cancel: bool,
    expected_status: str,
):
    root = tmp_path / "delegated"
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    system = IoControllerFakeCgroupV2System(root)
    state_dir = tmp_path / "state"
    backend = CgroupV2Backend(
        root,
        state_dir=state_dir,
        system=system,
        io_paths=[scratch],
        io_resolver=StaticIoResolver(),
    )
    binding = {"write_bps": IO_BINDINGS["write_bps"]}
    running = RunningReferenceBroker(
        state_dir,
        capacities={"jobs": 1, "write_bps": 8 * MIB},
        resource_bindings=binding,
        resource_backends={"cgroup-v2": backend},
    )
    client = running.start()
    repository = _repository(tmp_path / "repository")
    entered = tmp_path / "entered"
    release = tmp_path / "release"

    try:
        run_id = client.submit(
            _waiting_command(entered, release),
            checkout=str(repository),
            resources={"write_bps": 8 * MIB},
            caller_pid=os.getpid(),
            environment=caller_environment(),
        )
        wait_for(entered.exists, "the cancellable I/O run did not start")
        leaf = next(path for path in system.groups() if path.name.startswith("run-"))
        system.terminal_io[leaf] = (
            2_000_000_000,
            {
                "7:31": {
                    "rbytes": 0,
                    "wbytes": 6 * MIB,
                    "rios": 0,
                    "wios": 24,
                    "dbytes": 0,
                    "dios": 0,
                }
            },
        )
        if cancel:
            client.cancel(run_id)
        else:
            release.touch()
        finished = wait_for(
            lambda: _terminal(client, run_id),
            "the terminal I/O run did not finish",
        )
        assert finished["status"] == expected_status
        assert finished["resource_receipt"]["peak"] == {
            "write_bps": 3 * MIB
        }
        assert system.groups() == set()
    finally:
        release.touch()
        running.stop()
        system.close()


@pytest.mark.parametrize(
    ("mode", "expected_status", "expected_exit", "ran"),
    [
        ("required", "failed", 125, False),
        ("best-effort", "passed", 0, True),
    ],
)
def test_ambiguous_device_resolution_obeys_enforcement_mode(
    tmp_path: Path,
    mode: str,
    expected_status: str,
    expected_exit: int,
    ran: bool,
):
    root = tmp_path / "delegated"
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    system = IoControllerFakeCgroupV2System(root)
    resolver = StaticIoResolver(
        error=CgroupV2Error(
            "io-device-ambiguous",
            "the configured path has no safe single backing device",
        )
    )
    state_dir = tmp_path / "state"
    backend = CgroupV2Backend(
        root,
        state_dir=state_dir,
        system=system,
        io_paths=[scratch],
        io_resolver=resolver,
    )
    binding = {
        "write_bps": {**IO_BINDINGS["write_bps"], "mode": mode},
    }
    running = RunningReferenceBroker(
        state_dir,
        capacities={"jobs": 1, "write_bps": MIB},
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
            resources={"write_bps": MIB},
            caller_pid=os.getpid(),
            environment=caller_environment(),
        )
        finished = wait_for(
            lambda: _terminal(client, run_id),
            "the ambiguous I/O device run did not finish",
        )
        assert finished["status"] == expected_status
        assert finished["exit_status"] == expected_exit
        assert marker.exists() is ran
        assert finished["resource_receipt"]["applied"] == {}
        assert {
            event["code"] for event in finished["resource_receipt"]["events"]
        } == {"io-device-ambiguous"}
        assert system.groups() == set()
    finally:
        running.stop()
        system.close()


@pytest.mark.parametrize(
    ("filesystem", "source"),
    [("overlay", "overlay"), ("nfs", "server:/scratch")],
)
def test_virtual_or_network_path_is_refused_without_guessing_a_device(
    tmp_path: Path,
    filesystem: str,
    source: str,
):
    mountinfo = tmp_path / "mountinfo"
    mountinfo.write_text(
        f"42 1 0:99 / {tmp_path} rw,relatime - {filesystem} {source} rw\n",
        encoding="utf-8",
    )
    resolver = LinuxIoDeviceResolver(
        mountinfo=mountinfo,
        sys_dev_block=tmp_path / "sys-dev-block",
    )

    with pytest.raises(CgroupV2Error) as raised:
        resolver.resolve((tmp_path,))
    assert raised.value.code == "io-filesystem-unsupported"


def test_stacked_mounts_at_one_path_are_refused_as_ambiguous(tmp_path: Path):
    mountinfo = tmp_path / "mountinfo"
    mountinfo.write_text(
        "\n".join(
            [
                f"42 1 7:31 / {tmp_path} rw - ext4 /dev/loop31 rw",
                f"43 1 8:0 / {tmp_path} rw - xfs /dev/sda rw",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    resolver = LinuxIoDeviceResolver(
        mountinfo=mountinfo,
        sys_dev_block=tmp_path / "sys-dev-block",
    )

    with pytest.raises(CgroupV2Error) as raised:
        resolver.resolve((tmp_path,))
    assert raised.value.code == "io-mount-ambiguous"


def test_cgroup_io_paths_are_loaded_from_the_single_broker_configuration():
    parsed = parse_broker_config(
        json.dumps(
            {
                "cgroup_root": "/sys/fs/cgroup/example",
                "cgroup_io": {"paths": ["/srv/agcoord-scratch"]},
            }
        )
    )
    assert parsed.cgroup_io == {"paths": ["/srv/agcoord-scratch"]}


@pytest.mark.parametrize(
    "section",
    [
        {},
        {"paths": []},
        {"paths": "/srv/agcoord-scratch"},
        {"paths": ["relative/scratch"]},
        {"paths": ["/srv/agcoord-scratch", "/srv/agcoord-scratch/"]},
        {"paths": ["/srv/agcoord-scratch"], "device": "7:31"},
    ],
)
def test_malformed_cgroup_io_configuration_is_rejected_when_loaded(
    section: object,
):
    with pytest.raises(BrokerConfigError, match="cgroup_io"):
        parse_broker_config(json.dumps({"cgroup_io": section}))


def _io_request(
    run_id: str,
    resources: dict[str, int],
    bindings: dict[str, dict[str, object]],
) -> ResourceRequest:
    return ResourceRequest.build(run_id, "cgroup-v2", resources, bindings)


def test_recovery_refuses_when_the_configured_device_identity_changes(
    tmp_path: Path,
):
    root = tmp_path / "delegated"
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    system = IoControllerFakeCgroupV2System(root)
    state_dir = tmp_path / "state"
    request = _io_request(
        "check-io-recovery",
        {"write_bps": MIB},
        {"write_bps": IO_BINDINGS["write_bps"]},
    )
    original = CgroupV2Backend(
        root,
        state_dir=state_dir,
        system=system,
        io_paths=[scratch],
        io_resolver=StaticIoResolver(),
    )
    handle = original.prepare(request)
    assert handle["io_devices"] == [{"device": "7:31", "filesystem": "ext4"}]
    replacement = CgroupV2Backend(
        root,
        state_dir=state_dir,
        system=system,
        io_paths=[scratch],
        io_resolver=StaticIoResolver(),
    )

    try:
        assert replacement.prepare(request) == handle
        changed = CgroupV2Backend(
            root,
            state_dir=state_dir,
            system=system,
            io_paths=[scratch],
            io_resolver=StaticIoResolver((IoDevice("7:32", "ext4"),)),
        )
        with pytest.raises(CgroupV2Error) as raised:
            changed.prepare(request)
        assert raised.value.code == "io-device-changed"
    finally:
        replacement.cleanup(request, handle)
        system.close()


def test_symmetric_and_directional_limits_cannot_claim_the_same_control(
    tmp_path: Path,
):
    root = tmp_path / "delegated"
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    system = IoControllerFakeCgroupV2System(root)
    backend = CgroupV2Backend(
        root,
        state_dir=tmp_path / "state",
        system=system,
        io_paths=[scratch],
        io_resolver=StaticIoResolver(),
    )
    request = _io_request(
        "check-overlapping-io-controls",
        {"all_bps": MIB, "read_bps": MIB},
        {
            "all_bps": {
                "backend": "cgroup-v2",
                "kind": "io-bandwidth",
                "mode": "required",
                "unit": "bytes-per-second",
            },
            "read_bps": IO_BINDINGS["read_bps"],
        },
    )

    try:
        with pytest.raises(CgroupV2Error) as raised:
            backend.prepare(request)
        assert raised.value.code == "controller-ambiguous"
        assert system.groups() == set()
    finally:
        system.close()


def test_symmetric_io_units_apply_the_same_limit_to_both_directions(
    tmp_path: Path,
):
    root = tmp_path / "delegated"
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    system = IoControllerFakeCgroupV2System(root)
    backend = CgroupV2Backend(
        root,
        state_dir=tmp_path / "state",
        system=system,
        io_paths=[scratch],
        io_resolver=StaticIoResolver(),
    )
    request = _io_request(
        "check-symmetric-io-controls",
        {"bandwidth": MIB, "operations": 40},
        {
            "bandwidth": {
                "backend": "cgroup-v2",
                "kind": "io-bandwidth",
                "mode": "required",
                "unit": "bytes-per-second",
            },
            "operations": {
                "backend": "cgroup-v2",
                "kind": "io-operations",
                "mode": "required",
                "unit": "operations-per-second",
            },
        },
    )
    handle = backend.prepare(request)

    try:
        leaf = next(path for path in system.groups() if path.name.startswith("run-"))
        assert system.io_limits[leaf] == {
            "7:31": {
                "rbps": str(MIB),
                "wbps": str(MIB),
                "riops": "40",
                "wiops": "40",
            }
        }
    finally:
        backend.cleanup(request, handle)
        system.close()


def test_io_weight_outside_the_kernel_range_is_refused(tmp_path: Path):
    root = tmp_path / "delegated"
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    system = IoControllerFakeCgroupV2System(root)
    backend = CgroupV2Backend(
        root,
        state_dir=tmp_path / "state",
        system=system,
        io_paths=[scratch],
        io_resolver=StaticIoResolver(),
    )
    request = _io_request(
        "check-invalid-io-weight",
        {"io_weight": 10_001},
        {"io_weight": IO_BINDINGS["io_weight"]},
    )

    try:
        with pytest.raises(CgroupV2Error) as raised:
            backend.prepare(request)
        assert raised.value.code == "io-weight-invalid"
        assert system.groups() == set()
    finally:
        system.close()


@pytest.mark.parametrize(
    ("ignored", "name", "units"),
    [("io.max", "write_bps", MIB), ("io.weight", "io_weight", 250)],
)
def test_unverified_io_controller_value_is_refused_before_attach(
    tmp_path: Path,
    ignored: str,
    name: str,
    units: int,
):
    root = tmp_path / "delegated"
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    system = IgnoringIoControllerFakeCgroupV2System(root, ignored=ignored)
    backend = CgroupV2Backend(
        root,
        state_dir=tmp_path / "state",
        system=system,
        io_paths=[scratch],
        io_resolver=StaticIoResolver(),
    )
    request = _io_request(
        f"check-unverified-{ignored}",
        {name: units},
        {name: IO_BINDINGS[name]},
    )

    try:
        with pytest.raises(CgroupV2Error) as raised:
            backend.prepare(request)
        assert raised.value.code == "controller-value-unverified"
        assert system.groups() == set()
    finally:
        system.close()


def test_required_io_controller_refusal_happens_before_user_code(tmp_path: Path):
    root = tmp_path / "delegated"
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    system = IoControllerFakeCgroupV2System(root, controllers=set())
    state_dir = tmp_path / "state"
    backend = CgroupV2Backend(
        root,
        state_dir=state_dir,
        system=system,
        io_paths=[scratch],
        io_resolver=StaticIoResolver(),
    )
    running = RunningReferenceBroker(
        state_dir,
        capacities={"jobs": 1, "write_bps": MIB},
        resource_bindings={"write_bps": IO_BINDINGS["write_bps"]},
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
                "open(__import__('sys').argv[1], 'w').close()",
                str(marker),
            ],
            checkout=str(repository),
            resources={"write_bps": MIB},
            caller_pid=os.getpid(),
            environment=caller_environment(),
        )
        finished = wait_for(
            lambda: _terminal(client, run_id),
            "the unavailable I/O controller run did not finish",
        )
        assert finished["status"] == "failed"
        assert finished["exit_status"] == 125
        assert not marker.exists()
        assert "kind-unsupported" in {
            event["code"] for event in finished["resource_receipt"]["events"]
        }
        assert system.groups() == set()
    finally:
        running.stop()
        system.close()


@pytest.mark.skipif(
    os.environ.get("AGCOORD_TEST_CGROUP_IO") != "1"
    or not os.environ.get("AGCOORD_TEST_CGROUP_ROOT"),
    reason=(
        "set AGCOORD_TEST_CGROUP_IO=1 and AGCOORD_TEST_CGROUP_ROOT to an "
        "exclusive writable delegation while running as init-namespace root"
    ),
)
def test_real_loop_ext4_limits_buffered_bandwidth_and_direct_iops(
    tmp_path: Path,
):
    for command in ("dd", "mkfs.ext4", "mount", "umount"):
        assert shutil.which(command), f"{command} is required for the real I/O test"
    root = Path(os.environ["AGCOORD_TEST_CGROUP_ROOT"]).resolve(strict=True)
    image = tmp_path / "block-io.ext4"
    mountpoint = tmp_path / "mounted"
    mountpoint.mkdir()
    image.touch()
    os.truncate(image, 128 * MIB)
    subprocess.run(["mkfs.ext4", "-q", "-F", str(image)], check=True)
    mounted = False
    running: RunningReferenceBroker | None = None
    try:
        subprocess.run(
            ["mount", "-o", "loop", str(image), str(mountpoint)],
            check=True,
        )
        mounted = True
        mountpoint.chmod(0o777)
        resolver = LinuxIoDeviceResolver()
        devices = resolver.resolve((mountpoint,))
        assert len(devices) == 1
        assert devices[0].filesystem == "ext4"

        state_dir = tmp_path / "state"
        backend = CgroupV2Backend(
            root,
            state_dir=state_dir,
            io_paths=[mountpoint],
            io_resolver=resolver,
        )
        bindings = {
            "write_bps": IO_BINDINGS["write_bps"],
            "write_iops": IO_BINDINGS["write_iops"],
        }
        running = RunningReferenceBroker(
            state_dir,
            capacities={"jobs": 1, "write_bps": MIB, "write_iops": 16},
            resource_bindings=bindings,
            resource_backends={"cgroup-v2": backend},
        )
        client = running.start()
        repository = _repository(tmp_path / "repository")

        buffered_report = tmp_path / "buffered.json"
        buffered_id = client.submit(
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

target = Path(os.environ["IO_SCRATCH"]) / "buffered"
started = time.monotonic()
with target.open("wb", buffering=0) as output:
    for _ in range(8):
        output.write(b"x" * (1024 * 1024))
    os.fsync(output.fileno())
elapsed = time.monotonic() - started
Path(sys.argv[1]).write_text(json.dumps({"elapsed": elapsed}), encoding="utf-8")
""",
                str(buffered_report),
            ],
            checkout=str(repository),
            resources={"write_bps": MIB},
            caller_pid=os.getpid(),
            environment={**caller_environment(), "IO_SCRATCH": str(mountpoint)},
        )
        buffered = wait_for(
            lambda: _terminal(client, buffered_id),
            "the real buffered bandwidth workload did not finish",
            timeout=30,
        )
        assert buffered["status"] == "passed"
        assert buffered["resource_receipt"]["applied"] == {"write_bps": MIB}
        assert buffered["resource_receipt"]["peak"]["write_bps"] > 0
        assert json.loads(buffered_report.read_text(encoding="utf-8"))["elapsed"] >= 3

        direct_report = tmp_path / "direct.json"
        direct_id = client.submit(
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

target = Path(os.environ["IO_SCRATCH"]) / "direct"
started = time.monotonic()
subprocess.run([
    "dd", "if=/dev/zero", f"of={target}", "bs=4096", "count=128",
    "oflag=direct", "status=none",
], check=True)
elapsed = time.monotonic() - started
Path(sys.argv[1]).write_text(json.dumps({"elapsed": elapsed}), encoding="utf-8")
""",
                str(direct_report),
            ],
            checkout=str(repository),
            resources={"write_iops": 16},
            caller_pid=os.getpid(),
            environment={**caller_environment(), "IO_SCRATCH": str(mountpoint)},
        )
        direct = wait_for(
            lambda: _terminal(client, direct_id),
            "the real direct IOPS workload did not finish",
            timeout=30,
        )
        assert direct["status"] == "passed"
        assert direct["resource_receipt"]["applied"] == {"write_iops": 16}
        assert direct["resource_receipt"]["peak"]["write_iops"] > 0
        assert json.loads(direct_report.read_text(encoding="utf-8"))["elapsed"] >= 3
    finally:
        if running is not None:
            running.stop()
        if mounted:
            subprocess.run(["umount", str(mountpoint)], check=True)
