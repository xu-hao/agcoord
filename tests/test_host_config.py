"""Host-derived broker configuration through the public `agc host config` command."""

from __future__ import annotations

import json
from pathlib import Path
import stat

import pytest

from agcoord import cli
from agcoord.config import parse_broker_config

MIB = 1024**2
GIB = 1024**3
CPU_BINDING = {
    "kind": "cpu",
    "unit": "logical-cpu",
    "mode": "required",
    "backend": "cgroup-v2",
}
MEMORY_BINDING = {
    "kind": "memory",
    "unit": "bytes",
    "mode": "required",
    "backend": "cgroup-v2",
}


def _delegated_root(tmp_path: Path, controllers: str) -> Path:
    """One fake delegated slice whose service leaf the broker would own."""
    slice_directory = tmp_path / "cgroup" / "app.slice"
    slice_directory.mkdir(parents=True)
    (slice_directory / "cgroup.controllers").write_text(f"{controllers}\n", encoding="utf-8")
    return slice_directory / "agcoord-broker.service"


def _total_memory() -> int:
    for line in Path("/proc/meminfo").read_text(encoding="utf-8").splitlines():
        if line.startswith("MemTotal:"):
            return int(line.split()[1]) * 1024
    raise AssertionError("this host reports no MemTotal")


def _configuration(capsys, *arguments: str) -> dict:
    assert cli.main(["host", "config", *arguments]) == 0
    printed = capsys.readouterr().out
    document = json.loads(printed)
    parse_broker_config(printed, source="agc host config")
    return document


def test_managed_configuration_declares_host_cpu_and_memory_with_enforced_bindings(
    capsys,
    tmp_path: Path,
):
    root = _delegated_root(tmp_path, "cpuset cpu io memory pids")

    document = _configuration(
        capsys,
        "--managed",
        "--cgroup-root",
        str(root),
        "--cpu",
        "8",
        "--reserve",
        "4GiB",
    )

    expected_memory = (_total_memory() - 4 * GIB) // MIB * MIB
    assert document["capacities"] == {"cpu": 8, "jobs": 8, "memory": expected_memory}
    assert document["bindings"] == {"cpu": CPU_BINDING, "memory": MEMORY_BINDING}
    assert document["cgroup_root"] == str(root)
    assert document["native_broker"] == {
        "path": "/usr/libexec/agcoord/agcoord-broker",
        "allow_development": False,
        "managed_service": True,
    }


def test_an_undelegated_memory_controller_leaves_memory_admission_only(capsys, tmp_path: Path):
    root = _delegated_root(tmp_path, "cpuset cpu pids")

    document = _configuration(
        capsys,
        "--managed",
        "--cgroup-root",
        str(root),
        "--cpu",
        "4",
        "--memory",
        "16GiB",
    )

    assert document["capacities"] == {"cpu": 4, "jobs": 4, "memory": 16 * GIB}
    assert document["bindings"] == {"cpu": CPU_BINDING}


def test_an_unmanaged_broker_is_configured_without_bindings_or_a_cgroup_root(
    capsys,
    tmp_path: Path,
):
    document = _configuration(capsys, "--user", "--cpu", "2", "--memory", "8GiB")

    assert document["capacities"] == {"cpu": 2, "jobs": 2, "memory": 8 * GIB}
    assert "bindings" not in document
    assert "cgroup_root" not in document
    assert document["native_broker"]["managed_service"] is False
    assert document["native_broker"]["path"].endswith("/.local/libexec/agcoord/agcoord-broker")


def test_scratch_is_opt_in_and_declares_its_atomic_tmpfs_policy(capsys, tmp_path: Path):
    root = _delegated_root(tmp_path, "cpu memory")

    document = _configuration(
        capsys,
        "--managed",
        "--cgroup-root",
        str(root),
        "--cpu",
        "8",
        "--memory",
        "16GiB",
        "--tmpfs",
        "4GiB",
    )

    assert document["capacities"]["tmpfs"] == 4 * GIB
    assert document["capacities"]["tmpfs_inodes"] == 4 * GIB // 8192
    assert document["bindings"]["tmpfs"] == {
        "kind": "tmpfs",
        "unit": "bytes",
        "mode": "required",
        "backend": "cgroup-v2",
    }
    assert document["bindings"]["tmpfs_inodes"] == {
        "kind": "inodes",
        "unit": "inodes",
        "mode": "required",
        "backend": "cgroup-v2",
    }


def test_scratch_without_enforcement_is_refused_rather_than_written_unenforced(
    capsys,
    tmp_path: Path,
):
    assert cli.main(["host", "config", "--user", "--tmpfs", "1GiB"]) != 0

    captured = capsys.readouterr()
    assert captured.out == ""
    assert "tmpfs" in captured.err


def test_write_installs_an_owner_only_configuration_and_refuses_to_replace_one(
    capsys,
    tmp_path: Path,
):
    root = _delegated_root(tmp_path, "cpu memory")
    state_dir = tmp_path / "state"
    arguments = [
        "host",
        "config",
        "--managed",
        "--cgroup-root",
        str(root),
        "--cpu",
        "8",
        "--memory",
        "16GiB",
        "--state-dir",
        str(state_dir),
        "--write",
    ]

    assert cli.main(arguments) == 0
    capsys.readouterr()
    written = state_dir / "config.json"
    assert stat.S_IMODE(state_dir.stat().st_mode) == 0o700
    assert stat.S_IMODE(written.stat().st_mode) == 0o600
    first = written.read_text(encoding="utf-8")
    assert json.loads(first)["capacities"]["memory"] == 16 * GIB

    assert cli.main(arguments) != 0
    assert "--force" in capsys.readouterr().err
    assert written.read_text(encoding="utf-8") == first

    assert cli.main([*arguments, "--force", "--memory", "8GiB"]) == 0
    capsys.readouterr()
    assert json.loads(written.read_text(encoding="utf-8"))["capacities"]["memory"] == 8 * GIB


@pytest.mark.parametrize("size", ["", "12x", "-4GiB", "GiB"])
def test_an_unreadable_size_is_refused_before_anything_is_written(capsys, size: str):
    try:
        refused = cli.main(["host", "config", "--user", "--memory", size])
    except SystemExit as stopped:  # a size that reads as an option is refused by the parser
        refused = stopped.code

    assert refused != 0
    assert capsys.readouterr().out == ""
