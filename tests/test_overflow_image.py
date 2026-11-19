"""The resource contract carried across the overflow placement boundary."""

import json

import pytest

from agcoord.overflow_image import (
    ContainerLimits,
    container_command,
    container_environment,
    container_limits,
    container_security_options,
    placement,
)
from agcoord.resources import resource_contract


@pytest.mark.parametrize("executable", [True, False])
@pytest.mark.parametrize("inodes", [None, 2000])
def test_container_limits_emit_the_requested_mount_options(executable, inodes):
    limits = ContainerLimits(tmpfs=1024, tmpfs_inodes=inodes, tmpfs_exec=executable)
    mount = "/tmp:rw," + ("exec," if executable else "") + "size=1024"
    if inodes is not None:
        mount += f",nr_inodes={inodes}"
    assert limits.run_options() == [f"--tmpfs={mount}"]


def test_container_limits_default_to_nonexecutable_scratch():
    assert ContainerLimits(tmpfs=1024).run_options() == ["--tmpfs=/tmp:rw,size=1024"]


@pytest.mark.parametrize("executable", [True, False, None])
@pytest.mark.parametrize("inodes", [None, 2000])
def test_placement_preserves_scratch_executability(executable, inodes):
    binding = {"backend": "cgroup-v2", "kind": "tmpfs", "mode": "required", "unit": "bytes"}
    if executable is not None:
        binding["exec"] = executable
    resources = {"scratch": 1024}
    bindings = {"scratch": binding}
    if inodes is not None:
        resources["files"] = inodes
        bindings["files"] = {
            "backend": "cgroup-v2", "kind": "inodes", "mode": "required", "unit": "inodes"
        }
    placed = placement({
        "AGCOORD_RUN_ID": "check-scratch",
        "AGCOORD_OVERFLOW_MAX_RUNTIME": "60",
        "AGCOORD_OVERFLOW_RESOURCES": json.dumps(resources),
        "AGCOORD_OVERFLOW_CONTRACT": json.dumps(resource_contract(resources, bindings)),
    })

    mount = "/tmp:rw," + ("exec," if executable else "") + "size=1024"
    if inodes is not None:
        mount += f",nr_inodes={inodes}"
    assert placed.limits.run_options() == [f"--tmpfs={mount}"]


def test_admission_only_and_unclaimed_scratch_do_not_create_a_mount():
    binding = {
        "backend": None, "kind": "tmpfs", "mode": "admission-only", "unit": "bytes", "exec": True
    }
    assert container_limits({"scratch": 1024}, {"scratch": binding}).run_options() == []
    assert container_limits({}, {"scratch": binding}).run_options() == []
    assert ContainerLimits().run_options() == []


@pytest.mark.parametrize("scratch", [None, 1024])
def test_scratch_environment_needs_no_explicit_forwarding(scratch):
    environment = container_environment(
        {"TMPDIR": "/host/scratch", "CI": "1"}, [], ContainerLimits(tmpfs=scratch)
    )

    assert environment == (
        {"TMPDIR": "/tmp", "TMP": "/tmp", "TEMP": "/tmp"} if scratch else {}
    )


def test_container_security_options_enable_no_new_privileges():
    assert container_security_options() == ["--security-opt=no-new-privileges"]


def test_container_command_enforces_umask_and_preserves_arguments():
    import os
    import subprocess
    import sys

    wrapped = container_command([sys.executable, "-c", "import os, sys; print(f'{os.umask(0o022):04o}', sys.argv[1:])", "a b", "c"])
    completed = subprocess.run(wrapped, capture_output=True, text=True, check=True)
    assert completed.stdout.strip() == "0077 ['a b', 'c']"


def test_container_command_merges_stderr_and_sequence_tags_chunks(tmp_path):
    import subprocess
    import sys

    wrapped = container_command(
        [
            sys.executable,
            "-c",
            (
                "import os, sys\n"
                "print(f'umask={os.umask(0o022):04o}', flush=True)\n"
                "print('err:%s\\\\n', file=sys.stderr, flush=True)\n"
                "print('', flush=True)\n"
                "print('Y' * 8500, flush=True)\n"
                "print(sys.argv[1:], file=sys.stderr, flush=True)\n"
                "sys.exit(9)\n"
            ),
            "x y",
            "z",
        ],
        log_token="abc123",
    )
    completed = subprocess.run(wrapped, capture_output=True, text=True)
    assert completed.returncode == 9
    assert completed.stderr == ""
    lines = completed.stdout.splitlines()
    assert lines[0] == "abc123:0:c:umask=0077"
    assert lines[1] == "abc123:1:c:err:%s\\n"
    assert lines[2] == "abc123:2:c:"
    assert lines[3] == "abc123:3:p:" + "Y" * 4000
    assert lines[4] == "abc123:4:p:" + "Y" * 4000
    assert lines[5] == "abc123:5:c:" + "Y" * 500
    assert lines[6] == "abc123:6:c:['x y', 'z']"

    # Also exercise the pure POSIX /bin/sh fallback when awk is absent on PATH.
    fallback = subprocess.run(
        wrapped,
        env={"PATH": str(tmp_path)},
        capture_output=True,
        text=True,
    )
    assert fallback.returncode == 9
    assert fallback.stdout.startswith("abc123:0:c:umask=0077\n")
    assert fallback.stdout.splitlines()[-1] == "abc123:4:c:['x y', 'z']"
