"""The resource contract carried across the overflow placement boundary."""

import json

import pytest

from agcoord.overflow_image import (
    ContainerLimits,
    container_environment,
    container_limits,
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
