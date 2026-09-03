"""The optional `exec` on a scratch binding is the operator's machine decision (#177)."""

from __future__ import annotations

import pytest

from agcoord.resources import ResourceContractError, validate_resource_bindings

TMPFS = {"backend": "cgroup-v2", "kind": "tmpfs", "mode": "required", "unit": "bytes"}
MEMORY = {"backend": "cgroup-v2", "kind": "memory", "mode": "required", "unit": "bytes"}


def test_a_tmpfs_binding_may_opt_in_to_executable_scratch():
    bindings = validate_resource_bindings({"scratch": {**TMPFS, "exec": True}})

    assert bindings == {"scratch": {**TMPFS, "exec": True}}


def test_exec_is_absent_from_a_contract_unless_set():
    """Every existing configuration and every stored contract keeps its exact shape."""
    assert validate_resource_bindings({"scratch": TMPFS}) == {"scratch": TMPFS}
    assert validate_resource_bindings({"scratch": {**TMPFS, "exec": False}}) == {"scratch": TMPFS}


@pytest.mark.parametrize(
    ("binding", "message"),
    [
        ({**MEMORY, "exec": True}, "only a tmpfs binding may"),
        ({**TMPFS, "exec": "yes"}, "must be true or false"),
        ({**TMPFS, "exec": 1}, "must be true or false"),
        ({**TMPFS, "executable": True}, "plus an optional exec"),
    ],
)
def test_exec_misuse_is_refused_naming_the_binding(binding, message):
    with pytest.raises(ResourceContractError, match=message) as refused:
        validate_resource_bindings({"scratch": binding})

    assert "'scratch'" in str(refused.value)
