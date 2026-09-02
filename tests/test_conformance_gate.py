from __future__ import annotations

import copy
import json
from pathlib import Path
import subprocess
import sys

import pytest


ROOT = Path(__file__).resolve().parent.parent
MANIFEST = ROOT / "conformance" / "manifest-v1.json"
CHECKER = ROOT / "scripts" / "check-conformance"


def _run(*arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(CHECKER), *arguments],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )


def _write_manifest(tmp_path: Path, document: dict[str, object]) -> Path:
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(document), encoding="utf-8")
    return path


def test_versioned_conformance_manifest_passes_its_public_validator():
    completed = _run("--validate-only")

    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.strip() == (
        "conformance manifest v1 passed: 23 behaviors, 2 intentional differences"
    )
    assert completed.stderr == ""


def test_public_checker_lists_both_implementation_selector_sets():
    completed = _run("--list-selectors")

    assert completed.returncode == 0, completed.stderr
    selectors = json.loads(completed.stdout)
    assert set(selectors) == {"python_reference", "rust_native"}
    assert len(selectors["python_reference"]) == 24
    assert len(selectors["rust_native"]) == 25
    assert (
        "tests/test_queue.py::test_submit_and_snapshot_have_the_strict_generic_schema"
        in selectors["python_reference"]
    )
    assert (
        "tests/test_queue.py::test_land_target_sync_updates_the_durable_head_before_the_gate"
        in selectors["python_reference"]
    )
    assert (
        "client_compatibility::python_public_commands_keep_the_protocol_five_json_contract"
        in selectors["rust_native"]
    )


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (
            lambda document: document.update(manifest_version=2),
            "manifest_version must be exactly 1",
        ),
        (
            lambda document: document["required_domains"].pop(),
            "required_domains must exactly match",
        ),
        (
            lambda document: document["behaviors"].pop(),
            "do not cover domains: no_unverified_enforcement",
        ),
        (
            lambda document: document["behaviors"][0]["tests"].update(
                rust_native=[]
            ),
            "rust_native must be a non-empty unique string list",
        ),
        (
            lambda document: document["behaviors"][0]["tests"].update(
                python_reference=["tests/test_queue.py::not_a_test"]
            ),
            "invalid selector",
        ),
        (
            lambda document: document.update(undeclared=True),
            "unknown undeclared",
        ),
    ],
)
def test_conformance_validator_fails_closed_for_incomplete_contracts(
    tmp_path: Path,
    mutate,
    message: str,
):
    document = copy.deepcopy(json.loads(MANIFEST.read_text(encoding="utf-8")))
    mutate(document)
    manifest = _write_manifest(tmp_path, document)

    completed = _run("--manifest", str(manifest), "--validate-only")

    assert completed.returncode == 1
    assert completed.stdout == ""
    assert completed.stderr.startswith("conformance refused: ")
    assert message in completed.stderr
