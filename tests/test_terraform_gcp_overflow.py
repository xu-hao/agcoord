"""Static checks of the Terraform module that provisions a GCP overflow project.

The formatting and schema checks need the ``terraform`` executable and a provider
download, so they are opt-in: set ``AGCOORD_TEST_TERRAFORM=1``.  They never contact a
cloud API and never read credentials.  Applying the module to a real project is an
operator action, not a test.
"""

from __future__ import annotations

import os
from pathlib import Path
import shutil
import subprocess

import pytest


MODULE = Path(__file__).resolve().parents[1] / "packaging" / "terraform" / "gcp-overflow"

needs_terraform = pytest.mark.skipif(
    os.environ.get("AGCOORD_TEST_TERRAFORM") != "1",
    reason="set AGCOORD_TEST_TERRAFORM=1 to check the Terraform module",
)


def _terraform(*arguments: str, cwd: Path) -> subprocess.CompletedProcess[str]:
    executable = shutil.which("terraform")
    assert executable is not None, "AGCOORD_TEST_TERRAFORM=1 requires terraform on PATH"
    environment = {
        name: value
        for name, value in os.environ.items()
        # Validation must not depend on, or be able to use, anyone's cloud credentials.
        if not name.startswith(("GOOGLE_", "CLOUDSDK_", "TF_VAR_"))
    }
    environment["TF_IN_AUTOMATION"] = "1"
    return subprocess.run(
        [executable, *arguments],
        cwd=cwd,
        env=environment,
        capture_output=True,
        text=True,
        timeout=600,
    )


@needs_terraform
def test_the_module_is_canonically_formatted():
    completed = _terraform("fmt", "-check", "-diff", "-recursive", cwd=MODULE)

    assert completed.returncode == 0, completed.stdout + completed.stderr


@needs_terraform
def test_the_module_is_valid_against_the_provider_schema(tmp_path: Path):
    # Work on a copy so the provider download never lands in the repository.
    module = tmp_path / "gcp-overflow"
    shutil.copytree(MODULE, module, ignore=shutil.ignore_patterns(".terraform*"))

    initialised = _terraform("init", "-backend=false", "-input=false", "-no-color", cwd=module)
    assert initialised.returncode == 0, initialised.stdout + initialised.stderr

    validated = _terraform("validate", "-no-color", cwd=module)
    assert validated.returncode == 0, validated.stdout + validated.stderr


def test_the_module_carries_no_operator_values_or_state():
    # The source distribution ships everything below packaging/, so a state or variable
    # file left in the module would publish an operator's project and billing account.
    names = {path.name for path in MODULE.rglob("*") if path.is_file()}

    assert names == {"main.tf", "outputs.tf", "variables.tf", "versions.tf"}
