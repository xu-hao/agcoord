"""Static checks of the Terraform module that provisions a GCP overflow project.

The formatting, schema, and mock-provider checks need the ``terraform`` executable
(1.7 or later for ``terraform test`` mocks) and a provider download, so they are opt-in:
set ``AGCOORD_TEST_TERRAFORM=1``.  They never contact a
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


# Run against a mock provider, so nothing is created and no credentials are needed.
RESULT_CACHE_TEST = """
mock_provider "google" {
  override_resource {
    target = google_service_account.runner
    values = {
      name   = "projects/overflow-example/serviceAccounts/agcoord-overflow-runner@overflow-example.iam.gserviceaccount.com"
      member = "serviceAccount:agcoord-overflow-runner@overflow-example.iam.gserviceaccount.com"
    }
  }
}

variables {
  project_id      = "overflow-example"
  billing_account = "000000-000000-000000"
  region          = "europe-west4"
  submitters      = ["user:alice@example.com", "group:builders@example.com"]
}

run "result_cache_is_private_and_expires" {
  command = apply

  assert {
    condition     = output.result_cache_url == "gs://overflow-example-result-cache"
    error_message = "result_cache_url does not name the project's result-cache bucket"
  }

  assert {
    condition = (
      google_storage_bucket.result_cache.location == "EUROPE-WEST4" ||
      google_storage_bucket.result_cache.location == "europe-west4"
    )
    error_message = "the result cache is not in the module's region"
  }

  assert {
    condition     = google_storage_bucket.result_cache.uniform_bucket_level_access
    error_message = "the result cache allows per-object ACLs"
  }

  assert {
    condition     = google_storage_bucket.result_cache.public_access_prevention == "enforced"
    error_message = "the result cache can be made public"
  }

  assert {
    condition = (
      one(one(google_storage_bucket.result_cache.lifecycle_rule).action).type == "Delete" &&
      one(one(google_storage_bucket.result_cache.lifecycle_rule).condition).age == 30
    )
    error_message = "result-cache objects are not deleted after the default 30 days"
  }

  assert {
    condition     = google_project_service.enabled["storage.googleapis.com"].service == "storage.googleapis.com"
    error_message = "the Cloud Storage API is not enabled"
  }

  assert {
    condition = (
      google_storage_bucket_iam_member.runner_uses_result_cache.role == "roles/storage.objectAdmin" &&
      google_storage_bucket_iam_member.runner_uses_result_cache.member == "serviceAccount:agcoord-overflow-runner@overflow-example.iam.gserviceaccount.com"
    )
    error_message = "the runner cannot read and write the result cache"
  }

  assert {
    condition = (
      toset([for grant in google_storage_bucket_iam_member.submitter_uses_result_cache : grant.member]) ==
      toset(["user:alice@example.com", "group:builders@example.com"]) &&
      alltrue([for grant in google_storage_bucket_iam_member.submitter_uses_result_cache : grant.role == "roles/storage.objectAdmin"])
    )
    error_message = "the result cache's writers are not exactly the runner and the submitters"
  }
}

run "result_cache_retention_is_configurable" {
  command = apply

  variables {
    result_cache_retention_days = 7
  }

  assert {
    condition     = one(one(google_storage_bucket.result_cache.lifecycle_rule).condition).age == 7
    error_message = "result_cache_retention_days does not set the result cache's retention"
  }
}

run "result_cache_retention_must_be_positive" {
  command = plan

  variables {
    result_cache_retention_days = 0
  }

  expect_failures = [var.result_cache_retention_days]
}
"""


@needs_terraform
def test_the_result_cache_is_private_written_only_by_runner_and_submitters(tmp_path: Path):
    module = tmp_path / "gcp-overflow"
    shutil.copytree(MODULE, module, ignore=shutil.ignore_patterns(".terraform*"))
    (module / "tests").mkdir()
    (module / "tests" / "result_cache.tftest.hcl").write_text(RESULT_CACHE_TEST)

    initialised = _terraform("init", "-backend=false", "-input=false", "-no-color", cwd=module)
    assert initialised.returncode == 0, initialised.stdout + initialised.stderr

    tested = _terraform("test", "-no-color", cwd=module)
    assert tested.returncode == 0, tested.stdout + tested.stderr


def test_the_module_carries_no_operator_values_or_state():
    # The source distribution ships everything below packaging/, so a state or variable
    # file left in the module would publish an operator's project and billing account.
    names = {path.name for path in MODULE.rglob("*") if path.is_file()}

    assert names == {"main.tf", "outputs.tf", "variables.tf", "versions.tf"}
