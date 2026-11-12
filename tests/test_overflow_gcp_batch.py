"""Behavior of the Google Cloud Batch overflow executor against a test-owned cloud.

The fake cloud is described in ``gcp_fakes``.  The executor itself runs as the real
``python -m agcoord overflow-gcp-batch`` subprocess the broker would start.  Nothing here
contacts a cloud API: running a real Batch job is a benchmark, not a test.
"""

from __future__ import annotations

import io
import json
from pathlib import Path
import signal
import subprocess
import sys
import tarfile

import pytest

from gcp_fakes import (
    GCLOUD_PROGRESS,
    JOB_IMAGE,
    JOB_IMAGE_DIGEST,
    MEMORY,
    PROJECT,
    RESOURCES,
    RUN_ID,
    RUNNER,
    TOOLCHAIN_IMAGE,
    Cloud,
    entry,
)


JOB = f"projects/{PROJECT}/locations/us-central1/jobs/agcoord-{RUN_ID}"


@pytest.fixture
def cloud(tmp_path: Path) -> Cloud:
    return Cloud(tmp_path, "overflow-gcp-batch")


def test_the_command_runs_as_one_batch_job_sized_to_the_claim(cloud: Cloud):
    cloud.answer(
        "logs",
        [
            [],
            [entry("a1", "collected 3 items"), entry("a2", "...\n")],
            # Cloud Logging returns an entry again when the next read starts at its time.
            [entry("a2", "...\n"), entry("a3", "3 passed")],
            [],
        ],
    )

    completed = cloud.run("python", "-m", "pytest", "-q")

    assert completed.returncode == 0, completed.stderr
    assert completed.stdout == "collected 3 items\n...\n3 passed\n"

    job = cloud.job()
    (group,) = job["taskGroups"]
    task = group["taskSpec"]
    assert group["taskCount"] == 1
    assert task["runnables"] == [
        {
            "container": {
                # The machine pulls exactly the image that was built for this job.
                "imageUri": JOB_IMAGE_DIGEST,
                "entrypoint": "python",
                "commands": ["-m", "pytest", "-q"],
                "options": f"--cpus=4 --memory={MEMORY} --memory-swap={MEMORY}",
            }
        }
    ]
    assert task["computeResource"] == {"cpuMilli": 4000, "memoryMib": 3072}
    # The machine stops the task itself at the pool's limit, and never runs it twice.
    assert task["maxRunDuration"] == "900s"
    assert task["maxRetryCount"] == 0
    assert job["allocationPolicy"] == {
        "instances": [{"policy": {"provisioningModel": "STANDARD"}}],
        "location": {"allowedLocations": ["regions/us-central1"]},
        "serviceAccount": {"email": RUNNER},
    }
    assert job["logsPolicy"] == {"destination": "CLOUD_LOGGING"}

    submit = cloud.wait_for_gcloud("batch", "jobs", "submit")
    assert submit[3] == f"agcoord-{RUN_ID}"
    assert {"--location=us-central1", f"--project={PROJECT}", "--config=-"} <= set(submit)
    reads = [call for call in cloud.gcloud() if call[:2] == ["logging", "read"]]
    assert 'labels.job_uid="job-uid-1"' in reads[0][2]
    assert 'log_id("batch_task_logs")' in reads[0][2]

    # The source layer holds the working tree without ignored files.
    stream = io.BytesIO(cloud.state.joinpath("context.tar").read_bytes())
    with tarfile.open(fileobj=stream) as context:
        names = set(context.getnames())
    assert "workspace/tracked.txt" in names
    assert not any("secret.env" in name for name in names)

    # The toolchain and the job image are pushed under the package names the registry's
    # cleanup policies match.
    pushes = [record for record in cloud.docker() if record["argv"][0] == "push"]
    assert [record["argv"][-1] for record in pushes] == [TOOLCHAIN_IMAGE, JOB_IMAGE]

    # Nothing of the job is left behind: the Batch job, the pushed image, the local image.
    assert cloud.wait_for_gcloud("batch", "jobs", "delete")[3] == JOB
    deleted = cloud.wait_for_gcloud("artifacts", "docker", "images", "delete")
    assert deleted[4] == JOB_IMAGE
    assert "--delete-tags" in deleted


def test_the_registry_login_never_touches_the_operator_container_configuration(cloud: Cloud):
    completed = cloud.run("true", DOCKER_CONFIG="/operator/docker-config")

    assert completed.returncode == 0, completed.stderr
    records = cloud.docker()
    (login,) = [record for record in records if record["argv"][0] == "login"]
    # The token arrives on standard input, in a private directory removed afterwards.
    assert login["token"] == "fake-access-token"
    assert "fake-access-token" not in " ".join(login["argv"])
    assert login["argv"][-1] == "us-central1-docker.pkg.dev"
    assert login["config"] != "/operator/docker-config"
    assert login["config_is_private_directory"] is True
    assert not Path(login["config"]).exists()
    pushes = [record for record in records if record["argv"][0] == "push"]
    assert {record["config"] for record in pushes} == {login["config"]}
    # Building uses the operator's own configuration, as any local build would.
    builds = [record for record in records if record["argv"][0] == "build"]
    assert {record["config"] for record in builds} == {"/operator/docker-config"}


def test_a_successful_registry_login_adds_nothing_to_the_job_log(cloud: Cloud):
    completed = cloud.run("true")

    assert completed.returncode == 0, completed.stderr
    # The token lives in a private directory that is removed after the push, so the
    # container tool's advice about storing credentials does not apply to the operator.
    assert "stored unencrypted" not in completed.stderr
    assert "credential helper" not in completed.stderr


def test_a_refused_registry_login_shows_what_the_container_tool_said(cloud: Cloud):
    cloud.state.joinpath("login-fails").touch()

    completed = cloud.run("true")

    assert completed.returncode == 125
    assert "cannot log in to us-central1-docker.pkg.dev" in completed.stderr
    assert "login denied for this account" in completed.stderr
    assert not any(record["argv"][0] == "push" for record in cloud.docker())
    assert not any(call[:3] == ["batch", "jobs", "submit"] for call in cloud.gcloud())


def test_a_failing_command_exit_status_is_the_executor_exit_status(cloud: Cloud):
    cloud.answer("states", ["RUNNING", "FAILED"])
    cloud.state.joinpath("exit-code").write_text("3")

    completed = cloud.run("false")

    assert completed.returncode == 3, completed.stderr
    task = cloud.wait_for_gcloud("batch", "tasks", "describe")
    assert task[3] == f"{JOB}/taskGroups/group0/tasks/0"


@pytest.mark.parametrize(
    ("batch_code", "expected", "message"),
    [
        (50001, 125, "reserved exit code 50001"),
        (50002, 125, "reserved exit code 50002"),
        (50005, 124, "runtime limit"),
    ],
)
def test_batch_reserved_exit_codes_are_never_reported_as_the_command_verdict(
    cloud: Cloud,
    batch_code: int,
    expected: int,
    message: str,
):
    cloud.answer("states", ["RUNNING", "FAILED"])
    cloud.state.joinpath("exit-code").write_text(str(batch_code))

    completed = cloud.run("true")

    assert completed.returncode == expected
    assert message in completed.stderr


def test_a_job_that_cannot_be_submitted_is_no_verdict_and_leaves_no_image(cloud: Cloud):
    cloud.state.joinpath("gcloud-fails").write_text("submit")

    completed = cloud.run("true")

    assert completed.returncode == 125
    assert "batch jobs submit failed: ERROR: (gcloud) fake gcloud failure" in completed.stderr
    assert cloud.wait_for_gcloud("artifacts", "docker", "images", "delete")


def test_a_job_survives_checks_of_its_state_that_fail_for_a_while(cloud: Cloud):
    cloud.answer("states", ["RUNNING", "RUNNING", "SUCCEEDED"])
    cloud.answer("logs", [[entry("a1", "still running")], []])
    cloud.fail_times("describe", 3)

    completed = cloud.run("true")

    assert completed.returncode == 0, completed.stderr
    assert completed.stdout == "still running\n"
    assert cloud.wait_for_gcloud("batch", "jobs", "delete")[3] == JOB


def test_a_job_whose_state_can_no_longer_be_read_ends_without_a_verdict(cloud: Cloud):
    cloud.answer("states", ["RUNNING"])
    cloud.fail_times("describe", 1000)

    completed = cloud.run("true")

    assert completed.returncode == 125
    assert "lost contact with the job" in completed.stderr
    # The job is still deleted, so its machine does not outlive the executor.
    assert cloud.wait_for_gcloud("batch", "jobs", "delete")[3] == JOB


def test_claims_without_enforcement_still_size_the_machine(cloud: Cloud):
    contract = {
        name: {"backend": None, "kind": "generic", "mode": "admission-only", "unit": "admission-unit"}
        for name in RESOURCES
    }

    completed = cloud.run("true", AGCOORD_OVERFLOW_CONTRACT=json.dumps(contract))

    assert completed.returncode == 0, completed.stderr
    task = cloud.job()["taskGroups"][0]["taskSpec"]
    # Nothing is enforced inside the container, as on this machine, but the job still
    # gets a machine of the size it claimed.
    assert "options" not in task["runnables"][0]["container"]
    assert task["computeResource"] == {"cpuMilli": 4000, "memoryMib": 3072}


def test_spot_machine_type_and_passed_environment_reach_the_job(cloud: Cloud):
    completed = cloud.run(
        "true",
        options=("--spot", "--machine-type=e2-standard-4", "--env=CI", "--env=UNSET_NAME"),
        CI="1",
    )

    assert completed.returncode == 0, completed.stderr
    job = cloud.job()
    assert job["allocationPolicy"]["instances"] == [
        {"policy": {"provisioningModel": "SPOT", "machineType": "e2-standard-4"}}
    ]
    runnable = job["taskGroups"][0]["taskSpec"]["runnables"][0]
    assert runnable["environment"] == {"variables": {"CI": "1"}}


def test_stopping_the_executor_deletes_the_batch_job(cloud: Cloud):
    cloud.answer("states", ["RUNNING"])
    process = cloud.start("sleep", "600")
    try:
        cloud.wait_for_gcloud("batch", "jobs", "describe")
        assert process.poll() is None

        process.send_signal(signal.SIGTERM)

        assert process.wait(timeout=10) == 128 + signal.SIGTERM
        # Deleting the job is what stops its machine.
        assert cloud.wait_for_gcloud("batch", "jobs", "delete")[3] == JOB
        assert cloud.wait_for_gcloud("artifacts", "docker", "images", "delete")
    finally:
        if process.poll() is None:
            process.kill()
        process.communicate()


def test_it_refuses_a_registry_that_is_not_an_artifact_registry_path(cloud: Cloud):
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "agcoord",
            "overflow-gcp-batch",
            f"--project={PROJECT}",
            "--region=us-central1",
            "--registry=docker.io/example",
            "--",
            "true",
        ],
        cwd=cloud.repository,
        env=cloud.environment(),
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert completed.returncode == 125
    assert "--registry" in completed.stderr
    assert cloud.gcloud() == []


def test_gcloud_progress_reports_stay_out_of_the_job_log(cloud: Cloud):
    completed = cloud.run("true")

    assert completed.returncode == 0, completed.stderr
    # The executor's standard error is the job's log.  What gcloud says while it works
    # is not about the job, and its console links name the project.
    assert cloud.gcloud()
    for line in GCLOUD_PROGRESS.splitlines():
        assert line not in completed.stderr
    assert "console.cloud.google.com" not in completed.stderr
