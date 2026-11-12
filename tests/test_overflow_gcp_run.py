"""Behavior of the Google Cloud Run overflow executor against a test-owned cloud.

The fake cloud is described in ``gcp_fakes``.  The executor itself runs as the real
``python -m agcoord overflow-gcp-run`` subprocess the broker would start.  Nothing here
contacts a cloud API: running a real Cloud Run job is a benchmark, not a test.
"""

from __future__ import annotations

import json
from pathlib import Path
import signal
import subprocess

import pytest

from agcoord.overflow_gcp_run import GIB, MIB, run_size
from agcoord.overflow_image import ContainerLimits, ExecutorError, Placement
from gcp_fakes import (
    ADMISSION_ONLY,
    CONTRACT,
    JOB_IMAGE,
    JOB_IMAGE_DIGEST,
    PROJECT,
    REGION,
    RESOURCES,
    RUN_ID,
    RUNNER,
    TOOLCHAIN_IMAGE,
    Cloud,
    entry,
)


JOB = f"agcoord-{RUN_ID}"
EXECUTION = f"{JOB}-x7k2p"
STDOUT = "run.googleapis.com%2Fstdout"
STDERR = "run.googleapis.com%2Fstderr"
SYSTEM = "run.googleapis.com%2Fvarlog%2Fsystem"


@pytest.fixture
def cloud(tmp_path: Path) -> Cloud:
    return Cloud(tmp_path, "overflow-gcp-run")


def _task(cloud: Cloud) -> dict[str, object]:
    return cloud.job()["spec"]["template"]["spec"]["template"]["spec"]


def test_the_command_runs_once_as_one_cloud_run_job_sized_to_the_claim(cloud: Cloud):
    cloud.answer(
        "logs",
        [
            [],
            [entry("a1", "collected 3 items", STDOUT), entry("a2", "a warning", STDERR)],
            # Cloud Logging returns an entry again when the next read starts at its time.
            [entry("a2", "a warning", STDERR), entry("a3", "3 passed", STDOUT)],
            [entry("a4", "Container called exit(0).", SYSTEM)],
            [],
        ],
    )

    completed = cloud.run("python", "-m", "pytest", "-q")

    assert completed.returncode == 0, completed.stderr
    # The command's two streams reach the job's log as its output; what Cloud Run itself
    # says about the job is kept apart from it.
    assert completed.stdout == "collected 3 items\na warning\n3 passed\n"
    assert "Container called exit(0)." in completed.stderr

    job = cloud.job()
    assert job["apiVersion"] == "run.googleapis.com/v1"
    assert job["kind"] == "Job"
    assert job["metadata"]["name"] == JOB
    assert job["spec"]["template"]["spec"]["taskCount"] == 1
    assert _task(cloud) == {
        # Cloud Run never runs the command a second time, and stops it at the pool's limit.
        "maxRetries": 0,
        "timeoutSeconds": 900,
        "serviceAccountName": RUNNER,
        "containers": [
            {
                # The job runs exactly the image that was built for it.
                "image": JOB_IMAGE_DIGEST,
                "command": ["python"],
                "args": ["-m", "pytest", "-q"],
                "resources": {"limits": {"cpu": "4", "memory": "3072Mi"}},
            }
        ],
    }

    (replace,) = cloud.called("run", "jobs", "replace")
    assert replace[3].endswith(".yaml") and f"--region={REGION}" in replace
    # The definition file is temporary and holds nothing after the job is created.
    assert not Path(replace[3]).exists()
    (execute,) = cloud.called("run", "jobs", "execute")
    assert execute[3] == JOB and "--async" in execute
    read = cloud.called("logging", "read")[0][2]
    assert 'resource.type="cloud_run_job"' in read
    assert f'labels."run.googleapis.com/execution_name"="{EXECUTION}"' in read
    assert f'resource.labels.location="{REGION}"' in read

    pushes = [record for record in cloud.docker() if record["argv"][0] == "push"]
    assert [record["argv"][-1] for record in pushes] == [TOOLCHAIN_IMAGE, JOB_IMAGE]

    # Nothing of the job is left behind, and a finished execution is not cancelled.
    assert cloud.wait_for_gcloud("run", "jobs", "delete")[3] == JOB
    assert cloud.wait_for_gcloud("artifacts", "docker", "images", "delete")[4] == JOB_IMAGE
    assert cloud.called("run", "jobs", "executions", "cancel") == []


def test_a_failing_command_exit_status_is_the_executor_exit_status(cloud: Cloud):
    cloud.answer("states", ["RUNNING", "FAILED"])
    cloud.state.joinpath("task-result.json").write_text(
        json.dumps({"exitCode": 3, "status": {"code": 2, "message": "The container exited."}})
    )

    completed = cloud.run("false")

    assert completed.returncode == 3, completed.stderr
    (tasks,) = cloud.called("run", "jobs", "executions", "tasks", "list")
    assert f"--execution={EXECUTION}" in tasks


@pytest.mark.parametrize(
    ("result", "expected"),
    [
        (
            {"termSignal": 9, "status": {"code": 8, "message": "Memory limit exceeded."}},
            "terminated with signal 9; Memory limit exceeded.",
        ),
        ({"status": {"code": 4, "message": "The task timed out."}}, "The task timed out."),
        # Without any detail from the task, the execution's own explanation is used.
        ({}, "Task failed."),
        # A failed execution whose command reports success is still not a pass.
        ({"exitCode": 0}, "Task failed."),
    ],
)
def test_a_job_cloud_run_ended_without_an_exit_status_is_no_verdict(
    cloud: Cloud,
    result: dict[str, object],
    expected: str,
):
    cloud.answer("states", ["RUNNING", "FAILED"])
    cloud.state.joinpath("task-result.json").write_text(json.dumps(result))

    completed = cloud.run("true")

    assert completed.returncode == 125
    assert "without an exit status from the command" in completed.stderr
    assert expected in completed.stderr


def test_a_cancelled_execution_is_no_verdict(cloud: Cloud):
    cloud.answer("states", ["RUNNING", "CANCELLED"])

    completed = cloud.run("true")

    assert completed.returncode == 125
    assert "ended as CANCELLED without a verdict" in completed.stderr


def _placed(cpus: int | None, memory: int | None, tmpfs: int | None) -> Placement:
    resources = {"jobs": 1}
    if cpus is not None:
        resources["cpu"] = cpus
    if memory is not None:
        resources["memory"] = memory
    return Placement(
        run_id=RUN_ID,
        max_runtime=900,
        resources=resources,
        limits=ContainerLimits(cpus=cpus, memory=memory, tmpfs=tmpfs),
    )


@pytest.mark.parametrize(
    ("cpus", "memory", "tmpfs", "expected_cpus", "expected_mib"),
    [
        # No claim at all: the smallest size, with the most memory it allows.
        (None, None, None, 1, 4096),
        # Cloud Run has no 3-CPU size.
        (3, None, None, 4, 16384),
        (2, 1 * GIB, None, 2, 1024),
        # Four CPUs come with at least 2 GiB.
        (4, 1 * GIB, None, 4, 2048),
        # Memory can force more CPUs than were claimed.
        (1, 6 * GIB, None, 2, 6144),
        # Scratch space is held in memory, so it is added to the memory claim.
        (2, 2 * GIB, 1 * GIB, 2, 3072),
        (2, None, 1 * GIB, 2, 8192),
        (8, 31 * GIB, 1 * GIB, 8, 32768),
        (1, 300 * MIB, None, 1, 512),
    ],
)
def test_the_smallest_cloud_run_size_that_holds_the_claim_is_chosen(
    cpus: int | None,
    memory: int | None,
    tmpfs: int | None,
    expected_cpus: int,
    expected_mib: int,
):
    size = run_size(_placed(cpus, memory, tmpfs))

    assert (size.cpus, size.memory_mib) == (expected_cpus, expected_mib)


@pytest.mark.parametrize(
    ("cpus", "memory", "tmpfs"),
    [(9, None, None), (16, 8 * GIB, None), (8, 32 * GIB, 1 * GIB), (1, 33 * GIB, None)],
)
def test_a_claim_no_cloud_run_size_holds_is_refused(
    cpus: int,
    memory: int | None,
    tmpfs: int | None,
):
    with pytest.raises(ExecutorError, match="at most 8 CPUs and 32 GiB"):
        run_size(_placed(cpus, memory, tmpfs))


def test_a_job_too_large_for_cloud_run_is_refused_before_anything_is_built(cloud: Cloud):
    completed = cloud.run(
        "true",
        AGCOORD_OVERFLOW_RESOURCES=json.dumps({"jobs": 1, "cpu": 16, "memory": 8 * GIB}),
    )

    assert completed.returncode == 125
    assert "Cloud Run runs at most 8 CPUs and 32 GiB" in completed.stderr
    assert "this job claims 16 CPUs" in completed.stderr
    assert cloud.docker() == []
    assert cloud.gcloud() == []


def test_a_scratch_claim_becomes_sized_in_memory_scratch_space(cloud: Cloud):
    contract = dict(CONTRACT)
    contract["tmpfs"] = {"backend": "cgroup-v2", "kind": "tmpfs", "mode": "required", "unit": "bytes"}

    completed = cloud.run(
        "true",
        AGCOORD_OVERFLOW_RESOURCES=json.dumps({**RESOURCES, "tmpfs": 512 * MIB}),
        AGCOORD_OVERFLOW_CONTRACT=json.dumps(contract),
    )

    assert completed.returncode == 0, completed.stderr
    task = _task(cloud)
    assert task["volumes"] == [
        {"name": "scratch", "emptyDir": {"medium": "Memory", "sizeLimit": "512Mi"}}
    ]
    (container,) = task["containers"]
    assert container["volumeMounts"] == [{"name": "scratch", "mountPath": "/tmp"}]
    # The scratch space is paid for out of the container's memory.
    assert container["resources"]["limits"] == {"cpu": "4", "memory": "3584Mi"}


@pytest.mark.parametrize(
    ("name", "kind"),
    [("pids", "processes"), ("tmpfs_inodes", "inodes"), ("disk", "storage")],
)
def test_a_required_claim_cloud_run_cannot_enforce_is_refused_before_anything_is_built(
    cloud: Cloud,
    name: str,
    kind: str,
):
    contract = dict(CONTRACT)
    contract[name] = {"backend": "cgroup-v2", "kind": kind, "mode": "required", "unit": "x"}

    completed = cloud.run(
        "true",
        AGCOORD_OVERFLOW_RESOURCES=json.dumps({**RESOURCES, name: 100}),
        AGCOORD_OVERFLOW_CONTRACT=json.dumps(contract),
    )

    assert completed.returncode == 125
    assert f"required claim {name} of kind {kind} cannot be enforced in a Cloud Run job" in (
        completed.stderr
    )
    assert cloud.docker() == []
    assert cloud.gcloud() == []


def test_a_best_effort_claim_cloud_run_cannot_enforce_is_skipped(cloud: Cloud):
    contract = dict(CONTRACT)
    contract["pids"] = {
        "backend": "cgroup-v2",
        "kind": "processes",
        "mode": "best-effort",
        "unit": "processes",
    }

    completed = cloud.run(
        "true",
        AGCOORD_OVERFLOW_RESOURCES=json.dumps({**RESOURCES, "pids": 100}),
        AGCOORD_OVERFLOW_CONTRACT=json.dumps(contract),
    )

    assert completed.returncode == 0, completed.stderr


def test_claims_without_enforcement_still_size_the_job(cloud: Cloud):
    contract = {name: ADMISSION_ONLY for name in RESOURCES}

    completed = cloud.run("true", AGCOORD_OVERFLOW_CONTRACT=json.dumps(contract))

    assert completed.returncode == 0, completed.stderr
    (container,) = _task(cloud)["containers"]
    assert container["resources"]["limits"] == {"cpu": "4", "memory": "3072Mi"}


def test_a_job_cloud_run_would_retry_is_never_executed(cloud: Cloud):
    cloud.state.joinpath("server-max-retries").write_text("3")

    completed = cloud.run("true")

    assert completed.returncode == 125
    assert "Cloud Run would retry the job 3 times" in completed.stderr
    assert cloud.called("run", "jobs", "execute") == []
    assert cloud.wait_for_gcloud("run", "jobs", "delete")[3] == JOB
    assert cloud.wait_for_gcloud("artifacts", "docker", "images", "delete")[4] == JOB_IMAGE


def test_a_line_the_command_printed_as_json_is_relayed_as_json(cloud: Cloud):
    structured = entry("a1", "", STDOUT)
    del structured["textPayload"]
    structured["jsonPayload"] = {"event": "done", "passed": 3}
    cloud.answer("logs", [[structured], []])

    completed = cloud.run("true")

    assert completed.returncode == 0, completed.stderr
    assert completed.stdout == '{"event":"done","passed":3}\n'


def test_passed_environment_and_an_omitted_service_account(cloud: Cloud):
    command = [
        argument
        for argument in cloud.command("true", options=("--env=CI", "--env=UNSET"))
        if not argument.startswith("--service-account")
    ]

    completed = subprocess.run(
        command,
        cwd=cloud.repository,
        env=cloud.environment(CI="1"),
        capture_output=True,
        text=True,
        timeout=60,
    )

    assert completed.returncode == 0, completed.stderr
    task = _task(cloud)
    assert "serviceAccountName" not in task
    assert task["containers"][0]["env"] == [{"name": "CI", "value": "1"}]


def test_a_job_that_cannot_be_created_is_no_verdict_and_leaves_no_image(cloud: Cloud):
    cloud.state.joinpath("gcloud-fails").write_text("replace")

    completed = cloud.run("true")

    assert completed.returncode == 125
    assert "run jobs replace failed" in completed.stderr
    assert cloud.called("run", "jobs", "execute") == []
    assert cloud.wait_for_gcloud("artifacts", "docker", "images", "delete")[4] == JOB_IMAGE


def test_stopping_the_executor_cancels_the_execution_and_deletes_the_job(cloud: Cloud):
    cloud.answer("states", ["RUNNING"])
    process = cloud.start("sleep", "600")
    try:
        cloud.wait_for_gcloud("run", "jobs", "executions", "describe")
        assert process.poll() is None

        process.send_signal(signal.SIGTERM)

        assert process.wait(timeout=10) == 128 + signal.SIGTERM
        # Cancelling the execution is what stops the container.
        assert cloud.wait_for_gcloud("run", "jobs", "executions", "cancel")[4] == EXECUTION
        assert cloud.wait_for_gcloud("run", "jobs", "delete")[3] == JOB
        assert cloud.wait_for_gcloud("artifacts", "docker", "images", "delete")[4] == JOB_IMAGE
        calls = cloud.gcloud()
        cancel = calls.index(cloud.called("run", "jobs", "executions", "cancel")[0])
        delete = calls.index(cloud.called("run", "jobs", "delete")[0])
        assert cancel < delete
    finally:
        if process.poll() is None:
            process.kill()
        process.communicate()


def test_the_project_is_named_on_every_cloud_call(cloud: Cloud):
    completed = cloud.run("true")

    assert completed.returncode == 0, completed.stderr
    cloud.wait_for_gcloud("artifacts", "docker", "images", "delete")
    assert all(f"--project={PROJECT}" in call for call in cloud.gcloud())
