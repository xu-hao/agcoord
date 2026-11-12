"""Behavior of the Google Cloud Batch overflow executor against a test-owned cloud.

Google Cloud is a fake ``gcloud`` that records every call and answers from files the test
controls, and the container tool is a fake ``docker``.  The executor itself runs as the
real ``python -m agcoord overflow-gcp-batch`` subprocess the broker would start.  Nothing
here contacts a cloud API: running a real Batch job is a benchmark, not a test.
"""

from __future__ import annotations

import io
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tarfile
import time

import pytest

import agcoord


FAKE_GCLOUD = """\
#!{python}
import json, os, sys
from pathlib import Path

state = Path(os.environ["FAKE_STATE"])
argv = sys.argv[1:]
stdin = sys.stdin.read() if "--config=-" in argv else ""
with open(state / "gcloud.log", "a", encoding="utf-8") as log:
    log.write(json.dumps({{"argv": argv, "stdin": stdin}}) + "\\n")


def next_of(name, default):
    # Answer from a list the test wrote; the last answer repeats.
    path = state / (name + ".json")
    answers = json.loads(path.read_text()) if path.exists() else default
    if len(answers) > 1:
        path.write_text(json.dumps(answers[1:]))
    return answers[0]


verb = argv[:3]
if (state / "gcloud-fails").exists() and (state / "gcloud-fails").read_text() in argv:
    sys.stderr.write("fake gcloud failure\\n")
    raise SystemExit(1)
if argv[:2] == ["auth", "print-access-token"]:
    print("fake-access-token")
elif verb == ["batch", "jobs", "submit"]:
    (state / "job.json").write_text(stdin)
    print(json.dumps({{"uid": "job-uid-1", "createTime": "2026-11-11T00:00:00Z"}}))
elif verb == ["batch", "jobs", "describe"]:
    state_name = next_of("states", ["QUEUED", "RUNNING", "SUCCEEDED"])
    print(json.dumps({{"status": {{"state": state_name}}}}))
elif argv[:2] == ["logging", "read"]:
    print(json.dumps(next_of("logs", [[]])))
elif verb == ["batch", "tasks", "describe"]:
    code = int((state / "exit-code").read_text())
    events = [{{"type": "STATUS_CHANGED"}}, {{"taskExecution": {{"exitCode": code}}}}]
    print(json.dumps({{"status": {{"statusEvents": events}}}}))
"""

FAKE_DOCKER = """\
#!{python}
import json, os, sys
from pathlib import Path

state = Path(os.environ["FAKE_STATE"])
argv = sys.argv[1:]
record = {{"argv": argv, "config": os.environ.get("DOCKER_CONFIG")}}
if argv[0] == "login":
    record["token"] = sys.stdin.read()
    record["config_is_private_directory"] = (
        os.path.isdir(record["config"]) and oct(os.stat(record["config"]).st_mode)[-3:] == "700"
    )
with open(state / "docker.log", "a", encoding="utf-8") as log:
    log.write(json.dumps(record) + "\\n")
if argv[0] == "login":
    # Docker answers every login into a plain configuration file with this warning.
    sys.stderr.write(
        "\\nWARNING! Your credentials are stored unencrypted in '"
        + record["config"] + "/config.json'.\\n"
        "Configure a credential helper to remove this warning.\\n\\n"
    )
    if (state / "login-fails").exists():
        sys.stderr.write("Error response from daemon: login denied for this account\\n")
        raise SystemExit(1)
if argv[0] == "build" and argv[-1] == "-":
    (state / "context.tar").write_bytes(sys.stdin.buffer.read())
    print("sha256:" + "cd" * 32)
elif argv[0] == "build":
    print("sha256:" + "ab" * 32)
elif argv[:2] == ["image", "inspect"]:
    repository = argv[-1].rsplit(":", 1)[0]
    print(json.dumps([repository + "@sha256:" + "12" * 32]))
"""

PROJECT = "example-project"
REGISTRY = f"us-central1-docker.pkg.dev/{PROJECT}/agcoord-overflow"
RUNNER = f"runner@{PROJECT}.iam.gserviceaccount.com"
RUN_ID = "check-0123456789ab"
JOB = f"projects/{PROJECT}/locations/us-central1/jobs/agcoord-{RUN_ID}"
MEMORY = 3 * 1024**3
CONTRACT = {
    "jobs": {"backend": None, "kind": "generic", "mode": "admission-only", "unit": "admission-unit"},
    "cpu": {"backend": "cgroup-v2", "kind": "cpu", "mode": "required", "unit": "logical-cpu"},
    "memory": {"backend": "cgroup-v2", "kind": "memory", "mode": "required", "unit": "bytes"},
}
RESOURCES = {"jobs": 1, "cpu": 4, "memory": MEMORY}


def _entry(identity: str, text: str) -> dict[str, str]:
    return {
        "insertId": identity,
        "timestamp": f"2026-11-11T00:00:0{identity[-1]}Z",
        "textPayload": text,
    }


class Cloud:
    def __init__(self, tmp_path: Path) -> None:
        self.state = tmp_path / "state"
        self.state.mkdir()
        self.bin = tmp_path / "bin"
        self.bin.mkdir()
        for name, body in (("gcloud", FAKE_GCLOUD), ("docker", FAKE_DOCKER)):
            path = self.bin / name
            path.write_text(body.format(python=sys.executable), encoding="utf-8")
            path.chmod(0o755)
        self.repository = tmp_path / "repository"
        self.repository.mkdir()
        self._git("init", "-q", "-b", "main")
        self._git("config", "user.name", "AGCoord test")
        self._git("config", "user.email", "agcoord@example.invalid")
        (self.repository / ".agcoord").mkdir()
        (self.repository / ".agcoord" / "Dockerfile").write_text("FROM scratch\n")
        (self.repository / ".agcoord" / "overflow.json").write_text(
            json.dumps({"dockerfile": ".agcoord/Dockerfile"})
        )
        (self.repository / ".gitignore").write_text("secret.env\n")
        (self.repository / "tracked.txt").write_text("tracked\n")
        self._git("add", ".")
        self._git("commit", "-q", "-m", "initial")
        (self.repository / "secret.env").write_text("TOKEN=never-leaves\n")

    def _git(self, *arguments: str) -> None:
        subprocess.run(["git", "-C", str(self.repository), *arguments], check=True)

    def answer(self, name: str, answers: list[object]) -> None:
        self.state.joinpath(f"{name}.json").write_text(json.dumps(answers))

    def environment(self, **overrides: str) -> dict[str, str]:
        environment = {
            "PATH": f"{self.bin}{os.pathsep}{os.environ['PATH']}",
            "HOME": os.environ.get("HOME", "/"),
            # The executor subprocess must import the package under test.
            "PYTHONPATH": str(Path(agcoord.__file__).resolve().parents[1]),
            "FAKE_STATE": str(self.state),
            "AGCOORD_RUN_ID": RUN_ID,
            "AGCOORD_OVERFLOW_POOL": "cloud",
            "AGCOORD_OVERFLOW_MAX_RUNTIME": "900",
            "AGCOORD_OVERFLOW_RESOURCES": json.dumps(RESOURCES),
            "AGCOORD_OVERFLOW_CONTRACT": json.dumps(CONTRACT),
        }
        environment.update(overrides)
        return environment

    def start(
        self,
        *command: str,
        options: tuple[str, ...] = (),
        **overrides: str,
    ) -> subprocess.Popen[str]:
        return subprocess.Popen(
            [
                sys.executable,
                "-m",
                "agcoord",
                "overflow-gcp-batch",
                f"--project={PROJECT}",
                "--region=us-central1",
                f"--registry={REGISTRY}",
                f"--service-account={RUNNER}",
                "--poll-interval=0.02",
                *options,
                "--",
                *command,
            ],
            cwd=self.repository,
            env=self.environment(**overrides),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

    def run(
        self,
        *command: str,
        options: tuple[str, ...] = (),
        **overrides: str,
    ) -> subprocess.CompletedProcess[str]:
        process = self.start(*command, options=options, **overrides)
        stdout, stderr = process.communicate(timeout=60)
        return subprocess.CompletedProcess(process.args, process.returncode, stdout, stderr)

    def _log(self, name: str) -> list[dict[str, object]]:
        path = self.state / f"{name}.log"
        if not path.exists():
            return []
        return [json.loads(line) for line in path.read_text().splitlines()]

    def gcloud(self) -> list[list[str]]:
        return [record["argv"] for record in self._log("gcloud")]

    def docker(self) -> list[dict[str, object]]:
        return self._log("docker")

    def job(self) -> dict[str, object]:
        return json.loads(self.state.joinpath("job.json").read_text())

    def wait_for_gcloud(self, *prefix: str) -> list[str]:
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            for call in self.gcloud():
                if call[: len(prefix)] == list(prefix):
                    return call
            time.sleep(0.02)
        pytest.fail(f"gcloud was never asked to {' '.join(prefix)}")


@pytest.fixture
def cloud(tmp_path: Path) -> Cloud:
    return Cloud(tmp_path)


def test_the_command_runs_as_one_batch_job_sized_to_the_claim(cloud: Cloud):
    cloud.answer(
        "logs",
        [
            [],
            [_entry("a1", "collected 3 items"), _entry("a2", "...\n")],
            # Cloud Logging returns an entry again when the next read starts at its time.
            [_entry("a2", "...\n"), _entry("a3", "3 passed")],
            [],
        ],
    )

    completed = cloud.run("python", "-m", "pytest", "-q")

    assert completed.returncode == 0, completed.stderr
    assert completed.stdout == "collected 3 items\n...\n3 passed\n"

    digest = f"{REGISTRY}/run@sha256:" + "12" * 32
    job = cloud.job()
    (group,) = job["taskGroups"]
    task = group["taskSpec"]
    assert group["taskCount"] == 1
    assert task["runnables"] == [
        {
            "container": {
                # The machine pulls exactly the image that was built for this job.
                "imageUri": digest,
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
    toolchain = f"{REGISTRY}/toolchain:" + ("ab" * 32)[:24]
    pushes = [record for record in cloud.docker() if record["argv"][0] == "push"]
    assert [record["argv"][-1] for record in pushes] == [toolchain, f"{REGISTRY}/run:{RUN_ID}"]

    # Nothing of the job is left behind: the Batch job, the pushed image, the local image.
    assert cloud.wait_for_gcloud("batch", "jobs", "delete")[3] == JOB
    deleted = cloud.wait_for_gcloud("artifacts", "docker", "images", "delete")
    assert deleted[4] == f"{REGISTRY}/run:{RUN_ID}"
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
    assert "batch jobs submit failed" in completed.stderr
    assert cloud.wait_for_gcloud("artifacts", "docker", "images", "delete")


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
