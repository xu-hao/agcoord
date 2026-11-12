"""A test-owned Google Cloud for the overflow executors.

Google Cloud is a fake ``gcloud`` that records every call and answers from files the test
controls, and the container tool is a fake ``docker`` that behaves like Docker where the
executors depend on it.  An executor under test runs as the real ``python -m agcoord
SUBCOMMAND`` subprocess the broker would start.  Nothing here contacts a cloud API:
running a real job is a benchmark, not a test.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
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


def fail():
    sys.stderr.write("fake gcloud failure\\n")
    raise SystemExit(1)


if (state / "gcloud-fails").exists() and (state / "gcloud-fails").read_text() in argv:
    fail()
flaky = state / "gcloud-fails-times.json"
if flaky.exists():
    plan = json.loads(flaky.read_text())
    if plan["times"] > 0 and plan["match"] in argv:
        plan["times"] -= 1
        flaky.write_text(json.dumps(plan))
        fail()

verb = argv[:3]
if argv[:2] == ["auth", "print-access-token"]:
    print("fake-access-token")
elif argv[:2] == ["logging", "read"]:
    print(json.dumps(next_of("logs", [[]])))

# --- Batch
elif verb == ["batch", "jobs", "submit"]:
    (state / "job.json").write_text(stdin)
    print(json.dumps({{"uid": "job-uid-1", "createTime": "2026-11-11T00:00:00Z"}}))
elif verb == ["batch", "jobs", "describe"]:
    state_name = next_of("states", ["QUEUED", "RUNNING", "SUCCEEDED"])
    print(json.dumps({{"status": {{"state": state_name}}}}))
elif verb == ["batch", "tasks", "describe"]:
    code = int((state / "exit-code").read_text())
    events = [{{"type": "STATUS_CHANGED"}}, {{"taskExecution": {{"exitCode": code}}}}]
    print(json.dumps({{"status": {{"statusEvents": events}}}}))

# --- Cloud Run jobs
elif verb == ["run", "jobs", "replace"]:
    # The definition is a file that exists only while this command runs.
    (state / "job.json").write_text(Path(argv[3]).read_text())
    (state / "job-file").write_text(argv[3])
elif verb == ["run", "jobs", "describe"]:
    job = json.loads((state / "job.json").read_text())
    task = job["spec"]["template"]["spec"]["template"]["spec"]
    if (state / "server-max-retries").exists():
        # A server that ignored the request and applied its own default.
        task["maxRetries"] = int((state / "server-max-retries").read_text())
    elif task.get("maxRetries") == 0:
        # Zero is a default value, and JSON responses leave default values out.
        del task["maxRetries"]
    print(json.dumps(job))
elif verb == ["run", "jobs", "execute"]:
    metadata = {{"name": argv[3] + "-x7k2p", "creationTimestamp": "2026-11-11T00:00:00Z"}}
    print(json.dumps({{"metadata": metadata, "status": {{}}}}))
elif argv[:4] == ["run", "jobs", "executions", "describe"]:
    name = next_of("states", ["PENDING", "RUNNING", "SUCCEEDED"])
    status = {{
        "PENDING": {{}},
        "RUNNING": {{"runningCount": 1, "conditions": [{{"type": "Completed", "status": "Unknown"}}]}},
        "SUCCEEDED": {{"succeededCount": 1, "conditions": [{{"type": "Completed", "status": "True"}}]}},
        "FAILED": {{
            "failedCount": 1,
            "conditions": [
                {{"type": "Started", "status": "True"}},
                {{"type": "Completed", "status": "False", "message": "Task failed."}},
            ],
        }},
        "CANCELLED": {{
            "cancelledCount": 1,
            "conditions": [{{"type": "Completed", "status": "False", "message": "Cancelled."}}],
        }},
    }}[name]
    print(json.dumps({{"metadata": {{"name": argv[4]}}, "status": status}}))
elif argv[:5] == ["run", "jobs", "executions", "tasks", "list"]:
    result = json.loads((state / "task-result.json").read_text())
    print(json.dumps([{{"status": {{"lastAttemptResult": result}}}}]))
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
REGION = "us-central1"
REGISTRY = f"{REGION}-docker.pkg.dev/{PROJECT}/agcoord-overflow"
RUNNER = f"runner@{PROJECT}.iam.gserviceaccount.com"
RUN_ID = "check-0123456789ab"
JOB_IMAGE = f"{REGISTRY}/run:{RUN_ID}"
JOB_IMAGE_DIGEST = f"{REGISTRY}/run@sha256:" + "12" * 32
TOOLCHAIN_IMAGE = f"{REGISTRY}/toolchain:" + ("ab" * 32)[:24]
MEMORY = 3 * 1024**3
ADMISSION_ONLY = {
    "backend": None,
    "kind": "generic",
    "mode": "admission-only",
    "unit": "admission-unit",
}
CONTRACT = {
    "jobs": ADMISSION_ONLY,
    "cpu": {"backend": "cgroup-v2", "kind": "cpu", "mode": "required", "unit": "logical-cpu"},
    "memory": {"backend": "cgroup-v2", "kind": "memory", "mode": "required", "unit": "bytes"},
}
RESOURCES = {"jobs": 1, "cpu": 4, "memory": MEMORY}


def entry(identity: str, text: str, log: str = "batch_task_logs") -> dict[str, str]:
    """Return one Cloud Logging entry whose time follows from the last digit of its ID."""
    return {
        "insertId": identity,
        "timestamp": f"2026-11-11T00:00:0{identity[-1]}Z",
        "logName": f"projects/{PROJECT}/logs/{log}",
        "textPayload": text,
    }


class Cloud:
    """One executor's view of the fake cloud, with a repository that opted in to overflow."""

    def __init__(self, tmp_path: Path, executor: str) -> None:
        self.executor = executor
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
        """Make the fake ``gcloud`` give these answers in turn; the last one repeats."""
        self.state.joinpath(f"{name}.json").write_text(json.dumps(answers))

    def fail_times(self, match: str, times: int) -> None:
        """Make the next ``times`` ``gcloud`` calls that contain ``match`` fail."""
        self.state.joinpath("gcloud-fails-times.json").write_text(
            json.dumps({"match": match, "times": times})
        )

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

    def command(self, *command: str, options: tuple[str, ...] = ()) -> list[str]:
        return [
            sys.executable,
            "-m",
            "agcoord",
            self.executor,
            f"--project={PROJECT}",
            f"--region={REGION}",
            f"--registry={REGISTRY}",
            f"--service-account={RUNNER}",
            "--poll-interval=0.02",
            *options,
            "--",
            *command,
        ]

    def start(
        self,
        *command: str,
        options: tuple[str, ...] = (),
        **overrides: str,
    ) -> subprocess.Popen[str]:
        return subprocess.Popen(
            self.command(*command, options=options),
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
        """Return the job definition the executor sent, whichever service it was for."""
        return json.loads(self.state.joinpath("job.json").read_text())

    def called(self, *prefix: str) -> list[list[str]]:
        return [call for call in self.gcloud() if call[: len(prefix)] == list(prefix)]

    def wait_for_gcloud(self, *prefix: str) -> list[str]:
        """Return the first matching call, waiting for cleanup that runs detached."""
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            calls = self.called(*prefix)
            if calls:
                return calls[0]
            time.sleep(0.02)
        pytest.fail(f"gcloud was never asked to {' '.join(prefix)}")
