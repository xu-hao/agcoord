"""Behavior of the SSH overflow executor against a test-owned destination.

The destination is a fake ``ssh`` that runs the remote command line in a local shell, with
a fake container tool that records every invocation.  The executor itself runs as the real
``python -m agcoord.overflow_ssh`` subprocess the broker would start.
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


FAKE_SSH = """\
#!/bin/sh
# Fake ssh: skip "-o VALUE" options and the destination, then run the remote command line.
while [ "$1" = -o ]; do shift 2; done
printf '%s\\n' "$1" >>"$FAKE_STATE/destinations"
shift
if [ -e "$FAKE_STATE/ssh-fails" ]; then
    exit 255
fi
FAKE_REMOTE=1 exec sh -c "$1"
"""

FAKE_DOCKER = """\
#!{python}
import json, os, sys, time
from pathlib import Path

state = Path(os.environ["FAKE_STATE"])
argv = sys.argv[1:]
remote = os.environ.get("FAKE_REMOTE") == "1"
with open(state / "docker.log", "a", encoding="utf-8") as log:
    log.write(json.dumps({{"remote": remote, "argv": argv}}) + "\\n")
if argv[:2] == ["image", "inspect"]:
    raise SystemExit(0 if (state / "remote-has-toolchain").exists() else 1)
if argv[0] == "save":
    sys.stdout.write("TOOLCHAIN-IMAGE-BYTES")
elif argv[0] == "load":
    (state / "loaded").write_bytes(sys.stdin.buffer.read())
    (state / "remote-has-toolchain").touch()
elif argv[0] == "build" and argv[-1] == "-":
    (state / "context.tar").write_bytes(sys.stdin.buffer.read())
    print("sha256:" + "cd" * 32)
elif argv[0] == "build":
    print("sha256:" + "ab" * 32)
elif argv[0] == "run":
    (state / "run-started").touch()
    print("remote output", flush=True)
    deadline = time.monotonic() + 20
    while (state / "run-blocks").exists() and time.monotonic() < deadline:
        time.sleep(0.02)
    status = state / "run-status"
    raise SystemExit(int(status.read_text()) if status.exists() else 0)
"""

MEMORY = 2 * 1024**3
CONTRACT = {
    "jobs": {"backend": None, "kind": "generic", "mode": "admission-only", "unit": "admission-unit"},
    "cpu": {"backend": "cgroup-v2", "kind": "cpu", "mode": "required", "unit": "logical-cpu"},
    "memory": {"backend": "cgroup-v2", "kind": "memory", "mode": "required", "unit": "bytes"},
}
RESOURCES = {"jobs": 1, "cpu": 2, "memory": MEMORY}


class Destination:
    def __init__(self, tmp_path: Path) -> None:
        self.state = tmp_path / "state"
        self.state.mkdir()
        self.bin = tmp_path / "bin"
        self.bin.mkdir()
        for name, body in (
            ("ssh", FAKE_SSH),
            ("docker", FAKE_DOCKER.format(python=sys.executable)),
        ):
            path = self.bin / name
            path.write_text(body, encoding="utf-8")
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
        (self.repository / "untracked.txt").write_text("working state\n")
        (self.repository / "secret.env").write_text("TOKEN=never-leaves\n")

    def _git(self, *arguments: str) -> None:
        subprocess.run(["git", "-C", str(self.repository), *arguments], check=True)

    def environment(self, **overrides: str) -> dict[str, str]:
        environment = {
            "PATH": f"{self.bin}{os.pathsep}{os.environ['PATH']}",
            "HOME": os.environ.get("HOME", "/"),
            # The executor subprocess must import the package under test.
            "PYTHONPATH": str(Path(agcoord.__file__).resolve().parents[1]),
            "FAKE_STATE": str(self.state),
            "AGCOORD_RUN_ID": "check-0123456789ab",
            "AGCOORD_OVERFLOW_POOL": "workstation",
            "AGCOORD_OVERFLOW_MAX_RUNTIME": "60",
            "AGCOORD_OVERFLOW_RESOURCES": json.dumps(RESOURCES),
            "AGCOORD_OVERFLOW_CONTRACT": json.dumps(CONTRACT),
        }
        environment.update(overrides)
        return environment

    def start(
        self, *command: str, options: tuple[str, ...] = (), **overrides: str
    ) -> subprocess.Popen[str]:
        return subprocess.Popen(
            [
                sys.executable,
                "-m",
                "agcoord.overflow_ssh",
                *options,
                "--ssh-option",
                "Port=2222",
                "builder@workstation",
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
        self, *command: str, options: tuple[str, ...] = (), **overrides: str
    ) -> subprocess.CompletedProcess[str]:
        process = self.start(*command, options=options, **overrides)
        stdout, stderr = process.communicate(timeout=30)
        return subprocess.CompletedProcess(process.args, process.returncode, stdout, stderr)

    def calls(self, *, remote: bool) -> list[list[str]]:
        path = self.state / "docker.log"
        if not path.exists():
            return []
        records = [json.loads(line) for line in path.read_text().splitlines()]
        return [record["argv"] for record in records if record["remote"] is remote]

    def wait_for_call(self, verb: str) -> None:
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if any(call[0] == verb for call in self.calls(remote=True)):
                return
            time.sleep(0.02)
        pytest.fail(f"the destination never received {verb}")


@pytest.fixture
def destination(tmp_path: Path) -> Destination:
    return Destination(tmp_path)


def test_the_command_runs_in_a_container_sized_to_the_claim(destination: Destination):
    completed = destination.run("python", "-m", "pytest", "-q")

    assert completed.returncode == 0, completed.stderr
    assert completed.stdout == "remote output\n"
    assert set(destination.state.joinpath("destinations").read_text().split()) == {
        "builder@workstation"
    }
    toolchain = "agcoord-toolchain:" + ("ab" * 32)[:24]
    # The toolchain is built here from the repository's Dockerfile and sent once.
    local = destination.calls(remote=False)
    assert local[0][:2] == ["build", "-q"]
    assert local[0][-1] == str(destination.repository / ".agcoord")
    assert ["save", toolchain] in local
    assert destination.state.joinpath("loaded").read_bytes() == b"TOOLCHAIN-IMAGE-BYTES"

    # The source is one copied layer on that toolchain: the working state without ignored
    # files, so a local secret never reaches the destination.
    stream = io.BytesIO(destination.state.joinpath("context.tar").read_bytes())
    with tarfile.open(fileobj=stream) as context:
        names = set(context.getnames())
        dockerfile = context.extractfile("Dockerfile").read().decode()
        assert context.extractfile("workspace/untracked.txt").read() == b"working state\n"
    assert dockerfile == f"FROM {toolchain}\nCOPY workspace /workspace\nWORKDIR /workspace\n"
    assert {"workspace/tracked.txt", "workspace/untracked.txt"} <= names
    assert "workspace/.agcoord/overflow.json" in names
    assert not any("secret.env" in name for name in names)

    remote = destination.calls(remote=True)
    (run,) = [call for call in remote if call[0] == "run"]
    assert run == [
        "run",
        "--rm",
        "--name=agcoord-check-0123456789ab",
        "--cpus=2",
        f"--memory={MEMORY}",
        f"--memory-swap={MEMORY}",
        "agcoord-run:check-0123456789ab",
        "python",
        "-m",
        "pytest",
        "-q",
    ]
    # Nothing of the job is left on the destination.
    assert ["rm", "-f", "agcoord-check-0123456789ab"] in remote
    assert ["rmi", "-f", "agcoord-run:check-0123456789ab"] in remote


def test_a_toolchain_the_destination_already_holds_is_not_sent_again(destination: Destination):
    destination.state.joinpath("remote-has-toolchain").touch()

    completed = destination.run("true")

    assert completed.returncode == 0, completed.stderr
    assert not any(call[0] == "save" for call in destination.calls(remote=False))
    assert not destination.state.joinpath("loaded").exists()


@pytest.mark.parametrize("executable", [True, False, None])
def test_scratch_mount_and_environment_reach_ssh(destination: Destination, executable):
    binding = {"backend": "cgroup-v2", "kind": "tmpfs", "mode": "required", "unit": "bytes"}
    if executable is not None:
        binding["exec"] = executable
    completed = destination.run(
        "true",
        options=("--env=CI", "--env=TMPDIR", "--env=TMP", "--env=TEMP"),
        AGCOORD_OVERFLOW_RESOURCES=json.dumps({**RESOURCES, "scratch": 1024}),
        AGCOORD_OVERFLOW_CONTRACT=json.dumps({**CONTRACT, "scratch": binding}),
        CI="1", TMPDIR="/host/scratch", TMP="/host/tmp", TEMP="/host/temp",
    )

    assert completed.returncode == 0, completed.stderr
    (run,) = [call for call in destination.calls(remote=True) if call[0] == "run"]
    assert f"--tmpfs=/tmp:rw,{('exec,' if executable else '')}size=1024" in run
    for name in ("TMPDIR", "TMP", "TEMP"):
        assert f"--env={name}=/tmp" in run
        assert len([arg for arg in run if arg.startswith(f"--env={name}=")]) == 1
    assert "--env=CI=1" in run


def test_the_command_exit_status_is_the_executor_exit_status(destination: Destination):
    destination.state.joinpath("run-status").write_text("3")

    assert destination.run("false").returncode == 3


def test_a_lost_connection_is_reported_as_no_verdict(destination: Destination):
    destination.state.joinpath("ssh-fails").touch()

    completed = destination.run("true")

    assert completed.returncode == 125
    assert "SSH" in completed.stderr
    assert not any(call[0] == "run" for call in destination.calls(remote=True))


def test_a_required_claim_a_container_cannot_enforce_is_refused_before_anything_runs(
    destination: Destination,
):
    contract = dict(CONTRACT)
    contract["disk"] = {
        "backend": "project-quota",
        "kind": "storage",
        "mode": "required",
        "unit": "bytes",
    }

    completed = destination.run(
        "true",
        AGCOORD_OVERFLOW_RESOURCES=json.dumps({**RESOURCES, "disk": 1024}),
        AGCOORD_OVERFLOW_CONTRACT=json.dumps(contract),
    )

    assert completed.returncode == 125
    assert "disk" in completed.stderr
    assert destination.calls(remote=False) == []
    assert destination.calls(remote=True) == []


def test_it_refuses_to_run_outside_a_broker_placement(destination: Destination):
    environment = destination.environment()
    del environment["AGCOORD_OVERFLOW_CONTRACT"]

    completed = subprocess.run(
        # The public spelling: the executor is a subcommand of the one agc command.
        [sys.executable, "-m", "agcoord", "overflow-ssh", "builder@workstation", "--", "true"],
        cwd=destination.repository,
        env=environment,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert completed.returncode == 125
    assert "started by the broker" in completed.stderr
    assert destination.calls(remote=True) == []


def test_stopping_the_executor_removes_the_remote_container(destination: Destination):
    destination.state.joinpath("run-blocks").touch()
    process = destination.start("sleep", "600")
    try:
        deadline = time.monotonic() + 10
        while not destination.state.joinpath("run-started").exists():
            assert time.monotonic() < deadline, "the container never started"
            assert process.poll() is None, process.stderr.read()
            time.sleep(0.02)

        process.send_signal(signal.SIGTERM)

        assert process.wait(timeout=10) == 128 + signal.SIGTERM
        destination.wait_for_call("rm")
        assert ["rm", "-f", "agcoord-check-0123456789ab"] in destination.calls(remote=True)
    finally:
        # Let the fake container the stopped executor left behind finish.
        destination.state.joinpath("run-blocks").unlink()
        if process.poll() is None:
            process.kill()
        process.communicate()


def test_a_configured_build_context_is_passed_to_the_toolchain_build(destination: Destination):
    (destination.repository / ".agcoord" / "overflow.json").write_text(
        json.dumps({"dockerfile": ".agcoord/Dockerfile", "context": "."}),
        encoding="utf-8",
    )

    completed = destination.run("true")

    assert completed.returncode == 0, completed.stderr
    local = destination.calls(remote=False)
    assert local[0] == [
        "build",
        "-q",
        "-f",
        str(destination.repository / ".agcoord" / "Dockerfile"),
        str(destination.repository.resolve()),
    ]
