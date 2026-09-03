from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import threading
import time
from typing import Callable, Sequence, TypeVar

import pytest

from agcoord.config import config_path
from agcoord.queue import (
    CoordinatorClient,
    CoordinatorError,
    broker_config,
    configured_capacities,
    queue_paths,
)


T = TypeVar("T")

ROOT = Path(__file__).resolve().parent.parent
NATIVE_BROKER_ENV = "AGCOORD_TEST_NATIVE_BROKER"
_native_broker_scratch: Path | None = None
_native_broker: Path | None = None


def wait_for(
    observe: Callable[[], T],
    failure: str,
    *,
    timeout: float = 10.0,
) -> T:
    deadline = time.monotonic() + timeout
    last_error: BaseException | None = None
    while time.monotonic() < deadline:
        try:
            result = observe()
            if result:
                return result
        except (CoordinatorError, FileNotFoundError, ProcessLookupError) as exc:
            last_error = exc
        time.sleep(0.01)
    detail = f"; last error: {last_error}" if last_error is not None else ""
    pytest.fail(f"{failure}{detail}")


def write_broker_config(state_dir: Path, **sections: object) -> Path:
    """Write one test-owned state directory's configuration file before its broker starts."""
    state_dir.mkdir(parents=True, exist_ok=True)
    path = config_path(state_dir)
    path.write_text(json.dumps(sections), encoding="utf-8")
    return path


def caller_environment() -> dict[str, str]:
    environment = dict(os.environ)
    for variable in (
        "AGCOORD_RUN_ID",
        "AGCOORD_RUN_KIND",
        "AGCOORD_STATE_DIR",
    ):
        environment.pop(variable, None)
    return environment


@pytest.fixture(scope="session", autouse=True)
def _native_broker_scratch_dir(tmp_path_factory: pytest.TempPathFactory):
    """Own the session directory that holds the one development broker the suite drives."""
    global _native_broker, _native_broker_scratch
    _native_broker_scratch = tmp_path_factory.mktemp("native-broker")
    yield
    _native_broker = None
    _native_broker_scratch = None


def native_broker_executable() -> Path:
    """Return the session's development broker, building and staging it on first use.

    ``AGCOORD_TEST_NATIVE_BROKER`` names a prebuilt executable; otherwise the workspace
    binary is built with the locked dependency set.  The staged copy is 0755 because the
    build output honours the umask and clients refuse a group-writable executable.
    """
    global _native_broker
    if _native_broker is not None:
        return _native_broker
    if _native_broker_scratch is None:
        raise RuntimeError("the native broker scratch directory is only available in a session")
    configured = os.environ.get(NATIVE_BROKER_ENV)
    if configured:
        source = Path(configured)
        if not source.is_absolute() or not source.is_file():
            raise RuntimeError(
                f"{NATIVE_BROKER_ENV} must name an absolute existing executable: {configured}"
            )
    else:
        cargo = shutil.which("cargo")
        if cargo is None:
            raise RuntimeError(
                f"cargo is not installed; set {NATIVE_BROKER_ENV} to a prebuilt agcoord-broker"
            )
        completed = subprocess.run(
            [cargo, "build", "--locked", "-p", "agcoord-broker"],
            cwd=ROOT,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False,
            text=True,
        )
        if completed.returncode != 0:
            raise RuntimeError(
                "cargo could not build the development broker the suite drives:\n"
                + completed.stdout
            )
        source = ROOT / "target" / "debug" / "agcoord-broker"
    staged = _native_broker_scratch / "agcoord-broker"
    shutil.copyfile(source, staged)
    staged.chmod(0o755)
    _native_broker = staged
    return staged


class RunningCoordinator:
    """One test-owned native broker serving an isolated state directory.

    The suite drives the protocol-5 owner exactly as installed clients do: the state
    directory's ``config.json`` selects the executable and every observation goes through
    the public JSON commands.  The test that constructs an instance starts and stops it.
    """

    def __init__(
        self,
        state_dir: Path,
        *,
        capacities: dict[str, int] | None = None,
        idle_timeout: float | None = 30.0,
        options: Sequence[str] = (),
    ) -> None:
        self.paths = queue_paths(state_dir=state_dir)
        self.capacities = dict(capacities) if capacities is not None else None
        self.idle_timeout = idle_timeout
        self.options = list(options)
        self.process: subprocess.Popen[bytes] | None = None
        state_dir = self.paths.state_dir
        self.stderr_path = state_dir.parent / f"{state_dir.name}.broker-stderr"

    @property
    def pid(self) -> int:
        if self.process is None:
            raise AssertionError("the native broker is not running")
        return self.process.pid

    def command(self) -> list[str]:
        command = [
            str(native_broker_executable()),
            "serve",
            "--state-dir",
            str(self.paths.state_dir),
        ]
        if self.idle_timeout is not None:
            command.extend(("--idle-timeout", str(self.idle_timeout)))
        capacities = self.capacities
        if capacities is None:
            capacities = configured_capacities(broker_config(self.paths.state_dir).capacities)
        for name, units in sorted(capacities.items()):
            command.extend(("--capacity", f"{name}={units}"))
        command.extend(self.options)
        return command

    def start(self) -> CoordinatorClient:
        assert self.process is None, "the native broker is already running"
        state_dir = self.paths.state_dir
        state_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        path = config_path(state_dir)
        document = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
        document["native_broker"] = {
            "path": str(native_broker_executable()),
            "allow_development": True,
        }
        path.write_text(json.dumps(document), encoding="utf-8")
        with open(self.stderr_path, "wb") as stderr:
            self.process = subprocess.Popen(
                self.command(),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=stderr,
                env=caller_environment(),
            )
        client = CoordinatorClient(state_dir=state_dir, autostart=False)

        def serving() -> bool:
            assert self.process is not None
            if self.process.poll() is not None:
                raise AssertionError(
                    "the native broker exited with "
                    f"{self.process.returncode} before serving: {self.stderr_text()}"
                )
            return client.snapshot()["broker_pid"] == self.process.pid

        wait_for(serving, "the native broker never exposed a snapshot")
        return client

    def is_running(self) -> bool:
        return self.process is not None and self.process.poll() is None

    def stderr_text(self) -> str:
        try:
            return self.stderr_path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            return "<stderr was not captured>"

    def kill(self) -> None:
        """Crash the broker the way an owner loss would, without any cleanup."""
        process = self.process
        self.process = None
        assert process is not None, "the native broker is not running"
        process.kill()
        process.wait(timeout=10)

    def wait(self, timeout: float = 10.0) -> int:
        """Wait for the broker to exit on its own and return its exit status."""
        process = self.process
        assert process is not None, "the native broker is not running"
        status = process.wait(timeout=timeout)
        self.process = None
        return status

    def stop(self) -> None:
        process = self.process
        if process is None:
            return
        self.process = None
        if process.poll() is None:
            process.send_signal(signal.SIGTERM)
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=10)
                raise AssertionError(
                    f"the native broker did not stop on SIGTERM: {self.stderr_text()}"
                )
        assert process.returncode == 0, (
            f"the native broker exited with {process.returncode}: {self.stderr_text()}"
        )


@pytest.fixture
def coordinator(tmp_path: Path):
    running = RunningCoordinator(tmp_path / "state", capacities={"jobs": 2})
    client = running.start()
    yield running, client
    running.stop()
