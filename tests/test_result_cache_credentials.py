"""Local jobs reach the GCP result cache with the workstation's client key on their own.

When the broker has a Google Cloud overflow pool and this workstation holds a result-cache
client key, a job admitted on this machine receives ``GOOGLE_APPLICATION_CREDENTIALS``
unless its submitter set that variable.  A job placed in a pool never receives it, so the
workstation key cannot travel to a Google Cloud job.  Every broker here is test-owned, and
each submission names a test-owned ``HOME``, so the operator's own key is never read.
"""

from __future__ import annotations

from pathlib import Path
import subprocess
import sys

import pytest

from agcoord.queue import CoordinatorClient, CoordinatorError

from conftest import RunningCoordinator, caller_environment, write_broker_config
from test_queue import (
    _git,
    _head,
    _install_land_gh,
    _land_environment,
    _publication_repository,
    _python,
    _repository,
    _row,
)


CREDENTIALS = "GOOGLE_APPLICATION_CREDENTIALS"
UNSET = "<unset>"
KEY = Path(".config", "agcoord", "result-cache-client.json")


def _home(tmp_path: Path, *, key: bool) -> tuple[Path, Path]:
    """Return a test-owned home directory and its default result-cache key path."""
    home = tmp_path / "home"
    path = home / KEY
    path.parent.mkdir(parents=True, mode=0o700)
    if key:
        path.write_text('{"type": "service_account"}', encoding="utf-8")
        path.chmod(0o600)
    return home, path


def _executor(tmp_path: Path) -> Path:
    """A pool executor that records the credentials variable it was started with."""
    executor = tmp_path / "executor"
    executor.write_text(
        "#!/bin/sh\n"
        'printf "%s" "${GOOGLE_APPLICATION_CREDENTIALS-<unset>}" >"$0.credentials"\n'
        'while [ "$1" != -- ]; do shift; done\n'
        "shift\n"
        'exec "$@"\n',
        encoding="utf-8",
    )
    executor.chmod(0o755)
    return executor


def _pool(executor: Path, subcommand: str = "overflow-gcp-batch", *options: str) -> dict:
    return {
        "name": "cloud",
        "capacities": {"jobs": 1, "cpu": 8},
        "executor": [str(executor), subcommand, "--project", "example-project", *options],
        "max_runtime": 60,
    }


def _report(output: Path) -> list[str]:
    """A job that records the credentials variable its command was started with."""
    return _python(
        "import os, pathlib, sys; "
        f"pathlib.Path(sys.argv[1]).write_text(os.environ.get({CREDENTIALS!r}, {UNSET!r}))",
        output,
    )


def _environment(home: Path, base: dict[str, str] | None = None) -> dict[str, str]:
    environment = dict(caller_environment() if base is None else base)
    environment.pop(CREDENTIALS, None)
    environment["HOME"] = str(home)
    return environment


class _Lane:
    """One repository and the submission that runs a reporting job there as one kind."""

    def __init__(self, tmp_path: Path, kind: str, *, overflow: bool = False) -> None:
        self.kind = kind
        self.output = tmp_path / f"{kind}.credentials"
        if kind == "check":
            self.repository = _repository(tmp_path / "repository")
            self.base = caller_environment()
        else:
            self.repository, _remote, branch, _ = _publication_repository(
                tmp_path / "repository"
            )
            self.branch = branch
            self.base = _land_environment(
                _install_land_gh(tmp_path),
                branch=branch,
                head_sha=_head(self.repository),
                tag=kind,
                event_log=tmp_path / "events",
            )
        if overflow:
            config = self.repository / ".agcoord" / "overflow.json"
            config.parent.mkdir()
            config.write_text('{"image": "example/toolchain:1"}', encoding="utf-8")
            _git(self.repository, "add", ".agcoord")
            _git(self.repository, "commit", "-m", "declare overflow")
            if kind != "check":
                _git(self.repository, "push", "origin", self.branch)
                self.base["AGCOORD_TEST_HEAD"] = _head(self.repository)

    def submit(
        self,
        client: CoordinatorClient,
        environment: dict[str, str],
        *,
        cpu: int = 1,
    ) -> str:
        common = dict(
            checkout=str(self.repository),
            resources={"cpu": cpu},
            environment=environment,
        )
        if self.kind == "land":
            return client.submit_land("github", 101, _report(self.output), **common)
        return client.submit(_report(self.output), kind=self.kind, **common)

    def observed(self) -> str:
        return self.output.read_text(encoding="utf-8")


def _start(tmp_path: Path, pools: list[dict]) -> tuple[RunningCoordinator, CoordinatorClient]:
    state_dir = tmp_path / "state"
    write_broker_config(state_dir, overflow_pools=pools)
    running = RunningCoordinator(state_dir, capacities={"jobs": 2, "cpu": 2})
    return running, running.start()


@pytest.mark.parametrize("kind", ["check", "full", "land"])
def test_a_local_job_receives_the_workstation_key_when_google_cloud_overflow_is_configured(
    tmp_path: Path, kind: str
):
    home, key = _home(tmp_path, key=True)
    lane = _Lane(tmp_path, kind)
    running, client = _start(tmp_path, [_pool(_executor(tmp_path))])
    try:
        run_id = lane.submit(client, _environment(home, lane.base))
        assert _row(client, run_id, "passed", "failed")["status"] == "passed"
        assert client.status(run_id)["pool"] is None
        assert lane.observed() == str(key)
    finally:
        running.stop()


@pytest.mark.parametrize("submitted", ["/elsewhere/own-key.json", ""])
def test_a_credentials_variable_the_submitter_set_is_kept(tmp_path: Path, submitted: str):
    home, _key = _home(tmp_path, key=True)
    lane = _Lane(tmp_path, "check")
    running, client = _start(tmp_path, [_pool(_executor(tmp_path), "overflow-gcp-run")])
    try:
        environment = _environment(home, lane.base)
        environment[CREDENTIALS] = submitted
        run_id = lane.submit(client, environment)
        assert _row(client, run_id, "passed", "failed")["status"] == "passed"
        assert lane.observed() == submitted
    finally:
        running.stop()


def test_without_the_key_a_local_job_environment_is_unchanged(tmp_path: Path):
    home, key = _home(tmp_path, key=False)
    lane = _Lane(tmp_path, "check")
    running, client = _start(tmp_path, [_pool(_executor(tmp_path))])
    try:
        run_id = lane.submit(client, _environment(home, lane.base))
        assert _row(client, run_id, "passed", "failed")["status"] == "passed"
        assert lane.observed() == UNSET
        assert not key.exists()
    finally:
        running.stop()


@pytest.mark.parametrize("pools", ["none", "ssh"])
def test_without_a_google_cloud_pool_a_local_job_environment_is_unchanged(
    tmp_path: Path, pools: str
):
    home, _key = _home(tmp_path, key=True)
    lane = _Lane(tmp_path, "check")
    executor = _executor(tmp_path)
    configured = (
        []
        if pools == "none"
        else [{**_pool(executor), "executor": [str(executor), "overflow-ssh", "builder@host"]}]
    )
    running, client = _start(tmp_path, configured)
    try:
        run_id = lane.submit(client, _environment(home, lane.base))
        assert _row(client, run_id, "passed", "failed")["status"] == "passed"
        assert lane.observed() == UNSET
    finally:
        running.stop()


@pytest.mark.parametrize("form", ["separate", "joined", "home"])
def test_a_key_named_by_the_pool_replaces_the_default(tmp_path: Path, form: str):
    home, _default = _home(tmp_path, key=True)
    configured = home / "keys" / "result-cache.json"
    configured.parent.mkdir(mode=0o700)
    configured.write_text("{}", encoding="utf-8")
    option = {
        "separate": ("--result-cache-key", str(configured)),
        "joined": (f"--result-cache-key={configured}",),
        "home": ("--result-cache-key", "~/keys/result-cache.json"),
    }[form]
    lane = _Lane(tmp_path, "check")
    running, client = _start(
        tmp_path, [_pool(_executor(tmp_path), "overflow-gcp-batch", *option)]
    )
    try:
        run_id = lane.submit(client, _environment(home, lane.base))
        assert _row(client, run_id, "passed", "failed")["status"] == "passed"
        assert lane.observed() == str(configured)
    finally:
        running.stop()


def test_a_missing_key_named_by_the_pool_leaves_the_environment_unchanged(tmp_path: Path):
    home, _default = _home(tmp_path, key=True)
    lane = _Lane(tmp_path, "check")
    running, client = _start(
        tmp_path,
        [_pool(_executor(tmp_path), "overflow-gcp-batch", "--result-cache-key", "/missing.json")],
    )
    try:
        run_id = lane.submit(client, _environment(home, lane.base))
        assert _row(client, run_id, "passed", "failed")["status"] == "passed"
        assert lane.observed() == UNSET
    finally:
        running.stop()


@pytest.mark.parametrize("kind", ["check", "full", "land"])
def test_a_job_placed_in_a_google_cloud_pool_never_receives_the_workstation_key(
    tmp_path: Path, kind: str
):
    home, _key = _home(tmp_path, key=True)
    lane = _Lane(tmp_path, kind, overflow=True)
    executor = _executor(tmp_path)
    running, client = _start(tmp_path, [_pool(executor)])
    try:
        # Four CPUs do not fit this machine's two, so the job can only run in the pool.
        run_id = lane.submit(client, _environment(home, lane.base), cpu=4)
        assert _row(client, run_id, "passed", "failed")["status"] == "passed"
        assert client.status(run_id)["pool"] == "cloud"
        assert (tmp_path / "executor.credentials").read_text(encoding="utf-8") == UNSET
        assert lane.observed() == UNSET
    finally:
        running.stop()


def test_a_submitter_cannot_name_the_reserved_local_environment(tmp_path: Path):
    home, _key = _home(tmp_path, key=True)
    lane = _Lane(tmp_path, "check")
    running, client = _start(tmp_path, [_pool(_executor(tmp_path))])
    try:
        environment = _environment(home, lane.base)
        environment["_AGCOORD_LOCAL_ENVIRONMENT"] = '{"EXAMPLE": "1"}'
        with pytest.raises(CoordinatorError, match="reserved _AGCOORD_LOCAL_ENVIRONMENT"):
            lane.submit(client, environment)
        assert client.snapshot()["queued"] == []
    finally:
        running.stop()


@pytest.mark.parametrize("configured", ["google-cloud", "no-pool", "no-key"])
def test_agc_run_gives_a_local_run_the_key_only_with_google_cloud_overflow_and_never_warns(
    tmp_path: Path, configured: str
):
    """``agc run`` takes HOME from the submitting shell, like every other variable."""
    home, key = _home(tmp_path, key=configured != "no-key")
    lane = _Lane(tmp_path, "check")
    pools = [] if configured == "no-pool" else [_pool(_executor(tmp_path))]
    running, _client = _start(tmp_path, pools)
    try:
        completed = subprocess.run(
            [
                sys.executable,
                "-m",
                "agcoord",
                "run",
                "--state-dir",
                str(running.paths.state_dir),
                "--checkout",
                str(lane.repository),
                "--resource",
                "cpu=1",
                "--",
                *_report(lane.output),
            ],
            env=_environment(home, lane.base),
            capture_output=True,
            text=True,
            timeout=120,
        )
        assert completed.returncode == 0, completed.stderr
        assert lane.observed() == (str(key) if configured == "google-cloud" else UNSET)
        # Nothing about the key is reported, whether or not a job received it.
        for mentioned in (CREDENTIALS, "result-cache"):
            assert mentioned not in completed.stdout + completed.stderr
    finally:
        running.stop()
