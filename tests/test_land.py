"""Exact-head overflow uses the coordinator's isolated source handoff."""
from __future__ import annotations

import os
from pathlib import Path
import sys
import tarfile

import pytest

from conftest import RunningCoordinator, caller_environment, wait_for, write_broker_config
from test_queue import (
    _git,
    _head,
    _install_land_gh,
    _land_environment,
    _publication_repository,
    _python,
    _row,
)


@pytest.mark.parametrize("kind", ["full", "land"])
def test_exact_head_overflow_hands_off_gate_tree_source(tmp_path: Path, kind: str):
    checkout, _remote, branch, _head_sha = _publication_repository(tmp_path / "repository")
    (checkout / ".agcoord").mkdir()
    (checkout / ".agcoord" / "overflow.json").write_text('{"image":"test/toolchain:1"}')
    _git(checkout, "add", ".agcoord")
    _git(checkout, "commit", "-m", "declare overflow")
    _git(checkout, "push", "origin", branch)
    head = _head(checkout)
    entered, release = tmp_path / "entered", tmp_path / "release"
    archive = tmp_path / "context.tar"
    executor = tmp_path / "executor"
    executor.write_text(
        f"#!{sys.executable}\n"
        "from pathlib import Path\nimport sys, time\n"
        "from agcoord.overflow_image import repository_root, write_build_context\n"
        f"Path({str(entered)!r}).touch()\n"
        f"while not Path({str(release)!r}).exists(): time.sleep(0.01)\n"
        f"with open({str(archive)!r}, 'wb') as stream:\n"
        "    write_build_context(repository_root(Path.cwd()), 'test/toolchain:1', stream)\n"
        "import subprocess\n"
        "raise SystemExit(subprocess.call(sys.argv[sys.argv.index('--')+1:]))\n"
    )
    executor.chmod(0o755)
    state = tmp_path / "state"
    write_broker_config(state, overflow_pools=[{
        "name": "test", "capacities": {"jobs": 1, "cpu": 2},
        "executor": [str(executor)], "max_runtime": 60,
    }])
    broker = RunningCoordinator(state, capacities={"jobs": 2, "cpu": 1})
    client = broker.start()
    try:
        environment = caller_environment()
        command = _python("assert open('tracked.txt').read() == 'candidate\\n'")
        options = dict(checkout=str(checkout), resources={"cpu": 2}, caller_pid=os.getpid())
        if kind == "land":
            environment = _land_environment(
                _install_land_gh(tmp_path), branch=branch, head_sha=head,
                tag="isolated-overflow", event_log=tmp_path / "events",
            )
            run = client.submit_land("github", 123, command, environment=environment, **options)
        else:
            run = client.submit(command, kind="full", environment=environment, **options)
        wait_for(entered.exists, "overflow executor never started")
        (checkout / "tracked.txt").write_text("concurrent edit\n")
        (checkout / "untracked.txt").write_text("private edit\n")
        release.touch()
        row = _row(client, run, "passed", "failed")
        assert row["status"] == "passed", client.log(run)
        assert row["pool"] == "test"
        assert Path(row["checkout"]) != checkout.resolve()
        assert _head(checkout) == head
        assert (checkout / "tracked.txt").read_text() == "concurrent edit\n"
        with tarfile.open(archive) as stream:
            assert stream.extractfile("workspace/tracked.txt").read() == b"candidate\n"
            assert "workspace/untracked.txt" not in stream.getnames()
    finally:
        release.touch()
        broker.stop()
