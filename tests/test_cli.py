"""Public CLI behavior without reaching a forge or another project distribution."""

from __future__ import annotations

from copy import deepcopy
from io import StringIO
import json
import os
from pathlib import Path
import subprocess
import threading

import pytest

from agcoord import cli
from agcoord.queue import PROTOCOL, CoordinatorError

from conftest import RunningCoordinator


def _row(
    run_id: str,
    status: str,
    kind: str,
    label: str,
    *,
    head_sha: str | None = None,
    gate_run_id: str | None = None,
    publication: dict[str, object] | None = None,
    failure_reason: str | None = None,
    phase: str | None = None,
    gate_exit_status: int | None = None,
) -> dict[str, object]:
    selected_phase = phase or (
        "queued"
        if status == "queued"
        else ("complete" if status in {"passed", "failed", "cancelled", "interrupted"} else "running")
    )
    return {
        "run_id": run_id,
        "sequence": 1,
        "status": status,
        "kind": kind,
        "label": label,
        "agent": "agent-7",
        "repository_id": "repo-abc",
        "repository": "/repos/example.git",
        "worktree_id": "worktree-def",
        "checkout": "/worktrees/example",
        "branch": "feature/example",
        "head_sha": head_sha,
        "barrier": kind in {"full", "merge", "land"},
        "resources": {"jobs": 1, "cpu": 1},
        "blocked_by": [],
        "gate_run_id": gate_run_id,
        "publication": publication,
        "failure_reason": failure_reason,
        "phase": selected_phase,
        "gate_exit_status": gate_exit_status,
        "caller_pid": 4100,
        "command": ["python", "-m", "pytest", "-q"],
        "created_at": "2026-08-30T12:00:00+00:00",
        "started_at": None if status == "queued" else "2026-08-30T12:00:01+00:00",
        "finished_at": "2026-08-30T12:00:02+00:00" if status == "passed" else None,
        "exit_status": 0 if status == "passed" else None,
        "worker_pid": 5100 if status == "running" else None,
        "cancel_requested": False,
        "log_bytes": 11,
        "position": 1 if status == "queued" else None,
    }


def _snapshot() -> dict[str, object]:
    return {
        "protocol": PROTOCOL,
        "broker_pid": 4001,
        "captured_at": "2026-08-30T12:00:03+00:00",
        "capacities": {"jobs": 2, "cpu": 4, "browser": 1},
        "allocations": {"jobs": 1, "cpu": 1, "browser": 0},
        "active": [_row("check-active", "running", "check", "unit tests")],
        "queued": [
            _row(
                "land-waiting",
                "queued",
                "land",
                "gate and publish change 123",
                head_sha="a" * 40,
                publication={"adapter": "github", "request": 123},
            )
        ],
        "recent": [
            _row(
                "full-passed",
                "passed",
                "full",
                "release gate",
                head_sha="a" * 40,
            )
        ],
    }


@pytest.fixture
def fake_client(monkeypatch):
    observations: dict[str, list] = {
        "constructed": [],
        "submitted": [],
        "landed": [],
        "status": [],
        "cancel": [],
        "log": [],
        "clear": [],
        "follow": [],
    }

    class Client:
        def __init__(self, *, state_dir=None, checkout=None, autostart=True):
            observations["constructed"].append(
                {
                    "state_dir": state_dir,
                    "checkout": checkout,
                    "autostart": autostart,
                    "thread": threading.get_ident(),
                }
            )

        def snapshot(self):
            return deepcopy(_snapshot())

        def status(self, run_id):
            observations["status"].append(run_id)
            if run_id == "land-failed":
                return _row(
                    run_id,
                    "failed",
                    "land",
                    "gate and publish change 123",
                    head_sha="a" * 40,
                    publication={"adapter": "github", "request": 123},
                    failure_reason="stale-main",
                    phase="complete",
                    gate_exit_status=0,
                )
            return _row(run_id, "running", "check", "selected job")

        def cancel(self, run_id):
            observations["cancel"].append(run_id)
            return {
                **_row(run_id, "running", "check", "selected job"),
                "cancel_requested": True,
            }

        def log(self, run_id, *, offset=0):
            observations["log"].append((run_id, offset))
            if offset == 0:
                return {
                    "run_id": run_id,
                    "offset": 0,
                    "next_offset": 6,
                    "text": "first\n",
                    "eof": False,
                }
            return {
                "run_id": run_id,
                "offset": 6,
                "next_offset": 13,
                "text": "second\n",
                "eof": True,
            }

        def submit(self, command, **metadata):
            observations["submitted"].append((list(command), metadata))
            return "check-new" if metadata.get("kind", "check") == "check" else "full-new"

        def submit_land(self, adapter, request, command, **metadata):
            observations["landed"].append(
                (adapter, request, list(command), metadata)
            )
            return "land-new"

        def clear(self):
            observations["clear"].append(True)
            return {"cleared": 3}

    def fake_follow(client, run_id, *, out):
        observations["follow"].append((client, run_id))
        print("followed exact job", file=out)
        return 7

    monkeypatch.setattr(cli, "CoordinatorClient", Client)
    monkeypatch.setattr(cli, "follow", fake_follow)
    return observations


def _args(*argv: str):
    return cli.build_parser().parse_args(list(argv))


def test_list_show_log_cancel_and_clear_use_stable_job_records(fake_client, tmp_path: Path):
    state_dir = tmp_path / "state"
    listed = StringIO()
    assert cli.run(_args("--state-dir", str(state_dir), "list"), out=listed) == 0
    rendered = listed.getvalue()
    assert rendered.index("check-active") < rendered.index("land-waiting") < rendered.index(
        "full-passed"
    )
    assert "check" in rendered and "land" in rendered and "full" in rendered
    assert "/repos/example.git" in rendered
    assert fake_client["constructed"][0]["state_dir"] == str(state_dir)

    shown = StringIO()
    assert cli.run(_args("show", "land-failed"), out=shown) == 0
    shown_row = json.loads(shown.getvalue())
    assert shown_row["publication"] == {"adapter": "github", "request": 123}
    assert shown_row["gate_run_id"] is None
    assert shown_row["head_sha"] == "a" * 40
    assert shown_row["phase"] == "complete"
    assert shown_row["gate_exit_status"] == 0
    assert shown_row["failure_reason"] == "stale-main"

    log = StringIO()
    assert cli.run(_args("log", "check-active"), out=log) == 0
    assert log.getvalue() == "first\nsecond\n"
    assert fake_client["log"] == [("check-active", 0), ("check-active", 6)]

    cancelled = StringIO()
    assert cli.run(_args("cancel", "check-active"), out=cancelled) == 0
    assert "check-active" in cancelled.getvalue()
    assert "cancellation" in cancelled.getvalue().lower()

    cleared = StringIO()
    assert cli.run(_args("clear"), out=cleared) == 0
    assert fake_client["clear"] == [True]
    assert "3" in cleared.getvalue()


@pytest.mark.parametrize("command_kind", ["run", "full"])
def test_run_and_full_parse_repeatable_resources_and_follow_the_durable_job(
    fake_client,
    tmp_path: Path,
    command_kind: str,
):
    checkout = tmp_path / "checkout"
    subprocess.run(
        ["git", "init", "-b", "main", str(checkout)],
        check=True,
        capture_output=True,
        text=True,
    )
    subprocess.run(["git", "-C", str(checkout), "config", "user.name", "AGCoord test"], check=True)
    subprocess.run(
        ["git", "-C", str(checkout), "config", "user.email", "agcoord@example.invalid"],
        check=True,
    )
    (checkout / "tracked.txt").write_text("clean\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(checkout), "add", "tracked.txt"], check=True)
    subprocess.run(
        ["git", "-C", str(checkout), "commit", "-m", "clean head"],
        check=True,
        capture_output=True,
        text=True,
    )
    output = StringIO()
    args = _args(
        command_kind,
        "--label",
        "behavior check",
        "--checkout",
        str(checkout),
        "--resource",
        "cpu=2",
        "--resource",
        "browser=1",
        "--",
        "python",
        "-m",
        "pytest",
        "-q",
    )

    assert cli.run(args, out=output) == 7
    command, metadata = fake_client["submitted"][-1]
    assert command == ["python", "-m", "pytest", "-q"]
    assert metadata["checkout"] == str(checkout.resolve())
    assert metadata["kind"] == ("full" if command_kind == "full" else "check")
    assert metadata["label"] == "behavior check"
    assert metadata["resources"] == {"cpu": 2, "browser": 1}
    expected_id = "full-new" if command_kind == "full" else "check-new"
    assert expected_id in output.getvalue()
    assert fake_client["follow"][-1][1] == expected_id


@pytest.mark.parametrize(
    "resource",
    ["cpu", "=1", "cpu=0", "cpu=-1", "cpu=one"],
)
def test_resource_arguments_fail_closed_before_client_construction(
    fake_client,
    resource: str,
):
    fake_client["submitted"].clear()
    try:
        args = _args("run", "--resource", resource, "--", "true")
    except SystemExit:
        return
    with pytest.raises(CoordinatorError):
        cli.run(args, out=StringIO())
    assert fake_client["submitted"] == []


def test_public_full_refuses_a_dirty_checkout_before_accepting_a_receipt(
    tmp_path: Path,
):
    state_dir = tmp_path / "state"
    running = RunningCoordinator(state_dir, capacities={"jobs": 1, "cpu": 1})
    client = running.start()
    checkout = tmp_path / "checkout"
    subprocess.run(
        ["git", "init", "-b", "main", str(checkout)],
        check=True,
        capture_output=True,
        text=True,
    )
    subprocess.run(["git", "-C", str(checkout), "config", "user.name", "AGCoord test"], check=True)
    subprocess.run(
        ["git", "-C", str(checkout), "config", "user.email", "agcoord@example.invalid"],
        check=True,
    )
    (checkout / "tracked.txt").write_text("clean\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(checkout), "add", "tracked.txt"], check=True)
    subprocess.run(
        ["git", "-C", str(checkout), "commit", "-m", "clean head"],
        check=True,
        capture_output=True,
        text=True,
    )
    (checkout / "untracked.txt").write_text("dirty\n", encoding="utf-8")

    try:
        with pytest.raises(CoordinatorError, match="clean|dirty|receipt"):
            cli.run(
                _args(
                    "--state-dir",
                    str(state_dir),
                    "full",
                    "--checkout",
                    str(checkout),
                    "--resource",
                    "cpu=1",
                    "--",
                    "true",
                ),
                out=StringIO(),
            )
        snapshot = client.snapshot()
        assert snapshot["active"] == []
        assert snapshot["queued"] == []
        assert snapshot["recent"] == []
    finally:
        running.stop()


@pytest.mark.parametrize("adapter_arguments", [(), ("--adapter", "github")])
def test_land_dispatches_gate_and_publication_as_one_followed_request(
    fake_client,
    tmp_path: Path,
    adapter_arguments: tuple[str, ...],
):
    checkout = tmp_path / "checkout"
    subprocess.run(
        ["git", "init", "-b", "feature/example", str(checkout)],
        check=True,
        capture_output=True,
        text=True,
    )
    subprocess.run(["git", "-C", str(checkout), "config", "user.name", "AGCoord test"], check=True)
    subprocess.run(
        ["git", "-C", str(checkout), "config", "user.email", "agcoord@example.invalid"],
        check=True,
    )
    (checkout / "tracked.txt").write_text("exact head\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(checkout), "add", "tracked.txt"], check=True)
    subprocess.run(
        ["git", "-C", str(checkout), "commit", "-m", "exact head"],
        check=True,
        capture_output=True,
        text=True,
    )
    output = StringIO()

    assert cli.run(
        _args(
            "land",
            "123",
            *adapter_arguments,
            "--label",
            "gate and publish change 123",
            "--checkout",
            str(checkout),
            "--resource",
            "network=1",
            "--",
            "python",
            "-m",
            "pytest",
            "-q",
        ),
        out=output,
    ) == 7
    adapter, request, command, metadata = fake_client["landed"][-1]
    assert adapter == "github"
    assert request == 123
    assert command == ["python", "-m", "pytest", "-q"]
    assert metadata["checkout"] == str(checkout.resolve())
    assert metadata["label"] == "gate and publish change 123"
    assert metadata["resources"] == {"network": 1}
    assert fake_client["follow"][-1][1] == "land-new"
    assert "land-new" in output.getvalue()


def test_tui_constructs_the_client_lazily(fake_client, monkeypatch):
    from agcoord import tui

    fake_client["constructed"].clear()

    def fake_tui_run(factory):
        assert fake_client["constructed"] == []
        worker = threading.Thread(target=factory)
        worker.start()
        worker.join(timeout=2)
        assert not worker.is_alive()
        return 19

    monkeypatch.setattr(tui, "run", fake_tui_run)
    assert cli.run(_args("tui"), out=StringIO()) == 19
    assert len(fake_client["constructed"]) == 1


def test_migration_is_explicit_and_starts_no_client(fake_client, monkeypatch, tmp_path: Path):
    fake_client["constructed"].clear()
    calls: list[Path] = []

    def fake_migrate(*, state_dir):
        calls.append(Path(state_dir))
        return {"changed": True, "from_protocol": 1, "to_protocol": PROTOCOL}

    monkeypatch.setattr(cli, "migrate_queue", fake_migrate)
    output = StringIO()
    state_dir = tmp_path / "legacy"

    assert cli.run(_args("--state-dir", str(state_dir), "migrate"), out=output) == 0
    assert calls == [state_dir]
    assert fake_client["constructed"] == []
    assert f"protocol 1" in output.getvalue()
    assert str(PROTOCOL) in output.getvalue()
