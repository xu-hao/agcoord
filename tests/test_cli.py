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
from agcoord.queue import NATIVE_PROTOCOL, CoordinatorError

from conftest import RunningCoordinator, caller_environment, wait_for


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
        "barrier": kind in {"merge", "land"},
        "resources": {"jobs": 1, "cpu": 1},
        "resource_contract": {
            name: {
                "backend": None,
                "kind": "generic",
                "mode": "admission-only",
                "unit": "admission-unit",
            }
            for name in ("jobs", "cpu")
        },
        "resource_receipt": {
            "requested": {"jobs": 1, "cpu": 1},
            "applied": {},
            "peak": {},
            "events": [],
        },
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
        "protocol": NATIVE_PROTOCOL,
        "broker_pid": 4001,
        "captured_at": "2026-08-30T12:00:03+00:00",
        "capacities": {"jobs": 2, "cpu": 4, "browser": 1},
        "allocations": {"jobs": 1, "cpu": 1, "browser": 0},
        "resource_bindings": {
            "memory": {
                "backend": "cgroup-v2",
                "kind": "memory",
                "mode": "best-effort",
                "unit": "bytes",
            }
        },
        "resource_capabilities": {
            "cgroup-v2": {
                "available": False,
                "kinds": [],
                "units": [],
                "operations": [],
                "reason": "backend-unavailable",
            }
        },
        "maintenance": None,
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
        "admitted_status": [],
        "cancel": [],
        "log": [],
        "clear": [],
        "drain": [],
        "resume": [],
        "follow": [],
        "snapshot": [_snapshot()],
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
            return deepcopy(observations["snapshot"][0])

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

        def admitted_run_status(self, run_id):
            observations["admitted_status"].append(run_id)
            return _row(run_id, "running", "land", "own admitted job")

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

        def drain(self, *, reason="maintenance", wait=True):
            observations["drain"].append({"reason": reason, "wait": wait})
            return {
                "state": "draining" if not wait else "drained",
                "drain_id": "drain-0123456789ab",
                "reason": reason,
                "started_at": "2026-09-02T03:30:00+00:00",
                "protocol": 4,
                "live": 1 if not wait else 0,
                "broker_pid": 4001 if not wait else None,
            }

        def resume(self, drain_id):
            observations["resume"].append(drain_id)
            return {
                "state": "open",
                "drain_id": drain_id,
                "resumed": True,
            }

    def fake_follow(client, run_id, *, out):
        observations["follow"].append((client, run_id))
        print("followed exact job", file=out)
        return 7

    monkeypatch.setattr(cli, "CoordinatorClient", Client)
    monkeypatch.setattr(cli, "follow", fake_follow)
    return observations


def _args(*argv: str):
    return cli.build_parser().parse_args(list(argv))


def test_parser_and_submission_validation_use_agc_command_name(fake_client):
    parser = cli.build_parser()

    assert parser.prog == "agc"
    assert parser.format_usage().startswith("usage: agc")
    with pytest.raises(CoordinatorError, match=r"^agc run needs a command after --$"):
        cli.run(parser.parse_args(["run"]), out=StringIO())


def test_json_cli_keeps_the_machine_readable_drain_refusal(
    monkeypatch,
    tmp_path: Path,
    capsys,
):
    class RefusingClient:
        def __init__(self, **_options):
            pass

        def submit(self, _command, **_metadata):
            raise CoordinatorError(
                "coordinator is draining; new submissions are refused",
                code="broker-draining",
            )

    monkeypatch.setattr(cli, "CoordinatorClient", RefusingClient)

    assert (
        cli.main(
            [
                "--json",
                "run",
                "--checkout",
                str(tmp_path),
                "--",
                "true",
            ]
        )
        == 2
    )
    captured = capsys.readouterr()
    assert captured.out == ""
    assert json.loads(captured.err) == {
        "code": "broker-draining",
        "message": "coordinator is draining; new submissions are refused",
    }


@pytest.mark.parametrize("command", ["run", "full", "land"])
def test_submission_help_describes_the_stable_unnamed_agent_default(
    command: str,
    capsys,
):
    with pytest.raises(SystemExit) as stopped:
        cli.main([command, "--help"])

    assert stopped.value.code == 0
    assert "AGCOORD_AGENT or unnamed" in capsys.readouterr().out


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
    assert "admission-only" in rendered
    assert fake_client["constructed"][0]["state_dir"] == str(state_dir)

    listed_json = StringIO()
    assert cli.run(_args("--json", "list"), out=listed_json) == 0
    machine = json.loads(listed_json.getvalue())
    assert machine["resource_bindings"]["memory"]["unit"] == "bytes"
    assert machine["resource_capabilities"]["cgroup-v2"]["reason"] == (
        "backend-unavailable"
    )

    shown = StringIO()
    assert cli.run(_args("show", "land-failed"), out=shown) == 0
    shown_row = json.loads(shown.getvalue())
    assert shown_row["publication"] == {"adapter": "github", "request": 123}
    assert shown_row["gate_run_id"] is None
    assert shown_row["head_sha"] == "a" * 40
    assert shown_row["phase"] == "complete"
    assert shown_row["gate_exit_status"] == 0
    assert shown_row["failure_reason"] == "stale-main"
    assert shown_row["resource_receipt"]["requested"] == {"cpu": 1, "jobs": 1}

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


def test_human_list_shows_the_durable_maintenance_receipt(fake_client):
    snapshot = fake_client["snapshot"][0]
    snapshot["broker_pid"] = None
    snapshot["maintenance"] = {
        "state": "drained",
        "drain_id": "drain-0123456789ab",
        "reason": "native host upgrade",
        "started_at": "2026-09-02T03:30:00+00:00",
        "protocol": 4,
        "live": 0,
        "broker_pid": None,
    }
    output = StringIO()

    assert cli.run(_args("list"), out=output) == 0
    rendered = output.getvalue()
    assert "drained as drain-0123456789ab" in rendered
    assert "native host upgrade" in rendered
    assert "0 live · broker none" in rendered


def test_show_uses_the_narrow_callback_only_for_its_exact_admitted_run(
    fake_client,
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("AGCOORD_RUN_ID", "land-own")

    own = StringIO()
    assert cli.run(_args("show", "land-own"), out=own) == 0
    assert json.loads(own.getvalue())["run_id"] == "land-own"
    assert fake_client["admitted_status"] == ["land-own"]

    other = StringIO()
    assert cli.run(_args("show", "land-other"), out=other) == 0
    assert json.loads(other.getvalue())["run_id"] == "land-other"
    assert fake_client["status"][-1] == "land-other"


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
            client.submit(
                ["true"],
                checkout=str(checkout),
                kind="full",
                resources={"cpu": 1},
            )
        snapshot = client.snapshot()
        assert snapshot["active"] == []
        assert snapshot["queued"] == []
        assert snapshot["recent"] == []
    finally:
        running.stop()


@pytest.mark.parametrize(
    ("adapter_arguments", "synchronize_target"),
    [
        ((), True),
        (("--adapter", "github"), True),
        (("--no-target-sync",), False),
    ],
)
def test_land_dispatches_gate_and_publication_as_one_followed_request(
    fake_client,
    tmp_path: Path,
    adapter_arguments: tuple[str, ...],
    synchronize_target: bool,
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
    assert metadata["synchronize_target"] is synchronize_target
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

def test_drain_and_resume_are_explicit_non_autostarting_cli_operations(
    fake_client,
    tmp_path: Path,
):
    fake_client["constructed"].clear()
    state_dir = tmp_path / "state"

    drained = StringIO()
    assert cli.run(
        _args(
            "--state-dir",
            str(state_dir),
            "drain",
            "--reason",
            "native host upgrade",
            "--no-wait",
        ),
        out=drained,
    ) == 0
    assert "drain-0123456789ab" in drained.getvalue()
    assert "draining" in drained.getvalue()
    assert fake_client["drain"] == [
        {"reason": "native host upgrade", "wait": False}
    ]

    resumed = StringIO()
    assert cli.run(
        _args("--json", "resume", "drain-0123456789ab"),
        out=resumed,
    ) == 0
    assert json.loads(resumed.getvalue()) == {
        "drain_id": "drain-0123456789ab",
        "resumed": True,
        "state": "open",
    }
    assert fake_client["resume"] == ["drain-0123456789ab"]
    assert fake_client["constructed"] == [
        {
            "state_dir": str(state_dir),
            "checkout": Path.cwd(),
            "autostart": False,
            "thread": threading.get_ident(),
        },
        {
            "state_dir": None,
            "checkout": Path.cwd(),
            "autostart": False,
            "thread": threading.get_ident(),
        },
    ]


def test_native_host_upgrade_is_one_public_cli_operation(monkeypatch, tmp_path: Path):
    package = tmp_path / "agcoord-native-host-x86_64-linux.tar.gz"
    package.write_bytes(b"verified release package")
    state_dir = tmp_path / "state"
    observed: list[dict[str, object]] = []
    result = {
        "state": "complete",
        "version": "0.4.0",
        "drain_id": "drain-0123456789ab",
        "service": "active",
        "proof_run_id": "check-native-host-proof",
    }

    def fake_upgrade(package_path, *, state_dir, checkout, require_pin, broker_sha256):
        observed.append(
            {
                "package": package_path,
                "state_dir": state_dir,
                "checkout": checkout,
                "require_pin": require_pin,
                "broker_sha256": broker_sha256,
            }
        )
        return result

    monkeypatch.setattr(cli, "upgrade_native_host", fake_upgrade, raising=False)
    output = StringIO()

    assert cli.run(
        _args(
            "--json",
            "--state-dir",
            str(state_dir),
            "host",
            "upgrade",
            str(package),
        ),
        out=output,
    ) == 0
    assert json.loads(output.getvalue()) == result
    assert observed == [
        {
            "package": package.resolve(),
            "state_dir": str(state_dir),
            "checkout": Path.cwd(),
            "require_pin": False,
            "broker_sha256": None,
        }
    ]


def _install_result() -> dict[str, object]:
    return {
        "state": "complete",
        "operation": "install",
        "version": "0.4.1",
        "service": "active",
        "proof_run_id": "check-native-host-proof",
    }


def test_native_host_install_downloads_this_client_s_matching_bundle(
    monkeypatch,
    tmp_path: Path,
):
    from agcoord import github_release

    fetched = tmp_path / "cache/agcoord-native-host-x86_64-linux.tar.gz"
    fetched.parent.mkdir(parents=True)
    fetched.write_bytes(b"downloaded release package")
    observed: list[dict[str, object]] = []

    def fake_fetch(*, expected_broker):
        observed.append({"fetched": expected_broker})
        return fetched

    def fake_install(package_path, *, state_dir, checkout, require_pin, broker_sha256):
        observed.append(
            {
                "package": package_path,
                "require_pin": require_pin,
                "broker_sha256": broker_sha256,
            }
        )
        return _install_result()

    monkeypatch.setattr(github_release, "fetch_native_host_bundle", fake_fetch)
    monkeypatch.setattr(cli, "install_native_host", fake_install, raising=False)
    output = StringIO()

    assert cli.run(_args("--json", "host", "install", "--download"), out=output) == 0
    assert json.loads(output.getvalue()) == _install_result()
    assert observed == [
        {"fetched": None},
        {"package": fetched, "require_pin": True, "broker_sha256": None},
    ]


def test_native_host_install_carries_an_operator_digest_to_both_boundaries(
    monkeypatch,
    tmp_path: Path,
):
    from agcoord import github_release

    fetched = tmp_path / "cache/agcoord-native-host-x86_64-linux.tar.gz"
    fetched.parent.mkdir(parents=True)
    fetched.write_bytes(b"downloaded release package")
    supplied = "d" * 64
    observed: list[dict[str, object]] = []

    def fake_fetch(*, expected_broker):
        observed.append({"fetched": expected_broker})
        return fetched

    def fake_install(package_path, *, state_dir, checkout, require_pin, broker_sha256):
        observed.append({"broker_sha256": broker_sha256})
        return _install_result()

    monkeypatch.setattr(github_release, "fetch_native_host_bundle", fake_fetch)
    monkeypatch.setattr(cli, "install_native_host", fake_install, raising=False)
    output = StringIO()

    assert cli.run(
        _args("--json", "host", "install", "--download", "--broker-sha256", supplied),
        out=output,
    ) == 0
    assert observed == [{"fetched": supplied}, {"broker_sha256": supplied}]


def test_native_host_install_refuses_two_bundle_sources(tmp_path: Path):
    package = tmp_path / "agcoord-native-host-x86_64-linux.tar.gz"
    package.write_bytes(b"verified release package")

    with pytest.raises(CoordinatorError) as failure:
        cli.run(_args("host", "install", str(package), "--download"))

    assert failure.value.code == "native-host-bundle-source-conflict"


def test_native_host_install_refuses_no_bundle_source():
    with pytest.raises(CoordinatorError) as failure:
        cli.run(_args("host", "install"))

    assert failure.value.code == "native-host-bundle-source-missing"


def test_native_host_install_user_refuses_another_bundle_source(tmp_path: Path):
    package = tmp_path / "agcoord-native-host-x86_64-linux.tar.gz"
    package.write_bytes(b"verified release package")

    with pytest.raises(CoordinatorError) as failure:
        cli.run(_args("host", "install", "--user", str(package)))
    assert failure.value.code == "native-host-bundle-source-conflict"

    with pytest.raises(CoordinatorError) as failure:
        cli.run(_args("host", "install", "--user", "--download"))
    assert failure.value.code == "native-host-bundle-source-conflict"


def test_native_host_install_user_fetches_then_installs_without_privileges(
    monkeypatch,
    tmp_path: Path,
):
    from agcoord import github_release

    observed: list[tuple[object, ...]] = []
    downloaded = tmp_path / "cache" / "agcoord-broker-x86_64-unknown-linux-musl"
    placed = tmp_path / "libexec" / "agcoord-broker"
    state_dir = tmp_path / "state"

    def fake_fetch(*, expected_broker=None):
        observed.append(("fetch", expected_broker))
        return downloaded

    def fake_install(broker, *, state_dir, broker_sha256):
        observed.append(("install", broker, state_dir, broker_sha256))
        return {
            "state": "complete",
            "operation": "install-user",
            "version": "0.6.2",
            "broker": str(placed),
            "broker_sha256": "f" * 64,
            "state_dir": str(state_dir),
            "configured": True,
        }

    monkeypatch.setattr(github_release, "fetch_native_broker", fake_fetch)
    monkeypatch.setattr(cli, "install_user_broker", fake_install)

    output = StringIO()
    assert (
        cli.run(
            _args("host", "install", "--user", "--state-dir", str(state_dir)),
            out=output,
        )
        == 0
    )
    assert observed == [
        ("fetch", None),
        ("install", downloaded, str(state_dir), None),
    ]
    assert output.getvalue() == (
        f"AGCoord: installed user broker 0.6.2 at {placed}; spool {state_dir} configured\n"
    )

    output = StringIO()
    assert (
        cli.run(
            _args("--json", "host", "install", "--user", "--broker-sha256", "f" * 64),
            out=output,
        )
        == 0
    )
    assert observed[-2:] == [("fetch", "f" * 64), ("install", downloaded, None, "f" * 64)]
    assert json.loads(output.getvalue())["operation"] == "install-user"


def test_native_host_install_refuses_an_unknown_download_adapter():
    with pytest.raises(CoordinatorError) as failure:
        cli.run(_args("host", "install", "--download", "--adapter", "gitlab"))

    assert failure.value.code == "native-host-download-unknown-adapter"


def test_native_host_install_is_one_public_cli_operation(monkeypatch, tmp_path: Path):
    package = tmp_path / "agcoord-native-host-x86_64-linux.tar.gz"
    package.write_bytes(b"verified release package")
    state_dir = tmp_path / "state"
    observed: list[dict[str, object]] = []
    result = {
        "state": "complete",
        "operation": "install",
        "version": "0.4.0",
        "service": "active",
        "proof_run_id": "check-native-host-proof",
    }

    def fake_install(package_path, *, state_dir, checkout, require_pin, broker_sha256):
        observed.append(
            {
                "package": package_path,
                "state_dir": state_dir,
                "checkout": checkout,
                "require_pin": require_pin,
                "broker_sha256": broker_sha256,
            }
        )
        return result

    monkeypatch.setattr(cli, "install_native_host", fake_install, raising=False)
    output = StringIO()

    assert cli.run(
        _args(
            "--json",
            "--state-dir",
            str(state_dir),
            "host",
            "install",
            str(package),
        ),
        out=output,
    ) == 0
    assert json.loads(output.getvalue()) == result
    assert observed == [
        {
            "package": package.resolve(),
            "state_dir": str(state_dir),
            "checkout": Path.cwd(),
            "require_pin": False,
            "broker_sha256": None,
        }
    ]


def _clean_repository(path: Path) -> str:
    subprocess.run(["git", "init", "-q", "-b", "main", str(path)], check=True)
    for key, value in (("user.name", "AGCoord test"), ("user.email", "agcoord@example.invalid")):
        subprocess.run(["git", "-C", str(path), "config", key, value], check=True)
    (path / "tracked.txt").write_text("clean\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(path), "add", "tracked.txt"], check=True)
    subprocess.run(["git", "-C", str(path), "commit", "-q", "-m", "clean head"], check=True)
    return subprocess.run(
        ["git", "-C", str(path), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _verify_from_inside_a_worker(
    tmp_path: Path,
    *,
    head_sha: str | None = None,
) -> tuple[str, str, str]:
    """Run `agc verify-admission` from inside an admitted full worker.

    Returns the subcommand's exit status, its standard error, and the row's terminal status.
    `$$` is the admitted worker itself; the public command runs as its child and names that PID.
    """
    import sys

    running = RunningCoordinator(tmp_path / "state", capacities={"jobs": 1})
    client = running.start()
    checkout = tmp_path / "checkout"
    exact_head = _clean_repository(checkout)
    report = tmp_path / "report.txt"
    stderr_log = tmp_path / "stderr.txt"
    try:
        run_id = client.submit(
            [
                "/bin/sh",
                "-c",
                '"$1" -m agcoord verify-admission --state-dir "$AGCOORD_STATE_DIR" '
                '--checkout "$2" --run-id "$AGCOORD_RUN_ID" --kind "$AGCOORD_RUN_KIND" '
                '--head-sha "$3" --worker-pid $$ 2>"$4"; printf %s $? >"$5"',
                "agcoord-test",
                sys.executable,
                str(checkout),
                head_sha or exact_head,
                str(stderr_log),
                str(report),
            ],
            checkout=str(checkout),
            kind="full",
            label="public admission proof",
            environment=caller_environment(),
        )
        wait_for(
            lambda: client.status(run_id)["status"] in {"passed", "failed", "cancelled"},
            "the admitted worker never reached a terminal status",
        )
        return (
            report.read_text(encoding="utf-8"),
            stderr_log.read_text(encoding="utf-8"),
            client.status(run_id)["status"],
        )
    finally:
        running.stop()


def test_verify_admission_is_a_public_subcommand_for_an_admitted_worker(tmp_path: Path):
    exit_code, stderr, status = _verify_from_inside_a_worker(tmp_path)

    assert exit_code == "0", stderr
    assert status == "passed"
    assert "invalid choice" not in stderr


def test_verify_admission_refuses_a_wrong_head_with_the_verifier_s_message(tmp_path: Path):
    exit_code, stderr, status = _verify_from_inside_a_worker(tmp_path, head_sha="f" * 40)

    assert exit_code == "2", stderr
    assert status == "passed"  # the wrapper decides what a refusal means; the row itself ran
    assert "invalid choice" not in stderr
    assert "error:" in stderr


def _locked_error() -> CoordinatorError:
    return CoordinatorError(
        "cannot inspect gate queue protocol in /spool/queue.sqlite3: database is locked"
    )


def test_follow_retries_a_transient_coordinator_error_and_keeps_the_verdict(
    monkeypatch,
    tmp_path: Path,
    capsys,
):
    from agcoord import queue as queue_module

    monkeypatch.setattr(queue_module, "FOLLOW_RETRY_SECONDS", 2.0)
    calls = {"status": 0}

    class FlakyClient:
        def __init__(self, **_options):
            pass

        def submit(self, _command, **_metadata):
            return "check-flaky"

        def status(self, run_id):
            calls["status"] += 1
            if calls["status"] == 2:
                raise _locked_error()
            return _row(run_id, "running" if calls["status"] < 3 else "passed", "check", "flaky")

        def log(self, run_id, *, offset=0):
            return {"run_id": run_id, "offset": offset, "next_offset": offset, "text": "", "eof": True}

    monkeypatch.setattr(cli, "CoordinatorClient", FlakyClient)

    output = StringIO()
    assert cli.run(_args("run", "--checkout", str(tmp_path), "--", "true"), out=output) == 0
    assert "AGCoord: accepted check-flaky" in output.getvalue()
    assert "lost contact" not in capsys.readouterr().err
    assert calls["status"] >= 3


def test_follow_reports_a_lost_stream_and_exits_75_without_claiming_a_verdict(
    monkeypatch,
    tmp_path: Path,
    capsys,
):
    from agcoord import queue as queue_module

    monkeypatch.setattr(queue_module, "FOLLOW_RETRY_SECONDS", 0.3)

    class LockedClient:
        def __init__(self, **_options):
            pass

        def submit(self, _command, **_metadata):
            return "full-locked"

        def status(self, _run_id):
            raise _locked_error()

        def log(self, _run_id, *, offset=0):
            raise AssertionError("the log is not read once the status poll fails")

    monkeypatch.setattr(cli, "CoordinatorClient", LockedClient)

    output = StringIO()
    assert cli.run(_args("full", "--checkout", str(tmp_path), "--", "true"), out=output) == 75
    captured = capsys.readouterr()
    assert output.getvalue() == "AGCoord: accepted full-locked\n"
    assert "lost contact with the coordinator while following full-locked" in captured.err
    assert "database is locked" in captured.err
    assert "the job continues" in captured.err
    assert "agc log full-locked --follow" in captured.err
    assert "agc show full-locked" in captured.err


def test_json_wait_reports_a_lost_stream_as_a_coded_object_with_the_run_id(
    monkeypatch,
    tmp_path: Path,
    capsys,
):
    from agcoord import queue as queue_module

    monkeypatch.setattr(queue_module, "FOLLOW_RETRY_SECONDS", 0.3)

    class LockedClient:
        def __init__(self, **_options):
            pass

        def submit(self, _command, **_metadata):
            return "check-locked"

        def status(self, _run_id):
            raise _locked_error()

    monkeypatch.setattr(cli, "CoordinatorClient", LockedClient)

    assert cli.main(["--json", "run", "--checkout", str(tmp_path), "--", "true"]) == 75
    captured = capsys.readouterr()
    assert captured.out == ""
    payload = json.loads(captured.err)
    assert payload["code"] == "coordinator-unreachable"
    assert payload["run_id"] == "check-locked"
    assert "database is locked" in payload["message"]
