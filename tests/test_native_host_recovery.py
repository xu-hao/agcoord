"""Guarded host recovery orchestration with owned bundles and offline adapters."""

from __future__ import annotations

import fcntl
import hashlib
from io import BytesIO, StringIO
import json
import os
from pathlib import Path
import subprocess
import tarfile
from types import SimpleNamespace

import pytest

from agcoord import __version__, cli, native_host
from agcoord.queue import CoordinatorError, RUN_ID_ENV, STATE_DIR_ENV


RECOVERY_ID = "recovery-0123456789ab"
BROKER_BYTES = b"owned recovery broker fixture\n"
BROKER_DIGEST = hashlib.sha256(BROKER_BYTES).hexdigest()
IDENTITY = {
    "name": "agcoord-broker", "version": __version__, "protocol": 5,
    "implementation": "rust-native", "build": "sha256:" + "a" * 64,
    "target": "x86_64-unknown-linux-musl", "sqlite": "3.53.2",
}


def _bundle(directory: Path) -> Path:
    bundle = directory / "release"
    bundle.mkdir(mode=0o755)
    package = bundle / "agcoord-native-host-x86_64-linux.tar.gz"
    manifest = json.dumps({"format": 1, "development": False,
                           "identity": IDENTITY, "files": {}}).encode()
    with tarfile.open(package, "w:gz") as archive:
        for name, content, mode in (
            ("./usr/share/doc/agcoord/native-host-manifest.json", manifest, 0o644),
            (native_host.BROKER_NAME, BROKER_BYTES, 0o755),
        ):
            member = tarfile.TarInfo(name)
            member.size, member.mode = len(content), mode
            archive.addfile(member, BytesIO(content))
    for name in ("check-native-host-package", "install-native-host",
                 "test-native-host-enforcement"):
        (bundle / name).write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    for asset in list(bundle.iterdir()):
        asset.chmod(0o644)
        asset.with_name(asset.name + ".sha256").write_text(
            f"{hashlib.sha256(asset.read_bytes()).hexdigest()}  {asset.name}\n",
            encoding="ascii",
        )
    for asset in bundle.iterdir():
        asset.chmod(0o644)
    bundle.chmod(0o755)
    return package


@pytest.fixture
def recovery(monkeypatch, tmp_path):
    monkeypatch.delenv(RUN_ID_ENV, raising=False)
    monkeypatch.delenv(STATE_DIR_ENV, raising=False)
    package = _bundle(tmp_path)
    pin = tmp_path / "pin.json"
    pin.write_text(json.dumps({"format": 1, "version": __version__,
                               "broker_sha256": BROKER_DIGEST}), encoding="utf-8")
    monkeypatch.setattr(native_host, "PIN_PATH", pin)
    state = tmp_path / "state"
    state.mkdir(mode=0o700)
    state.chmod(0o700)
    config = {
        "capacities": {"cpu": 2, "jobs": 2},
        "bindings": {"cpu": {"kind": "cpu", "unit": "logical-cpu",
                              "mode": "required", "backend": "cgroup-v2"}},
        "cgroup_root": (f"/sys/fs/cgroup/user.slice/user-{os.getuid()}.slice/"
                        f"user@{os.getuid()}.service/app.slice/agcoord-broker.service"),
        "native_broker": {"path": str(native_host.INSTALLED_BROKER),
                          "allow_development": False, "managed_service": True},
    }
    (state / "config.json").write_text(json.dumps(config), encoding="utf-8")
    (state / "config.json").chmod(0o600)
    queue = state / "queue.sqlite3"
    queue.write_bytes(b"owned retained queue fixture")
    queue.chmod(0o600)
    monkeypatch.setattr(native_host, "MANAGED_STATE_DIR", state.resolve())
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    observed = SimpleNamespace(
        package=package, pin=pin, state=state, checkout=checkout, queue=queue,
        events=[], guarded=False, parked=False, proof_seen=False,
        proof_statuses=["running", "passed"], applied_cpu=1, settling=0,
        timeout_phase=None, owner_unavailable=False, now=0.0,
        guard_overrides={}, completion_lost_responses=0, completion_refusals=0,
    )

    def run(arguments, **options):
        command = [str(arg) for arg in arguments]
        observed.events.append(("process", command))
        if command[0] in {str(native_host.SUDO), str(native_host.SYSTEMCTL),
                          str(native_host.INSTALLED_BROKER)}:
            assert 0 < options["timeout"] <= native_host.RECOVERY_PHASE_TIMEOUT
        if observed.timeout_phase and observed.timeout_phase in command:
            raise subprocess.TimeoutExpired(command, options["timeout"])
        if "recover" in command:
            observed.guarded = True
        if command[:2] == [str(native_host.INSTALLED_BROKER), "identity"]:
            return subprocess.CompletedProcess(command, 0, json.dumps(IDENTITY), "")
        return subprocess.CompletedProcess(command, 0, "ok\n", "")

    def invoke(command, state_dir, arguments=()):
        assert state_dir == state
        observed.events.append((command, list(arguments)))
        if command == "host-recover-status":
            if not observed.guarded:
                raise CoordinatorError("no guarded recovery is active", code="host-recovery-invalid")
            probe = package.parent / "test-native-host-enforcement"
            return {"format": 1, "recovery_id": RECOVERY_ID, "identity": IDENTITY,
                    "probe": str(probe),
                    "probe_sha256": hashlib.sha256(probe.read_bytes()).hexdigest(),
                    "proof_run_id": None, "attempts": 1, "phase": "guarded", "incident": None,
                    "state": "recovering", "protocol": 5, **observed.guard_overrides}
        assert observed.guarded or command == "host-recover-complete"
        assert arguments[:2] == ["--recovery-id", RECOVERY_ID]
        if command == "host-recover-proof":
            if observed.settling:
                observed.settling -= 1
                raise CoordinatorError("retained workers are settling",
                                       code="host-recovery-settling")
            return {"run_id": "check-owned-recovery-proof"}
        if command == "host-recover-complete":
            assert observed.proof_seen
            assert not observed.parked
            if observed.completion_refusals:
                observed.completion_refusals -= 1
                raise CoordinatorError("owned completion transport unavailable")
            observed.guarded = False
            if observed.completion_lost_responses:
                observed.completion_lost_responses -= 1
                raise CoordinatorError("owned completion committed but response lost")
            return {"state": "open", "recovery_id": RECOVERY_ID,
                    "proof_run_id": "check-owned-recovery-proof"}
        if command == "host-recover-park":
            observed.parked = True
            return {"state": "recovering"}
        raise AssertionError(f"unexpected native operation: {command}")

    class Client:
        def __init__(self, *, state_dir, checkout, autostart):
            assert state_dir == state and checkout == observed.checkout
            assert autostart is False

        def ping(self):
            observed.events.append(("ping", None))
            assert observed.guarded
            if observed.owner_unavailable:
                raise CoordinatorError("owned test service has no owner")
            return {"protocol": 5}

        def _prepare_submission(self, **metadata):
            assert observed.guarded
            return SimpleNamespace(identity=object(), branch="owned-test", head_sha=None,
                                   caller_pid=os.getpid(), environment={})

        def _native_submission_arguments(self, **metadata):
            assert metadata["resources"] == {"jobs": 1, "cpu": 1}
            assert metadata["command"] == [str(package.parent / "test-native-host-enforcement")]
            observed.events.append(("proof-metadata", metadata))
            return ["--run-id", metadata["run_id"]]

        def status(self, run_id):
            assert observed.guarded and run_id == "check-owned-recovery-proof"
            status = observed.proof_statuses[0]
            if len(observed.proof_statuses) > 1:
                observed.proof_statuses.pop(0)
            observed.events.append(("proof-status", status))
            observed.proof_seen = status == "passed"
            return {"run_id": run_id, "status": status,
                    "exit_status": 0 if status == "passed" else 1,
                    "resource_receipt": {"requested": {"cpu": 1},
                                         "applied": {"cpu": observed.applied_cpu},
                                         "peak": {"cpu": 1}, "events": []}}

        def drain(self, **kwargs):
            raise AssertionError("recovery must not drain the ordinary queue")

        def resume(self, *args, **kwargs):
            raise AssertionError("recovery must not resume the ordinary queue")

        def submit(self, *args, **kwargs):
            raise AssertionError("recovery must not submit through the ordinary queue")

    def sleep(seconds):
        observed.now += seconds

    monkeypatch.setattr(native_host.subprocess, "run", run)
    monkeypatch.setattr(native_host, "_recovery_invoke", invoke)
    monkeypatch.setattr(native_host, "CoordinatorClient", Client)
    monkeypatch.setattr(native_host.time, "monotonic", lambda: observed.now)
    monkeypatch.setattr(native_host.time, "sleep", sleep)
    monkeypatch.setattr(native_host, "OWNERSHIP_TIMEOUT", 0.3)
    monkeypatch.setattr(native_host, "OWNERSHIP_POLL_INTERVAL", 0.1)
    monkeypatch.setattr(native_host, "RECOVERY_PROOF_TIMEOUT", 0.3)
    return observed


def _recover(recovery, **kwargs):
    return native_host.recover_native_host(recovery.package, state_dir=recovery.state,
                                          checkout=recovery.checkout, **kwargs)


def _names(recovery):
    return [name for name, _ in recovery.events]


def _assert_parked(recovery):
    assert recovery.guarded and recovery.parked
    assert "host-recover-complete" not in _names(recovery)
    assert recovery.events[-1] == (
        "process", [str(native_host.SYSTEMCTL), "--user", "stop", native_host.SERVICE])
    assert recovery.queue.read_bytes() == b"owned retained queue fixture"


def test_recovery_opens_queue_only_after_enforcement_proof(recovery):
    result = _recover(recovery)
    assert result["state"] == "complete" and result["operation"] == "recover"
    assert result["recovery_id"] == RECOVERY_ID
    assert result["proof_run_id"] == "check-owned-recovery-proof"
    names = _names(recovery)
    assert names.index("host-recover-status") < names.index("ping")
    assert names.index("host-recover-proof") < names.index("proof-status")
    assert names[-1] == "host-recover-complete"
    assert not recovery.guarded and not recovery.parked
    assert recovery.queue.read_bytes() == b"owned retained queue fixture"


@pytest.mark.parametrize("status,applied", [("failed", 1), ("passed", 0)])
def test_failed_or_unenforced_proof_parks_and_stops(recovery, status, applied):
    recovery.proof_statuses = [status]
    recovery.applied_cpu = applied
    with pytest.raises(CoordinatorError) as error:
        _recover(recovery)
    assert error.value.code == "native-host-recovery-incomplete"
    assert error.value.__cause__.code == "native-host-recovery-proof-failed"
    _assert_parked(recovery)


@pytest.mark.parametrize("status", ["queued", "running"])
def test_unfinished_proof_has_bounded_deadline(recovery, status):
    recovery.proof_statuses = [status]
    with pytest.raises(CoordinatorError) as error:
        _recover(recovery)
    assert error.value.__cause__.code == "host-recovery-proof-timeout"
    assert 0.3 <= recovery.now <= 0.5
    _assert_parked(recovery)


def test_settlement_retries_under_guard_then_proves(recovery):
    recovery.settling = 2
    result = _recover(recovery)
    assert result["state"] == "complete"
    assert _names(recovery).count("host-recover-proof") == 3


def test_settlement_timeout_parks_without_admitting_proof(recovery):
    recovery.settling = 100
    with pytest.raises(CoordinatorError) as error:
        _recover(recovery)
    assert error.value.__cause__.code == "host-recovery-settling"
    assert 0.3 <= recovery.now <= 0.5
    assert "proof-status" not in _names(recovery)
    _assert_parked(recovery)


def test_missing_restarted_owner_is_bounded_and_parked(recovery):
    recovery.owner_unavailable = True
    with pytest.raises(CoordinatorError) as error:
        _recover(recovery)
    assert error.value.__cause__.code == "native-host-recovery-verification-failed"
    assert "host-recover-proof" not in _names(recovery)
    assert 0.3 <= recovery.now <= 0.5
    _assert_parked(recovery)


def test_privileged_phase_timeout_retains_guard_and_parks(recovery):
    recovery.timeout_phase = "start"
    with pytest.raises(CoordinatorError) as error:
        _recover(recovery)
    assert isinstance(error.value.__cause__.__cause__, subprocess.TimeoutExpired)
    _assert_parked(recovery)


def test_live_owner_is_refused_before_privileged_phase(recovery):
    with (recovery.state / "broker.lock").open("w") as owner:
        fcntl.flock(owner.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(CoordinatorError) as error:
            _recover(recovery)
    assert error.value.code == "native-host-user-live-broker"
    assert [event for event in recovery.events if event[0] == "process"] == [
        ("process", [str(recovery.package.parent / "check-native-host-package"), str(recovery.package)])]
    assert not recovery.guarded


@pytest.mark.parametrize("digest", [None, "f" * 64])
def test_recovery_always_requires_matching_pin_before_privilege(recovery, digest):
    recovery.pin.write_text(json.dumps({"format": 1, "version": __version__,
                                       "broker_sha256": digest}), encoding="utf-8")
    with pytest.raises(CoordinatorError) as error:
        _recover(recovery, require_pin=False)
    assert error.value.code in {"native-host-unpinned-client", "native-host-pin-mismatch"}
    assert recovery.events == [("process", [str(recovery.package.parent / "check-native-host-package"),
                                           str(recovery.package)])]
    assert not recovery.guarded


@pytest.mark.parametrize("download", [False, True])
def test_cli_recovery_selects_bundle_without_running_an_upgrade(monkeypatch, tmp_path, download):
    from agcoord import github_release

    package = tmp_path / "owned-package.tar.gz"
    calls = []
    result = {"state": "complete", "operation": "recover", "version": __version__,
              "recovery_id": RECOVERY_ID, "service": "active",
              "proof_run_id": "check-owned-recovery-proof"}

    def recover(selected, **options):
        calls.append((selected, options))
        return result

    def fetch(*, expected_broker):
        assert expected_broker is None
        return package

    monkeypatch.setattr(cli, "recover_native_host", recover)
    monkeypatch.setattr(github_release, "fetch_native_host_bundle", fetch)
    arguments = ["--json", "host", "recover", "--download" if download else str(package)]
    output = StringIO()
    assert cli.run(cli.build_parser().parse_args(arguments), out=output) == 0
    assert json.loads(output.getvalue()) == result
    assert calls == [(package, {"state_dir": None, "checkout": Path.cwd(),
                               "require_pin": download, "broker_sha256": None})]


def test_matching_live_recovery_guard_continues_without_reactivation(recovery):
    recovery.guarded = True
    with (recovery.state / "broker.lock").open("w") as owner:
        fcntl.flock(owner.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        result = _recover(recovery)
    assert result["state"] == "complete" and result["recovery_id"] == RECOVERY_ID
    processes = [command for name, command in recovery.events if name == "process"]
    assert processes == [
        [str(recovery.package.parent / "check-native-host-package"), str(recovery.package)],
        [str(native_host.SYSTEMCTL), "--user", "is-active", "--quiet", native_host.SERVICE],
        [str(native_host.INSTALLED_BROKER), "identity", "--json"],
    ]
    names = _names(recovery)
    assert names.index("host-recover-status") < names.index("host-recover-proof")
    assert names[-1] == "host-recover-complete"
    assert names.count("host-recover-proof") == 1
    assert not recovery.guarded and not recovery.parked


@pytest.mark.parametrize("overrides", [
    {"identity": {**IDENTITY, "build": "sha256:" + "f" * 64}},
    {"probe_sha256": "f" * 64},
    {"probe": "/owned-but-different/proof-helper"},
    {"state": "open"},
])
def test_mismatched_live_recovery_guard_refused_without_privileged_phase(recovery, overrides):
    recovery.guarded = True
    recovery.guard_overrides = overrides
    with (recovery.state / "broker.lock").open("w") as owner:
        fcntl.flock(owner.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(CoordinatorError):
            _recover(recovery)
    assert [event for event in recovery.events if event[0] == "process"] == [
        ("process", [str(recovery.package.parent / "check-native-host-package"), str(recovery.package)])]
    assert "host-recover-proof" not in _names(recovery)
    assert "host-recover-complete" not in _names(recovery)
    assert recovery.guarded and not recovery.parked


def test_completion_response_loss_retries_same_id_without_rerunning_proof(recovery):
    recovery.completion_lost_responses = 1
    result = _recover(recovery)
    assert result["state"] == "complete"
    completions = [arguments for name, arguments in recovery.events if name == "host-recover-complete"]
    assert completions == [["--recovery-id", RECOVERY_ID]] * 2
    assert _names(recovery).count("host-recover-proof") == 1
    assert _names(recovery).count("proof-metadata") == 1
    assert not recovery.guarded and not recovery.parked
    assert recovery.queue.read_bytes() == b"owned retained queue fixture"


def test_completion_transport_failure_has_bounded_retry_then_parks(recovery):
    recovery.completion_refusals = 100
    with pytest.raises(CoordinatorError):
        _recover(recovery)
    completions = [arguments for name, arguments in recovery.events if name == "host-recover-complete"]
    assert completions == [["--recovery-id", RECOVERY_ID]] * 3
    assert _names(recovery).count("host-recover-proof") == 1
    _assert_parked(recovery)
