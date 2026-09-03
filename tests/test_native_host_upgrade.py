"""Behavioral contract for the one-command native-host upgrade."""

from __future__ import annotations

import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import tarfile

import pytest

from agcoord import __version__
from agcoord.config import NativeBrokerConfig
from agcoord.native_client import NativeBrokerCommand, NativeClientError
from agcoord.queue import CoordinatorError, RUN_ID_ENV, STATE_DIR_ENV



@pytest.fixture(autouse=True)
def _unpinned_client(monkeypatch, tmp_path: Path) -> None:
    """Own the client's native-host pin so these tests are independent of the release pin.

    A release commit ships a real broker digest, and a pinned client digests the broker inside
    any bundle before staging. These tests exercise host operations with fake bundles and
    must behave the same way in a development checkout and in the release commit itself.
    """
    from agcoord import native_host

    pin = tmp_path / "native_host_pin.json"
    pin.write_text(
        json.dumps({"format": 1, "version": __version__, "broker_sha256": None}),
        encoding="utf-8",
    )
    monkeypatch.setattr(native_host, "PIN_PATH", pin)

IDENTITY = {
    "name": "agcoord-broker",
    "version": __version__,
    "protocol": 5,
    "implementation": "rust-native",
    "build": "sha256:" + "a" * 64,
    "target": "x86_64-unknown-linux-musl",
    "sqlite": "3.53.2",
}


def _sidecar(path: Path) -> None:
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    path.with_name(path.name + ".sha256").write_text(
        f"{digest}  {path.name}\n",
        encoding="ascii",
    )


def _release_bundle(
    tmp_path: Path,
    *,
    identity: dict[str, object] | None = None,
) -> Path:
    bundle = tmp_path / "release"
    bundle.mkdir()
    package = bundle / "agcoord-native-host-x86_64-linux.tar.gz"
    manifest = json.dumps(
        {
            "format": 1,
            "development": False,
            "identity": IDENTITY if identity is None else identity,
            "files": {},
        },
        sort_keys=True,
    ).encode()
    with tarfile.open(package, "w:gz") as archive:
        member = tarfile.TarInfo(
            "./usr/share/doc/agcoord/native-host-manifest.json"
        )
        member.size = len(manifest)
        member.mode = 0o644
        archive.addfile(member, io.BytesIO(manifest))
    _sidecar(package)
    for name in (
        "check-native-host-package",
        "install-native-host",
        "test-native-host-enforcement",
    ):
        helper = bundle / name
        helper.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        helper.chmod(0o644)
        _sidecar(helper)
    return package


def _proof() -> dict[str, object]:
    return {
        "run_id": "check-native-host-proof",
        "status": "passed",
        "exit_status": 0,
        "resource_receipt": {
            "requested": {"jobs": 1, "cpu": 1},
            "applied": {"cpu": 1},
            "peak": {"cpu": 1},
            "events": [],
        },
    }


def _managed_state(state_dir: Path, *, existing_spool: bool) -> None:
    state_dir.mkdir(mode=0o700)
    state_dir.chmod(0o700)
    (state_dir / "config.json").write_text(
        json.dumps(
            {
                "capacities": {"cpu": 8, "jobs": 8},
                "bindings": {
                    "cpu": {
                        "kind": "cpu",
                        "unit": "logical-cpu",
                        "mode": "required",
                        "backend": "cgroup-v2",
                    }
                },
                "cgroup_root": (
                    f"/sys/fs/cgroup/user.slice/user-{os.getuid()}.slice/"
                    f"user@{os.getuid()}.service/app.slice/agcoord-broker.service"
                ),
                "native_broker": {
                    "path": "/usr/libexec/agcoord/agcoord-broker",
                    "allow_development": False,
                    "managed_service": True,
                },
            }
        ),
        encoding="utf-8",
    )
    (state_dir / "config.json").chmod(0o600)
    if existing_spool:
        (state_dir / "queue.sqlite3").write_bytes(b"test-owned queue")
        (state_dir / "queue.sqlite3").chmod(0o600)


def _outgoing_broker(path: Path, *, version: str) -> Path:
    """Write one real executable that reports an outgoing protocol-5 identity."""
    identity = json.dumps(
        {
            "name": "agcoord-broker",
            "version": version,
            "protocol": 5,
            "implementation": "rust-native",
            "build": "development",
            "target": "x86_64-unknown-linux-gnu",
            "sqlite": "3.53.2",
        },
        separators=(",", ":"),
    )
    path.write_text(
        "#!/bin/sh\n"
        'if [ "$1" = identity ] && [ "$2" = --json ]; then\n'
        f"  printf '%s\\n' '{identity}'\n"
        "  exit 0\n"
        "fi\n"
        "exit 97\n",
        encoding="utf-8",
    )
    path.chmod(0o755)
    return path


def _install_fakes(
    monkeypatch,
    *,
    activation_status: int = 0,
    proof=None,
    drain_id: str = "drain-0123456789ab",
    installed_broker: Path | None = None,
    ownership_after: int = 0,
):
    from agcoord import native_host

    timeline: list[tuple[str, object]] = []
    clients: list[object] = []
    real_run = subprocess.run
    probes = {"count": 0}
    monkeypatch.delenv(RUN_ID_ENV, raising=False)
    monkeypatch.delenv(STATE_DIR_ENV, raising=False)

    def fake_run(arguments, **options):
        command = [str(value) for value in arguments]
        if installed_broker is not None and command[:1] == [str(installed_broker)]:
            return real_run(arguments, **options)
        timeline.append(("command", command))
        assert options["check"] is False
        assert options["text"] is True
        if command[:2] == [str(native_host.INSTALLED_BROKER), "identity"]:
            return subprocess.CompletedProcess(command, 0, json.dumps(IDENTITY), "")
        if "activate" in command:
            return subprocess.CompletedProcess(
                command,
                activation_status,
                "",
                "activation refused" if activation_status else "activated\n",
            )
        return subprocess.CompletedProcess(command, 0, "ok\n", "")

    class Client:
        def __init__(
            self,
            *,
            state_dir=None,
            checkout=None,
            autostart=True,
            host_maintenance=False,
        ):
            self.state_dir = state_dir
            self.checkout = checkout
            self.autostart = autostart
            self.host_maintenance = host_maintenance
            clients.append(self)

        def drain(self, *, reason, wait):
            timeline.append(("drain", {"reason": reason, "wait": wait}))
            if installed_broker is not None:
                select = (
                    NativeBrokerCommand.select_for_host_maintenance
                    if self.host_maintenance
                    else NativeBrokerCommand.select
                )
                try:
                    command = select(
                        NativeBrokerConfig(
                            path=str(installed_broker),
                            allow_development=True,
                        )
                    )
                except NativeClientError as exc:
                    raise CoordinatorError(str(exc)) from exc
                timeline.append(("drain-selected", command.identity.version))
            return {
                "state": "drained",
                "drain_id": drain_id,
                "reason": reason,
                "started_at": "2026-09-02T17:00:00Z",
                "protocol": 5,
                "live": 0,
                "broker_pid": None,
            }

        def resume(self, drain_id):
            timeline.append(("resume", drain_id))
            return {"state": "open", "drain_id": drain_id, "resumed": True}

        def _owned(self) -> bool:
            return probes["count"] >= ownership_after

        def ping(self):
            probes["count"] += 1
            timeline.append(("ping", probes["count"]))
            if not self._owned():
                raise CoordinatorError(f"no gate broker owns {self.state_dir}")
            return {"protocol": 5}

        def submit(self, command, **metadata):
            if not self._owned():
                raise CoordinatorError(f"no gate broker owns {self.state_dir}")
            timeline.append(
                ("proof-submit", {"command": list(command), "metadata": metadata})
            )
            return "check-native-host-proof"

    def fake_wait(client, run_id):
        timeline.append(("proof-wait", run_id))
        return _proof() if proof is None else proof

    monkeypatch.setattr(native_host.subprocess, "run", fake_run)
    monkeypatch.setattr(native_host, "CoordinatorClient", Client)
    monkeypatch.setattr(native_host, "wait", fake_wait)
    return native_host, timeline, clients


def test_upgrade_stages_before_drain_and_proves_the_restarted_host(
    monkeypatch,
    tmp_path: Path,
):
    native_host, timeline, clients = _install_fakes(monkeypatch)
    package = _release_bundle(tmp_path)
    state_dir = tmp_path / "state"
    _managed_state(state_dir, existing_spool=True)
    monkeypatch.setattr(native_host, "MANAGED_STATE_DIR", state_dir.resolve())
    checkout = tmp_path / "checkout"
    checkout.mkdir()

    result = native_host.upgrade_native_host(
        package,
        state_dir=state_dir,
        checkout=checkout,
    )

    installer = package.parent / "install-native-host"
    checker = package.parent / "check-native-host-package"
    probe = package.parent / "test-native-host-enforcement"
    assert timeline == [
        ("command", [str(checker), str(package)]),
        (
            "command",
            [str(native_host.SUDO), str(installer), "stage", str(package)],
        ),
        (
            "drain",
            {"reason": f"native host upgrade to {__version__}", "wait": True},
        ),
        ("command", [str(native_host.SUDO), "-v"]),
        (
            "command",
            [str(native_host.SYSTEMCTL), "--user", "stop", native_host.SERVICE],
        ),
        (
            "command",
            [
                str(native_host.SUDO),
                str(installer),
                "activate",
                str(state_dir),
                "--drain-id",
                "drain-0123456789ab",
            ],
        ),
        (
            "command",
            [str(native_host.SYSTEMCTL), "--user", "daemon-reload"],
        ),
        ("resume", "drain-0123456789ab"),
        (
            "command",
            [str(native_host.SYSTEMCTL), "--user", "start", native_host.SERVICE],
        ),
        (
            "command",
            [str(native_host.SYSTEMCTL), "--user", "is-active", "--quiet", native_host.SERVICE],
        ),
        (
            "command",
            [str(native_host.INSTALLED_BROKER), "identity", "--json"],
        ),
        ("ping", 1),
        (
            "proof-submit",
            {
                "command": [str(probe)],
                "metadata": {
                    "checkout": str(checkout),
                    "kind": "check",
                    "label": f"native host enforcement {__version__}",
                    "resources": {"cpu": 1},
                },
            },
        ),
        ("proof-wait", "check-native-host-proof"),
    ]
    assert all(client.autostart is False for client in clients)
    assert all(
        (package.parent / name).stat().st_mode & 0o777 == 0o755
        for name in (
            "check-native-host-package",
            "install-native-host",
            "test-native-host-enforcement",
        )
    )
    assert result == {
        "state": "complete",
        "operation": "upgrade",
        "version": __version__,
        "package": str(package),
        "drain_id": "drain-0123456789ab",
        "drain": {
            "state": "drained",
            "drain_id": "drain-0123456789ab",
            "reason": f"native host upgrade to {__version__}",
            "started_at": "2026-09-02T17:00:00Z",
            "protocol": 5,
            "live": 0,
            "broker_pid": None,
        },
        "resume": {
            "state": "open",
            "drain_id": "drain-0123456789ab",
            "resumed": True,
        },
        "service": "active",
        "identity": IDENTITY,
        "proof_run_id": "check-native-host-proof",
        "proof": _proof(),
    }


def test_upgrade_drains_a_previous_minor_installed_broker(monkeypatch, tmp_path: Path):
    outgoing = _outgoing_broker(tmp_path / "outgoing-broker", version="0.4.1")
    native_host, timeline, clients = _install_fakes(
        monkeypatch,
        installed_broker=outgoing,
    )
    package = _release_bundle(tmp_path)
    state_dir = tmp_path / "state"
    _managed_state(state_dir, existing_spool=True)
    monkeypatch.setattr(native_host, "MANAGED_STATE_DIR", state_dir.resolve())
    checkout = tmp_path / "checkout"
    checkout.mkdir()

    result = native_host.upgrade_native_host(
        package,
        state_dir=state_dir,
        checkout=checkout,
    )

    assert result["state"] == "complete"
    assert result["version"] == __version__
    assert ("drain-selected", "0.4.1") in timeline
    assert clients[0].host_maintenance is True
    assert clients[1].host_maintenance is False

def test_upgrade_waits_for_spool_ownership_after_starting_the_service(
    monkeypatch,
    tmp_path: Path,
):
    native_host, timeline, clients = _install_fakes(monkeypatch, ownership_after=2)
    package = _release_bundle(tmp_path)
    state_dir = tmp_path / "state"
    _managed_state(state_dir, existing_spool=True)
    monkeypatch.setattr(native_host, "MANAGED_STATE_DIR", state_dir.resolve())
    checkout = tmp_path / "checkout"
    checkout.mkdir()

    result = native_host.upgrade_native_host(
        package,
        state_dir=state_dir,
        checkout=checkout,
    )

    assert result["state"] == "complete"
    assert result["proof_run_id"] == "check-native-host-proof"
    probes = [entry for entry in timeline if entry[0] == "ping"]
    assert len(probes) == 2
    assert timeline.index(("ping", 2)) < next(
        index for index, entry in enumerate(timeline) if entry[0] == "proof-submit"
    )

def test_activation_failure_leaves_the_exact_drain_and_service_stopped(
    monkeypatch,
    tmp_path: Path,
):
    native_host, timeline, _clients = _install_fakes(
        monkeypatch,
        activation_status=1,
    )
    package = _release_bundle(tmp_path)
    state_dir = tmp_path / "state"
    _managed_state(state_dir, existing_spool=True)
    monkeypatch.setattr(native_host, "MANAGED_STATE_DIR", state_dir.resolve())

    with pytest.raises(CoordinatorError) as raised:
        native_host.upgrade_native_host(
            package,
            state_dir=state_dir,
            checkout=tmp_path,
        )

    assert raised.value.code == "native-host-upgrade-incomplete"
    assert "drain-0123456789ab" in str(raised.value)
    assert "service remains stopped" in str(raised.value)
    assert "coordinator remains drained" in str(raised.value)
    commands = [value for kind, value in timeline if kind == "command"]
    assert any("activate" in command for command in commands)
    assert not any("daemon-reload" in command for command in commands)
    assert not any("start" in command for command in commands)
    assert not any(kind == "resume" for kind, _value in timeline)
    assert not any(kind == "proof-submit" for kind, _value in timeline)


def test_invalid_helper_sidecar_refuses_before_privilege_or_drain(
    monkeypatch,
    tmp_path: Path,
):
    native_host, timeline, _clients = _install_fakes(monkeypatch)
    package = _release_bundle(tmp_path)
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    (package.parent / "install-native-host.sha256").write_text(
        f"{'0' * 64}  install-native-host\n",
        encoding="ascii",
    )

    with pytest.raises(CoordinatorError) as raised:
        native_host.upgrade_native_host(
            package,
            state_dir=state_dir,
            checkout=tmp_path,
        )

    assert raised.value.code == "native-host-bundle-invalid"
    assert "checksum" in str(raised.value)
    assert timeline == []


def test_upgrade_refuses_from_an_admitted_job_before_touching_the_bundle(
    monkeypatch,
    tmp_path: Path,
):
    native_host, timeline, _clients = _install_fakes(monkeypatch)
    package = _release_bundle(tmp_path)
    monkeypatch.setenv(RUN_ID_ENV, "check-parent")

    with pytest.raises(CoordinatorError) as raised:
        native_host.upgrade_native_host(package, checkout=tmp_path)

    assert raised.value.code == "native-host-upgrade-nested"
    assert timeline == []


def test_managed_host_operations_refuse_an_ambient_state_override_before_work(
    monkeypatch,
    tmp_path: Path,
):
    native_host, timeline, _clients = _install_fakes(monkeypatch)
    package = _release_bundle(tmp_path)
    monkeypatch.setenv(STATE_DIR_ENV, str(tmp_path / "alternate-state"))

    with pytest.raises(CoordinatorError) as raised:
        native_host.install_native_host(package, checkout=tmp_path)

    assert raised.value.code == "native-host-state-invalid"
    assert "AGCOORD_STATE_DIR" in str(raised.value)
    assert timeline == []


def test_install_prepares_a_fresh_greedy_capacity_and_activates_without_a_drain(
    monkeypatch,
    tmp_path: Path,
):
    native_host, timeline, clients = _install_fakes(monkeypatch)
    package = _release_bundle(tmp_path)
    state_dir = tmp_path / "state"
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    monkeypatch.setattr(native_host, "MANAGED_STATE_DIR", state_dir.resolve())
    monkeypatch.setattr(native_host, "_cpu_capacity", lambda: 6)

    result = native_host.install_native_host(
        package,
        state_dir=state_dir,
        checkout=checkout,
    )

    installer = package.parent / "install-native-host"
    checker = package.parent / "check-native-host-package"
    probe = package.parent / "test-native-host-enforcement"
    assert timeline == [
        ("command", [str(checker), str(package)]),
        (
            "command",
            [str(native_host.SUDO), str(installer), "stage", str(package)],
        ),
        ("command", [str(native_host.SUDO), "-v"]),
        (
            "command",
            [
                str(native_host.SUDO),
                str(installer),
                "activate",
                str(state_dir),
            ],
        ),
        (
            "command",
            [str(native_host.SYSTEMCTL), "--user", "daemon-reload"],
        ),
        (
            "command",
            [
                str(native_host.SYSTEMCTL),
                "--user",
                "enable",
                "--now",
                native_host.SERVICE,
            ],
        ),
        (
            "command",
            [str(native_host.SYSTEMCTL), "--user", "is-active", "--quiet", native_host.SERVICE],
        ),
        (
            "command",
            [str(native_host.INSTALLED_BROKER), "identity", "--json"],
        ),
        ("ping", 1),
        (
            "proof-submit",
            {
                "command": [str(probe)],
                "metadata": {
                    "checkout": str(checkout),
                    "kind": "check",
                    "label": f"native host enforcement {__version__}",
                    "resources": {"cpu": 1},
                },
            },
        ),
        ("proof-wait", "check-native-host-proof"),
    ]
    assert all(client.autostart is False for client in clients)
    assert not any(kind in {"drain", "resume"} for kind, _value in timeline)
    configuration = json.loads((state_dir / "config.json").read_text())
    assert configuration["capacities"] == {"cpu": 6, "jobs": 6}
    assert configuration["bindings"]["cpu"] == {
        "kind": "cpu",
        "unit": "logical-cpu",
        "mode": "required",
        "backend": "cgroup-v2",
    }
    assert configuration["native_broker"] == {
        "path": "/usr/libexec/agcoord/agcoord-broker",
        "allow_development": False,
        "managed_service": True,
    }
    assert state_dir.stat().st_mode & 0o777 == 0o700
    assert (state_dir / "config.json").stat().st_mode & 0o777 == 0o600
    assert result == {
        "state": "complete",
        "operation": "install",
        "version": __version__,
        "package": str(package),
        "service": "active",
        "identity": IDENTITY,
        "proof_run_id": "check-native-host-proof",
        "proof": _proof(),
    }


def test_upgrade_proof_failure_reports_the_exact_open_restarted_state(
    monkeypatch,
    tmp_path: Path,
):
    failed_proof = _proof()
    failed_proof["status"] = "failed"
    failed_proof["exit_status"] = 1
    native_host, timeline, _clients = _install_fakes(
        monkeypatch,
        proof=failed_proof,
    )
    package = _release_bundle(tmp_path)
    state_dir = tmp_path / "state"
    _managed_state(state_dir, existing_spool=True)
    monkeypatch.setattr(native_host, "MANAGED_STATE_DIR", state_dir.resolve())

    with pytest.raises(CoordinatorError) as raised:
        native_host.upgrade_native_host(
            package,
            state_dir=state_dir,
            checkout=tmp_path,
        )

    assert raised.value.code == "native-host-upgrade-incomplete"
    assert "drain-0123456789ab" in str(raised.value)
    assert "coordinator is open" in str(raised.value)
    assert "service was started but verification failed" in str(raised.value)
    assert any(kind == "proof-wait" for kind, _value in timeline)


def test_install_proof_failure_stops_the_unproved_fresh_service(
    monkeypatch,
    tmp_path: Path,
):
    failed_proof = _proof()
    failed_proof["status"] = "failed"
    failed_proof["exit_status"] = 1
    native_host, timeline, _clients = _install_fakes(
        monkeypatch,
        proof=failed_proof,
    )
    package = _release_bundle(tmp_path)
    state_dir = tmp_path / "state"
    monkeypatch.setattr(native_host, "MANAGED_STATE_DIR", state_dir.resolve())

    with pytest.raises(CoordinatorError) as raised:
        native_host.install_native_host(
            package,
            state_dir=state_dir,
            checkout=tmp_path,
        )

    assert raised.value.code == "native-host-install-incomplete"
    assert "unproved service was disabled and stopped" in str(raised.value)
    assert timeline[-1] == (
        "command",
        [
            str(native_host.SYSTEMCTL),
            "--user",
            "disable",
            "--now",
            native_host.SERVICE,
        ],
    )


def test_install_preserves_a_safe_existing_multi_resource_configuration(
    monkeypatch,
    tmp_path: Path,
):
    native_host, _timeline, _clients = _install_fakes(monkeypatch)
    package = _release_bundle(tmp_path)
    state_dir = tmp_path / "state"
    _managed_state(state_dir, existing_spool=False)
    config = state_dir / "config.json"
    document = json.loads(config.read_text(encoding="utf-8"))
    document["capacities"].update(
        {
            "memory": 32 * 1024**3,
            "ramdisk": 8 * 1024**3,
            "disk": 200 * 1024**3,
        }
    )
    config.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
    config.chmod(0o600)
    original = config.read_bytes()
    monkeypatch.setattr(native_host, "MANAGED_STATE_DIR", state_dir.resolve())

    result = native_host.install_native_host(
        package,
        state_dir=state_dir,
        checkout=tmp_path,
    )

    assert result["state"] == "complete"
    assert config.read_bytes() == original


def test_upgrade_refuses_a_malformed_drain_receipt_before_stopping_the_service(
    monkeypatch,
    tmp_path: Path,
):
    native_host, timeline, _clients = _install_fakes(
        monkeypatch,
        drain_id="not-a-drain-id",
    )
    package = _release_bundle(tmp_path)
    state_dir = tmp_path / "state"
    _managed_state(state_dir, existing_spool=True)
    monkeypatch.setattr(native_host, "MANAGED_STATE_DIR", state_dir.resolve())

    with pytest.raises(CoordinatorError) as raised:
        native_host.upgrade_native_host(
            package,
            state_dir=state_dir,
            checkout=tmp_path,
        )

    assert raised.value.code == "native-host-upgrade-drain-invalid"
    commands = [value for kind, value in timeline if kind == "command"]
    assert not any("stop" in command for command in commands)
    assert not any("activate" in command for command in commands)


def test_client_and_host_versions_must_match_before_privilege_or_drain(
    monkeypatch,
    tmp_path: Path,
):
    native_host, timeline, _clients = _install_fakes(monkeypatch)
    mismatched_identity = {**IDENTITY, "version": "0.3.999"}
    package = _release_bundle(tmp_path, identity=mismatched_identity)

    with pytest.raises(CoordinatorError) as raised:
        native_host.upgrade_native_host(package, checkout=tmp_path)

    assert raised.value.code == "native-host-version-mismatch"
    assert "matching client first" in str(raised.value)
    assert timeline == [
        (
            "command",
            [str(package.parent / "check-native-host-package"), str(package)],
        )
    ]


def test_install_refuses_an_existing_queue_before_privileged_host_changes(
    monkeypatch,
    tmp_path: Path,
):
    native_host, timeline, _clients = _install_fakes(monkeypatch)
    package = _release_bundle(tmp_path)
    state_dir = tmp_path / "state"
    _managed_state(state_dir, existing_spool=True)
    monkeypatch.setattr(native_host, "MANAGED_STATE_DIR", state_dir.resolve())

    with pytest.raises(CoordinatorError) as raised:
        native_host.install_native_host(
            package,
            state_dir=state_dir,
            checkout=tmp_path,
        )

    assert raised.value.code == "native-host-state-invalid"
    assert "agc host upgrade" in str(raised.value)
    assert timeline == [
        (
            "command",
            [str(package.parent / "check-native-host-package"), str(package)],
        )
    ]
