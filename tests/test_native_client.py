"""Behavioral selection and mixed-generation refusals for the native client boundary."""

from __future__ import annotations

import json
from pathlib import Path
import sqlite3

import pytest

from agcoord.config import (
    DEFAULT_NATIVE_BROKER_PATH,
    BrokerConfigError,
    NativeBrokerConfig,
    parse_broker_config,
)
from agcoord.native_client import NativeBrokerCommand, NativeClientError
from agcoord.queue import CoordinatorBroker, CoordinatorClient, CoordinatorError

from conftest import RunningCoordinator


def _identity_executable(
    path: Path,
    *,
    version: str = "0.3.0",
    build: str = "development",
    target: str = "x86_64-unknown-linux-gnu",
) -> Path:
    identity = json.dumps(
        {
            "name": "agcoord-broker",
            "version": version,
            "protocol": 5,
            "implementation": "rust-native",
            "build": build,
            "target": target,
            "sqlite": "3.51.1",
        },
        separators=(",", ":"),
    )
    path.write_text(
        "#!/bin/sh\n"
        "if [ \"$1\" = identity ] && [ \"$2\" = --json ]; then\n"
        f"  printf '%s\\n' '{identity}'\n"
        "  exit 0\n"
        "fi\n"
        "exit 97\n",
        encoding="utf-8",
    )
    path.chmod(0o755)
    return path


def test_native_broker_configuration_is_strict_and_defaults_to_host_package():
    default = parse_broker_config("{}")
    assert default.native_broker == NativeBrokerConfig(
        path=DEFAULT_NATIVE_BROKER_PATH,
        allow_development=False,
        managed_service=False,
    )

    selected = parse_broker_config(
        json.dumps(
            {
                "native_broker": {
                    "path": "/opt/agcoord/agcoord-broker",
                    "allow_development": True,
                    "managed_service": True,
                }
            }
        )
    )
    assert selected.native_broker == NativeBrokerConfig(
        path="/opt/agcoord/agcoord-broker",
        allow_development=True,
        managed_service=True,
    )

    for invalid in (
        {"native_broker": {}},
        {"native_broker": {"path": "relative"}},
        {"native_broker": {"path": "/bin/true", "allow_development": 1}},
        {"native_broker": {"path": "/bin/true", "managed_service": 1}},
        {"native_broker": {"path": "/bin/true", "fallback": "python"}},
    ):
        with pytest.raises(BrokerConfigError, match="native_broker"):
            parse_broker_config(json.dumps(invalid))


def test_explicit_development_selection_rejects_mutable_symlinked_and_wrong_target_binaries(
    tmp_path: Path,
):
    executable = _identity_executable(tmp_path / "broker")
    selected = NativeBrokerCommand.select(
        NativeBrokerConfig(path=str(executable), allow_development=True)
    )
    assert selected.path == executable
    assert selected.identity.build == "development"

    executable.chmod(0o775)
    with pytest.raises(NativeClientError, match="group- or world-writable"):
        NativeBrokerCommand.select(
            NativeBrokerConfig(path=str(executable), allow_development=True)
        )
    executable.chmod(0o755)

    symlink = tmp_path / "broker-link"
    symlink.symlink_to(executable)
    with pytest.raises(NativeClientError, match="symlink"):
        NativeBrokerCommand.select(
            NativeBrokerConfig(path=str(symlink), allow_development=True)
        )

    wrong_target = _identity_executable(
        tmp_path / "wrong-target",
        target="aarch64-unknown-linux-gnu",
    )
    with pytest.raises(NativeClientError, match="target is unsupported"):
        NativeBrokerCommand.select(
            NativeBrokerConfig(path=str(wrong_target), allow_development=True)
        )

    wrong_version = _identity_executable(tmp_path / "wrong-version", version="99.0.0")
    with pytest.raises(NativeClientError, match="version is unsupported"):
        NativeBrokerCommand.select(
            NativeBrokerConfig(path=str(wrong_version), allow_development=True)
        )


def test_release_policy_rejects_a_user_owned_development_binary(tmp_path: Path):
    executable = _identity_executable(tmp_path / "broker")
    with pytest.raises(NativeClientError, match="owned by root|development build"):
        NativeBrokerCommand.select(
            NativeBrokerConfig(path=str(executable), allow_development=False)
        )


def test_unsupported_platform_refusal_precedes_executable_discovery(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr("agcoord.native_client.platform.system", lambda: "Darwin")
    monkeypatch.setattr("agcoord.native_client.platform.machine", lambda: "arm64")
    with pytest.raises(NativeClientError, match="only Linux x86_64"):
        NativeBrokerCommand.select(
            NativeBrokerConfig(
                path="/does/not/exist/agcoord-broker",
                allow_development=False,
            )
        )


def test_default_client_refuses_a_live_protocol_four_owner_as_actionable_mixed_version(
    tmp_path: Path,
):
    running = RunningCoordinator(tmp_path / "state", capacities={"jobs": 1})
    explicit_legacy_client = running.start()
    try:
        with pytest.raises(CoordinatorError, match="protocol-4|migrate"):
            CoordinatorClient(
                state_dir=running.broker.paths.state_dir,
                autostart=True,
            ).ping()
        with pytest.raises(CoordinatorError, match="protocol-4|migrate"):
            CoordinatorClient(
                state_dir=running.broker.paths.state_dir,
                autostart=True,
            ).snapshot()
        assert explicit_legacy_client.snapshot()["protocol"] == 4
    finally:
        running.stop()


def test_idle_protocol_four_spool_requires_explicit_migration_before_autostart(
    tmp_path: Path,
):
    state_dir = tmp_path / "state"
    CoordinatorBroker(state_dir, capacities={"jobs": 1}, idle_timeout=None).close()

    with pytest.raises(CoordinatorError, match="uses protocol 4|agc migrate"):
        CoordinatorClient(state_dir=state_dir, autostart=True).snapshot()

    with sqlite3.connect(state_dir / "queue.sqlite3") as database:
        protocol = database.execute(
            "SELECT value FROM coordinator_meta WHERE key = 'protocol'"
        ).fetchone()[0]
    assert protocol == "4"
    owner_lock = state_dir / "broker.lock"
    assert not owner_lock.exists() or not owner_lock.read_text(encoding="utf-8")


def test_managed_native_autostart_uses_the_user_service_and_never_spawns_directly(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    from agcoord import queue

    state_dir = tmp_path / "agcoord"
    executable = _identity_executable(tmp_path / "broker")
    state_dir.mkdir()
    (state_dir / "config.json").write_text(
        json.dumps(
            {
                "native_broker": {
                    "path": str(executable),
                    "allow_development": True,
                    "managed_service": True,
                }
            }
        ),
        encoding="utf-8",
    )
    calls: list[list[str]] = []

    def fake_run(arguments, **_kwargs):
        calls.append(list(arguments))
        return __import__("subprocess").CompletedProcess(arguments, 0, b"", b"")

    def forbidden_spawn(*_args, **_kwargs):
        raise AssertionError("managed autostart spawned the broker directly")

    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
    monkeypatch.delenv("AGCOORD_STATE_DIR", raising=False)
    client = CoordinatorClient(
        autostart=True,
        connect_timeout=0.01,
    )
    client._native_command()
    monkeypatch.setattr(queue.subprocess, "run", fake_run)
    monkeypatch.setattr(queue.subprocess, "Popen", forbidden_spawn)
    with pytest.raises(CoordinatorError, match="did not start"):
        client.snapshot()
    assert calls == [["/usr/bin/systemctl", "--user", "start", "agcoord-broker.service"]]
