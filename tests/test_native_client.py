"""Behavioral selection and mixed-generation refusals for the native client boundary."""

from __future__ import annotations

import json
from pathlib import Path
import sqlite3
import stat
import subprocess
from types import SimpleNamespace

import pytest

from agcoord import __version__
from agcoord.config import (
    DEFAULT_NATIVE_BROKER_PATH,
    BrokerConfigError,
    NativeBrokerConfig,
    parse_broker_config,
)
from agcoord.native_client import (
    NativeBrokerCommand,
    NativeBrokerIdentity,
    NativeClientError,
)
from agcoord.queue import (
    NATIVE_IMPLEMENTATION,
    NATIVE_PROTOCOL,
    CoordinatorBroker,
    CoordinatorClient,
    CoordinatorError,
)

from conftest import RunningCoordinator


def _identity_executable(
    path: Path,
    *,
    version: str = "0.5.0",
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


def test_selection_admits_this_release_line_and_refuses_the_previous_one(tmp_path: Path):
    current = _identity_executable(tmp_path / "current", version=__version__)
    selected = NativeBrokerCommand.select(
        NativeBrokerConfig(path=str(current), allow_development=True)
    )
    assert selected.identity.version == __version__

    previous = _identity_executable(tmp_path / "previous", version="0.4.1")
    with pytest.raises(NativeClientError, match="version is unsupported"):
        NativeBrokerCommand.select(
            NativeBrokerConfig(path=str(previous), allow_development=True)
        )


def test_host_maintenance_selection_admits_the_outgoing_broker_line(tmp_path: Path):
    outgoing = _identity_executable(tmp_path / "outgoing", version="0.4.1")
    config = NativeBrokerConfig(path=str(outgoing), allow_development=True)

    with pytest.raises(NativeClientError, match="version is unsupported"):
        NativeBrokerCommand.select(config)

    maintenance = NativeBrokerCommand.select_for_host_maintenance(config)
    assert maintenance.identity.version == "0.4.1"
    assert maintenance.identity.protocol == NATIVE_PROTOCOL
    assert maintenance.identity.implementation == NATIVE_IMPLEMENTATION


def test_host_maintenance_selection_keeps_every_other_trust_boundary(tmp_path: Path):
    wrong_target = _identity_executable(
        tmp_path / "wrong-target",
        version="0.3.2",
        target="aarch64-unknown-linux-gnu",
    )
    with pytest.raises(NativeClientError, match="target is unsupported"):
        NativeBrokerCommand.select_for_host_maintenance(
            NativeBrokerConfig(path=str(wrong_target), allow_development=True)
        )

    unversioned = _identity_executable(tmp_path / "unversioned", version="not-a-version")
    with pytest.raises(NativeClientError, match="version is unsupported"):
        NativeBrokerCommand.select_for_host_maintenance(
            NativeBrokerConfig(path=str(unversioned), allow_development=True)
        )

    development = _identity_executable(tmp_path / "development", version="0.3.2")
    with pytest.raises(NativeClientError, match="owned by root|development build"):
        NativeBrokerCommand.select_for_host_maintenance(
            NativeBrokerConfig(path=str(development), allow_development=False)
        )

def test_release_policy_rejects_a_user_owned_development_binary(tmp_path: Path):
    executable = _identity_executable(tmp_path / "broker")
    with pytest.raises(NativeClientError, match="owned by root|development build"):
        NativeBrokerCommand.select(
            NativeBrokerConfig(path=str(executable), allow_development=False)
        )


def _mock_admitted_release(
    monkeypatch: pytest.MonkeyPatch,
    *,
    executable: Path | None = None,
    proc_overrides: dict[Path, str] | None = None,
    callback_stdout: bytes | None = None,
    callback_returncode: int = 0,
    callback_stderr: bytes = b"",
) -> tuple[NativeBrokerConfig, list[list[str]]]:
    selected = Path(DEFAULT_NATIVE_BROKER_PATH) if executable is None else executable
    details = SimpleNamespace(st_mode=stat.S_IFREG | 0o755, st_uid=65534)
    identity = json.dumps(
        {
            "name": "agcoord-broker",
            "version": "0.5.0",
            "protocol": 5,
            "implementation": "rust-native",
            "build": f"sha256:{'a' * 64}",
            "target": "x86_64-unknown-linux-musl",
            "sqlite": "3.53.2",
        },
        separators=(",", ":"),
    ).encode()
    callback = callback_stdout
    if callback is None:
        callback = json.dumps(
            {
                "ready": True,
                "profile": "agcoord-broker-client",
                "user_namespace_denied": True,
            },
            separators=(",", ":"),
        ).encode()
    proc = {
        Path("/proc/self/uid_map"): "1000 1000 1\n",
        Path("/proc/self/gid_map"): "1000 1000 1\n",
        Path("/proc/self/setgroups"): "deny\n",
        Path("/proc/sys/kernel/overflowuid"): "65534\n",
    }
    proc.update(proc_overrides or {})
    calls: list[list[str]] = []

    monkeypatch.setattr(Path, "lstat", lambda _path: details)
    monkeypatch.setattr(Path, "read_text", lambda path, **_kwargs: proc[path])
    monkeypatch.setattr("agcoord.native_client.os.access", lambda *_args: True)
    monkeypatch.setattr("agcoord.native_client.os.geteuid", lambda: 1000)
    monkeypatch.setattr("agcoord.native_client.os.getegid", lambda: 1000)

    def fake_run(arguments, **_kwargs):
        calls.append(list(arguments))
        if arguments == [str(selected), "identity", "--json"]:
            return subprocess.CompletedProcess(arguments, 0, identity, b"")
        if arguments == [str(selected), "host-client-preflight"]:
            return subprocess.CompletedProcess(
                arguments,
                callback_returncode,
                callback,
                callback_stderr,
            )
        raise AssertionError(f"unexpected native command: {arguments!r}")

    monkeypatch.setattr("agcoord.native_client.subprocess.run", fake_run)
    return (
        NativeBrokerConfig(
            path=str(selected),
            allow_development=False,
            managed_service=True,
        ),
        calls,
    )


def test_admitted_callback_attests_the_fixed_release_when_host_root_is_unmapped(
    monkeypatch: pytest.MonkeyPatch,
):
    executable = Path(DEFAULT_NATIVE_BROKER_PATH)
    configured, calls = _mock_admitted_release(monkeypatch)

    with pytest.raises(NativeClientError, match="owned by root"):
        NativeBrokerCommand.select(configured)

    selected = NativeBrokerCommand.select_for_admitted_callback(configured)
    assert selected.path == executable
    assert selected.identity.build == f"sha256:{'a' * 64}"
    assert calls == [
        [str(executable), "host-client-preflight"],
        [str(executable), "identity", "--json"],
    ]


def test_admitted_callback_never_trusts_an_arbitrary_overflow_owned_path(
    monkeypatch: pytest.MonkeyPatch,
):
    configured, calls = _mock_admitted_release(
        monkeypatch,
        executable=Path("/opt/agcoord/untrusted-broker"),
    )

    with pytest.raises(NativeClientError, match="owned by root"):
        NativeBrokerCommand.select_for_admitted_callback(configured)
    assert calls == []


@pytest.mark.parametrize(
    ("proc_path", "value"),
    [
        (Path("/proc/self/uid_map"), "1000 0 1\n"),
        (Path("/proc/self/gid_map"), "1000 0 1\n"),
        (Path("/proc/self/setgroups"), "allow\n"),
        (Path("/proc/sys/kernel/overflowuid"), "65533\n"),
    ],
)
def test_admitted_callback_rejects_every_inexact_namespace_marker(
    monkeypatch: pytest.MonkeyPatch,
    proc_path: Path,
    value: str,
):
    configured, calls = _mock_admitted_release(
        monkeypatch,
        proc_overrides={proc_path: value},
    )

    with pytest.raises(NativeClientError, match="owned by root"):
        NativeBrokerCommand.select_for_admitted_callback(configured)
    assert calls == []


@pytest.mark.parametrize(
    ("returncode", "stdout", "stderr", "message"),
    [
        (0, b'{"ready":true}', b"", "incompatible JSON shape"),
        (
            1,
            b"",
            b'{"code":"host-client-profile-mismatch","message":"wrong profile"}\n',
            "was refused.*host-client-profile-mismatch",
        ),
    ],
)
def test_admitted_callback_fails_closed_when_native_attestation_is_not_exact(
    monkeypatch: pytest.MonkeyPatch,
    returncode: int,
    stdout: bytes,
    stderr: bytes,
    message: str,
):
    configured, calls = _mock_admitted_release(
        monkeypatch,
        callback_stdout=stdout,
        callback_returncode=returncode,
        callback_stderr=stderr,
    )

    with pytest.raises(NativeClientError, match=message):
        NativeBrokerCommand.select_for_admitted_callback(configured)
    assert calls == [
        [DEFAULT_NATIVE_BROKER_PATH, "host-client-preflight"],
    ]


def test_client_routes_only_exact_admitted_status_through_the_callback_selector(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    from agcoord import queue

    state_dir = tmp_path / "state"
    state_dir.mkdir()
    (state_dir / "config.json").write_text(
        json.dumps(
            {
                "native_broker": {
                    "path": DEFAULT_NATIVE_BROKER_PATH,
                    "allow_development": False,
                    "managed_service": True,
                }
            }
        ),
        encoding="utf-8",
    )
    run_id = "land-callback"
    build = f"sha256:{'a' * 64}"
    identity = NativeBrokerIdentity(
        name="agcoord-broker",
        version="0.5.0",
        protocol=NATIVE_PROTOCOL,
        implementation=NATIVE_IMPLEMENTATION,
        build=build,
        target="x86_64-unknown-linux-musl",
        sqlite="3.53.2",
    )
    callback_calls: list[tuple[str, tuple[str, ...]]] = []

    class CallbackCommand:
        def __init__(self):
            self.identity = identity

        def invoke(self, command, *, state_dir, arguments=()):
            assert Path(state_dir) == state_dir_path
            callback_calls.append((command, tuple(arguments)))
            return {"run_id": run_id, "status": "running"}

    state_dir_path = state_dir.resolve()
    owner = {
        "protocol": NATIVE_PROTOCOL,
        "pid": 4100,
        "capacities": {"jobs": 1},
        "resource_bindings": {},
        "resource_capabilities": {},
        "implementation": NATIVE_IMPLEMENTATION,
        "version": identity.version,
        "build": identity.build,
    }
    monkeypatch.setattr(queue, "_read_broker_owner", lambda _paths: owner)

    def refuse_ordinary_selection(_cls, _configured):
        raise NativeClientError("ordinary native selection was refused")

    monkeypatch.setattr(
        NativeBrokerCommand,
        "select",
        classmethod(refuse_ordinary_selection),
    )
    monkeypatch.setattr(
        NativeBrokerCommand,
        "select_for_admitted_callback",
        classmethod(lambda _cls, _configured: CallbackCommand()),
    )
    monkeypatch.setenv("AGCOORD_RUN_ID", run_id)
    monkeypatch.setenv("AGCOORD_STATE_DIR", str(state_dir_path))
    client = CoordinatorClient(state_dir=state_dir_path, autostart=False)
    monkeypatch.setattr(client, "_maintenance_if_active", lambda: None)

    assert client.admitted_run_status(run_id) == {
        "run_id": run_id,
        "status": "running",
    }
    assert callback_calls == [("status", ("--run-id", run_id))]

    with pytest.raises(CoordinatorError, match="callback run does not match"):
        client.admitted_run_status("land-other")

    with pytest.raises(CoordinatorError, match="ordinary native selection was refused"):
        client.status(run_id)


def test_admitted_callback_never_autostarts_a_missing_broker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    from agcoord import queue

    state_dir = tmp_path / "state"
    monkeypatch.setenv("AGCOORD_RUN_ID", "land-callback")
    monkeypatch.setenv("AGCOORD_STATE_DIR", str(state_dir))
    monkeypatch.setattr(queue, "_read_broker_owner", lambda _paths: None)
    client = CoordinatorClient(
        state_dir=state_dir,
        autostart=True,
        connect_timeout=0.01,
    )
    monkeypatch.setattr(
        client,
        "_start_broker",
        lambda: pytest.fail("an admitted callback attempted to start a broker"),
    )

    with pytest.raises(CoordinatorError, match="no gate broker owns.*callback"):
        client.admitted_run_status("land-callback")


@pytest.mark.parametrize("operation", ["land", "merge"])
def test_native_publication_preserves_a_virtualenv_python_entry_point(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
):
    from agcoord import queue

    checkout = tmp_path / "repository"
    checkout.mkdir()
    subprocess.run(
        ["git", "init", "--initial-branch", "main", str(checkout)],
        check=True,
        capture_output=True,
        text=True,
    )
    subprocess.run(
        ["git", "-C", str(checkout), "config", "user.name", "AGCoord test"],
        check=True,
    )
    subprocess.run(
        [
            "git",
            "-C",
            str(checkout),
            "config",
            "user.email",
            "agcoord@example.invalid",
        ],
        check=True,
    )
    (checkout / "tracked.txt").write_text("tracked\n", encoding="utf-8")
    subprocess.run(
        ["git", "-C", str(checkout), "add", "tracked.txt"],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(checkout), "commit", "-m", "initial"],
        check=True,
        capture_output=True,
        text=True,
    )

    base_python = tmp_path / "base-python"
    base_python.write_text("base\n", encoding="utf-8")
    virtualenv_bin = tmp_path / "venv/bin"
    virtualenv_bin.mkdir(parents=True)
    virtualenv_python = virtualenv_bin / "python"
    virtualenv_python.symlink_to(base_python)
    monkeypatch.setattr(queue.sys, "executable", str(virtualenv_python))

    client = CoordinatorClient(state_dir=tmp_path / "state", autostart=False)
    owner = {"protocol": NATIVE_PROTOCOL, "capacities": {"jobs": 1}}
    submitted: list[str] = []
    monkeypatch.setattr(client, "_ensure_broker", lambda **_kwargs: owner)

    def capture(command: str, arguments=()):
        assert command == "submit"
        submitted.extend(arguments)
        run_id = submitted[submitted.index("--run-id") + 1]
        return {"run_id": run_id}

    monkeypatch.setattr(client, "_native_invoke", capture)
    if operation == "land":
        client.submit_land(
            "github",
            123,
            ["/bin/true"],
            checkout=str(checkout),
            resources={"jobs": 1},
            environment={},
        )
        assert f"_AGCOORD_LAND_PYTHON={virtualenv_python}" in submitted
    else:
        client.submit_merge(
            "github",
            123,
            checkout=str(checkout),
            resources={"jobs": 1},
            environment={},
        )
        separator = submitted.index("--")
        assert submitted[separator + 1] == str(virtualenv_python)


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
