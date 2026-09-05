"""Behavioral contract for installing this client's release broker as a user-owned executable."""

from __future__ import annotations

import fcntl
import functools
import hashlib
import http.server
import json
import os
from pathlib import Path
import stat
import threading

import pytest

from agcoord import __version__, native_host
from agcoord.config import NativeBrokerConfig, load_broker_config
from agcoord.native_client import NativeBrokerCommand
from agcoord.queue import CoordinatorError


ASSET = "agcoord-broker-x86_64-unknown-linux-musl"


def _broker_script(path: Path, *, version: str = __version__, salt: str = "") -> Path:
    """A user-owned fake release broker whose bytes, and therefore digest, the test owns."""
    identity = json.dumps(
        {
            "name": "agcoord-broker",
            "version": version,
            "protocol": 5,
            "implementation": "rust-native",
            "build": "sha256:" + "d" * 64,
            "target": "x86_64-unknown-linux-musl",
            "sqlite": "3.51.1",
        },
        separators=(",", ":"),
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "#!/bin/sh\n"
        f"# {salt}\n"
        "if [ \"$1\" = identity ] && [ \"$2\" = --json ]; then\n"
        f"  printf '%s\\n' '{identity}'\n"
        "  exit 0\n"
        "fi\n"
        "exit 97\n",
        encoding="utf-8",
    )
    path.chmod(0o644)
    return path


def _digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.fixture
def pinned(monkeypatch, tmp_path: Path):
    """Give this client a test-owned native-host pin."""
    pin = tmp_path / "native_host_pin.json"
    monkeypatch.setattr(native_host, "PIN_PATH", pin)

    def write(digest: str | None, *, version: str = __version__) -> Path:
        pin.write_text(
            json.dumps({"format": 1, "version": version, "broker_sha256": digest}),
            encoding="utf-8",
        )
        return pin

    return write


@pytest.fixture
def unadmitted(monkeypatch):
    from agcoord.queue import RUN_ID_ENV, STATE_DIR_ENV

    monkeypatch.delenv(RUN_ID_ENV, raising=False)
    monkeypatch.delenv(STATE_DIR_ENV, raising=False)


def test_user_install_places_verifies_and_configures_an_unmanaged_spool(
    pinned,
    unadmitted,
    tmp_path: Path,
):
    source = _broker_script(tmp_path / "download" / ASSET)
    digest = _digest(source)
    pinned(digest)
    destination = tmp_path / "libexec"
    state_dir = tmp_path / "state"

    result = native_host.install_user_broker(
        source,
        state_dir=state_dir,
        destination_dir=destination,
    )

    placed = destination / "agcoord-broker"
    assert result == {
        "state": "complete",
        "operation": "install-user",
        "version": __version__,
        "broker": str(placed),
        "broker_sha256": digest,
        "state_dir": str(state_dir.resolve()),
        "configured": True,
    }
    assert stat.S_IMODE(placed.stat().st_mode) == 0o755
    assert _digest(placed) == digest
    assert stat.S_IMODE(state_dir.stat().st_mode) == 0o700
    config = load_broker_config(state_dir)
    assert config.native_broker == NativeBrokerConfig(
        path=str(placed),
        allow_development=False,
        managed_service=False,
    )
    capacity = native_host._cpu_capacity()
    assert config.capacities == {"cpu": capacity, "jobs": capacity}
    assert NativeBrokerCommand.select(config.native_broker).path == placed

    again = native_host.install_user_broker(
        source,
        state_dir=state_dir,
        destination_dir=destination,
    )
    assert again["configured"] is False
    assert load_broker_config(state_dir).native_broker == config.native_broker
    assert not list(destination.glob(".*partial"))


def test_user_install_refuses_a_broker_that_is_not_the_pinned_release(
    pinned,
    unadmitted,
    tmp_path: Path,
):
    source = _broker_script(tmp_path / "download" / ASSET)
    pinned("e" * 64)
    destination = tmp_path / "libexec"
    state_dir = tmp_path / "state"

    with pytest.raises(CoordinatorError) as refused:
        native_host.install_user_broker(
            source,
            state_dir=state_dir,
            destination_dir=destination,
        )

    assert refused.value.code == "native-host-pin-mismatch"
    assert not destination.exists()
    assert not (state_dir / "config.json").exists()


def test_user_install_refuses_a_spool_configured_for_another_broker(
    pinned,
    unadmitted,
    tmp_path: Path,
):
    source = _broker_script(tmp_path / "download" / ASSET)
    pinned(_digest(source))
    destination = tmp_path / "libexec"
    state_dir = tmp_path / "state"
    state_dir.mkdir(mode=0o700)
    configuration = state_dir / "config.json"
    configuration.write_text(
        json.dumps(
            {
                "capacities": {"jobs": 2},
                "native_broker": {
                    "path": str(tmp_path / "elsewhere" / "agcoord-broker"),
                    "allow_development": True,
                    "managed_service": False,
                },
            }
        ),
        encoding="utf-8",
    )
    configuration.chmod(0o600)
    before = configuration.read_text(encoding="utf-8")

    with pytest.raises(CoordinatorError) as refused:
        native_host.install_user_broker(
            source,
            state_dir=state_dir,
            destination_dir=destination,
        )

    assert refused.value.code == "native-host-user-config-conflict"
    assert str(tmp_path / "elsewhere" / "agcoord-broker") in str(refused.value)
    assert configuration.read_text(encoding="utf-8") == before
    assert not destination.exists()


def test_user_install_refuses_while_a_broker_is_live(pinned, unadmitted, tmp_path: Path):
    source = _broker_script(tmp_path / "download" / ASSET)
    pinned(_digest(source))
    destination = tmp_path / "libexec"
    state_dir = tmp_path / "state"
    state_dir.mkdir(mode=0o700)
    lock = state_dir / "broker.lock"
    lock.touch(mode=0o600)
    descriptor = os.open(lock, os.O_RDWR)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)

        with pytest.raises(CoordinatorError) as refused:
            native_host.install_user_broker(
                source,
                state_dir=state_dir,
                destination_dir=destination,
            )
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)

    assert refused.value.code == "native-host-user-live-broker"
    assert "agc drain" in str(refused.value)
    assert not destination.exists()
    assert not (state_dir / "config.json").exists()


class _Forge:
    """One test-owned HTTP origin that serves exactly one release tag."""

    def __init__(self, root: Path) -> None:
        self.requests: list[str] = []
        handler = functools.partial(self._handler(), directory=str(root))
        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def _handler(self):
        recorder = self.requests

        class Handler(http.server.SimpleHTTPRequestHandler):
            def log_message(self, *arguments):  # pragma: no cover - silence the server
                pass

            def do_GET(self):
                recorder.append(self.path)
                return super().do_GET()

        return Handler

    @property
    def base_url(self) -> str:
        host, port = self.server.server_address[:2]
        return f"http://{host}:{port}"

    def start(self) -> "_Forge":
        self.thread.start()
        return self

    def stop(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=10)
        assert not self.thread.is_alive(), "the test forge did not stop"


@pytest.fixture
def forge(tmp_path: Path):
    """Serve /<owner>/<name>/releases/download/v<version>/<asset> from disk."""
    root = tmp_path / "forge"
    served = root / "xu-hao/agcoord/releases/download" / f"v{__version__}"
    broker = _broker_script(served / ASSET, salt="served")
    (served / f"{ASSET}.sha256").write_text(
        f"{_digest(broker)}  {ASSET}\n",
        encoding="ascii",
    )
    origin = _Forge(root).start()
    yield origin
    origin.stop()


def test_fetch_native_broker_downloads_verifies_and_caches_the_release_asset(
    forge,
    pinned,
    monkeypatch,
    tmp_path: Path,
):
    from agcoord.github_release import fetch_native_broker

    monkeypatch.setenv("AGCOORD_HOST_RELEASE_BASE_URL", forge.base_url)
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    served = tmp_path / "forge/xu-hao/agcoord/releases/download" / f"v{__version__}" / ASSET
    pinned(_digest(served))

    fetched = fetch_native_broker()

    expected = (tmp_path / "cache/agcoord/native-host" / f"v{__version__}" / "broker" / ASSET)
    assert fetched == expected.resolve()
    assert _digest(fetched) == _digest(served)
    assert forge.requests == [
        f"/xu-hao/agcoord/releases/download/v{__version__}/{ASSET}",
        f"/xu-hao/agcoord/releases/download/v{__version__}/{ASSET}.sha256",
    ]

    assert fetch_native_broker() == fetched
    assert len(forge.requests) == 2


def test_fetch_native_broker_refuses_an_asset_that_is_not_the_pinned_release(
    forge,
    pinned,
    monkeypatch,
    tmp_path: Path,
):
    from agcoord.github_release import fetch_native_broker

    monkeypatch.setenv("AGCOORD_HOST_RELEASE_BASE_URL", forge.base_url)
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    pinned("e" * 64)

    with pytest.raises(CoordinatorError) as refused:
        fetch_native_broker()

    assert refused.value.code == "native-host-pin-mismatch"
    cache = tmp_path / "cache/agcoord/native-host" / f"v{__version__}"
    assert not (cache / "broker").exists()
    assert not list(cache.glob("*.partial")) if cache.exists() else True
