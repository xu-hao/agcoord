"""Behavioral contract for fetching one matching native-host bundle from a forge."""

from __future__ import annotations

import functools
import hashlib
import http.server
import io
import json
from pathlib import Path
import subprocess
import tarfile
import threading

import pytest

from agcoord import __version__
from agcoord.queue import CoordinatorError


IDENTITY = {
    "name": "agcoord-broker",
    "version": __version__,
    "protocol": 5,
    "implementation": "rust-native",
    "build": "sha256:" + "a" * 64,
    "target": "x86_64-unknown-linux-musl",
    "sqlite": "3.53.2",
}
BROKER_MEMBER = "./usr/libexec/agcoord/agcoord-broker"
PACKAGE_NAME = "agcoord-native-host-x86_64-linux.tar.gz"
HELPERS = (
    "check-native-host-package",
    "install-native-host",
    "test-native-host-enforcement",
)


def _sidecar(path: Path) -> None:
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    path.with_name(path.name + ".sha256").write_text(
        f"{digest}  {path.name}\n",
        encoding="ascii",
    )


def _strip_group_world_write(directory: Path) -> None:
    """Make a staged bundle's modes umask-independent for the mode checks it must pass.

    A bundle directory or asset created under a group-writable umask would be refused by
    the native-host verification exactly as a real tampered bundle is; the fixtures own the
    modes they rely on instead of inheriting the caller's umask.
    """
    directory.chmod(0o755)
    for entry in directory.iterdir():
        entry.chmod(entry.stat().st_mode & ~0o022)


def _release(directory: Path, *, broker: bytes = b"static broker bytes\n") -> Path:
    """Write one publishable release layout and return the served directory."""
    directory.mkdir(parents=True, exist_ok=True)
    package = directory / PACKAGE_NAME
    manifest = json.dumps(
        {
            "format": 1,
            "development": False,
            "identity": IDENTITY,
            "files": {},
        },
        sort_keys=True,
    ).encode()
    with tarfile.open(package, "w:gz") as archive:
        entry = tarfile.TarInfo("./usr/share/doc/agcoord/native-host-manifest.json")
        entry.size = len(manifest)
        entry.mode = 0o644
        archive.addfile(entry, io.BytesIO(manifest))
        entry = tarfile.TarInfo(BROKER_MEMBER)
        entry.size = len(broker)
        entry.mode = 0o755
        archive.addfile(entry, io.BytesIO(broker))
    _sidecar(package)
    for name in HELPERS:
        helper = directory / name
        helper.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        helper.chmod(0o644)
        _sidecar(helper)
    _strip_group_world_write(directory)
    return directory


class _Forge:
    """One test-owned HTTP origin that serves exactly one release tag."""

    def __init__(self, root: Path) -> None:
        self.requests: list[str] = []
        self.root = root
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
    _release(served)
    origin = _Forge(root).start()
    yield origin
    origin.stop()


@pytest.fixture
def pinned(monkeypatch, tmp_path: Path):
    """Give this client a test-owned native-host pin."""
    from agcoord import native_host

    pin = tmp_path / "native_host_pin.json"
    monkeypatch.setattr(native_host, "PIN_PATH", pin)

    def write(digest: str | None, *, version: str = __version__) -> Path:
        pin.write_text(
            json.dumps({"format": 1, "version": version, "broker_sha256": digest}),
            encoding="utf-8",
        )
        return pin

    return write


def _served_broker_digest(broker: bytes = b"static broker bytes\n") -> str:
    return hashlib.sha256(broker).hexdigest()


def _cache(monkeypatch, tmp_path: Path) -> Path:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    return (tmp_path / "cache/agcoord/native-host" / f"v{__version__}").resolve()


def _install_fakes(monkeypatch, tmp_path: Path) -> tuple[object, list, Path]:
    """Own every privileged and coordinator seam install_native_host would touch."""
    from agcoord import native_host
    from agcoord.queue import RUN_ID_ENV, STATE_DIR_ENV

    timeline: list[tuple[str, object]] = []
    monkeypatch.delenv(RUN_ID_ENV, raising=False)
    monkeypatch.delenv(STATE_DIR_ENV, raising=False)

    def fake_run(arguments, **options):
        command = [str(value) for value in arguments]
        timeline.append(("command", command))
        if command[:2] == [str(native_host.INSTALLED_BROKER), "identity"]:
            return subprocess.CompletedProcess(command, 0, json.dumps(IDENTITY), "")
        return subprocess.CompletedProcess(command, 0, "ok\n", "")

    class Client:
        def __init__(self, *, state_dir=None, checkout=None, **options):
            self.state_dir = state_dir

        def ping(self):
            return {"protocol": 5}

        def submit(self, command, **metadata):
            timeline.append(("proof-submit", list(command)))
            return "check-native-host-proof"

    def fake_wait(client, run_id):
        return {
            "run_id": run_id,
            "status": "passed",
            "exit_status": 0,
            "resource_receipt": {
                "requested": {"jobs": 1, "cpu": 1},
                "applied": {"cpu": 1},
                "peak": {"cpu": 1},
                "events": [],
            },
        }

    state_dir = tmp_path / "state"
    state_dir.mkdir(mode=0o700)
    monkeypatch.setattr(native_host, "MANAGED_STATE_DIR", state_dir.resolve())
    monkeypatch.setattr(native_host.subprocess, "run", fake_run)
    monkeypatch.setattr(native_host, "CoordinatorClient", Client)
    monkeypatch.setattr(native_host, "wait", fake_wait)
    return native_host, timeline, state_dir


def test_a_downloaded_bundle_installs_the_matching_native_host(
    monkeypatch,
    tmp_path: Path,
    forge,
    pinned,
):
    from agcoord import github_release

    pinned(_served_broker_digest())
    monkeypatch.setenv(github_release.BASE_URL_ENV, forge.base_url)
    cache = _cache(monkeypatch, tmp_path)
    native_host, timeline, state_dir = _install_fakes(monkeypatch, tmp_path)
    checkout = tmp_path / "checkout"
    checkout.mkdir()

    package = github_release.fetch_native_host_bundle()
    result = native_host.install_native_host(
        package,
        state_dir=state_dir,
        checkout=checkout,
        require_pin=True,
    )

    assert package == cache / PACKAGE_NAME
    assert cache.stat().st_mode & 0o077 == 0
    for name in (PACKAGE_NAME, *HELPERS):
        assert (cache / name).is_file()
        assert (cache / f"{name}.sha256").is_file()
    assert result["state"] == "complete"
    assert result["package"] == str(package)
    commands = [entry[1] for entry in timeline if entry[0] == "command"]
    assert [str(cache / "check-native-host-package"), str(package)] in commands
    assert any("stage" in command for command in commands)


def test_download_refuses_a_bundle_that_is_not_the_pinned_broker(
    monkeypatch,
    tmp_path: Path,
    forge,
    pinned,
):
    from agcoord import github_release

    pinned(hashlib.sha256(b"a different broker\n").hexdigest())
    monkeypatch.setenv(github_release.BASE_URL_ENV, forge.base_url)
    _cache(monkeypatch, tmp_path)
    native_host, timeline, state_dir = _install_fakes(monkeypatch, tmp_path)
    checkout = tmp_path / "checkout"
    checkout.mkdir()

    package = github_release.fetch_native_host_bundle()
    with pytest.raises(CoordinatorError) as failure:
        native_host.install_native_host(
            package,
            state_dir=state_dir,
            checkout=checkout,
            require_pin=True,
        )

    assert failure.value.code == "native-host-pin-mismatch"
    assert "pinned by this agc" in str(failure.value)
    commands = [entry[1] for entry in timeline if entry[0] == "command"]
    assert not any("stage" in command for command in commands), (
        "a package that is not the pinned broker must be refused before staging"
    )


def test_an_unpinned_client_refuses_to_fetch_anything(
    monkeypatch,
    tmp_path: Path,
    forge,
    pinned,
):
    from agcoord import github_release

    pinned(None)
    monkeypatch.setenv(github_release.BASE_URL_ENV, forge.base_url)
    cache = _cache(monkeypatch, tmp_path)

    with pytest.raises(CoordinatorError) as failure:
        github_release.fetch_native_host_bundle()

    assert failure.value.code == "native-host-unpinned-client"
    assert forge.requests == []
    assert not cache.exists()


def test_a_pin_left_by_another_version_does_not_authorize_a_download(
    monkeypatch,
    tmp_path: Path,
    forge,
    pinned,
):
    from agcoord import github_release

    pinned(_served_broker_digest(), version="0.0.1")
    monkeypatch.setenv(github_release.BASE_URL_ENV, forge.base_url)
    _cache(monkeypatch, tmp_path)

    with pytest.raises(CoordinatorError) as failure:
        github_release.fetch_native_host_bundle()

    assert failure.value.code == "native-host-unpinned-client"
    assert forge.requests == []


def test_a_verified_cache_is_reused_without_refetching(
    monkeypatch,
    tmp_path: Path,
    forge,
    pinned,
):
    from agcoord import github_release

    pinned(_served_broker_digest())
    monkeypatch.setenv(github_release.BASE_URL_ENV, forge.base_url)
    _cache(monkeypatch, tmp_path)

    first = github_release.fetch_native_host_bundle()
    fetched = len(forge.requests)
    second = github_release.fetch_native_host_bundle()

    assert fetched == 8
    assert second == first
    assert len(forge.requests) == fetched


def test_a_corrupted_transfer_leaves_no_partial_bundle(
    monkeypatch,
    tmp_path: Path,
    forge,
    pinned,
):
    from agcoord import github_release

    pinned(_served_broker_digest())
    served = forge.root / "xu-hao/agcoord/releases/download" / f"v{__version__}"
    (served / "install-native-host").write_text("#!/bin/sh\nexit 1\n", encoding="utf-8")
    monkeypatch.setenv(github_release.BASE_URL_ENV, forge.base_url)
    cache = _cache(monkeypatch, tmp_path)

    with pytest.raises(CoordinatorError) as failure:
        github_release.fetch_native_host_bundle()

    assert failure.value.code == "native-host-download-digest-mismatch"
    assert not cache.exists()
    assert not cache.with_name(f"{cache.name}.partial").exists()


def test_a_plaintext_source_that_is_not_loopback_is_refused(
    monkeypatch,
    tmp_path: Path,
    pinned,
):
    from agcoord import github_release

    pinned(_served_broker_digest())
    monkeypatch.setenv(github_release.BASE_URL_ENV, "http://releases.example.com")
    cache = _cache(monkeypatch, tmp_path)

    with pytest.raises(CoordinatorError) as failure:
        github_release.fetch_native_host_bundle()

    assert failure.value.code == "native-host-download-insecure-url"
    assert not cache.exists()


def test_the_shipped_pin_is_well_formed():
    from agcoord import native_host

    pin = native_host.native_host_pin()

    assert pin["format"] == 1
    assert pin["version"] == __version__, (
        "the shipped native-host pin names another version; refresh it in the release "
        "commit"
    )
    assert pin["broker_sha256"] is None or len(pin["broker_sha256"]) == 64


def test_an_operator_digest_lets_a_development_client_download(
    monkeypatch,
    tmp_path: Path,
    forge,
    pinned,
):
    from agcoord import github_release

    pinned(None)
    monkeypatch.setenv(github_release.BASE_URL_ENV, forge.base_url)
    cache = _cache(monkeypatch, tmp_path)
    native_host, timeline, state_dir = _install_fakes(monkeypatch, tmp_path)
    checkout = tmp_path / "checkout"
    checkout.mkdir()

    package = github_release.fetch_native_host_bundle(
        expected_broker=_served_broker_digest(),
    )
    result = native_host.install_native_host(
        package,
        state_dir=state_dir,
        checkout=checkout,
        require_pin=True,
        broker_sha256=_served_broker_digest(),
    )

    assert package == cache / PACKAGE_NAME
    assert result["state"] == "complete"
    commands = [entry[1] for entry in timeline if entry[0] == "command"]
    assert any("stage" in command for command in commands)


def test_an_operator_digest_that_the_package_does_not_carry_is_refused(
    monkeypatch,
    tmp_path: Path,
    forge,
    pinned,
):
    from agcoord import github_release

    pinned(None)
    monkeypatch.setenv(github_release.BASE_URL_ENV, forge.base_url)
    _cache(monkeypatch, tmp_path)
    native_host, timeline, state_dir = _install_fakes(monkeypatch, tmp_path)
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    supplied = hashlib.sha256(b"a broker this bundle does not carry\n").hexdigest()

    package = github_release.fetch_native_host_bundle(expected_broker=supplied)
    with pytest.raises(CoordinatorError) as failure:
        native_host.install_native_host(
            package,
            state_dir=state_dir,
            checkout=checkout,
            require_pin=True,
            broker_sha256=supplied,
        )

    assert failure.value.code == "native-host-pin-mismatch"
    commands = [entry[1] for entry in timeline if entry[0] == "command"]
    assert not any("stage" in command for command in commands)


def test_an_operator_digest_never_overrides_a_released_client_s_pin(
    monkeypatch,
    tmp_path: Path,
    forge,
    pinned,
):
    from agcoord import github_release

    pinned(_served_broker_digest())
    monkeypatch.setenv(github_release.BASE_URL_ENV, forge.base_url)
    cache = _cache(monkeypatch, tmp_path)

    with pytest.raises(CoordinatorError) as failure:
        github_release.fetch_native_host_bundle(expected_broker="c" * 64)

    assert failure.value.code == "native-host-pin-conflict"
    assert forge.requests == []
    assert not cache.exists()


def test_an_operator_digest_that_agrees_with_the_shipped_pin_is_accepted(
    monkeypatch,
    tmp_path: Path,
    forge,
    pinned,
):
    from agcoord import github_release

    pinned(_served_broker_digest())
    monkeypatch.setenv(github_release.BASE_URL_ENV, forge.base_url)
    cache = _cache(monkeypatch, tmp_path)

    package = github_release.fetch_native_host_bundle(
        expected_broker=_served_broker_digest(),
    )

    assert package == cache / PACKAGE_NAME


def test_a_malformed_operator_digest_is_refused_before_any_request(
    monkeypatch,
    tmp_path: Path,
    forge,
    pinned,
):
    from agcoord import github_release

    pinned(None)
    monkeypatch.setenv(github_release.BASE_URL_ENV, forge.base_url)
    cache = _cache(monkeypatch, tmp_path)

    with pytest.raises(CoordinatorError) as failure:
        github_release.fetch_native_host_bundle(expected_broker="not-a-digest")

    assert failure.value.code == "native-host-digest-invalid"
    assert forge.requests == []
    assert not cache.exists()


def test_an_operator_digest_enforces_a_comparison_an_unpinned_client_would_skip(
    monkeypatch,
    tmp_path: Path,
    forge,
    pinned,
):
    """A bundle path is checked too, so a supplied digest is not download-only."""
    pinned(None)
    native_host, timeline, state_dir = _install_fakes(monkeypatch, tmp_path)
    bundle = _release(tmp_path / "bundle")
    checkout = tmp_path / "checkout"
    checkout.mkdir()

    with pytest.raises(CoordinatorError) as failure:
        native_host.install_native_host(
            bundle / PACKAGE_NAME,
            state_dir=state_dir,
            checkout=checkout,
            broker_sha256=hashlib.sha256(b"another broker\n").hexdigest(),
        )

    assert failure.value.code == "native-host-pin-mismatch"
    commands = [entry[1] for entry in timeline if entry[0] == "command"]
    assert not any("stage" in command for command in commands)


@pytest.fixture
def permissive_umask():
    """Run one test the way a stock Ubuntu account with a private group does."""
    import os

    previous = os.umask(0o002)
    try:
        yield 0o002
    finally:
        os.umask(previous)


def _writable_by_others(path: Path) -> bool:
    return bool(path.stat().st_mode & 0o022)


def test_downloaded_assets_are_owner_only_regardless_of_umask(
    monkeypatch,
    tmp_path: Path,
    forge,
    pinned,
    permissive_umask,
):
    from agcoord import github_release, native_host
    from agcoord.github_release import ASSET_NAMES

    pinned(_served_broker_digest())
    monkeypatch.setenv(github_release.BASE_URL_ENV, forge.base_url)
    cache = _cache(monkeypatch, tmp_path)

    package = github_release.fetch_native_host_bundle()

    assert package == cache / PACKAGE_NAME
    offenders = [
        str(path.relative_to(cache))
        for name in ASSET_NAMES
        for path in (cache / name, cache / f"{name}.sha256")
        if _writable_by_others(path)
    ]
    assert offenders == [], f"group- or world-writable assets: {offenders}"
    for directory in (cache, cache.parent, cache.parent.parent):
        assert directory.stat().st_mode & 0o077 == 0, f"{directory} is not owner-only"
    # The client's own verifier must accept exactly what the downloader produced.
    selected, installer, probe = native_host._verified_bundle(package)
    assert (selected, installer.name, probe.name) == (
        package,
        "install-native-host",
        "test-native-host-enforcement",
    )
    native_host._verify_pinned_broker(selected, require_pin=True, supplied=None)


def test_a_staged_install_bundle_is_owner_clean_regardless_of_umask(
    tmp_path: Path,
    permissive_umask,
):
    """A fixture-staged install bundle must pass the same mode checks as a downloaded one.

    The client refuses a group- or world-writable bundle directory or asset, so the fixture
    that builds one owns those modes rather than inheriting a permissive caller umask.
    """
    from agcoord import native_host

    bundle = _release(tmp_path / "bundle")
    offenders = [str(bundle)] if _writable_by_others(bundle) else []
    offenders += [
        str(path.relative_to(bundle))
        for path in bundle.iterdir()
        if _writable_by_others(path)
    ]
    assert offenders == [], f"group- or world-writable bundle entries: {offenders}"
    selected, installer, probe = native_host._verified_bundle(bundle / PACKAGE_NAME)
    assert (selected, installer.name, probe.name) == (
        bundle / PACKAGE_NAME,
        "install-native-host",
        "test-native-host-enforcement",
    )


def test_a_cache_left_group_writable_by_an_older_client_is_repaired_on_reuse(
    monkeypatch,
    tmp_path: Path,
    forge,
    pinned,
):
    from agcoord import github_release, native_host
    from agcoord.github_release import ASSET_NAMES

    pinned(_served_broker_digest())
    monkeypatch.setenv(github_release.BASE_URL_ENV, forge.base_url)
    cache = _cache(monkeypatch, tmp_path)
    github_release.fetch_native_host_bundle()
    for name in ASSET_NAMES:
        (cache / name).chmod(0o664)
        (cache / f"{name}.sha256").chmod(0o664)
    requests_before = len(forge.requests)

    package = github_release.fetch_native_host_bundle()

    assert len(forge.requests) == requests_before, "an intact cache must not refetch"
    assert not any(
        _writable_by_others(cache / entry)
        for name in ASSET_NAMES
        for entry in (name, f"{name}.sha256")
    )
    native_host._verified_bundle(package)
