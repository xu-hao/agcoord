"""Optional GitHub adapter that fetches one matching native-host release bundle.

The coordinator core stays forge-neutral: it accepts a prepared bundle directory and
never learns where that directory came from. Resolving the bundle for an installed
client from GitHub release assets is forge-specific, so it lives here.

Transport digests prove only that a download was not corrupted. A bundle's own
``.sha256`` sidecars travel with the files they describe, so they cannot establish that
a download is the artifact this client was released against. That check belongs to the
native-host pin shipped inside the client itself, and this adapter refuses to fetch
anything for a client that carries neither a pin nor an operator-supplied digest.
"""

from __future__ import annotations

import hashlib
import os
from pathlib import Path
import shutil
import urllib.error
import urllib.parse
import urllib.request

from . import __version__
from .native_host import (
    CHECKER_NAME,
    INSTALLER_NAME,
    PACKAGE_NAME,
    PROBE_NAME,
    require_expected_broker_digest,
)
from .queue import CoordinatorError


ADAPTER = "github"
DEFAULT_BASE_URL = "https://github.com"
DEFAULT_REPOSITORY = "xu-hao/agcoord"
BASE_URL_ENV = "AGCOORD_HOST_RELEASE_BASE_URL"
REPOSITORY_ENV = "AGCOORD_HOST_RELEASE_REPOSITORY"
ASSET_NAMES = (PACKAGE_NAME, CHECKER_NAME, INSTALLER_NAME, PROBE_NAME)
MAX_ASSET_BYTES = 256 * 1024 * 1024
DOWNLOAD_TIMEOUT = 120.0
DOWNLOAD_READ_SIZE = 1024 * 1024
LOOPBACK_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})


def _download_error(message: str, *, code: str = "native-host-download-failed"):
    return CoordinatorError(f"native-host bundle download {message}", code=code)


def bundle_cache(version: str | None = None) -> Path:
    """Return the owner-only cache directory holding one version's bundle."""
    selected = version or __version__
    configured = os.environ.get("XDG_CACHE_HOME")
    root = (
        Path(configured).expanduser()
        if configured
        else Path.home() / ".cache"
    )
    return (root / "agcoord/native-host" / f"v{selected}").resolve()


def _base_url() -> str:
    configured = os.environ.get(BASE_URL_ENV) or DEFAULT_BASE_URL
    parsed = urllib.parse.urlsplit(configured)
    if parsed.scheme == "https":
        return configured.rstrip("/")
    if parsed.scheme == "http" and parsed.hostname in LOOPBACK_HOSTS:
        return configured.rstrip("/")
    raise _download_error(
        f"source {configured!r} is not an https URL; refusing to fetch a native host "
        "over an unprotected transport",
        code="native-host-download-insecure-url",
    )


def _repository() -> str:
    configured = os.environ.get(REPOSITORY_ENV) or DEFAULT_REPOSITORY
    parts = configured.split("/")
    if len(parts) != 2 or not all(parts) or any("/" in part for part in parts):
        raise _download_error(
            f"repository {configured!r} is not an owner/name pair",
            code="native-host-download-invalid-repository",
        )
    return configured


def _asset_url(base: str, repository: str, version: str, name: str) -> str:
    return f"{base}/{repository}/releases/download/v{version}/{name}"


def _sidecar_digest(sidecar: Path, name: str) -> str:
    try:
        recorded = sidecar.read_text(encoding="ascii")
    except (OSError, UnicodeError) as exc:
        raise _download_error(f"sidecar for {name} is unreadable: {exc}") from exc
    fields = recorded.split()
    if len(fields) != 2 or fields[1] != name or len(fields[0]) != 64:
        raise _download_error(f"sidecar for {name} is malformed")
    return fields[0]


def _file_digest(path: Path) -> str:
    reader = hashlib.sha256()
    with path.open("rb") as source:
        while True:
            block = source.read(DOWNLOAD_READ_SIZE)
            if not block:
                break
            reader.update(block)
    return reader.hexdigest()


def _verify_transport(directory: Path) -> None:
    for name in ASSET_NAMES:
        asset = directory / name
        expected = _sidecar_digest(directory / f"{name}.sha256", name)
        actual = _file_digest(asset)
        if actual != expected:
            raise _download_error(
                f"of {name} produced digest {actual} instead of the published "
                f"{expected}; the transfer is corrupt or the source is not serving the "
                "published release",
                code="native-host-download-digest-mismatch",
            )


def _complete(directory: Path) -> bool:
    return all(
        (directory / name).is_file() and (directory / f"{name}.sha256").is_file()
        for name in ASSET_NAMES
    )


def _owner_only_mkdir(path: Path) -> None:
    """Create ``path`` and any missing ancestors owner-only, whatever the umask says.

    ``Path.mkdir(parents=True)`` applies its mode to the leaf only and lets the process
    umask shape every intermediate directory; the cache chain is a trust boundary for the
    bundle the client later stages as root, so each level is created explicitly.
    """
    missing: list[Path] = []
    current = path
    while not current.exists():
        missing.append(current)
        current = current.parent
    for directory in reversed(missing):
        os.mkdir(directory, 0o700)
        os.chmod(directory, 0o700)


def _normalize_modes(directory: Path) -> None:
    """Leave one complete bundle directory owner-only and its assets non-writable by others.

    Also repairs a cache written by a client that inherited a permissive umask, so an intact
    download is reusable instead of being refused by the verifier it is about to feed.
    """
    os.chmod(directory, 0o700)
    for name in ASSET_NAMES:
        for asset in (name, f"{name}.sha256"):
            os.chmod(directory / asset, 0o600)


def _fetch(url: str, target: Path) -> None:
    request = urllib.request.Request(
        url,
        headers={"User-Agent": f"agcoord/{__version__}"},
    )
    try:
        with urllib.request.urlopen(request, timeout=DOWNLOAD_TIMEOUT) as response:
            descriptor = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(descriptor, "wb") as sink:
                remaining = MAX_ASSET_BYTES
                while remaining > 0:
                    block = response.read(min(DOWNLOAD_READ_SIZE, remaining))
                    if not block:
                        return
                    remaining -= len(block)
                    sink.write(block)
                if response.read(1):
                    raise _download_error(f"from {url} exceeded {MAX_ASSET_BYTES} bytes")
    except urllib.error.HTTPError as exc:
        raise _download_error(f"from {url} failed: HTTP {exc.code}") from exc
    except (urllib.error.URLError, OSError) as exc:
        raise _download_error(f"from {url} failed: {exc}") from exc


def fetch_native_host_bundle(
    version: str | None = None,
    *,
    destination: str | os.PathLike[str] | None = None,
    expected_broker: str | None = None,
) -> Path:
    """Materialize one owner-only bundle directory and return its package path.

    A cached directory that is already complete and intact is reused, so repeated
    installs of one version do not refetch. The caller still runs the package checker
    and the digest comparison against these bytes; this refuses up front when no digest
    could establish what the download should be.
    """
    require_expected_broker_digest(expected_broker)
    selected = version or __version__
    target = (
        Path(destination).expanduser().resolve()
        if destination is not None
        else bundle_cache(selected)
    )
    if _complete(target):
        _verify_transport(target)
        _normalize_modes(target)
        return target / PACKAGE_NAME

    base = _base_url()
    repository = _repository()
    staging = target.with_name(f"{target.name}.partial")
    if staging.exists():
        shutil.rmtree(staging)
    try:
        _owner_only_mkdir(staging)
    except OSError as exc:
        raise _download_error(f"cannot create {staging}: {exc}") from exc
    try:
        for name in ASSET_NAMES:
            for asset in (name, f"{name}.sha256"):
                _fetch(_asset_url(base, repository, selected, asset), staging / asset)
        _verify_transport(staging)
        if target.exists():
            shutil.rmtree(target)
        staging.replace(target)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    _normalize_modes(target)
    return target / PACKAGE_NAME
