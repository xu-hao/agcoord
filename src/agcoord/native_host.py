"""Transactional orchestration for verified managed native-host operations."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import shutil
from pathlib import Path
import re
import stat
import subprocess
import tarfile
import time
from typing import Any, Mapping, Sequence

from . import __version__
from .config import (
    DEFAULT_NATIVE_BROKER_PATH,
    BrokerConfigError,
    NativeBrokerConfig,
    config_path,
    load_broker_config,
    parse_broker_config,
)
from .native_client import (
    NATIVE_IMPLEMENTATION,
    NATIVE_PROTOCOL,
    NativeBrokerCommand,
    NativeClientError,
)
from .queue import (
    RUN_ID_ENV,
    CoordinatorPaths,
    STATE_DIR_ENV,
    CoordinatorClient,
    CoordinatorError,
    configured_capacities,
    queue_paths,
    wait,
)
from .resources import ResourceContractError, validate_resource_bindings


MIB = 1024**2
MEMORY_RESERVE_FLOOR = 4 * 1024**3
MEMORY_RESERVE_DIVISOR = 8
TMPFS_BYTES_PER_INODE = 8192
USER_BROKER_PATH = Path.home() / ".local" / "libexec" / "agcoord" / "agcoord-broker"

SUDO = Path("/usr/bin/sudo")
SYSTEMCTL = Path("/usr/bin/systemctl")
INSTALLED_BROKER = Path(DEFAULT_NATIVE_BROKER_PATH)
SERVICE = "agcoord-broker.service"
OWNERSHIP_TIMEOUT = 30.0
OWNERSHIP_POLL_INTERVAL = 0.1
PACKAGE_NAME = "agcoord-native-host-x86_64-linux.tar.gz"
CHECKER_NAME = "check-native-host-package"
INSTALLER_NAME = "install-native-host"
PROBE_NAME = "test-native-host-enforcement"
MANIFEST_NAME = "./usr/share/doc/agcoord/native-host-manifest.json"
BROKER_NAME = "./usr/libexec/agcoord/agcoord-broker"
BROKER_ASSET_NAME = "agcoord-broker-x86_64-unknown-linux-musl"
USER_BROKER_NAME = "agcoord-broker"
USER_BROKER_DIR = (Path.home() / ".local/libexec/agcoord").resolve()
PIN_NAME = "native_host_pin.json"
PIN_PATH = Path(__file__).with_name(PIN_NAME)
MAX_MANIFEST_BYTES = 1024 * 1024
MAX_BROKER_BYTES = 128 * 1024 * 1024
BROKER_READ_SIZE = 1024 * 1024
_SHA256_LINE = re.compile(r"^([0-9a-f]{64})  ([^\r\n]+)\n?$")
_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_RELEASE_BUILD = re.compile(r"^sha256:[0-9a-f]{64}$")
_DRAIN_ID = re.compile(r"^drain-[0-9a-f]{12}$")


def _managed_state_dir() -> Path:
    configured_home = os.environ.get("XDG_STATE_HOME")
    root = (
        Path(configured_home).expanduser().resolve()
        if configured_home
        else Path.home() / ".local/state"
    )
    return (root / "agcoord").resolve()


MANAGED_STATE_DIR = _managed_state_dir()


def _invalid_bundle(message: str) -> CoordinatorError:
    return CoordinatorError(message, code="native-host-bundle-invalid")


def _regular_owner_file(path: Path, *, subject: str) -> os.stat_result:
    try:
        details = path.lstat()
    except OSError as exc:
        raise _invalid_bundle(f"cannot inspect {subject} {path}: {exc}") from exc
    if stat.S_ISLNK(details.st_mode) or not stat.S_ISREG(details.st_mode):
        raise _invalid_bundle(f"{subject} must be a regular file, not a symlink: {path}")
    if details.st_uid != os.geteuid():
        raise _invalid_bundle(f"{subject} must be owned by the current user: {path}")
    if details.st_mode & 0o022:
        raise _invalid_bundle(f"{subject} must not be group- or world-writable: {path}")
    return details


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as source:
            while chunk := source.read(1024 * 1024):
                digest.update(chunk)
    except OSError as exc:
        raise _invalid_bundle(f"cannot hash native-host asset {path}: {exc}") from exc
    return digest.hexdigest()


def _verify_sidecar(path: Path) -> None:
    _regular_owner_file(path, subject="native-host asset")
    sidecar = path.with_name(path.name + ".sha256")
    _regular_owner_file(sidecar, subject="native-host checksum")
    try:
        raw = sidecar.read_text(encoding="ascii")
    except (OSError, UnicodeError) as exc:
        raise _invalid_bundle(f"cannot read native-host checksum {sidecar}: {exc}") from exc
    matched = _SHA256_LINE.fullmatch(raw)
    if matched is None or matched.group(2) != path.name:
        raise _invalid_bundle(f"native-host checksum has an invalid format: {sidecar}")
    if _sha256(path) != matched.group(1):
        raise _invalid_bundle(f"native-host asset checksum does not match: {path}")


def _verified_bundle(package: str | os.PathLike[str]) -> tuple[Path, Path, Path]:
    requested = Path(package).expanduser()
    try:
        selected = requested.resolve(strict=True)
    except OSError as exc:
        raise _invalid_bundle(f"cannot resolve native-host package {requested}: {exc}") from exc
    if selected.name != PACKAGE_NAME:
        raise _invalid_bundle(
            f"native-host package must be named {PACKAGE_NAME}: {selected}"
        )
    directory = selected.parent
    try:
        details = directory.stat()
    except OSError as exc:
        raise _invalid_bundle(f"cannot inspect native-host bundle directory: {exc}") from exc
    if not stat.S_ISDIR(details.st_mode) or details.st_uid != os.geteuid():
        raise _invalid_bundle("native-host bundle directory must belong to the current user")
    if details.st_mode & 0o022:
        raise _invalid_bundle(
            "native-host bundle directory must not be group- or world-writable"
        )

    checker = directory / CHECKER_NAME
    installer = directory / INSTALLER_NAME
    probe = directory / PROBE_NAME
    for asset in (selected, checker, installer, probe):
        _verify_sidecar(asset)
    for helper in (checker, installer, probe):
        try:
            helper.chmod(0o755)
        except OSError as exc:
            raise _invalid_bundle(
                f"cannot restore executable mode on native-host helper {helper}: {exc}"
            ) from exc
    return selected, installer, probe


def _expected_identity(package: Path) -> dict[str, Any]:
    try:
        with tarfile.open(package, "r:gz") as archive:
            member = archive.getmember(MANIFEST_NAME)
            if member.size > MAX_MANIFEST_BYTES or not member.isfile():
                raise _invalid_bundle("native-host package manifest is invalid")
            source = archive.extractfile(member)
            if source is None:
                raise _invalid_bundle("native-host package manifest is unreadable")
            raw = source.read(MAX_MANIFEST_BYTES + 1)
    except (OSError, tarfile.TarError, KeyError) as exc:
        raise _invalid_bundle(f"cannot read native-host package manifest: {exc}") from exc
    if len(raw) > MAX_MANIFEST_BYTES:
        raise _invalid_bundle("native-host package manifest is oversized")
    try:
        manifest = json.loads(raw)
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise _invalid_bundle("native-host package manifest is not valid JSON") from exc
    identity = manifest.get("identity") if isinstance(manifest, dict) else None
    expected_keys = {
        "name",
        "version",
        "protocol",
        "implementation",
        "build",
        "target",
        "sqlite",
    }
    if (
        not isinstance(manifest, dict)
        or manifest.get("format") != 1
        or manifest.get("development") is not False
        or not isinstance(identity, dict)
        or set(identity) != expected_keys
        or identity.get("name") != "agcoord-broker"
        or identity.get("protocol") != NATIVE_PROTOCOL
        or identity.get("implementation") != NATIVE_IMPLEMENTATION
        or not isinstance(identity.get("build"), str)
        or _RELEASE_BUILD.fullmatch(identity["build"]) is None
        or identity.get("target") != "x86_64-unknown-linux-musl"
        or not isinstance(identity.get("sqlite"), str)
        or not identity["sqlite"]
    ):
        raise _invalid_bundle("native-host package manifest has an invalid release identity")
    if identity.get("version") != __version__:
        raise CoordinatorError(
            f"native-host package version {identity.get('version')!r} does not match "
            f"the installed agc client {__version__}; install the matching client first",
            code="native-host-version-mismatch",
        )
    return dict(identity)


def native_host_pin() -> dict[str, Any]:
    """Return this client's checked-in native-host pin."""
    try:
        raw = PIN_PATH.read_bytes()
    except OSError as exc:
        raise CoordinatorError(
            f"cannot read the native-host pin shipped with this client: {exc}",
            code="native-host-pin-invalid",
        ) from exc
    try:
        pin = json.loads(raw)
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise CoordinatorError(
            "the native-host pin shipped with this client is not valid JSON",
            code="native-host-pin-invalid",
        ) from exc
    digest = pin.get("broker_sha256") if isinstance(pin, dict) else None
    if (
        not isinstance(pin, dict)
        or pin.get("format") != 1
        or not isinstance(pin.get("version"), str)
        or not pin["version"]
        or not (digest is None or (isinstance(digest, str) and _DIGEST.fullmatch(digest)))
    ):
        raise CoordinatorError(
            "the native-host pin shipped with this client is malformed",
            code="native-host-pin-invalid",
        )
    return {"format": 1, "version": pin["version"], "broker_sha256": digest}


def pinned_broker_digest() -> str | None:
    """Return the broker digest this exact client release was pinned to, if any.

    A development checkout, or a pin left behind by an earlier version, carries no
    enforceable expectation and returns ``None``.
    """
    pin = native_host_pin()
    if pin["version"] != __version__:
        return None
    return pin["broker_sha256"]


def expected_broker_digest(supplied: str | None = None) -> str | None:
    """Return the digest a package must carry, honoring an operator's own digest.

    A supplied digest lets a development client demand a comparison it could not make
    on its own. It never relaxes one: a released client's shipped pin wins, and a
    supplied digest that disagrees with it is a refusal rather than an override.
    """
    pinned = pinned_broker_digest()
    if supplied is None:
        return pinned
    if not isinstance(supplied, str) or _DIGEST.fullmatch(supplied) is None:
        raise CoordinatorError(
            f"supplied broker digest {supplied!r} is not 64 lowercase hexadecimal digits",
            code="native-host-digest-invalid",
        )
    if pinned is not None and supplied != pinned:
        raise CoordinatorError(
            f"supplied broker digest {supplied} does not match the digest {pinned} this "
            f"agc {__version__} client was released against; a supplied digest cannot "
            "replace a shipped pin",
            code="native-host-pin-conflict",
        )
    return supplied


def require_expected_broker_digest(supplied: str | None = None) -> str:
    """Return the expected broker digest, refusing an operation that has none."""
    digest = expected_broker_digest(supplied)
    if digest is None:
        raise CoordinatorError(
            f"this agc {__version__} client carries no native-host pin, so a downloaded "
            "bundle cannot be verified against an independent digest; install a release "
            "client, pass a bundle path you verified yourself, or supply the digest you "
            "expect with --broker-sha256",
            code="native-host-unpinned-client",
        )
    return digest


def _archived_broker_digest(package: Path) -> str:
    """Digest the broker the package actually carries rather than its own claims."""
    reader = hashlib.sha256()
    try:
        with tarfile.open(package, "r:gz") as archive:
            member = archive.getmember(BROKER_NAME)
            if not member.isfile():
                raise _invalid_bundle("native-host package broker entry is not a file")
            if member.size > MAX_BROKER_BYTES:
                raise _invalid_bundle("native-host package broker entry is oversized")
            source = archive.extractfile(member)
            if source is None:
                raise _invalid_bundle("native-host package broker entry is unreadable")
            remaining = MAX_BROKER_BYTES
            while remaining > 0:
                block = source.read(min(BROKER_READ_SIZE, remaining))
                if not block:
                    break
                remaining -= len(block)
                reader.update(block)
            if source.read(1):
                raise _invalid_bundle("native-host package broker entry is oversized")
    except (OSError, tarfile.TarError, KeyError) as exc:
        raise _invalid_bundle(f"cannot read the native-host package broker: {exc}") from exc
    return reader.hexdigest()


def _verify_pinned_broker(
    package: Path,
    *,
    require_pin: bool,
    supplied: str | None = None,
) -> str | None:
    """Compare the package's own broker bytes against the expected digest.

    The package manifest and the ``.sha256`` sidecars travel with the bytes they
    describe, so they cannot establish that a bundle is the one this client was
    released against. Only a digest that reached the host another way can: the pin that
    arrived with the client, or one the operator supplies for a client without a pin.
    """
    expected = (
        require_expected_broker_digest(supplied)
        if require_pin
        else expected_broker_digest(supplied)
    )
    if expected is None:
        return None
    actual = _archived_broker_digest(package)
    if actual != expected:
        raise CoordinatorError(
            f"native-host package broker digest {actual} does not match the digest "
            f"{expected} pinned by this agc {__version__} client; the package is not the "
            "one this client was released against",
            code="native-host-pin-mismatch",
        )
    return actual


def _cpu_capacity() -> int:
    try:
        available = len(os.sched_getaffinity(0))
    except (AttributeError, OSError):
        available = os.cpu_count() or 1
    return max(1, available)


def _total_memory() -> int:
    """Total usable RAM this host reports, or zero when it reports none."""
    try:
        reported = Path("/proc/meminfo").read_text(encoding="utf-8")
    except OSError:
        return 0
    for line in reported.splitlines():
        if line.startswith("MemTotal:"):
            fields = line.split()
            if len(fields) >= 2 and fields[1].isdigit():
                return int(fields[1]) * 1024
    return 0


def _memory_capacity(*, total: int | None = None, reserve: int | None = None) -> int:
    """Declarable memory: everything but a reserve for the host, rounded down to whole MiB."""
    available = _total_memory() if total is None else total
    if available <= 0:
        return 0
    held_back = (
        max(MEMORY_RESERVE_FLOOR, available // MEMORY_RESERVE_DIVISOR)
        if reserve is None
        else reserve
    )
    return max(0, available - held_back) // MIB * MIB


def _delegated_controllers(cgroup_root: str | os.PathLike[str]) -> set[str]:
    """Controllers the broker's own leaf can enable, read from its parent's delegation."""
    try:
        delegated = (Path(cgroup_root).parent / "cgroup.controllers").read_text(encoding="utf-8")
    except OSError:
        return set()
    return set(delegated.split())


def _binding(kind: str, unit: str) -> dict[str, str]:
    return {"kind": kind, "unit": unit, "mode": "required", "backend": "cgroup-v2"}


def derive_host_config(
    *,
    managed: bool,
    broker_path: str | os.PathLike[str] | None = None,
    cgroup_root: str | os.PathLike[str] | None = None,
    cpu: int | None = None,
    jobs: int | None = None,
    memory: int | None = None,
    reserve: int | None = None,
    tmpfs: int | None = None,
    tmpfs_inodes: int | None = None,
) -> dict[str, Any]:
    """Describe this host as one broker configuration, enforcing only what it can.

    A capacity is declared whether or not the host can enforce it, because admission is what
    keeps agents from overcommitting the machine. A binding is added only for a kind whose
    controller is delegated to the broker's slice, so the derived configuration is never one
    the broker refuses at startup.
    """
    selected_cpu = _cpu_capacity() if cpu is None else cpu
    selected_jobs = selected_cpu if jobs is None else jobs
    if selected_cpu < 1 or selected_jobs < 1:
        raise _state_error("cpu and jobs capacities must each be at least one")
    selected_memory = _memory_capacity(reserve=reserve) if memory is None else memory
    if selected_memory < 0:
        raise _state_error("memory capacity cannot be negative")
    capacities: dict[str, int] = {"cpu": selected_cpu, "jobs": selected_jobs}
    if selected_memory > 0:
        capacities["memory"] = selected_memory
    root = str(_managed_cgroup_root() if cgroup_root is None else cgroup_root)
    bindings: dict[str, dict[str, str]] = {}
    if managed:
        delegated = _delegated_controllers(root)
        bindings["cpu"] = _binding("cpu", "logical-cpu")
        if "memory" in delegated and "memory" in capacities:
            bindings["memory"] = _binding("memory", "bytes")
    if tmpfs is not None:
        if "memory" not in bindings:
            raise _state_error(
                "tmpfs scratch needs an enforced memory binding, which this host does not "
                "offer; its pages are charged against the same memory limit"
            )
        if tmpfs > capacities["memory"]:
            raise _state_error("tmpfs capacity cannot exceed the memory capacity")
        capacities["tmpfs"] = tmpfs
        capacities["tmpfs_inodes"] = (
            tmpfs // TMPFS_BYTES_PER_INODE if tmpfs_inodes is None else tmpfs_inodes
        )
        if capacities["tmpfs_inodes"] < 1:
            raise _state_error("tmpfs inode capacity must be at least one")
        bindings["tmpfs"] = _binding("tmpfs", "bytes")
        bindings["tmpfs_inodes"] = _binding("inodes", "inodes")
    document: dict[str, Any] = {"capacities": dict(sorted(capacities.items()))}
    if bindings:
        document["bindings"] = dict(sorted(bindings.items()))
    if managed:
        document["cgroup_root"] = root
    default_broker = INSTALLED_BROKER if managed else USER_BROKER_PATH
    document["native_broker"] = {
        "path": str(default_broker if broker_path is None else broker_path),
        "allow_development": False,
        "managed_service": managed,
    }
    _validate_derived_config(document)
    return document


def _validate_derived_config(document: Mapping[str, Any]) -> None:
    """Refuse a derived document the client or broker would reject when it loads it."""
    encoded = json.dumps(document)
    try:
        configuration = parse_broker_config(encoded, source="derived configuration")
        configured_capacities(configuration.capacities)
        validate_resource_bindings(configuration.bindings)
    except (BrokerConfigError, CoordinatorError, ResourceContractError) as exc:
        raise _state_error(f"derived broker configuration is invalid: {exc}") from exc


def host_config(
    *,
    state_dir: str | os.PathLike[str],
    managed: bool = True,
    write: bool = False,
    force: bool = False,
    cpu: int | None = None,
    jobs: int | None = None,
    memory: int | None = None,
    reserve: int | None = None,
    tmpfs: int | None = None,
    tmpfs_inodes: int | None = None,
    cgroup_root: str | os.PathLike[str] | None = None,
) -> dict[str, Any]:
    """Derive this host's broker configuration, and write it when asked."""
    document = derive_host_config(
        managed=managed,
        cgroup_root=cgroup_root,
        cpu=cpu,
        jobs=jobs,
        memory=memory,
        reserve=reserve,
        tmpfs=tmpfs,
        tmpfs_inodes=tmpfs_inodes,
    )
    directory = Path(state_dir).expanduser()
    destination = config_path(directory)
    if not write:
        return {
            "state_dir": str(directory),
            "path": str(destination),
            "written": False,
            "configuration": document,
        }
    if (destination.exists() or destination.is_symlink()) and not force:
        raise _state_error(
            f"{destination} already exists; pass --force to replace it. A live broker keeps "
            "the configuration it acquired the spool with, so restart it to adopt a new one"
        )
    try:
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(directory, 0o700)
    except OSError as exc:
        raise _state_error(f"cannot prepare state directory {directory}: {exc}") from exc
    encoded = (json.dumps(document, indent=2, sort_keys=True) + "\n").encode()
    temporary = destination.with_name(f"{destination.name}.partial")
    flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW
    try:
        descriptor = os.open(temporary, flags, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, 0o600)
        os.replace(temporary, destination)
    except OSError as exc:
        temporary.unlink(missing_ok=True)
        raise _state_error(f"cannot write broker configuration {destination}: {exc}") from exc
    return {
        "state_dir": str(directory),
        "path": str(destination),
        "written": True,
        "configuration": document,
    }


def _state_error(message: str) -> CoordinatorError:
    return CoordinatorError(message, code="native-host-state-invalid")


def _validate_state_directory(
    state_dir: Path,
    *,
    create: bool,
    subject: str = "managed state directory",
) -> None:
    if create:
        try:
            state_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        except OSError as exc:
            raise _state_error(f"cannot create {subject} {state_dir}: {exc}") from exc
    try:
        details = state_dir.lstat()
    except OSError as exc:
        raise _state_error(f"cannot inspect {subject} {state_dir}: {exc}") from exc
    if (
        stat.S_ISLNK(details.st_mode)
        or not stat.S_ISDIR(details.st_mode)
        or details.st_uid != os.geteuid()
        or details.st_mode & 0o077
    ):
        raise _state_error(
            f"{subject} must be a real current-user-owned directory with mode 0700: "
            f"{state_dir}"
        )



def _refuse_live_broker(paths: CoordinatorPaths) -> None:
    """Refuse to replace a broker executable while a broker owns the spool."""
    try:
        descriptor = os.open(paths.owner_lock, os.O_RDWR)
    except FileNotFoundError:
        return
    except OSError as exc:
        raise _state_error(
            f"cannot open broker ownership file {paths.owner_lock}: {exc}"
        ) from exc
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise CoordinatorError(
                f"a broker currently owns {paths.state_dir}; run `agc drain` and let it exit "
                "before replacing its executable",
                code="native-host-user-live-broker",
            ) from None
        fcntl.flock(descriptor, fcntl.LOCK_UN)
    finally:
        os.close(descriptor)


def _user_config_plan(state_dir: Path, target: Path) -> bool:
    """Return whether a configuration must be written, refusing one for another broker."""
    path = config_path(state_dir)
    if not path.exists() and not path.is_symlink():
        return True
    try:
        configuration = load_broker_config(state_dir)
    except BrokerConfigError as exc:
        raise _state_error(str(exc)) from exc
    native = configuration.native_broker
    if native.path == str(target) and not native.managed_service:
        return False
    managed = " as a managed service" if native.managed_service else ""
    raise CoordinatorError(
        f"{path} already selects {native.path}{managed}; edit it to select {target} with "
        "managed_service false, or choose another --state-dir for the user-owned broker",
        code="native-host-user-config-conflict",
    )


def _write_user_config(state_dir: Path, target: Path) -> None:
    configuration = derive_host_config(managed=False, broker_path=target)
    destination = config_path(state_dir)
    encoded = (json.dumps(configuration, indent=2, sort_keys=True) + "\n").encode()
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW
    try:
        descriptor = os.open(destination, flags, 0o600)
    except OSError as exc:
        raise _state_error(f"cannot create broker configuration {destination}: {exc}") from exc
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
    except OSError as exc:
        raise _state_error(f"cannot write broker configuration {destination}: {exc}") from exc


def install_user_broker(
    source: str | os.PathLike[str],
    *,
    state_dir: str | os.PathLike[str] | None = None,
    destination_dir: str | os.PathLike[str] | None = None,
    broker_sha256: str | None = None,
) -> dict[str, Any]:
    """Install this client's release broker as a user-owned executable for an unmanaged spool.

    ``source`` is a downloaded copy of the standalone release broker. Its bytes are compared
    with the digest this client was released against before anything is placed, the placed
    copy is verified again, and it is then selected exactly as a later client will select
    it. No privilege is used and no service is created; the spool may be the default state
    directory or any ``--state-dir``.
    """
    if os.environ.get(RUN_ID_ENV):
        raise CoordinatorError(
            "the user broker cannot be installed from an admitted AGCoord job",
            code="native-host-upgrade-nested",
        )
    expected = require_expected_broker_digest(broker_sha256)
    source_path = Path(source).expanduser().resolve()
    _regular_owner_file(source_path, subject="downloaded broker")
    actual = _sha256(source_path)
    if actual != expected:
        raise CoordinatorError(
            f"broker digest {actual} does not match the digest {expected} pinned by this "
            f"agc {__version__} client; the file is not the broker this client was "
            "released with",
            code="native-host-pin-mismatch",
        )
    paths = queue_paths(state_dir=state_dir)
    spool = paths.state_dir
    _validate_state_directory(spool, create=True, subject="state directory")
    _refuse_live_broker(paths)
    destination = Path(destination_dir or USER_BROKER_DIR).expanduser().resolve()
    target = destination / USER_BROKER_NAME
    write_config = _user_config_plan(spool, target)
    try:
        destination.mkdir(parents=True, exist_ok=True)
        details = destination.lstat()
        if details.st_mode & 0o022:
            os.chmod(destination, stat.S_IMODE(details.st_mode) & ~0o022)
            details = destination.lstat()
    except OSError as exc:
        raise _state_error(f"cannot prepare {destination}: {exc}") from exc
    if (
        stat.S_ISLNK(details.st_mode)
        or not stat.S_ISDIR(details.st_mode)
        or details.st_uid != os.geteuid()
    ):
        raise _state_error(
            f"{destination} must be a real current-user-owned directory"
        )
    staging = destination / f".{USER_BROKER_NAME}.partial"
    try:
        shutil.copyfile(source_path, staging)
        os.chmod(staging, 0o755)
        os.replace(staging, target)
    except OSError as exc:
        staging.unlink(missing_ok=True)
        raise _state_error(f"cannot place the broker at {target}: {exc}") from exc
    placed = _sha256(target)
    if placed != expected:
        target.unlink(missing_ok=True)
        raise CoordinatorError(
            f"the placed broker {target} does not carry the pinned digest {expected}",
            code="native-host-pin-mismatch",
        )
    try:
        NativeBrokerCommand.select(
            NativeBrokerConfig(
                path=str(target),
                allow_development=False,
                managed_service=False,
            )
        )
    except NativeClientError as exc:
        raise CoordinatorError(
            f"the installed user broker was not selectable: {exc}",
            code=exc.code or "native-host-user-broker-invalid",
        ) from exc
    if write_config:
        _write_user_config(spool, target)
    return {
        "state": "complete",
        "operation": "install-user",
        "version": __version__,
        "broker": str(target),
        "broker_sha256": placed,
        "state_dir": str(spool),
        "configured": write_config,
    }


def _default_managed_config() -> dict[str, Any]:
    return derive_host_config(managed=True)


def _managed_cgroup_root() -> str:
    uid = os.getuid()
    return (
        f"/sys/fs/cgroup/user.slice/user-{uid}.slice/user@{uid}.service/"
        "app.slice/agcoord-broker.service"
    )


def _write_default_config(state_dir: Path) -> None:
    destination = config_path(state_dir)
    encoded = (json.dumps(_default_managed_config(), indent=2, sort_keys=True) + "\n").encode()
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW
    try:
        descriptor = os.open(destination, flags, 0o600)
    except FileExistsError:
        return
    except OSError as exc:
        raise _state_error(
            f"cannot create managed broker configuration {destination}: {exc}"
        ) from exc
    try:
        with os.fdopen(descriptor, "wb") as target:
            target.write(encoded)
            target.flush()
            os.fsync(target.fileno())
    except OSError as exc:
        raise _state_error(
            f"cannot write managed broker configuration {destination}: {exc}"
        ) from exc


def _validate_managed_config(state_dir: Path) -> None:
    path = config_path(state_dir)
    try:
        details = path.lstat()
    except OSError as exc:
        raise _state_error(f"cannot inspect managed broker configuration {path}: {exc}") from exc
    if (
        stat.S_ISLNK(details.st_mode)
        or not stat.S_ISREG(details.st_mode)
        or details.st_uid != os.geteuid()
        or details.st_mode & 0o077
    ):
        raise _state_error(
            "managed broker configuration must be a real current-user-owned file "
            f"with mode 0600: {path}"
        )
    try:
        configuration = load_broker_config(state_dir)
    except BrokerConfigError as exc:
        raise _state_error(str(exc)) from exc
    try:
        capacities = configured_capacities(configuration.capacities)
        bindings = validate_resource_bindings(configuration.bindings)
    except (CoordinatorError, ResourceContractError) as exc:
        raise _state_error(str(exc)) from exc
    native = configuration.native_broker
    cpu_binding = {
        "backend": "cgroup-v2",
        "kind": "cpu",
        "mode": "required",
        "unit": "logical-cpu",
    }
    if (
        native.path != str(INSTALLED_BROKER)
        or native.allow_development
        or not native.managed_service
        or capacities.get("cpu", 0) < 1
        or bindings.get("cpu") != cpu_binding
        or configuration.cgroup_root != _managed_cgroup_root()
        or configuration.capacities is None
        or configuration.bindings is None
    ):
        raise _state_error(
            "managed broker configuration must select the fixed release host, a positive "
            "cpu capacity, the required cgroup-v2 logical-cpu binding, and the managed "
            "service cgroup root"
        )


def _prepare_state(
    state_dir: Path,
    *,
    operation: str,
) -> None:
    if state_dir.resolve() != MANAGED_STATE_DIR:
        raise _state_error(
            f"the managed user service owns only {MANAGED_STATE_DIR}, not {state_dir}"
        )
    if operation == "install":
        _validate_state_directory(state_dir, create=True)
        database = state_dir / "queue.sqlite3"
        if database.exists() or database.is_symlink():
            raise _state_error(
                f"a queue already exists at {database}; use `agc host upgrade`"
            )
        if not config_path(state_dir).exists():
            _write_default_config(state_dir)
        _validate_managed_config(state_dir)
        return
    _validate_state_directory(state_dir, create=False)
    _validate_managed_config(state_dir)
    database = state_dir / "queue.sqlite3"
    try:
        details = database.lstat()
    except OSError as exc:
        raise _state_error(
            f"no existing queue is available at {database}; use `agc host install`"
        ) from exc
    if (
        stat.S_ISLNK(details.st_mode)
        or not stat.S_ISREG(details.st_mode)
        or details.st_uid != os.geteuid()
        or details.st_mode & 0o077
    ):
        raise _state_error(f"existing queue file is unsafe: {database}")


def _run_checked(
    arguments: Sequence[str | os.PathLike[str]],
    *,
    phase: str,
    code: str,
) -> subprocess.CompletedProcess[str]:
    command = [str(value) for value in arguments]
    try:
        completed = subprocess.run(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
    except OSError as exc:
        raise CoordinatorError(
            f"native-host {phase} could not run: {exc}",
            code=code,
        ) from exc
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip() or "no diagnostic"
        raise CoordinatorError(
            f"native-host {phase} failed with exit status {completed.returncode}: {detail}",
            code=code,
        )
    return completed


def _validate_drain(receipt: object) -> dict[str, Any]:
    if (
        not isinstance(receipt, dict)
        or receipt.get("state") != "drained"
        or type(receipt.get("live")) is not int
        or receipt["live"] != 0
        or receipt.get("protocol") != NATIVE_PROTOCOL
        or not isinstance(receipt.get("drain_id"), str)
        or _DRAIN_ID.fullmatch(receipt["drain_id"]) is None
    ):
        raise CoordinatorError(
            "native-host upgrade did not receive a drained protocol-5 receipt",
            code="native-host-upgrade-drain-invalid",
        )
    return receipt


def _decode_installed_identity(
    output: str,
    expected: dict[str, Any],
    *,
    operation: str,
) -> dict[str, Any]:
    try:
        identity = json.loads(output)
    except json.JSONDecodeError as exc:
        raise CoordinatorError(
            "installed native-host broker returned invalid identity JSON",
            code=f"native-host-{operation}-verification-failed",
        ) from exc
    if identity != expected:
        raise CoordinatorError(
            "installed native-host broker identity does not match the selected package",
            code=f"native-host-{operation}-verification-failed",
        )
    return identity


def _validate_proof(
    proof: object,
    run_id: str,
    *,
    operation: str,
) -> dict[str, Any]:
    receipt = proof.get("resource_receipt") if isinstance(proof, dict) else None
    if (
        not isinstance(proof, dict)
        or proof.get("run_id") != run_id
        or proof.get("status") != "passed"
        or proof.get("exit_status") != 0
        or not isinstance(receipt, dict)
        or not isinstance(receipt.get("requested"), dict)
        or not isinstance(receipt.get("applied"), dict)
        or not isinstance(receipt.get("peak"), dict)
        or receipt["requested"].get("cpu") != 1
        or receipt["applied"].get("cpu") != 1
        or not isinstance(receipt["peak"].get("cpu"), int)
        or receipt["peak"]["cpu"] < 1
    ):
        raise CoordinatorError(
            f"native-host enforcement proof {run_id} did not retain an enforced cpu=1 receipt",
            code=f"native-host-{operation}-proof-failed",
        )
    return proof


def _operator_context() -> None:
    if os.environ.get(RUN_ID_ENV):
        raise CoordinatorError(
            "native-host installation and upgrade cannot run from an admitted AGCoord job",
            code="native-host-upgrade-nested",
        )
    if STATE_DIR_ENV in os.environ:
        raise _state_error(
            f"unset {STATE_DIR_ENV} for managed native-host operations; the fixed user "
            f"service owns only {MANAGED_STATE_DIR}"
        )


def _release_inputs(
    package: str | os.PathLike[str],
    *,
    require_pin: bool = False,
    broker_sha256: str | None = None,
) -> tuple[Path, Path, Path, dict[str, Any]]:
    selected, installer, probe = _verified_bundle(package)
    _run_checked(
        [selected.parent / CHECKER_NAME, selected],
        phase="package validation",
        code="native-host-bundle-invalid",
    )
    _verify_pinned_broker(selected, require_pin=require_pin, supplied=broker_sha256)
    return selected, installer, probe, _expected_identity(selected)


def _await_spool_ownership(
    client: CoordinatorClient,
    *,
    operation: str,
    state_dir: Path,
) -> None:
    """Wait for the restarted broker to own the spool before submitting the proof.

    Starting the service and owning the state directory are separate events, so a
    single probe can land in the gap and report a completed upgrade as a failure.
    """
    deadline = time.monotonic() + OWNERSHIP_TIMEOUT
    while True:
        try:
            client.ping()
            return
        except CoordinatorError as exc:
            if time.monotonic() >= deadline:
                raise CoordinatorError(
                    f"native-host {operation} started the service but no broker owned "
                    f"{state_dir} within {OWNERSHIP_TIMEOUT:.0f}s: {exc}",
                    code=f"native-host-{operation}-verification-failed",
                ) from exc
        time.sleep(OWNERSHIP_POLL_INTERVAL)


def _verify_running_host(
    *,
    operation: str,
    expected_identity: dict[str, Any],
    probe: Path,
    state_dir: Path,
    checkout: Path,
) -> tuple[dict[str, Any], str, dict[str, Any]]:
    _run_checked(
        [SYSTEMCTL, "--user", "is-active", "--quiet", SERVICE],
        phase="service verification",
        code=f"native-host-{operation}-verification-failed",
    )
    identity_result = _run_checked(
        [INSTALLED_BROKER, "identity", "--json"],
        phase="broker identity verification",
        code=f"native-host-{operation}-verification-failed",
    )
    identity = _decode_installed_identity(
        identity_result.stdout,
        expected_identity,
        operation=operation,
    )
    proof_client = CoordinatorClient(
        state_dir=state_dir,
        checkout=checkout,
        autostart=False,
    )
    _await_spool_ownership(
        proof_client,
        operation=operation,
        state_dir=state_dir,
    )
    proof_run_id = proof_client.submit(
        [str(probe)],
        checkout=str(checkout),
        kind="check",
        label=f"native host enforcement {expected_identity['version']}",
        resources={"cpu": 1},
    )
    proof = _validate_proof(
        wait(proof_client, proof_run_id),
        proof_run_id,
        operation=operation,
    )
    return identity, proof_run_id, proof


def install_native_host(
    package: str | os.PathLike[str],
    *,
    state_dir: str | os.PathLike[str] | None = None,
    checkout: str | os.PathLike[str] | None = None,
    require_pin: bool = False,
    broker_sha256: str | None = None,
) -> dict[str, Any]:
    """Install one verified managed native host onto a fresh default spool."""
    _operator_context()
    selected, installer, probe, expected_identity = _release_inputs(
        package,
        require_pin=require_pin,
        broker_sha256=broker_sha256,
    )
    checkout_path = Path(checkout or ".").expanduser().resolve()
    paths = queue_paths(state_dir=state_dir, checkout=checkout_path)
    _prepare_state(paths.state_dir, operation="install")
    _run_checked(
        [SUDO, installer, "stage", selected],
        phase="package staging",
        code="native-host-install-stage-failed",
    )
    activated = False
    try:
        _run_checked(
            [SUDO, "-v"],
            phase="activation authorization",
            code="native-host-install-authorization-failed",
        )
        _run_checked(
            [SUDO, installer, "activate", paths.state_dir],
            phase="package activation",
            code="native-host-install-activation-failed",
        )
        activated = True
        _run_checked(
            [SYSTEMCTL, "--user", "daemon-reload"],
            phase="service reload",
            code="native-host-install-reload-failed",
        )
        _run_checked(
            [SYSTEMCTL, "--user", "enable", "--now", SERVICE],
            phase="service enable",
            code="native-host-install-start-failed",
        )
        identity, proof_run_id, proof = _verify_running_host(
            operation="install",
            expected_identity=expected_identity,
            probe=probe,
            state_dir=paths.state_dir,
            checkout=checkout_path,
        )
    except CoordinatorError as exc:
        cleanup = "the user service was not changed"
        if activated:
            try:
                _run_checked(
                    [SYSTEMCTL, "--user", "disable", "--now", SERVICE],
                    phase="failed-install service cleanup",
                    code="native-host-install-cleanup-failed",
                )
                cleanup = "the unproved service was disabled and stopped"
            except CoordinatorError as cleanup_error:
                cleanup = f"service cleanup also failed: {cleanup_error}"
        raise CoordinatorError(
            f"native-host installation did not complete: {exc}; {cleanup}; rerun the "
            "same install command after correcting the failure",
            code="native-host-install-incomplete",
        ) from exc
    return {
        "state": "complete",
        "operation": "install",
        "version": expected_identity["version"],
        "package": str(selected),
        "service": "active",
        "identity": identity,
        "proof_run_id": proof_run_id,
        "proof": proof,
    }


def upgrade_native_host(
    package: str | os.PathLike[str],
    *,
    state_dir: str | os.PathLike[str] | None = None,
    checkout: str | os.PathLike[str] | None = None,
    require_pin: bool = False,
    broker_sha256: str | None = None,
) -> dict[str, Any]:
    """Upgrade one managed native host without reopening an unverified activation."""
    _operator_context()
    selected, installer, probe, expected_identity = _release_inputs(
        package,
        require_pin=require_pin,
        broker_sha256=broker_sha256,
    )
    checkout_path = Path(checkout or ".").expanduser().resolve()
    paths = queue_paths(state_dir=state_dir, checkout=checkout_path)
    _prepare_state(paths.state_dir, operation="upgrade")
    _run_checked(
        [SUDO, installer, "stage", selected],
        phase="package staging",
        code="native-host-upgrade-stage-failed",
    )
    drain_client = CoordinatorClient(
        state_dir=paths.state_dir,
        checkout=checkout_path,
        autostart=False,
        host_maintenance=True,
    )
    drain = _validate_drain(
        drain_client.drain(
            reason=f"native host upgrade to {expected_identity['version']}",
            wait=True,
        )
    )
    drain_id = drain["drain_id"]
    service_stopped = False
    service_started = False
    resumed = False
    try:
        _run_checked(
            [SUDO, "-v"],
            phase="activation authorization",
            code="native-host-upgrade-authorization-failed",
        )
        _run_checked(
            [SYSTEMCTL, "--user", "stop", SERVICE],
            phase="service stop",
            code="native-host-upgrade-stop-failed",
        )
        service_stopped = True
        _run_checked(
            [
                SUDO,
                installer,
                "activate",
                paths.state_dir,
                "--drain-id",
                drain_id,
            ],
            phase="package activation",
            code="native-host-upgrade-activation-failed",
        )
        _run_checked(
            [SYSTEMCTL, "--user", "daemon-reload"],
            phase="service reload",
            code="native-host-upgrade-reload-failed",
        )
        resume_client = CoordinatorClient(
            state_dir=paths.state_dir,
            checkout=checkout_path,
            autostart=False,
        )
        resume = resume_client.resume(drain_id)
        resumed = True
        _run_checked(
            [SYSTEMCTL, "--user", "start", SERVICE],
            phase="service start",
            code="native-host-upgrade-start-failed",
        )
        service_started = True
        identity, proof_run_id, proof = _verify_running_host(
            operation="upgrade",
            expected_identity=expected_identity,
            probe=probe,
            state_dir=paths.state_dir,
            checkout=checkout_path,
        )
    except CoordinatorError as exc:
        if resumed:
            state = "coordinator is open"
        else:
            state = "coordinator remains drained"
        if service_started:
            service = "service was started but verification failed"
        elif service_stopped:
            service = "service remains stopped"
        else:
            service = "service was not stopped"
        raise CoordinatorError(
            f"native-host upgrade stopped at drain {drain_id}: {exc}; {state}; {service}",
            code="native-host-upgrade-incomplete",
        ) from exc

    return {
        "state": "complete",
        "operation": "upgrade",
        "version": expected_identity["version"],
        "package": str(selected),
        "drain_id": drain_id,
        "drain": drain,
        "resume": resume,
        "service": "active",
        "identity": identity,
        "proof_run_id": proof_run_id,
        "proof": proof,
    }
