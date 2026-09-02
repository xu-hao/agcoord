"""Transactional orchestration for verified managed native-host operations."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import tarfile
import time
from typing import Any, Sequence

from . import __version__
from .config import (
    DEFAULT_NATIVE_BROKER_PATH,
    BrokerConfigError,
    config_path,
    load_broker_config,
)
from .native_client import NATIVE_IMPLEMENTATION, NATIVE_PROTOCOL
from .queue import (
    RUN_ID_ENV,
    STATE_DIR_ENV,
    CoordinatorClient,
    CoordinatorError,
    configured_capacities,
    queue_paths,
    wait,
)
from .resources import ResourceContractError, validate_resource_bindings


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
MAX_MANIFEST_BYTES = 1024 * 1024
_SHA256_LINE = re.compile(r"^([0-9a-f]{64})  ([^\r\n]+)\n?$")
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


def _cpu_capacity() -> int:
    try:
        available = len(os.sched_getaffinity(0))
    except (AttributeError, OSError):
        available = os.cpu_count() or 1
    return max(1, available)


def _state_error(message: str) -> CoordinatorError:
    return CoordinatorError(message, code="native-host-state-invalid")


def _validate_state_directory(state_dir: Path, *, create: bool) -> None:
    if create:
        try:
            state_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        except OSError as exc:
            raise _state_error(
                f"cannot create managed state directory {state_dir}: {exc}"
            ) from exc
    try:
        details = state_dir.lstat()
    except OSError as exc:
        raise _state_error(
            f"cannot inspect managed state directory {state_dir}: {exc}"
        ) from exc
    if (
        stat.S_ISLNK(details.st_mode)
        or not stat.S_ISDIR(details.st_mode)
        or details.st_uid != os.geteuid()
        or details.st_mode & 0o077
    ):
        raise _state_error(
            "managed state directory must be a real current-user-owned directory "
            f"with mode 0700: {state_dir}"
        )


def _default_managed_config() -> dict[str, Any]:
    capacity = _cpu_capacity()
    return {
        "capacities": {"cpu": capacity, "jobs": capacity},
        "bindings": {
            "cpu": {
                "kind": "cpu",
                "unit": "logical-cpu",
                "mode": "required",
                "backend": "cgroup-v2",
            }
        },
        "cgroup_root": _managed_cgroup_root(),
        "native_broker": {
            "path": str(INSTALLED_BROKER),
            "allow_development": False,
            "managed_service": True,
        },
    }


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
) -> tuple[Path, Path, Path, dict[str, Any]]:
    selected, installer, probe = _verified_bundle(package)
    _run_checked(
        [selected.parent / CHECKER_NAME, selected],
        phase="package validation",
        code="native-host-bundle-invalid",
    )
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
) -> dict[str, Any]:
    """Install one verified managed native host onto a fresh default spool."""
    _operator_context()
    selected, installer, probe, expected_identity = _release_inputs(package)
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
) -> dict[str, Any]:
    """Upgrade one managed native host without reopening an unverified activation."""
    _operator_context()
    selected, installer, probe, expected_identity = _release_inputs(package)
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
