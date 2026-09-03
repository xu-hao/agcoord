"""One owner-only machine scheduler for development agents and repositories.

Clients append strict requests to SQLite; one detached, flock-owned broker admits them
against machine resource capacities and per-repository FIFO barriers.  Workers run in
their submitted checkouts with private scratch roots and process-group supervision.  The
module opens no network listener and has no dependency on a product repository.
"""

from __future__ import annotations

import argparse
from contextlib import closing
from dataclasses import dataclass, field
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import select
import shutil
import signal
import sqlite3
import stat
import subprocess
import sys
import threading
import time
from typing import Any, Iterable, Mapping, Sequence
from uuid import uuid4

from .cgroup import (
    CGROUP_BACKEND,
    CGROUP_ISOLATE_ENV,
    CgroupV2Backend,
)
from .config import BrokerConfig, BrokerConfigError, load_broker_config
from .native_client import (
    NATIVE_IMPLEMENTATION,
    NATIVE_PROTOCOL,
    NativeBrokerCommand,
    NativeClientError,
)
from .project_quota import PROJECT_QUOTA_BACKEND, ProjectQuotaBackend
from .resources import (
    ResourceBackend,
    ResourceBackendError,
    ResourceContractError,
    ResourceObservation,
    ResourceRequest,
    capability_issue,
    configured_resource_bindings,
    initial_resource_receipt,
    probe_resource_backends,
    resource_contract,
    validate_backend_state,
    validate_resource_backends,
    validate_resource_bindings,
    validate_resource_capabilities,
    validate_resource_contract,
    validate_resource_measurement,
    validate_resource_receipt,
)
from .worker import PROJECT_QUOTA_DROP_ENV, TMPFS_SETUP_ENV


PROTOCOL = 4
TERMINAL_STATUSES = frozenset({
    "passed", "failed", "cancelled", "interrupted",
})
LIVE_STATUSES = frozenset({"queued", "running"})
STATUSES = LIVE_STATUSES | TERMINAL_STATUSES
DEFAULT_RECENT_LIMIT = 50
DEFAULT_IDLE_SECONDS = 60.0
DEFAULT_JOB_CAPACITY = 2
DEFAULT_DATABASE_TIMEOUT = 10.0
MAX_LOG_BYTES = 64 * 1024
MAX_OWNER_METADATA_BYTES = 1024 * 1024
CANCEL_GRACE_SECONDS = 5.0
RUN_ID_ENV = "AGCOORD_RUN_ID"
RUN_KIND_ENV = "AGCOORD_RUN_KIND"
STATE_DIR_ENV = "AGCOORD_STATE_DIR"
AGENT_ENV = "AGCOORD_AGENT"
DEFAULT_AGENT = "unnamed"
CHILD_CPU_RESOURCE = "cpu"
CHILD_LEASE_POLL_SECONDS = 0.05
CHILD_LEASE_MAX_BYPASSES = 1
LAND_TARGET_SYNC_ENV = "_AGCOORD_LAND_TARGET_SYNC"
LAND_AVOID_ENV = "_AGCOORD_LAND_AVOID"
RUN_KINDS = frozenset({"check", "full", "merge", "land"})
RUN_PHASES = frozenset({
    "queued", "running", "preflight", "gating", "publishing", "complete",
})
_RESOURCE_NAME = re.compile(r"^[a-z][a-z0-9_.:-]{0,63}$")
_SETUP_CODE = re.compile(r"^[a-z][a-z0-9-]{0,63}$")
_DRAIN_ID = re.compile(r"^drain-[0-9a-f]{12}$")
_MAINTENANCE_TIME = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|\+00:00)$"
)
_WORKER_LAUNCHER = "from agcoord.worker import launcher_main; launcher_main()\n"
MAINTENANCE_REFUSAL = "agcoord-maintenance-draining"
MAINTENANCE_STATES = frozenset({"draining", "drained"})
MAINTENANCE_TRIGGER_NAMES = (
    "agcoord_maintenance_reject_runs",
    "agcoord_maintenance_reject_activity_insert",
    "agcoord_maintenance_reject_activity_update",
)
MAINTENANCE_METADATA_KEYS = (
    "maintenance_state",
    "maintenance_id",
    "maintenance_reason",
    "maintenance_started_at",
)
MAX_MAINTENANCE_REASON = 256


class CoordinatorError(RuntimeError):
    """A named local-coordinator refusal suitable for a terminal, not a traceback."""

    def __init__(self, message: str, *, code: str | None = None):
        super().__init__(message)
        self.code = code


class _OwnerMetadataError(CoordinatorError):
    """A live owner whose one startup metadata write is not readable yet or is invalid."""


class _ResourceEnforcementError(CoordinatorError):
    """A required backend contract failed before the blocked launcher was released."""


def _transient_database_error(exc: sqlite3.OperationalError) -> bool:
    code = getattr(exc, "sqlite_errorcode", None)
    if isinstance(code, int) and code & 0xFF in {
        getattr(sqlite3, "SQLITE_BUSY", 5),
        getattr(sqlite3, "SQLITE_LOCKED", 6),
    }:
        return True
    message = str(exc).lower()
    return "locked" in message or "busy" in message


def _agent_identity(agent: object) -> str:
    selected = agent or os.environ.get(AGENT_ENV) or DEFAULT_AGENT
    if not isinstance(selected, str) or not selected.strip():
        raise CoordinatorError("agent must be a non-empty string")
    return selected.strip()


def _resource_failure_code(exc: Exception, fallback: str) -> str:
    """Use only validated backend codes and never arbitrary exception text."""

    return exc.code if isinstance(exc, ResourceBackendError) else fallback


@dataclass(frozen=True)
class CoordinatorPaths:
    state_dir: Path
    database: Path
    owner_lock: Path
    daemon_log: Path
    logs: Path
    worker_tmp: Path
    legacy_worker_tmp: Path


@dataclass(frozen=True)
class RepositoryIdentity:
    """Stable logical repository plus the exact submitted worktree."""

    repository_id: str
    repository: str
    worktree_id: str
    checkout: Path


def _absolute(path: str | os.PathLike[str]) -> Path:
    return Path(path).expanduser().resolve()


def state_dir_for(checkout: str | os.PathLike[str] | None = None) -> Path:
    """Return the one user-scoped queue directory shared by every repository."""
    if "AGCOORD_STATE_DIR" in os.environ:
        configured = os.environ["AGCOORD_STATE_DIR"]
        if not configured:
            raise CoordinatorError("AGCOORD_STATE_DIR is empty")
        return _absolute(configured)

    configured_home = os.environ.get("XDG_STATE_HOME")
    root = _absolute(configured_home) if configured_home else Path.home() / ".local/state"
    return root / "agcoord"


def queue_paths(
    *,
    state_dir: str | os.PathLike[str] | None = None,
    checkout: str | os.PathLike[str] | None = None,
) -> CoordinatorPaths:
    root = _absolute(state_dir) if state_dir is not None else state_dir_for(checkout)
    namespace = hashlib.sha256(os.fsencode(str(root))).hexdigest()[:16]
    worker_tmp = Path("/tmp").resolve() / f"agcoord-{os.getuid()}" / namespace
    return CoordinatorPaths(
        state_dir=root,
        database=root / "queue.sqlite3",
        owner_lock=root / "broker.lock",
        daemon_log=root / "broker.log",
        logs=root / "logs",
        worker_tmp=worker_tmp,
        legacy_worker_tmp=root / "tmp",
    )


def _git_value(checkout: Path, *arguments: str, required: bool = True) -> str | None:
    try:
        result = subprocess.run(
            ["git", "-C", str(checkout), *arguments],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
    except OSError as exc:
        raise CoordinatorError(f"cannot run Git for {checkout}: {exc}") from exc
    value = result.stdout.strip()
    if result.returncode == 0 and value:
        return value
    if not required:
        return None
    detail = result.stderr.strip() or "Git returned no value"
    raise CoordinatorError(f"cannot inspect Git checkout {checkout}: {detail}")


def _remote_identity(value: str) -> str:
    """Normalize a remote without retaining URL credentials."""
    selected = value.strip().rstrip("/")
    scp = re.fullmatch(r"(?:[^@/]+@)?([^:/]+):(.+)", selected)
    if scp and "://" not in selected:
        host, path = scp.groups()
        normalized = f"{host.lower()}/{path}"
    elif "://" in selected:
        from urllib.parse import urlsplit

        parsed = urlsplit(selected)
        host = (parsed.hostname or "").lower()
        normalized = f"{host}/{parsed.path.lstrip('/')}" if host else parsed.path
    else:
        normalized = str(_absolute(selected))
    return normalized[:-4] if normalized.endswith(".git") else normalized


def discover_repository(
    checkout: str | os.PathLike[str],
    *,
    repository: str | None = None,
) -> RepositoryIdentity:
    """Discover a logical repository and distinct worktree without leaking credentials."""
    selected = _absolute(checkout)
    if not selected.is_dir():
        raise CoordinatorError(f"checkout does not exist: {selected}")
    root = _absolute(_git_value(selected, "rev-parse", "--show-toplevel") or selected)
    if repository is not None:
        if not isinstance(repository, str) or not repository.strip():
            raise CoordinatorError("repository must be a non-empty string")
        name = repository.strip()
        repository_id = name
    else:
        remote = _git_value(root, "config", "--get", "remote.origin.url", required=False)
        if remote:
            name = _remote_identity(remote)
        else:
            common = _git_value(
                root,
                "rev-parse",
                "--path-format=absolute",
                "--git-common-dir",
            )
            name = str(_absolute(common or root))
        repository_id = "repo-" + hashlib.sha256(name.encode()).hexdigest()[:16]
    worktree_id = "worktree-" + hashlib.sha256(os.fsencode(str(root))).hexdigest()[:16]
    return RepositoryIdentity(repository_id, name, worktree_id, root)


def _git_branch(checkout: Path) -> str:
    return (
        _git_value(checkout, "symbolic-ref", "--quiet", "--short", "HEAD", required=False)
        or _git_value(checkout, "rev-parse", "--short", "HEAD")
        or ""
    )


def _git_head(checkout: Path) -> str:
    return _validate_head_sha(_git_value(checkout, "rev-parse", "HEAD"), required=True) or ""


def _assert_clean_head(checkout: Path, expected_head: str) -> None:
    actual = _git_head(checkout)
    if actual != expected_head:
        raise CoordinatorError(
            f"checkout head changed from queued {expected_head} to {actual}"
        )
    status = _git_value(
        checkout,
        "status",
        "--porcelain",
        "--untracked-files=normal",
        required=False,
    )
    if status:
        raise CoordinatorError("checkout is dirty; commit or remove changes before a full run")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _parse_time(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None


def _shell_status(returncode: int) -> int:
    return 128 + abs(returncode) if returncode < 0 else returncode


def _process_identity(pid: int) -> tuple[int, str] | None:
    """Linux process identity beyond a reusable PID.

    The gate already relies on Linux ``flock``.  Field 22 of ``/proc/<pid>/stat`` is the
    process start tick and field 4 is its parent PID. Together they let a restarted broker
    distinguish its worker tree from later processes that reused one of its PIDs.
    """
    try:
        stat = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
        fields_after_name = stat[stat.rfind(")") + 2:].split()
        return int(fields_after_name[1]), fields_after_name[19]
    except (OSError, IndexError, ValueError):
        return None


def _process_start_token(pid: int) -> str | None:
    identity = _process_identity(pid)
    return identity[1] if identity is not None else None


def _same_process(pid: int | None, token: str | None) -> bool:
    if pid is None or token is None:
        return False
    try:
        os.kill(pid, 0)
    except (OSError, ProcessLookupError):
        return False
    return _process_start_token(pid) == token


def _is_descendant_process(
    pid: int,
    token: str,
    *,
    ancestor_pid: int,
    ancestor_token: str,
) -> bool:
    """Prove one live process belongs to an admitted worker tree without trusting env."""
    current_pid = pid
    expected_token = token
    visited: set[int] = set()
    while current_pid > 0 and current_pid not in visited:
        visited.add(current_pid)
        identity = _process_identity(current_pid)
        if identity is None or identity[1] != expected_token:
            return False
        if current_pid == ancestor_pid:
            return expected_token == ancestor_token
        parent_pid = identity[0]
        parent_identity = _process_identity(parent_pid)
        if parent_identity is None:
            return False
        current_pid = parent_pid
        expected_token = parent_identity[1]
    return False


def _process_group_exists(process_group: int | None) -> bool:
    if process_group is None:
        return False
    try:
        os.killpg(process_group, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _validate_head_sha(value: Any, *, required: bool) -> str | None:
    if value is None and not required:
        return None
    if not isinstance(value, str) or len(value) != 40:
        raise CoordinatorError("gate head_sha must be exactly 40 hexadecimal characters")
    lowered = value.lower()
    if any(character not in "0123456789abcdef" for character in lowered):
        raise CoordinatorError("gate head_sha must contain only hexadecimal characters")
    return lowered


def _validate_avoid_commits(value: object) -> tuple[str, ...]:
    """Normalize the commits one landing must refuse to publish."""
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise CoordinatorError("avoid_commits must be a sequence of 40-hex commit SHAs")
    selected: list[str] = []
    for item in value:
        sha = _validate_head_sha(item, required=True)
        if sha not in selected:
            selected.append(sha)
    return tuple(selected)


def _validate_environment(environment: Mapping[str, str] | None) -> dict[str, str]:
    selected = dict(os.environ if environment is None else environment)
    if not all(
        isinstance(key, str) and key and "=" not in key and "\0" not in key
        and isinstance(value, str) and "\0" not in value
        for key, value in selected.items()
    ):
        raise CoordinatorError(
            "gate environment must map non-empty string names to string values"
        )
    if RUN_ID_ENV in selected:
        raise CoordinatorError(
            "a coordinated job cannot submit another coordinated job; invoke it "
            "directly from the checkout"
        )
    return selected


def _positive_mapping(
    value: Mapping[str, int] | None,
    *,
    subject: str,
    include_job: bool,
) -> dict[str, int]:
    selected: dict[str, int] = {"jobs": 1} if include_job else {}
    if value is None:
        return selected
    if not isinstance(value, Mapping):
        raise CoordinatorError(f"{subject} must be a name-to-positive-integer mapping")
    for name, units in value.items():
        if not isinstance(name, str) or not _RESOURCE_NAME.fullmatch(name):
            raise CoordinatorError(f"invalid {subject} name {name!r}")
        if not isinstance(units, int) or isinstance(units, bool) or units <= 0:
            raise CoordinatorError(f"{subject} {name!r} must be a positive integer")
        selected[name] = units
    return dict(sorted(selected.items()))


def parse_resource_claims(values: Iterable[str]) -> dict[str, int]:
    """Parse repeatable CLI ``NAME=UNITS`` claims into the strict resource contract."""
    raw: dict[str, int] = {}
    for value in values:
        if not isinstance(value, str):
            raise CoordinatorError("resource claims must be strings")
        name, separator, raw_units = value.partition("=")
        if not separator:
            raise CoordinatorError(f"resource {value!r} must use NAME=UNITS")
        if name in raw:
            raise CoordinatorError(f"resource {name!r} was declared more than once")
        try:
            raw[name] = int(raw_units)
        except ValueError as exc:
            raise CoordinatorError(f"resource {name!r} must use integer units") from exc
    return _positive_mapping(raw, subject="resource", include_job=False)


def configured_capacities(section: Mapping[str, Any] | None) -> dict[str, int]:
    """Return validated machine capacities from one configuration file section."""
    if section is None:
        return {"jobs": DEFAULT_JOB_CAPACITY}
    capacities = _positive_mapping(section, subject="capacity", include_job=False)
    capacities.setdefault("jobs", DEFAULT_JOB_CAPACITY)
    return dict(sorted(capacities.items()))


def broker_config(state_dir: str | os.PathLike[str]) -> BrokerConfig:
    """Read one state directory's configuration as a coordinator-level failure."""
    try:
        return load_broker_config(state_dir)
    except BrokerConfigError as exc:
        raise CoordinatorError(str(exc)) from exc


def _spool_protocol(paths: CoordinatorPaths) -> int | None:
    """Read an existing idle spool generation without creating or migrating it."""
    if not paths.database.exists():
        return None
    try:
        uri = paths.database.resolve().as_uri() + "?mode=ro"
        with closing(sqlite3.connect(uri, uri=True)) as database:
            row = database.execute(
                "SELECT value FROM coordinator_meta WHERE key = 'protocol'"
            ).fetchone()
    except sqlite3.Error as exc:
        raise CoordinatorError(
            f"cannot inspect gate queue protocol in {paths.database}: {exc}"
        ) from exc
    if row is None:
        raise CoordinatorError(
            f"gate queue database {paths.database} has no protocol metadata"
        )
    try:
        return int(row[0])
    except (TypeError, ValueError) as exc:
        raise CoordinatorError(
            f"gate queue database {paths.database} has invalid protocol metadata"
        ) from exc


def _spool_initializing_error(error: CoordinatorError) -> bool:
    detail = str(error)
    return "no such table: coordinator_meta" in detail or (
        "has no protocol metadata" in detail
    )


def _validate_resources(
    resources: Mapping[str, int] | None,
    capacities: Mapping[str, int],
) -> dict[str, int]:
    selected = _positive_mapping(resources, subject="resource", include_job=True)
    for name, units in selected.items():
        if name not in capacities:
            raise CoordinatorError(
                f"resource {name!r} has no configured machine capacity"
            )
        if units > capacities[name]:
            raise CoordinatorError(
                f"resource {name!r} requests {units}, above capacity {capacities[name]}"
            )
    return selected


def _read_broker_owner(paths: CoordinatorPaths) -> dict[str, Any] | None:
    """Read either supported live flock owner, ignoring bytes from a dead broker."""
    try:
        descriptor = os.open(paths.owner_lock, os.O_RDWR)
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise CoordinatorError(
            f"cannot open gate broker ownership file {paths.owner_lock}: {exc}"
        ) from exc
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            try:
                raw = os.pread(
                    descriptor,
                    MAX_OWNER_METADATA_BYTES + 1,
                    0,
                ).decode("utf-8", errors="strict")
            except (OSError, UnicodeDecodeError) as exc:
                raise _OwnerMetadataError(
                    f"live gate broker ownership metadata is unreadable in "
                    f"{paths.owner_lock}"
                ) from exc
        else:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
            return None
    finally:
        os.close(descriptor)
    if len(raw.encode("utf-8")) > MAX_OWNER_METADATA_BYTES:
        raise _OwnerMetadataError(
            f"live gate broker ownership metadata is oversized in {paths.owner_lock}"
        )
    fields: dict[str, str] = {}
    for line in raw.splitlines():
        key, separator, value = line.partition("=")
        if not separator or key in fields:
            raise _OwnerMetadataError(
                f"live gate broker wrote invalid ownership metadata in {paths.owner_lock}"
            )
        fields[key] = value
    try:
        protocol = int(fields["protocol"])
        owner_pid = int(fields["pid"])
        capacities = _positive_mapping(
            json.loads(fields["capacities"]),
            subject="owner capacity",
            include_job=False,
        )
    except (KeyError, TypeError, ValueError, json.JSONDecodeError, CoordinatorError) as exc:
        raise _OwnerMetadataError(
            f"live gate broker wrote incomplete ownership metadata in {paths.owner_lock}"
        ) from exc
    try:
        bindings = validate_resource_bindings(
            json.loads(fields["resource_bindings"])
        )
        capabilities = validate_resource_capabilities(
            json.loads(fields["resource_capabilities"])
        )
    except (
        KeyError,
        TypeError,
        json.JSONDecodeError,
        ResourceContractError,
    ) as exc:
        raise _OwnerMetadataError(
            f"live gate broker wrote incomplete resource metadata in {paths.owner_lock}"
        ) from exc
    if owner_pid <= 0:
        raise _OwnerMetadataError("live gate broker wrote invalid numeric metadata")
    owner: dict[str, Any] = {
        "pid": owner_pid,
        "protocol": protocol,
        "capacities": capacities,
        "resource_bindings": bindings,
        "resource_capabilities": capabilities,
    }
    if protocol == NATIVE_PROTOCOL:
        try:
            implementation = fields["implementation"]
            version = fields["version"]
            build = fields["build"]
        except KeyError as exc:
            raise _OwnerMetadataError(
                f"live native broker wrote incomplete identity metadata in "
                f"{paths.owner_lock}"
            ) from exc
        if (
            implementation != NATIVE_IMPLEMENTATION
            or not version
            or not build
            or any("\0" in value or "\n" in value for value in (version, build))
        ):
            raise _OwnerMetadataError(
                f"live native broker wrote invalid identity metadata in {paths.owner_lock}"
            )
        owner.update(
            implementation=implementation,
            version=version,
            build=build,
        )
    return owner


def _flock_holder_pid(path: Path) -> int | None:
    """Return the kernel-reported PID holding one exclusive Linux flock."""
    try:
        details = path.stat()
        lines = Path("/proc/locks").read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise CoordinatorError(
            f"cannot verify the gate broker ownership lock: {exc}"
        ) from exc
    expected = (os.major(details.st_dev), os.minor(details.st_dev), details.st_ino)
    holders: set[int] = set()
    for line in lines:
        fields = line.split()
        if len(fields) < 6 or fields[1:4] != ["FLOCK", "ADVISORY", "WRITE"]:
            continue
        device = fields[5].split(":")
        if len(device) != 3:
            continue
        try:
            identity = (int(device[0], 16), int(device[1], 16), int(device[2]))
            pid = int(fields[4])
        except ValueError:
            continue
        if identity == expected and pid > 0:
            holders.add(pid)
    if len(holders) > 1:
        raise CoordinatorError("gate broker ownership lock has multiple kernel holders")
    return next(iter(holders), None)


def _stop_verified_legacy_owner(
    paths: CoordinatorPaths,
    owner: Mapping[str, Any],
) -> dict[str, Any] | None:
    """Signal only the exact process the kernel identifies as the legacy lock holder."""
    pid = owner["pid"]
    try:
        pidfd = os.pidfd_open(pid)
    except ProcessLookupError:
        return _read_broker_owner(paths)
    except OSError as exc:
        raise CoordinatorError(
            f"cannot identify the drained legacy broker {pid}: {exc}"
        ) from exc
    try:
        current = _read_broker_owner(paths)
        if (
            current is None
            or current["protocol"] != PROTOCOL
            or current["pid"] != pid
        ):
            return current
        if _flock_holder_pid(paths.owner_lock) != pid:
            refreshed = _read_broker_owner(paths)
            if refreshed is None or refreshed["pid"] != pid:
                return refreshed
            raise CoordinatorError(
                "cannot verify the drained legacy broker as the ownership-lock holder"
            )
        try:
            signal.pidfd_send_signal(pidfd, signal.SIGTERM)
        except ProcessLookupError:
            return _read_broker_owner(paths)
        except OSError as exc:
            raise CoordinatorError(
                f"cannot stop the drained legacy broker {pid}: {exc}"
            ) from exc
    finally:
        os.close(pidfd)
    return current


def _broker_owner(paths: CoordinatorPaths) -> dict[str, Any] | None:
    """Read only the legacy protocol-4 owner used by the Python reference broker."""
    owner = _read_broker_owner(paths)
    if owner is not None and owner["protocol"] != PROTOCOL:
        raise CoordinatorError(
            f"gate coordinator protocol mismatch: broker has {owner['protocol']}, "
            f"Python reference broker needs {PROTOCOL}"
        )
    return owner


def _maintenance_record(
    db: sqlite3.Connection,
) -> dict[str, str] | None:
    """Read and strictly validate the durable maintenance marker."""
    rows = db.execute(
        "SELECT 'metadata', key, value FROM coordinator_meta "
        "WHERE key IN (?, ?, ?, ?) "
        "UNION ALL "
        "SELECT 'trigger', name, NULL FROM sqlite_master WHERE type = 'trigger' "
        "AND name IN (?, ?, ?)",
        (*MAINTENANCE_METADATA_KEYS, *MAINTENANCE_TRIGGER_NAMES),
    ).fetchall()
    values = {
        str(row[1]): str(row[2])
        for row in rows
        if row[0] == "metadata"
    }
    guards = {
        str(row[1])
        for row in rows
        if row[0] == "trigger"
    }
    if not values:
        if guards:
            raise CoordinatorError(
                "coordinator maintenance submission guards have no marker"
            )
        return None
    if set(values) != set(MAINTENANCE_METADATA_KEYS):
        raise CoordinatorError("coordinator maintenance metadata is incomplete")
    if values["maintenance_state"] not in MAINTENANCE_STATES:
        raise CoordinatorError("coordinator maintenance state is invalid")
    if not _DRAIN_ID.fullmatch(values["maintenance_id"]):
        raise CoordinatorError("coordinator maintenance drain ID is invalid")
    reason = values["maintenance_reason"]
    if not reason or len(reason) > MAX_MAINTENANCE_REASON or "\0" in reason:
        raise CoordinatorError("coordinator maintenance reason is invalid")
    started_at = values["maintenance_started_at"]
    try:
        parsed_start = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
    except ValueError as exc:
        raise CoordinatorError("coordinator maintenance start time is invalid") from exc
    if (
        not _MAINTENANCE_TIME.fullmatch(started_at)
        or parsed_start.utcoffset() != timezone.utc.utcoffset(None)
    ):
        raise CoordinatorError("coordinator maintenance start time is invalid")
    if guards != set(MAINTENANCE_TRIGGER_NAMES):
        raise CoordinatorError(
            "coordinator maintenance submission guards are missing"
        )
    return values


def _install_maintenance_guards(db: sqlite3.Connection) -> None:
    db.execute(
        f"""
        CREATE TRIGGER {MAINTENANCE_TRIGGER_NAMES[0]}
        BEFORE INSERT ON runs
        WHEN EXISTS (
            SELECT 1 FROM coordinator_meta
            WHERE key = 'maintenance_state'
              AND value IN ('draining', 'drained')
        )
        BEGIN
            SELECT RAISE(ABORT, '{MAINTENANCE_REFUSAL}');
        END
        """
    )
    db.execute(
        f"""
        CREATE TRIGGER {MAINTENANCE_TRIGGER_NAMES[1]}
        BEFORE INSERT ON coordinator_meta
        WHEN NEW.key = 'last_activity'
          AND EXISTS (
              SELECT 1 FROM coordinator_meta
              WHERE key = 'maintenance_state'
                AND value = 'drained'
          )
        BEGIN
            SELECT RAISE(ABORT, '{MAINTENANCE_REFUSAL}');
        END
        """
    )
    db.execute(
        f"""
        CREATE TRIGGER {MAINTENANCE_TRIGGER_NAMES[2]}
        BEFORE UPDATE OF value ON coordinator_meta
        WHEN OLD.key = 'last_activity'
          AND EXISTS (
              SELECT 1 FROM coordinator_meta
              WHERE key = 'maintenance_state'
                AND value = 'drained'
          )
        BEGIN
            SELECT RAISE(ABORT, '{MAINTENANCE_REFUSAL}');
        END
        """
    )


def _remove_maintenance_guards(db: sqlite3.Connection) -> None:
    for name in MAINTENANCE_TRIGGER_NAMES:
        db.execute(f"DROP TRIGGER IF EXISTS {name}")


def _maintenance_public(
    record: Mapping[str, str],
    *,
    protocol: int,
    live: int,
    owner: Mapping[str, Any] | None,
) -> dict[str, Any]:
    return {
        "state": record["maintenance_state"],
        "drain_id": record["maintenance_id"],
        "reason": record["maintenance_reason"],
        "started_at": record["maintenance_started_at"],
        "protocol": protocol,
        "live": live,
        "broker_pid": None if owner is None else owner["pid"],
    }


def _validated_maintenance_receipt(value: Any) -> dict[str, Any]:
    expected = {
        "state",
        "drain_id",
        "reason",
        "started_at",
        "protocol",
        "live",
        "broker_pid",
    }
    if not isinstance(value, dict) or set(value) != expected:
        raise CoordinatorError("coordinator returned an invalid maintenance receipt")
    if value["state"] not in MAINTENANCE_STATES:
        raise CoordinatorError("coordinator returned an invalid maintenance state")
    if not isinstance(value["drain_id"], str) or not _DRAIN_ID.fullmatch(
        value["drain_id"]
    ):
        raise CoordinatorError("coordinator returned an invalid maintenance drain ID")
    reason = value["reason"]
    if (
        not isinstance(reason, str)
        or not reason
        or len(reason) > MAX_MAINTENANCE_REASON
        or "\0" in reason
    ):
        raise CoordinatorError("coordinator returned an invalid maintenance reason")
    started_at = value["started_at"]
    try:
        if (
            not isinstance(started_at, str)
            or not _MAINTENANCE_TIME.fullmatch(started_at)
        ):
            raise ValueError
        parsed_start = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
        if parsed_start.utcoffset() != timezone.utc.utcoffset(None):
            raise ValueError
    except ValueError as exc:
        raise CoordinatorError(
            "coordinator returned an invalid maintenance start time"
        ) from exc
    if value["protocol"] not in {PROTOCOL, NATIVE_PROTOCOL}:
        raise CoordinatorError("coordinator returned an invalid maintenance protocol")
    if (
        not isinstance(value["live"], int)
        or isinstance(value["live"], bool)
        or value["live"] < 0
    ):
        raise CoordinatorError("coordinator returned an invalid maintenance live count")
    broker_pid = value["broker_pid"]
    if broker_pid is not None and (
        not isinstance(broker_pid, int)
        or isinstance(broker_pid, bool)
        or broker_pid <= 0
    ):
        raise CoordinatorError("coordinator returned an invalid maintenance broker PID")
    return value


def _legacy_maintenance_status(
    paths: CoordinatorPaths,
    *,
    transition: bool,
) -> dict[str, Any]:
    if not paths.database.is_file():
        raise CoordinatorError(f"no gate queue database exists at {paths.database}")
    configuration = broker_config(paths.state_dir)
    timeout = (
        DEFAULT_DATABASE_TIMEOUT
        if configuration.database_timeout is None
        else configuration.database_timeout
    )
    with closing(sqlite3.connect(paths.database, timeout=timeout)) as db:
        db.row_factory = sqlite3.Row
        record = _maintenance_record(db)
        if record is None:
            raise CoordinatorError("coordinator is not draining")
        protocol_row = db.execute(
            "SELECT value FROM coordinator_meta WHERE key = 'protocol'"
        ).fetchone()
        try:
            protocol = int(protocol_row["value"])
        except (TypeError, ValueError) as exc:
            raise CoordinatorError("coordinator protocol metadata is invalid") from exc
        live = int(
            db.execute(
                "SELECT COUNT(*) FROM runs WHERE status IN ('queued', 'running')"
            ).fetchone()[0]
        )
    owner = _read_broker_owner(paths)
    if (
        transition
        and record["maintenance_state"] == "draining"
        and live == 0
        and owner is not None
        and owner["protocol"] == PROTOCOL
        and owner["pid"] != os.getpid()
    ):
        owner = _stop_verified_legacy_owner(paths, owner)
    if (
        transition
        and record["maintenance_state"] == "draining"
        and live == 0
        and owner is None
    ):
        descriptor = os.open(paths.owner_lock, os.O_RDWR | os.O_CREAT, 0o600)
        os.fchmod(descriptor, 0o600)
        try:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                owner = _read_broker_owner(paths)
            else:
                with closing(sqlite3.connect(paths.database, timeout=timeout)) as db:
                    db.row_factory = sqlite3.Row
                    db.execute("BEGIN IMMEDIATE")
                    current = _maintenance_record(db)
                    current_live = int(
                        db.execute(
                            "SELECT COUNT(*) FROM runs "
                            "WHERE status IN ('queued', 'running')"
                        ).fetchone()[0]
                    )
                    if (
                        current is not None
                        and current["maintenance_state"] == "draining"
                        and current_live == 0
                    ):
                        db.execute(
                            "UPDATE coordinator_meta SET value = 'drained' "
                            "WHERE key = 'maintenance_state'"
                        )
                        current = dict(current)
                        current["maintenance_state"] = "drained"
                    db.commit()
                    if current is None:
                        raise CoordinatorError("coordinator is not draining")
                    record = current
                    live = current_live
                owner = None
        finally:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
            finally:
                os.close(descriptor)
    return _maintenance_public(record, protocol=protocol, live=live, owner=owner)


def _legacy_begin_drain(
    paths: CoordinatorPaths,
    *,
    drain_id: str,
    reason: str,
) -> dict[str, Any]:
    if not paths.database.is_file():
        raise CoordinatorError(f"no gate queue database exists at {paths.database}")
    configuration = broker_config(paths.state_dir)
    timeout = (
        DEFAULT_DATABASE_TIMEOUT
        if configuration.database_timeout is None
        else configuration.database_timeout
    )
    with closing(sqlite3.connect(paths.database, timeout=timeout)) as db:
        db.row_factory = sqlite3.Row
        db.execute("BEGIN IMMEDIATE")
        protocol_row = db.execute(
            "SELECT value FROM coordinator_meta WHERE key = 'protocol'"
        ).fetchone()
        if protocol_row is None or protocol_row["value"] != str(PROTOCOL):
            selected = None if protocol_row is None else protocol_row["value"]
            raise CoordinatorError(
                f"durable draining supports protocol {PROTOCOL} and {NATIVE_PROTOCOL}; "
                f"state uses {selected!r}"
            )
        existing = _maintenance_record(db)
        if existing is None:
            started_at = _now()
            db.execute(
                "INSERT INTO coordinator_meta(key, value) VALUES ('last_activity', ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (str(time.time()),),
            )
            _install_maintenance_guards(db)
            for key, value in (
                ("maintenance_state", "draining"),
                ("maintenance_id", drain_id),
                ("maintenance_reason", reason),
                ("maintenance_started_at", started_at),
            ):
                db.execute(
                    "INSERT INTO coordinator_meta(key, value) VALUES (?, ?)",
                    (key, value),
                )
        db.commit()
    return _legacy_maintenance_status(paths, transition=True)


def _legacy_resume(paths: CoordinatorPaths, drain_id: str) -> dict[str, Any]:
    configuration = broker_config(paths.state_dir)
    timeout = (
        DEFAULT_DATABASE_TIMEOUT
        if configuration.database_timeout is None
        else configuration.database_timeout
    )
    descriptor = os.open(paths.owner_lock, os.O_RDWR | os.O_CREAT, 0o600)
    os.fchmod(descriptor, 0o600)
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise CoordinatorError(
                "cannot resume while a broker or maintenance operation owns the queue"
            ) from exc
        with closing(sqlite3.connect(paths.database, timeout=timeout)) as db:
            db.row_factory = sqlite3.Row
            db.execute("BEGIN IMMEDIATE")
            record = _maintenance_record(db)
            if record is None:
                raise CoordinatorError("coordinator is not draining")
            if record["maintenance_id"] != drain_id:
                raise CoordinatorError("maintenance drain ID does not match")
            live = db.execute(
                "SELECT run_id FROM runs WHERE status IN ('queued', 'running') "
                "ORDER BY sequence"
            ).fetchall()
            if live:
                raise CoordinatorError(
                    "cannot resume while drained work remains live: "
                    + ", ".join(str(row["run_id"]) for row in live)
                )
            _remove_maintenance_guards(db)
            db.execute(
                "DELETE FROM coordinator_meta WHERE key IN (?, ?, ?, ?)",
                MAINTENANCE_METADATA_KEYS,
            )
            db.commit()
    finally:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)
    return {"state": "open", "drain_id": drain_id, "resumed": True}


def _create_child_cpu_lease_table(db: sqlite3.Connection) -> None:
    db.execute(
        """
        CREATE TABLE child_cpu_leases (
            sequence INTEGER PRIMARY KEY AUTOINCREMENT,
            lease_id TEXT NOT NULL UNIQUE,
            run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
            status TEXT NOT NULL CHECK (
                status IN ('waiting', 'active', 'released', 'cancelled')
            ),
            requested INTEGER NOT NULL CHECK (requested > 0),
            minimum INTEGER NOT NULL CHECK (minimum > 0 AND minimum <= requested),
            granted INTEGER NOT NULL DEFAULT 0 CHECK (
                granted >= 0 AND granted <= requested
            ),
            owner_pid INTEGER NOT NULL CHECK (owner_pid > 0),
            owner_start_token TEXT NOT NULL,
            bypass_count INTEGER NOT NULL DEFAULT 0 CHECK (bypass_count >= 0),
            created_at TEXT NOT NULL,
            acquired_at TEXT,
            finished_at TEXT
        )
        """
    )
    db.execute(
        "CREATE INDEX child_cpu_leases_run_sequence "
        "ON child_cpu_leases(run_id, sequence)"
    )
    db.execute(
        "CREATE INDEX child_cpu_leases_status_sequence "
        "ON child_cpu_leases(status, sequence)"
    )


def _enable_wal(db: sqlite3.Connection) -> None:
    selected = db.execute("PRAGMA journal_mode = WAL").fetchone()
    mode = None if selected is None else str(selected[0]).lower()
    if mode != "wal":
        raise CoordinatorError(
            f"gate queue database refused WAL journal mode (reported {mode!r})"
        )


def migrate_queue(
    *,
    state_dir: str | os.PathLike[str] | None = None,
    checkout: str | os.PathLike[str] | None = None,
) -> dict[str, int | bool]:
    """Validate an idle spool and perform only explicitly defined protocol migrations."""
    paths = queue_paths(state_dir=state_dir, checkout=checkout)
    try:
        state_details = paths.state_dir.lstat()
    except FileNotFoundError as exc:
        raise CoordinatorError(
            f"no gate queue state exists at {paths.state_dir}"
        ) from exc
    except OSError as exc:
        raise CoordinatorError(
            f"cannot inspect gate queue state {paths.state_dir}: {exc}"
        ) from exc
    if stat.S_ISLNK(state_details.st_mode) or not stat.S_ISDIR(state_details.st_mode):
        raise CoordinatorError(
            f"gate queue state path {paths.state_dir} is not a real directory"
        )
    if state_details.st_uid != os.getuid():
        raise CoordinatorError(
            f"gate queue state {paths.state_dir} belongs to another user"
        )
    if not paths.database.is_file():
        raise CoordinatorError(
            f"no gate queue database exists at {paths.database}"
        )
    configuration = broker_config(paths.state_dir)
    database_timeout = (
        DEFAULT_DATABASE_TIMEOUT
        if configuration.database_timeout is None
        else configuration.database_timeout
    )

    descriptor = os.open(paths.owner_lock, os.O_RDWR | os.O_CREAT, 0o600)
    os.fchmod(descriptor, 0o600)
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise CoordinatorError(
                "cannot migrate while a gate broker owns the queue; let every live "
                "run and the old broker finish first"
            ) from exc

        with sqlite3.connect(paths.database, timeout=database_timeout) as db:
            db.row_factory = sqlite3.Row
            db.execute("BEGIN IMMEDIATE")
            try:
                stored = db.execute(
                    "SELECT value FROM coordinator_meta WHERE key = 'protocol'"
                ).fetchone()
            except sqlite3.Error as exc:
                raise CoordinatorError(
                    "gate queue database has no readable protocol metadata"
                ) from exc
            if stored is None:
                raise CoordinatorError(
                    "gate queue database has no protocol value"
                )
            previous = int(stored["value"])
            original_protocol = previous
            changed = False
            if previous not in {1, 2, 3, PROTOCOL}:
                raise CoordinatorError(
                    f"queue protocol is {previous}; no migration to {PROTOCOL} is defined"
                )
            if previous in {1, 2, 3}:
                live = db.execute(
                    "SELECT run_id FROM runs WHERE status IN ('queued', 'running') "
                    "ORDER BY sequence"
                ).fetchall()
                if live:
                    raise CoordinatorError(
                        f"cannot migrate protocol {previous} with live runs: "
                        + ", ".join(row["run_id"] for row in live)
                    )
            if previous == 1:
                db.execute(
                    """
                    CREATE TABLE runs_v2 (
                        sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                        run_id TEXT NOT NULL UNIQUE,
                        status TEXT NOT NULL CHECK (
                            status IN ('queued', 'running', 'passed', 'failed',
                                       'cancelled', 'interrupted')
                        ),
                        kind TEXT NOT NULL CHECK (
                            kind IN ('check', 'full', 'merge', 'land')
                        ),
                        phase TEXT NOT NULL CHECK (
                            phase IN ('queued', 'running', 'preflight', 'gating',
                                      'publishing', 'complete')
                        ),
                        label TEXT NOT NULL,
                        agent TEXT NOT NULL,
                        repository_id TEXT NOT NULL,
                        repository TEXT NOT NULL,
                        worktree_id TEXT NOT NULL,
                        checkout TEXT NOT NULL,
                        branch TEXT NOT NULL,
                        head_sha TEXT,
                        barrier INTEGER NOT NULL CHECK (barrier IN (0, 1)),
                        resources_json TEXT NOT NULL,
                        gate_run_id TEXT,
                        publication_adapter TEXT,
                        publication_request TEXT,
                        failure_reason TEXT,
                        gate_exit_status INTEGER,
                        reported_exit_status INTEGER,
                        caller_pid INTEGER NOT NULL,
                        command_json TEXT NOT NULL,
                        environment_json TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        started_at TEXT,
                        finished_at TEXT,
                        exit_status INTEGER,
                        worker_pid INTEGER,
                        worker_start_token TEXT,
                        cancel_requested INTEGER NOT NULL DEFAULT 0,
                        cancel_requested_at TEXT
                    )
                    """
                )
                db.execute(
                    """
                    INSERT INTO runs_v2 (
                        sequence, run_id, status, kind, phase, label, agent,
                        repository_id, repository, worktree_id, checkout, branch,
                        head_sha, barrier, resources_json, gate_run_id,
                        publication_adapter, publication_request, failure_reason,
                        gate_exit_status, reported_exit_status, caller_pid,
                        command_json, environment_json,
                        created_at, started_at, finished_at, exit_status, worker_pid,
                        worker_start_token, cancel_requested, cancel_requested_at
                    )
                    SELECT sequence, run_id, status, kind, 'complete', label, agent,
                           repository_id, repository, worktree_id, checkout, branch,
                           head_sha, barrier, resources_json, gate_run_id,
                           publication_adapter, publication_request, failure_reason,
                           NULL, NULL, caller_pid, command_json, environment_json, created_at,
                           started_at, finished_at, exit_status, worker_pid,
                           worker_start_token, cancel_requested, cancel_requested_at
                    FROM runs ORDER BY sequence
                    """
                )
                db.execute("DROP TABLE runs")
                db.execute("ALTER TABLE runs_v2 RENAME TO runs")
                db.execute(
                    "CREATE INDEX runs_status_sequence ON runs(status, sequence)"
                )
                db.execute(
                    "CREATE INDEX runs_repository_sequence "
                    "ON runs(repository_id, sequence)"
                )
                db.execute(
                    "UPDATE coordinator_meta SET value = ? WHERE key = 'protocol'",
                    ("2",),
                )
                previous = 2
                changed = True
            if previous == 2:
                db.execute(
                    "ALTER TABLE runs ADD COLUMN resource_contract_json "
                    "TEXT NOT NULL DEFAULT '{}'"
                )
                db.execute(
                    "ALTER TABLE runs ADD COLUMN resource_receipt_json "
                    "TEXT NOT NULL DEFAULT '{}'"
                )
                db.execute(
                    "ALTER TABLE runs ADD COLUMN resource_state_json "
                    "TEXT NOT NULL DEFAULT '{}'"
                )
                rows = db.execute(
                    "SELECT run_id, resources_json FROM runs ORDER BY sequence"
                ).fetchall()
                for row in rows:
                    try:
                        resources = _positive_mapping(
                            json.loads(row["resources_json"]),
                            subject="legacy stored resource",
                            include_job=False,
                        )
                        contract = resource_contract(resources, {})
                        receipt = initial_resource_receipt(resources)
                    except (
                        TypeError,
                        json.JSONDecodeError,
                        CoordinatorError,
                        ResourceContractError,
                    ) as exc:
                        raise CoordinatorError(
                            f"cannot migrate run {row['run_id']}: invalid stored resources"
                        ) from exc
                    db.execute(
                        "UPDATE runs SET resource_contract_json = ?, "
                        "resource_receipt_json = ?, resource_state_json = '{}' "
                        "WHERE run_id = ?",
                        (
                            json.dumps(contract, separators=(",", ":")),
                            json.dumps(receipt, separators=(",", ":")),
                            row["run_id"],
                        ),
                    )
                db.execute(
                    "UPDATE coordinator_meta SET value = ? WHERE key = 'protocol'",
                    ("3",),
                )
                previous = 3
                changed = True
            if previous == 3:
                _create_child_cpu_lease_table(db)
                db.execute(
                    "UPDATE coordinator_meta SET value = ? WHERE key = 'protocol'",
                    (str(PROTOCOL),),
                )
                changed = True
            db.commit()
            _enable_wal(db)
        paths.database.chmod(0o600)
        return {
            "changed": changed,
            "from_protocol": original_protocol,
            "to_protocol": PROTOCOL,
        }
    finally:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)


class CoordinatorBroker:
    """Own one fair queue and supervise its compatible active subprocesses.

    ``idle_timeout=None`` is useful for an explicitly managed server and for tests.  The
    on-demand daemon uses a short idle timeout once no live run remains, avoiding a broker
    from an old branch living indefinitely; the SQLite history remains for the next one.
    """

    def __init__(
        self,
        state_dir: str | os.PathLike[str],
        *,
        idle_timeout: float | None = DEFAULT_IDLE_SECONDS,
        recent_limit: int = DEFAULT_RECENT_LIMIT,
        capacities: Mapping[str, int] | None = None,
        resource_bindings: Mapping[str, Mapping[str, object]] | None = None,
        resource_backends: Mapping[str, ResourceBackend] | None = None,
    ):
        if recent_limit < 1:
            raise ValueError("recent_limit must be positive")
        if idle_timeout is not None and idle_timeout <= 0:
            raise ValueError("idle_timeout must be positive or None")
        self.paths = queue_paths(state_dir=state_dir)
        self.idle_timeout = idle_timeout
        self.recent_limit = recent_limit
        # One read of the state directory's configuration file serves capacity, bindings,
        # and the delegated cgroup root, so a broker cannot mix two file revisions.
        configuration = broker_config(self.paths.state_dir)
        self.database_timeout = (
            DEFAULT_DATABASE_TIMEOUT
            if configuration.database_timeout is None
            else configuration.database_timeout
        )
        self.capacities = (
            configured_capacities(configuration.capacities)
            if capacities is None
            else _positive_mapping(capacities, subject="capacity", include_job=False)
        )
        if "jobs" not in self.capacities:
            raise ValueError("capacities must include a positive 'jobs' capacity")
        try:
            self.resource_bindings = validate_resource_bindings(
                configured_resource_bindings(configuration.bindings)
                if resource_bindings is None
                else resource_bindings
            )
            self.resource_backends = validate_resource_backends(resource_backends)
            referenced_backends = {
                binding["backend"]
                for binding in self.resource_bindings.values()
                if binding["backend"] is not None
            }
            if (
                CGROUP_BACKEND in referenced_backends
                and CGROUP_BACKEND not in self.resource_backends
            ):
                self.resource_backends[CGROUP_BACKEND] = CgroupV2Backend.from_config(
                    configuration.cgroup_root,
                    state_dir=self.paths.state_dir,
                    cgroup_io=configuration.cgroup_io,
                )
            if (
                PROJECT_QUOTA_BACKEND in referenced_backends
                and PROJECT_QUOTA_BACKEND not in self.resource_backends
            ):
                self.resource_backends[PROJECT_QUOTA_BACKEND] = ProjectQuotaBackend(
                    self.paths.state_dir
                )
            self.resource_capabilities = probe_resource_backends(
                self.resource_backends,
                self.resource_bindings,
            )
        except ResourceContractError as exc:
            raise CoordinatorError(str(exc)) from exc
        self._db_lock = threading.RLock()
        self._lifecycle_lock = threading.RLock()
        self._stop = threading.Event()
        self.ready = threading.Event()
        self._last_request = time.monotonic()
        self._last_activity_write = 0.0
        self._owner_fd: int | None = None
        self._children: dict[str, subprocess.Popen[bytes]] = {}
        self._group_drain_started: dict[str, float] = {}
        self._last_repository: str | None = None
        self._serving_thread: int | None = None
        self._closed = False
        self._prepare_paths()
        self._initialize_database()

    def _prepare_paths(self) -> None:
        self.paths.state_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        try:
            details = self.paths.state_dir.stat()
        except OSError as exc:
            raise CoordinatorError(
                f"cannot inspect gate queue directory {self.paths.state_dir}: {exc}"
            ) from exc
        if details.st_uid != os.getuid():
            raise CoordinatorError(
                f"gate queue directory {self.paths.state_dir} belongs to another user"
            )
        if details.st_mode & 0o077:
            self.paths.state_dir.chmod(0o700)
        self.paths.logs.mkdir(exist_ok=True, mode=0o700)
        if self.paths.logs.stat().st_mode & 0o077:
            self.paths.logs.chmod(0o700)
        self.paths.daemon_log.touch(exist_ok=True)
        self.paths.daemon_log.chmod(0o600)

    def _prepare_worker_tmp_paths(self) -> None:
        """Allocate tmpfs namespace only for the process that owns scheduling."""
        for directory in (self.paths.worker_tmp.parent, self.paths.worker_tmp):
            try:
                directory.mkdir(parents=True, exist_ok=True, mode=0o700)
                details = directory.lstat()
            except OSError as exc:
                raise CoordinatorError(
                    f"cannot prepare gate worker temp directory {directory}: {exc}"
                ) from exc
            if stat.S_ISLNK(details.st_mode) or not stat.S_ISDIR(details.st_mode):
                raise CoordinatorError(
                    f"gate worker temp path {directory} is not a real directory"
                )
            if details.st_uid != os.getuid():
                raise CoordinatorError(
                    f"gate worker temp directory {directory} belongs to another user"
                )
            if details.st_mode & 0o077:
                directory.chmod(0o700)

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.paths.database,
            timeout=self.database_timeout,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        return connection

    def _initialize_database(self) -> None:
        with self._db_lock, self._connect() as db:
            # Serialize only first creation. Existing schemas are inspected before any DDL
            # so merely starting a client can never become an implicit migration.
            db.execute("BEGIN IMMEDIATE")
            tables = {
                row["name"]
                for row in db.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table' "
                    "AND name IN ('runs', 'coordinator_meta')"
                )
            }
            if not tables:
                db.execute(
                    """
                    CREATE TABLE runs (
                        sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                        run_id TEXT NOT NULL UNIQUE,
                        status TEXT NOT NULL CHECK (
                            status IN ('queued', 'running', 'passed', 'failed',
                                       'cancelled', 'interrupted')
                        ),
                        kind TEXT NOT NULL CHECK (
                            kind IN ('check', 'full', 'merge', 'land')
                        ),
                        phase TEXT NOT NULL CHECK (
                            phase IN ('queued', 'running', 'preflight', 'gating',
                                      'publishing', 'complete')
                        ),
                        label TEXT NOT NULL,
                        agent TEXT NOT NULL,
                        repository_id TEXT NOT NULL,
                        repository TEXT NOT NULL,
                        worktree_id TEXT NOT NULL,
                        checkout TEXT NOT NULL,
                        branch TEXT NOT NULL,
                        head_sha TEXT,
                        barrier INTEGER NOT NULL CHECK (barrier IN (0, 1)),
                        resources_json TEXT NOT NULL,
                        resource_contract_json TEXT NOT NULL,
                        resource_receipt_json TEXT NOT NULL,
                        resource_state_json TEXT NOT NULL,
                        gate_run_id TEXT,
                        publication_adapter TEXT,
                        publication_request TEXT,
                        failure_reason TEXT,
                        gate_exit_status INTEGER,
                        reported_exit_status INTEGER,
                        caller_pid INTEGER NOT NULL,
                        command_json TEXT NOT NULL,
                        environment_json TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        started_at TEXT,
                        finished_at TEXT,
                        exit_status INTEGER,
                        worker_pid INTEGER,
                        worker_start_token TEXT,
                        cancel_requested INTEGER NOT NULL DEFAULT 0,
                        cancel_requested_at TEXT
                    )
                    """
                )
                db.execute(
                    "CREATE INDEX runs_status_sequence ON runs(status, sequence)"
                )
                db.execute(
                    "CREATE INDEX runs_repository_sequence "
                    "ON runs(repository_id, sequence)"
                )
                db.execute(
                    "CREATE TABLE coordinator_meta ("
                    "key TEXT PRIMARY KEY, value TEXT NOT NULL)"
                )
                db.execute(
                    "INSERT INTO coordinator_meta(key, value) VALUES ('protocol', ?)",
                    (str(PROTOCOL),),
                )
                _create_child_cpu_lease_table(db)
            elif tables != {"runs", "coordinator_meta"}:
                raise CoordinatorError(
                    "gate queue database is partially initialized; it needs explicit "
                    "repair, not startup mutation"
                )
            stored = db.execute(
                "SELECT value FROM coordinator_meta WHERE key = 'protocol'"
            ).fetchone()
            if stored is None:
                raise CoordinatorError(
                    "gate queue database has no protocol value"
                )
            if stored["value"] != str(PROTOCOL):
                raise CoordinatorError(
                    f"gate queue database protocol is {stored['value']}; need {PROTOCOL}; "
                    "after the old broker exits run `agc "
                    f"migrate --state-dir {self.paths.state_dir}`"
                )
            lease_table = db.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' "
                "AND name = 'child_cpu_leases'"
            ).fetchone()
            if lease_table is None:
                raise CoordinatorError(
                    "gate queue database is missing the child CPU lease table"
                )
            required = {
                "kind", "phase", "agent", "repository_id", "repository", "worktree_id",
                "head_sha", "barrier", "resources_json", "gate_run_id",
                "resource_contract_json", "resource_receipt_json", "resource_state_json",
                "publication_adapter", "publication_request", "failure_reason",
                "gate_exit_status", "reported_exit_status",
                "created_at", "finished_at",
            }
            columns = {
                row["name"] for row in db.execute("PRAGMA table_info(runs)")
            }
            missing = sorted(required - columns)
            if missing:
                raise CoordinatorError(
                    "gate queue database is missing current protocol columns: "
                    + ", ".join(missing)
                )
            db.commit()
            _enable_wal(db)
        try:
            self.paths.database.chmod(0o600)
        except OSError as exc:
            raise CoordinatorError(
                f"cannot protect gate queue database {self.paths.database}: {exc}"
            ) from exc

    def _acquire_ownership(self) -> None:
        descriptor = os.open(
            self.paths.owner_lock,
            os.O_RDWR | os.O_CREAT,
            0o600,
        )
        os.fchmod(descriptor, 0o600)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            os.close(descriptor)
            raise CoordinatorError(
                f"another gate broker already owns {self.paths.state_dir}"
            ) from exc
        # Erase bytes left by the previous owner before doing fallible preparation. A
        # concurrent client sees the held flock plus incomplete metadata and waits; it can
        # never mistake the previous PID for this owner.
        os.ftruncate(descriptor, 0)
        self._owner_fd = descriptor
        try:
            self._prepare_worker_tmp_paths()
            self._remove_orphaned_worker_tmp()
            with self._db_lock, self._connect() as db:
                maintenance = _maintenance_record(db)
                if maintenance is not None:
                    live = int(
                        db.execute(
                            "SELECT COUNT(*) FROM runs "
                            "WHERE status IN ('queued', 'running')"
                        ).fetchone()[0]
                    )
                    if live == 0:
                        db.execute(
                            "UPDATE coordinator_meta SET value = 'drained' "
                            "WHERE key = 'maintenance_state'"
                        )
                        raise CoordinatorError(
                            f"coordinator is drained as {maintenance['maintenance_id']}; "
                            "resume it before starting a broker"
                        )
                    if maintenance["maintenance_state"] == "drained":
                        raise CoordinatorError(
                            "coordinator maintenance state is drained but live rows remain"
                        )
            self._touch()
            # Publish readable owner metadata last. Concurrent first clients already see
            # the flock and retry through this bounded preparation interval; none can
            # submit to an owner whose tmpfs sweep later fails.
            os.write(
                descriptor,
                (
                    f"pid={os.getpid()}\n"
                    f"protocol={PROTOCOL}\n"
                    f"capacities={json.dumps(self.capacities, separators=(',', ':'))}\n"
                    f"resource_bindings="
                    f"{json.dumps(self.resource_bindings, separators=(',', ':'))}\n"
                    f"resource_capabilities="
                    f"{json.dumps(self.resource_capabilities, separators=(',', ':'))}\n"
                    f"started_at={_now()}\n"
                ).encode(),
            )
        except BaseException:
            self._release_ownership()
            raise

    def serve_forever(self) -> None:
        """Own the durable inbox and pump until explicitly closed or idle."""
        with self._lifecycle_lock:
            if self._closed:
                raise CoordinatorError("this gate broker is closed")
            if self._serving_thread is not None:
                raise CoordinatorError("this gate broker is already serving")
            self._acquire_ownership()
            self._serving_thread = threading.get_ident()
            self.ready.set()
            self._append_daemon_log(
                f"broker {os.getpid()} started; protocol={PROTOCOL}; "
                f"capacities={self.capacities}; resource backends="
                f"{sorted(self.resource_capabilities)}"
            )
        failure: BaseException | None = None
        try:
            self._pump()
        except BaseException as exc:
            failure = exc
            self._append_daemon_log(
                f"broker failure: {type(exc).__name__}: {exc}; "
                "live workers left for replacement supervision"
            )
            raise
        finally:
            try:
                if failure is None:
                    # An explicit close is a cancellation request and retains ownership
                    # until every safe worker is reaped. An unexpected broker failure must
                    # release ownership without rewriting or signalling live rows, so a
                    # replacement can adopt their durable process identities.
                    self._cancel_active_for_shutdown()
            finally:
                with self._lifecycle_lock:
                    self._closed = True
                    self.ready.clear()
                self._append_daemon_log(f"broker {os.getpid()} stopped")
                self._release_ownership()

    def close(self) -> None:
        """Stop this broker, cancelling safe workers and preserving an active merge."""
        with self._lifecycle_lock:
            if self._closed:
                return
            if self._serving_thread is None:
                self._closed = True
            self._stop.set()
        if self._serving_thread is None:
            self._release_ownership()

    def _release_ownership(self) -> None:
        descriptor = self._owner_fd
        if descriptor is None:
            return
        try:
            self.paths.worker_tmp.rmdir()
            self.paths.worker_tmp.parent.rmdir()
        except OSError:
            # A live run, another repository namespace, or an unclean-recovery artifact
            # keeps the exact owner-only directory in place for the next broker to inspect.
            pass
        try:
            self.paths.legacy_worker_tmp.rmdir()
        except OSError:
            pass
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)
            self._owner_fd = None

    def _cancel_active_for_shutdown(self) -> None:
        """Stop cancellable work, but let an in-flight merge reach an authoritative result."""
        with self._db_lock, self._connect() as db:
            active = db.execute(
                "SELECT * FROM runs WHERE status = 'running' ORDER BY sequence"
            ).fetchall()
            now = _now()
            for row in active:
                # Killing an authenticated ref mutation can lose its response after the
                # forge committed it. Once a merge starts, graceful shutdown preserves the
                # worker and this broker's ownership until its exact result is durable.
                if row["kind"] == "merge" or (
                    row["kind"] == "land" and row["phase"] == "publishing"
                ):
                    continue
                db.execute(
                    "UPDATE runs SET cancel_requested = 1, cancel_requested_at = ? "
                    "WHERE run_id = ?",
                    (now, row["run_id"]),
                )
                self._cancel_resources(db, row)
                self._signal_worker(row, signal.SIGTERM)

        while True:
            with self._db_lock, self._connect() as db:
                active = db.execute(
                    "SELECT * FROM runs WHERE status = 'running' ORDER BY sequence"
                ).fetchall()
                for row in active:
                    self._observe_active(db, row)
                active = db.execute(
                    "SELECT * FROM runs WHERE status = 'running' ORDER BY sequence"
                ).fetchall()
            if not active:
                break
            time.sleep(0.05)

        # _observe_active polls and reaps every child that still has a running row. If an
        # inconsistent terminal-row repair left an owned child behind, graceful shutdown
        # still waits: it must never trade a quicker exit for an unknown merge outcome.
        for run_id, child in list(self._children.items()):
            child.wait()
            self._children.pop(run_id, None)

        with self._db_lock, self._connect() as db:
            self._prune(db)

    def _touch(self) -> None:
        self._last_request = time.monotonic()
        # Client operations happen in other processes, so the serving broker's monotonic
        # field cannot see them. A throttled durable heartbeat keeps an open TUI/follower
        # from losing its broker without turning every 100 ms log poll into a write.
        if self._last_request - self._last_activity_write < 0.5:
            return
        try:
            with self._db_lock, self._connect() as db:
                if _maintenance_record(db) is not None:
                    return
                db.execute(
                    "INSERT INTO coordinator_meta(key, value) VALUES ('last_activity', ?) "
                    "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                    (str(time.time()),),
                )
        except sqlite3.OperationalError as exc:
            if not _transient_database_error(exc):
                raise
            # The public operation already committed before its activity heartbeat. Never
            # turn an accepted submission or successful read into an apparent failure.
            self._append_daemon_log(
                f"activity heartbeat database contention; deferred: {exc}"
            )
            return
        self._last_activity_write = self._last_request

    # --------------------------------------------------------------- public operations

    def submit(
        self,
        command: Sequence[str],
        *,
        checkout: str,
        kind: str = "check",
        label: str = "run",
        resources: Mapping[str, int] | None = None,
        agent: str | None = None,
        repository: str | None = None,
        branch: str | None = None,
        head_sha: str | None = None,
        caller_pid: int | None = None,
        environment: Mapping[str, str] | None = None,
    ) -> str:
        selected_command = _validate_command(command)
        if kind not in RUN_KINDS:
            raise CoordinatorError(
                "kind must be exactly 'check', 'full', 'merge', or 'land'"
            )
        if kind in {"merge", "land"}:
            raise CoordinatorError(
                f"{kind} can only be submitted through submit_{kind}"
            )
        if not isinstance(label, str) or not label.strip():
            raise CoordinatorError("label must be a non-empty string")
        identity = discover_repository(checkout, repository=repository)
        selected_checkout = str(identity.checkout)
        selected_branch = branch.strip() if isinstance(branch, str) else _git_branch(identity.checkout)
        if not selected_branch:
            raise CoordinatorError("branch must be a non-empty string")
        selected_head = _validate_head_sha(head_sha, required=False)
        if kind == "full":
            selected_head = selected_head or _git_head(identity.checkout)
            _assert_clean_head(identity.checkout, selected_head)
        selected_pid = os.getpid() if caller_pid is None else caller_pid
        if (
            not isinstance(selected_pid, int)
            or isinstance(selected_pid, bool)
            or selected_pid <= 0
        ):
            raise CoordinatorError("caller_pid must be a positive integer")
        selected_agent = _agent_identity(agent)
        owner = _broker_owner(self.paths)
        capacities = owner["capacities"] if owner is not None else self.capacities
        selected_resources = _validate_resources(resources, capacities)
        selected_bindings = (
            owner["resource_bindings"] if owner is not None else self.resource_bindings
        )
        selected_contract = resource_contract(selected_resources, selected_bindings)
        selected_resource_receipt = initial_resource_receipt(selected_resources)
        selected_environment = _validate_environment(environment)
        run_id = f"{kind}-{uuid4().hex[:12]}"
        with self._db_lock, self._connect() as db:
            try:
                db.execute(
                    """
                    INSERT INTO runs (
                    run_id, status, kind, phase, label, agent, repository_id, repository,
                    worktree_id, checkout, branch, head_sha, barrier, resources_json,
                    resource_contract_json, resource_receipt_json, resource_state_json,
                    caller_pid, command_json, environment_json, created_at
                ) VALUES (?, 'queued', ?, 'queued', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                          '{}', ?, ?, ?, ?)
                    """,
                    (
                    run_id,
                    kind,
                    label.strip(),
                    selected_agent,
                    identity.repository_id,
                    identity.repository,
                    identity.worktree_id,
                    selected_checkout,
                    selected_branch,
                    selected_head,
                    int(kind == "full"),
                    json.dumps(selected_resources, separators=(",", ":")),
                    json.dumps(selected_contract, separators=(",", ":")),
                    json.dumps(selected_resource_receipt, separators=(",", ":")),
                    selected_pid,
                    json.dumps(selected_command, separators=(",", ":")),
                    json.dumps(selected_environment, separators=(",", ":")),
                    _now(),
                    ),
                )
            except sqlite3.IntegrityError as exc:
                if MAINTENANCE_REFUSAL in str(exc):
                    raise CoordinatorError(
                        "coordinator is draining; new submissions are refused",
                        code="broker-draining",
                    ) from exc
                raise
        self._touch()
        return run_id

    def submit_merge(
        self,
        adapter: str,
        request: object,
        *,
        checkout: str,
        gate_run_id: str | None = None,
        resources: Mapping[str, int] | None = None,
        agent: str | None = None,
        repository: str | None = None,
        branch: str | None = None,
        head_sha: str | None = None,
        caller_pid: int | None = None,
        environment: Mapping[str, str] | None = None,
        worker_python: str | os.PathLike[str] | None = None,
    ) -> str:
        """Queue one merge authorized by a passed full gate for this exact commit."""
        if adapter != "github":
            raise CoordinatorError(f"unknown publication adapter {adapter!r}")
        if not isinstance(request, int) or isinstance(request, bool) or request <= 0:
            raise CoordinatorError("the GitHub publication request must be a positive PR number")
        identity = discover_repository(checkout, repository=repository)
        selected_checkout = str(identity.checkout)
        selected_branch = branch.strip() if isinstance(branch, str) else _git_branch(identity.checkout)
        if not selected_branch:
            raise CoordinatorError("branch must be a non-empty string")
        selected_head = _validate_head_sha(head_sha, required=False) or _git_head(identity.checkout)
        _assert_clean_head(identity.checkout, selected_head)
        selected_pid = os.getpid() if caller_pid is None else caller_pid
        if (
            not isinstance(selected_pid, int)
            or isinstance(selected_pid, bool)
            or selected_pid <= 0
        ):
            raise CoordinatorError("caller_pid must be a positive integer")
        selected_agent = _agent_identity(agent)
        owner = _broker_owner(self.paths)
        capacities = owner["capacities"] if owner is not None else self.capacities
        selected_resources = _validate_resources(resources, capacities)
        selected_bindings = (
            owner["resource_bindings"] if owner is not None else self.resource_bindings
        )
        selected_contract = resource_contract(selected_resources, selected_bindings)
        selected_resource_receipt = initial_resource_receipt(selected_resources)
        selected_environment = _validate_environment(environment)
        executable_path = Path(worker_python or sys.executable).expanduser()
        if not executable_path.is_absolute():
            executable_path = Path.cwd() / executable_path
        executable = str(executable_path.absolute())
        if not Path(executable).is_file():
            raise CoordinatorError(
                f"merge worker Python does not exist: {executable}"
            )
        if gate_run_id is not None and (
            not isinstance(gate_run_id, str) or not gate_run_id
        ):
            raise CoordinatorError("gate_run_id must be a non-empty string")

        run_id = f"merge-{uuid4().hex[:12]}"
        command = [
            executable,
            "-m",
            "agcoord.github",
            "--run-id",
            run_id,
            "--state-dir",
            str(self.paths.state_dir),
            "--checkout",
            selected_checkout,
            "--branch",
            selected_branch,
            "--head-sha",
            selected_head,
            str(request),
        ]
        with self._db_lock, self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            if _maintenance_record(db) is not None:
                raise CoordinatorError(
                    "coordinator is draining; new submissions are refused",
                    code="broker-draining",
                )
            cutoff_row = db.execute(
                "SELECT value FROM coordinator_meta "
                "WHERE key = 'invalid_gate_through_sequence'"
            ).fetchone()
            try:
                invalid_gate_through = (
                    0 if cutoff_row is None else int(cutoff_row["value"])
                )
            except (TypeError, ValueError) as exc:
                raise CoordinatorError(
                    "rollback gate cutoff metadata is invalid"
                ) from exc
            if invalid_gate_through < 0:
                raise CoordinatorError("rollback gate cutoff metadata is invalid")
            if gate_run_id is None:
                receipt = db.execute(
                    """
                    SELECT * FROM runs
                    WHERE kind = 'full' AND status = 'passed'
                      AND repository_id = ? AND branch = ? AND head_sha = ?
                      AND sequence > ?
                    ORDER BY sequence DESC LIMIT 1
                    """,
                    (
                        identity.repository_id,
                        selected_branch,
                        selected_head,
                        invalid_gate_through,
                    ),
                ).fetchone()
                if receipt is None:
                    raise CoordinatorError(
                        "no passed full-gate receipt matches this checkout, branch, "
                        "and head; run the full gate for the exact current commit"
                    )
            else:
                receipt = db.execute(
                    "SELECT * FROM runs WHERE run_id = ?",
                    (gate_run_id,),
                ).fetchone()
                if receipt is None:
                    raise CoordinatorError(
                        f"unknown full-gate receipt {gate_run_id!r}"
                    )
                if receipt["sequence"] <= invalid_gate_through:
                    raise CoordinatorError(
                        f"gate receipt {gate_run_id} is stale after rollback; "
                        "run a new full gate"
                    )
                mismatches: list[str] = []
                if receipt["kind"] != "full" or receipt["status"] != "passed":
                    mismatches.append("a passed full gate")
                if receipt["repository_id"] != identity.repository_id:
                    mismatches.append("repository")
                if receipt["branch"] != selected_branch:
                    mismatches.append("branch")
                if receipt["head_sha"] != selected_head:
                    mismatches.append("head")
                if mismatches:
                    raise CoordinatorError(
                        f"gate receipt {gate_run_id} does not match "
                        + ", ".join(mismatches)
                    )
            selected_gate_receipt = receipt["run_id"]
            db.execute(
                """
                INSERT INTO runs (
                    run_id, status, kind, phase, label, agent, repository_id, repository,
                    worktree_id, checkout, branch, head_sha, barrier, resources_json,
                    resource_contract_json, resource_receipt_json, resource_state_json,
                    gate_run_id, publication_adapter, publication_request, caller_pid,
                    command_json, environment_json, created_at
                ) VALUES (?, 'queued', 'merge', 'queued', ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?,
                          ?, '{}', ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    f"merge GitHub PR #{request}",
                    selected_agent,
                    identity.repository_id,
                    identity.repository,
                    identity.worktree_id,
                    selected_checkout,
                    selected_branch,
                    selected_head,
                    json.dumps(selected_resources, separators=(",", ":")),
                    json.dumps(selected_contract, separators=(",", ":")),
                    json.dumps(selected_resource_receipt, separators=(",", ":")),
                    selected_gate_receipt,
                    adapter,
                    json.dumps(request, separators=(",", ":")),
                    selected_pid,
                    json.dumps(command, separators=(",", ":")),
                    json.dumps(selected_environment, separators=(",", ":")),
                    _now(),
                ),
            )
        self._touch()
        return run_id

    def submit_land(
        self,
        adapter: str,
        request: object,
        command: Sequence[str],
        *,
        checkout: str,
        label: str = "land",
        resources: Mapping[str, int] | None = None,
        agent: str | None = None,
        repository: str | None = None,
        branch: str | None = None,
        head_sha: str | None = None,
        caller_pid: int | None = None,
        environment: Mapping[str, str] | None = None,
        synchronize_target: bool = True,
        avoid_commits: Sequence[str] = (),
    ) -> str:
        """Queue one indivisible exact-head gate and publication."""
        selected_command = _validate_command(command)
        if adapter != "github":
            raise CoordinatorError(f"unknown publication adapter {adapter!r}")
        if not isinstance(request, int) or isinstance(request, bool) or request <= 0:
            raise CoordinatorError(
                "the GitHub publication request must be a positive PR number"
            )
        if not isinstance(label, str) or not label.strip():
            raise CoordinatorError("label must be a non-empty string")
        if not isinstance(synchronize_target, bool):
            raise CoordinatorError("synchronize_target must be boolean")
        identity = discover_repository(checkout, repository=repository)
        selected_checkout = str(identity.checkout)
        selected_branch = (
            branch.strip() if isinstance(branch, str) else _git_branch(identity.checkout)
        )
        if not selected_branch:
            raise CoordinatorError("branch must be a non-empty string")
        selected_head = (
            _validate_head_sha(head_sha, required=False) or _git_head(identity.checkout)
        )
        _assert_clean_head(identity.checkout, selected_head)
        selected_pid = os.getpid() if caller_pid is None else caller_pid
        if (
            not isinstance(selected_pid, int)
            or isinstance(selected_pid, bool)
            or selected_pid <= 0
        ):
            raise CoordinatorError("caller_pid must be a positive integer")
        selected_agent = _agent_identity(agent)
        owner = _broker_owner(self.paths)
        capacities = owner["capacities"] if owner is not None else self.capacities
        selected_resources = _validate_resources(resources, capacities)
        selected_bindings = (
            owner["resource_bindings"] if owner is not None else self.resource_bindings
        )
        selected_contract = resource_contract(selected_resources, selected_bindings)
        selected_receipt = initial_resource_receipt(selected_resources)
        selected_environment = _validate_environment(environment)
        selected_avoid = _validate_avoid_commits(avoid_commits)
        if LAND_TARGET_SYNC_ENV in selected_environment:
            raise CoordinatorError(
                f"gate environment uses the reserved {LAND_TARGET_SYNC_ENV} name"
            )
        selected_environment[LAND_TARGET_SYNC_ENV] = (
            "1" if synchronize_target else "0"
        )
        if LAND_AVOID_ENV in selected_environment:
            raise CoordinatorError(
                f"gate environment uses the reserved {LAND_AVOID_ENV} name"
            )
        if selected_avoid:
            selected_environment[LAND_AVOID_ENV] = ",".join(selected_avoid)
        run_id = f"land-{uuid4().hex[:12]}"
        with self._db_lock, self._connect() as db:
            try:
                db.execute(
                    """
                    INSERT INTO runs (
                    run_id, status, kind, phase, label, agent, repository_id,
                    repository, worktree_id, checkout, branch, head_sha, barrier,
                    resources_json, resource_contract_json, resource_receipt_json,
                    resource_state_json, publication_adapter, publication_request,
                    caller_pid, command_json, environment_json, created_at
                ) VALUES (?, 'queued', 'land', 'queued', ?, ?, ?, ?, ?, ?, ?, ?, 1,
                          ?, ?, ?, '{}', ?, ?, ?, ?, ?, ?)
                    """,
                    (
                    run_id,
                    label.strip(),
                    selected_agent,
                    identity.repository_id,
                    identity.repository,
                    identity.worktree_id,
                    selected_checkout,
                    selected_branch,
                    selected_head,
                    json.dumps(selected_resources, separators=(",", ":")),
                    json.dumps(selected_contract, separators=(",", ":")),
                    json.dumps(selected_receipt, separators=(",", ":")),
                    adapter,
                    json.dumps(request, separators=(",", ":")),
                    selected_pid,
                    json.dumps(selected_command, separators=(",", ":")),
                    json.dumps(selected_environment, separators=(",", ":")),
                    _now(),
                    ),
                )
            except sqlite3.IntegrityError as exc:
                if MAINTENANCE_REFUSAL in str(exc):
                    raise CoordinatorError(
                        "coordinator is draining; new submissions are refused",
                        code="broker-draining",
                    ) from exc
                raise
        self._touch()
        return run_id

    def snapshot(self) -> dict[str, Any]:
        owner = _broker_owner(self.paths)
        with self._db_lock, self._connect() as db:
            maintenance = _maintenance_record(db)
            if owner is None and maintenance is None:
                raise CoordinatorError(
                    f"no gate broker owns {self.paths.state_dir}"
                )
            rows = db.execute("SELECT * FROM runs ORDER BY sequence").fetchall()
        queued_rows = [row for row in rows if row["status"] == "queued"]
        active_rows = [row for row in rows if row["status"] == "running"]
        recent_rows = [row for row in rows if row["status"] in TERMINAL_STATUSES]
        recent_rows = list(reversed(recent_rows[-self.recent_limit:]))
        capacities = owner["capacities"] if owner is not None else self.capacities
        bindings = (
            owner["resource_bindings"]
            if owner is not None
            else self.resource_bindings
        )
        capabilities = (
            owner["resource_capabilities"]
            if owner is not None
            else {}
        )
        used = self._allocations(active_rows)
        allocations = {
            name: used.get(name, 0) for name in capacities
        }
        queued = [
            self._public(
                row,
                position=index,
                active=active_rows,
                queued=queued_rows,
                capacities=capacities,
            )
            for index, row in enumerate(queued_rows, start=1)
        ]
        self._touch()
        return {
            "protocol": PROTOCOL,
            "broker_pid": None if owner is None else owner["pid"],
            "captured_at": _now(),
            "capacities": capacities,
            "allocations": allocations,
            "resource_bindings": bindings,
            "resource_capabilities": capabilities,
            "maintenance": (
                None
                if maintenance is None
                else _maintenance_public(
                    maintenance,
                    protocol=PROTOCOL,
                    live=len(active_rows) + len(queued_rows),
                    owner=owner,
                )
            ),
            "active": [self._public(row, position=None) for row in active_rows],
            "queued": queued,
            "recent": [self._public(row, position=None) for row in recent_rows],
        }

    def status(self, run_id: str) -> dict[str, Any]:
        if not isinstance(run_id, str) or not run_id:
            raise CoordinatorError("run_id must be a non-empty string")
        position: int | None = None
        active: list[sqlite3.Row] = []
        queued: list[sqlite3.Row] = []
        capacities: Mapping[str, int] | None = None
        with self._db_lock, self._connect() as db:
            db.execute("BEGIN")
            row = db.execute(
                "SELECT * FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            if row is None:
                raise CoordinatorError(f"unknown run {run_id!r}")
            if row["status"] == "queued":
                active = db.execute(
                    "SELECT * FROM runs WHERE status = 'running' ORDER BY sequence"
                ).fetchall()
                queued = db.execute(
                    "SELECT * FROM runs WHERE status = 'queued' ORDER BY sequence"
                ).fetchall()
                position = next(
                    index for index, candidate in enumerate(queued, start=1)
                    if candidate["run_id"] == run_id
                )
        if row["status"] == "queued":
            owner = _broker_owner(self.paths)
            capacities = owner["capacities"] if owner is not None else self.capacities
        self._touch()
        return self._public(
            row,
            position=position,
            active=active,
            queued=queued,
            capacities=capacities,
        )

    def cancel(self, run_id: str) -> dict[str, Any]:
        with self._db_lock, self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT * FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            if row is None:
                raise CoordinatorError(f"unknown gate run {run_id!r}")
            if row["status"] in TERMINAL_STATUSES:
                raise CoordinatorError(
                    f"gate run {run_id} is already {row['status']}"
                )
            if row["status"] == "running" and (
                row["kind"] == "merge"
                or (row["kind"] == "land" and row["phase"] == "publishing")
            ):
                raise CoordinatorError(
                    f"publication job {run_id} is already publishing and cannot be cancelled; "
                    "wait for its authoritative result"
                )
            now = _now()
            if row["status"] == "queued":
                db.execute(
                    "UPDATE runs SET status = 'cancelled', phase = 'complete', finished_at = ?, "
                    "exit_status = 130, cancel_requested = 1, "
                    "cancel_requested_at = ?, environment_json = '{}' WHERE run_id = ?",
                    (now, now, run_id),
                )
                self._prune(db)
            else:
                db.execute(
                    "UPDATE runs SET cancel_requested = 1, cancel_requested_at = ? "
                    "WHERE run_id = ?",
                    (now, run_id),
                )
                # A client may live in a different PID namespace from the detached owner.
                # The durable request is the operation; only the broker pump supervises
                # and signals the recorded worker process group.
        self._touch()
        return self.status(run_id)

    def clear(self) -> dict[str, int]:
        """Remove terminal history and logs without deleting live state or ownership."""
        with self._db_lock, self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            if _maintenance_record(db) is not None:
                raise CoordinatorError(
                    "cannot clear history while the coordinator is draining"
                )
            live = db.execute(
                "SELECT run_id FROM runs WHERE status IN ('queued', 'running') "
                "ORDER BY sequence"
            ).fetchall()
            if live:
                raise CoordinatorError(
                    "cannot clear history while work is queued or running: "
                    + ", ".join(row["run_id"] for row in live)
                )
            rows = db.execute(
                "SELECT run_id FROM runs WHERE status IN "
                "('passed', 'failed', 'cancelled', 'interrupted')"
            ).fetchall()
            for row in rows:
                run_id = row["run_id"]
                if not self._remove_worker_tmp(run_id):
                    raise CoordinatorError(f"cannot clear unreclaimed temp state for {run_id}")
                self._log_path(run_id).unlink(missing_ok=True)
            db.execute(
                "DELETE FROM runs WHERE status IN "
                "('passed', 'failed', 'cancelled', 'interrupted')"
            )
        self._touch()
        return {"cleared": len(rows)}

    def _admitted_child_run(
        self,
        db: sqlite3.Connection,
        run_id: str,
        *,
        owner_pid: int,
        owner_start_token: str,
    ) -> sqlite3.Row:
        row = db.execute(
            "SELECT * FROM runs WHERE run_id = ?", (run_id,)
        ).fetchone()
        if row is None:
            raise CoordinatorError(f"unknown parent run {run_id!r}")
        if row["status"] != "running" or row["cancel_requested"]:
            raise CoordinatorError(
                f"parent run {run_id} is not accepting child CPU leases"
            )
        worker_pid = row["worker_pid"]
        worker_token = row["worker_start_token"]
        if not _same_process(worker_pid, worker_token):
            raise CoordinatorError(
                f"parent run {run_id} has no live worker identity"
            )
        if not _is_descendant_process(
            owner_pid,
            owner_start_token,
            ancestor_pid=worker_pid,
            ancestor_token=worker_token,
        ):
            raise CoordinatorError(
                f"caller is not a live descendant of admitted run {run_id}"
            )
        return row

    def request_child_cpu_lease(
        self,
        run_id: str,
        *,
        requested: int,
        minimum: int,
    ) -> dict[str, Any]:
        """Create one authenticated FIFO request within a running parent's CPU budget."""
        if not isinstance(run_id, str) or not run_id:
            raise CoordinatorError("child CPU lease parent run ID must be non-empty")
        if (
            not isinstance(requested, int)
            or isinstance(requested, bool)
            or requested <= 0
        ):
            raise CoordinatorError("child CPU lease request must be a positive integer")
        if (
            not isinstance(minimum, int)
            or isinstance(minimum, bool)
            or minimum <= 0
            or minimum > requested
        ):
            raise CoordinatorError(
                "child CPU lease minimum must be positive and no greater than requested"
            )
        if _broker_owner(self.paths) is None:
            raise CoordinatorError(
                f"no gate broker owns {self.paths.state_dir}"
            )
        owner_pid = os.getpid()
        owner_start_token = _process_start_token(owner_pid)
        if owner_start_token is None:
            raise CoordinatorError("cannot identify child CPU lease owner process")
        lease_id = f"cpu-lease-{uuid4().hex[:12]}"
        with self._db_lock, self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            parent = self._admitted_child_run(
                db,
                run_id,
                owner_pid=owner_pid,
                owner_start_token=owner_start_token,
            )
            budget = self._row_resources(parent).get(CHILD_CPU_RESOURCE)
            if budget is None:
                raise CoordinatorError(
                    f"parent run {run_id} has no {CHILD_CPU_RESOURCE!r} resource budget"
                )
            if minimum > budget:
                raise CoordinatorError(
                    f"child CPU lease minimum {minimum} exceeds parent budget {budget}"
                )
            db.execute(
                "INSERT INTO child_cpu_leases ("
                "lease_id, run_id, status, requested, minimum, owner_pid, "
                "owner_start_token, created_at"
                ") VALUES (?, ?, 'waiting', ?, ?, ?, ?, ?)",
                (
                    lease_id,
                    run_id,
                    requested,
                    minimum,
                    owner_pid,
                    owner_start_token,
                    _now(),
                ),
            )
            self._maintain_child_cpu_leases(db)
            row = db.execute(
                "SELECT * FROM child_cpu_leases WHERE lease_id = ?", (lease_id,)
            ).fetchone()
        self._touch()
        return self._public_child_cpu_lease(row, position=self._lease_position(row))

    def child_cpu_lease_status(self, lease_id: str) -> dict[str, Any]:
        if not isinstance(lease_id, str) or not lease_id:
            raise CoordinatorError("child CPU lease ID must be non-empty")
        with self._db_lock, self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            self._maintain_child_cpu_leases(db)
            row = db.execute(
                "SELECT * FROM child_cpu_leases WHERE lease_id = ?", (lease_id,)
            ).fetchone()
            if row is None:
                raise CoordinatorError(f"unknown child CPU lease {lease_id!r}")
            position = self._lease_position(row, db=db)
        self._touch()
        return self._public_child_cpu_lease(row, position=position)

    def child_cpu_leases(
        self,
        run_id: str | None = None,
        *,
        include_terminal: bool = False,
    ) -> list[dict[str, Any]]:
        if run_id is not None and (not isinstance(run_id, str) or not run_id):
            raise CoordinatorError("child CPU lease parent run ID must be non-empty")
        with self._db_lock, self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            self._maintain_child_cpu_leases(db)
            if run_id is not None:
                parent = db.execute(
                    "SELECT run_id FROM runs WHERE run_id = ?", (run_id,)
                ).fetchone()
                if parent is None:
                    raise CoordinatorError(f"unknown parent run {run_id!r}")
            conditions: list[str] = []
            values: list[object] = []
            if run_id is not None:
                conditions.append("run_id = ?")
                values.append(run_id)
            if not include_terminal:
                conditions.append("status IN ('waiting', 'active')")
            where = " WHERE " + " AND ".join(conditions) if conditions else ""
            rows = db.execute(
                "SELECT * FROM child_cpu_leases" + where + " ORDER BY sequence",
                values,
            ).fetchall()
            waiting_positions: dict[str, int] = {}
            public: list[dict[str, Any]] = []
            for row in rows:
                position = None
                if row["status"] == "waiting":
                    waiting_positions[row["run_id"]] = (
                        waiting_positions.get(row["run_id"], 0) + 1
                    )
                    position = waiting_positions[row["run_id"]]
                public.append(self._public_child_cpu_lease(row, position=position))
        self._touch()
        return public

    def _finish_child_cpu_lease(self, lease_id: str, *, status: str) -> dict[str, Any]:
        if status not in {"released", "cancelled"}:
            raise ValueError("child CPU lease terminal status is invalid")
        if not isinstance(lease_id, str) or not lease_id:
            raise CoordinatorError("child CPU lease ID must be non-empty")
        owner_pid = os.getpid()
        owner_start_token = _process_start_token(owner_pid)
        if owner_start_token is None:
            raise CoordinatorError("cannot identify child CPU lease owner process")
        with self._db_lock, self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT * FROM child_cpu_leases WHERE lease_id = ?", (lease_id,)
            ).fetchone()
            if row is None:
                raise CoordinatorError(f"unknown child CPU lease {lease_id!r}")
            if row["owner_pid"] != owner_pid or row["owner_start_token"] != owner_start_token:
                raise CoordinatorError(
                    f"caller does not own child CPU lease {lease_id}"
                )
            if row["status"] in {"waiting", "active"}:
                db.execute(
                    "UPDATE child_cpu_leases SET status = ?, finished_at = ? "
                    "WHERE lease_id = ?",
                    (status, _now(), lease_id),
                )
                self._maintain_child_cpu_leases(db)
            row = db.execute(
                "SELECT * FROM child_cpu_leases WHERE lease_id = ?", (lease_id,)
            ).fetchone()
        self._touch()
        return self._public_child_cpu_lease(row, position=None)

    def release_child_cpu_lease(self, lease_id: str) -> dict[str, Any]:
        return self._finish_child_cpu_lease(lease_id, status="released")

    def cancel_child_cpu_lease(self, lease_id: str) -> dict[str, Any]:
        return self._finish_child_cpu_lease(lease_id, status="cancelled")

    def _lease_position(
        self,
        row: sqlite3.Row,
        *,
        db: sqlite3.Connection | None = None,
    ) -> int | None:
        if row["status"] != "waiting":
            return None
        if db is None:
            with self._db_lock, self._connect() as selected:
                return selected.execute(
                    "SELECT COUNT(*) FROM child_cpu_leases "
                    "WHERE run_id = ? AND status = 'waiting' AND sequence <= ?",
                    (row["run_id"], row["sequence"]),
                ).fetchone()[0]
        return db.execute(
            "SELECT COUNT(*) FROM child_cpu_leases "
            "WHERE run_id = ? AND status = 'waiting' AND sequence <= ?",
            (row["run_id"], row["sequence"]),
        ).fetchone()[0]

    def _public_child_cpu_lease(
        self,
        row: sqlite3.Row,
        *,
        position: int | None,
    ) -> dict[str, Any]:
        granted = row["granted"]
        return {
            "lease_id": row["lease_id"],
            "run_id": row["run_id"],
            "status": row["status"],
            "requested": row["requested"],
            "minimum": row["minimum"],
            "granted": granted,
            "full": granted > 0 and granted == row["requested"],
            "owner_pid": row["owner_pid"],
            "created_at": row["created_at"],
            "acquired_at": row["acquired_at"],
            "finished_at": row["finished_at"],
            "position": position,
        }

    def _maintain_child_cpu_leases(self, db: sqlite3.Connection) -> None:
        """Reclaim dead owners and fairly admit durable requests per parent run."""
        now = _now()
        live = db.execute(
            "SELECT leases.*, runs.status AS run_status, "
            "runs.cancel_requested AS run_cancel_requested, "
            "runs.worker_pid AS run_worker_pid, "
            "runs.worker_start_token AS run_worker_start_token "
            "FROM child_cpu_leases AS leases JOIN runs USING (run_id) "
            "WHERE leases.status IN ('waiting', 'active') ORDER BY leases.sequence"
        ).fetchall()
        for lease in live:
            valid = (
                lease["run_status"] == "running"
                and not lease["run_cancel_requested"]
                and _same_process(
                    lease["run_worker_pid"], lease["run_worker_start_token"]
                )
                and _same_process(lease["owner_pid"], lease["owner_start_token"])
                and _is_descendant_process(
                    lease["owner_pid"],
                    lease["owner_start_token"],
                    ancestor_pid=lease["run_worker_pid"],
                    ancestor_token=lease["run_worker_start_token"],
                )
            )
            if not valid:
                db.execute(
                    "UPDATE child_cpu_leases SET status = 'cancelled', finished_at = ? "
                    "WHERE lease_id = ? AND status IN ('waiting', 'active')",
                    (now, lease["lease_id"]),
                )

        run_ids = [
            row["run_id"]
            for row in db.execute(
                "SELECT DISTINCT run_id FROM child_cpu_leases "
                "WHERE status IN ('waiting', 'active') ORDER BY run_id"
            )
        ]
        for run_id in run_ids:
            parent = db.execute(
                "SELECT * FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            if parent is None or parent["status"] != "running" or parent["cancel_requested"]:
                continue
            budget = self._row_resources(parent).get(CHILD_CPU_RESOURCE)
            if budget is None:
                raise CoordinatorError(
                    f"parent run {run_id} has live child leases without a CPU budget"
                )
            active = db.execute(
                "SELECT * FROM child_cpu_leases "
                "WHERE run_id = ? AND status = 'active' ORDER BY sequence",
                (run_id,),
            ).fetchall()
            used = sum(row["granted"] for row in active)
            if used > budget:
                raise CoordinatorError(
                    f"child CPU leases for {run_id} hold {used}, above parent budget {budget}"
                )
            available = budget - used
            waiting = list(db.execute(
                "SELECT * FROM child_cpu_leases "
                "WHERE run_id = ? AND status = 'waiting' ORDER BY sequence",
                (run_id,),
            ).fetchall())
            while available > 0 and waiting:
                oldest = waiting[0]
                selected = oldest if oldest["minimum"] <= available else None
                if selected is None:
                    if oldest["bypass_count"] >= CHILD_LEASE_MAX_BYPASSES:
                        break
                    selected = next(
                        (candidate for candidate in waiting[1:] if candidate["minimum"] <= available),
                        None,
                    )
                    if selected is None:
                        break
                    db.execute(
                        "UPDATE child_cpu_leases SET bypass_count = bypass_count + 1 "
                        "WHERE lease_id = ?",
                        (oldest["lease_id"],),
                    )
                granted = min(selected["requested"], available)
                if granted < selected["minimum"]:
                    raise CoordinatorError("child CPU lease scheduler selected an impossible grant")
                db.execute(
                    "UPDATE child_cpu_leases SET status = 'active', granted = ?, "
                    "acquired_at = ? WHERE lease_id = ? AND status = 'waiting'",
                    (granted, now, selected["lease_id"]),
                )
                available -= granted
                waiting = [
                    candidate for candidate in waiting
                    if candidate["lease_id"] != selected["lease_id"]
                ]

    def verify_admission(
        self,
        run_id: str,
        *,
        kind: str,
        checkout: str,
        head_sha: str,
        worker_pid: int,
    ) -> None:
        """Prove that this exact barrier worker owns one durable admission."""
        if not isinstance(run_id, str) or not run_id:
            raise CoordinatorError("broker admission run ID must be non-empty")
        if kind not in {"full", "merge", "land"}:
            raise CoordinatorError(
                "broker admission kind must be 'full', 'merge', or 'land'"
            )
        selected_checkout = str(_absolute(checkout))
        selected_head = _validate_head_sha(head_sha, required=True)
        if (
            not isinstance(worker_pid, int)
            or isinstance(worker_pid, bool)
            or worker_pid <= 0
        ):
            raise CoordinatorError("broker admission worker PID must be positive")
        owner = _broker_owner(self.paths)
        if owner is None:
            raise CoordinatorError(
                f"run {run_id!r} has no live broker admission"
            )
        with self._db_lock, self._connect() as db:
            row = db.execute(
                "SELECT * FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
        mismatches: list[str] = []
        if row is None:
            mismatches.append("run ID is not in the durable queue")
        else:
            if row["status"] != "running":
                mismatches.append(f"status is {row['status']!r}, not 'running'")
            if not row["barrier"]:
                mismatches.append("run is not a repository barrier")
            if row["kind"] != kind:
                mismatches.append(f"kind is {row['kind']!r}, not {kind!r}")
            if row["checkout"] != selected_checkout:
                mismatches.append("checkout does not match")
            if row["head_sha"] != selected_head:
                mismatches.append("head does not match")
            if row["worker_pid"] != worker_pid:
                mismatches.append("worker PID does not match")
            elif not _same_process(worker_pid, row["worker_start_token"]):
                mismatches.append("worker process identity is no longer live")
        if mismatches:
            raise CoordinatorError(
                f"run {run_id!r} has no exact broker admission: "
                + "; ".join(mismatches)
            )
        self._touch()

    def update_land_phase(
        self,
        run_id: str,
        *,
        phase: str,
        gate_exit_status: int | None,
        worker_pid: int,
        new_head_sha: str | None = None,
    ) -> None:
        """Advance one admitted land worker and establish its cancellation boundary."""
        if phase not in {"preflight", "gating", "publishing"}:
            raise CoordinatorError(f"invalid land phase {phase!r}")
        if gate_exit_status is not None and (
            not isinstance(gate_exit_status, int)
            or isinstance(gate_exit_status, bool)
            or not 0 <= gate_exit_status <= 255
        ):
            raise CoordinatorError("gate exit status must be null or an integer from 0 to 255")
        if not isinstance(worker_pid, int) or isinstance(worker_pid, bool) or worker_pid <= 0:
            raise CoordinatorError("land worker PID must be positive")
        selected_new_head = (
            _validate_head_sha(new_head_sha, required=True)
            if new_head_sha is not None
            else None
        )
        order = {"preflight": 0, "gating": 1, "publishing": 2}
        with self._db_lock, self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT * FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            mismatches: list[str] = []
            if row is None:
                mismatches.append("run ID is not in the durable queue")
            else:
                if row["kind"] != "land":
                    mismatches.append(f"kind is {row['kind']!r}, not 'land'")
                if row["status"] != "running":
                    mismatches.append(f"status is {row['status']!r}, not 'running'")
                if row["worker_pid"] != worker_pid:
                    mismatches.append("worker PID does not match")
                elif not _same_process(worker_pid, row["worker_start_token"]):
                    mismatches.append("worker process identity is no longer live")
                current_phase = row["phase"]
                if current_phase not in order:
                    mismatches.append(f"phase is {current_phase!r}, not a live land phase")
                elif order[phase] < order[current_phase]:
                    mismatches.append(
                        f"phase cannot move backward from {current_phase!r} to {phase!r}"
                    )
                stored_gate_status = row["gate_exit_status"]
                if (
                    stored_gate_status is not None
                    and gate_exit_status is not None
                    and stored_gate_status != gate_exit_status
                ):
                    mismatches.append("gate exit status cannot change")
                selected_gate_status = (
                    stored_gate_status if gate_exit_status is None else gate_exit_status
                )
                if phase == "preflight" and selected_gate_status is not None:
                    mismatches.append("preflight cannot have a gate exit status")
                if phase == "publishing" and selected_gate_status != 0:
                    mismatches.append("publication requires a passed gate")
                if selected_new_head is not None and (
                    phase != "preflight"
                    or current_phase != "preflight"
                    or selected_new_head == row["head_sha"]
                ):
                    mismatches.append(
                        "the durable head can change only during preflight and must differ"
                    )
                if row["cancel_requested"]:
                    raise CoordinatorError(
                        f"land job {run_id} has a cancellation request; publication refused"
                    )
            if mismatches:
                raise CoordinatorError(
                    f"run {run_id!r} cannot advance land phase: " + "; ".join(mismatches)
                )
            db.execute(
                "UPDATE runs SET phase = ?, gate_exit_status = ?, head_sha = ? "
                "WHERE run_id = ?",
                (
                    phase,
                    selected_gate_status,
                    selected_new_head or row["head_sha"],
                    run_id,
                ),
            )
        self._touch()

    def report_land_result(
        self,
        run_id: str,
        *,
        exit_status: int,
        worker_pid: int,
    ) -> None:
        """Durably report a land result without exposing terminal state early."""
        if (
            not isinstance(exit_status, int)
            or isinstance(exit_status, bool)
            or not 0 <= exit_status <= 255
        ):
            raise CoordinatorError("land result must be an integer exit status from 0 to 255")
        if not isinstance(worker_pid, int) or isinstance(worker_pid, bool) or worker_pid <= 0:
            raise CoordinatorError("land worker PID must be positive")
        with self._db_lock, self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT * FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            mismatches: list[str] = []
            if row is None:
                mismatches.append("run ID is not in the durable queue")
            else:
                if row["kind"] != "land":
                    mismatches.append(f"kind is {row['kind']!r}, not 'land'")
                if row["status"] != "running":
                    mismatches.append(f"status is {row['status']!r}, not 'running'")
                if row["worker_pid"] != worker_pid:
                    mismatches.append("worker PID does not match")
                elif not _same_process(worker_pid, row["worker_start_token"]):
                    mismatches.append("worker process identity is no longer live")
                if row["reported_exit_status"] is not None:
                    mismatches.append("land result was already reported")
                if row["phase"] == "gating" and row["gate_exit_status"] is None:
                    mismatches.append("gating result has no gate exit status")
                if row["phase"] == "publishing" and row["gate_exit_status"] != 0:
                    mismatches.append("publication result has no passed gate")
            if mismatches:
                raise CoordinatorError(
                    f"run {run_id!r} cannot report land result: " + "; ".join(mismatches)
                )
            db.execute(
                "UPDATE runs SET reported_exit_status = ? WHERE run_id = ?",
                (exit_status, run_id),
            )
        self._touch()

    def log(
        self,
        run_id: str,
        *,
        offset: int = 0,
        limit: int = MAX_LOG_BYTES,
    ) -> dict[str, Any]:
        self._one(run_id)
        if not isinstance(offset, int) or isinstance(offset, bool) or offset < 0:
            raise CoordinatorError("gate log offset must be a non-negative integer")
        if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= MAX_LOG_BYTES:
            raise CoordinatorError(
                f"gate log limit must be between 1 and {MAX_LOG_BYTES}"
            )
        path = self._log_path(run_id)
        if not path.exists():
            data = b""
            size = 0
        else:
            size = path.stat().st_size
            with path.open("rb") as stream:
                stream.seek(offset)
                data = stream.read(limit)
        if offset > size:
            raise CoordinatorError(
                f"gate log offset {offset} is past its {size}-byte end"
            )
        next_offset = offset + len(data)
        self._touch()
        return {
            "run_id": run_id,
            "offset": offset,
            "next_offset": next_offset,
            "text": data.decode("utf-8", errors="replace"),
            "eof": next_offset >= size,
        }

    def _one(self, run_id: str) -> sqlite3.Row:
        if not isinstance(run_id, str) or not run_id:
            raise CoordinatorError("gate run_id must be a non-empty string")
        with self._db_lock, self._connect() as db:
            row = db.execute(
                "SELECT * FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
        if row is None:
            raise CoordinatorError(f"unknown gate run {run_id!r}")
        return row

    def _row_resources(self, row: sqlite3.Row) -> dict[str, int]:
        try:
            raw = json.loads(row["resources_json"])
            return _positive_mapping(raw, subject="stored resource", include_job=False)
        except (TypeError, json.JSONDecodeError, CoordinatorError) as exc:
            raise CoordinatorError(
                f"run {row['run_id']} has invalid stored resources"
            ) from exc

    def _row_resource_contract(self, row: sqlite3.Row) -> dict[str, dict[str, object]]:
        resources = self._row_resources(row)
        try:
            return validate_resource_contract(
                json.loads(row["resource_contract_json"]),
                resources,
            )
        except (TypeError, json.JSONDecodeError, ResourceContractError) as exc:
            raise CoordinatorError(
                f"run {row['run_id']} has an invalid stored resource contract"
            ) from exc

    def _row_resource_receipt(self, row: sqlite3.Row) -> dict[str, object]:
        resources = self._row_resources(row)
        try:
            return validate_resource_receipt(
                json.loads(row["resource_receipt_json"]),
                resources,
            )
        except (TypeError, json.JSONDecodeError, ResourceContractError) as exc:
            raise CoordinatorError(
                f"run {row['run_id']} has an invalid stored resource receipt"
            ) from exc

    def _row_resource_state(self, row: sqlite3.Row) -> dict[str, dict[str, object]]:
        try:
            return validate_backend_state(json.loads(row["resource_state_json"]))
        except (TypeError, json.JSONDecodeError, ResourceContractError) as exc:
            raise CoordinatorError(
                f"run {row['run_id']} has invalid private resource state"
            ) from exc

    def _allocations(self, rows: Sequence[sqlite3.Row]) -> dict[str, int]:
        allocated: dict[str, int] = {}
        for row in rows:
            for name, units in self._row_resources(row).items():
                allocated[name] = allocated.get(name, 0) + units
        return dict(sorted(allocated.items()))

    def _blocked_by(
        self,
        row: sqlite3.Row,
        *,
        active: Sequence[sqlite3.Row],
        queued: Sequence[sqlite3.Row],
        capacities: Mapping[str, int],
    ) -> list[str]:
        if row["status"] != "queued":
            return []
        reasons: list[str] = []
        same_active = [
            candidate for candidate in active
            if candidate["repository_id"] == row["repository_id"]
        ]
        earlier = [
            candidate for candidate in queued
            if candidate["repository_id"] == row["repository_id"]
            and candidate["sequence"] < row["sequence"]
        ]
        if row["barrier"]:
            reasons.extend(
                f"repository:{row['repository_id']}:active:{candidate['run_id']}"
                for candidate in same_active
            )
            if earlier:
                reasons.append(
                    f"repository:{row['repository_id']}:fifo:{earlier[0]['run_id']}"
                )
        else:
            barriers = [candidate for candidate in [*same_active, *earlier] if candidate["barrier"]]
            if barriers:
                reasons.append(
                    f"repository:{row['repository_id']}:barrier:{barriers[0]['run_id']}"
                )
        allocated = self._allocations(active)
        for name, units in self._row_resources(row).items():
            if allocated.get(name, 0) + units > capacities.get(name, 0):
                reasons.append(f"resource:{name}")
        return reasons

    def _public(
        self,
        row: sqlite3.Row,
        *,
        position: int | None,
        active: Sequence[sqlite3.Row] = (),
        queued: Sequence[sqlite3.Row] = (),
        capacities: Mapping[str, int] | None = None,
    ) -> dict[str, Any]:
        path = self._log_path(row["run_id"])
        try:
            log_bytes = path.stat().st_size
        except FileNotFoundError:
            log_bytes = 0
        command = json.loads(row["command_json"])
        if not isinstance(command, list) or not all(isinstance(v, str) for v in command):
            raise CoordinatorError(
                f"gate run {row['run_id']} has an invalid stored command"
            )
        publication = None
        if row["publication_adapter"] is not None:
            try:
                request = json.loads(row["publication_request"])
            except json.JSONDecodeError as exc:
                raise CoordinatorError(
                    f"run {row['run_id']} has an invalid publication request"
                ) from exc
            publication = {
                "adapter": row["publication_adapter"],
                "request": request,
            }
        selected_capacities = capacities or self.capacities
        return {
            "run_id": row["run_id"],
            "sequence": row["sequence"],
            "status": row["status"],
            "kind": row["kind"],
            "phase": row["phase"],
            "label": row["label"],
            "agent": row["agent"],
            "repository_id": row["repository_id"],
            "repository": row["repository"],
            "worktree_id": row["worktree_id"],
            "checkout": row["checkout"],
            "branch": row["branch"],
            "head_sha": row["head_sha"],
            "barrier": bool(row["barrier"]),
            "resources": self._row_resources(row),
            "resource_contract": self._row_resource_contract(row),
            "resource_receipt": self._row_resource_receipt(row),
            "blocked_by": self._blocked_by(
                row,
                active=active,
                queued=queued,
                capacities=selected_capacities,
            ),
            "gate_run_id": row["gate_run_id"],
            "publication": publication,
            "failure_reason": row["failure_reason"],
            "gate_exit_status": row["gate_exit_status"],
            "caller_pid": row["caller_pid"],
            "command": command,
            "created_at": row["created_at"],
            "started_at": row["started_at"],
            "finished_at": row["finished_at"],
            "exit_status": row["exit_status"],
            "worker_pid": row["worker_pid"],
            "cancel_requested": bool(row["cancel_requested"]),
            "log_bytes": log_bytes,
            "position": position,
        }

    def _log_path(self, run_id: str) -> Path:
        return self.paths.logs / f"{run_id}.log"

    def _worker_tmp_path(self, run_id: str) -> Path:
        return self.paths.worker_tmp / run_id

    def _worker_tmp_paths(self, run_id: str) -> tuple[Path, Path]:
        if Path(run_id).name != run_id:
            raise CoordinatorError(f"invalid gate run id for temp cleanup: {run_id!r}")
        return (
            self._worker_tmp_path(run_id),
            self.paths.legacy_worker_tmp / run_id,
        )

    def _make_owned_tree_deletable(self, root: Path) -> None:
        pending = [root]
        while pending:
            current = pending.pop()
            try:
                if current.is_symlink():
                    continue
                current.chmod(0o700)
                with os.scandir(current) as entries:
                    for entry in entries:
                        if entry.is_dir(follow_symlinks=False):
                            pending.append(Path(entry.path))
            except FileNotFoundError:
                continue

    def _remove_worker_tmp(self, run_id: str) -> bool:
        removed = True
        for path in self._worker_tmp_paths(run_id):
            try:
                if path.is_symlink():
                    path.unlink()
                else:
                    self._make_owned_tree_deletable(path)
                    shutil.rmtree(path)
            except FileNotFoundError:
                continue
            except OSError as exc:
                removed = False
                self._append_daemon_log(
                    f"could not remove worker temp for {run_id}: "
                    f"{type(exc).__name__}: {exc}"
                )
        return removed

    def _remove_orphaned_worker_tmp(self) -> None:
        with self._db_lock, self._connect() as db:
            live = {
                row["run_id"]
                for row in db.execute(
                    "SELECT run_id FROM runs WHERE status = 'running'"
                )
            }
        roots = (self.paths.worker_tmp, self.paths.legacy_worker_tmp)
        for root in roots:
            if not root.is_dir():
                continue
            for path in root.iterdir():
                if path.name not in live and not self._remove_worker_tmp(path.name):
                    raise CoordinatorError(
                        f"cannot reclaim orphaned gate temp directory {path}"
                    )

    def _drain_finished_process_group(self, row: sqlite3.Row) -> bool:
        run_id = row["run_id"]
        process_group = row["worker_pid"]
        if not _process_group_exists(process_group):
            self._group_drain_started.pop(run_id, None)
            return True
        if row["cancel_requested"]:
            requested = _parse_time(row["cancel_requested_at"])
            elapsed = (
                (datetime.now(timezone.utc) - requested).total_seconds()
                if requested is not None else 0.0
            )
        else:
            started = self._group_drain_started.setdefault(run_id, time.monotonic())
            elapsed = time.monotonic() - started
        sent = signal.SIGKILL if elapsed >= CANCEL_GRACE_SECONDS else signal.SIGTERM
        try:
            os.killpg(process_group, sent)
        except ProcessLookupError:
            self._group_drain_started.pop(run_id, None)
            return True
        except OSError as exc:
            raise CoordinatorError(
                f"cannot drain gate worker process group {process_group}: {exc}"
            ) from exc
        return not _process_group_exists(process_group)

    # ---------------------------------------------------------------------- queue pump

    def _pump(self) -> None:
        while not self._stop.wait(0.1):
            try:
                self._pump_once()
            except sqlite3.OperationalError as exc:
                if _transient_database_error(exc):
                    self._append_daemon_log(
                        f"pump database contention; retrying: {exc}"
                    )
                else:
                    self._append_daemon_log(
                        f"pump error: {type(exc).__name__}: {exc}"
                    )
            except Exception as exc:  # keep one bad row from silently killing admission
                self._append_daemon_log(f"pump error: {type(exc).__name__}: {exc}")
            if self._should_idle_exit():
                return

    def _pump_once(self) -> None:
        with self._db_lock, self._connect() as db:
            self._maintain_child_cpu_leases(db)
            active = db.execute(
                "SELECT * FROM runs WHERE status = 'running' ORDER BY sequence"
            ).fetchall()
            self._validate_active_set(active)
            for row in active:
                self._observe_active(db, row)

            self._maintain_child_cpu_leases(db)

            # Observation may have completed any number of workers.  Admission reasons
            # from the durable rows again, in the same transaction.
            active = db.execute(
                "SELECT * FROM runs WHERE status = 'running' ORDER BY sequence"
            ).fetchall()
            self._validate_active_set(active)
            queued = db.execute(
                "SELECT * FROM runs WHERE status = 'queued' ORDER BY sequence"
            ).fetchall()
            while queued:
                row = self._next_admissible(active, queued)
                if row is None:
                    return
                now = _now()
                db.execute(
                    "UPDATE runs SET status = 'running', phase = CASE "
                    "WHEN kind = 'land' THEN 'preflight' ELSE 'running' END, "
                    "started_at = ? WHERE run_id = ?",
                    (now, row["run_id"]),
                )
                refreshed = db.execute(
                    "SELECT * FROM runs WHERE run_id = ?", (row["run_id"],)
                ).fetchone()
                self._start_worker(db, refreshed)
                current = db.execute(
                    "SELECT * FROM runs WHERE run_id = ?", (row["run_id"],)
                ).fetchone()
                queued = [candidate for candidate in queued if candidate["run_id"] != row["run_id"]]
                if current["status"] == "running":
                    active = [*active, current]
                self._last_repository = row["repository_id"]

    def _next_admissible(
        self,
        active: Sequence[sqlite3.Row],
        queued: Sequence[sqlite3.Row],
    ) -> sqlite3.Row | None:
        """Choose one repository-lane head in round-robin order."""
        heads: list[sqlite3.Row] = []
        seen: set[str] = set()
        for row in queued:
            repository_id = row["repository_id"]
            if repository_id not in seen:
                seen.add(repository_id)
                heads.append(row)
        if self._last_repository is not None:
            after = [row for row in heads if row["repository_id"] > self._last_repository]
            before = [row for row in heads if row["repository_id"] <= self._last_repository]
            heads = [*after, *before]
        for row in heads:
            if not self._blocked_by(
                row,
                active=active,
                queued=queued,
                capacities=self.capacities,
            ):
                return row
        return None

    def _validate_active_set(self, active: Sequence[sqlite3.Row]) -> None:
        allocations = self._allocations(active)
        for name, units in allocations.items():
            if units > self.capacities.get(name, 0):
                raise CoordinatorError(
                    f"active allocation for {name!r} is {units}, above capacity "
                    f"{self.capacities.get(name, 0)}"
                )
        repositories: dict[str, list[sqlite3.Row]] = {}
        for row in active:
            repositories.setdefault(row["repository_id"], []).append(row)
        for repository_id, rows in repositories.items():
            if any(row["barrier"] for row in rows) and len(rows) != 1:
                identities = ", ".join(row["run_id"] for row in rows)
                raise CoordinatorError(
                    f"repository {repository_id} has a barrier overlap: {identities}"
                )

    def _resource_request(
        self,
        row: sqlite3.Row,
        backend: str,
        names: Sequence[str],
    ) -> ResourceRequest:
        resources = self._row_resources(row)
        contract = self._row_resource_contract(row)
        selected_resources: dict[str, int] = {}
        selected_bindings: dict[str, Mapping[str, object]] = {}
        for name in names:
            binding = contract.get(name)
            if binding is None or binding["backend"] != backend:
                raise CoordinatorError(
                    f"run {row['run_id']} has inconsistent private resource state"
                )
            selected_resources[name] = resources[name]
            selected_bindings[name] = binding
        return ResourceRequest.build(
            row["run_id"],
            backend,
            selected_resources,
            selected_bindings,
        )

    def _append_resource_event(
        self,
        receipt: dict[str, object],
        *,
        backend: str,
        resource: str,
        stage: str,
        status: str,
        code: str,
    ) -> None:
        events = receipt["events"]
        if not isinstance(events, list):
            raise CoordinatorError("resource receipt events are not mutable")
        events.append(
            {
                "at": _now(),
                "backend": backend,
                "resource": resource,
                "stage": stage,
                "status": status,
                "code": code,
            }
        )

    def _save_resource_records(
        self,
        db: sqlite3.Connection,
        row: sqlite3.Row,
        receipt: Mapping[str, object],
        state: Mapping[str, Mapping[str, object]],
    ) -> None:
        resources = self._row_resources(row)
        try:
            selected_receipt = validate_resource_receipt(receipt, resources)
            selected_state = validate_backend_state(state)
        except ResourceContractError as exc:
            raise CoordinatorError(
                f"run {row['run_id']} produced invalid resource lifecycle data"
            ) from exc
        db.execute(
            "UPDATE runs SET resource_receipt_json = ?, resource_state_json = ? "
            "WHERE run_id = ?",
            (
                json.dumps(selected_receipt, separators=(",", ":")),
                json.dumps(selected_state, separators=(",", ":")),
                row["run_id"],
            ),
        )

    def _cleanup_resource_records(
        self,
        row: sqlite3.Row,
        receipt: dict[str, object],
        state: dict[str, dict[str, object]],
        *,
        only: set[str] | None = None,
    ) -> None:
        for backend_name in list(state):
            if only is not None and backend_name not in only:
                continue
            record = state[backend_name]
            names = record["resources"]
            request = self._resource_request(row, backend_name, names)
            backend = self.resource_backends.get(backend_name)
            try:
                if backend is None:
                    raise ResourceContractError("backend unavailable")
                backend.cleanup(request, record["handle"])
            except Exception as exc:
                failure_code = _resource_failure_code(exc, "cleanup-failed")
                self._append_daemon_log(
                    f"resource cleanup failed for {row['run_id']} via "
                    f"{backend_name}: {type(exc).__name__}"
                )
                for name in names:
                    self._append_resource_event(
                        receipt,
                        backend=backend_name,
                        resource=name,
                        stage="cleanup",
                        status="failed",
                        code=failure_code,
                    )
            else:
                for name in names:
                    self._append_resource_event(
                        receipt,
                        backend=backend_name,
                        resource=name,
                        stage="cleanup",
                        status="recorded",
                        code="cleaned",
                    )
            del state[backend_name]

    def _prepare_resources(self, db: sqlite3.Connection, row: sqlite3.Row) -> None:
        receipt = self._row_resource_receipt(row)
        state = self._row_resource_state(row)
        if state:
            raise CoordinatorError(
                f"run {row['run_id']} already has prepared resource state"
            )
        contract = self._row_resource_contract(row)
        eligible: dict[str, list[str]] = {}
        required_failure = False
        for name, binding in contract.items():
            mode = binding["mode"]
            if mode == "admission-only":
                continue
            backend_name = str(binding["backend"])
            issue = capability_issue(
                binding,
                self.resource_capabilities.get(backend_name),
            )
            if issue is not None:
                self._append_resource_event(
                    receipt,
                    backend=backend_name,
                    resource=name,
                    stage="probe",
                    status="failed" if mode == "required" else "unapplied",
                    code=issue,
                )
                required_failure = required_failure or mode == "required"
            else:
                eligible.setdefault(backend_name, []).append(name)
        if required_failure:
            self._save_resource_records(db, row, receipt, state)
            raise _ResourceEnforcementError(
                "a required resource backend or unit is unavailable"
            )

        for backend_name, names in sorted(eligible.items()):
            request = self._resource_request(row, backend_name, names)
            backend = self.resource_backends[backend_name]
            try:
                handle = backend.prepare(request)
                candidate = {
                    "handle": handle,
                    "resources": list(names),
                    "finished": False,
                    "cancelled": False,
                }
                selected = validate_backend_state({backend_name: candidate})
                state[backend_name] = selected[backend_name]
            except Exception as exc:
                failure_code = _resource_failure_code(exc, "prepare-failed")
                self._append_daemon_log(
                    f"resource prepare failed for {row['run_id']} via "
                    f"{backend_name}: {type(exc).__name__}"
                )
                for name in names:
                    mode = contract[name]["mode"]
                    self._append_resource_event(
                        receipt,
                        backend=backend_name,
                        resource=name,
                        stage="prepare",
                        status="failed" if mode == "required" else "unapplied",
                        code=failure_code,
                    )
                    required_failure = required_failure or mode == "required"
            else:
                for name in names:
                    self._append_resource_event(
                        receipt,
                        backend=backend_name,
                        resource=name,
                        stage="prepare",
                        status="recorded",
                        code="prepared",
                    )
        if required_failure:
            self._cleanup_resource_records(row, receipt, state)
            self._save_resource_records(db, row, receipt, state)
            raise _ResourceEnforcementError("a required resource could not be prepared")
        self._save_resource_records(db, row, receipt, state)

    def _attach_resources(
        self,
        db: sqlite3.Connection,
        row: sqlite3.Row,
        worker_pid: int,
    ) -> None:
        receipt = self._row_resource_receipt(row)
        state = self._row_resource_state(row)
        contract = self._row_resource_contract(row)
        required_failure = False
        failed_best_effort: set[str] = set()
        applied = receipt["applied"]
        if not isinstance(applied, dict):
            raise CoordinatorError("resource receipt application record is not mutable")
        for backend_name, record in state.items():
            names = record["resources"]
            request = self._resource_request(row, backend_name, names)
            backend = self.resource_backends.get(backend_name)
            try:
                if backend is None:
                    raise ResourceContractError("backend unavailable")
                backend.attach(request, record["handle"], worker_pid)
            except Exception as exc:
                failure_code = _resource_failure_code(exc, "attach-failed")
                self._append_daemon_log(
                    f"resource attach failed for {row['run_id']} via "
                    f"{backend_name}: {type(exc).__name__}"
                )
                for name in names:
                    mode = contract[name]["mode"]
                    self._append_resource_event(
                        receipt,
                        backend=backend_name,
                        resource=name,
                        stage="attach",
                        status="failed" if mode == "required" else "unapplied",
                        code=failure_code,
                    )
                    required_failure = required_failure or mode == "required"
                if not any(contract[name]["mode"] == "required" for name in names):
                    failed_best_effort.add(backend_name)
            else:
                for name, units in request.resources.items():
                    if (
                        backend_name == CGROUP_BACKEND
                        and contract[name]["kind"] in {"inodes", "tmpfs"}
                    ) or backend_name == PROJECT_QUOTA_BACKEND:
                        continue
                    applied[name] = units
                    self._append_resource_event(
                        receipt,
                        backend=backend_name,
                        resource=name,
                        stage="attach",
                        status="applied",
                        code="applied",
                    )
        if required_failure:
            self._cleanup_resource_records(row, receipt, state)
        elif failed_best_effort:
            self._cleanup_resource_records(
                row,
                receipt,
                state,
                only=failed_best_effort,
            )
        self._save_resource_records(db, row, receipt, state)
        if required_failure:
            raise _ResourceEnforcementError("a required resource could not be attached")

    def _resource_measurement(
        self,
        value: object,
        *,
        expected: set[str],
    ) -> tuple[dict[str, int], tuple[ResourceObservation, ...]]:
        return validate_resource_measurement(value, expected=expected)

    def _capture_resource_usage(
        self,
        db: sqlite3.Connection,
        row: sqlite3.Row,
        *,
        final: bool,
    ) -> None:
        receipt = self._row_resource_receipt(row)
        state = self._row_resource_state(row)
        peak = receipt["peak"]
        if not isinstance(peak, dict):
            raise CoordinatorError("resource receipt peak record is not mutable")
        changed = False
        for backend_name, record in state.items():
            if record["finished"]:
                continue
            names = record["resources"]
            request = self._resource_request(row, backend_name, names)
            backend = self.resource_backends.get(backend_name)
            stage = "finish" if final else "usage"
            try:
                if backend is None:
                    raise ResourceContractError("backend unavailable")
                raw = (
                    backend.finish(request, record["handle"])
                    if final
                    else backend.usage(request, record["handle"])
                )
                measured, observations = self._resource_measurement(
                    raw,
                    expected=set(names),
                )
            except Exception as exc:
                failure_code = _resource_failure_code(exc, f"{stage}-failed")
                already_recorded = any(
                    event["backend"] == backend_name
                    and event["stage"] == stage
                    and event["code"] == failure_code
                    for event in receipt["events"]
                )
                if not already_recorded:
                    self._append_daemon_log(
                        f"resource {stage} failed for {row['run_id']} via "
                        f"{backend_name}: {type(exc).__name__}"
                    )
                    for name in names:
                        self._append_resource_event(
                            receipt,
                            backend=backend_name,
                            resource=name,
                            stage=stage,
                            status="failed",
                            code=failure_code,
                        )
                    changed = True
            else:
                for name, units in measured.items():
                    if units > peak.get(name, -1):
                        peak[name] = units
                        changed = True
                for observation in observations:
                    if any(
                        event["backend"] == backend_name
                        and event["resource"] == observation.resource
                        and event["code"] == observation.code
                        for event in receipt["events"]
                    ):
                        continue
                    self._append_resource_event(
                        receipt,
                        backend=backend_name,
                        resource=observation.resource,
                        stage=stage,
                        status="recorded",
                        code=observation.code,
                    )
                    changed = True
                if final:
                    for name in names:
                        self._append_resource_event(
                            receipt,
                            backend=backend_name,
                            resource=name,
                            stage="finish",
                            status="recorded",
                            code="finished",
                        )
                    changed = True
            if final:
                record["finished"] = True
                changed = True
        if changed:
            self._save_resource_records(db, row, receipt, state)

    def _finish_and_cleanup_resources(
        self,
        db: sqlite3.Connection,
        row: sqlite3.Row,
    ) -> None:
        self._capture_resource_usage(db, row, final=True)
        refreshed = db.execute(
            "SELECT * FROM runs WHERE run_id = ?", (row["run_id"],)
        ).fetchone()
        receipt = self._row_resource_receipt(refreshed)
        state = self._row_resource_state(refreshed)
        if state:
            self._cleanup_resource_records(refreshed, receipt, state)
            self._save_resource_records(db, refreshed, receipt, state)

    def _cancel_resources(self, db: sqlite3.Connection, row: sqlite3.Row) -> None:
        receipt = self._row_resource_receipt(row)
        state = self._row_resource_state(row)
        changed = False
        for backend_name, record in state.items():
            if record["cancelled"]:
                continue
            names = record["resources"]
            request = self._resource_request(row, backend_name, names)
            backend = self.resource_backends.get(backend_name)
            try:
                if backend is None:
                    raise ResourceContractError("backend unavailable")
                backend.cancel(request, record["handle"])
            except Exception as exc:
                failure_code = _resource_failure_code(exc, "cancel-failed")
                self._append_daemon_log(
                    f"resource cancel failed for {row['run_id']} via "
                    f"{backend_name}: {type(exc).__name__}"
                )
                for name in names:
                    self._append_resource_event(
                        receipt,
                        backend=backend_name,
                        resource=name,
                        stage="cancel",
                        status="failed",
                        code=failure_code,
                    )
            else:
                for name in names:
                    self._append_resource_event(
                        receipt,
                        backend=backend_name,
                        resource=name,
                        stage="cancel",
                        status="recorded",
                        code="cancelled",
                    )
            record["cancelled"] = True
            changed = True
        if changed:
            self._save_resource_records(db, row, receipt, state)

    def _tmpfs_resource_names(self, row: sqlite3.Row) -> list[str]:
        contract = self._row_resource_contract(row)
        return [
            name
            for name, binding in contract.items()
            if binding["backend"] == CGROUP_BACKEND
            and binding["kind"] in {"inodes", "tmpfs"}
        ]

    def _project_quota_scratch_path(
        self,
        db: sqlite3.Connection,
        row: sqlite3.Row,
    ) -> Path | None:
        state = self._row_resource_state(row)
        record = state.get(PROJECT_QUOTA_BACKEND)
        if record is None:
            return None
        backend = self.resource_backends.get(PROJECT_QUOTA_BACKEND)
        try:
            if not isinstance(backend, ProjectQuotaBackend):
                raise ResourceContractError(
                    "project quota scratch backend is unavailable"
                )
            request = self._resource_request(
                row,
                PROJECT_QUOTA_BACKEND,
                record["resources"],
            )
            return backend.scratch_path(request, record["handle"])
        except Exception as exc:
            receipt = self._row_resource_receipt(row)
            contract = self._row_resource_contract(row)
            code = _resource_failure_code(exc, "quota-tree-unavailable")
            required = False
            for name in record["resources"]:
                mode = str(contract[name]["mode"])
                required = required or mode == "required"
                self._append_resource_event(
                    receipt,
                    backend=PROJECT_QUOTA_BACKEND,
                    resource=name,
                    stage="attach",
                    status="failed" if mode == "required" else "unapplied",
                    code=code,
                )
            self._cleanup_resource_records(
                row,
                receipt,
                state,
                only={PROJECT_QUOTA_BACKEND},
            )
            self._save_resource_records(db, row, receipt, state)
            cleanup_failed = any(
                event["backend"] == PROJECT_QUOTA_BACKEND
                and event["stage"] == "cleanup"
                and event["status"] == "failed"
                for event in receipt["events"]
            )
            if required or cleanup_failed:
                raise _ResourceEnforcementError(
                    "project quota scratch path is unavailable"
                ) from exc
            return None

    def _record_project_quota_worker_setup(
        self,
        db: sqlite3.Connection,
        row: sqlite3.Row,
        *,
        code: str,
    ) -> bool:
        state = self._row_resource_state(row)
        record = state.get(PROJECT_QUOTA_BACKEND)
        if record is None:
            raise CoordinatorError("project quota setup has no backend state")
        receipt = self._row_resource_receipt(row)
        contract = self._row_resource_contract(row)
        requested = self._row_resources(row)
        applied = receipt["applied"]
        if not isinstance(applied, dict):
            raise CoordinatorError("resource receipt application record is not mutable")
        if code == "ok":
            for name in record["resources"]:
                applied[name] = requested[name]
                self._append_resource_event(
                    receipt,
                    backend=PROJECT_QUOTA_BACKEND,
                    resource=name,
                    stage="attach",
                    status="applied",
                    code="quota-ready",
                )
            failed = False
        else:
            if not _SETUP_CODE.fullmatch(code):
                code = "worker-setup-failed"
            for name in record["resources"]:
                mode = str(contract[name]["mode"])
                self._append_resource_event(
                    receipt,
                    backend=PROJECT_QUOTA_BACKEND,
                    resource=name,
                    stage="attach",
                    status="failed" if mode == "required" else "unapplied",
                    code=code,
                )
            # A command must never inherit the broker's quota-administration power,
            # even when the quota resources themselves were requested best-effort.
            failed = True
        self._save_resource_records(db, row, receipt, state)
        return failed

    def _record_tmpfs_setup(
        self,
        db: sqlite3.Connection,
        row: sqlite3.Row,
        *,
        code: str,
        setup: Mapping[str, object] | None,
    ) -> bool:
        names = self._tmpfs_resource_names(row)
        if not names:
            return False
        receipt = self._row_resource_receipt(row)
        state = self._row_resource_state(row)
        contract = self._row_resource_contract(row)
        requested = self._row_resources(row)
        applied = receipt["applied"]
        if not isinstance(applied, dict):
            raise CoordinatorError("resource receipt application record is not mutable")
        required_failure = False
        if code == "ok":
            if setup is None:
                raise CoordinatorError("successful tmpfs setup has no private setup record")
            values = {
                "tmpfs": setup["size"],
                "inodes": setup["inodes"],
            }
            for name in names:
                value = values[str(contract[name]["kind"])]
                if (
                    not isinstance(value, int)
                    or isinstance(value, bool)
                    or value <= 0
                    or value > requested[name]
                ):
                    raise CoordinatorError("tmpfs setup applied an invalid capacity")
                applied[name] = value
                self._append_resource_event(
                    receipt,
                    backend=CGROUP_BACKEND,
                    resource=name,
                    stage="attach",
                    status="applied",
                    code="tmpfs-mounted",
                )
        else:
            if not _SETUP_CODE.fullmatch(code):
                code = "tmpfs-setup-failed"
            namespace_failure = code.startswith("namespace-") or code in {
                "controller-files-exposed",
                "tmpfs-namespace-required",
            }
            for name in names:
                mode = str(contract[name]["mode"])
                self._append_resource_event(
                    receipt,
                    backend=CGROUP_BACKEND,
                    resource=name,
                    stage="attach",
                    status="failed" if mode == "required" else "unapplied",
                    code=code,
                )
                required_failure = required_failure or mode == "required"
            if namespace_failure:
                required_failure = required_failure or any(
                    binding["backend"] == CGROUP_BACKEND
                    and binding["mode"] == "required"
                    for binding in contract.values()
                )
            record = state.get(CGROUP_BACKEND)
            if isinstance(record, dict):
                record["resources"] = [
                    name
                    for name in record["resources"]
                    if name not in names
                ]
        self._save_resource_records(db, row, receipt, state)
        return required_failure

    def _prepare_tmpfs_setup(
        self,
        db: sqlite3.Connection,
        row: sqlite3.Row,
        target: Path,
    ) -> dict[str, object] | None:
        names = self._tmpfs_resource_names(row)
        if not names:
            return None
        state = self._row_resource_state(row)
        record = state.get(CGROUP_BACKEND)
        backend = self.resource_backends.get(CGROUP_BACKEND)
        try:
            if record is None or not isinstance(backend, CgroupV2Backend):
                raise ResourceContractError("tmpfs cgroup state is unavailable")
            request = self._resource_request(
                row,
                CGROUP_BACKEND,
                record["resources"],
            )
            raw = backend.tmpfs_setup(request, record["handle"], target)
            if not isinstance(raw, Mapping):
                raise ResourceContractError("tmpfs setup is unavailable")
            setup = dict(raw)
            if (
                set(setup)
                != {"version", "target", "size", "inodes", "report", "token"}
                or type(setup["version"]) is not int
                or setup["version"] != 1
                or setup["target"] != str(target)
                or not isinstance(setup["report"], str)
                or not Path(setup["report"]).is_absolute()
                or not isinstance(setup["token"], str)
                or not re.fullmatch(r"[0-9a-f]{32}", setup["token"])
                or not isinstance(setup["size"], int)
                or isinstance(setup["size"], bool)
                or setup["size"] <= 0
                or not isinstance(setup["inodes"], int)
                or isinstance(setup["inodes"], bool)
                or setup["inodes"] <= 0
            ):
                raise ResourceContractError("tmpfs setup is invalid")
        except Exception as exc:
            code = _resource_failure_code(exc, "tmpfs-setup-failed")
            if self._record_tmpfs_setup(db, row, code=code, setup=None):
                raise _ResourceEnforcementError(
                    "a required tmpfs could not be prepared"
                ) from exc
            return None
        return setup

    @staticmethod
    def _read_worker_setup(descriptor: int, *, timeout: float = 5.0) -> str:
        readable, _writable, _exceptional = select.select(
            [descriptor],
            [],
            [],
            timeout,
        )
        if not readable:
            return "tmpfs-setup-timeout"
        try:
            payload = os.read(descriptor, 128).decode("ascii")
        except (OSError, UnicodeError):
            return "tmpfs-setup-failed"
        return payload if _SETUP_CODE.fullmatch(payload) else "tmpfs-setup-invalid"

    def _start_worker(self, db: sqlite3.Connection, row: sqlite3.Row) -> None:
        run_id = row["run_id"]
        command = json.loads(row["command_json"])
        log_path = self._log_path(run_id)
        environment = json.loads(row["environment_json"])
        if not isinstance(environment, dict) or not all(
            isinstance(key, str) and key and "=" not in key and "\0" not in key
            and isinstance(value, str) and "\0" not in value
            for key, value in environment.items()
        ):
            raise CoordinatorError(
                f"gate run {run_id} has an invalid stored environment"
            )
        environment[RUN_ID_ENV] = run_id
        environment[RUN_KIND_ENV] = row["kind"]
        environment[STATE_DIR_ENV] = str(self.paths.state_dir)
        environment.pop(CGROUP_ISOLATE_ENV, None)
        environment.pop(PROJECT_QUOTA_DROP_ENV, None)
        environment.pop(TMPFS_SETUP_ENV, None)
        for variable in ("TMPDIR", "TMP", "TEMP"):
            environment.pop(variable, None)
        worker_tmp = self._worker_tmp_path(run_id)
        release_read = -1
        release_write = -1
        setup_read = -1
        setup_write = -1
        process: subprocess.Popen[bytes] | None = None
        released = False
        try:
            self._prepare_resources(db, row)
            prepared_row = db.execute(
                "SELECT * FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            prepared_state = self._row_resource_state(prepared_row)
            cgroup_backend = self.resource_backends.get(CGROUP_BACKEND)
            if (
                CGROUP_BACKEND in prepared_state
                and isinstance(cgroup_backend, CgroupV2Backend)
                and cgroup_backend.isolate_workers
            ):
                environment[CGROUP_ISOLATE_ENV] = "1"
            if row["kind"] in {"full", "merge", "land"}:
                _assert_clean_head(Path(row["checkout"]), row["head_sha"])
            worker_command = command
            if row["kind"] == "land":
                worker_command = [
                    sys.executable,
                    "-m",
                    "agcoord.land",
                    "--run-id",
                    run_id,
                    "--state-dir",
                    str(self.paths.state_dir),
                    "--checkout",
                    row["checkout"],
                    "--branch",
                    row["branch"],
                    "--head-sha",
                    row["head_sha"],
                    "--adapter",
                    row["publication_adapter"],
                    "--request-json",
                    row["publication_request"],
                    "--",
                    *command,
                ]
            prepared_row = db.execute(
                "SELECT * FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            quota_scratch = self._project_quota_scratch_path(db, prepared_row)
            tmpfs_requested = bool(self._tmpfs_resource_names(prepared_row))
            scratch_target = quota_scratch
            tmpfs_setup = None
            if tmpfs_requested:
                scratch_target = worker_tmp
                scratch_target.mkdir(mode=0o700)
                scratch_target.chmod(0o700)
                tmpfs_setup = self._prepare_tmpfs_setup(
                    db,
                    prepared_row,
                    scratch_target,
                )
                if tmpfs_setup is None:
                    scratch_target = None
            elif quota_scratch is not None:
                environment[PROJECT_QUOTA_DROP_ENV] = "1"
            if tmpfs_setup is not None:
                environment[TMPFS_SETUP_ENV] = json.dumps(
                    tmpfs_setup,
                    separators=(",", ":"),
                )
            if scratch_target is not None:
                for variable in ("TMPDIR", "TMP", "TEMP"):
                    environment[variable] = str(scratch_target)
            release_read, release_write = os.pipe()
            worker_setup_required = tmpfs_setup is not None or quota_scratch is not None
            if worker_setup_required:
                setup_read, setup_write = os.pipe()
            with log_path.open("ab", buffering=0) as output:
                log_path.chmod(0o600)
                process = subprocess.Popen(
                    [
                        sys.executable,
                        "-c",
                        _WORKER_LAUNCHER,
                        str(release_read),
                        str(setup_write),
                        *worker_command,
                    ],
                    cwd=row["checkout"],
                    stdin=subprocess.DEVNULL,
                    stdout=output,
                    stderr=subprocess.STDOUT,
                    env=environment,
                    start_new_session=True,
                    pass_fds=(
                        (release_read,)
                        if setup_write < 0
                        else (release_read, setup_write)
                    ),
                )
            os.close(release_read)
            release_read = -1
            if setup_write >= 0:
                os.close(setup_write)
                setup_write = -1
            token = _process_start_token(process.pid)
            if token is None:
                raise CoordinatorError(
                    f"could not identify gate launcher process {process.pid}"
                )
            prepared_row = db.execute(
                "SELECT * FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            self._attach_resources(db, prepared_row, process.pid)
            db.execute(
                "UPDATE runs SET worker_pid = ?, worker_start_token = ?, "
                "environment_json = '{}' WHERE run_id = ?",
                (process.pid, token, run_id),
            )
            # The launcher cannot exec the requested command until this identity is
            # durable. If the broker dies before commit/release, pipe EOF makes it exit;
            # the row can never roll back to queued beside a live duplicate.
            db.commit()
            self._children[run_id] = process
            os.write(release_write, b"1")
            if worker_setup_required:
                setup_code = self._read_worker_setup(setup_read)
                os.close(setup_read)
                setup_read = -1
                setup_row = db.execute(
                    "SELECT * FROM runs WHERE run_id = ?", (run_id,)
                ).fetchone()
                if tmpfs_setup is not None:
                    setup_failure = self._record_tmpfs_setup(
                        db,
                        setup_row,
                        code=setup_code,
                        setup=tmpfs_setup,
                    )
                else:
                    setup_failure = self._record_project_quota_worker_setup(
                        db,
                        setup_row,
                        code=setup_code,
                    )
                db.commit()
                os.write(release_write, b"0" if setup_failure else b"1")
                if setup_failure:
                    raise _ResourceEnforcementError(
                        "required worker resource setup failed"
                    )
            released = True
        except Exception as exc:
            self._children.pop(run_id, None)
            self._group_drain_started.pop(run_id, None)
            if process is not None and not released:
                os.close(release_write)
                release_write = -1
                try:
                    process.wait(timeout=1.0)
                except subprocess.TimeoutExpired:
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    process.wait(timeout=1.0)
            refreshed = db.execute(
                "SELECT * FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            try:
                receipt = self._row_resource_receipt(refreshed)
                resource_state = self._row_resource_state(refreshed)
                if resource_state:
                    self._cleanup_resource_records(
                        refreshed,
                        receipt,
                        resource_state,
                    )
                    self._save_resource_records(
                        db,
                        refreshed,
                        receipt,
                        resource_state,
                    )
            except Exception as cleanup_exc:
                self._append_daemon_log(
                    f"resource rollback failed for {run_id}: "
                    f"{type(cleanup_exc).__name__}"
                )
            with log_path.open("a", encoding="utf-8") as output:
                output.write(f"Gate coordinator: could not start worker: {exc}\n")
            log_path.chmod(0o600)
            if not self._remove_worker_tmp(run_id):
                db.execute(
                    "UPDATE runs SET environment_json = '{}' WHERE run_id = ?",
                    (run_id,),
                )
                return
            resource_failure = isinstance(exc, _ResourceEnforcementError)
            failure_reason = (
                "resource-enforcement-failed"
                if resource_failure
                else ("merge-error" if row["kind"] == "merge" else None)
            )
            db.execute(
                "UPDATE runs SET status = 'failed', phase = 'complete', "
                "finished_at = ?, exit_status = ?, "
                "failure_reason = ?, environment_json = '{}' WHERE run_id = ?",
                (_now(), 125 if resource_failure else 127, failure_reason, run_id),
            )
            self._prune(db)
        finally:
            if release_read >= 0:
                os.close(release_read)
            if release_write >= 0:
                os.close(release_write)
            if setup_read >= 0:
                os.close(setup_read)
            if setup_write >= 0:
                os.close(setup_write)

    def _failure_reason_for(
        self,
        row: sqlite3.Row,
        *,
        status: str,
        exit_status: int | None,
    ) -> str | None:
        if status != "failed":
            return None
        receipt = self._row_resource_receipt(row)
        if any(event["code"] == "memory-oom" for event in receipt["events"]):
            return "memory-oom"
        if row["kind"] not in {"merge", "land"}:
            return None
        from .merge import FAILURE_REASONS

        if (
            row["kind"] == "land"
            and row["phase"] == "gating"
            and row["gate_exit_status"] is not None
        ):
            return "gate-failed"
        return FAILURE_REASONS.get(exit_status, "merge-error")

    def _observe_active(self, db: sqlite3.Connection, row: sqlite3.Row) -> None:
        run_id = row["run_id"]
        child = self._children.get(run_id)
        if child is not None:
            returncode = child.poll()
            if returncode is None:
                self._capture_resource_usage(db, row, final=False)
                refreshed = db.execute(
                    "SELECT * FROM runs WHERE run_id = ?", (run_id,)
                ).fetchone()
                self._escalate_cancel(db, refreshed)
                return
            if not self._drain_finished_process_group(row):
                return
            self._finish_and_cleanup_resources(db, row)
            if not self._remove_worker_tmp(run_id):
                return
            refreshed = db.execute(
                "SELECT * FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            exit_status = _shell_status(returncode)
            if refreshed["cancel_requested"]:
                status = "cancelled"
                exit_status = 130
            else:
                status = "passed" if exit_status == 0 else "failed"
            failure_reason = self._failure_reason_for(
                refreshed,
                status=status,
                exit_status=exit_status,
            )
            db.execute(
                "UPDATE runs SET status = ?, phase = 'complete', finished_at = ?, exit_status = ?, "
                "failure_reason = ? "
                "WHERE run_id = ?",
                (status, _now(), exit_status, failure_reason, run_id),
            )
            self._prune(db)
            # Keep the owned Popen until the terminal row update succeeds. A transient
            # writer lock can roll this transaction back; retaining the child preserves
            # its already-observed return code for the next pump attempt.
            self._children.pop(run_id, None)
            self._group_drain_started.pop(run_id, None)
            return

        # A broker can be SIGKILLed while its process group remains. The replacement does
        # not launch a second worker: it observes the exact pid+start token until the old
        # group ends, preserving the coordinator as the sole exclusion boundary.
        if _same_process(row["worker_pid"], row["worker_start_token"]):
            self._capture_resource_usage(db, row, final=False)
            refreshed = db.execute(
                "SELECT * FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            self._escalate_cancel(db, refreshed)
            return
        if not self._drain_finished_process_group(row):
            return
        self._finish_and_cleanup_resources(db, row)
        if not self._remove_worker_tmp(run_id):
            return
        self._group_drain_started.pop(run_id, None)
        # Re-read after the process disappears: a recovered land worker durably reports
        # immediately before exit, potentially after this pump's initial active snapshot.
        refreshed = db.execute(
            "SELECT * FROM runs WHERE run_id = ?", (run_id,)
        ).fetchone()
        if refreshed["cancel_requested"]:
            status = "cancelled"
            exit_status = 130
        elif refreshed["kind"] == "land" and refreshed["reported_exit_status"] is not None:
            exit_status = refreshed["reported_exit_status"]
            status = "passed" if exit_status == 0 else "failed"
        else:
            status = "interrupted"
            exit_status = None
        failure_reason = self._failure_reason_for(
            refreshed,
            status=status,
            exit_status=exit_status,
        )
        db.execute(
            "UPDATE runs SET status = ?, phase = 'complete', finished_at = ?, "
            "exit_status = ?, failure_reason = ? WHERE run_id = ?",
            (status, _now(), exit_status, failure_reason, run_id),
        )
        self._prune(db)

    def _escalate_cancel(self, db: sqlite3.Connection, row: sqlite3.Row) -> None:
        if not row["cancel_requested"]:
            return
        self._cancel_resources(db, row)
        requested = _parse_time(row["cancel_requested_at"])
        if requested is None:
            return
        elapsed = (datetime.now(timezone.utc) - requested).total_seconds()
        self._signal_worker(
            row,
            signal.SIGKILL if elapsed >= CANCEL_GRACE_SECONDS else signal.SIGTERM,
        )

    def _signal_worker(self, row: sqlite3.Row, sent: signal.Signals) -> None:
        pid = row["worker_pid"]
        if not _same_process(pid, row["worker_start_token"]):
            return
        try:
            os.killpg(pid, sent)
        except ProcessLookupError:
            return
        except OSError as exc:
            raise CoordinatorError(
                f"cannot signal gate worker {pid}: {exc}"
            ) from exc

    def _prune(self, db: sqlite3.Connection) -> None:
        stale = db.execute(
            """
            SELECT run_id FROM runs
            WHERE status IN ('passed', 'failed', 'cancelled', 'interrupted')
              AND run_id NOT IN (
                  SELECT gate_run_id FROM runs
                  WHERE kind = 'merge' AND status IN ('queued', 'running')
                    AND gate_run_id IS NOT NULL
              )
            ORDER BY sequence DESC LIMIT -1 OFFSET ?
            """,
            (self.recent_limit,),
        ).fetchall()
        for row in stale:
            if not self._remove_worker_tmp(row["run_id"]):
                continue
            self._log_path(row["run_id"]).unlink(missing_ok=True)
            db.execute("DELETE FROM runs WHERE run_id = ?", (row["run_id"],))

    def _should_idle_exit(self) -> bool:
        try:
            with self._db_lock, self._connect() as db:
                maintenance = _maintenance_record(db)
                activity = db.execute(
                    "SELECT value FROM coordinator_meta WHERE key = 'last_activity'"
                ).fetchone()
                live = db.execute(
                    "SELECT COUNT(*) FROM runs WHERE status IN ('queued', 'running')"
                ).fetchone()[0]
        except sqlite3.OperationalError as exc:
            if not _transient_database_error(exc):
                raise
            # A health check that cannot read the live-row count cannot prove idleness.
            # Treat contention as "not idle" and let the next pump iteration retry.
            self._append_daemon_log(
                f"idle check database contention; retrying: {exc}"
            )
            return False
        if live:
            return False
        if maintenance is not None:
            if maintenance["maintenance_state"] != "drained":
                with self._db_lock, self._connect() as db:
                    db.execute(
                        "UPDATE coordinator_meta SET value = 'drained' "
                        "WHERE key = 'maintenance_state'"
                    )
            return True
        if self.idle_timeout is None:
            return False
        last_activity = float(activity["value"]) if activity is not None else 0.0
        return time.time() - last_activity >= self.idle_timeout

    def _append_daemon_log(self, message: str) -> None:
        try:
            with self.paths.daemon_log.open("a", encoding="utf-8") as output:
                output.write(f"{_now()} {message}\n")
            self.paths.daemon_log.chmod(0o600)
        except OSError:
            pass

def _validate_command(command: Any) -> list[str]:
    if not isinstance(command, (list, tuple)) or not command:
        raise CoordinatorError("gate command must be a non-empty list")
    if not isinstance(command[0], str) or not command[0] or "\0" in command[0]:
        raise CoordinatorError("gate command executable must be a non-empty NUL-free string")
    if not all(isinstance(value, str) and "\0" not in value for value in command[1:]):
        raise CoordinatorError(
            "every gate command argument must be a NUL-free string"
        )
    return list(command)


@dataclass
class ChildCpuLease:
    """One granted share of an admitted run's finite CPU worker-token budget."""

    lease_id: str
    run_id: str
    requested: int
    minimum: int
    granted: int
    _client: "CoordinatorClient" = field(repr=False)
    _released: bool = field(default=False, init=False, repr=False)

    @property
    def full(self) -> bool:
        return self.granted == self.requested

    def release(self) -> None:
        if self._released:
            return
        self._client.release_child_cpu_lease(self.lease_id)
        self._released = True

    def __enter__(self) -> "ChildCpuLease":
        return self

    def __exit__(self, _exc_type, _exc, _traceback) -> None:
        self.release()


@dataclass(frozen=True)
class _PreparedSubmission:
    """Caller-side facts a submission settles before any broker may be started."""

    identity: RepositoryIdentity
    branch: str
    head_sha: str | None
    caller_pid: int
    environment: dict[str, str]

    def exact_head(self) -> str:
        if self.head_sha is None:
            raise CoordinatorError("this submission requires an exact clean head")
        return self.head_sha


class CoordinatorClient:
    """Strict synchronous client over the user-only durable spool.

    SQLite is the inbox rather than a network protocol: restricted Codex sessions refuse
    even local Unix-socket ``bind(2)``.  The ownership flock proves one live broker is
    supervising the rows, while SQLite gives simultaneous terminals atomic submissions and
    coherent snapshots.
    """

    def __init__(
        self,
        *,
        state_dir: str | os.PathLike[str] | None = None,
        checkout: str | os.PathLike[str] | None = None,
        autostart: bool = True,
        connect_timeout: float = 5.0,
        host_maintenance: bool = False,
    ):
        if connect_timeout <= 0:
            raise ValueError("connect_timeout must be positive")
        self.paths = queue_paths(state_dir=state_dir, checkout=checkout)
        self.autostart = autostart
        self.host_maintenance = host_maintenance
        self.connect_timeout = connect_timeout
        self._catalogue_instance: CoordinatorBroker | None = None
        self._native_command_instance: NativeBrokerCommand | None = None
        self._native_callback_command_instance: NativeBrokerCommand | None = None

    def _catalogue(self) -> CoordinatorBroker:
        if self._catalogue_instance is None:
            self._catalogue_instance = CoordinatorBroker(
                self.paths.state_dir,
                idle_timeout=None,
            )
        return self._catalogue_instance

    def _native_command(self) -> NativeBrokerCommand:
        if self._native_command_instance is None:
            config = broker_config(self.paths.state_dir)
            select = (
                NativeBrokerCommand.select_for_host_maintenance
                if self.host_maintenance
                else NativeBrokerCommand.select
            )
            try:
                self._native_command_instance = select(config.native_broker)
            except NativeClientError as exc:
                raise CoordinatorError(str(exc)) from exc
        return self._native_command_instance

    def _native_callback_command(self) -> NativeBrokerCommand:
        if self._native_callback_command_instance is None:
            config = broker_config(self.paths.state_dir)
            try:
                self._native_callback_command_instance = (
                    NativeBrokerCommand.select(config.native_broker)
                    if config.native_broker.allow_development
                    else NativeBrokerCommand.select_for_admitted_callback(
                        config.native_broker
                    )
                )
            except NativeClientError as exc:
                raise CoordinatorError(str(exc)) from exc
        return self._native_callback_command_instance

    def _validate_native_owner(
        self,
        owner: Mapping[str, Any],
        *,
        admitted_callback: bool = False,
    ) -> None:
        selected = (
            self._native_callback_command() if admitted_callback else self._native_command()
        ).identity
        if owner.get("implementation") != NATIVE_IMPLEMENTATION:
            raise CoordinatorError(
                "live protocol-5 owner is not the native Rust broker"
            )
        mismatches = [
            name
            for name, expected in (
                ("version", selected.version),
                ("build", selected.build),
            )
            if owner.get(name) != expected
        ]
        if mismatches:
            raise CoordinatorError(
                "live native broker does not match the selected executable: "
                + ", ".join(mismatches)
                + "; wait for the current owner to stop or restore its exact binary"
            )

    @staticmethod
    def _public_owner(owner: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "protocol": owner["protocol"],
            "broker_pid": owner["pid"],
            "capacities": owner["capacities"],
            "resource_bindings": owner["resource_bindings"],
            "resource_capabilities": owner["resource_capabilities"],
        }

    def _native_invoke(self, command: str, arguments: Sequence[str] = ()) -> Any:
        try:
            return self._native_command().invoke(
                command,
                state_dir=self.paths.state_dir,
                arguments=arguments,
            )
        except NativeClientError as exc:
            raise CoordinatorError(str(exc), code=exc.code) from exc

    def _native_callback_invoke(
        self,
        command: str,
        arguments: Sequence[str] = (),
    ) -> Any:
        try:
            return self._native_callback_command().invoke(
                command,
                state_dir=self.paths.state_dir,
                arguments=arguments,
            )
        except NativeClientError as exc:
            raise CoordinatorError(str(exc), code=exc.code) from exc

    def _admitted_callback_run_id(self) -> str:
        run_id = os.environ.get(RUN_ID_ENV)
        if not run_id:
            raise CoordinatorError(
                "callbacks are available only inside an admitted AGCoord run"
            )
        state_marker = os.environ.get(STATE_DIR_ENV)
        if not state_marker or _absolute(state_marker) != self.paths.state_dir:
            raise CoordinatorError(
                "callback state does not match the admitted AGCoord context"
            )
        return run_id

    def _assert_admitted_callback(self, run_id: str) -> None:
        if self._admitted_callback_run_id() != run_id:
            raise CoordinatorError(
                "callback run does not match the admitted AGCoord context"
            )

    def _maintenance_if_active(self) -> dict[str, Any] | None:
        protocol = _spool_protocol(self.paths)
        if protocol is None:
            return None
        configuration = broker_config(self.paths.state_dir)
        timeout = (
            DEFAULT_DATABASE_TIMEOUT
            if configuration.database_timeout is None
            else configuration.database_timeout
        )
        with closing(sqlite3.connect(self.paths.database, timeout=timeout)) as db:
            db.row_factory = sqlite3.Row
            if _maintenance_record(db) is None:
                return None
        if protocol == PROTOCOL:
            try:
                return _validated_maintenance_receipt(
                    _legacy_maintenance_status(self.paths, transition=True)
                )
            except CoordinatorError as exc:
                if str(exc) == "coordinator is not draining":
                    return None
                raise
        if protocol == NATIVE_PROTOCOL:
            try:
                result = self._native_invoke("drain-status")
            except CoordinatorError as exc:
                cause = exc.__cause__
                if (
                    isinstance(cause, NativeClientError)
                    and cause.code == "broker-not-draining"
                ):
                    return None
                raise
            if not isinstance(result, dict):
                raise CoordinatorError(
                    "native broker returned an invalid maintenance status"
                )
            return _validated_maintenance_receipt(result)
        return None

    def _recover_native_drain(
        self,
        maintenance: dict[str, Any],
    ) -> dict[str, Any]:
        """Start only the owner needed to finish already accepted native work."""
        if (
            self.autostart
            and maintenance["protocol"] == NATIVE_PROTOCOL
            and maintenance["state"] == "draining"
            and maintenance["live"] > 0
            and maintenance["broker_pid"] is None
        ):
            self._start_broker()
            refreshed = self._maintenance_if_active()
            if refreshed is None:
                raise CoordinatorError(
                    "coordinator drain disappeared during native recovery"
                )
            return refreshed
        return maintenance

    def _ensure_broker(
        self,
        *,
        for_submission: bool = False,
        admitted_callback: bool = False,
    ) -> dict[str, Any]:
        deadline = time.monotonic() + self.connect_timeout
        last_metadata_error: _OwnerMetadataError | None = None
        maintenance: dict[str, Any] | None = None
        while True:
            try:
                owner = _read_broker_owner(self.paths)
            except _OwnerMetadataError as exc:
                # flock ownership becomes visible a few instructions before its metadata
                # write. Concurrent first clients wait through only that bounded interval;
                # a persistently malformed live owner still fails closed.
                last_metadata_error = exc
                if time.monotonic() >= deadline:
                    raise CoordinatorError(str(exc)) from exc
                time.sleep(0.01)
                continue
            if owner is not None:
                break
            if admitted_callback:
                if time.monotonic() < deadline:
                    time.sleep(0.01)
                    continue
                break
            try:
                maintenance = self._maintenance_if_active()
            except CoordinatorError as exc:
                if _spool_initializing_error(exc) and time.monotonic() < deadline:
                    time.sleep(0.01)
                    continue
                raise
            break
        if owner is not None:
            if owner["protocol"] == NATIVE_PROTOCOL:
                self._validate_native_owner(
                    owner,
                    admitted_callback=admitted_callback,
                )
            if for_submission:
                maintenance = self._maintenance_if_active()
                if maintenance is not None:
                    raise CoordinatorError(
                        f"coordinator is {maintenance['state']} as "
                        f"{maintenance['drain_id']}; new submissions are refused "
                        "until resume",
                        code="broker-draining",
                    )
            if owner["protocol"] == PROTOCOL:
                if self.autostart:
                    raise CoordinatorError(
                        "a legacy protocol-4 Python broker still owns this state directory; "
                        "let it finish and stop, then run 'agc migrate' before retrying"
                    )
            elif owner["protocol"] != NATIVE_PROTOCOL:
                raise CoordinatorError(
                    f"gate coordinator protocol mismatch: broker has "
                    f"{owner['protocol']}; client supports {PROTOCOL} and "
                    f"{NATIVE_PROTOCOL}"
                )
            return self._public_owner(owner)
        if maintenance is not None:
            if (
                maintenance["protocol"] == NATIVE_PROTOCOL
                and maintenance["state"] == "draining"
                and maintenance["live"] > 0
                and self.autostart
            ):
                self._start_broker()
                return self.ping()
            raise CoordinatorError(
                f"coordinator is {maintenance['state']} as "
                f"{maintenance['drain_id']}; new submissions are refused until resume",
                code="broker-draining",
            )
        if admitted_callback:
            raise CoordinatorError(
                f"no gate broker owns {self.paths.state_dir} for the admitted callback"
            )
        if not self.autostart:
            if last_metadata_error is not None:
                raise CoordinatorError(str(last_metadata_error))
            raise CoordinatorError(
                f"no gate broker owns {self.paths.state_dir}"
            )
        self._start_broker()
        return self.ping()

    def submit(
        self,
        command: Sequence[str],
        *,
        checkout: str,
        kind: str = "check",
        label: str = "run",
        resources: Mapping[str, int] | None = None,
        agent: str | None = None,
        repository: str | None = None,
        branch: str | None = None,
        head_sha: str | None = None,
        caller_pid: int | None = None,
        environment: Mapping[str, str] | None = None,
    ) -> str:
        selected_command = _validate_command(command)
        if kind not in RUN_KINDS:
            raise CoordinatorError(
                "kind must be exactly 'check', 'full', 'merge', or 'land'"
            )
        if kind in {"merge", "land"}:
            raise CoordinatorError(f"{kind} can only be submitted through submit_{kind}")
        if not isinstance(label, str) or not label.strip():
            raise CoordinatorError("label must be a non-empty string")
        prepared = self._prepare_submission(
            checkout=checkout,
            repository=repository,
            branch=branch,
            head_sha=head_sha,
            caller_pid=caller_pid,
            environment=environment,
            exact_head=kind == "full",
        )
        owner = self._ensure_broker(for_submission=True)
        if owner["protocol"] == NATIVE_PROTOCOL:
            return self._native_submit(
                selected_command,
                kind=kind,
                label=label.strip(),
                resources=resources,
                agent=agent,
                prepared=prepared,
                owner=owner,
            )
        return self._catalogue().submit(
            command,
            checkout=checkout,
            kind=kind,
            label=label,
            resources=resources,
            agent=agent,
            repository=repository,
            branch=branch,
            head_sha=head_sha,
            caller_pid=caller_pid,
            environment=environment,
        )

    def _prepare_submission(
        self,
        *,
        checkout: str,
        repository: str | None,
        branch: str | None,
        head_sha: str | None,
        caller_pid: int | None,
        environment: Mapping[str, str] | None,
        exact_head: bool,
    ) -> _PreparedSubmission:
        """Decide every shared caller-side refusal before selection or autostart.

        Repository discovery, the exact clean head, the caller PID, and the nesting
        rule are properties of the caller, not of the target spool, so they are settled
        here and only the owner's capacities remain to be checked once a broker exists.
        The relative order of these refusals is unchanged.
        """
        identity = discover_repository(checkout, repository=repository)
        selected_branch = (
            branch.strip()
            if isinstance(branch, str)
            else _git_branch(identity.checkout)
        )
        if not selected_branch:
            raise CoordinatorError("branch must be a non-empty string")
        selected_head = _validate_head_sha(head_sha, required=False)
        if exact_head:
            selected_head = selected_head or _git_head(identity.checkout)
            _assert_clean_head(identity.checkout, selected_head)
        selected_pid = os.getpid() if caller_pid is None else caller_pid
        if (
            not isinstance(selected_pid, int)
            or isinstance(selected_pid, bool)
            or selected_pid <= 0
        ):
            raise CoordinatorError("caller_pid must be a positive integer")
        return _PreparedSubmission(
            identity=identity,
            branch=selected_branch,
            head_sha=selected_head,
            caller_pid=selected_pid,
            environment=_validate_environment(environment),
        )

    def _native_submit(
        self,
        command: Sequence[str],
        *,
        kind: str,
        label: str,
        resources: Mapping[str, int] | None,
        agent: str | None,
        prepared: _PreparedSubmission,
        owner: Mapping[str, Any],
    ) -> str:
        run_id = f"{kind}-{uuid4().hex[:12]}"
        arguments = self._native_submission_arguments(
            run_id=run_id,
            kind=kind,
            label=label,
            identity=prepared.identity,
            branch=prepared.branch,
            head_sha=prepared.head_sha,
            resources=_validate_resources(resources, owner["capacities"]),
            agent=_agent_identity(agent),
            caller_pid=prepared.caller_pid,
            environment=prepared.environment,
            command=command,
        )
        result = self._native_invoke("submit", arguments)
        if not isinstance(result, dict) or result != {"run_id": run_id}:
            raise CoordinatorError("native broker returned an invalid submission receipt")
        return run_id

    @staticmethod
    def _native_submission_arguments(
        *,
        run_id: str,
        kind: str,
        label: str,
        identity: RepositoryIdentity,
        branch: str,
        head_sha: str | None,
        resources: Mapping[str, int],
        agent: str,
        caller_pid: int,
        environment: Mapping[str, str],
        command: Sequence[str],
        publication: tuple[str, object] | None = None,
        gate_run_id: str | None = None,
    ) -> list[str]:
        arguments = [
            "--run-id",
            run_id,
            "--kind",
            kind,
            "--label",
            label,
            "--agent",
            agent,
            "--repository-id",
            identity.repository_id,
            "--repository",
            identity.repository,
            "--worktree-id",
            identity.worktree_id,
            "--checkout",
            str(identity.checkout),
            "--branch",
            branch,
            "--caller-pid",
            str(caller_pid),
        ]
        if head_sha is not None:
            arguments.extend(("--head", head_sha))
        if gate_run_id is not None:
            arguments.extend(("--gate-run-id", gate_run_id))
        if publication is not None:
            adapter, request = publication
            arguments.extend(
                (
                    "--publication-adapter",
                    adapter,
                    "--publication-request-json",
                    json.dumps(request, separators=(",", ":")),
                )
            )
        for name, units in resources.items():
            arguments.extend(("--resource", f"{name}={units}"))
        for name, value in environment.items():
            arguments.extend(("--env", f"{name}={value}"))
        arguments.append("--")
        arguments.extend(command)
        return arguments

    def submit_merge(
        self,
        adapter: str,
        request: object,
        *,
        checkout: str,
        gate_run_id: str | None = None,
        resources: Mapping[str, int] | None = None,
        agent: str | None = None,
        repository: str | None = None,
        branch: str | None = None,
        head_sha: str | None = None,
        caller_pid: int | None = None,
        environment: Mapping[str, str] | None = None,
    ) -> str:
        if adapter != "github":
            raise CoordinatorError(f"unknown publication adapter {adapter!r}")
        if not isinstance(request, int) or isinstance(request, bool) or request <= 0:
            raise CoordinatorError(
                "the GitHub publication request must be a positive PR number"
            )
        if gate_run_id is not None and (
            not isinstance(gate_run_id, str) or not gate_run_id
        ):
            raise CoordinatorError("gate_run_id must be a non-empty string")
        prepared = self._prepare_submission(
            checkout=checkout,
            repository=repository,
            branch=branch,
            head_sha=head_sha,
            caller_pid=caller_pid,
            environment=environment,
            exact_head=True,
        )
        owner = self._ensure_broker(for_submission=True)
        if owner["protocol"] == NATIVE_PROTOCOL:
            return self._native_submit_merge(
                adapter,
                request,
                gate_run_id=gate_run_id,
                resources=resources,
                agent=agent,
                prepared=prepared,
                owner=owner,
            )
        return self._catalogue().submit_merge(
            adapter,
            request,
            checkout=checkout,
            gate_run_id=gate_run_id,
            resources=resources,
            agent=agent,
            repository=repository,
            branch=branch,
            head_sha=head_sha,
            caller_pid=caller_pid,
            environment=environment,
            worker_python=sys.executable,
        )

    def _native_submit_merge(
        self,
        adapter: str,
        request: int,
        *,
        gate_run_id: str | None,
        resources: Mapping[str, int] | None,
        agent: str | None,
        prepared: _PreparedSubmission,
        owner: Mapping[str, Any],
    ) -> str:
        executable = str(Path(sys.executable).expanduser().absolute())
        if not Path(executable).is_file():
            raise CoordinatorError(f"merge worker Python does not exist: {executable}")
        run_id = f"merge-{uuid4().hex[:12]}"
        worker = [
            executable,
            "-m",
            "agcoord.github",
            "--run-id",
            run_id,
            "--state-dir",
            str(self.paths.state_dir),
            "--checkout",
            str(prepared.identity.checkout),
            "--branch",
            prepared.branch,
            "--head-sha",
            prepared.exact_head(),
            str(request),
        ]
        arguments = self._native_submission_arguments(
            run_id=run_id,
            kind="merge",
            label=f"merge GitHub PR #{request}",
            identity=prepared.identity,
            branch=prepared.branch,
            head_sha=prepared.exact_head(),
            resources=_validate_resources(resources, owner["capacities"]),
            agent=_agent_identity(agent),
            caller_pid=prepared.caller_pid,
            environment=prepared.environment,
            publication=(adapter, request),
            gate_run_id=gate_run_id,
            command=worker,
        )
        result = self._native_invoke("submit", arguments)
        if not isinstance(result, dict) or result != {"run_id": run_id}:
            raise CoordinatorError("native broker returned an invalid merge receipt")
        return run_id

    def submit_land(
        self,
        adapter: str,
        request: object,
        command: Sequence[str],
        *,
        checkout: str,
        label: str = "land",
        resources: Mapping[str, int] | None = None,
        agent: str | None = None,
        repository: str | None = None,
        branch: str | None = None,
        head_sha: str | None = None,
        caller_pid: int | None = None,
        environment: Mapping[str, str] | None = None,
        synchronize_target: bool = True,
        avoid_commits: Sequence[str] = (),
    ) -> str:
        selected_command = _validate_command(command)
        if adapter != "github":
            raise CoordinatorError(f"unknown publication adapter {adapter!r}")
        if not isinstance(request, int) or isinstance(request, bool) or request <= 0:
            raise CoordinatorError(
                "the GitHub publication request must be a positive PR number"
            )
        if not isinstance(label, str) or not label.strip():
            raise CoordinatorError("label must be a non-empty string")
        if not isinstance(synchronize_target, bool):
            raise CoordinatorError("synchronize_target must be boolean")
        selected_avoid = _validate_avoid_commits(avoid_commits)
        prepared = self._prepare_submission(
            checkout=checkout,
            repository=repository,
            branch=branch,
            head_sha=head_sha,
            caller_pid=caller_pid,
            environment=environment,
            exact_head=True,
        )
        owner = self._ensure_broker(for_submission=True)
        if owner["protocol"] == NATIVE_PROTOCOL:
            return self._native_submit_land(
                adapter,
                request,
                selected_command,
                label=label.strip(),
                resources=resources,
                agent=agent,
                prepared=prepared,
                synchronize_target=synchronize_target,
                avoid_commits=selected_avoid,
                owner=owner,
            )
        return self._catalogue().submit_land(
            adapter,
            request,
            command,
            checkout=checkout,
            label=label,
            resources=resources,
            agent=agent,
            repository=repository,
            branch=branch,
            head_sha=head_sha,
            caller_pid=caller_pid,
            environment=environment,
            synchronize_target=synchronize_target,
            avoid_commits=selected_avoid,
        )

    def _native_submit_land(
        self,
        adapter: str,
        request: int,
        command: Sequence[str],
        *,
        label: str,
        resources: Mapping[str, int] | None,
        agent: str | None,
        prepared: _PreparedSubmission,
        synchronize_target: bool,
        avoid_commits: Sequence[str],
        owner: Mapping[str, Any],
    ) -> str:
        selected_environment = dict(prepared.environment)
        selected_avoid = _validate_avoid_commits(avoid_commits)
        if LAND_TARGET_SYNC_ENV in selected_environment:
            raise CoordinatorError(
                f"gate environment uses the reserved {LAND_TARGET_SYNC_ENV} name"
            )
        selected_environment[LAND_TARGET_SYNC_ENV] = (
            "1" if synchronize_target else "0"
        )
        if LAND_AVOID_ENV in selected_environment:
            raise CoordinatorError(
                f"gate environment uses the reserved {LAND_AVOID_ENV} name"
            )
        if selected_avoid:
            selected_environment[LAND_AVOID_ENV] = ",".join(selected_avoid)
        if "_AGCOORD_LAND_PYTHON" in selected_environment:
            raise CoordinatorError(
                "gate environment uses the reserved _AGCOORD_LAND_PYTHON name"
            )
        executable = str(Path(sys.executable).expanduser().absolute())
        if not Path(executable).is_file():
            raise CoordinatorError(f"land worker Python does not exist: {executable}")
        selected_environment["_AGCOORD_LAND_PYTHON"] = executable
        run_id = f"land-{uuid4().hex[:12]}"
        arguments = self._native_submission_arguments(
            run_id=run_id,
            kind="land",
            label=label,
            identity=prepared.identity,
            branch=prepared.branch,
            head_sha=prepared.exact_head(),
            resources=_validate_resources(resources, owner["capacities"]),
            agent=_agent_identity(agent),
            caller_pid=prepared.caller_pid,
            environment=selected_environment,
            publication=(adapter, request),
            command=command,
        )
        result = self._native_invoke("submit", arguments)
        if not isinstance(result, dict) or result != {"run_id": run_id}:
            raise CoordinatorError("native broker returned an invalid land receipt")
        return run_id

    def snapshot(self) -> dict[str, Any]:
        maintenance = self._maintenance_if_active()
        if maintenance is not None:
            maintenance = self._recover_native_drain(maintenance)
            if maintenance["protocol"] == PROTOCOL:
                return self._catalogue().snapshot()
            if maintenance["protocol"] == NATIVE_PROTOCOL:
                result = self._native_invoke("snapshot")
                if not isinstance(result, dict):
                    raise CoordinatorError("native broker returned an invalid snapshot")
                return result
        owner = self._ensure_broker()
        if owner["protocol"] == NATIVE_PROTOCOL:
            result = self._native_invoke("snapshot")
            if not isinstance(result, dict):
                raise CoordinatorError("native broker returned an invalid snapshot")
            return result
        return self._catalogue().snapshot()

    def status(self, run_id: str) -> dict[str, Any]:
        return self._status(run_id, admitted_callback=False)

    def admitted_run_status(self, run_id: str) -> dict[str, Any]:
        """Read only the exact parent run through the admitted callback boundary."""
        self._assert_admitted_callback(run_id)
        return self._status(run_id, admitted_callback=True)

    def _status(
        self,
        run_id: str,
        *,
        admitted_callback: bool,
    ) -> dict[str, Any]:
        if admitted_callback:
            owner = self._ensure_broker(admitted_callback=True)
            if owner["protocol"] == NATIVE_PROTOCOL:
                result = self._native_callback_invoke(
                    "status",
                    ("--run-id", run_id),
                )
                if not isinstance(result, dict) or result.get("run_id") != run_id:
                    raise CoordinatorError("native broker returned an invalid run status")
                return result
            return self._catalogue().status(run_id)
        maintenance = self._maintenance_if_active()
        if maintenance is not None:
            maintenance = self._recover_native_drain(maintenance)
            if maintenance["protocol"] == PROTOCOL:
                return self._catalogue().status(run_id)
            if maintenance["protocol"] == NATIVE_PROTOCOL:
                result = self._native_invoke("status", ("--run-id", run_id))
                if not isinstance(result, dict):
                    raise CoordinatorError("native broker returned an invalid run status")
                return result
        owner = self._ensure_broker()
        if owner["protocol"] == NATIVE_PROTOCOL:
            result = self._native_invoke("status", ("--run-id", run_id))
            if not isinstance(result, dict):
                raise CoordinatorError("native broker returned an invalid run status")
            return result
        return self._catalogue().status(run_id)

    def cancel(self, run_id: str) -> dict[str, Any]:
        maintenance = self._maintenance_if_active()
        if maintenance is not None:
            maintenance = self._recover_native_drain(maintenance)
            if maintenance["protocol"] == PROTOCOL:
                return self._catalogue().cancel(run_id)
            if maintenance["protocol"] == NATIVE_PROTOCOL:
                result = self._native_invoke("cancel", ("--run-id", run_id))
                if not isinstance(result, dict):
                    raise CoordinatorError(
                        "native broker returned an invalid cancellation receipt"
                    )
                return result
        owner = self._ensure_broker()
        if owner["protocol"] == NATIVE_PROTOCOL:
            result = self._native_invoke("cancel", ("--run-id", run_id))
            if not isinstance(result, dict):
                raise CoordinatorError("native broker returned an invalid cancellation receipt")
            return result
        return self._catalogue().cancel(run_id)

    def clear(self) -> dict[str, int]:
        maintenance = self._maintenance_if_active()
        if maintenance is not None:
            raise CoordinatorError(
                f"cannot clear history while the coordinator is {maintenance['state']}"
            )
        owner = self._ensure_broker()
        if owner["protocol"] == NATIVE_PROTOCOL:
            result = self._native_invoke("clear")
            if (
                not isinstance(result, dict)
                or set(result) != {"cleared"}
                or not isinstance(result["cleared"], int)
            ):
                raise CoordinatorError("native broker returned an invalid clear receipt")
            return result
        return self._catalogue().clear()

    def migrate(self) -> dict[str, Any]:
        """Run the selected executable's explicit idle-spool migration."""
        if _read_broker_owner(self.paths) is not None:
            raise CoordinatorError(
                "cannot migrate while a gate broker owns the state directory"
            )
        result = self._native_invoke("migrate")
        if not isinstance(result, dict):
            raise CoordinatorError("native broker returned an invalid migration receipt")
        return result

    def drain(
        self,
        *,
        reason: str = "maintenance",
        wait: bool = True,
        poll_interval: float = 0.1,
    ) -> dict[str, Any]:
        """Atomically reject new submissions and optionally wait for ownership yield."""
        if (
            not isinstance(reason, str)
            or not reason.strip()
            or len(reason.strip()) > MAX_MAINTENANCE_REASON
            or "\0" in reason
        ):
            raise CoordinatorError(
                f"maintenance reason must be 1 to {MAX_MAINTENANCE_REASON} characters"
            )
        if not isinstance(wait, bool):
            raise CoordinatorError("maintenance wait must be boolean")
        if poll_interval <= 0:
            raise CoordinatorError("maintenance poll interval must be positive")
        protocol = _spool_protocol(self.paths)
        if protocol is None:
            raise CoordinatorError(
                f"no gate queue database exists at {self.paths.database}"
            )
        drain_id = f"drain-{uuid4().hex[:12]}"
        if protocol == PROTOCOL:
            result = _legacy_begin_drain(
                self.paths,
                drain_id=drain_id,
                reason=reason.strip(),
            )
        elif protocol == NATIVE_PROTOCOL:
            result = self._native_invoke(
                "drain",
                ("--drain-id", drain_id, "--reason", reason.strip()),
            )
        else:
            raise CoordinatorError(
                f"durable draining does not support queue protocol {protocol}"
            )
        result = _validated_maintenance_receipt(result)
        if not wait:
            return result
        while result.get("state") != "drained":
            result = self._recover_native_drain(result)
            if result["state"] == "drained":
                break
            time.sleep(poll_interval)
            result = self.drain_status()
        return result

    def drain_status(self) -> dict[str, Any]:
        """Return the validated durable drain status without starting a broker."""
        protocol = _spool_protocol(self.paths)
        if protocol == PROTOCOL:
            result = _legacy_maintenance_status(self.paths, transition=True)
        elif protocol == NATIVE_PROTOCOL:
            result = self._native_invoke("drain-status")
        elif protocol is None:
            raise CoordinatorError(
                f"no gate queue database exists at {self.paths.database}"
            )
        else:
            raise CoordinatorError(
                f"durable draining does not support queue protocol {protocol}"
            )
        return _validated_maintenance_receipt(result)

    def resume(self, drain_id: str) -> dict[str, Any]:
        """Remove one exact drained guard while holding exclusive spool ownership."""
        if not isinstance(drain_id, str) or not _DRAIN_ID.fullmatch(drain_id):
            raise CoordinatorError("maintenance drain ID is invalid")
        protocol = _spool_protocol(self.paths)
        if protocol == PROTOCOL:
            result = _legacy_resume(self.paths, drain_id)
        elif protocol == NATIVE_PROTOCOL:
            result = self._native_invoke(
                "resume",
                ("--drain-id", drain_id),
            )
        elif protocol is None:
            raise CoordinatorError(
                f"no gate queue database exists at {self.paths.database}"
            )
        else:
            raise CoordinatorError(
                f"durable draining does not support queue protocol {protocol}"
            )
        if result != {"state": "open", "drain_id": drain_id, "resumed": True}:
            raise CoordinatorError("coordinator returned an invalid resume receipt")
        return result

    def acquire_child_cpu_lease(
        self,
        requested: int,
        *,
        minimum: int | None = None,
        timeout: float | None = None,
        run_id: str | None = None,
        poll_interval: float = CHILD_LEASE_POLL_SECONDS,
    ) -> ChildCpuLease:
        """Wait for an exact or partial CPU-token grant inside the admitted worker tree."""
        if timeout is not None and (
            not isinstance(timeout, (int, float))
            or isinstance(timeout, bool)
            or timeout < 0
        ):
            raise CoordinatorError("child CPU lease timeout must be non-negative or null")
        if poll_interval <= 0:
            raise CoordinatorError("child CPU lease poll interval must be positive")
        marker = self._admitted_callback_run_id()
        selected_run = marker if run_id is None else run_id
        if marker != selected_run:
            raise CoordinatorError(
                "child CPU lease parent does not match the admitted run context"
            )
        selected_minimum = requested if minimum is None else minimum
        owner = self._ensure_broker(admitted_callback=True)
        if owner["protocol"] == NATIVE_PROTOCOL:
            if (
                not isinstance(requested, int)
                or isinstance(requested, bool)
                or requested <= 0
            ):
                raise CoordinatorError("child CPU lease request must be a positive integer")
            if (
                not isinstance(selected_minimum, int)
                or isinstance(selected_minimum, bool)
                or selected_minimum <= 0
                or selected_minimum > requested
            ):
                raise CoordinatorError(
                    "child CPU lease minimum must be positive and no greater than requested"
                )
            owner_pid = os.getpid()
            owner_token = _process_start_token(owner_pid)
            if owner_token is None:
                raise CoordinatorError("cannot identify child CPU lease owner process")
            lease_id = f"cpu-lease-{uuid4().hex[:12]}"
            row = self._native_callback_invoke(
                "lease-request",
                (
                    "--lease-id",
                    lease_id,
                    "--run-id",
                    selected_run,
                    "--requested",
                    str(requested),
                    "--minimum",
                    str(selected_minimum),
                    "--owner-pid",
                    str(owner_pid),
                    "--owner-start-token",
                    owner_token,
                ),
            )
            if not isinstance(row, dict) or row.get("lease_id") != lease_id:
                raise CoordinatorError("native broker returned an invalid child lease receipt")
        else:
            row = self._catalogue().request_child_cpu_lease(
                selected_run,
                requested=requested,
                minimum=selected_minimum,
            )
        lease_id = row["lease_id"]
        deadline = None if timeout is None else time.monotonic() + timeout
        try:
            while row["status"] == "waiting":
                if deadline is not None and time.monotonic() >= deadline:
                    self.cancel_child_cpu_lease(lease_id)
                    raise CoordinatorError(
                        f"timed out waiting for child CPU lease {lease_id}"
                    )
                time.sleep(poll_interval)
                row = self._child_cpu_lease_status(lease_id)
        except BaseException:
            try:
                current = self._child_cpu_lease_status(lease_id)
                if current["status"] in {"waiting", "active"}:
                    self.cancel_child_cpu_lease(lease_id)
            except CoordinatorError:
                pass
            raise
        if row["status"] != "active":
            raise CoordinatorError(
                f"child CPU lease {lease_id} ended as {row['status']} before grant"
            )
        return ChildCpuLease(
            lease_id=lease_id,
            run_id=row["run_id"],
            requested=row["requested"],
            minimum=row["minimum"],
            granted=row["granted"],
            _client=self,
        )

    def _child_cpu_lease_status(self, lease_id: str) -> dict[str, Any]:
        run_id = self._admitted_callback_run_id()
        owner = self._ensure_broker(admitted_callback=True)
        if owner["protocol"] == NATIVE_PROTOCOL:
            result = self._native_callback_invoke(
                "lease-status",
                ("--lease-id", lease_id),
            )
            if (
                not isinstance(result, dict)
                or result.get("lease_id") != lease_id
                or result.get("run_id") != run_id
            ):
                raise CoordinatorError("native broker returned an invalid child lease status")
            return result
        result = self._catalogue().child_cpu_lease_status(lease_id)
        if result.get("run_id") != run_id:
            raise CoordinatorError("child CPU lease does not belong to the admitted run")
        return result

    def child_cpu_leases(
        self,
        run_id: str | None = None,
        *,
        include_terminal: bool = False,
    ) -> list[dict[str, Any]]:
        owner = self._ensure_broker()
        if owner["protocol"] == NATIVE_PROTOCOL:
            arguments: list[str] = []
            if run_id is not None:
                if not isinstance(run_id, str) or not run_id:
                    raise CoordinatorError("child CPU lease parent run ID must be non-empty")
                arguments.extend(("--run-id", run_id))
            if include_terminal:
                arguments.append("--include-terminal")
            result = self._native_invoke("lease-list", arguments)
            if not isinstance(result, list) or any(
                not isinstance(row, dict) for row in result
            ):
                raise CoordinatorError("native broker returned an invalid child lease list")
            return result
        return self._catalogue().child_cpu_leases(
            run_id,
            include_terminal=include_terminal,
        )

    def release_child_cpu_lease(self, lease_id: str) -> dict[str, Any]:
        self._admitted_callback_run_id()
        owner = self._ensure_broker(admitted_callback=True)
        if owner["protocol"] == NATIVE_PROTOCOL:
            return self._native_finish_child_lease(lease_id, "lease-release")
        return self._catalogue().release_child_cpu_lease(lease_id)

    def cancel_child_cpu_lease(self, lease_id: str) -> dict[str, Any]:
        self._admitted_callback_run_id()
        owner = self._ensure_broker(admitted_callback=True)
        if owner["protocol"] == NATIVE_PROTOCOL:
            return self._native_finish_child_lease(lease_id, "lease-cancel")
        return self._catalogue().cancel_child_cpu_lease(lease_id)

    def _native_finish_child_lease(
        self,
        lease_id: str,
        command: str,
    ) -> dict[str, Any]:
        run_id = self._admitted_callback_run_id()
        if not isinstance(lease_id, str) or not lease_id:
            raise CoordinatorError("child CPU lease ID must be non-empty")
        owner_pid = os.getpid()
        owner_token = _process_start_token(owner_pid)
        if owner_token is None:
            raise CoordinatorError("cannot identify child CPU lease owner process")
        result = self._native_callback_invoke(
            command,
            (
                "--lease-id",
                lease_id,
                "--owner-pid",
                str(owner_pid),
                "--owner-start-token",
                owner_token,
            ),
        )
        if (
            not isinstance(result, dict)
            or result.get("lease_id") != lease_id
            or result.get("run_id") != run_id
        ):
            raise CoordinatorError("native broker returned an invalid child lease receipt")
        return result

    def verify_admission(
        self,
        run_id: str,
        *,
        kind: str,
        checkout: str,
        head_sha: str,
        worker_pid: int,
    ) -> None:
        self._assert_admitted_callback(run_id)
        owner = self._ensure_broker(admitted_callback=True)
        if owner["protocol"] == NATIVE_PROTOCOL:
            if kind not in {"full", "merge", "land"}:
                raise CoordinatorError(
                    "broker admission kind must be 'full', 'merge', or 'land'"
                )
            token = _process_start_token(worker_pid)
            if token is None:
                raise CoordinatorError("cannot identify admitted worker process")
            result = self._native_callback_invoke(
                "verify",
                (
                    "--run-id",
                    run_id,
                    "--kind",
                    kind,
                    "--worker-pid",
                    str(worker_pid),
                    "--worker-start-token",
                    token,
                    "--checkout",
                    str(_absolute(checkout)),
                    "--head",
                    _validate_head_sha(head_sha, required=True) or "",
                ),
            )
            if result != {"verified": True}:
                raise CoordinatorError("native broker returned an invalid admission receipt")
            return
        self._catalogue().verify_admission(
            run_id,
            kind=kind,
            checkout=checkout,
            head_sha=head_sha,
            worker_pid=worker_pid,
        )

    def update_land_phase(
        self,
        run_id: str,
        *,
        phase: str,
        gate_exit_status: int | None,
        worker_pid: int,
        new_head_sha: str | None = None,
    ) -> None:
        self._assert_admitted_callback(run_id)
        owner = self._ensure_broker(admitted_callback=True)
        if owner["protocol"] == NATIVE_PROTOCOL:
            token = _process_start_token(worker_pid)
            if token is None:
                raise CoordinatorError("cannot identify admitted land worker process")
            row = self.admitted_run_status(run_id)
            arguments = [
                "--run-id",
                run_id,
                "--worker-pid",
                str(worker_pid),
                "--worker-start-token",
                token,
                "--checkout",
                str(row["checkout"]),
                "--head",
                str(row["head_sha"]),
                "--phase",
                phase,
            ]
            if gate_exit_status is not None:
                arguments.extend(("--gate-exit-status", str(gate_exit_status)))
            if new_head_sha is not None:
                arguments.extend(
                    (
                        "--new-head",
                        _validate_head_sha(new_head_sha, required=True) or "",
                    )
                )
            result = self._native_callback_invoke("phase", arguments)
            if not isinstance(result, dict) or result.get("run_id") != run_id:
                raise CoordinatorError("native broker returned an invalid land phase receipt")
            return
        self._catalogue().update_land_phase(
            run_id,
            phase=phase,
            gate_exit_status=gate_exit_status,
            worker_pid=worker_pid,
            new_head_sha=new_head_sha,
        )

    def report_land_result(
        self,
        run_id: str,
        *,
        exit_status: int,
        worker_pid: int,
    ) -> None:
        self._assert_admitted_callback(run_id)
        owner = self._ensure_broker(admitted_callback=True)
        if owner["protocol"] == NATIVE_PROTOCOL:
            token = _process_start_token(worker_pid)
            if token is None:
                raise CoordinatorError("cannot identify admitted land worker process")
            result = self._native_callback_invoke(
                "report",
                (
                    "--run-id",
                    run_id,
                    "--worker-pid",
                    str(worker_pid),
                    "--worker-start-token",
                    token,
                    "--exit-status",
                    str(exit_status),
                ),
            )
            if result != {"reported": True}:
                raise CoordinatorError("native broker returned an invalid land result receipt")
            return
        self._catalogue().report_land_result(
            run_id,
            exit_status=exit_status,
            worker_pid=worker_pid,
        )

    def log(
        self,
        run_id: str,
        *,
        offset: int = 0,
        limit: int = MAX_LOG_BYTES,
    ) -> dict[str, Any]:
        if not isinstance(offset, int) or isinstance(offset, bool) or offset < 0:
            raise CoordinatorError("gate log offset must be a non-negative integer")
        if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= MAX_LOG_BYTES:
            raise CoordinatorError(
                f"gate log limit must be between 1 and {MAX_LOG_BYTES}"
            )
        maintenance = self._maintenance_if_active()
        if maintenance is not None:
            maintenance = self._recover_native_drain(maintenance)
            if maintenance["protocol"] == PROTOCOL:
                return self._catalogue().log(run_id, offset=offset, limit=limit)
            if maintenance["protocol"] == NATIVE_PROTOCOL:
                result = self._native_invoke(
                    "log",
                    (
                        "--run-id",
                        run_id,
                        "--offset",
                        str(offset),
                        "--limit",
                        str(limit),
                    ),
                )
                if not isinstance(result, dict):
                    raise CoordinatorError("native broker returned an invalid log page")
                return result
        owner = self._ensure_broker()
        if owner["protocol"] == NATIVE_PROTOCOL:
            result = self._native_invoke(
                "log",
                (
                    "--run-id",
                    run_id,
                    "--offset",
                    str(offset),
                    "--limit",
                    str(limit),
                ),
            )
            if not isinstance(result, dict):
                raise CoordinatorError("native broker returned an invalid log page")
            return result
        return self._catalogue().log(run_id, offset=offset, limit=limit)

    def ping(self) -> dict[str, Any]:
        owner = _read_broker_owner(self.paths)
        if owner is None:
            raise CoordinatorError(
                f"no gate broker owns {self.paths.state_dir}"
            )
        if owner["protocol"] == NATIVE_PROTOCOL:
            self._validate_native_owner(owner)
        elif owner["protocol"] == PROTOCOL and self.autostart:
            raise CoordinatorError(
                "a legacy protocol-4 Python broker still owns this state directory; "
                "let it finish and stop, then run 'agc migrate' before retrying"
            )
        elif owner["protocol"] != PROTOCOL:
            raise CoordinatorError(
                f"gate coordinator protocol mismatch: broker has {owner['protocol']}"
            )
        return self._public_owner(owner)

    def _start_broker(self) -> None:
        try:
            existing_protocol = _spool_protocol(self.paths)
        except CoordinatorError as exc:
            if not _spool_initializing_error(exc):
                raise
            deadline = time.monotonic() + self.connect_timeout
            while True:
                try:
                    starting_owner = _read_broker_owner(self.paths)
                except _OwnerMetadataError:
                    starting_owner = None
                if starting_owner is not None:
                    return
                if time.monotonic() >= deadline:
                    raise exc
                time.sleep(0.01)
        if existing_protocol is not None and existing_protocol != NATIVE_PROTOCOL:
            if 1 <= existing_protocol <= PROTOCOL:
                raise CoordinatorError(
                    f"gate queue uses protocol {existing_protocol}; run 'agc migrate' "
                    "while it is idle before starting the native broker"
                )
            raise CoordinatorError(
                f"gate queue protocol {existing_protocol} is unsupported by this client"
            )
        selected = self._native_command()
        config = broker_config(self.paths.state_dir)
        capacities = configured_capacities(config.capacities)
        if config.native_broker.managed_service:
            if "AGCOORD_STATE_DIR" in os.environ or self.paths.state_dir != state_dir_for():
                raise CoordinatorError(
                    "the managed native service owns only the default XDG state directory; "
                    "use an unmanaged development configuration for an isolated spool"
                )
            try:
                completed = subprocess.run(
                    [
                        "/usr/bin/systemctl",
                        "--user",
                        "start",
                        "agcoord-broker.service",
                    ],
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    check=False,
                    timeout=30,
                )
            except (OSError, subprocess.TimeoutExpired) as exc:
                raise CoordinatorError(
                    f"cannot start managed native broker service: {exc}"
                ) from exc
            if completed.returncode != 0:
                detail = completed.stderr.decode("utf-8", errors="replace").strip()
                raise CoordinatorError(
                    "cannot start managed native broker service"
                    + (f": {detail}" if detail else "")
                )
            self._wait_for_broker_start()
            return
        try:
            self.paths.state_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
            details = self.paths.state_dir.lstat()
        except OSError as exc:
            raise CoordinatorError(
                f"cannot prepare gate queue directory {self.paths.state_dir}: {exc}"
            ) from exc
        if stat.S_ISLNK(details.st_mode) or not stat.S_ISDIR(details.st_mode):
            raise CoordinatorError(
                f"gate queue path is not a real directory: {self.paths.state_dir}"
            )
        if details.st_uid != os.getuid():
            raise CoordinatorError(
                f"gate queue directory {self.paths.state_dir} belongs to another user"
            )
        if details.st_mode & 0o077:
            self.paths.state_dir.chmod(0o700)
        flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            descriptor = os.open(self.paths.daemon_log, flags, 0o600)
        except OSError as exc:
            raise CoordinatorError(
                f"cannot open gate broker log {self.paths.daemon_log}: {exc}"
            ) from exc
        try:
            log_details = os.fstat(descriptor)
            if not stat.S_ISREG(log_details.st_mode) or log_details.st_uid != os.getuid():
                raise CoordinatorError(
                    f"gate broker log is not a current-user regular file: "
                    f"{self.paths.daemon_log}"
                )
            os.fchmod(descriptor, 0o600)
            try:
                subprocess.Popen(
                    selected.serve_arguments(self.paths.state_dir, capacities),
                    stdin=subprocess.DEVNULL,
                    stdout=descriptor,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                    close_fds=True,
                )
            except OSError as exc:
                raise CoordinatorError(f"cannot start gate coordinator: {exc}") from exc
        finally:
            os.close(descriptor)

        self._wait_for_broker_start()

    def _wait_for_broker_start(self) -> None:
        deadline = time.monotonic() + self.connect_timeout
        last_error: CoordinatorError | None = None
        while time.monotonic() < deadline:
            try:
                self.ping()
                return
            except CoordinatorError as exc:
                last_error = exc
                try:
                    maintenance = self._maintenance_if_active()
                except CoordinatorError as maintenance_error:
                    if not _spool_initializing_error(maintenance_error):
                        raise
                    # The native process creates SQLite before its schema transaction
                    # commits. Treat only that bounded partial-spool window like the
                    # equally brief owner-metadata window handled by ping().
                    last_error = maintenance_error
                    time.sleep(0.05)
                    continue
                if (
                    maintenance is not None
                    and maintenance["state"] == "drained"
                    and maintenance["live"] == 0
                ):
                    return
                time.sleep(0.05)
        detail = f": {last_error}" if last_error else ""
        raise CoordinatorError(
            f"gate coordinator did not start; inspect {self.paths.daemon_log}{detail}"
        )


def follow(
    client: CoordinatorClient,
    run_id: str,
    *,
    out=sys.stdout,
    err=sys.stderr,
) -> int:
    """Print a submitted gate's one log stream and return its exact verdict."""
    offset = 0
    previous_status = ""
    try:
        while True:
            row = client.status(run_id)
            status = row["status"]
            if status != previous_status:
                if status == "queued":
                    print(
                        f"Gate queue: {run_id} waiting at position {row['position']} "
                        f"for branch {row['branch']}",
                        file=out,
                        flush=True,
                    )
                elif status == "running":
                    print(
                        f"Gate queue: {run_id} running as pid {row['worker_pid']} "
                        f"in {row['checkout']}",
                        file=out,
                        flush=True,
                    )
                previous_status = status
            page = client.log(run_id, offset=offset)
            if page["text"]:
                print(page["text"], end="", file=out, flush=True)
            offset = page["next_offset"]
            if status in TERMINAL_STATUSES and page["eof"]:
                return int(row["exit_status"] if row["exit_status"] is not None else 70)
            time.sleep(0.1)
    except KeyboardInterrupt:
        try:
            client.cancel(run_id)
            print(f"\nGate queue: cancellation requested for {run_id}", file=err)
        except CoordinatorError as exc:
            print(f"\nGate queue: could not cancel {run_id}: {exc}", file=err)
        return 130


def wait(client: CoordinatorClient, run_id: str, *, poll_interval: float = 0.1) -> dict[str, Any]:
    """Wait without consuming the log, for a script that wants one strict final row."""
    if poll_interval <= 0:
        raise ValueError("poll_interval must be positive")
    while True:
        row = client.status(run_id)
        if row["status"] in TERMINAL_STATUSES:
            return row
        time.sleep(poll_interval)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="agc queue")
    commands = parser.add_subparsers(dest="command_name", required=True)
    serve = commands.add_parser("serve", help=argparse.SUPPRESS)
    serve.add_argument("--state-dir", required=True)
    serve.add_argument("--idle-seconds", type=float, default=DEFAULT_IDLE_SECONDS)
    submit = commands.add_parser("submit", help=argparse.SUPPRESS)
    submit.add_argument("--state-dir")
    submit.add_argument("--checkout", required=True)
    submit.add_argument("--branch")
    submit.add_argument("--caller-pid", type=int)
    submit.add_argument("--agent")
    submit.add_argument("--repository")
    submit.add_argument("--resources-json", default="{}")
    submit.add_argument("--kind", choices=("check", "full"), default="check")
    submit.add_argument("--head-sha")
    submit.add_argument("--label", default="run")
    submit.add_argument("worker_command", nargs=argparse.REMAINDER)
    verify = commands.add_parser("verify-admission", help=argparse.SUPPRESS)
    verify.add_argument("--state-dir")
    verify.add_argument("--checkout", required=True)
    verify.add_argument("--run-id", required=True)
    verify.add_argument("--kind", choices=("full", "merge", "land"), required=True)
    verify.add_argument("--head-sha", required=True)
    verify.add_argument("--worker-pid", type=int, required=True)
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    if args.command_name == "serve":
        try:
            broker = CoordinatorBroker(
                args.state_dir,
                idle_timeout=args.idle_seconds,
            )
        except CoordinatorError as exc:
            print(f"AGCoord: invalid broker configuration: {exc}", file=sys.stderr)
            return 2

        def stop(_signum, _frame):
            threading.Thread(target=broker.close, daemon=True).start()

        signal.signal(signal.SIGTERM, stop)
        signal.signal(signal.SIGINT, stop)
        try:
            broker.serve_forever()
        except CoordinatorError as exc:
            print(f"AGCoord: {exc}", file=sys.stderr)
            return 2
        return 0

    if args.command_name == "verify-admission":
        try:
            client = CoordinatorClient(
                state_dir=args.state_dir,
                checkout=args.checkout,
                autostart=False,
            )
            client.verify_admission(
                args.run_id,
                kind=args.kind,
                checkout=args.checkout,
                head_sha=args.head_sha,
                worker_pid=args.worker_pid,
            )
        except CoordinatorError as exc:
            print(f"AGCoord: FAILED — {exc}", file=sys.stderr)
            return 2
        return 0

    command = list(args.worker_command)
    if command and command[0] == "--":
        command.pop(0)
    try:
        resources = json.loads(args.resources_json)
        client = CoordinatorClient(
            state_dir=args.state_dir,
            checkout=args.checkout,
            autostart=True,
        )
        run_id = client.submit(
            command,
            checkout=args.checkout,
            branch=args.branch,
            caller_pid=args.caller_pid,
            label=args.label,
            kind=args.kind,
            head_sha=args.head_sha,
            agent=args.agent,
            repository=args.repository,
            resources=resources,
        )
        print(f"AGCoord: accepted {run_id}", flush=True)
        return follow(client, run_id)
    except (CoordinatorError, json.JSONDecodeError) as exc:
        print(f"AGCoord: FAILED — {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
