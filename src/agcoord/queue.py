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
import sqlite3
import stat
import subprocess
import sys
import time
from typing import Any, Iterable, Mapping, Sequence
from uuid import uuid4

from .config import BrokerConfig, BrokerConfigError, load_broker_config
from .native_client import (
    NATIVE_IMPLEMENTATION,
    NATIVE_PROTOCOL,
    NativeBrokerCommand,
    NativeClientError,
)
from .resources import (
    ResourceBackendError,
    ResourceContractError,
    validate_resource_bindings,
    validate_resource_capabilities,
)


LAST_MIGRATING_RELEASE = "0.5.2"
TERMINAL_STATUSES = frozenset({
    "passed", "failed", "cancelled", "interrupted",
})
LIVE_STATUSES = frozenset({"queued", "running"})
STATUSES = LIVE_STATUSES | TERMINAL_STATUSES
DEFAULT_RECENT_LIMIT = 50
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


def _parent_pid(pid: int) -> int | None:
    try:
        stat = Path(f"/proc/{pid}/stat").read_text(encoding="ascii", errors="replace")
    except OSError:
        return None
    closing = stat.rfind(")")
    if closing < 0:
        return None
    fields = stat[closing + 2 :].split()
    try:
        return int(fields[1])
    except (IndexError, ValueError):
        return None


def _scratch_policy_declared(contract_json: object) -> bool:
    """Whether a run's resource contract carries a tmpfs or project-quota scratch provider."""
    try:
        contract = json.loads(contract_json) if isinstance(contract_json, str) else contract_json
    except ValueError:
        return False
    if not isinstance(contract, dict):
        return False
    return any(
        isinstance(entry, dict)
        and (entry.get("kind") == "tmpfs" or entry.get("backend") == "project-quota")
        for entry in contract.values()
    )


def _admitted_worker_mismatch(row: Mapping[str, Any], worker_pid: int) -> str | None:
    """Explain why ``worker_pid`` is not this row's admitted worker, or return None.

    Without a scratch policy the recorded worker is the command itself. Under a tmpfs or
    project-quota policy the recorded worker is the coordinator's launcher, which keeps the
    command as its direct child, so the command presents its own PID and is accepted when
    it is the live direct child of the live recorded launcher.
    """
    recorded = row["worker_pid"]
    token = row["worker_start_token"]
    if recorded == worker_pid:
        if not _same_process(worker_pid, token):
            return "worker process identity is no longer live"
        return None
    if not _scratch_policy_declared(row["resource_contract_json"]):
        return "worker PID does not match"
    if _parent_pid(worker_pid) != recorded or not _same_process(recorded, token):
        return "worker PID is neither the admitted launcher nor its live direct child"
    if _process_start_token(worker_pid) is None:
        return "worker process identity is no longer live"
    return None


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


def _pre_native_spool_refusal(
    paths: CoordinatorPaths,
    protocol: int,
) -> CoordinatorError:
    """Refuse a spool below the native protocol and name the release that migrates it.

    The Python reference broker and its in-process migrations were retired in AGCoord
    0.6.0. A spool at protocol 1 through 4 is migrated by the last release that still
    shipped them, after which the native broker owns it at protocol 5.
    """
    return CoordinatorError(
        f"gate queue at {paths.database} uses protocol {protocol}; AGCoord owns only "
        f"protocol {NATIVE_PROTOCOL} native spools and no longer migrates older ones — "
        f"install AGCoord {LAST_MIGRATING_RELEASE} to migrate this spool to the native "
        "broker, then upgrade"
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
    if value["protocol"] != NATIVE_PROTOCOL:
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
        self._native_command_instance: NativeBrokerCommand | None = None
        self._native_callback_command_instance: NativeBrokerCommand | None = None


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
            if owner["protocol"] != NATIVE_PROTOCOL:
                raise _pre_native_spool_refusal(self.paths, owner["protocol"])
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
        return self._native_submit(
            selected_command,
            kind=kind,
            label=label.strip(),
            resources=resources,
            agent=agent,
            prepared=prepared,
            owner=owner,
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
        return self._native_submit_merge(
            adapter,
            request,
            gate_run_id=gate_run_id,
            resources=resources,
            agent=agent,
            prepared=prepared,
            owner=owner,
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
            result = self._native_invoke("snapshot")
            if not isinstance(result, dict):
                raise CoordinatorError("native broker returned an invalid snapshot")
            return result
        self._ensure_broker()
        result = self._native_invoke("snapshot")
        if not isinstance(result, dict):
            raise CoordinatorError("native broker returned an invalid snapshot")
        return result

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
            self._ensure_broker(admitted_callback=True)
            result = self._native_callback_invoke(
                "status",
                ("--run-id", run_id),
            )
            if not isinstance(result, dict) or result.get("run_id") != run_id:
                raise CoordinatorError("native broker returned an invalid run status")
            return result
        maintenance = self._maintenance_if_active()
        if maintenance is not None:
            maintenance = self._recover_native_drain(maintenance)
            result = self._native_invoke("status", ("--run-id", run_id))
            if not isinstance(result, dict):
                raise CoordinatorError("native broker returned an invalid run status")
            return result
        self._ensure_broker()
        result = self._native_invoke("status", ("--run-id", run_id))
        if not isinstance(result, dict):
            raise CoordinatorError("native broker returned an invalid run status")
        return result

    def cancel(self, run_id: str) -> dict[str, Any]:
        maintenance = self._maintenance_if_active()
        if maintenance is not None:
            maintenance = self._recover_native_drain(maintenance)
            result = self._native_invoke("cancel", ("--run-id", run_id))
            if not isinstance(result, dict):
                raise CoordinatorError(
                    "native broker returned an invalid cancellation receipt"
                )
            return result
        self._ensure_broker()
        result = self._native_invoke("cancel", ("--run-id", run_id))
        if not isinstance(result, dict):
            raise CoordinatorError("native broker returned an invalid cancellation receipt")
        return result

    def clear(self) -> dict[str, int]:
        maintenance = self._maintenance_if_active()
        if maintenance is not None:
            raise CoordinatorError(
                f"cannot clear history while the coordinator is {maintenance['state']}"
            )
        self._ensure_broker()
        result = self._native_invoke("clear")
        if (
            not isinstance(result, dict)
            or set(result) != {"cleared"}
            or not isinstance(result["cleared"], int)
        ):
            raise CoordinatorError("native broker returned an invalid clear receipt")
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
        if protocol == NATIVE_PROTOCOL:
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
        if protocol == NATIVE_PROTOCOL:
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
        if protocol == NATIVE_PROTOCOL:
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
        self._ensure_broker(admitted_callback=True)
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
        self._ensure_broker(admitted_callback=True)
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
        if result.get("run_id") != run_id:
            raise CoordinatorError("child CPU lease does not belong to the admitted run")
        return result

    def child_cpu_leases(
        self,
        run_id: str | None = None,
        *,
        include_terminal: bool = False,
    ) -> list[dict[str, Any]]:
        self._ensure_broker()
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

    def release_child_cpu_lease(self, lease_id: str) -> dict[str, Any]:
        self._admitted_callback_run_id()
        self._ensure_broker(admitted_callback=True)
        return self._native_finish_child_lease(lease_id, "lease-release")

    def cancel_child_cpu_lease(self, lease_id: str) -> dict[str, Any]:
        self._admitted_callback_run_id()
        self._ensure_broker(admitted_callback=True)
        return self._native_finish_child_lease(lease_id, "lease-cancel")

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
        self._ensure_broker(admitted_callback=True)
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
        self._ensure_broker(admitted_callback=True)
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

    def report_land_result(
        self,
        run_id: str,
        *,
        exit_status: int,
        worker_pid: int,
    ) -> None:
        self._assert_admitted_callback(run_id)
        self._ensure_broker(admitted_callback=True)
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
        self._ensure_broker()
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

    def ping(self) -> dict[str, Any]:
        owner = _read_broker_owner(self.paths)
        if owner is None:
            raise CoordinatorError(
                f"no gate broker owns {self.paths.state_dir}"
            )
        if owner["protocol"] != NATIVE_PROTOCOL:
            raise _pre_native_spool_refusal(self.paths, owner["protocol"])
        self._validate_native_owner(owner)
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
            raise _pre_native_spool_refusal(self.paths, existing_protocol)
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
