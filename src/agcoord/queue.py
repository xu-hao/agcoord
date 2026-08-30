"""One owner-only machine scheduler for development agents and repositories.

Clients append strict requests to SQLite; one detached, flock-owned broker admits them
against machine resource capacities and per-repository FIFO barriers.  Workers run in
their submitted checkouts with private tmpfs roots and process-group supervision.  The
module opens no network listener and has no dependency on a product repository.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
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


PROTOCOL = 2
TERMINAL_STATUSES = frozenset({
    "passed", "failed", "cancelled", "interrupted",
})
LIVE_STATUSES = frozenset({"queued", "running"})
STATUSES = LIVE_STATUSES | TERMINAL_STATUSES
DEFAULT_RECENT_LIMIT = 50
DEFAULT_IDLE_SECONDS = 60.0
DEFAULT_JOB_CAPACITY = 2
MAX_LOG_BYTES = 64 * 1024
CANCEL_GRACE_SECONDS = 5.0
RUN_ID_ENV = "AGCOORD_RUN_ID"
RUN_KIND_ENV = "AGCOORD_RUN_KIND"
STATE_DIR_ENV = "AGCOORD_STATE_DIR"
RUN_KINDS = frozenset({"check", "full", "merge", "land"})
RUN_PHASES = frozenset({
    "queued", "running", "preflight", "gating", "publishing", "complete",
})
_RESOURCE_NAME = re.compile(r"^[a-z][a-z0-9_.:-]{0,63}$")
_WORKER_LAUNCHER = (
    "import os, sys\n"
    "release_fd = int(sys.argv[1])\n"
    "try:\n"
    "    admitted = os.read(release_fd, 1)\n"
    "finally:\n"
    "    os.close(release_fd)\n"
    "if admitted != b'1':\n"
    "    raise SystemExit(125)\n"
    "try:\n"
    "    os.execvpe(sys.argv[2], sys.argv[2:], os.environ)\n"
    "except OSError as exc:\n"
    "    print(f'AGCoord: could not exec worker: {exc}', "
    "file=sys.stderr, flush=True)\n"
    "    raise SystemExit(127)\n"
)


class CoordinatorError(RuntimeError):
    """A named local-coordinator refusal suitable for a terminal, not a traceback."""


class _OwnerMetadataError(CoordinatorError):
    """A live owner whose one startup metadata write is not readable yet or is invalid."""


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


def _process_start_token(pid: int) -> str | None:
    """Linux process identity beyond a reusable PID.

    The gate already relies on Linux ``flock``.  Field 22 of ``/proc/<pid>/stat`` is the
    process start tick and lets a restarted broker distinguish its worker from a later
    process that happened to receive the same PID.
    """
    try:
        stat = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
        fields_after_name = stat[stat.rfind(")") + 2:].split()
        return fields_after_name[19]
    except (OSError, IndexError):
        return None


def _same_process(pid: int | None, token: str | None) -> bool:
    if pid is None or token is None:
        return False
    try:
        os.kill(pid, 0)
    except (OSError, ProcessLookupError):
        return False
    return _process_start_token(pid) == token


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


def default_capacities() -> dict[str, int]:
    """Return validated machine capacities from one stable environment setting."""
    configured = os.environ.get("AGCOORD_CAPACITIES")
    if configured is None:
        return {"jobs": DEFAULT_JOB_CAPACITY}
    if not configured.strip():
        raise CoordinatorError("AGCOORD_CAPACITIES is empty")
    try:
        if configured.lstrip().startswith("{"):
            raw = json.loads(configured)
        else:
            raw = {}
            for item in configured.split(","):
                name, separator, units = item.partition("=")
                if not separator:
                    raise ValueError(f"missing '=' in {item!r}")
                raw[name.strip()] = int(units)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise CoordinatorError(
            "AGCOORD_CAPACITIES must be JSON or comma-separated name=units entries"
        ) from exc
    capacities = _positive_mapping(raw, subject="capacity", include_job=False)
    capacities.setdefault("jobs", DEFAULT_JOB_CAPACITY)
    return dict(sorted(capacities.items()))


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


def _broker_owner(paths: CoordinatorPaths) -> dict[str, Any] | None:
    """Read the live flock owner, ignoring stale bytes left after a dead broker."""
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
                raw = os.pread(descriptor, 4096, 0).decode("utf-8", errors="strict")
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
    fields: dict[str, str] = {}
    for line in raw.splitlines():
        key, separator, value = line.partition("=")
        if not separator or key in fields:
            raise _OwnerMetadataError(
                f"live gate broker wrote invalid ownership metadata in {paths.owner_lock}"
            )
        fields[key] = value
    try:
        capacities = json.loads(fields["capacities"])
        owner: dict[str, Any] = {
            "pid": int(fields["pid"]),
            "protocol": int(fields["protocol"]),
            "capacities": _positive_mapping(
                capacities,
                subject="owner capacity",
                include_job=False,
            ),
        }
    except (KeyError, TypeError, ValueError, json.JSONDecodeError, CoordinatorError) as exc:
        raise _OwnerMetadataError(
            f"live gate broker wrote incomplete ownership metadata in {paths.owner_lock}"
        ) from exc
    if owner["protocol"] != PROTOCOL:
        raise CoordinatorError(
            f"gate coordinator protocol mismatch: broker has {owner['protocol']}, "
            f"client needs {PROTOCOL}"
        )
    if owner["pid"] <= 0:
        raise _OwnerMetadataError("live gate broker wrote invalid numeric metadata")
    return owner


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

        with sqlite3.connect(paths.database, timeout=10) as db:
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
            changed = False
            if previous == 1:
                live = db.execute(
                    "SELECT run_id FROM runs WHERE status IN ('queued', 'running') "
                    "ORDER BY sequence"
                ).fetchall()
                if live:
                    raise CoordinatorError(
                        "cannot migrate protocol 1 with live runs: "
                        + ", ".join(row["run_id"] for row in live)
                    )
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
                    (str(PROTOCOL),),
                )
                changed = True
            elif previous != PROTOCOL:
                raise CoordinatorError(
                    f"queue protocol is {previous}; no migration to {PROTOCOL} is defined"
                )
        paths.database.chmod(0o600)
        return {
            "changed": changed,
            "from_protocol": previous,
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
    ):
        if recent_limit < 1:
            raise ValueError("recent_limit must be positive")
        if idle_timeout is not None and idle_timeout <= 0:
            raise ValueError("idle_timeout must be positive or None")
        self.paths = queue_paths(state_dir=state_dir)
        self.idle_timeout = idle_timeout
        self.recent_limit = recent_limit
        self.capacities = _positive_mapping(
            default_capacities() if capacities is None else capacities,
            subject="capacity",
            include_job=False,
        )
        if "jobs" not in self.capacities:
            raise ValueError("capacities must include a positive 'jobs' capacity")
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
        connection = sqlite3.connect(self.paths.database, timeout=10)
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
                    "after the old broker exits run `python -m agcoord "
                    f"migrate --state-dir {self.paths.state_dir}`"
                )
            required = {
                "kind", "phase", "agent", "repository_id", "repository", "worktree_id",
                "head_sha", "barrier", "resources_json", "gate_run_id",
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
                f"capacities={self.capacities}"
            )
        try:
            self._pump()
        finally:
            try:
                # Retain ownership until an explicit shutdown has stopped and reaped each
                # active process group. Otherwise a replacement can observe this broker's
                # unreaped child as live forever and cannot safely resume the queue.
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
        with self._db_lock, self._connect() as db:
            db.execute(
                "INSERT INTO coordinator_meta(key, value) VALUES ('last_activity', ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (str(time.time()),),
            )
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
        selected_agent = agent or os.environ.get("AGCOORD_AGENT") or f"pid:{selected_pid}"
        if not isinstance(selected_agent, str) or not selected_agent.strip():
            raise CoordinatorError("agent must be a non-empty string")
        owner = _broker_owner(self.paths)
        capacities = owner["capacities"] if owner is not None else self.capacities
        selected_resources = _validate_resources(resources, capacities)
        selected_environment = _validate_environment(environment)
        run_id = f"{kind}-{uuid4().hex[:12]}"
        with self._db_lock, self._connect() as db:
            db.execute(
                """
                INSERT INTO runs (
                    run_id, status, kind, phase, label, agent, repository_id, repository,
                    worktree_id, checkout, branch, head_sha, barrier, resources_json,
                    caller_pid, command_json, environment_json, created_at
                ) VALUES (?, 'queued', ?, 'queued', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    kind,
                    label.strip(),
                    selected_agent.strip(),
                    identity.repository_id,
                    identity.repository,
                    identity.worktree_id,
                    selected_checkout,
                    selected_branch,
                    selected_head,
                    int(kind == "full"),
                    json.dumps(selected_resources, separators=(",", ":")),
                    selected_pid,
                    json.dumps(selected_command, separators=(",", ":")),
                    json.dumps(selected_environment, separators=(",", ":")),
                    _now(),
                ),
            )
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
        selected_agent = agent or os.environ.get("AGCOORD_AGENT") or f"pid:{selected_pid}"
        if not isinstance(selected_agent, str) or not selected_agent.strip():
            raise CoordinatorError("agent must be a non-empty string")
        owner = _broker_owner(self.paths)
        capacities = owner["capacities"] if owner is not None else self.capacities
        selected_resources = _validate_resources(resources, capacities)
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
            if gate_run_id is None:
                receipt = db.execute(
                    """
                    SELECT * FROM runs
                    WHERE kind = 'full' AND status = 'passed'
                      AND repository_id = ? AND branch = ? AND head_sha = ?
                    ORDER BY sequence DESC LIMIT 1
                    """,
                    (identity.repository_id, selected_branch, selected_head),
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
            selected_receipt = receipt["run_id"]
            db.execute(
                """
                INSERT INTO runs (
                    run_id, status, kind, phase, label, agent, repository_id, repository,
                    worktree_id, checkout, branch, head_sha, barrier, resources_json,
                    gate_run_id, publication_adapter, publication_request, caller_pid,
                    command_json, environment_json, created_at
                ) VALUES (?, 'queued', 'merge', 'queued', ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    f"merge GitHub PR #{request}",
                    selected_agent.strip(),
                    identity.repository_id,
                    identity.repository,
                    identity.worktree_id,
                    selected_checkout,
                    selected_branch,
                    selected_head,
                    json.dumps(selected_resources, separators=(",", ":")),
                    selected_receipt,
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
    ) -> str:
        """Queue one indivisible fresh-base gate and exact publication."""
        selected_command = _validate_command(command)
        if adapter != "github":
            raise CoordinatorError(f"unknown publication adapter {adapter!r}")
        if not isinstance(request, int) or isinstance(request, bool) or request <= 0:
            raise CoordinatorError(
                "the GitHub publication request must be a positive PR number"
            )
        if not isinstance(label, str) or not label.strip():
            raise CoordinatorError("label must be a non-empty string")
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
        selected_agent = agent or os.environ.get("AGCOORD_AGENT") or f"pid:{selected_pid}"
        if not isinstance(selected_agent, str) or not selected_agent.strip():
            raise CoordinatorError("agent must be a non-empty string")
        owner = _broker_owner(self.paths)
        capacities = owner["capacities"] if owner is not None else self.capacities
        selected_resources = _validate_resources(resources, capacities)
        selected_environment = _validate_environment(environment)
        run_id = f"land-{uuid4().hex[:12]}"
        with self._db_lock, self._connect() as db:
            db.execute(
                """
                INSERT INTO runs (
                    run_id, status, kind, phase, label, agent, repository_id,
                    repository, worktree_id, checkout, branch, head_sha, barrier,
                    resources_json, publication_adapter, publication_request,
                    caller_pid, command_json, environment_json, created_at
                ) VALUES (?, 'queued', 'land', 'queued', ?, ?, ?, ?, ?, ?, ?, ?, 1,
                          ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    label.strip(),
                    selected_agent.strip(),
                    identity.repository_id,
                    identity.repository,
                    identity.worktree_id,
                    selected_checkout,
                    selected_branch,
                    selected_head,
                    json.dumps(selected_resources, separators=(",", ":")),
                    adapter,
                    json.dumps(request, separators=(",", ":")),
                    selected_pid,
                    json.dumps(selected_command, separators=(",", ":")),
                    json.dumps(selected_environment, separators=(",", ":")),
                    _now(),
                ),
            )
        self._touch()
        return run_id

    def snapshot(self) -> dict[str, Any]:
        owner = _broker_owner(self.paths)
        if owner is None:
            raise CoordinatorError(
                f"no gate broker owns {self.paths.state_dir}"
            )
        with self._db_lock, self._connect() as db:
            rows = db.execute("SELECT * FROM runs ORDER BY sequence").fetchall()
        queued_rows = [row for row in rows if row["status"] == "queued"]
        active_rows = [row for row in rows if row["status"] == "running"]
        recent_rows = [row for row in rows if row["status"] in TERMINAL_STATUSES]
        recent_rows = list(reversed(recent_rows[-self.recent_limit:]))
        used = self._allocations(active_rows)
        allocations = {
            name: used.get(name, 0) for name in owner["capacities"]
        }
        queued = [
            self._public(
                row,
                position=index,
                active=active_rows,
                queued=queued_rows,
                capacities=owner["capacities"],
            )
            for index, row in enumerate(queued_rows, start=1)
        ]
        self._touch()
        return {
            "protocol": PROTOCOL,
            "broker_pid": owner["pid"],
            "captured_at": _now(),
            "capacities": owner["capacities"],
            "allocations": allocations,
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
                if row["cancel_requested"]:
                    raise CoordinatorError(
                        f"land job {run_id} has a cancellation request; publication refused"
                    )
            if mismatches:
                raise CoordinatorError(
                    f"run {run_id!r} cannot advance land phase: " + "; ".join(mismatches)
                )
            db.execute(
                "UPDATE runs SET phase = ?, gate_exit_status = ? WHERE run_id = ?",
                (phase, selected_gate_status, run_id),
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
            except Exception as exc:  # keep one bad row from silently killing admission
                self._append_daemon_log(f"pump error: {type(exc).__name__}: {exc}")
            if self._should_idle_exit():
                return

    def _pump_once(self) -> None:
        with self._db_lock, self._connect() as db:
            active = db.execute(
                "SELECT * FROM runs WHERE status = 'running' ORDER BY sequence"
            ).fetchall()
            self._validate_active_set(active)
            for row in active:
                self._observe_active(db, row)

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
        worker_tmp = self._worker_tmp_path(run_id)
        release_read = -1
        release_write = -1
        process: subprocess.Popen[bytes] | None = None
        released = False
        try:
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
            worker_tmp.mkdir(mode=0o700)
            worker_tmp.chmod(0o700)
            for variable in ("TMPDIR", "TMP", "TEMP"):
                environment[variable] = str(worker_tmp)
            release_read, release_write = os.pipe()
            with log_path.open("ab", buffering=0) as output:
                log_path.chmod(0o600)
                process = subprocess.Popen(
                    [
                        sys.executable,
                        "-c",
                        _WORKER_LAUNCHER,
                        str(release_read),
                        *worker_command,
                    ],
                    cwd=row["checkout"],
                    stdin=subprocess.DEVNULL,
                    stdout=output,
                    stderr=subprocess.STDOUT,
                    env=environment,
                    start_new_session=True,
                    pass_fds=(release_read,),
                )
            os.close(release_read)
            release_read = -1
            token = _process_start_token(process.pid)
            if token is None:
                raise CoordinatorError(
                    f"could not identify gate launcher process {process.pid}"
                )
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
            with log_path.open("a", encoding="utf-8") as output:
                output.write(f"Gate coordinator: could not start worker: {exc}\n")
            log_path.chmod(0o600)
            if not self._remove_worker_tmp(run_id):
                db.execute(
                    "UPDATE runs SET environment_json = '{}' WHERE run_id = ?",
                    (run_id,),
                )
                return
            failure_reason = "merge-error" if row["kind"] == "merge" else None
            db.execute(
                "UPDATE runs SET status = 'failed', phase = 'complete', "
                "finished_at = ?, exit_status = 127, "
                "failure_reason = ?, environment_json = '{}' WHERE run_id = ?",
                (_now(), failure_reason, run_id),
            )
            self._prune(db)
        finally:
            if release_read >= 0:
                os.close(release_read)
            if release_write >= 0:
                os.close(release_write)

    def _failure_reason_for(
        self,
        row: sqlite3.Row,
        *,
        status: str,
        exit_status: int | None,
    ) -> str | None:
        if status != "failed" or row["kind"] not in {"merge", "land"}:
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
                self._escalate_cancel(row)
                return
            if not self._drain_finished_process_group(row):
                return
            if not self._remove_worker_tmp(run_id):
                return
            self._children.pop(run_id, None)
            self._group_drain_started.pop(run_id, None)
            exit_status = _shell_status(returncode)
            if row["cancel_requested"]:
                status = "cancelled"
                exit_status = 130
            else:
                status = "passed" if exit_status == 0 else "failed"
            failure_reason = self._failure_reason_for(
                row,
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
            return

        # A broker can be SIGKILLed while its process group remains. The replacement does
        # not launch a second worker: it observes the exact pid+start token until the old
        # group ends, preserving the coordinator as the sole exclusion boundary.
        if _same_process(row["worker_pid"], row["worker_start_token"]):
            self._escalate_cancel(row)
            return
        if not self._drain_finished_process_group(row):
            return
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

    def _escalate_cancel(self, row: sqlite3.Row) -> None:
        if not row["cancel_requested"]:
            return
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
        if self.idle_timeout is None:
            return False
        with self._db_lock, self._connect() as db:
            activity = db.execute(
                "SELECT value FROM coordinator_meta WHERE key = 'last_activity'"
            ).fetchone()
            live = db.execute(
                "SELECT COUNT(*) FROM runs WHERE status IN ('queued', 'running')"
            ).fetchone()[0]
        if live:
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
    ):
        if connect_timeout <= 0:
            raise ValueError("connect_timeout must be positive")
        self.paths = queue_paths(state_dir=state_dir, checkout=checkout)
        self.autostart = autostart
        self.connect_timeout = connect_timeout
        self._catalogue_instance: CoordinatorBroker | None = None

    def _catalogue(self) -> CoordinatorBroker:
        if self._catalogue_instance is None:
            self._catalogue_instance = CoordinatorBroker(
                self.paths.state_dir,
                idle_timeout=None,
            )
        return self._catalogue_instance

    def _ensure_broker(self) -> dict[str, Any]:
        deadline = time.monotonic() + self.connect_timeout
        last_metadata_error: _OwnerMetadataError | None = None
        while True:
            try:
                owner = _broker_owner(self.paths)
                break
            except _OwnerMetadataError as exc:
                # flock ownership becomes visible a few instructions before its metadata
                # write. Concurrent first clients wait through only that bounded interval;
                # a persistently malformed live owner still fails closed.
                last_metadata_error = exc
                if time.monotonic() >= deadline:
                    raise CoordinatorError(str(exc)) from exc
                time.sleep(0.01)
        if owner is not None:
            return {
                "protocol": owner["protocol"],
                "broker_pid": owner["pid"],
                "capacities": owner["capacities"],
            }
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
        self._ensure_broker()
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
        self._ensure_broker()
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
    ) -> str:
        self._ensure_broker()
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
        )

    def snapshot(self) -> dict[str, Any]:
        self._ensure_broker()
        return self._catalogue().snapshot()

    def status(self, run_id: str) -> dict[str, Any]:
        self._ensure_broker()
        return self._catalogue().status(run_id)

    def cancel(self, run_id: str) -> dict[str, Any]:
        self._ensure_broker()
        return self._catalogue().cancel(run_id)

    def clear(self) -> dict[str, int]:
        self._ensure_broker()
        return self._catalogue().clear()

    def verify_admission(
        self,
        run_id: str,
        *,
        kind: str,
        checkout: str,
        head_sha: str,
        worker_pid: int,
    ) -> None:
        self._ensure_broker()
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
    ) -> None:
        self._ensure_broker()
        self._catalogue().update_land_phase(
            run_id,
            phase=phase,
            gate_exit_status=gate_exit_status,
            worker_pid=worker_pid,
        )

    def report_land_result(
        self,
        run_id: str,
        *,
        exit_status: int,
        worker_pid: int,
    ) -> None:
        self._ensure_broker()
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
        self._ensure_broker()
        return self._catalogue().log(run_id, offset=offset, limit=limit)

    def ping(self) -> dict[str, Any]:
        owner = _broker_owner(self.paths)
        if owner is None:
            raise CoordinatorError(
                f"no gate broker owns {self.paths.state_dir}"
            )
        return {
            "protocol": owner["protocol"],
            "broker_pid": owner["pid"],
            "capacities": owner["capacities"],
        }

    def _start_broker(self) -> None:
        # Prepare and protocol-check the private spool before a detached process is born.
        # This also closes the interval in which its redirected log could inherit loose
        # permissions from a pre-existing operator-selected directory.
        self._catalogue()
        capacities = default_capacities()
        try:
            log = self.paths.daemon_log.open("ab", buffering=0)
        except OSError as exc:
            raise CoordinatorError(
                f"cannot open gate broker log {self.paths.daemon_log}: {exc}"
            ) from exc
        os.chmod(self.paths.daemon_log, 0o600)
        command = [
            sys.executable,
            "-m",
            "agcoord.queue",
            "serve",
            "--state-dir",
            str(self.paths.state_dir),
            "--capacities-json",
            json.dumps(capacities, separators=(",", ":")),
        ]
        try:
            subprocess.Popen(
                command,
                stdin=subprocess.DEVNULL,
                stdout=log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
                close_fds=True,
            )
        except OSError as exc:
            raise CoordinatorError(f"cannot start gate coordinator: {exc}") from exc
        finally:
            log.close()

        deadline = time.monotonic() + self.connect_timeout
        last_error: CoordinatorError | None = None
        while time.monotonic() < deadline:
            try:
                self.ping()
                return
            except CoordinatorError as exc:
                last_error = exc
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
    parser = argparse.ArgumentParser(prog="agcoord queue")
    commands = parser.add_subparsers(dest="command_name", required=True)
    serve = commands.add_parser("serve", help=argparse.SUPPRESS)
    serve.add_argument("--state-dir", required=True)
    serve.add_argument("--idle-seconds", type=float, default=DEFAULT_IDLE_SECONDS)
    serve.add_argument(
        "--capacities-json",
        default=json.dumps(default_capacities(), separators=(",", ":")),
    )
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
            capacities = json.loads(args.capacities_json)
        except json.JSONDecodeError as exc:
            print(f"AGCoord: invalid capacities JSON: {exc}", file=sys.stderr)
            return 2
        broker = CoordinatorBroker(
            args.state_dir,
            idle_timeout=args.idle_seconds,
            capacities=capacities,
        )

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
