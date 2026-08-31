"""Persistent per-run scratch trees backed by Linux project quotas."""

from __future__ import annotations

import ctypes
from dataclasses import dataclass
import errno
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import stat
import sys
import time
from typing import Callable, Mapping, Protocol
from uuid import uuid4

from .resources import (
    RESOURCE_OPERATIONS,
    ResourceBackendError,
    ResourceMeasurement,
    ResourceObservation,
    ResourceRequest,
)


PROJECT_QUOTA_BACKEND = "project-quota"
QUOTA_BLOCK_BYTES = 1024
_MAX_METADATA_BYTES = 64 * 1024
_TOKEN = re.compile(r"^[0-9a-f]{32}$")
_DEVICE = re.compile(r"^[0-9]+:[0-9]+$")
_PHASES = frozenset({"allocating", "allocated", "ready", "cleaning"})
_MIN_PROJECT_ID = 1_000_000_000
_MAX_PROJECT_ID = 2_000_000_000
_PROJECT_ATTEMPTS = 64
_FS_XFLAG_PROJINHERIT = 0x00000200


class ProjectQuotaError(ResourceBackendError):
    """A stable refusal from the project-quota backend."""


@dataclass(frozen=True)
class ProjectQuotaMount:
    path: Path
    source: Path
    filesystem: str
    device: str


@dataclass(frozen=True)
class ProjectAttributes:
    project_id: int
    inherit: bool


@dataclass(frozen=True)
class ProjectQuotaUsage:
    hard_bytes: int
    hard_inodes: int
    used_bytes: int
    used_inodes: int


@dataclass(frozen=True)
class _QuotaPolicy:
    storage_name: str
    inode_name: str
    hard_bytes: int
    hard_inodes: int


class ProjectQuotaSystem(Protocol):
    """Small privileged filesystem seam used by the backend and tests."""

    def probe(self, path: Path) -> ProjectQuotaMount: ...

    def get_attributes(self, path: Path) -> ProjectAttributes: ...

    def set_attributes(
        self,
        path: Path,
        *,
        project_id: int,
        inherit: bool,
    ) -> None: ...

    def get_quota(
        self,
        mount: ProjectQuotaMount,
        project_id: int,
    ) -> ProjectQuotaUsage: ...

    def set_quota(
        self,
        mount: ProjectQuotaMount,
        project_id: int,
        *,
        hard_bytes: int,
        hard_inodes: int,
    ) -> None: ...

    def sync(self, path: Path) -> None: ...


def _decode_mount_path(value: str) -> str:
    return re.sub(
        r"\\([0-7]{3})",
        lambda match: chr(int(match.group(1), 8)),
        value,
    )


def _covers(parent: Path, child: Path) -> bool:
    try:
        child.relative_to(parent)
    except ValueError:
        return False
    return True


def _effective_capability(number: int) -> bool:
    try:
        status = Path("/proc/self/status").read_text(encoding="ascii")
        mapping = Path("/proc/self/uid_map").read_text(encoding="ascii").split()
    except (OSError, UnicodeError):
        return False
    match = re.search(r"^CapEff:\s*([0-9a-fA-F]+)$", status, re.MULTILINE)
    if match is None or mapping != ["0", "0", "4294967295"]:
        return False
    return bool(int(match.group(1), 16) & (1 << number))


class _Fsxattr(ctypes.Structure):
    _fields_ = [
        ("xflags", ctypes.c_uint32),
        ("extsize", ctypes.c_uint32),
        ("nextents", ctypes.c_uint32),
        ("project_id", ctypes.c_uint32),
        ("cowextsize", ctypes.c_uint32),
        ("padding", ctypes.c_ubyte * 8),
    ]


class _Dqblk(ctypes.Structure):
    _fields_ = [
        ("block_hard", ctypes.c_uint64),
        ("block_soft", ctypes.c_uint64),
        ("current_space", ctypes.c_uint64),
        ("inode_hard", ctypes.c_uint64),
        ("inode_soft", ctypes.c_uint64),
        ("current_inodes", ctypes.c_uint64),
        ("block_time", ctypes.c_uint64),
        ("inode_time", ctypes.c_uint64),
        ("valid", ctypes.c_uint32),
    ]


class _XfsDiskQuota(ctypes.Structure):
    _fields_ = [
        ("version", ctypes.c_int8),
        ("flags", ctypes.c_int8),
        ("fieldmask", ctypes.c_uint16),
        ("project_id", ctypes.c_uint32),
        ("block_hard", ctypes.c_uint64),
        ("block_soft", ctypes.c_uint64),
        ("inode_hard", ctypes.c_uint64),
        ("inode_soft", ctypes.c_uint64),
        ("block_count", ctypes.c_uint64),
        ("inode_count", ctypes.c_uint64),
        ("inode_timer", ctypes.c_int32),
        ("block_timer", ctypes.c_int32),
        ("inode_warns", ctypes.c_uint16),
        ("block_warns", ctypes.c_uint16),
        ("inode_timer_hi", ctypes.c_int8),
        ("block_timer_hi", ctypes.c_int8),
        ("realtime_timer_hi", ctypes.c_int8),
        ("padding2", ctypes.c_int8),
        ("realtime_hard", ctypes.c_uint64),
        ("realtime_soft", ctypes.c_uint64),
        ("realtime_count", ctypes.c_uint64),
        ("realtime_timer", ctypes.c_int32),
        ("realtime_warns", ctypes.c_uint16),
        ("padding3", ctypes.c_int16),
        ("padding4", ctypes.c_char * 8),
    ]


def _ioc(direction: int, kind: int, number: int, size: int) -> int:
    return (direction << 30) | (size << 16) | (kind << 8) | number


_FS_IOC_FSGETXATTR = _ioc(2, ord("X"), 31, ctypes.sizeof(_Fsxattr))
_FS_IOC_FSSETXATTR = _ioc(1, ord("X"), 32, ctypes.sizeof(_Fsxattr))
_Q_GETQUOTA = 0x800007
_Q_SETQUOTA = 0x800008
_PRJQUOTA = 2
_QIF_BLIMITS = 1 << 0
_QIF_ILIMITS = 1 << 2
_Q_XGETQUOTA = (ord("X") << 8) + 3
_Q_XSETQLIM = (ord("X") << 8) + 4
_FS_PROJ_QUOTA = 1 << 1
_FS_DQ_ISOFT = 1 << 0
_FS_DQ_IHARD = 1 << 1
_FS_DQ_BSOFT = 1 << 2
_FS_DQ_BHARD = 1 << 3


def _qcmd(command: int) -> int:
    return (command << 8) | _PRJQUOTA


class LinuxProjectQuotaSystem:
    """Linux ext4/XFS project quota operations without external quota tools."""

    def __init__(self, *, mountinfo: Path = Path("/proc/self/mountinfo")) -> None:
        self.mountinfo = mountinfo

    @staticmethod
    def _raise_operation(exc: OSError, *, attributes: bool = False) -> None:
        if exc.errno in {errno.EPERM, errno.EACCES}:
            code = "quota-privilege-unavailable"
        elif attributes and exc.errno in {errno.EINVAL, errno.ENOTTY, errno.EOPNOTSUPP}:
            code = "quota-project-attributes-unavailable"
        elif exc.errno in {errno.EINVAL, errno.ENOSYS, errno.EOPNOTSUPP, errno.ESRCH}:
            code = "quota-enforcement-unavailable"
        else:
            code = "quota-operation-failed"
        raise ProjectQuotaError(code, "project quota operation failed") from exc

    def _mounts(self) -> list[tuple[ProjectQuotaMount, frozenset[str]]]:
        try:
            lines = self.mountinfo.read_text(encoding="utf-8").splitlines()
        except (OSError, UnicodeError) as exc:
            raise ProjectQuotaError(
                "quota-mountinfo-unavailable",
                "mount information is unavailable",
            ) from exc
        selected: list[tuple[ProjectQuotaMount, frozenset[str]]] = []
        for line in lines:
            try:
                left, right = line.split(" - ", 1)
                fields = left.split()
                right_fields = right.split()
                mount_path = Path(_decode_mount_path(fields[4]))
                filesystem = right_fields[0]
                source = Path(_decode_mount_path(right_fields[1]))
                options = frozenset(
                    fields[5].split(",") + right_fields[2].split(",")
                )
                device = fields[2]
            except (IndexError, ValueError):
                continue
            if mount_path.is_absolute() and _DEVICE.fullmatch(device):
                selected.append(
                    (
                        ProjectQuotaMount(
                            path=mount_path,
                            source=source,
                            filesystem=filesystem,
                            device=device,
                        ),
                        options,
                    )
                )
        return selected

    def probe(self, path: Path) -> ProjectQuotaMount:
        if not sys.platform.startswith("linux"):
            raise ProjectQuotaError(
                "quota-platform-unsupported",
                "project quotas require Linux",
            )
        if not _effective_capability(21):
            raise ProjectQuotaError(
                "quota-privilege-unavailable",
                "project quota administration needs init-namespace CAP_SYS_ADMIN",
            )
        try:
            target = path.resolve(strict=True)
        except OSError as exc:
            raise ProjectQuotaError(
                "quota-root-unavailable",
                "project quota storage root is unavailable",
            ) from exc
        candidates = [item for item in self._mounts() if _covers(item[0].path, target)]
        if not candidates:
            raise ProjectQuotaError(
                "quota-mount-unavailable",
                "project quota storage has no covering mount",
            )
        mount, options = max(candidates, key=lambda item: len(item[0].path.parts))
        if mount.filesystem not in {"ext4", "xfs"}:
            raise ProjectQuotaError(
                "quota-filesystem-unsupported",
                "project quota storage must use ext4 or XFS",
            )
        if "ro" in options or "rw" not in options:
            raise ProjectQuotaError(
                "quota-filesystem-read-only",
                "project quota storage must be writable",
            )
        if not ({"prjquota", "pquota"} & options) or {
            "noquota",
            "pqnoenforce",
        } & options:
            raise ProjectQuotaError(
                "quota-enforcement-unavailable",
                "project quota accounting and enforcement are not enabled",
            )
        try:
            source = mount.source.resolve(strict=True)
            source_details = source.stat()
        except OSError as exc:
            raise ProjectQuotaError(
                "quota-device-unavailable",
                "project quota backing device is unavailable",
            ) from exc
        if not stat.S_ISBLK(source_details.st_mode):
            raise ProjectQuotaError(
                "quota-device-unsupported",
                "project quota backing source is not one block device",
            )
        major, minor = map(int, mount.device.split(":"))
        if (
            os.major(source_details.st_rdev) != major
            or os.minor(source_details.st_rdev) != minor
            or (Path("/sys/dev/block") / mount.device / "dm").exists()
        ):
            raise ProjectQuotaError(
                "quota-device-unsupported",
                "project quota backing device is ambiguous or device-mapped",
            )
        verified = ProjectQuotaMount(
            path=mount.path,
            source=source,
            filesystem=mount.filesystem,
            device=mount.device,
        )
        self.get_attributes(target)
        self.get_quota(verified, 0)
        return verified

    @staticmethod
    def _open_directory(path: Path) -> int:
        try:
            return os.open(
                path,
                os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW,
            )
        except OSError as exc:
            LinuxProjectQuotaSystem._raise_operation(exc, attributes=True)
            raise AssertionError("quota attribute open returned")  # pragma: no cover

    @staticmethod
    def _raw_attributes(path: Path) -> _Fsxattr:
        descriptor = LinuxProjectQuotaSystem._open_directory(path)
        try:
            payload = bytearray(ctypes.sizeof(_Fsxattr))
            fcntl.ioctl(descriptor, _FS_IOC_FSGETXATTR, payload, True)
            return _Fsxattr.from_buffer_copy(payload)
        except OSError as exc:
            LinuxProjectQuotaSystem._raise_operation(exc, attributes=True)
            raise AssertionError("quota attribute read returned")  # pragma: no cover
        finally:
            os.close(descriptor)

    def get_attributes(self, path: Path) -> ProjectAttributes:
        raw = self._raw_attributes(path)
        return ProjectAttributes(
            project_id=int(raw.project_id),
            inherit=bool(raw.xflags & _FS_XFLAG_PROJINHERIT),
        )

    def set_attributes(
        self,
        path: Path,
        *,
        project_id: int,
        inherit: bool,
    ) -> None:
        if not 0 <= project_id < 2**32:
            raise ProjectQuotaError(
                "quota-project-id-invalid",
                "project quota identifier is invalid",
            )
        descriptor = self._open_directory(path)
        try:
            payload = bytearray(ctypes.sizeof(_Fsxattr))
            fcntl.ioctl(descriptor, _FS_IOC_FSGETXATTR, payload, True)
            raw = _Fsxattr.from_buffer_copy(payload)
            raw.project_id = project_id
            if inherit:
                raw.xflags |= _FS_XFLAG_PROJINHERIT
            else:
                raw.xflags &= ~_FS_XFLAG_PROJINHERIT
            fcntl.ioctl(descriptor, _FS_IOC_FSSETXATTR, bytes(raw))
        except OSError as exc:
            self._raise_operation(exc, attributes=True)
        finally:
            os.close(descriptor)
        if self.get_attributes(path) != ProjectAttributes(project_id, inherit):
            raise ProjectQuotaError(
                "quota-project-attributes-unverified",
                "project quota directory attributes were not accepted",
            )

    @staticmethod
    def _quotactl(
        command: int,
        mount: ProjectQuotaMount,
        project_id: int,
        value: ctypes.Structure,
    ) -> None:
        libc = ctypes.CDLL(None, use_errno=True)
        operation = libc.quotactl
        operation.argtypes = [
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_int,
            ctypes.c_void_p,
        ]
        operation.restype = ctypes.c_int
        if operation(
            ctypes.c_int(_qcmd(command)).value,
            os.fsencode(mount.source),
            project_id,
            ctypes.byref(value),
        ) != 0:
            error = ctypes.get_errno()
            LinuxProjectQuotaSystem._raise_operation(OSError(error, os.strerror(error)))

    def get_quota(
        self,
        mount: ProjectQuotaMount,
        project_id: int,
    ) -> ProjectQuotaUsage:
        if mount.filesystem == "xfs":
            raw = _XfsDiskQuota()
            raw.version = 1
            self._quotactl(_Q_XGETQUOTA, mount, project_id, raw)
            if raw.version != 1 or not raw.flags & _FS_PROJ_QUOTA:
                raise ProjectQuotaError(
                    "quota-response-invalid",
                    "XFS returned an invalid project quota response",
                )
            return ProjectQuotaUsage(
                hard_bytes=int(raw.block_hard) * 512,
                hard_inodes=int(raw.inode_hard),
                used_bytes=int(raw.block_count) * 512,
                used_inodes=int(raw.inode_count),
            )
        raw = _Dqblk()
        self._quotactl(_Q_GETQUOTA, mount, project_id, raw)
        return ProjectQuotaUsage(
            hard_bytes=int(raw.block_hard) * QUOTA_BLOCK_BYTES,
            hard_inodes=int(raw.inode_hard),
            used_bytes=int(raw.current_space),
            used_inodes=int(raw.current_inodes),
        )

    def set_quota(
        self,
        mount: ProjectQuotaMount,
        project_id: int,
        *,
        hard_bytes: int,
        hard_inodes: int,
    ) -> None:
        if hard_bytes % QUOTA_BLOCK_BYTES:
            raise ProjectQuotaError(
                "quota-byte-alignment-invalid",
                "project quota bytes must be aligned to 1024 bytes",
            )
        if mount.filesystem == "xfs":
            raw = _XfsDiskQuota()
            raw.version = 1
            raw.flags = _FS_PROJ_QUOTA
            raw.fieldmask = (
                _FS_DQ_ISOFT | _FS_DQ_IHARD | _FS_DQ_BSOFT | _FS_DQ_BHARD
            )
            raw.project_id = project_id
            raw.block_hard = hard_bytes // 512
            raw.inode_hard = hard_inodes
            self._quotactl(_Q_XSETQLIM, mount, project_id, raw)
        else:
            raw = _Dqblk()
            raw.block_hard = hard_bytes // QUOTA_BLOCK_BYTES
            raw.inode_hard = hard_inodes
            raw.valid = _QIF_BLIMITS | _QIF_ILIMITS
            self._quotactl(_Q_SETQUOTA, mount, project_id, raw)

    def sync(self, path: Path) -> None:
        descriptor = self._open_directory(path)
        try:
            if hasattr(os, "syncfs"):
                os.syncfs(descriptor)
        except OSError as exc:
            self._raise_operation(exc)
        finally:
            os.close(descriptor)


class ProjectQuotaBackend:
    """Own project IDs, private scratch roots, limits, receipts, and cleanup."""

    def __init__(
        self,
        state_dir: str | os.PathLike[str],
        *,
        system: ProjectQuotaSystem | None = None,
        project_id_source: Callable[[], int] | None = None,
        cleanup_timeout: float = 2.0,
    ) -> None:
        if cleanup_timeout <= 0:
            raise ValueError("cleanup_timeout must be positive")
        selected_state = Path(state_dir).expanduser()
        if not selected_state.is_absolute():
            selected_state = Path.cwd() / selected_state
        self.state_dir = selected_state.resolve(strict=False)
        self.storage_parent = self.state_dir.parent.resolve(strict=False)
        self.metadata_dir = self.state_dir / PROJECT_QUOTA_BACKEND
        self.runs_dir = self.metadata_dir / "runs"
        self.lock_path = self.metadata_dir / "owner.lock"
        self.system = LinuxProjectQuotaSystem() if system is None else system
        self.project_id_source = (
            self._random_project_id
            if project_id_source is None
            else project_id_source
        )
        self.cleanup_timeout = cleanup_timeout

    @staticmethod
    def _random_project_id() -> int:
        return _MIN_PROJECT_ID + secrets.randbelow(
            _MAX_PROJECT_ID - _MIN_PROJECT_ID
        )

    def probe(self) -> Mapping[str, object]:
        try:
            self.system.probe(self.storage_parent)
        except ProjectQuotaError as exc:
            return {
                "available": False,
                "kinds": [],
                "units": [],
                "operations": [],
                "reason": exc.code,
            }
        except Exception:
            return {
                "available": False,
                "kinds": [],
                "units": [],
                "operations": [],
                "reason": "quota-probe-failed",
            }
        return {
            "available": True,
            "kinds": ["inodes", "storage"],
            "units": ["bytes", "inodes"],
            "operations": list(RESOURCE_OPERATIONS),
            "reason": None,
        }

    @staticmethod
    def _policy(request: ResourceRequest) -> _QuotaPolicy:
        if request.backend != PROJECT_QUOTA_BACKEND:
            raise ProjectQuotaError(
                "quota-request-invalid",
                "project quota request names the wrong backend",
            )
        storage: list[str] = []
        inodes: list[str] = []
        for name, binding in request.bindings.items():
            if binding["backend"] != PROJECT_QUOTA_BACKEND:
                raise ProjectQuotaError(
                    "quota-request-invalid",
                    "project quota binding names the wrong backend",
                )
            pair = (binding["kind"], binding["unit"])
            if pair == ("storage", "bytes"):
                storage.append(name)
            elif pair == ("inodes", "inodes"):
                inodes.append(name)
            else:
                raise ProjectQuotaError(
                    "quota-request-unsupported",
                    "project quota binding is unsupported",
                )
        if len(storage) != 1 or len(inodes) != 1:
            raise ProjectQuotaError(
                "quota-policy-incomplete",
                "one storage byte and one inode resource are required",
            )
        if request.bindings[storage[0]]["mode"] != request.bindings[inodes[0]]["mode"]:
            raise ProjectQuotaError(
                "quota-mode-mismatch",
                "project quota byte and inode modes must match",
            )
        hard_bytes = request.resources[storage[0]]
        hard_inodes = request.resources[inodes[0]]
        if hard_bytes < QUOTA_BLOCK_BYTES or hard_bytes % QUOTA_BLOCK_BYTES:
            raise ProjectQuotaError(
                "quota-byte-alignment-invalid",
                "project quota bytes must be a positive multiple of 1024",
            )
        return _QuotaPolicy(
            storage_name=storage[0],
            inode_name=inodes[0],
            hard_bytes=hard_bytes,
            hard_inodes=hard_inodes,
        )

    def _prepare_paths(self, mount: ProjectQuotaMount) -> None:
        for path in (self.state_dir, self.metadata_dir, self.runs_dir):
            try:
                path.mkdir(parents=True, exist_ok=True, mode=0o700)
                details = path.lstat()
            except OSError as exc:
                raise ProjectQuotaError(
                    "quota-metadata-unavailable",
                    "project quota metadata cannot be prepared",
                ) from exc
            if (
                stat.S_ISLNK(details.st_mode)
                or not stat.S_ISDIR(details.st_mode)
                or details.st_uid != os.getuid()
                or path.resolve(strict=True) != path
            ):
                raise ProjectQuotaError(
                    "quota-metadata-invalid",
                    "project quota metadata path is unsafe",
                )
            if details.st_mode & 0o077:
                path.chmod(0o700)
        if self.system.probe(self.runs_dir) != mount:
            raise ProjectQuotaError(
                "quota-mount-changed",
                "project quota storage mount changed during preparation",
            )

    def _locked(self, mount: ProjectQuotaMount) -> tuple[int, int]:
        try:
            filesystem_descriptor = os.open(
                mount.path,
                os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC,
            )
        except OSError as exc:
            raise ProjectQuotaError(
                "quota-lock-unavailable",
                "project quota filesystem allocation lock is unavailable",
            ) from exc
        fcntl.flock(filesystem_descriptor, fcntl.LOCK_EX)
        descriptor = os.open(
            self.lock_path,
            os.O_RDWR | os.O_CREAT | os.O_CLOEXEC,
            0o600,
        )
        os.fchmod(descriptor, 0o600)
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        return filesystem_descriptor, descriptor

    @staticmethod
    def _unlock(descriptors: tuple[int, int]) -> None:
        filesystem_descriptor, descriptor = descriptors
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)
        try:
            fcntl.flock(filesystem_descriptor, fcntl.LOCK_UN)
        finally:
            os.close(filesystem_descriptor)

    def _manifest_path(self, run_id: str) -> Path:
        digest = hashlib.sha256(run_id.encode()).hexdigest()[:32]
        return self.metadata_dir / f"run-{digest}.json"

    @staticmethod
    def _request_record(request: ResourceRequest) -> dict[str, object]:
        return {
            "resources": dict(request.resources),
            "bindings": {
                name: dict(binding)
                for name, binding in request.bindings.items()
            },
        }

    @staticmethod
    def _mount_record(mount: ProjectQuotaMount) -> dict[str, str]:
        return {
            "path": str(mount.path),
            "source": str(mount.source),
            "filesystem": mount.filesystem,
            "device": mount.device,
        }

    def _write_json(self, path: Path, value: Mapping[str, object]) -> None:
        temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
        descriptor = -1
        try:
            descriptor = os.open(
                temporary,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC,
                0o600,
            )
            payload = json.dumps(
                value,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
            view = memoryview(payload)
            while view:
                written = os.write(descriptor, view)
                if written <= 0:  # pragma: no cover - write(2) contract
                    raise OSError("project quota metadata write made no progress")
                view = view[written:]
            os.fsync(descriptor)
            os.close(descriptor)
            descriptor = -1
            os.replace(temporary, path)
            directory = os.open(path.parent, os.O_RDONLY | os.O_CLOEXEC)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            temporary.unlink(missing_ok=True)

    @staticmethod
    def _read_json(path: Path) -> dict[str, object]:
        details = path.lstat()
        if (
            stat.S_ISLNK(details.st_mode)
            or not stat.S_ISREG(details.st_mode)
            or details.st_uid != os.getuid()
            or details.st_size > _MAX_METADATA_BYTES
        ):
            raise ProjectQuotaError(
                "quota-metadata-invalid",
                "project quota manifest is unsafe",
            )
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise ProjectQuotaError(
                "quota-metadata-invalid",
                "project quota manifest is unreadable",
            ) from exc
        if not isinstance(raw, dict):
            raise ProjectQuotaError(
                "quota-metadata-invalid",
                "project quota manifest is invalid",
            )
        return raw

    @staticmethod
    def _entry_present(path: Path) -> bool:
        try:
            path.lstat()
        except FileNotFoundError:
            return False
        except OSError as exc:
            raise ProjectQuotaError(
                "quota-tree-unavailable",
                "project quota scratch identity cannot be inspected",
            ) from exc
        return True

    def _manifest(
        self,
        request: ResourceRequest,
        policy: _QuotaPolicy,
        mount: ProjectQuotaMount,
        *,
        token: str,
        project_id: int,
    ) -> dict[str, object]:
        run_hash = hashlib.sha256(request.run_id.encode()).hexdigest()[:16]
        return {
            "version": 1,
            "request": self._request_record(request),
            "phase": "allocating",
            "token": token,
            "project_id": project_id,
            "path": str(self.runs_dir / f"run-{run_hash}-{token[:12]}"),
            "path_device": None,
            "path_inode": None,
            "original_project": None,
            "original_inherit": None,
            "mount": self._mount_record(mount),
            "hard_bytes": policy.hard_bytes,
            "hard_inodes": policy.hard_inodes,
        }

    @staticmethod
    def _manifest_keys() -> frozenset[str]:
        return frozenset(
            {
                "version",
                "request",
                "phase",
                "token",
                "project_id",
                "path",
                "path_device",
                "path_inode",
                "original_project",
                "original_inherit",
                "mount",
                "hard_bytes",
                "hard_inodes",
            }
        )

    def _validate_manifest(
        self,
        request: ResourceRequest,
        raw: Mapping[str, object],
        mount: ProjectQuotaMount,
    ) -> dict[str, object]:
        policy = self._policy(request)
        path = raw.get("path")
        token = raw.get("token")
        project_id = raw.get("project_id")
        phase = raw.get("phase")
        identity_values = (
            raw.get("path_device"),
            raw.get("path_inode"),
            raw.get("original_project"),
        )
        if (
            set(raw) != self._manifest_keys()
            or type(raw.get("version")) is not int
            or raw.get("version") != 1
            or raw.get("request") != self._request_record(request)
            or not isinstance(phase, str)
            or phase not in _PHASES
            or not isinstance(token, str)
            or not _TOKEN.fullmatch(token)
            or not isinstance(project_id, int)
            or isinstance(project_id, bool)
            or not _MIN_PROJECT_ID <= project_id < _MAX_PROJECT_ID
            or not isinstance(path, str)
            or not Path(path).is_absolute()
            or Path(path).parent != self.runs_dir
            or not Path(path).name.endswith(token[:12])
            or raw.get("mount") != self._mount_record(mount)
            or not isinstance(raw.get("hard_bytes"), int)
            or isinstance(raw.get("hard_bytes"), bool)
            or int(raw["hard_bytes"]) <= 0
            or int(raw["hard_bytes"]) != policy.hard_bytes
            or not isinstance(raw.get("hard_inodes"), int)
            or isinstance(raw.get("hard_inodes"), bool)
            or int(raw["hard_inodes"]) <= 0
            or int(raw["hard_inodes"]) != policy.hard_inodes
        ):
            raise ProjectQuotaError(
                "quota-metadata-invalid",
                "project quota manifest does not match the request",
            )
        if raw["path_device"] is None:
            if any(value is not None for value in identity_values):
                raise ProjectQuotaError(
                    "quota-metadata-invalid",
                    "project quota allocation identity is partial",
                )
            if raw.get("original_inherit") is not None:
                raise ProjectQuotaError(
                    "quota-metadata-invalid",
                    "project quota original attributes are partial",
                )
        elif (
            any(
                not isinstance(value, int) or isinstance(value, bool) or value < 0
                for value in identity_values
            )
            or not isinstance(raw.get("original_inherit"), bool)
        ):
            raise ProjectQuotaError(
                "quota-metadata-invalid",
                "project quota allocation identity is invalid",
            )
        return dict(raw)

    @staticmethod
    def _handle(manifest: Mapping[str, object]) -> dict[str, object]:
        mount = manifest["mount"]
        assert isinstance(mount, Mapping)
        return {
            "version": 1,
            "token": manifest["token"],
            "project_id": manifest["project_id"],
            "path": manifest["path"],
            "path_device": manifest["path_device"],
            "path_inode": manifest["path_inode"],
            "filesystem": mount["filesystem"],
            "mount_device": mount["device"],
            "hard_bytes": manifest["hard_bytes"],
            "hard_inodes": manifest["hard_inodes"],
        }

    @staticmethod
    def _validate_handle(raw: Mapping[str, object]) -> dict[str, object]:
        expected = {
            "version",
            "token",
            "project_id",
            "path",
            "path_device",
            "path_inode",
            "filesystem",
            "mount_device",
            "hard_bytes",
            "hard_inodes",
        }
        if (
            not isinstance(raw, Mapping)
            or set(raw) != expected
            or type(raw.get("version")) is not int
            or raw.get("version") != 1
            or not isinstance(raw.get("token"), str)
            or not _TOKEN.fullmatch(str(raw.get("token")))
            or not isinstance(raw.get("project_id"), int)
            or isinstance(raw.get("project_id"), bool)
            or not _MIN_PROJECT_ID <= int(raw.get("project_id", 0)) < _MAX_PROJECT_ID
            or not isinstance(raw.get("path"), str)
            or not Path(str(raw.get("path"))).is_absolute()
            or any(
                not isinstance(raw.get(name), int)
                or isinstance(raw.get(name), bool)
                or int(raw[name]) <= 0
                for name in (
                    "path_device",
                    "path_inode",
                    "hard_bytes",
                    "hard_inodes",
                )
            )
            or raw.get("filesystem") not in {"ext4", "xfs"}
            or not isinstance(raw.get("mount_device"), str)
            or not _DEVICE.fullmatch(str(raw.get("mount_device")))
        ):
            raise ProjectQuotaError(
                "quota-handle-invalid",
                "project quota private handle is invalid",
            )
        return dict(raw)

    @staticmethod
    def _zero(usage: ProjectQuotaUsage) -> bool:
        return usage == ProjectQuotaUsage(0, 0, 0, 0)

    def _project_available(
        self,
        mount: ProjectQuotaMount,
        project_id: int,
    ) -> bool:
        return self._zero(self.system.get_quota(mount, project_id))

    def _allocate_project(self, mount: ProjectQuotaMount) -> int:
        occupied: set[int] = set()
        for path in self.metadata_dir.glob("run-*.json"):
            try:
                raw = self._read_json(path)
            except ProjectQuotaError:
                raise
            value = raw.get("project_id")
            if isinstance(value, int) and not isinstance(value, bool):
                occupied.add(value)
        for _attempt in range(_PROJECT_ATTEMPTS):
            try:
                candidate = self.project_id_source()
            except (StopIteration, ValueError) as exc:
                raise ProjectQuotaError(
                    "quota-project-id-exhausted",
                    "project quota identifier source was exhausted",
                ) from exc
            if (
                not isinstance(candidate, int)
                or isinstance(candidate, bool)
                or not _MIN_PROJECT_ID <= candidate < _MAX_PROJECT_ID
            ):
                raise ProjectQuotaError(
                    "quota-project-id-invalid",
                    "project quota identifier source returned an invalid value",
                )
            if candidate not in occupied and self._project_available(mount, candidate):
                return candidate
        raise ProjectQuotaError(
            "quota-project-id-exhausted",
            "no unused project quota identifier was found",
        )

    @staticmethod
    def _validate_tree(path: Path, manifest: Mapping[str, object]) -> os.stat_result:
        try:
            details = path.lstat()
        except OSError as exc:
            raise ProjectQuotaError(
                "quota-tree-missing",
                "project quota scratch tree disappeared",
            ) from exc
        if (
            stat.S_ISLNK(details.st_mode)
            or not stat.S_ISDIR(details.st_mode)
            or details.st_uid != os.getuid()
            or details.st_dev != manifest["path_device"]
            or details.st_ino != manifest["path_inode"]
            or path.resolve(strict=True) != path
        ):
            raise ProjectQuotaError(
                "quota-tree-reused",
                "project quota scratch tree identity changed",
            )
        if details.st_mode & 0o077:
            raise ProjectQuotaError(
                "quota-tree-permissions-changed",
                "project quota scratch tree is no longer private",
            )
        return details

    def _complete_manifest(
        self,
        request: ResourceRequest,
        manifest_path: Path,
        manifest: dict[str, object],
        mount: ProjectQuotaMount,
    ) -> dict[str, object]:
        path = Path(str(manifest["path"]))
        if manifest["path_device"] is None:
            try:
                path.mkdir(mode=0o700)
            except FileExistsError:
                try:
                    entries = list(path.iterdir())
                    details = path.lstat()
                except OSError as exc:
                    raise ProjectQuotaError(
                        "quota-tree-collision",
                        "project quota scratch path collided",
                    ) from exc
                if (
                    entries
                    or stat.S_ISLNK(details.st_mode)
                    or not stat.S_ISDIR(details.st_mode)
                    or details.st_uid != os.getuid()
                    or path.resolve(strict=True) != path
                ):
                    raise ProjectQuotaError(
                        "quota-tree-collision",
                        "project quota scratch path collided",
                    )
            details = path.lstat()
            if details.st_mode & 0o077:
                path.chmod(0o700)
                details = path.lstat()
            original = self.system.get_attributes(path)
            manifest.update(
                {
                    "phase": "allocated",
                    "path_device": details.st_dev,
                    "path_inode": details.st_ino,
                    "original_project": original.project_id,
                    "original_inherit": original.inherit,
                }
            )
            self._write_json(manifest_path, manifest)
        self._validate_tree(path, manifest)
        desired = ProjectQuotaUsage(
            hard_bytes=int(manifest["hard_bytes"]),
            hard_inodes=int(manifest["hard_inodes"]),
            used_bytes=0,
            used_inodes=0,
        )
        quota = self.system.get_quota(mount, int(manifest["project_id"]))
        if self._zero(quota):
            self.system.set_quota(
                mount,
                int(manifest["project_id"]),
                hard_bytes=desired.hard_bytes,
                hard_inodes=desired.hard_inodes,
            )
        elif (
            quota.hard_bytes != desired.hard_bytes
            or quota.hard_inodes != desired.hard_inodes
            or quota.used_bytes > quota.hard_bytes
            or quota.used_inodes > quota.hard_inodes
        ):
            raise ProjectQuotaError(
                "quota-project-collision",
                "project quota identifier is already in use",
            )
        attributes = self.system.get_attributes(path)
        original = ProjectAttributes(
            int(manifest["original_project"]),
            bool(manifest["original_inherit"]),
        )
        selected = ProjectAttributes(int(manifest["project_id"]), True)
        if attributes == original:
            self.system.set_attributes(
                path,
                project_id=selected.project_id,
                inherit=True,
            )
        elif attributes != selected:
            raise ProjectQuotaError(
                "quota-tree-attributes-changed",
                "project quota scratch attributes changed",
            )
        quota = self.system.get_quota(mount, int(manifest["project_id"]))
        if (
            quota.hard_bytes != desired.hard_bytes
            or quota.hard_inodes != desired.hard_inodes
            or quota.used_bytes > quota.hard_bytes
            or quota.used_inodes > quota.hard_inodes
            or self.system.get_attributes(path) != selected
        ):
            raise ProjectQuotaError(
                "quota-enforcement-unverified",
                "project quota limits were not accepted",
            )
        manifest["phase"] = "ready"
        self._write_json(manifest_path, manifest)
        return self._handle(manifest)

    def _rollback_manifest(
        self,
        manifest_path: Path,
        manifest: Mapping[str, object],
        mount: ProjectQuotaMount,
    ) -> None:
        """Undo a caught preparation error; process death leaves recovery metadata."""

        path = Path(str(manifest["path"]))
        if self._entry_present(path):
            if manifest.get("path_device") is None:
                details = path.lstat()
                if (
                    stat.S_ISLNK(details.st_mode)
                    or not stat.S_ISDIR(details.st_mode)
                    or details.st_uid != os.getuid()
                    or path.resolve(strict=True) != path
                    or any(path.iterdir())
                ):
                    raise ProjectQuotaError(
                        "quota-rollback-refused",
                        "unowned project quota allocation cannot be rolled back",
                    )
            else:
                details = self._validate_tree(path, manifest)
                current = self.system.get_attributes(path)
                selected = ProjectAttributes(int(manifest["project_id"]), True)
                original = ProjectAttributes(
                    int(manifest["original_project"]),
                    bool(manifest["original_inherit"]),
                )
                if current == selected:
                    self.system.set_attributes(
                        path,
                        project_id=original.project_id,
                        inherit=original.inherit,
                    )
                elif current != original:
                    raise ProjectQuotaError(
                        "quota-rollback-refused",
                        "changed project quota attributes cannot be rolled back",
                    )
            self._make_deletable(path, expected_device=details.st_dev)
            try:
                shutil.rmtree(path)
            except OSError as exc:
                raise ProjectQuotaError(
                    "quota-rollback-refused",
                    "project quota allocation could not be rolled back",
                ) from exc
        quota = self.system.get_quota(mount, int(manifest["project_id"]))
        if quota.used_bytes or quota.used_inodes:
            raise ProjectQuotaError(
                "quota-rollback-refused",
                "live project quota usage cannot be rolled back",
            )
        if (
            quota.hard_bytes == manifest["hard_bytes"]
            and quota.hard_inodes == manifest["hard_inodes"]
        ):
            self.system.set_quota(
                mount,
                int(manifest["project_id"]),
                hard_bytes=0,
                hard_inodes=0,
            )
        elif quota.hard_bytes or quota.hard_inodes:
            raise ProjectQuotaError(
                "quota-rollback-refused",
                "changed project quota limits cannot be rolled back",
            )
        if not self._zero(
            self.system.get_quota(mount, int(manifest["project_id"]))
        ):
            raise ProjectQuotaError(
                "quota-rollback-refused",
                "project quota rollback could not be verified",
            )
        manifest_path.unlink(missing_ok=True)

    def _resolve(
        self,
        request: ResourceRequest,
        raw_handle: Mapping[str, object],
        *,
        require_path: bool,
    ) -> tuple[dict[str, object], dict[str, object], ProjectQuotaMount]:
        handle = self._validate_handle(raw_handle)
        handle_path = Path(str(handle["path"]))
        if (
            handle_path.parent != self.runs_dir
            or not handle_path.name.endswith(str(handle["token"])[:12])
        ):
            raise ProjectQuotaError(
                "quota-handle-invalid",
                "project quota private handle path is invalid",
            )
        mount = self.system.probe(self.storage_parent)
        self._prepare_paths(mount)
        if (
            handle["filesystem"] != mount.filesystem
            or handle["mount_device"] != mount.device
        ):
            raise ProjectQuotaError(
                "quota-mount-changed",
                "project quota storage mount changed",
            )
        manifest_path = self._manifest_path(request.run_id)
        try:
            manifest = self._validate_manifest(
                request,
                self._read_json(manifest_path),
                mount,
            )
        except FileNotFoundError:
            if require_path or self._entry_present(Path(str(handle["path"]))):
                raise ProjectQuotaError(
                    "quota-manifest-missing",
                    "project quota ownership manifest disappeared",
                )
            return handle, {}, mount
        if self._handle(manifest) != handle:
            raise ProjectQuotaError(
                "quota-handle-mismatch",
                "project quota handle does not own its manifest",
            )
        if require_path:
            self._validate_tree(Path(str(handle["path"])), manifest)
        return handle, manifest, mount

    def prepare(self, request: ResourceRequest) -> Mapping[str, object]:
        policy = self._policy(request)
        mount = self.system.probe(self.storage_parent)
        self._prepare_paths(mount)
        descriptors = self._locked(mount)
        try:
            manifest_path = self._manifest_path(request.run_id)
            if self._entry_present(manifest_path):
                manifest = self._validate_manifest(
                    request,
                    self._read_json(manifest_path),
                    mount,
                )
                try:
                    return self._complete_manifest(
                        request,
                        manifest_path,
                        manifest,
                        mount,
                    )
                except Exception:
                    try:
                        current = self._validate_manifest(
                            request,
                            self._read_json(manifest_path),
                            mount,
                        )
                        self._rollback_manifest(manifest_path, current, mount)
                    except Exception:
                        pass
                    raise
            project_id = self._allocate_project(mount)
            manifest = self._manifest(
                request,
                policy,
                mount,
                token=uuid4().hex,
                project_id=project_id,
            )
            self._write_json(manifest_path, manifest)
            try:
                return self._complete_manifest(
                    request,
                    manifest_path,
                    manifest,
                    mount,
                )
            except Exception:
                try:
                    current = self._validate_manifest(
                        request,
                        self._read_json(manifest_path),
                        mount,
                    )
                    self._rollback_manifest(manifest_path, current, mount)
                except Exception:
                    pass
                raise
        finally:
            self._unlock(descriptors)

    def scratch_path(
        self,
        request: ResourceRequest,
        state: Mapping[str, object],
    ) -> Path:
        self._policy(request)
        handle, manifest, mount = self._resolve(request, state, require_path=True)
        if manifest.get("phase") != "ready":
            raise ProjectQuotaError(
                "quota-tree-not-ready",
                "project quota scratch tree is not ready",
            )
        self._verify_enforcement(handle, mount)
        return Path(str(handle["path"]))

    def _verify_enforcement(
        self,
        handle: Mapping[str, object],
        mount: ProjectQuotaMount,
    ) -> ProjectQuotaUsage:
        path = Path(str(handle["path"]))
        if self.system.get_attributes(path) != ProjectAttributes(
            int(handle["project_id"]),
            True,
        ):
            raise ProjectQuotaError(
                "quota-tree-attributes-changed",
                "project quota scratch attributes changed",
            )
        quota = self.system.get_quota(mount, int(handle["project_id"]))
        if (
            quota.hard_bytes != handle["hard_bytes"]
            or quota.hard_inodes != handle["hard_inodes"]
            or quota.used_bytes > quota.hard_bytes
            or quota.used_inodes > quota.hard_inodes
        ):
            raise ProjectQuotaError(
                "quota-enforcement-changed",
                "project quota limits changed",
            )
        return quota

    def attach(
        self,
        request: ResourceRequest,
        state: Mapping[str, object],
        worker_pid: int,
    ) -> None:
        self._policy(request)
        self._validate_handle(state)
        if not isinstance(worker_pid, int) or isinstance(worker_pid, bool) or worker_pid <= 0:
            raise ProjectQuotaError(
                "quota-worker-invalid",
                "project quota worker identity is invalid",
            )

    def _measurement(
        self,
        request: ResourceRequest,
        state: Mapping[str, object],
    ) -> ResourceMeasurement:
        policy = self._policy(request)
        handle, _manifest, mount = self._resolve(request, state, require_path=True)
        quota = self._verify_enforcement(handle, mount)
        observations: list[ResourceObservation] = []
        if quota.used_bytes >= quota.hard_bytes:
            observations.append(
                ResourceObservation(policy.storage_name, "storage-byte-limit-hit")
            )
        if quota.used_inodes >= quota.hard_inodes:
            observations.append(
                ResourceObservation(policy.inode_name, "storage-inode-limit-hit")
            )
        return ResourceMeasurement(
            {
                policy.storage_name: quota.used_bytes,
                policy.inode_name: quota.used_inodes,
            },
            tuple(observations),
        )

    def usage(
        self,
        request: ResourceRequest,
        state: Mapping[str, object],
    ) -> ResourceMeasurement:
        return self._measurement(request, state)

    def finish(
        self,
        request: ResourceRequest,
        state: Mapping[str, object],
    ) -> ResourceMeasurement:
        return self._measurement(request, state)

    def cancel(
        self,
        request: ResourceRequest,
        state: Mapping[str, object],
    ) -> None:
        self._policy(request)
        self._resolve(request, state, require_path=True)

    @staticmethod
    def _make_deletable(path: Path, *, expected_device: int) -> None:
        def raise_walk_error(exc: OSError) -> None:
            raise exc

        try:
            path.chmod(0o700)
            for root, directories, files in os.walk(
                path,
                topdown=True,
                onerror=raise_walk_error,
                followlinks=False,
            ):
                root_path = Path(root)
                root_details = root_path.lstat()
                if (
                    stat.S_ISLNK(root_details.st_mode)
                    or not stat.S_ISDIR(root_details.st_mode)
                    or root_details.st_dev != expected_device
                ):
                    raise ProjectQuotaError(
                        "quota-tree-boundary-changed",
                        "project quota scratch tree crosses an unowned boundary",
                    )
                root_path.chmod(0o700)
                for name in [*directories, *files]:
                    child = root_path / name
                    details = child.lstat()
                    if details.st_dev != expected_device:
                        raise ProjectQuotaError(
                            "quota-tree-boundary-changed",
                            "project quota scratch tree crosses an unowned boundary",
                        )
                    if stat.S_ISDIR(details.st_mode) and not stat.S_ISLNK(
                        details.st_mode
                    ):
                        child.chmod(0o700)
        except ProjectQuotaError:
            raise
        except OSError as exc:
            raise ProjectQuotaError(
                "quota-tree-cleanup-failed",
                "project quota scratch data cannot be made removable",
            ) from exc

    def cleanup(
        self,
        request: ResourceRequest,
        state: Mapping[str, object],
    ) -> None:
        self._policy(request)
        current_mount = self.system.probe(self.storage_parent)
        self._prepare_paths(current_mount)
        descriptors = self._locked(current_mount)
        try:
            handle, manifest, mount = self._resolve(
                request,
                state,
                require_path=False,
            )
            path = Path(str(handle["path"]))
            if manifest:
                manifest["phase"] = "cleaning"
                self._write_json(self._manifest_path(request.run_id), manifest)
            if self._entry_present(path):
                if not manifest:  # pragma: no cover - guarded by _resolve
                    raise ProjectQuotaError(
                        "quota-manifest-missing",
                        "project quota ownership manifest disappeared",
                    )
                details = self._validate_tree(path, manifest)
                try:
                    path.chmod(0o700)
                except OSError as exc:
                    raise ProjectQuotaError(
                        "quota-tree-cleanup-failed",
                        "project quota scratch root cannot be made removable",
                    ) from exc
                self._verify_enforcement(handle, mount)
                self._make_deletable(path, expected_device=details.st_dev)
                try:
                    shutil.rmtree(path)
                except OSError as exc:
                    raise ProjectQuotaError(
                        "quota-tree-cleanup-failed",
                        "project quota scratch data could not be removed",
                    ) from exc
            deadline = time.monotonic() + self.cleanup_timeout
            while True:
                quota = self.system.get_quota(mount, int(handle["project_id"]))
                if quota.used_bytes == 0 and quota.used_inodes == 0:
                    break
                if time.monotonic() >= deadline:
                    raise ProjectQuotaError(
                        "quota-usage-still-live",
                        "project quota usage remains after descendant cleanup",
                    )
                time.sleep(0.02)
            if (
                quota.hard_bytes == handle["hard_bytes"]
                and quota.hard_inodes == handle["hard_inodes"]
            ):
                self.system.set_quota(
                    mount,
                    int(handle["project_id"]),
                    hard_bytes=0,
                    hard_inodes=0,
                )
            elif quota.hard_bytes != 0 or quota.hard_inodes != 0:
                raise ProjectQuotaError(
                    "quota-enforcement-changed",
                    "project quota limits changed before cleanup",
                )
            cleared = self.system.get_quota(mount, int(handle["project_id"]))
            if not self._zero(cleared):
                raise ProjectQuotaError(
                    "quota-cleanup-unverified",
                    "project quota identity could not be reclaimed",
                )
            self._manifest_path(request.run_id).unlink(missing_ok=True)
        finally:
            self._unlock(descriptors)
