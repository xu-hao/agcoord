"""Delegated Linux cgroup v2 ownership and per-run leaf lifecycle."""

from __future__ import annotations

from dataclasses import dataclass
import ctypes
import errno
import hashlib
import json
import os
from pathlib import Path
import re
import select
import signal
import stat
import time
from typing import Mapping, Protocol
from uuid import uuid4

from .resources import (
    RESOURCE_OPERATIONS,
    ResourceBackendError,
    ResourceMeasurement,
    ResourceObservation,
    ResourceRequest,
)


CGROUP_BACKEND = "cgroup-v2"
CGROUP_ISOLATE_ENV = "_AGCOORD_CGROUP_ISOLATE"
_CGROUP_V2_FILES = frozenset({"cgroup.procs", "cgroup.events"})
_OWNER_KEYS = frozenset(
    {
        "version",
        "root",
        "root_device",
        "root_inode",
        "owner",
        "owner_device",
        "owner_inode",
    }
)
_HANDLE_KEYS = frozenset(
    {
        "version",
        "owner",
        "owner_device",
        "owner_inode",
        "leaf",
        "leaf_device",
        "leaf_inode",
        "token",
    }
)
_MANIFEST_KEYS = frozenset({"version", "run_id", "handle"})
_OWNER_NAME = re.compile(r"^agcoord-u[0-9]+-[0-9a-f]{16}$")
_LEAF_NAME = re.compile(r"^run-[0-9a-f]{16}-[0-9a-f]{12}$")
_TOKEN = re.compile(r"^[0-9a-f]{32}$")
_TOKEN_OR_REASON = re.compile(r"^[a-z][a-z0-9-]{0,63}$")
_CONTROLLER = re.compile(r"^[a-z][a-z0-9_-]{0,31}$")
_MAX_METADATA_BYTES = 64 * 1024
_CLONE_NEWNS = 0x00020000
_CLONE_NEWCGROUP = 0x02000000
_CLONE_NEWUSER = 0x10000000
_MS_NOSUID = 2
_MS_NODEV = 4
_MS_NOEXEC = 8
_MS_REC = 16384
_MS_PRIVATE = 1 << 18
_MNT_DETACH = 2
CPU_PERIOD_USEC = 100_000
_CONTROL_BINDINGS = {
    ("cpu", "logical-cpu"): "cpu",
    ("processes", "processes"): "pids",
    ("memory", "bytes"): "memory.max",
    ("memory-high", "bytes"): "memory.high",
    ("swap", "bytes"): "memory.swap.max",
    ("tmpfs", "bytes"): "tmpfs.size",
    ("inodes", "inodes"): "tmpfs.nr_inodes",
}
_LIFECYCLE_BINDING = ("generic", "admission-unit")
_CONTROLLER_FILE = re.compile(
    r"^(?:cpu|pids|memory(?:\.swap)?)\.[a-z][a-z0-9_.-]{0,31}$"
)
_METRIC_KEY = re.compile(r"^[a-z][a-z0-9_.-]{0,63}$")
_TMPFS_REPORT_KEYS = frozenset(
    {
        "version",
        "token",
        "peak_bytes",
        "peak_inodes",
        "terminal_bytes",
        "terminal_inodes",
        "byte_limit_hit",
        "inode_limit_hit",
    }
)


class CgroupV2Error(ResourceBackendError):
    """A stable cgroup refusal whose host details remain broker-private."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(code, message)


class CgroupOwnershipError(CgroupV2Error):
    """A configured path no longer names the cgroup AGCoord created."""


class CgroupIsolationError(CgroupV2Error):
    """The worker could not hide and protect its cgroup control boundary."""


@dataclass(frozen=True)
class CgroupIdentity:
    device: int
    inode: int


@dataclass(frozen=True)
class CgroupProbe:
    available: bool
    reason: str | None
    controllers: frozenset[str]


@dataclass(frozen=True)
class CgroupMount:
    path: Path
    options: frozenset[str]


@dataclass(frozen=True)
class TmpfsPolicy:
    size_name: str
    inode_name: str
    memory_name: str
    size: int
    inodes: int


class CgroupV2System(Protocol):
    """Small kernel seam used by the backend and deterministic subprocess tests."""

    def probe(self, root: Path) -> CgroupProbe: ...

    def identity(self, path: Path) -> CgroupIdentity | None: ...

    def create_group(self, parent: Path, name: str) -> CgroupIdentity: ...

    def attach(self, path: Path, pid: int) -> None: ...

    def members(self, path: Path) -> set[int]: ...

    def populated(self, path: Path) -> bool: ...

    def kill(self, path: Path) -> None: ...

    def remove_group(self, path: Path) -> None: ...

    def monotonic_ns(self) -> int: ...

    def enable_controllers(self, path: Path, controllers: set[str]) -> None: ...

    def write_file(self, path: Path, name: str, value: str) -> None: ...

    def read_file(self, path: Path, name: str) -> str: ...

    def swap_total_bytes(self) -> int: ...


def _decode_mount_path(value: str) -> str:
    return re.sub(
        r"\\([0-7]{3})",
        lambda match: chr(int(match.group(1), 8)),
        value,
    )


def _write_all(descriptor: int, payload: bytes) -> None:
    view = memoryview(payload)
    while view:
        written = os.write(descriptor, view)
        if written <= 0:  # pragma: no cover - write(2) contract
            raise OSError(errno.EIO, "write made no progress")
        view = view[written:]


def _write_kernel_file(path: Path, value: str) -> None:
    flags = os.O_WRONLY | os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags)
    try:
        _write_all(descriptor, value.encode("ascii"))
    finally:
        os.close(descriptor)


def _cgroup2_mounts(mountinfo: Path) -> list[CgroupMount]:
    try:
        lines = mountinfo.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        raise CgroupIsolationError(
            "mountinfo-unreadable",
            "cgroup mount information is unavailable",
        ) from exc
    mounts: list[CgroupMount] = []
    for line in lines:
        try:
            left, right = line.split(" - ", 1)
            fields = left.split()
            right_fields = right.split()
            if right_fields[0] != "cgroup2":
                continue
            mounted = Path(_decode_mount_path(fields[4]))
            if not mounted.is_absolute():
                continue
            options = frozenset(
                fields[5].split(",") + right_fields[2].split(",")
            )
        except (IndexError, ValueError):
            continue
        mounts.append(CgroupMount(mounted, options))
    return mounts


def _filesystem_mounts(
    mountinfo: Path,
) -> list[tuple[Path, str, frozenset[str]]]:
    try:
        lines = mountinfo.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        raise CgroupIsolationError(
            "mountinfo-unreadable",
            "mount information is unavailable",
        ) from exc
    mounts: list[tuple[Path, str, frozenset[str]]] = []
    for line in lines:
        try:
            left, right = line.split(" - ", 1)
            fields = left.split()
            right_fields = right.split()
            mounted = Path(_decode_mount_path(fields[4]))
            filesystem = right_fields[0]
            options = frozenset(
                fields[5].split(",") + right_fields[2].split(",")
            )
            if not mounted.is_absolute() or not filesystem:
                continue
        except (IndexError, ValueError):
            continue
        mounts.append((mounted, filesystem, options))
    return mounts


def _covering_mounts(mounts: list[CgroupMount]) -> list[CgroupMount]:
    """Select mounts whose replacement hides every visible cgroup2 mount."""

    selected: list[CgroupMount] = []
    for candidate in sorted(mounts, key=lambda mount: len(mount.path.parts)):
        if any(
            os.path.commonpath((str(candidate.path), str(parent.path)))
            == str(parent.path)
            for parent in selected
        ):
            continue
        selected.append(candidate)
    return selected


def _call_libc(name: str, *arguments: object) -> None:
    libc = ctypes.CDLL(None, use_errno=True)
    function = getattr(libc, name)
    function.restype = ctypes.c_int
    if name == "unshare":
        function.argtypes = [ctypes.c_int]
    elif name == "mount":
        function.argtypes = [
            ctypes.c_char_p,
            ctypes.c_char_p,
            ctypes.c_char_p,
            ctypes.c_ulong,
            ctypes.c_char_p,
        ]
    elif name == "umount2":
        function.argtypes = [ctypes.c_char_p, ctypes.c_int]
    if function(*arguments) != 0:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error))


def _write_namespace_map(path: Path, value: str) -> None:
    try:
        _write_kernel_file(path, value)
    except OSError as exc:
        raise CgroupIsolationError(
            "namespace-mapping-failed",
            "worker user namespace identity could not be mapped",
        ) from exc


def isolate_current_cgroup(
    *,
    mountinfo: Path = Path("/proc/self/mountinfo"),
) -> None:
    """Root the current worker's cgroup view at its already-attached leaf.

    The initial cgroup2 mount must use ``nsdelegate``. A private user, cgroup,
    and mount namespace then hides every host cgroup2 mount behind a new view
    rooted at the worker's current cgroup. The kernel makes controller files at
    that namespace root unwritable while descendant limits remain hierarchical.
    """

    mounts = _cgroup2_mounts(mountinfo)
    covering = _covering_mounts(mounts)
    if not covering or any("nsdelegate" not in mount.options for mount in covering):
        raise CgroupIsolationError(
            "namespace-delegation-unavailable",
            "cgroup2 mounts do not provide namespace delegation",
        )
    uid = os.getuid()
    gid = os.getgid()
    try:
        _call_libc(
            "unshare",
            _CLONE_NEWUSER | _CLONE_NEWCGROUP | _CLONE_NEWNS,
        )
    except OSError as exc:
        raise CgroupIsolationError(
            "namespace-isolation-unavailable",
            "worker cgroup namespace isolation is unavailable",
        ) from exc

    setgroups = Path("/proc/self/setgroups")
    if setgroups.exists():
        _write_namespace_map(setgroups, "deny\n")
    _write_namespace_map(Path("/proc/self/uid_map"), f"{uid} {uid} 1\n")
    _write_namespace_map(Path("/proc/self/gid_map"), f"{gid} {gid} 1\n")
    try:
        _call_libc("mount", None, b"/", None, _MS_REC | _MS_PRIVATE, None)
        for mount in covering:
            _call_libc(
                "mount",
                b"none",
                os.fsencode(mount.path),
                b"cgroup2",
                _MS_NOSUID | _MS_NODEV | _MS_NOEXEC,
                None,
            )
    except OSError as exc:
        raise CgroupIsolationError(
            "namespace-mount-failed",
            "worker cgroup namespace mounts could not be isolated",
        ) from exc

    try:
        cgroup_lines = Path("/proc/self/cgroup").read_text(encoding="ascii").splitlines()
    except (OSError, UnicodeError) as exc:
        raise CgroupIsolationError(
            "namespace-verification-failed",
            "worker cgroup namespace could not be verified",
        ) from exc
    if "0::/" not in cgroup_lines or not all(
        (mount.path / "cgroup.events").is_file() for mount in covering
    ):
        raise CgroupIsolationError(
            "namespace-verification-failed",
            "worker cgroup namespace is not rooted at the run cgroup",
        )


def mount_current_tmpfs(
    target: str | os.PathLike[str],
    *,
    size: int,
    inodes: int,
    mountinfo: Path = Path("/proc/self/mountinfo"),
) -> os.statvfs_result:
    """Mount and verify one private, bounded tmpfs in the current mount namespace."""

    selected = Path(target)
    if (
        not selected.is_absolute()
        or not isinstance(size, int)
        or isinstance(size, bool)
        or size <= 0
        or not isinstance(inodes, int)
        or isinstance(inodes, bool)
        or inodes <= 0
    ):
        raise CgroupIsolationError(
            "tmpfs-setup-invalid",
            "tmpfs setup values are invalid",
        )
    try:
        details = selected.lstat()
        resolved = selected.resolve(strict=True)
    except OSError as exc:
        raise CgroupIsolationError(
            "tmpfs-target-invalid",
            "tmpfs target is unavailable",
        ) from exc
    if (
        resolved != selected
        or stat.S_ISLNK(details.st_mode)
        or not stat.S_ISDIR(details.st_mode)
        or details.st_uid != os.getuid()
        or details.st_mode & 0o077
    ):
        raise CgroupIsolationError(
            "tmpfs-target-invalid",
            "tmpfs target is not a private owned directory",
        )
    options = (
        f"size={size},nr_inodes={inodes},mode=700,uid={os.getuid()},gid={os.getgid()}"
    ).encode("ascii")
    mounted = False
    try:
        _call_libc(
            "mount",
            b"agcoord-tmpfs",
            os.fsencode(selected),
            b"tmpfs",
            _MS_NOSUID | _MS_NODEV | _MS_NOEXEC,
            options,
        )
        mounted = True
        matching = [
            mount
            for mount in _filesystem_mounts(mountinfo)
            if mount[0] == selected
        ]
        if len(matching) != 1 or matching[0][1] != "tmpfs" or not {
            "nodev",
            "noexec",
            "nosuid",
        } <= matching[0][2]:
            raise CgroupIsolationError(
                "tmpfs-mount-unverified",
                "tmpfs mount options could not be verified",
            )
        mounted_details = selected.stat()
        if (
            not stat.S_ISDIR(mounted_details.st_mode)
            or mounted_details.st_uid != os.getuid()
            or mounted_details.st_gid != os.getgid()
            or stat.S_IMODE(mounted_details.st_mode) != 0o700
        ):
            raise CgroupIsolationError(
                "tmpfs-mount-unverified",
                "tmpfs root ownership could not be verified",
            )
        usage = os.statvfs(selected)
        if usage.f_blocks * usage.f_frsize > size:
            raise CgroupIsolationError(
                "tmpfs-size-unverified",
                "tmpfs byte capacity exceeds its requested limit",
            )
        if usage.f_files > inodes:
            raise CgroupIsolationError(
                "tmpfs-inodes-unverified",
                "tmpfs inode capacity exceeds its requested limit",
            )
        return usage
    except CgroupIsolationError:
        if mounted:
            try:
                _call_libc("umount2", os.fsencode(selected), _MNT_DETACH)
            except OSError:
                pass
        raise
    except OSError as exc:
        if mounted:
            try:
                _call_libc("umount2", os.fsencode(selected), _MNT_DETACH)
            except OSError:
                pass
        raise CgroupIsolationError(
            "tmpfs-mount-unavailable",
            "private tmpfs mounting is unavailable",
        ) from exc


def unmount_current_tmpfs(target: str | os.PathLike[str]) -> None:
    """Detach a setup that will not be released to user code."""

    try:
        _call_libc("umount2", os.fsencode(target), _MNT_DETACH)
    except OSError as exc:
        raise CgroupIsolationError(
            "tmpfs-unmount-failed",
            "private tmpfs setup could not be rolled back",
        ) from exc


class LinuxCgroupV2System:
    """Raw cgroup v2 filesystem operations for an explicitly delegated subtree."""

    def __init__(self, *, mountinfo: Path = Path("/proc/self/mountinfo")) -> None:
        self.mountinfo = mountinfo

    def probe(self, root: Path) -> CgroupProbe:
        try:
            details = root.lstat()
        except FileNotFoundError:
            return CgroupProbe(False, "root-missing", frozenset())
        except OSError:
            return CgroupProbe(False, "root-unreadable", frozenset())
        if stat.S_ISLNK(details.st_mode) or not stat.S_ISDIR(details.st_mode):
            return CgroupProbe(False, "root-invalid", frozenset())

        try:
            mounts = _cgroup2_mounts(self.mountinfo)
        except CgroupIsolationError:
            return CgroupProbe(False, "mountinfo-unreadable", frozenset())
        mount = self._mount_for(root, mounts)
        if mount is None or mount[0] != "cgroup2":
            return CgroupProbe(False, "not-cgroup-v2", frozenset())
        if "ro" in mount[1]:
            return CgroupProbe(False, "delegation-read-only", frozenset())
        if "nsdelegate" not in mount[1]:
            return CgroupProbe(
                False,
                "namespace-delegation-unavailable",
                frozenset(),
            )
        if not all((root / name).is_file() for name in _CGROUP_V2_FILES):
            return CgroupProbe(False, "delegation-invalid", frozenset())
        try:
            controllers = frozenset(
                (root / "cgroup.controllers").read_text(encoding="utf-8").split()
            )
        except (FileNotFoundError, OSError, UnicodeError):
            return CgroupProbe(False, "controllers-unreadable", frozenset())
        if any(not _CONTROLLER.fullmatch(name) for name in controllers):
            return CgroupProbe(False, "controllers-invalid", frozenset())

        probe_name = f".agcoord-probe-{uuid4().hex[:12]}"
        probe_path = root / probe_name
        created = False
        reason: str | None = None
        try:
            self.create_group(root, probe_name)
            created = True
            if not all((probe_path / name).is_file() for name in _CGROUP_V2_FILES):
                reason = "delegation-invalid"
            elif not (probe_path / "cgroup.kill").is_file():
                reason = "kill-unsupported"
            else:
                reason = self._probe_isolation(probe_path)
                if reason is None:
                    self.kill(probe_path)
                    if self.populated(probe_path):
                        reason = "kill-failed"
        except PermissionError:
            reason = "delegation-undelegated"
        except OSError as exc:
            reason = (
                "delegation-read-only"
                if exc.errno == errno.EROFS
                else "delegation-unavailable"
            )
        finally:
            if created:
                try:
                    self.remove_group(probe_path)
                except OSError:
                    reason = "probe-cleanup-failed"
        return CgroupProbe(reason is None, reason, controllers if reason is None else frozenset())

    def _mount_for(
        self,
        root: Path,
        mounts: list[CgroupMount] | None = None,
    ) -> tuple[str, frozenset[str]] | None:
        selected: tuple[int, str, frozenset[str]] | None = None
        root_text = str(root)
        try:
            candidates = _cgroup2_mounts(self.mountinfo) if mounts is None else mounts
        except CgroupIsolationError:
            return None
        for mount in candidates:
            try:
                mounted = str(mount.path)
                common = os.path.commonpath((root_text, mounted))
            except ValueError:
                continue
            if common != mounted:
                continue
            candidate = (len(mounted), "cgroup2", mount.options)
            if selected is None or candidate[0] > selected[0]:
                selected = candidate
        if selected is None:
            return None
        return selected[1], selected[2]

    def _probe_isolation(self, probe_path: Path) -> str | None:
        release_read, release_write = os.pipe()
        result_read, result_write = os.pipe()
        child = -1
        try:
            child = os.fork()
            if child == 0:  # pragma: no branch - child exits through os._exit
                os.close(release_write)
                os.close(result_read)
                code = "namespace-isolation-failed"
                try:
                    if os.read(release_read, 1) != b"1":
                        raise CgroupIsolationError(
                            "namespace-isolation-failed",
                            "cgroup isolation probe was not released",
                        )
                    isolate_current_cgroup(mountinfo=self.mountinfo)
                    try:
                        _write_kernel_file(
                            _covering_mounts(_cgroup2_mounts(self.mountinfo))[0].path
                            / "cgroup.kill",
                            "1\n",
                        )
                    except PermissionError:
                        code = "ok"
                    else:
                        code = "controller-files-exposed"
                except CgroupV2Error as exc:
                    code = exc.code
                except OSError:
                    code = "namespace-isolation-failed"
                try:
                    _write_all(result_write, code.encode("ascii"))
                except OSError:
                    pass
                os._exit(0 if code == "ok" else 1)

            os.close(release_read)
            release_read = -1
            os.close(result_write)
            result_write = -1
            self.attach(probe_path, child)
            if child not in self.members(probe_path):
                return "attach-unverified"
            _write_all(release_write, b"1")
            os.close(release_write)
            release_write = -1
            readable, _writable, _exceptional = select.select(
                [result_read],
                [],
                [],
                5.0,
            )
            if not readable:
                return "namespace-isolation-timeout"
            payload = os.read(result_read, 256).decode("ascii", errors="replace")
            _pid, status = os.waitpid(child, 0)
            child = -1
            if os.WIFSIGNALED(status):
                return "controller-files-exposed"
            return None if payload == "ok" and os.WEXITSTATUS(status) == 0 else (
                payload if _TOKEN_OR_REASON.fullmatch(payload) else "namespace-isolation-failed"
            )
        except (OSError, ChildProcessError):
            return "namespace-isolation-unavailable"
        finally:
            for descriptor in (release_read, release_write, result_read, result_write):
                if descriptor >= 0:
                    try:
                        os.close(descriptor)
                    except OSError:
                        pass
            if child > 0:
                try:
                    os.kill(child, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                try:
                    os.waitpid(child, 0)
                except ChildProcessError:
                    pass

    def identity(self, path: Path) -> CgroupIdentity | None:
        try:
            details = path.lstat()
        except FileNotFoundError:
            return None
        if stat.S_ISLNK(details.st_mode) or not stat.S_ISDIR(details.st_mode):
            raise CgroupOwnershipError(
                "cgroup-path-invalid",
                "configured cgroup path is not a real directory",
            )
        return CgroupIdentity(details.st_dev, details.st_ino)

    def create_group(self, parent: Path, name: str) -> CgroupIdentity:
        os.mkdir(parent / name, mode=0o700)
        identity = self.identity(parent / name)
        if identity is None:  # pragma: no cover - kernel mkdir contract
            raise CgroupV2Error("create-failed", "created cgroup disappeared")
        return identity

    def _write_control(self, path: Path, value: str) -> None:
        _write_kernel_file(path, value)

    def attach(self, path: Path, pid: int) -> None:
        self._write_control(path / "cgroup.procs", f"{pid}\n")

    def members(self, path: Path) -> set[int]:
        try:
            lines = (path / "cgroup.procs").read_text(encoding="ascii").splitlines()
            return {int(line) for line in lines if line}
        except (OSError, UnicodeError) as exc:
            raise CgroupV2Error(
                "membership-unreadable",
                "cgroup membership file is unreadable",
            ) from exc
        except ValueError as exc:
            raise CgroupV2Error(
                "membership-invalid",
                "cgroup membership file contained a non-PID value",
            ) from exc

    def populated(self, path: Path) -> bool:
        try:
            fields = {
                name: value
                for name, value in (
                    line.split(maxsplit=1)
                    for line in (path / "cgroup.events")
                    .read_text(encoding="ascii")
                    .splitlines()
                )
            }
            return fields["populated"] == "1"
        except (OSError, UnicodeError) as exc:
            raise CgroupV2Error(
                "events-unreadable",
                "cgroup.events is unreadable",
            ) from exc
        except (KeyError, ValueError) as exc:
            raise CgroupV2Error(
                "events-invalid",
                "cgroup.events has no valid populated field",
            ) from exc

    def kill(self, path: Path) -> None:
        self._write_control(path / "cgroup.kill", "1\n")

    def remove_group(self, path: Path) -> None:
        os.rmdir(path)

    def monotonic_ns(self) -> int:
        return time.monotonic_ns()

    def _controller_names(self, path: Path, name: str) -> set[str]:
        try:
            values = (path / name).read_text(encoding="ascii").split()
        except (OSError, UnicodeError) as exc:
            raise CgroupV2Error(
                "controller-state-unreadable",
                "cgroup controller state is unreadable",
            ) from exc
        if any(not _CONTROLLER.fullmatch(value) for value in values):
            raise CgroupV2Error(
                "controller-state-invalid",
                "cgroup controller state is invalid",
            )
        return set(values)

    def enable_controllers(self, path: Path, controllers: set[str]) -> None:
        if not controllers:
            return
        if any(not _CONTROLLER.fullmatch(name) for name in controllers):
            raise CgroupV2Error(
                "controller-invalid",
                "requested cgroup controller name is invalid",
            )
        available = self._controller_names(path, "cgroup.controllers")
        if not controllers <= available:
            raise CgroupV2Error(
                "controller-unavailable",
                "a requested cgroup controller is unavailable",
            )
        enabled = self._controller_names(path, "cgroup.subtree_control")
        missing = controllers - enabled
        if missing:
            try:
                self._write_control(
                    path / "cgroup.subtree_control",
                    " ".join(f"+{name}" for name in sorted(missing)) + "\n",
                )
            except OSError as exc:
                raise CgroupV2Error(
                    "controller-enable-failed",
                    "requested cgroup controllers could not be enabled",
                ) from exc
        if not controllers <= self._controller_names(path, "cgroup.subtree_control"):
            raise CgroupV2Error(
                "controller-enable-unverified",
                "requested cgroup controllers were not enabled",
            )

    def write_file(self, path: Path, name: str, value: str) -> None:
        if not _CONTROLLER_FILE.fullmatch(name):
            raise CgroupV2Error(
                "controller-file-invalid",
                "requested cgroup controller file is invalid",
            )
        try:
            self._write_control(path / name, f"{value.rstrip()}\n")
        except OSError as exc:
            raise CgroupV2Error(
                "controller-write-failed",
                "cgroup controller value could not be written",
            ) from exc

    def read_file(self, path: Path, name: str) -> str:
        if not _CONTROLLER_FILE.fullmatch(name):
            raise CgroupV2Error(
                "controller-file-invalid",
                "requested cgroup controller file is invalid",
            )
        try:
            return (path / name).read_text(encoding="ascii")
        except FileNotFoundError as exc:
            raise CgroupV2Error(
                "controller-file-missing",
                "cgroup controller file is unavailable",
            ) from exc
        except (OSError, UnicodeError) as exc:
            raise CgroupV2Error(
                "controller-read-failed",
                "cgroup controller value could not be read",
            ) from exc

    def swap_total_bytes(self) -> int:
        try:
            lines = Path("/proc/meminfo").read_text(encoding="ascii").splitlines()
            matches = [line.split() for line in lines if line.startswith("SwapTotal:")]
            if (
                len(matches) != 1
                or len(matches[0]) != 3
                or matches[0][0] != "SwapTotal:"
                or not matches[0][1].isascii()
                or not matches[0][1].isdecimal()
                or matches[0][2] != "kB"
            ):
                raise ValueError
            return int(matches[0][1]) * 1024
        except (OSError, UnicodeError, ValueError) as exc:
            raise CgroupV2Error(
                "swap-state-unavailable",
                "host swap availability could not be determined",
            ) from exc


class CgroupV2Backend:
    """Own one collision-safe cgroup v2 leaf for each prepared run."""

    def __init__(
        self,
        root: str | os.PathLike[str] | None,
        *,
        state_dir: str | os.PathLike[str],
        system: CgroupV2System | None = None,
        empty_timeout: float = 5.0,
    ) -> None:
        if empty_timeout <= 0:
            raise ValueError("cgroup empty timeout must be positive")
        self.state_dir = Path(state_dir).absolute()
        self.metadata_dir = self.state_dir / "cgroup-v2"
        self.system = LinuxCgroupV2System() if system is None else system
        self.empty_timeout = empty_timeout
        self._configuration_reason: str | None = None
        if root is None or not os.fspath(root):
            self.root: Path | None = None
        else:
            selected = Path(root).absolute()
            try:
                resolved = selected.resolve(strict=False)
            except OSError:
                self.root = selected
                self._configuration_reason = "root-invalid"
            else:
                self.root = selected
                if resolved != selected:
                    self._configuration_reason = "root-invalid"
        owner_hash = hashlib.sha256(
            f"{os.getuid()}:{self.state_dir}".encode()
        ).hexdigest()[:16]
        self.owner_name = f"agcoord-u{os.getuid()}-{owner_hash}"
        self._probe_result: CgroupProbe | None = None
        self._cpu_samples: dict[str, tuple[int, int, int]] = {}
        self._pids_peaks: dict[str, int] = {}
        self._memory_peaks: dict[str, int] = {}
        self._swap_peaks: dict[str, int] = {}

    @property
    def isolate_workers(self) -> bool:
        """Whether the real kernel backend needs the launcher namespace step."""

        return isinstance(self.system, LinuxCgroupV2System)

    @classmethod
    def from_config(
        cls,
        cgroup_root: str | None,
        *,
        state_dir: str | os.PathLike[str],
    ) -> CgroupV2Backend:
        """Build the backend from one state directory's configured delegated root."""
        return cls(cgroup_root, state_dir=state_dir)

    def _probe(self) -> CgroupProbe:
        if self._probe_result is not None:
            return self._probe_result
        if self.root is None:
            result = CgroupProbe(False, "delegation-unconfigured", frozenset())
        elif self._configuration_reason is not None:
            result = CgroupProbe(False, self._configuration_reason, frozenset())
        else:
            try:
                result = self.system.probe(self.root)
            except Exception:
                result = CgroupProbe(False, "probe-failed", frozenset())
        if result.available and result.reason is not None:
            result = CgroupProbe(False, "probe-invalid", frozenset())
        if not result.available and result.reason is None:
            result = CgroupProbe(False, "probe-invalid", frozenset())
        self._probe_result = result
        return result

    def probe(self) -> Mapping[str, object]:
        result = self._probe()
        kinds = {"generic"}
        units = {"admission-unit"}
        if "cpu" in result.controllers:
            kinds.add("cpu")
            units.add("logical-cpu")
        if "pids" in result.controllers:
            kinds.add("processes")
            units.add("processes")
        if "memory" in result.controllers:
            kinds.update({"inodes", "memory", "memory-high", "swap", "tmpfs"})
            units.update({"bytes", "inodes"})
        return {
            "available": result.available,
            "kinds": sorted(kinds) if result.available else [],
            "units": sorted(units) if result.available else [],
            "operations": list(RESOURCE_OPERATIONS) if result.available else [],
            "reason": result.reason,
        }

    def _validate_request(self, request: ResourceRequest) -> None:
        if (
            request.backend != CGROUP_BACKEND
            or not request.resources
            or set(request.bindings) != set(request.resources)
        ):
            raise CgroupV2Error("request-invalid", "cgroup request is invalid")
        for name, binding in request.bindings.items():
            pair = (binding["kind"], binding["unit"])
            if pair != _LIFECYCLE_BINDING and pair not in _CONTROL_BINDINGS:
                raise CgroupV2Error(
                    "request-unsupported",
                    "cgroup backend does not support the requested typed unit",
                )

    def _controller_resources(self, request: ResourceRequest) -> dict[str, str]:
        selected: dict[str, str] = {}
        for name, binding in request.bindings.items():
            control = _CONTROL_BINDINGS.get((binding["kind"], binding["unit"]))
            if control is None:
                continue
            if control in selected:
                raise CgroupV2Error(
                    "controller-ambiguous",
                    "one run cannot bind two names to the same cgroup control",
                )
            selected[control] = name
        return selected

    def _controller_settings(self, request: ResourceRequest) -> dict[str, str]:
        resources = self._controller_resources(request)
        self._tmpfs_policy(request, resources=resources)
        selected: dict[str, str] = {}
        if cpu_name := resources.get("cpu"):
            quota = request.resources[cpu_name] * CPU_PERIOD_USEC
            selected["cpu.max"] = f"{quota} {CPU_PERIOD_USEC}"
        if pids_name := resources.get("pids"):
            selected["pids.max"] = str(request.resources[pids_name])
        hard_name = resources.get("memory.max")
        high_name = resources.get("memory.high")
        swap_name = resources.get("memory.swap.max")
        if hard_name or high_name or swap_name:
            hard = request.resources[hard_name] if hard_name else None
            high = request.resources[high_name] if high_name else None
            if hard is not None and high is not None and high > hard:
                raise CgroupV2Error(
                    "memory-limit-impossible",
                    "memory.high cannot exceed memory.max",
                )
            if swap_name and self.system.swap_total_bytes() == 0:
                raise CgroupV2Error(
                    "swap-disabled",
                    "a positive swap budget requires host swap",
                )
            selected["memory.high"] = "max" if high is None else str(high)
            selected["memory.max"] = "max" if hard is None else str(hard)
            selected["memory.swap.max"] = (
                str(request.resources[swap_name])
                if swap_name
                else ("0" if hard_name else "max")
            )
            selected["memory.oom.group"] = "1" if hard_name else "0"
        return selected

    def _tmpfs_policy(
        self,
        request: ResourceRequest,
        *,
        resources: Mapping[str, str] | None = None,
    ) -> TmpfsPolicy | None:
        selected = (
            self._controller_resources(request)
            if resources is None
            else dict(resources)
        )
        size_name = selected.get("tmpfs.size")
        inode_name = selected.get("tmpfs.nr_inodes")
        if size_name is None and inode_name is None:
            return None
        if size_name is None or inode_name is None:
            raise CgroupV2Error(
                "tmpfs-policy-incomplete",
                "tmpfs byte and inode controls must be requested together",
            )
        memory_name = selected.get("memory.max")
        if memory_name is None or request.bindings[memory_name]["mode"] != "required":
            raise CgroupV2Error(
                "tmpfs-memory-required",
                "bounded tmpfs requires a required hard memory envelope",
            )
        requested_size = request.resources[size_name]
        page_size = int(os.sysconf("SC_PAGE_SIZE"))
        size = requested_size - (requested_size % page_size)
        if size == 0:
            raise CgroupV2Error(
                "tmpfs-size-impossible",
                "tmpfs byte capacity is smaller than one memory page",
            )
        if size > request.resources[memory_name]:
            raise CgroupV2Error(
                "tmpfs-memory-impossible",
                "tmpfs capacity cannot exceed the hard memory envelope",
            )
        return TmpfsPolicy(
            size_name=size_name,
            inode_name=inode_name,
            memory_name=memory_name,
            size=size,
            inodes=request.resources[inode_name],
        )

    def _enable_controllers(
        self,
        request: ResourceRequest,
        owner_path: Path,
    ) -> None:
        controller_by_control = {
            "cpu": "cpu",
            "pids": "pids",
            "memory.high": "memory",
            "memory.max": "memory",
            "memory.swap.max": "memory",
        }
        controllers = {
            controller_by_control[control]
            for control in self._controller_resources(request)
            if control in controller_by_control
        }
        if not controllers:
            return
        result = self._probe()
        if not controllers <= result.controllers:
            raise CgroupV2Error(
                "controller-unavailable",
                "a requested cgroup controller is unavailable",
            )
        assert self.root is not None
        self.system.enable_controllers(self.root, controllers)
        self.system.enable_controllers(owner_path, controllers)

    def _configure_controller_values(
        self,
        request: ResourceRequest,
        leaf_path: Path,
    ) -> None:
        for name, expected in self._controller_settings(request).items():
            self.system.write_file(leaf_path, name, expected)
            observed = " ".join(self.system.read_file(leaf_path, name).split())
            if observed != expected:
                raise CgroupV2Error(
                    "controller-value-unverified",
                    "cgroup controller value could not be verified",
                )

    def _start_cpu_sample(self, request: ResourceRequest, leaf_path: Path) -> None:
        resources = self._controller_resources(request)
        if "cpu" not in resources:
            return
        stats = self._flat_values(self.system.read_file(leaf_path, "cpu.stat"))
        if "usage_usec" not in stats:
            raise CgroupV2Error(
                "cpu-stat-invalid",
                "cpu.stat does not contain aggregate usage",
            )
        self._cpu_samples[request.run_id] = (
            self.system.monotonic_ns(),
            stats["usage_usec"],
            0,
        )

    def _start_memory_sample(self, request: ResourceRequest, leaf_path: Path) -> None:
        resources = self._controller_resources(request)
        memory_names = [
            resources[control]
            for control in ("memory.max", "memory.high")
            if control in resources
        ]
        if memory_names:
            current = self._single_value(
                self.system.read_file(leaf_path, "memory.current"),
                code="memory-current-invalid",
            )
            peak = self._optional_peak(
                leaf_path,
                "memory.peak",
                current=current,
                code="memory-peak-invalid",
            )
            self._flat_values(self.system.read_file(leaf_path, "memory.events"))
            if "memory.high" in resources:
                self._pressure_totals(
                    self.system.read_file(leaf_path, "memory.pressure")
                )
            self._memory_peaks[request.run_id] = max(current, peak)
        if "memory.swap.max" in resources:
            current = self._single_value(
                self.system.read_file(leaf_path, "memory.swap.current"),
                code="swap-current-invalid",
            )
            peak = self._optional_peak(
                leaf_path,
                "memory.swap.peak",
                current=current,
                code="swap-peak-invalid",
            )
            self._flat_values(self.system.read_file(leaf_path, "memory.swap.events"))
            self._swap_peaks[request.run_id] = max(current, peak)

    def _require_available(self) -> None:
        result = self._probe()
        if not result.available:
            raise CgroupV2Error(
                str(result.reason),
                "configured cgroup v2 delegation is unavailable",
            )

    def _prepare_metadata(self) -> None:
        self.metadata_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        details = self.metadata_dir.lstat()
        if (
            stat.S_ISLNK(details.st_mode)
            or not stat.S_ISDIR(details.st_mode)
            or details.st_uid != os.getuid()
        ):
            raise CgroupOwnershipError(
                "metadata-invalid",
                "cgroup ownership metadata path is unsafe",
            )
        if details.st_mode & 0o077:
            self.metadata_dir.chmod(0o700)

    def _write_json(self, path: Path, value: Mapping[str, object]) -> None:
        temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
        try:
            descriptor = os.open(
                temporary,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC,
                0o600,
            )
            try:
                payload = json.dumps(
                    value,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode()
                _write_all(descriptor, payload)
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            os.replace(temporary, path)
            directory = os.open(path.parent, os.O_RDONLY | os.O_CLOEXEC)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        finally:
            temporary.unlink(missing_ok=True)

    def _read_json(self, path: Path) -> dict[str, object]:
        details = path.lstat()
        if (
            stat.S_ISLNK(details.st_mode)
            or not stat.S_ISREG(details.st_mode)
            or details.st_uid != os.getuid()
            or details.st_size > _MAX_METADATA_BYTES
        ):
            raise CgroupOwnershipError(
                "metadata-invalid",
                "cgroup ownership metadata file is unsafe",
            )
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise CgroupOwnershipError(
                "metadata-invalid",
                "cgroup ownership metadata is unreadable",
            ) from exc
        if not isinstance(raw, dict):
            raise CgroupOwnershipError(
                "metadata-invalid",
                "cgroup ownership metadata is not an object",
            )
        return raw

    @property
    def _owner_record_path(self) -> Path:
        return self.metadata_dir / "owner.json"

    def _manifest_path(self, run_id: str) -> Path:
        run_hash = hashlib.sha256(run_id.encode()).hexdigest()[:32]
        return self.metadata_dir / f"run-{run_hash}.json"

    def _tmpfs_report_path(self, run_id: str) -> Path:
        run_hash = hashlib.sha256(run_id.encode()).hexdigest()[:32]
        return self.metadata_dir / f"tmpfs-{run_hash}.json"

    def _owner_record(
        self,
        *,
        root_identity: CgroupIdentity,
        owner_identity: CgroupIdentity,
    ) -> dict[str, object]:
        assert self.root is not None
        return {
            "version": 1,
            "root": str(self.root),
            "root_device": root_identity.device,
            "root_inode": root_identity.inode,
            "owner": self.owner_name,
            "owner_device": owner_identity.device,
            "owner_inode": owner_identity.inode,
        }

    def _validate_owner_record(
        self,
        raw: Mapping[str, object],
    ) -> tuple[Path, CgroupIdentity | None]:
        if (
            set(raw) != _OWNER_KEYS
            or type(raw.get("version")) is not int
            or raw.get("version") != 1
        ):
            raise CgroupOwnershipError("owner-metadata-invalid", "owner record is invalid")
        if self.root is None or raw.get("root") != str(self.root):
            raise CgroupOwnershipError("root-changed", "delegated cgroup root changed")
        if raw.get("owner") != self.owner_name:
            raise CgroupOwnershipError("owner-changed", "cgroup owner identity changed")
        identity_fields = (
            "root_device",
            "root_inode",
            "owner_device",
            "owner_inode",
        )
        if any(
            not isinstance(raw.get(field), int)
            or isinstance(raw.get(field), bool)
            or int(raw[field]) < 0
            for field in identity_fields
        ):
            raise CgroupOwnershipError(
                "owner-metadata-invalid",
                "owner record identities are invalid",
            )
        root_identity = self.system.identity(self.root)
        expected_root = CgroupIdentity(
            int(raw["root_device"]),
            int(raw["root_inode"]),
        )
        if root_identity != expected_root:
            raise CgroupOwnershipError("root-reused", "delegated cgroup root was reused")
        owner_path = self.root / self.owner_name
        owner_identity = self.system.identity(owner_path)
        expected_owner = CgroupIdentity(
            int(raw["owner_device"]),
            int(raw["owner_inode"]),
        )
        if owner_identity is not None and owner_identity != expected_owner:
            raise CgroupOwnershipError("owner-reused", "AGCoord cgroup owner path was reused")
        return owner_path, owner_identity

    def _ensure_owner(self) -> tuple[Path, CgroupIdentity]:
        assert self.root is not None
        root_identity = self.system.identity(self.root)
        if root_identity is None:
            raise CgroupV2Error("root-missing", "delegated cgroup root disappeared")
        record_path = self._owner_record_path
        if record_path.exists():
            owner_path, owner_identity = self._validate_owner_record(
                self._read_json(record_path)
            )
            if owner_identity is None:
                if any(self.metadata_dir.glob("run-*.json")):
                    raise CgroupOwnershipError(
                        "owner-missing",
                        "live cgroup manifests lost their owner path",
                    )
                owner_identity = self.system.create_group(self.root, self.owner_name)
                self._write_json(
                    record_path,
                    self._owner_record(
                        root_identity=root_identity,
                        owner_identity=owner_identity,
                    ),
                )
            return owner_path, owner_identity

        owner_path = self.root / self.owner_name
        if self.system.identity(owner_path) is not None:
            raise CgroupOwnershipError(
                "owner-collision",
                "cgroup owner path exists without AGCoord ownership metadata",
            )
        owner_identity = self.system.create_group(self.root, self.owner_name)
        try:
            self._write_json(
                record_path,
                self._owner_record(
                    root_identity=root_identity,
                    owner_identity=owner_identity,
                ),
            )
        except BaseException:
            try:
                self.system.remove_group(owner_path)
            except OSError:
                pass
            raise
        return owner_path, owner_identity

    def _validate_handle(self, raw: object) -> dict[str, object]:
        if not isinstance(raw, Mapping) or set(raw) != _HANDLE_KEYS:
            raise CgroupOwnershipError("handle-invalid", "cgroup handle is invalid")
        selected = dict(raw)
        if (
            type(selected["version"]) is not int
            or selected["version"] != 1
            or selected["owner"] != self.owner_name
            or not isinstance(selected["owner_device"], int)
            or isinstance(selected["owner_device"], bool)
            or selected["owner_device"] < 0
            or not isinstance(selected["owner_inode"], int)
            or isinstance(selected["owner_inode"], bool)
            or selected["owner_inode"] < 0
            or not isinstance(selected["leaf_device"], int)
            or isinstance(selected["leaf_device"], bool)
            or selected["leaf_device"] < 0
            or not isinstance(selected["leaf_inode"], int)
            or isinstance(selected["leaf_inode"], bool)
            or selected["leaf_inode"] < 0
            or not isinstance(selected["leaf"], str)
            or not _LEAF_NAME.fullmatch(selected["leaf"])
            or not isinstance(selected["token"], str)
            or not _TOKEN.fullmatch(selected["token"])
        ):
            raise CgroupOwnershipError("handle-invalid", "cgroup handle fields are invalid")
        return selected

    def _manifest(
        self,
        request: ResourceRequest,
        handle: Mapping[str, object],
    ) -> dict[str, object]:
        return {"version": 1, "run_id": request.run_id, "handle": dict(handle)}

    def _read_manifest(
        self,
        request: ResourceRequest,
    ) -> tuple[Path, dict[str, object]]:
        path = self._manifest_path(request.run_id)
        try:
            raw = self._read_json(path)
        except FileNotFoundError as exc:
            raise CgroupOwnershipError(
                "manifest-missing",
                "cgroup run ownership manifest is missing",
            ) from exc
        if (
            set(raw) != _MANIFEST_KEYS
            or type(raw.get("version")) is not int
            or raw.get("version") != 1
        ):
            raise CgroupOwnershipError("manifest-invalid", "cgroup run manifest is invalid")
        if raw.get("run_id") != request.run_id:
            raise CgroupOwnershipError("manifest-collision", "cgroup run manifest collided")
        return path, self._validate_handle(raw.get("handle"))

    def _resolve(
        self,
        request: ResourceRequest,
        raw_handle: Mapping[str, object],
        *,
        allow_missing: bool,
    ) -> Path | None:
        handle = self._validate_handle(raw_handle)
        _manifest_path, manifest_handle = self._read_manifest(request)
        if manifest_handle != handle:
            raise CgroupOwnershipError("handle-mismatch", "cgroup handle does not own manifest")
        owner_path, owner_identity = self._validate_owner_record(
            self._read_json(self._owner_record_path)
        )
        expected_owner = CgroupIdentity(handle["owner_device"], handle["owner_inode"])
        if owner_identity is None:
            if allow_missing:
                return None
            raise CgroupOwnershipError("owner-missing", "cgroup owner path disappeared")
        if owner_identity != expected_owner:
            raise CgroupOwnershipError("owner-reused", "cgroup owner identity changed")
        leaf_path = owner_path / str(handle["leaf"])
        leaf_identity = self.system.identity(leaf_path)
        if leaf_identity is None:
            if allow_missing:
                return None
            raise CgroupOwnershipError("leaf-missing", "run cgroup leaf disappeared")
        expected_leaf = CgroupIdentity(handle["leaf_device"], handle["leaf_inode"])
        if leaf_identity != expected_leaf:
            raise CgroupOwnershipError("leaf-reused", "run cgroup leaf was reused")
        return leaf_path

    def prepare(self, request: ResourceRequest) -> Mapping[str, object]:
        self._validate_request(request)
        self._require_available()
        self._prepare_metadata()
        owner_path, owner_identity = self._ensure_owner()
        try:
            self._enable_controllers(request, owner_path)
        except BaseException:
            self._cleanup_owner()
            raise
        manifest_path = self._manifest_path(request.run_id)
        if manifest_path.exists():
            _path, handle = self._read_manifest(request)
            leaf_path = self._resolve(request, handle, allow_missing=True)
            if leaf_path is not None:
                if self.system.populated(leaf_path):
                    raise CgroupV2Error(
                        "leaf-populated",
                        "existing run cgroup is still populated",
                    )
                try:
                    self._configure_controller_values(request, leaf_path)
                    self._start_cpu_sample(request, leaf_path)
                    self._start_memory_sample(request, leaf_path)
                except BaseException:
                    self._cpu_samples.pop(request.run_id, None)
                    self._pids_peaks.pop(request.run_id, None)
                    self._memory_peaks.pop(request.run_id, None)
                    self._swap_peaks.pop(request.run_id, None)
                    self.system.remove_group(leaf_path)
                    manifest_path.unlink(missing_ok=True)
                    self._cleanup_owner()
                    raise
                return handle
            manifest_path.unlink()

        for _attempt in range(16):
            token = uuid4().hex
            run_hash = hashlib.sha256(request.run_id.encode()).hexdigest()[:16]
            leaf_name = f"run-{run_hash}-{token[:12]}"
            try:
                leaf_identity = self.system.create_group(owner_path, leaf_name)
            except FileExistsError:
                continue
            handle = {
                "version": 1,
                "owner": self.owner_name,
                "owner_device": owner_identity.device,
                "owner_inode": owner_identity.inode,
                "leaf": leaf_name,
                "leaf_device": leaf_identity.device,
                "leaf_inode": leaf_identity.inode,
                "token": token,
            }
            try:
                self._configure_controller_values(request, owner_path / leaf_name)
                self._start_cpu_sample(request, owner_path / leaf_name)
                self._start_memory_sample(request, owner_path / leaf_name)
                self._write_json(manifest_path, self._manifest(request, handle))
            except BaseException:
                self._cpu_samples.pop(request.run_id, None)
                self._pids_peaks.pop(request.run_id, None)
                self._memory_peaks.pop(request.run_id, None)
                self._swap_peaks.pop(request.run_id, None)
                try:
                    self.system.remove_group(owner_path / leaf_name)
                except OSError:
                    pass
                self._cleanup_owner()
                raise
            return handle
        raise CgroupV2Error("leaf-collision", "could not allocate a unique run cgroup")

    def attach(
        self,
        request: ResourceRequest,
        state: Mapping[str, object],
        worker_pid: int,
    ) -> None:
        self._validate_request(request)
        leaf_path = self._resolve(request, state, allow_missing=False)
        assert leaf_path is not None
        self.system.attach(leaf_path, worker_pid)
        if worker_pid not in self.system.members(leaf_path):
            raise CgroupV2Error(
                "attach-unverified",
                "launcher cgroup membership could not be verified",
            )

    def tmpfs_setup(
        self,
        request: ResourceRequest,
        state: Mapping[str, object],
        target: str | os.PathLike[str],
    ) -> Mapping[str, object] | None:
        """Return one validated private launcher setup for a bounded tmpfs."""

        self._validate_request(request)
        policy = self._tmpfs_policy(request)
        if policy is None:
            return None
        self._resolve(request, state, allow_missing=False)
        selected = Path(target)
        try:
            details = selected.lstat()
            resolved = selected.resolve(strict=True)
        except OSError as exc:
            raise CgroupV2Error(
                "tmpfs-target-invalid",
                "tmpfs target is unavailable",
            ) from exc
        if (
            not selected.is_absolute()
            or resolved != selected
            or stat.S_ISLNK(details.st_mode)
            or not stat.S_ISDIR(details.st_mode)
            or details.st_uid != os.getuid()
            or details.st_mode & 0o077
        ):
            raise CgroupV2Error(
                "tmpfs-target-invalid",
                "tmpfs target is not a private owned directory",
            )
        handle = self._validate_handle(state)
        return {
            "version": 1,
            "target": str(selected),
            "size": policy.size,
            "inodes": policy.inodes,
            "report": str(self._tmpfs_report_path(request.run_id)),
            "token": handle["token"],
        }

    @staticmethod
    def _flat_values(raw: str) -> dict[str, int]:
        selected: dict[str, int] = {}
        try:
            for line in raw.splitlines():
                name, value = line.split()
                if (
                    not _METRIC_KEY.fullmatch(name)
                    or name in selected
                    or not value.isascii()
                    or not value.isdecimal()
                ):
                    raise ValueError
                selected[name] = int(value)
        except ValueError as exc:
            raise CgroupV2Error(
                "controller-metrics-invalid",
                "cgroup controller metrics are invalid",
            ) from exc
        return selected

    @staticmethod
    def _single_value(raw: str, *, code: str) -> int:
        value = raw.strip()
        if not value.isascii() or not value.isdecimal():
            raise CgroupV2Error(code, "cgroup controller metric is invalid")
        return int(value)

    def _optional_peak(
        self,
        leaf_path: Path,
        name: str,
        *,
        current: int,
        code: str,
    ) -> int:
        try:
            return self._single_value(
                self.system.read_file(leaf_path, name),
                code=code,
            )
        except CgroupV2Error as exc:
            if exc.code != "controller-file-missing":
                raise
            return current

    @staticmethod
    def _pressure_totals(raw: str) -> dict[str, int]:
        selected: dict[str, int] = {}
        decimal = re.compile(r"^[0-9]+(?:\.[0-9]+)?$")
        try:
            for line in raw.splitlines():
                fields = line.split()
                if not fields or fields[0] not in {"some", "full"}:
                    raise ValueError
                category = fields[0]
                if category in selected:
                    raise ValueError
                values: dict[str, str] = {}
                for field in fields[1:]:
                    key, separator, value = field.partition("=")
                    if (
                        not separator
                        or key not in {"avg10", "avg60", "avg300", "total"}
                        or key in values
                        or not value.isascii()
                        or not decimal.fullmatch(value)
                    ):
                        raise ValueError
                    values[key] = value
                if set(values) != {"avg10", "avg60", "avg300", "total"}:
                    raise ValueError
                if not values["total"].isdecimal():
                    raise ValueError
                selected[category] = int(values["total"])
            if set(selected) != {"some", "full"}:
                raise ValueError
        except ValueError as exc:
            raise CgroupV2Error(
                "memory-pressure-invalid",
                "cgroup memory pressure metrics are invalid",
            ) from exc
        return selected

    def _measurement(
        self,
        request: ResourceRequest,
        leaf_path: Path,
    ) -> ResourceMeasurement:
        resources = self._controller_resources(request)
        peak: dict[str, int] = {}
        observations: list[ResourceObservation] = []

        if cpu_name := resources.get("cpu"):
            stats = self._flat_values(self.system.read_file(leaf_path, "cpu.stat"))
            if "usage_usec" not in stats:
                raise CgroupV2Error(
                    "cpu-stat-invalid",
                    "cpu.stat does not contain aggregate usage",
                )
            now_ns = self.system.monotonic_ns()
            sample = self._cpu_samples.get(request.run_id)
            measured_peak = 0
            if sample is not None:
                previous_ns, previous_usage, measured_peak = sample
                if now_ns < previous_ns or stats["usage_usec"] < previous_usage:
                    raise CgroupV2Error(
                        "cpu-stat-invalid",
                        "aggregate CPU sampling moved backwards",
                    )
                elapsed_usec = (now_ns - previous_ns) // 1_000
                if elapsed_usec > 0:
                    used_usec = stats["usage_usec"] - previous_usage
                    concurrency = (used_usec + elapsed_usec - 1) // elapsed_usec
                    measured_peak = max(measured_peak, concurrency)
            self._cpu_samples[request.run_id] = (
                now_ns,
                stats["usage_usec"],
                measured_peak,
            )
            peak[cpu_name] = measured_peak
            if stats.get("nr_throttled", 0) > 0 or stats.get("throttled_usec", 0) > 0:
                observations.append(ResourceObservation(cpu_name, "cpu-throttled"))

        if pids_name := resources.get("pids"):
            current = self._single_value(
                self.system.read_file(leaf_path, "pids.current"),
                code="pids-current-invalid",
            )
            try:
                reported_peak = self._single_value(
                    self.system.read_file(leaf_path, "pids.peak"),
                    code="pids-peak-invalid",
                )
            except CgroupV2Error as exc:
                if exc.code != "controller-file-missing":
                    raise
                reported_peak = current
            measured_peak = max(
                current,
                reported_peak,
                self._pids_peaks.get(request.run_id, 0),
            )
            self._pids_peaks[request.run_id] = measured_peak
            peak[pids_name] = measured_peak
            events = self._flat_values(self.system.read_file(leaf_path, "pids.events"))
            if events.get("max", 0) > 0:
                observations.append(ResourceObservation(pids_name, "pids-limit-hit"))

        hard_name = resources.get("memory.max")
        high_name = resources.get("memory.high")
        if hard_name or high_name:
            current = self._single_value(
                self.system.read_file(leaf_path, "memory.current"),
                code="memory-current-invalid",
            )
            reported_peak = self._optional_peak(
                leaf_path,
                "memory.peak",
                current=current,
                code="memory-peak-invalid",
            )
            measured_peak = max(
                current,
                reported_peak,
                self._memory_peaks.get(request.run_id, 0),
            )
            self._memory_peaks[request.run_id] = measured_peak
            for name in (hard_name, high_name):
                if name is not None:
                    peak[name] = measured_peak
            events = self._flat_values(
                self.system.read_file(leaf_path, "memory.events")
            )
            if hard_name is not None:
                if events.get("max", 0) > 0:
                    observations.append(
                        ResourceObservation(hard_name, "memory-max-hit")
                    )
                if (
                    events.get("oom_kill", 0) > 0
                    or events.get("oom_group_kill", 0) > 0
                ):
                    observations.append(ResourceObservation(hard_name, "memory-oom"))
            if high_name is not None:
                if events.get("high", 0) > 0:
                    observations.append(
                        ResourceObservation(high_name, "memory-high-throttled")
                    )
                pressure = self._pressure_totals(
                    self.system.read_file(leaf_path, "memory.pressure")
                )
                if any(value > 0 for value in pressure.values()):
                    observations.append(
                        ResourceObservation(high_name, "memory-pressure")
                    )

        if swap_name := resources.get("memory.swap.max"):
            current = self._single_value(
                self.system.read_file(leaf_path, "memory.swap.current"),
                code="swap-current-invalid",
            )
            reported_peak = self._optional_peak(
                leaf_path,
                "memory.swap.peak",
                current=current,
                code="swap-peak-invalid",
            )
            measured_peak = max(
                current,
                reported_peak,
                self._swap_peaks.get(request.run_id, 0),
            )
            self._swap_peaks[request.run_id] = measured_peak
            peak[swap_name] = measured_peak
            events = self._flat_values(
                self.system.read_file(leaf_path, "memory.swap.events")
            )
            if events.get("max", 0) > 0 or events.get("fail", 0) > 0:
                observations.append(ResourceObservation(swap_name, "swap-limit-hit"))

        policy = self._tmpfs_policy(request, resources=resources)
        if policy is not None:
            try:
                report = self._read_json(self._tmpfs_report_path(request.run_id))
            except FileNotFoundError:
                report = None
            if report is not None:
                _manifest_path, handle = self._read_manifest(request)
                numeric = (
                    "peak_bytes",
                    "peak_inodes",
                    "terminal_bytes",
                    "terminal_inodes",
                )
                if (
                    set(report) != _TMPFS_REPORT_KEYS
                    or type(report.get("version")) is not int
                    or report.get("version") != 1
                    or report.get("token") != handle["token"]
                    or any(
                        not isinstance(report.get(name), int)
                        or isinstance(report.get(name), bool)
                        or int(report[name]) < 0
                        for name in numeric
                    )
                    or not isinstance(report.get("byte_limit_hit"), bool)
                    or not isinstance(report.get("inode_limit_hit"), bool)
                    or int(report["terminal_bytes"]) > int(report["peak_bytes"])
                    or int(report["terminal_inodes"]) > int(report["peak_inodes"])
                    or int(report["peak_bytes"]) > policy.size
                    or int(report["peak_inodes"]) > policy.inodes
                ):
                    raise CgroupV2Error(
                        "tmpfs-report-invalid",
                        "tmpfs usage report is invalid",
                    )
                peak[policy.size_name] = int(report["peak_bytes"])
                peak[policy.inode_name] = int(report["peak_inodes"])
                if report["byte_limit_hit"]:
                    observations.append(
                        ResourceObservation(
                            policy.size_name,
                            "tmpfs-byte-limit-hit",
                        )
                    )
                if report["inode_limit_hit"]:
                    observations.append(
                        ResourceObservation(
                            policy.inode_name,
                            "tmpfs-inode-limit-hit",
                        )
                    )

        return ResourceMeasurement(peak, tuple(observations))

    def usage(
        self,
        request: ResourceRequest,
        state: Mapping[str, object],
    ) -> Mapping[str, int] | ResourceMeasurement:
        self._validate_request(request)
        leaf_path = self._resolve(request, state, allow_missing=False)
        assert leaf_path is not None
        if not self._controller_resources(request):
            return {}
        return self._measurement(request, leaf_path)

    def _kill_and_wait(self, leaf_path: Path) -> None:
        if self.system.populated(leaf_path):
            self.system.kill(leaf_path)
        deadline = time.monotonic() + self.empty_timeout
        while self.system.populated(leaf_path):
            if time.monotonic() >= deadline:
                raise CgroupV2Error(
                    "leaf-populated",
                    "run cgroup did not become unpopulated after kill",
                )
            time.sleep(0.02)

    def finish(
        self,
        request: ResourceRequest,
        state: Mapping[str, object],
    ) -> Mapping[str, int] | ResourceMeasurement:
        self._validate_request(request)
        leaf_path = self._resolve(request, state, allow_missing=True)
        if leaf_path is not None:
            self._kill_and_wait(leaf_path)
            if self._controller_resources(request):
                return self._measurement(request, leaf_path)
        return {}

    def cancel(
        self,
        request: ResourceRequest,
        state: Mapping[str, object],
    ) -> None:
        self._validate_request(request)
        leaf_path = self._resolve(request, state, allow_missing=True)
        if leaf_path is not None:
            self._kill_and_wait(leaf_path)

    def _cleanup_owner(self) -> None:
        if any(self.metadata_dir.glob("run-*.json")):
            return
        try:
            raw = self._read_json(self._owner_record_path)
        except FileNotFoundError:
            return
        owner_path, owner_identity = self._validate_owner_record(raw)
        if owner_identity is None:
            self._owner_record_path.unlink(missing_ok=True)
            return
        if self.system.populated(owner_path):
            return
        try:
            self.system.remove_group(owner_path)
        except OSError as exc:
            if exc.errno in {errno.EBUSY, errno.ENOTEMPTY}:
                return
            raise
        self._owner_record_path.unlink(missing_ok=True)

    def cleanup(
        self,
        request: ResourceRequest,
        state: Mapping[str, object],
    ) -> None:
        self._validate_request(request)
        manifest_path, manifest_handle = self._read_manifest(request)
        handle = self._validate_handle(state)
        if manifest_handle != handle:
            raise CgroupOwnershipError("handle-mismatch", "cgroup cleanup handle mismatched")
        leaf_path = self._resolve(request, handle, allow_missing=True)
        if leaf_path is not None:
            if self.system.populated(leaf_path):
                raise CgroupV2Error("leaf-populated", "cannot remove populated run cgroup")
            self.system.remove_group(leaf_path)
        self._cpu_samples.pop(request.run_id, None)
        self._pids_peaks.pop(request.run_id, None)
        self._memory_peaks.pop(request.run_id, None)
        self._swap_peaks.pop(request.run_id, None)
        self._tmpfs_report_path(request.run_id).unlink(missing_ok=True)
        manifest_path.unlink(missing_ok=True)
        self._cleanup_owner()
