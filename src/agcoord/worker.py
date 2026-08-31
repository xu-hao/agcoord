"""Blocked worker launcher and private tmpfs supervisor."""

from __future__ import annotations

import ctypes
import json
import os
from pathlib import Path
import re
import signal
import sys
import time
from typing import Mapping, Sequence
from uuid import uuid4

from .cgroup import (
    CGROUP_ISOLATE_ENV,
    CgroupIsolationError,
    isolate_current_cgroup,
    mount_current_tmpfs,
    unmount_current_tmpfs,
)
from .resources import ResourceBackendError


TMPFS_SETUP_ENV = "_AGCOORD_TMPFS_SETUP"
PROJECT_QUOTA_DROP_ENV = "_AGCOORD_PROJECT_QUOTA_DROP_ADMIN"
_TMPFS_SPEC_KEYS = frozenset(
    {"version", "target", "size", "inodes", "report", "token"}
)
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
_TOKEN = re.compile(r"^[0-9a-f]{32}$")
_CODE = re.compile(r"^[a-z][a-z0-9-]{0,63}$")
_LINUX_CAPABILITY_VERSION_3 = 0x20080522
_PR_SET_NO_NEW_PRIVS = 38
_PR_CAP_AMBIENT = 47
_PR_CAP_AMBIENT_CLEAR_ALL = 4


class _CapabilityHeader(ctypes.Structure):
    _fields_ = [("version", ctypes.c_uint32), ("pid", ctypes.c_int)]


class _CapabilityData(ctypes.Structure):
    _fields_ = [
        ("effective", ctypes.c_uint32),
        ("permitted", ctypes.c_uint32),
        ("inheritable", ctypes.c_uint32),
    ]


def _drop_initial_admin_capabilities() -> None:
    """Prevent a privileged quota broker from lending its powers to user code."""

    libc = ctypes.CDLL(None, use_errno=True)
    prctl = libc.prctl
    prctl.argtypes = [
        ctypes.c_int,
        ctypes.c_ulong,
        ctypes.c_ulong,
        ctypes.c_ulong,
        ctypes.c_ulong,
    ]
    prctl.restype = ctypes.c_int
    capset = libc.capset
    capset.argtypes = [
        ctypes.POINTER(_CapabilityHeader),
        ctypes.POINTER(_CapabilityData),
    ]
    capset.restype = ctypes.c_int
    header = _CapabilityHeader(_LINUX_CAPABILITY_VERSION_3, 0)
    data = (_CapabilityData * 2)()
    operations = (
        prctl(
            _PR_CAP_AMBIENT,
            _PR_CAP_AMBIENT_CLEAR_ALL,
            0,
            0,
            0,
        ),
        capset(ctypes.byref(header), data),
        prctl(_PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0),
    )
    if any(result != 0 for result in operations):
        error = ctypes.get_errno()
        raise CgroupIsolationError(
            "worker-privilege-drop-failed",
            f"worker privileges could not be dropped: {os.strerror(error)}",
        )
    try:
        status = Path("/proc/self/status").read_text(encoding="ascii")
    except (OSError, UnicodeError) as exc:
        raise CgroupIsolationError(
            "worker-privilege-drop-unverified",
            "worker privilege state could not be verified",
        ) from exc
    expected_zero = ("CapEff", "CapPrm", "CapInh", "CapAmb")
    values = {
        name: value
        for name, value in re.findall(
            r"^(CapEff|CapPrm|CapInh|CapAmb|NoNewPrivs):\s*([0-9a-fA-F]+)$",
            status,
            re.MULTILINE,
        )
    }
    if (
        any(int(values.get(name, "1"), 16) != 0 for name in expected_zero)
        or values.get("NoNewPrivs") != "1"
    ):
        raise CgroupIsolationError(
            "worker-privilege-drop-unverified",
            "worker retained administrative privilege",
        )


def _tmpfs_spec(raw: str) -> dict[str, object]:
    try:
        selected = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise CgroupIsolationError(
            "tmpfs-setup-invalid",
            "tmpfs launcher setup is invalid",
        ) from exc
    if not isinstance(selected, dict) or set(selected) != _TMPFS_SPEC_KEYS:
        raise CgroupIsolationError(
            "tmpfs-setup-invalid",
            "tmpfs launcher setup has an invalid shape",
        )
    if (
        type(selected["version"]) is not int
        or selected["version"] != 1
        or not isinstance(selected["target"], str)
        or not Path(selected["target"]).is_absolute()
        or not isinstance(selected["report"], str)
        or not Path(selected["report"]).is_absolute()
        or not isinstance(selected["token"], str)
        or not _TOKEN.fullmatch(selected["token"])
        or not isinstance(selected["size"], int)
        or isinstance(selected["size"], bool)
        or selected["size"] <= 0
        or not isinstance(selected["inodes"], int)
        or isinstance(selected["inodes"], bool)
        or selected["inodes"] <= 0
    ):
        raise CgroupIsolationError(
            "tmpfs-setup-invalid",
            "tmpfs launcher setup fields are invalid",
        )
    return selected


def _tmpfs_sample(
    target: Path,
    *,
    baseline_inodes: int,
) -> tuple[int, int, bool, bool]:
    usage = os.statvfs(target)
    used_bytes = (usage.f_blocks - usage.f_bfree) * usage.f_frsize
    used_inodes = max(0, usage.f_files - usage.f_ffree - baseline_inodes)
    return used_bytes, used_inodes, usage.f_bfree == 0, usage.f_ffree == 0


def _write_tmpfs_report(
    spec: Mapping[str, object],
    *,
    peak_bytes: int,
    peak_inodes: int,
    terminal_bytes: int,
    terminal_inodes: int,
    byte_limit_hit: bool,
    inode_limit_hit: bool,
) -> None:
    report = Path(str(spec["report"]))
    payload = {
        "version": 1,
        "token": spec["token"],
        "peak_bytes": peak_bytes,
        "peak_inodes": peak_inodes,
        "terminal_bytes": terminal_bytes,
        "terminal_inodes": terminal_inodes,
        "byte_limit_hit": byte_limit_hit,
        "inode_limit_hit": inode_limit_hit,
    }
    if set(payload) != _TMPFS_REPORT_KEYS:  # pragma: no cover - local invariant
        raise AssertionError("tmpfs report shape changed")
    temporary = report.with_name(f".{report.name}.{uuid4().hex}.tmp")
    descriptor = -1
    try:
        descriptor = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC,
            0o600,
        )
        encoded = json.dumps(payload, separators=(",", ":")).encode("ascii")
        view = memoryview(encoded)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:  # pragma: no cover - write(2) contract
                raise OSError("tmpfs report write made no progress")
            view = view[written:]
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = -1
        os.replace(temporary, report)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)


def _exec(command: Sequence[str], environment: Mapping[str, str]) -> None:
    try:
        os.execvpe(command[0], list(command), dict(environment))
    except OSError as exc:
        print(f"AGCoord: could not exec worker: {exc}", file=sys.stderr, flush=True)
        raise SystemExit(127) from exc


def _run_tmpfs_command(
    command: Sequence[str],
    environment: Mapping[str, str],
    spec: Mapping[str, object],
    *,
    baseline_inodes: int,
) -> None:
    target = Path(str(spec["target"]))
    peak_bytes = 0
    peak_inodes = 0
    byte_limit_hit = False
    inode_limit_hit = False
    child = os.fork()
    if child == 0:  # pragma: no branch - child replaces itself
        _exec(command, environment)
        os._exit(127)  # pragma: no cover - exec helper exits

    status: int | None = None
    report_failed = False
    terminal_bytes = 0
    terminal_inodes = 0
    while status is None:
        waited, observed = os.waitpid(child, os.WNOHANG)
        if waited == child:
            status = observed
        try:
            (
                terminal_bytes,
                terminal_inodes,
                byte_full,
                inode_full,
            ) = _tmpfs_sample(target, baseline_inodes=baseline_inodes)
            peak_bytes = max(peak_bytes, terminal_bytes)
            peak_inodes = max(peak_inodes, terminal_inodes)
            byte_limit_hit = byte_limit_hit or byte_full
            inode_limit_hit = inode_limit_hit or inode_full
            _write_tmpfs_report(
                spec,
                peak_bytes=peak_bytes,
                peak_inodes=peak_inodes,
                terminal_bytes=terminal_bytes,
                terminal_inodes=terminal_inodes,
                byte_limit_hit=byte_limit_hit,
                inode_limit_hit=inode_limit_hit,
            )
        except OSError:
            if not report_failed:
                print(
                    "AGCoord: tmpfs usage report could not be updated",
                    file=sys.stderr,
                    flush=True,
                )
                report_failed = True
        if status is None:
            time.sleep(0.05)

    assert status is not None
    if os.WIFEXITED(status):
        raise SystemExit(os.WEXITSTATUS(status))
    if os.WIFSIGNALED(status):
        selected_signal = os.WTERMSIG(status)
        signal.signal(selected_signal, signal.SIG_DFL)
        os.kill(os.getpid(), selected_signal)
        raise SystemExit(128 + selected_signal)  # pragma: no cover - signal wins
    raise SystemExit(125)  # pragma: no cover - waitpid only returns exit/signal here


def _read_release(descriptor: int) -> bool:
    try:
        return os.read(descriptor, 1) == b"1"
    finally:
        pass


def _write_setup_result(descriptor: int, code: str) -> None:
    if not _CODE.fullmatch(code):
        code = "tmpfs-setup-failed"
    os.write(descriptor, code.encode("ascii"))


def launcher_main() -> None:
    """Wait for cgroup attachment, provision optional tmpfs, then run the command."""

    try:
        release_fd = int(sys.argv[1])
        setup_fd = int(sys.argv[2])
    except (IndexError, ValueError) as exc:
        raise SystemExit(125) from exc
    command = sys.argv[3:]
    if not command:
        raise SystemExit(125)
    isolate_cgroup = os.environ.pop(CGROUP_ISOLATE_ENV, None) == "1"
    drop_admin = os.environ.pop(PROJECT_QUOTA_DROP_ENV, None)
    if drop_admin not in {None, "1"}:
        raise SystemExit(125)
    raw_spec = os.environ.pop(TMPFS_SETUP_ENV, None)
    if not _read_release(release_fd):
        os.close(release_fd)
        if setup_fd >= 0:
            os.close(setup_fd)
        raise SystemExit(125)

    spec: dict[str, object] | None = None
    mounted = False
    baseline_inodes = 0
    code = "ok"
    try:
        if isolate_cgroup:
            isolate_current_cgroup()
        if drop_admin == "1":
            _drop_initial_admin_capabilities()
        if raw_spec is not None:
            if not isolate_cgroup:
                raise CgroupIsolationError(
                    "tmpfs-namespace-required",
                    "tmpfs needs the private cgroup mount namespace",
                )
            spec = _tmpfs_spec(raw_spec)
            initial = mount_current_tmpfs(
                str(spec["target"]),
                size=int(spec["size"]),
                inodes=int(spec["inodes"]),
            )
            mounted = True
            baseline_inodes = initial.f_files - initial.f_ffree
            _write_tmpfs_report(
                spec,
                peak_bytes=0,
                peak_inodes=0,
                terminal_bytes=0,
                terminal_inodes=0,
                byte_limit_hit=False,
                inode_limit_hit=False,
            )
    except ResourceBackendError as exc:
        code = exc.code
    except Exception:
        code = "tmpfs-setup-failed"

    setup_required = raw_spec is not None or drop_admin == "1"
    if setup_required:
        try:
            _write_setup_result(setup_fd, code)
        except OSError:
            if mounted and spec is not None:
                unmount_current_tmpfs(str(spec["target"]))
            raise SystemExit(125)
        finally:
            os.close(setup_fd)
        continue_run = _read_release(release_fd)
        os.close(release_fd)
        if code != "ok":
            print(f"AGCoord: worker setup unavailable: {code}", file=sys.stderr, flush=True)
            if continue_run and raw_spec is not None and drop_admin is None:
                _exec(command, os.environ)
            raise SystemExit(125)
        if not continue_run:
            if mounted and spec is not None:
                unmount_current_tmpfs(str(spec["target"]))
            raise SystemExit(125)
        if spec is not None:
            _run_tmpfs_command(
                command,
                os.environ,
                spec,
                baseline_inodes=baseline_inodes,
            )
            raise AssertionError("tmpfs supervisor returned")  # pragma: no cover
        _exec(command, os.environ)

    if code != "ok":
        print(f"AGCoord: worker setup unavailable: {code}", file=sys.stderr, flush=True)
        os.close(release_fd)
        if setup_fd >= 0:
            os.close(setup_fd)
        raise SystemExit(125)
    os.close(release_fd)
    if setup_fd >= 0:
        os.close(setup_fd)
    _exec(command, os.environ)
