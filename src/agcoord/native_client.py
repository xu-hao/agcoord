"""Strict process boundary for the selected protocol-5 native broker.

The Python package remains the user-facing CLI and TUI.  It treats the Rust executable as a
versioned local protocol peer: selection is explicit, identity is checked before execution,
and every command exchanges one bounded JSON value over standard streams.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
import platform
import re
import stat
import subprocess
from typing import Any, Mapping, Sequence

from .config import NativeBrokerConfig


NATIVE_PROTOCOL = 5
NATIVE_IMPLEMENTATION = "rust-native"
MAX_NATIVE_JSON_BYTES = 1024 * 1024
_RELEASE_BUILD = re.compile(r"^sha256:[0-9a-f]{64}$")
_SUPPORTED_NATIVE_VERSION = re.compile(r"^0\.3\.\d+(?:[-+][0-9A-Za-z.-]+)?$")
_SUPPORTED_RELEASE_TARGET = "x86_64-unknown-linux-musl"
_SUPPORTED_DEVELOPMENT_TARGETS = frozenset(
    {_SUPPORTED_RELEASE_TARGET, "x86_64-unknown-linux-gnu"}
)


class NativeClientError(RuntimeError):
    """A stable native refusal translated for the Python command line."""

    def __init__(self, message: str, *, code: str | None = None):
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class NativeBrokerIdentity:
    """The immutable identity reported by one selected executable."""

    name: str
    version: str
    protocol: int
    implementation: str
    build: str
    target: str
    sqlite: str


@dataclass(frozen=True)
class NativeBrokerCommand:
    """One validated executable and the identity a live owner must retain."""

    path: Path
    identity: NativeBrokerIdentity

    @classmethod
    def select(cls, configured: NativeBrokerConfig) -> "NativeBrokerCommand":
        path = Path(configured.path)
        _validate_host_platform()
        if not path.is_absolute():
            raise NativeClientError("native broker executable path must be absolute")
        try:
            details = path.lstat()
        except FileNotFoundError as exc:
            raise NativeClientError(
                f"native broker executable does not exist: {path}; install the host "
                "package or configure native_broker.path"
            ) from exc
        except OSError as exc:
            raise NativeClientError(
                f"cannot inspect native broker executable {path}: {exc}"
            ) from exc
        if stat.S_ISLNK(details.st_mode) or not stat.S_ISREG(details.st_mode):
            raise NativeClientError(
                f"native broker executable must be a regular file, not a symlink: {path}"
            )
        if details.st_mode & 0o022:
            raise NativeClientError(
                f"native broker executable must not be group- or world-writable: {path}"
            )
        allowed_owners = {0, os.geteuid()} if configured.allow_development else {0}
        if details.st_uid not in allowed_owners:
            owner = "root" if not configured.allow_development else "root or the current user"
            raise NativeClientError(
                f"native broker executable must be owned by {owner}: {path}"
            )
        if details.st_mode & 0o111 == 0 or not os.access(path, os.X_OK):
            raise NativeClientError(f"native broker executable is not executable: {path}")
        identity = _read_identity(path)
        if identity.build == "development":
            if not configured.allow_development:
                raise NativeClientError(
                    "native broker reports a development build; set "
                    "native_broker.allow_development only for an explicitly trusted "
                    "development executable"
                )
            if identity.target not in _SUPPORTED_DEVELOPMENT_TARGETS:
                raise NativeClientError(
                    f"native broker target is unsupported: {identity.target}"
                )
        else:
            if not _RELEASE_BUILD.fullmatch(identity.build):
                raise NativeClientError("native broker reports an invalid release build ID")
            if identity.target != _SUPPORTED_RELEASE_TARGET:
                raise NativeClientError(
                    f"native broker release target is unsupported: {identity.target}"
                )
        return cls(path=path, identity=identity)

    def invoke(
        self,
        command: str,
        *,
        state_dir: str | os.PathLike[str],
        arguments: Sequence[str] = (),
    ) -> Any:
        argv = [
            str(self.path),
            command,
            "--state-dir",
            str(state_dir),
            *arguments,
        ]
        try:
            completed = subprocess.run(
                argv,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
                timeout=30,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise NativeClientError(
                f"cannot run native broker command {command!r}: {exc}"
            ) from exc
        if len(completed.stdout) > MAX_NATIVE_JSON_BYTES or len(completed.stderr) > MAX_NATIVE_JSON_BYTES:
            raise NativeClientError(
                f"native broker command {command!r} returned oversized output"
            )
        if completed.returncode != 0:
            refusal = _decode_refusal(completed.stderr)
            if refusal is not None:
                raise NativeClientError(refusal[1], code=refusal[0])
            raise NativeClientError(
                f"native broker command {command!r} failed with exit status "
                f"{completed.returncode} and no valid refusal"
            )
        if completed.stderr.strip():
            raise NativeClientError(
                f"native broker command {command!r} wrote unexpected standard error"
            )
        return _decode_json(completed.stdout, subject=f"native broker {command} result")

    def serve_arguments(
        self,
        state_dir: str | os.PathLike[str],
        capacities: Mapping[str, int],
    ) -> list[str]:
        command = [str(self.path), "serve", "--state-dir", str(state_dir)]
        for name, units in sorted(capacities.items()):
            command.extend(("--capacity", f"{name}={units}"))
        return command


def _validate_host_platform() -> None:
    if platform.system() != "Linux" or platform.machine() not in {"x86_64", "AMD64"}:
        raise NativeClientError(
            "the native broker currently supports only Linux x86_64 hosts"
        )


def _read_identity(path: Path) -> NativeBrokerIdentity:
    try:
        completed = subprocess.run(
            [str(path), "identity", "--json"],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise NativeClientError(f"cannot identify native broker executable {path}: {exc}") from exc
    if completed.returncode != 0:
        raise NativeClientError(
            f"native broker identity command failed with exit status {completed.returncode}"
        )
    if completed.stderr.strip():
        raise NativeClientError("native broker identity wrote unexpected standard error")
    value = _decode_json(completed.stdout, subject="native broker identity")
    expected = {
        "name",
        "version",
        "protocol",
        "implementation",
        "build",
        "target",
        "sqlite",
    }
    if not isinstance(value, dict) or set(value) != expected:
        raise NativeClientError("native broker identity has an incompatible JSON shape")
    if (
        value["name"] != "agcoord-broker"
        or value["protocol"] != NATIVE_PROTOCOL
        or value["implementation"] != NATIVE_IMPLEMENTATION
        or any(
            not isinstance(value[key], str) or not value[key]
            for key in ("version", "build", "target", "sqlite")
        )
    ):
        raise NativeClientError("native broker identity is incompatible with this client")
    if not _SUPPORTED_NATIVE_VERSION.fullmatch(value["version"]):
        raise NativeClientError(
            f"native broker version is unsupported by this client: {value['version']}"
        )
    return NativeBrokerIdentity(**value)


def _decode_json(raw: bytes, *, subject: str) -> Any:
    if len(raw) > MAX_NATIVE_JSON_BYTES:
        raise NativeClientError(f"{subject} is oversized")
    try:
        text = raw.decode("utf-8", errors="strict")
        value = json.loads(text)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise NativeClientError(f"{subject} is not valid UTF-8 JSON") from exc
    return value


def _decode_refusal(raw: bytes) -> tuple[str, str] | None:
    try:
        value = _decode_json(raw, subject="native broker refusal")
    except NativeClientError:
        return None
    if (
        isinstance(value, dict)
        and set(value) == {"code", "message"}
        and isinstance(value["code"], str)
        and value["code"]
        and isinstance(value["message"], str)
        and value["message"]
    ):
        return value["code"], value["message"]
    return None
