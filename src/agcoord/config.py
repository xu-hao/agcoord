"""The one JSON file that configures a state directory's broker.

Capacity, resource bindings, host-backed enforcement, and database lock waiting are operator
contracts for one state directory, so they live beside that directory's spool rather than in
ambient process environment.  This module owns only file location, JSON shape, and section
types; capacity semantics stay in :mod:`agcoord.queue`, binding semantics in
:mod:`agcoord.resources`.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import json
import math
import os
from typing import Any, Mapping


CONFIG_FILENAME = "config.json"
MAX_DATABASE_TIMEOUT = 2_147_483.647
DEFAULT_NATIVE_BROKER_PATH = "/usr/libexec/agcoord/agcoord-broker"
_CONFIG_KEYS = frozenset(
    {
        "capacities",
        "bindings",
        "cgroup_root",
        "cgroup_io",
        "database_timeout",
        "native_broker",
    }
)


class BrokerConfigError(RuntimeError):
    """A missing, unreadable, or malformed broker configuration file."""


@dataclass(frozen=True)
class NativeBrokerConfig:
    """The one explicitly selected native executable and its trust policy."""

    path: str
    allow_development: bool
    managed_service: bool = False


@dataclass(frozen=True)
class BrokerConfig:
    """One state directory's normalized configuration sections."""

    capacities: Mapping[str, Any] | None
    bindings: Mapping[str, Any] | None
    cgroup_root: str | None
    cgroup_io: Mapping[str, Any] | None
    database_timeout: float | None
    native_broker: NativeBrokerConfig


def config_path(state_dir: str | os.PathLike[str]) -> Path:
    """Return the single configuration file that belongs to one state directory."""
    return Path(state_dir) / CONFIG_FILENAME


def load_broker_config(state_dir: str | os.PathLike[str]) -> BrokerConfig:
    """Read one state directory's configuration, or the empty default when absent."""
    path = config_path(state_dir)
    try:
        raw_text = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return BrokerConfig(
            capacities=None,
            bindings=None,
            cgroup_root=None,
            cgroup_io=None,
            database_timeout=None,
            native_broker=NativeBrokerConfig(
                path=DEFAULT_NATIVE_BROKER_PATH,
                allow_development=False,
                managed_service=False,
            ),
        )
    except OSError as exc:
        raise BrokerConfigError(f"cannot read broker configuration {path}: {exc}") from exc
    return parse_broker_config(raw_text, source=path)


def parse_broker_config(
    raw_text: str,
    *,
    source: str | os.PathLike[str] = CONFIG_FILENAME,
) -> BrokerConfig:
    """Normalize one configuration document and its strict host-path shape."""
    try:
        document = json.loads(raw_text)
    except json.JSONDecodeError as exc:
        raise BrokerConfigError(f"broker configuration {source} is not valid JSON: {exc}") from exc
    if not isinstance(document, dict):
        raise BrokerConfigError(f"broker configuration {source} must be one JSON object")
    unknown = sorted(set(document) - _CONFIG_KEYS)
    if unknown:
        raise BrokerConfigError(
            f"broker configuration {source} has unknown keys: {', '.join(unknown)}"
        )
    capacities = _section(document, "capacities", source=source)
    bindings = _section(document, "bindings", source=source)
    cgroup_io = _cgroup_io_section(document, source=source)
    database_timeout = _database_timeout(document, source=source)
    native_broker = _native_broker_section(document, source=source)
    cgroup_root = document.get("cgroup_root")
    if cgroup_root is not None and (
        not isinstance(cgroup_root, str) or not cgroup_root.strip()
    ):
        raise BrokerConfigError(
            f"broker configuration {source} cgroup_root must be a non-empty string"
        )
    return BrokerConfig(
        capacities=capacities,
        bindings=bindings,
        cgroup_root=cgroup_root,
        cgroup_io=cgroup_io,
        database_timeout=database_timeout,
        native_broker=native_broker,
    )


def _native_broker_section(
    document: Mapping[str, Any],
    *,
    source: str | os.PathLike[str],
) -> NativeBrokerConfig:
    value = document.get("native_broker")
    if value is None:
        return NativeBrokerConfig(
            path=DEFAULT_NATIVE_BROKER_PATH,
            allow_development=False,
            managed_service=False,
        )
    if not isinstance(value, dict):
        raise BrokerConfigError(
            f"broker configuration {source} section 'native_broker' must be a JSON object"
        )
    unknown = sorted(set(value) - {"path", "allow_development", "managed_service"})
    if unknown:
        raise BrokerConfigError(
            f"broker configuration {source} native_broker has unknown keys: "
            + ", ".join(unknown)
        )
    path = value.get("path")
    if (
        not isinstance(path, str)
        or not path
        or "\0" in path
        or not Path(path).is_absolute()
    ):
        raise BrokerConfigError(
            f"broker configuration {source} native_broker path must be an absolute string"
        )
    allow_development = value.get("allow_development", False)
    if not isinstance(allow_development, bool):
        raise BrokerConfigError(
            f"broker configuration {source} native_broker allow_development must be boolean"
        )
    managed_service = value.get("managed_service", False)
    if not isinstance(managed_service, bool):
        raise BrokerConfigError(
            f"broker configuration {source} native_broker managed_service must be boolean"
        )
    return NativeBrokerConfig(
        path=path,
        allow_development=allow_development,
        managed_service=managed_service,
    )


def _section(
    document: Mapping[str, Any],
    key: str,
    *,
    source: str | os.PathLike[str],
) -> Mapping[str, Any] | None:
    value = document.get(key)
    if value is None:
        return None
    if not isinstance(value, dict):
        raise BrokerConfigError(
            f"broker configuration {source} section {key!r} must be a JSON object"
        )
    return value


def _cgroup_io_section(
    document: Mapping[str, Any],
    *,
    source: str | os.PathLike[str],
) -> Mapping[str, Any] | None:
    value = document.get("cgroup_io")
    if value is None:
        return None
    if not isinstance(value, dict) or set(value) != {"paths"}:
        raise BrokerConfigError(
            f"broker configuration {source} cgroup_io must contain exactly paths"
        )
    paths = value["paths"]
    if not isinstance(paths, list) or not paths:
        raise BrokerConfigError(
            f"broker configuration {source} cgroup_io paths must be a non-empty list"
        )
    selected: list[str] = []
    identities: set[Path] = set()
    for raw_path in paths:
        if (
            not isinstance(raw_path, str)
            or not raw_path
            or "\0" in raw_path
            or not Path(raw_path).is_absolute()
        ):
            raise BrokerConfigError(
                f"broker configuration {source} cgroup_io paths must be absolute strings"
            )
        identity = Path(raw_path)
        if identity in identities:
            raise BrokerConfigError(
                f"broker configuration {source} cgroup_io paths must be unique"
            )
        identities.add(identity)
        selected.append(raw_path)
    return {"paths": selected}


def _database_timeout(
    document: Mapping[str, Any],
    *,
    source: str | os.PathLike[str],
) -> float | None:
    value = document.get("database_timeout")
    if value is None:
        return None
    selected: float | None = None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        try:
            selected = float(value)
        except OverflowError:
            pass
    if (
        selected is None
        or not math.isfinite(selected)
        or selected <= 0
        or selected > MAX_DATABASE_TIMEOUT
    ):
        raise BrokerConfigError(
            f"broker configuration {source} database_timeout must be "
            f"a positive finite number of seconds no greater than "
            f"{MAX_DATABASE_TIMEOUT}"
        )
    return selected
