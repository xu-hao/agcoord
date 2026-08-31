"""The one JSON file that configures a state directory's broker.

Capacity, resource bindings, and the delegated cgroup root are operator contracts for
one state directory, so they live beside that directory's spool rather than in ambient
process environment.  This module owns only file location, JSON shape, and section
types; capacity semantics stay in :mod:`agcoord.queue`, binding semantics in
:mod:`agcoord.resources`, and delegation semantics in :mod:`agcoord.cgroup`.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import json
import os
from typing import Any, Mapping


CONFIG_FILENAME = "config.json"
_CONFIG_KEYS = frozenset({"capacities", "bindings", "cgroup_root"})


class BrokerConfigError(RuntimeError):
    """A missing, unreadable, or malformed broker configuration file."""


@dataclass(frozen=True)
class BrokerConfig:
    """One state directory's unvalidated configuration sections."""

    capacities: Mapping[str, Any] | None
    bindings: Mapping[str, Any] | None
    cgroup_root: str | None


def config_path(state_dir: str | os.PathLike[str]) -> Path:
    """Return the single configuration file that belongs to one state directory."""
    return Path(state_dir) / CONFIG_FILENAME


def load_broker_config(state_dir: str | os.PathLike[str]) -> BrokerConfig:
    """Read one state directory's configuration, or the empty default when absent."""
    path = config_path(state_dir)
    try:
        raw_text = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return BrokerConfig(capacities=None, bindings=None, cgroup_root=None)
    except OSError as exc:
        raise BrokerConfigError(f"cannot read broker configuration {path}: {exc}") from exc
    return parse_broker_config(raw_text, source=path)


def parse_broker_config(
    raw_text: str,
    *,
    source: str | os.PathLike[str] = CONFIG_FILENAME,
) -> BrokerConfig:
    """Normalize one configuration document without interpreting its sections."""
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
