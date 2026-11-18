"""The broker answers for its own spool; a client never reads the spool itself."""

from __future__ import annotations

import json
from pathlib import Path
import sqlite3

import pytest

from agcoord.config import config_path
from agcoord.queue import CoordinatorClient, CoordinatorError

from conftest import native_broker_executable


def _pre_native_spool(state_dir: Path, protocol: int = 4) -> None:
    """One spool from before the native broker owned this generation, with no owner."""
    state_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    database = state_dir / "queue.sqlite3"
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE coordinator_meta (key TEXT PRIMARY KEY, value TEXT)")
        connection.execute(
            "INSERT INTO coordinator_meta (key, value) VALUES ('protocol', ?)",
            (str(protocol),),
        )
    database.chmod(0o600)
    config_path(state_dir).write_text(
        json.dumps(
            {
                "native_broker": {
                    "path": str(native_broker_executable()),
                    "allow_development": True,
                }
            }
        ),
        encoding="utf-8",
    )
    config_path(state_dir).chmod(0o600)


def test_a_pre_native_spool_is_refused_with_the_broker_s_own_code(tmp_path: Path):
    """The generation check belongs to the broker, so its refusal carries the broker's code.

    A client that reads `coordinator_meta` itself can only invent a message, which no caller
    can match on and no adapter can translate.
    """
    state_dir = tmp_path / "state"
    _pre_native_spool(state_dir)
    client = CoordinatorClient(state_dir=state_dir, autostart=False)

    with pytest.raises(CoordinatorError) as refused:
        client.drain_status()

    assert refused.value.code == "broker-protocol-unsupported", str(refused.value)
    assert "4" in str(refused.value)


def test_an_empty_state_directory_is_refused_with_the_broker_s_own_code(tmp_path: Path):
    """An absent spool is equally the broker's answer, not a guess from a missing file."""
    state_dir = tmp_path / "state"
    state_dir.mkdir(mode=0o700)
    config_path(state_dir).write_text(
        json.dumps(
            {
                "native_broker": {
                    "path": str(native_broker_executable()),
                    "allow_development": True,
                }
            }
        ),
        encoding="utf-8",
    )
    config_path(state_dir).chmod(0o600)
    client = CoordinatorClient(state_dir=state_dir, autostart=False)

    with pytest.raises(CoordinatorError) as refused:
        client.drain_status()

    assert refused.value.code == "broker-state-missing", str(refused.value)


def test_a_served_spool_reports_its_owner_through_the_broker(coordinator):
    """`ping` describes the live owner from the broker's answer, not from `broker.lock`."""
    running, client = coordinator

    owner = client.ping()

    assert owner["broker_pid"] == running.process.pid
    assert owner["protocol"] == 5
    assert owner["capacities"]["jobs"] == 2
