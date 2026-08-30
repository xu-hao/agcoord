from __future__ import annotations

import os
from pathlib import Path
import threading
import time
from typing import Callable, TypeVar

import pytest

from agcoord.queue import CoordinatorBroker, CoordinatorClient, CoordinatorError


T = TypeVar("T")


def wait_for(
    observe: Callable[[], T],
    failure: str,
    *,
    timeout: float = 10.0,
) -> T:
    deadline = time.monotonic() + timeout
    last_error: BaseException | None = None
    while time.monotonic() < deadline:
        try:
            result = observe()
            if result:
                return result
        except (CoordinatorError, FileNotFoundError, ProcessLookupError) as exc:
            last_error = exc
        time.sleep(0.01)
    detail = f"; last error: {last_error}" if last_error is not None else ""
    pytest.fail(f"{failure}{detail}")


def caller_environment() -> dict[str, str]:
    environment = dict(os.environ)
    for variable in (
        "AGCOORD_RUN_ID",
        "AGCOORD_RUN_KIND",
        "AGCOORD_STATE_DIR",
    ):
        environment.pop(variable, None)
    return environment


class RunningCoordinator:
    def __init__(
        self,
        state_dir: Path,
        *,
        capacities: dict[str, int] | None = None,
    ) -> None:
        self.broker = CoordinatorBroker(
            state_dir=state_dir,
            capacities=capacities,
            idle_timeout=None,
        )
        self.errors: list[BaseException] = []
        self.thread = threading.Thread(
            target=self._serve,
            name="test-agcoord-broker",
        )

    def _serve(self) -> None:
        try:
            self.broker.serve_forever()
        except BaseException as exc:  # pragma: no cover - exposed by start/stop
            self.errors.append(exc)

    def start(self) -> CoordinatorClient:
        self.thread.start()
        wait_for(
            lambda: self.broker.ready.is_set() or self.errors,
            "the coordinator never acquired spool ownership",
        )
        if self.errors:
            raise self.errors[0]
        client = CoordinatorClient(
            state_dir=self.broker.paths.state_dir,
            autostart=False,
        )
        wait_for(client.snapshot, "the coordinator never exposed a snapshot")
        return client

    def stop(self) -> None:
        self.broker.close()
        self.broker.close()
        self.thread.join(timeout=10)
        assert not self.thread.is_alive(), "the coordinator did not stop"
        if self.errors:
            raise self.errors[0]


@pytest.fixture
def coordinator(tmp_path: Path):
    running = RunningCoordinator(tmp_path / "state", capacities={"jobs": 2})
    client = running.start()
    yield running.broker, client
    running.stop()
