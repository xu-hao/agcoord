"""Optional pytest-xdist adapter for an admitted run's child CPU budget."""

from __future__ import annotations

import os
import warnings

import pytest

from .queue import (
    ChildCpuLease,
    CoordinatorClient,
    CoordinatorError,
    RUN_ID_ENV,
    STATE_DIR_ENV,
)


_LEASE_KEY = pytest.StashKey[ChildCpuLease]()


def _is_xdist_controller(config: pytest.Config) -> bool:
    return (
        config.pluginmanager.hasplugin("xdist")
        and not hasattr(config, "workerinput")
    )


def _has_admitted_context() -> bool:
    return bool(os.environ.get(RUN_ID_ENV) and os.environ.get(STATE_DIR_ENV))


def _should_lease(config: pytest.Config) -> bool:
    return (
        _is_xdist_controller(config)
        and _has_admitted_context()
        and not config.getoption("collectonly", default=False)
    )


def _lease_for(config: pytest.Config) -> ChildCpuLease | None:
    return config.stash.get(_LEASE_KEY, None)


def _client_and_budget() -> tuple[CoordinatorClient, int]:
    state_dir = os.environ[STATE_DIR_ENV]
    run_id = os.environ[RUN_ID_ENV]
    client = CoordinatorClient(state_dir=state_dir, autostart=False)
    try:
        row = client.admitted_run_status(run_id)
        budget = row["resources"].get("cpu")
    except (AttributeError, CoordinatorError, KeyError, TypeError) as exc:
        raise pytest.UsageError(
            f"AGCoord pytest-xdist adapter cannot read parent CPU budget: {exc}"
        ) from exc
    if not isinstance(budget, int) or isinstance(budget, bool) or budget <= 0:
        raise pytest.UsageError(
            "AGCoord pytest-xdist adapter needs the admitted run to declare "
            "--resource cpu=N"
        )
    return client, budget


def _acquire(
    config: pytest.Config,
    *,
    requested: int,
    minimum: int,
    client: CoordinatorClient | None = None,
) -> ChildCpuLease:
    existing = _lease_for(config)
    if existing is not None:
        return existing
    if client is None:
        client, _budget = _client_and_budget()
    try:
        lease = client.acquire_child_cpu_lease(
            requested,
            minimum=minimum,
        )
    except CoordinatorError as exc:
        raise pytest.UsageError(
            "AGCoord pytest-xdist adapter could not acquire "
            f"{requested} CPU worker token(s): {exc}"
        ) from exc
    config.stash[_LEASE_KEY] = lease
    return lease


@pytest.hookimpl(tryfirst=True, optionalhook=True)
def pytest_xdist_auto_num_workers(config: pytest.Config) -> int | None:
    """Size ``-n auto`` and ``-n logical`` from one partial child lease."""
    if not _should_lease(config):
        return None
    client, budget = _client_and_budget()
    maximum = config.getoption("maxprocesses", default=None)
    requested = min(budget, maximum) if maximum and maximum > 0 else budget
    return _acquire(
        config,
        requested=requested,
        minimum=1,
        client=client,
    ).granted


@pytest.hookimpl(tryfirst=True)
def pytest_configure(config: pytest.Config) -> None:
    """Acquire an exact lease for an explicit positive ``-n N`` request."""
    if not _should_lease(config) or _lease_for(config) is not None:
        return
    workers = getattr(config.option, "numprocesses", None)
    if not isinstance(workers, int) or isinstance(workers, bool) or workers <= 0:
        return
    _acquire(config, requested=workers, minimum=workers)


@pytest.hookimpl(trylast=True)
def pytest_unconfigure(config: pytest.Config) -> None:
    """Hold the controller lease through worker teardown, then return it once."""
    lease = _lease_for(config)
    if lease is None:
        return
    try:
        lease.release()
    except CoordinatorError as exc:
        warnings.warn(
            pytest.PytestWarning(
                "AGCoord pytest-xdist adapter could not release child CPU lease "
                f"{lease.lease_id}: {exc}; the broker will reclaim it when the "
                "controller exits"
            ),
            stacklevel=2,
        )
    finally:
        del config.stash[_LEASE_KEY]
