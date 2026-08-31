"""Backend-neutral contracts for admitted and enforced machine resources.

Resource names remain project-defined admission tokens until the broker configuration
binds a name to a typed unit and an enforcement backend.  This module deliberately owns
only the contract and backend lifecycle seam; concrete cgroup, filesystem, and container
implementations live behind that seam.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
import re
from types import MappingProxyType
from typing import Mapping, Protocol, runtime_checkable


RESOURCE_MODES = ("admission-only", "best-effort", "required")
RESOURCE_UNITS_BY_KIND = {
    "generic": frozenset({"admission-unit"}),
    "cpu": frozenset({"logical-cpu"}),
    "memory": frozenset({"bytes"}),
    "memory-high": frozenset({"bytes"}),
    "swap": frozenset({"bytes"}),
    "tmpfs": frozenset({"bytes"}),
    "storage": frozenset({"bytes"}),
    "io-bandwidth": frozenset({"bytes-per-second"}),
    "io-operations": frozenset({"operations-per-second"}),
    "inodes": frozenset({"inodes"}),
    "processes": frozenset({"processes"}),
}
RESOURCE_OPERATIONS = (
    "prepare",
    "attach",
    "usage",
    "finish",
    "cancel",
    "cleanup",
)
RESOURCE_STAGES = frozenset(("probe", *RESOURCE_OPERATIONS))
RESOURCE_EVENT_STATUSES = frozenset({"applied", "recorded", "unapplied", "failed"})
_NAME = re.compile(r"^[a-z][a-z0-9_.:-]{0,63}$")
_CODE = re.compile(r"^[a-z][a-z0-9-]{0,63}$")
_BINDING_KEYS = frozenset({"kind", "unit", "mode", "backend"})
_CAPABILITY_KEYS = frozenset(
    {"available", "kinds", "units", "operations", "reason"}
)
_RECEIPT_KEYS = frozenset({"requested", "applied", "peak", "events"})
_EVENT_KEYS = frozenset({"at", "backend", "resource", "stage", "status", "code"})
ADMISSION_BINDING: dict[str, object] = {
    "backend": None,
    "kind": "generic",
    "mode": "admission-only",
    "unit": "admission-unit",
}


class ResourceContractError(RuntimeError):
    """A malformed public resource contract or backend response."""


class ResourceBackendError(ResourceContractError):
    """A backend refusal with a sanitized code safe for durable public receipts."""

    def __init__(self, code: str, message: str) -> None:
        if not isinstance(code, str) or not _CODE.fullmatch(code):
            raise ValueError("resource backend error code is invalid")
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class ResourceObservation:
    """One sanitized backend fact to record in the durable resource events."""

    resource: str
    code: str


@dataclass(frozen=True)
class ResourceMeasurement:
    """Peak values plus stable, non-sensitive observations from one sample."""

    peak: Mapping[str, int]
    observations: tuple[ResourceObservation, ...] = ()


@dataclass(frozen=True)
class ResourceRequest:
    """One backend's immutable subset of an admitted run's resource contract."""

    run_id: str
    backend: str
    resources: Mapping[str, int]
    bindings: Mapping[str, Mapping[str, object]]

    @classmethod
    def build(
        cls,
        run_id: str,
        backend: str,
        resources: Mapping[str, int],
        bindings: Mapping[str, Mapping[str, object]],
    ) -> ResourceRequest:
        frozen_bindings = {
            name: MappingProxyType(dict(binding))
            for name, binding in sorted(bindings.items())
        }
        return cls(
            run_id=run_id,
            backend=backend,
            resources=MappingProxyType(dict(sorted(resources.items()))),
            bindings=MappingProxyType(frozen_bindings),
        )


@runtime_checkable
class ResourceBackend(Protocol):
    """Lifecycle implemented by an optional native or container resource backend.

    ``prepare`` must return JSON-serializable private state. ``attach`` applies the
    prepared controls before AGCoord releases the blocked worker launcher. Lifecycle
    methods must be idempotent because a replacement broker can resume a durable row.
    Public failures are converted to stable event codes instead of exposing backend
    exception text or host paths.
    """

    def probe(self) -> Mapping[str, object]: ...

    def prepare(self, request: ResourceRequest) -> Mapping[str, object]: ...

    def attach(
        self,
        request: ResourceRequest,
        state: Mapping[str, object],
        worker_pid: int,
    ) -> None: ...

    def usage(
        self,
        request: ResourceRequest,
        state: Mapping[str, object],
    ) -> Mapping[str, int] | ResourceMeasurement: ...

    def finish(
        self,
        request: ResourceRequest,
        state: Mapping[str, object],
    ) -> Mapping[str, int] | ResourceMeasurement: ...

    def cancel(
        self,
        request: ResourceRequest,
        state: Mapping[str, object],
    ) -> None: ...

    def cleanup(
        self,
        request: ResourceRequest,
        state: Mapping[str, object],
    ) -> None: ...


def _resource_mapping(
    value: object,
    *,
    subject: str,
    allow_zero: bool,
) -> dict[str, int]:
    if not isinstance(value, Mapping):
        raise ResourceContractError(f"{subject} must be a resource-to-integer mapping")
    selected: dict[str, int] = {}
    for name, units in value.items():
        if not isinstance(name, str) or not _NAME.fullmatch(name):
            raise ResourceContractError(f"invalid {subject} resource name {name!r}")
        if (
            not isinstance(units, int)
            or isinstance(units, bool)
            or units < 0
            or (not allow_zero and units == 0)
        ):
            qualifier = "non-negative" if allow_zero else "positive"
            raise ResourceContractError(
                f"{subject} resource {name!r} must be a {qualifier} integer"
            )
        selected[name] = units
    return dict(sorted(selected.items()))


def validate_resource_bindings(
    value: Mapping[str, Mapping[str, object]] | None,
) -> dict[str, dict[str, object]]:
    """Normalize strict machine bindings without making familiar names special."""
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ResourceContractError("resource bindings must be a name-to-binding mapping")
    selected: dict[str, dict[str, object]] = {}
    for name, raw in value.items():
        if not isinstance(name, str) or not _NAME.fullmatch(name):
            raise ResourceContractError(f"invalid resource binding name {name!r}")
        if not isinstance(raw, Mapping) or set(raw) != _BINDING_KEYS:
            raise ResourceContractError(
                f"resource binding {name!r} must contain exactly "
                "backend, kind, mode, and unit"
            )
        kind = raw["kind"]
        unit = raw["unit"]
        mode = raw["mode"]
        backend = raw["backend"]
        if not isinstance(kind, str) or kind not in RESOURCE_UNITS_BY_KIND:
            raise ResourceContractError(
                f"resource binding {name!r} has unknown kind {kind!r}"
            )
        if not isinstance(unit, str) or unit not in RESOURCE_UNITS_BY_KIND[kind]:
            raise ResourceContractError(
                f"resource binding {name!r} cannot use unit {unit!r} for kind {kind!r}"
            )
        if not isinstance(mode, str) or mode not in RESOURCE_MODES:
            raise ResourceContractError(
                f"resource binding {name!r} has unknown mode {mode!r}"
            )
        if mode == "admission-only":
            if backend is not None:
                raise ResourceContractError(
                    f"admission-only resource binding {name!r} cannot select a backend"
                )
        elif not isinstance(backend, str) or not _NAME.fullmatch(backend):
            raise ResourceContractError(
                f"enforced resource binding {name!r} needs a valid backend name"
            )
        selected[name] = {
            "backend": backend,
            "kind": kind,
            "mode": mode,
            "unit": unit,
        }
    return dict(sorted(selected.items()))


def configured_resource_bindings(
    section: Mapping[str, Mapping[str, object]] | None,
) -> dict[str, dict[str, object]]:
    """Validate the bindings section of one state directory's configuration file."""
    if section is None:
        return {}
    return validate_resource_bindings(section)


def resource_contract(
    resources: Mapping[str, int],
    bindings: Mapping[str, Mapping[str, object]],
) -> dict[str, dict[str, object]]:
    requested = _resource_mapping(resources, subject="requested", allow_zero=False)
    configured = validate_resource_bindings(bindings)
    return {
        name: dict(configured.get(name, ADMISSION_BINDING))
        for name in requested
    }


def validate_resource_contract(
    value: object,
    resources: Mapping[str, int],
) -> dict[str, dict[str, object]]:
    requested = _resource_mapping(resources, subject="requested", allow_zero=False)
    contract = validate_resource_bindings(value if isinstance(value, Mapping) else None)
    if set(contract) != set(requested):
        raise ResourceContractError(
            "resource contract names do not match the requested resource names"
        )
    return contract


def initial_resource_receipt(resources: Mapping[str, int]) -> dict[str, object]:
    return {
        "requested": _resource_mapping(
            resources,
            subject="requested",
            allow_zero=False,
        ),
        "applied": {},
        "peak": {},
        "events": [],
    }


def validate_resource_measurement(
    value: object,
    *,
    expected: set[str],
) -> tuple[dict[str, int], tuple[ResourceObservation, ...]]:
    """Normalize one backend sample without admitting unexpected public names."""

    if isinstance(value, ResourceMeasurement):
        raw_peak = value.peak
        raw_observations = value.observations
    else:
        raw_peak = value
        raw_observations = ()
    peak = _resource_mapping(
        raw_peak,
        subject="resource usage",
        allow_zero=True,
    )
    if not set(peak).issubset(expected):
        raise ResourceContractError("resource usage names an unexpected resource")
    if not isinstance(raw_observations, tuple):
        raise ResourceContractError("resource observations must be a tuple")
    observations: list[ResourceObservation] = []
    for observation in raw_observations:
        if not isinstance(observation, ResourceObservation):
            raise ResourceContractError("resource observation is invalid")
        if (
            not isinstance(observation.resource, str)
            or observation.resource not in expected
        ):
            raise ResourceContractError(
                "resource observation names an unexpected resource"
            )
        if not isinstance(observation.code, str) or not _CODE.fullmatch(
            observation.code
        ):
            raise ResourceContractError("resource observation code is invalid")
        if observation not in observations:
            observations.append(observation)
    return peak, tuple(observations)


def validate_resource_receipt(
    value: object,
    resources: Mapping[str, int],
) -> dict[str, object]:
    requested_resources = _resource_mapping(
        resources,
        subject="requested",
        allow_zero=False,
    )
    if not isinstance(value, Mapping) or set(value) != _RECEIPT_KEYS:
        raise ResourceContractError(
            "resource receipt must contain exactly requested, applied, peak, and events"
        )
    requested = _resource_mapping(
        value["requested"],
        subject="receipt requested",
        allow_zero=False,
    )
    if requested != requested_resources:
        raise ResourceContractError("resource receipt request does not match the run")
    applied = _resource_mapping(
        value["applied"],
        subject="receipt applied",
        allow_zero=False,
    )
    peak = _resource_mapping(
        value["peak"],
        subject="receipt peak",
        allow_zero=True,
    )
    if not set(applied).issubset(requested):
        raise ResourceContractError("resource receipt applied unknown resources")
    if any(applied[name] > requested[name] for name in applied):
        raise ResourceContractError("resource receipt applied more than was requested")
    if not set(peak).issubset(requested):
        raise ResourceContractError("resource receipt measured unknown resources")
    raw_events = value["events"]
    if not isinstance(raw_events, list):
        raise ResourceContractError("resource receipt events must be a list")
    events: list[dict[str, str]] = []
    for raw in raw_events:
        if not isinstance(raw, Mapping) or set(raw) != _EVENT_KEYS:
            raise ResourceContractError(
                "resource receipt events have a strict six-field shape"
            )
        at = raw["at"]
        backend = raw["backend"]
        resource = raw["resource"]
        stage = raw["stage"]
        status = raw["status"]
        code = raw["code"]
        if not isinstance(at, str) or not at:
            raise ResourceContractError("resource event timestamp must be non-empty")
        if not isinstance(backend, str) or not _NAME.fullmatch(backend):
            raise ResourceContractError("resource event backend is invalid")
        if resource not in requested:
            raise ResourceContractError("resource event names an unrequested resource")
        if stage not in RESOURCE_STAGES:
            raise ResourceContractError(f"resource event stage {stage!r} is invalid")
        if status not in RESOURCE_EVENT_STATUSES:
            raise ResourceContractError(f"resource event status {status!r} is invalid")
        if not isinstance(code, str) or not _CODE.fullmatch(code):
            raise ResourceContractError("resource event code is invalid")
        events.append(
            {
                "at": at,
                "backend": backend,
                "resource": resource,
                "stage": stage,
                "status": status,
                "code": code,
            }
        )
    return {
        "requested": requested,
        "applied": applied,
        "peak": peak,
        "events": events,
    }


def validate_resource_backends(
    value: Mapping[str, ResourceBackend] | None,
) -> dict[str, ResourceBackend]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ResourceContractError("resource backends must be a name-to-backend mapping")
    selected: dict[str, ResourceBackend] = {}
    for name, backend in value.items():
        if not isinstance(name, str) or not _NAME.fullmatch(name):
            raise ResourceContractError(f"invalid resource backend name {name!r}")
        if not isinstance(backend, ResourceBackend):
            raise ResourceContractError(
                f"resource backend {name!r} does not implement the complete lifecycle"
            )
        selected[name] = backend
    return dict(sorted(selected.items()))


def _capability(value: object, *, backend: str) -> dict[str, object]:
    if not isinstance(value, Mapping) or set(value) != _CAPABILITY_KEYS:
        raise ResourceContractError(
            f"resource backend {backend!r} returned an invalid capability shape"
        )
    available = value["available"]
    if not isinstance(available, bool):
        raise ResourceContractError(
            f"resource backend {backend!r} capability availability must be boolean"
        )

    def names(field: str, allowed: set[str] | frozenset[str]) -> list[str]:
        raw = value[field]
        if not isinstance(raw, list) or not all(isinstance(item, str) for item in raw):
            raise ResourceContractError(
                f"resource backend {backend!r} capability {field} must be a string list"
            )
        if len(raw) != len(set(raw)) or any(item not in allowed for item in raw):
            raise ResourceContractError(
                f"resource backend {backend!r} capability {field} is invalid"
            )
        return sorted(raw)

    kinds = names("kinds", set(RESOURCE_UNITS_BY_KIND))
    units = names(
        "units",
        frozenset().union(*RESOURCE_UNITS_BY_KIND.values()),
    )
    operations = names("operations", set(RESOURCE_OPERATIONS))
    reason = value["reason"]
    if available:
        if reason is not None:
            raise ResourceContractError(
                f"available resource backend {backend!r} cannot have a refusal reason"
            )
    elif not isinstance(reason, str) or not _CODE.fullmatch(reason):
        raise ResourceContractError(
            f"unavailable resource backend {backend!r} needs a stable reason code"
        )
    return {
        "available": available,
        "kinds": kinds,
        "units": units,
        "operations": operations,
        "reason": reason,
    }


def probe_resource_backends(
    backends: Mapping[str, ResourceBackend],
    bindings: Mapping[str, Mapping[str, object]],
) -> dict[str, dict[str, object]]:
    selected_backends = validate_resource_backends(backends)
    selected_bindings = validate_resource_bindings(bindings)
    referenced = {
        binding["backend"]
        for binding in selected_bindings.values()
        if binding["backend"] is not None
    }
    capabilities: dict[str, dict[str, object]] = {}
    for name in sorted(set(selected_backends) | referenced):
        backend = selected_backends.get(name)
        if backend is None:
            capabilities[name] = {
                "available": False,
                "kinds": [],
                "units": [],
                "operations": [],
                "reason": "backend-unavailable",
            }
            continue
        try:
            capabilities[name] = _capability(backend.probe(), backend=name)
        except Exception:
            capabilities[name] = {
                "available": False,
                "kinds": [],
                "units": [],
                "operations": [],
                "reason": "probe-failed",
            }
    return capabilities


def validate_resource_capabilities(
    value: object,
) -> dict[str, dict[str, object]]:
    if not isinstance(value, Mapping):
        raise ResourceContractError("resource capabilities must be a backend mapping")
    selected: dict[str, dict[str, object]] = {}
    for name, raw in value.items():
        if not isinstance(name, str) or not _NAME.fullmatch(name):
            raise ResourceContractError(f"invalid resource capability backend {name!r}")
        selected[name] = _capability(raw, backend=name)
    return dict(sorted(selected.items()))


def capability_issue(
    binding: Mapping[str, object],
    capability: Mapping[str, object] | None,
) -> str | None:
    if capability is None:
        return "backend-unavailable"
    if not capability["available"]:
        return str(capability["reason"])
    if binding["kind"] not in capability["kinds"]:
        return "kind-unsupported"
    if binding["unit"] not in capability["units"]:
        return "unit-unsupported"
    if not set(RESOURCE_OPERATIONS).issubset(capability["operations"]):
        return "lifecycle-unsupported"
    return None


def validate_backend_state(value: object) -> dict[str, dict[str, object]]:
    """Validate private durable handles without exposing their values publicly."""
    if not isinstance(value, Mapping):
        raise ResourceContractError("resource backend state must be a backend mapping")
    selected: dict[str, dict[str, object]] = {}
    for backend, raw in value.items():
        if not isinstance(backend, str) or not _NAME.fullmatch(backend):
            raise ResourceContractError("resource backend state has an invalid name")
        if not isinstance(raw, Mapping) or set(raw) != {
            "handle",
            "resources",
            "finished",
            "cancelled",
        }:
            raise ResourceContractError(
                f"resource backend state {backend!r} has an invalid shape"
            )
        if not isinstance(raw["finished"], bool) or not isinstance(raw["cancelled"], bool):
            raise ResourceContractError(
                f"resource backend state {backend!r} has invalid lifecycle flags"
            )
        resources = raw["resources"]
        if (
            not isinstance(resources, list)
            or not resources
            or len(resources) != len(set(resources))
            or not all(isinstance(name, str) and _NAME.fullmatch(name) for name in resources)
        ):
            raise ResourceContractError(
                f"resource backend state {backend!r} has invalid resource names"
            )
        try:
            handle = json.loads(json.dumps(raw["handle"], separators=(",", ":")))
        except (TypeError, ValueError) as exc:
            raise ResourceContractError(
                f"resource backend {backend!r} returned non-JSON private state"
            ) from exc
        if not isinstance(handle, dict):
            raise ResourceContractError(
                f"resource backend {backend!r} private state must be a JSON object"
            )
        selected[backend] = {
            "handle": handle,
            "resources": sorted(resources),
            "finished": raw["finished"],
            "cancelled": raw["cancelled"],
        }
    return dict(sorted(selected.items()))


def resource_enforcement_summary(receipt: Mapping[str, object]) -> str:
    """Return one stable operator label derived from a strict durable receipt."""
    events = receipt.get("events", [])
    applied = bool(receipt.get("applied", {}))
    selected_events = (
        [event for event in events if isinstance(event, Mapping)]
        if isinstance(events, list)
        else []
    )
    failed = any(event.get("status") == "failed" for event in selected_events)
    unapplied = any(event.get("status") == "unapplied" for event in selected_events)
    if failed:
        return "failed"
    if applied and unapplied:
        return "partial"
    if unapplied:
        return "unapplied"
    if applied:
        return "applied"
    return "admission-only"
