"""Machine-local coordination for development agents and repository gates."""

from .queue import (
    ChildCpuLease,
    CoordinatorBroker,
    CoordinatorClient,
    CoordinatorError,
    RepositoryIdentity,
    discover_repository,
)

__all__ = [
    "ChildCpuLease",
    "CoordinatorBroker",
    "CoordinatorClient",
    "CoordinatorError",
    "RepositoryIdentity",
    "discover_repository",
]

__version__ = "0.5.2"
