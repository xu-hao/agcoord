"""Machine-local coordination for development agents and repository gates."""

from .queue import (
    ChildCpuLease,
    CoordinatorClient,
    CoordinatorError,
    RepositoryIdentity,
    discover_repository,
)

__all__ = [
    "ChildCpuLease",
    "CoordinatorClient",
    "CoordinatorError",
    "RepositoryIdentity",
    "discover_repository",
]

__version__ = "0.6.3"
