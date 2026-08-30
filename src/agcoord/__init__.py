"""Machine-local coordination for development agents and repository gates."""

from .queue import (
    CoordinatorBroker,
    CoordinatorClient,
    CoordinatorError,
    RepositoryIdentity,
    discover_repository,
)

__all__ = [
    "CoordinatorBroker",
    "CoordinatorClient",
    "CoordinatorError",
    "RepositoryIdentity",
    "discover_repository",
]

__version__ = "0.1.1"
