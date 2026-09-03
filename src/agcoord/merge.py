"""Forge-neutral exact-head publication contract.

Adapters provide strict metadata and atomic ref-publication implementations. Preparation may
merge an advanced target into the source; publication never rewrites the gated checkout.
"""

from .github import (
    EXIT_HEAD_CHANGED,
    EXIT_MERGE_ERROR,
    EXIT_PR_NOT_READY,
    EXIT_PUBLISH_FAILED,
    EXIT_STALE_MAIN,
    FAILURE_REASONS,
    MergePublisher,
    PullRequestMetadataClient,
    Wait,
    execute,
    prepare,
    preflight,
)

__all__ = [
    "EXIT_HEAD_CHANGED",
    "EXIT_MERGE_ERROR",
    "EXIT_PR_NOT_READY",
    "EXIT_PUBLISH_FAILED",
    "EXIT_STALE_MAIN",
    "FAILURE_REASONS",
    "MergePublisher",
    "PullRequestMetadataClient",
    "Wait",
    "execute",
    "prepare",
    "preflight",
]
