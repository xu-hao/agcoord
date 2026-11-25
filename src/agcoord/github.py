"""Prepare and publish one exact pull-request head through GitHub.

Before gating, the worker can merge an advanced target into the same-repository ticket
branch with a lease-protected push. Publication then observes the exact synchronized head,
asks the forge to create a merge commit whose tree is exactly that gated head, and atomically
compares and updates both refs. A changed source or post-gate target rejects publication.

GitHub can fail a publication request transiently (a transport error, an HTTP ``5xx``, or an
internal GraphQL error), including after it already applied the request. The publisher
therefore classifies each failure, retries a transient one a bounded number of times while the
caller keeps the landing lane, and never resends the atomic ref transaction until it has read
the remote refs and proved the forge did not apply it.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
from io import StringIO
from typing import Any, Callable, Mapping, Protocol, TextIO, TypeVar
from uuid import uuid4


RUN_ID_ENV = "AGCOORD_RUN_ID"
# After the coordinator's own lease-protected push, the forge can briefly keep reporting the
# head that push replaced. Exactly that head is re-read for a short bounded wait; every other
# head is a real source change.
PROPAGATION_WAIT_SECONDS = 30.0
PROPAGATION_POLL_SECONDS = 2.0
SUBMISSION_RETRY_DELAYS = (0.5, 1.0, 2.0, 4.0)
# Delays before each retry of a transient GitHub publication failure: four retries, so five
# attempts in at most 7.5 seconds of waiting.
PUBLICATION_RETRY_DELAYS = (0.5, 1.0, 2.0, 4.0)
Wait = Callable[[float], None]
_T = TypeVar("_T")
EXIT_STALE_MAIN = 75
EXIT_HEAD_CHANGED = 76
EXIT_PR_NOT_READY = 77
EXIT_PUBLISH_FAILED = 78
EXIT_MERGE_ERROR = 79
EXIT_AVOIDED = 80
FAILURE_REASONS = {
    EXIT_STALE_MAIN: "stale-main",
    EXIT_HEAD_CHANGED: "head-changed",
    EXIT_PR_NOT_READY: "pr-not-ready",
    EXIT_PUBLISH_FAILED: "publish-failed",
    EXIT_MERGE_ERROR: "merge-error",
    EXIT_AVOIDED: "avoided-commit",
}
_METADATA_FIELDS = {
    "number",
    "state",
    "is_draft",
    "base_ref",
    "head_ref",
    "head_sha",
    "same_repository",
    "title",
    "head_owner",
}


class PullRequestMetadataClient(Protocol):
    def pull_request(self, number: int) -> dict[str, object]: ...


class MergePublisher(Protocol):
    """Forge seam for publication.

    An implementation owns its forge's transient-failure policy: ``create_merge_commit`` and
    ``update_refs`` either complete, or raise after any bounded reconciliation and retry the
    forge needs, so a caller never has to tell a transient fault from a refusal itself.
    """

    def create_merge_commit(
        self,
        *,
        message: str,
        tree_sha: str,
        parent_shas: tuple[str, str],
    ) -> dict[str, object]: ...

    def update_refs(self, updates: tuple[dict[str, object], ...]) -> None: ...

    def is_ancestor(self, *, ancestor_sha: str, descendant_sha: str) -> bool: ...


class _MergeRefusal(RuntimeError):
    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status


class _TransientForgeError(RuntimeError):
    """One GitHub failure that says nothing about whether the request was applied."""


class _PublicationRetriesExhausted(RuntimeError):
    """Transient GitHub failures outlasted the bounded publication retry budget."""


_TRANSIENT_HTTP_STATUSES = frozenset({500, 502, 503, 504})
_HTTP_STATUS = re.compile(r"\bHTTP\s*:?\s*(\d{3})\b|\((\d{3})\)", re.IGNORECASE)
_SERVER_FAILURE = re.compile(
    r"something went wrong while executing your query|bad gateway|service unavailable|"
    r"gateway time-?out|server error",
    re.IGNORECASE,
)
_INTERNAL_GRAPHQL_ERROR = re.compile(
    r"something went wrong while executing your query", re.IGNORECASE
)
_TRANSPORT_FAILURE = re.compile(
    r"connection (?:reset|refused|timed out|closed)|i/o timeout|timeout awaiting|"
    r"tls handshake timeout|context deadline exceeded|client\.timeout|no such host|"
    r"temporary failure in name resolution|network is unreachable|unexpected eof|\beof\b|"
    r"broken pipe|server closed idle connection|error connecting to|timed out",
    re.IGNORECASE,
)


def _graphql_error_messages(body: object) -> list[str] | None:
    """Return the messages of a GraphQL ``errors`` response body, or None for any other body."""
    if not isinstance(body, dict):
        return None
    errors = body.get("errors")
    if not isinstance(errors, list) or not errors:
        return None
    return [
        str(error.get("message", "")) if isinstance(error, dict) else str(error)
        for error in errors
    ]


def _is_transient_failure(stdout: str, stderr: str) -> bool:
    """Decide whether a failed ``gh`` call is a transport or server fault worth retrying.

    A GraphQL error body or any HTTP status other than a server fault is GitHub's considered
    answer and is permanent: validation, permission, policy, and compare-and-swap refusals
    must never be retried. Only an internal GraphQL error, an HTTP ``500``, ``502``, ``503``,
    or ``504``, and a connection-level failure with no answer at all are transient.
    """
    try:
        messages = _graphql_error_messages(json.loads(stdout))
    except ValueError:
        messages = None
    if messages is not None:
        return any(_INTERNAL_GRAPHQL_ERROR.search(message) for message in messages)
    text = f"{stderr}\n{stdout}"
    statuses = {
        int(code)
        for match in _HTTP_STATUS.finditer(text)
        for code in match.groups()
        if code
    }
    if statuses & _TRANSIENT_HTTP_STATUSES:
        return True
    if statuses:
        return False
    return bool(_SERVER_FAILURE.search(text) or _TRANSPORT_FAILURE.search(text))


def _gh_failure(result: subprocess.CompletedProcess[str]) -> RuntimeError:
    detail = result.stderr.strip() or result.stdout.strip() or f"exit {result.returncode}"
    if _is_transient_failure(result.stdout, result.stderr):
        return _TransientForgeError(detail)
    return RuntimeError(detail)


def _attempts(count: int) -> str:
    return f"{count} attempt" if count == 1 else f"{count} attempts"


def _notify_stderr(message: str) -> None:
    print(f"GitHub publisher: {message}", file=sys.stderr, flush=True)


class SubmissionRefusal(RuntimeError):
    """Typed refusal from checking a pull request head before queueing a land."""

    def __init__(self, message: str, *, code: str):
        super().__init__(message)
        self.code = code


class _PreflightComplete(BaseException):
    """Stop the publication executor at the exact no-mutation boundary."""


class _PreflightPublisher:
    """Delegate observations while refusing to create or update forge objects."""

    def __init__(self, publisher: MergePublisher):
        self.publisher = publisher

    def create_merge_commit(
        self,
        *,
        message: str,
        tree_sha: str,
        parent_shas: tuple[str, str],
    ) -> dict[str, object]:
        raise _PreflightComplete

    def update_refs(self, updates: tuple[dict[str, object], ...]) -> None:
        raise AssertionError("preflight cannot update references")

    def is_ancestor(self, *, ancestor_sha: str, descendant_sha: str) -> bool:
        return self.publisher.is_ancestor(
            ancestor_sha=ancestor_sha,
            descendant_sha=descendant_sha,
        )


def _run(
    command: list[str],
    *,
    checkout: Path,
    input_text: str | None = None,
    env: Mapping[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            command,
            cwd=checkout,
            input=input_text,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
            env=env,
        )
    except OSError as exc:
        raise _MergeRefusal(
            EXIT_MERGE_ERROR,
            f"cannot run {command[0]}: {exc}",
        ) from exc


def _git(
    checkout: Path,
    *arguments: str,
    input_text: str | None = None,
    status: int = EXIT_MERGE_ERROR,
    operation: str | None = None,
) -> str:
    result = _run(
        ["git", "-C", str(checkout), *arguments],
        checkout=checkout,
        input_text=input_text,
    )
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip() or f"exit {result.returncode}"
        raise _MergeRefusal(
            status,
            f"{operation or 'Git operation failed'}: {detail}",
        )
    return result.stdout.strip()


def _sha(value: object, *, field: str) -> str:
    if not isinstance(value, str) or len(value) != 40:
        raise _MergeRefusal(
            EXIT_MERGE_ERROR,
            f"{field} must be exactly 40 hexadecimal characters",
        )
    lowered = value.lower()
    if any(character not in "0123456789abcdef" for character in lowered):
        raise _MergeRefusal(
            EXIT_MERGE_ERROR,
            f"{field} must contain only hexadecimal characters",
        )
    return lowered


def _metadata(record: dict[str, object], pull_request: int) -> dict[str, object]:
    if not isinstance(record, dict) or set(record) != _METADATA_FIELDS:
        raise _MergeRefusal(
            EXIT_MERGE_ERROR,
            "pull-request metadata does not match the coordinator contract",
        )
    if record["number"] != pull_request:
        raise _MergeRefusal(EXIT_MERGE_ERROR, "pull-request metadata has the wrong number")
    if record["state"] not in {"OPEN", "MERGED"}:
        raise _MergeRefusal(
            EXIT_PR_NOT_READY,
            f"pull request #{pull_request} is {record['state']!r}, not open or merged",
        )
    if not isinstance(record["is_draft"], bool):
        raise _MergeRefusal(EXIT_MERGE_ERROR, "pull-request draft state is not boolean")
    if not isinstance(record["same_repository"], bool):
        raise _MergeRefusal(EXIT_MERGE_ERROR, "pull-request repository state is not boolean")
    for field in ("base_ref", "head_ref", "title", "head_owner"):
        if not isinstance(record[field], str) or not record[field].strip():
            raise _MergeRefusal(
                EXIT_MERGE_ERROR,
                f"pull-request {field} must be a non-empty string",
            )
    normalized = dict(record)
    normalized["head_sha"] = _sha(record["head_sha"], field="pull-request head")
    return normalized


def _read_pull_request(
    metadata_client: PullRequestMetadataClient,
    pull_request: int,
    *,
    expected_head: str,
    replaced_head: str | None,
    wait: Wait,
    out: TextIO,
) -> dict[str, object]:
    """Read strict metadata, waiting briefly while the forge still reports a replaced head.

    Only the exact head that the coordinator's own push replaced counts as read-after-write
    lag. The caller still compares the returned head, so a replaced head that outlives the
    bounded wait, or any other head, remains a refusal.
    """
    waited = 0.0
    while True:
        try:
            record = metadata_client.pull_request(pull_request)
        except _MergeRefusal:
            raise
        except Exception as exc:
            raise _MergeRefusal(
                EXIT_MERGE_ERROR,
                f"cannot read pull request #{pull_request}: {exc}",
            ) from exc
        pr = _metadata(record, pull_request)
        if (
            replaced_head is None
            or pr["head_sha"] != replaced_head
            or waited >= PROPAGATION_WAIT_SECONDS
        ):
            return pr
        if waited == 0.0:
            print(
                f"Merge coordinator: forge still reports replaced head {replaced_head}; "
                f"waiting up to {PROPAGATION_WAIT_SECONDS:g}s for {expected_head} to propagate",
                file=out,
                flush=True,
            )
        wait(PROPAGATION_POLL_SECONDS)
        waited += PROPAGATION_POLL_SECONDS


def _remote_refs(
    checkout: Path,
    base: str,
    branch: str,
    *,
    require_head: bool,
) -> dict[str, str]:
    base_ref = f"refs/heads/{base}"
    head_ref = f"refs/heads/{branch}"
    output = _git(
        checkout,
        "ls-remote",
        "--refs",
        "origin",
        base_ref,
        head_ref,
        operation="cannot read remote base and pull-request refs",
    )
    refs: dict[str, str] = {}
    for line in output.splitlines():
        fields = line.split()
        if len(fields) != 2 or fields[1] not in {base_ref, head_ref}:
            raise _MergeRefusal(
                EXIT_MERGE_ERROR,
                "origin returned malformed merge reference data",
            )
        if fields[1] in refs:
            raise _MergeRefusal(
                EXIT_MERGE_ERROR,
                f"origin returned duplicate data for {fields[1]}",
            )
        refs[fields[1]] = _sha(fields[0], field=fields[1])
    if base_ref not in refs:
        raise _MergeRefusal(
            EXIT_PR_NOT_READY,
            f"pull-request base branch {base!r} does not exist on origin",
        )
    if require_head and head_ref not in refs:
        raise _MergeRefusal(EXIT_HEAD_CHANGED, f"origin is missing {head_ref}")
    return refs


def _created_commit(record: object) -> dict[str, object]:
    fields = {"sha", "tree_sha", "parent_shas"}
    if not isinstance(record, dict) or set(record) != fields:
        raise _MergeRefusal(
            EXIT_MERGE_ERROR,
            "created merge commit does not match the publisher contract",
        )
    parent_shas = record["parent_shas"]
    if (
        not isinstance(parent_shas, (list, tuple))
        or len(parent_shas) != 2
    ):
        raise _MergeRefusal(
            EXIT_MERGE_ERROR,
            "created merge commit must have exactly two ordered parents",
        )
    return {
        "sha": _sha(record["sha"], field="merge candidate"),
        "tree_sha": _sha(record["tree_sha"], field="merge candidate tree"),
        "parent_shas": tuple(
            _sha(parent, field="merge candidate parent") for parent in parent_shas
        ),
    }


def _is_ancestor(checkout: Path, ancestor: str, descendant: str) -> bool:
    result = _run(
        ["git", "-C", str(checkout), "merge-base", "--is-ancestor", ancestor, descendant],
        checkout=checkout,
    )
    if result.returncode == 0:
        return True
    if result.returncode in {1, 128}:
        return False
    detail = result.stderr.strip() or f"exit {result.returncode}"
    raise _MergeRefusal(EXIT_MERGE_ERROR, f"cannot compare merge ancestry: {detail}")


def _copy_output(source: StringIO, destination: TextIO) -> None:
    destination.write(source.getvalue())
    destination.flush()


def _restore_completed_merge(checkout: Path, old_head: str, merge_head: str) -> None:
    current_head = _git(
        checkout,
        "rev-parse",
        "HEAD",
        operation="cannot inspect synchronized source head",
    )
    dirty = _git(
        checkout,
        "status",
        "--porcelain=v1",
        "--untracked-files=all",
        operation="cannot inspect synchronized source checkout",
    )
    if current_head != merge_head or dirty:
        raise _MergeRefusal(
            EXIT_MERGE_ERROR,
            "source changed while target synchronization was being restored; "
            "the coordinator did not reset the checkout",
        )
    _git(
        checkout,
        "reset",
        "--hard",
        old_head,
        operation="cannot restore source after target synchronization failed",
    )
    restored = _git(
        checkout,
        "status",
        "--porcelain=v1",
        "--untracked-files=all",
        operation="cannot verify restored source checkout",
    )
    if restored:
        raise _MergeRefusal(
            EXIT_MERGE_ERROR,
            "target synchronization failed and did not restore a clean checkout",
        )


def _synchronize_target(
    pull_request: int,
    *,
    checkout: str,
    branch: str,
    head_sha: str,
    metadata_client: PullRequestMetadataClient,
    out: TextIO,
    err: TextIO,
    avoid_commits: Mapping[str, str] | None = None,
    allow_detached: bool = False,
) -> tuple[int, str]:
    """Merge one observed target into the source and push it with an exact lease."""
    expected_head = _sha(head_sha, field="submitted head")
    try:
        selected = Path(checkout).expanduser().resolve()
        current_branch = _git(
            selected,
            "rev-parse",
            "--abbrev-ref",
            "HEAD",
            operation="target synchronization requires the submitted branch",
        )
        current_head = _git(
            selected,
            "rev-parse",
            "HEAD",
            operation="cannot read source head before target synchronization",
        )
        dirty = _git(
            selected,
            "status",
            "--porcelain=v1",
            "--untracked-files=all",
            operation="cannot inspect source before target synchronization",
        )
        if (
            (current_branch != branch and not (allow_detached and current_branch == "HEAD"))
            or current_head != expected_head
            or dirty
        ):
            raise _MergeRefusal(
                EXIT_HEAD_CHANGED,
                "source checkout changed before target synchronization",
            )

        try:
            pr = _metadata(metadata_client.pull_request(pull_request), pull_request)
        except _MergeRefusal:
            raise
        except Exception as exc:
            raise _MergeRefusal(
                EXIT_MERGE_ERROR,
                f"cannot re-read pull request #{pull_request}: {exc}",
            ) from exc
        if (
            pr["state"] != "OPEN"
            or pr["is_draft"]
            or not pr["same_repository"]
            or pr["head_ref"] != branch
        ):
            raise _MergeRefusal(
                EXIT_PR_NOT_READY,
                "pull request is no longer an open, ready same-repository request",
            )
        if pr["head_sha"] != expected_head:
            raise _MergeRefusal(
                EXIT_HEAD_CHANGED,
                f"pull-request head changed from submitted {expected_head} "
                f"to {pr['head_sha']}",
            )
        base = str(pr["base_ref"])
        refs = _remote_refs(selected, base, branch, require_head=True)
        base_ref = f"refs/heads/{base}"
        head_ref = f"refs/heads/{branch}"
        base_sha = refs[base_ref]
        if refs[head_ref] != expected_head:
            raise _MergeRefusal(
                EXIT_HEAD_CHANGED,
                f"remote source changed from submitted {expected_head} to {refs[head_ref]}",
            )
        if _is_ancestor(selected, base_sha, expected_head):
            return 0, expected_head

        _git(
            selected,
            "fetch",
            "--no-tags",
            "--no-write-fetch-head",
            "origin",
            base_sha,
            status=EXIT_STALE_MAIN,
            operation=f"cannot fetch exact target {base} {base_sha}",
        )
        merge_env = dict(os.environ)
        for field, default in (("name", "AGCoord"), ("email", "agcoord@localhost")):
            configured = _run(
                ["git", "-C", str(selected), "config", "--get", f"user.{field}"],
                checkout=selected,
            )
            if configured.returncode not in (0, 1):
                raise _MergeRefusal(
                    EXIT_MERGE_ERROR,
                    f"cannot read Git user.{field}: {configured.stderr.strip()}",
                )
            for role in ("AUTHOR", "COMMITTER"):
                key = f"GIT_{role}_{field.upper()}"
                if not merge_env.get(key):
                    if configured.stdout.strip():
                        merge_env.pop(key, None)
                    else:
                        merge_env[key] = default
        merge = _run(
            [
                "git",
                "-C",
                str(selected),
                "merge",
                "--no-ff",
                "--no-gpg-sign",
                "-m",
                f"Merge {base} into {branch} for coordinated landing",
                base_sha,
            ],
            checkout=selected,
            env=merge_env,
        )
        if merge.returncode != 0:
            conflicts = _run(
                [
                    "git",
                    "-C",
                    str(selected),
                    "diff",
                    "--name-only",
                    "--diff-filter=U",
                    "-z",
                ],
                checkout=selected,
            )
            conflict_paths = sorted(
                path for path in conflicts.stdout.split("\0") if path
            )
            aborted = _run(
                ["git", "-C", str(selected), "merge", "--abort"],
                checkout=selected,
            )
            restored_head = _run(
                ["git", "-C", str(selected), "rev-parse", "HEAD"],
                checkout=selected,
            )
            restored_status = _run(
                [
                    "git",
                    "-C",
                    str(selected),
                    "status",
                    "--porcelain=v1",
                    "--untracked-files=all",
                ],
                checkout=selected,
            )
            if (
                aborted.returncode != 0
                or restored_head.returncode != 0
                or restored_head.stdout.strip() != expected_head
                or restored_status.returncode != 0
                or restored_status.stdout
            ):
                raise _MergeRefusal(
                    EXIT_MERGE_ERROR,
                    "target merge failed and the checkout could not be restored cleanly",
                )
            detail = merge.stderr.strip() or merge.stdout.strip() or "merge failed"
            if conflict_paths:
                raise _MergeRefusal(
                    EXIT_MERGE_ERROR,
                    "target merge conflicts in: "
                    + ", ".join(repr(path) for path in conflict_paths),
                )
            raise _MergeRefusal(EXIT_MERGE_ERROR, f"target merge failed: {detail}")

        merge_head = _sha(
            _git(
                selected,
                "rev-parse",
                "HEAD",
                operation="cannot read synchronized source head",
            ),
            field="synchronized source head",
        )
        parents = _git(
            selected,
            "show",
            "-s",
            "--format=%P",
            merge_head,
            operation="cannot inspect synchronized source parents",
        ).split()
        synchronized_status = _git(
            selected,
            "status",
            "--porcelain=v1",
            "--untracked-files=all",
            operation="cannot inspect synchronized source checkout",
        )
        if parents != [expected_head, base_sha] or synchronized_status:
            _restore_completed_merge(selected, expected_head, merge_head)
            raise _MergeRefusal(
                EXIT_MERGE_ERROR,
                "target synchronization did not create one clean ordered merge commit",
            )

        for avoided, reason in (avoid_commits or {}).items():
            if _is_ancestor(selected, avoided, merge_head):
                _restore_completed_merge(selected, expected_head, merge_head)
                raise _MergeRefusal(
                    EXIT_AVOIDED,
                    f"synchronized head {merge_head} would reach avoided commit "
                    f"{avoided} ({reason}); rebuild the request as a fresh branch from "
                    f"the current {base} and rerun the full gate",
                )
        before_push = _remote_refs(selected, base, branch, require_head=True)
        if before_push[head_ref] != expected_head:
            _restore_completed_merge(selected, expected_head, merge_head)
            raise _MergeRefusal(
                EXIT_HEAD_CHANGED,
                f"remote source changed from submitted {expected_head} "
                f"to {before_push[head_ref]}",
            )
        if before_push[base_ref] != base_sha:
            _restore_completed_merge(selected, expected_head, merge_head)
            raise _MergeRefusal(
                EXIT_STALE_MAIN,
                f"remote {base} advanced during target synchronization",
            )

        pushed = _run(
            [
                "git",
                "-C",
                str(selected),
                "push",
                f"--force-with-lease={head_ref}:{expected_head}",
                "origin",
                f"{merge_head}:{head_ref}",
            ],
            checkout=selected,
        )
        if pushed.returncode != 0:
            after = _remote_refs(selected, base, branch, require_head=False)
            if after.get(head_ref) != merge_head:
                _restore_completed_merge(selected, expected_head, merge_head)
                if after.get(head_ref) != expected_head:
                    changed = after.get(head_ref, "deleted")
                    raise _MergeRefusal(
                        EXIT_HEAD_CHANGED,
                        f"remote source changed from submitted {expected_head} to {changed}",
                    )
                detail = pushed.stderr.strip() or pushed.stdout.strip() or "push failed"
                raise _MergeRefusal(
                    EXIT_PUBLISH_FAILED,
                    f"cannot push synchronized source branch: {detail}",
                )
        print(
            f"Land coordinator: merged {base} {base_sha} into {branch}\n"
            f"  previous head: {expected_head}\n"
            f"  synchronized head: {merge_head}",
            file=out,
            flush=True,
        )
        return 0, merge_head
    except _MergeRefusal as exc:
        reason = FAILURE_REASONS[exc.status]
        print(f"Merge coordinator: REFUSED ({reason}) — {exc}", file=err, flush=True)
        return exc.status, expected_head


def execute(
    pull_request: int,
    *,
    checkout: str,
    branch: str,
    head_sha: str,
    metadata_client: PullRequestMetadataClient,
    publisher: MergePublisher,
    out: TextIO,
    err: TextIO,
    replaced_head: str | None = None,
    wait: Wait | None = None,
    allow_detached: bool = False,
) -> int:
    """Validate and conditionally publish one exact gated head.

    All expected values are copied into the durable merge row before this process starts.
    The forge's atomic before-OID checks are the final authority, so a race cannot turn
    stale evidence into a successful merge. ``replaced_head`` names the head that the
    coordinator's own target-sync push replaced, so a forge that still reports it is
    re-read for a bounded wait instead of being refused outright.
    """
    try:
        if (
            not isinstance(pull_request, int)
            or isinstance(pull_request, bool)
            or pull_request <= 0
        ):
            raise _MergeRefusal(EXIT_MERGE_ERROR, "pull request must be a positive integer")
        selected = Path(checkout).expanduser().resolve()
        if not selected.is_dir():
            raise _MergeRefusal(
                EXIT_MERGE_ERROR,
                f"merge checkout does not exist: {selected}",
            )
        if not isinstance(branch, str) or not branch:
            raise _MergeRefusal(EXIT_MERGE_ERROR, "merge branch must be non-empty")
        checked_ref = _run(
            ["git", "check-ref-format", "--branch", branch],
            checkout=selected,
        )
        if checked_ref.returncode != 0:
            raise _MergeRefusal(EXIT_MERGE_ERROR, f"invalid merge branch {branch!r}")
        expected_head = _sha(head_sha, field="gated head")
        superseded_head = (
            None if replaced_head is None else _sha(replaced_head, field="replaced head")
        )
        if superseded_head == expected_head:
            raise _MergeRefusal(
                EXIT_MERGE_ERROR,
                "replaced head must differ from the gated head",
            )

        repository_root = _git(
            selected,
            "rev-parse",
            "--show-toplevel",
            operation="cannot identify merge checkout",
        )
        if Path(repository_root).resolve() != selected:
            raise _MergeRefusal(
                EXIT_MERGE_ERROR,
                "merge checkout must be the repository root",
            )
        current_branch = _git(
            selected,
            "rev-parse",
            "--abbrev-ref",
            "HEAD",
            operation="merge checkout must be on the receipted branch and head",
        )
        if current_branch != branch and not (allow_detached and current_branch == "HEAD"):
            raise _MergeRefusal(
                EXIT_MERGE_ERROR,
                f"merge checkout branch is {current_branch!r}, expected {branch!r}",
            )
        local_head = _sha(
            _git(selected, "rev-parse", "HEAD", operation="cannot read local merge head"),
            field="local head",
        )
        if local_head != expected_head:
            raise _MergeRefusal(
                EXIT_MERGE_ERROR,
                f"local head changed from gated {expected_head} to {local_head}",
            )
        dirty = _git(
            selected,
            "status",
            "--porcelain=v1",
            "--untracked-files=all",
            operation="cannot inspect merge checkout",
        )
        if dirty:
            raise _MergeRefusal(
                EXIT_MERGE_ERROR,
                "merge checkout is not clean; commit or remove local changes before merging",
            )

        pr = _read_pull_request(
            metadata_client,
            pull_request,
            expected_head=expected_head,
            replaced_head=superseded_head,
            wait=wait or time.sleep,
            out=out,
        )
        if not pr["same_repository"]:
            raise _MergeRefusal(
                EXIT_PR_NOT_READY,
                "coordinated merge requires a branch in the same repository",
            )
        base = str(pr["base_ref"])
        checked_base = _run(
            ["git", "check-ref-format", "--branch", base],
            checkout=selected,
        )
        if checked_base.returncode != 0:
            raise _MergeRefusal(
                EXIT_PR_NOT_READY,
                f"pull request targets invalid base branch {base!r}",
            )
        if pr["head_ref"] != branch:
            raise _MergeRefusal(
                EXIT_PR_NOT_READY,
                f"pull-request branch is {pr['head_ref']!r}, expected {branch!r}",
            )
        if pr["head_sha"] == superseded_head:
            raise _MergeRefusal(
                EXIT_HEAD_CHANGED,
                f"pull-request head still reports replaced {superseded_head} instead of "
                f"gated {expected_head} after waiting {PROPAGATION_WAIT_SECONDS:g}s for "
                "forge propagation",
            )
        if pr["head_sha"] != expected_head:
            raise _MergeRefusal(
                EXIT_HEAD_CHANGED,
                f"pull-request head changed from gated {expected_head} to {pr['head_sha']}",
            )
        if pr["state"] == "OPEN" and pr["is_draft"]:
            raise _MergeRefusal(
                EXIT_PR_NOT_READY,
                f"pull request #{pull_request} is still a draft",
            )

        refs = _remote_refs(selected, base, branch, require_head=False)
        base_ref = f"refs/heads/{base}"
        head_ref = f"refs/heads/{branch}"
        base_sha = refs[base_ref]

        if pr["state"] == "MERGED":
            try:
                already_contains_head = publisher.is_ancestor(
                    ancestor_sha=expected_head,
                    descendant_sha=base_sha,
                )
            except _MergeRefusal:
                raise
            except Exception as exc:
                raise _MergeRefusal(
                    EXIT_MERGE_ERROR,
                    f"cannot verify merged pull request #{pull_request}: {exc}",
                ) from exc
            if already_contains_head:
                print(
                    f"Merge coordinator: PR #{pull_request} already contains gated "
                    f"head {expected_head} in {base} {base_sha}",
                    file=out,
                )
                return 0
            raise _MergeRefusal(
                EXIT_PR_NOT_READY,
                f"pull request #{pull_request} says merged, but gated head "
                f"{expected_head} is not in remote {base} {base_sha}",
            )

        remote_head = refs.get(head_ref)
        base_is_in_head = _is_ancestor(selected, base_sha, expected_head)
        if remote_head != expected_head or not base_is_in_head:
            try:
                already_contains_head = publisher.is_ancestor(
                    ancestor_sha=expected_head,
                    descendant_sha=base_sha,
                )
            except _MergeRefusal:
                raise
            except Exception as exc:
                raise _MergeRefusal(
                    EXIT_MERGE_ERROR,
                    f"cannot check whether remote {base} already contains the gated head: {exc}",
                ) from exc
            if already_contains_head:
                print(
                    f"Merge coordinator: PR #{pull_request} already contains gated "
                    f"head {expected_head} in {base} {base_sha}",
                    file=out,
                )
                return 0
        if remote_head != expected_head:
            changed_head = remote_head or "deleted"
            raise _MergeRefusal(
                EXIT_HEAD_CHANGED,
                f"remote pull-request head changed from gated {expected_head} "
                f"to {changed_head}",
            )

        if not base_is_in_head:
            raise _MergeRefusal(
                EXIT_STALE_MAIN,
                f"remote {base} {base_sha} is not an ancestor of gated head "
                f"{expected_head}; update branch {branch}, push it, and rerun the full gate",
            )

        tree_sha = _sha(
            _git(
                selected,
                "rev-parse",
                f"{expected_head}^{{tree}}",
                operation="cannot read the gated tree",
            ),
            field="gated tree",
        )
        subject = f"Merge pull request #{pull_request} from {pr['head_owner']}/{branch}"
        message = f"{subject}\n\n{pr['title']}\n"
        try:
            created_record = publisher.create_merge_commit(
                message=message,
                tree_sha=tree_sha,
                parent_shas=(base_sha, expected_head),
            )
        except _MergeRefusal:
            raise
        except Exception as exc:
            raise _MergeRefusal(
                EXIT_PUBLISH_FAILED,
                f"cannot create merge candidate on the forge: {exc}",
            ) from exc
        created = _created_commit(created_record)
        candidate = str(created["sha"])
        if (
            created["parent_shas"] != (base_sha, expected_head)
            or created["tree_sha"] != tree_sha
        ):
            raise _MergeRefusal(
                EXIT_MERGE_ERROR,
                "merge candidate does not exactly preserve the gated tree and parents",
            )

        updates = (
            {
                "name": base_ref,
                "before_oid": base_sha,
                "after_oid": candidate,
                "force": False,
            },
            {
                "name": head_ref,
                "before_oid": expected_head,
                "after_oid": expected_head,
                "force": False,
            },
        )
        try:
            publisher.update_refs(updates)
        except Exception as exc:
            # The authenticated API can fail after accepting a transaction (for example,
            # while its response is being returned). The GitHub publisher already reconciles
            # and boundedly retries transient failures, so an error that reaches here is a
            # refusal or an exhausted retry budget. Re-observe only on an error: the exact
            # candidate on main proves this transaction landed, while any other movement is
            # a typed handback. A successful atomic API response is itself the publication
            # authority; a later branch cleanup or new push does not revoke that success.
            after = _remote_refs(selected, base, branch, require_head=False)
            if after[base_ref] == candidate:
                pass
            elif after[base_ref] != base_sha:
                raise _MergeRefusal(
                    EXIT_STALE_MAIN,
                    f"remote {base} advanced from {base_sha} to {after[base_ref]}; "
                    f"update branch {branch}, push it, and rerun the full gate",
                ) from exc
            elif after.get(head_ref) != expected_head:
                changed_head = after.get(head_ref, "deleted")
                raise _MergeRefusal(
                    EXIT_HEAD_CHANGED,
                    f"remote pull-request head changed from gated {expected_head} "
                    f"to {changed_head}; rerun the full gate for the new head",
                ) from exc
            else:
                raise _MergeRefusal(
                    EXIT_PUBLISH_FAILED,
                    f"atomic merge publication failed: {exc}",
                ) from exc
        print(
            f"Merge coordinator: LANDED PR #{pull_request} as {candidate}\n"
            f"  gated head: {expected_head}\n"
            f"  previous {base}: {base_sha}",
            file=out,
        )
        return 0
    except _MergeRefusal as exc:
        reason = FAILURE_REASONS[exc.status]
        print(f"Merge coordinator: REFUSED ({reason}) — {exc}", file=err)
        return exc.status


def preflight(
    pull_request: int,
    *,
    checkout: str,
    branch: str,
    head_sha: str,
    metadata_client: PullRequestMetadataClient,
    publisher: MergePublisher,
    out: TextIO,
    err: TextIO,
    replaced_head: str | None = None,
    wait: Wait | None = None,
    allow_detached: bool = False,
) -> int:
    """Run exact publication validation without creating a forge object.

    The validation-only publisher delegates ancestry observations but stops execution at
    the first mutation boundary. This deliberately reuses the publication path so stale
    checks cannot drift from the checks repeated after a green gate.
    """
    try:
        status = execute(
            pull_request,
            checkout=checkout,
            allow_detached=allow_detached,
            branch=branch,
            head_sha=head_sha,
            metadata_client=metadata_client,
            publisher=_PreflightPublisher(publisher),
            out=out,
            err=err,
            replaced_head=replaced_head,
            wait=wait,
        )
    except _PreflightComplete:
        print(
            f"Land coordinator: preflight passed for request {pull_request} at {head_sha}",
            file=out,
        )
        return 0
    return status


def prepare(
    pull_request: int,
    *,
    checkout: str,
    branch: str,
    head_sha: str,
    metadata_client: PullRequestMetadataClient,
    publisher: MergePublisher,
    out: TextIO,
    err: TextIO,
    synchronize_target: bool = True,
    head_changed: Callable[[str, str], None] | None = None,
    wait: Wait | None = None,
    avoid_commits: Mapping[str, str] | None = None,
    allow_detached: bool = False,
) -> tuple[int, str]:
    """Reach one exact preflight head, synchronizing an advanced target by default."""
    effective_head = _sha(head_sha, field="submitted head")
    changed = head_changed or (lambda _old, _new: None)
    replaced_head: str | None = None
    for attempt in range(5):
        captured_out = StringIO()
        captured_err = StringIO()
        status = preflight(
            pull_request,
            checkout=checkout,
            allow_detached=allow_detached,
            branch=branch,
            head_sha=effective_head,
            metadata_client=metadata_client,
            publisher=publisher,
            out=captured_out,
            err=captured_err,
            replaced_head=replaced_head,
            wait=wait,
        )
        if status == 0:
            _copy_output(captured_out, out)
            _copy_output(captured_err, err)
            return 0, effective_head
        if status != EXIT_STALE_MAIN or not synchronize_target:
            _copy_output(captured_out, out)
            _copy_output(captured_err, err)
            return status, effective_head
        if attempt == 4:
            break

        status, synchronized_head = _synchronize_target(
            pull_request,
            checkout=checkout,
            allow_detached=allow_detached,
            branch=branch,
            head_sha=effective_head,
            metadata_client=metadata_client,
            out=out,
            err=err,
            avoid_commits=avoid_commits,
        )
        if status != 0:
            return status, effective_head
        if synchronized_head == effective_head:
            continue
        try:
            changed(effective_head, synchronized_head)
        except Exception as exc:
            print(
                "Merge coordinator: REFUSED (merge-error) — synchronized source was "
                f"pushed but its durable head could not be updated: {exc}",
                file=err,
                flush=True,
            )
            return EXIT_MERGE_ERROR, effective_head
        replaced_head = effective_head
        effective_head = synchronized_head

    print(
        "Merge coordinator: REFUSED (stale-main) — target kept advancing during "
        "bounded source synchronization",
        file=err,
        flush=True,
    )
    return EXIT_STALE_MAIN, effective_head


def _subprocess_env(env: Mapping[str, str] | None) -> dict[str, str] | None:
    if env is None:
        return None
    normalized = dict(env)
    if "PATH" not in normalized and "PATH" in os.environ:
        normalized["PATH"] = os.environ["PATH"]
    return normalized


def _has_commit(
    checkout: Path,
    commit_sha: str,
    *,
    env: Mapping[str, str] | None = None,
) -> bool:
    result = _run(
        ["git", "-C", str(checkout), "cat-file", "-e", f"{commit_sha}^{{commit}}"],
        checkout=checkout,
        env=env,
    )
    return result.returncode == 0


def verify_submission_head(
    pull_request: int,
    *,
    checkout: str | os.PathLike[str],
    branch: str,
    head_sha: str,
    metadata_client: PullRequestMetadataClient | None = None,
    publisher: MergePublisher | None = None,
    wait: Wait | None = None,
    env: Mapping[str, str] | None = None,
) -> None:
    """Verify before queueing that the submitted head still matches the pull request's head."""
    submitted_head = _sha(head_sha, field="submitted head")
    selected = Path(checkout).expanduser().resolve()
    subproc_env = _subprocess_env(env)
    client = metadata_client or GitHubMetadataClient(selected, env=subproc_env)
    merge_publisher = publisher or GitHubMergePublisher(selected, env=subproc_env)
    sleeper = wait or time.sleep

    def read_with_retry(action: Callable[[], Any], *, subject: str) -> Any:
        last_error: BaseException | None = None
        for attempt in range(len(SUBMISSION_RETRY_DELAYS) + 1):
            try:
                return action()
            except _MergeRefusal as exc:
                if exc.status == EXIT_PR_NOT_READY:
                    raise SubmissionRefusal(str(exc), code="pr-not-ready") from exc
                last_error = exc
            except Exception as exc:
                last_error = exc
            if attempt < len(SUBMISSION_RETRY_DELAYS):
                sleeper(SUBMISSION_RETRY_DELAYS[attempt])
        raise SubmissionRefusal(
            f"{subject}: {last_error}; retry once GitHub is reachable",
            code="forge-unavailable",
        ) from last_error

    record = read_with_retry(
        lambda: _metadata(client.pull_request(pull_request), pull_request),
        subject=f"cannot read pull request #{pull_request} before queueing",
    )
    if not record["same_repository"]:
        raise SubmissionRefusal(
            "coordinated land requires a branch in the same repository",
            code="pr-not-ready",
        )
    pr_branch = str(record["head_ref"])
    if pr_branch != branch:
        raise SubmissionRefusal(
            f"pull-request branch is {pr_branch!r}, expected {branch!r}",
            code="pr-not-ready",
        )
    remote_head = str(record["head_sha"])
    if remote_head == submitted_head:
        return

    if not _has_commit(selected, remote_head, env=subproc_env):
        fetched = _run(
            [
                "git",
                "-C",
                str(selected),
                "fetch",
                "--no-tags",
                "--no-write-fetch-head",
                "origin",
                remote_head,
            ],
            checkout=selected,
            env=subproc_env,
        )
        if fetched.returncode != 0:
            _run(
                [
                    "git",
                    "-C",
                    str(selected),
                    "fetch",
                    "--no-tags",
                    "--no-write-fetch-head",
                    "origin",
                    f"refs/heads/{pr_branch}",
                ],
                checkout=selected,
                env=subproc_env,
            )

    if _has_commit(selected, remote_head, env=subproc_env):
        behind = _is_ancestor(selected, submitted_head, remote_head)
    else:
        behind = bool(
            read_with_retry(
                lambda: merge_publisher.is_ancestor(
                    ancestor_sha=submitted_head,
                    descendant_sha=remote_head,
                ),
                subject=f"cannot compare pull request #{pull_request} heads {submitted_head} and {remote_head}",
            )
        )

    if behind:
        raise SubmissionRefusal(
            f"submitted head {submitted_head} is behind pull request #{pull_request} head "
            f"{remote_head}; run `git fetch && git merge --ff-only origin/{pr_branch}` and resubmit",
            code="submitted-head-behind",
        )
    raise SubmissionRefusal(
        f"submitted head {submitted_head} has diverged from pull request #{pull_request} head "
        f"{remote_head}; reconcile with `git fetch`, merge or push origin/{pr_branch}, and resubmit",
        code="submitted-head-diverged",
    )


class GitHubMetadataClient:
    """Normalize the installed GitHub CLI's external record into the strict worker seam."""

    def __init__(
        self,
        checkout: str | os.PathLike[str],
        *,
        env: Mapping[str, str] | None = None,
    ):
        self.checkout = Path(checkout).expanduser().resolve()
        self.env = dict(env) if env is not None else None

    def pull_request(self, number: int) -> dict[str, object]:
        fields = (
            "number,state,isDraft,baseRefName,headRefName,headRefOid,"
            "isCrossRepository,title,headRepositoryOwner"
        )
        result = _run(
            ["gh", "pr", "view", str(number), "--json", fields],
            checkout=self.checkout,
            env=self.env,
        )
        if result.returncode != 0:
            detail = result.stderr.strip() or result.stdout.strip() or f"exit {result.returncode}"
            raise RuntimeError(detail)
        try:
            raw: Any = json.loads(result.stdout)
            owner = raw["headRepositoryOwner"]
            return {
                "number": raw["number"],
                "state": raw["state"],
                "is_draft": raw["isDraft"],
                "base_ref": raw["baseRefName"],
                "head_ref": raw["headRefName"],
                "head_sha": raw["headRefOid"],
                "same_repository": not raw["isCrossRepository"],
                "title": raw["title"],
                "head_owner": owner["login"],
            }
        except (KeyError, TypeError, json.JSONDecodeError) as exc:
            raise RuntimeError("GitHub returned malformed pull-request metadata") from exc


class GitHubMergePublisher:
    """Create the candidate object, then compare/update both GitHub refs atomically.

    A transient GitHub failure (see :func:`_is_transient_failure`) is retried inside the
    caller's landing lane after each ``retry_delays`` wait, so ``len(retry_delays) + 1``
    attempts at most. Creating a commit, blob, or tree is content-addressed and moves no ref,
    so those requests are simply repeated. The atomic ref transaction may have been applied
    even though its response was lost, so after a transient failure the remote refs are read
    first: refs already at their new values mean the transaction was published, refs still at
    their old values are retried, and any other value is a compare-and-swap mismatch that fails
    at once. A permanent refusal is never retried. ``wait`` and ``notify`` are injectable so
    tests neither sleep nor print.
    """

    def __init__(
        self,
        checkout: str | os.PathLike[str],
        *,
        env: Mapping[str, str] | None = None,
        wait: Wait | None = None,
        retry_delays: tuple[float, ...] = PUBLICATION_RETRY_DELAYS,
        notify: Callable[[str], None] | None = None,
    ):
        self.checkout = Path(checkout).expanduser().resolve()
        self.env = dict(env) if env is not None else None
        self.wait: Wait = wait or time.sleep
        self.retry_delays = tuple(retry_delays)
        self.notify: Callable[[str], None] = notify or _notify_stderr
        self._repository_value: tuple[str, str] | None = None
        self._commit_trees: dict[str, str] = {}

    def _invoke(self, arguments: list[str], payload: dict[str, object]) -> object:
        result = _run(
            ["gh", *arguments, "--input", "-"],
            checkout=self.checkout,
            input_text=json.dumps(payload, separators=(",", ":")),
            env=self.env,
        )
        if result.returncode != 0:
            raise _gh_failure(result)
        try:
            return json.loads(result.stdout)
        except json.JSONDecodeError as exc:
            raise RuntimeError("GitHub returned malformed JSON") from exc

    def _retrying(self, action: Callable[[], _T], *, subject: str) -> _T:
        """Repeat a side-effect-free request while GitHub fails it transiently."""
        attempts = 0
        while True:
            attempts += 1
            try:
                return action()
            except _TransientForgeError as exc:
                if attempts > len(self.retry_delays):
                    raise _PublicationRetriesExhausted(
                        f"{subject} still failing after {_attempts(attempts)}: {exc}"
                    ) from exc
                delay = self.retry_delays[attempts - 1]
                self.notify(
                    f"transient GitHub error during {subject} (attempt {attempts} of "
                    f"{len(self.retry_delays) + 1}): {exc}; retrying in {delay:g}s"
                )
                self.wait(delay)

    def _invoke_retrying(
        self, arguments: list[str], payload: dict[str, object], *, subject: str
    ) -> object:
        return self._retrying(lambda: self._invoke(arguments, payload), subject=subject)

    def _repository(self, *, retry: bool = False) -> tuple[str, str]:
        if self._repository_value is not None:
            return self._repository_value
        if retry:
            return self._retrying(self._resolve_repository, subject="repository lookup")
        return self._resolve_repository()

    def _resolve_repository(self) -> tuple[str, str]:
        result = _run(
            ["gh", "repo", "view", "--json", "id,nameWithOwner"],
            checkout=self.checkout,
            env=self.env,
        )
        if result.returncode != 0:
            raise _gh_failure(result)
        try:
            raw: Any = json.loads(result.stdout)
            repository_id = raw["id"]
            name_with_owner = raw["nameWithOwner"]
        except (KeyError, TypeError, json.JSONDecodeError) as exc:
            raise RuntimeError("GitHub returned malformed repository metadata") from exc
        if not isinstance(repository_id, str) or not repository_id:
            raise RuntimeError("GitHub returned no repository node ID")
        if (
            not isinstance(name_with_owner, str)
            or name_with_owner.count("/") != 1
            or any(not part for part in name_with_owner.split("/"))
        ):
            raise RuntimeError("GitHub returned an invalid repository name")
        self._repository_value = (repository_id, name_with_owner)
        return self._repository_value

    def _ensure_tree_on_remote(
        self,
        name_with_owner: str,
        *,
        tree_sha: str,
        base_commit: str,
    ) -> None:
        base_tree = self._commit_trees.get(base_commit) or _git(
            self.checkout,
            "rev-parse",
            f"{base_commit}^{{tree}}",
            operation="resolve base tree",
        )
        raw_diff = subprocess.run(
            [
                "git",
                "-C",
                str(self.checkout),
                "diff-tree",
                "-r",
                "-z",
                "--no-abbrev",
                base_tree,
                tree_sha,
            ],
            capture_output=True,
            check=False,
        )
        if raw_diff.returncode != 0:
            detail = raw_diff.stderr.decode(errors="replace").strip()
            raise RuntimeError(f"cannot inspect tree difference: {detail}")
        tokens = [token for token in raw_diff.stdout.split(b"\0") if token]
        if not tokens:
            return
        tree_entries: list[dict[str, object]] = []
        idx = 0
        while idx + 1 < len(tokens):
            header = tokens[idx].decode("utf-8", errors="strict")
            path = tokens[idx + 1].decode("utf-8", errors="strict")
            idx += 2
            # Format: :<old_mode> <new_mode> <old_sha> <new_sha> <status>
            parts = header.lstrip(":").split()
            if len(parts) < 5:
                raise RuntimeError(f"unexpected diff-tree header: {header!r}")
            old_mode, new_mode, _old_sha, new_sha, status = parts[:5]
            if status.startswith("D"):
                tree_entries.append(
                    {"path": path, "mode": old_mode, "type": "blob", "sha": None}
                )
            elif new_mode == "160000":
                tree_entries.append(
                    {"path": path, "mode": "160000", "type": "commit", "sha": new_sha}
                )
            else:
                blob_res = subprocess.run(
                    ["git", "-C", str(self.checkout), "cat-file", "blob", new_sha],
                    capture_output=True,
                    check=False,
                )
                if blob_res.returncode != 0:
                    raise RuntimeError(f"cannot read blob {new_sha} for {path}")
                blob_resp = self._invoke_retrying(
                    ["api", "--method", "POST", f"repos/{name_with_owner}/git/blobs"],
                    {
                        "content": base64.b64encode(blob_res.stdout).decode("ascii"),
                        "encoding": "base64",
                    },
                    subject="blob upload",
                )
                if not isinstance(blob_resp, dict) or not isinstance(blob_resp.get("sha"), str):
                    raise RuntimeError("GitHub returned a malformed blob response")
                tree_entries.append(
                    {
                        "path": path,
                        "mode": new_mode,
                        "type": "blob",
                        "sha": blob_resp["sha"],
                    }
                )
        created = self._invoke_retrying(
            ["api", "--method", "POST", f"repos/{name_with_owner}/git/trees"],
            {"base_tree": base_tree, "tree": tree_entries},
            subject="tree upload",
        )
        if not isinstance(created, dict) or created.get("sha") != tree_sha:
            raise RuntimeError("GitHub tree upload did not match the expected gated tree")

    def create_merge_commit(
        self,
        *,
        message: str,
        tree_sha: str,
        parent_shas: tuple[str, str],
    ) -> dict[str, object]:
        if not isinstance(message, str) or not message.strip():
            raise RuntimeError("merge commit message must be non-empty")
        selected_tree = _sha(tree_sha, field="merge candidate tree")
        if not isinstance(parent_shas, tuple) or len(parent_shas) != 2:
            raise RuntimeError("merge candidate requires two ordered parents")
        selected_parents = tuple(
            _sha(parent, field="merge candidate parent") for parent in parent_shas
        )
        _repository_id, name_with_owner = self._repository(retry=True)
        commit_payload = {
            "message": message,
            "tree": selected_tree,
            "parents": list(selected_parents),
        }
        endpoint = ["api", "--method", "POST", f"repos/{name_with_owner}/git/commits"]
        subject = "merge commit creation"
        try:
            raw = self._invoke_retrying(endpoint, commit_payload, subject=subject)
        except _PublicationRetriesExhausted:
            raise
        except RuntimeError as first_error:
            if "tree" not in str(first_error).lower() or not self.checkout.exists():
                raise
            self._ensure_tree_on_remote(
                name_with_owner,
                tree_sha=selected_tree,
                base_commit=selected_parents[0],
            )
            raw = self._invoke_retrying(endpoint, commit_payload, subject=subject)
        if not isinstance(raw, dict):
            raise RuntimeError("GitHub returned a malformed merge commit")
        try:
            response_sha = raw["sha"]
            response_tree = raw["tree"]["sha"]
            response_parents = tuple(parent["sha"] for parent in raw["parents"])
        except (KeyError, TypeError) as exc:
            raise RuntimeError("GitHub returned a malformed merge commit") from exc
        if len(response_parents) != 2:
            raise RuntimeError("GitHub merge candidate does not have two parents")
        normalized_sha = _sha(response_sha, field="GitHub merge candidate")
        normalized_tree = _sha(response_tree, field="GitHub merge candidate tree")
        self._commit_trees[normalized_sha] = normalized_tree
        return {
            "sha": normalized_sha,
            "tree_sha": normalized_tree,
            "parent_shas": tuple(
                _sha(parent, field="GitHub merge candidate parent")
                for parent in response_parents
            ),
        }

    def update_refs(self, updates: tuple[dict[str, object], ...]) -> None:
        if not isinstance(updates, tuple) or len(updates) < 2:
            raise RuntimeError("atomic publication requires at least two ref updates")
        normalized: list[dict[str, object]] = []
        for update in updates:
            if not isinstance(update, dict) or set(update) != {
                "name", "before_oid", "after_oid", "force",
            }:
                raise RuntimeError("ref update does not match the publisher contract")
            name = update["name"]
            force = update["force"]
            if not isinstance(name, str) or not name.startswith("refs/heads/"):
                raise RuntimeError("publisher ref name must identify a branch")
            if not isinstance(force, bool):
                raise RuntimeError("publisher ref force flag must be boolean")
            normalized.append({
                "name": name,
                "beforeOid": _sha(update["before_oid"], field=f"{name} before OID"),
                "afterOid": _sha(update["after_oid"], field=f"{name} after OID"),
                "force": force,
            })

        repository_id, _name_with_owner = self._repository(retry=True)
        total_attempts = len(self.retry_delays) + 1
        attempts = 0
        retries = 0
        resend = True
        last_error: RuntimeError | None = None
        read_failure = ""
        while True:
            if resend:
                attempts += 1
                try:
                    self._send_update_refs(repository_id, normalized)
                    return
                except _TransientForgeError as exc:
                    last_error = exc
                except RuntimeError:
                    if attempts == 1:
                        raise
                    # An earlier attempt failed transiently and may still have been in flight
                    # when the refs were read, so this refusal can be GitHub reporting that
                    # earlier attempt's success as a before-OID mismatch.
                    if self._observe_refs(normalized)[0] != "published":
                        raise
                    self.notify(
                        "GitHub applied the earlier atomic ref update after its error was "
                        f"reported; remote refs are at their new values (after {_attempts(attempts)})"
                    )
                    return
            # GitHub may have applied the transaction before the failure reached us, so the
            # same request is only sent again once the remote refs prove it was not applied.
            verdict, detail = self._observe_refs(normalized)
            if verdict == "published":
                self.notify(
                    f"GitHub applied the atomic ref update before reporting {last_error}; "
                    f"remote refs are already at their new values, so it is not resent "
                    f"(after {_attempts(attempts)})"
                )
                return
            if verdict == "moved":
                raise RuntimeError(
                    f"atomic ref update failed after {_attempts(attempts)} ({last_error}) and "
                    f"the remote refs moved: {detail}; not retrying a compare-and-swap mismatch"
                ) from last_error
            resend = verdict == "unchanged"
            read_failure = "" if resend else detail
            if retries >= len(self.retry_delays):
                break
            delay = self.retry_delays[retries]
            retries += 1
            self.notify(
                f"transient GitHub error during atomic ref update (attempt {attempts} of "
                f"{total_attempts}): {last_error}; "
                + (
                    "remote refs unchanged"
                    if resend
                    else f"cannot read remote refs ({detail}), so it is not resent yet"
                )
                + f"; retrying in {delay:g}s"
            )
            self.wait(delay)
        if read_failure:
            raise _PublicationRetriesExhausted(
                f"cannot confirm whether GitHub applied the atomic ref update after "
                f"{_attempts(attempts)} (last error: {last_error}); the remote refs could "
                f"not be read ({read_failure}), so it was not resent"
            ) from last_error
        raise _PublicationRetriesExhausted(
            f"atomic ref update still failing after {_attempts(attempts)} with the remote "
            f"refs unchanged; last error: {last_error}"
        ) from last_error

    def _send_update_refs(self, repository_id: str, normalized: list[dict[str, object]]) -> None:
        """Send one atomic ref transaction exactly once and verify GitHub's acknowledgement."""
        mutation_id = f"agcoord-{uuid4().hex}"
        query = (
            "mutation UpdateRefs($input: UpdateRefsInput!) { "
            "updateRefs(input: $input) { clientMutationId } }"
        )
        raw = self._invoke(
            ["api", "graphql"],
            {
                "query": query,
                "variables": {
                    "input": {
                        "repositoryId": repository_id,
                        "clientMutationId": mutation_id,
                        "refUpdates": normalized,
                    }
                },
            },
        )
        if not isinstance(raw, dict):
            raise RuntimeError("GitHub returned a malformed atomic-ref response")
        messages = _graphql_error_messages(raw)
        if messages is not None:
            reason = f"GitHub rejected the atomic ref update: {'; '.join(messages)}"
            if any(_INTERNAL_GRAPHQL_ERROR.search(message) for message in messages):
                raise _TransientForgeError(reason)
            raise RuntimeError(reason)
        try:
            returned_id = raw["data"]["updateRefs"]["clientMutationId"]
        except (KeyError, TypeError) as exc:
            raise RuntimeError("GitHub returned a malformed atomic-ref response") from exc
        if returned_id != mutation_id:
            raise RuntimeError("GitHub returned the wrong atomic-ref mutation identity")

    def _observe_refs(self, updates: list[dict[str, object]]) -> tuple[str, str]:
        """Read the remote refs and say what became of an uncertain atomic transaction.

        Returns ``("published", "")`` when every ref that the transaction moves already holds
        its new value (only that transaction can have created the candidate, so later branch
        cleanup does not revoke it), ``("unchanged", "")`` when every ref still holds its
        guarded old value, ``("moved", detail)`` for any other value (a compare-and-swap
        mismatch), and ``("unreadable", detail)`` when the refs could not be read.
        """
        names = [str(update["name"]) for update in updates]
        try:
            result = _run(
                ["git", "-C", str(self.checkout), "ls-remote", "--refs", "origin", *names],
                checkout=self.checkout,
                env=self.env,
            )
        except _MergeRefusal as exc:
            return "unreadable", str(exc)
        if result.returncode != 0:
            detail = result.stderr.strip() or result.stdout.strip() or f"exit {result.returncode}"
            return "unreadable", f"git ls-remote failed: {detail}"
        current: dict[str, str] = {}
        for line in result.stdout.splitlines():
            fields = line.split()
            if len(fields) == 2 and fields[1] in names:
                current[fields[1]] = fields[0].lower()
        movers = [update for update in updates if update["beforeOid"] != update["afterOid"]]
        if movers and all(current.get(str(u["name"])) == u["afterOid"] for u in movers):
            return "published", ""
        changed = [u for u in updates if current.get(str(u["name"])) != u["beforeOid"]]
        if not changed:
            return "unchanged", ""
        return "moved", ", ".join(
            f"{update['name']} is {current.get(str(update['name']), 'absent')} "
            f"instead of {update['beforeOid']}"
            for update in changed
        )

    def is_ancestor(self, *, ancestor_sha: str, descendant_sha: str) -> bool:
        ancestor = _sha(ancestor_sha, field="ancestor commit")
        descendant = _sha(descendant_sha, field="descendant commit")
        _repository_id, name_with_owner = self._repository()
        result = _run(
            [
                "gh",
                "api",
                f"repos/{name_with_owner}/compare/{ancestor}...{descendant}",
            ],
            checkout=self.checkout,
            env=self.env,
        )
        if result.returncode != 0:
            detail = result.stderr.strip() or result.stdout.strip() or f"exit {result.returncode}"
            raise RuntimeError(detail)
        try:
            raw: Any = json.loads(result.stdout)
            merge_base = raw["merge_base_commit"]["sha"]
        except (KeyError, TypeError, json.JSONDecodeError) as exc:
            raise RuntimeError("GitHub returned a malformed commit comparison") from exc
        return _sha(merge_base, field="comparison merge base") == ancestor


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="agc github worker")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--state-dir")
    parser.add_argument("--checkout", required=True)
    parser.add_argument("--branch", required=True)
    parser.add_argument("--head-sha", required=True)
    parser.add_argument("pull_request", type=int)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    marker = os.environ.get(RUN_ID_ENV)
    if not args.run_id.startswith("merge-") or marker != args.run_id:
        print(
            "Merge coordinator: REFUSED (merge-error) — the merge worker requires "
            "its exact broker admission",
            file=sys.stderr,
        )
        return EXIT_MERGE_ERROR
    try:
        from .queue import CoordinatorError, CoordinatorClient

        CoordinatorClient(
            state_dir=args.state_dir,
            checkout=args.checkout,
            autostart=False,
        ).verify_admission(
            args.run_id,
            kind="merge",
            checkout=args.checkout,
            head_sha=args.head_sha,
            worker_pid=os.getpid(),
        )
    except CoordinatorError as exc:
        print(
            f"Merge coordinator: REFUSED (merge-error) — run {args.run_id!r} "
            f"has no exact broker admission: {exc}",
            file=sys.stderr,
        )
        return EXIT_MERGE_ERROR
    return execute(
        args.pull_request,
        checkout=args.checkout,
        branch=args.branch,
        head_sha=args.head_sha,
        metadata_client=GitHubMetadataClient(args.checkout),
        publisher=GitHubMergePublisher(args.checkout),
        out=sys.stdout,
        err=sys.stderr,
    )


if __name__ == "__main__":
    raise SystemExit(main())
