"""Publish one exact full-gated pull-request head through GitHub.

The worker never updates the ticket branch. It observes the local checkout and two remote
refs, asks the forge to create a merge commit whose tree is exactly the gated head, then
atomically compares and updates both refs. A changed base or head rejects the whole
publication.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Any, Protocol, TextIO
from uuid import uuid4


RUN_ID_ENV = "AGCOORD_RUN_ID"
EXIT_STALE_MAIN = 75
EXIT_HEAD_CHANGED = 76
EXIT_PR_NOT_READY = 77
EXIT_PUBLISH_FAILED = 78
EXIT_MERGE_ERROR = 79
FAILURE_REASONS = {
    EXIT_STALE_MAIN: "stale-main",
    EXIT_HEAD_CHANGED: "head-changed",
    EXIT_PR_NOT_READY: "pr-not-ready",
    EXIT_PUBLISH_FAILED: "publish-failed",
    EXIT_MERGE_ERROR: "merge-error",
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
) -> int:
    """Validate and conditionally publish one exact gated head.

    All expected values are copied into the durable merge row before this process starts.
    The forge's atomic before-OID checks are the final authority, so a race cannot turn
    stale evidence into a successful merge.
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
            "symbolic-ref",
            "--quiet",
            "--short",
            "HEAD",
            operation="merge checkout must be on the receipted branch and head",
        )
        if current_branch != branch:
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
            # while its response is being returned). Re-observe only on an error: the exact
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
            branch=branch,
            head_sha=head_sha,
            metadata_client=metadata_client,
            publisher=_PreflightPublisher(publisher),
            out=out,
            err=err,
        )
    except _PreflightComplete:
        print(
            f"Land coordinator: preflight passed for request {pull_request} at {head_sha}",
            file=out,
        )
        return 0
    return status


class GitHubMetadataClient:
    """Normalize the installed GitHub CLI's external record into the strict worker seam."""

    def __init__(self, checkout: str | os.PathLike[str]):
        self.checkout = Path(checkout).expanduser().resolve()

    def pull_request(self, number: int) -> dict[str, object]:
        fields = (
            "number,state,isDraft,baseRefName,headRefName,headRefOid,"
            "isCrossRepository,title,headRepositoryOwner"
        )
        result = _run(
            ["gh", "pr", "view", str(number), "--json", fields],
            checkout=self.checkout,
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
    """Create the candidate object, then compare/update both GitHub refs atomically."""

    def __init__(self, checkout: str | os.PathLike[str]):
        self.checkout = Path(checkout).expanduser().resolve()
        self._repository_value: tuple[str, str] | None = None

    def _invoke(self, arguments: list[str], payload: dict[str, object]) -> object:
        result = _run(
            ["gh", *arguments, "--input", "-"],
            checkout=self.checkout,
            input_text=json.dumps(payload, separators=(",", ":")),
        )
        if result.returncode != 0:
            detail = result.stderr.strip() or result.stdout.strip() or f"exit {result.returncode}"
            raise RuntimeError(detail)
        try:
            return json.loads(result.stdout)
        except json.JSONDecodeError as exc:
            raise RuntimeError("GitHub returned malformed JSON") from exc

    def _repository(self) -> tuple[str, str]:
        if self._repository_value is not None:
            return self._repository_value
        result = _run(
            ["gh", "repo", "view", "--json", "id,nameWithOwner"],
            checkout=self.checkout,
        )
        if result.returncode != 0:
            detail = result.stderr.strip() or result.stdout.strip() or f"exit {result.returncode}"
            raise RuntimeError(detail)
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
        _repository_id, name_with_owner = self._repository()
        raw = self._invoke(
            ["api", "--method", "POST", f"repos/{name_with_owner}/git/commits"],
            {
                "message": message,
                "tree": selected_tree,
                "parents": list(selected_parents),
            },
        )
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
        return {
            "sha": _sha(response_sha, field="GitHub merge candidate"),
            "tree_sha": _sha(response_tree, field="GitHub merge candidate tree"),
            "parent_shas": tuple(
                _sha(parent, field="GitHub merge candidate parent")
                for parent in response_parents
            ),
        }

    def update_refs(self, updates: tuple[dict[str, object], ...]) -> None:
        if not isinstance(updates, tuple) or len(updates) != 2:
            raise RuntimeError("atomic publication requires exactly two ref updates")
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

        repository_id, _name_with_owner = self._repository()
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
        if raw.get("errors"):
            raise RuntimeError("GitHub rejected the atomic ref update")
        try:
            returned_id = raw["data"]["updateRefs"]["clientMutationId"]
        except (KeyError, TypeError) as exc:
            raise RuntimeError("GitHub returned a malformed atomic-ref response") from exc
        if returned_id != mutation_id:
            raise RuntimeError("GitHub returned the wrong atomic-ref mutation identity")

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
    parser = argparse.ArgumentParser(prog="agcoord github worker")
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
