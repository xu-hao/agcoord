"""Prepare one exact head, gate it, and publish it as an indivisible job."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import errno
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import tempfile
from typing import Callable, Mapping, Sequence, TextIO

from .merge import (
    EXIT_AVOIDED,
    EXIT_HEAD_CHANGED,
    EXIT_MERGE_ERROR,
    EXIT_PR_NOT_READY,
    EXIT_PUBLISH_FAILED,
    EXIT_STALE_MAIN,
    MergePublisher,
    PullRequestMetadataClient,
    Wait,
    execute as publish,
    prepare,
)


PhaseChanged = Callable[[str, int | None], None]
HeadChanged = Callable[[str, str], None]
ReuseGate = Callable[[], str | None]


def _command(value: Sequence[str]) -> list[str]:
    selected = list(value)
    if (
        not selected
        or not isinstance(selected[0], str)
        or not selected[0]
        or "\0" in selected[0]
        or not all(
            isinstance(argument, str) and "\0" not in argument
            for argument in selected[1:]
        )
    ):
        raise ValueError(
            "gate command needs a non-empty executable and NUL-free string arguments"
        )
    return selected


def _environment(value: Mapping[str, str]) -> dict[str, str]:
    selected = dict(value)
    if not all(
        isinstance(name, str)
        and name
        and "=" not in name
        and "\0" not in name
        and isinstance(content, str)
        and "\0" not in content
        for name, content in selected.items()
    ):
        raise ValueError("gate environment must map valid string names to string values")
    return selected


def _shell_status(returncode: int) -> int:
    return 128 + abs(returncode) if returncode < 0 else returncode


def _run_gate(
    command: list[str],
    *,
    checkout: Path,
    environment: dict[str, str],
    out: TextIO,
) -> int:
    try:
        out.flush()
        try:
            out.fileno()
        except (AttributeError, OSError):
            completed = subprocess.run(
                command,
                cwd=checkout,
                env=environment,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                check=False,
            )
            out.write(completed.stdout)
            out.flush()
        else:
            completed = subprocess.run(
                command,
                cwd=checkout,
                env=environment,
                stdout=out,
                stderr=subprocess.STDOUT,
                text=True,
                check=False,
            )
    except OSError as exc:
        print(f"Land coordinator: could not execute gate: {exc}", file=out, flush=True)
        return 127
    return _shell_status(completed.returncode)


def execute(
    request: int,
    gate_command: Sequence[str],
    *,
    checkout: str,
    branch: str,
    head_sha: str,
    environment: Mapping[str, str],
    metadata_client: PullRequestMetadataClient,
    publisher: MergePublisher,
    out: TextIO,
    err: TextIO,
    phase_changed: PhaseChanged | None = None,
    head_changed: HeadChanged | None = None,
    synchronize_target: bool = True,
    wait: Wait | None = None,
    avoid_commits: Mapping[str, str] | None = None,
    reuse_gate: ReuseGate | None = None,
) -> int:
    """Preflight, gate, and publish without releasing the caller's reservation."""
    selected = Path(checkout).expanduser().resolve()
    command = _command(gate_command)
    selected_environment = _environment(environment)
    changed = phase_changed or (lambda _phase, _gate_status: None)
    retargeted = head_changed or (lambda _old_head, _new_head: None)

    changed("preflight", None)
    avoided = dict(avoid_commits or {})
    if avoided:
        print(
            "Land coordinator: avoiding "
            f"{len(avoided)} commit(s): "
            + ", ".join(f"{sha} ({reason})" for sha, reason in avoided.items()),
            file=out,
            flush=True,
        )
        refusal = _avoided_refusal(
            selected,
            request=request,
            metadata_client=metadata_client,
            heads={"request head": head_sha},
            avoided=avoided,
            out=out,
            err=err,
        )
        if refusal is not None:
            return refusal
    status, effective_head = prepare(
        request,
        checkout=str(selected),
        branch=branch,
        head_sha=head_sha,
        metadata_client=metadata_client,
        publisher=publisher,
        out=out,
        err=err,
        synchronize_target=synchronize_target,
        allow_detached=True,
        head_changed=retargeted,
        wait=wait,
        avoid_commits=avoided or None,
    )
    if status != 0:
        return status

    # The coordinator, not this worker, decides whether an existing receipt covers the
    # exact head preflight reached. A moved target already changed that head, so a
    # synchronized source can never match one.
    reused = reuse_gate() if reuse_gate is not None else None
    if reused is not None:
        print(
            f"Land coordinator: gate reused from {reused}; that receipt already gated "
            f"the exact head {effective_head}",
            file=out,
            flush=True,
        )
    else:
        changed("gating", None)
        print(f"Land coordinator: gate started for {effective_head}", file=out, flush=True)
        gate_status = _run_gate(
            command,
            checkout=selected,
            environment=selected_environment,
            out=out,
        )
        if gate_status != 0:
            changed("gating", gate_status)
            print(
                f"Land coordinator: gate failed with exit status {gate_status}; "
                "publication was not attempted",
                file=err,
                flush=True,
            )
            return gate_status

    if avoided:
        refusal = _avoided_refusal(
            selected,
            request=request,
            metadata_client=metadata_client,
            heads={"gated head": effective_head},
            avoided=avoided,
            out=out,
            err=err,
        )
        if refusal is not None:
            return refusal

    # The durable transition is also the cancellation boundary. Once it succeeds, the
    # coordinator must observe the authenticated publication through an exact result.
    changed("publishing", 0)
    print("Land coordinator: gate passed; publishing exact head", file=out, flush=True)
    return publish(
        request,
        checkout=str(selected),
        branch=branch,
        head_sha=effective_head,
        allow_detached=True,
        metadata_client=metadata_client,
        publisher=publisher,
        out=out,
        err=err,
    )


def _avoided_refusal(
    checkout: Path,
    *,
    request: int,
    metadata_client: PullRequestMetadataClient,
    heads: Mapping[str, str],
    avoided: Mapping[str, str],
    out: TextIO,
    err: TextIO,
) -> int | None:
    """Refuse when a named head or the current target reaches an avoided commit.

    The target is re-read from the remote every time, so a commit re-imported after an
    earlier check is still caught before publication.
    """
    from .avoid import reachable_avoided
    from .queue import CoordinatorError

    try:
        base = str(metadata_client.pull_request(request)["base_ref"])
        fetched = subprocess.run(
            ["git", "-C", str(checkout), "fetch", "--quiet", "origin", f"refs/heads/{base}"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
        if fetched.returncode != 0:
            detail = fetched.stderr.strip() or f"exit {fetched.returncode}"
            print(
                f"Land coordinator: REFUSED (merge-error) — cannot read target {base} "
                f"for the avoided-commit check: {detail}",
                file=err,
                flush=True,
            )
            return EXIT_MERGE_ERROR
        subjects = {**heads, f"current {base}": "FETCH_HEAD"}
        reported_unknown = False
        for description, revision in subjects.items():
            reachable, unknown = reachable_avoided(checkout, revision, avoided)
            if unknown and not reported_unknown:
                reported_unknown = True
                print(
                    "Land coordinator: avoided commit(s) unknown to this repository, "
                    "treated as unreachable: " + ", ".join(unknown),
                    file=out,
                    flush=True,
                )
            if reachable:
                sha = reachable[0]
                print(
                    f"Land coordinator: REFUSED (avoided-commit) — {description} reaches "
                    f"avoided commit {sha} ({avoided[sha]}); rebuild the request as a "
                    f"fresh branch from the current {base} and rerun the full gate",
                    file=err,
                    flush=True,
                )
                return EXIT_AVOIDED
    except (CoordinatorError, KeyError, TypeError) as exc:
        print(
            f"Land coordinator: REFUSED (merge-error) — avoided-commit check failed: {exc}",
            file=err,
            flush=True,
        )
        return EXIT_MERGE_ERROR
    return None


def _git_batch(checkout: Path, *arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", "-C", str(checkout), *arguments],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )


def _is_git_ancestor(checkout: Path, ancestor: str, descendant: str) -> bool:
    result = _git_batch(checkout, "merge-base", "--is-ancestor", ancestor, descendant)
    return result.returncode == 0


def _avoid_refusal_for_heads(
    checkout: Path,
    *,
    heads: Mapping[str, str],
    avoided: Mapping[str, str],
) -> tuple[str, str, str] | None:
    if not avoided:
        return None
    from .avoid import reachable_avoided

    for label, revision in heads.items():
        reachable, _unknown = reachable_avoided(checkout, revision, avoided)
        if reachable:
            sha = reachable[0]
            return label, sha, avoided[sha]
    return None


def _largest_power_of_two_le(value: int) -> int:
    if value <= 1:
        return 1
    return 1 << (value.bit_length() - 1)


def _largest_power_of_two_lt(value: int) -> int:
    if value <= 2:
        return 1
    return _largest_power_of_two_le(value - 1)


BATCH_ATTRIBUTION_FILE_ENV = "AGCOORD_BATCH_ATTRIBUTION_FILE"
ATTRIBUTION_FORMAT = "agcoord-failure-attribution-v1"
MAX_ATTRIBUTION_BYTES = 4 * 1024 * 1024
MAX_ATTRIBUTION_FAILURES = 10_000


def _validate_canonical_repo_path(value: object, *, checkout: Path) -> str | None:
    if (
        not isinstance(value, str)
        or not value
        or "\0" in value
        or "\\" in value
        or value.startswith("/")
        or value.endswith("/")
    ):
        return None
    segments = value.split("/")
    if any(not seg or seg in {".", ".."} for seg in segments):
        return None
    current = checkout
    for seg in segments:
        current = current / seg
        try:
            st = os.lstat(current)
        except OSError:
            break
        if stat.S_ISLNK(st.st_mode):
            return None
    return value


def _load_failure_attribution(
    path: Path,
    *,
    checkout: Path,
    err: TextIO,
) -> list[tuple[str, tuple[str, ...]]] | None:
    if os.path.islink(path.parent):
        print(
            "Land coordinator: ignoring failure attribution receipt inside symlinked parent",
            file=err,
            flush=True,
        )
        return None
    try:
        st = os.lstat(path)
    except FileNotFoundError:
        return None
    except OSError as exc:
        print(
            f"Land coordinator: ignoring unreadable failure attribution receipt: {exc}",
            file=err,
            flush=True,
        )
        return None
    if stat.S_ISLNK(st.st_mode) or not stat.S_ISREG(st.st_mode):
        print(
            "Land coordinator: ignoring non-regular or symlinked failure attribution receipt",
            file=err,
            flush=True,
        )
        return None
    if st.st_size == 0:
        return None
    if st.st_size > MAX_ATTRIBUTION_BYTES:
        print(
            f"Land coordinator: ignoring oversized failure attribution receipt ({st.st_size} bytes)",
            file=err,
            flush=True,
        )
        return None

    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
    try:
        fd = os.open(path, flags)
    except OSError as exc:
        if exc.errno != errno.ENOENT:
            print(
                f"Land coordinator: ignoring failure attribution receipt ({exc})",
                file=err,
                flush=True,
            )
        return None
    try:
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = os.read(fd, 65536)
            if not chunk:
                break
            total += len(chunk)
            if total > MAX_ATTRIBUTION_BYTES:
                print(
                    "Land coordinator: ignoring oversized failure attribution receipt",
                    file=err,
                    flush=True,
                )
                return None
            chunks.append(chunk)
        raw_bytes = b"".join(chunks)
    finally:
        os.close(fd)

    if not raw_bytes:
        return None
    try:
        document = json.loads(raw_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        print(
            f"Land coordinator: ignoring malformed failure attribution receipt: {exc}",
            file=err,
            flush=True,
        )
        return None
    if (
        not isinstance(document, dict)
        or set(document) != {"format", "failures"}
        or document.get("format") != ATTRIBUTION_FORMAT
    ):
        print(
            "Land coordinator: ignoring failure attribution receipt with invalid envelope",
            file=err,
            flush=True,
        )
        return None
    raw_failures = document.get("failures")
    if (
        not isinstance(raw_failures, list)
        or not raw_failures
        or len(raw_failures) > MAX_ATTRIBUTION_FAILURES
    ):
        return None

    parsed: list[tuple[str, tuple[str, ...]]] = []
    for entry in raw_failures:
        if not isinstance(entry, dict) or set(entry) != {"unit", "causal_paths"}:
            print(
                "Land coordinator: ignoring failure attribution receipt with invalid failure entry",
                file=err,
                flush=True,
            )
            return None
        unit = entry.get("unit")
        causal_raw = entry.get("causal_paths")
        if (
            not isinstance(unit, str)
            or not unit.strip()
            or "\0" in unit
            or "\n" in unit
            or "\r" in unit
            or not isinstance(causal_raw, list)
        ):
            print(
                "Land coordinator: ignoring failure attribution receipt with invalid unit or causal_paths",
                file=err,
                flush=True,
            )
            return None
        validated_paths: list[str] = []
        for raw_p in causal_raw:
            canonical = _validate_canonical_repo_path(raw_p, checkout=checkout)
            if canonical is None:
                print(
                    f"Land coordinator: ignoring failure attribution receipt with non-canonical path {raw_p!r}",
                    file=err,
                    flush=True,
                )
                return None
            if canonical not in validated_paths:
                validated_paths.append(canonical)
        parsed.append((unit.strip(), tuple(validated_paths)))
    return parsed


def _member_changed_paths(checkout: Path, base_sha: str, head_sha: str) -> tuple[str, ...]:
    res = _git_batch(checkout, "diff", "--name-only", "-z", f"{base_sha}...{head_sha}")
    if res.returncode != 0:
        return ()
    return tuple(item for item in res.stdout.split("\0") if item)


def _attribute_batch_failures(
    checkout: Path,
    t0_sha: str,
    merged: list[tuple[_BatchMember, str]],
    failures: list[tuple[str, tuple[str, ...]]],
) -> list[tuple[_BatchMember, list[str]]]:
    changed_by_run = {
        member.run_id: _member_changed_paths(checkout, t0_sha, member.expected_head_sha)
        for member, _tree in merged
    }
    blamed_units: dict[str, list[str]] = {}
    for unit, causal_paths in failures:
        if not causal_paths:
            continue
        matching = [
            member
            for member, _tree in merged
            if any(
                ch == cp or ch.startswith(f"{cp}/")
                for ch in changed_by_run[member.run_id]
                for cp in causal_paths
            )
        ]
        if len(matching) == 1:
            culprit = matching[0]
            units_for_culprit = blamed_units.setdefault(culprit.run_id, [])
            if unit not in units_for_culprit:
                units_for_culprit.append(unit)
    culprits = [
        (member, blamed_units[member.run_id])
        for member, _tree in merged
        if member.run_id in blamed_units
    ]
    if not culprits:
        return []
    if len(culprits) >= len(merged):
        culprits = culprits[:-1]
    return culprits


@dataclass
class _BatchMember:
    run_id: str
    pull_request: int
    branch: str
    expected_head_sha: str
    is_leader: bool
    target_sync: bool
    reuse_full: bool
    avoid_commits: dict[str, str]
    deductive_bisect: bool = True
    phase: str = "preflight"
    gate_exit_status: int | None = None
    title: str = ""


class _BatchRunner:
    """Coordinate batch formation, combined gating, atomic publication, and binary bisect."""

    def __init__(
        self,
        *,
        client: object,
        leader_run_id: str,
        worker_pid: int,
        checkout: Path,
        gate_command: list[str],
        environment: dict[str, str],
        metadata_client: PullRequestMetadataClient,
        publisher: MergePublisher,
        out: TextIO,
        err: TextIO,
    ) -> None:
        self.client = client
        self.leader_run_id = leader_run_id
        self.worker_pid = worker_pid
        self.checkout = checkout
        self.gate_command = gate_command
        self.environment = environment
        self.metadata_client = metadata_client
        self.publisher = publisher
        self.out = out
        self.err = err
        self.leader_cancelled = False
        self.leader_outcome: tuple[int, int | None] | None = None

    def _poll_cancellations(
        self, members: list[_BatchMember]
    ) -> list[_BatchMember]:
        status_payload = self.client.batch_status(
            self.leader_run_id,
            worker_pid=self.worker_pid,
        )
        if status_payload.get("cancel_requested"):
            self.leader_cancelled = True
            return []
        cancelled_ids = {
            str(entry["run_id"])
            for entry in status_payload["members"]
            if entry.get("cancel_requested")
        }
        remaining: list[_BatchMember] = []
        for member in members:
            if member.run_id in cancelled_ids:
                self._finalize_member(member, 130, "cancelled", None)
            else:
                remaining.append(member)
        return remaining

    def _advance_phase(
        self,
        member: _BatchMember,
        phase: str,
        gate_exit_status: int | None,
    ) -> None:
        if (
            member.phase == phase
            and (
                gate_exit_status is None
                or member.gate_exit_status == gate_exit_status
            )
        ):
            return
        if member.is_leader:
            self.client.update_land_phase(
                self.leader_run_id,
                phase=phase,
                gate_exit_status=gate_exit_status,
                worker_pid=self.worker_pid,
            )
        else:
            self.client.update_batch_member_phase(
                self.leader_run_id,
                member_run_id=member.run_id,
                phase=phase,
                gate_exit_status=gate_exit_status,
                worker_pid=self.worker_pid,
            )
        member.phase = phase
        if gate_exit_status is not None:
            member.gate_exit_status = gate_exit_status

    def _finalize_member(
        self,
        member: _BatchMember,
        exit_status: int,
        failure_reason: str | None,
        gate_exit_status: int | None,
    ) -> None:
        if member.is_leader:
            if (
                member.phase == "gating"
                and member.gate_exit_status is None
                and exit_status != 0
            ):
                resolved_gate = gate_exit_status if gate_exit_status is not None else 0
                self._advance_phase(member, "gating", resolved_gate)
                self.leader_outcome = (exit_status, resolved_gate)
                return
            self.leader_outcome = (exit_status, gate_exit_status)
            return
        self.client.finish_batch_member(
            self.leader_run_id,
            member_run_id=member.run_id,
            exit_status=exit_status,
            worker_pid=self.worker_pid,
            failure_reason=failure_reason,
            gate_exit_status=gate_exit_status,
        )

    def _fetch_remote_refs(
        self,
        target_branch: str,
        members: list[_BatchMember],
    ) -> tuple[str, dict[str, str]]:
        refs = [f"refs/heads/{target_branch}", *(f"refs/heads/{m.branch}" for m in members)]
        fetched = _git_batch(self.checkout, "fetch", "--quiet", "--no-tags", "origin", *refs)
        if fetched.returncode != 0:
            raise RuntimeError(
                fetched.stderr.strip() or f"git fetch failed with {fetched.returncode}"
            )
        ls = _git_batch(self.checkout, "ls-remote", "origin", *refs)
        if ls.returncode != 0:
            raise RuntimeError(
                ls.stderr.strip() or f"git ls-remote failed with {ls.returncode}"
            )
        remote_heads: dict[str, str] = {}
        for line in ls.stdout.splitlines():
            parts = line.split()
            if len(parts) == 2:
                remote_heads[parts[1]] = parts[0].lower()
        target_key = f"refs/heads/{target_branch}"
        t0_sha = remote_heads.get(target_key)
        if not t0_sha:
            raise RuntimeError(f"missing remote target branch {target_branch}")
        return t0_sha, remote_heads

    def _validate_member(
        self,
        member: _BatchMember,
        *,
        target_branch: str,
        t0_sha: str,
        remote_heads: Mapping[str, str],
        check_target_sync: bool,
    ) -> tuple[int, str] | None:
        try:
            pr = self.metadata_client.pull_request(member.pull_request)
        except Exception as exc:
            print(
                f"Land coordinator: REFUSED (merge-error) for PR #{member.pull_request} — {exc}",
                file=self.err,
                flush=True,
            )
            return EXIT_MERGE_ERROR, "merge-error"
        if (
            pr.get("state") != "OPEN"
            or pr.get("is_draft")
            or not pr.get("same_repository")
            or pr.get("base_ref") != target_branch
            or pr.get("head_ref") != member.branch
        ):
            print(
                f"Land coordinator: REFUSED (pr-not-ready) — PR #{member.pull_request} "
                "is no longer an open same-repository request on its target branch",
                file=self.err,
                flush=True,
            )
            return EXIT_PR_NOT_READY, "pr-not-ready"
        member.title = str(pr.get("title") or f"Pull request #{member.pull_request}")
        pr_head = str(pr.get("head_sha") or "").lower()
        remote_branch_sha = remote_heads.get(f"refs/heads/{member.branch}", "")
        if (
            pr_head != member.expected_head_sha.lower()
            or (remote_branch_sha and remote_branch_sha != member.expected_head_sha.lower())
        ):
            print(
                f"Land coordinator: REFUSED (head-changed) — PR #{member.pull_request} moved "
                f"from {member.expected_head_sha} to {pr_head or remote_branch_sha}",
                file=self.err,
                flush=True,
            )
            return EXIT_HEAD_CHANGED, "head-changed"
        if check_target_sync and not member.target_sync and not _is_git_ancestor(
            self.checkout, t0_sha, member.expected_head_sha
        ):
            print(
                f"Land coordinator: REFUSED (stale-main) — target {target_branch} at {t0_sha} "
                f"is not an ancestor of PR #{member.pull_request} and target sync is disabled",
                file=self.err,
                flush=True,
            )
            return EXIT_STALE_MAIN, "stale-main"
        avoided_hit = _avoid_refusal_for_heads(
            self.checkout,
            heads={
                f"request #{member.pull_request}": member.expected_head_sha,
                f"target {target_branch}": t0_sha,
            },
            avoided=member.avoid_commits,
        )
        if avoided_hit is not None:
            desc, sha, reason = avoided_hit
            print(
                f"Land coordinator: REFUSED (avoided-commit) — {desc} reaches avoided "
                f"commit {sha} ({reason})",
                file=self.err,
                flush=True,
            )
            return EXIT_AVOIDED, "avoided-commit"
        return None

    def _sequential_merge_slice(
        self,
        t0_sha: str,
        members: list[_BatchMember],
    ) -> tuple[list[tuple[_BatchMember, str]], str, list[dict[str, str]]]:
        for cmd in (
            ("update-ref", "--no-deref", "HEAD", t0_sha),
            ("reset", "--hard", t0_sha),
            ("clean", "-ffd"),
        ):
            res = _git_batch(self.checkout, *cmd)
            if res.returncode != 0:
                raise RuntimeError(res.stderr.strip() or f"git {' '.join(cmd)} failed")
        prev_tip = t0_sha
        merged: list[tuple[_BatchMember, str]] = []
        dropped: list[dict[str, str]] = []
        for member in members:
            merge_res = _git_batch(
                self.checkout,
                "-c",
                "user.name=AGCoord",
                "-c",
                "user.email=agcoord@localhost",
                "merge",
                "--no-ff",
                "--no-gpg-sign",
                "-m",
                f"Batch merge PR #{member.pull_request}",
                member.expected_head_sha,
            )
            if merge_res.returncode != 0:
                _git_batch(self.checkout, "merge", "--abort")
                _git_batch(self.checkout, "reset", "--hard", prev_tip)
                _git_batch(self.checkout, "clean", "-ffd")
                print(
                    f"Land coordinator: REFUSED (merge-error) — PR #{member.pull_request} "
                    f"({member.branch}) conflicted with batch base {prev_tip}",
                    file=self.err,
                    flush=True,
                )
                dropped.append({"run_id": member.run_id, "reason": "merge-conflict"})
                self._finalize_member(member, EXIT_MERGE_ERROR, "merge-error", None)
                continue
            tip_res = _git_batch(self.checkout, "rev-parse", "HEAD")
            tree_res = _git_batch(self.checkout, "rev-parse", "HEAD^{tree}")
            if tip_res.returncode != 0 or tree_res.returncode != 0:
                raise RuntimeError("cannot resolve batch merge tip or tree")
            prev_tip = tip_res.stdout.strip()
            merged.append((member, tree_res.stdout.strip()))
        return merged, prev_tip, dropped

    def _publish_slice(
        self,
        target_branch: str,
        t0_sha: str,
        merged: list[tuple[_BatchMember, str]],
        *,
        step: int,
        dropped: list[dict[str, str]],
    ) -> bool:
        slice_members = [member for member, _tree in merged]
        remaining_after_cancel = self._poll_cancellations(slice_members)
        if self.leader_cancelled:
            self.client.record_batch_step(
                self.leader_run_id,
                worker_pid=self.worker_pid,
                step=step,
                gate_exit_status=0,
                outcome="cancelled",
            )
            return False
        if len(remaining_after_cancel) != len(slice_members):
            remaining_ids = {member.run_id for member in remaining_after_cancel}
            updated_dropped = [
                *dropped,
                *(
                    {"run_id": member.run_id, "reason": "cancelled"}
                    for member in slice_members
                    if member.run_id not in remaining_ids
                ),
            ]
            self.client.record_batch_step(
                self.leader_run_id,
                worker_pid=self.worker_pid,
                step=step,
                gate_exit_status=0,
                dropped=updated_dropped,
                outcome="cancelled",
            )
            self.bisect_and_land(target_branch, remaining_after_cancel)
            return False

        try:
            current_t0, remote_heads = self._fetch_remote_refs(
                target_branch, slice_members
            )
        except Exception as exc:
            print(
                f"Land coordinator: REFUSED (merge-error) — {exc}",
                file=self.err,
                flush=True,
            )
            self.client.record_batch_step(
                self.leader_run_id,
                worker_pid=self.worker_pid,
                step=step,
                gate_exit_status=0,
                outcome="merge-error",
            )
            for member in slice_members:
                self._finalize_member(member, EXIT_MERGE_ERROR, "merge-error", None)
            return False

        if current_t0 != t0_sha:
            print(
                f"Land coordinator: REFUSED (stale-main) — target {target_branch} advanced from "
                f"{t0_sha} to {current_t0} during batch gate",
                file=self.err,
                flush=True,
            )
            self.client.record_batch_step(
                self.leader_run_id,
                worker_pid=self.worker_pid,
                step=step,
                gate_exit_status=0,
                outcome="stale-main",
            )
            for member in slice_members:
                self._finalize_member(member, EXIT_STALE_MAIN, "stale-main", None)
            return False

        clean_members: list[_BatchMember] = []
        post_gate_dropped: list[dict[str, str]] = []
        bad_failures: list[tuple[_BatchMember, int, str]] = []
        for member in slice_members:
            failure = self._validate_member(
                member,
                target_branch=target_branch,
                t0_sha=t0_sha,
                remote_heads=remote_heads,
                check_target_sync=False,
            )
            if failure is not None:
                exit_code, reason = failure
                post_gate_dropped.append({"run_id": member.run_id, "reason": reason})
                bad_failures.append((member, exit_code, reason))
            else:
                clean_members.append(member)
        if bad_failures:
            reasons = {item["reason"] for item in post_gate_dropped}
            step_outcome = (
                next(iter(reasons)) if len(reasons) == 1 else "preflight-failed"
            )
            self.client.record_batch_step(
                self.leader_run_id,
                worker_pid=self.worker_pid,
                step=step,
                gate_exit_status=0,
                dropped=[*dropped, *post_gate_dropped],
                outcome=step_outcome,
            )
            for member, exit_code, reason in bad_failures:
                self._finalize_member(member, exit_code, reason, None)
            if clean_members:
                self.bisect_and_land(target_branch, clean_members)
            return False

        for member in slice_members:
            self._advance_phase(member, "publishing", 0)

        try:
            parent_sha = t0_sha
            ref_updates: list[dict[str, object]] = []
            for member, tree_sha in merged:
                title = member.title or f"Pull request #{member.pull_request}"
                message = f"Merge pull request #{member.pull_request} from {member.branch}\n\n{title}\n"
                created = self.publisher.create_merge_commit(
                    message=message,
                    tree_sha=tree_sha,
                    parent_shas=(parent_sha, member.expected_head_sha),
                )
                parent_sha = str(created["sha"])
                ref_updates.append(
                    {
                        "name": f"refs/heads/{member.branch}",
                        "before_oid": member.expected_head_sha,
                        "after_oid": member.expected_head_sha,
                        "force": False,
                    }
                )
            all_updates = (
                {
                    "name": f"refs/heads/{target_branch}",
                    "before_oid": t0_sha,
                    "after_oid": parent_sha,
                    "force": False,
                },
                *ref_updates,
            )
            self.publisher.update_refs(all_updates)
        except Exception as exc:
            print(
                f"Land coordinator: REFUSED (publish-failed) — atomic batch update rejected: {exc}",
                file=self.err,
                flush=True,
            )
            self.client.record_batch_step(
                self.leader_run_id,
                worker_pid=self.worker_pid,
                step=step,
                gate_exit_status=0,
                outcome="publish-failed",
            )
            for member in slice_members:
                self._finalize_member(member, EXIT_PUBLISH_FAILED, "publish-failed", 0)
            return False

        self.client.record_batch_step(
            self.leader_run_id,
            worker_pid=self.worker_pid,
            step=step,
            gate_exit_status=0,
            outcome="published",
            published_sha=parent_sha,
        )
        prs = ", ".join(f"#{member.pull_request}" for member in slice_members)
        print(
            f"Land coordinator: batch published {prs} onto {target_branch} at {parent_sha}",
            file=self.out,
            flush=True,
        )
        for member in slice_members:
            self._finalize_member(member, 0, None, 0)
        return not dropped

    def bisect_and_land(
        self,
        target_branch: str,
        members: list[_BatchMember],
        *,
        known_bad_tree_sha: str | None = None,
        known_bad_exit_status: int | None = None,
    ) -> bool:
        if not members:
            return True
        attempted_ids = [member.run_id for member in members]
        members = self._poll_cancellations(members)
        if self.leader_cancelled:
            return False
        remaining_ids = {member.run_id for member in members}
        dropped: list[dict[str, str]] = [
            {"run_id": rid, "reason": "cancelled"}
            for rid in attempted_ids
            if rid not in remaining_ids
        ]
        if not members:
            self.client.record_batch_step(
                self.leader_run_id,
                worker_pid=self.worker_pid,
                member_run_ids=attempted_ids,
                gated_member_run_ids=[],
                dropped=dropped,
                outcome="cancelled",
            )
            return False

        try:
            t0_sha, remote_heads = self._fetch_remote_refs(target_branch, members)
        except Exception as exc:
            print(
                f"Land coordinator: REFUSED (merge-error) — {exc}",
                file=self.err,
                flush=True,
            )
            dropped.extend(
                {"run_id": member.run_id, "reason": "merge-error"}
                for member in members
            )
            self.client.record_batch_step(
                self.leader_run_id,
                worker_pid=self.worker_pid,
                member_run_ids=attempted_ids,
                gated_member_run_ids=[],
                dropped=dropped,
                outcome="merge-error",
            )
            for member in members:
                self._finalize_member(member, EXIT_MERGE_ERROR, "merge-error", None)
            return False

        valid_members: list[_BatchMember] = []
        preflight_failures: list[tuple[_BatchMember, int, str]] = []
        for member in members:
            failure = self._validate_member(
                member,
                target_branch=target_branch,
                t0_sha=t0_sha,
                remote_heads=remote_heads,
                check_target_sync=True,
            )
            if failure is not None:
                exit_code, reason = failure
                dropped.append({"run_id": member.run_id, "reason": reason})
                preflight_failures.append((member, exit_code, reason))
            else:
                valid_members.append(member)
        if not valid_members:
            reasons = {item["reason"] for item in dropped}
            step_outcome = (
                next(iter(reasons)) if len(reasons) == 1 else "preflight-failed"
            )
            self.client.record_batch_step(
                self.leader_run_id,
                worker_pid=self.worker_pid,
                member_run_ids=attempted_ids,
                gated_member_run_ids=[],
                dropped=dropped,
                base_sha=t0_sha,
                outcome=step_outcome,
            )
            for member, exit_code, reason in preflight_failures:
                self._finalize_member(member, exit_code, reason, None)
            return False
        for member, exit_code, reason in preflight_failures:
            self._finalize_member(member, exit_code, reason, None)

        merged, combined_tip, merge_dropped = self._sequential_merge_slice(
            t0_sha, valid_members
        )
        dropped.extend(merge_dropped)
        if not merged:
            self.client.record_batch_step(
                self.leader_run_id,
                worker_pid=self.worker_pid,
                member_run_ids=attempted_ids,
                gated_member_run_ids=[],
                dropped=dropped,
                base_sha=t0_sha,
                outcome="merge-conflict",
            )
            return False

        combined_tree = merged[-1][1]
        deductive_allowed = all(member.deductive_bisect for member, _tree in merged)
        if (
            known_bad_tree_sha is not None
            and not dropped
            and deductive_allowed
            and combined_tree == known_bad_tree_sha
        ):
            clean_slice = [member for member, _tree in merged]
            if len(clean_slice) == 1:
                singleton = clean_slice[0]
                deduced_exit = (
                    known_bad_exit_status
                    if known_bad_exit_status is not None and known_bad_exit_status != 0
                    else 1
                )
                self._advance_phase(singleton, "gating", deduced_exit)
                print(
                    f"Land coordinator: deduced gate failure for PR #{singleton.pull_request} "
                    "without re-running gate",
                    file=self.err,
                    flush=True,
                )
                self.client.record_batch_step(
                    self.leader_run_id,
                    worker_pid=self.worker_pid,
                    member_run_ids=attempted_ids,
                    gated_member_run_ids=[],
                    dropped=dropped,
                    base_sha=t0_sha,
                    gating_head_sha=combined_tip,
                    outcome="deduced-gate-failed",
                )
                self._finalize_member(
                    singleton, deduced_exit, "gate-failed", deduced_exit
                )
                return False

            mid = _largest_power_of_two_lt(len(clean_slice))
            left = clean_slice[:mid]
            right = clean_slice[mid:]
            self.client.record_batch_step(
                self.leader_run_id,
                worker_pid=self.worker_pid,
                member_run_ids=attempted_ids,
                gated_member_run_ids=[],
                dropped=dropped,
                base_sha=t0_sha,
                gating_head_sha=combined_tip,
                outcome="deduced-bisected",
                split=(len(left), len(right)),
            )
            print(
                f"Land coordinator: deduced failing slice of {len(clean_slice)} PR(s); "
                f"bisecting into {len(left)} + {len(right)} without re-running gate",
                file=self.out,
                flush=True,
            )
            left_clean = self.bisect_and_land(target_branch, left)
            if self.leader_cancelled:
                return False
            self.bisect_and_land(
                target_branch,
                right,
                known_bad_tree_sha=known_bad_tree_sha if left_clean else None,
                known_bad_exit_status=known_bad_exit_status if left_clean else None,
            )
            return False

        gated_ids = [member.run_id for member, _tree in merged]
        reused_source: str | None = None
        if len(merged) == 1 and merged[0][0].reuse_full and _is_git_ancestor(
            self.checkout, t0_sha, merged[0][0].expected_head_sha
        ):
            singleton = merged[0][0]
            if singleton.is_leader:
                reused_source = self.client.reuse_gate(
                    self.leader_run_id,
                    worker_pid=self.worker_pid,
                )
            else:
                reused_source = self.client.reuse_batch_member_gate(
                    self.leader_run_id,
                    member_run_id=singleton.run_id,
                    worker_pid=self.worker_pid,
                )

        if reused_source is not None:
            parsed_attribution = None
            singleton = merged[0][0]
            singleton.phase = "gating"
            singleton.gate_exit_status = 0
            print(
                f"Land coordinator: gate reused from {reused_source} for PR #{singleton.pull_request}",
                file=self.out,
                flush=True,
            )
            step = self.client.record_batch_step(
                self.leader_run_id,
                worker_pid=self.worker_pid,
                member_run_ids=attempted_ids,
                gated_member_run_ids=gated_ids,
                dropped=dropped,
                base_sha=t0_sha,
                gating_head_sha=combined_tip,
                gate_reused_from=reused_source,
                gate_exit_status=0,
                outcome="running",
            )
            gate_status = 0
        else:
            for member, _tree in merged:
                self._advance_phase(member, "gating", None)
            pr_list = ", ".join(f"#{member.pull_request}" for member, _tree in merged)
            print(
                f"Land coordinator: batch gate started for {len(merged)} PR(s) ({pr_list}) "
                f"at {combined_tip}",
                file=self.out,
                flush=True,
            )
            step = self.client.record_batch_step(
                self.leader_run_id,
                worker_pid=self.worker_pid,
                member_run_ids=attempted_ids,
                gated_member_run_ids=gated_ids,
                dropped=dropped,
                base_sha=t0_sha,
                gating_head_sha=combined_tip,
                outcome="running",
            )
            self.client.set_batch_slice(
                self.leader_run_id, gating_head_sha=combined_tip,
                member_run_ids=gated_ids,
                worker_pid=self.worker_pid,
            )
            parsed_attribution: list[tuple[str, tuple[str, ...]]] | None = None
            base_gate_env = {
                k: v
                for k, v in self.environment.items()
                if k != BATCH_ATTRIBUTION_FILE_ENV
            }
            attr_tmpdir = (
                tempfile.TemporaryDirectory(prefix="agcoord-batch-attr-")
                if len(merged) >= 2
                else None
            )
            try:
                gate_env = dict(base_gate_env)
                attr_path: Path | None = None
                if attr_tmpdir is not None:
                    attr_path = Path(attr_tmpdir.name) / "attribution.json"
                    fd = os.open(
                        attr_path,
                        os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                        0o600,
                    )
                    os.close(fd)
                    gate_env[BATCH_ATTRIBUTION_FILE_ENV] = str(attr_path)
                gate_status = _run_gate(
                    self.gate_command,
                    checkout=self.checkout,
                    environment=gate_env,
                    out=self.out,
                )
                if gate_status != 0 and attr_path is not None:
                    parsed_attribution = _load_failure_attribution(
                        attr_path,
                        checkout=self.checkout,
                        err=self.err,
                    )
            finally:
                if attr_tmpdir is not None:
                    attr_tmpdir.cleanup()
                self.client.clear_batch_slice(self.leader_run_id, worker_pid=self.worker_pid)

        if gate_status == 0:
            return self._publish_slice(
                target_branch, t0_sha, merged, step=step, dropped=dropped
            )

        status_payload = self.client.batch_status(
            self.leader_run_id,
            worker_pid=self.worker_pid,
        )
        if status_payload.get("cancel_requested"):
            self.leader_cancelled = True
            self.client.record_batch_step(
                self.leader_run_id,
                worker_pid=self.worker_pid,
                step=step,
                gate_exit_status=gate_status,
                outcome="cancelled",
            )
            return False

        clean_slice = [member for member, _tree in merged]
        if len(clean_slice) == 1:
            singleton = clean_slice[0]
            singleton_cancelled = any(
                str(entry["run_id"]) == singleton.run_id
                and bool(entry.get("cancel_requested"))
                for entry in status_payload["members"]
            )
            if singleton_cancelled:
                self.client.record_batch_step(
                    self.leader_run_id,
                    worker_pid=self.worker_pid,
                    step=step,
                    gate_exit_status=gate_status,
                    outcome="cancelled",
                )
                self._finalize_member(singleton, 130, "cancelled", None)
                return False
            self._advance_phase(singleton, "gating", gate_status)
            print(
                f"Land coordinator: gate failed for PR #{singleton.pull_request} "
                f"with exit status {gate_status}; publication was not attempted",
                file=self.err,
                flush=True,
            )
            self.client.record_batch_step(
                self.leader_run_id,
                worker_pid=self.worker_pid,
                step=step,
                gate_exit_status=gate_status,
                outcome="gate-failed",
            )
            self._finalize_member(singleton, gate_status, "gate-failed", gate_status)
            return False

        cancelled_member_ids = {
            str(entry["run_id"])
            for entry in status_payload["members"]
            if entry.get("cancel_requested")
        }
        if parsed_attribution is not None and len(clean_slice) >= 2:
            evicted = _attribute_batch_failures(
                self.checkout,
                t0_sha,
                merged,
                parsed_attribution,
            )
            evicted = [
                (member, units)
                for member, units in evicted
                if member.run_id not in cancelled_member_ids
            ]
            if evicted:
                evicted_ids = {member.run_id for member, _units in evicted}
                remaining_slice = [
                    member
                    for member in clean_slice
                    if member.run_id not in evicted_ids
                ]
                self.client.record_batch_step(
                    self.leader_run_id,
                    worker_pid=self.worker_pid,
                    step=step,
                    gate_exit_status=gate_status,
                    outcome="attributed-gate-failed",
                    attributed=[
                        {"run_id": member.run_id, "units": units}
                        for member, units in evicted
                    ],
                )
                for culprit, units in evicted:
                    self._advance_phase(culprit, "gating", gate_status)
                    print(
                        f"Land coordinator: attributed gate failure (exit {gate_status}) to "
                        f"PR #{culprit.pull_request} via {', '.join(units)}; evicting without solo gate",
                        file=self.err,
                        flush=True,
                    )
                    self._finalize_member(
                        culprit, gate_status, "gate-failed", gate_status
                    )
                if remaining_slice:
                    self.bisect_and_land(target_branch, remaining_slice)
                return False

        mid = _largest_power_of_two_lt(len(clean_slice))
        left = clean_slice[:mid]
        right = clean_slice[mid:]
        self.client.record_batch_step(
            self.leader_run_id,
            worker_pid=self.worker_pid,
            step=step,
            gate_exit_status=gate_status,
            outcome="bisected",
            split=(len(left), len(right)),
        )
        print(
            f"Land coordinator: batch gate failed (exit {gate_status}); bisecting "
            f"{len(clean_slice)} PR(s) into {len(left)} + {len(right)}",
            file=self.out,
            flush=True,
        )
        left_clean = self.bisect_and_land(target_branch, left)
        if self.leader_cancelled:
            return False
        next_bad_tree = combined_tree if (left_clean and deductive_allowed) else None
        self.bisect_and_land(
            target_branch,
            right,
            known_bad_tree_sha=next_bad_tree,
            known_bad_exit_status=gate_status if next_bad_tree is not None else None,
        )
        return False


def _execute_batch_if_applicable(
    *,
    args: argparse.Namespace,
    request: int,
    command: list[str],
    client: object,
    metadata_client: PullRequestMetadataClient,
    publisher: MergePublisher,
    target_sync: bool,
    avoid_commits: dict[str, str],
) -> int | None:
    if not args.batch_candidates_json:
        return None
    raw_candidates = json.loads(args.batch_candidates_json)
    if not isinstance(raw_candidates, list) or not raw_candidates:
        return None

    from .avoid import resolve_avoid_commits

    try:
        leader_pr = metadata_client.pull_request(request)
    except Exception:
        return None
    target_branch = leader_pr.get("base_ref")
    if (
        not isinstance(target_branch, str)
        or not target_branch
        or leader_pr.get("state") != "OPEN"
        or leader_pr.get("is_draft")
        or not leader_pr.get("same_repository")
        or leader_pr.get("head_ref") != args.branch
        or str(leader_pr.get("head_sha") or "").lower() != args.head_sha.lower()
    ):
        return None
    if _avoid_refusal_for_heads(
        Path(args.checkout).expanduser().resolve(),
        heads={"leader head": args.head_sha},
        avoided=avoid_commits,
    ):
        return None

    max_batch_size = (
        _largest_power_of_two_le(int(args.batch_max_size))
        if getattr(args, "batch_max_size", None) is not None
        else 4
    )
    if max_batch_size <= 1:
        return None

    selected_ids: list[str] = []
    checkout_path = Path(args.checkout).expanduser().resolve()
    for entry in raw_candidates:
        if len(selected_ids) >= max_batch_size - 1:
            break
        try:
            cand_run_id = str(entry["run_id"])
            cand_pr_num = int(entry["publication_request"])
            cand_branch = str(entry["branch"])
            cand_head = str(entry["head_sha"])
            cand_opts = dict(entry.get("environment_options") or {})
            cand_avoid = resolve_avoid_commits(
                args.state_dir,
                {},
                requested=[str(s) for s in cand_opts.get("avoid", [])],
            )
            cand_pr = metadata_client.pull_request(cand_pr_num)
        except Exception:
            continue
        if cand_pr.get("base_ref") != target_branch:
            continue
        if (
            cand_pr.get("state") != "OPEN"
            or cand_pr.get("is_draft")
            or not cand_pr.get("same_repository")
            or cand_pr.get("head_ref") != cand_branch
            or str(cand_pr.get("head_sha") or "").lower() != cand_head.lower()
        ):
            continue
        if _avoid_refusal_for_heads(
            checkout_path,
            heads={f"candidate #{cand_pr_num}": cand_head},
            avoided=cand_avoid,
        ):
            continue
        selected_ids.append(cand_run_id)

    target_total = _largest_power_of_two_le(min(1 + len(selected_ids), max_batch_size))
    selected_ids = selected_ids[: target_total - 1]
    if not selected_ids:
        return None

    claimed = client.claim_batch(
        args.run_id,
        member_run_ids=selected_ids,
        worker_pid=os.getpid(),
        target_branch=target_branch,
        max_batch_size=max_batch_size,
    )
    if len(claimed) <= 1:
        return None

    batch_members: list[_BatchMember] = []
    for entry in claimed:
        opts = dict(entry.get("environment_options") or {})
        is_leader = bool(entry.get("is_leader"))
        member_avoid = (
            dict(avoid_commits)
            if is_leader
            else resolve_avoid_commits(
                args.state_dir,
                {},
                requested=[str(s) for s in opts.get("avoid", [])],
            )
        )
        member_sync = (
            (target_sync and not args.no_target_sync)
            if is_leader
            else bool(opts.get("target_sync", True))
        )
        member_reuse = (
            bool(args.reuse_full)
            if is_leader
            else bool(opts.get("reuse_full", False))
        )
        member_deductive = (
            (not bool(getattr(args, "no_deductive_bisect", False)))
            if is_leader
            else bool(opts.get("deductive_bisect", True))
        )
        batch_members.append(
            _BatchMember(
                run_id=str(entry["run_id"]),
                pull_request=int(entry["publication_request"]),
                branch=str(entry["branch"]),
                expected_head_sha=str(entry["head_sha"]),
                is_leader=is_leader,
                target_sync=member_sync,
                reuse_full=member_reuse,
                avoid_commits=member_avoid,
                deductive_bisect=member_deductive,
            )
        )

    runner = _BatchRunner(
        client=client,
        leader_run_id=args.run_id,
        worker_pid=os.getpid(),
        checkout=checkout_path,
        gate_command=_command(command),
        environment=_environment(os.environ),
        metadata_client=metadata_client,
        publisher=publisher,
        out=sys.stdout,
        err=sys.stderr,
    )
    runner.bisect_and_land(target_branch, batch_members)
    if runner.leader_cancelled:
        return 130
    if runner.leader_outcome is None:
        return EXIT_MERGE_ERROR
    leader_exit, _ = runner.leader_outcome
    return leader_exit


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="agc land worker")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--state-dir", required=True)
    parser.add_argument("--checkout", required=True)
    parser.add_argument("--branch", required=True)
    parser.add_argument("--head-sha", required=True)
    parser.add_argument("--adapter", required=True)
    parser.add_argument("--request-json", required=True)
    parser.add_argument("--no-target-sync", action="store_true")
    parser.add_argument("--reuse-full", action="store_true")
    parser.add_argument("--no-deductive-bisect", action="store_true")
    parser.add_argument("--avoid", action="append", default=[])
    parser.add_argument("--batch-max-size", type=int, default=None)
    parser.add_argument("--batch-candidates-json", default=None)
    parser.add_argument("gate_command", nargs=argparse.REMAINDER)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    command = list(args.gate_command)
    if command and command[0] == "--":
        command.pop(0)
    marker = os.environ.get("AGCOORD_RUN_ID")
    kind_marker = os.environ.get("AGCOORD_RUN_KIND")
    state_marker = os.environ.get("AGCOORD_STATE_DIR")
    same_state = bool(state_marker) and (
        Path(state_marker).expanduser().resolve()
        == Path(args.state_dir).expanduser().resolve()
    )
    if (
        not args.run_id.startswith("land-")
        or marker != args.run_id
        or kind_marker != "land"
        or not same_state
    ):
        print(
            "Land coordinator: REFUSED (merge-error) — the land worker requires "
            "its exact broker admission context",
            file=sys.stderr,
        )
        return EXIT_MERGE_ERROR
    if args.adapter != "github":
        print(
            f"Land coordinator: REFUSED (merge-error) — unknown adapter {args.adapter!r}",
            file=sys.stderr,
        )
        return EXIT_MERGE_ERROR
    try:
        request = json.loads(args.request_json)
    except json.JSONDecodeError as exc:
        print(
            f"Land coordinator: REFUSED (merge-error) — invalid request: {exc}",
            file=sys.stderr,
        )
        return EXIT_MERGE_ERROR

    from .avoid import resolve_avoid_commits
    from .github import GitHubMergePublisher, GitHubMetadataClient
    from .queue import CoordinatorClient, CoordinatorError, LAND_TARGET_SYNC_ENV

    client = CoordinatorClient(
        state_dir=args.state_dir,
        checkout=args.checkout,
        autostart=False,
    )
    try:
        client.verify_admission(
            args.run_id,
            kind="land",
            checkout=args.checkout,
            head_sha=args.head_sha,
            worker_pid=os.getpid(),
        )
        target_sync = os.environ.get(LAND_TARGET_SYNC_ENV, "1")
        if target_sync not in {"0", "1"}:
            raise CoordinatorError(
                f"land admission has invalid {LAND_TARGET_SYNC_ENV} state"
            )
        avoid_commits = resolve_avoid_commits(
            args.state_dir,
            os.environ,
            requested=args.avoid,
        )

        def phase_changed(phase: str, gate_exit_status: int | None) -> None:
            client.update_land_phase(
                args.run_id,
                phase=phase,
                gate_exit_status=gate_exit_status,
                worker_pid=os.getpid(),
            )

        def head_changed(old_head: str, new_head: str) -> None:
            client.update_land_phase(
                args.run_id,
                phase="preflight",
                gate_exit_status=None,
                worker_pid=os.getpid(),
                new_head_sha=new_head,
            )

        def reuse_gate() -> str | None:
            return client.reuse_gate(args.run_id, worker_pid=os.getpid())

        metadata_client = GitHubMetadataClient(args.checkout)
        publisher = GitHubMergePublisher(args.checkout)

        batch_result = _execute_batch_if_applicable(
            args=args,
            request=request,
            command=command,
            client=client,
            metadata_client=metadata_client,
            publisher=publisher,
            target_sync=(target_sync == "1"),
            avoid_commits=avoid_commits,
        )
        if batch_result is not None:
            if batch_result != 130:
                client.report_land_result(
                    args.run_id,
                    exit_status=batch_result,
                    worker_pid=os.getpid(),
                )
            return batch_result

        result = execute(
            request,
            command,
            checkout=args.checkout,
            branch=args.branch,
            head_sha=args.head_sha,
            environment=os.environ,
            metadata_client=metadata_client,
            publisher=publisher,
            out=sys.stdout,
            err=sys.stderr,
            phase_changed=phase_changed,
            head_changed=head_changed,
            synchronize_target=(target_sync == "1" and not args.no_target_sync),
            avoid_commits=avoid_commits,
            reuse_gate=reuse_gate if args.reuse_full else None,
        )
        client.report_land_result(
            args.run_id,
            exit_status=result,
            worker_pid=os.getpid(),
        )
        return result
    except CoordinatorError as exc:
        cancelled = "cancel" in str(exc).lower()
        reason = "cancelled" if cancelled else "merge-error"
        print(f"Land coordinator: REFUSED ({reason}) — {exc}", file=sys.stderr)
        return 130 if cancelled else EXIT_MERGE_ERROR
    except (TypeError, ValueError) as exc:
        print(f"Land coordinator: REFUSED (merge-error) — {exc}", file=sys.stderr)
        return EXIT_MERGE_ERROR


if __name__ == "__main__":
    raise SystemExit(main())
