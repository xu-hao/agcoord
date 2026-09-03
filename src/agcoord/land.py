"""Prepare one exact head, gate it, and publish it as an indivisible job."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Callable, Mapping, Sequence, TextIO

from .merge import (
    EXIT_MERGE_ERROR,
    MergePublisher,
    PullRequestMetadataClient,
    Wait,
    execute as publish,
    prepare,
)


PhaseChanged = Callable[[str, int | None], None]
HeadChanged = Callable[[str, str], None]


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
) -> int:
    """Preflight, gate, and publish without releasing the caller's reservation."""
    selected = Path(checkout).expanduser().resolve()
    command = _command(gate_command)
    selected_environment = _environment(environment)
    changed = phase_changed or (lambda _phase, _gate_status: None)
    retargeted = head_changed or (lambda _old_head, _new_head: None)

    changed("preflight", None)
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
        head_changed=retargeted,
        wait=wait,
    )
    if status != 0:
        return status

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

    # The durable transition is also the cancellation boundary. Once it succeeds, the
    # coordinator must observe the authenticated publication through an exact result.
    changed("publishing", 0)
    print("Land coordinator: gate passed; publishing exact head", file=out, flush=True)
    return publish(
        request,
        checkout=str(selected),
        branch=branch,
        head_sha=effective_head,
        metadata_client=metadata_client,
        publisher=publisher,
        out=out,
        err=err,
    )


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

        result = execute(
            request,
            command,
            checkout=args.checkout,
            branch=args.branch,
            head_sha=args.head_sha,
            environment=os.environ,
            metadata_client=GitHubMetadataClient(args.checkout),
            publisher=GitHubMergePublisher(args.checkout),
            out=sys.stdout,
            err=sys.stderr,
            phase_changed=phase_changed,
            head_changed=head_changed,
            synchronize_target=(target_sync == "1" and not args.no_target_sync),
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
