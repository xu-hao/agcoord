"""Select the artifact authority for an exact tagged release commit."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import sys
from typing import Any


class SelectionError(ValueError):
    """The workflow-run response cannot authorize a production release."""


def select_run_id(payload: Any, *, head_sha: str) -> int:
    if re.fullmatch(r"[0-9a-f]{40}", head_sha) is None:
        raise SelectionError("the expected release head must be exactly 40 lowercase hex")
    if not isinstance(payload, dict):
        raise SelectionError("the workflow-run response must be an object")
    runs = payload.get("workflow_runs")
    if not isinstance(runs, list):
        raise SelectionError("the workflow-run response has no workflow_runs list")

    matches: list[tuple[str, int]] = []
    for row in runs:
        if not isinstance(row, dict):
            raise SelectionError("the workflow_runs list contains a non-object row")
        if not (
            row.get("head_sha") == head_sha
            and row.get("event") == "workflow_dispatch"
            and row.get("status") == "completed"
            and row.get("conclusion") == "success"
        ):
            continue
        run_id = row.get("id")
        created_at = row.get("created_at")
        if (
            not isinstance(run_id, int)
            or isinstance(run_id, bool)
            or run_id <= 0
            or not isinstance(created_at, str)
            or not created_at
        ):
            raise SelectionError(
                "a matching successful TestPyPI run has invalid identity metadata"
            )
        matches.append((created_at, run_id))

    if not matches:
        raise SelectionError(
            f"no successful TestPyPI workflow run exists for exact head {head_sha}"
        )
    return max(matches)[1]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="select the newest successful TestPyPI run for one exact Git head"
    )
    parser.add_argument("--head-sha", required=True)
    parser.add_argument("payload", type=Path)
    args = parser.parse_args(argv)
    try:
        with args.payload.open(encoding="utf-8") as source:
            payload = json.load(source)
        run_id = select_run_id(payload, head_sha=args.head_sha)
    except (OSError, json.JSONDecodeError, SelectionError) as exc:
        print(f"Release artifact selection: FAILED — {exc}", file=sys.stderr)
        return 2
    print(run_id)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
