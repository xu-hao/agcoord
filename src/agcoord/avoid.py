"""Commits that landing must never publish again, stored beside the spool's configuration.

After a deliberate rewrite of the target branch, any request branch that still reaches a
removed commit brings it back when merged. The operator who performed the rewrite records
the removed commits once; every later landing on this machine refuses to publish anything
that reaches one of them. The set lives in an owner-only file next to ``config.json`` so it
needs no broker, survives ``agc clear`` and broker restarts, and travels with the state
directory through migration and rollback.
"""

from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
from typing import Any, Mapping, Sequence

from .queue import LAND_AVOID_ENV, CoordinatorError

AVOID_FILENAME = "avoid.json"
MAX_REASON = 200
_SHA = re.compile(r"^[0-9a-f]{40}$")


def avoid_path(state_dir: str | os.PathLike[str]) -> Path:
    """Return the avoided-commit file that belongs to one state directory."""
    return Path(state_dir).expanduser() / AVOID_FILENAME


def validate_sha(value: object) -> str:
    if not isinstance(value, str) or _SHA.fullmatch(value.strip().lower()) is None:
        raise CoordinatorError(
            "an avoided commit must be one full 40-character hexadecimal SHA",
            code="avoid-sha-invalid",
        )
    return value.strip().lower()


def _validate_reason(value: object) -> str:
    if not isinstance(value, str) or "\0" in value or len(value) > MAX_REASON:
        raise CoordinatorError(
            f"an avoid reason must be text of at most {MAX_REASON} characters",
            code="avoid-reason-invalid",
        )
    return value.strip()


def load_avoided(state_dir: str | os.PathLike[str]) -> list[dict[str, str]]:
    """Read the stored set, or the empty set when the file is absent."""
    path = avoid_path(state_dir)
    try:
        raw = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return []
    except OSError as exc:
        raise CoordinatorError(f"cannot read {path}: {exc}", code="avoid-file-invalid") from exc
    try:
        document: Any = json.loads(raw)
        entries = document["commits"]
        assert isinstance(entries, list)
        selected: list[dict[str, str]] = []
        seen: set[str] = set()
        for entry in entries:
            sha = validate_sha(entry["sha"])
            if sha in seen:
                raise ValueError("duplicate")
            seen.add(sha)
            selected.append(
                {
                    "sha": sha,
                    "reason": _validate_reason(entry.get("reason", "")),
                    "added_at": str(entry["added_at"]),
                }
            )
        return selected
    except (AssertionError, CoordinatorError, KeyError, TypeError, ValueError) as exc:
        raise CoordinatorError(
            f"{path} is not a valid avoided-commit file; repair or remove it",
            code="avoid-file-invalid",
        ) from exc


def _write(state_dir: str | os.PathLike[str], entries: Sequence[Mapping[str, str]]) -> None:
    path = avoid_path(state_dir)
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    document = json.dumps({"commits": list(entries)}, indent=2, sort_keys=True) + "\n"
    handle, temporary = tempfile.mkstemp(prefix=".avoid.", dir=path.parent)
    try:
        with os.fdopen(handle, "w", encoding="utf-8") as stream:
            stream.write(document)
        os.chmod(temporary, 0o600)
        os.replace(temporary, path)
    except OSError as exc:
        try:
            os.unlink(temporary)
        except OSError:
            pass
        raise CoordinatorError(f"cannot write {path}: {exc}", code="avoid-file-invalid") from exc


def add_avoided(
    state_dir: str | os.PathLike[str],
    sha: object,
    *,
    reason: object = "",
) -> dict[str, Any]:
    """Store one commit; storing an already avoided commit changes nothing."""
    selected = validate_sha(sha)
    selected_reason = _validate_reason(reason)
    entries = load_avoided(state_dir)
    for entry in entries:
        if entry["sha"] == selected:
            return {**entry, "added": False}
    entry = {
        "sha": selected,
        "reason": selected_reason,
        "added_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    _write(state_dir, [*entries, entry])
    return {**entry, "added": True}


def remove_avoided(state_dir: str | os.PathLike[str], sha: object) -> dict[str, Any]:
    selected = validate_sha(sha)
    entries = load_avoided(state_dir)
    remaining = [entry for entry in entries if entry["sha"] != selected]
    if len(remaining) != len(entries):
        _write(state_dir, remaining)
    return {"sha": selected, "removed": len(remaining) != len(entries)}


def resolve_avoid_commits(
    state_dir: str | os.PathLike[str] | None,
    environment: Mapping[str, str],
) -> dict[str, str]:
    """Union the stored set with one-off commits carried in the admitted environment."""
    selected: dict[str, str] = {}
    if state_dir is not None:
        for entry in load_avoided(state_dir):
            selected[entry["sha"]] = entry["reason"] or "stored"
    raw = environment.get(LAND_AVOID_ENV, "")
    for item in filter(None, raw.split(",")):
        selected.setdefault(validate_sha(item), "requested for this landing")
    return dict(sorted(selected.items()))


def _git(checkout: Path, *arguments: str) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            ["git", "-C", str(checkout), *arguments],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
    except OSError as exc:
        raise CoordinatorError(f"cannot run git: {exc}", code="avoid-git-failed") from exc


def reachable_avoided(
    checkout: str | os.PathLike[str],
    revision: str,
    avoided: Mapping[str, str],
) -> tuple[list[str], list[str]]:
    """Return the avoided commits reachable from ``revision`` and those unknown locally.

    An avoided commit the repository does not have cannot be reached by anything in it,
    so it is reported separately rather than treated as an error.
    """
    selected = Path(checkout).expanduser().resolve()
    reachable: list[str] = []
    unknown: list[str] = []
    for sha in avoided:
        if _git(selected, "cat-file", "-e", f"{sha}^{{commit}}").returncode != 0:
            unknown.append(sha)
            continue
        result = _git(selected, "merge-base", "--is-ancestor", sha, revision)
        if result.returncode == 0:
            reachable.append(sha)
        elif result.returncode not in {1, 128}:
            detail = result.stderr.strip() or f"exit {result.returncode}"
            raise CoordinatorError(
                f"cannot compare ancestry against avoided commit {sha}: {detail}",
                code="avoid-git-failed",
            )
    return reachable, unknown
