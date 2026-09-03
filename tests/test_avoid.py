"""Behavioral contract for the stored set of commits landing must never publish."""

from __future__ import annotations

from io import StringIO
import json
from pathlib import Path
import stat

import pytest

from agcoord import cli
from agcoord.avoid import (
    AVOID_FILENAME,
    add_avoided,
    load_avoided,
    reachable_avoided,
    resolve_avoid_commits,
)
from agcoord.queue import LAND_AVOID_ENV, CoordinatorError

from conftest import RunningCoordinator

SHA_A = "a" * 40
SHA_B = "b" * 40


def _avoid(*arguments: str, state_dir: Path) -> tuple[int, str]:
    out = StringIO()
    args = cli.build_parser().parse_args(["--state-dir", str(state_dir), "avoid", *arguments])
    return cli.run(args, out=out), out.getvalue()


def _avoid_json(*arguments: str, state_dir: Path) -> object:
    out = StringIO()
    args = cli.build_parser().parse_args(
        ["--state-dir", str(state_dir), "--json", "avoid", *arguments]
    )
    assert cli.run(args, out=out) == 0
    return json.loads(out.getvalue())


def test_avoid_stores_lists_and_removes_commits_in_an_owner_only_file(tmp_path: Path):
    state_dir = tmp_path / "state"

    status, text = _avoid(SHA_A, "--reason", "removed from main", state_dir=state_dir)
    assert status == 0
    assert SHA_A in text and "refuses to publish" in text
    path = state_dir / AVOID_FILENAME
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert stat.S_IMODE(state_dir.stat().st_mode) == 0o700

    again = _avoid_json(SHA_A, state_dir=state_dir)
    assert again["added"] is False and again["reason"] == "removed from main"

    listed = _avoid_json("--list", state_dir=state_dir)
    assert [entry["sha"] for entry in listed["commits"]] == [SHA_A]
    assert listed["commits"][0]["reason"] == "removed from main"
    assert listed["commits"][0]["added_at"]

    status, text = _avoid(state_dir=state_dir)
    assert status == 0 and SHA_A in text and "removed from main" in text

    removed = _avoid_json("--remove", SHA_A, state_dir=state_dir)
    assert removed == {"sha": SHA_A, "removed": True}
    assert _avoid_json("--remove", SHA_A, state_dir=state_dir)["removed"] is False
    assert _avoid_json("--list", state_dir=state_dir) == {"commits": []}


@pytest.mark.parametrize(
    "arguments",
    [
        ["not-a-sha"],
        [SHA_A[:39]],
        [SHA_A, "--list"],
        [SHA_A, "--remove", SHA_B],
        ["--remove", "xyz"],
    ],
)
def test_avoid_refuses_invalid_input_without_writing(tmp_path: Path, arguments: list[str]):
    state_dir = tmp_path / "state"
    with pytest.raises(CoordinatorError):
        _avoid(*arguments, state_dir=state_dir)
    assert not (state_dir / AVOID_FILENAME).exists()


def test_avoid_refuses_a_malformed_file_instead_of_guessing(tmp_path: Path):
    state_dir = tmp_path / "state"
    state_dir.mkdir(mode=0o700)
    (state_dir / AVOID_FILENAME).write_text('{"commits": [{"sha": "short"}]}', encoding="utf-8")
    with pytest.raises(CoordinatorError, match="not a valid avoided-commit file"):
        load_avoided(state_dir)
    with pytest.raises(CoordinatorError, match="not a valid avoided-commit file"):
        add_avoided(state_dir, SHA_A)


def test_avoided_set_survives_clear_and_broker_restart(tmp_path: Path):
    state_dir = tmp_path / "state"
    running = RunningCoordinator(state_dir, capacities={"jobs": 1})
    client = running.start()
    try:
        add_avoided(state_dir, SHA_A, reason="removed from main")
        client.clear()
        assert [entry["sha"] for entry in load_avoided(state_dir)] == [SHA_A]
    finally:
        running.stop()

    restarted = RunningCoordinator(state_dir, capacities={"jobs": 1})
    restarted.start()
    try:
        assert [entry["sha"] for entry in load_avoided(state_dir)] == [SHA_A]
    finally:
        restarted.stop()


def test_resolve_unions_the_stored_set_with_one_off_commits(tmp_path: Path):
    state_dir = tmp_path / "state"
    add_avoided(state_dir, SHA_A, reason="removed from main")
    resolved = resolve_avoid_commits(state_dir, {LAND_AVOID_ENV: f"{SHA_B},{SHA_A.upper()}"})
    assert resolved == {SHA_A: "removed from main", SHA_B: "requested for this landing"}
    assert resolve_avoid_commits(None, {}) == {}
    with pytest.raises(CoordinatorError, match="40-character"):
        resolve_avoid_commits(state_dir, {LAND_AVOID_ENV: "nope"})


def test_reachability_distinguishes_reachable_unreachable_and_unknown(tmp_path: Path):
    import subprocess

    checkout = tmp_path / "checkout"
    subprocess.run(["git", "init", "-q", "-b", "main", str(checkout)], check=True)
    for key, value in (("user.name", "AGCoord test"), ("user.email", "avoid@example.invalid")):
        subprocess.run(["git", "-C", str(checkout), "config", key, value], check=True)
    (checkout / "a.txt").write_text("a\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(checkout), "add", "a.txt"], check=True)
    subprocess.run(["git", "-C", str(checkout), "commit", "-q", "-m", "first"], check=True)
    first = subprocess.run(
        ["git", "-C", str(checkout), "rev-parse", "HEAD"], check=True, capture_output=True, text=True
    ).stdout.strip()
    tree = subprocess.run(
        ["git", "-C", str(checkout), "rev-parse", "HEAD^{tree}"],
        check=True, capture_output=True, text=True,
    ).stdout.strip()
    dangling = subprocess.run(
        ["git", "-C", str(checkout), "commit-tree", tree, "-m", "dangling"],
        check=True, capture_output=True, text=True,
    ).stdout.strip()

    reachable, unknown = reachable_avoided(
        checkout, "HEAD", {first: "r", dangling: "d", SHA_B: "u"}
    )
    assert reachable == [first]
    assert unknown == [SHA_B]
