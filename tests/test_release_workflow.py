"""The production release promotes one exact successful TestPyPI artifact set."""

from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]
SELECTOR = ROOT / "tools" / "select_testpypi_run.py"
HEAD = "a" * 40


def _select(tmp_path: Path, runs: list[dict[str, object]]) -> subprocess.CompletedProcess[str]:
    payload = tmp_path / "runs.json"
    payload.write_text(json.dumps({"workflow_runs": runs}), encoding="utf-8")
    return subprocess.run(
        [sys.executable, str(SELECTOR), "--head-sha", HEAD, str(payload)],
        check=False,
        capture_output=True,
        text=True,
    )


def _run(run_id: int, **overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "id": run_id,
        "head_sha": HEAD,
        "event": "workflow_dispatch",
        "status": "completed",
        "conclusion": "success",
        "created_at": f"2026-08-30T12:{run_id:02d}:00Z",
    }
    row.update(overrides)
    return row


def test_selector_returns_the_newest_successful_dispatch_for_the_exact_head(tmp_path: Path):
    result = _select(
        tmp_path,
        [
            _run(7),
            _run(8, head_sha="b" * 40),
            _run(9, conclusion="failure"),
            _run(10, event="push"),
            _run(11),
        ],
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "11"


def test_selector_fails_closed_without_a_matching_successful_testpypi_run(tmp_path: Path):
    result = _select(
        tmp_path,
        [
            _run(12, head_sha="b" * 40),
            _run(13, status="in_progress", conclusion=None),
        ],
    )

    assert result.returncode != 0
    assert "no successful testpypi workflow run" in result.stderr.lower()
    assert result.stdout == ""
