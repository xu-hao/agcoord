"""Real-Textual behavior for the standalone multi-repository queue view."""

from __future__ import annotations

from collections import Counter
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import re
import threading
import time

import pytest

from agcoord.queue import CoordinatorError, PROTOCOL


textual = pytest.importorskip("textual", reason="the TUI dependency is not installed")

from agcoord.tui import build_app  # noqa: E402
from textual.widgets import Button, DataTable, Static  # noqa: E402


def _row(
    run_id: str,
    sequence: int,
    status: str,
    *,
    kind: str = "check",
    label: str = "unit tests",
    repository_id: str = "repo-alpha",
    repository: str | None = None,
    head_sha: str | None = None,
    gate_run_id: str | None = None,
    publication: dict[str, object] | None = None,
    failure_reason: str | None = None,
    phase: str | None = None,
    gate_exit_status: int | None = None,
    position: int | None = None,
) -> dict[str, object]:
    created = datetime(2026, 8, 30, 12, 0, tzinfo=timezone.utc)
    started = created + timedelta(seconds=5) if status != "queued" else None
    terminal = status in {"passed", "failed", "cancelled", "interrupted"}
    finished = created + timedelta(seconds=35) if terminal else None
    selected_phase = phase or (
        "queued" if status == "queued" else ("complete" if terminal else "running")
    )
    return {
        "run_id": run_id,
        "sequence": sequence,
        "status": status,
        "kind": kind,
        "label": label,
        "agent": "agent-7",
        "repository_id": repository_id,
        "repository": repository or f"/repos/{repository_id}.git",
        "worktree_id": f"worktree-{sequence}",
        "checkout": f"/worktrees/{repository_id}/{sequence}",
        "branch": f"feature/{sequence}",
        "head_sha": head_sha,
        "barrier": kind in {"full", "merge", "land"},
        "resources": (
            {"jobs": 1, "cpu": 1, "browser": 1}
            if sequence == 1
            else {"jobs": 1, "cpu": 1}
        ),
        "blocked_by": [],
        "gate_run_id": gate_run_id,
        "publication": publication,
        "failure_reason": failure_reason,
        "phase": selected_phase,
        "gate_exit_status": gate_exit_status,
        "caller_pid": 4100 + sequence,
        "command": ["python", "-m", "pytest", "-q", f"tests/{sequence}"],
        "created_at": created.isoformat(),
        "started_at": started.isoformat() if started else None,
        "finished_at": finished.isoformat() if finished else None,
        "exit_status": 0 if status == "passed" else (1 if status == "failed" else None),
        "worker_pid": 5100 + sequence if started and not terminal else None,
        "cancel_requested": False,
        "log_bytes": 24,
        "position": position,
    }


def _snapshot() -> dict[str, object]:
    return {
        "protocol": PROTOCOL,
        "broker_pid": 4001,
        "captured_at": "2026-08-30T12:00:45+00:00",
        "capacities": {"jobs": 2, "cpu": 4, "browser": 1},
        "allocations": {"jobs": 1, "cpu": 1, "browser": 1},
        "active": [_row("check-active", 1, "running")],
        "queued": [
            _row(
                "full-waiting",
                2,
                "queued",
                kind="full",
                label="release gate",
                head_sha="a" * 40,
                position=1,
            )
        ],
        "recent": [
            _row(
                "merge-recent",
                3,
                "failed",
                kind="merge",
                label="publish change 123",
                head_sha="a" * 40,
                gate_run_id="full-waiting",
                publication={"adapter": "github", "request": 123},
                failure_reason="stale-main",
            )
        ],
    }


class FakeClient:
    def __init__(self, snapshot: dict[str, object] | None = None) -> None:
        self.result = deepcopy(snapshot or _snapshot())
        self.calls: Counter[str] = Counter()
        self.threads: dict[str, list[int]] = {}
        self.cancelled: list[str] = []
        self.transcripts: dict[str, str] = {}

    def _called(self, name: str) -> None:
        self.calls[name] += 1
        self.threads.setdefault(name, []).append(threading.get_ident())

    def snapshot(self) -> dict[str, object]:
        self._called("snapshot")
        return deepcopy(self.result)

    def log(self, run_id: str, *, offset: int = 0) -> dict[str, object]:
        self._called("log")
        text = self.transcripts.get(
            run_id,
            f"complete log for {run_id}\n",
        ).encode()
        page = text[offset:]
        return {
            "run_id": run_id,
            "offset": offset,
            "next_offset": offset + len(page),
            "text": page.decode(),
            "eof": True,
        }

    def cancel(self, run_id: str) -> dict[str, object]:
        self._called("cancel")
        self.cancelled.append(run_id)
        for group in ("active", "queued"):
            rows = self.result[group]
            assert isinstance(rows, list)
            for index, row in enumerate(rows):
                if row["run_id"] != run_id:
                    continue
                cancelled = {
                    **row,
                    "status": "cancelled",
                    "finished_at": self.result["captured_at"],
                    "exit_status": 130,
                    "cancel_requested": True,
                    "position": None,
                }
                rows.pop(index)
                recent = self.result["recent"]
                assert isinstance(recent, list)
                recent.insert(0, cancelled)
                return deepcopy(cancelled)
        raise CoordinatorError(f"unknown job {run_id}")


async def _settled(pilot) -> None:
    for _ in range(10):
        await pilot.pause()
        if not list(pilot.app.workers):
            break
        await pilot.app.workers.wait_for_complete()
    await pilot.pause()


def _table(app) -> DataTable:
    return app.query_one(DataTable)


def _table_ids(app) -> list[str]:
    known = {
        "check-active",
        "full-waiting",
        "merge-recent",
        "land-active",
        "check-new",
    }
    result: list[str] = []
    table = _table(app)
    for index in range(table.row_count):
        cells = {str(value) for value in table.get_row_at(index)}
        matches = cells & known
        assert len(matches) == 1, cells
        result.append(matches.pop())
    return result


def _headers(table: DataTable) -> list[str]:
    return [str(column.label) for column in table.columns.values()]


def _screen_text(app) -> str:
    return "\n".join(str(widget.content) for widget in app.screen.query(Static))


@pytest.mark.asyncio
async def test_queue_order_and_detail_keep_repository_resource_and_publication_identity():
    client = FakeClient()
    app = build_app(lambda: client, refresh_interval=60)
    async with app.run_test(size=(80, 24)) as pilot:
        await _settled(pilot)

        table = _table(app)
        assert _headers(table) == ["STATE", "KIND", "REPO", "RUN", "LABEL", "AGE", "DUR"]
        assert table.max_scroll_x == 0
        assert _table_ids(app) == ["check-active", "full-waiting", "merge-recent"]
        selected = _screen_text(app)
        for fact in (
            "check-active",
            "repo-alpha",
            "/repos/repo-alpha.git",
            "/worktrees/repo-alpha/1",
            "agent-7",
            "cpu",
            "browser",
            "2026-08-30T12:00:00+00:00",
            "2026-08-30T12:00:05+00:00",
        ):
            assert fact in selected

        await pilot.press("down", "down", "enter")
        await pilot.pause()
        merge_detail = _screen_text(app)
        for fact in (
            "merge-recent",
            "github",
            "123",
            "full-waiting",
            "a" * 40,
            "stale-main",
        ):
            assert fact in merge_detail


@pytest.mark.asyncio
async def test_rendered_table_separates_columns_and_marks_truncated_detail_value():
    full_label = "LONGLABEL-abcdefghijklmnopqrstuvwxyz"
    snapshot = _snapshot()
    snapshot["active"] = [
        _row(
            "check-0123456789",
            1,
            "running",
            kind="check",
            label=full_label,
            repository_id="repo-123456",
        )
    ]
    snapshot["queued"] = []
    snapshot["recent"] = []
    client = FakeClient(snapshot)
    app = build_app(lambda: client, refresh_interval=60)

    async with app.run_test(size=(80, 24)) as pilot:
        await _settled(pilot)

        table = _table(app)
        rendered_row = table.render_line(table.header_height).text
        has_column_gutter = bool(
            re.search(r"\bcheck +repo-123456\b", rendered_row)
        )
        has_label_ellipsis = bool(
            re.search(r"LONGLABEL-\S*… +45s\b", rendered_row)
        )
        assert full_label not in rendered_row

        await pilot.press("enter")
        await pilot.pause()
        assert full_label in _screen_text(app)
        assert (has_column_gutter, has_label_ellipsis) == (True, True), rendered_row


@pytest.mark.asyncio
async def test_repository_table_and_filter_show_remote_and_local_names_not_internal_ids():
    snapshot = _snapshot()
    snapshot["active"] = [
        _row(
            "remote-active",
            4,
            "running",
            repository_id="repo-remote-identity",
            repository="github.com/example/widgets",
        ),
        _row(
            "local-active",
            5,
            "running",
            repository_id="repo-local-identity",
            repository="/srv/projects/widgets/.git",
        ),
    ]
    snapshot["queued"] = []
    snapshot["recent"] = []
    client = FakeClient(snapshot)
    app = build_app(lambda: client, refresh_interval=60)

    async with app.run_test(size=(100, 30)) as pilot:
        await _settled(pilot)
        table = _table(app)
        assert [str(table.get_row_at(index)[2]) for index in range(table.row_count)] == [
            "widgets",
            "widgets",
        ]

        await pilot.press("p")
        await pilot.pause()
        assert table.row_count == 1
        assert str(table.get_row_at(0)[2]) == "widgets"
        subject = str(app.query_one("#gate-subject", Static).content)
        assert "/srv/projects/widgets/.git" in subject
        assert "repo-local-identity" not in subject

        await pilot.press("p")
        await pilot.pause()
        assert table.row_count == 1
        assert str(table.get_row_at(0)[2]) == "widgets"
        subject = str(app.query_one("#gate-subject", Static).content)
        assert "github.com/example/widgets" in subject
        assert "repo-remote-identity" not in subject


@pytest.mark.asyncio
async def test_history_toggle_is_non_destructive_cached_and_discoverable():
    client = FakeClient()
    app = build_app(lambda: client, refresh_interval=60)
    async with app.run_test(size=(100, 30)) as pilot:
        await _settled(pilot)
        reads = client.calls["snapshot"]

        await pilot.press("h")
        await pilot.pause()
        assert _table_ids(app) == ["check-active", "full-waiting"]
        assert client.calls["snapshot"] == reads
        assert len(client.result["recent"]) == 1
        hidden = _screen_text(app).lower()
        assert "history hidden" in hidden and "h show" in hidden

        await pilot.press("?")
        await pilot.pause()
        assert "show completed history" in _screen_text(app).lower()
        await pilot.press("escape")
        await _settled(pilot)

        await pilot.press("h")
        await pilot.pause()
        assert _table_ids(app) == ["check-active", "full-waiting", "merge-recent"]
        assert client.calls["snapshot"] == reads
        shown = _screen_text(app).lower()
        assert "history shown" in shown and "h hide" in shown



@pytest.mark.asyncio
async def test_refresh_preserves_selected_job_and_viewport_when_it_still_exists():
    snapshot = _snapshot()
    snapshot["active"] = []
    snapshot["queued"] = []
    snapshot["recent"] = [
        _row(
            f"check-{number:02d}",
            number,
            "passed",
            label=f"wide row {number} " + "label-" * 12,
            repository_id=f"repo-{number % 3}",
        )
        for number in range(40)
    ]
    known_ids = {row["run_id"] for row in snapshot["recent"]}
    client = FakeClient(snapshot)
    app = build_app(lambda: client, refresh_interval=60)

    def ids() -> list[str]:
        table = _table(app)
        result = []
        for index in range(table.row_count):
            cells = {str(value) for value in table.get_row_at(index)}
            result.append((cells & known_ids).pop())
        return result

    async with app.run_test(size=(42, 18)) as pilot:
        await _settled(pilot)
        table = _table(app)
        target = "check-20"
        table.move_cursor(row=ids().index(target))
        table.scroll_to(
            x=min(6, table.max_scroll_x),
            y=min(7, table.max_scroll_y),
            animate=False,
            force=True,
            immediate=True,
        )
        await pilot.pause()
        before = (table.scroll_x, table.scroll_y)
        assert before[0] > 0 and before[1] > 0

        changed = deepcopy(snapshot)
        changed["active"] = [_row("check-new", 100, "running", repository_id="repo-new")]
        changed["captured_at"] = "2026-08-30T12:01:00+00:00"
        client.result = changed
        known_ids.add("check-new")
        await pilot.press("r")
        await _settled(pilot)

        assert ids()[table.cursor_row] == target
        assert (table.scroll_x, table.scroll_y) == before


@pytest.mark.asyncio
async def test_snapshot_log_and_cancel_io_run_off_the_textual_event_loop():
    client = FakeClient()
    event_loop_thread = threading.get_ident()
    app = build_app(lambda: client, refresh_interval=60)
    async with app.run_test(size=(100, 30)) as pilot:
        await _settled(pilot)
        assert client.threads["snapshot"]
        assert all(thread != event_loop_thread for thread in client.threads["snapshot"])

        await pilot.press("l")
        await _settled(pilot)
        assert "complete log for check-active" in _screen_text(app)
        assert all(thread != event_loop_thread for thread in client.threads["log"])

        await pilot.press("escape", "down", "c")
        await pilot.pause()
        buttons = list(app.screen.query(Button))
        assert any("keep job" in str(button.label).lower() for button in buttons)
        keep = next(button for button in buttons if "keep job" in str(button.label).lower())
        assert keep.has_focus
        await pilot.press("y")
        await _settled(pilot)
        assert client.cancelled == ["full-waiting"]
        assert all(thread != event_loop_thread for thread in client.threads["cancel"])


@pytest.mark.asyncio
async def test_running_publication_explains_that_only_queued_merge_is_cancellable():
    snapshot = _snapshot()
    snapshot["active"] = [
        _row(
            "merge-recent",
            3,
            "running",
            kind="merge",
            label="publish change 123",
            head_sha="a" * 40,
            gate_run_id="full-waiting",
            publication={"adapter": "github", "request": 123},
        )
    ]
    snapshot["queued"] = []
    snapshot["recent"] = []
    client = FakeClient(snapshot)
    app = build_app(lambda: client, refresh_interval=60)
    async with app.run_test(size=(100, 30)) as pilot:
        await _settled(pilot)
        assert "cancellable only while queued" in _screen_text(app).lower()
        await pilot.press("c")
        await pilot.pause()
        assert client.calls["cancel"] == 0
        assert not app.screen.query(Button)


@pytest.mark.asyncio
async def test_land_remains_one_selected_run_across_gate_publish_and_transcript():
    snapshot = _snapshot()
    snapshot["active"] = [
        _row(
            "land-active",
            4,
            "running",
            kind="land",
            label="gate and publish change 123",
            head_sha="b" * 40,
            publication={"adapter": "github", "request": 123},
            phase="gating",
        )
    ]
    snapshot["queued"] = []
    snapshot["recent"] = []
    client = FakeClient(snapshot)
    client.transcripts["land-active"] = (
        "gate transcript: all checks passed\n"
        "publication transcript: atomic refs updated\n"
    )
    app = build_app(lambda: client, refresh_interval=60)

    async with app.run_test(size=(100, 30)) as pilot:
        await _settled(pilot)
        assert _table_ids(app) == ["land-active"]
        gating = _screen_text(app).lower()
        assert "phase: gating" in gating
        assert "gate exit: —" in gating
        assert "github" in gating and "123" in gating

        await pilot.press("enter")
        await pilot.pause()
        detail = _screen_text(app).lower()
        assert "land-active" in detail
        assert "phase: gating" in detail
        assert "gate exit: —" in detail
        await pilot.press("escape")
        await _settled(pilot)

        publishing = deepcopy(snapshot)
        publishing["captured_at"] = "2026-08-30T12:01:00+00:00"
        publishing["active"][0]["phase"] = "publishing"
        publishing["active"][0]["gate_exit_status"] = 0
        client.result = publishing
        await pilot.press("r")
        await _settled(pilot)

        assert _table_ids(app) == ["land-active"]
        selected = _screen_text(app).lower()
        assert "phase: publishing" in selected
        assert "gate exit: 0" in selected
        await pilot.press("l")
        await _settled(pilot)
        transcript = _screen_text(app).lower()
        assert "gate transcript: all checks passed" in transcript
        assert "publication transcript: atomic refs updated" in transcript
        await pilot.press("escape")
        await _settled(pilot)

        await pilot.press("c")
        await pilot.pause()
        assert client.calls["cancel"] == 0
        assert not app.screen.query(Button)
        refusal = _screen_text(app).lower()
        assert "publishing" in refusal and "cannot be cancelled" in refusal
