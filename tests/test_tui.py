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
from textual.widgets import Button, DataTable, Input, OptionList, Static  # noqa: E402


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
    resource_receipt: dict[str, object] | None = None,
) -> dict[str, object]:
    created = datetime(2026, 8, 30, 12, 0, tzinfo=timezone.utc)
    started = created + timedelta(seconds=5) if status != "queued" else None
    terminal = status in {"passed", "failed", "cancelled", "interrupted"}
    finished = created + timedelta(seconds=35) if terminal else None
    selected_phase = phase or (
        "queued" if status == "queued" else ("complete" if terminal else "running")
    )
    resources = (
        {"jobs": 1, "cpu": 1, "browser": 1}
        if sequence == 1
        else {"jobs": 1, "cpu": 1}
    )
    selected_receipt = resource_receipt or {
        "requested": resources,
        "applied": {},
        "peak": {},
        "events": [],
    }
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
        "barrier": kind in {"merge", "land"},
        "resources": resources,
        "resource_contract": {
            name: {
                "backend": None,
                "kind": "generic",
                "mode": "admission-only",
                "unit": "admission-unit",
            }
            for name in resources
        },
        "resource_receipt": selected_receipt,
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
        "resource_bindings": {},
        "resource_capabilities": {},
        "maintenance": None,
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


@pytest.mark.asyncio
async def test_durable_maintenance_state_is_visible_without_a_broker():
    snapshot = _snapshot()
    snapshot["broker_pid"] = None
    snapshot["active"] = []
    snapshot["queued"] = []
    snapshot["maintenance"] = {
        "state": "drained",
        "drain_id": "drain-0123456789ab",
        "reason": "native host upgrade",
        "started_at": "2026-08-30T12:00:40+00:00",
        "protocol": PROTOCOL,
        "live": 0,
        "broker_pid": None,
    }
    app = build_app(lambda: FakeClient(snapshot), refresh_interval=60)

    async with app.run_test(size=(100, 24)) as pilot:
        await _settled(pilot)
        screen = _screen_text(app)
        assert "drained drain-0123456789ab" in screen
        assert "native host upgrade" in screen
        assert "broker none" in screen


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
    return "\n".join(_widget_text(widget) for widget in app.screen.query(Static))


def _widget_text(widget: Static) -> str:
    return str(widget.content)


@pytest.mark.asyncio
async def test_queue_order_and_detail_keep_repository_resource_and_publication_identity():
    client = FakeClient()
    app = build_app(lambda: client, refresh_interval=60)
    async with app.run_test(size=(80, 24)) as pilot:
        await _settled(pilot)

        table = _table(app)
        assert _headers(table) == [
            "STATE",
            "KIND",
            "REPO",
            "RUN",
            "BRANCH",
            "LABEL",
            "AGE",
            "DUR",
        ]
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
            "admission-only",
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
async def test_resource_enforcement_failure_is_inspectable_without_backend_paths():
    snapshot = _snapshot()
    row = _row("check-resource-failed", 1, "failed")
    row["resource_contract"]["cpu"] = {
        "backend": "cgroup-v2",
        "kind": "cpu",
        "mode": "required",
        "unit": "logical-cpu",
    }
    row["resource_receipt"] = {
        "requested": row["resources"],
        "applied": {},
        "peak": {},
        "events": [
            {
                "at": "2026-08-30T12:00:01+00:00",
                "backend": "cgroup-v2",
                "resource": "cpu",
                "stage": "probe",
                "status": "failed",
                "code": "backend-unavailable",
            }
        ],
    }
    snapshot["active"] = []
    snapshot["queued"] = []
    snapshot["recent"] = [row]
    snapshot["resource_bindings"] = {"cpu": row["resource_contract"]["cpu"]}
    snapshot["resource_capabilities"] = {
        "cgroup-v2": {
            "available": False,
            "kinds": [],
            "units": [],
            "operations": [],
            "reason": "backend-unavailable",
        }
    }
    app = build_app(lambda: FakeClient(snapshot), refresh_interval=60)

    async with app.run_test(size=(100, 30)) as pilot:
        await _settled(pilot)
        selected = _screen_text(app)
        assert "enforcement: failed" in selected
        await pilot.press("enter")
        await pilot.pause()
        detail = _screen_text(app)
        assert "backend-unavailable" in detail
        assert "/sys/fs/cgroup" not in detail


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
            re.search(
                r"\bcheck +repo-\S*… +check-\S*… +feature/1 +LONGLABEL-\S*…",
                rendered_row,
            )
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
async def test_default_width_long_queue_keeps_gutters_without_horizontal_scrollbar():
    snapshot = _snapshot()
    snapshot["active"] = []
    snapshot["queued"] = []
    snapshot["recent"] = [
        _row(
            f"check-scroll-{number:02d}",
            number,
            "passed",
            label=f"completed check {number}",
        )
        for number in range(30)
    ]
    app = build_app(lambda: FakeClient(snapshot), refresh_interval=60)

    async with app.run_test(size=(80, 24)) as pilot:
        await _settled(pilot)

        table = _table(app)
        rendered_row = table.render_line(table.header_height).text
        assert table.show_vertical_scrollbar
        assert re.search(r"\bpassed +check\b", rendered_row), rendered_row
        assert not table.show_horizontal_scrollbar, (
            f"size={table.size!r} virtual={table.virtual_size!r} "
            f"gutter={table.scrollbar_gutter!r} "
            f"scrollbar_space={table.scrollbars_space!r} "
            f"max_scroll_x={table.max_scroll_x}"
        )
        assert table.max_scroll_x == 0


@pytest.mark.asyncio
async def test_branch_and_label_use_wide_space_and_return_to_compact_ellipsis():
    full_branch = "feature/stable-work-context"
    full_label = "integration release validation"
    snapshot = _snapshot()
    row = _row("check-wide", 1, "running", label=full_label)
    row["branch"] = full_branch
    snapshot["active"] = [row]
    snapshot["queued"] = []
    snapshot["recent"] = []
    app = build_app(lambda: FakeClient(snapshot), refresh_interval=60)

    async with app.run_test(size=(120, 24)) as pilot:
        await _settled(pilot)

        table = _table(app)
        assert _headers(table) == [
            "STATE",
            "KIND",
            "REPO",
            "RUN",
            "BRANCH",
            "LABEL",
            "AGE",
            "DUR",
        ]
        wide_row = table.render_line(table.header_height).text
        assert full_branch in wide_row
        assert full_label in wide_row
        assert table.max_scroll_x == 0

        await pilot.resize_terminal(80, 24)
        await _settled(pilot)

        compact_row = table.render_line(table.header_height).text
        assert full_branch not in compact_row
        assert full_label not in compact_row
        assert re.search(
            r"feature/\S*… +integr\S*… +45s\b", compact_row
        ), compact_row
        assert table.max_scroll_x == 0


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
        options = app.screen.query_one("#filter-options", OptionList)
        prompts = [
            str(options.get_option_at_index(index).prompt)
            for index in range(options.option_count)
        ]
        assert prompts == [
            "All repositories",
            "/srv/projects/widgets/.git",
            "github.com/example/widgets",
        ]
        assert "repo-local-identity" not in prompts
        assert "repo-remote-identity" not in prompts
        for key in "/srv":
            await pilot.press(key)
        await pilot.press("enter")
        await pilot.pause()
        assert table.row_count == 1
        assert str(table.get_row_at(0)[2]) == "widgets"
        subject = _widget_text(app.query_one("#gate-subject", Static))
        assert "/srv/projects/widgets/.git" in subject
        assert "repo-local-identity" not in subject

        await pilot.press("p")
        await pilot.pause()
        for key in "github":
            await pilot.press(key)
        await pilot.press("enter")
        await pilot.pause()
        assert table.row_count == 1
        assert str(table.get_row_at(0)[2]) == "widgets"
        subject = _widget_text(app.query_one("#gate-subject", Static))
        assert "github.com/example/widgets" in subject
        assert "repo-remote-identity" not in subject


@pytest.mark.asyncio
async def test_repository_and_agent_filters_use_searchable_cancelable_picker_menus():
    snapshot = _snapshot()
    rows = []
    for index in range(50):
        repository = f"github.com/example/project-{index:03d}"
        agent = f"agent-{index:03d}"
        if index == 37:
            repository = "github.com/example/needle-project"
            agent = "agent-special"
        row = _row(
            f"check-{index:03d}",
            index + 1,
            "running",
            repository_id=f"repo-{index:03d}",
            repository=repository,
        )
        row["agent"] = agent
        rows.append(row)
    snapshot["active"] = rows
    snapshot["queued"] = []
    snapshot["recent"] = []
    app = build_app(lambda: FakeClient(snapshot), refresh_interval=60)

    async with app.run_test(size=(100, 30)) as pilot:
        await _settled(pilot)

        await pilot.press("p")
        await pilot.pause()
        query = app.screen.query_one("#filter-query", Input)
        options = app.screen.query_one("#filter-options", OptionList)
        assert query.has_focus
        assert str(options.get_option_at_index(0).prompt) == "All repositories"
        for key in "needle":
            await pilot.press(key)
        await pilot.pause()
        assert options.option_count == 2
        assert str(options.get_option_at_index(1).prompt) == (
            "github.com/example/needle-project"
        )
        await pilot.press("enter")
        await pilot.pause()
        assert _table(app).row_count == 1
        assert "github.com/example/needle-project" in _screen_text(app)

        await pilot.press("p")
        await pilot.pause()
        for key in "project-001":
            await pilot.press(key)
        await pilot.press("escape")
        await pilot.pause()
        assert _table(app).row_count == 1
        assert "github.com/example/needle-project" in _screen_text(app)

        await pilot.press("a")
        await pilot.pause()
        query = app.screen.query_one("#filter-query", Input)
        options = app.screen.query_one("#filter-options", OptionList)
        assert query.has_focus
        assert str(options.get_option_at_index(0).prompt) == "All agents"
        for key in "special":
            await pilot.press(key)
        await pilot.press("enter")
        await pilot.pause()
        assert _table(app).row_count == 1
        assert "agent-special" in _screen_text(app)

        await pilot.press("p")
        await pilot.pause()
        await pilot.press("tab", "home", "enter")
        await pilot.pause()
        assert _table(app).row_count == 1
        assert "all repos" in _screen_text(app)
        assert "agent-special" in _screen_text(app)

        await pilot.press("a")
        await pilot.pause()
        await pilot.press("tab", "home", "enter")
        await pilot.pause()
        assert _table(app).row_count == 50
        assert "all agents" in _screen_text(app)


@pytest.mark.asyncio
async def test_agent_picker_omits_legacy_pid_fallbacks_and_keeps_unnamed():
    snapshot = _snapshot()
    rows = []
    for index, agent in enumerate(
        ("pid:4101", "pid:4102", "unnamed", "agent-special"),
        start=1,
    ):
        row = _row(f"check-agent-{index}", index, "running")
        row["agent"] = agent
        rows.append(row)
    snapshot["active"] = rows
    snapshot["queued"] = []
    snapshot["recent"] = []
    app = build_app(lambda: FakeClient(snapshot), refresh_interval=60)

    async with app.run_test(size=(100, 30)) as pilot:
        await _settled(pilot)
        await pilot.press("a")
        await pilot.pause()

        options = app.screen.query_one("#filter-options", OptionList)
        prompts = [
            str(options.get_option_at_index(index).prompt)
            for index in range(options.option_count)
        ]
        assert prompts == ["All agents", "agent-special", "unnamed"]

        for key in "unnamed":
            await pilot.press(key)
        await pilot.press("enter")
        await pilot.pause()
        assert _table(app).row_count == 1
        assert "unnamed" in _screen_text(app)


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
async def test_refresh_preserves_selected_job_and_viewport_without_scroll_snapback():
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
        vertical_offsets: list[tuple[float, float]] = []
        app.watch(
            table,
            "scroll_y",
            lambda old, new: vertical_offsets.append((old, new)),
            init=False,
        )

        await pilot.press("r")
        await _settled(pilot)
        assert vertical_offsets == []

        changed = deepcopy(snapshot)
        changed["active"] = [_row("check-new", 100, "running", repository_id="repo-new")]
        changed["captured_at"] = "2026-08-30T12:01:00+00:00"
        client.result = changed
        known_ids.add("check-new")
        await pilot.press("r")
        await _settled(pilot)

        assert ids()[table.cursor_row] == target
        assert (table.scroll_x, table.scroll_y) == before
        assert all(new > 0 for _old, new in vertical_offsets), vertical_offsets


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
