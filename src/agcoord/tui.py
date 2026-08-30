"""A credential-free terminal view of all AGCoord machine work."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import json
import shlex
import threading
from typing import Callable

from .queue import CoordinatorError, CoordinatorClient, TERMINAL_STATUSES
from . import frame

try:
    from textual.app import App, ComposeResult
    from textual.binding import Binding
    from textual.containers import Horizontal, Vertical, VerticalScroll
    from textual.screen import ModalScreen
    from textual.widgets import Button, DataTable, Footer, Label, Static
except ImportError:  # pragma: no cover - the package declares Textual; CLI guard is tested
    App = None


_MISSING = (
    "the gate TUI needs Textual — install agcoord\n"
    "(the scriptable view is: python -m agcoord list)"
)


async def _off_loop(operation: Callable[[], object]) -> object:
    """Run bounded coordinator I/O without owning the event loop's default executor."""
    loop = asyncio.get_running_loop()
    future: asyncio.Future[object] = loop.create_future()

    def deliver(value: object = None, error: BaseException | None = None) -> None:
        if future.done():
            return
        if error is None:
            future.set_result(value)
        else:
            future.set_exception(error)

    def invoke() -> None:
        try:
            value = operation()
        except BaseException as exc:
            try:
                loop.call_soon_threadsafe(deliver, None, exc)
            except RuntimeError:
                pass
        else:
            try:
                loop.call_soon_threadsafe(deliver, value, None)
            except RuntimeError:
                pass

    threading.Thread(target=invoke, name="agcoord-tui-io", daemon=True).start()
    return await future


def _moment(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None


def _elapsed(start: datetime | None, end: datetime | None) -> str:
    if start is None:
        return "—"
    if end is None:
        return "—"
    seconds = max(0, round((end - start).total_seconds()))
    if seconds < 60:
        return f"{seconds}s"
    minutes, seconds = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes}m {seconds:02d}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h {minutes:02d}m"


def _age(row: dict, now: datetime | None = None) -> str:
    now = now or datetime.now(timezone.utc)
    return _elapsed(_moment(row["created_at"]), now)


def _duration(row: dict, now: datetime | None = None) -> str:
    started = _moment(row["started_at"])
    if started is None:
        return "—"
    now = now or datetime.now(timezone.utc)
    return _elapsed(started, _moment(row["finished_at"]) or now)


def _detail(row: dict | None) -> str:
    if row is None:
        return "no coordinated runs"
    process = f"caller {row['caller_pid']}"
    if row["worker_pid"] is not None:
        process += f" · worker {row['worker_pid']}"
    result = ""
    if row["exit_status"] is not None:
        result = f" · exit {row['exit_status']}"
    cancel = " · cancellation requested" if row["cancel_requested"] else ""
    failure = f" · {row['failure_reason']}" if row["failure_reason"] else ""
    publication_cancel = (
        " · cancellable only while queued"
        if row["status"] == "running"
        and (
            row["kind"] == "merge"
            or (row["kind"] == "land" and row["phase"] == "publishing")
        )
        else ""
    )
    resources = ", ".join(f"{name}={units}" for name, units in row["resources"].items())
    blockers = ", ".join(row["blocked_by"]) or "—"
    publication = row["publication"]
    target = (
        f" · publish {publication['adapter']} {publication['request']}"
        if publication
        else ""
    )
    return (
        f"{row['run_id']} · {row['kind']} · {row['repository']}{target}\n"
        f"phase: {row['phase']} · gate exit: "
        f"{row['gate_exit_status'] if row['gate_exit_status'] is not None else '—'}\n"
        f"agent: {row['agent']} · resources: {resources} · blocked: {blockers}\n"
        f"branch: {row['branch']}\n"
        f"checkout: {row['checkout']}\n"
        f"{process}{result}{failure}{cancel}{publication_cancel} · log {row['log_bytes']} bytes\n"
        f"created: {row['created_at']}\n"
        f"started: {row['started_at'] or '—'}\n"
        f"finished: {row['finished_at'] or '—'}"
    )


def _modal_detail(row: dict) -> str:
    command = shlex.join(row["command"])
    return (
        f"Status: {row['status']}\n"
        f"Kind: {row['kind']}\n"
        f"Phase: {row['phase']}\n"
        f"Gate exit: {row['gate_exit_status'] if row['gate_exit_status'] is not None else '—'}\n"
        f"Repository: {row['repository']} ({row['repository_id']})\n"
        f"Worktree: {row['worktree_id']}\n"
        f"Agent: {row['agent']}\n"
        f"Barrier: {'yes' if row['barrier'] else 'no'}\n"
        f"Resources: {json.dumps(row['resources'], sort_keys=True)}\n"
        f"Blocked by: {', '.join(row['blocked_by']) or '—'}\n"
        f"Label: {row['label']}\n"
        f"Run: {row['run_id']}\n"
        f"Branch: {row['branch']}\n"
        f"Head: {row['head_sha'] or '—'}\n"
        f"Gate receipt: {row['gate_run_id'] or '—'}\n"
        f"Publication: {json.dumps(row['publication'], sort_keys=True) if row['publication'] else '—'}\n"
        f"Failure reason: {row['failure_reason'] or '—'}\n"
        f"Checkout: {row['checkout']}\n"
        f"Caller PID: {row['caller_pid']}\n"
        f"Worker PID: {row['worker_pid'] if row['worker_pid'] is not None else '—'}\n"
        f"Created: {row['created_at']}\n"
        f"Started: {row['started_at'] or '—'}\n"
        f"Finished: {row['finished_at'] or '—'}\n"
        f"Exit: {row['exit_status'] if row['exit_status'] is not None else '—'}\n"
        f"Command: {command}"
    )


def build_app(
    client_factory: Callable[[], CoordinatorClient],
    *,
    refresh_interval: float = 1.0,
):
    """Build the standalone app without opening control-plane stores or credentials."""
    if App is None:  # pragma: no cover
        raise RuntimeError(_MISSING)
    if refresh_interval <= 0:
        raise ValueError("refresh_interval must be positive")

    class Show(ModalScreen[None]):
        BINDINGS = [Binding("escape,enter,q", "close", "close")]

        def __init__(self, title: str, body: str, *, wide: bool = False) -> None:
            super().__init__()
            self.title_text = title
            self.body_text = body
            self.wide = wide

        def compose(self) -> ComposeResult:
            with Vertical(id="show", classes="wide" if self.wide else ""):
                yield Label(self.title_text, markup=False, id="show-title")
                with VerticalScroll(id="show-scroll"):
                    yield Static(self.body_text, markup=False, id="show-body")
                yield Static("Enter close", markup=False, id="show-keys", classes="status")

        def on_mount(self) -> None:
            if self.wide:
                self.query_one("#show", Vertical).styles.height = "94%"
                self.query_one("#show-scroll", VerticalScroll).styles.height = "1fr"

        def action_close(self) -> None:
            self.dismiss(None)

    class Confirm(ModalScreen[bool]):
        BINDINGS = [
            Binding("escape,n", "cancel", "no"),
            Binding("y", "confirm", "yes"),
        ]

        def __init__(self, row: dict) -> None:
            super().__init__()
            self.row = row

        def compose(self) -> ComposeResult:
            with Vertical(id="confirm"):
                yield Label(
                    f"Cancel {self.row['label']}?",
                    markup=False,
                    id="confirm-title",
                )
                yield Static(
                    f"{self.row['run_id']} is {self.row['status']} on "
                    f"{self.row['branch']}.\n"
                    f"Checkout: {self.row['checkout']}\n"
                    + (
                        "Its whole process group will stop."
                        if self.row["status"] == "running"
                        else "Only this queued request will be removed."
                    ),
                    markup=False,
                    id="confirm-effects",
                )
                with Horizontal(id="confirm-buttons"):
                    yield Button("Cancel job", variant="primary", id="confirm-ok")
                    yield Button("Keep job", id="confirm-cancel")

        def on_mount(self) -> None:
            self.query_one("#confirm-cancel", Button).focus()

        def on_button_pressed(self, event: Button.Pressed) -> None:
            self.dismiss(event.button.id == "confirm-ok")

        def action_confirm(self) -> None:
            self.dismiss(True)

        def action_cancel(self) -> None:
            self.dismiss(False)

    class RunTable(DataTable):
        # Keep Enter owned by DataTable.RowSelected while making its actual meaning visible
        # in the one-line Footer. An App-level Enter binding would double-fire selection.
        BINDINGS = [
            Binding("enter", "select_cursor", "details"),
            *DataTable.BINDINGS[1:],
        ]

    class CoordinatorApp(App):
        TITLE = "AGCoord"
        CSS = frame.CSS + """
        #gates { height: 1fr; }
        #gate-detail { height: 9; min-height: 9; max-height: 9; }
        """
        ENABLE_COMMAND_PALETTE = False
        BINDINGS = [
            Binding("l", "log", "log"),
            Binding("c", "cancel", "cancel"),
            Binding("h", "history", "hide/show history"),
            Binding("p", "repository", "repository filter"),
            Binding("a", "agent", "agent filter"),
            Binding("r", "refresh", "refresh"),
            Binding("q", "quit", "quit"),
            Binding("question_mark", "keys", "keys"),
        ]

        def __init__(self) -> None:
            super().__init__()
            self.theme = "textual-light"
            self._client_factory = client_factory
            self._client: CoordinatorClient | None = None
            self._client_lock = threading.Lock()
            self._rows: dict[str, dict] = {}
            self._reading = False
            self._message = "loading …"
            self._snapshot: dict | None = None
            self._show_history = True
            self._repository_filter: str | None = None
            self._agent_filter: str | None = None

        def compose(self) -> ComposeResult:
            with Vertical():
                yield Static(
                    "MACHINE │ AGCOORD │ loading",
                    markup=False,
                    classes="subject",
                    id="gate-subject",
                )
                yield RunTable(id="gates", cursor_type="row")
                yield Static("", markup=False, classes="detail-rule", id="gate-rule")
                yield Static(
                    "loading AGCoord …",
                    markup=False,
                    classes="detail",
                    id="gate-detail",
                )
                yield Static(
                    "connecting …",
                    markup=False,
                    classes="status",
                    id="gate-status",
                )
            yield Footer()

        def on_mount(self) -> None:
            table = self.query_one("#gates", DataTable)
            table.cell_padding = 0
            for label, width in (
                ("STATE", 10),
                ("KIND", 5),
                ("REPO", 11),
                ("RUN", 16),
                ("LABEL", 14),
                ("AGE", 6),
                ("DUR", 6),
            ):
                table.add_column(label, width=width)
            self.action_refresh()
            self.set_interval(refresh_interval, self.action_refresh)

        def _client_for_worker(self) -> CoordinatorClient:
            with self._client_lock:
                if self._client is None:
                    self._client = self._client_factory()
                return self._client

        def action_refresh(self) -> None:
            if self._reading:
                return
            self._reading = True

            async def work() -> None:
                try:
                    result = await _off_loop(
                        lambda: self._client_for_worker().snapshot()
                    )
                except CoordinatorError as exc:
                    self._read_failed(exc)
                    return
                self._apply_snapshot(result)

            # `_reading` is the overlap guard. The Textual worker tracks the async delivery;
            # the bounded coordinator call itself stays off the paint/keyboard loop.
            self.run_worker(
                work(),
                group="gate-snapshot",
            )

        def _read_failed(self, exc: CoordinatorError) -> None:
            if not self.is_running:
                return
            self._reading = False
            self._message = f"unavailable · {exc}"
            self.query_one("#gate-subject", Static).update("MACHINE │ AGCOORD │ unavailable")
            self.query_one("#gate-status", Static).update(self._message)
            if not self._rows:
                self.query_one("#gate-detail", Static).update(
                    "AGCoord unavailable\n" + str(exc)
                )

        def _selected_id(self) -> str | None:
            table = self.query_one("#gates", DataTable)
            if table.row_count == 0 or table.cursor_row is None:
                return None
            key = table.coordinate_to_cell_key(table.cursor_coordinate).row_key
            return str(key.value)

        def _selected(self) -> dict | None:
            selected = self._selected_id()
            return self._rows.get(selected) if selected else None

        def _apply_snapshot(self, snapshot: dict) -> None:
            if not self.is_running:
                return
            self._reading = False
            self._snapshot = snapshot
            self._render_snapshot(snapshot)

        def _render_snapshot(self, snapshot: dict) -> None:
            selected = self._selected_id()
            table = self.query_one("#gates", DataTable)
            viewport = (table.scroll_x, table.scroll_y)
            ordered = [*snapshot["active"], *snapshot["queued"]]
            if self._show_history:
                ordered.extend(snapshot["recent"])
            if self._repository_filter is not None:
                ordered = [
                    row for row in ordered
                    if row["repository_id"] == self._repository_filter
                ]
            if self._agent_filter is not None:
                ordered = [row for row in ordered if row["agent"] == self._agent_filter]
            self._rows = {row["run_id"]: row for row in ordered}
            table.clear(columns=False)
            now = _moment(snapshot["captured_at"]) or datetime.now(timezone.utc)
            for row in ordered:
                table.add_row(
                    row["status"],
                    row["kind"],
                    row["repository_id"],
                    row["run_id"],
                    row["label"],
                    _age(row, now),
                    _duration(row, now),
                    key=row["run_id"],
                )
            if selected in self._rows:
                table.move_cursor(
                    row=[row["run_id"] for row in ordered].index(selected),
                    scroll=False,
                )
            elif ordered:
                table.move_cursor(row=0, scroll=False)

            # DataTable.clear() resets both scroll axes before its new dimensions are
            # available. Restore after layout so Textual can clamp only when the refreshed
            # content genuinely became smaller.
            table.call_after_refresh(
                table.scroll_to,
                x=viewport[0],
                y=viewport[1],
                animate=False,
                force=True,
                immediate=True,
            )
            active = len(snapshot["active"])
            queued = len(snapshot["queued"])
            history = "shown · h hide" if self._show_history else "hidden · h show"
            repository = self._repository_filter or "all repos"
            agent = self._agent_filter or "all agents"
            self.query_one("#gate-subject", Static).update(
                f"MACHINE │ AGCOORD │ {active} active · {queued} queued · "
                f"{repository} · {agent} · history {history}"
            )
            allocation = ", ".join(
                f"{name} {snapshot['allocations'].get(name, 0)}/{capacity}"
                for name, capacity in snapshot["capacities"].items()
            )
            self._message = (
                f"broker {snapshot['broker_pid']} · {allocation} · "
                f"read {snapshot['captured_at']}"
            )
            self.query_one("#gate-status", Static).update(self._message)
            self._update_detail()

        def action_history(self) -> None:
            self._show_history = not self._show_history
            if self._snapshot is not None:
                self._render_snapshot(self._snapshot)

        def _cycle_filter(self, field: str, current: str | None) -> str | None:
            if self._snapshot is None:
                return None
            rows = [
                *self._snapshot["active"],
                *self._snapshot["queued"],
                *self._snapshot["recent"],
            ]
            choices: list[str | None] = [None, *sorted({str(row[field]) for row in rows})]
            if current not in choices:
                return None
            return choices[(choices.index(current) + 1) % len(choices)]

        def action_repository(self) -> None:
            self._repository_filter = self._cycle_filter(
                "repository_id", self._repository_filter
            )
            if self._snapshot is not None:
                self._render_snapshot(self._snapshot)

        def action_agent(self) -> None:
            self._agent_filter = self._cycle_filter("agent", self._agent_filter)
            if self._snapshot is not None:
                self._render_snapshot(self._snapshot)

        def _update_detail(self) -> None:
            row = self._selected()
            rule = "── no gate selected"
            if row is not None:
                rule = f"── {row['label']} · {row['run_id']}"
            self.query_one("#gate-rule", Static).update(rule)
            self.query_one("#gate-detail", Static).update(_detail(row))

        def on_data_table_row_highlighted(self, _event: DataTable.RowHighlighted) -> None:
            if self.is_running:
                self._update_detail()

        def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
            # Enter belongs to the focused table. A competing App binding fires alongside
            # DataTable's selection action on some Textual releases and can double-push the
            # fixed-id modal; this event is the app's single activation boundary.
            if event.data_table.id == "gates":
                self.action_detail()

        def action_detail(self) -> None:
            row = self._selected()
            if row is not None:
                self.push_screen(Show(row["label"], _modal_detail(row), wide=True))

        def action_keys(self) -> None:
            lines = []
            for binding in self.BINDINGS:
                description = binding.description
                if binding.action == "history":
                    description = (
                        "hide completed history"
                        if self._show_history
                        else "show completed history"
                    )
                lines.append(f"{binding.key}  {description}")
            self.push_screen(Show(
                "Keys",
                "\n".join(lines)
                + "\n\nmerge  cancellable only while queued"
                + "\nland   cancellable until publishing",
            ))

        def action_log(self) -> None:
            row = self._selected()
            if row is None:
                return
            run_id = row["run_id"]
            label = row["label"]
            self.query_one("#gate-status", Static).update(f"reading log for {run_id} …")

            def read_log() -> str:
                offset = 0
                pages: list[str] = []
                total = 0
                truncated = False
                while total < 1024 * 1024:
                    page = self._client_for_worker().log(run_id, offset=offset)
                    pages.append(page["text"])
                    total += len(page["text"].encode("utf-8"))
                    if page["eof"]:
                        break
                    if page["next_offset"] <= offset:
                        raise CoordinatorError("gate log cursor did not advance")
                    offset = page["next_offset"]
                else:
                    truncated = True
                body = "".join(pages) or "(no output yet)"
                if truncated:
                    body += "\n\n[log view truncated at 1 MiB]"
                return body

            async def work() -> None:
                try:
                    body = await _off_loop(read_log)
                except CoordinatorError as exc:
                    self._operation_failed(exc)
                    return
                self._show_log(label, run_id, str(body))

            self.run_worker(work(), group="gate-log", exclusive=True)

        def _show_log(self, label: str, run_id: str, body: str) -> None:
            if not self.is_running:
                return
            self.query_one("#gate-status", Static).update(self._message)
            self.push_screen(Show(f"{label} · {run_id}", body, wide=True))

        def action_cancel(self) -> None:
            row = self._selected()
            if row is None:
                return
            if row["status"] in TERMINAL_STATUSES:
                self.query_one("#gate-status", Static).update(
                    f"{row['run_id']} is already {row['status']}"
                )
                return
            if row["status"] == "running" and (
                row["kind"] == "merge"
                or (row["kind"] == "land" and row["phase"] == "publishing")
            ):
                self.query_one("#gate-status", Static).update(
                    f"{row['run_id']} is publishing and cannot be cancelled"
                )
                return
            run_id = row["run_id"]
            self.push_screen(
                Confirm(row),
                lambda confirmed: self._cancel_confirmed(run_id, confirmed),
            )

        def _cancel_confirmed(self, run_id: str, confirmed: bool) -> None:
            if not confirmed:
                return
            self.query_one("#gate-status", Static).update(
                f"cancelling {run_id} …"
            )

            async def work() -> None:
                try:
                    await _off_loop(lambda: self._client_for_worker().cancel(run_id))
                except CoordinatorError as exc:
                    self._operation_failed(exc)
                    return
                self._cancelled(run_id)

            self.run_worker(work(), group="gate-cancel", exclusive=True)

        def _cancelled(self, run_id: str) -> None:
            if not self.is_running:
                return
            self.query_one("#gate-status", Static).update(
                f"cancellation requested for {run_id}"
            )
            self.action_refresh()

        def _operation_failed(self, exc: CoordinatorError) -> None:
            if self.is_running:
                self.query_one("#gate-status", Static).update(f"operation failed · {exc}")

    return CoordinatorApp()


def run(
    client_factory: Callable[[], CoordinatorClient],
    *,
    refresh_interval: float = 1.0,
) -> int:
    if App is None:  # pragma: no cover
        print(_MISSING)
        return 2
    build_app(
        client_factory,
        refresh_interval=refresh_interval,
    ).run()
    return 0
