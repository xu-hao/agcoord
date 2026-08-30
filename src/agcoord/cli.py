"""AGCoord: machine-local coordination for development agents and repositories."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
import time
from typing import Iterable, TextIO

from . import __version__
from .queue import (
    CoordinatorClient,
    CoordinatorError,
    follow,
    migrate_queue,
    parse_resource_claims,
    wait,
)
from .resources import resource_enforcement_summary


def _resources(values: list[str]) -> dict[str, int]:
    return parse_resource_claims(values)


def _table(rows: list[dict]) -> str:
    if not rows:
        return "(nothing)"
    display = [
        {
            "status": row["status"],
            "kind": row["kind"],
            "run": row["run_id"],
            "repository": row["repository"],
            "agent": row["agent"],
            "label": row["label"],
            "resources": ",".join(
                f"{name}={units}" for name, units in row["resources"].items()
            ),
            "enforcement": resource_enforcement_summary(row["resource_receipt"]),
        }
        for row in rows
    ]
    columns = [
        "status",
        "kind",
        "run",
        "repository",
        "agent",
        "label",
        "resources",
        "enforcement",
    ]
    widths = [max(len(name), *(len(str(row[name])) for row in display)) for name in columns]
    lines = [
        "  ".join(name.ljust(width) for name, width in zip(columns, widths)),
        "  ".join("-" * width for width in widths),
    ]
    lines.extend(
        "  ".join(str(row[name]).ljust(width) for name, width in zip(columns, widths))
        for row in display
    )
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="agcoord", description=__doc__)
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    parser.add_argument("--json", action="store_true", help="emit strict JSON")
    parser.add_argument("--state-dir", help="override the user-scoped machine spool")
    commands = parser.add_subparsers(dest="command", required=True)

    def state(command: argparse.ArgumentParser) -> argparse.ArgumentParser:
        command.add_argument(
            "--state-dir",
            default=argparse.SUPPRESS,
            help="override the user-scoped machine spool",
        )
        command.add_argument(
            "--json",
            action="store_true",
            default=argparse.SUPPRESS,
            help="emit strict JSON",
        )
        return command

    state(commands.add_parser("list", help="show active, queued, and recent runs"))
    show = state(commands.add_parser("show", help="show one run"))
    show.add_argument("run_id")
    log = state(commands.add_parser("log", help="print one run's log"))
    log.add_argument("run_id")
    log.add_argument("--follow", action="store_true", help="wait through terminal status")
    cancel = state(commands.add_parser("cancel", help="cancel one live run"))
    cancel.add_argument("run_id")
    state(commands.add_parser("clear", help="clear terminal history and logs while idle"))
    state(commands.add_parser("tui", help="open the machine queue terminal view"))
    state(commands.add_parser("migrate", help="explicitly migrate an idle spool"))

    def submission(name: str, help_text: str) -> argparse.ArgumentParser:
        command = state(commands.add_parser(name, help=help_text))
        command.add_argument("--label", default=name, help="short queue label")
        command.add_argument("--checkout", default=".", help="command working tree")
        command.add_argument("--repository", help="explicit stable repository identity")
        command.add_argument("--agent", help="agent identity (default: AGCOORD_AGENT or PID)")
        command.add_argument(
            "--resource",
            action="append",
            default=[],
            metavar="NAME=UNITS",
            help="repeatable machine resource claim",
        )
        command.add_argument("worker_command", nargs=argparse.REMAINDER)
        return command

    submission("run", "submit a compatible check")
    submission("full", "submit a clean exact-head repository barrier")

    land = state(commands.add_parser(
        "land",
        help="gate and publish one fresh exact head without releasing its barrier",
    ))
    land.add_argument("request", type=int, help="adapter request (GitHub PR number)")
    land.add_argument("--adapter", default="github", help="publication adapter")
    land.add_argument("--label", default="land", help="short queue label")
    land.add_argument("--checkout", default=".", help="ticket worktree")
    land.add_argument("--repository", help="explicit stable repository identity")
    land.add_argument("--agent", help="agent identity")
    land.add_argument("--resource", action="append", default=[], metavar="NAME=UNITS")
    land.add_argument("worker_command", nargs="+")
    return parser


def _client(args: argparse.Namespace, checkout: Path) -> CoordinatorClient:
    return CoordinatorClient(
        state_dir=getattr(args, "state_dir", None),
        checkout=checkout,
        autostart=True,
    )


def run(args: argparse.Namespace, *, out: TextIO = sys.stdout) -> int:
    checkout = Path(getattr(args, "checkout", ".")).expanduser().resolve()
    emit = (
        (lambda value: print(json.dumps(value, indent=2, sort_keys=True), file=out))
        if args.json
        else None
    )

    if args.command == "migrate":
        result = migrate_queue(state_dir=args.state_dir)
        if emit:
            emit(result)
        elif result["changed"]:
            print(
                f"AGCoord: migrated protocol {result['from_protocol']} "
                f"to {result['to_protocol']}",
                file=out,
            )
        else:
            print(f"AGCoord: protocol {result['to_protocol']} already current", file=out)
        return 0

    def client_factory() -> CoordinatorClient:
        return _client(args, checkout)

    if args.command == "tui":
        from .tui import run as run_tui

        return run_tui(client_factory)

    client = client_factory()
    if args.command == "list":
        snapshot = client.snapshot()
        if emit:
            emit(snapshot)
        else:
            print(_table([*snapshot["active"], *snapshot["queued"], *snapshot["recent"]]), file=out)
        return 0
    if args.command == "show":
        row = client.status(args.run_id)
        print(json.dumps(row, indent=2, sort_keys=True), file=out)
        return 0
    if args.command == "cancel":
        row = client.cancel(args.run_id)
        emit(row) if emit else print(
            f"{row['run_id']}: {row['status']}"
            + (" · cancellation requested" if row["cancel_requested"] else ""),
            file=out,
        )
        return 0
    if args.command == "clear":
        result = client.clear()
        emit(result) if emit else print(f"AGCoord: cleared {result['cleared']} run(s)", file=out)
        return 0
    if args.command == "log":
        offset = 0
        while True:
            page = client.log(args.run_id, offset=offset)
            if emit:
                emit(page)
                return 0
            print(page["text"], end="", file=out)
            offset = page["next_offset"]
            row = client.status(args.run_id)
            if page["eof"] and (not args.follow or row["status"] not in {"queued", "running"}):
                return 0
            time.sleep(0.1)

    if not checkout.is_dir():
        raise CoordinatorError(f"checkout does not exist: {checkout}")
    claims = _resources(args.resource)
    command = list(args.worker_command)
    if command and command[0] == "--":
        command.pop(0)
    if not command:
        raise CoordinatorError(f"agcoord {args.command} needs a command after --")
    if args.command == "land":
        run_id = client.submit_land(
            args.adapter,
            args.request,
            command,
            checkout=str(checkout),
            label=args.label,
            resources=claims,
            agent=args.agent,
            repository=args.repository,
        )
    else:
        run_id = client.submit(
            command,
            checkout=str(checkout),
            kind="full" if args.command == "full" else "check",
            label=args.label,
            resources=claims,
            agent=args.agent,
            repository=args.repository,
        )

    if emit:
        final = wait(client, run_id)
        emit(final)
        return int(final["exit_status"] if final["exit_status"] is not None else 70)
    print(f"AGCoord: accepted {run_id}", file=out, flush=True)
    return follow(client, run_id, out=out)


def main(argv: Iterable[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return run(args)
    except CoordinatorError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
