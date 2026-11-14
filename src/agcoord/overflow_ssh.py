"""Overflow executor that runs a placed job in a container on another machine over SSH.

Configure it as a pool's executor::

    "executor": ["agc", "overflow-ssh", "builder@workstation"]

The broker starts it in the job's checkout as ``agc overflow-ssh DESTINATION -- COMMAND``.
It sends the repository's toolchain image to the destination once, sends the job's source
and builds it into a run image there, runs the command in a container limited to the job's
claims, relays the output, and exits with the command's status.  The destination needs
only an SSH login and Docker or Podman; it never needs forge or registry credentials.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import shlex
import signal
import subprocess
import sys
from typing import Sequence

from agcoord.overflow_image import (
    ContainerLimits,
    EXIT_NO_VERDICT,
    ExecutorError,
    Stopped,
    build_toolchain,
    command_line,
    container_environment,
    container_tool,
    ignore_stop_signals,
    load_repository_config,
    placement,
    repository_root,
    stop_on_signals,
    write_build_context,
)


#: ``ssh`` exits with this status for its own failures rather than the remote command's.
SSH_FAILED = 255
#: Seconds the remote container is given to stop after its runtime limit before it is killed.
STOP_GRACE = 10


class SshExecutor:
    def __init__(
        self,
        *,
        destination: str,
        ssh_options: Sequence[str],
        local_tool: str,
        remote_tool: str,
        passed_environment: Sequence[str],
    ) -> None:
        self.ssh = ["ssh", "-o", "BatchMode=yes", *ssh_options, destination]
        self.local_tool = local_tool
        self.remote_tool = remote_tool
        self.passed_environment = list(passed_environment)

    def remote(self, arguments: Sequence[str], **options: object) -> subprocess.Popen[bytes]:
        """Start one command on the destination."""
        return subprocess.Popen([*self.ssh, shlex.join(arguments)], **options)

    def remote_status(self, arguments: Sequence[str]) -> int:
        process = self.remote(
            arguments,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return process.wait()

    def ensure_toolchain(self, toolchain: str) -> None:
        """Send the toolchain image unless the destination already holds it."""
        status = self.remote_status([self.remote_tool, "image", "inspect", toolchain])
        if status == 0:
            return
        if status == SSH_FAILED:
            raise ExecutorError("cannot reach the overflow destination over SSH")
        _say(f"sending toolchain image {toolchain}")
        save = subprocess.Popen(
            [self.local_tool, "save", toolchain],
            stdout=subprocess.PIPE,
        )
        load = self.remote(
            [self.remote_tool, "load"],
            stdin=save.stdout,
            stdout=subprocess.DEVNULL,
        )
        assert save.stdout is not None
        save.stdout.close()
        if load.wait() != 0 or save.wait() != 0:
            raise ExecutorError(f"cannot send toolchain image {toolchain} to the destination")

    def build_run_image(self, root: Path, toolchain: str, image: str) -> None:
        """Build the job's image on the destination from a streamed build context."""
        build = self.remote(
            [self.remote_tool, "build", "-q", "-t", image, "-"],
            stdin=subprocess.PIPE,
            stdout=subprocess.DEVNULL,
        )
        assert build.stdin is not None
        try:
            count = write_build_context(root, toolchain, build.stdin)
            build.stdin.close()
        except BrokenPipeError:
            count = 0
        if build.wait() != 0:
            raise ExecutorError("cannot build the job's image on the destination")
        _say(f"built {image} from {count} source files")

    def run(
        self,
        *,
        name: str,
        image: str,
        limits: ContainerLimits,
        max_runtime: int,
        command: Sequence[str],
    ) -> int:
        """Run the command in its limited container and return its exit status."""
        environment = container_environment(os.environ, self.passed_environment, limits)
        process = self.remote(
            [
                # The destination enforces the runtime limit itself, so a lost connection
                # or a lost broker cannot leave the container running.
                "timeout",
                f"--kill-after={STOP_GRACE}",
                str(max_runtime),
                self.remote_tool,
                "run",
                "--rm",
                f"--name={name}",
                *limits.run_options(),
                *(f"--env={name}={value}" for name, value in environment.items()),
                image,
                *command,
            ],
            stdin=subprocess.DEVNULL,
        )
        return process.wait()

    def cleanup_command(self, name: str, image: str) -> list[str]:
        script = (
            f"{shlex.quote(self.remote_tool)} rm -f {shlex.quote(name)} >/dev/null 2>&1; "
            f"{shlex.quote(self.remote_tool)} rmi -f {shlex.quote(image)} >/dev/null 2>&1; "
            "true"
        )
        return [*self.ssh[:1], "-o", "ConnectTimeout=10", *self.ssh[1:], script]

    def cleanup(self, name: str, image: str, *, detached: bool) -> None:
        """Remove the container and the job's image from the destination.

        A stopping broker kills this executor's process group shortly after signalling it,
        which may be sooner than an SSH round trip.  The stop path therefore cleans up from
        a detached process that outlives the group.
        """
        process = subprocess.Popen(
            self.cleanup_command(name, image),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=detached,
        )
        if not detached:
            process.wait()


def _say(message: str) -> None:
    print(f"agc overflow-ssh: {message}", file=sys.stderr, flush=True)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="agc overflow-ssh",
        description=(
            "AGCoord overflow executor: run a placed job in a limited container on "
            "another machine over SSH."
        ),
    )
    parser.add_argument("destination", help="SSH destination, such as user@host")
    parser.add_argument(
        "--ssh-option",
        action="append",
        default=[],
        metavar="OPTION",
        help="extra ssh -o option, such as Port=2222 (repeatable)",
    )
    parser.add_argument("--runtime", help="local container tool (default: docker, else podman)")
    parser.add_argument(
        "--remote-runtime",
        default="docker",
        help="container tool on the destination (default: docker)",
    )
    parser.add_argument(
        "--env",
        action="append",
        default=[],
        metavar="NAME",
        help="pass this environment variable into the container (repeatable)",
    )
    parser.add_argument("command", nargs=argparse.REMAINDER)
    return parser


def main(arguments: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(arguments)
    name = image = None
    executor = None
    stop_on_signals()
    try:
        command = command_line(args.command)
        placed = placement(os.environ)
        root = repository_root(Path.cwd())
        config = load_repository_config(root)
        local_tool = container_tool(args.runtime)
        ssh_options = [part for option in args.ssh_option for part in ("-o", option)]
        executor = SshExecutor(
            destination=args.destination,
            ssh_options=ssh_options,
            local_tool=local_tool,
            remote_tool=args.remote_runtime,
            passed_environment=args.env,
        )
        toolchain = build_toolchain(local_tool, root, config)
        executor.ensure_toolchain(toolchain)
        name = f"agcoord-{placed.run_id}"
        image = f"agcoord-run:{placed.run_id}"
        executor.build_run_image(root, toolchain, image)
        status = executor.run(
            name=name,
            image=image,
            limits=placed.limits,
            max_runtime=placed.max_runtime,
            command=command,
        )
    except Stopped:
        ignore_stop_signals()
        if executor is not None and name is not None and image is not None:
            executor.cleanup(name, image, detached=True)
        return 128 + signal.SIGTERM
    except ExecutorError as error:
        _say(str(error))
        if executor is not None and name is not None and image is not None:
            executor.cleanup(name, image, detached=False)
        return EXIT_NO_VERDICT
    executor.cleanup(name, image, detached=False)
    if status == SSH_FAILED:
        _say("the SSH connection failed before the command returned a verdict")
        return EXIT_NO_VERDICT
    return status


if __name__ == "__main__":
    raise SystemExit(main())
