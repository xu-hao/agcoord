"""The image step shared by overflow executors.

An overflow executor runs a placed job's command somewhere other than this machine, so
it must carry three things there: the repository's toolchain, the job's source, and the
limits the job claimed.  This module derives all three from the checkout and from the
environment the broker gives an executor, without knowing how the target is reached.
"""

from __future__ import annotations

from dataclasses import dataclass
import io
import json
import os
from pathlib import Path
import shlex
import shutil
import signal
import subprocess
import tarfile
from typing import IO, Mapping, Sequence


REPOSITORY_CONFIG = ".agcoord/overflow.json"
WORKSPACE = "/workspace"
#: The exit status with which an executor reports that it could not obtain a verdict.
EXIT_NO_VERDICT = 125


#: The exit status of a job stopped at its runtime limit, as ``timeout`` reports it.
EXIT_RUNTIME_EXCEEDED = 124


class ExecutorError(RuntimeError):
    """A failure that prevents the executor from obtaining the command's verdict."""


class Stopped(Exception):
    """The broker asked this executor to stop."""


def stop_on_signals() -> None:
    """Turn the broker's stop signal into :class:`Stopped` in the main thread."""

    def stop(_signal: int, _frame: object) -> None:
        raise Stopped()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)


def ignore_stop_signals() -> None:
    """Let cleanup proceed once a stop has been accepted."""
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    signal.signal(signal.SIGINT, signal.SIG_IGN)


def run_detached(commands: Sequence[Sequence[str]]) -> None:
    """Run cleanup commands in order from a process that outlives this executor.

    A stopping broker kills the executor's whole process group shortly after signalling
    it, which can be sooner than one round trip to another machine.  Cleanup that must
    still happen is therefore handed to a process in its own session.
    """
    script = "; ".join(f"{shlex.join(command)} >/dev/null 2>&1" for command in commands)
    subprocess.Popen(
        ["sh", "-c", script],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )


@dataclass(frozen=True)
class RepositoryOverflow:
    """How one repository's toolchain image is obtained: built here, or named."""

    dockerfile: Path | None
    image: str | None


@dataclass(frozen=True)
class ContainerLimits:
    """The limits a container must carry to honour a job's enforced claims."""

    cpus: int | None = None
    memory: int | None = None
    memory_reservation: int | None = None
    swap: int | None = None
    tmpfs: int | None = None
    tmpfs_inodes: int | None = None
    processes: int | None = None

    def run_options(self) -> list[str]:
        """Return the container-run options, shared by Docker and Podman."""
        options: list[str] = []
        if self.cpus is not None:
            options.append(f"--cpus={self.cpus}")
        if self.memory is not None:
            options.append(f"--memory={self.memory}")
            # Without an explicit swap claim the job gets no swap beyond its memory.
            options.append(f"--memory-swap={self.memory + (self.swap or 0)}")
        if self.memory_reservation is not None:
            options.append(f"--memory-reservation={self.memory_reservation}")
        if self.tmpfs is not None:
            mount = f"/tmp:rw,size={self.tmpfs}"
            if self.tmpfs_inodes is not None:
                mount += f",nr_inodes={self.tmpfs_inodes}"
            options.append(f"--tmpfs={mount}")
        if self.processes is not None:
            options.append(f"--pids-limit={self.processes}")
        return options


def repository_root(checkout: str | os.PathLike[str]) -> Path:
    """Return the top of the working tree that contains ``checkout``."""
    completed = subprocess.run(
        ["git", "-C", str(checkout), "rev-parse", "--show-toplevel"],
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        raise ExecutorError(f"{checkout} is not inside a Git working tree")
    return Path(completed.stdout.strip())


def has_repository_config(checkout: str | os.PathLike[str]) -> bool:
    """Say whether the working tree declares itself able to run in an overflow pool."""
    try:
        return (repository_root(checkout) / REPOSITORY_CONFIG).is_file()
    except ExecutorError:
        return False


def load_repository_config(root: Path) -> RepositoryOverflow:
    """Read the file through which a repository opts in to overflow."""
    path = root / REPOSITORY_CONFIG
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ExecutorError(
            f"{REPOSITORY_CONFIG} is missing; the repository has not opted in to overflow"
        ) from exc
    except (OSError, json.JSONDecodeError) as exc:
        raise ExecutorError(f"cannot read {REPOSITORY_CONFIG}: {exc}") from exc
    if not isinstance(document, dict) or set(document) - {"dockerfile", "image"}:
        raise ExecutorError(
            f"{REPOSITORY_CONFIG} must be an object with only dockerfile or image"
        )
    dockerfile = document.get("dockerfile")
    image = document.get("image")
    if (dockerfile is None) == (image is None):
        raise ExecutorError(f"{REPOSITORY_CONFIG} must name exactly one of dockerfile or image")
    if dockerfile is not None:
        if not isinstance(dockerfile, str) or not dockerfile.strip():
            raise ExecutorError(f"{REPOSITORY_CONFIG} dockerfile must be a non-empty path")
        selected = (root / dockerfile).resolve()
        if root.resolve() not in selected.parents or not selected.is_file():
            raise ExecutorError(
                f"{REPOSITORY_CONFIG} dockerfile must be an existing file in the repository"
            )
        return RepositoryOverflow(dockerfile=selected, image=None)
    if not isinstance(image, str) or not image.strip() or any(c.isspace() for c in image):
        raise ExecutorError(f"{REPOSITORY_CONFIG} image must be one image reference")
    return RepositoryOverflow(dockerfile=None, image=image)


def source_files(root: Path) -> list[str]:
    """List the working tree's tracked and untracked files, never its ignored ones.

    A clean exact head lists exactly the commit; a check lists the working state.  Ignored
    files, which is where local secrets live, never leave this machine.
    """
    completed = subprocess.run(
        [
            "git",
            "-C",
            str(root),
            "ls-files",
            "-z",
            "--cached",
            "--others",
            "--exclude-standard",
        ],
        capture_output=True,
    )
    if completed.returncode != 0:
        raise ExecutorError("cannot list the working tree's files")
    names = [os.fsdecode(name) for name in completed.stdout.split(b"\0") if name]
    # A tracked file deleted in the working tree is listed but no longer exists.
    return sorted(name for name in set(names) if os.path.lexists(root / name))


def write_build_context(root: Path, toolchain: str, stream: IO[bytes]) -> int:
    """Write a build context that adds the source as one layer on the toolchain image.

    The image only copies files: nothing is compiled at build time, because a job
    overflows exactly when this machine's CPUs are busy.  Returns the number of source
    files written.
    """
    dockerfile = (
        f"FROM {toolchain}\nCOPY workspace {WORKSPACE}\nWORKDIR {WORKSPACE}\n"
    ).encode()
    names = source_files(root)
    with tarfile.open(fileobj=stream, mode="w|") as archive:
        info = tarfile.TarInfo("Dockerfile")
        info.size = len(dockerfile)
        info.mode = 0o644
        archive.addfile(info, io.BytesIO(dockerfile))
        directory = tarfile.TarInfo("workspace")
        directory.type = tarfile.DIRTYPE
        directory.mode = 0o755
        archive.addfile(directory)
        for name in names:
            archive.add(root / name, arcname=f"workspace/{name}", recursive=False)
    return len(names)


@dataclass(frozen=True)
class EnforcementTarget:
    """Which kinds of claim one kind of destination can enforce.

    ``dependencies`` lists limits the destination can apply only together with another:
    ``(dependent, base, explanation)`` over :class:`ContainerLimits` field names.
    """

    description: str
    kinds: frozenset[str]
    dependencies: tuple[tuple[str, str, str], ...] = ()


#: A Docker or Podman container, which every executor that starts one itself can limit.
CONTAINER = EnforcementTarget(
    description="a container",
    kinds=frozenset({"cpu", "memory", "memory-high", "swap", "tmpfs", "inodes", "processes"}),
    dependencies=(
        ("swap", "memory", "a swap claim needs a memory claim"),
        ("tmpfs_inodes", "tmpfs", "an inode claim needs a tmpfs claim"),
    ),
)


def container_limits(
    resources: Mapping[str, int],
    contract: Mapping[str, Mapping[str, object]],
    target: EnforcementTarget = CONTAINER,
) -> ContainerLimits:
    """Translate a job's enforced claims into limits the target can apply, or refuse.

    Enforcement of a placed job is delegated to the executor.  A ``required`` claim of a
    kind the target cannot limit is refused rather than run unenforced; a best-effort one
    is skipped, as it would be on a host whose backend is unavailable.
    """
    selected: dict[str, int] = {}
    for name, units in resources.items():
        binding = contract.get(name, {})
        mode = binding.get("mode", "admission-only")
        if mode == "admission-only":
            continue
        kind = str(binding.get("kind"))
        field = {
            "cpu": "cpus",
            "memory": "memory",
            "memory-high": "memory_reservation",
            "swap": "swap",
            "tmpfs": "tmpfs",
            # Inodes are enforceable only as the inode limit of a claimed tmpfs.
            "inodes": "tmpfs_inodes",
            "processes": "processes",
        }.get(kind)
        if field is None or kind not in target.kinds:
            if mode == "required":
                raise ExecutorError(
                    f"required claim {name} of kind {kind} cannot be enforced in "
                    f"{target.description}"
                )
            continue
        selected[field] = units
    limits = ContainerLimits(**selected)
    for dependent, base, subject in target.dependencies:
        if getattr(limits, dependent) is not None and getattr(limits, base) is None:
            raise ExecutorError(f"{subject} to be enforced in {target.description}")
    return limits


@dataclass(frozen=True)
class Placement:
    """What the broker told an executor about the placed job it must run."""

    run_id: str
    max_runtime: int
    resources: Mapping[str, int]
    limits: ContainerLimits

    def machine_cpus(self) -> int | None:
        """CPUs a machine created for this job needs.

        An enforced CPU claim decides it.  Otherwise the claim named ``cpu`` does, by the
        same convention the pool's budget uses, so that a job whose claims are only
        admission units still gets a machine of the size it asked for.
        """
        return self.limits.cpus or self.resources.get("cpu")

    def machine_memory(self) -> int | None:
        """Bytes of memory a machine created for this job needs, by the same rule."""
        return self.limits.memory or self.resources.get("memory")


def placement(
    environment: Mapping[str, str],
    target: EnforcementTarget = CONTAINER,
) -> Placement:
    """Read the run ID, runtime limit, and claims the broker gave a placed job's executor."""
    try:
        run_id = environment["AGCOORD_RUN_ID"]
        max_runtime = int(environment["AGCOORD_OVERFLOW_MAX_RUNTIME"])
        resources = json.loads(environment["AGCOORD_OVERFLOW_RESOURCES"])
        contract = json.loads(environment["AGCOORD_OVERFLOW_CONTRACT"])
    except (KeyError, ValueError) as exc:
        raise ExecutorError(
            "this command is an overflow executor and must be started by the broker "
            "for a job placed in an overflow pool"
        ) from exc
    if max_runtime <= 0 or not isinstance(resources, dict) or not isinstance(contract, dict):
        raise ExecutorError("the broker's overflow placement is invalid")
    return Placement(
        run_id=run_id,
        max_runtime=max_runtime,
        resources=resources,
        limits=container_limits(resources, contract, target),
    )


def container_tool(preferred: str | None = None) -> str:
    """Return the local container tool: the one asked for, else Docker, else Podman."""
    for candidate in [preferred] if preferred else ["docker", "podman"]:
        if shutil.which(candidate):
            return candidate
    raise ExecutorError(
        "overflow needs Docker or Podman on this machine to build the job's image"
    )


def build_toolchain(tool: str, root: Path, config: RepositoryOverflow) -> str:
    """Return a local reference to the repository's toolchain image, building it if needed.

    A Dockerfile is built with the tool's own layer cache and tagged by its image ID, so
    an unchanged toolchain is found again without rebuilding or re-sending it.
    """
    if config.image is not None:
        return config.image
    assert config.dockerfile is not None
    completed = subprocess.run(
        [tool, "build", "-q", "-f", str(config.dockerfile), str(config.dockerfile.parent)],
        stdout=subprocess.PIPE,
        text=True,
    )
    image_id = completed.stdout.strip().removeprefix("sha256:")
    if completed.returncode != 0 or not image_id:
        raise ExecutorError(f"cannot build the toolchain image from {config.dockerfile}")
    reference = f"agcoord-toolchain:{image_id[:24]}"
    if subprocess.run([tool, "tag", image_id, reference]).returncode != 0:
        raise ExecutorError("cannot tag the toolchain image")
    return reference


def build_job_image(tool: str, root: Path, toolchain: str, image: str) -> int:
    """Build the job's image on this machine and return the number of source files in it."""
    build = subprocess.Popen(
        [tool, "build", "-q", "-t", image, "-"],
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
        raise ExecutorError("cannot build the job's image")
    return count


def command_line(arguments: Sequence[str]) -> list[str]:
    """Return the carried command that follows the executor's own ``--`` separator.

    Argument parsers differ on whether the separator itself is kept, so one leading
    ``--`` is dropped when present.
    """
    command = list(arguments[1:] if arguments[:1] == ["--"] else arguments)
    if not command:
        raise ExecutorError("an overflow executor needs a command after --")
    return command
