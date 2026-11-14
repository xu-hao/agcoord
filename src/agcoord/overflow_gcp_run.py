"""Overflow executor that runs a placed job as a Google Cloud Run job.

Configure it as a pool's executor::

    "executor": ["agc", "overflow-gcp-run",
                 "--project", "my-project", "--region", "us-central1",
                 "--registry", "us-central1-docker.pkg.dev/my-project/agcoord-overflow",
                 "--service-account", "agcoord-overflow-runner@my-project.iam.gserviceaccount.com"]

The broker starts it in the job's checkout as ``agc overflow-gcp-run OPTIONS -- COMMAND``.
It builds the job's image here, pushes it to the project's own Artifact Registry
repository, and creates and executes one Cloud Run job that exists only for this run.  It
relays the job's output from Cloud Logging and exits with the command's status.

Cloud Run needs no machine of its own but is smaller than Batch: one job gets at most 8
CPUs and 32 GiB of memory, and it can limit only CPU, memory, and in-memory scratch space.
It is not faster to start, and the container can see more processors than it was given.
Give a pool that uses this executor a matching ``max_claim``, and list in its
``capacities`` only the resources Cloud Run can provide.

Everything Google Cloud does is asked for through the operator's ``gcloud`` login.  The
project, repository, and service account are what the Terraform module in
``packaging/terraform/gcp-overflow`` provisions.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
import os
from pathlib import Path
import signal
import sys
import tempfile
from typing import Sequence

from agcoord.overflow_gcp import (
    Gcloud,
    LogRelay,
    Registry,
    follow_job,
    publish_job_image,
    resource_name,
)
from agcoord.overflow_image import (
    EXIT_NO_VERDICT,
    EnforcementTarget,
    ExecutorError,
    Placement,
    Stopped,
    build_toolchain,
    command_line,
    container_environment,
    container_tool,
    ignore_stop_signals,
    load_repository_config,
    placement,
    repository_root,
    run_detached,
    stop_on_signals,
)


MIB = 1024 * 1024
GIB = 1024 * MIB
#: The sizes one Cloud Run task can have: CPUs, least memory, most memory.
SIZES = (
    (1, 512 * MIB, 4 * GIB),
    (2, 512 * MIB, 8 * GIB),
    (4, 2 * GIB, 16 * GIB),
    (6, 4 * GIB, 24 * GIB),
    (8, 4 * GIB, 32 * GIB),
)
#: Cloud Run limits CPU and memory, and offers sized in-memory scratch space.  It has no
#: swap, so a limit on swap always holds.  It cannot limit processes or inodes.
#: Its in-memory volume API exposes no exec/noexec control or executability guarantee.
CLOUD_RUN = EnforcementTarget(
    description="a Cloud Run job",
    kinds=frozenset({"cpu", "memory", "swap", "tmpfs"}),
    executable_tmpfs=False,
)
#: Cloud Run job names leave room for the suffix Cloud Run adds to each execution's name.
NAME_LIMIT = 49
FINISHED = {"SUCCEEDED", "FAILED", "CANCELLED"}


@dataclass(frozen=True)
class RunSize:
    cpus: int
    memory: int

    @property
    def memory_mib(self) -> int:
        return -(-self.memory // MIB)


@dataclass(frozen=True)
class RunOptions:
    region: str
    service_account: str | None
    passed_environment: Sequence[str]
    poll_interval: float


def run_size(placed: Placement) -> RunSize:
    """Return the smallest Cloud Run size that holds the job's claim, or refuse.

    In-memory scratch space counts against a Cloud Run container's memory, so a tmpfs
    claim is added to the memory claim.  A job that claims no memory gets the most the
    chosen CPU count allows, 4 GiB for each CPU, as a general-purpose machine would have.
    """
    cpus = placed.machine_cpus() or 1
    claimed = placed.machine_memory()
    scratch = placed.limits.tmpfs or 0
    for size_cpus, least, most in SIZES:
        if size_cpus < cpus:
            continue
        if claimed is None:
            if scratch <= most:
                return RunSize(size_cpus, most)
        elif claimed + scratch <= most:
            return RunSize(size_cpus, max(claimed + scratch, least))
    largest_cpus, _least, largest_memory = SIZES[-1]
    wanted = f"{cpus} CPUs"
    if claimed is not None or scratch:
        wanted += f" and {((claimed or 0) + scratch) / GIB:.1f} GiB of memory and scratch"
    raise ExecutorError(
        f"Cloud Run runs at most {largest_cpus} CPUs and {largest_memory // GIB} GiB of "
        f"memory in one job; this job claims {wanted}"
    )


def job_spec(
    *,
    name: str,
    image: str,
    command: Sequence[str],
    placed: Placement,
    size: RunSize,
    options: RunOptions,
) -> dict[str, object]:
    """Return the Cloud Run job that runs one command once in one container."""
    container: dict[str, object] = {
        "image": image,
        # The first word replaces the image's entrypoint and the rest are its arguments, so
        # the command runs exactly as submitted whatever the toolchain image declares.
        "command": [command[0]],
        "args": list(command[1:]),
        "resources": {"limits": {"cpu": str(size.cpus), "memory": f"{size.memory_mib}Mi"}},
    }
    variables = container_environment(os.environ, options.passed_environment, placed.limits)
    if variables:
        container["env"] = [{"name": name, "value": value} for name, value in variables.items()]
    task: dict[str, object] = {
        # Cloud Run retries a failed task three times unless told otherwise, which would
        # run the command again without the broker knowing.
        "maxRetries": 0,
        # Cloud Run stops the task itself at the pool's runtime limit, so a lost broker or
        # executor cannot leave it billing.
        "timeoutSeconds": placed.max_runtime,
        "containers": [container],
    }
    if placed.limits.tmpfs is not None:
        container["volumeMounts"] = [{"name": "scratch", "mountPath": "/tmp"}]
        task["volumes"] = [
            {
                "name": "scratch",
                "emptyDir": {
                    "medium": "Memory",
                    "sizeLimit": f"{-(-placed.limits.tmpfs // MIB)}Mi",
                },
            }
        ]
    if options.service_account:
        task["serviceAccountName"] = options.service_account
    return {
        "apiVersion": "run.googleapis.com/v1",
        "kind": "Job",
        "metadata": {
            "name": name,
            "labels": {"agcoord-run": resource_name("run", placed.run_id)},
        },
        "spec": {"template": {"spec": {"taskCount": 1, "template": {"spec": task}}}},
    }


def _task_spec(job: object) -> dict[str, object]:
    """Return the task part of a Cloud Run job as ``gcloud`` prints it."""
    node = job
    for key in ("spec", "template", "spec", "template", "spec"):
        node = node.get(key) if isinstance(node, dict) else None
    return node if isinstance(node, dict) else {}


class RunJob:
    """One Cloud Run job and its single execution: state, output, and verdict."""

    def __init__(self, gcloud: Gcloud, region: str, name: str) -> None:
        self.gcloud = gcloud
        self.region = region
        self.name = name
        self.execution = ""
        self.output: LogRelay | None = None
        self._failure = ""

    def create(self, spec: dict[str, object]) -> None:
        with tempfile.TemporaryDirectory(prefix="agcoord-run-job-") as directory:
            path = Path(directory) / "job.yaml"
            # JSON is YAML, and it leaves nothing about quoting to chance.
            path.write_text(json.dumps(spec), encoding="utf-8")
            self.gcloud.run("run", "jobs", "replace", str(path), f"--region={self.region}")
        created = self.gcloud.json(
            "run", "jobs", "describe", self.name, f"--region={self.region}"
        )
        retries = _task_spec(created).get("maxRetries")
        if retries not in (None, 0):
            # Never let Cloud Run decide to run the command again.
            raise ExecutorError(
                f"Cloud Run would retry the job {retries} times; refusing to execute it"
            )

    def execute(self) -> None:
        execution = self.gcloud.json(
            "run", "jobs", "execute", self.name, f"--region={self.region}", "--async"
        )
        metadata = execution.get("metadata") if isinstance(execution, dict) else None
        if not isinstance(metadata, dict) or not metadata.get("name"):
            raise ExecutorError("Cloud Run started the job without returning its execution")
        self.execution = str(metadata["name"]).rsplit("/", 1)[-1]
        self.output = LogRelay(
            self.gcloud,
            'resource.type="cloud_run_job" '
            f'AND labels."run.googleapis.com/execution_name"="{self.execution}" '
            f'AND resource.labels.location="{self.region}"',
            str(metadata.get("creationTimestamp") or ""),
            is_output=lambda entry: str(entry.get("logName", "")).endswith(
                ("%2Fstdout", "%2Fstderr", "/stdout", "/stderr")
            ),
        )

    def state(self) -> str:
        execution = self.gcloud.json(
            "run", "jobs", "executions", "describe", self.execution, f"--region={self.region}"
        )
        status = execution.get("status") if isinstance(execution, dict) else None
        if not isinstance(status, dict):
            return "PENDING"
        for condition in status.get("conditions") or []:
            if isinstance(condition, dict) and condition.get("type") == "Completed":
                if condition.get("status") == "True":
                    return "SUCCEEDED"
                if condition.get("status") == "False":
                    self._failure = str(condition.get("message") or "")
                    return "CANCELLED" if status.get("cancelledCount") else "FAILED"
        if status.get("succeededCount"):
            return "SUCCEEDED"
        if status.get("failedCount"):
            return "FAILED"
        if status.get("cancelledCount"):
            return "CANCELLED"
        return "RUNNING" if status.get("runningCount") else "PENDING"

    def relay_output(self) -> int:
        """Print the output lines that have arrived since the last call; return how many."""
        return self.output.relay() if self.output is not None else 0

    def exit_status(self) -> int:
        """Return the command's exit status, or report that Cloud Run produced no verdict."""
        tasks = self.gcloud.json(
            "run",
            "jobs",
            "executions",
            "tasks",
            "list",
            f"--execution={self.execution}",
            f"--region={self.region}",
        )
        task = tasks[0] if isinstance(tasks, list) and tasks else {}
        result = ((task.get("status") or {}).get("lastAttemptResult") or {}) if task else {}
        code = result.get("exitCode")
        if isinstance(code, int) and code != 0:
            return code
        # Without an exit status the command did not end by itself: Cloud Run stopped it,
        # for example at a memory or time limit, or never started it.
        detail = str((result.get("status") or {}).get("message") or self._failure)
        signal_number = result.get("termSignal")
        if signal_number:
            detail = f"terminated with signal {signal_number}; {detail}"
        raise ExecutorError(
            "Cloud Run ended the job without an exit status from the command"
            + (f": {detail.strip('; ')}" if detail.strip("; ") else "")
        )

    def cancel_command(self) -> list[str]:
        return self.gcloud.command(
            "run", "jobs", "executions", "cancel", self.execution, f"--region={self.region}"
        )

    def delete_command(self) -> list[str]:
        return self.gcloud.command("run", "jobs", "delete", self.name, f"--region={self.region}")


def _say(message: str) -> None:
    print(f"agc overflow-gcp-run: {message}", file=sys.stderr, flush=True)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="agc overflow-gcp-run",
        description=(
            "AGCoord overflow executor: run a placed job as a Google Cloud Run job "
            "that exists only for that run."
        ),
    )
    parser.add_argument("--project", required=True, help="Google Cloud project ID")
    parser.add_argument("--region", required=True, help="region of the job, such as us-central1")
    parser.add_argument(
        "--registry",
        required=True,
        metavar="PREFIX",
        help="image path prefix, LOCATION-docker.pkg.dev/PROJECT/REPOSITORY",
    )
    parser.add_argument(
        "--service-account",
        metavar="EMAIL",
        help="identity the job runs as (default: the Compute Engine default)",
    )
    parser.add_argument("--runtime", help="local container tool (default: docker, else podman)")
    parser.add_argument(
        "--env",
        action="append",
        default=[],
        metavar="NAME",
        help="pass this environment variable into the container (repeatable)",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=5.0,
        metavar="SECONDS",
        help="seconds between checks of the job's state and output (default: 5)",
    )
    parser.add_argument("command", nargs=argparse.REMAINDER)
    return parser


def main(arguments: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(arguments)
    cleanup: list[list[str]] = []
    job: RunJob | None = None
    stop_on_signals()
    try:
        command = command_line(args.command)
        placed = placement(os.environ, CLOUD_RUN)
        # A job Cloud Run cannot hold is refused before anything is built or pushed.
        size = run_size(placed)
        root = repository_root(Path.cwd())
        config = load_repository_config(root)
        tool = container_tool(args.runtime)
        gcloud = Gcloud(args.project)
        registry = Registry(args.registry, tool, gcloud)
        options = RunOptions(
            region=args.region,
            service_account=args.service_account,
            passed_environment=args.env,
            poll_interval=max(args.poll_interval, 0.01),
        )

        toolchain = build_toolchain(tool, root, config)
        reference, count = publish_job_image(
            registry, root, config, toolchain, placed.run_id, cleanup
        )
        _say(f"pushed the job image, built from {count} source files")

        job = RunJob(gcloud, args.region, resource_name("agcoord", placed.run_id, NAME_LIMIT))
        cleanup.append(job.delete_command())
        job.create(
            job_spec(
                name=job.name,
                image=reference,
                command=command,
                placed=placed,
                size=size,
                options=options,
            )
        )
        job.execute()
        _say(
            f"started Cloud Run execution {job.execution} "
            f"with {size.cpus} CPUs and {size.memory_mib} MiB"
        )

        state = follow_job(
            state=job.state,
            relay=job.relay_output,
            finished=FINISHED,
            poll_interval=options.poll_interval,
            say=_say,
        )
        if state == "SUCCEEDED":
            status = 0
        elif state == "FAILED":
            status = job.exit_status()
        else:
            raise ExecutorError(f"the Cloud Run job ended as {state} without a verdict")
    except Stopped:
        ignore_stop_signals()
        # The execution is cancelled first: that is what stops the container.
        stopping = [job.cancel_command()] if job is not None and job.execution else []
        run_detached([*stopping, *reversed(cleanup)])
        return 128 + signal.SIGTERM
    except ExecutorError as error:
        _say(str(error))
        run_detached(list(reversed(cleanup)))
        return EXIT_NO_VERDICT
    run_detached(list(reversed(cleanup)))
    return status


if __name__ == "__main__":
    raise SystemExit(main())
