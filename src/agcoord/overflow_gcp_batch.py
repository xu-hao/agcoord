"""Overflow executor that runs a placed job on a Google Cloud Batch machine.

Configure it as a pool's executor::

    "executor": ["agc", "overflow-gcp-batch",
                 "--project", "my-project", "--region", "us-central1",
                 "--registry", "us-central1-docker.pkg.dev/my-project/agcoord-overflow",
                 "--service-account", "agcoord-overflow-runner@my-project.iam.gserviceaccount.com"]

The broker starts it in the job's checkout as ``agc overflow-gcp-batch OPTIONS -- COMMAND``.
It builds the job's image here, pushes it to the project's own Artifact Registry
repository, and submits one Batch job whose machine exists only for this run.  It relays
the job's output from Cloud Logging and exits with the command's status.  The machine
pulls the image with its own Google Cloud identity; it never holds forge credentials.

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
from typing import Sequence

from agcoord.overflow_gcp import (
    CloudJobOutput,
    Gcloud,
    LogRelay,
    Registry,
    follow_job,
    publish_job_image,
    resource_name,
)
from agcoord.overflow_gcp_key import (
    CREATE_COMMAND,
    CREDENTIALS_ENV,
    DEFAULT_KEY,
    KEY_OPTION,
    create_main,
    passed_environment,
)
from agcoord.overflow_image import (
    EXIT_NO_VERDICT,
    EXIT_RUNTIME_EXCEEDED,
    ExecutorError,
    Placement,
    Stopped,
    build_toolchain,
    command_line,
    container_command,
    container_environment,
    container_security_options,
    container_tool,
    ignore_stop_signals,
    load_repository_config,
    placement,
    repository_root,
    run_detached,
    stop_on_signals,
)


MIB = 1024 * 1024
#: Batch reports its own failures, as opposed to the command's, with exit codes from here.
BATCH_RESERVED = 50000
#: The reserved code for a task that ran past ``maxRunDuration``.
BATCH_RUNTIME_EXCEEDED = 50005
FINISHED = {"SUCCEEDED", "FAILED", "CANCELLED", "DELETION_IN_PROGRESS"}


@dataclass(frozen=True)
class BatchOptions:
    region: str
    service_account: str | None
    machine_type: str | None
    spot: bool
    passed_environment: Sequence[str]
    poll_interval: float
    privileged: bool = False


def job_config(
    *,
    image: str,
    command: Sequence[str],
    placed: Placement,
    options: BatchOptions,
    log_token: str | None = None,
) -> dict[str, object]:
    """Return the Batch job that runs one command in one container on one machine."""
    wrapped_command = container_command(command, log_token=log_token)
    container: dict[str, object] = {
        "imageUri": image,
        # The first word replaces the image's entrypoint and the rest are its arguments, so
        # the command runs exactly as submitted whatever the toolchain image declares.
        "entrypoint": wrapped_command[0],
        "commands": wrapped_command[1:],
    }
    container_options = list(container_security_options())
    if (options.privileged or placed.privileged) and "--privileged" not in container_options:
        container_options.append("--privileged")
    container_options.extend(placed.limits.run_options())
    if container_options:
        container["options"] = " ".join(container_options)
    runnable: dict[str, object] = {"container": container}
    variables = container_environment(os.environ, options.passed_environment, placed.limits)
    if variables:
        runnable["environment"] = {"variables": variables}

    compute: dict[str, int] = {}
    cpus = placed.machine_cpus()
    if cpus is not None:
        compute["cpuMilli"] = cpus * 1000
    memory = placed.machine_memory()
    if memory is not None:
        compute["memoryMib"] = -(-memory // MIB)

    task: dict[str, object] = {
        "runnables": [runnable],
        # The machine itself stops the task at the pool's runtime limit, so a lost broker
        # or executor cannot leave it billing.
        "maxRunDuration": f"{placed.max_runtime}s",
        # A retry would run the command a second time without the broker knowing.
        "maxRetryCount": 0,
    }
    if compute:
        task["computeResource"] = compute

    policy: dict[str, object] = {
        "provisioningModel": "SPOT" if options.spot else "STANDARD",
    }
    if options.machine_type:
        policy["machineType"] = options.machine_type
    allocation: dict[str, object] = {
        "instances": [{"policy": policy}],
        "location": {"allowedLocations": [f"regions/{options.region}"]},
    }
    if options.service_account:
        allocation["serviceAccount"] = {"email": options.service_account}

    return {
        "taskGroups": [{"taskCount": 1, "taskSpec": task}],
        "allocationPolicy": allocation,
        "logsPolicy": {"destination": "CLOUD_LOGGING"},
        "labels": {"agcoord-run": resource_name("run", placed.run_id)},
    }


class BatchJob(CloudJobOutput):
    """One submitted Batch job: its state, its output, and its verdict."""

    def __init__(self, gcloud: Gcloud, region: str, name: str) -> None:
        super().__init__()
        self.gcloud = gcloud
        self.region = region
        self.name = name
        self.path = f"projects/{gcloud.project}/locations/{region}/jobs/{name}"
        self.uid = ""

    def submit(self, config: dict[str, object]) -> None:
        job = self.gcloud.json(
            "batch",
            "jobs",
            "submit",
            self.name,
            f"--location={self.region}",
            "--config=-",
            stdin=json.dumps(config),
        )
        if not isinstance(job, dict) or not job.get("uid"):
            raise ExecutorError("Batch accepted the job without returning its identity")
        self.uid = str(job["uid"])
        self.output = LogRelay(
            self.gcloud,
            f'labels.job_uid="{self.uid}" AND log_id("batch_task_logs")',
            str(job.get("createTime") or ""),
            log_token=self.log_token,
        )

    def state(self) -> str:
        job = self.gcloud.json("batch", "jobs", "describe", self.path)
        if not isinstance(job, dict):
            raise ExecutorError("Batch returned an invalid job")
        return str((job.get("status") or {}).get("state") or "STATE_UNSPECIFIED")

    def exit_status(self) -> int:
        """Return the command's exit status, or report that Batch produced no verdict."""
        task = self.gcloud.json(
            "batch", "tasks", "describe", f"{self.path}/taskGroups/group0/tasks/0"
        )
        events = ((task or {}).get("status") or {}).get("statusEvents") or []
        codes = [
            event["taskExecution"]["exitCode"]
            for event in events
            if isinstance(event.get("taskExecution"), dict)
            and isinstance(event["taskExecution"].get("exitCode"), int)
        ]
        if not codes:
            raise ExecutorError("the Batch job failed without running the command")
        code = codes[-1]
        if code == BATCH_RUNTIME_EXCEEDED:
            _say("the job was stopped at the pool's runtime limit")
            return EXIT_RUNTIME_EXCEEDED
        if code >= BATCH_RESERVED:
            raise ExecutorError(
                f"Batch could not complete the job (reserved exit code {code}), "
                "for example because its machine was preempted"
            )
        if code == 0:
            raise ExecutorError("the Batch job failed although its command succeeded")
        return code

    def delete_command(self) -> list[str]:
        return self.gcloud.command("batch", "jobs", "delete", self.path)


def _say(message: str) -> None:
    print(f"agc overflow-gcp-batch: {message}", file=sys.stderr, flush=True)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="agc overflow-gcp-batch",
        description=(
            "AGCoord overflow executor: run a placed job on a Google Cloud Batch "
            "machine that exists only for that run."
        ),
        epilog=(
            "To create this workstation's result-cache client key, run "
            f"agc overflow-gcp-batch {CREATE_COMMAND} --help."
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
        help="identity of the job's machine (default: the Compute Engine default)",
    )
    parser.add_argument(
        "--machine-type",
        help="Compute Engine machine type (default: Batch chooses one that fits the claim)",
    )
    parser.add_argument(
        "--spot",
        action="store_true",
        help="use a Spot machine: cheaper, but it can be preempted, which ends the job "
        "without a verdict",
    )
    parser.add_argument("--runtime", help="local container tool (default: docker, else podman)")
    parser.add_argument(
        "--privileged",
        action="store_true",
        help="run the container with --privileged (also enabled when AGCOORD_OVERFLOW_PRIVILEGED=1)",
    )
    parser.add_argument(
        "--env",
        action="append",
        default=[],
        metavar="NAME",
        help="pass this environment variable into the container (repeatable)",
    )
    parser.add_argument(
        KEY_OPTION,
        metavar="PATH",
        help=(
            "this workstation's result-cache client key, which jobs that run on this "
            f"machine receive as {CREDENTIALS_ENV} (default: ~/{DEFAULT_KEY}); it never "
            "reaches a job placed here"
        ),
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
    selected = list(sys.argv[1:] if arguments is None else arguments)
    if selected[:1] == [CREATE_COMMAND]:
        return create_main("overflow-gcp-batch", selected[1:])
    args = _parser().parse_args(selected)
    cleanup: list[list[str]] = []
    stop_on_signals()
    try:
        command = command_line(args.command)
        placed = placement(os.environ)
        root = repository_root(Path.cwd())
        config = load_repository_config(root)
        tool = container_tool(args.runtime)
        gcloud = Gcloud(args.project)
        registry = Registry(args.registry, tool, gcloud)
        options = BatchOptions(
            region=args.region,
            service_account=args.service_account,
            machine_type=args.machine_type,
            spot=args.spot,
            passed_environment=passed_environment(args.env, _say),
            poll_interval=max(args.poll_interval, 0.01),
            privileged=args.privileged or placed.privileged,
        )

        toolchain = build_toolchain(tool, root, config)
        reference, count = publish_job_image(
            registry, root, config, toolchain, placed.run_id, cleanup
        )
        _say(f"pushed the job image, built from {count} source files")

        job = BatchJob(gcloud, args.region, resource_name("agcoord", placed.run_id))
        cleanup.append(job.delete_command())
        job.submit(
            job_config(
                image=reference,
                command=command,
                placed=placed,
                options=options,
                log_token=job.log_token,
            )
        )
        _say(f"submitted Batch job {job.name}; waiting for a machine")

        state = follow_job(
            state=job.state,
            relay=job.relay_output,
            finished=FINISHED,
            poll_interval=options.poll_interval,
            say=_say,
            has_pending=job.has_pending_output,
            flush=job.flush_output,
        )
        if state == "SUCCEEDED":
            status = 0
        elif state == "FAILED":
            status = job.exit_status()
        else:
            raise ExecutorError(f"the Batch job ended as {state} without a verdict")
    except Stopped:
        ignore_stop_signals()
        # The job is deleted first: that is what stops the machine.
        run_detached(list(reversed(cleanup)))
        return 128 + signal.SIGTERM
    except ExecutorError as error:
        _say(str(error))
        run_detached(list(reversed(cleanup)))
        return EXIT_NO_VERDICT
    run_detached(list(reversed(cleanup)))
    return status


if __name__ == "__main__":
    raise SystemExit(main())
