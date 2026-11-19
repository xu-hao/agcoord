"""What the Google Cloud overflow executors share: ``gcloud`` and the image registry.

Everything goes through the operator's own ``gcloud`` command and its existing login, so
the executors hold no credentials of their own and add no Python dependencies.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import re
import secrets
import subprocess
import sys
import tempfile
import time
from typing import Callable, Collection, Iterator, Sequence

from agcoord.overflow_image import ExecutorError, RepositoryOverflow, build_job_image


#: The package names under which images are pushed.  The cleanup policies of the
#: ``packaging/terraform/gcp-overflow`` module match these names.
TOOLCHAIN_PACKAGE = "toolchain"
JOB_PACKAGE = "run"
_RFC3339 = re.compile(
    r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.\d+)?(Z|[+-]\d{2}:\d{2})$"
)


def _parse_rfc3339(raw: str) -> datetime | None:
    """Parse an RFC 3339 timestamp to UTC whole seconds."""
    match = _RFC3339.fullmatch(raw.strip())
    if match is None:
        return None
    whole, zone = match.groups()
    try:
        return datetime.fromisoformat(f"{whole}{zone}").astimezone(timezone.utc)
    except ValueError:
        return None


def _format_rfc3339(moment: datetime) -> str:
    return moment.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class Gcloud:
    """Run ``gcloud`` commands against one project."""

    def __init__(self, project: str) -> None:
        self.project = project

    def command(self, *arguments: str) -> list[str]:
        return ["gcloud", *arguments, f"--project={self.project}", "--quiet"]

    def run(self, *arguments: str, stdin: str | None = None) -> str:
        """Run one command and return its output, or fail without a verdict.

        ``gcloud`` reports progress on standard error even with ``--quiet``: "Creating
        job... Done.", advice on what to run next, and console links that name the
        project.  The executor's standard error is the job's log, so what ``gcloud`` says
        there is kept out of it unless the command fails, when it explains the failure.
        """
        try:
            completed = subprocess.run(
                self.command(*arguments),
                input=stdin,
                stdin=None if stdin is not None else subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
        except FileNotFoundError as exc:
            raise ExecutorError("gcloud is not installed on this machine") from exc
        if completed.returncode != 0:
            detail = " ".join(completed.stderr.split())
            raise ExecutorError(
                f"gcloud {' '.join(arguments[:3])} failed" + (f": {detail}" if detail else "")
            )
        return completed.stdout

    def json(self, *arguments: str, stdin: str | None = None) -> object:
        output = self.run(*arguments, "--format=json", stdin=stdin)
        try:
            return json.loads(output or "null")
        except json.JSONDecodeError as exc:
            raise ExecutorError(
                f"gcloud {' '.join(arguments[:3])} returned output that is not JSON"
            ) from exc


#: Lookback applied to ``receiveTimestamp`` so shards indexed moments apart are not missed.
RECEIVE_LOOKBACK_SECONDS = 30


def new_log_token() -> str:
    """Return a per-job correlation token for sequence-tagging remote output."""
    return secrets.token_hex(6)


class LogRelay:
    """Print the Cloud Logging entries of one job in emission order.

    Cloud Logging is read by polling.  ``timestamp>=`` stays anchored at the job's start
    so buffered lines or entries indexed out of emission-timestamp order are never skipped,
    while ``receiveTimestamp>=`` moves forward with a lookback window and ``insertId``
    deduplication processes each entry once.  When ``log_token`` is provided, sequence-tagged
    lines are reassembled by contiguous sequence number so out-of-order upload chunks wait
    for earlier chunks rather than being written prematurely to standard output.
    """

    def __init__(
        self,
        gcloud: Gcloud,
        log_filter: str,
        since: str,
        is_output: Callable[[dict[str, object]], bool] = lambda entry: True,
        *,
        log_token: str | None = None,
    ) -> None:
        self.gcloud = gcloud
        self.log_filter = log_filter
        self.is_output = is_output
        self._since = since or "1970-01-01T00:00:00Z"
        self._latest_received: datetime | None = None
        self._seen: set[str] = set()
        self._tag_pattern = (
            re.compile(rf"^{re.escape(log_token)}:(\d+):([cp]):(.*)$")
            if log_token is not None
            else None
        )
        self._next_seq = 0
        self._pending: dict[int, str] = {}

    def _emit_ready(self) -> None:
        wrote = False
        while self._next_seq in self._pending:
            sys.stdout.write(self._pending.pop(self._next_seq))
            self._next_seq += 1
            wrote = True
        if wrote:
            sys.stdout.flush()

    def has_pending(self) -> bool:
        """Return whether out-of-order chunks are still waiting for an earlier sequence."""
        return bool(self._pending)

    def flush(self) -> bool:
        """Emit any remaining buffered chunks in sequence order; report if a gap existed."""
        if not self._pending:
            return False
        had_gap = min(self._pending) > self._next_seq
        for seq in sorted(self._pending):
            sys.stdout.write(self._pending.pop(seq))
            self._next_seq = max(self._next_seq, seq + 1)
        sys.stdout.flush()
        return had_gap

    def relay(self) -> int:
        """Read newly arrived entries, print ready output in sequence, and return item count.

        The job's own output goes to standard output unchanged.  Anything else the
        platform logged about the job goes to standard error, so it reaches the job's log
        without being mistaken for the command's output.
        """
        query = f'{self.log_filter} AND timestamp>="{self._since}"'
        if self._latest_received is not None:
            lookback = self._latest_received - timedelta(seconds=RECEIVE_LOOKBACK_SECONDS)
            query += f' AND receiveTimestamp>="{_format_rfc3339(lookback)}"'
        entries = self.gcloud.json(
            "logging",
            "read",
            query,
            "--order=asc",
        )
        received = 0
        wrote_stdout = False
        wrote_stderr = False
        for entry in entries if isinstance(entries, list) else []:
            if not isinstance(entry, dict):
                continue
            received_at = _parse_rfc3339(str(entry.get("receiveTimestamp") or ""))
            if received_at is not None and (
                self._latest_received is None or received_at > self._latest_received
            ):
                self._latest_received = received_at
            identity = str(entry.get("insertId") or "")
            if identity:
                if identity in self._seen:
                    continue
                self._seen.add(identity)
            if isinstance(entry.get("textPayload"), str):
                text = entry["textPayload"]
            elif entry.get("jsonPayload") is not None:
                # A line the command printed as JSON is stored parsed; print it as JSON.
                text = json.dumps(entry["jsonPayload"], separators=(",", ":"))
            else:
                continue
            if not self.is_output(entry):
                sys.stderr.write(text if text.endswith("\n") else text + "\n")
                wrote_stderr = True
                received += 1
                continue
            lines = text[:-1].split("\n") if text.endswith("\n") else text.split("\n")
            for line in lines:
                match = self._tag_pattern.fullmatch(line) if self._tag_pattern is not None else None
                if match is None:
                    sys.stdout.write(line + "\n")
                    wrote_stdout = True
                    received += 1
                    continue
                seq = int(match.group(1))
                kind = match.group(2)
                payload = match.group(3)
                if seq >= self._next_seq and seq not in self._pending:
                    self._pending[seq] = payload + ("\n" if kind == "c" else "")
                received += 1
        if wrote_stdout:
            sys.stdout.flush()
        if wrote_stderr:
            sys.stderr.flush()
        self._emit_ready()
        return received


class CloudJobOutput:
    """Shared log-token and relay lifecycle for Google Cloud overflow jobs."""

    def __init__(self) -> None:
        self.log_token = new_log_token()
        self.output: LogRelay | None = None

    def relay_output(self) -> int:
        """Relay newly arrived Cloud Logging entries; return how many arrived."""
        return self.output.relay() if self.output is not None else 0

    def has_pending_output(self) -> bool:
        """Say whether any sequence-tagged chunks are waiting for an earlier sequence."""
        return self.output.has_pending() if self.output is not None else False

    def flush_output(self) -> bool:
        """Flush any remaining buffered chunks in sequence order."""
        return self.output.flush() if self.output is not None else False


#: Consecutive failed checks of a job's state after which contact counts as lost.
LOST_CONTACT_CHECKS = 12
#: Minimum post-finish reads so entries indexed seconds after exit are not missed.
LOG_DRAIN_READS = 3
#: Upper bound on post-finish reads when output continues arriving across polls.
MAX_LOG_DRAIN_READS = 6


def follow_job(
    *,
    state: Callable[[], str],
    relay: Callable[[], int],
    finished: Collection[str],
    poll_interval: float,
    say: Callable[[str], None],
    has_pending: Callable[[], bool] = lambda: False,
    flush: Callable[[], bool] = lambda: False,
) -> str:
    """Relay a running job's output until the job ends, and return its final state.

    A job can run for an hour, and one failed ``gcloud`` call in that time is not a reason
    to give up on it.  A check that fails is repeated at the next poll; only a run of
    failures as long as ``LOST_CONTACT_CHECKS`` ends the job without a verdict.  Output
    that cannot be read is read again later, and its loss is reported, never fatal.
    """
    current = ""
    failures = 0
    try:
        while current not in finished:
            time.sleep(poll_interval)
            try:
                observed = state()
            except ExecutorError as error:
                failures += 1
                if failures >= LOST_CONTACT_CHECKS:
                    raise ExecutorError(f"lost contact with the job: {error}") from error
                continue
            failures = 0
            if observed != current:
                current = observed
                say(f"job is {current}")
            try:
                relay()
            except ExecutorError:
                pass
        for drain_index in range(MAX_LOG_DRAIN_READS):
            time.sleep(poll_interval)
            try:
                printed = relay()
            except ExecutorError:
                say("some of the job's output could not be read from Cloud Logging")
                break
            if drain_index + 1 >= LOG_DRAIN_READS and printed == 0 and not has_pending():
                break
    finally:
        if flush():
            say("remote output had missing sequence numbers; remaining lines were written in sequence order")
    return current


def resource_name(prefix: str, run_id: str, limit: int = 63) -> str:
    """Return a name Google Cloud accepts for a job: lowercase, digits, and hyphens."""
    name = re.sub(r"[^a-z0-9-]+", "-", f"{prefix}-{run_id}".lower())
    return name[:limit].strip("-")


class Registry:
    """An Artifact Registry Docker repository, named by its image path prefix."""

    def __init__(self, prefix: str, tool: str, gcloud: Gcloud) -> None:
        prefix = prefix.rstrip("/")
        if not re.fullmatch(r"[a-z0-9-]+-docker\.pkg\.dev/[^/\s]+/[^/\s]+", prefix):
            raise ExecutorError(
                "--registry must be LOCATION-docker.pkg.dev/PROJECT/REPOSITORY"
            )
        self.prefix = prefix
        self.host = prefix.split("/", 1)[0]
        self.tool = tool
        self.gcloud = gcloud

    def toolchain_image(self, local_reference: str) -> str:
        tag = local_reference.rsplit(":", 1)[-1]
        return f"{self.prefix}/{TOOLCHAIN_PACKAGE}:{tag}"

    def job_image(self, run_id: str) -> str:
        tag = re.sub(r"[^A-Za-z0-9_.-]+", "-", run_id)[:128]
        return f"{self.prefix}/{JOB_PACKAGE}:{tag}"

    @contextmanager
    def login(self) -> Iterator[dict[str, str]]:
        """Yield an environment in which the container tool may push to the registry.

        The short-lived access token is written to a private temporary directory that is
        removed afterwards, so the operator's own container configuration is neither read
        nor changed and the token never appears on a command line.
        """
        token = self.gcloud.run("auth", "print-access-token").strip()
        if not token:
            raise ExecutorError("gcloud returned no access token; run gcloud auth login")
        with tempfile.TemporaryDirectory(prefix="agcoord-registry-") as directory:
            environment = dict(os.environ)
            environment["DOCKER_CONFIG"] = directory
            environment["REGISTRY_AUTH_FILE"] = os.path.join(directory, "auth.json")
            login = subprocess.run(
                [self.tool, "login", "--username=oauth2accesstoken", "--password-stdin", self.host],
                input=token,
                text=True,
                env=environment,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
            )
            if login.returncode != 0:
                detail = " ".join(login.stderr.split())
                raise ExecutorError(
                    f"cannot log in to {self.host}" + (f": {detail}" if detail else "")
                )
            # A successful login is silent.  Docker warns that the token is stored
            # unencrypted, which is advice about an operator's lasting configuration; here
            # the token is short-lived and its directory is removed after the push.
            yield environment

    def tag(self, source: str, target: str) -> None:
        if subprocess.run([self.tool, "tag", source, target]).returncode != 0:
            raise ExecutorError(f"cannot tag {target}")

    def push(self, image: str, environment: dict[str, str]) -> str:
        """Push one image and return the reference a machine should pull.

        The reference names the pushed content by digest when the tool reports one, so
        the job runs exactly the image built for it.
        """
        pushed = subprocess.run(
            [self.tool, "push", "--quiet", image],
            env=environment,
            stdout=subprocess.DEVNULL,
        )
        if pushed.returncode != 0:
            raise ExecutorError(f"cannot push {image}")
        inspected = subprocess.run(
            [self.tool, "image", "inspect", "--format", "{{json .RepoDigests}}", image],
            stdout=subprocess.PIPE,
            text=True,
        )
        repository = image.rsplit(":", 1)[0]
        try:
            digests = json.loads(inspected.stdout or "[]") or []
        except json.JSONDecodeError:
            digests = []
        for digest in digests:
            if isinstance(digest, str) and digest.startswith(f"{repository}@sha256:"):
                return digest
        return image

    def remote_delete_command(self, image: str) -> list[str]:
        return self.gcloud.command(
            "artifacts", "docker", "images", "delete", image, "--delete-tags"
        )

    def local_delete_command(self, images: Sequence[str]) -> list[str]:
        return [self.tool, "rmi", "-f", *images]


def publish_job_image(
    registry: Registry,
    root: Path,
    config: RepositoryOverflow,
    toolchain: str,
    run_id: str,
    cleanup: list[list[str]],
) -> tuple[str, int]:
    """Build a job's image, push it, and return its reference and source file count.

    ``cleanup`` receives the commands that remove each copy of the job's image, as soon
    as that copy can exist, so that a failure or a stop part-way leaves nothing behind.
    """
    image = registry.job_image(run_id)
    count = build_job_image(registry.tool, root, toolchain, image)
    cleanup.append(registry.local_delete_command([image]))
    with registry.login() as environment:
        if config.dockerfile is not None:
            # Keeping the toolchain in the registry under its own name means later jobs
            # upload only their source layer.
            published = registry.toolchain_image(toolchain)
            registry.tag(toolchain, published)
            registry.push(published, environment)
        cleanup.append(registry.remote_delete_command(image))
        reference = registry.push(image, environment)
    return reference, count
