"""What the Google Cloud overflow executors share: ``gcloud`` and the image registry.

Everything goes through the operator's own ``gcloud`` command and its existing login, so
the executors hold no credentials of their own and add no Python dependencies.
"""

from __future__ import annotations

from contextlib import contextmanager
import json
import os
import re
import subprocess
import tempfile
from typing import Iterator, Sequence

from agcoord.overflow_image import ExecutorError


#: The package names under which images are pushed.  The cleanup policies of the
#: ``packaging/terraform/gcp-overflow`` module match these names.
TOOLCHAIN_PACKAGE = "toolchain"
JOB_PACKAGE = "run"


class Gcloud:
    """Run ``gcloud`` commands against one project."""

    def __init__(self, project: str) -> None:
        self.project = project

    def command(self, *arguments: str) -> list[str]:
        return ["gcloud", *arguments, f"--project={self.project}", "--quiet"]

    def run(self, *arguments: str, stdin: str | None = None) -> str:
        """Run one command and return its output, or fail without a verdict."""
        try:
            completed = subprocess.run(
                self.command(*arguments),
                input=stdin,
                stdin=None if stdin is not None else subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                text=True,
            )
        except FileNotFoundError as exc:
            raise ExecutorError("gcloud is not installed on this machine") from exc
        if completed.returncode != 0:
            raise ExecutorError(f"gcloud {' '.join(arguments[:3])} failed")
        return completed.stdout

    def json(self, *arguments: str, stdin: str | None = None) -> object:
        output = self.run(*arguments, "--format=json", stdin=stdin)
        try:
            return json.loads(output or "null")
        except json.JSONDecodeError as exc:
            raise ExecutorError(
                f"gcloud {' '.join(arguments[:3])} returned output that is not JSON"
            ) from exc


def resource_name(prefix: str, run_id: str) -> str:
    """Return a name Google Cloud accepts for a job: lowercase, digits, and hyphens."""
    name = re.sub(r"[^a-z0-9-]+", "-", f"{prefix}-{run_id}".lower())
    return name[:63].strip("-")


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
