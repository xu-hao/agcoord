"""This workstation's key for the Google Cloud result cache, and the local jobs that use it.

The ``packaging/terraform/gcp-overflow`` module creates a result-cache client service account
for workstations.  A workstation holds one JSON key for it, by default at
``~/.config/agcoord/result-cache-client.json``.  ``agc overflow-gcp-batch
create-result-cache-key`` (the same command exists under ``overflow-gcp-run``) creates that key
through the operator's own ``gcloud``, readable only by this user, and never prints its private
material.

While the broker has a Google Cloud overflow pool and the key exists, a job admitted on this
machine receives ``GOOGLE_APPLICATION_CREDENTIALS`` naming the key unless its submitter set
that variable.  The broker adds it only to a run it admits locally: a job placed in a pool
reaches Google Cloud as its machine's service account, and the Google Cloud executors never
pass the variable into a job's container.  A machine without a Google Cloud pool, or without
the key, is left exactly as it was, with no warning.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
from pathlib import Path
import re
import sys
import tempfile
from typing import Callable, Iterable, Mapping, Sequence

from agcoord.overflow_gcp import Gcloud
from agcoord.overflow_image import ExecutorError


#: The built-in executors that run a placed job on Google Cloud.
EXECUTORS = ("overflow-gcp-batch", "overflow-gcp-run")
#: The variable through which Google Cloud client libraries find a service account key.
CREDENTIALS_ENV = "GOOGLE_APPLICATION_CREDENTIALS"
#: The executor option that names this workstation's key in place of the default path.
KEY_OPTION = "--result-cache-key"
#: Where this workstation keeps the key, relative to its home directory.
DEFAULT_KEY = Path(".config", "agcoord", "result-cache-client.json")
#: The subcommand of each Google Cloud executor that creates the key.
CREATE_COMMAND = "create-result-cache-key"
_KEY_ID = re.compile(r"^[0-9A-Za-z_-]{1,128}$")


def _executor_options(executor: Sequence[str]) -> list[str] | None:
    """Return the options of a built-in Google Cloud executor, or None for any other."""
    for index, word in enumerate(executor):
        if word in EXECUTORS:
            options = list(executor[index + 1 :])
            return options[: options.index("--")] if "--" in options else options
    return None


def _named_key(options: Sequence[str]) -> str | None:
    """Return the key one executor's options name, if they name one."""
    for index, word in enumerate(options):
        if word == KEY_OPTION:
            return options[index + 1] if index + 1 < len(options) else None
        if word.startswith(f"{KEY_OPTION}="):
            return word[len(KEY_OPTION) + 1 :]
    return None


def _home(environment: Mapping[str, str]) -> str:
    return environment.get("HOME") or os.path.expanduser("~")


def default_key(environment: Mapping[str, str] = os.environ) -> Path:
    """Return the key's default path under the home directory ``environment`` names."""
    return Path(_home(environment)) / DEFAULT_KEY


def result_cache_key(
    executors: Iterable[Sequence[str]],
    environment: Mapping[str, str],
) -> Path | None:
    """Return where local jobs find the key, or None when no pool runs on Google Cloud.

    The first Google Cloud pool whose executor names a key with ``--result-cache-key``
    decides; otherwise the key is at its default path.  ``~`` is the submitter's home.
    """
    google_cloud = [
        options for options in map(_executor_options, executors) if options is not None
    ]
    if not google_cloud:
        return None
    named = next((key for key in map(_named_key, google_cloud) if key is not None), None)
    if named is None:
        return default_key(environment)
    if named == "~" or named.startswith("~/"):
        return Path(_home(environment) + named[1:])
    return Path(named)


def local_job_environment(
    executors: Iterable[Sequence[str]],
    environment: Mapping[str, str],
) -> dict[str, str]:
    """Return what a job admitted on this machine receives to reach the result cache.

    That is ``GOOGLE_APPLICATION_CREDENTIALS`` naming the key, when a configured pool runs
    on Google Cloud, the key file exists, and the submitter did not set the variable, even
    to an empty value.  Otherwise it is nothing, so a machine that does not use Google
    Cloud overflow never needs a key and never hears about one.
    """
    if CREDENTIALS_ENV in environment:
        return {}
    key = result_cache_key(executors, environment)
    if key is None or not key.is_absolute() or not os.path.isfile(key):
        return {}
    return {CREDENTIALS_ENV: str(key)}


def passed_environment(names: Sequence[str], say: Callable[[str], None]) -> list[str]:
    """Remove the workstation credentials variable from what a Google Cloud job receives.

    A job reaches Google Cloud as its machine's service account.  A workstation key path
    does not exist in its container, and naming one would stop client libraries there from
    using that account.
    """
    if CREDENTIALS_ENV not in names:
        return list(names)
    say(f"{CREDENTIALS_ENV} is not passed to the job, which uses its service account")
    return [name for name in names if name != CREDENTIALS_ENV]


def _key_id(path: Path) -> str | None:
    """Read only the ID of the key in one key file; its private material is never shown."""
    try:
        if not path.is_file():
            return None
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ValueError):
        return None
    key_id = document.get("private_key_id") if isinstance(document, dict) else None
    return key_id if isinstance(key_id, str) and _KEY_ID.fullmatch(key_id) else None


def _revoke(key_id: str, account: str, project: str) -> str:
    return (
        f"gcloud iam service-accounts keys delete {key_id} "
        f"--iam-account={account} --project={project}"
    )


def _install(staged: Path, output: Path, *, replace: bool) -> None:
    """Move a complete key into place, never over another file unless replacing it."""
    if replace:
        os.replace(staged, output)
        return
    try:
        # A hard link fails rather than replace a file that appeared in the meantime.
        os.link(staged, output)
    except FileExistsError:
        raise
    except OSError:
        # A filesystem without hard links: the name was free a moment ago.
        if os.path.lexists(output):
            raise FileExistsError(output) from None
        os.rename(staged, output)


def create_key(
    gcloud: Gcloud,
    account: str,
    output: Path,
    *,
    replace: bool = False,
) -> list[str]:
    """Create a new key for ``account`` at ``output``, readable only by this user.

    ``gcloud`` writes the key under umask 077 into a private directory beside ``output``,
    and the complete key is moved into place from there, so a failure leaves no partial key
    and a replaced key stays in place until its successor is whole.  An existing file is
    never overwritten unless ``replace`` is set.  Returns the lines that report what was
    done; none of them contains private key material.
    """
    if output.is_dir():
        raise ExecutorError(f"{output} is a directory")
    exists = os.path.lexists(output)
    if exists and not replace:
        raise ExecutorError(
            f"{output} already exists; pass --replace to put a new key in its place"
        )
    replaced = _key_id(output) if exists else None
    output.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=".result-cache-key-", dir=output.parent))
    staged = staging / "key.json"
    try:
        gcloud.run(
            "iam",
            "service-accounts",
            "keys",
            "create",
            str(staged),
            f"--iam-account={account}",
            umask=0o077,
        )
        if not staged.is_file():
            raise ExecutorError("gcloud reported success but wrote no key")
        staged.chmod(0o600)
        created = _key_id(staged)
        try:
            _install(staged, output, replace=replace)
        except FileExistsError:
            revoke = (
                f"; revoke it with: {_revoke(created, account, gcloud.project)}"
                if created
                else ""
            )
            raise ExecutorError(
                f"{output} appeared while the key was being created, so the new key "
                f"was discarded{revoke}"
            ) from None
    finally:
        with contextlib.suppress(FileNotFoundError):
            staged.unlink()
        with contextlib.suppress(OSError):
            staging.rmdir()
    lines = [
        f"created key {created or '(unknown ID)'} for {account} at {output}, "
        "readable only by you"
    ]
    if replaced is not None and replaced != created:
        lines.append(
            f"the key it replaced, {replaced}, stays valid until you revoke it: "
            + _revoke(replaced, account, gcloud.project)
        )
    if output == default_key():
        lines.append(
            f"jobs that run on this machine receive it as {CREDENTIALS_ENV} while the "
            "broker has a Google Cloud overflow pool"
        )
    else:
        lines.append(
            f"jobs that run on this machine receive it as {CREDENTIALS_ENV} once each "
            f"Google Cloud pool's executor has {KEY_OPTION} {output}"
        )
    return lines


def _parser(executor: str) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=f"agc {executor} {CREATE_COMMAND}",
        description=(
            "Create this workstation's key for the GCP result-cache client service account "
            "through your gcloud login, readable only by you. The private key is never "
            "printed."
        ),
    )
    parser.add_argument(
        "--project",
        required=True,
        help="the overflow project ID (the Terraform module's project_id output)",
    )
    parser.add_argument(
        "--iam-account",
        required=True,
        metavar="EMAIL",
        help="the client service account (the module's result_cache_client_email output)",
    )
    parser.add_argument(
        "--output",
        metavar="PATH",
        help=f"where to write the key (default: ~/{DEFAULT_KEY})",
    )
    parser.add_argument(
        "--replace",
        action="store_true",
        help="put a new key in place of an existing key file; the old key stays valid "
        "until you revoke it",
    )
    return parser


def create_main(executor: str, arguments: Sequence[str]) -> int:
    """Run ``agc EXECUTOR create-result-cache-key``; exit 1 when it refuses or fails."""
    args = _parser(executor).parse_args(arguments)
    try:
        if args.output == "-":
            raise ExecutorError("the key is never written to standard output")
        output = (
            default_key()
            if args.output is None
            else Path(os.path.expanduser(args.output)).absolute()
        )
        lines = create_key(Gcloud(args.project), args.iam_account, output, replace=args.replace)
    except (ExecutorError, OSError) as error:
        print(f"agc {executor} {CREATE_COMMAND}: error: {error}", file=sys.stderr)
        return 1
    for line in lines:
        print(line)
    return 0
