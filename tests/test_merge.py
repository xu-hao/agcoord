"""Forge-neutral behavior for publishing an exact receipt-backed change."""

from __future__ import annotations

from dataclasses import dataclass
from io import StringIO
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

import pytest


GIT = shutil.which("git") or "git"
PR_NUMBER = 123
BRANCH = "feature/atomic-publication"


@dataclass(frozen=True)
class Repository:
    checkout: Path
    remote: Path
    main_sha: str
    head_sha: str


class MetadataClient:
    def __init__(self, record: dict[str, object]):
        self.record = record
        self.calls: list[int] = []

    def pull_request(self, number: int) -> dict[str, object]:
        self.calls.append(number)
        return dict(self.record)


class Publisher:
    """Forge double that creates objects and updates both refs transactionally."""

    def __init__(
        self,
        repository: Repository,
        *,
        race_ref: str | None = None,
        race_sha: str | None = None,
        fail_update: bool = False,
        branch: str = BRANCH,
    ):
        self.repository = repository
        self.race_ref = race_ref
        self.race_sha = race_sha
        self.fail_update = fail_update
        self.branch = branch
        self.create_calls: list[dict[str, object]] = []
        self.created: list[dict[str, object]] = []
        self.update_calls: list[tuple[dict[str, object], ...]] = []
        self.ancestor_calls: list[tuple[str, str]] = []
        self.refs_around_create: list[tuple[tuple[str, str], tuple[str, str]]] = []

    def _refs(self) -> tuple[str, str]:
        return (
            _remote_sha(self.repository, "refs/heads/main"),
            _remote_sha(self.repository, f"refs/heads/{self.branch}"),
        )

    def create_merge_commit(
        self,
        *,
        message: str,
        tree_sha: str,
        parent_shas: tuple[str, str],
    ) -> dict[str, object]:
        call = {
            "message": message,
            "tree_sha": tree_sha,
            "parent_shas": parent_shas,
        }
        self.create_calls.append(call)
        before = self._refs()
        environment = dict(os.environ)
        environment.update(
            {
                "GIT_AUTHOR_NAME": "AGCoord test forge",
                "GIT_AUTHOR_EMAIL": "agcoord@example.invalid",
                "GIT_COMMITTER_NAME": "AGCoord test forge",
                "GIT_COMMITTER_EMAIL": "agcoord@example.invalid",
            }
        )
        completed = subprocess.run(
            [
                GIT,
                "--git-dir",
                str(self.repository.remote),
                "commit-tree",
                tree_sha,
                "-p",
                parent_shas[0],
                "-p",
                parent_shas[1],
            ],
            check=False,
            input=f"{message}\n",
            text=True,
            capture_output=True,
            env=environment,
        )
        assert completed.returncode == 0, completed.stderr
        result: dict[str, object] = {
            "sha": completed.stdout.strip(),
            "tree_sha": tree_sha,
            "parent_shas": parent_shas,
        }
        self.created.append(result)
        after = self._refs()
        self.refs_around_create.append((before, after))
        return dict(result)

    def update_refs(self, updates: tuple[dict[str, object], ...]) -> None:
        copied = tuple(dict(update) for update in updates)
        self.update_calls.append(copied)
        if self.race_ref is not None:
            assert self.race_sha is not None
            _git(
                self.repository.remote,
                "update-ref",
                self.race_ref,
                self.race_sha,
            )

        for update in copied:
            name = update["name"]
            before_oid = update["before_oid"]
            assert isinstance(name, str)
            assert isinstance(before_oid, str)
            if _sha(self.repository.remote, name) != before_oid:
                raise RuntimeError(f"beforeOid mismatch for {name}")
        if self.fail_update:
            raise RuntimeError("injected atomic ref update failure")

        commands = ["start"]
        commands.extend(
            f"update {update['name']} {update['after_oid']} {update['before_oid']}"
            for update in copied
        )
        commands.extend(("prepare", "commit"))
        _git(
            self.repository.remote,
            "update-ref",
            "--stdin",
            input_text="\n".join(commands) + "\n",
        )

    def is_ancestor(self, *, ancestor_sha: str, descendant_sha: str) -> bool:
        self.ancestor_calls.append((ancestor_sha, descendant_sha))
        result = _git(
            self.repository.remote,
            "merge-base",
            "--is-ancestor",
            ancestor_sha,
            descendant_sha,
            check=False,
        )
        assert result.returncode in {0, 1}, result.stderr
        return result.returncode == 0


def _git(
    repository: Path,
    *arguments: str,
    input_text: str | None = None,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(
        [GIT, "-C", str(repository), *arguments],
        check=False,
        input=input_text,
        text=True,
        capture_output=True,
    )
    if check and completed.returncode != 0:
        pytest.fail(
            f"git {' '.join(arguments)} failed ({completed.returncode}): "
            f"{completed.stderr}"
        )
    return completed


def _sha(repository: Path, revision: str) -> str:
    return _git(repository, "rev-parse", revision).stdout.strip()


def _remote_sha(repository: Repository, reference: str) -> str:
    result = _git(
        repository.checkout,
        "ls-remote",
        "origin",
        reference,
    ).stdout.strip()
    assert result, f"remote reference {reference} is absent"
    return result.split()[0]


def _commit_object(repository: Repository, parent: str, message: str) -> str:
    tree = _sha(repository.checkout, f"{parent}^{{tree}}")
    environment = dict(os.environ)
    environment.update(
        {
            "GIT_AUTHOR_NAME": "AGCoord test",
            "GIT_AUTHOR_EMAIL": "agcoord@example.invalid",
            "GIT_COMMITTER_NAME": "AGCoord test",
            "GIT_COMMITTER_EMAIL": "agcoord@example.invalid",
        }
    )
    completed = subprocess.run(
        [GIT, "-C", str(repository.checkout), "commit-tree", tree, "-p", parent],
        check=False,
        input=f"{message}\n",
        text=True,
        capture_output=True,
        env=environment,
    )
    assert completed.returncode == 0, completed.stderr
    return completed.stdout.strip()


def _transfer_race_object(repository: Repository, sha: str, name: str) -> None:
    """Make a race commit available to the bare remote without moving either leased ref."""
    _git(
        repository.checkout,
        "push",
        "origin",
        f"{sha}:refs/test-fixtures/{name}",
    )


def _advance_remote_main(
    repository: Repository,
    checkout: Path,
    changes: dict[str, str],
) -> str:
    subprocess.run(
        [GIT, "clone", "--branch", "main", str(repository.remote), str(checkout)],
        check=True,
        text=True,
        capture_output=True,
    )
    _git(checkout, "config", "user.name", "AGCoord target test")
    _git(checkout, "config", "user.email", "target@example.invalid")
    for name, content in changes.items():
        (checkout / name).write_text(content, encoding="utf-8")
    _git(checkout, "add", *changes)
    _git(checkout, "commit", "-m", "advance target")
    _git(checkout, "push", "origin", "main")
    return _sha(checkout, "HEAD")


def _add_feature_checkout(
    repository: Repository,
    checkout: Path,
    branch: str,
) -> Repository:
    subprocess.run(
        [GIT, "clone", "--branch", "main", str(repository.remote), str(checkout)],
        check=True,
        text=True,
        capture_output=True,
    )
    _git(checkout, "config", "user.name", "AGCoord queued land test")
    _git(checkout, "config", "user.email", "queued-land@example.invalid")
    main_sha = _sha(checkout, "HEAD")
    _git(checkout, "switch", "-c", branch)
    (checkout / "second.txt").write_text("second land\n", encoding="utf-8")
    _git(checkout, "add", "second.txt")
    _git(checkout, "commit", "-m", "second queued land")
    head_sha = _sha(checkout, "HEAD")
    _git(checkout, "push", "-u", "origin", branch)
    return Repository(checkout, repository.remote, main_sha, head_sha)


def _install_sync_push_fault(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    remote: Path,
    source_ref: str,
    race_sha: str | None = None,
    fail_push: bool = False,
) -> None:
    bin_dir = tmp_path / "sync-fault-bin"
    bin_dir.mkdir()
    wrapper = bin_dir / "git"
    wrapper.write_text(
        f"""#!{sys.executable}
import os
import subprocess
import sys

arguments = sys.argv[1:]
sync_push = "push" in arguments and any(
    argument.startswith("--force-with-lease=") for argument in arguments
)
if sync_push and os.environ.get("AGCOORD_TEST_RACE_SHA"):
    subprocess.run(
        [
            os.environ["AGCOORD_TEST_REAL_GIT"],
            "-C",
            os.environ["AGCOORD_TEST_RACE_REMOTE"],
            "update-ref",
            os.environ["AGCOORD_TEST_RACE_REF"],
            os.environ["AGCOORD_TEST_RACE_SHA"],
        ],
        check=True,
    )
if sync_push and os.environ.get("AGCOORD_TEST_FAIL_SYNC_PUSH") == "1":
    print("injected synchronized-source push failure", file=sys.stderr)
    raise SystemExit(88)
os.execv(
    os.environ["AGCOORD_TEST_REAL_GIT"],
    [os.environ["AGCOORD_TEST_REAL_GIT"], *arguments],
)
""",
        encoding="utf-8",
    )
    wrapper.chmod(0o755)
    monkeypatch.setenv("AGCOORD_TEST_REAL_GIT", GIT)
    monkeypatch.setenv("AGCOORD_TEST_RACE_REMOTE", str(remote))
    monkeypatch.setenv("AGCOORD_TEST_RACE_REF", source_ref)
    if race_sha is not None:
        monkeypatch.setenv("AGCOORD_TEST_RACE_SHA", race_sha)
    if fail_push:
        monkeypatch.setenv("AGCOORD_TEST_FAIL_SYNC_PUSH", "1")
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")


@pytest.fixture
def repository(tmp_path: Path) -> Repository:
    remote = tmp_path / "origin.git"
    checkout = tmp_path / "checkout"
    subprocess.run(
        [GIT, "init", "--bare", str(remote)],
        check=True,
        text=True,
        capture_output=True,
    )
    subprocess.run(
        [GIT, "init", "-b", "main", str(checkout)],
        check=True,
        text=True,
        capture_output=True,
    )
    _git(checkout, "config", "user.name", "AGCoord test")
    _git(checkout, "config", "user.email", "agcoord@example.invalid")
    _git(checkout, "remote", "add", "origin", str(remote))

    (checkout / "answer.txt").write_text("base\n", encoding="utf-8")
    _git(checkout, "add", "answer.txt")
    _git(checkout, "commit", "-m", "base")
    main_sha = _sha(checkout, "HEAD")
    _git(checkout, "push", "-u", "origin", "main")

    _git(checkout, "switch", "-c", BRANCH)
    (checkout / "answer.txt").write_text("gated head\n", encoding="utf-8")
    (checkout / "feature.txt").write_text("ready\n", encoding="utf-8")
    _git(checkout, "add", "answer.txt", "feature.txt")
    _git(checkout, "commit", "-m", "gated pull request head")
    head_sha = _sha(checkout, "HEAD")
    _git(checkout, "push", "-u", "origin", BRANCH)

    assert len(main_sha) == len(head_sha) == 40
    assert _git(checkout, "status", "--porcelain").stdout == ""
    return Repository(checkout, remote, main_sha, head_sha)


def _metadata(
    repository: Repository,
    **overrides: object,
) -> MetadataClient:
    record: dict[str, object] = {
        "number": PR_NUMBER,
        "state": "OPEN",
        "is_draft": False,
        "base_ref": "main",
        "head_ref": BRANCH,
        "head_sha": repository.head_sha,
        "same_repository": True,
        "title": "Publish the gated head",
        "head_owner": "octo-owner",
    }
    record.update(overrides)
    return MetadataClient(record)


def _execute(
    repository: Repository,
    metadata_client: MetadataClient,
    publisher: Publisher | None = None,
) -> tuple[int, str, str]:
    from agcoord.merge import execute

    out = StringIO()
    err = StringIO()
    publisher = publisher or Publisher(repository)
    status = execute(
        PR_NUMBER,
        checkout=str(repository.checkout),
        branch=BRANCH,
        head_sha=repository.head_sha,
        metadata_client=metadata_client,
        publisher=publisher,
        out=out,
        err=err,
    )
    return status, out.getvalue(), err.getvalue()


def _land_execute(
    repository: Repository,
    gate_command: list[str],
    metadata_client: MetadataClient,
    publisher: Publisher | None = None,
    *,
    environment: dict[str, str] | None = None,
    synchronize_target: bool = True,
    head_updates: list[tuple[str, str]] | None = None,
    branch: str = BRANCH,
) -> tuple[int, str, str, list[tuple[str, int | None]]]:
    from agcoord.land import execute

    out = StringIO()
    err = StringIO()
    phases: list[tuple[str, int | None]] = []

    def head_changed(old_head: str, new_head: str) -> None:
        metadata_client.record["head_sha"] = new_head
        if head_updates is not None:
            head_updates.append((old_head, new_head))

    status = execute(
        PR_NUMBER,
        gate_command,
        checkout=str(repository.checkout),
        branch=branch,
        head_sha=repository.head_sha,
        environment=environment or dict(os.environ),
        metadata_client=metadata_client,
        publisher=publisher or Publisher(repository, branch=branch),
        out=out,
        err=err,
        phase_changed=lambda phase, gate_status: phases.append((phase, gate_status)),
        head_changed=head_changed,
        synchronize_target=synchronize_target,
    )
    return status, out.getvalue(), err.getvalue(), phases


def _install_recording_git(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> Path:
    """Record Git behavior so publication cannot silently fall back to push."""
    bin_dir = tmp_path / "recording-bin"
    bin_dir.mkdir()
    log = tmp_path / "git-invocations.jsonl"
    wrapper = bin_dir / "git"
    wrapper.write_text(
        f"""#!{sys.executable}
import json
import os
from pathlib import Path
import subprocess
import sys

arguments = sys.argv[1:]
with Path(os.environ["AGCOORD_TEST_GIT_LOG"]).open("a", encoding="utf-8") as stream:
    stream.write(json.dumps(arguments) + "\\n")

os.execv(os.environ["AGCOORD_TEST_REAL_GIT"], [os.environ["AGCOORD_TEST_REAL_GIT"], *arguments])
""",
        encoding="utf-8",
    )
    wrapper.chmod(0o755)
    monkeypatch.setenv("AGCOORD_TEST_GIT_LOG", str(log))
    monkeypatch.setenv("AGCOORD_TEST_REAL_GIT", GIT)
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")
    return log


def _invocations(log: Path) -> list[list[str]]:
    if not log.exists():
        return []
    return [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines()]


def _pushes(log: Path) -> list[list[str]]:
    return [arguments for arguments in _invocations(log) if "push" in arguments]


def _worktree_observation(repository: Repository) -> tuple[str, str, str]:
    return (
        _sha(repository.checkout, "HEAD"),
        _git(repository.checkout, "branch", "--show-current").stdout.strip(),
        _git(
            repository.checkout,
            "status",
            "--porcelain=v1",
            "--untracked-files=all",
        ).stdout,
    )


def test_fresh_merge_publishes_exact_candidate_with_one_atomic_ref_update(
    repository: Repository,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    metadata = _metadata(repository)
    before = _worktree_observation(repository)
    publisher = Publisher(repository)
    log = _install_recording_git(monkeypatch, tmp_path)

    status, _out, err = _execute(repository, metadata, publisher)

    assert status == 0, err
    assert metadata.calls == [PR_NUMBER]
    assert len(publisher.create_calls) == 1
    create_call = publisher.create_calls[0]
    assert isinstance(create_call["message"], str) and create_call["message"].strip()
    assert create_call["tree_sha"] == _sha(
        repository.remote, f"{repository.head_sha}^{{tree}}"
    )
    assert create_call["parent_shas"] == (
        repository.main_sha,
        repository.head_sha,
    )
    assert publisher.refs_around_create == [
        (
            (repository.main_sha, repository.head_sha),
            (repository.main_sha, repository.head_sha),
        )
    ]
    assert len(publisher.created) == 1
    candidate = publisher.created[0]["sha"]
    assert isinstance(candidate, str)
    assert candidate != repository.main_sha
    assert publisher.update_calls == [
        (
            {
                "name": "refs/heads/main",
                "before_oid": repository.main_sha,
                "after_oid": candidate,
                "force": False,
            },
            {
                "name": f"refs/heads/{BRANCH}",
                "before_oid": repository.head_sha,
                "after_oid": repository.head_sha,
                "force": False,
            },
        )
    ]
    assert _remote_sha(repository, "refs/heads/main") == candidate
    assert _remote_sha(repository, f"refs/heads/{BRANCH}") == repository.head_sha
    assert _git(repository.remote, "show", "-s", "--format=%P", candidate).stdout.split() == [
        repository.main_sha,
        repository.head_sha,
    ]
    assert _sha(repository.remote, f"{candidate}^{{tree}}") == _sha(
        repository.remote, f"{repository.head_sha}^{{tree}}"
    )
    assert _worktree_observation(repository) == before
    assert _pushes(log) == []
    forbidden = {"fetch", "pull", "rebase", "checkout", "switch", "reset"}
    assert not [
        arguments
        for arguments in _invocations(log)
        if forbidden.intersection(arguments)
    ]


def test_main_advancing_during_atomic_publication_is_a_stale_main_failure(
    repository: Repository,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    advanced_main = _commit_object(repository, repository.main_sha, "concurrent main")
    _transfer_race_object(repository, advanced_main, "advanced-main")
    before = _worktree_observation(repository)
    publisher = Publisher(
        repository,
        race_ref="refs/heads/main",
        race_sha=advanced_main,
    )
    log = _install_recording_git(monkeypatch, tmp_path)

    status, _out, err = _execute(repository, _metadata(repository), publisher)

    assert status == 75
    assert "stale" in err.lower() and "main" in err.lower()
    assert _remote_sha(repository, "refs/heads/main") == advanced_main
    assert _remote_sha(repository, f"refs/heads/{BRANCH}") == repository.head_sha
    assert _worktree_observation(repository) == before
    assert len(publisher.create_calls) == len(publisher.update_calls) == 1
    candidate = publisher.created[0]["sha"]
    assert _remote_sha(repository, "refs/heads/main") != candidate
    assert _pushes(log) == []


def test_pr_head_advancing_during_atomic_publication_is_a_head_changed_failure(
    repository: Repository,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    advanced_head = _commit_object(repository, repository.head_sha, "concurrent head")
    _transfer_race_object(repository, advanced_head, "advanced-head")
    before = _worktree_observation(repository)
    publisher = Publisher(
        repository,
        race_ref=f"refs/heads/{BRANCH}",
        race_sha=advanced_head,
    )
    log = _install_recording_git(monkeypatch, tmp_path)

    status, _out, err = _execute(repository, _metadata(repository), publisher)

    assert status == 76
    assert "head" in err.lower() and ("changed" in err.lower() or "stale" in err.lower())
    assert _remote_sha(repository, "refs/heads/main") == repository.main_sha
    assert _remote_sha(repository, f"refs/heads/{BRANCH}") == advanced_head
    assert _worktree_observation(repository) == before
    assert len(publisher.create_calls) == len(publisher.update_calls) == 1
    assert _remote_sha(repository, "refs/heads/main") != publisher.created[0]["sha"]
    assert _pushes(log) == []


@pytest.mark.parametrize(
    ("overrides", "message_fragment"),
    [
        ({"is_draft": True}, "draft"),
        ({"head_ref": "another-branch"}, "branch"),
        ({"same_repository": False}, "repository"),
    ],
)
def test_unready_open_pull_request_is_rejected_before_publication(
    repository: Repository,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    overrides: dict[str, object],
    message_fragment: str,
):
    log = _install_recording_git(monkeypatch, tmp_path)

    status, _out, err = _execute(repository, _metadata(repository, **overrides))

    assert status == 77, err
    assert message_fragment in err.lower()
    assert _remote_sha(repository, "refs/heads/main") == repository.main_sha
    assert _remote_sha(repository, f"refs/heads/{BRANCH}") == repository.head_sha
    assert _pushes(log) == []


def test_real_non_main_base_is_published_with_the_same_atomic_contract(
    repository: Repository,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    _git(
        repository.remote,
        "update-ref",
        "refs/heads/release",
        repository.main_sha,
    )
    publisher = Publisher(repository)
    log = _install_recording_git(monkeypatch, tmp_path)

    status, _out, err = _execute(
        repository,
        _metadata(repository, base_ref="release"),
        publisher,
    )

    assert status == 0, err
    candidate = publisher.created[0]["sha"]
    assert publisher.update_calls == [
        (
            {
                "name": "refs/heads/release",
                "before_oid": repository.main_sha,
                "after_oid": candidate,
                "force": False,
            },
            {
                "name": f"refs/heads/{BRANCH}",
                "before_oid": repository.head_sha,
                "after_oid": repository.head_sha,
                "force": False,
            },
        )
    ]
    assert _remote_sha(repository, "refs/heads/release") == candidate
    assert _remote_sha(repository, "refs/heads/main") == repository.main_sha
    assert _pushes(log) == []


def test_missing_remote_base_is_a_typed_readiness_refusal_without_publication(
    repository: Repository,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    publisher = Publisher(repository)
    log = _install_recording_git(monkeypatch, tmp_path)

    status, _out, err = _execute(
        repository,
        _metadata(repository, base_ref="absent-release"),
        publisher,
    )

    assert status == 77, err
    assert "base" in err.lower() or "absent-release" in err.lower()
    assert publisher.create_calls == []
    assert publisher.update_calls == []
    assert _remote_sha(repository, "refs/heads/main") == repository.main_sha
    assert _pushes(log) == []


def test_metadata_or_remote_head_different_from_receipt_is_head_changed(
    repository: Repository,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    changed = _commit_object(repository, repository.head_sha, "changed head")
    log = _install_recording_git(monkeypatch, tmp_path)

    status, _out, err = _execute(
        repository,
        _metadata(repository, head_sha=changed),
    )
    assert status == 76
    assert "head" in err.lower()
    assert _pushes(log) == []

    _git(
        repository.checkout,
        "push",
        "origin",
        f"{changed}:refs/heads/{BRANCH}",
        "--force",
    )
    status, _out, err = _execute(repository, _metadata(repository))
    assert status == 76
    assert "head" in err.lower()
    assert _remote_sha(repository, "refs/heads/main") == repository.main_sha
    assert _pushes(log) == []


@pytest.mark.parametrize("local_problem", ["dirty", "wrong-head"])
def test_local_checkout_must_be_clean_on_the_exact_receipted_head(
    repository: Repository,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    local_problem: str,
):
    if local_problem == "dirty":
        (repository.checkout / "untracked.txt").write_text("dirty\n", encoding="utf-8")
    else:
        _git(repository.checkout, "switch", "--detach", repository.main_sha)
    before = _worktree_observation(repository)
    log = _install_recording_git(monkeypatch, tmp_path)

    status, _out, err = _execute(repository, _metadata(repository))

    assert status == 79
    expected = "clean" if local_problem == "dirty" else "head"
    assert expected in err.lower()
    assert _remote_sha(repository, "refs/heads/main") == repository.main_sha
    assert _remote_sha(repository, f"refs/heads/{BRANCH}") == repository.head_sha
    assert _worktree_observation(repository) == before
    assert _pushes(log) == []


def test_remote_main_must_be_an_ancestor_of_the_gated_head(
    repository: Repository,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    unrelated = _commit_object(repository, repository.main_sha, "diverged main")
    _git(
        repository.checkout,
        "push",
        "origin",
        f"{unrelated}:refs/heads/main",
        "--force",
    )
    before = _worktree_observation(repository)
    log = _install_recording_git(monkeypatch, tmp_path)

    status, _out, err = _execute(repository, _metadata(repository))

    assert status == 75
    assert "stale" in err.lower() and "main" in err.lower()
    assert _remote_sha(repository, "refs/heads/main") == unrelated
    assert _remote_sha(repository, f"refs/heads/{BRANCH}") == repository.head_sha
    assert _worktree_observation(repository) == before
    assert _pushes(log) == []


def test_non_race_atomic_ref_update_failure_is_reported_without_mutating_refs(
    repository: Repository,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    before = _worktree_observation(repository)
    publisher = Publisher(repository, fail_update=True)
    log = _install_recording_git(monkeypatch, tmp_path)

    status, _out, err = _execute(repository, _metadata(repository), publisher)

    assert status == 78
    assert "publish" in err.lower() or "update" in err.lower()
    assert _remote_sha(repository, "refs/heads/main") == repository.main_sha
    assert _remote_sha(repository, f"refs/heads/{BRANCH}") == repository.head_sha
    assert _worktree_observation(repository) == before
    assert len(publisher.create_calls) == len(publisher.update_calls) == 1
    assert _pushes(log) == []


@pytest.mark.parametrize("reported_state", ["MERGED", "OPEN"])
def test_already_landed_head_is_idempotent_during_forge_state_or_branch_lag(
    repository: Repository,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    reported_state: str,
):
    first_log = _install_recording_git(monkeypatch, tmp_path)
    first_publisher = Publisher(repository)
    status, _out, err = _execute(
        repository,
        _metadata(repository),
        first_publisher,
    )
    assert status == 0, err
    merged_main = _remote_sha(repository, "refs/heads/main")
    assert len(first_publisher.update_calls) == 1
    assert _pushes(first_log) == []

    _git(repository.remote, "update-ref", "-d", f"refs/heads/{BRANCH}")
    first_log.write_text("", encoding="utf-8")
    second_publisher = Publisher(repository)
    status, _out, err = _execute(
        repository,
        _metadata(repository, state=reported_state),
        second_publisher,
    )
    assert status == 0, err
    assert _remote_sha(repository, "refs/heads/main") == merged_main
    assert second_publisher.create_calls == []
    assert second_publisher.update_calls == []
    assert second_publisher.ancestor_calls == [(repository.head_sha, merged_main)]
    assert _pushes(first_log) == []


def test_merged_metadata_without_head_in_remote_main_is_not_success(
    repository: Repository,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    log = _install_recording_git(monkeypatch, tmp_path)

    status, _out, err = _execute(
        repository,
        _metadata(repository, state="MERGED"),
    )

    assert status == 77
    assert "merged" in err.lower() and "main" in err.lower()
    assert _remote_sha(repository, "refs/heads/main") == repository.main_sha
    assert _pushes(log) == []


def test_land_opt_out_rejects_a_stale_base_before_starting_the_gate(
    repository: Repository,
    tmp_path: Path,
):
    unrelated = _commit_object(repository, repository.main_sha, "diverged before land")
    _git(
        repository.checkout,
        "push",
        "origin",
        f"{unrelated}:refs/heads/main",
        "--force",
    )
    gate_marker = tmp_path / "gate-ran"
    publisher = Publisher(repository)

    status, _out, err, phases = _land_execute(
        repository,
        [
            sys.executable,
            "-c",
            "from pathlib import Path; import sys; Path(sys.argv[1]).touch()",
            str(gate_marker),
        ],
        _metadata(repository),
        publisher,
        synchronize_target=False,
    )

    assert status == 75
    assert "stale" in err.lower() and "main" in err.lower()
    assert phases == [("preflight", None)]
    assert not gate_marker.exists()
    assert publisher.create_calls == []
    assert publisher.update_calls == []
    assert _remote_sha(repository, "refs/heads/main") == unrelated


def test_land_merges_an_advanced_target_into_the_source_before_gating(
    repository: Repository,
    tmp_path: Path,
):
    advanced_main = _advance_remote_main(
        repository,
        tmp_path / "target-checkout",
        {"target.txt": "first land\n"},
    )
    old_head = repository.head_sha
    gate_marker = tmp_path / "gate-ran"
    publisher = Publisher(repository)
    head_updates: list[tuple[str, str]] = []

    status, out, err, phases = _land_execute(
        repository,
        [
            sys.executable,
            "-c",
            "from pathlib import Path; import sys; Path(sys.argv[1]).touch()",
            str(gate_marker),
        ],
        _metadata(repository),
        publisher,
        head_updates=head_updates,
    )

    assert status == 0, err
    assert gate_marker.exists()
    assert phases == [
        ("preflight", None),
        ("gating", None),
        ("publishing", 0),
    ]
    merge_head = _sha(repository.checkout, "HEAD")
    assert head_updates == [(old_head, merge_head)]
    assert _git(
        repository.checkout,
        "show",
        "-s",
        "--format=%P",
        merge_head,
    ).stdout.split() == [old_head, advanced_main]
    assert _git(
        repository.checkout,
        "status",
        "--porcelain=v1",
        "--untracked-files=all",
    ).stdout == ""
    assert _remote_sha(repository, f"refs/heads/{BRANCH}") == merge_head
    candidate = publisher.created[-1]["sha"]
    assert isinstance(candidate, str)
    assert _remote_sha(repository, "refs/heads/main") == candidate
    assert publisher.created[-1]["parent_shas"] == (advanced_main, merge_head)
    assert f"gate started for {merge_head}" in out


def test_land_can_opt_out_of_target_synchronization(
    repository: Repository,
    tmp_path: Path,
):
    advanced_main = _advance_remote_main(
        repository,
        tmp_path / "target-checkout",
        {"target.txt": "first land\n"},
    )
    gate_marker = tmp_path / "gate-ran"
    publisher = Publisher(repository)
    head_updates: list[tuple[str, str]] = []

    status, _out, err, phases = _land_execute(
        repository,
        [sys.executable, "-c", "raise SystemExit('must not run')"],
        _metadata(repository),
        publisher,
        synchronize_target=False,
        head_updates=head_updates,
    )

    assert status == 75
    assert "stale" in err.lower()
    assert phases == [("preflight", None)]
    assert not gate_marker.exists()
    assert head_updates == []
    assert _sha(repository.checkout, "HEAD") == repository.head_sha
    assert _remote_sha(repository, f"refs/heads/{BRANCH}") == repository.head_sha
    assert _remote_sha(repository, "refs/heads/main") == advanced_main
    assert publisher.create_calls == []
    assert publisher.update_calls == []


def test_land_conflict_is_reported_and_the_checkout_is_restored_before_the_gate(
    repository: Repository,
    tmp_path: Path,
):
    advanced_main = _advance_remote_main(
        repository,
        tmp_path / "target-checkout",
        {"answer.txt": "advanced target\n"},
    )
    gate_marker = tmp_path / "gate-ran"
    publisher = Publisher(repository)
    head_updates: list[tuple[str, str]] = []

    status, _out, err, phases = _land_execute(
        repository,
        [
            sys.executable,
            "-c",
            "from pathlib import Path; import sys; Path(sys.argv[1]).touch()",
            str(gate_marker),
        ],
        _metadata(repository),
        publisher,
        head_updates=head_updates,
    )

    assert status == 79
    assert "conflict" in err.lower()
    assert "answer.txt" in err
    assert phases == [("preflight", None)]
    assert not gate_marker.exists()
    assert head_updates == []
    assert _sha(repository.checkout, "HEAD") == repository.head_sha
    assert _git(
        repository.checkout,
        "status",
        "--porcelain=v1",
        "--untracked-files=all",
    ).stdout == ""
    assert _remote_sha(repository, f"refs/heads/{BRANCH}") == repository.head_sha
    assert _remote_sha(repository, "refs/heads/main") == advanced_main
    assert publisher.create_calls == []
    assert publisher.update_calls == []


def test_two_queued_lands_mechanically_synchronize_the_second_request(
    repository: Repository,
    tmp_path: Path,
):
    second_branch = "feature/second-publication"
    second = _add_feature_checkout(
        repository,
        tmp_path / "second-checkout",
        second_branch,
    )

    first_publisher = Publisher(repository)
    first_status, _out, first_err, _phases = _land_execute(
        repository,
        [sys.executable, "-c", "pass"],
        _metadata(repository),
        first_publisher,
    )
    assert first_status == 0, first_err
    first_landed = _remote_sha(repository, "refs/heads/main")

    second_publisher = Publisher(second, branch=second_branch)
    head_updates: list[tuple[str, str]] = []
    second_status, _out, second_err, phases = _land_execute(
        second,
        [sys.executable, "-c", "pass"],
        _metadata(second, head_ref=second_branch),
        second_publisher,
        branch=second_branch,
        head_updates=head_updates,
    )

    assert second_status == 0, second_err
    assert phases == [
        ("preflight", None),
        ("gating", None),
        ("publishing", 0),
    ]
    synchronized = _sha(second.checkout, "HEAD")
    assert head_updates == [(second.head_sha, synchronized)]
    assert _git(
        second.checkout,
        "show",
        "-s",
        "--format=%P",
        synchronized,
    ).stdout.split() == [second.head_sha, first_landed]
    assert _remote_sha(second, f"refs/heads/{second_branch}") == synchronized
    assert second_publisher.created[-1]["parent_shas"] == (
        first_landed,
        synchronized,
    )


def test_source_change_during_target_sync_fails_closed_before_the_gate(
    repository: Repository,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    advanced_main = _advance_remote_main(
        repository,
        tmp_path / "target-checkout",
        {"target.txt": "first land\n"},
    )
    changed_source = _commit_object(repository, repository.head_sha, "source race")
    _transfer_race_object(repository, changed_source, "source-race")
    _install_sync_push_fault(
        monkeypatch,
        tmp_path,
        remote=repository.remote,
        source_ref=f"refs/heads/{BRANCH}",
        race_sha=changed_source,
    )
    gate_marker = tmp_path / "gate-ran"
    publisher = Publisher(repository)

    status, _out, err, phases = _land_execute(
        repository,
        [
            sys.executable,
            "-c",
            "from pathlib import Path; import sys; Path(sys.argv[1]).touch()",
            str(gate_marker),
        ],
        _metadata(repository),
        publisher,
    )

    assert status == 76
    assert "source" in err.lower() and "changed" in err.lower()
    assert phases == [("preflight", None)]
    assert not gate_marker.exists()
    assert _sha(repository.checkout, "HEAD") == repository.head_sha
    assert _git(
        repository.checkout,
        "status",
        "--porcelain=v1",
        "--untracked-files=all",
    ).stdout == ""
    assert _remote_sha(repository, f"refs/heads/{BRANCH}") == changed_source
    assert _remote_sha(repository, "refs/heads/main") == advanced_main
    assert publisher.create_calls == []
    assert publisher.update_calls == []


def test_target_sync_push_failure_restores_the_checkout_and_never_gates(
    repository: Repository,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    advanced_main = _advance_remote_main(
        repository,
        tmp_path / "target-checkout",
        {"target.txt": "first land\n"},
    )
    _install_sync_push_fault(
        monkeypatch,
        tmp_path,
        remote=repository.remote,
        source_ref=f"refs/heads/{BRANCH}",
        fail_push=True,
    )
    gate_marker = tmp_path / "gate-ran"
    publisher = Publisher(repository)

    status, _out, err, phases = _land_execute(
        repository,
        [
            sys.executable,
            "-c",
            "from pathlib import Path; import sys; Path(sys.argv[1]).touch()",
            str(gate_marker),
        ],
        _metadata(repository),
        publisher,
    )

    assert status == 78
    assert "push" in err.lower()
    assert phases == [("preflight", None)]
    assert not gate_marker.exists()
    assert _sha(repository.checkout, "HEAD") == repository.head_sha
    assert _git(
        repository.checkout,
        "status",
        "--porcelain=v1",
        "--untracked-files=all",
    ).stdout == ""
    assert _remote_sha(repository, f"refs/heads/{BRANCH}") == repository.head_sha
    assert _remote_sha(repository, "refs/heads/main") == advanced_main
    assert publisher.create_calls == []
    assert publisher.update_calls == []


def test_land_red_gate_returns_its_status_and_never_publishes(
    repository: Repository,
    tmp_path: Path,
):
    environment = dict(os.environ)
    environment["AGCOORD_LAND_TEST_VALUE"] = "request-environment"
    observed = tmp_path / "gate-environment"
    publisher = Publisher(repository)

    status, out, err, phases = _land_execute(
        repository,
        [
            sys.executable,
            "-c",
            (
                "import os, pathlib, sys; "
                "pathlib.Path(sys.argv[1]).write_text(os.environ['AGCOORD_LAND_TEST_VALUE']); "
                "print('gate stdout'); print('gate stderr', file=sys.stderr); "
                "raise SystemExit(23)"
            ),
            str(observed),
        ],
        _metadata(repository),
        publisher,
        environment=environment,
    )

    assert status == 23
    assert observed.read_text(encoding="utf-8") == "request-environment"
    assert "gate stdout" in out
    assert "gate stderr" in out
    assert phases == [
        ("preflight", None),
        ("gating", None),
        ("gating", 23),
    ]
    assert publisher.create_calls == []
    assert publisher.update_calls == []
    assert _remote_sha(repository, "refs/heads/main") == repository.main_sha


def test_land_green_gate_publishes_in_the_same_phase_transcript(
    repository: Repository,
):
    publisher = Publisher(repository)

    status, out, err, phases = _land_execute(
        repository,
        [sys.executable, "-c", "print('gate green', flush=True)"],
        _metadata(repository),
        publisher,
    )

    assert status == 0, err
    assert "gate green" in out
    assert phases == [
        ("preflight", None),
        ("gating", None),
        ("publishing", 0),
    ]
    assert len(publisher.create_calls) == 1
    assert len(publisher.update_calls) == 1
    candidate = publisher.created[0]["sha"]
    assert _remote_sha(repository, "refs/heads/main") == candidate
    assert _remote_sha(repository, f"refs/heads/{BRANCH}") == repository.head_sha


@pytest.mark.parametrize(
    ("moved_ref", "expected_status", "message_fragment"),
    [
        ("base", 75, "stale"),
        ("head", 76, "head"),
    ],
)
def test_land_remote_movement_during_gate_is_a_typed_refusal_without_publication(
    repository: Repository,
    moved_ref: str,
    expected_status: int,
    message_fragment: str,
):
    if moved_ref == "base":
        advanced = _commit_object(repository, repository.main_sha, "main moved during gate")
        fixture_ref = "advanced-main-during-land"
        target_ref = "refs/heads/main"
    else:
        advanced = _commit_object(repository, repository.head_sha, "head moved during gate")
        fixture_ref = "advanced-head-during-land"
        target_ref = f"refs/heads/{BRANCH}"
    _transfer_race_object(repository, advanced, fixture_ref)
    publisher = Publisher(repository)

    status, _out, err, phases = _land_execute(
        repository,
        [
            GIT,
            "--git-dir",
            str(repository.remote),
            "update-ref",
            target_ref,
            advanced,
        ],
        _metadata(repository),
        publisher,
    )

    assert status == expected_status
    assert message_fragment in err.lower()
    assert phases == [
        ("preflight", None),
        ("gating", None),
        ("publishing", 0),
    ]
    assert publisher.create_calls == []
    assert publisher.update_calls == []
    assert _remote_sha(repository, "refs/heads/main") == (
        advanced if moved_ref == "base" else repository.main_sha
    )
