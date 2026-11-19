"""Behavioral contract for the optional GitHub atomic-ref publisher."""

from __future__ import annotations

import json
import os
from pathlib import Path
import stat
import sys

import pytest

from agcoord.github import GitHubMergePublisher


MAIN = "1" * 40
HEAD = "2" * 40
TREE = "a" * 40
CANDIDATE = "c" * 40
OTHER = "d" * 40
BRANCH = "feature/atomic-publication"


def _install_recording_gh(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> Path:
    log = tmp_path / "gh-calls.jsonl"
    executable = tmp_path / "gh"
    executable.write_text(
        f"""#!{sys.executable}
import json
import os
from pathlib import Path
import sys

arguments = sys.argv[1:]
raw_input = sys.stdin.read()
payload = json.loads(raw_input) if raw_input else None
with Path(os.environ["GH_TEST_LOG"]).open("a", encoding="utf-8") as stream:
    stream.write(json.dumps({{"arguments": arguments, "payload": payload}}) + "\\n")

if arguments[:2] == ["repo", "view"]:
    target = "repo"
elif arguments[:3] == ["api", "--method", "POST"]:
    target = "commit"
elif arguments[:2] == ["api", "graphql"]:
    target = "update"
else:
    target = "compare"

if os.environ.get("GH_TEST_TARGET") == target:
    mode = os.environ.get("GH_TEST_MODE")
    if mode == "nonzero":
        print(f"injected {{target}} failure", file=sys.stderr)
        raise SystemExit(17)
    if mode == "malformed-json":
        print("{{not-json")
        raise SystemExit(0)
    if mode == "malformed":
        print("{{}}")
        raise SystemExit(0)

if target == "repo":
    print(json.dumps({{"id": "R_node_2332", "nameWithOwner": "example/widgets"}}))
elif target == "commit":
    print(json.dumps({{
        "sha": "C" * 40,
        "tree": {{"sha": "A" * 40}},
        "parents": [{{"sha": parent.upper()}} for parent in payload["parents"]],
    }}))
elif target == "update":
    mutation_id = payload["variables"]["input"]["clientMutationId"]
    print(json.dumps({{
        "data": {{"updateRefs": {{"clientMutationId": mutation_id}}}},
    }}))
else:
    merge_base = os.environ.get("GH_TEST_MERGE_BASE", "{HEAD}")
    print(json.dumps({{"merge_base_commit": {{"sha": merge_base.upper()}}}}))
""",
        encoding="utf-8",
    )
    executable.chmod(executable.stat().st_mode | stat.S_IEXEC)
    monkeypatch.setenv("GH_TEST_LOG", str(log))
    monkeypatch.setenv("PATH", f"{tmp_path}{os.pathsep}{os.environ['PATH']}")
    return log


def _calls(log: Path) -> list[dict[str, object]]:
    return [
        json.loads(line)
        for line in log.read_text(encoding="utf-8").splitlines()
        if line
    ]


def test_create_merge_commit_resolves_repository_and_normalizes_exact_payload(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    log = _install_recording_gh(monkeypatch, tmp_path)
    publisher = GitHubMergePublisher(tmp_path)

    created = publisher.create_merge_commit(
        message="Merge PR #2332\n\nExact gated tree\n",
        tree_sha=TREE.upper(),
        parent_shas=(MAIN.upper(), HEAD.upper()),
    )

    assert created == {
        "sha": CANDIDATE,
        "tree_sha": TREE,
        "parent_shas": (MAIN, HEAD),
    }
    assert _calls(log) == [
        {
            "arguments": ["repo", "view", "--json", "id,nameWithOwner"],
            "payload": None,
        },
        {
            "arguments": [
                "api",
                "--method",
                "POST",
                "repos/example/widgets/git/commits",
                "--input",
                "-",
            ],
            "payload": {
                "message": "Merge PR #2332\n\nExact gated tree\n",
                "tree": TREE,
                "parents": [MAIN, HEAD],
            },
        },
    ]


def test_update_refs_sends_one_exact_ordered_before_oid_transaction(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    log = _install_recording_gh(monkeypatch, tmp_path)
    publisher = GitHubMergePublisher(tmp_path)

    publisher.update_refs(
        (
            {
                "name": "refs/heads/main",
                "before_oid": MAIN,
                "after_oid": CANDIDATE,
                "force": False,
            },
            {
                "name": f"refs/heads/{BRANCH}",
                "before_oid": HEAD,
                "after_oid": HEAD,
                "force": False,
            },
        )
    )

    calls = _calls(log)
    assert calls[0] == {
        "arguments": ["repo", "view", "--json", "id,nameWithOwner"],
        "payload": None,
    }
    graphql = calls[1]
    assert graphql["arguments"] == ["api", "graphql", "--input", "-"]
    payload = graphql["payload"]
    assert isinstance(payload, dict)
    assert "updateRefs" in payload["query"]
    atomic_input = payload["variables"]["input"]
    assert atomic_input["repositoryId"] == "R_node_2332"
    assert isinstance(atomic_input["clientMutationId"], str)
    assert atomic_input["clientMutationId"].startswith("agcoord-")
    assert atomic_input["refUpdates"] == [
        {
            "name": "refs/heads/main",
            "beforeOid": MAIN,
            "afterOid": CANDIDATE,
            "force": False,
        },
        {
            "name": f"refs/heads/{BRANCH}",
            "beforeOid": HEAD,
            "afterOid": HEAD,
            "force": False,
        },
    ]


def test_compare_normalizes_forge_ancestry_and_reuses_repository_resolution(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    log = _install_recording_gh(monkeypatch, tmp_path)
    publisher = GitHubMergePublisher(tmp_path)

    assert publisher.is_ancestor(ancestor_sha=HEAD, descendant_sha=CANDIDATE) is True
    monkeypatch.setenv("GH_TEST_MERGE_BASE", OTHER)
    assert publisher.is_ancestor(ancestor_sha=HEAD, descendant_sha=CANDIDATE) is False

    calls = _calls(log)
    assert [call["arguments"] for call in calls] == [
        ["repo", "view", "--json", "id,nameWithOwner"],
        ["api", f"repos/example/widgets/compare/{HEAD}...{CANDIDATE}"],
        ["api", f"repos/example/widgets/compare/{HEAD}...{CANDIDATE}"],
    ]


@pytest.mark.parametrize(
    ("target", "mode", "operation"),
    [
        ("repo", "malformed", "create"),
        ("commit", "malformed", "create"),
        ("commit", "nonzero", "create"),
        ("update", "malformed-json", "update"),
        ("update", "nonzero", "update"),
        ("compare", "malformed", "compare"),
        ("compare", "nonzero", "compare"),
    ],
)
def test_github_publisher_fails_closed_on_bad_or_unsuccessful_responses(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    target: str,
    mode: str,
    operation: str,
):
    _install_recording_gh(monkeypatch, tmp_path)
    monkeypatch.setenv("GH_TEST_TARGET", target)
    monkeypatch.setenv("GH_TEST_MODE", mode)
    publisher = GitHubMergePublisher(tmp_path)

    with pytest.raises(RuntimeError):
        if operation == "create":
            publisher.create_merge_commit(
                message="Merge PR #2332",
                tree_sha=TREE,
                parent_shas=(MAIN, HEAD),
            )
        elif operation == "update":
            publisher.update_refs(
                (
                    {
                        "name": "refs/heads/main",
                        "before_oid": MAIN,
                        "after_oid": CANDIDATE,
                        "force": False,
                    },
                    {
                        "name": f"refs/heads/{BRANCH}",
                        "before_oid": HEAD,
                        "after_oid": HEAD,
                        "force": False,
                    },
                )
            )
        else:
            publisher.is_ancestor(ancestor_sha=HEAD, descendant_sha=CANDIDATE)


def test_update_refs_accepts_target_and_multiple_pull_request_refs(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    log = _install_recording_gh(monkeypatch, tmp_path)
    publisher = GitHubMergePublisher(tmp_path)
    head_two = "3" * 40

    publisher.update_refs(
        (
            {
                "name": "refs/heads/main",
                "before_oid": MAIN,
                "after_oid": CANDIDATE,
                "force": False,
            },
            {
                "name": f"refs/heads/{BRANCH}",
                "before_oid": HEAD,
                "after_oid": HEAD,
                "force": False,
            },
            {
                "name": "refs/heads/feature/second-pr",
                "before_oid": head_two,
                "after_oid": head_two,
                "force": False,
            },
        )
    )

    calls = _calls(log)
    assert len(calls) == 2
    assert len(calls[1]["payload"]["variables"]["input"]["refUpdates"]) == 3


def test_create_merge_commit_uploads_missing_tree_from_checkout(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    import subprocess

    repo = tmp_path / "repo"
    repo.mkdir()
    for cmd in (
        ["git", "init", "-b", "main"],
        ["git", "config", "user.name", "Test"],
        ["git", "config", "user.email", "test@example.invalid"],
    ):
        subprocess.run(cmd, cwd=repo, check=True, capture_output=True)
    (repo / "base.txt").write_text("base\n", encoding="utf-8")
    subprocess.run(["git", "add", "base.txt"], cwd=repo, check=True, capture_output=True)
    subprocess.run(["git", "commit", "-m", "base"], cwd=repo, check=True, capture_output=True)
    base_sha = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    (repo / "batch.txt").write_text("merged batch\n", encoding="utf-8")
    subprocess.run(["git", "add", "batch.txt"], cwd=repo, check=True, capture_output=True)
    subprocess.run(["git", "commit", "-m", "batch"], cwd=repo, check=True, capture_output=True)
    tree_sha = subprocess.run(
        ["git", "rev-parse", "HEAD^{tree}"],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    head_sha = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()

    log = tmp_path / "gh-upload.jsonl"
    executable = tmp_path / "gh"
    executable.write_text(
        f"""#!/usr/bin/env python3
import json, os, sys
from pathlib import Path

args = sys.argv[1:]
raw = sys.stdin.read()
payload = json.loads(raw) if raw else None
with Path(os.environ["GH_TEST_LOG"]).open("a", encoding="utf-8") as s:
    s.write(json.dumps({{"args": args, "payload": payload}}) + "\\n")

created_flag = Path({str(tmp_path / "tree-created")!r})
if args[:2] == ["repo", "view"]:
    print(json.dumps({{"id": "R_1", "nameWithOwner": "example/widgets"}}))
elif args == ["api", "--method", "POST", "repos/example/widgets/git/commits", "--input", "-"]:
    if not created_flag.exists():
        print("Tree does not exist (422)", file=sys.stderr)
        raise SystemExit(1)
    print(json.dumps({{
        "sha": "e" * 40,
        "tree": {{"sha": payload["tree"]}},
        "parents": [{{"sha": p}} for p in payload["parents"]],
    }}))
elif args == ["api", "--method", "POST", "repos/example/widgets/git/blobs", "--input", "-"]:
    print(json.dumps({{"sha": "f" * 40}}))
elif args == ["api", "--method", "POST", "repos/example/widgets/git/trees", "--input", "-"]:
    created_flag.write_text("1", encoding="utf-8")
    print(json.dumps({{"sha": {tree_sha!r}}}))
else:
    raise SystemExit(f"unexpected gh call: {{args!r}}")
""",
        encoding="utf-8",
    )
    executable.chmod(0o755)
    monkeypatch.setenv("GH_TEST_LOG", str(log))
    monkeypatch.setenv("PATH", f"{tmp_path}{os.pathsep}{os.environ['PATH']}")

    publisher = GitHubMergePublisher(repo)
    created = publisher.create_merge_commit(
        message="Batch merge",
        tree_sha=tree_sha,
        parent_shas=(base_sha, head_sha),
    )
    assert created["tree_sha"] == tree_sha
    assert created["sha"] == "e" * 40
    recorded = _calls(log)
    assert any("git/trees" in " ".join(c["args"]) for c in recorded)


def test_verify_submission_head_checks_equal_behind_diverged_and_retry(
    tmp_path: Path,
):
    import subprocess
    from agcoord.github import SubmissionRefusal, verify_submission_head

    repo = tmp_path / "repo"
    subprocess.run(["git", "init", "-q", "-b", "feat/check", str(repo)], check=True)
    subprocess.run(["git", "-C", str(repo), "config", "user.name", "AGCoord"], check=True)
    subprocess.run(["git", "-C", str(repo), "config", "user.email", "t@example.invalid"], check=True)
    (repo / "f.txt").write_text("base\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(repo), "add", "f.txt"], check=True)
    subprocess.run(["git", "-C", str(repo), "commit", "-q", "-m", "base"], check=True)
    base_sha = subprocess.check_output(["git", "-C", str(repo), "rev-parse", "HEAD"], text=True).strip()

    class _Client:
        def __init__(self, heads: list[str | BaseException]):
            self.heads = list(heads)

        def pull_request(self, number: int) -> dict[str, object]:
            nxt = self.heads.pop(0)
            if isinstance(nxt, BaseException):
                raise nxt
            return {
                "number": number,
                "state": "OPEN",
                "is_draft": False,
                "base_ref": "main",
                "head_ref": "feat/check",
                "head_sha": nxt,
                "same_repository": True,
                "title": "PR",
                "head_owner": "octo",
            }

    class _Publisher:
        def __init__(self, behind: bool):
            self.behind = behind
            self.calls = 0

        def create_merge_commit(self, **_kwargs):
            raise AssertionError("unused")

        def update_refs(self, _updates):
            raise AssertionError("unused")

        def is_ancestor(self, *, ancestor_sha: str, descendant_sha: str) -> bool:
            self.calls += 1
            return self.behind

    # Equal head after transient failure retries and succeeds.
    waits: list[float] = []
    verify_submission_head(
        10,
        checkout=repo,
        branch="feat/check",
        head_sha=base_sha,
        metadata_client=_Client([RuntimeError("502"), base_sha]),
        publisher=_Publisher(False),
        wait=waits.append,
    )
    assert len(waits) == 1

    # Remote head descending from submitted head refuses submitted-head-behind.
    pub_behind = _Publisher(True)
    remote_sha = "b" * 40
    with pytest.raises(SubmissionRefusal) as behind_refusal:
        verify_submission_head(
            10,
            checkout=repo,
            branch="feat/check",
            head_sha=base_sha,
            metadata_client=_Client([remote_sha]),
            publisher=pub_behind,
            wait=lambda _: None,
        )
    assert behind_refusal.value.code == "submitted-head-behind"
    assert pub_behind.calls == 1
    assert f"git fetch && git merge --ff-only origin/feat/check" in str(behind_refusal.value)

    # Diverged head refuses submitted-head-diverged.
    with pytest.raises(SubmissionRefusal) as diverged_refusal:
        verify_submission_head(
            10,
            checkout=repo,
            branch="feat/check",
            head_sha=base_sha,
            metadata_client=_Client(["c" * 40]),
            publisher=_Publisher(False),
            wait=lambda _: None,
        )
    assert diverged_refusal.value.code == "submitted-head-diverged"

    # Persistent forge failure refuses forge-unavailable after 5 attempts.
    with pytest.raises(SubmissionRefusal) as outage:
        verify_submission_head(
            10,
            checkout=repo,
            branch="feat/check",
            head_sha=base_sha,
            metadata_client=_Client([RuntimeError("timeout")] * 5),
            publisher=_Publisher(False),
            wait=lambda _: None,
        )
    assert outage.value.code == "forge-unavailable"
