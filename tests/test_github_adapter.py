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
