"""Bounded in-lane reconciliation and retry of transient GitHub publication errors.

The tests drive the real adapter, the real singleton publication executor, and the real
batch coordinator against the stateful fake forge of the batch suite (a bare Git origin plus a
fake ``gh``). The fake forge replays scripted faults so a transient error, a dropped response
after the forge applied a transaction, and a concurrent ref move are all deterministic. Every
adapter and executor test injects the sleep, so none of them waits in real time.
"""

from __future__ import annotations

import json
import os
from io import StringIO
from pathlib import Path

import pytest

from agcoord.github import (
    EXIT_HEAD_CHANGED,
    EXIT_PUBLISH_FAILED,
    EXIT_STALE_MAIN,
    GitHubMergePublisher,
    GitHubMetadataClient,
    execute,
)

# The stateful fake forge and repository fixtures are shared with the batch suite.
from test_land_batch import (
    _env_with_gh,
    _git,
    _head,
    _install_stateful_gh,
    _python,
    _setup_remote_and_worktrees,
    _submit_hold,
    _wait_terminal,
)

PR = 71
BRANCH = "feat/retry"
BACKOFF = [0.5, 1.0, 2.0, 4.0]

GRAPHQL_INTERNAL = {
    "stdout": json.dumps(
        {
            "data": {"updateRefs": None},
            "errors": [
                {
                    "message": "Something went wrong while executing your query. "
                    "Please include `1A2B:3C4D` when reporting this issue."
                }
            ],
        }
    ),
    "stderr": "gh: Something went wrong while executing your query. "
    "Please include `1A2B:3C4D` when reporting this issue.\n",
    "exit": 1,
}
BAD_GATEWAY = {"stderr": "gh: Bad Gateway (HTTP 502)\n", "exit": 1}
UNAVAILABLE = {
    "stderr": "HTTP 503: Service Unavailable (https://api.github.com/graphql)\n",
    "exit": 1,
}
GATEWAY_TIMEOUT = {"stderr": "gh: Gateway Timeout (HTTP 504)\n", "exit": 1}
TRANSPORT_FAILURES = [
    'Post "https://api.github.com/graphql": read tcp 10.0.0.2:51514->140.82.112.5:443: '
    "read: connection reset by peer",
    'Post "https://api.github.com/graphql": dial tcp: lookup api.github.com: no such host',
    'Post "https://api.github.com/graphql": context deadline exceeded '
    "(Client.Timeout exceeded while awaiting headers)",
    'Post "https://api.github.com/graphql": EOF',
]
PERMANENT_REFUSALS = {
    "required-check": {
        "stdout": json.dumps(
            {"errors": [{"message": 'Required status check "ci" is expected.'}]}
        ),
        "stderr": 'gh: Required status check "ci" is expected.\n',
        "exit": 1,
    },
    "validation": {"stderr": "gh: Validation Failed (HTTP 422)\n", "exit": 1},
    "forbidden": {
        "stderr": "gh: Resource not accessible by personal access token (HTTP 403)\n",
        "exit": 1,
    },
    "branch-named-timeout": {
        "stdout": json.dumps(
            {"errors": [{"message": "Branch feat/fix-timeout-handling is protected"}]}
        ),
        "stderr": "gh: Branch feat/fix-timeout-handling is protected\n",
        "exit": 1,
    },
}


class Forge:
    """One pull request on a fake forge whose responses a test scripts."""

    def __init__(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        remote, state_file, checkouts, _ = _setup_remote_and_worktrees(
            tmp_path,
            [{"pr": PR, "base": "main", "branch": BRANCH, "files": {"retry.txt": "1\n"}}],
        )
        self.remote = remote
        self.wt = checkouts[PR]
        self.gh_log = tmp_path / "gh.jsonl"
        self.faults_file = tmp_path / "faults.json"
        bin_dir = _install_stateful_gh(tmp_path, remote, state_file, self.gh_log)
        monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")
        monkeypatch.setenv("GH_FAULTS_FILE", str(self.faults_file))
        self.head = _head(self.wt)
        self.main = _git(remote, "rev-parse", "refs/heads/main")
        self.tree = _git(self.wt, "rev-parse", "HEAD^{tree}")
        # An unrelated commit already on the forge, used as a concurrent push's new tip.
        seed = tmp_path / "seed"
        _git(seed, "switch", "-c", "side")
        (seed / "side.txt").write_text("side\n", encoding="utf-8")
        _git(seed, "add", "side.txt")
        _git(seed, "commit", "-m", "concurrent push")
        _git(seed, "push", "origin", "side")
        self.side = _head(seed)

    def script(self, **faults: list[dict[str, object]]) -> None:
        self.faults_file.write_text(json.dumps(faults), encoding="utf-8")

    def calls(self, kind: str) -> list[dict[str, object]]:
        recorded = [
            json.loads(line)
            for line in self.gh_log.read_text(encoding="utf-8").splitlines()
            if line
        ]
        if kind == "graphql":
            return [call for call in recorded if call["args"][:2] == ["api", "graphql"]]
        assert kind == "commit"
        return [
            call
            for call in recorded
            if call["args"][:3] == ["api", "--method", "POST"]
            and call["args"][3].endswith("/git/commits")
        ]

    def remote_ref(self, name: str) -> str:
        return _git(self.remote, "rev-parse", name)

    def publisher(self, sleeps: list[float], notes: list[str] | None = None):
        return GitHubMergePublisher(
            self.wt,
            wait=sleeps.append,
            notify=(notes if notes is not None else []).append,
        )

    def candidate_updates(
        self, publisher: GitHubMergePublisher
    ) -> tuple[str, tuple[dict[str, object], ...]]:
        created = publisher.create_merge_commit(
            message=f"Merge pull request #{PR} from {BRANCH}\n",
            tree_sha=self.tree,
            parent_shas=(self.main, self.head),
        )
        candidate = str(created["sha"])
        return candidate, (
            {
                "name": "refs/heads/main",
                "before_oid": self.main,
                "after_oid": candidate,
                "force": False,
            },
            {
                "name": f"refs/heads/{BRANCH}",
                "before_oid": self.head,
                "after_oid": self.head,
                "force": False,
            },
        )

    def land(self) -> tuple[int, str, str, list[float]]:
        """Run singleton publication of the gated head with every sleep injected."""
        sleeps: list[float] = []
        out, err = StringIO(), StringIO()
        status = execute(
            PR,
            checkout=str(self.wt),
            branch=BRANCH,
            head_sha=self.head,
            metadata_client=GitHubMetadataClient(self.wt),
            publisher=self.publisher(sleeps),
            out=out,
            err=err,
            wait=sleeps.append,
        )
        return status, out.getvalue(), err.getvalue(), sleeps


@pytest.fixture
def forge(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Forge:
    return Forge(tmp_path, monkeypatch)


# --- create_merge_commit ---------------------------------------------------------------


def test_create_merge_commit_retries_transient_errors_until_the_forge_answers(forge: Forge):
    forge.script(commit=[BAD_GATEWAY, GATEWAY_TIMEOUT])
    sleeps: list[float] = []
    notes: list[str] = []

    created = forge.publisher(sleeps, notes).create_merge_commit(
        message="Merge PR\n",
        tree_sha=forge.tree,
        parent_shas=(forge.main, forge.head),
    )

    assert created["tree_sha"] == forge.tree
    assert created["parent_shas"] == (forge.main, forge.head)
    assert len(forge.calls("commit")) == 3
    assert sleeps == BACKOFF[:2]
    assert len(notes) == 2 and "502" in notes[0] and "504" in notes[1]


def test_create_merge_commit_surfaces_exhausted_retries_naming_attempts_and_last_error(
    forge: Forge,
):
    forge.script(commit=[BAD_GATEWAY] * 5)
    sleeps: list[float] = []

    with pytest.raises(RuntimeError, match=r"after 5 attempts.*502"):
        forge.publisher(sleeps).create_merge_commit(
            message="Merge PR\n",
            tree_sha=forge.tree,
            parent_shas=(forge.main, forge.head),
        )

    assert len(forge.calls("commit")) == 5
    assert sleeps == BACKOFF


def test_create_merge_commit_does_not_retry_a_permanent_refusal(forge: Forge):
    forge.script(commit=[PERMANENT_REFUSALS["validation"]])
    sleeps: list[float] = []

    with pytest.raises(RuntimeError, match="Validation Failed"):
        forge.publisher(sleeps).create_merge_commit(
            message="Merge PR\n",
            tree_sha=forge.tree,
            parent_shas=(forge.main, forge.head),
        )

    assert len(forge.calls("commit")) == 1
    assert sleeps == []


# --- update_refs -----------------------------------------------------------------------


@pytest.mark.parametrize(
    "transient",
    [GRAPHQL_INTERNAL, BAD_GATEWAY, UNAVAILABLE]
    + [{"stderr": failure + "\n", "exit": 1} for failure in TRANSPORT_FAILURES],
    ids=["graphql-internal", "http-502", "http-503", "reset", "no-host", "deadline", "eof"],
)
def test_update_refs_retries_a_transient_failure_while_the_remote_refs_are_unchanged(
    forge: Forge, transient: dict[str, object]
):
    sleeps: list[float] = []
    publisher = forge.publisher(sleeps)
    candidate, updates = forge.candidate_updates(publisher)
    forge.script(update=[transient])

    publisher.update_refs(updates)

    assert len(forge.calls("graphql")) == 2
    assert sleeps == BACKOFF[:1]
    assert forge.remote_ref("refs/heads/main") == candidate
    assert forge.remote_ref(f"refs/heads/{BRANCH}") == forge.head


def test_update_refs_reconciles_a_dropped_response_without_a_second_mutation(forge: Forge):
    sleeps: list[float] = []
    notes: list[str] = []
    publisher = forge.publisher(sleeps, notes)
    candidate, updates = forge.candidate_updates(publisher)
    # The forge commits the transaction, then the response is lost.
    forge.script(update=[{**BAD_GATEWAY, "apply": True}])

    publisher.update_refs(updates)

    assert len(forge.calls("graphql")) == 1
    assert sleeps == []
    assert forge.remote_ref("refs/heads/main") == candidate
    assert notes, "the reconciliation should be visible in the landing log"


def test_update_refs_recognizes_an_in_flight_attempt_reported_as_a_refusal_on_resend(
    forge: Forge,
):
    sleeps: list[float] = []
    publisher = forge.publisher(sleeps)
    candidate, updates = forge.candidate_updates(publisher)
    # The first attempt fails before the refs are read, so the transaction is resent. By then
    # GitHub has applied the first one, and answers the resend with a before-OID refusal.
    forge.script(
        update=[
            BAD_GATEWAY,
            {**PERMANENT_REFUSALS["required-check"], "apply": True},
        ]
    )

    publisher.update_refs(updates)

    assert len(forge.calls("graphql")) == 2
    assert sleeps == BACKOFF[:1]
    assert forge.remote_ref("refs/heads/main") == candidate


@pytest.mark.parametrize("moved", ["target", "member"])
def test_update_refs_compare_and_swap_mismatch_fails_at_once_without_retry(
    forge: Forge, moved: str
):
    sleeps: list[float] = []
    publisher = forge.publisher(sleeps)
    _candidate, updates = forge.candidate_updates(publisher)
    ref = "refs/heads/main" if moved == "target" else f"refs/heads/{BRANCH}"
    # A concurrent push lands, then the forge reports a transient failure.
    forge.script(update=[{**BAD_GATEWAY, "move": {"ref": ref, "to": forge.side}}])

    with pytest.raises(RuntimeError, match=r"moved.*" + ref):
        publisher.update_refs(updates)

    assert len(forge.calls("graphql")) == 1
    assert sleeps == []
    assert forge.remote_ref(ref) == forge.side


@pytest.mark.parametrize("refusal", sorted(PERMANENT_REFUSALS))
def test_update_refs_does_not_retry_a_permanent_refusal(forge: Forge, refusal: str):
    sleeps: list[float] = []
    publisher = forge.publisher(sleeps)
    _candidate, updates = forge.candidate_updates(publisher)
    forge.script(update=[PERMANENT_REFUSALS[refusal]])

    with pytest.raises(RuntimeError):
        publisher.update_refs(updates)

    assert len(forge.calls("graphql")) == 1
    assert sleeps == []
    assert forge.remote_ref("refs/heads/main") == forge.main


def test_update_refs_surfaces_exhausted_retries_naming_attempts_and_last_error(forge: Forge):
    sleeps: list[float] = []
    publisher = forge.publisher(sleeps)
    _candidate, updates = forge.candidate_updates(publisher)
    forge.script(update=[GRAPHQL_INTERNAL] * 5)

    with pytest.raises(RuntimeError, match=r"after 5 attempts.*Something went wrong"):
        publisher.update_refs(updates)

    assert len(forge.calls("graphql")) == 5
    assert sleeps == BACKOFF
    assert forge.remote_ref("refs/heads/main") == forge.main


def test_update_refs_never_resends_when_the_remote_refs_cannot_be_read(
    forge: Forge, tmp_path: Path
):
    import subprocess

    # A checkout without an origin cannot confirm what the forge did.
    lonely = tmp_path / "lonely"
    subprocess.run(["git", "init", "-q", str(lonely)], check=True)
    sleeps: list[float] = []
    publisher = GitHubMergePublisher(lonely, wait=sleeps.append, notify=lambda _text: None)
    forge.script(update=[BAD_GATEWAY])

    with pytest.raises(RuntimeError, match=r"cannot confirm"):
        publisher.update_refs(
            (
                {
                    "name": "refs/heads/main",
                    "before_oid": forge.main,
                    "after_oid": forge.head,
                    "force": False,
                },
                {
                    "name": f"refs/heads/{BRANCH}",
                    "before_oid": forge.head,
                    "after_oid": forge.head,
                    "force": False,
                },
            )
        )

    assert len(forge.calls("graphql")) == 1, "an unconfirmed transaction is never resent blind"
    assert sleeps == BACKOFF


# --- singleton publication -------------------------------------------------------------


def test_singleton_landing_retries_a_transient_update_inside_the_lane(forge: Forge):
    forge.script(update=[GRAPHQL_INTERNAL])

    status, out, err, sleeps = forge.land()

    assert status == 0, err
    assert f"LANDED PR #{PR}" in out
    assert len(forge.calls("graphql")) == 2
    assert sleeps == BACKOFF[:1]
    assert _git(forge.remote, "rev-parse", "refs/heads/main^1") == forge.main
    assert _git(forge.remote, "rev-parse", "refs/heads/main^2") == forge.head


def test_singleton_landing_retries_a_transient_merge_commit_error(forge: Forge):
    forge.script(commit=[GATEWAY_TIMEOUT])

    status, out, err, sleeps = forge.land()

    assert status == 0, err
    assert f"LANDED PR #{PR}" in out
    assert len(forge.calls("commit")) == 2
    assert sleeps == BACKOFF[:1]


def test_singleton_landing_recognizes_a_merge_the_forge_already_applied(forge: Forge):
    forge.script(update=[{**BAD_GATEWAY, "apply": True}])

    status, out, err, sleeps = forge.land()

    assert status == 0, err
    assert f"LANDED PR #{PR}" in out
    assert len(forge.calls("graphql")) == 1
    assert sleeps == []
    assert _git(forge.remote, "rev-parse", "refs/heads/main^2") == forge.head


def test_singleton_landing_hands_back_a_moved_head_without_retry(forge: Forge):
    forge.script(
        update=[{**BAD_GATEWAY, "move": {"ref": f"refs/heads/{BRANCH}", "to": forge.side}}]
    )

    status, _out, err, sleeps = forge.land()

    assert status == EXIT_HEAD_CHANGED
    assert "head-changed" in err
    assert len(forge.calls("graphql")) == 1
    assert sleeps == []


def test_singleton_landing_hands_back_a_moved_target_without_retry(forge: Forge):
    forge.script(update=[{**BAD_GATEWAY, "move": {"ref": "refs/heads/main", "to": forge.side}}])

    status, _out, err, sleeps = forge.land()

    assert status == EXIT_STALE_MAIN
    assert "stale-main" in err
    assert len(forge.calls("graphql")) == 1
    assert sleeps == []


def test_singleton_landing_respects_a_permanent_refusal(forge: Forge):
    forge.script(update=[PERMANENT_REFUSALS["required-check"]])

    status, _out, err, sleeps = forge.land()

    assert status == EXIT_PUBLISH_FAILED
    assert "publish-failed" in err and "Required status check" in err
    assert len(forge.calls("graphql")) == 1
    assert sleeps == []
    assert forge.remote_ref("refs/heads/main") == forge.main


def test_singleton_landing_reports_publish_failed_when_transient_errors_persist(forge: Forge):
    forge.script(update=[GRAPHQL_INTERNAL] * 5)

    status, _out, err, sleeps = forge.land()

    assert status == EXIT_PUBLISH_FAILED
    assert "publish-failed" in err
    assert "after 5 attempts" in err and "Something went wrong" in err
    assert len(forge.calls("graphql")) == 5
    assert sleeps == BACKOFF
    assert forge.remote_ref("refs/heads/main") == forge.main


# --- batch publication through the broker ----------------------------------------------


@pytest.mark.parametrize("scenario", ["retry", "dropped-response"])
def test_land_batch_publishes_through_transient_errors_without_regating(
    coordinator, tmp_path: Path, scenario: str
):
    _broker, client = coordinator
    remote, state_file, checkouts, _ = _setup_remote_and_worktrees(
        tmp_path,
        [
            {"pr": 91, "base": "main", "branch": "feat/r91", "files": {"r91.txt": "1\n"}},
            {"pr": 92, "base": "main", "branch": "feat/r92", "files": {"r92.txt": "2\n"}},
        ],
    )
    gh_log = tmp_path / "gh.jsonl"
    bin_dir = _install_stateful_gh(tmp_path, remote, state_file, gh_log)
    faults = tmp_path / "faults.json"
    if scenario == "retry":
        script = {"commit": [BAD_GATEWAY], "update": [GRAPHQL_INTERNAL]}
        expected_graphql = 2
    else:
        script = {"update": [{**BAD_GATEWAY, "apply": True}]}
        expected_graphql = 1
    faults.write_text(json.dumps(script), encoding="utf-8")
    gate_log = tmp_path / "gates.jsonl"
    gate_cmd = _python(
        """
import os
from pathlib import Path

with Path(os.environ["BATCH_GATE_LOG"]).open("a", encoding="utf-8") as stream:
    stream.write("gated\\n")
"""
    )
    hold_id, hold_release = _submit_hold(client, tmp_path, checkouts[91], "retry")
    env = _env_with_gh(
        bin_dir,
        {"BATCH_GATE_LOG": str(gate_log), "GH_FAULTS_FILE": str(faults)},
    )
    run_ids = {
        pr: client.submit_land(
            "github",
            pr,
            gate_cmd,
            checkout=str(checkouts[pr]),
            label=f"land #{pr} transient publication error",
            resources={"jobs": 1},
            caller_pid=os.getpid(),
            environment=env,
        )
        for pr in (91, 92)
    }
    hold_release.touch()
    _wait_terminal(client, hold_id, "passed")
    results = {pr: _wait_terminal(client, rid, "passed") for pr, rid in run_ids.items()}

    assert gate_log.read_text(encoding="utf-8").splitlines() == ["gated"]
    step = results[91]["batch"]["steps"][0]
    assert step["outcome"] == "published"
    assert step["published_sha"] == _git(remote, "rev-parse", "refs/heads/main")
    assert set(_git(remote, "ls-tree", "--name-only", "refs/heads/main").splitlines()) == {
        "base.txt",
        "r91.txt",
        "r92.txt",
    }
    recorded = [
        json.loads(line) for line in gh_log.read_text(encoding="utf-8").splitlines() if line
    ]
    graphql = [call for call in recorded if call["args"][:2] == ["api", "graphql"]]
    commits = [
        call
        for call in recorded
        if call["args"][:3] == ["api", "--method", "POST"]
        and call["args"][3].endswith("/git/commits")
    ]
    assert len(graphql) == expected_graphql
    if scenario == "retry":
        # The faulted commit request is repeated unchanged, then the batch carries on.
        assert commits[0]["payload"] == commits[1]["payload"]
    assert len({json.dumps(call["payload"], sort_keys=True) for call in commits}) == 2
