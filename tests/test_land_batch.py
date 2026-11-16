"""Behavioral tests for multi-PR land batching and binary-split bisection."""

from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

import pytest

from conftest import caller_environment, wait_for

GIT = shutil.which("git") or "git"


def _python(source: str, *arguments: str | Path) -> list[str]:
    return [sys.executable, "-u", "-c", source, *(str(value) for value in arguments)]


def _git(repository: Path, *arguments: str) -> str:
    completed = subprocess.run(
        [GIT, "-C", str(repository), *arguments],
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr
    return completed.stdout.strip()


def _head(repository: Path) -> str:
    return _git(repository, "rev-parse", "HEAD")


def _wait_terminal(client, run_id: str, expected_status: str | None = None) -> dict[str, object]:
    terminal = {"passed", "failed", "cancelled", "interrupted"}
    result: dict[str, object] = {}

    def finished() -> bool:
        nonlocal result
        row = client.status(run_id)
        if row["status"] in terminal:
            result = row
            return True
        return False

    wait_for(finished, f"run {run_id} did not finish", timeout=30.0)
    if expected_status is not None:
        if result["status"] != expected_status:
            log_text = client.log(run_id, offset=0, limit=65536)["text"]
            raise AssertionError(f"run {run_id} returned {result}\n--- log ---\n{log_text}")
    return result


def _setup_remote_and_worktrees(tmp_path: Path, specs: list[dict[str, object]]):
    """Create a bare origin remote, base checkouts, and one PR branch/worktree per spec.

    Each spec is ``{"pr": int, "base": str, "branch": str, "files": {path: text}}``.
    If ``base`` is not ``main``, a branch off initial ``main`` is created for it too.
    """
    tmp_path.mkdir(parents=True, exist_ok=True)
    remote = tmp_path / "origin.git"
    subprocess.run([GIT, "init", "--bare", str(remote)], check=True, capture_output=True, text=True)

    seed = tmp_path / "seed"
    seed.mkdir()
    _git(seed, "init", "-b", "main")
    _git(seed, "config", "user.name", "AGCoord batch test")
    _git(seed, "config", "user.email", "batch@example.invalid")
    (seed / "base.txt").write_text("base\n", encoding="utf-8")
    _git(seed, "add", "base.txt")
    _git(seed, "commit", "-m", "initial")
    _git(seed, "remote", "add", "origin", str(remote))
    _git(seed, "push", "-u", "origin", "main")
    main_sha = _head(seed)

    bases = {"main": main_sha}
    for spec in specs:
        base = str(spec.get("base", "main"))
        if base not in bases:
            _git(seed, "branch", base, main_sha)
            _git(seed, "push", "-u", "origin", base)
            bases[base] = main_sha

    pr_state: dict[str, dict[str, object]] = {}
    checkouts: dict[int, Path] = {}
    for spec in specs:
        pr_number = int(spec["pr"])
        base = str(spec.get("base", "main"))
        branch = str(spec["branch"])
        wt = tmp_path / f"wt-pr-{pr_number}"
        subprocess.run(
            [GIT, "clone", "--branch", base, str(remote), str(wt)],
            check=True,
            capture_output=True,
            text=True,
        )
        _git(wt, "config", "user.name", "AGCoord batch test")
        _git(wt, "config", "user.email", "batch@example.invalid")
        _git(wt, "switch", "-c", branch)
        files: dict[str, str] = dict(spec["files"])  # type: ignore[arg-type]
        for rel, content in files.items():
            target = wt / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(content, encoding="utf-8")
            _git(wt, "add", rel)
        _git(wt, "commit", "-m", f"PR #{pr_number}")
        head_sha = _head(wt)
        _git(wt, "push", "-u", "origin", branch)
        checkouts[pr_number] = wt
        pr_state[str(pr_number)] = {
            "number": pr_number,
            "state": "OPEN",
            "isDraft": False,
            "baseRefName": base,
            "headRefName": branch,
            "headRefOid": head_sha,
            "isCrossRepository": False,
            "title": f"PR #{pr_number}",
            "headRepositoryOwner": {"login": "pytest"},
        }

    state_file = tmp_path / "pr-state.json"
    state_file.write_text(json.dumps(pr_state, indent=2), encoding="utf-8")
    return remote, state_file, checkouts, pr_state


def _install_stateful_gh(tmp_path: Path, remote: Path, state_file: Path, log_file: Path) -> Path:
    """Install a fake `gh` CLI that reads PR metadata and advances refs in `remote`."""
    bin_dir = tmp_path / "fake-gh"
    bin_dir.mkdir(parents=True, exist_ok=True)
    gh = bin_dir / "gh"
    gh.write_text(
        f"""#!{sys.executable}
import base64
import fcntl
import json
import os
from pathlib import Path
import subprocess
import sys

REMOTE = Path({str(remote)!r})
STATE_FILE = Path({str(state_file)!r})
LOG_FILE = Path({str(log_file)!r})

def git_remote(*args, input_bytes=None):
    return subprocess.run(
        [{GIT!r}, "-c", "user.name=AGCoord Forge", "-c", "user.email=forge@example.invalid", "--git-dir", str(REMOTE), *args],
        input=input_bytes,
        capture_output=True,
        check=False,
    )

def rev_parse(ref):
    res = git_remote("rev-parse", "--verify", f"{{ref}}^{{{{commit}}}}")
    if res.returncode != 0:
        res = git_remote("rev-parse", "--verify", ref)
    return res.stdout.decode().strip() if res.returncode == 0 else None

args = sys.argv[1:]
raw_stdin = sys.stdin.read()
payload = json.loads(raw_stdin) if raw_stdin else None

with LOG_FILE.open("a", encoding="utf-8") as stream:
    stream.write(json.dumps({{"args": args, "payload": payload}}) + "\\n")

if args[:2] == ["pr", "view"]:
    number = str(args[2])
    with STATE_FILE.open("r+", encoding="utf-8") as stream:
        fcntl.flock(stream, fcntl.LOCK_SH)
        data = json.load(stream)
    if number not in data:
        raise SystemExit(f"unknown pr {{number}}")
    print(json.dumps(data[number]))
elif args[:2] == ["repo", "view"]:
    print(json.dumps({{"id": "R_batch_test", "nameWithOwner": "test/repository"}}))
elif args[:3] == ["api", "--method", "POST"] and args[3].endswith("/git/commits"):
    parents = payload["parents"]
    tree = payload["tree"]
    message = payload["message"]
    # Verify tree exists locally in bare remote or worktree object store? Wait: bare remote
    # only receives pushed objects unless git/trees uploaded them, OR create commit if tree exists.
    tree_check = git_remote("cat-file", "-t", tree)
    if tree_check.returncode != 0 or tree_check.stdout.decode().strip() != "tree":
        print("Tree does not exist (422)", file=sys.stderr)
        raise SystemExit(1)
    cmd = ["commit-tree", tree]
    for p in parents:
        cmd.extend(["-p", p])
    cmd.extend(["-m", message])
    created = git_remote(*cmd)
    if created.returncode != 0:
        print(created.stderr.decode(), file=sys.stderr)
        raise SystemExit(1)
    commit_sha = created.stdout.decode().strip()
    print(json.dumps({{
        "sha": commit_sha,
        "tree": {{"sha": tree}},
        "parents": [{{"sha": p}} for p in parents],
    }}))
elif args[:3] == ["api", "--method", "POST"] and args[3].endswith("/git/blobs"):
    content = base64.b64decode(payload["content"])
    created = git_remote("hash-object", "-w", "--stdin", input_bytes=content)
    if created.returncode != 0:
        raise SystemExit(1)
    print(json.dumps({{"sha": created.stdout.decode().strip()}}))
elif args[:3] == ["api", "--method", "POST"] and args[3].endswith("/git/trees"):
    base_tree = payload.get("base_tree")
    # Build tree index in bare remote using read-tree + update-index + write-tree
    tmp_idx = REMOTE / "tmp-gh-index"
    env = os.environ.copy()
    env["GIT_INDEX_FILE"] = str(tmp_idx)
    if tmp_idx.exists():
        tmp_idx.unlink()
    if base_tree:
        subprocess.run([{GIT!r}, "--git-dir", str(REMOTE), "read-tree", base_tree], env=env, check=True)
    for item in payload["tree"]:
        if item.get("sha") is None:
            subprocess.run(
                [{GIT!r}, "--git-dir", str(REMOTE), "update-index", "--force-remove", item["path"]],
                env=env,
                check=True,
            )
        else:
            subprocess.run(
                [
                    {GIT!r},
                    "--git-dir",
                    str(REMOTE),
                    "update-index",
                    "--add",
                    "--cacheinfo",
                    f"{{item['mode']}},{{item['sha']}},{{item['path']}}",
                ],
                env=env,
                check=True,
            )
    res = subprocess.run(
        [{GIT!r}, "--git-dir", str(REMOTE), "write-tree"],
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    if tmp_idx.exists():
        tmp_idx.unlink()
    print(json.dumps({{"sha": res.stdout.strip()}}))
elif args[:2] == ["api", "graphql"]:
    if os.environ.get("BATCH_CLEAR_LOG"):
        from agcoord.queue import CoordinatorClient
        leader = os.environ["AGCOORD_RUN_ID"]
        row = CoordinatorClient(state_dir=os.environ["AGCOORD_STATE_DIR"]).admitted_run_status(leader)
        with open(os.environ["BATCH_CLEAR_LOG"], "a", encoding="utf-8") as cleared:
            cleared.write(json.dumps(row.get("batch_slice")) + "\\n")
    updates = payload["variables"]["input"]["refUpdates"]
    # Atomically verify all beforeOid values and update remote refs + mark PRs MERGED.
    with STATE_FILE.open("r+", encoding="utf-8") as stream:
        fcntl.flock(stream, fcntl.LOCK_EX)
        pr_data = json.load(stream)
        for u in updates:
            name = u["name"]
            current = rev_parse(name)
            if current != u["beforeOid"]:
                print(json.dumps({{"errors": [{{"message": f"stale ref {{name}}: {{current}} != {{u['beforeOid']}}"}}]}}))
                raise SystemExit(1)
        for u in updates:
            res = git_remote("update-ref", u["name"], u["afterOid"], u["beforeOid"])
            if res.returncode != 0:
                print(res.stderr.decode(), file=sys.stderr)
                raise SystemExit(1)
        target_ref = updates[0]["name"]
        base_name = target_ref.removeprefix("refs/heads/")
        head_refs = {{u["name"].removeprefix("refs/heads/"): u["beforeOid"] for u in updates[1:]}}
        for entry in pr_data.values():
            if entry["baseRefName"] == base_name and entry["headRefName"] in head_refs:
                entry["state"] = "MERGED"
        stream.seek(0)
        stream.truncate()
        json.dump(pr_data, stream, indent=2)
    mutation_id = payload["variables"]["input"]["clientMutationId"]
    print(json.dumps({{"data": {{"updateRefs": {{"clientMutationId": mutation_id}}}}}}))
elif args[:1] == ["api"] and "/compare/" in args[1]:
    spec = args[1].split("/compare/", 1)[1]
    a, b = spec.split("...", 1)
    res = git_remote("merge-base", a, b)
    sha = res.stdout.decode().strip() if res.returncode == 0 else "0" * 40
    print(json.dumps({{"merge_base_commit": {{"sha": sha}}}}))
else:
    print(f"unexpected gh arguments: {{args!r}}", file=sys.stderr)
    raise SystemExit(93)
""",
        encoding="utf-8",
    )
    gh.chmod(0o755)
    return bin_dir


def _env_with_gh(bin_dir: Path, extra: dict[str, str] | None = None) -> dict[str, str]:
    env = caller_environment()
    env["PATH"] = f"{bin_dir}{os.pathsep}{env['PATH']}"
    if extra:
        env.update(extra)
    return env


def _submit_hold(client, tmp_path: Path, checkout: Path, suffix: str) -> tuple[str, Path]:
    entered = tmp_path / f"hold-entered-{suffix}"
    release = tmp_path / f"hold-release-{suffix}"
    hold_id = client.submit(
        _python(
            """
from pathlib import Path
import sys, time
Path(sys.argv[1]).touch()
while not Path(sys.argv[2]).exists():
    time.sleep(0.02)
""",
            entered,
            release,
        ),
        kind="check",
        checkout=str(checkout),
        resources={"jobs": 1},
        caller_pid=os.getpid(),
        environment=caller_environment(),
    )
    wait_for(entered.exists, "hold check did not start")
    return hold_id, release


@pytest.mark.parametrize("bad_pr", [None, 1, 2])
def test_batch_slice_admission_and_status(coordinator, tmp_path: Path, bad_pr):
    _broker, client = coordinator
    remote, state_file, checkouts, pr_state = _setup_remote_and_worktrees(
        tmp_path,
        [
            {"pr": pr, "branch": f"feat/p{pr}", "files": {f"p{pr}.txt": "yes\n"}}
            for pr in (1, 2)
        ],
    )
    bin_dir = _install_stateful_gh(tmp_path, remote, state_file, tmp_path / "gh.jsonl")
    gate_log = tmp_path / "slice-gates.jsonl"
    gate = _python('''
import json, os, subprocess, sys
from pathlib import Path
from agcoord.queue import CoordinatorClient
head = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
args = [sys.executable, "-m", "agcoord", "--json", "verify-admission", "--state-dir", os.environ["AGCOORD_STATE_DIR"],
        "--checkout", os.getcwd(), "--run-id", os.environ["AGCOORD_RUN_ID"], "--kind", "land",
        "--worker-pid", str(os.getppid()), "--head-sha"]
good = subprocess.run(args + [head], capture_output=True, text=True)
wrong = subprocess.run(args + ["f" * 40], capture_output=True, text=True)
row = CoordinatorClient(state_dir=os.environ["AGCOORD_STATE_DIR"]).admitted_run_status(os.environ["AGCOORD_RUN_ID"])
with open(os.environ["BATCH_GATE_LOG"], "a") as output:
    output.write(json.dumps({"head": head, "slice": row.get("batch_slice"), "good": good.returncode,
                             "good_err": good.stderr, "wrong": wrong.returncode, "wrong_err": wrong.stderr}) + "\\n")
if good.returncode or not wrong.returncode:
    sys.exit(91)
if os.environ["BAD_PR"] and Path("p" + os.environ["BAD_PR"] + ".txt").exists():
    sys.exit(5)
''')
    hold_id, release = _submit_hold(client, tmp_path, checkouts[1], "slice")
    clear_log = tmp_path / "cleared-slices.jsonl"
    env = _env_with_gh(bin_dir, {"BATCH_GATE_LOG": str(gate_log), "BAD_PR": str(bad_pr or ""),
                                 "BATCH_CLEAR_LOG": str(clear_log)})
    run_ids = {pr: client.submit_land("github", pr, gate, checkout=str(checkouts[pr]),
                resources={"jobs": 1}, caller_pid=os.getpid(), environment=env) for pr in (1, 2)}
    release.touch()
    _wait_terminal(client, hold_id, "passed")
    rows = {pr: _wait_terminal(client, run_ids[pr]) for pr in (1, 2)}
    gates = [json.loads(line) for line in gate_log.read_text().splitlines()]
    assert all(json.loads(line) is None for line in clear_log.read_text().splitlines())
    expected_slices = [[1, 2]] if bad_pr is None else [[1, 2], [1], [2]]
    assert len(gates) == len(expected_slices), gates
    for observed, members in zip(gates, expected_slices):
        assert observed["good"] == 0, observed
        assert observed["wrong"] != 0 and "broker-admission-mismatch" in observed["wrong_err"]
        assert observed["slice"]["gating_head_sha"] == observed["head"]
        assert observed["slice"]["members"] == [
            {"run_id": run_ids[pr], "branch": f"feat/p{pr}",
             "head_sha": pr_state[str(pr)]["headRefOid"], "original_checkout": str(checkouts[pr]),
             "publication": rows[pr]["publication"]} for pr in members
        ]
    for pr in (1, 2):
        assert rows[pr]["status"] == ("failed" if pr == bad_pr else "passed")
        assert rows[pr]["batch_slice"] is None
        if pr == bad_pr:
            assert rows[pr]["failure_reason"] == "gate-failed"


def test_land_batch_merges_and_publishes_clean_prs_with_one_gate(
    coordinator,
    tmp_path: Path,
):
    _broker, client = coordinator
    remote, state_file, checkouts, _ = _setup_remote_and_worktrees(
        tmp_path,
        [
            {"pr": 1, "base": "main", "branch": "feat/one", "files": {"one.txt": "1\n"}},
            {"pr": 2, "base": "main", "branch": "feat/two", "files": {"two.txt": "2\n"}},
            {"pr": 3, "base": "main", "branch": "feat/three", "files": {"three.txt": "3\n"}},
            {"pr": 4, "base": "release-1", "branch": "feat/rel", "files": {"rel.txt": "r\n"}},
        ],
    )
    gh_log = tmp_path / "gh.jsonl"
    bin_dir = _install_stateful_gh(tmp_path, remote, state_file, gh_log)
    gate_log = tmp_path / "gates.jsonl"
    gate_cmd = _python(
        """
import json, os
from pathlib import Path
import subprocess

head = subprocess.run(["git", "rev-parse", "HEAD"], check=True, text=True, capture_output=True).stdout.strip()
files = sorted(p.name for p in Path.cwd().iterdir() if p.name != ".git")
print(f"gated files={files}", flush=True)
with Path(os.environ["BATCH_GATE_LOG"]).open("a", encoding="utf-8") as s:
    s.write(json.dumps({"head": head, "files": files}) + "\\n")
""",
    )
    hold_id, hold_release = _submit_hold(client, tmp_path, checkouts[1], "clean")

    env = _env_with_gh(bin_dir, {"BATCH_GATE_LOG": str(gate_log)})
    run_ids = {}
    for pr in (1, 2, 3, 4):
        run_ids[pr] = client.submit_land(
            "github",
            pr,
            gate_cmd,
            checkout=str(checkouts[pr]),
            resources={"jobs": 1},
            caller_pid=os.getpid(),
            environment=env,
        )
    hold_release.touch()
    _wait_terminal(client, hold_id, "passed")

    results = {pr: _wait_terminal(client, rid, "passed") for pr, rid in run_ids.items()}

    gated_runs = [json.loads(line) for line in gate_log.read_text(encoding="utf-8").splitlines() if line]
    assert len(gated_runs) == 2
    main_gate = next(entry for entry in gated_runs if "one.txt" in entry["files"])
    rel_gate = next(entry for entry in gated_runs if "rel.txt" in entry["files"])
    assert set(main_gate["files"]) == {"base.txt", "one.txt", "two.txt", "three.txt"}
    assert set(rel_gate["files"]) == {"base.txt", "rel.txt"}

    # All 3 main-target PR rows share the batch gate log.
    log_texts = {pr: str(client.log(run_ids[pr], offset=0, limit=65536)["text"]) for pr in (1, 2, 3)}
    assert all("one.txt" in text and "three.txt" in text for text in log_texts.values())

    # Structured batch history is exposed on leader and all followers, and null on standalone PR #4.
    main_final_sha = _git(remote, "rev-parse", "refs/heads/main")
    assert results[4]["batch"] is None
    for idx, pr in enumerate((1, 2, 3)):
        batch = results[pr]["batch"]
        assert batch is not None
        assert batch["leader_run_id"] == run_ids[1]
        assert batch["role"] == ("leader" if pr == 1 else "follower")
        assert batch["target_branch"] == "main"
        assert [m["run_id"] for m in batch["members"]] == [run_ids[1], run_ids[2], run_ids[3]]
        assert [m["status"] for m in batch["members"]] == ["passed", "passed", "passed"]
        assert batch["members"][idx]["position"] == idx
        assert len(batch["steps"]) == 1
        step = batch["steps"][0]
        assert step["step"] == 1
        assert step["member_run_ids"] == [run_ids[1], run_ids[2], run_ids[3]]
        assert step["gated_member_run_ids"] == [run_ids[1], run_ids[2], run_ids[3]]
        assert step["dropped"] == []
        assert step["gating_head_sha"] == main_gate["head"]
        assert step["gate_exit_status"] == 0
        assert step["outcome"] == "published"
        assert step["published_sha"] == main_final_sha

    snap = client.snapshot()
    snap_by_id = {row["run_id"]: row for row in snap["recent"]}
    assert snap_by_id[run_ids[2]]["batch"] == results[2]["batch"]
    assert snap_by_id[run_ids[4]]["batch"] is None

    # Remote refs/heads/main has all 3 PR files; refs/heads/release-1 stays isolated.
    main_tree = _git(remote, "ls-tree", "--name-only", "refs/heads/main").splitlines()
    rel_tree = _git(remote, "ls-tree", "--name-only", "refs/heads/release-1").splitlines()
    assert set(main_tree) == {"base.txt", "one.txt", "two.txt", "three.txt"}
    assert set(rel_tree) == {"base.txt", "rel.txt"}

    gh_calls = [json.loads(line) for line in gh_log.read_text(encoding="utf-8").splitlines() if line]
    graphql_calls = [c for c in gh_calls if c["args"][:2] == ["api", "graphql"]]
    assert len(graphql_calls) == 2
    assert sorted(len(c["payload"]["variables"]["input"]["refUpdates"]) for c in graphql_calls) == [2, 4]


def test_land_batch_excludes_pre_gate_conflict_with_merge_error(
    coordinator,
    tmp_path: Path,
):
    _broker, client = coordinator
    remote, state_file, checkouts, _ = _setup_remote_and_worktrees(
        tmp_path,
        [
            {"pr": 10, "base": "main", "branch": "feat/clean-a", "files": {"a.txt": "a\n"}},
            {"pr": 11, "base": "main", "branch": "feat/shared-first", "files": {"shared.txt": "first\n"}},
            {"pr": 12, "base": "main", "branch": "feat/shared-conflict", "files": {"shared.txt": "conflict\n"}},
            {"pr": 13, "base": "main", "branch": "feat/clean-b", "files": {"b.txt": "b\n"}},
        ],
    )
    gh_log = tmp_path / "gh.jsonl"
    bin_dir = _install_stateful_gh(tmp_path, remote, state_file, gh_log)
    gate_log = tmp_path / "gates.jsonl"
    gate_cmd = _python(
        """
import json, os
from pathlib import Path

files = sorted(p.name for p in Path.cwd().iterdir() if p.name != ".git")
with Path(os.environ["BATCH_GATE_LOG"]).open("a", encoding="utf-8") as s:
    s.write(json.dumps({"files": files}) + "\\n")
""",
    )
    hold_id, hold_release = _submit_hold(client, tmp_path, checkouts[10], "conflict")

    env = _env_with_gh(bin_dir, {"BATCH_GATE_LOG": str(gate_log)})
    run_ids = {
        pr: client.submit_land(
            "github",
            pr,
            gate_cmd,
            checkout=str(checkouts[pr]),
            resources={"jobs": 1},
            caller_pid=os.getpid(),
            environment=env,
        )
        for pr in (10, 11, 12, 13)
    }
    hold_release.touch()
    _wait_terminal(client, hold_id, "passed")

    assert _wait_terminal(client, run_ids[10], "passed")
    assert _wait_terminal(client, run_ids[11], "passed")
    conflict_row = _wait_terminal(client, run_ids[12], "failed")
    assert conflict_row["failure_reason"] == "merge-error"
    assert conflict_row["exit_status"] == 79
    assert _wait_terminal(client, run_ids[13], "passed")

    gated_runs = [json.loads(line) for line in gate_log.read_text(encoding="utf-8").splitlines() if line]
    assert len(gated_runs) == 1
    assert set(gated_runs[0]["files"]) == {"base.txt", "a.txt", "shared.txt", "b.txt"}
    assert (
        _git(remote, "show", "refs/heads/main:shared.txt") == "first"
    )
    conflict_batch = conflict_row["batch"]
    assert conflict_batch is not None
    assert conflict_batch["role"] == "follower"
    assert conflict_batch["members"][2]["status"] == "failed"
    assert conflict_batch["members"][2]["failure_reason"] == "merge-error"
    assert conflict_batch["steps"][0]["member_run_ids"] == [
        run_ids[10],
        run_ids[11],
        run_ids[12],
        run_ids[13],
    ]
    assert conflict_batch["steps"][0]["gated_member_run_ids"] == [
        run_ids[10],
        run_ids[11],
        run_ids[13],
    ]
    assert conflict_batch["steps"][0]["dropped"] == [
        {"run_id": run_ids[12], "reason": "merge-conflict"}
    ]
    assert conflict_batch["steps"][0]["outcome"] == "published"


def test_land_batch_binary_bisects_and_isolates_order_dependent_failure(
    coordinator,
    tmp_path: Path,
):
    _broker, client = coordinator
    # Order-dependent failure: PR #22 adds left.txt="present", PR #23 adds right_boom.txt="x".
    # Gate fails ONLY when BOTH left.txt AND right_boom.txt exist in the gated tree.
    # Because Left=[21,22] passes and publishes onto main BEFORE Right=[23,24] is gated,
    # Right is gated against updated main (which has left.txt), reproducing the failure
    # cleanly and isolating PR #23 -> gate-failed while #21, #22, #24 all land!
    remote, state_file, checkouts, _ = _setup_remote_and_worktrees(
        tmp_path,
        [
            {"pr": 21, "base": "main", "branch": "feat/p21", "files": {"p21.txt": "1\n"}},
            {"pr": 22, "base": "main", "branch": "feat/p22", "files": {"left.txt": "present\n"}},
            {"pr": 23, "base": "main", "branch": "feat/p23", "files": {"right_boom.txt": "x\n"}},
            {"pr": 24, "base": "main", "branch": "feat/p24", "files": {"p24.txt": "4\n"}},
        ],
    )
    gh_log = tmp_path / "gh.jsonl"
    bin_dir = _install_stateful_gh(tmp_path, remote, state_file, gh_log)
    gate_log = tmp_path / "gates.jsonl"
    gate_cmd = _python(
        """
import json, os
from pathlib import Path

files = sorted(p.name for p in Path.cwd().iterdir() if p.name != ".git")
ok = not ("left.txt" in files and "right_boom.txt" in files)
with Path(os.environ["BATCH_GATE_LOG"]).open("a", encoding="utf-8") as s:
    s.write(json.dumps({"files": files, "ok": ok}) + "\\n")
if not ok:
    print("order-dependent conflict between left.txt and right_boom.txt", flush=True)
    raise SystemExit(5)
""",
    )
    hold_id, hold_release = _submit_hold(client, tmp_path, checkouts[21], "bisect")

    env = _env_with_gh(bin_dir, {"BATCH_GATE_LOG": str(gate_log)})
    run_ids = {
        pr: client.submit_land(
            "github",
            pr,
            gate_cmd,
            checkout=str(checkouts[pr]),
            resources={"jobs": 1},
            caller_pid=os.getpid(),
            environment=env,
        )
        for pr in (21, 22, 23, 24)
    }
    hold_release.touch()
    _wait_terminal(client, hold_id, "passed")

    _wait_terminal(client, run_ids[21], "passed")
    _wait_terminal(client, run_ids[22], "passed")
    failed = _wait_terminal(client, run_ids[23], "failed")
    assert failed["failure_reason"] == "gate-failed"
    assert failed["gate_exit_status"] == 5
    _wait_terminal(client, run_ids[24], "passed")

    main_tree = set(_git(remote, "ls-tree", "--name-only", "refs/heads/main").splitlines())
    assert main_tree == {"base.txt", "p21.txt", "left.txt", "p24.txt"}

    gated_runs = [json.loads(line) for line in gate_log.read_text(encoding="utf-8").splitlines() if line]
    # Exact binary split sequence: [21,22,23,24] fail -> [21,22] pass & publish ->
    # [23,24] fail -> [23] fail -> [24] pass & publish = 5 gate invocations.
    assert [entry["ok"] for entry in gated_runs] == [False, True, False, False, True]

    # Early-finalized follower #22 (finalized at Step 2) exposes ALL 5 steps of the batch
    # in status, snapshot, agc --json show, and plain agc show!
    final_rows = {pr: client.status(run_ids[pr]) for pr in (21, 22, 23, 24)}
    expected_outcomes = ["bisected", "published", "bisected", "gate-failed", "published"]
    for pr in (21, 22, 23, 24):
        batch = final_rows[pr]["batch"]
        assert batch is not None
        assert batch["role"] == ("leader" if pr == 21 else "follower")
        assert [s["outcome"] for s in batch["steps"]] == expected_outcomes
        assert batch["steps"][0]["split"] == [2, 2]
        assert batch["steps"][2]["split"] == [1, 1]
        assert [m["status"] for m in batch["members"]] == [
            "passed",
            "passed",
            "failed",
            "passed",
        ]
    shown_plain = subprocess.run(
        [
            sys.executable,
            "-m",
            "agcoord",
            "show",
            run_ids[22],
            "--state-dir",
            str(client.paths.state_dir),
        ],
        capture_output=True,
        text=True,
        env=caller_environment(),
        check=True,
    )
    assert "Batch & Bisect History:" in shown_plain.stdout
    assert "gate failed (exit 5) -> bisected 4 into 2 + 2" in shown_plain.stdout
    assert "gate failed (exit 5) -> bisected 2 into 1 + 1" in shown_plain.stdout
    assert f"Step 5: [#24 ({run_ids[24]})]" in shown_plain.stdout


def test_land_batch_rechecks_head_changed_and_stale_main_mid_batch(
    coordinator,
    tmp_path: Path,
):
    _broker, client = coordinator
    # Case A: PR #32 head moves while batch gate is running -> PR #32 fails head-changed (76),
    # PR #31 still publishes cleanly as remaining slice.
    remote, state_file, checkouts, pr_state = _setup_remote_and_worktrees(
        tmp_path / "case-head",
        [
            {"pr": 31, "base": "main", "branch": "feat/p31", "files": {"p31.txt": "1\n"}},
            {"pr": 32, "base": "main", "branch": "feat/p32", "files": {"p32.txt": "2\n"}},
        ],
    )
    gh_log = tmp_path / "gh-head.jsonl"
    bin_dir = _install_stateful_gh(tmp_path, remote, state_file, gh_log)
    gate_entered = tmp_path / "head-gate-entered"
    gate_release = tmp_path / "head-gate-release"
    gate_cmd = _python(
        """
import os
from pathlib import Path
import sys, time
Path(sys.argv[1]).touch()
while not Path(sys.argv[2]).exists():
    time.sleep(0.02)
""",
        gate_entered,
        gate_release,
    )
    hold_id, hold_release = _submit_hold(client, tmp_path, checkouts[31], "head-a")
    env = _env_with_gh(bin_dir)
    r31 = client.submit_land(
        "github",
        31,
        gate_cmd,
        checkout=str(checkouts[31]),
        resources={"jobs": 1},
        caller_pid=os.getpid(),
        environment=env,
    )
    r32 = client.submit_land(
        "github",
        32,
        gate_cmd,
        checkout=str(checkouts[32]),
        resources={"jobs": 1},
        caller_pid=os.getpid(),
        environment=env,
    )
    hold_release.touch()
    _wait_terminal(client, hold_id, "passed")
    wait_for(gate_entered.exists, "batch gate did not start")

    # Mutate PR #32's remote branch and PR metadata headRefOid mid-gate.
    (checkouts[32] / "p32_extra.txt").write_text("moved\n", encoding="utf-8")
    _git(checkouts[32], "add", "p32_extra.txt")
    _git(checkouts[32], "commit", "-m", "move pr 32 head")
    new_head_32 = _head(checkouts[32])
    _git(checkouts[32], "push", "origin", "feat/p32")
    pr_state["32"]["headRefOid"] = new_head_32
    state_file.write_text(json.dumps(pr_state, indent=2), encoding="utf-8")
    gate_release.touch()

    _wait_terminal(client, r31, "passed")
    row32 = _wait_terminal(client, r32, "failed")
    assert row32["failure_reason"] == "head-changed"
    assert row32["exit_status"] == 76
    assert set(_git(remote, "ls-tree", "--name-only", "refs/heads/main").splitlines()) == {"base.txt", "p31.txt"}

    # Case B: target branch `main` advances externally mid-gate -> fails stale-main (75) without publishing.
    (tmp_path / "case-stale").mkdir()
    remote_b, state_file_b, checkouts_b, _ = _setup_remote_and_worktrees(
        tmp_path / "case-stale",
        [
            {"pr": 41, "base": "main", "branch": "feat/p41", "files": {"p41.txt": "1\n"}},
            {"pr": 42, "base": "main", "branch": "feat/p42", "files": {"p42.txt": "2\n"}},
        ],
    )
    gh_log_b = tmp_path / "gh-stale.jsonl"
    bin_dir_b = _install_stateful_gh(tmp_path / "bin-b", remote_b, state_file_b, gh_log_b)
    gate_entered_b = tmp_path / "stale-gate-entered"
    gate_release_b = tmp_path / "stale-gate-release"
    gate_cmd_b = _python(
        """
from pathlib import Path
import sys, time
Path(sys.argv[1]).touch()
while not Path(sys.argv[2]).exists():
    time.sleep(0.02)
""",
        gate_entered_b,
        gate_release_b,
    )
    hold_id_b, hold_release_b = _submit_hold(client, tmp_path, checkouts_b[41], "stale-b")
    env_b = _env_with_gh(bin_dir_b)
    r41 = client.submit_land(
        "github",
        41,
        gate_cmd_b,
        checkout=str(checkouts_b[41]),
        resources={"jobs": 1},
        caller_pid=os.getpid(),
        environment=env_b,
    )
    r42 = client.submit_land(
        "github",
        42,
        gate_cmd_b,
        checkout=str(checkouts_b[42]),
        resources={"jobs": 1},
        caller_pid=os.getpid(),
        environment=env_b,
    )
    hold_release_b.touch()
    _wait_terminal(client, hold_id_b, "passed")
    wait_for(gate_entered_b.exists, "batch gate B did not start")

    # Externally advance `main` on remote_b while gate is paused.
    external = tmp_path / "external-main"
    subprocess.run([GIT, "clone", "--branch", "main", str(remote_b), str(external)], check=True, capture_output=True)
    _git(external, "config", "user.name", "External")
    _git(external, "config", "user.email", "ext@example.invalid")
    (external / "ext.txt").write_text("ext\n", encoding="utf-8")
    _git(external, "add", "ext.txt")
    _git(external, "commit", "-m", "external main advance")
    ext_main = _head(external)
    _git(external, "push", "origin", "main")
    gate_release_b.touch()

    row41 = _wait_terminal(client, r41, "failed")
    row42 = _wait_terminal(client, r42, "failed")
    assert row41["failure_reason"] == "stale-main"
    assert row41["exit_status"] == 75
    assert row42["failure_reason"] == "stale-main"
    assert row42["exit_status"] == 75
    assert _git(remote_b, "rev-parse", "refs/heads/main") == ext_main
    assert row41["batch"]["steps"][0]["outcome"] == "stale-main"
    assert row42["batch"]["steps"][0]["outcome"] == "stale-main"


def test_land_batch_records_publish_failure_reuse_full_and_requeued_follower_rebatch(
    coordinator,
    tmp_path: Path,
):
    _broker, client = coordinator
    # Part 1: Publish failure records step outcome "publish-failed" and member status "failed".
    remote, state_file, checkouts, pr_state = _setup_remote_and_worktrees(
        tmp_path / "pub-fail",
        [
            {"pr": 51, "base": "main", "branch": "feat/p51", "files": {"p51.txt": "1\n"}},
            {"pr": 52, "base": "main", "branch": "feat/p52", "files": {"p52.txt": "2\n"}},
        ],
    )
    bin_dir = _install_stateful_gh(tmp_path / "bin-pf", remote, state_file, tmp_path / "gh-pf.jsonl")
    # Corrupt graphql updateRefs by replacing fake gh's graphql branch with a rejection.
    gh_script = bin_dir / "gh"
    gh_script.write_text(
        gh_script.read_text(encoding="utf-8").replace(
            'elif args[:2] == ["api", "graphql"]:',
            'elif args[:2] == ["api", "graphql"]:\n    print("atomic update rejected", file=sys.stderr)\n    raise SystemExit(1)',
        ),
        encoding="utf-8",
    )
    hold_id, hold_release = _submit_hold(client, tmp_path, checkouts[51], "pf")
    env_pf = _env_with_gh(bin_dir)
    r51 = client.submit_land(
        "github", 51, ["true"], checkout=str(checkouts[51]),
        resources={"jobs": 1}, caller_pid=os.getpid(), environment=env_pf,
    )
    r52 = client.submit_land(
        "github", 52, ["true"], checkout=str(checkouts[52]),
        resources={"jobs": 1}, caller_pid=os.getpid(), environment=env_pf,
    )
    hold_release.touch()
    _wait_terminal(client, hold_id, "passed")
    row51 = _wait_terminal(client, r51, "failed")
    row52 = _wait_terminal(client, r52, "failed")
    assert row51["failure_reason"] == "publish-failed"
    assert row52["failure_reason"] == "publish-failed"
    assert row51["batch"]["steps"][0]["outcome"] == "publish-failed"
    assert row52["batch"]["steps"][0]["outcome"] == "publish-failed"

    # Part 2: Leader cancellation mid-gate requeues follower, which then leads a new batch
    # and reuses a passed `full` receipt after bisecting!
    remote2, state_file2, checkouts2, _ = _setup_remote_and_worktrees(
        tmp_path / "requeue-rebatch",
        [
            {"pr": 61, "base": "main", "branch": "feat/p61", "files": {"p61.txt": "1\n"}},
            {"pr": 62, "base": "main", "branch": "feat/p62", "files": {"p62.txt": "2\n"}},
            {"pr": 63, "base": "main", "branch": "feat/p63", "files": {"bad63.txt": "x\n"}},
        ],
    )
    bin_dir2 = _install_stateful_gh(tmp_path / "bin-rq", remote2, state_file2, tmp_path / "gh-rq.jsonl")
    gate_entered = tmp_path / "rq-entered"
    gate_release = tmp_path / "rq-release"
    gate_cmd = _python(
        """
import os, sys, time
from pathlib import Path
if os.environ.get("PAUSE_FIRST") == "1" and not Path(sys.argv[1]).exists():
    Path(sys.argv[1]).touch()
    while not Path(sys.argv[2]).exists():
        time.sleep(0.02)
if Path("bad63.txt").exists():
    sys.exit(4)
""",
        gate_entered,
        gate_release,
    )
    # First record a passed `full` receipt for PR #62 with identical gate command!
    full62 = client.submit(
        gate_cmd,
        kind="full",
        checkout=str(checkouts2[62]),
        resources={"jobs": 1},
        caller_pid=os.getpid(),
        environment=_env_with_gh(bin_dir2),
    )
    _wait_terminal(client, full62, "passed")

    hold_rq, release_rq = _submit_hold(client, tmp_path, checkouts2[61], "rq")
    r61 = client.submit_land(
        "github", 61, gate_cmd, checkout=str(checkouts2[61]),
        resources={"jobs": 1}, caller_pid=os.getpid(),
        environment=_env_with_gh(bin_dir2, {"PAUSE_FIRST": "1"}),
    )
    r62 = client.submit_land(
        "github", 62, gate_cmd, checkout=str(checkouts2[62]),
        resources={"jobs": 1}, caller_pid=os.getpid(),
        reuse_full=True,
        environment=_env_with_gh(bin_dir2),
    )
    release_rq.touch()
    _wait_terminal(client, hold_rq, "passed")
    wait_for(gate_entered.exists, "first batch gate did not start")

    # Queue PR #63 before cancelling r61 so requeued r62 batches with r63!
    r63 = client.submit_land(
        "github", 63, gate_cmd, checkout=str(checkouts2[63]),
        resources={"jobs": 1}, caller_pid=os.getpid(),
        environment=_env_with_gh(bin_dir2),
    )
    client.cancel(r61)
    gate_release.touch()

    row61 = _wait_terminal(client, r61, "cancelled")
    row62 = _wait_terminal(client, r62, "passed")
    row63 = _wait_terminal(client, r63, "failed")

    # Cancelled batch 1 retains r61 as cancelled and r62 as requeued.
    assert [m["status"] for m in row61["batch"]["members"]] == ["cancelled", "requeued"]
    assert row61["batch"]["steps"][0]["outcome"] == "cancelled"

    # Requeued r62 led batch 2 ([r62, r63]), bisected, reused full62 on singleton [r62], and failed [r63]!
    b2 = row62["batch"]
    assert b2["leader_run_id"] == r62
    assert b2["role"] == "leader"
    assert row63["batch"]["leader_run_id"] == r62
    assert row63["batch"]["role"] == "follower"
    assert [s["outcome"] for s in b2["steps"]] == ["bisected", "published", "gate-failed"]
    assert b2["steps"][1]["gate_reused_from"] == full62
