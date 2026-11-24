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
    entry = dict(data[number])
    if entry.get("state") == "OPEN" and entry.get("headRefName"):
        live_oid = rev_parse(f"refs/heads/{{entry['headRefName']}}")
        if live_oid:
            entry["headRefOid"] = live_oid
    print(json.dumps(entry))
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
        label=f"hold {suffix}",
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
    run_ids = {
        pr: client.submit_land(
            "github",
            pr,
            gate,
            checkout=str(checkouts[pr]),
            label=f"land #{pr} slice",
            resources={"jobs": 1},
            caller_pid=os.getpid(),
            environment=env,
        )
        for pr in (1, 2)
    }
    release.touch()
    _wait_terminal(client, hold_id, "passed")
    rows = {pr: _wait_terminal(client, run_ids[pr]) for pr in (1, 2)}
    gates = [json.loads(line) for line in gate_log.read_text().splitlines()]
    assert all(json.loads(line) is None for line in clear_log.read_text().splitlines())
    if bad_pr is None:
        expected_slices = [[1, 2]]
    elif bad_pr == 1:
        expected_slices = [[1, 2], [1], [2]]
    else:
        expected_slices = [[1, 2], [1]]
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
            {"pr": 4, "base": "main", "branch": "feat/four", "files": {"four.txt": "4\n"}},
            {"pr": 5, "base": "release-1", "branch": "feat/rel", "files": {"rel.txt": "r\n"}},
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
    for pr in (1, 2, 3, 4, 5):
        run_ids[pr] = client.submit_land(
            "github",
            pr,
            gate_cmd,
            checkout=str(checkouts[pr]),
            label=f"land #{pr} clean batch",
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
    assert set(main_gate["files"]) == {"base.txt", "one.txt", "two.txt", "three.txt", "four.txt"}
    assert set(rel_gate["files"]) == {"base.txt", "rel.txt"}

    # All 4 main-target PR rows share the batch gate log.
    log_texts = {pr: str(client.log(run_ids[pr], offset=0, limit=65536)["text"]) for pr in (1, 2, 3, 4)}
    assert all("one.txt" in text and "four.txt" in text for text in log_texts.values())

    # Structured batch history is exposed on leader and all followers, and null on standalone PR #5.
    main_final_sha = _git(remote, "rev-parse", "refs/heads/main")
    assert results[5]["batch"] is None
    expected_ids = [run_ids[1], run_ids[2], run_ids[3], run_ids[4]]
    for idx, pr in enumerate((1, 2, 3, 4)):
        batch = results[pr]["batch"]
        assert batch is not None
        assert batch["leader_run_id"] == run_ids[1]
        assert batch["role"] == ("leader" if pr == 1 else "follower")
        assert batch["target_branch"] == "main"
        assert batch["max_batch_size"] == 4
        assert [m["run_id"] for m in batch["members"]] == expected_ids
        assert [m["status"] for m in batch["members"]] == ["passed", "passed", "passed", "passed"]
        assert batch["members"][idx]["position"] == idx
        assert len(batch["steps"]) == 1
        step = batch["steps"][0]
        assert step["step"] == 1
        assert step["member_run_ids"] == expected_ids
        assert step["gated_member_run_ids"] == expected_ids
        assert step["dropped"] == []
        assert step["gating_head_sha"] == main_gate["head"]
        assert step["gate_exit_status"] == 0
        assert step["outcome"] == "published"
        assert step["published_sha"] == main_final_sha

    snap = client.snapshot()
    snap_by_id = {row["run_id"]: row for row in snap["recent"]}
    assert snap_by_id[run_ids[2]]["batch"] == results[2]["batch"]
    assert snap_by_id[run_ids[5]]["batch"] is None

    # Remote refs/heads/main has all 4 PR files; refs/heads/release-1 stays isolated.
    main_tree = _git(remote, "ls-tree", "--name-only", "refs/heads/main").splitlines()
    rel_tree = _git(remote, "ls-tree", "--name-only", "refs/heads/release-1").splitlines()
    assert set(main_tree) == {"base.txt", "one.txt", "two.txt", "three.txt", "four.txt"}
    assert set(rel_tree) == {"base.txt", "rel.txt"}

    gh_calls = [json.loads(line) for line in gh_log.read_text(encoding="utf-8").splitlines() if line]
    graphql_calls = [c for c in gh_calls if c["args"][:2] == ["api", "graphql"]]
    assert len(graphql_calls) == 2
    assert sorted(len(c["payload"]["variables"]["input"]["refUpdates"]) for c in graphql_calls) == [2, 5]


def test_object_store_merges_preserve_checkout_without_git_identity(tmp_path, monkeypatch):
    from agcoord import land

    remote, _state, checkouts, _prs = _setup_remote_and_worktrees(
        tmp_path,
        [
            {"pr": 1, "branch": "feat/one", "files": {"one.txt": "one\n"}},
            {"pr": 2, "branch": "feat/two", "files": {"two.txt": "two\n"}},
        ],
    )
    checkout = checkouts[1]
    for key in ("user.name", "user.email"):
        _git(checkout, "config", "--unset", key)
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", os.devnull)
    monkeypatch.setenv("GIT_CONFIG_SYSTEM", os.devnull)
    (checkout / "staged.txt").write_text("staged\n")
    _git(checkout, "add", "staged.txt")
    (checkout / "base.txt").write_text("unstaged\n")
    index = checkout / ".git" / "index"
    before = (_head(checkout), _git(checkout, "status", "--porcelain"), index.read_bytes())
    refs = ["refs/heads/main", "refs/heads/feat/one", "refs/heads/feat/two"]
    fetched = land._fetch_preclaim_heads(checkout, [*refs, "refs/heads/deleted"])
    assert set(fetched) == set(refs)
    base = fetched[refs[0]]
    first = land._merge_in_object_store(checkout, base, fetched[refs[1]])
    assert first is not None
    combined = land._merge_in_object_store(checkout, first, fetched[refs[2]])
    assert combined is not None
    assert _git(checkout, "show", f"{combined}:one.txt") == "one"
    assert _git(checkout, "show", f"{combined}:two.txt") == "two"
    assert land._merge_in_object_store(checkout, combined, fetched[refs[1]]) == combined
    assert (_head(checkout), _git(checkout, "status", "--porcelain"), index.read_bytes()) == before
    assert (checkout / "base.txt").read_text() == "unstaged\n"

    git_batch = land._git_batch

    def fail_one_fetch(path, *arguments):
        if arguments[0] == "fetch" and arguments[-1] == refs[1]:
            return subprocess.CompletedProcess(arguments, 1, "", "ref disappeared")
        return git_batch(path, *arguments)

    monkeypatch.setattr(land, "_git_batch", fail_one_fetch)
    assert set(land._fetch_preclaim_heads(checkout, refs)) == {refs[0], refs[2]}


@pytest.mark.parametrize("scenario", ["replacement", "fairness", "leader", "postclaim"])
def test_land_batch_preclaim_merge_screening(coordinator, tmp_path: Path, scenario):
    _broker, client = coordinator
    specs = [
        {"pr": 10, "branch": "feat/a", "files": {"a.txt": "a\n"}},
        {"pr": 11, "branch": "feat/first", "files": {"shared.txt": "first\n"}},
        {"pr": 12, "branch": "feat/second", "files": {"shared.txt": "second\n"}},
        {"pr": 13, "branch": "feat/b", "files": {"b.txt": "b\n"}},
        {"pr": 14, "branch": "feat/c", "files": {"c.txt": "c\n"}},
    ]
    if scenario == "leader":
        specs = specs[:3]
        specs[2]["files"] = {"other.txt": "other\n"}
    elif scenario == "fairness":
        specs = specs[:4]
    elif scenario == "postclaim":
        specs = specs[:2]
    remote, state_file, checkouts, _ = _setup_remote_and_worktrees(tmp_path, specs)
    bin_dir = _install_stateful_gh(tmp_path, remote, state_file, tmp_path / "gh.jsonl")
    gate_log = tmp_path / "gates.jsonl"
    gate_cmd = _python(
        r"""
import json, os, sys
from pathlib import Path
files = sorted(p.name for p in Path.cwd().iterdir() if p.name != ".git")
with Path(os.environ["BATCH_GATE_LOG"]).open("a") as stream:
    stream.write(json.dumps(files) + "\n")
if os.environ["SCENARIO"] == "fairness" and Path("shared.txt").exists():
    sys.exit(1 if Path("shared.txt").read_text() == "first\n" else 0)
"""
    )
    extra = {"BATCH_GATE_LOG": str(gate_log), "SCENARIO": scenario}
    if scenario in {"leader", "postclaim"}:
        seed = tmp_path / "seed"
        name = "a.txt" if scenario == "leader" else "shared.txt"
        (seed / name).write_text("target conflict\n")
        _git(seed, "add", name)
        _git(seed, "commit", "-m", "concurrent target change")
        _git(seed, "push", "origin", "HEAD:refs/heads/advance")
        advance = _head(seed)
        if scenario == "postclaim":
            hook_dir = tmp_path / "claim-hook"
            hook_dir.mkdir()
            (hook_dir / "sitecustomize.py").write_text(
                "import subprocess\n"
                "from agcoord.queue import CoordinatorClient\n"
                "_claim = CoordinatorClient.claim_batch\n"
                "def claim(self, *args, **kwargs):\n"
                "    result = _claim(self, *args, **kwargs)\n"
                f"    subprocess.run([{GIT!r}, '--git-dir', {str(remote)!r}, "
                f"'update-ref', 'refs/heads/main', {advance!r}], check=True)\n"
                "    return result\n"
                "CoordinatorClient.claim_batch = claim\n"
            )
            extra["PYTHONPATH"] = str(hook_dir) + os.pathsep + str(Path(__file__).resolve().parents[1] / "src")
    hold_id, release = _submit_hold(client, tmp_path, checkouts[10], scenario)
    env = _env_with_gh(bin_dir, extra)
    run_ids = {
        pr: client.submit_land(
            "github", pr, gate_cmd, checkout=str(checkouts[pr]),
            label=f"land #{pr} merge screening", resources={"jobs": 1},
            caller_pid=os.getpid(), environment=env,
        )
        for pr in checkouts
    }
    if scenario == "leader":
        _git(remote, "update-ref", "refs/heads/main", advance)
    release.touch()
    _wait_terminal(client, hold_id, "passed")
    failed = {"replacement": 12, "fairness": 11, "leader": 10, "postclaim": 11}[scenario]
    rows = {
        pr: _wait_terminal(client, run_id, "failed" if pr == failed else "passed")
        for pr, run_id in run_ids.items()
    }
    bad = rows[failed]
    assert bad["failure_reason"] == ("gate-failed" if scenario == "fairness" else "merge-error")
    assert bad["exit_status"] == (1 if scenario == "fairness" else 79)
    if scenario == "replacement":
        assert bad["batch"] is None
        batch = rows[10]["batch"]
        assert [m["run_id"] for m in batch["members"]] == [run_ids[p] for p in (10, 11, 13, 14)]
        assert len(batch["steps"]) == 1
        assert batch["steps"][0]["outcome"] == "published"
        assert batch["steps"][0]["gated_member_run_ids"] == [run_ids[p] for p in (10, 11, 13, 14)]
        assert [json.loads(line) for line in gate_log.read_text().splitlines()] == [
            ["a.txt", "b.txt", "base.txt", "c.txt", "shared.txt"]
        ]
        assert _git(remote, "show", "refs/heads/main:shared.txt") == "first"
    elif scenario == "fairness":
        assert [m["run_id"] for m in rows[10]["batch"]["members"]] == [run_ids[10], run_ids[11]]
        assert rows[12]["batch"]["leader_run_id"] != rows[10]["batch"]["leader_run_id"]
        assert _git(remote, "show", "refs/heads/main:shared.txt") == "second"
    elif scenario == "leader":
        assert bad["batch"] is None
        assert [m["run_id"] for m in rows[11]["batch"]["members"]] == [run_ids[11], run_ids[12]]
    else:
        batch = bad["batch"]
        assert batch is not None
        step = batch["steps"][0]
        assert step["dropped"] == [{"run_id": run_ids[11], "reason": "merge-conflict"}]
        assert step["member_run_ids"] == [run_ids[10], run_ids[11]]
        assert step["gated_member_run_ids"] == [run_ids[10]]
        assert batch["members"][1]["failure_reason"] == "merge-error"


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
            label=f"land #{pr} order bisect",
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
    # Deductive binary split sequence: [21,22,23,24] fail -> [21,22] pass & publish ->
    # [23,24] deduced-bisected (same tree as step 1!) -> [23] fail -> [24] pass & publish = 4 gate invocations.
    assert [entry["ok"] for entry in gated_runs] == [False, True, False, True]

    # Early-finalized follower #22 (finalized at Step 2) exposes ALL 5 steps of the batch
    # in status, snapshot, agc --json show, and plain agc show!
    final_rows = {pr: client.status(run_ids[pr]) for pr in (21, 22, 23, 24)}
    expected_outcomes = ["bisected", "published", "deduced-bisected", "gate-failed", "published"]
    for pr in (21, 22, 23, 24):
        batch = final_rows[pr]["batch"]
        assert batch is not None
        assert batch["role"] == ("leader" if pr == 21 else "follower")
        assert batch["max_batch_size"] == 4
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
    assert "Max batch (B): 4" in shown_plain.stdout
    assert "gate failed (exit 5) -> bisected 4 into 2 + 2" in shown_plain.stdout
    assert "deduced failing slice -> bisected 2 into 1 + 1" in shown_plain.stdout
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
        label="land #31 head-changed survivor",
        resources={"jobs": 1},
        caller_pid=os.getpid(),
        environment=env,
    )
    r32 = client.submit_land(
        "github",
        32,
        gate_cmd,
        checkout=str(checkouts[32]),
        label="land #32 moving head",
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
        label="land #41 stale main",
        resources={"jobs": 1},
        caller_pid=os.getpid(),
        environment=env_b,
    )
    r42 = client.submit_land(
        "github",
        42,
        gate_cmd_b,
        checkout=str(checkouts_b[42]),
        label="land #42 stale main",
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
        label="land #51 publish failure",
        resources={"jobs": 1}, caller_pid=os.getpid(), environment=env_pf,
    )
    r52 = client.submit_land(
        "github", 52, ["true"], checkout=str(checkouts[52]),
        label="land #52 publish failure",
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
        label="PR 62 full receipt",
        resources={"jobs": 1},
        caller_pid=os.getpid(),
        environment=_env_with_gh(bin_dir2),
    )
    _wait_terminal(client, full62, "passed")

    hold_rq, release_rq = _submit_hold(client, tmp_path, checkouts2[61], "rq")
    r61 = client.submit_land(
        "github", 61, gate_cmd, checkout=str(checkouts2[61]),
        label="land #61 cancelled leader",
        resources={"jobs": 1}, caller_pid=os.getpid(),
        environment=_env_with_gh(bin_dir2, {"PAUSE_FIRST": "1"}),
    )
    r62 = client.submit_land(
        "github", 62, gate_cmd, checkout=str(checkouts2[62]),
        label="land #62 requeued follower",
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
        label="land #63 failing follower",
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
    assert [s["outcome"] for s in b2["steps"]] == [
        "bisected",
        "published",
        "deduced-gate-failed",
    ]
    assert b2["steps"][1]["gate_reused_from"] == full62


def test_land_batch_deductive_bisect_invalidation_opt_out_and_adaptive_power_of_two_sizing(
    coordinator,
    tmp_path: Path,
):
    _broker, client = coordinator

    # 1. Pair run-count verification across (pass, fail), (fail, pass), (fail, fail), and --no-deductive-bisect.
    def run_pair_case(name: str, bad_prs: tuple[int, ...], *, deductive: bool = True) -> tuple[int, dict[int, dict]]:
        case_dir = tmp_path / name
        remote, state_file, checkouts, _ = _setup_remote_and_worktrees(
            case_dir,
            [
                {"pr": 1, "base": "main", "branch": "feat/p1", "files": {"p1.txt": "bad\n" if 1 in bad_prs else "ok\n"}},
                {"pr": 2, "base": "main", "branch": "feat/p2", "files": {"p2.txt": "bad\n" if 2 in bad_prs else "ok\n"}},
            ],
        )
        bin_dir = _install_stateful_gh(case_dir / "bin", remote, state_file, case_dir / "gh.jsonl")
        gate_log = case_dir / "gates.jsonl"
        gate_cmd = _python(
            """
import json, os, sys
from pathlib import Path
bad = any(
    Path(f).exists() and Path(f).read_text(encoding="utf-8").strip() == "bad"
    for f in ("p1.txt", "p2.txt")
)
with Path(os.environ["BATCH_GATE_LOG"]).open("a", encoding="utf-8") as s:
    s.write(json.dumps({"bad": bad}) + "\\n")
if bad:
    sys.exit(3)
""",
        )
        hold_id, release = _submit_hold(client, case_dir, checkouts[1], name)
        env = _env_with_gh(bin_dir, {"BATCH_GATE_LOG": str(gate_log)})
        ids = {
            pr: client.submit_land(
                "github",
                pr,
                gate_cmd,
                checkout=str(checkouts[pr]),
                label=f"land #{pr} {name}",
                resources={"jobs": 1},
                caller_pid=os.getpid(),
                deductive_bisect=deductive,
                environment=env,
            )
            for pr in (1, 2)
        }
        release.touch()
        _wait_terminal(client, hold_id, "passed")
        rows = {
            pr: _wait_terminal(client, ids[pr], "failed" if pr in bad_prs else "passed")
            for pr in (1, 2)
        }
        count = len(gate_log.read_text(encoding="utf-8").splitlines())
        return count, rows

    pf_runs, pf_rows = run_pair_case("pair-pass-fail", (2,))
    assert pf_runs == 2
    assert [s["outcome"] for s in pf_rows[2]["batch"]["steps"]] == [
        "bisected",
        "published",
        "deduced-gate-failed",
    ]

    pf_optout_runs, pf_optout_rows = run_pair_case("pair-opt-out", (2,), deductive=False)
    assert pf_optout_runs == 3
    assert [s["outcome"] for s in pf_optout_rows[2]["batch"]["steps"]] == [
        "bisected",
        "published",
        "gate-failed",
    ]

    fp_runs, _ = run_pair_case("pair-fail-pass", (1,))
    assert fp_runs == 3
    ff_runs, _ = run_pair_case("pair-fail-fail", (1, 2))
    assert ff_runs == 3

    # 2. Member-drop invalidation of known_bad: [71, 72, 73, 74] where #73 is bad;
    # while [71, 72] gates and publishes, #73's PR head changes so [73, 74] drops #73 as head-changed.
    # Remaining singleton #74 MUST NOT be deduced gate-failed — it runs its gate and passes!
    inv_dir = tmp_path / "drop-invalidates-known-bad"
    remote_i, state_file_i, checkouts_i, pr_state_i = _setup_remote_and_worktrees(
        inv_dir,
        [
            {"pr": 71, "base": "main", "branch": "feat/p71", "files": {"p71.txt": "ok\n"}},
            {"pr": 72, "base": "main", "branch": "feat/p72", "files": {"p72.txt": "ok\n"}},
            {"pr": 73, "base": "main", "branch": "feat/p73", "files": {"bad73.txt": "boom\n"}},
            {"pr": 74, "base": "main", "branch": "feat/p74", "files": {"p74.txt": "ok\n"}},
        ],
    )
    bin_i = _install_stateful_gh(inv_dir / "bin", remote_i, state_file_i, inv_dir / "gh.jsonl")
    left_gate_hit = inv_dir / "left-gate-hit"
    left_gate_go = inv_dir / "left-gate-go"
    inv_gate = _python(
        """
import sys, time
from pathlib import Path
if Path("bad73.txt").exists():
    sys.exit(6)
if Path("p71.txt").exists() and not Path(sys.argv[1]).exists():
    Path(sys.argv[1]).touch()
    while not Path(sys.argv[2]).exists():
        time.sleep(0.02)
""",
        left_gate_hit,
        left_gate_go,
    )
    hold_i, release_i = _submit_hold(client, inv_dir, checkouts_i[71], "inv")
    env_i = _env_with_gh(bin_i)
    ids_i = {
        pr: client.submit_land(
            "github", pr, inv_gate, checkout=str(checkouts_i[pr]),
            label=f"land #{pr} drop invalidation",
            resources={"jobs": 1}, caller_pid=os.getpid(), environment=env_i,
        )
        for pr in (71, 72, 73, 74)
    }
    release_i.touch()
    _wait_terminal(client, hold_i, "passed")
    wait_for(left_gate_hit.exists, "left slice [71, 72] did not enter gate")
    # Advance PR #73 on remote while [71, 72] is gating so #73 drops as head-changed in right slice!
    (checkouts_i[73] / "extra.txt").write_text("moved\n", encoding="utf-8")
    _git(checkouts_i[73], "add", "extra.txt")
    _git(checkouts_i[73], "commit", "-m", "move 73")
    _git(checkouts_i[73], "push", "origin", "feat/p73")
    pr_state_i["73"]["headRefOid"] = _git(checkouts_i[73], "rev-parse", "HEAD")
    state_file_i.write_text(json.dumps(pr_state_i), encoding="utf-8")
    left_gate_go.touch()

    _wait_terminal(client, ids_i[71], "passed")
    _wait_terminal(client, ids_i[72], "passed")
    row73 = _wait_terminal(client, ids_i[73], "failed")
    row74 = _wait_terminal(client, ids_i[74], "passed")
    assert row73["failure_reason"] == "head-changed"
    assert row74["status"] == "passed"

    # 3. N = 6 multi-failure vector at cold-start B = 4 -> adaptive B = 2 -> adaptive B = 1:
    # 6 queued PRs (#81..#86) where #82 and #86 fail -> chunks into batch of 4 (B=4) + batch of 2 (B=2)!
    # With deductive skipping: [81..84] (root fail -> [81,82] fail -> [81] pass -> [82] deduced!
    # -> [83,84] pass = 4 gates) + [85,86] (root fail -> [85] pass -> [86] deduced! = 2 gates) = 6 gates total!
    six_dir = tmp_path / "six-vector"
    remote_6, state_file_6, checkouts_6, _ = _setup_remote_and_worktrees(
        six_dir,
        [
            {
                "pr": pr,
                "base": "main",
                "branch": f"feat/p{pr}",
                "files": {
                    f"p{pr}.txt": "bad\n" if pr in {82, 86, 87, 88} else "ok\n"
                },
            }
            for pr in range(81, 91)
        ],
    )
    bin_6 = _install_stateful_gh(six_dir / "bin", remote_6, state_file_6, six_dir / "gh.jsonl")
    six_gate_log = six_dir / "gates.jsonl"
    six_gate = _python(
        """
import json, os, sys
from pathlib import Path
bad = any(
    Path(f"p{pr}.txt").exists() and Path(f"p{pr}.txt").read_text(encoding="utf-8").strip() == "bad"
    for pr in range(81, 91)
)
with Path(os.environ["BATCH_GATE_LOG"]).open("a", encoding="utf-8") as s:
    s.write(json.dumps({"bad": bad}) + "\\n")
if bad:
    sys.exit(4)
""",
    )
    hold_6, release_6 = _submit_hold(client, six_dir, checkouts_6[81], "six")
    env_6 = _env_with_gh(bin_6, {"BATCH_GATE_LOG": str(six_gate_log)})
    ids_6 = {
        pr: client.submit_land(
            "github", pr, six_gate, checkout=str(checkouts_6[pr]),
            label=f"land #{pr} adaptive batch",
            resources={"jobs": 1}, caller_pid=os.getpid(), environment=env_6,
        )
        for pr in range(81, 87)
    }
    release_6.touch()
    _wait_terminal(client, hold_6, "passed")
    rows_6 = {
        pr: _wait_terminal(client, ids_6[pr], "failed" if pr in {82, 86} else "passed")
        for pr in range(81, 87)
    }
    assert len(rows_6[81]["batch"]["members"]) == 4
    assert rows_6[81]["batch"]["max_batch_size"] == 4
    assert len(rows_6[85]["batch"]["members"]) == 2
    assert rows_6[85]["batch"]["max_batch_size"] == 2
    assert len(six_gate_log.read_text(encoding="utf-8").splitlines()) == 6

    # Submit two more failing PRs (#87, #88) so repository history reaches 4 passed + 4 failed
    # (p_hat = 0.50 >= 0.38), clamping adaptive B = 1 so #89 and #90 execute sequentially unbatched!
    for pr in (87, 88):
        rid = client.submit_land(
            "github", pr, six_gate, checkout=str(checkouts_6[pr]),
            label=f"land #{pr} adaptive fail",
            resources={"jobs": 1}, caller_pid=os.getpid(), environment=env_6,
        )
        _wait_terminal(client, rid, "failed")

    hold_b1, release_b1 = _submit_hold(client, six_dir, checkouts_6[89], "b1")
    r89 = client.submit_land(
        "github", 89, six_gate, checkout=str(checkouts_6[89]),
        label="land #89 sequential",
        resources={"jobs": 1}, caller_pid=os.getpid(), environment=env_6,
    )
    r90 = client.submit_land(
        "github", 90, six_gate, checkout=str(checkouts_6[90]),
        label="land #90 sequential",
        resources={"jobs": 1}, caller_pid=os.getpid(), environment=env_6,
    )
    release_b1.touch()
    _wait_terminal(client, hold_b1, "passed")
    row89 = _wait_terminal(client, r89, "passed")
    row90 = _wait_terminal(client, r90, "passed")
    assert row89["batch"] is None
    assert row90["batch"] is None


def test_land_batch_failure_attribution_evicts_culprits_and_falls_back_safely(
    coordinator,
    tmp_path: Path,
):
    from io import StringIO
    from agcoord.land import (
        ATTRIBUTION_FORMAT,
        BATCH_ATTRIBUTION_FILE_ENV,
        _load_failure_attribution,
    )

    _broker, client = coordinator

    # 1. Receipt validator refuses missing, empty, malformed, traversal, absolute, and symlinked receipts/paths.
    fake_checkout = tmp_path / "validator-checkout"
    (fake_checkout / "src").mkdir(parents=True)
    (fake_checkout / "src" / "real.py").write_text("x = 1\n", encoding="utf-8")
    (fake_checkout / "src" / "link.py").symlink_to(fake_checkout / "src" / "real.py")

    err_buf = StringIO()
    missing_path = tmp_path / "missing-attr.json"
    assert _load_failure_attribution(missing_path, checkout=fake_checkout, err=err_buf) is None

    empty_path = tmp_path / "empty-attr.json"
    empty_path.write_text("", encoding="utf-8")
    assert _load_failure_attribution(empty_path, checkout=fake_checkout, err=err_buf) is None

    symlink_receipt = tmp_path / "symlink-attr.json"
    real_receipt = tmp_path / "real-attr.json"
    real_receipt.write_text(
        json.dumps(
            {
                "format": ATTRIBUTION_FORMAT,
                "failures": [{"unit": "tests/test_x.py", "causal_paths": ["src/real.py"]}],
            }
        ),
        encoding="utf-8",
    )
    symlink_receipt.symlink_to(real_receipt)
    assert _load_failure_attribution(symlink_receipt, checkout=fake_checkout, err=err_buf) is None
    assert _load_failure_attribution(real_receipt, checkout=fake_checkout, err=err_buf) == [
        ("tests/test_x.py", ("src/real.py",))
    ]

    for bad_payload in (
        "{not json",
        json.dumps({"format": "wrong-v1", "failures": [{"unit": "u", "causal_paths": ["src/real.py"]}]}),
        json.dumps({"format": ATTRIBUTION_FORMAT, "extra": 1, "failures": [{"unit": "u", "causal_paths": ["src/real.py"]}]}),
        json.dumps({"format": ATTRIBUTION_FORMAT, "failures": [{"unit": "", "causal_paths": ["src/real.py"]}]}),
        json.dumps({"format": ATTRIBUTION_FORMAT, "failures": [{"unit": "u", "causal_paths": ["../escape.py"]}]}),
        json.dumps({"format": ATTRIBUTION_FORMAT, "failures": [{"unit": "u", "causal_paths": ["./src/real.py"]}]}),
        json.dumps({"format": ATTRIBUTION_FORMAT, "failures": [{"unit": "u", "causal_paths": ["/abs/path.py"]}]}),
        json.dumps({"format": ATTRIBUTION_FORMAT, "failures": [{"unit": "u", "causal_paths": ["src/trailing/"]}]}),
        json.dumps({"format": ATTRIBUTION_FORMAT, "failures": [{"unit": "u", "causal_paths": ["src\\win.py"]}]}),
        json.dumps({"format": ATTRIBUTION_FORMAT, "failures": [{"unit": "u", "causal_paths": ["src/link.py"]}]}),
    ):
        bad_file = tmp_path / "bad-attr.json"
        bad_file.write_text(bad_payload, encoding="utf-8")
        assert _load_failure_attribution(bad_file, checkout=fake_checkout, err=err_buf) is None

    # 2. Disjoint 2-PR failure cases: (fail, pass), (pass, fail), and (fail, fail) ALL finish in 2 gate runs!
    def run_attributed_pair(name: str, bad_prs: tuple[int, ...]) -> tuple[list[dict], dict[int, dict]]:
        case_dir = tmp_path / name
        remote, state_file, checkouts, _ = _setup_remote_and_worktrees(
            case_dir,
            [
                {
                    "pr": 1,
                    "base": "main",
                    "branch": "feat/billing",
                    "files": {"src/billing/invoice.py": "bad\n" if 1 in bad_prs else "ok\n"},
                },
                {
                    "pr": 2,
                    "base": "main",
                    "branch": "feat/auth",
                    "files": {"src/auth/token.py": "bad\n" if 2 in bad_prs else "ok\n"},
                },
            ],
        )
        bin_dir = _install_stateful_gh(case_dir / "bin", remote, state_file, case_dir / "gh.jsonl")
        gate_log = case_dir / "gates.jsonl"
        gate_cmd = _python(
            """
import json, os, sys
from pathlib import Path

cwd = Path.cwd().resolve()
attr_env = os.environ.get("AGCOORD_BATCH_ATTRIBUTION_FILE")
failures = []
for unit, rel_file, causal in (
    ("tests/test_billing.py", "src/billing/invoice.py", ["src/billing"]),
    ("tests/test_auth.py", "src/auth/token.py", ["src/auth/token.py"]),
):
    p = cwd / rel_file
    if p.exists() and p.read_text(encoding="utf-8").strip() == "bad":
        failures.append({"unit": unit, "causal_paths": causal})

with Path(os.environ["BATCH_GATE_LOG"]).open("a", encoding="utf-8") as s:
    s.write(json.dumps({"attr_env": attr_env, "cwd": str(cwd), "failures": [f["unit"] for f in failures]}) + "\\n")

if failures:
    if attr_env:
        Path(attr_env).write_text(
            json.dumps({"format": "agcoord-failure-attribution-v1", "failures": failures}),
            encoding="utf-8",
        )
    sys.exit(5)
""",
        )
        hold_id, release = _submit_hold(client, case_dir, checkouts[1], name)
        env = _env_with_gh(bin_dir, {"BATCH_GATE_LOG": str(gate_log)})
        ids = {
            pr: client.submit_land(
                "github", pr, gate_cmd, checkout=str(checkouts[pr]),
                label=f"land #{pr} {name}",
                resources={"jobs": 1}, caller_pid=os.getpid(), environment=env,
            )
            for pr in (1, 2)
        }
        release.touch()
        _wait_terminal(client, hold_id, "passed")
        rows = {
            pr: _wait_terminal(client, ids[pr], "failed" if pr in bad_prs else "passed")
            for pr in (1, 2)
        }
        records = [json.loads(line) for line in gate_log.read_text(encoding="utf-8").splitlines()]
        return records, rows

    for label, bad_set in (
        ("attr-fail-pass", (1,)),
        ("attr-pass-fail", (2,)),
        ("attr-fail-fail", (1, 2)),
    ):
        records, rows = run_attributed_pair(label, bad_set)
        assert len(records) == 2, (label, records)
        first_attr_path = Path(records[0]["attr_env"])
        assert not first_attr_path.is_relative_to(Path(records[0]["cwd"]))
        assert not first_attr_path.exists()
        assert records[1]["attr_env"] is None
        assert rows[1]["batch"]["steps"][0]["outcome"] == "attributed-gate-failed"
        evicted_pr = bad_set[0]
        expected_unit = (
            "tests/test_billing.py" if evicted_pr == 1 else "tests/test_auth.py"
        )
        assert rows[1]["batch"]["steps"][0]["attributed"] == [
            {"run_id": rows[evicted_pr]["run_id"], "units": [expected_unit]}
        ]
        if label == "attr-fail-pass":
            shown = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "agcoord",
                    "show",
                    rows[1]["run_id"],
                    "--state-dir",
                    str(client.paths.state_dir),
                ],
                capture_output=True,
                text=True,
                env=caller_environment(),
                check=True,
            )
            assert "attributed-gate-failed -> evicted #1" in shown.stdout
            assert "tests/test_billing.py" in shown.stdout

    # 3. Multi-culprit eviction in one run (4 PRs, 2 culprits evicted simultaneously -> 2 gate runs total!).
    multi_dir = tmp_path / "attr-multi"
    remote_m, state_file_m, checkouts_m, _ = _setup_remote_and_worktrees(
        multi_dir,
        [
            {"pr": 1, "base": "main", "branch": "feat/m1", "files": {"pkg/a/mod.py": "bad\n"}},
            {"pr": 2, "base": "main", "branch": "feat/m2", "files": {"pkg/b/mod.py": "ok\n"}},
            {"pr": 3, "base": "main", "branch": "feat/m3", "files": {"pkg/c/mod.py": "bad\n"}},
            {"pr": 4, "base": "main", "branch": "feat/m4", "files": {"pkg/d/mod.py": "ok\n"}},
        ],
    )
    bin_m = _install_stateful_gh(multi_dir / "bin", remote_m, state_file_m, multi_dir / "gh.jsonl")
    multi_log = multi_dir / "gates.jsonl"
    multi_gate = _python(
        """
import json, os, sys
from pathlib import Path

failures = []
for name in ("a", "b", "c", "d"):
    p = Path(f"pkg/{name}/mod.py")
    if p.exists() and p.read_text(encoding="utf-8").strip() == "bad":
        failures.append({"unit": f"tests/test_{name}.py", "causal_paths": [f"pkg/{name}"]})
with Path(os.environ["BATCH_GATE_LOG"]).open("a", encoding="utf-8") as s:
    s.write(json.dumps({"failures": [f["unit"] for f in failures]}) + "\\n")
if failures:
    attr = os.environ.get("AGCOORD_BATCH_ATTRIBUTION_FILE")
    if attr:
        Path(attr).write_text(
            json.dumps({"format": "agcoord-failure-attribution-v1", "failures": failures}),
            encoding="utf-8",
        )
    sys.exit(4)
""",
    )
    hold_m, release_m = _submit_hold(client, multi_dir, checkouts_m[1], "multi")
    env_m = _env_with_gh(bin_m, {"BATCH_GATE_LOG": str(multi_log)})
    ids_m = {
        pr: client.submit_land(
            "github", pr, multi_gate, checkout=str(checkouts_m[pr]),
            label=f"land #{pr} multi attribution",
            resources={"jobs": 1}, caller_pid=os.getpid(), environment=env_m,
        )
        for pr in (1, 2, 3, 4)
    }
    release_m.touch()
    _wait_terminal(client, hold_m, "passed")
    rows_m = {
        pr: _wait_terminal(client, ids_m[pr], "failed" if pr in {1, 3} else "passed")
        for pr in (1, 2, 3, 4)
    }
    assert len(multi_log.read_text(encoding="utf-8").splitlines()) == 2
    assert [s["outcome"] for s in rows_m[2]["batch"]["steps"]] == [
        "attributed-gate-failed",
        "published",
    ]
    assert rows_m[2]["batch"]["steps"][0]["attributed"] == [
        {"run_id": ids_m[1], "units": ["tests/test_a.py"]},
        {"run_id": ids_m[3], "units": ["tests/test_c.py"]},
    ]

    # 4. Shared-directory ambiguous receipt (both PRs touch src/shared/*) + malformed receipt fallback:
    # zero false evictions, falls back cleanly to standard bisection (3 gate runs for (fail, pass)).
    ambig_dir = tmp_path / "attr-ambig"
    remote_a, state_file_a, checkouts_a, _ = _setup_remote_and_worktrees(
        ambig_dir,
        [
            {"pr": 1, "base": "main", "branch": "feat/s1", "files": {"src/shared/one.py": "bad\n"}},
            {"pr": 2, "base": "main", "branch": "feat/s2", "files": {"src/shared/two.py": "ok\n"}},
        ],
    )
    bin_a = _install_stateful_gh(ambig_dir / "bin", remote_a, state_file_a, ambig_dir / "gh.jsonl")
    ambig_log = ambig_dir / "gates.jsonl"
    ambig_gate = _python(
        """
import json, os, sys
from pathlib import Path

bad = Path("src/shared/one.py").exists()
with Path(os.environ["BATCH_GATE_LOG"]).open("a", encoding="utf-8") as s:
    s.write(json.dumps({"bad": bad}) + "\\n")
if bad:
    attr = os.environ.get("AGCOORD_BATCH_ATTRIBUTION_FILE")
    if attr:
        Path(attr).write_text(
            json.dumps({
                "format": "agcoord-failure-attribution-v1",
                "failures": [{"unit": "tests/test_shared.py", "causal_paths": ["src/shared"]}],
            }),
            encoding="utf-8",
        )
    sys.exit(6)
""",
    )
    hold_a, release_a = _submit_hold(client, ambig_dir, checkouts_a[1], "ambig")
    env_a = _env_with_gh(bin_a, {"BATCH_GATE_LOG": str(ambig_log)})
    ra1 = client.submit_land(
        "github", 1, ambig_gate, checkout=str(checkouts_a[1]),
        label="land #1 ambiguous attribution",
        resources={"jobs": 1}, caller_pid=os.getpid(), environment=env_a,
    )
    ra2 = client.submit_land(
        "github", 2, ambig_gate, checkout=str(checkouts_a[2]),
        label="land #2 ambiguous attribution",
        resources={"jobs": 1}, caller_pid=os.getpid(), environment=env_a,
    )
    release_a.touch()
    _wait_terminal(client, hold_a, "passed")
    row_a1 = _wait_terminal(client, ra1, "failed")
    row_a2 = _wait_terminal(client, ra2, "passed")
    assert len(ambig_log.read_text(encoding="utf-8").splitlines()) == 3
    assert [s["outcome"] for s in row_a2["batch"]["steps"]] == [
        "bisected",
        "gate-failed",
        "published",
    ]
    assert row_a1["failure_reason"] == "gate-failed"
