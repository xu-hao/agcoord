"""Executable validation of the native host service and AppArmor policy artifacts."""

from __future__ import annotations

from collections import deque
from pathlib import Path
import re
import shutil
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[1]
UNIT = ROOT / "packaging/systemd/agcoord-broker.service"
PROFILE = ROOT / "packaging/apparmor/usr.libexec.agcoord.agcoord-broker"
CI_WORKFLOW = ROOT / ".github/workflows/ci.yml"
WORKER = ROOT / "native/agcoord-broker/src/worker.rs"
PROBE = ROOT / "scripts/test-native-host-enforcement"


def test_native_user_service_passes_systemd_verification(tmp_path: Path):
    analyzer = shutil.which("systemd-analyze")
    if analyzer is None:
        pytest.skip("systemd-analyze is unavailable")
    root = tmp_path / "root"
    # The parser uses the same unit grammar for system and user services. A staged system search
    # path lets it also resolve the fixed executable without installing anything on the host.
    staged_unit = root / "usr/lib/systemd/system/agcoord-broker.service"
    staged_binary = root / "usr/libexec/agcoord/agcoord-broker"
    staged_unit.parent.mkdir(parents=True)
    staged_binary.parent.mkdir(parents=True)
    shutil.copyfile(UNIT, staged_unit)
    staged_binary.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    staged_binary.chmod(0o755)
    for target in ("sysinit.target", "basic.target", "shutdown.target"):
        (staged_unit.parent / target).write_text(
            "[Unit]\nDefaultDependencies=no\n",
            encoding="utf-8",
        )

    completed = subprocess.run(
        [
            analyzer,
            f"--root={root}",
            "verify",
            "agcoord-broker.service",
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    if "SO_PASSCRED failed" in completed.stderr:
        pytest.skip("the test sandbox forbids systemd-analyze's credential socket")
    assert completed.returncode == 0, completed.stderr
    assert "Unknown key" not in completed.stderr


def test_native_apparmor_policy_compiles_with_all_domains():
    parser = shutil.which("apparmor_parser") or shutil.which("apparmor_parser", path="/usr/sbin:/sbin")
    if parser is None:
        pytest.skip("apparmor_parser is unavailable")
    completed = subprocess.run(
        [parser, "--skip-kernel-load", "--skip-cache", "--names", str(PROFILE)],
        text=True,
        capture_output=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    assert set(completed.stdout.splitlines()) == {
        "agcoord-admitted",
        "agcoord-broker",
        "agcoord-broker-client",
        "agcoord-bwrap",
        "agcoord-bwrap-child",
        "agcoord-job",
        "agcoord-gate-git",
    }


def test_native_apparmor_policy_compiles_to_binary_without_loading(tmp_path: Path):
    parser = shutil.which("apparmor_parser") or shutil.which("apparmor_parser", path="/usr/sbin:/sbin")
    if parser is None:
        pytest.skip("apparmor_parser is unavailable")
    binary = tmp_path / "policy.bin"
    completed = subprocess.run(
        [parser, "--skip-kernel-load", "--skip-cache", "--ofile", str(binary), str(PROFILE)],
        text=True,
        capture_output=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    assert binary.stat().st_size > 0


def test_native_apparmor_policy_compiles_all_domains_in_enforce_mode():
    parser = shutil.which("apparmor_parser") or shutil.which("apparmor_parser", path="/usr/sbin:/sbin")
    if parser is None:
        pytest.skip("apparmor_parser is unavailable")
    completed = subprocess.run(
        [parser, "--skip-kernel-load", "--skip-cache", "--debug", str(PROFILE)],
        text=True,
        capture_output=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    diagnostics = completed.stdout + completed.stderr
    assert diagnostics.count("Mode: enforce") == 7, diagnostics
    assert "Mode: default_allow" not in diagnostics


def _aare_path_pattern(pattern: str) -> re.Pattern[str]:
    """Match the path-glob operators used by the shipped exec rules.

    AppArmor's single star and question mark cannot cross a slash; a double star can.
    Character classes and brace alternatives are kept as separate regex constructs.
    """
    parts: list[str] = []
    index = 0
    while index < len(pattern):
        char = pattern[index]
        if pattern[index : index + 2] == "**":
            parts.append(r"[\s\S]*")
            index += 2
        elif char == "*":
            parts.append(r"[^/]*")
            index += 1
        elif char == "?":
            parts.append(r"[^/]")
            index += 1
        elif char == "[":
            end = pattern.index("]", index)
            parts.append(pattern[index : end + 1])
            index = end + 1
        elif char in "{},":
            parts.append({"{": "(?:", "}": ")", ",": "|"}[char])
            index += 1
        else:
            parts.append(re.escape(char))
            index += 1
    return re.compile("".join(parts))


def _assert_complete_exec_partition(rules):
    """Explore every distinct state of the rules' glob automata, including unbounded tails."""
    def expand(pattern):
        if "{" not in pattern:
            return [pattern]
        start = pattern.index("{")
        end = pattern.index("}", start)
        return [
            expanded
            for alternative in pattern[start + 1 : end].split(",")
            for expanded in expand(pattern[:start] + alternative + pattern[end + 1 :])
        ]

    def tokenize(pattern):
        tokens = []
        index = 0
        while index < len(pattern):
            char = pattern[index]
            if pattern[index : index + 2] == "**":
                tokens.append(("tree", ""))
                index += 2
            elif char == "*":
                tokens.append(("star", ""))
                index += 1
            elif char == "?":
                tokens.append(("any", ""))
                index += 1
            elif char == "[":
                end = pattern.index("]", index)
                excluded = pattern[index + 1 : end]
                assert excluded.startswith("^")
                tokens.append(("except", excluded[1:]))
                index = end + 1
            else:
                tokens.append(("literal", char))
                index += 1
        return tokens

    # The final two rules are an independent oracle for the only paths allowed to inherit.
    oracle = len(rules)
    patterns = [rule[0] for rule in rules] + ["/usr/bin/git", "/usr/lib/git-core/git{,-*}"]
    automata = [
        (rule, tokenize(expanded))
        for rule, pattern in enumerate(patterns)
        for expanded in expand(pattern)
    ]
    alphabet = {"/", "\x01"}
    for _, tokens in automata:
        for kind, value in tokens:
            if kind in {"literal", "except"}:
                alphabet.update(value)
    # All other filename bytes are equivalent to \x01: no literal or class singles them out.
    assert all("\x01" not in value for _, tokens in automata for _, value in tokens)

    def closure(states):
        result = set(states)
        pending = list(result)
        while pending:
            pattern, position = pending.pop()
            tokens = automata[pattern][1]
            if position < len(tokens) and tokens[position][0] in {"star", "tree"}:
                following = (pattern, position + 1)
                if following not in result:
                    result.add(following)
                    pending.append(following)
        return frozenset(result)

    def advance(states, char):
        following = set()
        for pattern, position in states:
            tokens = automata[pattern][1]
            if position == len(tokens):
                continue
            kind, value = tokens[position]
            if kind == "tree" or (kind == "star" and char != "/"):
                following.add((pattern, position))
            elif (kind == "literal" and char == value) or (
                kind == "any" and char != "/"
            ) or (kind == "except" and char not in value):
                following.add((pattern, position + 1))
        return closure(following)

    start = closure((index, 0) for index in range(len(automata)))
    first = advance(start, "/")
    queue = deque([(first, True, "/")])
    visited = {(first, True)}
    while queue:
        states, slash, path = queue.popleft()
        if not slash:  # Executable paths have a non-empty final component.
            accepted = {
                automata[pattern][0]
                for pattern, position in states
                if position == len(automata[pattern][1])
            }
            matches = [rules[index] for index in accepted if index < oracle]
            expected = ("ix", None) if any(index >= oracle for index in accepted) else (
                "px", "agcoord-admitted//&agcoord-job"
            )
            assert len(matches) == 1 and matches[0][2:] == expected, (path, matches, expected)
        for char in alphabet:
            if slash and char == "/":
                continue  # AppArmor sees normalized paths, without empty components.
            following = advance(states, char)
            key = (following, char == "/")
            if key not in visited:
                visited.add(key)
                queue.append((following, char == "/", path + char))


def test_gate_git_exec_rules_partition_paths_without_overlapping():
    policy = PROFILE.read_text(encoding="utf-8")
    gate = policy.split("profile agcoord-gate-git ", 1)[1].split("\n}", 1)[0]
    rules = []
    for line in gate.splitlines():
        match = re.fullmatch(r"\s*(/\S+)\s+(ix|px)(?: -> (\S+))?,", line)
        if match:
            glob, mode, target = match.groups()
            rules.append((glob, _aare_path_pattern(glob), mode, target))
    assert rules
    _assert_complete_exec_partition(rules)

    cases = {
        "/a", "/u", "/us", "/usr", "/usr/a", "/usr/b", "/usr/bi",
        "/usr/bin", "/usr/bin2/x", "/usr/bin/g", "/usr/bin/gi", "/usr/bin/git",
        "/usr/bin/gitx", "/usr/bin/git/x", "/usr/bin/sh", "/bin/sh", "/usr/l",
        "/usr/li", "/usr/lib", "/usr/lib64/x", "/usr/lib/g", "/usr/lib/gi",
        "/usr/lib/git", "/usr/lib/git-", "/usr/lib/git-c", "/usr/lib/git-co",
        "/usr/lib/git-cor", "/usr/lib/git-core", "/usr/lib/git-core2/x",
        "/usr/lib/git-core/g", "/usr/lib/git-core/gi", "/usr/lib/git-core/git",
        "/usr/lib/git-core/git-", "/usr/lib/git-core/git-checkout--worker",
        "/usr/lib/git-core/git-upload-pack", "/usr/lib/git-core/gitx",
        "/usr/lib/git-core/git/x", "/usr/lib/git-core/git-upload-pack/x",
        "/usr/lib/git-core/scalar", "/tmp/filter",
    }
    # Cover each byte at every divergence from a trusted path, plus both a component tail and
    # deeper descendants. This checks short prefixes, arbitrary filenames and slash boundaries.
    for trusted in ("/usr/bin/git", "/usr/lib/git-core/git", "/usr/lib/git-core/git-upload-pack"):
        for index in range(1, len(trusted) + 1):
            prefix = trusted[:index]
            if not prefix.endswith("/"):
                cases.add(prefix)
            for byte in range(1, 256):
                for suffix in ("", "x", "/x", "/x/y"):
                    path = prefix + chr(byte) + suffix
                    if "//" not in path and not path.endswith("/"):
                        cases.add(path)

    target = "agcoord-admitted//&agcoord-job"
    for path in cases:
        trusted_git = path == "/usr/bin/git" or re.fullmatch(
            r"/usr/lib/git-core/git(?:-[^/]*)?", path
        ) is not None
        expected = ("ix", None) if trusted_git else ("px", target)
        matched = [
            (glob, mode, destination)
            for glob, regex, mode, destination in rules
            if regex.fullmatch(path)
        ]
        assert len(matched) == 1 and matched[0][1:] == expected, (path, matched, expected)


def test_native_apparmor_transitions_are_safe_after_no_new_privileges():
    policy = PROFILE.read_text(encoding="utf-8")
    broker = policy.split("profile agcoord-broker ", 1)[1].split("\n}", 1)[0]
    client = policy.split("profile agcoord-broker-client ", 1)[1].split("\n}", 1)[0]
    admitted = policy.split("profile agcoord-admitted ", 1)[1].split("\n}", 1)[0]
    job = policy.split("profile agcoord-job ", 1)[1].split("\n}", 1)[0]
    bwrap = policy.split("profile agcoord-bwrap ", 1)[1].split("\n}", 1)[0]
    child = policy.split("profile agcoord-bwrap-child ", 1)[1].split("\n}", 1)[0]
    worker = WORKER.read_text(encoding="utf-8").split("fn child_main(", 1)[1]

    # The launcher enters the permissive admitted anchor and sets no_new_privs; the kernel records
    # that anchor, so every later exec keeps it. The anchor's exec rule applies the job restriction
    # by stacking at exec (the mechanism Ubuntu's bwrap profile uses), which is permitted under
    # no_new_privs and fails closed if the job profile is not loaded. Submitted code therefore runs
    # as agcoord-admitted//&agcoord-job and never as the bare anchor.
    assert "change_profile -> agcoord-admitted," in broker
    assert "/** px -> agcoord-admitted//&agcoord-job," in admitted
    assert "/** px -> agcoord-admitted//&agcoord-job," in broker
    assert "/** ix," not in admitted
    assert worker.index("enter_admitted_profile(") < worker.index("drop_privileges()")

    # Submitted code keeps the admitted job restriction, which denies namespace creation and
    # capabilities; only the fixed system bubblewrap path replaces that restriction, and nothing
    # reached through another mount namespace can match it.
    for restricted in (client, job, child):
        assert "deny userns," in restricted
        assert "deny capability," in restricted
        assert "/** ix," in restricted
    assert "allow userns create," in admitted
    assert "allow userns create," in bwrap
    assert (
        "/usr/bin/bwrap px -> agcoord-admitted//&agcoord-bwrap," in admitted
    )
    assert "/usr/bin/bwrap px -> agcoord-admitted//&agcoord-bwrap," in job
    assert (
        "/** px -> agcoord-admitted//&agcoord-job//&agcoord-bwrap-child," in bwrap
    )
    assert "attach_disconnected.path=/agcoord-disconnected" in (
        policy.split("profile agcoord-job ", 1)[1].split("{", 1)[0]
    )


def test_host_enforcement_startup_probe_is_bounded_and_retains_diagnostics():
    workflow = CI_WORKFLOW.read_text(encoding="utf-8")

    assert "timeout --signal=KILL 2s agc list" in workflow
    assert "cat native-host-client-error.txt" in workflow
    assert "systemctl --user --no-pager status agcoord-broker.service" in workflow
    assert workflow.count("journalctl --user --unit agcoord-broker.service --no-pager") == 2
    assert "cat native-host-receipt.json" in workflow
    assert 'agc log "$run_id"' in workflow
    assert "sudo journalctl --dmesg --no-pager" in workflow
    assert ".resource_receipt.applied.cpu == 1" in workflow
    assert ".resource_receipt.peak.cpu >= 1" in workflow
    assert ".resource_receipt.peak.cpu == 1" not in workflow
    # On an enforcement failure the job also retains the AppArmor transition diagnostics that make
    # a stack/exec-transition refusal legible: the loaded domains, the admitted anchor's rules, and
    # the agcoord/apparmor audit lines.
    assert "sudo aa-status" in workflow
    assert (
        "apparmor_parser --skip-kernel-load --skip-cache --debug"
        in workflow
    )
    assert "grep -iE 'apparmor|agcoord'" in workflow


def test_enforcement_probe_expects_apparmor_rendering_of_the_bubblewrap_child_stack():
    # AppArmor stores and prints a stacked label with its profiles sorted by name, so the label a
    # sandboxed process reports is the sorted rendering of the agcoord-bwrap exec target, not the
    # order the policy rule happens to list.
    policy = PROFILE.read_text(encoding="utf-8")
    bwrap = policy.split("profile agcoord-bwrap ", 1)[1].split("\n}", 1)[0]
    target = bwrap.split("/** px -> ", 1)[1].split(",", 1)[0]
    rendered = "//&".join(sorted(target.split("//&"))) + " (enforce)"
    probe = PROBE.read_text(encoding="utf-8")

    assert rendered == "agcoord-admitted//&agcoord-bwrap-child//&agcoord-job (enforce)"
    assert f"expected='{rendered}'" in probe
    assert 'bwrap=/usr/bin/bwrap' in probe
    assert "printf 'bubblewrap sandbox widening verified\\n'" in probe


def test_ci_installs_bubblewrap_and_requires_the_exercised_sandbox_widening():
    workflow = CI_WORKFLOW.read_text(encoding="utf-8")
    enforcement = workflow.split("native-host-enforcement:", 1)[1].split("\n  test:", 1)[0]

    assert "apparmor jq bubblewrap" in enforcement
    assert "test -x /usr/bin/bwrap" in enforcement
    assert "grep -qx 'bubblewrap sandbox widening verified'" in enforcement
