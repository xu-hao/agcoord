"""Executable validation of the native host service and AppArmor policy artifacts."""

from __future__ import annotations

from pathlib import Path
import shutil
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[1]
UNIT = ROOT / "packaging/systemd/agcoord-broker.service"
PROFILE = ROOT / "packaging/apparmor/usr.libexec.agcoord.agcoord-broker"
CI_WORKFLOW = ROOT / ".github/workflows/ci.yml"
WORKER = ROOT / "native/agcoord-broker/src/worker.rs"


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


def test_native_apparmor_policy_compiles_with_all_three_domains():
    parser = shutil.which("apparmor_parser")
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
    }


def test_native_apparmor_policy_compiles_all_domains_in_enforce_mode():
    parser = shutil.which("apparmor_parser")
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
    assert diagnostics.count("Mode: enforce") == 3, diagnostics
    assert "Mode: default_allow" not in diagnostics


def test_native_apparmor_transitions_are_safe_after_no_new_privileges():
    policy = PROFILE.read_text(encoding="utf-8")
    broker = policy.split("profile agcoord-broker ", 1)[1].split("\n}", 1)[0]
    client = policy.split("profile agcoord-broker-client ", 1)[1].split("\n}", 1)[0]
    admitted = policy.split("profile agcoord-admitted ", 1)[1].split("\n}", 1)[0]
    worker = WORKER.read_text(encoding="utf-8").split("fn child_main(", 1)[1]

    assert "change_profile -> agcoord-admitted," in broker
    assert "/** ix," in client
    assert (
        "/usr/libexec/agcoord/agcoord-broker px -> &agcoord-broker-client,"
        in admitted
    )
    assert "/** ix," in admitted
    assert worker.index("enter_admitted_profile(") < worker.index("drop_privileges()")


def test_host_enforcement_startup_probe_is_bounded_and_retains_diagnostics():
    workflow = CI_WORKFLOW.read_text(encoding="utf-8")

    assert "timeout --signal=KILL 2s agc list" in workflow
    assert "cat native-host-client-error.txt" in workflow
    assert "systemctl --user --no-pager status agcoord-broker.service" in workflow
    assert workflow.count("journalctl --user --unit agcoord-broker.service --no-pager") == 2
    assert "cat native-host-receipt.json" in workflow
    assert 'agc log "$run_id"' in workflow
    assert "sudo journalctl --dmesg --no-pager" in workflow
