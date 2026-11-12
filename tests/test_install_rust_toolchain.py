"""The CI step that installs the pinned Rust toolchain, against a test-owned ``rustup``.

The fake ``rustup`` behaves like the real one where the step depends on it: it refuses to
install over a partly installed copy of the toolchain whose files its records do not cover,
and it cannot uninstall such a copy.  Nothing here downloads a toolchain.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys

import pytest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = "scripts/install-rust-toolchain"
HOST = "x86_64-unknown-linux-gnu"
PIN = """\
[toolchain]
channel = "1.94.1"
profile = "minimal"
components = ["clippy", "rustfmt"]
targets = ["x86_64-unknown-linux-musl"]
"""
INSTALL = [
    "toolchain",
    "install",
    "1.94.1",
    "--profile",
    "minimal",
    "--component",
    "clippy,rustfmt",
    "--target",
    "x86_64-unknown-linux-musl",
]

FAKE_RUSTUP = f"""\
#!{sys.executable}
import json, os, shutil, sys
from pathlib import Path

home = Path(os.environ["RUSTUP_HOME"])
state = Path(os.environ["FAKE_STATE"])
argv = sys.argv[1:]
with open(state / "rustup.log", "a", encoding="utf-8") as log:
    log.write(json.dumps({{"argv": argv, "cwd": os.getcwd()}}) + "\\n")


def toolchain(channel):
    return home / "toolchains" / f"{{channel}}-{HOST}"


if argv[:2] == ["show", "home"]:
    print(home)
elif argv[:2] == ["toolchain", "uninstall"]:
    path = toolchain(argv[2])
    if not (path / "lib" / "rustlib" / "components").exists():
        sys.stderr.write("error: toolchain is not installed\\n")
        raise SystemExit(1)
    shutil.rmtree(path)
elif argv[:2] == ["toolchain", "install"]:
    path = toolchain(argv[2])
    failures = state / "failing-installs"
    if failures.exists() and int(failures.read_text()) > 0:
        failures.write_text(str(int(failures.read_text()) - 1))
        # An interrupted download leaves a partial toolchain behind.
        (path / "bin").mkdir(parents=True, exist_ok=True)
        (path / "bin" / "cargo-clippy").write_text("partial")
        sys.stderr.write("error: component download failed\\n")
        raise SystemExit(1)
    if (path / "bin" / "cargo-clippy").exists() and not (
        path / "lib" / "rustlib" / "components"
    ).exists():
        sys.stderr.write(
            "info: recovering from a partially installed toolchain\\n"
            "error: failed to install component: 'clippy-preview-{HOST}', "
            "detected conflict: 'bin/cargo-clippy'\\n"
        )
        raise SystemExit(1)
    (path / "bin").mkdir(parents=True, exist_ok=True)
    (path / "bin" / "cargo-clippy").write_text("installed")
    (path / "lib" / "rustlib").mkdir(parents=True, exist_ok=True)
    (path / "lib" / "rustlib" / "components").write_text("clippy\\nrustfmt\\n")
    hashes = home / "update-hashes"
    hashes.mkdir(parents=True, exist_ok=True)
    (hashes / f"{{argv[2]}}-{HOST}").write_text("hash")
else:
    sys.stderr.write(f"fake rustup: unexpected {{argv}}\\n")
    raise SystemExit(2)
"""


class Runner:
    """A CI runner's view: a checkout with the script and a pin, and a rustup home."""

    def __init__(self, tmp_path: Path) -> None:
        self.checkout = tmp_path / "checkout"
        (self.checkout / "scripts").mkdir(parents=True)
        shutil.copy2(ROOT / SCRIPT, self.checkout / SCRIPT)
        (self.checkout / "rust-toolchain.toml").write_text(PIN)
        self.home = tmp_path / "rustup"
        self.home.mkdir()
        self.state = tmp_path / "state"
        self.state.mkdir()
        self.bin = tmp_path / "bin"
        self.bin.mkdir()
        rustup = self.bin / "rustup"
        rustup.write_text(FAKE_RUSTUP)
        rustup.chmod(0o755)
        self.toolchain = self.home / "toolchains" / f"1.94.1-{HOST}"

    def leave_partial_toolchain(self) -> None:
        """What a runner image can carry: toolchain files rustup has no record of."""
        (self.toolchain / "bin").mkdir(parents=True)
        (self.toolchain / "bin" / "cargo-clippy").write_text("from the image")
        (self.home / "update-hashes").mkdir()
        (self.home / "update-hashes" / f"1.94.1-{HOST}").write_text("stale")

    def run(self, **environment: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [str(self.checkout / SCRIPT)],
            cwd=self.checkout,
            env={
                "PATH": f"{self.bin}{os.pathsep}{os.environ['PATH']}",
                "RUSTUP_HOME": str(self.home),
                "FAKE_STATE": str(self.state),
                "AGCOORD_RUST_INSTALL_RETRY_DELAY": "0",
                **environment,
            },
            capture_output=True,
            text=True,
            timeout=60,
        )

    def calls(self) -> list[dict[str, object]]:
        log = self.state / "rustup.log"
        if not log.exists():
            return []
        return [json.loads(line) for line in log.read_text().splitlines()]

    def installs(self) -> list[list[str]]:
        return [call["argv"] for call in self.calls() if call["argv"][:2] == ["toolchain", "install"]]


@pytest.fixture
def runner(tmp_path: Path) -> Runner:
    return Runner(tmp_path)


def test_installs_every_component_and_target_the_pin_names(runner: Runner):
    completed = runner.run()

    assert completed.returncode == 0, completed.stderr
    assert runner.installs() == [INSTALL]
    assert (runner.toolchain / "lib" / "rustlib" / "components").exists()


def test_a_partly_installed_toolchain_from_the_runner_image_is_replaced(runner: Runner):
    runner.leave_partial_toolchain()

    completed = runner.run()

    assert completed.returncode == 0, completed.stderr
    assert "detected conflict" not in completed.stderr
    assert runner.installs() == [INSTALL]
    assert (runner.toolchain / "bin" / "cargo-clippy").read_text() == "installed"


def test_rustup_runs_outside_the_checkout_so_the_pin_cannot_install_implicitly(runner: Runner):
    completed = runner.run()

    assert completed.returncode == 0, completed.stderr
    checkout = str(runner.checkout)
    for call in runner.calls():
        assert not str(call["cwd"]).startswith(checkout), call


def test_a_failed_install_is_retried_from_a_clean_state(runner: Runner):
    runner.state.joinpath("failing-installs").write_text("1")

    completed = runner.run()

    assert completed.returncode == 0, completed.stderr
    assert runner.installs() == [INSTALL, INSTALL]
    assert "retrying from a clean state (1 of 3)" in completed.stderr


def test_an_install_that_keeps_failing_fails_the_step(runner: Runner):
    runner.state.joinpath("failing-installs").write_text("5")

    completed = runner.run(AGCOORD_RUST_INSTALL_ATTEMPTS="2")

    assert completed.returncode == 1
    assert len(runner.installs()) == 2
    assert "could not install Rust 1.94.1 after 2 attempts" in completed.stderr


def _jobs(workflow: str) -> dict[str, str]:
    body = workflow.split("\njobs:\n", 1)[1]
    parts = re.split(r"^  ([a-z][a-z0-9-]*):\n", body, flags=re.MULTILINE)
    return dict(zip(parts[1::2], parts[2::2]))


@pytest.mark.parametrize("workflow_name", ["ci.yml", "release.yml"])
def test_every_workflow_job_using_cargo_installs_the_toolchain_with_the_script_first(
    workflow_name: str,
):
    workflow = (ROOT / ".github/workflows" / workflow_name).read_text(encoding="utf-8")

    assert "rustup toolchain install" not in workflow
    for name, job in _jobs(workflow).items():
        cargo = re.search(r"^\s+(- )?run: cargo ", job, flags=re.MULTILINE)
        if cargo is None:
            continue
        install = job.find(f"./{SCRIPT}")
        assert 0 <= install < cargo.start(), f"{workflow_name} job {name}"
