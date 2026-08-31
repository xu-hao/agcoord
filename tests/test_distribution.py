"""The published project is usable as a standalone `agcoord` distribution."""

from __future__ import annotations

import importlib.metadata
import json
import os
from pathlib import Path
import re
import subprocess
import sys

from packaging.requirements import Requirement


PUBLIC_COMMANDS = {
    "run",
    "full",
    "list",
    "show",
    "log",
    "cancel",
    "tui",
    "land",
    "migrate",
    "clear",
}


def _module(*arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "agcoord", *arguments],
        check=False,
        capture_output=True,
        text=True,
    )


def test_distribution_import_and_console_identity_are_exact():
    distribution = importlib.metadata.distribution("agcoord")
    console_scripts = {
        entry.name: entry.value
        for entry in distribution.entry_points
        if entry.group == "console_scripts"
    }

    assert distribution.metadata["Name"] == "agcoord"
    assert console_scripts == {"agc": "agcoord.cli:main"}
    assert {
        entry.name: entry.value
        for entry in distribution.entry_points
        if entry.group == "pytest11"
    } == {"agcoord-xdist": "agcoord.pytest_xdist"}
    assert "xdist" in (distribution.metadata.get_all("Provides-Extra") or [])
    assert not Path(sys.executable).with_name("agcoord").exists()
    assert importlib.metadata.version("agcoord")


def test_distribution_requires_supported_textual_8_release_line():
    requirements = [
        Requirement(value)
        for value in importlib.metadata.distribution("agcoord").requires or []
    ]
    textual = [requirement for requirement in requirements if requirement.name == "textual"]

    assert len(textual) == 1
    assert textual[0].marker is None
    assert {
        (specifier.operator, specifier.version)
        for specifier in textual[0].specifier
    } == {(">=", "8.2"), ("<", "9")}


def test_module_entrypoint_exposes_the_complete_public_command_set():
    completed = _module("--help")

    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.startswith("usage: agc")
    help_text = completed.stdout.lower()
    exposed = set(re.findall(r"\b[a-z][a-z-]*\b", help_text))
    assert PUBLIC_COMMANDS <= exposed
    assert "merge" not in exposed


def test_core_imports_without_a_parent_project_or_forge_dependency(tmp_path: Path):
    probe = tmp_path / "standalone_probe.py"
    probe.write_text(
        """
import builtins
import json
import sys

real_import = builtins.__import__

def guarded_import(name, globals=None, locals=None, fromlist=(), level=0):
    if name in {"github", "gh"}:
        raise AssertionError(f"forge dependency imported by core: {name}")
    return real_import(name, globals, locals, fromlist, level)

builtins.__import__ = guarded_import
import agcoord
from agcoord.queue import CoordinatorBroker, CoordinatorClient

print(json.dumps({
    "package": agcoord.__name__,
    "broker": CoordinatorBroker.__name__,
    "client": CoordinatorClient.__name__,
    "package_root": agcoord.__file__.split("/")[-2],
}))
""",
        encoding="utf-8",
    )
    environment = dict(os.environ)
    environment.pop("PYTHONPATH", None)
    completed = subprocess.run(
        [sys.executable, "-I", str(probe)],
        check=False,
        capture_output=True,
        text=True,
        env=environment,
    )

    assert completed.returncode == 0, completed.stderr
    assert json.loads(completed.stdout) == {
        "package": "agcoord",
        "broker": "CoordinatorBroker",
        "client": "CoordinatorClient",
        "package_root": "agcoord",
    }


def test_console_script_and_module_entrypoint_report_the_same_version():
    module = _module("--version")
    console = subprocess.run(
        [str(Path(sys.executable).with_name("agc")), "--version"],
        check=False,
        capture_output=True,
        text=True,
    )

    assert module.returncode == 0, module.stderr
    assert console.returncode == 0, console.stderr
    assert console.stdout.strip() == module.stdout.strip()
    assert module.stdout.startswith("agc ")
    assert importlib.metadata.version("agcoord") in module.stdout
