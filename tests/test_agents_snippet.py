"""The persistent agent trigger ships as one file in every distribution surface."""

from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile


ROOT = Path(__file__).resolve().parents[1]
SNIPPET = "AGENTS.snippet.md"


def _copy_checkout(destination: Path) -> Path:
    """Copy the sources so that building or validating never writes into this checkout."""
    shutil.copytree(
        ROOT,
        destination,
        ignore=shutil.ignore_patterns(
            ".git",
            ".venv",
            ".pytest_cache",
            "__pycache__",
            "*.egg-info",
            "build",
            "dist",
            "target",
        ),
    )
    return destination


def _isolated_environment(home: Path, **overrides: str) -> dict[str, str]:
    home.mkdir()
    environment = {
        name: value
        for name, value in os.environ.items()
        if name != "PYTHONPATH" and not name.startswith("XDG_")
    }
    environment["HOME"] = str(home)
    environment.update(overrides)
    return environment


def test_source_distribution_ships_the_exact_agent_snippet(tmp_path: Path):
    source = _copy_checkout(tmp_path / "source")
    output = tmp_path / "dist"
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "build",
            "--sdist",
            "--no-isolation",
            "--outdir",
            str(output),
            str(source),
        ],
        cwd=tmp_path,
        env=_isolated_environment(tmp_path / "home"),
        capture_output=True,
        text=True,
        check=False,
        timeout=300,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr

    (sdist,) = output.glob("agcoord-*.tar.gz")
    member = f"{sdist.name.removesuffix('.tar.gz')}/{SNIPPET}"
    with tarfile.open(sdist, "r:gz") as archive:
        assert member in archive.getnames()
        shipped = archive.extractfile(member)
        assert shipped is not None
        assert shipped.read() == (ROOT / SNIPPET).read_bytes()


# Stands in for both host CLIs: it answers like a clean validation and records what the
# strictly validated Antigravity payload held while that temporary directory still existed.
_FAKE_HOST_CLI = """\
import json
import os
from pathlib import Path
import sys

arguments = sys.argv[1:]
if "--strict" in arguments and "--json" in arguments:
    payload = Path(arguments[2])
    held = {
        path.relative_to(payload).as_posix(): path.read_text(encoding="utf-8")
        for path in sorted(payload.rglob("*"))
        if path.is_file()
    }
    Path(os.environ["FAKE_HOST_CLI_RECORD"]).write_text(json.dumps(held), encoding="utf-8")
print(json.dumps({"findings": [], "components": {"skills": 1}}))
"""


def test_strict_antigravity_payload_carries_the_exact_agent_snippet(tmp_path: Path):
    source = _copy_checkout(tmp_path / "source")
    binaries = tmp_path / "bin"
    binaries.mkdir()
    for name in ("claude", "agy"):
        executable = binaries / name
        executable.write_text(f"#!{sys.executable}\n{_FAKE_HOST_CLI}", encoding="utf-8")
        executable.chmod(0o755)
    record = tmp_path / "payload.json"
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    environment = _isolated_environment(
        tmp_path / "home",
        PATH=f"{binaries}{os.pathsep}{os.environ['PATH']}",
        TMPDIR=str(scratch),
        FAKE_HOST_CLI_RECORD=str(record),
    )

    completed = subprocess.run(
        [sys.executable, str(source / "scripts/check-plugins.py")],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
        timeout=60,
    )

    assert completed.returncode == 0, completed.stdout + completed.stderr
    held = json.loads(record.read_text(encoding="utf-8"))
    assert held[SNIPPET] == (ROOT / SNIPPET).read_text(encoding="utf-8")
    assert "skills/agcoord/SKILL.md" in held
    assert ".antigravity-plugin/plugin.json" in held
    assert not {"AGENTS.md", "CLAUDE.md", "plugin.json"} & held.keys()
    assert list(scratch.iterdir()) == []
