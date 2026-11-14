"""Offline validation of the actual native host package checker."""

from __future__ import annotations

import hashlib
import io
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import tarfile

import pytest


ROOT = Path(__file__).resolve().parents[1]
CHECKER = ROOT / "scripts/check-native-host-package"
BUILDER = ROOT / "scripts/build-native-host-package"
PROFILE_NAME = "./etc/apparmor.d/usr.libexec.agcoord.agcoord-broker"
MANIFEST_NAME = "./usr/share/doc/agcoord/native-host-manifest.json"


@pytest.fixture
def host_package(tmp_path: Path) -> tuple[Path, dict[str, str], Path]:
    parser = shutil.which("apparmor_parser", path="/usr/sbin:/sbin")
    if parser is None:
        pytest.skip("apparmor_parser is unavailable in /usr/sbin or /sbin")

    # Keep the parser off PATH so the builder and checker must find the sbin copy. All files
    # and the fake development broker belong to this test; no host service is touched.
    bindir = tmp_path / "bin"
    bindir.mkdir()
    for command in (
        "bash", "readlink", "dirname", "mkdir", "env", "jq", "mktemp", "rm", "install",
        "sha256sum", "awk", "chmod", "tar", "gzip", "basename", "stat", "sort", "grep",
    ):
        executable = shutil.which(command)
        if executable is None:
            pytest.skip(f"{command} is unavailable")
        (bindir / command).symlink_to(executable)
    assert shutil.which("apparmor_parser", path=str(bindir)) is None

    identity = {
        "name": "agcoord-broker", "protocol": 5, "implementation": "rust-native",
        "build": "development", "target": "x86_64-unknown-linux-gnu",
    }
    broker = tmp_path / "broker"
    broker.write_text(f"#!/bin/sh\nprintf '%s\\n' '{json.dumps(identity)}'\n", encoding="utf-8")
    broker.chmod(0o755)
    env = {**os.environ, "PATH": str(bindir), "AGCOORD_HOST_ALLOW_DEVELOPMENT": "1"}
    completed = subprocess.run(
        [str(BUILDER), str(broker), str(tmp_path / "bundle")],
        env=env, capture_output=True, text=True, check=False,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    package = tmp_path / "bundle/agcoord-native-host-x86_64-linux.tar.gz"
    return package, env, Path(parser)


def _tamper_with_conflicting_exec_rule(package: Path) -> None:
    with tarfile.open(package, "r:gz") as archive:
        members = archive.getmembers()
        contents = {
            member.name: archive.extractfile(member).read()
            for member in members if member.isfile()
        }

    profile = contents[PROFILE_NAME].decode()
    prefix, gate = profile.split("profile agcoord-gate-git ", 1)
    gate = gate.replace("\n}", "\n  /** px -> agcoord-admitted//&agcoord-job,\n}", 1)
    contents[PROFILE_NAME] = (prefix + "profile agcoord-gate-git " + gate).encode()
    manifest = json.loads(contents[MANIFEST_NAME])
    manifest["files"]["/etc/apparmor.d/usr.libexec.agcoord.agcoord-broker"] = hashlib.sha256(
        contents[PROFILE_NAME]
    ).hexdigest()
    contents[MANIFEST_NAME] = json.dumps(manifest).encode()

    with tarfile.open(package, "w:gz") as archive:
        for member in members:
            data = contents.get(member.name)
            if data is not None:
                member.size = len(data)
            archive.addfile(member, io.BytesIO(data) if data is not None else None)
    digest = hashlib.sha256(package.read_bytes()).hexdigest()
    package.with_name(package.name + ".sha256").write_text(
        f"{digest}  {package.name}\n", encoding="ascii"
    )


def test_checker_rejects_binary_compilation_conflicts(host_package):
    package, env, _parser = host_package
    _tamper_with_conflicting_exec_rule(package)
    completed = subprocess.run(
        [str(CHECKER), str(package)], env=env, capture_output=True, text=True, check=False,
    )
    assert completed.returncode != 0
    assert "profile has merged rule with conflicting x modifiers" in completed.stderr
    assert "agcoord-gate-git" in completed.stderr


def test_checker_rejects_empty_binary_output(host_package):
    package, env, parser = host_package
    wrapper = Path(env["PATH"]) / "apparmor_parser"
    wrapper.write_text(
        "#!/bin/sh\ncase \" $* \" in *' --ofile '*) exit 0;; esac\n"
        f"exec {shlex.quote(str(parser))} \"$@\"\n",
        encoding="utf-8",
    )
    wrapper.chmod(0o755)
    completed = subprocess.run(
        [str(CHECKER), str(package)], env=env, capture_output=True, text=True, check=False,
    )
    assert completed.returncode != 0
    assert "binary compilation produced no output" in completed.stderr
