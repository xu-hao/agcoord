"""Creating this workstation's result-cache client key through the operator's ``gcloud``.

``agc overflow-gcp-batch create-result-cache-key`` and the same command under
``overflow-gcp-run`` run as real subprocesses against the fake ``gcloud`` described in
``gcp_fakes``.  Nothing here contacts a cloud API, and ``HOME`` is a test directory, so the
operator's own key is never read or replaced.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import stat
import subprocess
import sys

import pytest

import agcoord
from gcp_fakes import PRIVATE_KEY, PROJECT, Cloud


ACCOUNT = f"agcoord-result-cache-client@{PROJECT}.iam.gserviceaccount.com"
KEY = Path(".config", "agcoord", "result-cache-client.json")
SECRET = "FAKEPRIVATEKEYMATERIALNEVERPRINTED"
FIRST_KEY_ID = "0123456789abcdef0123456789abcdef01234567"


@pytest.fixture(params=["overflow-gcp-batch", "overflow-gcp-run"])
def cloud(request: pytest.FixtureRequest, tmp_path: Path) -> Cloud:
    return Cloud(tmp_path, request.param)


@pytest.fixture
def home(tmp_path: Path) -> Path:
    home = tmp_path / "home"
    home.mkdir()
    return home


def _create(cloud: Cloud, home: Path, *options: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "agcoord",
            cloud.executor,
            "create-result-cache-key",
            f"--project={PROJECT}",
            f"--iam-account={ACCOUNT}",
            *options,
        ],
        env={
            "PATH": f"{cloud.bin}{os.pathsep}{os.environ['PATH']}",
            "HOME": str(home),
            "PYTHONPATH": str(Path(agcoord.__file__).resolve().parents[1]),
            "FAKE_STATE": str(cloud.state),
        },
        capture_output=True,
        text=True,
        timeout=60,
    )


def _mode(path: Path) -> int:
    return stat.S_IMODE(path.lstat().st_mode)


def _keys_created(cloud: Cloud) -> list[list[str]]:
    return cloud.called("iam", "service-accounts", "keys", "create")


def _assert_private_material_hidden(completed: subprocess.CompletedProcess[str]) -> None:
    output = completed.stdout + completed.stderr
    assert SECRET not in output
    assert "PRIVATE KEY" not in output


def test_it_creates_the_default_key_privately_and_never_prints_it(cloud: Cloud, home: Path):
    completed = _create(cloud, home)

    assert completed.returncode == 0, completed.stderr
    key = home / KEY
    assert json.loads(key.read_text(encoding="utf-8"))["private_key"] == PRIVATE_KEY
    assert _mode(key) == 0o600
    assert _mode(key.parent) == 0o700
    # gcloud wrote the key under umask 077 into a private directory beside the final path,
    # from which it was moved into place; nothing else is left behind.
    (call,) = _keys_created(cloud)
    written = Path(call[4])
    assert written.parent.parent == key.parent
    assert call[5:] == [f"--iam-account={ACCOUNT}", f"--project={PROJECT}", "--quiet"]
    assert cloud.state.joinpath("keys-create-umask").read_text() == oct(0o077)
    assert sorted(os.listdir(key.parent)) == [KEY.name]
    _assert_private_material_hidden(completed)
    assert FIRST_KEY_ID in completed.stdout
    assert str(key) in completed.stdout


@pytest.mark.parametrize("existing", ["file", "dangling-link"])
def test_it_never_overwrites_an_existing_key_without_replace(
    cloud: Cloud, home: Path, existing: str
):
    key = home / KEY
    key.parent.mkdir(parents=True)
    if existing == "file":
        key.write_text("existing key\n", encoding="utf-8")
    else:
        key.symlink_to(home / "missing.json")

    completed = _create(cloud, home)

    assert completed.returncode == 1
    assert "--replace" in completed.stderr
    if existing == "file":
        assert key.read_text(encoding="utf-8") == "existing key\n"
    else:
        assert key.is_symlink() and not key.exists()
    assert _keys_created(cloud) == []


def test_replace_installs_a_new_key_and_names_the_old_one_for_revocation(
    cloud: Cloud, home: Path
):
    key = home / KEY
    key.parent.mkdir(parents=True, mode=0o700)
    key.write_text(
        json.dumps({"private_key_id": "old0key0id", "private_key": "OLDPRIVATEKEYMATERIAL"}),
        encoding="utf-8",
    )
    key.chmod(0o600)
    cloud.answer("key-ids", ["new0key0id"])

    completed = _create(cloud, home, "--replace")

    assert completed.returncode == 0, completed.stderr
    assert json.loads(key.read_text(encoding="utf-8"))["private_key_id"] == "new0key0id"
    assert _mode(key) == 0o600
    assert sorted(os.listdir(key.parent)) == [KEY.name]
    assert "new0key0id" in completed.stdout
    assert (
        f"gcloud iam service-accounts keys delete old0key0id --iam-account={ACCOUNT} "
        f"--project={PROJECT}"
    ) in completed.stdout
    _assert_private_material_hidden(completed)
    assert "OLDPRIVATEKEYMATERIAL" not in completed.stdout + completed.stderr


def test_an_explicit_output_path_is_created_privately(
    cloud: Cloud, home: Path, tmp_path: Path
):
    output = tmp_path / "keys" / "client.json"

    completed = _create(cloud, home, f"--output={output}")

    assert completed.returncode == 0, completed.stderr
    assert json.loads(output.read_text(encoding="utf-8"))["client_email"] == ACCOUNT
    assert _mode(output) == 0o600
    assert _mode(output.parent) == 0o700
    assert not (home / KEY).exists()
    # Local jobs find a key outside the default path only through the pool's executor.
    assert f"--result-cache-key {output}" in completed.stdout
    _assert_private_material_hidden(completed)


def test_a_failed_key_creation_leaves_nothing_behind(cloud: Cloud, home: Path):
    cloud.state.joinpath("gcloud-fails").write_text("create")

    completed = _create(cloud, home)

    assert completed.returncode == 1
    assert "fake gcloud failure" in completed.stderr
    assert os.listdir(home / KEY.parent) == []
    _assert_private_material_hidden(completed)


def test_the_key_is_never_written_to_standard_output(cloud: Cloud, home: Path):
    completed = _create(cloud, home, "--output=-")

    assert completed.returncode == 1
    assert "standard output" in completed.stderr
    assert _keys_created(cloud) == []
    _assert_private_material_hidden(completed)
