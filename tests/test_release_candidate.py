"""Public artifact-boundary tests for the native 0.4 release candidate."""

from __future__ import annotations

import hashlib
import io
import json
from pathlib import Path
import runpy
import subprocess
import sys
import tarfile
import zipfile

import pytest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/verify-release-candidate"
RELEASE = runpy.run_path(str(SCRIPT))
CandidateError = RELEASE["CandidateError"]
MIGRATION_SCRIPT = ROOT / "scripts/rehearse-native-migration"
MIGRATION = runpy.run_path(str(MIGRATION_SCRIPT))


def test_cli_preserves_the_supplied_virtualenv_interpreter(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    base_python = tmp_path / "base-python"
    base_python.write_text("base\n", encoding="utf-8")
    virtualenv_bin = tmp_path / "release-venv/bin"
    virtualenv_bin.mkdir(parents=True)
    virtualenv_python = virtualenv_bin / "python"
    virtualenv_python.symlink_to(base_python)
    observed: dict[str, Path] = {}

    def fake_verify_candidate(
        python_dir: Path,
        native_dir: Path,
        host_dir: Path,
        output: Path,
        python: Path,
        tag: str | None,
    ) -> dict[str, str]:
        observed["python"] = python
        return {"version": "0.3.0"}

    monkeypatch.setitem(
        RELEASE["main"].__globals__, "verify_candidate", fake_verify_candidate
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            str(SCRIPT),
            "--python-dir",
            str(tmp_path / "python"),
            "--native-dir",
            str(tmp_path / "native"),
            "--host-dir",
            str(tmp_path / "host"),
            "--output-dir",
            str(tmp_path / "output"),
            "--python",
            str(virtualenv_python),
        ],
    )

    assert RELEASE["main"]() == 0
    assert observed["python"] == virtualenv_python


def test_migration_cli_preserves_the_supplied_virtualenv_interpreter(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    base_python = tmp_path / "base-python"
    base_python.write_text("base\n", encoding="utf-8")
    virtualenv_bin = tmp_path / "migration-venv/bin"
    virtualenv_bin.mkdir(parents=True)
    virtualenv_python = virtualenv_bin / "python"
    virtualenv_python.symlink_to(base_python)
    agc = tmp_path / "agc"
    broker = tmp_path / "agcoord-broker"
    observed: dict[str, Path] = {}

    def fake_rehearse(python: Path, supplied_agc: Path, supplied_broker: Path):
        observed["python"] = python
        observed["agc"] = supplied_agc
        observed["broker"] = supplied_broker
        return {"final_protocol": 5}

    monkeypatch.setitem(MIGRATION["main"].__globals__, "rehearse", fake_rehearse)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            str(MIGRATION_SCRIPT),
            "--python",
            str(virtualenv_python),
            "--agc",
            str(agc),
            "--broker",
            str(broker),
        ],
    )

    assert MIGRATION["main"]() == 0
    assert observed["python"] == virtualenv_python


def test_release_sources_declare_one_stable_version_and_ship_the_gate():
    assert RELEASE["source_versions"]() == ("0.4.1", "0.4.1", "0.4.1")
    assert SCRIPT.stat().st_mode & 0o111
    assert (ROOT / "scripts/rehearse-native-migration").stat().st_mode & 0o111
    manifest = (ROOT / "MANIFEST.in").read_text(encoding="utf-8")
    assert "include scripts/verify-release-candidate" in manifest
    assert "include scripts/*native*" in manifest

    help_result = subprocess.run(
        [SCRIPT, "--help"],
        text=True,
        capture_output=True,
        check=False,
    )
    assert help_result.returncode == 0, help_result.stderr
    assert "--python-dir" in help_result.stdout
    assert "--native-dir" in help_result.stdout
    assert "--host-dir" in help_result.stdout
    assert "--output-dir" in help_result.stdout


def test_fresh_release_candidate_jobs_fetch_locked_rust_dependencies():
    for workflow_name in ("ci.yml", "release.yml"):
        workflow = (ROOT / ".github/workflows" / workflow_name).read_text(
            encoding="utf-8"
        )
        release_job = workflow.split("\n  release-candidate:\n", 1)[1]
        toolchain = release_job.index(
            "rustup toolchain install 1.94.1 --profile minimal"
        )
        fetch = release_job.index("cargo fetch --locked")
        verify = release_job.index("./scripts/verify-release-candidate")

        assert toolchain < fetch < verify


def test_checksum_sidecar_binds_one_exact_basename(tmp_path: Path):
    sidecar = tmp_path / "artifact.sha256"
    digest = "a" * 64
    sidecar.write_text(f"{digest}  artifact\n", encoding="ascii")
    assert RELEASE["_sidecar"](sidecar, "artifact") == digest

    sidecar.write_text(f"{digest}  ../artifact\n", encoding="ascii")
    with pytest.raises(CandidateError, match="canonical"):
        RELEASE["_sidecar"](sidecar, "artifact")


def _pin_inputs(tmp_path: Path, broker: bytes = b"reproducible broker\n"):
    tmp_path.mkdir(parents=True, exist_ok=True)
    artifact = tmp_path / "agcoord-broker-x86_64-unknown-linux-musl"
    artifact.write_bytes(broker)
    package = tmp_path / "agcoord-native-host-x86_64-linux.tar.gz"
    with tarfile.open(package, "w:gz") as archive:
        entry = tarfile.TarInfo("./usr/libexec/agcoord/agcoord-broker")
        entry.size = len(broker)
        entry.mode = 0o755
        archive.addfile(entry, io.BytesIO(broker))
    return artifact, package, hashlib.sha256(broker).hexdigest()


def _write_pin(path: Path, digest, *, version: str = "0.5.0") -> Path:
    path.write_text(
        json.dumps({"format": 1, "version": version, "broker_sha256": digest}),
        encoding="utf-8",
    )
    return path


def _redirect_pin(monkeypatch: pytest.MonkeyPatch, pin: Path) -> None:
    monkeypatch.setitem(RELEASE["_shipped_pin"].__globals__, "PIN_SOURCE", pin)


def test_a_release_must_pin_the_broker_its_clients_will_download(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    artifact, package, digest = _pin_inputs(tmp_path)
    pin = _write_pin(tmp_path / "native_host_pin.json", None)
    _redirect_pin(monkeypatch, pin)

    with pytest.raises(CandidateError, match="carries no broker digest"):
        RELEASE["_shipped_pin"](artifact, package, "0.5.0")

    _write_pin(pin, digest)
    assert RELEASE["_shipped_pin"](artifact, package, "0.5.0") == digest


def test_a_pin_that_names_another_broker_closes_the_release_gate(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    artifact, package, _ = _pin_inputs(tmp_path)
    pin = _write_pin(tmp_path / "native_host_pin.json", "b" * 64)
    _redirect_pin(monkeypatch, pin)

    with pytest.raises(CandidateError, match="does not match the released broker"):
        RELEASE["_shipped_pin"](artifact, package, "0.5.0")

    _write_pin(pin, hashlib.sha256(b"reproducible broker\n").hexdigest(), version="0.4.9")
    with pytest.raises(CandidateError, match="does not name this release version"):
        RELEASE["_shipped_pin"](artifact, package, "0.5.0")


def test_a_host_package_carrying_another_broker_closes_the_release_gate(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    artifact, _, digest = _pin_inputs(tmp_path)
    _, other_package, _ = _pin_inputs(tmp_path / "other", b"a different broker\n")
    _redirect_pin(monkeypatch, _write_pin(tmp_path / "native_host_pin.json", digest))

    with pytest.raises(CandidateError, match="host package broker"):
        RELEASE["_shipped_pin"](artifact, other_package, "0.5.0")


def test_release_artifact_modes_are_exact_not_umask_dependent(tmp_path: Path):
    artifact = tmp_path / "artifact"
    artifact.write_bytes(b"artifact")
    artifact.chmod(0o644)
    RELEASE["_require_mode"](artifact, 0o644)

    artifact.chmod(0o664)
    with pytest.raises(CandidateError, match="expected 0644"):
        RELEASE["_require_mode"](artifact, 0o644)


def _write_wheel(path: Path, *, legacy_console: bool = False) -> None:
    console = "agc = agcoord.cli:main\n"
    if legacy_console:
        console += "agcoord = agcoord.cli:main\n"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(
            "agcoord-0.3.0.dist-info/METADATA",
            "Metadata-Version: 2.4\nName: agcoord\nVersion: 0.3.0\n\n",
        )
        archive.writestr(
            "agcoord-0.3.0.dist-info/entry_points.txt",
            f"[console_scripts]\n{console}\n"
            "[pytest11]\nagcoord-xdist = agcoord.pytest_xdist\n",
        )
        source_root = ROOT / "src/agcoord"
        for source in source_root.rglob("*"):
            if source.is_file() and "__pycache__" not in source.parts:
                archive.write(
                    source,
                    f"agcoord/{source.relative_to(source_root).as_posix()}",
                )


def test_wheel_contract_rejects_the_removed_console_name(tmp_path: Path):
    wheel = tmp_path / "agcoord-0.3.0-py3-none-any.whl"
    _write_wheel(wheel)
    RELEASE["_wheel_metadata"](wheel, "0.3.0")

    _write_wheel(wheel, legacy_console=True)
    with pytest.raises(CandidateError, match="console entry points"):
        RELEASE["_wheel_metadata"](wheel, "0.3.0")


def _tar_file(archive: tarfile.TarFile, name: str, data: bytes, mode: int) -> None:
    member = tarfile.TarInfo(name)
    member.size = len(data)
    member.mode = mode
    archive.addfile(member, io.BytesIO(data))


def _write_sdist(path: Path, *, executable: bool = True) -> None:
    prefix = "agcoord-0.3.0/"
    with tarfile.open(path, "w:gz") as archive:
        root = tarfile.TarInfo(prefix.rstrip("/"))
        root.type = tarfile.DIRTYPE
        root.mode = 0o755
        archive.addfile(root)
        _tar_file(
            archive,
            f"{prefix}PKG-INFO",
            b"Metadata-Version: 2.4\nName: agcoord\nVersion: 0.3.0\n\n",
            0o644,
        )
        _tar_file(
            archive,
            f"{prefix}docs/native_migration.md",
            (ROOT / "docs/native_migration.md").read_bytes(),
            0o644,
        )
        for script in ("rehearse-native-migration", "verify-release-candidate"):
            _tar_file(
                archive,
                f"{prefix}scripts/{script}",
                (ROOT / f"scripts/{script}").read_bytes(),
                0o755 if executable else 0o644,
            )


def test_sdist_contract_requires_executable_release_procedures(tmp_path: Path):
    sdist = tmp_path / "agcoord-0.3.0.tar.gz"
    _write_sdist(sdist)
    RELEASE["_sdist_metadata"](sdist, "0.3.0")

    _write_sdist(sdist, executable=False)
    with pytest.raises(CandidateError, match="not executable"):
        RELEASE["_sdist_metadata"](sdist, "0.3.0")


def test_assembled_bundle_has_one_complete_aggregate_checksum_set(tmp_path: Path):
    inputs = tmp_path / "inputs"
    inputs.mkdir()
    sources = []
    for name, content in (
        (RELEASE["HOST_NAME"], b"host"),
        ("agcoord-0.3.0.tar.gz", b"sdist"),
        ("agcoord-0.3.0-py3-none-any.whl", b"wheel"),
    ):
        path = inputs / name
        path.write_bytes(content)
        sources.append(path)
    identity = {
        "name": "agcoord-broker",
        "version": "0.3.0",
        "protocol": 5,
        "implementation": "rust-native",
        "build": "sha256:" + "0" * 64,
        "target": RELEASE["TARGET"],
        "sqlite": "3.53.2",
    }
    output = tmp_path / "release"
    manifest_path = RELEASE["_assemble"](
        output,
        sources,
        "0.3.0",
        identity,
        {"identity": identity},
        {
            "backup_sha256": "1" * 64,
            "final_protocol": 5,
            "rollback_protocol": 4,
        },
    )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["version"] == "0.3.0"
    assert manifest["compatibility"]["protocol"] == 5
    checksums = (output / "SHA256SUMS").read_text(encoding="ascii").splitlines()
    assert len(checksums) == len(sources) + 1
    assert {line.split("  ", 1)[1] for line in checksums} == {
        *(source.name for source in sources),
        "release-manifest.json",
    }
    assert "SHA256SUMS" not in {line.split("  ", 1)[1] for line in checksums}

    with pytest.raises(CandidateError, match="already exists"):
        RELEASE["_assemble"](
            output,
            sources,
            "0.3.0",
            identity,
            {"identity": identity},
            {"backup_sha256": "1" * 64},
        )
