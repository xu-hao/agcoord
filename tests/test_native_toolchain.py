"""The C toolchain that compiles the native broker's bundled C sources is a declared build input."""

from __future__ import annotations

import json
import os
from pathlib import Path
import runpy
import shutil
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[1]
TOOLCHAIN = ROOT / "scripts/native-toolchain"
DECLARATION = ROOT / "native/build-toolchain.json"
RELEASE = runpy.run_path(str(ROOT / "scripts/verify-release-candidate"))

pytestmark = pytest.mark.skipif(shutil.which("jq") is None, reason="jq is unavailable")


def _run(*arguments: str, stdin: str | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [str(TOOLCHAIN), *arguments],
        input=stdin,
        text=True,
        capture_output=True,
        check=False,
    )


def _fake_musl_compiler(tmp_path: Path, *, version_line: str, musl_version: str) -> Path:
    musl = tmp_path / "lib/x86_64-linux-musl"
    musl.mkdir(parents=True)
    (musl / "musl-gcc.specs").write_text("%rename cpp_options old_cpp_options\n")
    libc = musl / "libc.so"
    libc.write_text(
        "#!/bin/sh\n"
        f"printf 'musl libc (x86_64)\\nVersion {musl_version}\\nDynamic Program Loader\\n' >&2\n"
        "exit 1\n"
    )
    libc.chmod(0o755)
    compiler = tmp_path / "bin/x86_64-linux-musl-gcc"
    compiler.parent.mkdir()
    compiler.write_text(
        "#!/bin/sh\n"
        'if [ "$1" = --version ]; then\n'
        f"  printf '%s\\n' '{version_line}' 'Copyright (C) 2026'\n"
        "  exit 0\n"
        "fi\n"
        f"printf 'Reading specs from {musl}/musl-gcc.specs\\n' >&2\n"
    )
    compiler.chmod(0o755)
    return compiler


def _declared() -> dict[str, str]:
    declaration = json.loads(DECLARATION.read_text(encoding="utf-8"))
    return {
        "c_compiler_version": declaration["c_compiler_version"],
        "musl_version": declaration["musl_version"],
    }


def test_describe_reports_the_compiler_version_line_and_musl_library_version(tmp_path: Path):
    compiler = _fake_musl_compiler(
        tmp_path, version_line="x86_64-linux-gnu-gcc (Test 1.0) 1.0", musl_version="9.9.9"
    )

    completed = _run("describe", str(compiler))

    assert completed.returncode == 0, completed.stderr
    assert json.loads(completed.stdout) == {
        "c_compiler_version": "x86_64-linux-gnu-gcc (Test 1.0) 1.0",
        "musl_version": "9.9.9",
    }


def test_describe_refuses_a_compiler_without_a_musl_specs_file(tmp_path: Path):
    compiler = tmp_path / "cc"
    compiler.write_text("#!/bin/sh\necho 'cc (Test) 1.0'\n")
    compiler.chmod(0o755)

    completed = _run("describe", str(compiler))

    assert completed.returncode == 2
    assert "musl specs" in completed.stderr


def test_check_accepts_exactly_the_declared_toolchain():
    completed = _run("check", "-", stdin=json.dumps(_declared()))

    assert completed.returncode == 0, completed.stderr


def test_check_refuses_the_runner_image_toolchain_that_changed_the_tagged_broker():
    # The v0.7.4 tag workflow ran on a newer runner image whose compiler and musl differed from
    # the pull-request build of the same source, and produced different broker bytes.
    drifted = {
        "c_compiler_version": "x86_64-linux-gnu-gcc (Ubuntu 15.2.0-16ubuntu1) 15.2.0",
        "musl_version": "1.2.5",
    }

    completed = _run("check", "-", stdin=json.dumps(drifted))

    assert completed.returncode == 1
    assert "c_compiler_version: declared" in completed.stderr
    assert "musl_version: declared" in completed.stderr


def test_check_refuses_provenance_that_does_not_record_the_musl_version(tmp_path: Path):
    provenance = tmp_path / "agcoord-broker.provenance.json"
    provenance.write_text(json.dumps({"c_compiler_version": _declared()["c_compiler_version"]}))

    completed = _run("check", str(provenance))

    assert completed.returncode == 1
    assert "musl_version: declared" in completed.stderr


def test_release_verifier_refuses_a_broker_built_with_an_undeclared_toolchain(tmp_path: Path):
    provenance = tmp_path / "agcoord-broker.provenance.json"
    provenance.write_text(json.dumps({**_declared(), "musl_version": "1.2.5"}))

    with pytest.raises(RELEASE["CandidateError"], match="native/build-toolchain.json"):
        RELEASE["_declared_c_toolchain"](provenance)

    provenance.write_text(json.dumps(_declared()))
    RELEASE["_declared_c_toolchain"](provenance)


def test_the_toolchain_declaration_is_part_of_the_native_source_identity(tmp_path: Path):
    copy = tmp_path / "source"
    for relative in (
        "Cargo.lock",
        "Cargo.toml",
        "LICENSE",
        "rust-toolchain.toml",
        "native/THIRD_PARTY_LICENSES.tsv",
        "native/build-toolchain.json",
    ):
        (copy / relative).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ROOT / relative, copy / relative)
    shutil.copytree(ROOT / "native/agcoord-broker/src", copy / "native/agcoord-broker/src")
    shutil.copy2(ROOT / "native/agcoord-broker/Cargo.toml", copy / "native/agcoord-broker/Cargo.toml")
    (copy / "scripts").mkdir()
    for script in (ROOT / "scripts").glob("*native*"):
        shutil.copy2(script, copy / "scripts" / script.name)

    def source_id() -> str:
        return subprocess.run(
            [str(copy / "scripts/native-source-id")],
            text=True,
            capture_output=True,
            check=True,
        ).stdout.strip()

    before = source_id()
    declaration = json.loads((copy / "native/build-toolchain.json").read_text(encoding="utf-8"))
    declaration["musl_version"] = "1.2.5"
    (copy / "native/build-toolchain.json").write_text(json.dumps(declaration))

    assert source_id() != before


def test_build_refuses_an_undeclared_toolchain_before_compiling(tmp_path: Path):
    rustc = shutil.which("rustc")
    if rustc is None or not subprocess.run(
        [rustc, "--version"], cwd=ROOT, text=True, capture_output=True, check=False
    ).stdout.startswith("rustc 1.94.1 "):
        pytest.skip("the pinned Rust 1.94.1 toolchain is unavailable")
    compiler = _fake_musl_compiler(
        tmp_path,
        version_line="x86_64-linux-gnu-gcc (Ubuntu 15.2.0-16ubuntu1) 15.2.0",
        musl_version="1.2.5",
    )
    environment = {
        key: value
        for key, value in os.environ.items()
        if key != "AGCOORD_MUSL_CC"
    }
    environment["PATH"] = f"{compiler.parent}:{environment['PATH']}"

    completed = subprocess.run(
        [str(ROOT / "scripts/build-native-broker"), str(tmp_path / "dist")],
        cwd=ROOT,
        env=environment,
        text=True,
        capture_output=True,
        check=False,
    )

    assert completed.returncode == 2, completed.stderr
    assert "declared in native/build-toolchain.json" in completed.stderr
    assert not (tmp_path / "dist").exists()
