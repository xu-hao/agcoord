# Packaging and release

AGCoord is a standalone Python distribution. Its PyPI project, installed distribution,
import package, and module entry point are named `agcoord`; its installed console command is
`agc`:

```text
pip install agcoord
import agcoord
python -m agcoord --help
agc --help
```

The source uses the `src/agcoord/` layout and tests live in `tests/`. Runtime code must not
import a parent application or assume a sibling source checkout. A wheel must work in a
clean environment with no project checkout on `PYTHONPATH`.

## Native broker artifact

The native broker is a separate Linux release artifact, not a copied Python interpreter or a
file silently injected into a universal Python wheel. The currently supported release matrix
has one target: `x86_64-unknown-linux-musl`. Other architectures remain unsupported until they
have a native CI runner and the same kernel conformance coverage.

Build inputs are pinned by `rust-toolchain.toml`, `Cargo.lock`, exact direct dependency
requirements, and the source digest produced by `scripts/native-source-id`. The artifact embeds
that digest as its build identity and reports protocol, implementation, target, and bundled
SQLite version through `identity --json`. Install Rust 1.94.1 with the declared musl target and
Ubuntu's `musl-tools`, then run:

```bash
./scripts/build-native-broker
./scripts/audit-native-broker \
  dist/native/agcoord-broker-x86_64-unknown-linux-musl
./scripts/check-native-licenses \
  dist/native/agcoord-broker-x86_64-unknown-linux-musl
./scripts/check-native-reproducible
```

The build emits the executable, a SHA-256 sidecar, and JSON provenance recording the artifact
and source digests, Rust and Cargo versions, C compiler, target, protocol, and source epoch. The
audit requires a static or static-PIE ELF, no program interpreter, no `DT_NEEDED` entry, a valid
checksum, and an exact release identity from an empty environment. Bundled SQLite is part of
that ELF. The dependency graph must exactly match `native/THIRD_PARTY_LICENSES.tsv`, use only
the crates.io registry or workspace source, and contain only the reviewed license expressions.

Reproducibility CI builds two fresh checkout copies with separate Cargo target directories,
path remapping, disabled incremental compilation, a fixed source epoch, and the same recorded
toolchains; their executable bytes must match. This proves reproducibility for the declared
runner and compiler inputs. It does not claim byte identity across different C compilers or
host toolchain builds. `AGCOORD_MUSL_CC=cc` is an explicit local compatibility escape hatch for
an audited x86_64 compiler when `musl-gcc` cannot be installed; release CI never uses it.

Building the artifact does not activate it. Host packaging installs the verified executable at
`/usr/libexec/agcoord/agcoord-broker`, and a development configuration may explicitly select
another absolute path with `native_broker.allow_development=true`. Python clients verify the
regular-file mode and ownership plus the exact `identity --json` version, protocol,
implementation, build, and target before startup and commands. They never search `PATH`, accept
a symlink, or silently fall back to the Python broker. The Python wheel deliberately contains
no copied interpreter or native executable.

## Dependency posture

Queue ownership, durable state, scheduling, process supervision, and the forge-neutral
atomic-landing protocol belong to the core distribution. Forge credentials and SDKs do not.
GitHub is an optional adapter and `gh` is not required to import or use the core coordinator.
Textual is a base dependency because the installed `agc tui` command is part of the core
interface. The supported framework line is `textual>=8.2,<9`; older releases have no
compatibility promise, and a new Textual major requires explicit real-TUI validation before the
upper bound moves. Whichever install extras the package declares are also part of the published
interface and must be smoke-tested from the built wheel.

## Release checklist

1. Start from a clean release commit and select the version once in package metadata.
   Update the matching section in [the changelog](../CHANGELOG.md) with the release date and
   user-visible changes.
2. Run the complete test suite, including generic multi-repository scheduling, one-row
   exact-head gate-and-publication behavior, recovery/cancellation boundaries, the optional
   GitHub adapter, child CPU lease contention and recovery, and real terminal UI coverage. A
   standalone `agc full` may validate a release candidate, but repository changes must land
   with `agc land` so the verdict and publication cannot be separated.
   Run the deterministic cgroup lifecycle suite on every host; when an exclusive delegated v2
   subtree is available, also set `AGCOORD_TEST_CGROUP_ROOT` and require the opt-in kernel tests
   to prove namespace-root protection, aggregate CPU throttling, PID exhaustion, terminal
   metrics, and complete cleanup. On an init-namespace-root host, set
   `AGCOORD_TEST_CGROUP_IO=1` for the test-owned loop-device bandwidth and IOPS checks.
3. Build both Python artifacts with `python -m build` and validate them with
   `python -m twine check dist/*`. Build and audit the native artifact with the commands above;
   retain its executable, checksum, provenance, and reviewed license inventory together.
4. Create a fresh virtual environment outside the checkout. Install the wheel, with each
   supported optional extra in at least one smoke environment, and exercise both
   `python -m agcoord --help` and `agc --help`. Verify that no `agcoord` console executable
   is installed. For the `xdist` extra, verify pytest discovers the `agcoord-xdist` entry point,
   plain pytest remains serial, and an admitted `-n auto` run starts its leased worker count.
5. In that clean environment, start a temporary explicit state directory, run a check, read
   its log, clear terminal history, and verify that importing the core does not require a
   forge executable or credentials.
6. Record both artifact hashes, then upload those exact files directly to production PyPI
   with `python -m twine upload --repository pypi <wheel> <sdist>`. Twine reads the existing
   `pypi` login from `~/.pypirc`; keep that file owner-only, use a project-scoped API token,
   and never place the token in a repository, command line, or long-lived shell variable.
7. Read the production PyPI JSON metadata for the new version. Require exactly the expected
   wheel and source archive, compare their SHA-256 values with the local records, and install
   `agcoord==<version>` from `https://pypi.org/simple/` into another clean environment.
8. Tag the exact source commit as `v<version>` and push only that tag. The tag workflow
   independently rebuilds and smoke-tests the tagged source without publishing it. Require
   that workflow to pass, then create the GitHub release and attach the same local wheel and
   source archive that production PyPI accepted.

The release workflow should fail closed if artifact versions differ, files are dirty, a tag
does not match the declared version, or the wheel exposes a package name other than `agcoord`
or a console command other than `agc`. Publishing remains an explicit maintainer action
through Twine; GitHub Actions has no package-index credentials or deployment job. Releases
never migrate a user's live spool implicitly; protocol changes require the explicit
`agc migrate` runbook in
[the coordinator guide](coordinator.md#migrations).
