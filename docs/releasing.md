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

## Dependency posture

Queue ownership, durable state, scheduling, process supervision, and the forge-neutral
atomic-landing protocol belong to the core distribution. Forge credentials and SDKs do not.
GitHub is an optional adapter and `gh` is not required to import or use the core coordinator.
The TUI may use an install extra if its third-party runtime is not kept in the base package;
whichever extra names the package declares are part of the published interface and must be
smoke-tested from the built wheel.

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
3. Build both artifacts with `python -m build` and validate them with
   `python -m twine check dist/*`.
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
