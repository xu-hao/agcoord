# Packaging and release

AGCoord is a standalone Python distribution. Its PyPI project, installed distribution,
import package, module entry point, and console command are all named `agcoord`:

```text
pip install agcoord
import agcoord
python -m agcoord --help
agcoord --help
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
   GitHub adapter, and real terminal UI coverage. A standalone `agcoord full` may validate a
   release candidate, but landing repository changes uses `agcoord land` so the verdict and
   publication cannot be separated.
3. Build both artifacts with `python -m build` and validate them with
   `python -m twine check dist/*`.
4. Create a fresh virtual environment outside the checkout. Install the wheel, with each
   supported optional extra in at least one smoke environment, and exercise both
   `python -m agcoord --help` and `agcoord --help`.
5. In that clean environment, start a temporary explicit state directory, run a check, read
   its log, clear terminal history, and verify that importing the core does not require a
   forge executable or credentials.
6. Dispatch the TestPyPI workflow for the intended release commit. Install that run's exact
   version in another clean environment and complete the command/state smoke before tagging;
   do not dispatch another successful build for the same commit after selecting the artifact
   authority.
7. Tag that exact commit and publish through the identity-bound trusted publisher. The tag
   workflow selects the newest successful TestPyPI dispatch for the exact tag SHA, downloads
   its retained `testpypi-distributions` artifact, repeats Twine and clean-install checks, and
   promotes those same wheel/sdist bytes. It fails closed when no matching successful run or
   retained artifact exists. Do not keep a long-lived upload token in a repository or shell.

The release workflow should fail closed if the TestPyPI authority is absent, artifact versions
differ, files are dirty, a tag does not match the declared version, or the wheel exposes a
command/package name other than `agcoord`. Releases never migrate a user's live spool
implicitly; protocol changes require the explicit `agcoord migrate` runbook in
[the coordinator guide](coordinator.md#migrations).
