# Contributor workflow

This guide contains the repository procedure summarized by
[AGENTS.md](../AGENTS.md). The [coordinator guide](coordinator.md) remains authoritative
for scheduling and landing behavior, and the [release guide](releasing.md) remains
authoritative for package releases.

## Track and isolate the change

Before changing code, tests, documentation, packaging, or live state, create a
descriptive issue. Whenever reporting its number, include a sentence that explains the
work it tracks.

Create an issue-specific branch and Git worktree from `main`, then make and validate the
change there. Do not develop in the primary checkout. Preserve unrelated edits, and
never inspect or clean another contributor's worktree, coordinator state, or temporary
resources.

Keep the coordinator core forge-neutral. Forge-specific metadata, credentials, and
publication behavior belong in optional adapters.

## Test behavior and document contracts

Reproduce a bug with a failing behavioral test before changing runtime code. Exercise
public APIs, commands, subprocesses, Git repositories, and the real TUI as appropriate;
assertions against source code or documentation text do not prove runtime behavior.

Every runtime change needs focused tests and updates to affected canonical
documentation. Register new canonical documents in
[the documentation index](index.md), and record published user-facing changes in
[the changelog](../CHANGELOG.md). Tests must own and stop every broker, worker,
repository, and temporary state they start.

## Coordinate validation

Once AGCoord is installed, submit every check through the local coordinator and declare
each scarce resource it consumes:

```bash
agc run \
  --label "focused tests" \
  --resource cpu=1 \
  -- python -m pytest -q tests/test_area.py
agc full \
  --label "standalone full validation" \
  --resource cpu=4 \
  -- python -m pytest -q
```

When one admitted gate starts multiple worker-owning tools concurrently, each controller
must acquire a [child CPU lease](coordinator.md#child-cpu-leases-for-parallel-tools) and use
the granted count. Do not give every tool the gate's complete CPU allocation through one
gate-wide environment value. Tool-specific translation, such as mapping a lease to a test
runner's worker option, belongs in that tool's adapter rather than the coordinator core. The
built-in optional [pytest-xdist adapter](coordinator.md#optional-pytest-xdist-adapter) applies
this contract for positive `-n` modes; use `--maxprocesses` when several automatic controllers
should reserve room for one another.

Use `agc full` from a clean checkout when an exact-head verdict is useful
independently of publication. It is a barrier for its repository, not an undeclared
machine-global lock. Do not invoke `agc`, a gate wrapper, or publication from inside
an admitted AGCoord job; nested submissions are rejected to prevent self-deadlock.

In a fresh AGCoord development environment, direct package installation and focused
tests are allowed only long enough to make `agc run` available. Bootstrap is never
an exception for a direct full gate or publication.

## Land and clean up

Push the ticket branch and open its publication request. Then use one landing request
whose gate validates that exact clean 40-character head and publishes it without
releasing the repository lane or declared resources:

```bash
agc land <request> \
  --label "gate and publish" \
  --resource cpu=4 \
  -- python -m pytest -q
```

Do not use a direct target-branch push, a forge merge command, a separate
full-plus-merge sequence, or any equivalent path that separates the landing verdict
from publication.

A stale-target or changed-head refusal hands the work back to the contributor. Update
the branch explicitly, push it, and submit a fresh `agc land` request. AGCoord does
not refresh, rebase, or rewrite the worktree, and a moved reference invalidates the
earlier gate result.

Landing repository work does not authorize a package upload. Upload an AGCoord
distribution to PyPI only when the user explicitly requests it; permission to implement,
test, commit, push, open or merge a pull request, tag, or create a GitHub release is not
upload permission. Follow the [release guide](releasing.md) for the exact procedure.

Remove the merged ticket worktree and branch only after publication succeeds.
