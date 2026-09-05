# Contributor workflow

This guide contains the repository procedure summarized by
[the contributor guardrails](guardrails.md). The [coordinator guide](coordinator.md) remains authoritative
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
[the changelog](changelog.md). Tests must own and stop every broker, worker,
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

The Python suite drives a test-owned native broker rather than an in-process reference.
`tests/conftest.py` builds it on first use with `cargo build --locked -p agcoord-broker` and
stages one private mode-0755 copy for the session, so a fresh checkout needs the pinned Rust
toolchain; set `AGCOORD_TEST_NATIVE_BROKER` to an absolute prebuilt executable to skip the
build. Every test starts and stops the brokers it creates.

When one admitted gate starts multiple worker-owning tools concurrently, each controller
must acquire a [child CPU lease](coordinator.md#child-cpu-leases-for-parallel-tools) and use
the granted count. Do not give every tool the gate's complete CPU allocation through one
gate-wide environment value. Tool-specific translation, such as mapping a lease to a test
runner's worker option, belongs in that tool's adapter rather than the coordinator core. The
built-in optional [pytest-xdist adapter](coordinator.md#optional-pytest-xdist-adapter) applies
this contract for positive `-n` modes; use `--maxprocesses` when several automatic controllers
should reserve room for one another.

The cgroup lifecycle and compute suites use real subprocesses with a deterministic, test-owned
kernel seam by default, so they are safe on hosts whose cgroup hierarchy is not delegated. To
exercise the real kernel boundary and CPU/PID controllers, provision an exclusive disposable
subtree that satisfies the
[delegated cgroup contract](coordinator.md#delegated-cgroup-v2-lifecycle), set
`AGCOORD_TEST_CGROUP_ROOT` to it, run the test process inside that delegation boundary, and run
`tests/test_cgroup.py tests/test_cgroup_compute.py`. The tests remove only their own owner and run
leaves; they never create, change ownership of, or remove the configured root. Exercise the Rust
owner against the same delegation with:

```bash
AGCOORD_TEST_CGROUP_ROOT=/sys/fs/cgroup/path/to/exclusive-delegation \
  agc run --label "native real cgroup tests" --resource cpu=1 -- \
  cargo test -p agcoord-broker --test scheduler real_cgroup_ --offline
```

To additionally exercise block-I/O enforcement as init-namespace root, set
`AGCOORD_TEST_CGROUP_IO=1`. The Python and Rust I/O tests each format and mount their own
loop-backed ext4 image, run buffered and direct workloads, and unmount only that test-owned
filesystem. The native test covers symmetric and directional bandwidth and IOPS units plus I/O
weight.

Exercise the native persistent-scratch owner against a real ext4 project-quota filesystem with:

```bash
AGCOORD_TEST_PROJECT_QUOTA=1 \
  agc run --label "native real project-quota tests" --resource cpu=1 -- \
  cargo test -p agcoord-broker --test scheduler \
  real_ext4_project_quota_enforces_bytes_inodes_and_parallel_identity --offline
```

Run this opt-in test as init-namespace root on an exclusive disposable host with `mkfs.ext4`,
`mount`, and `umount` available. It owns a loop-backed image and mount, proves byte and inode
exhaustion, parallel project identity, cleanup, and replacement-broker crash recovery, and
unmounts only its test-owned filesystem.

Use `agc full` from a clean checkout when an exact-head verdict is useful
independently of publication. It is ordinary lane work, not a barrier or an undeclared
machine-global lock. Do not invoke `agc`, a gate wrapper, or publication from inside
an admitted AGCoord job; nested submissions are rejected to prevent self-deadlock.

The canonical native release check is the versioned
[cross-implementation conformance gate](conformance.md):

```bash
agc land <request> \
  --label "conformance gate and publish" \
  --resource cpu=4 \
  -- ./scripts/check-conformance
```

The checker validates collected native selectors, builds the development broker with four
Rust build jobs, then runs both complete suites with one pytest worker and one Rust test thread
so process-lifecycle tests retain exclusive ownership of every broker and child they create. Do
not replace it in a native release path with an unversioned `cargo test` invocation.

The final release-artifact boundary is `scripts/verify-release-candidate`. It accepts only the
two clean Python distributions, the exact five-file native artifact set, and the exact
eight-file host artifact set produced from one clean versioned commit. It re-audits every
identity and sidecar, installs the wheel and sdist into fresh environments, runs the migration
and rollback rehearsal against the installed wheel, and only then writes an aggregate manifest
and `SHA256SUMS`. The CI and tag workflows download the independently built inputs and run this
same verifier; missing coverage or any artifact is a closed gate, not a manual checklist waiver.
The verifier owns temporary state and never targets the default coordinator.

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

By default, a stale target is merged mechanically into an otherwise unchanged
same-repository request branch before the gate. The exact lease-protected push becomes the
durable head that is gated; AGCoord does not rebase or rewrite existing commits. Pass
`--no-target-sync` when the request requires a stale-target refusal instead. A merge conflict,
concurrent source change, or target movement during the gate hands the work back to the
contributor and never reuses the earlier verdict.

If the target branch was deliberately rewritten to remove a commit, do not land any request
branch that still reaches it, and do not rely on target synchronization to make it safe. Store the
removed commit with `agc avoid SHA --reason ...` so every later landing on the machine refuses to
publish anything that reaches it, then rebuild the request as a fresh branch from the current
target and land it with `--no-target-sync`. The coordinator guide describes the
[avoided-commit refusal](coordinator.md#avoided-commits-after-a-target-rewrite).

Landing repository work does not authorize a package upload. Upload an AGCoord
distribution to PyPI only when the user explicitly requests it; permission to implement,
test, commit, push, open or merge a pull request, tag, or create a GitHub release is not
upload permission. Follow the [release guide](releasing.md) for the exact procedure.

Remove the merged ticket worktree and branch only after publication succeeds.
