# AGCoord

<img src="https://raw.githubusercontent.com/xu-hao/agcoord/main/docs/assets/agcoord-gourd-mascot.png" alt="Golden botanical AGCoord gourd with a curled green stem and leaf" width="240">

AGCoord is a machine-local coordinator for developers and coding agents that share a
workstation. It gives every check, standalone full gate, and atomic gate-and-publication
request one durable job ID, then schedules compatible work across repositories without
letting two agents accidentally publish stale or untested code.

The coordinator is local infrastructure: one detached broker per OS user, a private durable
spool, per-job logs, and an optional terminal UI. It does not require a hosted service. The
core package is forge-neutral; GitHub support is an optional adapter.

## Install and start

Install the published Python client in a tool environment first, then install the exact matching
native-host bundle. Replace `RELEASE_VERSION` with the version attached to the bundle and keep
the package, all four sidecars, and all three helpers together in one owner-only directory:

```bash
version=RELEASE_VERSION
python -m pip install "agcoord==$version"
chmod 0700 /path/to/native-host-bundle
agc host install /path/to/native-host-bundle/agcoord-native-host-x86_64-linux.tar.gz
```

`agc host install` verifies the complete bundle, creates or validates the default managed
configuration, performs the privileged activation, enables and starts the user service, checks
the installed identity, and submits an enforced one-CPU proof. Upgrade in the same client-first
order:

```bash
python -m pip install --upgrade "agcoord==$version"
agc host upgrade /path/to/native-host-bundle/agcoord-native-host-x86_64-linux.tar.gz
```

The low-level commands and failure recovery contract remain in the
[native host runbook](docs/native_host.md).

The client refuses to search `PATH` or fall back to the old Python broker. Source developers may
select an absolute current-user-owned development build with the documented
[`native_broker` configuration](docs/native_broker.md#executable-discovery); release installs
require the root-owned static artifact.
Production installation and upgrades use the staged package, long-lived user service, and
AppArmor policy; upgrade activation waits for a durable drain and never restarts a broker while
queued or running work remains.

The base package installs the supported Textual 8 release line (`textual>=8.2,<9`) for the
terminal UI. Textual 1 through 7 are not supported; a future Textual major is admitted only
after its real-TUI behavior is validated.

### Upgrading to 0.4.0

Version 0.4.0 removes the implicit unbounded scratch directory. A run that declares neither a
complete tmpfs policy nor a complete project-quota policy now receives no AGCoord-provided
temporary directory, and its inherited `TMPDIR`, `TMP`, and `TEMP` values are removed. Before
upgrading, audit admitted commands that relied on a private temporary directory and give each
one an explicit tmpfs or project-quota declaration; the
[native migration runbook](docs/native_migration.md) records that audit step. The host
transition itself is unchanged: install the matching 0.4 client, then run `agc host upgrade`
with the verified 0.4 bundle.

If upgrading from 0.2.x or earlier, first apply the 0.3 native-owner transition below.

### Upgrading to 0.3.0

Version 0.3.0 replaces the production Python queue owner with the fixed, statically linked Rust
broker and durable protocol 5. Keep the old client and state backup through a tested rollback
window. Install the matching client, run `agc drain` to atomically close submissions while
accepted work finishes, and retain its exact drain ID. Then install and activate the matching
host bundle with that ID, rehearse migrate/rollback against a copy, migrate the guarded live
spool explicitly, run `agc resume DRAIN_ID`, and start the managed service. The complete
commands, compatibility matrix, refusal modes, and rollback procedure are in the
[native migration runbook](docs/native_migration.md). Neither package installation nor service
activation changes the spool implicitly.

If upgrading directly from 0.1.x, first apply the 0.2 command and configuration changes below.

Version 0.2.0 removes the `agcoord` console executable. Replace downstream shell commands,
service units, and automation with `agc`; the PyPI project, Python import, stable state-directory
name, and `python -m agcoord` entry point remain `agcoord`.

Let every 0.1.x job finish and move capacity, resource
binding, and cgroup-root values from `AGCOORD_CAPACITIES`, `AGCOORD_RESOURCE_BINDINGS`, and
`AGCOORD_CGROUP_ROOT` into that state directory's single `config.json`; those three environment
variables and the old comma-separated capacity syntax are no longer accepted. The 0.3 migration
preserves historical meaning and never reinterprets legacy resource names as enforced limits.

With the production host package configured, the first command asks systemd to start the
long-lived user service; later shells and repositories join the same user-scoped coordinator.
Explicit development binaries retain detached on-demand startup:

```bash
agc list
agc tui
```

State defaults to `${XDG_STATE_HOME:-~/.local/state}/agcoord`. Set `AGCOORD_STATE_DIR` or pass
`--state-dir` to use a deliberate alternate spool for an unmanaged coordinator; the fixed
managed service and `agc host` operations accept only the default state. With no configuration,
capacity defaults to two concurrent job slots. A fresh `agc host install` instead records the
process's available CPU-affinity count as both `cpu` and `jobs` capacity and requires cgroup-v2
CPU enforcement. One JSON file, `config.json` in the state directory, configures the broker that
owns it:

```json
{"capacities": {"jobs": 4, "cpu": 8, "browser": 1}, "database_timeout": 10}
```

`database_timeout` is the optional positive SQLite lock-wait limit in seconds and defaults to
10. Current-protocol spools use WAL mode automatically so ordinary readers do not block behind
writers; transient broker-pump and idle-check contention is retried.

## Run work

Submit focused checks with the resources they consume:

```bash
agc run --label "unit tests" --resource cpu=2 -- python -m pytest -q
```

Every job implicitly holds one `jobs` slot. Repeatable `--resource` options add only named,
configured resources; unknown or impossible requests fail instead of waiting forever.
Those names are admission accounting by default. Optional `bindings` entries in the same
`config.json` give selected names explicit units and `admission-only`, `best-effort`, or
`required` backend semantics; every run then reports what was requested, actually applied, and
measured. See the
[resource contract](docs/coordinator.md#repository-lanes-and-resources) for the strict binding
shape and current backend availability.

On Linux, the built-in `cgroup-v2` backend can own the complete descendant lifecycle for an
explicitly delegated, namespace-safe cgroup root. It attaches the blocked launcher before user
code, kills detached descendants on finish or cancellation, and recovers ownership across broker
restart. Typed `cpu/logical-cpu` and `processes/processes` bindings add aggregate `cpu.max` and
`pids.max` limits plus peak and violation reporting; they do not imply CPU affinity. Memory and
swap envelopes, bounded temporary storage, and verified per-device block-I/O limits remain
separate opt-in contracts. See
[delegated cgroup setup](docs/coordinator.md#delegated-cgroup-v2-lifecycle) before enabling a
required binding.

Scratch is opt-in: a run that declares neither a complete tmpfs policy nor a complete
project-quota policy receives no AGCoord-provided temporary directory, and inherited `TMPDIR`,
`TMP`, and `TEMP` values are removed. Jobs that need accounted temporary storage must declare one
of those providers explicitly.

If a gate starts several worker-owning tools concurrently, admitted subprocesses can use
the public Python [child CPU lease API](docs/coordinator.md#child-cpu-leases-for-parallel-tools)
to divide the job's declared CPU budget fairly. Leases support exact or partial grants,
waiting, cancellation, crash reclamation, and broker recovery without creating nested jobs.
Install `agcoord[xdist]` to activate the optional
[pytest-xdist adapter](docs/coordinator.md#optional-pytest-xdist-adapter): positive `-n` modes
then lease their worker count inside admitted runs, while plain pytest, `-n 0`, and pytest
outside AGCoord keep their upstream behavior.

Run a standalone full validation for an exact clean Git head when publication is not part of
the request:

```bash
agc full --label "release gate" --resource cpu=4 -- ./scripts/test.sh
```

`full` records the checkout's full 40-character `HEAD`, checks that the worktree is clean,
and establishes a barrier in that repository's lane. It remains useful for validation and
release preparation, but normal landing does not compose a full row with a later publication
row. It is not a machine-global lock: compatible work in other repositories can overlap when
configured resource capacities allow it.

After pushing the exact clean head and opening a pull request, gate and publish it as one
indivisible request:

```bash
agc land 123 \
  --label "gate and publish PR 123" \
  --resource cpu=4 \
  -- ./scripts/test.sh
# GitHub is the convenience default and may also be named explicitly.
agc land 123 --adapter github -- ./scripts/test.sh
# Opt out when the request must fail instead of merging an advanced target.
agc land 123 --no-target-sync -- ./scripts/test.sh
```

`land` stores the adapter, request, exact checkout/head, gate command, caller environment,
and resource claim in one durable repository barrier. The core record keeps adapter and
request separate even though the current installed adapter uses GitHub pull-request numbers.
If the target advanced while a same-repository request waited, the default GitHub adapter makes
one ordinary merge commit from the current target into the unchanged request branch, pushes it
with an exact lease, and records that commit as the durable head before running the gate. It
then runs the gate once and publishes immediately after a green result without releasing the
lane or resources. A red gate publishes nothing.
`--adapter github` is the default when the option is omitted; the core request remains
forge-neutral.

Target synchronization never rebases or rewrites existing commits. A merge conflict is aborted
and reported before the gate, with the checkout restored cleanly. A concurrent source change,
failed lease-protected push, target movement during the gate, or changed post-gate observation
ends with a named handback and does not update the target. Use `--no-target-sync` when even the
pre-gate source merge is unwanted. A separate full-plus-merge sequence is not a landing
substitute.

Inspect or manage jobs from any terminal:

```bash
agc list
agc show land-0123456789ab
agc log land-0123456789ab --follow
agc cancel land-0123456789ab
agc clear
# For planned maintenance:
agc drain --reason "native host upgrade"
agc resume drain-0123456789ab
```

`clear` removes terminal history and its logs only. It refuses while queued or running work
exists and never removes the spool, broker ownership, or migration history.
`drain` durably and atomically refuses new submissions without cancelling work already admitted.
It waits for those rows—including an authoritative land publication—to become terminal and for
the broker to yield ownership. `list`, `show`, `log`, the TUI, and explicit cancellation remain
available. Save the returned `drain-…` ID: only `resume` with that exact ID reopens submissions.

The full operating contract, recovery behavior, TUI keys, and resource model are in
[the coordinator guide](docs/coordinator.md). Package maintainers should also read
[the release guide](docs/releasing.md), and published user-facing changes are recorded in
[the changelog](CHANGELOG.md). Contributors follow the repository workflow in
[AGENTS.md](AGENTS.md).

## Project status

AGCoord is distributed as the `agcoord` project on PyPI, with import package `agcoord` and
the `agc` console command. The `python -m agcoord` module entry point remains available.
Build and install development checkouts in an isolated environment rather than copying
modules into another project.

The gourd mascot and its asset notes live under [docs/assets](docs/assets/README.md).
