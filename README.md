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

Install the published package in a tool environment:

```bash
python -m pip install agcoord
```

The base package installs the supported Textual 8 release line (`textual>=8.2,<9`) for the
terminal UI. Textual 1 through 7 are not supported; a future Textual major is admitted only
after its real-TUI behavior is validated.

### Upgrading from 0.1.x

Version 0.2.0 removes the `agcoord` console executable. Replace downstream shell commands,
service units, and automation with `agc`; the PyPI project, Python import, stable state-directory
name, and `python -m agcoord` entry point remain `agcoord`.

Before upgrading, let every 0.1.x job finish, wait for its on-demand broker to relinquish the
idle spool, and back up the state directory when its history matters. Move capacity, resource
binding, and cgroup-root values from `AGCOORD_CAPACITIES`, `AGCOORD_RESOURCE_BINDINGS`, and
`AGCOORD_CGROUP_ROOT` into that state directory's single `config.json`; those three environment
variables and the old comma-separated capacity syntax are no longer accepted. After installing
0.2.0, run `agc migrate` once for the idle default spool (or
`agc --state-dir /path/to/state migrate`), then use `agc list` to start the new broker. Migration
preserves historical meaning and never reinterprets legacy resource names as enforced limits.

There is no separate service-install step. The first command starts the detached broker on
demand; later shells and repositories join the same user-scoped coordinator:

```bash
agc list
agc tui
```

State defaults to `${XDG_STATE_HOME:-~/.local/state}/agcoord`. Set
`AGCOORD_STATE_DIR` or pass `--state-dir` to use a deliberate alternate spool.
Machine capacity defaults to two concurrent job slots. One JSON file, `config.json` in the
state directory, configures the broker that owns it:

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
```

`land` stores the adapter, request, exact checkout/head, gate command, caller environment,
and resource claim in one durable repository barrier. The core record keeps adapter and
request separate even though the current installed adapter uses GitHub pull-request numbers.
It rejects a stale target before the gate, runs the gate once, and publishes immediately
after a green result without releasing the lane or resources. A red gate publishes nothing.
`--adapter github` is the default when the option is omitted; the core request remains
forge-neutral.

AGCoord never refreshes, rebases, or rewrites the checkout. If the source head or target
branch moves before or during the gate, the same job ends with a named handback and does not
publish. Update the branch yourself, push it, and submit a fresh `agc land` request; a
separate full-plus-merge sequence is not a landing substitute.

Inspect or manage jobs from any terminal:

```bash
agc list
agc show land-0123456789ab
agc log land-0123456789ab --follow
agc cancel land-0123456789ab
agc clear
```

`clear` removes terminal history and its logs only. It refuses while queued or running work
exists and never removes the spool, broker ownership, or migration history.

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
