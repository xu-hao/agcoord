# AGCoord

<img src="https://raw.githubusercontent.com/xu-hao/agcoord/main/docs/assets/agcoord-gourd-mascot.png" alt="AGCoord gourd mascot" width="240">

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

There is no separate service-install step. The first command starts the detached broker on
demand; later shells and repositories join the same user-scoped coordinator:

```bash
agcoord list
agcoord tui
```

State defaults to `${XDG_STATE_HOME:-~/.local/state}/agcoord`. Set
`AGCOORD_STATE_DIR` or pass `--state-dir` to use a deliberate alternate spool.
Machine capacity defaults to two concurrent job slots. Configure named resources before the
broker starts with either `AGCOORD_CAPACITIES='jobs=4,cpu=8,browser=1'` or an equivalent JSON
object.

## Run work

Submit focused checks with the resources they consume:

```bash
agcoord run --label "unit tests" --resource cpu=2 -- python -m pytest -q
```

Every job implicitly holds one `jobs` slot. Repeatable `--resource` options add only named,
configured resources; unknown or impossible requests fail instead of waiting forever.

Run a standalone full validation for an exact clean Git head when publication is not part of
the request:

```bash
agcoord full --label "release gate" --resource cpu=4 -- ./scripts/test.sh
```

`full` records the checkout's full 40-character `HEAD`, checks that the worktree is clean,
and establishes a barrier in that repository's lane. It remains useful for validation and
release preparation, but normal landing does not compose a full row with a later publication
row. It is not a machine-global lock: compatible work in other repositories can overlap when
configured resource capacities allow it.

After pushing the exact clean head and opening a pull request, gate and publish it as one
indivisible request:

```bash
agcoord land 123 \
  --label "gate and publish PR 123" \
  --resource cpu=4 \
  -- ./scripts/test.sh
# GitHub is the convenience default and may also be named explicitly.
agcoord land 123 --adapter github -- ./scripts/test.sh
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
publish. Update the branch yourself, push it, and submit a fresh `agcoord land` request; a
separate full-plus-merge sequence is not a landing substitute.

Inspect or manage jobs from any terminal:

```bash
agcoord list
agcoord show land-0123456789ab
agcoord log land-0123456789ab --follow
agcoord cancel land-0123456789ab
agcoord clear
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
both `agcoord` and `python -m agcoord` command forms. Build and install development checkouts
in an isolated environment rather than copying modules into another project.

The gourd mascot and its asset notes live under [docs/assets](docs/assets/README.md).
