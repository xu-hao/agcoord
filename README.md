# AGCoord

<img src="https://raw.githubusercontent.com/xu-hao/agcoord/main/docs/assets/agcoord-gourd-mascot.png" alt="Golden botanical AGCoord gourd with a curled green stem and leaf" width="240">

**Local CI and merge queue for coding agents that share one machine.**

When several coding agents share one workstation, nothing coordinates them. They compete for
the machine, so test runs time out or get OOM-killed for reasons that have nothing to do with
the code. They merge on stale evidence, so `main` breaks from changes that were never tested
together. And they leave no shared record of what ran, on which head, with what result.

AGCoord is the missing layer. One detached broker per OS user owns a queue for every
repository and worktree on the machine:

- **Resource-aware admission.** A job declares the CPU, memory, and scratch it needs. It starts
  when the machine has room, and on a supported host cgroup v2 holds it to what it declared.
- **Atomic landing.** `agc land 123 -- ./scripts/test.sh` brings the current target into the
  pull-request branch, runs your gate once, and merges the pull request in the same durable
  step. A red gate publishes nothing. A moved target never reuses a green result.
- **One record.** Every job has a stable ID, a combined log, and a row you can list, follow,
  cancel, or watch in a terminal UI from any shell.

It works with any agent that can run a shell command, such as Claude Code, Codex, or Aider,
and with people. It complements hosted CI rather than replacing it. The core is
forge-neutral; GitHub support is an optional adapter. [Why AGCoord exists](docs/why.md) makes
the case in depth.

## What you need

- Linux on x86_64, and Python 3.10 or newer for the `agcoord` client.
- For the two-minute try-out below: nothing else. The released broker runs as your own user
  with admission-only accounting.
- For enforced limits: an Ubuntu 24.04-class host (unified cgroup v2 mounted read-write with
  `nsdelegate`, AppArmor ABI 4, `kernel.apparmor_restrict_unprivileged_userns=1`, systemd 254
  or newer) and one privileged install step. The broker itself runs as an unprivileged user
  service; no root daemon is installed.
- For landing: a GitHub pull request. Checks and full gates need no forge at all.

AGCoord is alpha software that moves quickly. A client commands only a broker of its own
minor release line, and the [changelog](https://github.com/xu-hao/agcoord/blob/main/CHANGELOG.md) records every user-facing change.

## Try it in two minutes, without root

The client talks to a broker executable that must match its version exactly, and it ships
the SHA-256 of that executable. One command fetches the release broker, verifies it against
that pin, places it under `~/.local/libexec/agcoord`, and configures an unmanaged spool:

```bash
python -m pip install agcoord            # in a virtual environment, or: pipx install agcoord
agc host install --user
```

Now submit work from inside any Git checkout:

```bash
cd ~/src/your-repo
agc run --label "unit tests" --resource cpu=2 -- python -m pytest -q
agc list
agc tui
```

The first client starts the broker on demand, and closing the terminal does not cancel the
job. The [quickstart](docs/quickstart.md) continues from here: watching two oversized jobs
queue behind each other, following logs, moving to the enforced host, and landing a pull
request. After `pip install --upgrade agcoord`, run `agc host install --user` again; a client
refuses a user-owned broker that is not the one it was released with.

## Turn on enforcement

On a supported Ubuntu host, one privileged step installs the pinned broker as a root-owned
file, enables a systemd user service and an enforcing AppArmor profile, and proves a one-CPU
limit before reporting success. It needs an empty default spool: if you ran the try-out on
this machine, `agc drain` it and move `~/.local/state/agcoord` aside first.

```bash
python -m pip install --upgrade agcoord
agc host install --download
```

`--download` fetches the release bundle that matches the installed client, verifies its
checksums and the broker against the digest the client ships with, and refuses anything else.
A fresh install records the machine's available CPU count as both `cpu` and `jobs` capacity
with a required cgroup-v2 binding, so `--resource cpu=N` becomes a real `cpu.max` limit.
Memory, tmpfs, persistent scratch, process, and block-I/O bindings live in the same
`config.json`. The [native host runbook](docs/native_host.md) has the full host contract,
offline installation from a bundle, upgrades, recovery, and rollback.

## Tell your agents

Agents follow the tools they are given. Paste this into your repository's `CLAUDE.md` or
`AGENTS.md`, and every agent on the machine coordinates through the same queue:

```text
Run every check through the local coordinator and declare what it uses:
  agc run --label "<what>" --resource cpu=2 -- <command>
Land a pull request only through one gate-and-publish request; never merge directly:
  agc land <pr> --resource cpu=4 -- <full test command>
Never run agc from inside an admitted job. A stale-main or head-changed refusal means:
update the branch, push, and submit a new land request.
```

The [agent guide](docs/agents.md) has the long form, what every refusal and handback means
and what to do next, and notes for sandboxed shells. This repository's own
[AGENTS.md](https://github.com/xu-hao/agcoord/blob/main/AGENTS.md) is a worked example.

## Run work

<img src="https://raw.githubusercontent.com/xu-hao/agcoord/main/docs/assets/agcoord-tui.svg" alt="agc tui showing two repositories: a landing in its gating phase, a running check, two queued jobs, recent history, the selected row's detail, and the machine's capacity footer" width="820">

```bash
agc run  --label "unit tests"   --resource cpu=2 -- python -m pytest -q   # an ordinary check
agc full --label "release gate" --resource cpu=4 -- ./scripts/test.sh      # a clean exact-head receipt
agc land 123 --label "land PR 123" --resource cpu=4 -- ./scripts/test.sh   # gate and publish, one row
agc list                                                                   # every job on the machine
agc show check-0123456789ab                                                # one durable row, as JSON
agc log land-0123456789ab --follow                                         # one combined log
agc cancel check-0123456789ab                                              # process-group cancellation
agc tui                                                                    # live view across repositories
```

Every job implicitly holds one `jobs` slot. Repeatable `--resource NAME=UNITS` options add
named, configured resources; an unknown or impossible request fails instead of waiting
forever. Without a binding, a name is admission accounting only. With a binding in
`config.json`, the broker applies and measures it, and every run reports what was requested,
applied, and observed. Scratch is opt-in: a job that declares neither a tmpfs nor a
project-quota policy receives no temporary directory from AGCoord, and inherited `TMPDIR`,
`TMP`, and `TEMP` values are removed. The
[resource contract](docs/coordinator.md#repository-lanes-and-resources) covers bindings,
[delegated cgroups](docs/coordinator.md#delegated-cgroup-v2-lifecycle),
[child CPU leases](docs/coordinator.md#child-cpu-leases-for-parallel-tools) for tools that
fan out inside one job, and the optional
[pytest-xdist adapter](docs/coordinator.md#optional-pytest-xdist-adapter).

`full` validates an exact clean head and keeps a durable receipt; it is ordinary lane work,
not a barrier. `land` is the only barrier. Push the branch, open the pull request, and submit
from a clean checkout of that head: the row excludes other lands in its repository and jobs
from its own worktree, holds its lane and resources from preflight through publication, and
never rebases or rewrites commits. If the target advanced, the default GitHub adapter first
merges it into the request branch and pushes with an exact lease; `--no-target-sync` refuses
instead. A red gate records `gate-failed` and publishes nothing. `stale-main`,
`head-changed`, `pr-not-ready`, `publish-failed`, `merge-error`, and `avoided-commit` are the
other handbacks, and none of them moves the target. Cancellation is refused only once a land
is publishing. The [atomic landing contract](docs/coordinator.md#atomic-landing) is the
authority.

Maintenance stays in the same tool. `agc drain --reason ...` refuses new submissions while
admitted work finishes, and `agc resume <drain-id>` reopens the queue. `agc avoid <sha>`
stores a commit that no landing on this machine may publish again after a target rewrite.
`agc clear` removes terminal history while the queue is idle.

State lives in `${XDG_STATE_HOME:-~/.local/state}/agcoord`; `AGCOORD_STATE_DIR` or
`--state-dir` selects a different spool for an unmanaged coordinator. One `config.json` there
holds `capacities`, `bindings`, `cgroup_root`, `database_timeout`, and `native_broker`; with
no file at all, capacity defaults to two job slots. A spool left below protocol 5 by a release
before 0.6.0 is refused with instructions; see
[migrating a pre-native spool](docs/native_migration.md).

## How it compares

| If you use | What it coordinates | Machine resources | Merge gating |
| --- | --- | --- | --- |
| GitHub merge queue, Mergify | Pull requests, on hosted CI | No | Yes, in CI |
| pueue, task-spooler, nq | Shell commands on one machine | Parallel count only | No |
| Claude Squad, Conductor, Vibe Kanban | Agent sessions and worktrees | No | Manual |
| Gas Town | An agent workforce with an LLM-run merge queue | Session count cap | Yes |
| Container limits | One container | Static per container | No |
| **AGCoord** | Jobs from any agent, tool, or person | Declared and enforced | Yes, atomic with the gate |

AGCoord sits under the session managers and beside the hosted queues. Use one or the other
per branch: a branch that requires a hosted merge queue rejects the direct ref update that
atomic publication performs for anyone without bypass rights.
The [comparison](docs/comparison.md) covers each of these by situation, with Gas Town in
depth.

## Documentation

- [Quickstart](docs/quickstart.md): the no-root try-out and the enforced host, step by step.
- [Coordinator contract](docs/coordinator.md): scheduling, lanes, resources, landing,
  recovery, CLI, and TUI.
- [Native host runbook](docs/native_host.md) and
  [native broker architecture](docs/native_broker.md).
- [Documentation index](docs/index.md) for everything else, including the release and
  conformance contracts.

## Project status

AGCoord is distributed as the `agcoord` project on PyPI, with import package `agcoord` and
the `agc` console command; `python -m agcoord` remains an equivalent module entry point.
Build and install development checkouts in an isolated environment rather than copying
modules into another project. Contributors follow [AGENTS.md](https://github.com/xu-hao/agcoord/blob/main/AGENTS.md) and the
[contributor workflow](docs/contributing.md). The gourd mascot and its asset notes live under
[docs/assets](docs/assets/README.md).
