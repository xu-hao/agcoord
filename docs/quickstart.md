# Quickstart

This page takes you from nothing to two coordinated jobs in a few minutes, without root, and
then to the enforced host and a first landing. Every command and output below was run against
AGCoord on x86_64 Linux. The [overview](overview.md) explains what AGCoord is for; the
[coordinator contract](coordinator.md) defines every guarantee mentioned here.

## 1. Install the client and its broker

The Python client and the broker executable must be the same version, and the client ships
the SHA-256 of the broker it was released with. Install the client, then let it fetch, verify,
and place its own broker:

```bash
python -m pip install agcoord            # in a virtual environment, or: pipx install agcoord
agc host install --user
```

```text
AGCoord: installed user broker 0.6.4 at /home/you/.local/libexec/agcoord/agcoord-broker; spool /home/you/.local/state/agcoord configured
```

Four things happened. The client downloaded the standalone release broker for its own version
from the GitHub release, together with its `.sha256` sidecar. It compared the download with
the digest pinned inside the client itself, not only with the sidecar that travelled with it.
It placed the file at `~/.local/libexec/agcoord/agcoord-broker` with mode `0755` and selected
it once, exactly as every later command will. And it wrote a `config.json` in the default
state directory, `~/.local/state/agcoord`, declaring your available CPU count as both `cpu`
and `jobs` capacity and `managed_service: false`, so the first client starts the broker on
demand instead of asking systemd.

No privilege was used and no service was created. The command is safe to repeat: after
`pip install --upgrade agcoord`, run it again, because a client accepts a user-owned broker
only when the file is the one that client was released with. `AGCOORD_STATE_DIR` or
`--state-dir` selects a different spool; `native_broker.allow_development` is needed only for
a broker you built from source.

## 2. Run something

`agc` schedules work per repository and worktree, so run it from inside a Git checkout:

```bash
cd ~/src/your-repo
agc run --label "hello" --resource cpu=1 -- sh -c 'echo hello from agcoord; nproc'
```

The row is accepted, queued, and admitted, and then the command's own output follows:

```text
AGCoord: accepted check-f299fa38517f
Gate queue: check-f299fa38517f waiting at position 1 for branch main
Gate queue: check-f299fa38517f running as pid 3281029 in /home/you/src/your-repo
hello from agcoord
8
```

Closing that terminal would not have cancelled anything; the broker owns the job. Look at it
from any shell:

```bash
agc list
agc show check-f299fa38517f
agc log check-f299fa38517f --follow
agc tui
```

`list` prints one line per job with its status, kind, repository, agent, label, declared
resources, and an enforcement summary, which reads `admission-only` in this spool. `show`
prints the complete durable row as JSON, including the exact command, checkout, branch, and
resource contract. `log` prints the combined stdout and stderr, and `--follow` waits through
completion. The TUI shows every repository on the machine; `r` refreshes, `Enter` opens a
row, `l` shows its log, `c` cancels, `p` and `a` filter by repository and agent, and `q`
leaves the jobs running. Set `AGCOORD_AGENT` in each agent's environment, or pass `--agent`,
and the rows say which agent submitted them.

## 3. Watch two jobs queue

The spool's `cpu` capacity is your CPU count. Submit two jobs that each claim more than half
of it:

```bash
n=$(nproc); claim=$((n / 2 + 1))
agc run --label "first"  --resource cpu=$claim -- sh -c 'sleep 10; echo first done' &
sleep 1
agc run --label "second" --resource cpu=$claim -- sh -c 'echo second done'
```

The second job is accepted at once, reports `waiting at position 1`, and runs only after the
first finishes. That is the whole idea: every agent on the machine declares what it needs,
and the broker admits work when the machine has room. Both rows stay in `agc list` as
history.

## 4. Keep going, or clean up

To keep using AGCoord unmanaged there is nothing more to do. Unmanaged mode gives you
scheduling, lanes, logs, landings, and the TUI on any x86_64 Linux machine. What it cannot do
is enforce a declared limit; a job that claims two CPUs can still use eight.

To remove the try-out, drain the spool so the broker exits, then delete what the install
created:

```bash
agc drain --reason "done with the try-out"
rm -r ~/.local/state/agcoord ~/.local/libexec/agcoord
```

## 5. Turn on enforcement

Enforcement needs an Ubuntu 24.04-class host: unified cgroup v2 mounted read-write with
`nsdelegate`, AppArmor ABI 4, `kernel.apparmor_restrict_unprivileged_userns=1`, and systemd
254 or newer. The managed install needs an empty default spool, so drain the try-out and move
it aside first. Then one command installs the pinned broker as a root-owned file, enables a
systemd user service and an enforcing AppArmor profile, and proves a one-CPU limit before
reporting success:

```bash
agc drain --reason "moving to the managed host"
mv ~/.local/state/agcoord ~/.local/state/agcoord-user
python -m pip install --upgrade agcoord
agc host install --download
```

The client downloads the release bundle for its own version, verifies the checksums and the
broker digest it ships with, and performs the privileged activation. A fresh install writes a
managed `config.json` with the machine's available CPU count as both `cpu` and `jobs`
capacity and a required cgroup-v2 binding, so `--resource cpu=2` becomes a `cpu.max` quota
on the job's cgroup. Memory, tmpfs, persistent scratch, process, and block-I/O bindings are
added to the same file; because scratch is opt-in, a job that needs temporary space declares
a tmpfs or project-quota policy. The
[native host runbook](native_host.md) covers the host contract, offline installation,
upgrades, recovery, and rollback, and the
[resource contract](coordinator.md#repository-lanes-and-resources) covers every binding.

## 6. Land a pull request

Landing works in both modes and needs an authenticated GitHub CLI (`gh`), because GitHub is
the publication adapter. Push the branch, open its pull request, and submit one landing
request from a clean checkout of that exact head:

```bash
git push -u origin HEAD
gh pr create --base main --fill
agc land 42 --label "land my-branch" --resource cpu=4 -- ./scripts/test.sh
agc log land-0123456789ab --follow
```

The row is the repository's one barrier while it runs. It checks that the pull request is
open, ready, and points at this head; if `main` moved since you branched, it merges the
current `main` into your branch and pushes with an exact lease, then runs your gate once. A
green gate publishes immediately, without releasing the lane, as a merge commit whose parents
are the current target and your head. A red gate records `gate-failed` and publishes nothing.
`stale-main`, `head-changed`, `pr-not-ready`, `publish-failed`, `merge-error`, and
`avoided-commit` hand the work back without moving the target; fix the branch and submit a
new request. The [atomic landing contract](coordinator.md#atomic-landing) describes each
step and refusal.

## Where next

- Tell your agents. The [agent guide](agents.md) has the paragraph to paste into `CLAUDE.md`
  or `AGENTS.md` and what to do with every refusal.
- Give jobs real budgets. The
  [resource contract](coordinator.md#repository-lanes-and-resources) explains bindings,
  receipts, and the [child CPU leases](coordinator.md#child-cpu-leases-for-parallel-tools)
  that let tools such as pytest-xdist share one job's CPU budget.
- Operate the machine. The [coordinator contract](coordinator.md) covers drain and resume,
  avoided commits after a target rewrite, crash recovery, and the TUI in full.
