# Quickstart

This page takes you from nothing to two coordinated jobs in a few minutes, without root, and
then to the enforced host and a first landing. Every command and output below was run against
AGCoord 0.6.2 on x86_64 Linux. The [README](../README.md) explains what AGCoord is for; the
[coordinator contract](coordinator.md) defines every guarantee mentioned here.

## 1. Install the client and the matching broker

The Python client and the broker executable must be the same version. Install the client,
read its version, and fetch that release's broker:

```bash
python -m pip install agcoord            # in a virtual environment, or: pipx install agcoord
version=$(agc --version | awk '{print $2}')

mkdir -p ~/.local/libexec/agcoord && cd ~/.local/libexec/agcoord
base="https://github.com/xu-hao/agcoord/releases/download/v$version"
curl -fsSL -O "$base/agcoord-broker-x86_64-unknown-linux-musl" \
     -O "$base/agcoord-broker-x86_64-unknown-linux-musl.sha256"
sha256sum -c agcoord-broker-x86_64-unknown-linux-musl.sha256
chmod 0755 agcoord-broker-x86_64-unknown-linux-musl
./agcoord-broker-x86_64-unknown-linux-musl --version
```

The broker is one static executable and needs nothing else to run unmanaged. Its
`--version` line names the protocol and its build digest:

```text
agcoord-broker 0.6.2 (protocol 5, sha256:e9c1aebb…)
```

## 2. Give the try-out its own spool

Keep the try-out out of the default state directory. A later `agc host install` needs an
untouched default spool: it refuses one that already holds a queue or a configuration that
is not the managed one. Export the variable in every shell that will run `agc`, including
the one your agent runs in:

```bash
export AGCOORD_STATE_DIR="$HOME/.local/state/agcoord-try"
mkdir -p "$AGCOORD_STATE_DIR" && chmod 0700 "$AGCOORD_STATE_DIR"
cat > "$AGCOORD_STATE_DIR/config.json" <<EOF
{"capacities": {"jobs": 2, "cpu": 4},
 "native_broker": {"path": "$HOME/.local/libexec/agcoord/agcoord-broker-x86_64-unknown-linux-musl",
                   "allow_development": true, "managed_service": false}}
EOF
chmod 0600 "$AGCOORD_STATE_DIR/config.json"
```

Three things are going on in that file. `capacities` declares what this spool hands out:
every job implicitly takes one `jobs` slot, and `cpu` here is admission accounting because no
binding enforces it. `allow_development: true` is the current name for "trust an executable
owned by the current user"; the client still refuses a symlink, a group- or world-writable
file, or a broker whose version, target, or build identity does not match.
`managed_service: false` lets the first client start the broker on demand instead of asking
systemd for a service.

## 3. Run something

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

## 4. Watch two jobs queue

The spool has four CPU units. Submit two jobs that each want three:

```bash
agc run --label "first"  --resource cpu=3 -- sh -c 'sleep 10; echo first done' &
sleep 1
agc run --label "second" --resource cpu=3 -- sh -c 'echo second done'
```

The second job is accepted at once, reports `waiting at position 1`, and runs only after the
first finishes. That is the whole idea: every agent on the machine declares what it needs,
and the broker admits work when the machine has room. Both rows stay in `agc list` as
history.

## 5. Clean up, or keep going

`agc drain` refuses new submissions, lets admitted work finish, and stops the unmanaged
broker:

```bash
agc drain --reason "done with the try-out"
unset AGCOORD_STATE_DIR
rm -r ~/.local/state/agcoord-try
```

Keep the spool instead if you want to go on unmanaged: `agc resume <drain-id>`, with the ID
that `drain` printed, reopens it. Unmanaged mode gives you scheduling, lanes, logs, landings,
and the TUI on any x86_64 Linux machine. What it cannot do is enforce a declared limit; a job
that claims two CPUs can still use eight.

## 6. Turn on enforcement

Enforcement needs an Ubuntu 24.04-class host: unified cgroup v2 mounted read-write with
`nsdelegate`, AppArmor ABI 4, `kernel.apparmor_restrict_unprivileged_userns=1`, and systemd
254 or newer. With `AGCOORD_STATE_DIR` unset, one command installs the pinned broker as a
root-owned file, enables a systemd user service and an enforcing AppArmor profile, and proves
a one-CPU limit before reporting success:

```bash
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

## 7. Land a pull request

Landing works in both spools and needs an authenticated GitHub CLI (`gh`), because GitHub is
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

- Tell your agents. The [README](../README.md#tell-your-agents) has a paragraph to paste into
  `CLAUDE.md` or `AGENTS.md`.
- Give jobs real budgets. The
  [resource contract](coordinator.md#repository-lanes-and-resources) explains bindings,
  receipts, and the [child CPU leases](coordinator.md#child-cpu-leases-for-parallel-tools)
  that let tools such as pytest-xdist share one job's CPU budget.
- Operate the machine. The [coordinator contract](coordinator.md) covers drain and resume,
  avoided commits after a target rewrite, crash recovery, and the TUI in full.
