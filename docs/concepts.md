# Concepts

This page explains AGCoord's model in plain sentences: what the broker is, what the three kinds
of job promise, why one of them is a barrier and the others are not, how a resource claim
becomes a kernel limit, what a receipt records, and how a job moves through its life. Every
section ends with a link into the [coordinator contract](coordinator.md), which states the
exact rule; this page never overrides it.

## The machine, the broker, and the spool

One OS user on one machine has one coordinator. Its state lives in one directory, the
*spool*, which is `~/.local/state/agcoord` unless `AGCOORD_STATE_DIR` or `--state-dir` says
otherwise. Clients, which are the `agc` command, the TUI, and the pytest-xdist adapter, write
submissions into a SQLite database in that directory and read snapshots back from it. There
is no network listener.

Exactly one *broker* owns the spool at a time, proven by a lock file. The broker is the only
process that admits work, starts and supervises it, applies resource limits, publishes a
landing, and recovers after a crash. Inserting a row never runs anything; the broker does.
An *unmanaged* broker is a user-owned executable that the first client starts on demand and
that exits when idle. A *managed* broker is the root-owned release executable run by a systemd
user service inside an AppArmor profile, which is what makes enforcement possible. One
`config.json` in the spool configures whichever broker owns it.

Exact rules: [machine state and ownership](coordinator.md#machine-state-and-ownership).

## Jobs: check, full, and land

A job is one command, run once, from one checkout, with one stable ID, one durable row, and
one combined log. There are three kinds.

| Kind | You give it | It promises | Barrier? |
| --- | --- | --- | --- |
| `check` | A command from any checkout, clean or not | Runs it when the machine has room, with the resources you declared | No |
| `full` | A command from a clean checkout at an exact head | Runs it against exactly that head and keeps the passed row as a receipt for it | No |
| `land` | A pull request and a gate command from a clean checkout of the pushed head | Brings the target into the branch, runs the gate once, and merges the pull request in the same step, or publishes nothing | Yes |

`check` is the everyday job. `full` adds one thing, an exact-head receipt: the checkout must
be clean, the head is recorded at submission, and the worker refuses to run if either changed
while the job waited. `land` adds publication and is the only job that holds anything against
other jobs.

Exact rules: [running checks and standalone full gates](coordinator.md#running-checks-and-standalone-full-gates)
and [atomic landing](coordinator.md#atomic-landing).

## Lanes, and why land is the only barrier

Every job belongs to a *lane*, which is its repository identity, and records its *worktree*.
Two agents working in two worktrees of the same repository share a lane.

Within a lane, most work overlaps: checks and fulls from different worktrees run side by side
whenever the machine has room. A `land` is different. While it runs, from preflight through
publication, no other `land` in the lane and no job from the land's own worktree can begin.
Everything else in the lane, and everything in other lanes, only competes for capacity.

The reason is the guarantee the landing makes. Between a gate turning green and the merge,
nothing may change what was gated. Holding the lane and the worktree closes that gap without
turning one repository's landing into a lock on the whole machine. A queued job that is
blocked by a land, by its worktree, or by capacity lets later admissible work pass it, so a
long landing never leaves the machine idle.

Exact rules: [repository lanes and resources](coordinator.md#repository-lanes-and-resources).

## Resources: claims, capacities, and bindings

A job *claims* resources with `--resource name=units`, and every job implicitly claims one
`jobs` slot. The broker's `config.json` declares *capacities*, the total units of each name
the machine hands out. Admission is arithmetic: a job starts when, for every name it claims,
the units already held plus its own fit under the capacity. A name with no capacity is refused
at submission, and so is a claim larger than the capacity, because neither could ever run.

A *binding* gives a name a meaning beyond arithmetic: a kind, a unit, a backend, and a mode.
Without a binding, a claim is *admission accounting*, a promise the job makes and nothing
checks. With a `cgroup-v2` binding, the claim becomes a limit the kernel applies to the job's
whole process tree: `cpu` becomes a `cpu.max` quota, `memory` a hard limit that ends the job as
one group when exceeded, `pids` a process ceiling, `tmpfs` a bounded RAM-backed scratch, and
the block-I/O names bandwidth and operation ceilings on a device. The `project-quota` backend
does the same for persistent scratch on ext4 or XFS.

The mode says what happens when the backend cannot apply the limit. `required` refuses the
job before user code runs. `best-effort` runs the job anyway and records that the limit was
not applied. `admission-only` never tries.

Scratch is opt-in. A job that declares no tmpfs or project-quota policy receives no temporary
directory from AGCoord, and the `TMPDIR`, `TMP`, and `TEMP` it inherited are removed, so a job
that needs scratch must say so with the complete set of names its policy requires.

A job that fans out, such as a test runner with many workers, divides its own `cpu` claim
rather than assuming the machine: the child CPU lease API and the pytest-xdist adapter do that
division inside the job without creating new jobs.

Exact rules: [repository lanes and resources](coordinator.md#repository-lanes-and-resources),
[delegated cgroup v2 lifecycle](coordinator.md#delegated-cgroup-v2-lifecycle), and
[child CPU leases](coordinator.md#child-cpu-leases-for-parallel-tools).

## Receipts

When the broker accepts a job it freezes a *resource contract*: for each claimed name, the
backend, kind, mode, and unit it meant at that moment, so a later configuration change cannot
reinterpret an old row. When the job runs, the broker fills a *receipt* with four fields:
`requested`, the claim; `applied`, what the backend actually set, recorded only after a
successful attach; `peak`, what the backend measured in the bound unit; and `events`, stable
facts such as `cpu-throttled` or `tmpfs-byte-limit-hit`, each recorded once. Admission-only
names have empty `applied`, `peak`, and `events`.

`agc list` compresses a receipt into one word, `admission-only`, `applied`, `partial`,
`unapplied`, or `failed`, and `agc --json show <id>` returns all of it. The practical use is
sizing: a peak far below a claim says the next claim can shrink, and a throttle event says it
should grow.

Exact rules: [durable job shape](coordinator.md#durable-job-shape).

## The life of a job

```text
                         queued ──────────────► cancelled
                           │
                           ▼
                        running ─────► passed | failed | cancelled | interrupted

  a land's phases inside "running":
                 queued ► preflight ► gating ► publishing ► complete
                                       │           │
                       red gate: failed (gate-failed)   moved refs: failed (stale-main,
                                                        head-changed), nothing published
```

Every job is `queued` until the broker admits it, `running` while its command lives, and then
exactly one of four terminal states. `passed` and `failed` carry the command's verdict.
`cancelled` means someone asked, with `agc cancel` or a graceful broker stop; the whole process
group is gone before the row becomes terminal. `interrupted` means the worker vanished before
it could report, and the coordinator claims no verdict for it. Terminal rows are immutable
history until `agc clear` removes them while the queue is idle.

A `land` moves through named phases inside `running`. *Preflight* checks the pull request,
the head, and the target, and merges an advanced target into the branch with an exact lease.
*Gating* runs the gate command once. *Publishing* is one atomic update of the target and the
source on the forge; it is the only phase that cannot be cancelled, because ending a client in
the middle of an authenticated mutation would leave the outcome unknown. If the target or the
source moved at any point, the row fails with a named handback and the verdict is discarded
rather than reused.

If the broker crashes, a replacement adopts each live worker whose recorded process identity
still matches, never reruns a command that already started, and classifies anything it cannot
verify as `interrupted`.

Exact rules: [durable job shape](coordinator.md#durable-job-shape),
[atomic landing](coordinator.md#atomic-landing), and
[observation and cancellation](coordinator.md#observation-and-cancellation).

## The maintenance words

*Drain* closes the queue for maintenance: new submissions are refused with a stable code,
admitted work finishes, and the owner yields; the receipt's `drain-…` ID is the only key that
reopens it with *resume*. *Avoid* records a commit that no landing on this machine may publish
again, which is how a deliberately rewritten target stays rewritten. *Clear* removes terminal
history and its logs while nothing is live, and nothing else.

Exact rules: [durable maintenance drain](coordinator.md#durable-maintenance-drain) and
[avoided commits](coordinator.md#avoided-commits-after-a-target-rewrite).

## Trust, in one paragraph

Everything running as your user can write the spool, so the spool is not a boundary between
processes of the same user; the boundary is around the machine, unrelated processes, and the
landing verdict, and it is enforced against admitted commands. A managed host proves that
boundary with a root-owned release broker under AppArmor and systemd. A user-owned broker is
trusted only when its digest equals the one the client was released with, which
`agc host install --user` arranges. A build from source is trusted only when `config.json`
says so with `allow_development`.

Exact rules: [native broker trust model](native_broker.md#trust-model).
