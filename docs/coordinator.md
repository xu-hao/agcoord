# Coordinator contract and operations

AGCoord coordinates development jobs submitted by multiple agents, terminals, worktrees, and
repositories on one machine. One detached broker is the scheduling and process-supervision
authority for the current OS user. Clients communicate through a private durable spool; they
do not launch a job merely because they inserted a row.

The public Python surface uses `CoordinatorBroker` and `CoordinatorClient`. The public CLI is
installed as `agc`; `python -m agcoord` remains an equivalent module entry point. Both expose
`run`, `full`, `list`, `show`, `log`, `cancel`, `tui`, `land`, `drain`, `resume`, `migrate`,
and `clear`. Worker and broker verbs used to detach or validate an admitted process are
internal interfaces, not alternate user workflows.

## Machine state and ownership

All repositories for one user share a default state directory:

```text
${XDG_STATE_HOME:-~/.local/state}/agcoord
```

`AGCOORD_STATE_DIR` overrides that location. `--state-dir` overrides it for one command and
is useful for tests or a deliberately isolated coordinator. The state path is independent of
the current checkout, so two unrelated repositories converge on one broker while retaining
different stable repository and worktree identities.

The state directory, spool, lock, broker diagnostics, run logs, and transient sidecars are
owner-only, and the spool is private to the broker. Clients never open `queue.sqlite3` or
`broker.lock`: they submit, observe, and inspect by running the broker's own commands, so
the spool's generation, its live owner, and any durable drain are answered by the one
component that defines them and every refusal carries a stable broker code. An ownership
lock elects exactly one broker. Simultaneous first clients either join that owner or fail
without an accepted row; they never create two supervisors for the same spool. An unmanaged development configuration lets the first ordinary client start the
explicitly selected native executable as a detached owner on demand. A production managed
configuration instead asks systemd to start the installed long-lived user service; clients
never spawn that broker directly. Closing the submitting terminal does not cancel accepted
work. Concurrent first clients converge on the same ownership lock; a loser joins the exact
compatible owner instead of starting a second supervisor.

An unmanaged broker may exit after the queue is empty and idle; the managed user service does
not. Durable history and logs remain, and a later unmanaged client or systemd starts a
replacement owner against the same spool. An unclean restart observes
the recorded process identity before classifying a live job; it never reruns an already
spawned command merely because the old broker disappeared. An exception that ends the broker
does not request cancellation or signal live process groups: ownership is released so a
replacement can adopt each worker whose recorded PID and process-start token still match.
A graceful stop — SIGTERM or SIGINT, which is what `systemctl --user stop` and `restart` send —
drains instead of cancelling. The owner stops admitting work, leaves queued rows for the next
owner, keeps observing running jobs and granting their child CPU leases, and releases ownership
once none is running. If jobs are still running when `stop_grace` elapses, or when a second stop
signal arrives, the owner ends each one that is not publishing as a complete process group and
records it `interrupted` with exit status 125 and failure reason `broker-stopped`, so its agent
can tell an operator stop from a verdict and submit it again. A publishing land or retained
merge is never interrupted; the owner waits for its authoritative result. A caller's own
`agc cancel` during a stop still records `cancelled`.

A land worker reports its final overall status durably while its row is still running. After
an unclean owner loss, a replacement preserves the live worker's lane and resources, never
reruns its gate, waits for the exact process group to disappear, reclaims scratch, and then
exposes the reported passed or typed-failed result. If the worker disappears before it can
report a result, `interrupted` is the only safe terminal classification.

If systemd recreated the broker service's delegated cgroup root after every recorded worker
using the old root disappeared, the replacement abandons only its private recovery metadata
for that old root, records the dead rows `interrupted`, and prepares later work under the new
root. It never removes or signals anything in the new root. A matching live worker, or any
other identity mismatch, still refuses recovery rather than treating a reused path as owned.

## Durable maintenance drain

Maintenance closes the submission race before waiting for an idle spool:

```bash
agc --json drain --reason "native host upgrade" >drain-receipt.json
drain_id=$(jq -r '.drain_id' drain-receipt.json)
# Perform owner-locked maintenance while the coordinator remains drained.
agc resume "$drain_id"
```

`drain` takes an immediate SQLite write transaction that creates a durable `draining` marker
and submission guards. The transaction is the linearization point: every competing submission
commits completely before the guard or is refused completely after it. Native clients receive
the stable `broker-draining` code; a direct or older SQLite writer receives
`agcoord-maintenance-draining`. With `--json`, `agc` writes the native-style `code` and `message`
refusal object to standard error. Repeating `drain` while a marker exists returns the original
receipt instead of replacing its identity or reason.

Queued and running rows remain owned work. The broker admits or recovers them, preserves
resource and repository-lane ownership, and lets them reach a durable terminal result. A land
whose authoritative publication transition has committed remains authoritative. `list`,
`show`, `log`, the TUI, and named cancellation remain available while draining; new `run`,
`full`, and `land` submissions and `clear` are refused. A replacement owner may start only to
recover live rows while the state is `draining`. Once no live row remains, the owner commits
`drained` and yields its ownership lock even when configured as a managed service; a drained
spool cannot autostart an ordinary owner.

The receipt contains exactly `state`, `drain_id`, `reason`, `started_at`, `protocol`, `live`,
and `broker_pid`. The marker and its SQLite guards survive client exit, broker crash, host
activation, migration, and rollback. `resume` requires the exact `drain-…` identifier. While
`state` is `draining`, `resume` atomically removes the marker and guards inside an immediate
SQLite write transaction without stopping live rows or displacing the serving owner, cancelling
an unwanted drain before maintenance begins. Once `state` is `drained`, `resume` requires zero
queued or running rows and exclusive owner-lock acquisition so it cannot race an active
maintenance holder. A wrong or stale identifier cannot reopen submissions. Migration and
rollback preserve the current marker across the protocol boundary and never resurrect a marker
already removed by a successful resume.

A normal drain stranded behind a dead or unstartable managed owner may be repaired through
[guarded managed host recovery](native_host.md#guarded-recovery-when-the-outgoing-broker-cannot-drain).
The original drain ID, state, reason, and start time remain durable maintenance
intent. Submission guards stay continuous while the temporary recovery guard admits only its
bound host proof. After verification, recovery atomically restores the original drain;
accepted queued work finishes normally, and the owner yields after the last accepted row
becomes terminal. New submissions remain refused until explicit `resume` with the exact
original drain ID after the intended maintenance succeeds. Repair and proof do not authorize that resume.
Recovery completion and its retry receipt retain the original drain audit rather than always
reporting `open`. Invalid maintenance metadata or live/ambiguous worker identities refuse;
retries never reset proof attempts. Repeating a stranded drain alone cannot make an
unstartable broker progress, and raw spool edits or cancellation of accepted work are not a
repair path.

## Repository lanes and resources

Each submission belongs to a stable repository lane and records its resolved worktree. A
lane preserves publication order without turning one repository's full gate into a lock on
the entire machine:

- `check` is ordinary work. Compatible work overlaps across repositories, worktrees, and
  kinds whenever the declared resources fit.
- `full` is ordinary lane work with a clean exact-head submission requirement and a durable
  receipt. It is not a barrier: checks, other fulls, and lands in other worktrees overlap it
  when their resources fit and the gates name different branches.
- `land` is the only barrier. It is one gate-and-publication barrier in its lane: no other
  land in the lane, and no job from the same worktree, can begin between its preflight, gate,
  and atomic publication. Work from other worktrees of the same repository only competes for
  capacity, except that exact-head gates for the same branch serialize. Retained legacy
  `merge` rows are barriers of the same shape, remain identifiable
  in migrated history, and are not the normal public landing workflow.

### Coordinator-owned gate trees

`full` and `land` run in a reusable detached Git worktree per repository identity and
branch below `<state-dir>/gate-trees/`. Checks continue to run in the submitted checkout.
The broker reserves a branch tree from admission through completion, including pending
setup. A full is still ordinary lane work; only other gates for its branch serialize.
Queued gates report `gate-tree:active:<run_id>` while an admitted gate holds their tree,
and `gate-tree:fifo:<run_id>` while an earlier queued gate for the same branch waits.

After attaching the authenticated launcher and its resources, the launcher materializes
and resets the tree with plain `git worktree add --detach` and `git reset --hard <head>`.
It removes nonignored untracked outputs before reuse. Unchanged tracked files keep their
mtimes and ignored build caches survive. The first gate on a branch is cold; ignored
files from the submitter's checkout are not copied. Commands and tools must be available
in the committed tree or through absolute paths and the submitted environment.

Git operations and cache scans run in the forked worker setup path, never in the
single-threaded scheduler. While setup is pending, other lanes and checks remain
admissible, cancellation stops the setup process group, and replacement owners retain
its reservation until that process group is gone. A replacement owner interrupts unfinished
setup with `worker-setup-lost` (exit 125); resubmit the job. Setup has a five-minute deadline.
A setup failure returns exit 125 with `gate-tree-checkout-missing`,
`gate-tree-head-missing`, `gate-tree-setup-failed`, or `gate-tree-capacity`; the run log
contains the detail. Restore the source checkout/head or resolve the reported filesystem
failure, then resubmit. No gate command or publication runs on a setup refusal.

Checks and recovery proof workers do not materialize a gate tree. The broker completes their
setup and releases them during admission instead of deferring setup to a later scheduler tick.

The executed row's `checkout` becomes the prepared gate tree before command release.
`worktree_id` remains the submitter's logical identity, so `--reuse-full` still matches
receipts from that worktree, exact head and command. Workers verify their executed
checkout. Overflow executors start there too and hand off that tree's source; target
synchronization and publication run there without changing the submitter's files or
local branch. Independent clones with the same repository identity reuse that branch tree
by importing the requested commit objects from the current source clone. After synchronization, fetch the pushed branch and reconcile the local
checkout before another submission.

At each gate setup, the spool prunes idle trees whose local branch ref or source Git
repository is gone, then evicts least recently prepared idle trees to retain at most
16 trees and 20 GiB of allocated cache blocks. Active and pending trees are protected;
admission refuses if those prevent eviction or a materialized tree exceeds the budget.
This is a setup-time cache budget, not a runtime filesystem quota: a running gate may
grow ignored artifacts, which the next setup accounts for. Setup/eviction is serialized
under a spool-owned file lock. Before forking a gate launcher, the broker atomically
publishes the active gate paths in `gate-reservations.json`; setup reads that snapshot
under the lock without reopening the broker's inherited SQLite state. Only exact
spool-owned worktree paths are removed; no
repository-wide `git worktree prune` or contributor cleanup runs.

The broker now has authority to update the source repository's `.git/worktrees` metadata
and its own gate cache. On managed hosts, only the trusted launcher enters the fixed
`/usr/bin/git` helper's `agcoord-gate-git` AppArmor profile; hooks and fsmonitor are
disabled, helper programs transition to admitted restrictions, and submitted Git retains
its ordinary admitted profile.

One JSON file, `config.json` in the state directory, configures the broker that owns that
directory. It holds at most `capacities`, `bindings`, `cgroup_root`, `cgroup_io`,
`database_timeout`, `land_gate_reuse_max_age`, `land_max_batch_size`, `stop_grace`,
`preempt_after`, `native_broker`, and `overflow_pools`; invalid JSON, a top-level
value that is not an object,
an unknown key, a section that is not an object, or an empty `cgroup_root` is refused when the
client or broker loads its configuration. When present, `cgroup_io` contains exactly one
nonempty `paths` list of unique absolute strings. `database_timeout` is a positive finite number
of seconds no greater than `2147483.647` (SQLite's millisecond limit) and defaults to `10`.
`land_gate_reuse_max_age` is a finite number of seconds from `0` to `2147483.647`, defaults to
`3600`, and bounds how long a passed `full` receipt may stand in for a land gate; `0` refuses
every reuse on that host even when a caller asks for one. `land_max_batch_size` is an integer from
`1` to `1024`, defaults to `8`, and bounds the adaptive power-of-two batch size ceiling (`1`
disables batch claiming so queued lands execute one at a time). `stop_grace` is a finite number of
seconds from `0` to `2147483.647`, defaults to `600`, and bounds how long a gracefully stopping
broker lets running jobs finish before it interrupts them; `0` interrupts them at once.
`preempt_after` is a finite number of seconds from `0` to `2147483.647` and is how long the
oldest queued job that lacks only local capacity waits, counted from when it last entered the
queue, before it may [preempt later checks](#first-fit-admission-and-preemption); it defaults
to `300` seconds, independent of any overflow pool's `wait_threshold`. A `land` does not wait for it. An
absent file uses `/usr/libexec/agcoord/agcoord-broker`, requires its release trust policy, and
defaults capacity to `jobs=2`.

`agc host config` prints the configuration this host would be given, and writes it with
`--write`; it refuses to replace an existing file without `--force`, because a live owner keeps
the configuration it acquired the spool with. The same derivation writes the configuration an
install creates when a state directory has none: `cpu` and `jobs` from the available
CPU-affinity count, `memory` from `MemTotal` minus a reserve, and a required cgroup-v2 binding
for each of those kinds whose controller is delegated to the broker's slice, so a derived
configuration is never one the broker refuses at startup. `--cpu`, `--jobs`, `--memory`, and
`--reserve` override what is derived, `--cgroup-root` selects the slice to inspect, and
`--tmpfs` with `--tmpfs-inodes` adds the scratch policy, which stays opt-in because its bytes
are charged against the same memory capacity.

```json
{
  "capacities": {"jobs": 4, "cpu": 8, "browser": 1},
  "bindings": {
    "cpu": {"kind": "cpu", "unit": "logical-cpu", "mode": "required", "backend": "cgroup-v2"}
  },
  "cgroup_root": "/sys/fs/cgroup/user.slice/user-1000.slice/user@1000.service/app.slice/agcoord-broker.service",
  "database_timeout": 30,
  "land_gate_reuse_max_age": 3600,
  "native_broker": {
    "path": "/usr/libexec/agcoord/agcoord-broker",
    "allow_development": false,
    "managed_service": true
  }
}
```

`native_broker.path` must be absolute. The default/release policy requires a root-owned,
non-writable-by-group-or-others x86_64 Linux artifact with the supported protocol, version,
implementation, and SHA-256 build identity; host packaging separately audits its static ELF
contract. A current-user-owned release executable is accepted without any flag when its
SHA-256 equals the digest pinned in the client (`native_host_pin.json`), which the client
computes before executing the file; `agc host install --user` places and configures such a
broker. Development from a checkout is explicit: select the absolute regular executable and
set `allow_development` to `true`. That permits only a current-user- or root-owned supported GNU
or musl development build; it does not weaken file, identity, protocol, or live-owner matching
checks. `managed_service=true` is valid only for the installed service's default state directory
and makes autostart call the fixed user unit instead of spawning the binary. Clients never
search `PATH` and never fall back to an unpinned broker.

No environment variable configures capacity, bindings, the delegated cgroup root, or block-I/O
paths or the database timeout; `AGCOORD_STATE_DIR` selects which state directory, and therefore
which configuration file, a client and broker share. A live owner keeps the capacity and
enforcement configuration with which it acquired the spool, so editing those sections changes
the next broker rather than the running one. Each process retains the `database_timeout` it
read when opening the spool; newly started clients or brokers can therefore use an updated lock
wait without changing existing connections.

Schema setup places every current-protocol spool in SQLite WAL journal mode, including an
existing spool the next time a compatible client or broker opens it. Readers therefore do not
block behind an ordinary writer. A live owner also keeps one connection open for as long as
it owns the spool. Closing the last connection to a WAL database checkpoints and removes the
WAL under an exclusive lock that every opening reader must wait behind, so an owner whose own
short-lived connections were the last would stall clients on a slow disk. The configured
timeout bounds each remaining lock wait, and a lock that outlasts it is reported as the
retryable `broker-database-busy`, never as a schema fault;
transient busy or locked results in the broker pump and idle health check are retried, and a
contended best-effort activity heartbeat never changes an already successful public operation
into an apparent failure.

Every job implicitly requests `jobs=1`. Jobs add resources with repeatable
`--resource NAME=UNITS` options. Names are generic machine capabilities such as `cpu`,
`memory`, `browser`, or a project-defined singleton; units are positive integers and the name
must exist in the configured capacity map. Admission requires every request to fit, and
allocation is held until the complete worker process group is gone. A request that can never
fit is rejected rather than left queued forever, unless it allowed overflow and an
[overflow pool](#overflow-pools) offers it. Scheduling does not infer resource use from
labels or commands.

### First-fit admission and preemption

Admission is first fit. On each pass the scheduler walks the queue in submission order, across
all repositories and row kinds, and starts every queued job whose blockers are empty: its
repository lane and worktree allow it, this machine offers the runtime and capabilities it
needs, and each resource it claims fits beside the claims that are already running. A blocked
job—waiting for a land, for its worktree, or for capacity—never holds back a later job that
fits; only lands keep submission order among themselves and behind earlier same-worktree work.
No capacity is reserved for a waiting job. Thus an eight-CPU host can overlap requests for six
and two CPUs, or four requests for two CPUs, without a preselected “two jobs at four CPUs each”
partition, and a job that is short of one resource leaves the others in use by later jobs.
`jobs` remains an independent concurrency ceiling and does not replace CPU, memory,
temporary-storage, or disk claims. A queued job reports what blocks it in `blocked_by`:
`repository:*` and `gate-tree:*` for its lane, `runtime:local` or `capability:<name>` for an
exclusion from this machine, and `resource:<name>` for each claimed resource that is short.
These blockers appear in `agc list --json`, `agc show`, and the TUI's selection and detail
views.

First fit keeps the machine busy, but by itself it lets one job wait indefinitely: a job that
needs a large share of one resource stays short while later, smaller jobs keep fitting.
Preemption shortens that wait when the later work is checks, which can be stopped and run
again. It does not bound the wait; see [the starvation gap](#the-starvation-gap) below.

- **The starving job.** Only one queued job may preempt at a time: the oldest job, by
  submission sequence across all repositories, that is blocked by `resource:*` shortages alone
  and whose complete claim fits this machine when idle. A job that also waits on its repository
  could not start even with the capacity, and a claim accepted only because an overflow pool
  offers it can never run here, so neither preempts anything.
- **When.** The starving job may preempt once it has been queued for `preempt_after` seconds,
  fractions included, counted from when it last entered the queue: its submission, or the moment a preemption
  returned it to the queue. A check that was just requeued therefore waits the whole
  `preempt_after` again before it may stop other checks in its turn. A `land` does not wait: it
  holds up every later landing in its repository, which costs far more than restarting checks,
  so it may preempt as soon as it is the starving job.
- **Overflow first.** In every pass the broker first admits everything that fits here, then
  places jobs in [overflow pools](#overflow-pools), and only then preempts. A starving job that
  a pool can take now is placed there instead, even if `preempt_after` has elapsed, while the
  default `preempt_after` (`300` seconds) stays independent of overflow `wait_threshold`.
- **Victims.** Victims are running `check` jobs on this machine that were submitted after the
  starving job. They are taken latest submission first (among equal submissions, latest start
  first) until their claims cover every shortage; a victim whose claim is not needed after all
  is then spared, earliest submission first, so the set is minimal. The work that is restarted
  thus belongs to the jobs that arrived last. When no set of victims would let the starving job
  fit, nothing is preempted and it keeps waiting. A `land`, a `full`, and a retained `merge` are never victims, because they hold
  barriers or produce reusable receipts. Neither is a job placed in an overflow pool, a check
  submitted with `--no-preempt`, or a check that was already preempted twice, which bounds how
  often one job is restarted.
- **Stopping and requeueing.** A victim is stopped exactly like a cancelled job: its process
  group or cgroup is signalled, it keeps its claim until every descendant is gone, and its
  scratch and enforcement state are reclaimed. While it is being stopped the row stays
  `running` with `cancel_requested` set and `failure_reason` `preempted`. Instead of becoming
  terminal it then returns to `queued` with its original sequence, creation time, and submitted
  environment, a fresh resource receipt, and `attempts` raised by one; `preempted_by` names the
  job it was stopped for. Its log receives the line
  `AGCoord: preempted by <run> at <time>, attempt <n>; requeued at its original position as attempt <n+1>`,
  and the next attempt appends to the same log. The client that submitted the check keeps
  waiting and receives the final attempt's status. A caller's own `agc cancel` during the stop
  wins: the job ends `cancelled`.
- **A victim that passes first.** A victim whose command passes on its own before the broker
  has delivered any stop to it keeps that verdict: the requested stop is withdrawn, the row ends
  `passed`, and it is not run again. A victim that fails in that window is requeued like any
  other, because the requested stop may have caused the failure: its
  [child CPU leases](#child-cpu-leases-for-parallel-tools) are cancelled as soon as the stop is
  requested.
- **A victim that is not reclaimed.** A victim is requeued only when everything its attempt
  held was reclaimed cleanly, judged by the final state of each enforced resource: the outcome
  of the last cancel, finish, or cleanup step its backend recorded for it. A cancel that gave up
  waiting for a cgroup to empty, followed by a clean finish and cleanup, counts as clean; the
  victim is requeued and its log, and the broker's diagnostics, receive
  `AGCoord: incident: the cancel of <victim> for the preemption requested by <run> failed (<backend> <resource>: <code>); the attempt was reclaimed cleanly afterwards, so <victim> is requeued`.
  If that last step failed instead, for example because a cgroup leaf is still populated when it
  is to be removed, the row ends `interrupted` with exit status 125 and `failure_reason`
  `preemption-reclaim-failed`, its resource receipt keeps the failed events, and its log receives
  `AGCoord: preempted by <run> at <time>, attempt <n>; not requeued, because the resources of that attempt were not reclaimed cleanly (preemption-reclaim-failed)`.
  Like a cancelled job whose cleanup fails, the row releases its claim at that transition,
  although the host may still hold what was not reclaimed; the receipt says what that is.
- **What waits while victims stop.** From the moment a preemption is requested until its
  victims are gone, a job submitted after the starving job is not admitted here if admitting it
  would leave the starving job unable to fit once the victims are gone: for some resource both
  claim, the units held now, the stopping victims included, plus both claims exceed the
  capacity. Such a job reports `preemption:<starving_run_id>` in `blocked_by`, so the capacity
  being released reaches the starving job. A later job that claims nothing the starving job
  claims, or that fits beside it, and every older job is admitted as usual, and a waiting job
  may still be placed in an overflow pool.
- **A victim that does not stop.** If a victim is still present five seconds after the owning
  broker first delivered its stop (its first signal, or the kill of its cgroup), the broker
  abandons that preemption. The five seconds run from that delivery, not from the request, so a
  replacement broker that completes a request its predecessor made gives the victim the whole
  bound. Later jobs stop waiting behind the abandoned preemption, and the victim keeps its claim
  until its processes are really gone, so nothing is admitted into its claim while any of them
  still runs. The broker records
  `AGCoord: incident: <victim> was still running 5 seconds after it was told to stop for the preemption requested by <run>; …`
  in the victim's log and in its own diagnostics. The victim is still requeued once it
  disappears, and the starving job may preempt other checks meanwhile.

A requested preemption is durable. A replacement broker adopts the live victim, stops it,
requeues it, and only then admits the starving job. So that a preempted check can restart
unchanged, the broker keeps the submitted environment of a running check that may still be
preempted in the owner-only spool until that attempt ends. It keeps none for a job that is
never a victim, and a starting broker discards any environment or requested stop it finds kept
for a row that is not running.

A check that is safe to run twice is not necessarily safe to restart after it was killed. A
victim's attempt is ended by `SIGTERM`, then `SIGKILL` after two seconds, or by the kill of its
cgroup, so it can leave behind state that a clean finish would have removed, such as a stale
`.git/index.lock`, and its next attempt starts in that state. Submit a check that is not safe
in either sense with `--no-preempt`.

#### The starvation gap

Preemption never stops a `land`, a `full`, a `merge`, a check submitted with `--no-preempt`, or
a check that was already preempted twice, and admission holds nothing back for a waiting job.
A job whose shortage can be covered only by such work may therefore wait indefinitely, for as
long as such work keeps starting. For example, on a host with four CPUs, a check that claims all
four waits for as long as two-CPU `full` jobs from other repositories keep overlapping one
another, because one of them always holds two CPUs and none of them is ever stopped. AGCoord
accepts this gap rather than idling capacity for a job that cannot start yet; such a job starts
once that work stops arriving, or earlier in an [overflow pool](#overflow-pools) when its
submitter allowed overflow.

### Resource bindings and enforcement

Capacity and enforcement are separate contracts. An unbound name is always a generic
`admission-unit`, even when it is spelled `cpu`, `memory`, or `disk`: AGCoord schedules it but
does not claim to have constrained or measured the process. Bind selected capacity names before
the broker starts with the `bindings` section of `config.json`:

```json
{
  "bindings": {
    "cpu": {
      "kind": "cpu",
      "unit": "logical-cpu",
      "mode": "required",
      "backend": "cgroup-v2"
    },
    "memory": {
      "kind": "memory",
      "unit": "bytes",
      "mode": "required",
      "backend": "cgroup-v2"
    },
    "memory_pressure": {
      "kind": "memory-high",
      "unit": "bytes",
      "mode": "required",
      "backend": "cgroup-v2"
    },
    "swap": {
      "kind": "swap",
      "unit": "bytes",
      "mode": "required",
      "backend": "cgroup-v2"
    }
  }
}
```

A binding contains exactly `kind`, `unit`, `mode`, and `backend`, plus an optional `exec` on a
`tmpfs` binding (see [bounded tmpfs scratch](#bounded-tmpfs-scratch)). The supported typed pairs are
`cpu/logical-cpu`; `memory`, `memory-high`, `swap`, `tmpfs`, or `storage` with `bytes`;
`io-bandwidth` with `bytes-per-second`, `read-bytes-per-second`, or
`write-bytes-per-second`; `io-operations` with `operations-per-second`,
`read-operations-per-second`, or `write-operations-per-second`; `io-weight/weight`;
`inodes/inodes`; and `processes/processes`. `generic/admission-unit` remains available for an
explicitly typed admission-only resource. `admission-only` requires a null backend,
`best-effort` runs when its backend or unit is unavailable but records that it was not applied,
and `required` fails the row with exit status 125 and
`failure_reason=resource-enforcement-failed` before releasing the blocked worker launcher. A
binding does not create capacity; its name must still be present in the `capacities` section
before a job can request it.

Backends expose a sanitized capability probe and the idempotent lifecycle `prepare`, `attach`,
`usage`, `finish`, `cancel`, and `cleanup`. Attach happens before user code can start; usage,
finish, cancellation, and cleanup remain owned by the broker. The core currently defines this
backend-neutral seam, a Linux cgroup v2 lifecycle backend, and a Linux project-quota scratch
backend. Controller values, provisioned filesystems, process-specific adapters, and executors
remain separate implementations; a configured backend is never silently treated as successful.

### Overflow pools

An overflow pool extends this machine's capacity with capacity somewhere else: another machine
the operator owns, or a machine a cloud provider creates for one run. The coordinator core does
not know which. A pool is only a name, its own capacities and runtime capabilities, an optional
privileged-container flag, and an *executor* command; reaching the other machine is the
executor's job, so provider-specific behavior stays in adapters.

`local` and `overflow_pools` share one unified runtime capability model. The host runtime is
always named `local` (`"privileged": false`, native host execution with no local container
runtime). When the broker starts, it probes whether an admitted local process can create an
unprivileged user namespace (exercising `bwrap` when installed, under the admitted AppArmor
profile when confined) and adds `"userns"` to `local` automatically when that probe succeeds,
merged with any optional top-level `"capabilities"` list in `config.json`.

Pools are the ordered `overflow_pools` list in `config.json`, and like every other section they
are frozen when the broker starts:

```json
{
  "capacities": {"jobs": 8, "cpu": 16},
  "capabilities": ["kvm"],
  "overflow_pools": [
    {
      "name": "workstation",
      "capacities": {"jobs": 2, "cpu": 32},
      "executor": ["run-on-workstation"],
      "wait_threshold": 120,
      "max_runtime": 3600,
      "privileged": true,
      "capabilities": ["kvm"]
    },
    {
      "name": "cloud",
      "capacities": {"jobs": 4, "cpu": 64},
      "max_claim": {"cpu": 16},
      "executor": ["run-in-cloud"],
      "wait_threshold": 300,
      "max_runtime": 3600,
      "budget": {"cpu_hours_per_day": 100, "rollover_utc": "08:00"},
      "cpu_hour_price": 0.034
    }
  ]
}
```

Each pool has only these keys. `name` follows the resource-name rules, is unique, and may not
be `local` (reserved for the host runtime). `capacities` lists every resource the pool offers
as positive integers, `jobs` included; a claim on a name the pool does not list can never be
placed there. `max_claim` is optional and gives, per listed resource, the largest amount one
job may claim there, for a target that can run many jobs but only up to a certain size each; a
job above it skips this pool and is offered to the next one. `executor` is a non-empty command.
`wait_threshold` is whole seconds, default `0`. `max_runtime` is required whole seconds.
`privileged` is an optional boolean, default `false`; setting `"privileged": true` marks the
pool's containers as privileged and automatically implies the `"userns"` capability without
requiring manual duplication. Privileged pools remain eligible for ordinary overflow jobs—no
per-job permission flag is required to land on one. `capabilities` is an optional list of unique
capability names following the resource-name rules (as is top-level `"capabilities"` for
`local`). `budget` is optional: `cpu_hours_per_day` is a positive number and `rollover_utc` is
the `HH:MM` UTC time at which a new budget day begins, default `00:00`. `cpu_hour_price` is an
optional non-negative number that clients may use to display an estimated cost; the broker never
enforces anything with it. Both `local` runtime metadata and pool metadata are published in the
broker's ownership handshake. An invalid capability list or pool is `broker-config-invalid`. To add
or update `overflow_pools` on an already-running managed host without disrupting active work or
overwriting existing pools with `agc host config --write --force`, follow
[updating configuration and overflow pools on a live host](native_host.md#updating-configuration-and-overflow-pools-on-a-live-host).

A job submitted with `agc run`, `agc full`, or `agc land` is eligible for overflow by default
when its checkout contains `.agcoord/overflow.json`. Pass `--no-overflow` to keep a job on this
machine, for example when it needs local files, tools, or ignored state. Pass `--overflow` to
require a valid repository declaration. These options are mutually exclusive. Repeat
`--require-capability NAME` to require runtime capabilities (such as `userns`) and repeat
`--runtime NAME` (`local` or configured pool names) to restrict which runtimes may run the job;
`CoordinatorClient.submit` and `submit_land` expose `require_capabilities` and `runtimes`. No
per-job privilege selector exists: jobs request capabilities or runtimes, and pool configuration
remains the single source of truth for container privilege. Submission fails closed before
queuing when an identifier or runtime is unknown, when selecting only overflow runtimes
conflicts with `--no-overflow` or lacks a valid `.agcoord/overflow.json`, or when no single
allowed runtime offers every requested resource and capability together. A retained `merge`
never overflows.

This machine is always preferred when `local` is an allowed runtime and holds every required
capability. In each scheduling pass the broker first admits everything that fits locally. Rows
excluded from `local` by `--runtime` or `--require-capability` report `runtime:local` and/or
`capability:<name>` in `blocked_by` and never preempt local work. Next, the broker
takes the earliest queued job that allowed overflow, is blocked only by `resource:*`,
`preemption:*`, `runtime:local`, and/or `capability:*` reasons, and has been queued for at
least a matching pool's `wait_threshold`, and places it in the first configured pool that is
allowed by `--runtime`, holds every required capability, and has room for its complete claim and
enough budget. Lane and barrier rules are unchanged: a placed job
still counts as active work in its repository and worktree, and a job waiting for a land or for
its worktree never overflows around it. For an eligible job, a claim larger than this machine's
capacity or requiring a capability `local` lacks is accepted at submission when one allowed pool
satisfies both the complete resource claim and every required capability together, and reaches
that pool by the same rule. With `--no-overflow`, claims and capabilities must fit `local`.
A job that waits behind an in-flight
[preemption](#first-fit-admission-and-preemption) may overflow after that threshold:
placement consumes the pool's capacity rather than local capacity, so it does not delay the job
the preemption serves. A starving job preempts local checks only when no pool can take it. Pool
allocations are excluded from local shortages, and a job placed in a pool is never a preemption
victim, while its repository and worktree exclusions still apply.

A placed job's row moves to `running` with its `pool` set. Its worker on this machine is
`EXECUTOR... -- COMMAND...`, started in the executed checkout (the gate tree for
`full`/`land`) with the usual admission
environment (after stripping any caller-supplied `AGCOORD_OVERFLOW_*` variables) plus
`AGCOORD_OVERFLOW_POOL`, `AGCOORD_OVERFLOW_MAX_RUNTIME` in seconds,
`AGCOORD_OVERFLOW_RESOURCES` (the claim as a JSON object), `AGCOORD_OVERFLOW_CONTRACT` (the
row's `resource_contract`, which gives each claim's kind and unit), and
`AGCOORD_OVERFLOW_PRIVILEGED` (`1` when placed onto a `"privileged": true` pool, `0` on an
unprivileged pool; unset on `local` runs). Variables a submission reserves for a run on this
machine, such as the workstation key described under [GCP result cache](#gcp-result-cache), are
added only to a job admitted locally and never reach a placed job's executor. The executor runs
the command on the pool's machine, relays its output, and exits with its status; that exit
status, the log, and cancellation are those of any other worker, so stopping the job signals the
executor's process group and the executor must stop the remote run. An executor that cannot
obtain a verdict at all, for example because the connection was lost or the remote machine was
preempted, exits with status 125. The job then ends `interrupted` with
`failure_reason=overflow-executor-failed` rather than `failed`, and for a `land` nothing is
published and no gate verdict is claimed. A `land` keeps its local worker for preflight
and publication, and only its gate command goes through the executor. The claim is held by the
pool, not by this machine: it does not appear in `allocations`, and no local enforcement is
applied. Enforcing the claim becomes the executor's duty: it must run the command in an
environment sized to the claim, such as a container or a machine with those CPU and memory
limits, and must exit 125 instead of running a claim it cannot enforce. Each enforced binding,
`required` or best-effort, is recorded in the receipt as a `prepare` event with status
`recorded` and code `overflow-delegated`, and `agc list` shows such a job's enforcement as
`delegated`.

A placed job that has held its pool capacity for `max_runtime` is stopped like a cancelled job
and ends `failed` with exit status 124 and `failure_reason=overflow-runtime-exceeded`. The limit
is what bounds a budget reservation, and it stops a forgotten remote machine from running on.
An executor for a paid pool must also set the same limit on the remote resource itself, so that
a lost broker cannot leave it billing.

A budget counts CPU-hours of the `cpu` claim per budget day. Placement reserves the worst case,
the claimed CPUs for the whole `max_runtime`, in the same transaction that admits the row, so
concurrent placements cannot exceed the budget. When the run ends the charge becomes the claimed
CPUs for the time it actually held the pool, and the rest is returned. A job whose reservation
does not fit today's remaining budget is not placed and keeps waiting for local capacity. The
budget counts claimed CPU time, not a provider's bill; use the provider's own billing alert as
the backstop. `agc clear` does not reset a budget day.

#### Repository opt-in and the job image

A repository declares that its jobs can run away from this machine with one file,
`.agcoord/overflow.json`, which names its toolchain in exactly one of two ways:

```json
{"dockerfile": ".agcoord/Dockerfile"}
```

```json
{"dockerfile": ".agcoord/Dockerfile", "context": "."}
```

```json
{"image": "registry.example/team/toolchain:2026-11"}
```

`dockerfile` is a relative path inside the repository; the toolchain image is built from it on
this machine with Docker or Podman, using the optional relative `context` directory inside the
repository (or the Dockerfile's directory when `context` is omitted) as build context and the
tool's own layer cache. The container tool reads `context` directly (honoring `.dockerignore` or
`.containerignore`), so a toolchain Dockerfile with a wider context should `COPY` only specific
tracked files such as a dependency lockfile rather than `.`. `image` names a prebuilt image
instead and cannot be combined with `context`. Because the file is part of the
checkout, a branch that changes its toolchain changes it in the same commit. An absent file keeps
default submissions local. A present but invalid file, including a broken symlink or missing
Dockerfile, refuses default submissions before anything is queued. `--overflow` also refuses an
absent file; `--no-overflow` skips the file entirely. The Python client exposes the same choices
as `overflow=None` (default), `True`, and `False` on `submit` and `submit_land`.

The built-in executors run a job from an image made of that toolchain plus the job's source as
one copied layer at `/workspace`, which is also the working directory. Nothing is compiled when
the image is built. The source is the working tree's tracked and untracked files; ignored files
are never included, so a local `.env` or credential file does not leave this machine, and a job
that needs one cannot overflow. A `full` or `land` runs from a clean exact head, so its image
holds exactly that commit. Submodule contents are not included.

Enforced claims become container limits: `cpu` to a CPU limit, `memory` to a memory limit with
no additional swap unless `swap` is claimed, `memory-high` to a memory reservation, `tmpfs` to a
sized `/tmp` (with `inodes` as its inode limit), and `processes` to a process limit. A `required`
claim of any other kind is refused with exit status 125 before anything runs; a best-effort one
is skipped.

SSH and Batch preserve the `tmpfs` binding's executability: `"exec": true` adds `exec` to
the `/tmp` mount options; false or omitted retains the container runtime's `noexec` default.
Whenever a built-in executor applies scratch, it sets `TMPDIR`, `TMP`, and `TEMP` to `/tmp`
inside the container, overriding those names passed with `--env`. Admission-only or skipped
scratch claims do not mount scratch or inject these variables.

Remote container process contract:

- **User:** runs as the `USER` configured in the repository's toolchain image (or container root
  if the Dockerfile leaves `USER` unset).
- **Umask:** `0077`, matching local managed broker rows; the built-in executors wrap the command
  in `/bin/sh -c 'umask 0077 && exec "$@"'` before exec'ing user code.
- **`no_new_privs`:** enabled (`--security-opt=no-new-privileges`, `NoNewPrivs=1`) on Docker/Podman
  and GCP Batch containers, matching local rows. Cloud Run runs in its own sandbox without setuid
  privilege escalation.
- **Capabilities and `userns`:** unprivileged pools (`"privileged": false`, the default) do not
  provide unprivileged user namespaces (`bwrap --unshare-user` fails inside default container
  confinement). Jobs that need user namespaces must pass `--require-capability userns` so they
  stay on `local` when supported or route only to a pool configured with `"privileged": true` (or
  explicit `"capabilities": ["userns"]`).
- **Checkout:** `/workspace` holds only tracked and untracked working-tree files (for `full` and
  `land`, the exact clean commit); `.git`, gitignored files, and submodule contents are absent.

#### SSH executor

`agc overflow-ssh` runs a placed job in a container on another machine the operator owns:

```json
{
  "name": "workstation",
  "capacities": {"jobs": 2, "cpu": 32, "memory": 68719476736},
  "executor": ["agc", "overflow-ssh", "builder@workstation"],
  "wait_threshold": 120,
  "max_runtime": 3600
}
```

The destination needs a non-interactive SSH login for the broker's user and Docker or Podman.
It needs no registry and no forge credentials. `--ssh-option OPTION` adds an `ssh -o` option
such as `Port=2222`, `--runtime` selects the local container tool, `--remote-runtime` the
destination's (default `docker`), `--privileged` passes `--privileged` to the remote container
`run` command (also enabled automatically when the broker sets `AGCOORD_OVERFLOW_PRIVILEGED=1`
for a `"privileged": true` pool), and `--env NAME` passes one environment variable into the
container; by default the container receives none of the submitter's environment.

For each job the executor sends the toolchain image over SSH unless the destination already
holds it, streams the source and builds the job's image there, and runs the command in a
container named `agcoord-RUN_ID` with the limits above. The destination itself stops the
container after the pool's `max_runtime`, so a lost connection or broker cannot leave it
running. When the job ends or is stopped, the container and the job's image are removed; the
toolchain image stays on both machines as a cache. The executor exits with the command's
status, or 125 when SSH or the container tool failed before the command produced a verdict.

#### GCP project for overflow executors

The executors that run jobs on Google Cloud need a project with billing, an image repository,
and an identity for the job machines. The Terraform module in
`packaging/terraform/gcp-overflow/` provides them as infrastructure as code. It manages:

- the project and its billing link, with deletion prevented;
- the Artifact Registry, Batch, Cloud Run, Compute Engine, Cloud Logging, Cloud Storage, and IAM
  APIs;
- one Docker repository in Artifact Registry, whose cleanup policies delete images under the
  `run` package after `job_image_retention_days` (default 2) and under the `toolchain` package
  after `toolchain_image_retention_days` (default 30). An executor deletes a job's image itself
  when the job ends; the policies are the backstop for an executor that was killed first;
- a private result-cache bucket, `PROJECT_ID-result-cache` in the module's region, described
  under [GCP result cache](#gcp-result-cache) below;
- a service account for job machines that may only report to Batch, write logs, read that
  repository, and read and add to the result cache. It holds no forge credentials;
- a result-cache client service account, `agcoord-result-cache-client`, that may only read and
  add to the result cache, for workstations;
- for each member of the optional `submitters` set, the grants to push images, start jobs as the
  job service account, and read job output. A project owner needs none of them.

Nothing the module creates runs or bills by itself; machines exist only while an executor runs
a job, and stored images and cached results cost by size. The module has no provider configuration, so call it from
a small root of your own, kept outside any repository:

```hcl
provider "google" {}

module "overflow" {
  source = "/path/to/agcoord/packaging/terraform/gcp-overflow"

  project_id      = var.project_id
  billing_account = var.billing_account
}

output "overflow" {
  value = module.overflow
}
```

`project_id` and `billing_account` are required. `region` defaults to `us-central1`,
`repository_id` to `agcoord-overflow`, and `org_id` or `folder_id` place the project when an
organization requires it. The outputs are the facts an executor needs: `project_id`, `region`,
`registry` (the image path prefix), `runner_service_account`, and the two package names, plus
`result_cache_url`, the `gs://` URL of the result-cache bucket, and `result_cache_client_email`,
the email of the result-cache client service account.

Terraform state and variable files name your project and billing account. They are operator
state, like the broker's `config.json`: keep them in that root, never in a repository. The
provider reads application default credentials, or a short-lived token from
`GOOGLE_OAUTH_ACCESS_TOKEN="$(gcloud auth print-access-token)"`. To adopt a project that already
exists, `terraform import` it, and any repository or enabled API it already has, before the
first apply. For mapping `terraform -chdir=~/.config/agcoord/gcp-overflow output -json overflow`
and raw byte capacities into `~/.local/state/agcoord/config.json` on a running managed host, see
[updating configuration and overflow pools on a live host](native_host.md#updating-configuration-and-overflow-pools-on-a-live-host).

#### GCP result cache

A job machine exists only for one run, so a cache a check writes inside it, such as a
content-addressed test-result or build cache, is lost with the machine, and the next job computes
everything again. The module's result-cache bucket gives such a cache a place that outlives the
machine and that the submitting workstations share with the jobs they place.

The bucket is private: uniform bucket-level access is on, so no object carries its own access
list, and public access prevention is enforced, so no grant can make it public. Objects are
deleted `result_cache_retention_days` (default 30) days after they were written; a job that
misses an expired entry computes the result again. Using the bucket is optional: a repository
that does not point its cache at it leaves it empty, and an empty bucket costs nothing.

The module grants the bucket to exactly two identities, each with `roles/storage.objectViewer`
(read and list objects) and `roles/storage.objectCreator` (create objects):

- the job service account, so every command a job runs, including the code under test, holds
  these powers;
- the result-cache client service account, so any process that can read a workstation's key for
  it holds them.

Neither role can delete an object, and replacing an existing object needs the delete permission,
so no automated identity can remove or overwrite a cached result. A repository's cache tool must
therefore add each entry only when its name is absent, for example by uploading with the
`ifGenerationMatch=0` precondition, and treat a refused write of an existing name as an entry
that is already stored. Members of `submitters` get no grant on the bucket; their workstations
use the client service account. Project owners keep full control through their project role, so
deleting or replacing entries, such as removing a false entry or emptying the cache, is an
administrator action that a project owner performs with their own credentials, for example
`gcloud storage rm gs://my-overflow-project-result-cache/PATH`.

Anyone who can add to the bucket can cause cached results to be reused, so a writer can make a
later check report a result it never computed. Give the client service account's key only to
workstations whose checks you would accept, and do not grant the bucket to anyone else. A
repository's cache should key its entries by everything that determines the result, so an entry
is reused only for the same inputs, but that key does not protect against a writer who stores a
false entry under a correct key before the true one exists.

A workstation reaches the bucket with a key for the client service account. A project owner
creates it outside Terraform, so no private key enters Terraform state, with one command on the
workstation:

```bash
agc overflow-gcp-batch create-result-cache-key \
  --project my-overflow-project \
  --iam-account agcoord-result-cache-client@my-overflow-project.iam.gserviceaccount.com
```

`--project` takes the `project_id` output and `--iam-account` the `result_cache_client_email`
output; `agc overflow-gcp-run create-result-cache-key` is the same command. It asks the operator's
`gcloud` login for a new key and stores it at `~/.config/agcoord/result-cache-client.json`, or at
`--output PATH`, readable only by the workstation's user: `gcloud` writes the key under umask
`077` into a private directory beside that path, and only the complete key is moved into place,
with mode `0600`, so a failure leaves no partial key behind. The command prints the new key's ID
but never its private material, and it never writes the key to standard output. It refuses to
overwrite an existing file. `--replace` puts a new key in its place and prints the replaced key's
ID together with the `gcloud iam service-accounts keys delete` command that revokes it, because a
replaced key stays valid until it is revoked. If the organization enforces the
`iam.disableServiceAccountKeyCreation` constraint, key creation fails until an organization policy
administrator exempts the project. List and revoke keys with `gcloud iam service-accounts keys
list` and `gcloud iam service-accounts keys delete`, passing the same `--iam-account`.

Jobs on the workstation then find the key without any shell profile or service environment. While
the broker has a Google Cloud pool, one whose `executor` runs `agc overflow-gcp-batch` or
`agc overflow-gcp-run`, and the key file exists, every `run`, `full`, and `land` job admitted on
this machine receives `GOOGLE_APPLICATION_CREDENTIALS` naming it. A value the submitter's
environment already sets, even an empty one, is kept instead. The repository's cache tool finds
the key through that variable, like any Google Cloud client library. The default key is
`.config/agcoord/result-cache-client.json` under the submitting environment's `HOME`; a key kept
elsewhere is named by adding `--result-cache-key PATH` to each Google Cloud pool's `executor`,
where a leading `~/` stands for that `HOME`. The first Google Cloud pool that names a key decides,
and the executor itself ignores the option. Without a Google Cloud pool, or without the key file,
jobs run with their environment unchanged and nothing warns about a missing key, so a machine that
does not use Google Cloud overflow never needs one.

The key never leaves the workstation. A job placed in a pool does not receive the variable, and
the Google Cloud executors never pass `GOOGLE_APPLICATION_CREDENTIALS` into a job's container,
even when an `executor` lists `--env GOOGLE_APPLICATION_CREDENTIALS`; they leave it out and say so
in the job's log, so the job reaches Google Cloud as the job service account.

A user's application default credentials do not confine a workstation the same way, even with
service account impersonation. `gcloud auth application-default login
--impersonate-service-account=EMAIL` writes a credential file whose source credential is the
user's own refresh token; impersonation is only the first exchange the client library makes.
Code that can read that file can use the refresh token directly and act as the user, with every
role the user holds, which for a project owner is the whole project. A client service account
key carries only the client account's two bucket roles.

AGCoord itself never reads or writes the bucket; the repository's own cache tool does. To point
a repository's cache at it:

1. Give the repository's cache tool the bucket as its remote location, read from an environment
   variable the repository chooses, so the project's URL stays operator state rather than a
   committed value. A Batch or Cloud Run job reaches Cloud Storage as the job service account
   through its machine's own credentials; a submitting workstation uses the client service
   account's key.
2. Set that variable to the `result_cache_url` output in the environment that submits the job,
   for example `CHECK_CACHE_URL=gs://my-overflow-project-result-cache`.
3. Add `--env CHECK_CACHE_URL` to each Google Cloud pool's `executor`, so the variable reaches the
   job's container; by default the container receives none of the submitter's environment.
   `GOOGLE_APPLICATION_CREDENTIALS` is never passed: a job uses the job service account.

A job that runs locally reads and adds to the same bucket with the workstation's client key,
which the broker gives it as described above, so local and overflow runs share one cache.

#### GCP Batch executor

`agc overflow-gcp-batch` runs a placed job on a Google Cloud Batch machine that exists only for
that run. Its options are the outputs of the Terraform module above:

```json
{
  "name": "cloud",
  "capacities": {"jobs": 4, "cpu": 64, "memory": 274877906944},
  "max_claim": {"cpu": 32},
  "executor": [
    "agc", "overflow-gcp-batch",
    "--project", "my-overflow-project",
    "--region", "us-central1",
    "--registry", "us-central1-docker.pkg.dev/my-overflow-project/agcoord-overflow",
    "--service-account", "agcoord-overflow-runner@my-overflow-project.iam.gserviceaccount.com"
  ],
  "wait_threshold": 300,
  "max_runtime": 3600,
  "budget": {"cpu_hours_per_day": 100}
}
```

This machine needs Docker or Podman and a `gcloud` that is signed in with permission to push to
the repository and submit Batch jobs; the executor holds no credentials of its own and asks for
everything through that `gcloud`. `--machine-type` fixes the Compute Engine machine type; without
it Batch chooses one that fits the job. `--spot` uses a Spot machine, which is cheaper but can be
preempted. `--runtime` selects the local container tool, `--privileged` adds `--privileged` to
the Batch container's `options` (also enabled automatically when `AGCOORD_OVERFLOW_PRIVILEGED=1`
is set for a `"privileged": true` pool), `--env NAME` passes one environment
variable into the container except `GOOGLE_APPLICATION_CREDENTIALS`, and `--poll-interval` sets
how often the job is checked. `--result-cache-key PATH` names the workstation's result-cache key
for jobs that run on this machine, as described under [GCP result cache](#gcp-result-cache), and
does nothing for a placed job.

For each job the executor builds the job's image on this machine, pushes the repository's
toolchain image as `toolchain` and the job's image as `run` in the registry, and submits one
Batch job named `agcoord-RUN_ID` with a single task that runs the command in that image by
digest. The registry login uses a short-lived access token in a private temporary directory, so
the operator's own container configuration is neither read nor changed. Because the toolchain
stays in the registry, later jobs upload only their source layer.

The machine is sized to the job: its CPU and memory requirements are the job's enforced `cpu` and
`memory` claims, or, for a job whose claims are only admission units, the claims named `cpu` and
`memory` in bytes. Enforced claims are also applied to the container as the limits described
above. The task carries the pool's `max_runtime` as its own maximum run duration and is never
retried, so a lost broker cannot leave a machine running and the command never runs twice.

The job's output is relayed from Cloud Logging, so it appears a few seconds late. To preserve
the same line-for-line order as a local row (`stdout` and `stderr` merged onto one log file), the
container wrapper merges standard error into standard output at capture and tags each emitted line
or chunk with a per-run token and monotonic sequence number (splitting long lines under 16 KiB).
Reads stay anchored at the job's creation timestamp while advancing a `receiveTimestamp` lookback
window and deduplicating by `insertId`, so buffered lines or entries indexed out of
emission-timestamp order are still collected; `LogRelay` strips the sequence tags, holds
out-of-order chunks until earlier sequences arrive, drains several polls after the job reaches a
terminal state before deleting it, and flushes any remaining lines in sequence order on finish
(reporting on standard error if a sequence gap remained). `agc overflow-ssh` also merges standard
error into standard output inside the container before streaming. The executor
exits with the command's status. A job stopped by Batch at the runtime limit exits 124. Any other
failure that is Batch's rather than the command's, such as a preempted Spot machine, a job that
could not be submitted, or an image that could not be pushed, exits 125, so the run ends without
a verdict. When the job ends or is stopped, the Batch job, the pushed job image, and the local
job image are deleted; deleting the Batch job is what stops its machine.

The executors' standard error is the job's log, so the progress reports `gcloud` prints there,
such as "Creating job... Done." and console links that name the project, are kept out of it.
What `gcloud` said is shown only when one of its commands fails, as part of the error.

Both Google Cloud executors keep following a job through `gcloud` calls that fail for a while:
a failed check of the job's state is repeated at the next poll, and only twelve failures in a
row end the run without a verdict.

#### GCP Cloud Run executor

`agc overflow-gcp-run` runs a placed job as a Cloud Run job that exists only for that run. It
takes the same project, region, registry, and service account as the Batch executor, and the
same `--env` and `--result-cache-key` options; `--env` never passes
`GOOGLE_APPLICATION_CREDENTIALS` into the job here either. It is not faster to start: measured
once in `us-central1`, a job's first output arrived 89 seconds after the executor started,
against 81 seconds on Batch. What it offers is no machine to manage, at the cost of being smaller
and limiting less:

```json
{
  "name": "cloud-run",
  "capacities": {"jobs": 8, "cpu": 32, "memory": 137438953472, "tmpfs": 34359738368},
  "max_claim": {"cpu": 8, "memory": 34359738368},
  "executor": [
    "agc", "overflow-gcp-run",
    "--project", "my-overflow-project",
    "--region", "us-central1",
    "--registry", "us-central1-docker.pkg.dev/my-overflow-project/agcoord-overflow",
    "--service-account", "agcoord-overflow-runner@my-overflow-project.iam.gserviceaccount.com"
  ],
  "wait_threshold": 120,
  "max_runtime": 3600
}
```

One Cloud Run job gets at most 8 CPUs and 32 GiB of memory, so give the pool a `max_claim` that
says so; a larger job then goes to the next pool instead of failing here. Cloud Run does not
support privileged containers: if a privileged placement (`AGCOORD_OVERFLOW_PRIVILEGED=1`) or
`--privileged` reaches `agc overflow-gcp-run`, it fails closed with exit status 125 before
building or pushing any image. Cloud Run can enforce
only `cpu`, `memory`, and `tmpfs` claims. A limit on `swap` always holds, because Cloud Run has
none. A `required` claim of any other kind, such as an inode or process limit, is refused with
exit status 125 before anything is built, so list in the pool's `capacities` only the resources
Cloud Run provides: a job that claims anything else is then never placed here.

The executor chooses the smallest Cloud Run size that holds the claim, from 1, 2, 4, 6, or 8
CPUs. Scratch space is held in memory on Cloud Run, so a `tmpfs` claim becomes sized in-memory
scratch space at `/tmp` and is added to the memory the job is given. A job that claims no memory
gets the most its CPU count allows, 4 GiB for each CPU; claim memory to pay for less. The job
carries the pool's `max_runtime` as its own timeout and is created to never retry, and the
executor reads the created job back and refuses to execute one that Cloud Run would retry.

Cloud Run's [in-memory volume API](https://cloud.google.com/run/docs/reference/rest/v1/Volume#EmptyDirVolumeSource)
exposes a medium and size limit, with no `exec`/`noexec` mount option or documented
executability guarantee. The executor therefore refuses a required `tmpfs` binding with
`"exec": true` before building or pushing an image, and skips a best-effort one. Other tmpfs
bindings use Cloud Run's managed mount defaults; they carry no `noexec` guarantee. Use SSH or
Batch when scratch executability must match the local binding.

Cloud Run applies the CPU limit from outside the container, and the container cannot read it:
`nproc` and the processor count Python or a test runner sees can be larger than the CPUs the job
claimed (a 2-CPU job saw 6), and the cgroup limit files are not readable. A command that sizes
its parallelism from the visible processor count oversubscribes its claim and runs slower, so
give it the count to use, for example `pytest -n 2` rather than `pytest -n auto`.

Output is relayed from Cloud Logging: the command's standard output and standard error as the
job's output, and what Cloud Run itself reports about the job on standard error. A line the
command printed as JSON is relayed as JSON. The executor exits with the command's status. When
Cloud Run ends the job without an exit status from the command, for example at a memory limit,
the executor exits 125 and the run ends without a verdict. When the job ends, the Cloud Run job
and both copies of the job's image are deleted; when it is stopped, its execution is cancelled
first.

### Delegated cgroup v2 lifecycle

The built-in `cgroup-v2` backend owns process-tree lifecycle and, when the matching controllers
are delegated, aggregate CPU bandwidth, task counts, memory, swap, and explicitly mapped block
I/O. Both broker implementations expose the same binding and receipt contract; the Rust owner
performs its probe, leaf lifecycle, controller operations, namespace setup, tmpfs supervision,
measurement, recovery, and cancellation inside the single executable described by the
[native broker architecture and security contract](native_broker.md). Configure one exclusive
delegated root and explicitly bind the capacity names before the broker starts:

```json
{
  "capacities": {"jobs": 2, "cpu": 4, "pids": 128},
  "bindings": {
    "cpu": {
      "kind": "cpu",
      "unit": "logical-cpu",
      "mode": "required",
      "backend": "cgroup-v2"
    },
    "pids": {
      "kind": "processes",
      "unit": "processes",
      "mode": "required",
      "backend": "cgroup-v2"
    }
  },
  "cgroup_root": "/sys/fs/cgroup/user.slice/example.slice/agcoord.service"
}
```

```bash
agc run --resource cpu=2 --resource pids=64 -- python -m pytest -q
```

The root must be an absolute, real directory on the unified cgroup v2 hierarchy, writable by
the broker, dedicated to that state directory's owner, and an ancestor of the broker so its
children can be moved within the delegation boundary. Its cgroup2 mount must be read-write with
`nsdelegate`; unprivileged user, cgroup, and mount namespaces must also be available. The probe
creates and removes an empty child to verify delegation, `cgroup.kill`, namespace rooting, and
controller-file protection. It publishes only stable refusal codes and controller
capabilities—never the configured path or an operating-system exception. Missing CPU or PID
controllers, an unsupported valid kind/unit, or a setting that cannot be written and read back
fails a `required` run before user code with exit status 125; `best-effort` records the refusal
and runs without claiming application. A syntactically invalid binding is rejected when the
broker configuration is loaded.

Before creating a typed leaf, AGCoord enables only its requested controllers in
`cgroup.subtree_control` at the configured root and private owner node. Both must be empty inner
nodes for domain controllers; a populated or partially delegated boundary is a preparation
failure, never a reason to run a required job without its limit.

For systemd-managed hosts, place the broker in a dedicated service or scope with `Delegate=yes`
and give AGCoord that unit's delegated cgroup, rather than a slice or the hierarchy root. On
systemd 254 or newer, `DelegateSubgroup=supervisor` keeps the broker out of the inner node that
distributes controllers. Keep systemd as the single writer above that boundary. For
direct delegation, a privileged manager must create one exclusive subtree, place the broker in
a supervisor child of that boundary, and grant only the delegatable directory and membership
files to the broker user; do not recursively change ownership of controller files. The kernel's
[cgroup v2 delegation contract](https://www.kernel.org/doc/html/latest/admin-guide/cgroup-v2.html#delegation)
and systemd's
[delegation guidance](https://systemd.io/CGROUP_DELEGATION/) are the authoritative host setup
references.

Ubuntu hosts may additionally set `kernel.apparmor_restrict_unprivileged_userns=1`. On those
hosts, an otherwise correct systemd delegation can probe as unavailable with
`reason=namespace-mapping-failed`: the unconfined broker is moved into Ubuntu's restrictive
user-namespace profile before it can write its private UID and GID maps. Do not disable that
host-wide policy merely to start the broker, and do not switch a binding to `required` until the
backend probe succeeds. Attaching `userns` permission to a general-purpose Python interpreter,
including one copied into a root-owned virtual environment, is not a narrow workaround: the
broker account could invoke that interpreter directly with arbitrary code under the same
permission. A general-purpose interpreter therefore has no supported AppArmor exception for
this host policy. The Rust executable provides the narrow broker-specific target, but it does not bypass
AppArmor by itself: use the native backend only after the documented host package and broker
profile are installed and verified. Otherwise keep the cgroup backend disabled unless the
administrator deliberately accepts a broader account-level user-namespace opt-in.
The supported service, AppArmor policy, preflight, installation, and real enforced-receipt
procedure are defined by the [native host runbook](native_host.md).

For each run, AGCoord creates an owner-specific, collision-safe leaf and records device/inode
identities plus a random ownership token in the private spool. The existing launcher stays
blocked while the broker moves it into that leaf and verifies membership. On real cgroupfs, the
launcher then creates private cgroup and mount namespace views rooted at the leaf; `nsdelegate`
keeps the parent controller files kernel-unwritable while descendants may only redistribute
resources beneath the run boundary. The configured root and internal isolation marker are
removed from the job environment.

`cpu/logical-cpu=N` is a hard aggregate fair-scheduler bandwidth ceiling, not a worker count or
affinity request. AGCoord uses a fixed 100000µs period and writes `cpu.max` as
`N*100000 100000`; integer allocations therefore need no quota rounding. It deliberately leaves
`cpu.weight` unchanged. It does not write `cpuset.cpus`, so a CPU binding makes no placement,
NUMA, exclusive-core, or affinity claim. Realtime scheduling is outside this bandwidth contract.

CPU peak measurement samples `cpu.stat usage_usec`, divides each usage delta by elapsed wall
time, and rounds a positive fractional result upward to a conservative integer logical-CPU
peak. If `nr_throttled` or `throttled_usec` reports throttling, one deduplicated
`cpu-throttled` resource event is retained. The event is a stable violation fact rather than raw
kernel text; the applied map retains the exact logical-CPU limit.

`processes/processes=N` writes `pids.max=N` for the complete run subtree. The kernel counts tasks,
including threads, so the blocked launcher and every later descendant consume this shared limit;
PID exhaustion returns `EAGAIN` to the owning run and does not cancel sibling runs. Peak usage
comes from `pids.peak` when the host provides it, otherwise from broker samples of
`pids.current`. A nonzero `pids.events max` counter records one deduplicated `pids-limit-hit`
event. Normal completion and cancellation capture final CPU and PID measurements after the leaf
is unpopulated but before it is removed.

`memory/bytes=N` writes the hard `memory.max=N` envelope for the complete run subtree and sets
`memory.oom.group=1`, so a local hard-limit OOM terminates the run as one workload instead of
leaving a partial process tree. With no explicit `swap/bytes` binding on that run,
`memory.swap.max` defaults to `0`; a RAM budget therefore cannot silently consume unbounded host
swap. With no `memory-high/bytes` binding, `memory.high` keeps its disabled `max` default.

`memory-high/bytes=N` writes the reclaim and throttle boundary `memory.high=N`; if no hard memory
binding is present, `memory.max` and `memory.swap.max` retain `max` and
`memory.oom.group` remains `0`. A high value above an explicitly requested hard value is refused
as `memory-limit-impossible`. Crossing the high boundary records `memory-high-throttled`, and
nonzero per-cgroup PSI stall totals record `memory-pressure`; neither is reported as an OOM.

`swap/bytes=N` writes `memory.swap.max=N`. A positive explicit swap budget is refused as
`swap-disabled` when `/proc/meminfo` reports no host swap. Missing controller files, an
undelegated memory controller, malformed counters, and an unmeasurable pressure interface also
fail a required binding before user code begins. The backend reports the greatest observed
`memory.current`/`memory.peak` value under each selected memory name and the greatest observed
`memory.swap.current`/`memory.swap.peak` value under the swap name. It converts final
`memory.events`, `memory.swap.events`, and pressure counters into deduplicated
`memory-max-hit`, `memory-oom`, `swap-limit-hit`, and pressure events before removing the leaf.

A group OOM normally makes the command observable as status `failed`, exit status `137`
(SIGKILL), and `failure_reason=memory-oom`. An explicit user or broker cancellation retains its
own `cancelled` status and is never relabeled as OOM, even if final memory evidence is present.

Configure the schedulable hard-memory capacity below the effective ancestor `memory.max`, after
reserving operator-chosen headroom for the broker, operating system, page cache outside admitted
leaves, and unrelated sibling services. Do not derive it blindly from host `MemTotal`, especially
inside a container or user slice. High-boundary capacity may deliberately overcommit when the
operator wants reclaim pressure, but every safety-critical run should also request a hard-memory
binding. Swap capacity should not exceed the swap the host can lose without harming those
uncontrolled services.

### Bounded tmpfs scratch

The `cgroup-v2` backend can provide a run with bounded in-memory scratch. Bind one `tmpfs/bytes`
name and one `inodes/inodes` name to that backend, and bind a required `memory/bytes` name in the
same run:

```json
{
  "capacities": {
    "jobs": 2,
    "memory": 2147483648,
    "tmpfs": 1073741824,
    "tmpfs_inodes": 131072
  },
  "bindings": {
    "memory": {
      "kind": "memory", "unit": "bytes", "mode": "required", "backend": "cgroup-v2"
    },
    "tmpfs": {
      "kind": "tmpfs", "unit": "bytes", "mode": "required", "backend": "cgroup-v2"
    },
    "tmpfs_inodes": {
      "kind": "inodes", "unit": "inodes", "mode": "required", "backend": "cgroup-v2"
    }
  },
  "cgroup_root": "/sys/fs/cgroup/user.slice/example.slice/agcoord.service"
}
```

```bash
agc run --resource memory=1073741824 --resource tmpfs=536870912 \
  --resource tmpfs_inodes=65536 -- python -m pytest -q
```

The byte and inode requests are an atomic tmpfs policy: omitting either is refused. The hard
memory binding must be `required` and at least as large as the page-rounded-down tmpfs capacity.
All three names reserve their independently configured admission capacities, while actual tmpfs
pages are charged by the kernel inside the same run cgroup's `memory.max`; tmpfs therefore cannot
act as hidden memory. Because a hard-memory run defaults `memory.swap.max` to zero unless it also
requests swap, its tmpfs pages cannot spill into undeclared host swap. Leave memory headroom for
the interpreter, subprocesses, and non-tmpfs allocations instead of setting both limits equal.

After the launcher is attached to its cgroup but before user code can execute, it enters the
verified private user/cgroup/mount namespace and mounts `tmpfs` at the already-owned run temp
directory with explicit `size`, `nr_inodes`, `mode=700`, `uid`, and `gid`, plus
`nosuid,nodev,noexec`. It verifies the mount type, options, ownership, and effective ceilings,
reports success to the broker, and waits for a second release. Only then does AGCoord durably mark
the tmpfs resources applied and let the command start. `TMPDIR`, `TMP`, and `TEMP` all name that
mount (or, when the mount path exceeds 48 bytes, a short broker-owned symlink alias pointing to it
so Unix-domain sockets created beneath `TMPDIR` stay within `sun_path`); the private setup record
and ownership token are removed from the command environment.
The `noexec` policy means tools must execute generated programs through an interpreter or place
executables outside temporary scratch. A machine whose jobs legitimately execute what they write
under `TMPDIR` — command doubles a test suite runs, throwaway virtual environments, plugin
executables a tool downloads into a temporary data directory — opts in once, in its
configuration, by setting `"exec": true` on the `tmpfs` binding; the run's scratch is then mounted
`nosuid,nodev` without `noexec`, and verified as such. This is the operator's decision for the
machine, never a per-row claim: a job cannot request executable scratch a configuration did not
grant. `exec` on any other binding kind, or a non-boolean value, is refused when the
configuration is loaded, and a stored resource contract carries `exec` only when it is set.

The launcher supervises the direct command and samples `statvfs` while the mount exists. Receipts
retain peak allocated bytes and user-created inodes (above the mount's initial root inode), plus
deduplicated `tmpfs-byte-limit-hit` or `tmpfs-inode-limit-hit` events. The configured inode limit
is the kernel filesystem ceiling and includes tmpfs's own root inode. A token-bound report in the
private backend metadata lets a replacement broker retain the last sample without exposing file
names or contents.

A namespace-rooting failure always stops before user code with exit status 125 and
`failure_reason=resource-enforcement-failed`; the launcher is never released with parent controls
visible. A required tmpfs mount failure has the same outcome. If both tmpfs bindings are
`best-effort`, a mount-specific failure is recorded as unapplied and the command continues with
`TMPDIR`, `TMP`, and `TEMP` absent; the verified namespace and hard memory control remain applied.
AGCoord does not substitute an unbounded disk directory for the unavailable request.

Exit status 125 belongs only to a setup or release that fails before user code. Once a
tmpfs-backed command has started, the supervising launcher relays that command's own exit status
or termination signal as the run's result and never replaces it with 125. It has already dropped
every capability by then, so it does not unmount the scratch itself.

Every successfully applied tmpfs request gets a distinct mount namespace and target. The mount
remains alive while any process in that worker tree retains the namespace, then the kernel tears
it down when the tree is gone; only afterward does the broker remove the owned underlying
directory and token-bound report. Normal completion, cancellation, and replacement-broker
recovery use the same ordering. Tmpfs is temporary virtual memory rather than a general
filesystem sandbox; the kernel's
[tmpfs contract](https://www.kernel.org/doc/html/latest/filesystems/tmpfs.html) defines its size,
inode, and swap behavior.

### Persistent quota-backed scratch

The optional `project-quota` backend provides disk-backed scratch with a hard storage-byte limit
and a separate hard inode limit. Put the state directory on a dedicated local ext4 or XFS block
filesystem that was prepared by an operator for project quotas and mounted read-write with
`prjquota` (or XFS `pquota`). Then bind one `storage/bytes` name and one `inodes/inodes` name to
the backend with the same enforcement mode:

```json
{
  "capacities": {
    "jobs": 2,
    "disk": 107374182400,
    "disk_inodes": 2000000
  },
  "bindings": {
    "disk": {
      "kind": "storage",
      "unit": "bytes",
      "mode": "required",
      "backend": "project-quota"
    },
    "disk_inodes": {
      "kind": "inodes",
      "unit": "inodes",
      "mode": "required",
      "backend": "project-quota"
    }
  }
}
```

```bash
agc run --resource disk=8589934592 --resource disk_inodes=200000 \
  -- python -m pytest -q
```

The two resources form one atomic policy and cannot be requested separately or with different
modes. Byte limits must be exact multiples of 1024 bytes, the common granularity used by this
backend across ext4 and XFS. The inode ceiling includes the scratch root itself, and filesystem
directory blocks and other quota-accounted data consume part of the byte ceiling. A run cannot
combine persistent quota scratch with bounded tmpfs scratch because both would claim `TMPDIR`.

The backend supports only a directly identifiable ext4 or XFS local block device. It refuses
overlay, network, read-only, quota-disabled, invisible, and device-mapper-backed mounts rather
than inferring that a directory is constrained. It never enables a filesystem feature, changes a
mount, or edits `/etc/projects` or `/etc/projid`. Quota administration and project-inherit
attributes require `CAP_SYS_ADMIN` in the initial user namespace; provision that capability only
for the explicitly managed broker. Before user code starts, the worker clears its effective,
permitted, inheritable, and ambient capabilities and enables `no_new_privs`, so the broker's
quota authority is not lent to the command. It reports that verified state to the broker and
waits for a second durable release before executing the command.

Preparation creates an owned `0700` tree below `<state_dir>/project-quota/runs`, applies a fresh
high-range project ID and project inheritance, installs both hard limits, reads them back, and
only then exposes the path through `TMPDIR`, `TMP`, and `TEMP`. Allocation takes a mount-global
advisory lock, verifies that the selected quota record has no limits or usage, and records the
path device/inode, project ID, mount identity, token, and request in a private durable manifest.
Concurrent coordinators using this backend therefore cannot knowingly reuse a live AGCoord
project identity on the same mount.

Receipts retain the greatest sampled kernel quota usage for bytes and inodes, including the final
sample after the worker tree is gone, and record `storage-byte-limit-hit` or
`storage-inode-limit-hit` when usage reaches a hard ceiling. Cancellation uses the same terminal
sample. Cleanup validates the manifest and exact path identity, removes only that owned tree,
waits for its quota usage to reach zero, clears the limits, verifies the project ID is reusable,
and then removes the manifest. A replacement broker adopts an incomplete or live allocation from
the manifest; a changed path, mount, attributes, limits, or token is refused rather than removed.

Required probe or setup failures stop before user code with
`failure_reason=resource-enforcement-failed`. When both bindings are `best-effort`, an unavailable
backend or pre-spawn setup failure is recorded as unapplied and the command continues with
`TMPDIR`, `TMP`, and `TEMP` absent. AGCoord does not substitute an unbounded disk directory. A
worker that cannot prove it dropped the broker's capabilities is never released, regardless of
quota mode. The checkout, bind mounts, and every path outside `TMPDIR` remain outside the quota.
Project quotas are resource accounting, not a confidentiality boundary: processes of the same
account may be able to name sibling paths. The kernel [`quotactl(2)`
contract](https://man7.org/linux/man-pages/man2/quotactl.2.html), [ext4 project-quota
options](https://www.man7.org/linux/man-pages/man5/ext4.5.html), and [XFS project-tree
semantics](https://www.man7.org/linux/man-pages/man8/xfs_quota.8.html) define the underlying
enforcement.

The Python and Rust broker owners enforce this same contract and use compatible durable handles,
receipts, and protocol-5 recovery rules. The native owner performs the filesystem attribute and
quota operations through kernel interfaces in the static broker; it does not invoke `chattr`,
`setquota`, `xfs_quota`, or another host-side quota helper.

### Per-device block I/O

The `cgroup-v2` backend can enforce bandwidth and operation-rate ceilings for explicitly
configured scratch devices. The paths identify devices; AGCoord does not create those paths,
redirect `TMPDIR`, or limit filesystem capacity. Bind directional names, and configure one or
more scratch paths in the same broker file:

```json
{
  "capacities": {
    "jobs": 2,
    "read_bps": 268435456,
    "write_bps": 134217728,
    "read_iops": 4000,
    "write_iops": 2000
  },
  "bindings": {
    "read_bps": {
      "kind": "io-bandwidth", "unit": "read-bytes-per-second",
      "mode": "required", "backend": "cgroup-v2"
    },
    "write_bps": {
      "kind": "io-bandwidth", "unit": "write-bytes-per-second",
      "mode": "required", "backend": "cgroup-v2"
    },
    "read_iops": {
      "kind": "io-operations", "unit": "read-operations-per-second",
      "mode": "required", "backend": "cgroup-v2"
    },
    "write_iops": {
      "kind": "io-operations", "unit": "write-operations-per-second",
      "mode": "required", "backend": "cgroup-v2"
    }
  },
  "cgroup_root": "/sys/fs/cgroup/user.slice/example.slice/agcoord.service",
  "cgroup_io": {"paths": ["/srv/agcoord-scratch"]}
}
```

```bash
agc run --resource read_bps=134217728 --resource write_bps=67108864 \
  --resource read_iops=2000 --resource write_iops=1000 -- python -m pytest -q
```

The generic `bytes-per-second` and `operations-per-second` units apply the same requested value
to both directions. They cannot be combined with a directional binding that would claim the
same `rbps`, `wbps`, `riops`, or `wiops` control in one run. An optional `io-weight/weight`
binding writes a per-device proportional weight from 1 through 10000; kernel or scheduler
support is still verified, and the requested weight consumes its ordinary named admission
capacity like every other resource.

Before creating the run leaf, AGCoord resolves every configured path to a distinct device and
records the sorted major:minor and filesystem identities in private ownership metadata. It
accepts only real, writable mount trees on directly identifiable whole block devices using
ext2, ext4, F2FS, or XFS. It refuses symlinks, bind/subdirectory mounts, overlay and network
filesystems, Btrfs, partitions, device mapper, MD, and other stacked devices. Linux itself
rejects partition numbers for per-block-cgroup settings, so AGCoord never substitutes a parent
disk and pretends that it is the requested target. The mapping is resolved again before launcher
attachment and during recovery; any identity change fails closed.

AGCoord writes and reads back every requested `io.max` or `io.weight` value before releasing
user code. The policy applies to all I/O issued by the complete run cgroup to each recorded
device, including other paths on that device; it does not constrain checkout or scratch I/O on
an unlisted device. The requested number is applied independently to every selected device; it
is not divided into one aggregate multi-device ceiling. Multiple configured paths that resolve
to one device are deduplicated and receive one policy.

Receipts retain the exact named limits in `applied`. The backend samples the recorded devices'
monotonic `io.stat` byte and operation counters and reports the greatest interval rate under
each binding in `peak`; a symmetric binding uses the greater directional rate. A terminal sample
is taken after the worker tree is gone but before the leaf is removed for normal completion and
cancellation. A weight has no absolute usage peak. The raw device identity stays in the private
handle so public output does not disclose host topology. Temporary bursts remain possible under
the kernel's `io.max` contract, and buffered writeback is supported only for the filesystems
listed above. Block-I/O controls do not limit bytes or inodes stored; use the separate project
quota or tmpfs contracts when capacity is the requirement. See the kernel's
[`io.max`, `io.weight`, `io.stat`, and writeback contract`](https://www.kernel.org/doc/html/latest/admin-guide/cgroup-v2.html#io)
for the underlying behavior.

Normal finish and cancellation use `cgroup.kill`, wait for `cgroup.events` to report
`populated 0`, and remove only the identity recorded for that run. A replacement broker adopts a
still-live durable handle without creating a second leaf. Missing partial state is cleaned
idempotently, while a changed device/inode, mismatched token, unrecorded collision, or populated
stale leaf is refused and never removed as if it were owned.

Optional tmpfs and project-quota trees plus block-I/O policies are resource controls rather than
credential, network, or general security sandboxes.

Fairness applies across lanes and capacity: a compatible check can overlap work in another
repository or in another worktree of its own, but it cannot leapfrog an earlier land in its own
worktree. Capacity is handed out in submission order to whatever fits, and
[preemption](#first-fit-admission-and-preemption) lets an older job stop later checks that
hold what it lacks. Nothing bounds how long a job waits behind later work that is never
preempted; see [the starvation gap](#the-starvation-gap).

## Durable job shape

The snapshot top level is strict:

```text
protocol · broker_pid · captured_at · capacities · allocations
resource_bindings · resource_capabilities · overflow_pools
active · queued · recent
```

`capacities` maps each configured resource to a positive integer; `allocations` maps it to
the nonnegative units currently held. `resource_bindings` is the exact machine binding map
frozen by the live broker. `resource_capabilities` maps each installed or referenced backend to
its `available`, `kinds`, `units`, `operations`, and stable `reason` fields. It contains no host
paths or backend exception text. `overflow_pools` lists each configured
[overflow pool](#overflow-pools) in order with its configuration (including `privileged` and
sorted `capabilities`), the `allocations` its running
jobs hold, and, inside a configured `budget`, the current budget day's `used_cpu_hours` and
`remaining_cpu_hours`. Active and queued jobs appear in scheduling order; recent
contains retained terminal history ordered by completion time (`finished_at` descending, tie-broken
by sequence descending).

Each row contains exactly:

```text
run_id · sequence · status · kind · label · agent
repository_id · repository · worktree_id · checkout · branch · head_sha
barrier · resources · resource_contract · resource_receipt · blocked_by · pool
attempts · preempted_by
gate_run_id · publication · failure_reason · phase · gate_exit_status
caller_pid · command · created_at · started_at · finished_at
exit_status · worker_pid · cancel_requested · log_bytes · position
```

`kind` is exactly `check`, `full`, `merge`, or `land`; `merge` remains representable for
existing receipt-backed history while new public landing uses `land`. `publication` is
either null or the normalized `{adapter, request}` record for publication work. `resources`
is the immutable requested-unit mapping,
including implicit `jobs=1`. `resource_contract` freezes each requested name's exact
`{backend, kind, mode, unit}` meaning when the row is accepted. `resource_receipt` has exactly
`requested`, `applied`, `peak`, and `events`: the first three are resource-to-integer maps, and
each event has exactly `at`, `backend`, `resource`, `stage`, `status`, and a stable `code`.
Applied values are recorded only after a successful pre-release attach; peak values are
backend measurements in the bound unit. Admission-only claims have empty applied, peak, and
event fields. A backend sample may also return stable, sanitized observations such as a
controller-limit event; the broker validates the resource and code, records each observation
once, and never exposes a raw controller file or exception. Backend handles and host paths remain
private spool state and never enter the public row;
`blocked_by` identifies repository predecessors, resource shortages, an in-flight preemption
(`preemption:<run_id>`), and local runtime or capability exclusions (`runtime:local`,
`capability:<name>`) currently preventing admission. `pool` is the
name of the overflow pool the job was placed in, or null for a job that ran or will run on this
machine. `attempts` is the number of the row's current or next attempt: `1` until a
[preemption](#first-fit-admission-and-preemption) requeues the row, and one more after
each. `preempted_by` is the run ID of the job that most recently preempted the row, including
one whose stop is still in progress, or null. Repository and
worktree IDs are stable opaque identities, while their resolved values remain available for
operators. `agent` is the explicit `--agent` value, then `AGCOORD_AGENT`, or the stable value
`unnamed` when neither is set; `caller_pid` remains the exact submitting process diagnostic and
is never used as the fallback identity. Missing times and kind-specific values are null rather
than guessed. A queued
position is one-based; active and terminal rows use null.

The durable status state machine is
`queued -> running -> passed|failed|cancelled|interrupted`, with direct
`queued -> cancelled` allowed; a preempted check also returns from `running` to `queued` for
its next attempt. `phase` is always present. Checks, full gates, and retained
merge jobs use `queued -> running -> complete` and keep `gate_exit_status` null. A land job
uses `queued -> preflight -> gating -> publishing -> complete`; its gate status remains null
until the gate finishes and is then the actual shell status. Terminal rows are immutable
history. `passed` and `failed` carry the overall job status, `cancelled` uses the
coordinator's cancellation status, and `interrupted` never claims a gate or publication
verdict.

## Running checks and standalone full gates

Submit a check and follow its combined output to completion:

```bash
agc run \
  --label "API unit tests" \
  --resource cpu=2 \
  -- python -m pytest -q tests/api
```

Use `full` for an exact-head verdict that is useful independently of publication:

```bash
agc full \
  --label "full repository gate" \
  --checkout /absolute/path/to/worktree \
  --resource cpu=4 \
  -- ./scripts/test.sh
```

The public `full` command resolves the repository and worktree, requires a clean checkout,
captures the exact 40-hex Git `HEAD`, and submits `kind=full`. The admitted worker verifies
its exact durable identity, the unchanged head, and cleanliness again after waiting in the
queue. A passed row is therefore an immutable validation receipt for that repository,
branch, and head. It remains useful for standalone release preparation and audit, but normal
publication uses the gate embedded in one `land` row rather than treating a prior full
receipt as a separate authorization step. The one exception is the opt-in described in
[reusing a full receipt as the land gate](#reusing-a-full-receipt-as-the-land-gate), which
still records the verdict inside the same land row rather than making the receipt a separate
authorization.

`full` is neither a lane barrier nor machine-wide exclusivity. Declare every scarce machine
resource it requires. Capacities—not the fact that the job is named “full”—decide whether
other work can overlap it.

A check may be [preempted](#first-fit-admission-and-preemption): stopped and requeued
so that an older job that has waited long enough can start. The submitting client keeps waiting
and reports the final attempt. Pass `--no-preempt` to `agc run`, or `preempt=False` to
`CoordinatorClient.submit`, for a check that must not be restarted, such as one that is not safe
to run twice; it is then never stopped for another job. A `full` and a `land` are never
preempted, so they take no such option.

The coordinator snapshots the submitting client's execution environment for its worker, but
does not expose it through rows, CLI output, or the TUI, and clears it when the job starts or
is cancelled while queued. The one exception is a running check that may still be preempted:
its environment stays in the owner-only spool until that attempt ends, so that the check can
be restarted unchanged. A reserved admission marker prevents a coordinated worker from
submitting another coordinated job and deadlocking behind itself.

### Admission context for repository wrappers

Every admitted worker receives three coordinator-owned environment values:

```text
AGCOORD_RUN_ID · AGCOORD_RUN_KIND · AGCOORD_STATE_DIR
```

The broker overwrites caller-supplied values with the durable run ID, exact durable kind
(`check`, `full`, `land`, or a retained legacy `merge`), and absolute resolved state
directory that owns the admission. This includes a state directory selected with
`--state-dir`; a repository wrapper must use the supplied value instead of assuming the
user-scoped default. These values are private process context, not public job fields. They do
not add rows or keys to the snapshot, `show`, or TUI, and they do not contain or grant forge
credentials.

The values are claims, not admission by themselves. A repository gate wrapper proves them with
the public verifier, passing its resolved checkout, fresh exact head, and process identity:

```bash
agc verify-admission --state-dir "$AGCOORD_STATE_DIR" --checkout "$root" \
  --run-id "$AGCOORD_RUN_ID" --kind "$AGCOORD_RUN_KIND" --head-sha "$head" --worker-pid "$pid"
```

It exits 0 when the process is the exact durable admission and 2 with an `error:` line when it
is not. It needs no `agcoord` in the wrapper's own interpreter: a machine that installs one `agc`
beside its one native broker resolves it from `PATH` like `run`, `full`, and `land`. The hidden
`python -m agcoord.queue verify-admission` entry point keeps the same arguments and exit codes for
wrappers that still import the package. A `full` wrapper is the admitted worker and verifies with
its own PID. A `land` gate is a child of the admitted land worker and verifies with its parent
PID. Checks receive the same context for diagnostics and nested-run protection, but
admission verification accepts only `full`, `merge`, and `land` rows and therefore rejects them.

A worker that declared a scratch policy presents exactly the same thing. Under a tmpfs or
project-quota policy the coordinator starts a launcher that enters the namespace, mounts the
scratch, and only then starts the command as its direct child; the durable row records the
launcher as the worker. Verification therefore also accepts the live direct child of the live
recorded launcher, proven by the child's own start identity, so `$$` in a `full` wrapper and
`$PPID` in a `land` gate keep working unchanged. A grandchild, or any process once the launcher
has gone, is still refused.

Verification fails closed when the state directory has no matching live owner or when the
run ID, kind, checkout, head, PID, or process start identity differs from the durable active
row. Repository wrappers should verify immediately before protected gate work and treat a
refusal as a failed gate. Verification is a proof about the calling process, not a second
submission: it never creates a row or starts a broker.

On a managed native host, these verifier calls and the other run-scoped operations below use an
explicit admitted callback path. The path accepts only the exact current run and state context,
the fixed installed release, the worker's exact one-entry user-namespace maps, and the restricted
native client-profile attestation. `agc show "$AGCOORD_RUN_ID"` uses the same path for the exact
parent row. Other CLI reads and every submission retain ordinary client selection, so a gate
script remains a standalone command and never needs to invoke a nested coordinator. A submission
made from inside an admitted run is refused by the client before it selects or starts any broker,
so the attempt leaves no owner or queue behind in the target state directory; a dirty checkout is
still refused before the nesting rule is.

### Child CPU leases for parallel tools

A job that declares `--resource cpu=N` owns one finite worker-token budget for its entire
descendant process tree. Parallel tools must divide that budget instead of each treating `N`
as its private worker count. An admitted subprocess can acquire a tool-neutral child lease
through the public Python client without submitting a nested job:

```python
import os
import subprocess

from agcoord import CoordinatorClient

client = CoordinatorClient(
    state_dir=os.environ["AGCOORD_STATE_DIR"],
    autostart=False,
)
with client.acquire_child_cpu_lease(8, minimum=1, timeout=30) as lease:
    subprocess.run(["my-test-runner", f"--workers={lease.granted}"], check=True)
```

Omitting `minimum` requests an exact grant: if that many tokens exceed the parent's CPU
budget, acquisition fails immediately. Supplying `minimum` permits a partial grant between
that value and `requested`; a minimum above the parent budget also fails immediately. A
compatible request waits when the tokens are merely busy, and `timeout` cancels that waiting
request. The context manager releases its grant on normal or exceptional exit.

Waiting is FIFO within the parent run, with one bounded bypass when the oldest request cannot
fit the temporarily available tokens and a younger request can. This lets mixed large and
small requests make progress without allowing a stream of small requests to starve the older
large one. Active grants for a run never total more than its declared parent CPU allocation.
Child leases partition that allocation only: they do not change machine-level allocations,
repository barriers, publication order, or the normal job history, and they do not replace an
OS enforcement backend such as cgroup v2.

The three admission environment values select the parent but do not authenticate it. Lease
acquisition also verifies the live worker and caller through durable PID start identities and
requires the caller to be a current descendant of that worker. A copied environment from an
unrelated process is refused. Parent cancellation cancels both waiting and active leases;
owner exit or crash reclaims its tokens. Lease rows are durable, so a replacement broker
preserves a still-live owner after verifying both identities and otherwise cancels the stale
grant without minting capacity.

`CoordinatorClient.child_cpu_leases(run_id)` exposes current waiting and active leases to the
owner-only spool's operators. Each record reports the lease and parent IDs, status, requested,
minimum and granted counts, whether the grant is full, owner PID, timestamps, and waiting
position. Terminal lease records are omitted unless `include_terminal=True`; none appear as
child jobs in `list`, `show`, the TUI, or ordinary run history.

#### Optional pytest-xdist adapter

Install the process-specific integration with its dependency:

```bash
python -m pip install 'agcoord[xdist]'
agc run --label "distributed tests" --resource cpu=4 -- \
  python -m pytest -n auto
```

The installed pytest plugin is inert unless xdist is present, a positive `-n` mode is selected,
and the process is the controller inside an admitted AGCoord run. Plain pytest and `-n 0` stay
serial and acquire no lease; installing the extra never enables distribution by itself. Xdist
workers, including replacements after a worker crash, recognize their worker context and never
acquire recursively. Outside an admitted run the hook returns control to xdist unchanged.

Inside an admitted run, precedence is deliberate:

- `-n N` for a positive integer acquires exactly `N` tokens. The adapter never rewrites it;
  a permanently impossible request fails with a pytest usage error before workers start, while
  temporarily busy tokens follow the generic fair wait.
- `-n auto` and `-n logical` request a partial lease up to the parent's declared CPU count and
  start exactly the granted number of workers. `--maxprocesses=N` caps that request. The lease
  grant takes precedence over xdist's host detection and `PYTEST_XDIST_AUTO_NUM_WORKERS`; AGCoord
  does not set or copy a gate-wide worker count for parallel controllers.
- A distributed mode without a parent `cpu=N` declaration fails clearly. `--collect-only` and
  xdist's `--pdb` serial fallback do not lease worker tokens.

The controller holds its lease until pytest has torn down the worker session. Normal and
exceptional pytest shutdown releases it; controller exit, crash, or AGCoord cancellation lets
the broker reclaim it from durable process identity. If a gate deliberately starts several
controllers and wants them to overlap instead of allowing the first automatic controller to
take the complete budget, give each an appropriate `--maxprocesses` cap. Across all cases the
adapter only sizes worker processes; cgroup CPU and PID limits remain the aggregate enforcement
boundary.

### Inline scripts for `run` and `full`

`agc run` and `agc full` accept either the existing trailing worker command or `--script FILE`;
the two forms are mutually exclusive, and one is required. `--script -` reads UTF-8 text from
standard input. A file is read as UTF-8 by the submitting client, then its text is sent as the
worker command, so the worker does not need the submitter's script path. Missing, unreadable,
non-UTF-8, whitespace-only, and NUL-containing scripts are refused before submission.

The complete script body, including its first line, is passed unchanged to one portable
interpreter command. With no shebang the command is `bash -ec BODY`. A first line beginning
with `#!` dispatches `sh` to `sh -ec BODY`, `bash` to `bash -ec BODY`, and `python` or
`python3` to `python3 -c BODY`. A simple absolute interpreter path and `/usr/bin/env NAME`
form are accepted for those names. Any other shebang or shebang arguments are refused so a
script cannot silently run under the wrong language. The leading shebang remains part of the
body; each supported interpreter treats it as a comment.

Inline scripts use the same `--overflow` and `--no-overflow` behavior as trailing commands.
The default still opts into overflow when the checkout contains a valid
`.agcoord/overflow.json`; `--no-overflow` keeps the run local, and `--overflow` explicitly
requires a valid declaration. The script body itself is submitted as command data and does not
change overflow eligibility.

### Atomic landing

The normal landing operation is one durable request containing the forge adapter/request,
exact checkout/branch/head, gate command, private caller environment, and resource claim:

```bash
agc land 123 \
  --label "gate and publish PR 123" \
  --checkout /absolute/path/to/worktree \
  --resource cpu=4 \
  -- ./scripts/test.sh
agc land 123 --adapter github -- ./scripts/test.sh
```

The CLI defaults `--adapter` to `github` as a convenience, while the client API and durable
publication record keep the adapter and request separate. The current GitHub adapter accepts
a pull-request number; future adapters can normalize their own request shape. Submission
requires a clean checkout at one full 40-hex `HEAD` and inserts exactly one `kind=land`
repository barrier. Before queueing a GitHub land request, the optional GitHub adapter reads the
pull request's current head (retrying transient forge errors with a bounded wait) and compares it
to the submitted head: equal heads enter the queue normally, behind heads are refused as
`submitted-head-behind` naming both heads and `git fetch && git merge --ff-only origin/<branch>`,
diverged or unpushed heads are refused as `submitted-head-diverged`, and persistent forge read
errors refuse as `forge-unavailable` without creating a row. Its command and environment are the gate, not a second queued job, and it
is submitted with a null `gate_run_id`. The coordinator sets that field only when it authorizes
[reuse of a full receipt](#reusing-a-full-receipt-as-the-land-gate), where it names the receipt
the verdict came from; a land that ran its own gate keeps it null.

The worker first validates local and forge identity, readiness, and the current target and
source refs. If the target advanced while an open, ready, same-repository request waited, the
default GitHub adapter fetches that exact target commit and creates one ordinary `--no-ff` merge
commit in the coordinator-owned detached gate tree. It verifies the ordered parents
`(old source, target)`, pushes
the commit to the request branch with an exact `--force-with-lease`, and atomically replaces the
row's durable head while it is still in `preflight`. Preflight is then repeated against the new
head. This retry is bounded if the target keeps moving. `--no-target-sync` instead returns the
original `stale-main` refusal without changing the source.

Immediately after that push, the forge's pull-request metadata can still report the head the
push replaced. The repeated preflight treats exactly that head as read-after-write lag: it
re-reads the metadata every 2 seconds for at most 30 seconds before deciding, and proceeds once
the forge reports the synchronized head. Any other head, or the replaced head still reported when
the wait ends, is the ordinary `head-changed` refusal. The tolerance applies only to the head the
coordinator itself just replaced and never relaxes the remote-ref comparison or the atomic
before-OID checks, so lag cannot admit a real concurrent source change.

A merge conflict is aborted before the gate, restores the exact clean gate tree, and
reports the conflicting paths. A concurrent source update or rejected push also fails before
the gate; the lease prevents overwriting another writer. Once preflight succeeds, the worker
runs the gate once with the captured environment and combined transcript. A red gate records
its actual shell status with `failure_reason=gate-failed` and never calls the publisher. A green
gate transitions the same row directly to `publishing` while retaining the repository barrier
and every resource allocation. No same-worktree job, gate, or second landing in that lane can
enter the gap because no separate job or gap exists.

Publication repeats the exact validation after the gate. Let `M` be the target's observed
remote commit and `H` the submitted head; `M` must be an ancestor of `H`. The forge-neutral
publisher creates candidate `C` with tree exactly `H` and parents exactly `(M, H)`, without
moving a reference, then performs one atomic compare-and-update for target `M -> C` and
source `H -> H`. Both comparisons participate even though the source value does not change.
A real non-main target such as `release` uses the same contract; an absent reported base is
a readiness refusal. GitHub support is an optional adapter and is not needed to install,
import, or run checks in the core coordinator.

If the target advances after preflight or during the gate, the result is `stale-main`; a passed
gate is never reused after either ref moves. If the source head advances, the result is
`head-changed`. `pr-not-ready`, `publish-failed`, and `merge-error` remain distinct handbacks.
A failed publication leaves the remote target untouched. If the exact `H` is already an
ancestor of the remote target, recovery/retry is idempotent success even if forge metadata still
says open or an auto-delete removed the source reference.

The pre-gate merge is the only automatic worktree mutation: AGCoord never rebases, amends, or
rewrites existing source commits, and it never moves the target without a green gate. A conflict
or changed-head refusal hands control back to the agent. Resolve the branch or submit its new
exact head in a fresh `agc land` request; use `--no-target-sync` when policy requires every stale
target to be handled manually. Do not substitute a separate full-plus-merge sequence or direct
target update.

A land request is cancellable while queued, preflighting, or gating. Once its durable phase
is `publishing`, cancellation is refused because killing a client during an authenticated
atomic mutation would leave the outcome indeterminate. A graceful broker stop lets every phase
finish; once `stop_grace` elapses it interrupts earlier phases as `broker-stopped` but still
waits for publishing and records its authoritative result.

### Batching and bisecting queued lands

When an admitted `land` row starts while other queued `land` rows target the same repository,
target branch, normalized gate command, and compatible resource envelope, the coordinator bounds the
batch to an adaptive power-of-two size $B = 2^k$ (clamped by `land_max_batch_size`, default `8`)
from the sliding window of the last 16 `passed`/`failed` `land` runs on that `repository_id`:
cold-start (`< 4` runs) uses $B = 4$; failure rate $\hat{p} < 0.11 \rightarrow B = 8$ (or the
configured power-of-two ceiling); $0.11 \le \hat{p} < 0.21 \rightarrow B = 4$;
$0.21 \le \hat{p} < 0.38 \rightarrow B = 2$; and $\hat{p} \ge 0.38 \rightarrow B = 1$ (falling back
to sequential single-PR runs above Ungar's ~38.2% group-testing crossover until pass rate recovers).
When $Q$ ready compatible `land` requests are available, the leader claims the largest power of two
$\le \min(Q, B)$ (leaving trailing requests queued for the next batch):

1. **Pre-gate assembly (`T_i = merge(T_{i-1}, P_i)`):** Starting from `T_0 = main`, the leader
   inspects each candidate's live PR state and head SHA (skipping `pr-not-ready`,
   `head-changed`, or `avoided-commit` rows without pulling them into the batch), claims a
   power-of-two batch $\le B$, checks out `T_0`, and sequentially runs `git merge --no-ff` in the
   coordinator gate tree. Any candidate that conflicts with `T_{i-1}` is recorded immediately as
   `merge-error` and dropped so the remaining clean candidates continue assembling from `T_{i-1}`.
2. **Combined gating and atomic multi-PR publication:** The leader runs the gate command once on
   the combined merge tree `T_k`. When the gate succeeds and `main` plus all `P_1..P_k` head SHAs
   still match, the leader constructs chained merge commits `M_1..M_k` on GitHub and advances
   `main` to `M_k` along with each PR's `avoid-commit` / closure ref via `update_refs` in one
   atomic transaction, closing every member row as `passed` with the batch log.
3. **Deductive power-of-two bisection on gate failure:** If `T_k` fails its gate (`k > 1`), the
   slice splits at the largest power of two strictly less than `k` into `Left` and `Right`. `Left`
   is assembled and gated first against the current `main`; when every member of `Left` publishes
   cleanly onto `main` ($M' = M + \text{Left}$) and `Right` re-merges onto $M'$ with zero drops to
   the exact root tree SHA that already failed in the parent slice, `Right` is deductively known to
   fail without re-running its combined or singleton gate (`deduced-bisected` for $|Right| \ge 2$,
   or `deduced-gate-failed` finalizing a singleton immediately as `gate-failed`). Any cancellation,
   preflight invalidation, merge conflict, or external target move changes the tree and invalidates
   `known_bad`, running an explicit gate instead. Pass `--no-deductive-bisect` on `agc land` when a
   flaky non-deterministic gate requires an explicit singleton failure log on every rejected PR.
4. **Causal failure-attribution eviction (`AGCOORD_BATCH_ATTRIBUTION_FILE`):** Before invoking the
   gate for any multi-PR batch slice (`k >= 2`), the leader creates an owned empty temporary file
   outside the checkout and sets `AGCOORD_BATCH_ATTRIBUTION_FILE=<path>` in the gate environment
   (cleaned up after each gate). Dependency-aware test runners may write an optional JSON receipt
   (`<= 4 MiB`, regular non-symlink file):

   ```json
   {
     "format": "agcoord-failure-attribution-v1",
     "failures": [
       {
         "unit": "tests/test_billing.py",
         "causal_paths": ["src/billing/invoice.py", "tests/test_billing.py"]
       }
     ]
   }
   ```

   Each `causal_paths` entry must be a canonical repository-relative POSIX file or directory path
   (no leading/trailing `/`, `.`/`..`, backslashes, or symlinks; a directory `"src/billing"` matches
   any changed path below `src/billing/`). On non-zero gate exit, `LandCoordinator` intersects each
   failure's `causal_paths` with every slice member's `git diff --name-only <T_0>...<head>`. When
   one or more members form a strict subset uniquely blamed by individual failing units (`|M_f| == 1`),
   those culprits are finalized immediately as `gate-failed` (recording `attributed-gate-failed` and
   `attributed` `[{run_id, units}]` in batch history) without solo confirmation gates, and the
   remaining unblamed members proceed in a fresh slice (`known_bad=False`). Missing, malformed,
   zero-match, or multi-member-match receipts fall back safely to deductive binary splitting.

While a batch slice's gate runs, the leader's `admitted_run_status` and `agc --json show`
include `batch_slice`: its exact `gating_head_sha` and an ordered `members` array containing
each member's `run_id`, `branch`, `head_sha`, `original_checkout`, and `publication`.
The field is `null` outside an active slice. A gate invoking `verify-admission` with its
checkout's `git rev-parse HEAD` verifies that exact recorded slice head, including during
bisection when the slice does not contain the leader. Any other head is refused. Outside an
active slice, admission continues to require the leader run's recorded head.

Every member of a batch — both the leader (`role: "leader"`) and each follower (`role: "follower"`,
whether dropped on a merge conflict, finalized early on a bisected slice, or published at the end) —
also exposes a durable `batch` provenance object on `status` (`agc --json show`) and `snapshot`
(`agc --json list`, `agc tui`). Standalone runs (including a `land` row that found zero eligible
followers) and followers requeued to `queued` on leader cancellation or crash report `batch: null`;
if a requeued follower is later claimed into a new batch, `batch` reflects that newer batch.
A populated `batch` object records `leader_run_id`, `role`, `target_branch`, `max_batch_size`,
`created_at`, ordered `members` (`position`, `run_id`, `branch`, `head_sha`, `publication`,
`status`, `failure_reason`), and chronological `steps` (`step`, `base_sha`, `gating_head_sha`,
`member_run_ids`, `gated_member_run_ids`, `dropped`, `attributed`, `gate_reused_from`,
`gate_exit_status`, `outcome`, `split`, `published_sha`, `started_at`, `finished_at`) across clean
multi-PR gates, causal failure-attribution culprit evictions (`attributed-gate-failed`), recursive
and deductive binary-split bisections (`bisected`, `deduced-bisected`, `deduced-gate-failed`),
merge-conflict drops, `--reuse-full` substitutions, stale-target or changed-head handbacks,
publication failures, and cancellations.

### Reusing a full receipt as the land gate

The two-step a worktree usually follows — `agc full`, then `agc land` — exists so that a red gate
is discovered outside the repository lane barrier rather than while holding it. `--reuse-full`
keeps that pre-barrier check and stops paying for it twice:

```bash
agc land 123 --reuse-full -- ./scripts/test.sh
```

The worker never decides this. After preflight reaches its exact head it asks the coordinator,
which resolves and records the reuse in one immediate transaction, so a gate is skipped only on a
verdict the durable record already holds. A candidate must satisfy every one of:

- `kind` is exactly `full` and its status is `passed`. A `check` is never eligible, because it
  asserts no clean head and so its `head_sha` does not pin a tree.
- the same `repository_id` **and** the same `worktree_id`. A second worktree of the same
  repository at the same commit holds a different ignored working tree, so its receipt is
  not accepted here;
- `head_sha` equal to the head preflight reached, not the head that was submitted;
- an identical gate command;
- `finished_at` no older than `land_gate_reuse_max_age`.

On a hit the row moves to `gating` with gate exit status `0`, its `gate_run_id` names the receipt,
and the transcript records the substitution:

```
Land coordinator: gate reused from full-abc123def456; that receipt already gated the exact head <sha>
```

On a miss nothing changes and the gate runs. Without the flag the coordinator is never asked.

What reuse preserves, because each happens at land time regardless of where the verdict came
from: preflight validates the target inside the barrier; publication is one atomic
compare-and-swap on both refs; the barrier still serializes landings in the repository; and the
avoided-commit check still re-reads the target. **A moved target cannot reuse a green result**,
and not by policy: synchronizing an advanced target creates a merge commit, which changes the
exact head, so no earlier receipt can match it.

Target synchronization uses the invoking Git identity when supplied in the environment or Git
configuration. Missing author and committer name or email fields fall back to `AGCoord` and
`agcoord@localhost` for that merge only; configured or environment values remain in effect.

What reuse does not preserve is freshness against a gate that is not a function of the tree. The
same commit is not the same gate inputs when the gate reads the clock, the network, the installed
toolchain, or the ignored working tree — `node_modules`, a virtual environment, a build directory.
A clean `git status` says nothing about those. The coordinator cannot detect it, which is why the
flag is opt-in, why receipts expire, and why an operator can set `land_gate_reuse_max_age` to `0`
to refuse every reuse on a host.

### Avoided commits after a target rewrite

A deliberately rewritten target — `main` force-pushed to replace a commit — cannot be merged
safely by any request branch that still reaches the replaced commit, whether the branch was
created from the old target or received it through target synchronization. Git cannot know the
commit was removed on purpose, so the operator who performed the rewrite records it once:

```bash
agc avoid 0123456789abcdef0123456789abcdef01234567 --reason "removed from main"
agc avoid --list
agc avoid --remove 0123456789abcdef0123456789abcdef01234567
```

The set is an owner-only `avoid.json` beside `config.json`. It needs no broker, is untouched by
`agc clear` (which removes terminal history only), survives broker restarts, and travels with the
state directory through migration and rollback. It is machine-local: a coordinator elsewhere does
not know about it.

Every `agc land` applies the stored set, unioned with any `--avoid SHA` given for that landing.
Before any push it refuses with the stable code `avoided-commit` when an avoided commit is
reachable from the request head or from the current target; target synchronization refuses
before pushing a synchronized head that would reach one, restoring the checkout; and the target
is read again after a green gate so a commit re-imported during the gate is still caught before
publication. The run log names the avoided commits that were checked and any the repository does
not have, which are trivially unreachable. The recovery is always the same: rebuild the request
as a fresh branch from the current target and rerun the full gate.

## Observation and cancellation

Every accepted job has one stable ID, durable row, and combined stdout/stderr log. Use:

```bash
agc list [--repository <repo>] [--agent <agent>] [--kind <kind>] [--pool <pool>] [--limit <n>]
agc list --json
agc show [--json] <run-id>
agc log <run-id> [--follow]
agc cancel <run-id>
```

The non-JSON `list` table summarizes each row as `admission-only`, `applied`, `partial`,
`unapplied`, or `failed`, and `agc show <run-id>` prints a readable summary of one durable row
(including its `Attempts` and the job that last preempted it, and chronological
`Batch & Bisect History:` when the row participated in a land batch).
`list --json` and `show --json` (or `agc --json show`) retain the complete binding, capability,
receipt, and batch fields as JSON so automation can distinguish scheduling, actual application,
and measurement.
Optional `--repository`, `--agent`, `--kind`, `--pool`, and `--limit` filter flags query the broker for
matching runs across the durable spool rather than limiting matches to the global default window.
`--pool local` selects runs without an overflow placement, including queued runs.

Queued work is cancellable. Running checks and full gates, plus land preflight and gating,
receive process-group cancellation and become terminal only after every descendant is gone.
Cancelling a check that a preemption is already stopping ends it `cancelled` instead of
requeued. Publishing land jobs refuse cancellation as described above. Unknown IDs and terminal jobs
produce named errors rather than silently changing another row.

A submitting client streams the job it accepted. A preempted check is followed across its
attempts: the client prints the rest of the stopped attempt's output and the preemption line,
reports that the job waits again, and continues with the next attempt. The stream ends with
the job's own exit
status, with 70 when a terminal row carries none, with 130 when the caller interrupted it and
cancellation was requested, and with 75 when the client lost contact with the coordinator
after acceptance. In that last case the client first retries a transient error for five
seconds, then prints that the job continues on the broker and how to keep following it
(`agc log <id> --follow`, `agc show <id>`), and claims no verdict; `--json` reports the same
case as `{"code": "coordinator-unreachable", "message": …, "run_id": …}`. The spool
inspection every client command performs is itself a broker command, and the broker waits
through transient SQLite contention for the configured `database_timeout` rather than
aborting on the first busy lock, so a broker committing a publication or checkpointing its
WAL does not end a client that is only watching its own row. A lock that outlives that
timeout is reported as the retryable `broker-database-busy`.

Watching costs the host little while work waits. A following client polls a queued job's row
at an interval that doubles from a tenth of a second to one second, then streams the admitted
job's log every tenth of a second; a `--json` wait, which streams nothing, backs off the same
way for the job's whole life. The broker's scheduling tick and each `status` read only queued
and running rows, and `snapshot` (`agc list`, `agc tui`) and `log` decode only live rows and the
bounded rows or log pages requested, so none of them grows with the terminal history that
accumulates until `agc clear`.

The native owner authenticates a worker with its PID, Linux start token, and requirement that
the PID lead its recorded process group. Replacement recovery adopts only that exact live
identity. If the PID has been reused, its token changed, or it belongs to another group, the run
is interrupted without signaling that process. Only a group already tied to the vanished
verified leader is eligible for recovery drain.

`agc clear` is intentionally narrow. It refuses while any job is queued or running. Once
the coordinator is inactive, it removes terminal rows and their run logs but preserves the
spool, ownership protocol, broker diagnostics, and migration records. There is no `--all`
shortcut that deletes coordinator state.

## Terminal UI

`agc tui` is a credential-free live view over the same client API on every supported Python
version. It shows active, queued, and recent terminal jobs across repositories; kind, lane/repository identity, pool,
declared resources, status, phase, label, timing, head, publication, gate exit, and failure details
remain inspectable without truncating the durable values. Persistent selection detail includes
the pool, enforcement summary, a preempted job's attempt and preemptor, and compact batch
position, slice/bisect summary, and leader/follower
role when batched; the modal detail (`Enter`) includes the pool, attempts, exact resource contract,
sanitized receipt events, and live-updating chronological **Batch & Bisect History** across active
and completed batched lands; and the status bar summarizes configured overflow pools' active
allocations and used/limit daily CPU-hour budgets (with estimated used/limit cost when
`cpu_hour_price` is configured).

The expected key map is:

| Key | Action |
| --- | --- |
| `r` | refresh from the durable snapshot |
| `Enter` | inspect the selected job and exact fields |
| `l` | read the selected job log |
| `c` | request cancellation; a publishing land explains why it cannot be cancelled |
| `h` | hide or restore terminal history without deleting it or rereading the spool |
| `k` | open the searchable kind picker |
| `p` | open the searchable repository picker |
| `o` | open the searchable pool picker |
| `a` | open the searchable agent picker |
| `?` | show context-sensitive help |
| `q` | quit the view; jobs continue |

Each filter picker starts in a focused search field. Typing narrows the complete snapshot's
choices, `Tab` moves into the scrollable result menu, and `Enter` applies the highlighted
choice. `All repositories`, `All agents`, `All kinds`, or `All pools` clears that filter directly; `Esc` closes the picker
without changing the current filter. Repository choices show their complete readable remote or
local identity rather than the internal hashed lane key.
The broker includes `available_filters` in snapshots with all historically known repositories,
agents, kinds, and pools across the full spool so pickers remain fully populated with choices even while active
filters are narrowing visible table rows. Active filters request server-side filtered snapshots from
the broker, displaying matching runs across the spool beyond the default global recent window.
The agent picker includes stable named identities and the single `unnamed` value. It omits
legacy `pid:<positive integer>` fallback choices from older retained history; those rows remain
visible in the unfiltered snapshot with their separate caller PID diagnostic intact.

Refresh reconciles keyed rows in place, preserving the selected run and both viewport offsets
when that row still exists without first painting a reset-to-top frame. Periodic refreshes wait
the full `refresh_interval` after each completed snapshot (and yield while a log read is in
flight) so a slow snapshot never chains back-to-back on the broker socket, while explicit `r`
refreshes and filter selections still queue an immediate update. Pressing `l` reads up to 1 MiB
of the selected job's log under a single spool preflight.
At 80 columns the compact table is exactly
`STATE · KIND · REPO · RUN · POOL · BRANCH · LABEL · AGE · DUR` and does not require horizontal
navigation, including when a long queue also needs its vertical scrollbar. Adjacent columns
retain a visible gutter; `LABEL` yields the small amount of width needed by that scrollbar
instead of creating a horizontal one. A value wider than its compact cell ends in a visible
ellipsis instead of running into the next column or disappearing at a hard edge. Above 80
columns, `POOL` and `BRANCH` consume additional width up to their useful caps before `LABEL`
expands into the remaining space; all three contract back to compact ellipsis forms when the
terminal narrows. `POOL` shows the overflow pool name for placed work, `local` once a job has
started on this machine, and `—` before placement.
`BRANCH` shows the exact captured Git branch rather than relying on the caller-supplied label.
`REPO`
shows the compact repository name derived from the credential-free normalized remote
identity, or from the local Git identity when the checkout has no remote. The
repository-filter header and persistent selection detail retain the complete readable value
so equal basenames remain distinguishable. The hashed `repository_id` remains the internal
lane/filter key and is available with the full repository and worktree identities in the
detail view. `AGE` is time since creation; `DUR` is absent before start, live while running,
and fixed at finish. Full agent, label, checkout, branch, barrier, resources, blockers,
command, exact created/started/finished timestamps, head, receipt, publication, phase, gate
exit, and failure values remain available without abbreviation in persistent selection detail
and the detail view. A land remains one selected row as it moves from gating to publishing,
and `l` reads one combined gate-and-publication transcript rather than separate run logs.

## Worker scratch and cleanup

Scratch is an explicit resource entitlement. A run receives an AGCoord-managed temporary path
only after it requests and successfully applies either a complete tmpfs byte/inode/memory policy
or a complete project-quota storage/inode policy. The broker then sets `TMPDIR`, `TMP`, and `TEMP`
to that private path. A job cannot combine the two providers.

If a run declares neither provider, the broker creates no per-run scratch directory and removes
`TMPDIR`, `TMP`, and `TEMP` inherited from the caller. The same no-scratch environment is used when
a best-effort scratch provider cannot be applied. This boundary means the run has no
AGCoord-provided or accounted scratch entitlement; it is not a general filesystem sandbox.
Commands can still name their checkout or other paths directly, and language runtimes may choose
a system fallback such as `/tmp` when all three variables are absent. Jobs that require bounded
temporary storage must declare a provider and use the advertised path.

An applied provider's root exists while the worker process group is live, including both gate and
publication phases of one land. Terminal status is withheld until the group is gone, the leader
is reaped, and owned scratch has been reclaimed. Cleanup restores owner traversal on nested
mode-`000` trees before removal. On restart, the broker preserves a genuinely live recovered
provider root and reclaims only identity-verified terminal or orphan state. Reclamation returns
temporary pages or quota capacity to the kernel and prevents one job inheriting another's files.

## Migrations

AGCoord 0.6.0 retired the Python reference broker and its in-process migrations. The native
broker owns every spool at protocol 5. A client that meets a spool left at protocol 1 through 4
by an earlier release refuses every command before starting or claiming a broker and names
**AGCoord 0.5.2** as the release that migrates it, so an un-migrated spool is never silently
mutated on a hot path. Migrate such a spool by installing 0.5.2, running its `agc migrate` while
the spool is idle, and upgrading; roll a 0.5.2 migration back with 0.5.2 itself. The
[pre-native spool guide](native_migration.md) has the exact steps.
