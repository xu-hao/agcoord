# Coordinator contract and operations

AGCoord coordinates development jobs submitted by multiple agents, terminals, worktrees, and
repositories on one machine. One detached broker is the scheduling and process-supervision
authority for the current OS user. Clients communicate through a private durable spool; they
do not launch a job merely because they inserted a row.

The public Python surface uses `CoordinatorBroker` and `CoordinatorClient`. The public CLI is
available as either `agcoord` or `python -m agcoord` and exposes `run`, `full`, `list`, `show`,
`log`, `cancel`, `tui`, `land`, `migrate`, and `clear`. Worker and broker verbs used to detach
or validate an admitted process are internal interfaces, not alternate user workflows.

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
owner-only. An ownership lock elects exactly one broker. Simultaneous first clients either
join that owner or fail without an accepted row; they never create two supervisors for the
same spool. The first ordinary client starts a detached owner on demand, so there is no
separate daemon-install command. Closing the submitting terminal does not cancel accepted
work.

The broker may exit after the queue is empty and idle. Durable history and logs remain, and a
later client starts a replacement owner against the same spool. An unclean restart observes
the recorded process identity before classifying a live job; it never reruns an already
spawned command merely because the old broker disappeared.

A land worker reports its final overall status durably while its row is still running. After
an unclean owner loss, a replacement preserves the live worker's lane and resources, never
reruns its gate, waits for the exact process group to disappear, reclaims scratch, and then
exposes the reported passed or typed-failed result. If the worker disappears before it can
report a result, `interrupted` is the only safe terminal classification.

## Repository lanes and resources

Each submission belongs to a stable repository lane and records its resolved worktree. A
lane preserves publication order without turning one repository's full gate into a lock on
the entire machine:

- `check` is ordinary work. Compatible checks from unrelated repositories may overlap.
- `full` is a barrier in its repository lane. Earlier lane work finishes first; later lane
  work cannot pass it. Other repositories may continue when their declared resources fit.
- `land` is one gate-and-publication barrier in the same lane. No later lane job can begin
  between its preflight, gate, and atomic publication. Retained legacy `merge` rows remain
  identifiable in migrated history but are not the normal public landing workflow.

Capacity defaults to `jobs=2`. Set `AGCOORD_CAPACITIES` before the broker starts using either
JSON (`{"jobs":4,"cpu":8,"browser":1}`) or comma-separated pairs
(`jobs=4,cpu=8,browser=1`). The state-directory settings do not configure capacity, and a
live owner keeps the capacity map with which it acquired the spool.

Every job implicitly requests `jobs=1`. Jobs add resources with repeatable
`--resource NAME=UNITS` options. Names are generic machine capabilities such as `cpu`,
`memory`, `browser`, or a project-defined singleton; units are positive integers and the name
must exist in the configured capacity map. Admission requires every request to fit, and
allocation is held until the complete worker process group is gone. A request that can never
fit is rejected rather than left queued forever. Scheduling does not infer resource use from
labels or commands.

Fairness applies across lane barriers and capacity: a compatible check can overlap work in
another repository, but it cannot leapfrog an earlier barrier in its own lane or starve an
older request indefinitely.

## Durable job shape

The snapshot top level is strict:

```text
protocol · broker_pid · captured_at · capacities · allocations
active · queued · recent
```

`capacities` maps each configured resource to a positive integer; `allocations` maps it to
the nonnegative units currently held. Active and queued jobs appear in scheduling order;
recent contains retained terminal history.

Each row contains exactly:

```text
run_id · sequence · status · kind · label · agent
repository_id · repository · worktree_id · checkout · branch · head_sha
barrier · resources · blocked_by
gate_run_id · publication · failure_reason · phase · gate_exit_status
caller_pid · command · created_at · started_at · finished_at
exit_status · worker_pid · cancel_requested · log_bytes · position
```

`kind` is exactly `check`, `full`, `merge`, or `land`; `merge` remains representable for
existing receipt-backed history while new public landing uses `land`. `publication` is
either null or the normalized `{adapter, request}` record for publication work. `resources`
is the immutable requested-unit mapping,
including implicit `jobs=1`;
`blocked_by` identifies durable predecessors currently preventing admission. Repository and
worktree IDs are stable opaque identities, while their resolved values remain available for
operators. Missing times and kind-specific values are null rather than guessed. A queued
position is one-based; active and terminal rows use null.

The durable status state machine is
`queued -> running -> passed|failed|cancelled|interrupted`, with direct
`queued -> cancelled` allowed. `phase` is always present. Checks, full gates, and retained
merge jobs use `queued -> running -> complete` and keep `gate_exit_status` null. A land job
uses `queued -> preflight -> gating -> publishing -> complete`; its gate status remains null
until the gate finishes and is then the actual shell status. Terminal rows are immutable
history. `passed` and `failed` carry the overall job status, `cancelled` uses the
coordinator's cancellation status, and `interrupted` never claims a gate or publication
verdict.

## Running checks and standalone full gates

Submit a check and follow its combined output to completion:

```bash
agcoord run \
  --label "API unit tests" \
  --resource cpu=2 \
  -- python -m pytest -q tests/api
```

Use `full` for an exact-head verdict that is useful independently of publication:

```bash
agcoord full \
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
receipt as a separate authorization step.

`full` is a repository-lane barrier, not machine-wide exclusivity. Declare every scarce
machine resource it requires. Capacities—not the fact that the job is named “full”—decide
whether work from another repository can overlap.

The coordinator snapshots the submitting client's execution environment for its worker, but
does not expose it through rows, CLI output, or the TUI, and clears it when the job starts or
is cancelled while queued. A reserved admission marker prevents a coordinated worker from
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

The values are claims, not admission by themselves. An internal repository gate wrapper can
pass them to the existing internal admission verifier together with its resolved checkout,
fresh exact head, and process identity. A `full` wrapper is the admitted worker and verifies
with its own PID. A `land` gate is a child of the admitted land worker and verifies with its
parent PID. Checks receive the same context for diagnostics and nested-run protection, but
are not repository barriers and therefore cannot pass barrier admission verification.

Verification fails closed when the state directory has no matching live owner or when the
run ID, kind, checkout, head, PID, or process start identity differs from the durable active
row. Repository wrappers should verify immediately before protected gate work and treat a
refusal as a failed gate. The verifier remains an internal seam for wrappers, not a second
submission or user-facing workflow.

## Atomic landing

The normal landing operation is one durable request containing the forge adapter/request,
exact checkout/branch/head, gate command, private caller environment, and resource claim:

```bash
agcoord land 123 \
  --label "gate and publish PR 123" \
  --checkout /absolute/path/to/worktree \
  --resource cpu=4 \
  -- ./scripts/test.sh
agcoord land 123 --adapter github -- ./scripts/test.sh
```

The CLI defaults `--adapter` to `github` as a convenience, while the client API and durable
publication record keep the adapter and request separate. The current GitHub adapter accepts
a pull-request number; future adapters can normalize their own request shape. Submission
requires a clean checkout at one full 40-hex `HEAD` and inserts exactly one `kind=land`
repository barrier. Its command and environment are the gate, not a second queued job, and
its `gate_run_id` is null.

The worker first validates local and forge identity, readiness, and the current target and
source refs. A stale base is refused before the gate command can run. If preflight succeeds,
the worker runs the gate once with the captured environment and combined transcript. A red
gate records its actual shell status with `failure_reason=gate-failed` and never calls the
publisher. A green gate transitions the same row directly to `publishing` while retaining
the repository barrier and every resource allocation. No check, gate, or second landing in
that lane can enter the gap because no separate job or gap exists.

Publication repeats the exact validation after the gate. Let `M` be the target's observed
remote commit and `H` the submitted head; `M` must be an ancestor of `H`. The forge-neutral
publisher creates candidate `C` with tree exactly `H` and parents exactly `(M, H)`, without
moving a reference, then performs one atomic compare-and-update for target `M -> C` and
source `H -> H`. Both comparisons participate even though the source value does not change.
A real non-main target such as `release` uses the same contract; an absent reported base is
a readiness refusal. GitHub support is an optional adapter and is not needed to install,
import, or run checks in the core coordinator.

If the target advances before or during the gate, the result is `stale-main`; if the source
head advances, it is `head-changed`. `pr-not-ready`, `publish-failed`, and `merge-error`
remain distinct handbacks. A failed publication leaves the remote target untouched. If the
exact `H` is already an ancestor of the remote target, recovery/retry is idempotent success
even if forge metadata still says open or an auto-delete removed the source reference.

AGCoord never fetches a replacement branch into the worktree, refreshes, rebases, amends, or
mutates the ticket branch. A stale or changed-head refusal hands control back to the agent:
update the branch explicitly, push, and submit a fresh `agcoord land` request so the new head
is gated and published together. Do not substitute a separate full-plus-merge sequence or
direct target update.

A land request is cancellable while queued, preflighting, or gating. Once its durable phase
is `publishing`, cancellation is refused because killing a client during an authenticated
atomic mutation would leave the outcome indeterminate. Graceful broker stop cancels safe
earlier phases but waits for publishing and records its authoritative result.

## Observation and cancellation

Every accepted job has one stable ID, durable row, and combined stdout/stderr log. Use:

```bash
agcoord list
agcoord list --json
agcoord show <run-id>
agcoord log <run-id> [--follow]
agcoord cancel <run-id>
```

Queued work is cancellable. Running checks and full gates, plus land preflight and gating,
receive process-group cancellation and become terminal only after every descendant is gone.
Publishing land jobs refuse cancellation as described above. Unknown IDs and terminal jobs
produce named errors rather than silently changing another row.

`agcoord clear` is intentionally narrow. It refuses while any job is queued or running. Once
the coordinator is inactive, it removes terminal rows and their run logs but preserves the
spool, ownership protocol, broker diagnostics, and migration records. There is no `--all`
shortcut that deletes coordinator state.

## Terminal UI

`agcoord tui` is a credential-free live view over the same client API. It shows active,
queued, and recent terminal jobs across repositories; kind, lane/repository identity,
declared resources, status, phase, label, timing, head, publication, gate exit, and failure details
remain inspectable without truncating the durable values.

The expected key map is:

| Key | Action |
| --- | --- |
| `r` | refresh from the durable snapshot |
| `Enter` | inspect the selected job and exact fields |
| `l` | read the selected job log |
| `c` | request cancellation; a publishing land explains why it cannot be cancelled |
| `h` | hide or restore terminal history without deleting it or rereading the spool |
| `?` | show context-sensitive help |
| `q` | quit the view; jobs continue |

Refresh preserves the selected run and both viewport offsets when that row still exists.
At 80 columns the compact table is exactly `STATE · KIND · REPO · RUN · LABEL · AGE · DUR`
and does not require horizontal navigation. Adjacent columns retain a visible gutter, and a
value wider than its compact cell ends in a visible ellipsis instead of running into the next
column or disappearing at a hard edge. `AGE` is time since creation; `DUR` is absent before
start, live while running, and fixed at finish. Full agent, repository and worktree identity,
label, checkout, branch, barrier, resources, blockers, command, exact created/started/finished
timestamps, head, receipt, publication, phase, gate exit, and failure values remain available
without abbreviation in persistent selection detail and the detail view. A land remains one
selected row as it moves from gating to publishing, and `l` reads one combined
gate-and-publication transcript rather than separate run logs.

## Worker scratch and cleanup

Every admitted job receives a private owner-only run directory rooted under the host system
temporary filesystem. AGCoord overrides `TMPDIR`, `TMP`, and `TEMP` even if the caller set a
different value. A stable namespace prevents two state directories or repositories from
colliding, and the exact run ID is the leaf.

The root exists while the worker process group is live, including both gate and publication
phases of one land. Terminal status is withheld until the group is gone, the leader is
reaped, and scratch has been reclaimed. Cleanup restores
owner traversal on nested mode-`000` trees before removal. On restart, the broker removes
terminal and orphan roots but preserves the root of a genuinely live recovered process.
Reclamation returns temporary pages to the kernel and prevents one job inheriting another's
files.

## Migrations

Protocol migration is explicit and out of band. Normal broker or client initialization fails
closed on an older spool and names the required command; it never mutates schema on a hot
path. With no live old owner or jobs, run:

```bash
agcoord migrate
# or, for an intentionally isolated spool
agcoord --state-dir /path/to/state migrate
```

Back up the owner-only state directory first when its history matters. After migration,
`agcoord list` starts or joins a broker using the new protocol. Migration preserves only
facts represented by the old schema; it never upgrades a legacy label into an exact-head
receipt, fuses separate full and merge rows into a land, or invents a gate phase/status that
the legacy row did not record.
