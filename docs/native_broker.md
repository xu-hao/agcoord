# Native broker architecture and security contract

This document defines the boundary for AGCoord's single-executable Rust broker. It is
normative for the native implementation, Python client compatibility, host packaging, and the
migration from the protocol-4 Python broker.

## Scope and one-executable definition

The long-lived scheduler and its pre-exec worker setup run from one root-owned Rust executable.
That executable statically contains its scheduler, SQLite, configuration parser, resource
backends, recovery code, and worker setup. Starting or supervising the broker never imports
Python or loads AGCoord project files.

The one-executable contract does not fold mutable operator data into the binary. These remain
external:

- the state directory, `config.json`, SQLite spool, logs, and recovery manifests;
- the systemd service and AppArmor policy;
- the package manifest, checksums, provenance, and reviewed license inventory;
- the submitted command, its repository, and its own runtime dependencies;
- the Python `agc` CLI, TUI, pytest-xdist plugin, and optional publication adapters.

A `land` worker may execute the Python publication adapter selected by the submitting client.
That adapter is admitted workload, not a broker startup dependency, and runs after the native
worker has left the broker setup profile.

A Python virtual environment, zip application, copied interpreter, or self-extracting Python
bundle is not a compliant broker artifact. A dynamically linked development build is allowed
only when clearly identified as such; a release artifact must pass the documented static-link
audit.

## Process topology

```text
Python clients                 private state directory
agc / TUI / xdist  ──SQLite──> queue.sqlite3 + logs
       │                                ▲
       │ starts or observes             │ owns authoritative transitions
       ▼                                │
root-owned Rust broker ─────────────────┘
       │ fork/clone; never execs a setup helper
       ▼
blocked native worker ──attach/verify──> delegated cgroup leaf
       │ private release and setup pipes
       │ user + cgroup + mount namespace; capability drop
       ▼
submitted command (including an optional Python land adapter)
```

The broker owns one state directory by holding an exclusive `flock` on `broker.lock`. It is the
only process allowed to perform scheduler transitions, resource lifecycle operations, worker
release, publication-phase authority, recovery, and terminal receipt updates. Clients submit
and observe through the private durable spool; there is no network listener.

Native startup retries acquisition for at most 250 milliseconds so a short ownership probe
cannot make the selected broker abandon startup. A lock held through that bounded interval is
still a live-owner conflict and returns `broker-already-owned`; the retry never permits two
owners or weakens the exclusive lock.

### Why the durable spool remains the client protocol

Restricted agent sessions may refuse local socket creation, while SQLite transactions in a
user-owned directory already support concurrent terminals, crash-safe submission, and offline
inspection. Protocol 5 therefore remains a filesystem protocol rather than adding a required
Unix socket. The database file and state directory are mode `0600` and `0700` respectively and
must be owned by the broker account.

The state directory is not a boundary between mutually hostile processes with the same UID.
Any same-UID process that can write the spool is inside the coordinator trust domain. The native
boundary protects kernel resource controls, publication ordering, unrelated processes, and the
host from admitted commands; it does not claim to make a user-writable SQLite file tamper-proof.
A future multi-user service would require a different authenticated IPC and service identity.

## Trust model

The implementation trusts:

- the kernel primitives it verifies, the root-owned native executable, and the loaded policy;
- systemd above the delegated cgroup boundary;
- the broker account as owner of its configuration and spool;
- the exact publication adapter and request admitted by a `land` row.

It treats these inputs as malformed or hostile until validated:

- every client-authored database field and JSON value;
- repository paths, Git output, commands, environment entries, and labels;
- PIDs, process start tokens, inherited descriptors, recovery manifests, and stale cgroup leaves;
- controller files, mount topology, quota records, stored receipts, and previous broker state;
- direct binary invocation and any attempt to enter internal worker setup independently.

The protected assets are an exact landing verdict, durable history, resource-enforcement truth,
unrelated processes and cgroups, host filesystems, and credentials omitted from public rows.

### Required threat responses

| Threat | Required response |
| --- | --- |
| A second Python or Rust owner | Exclusive lock acquisition fails before any worker or schema mutation. |
| Unsupported or partial schema | Refuse startup with a stable protocol error; never repair implicitly. |
| Malformed submitted or stored data | Refuse or interrupt only the owning row; never pass unchecked data to a syscall. |
| PID reuse or forged worker identity | Match PID, start token, process group, and private inherited channel. |
| Direct internal-worker invocation | No public worker subcommand; setup requires broker-created inherited descriptors and token. |
| Command execs the broker binary | From admitted work it enters the restricted client profile and cannot regain the setup domain. The public setup binary exposes no arbitrary worker mode. |
| Broker crash before worker release | Pipe EOF keeps the blocked launcher from executing user code. |
| Broker crash after release | A replacement adopts only identity-verified live state and never executes the command again. |
| Changed head or target during landing | Refuse publication and discard the prior verdict. |
| Missing or unverifiable enforcement | A required binding fails before release; best-effort records an unapplied event. |
| Cancellation during publication | Once the authoritative ref mutation begins, retain ownership until its result is durable. |
| Spool modified by the broker UID | Revalidate every row and fail closed; do not claim same-UID tamper resistance. |

## Durable protocol 5

Protocol 5 is the first native-owner protocol. Its compatibility boundary consists of the
SQLite schema, canonical JSON shapes, owner-lock metadata, log paging rules, process identity
format, and stable refusal codes. JSON is UTF-8, uses object keys exactly as documented by the
coordinator contract, and stores integers within signed 64-bit range. Commands and environment
values contain no NUL bytes.

Protocol 5 retains the protocol-4 `runs` and `child_cpu_leases` tables so current history can be
migrated without reinterpretation. It adds these `coordinator_meta` values:

```json
{
  "protocol": "5",
  "owner_implementation": "rust-native",
  "schema_fingerprint": "agcoord-spool-v5"
}
```

The owner lock is newline-delimited UTF-8 with unique keys. Clients reject missing, duplicate,
unknown-version, invalid-JSON, or oversized metadata while the lock is held. A canonical native
owner record is:

```text
pid=12345
protocol=5
implementation=rust-native
version=0.3.0
build=sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
capacities={"cpu":8,"jobs":2}
resource_bindings={}
resource_capabilities={}
started_at=2026-08-31T17:00:00+00:00
```

The build digest may be `development` only for an explicitly selected development binary. A
release client refuses that value when its configured policy requires a release artifact.

### Executable discovery

Every Python client selects one absolute executable from the state directory's `config.json`.
The default is the host-package path `/usr/libexec/agcoord/agcoord-broker`; clients never search
`PATH`, copy an interpreter, import a checkout as the broker, or fall back to the Python owner.
When selecting the command, the client rejects symlinks, non-executable or group/world writable
files, unsupported hosts and targets, and incompatible identity JSON. A release binary must be
root-owned, identify the `x86_64-unknown-linux-musl` target, and report a SHA-256 build identity;
host artifact auditing establishes that it is static. A deliberately selected development
binary may instead be owned by the current user and report `development` only when
`allow_development` is true.

```json
{
  "native_broker": {
    "path": "/absolute/path/to/target/debug/agcoord-broker",
    "allow_development": true,
    "managed_service": false
  }
}
```

The selected executable and a live owner must have the same supported version and build
identity. A missing, stale, malformed, or incompatible executable produces an actionable
refusal without accepting work or replacing the live owner.

### Implemented scheduler and state boundary

The native executable implements the protocol-5 owner lock, SQLite spool initialization,
submission validation, admission, repository barriers, queue-order-preserving round-robin
selection, generic capacity accounting, cancellation, land-phase authority, history reads,
worker observation, child leases, and explicit migration and rollback. The Python CLI, client,
TUI, and pytest-xdist adapter use these native commands while retaining their public JSON and
environment contracts.

`serve` validates the complete schema and every stored run before changing activity metadata,
puts new and migrated databases in WAL mode, and uses `database_timeout` from the state
directory's strict `config.json`. Busy or locked pump transactions are retried. Other structural
errors stop the owner with a stable JSON refusal; one executable that cannot be spawned fails
only its owning row.

Admission commits before worker creation and worker identity commits separately. A replacement
therefore classifies a committed admission with no live worker as `interrupted`, adopts an exact
live PID/start-token identity without running the command twice, and preserves an already
terminal commit. Debug builds expose bounded crash-injection points for these commits and their
cleanup; release builds reject the injection option.

On `SIGINT` or `SIGTERM`, the owner stops admitting work, leaves queued rows durable for the next
owner, requests cancellation of active checks, full gates, and pre-publication lands, and drains
them. A running legacy merge or a land whose `publishing` transition committed remains
authoritative and is drained to a durable result. The identity-verified land-phase transaction
and cancellation transaction both take the same immediate SQLite write lock, so exactly one
wins their race.

### Implemented worker boundary

The native owner now forks its launcher path directly without an internal command-line mode.
Two `O_CLOEXEC` pipes and a kernel-random 256-bit token bind the launcher hello, its PID and
process-group identity, the setup result, and two distinct releases. The broker commits the
PID/start token and clears the durable environment before the first release. EOF, a short or
substituted channel, a stale token, launcher death, or broker death before final release exits
the child with status 125 and cannot execute the submitted command.

Between the releases, the child resets inherited broker signal handlers and completes namespace
and scratch setup. When it inherited the managed `agcoord-broker` AppArmor domain, it makes the
one-way transition into `agcoord-admitted` and verifies that domain before clearing effective,
permitted, inheritable, and ambient capabilities, setting `no_new_privs`, and verifying those
values from `/proc/self/status`. It closes every inherited descriptor except standard I/O and the
two private channels, reports the token-bound setup result, waits for final release, closes those
channels, verifies the final descriptor set, and calls `execve`. Submitted `_AGCOORD_*` variables
are removed; the broker supplies only the public admission context itself. Debug builds can
inject bounded launcher, token, channel, privilege, descriptor, setup, and release failures;
release builds do not accept those controls.

Cancellation targets the verified process group and keeps the row and allocation live until
every descendant is gone, escalating from `SIGTERM` to `SIGKILL`. Recovery adopts only a live
leader whose PID, start token, and process group all match. A conflicting live PID is classified
as lost without receiving any signal; a vanished verified leader may leave its original group
to be drained. For cgroup-backed runs, cancellation instead uses the identity-verified owned leaf
and `cgroup.kill`, waits for that leaf to become empty, retains its final measurements, and then
removes only the recorded leaf and owner metadata.

The native `cgroup-v2` backend now implements the delegated-root probe, collision-safe owner and
run leaves, controller enablement and readback, attachment, namespace rooting, recovery,
cancellation, and cleanup. It enforces CPU, process, memory, memory-pressure, swap, tmpfs,
bandwidth, IOPS, and I/O-weight bindings, and retains conservative peaks and deduplicated events
in the same receipt shape as the Python owner. Required setup failures stop before user code;
best-effort tmpfs mount failures can receive an explicit second release onto the owned disk
directory only after capability and descriptor cleanup still verifies.

The native `project-quota` backend implements the same independent persistent-scratch contract as
the Python owner. It resolves a directly identifiable local ext4 or XFS mount, allocates a
collision-safe high-range project ID under a mount-global lock, applies and reads back byte and
inode limits, and records the exact mount, tree, and project identity for recovery. Required
failures stop before user code, and best-effort fallback is allowed only before allocation and
after the launcher proves capability cleanup and `no_new_privs`. Completion, cancellation, and
replacement-broker recovery retain terminal usage before clearing limits and removing only the
identity-verified owned tree. Unsupported or changed topology and durable state are refused
without enabling filesystem features, editing system project databases, or mutating an
unverified tree.

### Client-authored operations

Clients continue to use short SQLite transactions rather than mutating live processes:

- submission inserts one complete `runs` row in `queued` state with an immutable command,
  environment, checkout identity, resource request and contract;
- cancellation sets the durable cancellation request, except that a queued row may be made
  terminal atomically;
- child lease acquisition inserts a waiting lease tied to the admitted run, PID and start token;
- lease release or cancellation updates only the authenticated lease request;
- an admitted land adapter reports phase and result only when run ID, kind, exact checkout head,
  worker PID and start token all match.

Every mutation is committed before success is returned. The broker independently validates the
complete row before acting. Client code never performs admission, attaches resources, releases
a worker, marks publication authoritative, or writes an applied enforcement receipt.

The canonical queued submission fields remain the protocol-4 columns. This logical fixture
omits broker-owned null fields but fixes the submitted values:

```json
{
  "run_id": "check-0123456789ab",
  "status": "queued",
  "kind": "check",
  "phase": "queued",
  "label": "focused tests",
  "agent": "unnamed",
  "repository_id": "sha256:repository",
  "repository": "github.com/example/project",
  "worktree_id": "sha256:worktree",
  "checkout": "/absolute/ticket-worktree",
  "branch": "issue-123-example",
  "head_sha": null,
  "barrier": 0,
  "resources_json": "{\"cpu\":1,\"jobs\":1}",
  "command_json": "[\"python\",\"-m\",\"pytest\",\"-q\"]"
}
```

Public snapshot, status, log, lease and migration results retain their current strict JSON
shapes unless a later protocol explicitly versions them. Unknown fields may be added only where
the current validator permits them; otherwise a protocol increment is required.

### Broker-authored transitions

The broker owns these state-machine edges:

```text
queued ──admit──> running ──worker result──> passed | failed
   │                 │
   └─cancel────────> cancelled
                     ├─cancel─────────────> cancelled
                     └─lost identity──────> interrupted

land phases: queued -> preflight -> gating -> publishing -> complete
```

It writes `started_at`, `finished_at`, worker identity, resource private state, applied and peak
receipts, failure reason, exit status, authoritative gate status, and publication phases. Each
transition and its durable evidence commit atomically. The pump retries only SQLite busy or
locked results; semantic and structural failures are terminal or broker-fatal according to the
coordinator contract.

### Stable refusal envelope

Stored resource events keep the existing backend, resource, stage, status and code fields.
Native-only startup and protocol refusals use a stable kebab-case code plus a human message:

```json
{
  "code": "broker-protocol-mismatch",
  "message": "native broker needs spool protocol 5"
}
```

Messages may include an operator-selected path but never credentials, raw kernel exceptions,
environment values, or publication secrets. Clients branch only on codes and protocol numbers.

The scheduler/state implementation freezes these refusal families:

| Family | Stable codes |
| --- | --- |
| Command and configuration | `broker-command-invalid`, `broker-config-invalid` |
| State and ownership | `broker-state-invalid`, `broker-state-missing`, `broker-already-owned`, `broker-not-running`, `broker-owner-lock-unavailable`, `broker-owner-metadata-invalid` |
| Protocol and storage | `broker-protocol-mismatch`, `broker-protocol-unsupported`, `broker-schema-invalid`, `broker-row-invalid`, `broker-wal-unavailable`, `broker-database-busy`, `broker-database-error` |
| Submission and admission | `broker-submission-invalid`, `broker-run-exists`, `broker-run-unknown`, `broker-run-terminal`, `broker-resource-unavailable`, `broker-active-state-invalid` |
| Gate and land authority | `broker-gate-required`, `broker-gate-mismatch`, `stale-gate-verdict`, `broker-land-phase-invalid`, `broker-land-identity-mismatch`, `broker-land-cancelled`, `broker-publication-authoritative` |
| Migration | `broker-migration-live-runs`, `broker-migration-row-invalid`, `broker-migration-state-changed`, `broker-migration-backup-failed`, `broker-migration-backup-invalid` |
| Worker ownership | `broker-worker-start-failed`, `broker-worker-handshake-failed`, `broker-worker-identity-invalid`, `broker-worker-identity-mismatch`, `broker-worker-observation-failed`, `broker-worker-signal-failed`, `worker-privilege-drop-failed`, `worker-privilege-drop-unverified`, `worker-profile-transition-failed`, `worker-profile-transition-unverified`, `worker-descriptor-leak` |

Later resource backends may add backend-specific refusal or receipt-event codes without changing
the meaning of these codes.

## Native worker contract

The serving process forks or clones its worker path inside the same executable. There is no
command-line worker mode. Before the fork, the broker creates release and setup channels with
close-on-exec defaults plus a random 256-bit run token. The child proves the inherited channel
and token, blocks, and exits with status 125 if the broker closes the channel before release.

The broker then:

1. durably prepares every backend;
2. records the child PID, process start token and process group;
3. attaches the complete process tree to the prepared cgroup leaf;
4. verifies membership and every requested control value;
5. commits the attached state; and
6. releases setup exactly once.

The child creates private user, cgroup and mount namespaces, maps only its broker UID and GID,
roots the visible cgroup hierarchy at its leaf, provisions optional tmpfs, enters and verifies
the admitted AppArmor domain when managed policy is active, drops effective, permitted,
inheritable and ambient capabilities, sets `no_new_privs`, reports verified setup, and waits for
the final release. It then `execve`s the submitted command.

The setup-only `agcoord-broker` AppArmor profile attaches to the immutable root-owned executable,
not to a user-editable service directive. Its public command parser exposes no worker mode or
arbitrary setup-domain exec path. The authenticated worker makes a one-way transition into
`agcoord-admitted` before setting `no_new_privs` and before reporting successful setup. Arbitrary
interpreters inherit that domain. When admitted work invokes the broker, AppArmor stacks
`agcoord-broker-client` onto the admitted domain; adding confinement is compatible with
`no_new_privs` and cannot regain setup permission. Both restricted profiles deny user-namespace
creation and changing back to the setup domain. Every domain is compiled in explicit enforce
mode rather than Ubuntu 24.04's unconfined `default_allow` mode. A failure to verify the loaded
setup profile, service cgroup, global restriction, or backend namespace probe makes required work
unavailable.
The complete package and transition contract is in [the native host runbook](native_host.md).

## Compatibility and migration

Python protocol-4 and Rust protocol-5 owners never share a live state directory. Migration is
explicit and requires:

1. no held owner lock;
2. no queued or running rows;
3. a private verified backup of the database, WAL and SHM state after checkpoint;
4. validation of every protocol-4 row, receipt and lease;
5. one atomic metadata transaction changing protocol and native-owner fingerprint; and
6. a post-migration open by the exact native binary selected for startup.

Protocol 1 through 3 first use their already-defined migrations to protocol 4. Rollback is a
separate explicit idle operation that restores the verified protocol-4 backup; neither client
nor broker performs an implicit down-migration. A moved binary, changed build digest, target
branch move, or incompatible client invalidates previous startup or landing evidence.

The native migration command checkpoints WAL, creates and fsyncs a mode-`0600` backup, verifies
its owner, protocol, SQLite integrity, schema, rows, and lack of live work, and only then commits
the protocol-5 owner fingerprint and the first sequence eligible to authorize native
publication. Protocol-1 through protocol-3 input is transactionally normalized to protocol 4
first, without inventing resource enforcement or child leases, and the normalized protocol-4
database is the rollback baseline.

Rollback also requires an idle owner lock. In one transaction it restores that verified
protocol-4 baseline and replays terminal protocol-5 rows and leases, preserving history while
discarding no authoritative result. It records `invalid_gate_through_sequence` at the greatest
sequence observed before rollback. A protocol-4 client excludes every receipt at or below that
cutoff, whether selected automatically or named explicitly, so publication requires a new full
gate after rollback.

Python clients recognize protocol-4 history only to provide a controlled migration path. A
default/autostart client never launches or joins the old Python owner: a live protocol-4 owner
must finish and stop, and an idle protocol-1-through-4 spool requires `agc migrate`. The native
broker refuses an old spool until that explicit migration succeeds, while the old Python
`serve` entry point refuses protocol 5. Internal non-autostart compatibility access remains
available only for migration tests and already admitted legacy workers; it is not a public
startup fallback. Operators use the separately tested
[native migration and rollback runbook](native_migration.md) for compatibility selection,
whole-spool backup, rollback rehearsal, live transition, capability evidence, troubleshooting,
and retirement of the old production path.

## Implementation and release order

The epic proceeds through the linked issues in this order: architecture and threat model,
reproducible artifact, scheduler and state, worker lifecycle, cgroup parity, project quota,
Python compatibility, host packaging, conformance, then migration documentation and release.
No resource backend becomes the default until its public behavior passes the shared black-box
conformance suite. No ticket authorizes a PyPI upload without a separate explicit user request.
