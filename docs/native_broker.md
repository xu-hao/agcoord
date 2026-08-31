# Native broker architecture and security contract

This document defines the target boundary for AGCoord's single-executable Rust broker. It is
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
| Command execs the broker binary | The admitted-command AppArmor profile denies execution of the setup executable. |
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

Native-client integration must select an absolute executable path explicitly. A host package
uses `/usr/libexec/agcoord/agcoord-broker`; a development operator may instead configure a
different absolute path. Clients never search `PATH`, copy an interpreter, import a checkout,
or fall back to Python after selecting protocol 5. Before starting an owner, the client runs
`identity --json` and requires the configured protocol, `rust-native` implementation, supported
target, non-development build policy, and expected executable ownership.

The protocol-4 Python broker remains the current owner until the explicit migration and client
integration tickets land. Merely building or placing the native artifact does not change a live
broker or state directory.

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
roots the visible cgroup hierarchy at its leaf, provisions optional tmpfs, drops effective,
permitted, inheritable and ambient capabilities, sets `no_new_privs`, reports verified setup,
and waits for the final release. It then `execve`s the submitted command.

The AppArmor setup profile attaches only to the root-owned native binary. Its exec transition
moves submitted commands into an admitted-command profile that denies user-namespace creation,
administrative capabilities, and execution of the broker binary. The broker performs no other
exec after startup. A failure to verify the loaded profile or transition makes required work
unavailable.

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

During rollout, Python clients may understand both owner records but start only the native
binary for protocol 5. The old Python `serve` entry point refuses protocol 5. The native broker
refuses protocol 4 until the explicit migration command succeeds.

## Implementation and release order

The epic proceeds through the linked issues in this order: architecture and threat model,
reproducible artifact, scheduler and state, worker lifecycle, cgroup parity, project quota,
Python compatibility, host packaging, conformance, then migration documentation and release.
No resource backend becomes the default until its public behavior passes the shared black-box
conformance suite. No ticket authorizes a PyPI upload without a separate explicit user request.
