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
owner-only. An ownership lock elects exactly one broker. Simultaneous first clients either
join that owner or fail without an accepted row; they never create two supervisors for the
same spool. An unmanaged development configuration lets the first ordinary client start the
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
An explicit broker close remains a graceful cancellation boundary and reaps safe workers
before releasing ownership.

A land worker reports its final overall status durably while its row is still running. After
an unclean owner loss, a replacement preserves the live worker's lane and resources, never
reruns its gate, waits for the exact process group to disappear, reclaims scratch, and then
exposes the reported passed or typed-failed result. If the worker disappears before it can
report a result, `interrupted` is the only safe terminal classification.

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
spool cannot autostart an ordinary owner. A legacy protocol-4 owner that predates this command
is asked to stop only after its live count reaches zero, and only through a pidfd for the PID
the kernel reports as the current ownership-lock holder.

The receipt contains exactly `state`, `drain_id`, `reason`, `started_at`, `protocol`, `live`,
and `broker_pid`. The marker and its SQLite guards survive client exit, broker crash, host
activation, migration, and rollback. `resume` requires the exact `drain-…` identifier, zero
queued or running rows, and exclusive owner-lock acquisition. It removes the marker and guards
in one transaction and returns the spool to `open`; a wrong or stale identifier cannot reopen
submissions. Migration and rollback preserve the current marker across the protocol boundary
and never resurrect a marker already removed by a successful resume.

## Repository lanes and resources

Each submission belongs to a stable repository lane and records its resolved worktree. A
lane preserves publication order without turning one repository's full gate into a lock on
the entire machine:

- `check` is ordinary work. Compatible work overlaps across repositories, worktrees, and
  kinds whenever the declared resources fit.
- `full` is ordinary lane work with a clean exact-head submission requirement and a durable
  receipt. It is not a barrier: checks, other fulls, and lands in other worktrees overlap it
  when their resources fit.
- `land` is the only barrier. It is one gate-and-publication barrier in its lane: no other
  land in the lane, and no job from the same worktree, can begin between its preflight, gate,
  and atomic publication. Work from other worktrees of the same repository only competes for
  capacity. Retained legacy `merge` rows are barriers of the same shape, remain identifiable
  in migrated history, and are not the normal public landing workflow.

One JSON file, `config.json` in the state directory, configures the broker that owns that
directory. It holds at most `capacities`, `bindings`, `cgroup_root`, `cgroup_io`,
`database_timeout`, and `native_broker`; invalid JSON, a top-level value that is not an object,
an unknown key, a section that is not an object, or an empty `cgroup_root` is refused when the
client or broker loads its configuration. When present, `cgroup_io` contains exactly one
nonempty `paths` list of unique absolute strings. `database_timeout` is a positive finite number
of seconds no greater than `2147483.647` (SQLite's millisecond limit) and defaults to `10`. An
absent file uses `/usr/libexec/agcoord/agcoord-broker`, requires its release trust policy, and
defaults capacity to `jobs=2`.

```json
{
  "capacities": {"jobs": 4, "cpu": 8, "browser": 1},
  "bindings": {
    "cpu": {"kind": "cpu", "unit": "logical-cpu", "mode": "required", "backend": "cgroup-v2"}
  },
  "cgroup_root": "/sys/fs/cgroup/user.slice/user-1000.slice/user@1000.service/app.slice/agcoord-broker.service",
  "database_timeout": 30,
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
contract. Development from a checkout is explicit: select the absolute regular executable and
set `allow_development` to `true`. That permits only a current-user- or root-owned supported GNU
or musl development build; it does not weaken file, identity, protocol, or live-owner matching
checks. `managed_service=true` is valid only for the installed service's default state directory
and makes autostart call the fixed user unit instead of spawning the binary. Clients never
search `PATH` and never fall back to the Python broker.

No environment variable configures capacity, bindings, the delegated cgroup root, or block-I/O
paths or the database timeout; `AGCOORD_STATE_DIR` selects which state directory, and therefore
which configuration file, a client and broker share. A live owner keeps the capacity and
enforcement configuration with which it acquired the spool, so editing those sections changes
the next broker rather than the running one. Each process retains the `database_timeout` it
read when opening the spool; newly started clients or brokers can therefore use an updated lock
wait without changing existing connections.

Schema setup places every current-protocol spool in SQLite WAL journal mode, including an
existing spool the next time a compatible client or broker opens it. Readers therefore do not
block behind an ordinary writer. The configured timeout bounds each remaining lock wait;
transient busy or locked results in the broker pump and idle health check are retried, and a
contended best-effort activity heartbeat never changes an already successful public operation
into an apparent failure.

Every job implicitly requests `jobs=1`. Jobs add resources with repeatable
`--resource NAME=UNITS` options. Names are generic machine capabilities such as `cpu`,
`memory`, `browser`, or a project-defined singleton; units are positive integers and the name
must exist in the configured capacity map. Admission requires every request to fit, and
allocation is held until the complete worker process group is gone. A request that can never
fit is rejected rather than left queued forever. Scheduling does not infer resource use from
labels or commands.

Admission greedily packs the complete declared resource vectors rather than assigning fixed
per-job shares. On each pass the scheduler takes each repository lane's first queued job whose
blockers are empty, rotates fairly among repositories, admits one, and repeats until no lane has
an admissible job. A blocked lane job—waiting for a land, for its worktree, or for capacity—lets
later admissible lane work pass it; only lands keep submission order among themselves and behind
earlier same-worktree work. Thus an eight-CPU host can overlap requests for six
and two CPUs, or four requests for two CPUs, without a preselected “two jobs at four CPUs each”
partition. `jobs` remains an independent concurrency ceiling and does not replace CPU, memory,
temporary-storage, or disk claims. Greedy backfill can delay a large request while smaller
requests continue to fit; it does not reserve future capacity for that request.

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

A binding contains exactly `kind`, `unit`, `mode`, and `backend`. The supported typed pairs are
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
permission. The Python broker therefore has no supported AppArmor exception for this host
policy. The Rust executable provides the narrow broker-specific target, but it does not bypass
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
mount; the private setup record and ownership token are removed from the command environment.
The `noexec` policy means tools must execute generated programs through an interpreter or place
executables outside temporary scratch.

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
worktree, and lane rotation keeps one repository from monopolizing admission. Greedy backfill
remains: a smaller request can pass a larger one that does not yet fit.

## Durable job shape

The snapshot top level is strict:

```text
protocol · broker_pid · captured_at · capacities · allocations
resource_bindings · resource_capabilities
active · queued · recent
```

`capacities` maps each configured resource to a positive integer; `allocations` maps it to
the nonnegative units currently held. `resource_bindings` is the exact machine binding map
frozen by the live broker. `resource_capabilities` maps each installed or referenced backend to
its `available`, `kinds`, `units`, `operations`, and stable `reason` fields. It contains no host
paths or backend exception text. Active and queued jobs appear in scheduling order; recent
contains retained terminal history.

Each row contains exactly:

```text
run_id · sequence · status · kind · label · agent
repository_id · repository · worktree_id · checkout · branch · head_sha
barrier · resources · resource_contract · resource_receipt · blocked_by
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
`blocked_by` identifies durable predecessors currently preventing admission. Repository and
worktree IDs are stable opaque identities, while their resolved values remain available for
operators. `agent` is the explicit `--agent` value, then `AGCOORD_AGENT`, or the stable value
`unnamed` when neither is set; `caller_pid` remains the exact submitting process diagnostic and
is never used as the fallback identity. Missing times and kind-specific values are null rather
than guessed. A queued
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
receipt as a separate authorization step.

`full` is neither a lane barrier nor machine-wide exclusivity. Declare every scarce machine
resource it requires. Capacities—not the fact that the job is named “full”—decide whether
other work can overlap it.

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

## Atomic landing

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
repository barrier. Its command and environment are the gate, not a second queued job, and
its `gate_run_id` is null.

The worker first validates local and forge identity, readiness, and the current target and
source refs. If the target advanced while an open, ready, same-repository request waited, the
default GitHub adapter fetches that exact target commit and creates one ordinary `--no-ff` merge
commit in the submitted checkout. It verifies the ordered parents `(old source, target)`, pushes
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

A merge conflict is aborted before the gate, restores the exact clean source checkout, and
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
atomic mutation would leave the outcome indeterminate. Graceful broker stop cancels safe
earlier phases but waits for publishing and records its authoritative result.

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
agc list
agc list --json
agc show <run-id>
agc log <run-id> [--follow]
agc cancel <run-id>
```

The non-JSON `list` table summarizes each row as `admission-only`, `applied`, `partial`,
`unapplied`, or `failed`. `list --json` and `show` retain the complete binding, capability, and
receipt fields so automation can distinguish scheduling, actual application, and measurement.

Queued work is cancellable. Running checks and full gates, plus land preflight and gating,
receive process-group cancellation and become terminal only after every descendant is gone.
Publishing land jobs refuse cancellation as described above. Unknown IDs and terminal jobs
produce named errors rather than silently changing another row.

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

`agc tui` is a credential-free live view over the same client API. It shows active,
queued, and recent terminal jobs across repositories; kind, lane/repository identity,
declared resources, status, phase, label, timing, head, publication, gate exit, and failure details
remain inspectable without truncating the durable values. Persistent selection detail includes
the enforcement summary; the modal detail includes the exact resource contract and sanitized
receipt events.

The expected key map is:

| Key | Action |
| --- | --- |
| `r` | refresh from the durable snapshot |
| `Enter` | inspect the selected job and exact fields |
| `l` | read the selected job log |
| `c` | request cancellation; a publishing land explains why it cannot be cancelled |
| `h` | hide or restore terminal history without deleting it or rereading the spool |
| `p` | open the searchable repository picker |
| `a` | open the searchable agent picker |
| `?` | show context-sensitive help |
| `q` | quit the view; jobs continue |

Each filter picker starts in a focused search field. Typing narrows the complete snapshot's
choices, `Tab` moves into the scrollable result menu, and `Enter` applies the highlighted
choice. `All repositories` or `All agents` clears that filter directly; `Esc` closes the picker
without changing the current filter. Repository choices show their complete readable remote or
local identity rather than the internal hashed lane key.
The agent picker includes stable named identities and the single `unnamed` value. It omits
legacy `pid:<positive integer>` fallback choices from older retained history; those rows remain
visible in the unfiltered snapshot with their separate caller PID diagnostic intact.

Refresh reconciles keyed rows in place, preserving the selected run and both viewport offsets
when that row still exists without first painting a reset-to-top frame.
At 80 columns the compact table is exactly
`STATE · KIND · REPO · RUN · BRANCH · LABEL · AGE · DUR` and does not require horizontal
navigation, including when a long queue also needs its vertical scrollbar. Adjacent columns
retain a visible gutter; `LABEL` yields the small amount of width needed by that scrollbar
instead of creating a horizontal one. A value wider than its compact cell ends in a visible
ellipsis instead of running into the next column or disappearing at a hard edge. Above 80
columns, `BRANCH` consumes additional width up to its useful cap before `LABEL` expands into the
remaining space; both contract back to compact ellipsis forms when the terminal narrows.
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

Protocol migration is explicit and out of band. Normal broker or client initialization fails
closed on an older spool and names the required command; it never mutates schema on a hot
path. Production upgrades must first install a durable drain, retain its exact receipt, copy
the whole owner-locked spool, and prove the installed binary's rollback against a disposable
copy by following the [native migration runbook](native_migration.md). Once the receipt says
`drained`, run:

```bash
agc migrate
# or, for an intentionally isolated spool
agc --state-dir /path/to/state migrate
agc resume drain-0123456789ab
```

Migration leaves the maintenance marker installed, so no new owner or submission can race the
remaining operator steps. Resume with the exact retained ID only after maintenance is complete;
the next `agc list` then starts or joins a broker using the new protocol. Migration preserves only
facts represented by the old schema; it never upgrades a legacy label into an exact-head
receipt, fuses separate full and merge rows into a land, or invents a gate phase/status that
the legacy row did not record. Protocol-1 and protocol-2 resource maps migrate as generic
admission-only contracts with empty applied, peak, and event fields. Familiar legacy names are
not reinterpreted as typed or enforced resources. Protocol 3 migrates by adding the durable
child-CPU-lease catalogue; terminal run history remains unchanged and no lease is invented for
old work.

Protocol-5 migration first produces and verifies
a normalized protocol-4 rollback backup. An explicit rollback restores that baseline, replays
terminal native history, writes `invalid_gate_through_sequence`, and retains the current drain
marker rather than restoring a stale marker from the baseline. Protocol-4 merge submission then
ignores all full-gate receipts through that sequence, including an explicitly named one; run a
new exact-head full gate before any legacy merge workflow. The installed `agc migrate` selects
and verifies the configured native executable, requires no owner or live row, and preserves the
production drain across its schema transaction. Ordinary client commands refuse a live
protocol-4 owner or an older spool that has not completed this procedure; installing or building
the native artifact alone never performs the transition. The canonical runbook also defines the
0.5 compatibility matrix, whole-spool backup, capability proof, rollback, Python production-path
retirement, and safe actions for every migration refusal; this section defines only the durable
protocol semantics.
