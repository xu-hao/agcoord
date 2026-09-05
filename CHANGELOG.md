# Changelog

All notable user-facing changes to AGCoord are recorded here. Versions follow semantic
versioning; dates use ISO 8601.

## Unreleased

- Add `docs/troubleshooting.md`: how a refusal reaches you, and one table per situation for
  submissions, landing handbacks, maintenance, broker startup and the spool, the managed host,
  the enforced-host probe, and enforcement receipts, each with its meaning and next action
  (#201).
- Add `docs/concepts.md`, which explains the broker and the spool, the three kinds of job,
  lanes and the one barrier, claims, bindings, and receipts, the life of a job, and the
  maintenance words in plain sentences, each section linking to its exact rule (#203).
- Show the terminal UI in the README with `docs/assets/agcoord-tui.svg`, an SVG screenshot that
  Textual exported from the real `agc tui` application driven by a fixed snapshot, with its
  generation record in the assets README (#204).
- Point the distribution's Documentation URL and the README at the documentation site,
  https://agcoord.readthedocs.io, now that it builds there (#208).
- Fix a submitting client aborting with `cannot inspect gate queue protocol …: database is
  locked` while its admitted row kept running, so a wrapper read exit status 2 as a verdict.
  The read-only protocol inspection now waits through transient SQLite contention for the
  configured `database_timeout` instead of failing on the first busy lock; `agc run`, `full`,
  and `land` retry a transient coordinator error for five seconds while following; and a
  stream that is still lost ends with exit status 75, a message that the job continues with
  `agc log <id> --follow` and `agc show <id>`, and, under `--json`, a `coordinator-unreachable`
  object carrying the run ID, so it cannot be mistaken for a refusal or a red gate (#207).

## 0.6.3 — 2026-09-05

- Rewrite the README around the problem AGCoord solves for coding agents that share one
  machine, add a no-root quickstart (`docs/quickstart.md`) that runs the released broker as an
  unmanaged user-owned executable in a dedicated spool, regroup the documentation index by
  reader journey, and replace the unresolved `RELEASE_VERSION` placeholder in the README and
  the native host runbook with runnable commands (#185).
- Publish the documentation as a Sphinx site for Read the Docs: `.readthedocs.yaml`,
  `docs/conf.py` (MyST, Furo, warnings fatal, version read from the installed
  distribution), hidden toctrees in the documentation index, and include pages that carry the
  root README, changelog, and contributor guardrails into the site (#187).
- Add the agent integration guide (`docs/agents.md`): the paragraph to paste into `CLAUDE.md`
  or `AGENTS.md`, the three rules and why they exist, a table of every submission refusal
  and landing handback with the agent's next action, naming agents, sizing claims from
  receipts, parallel tools inside one job, sandboxed shells, and orchestrators (#189).
- Add `docs/why.md`, the case for AGCoord in depth, and `docs/comparison.md`, which places it
  next to hosted merge queues, local job queues, agent session managers, Gas Town, sandboxes,
  and local CI runners by the reader's situation (#191).
- Refuse a `run`, `full`, or `land` submission made outside a Git repository with one line that
  names the rule and the remedy (`… is not inside a Git repository; agc schedules work per
  repository and worktree, so run it from a checkout or pass --checkout PATH`), keeping Git's
  own reason after it instead of printing it alone. Describe the distribution on PyPI as a
  local CI and merge queue for coding agents that share one machine, with keywords, so the
  package can be found (#193).
- Trust a broker executable owned by the current user when its SHA-256 equals the digest
  pinned in the client, computed before the file is executed; every other selection check is
  unchanged, `allow_development` is needed only for a build from source, and a user-owned
  file from another release is refused as `native-broker-pin-mismatch` with the command to
  fetch the right one. Add `agc host install --user`, which downloads this client's standalone
  release broker and sidecar through the GitHub adapter, verifies both, places the file at
  `~/.local/libexec/agcoord/agcoord-broker`, selects it once, and writes an unmanaged
  configuration into the default or `--state-dir` spool when none exists; it needs no
  privilege, is idempotent, is the upgrade path after a client upgrade, and refuses a spool
  configured for another broker or one whose broker is live. The README and quickstart
  try-out become two commands (#195).

## 0.6.2 — 2026-09-03

- Fix a tmpfs-backed `check` or `full` run whose command exited 0 being recorded as failed with
  exit status 125 and no `failure_reason`. The native launcher that supervises a tmpfs-backed
  command tried to unmount the scratch after the command finished, which the kernel refuses once
  the launcher's capabilities are dropped, and it reported that refusal in place of the command's
  status; `land` rows hid the defect because the broker prefers the land worker's own reported
  status. The launcher now relays the command's exit status or termination signal unchanged and
  leaves the private mount namespace to the kernel's teardown (#181).

## 0.6.1 — 2026-09-03

- Let a `tmpfs` binding opt in to executable scratch with `"exec": true` in the broker's
  configuration: the run's tmpfs is then mounted `nosuid,nodev` without `noexec` and verified as
  such, so jobs that execute what they write under `TMPDIR` — command doubles, throwaway virtual
  environments, downloaded plugin executables — can run on accounted scratch. The default and
  every existing configuration stay `noexec`; `exec` on any other kind or a non-boolean value is
  refused at configuration load, and stored contracts carry the flag only when set (#177).

## 0.6.0 — 2026-09-03

- **Breaking:** retire the Python reference broker. `CoordinatorBroker`, the hidden `serve`
  entry point, protocol-4 owner discovery, the legacy drain/resume/maintenance helpers,
  `migrate_queue`, `agc migrate`, and the Python cgroup, project-quota, and worker enforcement
  backends are removed; the wheel's only broker is the native executable it installs. A client
  that meets a coordinator spool below protocol 5 refuses every command before starting or
  claiming a broker and names AGCoord 0.5.2 as the release that migrates it. Conformance
  manifest v3 points its migration behaviour at the native broker's own legacy-synthesis tests,
  and the release verifier no longer runs a Python migration rehearsal. Final stage of
  retiring the Python broker (#165).
- Fix `--repository` against the native broker: an explicit repository identity is now hashed
  into its `repository_id` like a discovered one, so names such as `example/widgets` are no
  longer refused as invalid identifiers (#168).
- Fix `agc land --no-target-sync` and per-request `agc land --avoid` on the native broker. The
  broker removed every coordinator-reserved environment name before launching the land worker,
  so both options were silently ignored; it now passes them to the worker as `--no-target-sync`
  and `--avoid` arguments and refuses an invalid setting at submission (#169).
- Fix failed lands on the native broker recording no `failure_reason`. A red gate now records
  `gate-failed` and every other handback its documented stable code (`stale-main`,
  `head-changed`, `pr-not-ready`, `publish-failed`, `merge-error`, `avoided-commit`), as the
  coordinator guide describes (#170).
- The Python test suite drives a test-owned native broker. `tests/conftest.py` builds the
  development `agcoord-broker` on first use (or takes `AGCOORD_TEST_NATIVE_BROKER`), and
  client-boundary tests run against that protocol-5 owner instead of the in-process protocol-4
  reference; `scripts/check-conformance` builds the broker before the Python suite. Second
  stage of retiring the Python broker (#165).

- Make conformance single-implementation. Manifest version 3 names native test selectors only,
  drops the Python-reference owner-eligibility difference, and `scripts/check-conformance`
  validates and collects one implementation while still running the complete Python and Rust
  suites. The protocol-4 Python broker is no longer a conformance oracle; this is the first stage
  of retiring it (#165).

- Let a `full` or `land` worker that declared a tmpfs or project-quota scratch policy verify its
  admission. Under such a policy both brokers start the command as the direct child of a
  launcher and record the launcher as the worker, so the command's own PID never matched;
  admission verification and land phase reporting now accept the live direct child of the live
  recorded launcher, proven by its start identity. Rows without a scratch policy are unchanged.

## 0.5.2 — 2026-09-03

- Add `agc verify-admission` as a public subcommand with the same arguments and exit codes as the
  hidden `python -m agcoord.queue verify-admission` entry point, so an admitted worker whose own
  interpreter has no `agcoord` installed can prove its exact admission through the machine's `agc`.
  The module entry point keeps working for the transition.

## 0.5.1 — 2026-09-03

- Create every downloaded native-host asset and cache directory owner-only regardless of the
  invoking umask. On accounts with a permissive umask, `agc host install --download` and
  `agc host upgrade --download` wrote group-writable assets and then refused the bundle they had
  just fetched; the downloader now sets explicit modes, and an intact cache left in that state
  by an earlier client is repaired on reuse instead of refused.

## 0.5.0 — 2026-09-03

- Move the client's supported native broker line to 0.5.x. A 0.5.0 Python client selects and
  commands only a 0.5.x broker for ordinary work and refuses a retained 0.4.x executable; the
  host upgrade's maintenance drain still admits the outgoing 0.4.x broker.
- Make `land` the only repository-lane barrier and scope it to its worktree. A `full` keeps its
  clean exact-head submission requirement and receipt but is admitted like a check, and a running
  or earlier-queued land now excludes only other lands in its repository and jobs submitted from
  its own worktree; checks and fulls from other worktrees of the same repository overlap it when
  their declared resources fit. Within one lane, a queued job that is blocked by a land, by its
  worktree, or by capacity lets later admissible lane work pass it, so lane work packs by declared
  resources the way unrelated repositories already did. Both brokers apply the same rule, and the
  conformance manifest pairs the new lane-packing and worktree-scoped-barrier tests.
- Stop refusing a landing as `head-changed` when the forge briefly still reports the head that
  `agc land`'s own target-sync push replaced. The repeated preflight now treats exactly that head
  as read-after-write lag and re-reads for a bounded wait (every 2 seconds, at most 30 seconds)
  before deciding; any other head, or the replaced head outliving the wait, is still refused.
- Accept `--broker-sha256` on `agc host install` and `agc host upgrade`, so a client that ships
  no native-host pin can still download and install a bundle it can actually check, and so an
  operator can demand a digest comparison on a bundle path that an unpinned client would
  otherwise skip. A supplied digest never weakens a released client: one that disagrees with the
  shipped pin is a refusal rather than an override.
- Add `agc avoid SHA [--reason TEXT]`, with `--list` and `--remove SHA`, storing commits that no
  landing on this machine may publish again. Every `agc land` now refuses before any push when a
  stored commit — or one passed with `--avoid SHA` for that landing — is reachable from the
  request head, from the current target, or from the head that target synchronization would
  push, and checks the target once more before publishing. The set lives in an owner-only
  `avoid.json` beside `config.json`, needs no broker, survives `agc clear` and broker restarts,
  and travels with the state directory through migration and rollback. The refusal code is
  `avoided-commit`; the recovery is a fresh request branch from the current target.

- Decide every shared caller-side refusal before starting a broker. `agc run`, `agc full`, and
  the land and merge submissions now settle repository discovery, the exact clean head, the
  caller PID, and the no-nesting rule before selection or autostart, so a refused submission
  can no longer leave a new owner and queue behind in the target state directory. Only the
  owner's declared capacities are checked after a broker exists, and a dirty checkout is still
  refused before a nested submission is.
- Fetch the matching native-host bundle with `agc host install --download` and
  `agc host upgrade --download` instead of requiring an operator to assemble the eight release
  files by hand. The download is an optional GitHub adapter; the coordinator core still accepts
  only a prepared bundle directory, and an explicit bundle path keeps working unchanged for
  hosts without network access.
- Refuse a native-host package whose broker is not the one this client release was built
  against. Clients now ship a checked-in digest of the reproducible broker, compare it against
  the bytes the package actually carries, and refuse to download at all when a client carries no
  pin. A bundle's own manifest and `.sha256` sidecars travel with the files they describe, so
  they cannot establish which release a download is; the shipped pin arrives with the Python
  distribution instead. The release gate refuses to publish a version whose pin is missing or
  does not match the released broker.
- Stop reporting a completed `agc host install` or `agc host upgrade` as a failure. Starting the
  managed service and owning the state directory are separate events, so verification now waits
  for the restarted broker to own the spool before submitting the enforcement proof, and still
  fails closed with the exact drain ID and service state if ownership never appears.

## 0.4.1 — 2026-09-02

- Let one `agc host upgrade` cross a minor release boundary. The upgrade's drain now selects the
  broker that is actually installed, which is still on the outgoing release line until activation,
  instead of refusing it against the client's own supported line. Ordinary client commands keep
  the strict release-line policy, and every other selection boundary is unchanged.

## 0.4.0 — 2026-09-02

- Move the client's supported native broker line to 0.4.x. A 0.4.0 Python client selects and
  commands only a 0.4.x broker and refuses a retained 0.3.x executable, matching the release
  compatibility matrix.
- Stop granting implicit unbounded scratch to jobs that declare no scratch resources. The Python
  and native brokers now remove inherited `TMPDIR`, `TMP`, and `TEMP` values and create no per-run
  scratch directory unless the job requests a complete tmpfs or project-quota policy. A
  best-effort scratch setup failure likewise continues without managed scratch instead of
  falling back to an unbounded disk directory.
- Add `agc host install` and `agc host upgrade` as verified one-command native-host operations
  after the exact matching Python client is installed. They validate the complete release bundle,
  enforce the managed default-spool boundary, orchestrate fresh activation or exact-ID draining,
  verify the restarted identity, and retain an enforced CPU proof. Fresh managed installs create
  CPU and job capacity from the process's available affinity so the existing greedy scheduler can
  pack declared requests without a fixed two-way partition.

## 0.3.2 — 2026-09-02

- Allow the installed Python client to perform only its authenticated run-scoped callbacks from
  a managed admitted user namespace, where Linux represents the fixed host-root-owned broker
  with the overflow UID. Selection still rejects arbitrary overflow-owned paths and requires
  exact namespace maps plus the restricted AppArmor preflight; general nested client operations
  retain the ordinary root-owner policy. Publication workers now preserve a virtual environment's
  Python entry-point instead of resolving it to a potentially stale base installation.
- Normalize host-package creation modes and preserve archived modes during verification, so
  package bytes are independent of the invoking umask and the atomic release gate validates the
  artifact correctly under a restrictive worker umask.

## 0.3.1 — 2026-09-02

- Add durable `agc drain` and exact-token `agc resume` maintenance operations. Drain atomically
  refuses new submissions while admitted checks and authoritative lands finish, survives broker
  replacement and protocol migration/rollback, remains visible through JSON and the TUI, and is
  now required—with its exact receipt—before replacing host files for an existing spool.

## 0.3.0 — 2026-09-01

- Make `land` mechanically merge an advanced target into an unchanged same-repository request
  branch before gating by default. The lease-protected source update becomes the row's durable
  exact head before the gate; conflicts and source races fail closed, and
  `--no-target-sync` retains the explicit stale-target refusal.
- Add a tested native migration and rollback runbook with a compatibility matrix, whole-spool
  backup rehearsal, mixed-owner refusals, capability verification, retirement criteria, and
  stable troubleshooting actions. A clean-install release verifier now binds the Python, native,
  and host artifacts to one versioned manifest and aggregate checksum set before publication.
- Add the verified native host bundle, long-lived delegated systemd user service, and
  three-domain enforcing AppArmor boundary without `default_allow`/unconfined policy. Staged
  installation and upgrade refuse live work and never
  restart implicitly; managed startup verifies the fixed root-owned release identity, service
  cgroup, controllers, global namespace restriction, and setup profile, while admitted broker
  calls and arbitrary submitted commands remain namespace-restricted. A real-host probe proves
  an enforced CPU receipt without weakening Ubuntu's global policy and retains broker and kernel
  diagnostics, including the failing cgroup2 mount's Linux error number, when namespace setup
  fails. Namespace isolation falls back to a leaf-only bind mount when Linux reports that the
  namespace-rooted cgroup2 view collides with the inherited mountpoint, and native metric parsing
  accepts dotted cgroup fields emitted by Linux 6.17 and later. The trusted launcher enters the
  admitted AppArmor profile before setting `no_new_privs`; later broker-client execution stacks
  another restricted profile instead of requesting a forbidden replacement domain.
- Retry native broker startup through a bounded transient ownership-lock collision while
  preserving the stable refusal for a genuinely live owner.
- Publish a native broker's live owner identity only after its termination handlers are ready,
  and require crash-recovery startup checks to observe the exact replacement PID rather than
  accepting stale metadata retained in the locked ownership file.
- Add a strict version-1 Python/native conformance manifest and executable release gate covering
  commands, repositories, atomic publication, the real TUI, protocols, resources and receipts,
  migrations, contention, cancellation, crash recovery, malformed-input corpora, and the core
  no-duplicate/no-stale/no-unrelated-kill/no-unverified-enforcement safety properties.
- Route the Python CLI, client, TUI, atomic publication worker, and pytest-xdist child leases
  through the protocol-5 Rust broker. Native selection now verifies one explicit executable and
  exact live-owner identity, concurrent autostart elects one owner, protocol-4 state requires an
  explicit idle migration, and missing, stale, or incompatible binaries fail without a Python
  fallback.
- Bring the Rust broker's persistent project-quota scratch backend to parity: resolve supported
  ext4 and XFS mounts, allocate collision-safe project identities, apply and verify byte and inode
  ceilings, isolate quota authority from workers, retain terminal usage, and recover or clean up
  only identity-verified state across completion, cancellation, and broker crashes.
- Bring the Rust broker's cgroup v2 backend to enforcement and receipt parity: verify delegated
  namespace rooting, own collision-safe leaves, apply and measure CPU, process, memory, pressure,
  swap, tmpfs, bandwidth, IOPS, and I/O-weight controls, retain terminal and recovery evidence,
  support required refusal or best-effort tmpfs disk fallback, and reject malformed durable
  resource contracts, receipts, state, or recovery handles without rewriting them.
- Replace the native broker's direct command spawn with an authenticated in-process launcher:
  user code remains blocked through durable PID/start-token/process-group ownership, verified
  capability and descriptor cleanup, and a token-bound final release. Cancellation now drains
  the complete owned process group, while recovery refuses conflicting or reused identities
  without signaling unrelated processes.
- Add the protocol-5 Rust scheduler and durable SQLite owner with repository barriers and fair
  lanes, capacity admission, cancellation and atomic land authority, WAL contention handling,
  graceful shutdown, identity-safe crash adoption, stable corruption refusals, and verified
  protocol-1-through-4 migration. Native rollback restores its protocol-4 baseline, preserves
  terminal native history, and prevents reuse of every pre-rollback gate receipt.
- Add a pinned Rust workspace and reproducible static x86_64 Linux broker artifact with bundled
  SQLite, exact protocol/build identity, checksums, provenance, dependency inventory, and
  automated ELF and clean-copy reproducibility audits.
- Define the target single-executable Rust broker architecture, including its trust boundary,
  durable protocol 5, native worker handshake, AppArmor transition, explicit protocol-4
  migration, Python client compatibility, and ordered implementation constraints.
- Document that Ubuntu's unprivileged user-namespace restriction can make an explicitly
  delegated cgroup v2 backend fail with `namespace-mapping-failed`, and that granting `userns`
  permission to a copied Python interpreter or root-owned virtual environment is not a narrow
  broker-specific workaround.

## 0.2.2 — 2026-08-31

- Preserve every identity-verified live worker when a broker exits unexpectedly, allowing a
  replacement owner to adopt it instead of recording an unrequested cancellation; explicit
  broker shutdown remains a graceful cancellation boundary.
- Put current queue databases in WAL mode, retry transient SQLite contention in the pump and
  idle health check, defer contended activity heartbeats without masking successful operations,
  and add a positive `database_timeout` setting (10 seconds by default).

## 0.2.1 — 2026-08-31

- Use one stable `unnamed` identity when neither `--agent` nor `AGCOORD_AGENT` is set, retain
  the exact caller PID separately, and omit legacy per-PID fallbacks from the TUI agent picker.
- Keep the TUI's vertical viewport stable throughout periodic refreshes instead of briefly
  snapping to the top while rows are rebuilt and then restoring the previous position.
- Replace repository and agent filter cycling in the TUI with searchable picker menus that
  provide explicit all-values choices and preserve the current filter when dismissed.
- Show each run's branch in a dedicated TUI table column, prioritizing branch and then label
  width on larger terminals while retaining the no-horizontal-scrollbar 80-column layout.
- Require the supported Textual 8 release line (`textual>=8.2,<9`) instead of maintaining
  compatibility with Textual 1 through 7 or accepting an unvalidated future major.

## 0.2.0 — 2026-08-30

- Rename the installed console command from `agcoord` to `agc` while retaining `agcoord` as
  the PyPI project, import package, module entry point, state namespace, and protocol identity.
- Add typed resource bindings, explicit admission-only/best-effort/required modes, sanitized
  backend capability probes, and durable requested/applied/peak/event receipts. Protocol-1 and
  protocol-2 history migrates without reinterpreting legacy resource names as enforced limits.
- Add authenticated child CPU leases so concurrent tools inside one admitted run can fairly
  divide its declared CPU budget with exact or partial grants, cancellation, crash reclamation,
  and broker recovery without nested jobs or extra history rows. Protocol 3 requires an explicit
  migration that preserves terminal history while adding the durable lease catalogue.
- Add an optional `agcoord[xdist]` pytest plugin that sizes `-n auto`, `-n logical`, and explicit
  positive worker counts from the admitted run's child CPU budget. Plain pytest and `-n 0` stay
  serial, outside-run behavior stays upstream, workers never lease recursively, and controller
  crash or cancellation returns the complete lease.
- Add a rootless Linux cgroup v2 lifecycle backend for an explicit delegated subtree. It probes
  namespace-safe delegation, attaches the blocked launcher before user code, contains later
  descendants behind an `nsdelegate` boundary, kills the full tree, and recovers or cleans only
  identity-verified run leaves across broker restarts, with explicit required and best-effort
  fallback.
- Enforce typed CPU allocations as aggregate `cpu.max` bandwidth with a fixed 100ms period and
  typed process allocations as subtree-wide `pids.max` task limits. Preserve CPU weight and
  affinity as separate policies, measure conservative CPU and PID peaks, and retain deduplicated
  throttle or PID-exhaustion events through normal completion and cancellation.
- Enforce hard memory, pressure, and swap byte budgets with cgroup v2; default hard-memory runs
  to zero swap, terminate local OOMs as one process group, preserve final memory/swap peaks and
  pressure or limit evidence, and expose `memory-oom` without overriding cancellation.
- Provision optional per-run tmpfs scratch with byte and inode ceilings, safe private mount
  options, memory-cgroup accounting, a pre-exec setup handshake, best-effort directory fallback,
  durable usage/limit evidence, and namespace-owned teardown across completion or cancellation.
- Provision optional persistent scratch trees on explicitly project-quota-enabled ext4 or XFS
  filesystems, with atomic byte/inode limits, collision-safe project identities, terminal usage,
  capability-stripped workers, crash recovery, and required or best-effort fallback semantics.
- Enforce typed symmetric or directional per-device bandwidth and IOPS ceilings plus optional
  I/O weights for safely resolved scratch devices, with verified controller values, durable
  device identity, interval-rate peaks, terminal sampling, and fail-closed storage-stack checks.
- Keep the default-width TUI free of an unnecessary horizontal scrollbar when long queues
  require vertical scrolling, without removing the visible gutters between columns.
- Configure every broker from one `config.json` in its state directory, holding `capacities`,
  `bindings`, `cgroup_root`, and optional `cgroup_io` paths. **Breaking:**
  `AGCOORD_CAPACITIES`, `AGCOORD_RESOURCE_BINDINGS`, and `AGCOORD_CGROUP_ROOT`, and the
  comma-separated `name=units` capacity syntax, are removed with no deprecation period; move
  those values into the file. `AGCOORD_STATE_DIR` is unchanged and now selects which
  configuration file a client and broker share.

## 0.1.1 — 2026-08-30

- Replace the cartoon character with a naturalist botanical gourd while retaining
  the stable mascot URL, transparent canvas, and compact README presentation.
- Separate compact TUI columns with visible gutters and mark every clipped value with an
  ellipsis while retaining the complete value in the detail view.
- Expand the TUI `LABEL` column into space available beyond 80 terminal columns and restore
  its compact ellipsis form when the terminal narrows.
- Show compact human-readable remote or local repository names in the TUI table and full
  readable identities in its filter header while retaining hashes as internal lane keys.

## 0.1.0 — 2026-08-30

- Publish the standalone `agcoord` distribution, import package, module entry point, and
  console command for Python 3.10 and newer.
- Coordinate multiple agents, Git worktrees, and repositories through one detached,
  user-scoped broker with durable job IDs, logs, resource capacities, repository barriers,
  cancellation, history clearing, recovery, and private scratch reclamation.
- Add `land`, one durable repository-barrier request that preflights an exact clean head,
  runs its captured gate, and immediately publishes a green result without releasing its
  lane or resources; red gates and stale target/head observations publish nothing.
- Expose land phases, gate exit status, one combined transcript, safe cancellation
  boundaries, and crash recovery that never reruns a gate. Keep `full` as a standalone
  exact-head validation command instead of a full-plus-publication landing sequence.
- Provide GitHub metadata and exact ref-publication adapters without making `gh` a core
  import dependency.
- Add `run`, `full`, `land`, `list`, `show`, `log`, `cancel`, `tui`, `migrate`, and `clear`
  workflows, including a compact live multi-repository terminal view.
- Give admitted workers immutable run ID, exact kind, and resolved state-directory context
  so internal repository gate wrappers can verify the correct full or land admission even
  with an explicit state directory, without exposing environment values or credentials in
  public job rows.
- Build, audit, and clean-install the standalone wheel and source archive before publishing
  those exact artifacts directly to production PyPI.
