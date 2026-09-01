# Changelog

All notable user-facing changes to AGCoord are recorded here. Versions follow semantic
versioning; dates use ISO 8601.

## Unreleased

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
