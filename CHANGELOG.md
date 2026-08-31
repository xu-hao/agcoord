# Changelog

All notable user-facing changes to AGCoord are recorded here. Versions follow semantic
versioning; dates use ISO 8601.

## Unreleased

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
- Keep the default-width TUI free of an unnecessary horizontal scrollbar when long queues
  require vertical scrolling, without removing the visible gutters between columns.
- Configure every broker from one `config.json` in its state directory, holding `capacities`,
  `bindings`, and `cgroup_root`. **Breaking:** `AGCOORD_CAPACITIES`, `AGCOORD_RESOURCE_BINDINGS`,
  and `AGCOORD_CGROUP_ROOT`, and the comma-separated `name=units` capacity syntax, are removed
  with no deprecation period; move those values into the file. `AGCOORD_STATE_DIR` is unchanged
  and now selects which configuration file a client and broker share.

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
