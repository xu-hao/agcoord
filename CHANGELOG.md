# Changelog

All notable user-facing changes to AGCoord are recorded here. Versions follow semantic
versioning; dates use ISO 8601.

## Unreleased

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
