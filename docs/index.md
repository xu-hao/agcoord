# Documentation index

Start with [the root README](../README.md) for installation and the common workflow.
See [the changelog](../CHANGELOG.md) for published user-facing changes by version.
[AGENTS.md](../AGENTS.md) contains repository-wide contributor guardrails; the detailed
procedure is in [the contributor workflow](contributing.md).

## Canonical contracts

- [Contributor workflow](contributing.md) — issue tracking, isolated worktrees,
  behavioral validation, coordinated checks, atomic landing, and cleanup.
- [Coordinator contract and operations](coordinator.md) — machine-local topology,
  resource scheduling, repository lanes, atomic landing, cleanup, recovery, CLI, and
  TUI.
- [Native broker architecture](native_broker.md) — single-executable scope, trust model,
  durable protocol, worker boundary, compatibility, and migration requirements.
- [Native host deployment](native_host.md) — supported host, package verification, managed
  service, AppArmor boundary, enforced-host proof, upgrades, recovery, and rollback.
- [Cross-implementation conformance](conformance.md) — versioned Python/native behavior,
  failure-injection coverage, intentional differences, and the executable release gate.
- [Packaging and release](releasing.md) — PyPI identity, build validation, direct manual
  publication, optional adapters, and release posture.
- [Session handoff format](session_handoff_format.md) — closing changed work with concise
  status, open work, genuine decisions, and recommended options.

## Assets

- [Asset sources](assets/README.md) — canonical location and provenance requirements for
  the AGCoord gourd mascot.
