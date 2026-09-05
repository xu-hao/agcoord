# Documentation index

## Start

- [README](../README.md) — what AGCoord is for, the two-minute try-out, the enforced host,
  and the command reference.
- [Quickstart](quickstart.md) — the no-root try-out step by step: install, first job,
  watching jobs queue, cleanup, enforcement, and a first landing.
- [Changelog](../CHANGELOG.md) — published user-facing changes by version.

## Operate and reference

These are the canonical contracts. Every guarantee the Start pages mention is defined here.

- [Coordinator contract and operations](coordinator.md) — machine-local topology,
  resource scheduling, repository lanes, atomic landing, cleanup, recovery, CLI, and
  TUI.
- [Native broker architecture](native_broker.md) — single-executable scope, trust model,
  durable protocol, worker boundary, compatibility, and migration requirements.
- [Native host deployment](native_host.md) — supported host, package verification, managed
  service, AppArmor boundary, enforced-host proof, upgrades, recovery, and rollback.
- [Conformance](conformance.md) — the versioned native behavior contract, failure-injection
  coverage, execution-mode opt-ins, and the executable release gate.
- [Migrating a pre-native spool](native_migration.md) — how a spool left below protocol 5 by a
  pre-0.6.0 release is refused and migrated through AGCoord 0.5.2.

## Project

- [AGENTS.md](../AGENTS.md) — repository-wide contributor guardrails.
- [Contributor workflow](contributing.md) — issue tracking, isolated worktrees,
  behavioral validation, coordinated checks, atomic landing, and cleanup.
- [Packaging and release](releasing.md) — PyPI identity, build validation, direct manual
  publication, optional adapters, and release posture.
- [Session handoff format](session_handoff_format.md) — closing changed work with concise
  status, open work, genuine decisions, and recommended options.
- [Asset sources](assets/README.md) — canonical location and provenance requirements for
  the AGCoord gourd mascot.
