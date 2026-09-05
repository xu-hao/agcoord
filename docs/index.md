# Documentation index

## Start

- [Overview](overview.md) — the README: what AGCoord is for, the two-minute try-out, the
  enforced host, and the command reference.
- [Why AGCoord exists](why.md) — the problem coding agents create when they share one
  machine, why the gate and the merge are one step, and what AGCoord deliberately does not do.
- [Quickstart](quickstart.md) — the no-root try-out step by step: install, first job,
  watching jobs queue, cleanup, enforcement, and a first landing.
- [Changelog](changelog.md) — published user-facing changes by version.

## Integrate

- [Using AGCoord from coding agents](agents.md) — the paragraph to paste into `CLAUDE.md`
  or `AGENTS.md`, the rules behind it, every refusal and handback with the next action,
  sizing claims, parallel tools, sandboxed shells, and orchestrators.
- [Where AGCoord sits](comparison.md) — hosted merge queues, local job queues, session
  managers, Gas Town in depth, sandboxes, and local CI runners, by the reader's situation.

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

- [Contributor guardrails](guardrails.md) — the repository-wide rules in `AGENTS.md`.
- [Contributor workflow](contributing.md) — issue tracking, isolated worktrees,
  behavioral validation, coordinated checks, atomic landing, and cleanup.
- [Packaging and release](releasing.md) — PyPI identity, build validation, direct manual
  publication, optional adapters, and release posture.
- [Session handoff format](session_handoff_format.md) — closing changed work with concise
  status, open work, genuine decisions, and recommended options.
- [Asset sources](assets/README.md) — canonical location and provenance requirements for
  the AGCoord gourd mascot.

```{toctree}
:hidden:
:caption: Start

overview
why
quickstart
changelog
```

```{toctree}
:hidden:
:caption: Integrate

agents
comparison
```

```{toctree}
:hidden:
:caption: Operate and reference

coordinator
native_broker
native_host
conformance
native_migration
```

```{toctree}
:hidden:
:caption: Project

guardrails
contributing
releasing
session_handoff_format
assets/README
```
