# Conformance

The protocol-5 Rust broker is AGCoord's only implementation. The strict versioned contract in
`conformance/manifest-v3.json` names the public black-box tests that prove its required
behavior. A release is not conformant merely because the test suites happen to pass: every
required domain must remain named in the manifest and backed by collected tests, and a selector
that is renamed or no longer collected fails the gate before any suite runs.

## Version 3 contract

Version 3 keeps the version-2 domains for a single implementation: public commands, repository
lanes, atomic publication, the real TUI, protocol handling, typed resources and receipts,
migrations, contention, cancellation, and replacement recovery. It separately names crash
boundaries around database transactions, launcher release, cgroup attachment, publication
authority, terminal cleanup, and replacement ownership. Its client and stored-state corpora cover
malformed values, and its safety properties prove no duplicate execution, stale publication,
unrelated process kill, or unverified enforcement claim.

Resource coverage proves the zero-scratch default at the worker boundary — the broker removes
caller temporary-directory variables and creates no managed run scratch path unless a provider is
declared — and that a worker under a declared scratch policy still verifies its admission.

Contention coverage includes durable maintenance draining: competing submissions linearize
entirely before or after the guard, accepted work and authoritative publication complete, a
crashed owner is replaced only to recover live work, migration preserves the marker, host
activation requires its exact ID, and only exact-ID resume reopens submissions.

Each behavior lists one or more Rust integration test selectors. Selectors exercise clients,
commands, repositories, subprocesses, the Textual application, and test-owned kernel seams;
documentation or source-text assertions are not conformance evidence.

The manifest records one intentional difference in execution mode: ordinary tests use
deterministic, test-owned cgroup and project-quota seams, while real kernel enforcement remains
an explicit dedicated-host proof.

Version 3 replaced the paired Python-reference selectors of version 2. The protocol-4 Python
broker is no longer a conformance oracle; the Python package is the client, and its own suite
still runs in full as part of the gate. Changing a required domain, the implementation identity,
or the execution contract requires a new manifest version. Adding behavior within version 3
requires a public native test and a manifest entry in the same change.

## Executable gate

Run the canonical checker from a dependency-complete checkout:

```bash
./scripts/check-conformance --validate-only
./scripts/check-conformance --coverage-only
./scripts/check-conformance
```

Validation is a fast strict JSON/schema check. Coverage mode asks Cargo to list the named native
tests and refuses any missing selector. The default mode builds the development broker with the
declared Rust build jobs, then runs the complete Python and Rust workspace suites; CI and the tag
workflow use this mode, so a native artifact cannot be released after deleting or renaming
declared coverage.

The process-lifecycle suites are deliberately serial: pytest uses one worker and the Rust test
harness uses one test thread. Their individual scenarios still create real concurrent clients,
workers, controllers, and crash replacements, but serial outer execution preserves exact process
and temporary-state ownership. Rust compilation may use four jobs because it completes before
the serial test harness begins. When the checker is the command of an `agc land` request, declare
`cpu=4`; it runs the Python and Cargo controllers sequentially and never submits nested work.

Real cgroup, I/O, tmpfs, and project-quota opt-ins remain governed by the dedicated-host commands
in [the contributor workflow](contributing.md). The native host package adds its separate
AppArmor/systemd enforcement receipt; deterministic conformance does not claim that a host
resource was applied unless the corresponding real-host probe succeeded.
