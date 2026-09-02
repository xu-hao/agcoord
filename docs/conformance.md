# Cross-implementation conformance

AGCoord keeps the protocol-4 Python broker as an executable reference oracle while the
protocol-5 Rust broker is the only production owner selected by installed clients. The strict
versioned contract in `conformance/manifest-v1.json` pairs public black-box tests for those two
implementations. A release is not conformant merely because both test suites happen to pass:
every required domain and every intentional difference must also remain named in the manifest
and backed by collected tests.

## Version 1 contract

Version 1 requires paired coverage for public commands, repository lanes, atomic publication,
the real TUI, protocol handling, typed resources and receipts, migrations, contention,
cancellation, and replacement recovery. It separately names crash boundaries around database
transactions, launcher release, cgroup attachment, publication authority, terminal cleanup,
and replacement ownership. Its client and stored-state corpora cover malformed values, and its
safety properties prove no duplicate execution, stale publication, unrelated process kill, or
unverified enforcement claim.

Contention coverage includes durable maintenance draining: competing submissions linearize
entirely before or after the guard, accepted work and authoritative publication complete, a
crashed owner is replaced only to recover live work, migration preserves the marker, host
activation requires its exact ID, and only exact-ID resume reopens submissions.

Each behavior contains one or more pytest node IDs for the Python reference and Rust integration
test selectors for the native owner. Selectors exercise clients, commands, repositories,
subprocesses, the Textual application, and test-owned kernel seams; documentation or source-text
assertions are not conformance evidence. A selector that is renamed or no longer collected makes
the gate fail before either complete suite runs.

The manifest also records two intentional differences:

- the Python owner remains a protocol-4 conformance fixture and is never an automatic production
  fallback, while the Rust executable publishes the audited protocol-5 release identity;
- ordinary tests use deterministic, test-owned cgroup and project-quota seams, while real kernel
  enforcement remains an explicit dedicated-host proof.

Changing a required domain, implementation identity, or execution contract requires a new
manifest version. Adding behavior within version 1 requires paired public tests and a manifest
entry in the same change.

## Executable gate

Run the canonical checker from a dependency-complete checkout:

```bash
./scripts/check-conformance --validate-only
./scripts/check-conformance --coverage-only
./scripts/check-conformance
```

Validation is a fast strict JSON/schema check. Coverage mode asks pytest and Cargo to collect the
named tests and refuses any missing selector. The default mode then runs the complete Python and
Rust workspace suites; CI and the tag workflow use this mode, so a native artifact cannot be
released after deleting or renaming declared coverage.

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
