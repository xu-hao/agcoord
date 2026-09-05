# Packaging and release

AGCoord is a standalone Python distribution. Its PyPI project, installed distribution,
import package, and module entry point are named `agcoord`; its installed console command is
`agc`:

```text
pip install agcoord
import agcoord
python -m agcoord --help
agc --help
```

The source uses the `src/agcoord/` layout and tests live in `tests/`. Runtime code must not
import a parent application or assume a sibling source checkout. A wheel must work in a
clean environment with no project checkout on `PYTHONPATH`.

## Native broker artifact

The native broker is a separate Linux release artifact, not a copied Python interpreter or a
file silently injected into a universal Python wheel. The currently supported release matrix
has one target: `x86_64-unknown-linux-musl`. Other architectures remain unsupported until they
have a native CI runner and the same kernel conformance coverage.

Build inputs are pinned by `rust-toolchain.toml`, `Cargo.lock`, exact direct dependency
requirements, and the source digest produced by `scripts/native-source-id`. The artifact embeds
that digest as its build identity and reports protocol, implementation, target, and bundled
SQLite version through `identity --json`. Install Rust 1.94.1 with the declared musl target and
Ubuntu's `musl-tools`, then run:

```bash
./scripts/build-native-broker
./scripts/audit-native-broker \
  dist/native/agcoord-broker-x86_64-unknown-linux-musl
./scripts/check-native-licenses \
  dist/native/agcoord-broker-x86_64-unknown-linux-musl
./scripts/check-native-reproducible
./scripts/build-native-host-package \
  dist/native/agcoord-broker-x86_64-unknown-linux-musl
```

The build emits the executable, a SHA-256 sidecar, and JSON provenance recording the artifact
and source digests, Rust and Cargo versions, C compiler, target, protocol, and source epoch. The
audit requires a static or static-PIE ELF, no program interpreter, no `DT_NEEDED` entry, a valid
checksum, and an exact release identity from an empty environment. Bundled SQLite is part of
that ELF. The dependency graph must exactly match `native/THIRD_PARTY_LICENSES.tsv`, use only
the crates.io registry or workspace source, and contain only the reviewed license expressions.

Reproducibility CI builds two fresh checkout copies with separate Cargo target directories,
path remapping, disabled incremental compilation, a fixed source epoch, and the same recorded
toolchains; their executable bytes must match. This proves reproducibility for the declared
runner and compiler inputs. It does not claim byte identity across different C compilers or
host toolchain builds. `AGCOORD_MUSL_CC=cc` is an explicit local compatibility escape hatch for
an audited x86_64 compiler when `musl-gcc` cannot be installed; release CI never uses it.

The host-package build emits a deterministic root-owned tar archive, package checker, staged
installer, enforced-host probe, and their SHA-256 sidecars under `dist/host/`. CI retains these
together with the raw native executable. The archive's manifest binds the executable identity,
service unit, and AppArmor policy; see the [native host runbook](native_host.md) for supported
hosts, activation, and rollback.

The client and native broker ship at the same stable version, the native artifact and host
manifest must report the same protocol-5 identity, and the running owner must match the exact
configured build. The native broker is the only broker AGCoord ships.

Building the artifact does not activate it. Host packaging installs the verified executable at
`/usr/libexec/agcoord/agcoord-broker`, and a development configuration may explicitly select
another absolute path with `native_broker.allow_development=true`. Python clients verify the
regular-file mode and ownership plus the exact `identity --json` version, protocol,
implementation, build, and target before startup and commands. They never search `PATH`, accept
a symlink, or silently fall back to an unpinned broker. The Python wheel deliberately contains
no native executable.

## Dependency posture

Queue ownership, durable state, scheduling, process supervision, and the forge-neutral
atomic-landing protocol belong to the core distribution. Forge credentials and SDKs do not.
GitHub is an optional adapter and `gh` is not required to import or use the core coordinator.
Textual is a base dependency because the installed `agc tui` command is part of the core
interface. The supported framework line is `textual>=8.2,<9`; older releases have no
compatibility promise, and a new Textual major requires explicit real-TUI validation before the
upper bound moves. Whichever install extras the package declares are also part of the published
interface and must be smoke-tested from the built wheel.

## Release checklist

1. Start from one clean release commit. Set the same stable version in
   `src/agcoord/__init__.py`, the Cargo workspace, and `Cargo.lock`; development suffixes are
   release refusals. Add a dated matching section to [the changelog](changelog.md). Record the
   release's broker digest in `src/agcoord/native_host_pin.json` in that same commit; step 5
   explains where the digest comes from and step 6 refuses a candidate without it.
2. Land that exact commit through `agc land` with `./scripts/check-conformance`. The checker
   validates the version-3 manifest and collected native selectors before running both
   complete suites serially at their process boundary. It includes generic scheduling, atomic
   publication, real TUI behavior, resources/receipts, migration/rollback, malformed
   state, contention, cancellation, and crash-recovery safety properties. Missing declared
   coverage closes the gate.
3. On every host, run the deterministic cgroup lifecycle suite. On an exclusive delegated v2
   host, set `AGCOORD_TEST_CGROUP_ROOT` and require the opt-in namespace, CPU, PID, metrics, and
   cleanup tests. On an init-namespace-root host, also set `AGCOORD_TEST_CGROUP_IO=1` for owned
   loop-device bandwidth and IOPS tests. These do not replace the supported-Ubuntu host proof.
4. Build into three initially absent directories and audit the inputs:

   ```bash
   umask 022
   python -m build --outdir dist/python
   python -m twine check dist/python/*
   ./scripts/build-native-broker dist/native
   ./scripts/audit-native-broker \
     dist/native/agcoord-broker-x86_64-unknown-linux-musl
   ./scripts/check-native-licenses \
     dist/native/agcoord-broker-x86_64-unknown-linux-musl
   ./scripts/check-native-reproducible
   ./scripts/build-native-host-package \
     dist/native/agcoord-broker-x86_64-unknown-linux-musl dist/host
   ```

5. Confirm the release commit's native-host pin names the broker just built:

   ```json
   {"format": 1, "version": "<release version>", "broker_sha256": "<broker digest>"}
   ```

   The digest is the reproducible checksum `./scripts/check-native-reproducible` prints, which
   equals `sha256sum dist/native/agcoord-broker-x86_64-unknown-linux-musl`. Because that build is
   reproducible and the pin file is not an input to `scripts/native-source-id`, the digest can be
   computed before the release commit exists and recording it does not change the broker it
   names. Between releases `broker_sha256` stays `null`; such a client refuses
   `agc host install --download` instead of fetching bytes it cannot check. The
   [pin contract](native_host.md#the-native-host-pin) explains what the pin establishes.

6. From that still-clean source commit, assemble the candidate through the single artifact
   boundary:

   ```bash
   ./scripts/verify-release-candidate \
     --python-dir dist/python \
     --native-dir dist/native \
     --host-dir dist/host \
     --output-dir dist/release
   (cd dist/release && sha256sum --check SHA256SUMS)
   ```

   The verifier requires exactly two Python files, five native files, and eight host files. It
   rejects symlinks, extras, missing helpers, unsafe archive paths/modes, dirty source, unstable
   or unequal versions, a wrong wheel name/entry point, a copied broker in the wheel, changed
   sidecars, non-static ELF, unreviewed dependencies, differing raw/host identities, and a
   native-host pin that is absent, names another version, or does not match both the released
   broker and the broker inside the host package. It then
   installs the wheel with `xdist` and the sdist into separate fresh environments outside the
   checkout, and verifies `agc`/module entry points and the pytest plugin. Only
   after those checks pass does it create `release-manifest.json` and aggregate `SHA256SUMS`.
   GitHub's zipped workflow-artifact transport normalizes file modes, so the workflow restores
   `0755` on only the fixed broker and three fixed helper names before running this verifier;
   content sidecars and the host archive's internal modes remain independently checked.
7. Install the host bundle on the supported Ubuntu configuration through the staged runbook.
   For an existing spool, retain the exact durable drain receipt, require activation to match its
   ID and resume only after owner-locked maintenance completes.
   Keep `kernel.apparmor_restrict_unprivileged_userns=1`, start the ordinary unprivileged managed
   service, and retain the shipped `cpu=1` receipt proving its AppArmor transition, cgroup
   namespace root, exact CPU control, and durable applied/peak evidence. A spool left below
   protocol 5 by a pre-0.6.0 release is migrated through AGCoord 0.5.2 per the
   [pre-native spool guide](native_migration.md).
8. A PyPI upload is a separate, explicit maintainer action. Only after the user explicitly asks
   for it, upload the exact wheel and sdist already named in `release-manifest.json` with Twine.
   Twine reads an owner-only `~/.pypirc` and a project-scoped token; never put credentials in the
   repository, command line, workflow, or long-lived environment. No permission to implement,
   land, tag, or create a GitHub release implies PyPI authorization.
9. Read production PyPI JSON for the uploaded version, require exactly that wheel and sdist,
   compare their hashes to `SHA256SUMS`, and install `agcoord==<version>` from the production
   simple index into one more clean environment. If no PyPI upload was authorized, skip this
   step and state that the release is not on PyPI; never substitute TestPyPI evidence.
10. Tag the exact release commit as `v<version>`. The tag workflow independently rebuilds the
   Python, native, and host inputs, reruns conformance and the candidate verifier with the tag
   check enabled, and uploads one credential-free workflow artifact. After it passes and the
   supported-host receipt is retained, create the GitHub release and attach every file from the
   verified release bundle. If PyPI was authorized, the attached wheel and sdist must be the
   exact files production PyPI accepted.

The release workflow contains no package-index credentials, upload step, service activation, or
implicit spool migration. It fails closed on a dirty/mistagged source, missing conformance,
artifact or identity mismatch, failed clean install, or incomplete
checksum set. Release automation owns only temporary state and never opens the user's default
spool.
