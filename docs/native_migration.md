# Native broker migration and rollback

This runbook durably drains one AGCoord state directory, moves it from the Python reference
owner (protocol 1 through 4) to the Rust owner (protocol 5), proves the rollback path before
changing the live spool, and defines when the old production path may be removed. It complements
the [native host deployment runbook](native_host.md), which owns package installation, systemd,
AppArmor, delegated cgroups, and the enforced-host proof.

## Compatibility matrix

| Client or owner | State | Supported operation |
| --- | --- | --- |
| AGCoord 0.4.x Python client | Protocol 5, exact selected 0.4.x Rust identity | Normal CLI, TUI, adapter, and xdist operation |
| AGCoord 0.4.x Python client | Protocol 1–3 | `agc migrate` only; ordinary commands refuse |
| AGCoord 0.4.x Python client | Open protocol 4 | `agc drain` or explicit migration; ordinary submissions never start the legacy owner |
| AGCoord 0.4.x Python client | Draining/drained protocol 4 or 5 | Observe, explicitly cancel, migrate/rollback while fully drained, or resume with the exact drain ID |
| AGCoord 0.4.x Python client | Live protocol-4 Python owner | Install a durable drain; accepted work finishes before the old owner yields |
| Protocol-4 Python reference owner | Protocol 4 | Rollback inspection and compatibility testing only; never automatic 0.4 startup |
| Protocol-4 Python reference owner | Protocol 5 | Refuse; run the installed Rust `rollback` command while idle first |
| Rust 0.4.x broker | Protocol 1–4 | Explicit idle migration only; `serve` refuses |
| Rust 0.4.x broker | Protocol 5 created by another build | Refuse until the configured executable and live owner identities match exactly |

Protocol 1 through 3 is normalized to protocol 4 before the native transition. Migration never
invents resource enforcement, child leases, exact-head evidence, or publication authority that
the older schema could not represent. There is no supported live mixed-owner mode and no
configuration switch that makes the 0.4 client fall back to the Python broker.

Before deploying a build with the no-scratch default, audit admitted commands that relied on an
implicit private temporary directory. Such jobs must request a complete tmpfs policy or a complete
project-quota policy and honor the resulting `TMPDIR`; otherwise both owners remove inherited
`TMPDIR`, `TMP`, and `TEMP` and provision no per-run scratch path. Best-effort provider failure has
the same no-scratch environment. This is not a general filesystem sandbox, so separately named
checkout and host paths remain outside the resource contract.

The supported production host is x86_64 Ubuntu with the requirements in the
[host runbook](native_host.md). The release executable target is
`x86_64-unknown-linux-musl`; a client and broker must both be on the 0.4 release line, and a
running owner must retain the exact selected version and build digest.

## What “one executable” means

`/usr/libexec/agcoord/agcoord-broker` is one statically linked Rust ELF containing the broker,
scheduler, worker launcher, resource backends, migration engine, and bundled SQLite. It does
not mean the complete installation is one file. These deliberately remain external and
auditable:

- the owner-only `config.json`, SQLite spool, logs, rollback backup, and transient state;
- the systemd user unit that grants and supervises the delegated cgroup;
- the root-owned AppArmor policy that establishes setup, admitted-command, and client domains;
- checksums, provenance, licenses, and the host-package manifest; and
- the Python `agc` client, TUI, optional forge adapters, and pytest-xdist integration.

Copying the ELF alone therefore does not install a production broker. The host bundle binds the
ELF, unit, policy, manifest, and helper digests, and activation verifies all of them before the
service may start.

## Drain and retain a protocol-4 baseline

Keep the old client environment and the downloaded old package until the rollback window closes.
Install the matching 0.4 client in a separate environment, but do not change the live broker
configuration yet. Use that client against the exact state directory to install the durable
submission guard and wait for the accepted queue to finish. Do not infer idleness from process
names or repeatedly race an idle-timeout window.

Set explicit paths in one owner-only administrative shell:

```bash
state=${XDG_STATE_HOME:-$HOME/.local/state}/agcoord
backup_parent=$HOME/agcoord-migration-backups
install -d -m 0700 "$backup_parent"
agc --json --state-dir "$state" drain --reason "protocol 5 migration" \
  >drain-receipt.json
drain_id=$(jq -er 'select(.state == "drained" and .live == 0) | .drain_id' \
  drain-receipt.json)
```

Stage and validate the native host bundle as described in the host runbook. Staging is safe
while jobs run; activation is not. `drain` atomically rejects every later submission, allows
already queued/running rows to reach their normal result, commits `drained`, and makes the old
owner yield even if it has no idle timeout. Acquire the same ownership lock non-blockingly while
copying the whole directory, including the database, WAL, SHM, configuration, logs, lock
metadata, durable drain keys, and SQLite guards:

```bash
backup="$backup_parent/protocol4-$(date -u +%Y%m%dT%H%M%SZ)"
install -d -m 0700 "$backup"
flock --exclusive --nonblock "$state/broker.lock" \
  cp --archive --one-file-system "$state/." "$backup/"
find "$backup" -xdev -type f -print0 | sort -z | xargs -0 sha256sum \
  >"$backup_parent/$(basename "$backup").sha256"
```

A failed `flock` means an owner still holds the spool; inspect the retained drain receipt and
wait for accepted work or explicitly cancel named rows. Never copy only `queue.sqlite3`, copy
while the owner lock is held elsewhere, resume merely to make the copy succeed, or edit protocol
metadata with SQLite. Keep the backup, drain receipt, and checksum manifest owner-only.

## Prove rollback before touching the live spool

Make a second copy of the retained baseline and exercise the exact installed Rust binary against
that disposable copy. This validates its checkpoint, private protocol-4 backup, migration, and
rollback logic without changing the live state:

```bash
rehearsal="$backup_parent/rollback-rehearsal"
install -d -m 0700 "$rehearsal"
cp --archive --one-file-system "$backup/." "$rehearsal/"
/usr/libexec/agcoord/agcoord-broker migrate --state-dir "$rehearsal"
/usr/libexec/agcoord/agcoord-broker rollback --state-dir "$rehearsal"
```

The two receipts must report `4 -> 5` and `5 -> 4`. Use the retained old client to inspect the
rehearsal copy and require its terminal history to remain readable. Do not proceed if rollback
reports a missing/corrupt backup, live work, or unexpected protocol.

Repository releases automate the same sequence with `scripts/rehearse-native-migration`. It
uses only owned temporary state, creates both legacy and native terminal history, proves live
and idle mixed-version refusals, rolls back, reopens with the Python reference owner, migrates
again, and stops every broker it creates. `scripts/verify-release-candidate` runs that rehearsal
from a clean wheel installation against the exact release ELF.

## Install and migrate the live spool

With the live state still drained, activate the staged host package using the retained ID.
Activation takes the spool lock, independently verifies the exact durable marker and zero live
rows, installs the fixed files, and does not start or restart the service. Write the production
`config.json` from the host runbook only after the old owner has stopped; an older client may
reject the new `managed_service` field.

Install the matching 0.4 Python client, then run the one explicit schema transition before
resuming or starting the service:

```bash
sudo ./install-native-host activate "$state" --drain-id "$drain_id"
systemctl --user daemon-reload
agc --json --state-dir "$state" migrate | tee migration-receipt.json
jq -e '.changed and .from_protocol <= 4 and .to_protocol == 5' \
  migration-receipt.json
agc --json --state-dir "$state" list | jq -e --arg drain_id "$drain_id" \
  '.protocol == 5 and .maintenance.state == "drained" and
   .maintenance.drain_id == $drain_id'
agc --state-dir "$state" resume "$drain_id"
systemctl --user start agcoord-broker.service
systemctl --user --no-pager status agcoord-broker.service
agc --json list | jq -e '.protocol == 5'
```

`agc migrate` selects and verifies the configured native executable, requires no owner lock or
live rows, checkpoints WAL completely, writes and verifies a mode-`0600` rollback database, and
then changes native ownership metadata atomically. It preserves the exact drain marker and
guards, so migration cannot reopen submissions. Exact-ID `resume` removes them only after all
owner-locked maintenance succeeds. Building or installing either distribution never migrates
a spool implicitly.

On the supported Ubuntu host, run the shipped enforced-host proof from the host runbook. Require
the service preflight and an ordinary `cpu=1` job to prove the fixed AppArmor domains, global
unprivileged-user-namespace restriction, namespace-rooted delegated cgroup, exact `cpu.max`, and
durable applied/peak receipt. A protocol-5 snapshot without that receipt is not evidence that
the host enforcement boundary works.

## Roll back during the retained window

Rollback is an explicit operational decision. Install a durable drain with the current 0.4
client, retain its exact ID, let every queued and running row finish (or cancel named rows under
an explicit cancellation policy), and stop the native service. Keep the installed native binary
in place until its rollback command succeeds:

```bash
agc --json --state-dir "$state" drain --reason "protocol 4 rollback" \
  >rollback-drain-receipt.json
drain_id=$(jq -er 'select(.state == "drained" and .live == 0) | .drain_id' \
  rollback-drain-receipt.json)
systemctl --user stop agcoord-broker.service
/usr/libexec/agcoord/agcoord-broker rollback --state-dir "$state"
agc --json --state-dir "$state" list | jq -e --arg drain_id "$drain_id" \
  '.protocol == 4 and .maintenance.state == "drained" and
   .maintenance.drain_id == $drain_id'
agc --state-dir "$state" resume "$drain_id"
```

The receipt must report `5 -> 4`. Rollback restores the verified normalized protocol-4 baseline
and replays terminal native rows and leases. It preserves the currently installed drain and
does not resurrect a stale marker copied into the older baseline. It records a cutoff that makes
every earlier full gate stale; the old workflow must run a new exact-head full gate before any
publication.

Restore the old client environment and a protocol-4-compatible `config.json` before starting the
reference owner. Do not leave `managed_service`, the native executable selection, or new binding
syntax in a configuration consumed by a release that does not recognize them. Leave the native
service stopped while the protocol-4 owner is in use. Preserve the current state directory and
both backups even if rollback fails; do not repeatedly migrate, copy a database over live state,
or delete evidence to make startup succeed.

## Close the rollback window

Close the window only after the enforced-host proof, normal workloads, restart recovery, and the
organization's retention interval pass. Record the release manifest, live migration receipt,
host enforcement receipt, and backup disposition. Then remove custom service units or automation
that can invoke `python -m agcoord.queue serve` and remove the retained old tool environment when
policy permits. The Python reference broker remains in the source distribution as a conformance
oracle and rollback reader; it is not a production fallback path.

Do not remove the root-owned native executable, its AppArmor policy, or its systemd unit from an
active protocol-5 installation. A future package removal must first drain and stop the service
and follow either a supported native upgrade or the rollback procedure above.

## Troubleshooting refusals

| Refusal or symptom | Meaning and safe response |
| --- | --- |
| Live `legacy protocol-4` owner | Use the matching 0.4 client to install a durable drain; accepted work finishes and the old owner then yields. |
| Idle `queue uses protocol 4` | Install and retain a durable drain, back up and rehearse rollback, then run exactly one `agc migrate`. |
| `broker-draining` or `agcoord-maintenance-draining` | Expected submission refusal: maintenance owns the admission boundary. Observe/cancel accepted rows or resume with the exact retained ID after maintenance. |
| `broker-drain-id-mismatch` | The supplied token is not the active drain. Recover the owner-only receipt or inspect the visible maintenance status; never guess or delete the marker. |
| `host-drain-required` or `host-drain-incomplete` | Activation saw no complete `drained` marker. Return to `agc drain`; zero rows alone is not sufficient. |
| `broker-migration-live-runs` | Rows are still queued/running; inspect and drain or explicitly cancel them. |
| `broker-migration-backup-failed` | WAL checkpoint or private backup verification failed; preserve state and resolve storage/lock health before retrying. |
| `broker-migration-backup-invalid` on rollback | Do not rewrite metadata; retain the spool and operator backup and investigate the recorded internal backup. |
| Selected/live native identity mismatch | The configured file and owner differ; restore the exact file or drain and perform a verified package activation. |
| Managed service preflight refusal | Use the stable host refusal code to fix executable ownership, AppArmor, global policy, cgroup path/controllers, or service identity; do not weaken a required binding. |
| Service starts but enforced receipt is absent | Treat enforcement as unverified, keep publication disabled, and rerun the shipped host proof after fixing the boundary. |
| Old Python owner refuses protocol 5 | Expected fail-closed behavior; stop it and either continue native operation or run the installed Rust rollback command while idle. |

Never troubleshoot migration by disabling AppArmor's global restriction, recursively changing
cgroup ownership, editing the spool directly, killing an unverified PID, or deleting the state
directory. Those actions erase the evidence the refusal is protecting.
