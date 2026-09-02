# Native broker host deployment

This runbook is the canonical installation and operations contract for AGCoord's native broker
host package. It applies to x86_64 Ubuntu hosts with AppArmor ABI 4, unified cgroup v2 mounted
read-write with `nsdelegate`, `kernel.apparmor_restrict_unprivileged_userns=1`, and systemd 254
or newer. The broker is an ordinary unprivileged user service; no root daemon is installed.

## Package boundary

The release produces `agcoord-native-host-x86_64-linux.tar.gz` and its SHA-256 sidecar, plus
`check-native-host-package`, `install-native-host`, and `test-native-host-enforcement` with
individual sidecars. The tar archive contains only these root-owned host files:

- `/usr/libexec/agcoord/agcoord-broker` and its SHA-256 sidecar;
- `/usr/lib/systemd/user/agcoord-broker.service`;
- `/etc/apparmor.d/usr.libexec.agcoord.agcoord-broker`;
- the exact native identity and file-digest manifest, AGCoord license, and reviewed Rust
  dependency inventory under `/usr/share/doc/agcoord/`.

The builder normalizes its creation umask to `022`, so archive directory modes and bytes do not
depend on the invoking shell. The checker rejects a changed package checksum, unexpected path,
symlink, non-root archive owner, unsafe mode, changed embedded file, mismatched identity, or
invalid AppArmor policy. It preserves the archived permissions in its test-owned extraction, so
it validates the artifact's modes independently of the caller's umask, including the broker's
restrictive admitted worker umask. The installer rechecks the package before staging and rechecks
the installed binary's digest and identity after activation. A production activation also
requires a release musl identity and installs every live file as root-owned.

The Python wheel intentionally contains no broker executable. “One executable” describes the
statically linked Rust runtime, not a one-file installation: configuration, durable state,
systemd supervision, AppArmor policy, checksums, provenance, and the Python clients remain
external and independently auditable. The exact boundary and compatibility matrix are in the
[migration runbook](native_migration.md#what-one-executable-means).

## Configuration

The managed service owns only the default `%S/agcoord` state directory and stays alive until
systemd stops it. It reads capacities and bindings from that directory's `config.json`; command
line capacity and idle-timeout overrides are refused. Replace `1000` below with `id -u` and set
the capacities and bindings appropriate for the host:

```json
{
  "capacities": {"jobs": 4, "cpu": 4},
  "bindings": {
    "cpu": {
      "kind": "cpu",
      "unit": "logical-cpu",
      "mode": "required",
      "backend": "cgroup-v2"
    }
  },
  "cgroup_root": "/sys/fs/cgroup/user.slice/user-1000.slice/user@1000.service/app.slice/agcoord-broker.service",
  "native_broker": {
    "path": "/usr/libexec/agcoord/agcoord-broker",
    "allow_development": false,
    "managed_service": true
  }
}
```

Create the state directory as the broker user with mode `0700` and `config.json` with mode
`0600`. Do not set `AGCOORD_STATE_DIR` for a managed service. `Delegate=yes`, the explicit
`app.slice`, and `DelegateSubgroup=supervisor` make the configured service root deterministic
and keep the broker process out of the inner node where it enables controllers.

On every managed start, the broker fails before acquiring the spool unless all of these match:

- the running executable is the fixed, root-owned, non-writable release file and its installed
  checksum and musl build identity agree;
- configuration selects that file, release trust, and managed-service mode;
- the fixed executable has attached the enforced `agcoord-broker` AppArmor domain while Ubuntu's global
  unprivileged-user-namespace restriction remains enabled;
- its unique unified cgroup is the `supervisor` subgroup of the configured service root, both
  directories are owned safely, the mount is read-write with `nsdelegate`, and CPU, memory, and
  PID controllers are delegated.

The cgroup backend then performs its destructive probe, including user-, cgroup-, and mount-
namespace creation and controller-file protection. An unavailable required binding refuses the
run before user code; it is never silently treated as admission-only.
The capability probe first mounts the namespace-rooted cgroup2 view directly. Linux may report
`EBUSY` when that view collides with the inherited cgroup2 mountpoint; in that case the broker bind
mounts only its already-attached leaf over the inherited view. `CLONE_NEWCGROUP` and `nsdelegate`
still protect the namespace root, and the probe verifies both properties before admitting work.
Other refusals distinguish `namespace-propagation-mount-failed`,
`namespace-cgroup2-mount-failed-errno-N`, and `namespace-cgroup2-bind-failed-errno-N`, where `N` is
the retained Linux error number. A later worker-side refusal is normalized to the stable
`namespace-mount-failed` setup code. Inspect the kernel journal alongside that receipt to
distinguish a kernel namespace refusal from an AppArmor denial. Controller metrics accept the
kernel's dotted field names, including Linux 6.17's `core_sched.force_idle_usec` CPU statistic.

## First install

Download all host artifacts into one owner-only directory. Verify the three helper sidecars,
restore the helpers' declared executable mode (ordinary HTTP and workflow-artifact downloads do
not carry a POSIX mode), then let the checker validate the package:

```bash
chmod 0755 check-native-host-package install-native-host test-native-host-enforcement
sha256sum --check check-native-host-package.sha256
sha256sum --check install-native-host.sha256
sha256sum --check test-native-host-enforcement.sha256
./check-native-host-package agcoord-native-host-x86_64-linux.tar.gz
sudo ./install-native-host stage agcoord-native-host-x86_64-linux.tar.gz
```

`stage` writes only `/var/lib/agcoord/native-host-pending/<package-sha256>` and an owner-only
selection marker. It is safe while jobs run and does not change a live file, policy, unit, or
service. Install the matching Python client before changing the host files. For an existing
spool, atomically close submissions and retain the exact durable drain receipt:

```bash
state=${XDG_STATE_HOME:-$HOME/.local/state}/agcoord
agc --json --state-dir "$state" drain --reason "native host activation" \
  >native-host-drain.json
drain_id=$(jq -er 'select(.state == "drained" and .live == 0) | .drain_id' \
  native-host-drain.json)
systemctl --user stop agcoord-broker.service
sudo ./install-native-host activate "$state" --drain-id "$drain_id"
systemctl --user daemon-reload
```

`drain` rejects every later submission at the database transaction boundary while accepted work
finishes normally; it does not cancel those rows. The owner commits `drained` and yields its lock
when the live count reaches zero. Activation then takes that same lock, validates the complete
durable marker, independently confirms zero live rows, and requires `--drain-id` to match it
exactly. It installs and verifies the selected files without starting or restarting the service.
An unmarked, still-draining, mismatched-ID, live, or owned existing spool is refused.
The holder's stable JSON codes are `host-drain-required`, `host-drain-incomplete`,
`host-drain-live-work`, `host-drain-owner-live`, `host-drain-state-invalid`,
`host-drain-lock-invalid`, and `host-drain-protocol-mismatch`. The installer additionally exits
with status 2 before acquiring the lock when an existing spool has no syntactically valid
`--drain-id` or a fresh spool is given one; a holder receipt that does not match the exact ID is
an activation failure with status 1.

For a fresh owner-only state directory with no database, omit `--drain-id`. The native
maintenance holder creates `broker.lock` as that directory's owner with mode `0600` and keeps it
locked through final identity verification. A fresh spool rejects a drain ID. Existing locks
with a wrong owner, mode, type, or link count are refused rather than repaired by a root shell.

For a fresh state directory, activate without a token, reload, and start:

```bash
state=${XDG_STATE_HOME:-$HOME/.local/state}/agcoord
sudo ./install-native-host activate "$state"
systemctl --user daemon-reload
systemctl --user start agcoord-broker.service
agc list
```

For an existing spool, first complete the backup and rollback rehearsal and follow the explicit
transition in the [native migration runbook](native_migration.md). After activation and reload,
keep the service stopped while completing the remaining steps:

```bash
agc migrate                 # omit for an already protocol-5 spool
agc resume "$drain_id"
systemctl --user start agcoord-broker.service
agc list
```

Migration preserves the drain marker across protocol 4 to 5, and activation never removes it.
Resume only after every owner-locked maintenance step has succeeded. If any step fails, leave
the service stopped and the marker in place; rerunning `drain` reports the same ID.

## Enforced-host proof

Run the shipped probe as an ordinary coordinated job with exactly one CPU unit; do not invoke
it from a nested coordinator. It proves the admitted AppArmor transition, user-namespace
denial, cgroup namespace root, exact `cpu.max`, release owner identity, and the restricted
profile reached when admitted work invokes the broker. It also calls the installed Python
client's operation-specific callback by showing the exact admitted run while it is live; this
guards the complete `agc`-to-native path used by land reporting and pytest-xdist child leases.
The final assertion proves that the durable public receipt records the enforced allocation:

```bash
agc --json run --label "native host enforcement" --resource cpu=1 \
  -- ./test-native-host-enforcement >native-host-receipt.json
jq -e '
  .status == "passed"
  and .resource_receipt.requested.cpu == 1
  and .resource_receipt.applied.cpu == 1
  and .resource_receipt.peak.cpu >= 1
' native-host-receipt.json
```

The requested and applied values prove the exact configured CPU limit. The nonzero peak proves
that usage was measured; it is a conservative ceiling of sampled usage concurrency and may exceed
the quota when short parallel bursts are rounded upward.

The setup-only `agcoord-broker` profile attaches only to the fixed root-owned executable; it is
not selected by the user-editable systemd unit. The public binary has no internal-worker or
arbitrary setup-domain exec command. The broker's authenticated in-process worker makes a one-way
transition into `agcoord-admitted`, verifies it, and only then clears capabilities and sets
`no_new_privs` before release. Arbitrary interpreter execution inherits that profile. When
admitted work invokes the fixed broker, AppArmor stacks `agcoord-broker-client` onto the existing
admitted confinement; this adds restrictions without requesting a replacement domain after
`no_new_privs`. Both restricted profiles deny user-namespace creation and changing back to setup.
All three profiles use explicit enforce mode and broad enumerated host permissions;
`default_allow` is not accepted because Ubuntu 24.04 implements it as an unconfined profile that
does not apply these denials.

Within the admitted user namespace, `stat(2)` reports the host-root-owned installed binary with
the overflow UID because host root is intentionally absent from the one-entry identity map. The
Python client does not weaken its ordinary root-ownership policy or whitelist that UID. Only its
admitted callback selector accepts this view, and only after checking the fixed installed path,
managed release configuration, exact UID/GID maps, denied `setgroups`, and the native restricted
profile preflight. The selector exposes own-run status, authenticated child leases, and land
verification/phase/result reporting; it does not turn `agc` into a nested submission mechanism.

## Upgrade, recovery, and rollback

For an upgrade, run `stage` while the current service remains available. Inspect the staged
package digest, run `agc drain`, retain its exact ID, and stop the user service after the receipt
says `drained`. Run `activate --drain-id ID`, `daemon-reload`, any required `migrate`, exact-ID
`resume`, and `start` in that order. An explicitly chosen cancellation policy may shorten the drain,
but cancellation never replaces its durable submission guard. Never replace the live binary and
ask systemd to restart while work remains. After start, inspect `systemctl --user status
agcoord-broker.service`, `agc list`, and rerun the enforced-host proof.

`Restart=on-failure` recovers an unexpected broker exit without an idle shutdown. The durable
spool remains the authority: the replacement adopts only identity-verified live workers and
never reruns their command. A deliberate `systemctl stop` does not restart. If staging is
interrupted, no live file changed; repeat it. If activation is interrupted, leave the service
stopped and repeat activation for the same selected package, which revalidates every final
file before start. Never delete or replace the state directory as host-package recovery.

To roll back between protocol-compatible native packages, stage the previously retained bundle
and use the identical drain, stop, activate, reload, resume, and start sequence. Host activation
never rewrites the spool. To return to the Python protocol-4 owner, first drain and stop the
native service, run `/usr/libexec/agcoord/agcoord-broker rollback --state-dir PATH` while the
current binary is still installed, then use the current client to `agc resume ID` before changing
the client/configuration according to the target release. That rollback restores
the verified protocol-4 baseline, replays terminal native history, and invalidates every old
gate receipt through the recorded cutoff. Preserve the state directory and its rollback backup;
a fresh exact-head gate is required before legacy publication. Follow the complete
[rollback procedure](native_migration.md#roll-back-during-the-retained-window), including the
configuration change and retirement criteria; do not treat this abbreviated host-package
sequence as a live-spool runbook.
