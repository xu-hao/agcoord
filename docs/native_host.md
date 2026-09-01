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

The checker rejects a changed package checksum, unexpected path, symlink, non-root archive
owner, unsafe mode, changed embedded file, mismatched identity, or invalid AppArmor policy. The
installer rechecks the package before staging and rechecks the installed binary's digest and
identity after activation. A production activation also requires a release musl identity and
installs every live file as root-owned.

The Python wheel intentionally contains no broker executable. Configuration, the spool,
systemd unit, and AppArmor policy remain external to the one-file native runtime.

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
distinguish a kernel namespace refusal from an AppArmor denial.

## First install

Download all host artifacts into one owner-only directory. Verify the three helper sidecars,
then let the checker validate the package:

```bash
sha256sum --check check-native-host-package.sha256
sha256sum --check install-native-host.sha256
sha256sum --check test-native-host-enforcement.sha256
./check-native-host-package agcoord-native-host-x86_64-linux.tar.gz
sudo ./install-native-host stage agcoord-native-host-x86_64-linux.tar.gz
```

`stage` writes only `/var/lib/agcoord/native-host-pending/<package-sha256>` and an owner-only
selection marker. It is safe while jobs run and does not change a live file, policy, unit, or
service. Before activation, use `agc list` to require no queued or running row. If policy calls
for cancellation instead, cancel each named row and wait until every result is terminal. Then:

```bash
systemctl --user stop agcoord-broker.service
sudo ./install-native-host activate "${XDG_STATE_HOME:-$HOME/.local/state}/agcoord"
systemctl --user daemon-reload
```

Activation takes the same exclusive spool ownership lock and independently counts live rows.
It refuses if a broker still owns the lock or any queued/running row remains. It accepts an idle
protocol-1-through-5 spool without changing it, installs and verifies the selected files, and
deliberately does not start or restart the service. For a fresh owner-only state directory, the
native maintenance holder creates `broker.lock` as that directory's owner with mode `0600` and
keeps it locked through final identity verification. Existing locks with a wrong owner, mode,
type, or link count are refused rather than repaired by a root shell.

For a fresh state directory, start the service. For an older spool, first run the explicit
`agc migrate` procedure in [the coordinator guide](coordinator.md#migrations), then start it:

```bash
agc migrate                 # omit for a fresh or already protocol-5 spool
systemctl --user start agcoord-broker.service
agc list
```

## Enforced-host proof

Run the shipped probe as an ordinary coordinated job with exactly one CPU unit; do not invoke
it from a nested coordinator. It proves the admitted AppArmor transition, user-namespace
denial, cgroup namespace root, exact `cpu.max`, release owner identity, and the restricted
profile reached when admitted work invokes the broker. The final assertion proves that the durable public
receipt records the enforced allocation:

```bash
agc --json run --label "native host enforcement" --resource cpu=1 \
  -- ./test-native-host-enforcement >native-host-receipt.json
jq -e '
  .status == "passed"
  and .resource_receipt.requested.cpu == 1
  and .resource_receipt.applied.cpu == 1
  and .resource_receipt.peak.cpu == 1
' native-host-receipt.json
```

The setup-only `agcoord-broker` profile attaches only to the fixed root-owned executable; it is
not selected by the user-editable systemd unit. The public binary has no internal-worker or
arbitrary setup-domain exec command. The broker's authenticated in-process worker transitions
the submitted command into `agcoord-admitted` before release. When that admitted command invokes
the fixed broker, an explicit transition selects `agcoord-broker-client` instead of reattaching
the setup profile. Both domains deny user-namespace creation and changing back to setup, and the
admitted restriction persists across arbitrary interpreter execution. All three profiles use
explicit enforce mode and broad enumerated host permissions; `default_allow` is not accepted
because Ubuntu 24.04 implements it as an unconfined profile that does not apply these denials.

## Upgrade, recovery, and rollback

For an upgrade, run `stage` while the current service remains available. Inspect the staged
package digest, drain normally or apply an explicitly chosen cancellation policy, stop the user
service, and run `activate`, `daemon-reload`, and `start` in that order. Never replace the live
binary and ask systemd to restart while work remains. After start, inspect `systemctl --user
status agcoord-broker.service`, `agc list`, and rerun the enforced-host proof.

`Restart=on-failure` recovers an unexpected broker exit without an idle shutdown. The durable
spool remains the authority: the replacement adopts only identity-verified live workers and
never reruns their command. A deliberate `systemctl stop` does not restart. If staging is
interrupted, no live file changed; repeat it. If activation is interrupted, leave the service
stopped and repeat activation for the same selected package, which revalidates every final
file before start. Never delete or replace the state directory as host-package recovery.

To roll back between protocol-compatible native packages, stage the previously retained bundle
and use the identical drain, stop, activate, reload, and start sequence. Host activation never
rewrites the spool. To return to the Python protocol-4 owner, first drain and stop the native
service, run `/usr/libexec/agcoord/agcoord-broker rollback --state-dir PATH` while the current
binary is still installed, then change the client/configuration according to the target release.
That rollback restores
the verified protocol-4 baseline, replays terminal native history, and invalidates every old
gate receipt through the recorded cutoff. Preserve the state directory and its rollback backup;
a fresh exact-head gate is required before legacy publication.
