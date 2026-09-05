# Troubleshooting

AGCoord refuses rather than guesses, and every refusal carries a stable code or a fixed
message. This page collects the ones an operator or an agent meets, what each means, and what
to do next. The [coordinator contract](coordinator.md), the
[native broker contract](native_broker.md), and the [native host runbook](native_host.md)
remain the authority for the behavior behind each entry, and the
[agent guide](agents.md) has the shorter table an agent needs.

## How a refusal reaches you

- **A command that is refused** prints one `error:` line to standard error and exits with
  status 2. With `--json`, standard error carries `{"code": …, "message": …}` instead, and
  automation should branch on `code`.
- **A job that ran and failed** is a row: `agc --json show <id>` gives `status`, `phase`,
  `failure_reason`, `gate_exit_status` for a landing, `exit_status`, and a `resource_receipt`
  whose `events` list carries `code`, `stage`, `status`, `backend`, and `resource` for every
  enforcement fact. `agc log <id>` prints the job's combined output; for a landing it is one
  transcript covering preflight, gate, and publication.
- **A host operation** prints a receipt with its own stable codes, and `agc host … --json`
  emits it as an object. Its failures name the phase that failed and what was rolled back.
- **The broker itself** writes `broker.log` in the state directory. On a managed host,
  `journalctl --user -u agcoord-broker.service` has the service log and `journalctl -k` has the
  kernel's AppArmor and namespace messages.

## Submitting work

| What appears | Meaning | Next action |
| --- | --- | --- |
| `<dir> is not inside a Git repository; agc schedules work per repository and worktree, so run it from a checkout or pass --checkout PATH` | `run`, `full`, and `land` need a repository and worktree identity. | Run from inside the checkout or pass `--checkout PATH`. |
| `checkout does not exist: <path>` | `--checkout` named a missing directory. | Fix the path. |
| `checkout is dirty; commit or remove changes before a full run` | `full` and `land` bind an exact clean head. | Commit or remove the changes; for `land`, push the head. |
| `resource '<name>' has no configured machine capacity` | The broker's `config.json` declares no such capacity. | Claim only configured names, or add the capacity to `config.json` and restart the broker. |
| `resource '<name>' requests N, above capacity M` | The claim can never be admitted on this machine. | Lower the claim; `agc --json list` prints `capacities`. |
| `a coordinated job cannot submit another coordinated job; invoke it directly from the checkout` | Nested submission from inside an admitted job. | Run the tool directly; a job may only call `agc show "$AGCOORD_RUN_ID"` and `agc verify-admission`. |
| `broker-draining` (`agcoord-maintenance-draining` for a direct SQLite writer) | Maintenance closed submissions; admitted work is finishing. | Wait. `list`, `show`, `log`, the TUI, and `cancel` still work. |
| `broker-drained` | The spool finished draining and has no owner; nothing autostarts. | `agc resume <drain-id>` with the ID the drain printed, or finish the maintenance that needed it. |
| `Gate queue: <id> waiting at position N for branch …` | Queued, not refused: capacity or a barrier is holding it. | `blocked_by` in `agc --json show <id>` names the jobs ahead; a `land` in the same lane, or a job from the same worktree, waits for that land. |
| `native broker executable does not exist: /usr/libexec/agcoord/agcoord-broker; install the host package or configure native_broker.path` | No broker is installed and the configuration selects the default managed path. | Run `agc host install --user` for an unmanaged broker, or `agc host install --download` on a supported host. |
| `native broker version is unsupported: <v>` | The client and the broker are on different minor lines. | Upgrade the client (`pip install -U agcoord`) and then the broker: `agc host upgrade --download` on a managed host, `agc host install --user` for a user-owned broker. |

## Landing handbacks

These arrive as a failed row. `failure_reason` is stable; the log has the detail.

| `failure_reason` | Meaning | Next action |
| --- | --- | --- |
| `gate-failed` | The gate command exited non-zero; `gate_exit_status` has the status. | Fix the code, push, and submit a new land request. |
| `stale-main` | The target moved after preflight or during the gate, or `--no-target-sync` met an advanced target. | Update the branch from the target, push, and resubmit. With target synchronization on, this normally means the target moved again during the gate. |
| `head-changed` | The source branch moved while the landing ran. | Do not push to a branch that is landing; resubmit for the new head. |
| `pr-not-ready` | The pull request is closed, a draft, on another base, or not at this head. | Fix the pull request so its head equals the local head, then resubmit. |
| `merge-error` | The pre-gate target merge conflicted; the checkout was restored and the log names the paths. | Merge the target locally, resolve, push, and resubmit. |
| `avoided-commit` | The branch, the target, or the synchronized head reaches a commit stored with `agc avoid`. | Rebuild the request as a fresh branch from the current target and resubmit. |
| `publish-failed` | The forge rejected the atomic update; nothing moved. | Check `gh auth status`, branch protection (a required hosted merge queue rejects the update), and the pull request; resubmit. |
| `memory-oom` | The job exceeded its declared memory and was ended as one process group. | Raise the `memory` claim or reduce the job's footprint; the receipt's `peak` says how far over it went. |
| `resource-enforcement-failed` | A `required` binding could not be applied before user code; the launcher exited 125. | Read the receipt's events for the backend's code; fix the host or make the binding `best-effort`. |
| status `interrupted` | The worker vanished before it could report; no verdict is claimed. | Read the log; unless it ends with `LANDED`, submit again. |
| status `cancelled` | `agc cancel`, or a graceful broker stop reaped a queued or gating job. | Resubmit when appropriate. Publication is never cancelled. |

## Maintenance

| Command and code | Meaning | Next action |
| --- | --- | --- |
| `agc drain --reason …` | Installs a durable guard, waits for admitted work, then the owner yields. The receipt's `drain_id` is the only key that reopens the spool. | Save the ID. `--no-wait` returns after installing the guard. Repeating `drain` returns the original receipt. |
| `agc resume <id>` refused with `broker-drain-id-mismatch` or `broker-drain-id-invalid` | Not the exact `drain-…` identifier the drain printed. | Use the receipt's ID; `agc list` prints it in its header while drained. |
| `broker-drain-live-work` | Resume was asked while rows are still queued or running. | Wait for the rows to end, or cancel them. |
| `broker-resume-owner-live` | A broker still holds the ownership lock. | Let it exit; on a managed host, stop the service first. |
| `broker-not-draining` | Nothing to resume. | No action. |
| `agc avoid` refused with `avoid-sha-invalid` | The commit must be the full 40-character SHA. | Pass the full SHA; `--list` shows the stored set and `--remove SHA` deletes one. |
| `avoid-file-invalid`, `avoid-git-failed` | `avoid.json` beside `config.json` is unreadable, or Git could not be run in the checkout. | Restore the owner-only file, or run from a checkout. |
| `agc clear` refused with `broker-clear-live-work` | Terminal history is removed only while nothing is queued or running. | Wait, or cancel the live rows. `clear` never removes the spool or the avoided set. |

## Broker startup and the spool

| Code | Meaning | Next action |
| --- | --- | --- |
| `broker-protocol-mismatch`, or a refusal naming AGCoord 0.5.2 | The spool is below protocol 5, left by a release before 0.6.0. | Follow [migrating a pre-native spool](native_migration.md). |
| `broker-already-owned` | Another broker holds the lock; concurrent first clients race for it and the loser joins the winner. | Nothing, normally. If it persists with no live broker, inspect `broker.lock` and `broker.log` in the state directory. |
| `broker-not-running` | A command needed a live owner and found none. | Unmanaged: any `agc list` starts one. Managed: `systemctl --user status agcoord-broker.service`. |
| `broker-database-busy` | SQLite stayed locked longer than `database_timeout` (10 s by default). | Look for a stuck writer; raise `database_timeout` in `config.json` if the disk is slow. |
| `broker-config-invalid` | `config.json` has an unknown key, a non-object section, an invalid binding, or an empty `cgroup_root`. | Fix the file against the [configuration contract](coordinator.md#repository-lanes-and-resources). |
| `broker-state-invalid`, `broker-schema-invalid`, `broker-owner-metadata-invalid`, `broker-owner-lock-unavailable` | The state directory or its files are not what the broker expects. | Do not repair by hand. Check ownership and modes (`0700` directory, `0600` files), then the [native broker contract](native_broker.md#stable-refusal-envelope). |
| `native broker identity is incompatible with this client` | The selected executable is not a protocol-5 AGCoord broker of a supported line. | Point `native_broker.path` at the installed broker, or reinstall it. |

## The managed host

| Code | Meaning | Next action |
| --- | --- | --- |
| `native-host-state-invalid` | An ambient `AGCOORD_STATE_DIR`, a nondefault spool, an unsafe state directory, or a configuration that is not the managed one. | Unset the variable; the managed service owns only the default spool. For an existing user-owned spool, drain it and move it aside first. |
| `a queue already exists at …; use agc host upgrade` | `install` is for a fresh spool. | Use `agc host upgrade --download`. |
| `native-host-version-mismatch` | The bundle is not the installed client's version. | Install the matching client first; `--download` then fetches the right bundle. |
| `native-host-bundle-invalid` | A missing, oversized, symlinked, or wrongly owned file in the bundle. | Download the bundle again, or check the directory's modes (`0700`). |
| `native-host-pin-mismatch` | The bundle's broker is not the one this client was released against. | Fetch the release that matches the client. Never override the pin. |
| `native-host-unpinned-client` | A development client carries no pin, so it cannot verify a download. | Install a release client, pass a bundle path you verified yourself, or supply `--broker-sha256`. |
| `native-host-pin-conflict` | `--broker-sha256` disagrees with the shipped pin. | Drop the option; a supplied digest never replaces a pin. |
| `native-host-bundle-source-conflict`, `native-host-bundle-source-missing` | Both or neither of a bundle path and `--download` (or `--user`). | Pass exactly one source. |
| `native-host-download-insecure-url`, `native-host-download-invalid-repository`, `native-host-download-unknown-adapter`, `native-host-download-digest-mismatch` | The download source or its bytes are not acceptable. | Use an `https` base URL, an `owner/name` repository, the `github` adapter; a transport digest mismatch means a corrupted or substituted download. |
| `native-host-upgrade-nested` | Host operations cannot run from an admitted job. | Run them from an ordinary shell. |
| `native-host-install-authorization-failed`, `native-host-upgrade-authorization-failed` | `sudo` could not authorize the privileged step. | Run the command in a terminal that can answer sudo; nothing was changed. |
| `…-stage-failed`, `…-activation-failed`, `…-reload-failed`, `…-start-failed`, `…-stop-failed` | The named phase failed; the receipt says what was rolled back. | Fix the cause the phase names and rerun the same command. |
| `native-host-install-incomplete`, `native-host-upgrade-incomplete` | Activation happened but verification or the enforcement proof failed; an unproved fresh service is disabled again. | Read the receipt's inner error, fix it, rerun. An upgrade's drain ID is retained; `agc resume` it only when the host is proved. |
| `native-host-upgrade-drain-invalid` | The drain receipt the upgrade needs is missing or malformed. | Rerun `agc drain`, then the upgrade. |

## Enforced-host probe failures

The managed broker refuses to own the spool, or a required run refuses before user code, when
the host does not match the [host contract](native_host.md#configuration). The code names the
check; `journalctl -k` distinguishes a kernel refusal from an AppArmor denial.

| Code | Meaning | What to inspect |
| --- | --- | --- |
| `host-apparmor-profile-mismatch` | The service did not enter the enforced `agcoord-broker` profile. | `aa-status`; reload the profile from `/etc/apparmor.d`; `systemctl --user restart agcoord-broker.service`. |
| `host-apparmor-restriction-disabled` | `kernel.apparmor_restrict_unprivileged_userns` is not 1. | Restore the sysctl; the boundary depends on it. |
| `host-cgroup-controllers-unavailable`, `host-cgroup-mount-invalid`, `host-service-cgroup-mismatch` | CPU, memory, or PID controllers are not delegated, the mount is not read-write `nsdelegate` cgroup2, or the service's cgroup is not the configured root. | `cat /sys/fs/cgroup/user.slice/user-<uid>.slice/user@<uid>.service/cgroup.subtree_control`; compare `cgroup_root` in `config.json` with the unit's `Delegate=yes` and `DelegateSubgroup=supervisor`. |
| `host-executable-digest-mismatch`, `host-executable-identity-mismatch`, `host-executable-invalid`, `host-executable-path-mismatch` | The installed file changed, is not root-owned, or the configuration does not select it with release trust and `managed_service: true`. | Rerun `agc host install --download` or `agc host upgrade --download`; check `native_broker` in `config.json`. |
| `host-drain-required` and the other `host-drain-*` codes | Activation on an existing spool needs the exact drain receipt. | `agc drain`, keep the ID, and let the upgrade pass it. |
| `namespace-mapping-failed` | The admitted worker could not create its user namespace. | Ubuntu's unprivileged-userns restriction is meant to be bypassed only by the enforced profile; confirm the profile is loaded and the broker is the installed root-owned release. |
| `namespace-propagation-mount-failed`, `namespace-cgroup2-mount-failed-errno-N`, `namespace-cgroup2-bind-failed-errno-N` | The private cgroup2 view could not be mounted; `N` is the Linux error number. | `errno 16` (`EBUSY`) is the collision the broker handles by bind-mounting its leaf; `errno 13` (`EACCES`) points at an AppArmor denial in `journalctl -k`; `errno 1` (`EPERM`) at a missing delegation. |
| `namespace-mount-failed` | The worker-side normalization of any of the above. | Read the broker's receipt for the specific code. |

## Enforcement receipts

`agc list` summarizes each row as `admission-only`, `applied`, `partial`, `unapplied`, or
`failed`; `agc --json show <id>` has the full receipt. Events are deduplicated facts, not raw
controller output.

| Event code | Meaning | Next action |
| --- | --- | --- |
| `cpu-throttled` | The job hit its `cpu.max` quota. | Raise the `cpu` claim, or accept the throttling. |
| `pids-limit-hit` | The job reached `pids.max`. | Raise the `pids` claim, or find the process leak. |
| `memory-high-throttled`, `memory-pressure` | Memory reclaim slowed the job; not an OOM. | Raise the claim if the job needs the room. |
| `memory-max-hit`, `memory-oom` | The hard limit was reached; `memory-oom` ended the group. | Raise `memory`, or shrink the job. |
| `memory-limit-impossible` | The claim cannot be honored inside the parent's limits. | Lower the claim or raise the delegated budget. |
| `swap-limit-hit`, `swap-disabled` | The swap envelope was reached, or the host has no swap. | Informational unless the job depends on swap. |
| `tmpfs-byte-limit-hit`, `tmpfs-inode-limit-hit` | The job filled its tmpfs scratch. | Raise `tmpfs` or `tmpfs_inodes`; the receipt's `peak` says what was used. |
| `storage-byte-limit-hit`, `storage-inode-limit-hit` | The job filled its project-quota scratch. | Raise the storage claims. |
| An `unapplied` event on a `best-effort` binding | The backend was unavailable; the job ran without that limit. | Fix the host if the limit matters; a `required` binding refuses instead. |
| `tmpfs-policy-incomplete`, `quota-policy-incomplete` | A scratch policy needs every one of its names declared. | Declare the complete policy the [resource contract](coordinator.md#bounded-tmpfs-scratch) lists. |

A job that declares no scratch policy receives no `TMPDIR`, `TMP`, or `TEMP` from AGCoord. A
tool that then writes to the system temporary directory is not misconfigured by AGCoord; it is
telling you the job needs a declared scratch claim.

## Everything else

The native broker's complete refusal families, including the `quota-*`, `tmpfs-*`, `io-*`,
`worker-*`, and `broker-*` codes that only appear in receipts or in `broker.log`, are frozen in
the [stable refusal envelope](native_broker.md#stable-refusal-envelope), and each resource
backend's section of the [coordinator contract](coordinator.md#repository-lanes-and-resources)
defines its own codes. If a code you see is not on this page, it is there.
