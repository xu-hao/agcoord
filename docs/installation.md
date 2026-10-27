# Installation, upgrades, and uninstallation

AGCoord is composed of two components:

1. A Python client package (`agcoord`, providing the `agc` command) that agents and users interact
   with.
2. A standalone native broker executable (`agcoord-broker`) that manages queue concurrency,
   repository lanes, lease allocations, process lifecycles, and atomic landing.

AGCoord supports two deployment models:

- **User mode (unmanaged / no-root)**: Runs on any x86_64 Linux machine without root privileges.
  The broker executable is placed in `~/.local/libexec/agcoord/agcoord-broker`, runs on demand as a
  normal user process without systemd services, and uses the spool at `~/.local/state/agcoord`.
  This mode provides queue coordination, task history, and atomic landing, but does not enforce
  hardware cgroup limits.
- **Managed mode (enforced host)**: Supported on Ubuntu 24.04-class hosts with unified cgroup v2
  (`nsdelegate`), AppArmor ABI 4, `kernel.apparmor_restrict_unprivileged_userns=1`, and systemd 254
  or newer. The broker is installed as a root-owned binary at `/usr/libexec/agcoord/agcoord-broker`,
  runs as a managed systemd user service (`agcoord-broker.service`), and executes under an
  enforcing AppArmor profile that guarantees hardware isolation (CPU, memory, tmpfs, project quota,
  and IOPS limits).

---

## 1. Install the Python client

Install `agcoord` in a Python 3.10+ virtual environment, or globally with `pipx`:

```bash
# In an activated virtual environment:
python -m pip install agcoord

# Or using pipx for an isolated command:
pipx install agcoord
```

### Pytest distributed testing extra

If you plan to run distributed pytest suites coordinated through AGCoord, install the `xdist` extra:

```bash
python -m pip install 'agcoord[xdist]'
```

Verify that the CLI is accessible:

```bash
agc --version
```

---

## 2. Install the broker

The Python client and broker executable ship at the same stable version. The client embeds the
exact SHA-256 checksum of the matching native release broker and will refuse any broker whose
identity does not match.

### Option A: User mode (no root required)

To set up an unmanaged user broker on any x86_64 Linux host:

```bash
agc host install --user
```

This command:
1. Downloads the standalone `agcoord-broker` binary and its checksum sidecar from the release assets
   matching the installed client version.
2. Verifies the downloaded binary against the client's internally pinned SHA-256 digest.
3. Installs the binary to `~/.local/libexec/agcoord/agcoord-broker` with permissions `0755`.
4. Creates the default state directory at `~/.local/state/agcoord` with a configuration declaring
   detected CPU capacity and `managed_service: false`.

Verify the installation:

```bash
agc list
```

### Option B: Managed host mode (hardware-enforced limits)

To enable hardware-enforced resource quotas on a supported Ubuntu 24.04+ system, run the privileged
installer. The default spool must be empty (drain and move any existing `~/.local/state/agcoord`
aside first):

```bash
agc host install --download
```

This command:
1. Downloads the verified native host release bundle matching the client version.
2. Stages the package and prompts for `sudo` to perform privileged activation.
3. Installs the broker binary to `/usr/libexec/agcoord/agcoord-broker` owned by `root:root`.
4. Installs and enforces the AppArmor profile at
   `/etc/apparmor.d/usr.libexec.agcoord.agcoord-broker`.
5. Installs and starts the systemd user service `agcoord-broker.service`.
6. Executes an internal verification probe that verifies AppArmor confinement, user-namespace
   delegation, and cgroup v2 CPU throttling before reporting success.

For air-gapped or offline hosts, supply a pre-downloaded bundle archive instead of `--download`:

```bash
agc host install /path/to/agcoord-native-host-x86_64-unknown-linux-musl-0.7.5.tar.gz
```

---

## 3. Upgrading AGCoord

A client commands only a broker from its own exact release line. When upgrading the Python client,
always upgrade the broker to match.

### Upgrading the Python client

```bash
python -m pip install --upgrade agcoord
# Or if using pipx:
pipx upgrade agcoord
```

### Upgrading User mode

In user mode, reinstalling the broker pulls the matching release binary:

```bash
# 1. Drain pending work (optional, allows running tasks to complete cleanly)
agc drain --reason "upgrading broker"

# 2. Fetch and install the matching release broker
agc host install --user

# 3. Resume the queue if drained
agc resume <drain_id>
```

### Upgrading Managed host mode

Managed host upgrades are atomic and preserve running work without dropping queue state:

```bash
agc host upgrade --download
```

`agc host upgrade` automatically:
1. Drains incoming submissions while allowing in-flight work to complete.
2. Acquires the native host maintenance lock.
3. Replaces root-owned broker binaries and systemd units via `sudo`.
4. Reloads the systemd user daemon and restarts `agcoord-broker.service`.
5. Verifies the newly running broker's identity and runs the enforcement proof.
6. Resumes the queue with the retained drain ID.

---

## 4. Uninstallation and removal

### Removing User mode

To cleanly remove an unmanaged user-mode installation:

```bash
# 1. Drain and stop any running broker process
agc drain --reason "uninstalling"

# 2. Remove the spool and the broker executable
rm -rf ~/.local/state/agcoord ~/.local/libexec/agcoord

# 3. Uninstall the Python client
python -m pip uninstall -y agcoord
# Or if installed via pipx:
pipx uninstall agcoord
```

### Removing Managed host mode

To cleanly uninstall AGCoord and all host integration from a managed host:

```bash
# 1. Drain any active or queued work
agc drain --reason "uninstalling"

# 2. Stop and disable the systemd user service
systemctl --user disable --now agcoord-broker.service
systemctl --user daemon-reload

# 3. Unload and delete the AppArmor profile (requires sudo)
sudo apparmor_parser -R /etc/apparmor.d/usr.libexec.agcoord.agcoord-broker 2>/dev/null || true
sudo rm -f /etc/apparmor.d/usr.libexec.agcoord.agcoord-broker

# 4. Remove root-owned binaries, systemd units, and package cache (requires sudo)
sudo rm -f /usr/lib/systemd/user/agcoord-broker.service
sudo rm -rf /usr/libexec/agcoord /usr/share/doc/agcoord /var/lib/agcoord

# 5. Remove the user spool directory
rm -rf ~/.local/state/agcoord

# 6. Uninstall the Python package
python -m pip uninstall -y agcoord
# Or if installed via pipx:
pipx uninstall agcoord
```

---

## Related documentation

- [Quickstart](quickstart.md) — Two-minute hands-on walkthrough.
- [Native host deployment runbook](native_host.md) — Detailed host requirements, offline
  packaging, systemd unit definitions, AppArmor policies, and guarded recovery.
- [Troubleshooting](troubleshooting.md) — Resolving installation, upgrade, or host probe refusals.
