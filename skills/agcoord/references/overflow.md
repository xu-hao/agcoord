# Portable overflow checks

## Eligibility and runtime requirements

When a checkout contains `.agcoord/overflow.json`, `agc run` opts into overflow by default. The local machine is preferred when it can satisfy the claim and capabilities; otherwise an eligible configured pool may run the job. Use `--no-overflow` to keep a check local. `--overflow` explicitly requires a valid overflow declaration. Runtime capability flags are `--require-capability <name>` (repeatable) and `--runtime <name>` (repeatable; `local` or a configured pool name). A single allowed runtime must offer the complete resource claim and every required capability.

## Write portable commands

Overflow workers receive a working-tree snapshot, not your host environment. Use repository-relative paths, bare executable names, and scratch under `$TMPDIR`. Declare scratch needs with `--resource tmpfs=<bytes>` and, when relevant, `--resource tmpfs_inodes=<count>`. Do not depend on host `/var/tmp` or create wrapper scripts there.

Pass a multi-line script already in the checkout with `--script <file>`, or use an inline `bash -ec '...'` command for a short script. Host virtual environments, `.git`, local services, ignored files, and host daemons are unavailable in overflow snapshots.

Keep tiny checks that inspect `.git` or need host state separate and local, for example `agc run --no-overflow --resource cpu=1 -- <check>`. Submit heavy portable test suites separately so they can overflow when local capacity is full.
