# Migrating a pre-native spool

AGCoord 0.6.0 retired the Python reference broker and its in-process migrations. The native
Rust broker owns every coordinator spool at protocol 5, and the client no longer carries any
protocol 1–4 migration or rollback machinery.

## Refusal

A current client that meets a state directory left at protocol 1 through 4 by an AGCoord
release before 0.6.0 refuses every command before starting or claiming a broker, and names the
release that migrates it. An un-migrated spool is never silently touched.

## Migrate through 0.5.2

**AGCoord 0.5.2** is the last release that shipped the protocol 1–4 migrations and the
protocol 4 → 5 native transition. To bring a pre-native spool forward:

1. Install AGCoord 0.5.2 (`pip install 'agcoord==0.5.2'`).
2. With the spool idle, `agc drain` it, run `agc migrate` until it reports protocol 5, then
   `agc host upgrade` to install the native host. The 0.5.2
   [native migration runbook](https://github.com/xu-hao/agcoord/blob/v0.5.2/docs/native_migration.md)
   describes the backup, rollback rehearsal, and retained-window steps in full.
3. Upgrade to the current AGCoord; its client now owns the protocol-5 spool directly.

## Roll back

Rollback also lives in 0.5.2. To undo a 0.5.2 migration inside its retained window, reinstall
AGCoord 0.5.2 and follow its rollback command; a current AGCoord cannot roll a spool back below
protocol 5.
