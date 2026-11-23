---
name: agcoord
description: Use agcoord (agc) to run resource-bounded local or overflow checks, size CPU/memory/tmpfs claims, and land pull requests through atomic or batched gates without direct merges.
---

# Use agcoord

1. Coordinate each check with `agc run --label "<short plain-English summary>" --resource cpu=<n> [--resource NAME=<units>] -- <command>`. Declare the CPU, memory, and scratch it needs. Never invoke `agc` recursively inside an admitted job; nested submissions are rejected when `AGCOORD_RUN_ID` is set.
2. Write human-readable labels: `--label` is required on `agc run`, `agc full`, and `agc land` and is read by humans in `agc list` and `agc tui`. Lead with a short plain-English summary of *what* is being checked or landed (for example, `--label "API unit tests"` or `--label "land #123 retry budget"`). Do **not** stuff commit SHAs, branch names, `"pre-land"` stage tags, or raw directory paths into `--label`—`KIND`, `REPO`, `BRANCH`, and `HEAD` already have separate columns, and leading boilerplate truncates the subject in narrow TUI views. Set `AGCOORD_AGENT` or pass `--agent` when you have a session or worker name.
3. Size claims from `agc --json list` (`capacities`) and `agc --json show <id>` (`resource_receipt`). Divide a job's CPU budget among concurrent tools with `agcoord-xdist` or `ChildCpuLease`.
4. Keep `agc run` commands portable so they can overflow when eligible. See [overflow guidance](references/overflow.md).
5. Land pull requests with one guarded `agc land <pr> --label "<short plain-English summary>" --resource cpu=<n> [--resource NAME=<units>] -- <gate>` request. The usual flow is focused `agc run` checks, then `agc land`. For high-risk or cross-suite changes, optionally run `agc full --label "<short plain-English summary>" --resource cpu=<n> -- <gate>` first and add `--reuse-full` to `agc land` when the head and external gate dependencies remain unchanged. See [landing and recovery](references/landing-and-recovery.md).
6. Treat exit 75 as a client stream disconnect, not a failed gate. Reconnect with `agc show <id>` or `agc log <id> --follow`. Follow structured landing handbacks without rewriting commits.
7. A check can be preempted: stopped and requeued, at most twice, so that an older job can start. `agc run` keeps waiting and returns the final attempt, so never resubmit a check that is still queued or running. Pass `--no-preempt` to a check that is not safe to run twice or to restart after being killed (for example, one that can leave a stale `.git/index.lock`).
