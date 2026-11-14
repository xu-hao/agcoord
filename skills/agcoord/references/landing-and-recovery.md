# Landing and recovery

## Gate and publish together

Use focused `agc run` checks during development, then one `agc land <pr> ... -- <gate>` request to validate and publish the exact head. `full` and `land` gates run in coordinator-owned detached worktrees, leaving the submitter's working tree alone during gating. The submitted branch/head remains unchanged until target synchronization is needed or publication completes; AGCoord merges an advanced target into the request branch mechanically and never rebases or rewrites commits.

For high-risk or cross-suite changes, an optional `agc full` before landing can find likely-red work before it enters a multi-PR landing batch. Add `--reuse-full` to `agc land` when reusing that result is safe. It reuses a passed receipt only for an exact head and identical gate command, same repository and worktree, and a one-PR landing whose target `main` is still an ancestor of `expected_head_sha`. A moved target changes the synchronized head and cannot reuse the old receipt.

Compatible queued lands may form a batch of `k` PRs. AGCoord merges candidates into one batch tree `T_k`, gates once, then atomically publishes all `k` if green. If the combined gate fails, it bisects `T_1..T_k` to find the first breaking PR, publishes the clean prefix `0..bad-1`, and requeues the untested suffix `bad+1..k`. A pre-land `agc full` on risky cross-suite work can expose breakage before that work triggers batch bisection, without adding a separate full gate to every routine clean PR.

## Exit 75 and landing handbacks

Exit 75 (`EXIT_COORDINATOR_UNREACHABLE`) means the client stream disconnected while the broker may still be running the job. Reconnect with `agc show <id>` or `agc log <id> --follow`; do not count it as a failed gate.

Apply the indicated recovery and submit a new landing request when needed:

- `stale-main`: merge the current target branch into your request branch, resolve conflicts, commit, and push; do not rebase.
- `merge-error`: fix the reported merge conflict or repository/adapter issue on the request branch, commit, and push.
- `avoided-commit`: create a fresh branch from the current target that does not reach the avoided commit, then reapply the work as new commits.
- `head-changed`: inspect the changed PR head, incorporate the intended changes into your branch, and push the resulting new head.
- `pr-not-ready`: make the PR ready for the adapter's landing requirements, then retry.
- `publish-failed`: inspect target and PR refs because the atomic publication did not complete; repair the branch/adapter state and submit a new landing request after confirming the intended head.
- `gate-failed`: fix the failing gate, commit and push the fix, then request a new landing.

Never rewrite existing commits to recover a handback. A normal target merge and push preserves their identities.
