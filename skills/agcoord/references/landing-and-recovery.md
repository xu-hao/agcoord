# Landing and recovery

## Gate and publish together

Use focused `agc run --label "<short plain-English summary>" ...` checks during development, then one `agc land <pr> --label "<short plain-English summary>" --resource cpu=<n> ... -- <gate>` request (for example, `--label "land #123 retry budget"`) to validate and publish the exact head. `full` and `land` gates run in coordinator-owned detached worktrees, leaving the submitter's working tree alone during gating. The submitted branch/head remains unchanged until target synchronization is needed or publication completes; AGCoord merges an advanced target into the request branch mechanically and never rebases or rewrites commits.

For high-risk or cross-suite changes, an optional `agc full --label "<short plain-English summary>" --resource cpu=<n> -- <gate>` before landing can find likely-red work before it enters a multi-PR landing batch. Add `--reuse-full` to `agc land` when reusing that result is safe. It reuses a passed receipt only for an exact head and identical gate command, same repository and worktree, and a one-PR landing whose target `main` is still an ancestor of `expected_head_sha`. A moved target changes the synchronized head and cannot reuse the old receipt.

Compatible queued lands form adaptive power-of-two batches ($B \in \{1, 2, 4, 8\}$, clamped by `land_max_batch_size` and falling back to $B = 1$ above ~38% recent repository failure rate). AGCoord merges candidates into one batch tree `T_k`, gates once, then atomically publishes all `k` if green. For multi-PR slices (`k >= 2`), AGCoord sets `AGCOORD_BATCH_ATTRIBUTION_FILE` to a temporary path outside the checkout where dependency-aware runners may write an optional `agcoord-failure-attribution-v1` JSON receipt (`{"format": "agcoord-failure-attribution-v1", "failures": [{"unit": "<id>", "causal_paths": ["<canonical-repo-relative-file-or-dir>", ...]}]}`). Uniquely blamed culprits (`|M_f| == 1` against `git diff --name-only <T_0>...<head>`) are evicted immediately as `gate-failed` (`attributed-gate-failed` in `agc show` / `agc tui`) while remaining PRs proceed; missing or ambiguous receipts fall back to deductive power-of-two bisection (`--no-deductive-bisect` forces explicit singleton gates). Running `agc full` beforehand on risky cross-suite work can expose breakage before that work enters a batch.

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
