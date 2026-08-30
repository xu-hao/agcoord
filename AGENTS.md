# AGCoord contributor instructions

Keep this file compact and broadly applicable. Component contracts and procedures belong in
the canonical documents listed by [docs/index.md](docs/index.md).

## Track and isolate changes

- Create a descriptive issue before changing code, tests, documentation, packaging, or live
  state. Pair every issue number with a sentence explaining the work it tracks.
- Make and validate the change in a ticket-specific Git worktree branched from `main`.
  Preserve unrelated edits and do not develop in the primary checkout.
- Keep the core coordinator forge-neutral. Forge-specific metadata and publication behavior
  belongs in an optional adapter.

## Test behavior and document contracts

- Reproduce a bug with a failing behavioral test before changing runtime code. Exercise
  public APIs, commands, subprocesses, Git repositories, and the real TUI; do not inspect
  source or documentation text to prove behavior.
- Every runtime change includes focused tests and updates affected canonical documentation.
  Register new documents in `docs/index.md` and record published user-facing changes in
  `CHANGELOG.md`.
- Tests own and stop every broker, worker, repository, and temporary state they start. Never
  inspect or clean another agent's state.

## Coordinate checks and publication

Once AGCoord is installed, every check and publication uses the local coordinator:

```bash
agcoord run --label "focused tests" --resource cpu=1 -- python -m pytest -q tests/test_area.py
agcoord full --label "standalone full validation" --resource cpu=4 -- python -m pytest -q
agcoord land <request> --label "gate and publish" --resource cpu=4 -- python -m pytest -q
```

- Declare every scarce resource a command consumes. A full gate is a barrier for its
  repository, not an undeclared machine-global lock.
- Use `agcoord full` from a clean checkout when an exact-head validation is useful without
  publication. It remains a repository barrier, but a separate full row followed by a merge
  is not the normal landing workflow.
- Push/open the publication request, then use one `agcoord land` request whose gate command
  validates that exact clean 40-character head and publishes it without releasing the lane
  or declared resources. Do not use direct target-branch pushes, forge merge commands, a
  full-plus-merge gap, or an equivalent path that separates the landing verdict from its
  publication.
- A stale-target or changed-head refusal hands the work back to the agent: update the branch
  explicitly, push, and submit a fresh `agcoord land` request. AGCoord never refreshes,
  rebases, or rewrites the worktree for you, and never reuses the previous gate result after
  either reference moves.
- Do not invoke `agcoord`, a gate wrapper, or publication from inside an admitted AGCoord
  job; nested submissions are rejected to prevent self-deadlock.
- Do not upload an AGCoord distribution to PyPI unless the user explicitly asks for that
  upload. Permission to implement, test, commit, push, open or merge a pull request, tag,
  or create a GitHub release is not permission to upload to PyPI.
- Remove the merged ticket worktree and branch only after publication succeeds.

Bootstrap exception: before AGCoord is installed in a fresh AGCoord development environment,
direct package installation and focused tests are allowed only long enough to make
`agcoord run` available; direct full gates and publication are never bootstrap shortcuts.

See [docs/coordinator.md](docs/coordinator.md) for scheduling, receipts, recovery, cleanup,
TUI, and migration details, and [docs/releasing.md](docs/releasing.md) for the release gate.
