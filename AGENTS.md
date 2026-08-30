# AGCoord contributor instructions

Keep this file limited to repository-wide guardrails. Follow the canonical contracts and
procedures listed in [docs/index.md](docs/index.md).

- Track every code, test, documentation, packaging, or live-state change with a
  descriptive issue. Pair every issue number with a sentence explaining the work it
  tracks.
- Develop and validate only in an issue-specific Git worktree branched from `main`.
  Preserve unrelated work and do not develop in the primary checkout.
- Verify behavior at public boundaries. Start bug fixes with a failing behavioral test;
  accompany runtime changes with focused tests, affected canonical documentation, and a
  `CHANGELOG.md` entry when the change is user-facing.
- Tests own and stop every broker, worker, repository, and temporary state they create.
  Never inspect or clean another agent's state.
- Keep the core coordinator forge-neutral. Forge-specific metadata and publication
  behavior belong in optional adapters.
- After bootstrap, route checks and publication through the local coordinator, declare
  every scarce resource, and never submit coordinated work from an admitted job. Gate
  and publish through one `agcoord land` request; never bypass or separate its verdict
  from publication.
- Never upload an AGCoord distribution to PyPI unless the user explicitly requests it.
  Permission to implement, test, commit, push, merge, tag, or create a GitHub release
  does not authorize an upload.
- Follow the [contributor workflow](docs/contributing.md) for commands, bootstrap,
  recovery, and cleanup. The [coordinator guide](docs/coordinator.md) and
  [release guide](docs/releasing.md) define the corresponding component contracts.
