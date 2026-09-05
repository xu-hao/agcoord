# AGCoord contributor instructions

Keep this file limited to repository-wide guardrails. Follow the canonical contracts and
procedures listed in [docs/index.md](docs/index.md).

- Track every code, test, documentation, or packaging change with a descriptive issue.
  Pair every issue number with a sentence explaining the work it tracks. Do not file an
  issue solely for machine-local installation, deployment, configuration, service
  lifecycle, or other operator state; create one only when that work also requires a
  repository change or the user explicitly requests an issue.
- Develop and validate only in an issue-specific Git worktree branched from `main`.
  Preserve unrelated work and do not develop in the primary checkout.
- Verify behavior at public boundaries. Start bug fixes with a failing behavioral test;
  accompany runtime changes with focused tests, affected canonical documentation, and a
  `CHANGELOG.md` entry when the change is user-facing.
- A documentation-only change needs no tests or AGCoord gate. It qualifies only when every
  changed path is `README.md`, `CHANGELOG.md`, `AGENTS.md`, Markdown below `docs/`, or a
  `.png` or `.svg` image below `docs/assets/` whose provenance is recorded in
  [docs/assets/README.md](docs/assets/README.md). Verify the exact pull-request file list and
  diff, then merge it through the forge; the issue, worktree, branch, pull-request, and
  no-direct-target-push rules still apply.
- Tests own and stop every broker, worker, repository, and temporary state they create.
  Never inspect or clean another agent's state.
- Keep the core coordinator forge-neutral. Forge-specific metadata and publication
  behavior belong in optional adapters.
- For every other change after bootstrap, route checks and publication through the local
  coordinator, declare every scarce resource, and never submit coordinated work from an
  admitted job. Gate and publish through one `agc land` request; never bypass or separate
  its verdict from publication.
- Never upload an AGCoord distribution to PyPI unless the user explicitly requests it.
  Permission to implement, test, commit, push, merge, tag, or create a GitHub release
  does not authorize an upload.
- End changed work with the four-part [session handoff](docs/session_handoff_format.md);
  answer read-only questions directly without an empty handoff shell.
- Follow the [contributor workflow](docs/contributing.md) for commands, bootstrap,
  recovery, and cleanup. The [coordinator guide](docs/coordinator.md) and
  [release guide](docs/releasing.md) define the corresponding component contracts.
