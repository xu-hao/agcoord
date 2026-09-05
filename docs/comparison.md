# Where AGCoord sits

Most tools near AGCoord live either above it, managing agent sessions and worktrees, or beside
it, gating merges on hosted CI. None of them schedules the machine, and none combines
machine-level admission with a local landing barrier. This page is organized by the question a
reader arrives with: I already use this, so what does AGCoord add, and does anything conflict?
Figures are from each project's repository or documentation in early September 2026.

| If you use | What it coordinates | Machine resources | Merge gating | Runs |
| --- | --- | --- | --- | --- |
| GitHub merge queue, Mergify, Aviator, Graphite | Pull requests in one repository | No | Yes, on hosted CI | Cloud |
| pueue, task-spooler, nq | Shell commands on one machine | Parallel count only | No | Local |
| Claude Squad, Conductor, Vibe Kanban, Nimbalyst | Agent sessions and worktrees | No | Manual | Local |
| Agent Orchestrator, OpenAI Symphony | Agents driven from tasks or tickets | No | A human, or CI, decides | Local |
| Gas Town | A whole agent workforce, with a merge queue | Session count cap | Yes, an LLM session runs the queue | Local |
| Container and sandbox limits | One container or sandbox | Static per container | No | Local or cloud |
| act, Dagger | One pipeline run | No | No | Local |
| **AGCoord** | Jobs from any agent, tool, or person | Declared and enforced | Yes, atomic with the gate | Local |

## I use GitHub's merge queue or Mergify

[GitHub's merge queue](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/configuring-pull-request-merges/managing-a-merge-queue)
builds temporary `gh-readonly-queue/*` branches, groups one to a hundred pull requests, runs up
to a hundred concurrent builds, removes a failing request from the queue, and needs your CI to
report on `merge_group` events. [Mergify](https://docs.mergify.com/merge-queue/) is the same
idea as a service, with speculative checks up to 128 wide, batching it advertises as "50–80%
fewer CI runs", two-step CI, priorities, a freeze switch, and path-scoped queues across GitHub
Actions, Buildkite, and Jenkins. Aviator and Graphite sell variations. bors-ng, the open-source
ancestor, was archived in April 2024.

All of them run where CI runs. They solve stale evidence for the repository's canonical gate
and know nothing about the workstation where the agents live, and their latency is CI latency.
AGCoord governs the local loop instead: the gate is a shell command on the machine, admitted by
declared resources, and the merge happens in the same row.

The two are complementary across a repository and exclusive on one branch. Required checks on
a pull request stay in force whichever tool merges. But a branch that requires a hosted merge
queue rejects a direct update of the branch reference for anyone without bypass rights, and
AGCoord's publication is exactly such an update. Pick one per branch: AGCoord where the real
gate runs on the machine, the hosted queue where the real gate runs in CI. `agc drain` is
AGCoord's equivalent of a queue freeze.

## I use pueue, task-spooler, or nq

[pueue](https://github.com/Nukesor/pueue) (Rust, 6.3k stars, declared feature-complete) is a
daemon that processes a queue of shell commands with named groups, per-group parallelism,
dependencies, pause and resume, and persistent logs. task-spooler is the same shape in C with
one knob, the number of simultaneous jobs; a GPU fork adds device allocation.
[nq](https://github.com/leahneukirchen/nq) (3.1k stars) needs no daemon and runs a directory
queue sequentially. GNU parallel can gate its own workers on host load and free memory with
`--load` and `--memfree`, but only inside one invocation. Slurm and HTCondor are the real
ancestors, with declared resources, cgroup enforcement, accounting, and fair share, at the
cost of a multi-daemon cluster install and no notion of Git.

pueue is the right mental model for AGCoord's queue half. The differences are the ones that
matter once the submitters are agents: resources are typed, declared by independent
submitters, and enforced by the kernel rather than counted; jobs belong to repository lanes;
and there is a landing step. For readers with a cluster background: Slurm's admission model
shrunk to one user and one machine, with Git lanes and an atomic merge added.

## I use Claude Squad, Conductor, Vibe Kanban, or Nimbalyst

[Claude Squad](https://github.com/smtg-ai/claude-squad) (Go, AGPL-3.0, 8.4k stars) gives
each agent a tmux session and a worktree, supports Claude Code, Codex, Gemini, and Aider, and
pushes a branch with one key; you open and merge the pull request.
[Conductor](https://www.conductor.build/docs/) is a macOS app that runs Claude Code, Codex,
Cursor, and OpenCode in isolated workspaces, each with a branch, terminal, diff, and review
path, and helps you "review the diff, open a pull request, merge, and archive the workspace".
[Vibe Kanban](https://github.com/BloopAI/vibe-kanban) (Rust, Apache-2.0, 28k stars) puts
tasks on a board, runs each in a worktree with a dev-server preview, and creates and merges
pull requests from the board; it is sunsetting after its company's April 2026 shutdown and is
community-maintained. [Nimbalyst](https://nimbalyst.com/), Crystal's successor, is an MIT
desktop app with a kanban of sessions and opt-in worktrees, free for individuals.
[Agent Orchestrator](https://aoagents.dev/) adds a main agent that plans work and spawns
workers into worktrees, opens pull requests, and routes failed checks back to the owning
session until a human merges. [OpenAI Symphony](https://github.com/openai/symphony) (Elixir,
Apache-2.0, 27k stars) turns Linear issues into isolated Codex runs and calls itself "a
low-key engineering preview". Claude Code itself has `--worktree` since v2.1.49, subagents
with worktree isolation, and agent teams with a shared task list and file locking; its
sandbox isolates the filesystem and network and imposes no CPU, memory, or process limits.

Every one of these stops at the same line: each agent gets a worktree, and a human or a hosted
queue decides what merges. None schedules the machine. Augment Code's
[survey of nine orchestrators](https://www.augmentcode.com/tools/open-source-agent-orchestrators)
says it flatly: "None explicitly document CPU or memory limits." They compose with AGCoord in
two places: the agents inside them read the repository's instruction file, so the paragraph in
the [agent guide](agents.md) reaches them unchanged, and wherever the tool takes a shell
command for setup, tests, or checks, that command can be `agc run`.

## I use Gas Town

[Gas Town](https://github.com/steveyegge/gastown) is the comparison people make, so it gets a
precise answer. It is Steve Yegge's multi-agent workspace manager: Go, MIT, created December
2025, 17.9k stars and 1,654 forks, latest release v1.2.1 in June 2026. Its author scopes it to
developers already running five or more agents a day and aims it at twenty to thirty Claude
Code sessions at once.

The two tools answer different questions. Gas Town asks how to run a workforce of agents and
keep their work from getting lost: it assigns work through its Beads ledger and convoys, gives
agents persistent identities and mail, patrols them with a Witness and a Deacon, and lands
their output through a Refinery. AGCoord asks how to keep the agents you already run from
wrecking the machine and `main`, and has no opinion about how agents are launched, tracked,
or messaged. The two overlap in exactly one organ, the merge queue, and differ completely in
the layer below it.

| | Gas Town Refinery | AGCoord `agc land` |
| --- | --- | --- |
| What performs the landing | A Claude Code session following a fourteen-step [patrol prompt](https://github.com/steveyegge/gastown/blob/main/internal/formula/formulas/mol-refinery-patrol.formula.toml) with Go helpers. Its [role prompt](https://github.com/steveyegge/gastown/blob/main/internal/templates/roles/refinery.md.tmpl) says "You NEVER write application code. You merge branches mechanically." | A deterministic worker owned by the native broker. No model in the trust path. |
| How work arrives | A polecat runs `gt done`; the Refinery wakes on events with 30 s to 15 min backoff. | Any agent or person runs `agc land <pr> -- <gate>`; the row is admitted when its lane and resources allow. |
| Bringing the branch up to date | A merge rehearsal of the target into a temp branch; the role prompt instead says rebase then fast-forward. | One `--no-ff` merge of the current target into the request branch, pushed with a lease and recorded as the durable head. Never rebases or rewrites. |
| Tests | Rig-configured commands. Flaky tests retry. A "pre-verified" request with a matching base skips all gates; `run_tests = "false"` skips them entirely. | The gate command is part of the request and runs exactly once. A verdict is never reused after either ref moves. |
| When tests fail | The agent judges pre-existing versus branch-caused; pre-existing means file a bug and merge anyway. | `gate-failed`. Nothing is published. |
| Publishing | Default: `git merge --no-ff` and `git push` to the target, serialized by a merge slot. PR mode: `gh pr create`, wait for CI, require approval, `gh pr merge`. | One atomic compare-and-update of target and source on the forge, in the same row that ran the gate. |
| Conflicts | Abort, file a task for another agent, keep the request queued. | Abort before the gate, restore the checkout, hand back with the conflicting paths. |
| Forges | Direct git push, GitHub, Bitbucket. | GitHub adapter today. |
| Idle cost | Tokens on every patrol wake. | None. |

Below the merge queue, Gas Town governs capacity by counting sessions: `scheduler.max_polecats`
caps how many agents run. Its own [scheduler design](https://github.com/steveyegge/gastown/blob/main/docs/design/scheduler.md)
gives the reason: "Without the scheduler, slinging N beads spawns N polecats simultaneously,
exhausting API rate limits, memory, and CPU." There is no host awareness, no per-job CPU or
memory limit, no scratch accounting, and no process-group ownership. Its
[sandboxing proposal](https://github.com/steveyegge/gastown/blob/main/docs/design/sandboxed-polecat-execution.md),
still a design, concedes that "a developer laptop cannot sustain 10–20 simultaneous Claude
sessions without resource contention". One
[week-long trial](https://tenzinwangdhen.com/posts/gastown-good-bad-ugly/) reported 141
orphaned Claude Code processes and memory pressure on a 32 GB laptop before its author decided
not to adopt it.

That layer is AGCoord's ground: declared CPU, memory, tmpfs, inode, and process budgets
enforced by cgroup v2; a job killed as a whole process group on cancel or finish; crash
recovery that adopts only identity-verified workers; and zero tokens spent on coordination.
AGCoord does not manage agent sessions at all, which is the mirror image of Gas Town's gap.
Gas Town's rig gate commands are plain shell strings, so a rig can route its tests through
`agc run` today; its landing path is not pluggable, so the atomic half would need the Refinery
to delegate publication.

## I run agents in containers or sandboxes

Docker's `--cpus`, `--memory`, and `--pids-limit`, dev containers, and cloud sandboxes such as
Daytona, E2B, and Modal cap one container. They answer what an agent may touch and how much
one box may use. They do not answer when a job may start: five containers each capped at four
CPUs on an eight-core machine still oversubscribe it, and nothing queues the sixth. Claude
Code's own sandbox restricts filesystem paths and network hosts and sets no resource limits.
The [AgentCgroup](https://github.com/eunomia-bpf/agentcgroup) research prototype, which
applies cgroup v2 controls per tool call with eBPF and sched_ext, shows the resource problem
has reached the academic literature.

Sandboxes and AGCoord compose. The sandbox draws the boundary; AGCoord decides admission
inside it and enforces the declared budget with the same cgroup machinery. The
[agent guide](agents.md) covers the one integration detail, allowing the coordinator's state
directory inside a sandboxed shell.

## I run act or Dagger

[act](https://github.com/nektos/act) (71.8k stars) runs GitHub Actions workflows locally in
Docker, one invocation at a time. [Dagger](https://github.com/dagger/dagger) (16k stars) runs
programmable pipelines on a local engine with caching. Both run a pipeline; neither decides
when pipelines may run across several submitters, and neither lands anything. They make good
gate commands: `agc run -- act`, or `agc land 123 -- dagger call test`.

## What the landscape says

No one occupies the combination. The nearest real alternatives are GitHub's merge queue plus
hand-written agent discipline, and Gas Town. The session managers are where AGCoord's users
already are, and each has a place to put a shell command. Two exclusions are worth stating
once: not with a hosted merge queue on the same branch, and not without allowing the state
directory inside a sandbox.
