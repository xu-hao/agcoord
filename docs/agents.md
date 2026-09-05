# Using AGCoord from coding agents

AGCoord has no agent-specific API. An agent uses it the way a person does, through the `agc`
command, and the coordinator does not care which model, tool, or terminal submitted a job.
What is specific to agents is how they adopt a tool: they follow the instruction file a
repository gives them and the rules they can read back from the environment. This page is
that instruction file's source material. It gives the paragraph to paste into `CLAUDE.md` or
`AGENTS.md`, explains the rules behind it, lists what an agent sees when the coordinator
refuses or hands work back and what to do next, and covers sandboxed shells. The
[coordinator contract](coordinator.md) remains the authority for every guarantee mentioned.

## The paragraph to paste

The short form covers a repository whose agents run checks and land pull requests:

```text
Run every check through the local coordinator and declare what it uses:
  agc run --label "<what>" --resource cpu=2 -- <command>
Land a pull request only through one gate-and-publish request; never merge directly:
  agc land <pr> --resource cpu=4 -- <full test command>
Never run agc from inside an admitted job. A stale-main or head-changed refusal means:
update the branch, push, and submit a new land request.
```

The long form suits a repository with tickets, several agents, and an enforced host:

```text
## Coordinate every check and landing

Run every check through the local coordinator and declare the resources it uses:
  agc run --label "<ticket> <what>" --resource cpu=<n> -- <command>
Use `agc full` for a clean exact-head verdict when you need one without publishing.
Land only through one gate-and-publish request, from a clean checkout of the pushed
head, after the pull request is open and ready:
  agc land <pr> --label "<ticket> land" --resource cpu=<n> -- <full test command>
Never use `gh pr merge`, a direct push to the target branch, or a separate full-then-merge
sequence; the coordinator's verdict and the merge are one step.
Never run `agc` from inside an admitted job; nested submissions are refused.
A `stale-main` or `head-changed` handback means: update the branch from the target, push,
and submit a new land request. AGCoord never rebases or rewrites your commits.
Set `AGCOORD_AGENT` to your session name so the queue shows who submitted what.
```

Fill in the resource names your broker configures. `agc --json list` prints them under
`capacities`, and a claim on a name the broker does not know is refused rather than queued.

## The three rules, and why they exist

**Declare what you use.** Admission is by declared resources. A job that claims less than it
uses starves the jobs admitted beside it; a job that claims more waits longer than it needs
to. On an enforced host the claim is also the limit: `cpu` becomes a `cpu.max` quota, so a
job that declared two CPUs and runs eight threads is throttled rather than killed, while a
job that exceeds a declared `memory` claim is ended as one process group. Scratch is opt-in.
A job that declares neither a tmpfs nor a project-quota policy receives no temporary directory
from AGCoord, and inherited `TMPDIR`, `TMP`, and `TEMP` values are removed, so a job that
needs temporary space declares the scratch names its broker binds. The
[resource contract](coordinator.md#repository-lanes-and-resources) lists the kinds.

**Never submit from inside an admitted job.** A job that submits another job and waits for
it would deadlock behind itself, so the client refuses before any broker is involved:

```text
error: a coordinated job cannot submit another coordinated job; invoke it directly from the checkout
```

Inside a job, run tools directly. The only `agc` calls a job should make are
`agc show "$AGCOORD_RUN_ID"` for its own row and `agc verify-admission` in a gate wrapper;
other reads may be refused on an enforced host, where the admitted namespace cannot see the
broker's owner. The three values every admitted job receives, `AGCOORD_RUN_ID`,
`AGCOORD_RUN_KIND`, and `AGCOORD_STATE_DIR`, are described under
[admission context](coordinator.md#admission-context-for-repository-wrappers).

**Land through one request.** `agc land` is the only path from a green gate to a merged
pull request that the coordinator can vouch for. It holds the repository lane and the declared
resources from preflight through publication, runs the gate exactly once against the exact
head it publishes, and never reuses a verdict after either the target or the source moves. A
`gh pr merge`, a direct push, or a separate full-then-merge sequence reopens the gap between
verdict and publication that the tool exists to close. The
[atomic landing contract](coordinator.md#atomic-landing) describes each phase.

## What the agent sees, and what to do next

Submission refusals arrive as an `error:` line and a non-zero exit before any row exists.
The [troubleshooting page](troubleshooting.md) covers every other refusal, including host
and enforcement codes; this table keeps to what an agent meets while working.
Landing handbacks arrive as a failed row whose `failure_reason` carries a stable code; read it
with `agc --json show <id>` rather than parsing the log, and read the log for the detail.

| What appears | Meaning | Next action |
| --- | --- | --- |
| `error: … is not inside a Git repository; agc schedules work per repository and worktree, so run it from a checkout or pass --checkout PATH` | The working directory is not in a Git repository. | Run from inside the checkout, or pass `--checkout PATH`. |
| `error: checkout is dirty; commit or remove changes before a full run` | `full` and `land` bind an exact clean head. | Commit or remove the changes; for `land`, push the head first. |
| `error: resource 'gpu' has no configured machine capacity` | The broker's `config.json` has no such capacity. | Claim only configured names; ask the operator to add one. |
| `error: resource 'cpu' requests 99, above capacity 4` | The claim can never be admitted. | Lower the claim; `agc --json list` shows `capacities`. |
| `error: a coordinated job cannot submit another coordinated job; …` | Nested submission. | Run the tool directly inside the job. |
| `error: … is not the broker this agc <version> client was released with …` | A user-owned broker no longer matches the client's pin, usually after a client upgrade. | Run `agc host install --user` again; set `native_broker.allow_development` only for a build from source. |
| A refusal with code `broker-draining` | Maintenance is closing the queue. | Wait; `agc list`, `show`, and `log` still work. |
| `Gate queue: <id> waiting at position N for branch …` | Queued, not refused. | Wait; `blocked_by` in `agc --json show <id>` names the jobs ahead. |
| `Gate queue: lost contact with the coordinator while following <id>: …`, exit status 75 | This client's stream ended; the job continues on the broker. | Read the verdict with `agc show <id>`; keep watching with `agc log <id> --follow`. Do not report 75 as a failed gate. |
| `failure_reason: gate-failed` | The gate command exited non-zero; `gate_exit_status` has the status. | Fix the code, push, and submit a new land request. |
| `failure_reason: stale-main` | The target moved after preflight or during the gate, or `--no-target-sync` met an advanced target. | Update the branch from the target, push, and submit a new land request. |
| `failure_reason: head-changed` | The source branch moved while the landing ran. | Do not push to a branch that is landing; submit a new request for the new head. |
| `failure_reason: pr-not-ready` | The pull request is closed, a draft, on another base, or not at this head. | Fix the pull request, make its head match the local head, and resubmit. |
| `failure_reason: merge-error` | The pre-gate target merge conflicted; the checkout is restored and the log names the paths. | Merge the target locally, resolve, push, and resubmit. |
| `failure_reason: avoided-commit` | The branch reaches a commit the operator removed from the target. | Rebuild the request as a fresh branch from the current target and resubmit. |
| `failure_reason: publish-failed` | The forge rejected the atomic update; nothing moved. | Check `gh auth status`, branch protection, and the pull request, then resubmit. |
| `status: interrupted` | The worker vanished before reporting; no verdict is claimed. | Read the log; unless it says `LANDED`, submit again. |
| `status: cancelled` | Someone ran `agc cancel`, or a graceful broker stop reaped a queued or gating job. | Resubmit when appropriate. |

Two habits keep handbacks rare. Push the head you intend to land and then leave the branch
alone until the row is terminal. And let the default target synchronization do its work: when
`main` advanced while the request waited, the landing merges it into the branch with an exact
lease before the gate, so `stale-main` normally appears only when the target moves again
during the gate itself.

## Name the agent

Set `AGCOORD_AGENT` in each agent's environment, or pass `--agent`, so every row records who
submitted it. Without it the identity is the single value `unnamed`. Labels are free text and
are the second thing a human reads in `agc list` and the TUI, so start them with the ticket
or task name. The TUI's `a` and `p` keys filter by agent and repository.

## Size claims from receipts

Start with a generous claim, then read what the job actually used. `agc --json show <id>`
returns a `resource_receipt` with `requested`, `applied`, `peak`, and `events`. Under
enforcement, `peak` is the backend's measurement in the bound unit, such as bytes for memory
or tmpfs, and `events` records throttling or limit hits with stable codes. A CPU peak far
below the claim means the next claim can shrink; a throttle event means it should grow.
Admission-only names carry empty `applied`, `peak`, and `events` fields.

## Parallel tools inside one job

One job owns one CPU budget for its entire process tree. Tools that fan out must divide it
rather than each assuming the whole machine. The public
[child CPU lease API](coordinator.md#child-cpu-leases-for-parallel-tools) does that for any
tool, and the optional [pytest-xdist adapter](coordinator.md#optional-pytest-xdist-adapter)
applies it to pytest: with `agcoord[xdist]` installed, `-n auto` inside an admitted run starts
exactly the granted number of workers, and a distributed mode without a `cpu` claim on the
job fails clearly before collection. When a gate starts several controllers, cap each with
`--maxprocesses` so they leave room for one another.

## Sandboxed agent shells

Agent sandboxes confine where a command may write and which hosts it may reach. AGCoord's
client talks to its broker through a SQLite spool in the state directory, which is
`${XDG_STATE_HOME:-~/.local/state}/agcoord` unless `AGCOORD_STATE_DIR` selects another.

**Claude Code.** Its sandbox lets commands write to the working directory and the session
temp directory. Add the state directory to the writable paths in your settings:

```json
{
  "sandbox": {
    "enabled": true,
    "filesystem": {
      "allowWrite": ["~/.local/state/agcoord"]
    }
  }
}
```

Landing also needs the sandbox's network allow list to include the GitHub hosts that your
`git` remote and `gh` use. With an unmanaged spool, start the broker from a shell outside the
sandbox first (any `agc list` does it); a broker started inside a sandboxed command may not
outlive that command. The managed host service has no such concern.

**Codex.** In its restricted command sandbox, run the coordinated command outside the sandbox
with scoped approval, so a detached broker survives the command container's exit and the
spool is writable.

Whichever agent runs, the `TMPDIR` the sandbox sets is not what a job sees: the coordinator
removes inherited temporary-directory variables and provides scratch only for jobs that
declare a scratch policy.

## Orchestrators and session managers

Tools that run several agents in parallel worktrees, such as Claude Squad, Conductor, Vibe
Kanban, and Nimbalyst, do not schedule the machine or gate merges. They compose with AGCoord
in two places. The agents inside them read the repository's instruction file, so the paragraph
above reaches them unchanged. And wherever such a tool takes a shell command for setup, tests,
or checks, that command can be `agc run --label "…" --resource cpu=<n> -- <command>`, which
queues the work instead of running it immediately. Orchestrators that let agents land their
own work, such as Gas Town rigs or Symphony runs, can point their configured test or landing
step at `agc run` and `agc land` in the same way. Nothing in the coordinator needs to know
which tool submitted the job.

## Repository gate wrappers

A repository can make its own test script refuse to run outside an admitted job. Every
admitted job receives `AGCOORD_RUN_ID`, `AGCOORD_RUN_KIND`, and `AGCOORD_STATE_DIR`, and the
public verifier proves that the calling process is that exact admission:

```bash
agc verify-admission --state-dir "$AGCOORD_STATE_DIR" --checkout "$root" \
  --run-id "$AGCOORD_RUN_ID" --kind "$AGCOORD_RUN_KIND" --head-sha "$head" --worker-pid "$pid"
```

It exits 0 for the exact `full` or `land` admission and 2 otherwise, and it never creates a
row or starts a broker. A wrapper that also submits a `full` row when invoked outside any
job gives agents one command that is always coordinated. The
[admission context](coordinator.md#admission-context-for-repository-wrappers) section defines
the proof and which PID each kind verifies with.
