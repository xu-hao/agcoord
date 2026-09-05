# Why AGCoord exists

> Your coding agents just made writing code the cheapest part of shipping software. They did
> not make landing it any cheaper.
>
> — [The Merge Queue Is the New Bottleneck](https://tianpan.co/blog/2026-07-02-the-merge-queue-is-the-new-bottleneck), July 2026

## One developer, many agents

Before coding agents, one developer used one workstation on one branch at a time. The human
was the scheduler: tests ran when the human ran them, one suite at a time, and the human was
also the serialization point for merging. Hosted CI and pull-request review guarded the
shared repository, and nothing needed to guard the machine, because nothing on it competed.

Coding agents changed the shape of the work without changing the machine. One developer now
runs several agents at once, each in its own worktree, each running builds and test suites and
landing pull requests on its own clock. Teams report four to eight concurrent worktrees per
developer as routine and twenty or more as the ambition. Every one of those agents behaves as
if it had the machine to itself, because nothing tells it otherwise. That is the new problem,
and it has three faces.

## Three failure modes

### They compete for the machine

Five test suites, each sized for the whole machine, thrash it. Runs that take two minutes
alone take ten together, then time out. Memory pressure turns into OOM kills that land on
whichever process the kernel picks. Scratch space fills. The failures look exactly like
regressions, so the agent that sees one starts debugging code that was never broken, and the
human reviewing its work cannot tell the difference either.

The evidence is public. Claude Code's issue tracker holds reports of many instances saturating
CPU ([#30807](https://github.com/anthropics/claude-code/issues/30807),
[#11122](https://github.com/anthropics/claude-code/issues/11122)). A write-up on headless
sessions puts it plainly: five parallel sessions on a 16 GB machine produce OOM kills because
"there's no shared scheduler, no memory backpressure". The
[AgentCgroup](https://arxiv.org/html/2602.09345v2) paper (February 2026) treats per-tool-call
cgroup control of coding agents as a research problem. On the author's own workstation, before
scratch became a declared resource, concurrent gates fell through to the system temp directory
on a shared 16 GB tmpfs, filled it, and turned green landings red for a day.

### They merge on stale evidence

An agent's tests passed on a base that another agent has since changed. Each branch was green
alone; together they break `main`. The check and the merge were two separate steps, and
anything can happen between them: another landing, a force-push, a second agent deciding the
same file needed a different shape. Hosted merge queues solve this for the repository's
canonical gate, but the local loop that agents actually run in, where the gate is a shell
command on the workstation, has had no equivalent, and agents are prolific enough to hit the
gap daily on a single machine.

### They leave no shared record

What ran, on which head, with what result, is scattered across a dozen terminal sessions and
the context windows of agents that have since been restarted. Cancelling a runaway test run
means hunting for process IDs. Answering "did anything test this commit?" means trusting an
agent's summary of its own work.

## The fourth face: agents follow the tools they are given

An agent under pressure to finish will take any path that exists. If `gh pr merge` works, it
will be used. If a direct push to `main` works, that will be used too. Written instructions
help, and every repository that runs agents accumulates them, but an instruction is not an
enforcement point.

A coordinator is. When the only path from a green gate to a merged pull request is one
command that the coordinator owns, the rules stop being prose. The job declares its resources
or it is not admitted. A job cannot submit another job. A landing whose target moved does not
reuse its verdict. A branch that carries a commit the operator removed from `main` is refused
before anything is pushed. A few lines in `CLAUDE.md` or `AGENTS.md` point every agent on the
machine at the same queue, and the coordinator refuses everything else. That turns it into the
thing a human can trust so that they do not have to trust each agent's discipline.

## Why the gate and the merge must be one step

The tempting design is two tools: a scheduler that runs checks, and a merge script that runs
after a check is green. It fails in exactly the situations that matter. Between the green
result and the merge, another landing can move the target, or the branch can be pushed to
again, and the merge then publishes something the gate never saw. Every retry policy layered
on top of two tools reopens the same gap.

AGCoord's `land` is one durable row that holds the repository lane and the declared resources
from preflight through publication. It brings the current target into the branch first, with
an exact lease, so the gate sees what will actually be merged. It runs the gate once. A green
result moves the same row into publication without releasing anything, and the publication is
a single atomic compare-and-update of the target and the source references on the forge. If
either reference moved, nothing is published and the verdict is discarded. A red gate
publishes nothing. There is no gap because there is no second step.

## What AGCoord deliberately does not do

- It does not orchestrate agents. It does not start them, assign them work, track tickets,
  message them, or restart them. Session managers and orchestrators do that, and AGCoord sits
  underneath them.
- It does not replace hosted CI. Required checks on a pull request stay where they are. A
  branch that requires a hosted merge queue should keep using it; the two are exclusive per
  branch, not complementary.
- It does not resolve conflicts, rebase, or rewrite commits. A conflict is handed back to
  whoever owns the branch, with the checkout restored.
- It does not batch or bisect landings. One repository lane lands one request at a time,
  against the exact head it gated. That trades throughput for a guarantee, and the trade is
  deliberate.
- It does not make a user-writable spool tamper-proof against processes running as the same
  user. The boundary protects the machine, unrelated processes, and the landing verdict from
  admitted commands, not the owner from themselves.

## Where to go next

The [quickstart](quickstart.md) shows the whole loop in a few minutes without root. The
[agent guide](agents.md) has the paragraph to paste into an agent's instruction file and what
to do with every refusal. The [comparison](comparison.md) places AGCoord next to hosted merge
queues, local job queues, session managers, Gas Town, sandboxes, and local CI runners.
