# Closing work with a handoff

Work that **changed something** — code, documentation, tickets, packaging, infrastructure,
or live state — ends with a handoff, not a narrative. The reader was not watching. They need
to know where things stand and what only they can decide.

The trigger is a change, not a length. A long session and a one-commit fix use the same four
sections; the smaller change simply needs less detail. Do not skip the handoff because the
work felt small.

**Answering a question is not changed work.** A read-only explanation ends with the answer.
If an exchange started as investigation but changed something, the change triggers the
handoff.

Use these four sections in this order. Omit a section that is genuinely empty; never merge
two sections.

## 1. What I did

State what changed and where it landed: branch, pull request, issue, and merge status. Report
failed or skipped validation plainly. Give facts, not a transcript of the process.

## 2. What is open

List work that exists but is unfinished: pull requests awaiting review or landing, follow-up
issues, validation not run, or work deliberately left out of scope.

Every issue reference includes its number and one sentence describing the work it tracks.
For example, `#42 — cap worker memory without terminating unrelated jobs` is useful on its
own; `#42` is not.

## 3. Decisions needed

Include only decisions that genuinely belong to the reader and whose answers lead to
materially different work. Make an obvious default yourself and report it under **What I
did**.

## 4. Options and recommendation

For every decision above, give the options, the cost of each, and the option you recommend
with a reason. A decision without a recommendation hands the work back instead of finishing
it.

## What this replaces

Do not close changed work with a chronological transcript that buries status and next steps.
Arguments and durable design reasoning belong in an issue, pull request, or canonical design
document. The handoff points to that record instead of duplicating it.
