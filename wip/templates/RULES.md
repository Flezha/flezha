# Campaign rules

**Campaign**: `<id>` · **Opened**: `<YYYY-MM-DD>` · **Maintainer**: `<name>`

Read with `wip/INVARIANTS.md`, which this file extends and may not relax.
Changing a rule here takes effect on the next wake up.

## Goal

<One paragraph. What is different about the system when this campaign is done.>

## Exit condition

<The test for "finished". A count, a gate that goes green, a list emptied.
Written on day one so "is it done" is never a matter of opinion.>

## Scope

<What is in. Rows live in WORK.md; this says what may join them.>

## Do not touch

<Files, areas or behaviours a worker must not open in this campaign, and why.
A worker that "helpfully" fixes one of these has taken a decision that is the
maintainer's.>

## Clean-run bar

`INVARIANTS.md` requires a warning-free, noise-free run. Name here which
commands this campaign holds to that bar and how each is checked, so the
requirement is a gate rather than an intention.

Per item: only the tests the change affects. The full suite: once every <5 to
10> closed items, and at exit.

## Definition of done, campaign additions

<Anything beyond the seven steps in INVARIANTS.md. Extra gates, extra proof.>

## OpenSpec policy

- **No change proposal** when the fix restores behaviour a spec already
  mandates. The record is the WORK.md row, the commit, and a dev note if it
  earned one.
- **A change proposal** when the work alters a requirement, a public surface or
  spec'd behaviour, because there the spec delta is the deliverable.
- **One proposal per batch** when several related items together move specs.

## Subagents

- Default: supervisor plus one coder per row. Rows whose files do not overlap run
  in parallel, each coder in its own git worktree or submodule; list here what
  stays serialized (shared pages, an archive order, live runs on one controller).
- Reproduction runs one row ahead of the coder, read only, its test a draft
  under `wip/repro/<row-id>/` (`wip/briefs/reproduce.md`), except while the row
  in flight measures performance.
- Every item ends with `/cx-refine` over its diff, run by the coder before it
  reports; a bigger change (the critic's threshold below) gets at least three
  passes in a row, more while a pass still accepts a structural candidate, five
  at most. Every pass updates the docs and specs its own edits made false.
- The critic reads the code as soon as the last refine pass is done, while the
  coder writes the docs; the docs get a short second look.
- Critic: only for a bigger change (more than a few dozen production lines, a new
  module or public surface, or concurrency, authorization or durability logic
  beyond a few lines). A fresh agent, never a fork, diff only, a word limit. The
  cheaper model unless correctness turns on concurrency, authorization,
  durability or a hot path.
- Fix rounds go back to the same coder, which keeps its context.
- Fan out to at most 5 for read only work: surveys, sweeps, reproductions.
- Parallel writers only in separate git worktrees, one item each, never in the
  shared tree.

## Tools

The `wip/tools/` scripts this campaign uses (environment up and down, the
verify command, walk scripts, codemods), which exist and which the first row that
needs them writes. The shared build cache: where it lives and what it holds.

## Timeboxes

- Reproduction attempt: <30 minutes>, then the row closes as not reproducible.
- Fixable or a decision: settled within <10 minutes> of a worker starting.
- One item: <4 hours> of worker time, then it goes to DECISIONS.md.
- Critic rounds: one; a second only when the fixes changed production code the
  first critic did not see, or a fix-now finding was a correctness defect.
  Then the supervisor rules and the item moves.

## Reporting

`INVARIANTS.md` sets the cadence: five lines when a row closes, one line every
thirty minutes while a row is in flight, and an escalation the moment something
is the maintainer's call. Every number is derived from `wip/status.py` or git,
never retyped.

Name here anything this campaign wants on top of that, and who reads it.
