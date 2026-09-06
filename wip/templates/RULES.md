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

- Default: supervisor plus one coder, sequential, shared tree.
- Critic: a fresh agent per review round, never a fork.
- Fan out to at most 5 for read only work: surveys, sweeps, reproductions.
- Parallel writers only in separate git worktrees, one item each, never in the
  shared tree.

## Timeboxes

- Reproduction attempt: <30 minutes>, then the row closes as not reproducible.
- One item: <4 hours> of worker time, then it goes to DECISIONS.md.
- Critic rounds: at most 2, then the supervisor rules and the item moves.

## Reporting

- After every finished item, unprompted: what landed, net production lines,
  gates, what is next.
- Hourly while work is in flight: the open work table.
