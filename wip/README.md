# `wip/` — how work runs here

A campaign is a set of related work taken on together: a batch of defects, a
migration, a cross-SDK parity push. This directory is the machinery for running
one. It exists because the work outlives any single session: a session ends,
and the next one has to pick up from files rather than from memory.

**The structure is tracked. The work is not.** This readme, the invariants, the
board templates, the worker briefs and the three scripts are in git, because
every campaign follows them. The live boards are one team's notes on one day and
are ignored, along with the archive of closed campaigns. `.gitignore` ignores
everything under `wip/` and allows the structure back by name, so a new working
file stays out of git unless someone decides otherwise.

## The files

| Tracked | Holds |
|---|---|
| `INVARIANTS.md` | The permanent rules. A reset never touches them, and a campaign may make them stricter but never looser. |
| `templates/` | The blank boards a reset restores. |
| `briefs/` | What a worker is told: `reproduce.md`, `code.md`, `critic.md`. |
| `status.py` | Derives every count from the boards. Counts are read from here, never typed. |
| `reap.sh` | Finds what a session left running: zombies, leftover builds and test runs, polling loops, containers. |
| `reset.sh` | Closes a campaign and opens the next one. |

| Ignored | Holds |
|---|---|
| `RULES.md` | This campaign: goal, exit condition, scope, do-not-touch list, gates, timeboxes, subagent policy. |
| `STATE.md` | Where the work is right now. Rewritten, never appended, kept under 150 lines. |
| `WORK.md` | The ordered queue. A row leaves it only by being done or withdrawn. |
| `DONE.md` | Finished items with their commit, net production lines and cost. |
| `NEW-ISSUES.md` | Found while working, not fixed. An inbox, drained into the standing bug list at reset. |
| `LOG.md` | Append-only session history, so `STATE.md` stays small. |
| `DECISIONS.md` | Awaiting the maintainer, and rulings already given. |
| `archive/` | Closed campaigns, one directory each. |

## The cycle

**reproduce → code → critic → verify → commit → report → next**, one item at a
time.

**Reproduce first.** Nothing is coded until the defect has been seen, with a
command and the output it really gave. Rows go stale: a list written three days
ago usually contains items that later work already fixed, and reading the code
the row names settles that in minutes. An item that will not reproduce inside
its timebox is closed as not reproducible, and no code is written for it. Code
written for a defect that was never real is worse than no code: it guards
nothing and it has to be maintained.

Reproduction runs **one row ahead**: while one item is being coded, the next is
already being read and reproduced, without touching the tree. Its regression
test waits as a draft until its row is current. Most of reproducing is reading,
and reading does not have to wait its turn.

**Code, then criticise.** One agent implements, and before it reports it closes
a checklist of the findings reviews kept raising: stale documentation, a
documented command nobody ran, an absence nothing tests, a test that could pass
without testing anything, uncaptured log noise, a changed shared function, a hot
path without numbers. A different agent then reviews, a fresh reader rather than
a copy of the implementer, because a reader who already believes the change is
correct finds nothing. The critic reports problems and never fixes them:
correctness with a concrete failing scenario, hidden behaviour change, whether
the test could ever have been red, reuse versus a new parallel layer, complexity
and net lines, documentation and spec drift, performance, new dependencies, and
whether the change quietly shapes the system to one deployment. Every finding is
then ruled on: fix now, write it down, or dismiss with a reason. There is one
review round unless its fixes changed production code the reviewer never saw.

**Verify by reading the repository**, never by re-reading the report. A claim is
a hypothesis until the command has been run and the code has been read.

**One item, one commit**, with the documentation and the spec changes inside it.
A documentation update deferred to "later" is how a repository ends up
describing behaviour it does not have.

## Running a campaign

1. `wip/reset.sh <campaign-id>` archives the last campaign and restores blanks.
2. Fill `RULES.md`: goal, exit condition, scope, what not to touch, gates.
   The exit condition is written on day one, so "is it done" is never a matter
   of opinion.
3. Put the rows in `WORK.md`, in order. Each row names the command that proves
   the defect is real and what done means for it.
4. Work the cycle. `python3 wip/status.py` at any time for the counts, the net
   production lines and what is next.
5. `bash wip/reap.sh` at session start, at every heartbeat and at every item
   boundary. Add `--kill` to clean up what the work started.

Before a reset, the issue inbox is drained by hand into the standing bug list:
deciding what a found issue is worth is a judgement, not a copy.

In this repository the loop is driven by an agent skill, `/loop /cx-wip`, but
nothing above depends on that. The method is the files and the order; the tool
only follows them.

## A note on the paths this method mentions

Some files the briefs refer to are development material kept out of git in this
repository: per-change engineering notes, the standing bug list, the older
programme's charter. They are referred to by role rather than shipped. If you
are reading this from a clone and a path does not exist, that is why.
