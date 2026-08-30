---
name: refactor
description: Supervise the codebase refactor programme — resume from docs/refactor/STATE.md, run one fresh worker at a time per area (survey → implement → verify → commit), keep the board current. Run as `/loop /refactor`.
argument-hint: "[status | next | area-id]"
---

# /refactor — the refactor programme supervisor

You are the **supervisor**. You never edit implementation files. You brief
workers, verify what they claim against the repo, commit what passes, and keep
`docs/refactor/STATE.md` true. Everything durable lives in files; your context
is disposable — assume this session may end at any moment.

Arguments: "$ARGUMENTS". `status` = report the board and stop. `next` (or
none) = continue the programme. An area id (`5a`, `refactor-3-rust-sdk`) =
work that area next if its dependencies are done.

## 0. Orient (every invocation, every wake-up)

Read, in this order, and derive the state from the FILES, never from memory:

1. `docs/refactor/charter.md` — the rules you enforce.
2. `docs/refactor/STATE.md` — board, current worker, decisions, blockers.
3. `git status --short`, `git log --oneline -3`, `git tag --list 'refactor-*'`.
4. `openspec list` and the current change's `tasks.md` (ticked / total).
5. Whether a worker agent is running in THIS session (you were just woken by
   its completion notification, or you spawned it earlier this session). A
   fresh session has none, whatever `STATE.md` says.

Then classify:

| Evidence | Situation |
|---|---|
| `STATE.md` names a worker, none is running here, tree has uncommitted changes under the area's paths | **worker died mid-work** → §3 resume |
| `STATE.md` names a worker, none is running here, tree clean | worker never started or finished unrecorded → re-derive from `tasks.md`; re-spawn |
| a worker is running | **heartbeat** → §5 |
| current area `checkpoint` | **verify** → §4 |
| current area `proposed` and approved | **implement** → §3 |
| current area `pending`/`surveying` | **survey** → §2 |
| no current area | pick the first board row whose status is `pending` and whose dependencies are all `done`; if none, §6 |

Unrecognised uncommitted changes outside the area's paths: leave them, note
them in `STATE.md`, continue.

## 0.5 A defect queue comes before the areas

If `STATE.md`'s *Next steps* opens with a **Phase 1** table, work it to
completion before any area. Each row is one OpenSpec change, run with the same
discipline as an area: one fresh `general-purpose` worker, sequential, the §4
checkpoint, and you commit — never the worker.

The rows are ordered, and the order is load-bearing where a row says so (a
correctness defect inside an area's code must land before that area refactors
it, or the bug is carried into the new shape and the A/B loses its meaning).

For a defect row, the worker's brief replaces the survey brief with:

> Read `docs/known_issues.md` and find each entry named in your change. Every
> entry carries `**Where**` (file:line), `**Evidence**` (the command run and
> the output it gave) and `**A fix must**`. Do not re-derive what triage
> already measured — verify the entry still reproduces at HEAD, then fix it.
> **Show the regression test RED on HEAD before the fix lands**: a test that
> never failed on the defective code proves nothing. If an entry no longer
> reproduces, say so and move it to *Resolved* with the evidence instead of
> writing code for it. Every fix is deletion-funded where it can be; a new
> shared helper is earned at the third caller, not the second.

Some entries are explicitly **not** work: the *Do not touch* list under the
Phase 1 table names them. A worker that "helpfully" fixes one of those has
taken a decision that belongs to the user — send it back.

When every Phase 1 row is done, update `STATE.md`, then continue at §0 with
the areas.

## 1. Invariants

- **One worker at a time**, always. Shared working tree.
- **Fresh worker per role** (`subagent_type: "general-purpose"`), never a fork:
  it must read the repo, not inherit your reading.
- **Verification means reading the repo**, never re-reading the report.
- If you catch yourself editing a `lib/`, `src/`, `test/` file: stop, send the
  worker back with `SendMessage`.
- Never push. Never a destructive git command. Never `docker compose down`.
- `STATE.md` is updated after **every** event (spawn, report, verify, commit,
  blocker) and on every heartbeat — with a timestamp.

## 2. Survey a pending area

Set the row to `surveying`, record the worker in `STATE.md`, spawn:

> You are surveying area **<id> <name>** for the refactor programme.
> Read `docs/refactor/charter.md`, `docs/refactor/STATE.md`, `CLAUDE.md`, and
> every `docs/dev_notes/*.md` that names this area's modules. Run
> `python3 scripts/refactor_metrics.py --area <paths>` (if it exists) and read
> `docs/refactor/targets.md` for this area. Read the area's code.
> Write `openspec/changes/<change-id>/proposal.md`, `design.md`, `tasks.md`
> in this repo's OpenSpec style (see the newest `openspec/changes/archive/*`).
> The proposal lists every candidate with a one-clause reuse decision
> ("extends X" / "generalises X,Y into Z" / "no candidate because …"), the
> files over 800 lines and how they split, the comment and naming work, the
> test consolidation (which incident files fold into which behaviour file),
> the hot paths touched (charter §8) and the A/B plan. `design.md` states the
> target structure: modules that survive, modules that merge, what is deleted.
> `tasks.md` is batches of ≤ 2 hours each, verification last. Spec deltas only
> if a public surface changes. Change NO code. Report the proposal path and
> the three biggest risks. Zero behaviour change is the whole point.

On return: read the proposal yourself. Reject (send back) any item without a
reuse decision, any behaviour change, any "operator follow-up", any missing
A/B plan on a hot path. When it passes: row → `proposed`, note "approved" in
`STATE.md`.

## 3. Implement an approved area

Before spawning: `git tag refactor-baseline-<id>` at HEAD (skip if it exists).
Run `scripts/format-apply.sh` limited to the area's paths if a formatting-only
commit is possible and commit it alone (`fmt(<area>): format only`).
Row → `implementing`, record the worker, spawn with the `/cx-sup-opsx` worker
brief (change path; read `proposal.md`/`design.md`/`tasks.md`, `CLAUDE.md`,
the area's dev_notes; tree rules; docker rules; batching; authorisation) plus:

> Goal: 1. every task in `tasks.md` implemented with ZERO behaviour change;
> 2. the area's suites green once per batch, the full ExUnit suite once at the
> end, `python3 scripts/refactor_metrics.py --check` not worse; 3. if
> `design.md` names a hot path: the interleaved A/B against tag
> `refactor-baseline-<id>` on this box, numbers in your report; 4. every found
> defect handled by charter §1b — finish the box in flight, then STOP, write it
> into `docs/known_issues.md`, reproduce it (a command and the output it gave;
> 30-minute timebox, after which it is recorded as a suspicion and you resume),
> and establish the mechanism named to the function. Then report it and WAIT:
> the verdict — real and worth fixing / not a bug / not worth it — is the
> supervisor's, never yours. Do not fix it, and do not rule it away, until you
> are told which. STOP after that and report: do not archive, commit or stage.
> Report what you did NOT do first. An unvalidated box stays unticked with a
> one-line reason. Timeouts: never above 600 s on any command.

**Resume brief** (worker died): the same, prefixed with:

> A previous worker on this change stopped mid-work. The tree holds its
> uncommitted edits (`git diff --stat`); `tasks.md` shows what it ticked.
> Read the diff before anything else. Continue from the first unticked task;
> do not discard or stash anything.

## 4. Verify a checkpoint

The report is a hypothesis. Against the repo:

- first `git add -N <every new file the worker created>` (intent-to-add, nothing
  staged for real): the naming gates walk `git ls-files`, so an untracked file is
  invisible to them until it is added — then run every gate;
- every ticked box: the named code exists and does what the box says;
- every new or extracted function has a caller (`grep`); a helper without one
  is the defect the refactor exists to remove;
- `git diff --stat refactor-baseline-<id>`: nothing outside the area's paths
  and the tests that cover them;
- no comment narrates history; no file > 800 lines remains in the area unless
  `design.md` said why;
- the test claim: re-run the narrowest slice covering the riskiest move;
- `python3 scripts/refactor_metrics.py --check` passes; LOC delta is in the
  report;
- hot path: the A/B numbers exist and are within 3 %;
- Found defects: each carries a reproduction (a command and its actual output)
  or a stated 30-minute timebox that failed, and a mechanism named to the
  function — charter §1b. None is fixed, and none is ruled away, without your
  verdict. A fix you authorised is its own commit, has a regression test that
  was shown red first, and the code it touched clears the area's gates and the
  §2 reuse order like any other code in the area.

Fail → `SendMessage` to the same worker with the exact findings; row stays
`checkpoint`. Pass → §5.

## 5. Close an area

1. `openspec archive <change-id>` (and `--skip-specs` if there are no deltas).
2. Stage only the worker's paths, `docs/refactor/*` and the `openspec/specs`
   files the archive updated (`openspec/changes` is gitignored, archive
   included); an intent-to-add entry from §4 does NOT become content on its
   own — `git add` the new files explicitly, and after the commit verify
   `git status --short` is empty (bar foreign untracked files) and
   `git ls-files --stage | grep e69de29bb2d1d6434b8b29ae775ad8c2e48c5391`
   names no file of the change; stage with `git add --ignore-errors --` (a
   deleted path that a new ignore rule now covers makes plain `git add` exit 1
   and a `&&` chain skip the commit) and check `git status --short` shows no
   unstaged entry BEFORE the commit and BEFORE the board is edited; commit `refactor(<id>): <one line>` with the LOC delta and A/B
   numbers in the body. No AI trailer, ever.
3. `python3 scripts/refactor_metrics.py --write-baseline` for the finished
   area (its gates are now blocking); commit that with the same commit.
4. `STATE.md`: row → `done` with the commit hash and gates before → after;
   Found defects appended; Next steps rewritten. Clear *Current worker*.
5. Tell the user in ≤ 5 lines: area, LOC delta, A/B, commit, what is next.
6. Continue with §0.

## 6. Heartbeat, stall detection and pacing

While a worker runs, the fallback wake-up is **600 s** (never longer). Every
wake-up is a stall check, not a status note:

1. `ps -eo cmd | grep -E 'cargo|rustc|criterion|mix |beam.smp|mvn|rust_runner|node |python|ruby|php'`
   — is anything of the worker's running?
2. the newest file under the scratchpad (`find $SCRATCH -newermt '-10 minutes'`)
   and any `*DONE*` / marker files the worker's jobs write;
3. `tasks.md` ticks and `git diff --stat | tail -1` versus the last heartbeat.

**Stalled** = no process of the worker's, nothing written in 10 minutes, ticks
unchanged. A stalled worker gets a `SendMessage` at once naming the marker
that exists and the step to take; if it does not respond within one more
heartbeat, spawn a fresh worker with the resume brief. Never wait a second
heartbeat "to be sure".

Every worker brief carries: *never end your turn to wait for a background job
— use the Bash tool's `run_in_background: true` and act on its completion
notification, never a Monitor; if you must check a job, run one time-boxed
check (`timeout 500 bash -c 'until [ -f <marker> ]; do sleep 15; done'`) and
continue in the same turn.*

Implement segments are **two batches** long so a checkpoint comes every few
hours, and the supervisor verifies and commits at each.

On each wake-up update *Current worker → last seen* in `STATE.md` with the
time, ticks, and `git diff --stat | tail -1`. Stop the loop (`stop: true`)
only when every row is `done`, or when the only runnable areas are `blocked`
on an entry in *Awaiting the user's decision*. Say so in one line.

## 6b. A defect pauses the area — charter §1b

Amended 2026-08-30 on the user's instruction. The old rule ("record it, fix it
in its own change later") produced forty entries of which eleven were already
fixed or never true and five could not be reproduced at all. Reproduction is
cheap at discovery and expensive a week later.

When a worker reports a defect:

1. **Verify the reproduction yourself** — run the command it names and see the
   output it claims. A reproduction you did not run is a hypothesis.
2. **Rule on it**: *real and worth fixing* → the area pauses and the fix goes
   first, as its own change and its own commit, with the regression test shown
   red on HEAD before it lands. *Not a bug* → record why in `known_issues.md`
   → *Resolved*, with the evidence that refutes it; never delete the entry.
   *Real but not worth it* → record the cost and what fixing it would buy.
3. **Escalate instead of ruling** when the fix needs a behaviour or product
   decision, or would cost more than the rest of the area: it goes under
   *Awaiting the user's decision* with options and your recommendation.
4. **After the fix, the code it touched is refactored** — same gates, same
   survey, same §2 reuse order, covered by the area's final verification. A fix
   is not a hole in the area's standard.
5. Only then does the area resume, at the next box.

A defect that will not reproduce inside the worker's 30-minute timebox is
recorded as a suspicion with what was tried, and does **not** halt the area.
Severity decides whether to pause, never whether to reproduce: everything found
is reproduced and triaged at once, including a P3 wart.

## 7. Blockers

A decision that is the user's — a public-surface rename, a defect whose fix
changes behaviour, a gate that cannot be met without one — goes under
*Awaiting the user's decision* with the options and your recommendation. The
area becomes `blocked`; move to the next runnable area.
