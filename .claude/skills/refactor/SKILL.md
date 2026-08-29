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
> defect listed in your report under "Found defects", NOT fixed unless it
> blocks a task. STOP after that and report: do not archive, commit or stage.
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
- Found defects are listed, not silently fixed.

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
   names no file of the change; commit `refactor(<id>): <one line>` with the LOC delta and A/B
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
notification, never a Monitor; if you must check a job, run one bounded
check (`timeout 500 bash -c 'until [ -f <marker> ]; do sleep 15; done'`) and
continue in the same turn.*

Implement segments are **two batches** long so a checkpoint comes every few
hours, and the supervisor verifies and commits at each.

On each wake-up update *Current worker → last seen* in `STATE.md` with the
time, ticks, and `git diff --stat | tail -1`. Stop the loop (`stop: true`)
only when every row is `done`, or when the only runnable areas are `blocked`
on an entry in *Awaiting the user's decision*. Say so in one line.

## 7. Blockers

A decision that is the user's — a public-surface rename, a defect whose fix
changes behaviour, a gate that cannot be met without one — goes under
*Awaiting the user's decision* with the options and your recommendation. The
area becomes `blocked`; move to the next runnable area.
