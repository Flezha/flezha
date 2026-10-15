# Invariants

**These rules are permanent. A reset never touches this file.** A campaign's
`RULES.md` extends them and may make them stricter. It may never relax one.

## What must not change

- **The architecture stays.** Controller and runners, stateful streams, and
  bounded relays are the system's shape. Work inside them, never around them.
- **No new third party dependency without the maintainer's consent.** A new entry in
  `Cargo.toml`, `mix.exs`, `package.json`, `pom.xml`, `composer.json` or
  `Gemfile` is a decision, not an implementation detail.
- **No change that costs throughput, latency or reliability.** A change on a hot
  path carries interleaved A/B numbers taken on this box, or it does not land.
- **Flezha is a general purpose open source system.** A change must not shape it
  to this deployment. Behaviour that only we need is off by default, and every
  knob it adds is documented in the `docs/configuration/` pages in the same commit.

## Reproduce before you fix

**Nothing is worked until it is reproduced.** The row carries the command and
the output it really gave, taken before any code changed. A regression test is
shown red on the unfixed code and green after. A reproduction nobody ran is a
hypothesis, and the supervisor runs it rather than reading about it.

An item that will not reproduce inside its timebox is closed as *not
reproducible*, with what was tried. No code is written for it. Fixes for defects
that were never real are how the tree grew code that guards nothing.

## A run is clean, or it is a failure

**No warnings. No unexpected errors. Ever.** A test run prints its results and
nothing else. This is not a preference: a run that prints fifty warnings is a
run where the fifty-first, the one that matters, is invisible, and every session
after it learns to scroll past the noise.

- Compile warnings are errors, in **every** environment, and that includes the
  test files themselves. `mix compile --warnings-as-errors` does not compile
  `test/**`, so the test tree needs its own gate.
- The same holds for every other toolchain here: Rust, Java, Node, Python, Ruby
  and PHP each build and test warning free.
- An error or warning **logged** during a run is either expected by the test
  that provokes it, and captured, or it is a defect. A log line nobody
  captured is noise, and noise is where a real failure hides.
- A run that is not clean is not green, whatever the pass count says. Do not
  add to a noisy run: clean it, or make the noise fail the build, before the
  work that needed the run.

## Say what happened, from the files

The work is not done in private. A session reports as it goes, and the reporting
costs almost nothing next to the verification it is reporting on.

- **After every row closes**, unprompted, at most five lines: the row and its
  verdict, the one command that proved it, the commit, net production lines,
  and what is next.
- **Every thirty minutes while a row is in flight**, one line: row, phase,
  elapsed, and what the worker last wrote. Silence for an hour is a defect in
  the reporting, not a sign of progress.
- **Immediately** when something is blocked or a decision is the maintainer's,
  with the options, the cost of each and a recommendation. Then take the next
  row: waiting is not a state this work has.

**A report is derived, never composed.** Every number in it comes from
`python3 wip/status.py` or from git, and every verdict comes from the row
already written into `DONE.md`. Nothing is retyped from memory, because a
remembered count is how a report starts disagreeing with the files the next
session reads.

The same view is on disk for anyone who wants it without asking, and it costs a
running session nothing at all:

```
watch -n 60 python3 wip/status.py     # counts, what is in flight, what closed
tail -f wip/LOG.md                    # the running narrative
```

## Less code, not more

The goal is a smaller and simpler system. Every item reports production lines
added and removed, and the campaign total is expected to be negative. Prefer, in
this order: use what exists, extend or parameterise it, refactor two things into
one shared core, and only then write something new. A new module beside a
near-identical old one is a defect, not a delivery.

`git diff --numstat` measures it, split by hand into production and test paths.
The ceilings a change is held to (no file over 800 lines, no function over 50,
no new dependency cycle) are in `docs/contributing/code_standards.md`, along with the rules
for writing a check that enforces one.

## What one item costs

A row used to take an hour and a half, and most of that was not fixing anything:
fresh agents reading the rulebooks end to end, a review round on a ten-line
change, whole suites after small edits, and evidence pasted into boards nobody
read again. These defaults hold for every campaign.

- **A fresh worker per item, with a self-contained brief.** The brief carries the
  row, the recorded reproduction, the files it names, any ruling already given
  and the rules that bind the worker. The worker reads the code the item
  touches, not the rulebooks.
- **Fixable or a decision, settled in the first ten minutes.** An item whose fix
  needs a design decision (a behaviour or public surface change, a record that
  is wrong in some window, a rule every SDK must share) goes to `DECISIONS.md` at
  once, and the next item starts.
- **Every item ends with `/cx-refine`** over its own diff, run by the worker
  before it reports. **A bigger change (below) gets at least three passes in a
  row**, each a fresh `/cx-refine` over the whole item diff, so a pass also
  reviews what the previous pass restructured. Past three it continues while
  the last pass still accepted a structural candidate, five at most. Every
  pass updates the docs and specs its own edits made false. The report
  shows one line per pass; too few passes sends the item back to the coder.
- **A critic only for a bigger change**: more than a few dozen production lines,
  a new module or public surface, or concurrency, authorization or durability
  logic changed beyond a few lines. A small or low-risk change gets the refine
  pass and the supervisor's reading, nothing more.
- **Only the tests the change affects.** A test is repeated under load only when
  it depends on timing. The full suite runs once every five to ten items, and a
  red there is diagnosed before the next item starts.
- **Build and test at milestones, never after each edit.** A milestone is a
  finished section of work: the API changed, all fixtures moved, the example
  written, the docs done. Between milestones the worker edits and reads; it does
  not build or run tests. At a milestone: one build and the tests that section
  touched. Each full suite (the SDK's, the core `mix test`, the system and e2e
  runs) runs once, at the end. A red is fixed in one batch from inspection and
  only the failing tests rerun; the full suite runs once more at most. One row
  of 2026-09-28 built its SDK 39 times, another ran the core suite 7 times.
- **Paperwork is what someone reads**: the commit, one `DONE.md` line carrying the
  reproduction command and its key output, and the bug-list row moved. A dev
  note only for a trap the next session would fall into. No evidence pasted into
  `WORK.md`, no `LOG.md` entry per item, and `STATE.md` rewritten when the phase
  changes, not at every event.

## Tools, not hand steps

Time goes into steps, not code: on 2026-09-28 each row took 330 to 460 agent
steps, and most of them were the same setup, checks and live commands typed by
hand in every row. Whatever a campaign does in every row is done by a script.

- **Scripts are part of the kit.** A campaign keeps, under `wip/tools/`:
  - an environment script that brings up a private controller release, state
    directory, ports, the demo exchange and fresh native libraries in one
    command, and a teardown that stops all of it. No worker builds its own by
    hand, and none points a controller at a shared state directory;
  - one verify command that runs every gate an item needs (the specs, the
    citation gate, the naming gate, the formatters, the tests the change
    touched) and prints one line per gate, the log kept on disk. The worker runs
    it at each milestone; the supervisor runs the same command to verify;
  - walk scripts for any guide the campaign changes: each runs the page's steps
    against a real controller and writes the output blocks. A guide's output is
    never typed by hand, and the walk is re-run by the next change to that page;
  - a codemod for any change repeated across many files, its diff read and the
    milestone tests run on it. Hand edits are for what a script cannot express.

  A script that already exists is extended, never copied. The first row that
  needs one writes it, and it is tracked for every campaign after.
- **Build expensive things once and share them.** Native libraries and the
  controller release are built once per core commit, into a shared cache under
  the campaign's scratch space, and every worker uses that build. A worker
  rebuilds only what its own change touched.
- **Pipeline, not queue.** The next item is screened while the current one is
  coded. The critic reads the code as soon as the code is finished, while the
  worker writes the docs; the docs get a short second look. Items whose files do
  not overlap run in parallel by default; only shared files, ordered steps (an
  archive order, a shared page) and live runs against one controller are
  serialized, and the rules say which.

## Definition of done, for every item

1. Reproduced, with the command and its key output recorded in the item's
   `DONE.md` line.
2. Fixed at the mechanism, then swept: every other site with the same shape is
   fixed or named.
3. A test that was red before the fix and is green after.
4. Documentation and specs updated **in the same commit**. Never a follow up.
5. Gates green: `openspec validate --strict`, the citation gate, the naming and
   boundary gates, the tests the change affects. The full suite runs once every
   five to ten items, not per item.
6. One item, one commit. No AI attribution trailer, ever.
7. The row moves to `DONE.md` with its commit, its net lines and its cost.

## Working rules

- **Everything written into the repository is English.** Files, comments,
  commit messages, worker briefs. Conversation may be in another language.
- **Commit messages are 10 to 15 lines, plain language.** What changed and why
  it mattered. Detail belongs in `docs/dev_notes/<change-id>.md`.
- **The repository is a shipped artifact.** `wip/` splits in two: its structure
  is tracked because every campaign follows it, and the live boards are
  development material and are ignored. A board never becomes a commit, and no
  shipped document ever cites one. If a board shows up in `git status`, the
  ignore rules are broken: fix them before doing anything else.
- **Never push. Never a destructive git command.** No `stash`, no
  `reset --hard`, no `checkout --`, no `clean -fd` on this tree. Stop containers
  by name, never with `docker compose down`: another project on the same machine
  may share the compose project name.
- **Verification is never delegated, and never duplicated.** The supervisor reads
  the diff and runs the item's own tests and gates once; it does not re-run
  everything the worker ran.
- **A session never ends with a subagent alive**, and the check is `ListAgents`
  showing no subagent section, not memory.
- **Clean up as you go, unasked.** `bash wip/reap.sh --kill` at session start
  and at every heartbeat kills orphans: zombie loops, leaked builds and tests,
  Playwright browsers and agents whose session is gone. At every item boundary
  and before a session or loop ends: stop every finished subagent, close the
  Playwright browser, then `--kill-mine` for whatever else this session started,
  this repository's containers included. A process high in the CPU list that
  nobody can name as needed is a leak. Every kill is logged in `LOG.md`.
- **A ruling the maintainer has given is work, never a question.** It lives
  in `DECISIONS.md` and is not re-asked.
