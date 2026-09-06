# Invariants

**These rules are permanent. A reset never touches this file.** A campaign's
`RULES.md` extends them and may make them stricter. It may never relax one.

## What must not change

- **The architecture stays.** Controller and runners, stateful streams, and
  workflows are the system's shape. Work inside them, never around them.
- **No new third party dependency without the maintainer's consent.** A new entry in
  `Cargo.toml`, `mix.exs`, `package.json`, `pom.xml`, `composer.json` or
  `Gemfile` is a decision, not an implementation detail.
- **No change that costs throughput, latency or reliability.** A change on a hot
  path carries interleaved A/B numbers taken on this box, or it does not land.
- **Flezha is a general purpose open source system.** A change must not shape it
  to this deployment. Behaviour that only we need is off by default, and every
  knob it adds is documented in `docs/configuration.md` in the same commit.

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

`python3 scripts/refactor_metrics.py` measures it: production and test lines
apart, files over 800 lines, functions over 50, dependency cycles, duplicate
pairs.

## Definition of done, for every item

1. Reproduced, with the output recorded.
2. Fixed at the mechanism, then swept: every other site with the same shape is
   fixed or named.
3. A test that was red before the fix and is green after.
4. Documentation and specs updated **in the same commit**. Never a follow up.
5. Gates green: `openspec validate --strict`, the citation gate, the naming and
   boundary gates, the suite that covers the change.
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
- **Verification is never delegated.** The supervisor reads the repo itself.
- **A session never ends with a subagent alive**, and the check is `ListAgents`
  showing no subagent section, not memory.
- **A ruling the maintainer has given is work, never a question.** It lives
  in `DECISIONS.md` and is not re-asked.
