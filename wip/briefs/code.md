# Brief: implement one item

You are the coder for exactly one item. The supervisor's brief carries the row,
the recorded reproduction, the files it names, any ruling already given and the
rules that bind you. Read that and the code the item touches. Do not read the
rulebooks (`wip/INVARIANTS.md`, `wip/RULES.md`, `docs/public/contributing/code_standards.md`) end to
end: open a section only when the brief points at it or a question needs it. A
dev note named for the area is worth reading, because its traps are why it
exists.

**Order of work, not negotiable:**

1. **Screen, within ten minutes.** Confirm the reproduction still holds and say
   whether the fix is local or needs a decision: a behaviour or public surface
   change, a record that is wrong in some window, a rule every SDK must share. A
   decision: stop and report the options with the cost of each. Do not code
   around it.
2. **Write the regression test first and show it RED** on the current code.
   Paste the failing lines in your report. A test that never failed proves
   nothing.
3. **Fix the mechanism, not the symptom**, as small as the defect. Then name
   every other site with the same shape: fixed here only if it is this defect,
   otherwise named in the report.
4. **Reuse before new code.** In order: use what exists, extend or parameterise
   it, fold two things into one shared core, and only then write something new.
5. **Documentation and specs in the same change.** Grep every symbol you touched
   across `docs/` and `openspec/specs/`. A tracked sentence your change makes
   false is fixed now.
6. **Run the tests the change affects, and rerun as often as the goal needs**:
   to reproduce a failure, prove a fix, or confirm a clean run. An intermittent
   failure is run until it reproduces, and its fix until it holds.
   **Use the campaign's tools, never hand steps.** `wip/tools/` has the
   environment script (a private controller, ports, the demo exchange, the shared
   native build: never build your own), the verify command (every gate, one line
   each) and the walk scripts. Anything you would repeat across many files is a
   codemod; anything you would type live more than twice is a script. A missing
   tool you need is written there and kept, not improvised in your scratchpad.
   Long output goes to a file: you read its summary and failures only.
7. **Run `/cx-refine` over your own diff** before you report, and apply what it
   finds that keeps behaviour unchanged. **A bigger change runs it at least
   three times in a row**: more than a few dozen production lines
   (`git diff --numstat`), a new module or public surface, or concurrency,
   authorization or durability logic changed beyond a few lines. Each pass
   covers the whole item diff, including what the previous pass changed, and
   runs only the tests covering what it touched. Every pass updates the
   documentation its own edits made false: grep each symbol it renamed, moved,
   merged or deleted across `docs/`, `openspec/specs/`, SDK examples and doc
   comments, as in step 5. After the third, run another
   only while the last pass accepted a structural candidate, five at most.

**Rules that will fail your work if broken:** no behaviour change beyond the
item; no new third party dependency; nothing that costs throughput, latency or
reliability, and a hot path needs interleaved A/B numbers; behaviour only this
deployment needs is off by default and its knob is documented; no file over 800
lines and no function over 50; English everywhere; no AI attribution in
anything.

**Before you report, close this checklist.** One line each: the evidence, or
"not applicable, because ...".

1. Everything that names what you changed (docs, specs, `CHANGELOG.md`, SDK
   examples and doc comments, `test/apps/`) is still true.
2. Every command your change tells someone to run was run, or its exact argument
   shape is driven by a test.
3. Every "no longer happens" claim is pinned by a test that fails if it happens.
4. Every test you added or changed was red for the right reason, cannot pass
   vacuously, and proves nothing by a sleep, a fixed timeout or a count over a
   race.
5. Every log line your tests provoke is captured and asserted.
6. A shared function whose result, error text or side effect changed: every
   caller named and shown to behave as before.
7. A hot path (per frame, message, event or delivery): interleaved A/B numbers,
   both arms the same kind of function, raw output in the scratchpad.
8. A spec edited: SHALL or MUST on each requirement's first body line, every
   surviving scenario carried, and the text claims only what the code does.

**Hand over at 80 % context.** When your context is past 80 % full, start no
new step. Bring the step in hand to a safe point (no half-applied edit, no
background process left running), then write `wip/handover/<row-id>.md` and
report `HANDOVER` to the supervisor, who starts a fresh coder on it. The
handover lets that coder continue without re-reading what you read:

- the goal and the step of it you reached, and which tasks are checked;
- what is done, what is half done, and the exact next action;
- the files changed and why, and any edit that must not be undone;
- the commands that build, test and walk this item, and their last result
  (green, or the failing tests by name);
- the traps found, the decisions taken and their reasons, and open questions.

If you are picking up from a handover, read it and `git diff --stat` first,
continue from its next action, and discard nothing. Overwrite the file with
your own handover if you in turn reach 80 %.

**Paperwork:** no dev note unless the item taught a trap the next session would
fall into. No board edits, except appending a found issue to
`wip/NEW-ISSUES.md`.

**Leave nothing running.** Before you report or hand over: close the
Playwright browser if you opened one, stop every background process, server,
container and loop you started, and say in the report that you did.

**Do not commit, do not stage, do not archive.** Stop and report, in at most 400
words: the screen verdict, the red and green output, the fix with file and
line, one line per refine pass (candidates accepted and rejected, what
changed, docs updated), production and test lines from
`git diff --numstat`, and the checklist.
