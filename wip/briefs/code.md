# Brief: implement one item

You are the coder for exactly one item. Read `wip/INVARIANTS.md`,
`wip/RULES.md`, `CLAUDE.md`, the item's row in `wip/WORK.md` with its recorded
reproduction, and every `docs/dev_notes/*.md` that names the area.

The reproduction is already recorded and is not yours to re-derive. Your job is
the fix, its proof, and the documentation that goes with it.

**Order of work, not negotiable:**

1. **Write the regression test first and show it RED** on the current code.
   Paste the failing output in your report. A test that never failed proves
   nothing, and this campaign has already shipped code for defects that were
   not real.
2. **Fix the mechanism, not the symptom.** Then **sweep**: every other site with
   the same shape is fixed in this item or named in your report.
3. **Reuse before new code.** In order: use what exists, extend or parameterise
   it, fold two things into one shared core, and only then write something new.
   State the decision for each thing you touched, in one clause. A new helper
   beside a near identical old one is a defect.
4. **Delete more than you add** where the item allows it. Report production
   lines added and removed, separately from test lines.
5. **Update the documentation and the specs in the same change.** Grep every
   symbol you touched across `docs/` and `openspec/specs/`. A doc left for later
   is how this repository shipped four false statements.
6. **Run the gates**: `openspec validate --strict` if specs moved, the citation
   gate, the naming and boundary gates, and the narrowest suite that covers your
   change. The full suite runs once, at the end, not after each edit.

**Rules that will fail your work if broken:** no behaviour change beyond the
item; no new third party dependency; nothing that costs throughput, latency or
reliability, and a hot path needs interleaved A/B numbers; behaviour only this
deployment needs is off by default and its knob is documented; English
everywhere; no AI attribution in anything.

**Before you report, close this checklist.** Each item is a finding a critic
raised on a previous item, so a report that skips one comes back as another round
of work. For each, give the evidence or "not applicable, because ...":

1. **Everything that names what you changed.** Grep every symbol, command,
   default and error text you touched across `docs/`, `openspec/specs/`,
   `CHANGELOG.md`, the examples and doc comments of every SDK, and `test/apps/`.
   A tracked sentence your change makes false is fixed now.
2. **Every command your change tells someone to run** (docs, changelog, examples,
   help text): run it, or drive its exact argument shape through the test that
   covers that command. A documented command that cannot work is a blocker.
3. **Every "no longer happens" claim** (no record, no telemetry, no wake, no log
   line) is pinned by a test that fails if it does happen.
4. **Every test you add or change** was shown red for the right reason, cannot
   pass vacuously (it asserts the condition it tests really occurred), and proves
   nothing by a sleep, a fixed timeout or an exact count over a race. A timing
   test is also run with the machine loaded.
5. **Every log line your tests provoke on purpose** is captured and asserted; a
   run of each file you touched prints nothing else.
6. **A shared function whose result shape, error text or side effect you changed**:
   name every caller and show each behaves as before, or keep the change on your
   own path.
7. **A hot path** (per frame, message, event or delivery): interleaved A/B
   numbers on this box, both arms the same kind of function, a control arm that
   removes the new work, and the raw output saved to the session scratchpad.
8. **A spec delta**: SHALL or MUST on each requirement's first body line, a
   MODIFIED block carries every surviving scenario, and the text claims only what
   the code and its tests do.
9. **The ceilings** in `docs/code_standards.md`: no file over 800 lines, no
   function over 50, tests included.
10. **Review your own diff against `wip/briefs/critic.md`**, as the critic will.

**Do not commit, do not stage, do not archive.** Stop and report.

Report: what you did NOT do first. Then the red output, the fix, the sweep, the
reuse decisions, net lines, gates run with their real output, the checklist with
its evidence, and anything you found and did not fix (which goes to
`wip/NEW-ISSUES.md`, written down, not fixed, unless it blocked you).
