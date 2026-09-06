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

**Do not commit, do not stage, do not archive.** Stop and report.

Report: what you did NOT do first. Then the red output, the fix, the sweep, the
reuse decisions, net lines, gates run with their real output, and anything you
found and did not fix (which goes to `wip/NEW-ISSUES.md`, written down, not
fixed, unless it blocked you).
