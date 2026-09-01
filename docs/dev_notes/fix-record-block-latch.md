# A record block exempts its own lines and no others

**Provenance.** Reconstructed after the fact from the landed commit `67c762c2`
and the archived change directory, which is untracked and exists on one machine.
Nothing here is a measurement this note took.

## The defect

`naming_gate.walk()` tested for the **opening** marker first and `continue`d, so a
line carrying **both** markers never reached the closing branch and **the
exemption latched to end of file**. One inline record pair therefore blinded its
gate to the whole rest of the file — while the gate printed `OK` and exited 0.

## What was wrong is not that something was hidden

The blinding was real but **latent**: with the ordering corrected, all three
record-tagged gates still pass, so no live violation had been hidden. What was
wrong is that **`OK` meant either "nothing to report" or "stopped reading", and
nothing distinguished them.** Coverage returns across five files; the worst was a
dev note read **four lines out of a hundred and fifty-nine**, because its inline
pair sits near the top with nothing below it to close.

That is the lesson worth keeping: a gate's green is only evidence if the gate
can say how much it read.

## An inline pair stays LEGAL

Refusing the inline form would force prose edits on correct prose — four of the
five live instances wrap a phrase **inside a sentence** and one is a **table
cell**, where a line break ends the row. Legality costs no coverage: an inline
pair exempts one line either way, and **all the blinding came from the latch**.

## An unclosed block is now REFUSED

A block left open exempts the rest of the file, which is the same defect by a
different route and equally invisible. **No tracked file is unclosed, so the
check is armed rather than owed.** It reports separately from the naming
failures, because it is not one.

## The self-test judges the logic, not itself

`python3 scripts/naming_gate.py --self-test` runs the **real walk** over a
throwaway git tree with four fixtures and asserts the **exact set** of flagged
lines. Against the unfixed walk it reports
`expected [block.md:4, inline.md:2, unclosed.md:1], found [block.md:4]`. It runs
as a sixth step of the Naming job, needs only `python3` and `git`, and **nothing
else in this repository tested `scripts/` at all** before it.
