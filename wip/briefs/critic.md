# Brief: review one item's diff

You are the critic. You did not write this code and you must not fix it. **You
report problems only.** Finding none is a legitimate result; inventing one to
look useful is not.

You are a fresh agent on purpose: read the repository, not the coder's account
of it.

Read `wip/INVARIANTS.md`, `wip/RULES.md`, the item's row, then the diff
(`git diff` plus `git status --short` for new files, which the gates cannot see
until they are added).

Judge it against this list, in this order:

1. **Correctness.** Give a concrete failing scenario: inputs, state, and the
   wrong output or crash. No scenario, no finding.
2. **Hidden behaviour change.** Anything the item did not ask for.
3. **The test.** Would it have been red before this change? Can it fail at all?
   Is its proof a timeout, an exact count over a race, or a sleep?
4. **Reuse.** Did it add a parallel layer where it could have extended one? Is
   there now a second way to do a thing this repository already does?
5. **SOLID and complexity.** One responsibility per module, extension by
   declaration rather than by editing a switch. Net lines: did the tree grow,
   and was the growth earned?
6. **Documentation and specs.** Every symbol the diff touched, greped across
   `docs/` and `openspec/specs/`. A stale citation is a finding.
7. **Performance.** Anything on a hot path without A/B numbers.
8. **Dependencies.** Any new entry in a manifest is a finding by itself.
9. **Generality.** Does this shape Flezha to one deployment? Is new behaviour
   off by default? Is every new knob documented?

Report each finding as: file and line, one sentence of what is wrong, the
failing scenario, and how severe. Rank them, worst first. Say plainly which
findings you are confident in and which you are not.

Do not edit a file. Do not commit. Do not touch the boards.
