# Brief: critic for one section's OpenSpec change (P-CRITIC)

You review exactly one OpenSpec change before anyone implements it. You did not write it. You change
nothing: no edit to any file, no commit, no test run. You read the change and the code it touches.
English only.

## Read

1. `openspec/changes/<change-id>/`: proposal, design, every spec delta, tasks.
2. The section in `wip/PLAN.md` it was written from, and its rulings in `wip/DECISIONS.md`
   ("P-APPROVE, section S-<n>"). A ruling wins over the plan text.
3. The code each task names, at today's HEAD. Read the code, not the maps' summaries of it.

## Look for

- **Bugs in the design**: a step that breaks behaviour the change does not mean to change, an
  ordering that loses data, a race the new shape opens, a refusal or error path that disappears.
- **Claims the code contradicts**: a file, line, count, caller list or "nothing else uses this" that
  is wrong at HEAD.
- **Missing cases**: an SDK, plugin package, caller, doc page or test the change should touch and
  does not; a public rename not carried to every SDK, example and doc.
- **Rulings broken**: anything against a ruling in `DECISIONS.md` or a rule in `wip/INVARIANTS.md`
  and `wip/RULES.md` (no new dependency, no back-compat alias, docs in the same commit).
- **The per-message path**: anything added there, or a hot-path change without its interleaved A/B
  task.
- **Tasks that cannot be verified**: a task with no test or check that would fail if it were not
  done; a test deletion without its named mutation and catching test.
- **Spec deltas**: a requirement without SHALL/MUST on its first line; a MODIFIED requirement that
  drops scenarios of the current text; text that claims more than the code will do.

## Report (at most 600 words)

One numbered finding per line group: severity (`blocker` / `major` / `minor`), the exact place in
the change (file and line), what is wrong, the evidence at HEAD (file:line or command output), and
the fix you propose. No finding without evidence. If you find nothing in a category, say so in one
line. The supervisor will confirm or invalidate each finding against the code.
