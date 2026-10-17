# Brief: write one section's OpenSpec change (P-SPECS)

You write exactly one OpenSpec change for one approved section of the campaign plan. You change no
code. You create files only under `openspec/changes/<change-id>/`. You do not commit, stage, push or
archive. English only. Never run a destructive git command.

## Inputs (read these, in this order, and nothing else end to end)

1. `wip/PLAN.md`: section 0 (conventions) and your section S-<n> in full; the rows of sections 11
   and 12 that name S-<n>; your section's lines in section 13.
2. `wip/DECISIONS.md`: "Rulings already given": your section's block, and every campaign-wide
   ruling block the supervisor names in your prompt. A ruling that differs
   from the plan's assumption WINS over the plan text; the block says which ones differ.
3. The maps the section cites (`wip/analysis/A-*.md`), only the parts your section uses.
4. `.claude/skills/openspec-propose/SKILL.md` for the mechanics (`openspec new change`,
   `openspec instructions <artifact> --change <id> --json`, follow each artifact's template and
   instruction). Skip its "ask the user" steps: the plan and rulings are your input.

## What the change must contain

- **proposal.md**: why (the campaign goal for this section), what changes, every public rename and
  removal (with or without aliases, as the campaign's rulings say), impact per repo/SDK.
- **design.md**: before/after per concern, the extension points, the ruled decisions with their
  option and reason, the hot-path changes each with its interleaved A/B plan, risks.
- **specs/**: deltas only for capabilities whose behaviour changes (a pure refactor with no
  behaviour change needs no spec delta; say so in the proposal). Each requirement's first body line
  carries SHALL or MUST; every MODIFIED requirement copies the full current text from
  `openspec/specs/` before editing it.
- **tasks.md**: ordered, each task one verifiable step, grouped in commits that each build and pass
  the tests they touch. Include: tests deleted only with a named one-line mutation and the test that
  catches it; docs fixed in the same task as the code that made them false; the AI kit regenerated
  in any task that edits a published page; an interleaved A/B task before every per-message change
  lands; at least three `/cx-refine` passes; the critic; the section-end full
  `wip/tools/clean-run.sh` and the measurement set (RULES phase 4). No task may say "cannot test" or
  "operator follow-up"; the only exception is a change in another repository the maintainer owns
  (e.g. flezha_ui), named as such.

## Verify what you lean on

The plan cites files and lines at an older HEAD. Before a task names a file, line, count or symbol,
check it exists at today's HEAD (`git log -1 --oneline`); correct it in the change if it moved. A
claim you could not confirm is written as "to confirm at apply" in the task, not stated as fact.

## Finish

`openspec validate <change-id> --strict` must pass. Report in at most 300 words: the change id, the
artifact list with line counts, the ruled decisions it carries, every plan claim you corrected
(old -> new), anything you could not confirm, and the validate output line. Leave nothing running.
