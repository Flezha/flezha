# The kind gate stops failing on correct prose, and the allowlist stops hiding a real one

**Provenance.** Reconstructed after the fact from the landed commit `a79cefc0`
and the archived change directory, which is untracked and exists on one machine.
Nothing here is a measurement this note took.

## The failure mode: a gate whose count grows while nobody touches code

`check-kind-naming` had been red for **the entire refactor programme** — 26
occurrences at the parent commit, every one the capacity or plain-English sense,
**none** a kind-sense use. Describing the gate on the board added hits, so the
count grew while nobody touched code, and **three separate sessions reworded
correct prose to satisfy a regex**. That is the shape to recognise: a gate nobody
can get green stops being a gate, and the fourth rewording was refused.

## The fix is a second judgement, not a wider allowlist

Widening the allowlist could not be safe, because **a broad masking rule hides
the error the gate exists to catch**. So the kind sense is now **unmaskable**:
`check-kind-naming` runs `naming_gate.pattern_check` twice — once with the
allowlist, once with it empty — and fails a line tripping **either**. That is the
match-directly judgement `check-overflow-naming` already used, so it is a second
instance of a mechanism the tree has, not a third mechanism.
`scripts/naming_gate.py` gained **no code**; only its docstring changed. The
allowlist then carries only the open-ended plain-English half and drops from
**69 rules to 29**.

`KIND_SENSE` binds `operative | kind | class | factory | one` and deliberately
leaves `stream`, `workflow`, `run`, `input` and `iteration` alone, because
`operative-kind-naming/spec.md` **mandates** prose using those adjectives for the
run — so a gate binding those nouns would fail its own capability's requirements.

## The allowlist had been hiding a retired identifier in shipped source

KI-105, and it is the argument for the whole change. The allowlist's `time`
branch matched a two-word span inside the Java identifier
<!-- kind-naming-record:begin -->`registerUnbounded time`<!-- kind-naming-record:end -->
and left the residue `registerUn`, which carries no match — so two javadocs in
`Procedure.java` passed. Both now say `registerStream`, already the spelling in
`StreamOperativeConfigurationException.java` and `ProcedureCtx.java`, and **the
exemption is deleted, so coverage is restored rather than moved**. Prose only —
no signature, annotation or behaviour changed.

**The general rule this proves**: an allowlist entry that matches a *substring*
can leave a residue that no pattern matches, and the gate then reports OK for a
line that contains the retired name. An exemption must be pinned to what it
exempts.

## A five-step job is red if any one step is red

KI-106: `check-module-naming` was red at HEAD on two citations inside a
`docs/known_issues.md` entry. `.github/workflows/naming.yml` runs five checks as
**five steps of one job**, so that alone kept the workflow red — closing only the
kind gate would have left CI failing and the change unverifiable. Wrapped in a
`module-naming-record` block; no prose edited.

## Verification

All five naming checks exit 0. The negative control was run **independently of
the worker's**: the four kind-sense shapes are caught, **including the KI-105
shape the old allowlist passed**, while new limit prose and the spec-mandated
sentence still pass, with the tree byte-identical after the drill.
`refactor_metrics.py --check`: 0 regressions.

## What re-reds this gate tomorrow, and what does not

Writing *about* the gate is the recurring source, and it is not hypothetical —
this gate has been reddened by prose describing it more than once. The remedy is
a `kind-naming-record` block around the quotation, **not** a reworded sentence
and **not** a new allowlist entry: the allowlist now carries only the open-ended
plain-English half by design, and adding a kind-sense entry to it would restore
exactly the masking KI-105 came from.
