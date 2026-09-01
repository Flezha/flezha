# `test/` and `test/apps/` — the async budget governed everything, and it was spent on nothing

**Provenance.** This note was reconstructed after the fact, from the five landed
commits (`5e3b32c6`, `0a9943dd`, `3d61b2a9`, `08edc609`, `f74a3c25`) and the
archived change directory, which is untracked and exists on one machine. Every
claim below is sourced from one of those; nothing here is a measurement this
note took. Where a number could be re-read from the tree today it was.

## The number that shaped the plan

`async_false_share` started at **0.788 against a 0.788 ceiling** — zero
headroom, one added `async: false` file away from reddening the merge gate —
across 203 test files with 43 `async: true`. That is why the batch order is
load-bearing rather than stylistic: **fold first, split second.** Folding
eighteen `async: false` files bought the slack the six mandatory splits then
spent, and the area ends at **0.777 across 193 files with the same 43 async
ones it started with**. Nothing was converted to `async: true`.

**2 016 test macros before and after**, counted at every segment boundary with
an identical file filter on both sides. A test that stops running cannot fail,
so a green suite could never have proved the fold preserved coverage.

## `test/support/` and `test/helpers/` were one directory wearing two names

Both were `elixirc_paths` entries with identical semantics — twenty files in one
and three in the other. They are now one (`test/helpers/`), and the real test
file that had been sitting among the compiled helpers moved the other way.

That falsified `controller-packaging`, which pinned `test/support/testnet.ex` as
a **path** when its intent is that the module compiles only outside `:prod`. The
requirement now states the intent, so a later consolidation cannot break it the
same way.

`tmp_app_data_dir/0` registers its own cleanup, deleting **twenty-two** identical
`rm_rf` callbacks across forty-one files. Eighteen more are kept because each
does something the helper does not — restoring env keys, detaching telemetry,
closing a writer, uninstalling a stub — and the helper's callback registers
first so it still runs last. One caller runs inside a **second BEAM**, where
`on_exit` raises, so registration is guarded on the ExUnit server existing.

## A fold is a concatenation, and that is what makes it safe

Twenty-five incident files fold into five behaviour files. Each source keeps its
own `defmodule`, its own `setup` and its own `@moduletag`; **`test_loc` is
byte-identical across the fold batch**, which is the evidence that no line moved.
Two folds show why a semantic merge was never available: `entity_state`'s six
setups include one that depends on the case template's context and one that
predates it, and `sinks` carries a `:capture_log` moduletag on exactly one of its
five modules — the shape that silenced another module's tests when an earlier
area got it wrong.

## Helpers are earned by measurement, not by the plan

Two helpers the plan named were **refused on measurement**: the convergence
fixtures were predicted to have three callers after the split and have **one**,
all in the same child; and the three folded transport modules were said to share
a procedure fixture and carry **three different ones**, declaring three different
factory ids and channel names. One helper the plan did **not** name was added:
both `http_poll` children drive the same 190-line `gen_tcp` origin fixture.

`ScheduleTestHelper.wait_until!/3` stays, and the reason generalises: it renders
its failure message **at the deadline** from a zero-arity description, where
`assert wait_until(...), "msg"` builds the message before the wait and can only
report the pre-wait state; and it distinguishes a poll that legitimately returns
a falsy value from one that has not settled. Moving it beside the shared
`wait_until/3` was refused because **offering a second polling entry point to 190
files is what produced the twelve wrappers this area deleted** (eleven of twelve
went, their 66 call sites naming the shared function with the wrapper's own
constants).

## `duplicate_pairs` on a split — measure the cause, do not trade it away

The producer-worker split first read **+7 pairs, and none of the seven was a
per-child prologue**: all were pre-existing **in-file** clones in the 1 671-line
parent becoming cross-file visible. Extracting the three families they name took
it back to where the fold batch left it. The plan's list of that file's shared
privates misses `attach/1`, which all three children use and is the largest.

One pair was accepted and its cost named rather than traded: **sorting a file's
aliases for credo made its 50-token prologue converge with a sibling's**, and
unsorting is what the credo gate forbids. One blocking rule may not be broken for
another in silence.

## `test/apps` — the packing floor, and a proof rather than an assertion

`rust_runner`'s `main.rs`, 3 517 lines, becomes seven modules and a 115-line entry
point. The design's five leave `workflow` at 1 048 and `stream` at 1 515, **both
over the ceiling the split exists to clear**, so seven is what the ceiling
requires and is the packing floor for family-atomic units. The eleven copies of
the import prologue that are the classic split artefact are avoided by
re-exporting from `main` and taking one glob per module. The move is **proved**:
3 195 non-blank lines reconstruct byte-identical in original order (611 lines and
zero diff for `python_runner`'s fixtures, operative list still 32).

`test/apps duplicate_pairs` is recorded at the **full-scan 140, not the narrow
102** — jscpd's corpus decides what it can match, so the two reads answer
different questions and `--check` compares the full one. Of the 140, **79 have
both halves inside the area and 61 are peer-driven — `sdk/rust` 42, `sdk/java`
12, `sdk/php` 5, `lib/mix` 2 — unchanged before and after**, so an edit in any of
those four blocking areas moves this number here without anyone touching it.
Without that recorded, the next session reads a peer's edit as a regression. (The
survey's `sdk/rust` row said 36; it is 42.)

The thirteen new pairs are refused with a measurement behind the refusal: every
Rust pair is the SDK's mandatory `Procedure` surface — six associated types,
`name`, `init_state`, the run signature — and two of them match **inside the
`durable_step!` and `stateless_step!` macro bodies**, which is jscpd matching a
macro against the code that macro exists to replace. `sugar.rs`'s twins exist to
assert that macro-generated and hand-written agree, so collapsing them would
delete the assertion.

**Eight sleeps stay and each was read**: six are wedge-probe handlers that park
for ever by contract, one per SDK, and two are a poll in the crash-resume release
gate. None stands in for a settle.

## Things that cost a turn

- **Line numbers drift; re-derive the cut from content.** The crash-resume file
  is 1 321 lines, not the surveyed 1 296, because segment A's inlining grew it.
  `flezha_test.exs` cuts at 388 rather than 389, because 389 is a test and 388 is
  the tag that travels with it.
- **A split's shared privates are not one function.** Design §2.3 named one
  shared private in `flezha_test.exs`, "deleted". **Eleven** helpers are driven by
  both halves, about 145 lines including a 54-line crash-resume driver. They went
  to a harness of their own rather than into the harness eight unrelated modules
  import wholesale, because nine new names there is a collision surface for
  something two files need.
- **A dead file is proved dead four independent ways**, not by a grep: its
  extension matches no ExUnit pattern, its directory is not in `elixirc_paths`,
  the metrics classifier never counted it, and every module it names is absent
  from `lib/`. Five went on that proof.
- **Deleting a TODO tag erases what it named.** Six were removed; four named no
  work, and the two that named real coverage gaps were written into
  `docs/known_issues.md` **before** their tags went.
- **`--timing` MERGES; it does not replace.** The re-recorded timings (62.3 s
  ExUnit, 68.93 s wall on an idle box after a completed compile, against 56.2 and
  59.0 at `a147111c`) left the Java timing at its own head rather than re-stamping
  it — contrary to a dev note written before `a58f8144` fixed it.
- **The one production edit is a comment**, and it is recorded as a deliberate
  exception rather than smuggled: `application.ex` named the deleted directory.
  The fence exists to stop production changes arriving under a test refactor, not
  to require shipping a comment that names a directory which no longer exists.

## Filed on the way, not fixed

`release_cli_system_test` omits `FLEZHA_STATE_DIR` so it can never pass from
inside `mix test`, and registers its stop callback **after** the point it flunks,
so a failed setup leaks a live daemon that poisons every later run. And KI-18's
fourth sighting, whose recorded coordinates segment C's move had made stale.

## Where it finished

`files_over_800` **6 → 0**, `narrative_markers` **36 → 0**, credo warnings under
`test/` **81 → 0**, sleeps 3 → 2, `duplicate_pairs` 108 → 85 → 77 and then **+1**
in segment C for the alias-sort convergence named above, `test/apps`
`files_over_800` **2 → 0** and `narrative_markers` **4 → 0**. `test/apps` is
armed as the **thirty-sixth blocking area**. The `rust_runner` A/B row is
`direction` and was NOT run — it is on a measured path, so both arms' `sha256sum`
(`3bc991c2` before, `18536725` after) and an `--include sdk_smoke` run of 1 946
tests stand in its place, with the Rust conformance manifest deferring nothing.
