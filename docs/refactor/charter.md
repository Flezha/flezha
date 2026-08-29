# Refactor charter

The rules every refactor session follows. One page of law, read before any
area is touched. `STATE.md` beside this file is the live board; the per-area
OpenSpec change under `openspec/changes/refactor-*` is the plan for one area.

## 1. Non-negotiables

- **Zero behaviour change per commit.** A refactor commit changes structure,
  names, comments, tests and formatting — never what the system does. A defect
  found on the way is recorded in `STATE.md` → *Found defects* and fixed in its
  own change, unless it blocks the area.
- **Every commit is green**: the area's suites, the full ExUnit suite once, the
  gates in §9, and — on a hot path — the A/B in §8.
- **Net code goes down.** A session that ends with more lines than it started
  must say why in its report, and the reason must be §2, not convenience.
- **Reuse before new code** (§2). A plan item without a reuse decision is
  rejected by the supervisor.
- **Cross-SDK rule** stands (see `CLAUDE.md`): a surface every SDK carries
  changes in every SDK in one change, or names the SDK it defers and the
  follow-up that owes it.

## 2. Reuse

Order, strictly: **use as-is → extend/parameterise → refactor into a shared
core → new code.** Every proposal item states its decision in one clause:
"extends X", "generalises X and Y into Z", or "no candidate because …".

- Two functions that do the same thing are one function with a parameter.
- A helper used by two siblings moves to their common lower layer — never into
  a new `util`/`helpers` bucket.
- A parallel mechanism (a second state machine, a second cache, a second
  "last good" tracker) is a defect, not a design.
- Deleting is the preferred way to reduce duplication.

## 3. Layers and dependency direction

A lower layer never names a higher one. Shared code lives in the lowest layer
that needs it.

**Rust**: `flz_transport` → `flz_core` → { `sdk/rust` (+ `flezha-macros`),
`flz_core_ffi` → the veneer SDKs, `flz_nif` }. `cli` uses transport and core
only. Nothing depends on `sdk/rust` except its test apps.

**Elixir controller** — the target order is `transport` → `runner` →
`operative` → `state` / `plugin` → `service` → `cli` / `http` / `healthz`,
with `observability`, `authz`, `convergence` as cross-cutting consumers that
no layer below `service` names. Measured at `4405cf9d` (`mix xref graph
--format cycles`, `--format dot` aggregated by subtree) the tree is not that:
one 55-file cycle spans `state` (20 files), `runner` (9), `operative` (9),
`service` (6), `convergence` (5), `shutdown` (2), `plugin` (2), `transport`
(1) and `dlq` (1); six smaller cycles are supervisor ↔ worker pairs
(`kafka`, `postgres`, `operative_forward`), the durability-tier backend
behaviour ↔ its three backends, the `sdk` quartet and `observability`
`bus` ↔ `registry`. Against the target order the lower layer names the
higher one in 109 file-level references — `operative → state` 33,
`operative → plugin` 16, `runner → state` 15, `runner → service` 13,
`operative → service` 12, `plugin → service` 9, `runner → operative` 4,
`state → service` 2, `transport → runner` 2, `transport → service` 2,
`state → http` 1 — and the cross-cutting three are named from below
`service` 12 times (`state → authz` 4, `runner → authz` 3, `runner →
convergence` 2, `state → convergence` 2, `operative → authz` 1). The
controller areas (5a–5e) each cut their part of that cycle; the charter's
order is what they cut towards, and `refactor_metrics.py` ratchets the cycle
count per area.

**One settlement tail, one buffer.** The poll loop is already one core:
`PollCore`, one `EntitySlot` with erased state, one `ProcedureDispatcher`
trait with two impls, one `StagedInterval<C>::fold`. What the typed Rust path
and the erased host duplicate is the effect-settlement tail (`invoke_once` vs
`resolve_item`) and the buffer (`TypedBuffer<S>` vs `BytesBuffer`). Area 1
re-implements the spike's seam — a `Bodies` codec marker, `EffectBuffer<B>`,
`settle_effects<B>` (branch `spike/1s-dispatcher`, measured ≤ 1.5 % on every
resolvable row with allocations flat) — under this charter. Java's
`ActivationDispatcher` is a host over the erased loop, not a third loop; the
per-language host half stays per language.

## 4. Naming

- One word per concept, in every language: the glossary (`docs/glossary.md`)
  is authoritative. `apply_change` / `applyChange` / `apply_change` — the same
  word, the language's casing. A synonym is a bug.
- Verbs mean one thing: `resolve` (pure, from inputs), `derive` (from other
  state), `probe` (observe with I/O), `diagnose` (probe and explain), `claim`,
  `refuse`, `settle`, `drain`, `ship`. `try_` returns an Option/Result;
  Elixir `?` is a boolean, `!` raises.
- Units are suffixes: `_ms`, `_nanos`, `_bytes`.
- Production function names ≤ 32 characters. Test names are sentences and are
  exempt. Modules are nouns (what they own); functions are verbs (what they do).
- Existing gates stay and grow: a new naming rule extends
  `scripts/naming_gate.py`, never a second gate script.

## 5. Comments

- A comment states an invariant, a constraint, or a *why* the code cannot say.
- A comment never narrates history: no "used to", "before this change", "the
  first draft", incident dates, proposal or task ids, design-decision labels.
  History belongs in `docs/dev_notes/<change>.md` and commit messages.
- `@moduledoc` / `//!` ≤ 15 lines: what the module owns and its one invariant.
  `@doc` / `///` on public API only, and only when the name does not say it.
- A comment that restates the code is deleted.

## 6. Errors

- **Rust**: one error enum per crate, typed; no `Result<_, String>` in
  refactored code. The C ABI maps it to the error surface in one place.
- **Elixir**: one refusal shape per boundary. Phase 0 inventories the shapes
  in use; the first controller area decides the shape and amends this section.
  Until then: never add a third.
- A refusal names the thing refused and the rule it broke. It never names
  history.

## 7. Size and complexity

- File ≤ 800 lines. Function ≤ 50 lines. Cyclomatic complexity ≤ 10 (credo),
  cognitive complexity ≤ 25 (clippy).
- Complexity that comes from a shared, parameterised core is accepted and
  named in the proposal. Complexity from accidental branching is flattened.
- A GenServer or dispatcher is split by *mutation × strongly-connected
  component*, never by handler kind.

## 8. Performance

Hot paths: `flz_core` poll core and host, the dispatchers, `flz_transport`,
`wal_writer`, `input_log`, the Kafka producer worker, the plugin routing seam.

- A session that touches a hot path runs an **interleaved A/B on the same box**
  against the tag `refactor-baseline-<area>` set at its start: `mix
  bench.roundtrip` for the controller/kernel, the Rust SDK bench for the SDK.
  A regression beyond noise (3 %) blocks the commit; an improvement is recorded
  in `STATE.md` and the commit message.
- CI floors stay as they are; they detect hardware, not refactors.
- The dispatcher unification (§3) is measured as a spike change with no other
  output before any code moves.

## 9. Tests

Four buckets, and each test is in exactly one:

- **contract** — wire, ABI, decl keys, conformance fixtures. Kept; must be fast.
- **behaviour** — one per spec scenario, one file per module. Kept and merged:
  an area session folds its incident files (`foo_<incident>_test.exs`) into the
  module's behaviour file.
- **incident** — a regression test for one old bug. Folded into behaviour, or
  deleted when the behaviour test already covers it.
- **fake-proof** — stubs the collaborator the test exists to check. Deleted or
  rewritten against the real collaborator.

Rules: no `Process.sleep` / `thread::sleep`; `refute_receive` only after a
positive settle signal and ≤ 100 ms; `async: true` unless the test names the
global it needs; one fleet per e2e module (`setup_all`); a flake is fixed in
the session that meets it; a test is deleted only when the coverage diff shows
nothing lost — consolidate before the area's refactor, delete after it.

Time is a ratchet: the suite is never slower than the previous commit.

## 10. Formatting

One command formats every language: `scripts/format-apply.sh`;
`scripts/format-check.sh` is the gate. Formatter versions are pinned and equal
to CI. A formatting-only commit precedes an area's first refactor commit so
the refactor diff shows only the refactor.

## 11. Gates and the ratchet

`scripts/refactor_metrics.py` measures, per area: production and test LOC,
comment density and narrative-marker count, files over the ceiling, long
names, duplicate clusters, dependency cycles, `async: false` share, sleeps and
refutes, suite wall-clock. `--write-baseline` records `docs/refactor/
baseline.json`; `--check` fails when a ratcheted metric is worse than the
baseline. A finished area flips its gates from measuring to blocking.

## 12. The session contract

Every area runs as one OpenSpec change, `refactor-<n>-<area>`, through the
supervisor (`/refactor`) with fresh workers:

1. **Survey** (a read-only worker): reads this charter, `STATE.md`, the area's
   `docs/dev_notes`, runs the metrics; writes `proposal.md` (candidates with a
   reuse decision each), `design.md` (target structure: which modules survive,
   which merge, hot paths named), `tasks.md` (batches). No spec deltas unless a
   public surface changes.
2. **Supervisor approves** the proposal against this charter.
3. **Implement** (a fresh worker): batches; the area's suites once per batch;
   the full suite once; gates; A/B if a hot path moved. Stops at a checkpoint —
   no archive, no commit, no staging.
4. **Supervisor verifies against the repo**: every ticked box exists in code;
   every new or extracted function has a caller; nothing outside scope; the
   test claim matches a real run; the gate numbers are not worse; the A/B
   report exists.
5. **Supervisor closes**: archives, stages only the worker's paths, commits
   (no push, no AI trailer), updates `STATE.md`, flips the area's gates.

Git rules for every worker: stage only your own paths; never `add -A`, never
stash/restore/checkout/reset/clean; never push. Docker: never `compose down`.
