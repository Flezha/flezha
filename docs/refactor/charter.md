# Refactor charter

The rules every refactor session follows. One page of law, read before any
area is touched. `STATE.md` beside this file is the live board; the per-area
OpenSpec change under `openspec/changes/refactor-*` is the plan for one area.

## 1. Non-negotiables

- **Zero behaviour change per commit.** A refactor commit changes structure,
  names, comments, tests and formatting — never what the system does. A defect
  found on the way is recorded, reproduced and dealt with before the area
  continues — see §1b — and its fix is always a separate commit.
- **Every commit is green**: the area's suites, the full ExUnit suite once, the
  gates in §9, and — on a hot path — the A/B in §8.
- **Net code goes down**, measured over an area, not over a session. A session
  that ends with more lines than it started must say why in its report, and the
  reason must be another rule of this charter — §2's reuse order, or a §7
  ceiling, whose extractions each cost a signature and a brace — never
  convenience. Clearing a ceiling and reducing total lines pull against each
  other; when they do, the ceiling wins and the area's total is what must fall.
- **Reuse before new code** (§2). A plan item without a reuse decision is
  rejected by the supervisor.
- **Cross-SDK rule** stands (see `CLAUDE.md`): a surface every SDK carries
  changes in every SDK in one change, or names the SDK it defers and the
  follow-up that owes it.

## 1b. A defect stops the area until it is understood

Amended 2026-08-30, on the user's instruction, replacing "record it and fix it
later". The old rule produced forty entries of which eleven were already fixed
or never true and five could not be reproduced at all: a defect written down in
passing, mid-other-work, is a suspicion, and a suspicion cannot be verified,
closed, or acted on by anyone but its author. Reproduction is cheap at the
moment of discovery and expensive a week later, when the context is gone.

**The sequence, in order, no step skipped:**

1. **Finish the task box in flight, then stop.** Not mid-edit: the tree must
   compile and the structural move must be whole, or the fix diff and an
   unfinished refactor become one unreadable change that cannot be committed
   safely. Not at the batch's end either — a P1 does not wait two hours.
2. **Write it down** — `docs/known_issues.md`, in the right severity section,
   with `**Where**` (file:line), what it does, and why it was not fixed on the
   spot.
3. **Reproduce it.** A command, and the output that command actually gave. This
   is not optional and it is not "run the suite again": name the smallest
   invocation that shows the defect, and record the rate if it is intermittent.
   **Timebox: 30 minutes of worker time.** If it will not reproduce in that
   budget, record it as a *suspicion* with everything that was tried and how
   long, and the area resumes — an unreproducible defect does not halt work.
4. **Establish the mechanism.** *Why* it happens, named to the function. "The
   driver probably skips a check" is not a mechanism. If two mechanisms remain
   possible, name the experiment that separates them.
5. **Triage, on the evidence** — exactly one of:
   - **Real and worth fixing** → the area pauses and the fix lands first (6).
   - **Not a bug** → record why, with the evidence that refutes it, in
     *Resolved*. Do not delete the entry: "never true" and "fixed" must stay
     distinguishable.
   - **Real but not worth fixing** → record the cost and what it would buy.
     This verdict is the supervisor's, and it **escalates to the user** whenever
     the fix needs a behaviour or product decision, or would cost more than the
     rest of the area. A worker never rules a defect away.
6. **Fix it in its own commit**, with a regression test **shown red first** — a
   test that never failed on the defective code proves nothing. The refactor
   commits stay behaviour-free and bisectable; a bisect must always be able to
   tell a structural regression from a fix.
7. **Then refactor what the fix touched.** Code that arrives via a fix is not
   exempt: it re-enters the area's scope, takes the same gates (§7 ceilings,
   §9 naming, no narrative comments), is re-surveyed for the §2 reuse order,
   and is covered by the area's final verification. A fix is not a hole in the
   area's standard.
8. **Resume the refactor** at the next box.

**Who decides what.** The worker reproduces, establishes the mechanism and
proposes a verdict; it never rules a defect away by itself. The supervisor
verifies the reproduction against the repo and rules. The user is asked only
for a behaviour/product trade-off, or a fix that outweighs the area.

**Severity does not decide whether to reproduce — only whether to pause.**
Everything found is reproduced and triaged at once, including a P3 wart. Only a
verdict of *real and worth fixing* pauses the area for a fix; *not a bug* and
*not worth it* are recorded and the refactor resumes immediately.

**A defect in code the area has already refactored is not a special case.** It
follows the same eight steps; a finished area is a blocking ratchet, so the fix
must clear that area's gates too.

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
no layer below `service` names. Measured at `d40ccdc9` (`mix xref graph
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

- File ≤ 800 lines. Production function ≤ 50 lines. Cyclomatic complexity ≤ 10
  (credo), cognitive complexity ≤ 25 (clippy). The function ceiling is production
  only — as §4 already exempts test names, a test body that is one flat table of
  assertions reads worse split at an arbitrary point, and this is what
  `refactor_metrics.py` has measured all along. The file ceiling binds tests too.
- Complexity that comes from a shared, parameterised core is accepted and
  named in the proposal. Complexity from accidental branching is flattened.
- A GenServer or dispatcher is split by *mutation × strongly-connected
  component*, never by handler kind.

## 8. Performance

Hot paths: `flz_core` poll core and host, the dispatchers, `flz_transport`,
`wal_writer`, `input_log`, the Kafka producer worker, the plugin routing seam.

An area that touches a hot path proves it with an **interleaved A/B on the same
box** against the tag `refactor-baseline-<area>`. A regression beyond the row's
measured floor blocks the commit; an improvement is recorded in `STATE.md` and
the commit message. Measuring is the most expensive thing this programme does —
it was 61 % of area 2's closing segment — so it obeys four rules:

- **Build every checkpoint, measure once.** Each checkpoint BUILDS its arm and
  keeps it with a `sha256sum` (about two minutes); only the area's LAST
  checkpoint measures. A regression found there is attributed by a bisect over
  the kept arms, run only when there is something to attribute. Area 2 measured
  three times and the decisive answer came from a pair of kept arms both times —
  so keeping arms is what buys attribution, and re-measuring at every checkpoint
  buys nothing.
- **An unchanged binary is not re-measured.** Before running a row, compare the
  driving binary's `sha256sum` between the arms; if they are equal the row cannot
  have moved, and the sha IS the evidence. Record it and skip the row.
- **Three rounds detect, five confirm.** Run three paired alternating rounds. Go
  to five only when the direction is consistent across all three — a real
  regression shows as an unbroken direction early (area 2's was 5 of 5), and a
  clean row does not.
- **Only a gate row gets the long window.** Each row is declared `gate`, `guard`
  or `direction` in the area's `design.md` with its measured floor. A gate row
  gets the window its floor needs; a guard row gets the short window; a direction
  row is reported and never blocks. A row whose floor cannot resolve the effect
  being claimed is a direction row — say so rather than running it.
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

`duplicate_pairs` counts pairs among the files a run scanned, so a narrower
`--area` set answers a different question — `sdk/rust` reads 169 alone and 215
whole-repo on identical source. A recording therefore names its area set, and
`--check` refuses a baseline recorded under a different one rather than
differencing two numbers that mean different things. Changing the area table
means re-recording the baseline.

**Lines that arrive from a higher layer are not a breach.** §3 sends shared
code down to the lowest layer that needs it, so a later area moving its
scaffolding into a finished one makes that area's production count rise by
construction — the ratchet is per-area and cannot see the deletion on the
other side. Such a rise is accepted at the checkpoint when the change shows a
LARGER deletion in the layers the code left, per-area numbers printed from
`--check`, and the lower area is re-baselined at the close. An excess over that
deletion is accepted only when it is itself forced by another ceiling of §7,
named and counted line by line — one blocking rule may not be broken to satisfy
another in silence. Assertion is not
enough: no evidence, no exemption. Nothing else exempts a blocking area.

## 11b. The machine

This runs on a laptop. A session that runs builds, suites or benchmarks **checks
the machine every 30–60 minutes** and kills what it started — zombies, orphaned
`cargo`/`rustc`/`criterion`/`beam.smp`/`mix`/`mvn` processes, polling loops,
containers. The commands are in `CLAUDE.md` under "Machine hygiene".

**A `top` snapshot lies about benchmark load.** Criterion and the round-trip
benches measure in 60-second windows separated by builds and process startup, so
a snapshot usually lands in a gap and shows an idle box while the fan runs hard.
Judge by runs over a window (`find <log-dir> -mmin -60 | wc -l`) and the
15-minute load average. On 2026-08-30 an area's A/B put 76 runs and a 281 % CPU
`beam.smp` on the box for an hour; the user's own `top` showed nothing.

**When your own measurement is the load, cut the rows that cannot gate.** A row
whose noise floor exceeds the effect it is asked to detect (§8's `direction`
rows) spends the machine for a number nobody may act on. Record its arm sha as
the evidence and skip it.

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
