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
     This verdict is the supervisor's. A worker never rules a defect away.
   - **Too large, or the user's call** → §1c. Skip it and keep working; do not
     stop the programme for it.
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

## 1c. A defect that needs the user does not stop the work

Added 2026-08-30 on the user's instruction. A defect whose fix needs a
behaviour or product decision, or which would cost more than the rest of the
area, is **skipped, not blocked on**: it goes to `STATE.md` → *Awaiting the
user's decision* with what it is, why it is the user's call, the options with
the cost of each, and a recommendation — then the session **moves straight on
to the next runnable thing**. Waiting for an answer is not a state this
programme has.

The user asks about that section roughly daily. When they do: discuss it,
answer with the options and the cost, and **keep the current work running
meanwhile** — the discussion is not a checkpoint.

When the user agrees a shape and says to fix it, the item becomes the **next**
work item: finish the segment in flight, commit it, and take the agreed item
before the next queued row. The segment boundary is the insertion point because
it is the first moment the tree is clean — one worker at a time in a shared
tree means a new worker cannot start over another's uncommitted edits.

**That section must be true.** An item that has landed is deleted from it the
same day; a list carrying two already-fixed entries is what stops it being
read. An entry with no recommendation is a question the supervisor has not
finished thinking about, not an escalation.

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

**The first run of a series on this box is not a result** (2026-08-31, [[KI-92]]).
Five consecutive runs of *one identical arm* of `bench.workflow --scenario chain` read
93.2 / 33.3 / 23.1 / 24.2 / 23.1 workflows/s — a **303 % spread over the five, 4.8 %
over the plateau**. Disk accumulation and package thermal throttling were both refuted
(a sixth cold run after idle came back at 91.1; the throttle counter never moved). So a
floor computed from a series that includes its own warm-up is not a floor, and a row
whose arms are not interleaved can read a warm-up as an effect. **Discard until the
plateau, then interleave, and report how many plateau samples you took.**

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

**A session hands over with no worker alive.** Ending a session — or declaring a
handover point — while a subagent runs leaves a worker editing this tree that
the successor cannot see or address: `ListAgents` reports peer sessions with a
busy/idle flag and never their subagents. The check is that `ListAgents` shows
no `Subagents` section at all; anything still listed is stopped first. Prefer
letting a worker finish: its analysis lives in a context that dies with it, and
its output under `openspec/changes/` is gitignored, so a kill can destroy work
that appears in no diff. A worker killed mid-work has its remaining state
written to the board, because that is the successor's only trace of it.

**`ps` cannot see a worker.** A subagent runs inside the `claude` process, so
every OS-level check below is blind to it: an agent burning time and tokens
appears nowhere in `ps`, in the load average, or in a file listing. Agent-level
hygiene is a separate instrument — `ListAgents`, read for elapsed time and spend
— and it belongs at the START of a sweep, not as an afterthought. A supervisor
that only runs `ps` is checking the half of the machine its workers do not live
in.

**Attribute the CPU, do not just count the leaks.** A sweep that greps for known
leak shapes finds only the leaks it already knows. Every sweep therefore also
sorts by CPU and rules each consumer *logical* or *leaked*, by name:

```
ps -eo pcpu,etime,pid,ppid,user,cmd --sort=-pcpu | head -12
uptime          # 1-min BELOW 15-min = the box is quieting; above = something started
```

A consumer that cannot be explained is investigated before it is killed, and a
process you cannot attribute to yourself is asked about, never killed (`CLAUDE.md`).

**Long-lived here and NOT leaks** — killing one is a real cost, so they are named
rather than left to judgement: `epmd -daemon` (the Erlang port mapper; every BEAM
test run leaves it and it costs nothing idle), one `playwright-mcp` node process
per live Claude session, and the `claude` processes of the peer sessions sharing
this tree. What IS a leak after a run has finished: any `beam.smp`, `java`, `mvn`,
`cargo`, `rustc`, `rust_runner`, or a `node`/`python`/`ruby`/`php` test app.

**The strongest evidence a run tore down cleanly is a negative one**: immediately
after an e2e or bench arm, none of those names appears at all. Record that, not
"looked fine".

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

## 11c. Two workers, on disjoint file sets

Amended 2026-08-31 on the repository owner's instruction, after four days of work
reached 42 % by production LOC and the owner asked for more throughput.

**The one-worker rule was never about workers. It was about the shared tree.** Two
agents editing one working tree cannot see each other, and neither can the supervisor.
So the rule becomes explicit about what it protects:

- **Two workers may run concurrently if and only if their file sets are disjoint**, and
  the supervisor states the disjointness before spawning the second. `sdk/java` and
  `cli` share no file; `5a` and `5b` do.
- **A survey needs no isolation at all** — it changes no code, so it may run in the
  main tree beside an implement worker. Start parallel work with a survey and the
  setup cost is zero.
- **An implement worker on a second area takes a `git worktree`.** Note what that
  costs here: symlinked `deps`, a scratch `MIX_BUILD_PATH` and `CARGO_TARGET_DIR`, its
  own `FLEZHA_SOCKET_FILES_PATH`, and a real NIF build — `RUSTLER_SKIP_COMPILATION`
  leaves `Transport.Native` undefined and the app will not boot.
- **A worktree arm is not comparable to a tree arm** (§8). Anything measured stays in
  the main tree; a worktree is for editing, never for a benchmark row.
- **The supervisor still verifies and commits both**, one checkpoint at a time. Two
  workers, one committer — the serialisation that matters is at the commit, not the
  edit.

### What was cut alongside it, and what was not

Cut as waste, same amendment: the **double full-suite run** (the worker runs it, the
supervisor re-ran the whole thing rather than §4's narrowest slice covering the
riskiest move); the **heavyweight Maven profile** on mid-area segments
(`-P coverage,native-verify clean verify` belongs at the close-out, not every
checkpoint); and **survey-then-implement for rows under ~2 000 lines**, where one
verify-then-fix worker is enough — rows 5+6 proved it.

**Not cut, because this week priced both**: the **negative control** on a regression
test — the one time it was accepted on report rather than run, a test that passed on
broken code shipped and cost a whole segment to repair (KI-67) — and the **red-first
drive** before a fix, because row 6.5 segment B was fully green, 1 870 tests and clippy
clean with both filed defects fixed, while the transport still discarded 20 % of a
burst under an `:ok`. Speed comes from not doing work twice, not from believing claims
that were never checked.

## 11d. Lanes — how two workers stay out of each other's way

Added 2026-08-31, after the first concurrent pair collided on `docs/known_issues.md`
KI numbering (the `cli` survey found KI-76 taken and skipped to 79). Content survived;
the numbering did not. That collision is the specification for this section.

**Every resource two workers can touch is one of two kinds, and they need opposite
remedies.**

- **Partitionable** — files, directories, number ranges, output paths. Remedy:
  **allocate disjoint slices up front**, in writing, before the second worker spawns.
- **Singleton** — a thing there is exactly one of, which cannot be split. Remedy:
  **one holder, or isolation.** Treating a singleton as partitionable is what fails,
  and it is what failed here.

### A lane is a worker's written allocation

Every concurrent worker is given a **lane**, stated in its brief and recorded on the
board *before* it spawns:

| field | meaning |
|---|---|
| **paths** | the only paths it may write. Everything else in the tree is read-only to it. |
| **dev note** | `docs/dev_notes/<change-id>.md` — one per change, so disjoint by construction. |
| **singletons held** | which of the list below it holds, or `none`. |

A lane is exclusive on write and unrestricted on read. Two lanes must be disjoint on
paths, and the supervisor states that disjointness in the brief — not as a hope, as a
claim it has checked.

### The singleton list, and who may hold each

| singleton | includes | holder |
|---|---|---|
| **the Elixir suite** | `mix test` in any form, `_build/`, `deps/`, `/tmp/flezha_sockets`, `/tmp/flezha_state_test_*`, the `FsyncBatcher` | one worker at a time |
| **the Maven reactor** | `mvn` in any form, `sdk/java/**/target/` | one worker at a time |
| **the cargo target dir** | `cargo build|test|clippy` | one worker at a time |
| **the git index** | `git add` in **any** form, including `-N` | **the supervisor only** |
| **`docs/refactor/STATE.md`** | the board | **the supervisor only** |
| **`docs/known_issues.md`** | the defect record | **the supervisor only** — see below |

### Workers no longer write `docs/known_issues.md`

This is the change the collision forces, and it costs nothing.

**A worker reports a finding in full in its report — `Where`, `Evidence` with the
command and its actual output, `A fix must`, the mechanism named to the function — and
the supervisor appends it.** Every §1b finding this week already arrived fully formed
in the report text; the file write was pure duplication, and it was the one shared
write left.

Three things fall out, all good: the last shared-write file becomes single-writer, so
the numbering collision cannot recur; the supervisor was **already** appending a
verdict to every entry, so this is one edit instead of two; and entries stop needing
correction after the fact — this week the supervisor had to re-point KI-55's
coordinates and move a verdict that had landed under the wrong heading.

**A worker still owns the reproduction.** The rule moves the *typing*, not the
*evidence*. A finding without a command and its output is still refused.

### When both workers need the same singleton

The second takes a **`git worktree`** with its own `MIX_BUILD_PATH`,
`CARGO_TARGET_DIR` and `FLEZHA_SOCKET_FILES_PATH`, and a real NIF build
(`RUSTLER_SKIP_COMPILATION` leaves `Transport.Native` undefined and the app will not
boot). Until then, **start the second track with a survey** — a survey holds no
singleton and writes no code, so it needs no isolation at all and can begin
immediately.

### The heartbeat check that makes this operational

At every heartbeat the supervisor runs `git status --short` and asks one question:
**does every dirty path belong to exactly one live lane, or to the supervisor?** A path
outside every lane is the alarm, and it is the only mechanical detector this scheme
has.

**What this does not do**, stated plainly: nothing *enforces* a lane. A worker that
ignores its brief can still write anywhere, and the check catches it after the fact
rather than before. The scheme buys a small, checkable invariant and a clear owner for
every shared thing — not a sandbox.

**The Elixir suite is not merely a convention-level singleton — it is enforced by the
OS, and the collision costs runs** (2026-08-31, measured). Two workers running full
suites concurrently produced, in one hour: an `EADDRINUSE :4500` abort (the test app
binds a fixed port), **two** aborts on a state-root lock held by the peer's pids, and one
`Waiting for lock on the build directory (held by process …)`. **Four wasted runs at
~60 s each**, plus the compile stalls. Nothing was corrupted — every failure is loud —
but the time is real and it is subtracted from what the parallelism buys.

**The rule, since two workers cannot see each other**: each runs the **narrow** slice
freely and the **full** suite **once, at its segment end**; on a lock or port collision
it **waits and retries, and never kills the peer's processes** — both workers did this
correctly and it is why nothing was lost. **Take every close-out measurement when the
other lane is committed and idle**, which the attribution rule above already requires for
a different reason.

**The `git worktree` alternative is real but is not free here**: it needs symlinked
`deps`, a scratch `MIX_BUILD_PATH` and `CARGO_TARGET_DIR`, its own
`FLEZHA_SOCKET_FILES_PATH`, and a genuine NIF build — `RUSTLER_SKIP_COMPILATION=true`
leaves `Transport.Native` undefined and the app cannot boot. That is a setup cost paid
against a 14 GB `target/`. It is worth it for a long parallel run and **not** worth it
for two areas that are each one segment from close-out.

**A whole-repo metric is not attributable while a second worker is dirty** (2026-08-31,
found by 5a mid-segment). `cycles`, `max_cycle_files` and full-scan `duplicate_pairs` are
computed over the tree, not over the lane. 5a watched `max_cycle_files` read 20 at HEAD
and 17 after a batch that touched no dependency edge, and `mix xref` report 4 cycles
where its own survey measured 7 — all of it 5b's uncommitted work. So: **read a
whole-repo number only against a tree that holds one area's work**, take every close-out
measurement when the other lane is committed, and say which commit the baseline was.
**Exactly three rows are movable by a peer**, corrected 2026-08-31 by 5a against the
supervisor's own looser statement: `duplicate_pairs`, `cycles` **and**
`max_cycle_files`. The last two come from **one** `xref_cycles()` output by the **same**
membership test, so there is no sense in which one is exposed and the other is not —
and 5c, 5d and 5e all cut cycles. The other **fourteen** rows are peer-independent by
construction, summed or listed over the area's own tracked files: `prod_loc`,
`test_loc`, `comment_lines`, `doc_lines`, `narrative_markers`, `sleeps`, `refutes`,
`files`, `test_files`, `comment_density`, `files_over_800`, `functions_over_50`,
`long_names`, `async_false_share`.

**The `test` area is the harder case and every area that folds tests shares it.** There
a peer's files are *members* of the area, not merely paired with it. Excluding them
moved **nine** rows, five of them ratcheted: `test_loc` 38 273 → 29 975,
`files_over_800` 7 → 4, `narrative_markers` 37 → 32, `refutes` 16 → 13,
`async_false_share` 0.811 → 0.865, `duplicate_pairs` 115 → 80.

**And the obvious experiment is the wrong instrument.** Dropping the peer's files from
`tracked_files()` left `lib/flezha/state`'s `duplicate_pairs` unchanged, which *looks*
like proof of independence and is not: a pair counts when **either** half is in the
area, and jscpd scans the **filesystem**, so a peer moves the number by editing its own
source rather than by being tracked. The exclusion test measures attribution, not the
scan. On record five deep: `test duplicate_pairs` read **118 → 109 → 115 → 109 → 115**
in one afternoon with 5a's own test files unchanged across the last four. Two workers
reading different numbers were both reading correctly, from different trees.

**NEVER `git add` a directory while a peer is live — it sweeps their in-flight edits into
your commit** (2026-08-31, and it produced a commit that does not compile). Staging
`lib/flezha/runner/` and `lib/flezha/service/operative_manager.ex` to commit area 5e-a
swept area 5e-b's uncommitted `alias Flezha.SafeCall` lines out of two files. The module
`lib/flezha/safe_call.ex` was still untracked in the peer's tree, so **`85f722cd` aliases
a module it does not define, with four live call sites** — an undefined-remote-call
warning, fatal under this repo's `--warnings-as-errors`, and an `UndefinedFunctionError`
on the fence-write and overwatch-state paths. It self-healed at the peer's next commit
(`a26b203b`), so only the intermediate is broken — but it is a **bisect point**, and a
bisect that lands there gets a build failure that has nothing to do with the bug being
hunted.

The rule was already written ("stage paths one by one, never a directory"; "run
`git diff <file>` and confirm every hunk is yours **before** `git add`") and the
supervisor broke it under time pressure, on the twelfth commit of an evening. **The
peer found it, from the object store, without checking the commit out** — because
checking out is destructive to a shared tree and a worktree costs the full §11c setup.
`git ls-tree` and `git grep <rev>` answer this question without touching anything, and
that is the technique to reach for.

**A rename owns its callers, wherever they live** (2026-08-31, first parallel segment).
Area 5a's `workflow_completed_retention_hours` → `completed_retention_hours` has a caller
in `lib/flezha/service/cli_manager.ex` — area **5c**'s file. That one-line edit is not a
lane breach and must not be sent back: a rename that leaves its callers broken is not a
rename, and the alternative — deferring every rename whose callers cross a boundary —
would freeze half the programme's cheapest work behind areas that have not started.

The rule: **a worker may edit a call site outside its lane when, and only when, the edit
is the mechanical consequence of a change inside it** — a rename, a signature change, a
deleted function's last caller. It may not edit anything else there, and it names every
such file in its report so the supervisor checks the diff is only the consequence. The
supervisor verifies by reading the hunk, not by trusting the count.

## 11g. A Mix task's `@moduledoc` is a product surface, not narration

Added 2026-08-31, ruling on area 5d's question R6. `use Mix.Task` moduledocs **are
`mix help` output** — the text an operator reads to use the task. They are exempt from
§5's 15-line ceiling for the same reason area 6's help screens were treated as
operator-visible text rather than prose to be trimmed: shortening them removes
documentation from a user-facing surface while the metric records an improvement.

Four of area 5d's nineteen over-ceiling docs are Mix tasks. They stay. Every other
moduledoc in that area is ordinary prose and the ceiling binds it.

**The general form**: a comment ceiling applies to text that only a reader of the source
sees. Where the text is *rendered* — `mix help`, a `--help` screen, a generated
reference — it is a product surface and is governed by whether it is correct and
complete, never by its length.

## 11e. The instrument flatters — three silences, one tendency

Added 2026-08-31 after the third was found. `scripts/refactor_metrics.py` has a design
tendency, not three bugs: **when it cannot measure something, it reports better than
the truth and says nothing.**

| silence | what it did | found by |
|---|---|---|
| `--lint` omitted | wrote `null` into every `credo_warnings`/`clippy_warnings`, and `--check` **skips a null on either side** — disarming 72 ratchets | area 4 segment A, in the diff |
| `--timing` given | **replaced** the timings map rather than merging, so recording one deleted the rest | area 4 segment F, recording one |
| **untracked files** | invisible to every metric, and since refactors *add* files the error is **one-directional** — a phantom −599 on `cli` | area 6 batch 1 |

**The operational rule, until [[KI-87]] lands**: the supervisor runs
**`git add -N` on every new file before reading any number** — a gate's exit code, a
metric, a `--check` verdict. It has caught a wrong number **five times** in one
session. A worker cannot do it (the git index is the supervisor's singleton, §11d), so
**a worker names its new files and the supervisor intent-adds them**. A number read
before that step is not evidence.

Two of the three are now loud: `--write-baseline` refuses without `--lint`, and
`--check` names the metrics it could not compare. The third is filed.

## 11i. A module move re-keys anything spelled `__MODULE__`

Added 2026-08-31 by area 5e-a, after two mechanisms fired that its Mimic census did not
cover. Both were caught by a **red test**, not by review, and neither is visible in a
call graph:

- **`Logger.put_module_level/2`** scopes a level to one module *by name*. Two tests
  raised the level of a module whose function then moved to a sibling, so three
  assertions read `left: ""`. The overrides now name both modules.
- **`:persistent_term` under `{__MODULE__, key}`** re-keys when the writer moves while
  the reader stays. Moving one writer turned **eleven** tests red, every one reporting
  "the stream is unbound". The writer went back to the parent, which owns that key and
  had no caller in the sibling.

**The rule: a module move re-keys anything spelled `__MODULE__` and anything a test
scopes by module name.** Before a public function moves, grep the pair of trees for
`__MODULE__` in a key position — `:persistent_term`, `:ets` table names, registry keys,
`Logger.put_module_level`, `:telemetry` event prefixes — and for the module's own name
inside test setup. §11d's stub census covers `Mimic.copy`; this covers the rest, and
between them they are what makes a move safe.

## 11h. A struct is an edge — and a call-graph tool will not see it

Added 2026-08-31 by area 5e-b, on the **ninth** designed split this programme refuted by
measurement. Every acyclicity tool written today resolved **local function calls between
groups**. None of them saw that a sibling pattern-matching `%Parent{}` — in a head, a
`@spec`, or a `%{state | …}` update — **is a dependency edge in `mix xref`**, whether or
not it calls anything.

The proof is one line, and it is the technique to copy: add
`def probe_struct_edge(%Parent{}), do: :ok` to a candidate sibling and re-measure.
5e-b's SCC went **63 → 66 and `loop.ex` entered the cycle it had been outside**;
reverted, back to 63. A bare type reference is enough — `diff.ex → intent.ex` exists in
the graph although `diff.ex` names `Intent` only in a `@spec` and a comment.

**The remedy is already a house precedent, twice**: put the `defstruct` in its own
module (`wal_writer/state.ex`, now `loop/state.ex`) so siblings thread the same struct
**without naming the module that owns it**. Where a struct cannot move, pass plain
values across the seam instead and let the owner keep the `%{s | …}` write.

**So a split's acyclicity check must resolve struct references, not only calls** — and a
tool that reports a clean partition without having looked at `%Mod{}` patterns and
`@spec` types has not checked the thing that fails.

## 11f. The ruler can also punish — a metric that moves against the work

Added 2026-08-31, from area 5a's survey. §11e is about the instrument reporting
**better** than the truth. This is the other sign, and it is more dangerous, because a
worker who trusts the number will *choose the wrong work*.

`cycles` is, per area, **how many of the repo's dependency cycles contain at least one
of this area's files**. `lib/flezha/state` sits in 2, and one of them is an 18-file
strongly-connected component of its own files. Simulated on the real graph:

| change | area `cycles` | largest cycle, in this area's files |
|---|---|---|
| HEAD | 2 | **20** |
| move three module names out of one 14-line function | **1** | 20 |
| the whole job — every cycle edge cut | **3** | **3** |

**Doing the work reports as a regression.** A worker optimising the ratchet banks the
fourteen-line move and never touches the eighteen files. The count is not wrong; it is
measuring the wrong thing — the *number* of cycles rather than how deeply the area is
tangled in any one of them.

**A second instance, found the same day by area 5b's survey — it is a pattern, not an
incident.** `async_false_share` is `1 − (async:true files) / (test files)`, an unweighted
**file** count, and `test` is BLOCKING at it. Charter §9 mandates folding
incident files into behaviour files. Folding nine websocket test files into three cost
3 `async: true` and 6 total: `1 − 40/224 = 0.821` — **a blocking regression produced by
obeying §9**.

The exact condition, which is more useful than "folding regresses": removing `k` files
of which `j` are `async: true` regresses iff **`j/k` exceeds `1 − async_false_share`, the
current share of true files**. **Read that share from the instrument, never from this
page** — it was `0.187` (43 of 230) when this section was written and `0.203` (46 of 227)
a few hours later, because two areas folded test files in between. A worker using the
stale figure refuses folds the ratchet would have allowed. (Corrected 2026-08-31 by area
5e-b, which measured it rather than quoting it.) **Compute the floor from the BASELINE share, never the current one** (added 2026-08-31,
after this number was misread three times in one day — by the supervisor at `0.187`, by
a worker at a stale `0.206`, and by the supervisor again using the *current* share,
which is circular: the ratchet compares now against baseline, so the requirement is set
by baseline). The arithmetic, once:

> `required_true = ceil((1 − baseline_async_false_share) × test_files_now)`
> `headroom = async_true_now − required_true`

At `e28de881`'s baseline of `0.797` with 205 files today: required 42, held 43,
**headroom +1**. And read `async_true_now` from the instrument — a `grep -rl 'async:
true' test/` sweeps directories `test_files` does not count and reads **47** where the
metric reads 43.

So a fold is free when it consolidates predominantly `async: false` files,
and must be funded when it does not. **Ruled: the programme accepts this arithmetic and
funds it** rather than reweighting — after folding to `N` files an area holds
`async: true ≥ ⌈(1 − async_false_share) × N⌉` **read from the instrument**, each
conversion naming the global state the test does not
need. Reweighting by test LOC would move an already-armed baseline under all 36 areas
at once, which is a re-baseline, not a fix.

**A third instance, and this one is about the ruler §11f itself installed.** Area 5a
split `wal_writer.ex` — one file inside a 56-file strongly-connected component — into
five. **No edge changed**: the siblings reach exactly the collaborators the single file
reached. `max_cycle_files` rose **17 → 21**, because five of the area's files now sit in
the cycle where one did.

So `cycles` and `max_cycle_files` are **both file-count proxies for an edge property**,
and a file split moves both. `max_cycle_files` fixes the failure `cycles` had (splitting
one SCC into three) and has a failure of its own (splitting one member into five). That
is worth stating plainly rather than adding a third ruler under pressure — **the mistake
to avoid is installing a new metric every time one misreads.**

**Ruled: no third metric. The reporting requirement changes instead.** An area whose work
touches a cycle reports **the edges it cut, by name**, beside `max_cycle_files` — edges
are what the work actually changes, and a worker enumerating them is already doing the
analysis. 5a cut four `→ wal_writer` edges as side effects of one extraction and would
have shown a flat metric without them. The number stays recorded-but-uncompared until an
area's close-out, so it blocks nothing while it is being read this way.

**The rule**: when a metric's direction and the work's direction disagree, **fix the
ruler before the area starts, in its own commit, with a cross-area simulation showing
exactly which rows move** — the guard area 4 segment A's classifier correction
established. Never leave it and "explain the regression in the report": the next area
inherits the gate, not the report. And never optimise the number you know to be
measuring the wrong thing — say so, and change it.

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
