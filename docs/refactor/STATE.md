# Refactor board

**Last updated**: 2026-08-31 — session 4. **Phase 1 complete (8 rows); areas 0a, 0b, 1s,
1, 2, 3, 4, 5a, 5b, 5c, 5d, 5e-a, 5e-b and 6 are ALL DONE.** **35 areas are blocking
ratchets and 100 % of `lib/` production lines are ratcheted** — the controller is
finished. `refactor_metrics.py --check` **exit 0** at `887bf308`. **Next: area 7**
(tests, helpers, test apps — 38 737 test_loc over 203 files, the last production-shaped
row), then area 8 (docs reconcile). Awaiting the user: KI-52, KI-80, KI-81, KI-100.

**The board table's rows 4 and 6 read `pending` until session 4 corrected them** — both
areas had been committed and armed hours earlier. Session 3's own header said so and the
table did not. **Fix the table in the same edit as the header, or the next session
classifies off a lie.**
**Resume**: open a session in this repo and run `/loop /refactor`. The skill
reads this file, `git status`, and the open change's `tasks.md`, then continues.

### Read this first if you are a fresh session

1. **The scratchpad is per SESSION.** `$SCRATCH/area3/` currently holds 160 MB of
   benchmark arms (A, A′, B2, B3, B4, B5, K1) — **a new session gets none of
   them**. If an area's A/B is unfinished when a session ends, the next one
   cannot bisect without rebuilding every arm, and an arm rebuilt in a `git
   worktree` is not comparable to one built in the tree (charter §8). So: finish
   an area's measurement in the session that started it, or write down which
   arms are owed. Session 1 lost area 2's arms exactly this way.
2. **Check the machine every 30–60 minutes** — charter §11b and `CLAUDE.md`'s
   "Machine hygiene". Kill what you started. A `top` snapshot lies about
   benchmark load; count runs over a window instead.
3. **The defect list moved.** Everything found and not fixed now lives in
   `docs/known_issues.md` (tracked, severity-ordered, each with evidence and
   what a fix must account for). `fix-outbox-watermark-overwrite` closed KI-1;
   `fix-first-input-drop-on-bind` (KI-2) is still owed **before** the controller
   area that refactors those files.
   `openspec/changes/` is gitignored, so those proposals are machine-local —
   `docs/known_issues.md` is the durable record.
4. **The charter was amended four times on 2026-08-29/30**, each time because
   doing the work exposed a rule that could not be satisfied: §1 (net lines vs
   the §7 ceiling), §7 (the function ceiling is production-only, matching what
   the gate always measured), §8 (build every checkpoint, measure once), §11
   (lines arriving from a higher layer, and ceiling-forced excess). Read them
   before planning an area; they are not the rules a previous session followed.
5. **A defect now STOPS the area — charter §1b, amended 2026-08-30 on the
   user's instruction, and the skill's §6b.** The old rule ("record it, fix it
   in its own change later") is gone: it produced forty entries of which eleven
   were already fixed or never true and five could not be reproduced at all.
   Now — finish the box in flight, stop, write it down, **reproduce it** (a
   command and the output it gave; 30-minute timebox, after which it is a
   recorded suspicion and work resumes), establish the mechanism named to the
   function, and only then triage: real and worth fixing / not a bug / not
   worth it. The verdict is the supervisor's, never the worker's, and it
   escalates to the user for a behaviour trade-off or a fix bigger than the
   area. A fix is its own commit with a red-first regression test, and the code
   it touched is then refactored to the area's standard like any other code.
**Note**: `openspec/changes/` is gitignored — open and archived changes live on
this machine's disk only; their `tasks.md` is still the fine-grained progress.
**Rules**: `charter.md`. **Programme**: zero behaviour change, net code down,
one host core, tests fast and real.

## Status

Done: Phase 0, 1s, area 1 (kernel), area 2 (all five segments, `5154bceb`) and
area 3 (segments A–F, `c5bdc26b`). `flz_core`, `flz_transport`, `flz_nif`,
`flz_core_ffi`, the four veneers, `sdk/rust` and `flezha-macros` are all BLOCKING
ratchets. NEXT: **area 4 (`sdk/java`, 8 441 lines)** — its only dependency, area 1,
is done.

**Progress, measured not estimated**: ~42 % by production LOC. The 45 % figure this
board carried earlier was my own count of board ROWS, which over-counts badly: the
five controller areas still ahead are 66 K production lines on their own — 46 % of
the codebase — and they also have to cut the 55-file dependency cycle, which is the
hardest work in the programme and has not started. Area 4 (`sdk/java`) sits between
here and there.

**The history rewrite has been undone.** `fix.sh` (an untracked `git filter-branch`)
ran at 14:52 on 2026-08-29 and changed every commit hash on this branch — while
removing nothing: the tree and blob object sets were byte-identical either side. It
left the branch on a history disjoint from `origin`, which a later
`git reset --hard ORIG_HEAD` then fell off, dropping 43 commits.

All 43 were replayed onto origin's untouched history and pushed as a fast-forward,
so **this branch is back on the same history as `origin/dev` and no remote hash ever
changed**. Every hash cited in this board, `CLAUDE.md`, `docs/known_issues.md`,
`docs/dev_notes/`, `.git-blame-ignore-revs` and `baseline.json` has been repointed to
the surviving commit and verified to resolve. The pre-recovery state survives on
branch `RESCUE-area3-c1c54a10`; `backup-20260829-145208` and the other 14 stale
branches are deleted, with `archive/*` tags kept for the three that held unique work.

The bloat `fix.sh` was aimed at is real and untouched — 2.05 GB of blob bytes, 1.78 GB
of it seven committed Java artefacts. It now has a proposal:
`openspec/changes/unbundle-committed-build-artifacts`.

## Board

Order is dependency order; an area starts only when its dependencies are `done`.

| # | Area | Change id | Depends on | Status | Commit | Gates before → after |
|---|------|-----------|------------|--------|--------|----------------------|
| 0a | Baseline, gates, formatters | `refactor-0-baseline` | — | done | `4a0a3bac` | gates now measure; `blocking: []` |
| 0b | Test suite mechanics (flakes, sleeps, fleets) | `refactor-0-tests` | 0a | done | `9847ff7d` | sleeps 212→2, refutes 107→12, ExUnit 96.7 s→56.2 s; `test` now blocking |
| 1s | Dispatcher-unification A/B spike (numbers only) | `refactor-1s-dispatcher-spike` | 0a | done | `54f8cc11` (+ branch `spike/1s-dispatcher`) | converge; ≤ 1.45 % loss on every resolvable row, allocs flat |
| 1 | `flz_core` + `flz_transport` + `flz_nif` | `refactor-1-kernel` | 0b, 1s | done | `379d795c`, `65d0dc9a`, `024efa60` | −1 851 dead lines; host/engine/driver/transport by owner; one error enum per crate; doc density 25→15 %, markers 21→0, clippy 24→0, files>800 7→0; A/B: erased +5 %, roundtrip +1–13 %, typed −1.4 % (area 3) |
| 2 | `flz_core_ffi` glue + Node/Ruby/PHP/Python veneers | `refactor-2-ffi-veneers` | 1 | **done** | A `8981157e`, B `d6f6e16f`, fmt `ec0a8725`, C `7decf389`, D `3939cdac`, E below | one veneer core, one shape, the ABI scenario by owner, validators by rule family; prod 15 285→15 192, test 8 769→8 415; fn>50 0, files>800 0, narrative 0, sleeps 7 in all five areas; all three A/B rows clean; **the five areas are now BLOCKING ratchets** |
| 3 | `sdk/rust` + `flezha-macros` | `refactor-3-rust-sdk` | 1 | **done** | A–F: `119a20d4`, `024daaaa`, `be54a5cc`, `d54b61f4`, `588094e4`, `c5bdc26b` | **fn>50 12→0 and 2→0, files>800 9→0, long_names 9→0**, narrative 20→2, sleeps 9→3; three flakes cured; **rows 1/2 carry ~2–4 % from segment B's seam — accepted, re-measure owed**; both crates now BLOCKING |
| 4 | `sdk/java` | `refactor-4-java-sdk` | 1 | **done** | `91699f03`…`352af734` | **files>800 4→0, fn>50 0, long_names 0, markers 0**; prod_loc 8829, duplicate_pairs 92; prod +372 / test −62; javap diff empty (163 public types each side); J1 detects nothing (+0.03 % trimmed vs an 11.61 % floor) |
| 5a | Controller `state` | `refactor-5a-state` | 0b | **done** | `9bb349dc` (+6) | **files>800 3→0, fn>50 1→0, long_names 2→0, markers 17→0, cycles 2→1, 14 edges cut**; prod_loc +67 ceiling-forced; KI-41+KI-61 red-first; 14th blocking |
| 5b | Controller `plugin` | `refactor-5b-plugin` | **none (5a dep refuted)** | **done** | `826c227c` (+9) | **cycles 4→0, mcf 3→0, fn>50 9→0, markers 8→0, credo 51→0**, files>800 3→1 by ruling, prod_loc −66; KI-93+KI-95 red-first; 15th blocking |
| 5c | Controller `service` (+ `cli_manager` split) | `refactor-5c-service` | 5b | **done** | `a92b79a1` (+7) | **files>800 3→0, fn>50 4→0, markers 4→0, dup 8→0, credo 53→42; 23 edges cut**; cli_manager 4294→15 modules; 16th blocking |
| 5d | Controller `observability` + `mix` tasks | `refactor-5d-observability` | **none (5c dep refuted)** | **done** | `a92b79a1` (+5) | **fn>50 2→0 and 13→0, files>800 2→0, cycles 1→0, markers→0**; obs prod_loc −215; 17th+18th blocking |
| 5e-a | `runner` + `operative` | `refactor-5e-a-runtime` | — | **done** | `2b1355f4` | **every ceiling 0/0, credo 25+2→0, 11 edges cut**, area −33 |
| 5e-b | `convergence`, `sdk`, `authz`, root, rest | `refactor-5e-b-platform` | — | **done** | `2b1355f4` | **every ceiling row 0 across 15 areas, credo 80→39, 9 edges cut, repo cycles 2→1** |
| 6 | `cli` | `refactor-6-cli` | 1 | **done** | `b73c940c`, `b5b0b1d8` | **every ceiling 0** (files>800, fn>50, long_names, markers); prod_loc 2831, duplicate_pairs 3; **−438 honest** (raw −513, 75 lines a classifier artefact); 31 help screens golden-pinned; both A/B rows `direction`, sha256 recorded in their place |
| 7 | Tests, helpers, test apps | `refactor-7-tests` | 2,3,4,5e,6 | **proposed** (approved 2026-09-01) | | |
| 8 | Docs reconcile (`CLAUDE.md` index, dev_notes names) | `refactor-8-docs` | 7 | pending | | |

Status values: `pending` → `surveying` → `proposed` → `implementing` →
`checkpoint` → `done`; `blocked` names the blocker below.

## Current worker

**Session 4, 2026-08-31 21:40 → 2026-09-01.** All decisions in this session were taken
with the user present; every one is recorded in *Next steps* → **THE WORK QUEUE** and in
`docs/known_issues.md`. Nothing agreed lives only in a transcript.

**NO WORKER IS ALIVE. Session 4 closed cleanly 2026-09-01 on the user's request.**

Row 1's worker (the `check-kind-naming` gate) was **stopped deliberately** three minutes
in, for a clean handover. **It wrote nothing** — `git status --short` is empty, both
gates are byte-identical to how it found them (`check-kind-naming` **exit 1**,
`check-overflow-naming` **exit 0**), and no analysis was lost that is not already in its
brief. Row 1 starts from scratch; the brief is in the row-1 detail below.

**A finding from the kill, and the successor needs it: the count moved 19 → 22 while
nobody touched code.** The three new hits are **this board's own prose** — the supervisor
writing *about* the gate ("a bounded fix", "the bounded await") makes the gate redder.
`docs/refactor/` was already the largest single source at 8 of 19. Two consequences: the
allowlist must cover `docs/refactor/STATE.md` as a path rather than chase individual
phrases, and **the count in any brief goes stale as soon as the board is edited** — always
re-run the gate before quoting a number.

**Two workers completed and were committed this session**: the area 7 survey (approved,
see below) and `retire-cluster-snapshot-remnants` (`f3c30502`, archived).

## START HERE IF YOU ARE A FRESH SESSION

**Run `/loop /refactor`. It reads this file, `git status` and the open change's
`tasks.md`, then continues. Everything you need is below — in this file.**

**Do these three things in order:**

1. **Read *Next steps* → THE WORK QUEUE.** Ten rows, each with status and the date the
   user decided it. Row 0 is done; row 1 was in flight when session 4 ended (see *Current
   worker* above for how to tell where it got to). Rows 1–5 and 8–10 are small-to-medium
   and independent of the areas.
2. **Check for a live worker.** `ListAgents` — **a fresh session has no subagents no
   matter what this file says.** If *Current worker* names one and none is running, the
   tree holds whatever it finished; resume from there.
3. **Area 7 is APPROVED and ready to implement** — it is the last big refactor area.
   Its proposal is at `openspec/changes/refactor-7-tests/` and `openspec list` shows it
   with 0/47 tasks ticked.

**The one thing that does NOT survive a machine change**: `openspec/changes/` is
gitignored (`.gitignore:55`), so area 7's 1 052-line proposal exists on THIS machine only.
It is not in any commit and appears in no diff. If you are on a different box, it must be
re-surveyed — the survey took ~20 minutes and its findings are summarised under *Area 7
survey — VERIFIED and APPROVED* below, which is committed and does survive.

**Status in one paragraph.** All 35 production areas are blocking ratchets; 100 % of
`lib/` production lines are ratcheted; the controller is finished. Areas 0a, 0b, 1s, 1, 2,
3, 4, 5a–5e and 6 are done. **Area 7 (tests) is approved and unstarted; area 8 (docs)
follows it.** `refactor_metrics.py --check` exits 0. The full suite is 1922 passing.
**`## Awaiting the user's decision` is empty of open items** — KI-3, KI-52, KI-80, KI-81,
KI-100 and the Kafka rename were all answered on 2026-08-31/09-01 and are queued or done.

**CI is red on `main` and mostly not our problem** — six failing workflows, four causes,
and two of them are fixed on `dev` already (`main` is 287 commits behind). See *CI is red*
below. Only the naming gate (row 1) was red on `dev`.

### The five things area 7 must know before it starts

1. **The fold arithmetic, pinned today after being misread three times** (charter §11f):
   `required_true = ceil((1 − baseline_async_false_share) × test_files_now)`. At the
   recorded baseline of `0.797` with 202 files: required **41**, held 43. **Read
   `async_true` from the instrument** — a `grep -rl 'async: true' test/` sweeps
   directories `test_files` does not count and over-reads by four.
2. **Owed to area 7 by name**: area 5b's three test files over 800 need a shared case
   module under `test/support/`, which only area 7 owns. `test files_over_800` is 6.
3. **A fold is a concatenation, not a semantic merge** — the sources carry different
   module-level `setup` blocks, and a `setup` that silently stops running is a test that
   passes while checking nothing (5e-a, 44 → 20 files, 286 tests before and after).
4. **A `@moduletag` does not survive a fold.** 5e-b found one `:capture_log` moduletag
   that would have silenced another module's correlation tests; it became two per-test
   tags so the uncaptured-log gate still sees them.
5. **`test/flezha_test.exs:419` is a filed flake with seven sightings** and its exposure
   was corrected today: **concurrent load, not maven**. Two workers hit it while a peer
   lane ran its own suite. Do not chase it; do check a failure against
   `docs/known_issues.md:889` before blaming your own change.

### Open, and needing the user rather than a worker

`docs/refactor/STATE.md` → *Awaiting the user's decision* has four items: **KI-52**
(five native triples vs one), **KI-80** (`--json` vs `--output json`), **KI-81**
(`docker/prod` calls two CLI verbs that do not exist), **KI-100** (the `Source`
behaviour with zero implementers — delete it, or make the claim true), and the Kafka
plugin queue still spelling `:block` where the SDK says `drop_nothing`.

### Filed and unfixed, worth a worker's time

**KI-101** (`bench.workflow` prints `in 1.0e3 ms` on a round millisecond — a test exists
that this path does not cover), **KI-102** (four small defects in `runner`/`operative`,
three dead code), **KI-103** (a dead function a dev note calls the convergence Loop's
remedy, where the Loop mints no such op — decide which claim is true and fix the note in
the same commit), **KI-94** (`check-kind-naming` red on 23 prose hits, none in code),
**KI-90** (`narrative_markers` cannot see `task 2.7` or `§5`).

### One discrepancy left unresolved

5e-b reported `scripts/format-check.sh` red at HEAD on `lib/flezha/service/cli_manager.ex`.
**I measured `mix format --check-formatted` on that file twice and it exits 0 both
times.** Either the script's per-file loop differs from the direct invocation, or the
condition was transient. Worth one minute before anyone trusts either reading.

### Area 5e-a survey — VERIFIED and APPROVED 2026-08-31

Proposal at `openspec/changes/refactor-5e-a-runtime/`, `--strict` valid, tree clean.
**Both 5e lanes are approved and implement in parallel.**

**It rebuilt the dependency graph and reproduced the known cycle sizes — 62 and 4 —
before using it for anything.** Every split was then checked, and **all three were
cyclic as first drawn**: `fleet_manager` on two parent back-edges, `lifecycle` on
`complete_init → cancel_wake_timer` at its second attempt, and `stream_channel`'s
apparent back edge was **a local variable named `init`, not a call** — read at the
source and dismissed. That is now **eight designed splits refuted by measurement today**.

**RULING — E7 is approved, with a condition.** E1 is the §11f case in the open: it cuts
**three real edges and moves no metric at all.** Without E7 the area ends at `runner`
`max_cycle_files` 10 and `operative` 9 — **both regressions for work that improves the
code**; with it, `runner` is flat at 9 and `operative` 9 → 8. E7 edits `lib/flezha/state`,
which is blocking, so the condition is arithmetic rather than permission: **`state`'s
`prod_loc` row may rise by exactly the lines moved, and only if `operative` falls by at
least as much** — net non-positive across the pair, accounted by cause in the commit.
That is the same accounting already accepted today for 5c's root relocation (+130) and
its credo +2, and refusing it would let a ratchet forbid a correct cross-area move.

**Three of the four handed-over items were REFUSED or re-priced, and the reasoning is
better than the hand-over.** `ClassRegistry`: by §3's own order `state` sits *above*
`operative`, so those nine edges are **legal today** — the move turns 3 breaches internal
and creates 3 new ones, **net upward breaches zero**, while pushing 359 lines into a
blocking area. E7 moves the ~60 lines that are the actual cause instead.
`OperativeForward`: moving it *down* makes its `→ service` breach span one more layer,
and moving one of nine sibling providers is §2's parallel-structure smell — priced at
+500 lines, 3 files, one breach worse, **zero edges cut**. `live_writers/1` confirmed,
**with two catches the hand-over missed**: a local caller, and two tests that
Mimic-stub the very delegation being deleted.

**And it named the risk that has bitten twice today**: a moved public function silently
changes which calls a Mimic stub intercepts. Task 0.4 enumerates every stub before
anything moves.

**KI-102 filed** — four small defects, three dead code. `Builder.from_json/1` cases over
`Jason.decode!/1`'s **decoded term**, so it raises on every valid JSON, and has zero
callers; and a `@doc` describing `connect_handler/4` sits above `list_factories/1`, so
that function's published documentation describes a different one. I verified both at
the source. All four are ordinary deletion-funded work — nothing is reachable, so there
is no red to drive.

**It corrected itself before shipping**: an apparently empty `@doc ` was an
artefact of its own `sed` window, and it fixed the design rather than dropping the claim.

**A gap in my lane model, found by it and now closed** (charter §11d): two surveys are
two subagents of one session, so `$SCRATCH` is the **same directory** for both, and the
peer's script overwrote its mid-analysis — silent, and it looks like a bug in your own
code. Every worker now writes under `$SCRATCH/<lane-id>/`.

**`targets.md` stale again — the fifth and sixth areas in a row.** `stream_channel.ex`
listed at 806, actually **983**.

### Area 5e-b survey — VERIFIED and APPROVED 2026-08-31

Proposal at `openspec/changes/refactor-5e-b-platform/`, `--strict` valid, tree clean.

**It corrected the charter, and the correction was mine.** §11f carried `0.187` (43 of
230) as the fold threshold because I wrote it there this morning; the instrument reads
**0.203** (46 of 227) tonight, because two areas folded test files in between. A worker
obeying the page would **refuse folds the ratchet allows**. §11f now names the instrument
as the source and says explicitly not to read the number off the page (`6b7d3bc3`,
`be2eb9bf`). It also caught that its own brief undercounted its functions over 50 — seven,
not five — and that the largest is in the root row, not `convergence`.

**Nine edges cut by name**, and the shape of the analysis is what earns it: A1 is a
**two-into-one fold** where both sites are the identical `GenServer.cast` and all three
files already name the target, so **no edge is added**. It then simulated the result
rather than assuming it — A1 alone leaves a 2-file SCC, and **every acyclic cut of the
quartet must cut one of two edges that are both on the per-record path.** That is why A2
is put to me as a ruling instead of taken.

**RULING — A2 is approved**, with the fallback recorded. Moving which module owns a
one-line `GenServer.cast` wrapper adds no call, removes none, and changes neither the
message, the target process nor the ordering; the risk is locality, not behaviour. It is
what takes `sdk cycles` 1 → 0 and **repo cycles 2 → 1**. Its four falsifiable structural
checks are the proof, and if any fails the area **stops at A1 and records `sdk cycles`
1** rather than forcing it.

**RULING — C1 is REFUSED whole**, which is its own fallback. The largest duplicate
cluster is `healthz ↔ observability/export` (3 pairs, 379 tokens), and `observability`
closed today and is **blocking**. The worker *measured* that moving only its half
**re-anchors the clones rather than removing them** — so half the fix is worse than
none. Recorded as owed to a cross-area change that can take both halves.

**The dead-code sweep paid, and it caught something about 5c.** `Flezha.Transport.Protocol`
is a whole dead module — I verified the only reference in the tree is its own
`defmodule`, and its two stream-type constants are a **third** copy of values that live
in `flz_transport/src/transport/protocol.rs` and are re-declared in `transport/server.ex`.
Eleven public functions have zero callers anywhere, and **two of them arrived at the root
three commits ago in 5c's relocation** — `Flezha.Config.get_provider_info/0` and
`Flezha.Identificator.stream_listener/2`, each with exactly one hit in the repo, its own
`def`. Not a defect in 5c's work (they were dead before the move) but a good argument for
sweeping *after* a relocation. ~112 lines, all deletion-funded.

**Fragmentation: both refused, with prices.** `authz` 18 files / 1 386 lines and `sdk`
17 / 1 749 are **earned** — each small module owns a noun with its own validation, the
surviving jscpd pairs are `@doc`/`@spec` header runs, and folding three of them saves
~20 lines while building the `util` bucket §2 forbids.

**And it answered the bucket question better than I asked it.** The root *is* becoming a
bucket, and it is **the least-bad one**, because `areas()` derives the metric table from
`lib/flezha`'s directories — so a new `lib/flezha/util/` forces a re-baseline of all 36
areas. Its proposed rule is worth keeping: *root iff a subtree facade, the OTP entry, or
a zero-out-edge leaf named by ≥ 3 trees.* `boot_loader.ex` fails all three.

**A/B: none proposed, with the argued case.** A1/A2 move four one-line cast wrappers —
no call added or removed, same message, process and order — so the effect is exactly
zero and §8's `direction` class applies. It explicitly refused to substitute a made-up
floor, citing KI-92: a floor for `bench.roundtrip --runtime elixir` would have to be
measured **on that driver** first.

**Its one spec delta is KI-72's shape**: *"/healthz never lies"* promises a 503 on two
verdicts that `flz_nif/src/server.rs:111`'s `stats: None` makes **unmintable on QUIC**.
The delta states the transport scope and carries all seven existing scenarios across
verbatim. It does **not** close KI-72 — the Rust arm is out of lane, and it says so.

**`targets.md` is stale on its rows — the fifth area in a row.** `baseline.json` was
exact on 12 of 13 comparable rows across all fifteen areas, the 13th being the documented
scan-set effect.

### Areas 5c and 5d — DONE — `a92b79a1`, both archived

**Eighteen blocking areas** (`service`, `observability`, `lib/mix` all armed at
`e0055657`).

| | 5c `service` | 5d `observability` / `lib/mix` |
|---|---|---|
| `files_over_800` | **3 → 0** | 0 / **2 → 0** |
| `functions_over_50` | **4 → 0** | **2 → 0** / **13 → 0** |
| `narrative_markers` | **4 → 0** | **3 → 0** / **1 → 0** |
| `duplicate_pairs` (full) | 11 → **3** | 27 → **13** / 3 → **2** |
| `cycles` | 1 | **1 → 0** / 0 |
| credo | 53 → **42** | 32 → **31** / 20 → **15** |
| `prod_loc` | 5 792 → 5 817 (+25) | 4 148 → **3 933** / 3 360 → 3 474 |

**`cli_manager.ex`, 4 294 lines — the largest file in the repository — is fifteen
modules and a 109-line facade**, largest piece 535. Its module name could not move:
`Flezha.Service.CliManager` is a **wire identifier** built into RPC strings at 55 sites
across 14 `cli/src` files. The surface is proved, not assumed — 178 exported pairs → 139,
and I checked independently that **none of the 20 dropped names is called from `cli/src`**,
statically or through the seven generic helpers that build a name from a variable. A
guard test now resolves 67 call sites out of the Rust source and was driven red twice.

**The area's real result is the edge ledger: `lib/flezha/** → service` 60 → 37, twenty-
three cut, accounted by name** — 17 `Identificator` + 2 `Payloader` + 4 `ConfigManager`.
`transport → service` and `boot → service` reach **zero**. I re-derived the 17 myself:
the union at the pre-move commit is exactly 17, one file carrying both alias forms.
And `payloader.ex` **left the cycle outright** (63 → 62) because the worker checked
`construct_operative!/2`'s callers instead of believing the design — nothing calls it.

**5d's ledger is one edge, and the metric was its smaller half**: `Registry → Bus`, a
single `Bus.emit/1` inside `emit_budget_exceeded_event/1`, was also the P2 that let a
`:block` sink hold the registry for **30 s** while every source registration queued
behind it on a 5 s default.

**Four defects fixed red-first, three filed.** KI-98 (the ruler), KI-99 + the `Sink`
callback, KI-97, F1/F3 — and **KI-100** and **KI-101** filed.

**What today proves about the method, in three lines.**

- **Five designed splits were refuted by measurement** before a file moved — 5a's two
  destinations, 5b's D5/D6/D7, 5c's `hub_manager` and `operative_manager`, 5d's
  `roundtrip`. Not one shipped wrong. A split nobody checked for cycles is not a design.
- **The baseline diff caught an unattributed rise for the fifth time today.**
  `operative credo_warnings` +1 from alias order the rename disturbed: baseline
  restored rather than committed, cause fixed, area now **two below** its baseline.
- **A refusal found a defect.** 5d was told to fold four duration renderers, measured
  first, found **5 of 2 016** inputs disagree, and stopped on its own rule — which is
  how KI-101 (`in 1.0e3 ms`) was found. Taking a rule literally beat eyeballing two
  functions that look alike.

### Area 5b — `lib/flezha/plugin` is DONE — `826c227c`, archived

**The fifteenth blocking ratchet.** Ten commits.

| metric | before | after |
|---|---|---|
| **`cycles` / `max_cycle_files`** | 4 / 3 | **0 / 0** |
| `functions_over_50` | 9 | **0** |
| `narrative_markers` | 8 | **0** |
| `credo_warnings` | 51 | **0** |
| `files_over_800` | 3 | **1** (by ruling) |
| `duplicate_pairs` (full scan) | 14 | **13** |
| `prod_loc` | 7 581 | **7 515** (−66) |

**No `lib/flezha/plugin` file is in any dependency cycle** — six edges across four
cycles, each named. The ledger records what the metric hides: **the `:via` inversion
alone does not cut the kafka and forwarder cycles**, because with the worker→sup edge
gone the supervisor is an *intermediate* node, not the back-edge. Only the telemetry
edge closes them.

**Two defects fixed red-first** — KI-93 (`build/1` raising where its own `@callback`
says it returns: 44 fields across 7 providers → 0 across 9) and KI-95 (the operator-
facing parameter order coming from the **atom table**, so it varied with what the VM had
loaded). KI-95's fix also **deleted a mechanism that read `@enforce_keys` out of the text
of a compiler error message.** **KI-97 filed** — `mix bench.extension` dead since a
false-positive rename treated a *Kafka* partition as a Flezha Entity.

**Its judgement, which is what the numbers do not show.**

- **Its own plan was the defect, twice.** A1.3/A1.4 would have recreated two modules a
  landed commit deleted on purpose; it built them, measured a blocking gate red, and
  **reverted**. Then D5, D6 and D7 were all cyclic as designed — measured before a file
  moved, all three.
- **It reported two of its own 24 extractions as behaviour changes**, naming the suite
  rather than its reading as what caught them. Two-in-twenty-four is the rate to assume;
  a batch claiming zero is the one to distrust.
- **It refused four folds with the price stated**, including *"6 lines before, 6 after,
  and the shared function's answer discarded"* — a helper whose answer you throw away is
  not shared.
- **It corrected a figure I had already endorsed** (41 → 44 raising fields), unprompted.
- **`files_over_800` finishes at 1 and that is the answer**: `producer_worker.ex` at
  1 322, four genuine mutual lifecycle edges, **no struct to pull** because the state is
  a bare map. A recorded 1 322 beats a split that re-forms the cycle just removed.

**The task record is honest**: 126 ticked, **18 unticked each with its verdict inline** —
reverted, refuted, ruled out, refused with its price, condition false, instrument
produces no number, held and owed. **Two boxes whose own text asserted something now
known false are marked FALSE in place**, because that text goes into the archive and
someone will read it as a claim.

**Owed to area 7**: E2.4's three test files over 800 need a shared case module under
`test/support/`, which area 7 owns. `test` `files_over_800` holds at its baseline 7.

### Areas 5c and 5d — surveys VERIFIED and APPROVED 2026-08-31

Both proposals `--strict` valid, both written while peers were live, neither touching a
file. **Both are approved. Neither implements until 5b closes** — two implement workers
is the ceiling on a shared Elixir suite (§11d), and the measured cost of exceeding it is
already recorded.

**5d's `5d depends on 5c` is refuted, one-way and weak** — three read-only references
out, one in via `metadata_text/1` which the change does not touch. **They implement in
parallel**, which is the second dependency this programme has found to be nominal rather
than real.

#### The fourth ruler defect — KI-98, and it lands on 5c's best change

`refactor_metrics.py:459` keeps `mix xref`'s ` (compile)` / ` (export)` suffix on a
cycle member, and `summarise()` tests membership against bare `git ls-files` paths — so
**every annotated member is invisible to both `cycles` and `max_cycle_files`.** I
reproduced it: **ten** members in the real graph carry an annotation. Four areas
under-report, `lib/flezha` at **46 against a true 56**.

**It lands exactly on the highest-value change in 5c.** That area's headline is one
line: `operative_manager.ex:23`'s `@id_in_use Validator.id_in_use()` is a **module
attribute**, hence a compile-time dependency, and it is the `1 compile` in
`Cycle of length 56 (1 compile, 9 export)`. Cutting it reports **nothing** today — and
had the edge merely been downgraded, the file becomes visible and the row goes
**5 → 6, a regression for progress**. Fix first, own commit, red-first self-test, and
**no third metric** — §11f rules that out and this is a correction, not a new ruler.

#### 5c — `cli_manager.ex` 4 294 lines → 15 modules + a facade, ACYCLIC

**The module name cannot move**: `Flezha.Service.CliManager` is a **wire identifier**,
built into RPC strings at **55 sites across 14 `cli/src` files**, five of them generic
helpers taking the function name as a parameter. So the file becomes ~91 one-line
`defdelegate`s — and the survey **ran Elixir 1.20.2 to confirm `defdelegate` accepts
default arguments** rather than assuming.

**It was not acyclic as first drawn, and neither was `hub_manager`.** Both were refuted
and repaired before reaching the proposal. Its method also caught itself: attributing
`@doc`/`@spec` blocks to the wrong function invented a nine-member SCC, *"caught by
disbelieving it"*.

**Twenty edges cut by name**, and the structural finding under them: **`lib/flezha/service`
is two things** — a service layer of 5 522 lines and eight utilities of 326 that are not
services. `Identificator` alone is named by **ten areas**. Moving three of them to the
existing root-module convention cuts **24 upward breaches for ~140 relocated lines**.

**5a's delegation handover is partly refuted**: `operative_manager.ex` calls none of the
four, `registered_ids/0` has **no caller anywhere**, and `lookup/1` has **15** — one 5c's,
one 5d's, one 5e's and 13 in the blocking `test` area. 5c can fund exactly one.

**The eight `plugin → service` breaches**: four cuttable, four not, and the proposed
surface is *a `plugin` module returns what it wants done and the caller performs it* —
5b's `Extension.Table` seam widened. **Deliberately not a behaviour**, because a
`service` behaviour that `plugin` implements is the exact cycle shape 5a's own new spec
requirement forbids.

**Risks it named that I would not have**: `Mimic` intercepts remote calls but not local
ones, so routing a sibling's call through its facade **silently changes which calls a
stub intercepts**; and the RPC surface is invisible to the compiler, so a missed
delegation compiles clean on both sides and fails on an operator's machine.

**Also found**: two dead modules (`Service.Warden`, a never-started GenServer with a
typo'd table `:flecha_warden`; `Service.PluginHelper`) with zero references anywhere,
and nine public CLI wrappers no CLI calls. **Ruled: delete all of it**, per KI-82's
precedent — deletion-funded work is the best kind.

#### 5d — the instrument tree, and three defects

**`roundtrip.ex`'s own eight section rules are CYCLIC as a partition** (`config →
engines → config`). The survey built a comment-stripped intra-file call-graph tool and
DFS'd every partition — which is the third area today to find its obvious split cyclic.

**It refused to build an A/B row and said why.** The Prometheus funnel is the one real
hot path and the existing bench provably cannot reach it; rather than propose a row that
cannot resolve the effect, it names the ~40-line harness that would be needed, refuses
it, and offers a **byte diff** over the funnel instead — stronger than any row, because
the change does not touch it.

**On refactoring the instrument itself**: an extraction that moves a statement across a
timestamp changes what the bench measures and **no A/B can detect it**, because both arms
would run the new code. Four checkable proofs replace measurement, including a `mix help`
golden capture taken **before the first hunk**.

**RULINGS on its three findings.**

- **F1 — real, P2, and it becomes batch 0, red-first.** `registry.ex:480` emits from
  inside its own `handle_call`, into `:block` sinks with a **30 s** ceiling, and
  `bus.ex:136`'s `pid == self()` guard cannot fire because `self()` is the Registry. So
  every `register_source/1` queues behind it on a 5 s default and a boot-time source
  exits `:timeout` in `init/1` — KI-44's cascade through a different door. It changes
  which process blocks, so it is its own commit and not folded in.
- **F2 — delete the callback and correct the spec.** I verified: `Sink.handle_signal/2`
  is declared at `sink.ex:57` and **has no invoker** — the only calls to that name in the
  tree are `shutdown/signal_handler.ex`'s own unrelated private function. Delivery is
  `{:signal, …}` answered in `handle_info/2`. Four of eight implementations are no-op
  stubs and `prometheus_sink.ex` duplicates its own live body. Four spec clauses say it
  SHALL be invoked. **A spec requirement that has never been implemented is a false
  statement about the system**, and keeping dead code so the text stays literally true is
  backwards.
- **F3 — real, fix it in 5d.** `bench.workflow` still carries the runtime clause list
  KI-51 removed from its two siblings, so `--runtime node` is refused though its manifest
  exists. It survives the gate because the gate's task list names only two tasks.
- **R6 answered as charter §11g**: a `use Mix.Task` moduledoc **is `mix help` output**, so
  §5's ceiling does not bind it. Trimming rendered text removes documentation from a
  user-facing surface while the metric records an improvement.

**And it caught two silences in the ruler**: `comment_density` and `doc_lines` are in
§11's prose but **not in `RATCHETED`** — a worker must not chase them; and `sleeps: 0`
hides **21 production `Process.sleep/1`** in the area, which is KI-91's `in_test` gate
working as designed.

**`targets.md` is stale on every row both areas own — the third and fourth areas in a
row.** It is a Phase-0 snapshot. `baseline.json` was exact on every row for both.

### Area 5a — `lib/flezha/state` is DONE — `9bb349dc`, archived

**The fourteenth blocking ratchet.** Seven commits.

| metric | before | after |
|---|---|---|
| `files_over_800` | 3 | **0** |
| `functions_over_50` | 1 | **0** |
| `long_names` | 2 | **0** |
| `narrative_markers` | 17 | **0** |
| `duplicate_pairs` | 7 | **5** |
| `cycles` | 2 | **1** |
| credo | 28 | **26** |
| `prod_loc` | 6 844 | 6 911 (+67, ceiling-forced, counted by cause) |

`wal_writer.ex` **2 021 → 666** behind five siblings split by the state they mutate;
`proc_journal` 946 → 704, `input_log` 907 → 776. **Fourteen dependency edges cut, named
one by one**, the four-file backend cycle gone and the big cycle's *compile* edges 3 → 1.

**Two defects fixed red-first** (KI-41, KI-61) and **KI-92 filed** from the A/B floor
measurement.

**What this area is actually worth remembering for is its instrument work.**

- **The ruler was wrong and got fixed before the area started** — `max_cycle_files`,
  now in the baseline and comparable from here on. Then it turned out to have **its own**
  blind spot (a file split inside a cycle raises it for unchanged edges), which is in
  §11f as the third instance, with the ruling that the *reporting* changes and no third
  metric is added.
- **§11e produced its most convincing catch**: the untracked read returned a number
  *identical to a plausible baseline*. The worker's first reading of the consequence was
  wrong and it corrected itself — it was never a `+6` regression; the real risk is that
  **`--write-baseline` against an under-reporting tree arms a ratchet below reality**,
  which is precisely the moment the blindness does most damage.
- **A re-record rewrites every row**, so a regression silently becomes the new normal.
  Diffing the new baseline against a byte copy caught `test credo_warnings` rising by
  one — two pre-existing hits in this area's own test files that none of its commits
  touched. Fixed to 82, better than it started, and re-recorded.
- **It corrected my own instruction**: three rows are peer-movable, not the two I named,
  because `cycles` and `max_cycle_files` come from one `xref_cycles()` output by the same
  membership test. In the `test` area a peer's files are *members*, and excluding them
  moves nine rows, five ratcheted. Charter updated (`6eedcbb9`).
- **The flip is proved in two arms with a control** — same regression, same numbers,
  exit code flipping purely on `blocking` membership.

**Task 4.3 closed on a false condition, not skipped**: it reads *"if the log is still
over 800"* and `input_log.ex` is 776, which I verified before archiving.

**Owed to later areas, recorded**: three uncut upward breaches by name for 5c, four
`Supervisor` delegations that can be deleted outright once their callers re-point, the
last compile edge inside the 56-cycle (`service/operative_manager.ex`), and 5e's
`ClassRegistry` question — measured as changing the cycle by nothing while adding ~450
lines to a now-blocking area, so recorded and not taken.

### Area 5b — KI-93 fixed and segment B mostly landed — `17ec6993`, `153a9434`

**KI-93 is closed and it was the right call to stop for it.** The generated `build/1`
now returns the error its own `@callback` documents. **44 raising fields across 7
providers → 0 across 9**, and the golden capture over four surfaces differs in **exactly
5 rows**, every one a raise becoming its documented error — which is how you show a
behaviour change is the *only* one.

**I re-drove the red myself**: HEAD's macro restored under a copy, the new test run,
**4 of 8 failed** on the same split the worker reported, file restored byte-identical.
The 4 that pass are the refusals the fix must not touch — a red-first that also pins
what must *not* move.

**The check I made mandatory was done before editing, not after**: no test asserted the
raise. And there is **one honest deviation, reported rather than taken silently** —
`:377`'s literal arm could not survive, because `params_data` would have to be a map and
the reason in one slot. Its *behaviour* became reachable; its text would have recreated
the double wrap. That is the right way to deviate from a supervisor's wording.

**Segment B: three folds taken, two REFUSED with the price stated.** `worker_registry`
had a **third** copy of the swap notification the task never named. `installer`/`loader`
carried one classifier under two names. Refused: `filter.ex`'s boolean parse (6 lines
before, 6 after, **and the shared function's answer discarded — the tell that it is not
shared**), and the three tree walks (a new module to save 11 lines, net −3 and one more
file, while the third shape *keeps* non-regular entries the others drop, so it is not a
copy at all). `prod_loc` 7 581 → **7 531**, `functions_over_50` 9 → 8,
`duplicate_pairs` 13 → 11.

**B1 and B2.1 are HELD, not skipped** — both edit `provider_behaviour.ex`, which right
now carries the KI-93 fix alone. Landing a refactor of that file would make the two
unsplittable by path and defeat §1b.6. That is the rule being understood rather than
recited.

**The suite's one failure was confirmed, not dismissed.** `flezha_test.exs:419` matches
`known_issues.md:889` **to the field** — same test, same assertion, same competing
`drain_progress` message, same `migrated: 0, forced: 0, remaining_hubs: 0` shape. I read
the entry. Fourth-plus sighting of a filed flake, and the diff reaches no shutdown or
drain code.

### Area 5b segment A — VERIFIED and COMMITTED `c78f7deb` — and the area's OWN PLAN was wrong

**The headline is that the plan was the defect.** Tasks A1.3/A1.4 would have recreated
`Kafka.Telemetry` and `OperativeForward.Telemetry` — **the exact inverse of `055054f2`**,
which deleted both on the judgement that a `Telemetry` sibling is its parent's own event
block and not an earned namespace. Both paths sit in `check-module-naming.sh`'s RETIRED
list; building them turns a **blocking** gate red. The worker built it, measured the gate
red, and **reverted** rather than routing around, and its own grep refuted the design's
justification: the emitters have **one** caller, not the two D3 claimed. I confirmed the
deletion commit and the gate entries myself.

**RULING: the telemetry placement DEFERS to segment C, after the split.** Not a dodge —
the split decides whether those emitters serve one module or several, so choosing now is
choosing without the information, and §2 forbids creating a module before it is earned.
If the split leaves several callers, the module becomes earned and the RETIRED entry is a
decision to revisit **with evidence**, not to route around. A2.4's `cycles → 0` moves to
segment C; **`cycles` 4 → 2 stands and is real** (postgres's 2-cycle genuinely cut).

**What landed is a layering fix, not metric-chasing.** `Extension.Table` is a data
structure that was issuing effects — two `PluginManager` casts inside `unsubscribe/2` and
`teardown_all/1` — and that single edge was `plugin`'s only route into the 55-file cycle.
The pair now answers `{table, dropped_ids}` and `Operative.Plugins.unregister/1` casts.
All four call sites stay one line; `plugin → service` **9 → 8**, `operative → service`
unchanged at 12. `prod_loc` **7 581 → 7 551**.

**Its neutrality was proved, not argued** — the `try/catch` around the cast was dead code
(a cast to an unregistered name returns `:ok`), the reported set equals what the old loop
cast in the same enumeration order, and a beam-chunk import check on `Table` was observed
**red against the HEAD-compiled module and green after**. I verified the source side: 0
`PluginManager` references remain in `table.ex`, and I read the four `lib/flezha/operative/`
hunks — each is the mechanical consequence, per §11d's new rule.

**KI-93 filed and ruled fix-first.** The generated `build/1` **raises where its own
`@callback` says it returns**: `Enum.reduce_while` halts with `{:error, binary}`, the next
expression destructures it as `{status, map}` and calls `Map.has_key?/2` on the binary, so
the `convert_status == :error` arm — the author's own evidence of intent — is unreachable.
**7 of 9 providers, 41 fields.** I reproduced it. It is invisible because `build/1` has no
production caller, and it sits inside the exact macro segment B rewrites, so §1b.6 puts
the fix first. The worker stopped for that reason and stopping was right.

**A caveat that matters for the record**: every one of 5b's suite runs (1878 passed) was
against a tree carrying 5a's uncommitted work. That is the cost of two workers in one
tree, it is not a clean read of either area, and 5b re-runs once 5a's next commit lands.

### Area 5a batches 0–1 — VERIFIED and COMMITTED

| commit | what |
|---|---|
| `7e5b9884` | the ruler: `max_cycle_files` beside `cycles` |
| `e32f1c99` | **KI-41** — a park is durable before a nested window can pass it |
| `c6137a8b` | **KI-61** — `RecordLog`'s specs name the module that has the type |

**I re-drove KI-41's red myself** rather than accepting the claim: reverted the two-line
swap in a copy-protected edit, ran the single test, got
`[consumed: 2, park: 1, consumed: 1]` — KI-41's published order function for function —
then restored the file and verified it byte-identical by sha256. **This is the KI-67
discipline**: the one time a negative-control claim was accepted unrun, a regression
test that passed on defective code shipped. It costs two minutes.

**The ruler's own control is load-bearing, also verified by me**: dropping
`max_cycle_files` from `RATCHETED` fails `self_test_cycle_depth` on the ratchet
assertion by name. And the new metric immediately justifies itself — `plugin` has the
**highest** cycle count (4) and the **shallowest** tangle (3); `state` has 2 cycles and
depth **20**. The old number ranked them backwards.

Every one of the eleven area rows re-measured and unchanged; `--check` exit 0 both
sides; full suite **1873 passed** in 64 s with the uncaptured-log gate clean.

**KI-61's proof is the honest kind.** No red-first test was constructible — the defect
is invisible to the compiler and only Dialyzer sees it, and this repo gates no Dialyzer.
The worker **did not manufacture one**; it read the compiled artefact instead, which is
stronger than the entry's own check because it reads what the spec resolved to:
`{Registry, :entity_id}` → `{Flezha.State.Paths, :entity_id}`, with `Elixir.Registry`
confirmed to export no such type. A sweep of every spec in the directory found this was
the only instance, and the residue (stdlib plus one Protox module) was verified to
really export what it names.

**KI-92 filed, and charter §8 now carries the rule.** Batch 0's floor measurement found
this box's `bench.workflow` chain row runs **93.2 / 33.3 / 23.1 / 24.2 / 23.1** on five
identical runs of one arm — 303 % spread over five, 4.8 % over the plateau — with disk
accumulation and thermal throttling both refuted (a sixth cold run after idle: 91.1; the
throttle counter flat). **A floor computed from a series containing its own warm-up is
not a floor**, and a row whose arms are not interleaved can read a warm-up as an effect.
S1 stays `direction` — 4.8 % on three plateau samples does not earn `guard`, and the
worker was right to refuse the promotion and leave it to me.

### Area 5b survey — VERIFIED and APPROVED 2026-08-31 — and 5a/5b are INDEPENDENT

Proposal at `openspec/changes/refactor-5b-plugin/`, `--strict` valid. Row → `proposed`.
Measured at `ecdafbf5` — it noticed HEAD had moved under it and said so, which is the
right instinct.

**THE SCHEDULE CHANGES. The board's `5b depends on 5a` is NOT REAL, and I verified the
edge count myself.** 5b → 5a is **8 references, every one `Flezha.State.Outbox`**, 5 of
its 16 public functions, no internal reach. 5a → 5b is **one call**:
`shipper.ex:517` → `ProviderBehaviour.retry_backoff_ms/3`, three pure lines. My own
grep found *three* module references and the other two are inside a `wire.ex` doc
comment — the survey's "one call" survived a check that tried to refute it.

**So 5a and 5b implement in parallel**, and the one thing that belongs to neither is the
retry ladder: 5 callers across `plugin`, `state` and `schedule`, so moving it would put
both workers in `shipper.ex` and `job.ex`. Recorded as owed cross-area, in both briefs.

**RULING — F1 is the second ruler defect of the day, and §11f is now a pattern.**
`async_false_share` is an unweighted **file** count and `test` is blocking at 0.813.
Folding nine websocket test files into three — exactly what §9 mandates — costs 3
`async: true` of 6 files and reports `0.821`, **a blocking regression for obeying the
charter**. I sharpened the condition beyond the report: a fold regresses iff the removed
set is more than **18.7 %** `async: true`, which also names the cheap path — consolidate
`async: false` files first. **Ruled: accept the arithmetic and fund it** (`async: true ≥
⌈0.187 × N⌉` after folding, each conversion naming the global the test does not need).
Reweighting by test LOC would move an already-armed baseline under all 36 areas at once;
that is a re-baseline, not a fix.

**KI-90 filed** — `narrative_markers` cannot see `task 2.7` or `§5`, so `describe`
blocks naming task ids that no future reader can resolve are uncounted; widening the
regex raises every area at once, so it needs the widening and a re-baseline in one
commit. **KI-91 filed** — a production `Process.sleep(2)` × 50 poll at
`worker_sup.ex:82` that the `sleeps` metric is *designed* not to see (`in_test` gate);
**real, not taken in 5b**, because replacing a bounded poll with a monitor changes a
restart path under a zero-behaviour-change contract with no instrument to prove it
neutral. **KI-31's count corrected** — its "nine `convert_value` copies" is nine
*definition* sites of which only four are folds, which sent readers hunting one that is
not there.

**KI-10 independently re-confirmed resolved** by this survey, on evidence gathered
separately from 5a's. Two workers reached the same verdict without seeing each other.

**Cycle work is real and cheap here**: area `cycles` **4 → 0**, repo 7 → 4, and the
55-file cycle 55 → 53 — from cutting `table.ex`'s two `PluginManager.unregister_plugin`
casts (`:456`, `:989`, both verified) plus three supervisor↔worker back-edges. The
charter's `plugin → service` breach count of **9** is confirmed exactly; this change
cuts one and the other eight need a callback surface that is 5c's to design.

**Seven provider-twin seams priced, four REFUSED with the price stated** — the three
`*_sup.ex` wrappers ("a `use` macro trades 45 readable lines for ~0"), `on_input`/
`on_output` (the matched tokens are the two dispatch lines; the bodies differ entirely),
the six `Ctx.*` structs, and kafka's overflow policy against the extracted bounded-queue
pair ("a parameter two of three callers never set is not a generalisation"). That is
§2 applied properly: reuse is the default, not an obligation to merge anything that
rhymes.

**A/B**: P1 provisionally `guard` **contingent on its own floor** — no floor for
`bench.extension` exists anywhere in the programme, so A vs A′ is measured first and it
reclassifies to `direction` if the instrument cannot hold 3 %. P2 and P3 are `direction`
and NOT RUN, P2 against a measured 5.1–12.1 % floor. **And it stated what a clean P1
cannot conclude**: `bench.extension` drives a provider that returns immediately, so it
says nothing about the Kafka producer path, and no row on this box can — the close-out
must say the producer split's per-record cost is unmeasured.

**`targets.md` is accurate for this area** (one line of drift), unlike 5a's and area
4's. It is stale where it is stale, not uniformly — so check it, don't assume either way.

### Area 5a survey — VERIFIED and APPROVED 2026-08-31

Proposal at `openspec/changes/refactor-5a-state/`. Row → `proposed`. Nothing outside
its lane was written; `git status --short` empty, confirmed by me.

**Every measured number re-derived and exact**: `prod_loc` 6 844, `files_over_800` 3
(`wal_writer` 2 021, `proc_journal` 946, `input_log` 907), `functions_over_50` 1
(`wal_writer.ex:586`, 51), `long_names` 2, `narrative_markers` 17, `cycles` 2,
`duplicate_pairs` 2, `comment_density` 0.348. Not one figure needed correcting.

**RULING 1 — the ruler is wrong and gets fixed first.** `cycles` counts *how many
cycles this area appears in*. Doing the whole job takes it **1 → 3** while taking the
area's largest cycle from **20 files to 3**: a worker optimising the number banks a
fourteen-line move and never touches the eighteen. **`max_cycle_files` is approved**,
in batch 0, before any file of the area moves, with area 4's cross-area simulation
guard. It degrades safely — `--check` skips a metric null on either side, so it is
recorded-but-uncompared until 5a's close-out `--write-baseline --lint`. Written up as
**charter §11f**: §11e is the instrument reporting *better* than the truth; this is the
other sign, and it is worse, because it makes a worker choose the wrong work.

**RULING 2 — KI-41 is fixed here, first, its own commit, red-first**, as the board has
said for weeks. Verified myself at HEAD: `await_entry/4` at `wal_writer.ex:1060`,
`apply_effect/2` at `:1062`. **The entry said `:1058`/`:1060`** — two lines of drift,
corrected in `known_issues.md`. The re-entrancy itself stays; a guard flag would be a
37th struct field and a second control-flow mechanism.

**RULING 3 — KI-10 is RESOLVED and 5a owes nothing.** The survey went looking for a
decision the register told it it owed, and found the whole thing already in the tree:
`.credo.exs` disables `ParenthesesOnZeroArityDefs`, row 3.5 *adopted* the other half
(`AliasUsage` 689 → 0), all 36 areas carry a recorded `credo_warnings`, and the spec
requirement exists. Moved to *Resolved*; the "area 5a owes the decision" line is struck
from both files. **An entry whose remedy lands elsewhere must be closed by whoever
lands it** — otherwise the next area's worker believes it.

**`targets.md` is stale on every row this area owns**, a second area finding the same
thing: `input_log` 822 → 907 (the KI-53 fix), `wal_writer` 2 019 → 2 021, `proc_journal`
945 → 946, and four line numbers drifted. It is a Phase-0 snapshot. **Read it for
orientation, never for a number.**

**Also established**: state owns 20 of the 55-file cycle and reaches out through 16
edges — and cutting all sixteen removes **no** cycle, because the area contains an
18-file SCC entirely of its own files. The cycle work is internal and needs no other
area to move. The A/B is one row (`bench.workflow --scenario chain`), floor measured in
batch 0 before any arm; `scripts/bench.sh` is **excluded with a reason worth keeping** —
every stress procedure declares `NoChange`, so the merge-gate row never calls
`WalWriter.append/4`. Cross-SDK: nothing owed, with the check recorded for the
implement worker.

**Not a defect, recorded so it is not re-diagnosed as one**: `wal_writer.ex`'s `@type t`
declares 23 of 36 struct fields. Elixir expands the rest to `term()`, so nothing is
mis-typed — but the spec stopped describing its struct, and the split is when to fix or
drop it.

**Held**: the 5a implement worker is NOT spawned while the 5b survey runs. Batch 0
edits `scripts/refactor_metrics.py`, which the 5b survey executes — a §11d singleton,
and the one real collision between these two lanes.

### TWO WORKERS FROM 2026-08-31 — charter §11c

The owner asked for more throughput after four days reached 42 % by production LOC.
**Two workers now run concurrently on disjoint file sets**, and area 6 (`cli`, 3 314
lines) is the second track — its only dependency, area 1, is done, and it shares **no
file** with `sdk/java`.

**The second track started as a survey deliberately: a survey changes no code, so it
needs no worktree and carries zero conflict risk.** The `git worktree` cost (symlinked
`deps`, scratch `MIX_BUILD_PATH`/`CARGO_TARGET_DIR`, its own socket path, a real NIF
build) is paid only when area 6 reaches *implementation*.

**Three cuts made alongside**, all waste rather than verification:

1. **The heavyweight Maven profile is gone from mid-area segments.** Segments A–C each
   ran `-P coverage,native-verify clean verify`; a class extraction needs neither
   JaCoCo instrumentation nor a native re-verify. Segment D was told mid-flight. The
   full form belongs at segment F. **The `Building jar:` tell still gets checked** —
   dropping `clean` makes the shade fixpoint *more* likely, not less.
2. **No more double full-suite runs.** The worker runs `mix test`; the supervisor was
   re-running the whole thing rather than §4's *narrowest slice covering the riskiest
   move*. Pure duplication, and it was mine.
3. **One verify-then-fix worker for rows under ~2 000 lines**, not survey-then-
   implement. Rows 5+6 proved it.

**Two things explicitly NOT cut, because this week priced them both**: the **negative
control** on a regression test — accepted on report once, and a test that passed on
broken code shipped (KI-67), costing a whole segment to repair — and the **red-first
drive**, because row 6.5 segment B was fully green, 1 870 tests and clippy clean with
both filed defects "fixed", while the transport still discarded **20 % of a burst**
under an `:ok`. Speed comes from not doing work twice, not from believing claims nobody
checked.

**None. AREA 6 IS COMPLETE, archived and committed.** Areas 4 and 6 both closed today.

### Area 6 — `cli` is DONE — `b5b0b1d8`

Three batches, archived as `2026-08-31-refactor-6-cli`. **`cli` is the thirteenth
BLOCKING ratchet.**

| batch | commit | what |
|---|---|---|
| 0 | `b0b27a30`, `a58f8144` | KI-79 (five verbs exiting 0 on a failed RPC) and KI-83 (`--timing` deleting timings) |
| 1 | `b73c940c` | one rpc seam, one help renderer, one dispatcher |
| close | `b5b0b1d8` | the baseline, the flip, the dev note |

**`prod_loc` 3 344 → 2 831, honest −438** (raw −513; 75 lines are the classifier
artefact of blank lines inside raw strings). Every ceiling zero,
`duplicate_pairs` **22 → 3**. **This is the first area in the programme to end
smaller.**

**The flip was proved armed, not assumed** — and that matters because these numbers now
block every future change. With `cli`'s baseline `prod_loc` lowered by one, the same
regression reports **BLOCKING / exit 1** with `cli` in the list and **measuring / exit
0** without it. Baseline restored from a byte copy and checksum-verified. Testing that
a newly-armed gate actually fires is the same instinct as proving a regression test red.

**It refused a flattering number twice.** The `include_str!` help encoding would have
looked like **−466** and was rejected because a `.txt` is invisible to every metric
while the text still exists. And the suite timing — **60.5 s against the baseline's
56.2** — was measured and **deliberately not recorded**, because this area changes no
Elixir, a release build had just run on the box, and recording it would relax a §9
ratchet on a load artefact. The time ratchet needs a re-measure on an idle box by
someone who means it.

**Neither A/B row ran, and that is a complete answer rather than a gap**: `cli` is on
none of §8's hot paths, nothing benchmarks the binary, and both floors were measured
*before* deciding — a 0.3–3.3 ms spread on a 1.7–3.8 ms median against an effect three
orders smaller. The arm shas are the evidence, with arm A taken at `dd04cd33` rather
than `81b3010c`, because batch 0 is a deliberate behaviour change and the pair that
isolates the *refactor* is the one after it. That distinction is easy to get wrong and
it got it right.

**KI-89 filed** for the two help misalignments the golden capture now pins — same
family as KI-88, and they should land together: one commit, three cosmetic
corrections, three golden re-captures.

### Area 6 batch 1 — three parallel mechanisms become one each

`prod_loc` **3344 → 2831**, `files_over_800` **1 → 0**, `functions_over_50` **8 → 0**,
`long_names` **1 → 0**, `duplicate_pairs` **22 → 3**. All supervisor-verified after
intent-adding the 42 new files; `cargo test -p flz` **77 passed**, `--check` exit 0.

**The golden file was captured before any edit** — 31 screens from the HEAD binary,
both known misalignments preserved byte-for-byte, negative control run twice. And the
renderer model was **proved in Python first** by decomposing and re-rendering all 31,
which found two screens the obvious model breaks on. That is how you refactor 506 lines
of hand-aligned text without changing output nobody would notice.

**It refuted its own survey's estimate by measuring.** The survey projected ≈ −200 lines
for the help table. Three encodings priced: structured tables **623 lines (670 after
rustfmt)**, a flat array ≈ 516, raw strings **468** against 506 today — a real saving of
**−38**. The win is 27 functions becoming one renderer, **not LOC**. And it **refused**
the encoding that would have looked like −466 (`include_str!` from a `.txt`), because
that file is invisible to every metric and the text would still exist. Refusing a
flattering number is the same instinct as [[KI-87]] below.

**Batch 0's reasoning was right in shape and wrong in detail, which only doing it
exposed**: six live failure policies, not three, and the message is not one string — 17
sites, four sites, and one embedding the child's `ExitStatus`.

**`duplicate_pairs` measured after the split ALONE: 22, unchanged.** Area 3's 5 → 65
shape did not repeat, because the cut is at function boundaries with no repeated
prologue. That is the risk the survey named, priced, and then found absent — which is
worth as much as finding it present.

**KI-84 fixed and measured**: 3 failures in 60 loaded runs before, **0 in 180** after,
and the non-gitignored directory went with it.

**Honest LOC −438, not −513** — 75 lines are a classifier artefact (blank lines inside
raw strings count as blank where `println!()` counted as code). It gave both numbers
and said which to quote.

### Charter §11e — the instrument flatters, and that is now written down

KI-87 is the **third** silence of one family, and the pattern is worth more than the
three fixes: `--lint` omitted wrote `null` and `--check` skipped it; `--timing`
replaced rather than merged; and the file walk reads `git ls-files`, so **untracked
files are invisible — and since refactors add files, the error only ever flatters**.
Batch 1 measured a phantom **−599** on `cli` that way.

**The operational rule is now charter §11e**: the supervisor runs `git add -N` on every
new file **before reading any number**. It has caught a wrong number **five times this
session**. A worker cannot do it — the git index is the supervisor's singleton under
§11d — so a worker names its new files and the supervisor intent-adds them.

### Area 6 batch 0 — two tools that reported success they had not earned

**KI-79**: five `flz` verbs exited **0** on a refused RPC. Proved red first against a
shim exiting 7 — all five at 0, the regression test naming all five in one failure —
and all five exit 1 after. The four *other* exit dialects (1, 7, 3, 7) are unmoved and
**now pinned by the same test**, so the seam refactor that follows cannot drift them.

**The reuse decision is the good part.** It extended the reference shape to five sites
rather than extracting a helper, and defended it: the six bodies are **not** one
function with a parameter, and a helper taking success message, failure message and
policy **is** `rpc::run(expr, Failure)` from the next batch — so extracting a different
six-site helper now would be a parallel mechanism that batch then deletes. Charter §2's
order stops at *extend*. That is the rule applied against its own grain.

**KI-83**: `write_baseline` replaced rather than merged, so recording one timing deleted
the others. Now a merge, proved red first with a test **in the existing `--self-test`
mechanism** rather than a second harness — which required splitting the function into a
pure `baseline_payload` and the write.

### Both silences are now loud, and one of them was mine to notice

- **`--write-baseline` without `--lint` REFUSES** (exit 2, before measuring, file
  untouched — supervisor-verified byte-identical either side). Refusal rather than
  merge-forward, because an area's credo count carries **no per-metric stamp**, so
  carrying the old number forward would attribute a stale count to a new head — the
  exact objection KI-83 raises about re-passing timings.
- **`--check` now reports what it could not compare**: *"not compared, unmeasured on
  this run: clippy_warnings in 36 area(s), credo_warnings in 36 area(s)"*. Printing
  `0 regression(s); exit 0` while silently skipping **72 ratchets** is the same
  success-it-did-not-earn as KI-79's exit codes. **Ruled: keep the line.** Exit code
  and behaviour are unchanged so CI is unaffected (`--check` verified exit 0); silence
  is what cost this programme, not one line of output.

### Two findings, filed as KI-84 and KI-85

**KI-84** is the best-diagnosed flake of the session. `cli/tests/reset.rs`'s "dead
controller" fixture hands the command under test a **live** socket, ~1 run in 26.
Mechanism **demonstrated, not inferred**: the fixture drops a `UnixListener`, and a
process a *sibling test* forks in the window inherits the listener fd — `CLOEXEC`
closes at `exec`, not at `fork` — so the socket the parent closed still accepts a
connect. Rates: 1/26 plain, 3/12 under load, **1/12 at HEAD content with the new test
removed (pre-existing)**, and **0/150 sequential**, which proves the CLI deterministic
and the harness at fault. It also leaves a **non-gitignored** `cli/app_data/sockets`
behind, invisible only because it was empty.

**KI-85** is the production question KI-84 raised and did not answer: `flz reset
--include-sockets` can `mkdir` on a later rung when the rung it was given looks live.
Not a defect — `fix-reset-socket-dir-resolution` documents that resolving as the
Controller *is* what "creatable" means — but a behaviour choice worth making
deliberately.

**And the worker found and fixed a race in its own new test in flight** — two `#[test]`s
each copying the binary hit `ExecutableFileBusy`, the same fork/`CLOEXEC` window from
the other side. Collapsed to one test with two phase functions, the `refactor-3`
precedent, and re-verified 0 failures in 12 loaded runs.

### Area 4 — `sdk/java` is DONE — `352af734`

Six segments, archived as `2026-08-31-refactor-4-java-sdk`. **`sdk/java` is now a
BLOCKING ratchet** (supervisor-verified in `baseline.json`), and `--check` is **exit 0**
against it.

| segment | commit | what |
|---|---|---|
| A | `91699f03` | the classifier correction — record headers and constructors miscounted |
| B | `c35d4ebe` | `StreamOperativeTopologyBuilder` 1 484 → 761 |
| C | `d4abc020` | `FlezhaProcessor` 1 075 → 148; twelve emitters → three |
| D | `5b59d1f9` | `ActivationDispatcher` 993 → 68 — the hot path |
| E | `7610ea3d` | the test split; every ceiling reaches zero |
| F | `352af734` | the measurement, the baseline, the close-out |

**Every ceiling is zero**: `files_over_800` **4 → 0**, `functions_over_50` **10 → 0**,
`long_names` **1 → 0**, `narrative_markers` **6 → 0**, `sleeps` **6 → 1**, credo 0,
clippy 0, cycles 0. Verified in the written baseline, not just the report.

### The A/B, and the analysis that beat my own read

Five paired rounds, interleaved, natives sha-identical on both arms. **No direction
detected** — geometric mean **+1.98 %**, **+0.03 %** with each arm's slowest reading
dropped, and the paired deltas span **12.36 points** against the row's own **11.61 %**
floor. F holds both the fastest of the ten readings and the slowest.

**It found an order confound I had missed.** I read the first four pairs as "mixed, 2
and 2". The worker saw *why*: the first three rounds **all ran A first**, putting F in
the hotter second slot every time. Rounds 4 and 5 reverse the order and the sign flips
again. That is a better reading of the same table than mine, and it is the reason the
row went to five — not §8's consistency test, which had already failed.

**And it wrote down what the row cannot say**, in `design.md` and the dev note rather
than leaving it implied: no JMH exists here, no row resolves the ≤2 % a class
extraction plausibly costs, and **the point estimate sits inside exactly that band**.
*A clean J1 is not "no cost."* The evidence that the hot path is intact is segment D's
**structural** check — 2 objects per operative and 0 per record, no interface so every
call stays monomorphic, every cached lookup enumerated.

### The line budget, stated plainly

**prod +372, test −62, net +310** against a ~150 estimate. **The estimate was wrong,
not the work.** Every line is accounted to a §7 ceiling; charter §1 says the ceiling
wins when the two pull against each other, and it won five times. Funding was **taken**
where it existed (C ≈ −127, E −50) and **refused** where it would have cost a
per-record allocation (D's three candidates) or public surface (~50 dead lines, KI-82).

### KI-83 — the instrument has a pattern, not two accidents

`--write-baseline --timing` **replaces** rather than merges
(`refactor_metrics.py:793`, supervisor-verified verbatim), so recording one timing
deletes every other. Segment F hit it recording `mvn_test_sdk_java`, avoided it, and
reported the trap.

**It is the same failure class as `--lint`**, whose omission writes `null` into every
credo count while `--check` skips nulls — silently disarming the ratchet row 3.5 spent
a whole change establishing. **Two arguments on one script, both destroying state when
you forget them, both silently.** That is a pattern in the programme's own instrument,
and both fixes go to area 6's batch 0 beside KI-79 — which is also a tool reporting
success it did not earn.

### Area 4 segment E — verified 2026-08-31, commit `7610ea3d` — every ceiling at zero

`files_over_800` **1 → 0**, `functions_over_50` **4 → 0**, `narrative_markers`
**5 → 0**, `long_names` 0, `sleeps` **6 → 1**. Supervisor-verified: **no file over 800
lines in `sdk/java`**, KI-55's test intact in its new home with the deliberately-wrong
assertion, `--check` exit 0.

**The test split preserved the per-class count exactly** — 29 → 3 + 8 + 8 + 5 + 5 — and
the Java total went 208 → **209**, where the extra is *coverage*: parameterising four
methods that drove five spellings means the diagnostics check now runs on **all five**
instead of only the first.

**The shared setup is extracted once, not copied five times**, which is the difference
between a split and a fan-out. Fifteen fixtures that were fifteen copies of
`factoryId()`+`channels()` are one base; the seven procedure fixtures that *were* the
twelve jscpd clones are one base.

**`sleeps` 6 → 1, and the five went by being replaced with observations, not shorter
waits** — a park on a latch nothing releases, a bounded spin on the clock itself, and
one released by the **fetcher drain itself**, because releasing it from the test would
let the dispatch finish before `close()` was entered and the assertion would pass
vacuously. That last reason is the segment in miniature.

**The area's line budget came back**: prod +12, test −62 ⇒ segment **−50**, area
**+360 → +310**. The +12 is ceiling-forced and counted line by line.

**`duplicate_pairs` +2, with the sharpest metric observation of the area**: of 24 new
pairs, **nineteen are the same clone re-anchored after a move** — the twelve
`StatelessTestProcedure` clones are now anchored at *one* copy in a base class instead
of twelve in one file. That is a **real reduction the metric cannot see**, and it is
segment B's blind spot running in reverse: jscpd counts cross-file pairs, so collapsing
twelve in-file copies into one shared base *raises* the number while removing the
duplication.

**Four coordinates went stale the moment these renames landed** — two inside defect
entries, two in docs. Re-pointed by the supervisor; the worker correctly declined to
edit a defect entry's `Where`.

**KI-82 filed**: seven unreferenced public members, plus `operativeId()` from segment
D. **Both segments found dead surface, both left it, and both said why** — every one is
`public`, and this area's contract is an empty `javap` diff. Correct twice. Worth a
separate change while the SDK is unpublished and removal is free.

### Area 4 segment D — verified 2026-08-31, commit `5b59d1f9` — the hot path

`ActivationDispatcher` **993 → 68**; `files_over_800` **2 → 1** (only the test file,
segment E's). Supervisor-verified: dispatcher at 68, `--check` **exit 0**, Java suite
**BUILD SUCCESS** on the new light `mvn verify`.

**This is the segment where measurement could not help, and it was handled by not
relying on it.** J1 is a `direction` row at 11.61 % spread, so the brief asked for the
three *categorical* failures to be checked directly. Each came back with a mechanism,
not a reassurance:

- **Allocation**: `EntityRouting` and `WorkflowDispatch` are constructed once per
  materialized operative — **2 objects per operative, 0 per record**. `StateExchange`
  and `EdgeTransforms` are static-only. And `EmitContext`, the one object minted per
  work item, **kept its single reference and its 11 fields** — task 6.2's literal
  reading would have given it four fields where there was one, growing a per-record
  allocation. It declined and said why.
- **Megamorphic**: no interface introduced; every new type `final` with one
  implementation, so calls stay monomorphic. Nested-private → top-level
  package-private is the **same bytecode** on Java 11 nestmates.
- **Cached lookups**: all preserved and enumerated — the `identityBytes` cache, the
  shared `edgeCodec`, the `procedureId`-indexed lists, both `ConcurrentHashMap`s, and
  the static `ObjectMapper`, which moved but is still one per classloader.

**Three funding candidates were rejected on that rule and recorded** so nobody
re-proposes them: merging the two run prologues needs a record *per record*; a lambda
over `prepareKeys`' arms captures `edge`; hoisting the `LinkedHashSet` allocates when
neither branch is taken. **Refusing funding for a stated reason is worth more than the
lines**, on a path nothing can measure.

**KI-55 is untouched and provably so** — `ActivationDispatcherTest` is byte-identical,
sha `90003eb79113` before and after (supervisor-checked), so the test asserting the
wrong value on purpose crossed intact. And `valuesForEntity`'s javadoc, the sentence
KI-55 calls false, was carried **verbatim** rather than corrected: that is the entry's
to fix, not a refactor's. That restraint is the right instinct.

**Three findings, all ruled:**

1. **KI-55's coordinates were stale the moment the file split.** Fixed here by the
   supervisor — the worker correctly declined to edit a P1 entry's `Where`. Both
   `known_issues.md` and `CLAUDE.md`'s index line now point at `EmitContext` and
   `EntityRouting`. A stale P1 coordinate is how a defect list stops being trusted.
2. **A log category moved** (`workflowRun`'s warn now under `…core.WorkflowDispatch`).
   Message byte-identical, no logback/log4j config exists in the repo, nothing greps
   the category. **Accepted** — and the worker was right not to call it zero.
3. **`OperativeInstance.operativeId()` has zero call sites** but is `public` on a class
   users reach via `Flezha.operatives()`. **Ruled: not funding.** Deleting it is an API
   change against an area whose contract is an empty public-signature diff. Noted for a
   future API review.

**The area is +360 against `design.md`'s ~150** (B +198, C +78, D +84). Segment F owes
an honest statement of that, not a quiet one. D's share is almost entirely file-header
cost and it said so rather than hunting for cosmetic deletions.

### Area 4 segment C — verified 2026-08-31, commit `d4abc020`

`FlezhaProcessor` **1 075 → 148**; `files_over_800` **3 → 2**, `functions_over_50`
**7 → 4**, `long_names` **1 → 0**. Supervisor-verified at 148 lines, only the two
`ActivationDispatcher*` files left over ceiling, `--check` **exit 0**.

**The reuse is the segment, not the file count.** Twelve `StateGenerator` emitters were
one function with a parameter — four factories, four variants, four handle mutators —
and are now **three**. `TypeMirrors` folds four copies of *"erasure equals X, arity N"*
onto one probe. `classLit` is **deleted outright**: dead, zero call sites. That is
deletion funding found inside the segment, which is what segment B's checkpoint asked
for: ≈ **−127** taken against **+205** forced by headers, models and the 50-line
ceiling.

**Byte-identity proved on two corpora, not argued.** A branch-complete synthetic corpus
of 26 sources through `javac -proc:only`: **29 files, 738 lines, `diff -r` identical,
and all 35 javac diagnostics identical including `file:line`**. Then the real corpus,
byte-identical across the rebuild. **The bad corpus is load-bearing** — two refusal
fixtures in both runs pin that the sink refusal fires *after* the procedure source is
written, which is why that check stayed in the render half instead of being hoisted.

**`tasks.md` named a corpus that does not exist** — `flezha-sdk-examples`' generated
sources, from a module that declares no `@State` or `@Procedure`. Corrected to the one
that generates.

### The stall call was right, and a correction against me was wrong

**Not replacing at 43 m was correct**: the worker was running the byte-identity proofs
and composing the report — work that produces no file, no process and no diff. Killing
it would have destroyed both corpora and the diagnostic comparison.

**It also reported that my stall-check commit `0efb0325` swept its `tasks.md` edits.
That is false, and I checked rather than accepting it.** `git show --stat` on that
commit is **one file, `docs/refactor/STATE.md`, 22 insertions** — and
`openspec/changes` is gitignored at `.gitignore:55`, so `tasks.md` cannot be committed
by anyone. Its ticks are in the working tree, ignored, exactly where they have been all
area. The hazard it names is **real** (§1's shared-tree rule, and this supervisor did
sweep a worker's edits on 2026-08-30) — but it did not happen here, and a hazard
recorded as an event that did not occur is how a board stops being trusted.

**KI-18 gained two more sightings and one genuinely new observation**: four of the five
across this module's two sites follow a **maven build immediately before the suite** —
a sharper reproducer than "under load", and worth more than the sightings themselves.

### Area 4 segment B — verified 2026-08-31, commit `c35d4ebe`

`StreamOperativeTopologyBuilder` **1 484 → 761**; `files_over_800` **4 → 3**,
`functions_over_50` **10 → 7**. Supervisor-verified: builder at 761, the three
remaining over-ceiling files are the ones segments C–E own, `--check` **exit 0**.
Nine new files, each named for what it *is*.

**Three rulings this segment needed.**

**1. No `dsl/` sub-package — a language rule, not a preference.** Java's
package-private access **does not cross into a sub-package**, so the directory tasks
4.1 and 4.2 ask for are mutually exclusive: getting `builder.dsl` costs seven members
and two types going `public`, against a change whose own proof obligation is an
**empty** public-signature diff. **Ruled: encapsulation beats a directory**; `design.md`
task 4.1 is corrected. The worker found this, refused to spend public surface
unilaterally, and asked — which is right, because that trade was not its to make.

**2. `prod_loc` +198 against a ~150 estimate — accepted, and the lever declined.**
Charter §1 is explicit: *"Clearing a ceiling and reducing total lines pull against each
other; when they do, **the ceiling wins**."* Every one of the 198 is accounted to §7 —
~100 for nine file headers and imports, ~18 for six inner classes gaining an `owner`,
~70 for the two extracted modules' explicit parameter lists, ~30 for the extracted
methods and accessors. That is a charter-admissible reason, unlike row 3.5's alias
lines, which had none. The offered lever (pass the builder instead of its parts, −27
lines) is **declined**: it buys 27 lines by making the rules no longer a pure function
of their inputs and forcing the staging fields package-private.

**But §1's other half binds too — "the area's total is what must fall."** Segment B
alone is +198 with C, D and E still to come and only **−14** of named funding (segment
E's fixture collapse and testing merge). **Segments C–E must look harder for deletion
funding, and segment F states the area's net honestly rather than quietly.** An area
that clears every ceiling and ends 400 lines heavier is a real trade, not a free one.

**3. `duplicate_pairs` 68 → 75 (area) / 81 → 88 (full), and `design.md` D6's stated
cause is REFUTED by measurement.** D6 said a split inflates the metric via a copied
import block. `removeUnusedImports` is configured, ran, and every new file carries only
what it uses — yet the metric still moved. The real mechanism: **`.jscpd.json` runs
`mode: weak` at `minTokens: 40`, which matches four to six consecutive long-FQN imports
against *different* identifiers**. Verified in the `--check` output: most new pairs are
41–52-token import runs between files in one package. Only two are real code (the two
`into` overloads, a duplication that existed at HEAD *inside one file*, where jscpd
could not see it).

The worker **measured the counterfactual instead of asserting it** — folding the six
handle classes into one file gives +3 rather than +7 — and declined it because it
contradicts the design's target table more than the package deviation does. That is the
right shape: price the alternative, then choose with the number in hand.

### Area 4 segment A — verified 2026-08-31, commit `91699f03`

Batch 0, the classifier correction. **No `sdk/java` file was touched** — this corrects
the ruler before anything is measured against it. Supervisor-verified: `--self-test`
**OK**, `--check` **exit 0**, and a baseline diff showing exactly three moved rows with
**zero blocking areas loosened**.

**The guard on a ratchet-loosening change came back clean, and the worker made it
stronger than I asked.** I required a cross-area simulation: 36 areas, **exactly one
moved** (`sdk/java`, `functions_over_50` 11→10 and `long_names` 7→1), the survey's
predicted numbers to the digit. It then did better — diffing the **complete Java
function list across all 251 files**, old classifier against new: **84 removals, 0
additions, 0 unexplained**, each verified from its own source line as a record header
or a constructor. A metric diff shows what moved; a full-list diff shows that nothing
moved for a reason nobody checked.

**The one loosened row is not the classifier**: `lib/flezha/state prod_loc` 6823 →
6844, which is row 7's own landed work, and that area is *measuring*, not blocking.

**The baseline used `--lint`**, so credo and clippy survive rather than being written
as `null` and silently disabling every credo ratchet — 36 areas, 0 credo nulls, repo
378. That trap has now been avoided twice by naming it in the brief.

### J1 is reclassified `guard` → `direction`, and the perf row is ruled

The floor was **declared before measuring** at >6 % ⇒ downgrade. Five arm-A runs
measured **11.61 %** end-to-end spread (14.47 % receive), so J1 was reclassified in
`design.md` in the same edit as its floor, and the two stale sentences elsewhere in
that document were corrected so it does not contradict itself. **That is charter §8
working exactly as written** — declare the class and the floor first, then honour the
measurement even when it costs you the row.

**The worker's own observation changes the answer, and it asked me to rule.** The five
readings rise **monotonically** — that is thermal drift on a laptop, not scatter — and
it noted that *"a paired, alternating A/B can still read a difference; an absolute
floor cannot."*

**Ruled: keep J1, run it INTERLEAVED, and state what it can and cannot catch.** §11b
says cut a row that cannot gate, and an absolute-floor J1 cannot. But drift **cancels
in an interleaved design**, which is why areas 2 and 3 used alternating arms and why
this row is not the same row when run that way. What it catches is the **categorical**
failure this split actually risks — a megamorphic call site, a lost `ClassValue`, an
allocation moved onto the per-record path — which are 2-5× effects, not 2 % ones. What
it cannot do is certify the absence of a small regression, and segment F must say so
rather than implying a clean row means no cost. **J2 stays cut**: never floored, and it
stalls at `--pool-sizes` 1/2/4 on this box.

### KI-76 — ruled at segment A: real, not area 4's, and half of it is the user's

Segment A filed it while setting up measurement. `sdk/php/composer.json` declares
`php >=8.1` with **no `config.platform.php`**, while `composer.lock` pins
`symfony/console v8.1.5`, which requires `php >=8.4.1`. The installed vendor tree
therefore carries PHP 8.4 syntax (`(new \ReflectionClass(...)->getAttributes(...)`)
that **no interpreter at the declared floor can parse**, so `php-cs-fixer` exits 255
on this box's system `php` 8.3.6.

**Its worst part is not the version skew.** `scripts/format-check.sh` then reports
`UNFORMATTED — php-cs-fixer`. Nothing is unformatted; the tool crashed. That is the
same failure class as `docs/dev_notes/format-gate-tells-the-truth.md` — **a gate
naming the wrong cause is worse than a gate that is simply red**, because it sends the
next person to fix formatting that is already correct.

**It also explains a dismissal.** Two workers earlier in this session hit this exact
`php-cs-fixer` parse error and recorded it as *"environment, pre-existing"* — true, and
causeless. It now has a mechanism.

**Ruled: real, P2, NOT area 4's work** — it is `sdk/php` and the format gate, and
folding it into an area whose contract is zero behaviour change is the scope drift this
programme removes. **The gate half is fixable by whoever next owns the format gate.
The floor half is escalated**: whether the PHP SDK supports 8.1 or 8.4+ is a
support-matrix decision, not a supervisor's.

### Area 4 — approved 2026-08-31, with one classifier ruling

`openspec/changes/refactor-4-java-sdk/`, `--strict` valid, **six segments, 61 boxes**,
and a real spec delta (one ADDED requirement on `refactor-gates` flipping `sdk/java`
to blocking, on the model of the three areas already there).

**Measured shape**: files>800 **4** (`StreamOperativeTopologyBuilder` 1484,
`ActivationDispatcherTest` 1392, `FlezhaProcessor` 1075, `ActivationDispatcher` 993);
functions>50 **11**; and the ratchets that will move — `narrative_markers` 6→0,
`sleeps` 6→1, `long_names` 7→1.

**The survey measured the risk that sank a previous area rather than fearing it.**
Area 3 learned that a split *multiplies* `duplicate_pairs`. This survey ran jscpd on
scratch arms before proposing anything: HEAD **68**; a 9-file split with the import
header copied → **84**; the same split carrying only the imports each file uses →
**69**; merging three testing files → **66**. The mitigation (`spotless:apply`, already
configured with `removeUnusedImports`, before any batch reports numbers) is therefore
a measurement, not a hope.

**The A/B statement is the row-6.5 shape, made before measuring**: there is **no JMH in
this repository** (`grep -rn jmh` over every pom, workflow and script is empty), so the
finest instrument for the Java host is a whole-system row floored at ~5 %, and **no row
here can resolve the ≤2 % a JVM class extraction plausibly costs**. J1 is declared a
**guard** for the *categorical* failure — a megamorphic site, a lost `ClassValue`, an
allocation moved onto the per-record path — and explicitly **not** a certificate of a
2 % absence. J1r is `direction` always. The arm is one `sha256sum` because
`java_runner.jar` shades every SDK class, and `libflz_core_ffi.so` being identical
across arms is what makes any delta Java-side.

**Cross-SDK answered with five checks, none deferred** — the `HostClassDecl` key
fixture, a `javap -protected` public-signature diff, the 17 JNI names untouched, a
≥12-char string-literal diff for defaults, and fixtures living in a different area.

### The ruling the survey asked for: fix the classifier, as batch 0

Three metrics read as **more defects than they are**, and I verified the decisive one
from source: **`StableId` is `public record StableId(long hi, long lo)`** — a record
header the classifier counts as a 50-line function. The other two are the same kind of
error (a regex matching domain vocabulary — "no longer owns this key after a rescale" —
and constructors reported under their public exception class name).

**Ruled: fix it, in batch 0, before any code moves.** Three reasons. A gate that counts
the wrong thing **makes the wrong work look required** — six of seven `long_names` are
names that are already right. The programme has done exactly this before, mid-area, and
recorded it as a *measurement correction* (area 3's `6818eec1`, where `brace_extent`
stripped `#` as a comment and hid a 348-line function). And it must be batch 0 or the
before/after numbers are read off two different rulers.

**The guard, because this LOOSENS ratchets** (repo `fn>50` 66→65, `long_names` 14→8):
the survey simulated the change across **every** area and only `sdk/java` moves. That
simulation is what separates a correction from a loosening, and it is re-verified after
the change with the baseline re-recorded and the reason stated.

### What the tree contradicted, and the board is corrected

- **8 441 lines → 8 457** (`baseline.json` already said 8 457).
- **"Java measures 27–55 % under Rust" is superseded** by KI-34's own 2026-08-30
  triage: *"OLD AND SOFT… not reproducible"*, re-measured at **11–16 % end-to-end**,
  with the receive gap inside the rust arm's own 12.1 % spread. The honest form is
  **direction holds, magnitude unestablished** — and the board carried the old figure
  into this session's own brief.
- **`targets.md` is a Phase-0 snapshot** and stale: 147 functions>50 repo-wide against
  today's 66, and two file sizes that `java-diamond-resume` has since grown.

**And the refactor helps the owed `java-host-dispatch-cost` work** rather than
foreclosing it: after the split the emit funnel, routing seam and state exchange are
separately constructible with explicit dependencies, which is what a JMH harness needs
and what a 993-line file with four private nested classes prevents.

**Two findings outside the area, filed as KI-74 and KI-75** and both verified by the
supervisor: `scripts/check-jacoco-coverage.py` has **no caller anywhere** — a coverage
threshold nothing enforces, which reads as an enforced floor to anyone grepping — and
`sdk/java/flezha-sdk-protobuf/` holds **only** a `target/` tree for a module the
reactor no longer lists (`grep -c` in `pom.xml` → 0), untracked and therefore invisible
to every gate.

*Last seen 08:57* — 21 m in. All four artefacts written (`proposal.md`, `design.md`,
`tasks.md`, `specs/`) with the **tree still clean** — a survey that changed no code,
which is the requirement. Machine idle at load 0.29, **no leaked `mvn` or `java`**,
the stated hygiene risk for a Java area. Earlier at 08:46 — 10 m in, tree clean, no
proposal yet, load 0.30.

**Observation for the invariant, not a problem yet**: `ListAgents` showed a **second
subagent** (`a86bbce…`, since *completed*) that this supervisor did not spawn — the
survey worker delegated. The one-worker-at-a-time rule is about who may edit the
shared tree, and a survey edits nothing, so nothing was violated here. But a worker
that spawns workers is worth knowing about: if an *implement* worker did it, two
agents could write the same files with neither aware of the other. Worth a line in a
future implement brief.

### Row 8 — the uncaptured-log gate is PROVED, and Phase 1 closes

Not a change; the supervisor ran it directly, which is what it is for.

**The gate catches what it exists to catch.** A scratch uncaptured `Logger.error` in
a test module: `mix test` **exit 0** — a *green* suite — while the CI grep
(`elixir.yml:159`–`:163`) reports **1** hit. That is the whole point: a suite can pass
and still leak, and the gate is what notices. Scratch removed, file restored
**byte-identical** by `sha256sum -c`.

**Then two full runs, different seeds — 793795 and 161433 — both `exit 0`, 1872
passed, ZERO gate hits**, tree clean after.

**This is the first time the merge gate at `elixir.yml:150`–`:163` can actually
report**, and it took five rows to get here: KI-48 and KI-49 (row 2), KI-54 (row 4),
KI-53 (row 7), plus the two sources rows 2 and 4 found that nobody had filed. The
board's earlier claim that three lines stood between the gate and green was one run's
sample; the true count was five sources across four changes.

**`code-quality-baseline`'s SHALL about the gate now holds in practice**, not just in
text.

**The row 7 shutdown-noise risk did not materialise in either run** — the fix stops
the log at shutdown where HEAD logged one line, and neither clean run carried it.
Honest caveat: KI-53's own line was 1-in-6, so two runs are two data points, not a
proof of absence. If it appears later it is expected, understood and recorded — not a
new defect.

### Phase 1, in total

Eight rows, ~20 commits, and the through-line is that **a defect list is a set of
hypotheses**. Of the entries these rows carried: KI-21 no longer reproduced at all;
KI-54's filed mechanism was refuted; KI-33's diagnosis was wrong in both halves;
KI-23's remedy was unenforceable as filed; KI-18's "most urgent" item was already
done; KI-13 turned out not to be a repo defect; and KI-3's own published harness did
not run. Meanwhile the work **found more than it was given** — KI-59 through KI-73,
including two P1s in code every SDK compiles in, and KI-73, which held row 6.5 open
until the transport genuinely stopped discarding frames.



### Row 7 — `close-input-log-syncer-blindness` is DONE — `81fc0db2`

Archived as `2026-08-31-close-input-log-syncer-blindness`, one ADDED requirement on
`durable-input-log`. `mix test` **1872 passed, exit 0, zero uncaptured-log gate
hits**; `--include system` 1953; `--check` exit 0.

**The survey found a worse half nobody had filed**, and it is what made the design
question answerable. The filed defect was silent fsyncs (ten sends to a dead pid with
the batcher's dirty set empty). The unfiled one: `sync_now/1` was a bare `receive`
with no `after`, so a `:fsync` **call** against a dead syncer **wedged the GenServer
permanently** — alive, answering nothing, every append for that class parked forever,
reachable through graceful shutdown's final flush.

**Both halves proved red on HEAD by the supervisor**, not accepted on report:
`left: 10, right: 0`, and the log pinned in `sync_now/1` at line 531 with
`status: :waiting`. Restored byte-identical. The two negative controls are
**separated** — reverting only the monitor reddens only the wedge test, reverting
only the `{:EXIT, …}` clause reddens only the count — which is what proves two
mechanisms rather than one wearing two hats.

**The false comment was the cause and is now the fix's centrepiece.** `sync_now/1`
said *"the caller's call timeout bounds the wait"*. It bounds the caller, never the
callee. A sentence that is confidently wrong about which process a timeout protects
is worth more attention than the missing clause it hid.

### The worker overrode a supervisor instruction, correctly

My brief named a new file, `input_log_syncer_test.exs`. It declined, because a new
`async: false` module moves the **blocking** `test async_false_share` row — and cited
charter §9, which folds an incident file into the behaviour file anyway.

**I verified the claim rather than taking it**: a scratch `async: false` test file
moves the metric **0.813 → 0.814** and `--check` goes **exit 1, BLOCKING**. The brief
was wrong and following it would have landed a change that fails a blocking gate.

That is the behaviour I want from a worker: override an instruction **with a
measurement and a charter citation**, and say so in the report. The failure mode is
the opposite — following a brief into a red gate and reporting it as the brief's
fault.

### Accepted at close, and it is row 8's to measure

At application shutdown the app master kills processes in unspecified order — KI-53's
original condition. Where HEAD logged one `[error] … unexpected message`, the fix now
**stops** the log, and `gen_server` plus the supervisor each report that at
`[error]`. Zero hits in the close-out run, but KI-53's own line was 1-in-6.

**Left in place deliberately.** Suppressing it means special-casing a shutdown-shaped
reason, which re-hides the failure the row exists to close, and it would be
optimising the gate rather than the system. **Row 8 measures it** — if the gate goes
red there, that is information row 8 must have and not something to have hidden in
advance.

### Row 6.5 — `close-transport-send-buffer-loss` is DONE

Three segments, archived as `2026-08-31-close-transport-send-buffer-loss`. Tag
`refactor-baseline-6.5`.

| segment | commit | what |
|---|---|---|
| A | `269072b9` | both defects reproduced on the ungated build; the shared gate |
| B | `443903f9` | the park; the QUIC client (KI-70/71); the dead `send_data` NIF |
| C | `3e22f480` | **KI-73** — the sender raises the gate; the knobs; the close-out |

**The guarantee is now true, and the supervisor proved it on the same file that
disproved it.** Running KI-73's harness unchanged, three times:

```
SENDER: %{ok: 400}
FINAL:  frames_written=400 write_buffer_dropped=0
```

against the `write_buffer_dropped=79` the identical file produced before segment C.
Every frame delivered, nothing discarded, and the crossing is exact — 107 frames of
64 KiB against a 6.71 MiB mark, call 108 refused.

**Holding the row open was the right call and it is the row's main lesson.** After
segment B everything was green — 1870 tests, clippy clean, both original defects
"fixed" — and the platform still dropped 20 % of a burst under an `:ok`. The thing
that caught it was a worker writing the test that would have proved its own work,
finding the assertion would be false, and **declining to omit it quietly**. Had that
assertion been skipped, this row would have archived a spec requirement
(*Congestion increments nothing*) that the code did not meet.

**Final state, supervisor-verified**: `mix test` **1870 passed, exit 0, zero
uncaptured-log gate hits**; `--include system` **1951**; cargo **147 + 20 + 45**;
clippy **0**; `refactor_metrics.py --check` **exit 0**; five naming gates green and
`check-kind-naming` red only on pre-existing prose, with row 6.5 contributing **zero**
after one capacity-sense phrase was classified in the allowlist.

**`git add -N` earned its place a third time.** The worker measured "row 6.5
contributes zero" before its own new dev note existed to the gate; intent-adding it
surfaced one more hit. Every checkpoint this session that skipped that step read a
gate's exit code for the wrong reason.

**A warning worth more than this row**: `refactor_metrics.py --write-baseline`
**without `--lint` writes `null` into every `credo_warnings` and `clippy_warnings`**,
and `--check` skips a null on either side — so every credo ratchet silently stops
blocking. The worker hit it, saw it in the diff, and re-recorded with `--lint`.
Verified at close: 36 areas, **0 credo nulls**, repo **378** — row 3.5's ratchet
survives.

**KI-72 stays open on purpose** and is named *inside* the requirement: the QUIC
driver emits no `DriverStats`, so this row's reporting half is UDS-only and a reader
cannot conclude otherwise. It joins KI-68 and KI-69 as the runner/controller-seam
group to be surveyed together.

### Row 6.5 segment B — verified 2026-08-31, commit `443903f9`

Batches 3-4 plus KI-70/71 and the `send_data` deletion. `mix test` **1870 passed,
exit 0, zero uncaptured-log gate hits**; `--include system` **1951**; cargo
**20 + 39 + 147**; clippy 0 warnings.

**KI-70 and KI-71 are driven, not argued.** The `quinn_proto` pair harness moved
**down** into `flz_transport` so one harness covers both send buffers, and each fix
has an isolated red-first control: restoring the assign loses the first writer's
4-byte remainder mid-frame (`left: 65536, right: 65540`); restoring the ceiling arm
answers `Ok(n)` for a discarded frame. The control — a reading peer refuses nothing —
stays green on both reverted arms. The same function held an **unfiled third
defect**: a non-empty buffer did not stop a later frame going on the wire ahead of an
earlier frame's remainder.

**Two callers would have broken silently on the new error term** — `Ingest.verdict/2`
matched `{:error, :no_stream}` exhaustively (a `CaseClauseError` waiting), and
`Replay.reforward/1` compared with `==`, so a gate-timed-out re-forward would **never
have armed the bind re-forward**. Widening the error term is the kind of change whose
damage is in the callers, and this one went looking.

### KI-73 — the row's own goal is not met, and this is why it stays open

**Reproduced independently by the supervisor**, from the entry's own harness,
unchanged, matching to the frame on the first run:

```
HELD:   send_gate_held=1 send_frame_calls=211
DRIVER: frames_written=131 write_buffer_dropped=79
SENDER: %{ok: 400}
```

**79 of 400 frames discarded, all 400 answered `:ok`** — KI-3's exact symptom, still
reproducing with the gate and the park in place. The bit is written by
`SendGate::observe` on the **driver thread** behind an unbounded channel, so the
sender's view lags by whatever is queued; the park removes the *repeat* refusals and
not the *overshoot*.

**Ruled: segment C fixes it before the row closes.** Two reasons, and the second is
the harder one. Committing this as done would claim to resolve KI-3 while KI-3's own
reproduction returns 79 — and the spec delta's *Congestion increments nothing*
scenario is **not satisfiable in this state**, so archiving would publish a
requirement the code does not meet. A requirement that is false on the day it lands
is exactly what this session spent the day removing from `known_issues.md`.

**It refutes D13**, a *measured* claim from segment A that the residual 83 frames
were "what batch 3 removes". D13 is corrected, not dropped.

**The worker found this with the test written to prove its own work correct**, then
declined to assert `write_buffer_dropped == 0` because the assertion would have been
false, and said so. A test that quietly omits the assertion its subject cannot pass
is how KI-67 happened; naming the omission is the opposite, and it is why this row
will close honestly.

### Two metric rows the worker could not see, and what they are

`git add -N` on its four new files surfaced **two blocking rows its own `--check`
missed** — the trap that bit this supervisor at row 4 segment A, now caught by doing
the intent-add first:

- **`test refutes` 13 → 16.** Three `refute_receive {:sender_done, _}, 500` in the
  park test. **Earned**: proving a sender is *held* is inherently a negative
  assertion — there is no positive signal for "still parked". Re-baseline at row
  close with that reason recorded, do not remove them.
- **`test narrative_markers` 47 → 48.** A **false positive**: the detector matches
  "no longer" in `# … a hold on a binding that no longer exists`, which describes
  current behaviour, not history. One word fixes it; segment C does, because a
  supervisor does not edit `test/` files.

### Row 6.5 segment A — verified 2026-08-31, commit `269072b9`

Batches 1-2: both defects reproduced on the **ungated** build, then one shared gate.
Baseline tag **`refactor-baseline-6.5` created at `74d9132b`**.

Supervisor-verified: `cargo test -p flz_nif -p flz_transport` **20 + 36 green**; the
gate calls the existing `would_overflow_write_buffer`, so `config.rs`'s "one
predicate behind every driver's send-buffer bound" **becomes true** rather than
staying a false comment.

**Batch 1 did its job, and its first result was that a filed harness does not run.**
KI-3's published reproduction raised `:erlang.hd([])` 3 of 3 — the `StreamHandle` is
collectable before `driver_stats/1`, its `Drop` sends `CloseStream`, and the snapshot
comes back empty. Pinned and re-run: **`%{ok: 400}` replies, `frames_written=131`,
`write_buffer_dropped=269`** — 269 frames discarded after the caller was told `:ok`.
Control at a 1 GiB cap: 400 written, 0 dropped.

**KI-56's filed reproduction was unperformable, and the worker said so instead of
faking it.** `TransportServer.stats` is `None` for QUIC, so there is no
`driver_write_buffer_bytes` to watch grow. Reproduced by another route — an in-memory
`quinn_proto` pair whose client never reads: **`offered=52428800 buffered=44040192`
against a cap of 8388608**, and the second 25 MiB burst buffered **byte for byte**
(`growth=26214400`). That is stronger than "over the cap": there is no bound at all.

**The finding that changes the row's shape — KI-70, confirmed by the supervisor from
source.** `flz_transport/src/transport/quic.rs:543`'s `write_or_buffer` drops a frame
at the ceiling and then `reply.send(Ok(total_len))`: it reports **every byte written**
for a frame it discarded. That is KI-3's defect on the client side, in code **every
SDK compiles in**. **Ruled into this row (segment B)** — closing both drivers while
the client path still lies would ship a half-fix.

**Two more filed**: KI-71 (same function — `write_buf` and `pending_write_reply` both
**assign**, so a concurrent write tears a frame and drops a reply sender), ruled
**fixed in the same pass but only on a driven reproduction**, since read-established
is the KI-67 shape; and KI-72 (the QUIC driver emits no `DriverStats` at all), ruled
**deferral named, row does not grow** — which makes the row's reporting half
explicitly UDS-only and must be written where the requirement is stated.

**The A/B rows are declared, and the honest answer is that none can resolve the
change.** One relaxed atomic load is ~1 ns against an ~8.7 µs per-frame budget —
**~0.01 %**, two orders under row 1's 1.93 % floor and three under row 6's ~18 %
spread. Row 6 and the new QUIC arm are **direction** rows; the cost is argued from
*where the load sits*, not measured. That argument is a **constraint, not a
preference**: a gate read costing its own NIF call would be 1–2 %, which row 6 also
cannot resolve.

**Cross-SDK answered with evidence: no Java and no Elixir work owed** — provided the
knob is read *inside* `flz_core`. The credit window has one caller
(`flusher.rs:260`), `PARK_TIMEOUT` is a private `const` with no accessor, Java reaches
it as compiled `flz_core` over the C ABI with no credit surface of its own
(`grep -ril credit sdk/java` is empty), and the Elixir SDK answers
`:credit_window_init` in its "persistence is deferred" ignore clause. **This is a
constraint on task 5.3**: read it with `util::env_parse` as `dynamic.rs` already does;
thread it through a builder or API parameter instead and Java immediately owes a
`HostClassDecl` key and a JNI setter.

**`refactor_metrics.py --check` is exit 1 with 3 regressions and they are this
change's** — `flz_transport prod_loc` +85, `flz_nif prod_loc` +60,
`flz_nif narrative_markers` 0 → 1. **Ruled**: a Phase 1 defect row that adds a
required mechanism is not the case charter §1's net-lines rule governs — that rule is
measured over a *refactor area*. The two `prod_loc` rows are **expected red for the
duration of row 6.5 and re-baselined at its close**, with the number justified there.
The `narrative_markers` hit is **not** accepted: it is a quality defect in a new file
and segment B fixes it.

**Deletion funding ruled in**: the `send_data` NIF is **dead** — its only Elixir
reference is a stub declaration at `native.ex:53`, `Transport.Stream` omits it
deliberately, and `ServerCommand::SendData` is produced only by it. ~30 lines across
four files. **Delete it in segment B.** It is a public-surface removal, which is why
the worker left it to me, and the deciding reason is not the line count: a dead
second send path that must track the gate forever is a trap for the next reader.

### Row 6.5 — approved 2026-08-31, with two required additions

The user queued this on 2026-08-30 and another session wrote the proposal. It is a
**P1 production transport change** and the strongest proposal in the queue: both
defects carry file:line evidence, the mechanism is **one** watermark gate shared by
both drivers, and the reuse decision is explicit — it extends the existing
`push_gated` / `rearm_deferred_pushes` holdback rather than inventing a second
backpressure mechanism. `--strict` valid. 6 batches, 30 boxes, ~8.5 h.

**What it fixes**: UDS silently discards a frame after `send_frame` has already
answered `:ok`, and **nothing in `lib/` reads the drop counter**; QUIC calls the
overflow predicate **nowhere** and grows without bound on the same non-draining
peer — in a repository that has already taken a production OOM from unbounded
buffers. Both doc comments claiming a shared bound are false as written.

**Design risk 1 is the best sentence in the proposal** and it names this session's
own failure: *"A gate that never engages looks exactly like a gate that works.
Every test must first drive the ungated build and see the drop or the growth, or it
proves nothing."* That is KI-45's trap, KI-33's trap, and — as of this session —
KI-67's. Batch 1 exists to do it in the right order.

**Approved with two additions, briefed rather than sent back** (the proposal is
otherwise excellent; a rewrite cycle would cost more than it buys):

1. **The A/B is under-specified against charter §8.** Task 6.5 has the right shape —
   interleaved, mains power, and 6.4 adds an explicit QUIC arm because the default
   sweep covers QUIC nowhere. What it lacks is §8's contract: a baseline **tag**, and
   each row declared **`gate` / `guard` / `direction` with its measured floor, in
   `design.md`, before anything is measured**. A row whose floor cannot resolve the
   effect being claimed is a direction row and must say so. This is a data-path
   change — one relaxed atomic load per frame on a path measured at 115K msg/s.
2. **The cross-SDK rule is unanswered.** `CLAUDE.md` requires that a surface every
   SDK carries — explicitly including **a default** — changes in Rust, Java **and**
   Elixir in one change, *or* the change names the SDK it defers and the follow-up
   that owes it. This makes `flz_core/src/credit.rs`'s hard-coded `PARK_TIMEOUT`
   configurable, and `flz_core` is compiled into the **Java** SDK over JNI. The
   proposal mentions Java nowhere. It may well resolve as "the knob is read inside
   the shared core, so Java gets it for free" — but that must be **established and
   written down**, not assumed.

### Segmentation of row 6.5

| segment | batches | content | state |
|---|---|---|---|
| A | 1, 2 | make both defects reproducible; the shared gate | **done**, `269072b9` |
| B | 3, 4 (+KI-70/71, send_data) | the sender side; the QUIC client fix; the dead NIF | **done**, `443903f9` |
| C | 5, 6 (+KI-73) | KI-73 fixed; configuration; the close-out | **done**, `3e22f480` |



### Row 5+6 — `correct-runtime-and-conformance-docs` is DONE — `e4f919c8`

Archived as `2026-08-31-correct-runtime-and-conformance-docs`, one MODIFIED
requirement on `sdk-conformance`. Supervisor-verified: `mix test` **1864 passed,
exit 0, zero uncaptured-log gate hits**; `--check` exit 0; five naming gates green.
Production `lib/` **net −1**.

**The entry named one stale list; there were three.** `bench/roundtrip.ex` advertised
three runtimes of seven, and **`bench.ex` — which KI-51 does not mention** — named
five and omitted `python`. A third copy restated a manifest's `deferred` key for one
runtime when the restriction holds for three. **The fix is one list, not a corrected
second one**: both help texts name `test/apps/<runtime>_runner/conformance.json` and
enumerate nothing, which is the rule `sdk-conformance` already states for the launch
line. Two lists that must agree *are* the defect; correcting the copy would have
rebuilt it.

**The drift is now gated, and the supervisor proved it red rather than accepting the
claim** — the KI-67 lesson applied. Reverting `bench.ex` alone: the gate reports
`Mix.Tasks.Bench names ["elixir", "java", "node", "ruby"] … a second copy of the set
its --runtime reads from the manifests`, **18/19**; restored byte-identical, **19
passed**.

**KI-13 needed no code, exactly as its triage said** — and the useful finding is
*where* the knowledge lived. The shim's requirements and the Ruby version floor
existed only in a refactor dev note, not where a person running the suite would look;
`docs/sdk/conformance.md` now carries both. Where the exact `docker run` mount and
`-e` spellings are **written down nowhere**, the worker stated the requirement with a
placeholder rather than inventing flags — the right call, since a wrong command in
the docs is worse than a pointer.

**A triage fact was false and is corrected, not carried**: KI-13 records "there is no
PHP on this box at all". There is — **PHP 8.3.6 with `ext-ffi`**, supervisor-verified,
which satisfies the default `sync` executor. The shim remains the only route to the
`swoole` executor. An entry that has gone stale in the *reassuring* direction is the
kind that quietly licenses a wrong conclusion later.

**KI-65 recursion, twice in one session.** `check-kind-naming` reached 10, then 11 —
and on both occasions the new hit was a **supervisor heartbeat line on this board**,
the second one being a line whose subject was the count drifting. Both reworded. The
board no longer quotes a fixed number for that gate, because the number is a function
of how much the board discusses it. That is KI-65's argument for `record_tag` made
visible in the metric itself.

**KI-18 gains a third sighting** of `flezha_test.exs:403` (`assert m.migrated >= 1`,
`left: 0`) — new information, because the entry recorded the exposure as
self-inflicted triple load and this was one ordinary run on an idle box. It did
**not** recur in the supervisor's own run, and the change cannot reach that file.

### Row 4 — `green-the-test-hygiene-tail` is DONE

Three segments, three commits plus close-outs, archived as
`2026-08-31-green-the-test-hygiene-tail` with 2 ADDED and 1 MODIFIED requirement on
`code-quality-baseline`.

| segment | commit | what |
|---|---|---|
| A | `656fd444` | KI-54 ownership; the `worker_registry` barrier; the KI-66 claim |
| B | `9820f75d` | the watchdog pin; KI-23's generator bound; four sibling mints unified |
| C | `5946464e` | the KI-67 repair; the entries made true; verification |

**Final state, supervisor-verified**: `mix test` **1863 passed, exit 0, ZERO
uncaptured-log gate hits**; `--include system` 1944 passed; `--include multi_node`
1872 passed; `refactor_metrics.py --check` **exit 0**; five of six naming gates
green (the sixth is KI-65, untouched by this row).

**Four of the row's own entries did not survive verification as filed**, which is
the row's real product: KI-21 no longer reproduced at all, KI-54's landing site was
refuted, KI-23's remedy was unenforceable because three of five sites are not
literals, and KI-18's "most urgent" item was already done. A defect list is a set of
hypotheses, and this row tested them.

### The lesson that outlives row 4 — KI-67

Segment A's KI-66 regression test **passed on the code it existed to catch**, and it
took a *later worker running its negative control* to find that out. Measured three
times: the segment-B worker 15 red / 20 seeds, the supervisor **7 red / 5 green** at
12 seeds, and the repaired test **12 red / 0 green** at the same twelve.

The mechanism is worth carrying: `System.unique_integer` is node-global **and
per-scheduler striped**, so a fixed band has no relation to where the next mint
lands — drift measured from `+6365` to **`−260`**. And the old test's vacuity guard
was **true in both the caught and the missed case**, which is what made it look
rigorous while separating nothing.

**Two supervisor process failures caused it, both recorded rather than quietly
repaired:**

1. At segment A's checkpoint I ran the full suite, read the fix, and **accepted the
   worker's negative-control claim without running it**. A regression test is proved
   by its negative control and by nothing else. Row 3.5 proved its gate red twice —
   worker and supervisor — and that is the standard segment A's checkpoint missed.
   At segment C's checkpoint I ran all twelve seeds myself before believing the
   repair.
2. I skipped the skill's §4 `git add -N`, so `--check` read exit 0 only because the
   new file was **untracked and invisible to the gates**. The segment-C worker
   flagged the same trap back at me in its own report.

**The repair's construction is the durable part**: a **real second BEAM**, because
the defect *is* a previous BEAM's leftovers — reproducing it beats simulating it,
with no retry loop and no probabilistic argument. Its guard was **demonstrated** to
discriminate (on a deliberately mis-sized arm it failed with *"the claim asked for
325 first, outside the planted band"* — precisely the case the old test passed).

### KI-65 demonstrated on the supervisor

`check-kind-naming` reached **10** hits during this row, and the tenth was **my own
heartbeat prose** on this board describing a worker's "bounded" wait. I reworded it;
back to 9. The gate genuinely does get redder every time anyone writes about it,
which is what KI-65 says and why its fix is `record_tag`, not more allowlist entries.

### Filed this row, not fixed — both `lib/`, both queued

- **KI-68** — an opted-out workflow's teardown burns the whole 60 s drain bound
  after a crash-resume (`deregistered=true after 59994 ms`). Deterministic in
  isolation, pre-existing since `a9787a0f`.
- **KI-69** — a runner connect racing a hub teardown exits `:shutdown` out through
  `connect_handler/4` and takes `Transport.Server.Uds` — the acceptor for **every**
  runner — down. **One sighting, not reproduced** in the timebox (3 full re-runs +
  5 module runs), but the mechanism is read off a real stack. Recorded as a
  suspicion per charter §1b.

Both are teardown-ordering defects on the runner/controller seam and **should be
surveyed together**, not separately.

### Row 4 segment B — verified 2026-08-31, commit `9820f75d`

Batches 4-5 plus the four sibling mints. Supervisor-verified: `mix test` **1863
passed, exit 0, zero CI gate hits**; `refactor_metrics.py --check` **exit 0**;
format and `--warnings-as-errors` clean; no `lib/` change.

**The watchdog pin** carries a deliberate foreign breach that stays in the test —
it is what makes the pin load-bearing forever, and removing only the pin is the
negative control, which was run and came back red. The pin is used **twice**: the
closing assertion and the `wait_until` eight lines above, which acquires the same
exposure the moment a foreign entry exists.

**KI-23's inertness was proved rather than asserted** — all **40** literal
`unique("…")` prefixes × widths 1-7 = **280 pairs, 280 byte-identical**. Exactly 5
ids change, all from the two over-conservative hand truncations, all getting
*longer* and all still inside 32 by construction. Its red-first is a **negative
control, not a natural red**, and the report says so: the natural red needs ~167×
a full suite. Four private `unique_id_path/0` copies were deleted, not the one the
design named.

**The shared claim is `NodeTestHelper.claim_tmp_dir/1`, five call sites, net 0
lines** — `+20` in the helper, `−20` across the four callers. Fully
deletion-funded.

### KI-67 — the finding that matters, and it is against MY verification

Segment B ran the negative control on **segment A's KI-66 regression test** before
reusing its mechanism. **The test passes on the defective helper.** I reproduced it
independently: claim reverted to `File.mkdir_p!/1`, the module run at 12 seeds —
**7 red, 5 green**. The worker measured 15/20 red; I measure 7/12. Either way the
test green-lights the code it exists to catch, roughly two times in five.

**Mechanism**: `System.unique_integer([:positive])` is node-global **and
per-scheduler striped**, so a band planted at `counter+1 … counter+512` bears no
fixed relation to where the next mint lands — drift measured `+6365`, `+829`,
`+509`, `+29` and **`−260`**, and a negative drift puts the mint below the band
entirely. The vacuity guard is true in both the caught and the missed case, so it
separates nothing. One-sided: only false greens.

**This invalidates a claim in `656fd444`'s commit message**, which says the KI-66
fix was proved red-first. The fix itself stands — `File.mkdir/1` is atomic by
construction, not by sampling — but the proof does not, and a test that passes on
broken code two times in five is worse than no test because it will be trusted.

**The cause is mine, and it is recorded as such.** At segment A's checkpoint I ran
the full suite and read the fix, then accepted the worker's negative-control claim
**without running it**. A regression test is proved by its negative control and by
nothing else. Row 3.5 segment B proved its gate red twice — worker and supervisor —
and that is the standard this checkpoint failed to meet. **Ruled: fix the test in
segment C**, acceptance **12 of 12 red** against the reverted helper, never a
majority.

**A second process gap, same checkpoint.** `refactor_metrics.py --check` read exit
0 at segment A for the wrong reason: the new test file was **untracked**, and the
gates walk `git ls-files`, so it was invisible. The skill's §4 opens with `git add
-N` on every new file *precisely* for this, and I skipped it. With the file
intent-added, `--check` is exit 0 honestly — verified this checkpoint.

### KI-68 — real, pre-existing, and queued as its own row

`workflow_crash_resume_e2e_test.exs:296` fails deterministically (1/1 in module,
3/3 alone) because `ControlChannel.drain_owed_outputs/3`'s `@drain_owed_ms 60_000`
(`control_channel.ex:492`) waits the full minute for direct outputs a resumed run
never delivers — raising only the test budget to 90 s gives `deregistered=true
after 59994 ms`. **Pre-existing**: `git log -S` puts that constant in `a9787a0f`,
the workflow perf campaign.

**Ruled: filed, not fixed in row 4, queued as a new Phase 1 row.** Charter §1b
pauses an area for a defect in the code that area is working on; row 4 changes
nothing in `lib/` by design, and a 60-second production drain cap is a behaviour
decision. Folding it in would be the scope drift this programme removes.

### Row 4 segment A — verified 2026-08-31, commit `656fd444`

Batches 1-3 plus the KI-66 fix. **Independently verified by the supervisor**:
`mix test` **1858 passed, exit 0, ZERO CI gate hits**; `refactor_metrics.py
--check` exit 0; `mix format --check-formatted` clean; **no `lib/` change**; no
scratch instrument residue in any of the three test files (`grep` for
`SCRATCH|await_queue|blocker_entered|IO.puts` returns 0 in all three).

**KI-54 — the acceptance is 34 → 0 surviving workers, and it never counts runs.**
The defect shows through the log 1-in-6; it shows through the registry every time.
Converting a sampled symptom into a deterministic one is the whole reason this fix
can be believed. Detector red at **1 of 28** before, module green at 28 after.

**Two deviations from design D1, both forced by measurement, both improvements:**

1. **The design's snapshot sweep is racy** and left 8 of 28 passing.
   `WorkerSup.ensure_worker/4` → `reap_and_start/4` drops a key, waits, then
   re-registers a replacement, so a single `Registry.select` can miss a live
   worker. The sweep now repeats until the registry stays empty, reusing the
   module's own `wait_until/1` — the same bounded shape production already uses in
   `await_unregistered/3`.
2. **`on_exit` is armed BEFORE the assertion, not after.** In the design's order
   the first leak raises in `setup` *before* `on_exit` is registered, disarming the
   sweep for every test behind it — which is why one key appeared in **20
   consecutive failures**. Reordering costs nothing.

The dead `unless Process.whereis(ForwardSup), do: start_supervised!(ForwardSup)` is
deleted: it is proved unreachable, and leaving it beside a detector reading the
same registry would claim the module might own the supervisor.

**Batch 3 landed on face A**, exactly as `design.md` D2 predicted, not the
`entry.callers` face — the forced window was built well enough to say which. The
barrier is a positive settle on `{:kafka_worker_dead, …}`, a signal the registry
already publishes, so it proves the DOWN was **processed**; the `eventually/2` poll
would only prove a side effect became visible, and needs a timeout to tune. The
instrument is deleted and the module is 5-for-5 across five seeds.

**KI-66 fixed by making the mint a CLAIM** — `File.mkdir/1` instead of `mkdir_p!/1`,
retrying on `:eexist`. No new naming scheme, no new helper, no call site changed,
and it does not depend on `/tmp` being clean, which was the binding constraint. Its
regression test **forces** the collision by claiming the whole band the next mint
can land in, and carries a vacuity guard that the returned name is past every
claimed one — so it cannot pass by the band having missed.

**The refusal to share a mechanism with KI-23 is correct and is the segment's best
piece of reasoning.** An operative id **cannot be claimed at mint time** — nothing
exists on disk when the generator returns, so there is no `mkdir` to fail. And the
one scheme that would serve both, a monotonic `System.os_time(:microsecond)`
suffix, **spends KI-23's entire budget**: 16 digits plus a separator leaves 15
bytes of 32. Two problems that look identical, one primitive that fits one of them.

**LOC +90, and it is NOT deletion-funded — stated plainly rather than dressed up.**
The three fixes are additive by nature (a sweep, a barrier, a claim); the only
deletions available were the dead `unless`, a `_ = refs`, and the `mkdir_p!`.

### Ruled at this checkpoint — the four sibling mint sites go to segment B

The worker found four helpers carrying KI-66's hazard exactly and **stopped rather
than growing the segment**, which is what the brief asked for. Verified: 
`schedule_test_helper.ex:20`, `durability_backend_stub.ex:61`,
`tier_e2e_harness.ex:189` (`unique_id_path/0`) and `auth_test_helper.ex:40` all mint
`System.tmp_dir!()` names from `unique_integer`. `node_test_helper.ex:147` is
correctly **excluded** — its `flezha_peer_<purpose>_<node>` name carries no
`unique_integer` and is meant to be stable.

**Ruled: fix all four in segment B**, with one shared claim. Three reasons: charter
§2 earns a shared mechanism at the third caller and this is the fifth;
`unique_id_path/0` is **already opened by batch 5** for KI-23, so fixing it there
costs nothing and stops two workers editing one function in sequence; and leaving
them is a known leak left standing in a row whose subject is tests that leak.

### Segmentation of row 4

| segment | batches | content | state |
|---|---|---|---|
| A | 1, 2, 3 (+KI-66) | baseline; KI-54 ownership; the `worker_registry` barrier; the KI-66 claim | **done**, `656fd444` |
| B | 4, 5 (+4 mints) | the watchdog pin; KI-23 generator bound; the four sibling mint sites | **done**, `9820f75d` |
| C | 6, 7 (+KI-67) | fix the KI-66 regression test; make the entries true; verification | **done**, `5946464e` |

### Row 3.5 — `fix-opaque-module-names` is DONE

Four segments, four commits plus four close-outs, archived as
`2026-08-31-fix-opaque-module-names` with 5 requirements added to
`code-quality-baseline`.

| segment | commit | what |
|---|---|---|
| A | `0cb58166` | 3 renames — `State.Registry`→`Paths`, `AuthZ.Store`→`DetsStore`, `SDK.Backoff`→`Jitter` |
| B | `055054f2` | 4 folds + `scripts/check-module-naming.sh`, proved red first |
| C | `1848cb80` | the alias pass — 689 sites, 103 modules, 189 files |
| D | `14c7cfc5` | `.credo.exs`, the KI-63 fix, the measured ratchet |

**The headline, measured twice — once by the worker, once by the supervisor
independently:**

```
credo total                 1463 -> 378
AliasUsage                   689 -> 0     (adopted; the naming problem fixed)
ParenthesesOnZeroArityDefs   431 -> 0     (rejected; disabled)
AliasOrder                    66 -> 66    (unchanged across ~650 insertions)
```

**`credo_warnings` is now an integer in all 36 areas and at the repo level**,
replacing `null` everywhere — the metric ratchets for the first time.
`refactor_metrics.py --check` is exit 0, **0 regressions**, for the first time in
60+ commits.

**378 is a measured number.** The task list's 379 carried the same off-by-one this
change corrected in four separate board figures — and that is the change's
signature: *every* number it inherited was checked, and the survey's own prose was
wrong twice (47 colliding modules for 49; `excluded_lastnames` 47 for 50) while its
classifications held exactly.

**What the four segments actually cost and bought**: net **+459** lines, 4 files
deleted, 1 added. No charter rule is cited for the increase and none exists —
charter §1 admits only §2's reuse order or a §7 ceiling, and this is neither. The
lines exist because the user ruled on KI-10, and saying so is the honest accounting.

**Three lessons worth more than the diff:**

1. **A bare name in value position is a defect only when its BINDING changes.**
   Segment A removed a binding and silently repointed `:erpc.call(source_node,
   Registry, …)` at the stdlib — compiler-silent. Segment C added 374 bindings and
   repointed nothing. Two sweeps by two different methods agreed, and the method
   that looked alarming (7 global hits) was the one asking the wrong question.
2. **A red system run is not evidence of a regression until its first crash line is
   read.** Segment B's 471 failures came from one `"driver init failed"` 408 lines
   above the first reported failure. See the KI-57 escalation below.
3. **A gate must be proved red before it is trusted.** Both new gates were, and the
   `record_tag` exemption list was wrong as designed in both directions — too
   narrow (dev_notes only) and measured against a tree that moved while it was
   being measured.

**Filed this row and not fixed**: KI-61 (deferred to `refactor-5a-state`), KI-62
(not a bug), KI-64 and KI-65 (both new, below in `known_issues.md`). KI-63 fixed
and resolved.

### Segment C — verified 2026-08-31, commit `1848cb80`

Batches 7-8: the alias pass, **689 sites over 103 modules in 189 files**. 9 boxes
ticked, each verified.

**The central claim was re-measured by the supervisor, not read off the report.**
An independent `mix credo --strict` run: **`AliasUsage` 0** (from 689),
**`AliasOrder` 66 — unchanged from HEAD**, `ParenthesesOnZeroArityDefs` 431.
`mix compile --warnings-as-errors`, `mix format --check-formatted` and
`check-module-naming.sh` all clean; a supervisor slice over the three most-edited
areas **632 passed**; the worker's full suite 1857 passed.

**The value-position trap was cleared by two independent methods and they agreed.**
Segment A's `wal_transfer.ex:191` lesson made this the segment's one real risk. The
worker swept per-file over the 374 bindings it introduced: 0 hits. The supervisor
swept differently — collect every introduced last segment, then grep the WHOLE tree
for it in `:erpc.call` / `apply` / `{:via, ...}` / child-spec position — which
surfaced 7 real hits the per-file method excludes by construction. **All 7 were
already present at HEAD and segment C added an alias for none of them**
(`added_by_segC=0, present_at_HEAD=1`, seven for seven). Nothing was repointed.

The general rule, worth keeping: **a bare name in value position is a defect only
when its binding CHANGES.** Segment A removed a binding and broke one; segment C
only adds, and an added alias can only repoint a name that resolved elsewhere
before — so the check is "did this file's binding for this name move", never "does
this file use a bare name".

**D4's list is larger than the pass needs, and two spellings were missing.** Only
**7 of D4's 24** are reachable by an `AliasUsage` finding at all — the other 17 are
reached solely through `@behaviour`, `use`, a struct literal or an existing alias,
and credo ignores `@`-rooted and non-call references. Two spellings D4 does not
name were required: **`ProcessHub.Service.Recovery` -> `HubRecovery`** (collides
with `Flezha.State.Recovery`) and **`KafkaEx.Auth.Config` -> `KafkaAuthConfig`**
(collides with `Flezha.AuthZ.Config`) — the two non-`lib` targets segment A
flagged, caught exactly where they were expected.

**Segments A and B paid for themselves here.** A fresh inventory shows `lib/` at
**263** modules with **15** colliding last segments over 40 — `Metrics`, `Backoff`,
`Registry`, `Parser` and `Telemetry` are all gone from that list, so `tasks.md`
7.2/8.2's demand for an `as:` on `Flezha.State.Metrics` is now wrong and was
corrected: it takes a bare alias.

**Diff shape proved mechanically rather than by eye** — for each of the 189 files a
shadow tree of "HEAD + only alias insertions and prefix removals" was compared as
metadata-stripped ASTs against the real file: **0 of 54 differ in lib, 0 of 135 in
test.** The diff adds 0 comment lines, 0 `Process.sleep`, and nets 0 `refute`.

**Net +419 lines** (lib +164, test +255) = 374 alias lines + 53 blank separators, less
8 from formatter joins. **No charter rule is cited and none exists** — §1 admits only
§2's reuse order or a §7 ceiling, and this is neither. These lines exist because the
owner ruled on KI-10.

**KI-63 filed, reproduced by the supervisor from credo's source, and ruled.**
`.credo.exs:9`'s `~r"^test/apps/"` is inert and always has been: `include/2` builds
an ABSOLUTE-path file set, `convert_globs_to_local_paths/2`
(`deps/credo/lib/credo/sources.ex:132`) expands only `is_binary` patterns and passes
a `%Regex{}` through untouched, and `exclude/2` (`:183`) is a bare `String.match?`.
A `^`-anchored relative regex cannot match an absolute path; the three siblings work
only because they are **unanchored**. **Fixed in segment D, with the ratchet** — the
two must land together or the baseline is recorded against a file set that shifts
underneath it, and a full run right now reports **zero** findings under
`test/apps/`, so honouring the exclusion costs 0 today and would be a silent ratchet
shift if deferred. Fix the mechanism, not the policy: whether `test/apps/` belongs
outside credo's view is `refactor-7-tests`'s question.

### KI-57 escalated at segment B — a filed P2 took down a quarter of the tier

Segment B's **first** `--include system` run reported **471 failures** (1466/1937).
It is **not the change**: the full suite was 1857 passed / 0 failed on the same
tree, and the re-run at the same seed came back 1938 passed, exit 0.

**The origin is one line, 408 lines above the first reported failure:**

```
[flz_nif] UDS driver fatal error: path must be shorter than libc::sockaddr_un.sun_path
[StreamAcceptor] Failed to start secondary listener: "driver init failed"
Child :secondary_acceptor of Supervisor {Flezha.Service.Identificator,
  "schema_evolution_e2e_17698_196642579963_wus"} failed to start
```

Every one of the 471 is downstream: the acceptor dies, `runner_connect` refuses,
and thereafter each test's `on_exit` hits `:noproc` on
`Flezha.Observability.Registry` or `Flezha.Runner.FleetManager` and reports
*"possibly because its application isn't started"* — which reads like an
application that never booted and is in fact one transport failure with 471
casualties. `SUN_LEN` is 108 bytes; the longest socket path on this box measures
**89** and the failing runner's renders at **91**.

The visible cascade also crossed `Flezha.State.Watchdog`, whose `:sweep_tick` made
an unguarded `GenServer.call(Flezha.AuthZ.QuotaCounters, ...)` (`watchdog.ex:233`
-> `authz/quotas.ex:120`) 60 ms after an authz test tore that tree down; because
`Watchdog` is a child of `Flezha.Supervisor`, that crash is what turned a transport
failure into "the application isn't started" for every later `on_exit`.

**Supervisor ruling — KI-57 is under-filed.** It is recorded as one test dying. It
can take out **471 of 1937** and present as a dead application, a diagnosis nobody
would reach from the summary line. Its entry owes a corrected severity and blast
radius, and the `Watchdog` link above is the mechanism to record with it.

**The rule this buys, and it belongs in every verification brief**: a green system
run is evidence; **a red one is not evidence of a regression until its first crash
line is read.** The summary tells you nothing about which of the two it was.

### Board-hygiene incident — this supervisor deleted two of its own records

Writing segment B's checkpoint, I replaced the span running from segment B's worker
note down to the segmentation heading. Segment A's record and the KI-57 escalation
block sat **inside that span**. Segment A's survived in `0ce625c5` and was restored
from it; the KI-57 block had never been committed and was reconstructed from this
session's own context, which a later session would not have had.

**The rule**: a checkpoint rewrite replaces the block it is replacing, never a span
delimited by whatever heading happens to come next — headings accumulate between
edits. Anchor on both ends of the intended block, and after any board rewrite,
grep the committed copy for a heading you did not mean to touch. Every earlier
segment's record is content the next session depends on, and no diff will flag it
as missing.

### Segment B — verified 2026-08-31, commit `055054f2`

Batches 4-6: the four folds, full verification with a live restore drive, and the
naming gate. 25 boxes ticked, each verified against the repo.

**The folds landed exactly as designed.** Four files deleted; parents at
**131 / 149 / 484 / 387** against the predicted ~130 / ~156 / ~490 / ~390, none
near the 800 ceiling. The moved regions are byte-identical — checked by diffing
each against `git show HEAD:<old file>`, not by reading the report — and both
`@prefix` literals plus all 11 `:telemetry.execute` tuples are unchanged, so no
event name moved. **`lib/` net −37 lines**, deletion-funded as required.

**The bare-token sweep that segment A's `wal_transfer.ex:191` bought found nothing
this time**, which is the right outcome and still worth running: `Parser` zero
hits, `Telemetry` five and all prose, `Metrics` six and all aliases of the
surviving `Flezha.State.Metrics`.

**The gate is proved, twice.** The worker's red-first proof, and then the
supervisor's own: a retired name appended to a tracked file turns it red and the
output **names the file and line**; removing it turns it green; the file restores
byte-identical by `sha256sum -c`. Its seven patterns are `re.escape(path) + r"\b"`
— the boundary is what keeps `Flezha.AuthZ.StoresTest` out **by construction**,
which segment A hit as a false positive. Wired into `naming.yml`.

**The `record_tag` correction was load-bearing and moved again while it was being
made.** `tasks.md` 6.4 scoped historical citations to `docs/dev_notes/`; the true
set is six across three places — the three dev-notes citations plus
`docs/known_issues.md` and this board. A `dev_notes`-only exemption would have
been red on day one. The count went 8 → 6 between segment A's grep and segment
B's because this board was rewritten in between: **a gate's exemption list is a
measurement of a moving tree, not a constant.**

**Supervisor's own verification**: `mix test test/plugin/ test/flezha/service/
test/flezha/state/metrics_test.exs` **588 passed**; `mix compile
--warnings-as-errors` and `mix format --check-formatted` clean; no retired name
live anywhere in `lib test config openspec/specs`; diff confined to `lib`, `test`,
`docs`, `scripts`, the one CI workflow and the one mechanical spec rename.

**5.4 was driven, not reasoned about** — a real Rust runner and controller, three
legs each asserted on a *delivered payload* rather than the phase word (which
`extension/table.ex:177-182` will report as `:running` for a dead plugin). Leg 1
ran the folded `Identificator.string_to_pid/1` live; leg 2 stopped
`preserve_state: true` and restarted, with seq continuing 0.0 → 1.0 so the state
genuinely survived; leg 3 killed the Overwatch so process_hub restarted it **from
the declared cspec** — the term carrying the provider module atom — and the sink
delivered at seq 2.0. That is the durable-name question of batch 2, answered live.

**The drive is NOT kept as a test, deliberately.** Its value was the one-time live
observation, and the paths it exercises already have system-tier coverage
(`effectively_once_e2e`, `schema_evolution_e2e`, `remote_durability_e2e`,
`TierE2EHarness`) which ran **1938 green on this exact tree**. A bespoke drive
script beside `TierE2EHarness` is the parallel mechanism this programme exists to
remove — charter §2. It is at `/tmp/segb_drive_test_kept.exs` until the box is
rebooted; nothing depends on it.

**KI-62 filed and ruled NOT A BUG** — `convert_value/2` does not run on the struct
plugin path. Both contracts are written down and they differ on purpose:
`provider_behaviour.ex:78` says the function exists because *"CLI plugin opts
arrive as"* strings, which is why its only caller takes a `string_map`, and
`beam_msg_forward.ex`'s moduledoc says **"Required opts: `:recv_pid` (pid)"**. A
binary there is a caller breaking a documented contract. Recorded in *Resolved*
with the residual — the contract is enforced by a raise deep in the sink chain
instead of a refusal at enable, so the mistake surfaces far from its cause.

**Three reds confirmed pre-existing and left standing**: `check-kind-naming.sh`
(five `bounded` prose hits; `git diff HEAD | grep '^+' | grep -ci bounded` = 0),
`php-cs-fixer` (a parse error inside `sdk/php/vendor/symfony/`, and the diff has
zero PHP files), and `reuse lint` (it listed the four deleted files while their
deletions were unstaged — self-clearing at commit, and it did).

`refactor_metrics.py --check` stays exit 1 on drift from `c5bdc26b`. The one line
the diff appears in is `lib/flezha/state prod_loc` via `state/metrics.ex (+16)` —
charter §11's named case, lines arriving *from* `lib/flezha/plugin`, which does not
appear in the regression list at all because the deletion on that side is larger.

### Segment A — verified 2026-08-31, commit `0cb58166`

Batches 1–3: the survey and the durable set re-derived, then the three renames.
18 of 18 in-scope boxes ticked and each verified against the repo by the
supervisor, not read off the report.

**What the supervisor re-ran rather than trusted**: all five moved files are
line-identical to their originals (601 / 237 / 14 / 46 / 111); the cross-tree
grep for the three retired names returns nothing live; `mix test
test/flezha/state/` **314 passed**; `mix compile --warnings-as-errors` and
`mix format --check-formatted` both clean; four of five `check-*.sh` gates green.

**The survey held.** Every number in batch 1 was exact except two prose sums,
and neither moves a classification: the colliding-module count is **49, not 47**
(`design.md`'s own D3 table has 49 rows carrying `seg ×>1`, so the 47 in
`proposal.md` is an arithmetic slip in the prose, not a survey that moved), and
credo's `excluded_lastnames` holds **50** names, not 47 — the quoted list is
correct, only the count sentence is wrong. The durable chain verified link by
link, the pinned set is 13, and none of the renamed modules is in it.

**Four `tasks.md` counts were wrong and are corrected in place**: `openspec/specs`
holds **2** references to `State.Registry`, not 14 — the 14 is
`openspec/changes/archive/`, which is history and outside the grep's tree list;
`as: StateRegistry` is in **2** files, not 1; `AuthZ.Store`'s "5 references" is
FQN-only and misses five grouped `alias Flezha.AuthZ.{…, Store, …}` lines with 26
call sites; and **`AliasOrder` at HEAD is 66, not D8's 50** — a fourth board
number off, so D8's excess-accounting starts from 66.

**The catch that justifies the segment.** `wal_transfer.ex:191` passes the module
as a *value* — `:erpc.call(source_node, Registry, :delete_files, …)`, no `.`
after it — so the first pass left it pointing at the stdlib `Elixir.Registry`
once the alias moved. **Compiler-silent.** The worker found it with a bare-token
sweep and fixed it; the supervisor re-verified the line reads `Paths`. This is
the failure mode a rename has that a fold does not, and segments C and D must
keep the sweep.

**Two consequences segments C and D must budget:**

1. **AliasUsage 653 → 689** (+36; lib 202→220, test 451→469). `Registry` is on
   credo's `excluded_lastnames` and **`Paths` is not**, so every site that was
   free now asks for an alias. None is blocked (`Paths` is unique), so D6's
   verdict stands and D8's floor is untouched: 1499 − 431 − 689 = **379**.
2. **Two non-`lib` alias targets share a last segment with a lib module** —
   `ProcessHub.Service.Recovery` ×6 and `KafkaEx.Auth.Config` ×2. Outside D6's 51
   by construction; the alias pass must not spell them bare unchecked.

**`refactor_metrics.py --check` is exit 1 with 13 regressions and 12 are drift**,
not this change: `baseline.json`'s head is `c5bdc26b` and HEAD is 58 commits past
it. Verified mechanically rather than accepted — every BLOCKING regression names
a file this change never touched (`flz_core/src/control_unit.rs`,
`export_test.exs`, `control_channel_send_control_test.exs`,
`ActivationDispatcher.java`), and the one it does name,
`wal_writer_outbox_test.exs`, is `7 added / 7 deleted` so its 973 lines did not
move. **The 13th is this change's own**: `duplicate_pairs` +1 in
`lib/flezha/authz` and `lib/flezha/schedule`, both `measuring` not blocking —
`authz/namespace_store.ex:50-56` ↔ `schedule/store.ex:78-84`, 70 identical
tokens that were **hidden only by the two files spelling the same module
differently**. That is D10.2's layering finding surfacing as a metric, and it is
the rename doing its job.

**`check-kind-naming.sh` is red and it is not this change** — four `bounded`
prose hits in `docs/known_issues.md` and this board, and `git diff` adds no line
containing the word. Pre-existing, as this board has recorded before.

**LOC: −1 production line** (the `state/supervisor.ex` parenthetical, deleted
because the rename made it noise). Every other edit is a 1:1 line replacement.
No charter rule is cited because none is needed.

**KI-61 filed and ruled** — see `docs/known_issues.md`. Real, **deferred to
`refactor-5a-state`**: `RecordLog`'s two `@spec`s name `Elixir.Registry.entity_id/0`,
a type the stdlib does not export, because the file never aliased the path module.
Pre-existing and behaviour-free. The supervisor reproduced it and then swept `lib/`
for a family — there is exactly one instance. Fixing it needs Dialyzer to satisfy
§1b's red-first rule and this repo gates none, so it goes where the proof is
affordable, the same treatment KI-41 has.

### Segmentation of row 3.5

| segment | batches | content | state |
|---|---|---|---|
| A | 1, 2, 3 | re-derive survey + durable set; commit 1a — three renames | **done**, `0cb58166` |
| B | 4, 5, 6 | commit 1b folds; commit 1c full verification + live restore drive; commit 1d the gate, proved red first | **done**, `055054f2` |
| C | 7, 8 | the alias pass — 689 sites, 103 modules, 189 files | **done**, `1848cb80` |
| D | 9, 10 | `.credo.exs` + the KI-63 fix, the measured ratchet, final verification | **done**, `14c7cfc5` |

**Two supervisor rulings every remaining segment carries:**

1. **The net-LOC excess has no charter justification and must not be given one.**
   `tasks.md` 10.5 says to "name the charter rule that forced it per §1". There is
   none: the alias lines are added because the user ruled on KI-10, and charter §1
   admits only §2's reuse order or a §7 ceiling. An honest "no charter rule; the
   user ruled" is the right answer.
2. **The `openspec/specs` edits are mechanical name updates carrying no requirement
   change**, so they need no `## MODIFIED Requirements` delta — the change's only
   delta stays `code-quality-baseline` (ADDED).

**Phase 1 row 3, `close-prometheus-label-coercion`, is DONE** — `55f32234` +
the close-out commit; archived, `observability-export` amended. Slice 124
passed, full suite 1857, verified by the supervisor.

**KI-45 was filed P2 "waiting on one new source" and was a live P1.** Two
`TelemetryBridge` paths carry a non-scalar today (`wal_backpressure`'s 3-tuple
`entity_id`, `operative_forward.retry`'s tuple `reason`), and the crash wiped
**five** ETS tables — every metric since boot — then four crashes in a measured
376 ms took the whole Export supervisor down. Three further failures were found
that do NOT raise, which a crash-only fix would have left standing.

**The change measured itself and the measurement changed it, twice.**
- The obvious regex sanitizer costs **9.7 µs per label name** in compiled module
  code — 12× a byte comprehension, and a module-attribute regex was no better.
  Caught before shipping; without it the funnel would have been ~40 µs/emit.
- The first cut still cost **4.85×** HEAD. Sent back for a bounded fix rather
  than filing a regression the change itself introduced: a validity fast path
  (no allocation when the name is already legal) plus a list scan for the
  collision check brought it to **1216 → 2166 ns/emit, 1.78×**, winning back
  ~65 % over three runs.
- The fast path had a trap the worker caught: `__foo` is grammatical under
  `[a-zA-Z_][a-zA-Z0-9_]*` but must NOT fast-path, because collapsing the
  reserved `__` run is part of the rule — fast-pathing it would have silently
  reopened the `__truncated__` forgery the change closes. Proved over 45,036
  inputs, 0 disagreements.

**Two task texts were refuted by doing them**, both now corrected in place:
"the OTLP body is valid UTF-8" is not an assertion that can exist (a fixed64
timestamp guarantees every body is invalid UTF-8; 0 of 20 on both signal kinds),
and the named bench emits against zero interested sinks so it cannot measure
this funnel at all.

Found and filed, not fixed: **KI-58** (the metric NAME is never checked against
the exposition grammar — deliberately a different fix, since a name is a
developer-chosen atom that belongs refused at registration) and **KI-60** (two
different JSON *value* rules shipping in one tree, which disagree).

Areas 0a, 0b, 1s, 1, 2, 3 done. **Next: row 3.5, `fix-opaque-module-names`** —
the user's queued item, see its detail block below.

### The one verdict that outlives this area

**Rows 1 and 2 carry a real regression of roughly 2–4 %, and it bisects to segment
B** — the typed settlement tail moving onto the shared `settle_effects`. Every later
arm is flat against B2 (+0.31 % on row 1, +0.13 % on row 2) and allocations are flat
across the step, so it is instruction path.

Read it against the **session's** floor, not batch 0's. B4 and B5 carry identical
production code — E and F touched only test modules, comments and two private names
— and still separated **1.93 %** on row 1; arm A′ spanned 5.6 % within itself. The
0.33 %/0.95 % floors were taken on a cold idle box and did not survive an hour of
measurement. So: a real cost, imprecisely measured.

**Accepted, not reverted.** The seam is charter §3's stated goal — one settlement
tail instead of two — and reverting the area's core change on a number this noisy
trades a structural gain for a measurement artifact. **A clean re-measure on a cold
box is owed** before anyone decides to optimise or keep it. To reproduce: arm A′ is
the tree at `119a20d4`, arm B2 the tree at `024daaaa`; build **both the same way**
(two worktrees are comparable to each other, a worktree and the tree are not) and run
row 1 at `FLEZHA_BENCH_MEASUREMENT_SECS=60`, `FLEZHA_WORKER_POOL_SIZE=8`, five paired
rounds. The arms themselves die with this session's scratchpad.

This is also the first time the seam has been priced at all. The 1s spike put it at
≤ 1.45 % on a branch; checkpoint 2 built its arm and measured nothing, by the
rewritten §8. That is the policy working as designed — the cost was found, and found
attributably, for one 7-minute bisect instead of six measured checkpoints.

## CI is red — added to the work list 2026-08-31 (session 4, on the user's instruction)

**Six workflows are failing, and they are FOUR causes, not six.** Every failing run is
on **`main`**, which is **287 commits behind `dev`**. Diagnosed by reading the run logs
and then testing each cause against `dev` — which is the only question that matters,
because `main`'s red says nothing about the branch we are working on.

| workflow | run | failing step | cause | red on `dev`? |
|---|---|---|---|---|
| Nightly E2E | `33374859562` | Compile Elixir (warnings as errors) | **A** | **no** |
| Rust SDK Bench Cron | `33378395915` | mix deps.get + compile | **A** | **no** |
| Elixir | `33310818915` | Compile Elixir (warnings as errors) | **A** | **no** |
| Java SDK · mix-integration-matrix | `33310818924` | shared smoke tier vs the Java runtime | **A** | **no** |
| Java SDK · java-unit-tests | `33310818924` | Verify vs committed natives | **B** | **no** |
| Naming | `33310818916` | check-kind-naming | **C** | **YES** |
| Ruby SDK · gem (3.3), gem (3.4) | `33310818919` | Unit tests | **D** | unverified |
| Ruby SDK · cross-build ×3 | `33310818919` | `oxidize-rb/actions/cross-gem@v1` | **D′** | unverified |

### Cause A — `process_hub` will not compile (4 of the 6 workflows). ALREADY FIXED ON `dev`.

    error: ProcessHub.Hub.__struct__/1 is undefined, cannot expand struct ProcessHub.Hub
    └─ lib/process_hub/hub.ex:79:19: ProcessHub.Hub.for_workers/1
    == Compilation error in file lib/process_hub/hub.ex ==

`main`'s `mix.exs` still carries `process_hub_dep/0` returning
`[git: "https://github.com/alfetahe/process-hub", branch: "dev"]`, and something in CI
takes the branch **tip** over the committed lock — `adopt-process-hub-hex-release`'s own
notes record `docker/prod/Dockerfile.controller` doing exactly that, and the failing
function is `for_workers/1`, **the one function that change measured as having moved**.
`dev` resolves `"~> 0.7.0"` with `mix.lock` pinning hex 0.7.0. **No new work: this is
`adopt-process-hub-hex-release`, already landed on `dev`.**

### Cause B — the Maven enforcer requires natives that no longer exist. ALREADY FIXED ON `dev`.

    [ERROR] maven-enforcer-plugin:3.5.0:enforce (require-all-native-binaries)
    [ERROR] native-verify: one or more of the five committed libflz_core_ffi binaries
            are missing. Refresh them with `scripts/build-dist.sh` and `git add`.

`main` carries the rule at `sdk/java/flezha-sdk-native/pom.xml:170` while tracking
**zero** natives — `unbundle-committed-build-artifacts` untracked them and `main` got
the purge without the pom change. `dev` removed the rule in `5457bb8f`
(*build(java): the seven build artefacts leave git*); `git grep require-all-native-binaries`
over `dev` returns nothing. **No new work.**

### Cause C — `check-kind-naming.sh` is red, on `main` AND on `dev`. REAL WORK. This is KI-94.

Measured on `dev` at `887bf308`: **exit 1, 19 occurrences** (`main` had 24).
`check-overflow-naming.sh` is **green**. The 19 split:

| where | count | note |
|---|---|---|
| `docs/refactor/` | **8** | **this board's own prose** — supervisor-authored, including two lines *about* the gate being red |
| `docs/` | 6 | prose |
| `docs/dev_notes/` | 1 | prose |
| `lib/` | **4** | `http_poll.ex:251`,`:256` (`bounded_put`), `service/cli/boot.ex:3`, `service/operative_manager/placement.ex:4` |

Every one is the **capacity or plain-English sense of "bounded"**, which is precisely
what `scripts/kind-naming-allowlist.txt` exists to absorb — the gate's own failure
message says so. KI-94 already files this as *red on prose hits, none in code*; the
count has since grown because the programme keeps writing the word.

**This gate has been red for the entire refactor programme and nobody has been able to
merge past it.** It is cheap to fix and it is the only one of the four causes that
blocks `dev`.

### Cause D — Ruby SDK unit tests fail on both 3.3 and 3.4. REAL WORK, unverified on `dev`.

    RunnerTest#test_warmup_runs_after_the_last_declaration_and_before_the_first_poll:
    NoMethodError: undefined method 'stub' for module Process
        test/runner_test.rb:60:in 'RunnerTest#boot_recording'

`Object#stub` comes from `minitest/mock`; it is not available on a **module** the way
the test uses it, and it fails identically on 3.3 and 3.4. `Process.warmup` is the
3.3+ call `ruby-sdk` made an unconditional requirement, so this test is stubbing the
very thing that pinned the floor. **Not yet reproduced on `dev` — do that first.**

### Cause D′ — Ruby `cross-build` fails on all three triples.

`oxidize-rb/actions/cross-gem@v1` exits 1 with no error line of its own. The only
diagnostics are benign (absent git credentials; a `402 Payment Required` from the
`warehouse-clerk-tmp.vercel.app` quickinstall mirror, which is a third-party binary
cache, not ours). **Likely external/infra rather than our code — confirm before
spending on it**, and note it is the same three-triple cross-build that KI-52's
distribution question is about.

### The order this should be worked

1. **Cause C first** — it is red on `dev`, it is minutes of work, and it is the only
   one gating the branch. Mostly an allowlist edit; the four `lib/` hits want reading
   before they are allowlisted rather than renamed.
2. **Cause D** — reproduce on `dev` (`cd sdk/ruby && rake test`) before writing
   anything. If it reproduces it is a genuine SDK test defect.
3. **Cause D′** — one look to decide external-vs-ours.
4. **Causes A and B need no code at all.** They are `main` being 287 commits stale.
   What they actually argue for is a **`dev` → `main` merge**, which is the user's
   call and is not something this programme does unasked — the branch carries 287
   unreviewed-by-them commits including the whole refactor.

**A CI honesty note worth keeping**: four of six red workflows were fixed days ago and
stayed red because nothing re-ran them on a branch that had the fix. A red CI nobody
can act on decays into a red CI nobody reads.

## Awaiting the user's decision

### Gate finding — allowlist rules erase in file order

`scripts/naming_gate.py`'s `pattern_check` applies allowlist rules **destructively
and in file order**, so a later rule cannot match text an earlier rule has already
partially consumed. Adding a phrase-shaped entry for the board's `const bounded`
citation failed for exactly this reason: the existing `[Bb]ounded[Mm]ailbox` rule
ate half the phrase first, leaving the bare word to trip the gate. The entry now
matches the bare identifier and the comment beside it says why. Worth knowing before
anyone writes another multi-token allowlist entry.

### A/B policy changed 2026-08-29 — measure once per area

The user's call, on the evidence that benchmarking was **61 % of area 2's closing
segment** (66 of 108 minutes) against 30 % tests and 9 % builds. Charter §8 rewritten
(`ecdd834f`); every later area follows it:

1. **Build every checkpoint, measure once.** A checkpoint builds and sha-keeps its
   arm (~2 min); only the area's LAST checkpoint measures. A regression there is
   attributed by a bisect over the kept arms, run only when there is something to
   attribute. Area 2 measured three times and both decisive answers came from kept
   arms — keeping arms buys attribution, re-measuring per checkpoint bought nothing.
2. **An unchanged binary is not re-measured** — equal `sha256sum` across arms means
   the row cannot have moved, and the sha is the evidence.
3. **Three rounds detect, five confirm** — escalate only on an unbroken direction.
   Area 2's real regression was 5/5 and would have shown at 3.
4. **Only a gate row gets the long window** — each row is declared gate/guard/
   direction with its floor in the area's `design.md`, which is now the charter's
   mechanism rather than a convention.

Expected effect: an area's bench cost falls from three measured passes at five rounds
to one at three, plus a bisect only when a regression appears — roughly a 70 % cut
with attribution preserved, because the arms are still kept at every checkpoint.

## Found defects (not fixed inline — each becomes its own change)

> **These now live in `docs/known_issues.md`**, which is tracked and outlives this
> programme, with severity, evidence and what a fix must account for. Forty entries
> as of 2026-08-30, including the pre-existing ones this board never held (the bench
> gate that cannot complete, Java's workflow resume re-running committed producers,
> the unvalidated OOM fix, the production items). `fix-outbox-watermark-overwrite`
> has since closed KI-1; `fix-first-input-drop-on-bind` closed KI-2 (`e16260a7`). Both are done.
> Keep appending here as areas find things; move them across when an area closes.

- 0a: `.github/workflows/format.yml` does not run the four veneer formatters, so CI
  and `format-check.sh` no longer mirror each other for Node/Python/Ruby/PHP (owed).
- 0a: `.formatter.exs` `inputs` miss `scripts/auto_restore_persistence_smoke.exs`
  and `bench/acceptance/summarize.exs`.
- 0a: `format-apply.sh` runs `cargo +nightly fmt` while CI pins `nightly-2026-05-07`.
- 0a: `reuse` was not installed on this box, so `format-check.sh` could never have
  been green locally before (now installed under `~/.local`).
- 0a: the Elixir controller has no layering today — one 55-file cycle across nine
  subtrees (charter §3 records the measured graph; areas 5a–5e cut it).
- 0a: credo strict reports 1349 issues, 967 of them `AliasUsage` and
  `ParenthesesOnZeroArityDefs`, which contradict house style — RESOLVED by row 3.5 — see KI-10
  disable-or-adopt before the count is ratcheted as blocking.
- 0a: `cargo clippy --workspace --all-targets` and `cargo test --workspace` are red
  without `--features flezha_runner/bench-internals`.
- 0a: `mix deps.get` reports packages with security advisories (pre-existing).
- 0a: `.git/info/exclude` on this box hides `bench/flink/`; other boxes see reports.
- 0b **F1 — PRODUCT DEFECT, CLOSED by `fix-outbox-watermark-overwrite` (`9410eea7`)**:
  after an effectively-once park, `WalWriter.take_outbox_window/3` re-enters
  `dispatch_outbox_window` via `complete_entry({:cursor,1})` → `try_dispatch_outbox`
  (`wal_writer.ex:1288`) while the outer window is open; the nested window advances
  the consumed watermark to 2, then the outer window's `InputLog.record_consumed/3`
  (`input_log.ex:189`, unconditional `:ets.insert`) overwrites it with 1. Fires
  whenever the park record and the next record share one fsync batch. Impact: the
  input-log retention floor and credit accounting under-report by one offset after
  every park until the next input. `test/system/dlq_e2e_test.exs` (now line **121**, was 119) fails 14/20
  alone — it was never a load flake. Left red, untagged, per charter §9.
- 0b F5 — `test/flezha_test.exs:591` (runner disconnect, preserve) failed 1/6 full
  runs, 0/10 alone, 0/3 instrumented; root cause not established; left as is.
- 0b ~~`mix test --include system` always exits 2~~ **REFUTED at HEAD (2e)**: both
  `:postgres_integration` modules now self-skip via `SystemCase.skip_unless`, and four
  of nine runtime-arm runs exited 0. The `:interop` self-skip pattern landed.
- 0b UDS socket path overflow in the system tier: a stream socket path of 114 bytes
  (`/tmp/flezha_sockets/<node>/secondary_stream_<namespace>_<ids>_wus.sock`) exceeds
  `sun_path`; 120 "Failed to start primary listener" + 167 "Failed to connect runner"
  error lines per run while the tests still pass. Owed.
- 0b commit `087d0d28`'s `:sys.get_state(Loop)` drain in the output-stall drill is
  inert (a 200 000-iteration probe: the `send_after(…, 0)` tick lands after the
  reply every time).
- 0b test hygiene: 738 leaked `/tmp/flezha_*` dirs (state-test and durability-stub
  roots never removed); `app_data/dlq` holds leftovers from tests using the default
  DLQ dir. Owed (area 7).
- 0b three uncaptured `[error]` lines in a plain `mix test` (two sdk_lang-mismatch
  restore refusals, one ControlUnit abnormal shutdown) would trip CI's uncaptured-log
  gate on this suite shape.
- 0b **first-input drop after `start_operative/2` — PRODUCT DEFECT, own change owed**:
  the start reply comes on the control-channel ack while the runner's primary data
  stream binds later; a `send_io` cast in between hits `io_in/3` → `{:error,
  :no_stream}` and is dropped with only `nil_drops` + `[:flezha, :stream, :drop]`;
  a tier-on input is journaled (`Ingest.verdict/2` answers `:ok` on `:no_stream`)
  and never forwarded at bind. A drop probe logged 38 drops on exactly the failing
  e2e operatives. Cure: a start reply that waits for the bind, or a cast that
  cannot drop. **RESOLVED by `fix-first-input-drop-on-bind`** (KI-2): the
  operative bind waits for its data-stream channel and an undelivered payload
  arms a bind re-forward, so `await_stream_bound/2` is deleted from the tree.
- 0b UDS driver `DropNewest` write-buffer drops are invisible to the caller
  (`Native.send_frame/4` always answers `:ok`); only `driver_stats.write_buffer_dropped`
  sees them (the last silent-drop default; already on the memory list).
- 0b `unique/1` suffixes reach 7 digits in one BEAM, so any id prefix over 24 chars
  breaches the 32-byte operative-id cap late in a combined run.
- 0b `sdk_smoke_e2e_test.exs` `send_until_forwarded` is the old re-send workaround
  for the first-input drop. **RESOLVED by `fix-first-input-drop-on-bind`**
  (KI-20): deleted, with nothing folded onto — the single `send_io` stands, and
  `await_stream_bound/2` no longer exists to fold onto.
- 0b F5 (`flezha_test.exs:591`) still unexplained; 10/10 alone again.
- 1s `test/apps/python_runner/__pycache__/fixtures.cpython-312.pyc` is COMMITTED
  and stale; every Python runner launch dirties the tree (area 2 deletes it and
  ignores `__pycache__`).
- 1s/2e the installed veneer `.so`/`.node` are DEBUG builds whenever a script runs
  without `--release`, which is its default — **this is general to all four veneers,
  not Python's alone**, and the tree ends area 2 holding debug artifacts. A
  `--runtime <veneer>` round trip under `FLEZHA_BENCH_SKIP_BUILD=1` then measures a
  debug veneer silently.
- 1s `mix bench.roundtrip --runtime python -p 2` with the default `--runners 2` is
  bimodal (~20 k vs ~33 k rec/s) on placement and the report records nothing
  about placement.
- 1s the erased path pays 8.09 allocs/elem vs the typed 7.02, pre-existing:
  `to_schema` clones payload + metadata and mints two `String`s per activation;
  `resolve_item` calls `entity_ref_of` per item even with no outputs (area 1).
- 1s `sdk/rust` `coalesce` allocates its map + mask whenever the buffer is
  non-empty, key or no key (area 3).
- 1a row 5 (`host_loop`, 60 s) noise floor is 4.13 % on this box today (spike: 2.60 %)
  — above the gates spec's 3 % clause; the paired-round rule decides that row.
- 1a `openspec/specs/controller-packaging/spec.md:270,290` still name
  `libflz_transport.{so,dylib,dll}` as committed binaries, and `flz_jni` (a crate
  that does not exist) is named in `README.md:48`, `docker/dev/README.md:26` and
  four specs — wording corrections owed in segment C (batch 11 docs), wording only.
- 1a `flz_core_ffi/benches/host_loop.rs` copies its scaffolding from
  `sdk/rust/benches/{dispatch,correctness}.rs` and `sdk/rust/tests/common/mod.rs`
  (7 jscpd pairs) and carries one `thread::sleep`: segment C (batch 10) extracts
  the shared scaffolding into `flz_core` behind `bench-internals`; area 3 moves the
  SDK benches onto it.
- 1a `effect.rs` ↔ `sdk/rust/src/entity/flusher.rs` duplicate pairs are the
  interim of design D1; area 3's port removes them (owed, by design).
- 1a a new pair `effect.rs:31-54` (`EffectEntry`) ↔ `schema.rs:234-240`
  (`ItemEffect`): the same field list twice — segment B decides in batch 4.
- 1b the typed dispatch bench (row 1) is 0.6–1.2 % behind after batches 4–7, B
  slower in 10/10 pairs across two arms — inside the floor and the rule, but a
  consistent small cost on the typed path; area 3 (which rewrites that path) owns
  finding it.
- 1b `flz_nif/src/frame_buffer.rs` `feed` (69 lines) is over the function ceiling
  and no task owned it; segment C takes it in batch 9.
- 1b the Rust SDK test `tier_replay_skips…` has a second flake shape under parallel
  runs (5 s `wait_until`, ~1–2/26) attributed to pool starvation by parking tests;
  area 3.
- 1b row 6 (whole-system round trip) has no measured floor; 5-second runs spread
  ~18 % within one arm — the row is a direction, not a gate, until the shape is
  lengthened.
- 1c **`effectively_once_checkpoint_e2e_test.exs:37` is the 0b first-input drop on
  the RESTORE re-forward path**: after a rebind the controller re-forwards offsets
  onto a data stream the fresh runner has not bound yet, three
  `[:flezha, :stream, :drop]` in one ms, never re-driven; reproduced on the 40th
  probed iteration alone. Same cure as the first-input drop (a bind-aware forward).
- 1c `mvn … verify` straight after `scripts/build-dist.sh` fails at the shade step
  (stale `target/`); the Java suite command is `clean verify`.
- 1c the Ruby veneer does not build here without
  `BINDGEN_EXTRA_CLANG_ARGS=-I/usr/lib/gcc/x86_64-linux-gnu/13/include` (no
  `libclang-common-*-dev`); PHP's `composer install` runs only in the
  `flezha-php-zts` container.
- 1c row-5 (`host_loop`) re-measure after the bench-scaffolding extraction was
  REFUSED (box on battery); the pre-extraction B11 figure stands; area 3's A/B
  measures row 5 again on mains.
- 1c `target/release/rust_runner` in the tree is arm B7's binary (the A/B restore);
  harmless — the e2e builds via `cargo run`.
- 1c `sdk/rust/tests/stream_transforms.rs:249` fails under a parallel suite run on a
  loaded box (5 s `wait_until`); same family as `tier_replay_skips…`; area 3.
- 3e **`sdk/rust/tests/ui/*.rs` must NOT be rustfmt-formatted.** Their line numbers
  are pinned by the `.stderr` snapshots, and `cargo fmt` never reaches them because
  trybuild compiles them out-of-tree — so a per-file `rustfmt` loop DOES reach them
  and breaks the snapshots. Segment E's worker hit this, restored all five files
  byte-identically with `git show HEAD:<path> > <path>` (no checkout/restore/reset),
  and reported it. Owed a line in the area's dev notes.
- 3e whole-repo `duplicate_pairs` rose **183 → 235**: fifteen new test binaries each
  repeat the same `use` header, and `benches/workflow_scaling.rs` now pairs with the
  seven workflow-fixture files batch 9 split out of one 4 281-line file. Structural,
  not sloppiness — but `sdk/rust` re-baselines at the area close, so the rise would
  become the new floor. Added as task **11.0** (a `tests/common::prelude`), with the
  instruction to report the count in ONE scan mode and to say plainly if the fix does
  not move it.
- 3d a stale doc block rode along with `channel_metadata` into
  `sdk/rust/src/operative/dispatcher/emit_sink.rs:344-350`. It documents a function
  that no longer exists, names the RETIRED `Block` overflow policy, and carries a
  design-decision label charter §5 forbids. Relocated verbatim by an extraction-only
  task, correctly; batch 11.1/11.3 owns the rewrite and the new location is recorded
  here so it is not lost.
- 3b the board's `sdk/rust` `duplicate_pairs = 178` (recorded after segment A from the
  ratchet's full-repo run) and the worker's own repeated reading of **128** on the
  same untouched tree are the SAME scan-mode hazard as 3a. Segment B's worker refused
  to claim a −50 improvement it could not evidence and reported the metric flat. The
  two numbers must never be compared until the hazard is fixed.
- 3a **`duplicate_pairs` depends on which areas are scanned in one invocation** —
  `sdk/rust` reads **128** alone, **136** with two sibling areas and **178** full-repo
  on IDENTICAL source, because a cross-area pair is attributed to both areas.
  Reproduced by the supervisor. `--check` uses the full run and today's baseline was
  written the same way, so the gate is self-consistent right now; but a baseline
  recorded with `--area` and checked against the full run would fire falsely, and the
  per-area numbers quoted in reports are not comparable with the ratchet's. Owed a fix
  in the metrics script (area 0a's tooling): either scan the full repo always, or
  record the mode alongside the number.
- 3a the proposal's "21 of 133 duplicate pairs involve `benches/`" was wrong on the
  ratchet's own full-repo run — the baseline has **27** and batch 2 removed **5**.
  The worker corrected this against its own interest; the pair COUNT understates the
  work, since duplicated tokens in bench pairs fell **2086 → 1427 (−32 %)** and both
  giants (290 and 280 tokens) are gone.
- 2e **the PHP conformance arm through a container needs `--network host`** — without
  it every `transport=quic` test fails ("temporary test runner not connected") while
  UDS passes; and the container shim LEAKS one container per wedge drill, because the
  port kill reaches only the `docker run` client while `wedge_probe`'s handler never
  returns. Both leaked containers were removed by the worker; none remain.
- 2e `flz_core/src/control_unit.rs:19` — `unused_imports` on `socket_dir` when
  cross-compiled to `x86_64-pc-windows-gnu`; only `build-dist.sh` surfaces it. Used
  solely inside `#[cfg(unix)]`. Pre-existing at the baseline, area 1's crate.
- 2e FOUR load-sensitive reds not previously on the board, each seen once and green
  in isolation afterwards: `workflow_crash_resume_e2e_test.exs:249` (**reproduced at
  `refactor-baseline-2`, so pre-existing**), `effectively_once_e2e_test.exs:34` (a
  DIFFERENT module from the board's `effectively_once_checkpoint_e2e_test.exs:37`),
  `plugin/kafka/worker_registry_test.exs:303` (a controller unit test with no runner),
  and `flezha_test.exs:383` live handover (NOT the board's F5 at `:591`).
- 2e the UDS `sun_path` overflow recorded under 0b has a NEW consequence: it can FAIL
  a test, not merely log. `schema_evolution_e2e_test.exs:65` on the python arm is
  preceded by `[flz_nif] UDS driver fatal error: path must be shorter than
  libc::sockaddr_un.sun_path`.
- 2d `sdk/php/src/Executor/SwooleExecutor.php:271` carries a PRODUCTION
  `usleep(5_000)` that nothing states or bounds in writing. The sleep metric counts
  test paths only, so no gate sees it. Probably D8's "production, bounded, stated"
  family — but it is not stated. Owed a decision in area 7 or a PHP change.
- 2d the `flezha-php-zts` image has NO coverage driver (neither xdebug nor pcov), so
  the design's `phpunit --coverage-text` cannot run in the container. Harmless this
  segment (no PHP test was deleted) and blocking for any future PHP D9 diff.
- 2d `ruff check` is red at HEAD (68 findings, 16 of them `I001` import ordering) and
  is run by NEITHER CI nor `format-check.sh` — only `ruff format` is. The area is
  otherwise warning-clean, so this is a gate hole, not a code defect.
- 2c `flz_core_ffi/tests/abi/shape.rs` carries two test helpers over the charter's
  50-line function ceiling (`primitives_round_trip` 75, `assert_kind_maps` 72).
  Both are segment B's content relocated by the split, both are flat tables, and
  `refactor_metrics.py` counts production functions only — so no gate sees them.
  RESOLVED 2026-08-29 by amending charter §7 (`63c45c1d`): the function ceiling is
  production only, which is what the gate has always measured, so the charter and
  the gate now agree instead of leaving an invisible gap. The file ceiling still
  binds tests — that is what forced the `abi_scenario` split.
- 2c `c8` is not installed in `sdk/node`, so the D9 coverage diff for Node used
  Node 25's built-in `--experimental-test-coverage` rather than adding a
  devDependency. Same line data; recorded as a deviation from the design's named
  tool.
- 2c `coverage` 7.16.0 was installed into the gitignored `sdk/python/.venv` to run
  the D9 diff — a box-state change, not a tree change.
- 2 (survey) the board's "Python veneer `.so` is a debug build" is STALE: Python's
  installed `.so` is the release build; Ruby's, Node's and PHP's `linux-x64` are
  debug; none is tracked.
- 2 (survey) three committed `.pyc` (two under `examples/python/durable_agent/`, one
  under `test/apps/python_runner/`); root `.gitignore` lacks `__pycache__` — area 2
  batch 0 deletes and ignores them.
- 2 (survey) **owed follow-up `sdk-declaration-refusals-in-the-core`**: the core
  refuses 19 declaration rules the four veneers ALSO refuse (different wording), the
  core's cycle/reachability/single-root checks are workflow-only (a stream class's
  DAG is checked by every SDK and never by the core), and two veneer rules the core
  lacks. Moving them changes refusal texts → its own cross-SDK change after area 4;
  it removes ~19 rules × 4 SDKs of duplicated logic.
- 2 (survey) `mix bench.roundtrip`'s moduledoc names `--runtime rust|java|elixir`;
  the manifests give it five more (area 5d docs).
- 2a the checkpoint-1 A/B (rows 5, 7py, 7node; noise floors for rows 7) is OWED:
  the box was on battery for the whole segment. Arm A (`$SCRATCH/area2/A/`) and
  arm B2 (`$SCRATCH/area2/B2/`) are kept with sha256; `$SCRATCH/area2/ab_run.sh
  floors|checkpoint1` runs them on mains. **Plug the laptop in.**
- 2a two JNI-only refusal texts changed wording ("namespace contains NUL" →
  "namespace contains a NUL byte", same for "instance id") because JNI now reads
  `veneer::c_string`; nothing in `sdk/java` asserts them. Two napi_host diagnostic
  log lines lost a redundant prefix. Recorded as text deltas, not behaviour.
- HANDOVER note: the scratchpad (`$SCRATCH`) is per session — area 2's arm A / B2
  artifacts and the `ab_run.sh` driver from session 1 are gone in a new session;
  the A/B driver has to be rebuilt (a short script: copy the arm's `.so`/`.node`
  in, sha-verify, run `mix bench.roundtrip … FLEZHA_BENCH_SKIP_BUILD=1`, restore).
- **0a REGRESSION, introduced by this programme, fixed inline under charter §1's
  "unless it blocks the area" clause**: `4a0a3bac` (`refactor(0a)`) renamed
  `const bounded` → `const boundedMailbox` in `test/apps/node_runner/fixtures.mjs`
  onto a name the file already declared at top level, so the module has been a
  `SyntaxError` ever since (`node --check` fails at `refactor-baseline-2` and at
  every commit from `4a0a3bac` on) and the Node runner test app could not load a
  single fixture. Nothing caught it for four areas: the ExUnit suite never parses
  the test apps, and the e2e runtime axis (`FLEZHA_E2E_RUNTIME`) defaults to rust,
  so no Node e2e has run since 0a. It surfaced only because row 7node of area 2's
  A/B drives that app. The segment-B worker renamed the procedure factory to
  `boundedMailboxProcedure`, the minimal restoration that keeps 0a's intent.
  A parse sweep of every tracked `.mjs`/`.js`/`.py`/`.rb` (supervisor, 14:22)
  found NO other casualty. **Owed**: a gate that parses the test apps — a
  `node --check` / `py_compile` / `ruby -c` pass in `scripts/format-check.sh` is
  the cheap form, and it belongs beside the naming gate that caused this, since a
  cross-language identifier rename is exactly what needs a parse check.
- 2 (supervisor, session 2) the history rewrite (`fix.sh`, `filter-branch`) left
  `.git-blame-ignore-revs` naming five dead SHAs, `docs/refactor/baseline.json`'s
  `head` at `309e8c71` (dead), and dead hashes in `CLAUDE.md`, `docs/dev_notes/
  refactor-0-baseline.md`, `docs/refactor/targets.md` and `charter.md`. Supervisor
  reconciles them in a docs commit this session.
- 2 (supervisor, session 2) foreign untracked file `fix.sh` in the tree (the
  rewrite script); the untracked `test/apps/node_runner/package-lock.json` noted
  by session 1 is gone. Neither is this change's; left alone.
- 1s a stale worktree from another session (`…/55da9198…/scratchpad/baseline2`,
  detached at `d59d630f`, a pre-rewrite hash, prunable) is not ours; left alone.

## Decisions

- 2026-08-28 Workers run sequentially in the shared tree; worktrees only for a
  proven-disjoint pair, and only after Phase 0.
- 2026-08-28 The supervisor commits per area without asking; nothing is pushed
  by any session.
- 2026-08-28 Flakes are fixed, not quarantined.
- 2026-08-28 Public SDK authoring names change only where the charter's
  vocabulary demands it, and then in every SDK + docs in one change.
- 2026-08-28 The typed Rust/Java dispatchers and the erased host converge on
  one generic core only if the 1s spike shows no loss beyond noise.
- 2026-08-28 File ceiling 800 lines; function ceiling 50.
- 2026-08-28 Dialyzer is not a Phase 0 gate (PLT cost); revisit in area 8.
- 2026-08-28 Elixir formatter version: CI moves to 1.20.2 (prod/dev images and
  the box already are; OTP stays 28 in CI). Measured at `31866dc5`: 1.19.2 and
  1.20.2 format the tree identically, so the "local vs CI disagree" claim was stale.
- 2026-08-28 `sdk/node/types-tests/` is excluded from prettier (a reflow moves
  `@ts-expect-error` off its line).
- 2026-08-28 jscpd is the one duplication detector for all seven languages
  (Elixir via `formatsExts`); no script-side detector.
- 2026-08-28 Lint measuring mode: credo strict + cyclomatic ≤ 10; clippy
  `cognitive_complexity` warn at 25 through `[workspace.lints]`, every crate opted in.
- 2026-08-28 Bench numbers are recorded, never ratcheted by `--check`; charter §8's
  interleaved A/B is the only perf gate.
- 2026-08-28 `baseline.json` keeps per-file production LOC / markers / sleeps /
  refutes so a regression names its file.
- 2026-08-29 Flake instrumentation lives in the scratchpad (`elixir -r probes.exs
  -S mix test`), never in the tree.
- 2026-08-29 "Green in isolation" is not the discriminator: F1 fails alone 70 %,
  F5/F6 pass alone. Reproduce per charter D5 before touching anything.
- 2026-08-29 The one sanctioned poll primitive (`NodeTestHelper.wait_until/2`, the
  target the copied loops collapse onto) keeps a real `Process.sleep` for its
  interval, honestly named; the `test` sleeps metric floors at 1, never disguised
  as `receive … after`.
- 2026-08-29 The naming gates walk `git ls-files`: an untracked new file is invisible
  to them (commit `4a0a3bac` landed with three offences in its own dev notes this
  way). The supervisor `git add -N`s a worker's new files before gating.
- 2026-08-29 The e2e first-input race is NOT sink-plugin activation (that wait ran
  green as a no-op and the failures continued); it is the data-stream bind. The
  settle was `NodeTestHelper.await_stream_bound/2` on the acceptor's own
  `:stream_ref_status` — a positive signal — never a re-send.
  **`fix-first-input-drop-on-bind` moved that wait into the product and deleted
  the helper**, so a bind race now fails the suite instead of being settled around.
- 2026-08-29 The combined one-BEAM `--include system` run is a different load
  profile from per-half runs and is the one that exposes the drop; both are run.
- 2026-08-29 Charter §3 "three dispatchers" is refuted by the code (1s survey): the
  loop is already one (`PollCore`, `EntitySlot`, `ProcedureDispatcher` with two
  impls, `StagedInterval<C>::fold`); Java's `ActivationDispatcher` is a host over
  the erased loop, not a third loop. What is duplicated is the effect-settlement
  tail (`invoke_once` vs `resolve_item`, ~230 lines) and the buffer struct
  (`TypedBuffer<S>` vs `BytesBuffer`). The spike measures exactly that seam
  (`EffectBuffer<C>` + `settle_effects<C>`); §3 is amended after the verdict.
- 2026-08-29 1s: converge — 4 tier_on: A 0.6467, B 0.6373 Melem/s (loss 1.45 %,
  floor n/a); rows 1/2/5 +0.45/−0.36/+0.97 % inside floors 0.73/1.91/2.60 %;
  allocs/elem flat (7.02→7.02 typed, 8.09→8.09 erased); row 6 (Rust whole-system)
  B faster 5/5; row 7 (Python whole-system) unresolvable — bimodal on placement,
  paired 2/5 on the one-runner supplement. Box moon, i7-8650U, powersave, AC.
- 2026-08-29 The seam's shape is a `Bodies` codec marker (`RawBodies` in the core,
  `TypedBodies<S>` in the SDK; `EffectBuffer<B: Bodies>`, `settle_effects<B>`), not
  a trait on `C` — the orphan rule forbids that (`Change` would move into
  `flz_core` and every user `impl Change for <std type>` breaks). Area 1
  re-implements it; the spike branch is read, never merged.
- 2026-08-29 Charter §3 "one host core" rewritten to the measured truth: one
  settlement tail and one buffer; the loop was already one.
- 2026-08-29 Hot-path A/B rows for the kernel: row 5 (`host_loop`) at
  `FLEZHA_BENCH_MEASUREMENT_SECS=60`; row 7 (`--runtime python`) needs
  `--runners 1` and ≥ 80 000 records to resolve 3 % at all.
- 2026-08-29 Area 1 runs as three worker segments (batches 0–3, 4–7, 8–11) with a
  supervisor commit at each checkpoint — one change, three green commits — because
  eleven batches exceed one worker's context and every batch leaves the tree green
  by construction.
- 2026-08-29 `flz_transport/src/ffi/` (459 lines, a cdylib nothing loads — Java
  bundles `libflz_core_ffi` via `scripts/build-native.sh`) and
  `flz_transport/src/generated/runner_commands.rs` (625, a stale copy of
  `flz_core::pb`) are deleted, not moved: verified by grep before approval.
- 2026-08-29 Batch 0 of area 1 corrects the metrics classifier (`#[cfg(all(test,…))]`,
  mid-file `mod tests` no longer swallows the rest of the file, `generated/`
  excluded) and re-baselines every area on the unchanged tree.
- 2026-08-29 Area 1 arm A = the working tree at the tag + the bench files (no
  worktree); Rust test regions end at the module's own-indentation `}`, never a
  brace count (a raw-string JSON in `host.rs` closes a brace count early).
- 2026-08-29 `EffectEntry<C>` and `ItemEffect` both stay: `ItemEffect` is the frozen
  ABI wire form, `EffectEntry` the generic buffered record; folding them is an ABI
  change. A NIF-touching batch's row 6 swaps `priv/native/flz_nif.so` per arm with
  sha checks. The routing admission callback is `impl FnMut` (static dispatch).
- 2026-08-29 A worker waiting on a background job's completion can miss the event
  (twice in segment B); the supervisor's heartbeat checks the job's marker file and
  nudges — a stalled worker is a heartbeat finding, not a failure.
- 2026-08-29 Area 1's error enums carry the old refusal STRINGS as variant payloads
  (`Error::Declaration(String)` …) with `Display` byte-identical: one shape per
  crate now, the strings themselves are wire/ABI contracts and stay; deeper typing
  is a later change, never a rephrase.
- 2026-08-29 Area 1 is +738 production lines by the metrics script (seam and bench
  scaffolding moved INTO the shared core, +391, twins still in `sdk/rust`; error
  enum +56; owner splits' mod.rs/imports) while the raw tree lost 1,851 dead
  lines. The net loss of the programme lands in areas 2–4 as the twins leave the
  SDKs — the kernel growing a little is what "one core" costs.
- 2026-08-29 A test is deleted only with an lcov diff (`cargo llvm-cov --lcov`,
  `DA:`/`FNDA:` per file) showing every lost line inside the deleted test's body.
- 2026-08-29 `flz_core`, `flz_transport`, `flz_nif` are blocking ratchets from
  `024efa60` (prod 12 639 / 1 743 / 2 048; markers 0; files>800 0; clippy 0).
- 2026-08-29 Rows 7 (veneer whole-system) run `-p 2 -m 40000 --runners 1` = 80 000
  records per run, the 1s spike's resolving shape; K33's "80 000 records" means the
  total, not `-m`.
- 2026-08-29 A segment whose A/B the box refused (battery) is still committed when
  every suite is green and the hot-path edit is a thin typed wrapper; the A/B is
  owed at the next checkpoint on mains with the earlier arm kept for attribution.
- 2026-08-29 `test` is a blocking ratchet from `9847ff7d`: sleeps ≤ 2, refutes ≤ 12,
  `async: false` share ≤ 0.815, ExUnit ≤ 56.2 s ± 5 % on this box.

## Baseline (rough, pre-Phase-0 — replaced by `baseline.json` in 0a)

Measured 2026-08-28 with grep/awk on this box; production = non-test lines.

| Area | Prod | Test | Notes |
|------|------|------|-------|
| `lib/` (controller) | 66.2K | 49.6K | 8 % `#` lines + 6.9K doc-block lines; 159/200 test files `async: false` |
| `flz_core` | 18.2K | 3.7K | 19 % doc comments; `host.rs` 3389 lines |
| `flz_transport` / `flz_nif` | 3.7K / 2.6K | 0.4K / 0.2K | |
| `flz_core_ffi` | 4.7K | 2.3K | four glue files ≈ one adapter ×4 |
| `sdk/rust` + macros | 10.6K + 1.2K | 20.1K | reference impl; `workflow/mod.rs` 426 prod / 3.8K test |
| `sdk/java` | 14.3K | 6.6K | incl. 1.5K examples |
| Node / Ruby / PHP / Python | 4.0K / 2.8K / 6.1K / 4.7K | 2.0K / 1.5K / 2.5K / 2.9K | |
| `cli` | 4.1K | 1.2K | |

Signals: 33 files > 800 lines; `convert_value` ×9, `now_ms` ×4,
`monotonic_nanos` ×4, `schedule_sweep` ×4; 213 sleeps + 138 `refute_receive`
in ExUnit, 60 `thread::sleep` in Rust tests; 11/22 e2e modules spawn a fleet
per test; both naming gates RED at HEAD; `mix format` differs local (1.20) vs
CI (1.19); no formatter for the four veneers.

## Next steps

### THE WORK QUEUE — everything agreed with the user, in order (2026-09-01)

Nothing agreed in conversation is allowed to live only in a transcript. Every row below
was decided with the user; the detail blocks follow further down this section.

| # | Change | Status | Decided |
|---|---|---|---|
| 0 | `retire-cluster-snapshot-remnants` | **DONE** `f3c30502`, archived | 2026-08-31 |
| 1 | **CI cause C** — `check-kind-naming` red on `dev` (19 hits, 15 prose, 4 in `lib/`) | queued, next | 2026-08-31 |
| 2 | `rename-kafka-queue-overflow` — `:block` → `:drop_nothing`, **plain rename, no accept-both**, + widen `check-overflow-naming.sh:47` which requires adjacency and so missed it | queued | 2026-08-31 |
| 3 | `collapse-observability-source-behaviours` — KI-100: rename `OtpBackend.Source` → `Poller`, macro declares the real behaviour | queued | 2026-09-01 |
| 4 | **CI cause D** — Ruby SDK `NoMethodError: undefined method 'stub' for module Process` on 3.3 and 3.4. **Reproduce on `dev` first** (`cd sdk/ruby && rake test`); it has only been seen on `main` | queued | 2026-08-31 |
| 5 | **CI cause D′** — Ruby `cross-gem` fails on all three triples with no error of its own. One look to decide external-vs-ours before spending | queued | 2026-08-31 |
| 6 | **Area 7** (`refactor-7-tests`) — proposal written and `--strict` valid, **awaiting supervisor verification** before implementation | in flight | — |
| 7 | **Area 8** (docs reconcile) — depends on 7 | pending | — |
| 8 | `unify-cli-output-flag` — KI-80: **`--output <fmt>` wins**; `--json` becomes its alias or is dropped. Today `cluster status --json` ignores `--output json` and `audit list --output json` ignores `--json` | queued | 2026-09-01 |
| 9 | `add-publish-readiness-checklist` — the pre-publish gate the user asked for; see below | queued | 2026-09-01 |
| 10 | `sdk-durable-fixture-parity` — **every SDK carries every capability, and proves it with a test.** The user's instruction 2026-09-01. Large; phased; see below | queued | 2026-09-01 |

**`## Awaiting the user's decision` is now EMPTY of open items for the first time since
the section was created.** KI-52 is deferred into row 9's checklist; KI-80, KI-100,
KI-81 and the Kafka rename are all decided and queued above.

### Area 7 survey — VERIFIED and APPROVED 2026-09-01

`openspec/changes/refactor-7-tests/`, `--strict` valid, tree untouched. Verified against
the repo, not re-read from the report.

**Passes §2 on every count.** All **23 helper modules** carry an explicit one-clause
decision — use-as-is / extend / generalise — in one table; the six files over 800 each
name their split AND its `duplicate_pairs` cost; the five folds are budget-accounted; the
36 markers and 81 credo warnings are classified by cause. Six candidates are **refused
with a price**, which is the part that makes the rest credible: the helper-namespace
rename (200 sites, zero metric movement), the `cert_fixtures` ↔ `cert_generator` merge (a
behaviour question about a finished blocking area), KI-31's de-duplication, chasing the
filed flakes, any `async: true` conversion, and the websocket fold — the last because five
of those seven files are already `async: true` and folding them would spend the very
budget the plan is funded by.

**It caught a hot-path subtlety I would have missed.** §8's fence is "this area edits no
file under `lib/`, `flz_core/`, `flz_transport/`, `flz_nif/`, `cli/` or `sdk/*/src`" — a
scope fence checkable from the diff rather than a claim. But it then names the exception
itself: **`test/apps/rust_runner` IS on a measured path**, because `mix bench.roundtrip
--runtime rust` spawns that binary through `PortPilot`, so §8's "an unchanged binary is
not re-measured" does not apply. Declared `direction` and not run — a module split inside
one binary crate gives rustc the same crate, MIR and codegen units, so the claimed effect
is zero against measured floors of 1.93 % (area 3) and a 0.3–3.3 ms spread (area 6) — with
each arm's `sha256sum` plus one `--include sdk_smoke` run recorded in the row's place.

**The correction that matters most**: the async budget has **zero** headroom, not the +2
this board claimed. Held is 43 true of 203 → `0.788`, and `round(1 − 43/204, 3) = 0.789`
reds the gate — so **one added `async: false` file breaks the ratchet**. The six mandatory
splits add ≥ 7 files on their own, which is why the batch order (templates → folds →
splits) is load-bearing rather than stylistic. The plan folds 18 `async: false` files
away, splits 6 into 14, and lands at **193 files / 43 true / 0.777** with +1 file of slack
and no `async: true` conversion.

**Also corrected**: charter §11f's fold formula was wrong by a rounding step
(`ceil((1−0.788)×203) = 44` declares the tree already short while `--check` exits 0; the
exact condition is `round(1 − true/files, 3) ≤ 0.788`); the `grep -rl 'async: true'`
warning over-reads by **seven**, not four; `targets.md` is stale for the sixth area running
**and names four paths that do not exist**; and `refactor-0-tests.md` cites
`NodeTestHelper.await_stream_bound/2`, which no longer exists — area 8's row.

**`test/apps` has never been ratcheted** (8 717 lines, 2 files over 800, 127 pairs, 8
sleeps). Batch 7 arms it as the 36th blocking area. Its recording must name which
`duplicate_pairs` rows are peer-driven: **61 of 127 have their other half in `sdk/rust`
(36), `sdk/java` (12), `sdk/php` (5) and `lib/mix` (2)**, all blocking, so a later edit
there must not read as a regression here.

### Row 10 — SDK capability parity, measured before it was queued

**The user's instruction**: every SDK carries the same features, and each is covered by a
test. This is the largest row in the queue and it is phased.

**Where it stands, read from the manifests, not from memory:**

| SDK | fixtures carried | deferred |
|---|---|---|
| rust (reference) | 16 | 0 |
| java | 16 | 0 |
| **python** | **15** | **1** (`workflow`) |
| node · ruby · php · elixir | 7 | **9 each** |

The nine are identical in all four: `state_persistence`, `effectively_once`,
`entity_timers`, `correction_window`, `keyed_channels`, `workflow`, `dynamic_entity`,
`entry_idle`, `schema_evolution` — which is, between them, most of the reason to choose
this platform over a queue.

**The fact that sizes the work: PYTHON ALREADY DID EIGHT OF THE NINE.** Python is a
veneer over the same `flz_core` C ABI as Node, Ruby and PHP, so the porting template is
**Python, not Rust** — a veneer has already been shown to carry `state_persistence`,
`effectively_once`, `entity_timers`, `correction_window`, `keyed_channels`,
`dynamic_entity`, `entry_idle` and `schema_evolution`. That converts most of this row
from "unknown feasibility" into repetition.

**Not all of it is fixtures, and the estimate must not pretend otherwise:**
- `keyed_channels` is a REAL mechanism gap in Node, Ruby and PHP — the SDK mints the
  identity stamps (`flz.entity`/`flz.seq`/`flz.key`) on a non-tier keyed plugin emit,
  which Java and Python do and those three do not.
- `workflow` is deferred by EVERY veneer including Python, owing two builder surfaces
  (an inter-proc `codec()` and a hybrid `sink().into()`). It is mechanism everywhere.
- **Elixir is the odd one out**: its SDK is its own core and inherits NOTHING from
  `flz_core`, so its nine cannot ride the veneer template at all.

**Phases:**
- **A · audit** — for each of the 9 × 4, is it a missing fixture or missing mechanism?
  Python's result predicts "fixture" for most; the answer is written down per cell
  before any code is written.
- **B · mechanism** — the identity stamps in Node/Ruby/PHP, and `workflow`'s two builder
  surfaces across the veneers.
- **C · fixtures** — port Python's, SDK by SDK. Elixir separately.
- **D · the ratchet, and the reason this sticks.** `test/sdk_conformance/gate_test.exs`
  already refuses a manifest that leaves a capability unaccounted, and refuses deferring
  anything in `NodeTestHelper.@required_capabilities` — today only six
  (`runner_lifecycle`, `graph_builder`, `liveness`, `retry_ladder`, `partitioned`,
  `pool_wedge`). **Each capability moves into that list the moment every SDK carries
  it.** That makes parity un-regressable and shrinks the remaining list visibly, rather
  than relying on anyone remembering.

**Sequencing**: this does not block the refactor and the refactor does not block it —
they touch different trees (`sdk/**` + `test/apps/**` versus `lib/**`). It is a
candidate for the second lane once area 7 is under way, at the user's call.

### Row 9 — the pre-publish readiness checklist (the user's idea, 2026-09-01)

**Why it exists**: publishing is a **one-way door**. While nothing is published, a
breaking change costs a commit; afterwards every one of these costs a deprecation cycle,
an accept-both parser and a migration note. `feedback_no_legacy_shims` — "no legacy
shims; the library is unpublished" — is a standing rule that **expires the day we
publish**, and several open items are only cheap because it still holds.

**Shape**: a tracked `docs/publishing/readiness.md`, one line per item with status, plus
the OpenSpec change that creates it and states the gate. NOT a section of
`docs/known_issues.md` — that file is defects, and most of this is packaging, policy and
end-user ergonomics, which is a different question with a different bar.

**Seed, from what is already known today** (the point is to capture it while it is fresh,
not the ceremony):
- **KI-52** — Java and PHP ship five native triples in one artefact; Node/Ruby/Python
  each solved it differently. Decide the distribution shape before the first release,
  never after.
- **KI-104** — a public API surface, so `flezha_ui` (and anyone else) stops consuming
  controller internals. This is the largest item and the most valuable.
- **SDK capability parity** — Node, Ruby, PHP and Elixir each defer nine capabilities on
  FIXTURES not mechanism (`sdk-durable-fixture-parity`); Python defers `workflow`. A
  published SDK matrix with nine holes is a support burden.
- **Licensing and governance** — `open-source-licensing` records what is still owed: a
  CLA bot, a `security@` contact and disclosure policy, and trademark. `process_hub` is
  now a hex release, so that half is done.
- **CLI consistency** — row 8 is one instance; sweep for others before operators' muscle
  memory sets.
- **Naming** — `check-kind-naming` is red (row 1) and `check-overflow-naming` misses the
  Kafka shape (row 2). Both gates exist precisely so published vocabulary is one rule.
- **CI gaps** — the suite skips `entity_timers`/`sugar_emit`/`simple_procedure` and never
  runs `cargo test -p flz_core`.
- **Versioning** — a semver policy and a real CHANGELOG discipline; there is no stated
  compatibility promise today.
- **First-run experience** — quickstart, error-message quality, and the defaults an
  evaluator meets in the first ten minutes.

**Not to be built now.** Seed it, keep adding to it as the refactor turns things up, and
work it when a publish date exists. Its value is that nothing is silently carried past
the one-way door.

**RULED BY THE USER 2026-09-01 — CI red on `main` is ACCEPTED until `main` is updated.**
Do not treat a red `main` as an incident, do not "fix" causes A and B, and do not open a
change for them. They are already fixed on `dev`; `main` is 287 commits behind and the
merge is the user's call on commits they have not reviewed. **What a session SHOULD do is
check whether a failure is red on `dev`** — that is the only question that matters here,
and the method is in *CI is red* below: reproduce the cause against this branch rather
than reading `main`'s run log. Only the naming gate (row 1) was ever red on `dev`.

**Not queued, and deliberately so:**
- **KI-104** (`flezha_ui` on controller internals) — the owed fix is a **public API
  surface**, and how big that surface is is the user's design call, not a refactor task.
  The user ruled the programme does not slow down for it and the UI is updated after.
- **CI causes A and B** — already fixed on `dev`; they are red only because `main` is
  287 commits behind. What they argue for is a `dev` → `main` merge, which is the
  user's call on 287 commits they have not reviewed.

**Still awaiting the user**: KI-80 (`--json` vs `--output json`), KI-52 (five native
triples vs one).


**DONE `f3c30502` (archived, three specs updated) — `retire-cluster-snapshot-remnants`.**
Net −207, `service` prod_loc 5828 → 5778 and `cli` 2831 → 2809, ceilings unchanged at
zero, full suite 1922 passed, `--check --lint` 0 regressions.

**The worker reversed four of my instructions and was right on all four**, which is the
value of the checkpoint and is recorded so the next brief is better: the
disabled-shutdown branch STAYS (`FLEZHA_GRACEFUL_SHUTDOWN_ENABLED` is a live kill-switch
read by `runtime.exs:261`, spec'd, and set by three compose files — deleting it would
have been a behaviour change inside a zero-behaviour-change task);
`FLEZHA_RESTORE_WAIT_TIMEOUT_S` bounds the SURVIVING control-socket wait so
`durability_posture.sh` needed no edit; FOUR scenarios leave `controller-packaging`, not
one; and `safe_registry_count/1` still has a caller in `boot_status/0`. It also found
sites the brief missed — `.dockerignore`, the authz action and its admin grant, two
further capability specs, and `docker/README.md`'s recovery section stale in five places.

**Session 3's open format discrepancy is RESOLVED.** `lib/flezha/service/cli_manager.ex`
IS unformatted, and it is unformatted **at HEAD** — verified by running
`mix format --check-formatted` on a `git show HEAD:` copy and on the worktree, both red.
Session 3 recorded it as exiting 0 twice; that reading was wrong. This is the documented
local-1.20 / CI-1.19 divergence, so it is NOT reformatted — see the standing rule never
to run a bare `mix format` here.

_(superseded queue entry below, kept for the reasoning)_
**Was queued on the user's instruction 2026-08-31 (session 4): `retire-cluster-snapshot-remnants`.**
KI-81 is DECIDED — **delete both**, the user's call, taken after a code review that
corrected the entry on four points. It goes FIRST, ahead of CI.

The decisive fact is not a judgement: `openspec/specs/controller-packaging/spec.md`
says of the snapshot branch *"is removed once `auto-restore-on-boot` lands and
`cluster-snapshot-restore` is deleted"* — **both happened in `9235bcde`, 2026-05-09.**
The spec scheduled its own deletion and the trigger fired three and a half months ago.

Corrections to the KI-81 entry, all verified at the source:
1. The k8s preStop does **not** run `snapshot-on-stop.sh` — `flezha-daemonset.yaml:179`
   runs `flz controller stop`. Three comments in the tree still claim otherwise.
2. The default shutdown path never invokes it either (`controller-entrypoint.sh:122`
   says so explicitly). It runs ONLY under `FLEZHA_GRACEFUL_SHUTDOWN_ENABLED=false`,
   so "every graceful shutdown fails" — this board's own words — was wrong.
3. `docs/operations/deployment.md:203`–`:211` **already documents the whole defect
   accurately**, crashloop and workaround included. The docs were right; the code and
   the spec are the stale halves.
4. The upgrade-crashloop I priced as the reason to REPOINT rather than delete is not
   reachable: the verbs existed 2026-04-25 → 2026-05-09, a two-week window, and
   `9235bcde` introduced the durable registry in the SAME commit — so a node holding a
   `latest.flz` predates the registry entirely and would have crashlooped on its first
   upgrade past May. My repoint recommendation is retired.

Scope (pure deletion, ~half a day — the spec delta is why it is not an hour):
`snapshot-on-stop.sh`; the entrypoint's restore branch and every `FLEZHA_SNAPSHOT_*` /
`FLEZHA_RESTORE_WAIT_*` knob; `Dockerfile.controller:125`'s COPY/chmod;
`flz boot import-legacy-snapshot` **and** `CliManager.boot_import_legacy_snapshot/2`
(the user's call — it reads a format nothing has written since May); the ConfigMap's
`FLEZHA_SNAPSHOT_DIR` and its **dead `FLEZHA_DATA_DIR`** (set at `:38`, read by NOTHING
in `lib/`, `cli/`, `scripts/` or `docker/`, while `flezha-daemonset.yaml:197` tells
operators to match it); `test/scripts/durability_posture.sh:141`; the three false
preStop comments; and docs at `deployment.md` 195–211 + 413, `docker/prod/README.md:44`,
`kubernetes/README.md:129`, `docs/internals/process_hub.md`, `README.md`.

**Spec delta required** on `controller-packaging`: requirement point 3, point 4's
disabled branch, the `snapshot-on-stop.sh` paragraph, and the *"Cold start with no
snapshot file"* scenario. A `## MODIFIED Requirements` block REPLACES the whole
requirement — carry every surviving scenario across or they are deleted silently.

**Ruled and NOT in scope**: the prod boot refusal on an unset
`FLEZHA_DURABILITY_BACKEND` STAYS. The user agreed. A data platform makes the operator
state a durability posture once; `posture()` already surfaces `:local_only` on
`/healthz` and `flz controller status` so an audit can find such a node.

**Queued and DECIDED 2026-08-31 (session 4): `rename-kafka-queue-overflow`.**
The user ruled: **plain rename, no accept-both, no deprecation window** — asked twice
whether a real reason existed to keep both spellings and told to proceed. So
`:block` → `:drop_nothing` in `kafka.ex` (the option docs at `:31`, the default at `:70`,
the coercion at `:389`–`:394`, the guard at `:398`–`:399`), `producer_worker.ex`'s
`overflow/4` clauses and its `:95`/`:419` comments, `docs/configuration.md:582`.

**The one exposure, priced and accepted**: plugin opts are written to the DURABLE cspec
(`plugin_manager.ex:384`, `Plugin.strip_transient/1`), so a stored row containing
`queue_overflow: :block` stops matching the guard and that plugin fails to enable on the
next boot. Small because `:block` is the DEFAULT — a row only carries it if an operator
typed the value already in force. Remedy if hit is re-enabling that plugin. I offered a
one-line read-side `"block" -> {:ok, :drop_nothing}` migration clause; the user declined
it. Do not add one back without asking.

**Also widen `scripts/check-overflow-naming.sh:47`** — it matches
`:block\s*[,|]\s*:drop_(newest|oldest)` and so requires ADJACENCY, which is exactly why
it passed over the text it exists to catch. The gate missing this is part of the defect.

**Queued and DECIDED 2026-09-01: `collapse-observability-source-behaviours`.** KI-100,
shape chosen by me on the user's "delete or collapse — you choose". **Collapse.**
`OtpBackend.Source` declares `init_state/1` + `poll/1` and its moduledoc calls itself
*"shared GenServer plumbing"* — it is a POLLER, and calling it `Source` is what made the
real contract look redundant for months. Rename it `OtpBackend.Poller`; its macro also
declares `@behaviour Flezha.Observability.Source` with an overridable default
`config_schema/0`. Deletion refused on a measurement: three live registrations are
`flavour: :declarative` and the template cannot describe them (`use GenServer`).
Touches `openspec/specs/observability-otp-backend/spec.md:5`, whose sentence about the
behaviour is already false, and `registry.ex:61` + `docs/operations/observability.md:261`.

**Filed 2026-09-01: KI-104 — `flezha_ui` is built on controller internals.** The user
asked whether the UI used the module being renamed; it does not, but the check found it
names ~30 internal `Flezha.*` modules over `erpc` and **two are already broken by this
programme** (`State.Registry` → `State.Paths`, `Service.Identificator` →
`Identificator`), four more gone with no successor and untraced. They fail at RUNTIME —
`flezha_ui` has an `{:error, {:api_missing, …}}` path — so no build anywhere reports
them and six accumulated unseen. **The user ruled the programme does NOT slow down for
this and `flezha_ui` is updated after the refactor**; what is owed is a **public API
surface** so the UI stops consuming internals. Not queued as work yet — it is the
user's design call how big that surface is.

**Queued on the user's instruction 2026-08-31 (session 4): fix CI.** See *CI is red*
above. Only two of the four causes are work on this branch — the naming gate (cause C,
red on `dev`, minutes) and the Ruby SDK unit tests (cause D, reproduce first). It is
queued **after** the area 7 survey returns, because one worker at a time in a shared
tree means a new worker cannot start over another's uncommitted edits; the survey
writes no code, so the boundary comes as soon as it reports.


Two phases, in this order. **Phase 1 first** — area 4 refactors `sdk/java`
with zero behaviour change, and KI-33 is a live correctness defect in exactly
that code. Refactoring over it would carry the bug into the new shape and cost
the A/B its meaning.

### Phase 1 — the twelve actionable defects

Every entry below carries a `**Where**` with file:line, an `**Evidence**`
block with the command and its output, and an `**A fix must**` clause. Read the
entry before briefing a worker; do not re-derive what triage already measured.

| # | Change | Entries | Why grouped |
|---|---|---|---|
| 1 | ~~`fix-java-workflow-resume-duplication`~~ → `java-diamond-resume` | KI-33 (P1) | **DONE** — `4de341b5` + `f2c7868d`, archived. Its filed diagnosis was refuted in both halves: nothing was re-run (one `proc_committed` per proc at `iter=0`) and it is not a resume defect at all (the same 9 payloads arrive with no crash). The mechanism was `collectEdgeOutputs` emitting one `Edge` output per CONSUMER — right for a stream, wrong for a workflow stage. Java e2e 17/21 → 21/21. **Area 4 is unblocked.** |
| 2 | ~~`fix-control-unit-abnormal-exit`~~ → `finish-the-suite-log-gate` | KI-48, KI-49, KI-50 | **DONE** — `21185253` + the close-out commit. Absorbed all three: they were believed to be the only sources keeping the uncaptured-log gate from reporting. They were not — see rows 7 and 8. |
| 3 | `close-prometheus-label-coercion` | KI-45 (P1), KI-59 | **DONE** — `55f32234` + close-out. KI-46 deferred by name (`close-otlp-typed-attributes`): typing the values would have destroyed this change's only control. |
| 3.5 | `fix-opaque-module-names` | KI-10 | **DONE** — `0cb58166`, `055054f2`, `1848cb80`, `14c7cfc5`, archived. credo 1463 → 378; `credo_warnings` ratchets in all 36 areas for the first time; `--check` exit 0. Fixed KI-63 on the way; filed KI-61/62/64/65. 3 renames, 4 folds, 260 keeps; one change in three ordered commits. The survey refuted this board's sequencing claim by an order of magnitude — see the detail block below. |
| 4 | `green-the-test-hygiene-tail` | KI-18, KI-21, KI-23, **KI-54** | **DONE** — `656fd444`, `9820f75d`, `5946464e`, archived. Four of its own entries were refuted or already fixed. Also closed KI-66 and KI-67; filed KI-68 and KI-69. |
| 5+6 | `correct-runtime-and-conformance-docs` | KI-13, KI-51 | **DONE** — `e4f919c8`, archived. Found a second stale list the entry never named, and fixed it as one list rather than a corrected copy. **MERGED and re-scoped 2026-08-31.** KI-13's own triage says it is **not a defect in this repository** — there is no repo code to change, only a `docs/sdk/conformance.md` line recording the shim's `docker run` (with `--network host` and `-i`). KI-51 is a one-line help string. Together they are one small change, not two rows. |
| ~~6~~ | — | KI-52 | **NOT WORK — escalated** to *Awaiting the user's decision*. It is a distribution-shape call ("does this block going public"), not a defect: nothing is broken and there is no reproduction to run. Its other half, KI-51, moved to row 5+6. |
| 6.5 | `close-transport-send-buffer-loss` | **KI-3, KI-56, KI-70, KI-71, KI-73** | **DONE** — `269072b9`, `443903f9`, `3e22f480`, archived. Closed five P1s; KI-73 was found by the test written to prove the work, and held the row open until nothing dropped. **Queued 2026-08-30 by the user; proposal WRITTEN and `--strict` valid.** One watermark gate replaces UDS's silent drop and gives QUIC the bound it never had; the sender parks a BEAM process, never a scheduler thread. Four knobs (both watermarks, the gate's fallback interval, credit's `PARK_TIMEOUT`). Does NOT replace the credit system — see the KI-3 entry above for why they compose. |
| 7 | `close-input-log-syncer-blindness` | **KI-53 (P1)** | **DONE** — `81fc0db2`, archived. The survey found a second, unfiled half: `sync_now/1`'s bare `receive` wedged the log permanently. Both halves red-first with separated controls. |
| 8 | — | the gate itself | **DONE 2026-08-31.** Proved red (a scratch uncaptured `Logger.error` gives `mix test` exit 0 with 1 gate hit), then **two full runs at different seeds, both 1872 passed with zero hits**. The merge gate can report for the first time. |

**Do not touch, and do not let a worker "helpfully" fix:**

- **The user decides**: KI-3, KI-10. Put options and a recommendation under
  *Awaiting the user's decision*; never pick for them.
- **Deferred with a reason already recorded**: KI-41 (folds into
  `refactor-5a-state`), KI-36 (needs a soak under real load), KI-34 (needs the
  Kafka stack), KI-14 (rides the next PHP image change), KI-6 (a Python SDK
  source decision), KI-15 (impossible as filed, and says so).
- **Notes, not work**: KI-25, KI-29, KI-30, KI-31.

### Row 3.5 — `fix-opaque-module-names`, in detail

**SURVEYED 2026-08-31. The proposal is written and `--strict` valid**
(`openspec/changes/fix-opaque-module-names/` — 817-line `design.md` carrying
the 267-row table, `proposal.md`, `tasks.md`, a `code-quality-baseline` delta).
That directory is gitignored, so it exists on this machine only and appears in
no diff. **It has not been supervisor-reviewed** — the next session reviews it
before implementing.

**The survey refuted this board's own analysis. The corrections are the point,
so they are recorded before the plan.**

- **The sequencing claim was wrong by an order of magnitude.** This board said
  the naming pass was a prerequisite for the alias pass because "for a large
  fraction of these the last segment is unacceptable". Measured: **51 of 653
  sites (7.8 %) over 8 modules, and 0 of 653 mechanically blocked.** The cause
  is that credo's `AliasUsage` ships `excluded_lastnames` already covering
  `Registry`, `Stream`, `Supervisor`, `IO`, `Task`, `Application` — **credo
  enforces rule 2 for us and never asks for those renames.**
<!-- module-naming-record:begin -->
- **"The tree uses Elixir's own `Registry` by bare name in 7 places, so
  `Flezha.State.Registry` cannot be aliased there" was wrong.** 7 occurrences in
  5 files, and **none of them references `Flezha.State.Registry`**. Zero actual
  conflict.
<!-- module-naming-record:end -->
- **The collision list was incomplete**: 19 segments over 47 modules, not 13
  over 35 — `State`×3 and `Postgres`/`Extension`/`Counters`/`Codec`/`Backoff`×2
  were missed.
- Counts: `ParenthesesOnZeroArityDefs` **431** not 430; KI-10's total **1463**
  not 1461; the residue to ratchet **379** not 378. Correct as filed: **259**
  modules, **653** `AliasUsage` sites.

**The durable question is settled: YES, and it does not block.** An Elixir module
name IS written verbatim into durable state — `operative_manager.ex:789` puts
`Flezha.Operative.Overwatch` in the cspec `start` MFA, and the stored term
carries `%Builder{}` and `%Plugin{}` struct tags, landing in
`<cspec_root>/<hub_id>/registry.dets`. **13 modules are pinned** (those three
plus the ten providers in `config/config.exs:15`–`:31`). None of them is a
rename candidate, so **no rename here is a migration and no `flz reset` is
needed.** Every other durable surface was opened and is clean. Honesty note: no
local `.dets` holds an `Elixir.Flezha` atom today, so the verdict rests on the
read and write paths, not on a live sample. The tree already half-knows —
`declared_manifest.ex:75` decodes WITHOUT `[:safe]` where `manifest.ex:111`
decodes with it, and a failure is silent (`extension/table.ex:177` logs a
warning and still reports `:running`).

**The plan: 3 renames, 4 folds, 260 keeps, one change in three ordered commits.**
Renames: `State.Registry`→`State.Paths` (it is path resolution, 601 LOC so
folding is impossible, and `state/supervisor.ex:148` writes a parenthetical to
disambiguate it today), `AuthZ.Store`→`AuthZ.DetsStore`, `SDK.Backoff`→
`SDK.Jitter`. Folds: `Service.Parser`→`Identificator`,
`Plugin.Provider.Metrics`→`State.Metrics`, and the two `*.Telemetry` modules
into their parents. 24 further modules take a written-down `as:` rule instead.

Order — renames, then aliases, then `.credo.exs` + ratchet — because 8 of the 51
`as:` decisions change if the rename lands after, and because commit 1's blast
radius reaches `openspec/specs` (14 refs for `State.Registry` alone) and `docs/`
where the compiler is blind; burying that under ~650 alias lines makes the risky
half unreviewable. The ratchet target is 379 but must be RECORDED from a
measured post-pass run, never the arithmetic — ~650 new aliases raise
`AliasOrder` unless inserted alphabetically.

**Two items the survey flagged for the user, deliberately not fixed:**
`Plugin.Provider.Metrics` is **dead** (zero callers for either public function,
and its moduledoc's `flz state list` claim is false), and `AuthZ.DetsStore` is
shared code in the wrong layer (`Schedule.Store` depends on it).

**New finding**: the tree already answers the ambiguity two ways at once —
`as: ObsRegistry` in 7 files, `as: StateRegistry` in 1, bare in 11. One
question, two answers.

### Phase 2 — the refactor resumes

1. **Area 4** (`sdk/java`, 8 441 lines). Its dependency, area 1, is done. Run
   the survey first, as every area does. `sdk/java` changed on 2026-08-30
   outside the programme (native profiles, `.gitignore`, CI moved the
   five-triple cross-build to release tags, seven artefacts untracked) — none
   of it is refactoring, the survey reads the current tree, and the worker must
   **not** re-track natives or jars (`scripts/check-build-artifacts.sh`
   refuses it).
2. Areas 5a–5e, 6, 7, 8 after it, in dependency order.

### What the defect programme leaves behind

`docs/known_issues.md` is now the durable record, and every entry carries a
command that reproduces it and that command's output — which none of the
original forty had. **Twenty** entries stay genuinely open, and they split
three ways:

- **Two are the user's decision, not work**: KI-3 (what a caller is told when
  its frame was discarded) and KI-10 (Credo — disable two checks, or edit 1 083
  sites).
- **Six are deferred with a reason**: KI-41 folds into `refactor-5a-state`,
  which restructures that seam anyway; KI-36 needs a soak under real load;
  KI-34 needs the Kafka stack; KI-14 rides the next PHP image change; KI-6 is a
  Python SDK source decision; KI-15 is technically impossible as filed and says
  so.
- **Five were found during the programme itself**, two of them P1: KI-48
  (`ControlUnit` exits abnormally whenever a runner tree comes down) and KI-49
  (a test leaks a retrying task onto the next test's stub). KI-33 is now
  confirmed and reproducible for the first time.

### Method notes worth keeping

Half the original list was not what it claimed: eleven entries were already
fixed or never true, five overstated their scope, and four **understated** it.
The rule that separated them is the one now in the spec — an entry carries a
reproduction, or it records a suspicion.

Charter §1's "record it, fix it in its own change" is what produced those forty
entries, and it is right. What it lacked was the requirement that the recording
be reproducible. The next area should record defects the same way, with the
command.

4. Plug the laptop in before any checkpoint: every A/B is refused on battery.
