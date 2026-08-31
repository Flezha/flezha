# refactor-5e-a-runtime — `lib/flezha/runner` + `lib/flezha/operative`

Two segments: batches 0–1 (`85f722cd`) and batches 2–5 (`8ba87462`).

## Batch 0.4 — the Mimic stub census, taken before anything moved

Risk D7.1: `Mimic` intercepts **remote** calls, so moving a public function to a
new module silently changes which calls a stub reaches. The census below is the
complete set of stubs naming a module in scope, taken at `cbfb83a1` with

```
grep -rn 'stub(\|expect(\|copy(' test/ | grep -E 'FleetManager|StreamSup|StreamChannel|\
ControlChannel|ControlUnit|Lifecycle|Overwatch|Builder|State.Supervisor'
```

| stubbed | function | sites | batch 1 verdict |
|---|---|---|---|
| `Flezha.Runner.FleetManager` | `:connect_handler` | `transport/server_close_on_reject_test.exs:37`,`:54`; `transport/server_stream_routing_test.exs:29`,`:47` | **safe** — `connect_handler/4` did not move |
| `Flezha.Runner.FleetManager` | `:runners_for_factory` | `helpers/fake_start_registry.ex:144`; `service/cli_manager_schedule_test.exs` ×9; `service/operative_manager_spawn_test.exs:106` | **safe** — public reader, did not move |
| `Flezha.Runner.FleetManager` | `:active_runners` | `helpers/fake_start_registry.ex:145`; `operative/partition_group_test.exs:74` | **safe** — did not move |
| `Flezha.Runner.StreamChannel` | `:io_in` | `operative/overwatch_confirming_ingest_test.exs:47`; `state/input_log_test.exs:206`,`:279` | **safe** — `io_in/3` stays on the parent (C2, batch 2) |
| `Flezha.Runner.ControlChannel` | `:send_control` | `state/recovery_test.exs:23`; `state/compensation_markers_test.exs:85`; `runner/control_channel_proc_commit_test.exs:404` | **safe** — no module move in batch 1 |
| `Flezha.Runner.ControlChannel` | `:send_control_async` | `runner/stream_channel_credit_test.exs:70`; `state/wal_writer_credit_test.exs:27` | **safe** |
| `Flezha.Runner.ControlUnit` | `:stop_operative_async` | `operative/overwatch_suspension_test.exs:95`,`:119`,`:146`,`:279`,`:428` | **safe** |
| `Flezha.Operative.PartitionGroup` | `:members`, `:start` | `plugin/provider/operative_forward_test.exs` ×8; `service/plugin_manager_test.exs:251`; `service/cli_manager_partition_test.exs` ×7 | **safe** |
| `Flezha.Operative.ClassRegistry` | `:decode_declarations` | `system/sdk_smoke_e2e_test.exs:280` | **safe** — E7 moves other functions |
| `Flezha.State.Supervisor` | `:live_writers` | `operative/overwatch_suspension_test.exs:386`,`:412` | **BROKEN BY E5** — re-pointed to `Flezha.State.WriterRegistry` in the same commit, with a new `Mimic.copy/1` in `test/test_helper.exs` |
| `Flezha.State.Supervisor` | `:find_or_start` | `operative/restore_refusal_test.exs:106` | **safe** |

`Mimic.copy/1` list touched: exactly one line added,
`Mimic.copy(Flezha.State.WriterRegistry)`, beside the existing
`Mimic.copy(Flezha.State.Supervisor)` (which stays — `:find_or_start` still
uses it).

**The census also found the non-Mimic callers the design missed**, which are the
same class of risk one level down: `test/operative/kind_test.exs:58` called
`Builder.validate_operative/2` directly (E3's "single caller" is a production
statement), and `test/flezha/state/wal_writer_test.exs:493`/`:501` still call
`State.Supervisor.live_writers/1`, so E5 does **not** leave `supervisor.ex:121`
as the delegation's last caller.

## Batch 1 — the edge ledger, measured

Instrument: `mix xref graph --format dot` + an iterative Tarjan, reproducing the
ruler (**62 and 4**) before being used, exactly as the survey's. The ledger below
is a `comm -23`/`comm -13` on the two edge sets, not a claim.

| # | designed | measured |
|---|---|---|
| E1a | `runner/control_channel.ex → runner/fleet_manager.ex` | **NOT CUT** |
| E1b | `runner/stream_channel.ex → runner/fleet_manager.ex` | cut |
| E1c | `state/wal_writer/epoch.ex → runner/fleet_manager.ex` | cut |
| E2a | `runner/stream_acceptor.ex → runner/stream_sup.ex` | cut |
| E2b | `runner/fleet_manager.ex → runner/stream_sup.ex` | cut |
| E2c | `operative/lifecycle.ex → runner/stream_sup.ex` | cut |
| E2d | `transport/server.ex → runner/stream_sup.ex` | cut |
| — | `runner/stream_sup.ex → flezha/config.ex` | cut (unplanned; `Config` left with `stream_ids/2`) |
| E3 | `operative/builder.ex → service/validator.ex` | cut |
| E4 | `runner/fleet_supervisor.ex → runner/runner_supervisor.ex` | cut |
| E5 | `operative/handover.ex → state/supervisor.ex` | cut (re-pointed to `state/writer_registry.ex`) |

**Ten of eleven. E1a is refuted, and the refutation is in the grep.** The design
verified it with `grep -n 'FleetManager\.' lib/flezha/runner/control_channel.ex`
→ one call site. That pattern cannot see a module used as a **process name**:
`control_channel.ex:312` is `GenServer.call(FleetManager, {:runner_disconnected, …})`
and `:674` is `GenServer.cast(FleetManager, {:sync_factories, …})`. Both are real
xref edges. E1 therefore cuts **two** edges into `fleet_manager.ex`, not three —
which changes no metric, because E1 was already predicted to change none.

Reading a module-move's edge set with `Mod\.` misses every `GenServer.call(Mod, …)`,
`Supervisor.start_child(Mod, …)`, `:sys.get_state(Mod)` and `Mimic.copy(Mod)`. The
graph is the instrument; the grep is a hint.

### Membership

| stage | repo `|SCC|` | `runner` mcf | `operative` mcf |
|---|---|---|---|
| `cbfb83a1` | 62 | 9 | 9 |
| after E1–E5 | **59** | **7** | **8** |

Read attributably: the peer lane committed at `7cb37b03` and the tree then held
only this work. The 62-member set lost **exactly three** files and gained none —
`comm -23` over the two membership lists returns `operative/builder.ex`,
`runner/fleet_supervisor.ex`, `runner/stream_acceptor.ex` and nothing else. The
repo's separate 4-member `sdk` cycle disappearing over the same window is 5e-b's
A2, not this work.

Left the cycle: `runner/fleet_supervisor.ex`, `runner/stream_acceptor.ex`,
`operative/builder.ex` — the three predicted for this batch.
`operative/class_registry.ex` leaves with E7 (batch 3).

New siblings, alias list intersected with the 59-member SCC:
`fleet_manager/marks.ex` has **no out-edge at all**; `stream_pool.ex` reaches
`flezha/config.ex` and `flezha/identificator.ex`, neither in the SCC. Both
**SCC-free**, as designed.

## Where the plan was wrong, marked in place

- **`prune_marks/1`** (tasks 1.1) is `prune_marks/0`. It became `Marks.prune/0`:
  `Marks.prune_marks()` stutters, and §4 wants a verb on a noun module.
- **"`operative_manager.ex` … already aliases `Validator`"** (tasks 1.4) is false.
  It does not, and did not; the alias is part of E3's cost.
- **"`supervisor.ex:121` is its last caller"** (tasks 1.6) is false —
  `test/flezha/state/wal_writer_test.exs:493`/`:501` call
  `State.Supervisor.live_writers/1` too. The delegation is still deletable, but
  deleting it is three edits, not one, and both are 5a's.

## Two decisions that departed from the plan text, and why

- **E3 lands in `Service.OperativeManager.Admission`, not in `OperativeManager`.**
  Every other clause of `do_start_operative/2`'s `with` is already
  `Admission.*` — `check_not_shutting_down`, `refuse_group_base_claim`,
  `validate_factory`, `check_operative_quota`, `check_entity_quota`,
  `check_disk_quota`. `validate_operative/2` is an admission check and
  `Admission` is the module that owns admission checks (§2). Same eleven lines
  either way; a better home, and `do_start_operative/2` stays at 46 lines rather
  than 49 against a **blocking** `functions_over_50` of 0, which is what an
  inline of the four `Validator` clauses would have cost.
- **E4 keeps one `stop_runner_sup/1`, private to `FleetManager`.** Inlining
  `Supervisor.terminate_child/2` + `delete_child/2` at all three call sites is
  three copies of a two-line teardown — §2's "two functions that do the same
  thing are one function with a parameter" applies to three copies of one.
  `start_runner_sup/4` has a single call site and its spec is inlined there, so
  `FleetSupervisor` sheds the `RunnerSupervisor` name either way.

## E3's cross-area arithmetic — `lib/flezha/service` is blocking

`--check` reports `BLOCKING lib/flezha/service prod_loc: 5817 -> 5828`, and it is
this change's. §11's condition, accounted by cause:

| area | before | after | delta | cause |
|---|---|---|---|---|
| `lib/flezha/service` | 5 817 | 5 828 | **+11** | `admission.ex`: `validate_operative/2` (10) + `alias Flezha.Service.Validator` (1) |
| `lib/flezha/operative` | 2 453 | 2 416 | **−37** | `builder.ex` 49 → 11: `validate_operative/2`, `from_json/1`, `from_map/1`, `get_from_map/2`, the `Validator` alias; `handover.ex` +1 (the `WriterRegistry` alias) |

**Net −26 across the pair, and it is a relocation, not new code.** The moved
function is byte-identical: `git show refactor-baseline-5e-a:lib/flezha/operative/builder.ex`
sliced at `validate_operative` and the same slice of `admission.ex` `diff` clean,
10 lines each. The eleventh line is the `alias` the body needs.

The proposal did not notice that `service` is a blocking area — it named `state`
(E7) as the only cross-area accounting item.

## `lib/flezha/runner`'s own count, and the instrument's blind spot

`refactor_metrics.py` reads `runner` `prod_loc` **2 646 → 2 540**, and that number
is wrong by construction: `fleet_manager/marks.ex` (66) and `stream_pool.ex` (36)
are **untracked**, and §11e's third silence makes untracked files invisible in the
flattering direction. The true reading is **2 642, −4** — the two new files must
be `git add -N`'d before any number off this area is evidence. *(That is the
figure at the end of batch 1 only; the area closes at 2 679. See the close-out
record.)*

The blind spot recurred at every later checkpoint and every figure in the
`Batches 2–5` section below was hand-computed around it. **All of them were
re-read from the instrument at `8ba87462` once the files were tracked, and one
came back different** — the close-out records which.


# Batches 2–5

## The three splits, re-verified against the tree as it is

Instrument: an intra-file call graph that blanks heredocs, strips line comments,
strings and sigils, and attributes an `@doc`/`@spec`/`@impl` block to the
function that FOLLOWS it. **It had three bugs of its own, each found by the tree
refusing the result, and each is a rule worth keeping:**

1. **`?` is a char literal only at the start of a token.** Treating every `?`
   as one ate the `?(` of `runner_active?(`, so a predicate call was invisible.
   `handle_call -> runner_active?` was missing from the first C1 graph. The same
   error in a shell is what made `grep -n 'runner_active?\b'` return nothing:
   `\b` after `?` never matches.
2. **A multi-line `@spec` continuation breaks a backward walk.** `bind/3`'s
   `@spec` wraps, so its `@doc` was attributed to `rewind/2` above it and would
   have moved with it — producing exactly the orphan `@doc` KI-102 records
   elsewhere in this area. Caught because the extractor also rewrote the stray
   `@spec bind(` into `@spec Lifecycle.bind(`, which does not compile.
3. **The module-closing `end` belongs to no function.** The last span reaches
   EOF, so the first C1 draft moved the module's `end` into `Session` and left
   the parent unterminated.

After the repairs, both earlier splits were re-derived and diffed against what
had been installed: `Roster`, `Factories`, `Session` and `Records` came out
**byte-identical**, and an orphan-`@doc` sweep over all six new files finds none.

| split | verdict | cross-group edges |
|---|---|---|
| **C1** `fleet_manager.ex` | **ACYCLIC** | `Factories→Roster`, `Session→{Factories,Roster}`, `_PARENT→{Factories,Roster,Session}` |
| **C2** `stream_channel.ex` | **ACYCLIC** | `_PARENT→Gate`, `_PARENT→Records` only; neither sibling names the parent |
| **C3** `lifecycle.ex` | **ACYCLIC** | `Migration→_PARENT` only (4 edges), `_PARENT→Migration` none |

`files_over_800` is **0 / 0**. Largest survivors: `overwatch.ex` 765,
`class_registry.ex` 627, `control_channel.ex` 710.

## Two module-scoped mechanisms a split silently re-keys

Batch 0's Mimic census covered `Mimic`. It should have covered **every mechanism
keyed on a module name**, because two more fired here and both were caught by a
red test rather than by the census.

- **`Logger.put_module_level/2`.** `fleet_manager_sdk_lang_test.exs` and
  `fleet_manager_sync_factories_test.exs` raise the level of
  `Flezha.Runner.FleetManager` alone (`config/test.exs` pins the primary logger
  to `:error`). `establish_lang/4` and `warn_dropped_factories/4` moved to
  `FleetManager.Factories`, so three assertions read `left: ""`. Both overrides
  now name **both** modules.
- **`:persistent_term` under `__MODULE__`.** `publish_stream/3` writes
  `{__MODULE__, name}` and `io_in/3` reads it. Moving `publish_stream` into
  `Gate` changed the key while the reader kept the old one: **11 tests red**,
  every one of them "the stream is unbound". `publish_stream/3` went back to the
  parent — it is the writer of the parent's own key and had no Gate caller — and
  `Gate` is left naming no module of ours at all.

The general rule: **a module move re-keys anything spelled `__MODULE__` and
anything a test scopes by module name.** Grep for both before moving code, not
just for `stub(`. This is now **charter §11i** (`fc9220b8`), which lists the key
positions to sweep: `:persistent_term`, `:ets` table names, registry keys,
`Logger.put_module_level`, `:telemetry` event prefixes, and the module's own
name inside test setup.

## E7 — the design's home was refuted by measurement

E7 was designed as `announce_derived_retention/2` + `emit_retention_derived/4`
→ `State.Retention` and `establish_sdk_lang_from_state/3` → `State.Restore`.
The second half is right. **The first half, measured, moves cycle membership
rather than cutting an edge** — §11f's own failure mode:

| landing | `operative` mcf | `state` mcf |
|---|---|---|
| HEAD | 9 | 23 |
| all three → `Retention` (as designed) | **8** | **24** ✗ |
| all three → `PersistenceResolver` | **8** | **23** ✓ |

`lifecycle.ex` is inside the SCC and calls the announcement, so whichever module
hosts it gains an in-cycle caller; `Retention` also reaches `wal_writer`,
`writer_registry` and `input_log/sweep`, so it joins the cycle and drags the
sweep in with it. `PersistenceResolver` already owns two of the four values the
function reads (`compact_after_age_ms/1`, `configured_compact_after_age_ms/1`),
is reached from the SCC only through this new caller, and reaches back only
through `input_log.ex`, which is in the cycle already. So it costs **nothing**
and is the better §2 home besides.

**What E7 actually cut**: `class_registry.ex → input_log.ex`. That pair was
mutual — `InputLog.hold_window_ms/1` calls `ClassRegistry.correction_window_ms/1`
— so it was a two-cycle on its own, and cutting the outbound half is what takes
`class_registry.ex` out of the SCC.

**Refuted**: the design's "`fleet_manager/factories.ex` never enters, because
with E7 its only in-cycle dependency is gone". Its in-cycle dependency is
`Flezha.Service.HubManager`, not `ClassRegistry`. `Factories` is **in** the SCC.

**E7's arithmetic, isolated**, measured per file with the metric's own classifier:

| file | area | prod_loc |
|---|---|---|
| `operative/class_registry.ex` | operative | 359 → 312 (**−47**) |
| `state/persistence_resolver.ex` | state | 183 → 217 (**+34**) |
| `state/restore.ex` | state | 96 → 109 (**+13**) |
| `state/retention.ex` | state | 149 → 149 (0) |

`state` **+47**, `operative` **−47** — exactly the lines moved, before the one
`alias Flezha.State.Restore` the caller now needs (**+1** to `operative`). Net
across the pair is **+1**, and no part of E3's −26 headroom was spent on it.

## The area's line accounting, by cause

| area | base | now | Δ |
|---|---|---|---|
| `lib/flezha/runner` | 2 646 | 2 679 | **+33** |
| `lib/flezha/operative` | 2 453 | 2 387 | **−66** |
| **the area** | **5 099** | **5 066** | **−33** |

`runner`'s +33 is §1's admitted case — "a §7 ceiling, whose extractions each
cost a signature and a brace" — and it is exactly that, line for line:
`fleet_manager` −540 against four siblings +571 (**+31**), `stream_channel` −336
against two siblings +350 (**+14**), `stream_sup` −31 against `stream_pool` +36
(**+5**), the `FleetSupervisor` deletions **−23**, `control_channel` **+6**,
`runner_supervisor` **+1**, `commits` **−1**.

## The test fold: 44 → 20 files, 286 → 286 tests

**The merge is a concatenation, not a semantic merge, and that is deliberate.**
The source modules carry different module-level `setup` blocks, different
`@moduletag`s and colliding `@namespace` attributes; unifying them means moving
each `setup` into a `describe`, and a `setup` that silently stops running is a
test that passes while checking nothing. Each source `defmodule` therefore
survives intact inside its target file, so the fold is behaviour-preserving **by
construction**. A target with one source is renamed to match its file.

The consequence, stated rather than hidden: **`test_loc` does not fall** — a
concatenation preserves every line. Task 4.3's "and that `test_loc` fell" is
false as written and is marked so in place.

Measured **by the instrument, after the files were tracked** — the hand count
taken while they were untracked (§11e) got one row wrong and is corrected here,
which is §11f's own instruction not to read the share off a page:

| row | before | after |
|---|---|---|
| `test_files` | 228 | **205** (204 mine + one 5e-b added in the window) |
| `async_false_share` | 0.794 | **0.790** — an improvement, and NOT the 0.7745 I first computed by dividing a filesystem-wide `async: true` count by `test_files` |
| `files_over_800` | 6 | 6 (largest target 684) |
| `test_loc` | 38 662 | 38 745 (+83, all of it 5e-b's new file) |
| `duplicate_pairs` (narrow) | 105 | 101 |
| `narrative_markers` / `refutes` / `sleeps` | 37 / 16 / 3 | unchanged |

**§11f's floor is met exactly, not with room to spare.** After folding to 205
test files the area must hold `async: true ≥ ⌈0.206 × 205⌉ = 43`, and it holds
**43**. The next area that folds any of these files has **zero headroom** and
must fund its fold by converting a target, naming the global it does not need.

## The A/B — row 1 reclassified `direction`, per `design.md` D6's own rule

D6 pre-committed: "If row 1's measured floor comes out above ~1 %, it is
reclassified `direction` and the arm sha is the evidence." Three facts settle it
without spending the machine:

1. The effect is **one additional remote call per frame on each of two paths**,
   verified structurally: `io_in/3` makes exactly two cross-module calls
   (`Gate.send_held/6` and `Gate.undeliverable/2`) which are mutually exclusive
   branches; `handle_info` reaches `Records` once per frame because a frame
   matches one clause; every callee of `Gate.send_held/6` and of
   `Records.handle_entity_state_change/2` is local to its own module; and
   **neither sibling names the parent at all**, so no crossing multiplies. No
   call is added or removed — only which module holds it.
2. `bench.roundtrip` needs a live Kafka broker and a Rust runner build; it is
   the heaviest instrument in the programme, and every Elixir floor this
   programme has measured is 1–12 %. A floor of 1 % cannot resolve an effect of
   nanoseconds per frame.
3. §11d: the peer lane was dirty and compiling throughout the close-out, so any
   arm taken now is unattributable anyway.

Arms recorded in place of the numbers: **A = `refactor-baseline-5e-a`
(`cbfb83a1`)**, **B = the commit carrying this segment**. Rows 2 and 3 are
`direction` by declaration; row 2's driver is [[KI-92]]'s own and was not run.

# Close-out record

Read by the instrument at `8ba87462` with both lanes' work in the tree. The three
peer-movable rows (`cycles`, `max_cycle_files`, `duplicate_pairs`, §11d) were read
while 5e-b held six dirty files, and are marked.

| row | `runner` | `operative` |
|---|---|---|
| `prod_loc` | 2 646 → **2 679** (+33) | 2 453 → **2 387** (−66) |
| `files` | 10 → 17 | 10 → 11 |
| `files_over_800` | 2 → **0** | 1 → **0** |
| `functions_over_50` | 3 → **0** | 1 → **0** |
| `long_names` | 2 → **0** | 0 → 0 |
| `narrative_markers` | 7 → **0** | 3 → **0** |
| `credo_warnings` | 25 → **0** | 2 → **0** |
| `doc_lines` | 260 → 433 | 861 → 770 |
| `comment_density` | 0.258 → 0.290 | 0.382 → 0.374 |
| `duplicate_pairs` (peer-movable) | see below | see below |
| `cycles` (peer-movable) | 1 | 1 |
| `max_cycle_files` (peer-movable) | 9 → **10** | 9 → **8** |

The area's `prod_loc` is **5 099 → 5 066, −33**. Of the lines that left, **47
were not deleted** — E7 moved them to `lib/flezha/state`, which is accounted
against `operative`'s fall above and shows as that area's own `+47`.

**`duplicate_pairs` is deliberately not given a before/after here.** The baseline
records 1 and 2 under the **full 36-area scan set**; a two-area `--area` run
reads 0 and 0. Those are answers to different questions (§11d), and differencing
them would be exactly the comparison `--check` refuses. What is true and
checkable: the full-scan `--check` at close reported **no `duplicate_pairs`
regression for either area**, and `runner`'s single recorded pair
(`ControlChannel.encode_request/2` vs `Sdk.Codec.encode_request/2`) was refused
on its merits and handed up as N2, not removed.

## The edge ledger — eleven cut, one refuted

Charter §11f: an area whose work touches a cycle reports **the edges it cut, by
name**, because `cycles` and `max_cycle_files` are file-count proxies for an edge
property. Verified by `comm` over the two `mix xref graph --format dot` edge sets,
not by grep.

Two counts, both true and easy to confuse: **batches 0–1 cut ten of the eleven
edges they designed** (E1a refuted), and **the area as a whole cut eleven** —
those ten, plus one nobody planned (E2e) and E7's.

| # | edge | result |
|---|---|---|
| E1a | `runner/control_channel.ex → runner/fleet_manager.ex` | **NOT CUT — refuted** |
| E1b | `runner/stream_channel.ex → runner/fleet_manager.ex` | cut |
| E1c | `state/wal_writer/epoch.ex → runner/fleet_manager.ex` | cut |
| E2a | `runner/stream_acceptor.ex → runner/stream_sup.ex` | cut |
| E2b | `runner/fleet_manager.ex → runner/stream_sup.ex` | cut |
| E2c | `operative/lifecycle.ex → runner/stream_sup.ex` | cut |
| E2d | `transport/server.ex → runner/stream_sup.ex` | cut |
| E2e | `runner/stream_sup.ex → flezha/config.ex` | cut (unplanned — `Config` left with `stream_ids/2`) |
| E3 | `operative/builder.ex → service/validator.ex` | cut |
| E4 | `runner/fleet_supervisor.ex → runner/runner_supervisor.ex` | cut |
| E5 | `operative/handover.ex → state/supervisor.ex` | cut (re-pointed to `state/writer_registry.ex`) |
| E7 | `operative/class_registry.ex → state/input_log.ex` | cut — **and this pair was mutual**, since `InputLog.hold_window_ms/1` calls `ClassRegistry.correction_window_ms/1`, so it was a two-cycle on its own |

**E1a is the entry worth keeping.** The design verified it with
`grep -n 'FleetManager\.' lib/flezha/runner/control_channel.ex` → one call site.
That pattern cannot see a module used as a **process name**: `:313` is
`GenServer.call(FleetManager, {:runner_disconnected, …})` and `:674` is
`GenServer.cast(FleetManager, {:sync_factories, …})`. E1 cuts **two** edges into
`fleet_manager.ex`, not three, and it moves **no metric at all** — which is
precisely why §11f asks for the ledger rather than the count. **A ledger has to
be true even where the metric cannot tell the difference.**

Reading a module move's edge set with `Mod\.` misses every
`GenServer.call(Mod, …)`, `Supervisor.start_child(Mod, …)`, `:sys.get_state(Mod)`
and `Mimic.copy(Mod)`. The graph is the instrument; the grep is a hint.

## `runner max_cycle_files` 9 → 10 — §11f in the open, with the arithmetic

The area cut ten edges, cleared three 800-line ceilings and took `operative` from
9 to 8 — and `runner`'s cycle row got **worse**. Both facts are true and the
second is the metric measuring the wrong thing. Every step measured on the real
graph:

| stage | repo `\|SCC\|` | `runner` mcf | `operative` mcf |
|---|---|---|---|
| `cbfb83a1` | 62 | 9 | 9 |
| after E1–E5 (batch 1) | **59** | **7** | **8** |
| after C1/C2/C3 (batch 2) | 63 | 10 | 9 |
| after E7 (batch 3) | **62** | **10** | **8** |

**Left the cycle — five files, every one by an edge that was cut:**
`runner/fleet_supervisor.ex` (E4), `runner/stream_acceptor.ex` (E2),
`operative/builder.ex` (E3), `operative/class_registry.ex` (E7), and
`state/persistence_resolver.ex` briefly, before E7's landing put it back.
A `comm -23` over the membership lists at `cbfb83a1` and after batch 1 returns
exactly the first three and **nothing else joined**.

**Joined — four files, every one because the 800-line ceiling made it a file:**
`runner/fleet_manager/session.ex`, `runner/fleet_manager/factories.ex`,
`runner/stream_channel/records.ex`, `operative/migration.ex`. **No edge was added
by any of them** — each sibling reaches exactly the collaborators its parent
already reached. This is 5a's `wal_writer` case a second time: one member becomes
four, and a file-count proxy rises while the edge set shrinks.

So `runner` is `9 − 2 + 3 = 10`, and the arithmetic is the answer to the row, not
an excuse for it. **`design.md`'s prediction of a flat 9 was wrong for one
reason**: it had `fleet_manager/factories.ex` never entering the cycle, on the
grounds that E7 removes its only in-cycle dependency. Measured, its in-cycle
dependency is **`Flezha.Service.HubManager`**, not `ClassRegistry`, and E7 does
not touch it. That is the one refuted membership claim.

The four new SCC-free siblings are the other half of the picture and the metric
never shows them: `fleet_manager/marks.ex` has **no out-edge at all**,
`fleet_manager/roster.ex` none, `runner/stream_pool.ex` reaches only
`config.ex` and `identificator.ex`, `stream_channel/gate.ex` only two transport
modules. Four of the seven new `runner` files are outside the cycle by
construction, and the rule that makes them so is checkable: **a sibling extracted
from a file inside the SCC joins it if and only if it calls an SCC member.**

## The two refused handovers, with their prices

- **`ClassRegistry` moving wholesale into `lib/flezha/state/`** (5a's hand-over).
  **Refused.** By §3's own order `state` sits *above* `operative`, so the nine
  `state → operative` edges are legal traffic today; moving the file turns three
  `operative → state` breaches internal and creates three new ones
  (`lifecycle`, `partition_group`, `replay` → `state`). **Net upward breaches:
  zero.** Price: **+359 production lines into a blocking area**, and a
  `max_cycle_files` change bought by relabelling rather than by cutting an edge —
  §11f's warning by name. E7 moved the **47 lines that are the actual cause**
  instead, and took `class_registry.ex` out of the cycle for them.
- **`Plugin.Provider.OperativeForward` moving into `operative/`** (5c's
  hand-over). **Refused.** It calls `Service.OperativeManager`, so today it is a
  `plugin → service` breach; moving it *down* to `operative` makes the same
  breach span one more layer, which is the wrong direction under §3. It is also
  one of nine providers under `plugin/provider/`, and moving one out while eight
  stay is §2's parallel-structure smell. Price: **+~500 lines and 3 files into
  `operative`, one breach made worse, zero edges cut.**

## The three refused deduplications, with their prices

- **`ControlChannel.encode_request/2` vs `Sdk.Codec.encode_request/2`** —
  byte-identical, 45 tokens, and `runner`'s only `duplicate_pairs` entry.
  Refused: pointing the controller at `Flezha.Sdk.Codec` makes it depend on the
  module set `scripts/check-sdk-boundary.sh` exists to keep extractable to hex,
  and the only other shared home, `Flezha.Runner.CommandsProto`, is a
  `use Protox` code-generation module. Price of taking it: **−6 lines, −1 pair.**
  Handed up as N2.
- **`operative/plugins.ex:94` vs `service/hub_manager/rows.ex:295`** — the same
  four-line `%Plugin{}` list normalisation twice. The right home is
  `Flezha.Plugin` (root, 5e-b's) or `Plugin.Extension.Table.bulk_subscribe/2`
  (5b's blocking area), both outside this lane. Price: **−4 lines, −1 pair in
  each of `operative` and `service`.** N3. *This pair is why `operative`'s narrow
  `duplicate_pairs` reads 0 with `--area lib/flezha/operative` alone and 1 when
  `lib/flezha/service` joins the scan — the scan-set effect §11d records, not a
  new clone.*
- **The `ensure_table` ETS idiom in SEVEN files** —
  `observability/ring_buffer_sink.ex`, `observability/export/prometheus_registry.ex`,
  `transport/liveness_counters.ex`, `operative/class_registry.ex`,
  `state/metrics.ex`, `state/durability_tier/shipper.ex`, and now
  `runner/fleet_manager/marks.ex`. A single `ensure/2` in the root layer is §2's
  answer and it touches five areas, three of them blocking. Refused here; the
  count is the evidence. N4.

## The intra-file tool: one false-positive class, one false-negative class

The survey's tool matched a bare identifier followed by `(` **or whitespace**, so
a local variable with a function's name read as a call — it fired once, on
`init = %RunnerCommands.CreditWindowInit{…}` inside `offer_credit_window/2`,
and would have made C2 look cyclic.

**This tool requires `(` or `&` after the name, so that class cannot fire — and
it buys a false NEGATIVE instead**: a zero-arity local called without parentheses
is invisible. That is a real risk in Elixir and it was checked rather than
assumed. Every zero-arity `defp` in the three split files was enumerated
(`manager_call_timeout`, `runner_connect_timeout`, `boot_recovery_timeout`,
`schedule_runner_cleanup`, `get_runner_keepalive`, `send_gate_hold_ms`,
`credit_enabled?`) and every call site of each was read: **all use parentheses**,
so the edge sets are complete for these files. A file where they did not would
need the loose match and a manual read of every hit.

Both classes are the same underlying fact: **an intra-file call graph is a
heuristic over text, and the only safe use of one is to propose a partition that
is then confirmed by the compiler and the suite.** Here it proposed three
partitions; the compiler refused two drafts (a stray `@spec Lifecycle.bind(`, an
unterminated parent) and the suite refused two more (the `Logger` scope, the
`:persistent_term` key). Nothing reached the ledger on the tool's word alone.

## Filed on the way

- **[[KI-102]]** — all four items closed. `Builder.from_json/1`, `from_map/1`,
  `get_from_map/2` deleted; `FleetSupervisor.stop_fleet/0` and `default_runner/0`
  deleted; the `control_unit.ex` TODO deleted; the `@doc` describing
  `connect_handler/4` re-attached to it, off `list_factories/1`.
- **[[KI-103]]** — `FleetManager.rebind_data_streams/1` is public with **no
  caller anywhere**, and `docs/dev_notes/fix-first-input-drop-on-bind.md:33`
  calls it the convergence Loop's remedy for a mid-life data-stream rebind while
  `Convergence.Diff` mints no such op. Not fixed: deleting it rules the missing
  caller away and wiring it is a behaviour change. **The fix must correct that
  dev note in the same commit** — the note is why it read as a capability.
- Two integrity findings against `85f722cd`, both the supervisor's staging and
  both healed: one of this lane's test files was left out of the commit while its
  production half went in (the `live_writers` stub could not intercept, so two
  assertions were red at that commit — risk D7.1 landing exactly as the census
  predicted), and a peer's in-flight `Flezha.SafeCall` rename was swept into it
  against a module that commit does not contain.
