# `lib/flezha/state` — ten modules out of four, and fourteen edges named

Area 5a, seven batches, `refactor-baseline-5a` (`f7df3fb3`) → close-out. Every
number here was measured on this box with the new files intent-added; a figure
read before that step is marked as such, because three times in this area it was
wrong in the flattering direction.

## The instrument lies about untracked files, and it lied twice here

`refactor_metrics.py` reads `git ls-files`, so a file the worker has created but
nobody has staged is invisible to every metric — and refactors *add* files, so
the error is one-directional (charter §11e).

- **`lib/flezha/state` `prod_loc` read 6 138 where the truth was 6 897** — a
  759-line phantom saving, from seven untracked modules.
- **The `test` area's `duplicate_pairs` read 112 where the truth was 118.**
  jscpd scans the filesystem, so it *sees* an untracked file; `summarise` then
  keeps a pair only when `pa in files or pb in files`, so a pair with **both**
  halves untracked vanishes. Batch 6 created exactly that shape — a new test
  file plus a new case template — and six pairs went missing.

  **What that would have cost is a baseline written three low.** The recorded
  baseline for `test` is **118**; the unpatched 112 was not a regression against
  it, and calling it one (as this note first did) was wrong. The real danger is
  the opposite direction: `--write-baseline` run against the unpatched tree would
  have recorded 112 as the new normal while the tree actually stood at 115,
  arming the ratchet **three below reality** and guaranteeing a red on the very
  next run, in an area nobody had changed. A blindness that under-reports is
  worst precisely at the moment a baseline is written.

The workaround a worker can run alone: import the script and monkey-patch
`tracked_files()` to include the new paths. It is read-only and touches no
index. Do it before quoting any number.

## A behaviour must not name its implementations

The four-file `backend ⇄ {postgres, s3, local_path}` cycle existed because
`Backend.resolve/0` — inside the behaviour module — listed the three modules
that implement it. The survey simulated moving `resolve/0` to `DurabilityTier`
and predicted no collateral. **It was wrong**: the big cycle went 56 → 57 and
`declared_manifest.ex` joined it, because `HubManager` reaches `DeclaredManifest`
and `DurabilityTier` reaches `HubManager`. A set-diff of the `mix xref` output
named the file in one line.

`DeclaredManifest` cannot be argument-threaded — its three uses sit in ProcessHub
behaviour callbacks of fixed arity — so it must resolve from *somewhere*, and
anything inside the tier's own graph drags it in. The fix separates the contract
from the registry: `DurabilityTier.Backend.Config` names the three backends and
nothing else, and `Backend` names nothing at all. Re-measured: the 4-cycle gone,
56 unchanged, `joined: []`, `left: []`.

## `max_cycle_files` rises when one cycle member becomes five

Splitting `wal_writer.ex` put four siblings where one file had been, all reaching
the same collaborators (`InputLog`, `Commands`, `Recovery`, `ClassRegistry`).
No edge changed and the cycle's membership is byte-identical before and after —
`joined: []`, `left: []` — but the per-file count went 17 → 21 → 23. This is the
third instance of charter §11f's pattern. **The ruling is that the area reports
the edges it cut and puts the count beside them**, not that a fourth metric gets
installed.

`record_frame.ex`, `wal_writer/state.ex`, `proc_journal/codec.ex`,
`input_log/sweep.ex`, `writer_registry.ex` and `backend/config.ex` all sit
**outside every cycle** — that placement was the design goal and it held.

**`mix xref` reads the compiled manifest.** A cycle number taken against a stale
`_build` is wrong; `max_cycle_files` read 21 at one checkpoint and 23 at the next
with identical membership, and the clean-compile reading is the one to trust.

## The fourteen edges, and why the other seven stayed

Cut: `seq_index → wal_writer`, `time_travel → wal_writer`,
`durability_tier/artifact → wal_writer`, `input_log → wal_writer` (all four by
`RecordFrame`); `backend → backend/{postgres,s3,local_path}`;
`{fsync_batcher, outbox, retention, snapshot, watchdog, wal_writer} → supervisor`
(by `WriterRegistry`); and `watchdog → convergence/backoff` **(compile)**, by
moving a `defstruct` default into `init/1` — a struct default is evaluated at
compile time, which is the whole reason that edge was compile-labelled.

Left, with reasons: `restore → snapshot`, `commands → watchdog`,
`proc_journal → record_log` and `recovery → commands` are **same-tier** edges the
design's table permits — cutting them duplicates or inverts working code.
`settlement → durability_tier`, `admission → commands` and `outbox → proc_journal`
are **genuine upward breaches of one call each**, and every removal is a
behaviour change (the tier would subscribe to telemetry instead of being called),
which §1 forbids in a refactor commit.

## A defect this area introduced, and fixed two batches later

Batch 2's one-definition collapse gave `snapshot.ex` and `restore.ex`
`@header_size Wire.header_size()`. `Wire` is **inside the 56-node cycle**, so
both files became compile-connected to it: any change to any of the 56 recompiled
them. HEAD had plain literals and neither file named `Wire` at all. Found by
`mix xref graph --label compile --source <file>` while verifying D4, and fixed by
pointing all three constants at `RecordFrame.header_size()`, a leaf. The big
cycle's compile count went **3 → 1**.

**Two forms of that collapse are unbuildable, and the proposal named both.**
`@header_size` is used in `binary-size(@header_size)` patterns and in guards, so
"the other three call `Wire.header_size/0`" cannot be written — only
`@header_size RecordFrame.header_size()` compiles, which is the idiom
`wal_writer.ex` already used for `@reserved_entity_keys`. And renaming the
private `class_or_global_compact_after_bytes` to `compact_after_bytes` collides:
the public `compact_after_bytes/2` has a default argument, so it already defines
`/1`.

## A `child_spec` deletion is proved, not argued

Four hand-written `child_spec/1`s were byte-equivalent to what `use GenServer`
generates. The proof: capture `Module.child_spec(arg)` for six modules × three
argument shapes through `mix run --no-start` **before** deleting, delete, capture
again, `diff`. **18 rows identical.** `input_log.ex` (`id: {__MODULE__, class}`,
`restart: :transient`) and `supervisor.ex` (`type: :supervisor`, and it discards
its argument) differ and stayed. The deletion also killed **both** intra-area
`duplicate_pairs`.

## Splitting a GenServer: what the arithmetic said before any code moved

The design's four extractions supply ~895 lines against the ~989 the ceiling
needs — measured, before writing anything — so `wal_writer.ex` would have landed
at ~894 with all four taken. The gap closes only with `Boot` (the `init/1` +
`handle_continue/2` bodies) and the struct. Result: **2 021 → 666.**

Two of the design's destinations were wrong and had to be re-ruled.
`CreditLedger` is tier 0 and its own moduledoc says *"Accounting only — no
processes, no side effects"*, while six of the ten members named for it spawn,
emit telemetry, `send/2`, or call `Commands`/`Recovery` (tier 3). `Outbox`
already exports `settled/1`, `confirm/1`, `complete/2` and `dispatch/2` against
the cluster's `settled/2`, `confirmed/2`, `complete_entry/2` and
`dispatch_outbox_window/2`. Both went to `WalWriter.*` siblings instead.

**The struct moved for a named edge, not for tidiness.** Four siblings need
`now_ms/0`; a sibling naming its parent for it is a 2-cycle. `WalWriter.State`
holds the struct and the clock, names only `Paths` and `SeqIndex`, and sits in no
cycle. `RecordFrame` took `file_size/1` for the same reason.

**Build the call graph with comments stripped before cutting.** Doing so found a
real `Admission ⇄ Epoch` 2-cycle before it landed: `shed → emit_seq_gap` against
`await_baseline`/`realign_epoch`/`rewind → resync_rate_limited`/
`flush_credit_pending`. `emit_seq_gap/3` has no `Epoch` caller — only `shed` and
`do_append_async` — so it moved to `Admission` and the cycle never existed.

**Incremental compilation hides a visibility regression.** The extracted
functions were all made public, then flipped back to `defp` by a script that
looked for external references. `\b` after an escaped `?` never matches before
`(`, so `Epoch.stale_writer?`, `Epoch.realign?` and `Admission.over_hard_bound?`
were privatised while still called from another module. `mix compile` said
nothing; `mix compile --force` (274 files) found all three.

## Splitting a test file makes its in-file duplication visible

jscpd counts **cross-file** pairs only. Splitting `wal_writer_outbox_test.exs`
added **nine** pairs for source that had been duplicated inside one file all
along — area 3's 5 → 65 phenomenon, at a smaller scale. Hiding it by not
splitting is not available (the file was 1 040 lines); the fix is to remove the
duplication: `armed_writer/2`, `append_effect/5`, `stage_effect/5` and
`reopen_with_stale_cursor/1` on the shared case template collapsed 22 + 19 + 9 +
3 call sites and took the pairs back to zero. Two hand-rolled `start_supervised`
loops then became `TestHelpers.start_state_tree/0`, and on the quiescent tree the
area finished at **115 against a recorded baseline of 118**.

**An `ExUnit.CaseTemplate` is invisible to `async_false_share`.** The classifier
matches `use\s+[\w.]*Case\b`; `ExUnit.CaseTemplate` has no word boundary after
`Case`, so the template counts as no module while its users still count with
their own `async:`. That is what makes a 1 → 2 split affordable against a
blocking share.

**Folding is free when the removed set is predominantly `async: false`.** Three
modules removed, none of them `async: true` (j/k = 0), one added: 0.8130 →
0.8114 against a 0.813 blocking baseline.

## The four `refute_receive` are justified, not replaced

All four are ≤ 100 ms and follow a positive settle: two behind a synchronous
`GenServer.call(sc, :stream_ref_status)` barrier, two behind an
`assert_receive ..., 1_000`. §9 permits them; the audit is the deliverable.

## Performance

S1 (`bench.workflow --scenario chain`) is `direction` by its own measured floor —
303 % over five arm-A runs, 4.8 % over the settled plateau — and was **not run**.
The evidence in its place is that the driving binary cannot have moved:
`target/release/rust_runner`
`sha256:3bc991c21915581b109d1ffac8509f7fb52182a1070ac9afddb0a8ebfe4a60f9`,
identical to the batch-0 arm, with `git diff --stat` over `sdk/ flz_core/
flz_core_ffi/ flz_transport/ flz_nif/ test/apps/` empty. The categorical checks
stand in for it: no per-record allocation added, no function call became a
`GenServer.call`, `:file` call counts unchanged, and the KI-41 dispatch-order
test — which is red on the defective code — still passes.

**What no row here can conclude**: none resolves the ≤ 2 % a module extraction
plausibly costs, and S2, the row that would come closest, cannot run on this box
(it needs `bench/flink/compose.yml`, which is gitignored and not up).

## The shared Elixir suite is enforced by the OS

Running beside a second worker, four runs aborted outright — twice on the state
root's `fcntl` claim (`state root ... is already claimed (held by pid N)`), twice
on `EADDRINUSE :4500` from the peer's testnet app — and one `mix compile` waited
on the build-directory lock. Nothing was corrupted; the failures are loud. The
remedy is to narrow the slice, run the full suite once at segment end, and retry
on a collision. Never kill the peer's processes.

## Which of an area's rows a concurrent peer can move

Read off `summarise/5` rather than assumed, because the answer decides which
numbers a worker may quote while another lane is live.

**Peer-independent by construction** — summed or listed over `files`, the area's
own tracked files, and nothing else: `prod_loc`, `test_loc`, `comment_lines`,
`doc_lines`, `narrative_markers`, `sleeps`, `refutes`, `files`, `test_files`,
`comment_density`, `files_over_800`, `functions_over_50`, `long_names`,
`async_false_share`. For `lib/flezha/state` all fourteen are fixed the moment its
own files stop changing, and they read identically to me and to the supervisor.

**Whole-repo derived, and therefore movable — three, not two.**
`duplicate_pairs` comes from `jscpd_pairs` over the full scan set; `cycles` and
`max_cycle_files` come from one `xref_cycles()` output by the same membership
test. **`cycles` belongs on that list for exactly the reason `max_cycle_files`
does**, and leaving it off understates the exposure.

**Here the exposure is concrete, not theoretical.** Two of this area's five
duplicate pairs have their other half inside the peer's live lane —
`plugin/provider/postgres.ex:169-173 ↔ state/durability_tier/backend/postgres.ex`
and `plugin/provider/worker_sup.ex:73-78 ↔ state/supervisor.ex:74-79`. Either can
vanish or move when the peer edits its own file, with no state file touched.

**An exclusion experiment is the wrong instrument for it**, which is worth
knowing before someone runs one. Dropping the peer's files from `tracked_files()`
left `lib/flezha/state duplicate_pairs` at 5: a pair counts when `pa in files or
pb in files`, so the state half alone keeps it. jscpd scans the filesystem, so
the peer moves the number by changing its own *source*, not by being tracked.

**`test` is the harder case and it is every later area's problem**, because the
peer's files are *members* of it rather than merely paired with it. The same
experiment moved nine rows: `test_loc` 38 273 → 29 975, `files_over_800` 7 → 4,
`narrative_markers` 37 → 32, `refutes` 16 → 13, `async_false_share`
0.811 → 0.865, `duplicate_pairs` 115 → 80. Five of those are ratcheted. An area
that folds or splits tests shares that ratchet with every concurrent worker, so
its close-out reading is only meaningful with the other lane committed.

The instability is not subtle: this area read `test duplicate_pairs` as
118 → 109 → 115 → 109 → 115 over one afternoon, with its own test files
unchanged between the last four, and **115 on the finally-quiescent tree** — so
the two 109s were the peer's uncommitted test deletions, not this area's work. **Only a reading taken with the other lane
quiescent means anything**, and a baseline written from any other reading arms
36 ratchets against a number that never existed in git.

## Provenance of the baseline this area recorded

Recorded at **`63df836e`**, on `moon`, with `--lint`, **with the tree clean and
one lane live** — `git status --short` empty, the peer paused. That condition is
part of the measurement, and the three movable rows are only meaningful with it:

| row | recorded | why it needed a quiescent tree |
|---|---|---|
| `duplicate_pairs` | 5 | two of the five pairs have their other half in `lib/flezha/plugin` |
| `cycles` | 1 | `xref_cycles()` is whole-repo |
| `max_cycle_files` | 23 | same output, same membership test |

The other fourteen rows are summed over the area's own files and needed no such
window.

**`baseline.json` cannot carry this note itself.** `baseline_payload/4` rebuilds
the payload from the current measurement and carries over only `blocking`,
`timings` and `bench`, so any extra top-level key is dropped by the next
`--write-baseline`. What the file *does* pin is `head` and `box`; the condition
of the tree at that head lives here.

## The flip was proved armed, in two arms

`prod_loc` lowered by one in the baseline (6 911 → 6 910) against a tree at
6 911:

- **blocking**: `BLOCKING lib/flezha/state prod_loc: 6910 -> 6911`, **exit 1**
- **the same regression with the area removed from `blocking`**:
  `measuring lib/flezha/state prod_loc: 6910 -> 6911`, **exit 0**

The second arm is what proves the exit code comes from the `blocking`
membership rather than from the regression alone. Baseline then restored from a
byte copy and verified with `sha256sum -c`
(`d3b61b3803843d0e74f4e2b5122be96ec80f87c2eae207e8ac708d2974091da6`), and
`--check` re-read **0 regressions, exit 0**.

## A re-record absorbs whatever is worse, so diff it first

`--write-baseline` rewrites every row, so anything that regressed since the last
recording becomes the new normal silently. Diffing the new file against a byte
copy of the old one across `RATCHETED` found three rows worse: `lib/flezha/state
prod_loc` 6 844 → 6 911 (this area's, ceiling-forced and accounted line by line),
`lib/flezha/operative prod_loc` 2 447 → 2 453 (the peer's, agreed), and
**`test credo_warnings` 83 → 84**, which was neither. It was two pre-existing
`AliasOrder` hits in this area's own tests (`watchdog_test.exs:21`,
`wal_transfer_test.exs:12`); fixing them took the row to **82** and the baseline
was re-recorded rather than left absorbing a rise nobody had attributed.
