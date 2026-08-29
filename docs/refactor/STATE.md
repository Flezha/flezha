# Refactor board

**Last updated**: 2026-08-29 12:46 (supervisor session 1)
**Resume**: open a session in this repo and run `/loop /refactor`. The skill
reads this file, `git status`, and the open change's `tasks.md`, then continues.
**Note**: `openspec/changes/` is gitignored — open and archived changes live on
this machine's disk only; their `tasks.md` is still the fine-grained progress.
**Rules**: `charter.md`. **Programme**: zero behaviour change, net code down,
one host core, tests fast and real.

## Status

Area 2: segment A committed (`01f45d7d`, batches 0–2; A/B owed — box on battery); segment B (batches 3–4) running.

## Board

Order is dependency order; an area starts only when its dependencies are `done`.

| # | Area | Change id | Depends on | Status | Commit | Gates before → after |
|---|------|-----------|------------|--------|--------|----------------------|
| 0a | Baseline, gates, formatters | `refactor-0-baseline` | — | done | `e8f81b8a` | gates now measure; `blocking: []` |
| 0b | Test suite mechanics (flakes, sleeps, fleets) | `refactor-0-tests` | 0a | done | `0670e992` | sleeps 212→2, refutes 107→12, ExUnit 96.7 s→56.2 s; `test` now blocking |
| 1s | Dispatcher-unification A/B spike (numbers only) | `refactor-1s-dispatcher-spike` | 0a | done | `3914b140` (+ branch `spike/1s-dispatcher`) | converge; ≤ 1.45 % loss on every resolvable row, allocs flat |
| 1 | `flz_core` + `flz_transport` + `flz_nif` | `refactor-1-kernel` | 0b, 1s | done | `e709cd40`, `f1b4fe54`, `d6288963` | −1 851 dead lines; host/engine/driver/transport by owner; one error enum per crate; doc density 25→15 %, markers 21→0, clippy 24→0, files>800 7→0; A/B: erased +5 %, roundtrip +1–13 %, typed −1.4 % (area 3) |
| 2 | `flz_core_ffi` glue + Node/Ruby/PHP/Python veneers | `refactor-2-ffi-veneers` | 1 | implementing | A: `01f45d7d` | A: one veneer core, error enum, hygiene; prod 3 444→3 274, dups 23→7; A/B owed (battery) |
| 3 | `sdk/rust` + `flezha-macros` | `refactor-3-rust-sdk` | 1 | pending | | |
| 4 | `sdk/java` | `refactor-4-java-sdk` | 1 | pending | | |
| 5a | Controller `state` | `refactor-5a-state` | 0b | pending | | |
| 5b | Controller `plugin` | `refactor-5b-plugin` | 5a | pending | | |
| 5c | Controller `service` (+ `cli_manager` split) | `refactor-5c-service` | 5b | pending | | |
| 5d | Controller `observability` + `mix` tasks | `refactor-5d-observability` | 5c | pending | | |
| 5e | Controller `operative`, `runner`, `convergence`, rest | `refactor-5e-runtime` | 5d | pending | | |
| 6 | `cli` | `refactor-6-cli` | 1 | pending | | |
| 7 | Tests, helpers, test apps | `refactor-7-tests` | 2,3,4,5e,6 | pending | | |
| 8 | Docs reconcile (`CLAUDE.md` index, dev_notes names) | `refactor-8-docs` | 7 | pending | | |

Status values: `pending` → `surveying` → `proposed` → `implementing` →
`checkpoint` → `done`; `blocked` names the blocker below.

## Current worker

- change `refactor-2-ffi-veneers` · role implement, segment B = batches 3–4 · started 2026-08-29 12:46
- checkpoint 2 after batch 4 (its A/B covers rows 5, 7py, 7node against arm A —
  which also settles the owed checkpoint-1 rows; arm B2 is kept for attribution)
- worker agent lives in supervisor session 1 only; a new session must re-spawn
  with the resume brief from the first unticked task
- last seen: 2026-08-29 12:46 (spawned)

## Awaiting the user's decision

- THREE product defects need their own (behaviour-changing) changes and a priority
  call — the first-input drop now has a second face (1c: the restore re-forward
  path drops the same way): **F1** (watermark overwrite after a park, `wal_writer.ex:1288`
  + `input_log.ex:189`) and the **first-input drop after `start_operative/2`**.
  Recommendation: fix both as ordinary OpenSpec changes before area 5a (state) and
  5e (runner), since those areas refactor exactly these files; the programme does
  not block on them — areas 1s–4 and 6 do not touch that code.

## Found defects (not fixed inline — each becomes its own change)

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
  `ParenthesesOnZeroArityDefs`, which contradict house style — area 5a decides
  disable-or-adopt before the count is ratcheted as blocking.
- 0a: `cargo clippy --workspace --all-targets` and `cargo test --workspace` are red
  without `--features flezha_runner/bench-internals`.
- 0a: `mix deps.get` reports packages with security advisories (pre-existing).
- 0a: `.git/info/exclude` on this box hides `bench/flink/`; other boxes see reports.
- 0b **F1 — PRODUCT DEFECT, own change owed, before area 5a touches `wal_writer`**:
  after an effectively-once park, `WalWriter.take_outbox_window/3` re-enters
  `dispatch_outbox_window` via `complete_entry({:cursor,1})` → `try_dispatch_outbox`
  (`wal_writer.ex:1288`) while the outer window is open; the nested window advances
  the consumed watermark to 2, then the outer window's `InputLog.record_consumed/3`
  (`input_log.ex:189`, unconditional `:ets.insert`) overwrites it with 1. Fires
  whenever the park record and the next record share one fsync batch. Impact: the
  input-log retention floor and credit accounting under-report by one offset after
  every park until the next input. `test/system/dlq_e2e_test.exs:119` fails 14/20
  alone — it was never a load flake. Left red, untagged, per charter §9.
- 0b F5 — `test/flezha_test.exs:591` (runner disconnect, preserve) failed 1/6 full
  runs, 0/10 alone, 0/3 instrumented; root cause not established; left as is.
- 0b `mix test --include system` always exits 2: the two `:postgres_integration`
  modules raise in `setup_all` because an include beats an exclude (the `:interop`
  self-skip pattern is the fix; authorised inside 0b).
- 0b UDS socket path overflow in the system tier: a stream socket path of 114 bytes
  (`/tmp/flezha_sockets/<node>/secondary_stream_<namespace>_<ids>_wus.sock`) exceeds
  `sun_path`; 120 "Failed to start primary listener" + 167 "Failed to connect runner"
  error lines per run while the tests still pass. Owed.
- 0b commit `e3d6e2e3`'s `:sys.get_state(Loop)` drain in the output-stall drill is
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
  cannot drop. Every e2e module now settles on `await_stream_bound/2`;
  `sdk_smoke` still carries an older re-send loop.
- 0b UDS driver `DropNewest` write-buffer drops are invisible to the caller
  (`Native.send_frame/4` always answers `:ok`); only `driver_stats.write_buffer_dropped`
  sees them (the last silent-drop default; already on the memory list).
- 0b `unique/1` suffixes reach 7 digits in one BEAM, so any id prefix over 24 chars
  breaches the 32-byte operative-id cap late in a combined run.
- 0b `sdk_smoke_e2e_test.exs` `send_until_forwarded` is the old re-send workaround
  for the first-input drop; fold onto `await_stream_bound/2` in area 7.
- 0b F5 (`flezha_test.exs:591`) still unexplained; 10/10 alone again.
- 1s `test/apps/python_runner/__pycache__/fixtures.cpython-312.pyc` is COMMITTED
  and stale; every Python runner launch dirties the tree (area 2 deletes it and
  ignores `__pycache__`).
- 1s the tree's installed Python veneer `.so` is a DEBUG build
  (`build-python-sdk.sh` default, 24.5 MB); a `--runtime python` round trip under
  `FLEZHA_BENCH_SKIP_BUILD=1` measures a debug veneer silently.
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
- 1s a stale worktree from another session (`…/55da9198…/scratchpad/baseline2`,
  detached at `30e64258`, prunable) is not ours; left alone.

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
  the box already are; OTP stays 28 in CI). Measured at `17bdcb9c`: 1.19.2 and
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
  to them (commit `e8f81b8a` landed with three offences in its own dev notes this
  way). The supervisor `git add -N`s a worker's new files before gating.
- 2026-08-29 The e2e first-input race is NOT sink-plugin activation (that wait ran
  green as a no-op and the failures continued); it is the data-stream bind. The
  settle is `NodeTestHelper.await_stream_bound/2` on the acceptor's own
  `:stream_ref_status` — a positive signal — never a re-send.
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
  `d6288963` (prod 12 639 / 1 743 / 2 048; markers 0; files>800 0; clippy 0).
- 2026-08-29 Rows 7 (veneer whole-system) run `-p 2 -m 40000 --runners 1` = 80 000
  records per run, the 1s spike's resolving shape; K33's "80 000 records" means the
  total, not `-m`.
- 2026-08-29 A segment whose A/B the box refused (battery) is still committed when
  every suite is green and the hot-path edit is a thin typed wrapper; the A/B is
  owed at the next checkpoint on mains with the earlier arm kept for attribution.
- 2026-08-29 `test` is a blocking ratchet from `0670e992`: sleeps ≤ 2, refutes ≤ 12,
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

1. Survey area 2 (FFI glue + four veneers), approve, implement in two-batch
   segments; the seam's SDK twins are area 3's, the glue dedup is area 2's.
2. Then area 3 (Rust SDK: `TypedBodies<S>`, the typed tail onto `settle_effects`,
   the bench scaffolding, the typed-path −1.4 %).
3. Owed defect changes (user's priority call): F1 dlq watermark, the first-input
   drop (two faces) — before areas 5a/5e.
