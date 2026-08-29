# Refactor board

**Last updated**: 2026-08-29 04:23 (supervisor session 1)
**Resume**: open a session in this repo and run `/loop /refactor`. The skill
reads this file, `git status`, and the open change's `tasks.md`, then continues.
**Note**: `openspec/changes/` is gitignored — open and archived changes live on
this machine's disk only; their `tasks.md` is still the fine-grained progress.
**Rules**: `charter.md`. **Programme**: zero behaviour change, net code down,
one host core, tests fast and real.

## Status

Phase 0 and 1s done (1s `3914b140`, verdict converge). Area 1 (`flz_core` + `flz_transport` + `flz_nif`) surveying.

## Board

Order is dependency order; an area starts only when its dependencies are `done`.

| # | Area | Change id | Depends on | Status | Commit | Gates before → after |
|---|------|-----------|------------|--------|--------|----------------------|
| 0a | Baseline, gates, formatters | `refactor-0-baseline` | — | done | `e8f81b8a` | gates now measure; `blocking: []` |
| 0b | Test suite mechanics (flakes, sleeps, fleets) | `refactor-0-tests` | 0a | done | `0670e992` | sleeps 212→2, refutes 107→12, ExUnit 96.7 s→56.2 s; `test` now blocking |
| 1s | Dispatcher-unification A/B spike (numbers only) | `refactor-1s-dispatcher-spike` | 0a | done | `3914b140` (+ branch `spike/1s-dispatcher`) | converge; ≤ 1.45 % loss on every resolvable row, allocs flat |
| 1 | `flz_core` + `flz_transport` + `flz_nif` | `refactor-1-kernel` | 0b, 1s | surveying | | |
| 2 | `flz_core_ffi` glue + Node/Ruby/PHP/Python veneers | `refactor-2-ffi-veneers` | 1 | pending | | |
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

- change `refactor-1-kernel` · role survey · started 2026-08-29 04:23
- deliverable: proposal/design/tasks for area 1 (no code)
- worker agent lives in supervisor session 1 only; a new session must re-spawn
- last seen: 2026-08-29 04:23 (spawned)

## Awaiting the user's decision

- Two product defects found by 0b need their own (behaviour-changing) changes and
  a priority call: **F1** (watermark overwrite after a park, `wal_writer.ex:1288`
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

1. Survey area 1 (kernel), approve, implement (the seam from `spike/1s-dispatcher`
   is a named task; hot path → A/B rows 1–6 per the refactor-gates requirement).
2. Then area 2 (FFI glue + veneers) or 5a (state), by board order: 2.
3. Owed defect changes (user's priority call, see "Awaiting"): F1 dlq watermark,
   first-input drop after `start_operative/2`.
