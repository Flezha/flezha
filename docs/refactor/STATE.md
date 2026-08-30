# Refactor board

**Last updated**: 2026-08-30 01:25 (supervisor session 2 — **AREA 3 DONE**, ready for hand-over)
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
   `docs/known_issues.md` (tracked, 40 entries, severity-ordered, each with
   evidence and what a fix must account for). Two carry proposals:
   `fix-outbox-watermark-overwrite` (KI-1) and `fix-first-input-drop-on-bind`
   (KI-2), both owed **before** the controller areas that refactor those files.
   `openspec/changes/` is gitignored, so those proposals are machine-local —
   `docs/known_issues.md` is the durable record.
4. **The charter was amended four times on 2026-08-29/30**, each time because
   doing the work exposed a rule that could not be satisfied: §1 (net lines vs
   the §7 ceiling), §7 (the function ceiling is production-only, matching what
   the gate always measured), §8 (build every checkpoint, measure once), §11
   (lines arriving from a higher layer, and ceiling-forced excess). Read them
   before planning an area; they are not the rules a previous session followed.
**Note**: `openspec/changes/` is gitignored — open and archived changes live on
this machine's disk only; their `tasks.md` is still the fine-grained progress.
**Rules**: `charter.md`. **Programme**: zero behaviour change, net code down,
one host core, tests fast and real.

## Status

Done: Phase 0, 1s, area 1 (kernel), area 2 segments A (`01f45d7d`), B (`fae9184f`)
C (`853f37fe`, plus the corrective `fmt(2)` `77287b1d`) and D (`e89e4a7a`). All five
areas now have no file over 800 lines and no function over 50; the area is
**−93 production and −354 test lines**. NEXT: area 2 segment E — batch 9 only
(verification, the final A/B on all three rows, artifacts, hand-over), which closes
the area.

**Progress, measured not estimated**: ~33 % by production LOC — 46.8 K of ~143 K
touched (`flz_core` 18.2, `flz_transport`+`flz_nif` 6.3, `flz_core_ffi` 4.7, the four
veneers 17.6). The 45 % figure this board carried earlier was my own count of board
ROWS, which over-counts badly: the five controller areas still ahead are 66 K
production lines on their own — 46 % of the codebase — and they also have to cut the
55-file dependency cycle, which is the hardest work in the programme and has not
started. Areas 3 (`sdk/rust`, 10.6 K + 20.1 K test) and 4 (`sdk/java`, 14.3 K) sit
between here and there.

**The history was rewritten outside the programme.** The user ran `fix.sh` (an
untracked `git filter-branch` that shifts commit timestamps) at 14:52; every commit
hash on this branch changed and the `refactor-baseline-*` tags moved with it (all
still ancestors of HEAD, so every `git diff refactor-baseline-<id>` still works).
This board's hashes are remapped to the new history; `.git-blame-ignore-revs`,
`baseline.json`'s `head`, `CLAUDE.md` and `docs/dev_notes/` still name dead hashes
(supervisor task, below). The pre-rewrite history survives on branch
`backup-20260829-145208`.

## Board

Order is dependency order; an area starts only when its dependencies are `done`.

| # | Area | Change id | Depends on | Status | Commit | Gates before → after |
|---|------|-----------|------------|--------|--------|----------------------|
| 0a | Baseline, gates, formatters | `refactor-0-baseline` | — | done | `8fb9d167` | gates now measure; `blocking: []` |
| 0b | Test suite mechanics (flakes, sleeps, fleets) | `refactor-0-tests` | 0a | done | `ea1f6f26` | sleeps 212→2, refutes 107→12, ExUnit 96.7 s→56.2 s; `test` now blocking |
| 1s | Dispatcher-unification A/B spike (numbers only) | `refactor-1s-dispatcher-spike` | 0a | done | `3914b140` (+ branch `spike/1s-dispatcher`) | converge; ≤ 1.45 % loss on every resolvable row, allocs flat |
| 1 | `flz_core` + `flz_transport` + `flz_nif` | `refactor-1-kernel` | 0b, 1s | done | `e709cd40`, `f1b4fe54`, `d6288963` | −1 851 dead lines; host/engine/driver/transport by owner; one error enum per crate; doc density 25→15 %, markers 21→0, clippy 24→0, files>800 7→0; A/B: erased +5 %, roundtrip +1–13 %, typed −1.4 % (area 3) |
| 2 | `flz_core_ffi` glue + Node/Ruby/PHP/Python veneers | `refactor-2-ffi-veneers` | 1 | **done** | A `01f45d7d`, B `fae9184f`, fmt `77287b1d`, C `853f37fe`, D `e89e4a7a`, E below | one veneer core, one shape, the ABI scenario by owner, validators by rule family; prod 15 285→15 192, test 8 769→8 415; fn>50 0, files>800 0, narrative 0, sleeps 7 in all five areas; all three A/B rows clean; **the five areas are now BLOCKING ratchets** |
| 3 | `sdk/rust` + `flezha-macros` | `refactor-3-rust-sdk` | 1 | **done** | A–F: `467a378a`, `52a5b2f2`, `81bb57d7`, `f293aa7b`, `2c4a39bd`, `fcb3ced9` | **fn>50 12→0 and 2→0, files>800 9→0, long_names 9→0**, narrative 20→2, sleeps 9→3; three flakes cured; **rows 1/2 carry ~2–4 % from segment B's seam — accepted, re-measure owed**; both crates now BLOCKING |
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

None. **Area 3 is done** (`fcb3ced9`, archived as `2026-08-29-refactor-3-rust-sdk`;
`sdk/rust` and `flezha-macros` flipped to blocking ratchets). Areas 0a, 0b, 1s, 1, 2
and 3 are complete — **~42 % of the codebase by production LOC**.

**Next**: area 4 (`sdk/java`, 8 441 lines) — its dependency (area 1) is done, and it
is the last area before the five controller areas, which are 42 % of the codebase on
their own and carry the 55-file dependency cycle.

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
the tree at `467a378a`, arm B2 the tree at `52a5b2f2`; build **both the same way**
(two worktrees are comparable to each other, a worktree and the tree are not) and run
row 1 at `FLEZHA_BENCH_MEASUREMENT_SECS=60`, `FLEZHA_WORKER_POOL_SIZE=8`, five paired
rounds. The arms themselves die with this session's scratchpad.

This is also the first time the seam has been priced at all. The 1s spike put it at
≤ 1.45 % on a branch; checkpoint 2 built its arm and measured nothing, by the
rewritten §8. That is the policy working as designed — the cost was found, and found
attributably, for one 7-minute bisect instead of six measured checkpoints.

## Awaiting the user's decision

- THREE product defects need their own (behaviour-changing) changes and a priority
  call — the first-input drop now has a second face (1c: the restore re-forward
  path drops the same way): **F1** (watermark overwrite after a park, `wal_writer.ex:1288`
  + `input_log.ex:189`) and the **first-input drop after `start_operative/2`**.
  Recommendation: fix both as ordinary OpenSpec changes before area 5a (state) and
  5e (runner), since those areas refactor exactly these files; the programme does
  not block on them — areas 1s–4 and 6 do not touch that code.

### Gate finding — the format gate aborts before it checks Rust

`scripts/format-apply.sh` runs a `need` preflight for every formatter (line 33,
rubocop) BEFORE running any of them, and `cargo +nightly fmt --all` is line 43. On a
box missing one veneer formatter the script fails loudly about that formatter and
never reports that Rust is dirty. That is how segment B's six unformatted production
files reached commit `fae9184f`: the worker's "format-check.sh exits 0" was taken on
trust and the supervisor ran the naming and metrics gates but not the format gate.
Corrected by a `fmt(2)` commit; the lesson is that **the supervisor runs
`cargo +nightly fmt --all --check` itself at every close** — it is read-only and
takes seconds. Worth considering whether the preflight should warn-and-continue
rather than abort, so a missing veneer formatter cannot mask a dirty Rust tree.

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
(`f746f798`); every later area follows it:

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
> the unvalidated OOM fix, the production items). Two carry proposals:
> `fix-outbox-watermark-overwrite` (KI-1) and `fix-first-input-drop-on-bind` (KI-2).
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
- 0b commit `220ba60c`'s `:sys.get_state(Loop)` drain in the output-stall drill is
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
  RESOLVED 2026-08-29 by amending charter §7 (`6b38f2b9`): the function ceiling is
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
  "unless it blocks the area" clause**: `8fb9d167` (`refactor(0a)`) renamed
  `const bounded` → `const boundedMailbox` in `test/apps/node_runner/fixtures.mjs`
  onto a name the file already declared at top level, so the module has been a
  `SyntaxError` ever since (`node --check` fails at `refactor-baseline-2` and at
  every commit from `8fb9d167` on) and the Node runner test app could not load a
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
  `head` at `1e371a37` (dead), and dead hashes in `CLAUDE.md`, `docs/dev_notes/
  refactor-0-baseline.md`, `docs/refactor/targets.md` and `charter.md`. Supervisor
  reconciles them in a docs commit this session.
- 2 (supervisor, session 2) foreign untracked file `fix.sh` in the tree (the
  rewrite script); the untracked `test/apps/node_runner/package-lock.json` noted
  by session 1 is gone. Neither is this change's; left alone.
- 1s a stale worktree from another session (`…/55da9198…/scratchpad/baseline2`,
  detached at `30e64258`, a pre-rewrite hash, prunable) is not ours; left alone.

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
  the box already are; OTP stays 28 in CI). Measured at `dbcc5520`: 1.19.2 and
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
  to them (commit `8fb9d167` landed with three offences in its own dev notes this
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
- 2026-08-29 `test` is a blocking ratchet from `ea1f6f26`: sleeps ≤ 2, refutes ≤ 12,
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

1. `/loop /refactor` in a new session: it reads this file, finds no worker and area 2
   `implementing`, and re-spawns segment B (batches 3–4) from task 3.1.
2. Then segments C (5–6), D (7–8), E (9) of area 2; supervisor commit at each.
3. Area 3 (Rust SDK): `TypedBodies<S>`, the typed tail onto `settle_effects`, the
   bench scaffolding onto `flz_core::bench_support`, the typed-path −1.4 %.
4. Owed defect changes (user's priority call, see "Awaiting"): F1 dlq watermark, the
   first-input drop (two faces) — before areas 5a/5e. Owed follow-up
   `sdk-declaration-refusals-in-the-core` after area 4.
5. Plug the laptop in before any checkpoint: every A/B is refused on battery.
