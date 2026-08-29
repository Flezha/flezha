# Known issues

Defects found but not fixed where they were found, with enough context that a
session which has never seen this repository can pick one up and work it.

**Why this file exists.** The refactor programme's charter (`docs/refactor/
charter.md` §1) forbids fixing a defect inside a refactor commit: a refactor
changes structure and never behaviour, so anything found on the way is recorded
and fixed in its own change. That recording used to live in the programme's
board (`docs/refactor/STATE.md`), which is programme-scoped and retires when the
programme does. These outlive it, so they live here.

**How to use it.** Each entry states where the defect is, what it does, the
evidence it is real, why it was not fixed on the spot, and what a fix has to
account for. `P1` blocks correctness or hides data loss; `P2` costs real time or
lets a gate lie; `P3` is a wart. An entry marked **proposal owed** needs an
OpenSpec change written before code moves; one marked **investigation first**
cannot be specified yet.

**Adding to it.** New entries go in the right section with the same fields. Do
not delete a resolved entry — move it to *Resolved* with the commit that closed
it, so a future reader can tell "fixed" from "never recorded".

**Note on proposals.** `openspec/changes/` is gitignored, so an OpenSpec change
named below exists only on the machine that wrote it until it is archived. **This
file is the durable record**; a proposal is the working artifact. If a change
named here is missing from `openspec list`, it was never archived and the entry
below is all that survives — which is enough to write it again.

---

## P1 — correctness and silent data loss

### KI-1 · Watermark overwritten after an effectively-once park

**OpenSpec change**: `fix-outbox-watermark-overwrite` (proposed 2026-08-30).

- **Where**: `lib/flezha/state/wal_writer.ex:1288`, `lib/flezha/state/input_log.ex:189`.
- **What happens**: after an effectively-once park, `WalWriter.take_outbox_window/3`
  re-enters `dispatch_outbox_window` via `complete_entry({:cursor,1})` →
  `try_dispatch_outbox` while the outer window is still open. The nested window
  advances the consumed watermark to 2; the outer window's
  `InputLog.record_consumed/3` then overwrites it back to 1 with an
  unconditional `:ets.insert`.
- **Impact**: the input-log retention floor and credit accounting under-report by
  one offset after every park, until the next input arrives.
- **Fires when**: the park record and the next record share one fsync batch.
- **Evidence**: `test/system/dlq_e2e_test.exs:121` (was `:119` before the file
  moved) fails 14 of 20 runs **in isolation** — it was never a load flake.
  Re-confirmed 2026-08-29 by an interleaved run against tag
  `refactor-baseline-2`: the failure reproduces on the baseline arm, so it
  predates the refactor programme entirely.
- **Why not fixed here**: behaviour change; charter §1.
- **A fix must**: make the nested and outer windows agree on who owns the
  watermark, rather than making `record_consumed/3` conditional and leaving two
  writers. Note the test is currently red and untagged, deliberately.

### KI-2 · First input after `start_operative/2` is journaled and never forwarded

**OpenSpec change**: `fix-first-input-drop-on-bind` (proposed 2026-08-30).

- **Where**: the start path in `lib/flezha/runner/`, and the restore re-forward
  path (second face, below).
- **What happens**: `start_operative/2` returns on the control-channel ack, but
  the runner's primary data stream binds later — `StreamChannel` publishes
  `stream_ref: nil` at init. A `send_io` cast in that window hits `io_in/3` →
  `{:error, :no_stream}` and is dropped, recorded only as `nil_drops` and a
  `[:flezha, :stream, :drop]` telemetry event. Under tier-on the input **is**
  journaled (`Ingest.verdict/2` answers `:ok` on `:no_stream`) and then never
  forwarded at bind.
- **Impact**: silent loss of the first input(s) of an operative's life, with a
  durable record claiming they were accepted.
- **Evidence**: a telemetry probe logged 38 drops on exactly the failing e2e
  operatives in one combined run.
- **Second face**: `test/system/effectively_once_checkpoint_e2e_test.exs:37` —
  after a rebind the controller re-forwards offsets onto a data stream the fresh
  runner has not bound yet; three `[:flezha, :stream, :drop]` in one millisecond,
  never re-driven. Reproduced on the 40th probed iteration in isolation.
- **Why not fixed here**: behaviour change; charter §1.
- **A fix must**: either make the start reply wait for the bind, or make the cast
  un-droppable (queue until bound). Both faces need the same cure — a
  bind-aware forward — or the restore path will keep dropping after the start
  path is fixed.
- **Worked around in tests by**: `NodeTestHelper.await_stream_bound/2`, which
  settles on the acceptor's own `:stream_ref_status`. Every e2e module now uses
  it. `test/system/sdk_smoke_e2e_test.exs`'s `send_until_forwarded` is an older
  re-send workaround for the same defect and should fold onto it (see KI-20).

### KI-3 · UDS write-buffer drops are invisible to the caller

- **Severity**: P1 · **investigation first**, then proposal.
- **Where**: the UDS driver behind `Flezha.Transport.Native.send_frame/4`.
- **What happens**: the write buffer's overflow policy is `DropNewest`, and
  `send_frame/4` answers `:ok` regardless. Only
  `driver_stats.write_buffer_dropped` records the loss.
- **Impact**: this is the last silent-drop default in the platform, against a
  stated guarantee that nothing drops by default (see
  `docs/dev_notes/close-silent-data-loss-defaults.md`).
- **Why not proposed yet**: the fix shape is not obvious — the cast path cannot
  simply block without reintroducing the wedge that
  `nonblocking-drop-nothing-backpressure` removed. Needs a design pass first.
- **A fix must**: decide what a caller is told when the buffer is full, without
  parking a host thread.

---

## P2 — gates that lie, and time sinks

### KI-4 · The format gate aborts before it checks Rust

- **Where**: `scripts/format-apply.sh` — the `need` preflight (line ~33) runs for
  every formatter *before* any formatter runs, and `cargo +nightly fmt` is line ~43.
- **What happens**: on a box missing one veneer formatter (commonly `rubocop`),
  the script fails loudly about that formatter and never reports that the Rust
  tree is dirty.
- **Evidence**: this is exactly how commit `d6ef9947` landed with six unformatted
  production files. The worker's "format-check.sh exits 0" was taken on trust.
- **A fix should**: warn-and-continue on a missing formatter, or run each
  formatter's check independently and report all failures, so one absent tool
  cannot mask another language's state.
- **Meanwhile**: run `cargo +nightly fmt --all --check` directly; it is read-only
  and takes seconds.

### KI-5 · `duplicate_pairs` depends on which areas are scanned

- **Where**: `scripts/refactor_metrics.py`.
- **What happens**: a cross-area duplicate pair is attributed to both areas, so
  the count depends on how many areas one invocation scans. `sdk/rust` reads
  **128** alone, **136** with two siblings, **178** whole-repo — on identical
  source.
- **Impact**: a baseline recorded with `--area` and checked against the whole-repo
  run would fire falsely. Today's baseline and `--check` both use the whole-repo
  scan, so the gate is self-consistent — but the per-area numbers quoted in
  reports are not comparable with it.
- **A fix should**: always scan the whole repo, or record the mode alongside the
  number and refuse to compare across modes.

### KI-6 · `ruff check` is red and no gate runs it

- **Where**: `sdk/python`; `scripts/format-check.sh` runs `ruff format` only.
- **What happens**: `ruff check` reports 68 findings at HEAD, 16 of them `I001`
  import ordering. Neither CI nor the local gate runs it.
- **A fix should**: decide whether `ruff check` joins the gate (and fix the 68) or
  is deliberately out of scope, and say which in the script.

### KI-7 · CI does not run the four veneer formatters

- **Where**: `.github/workflows/format.yml`.
- **What happens**: CI and `scripts/format-check.sh` no longer mirror each other
  for Node, Python, Ruby and PHP.

### KI-8 · `cargo +nightly fmt` is unpinned locally

- **Where**: `scripts/format-apply.sh` runs `cargo +nightly fmt`; CI pins
  `nightly-2026-05-07`. A local nightly bump silently diverges from CI.

### KI-9 · `.formatter.exs` misses two scripts

- **Where**: `inputs` omits `scripts/auto_restore_persistence_smoke.exs` and
  `bench/acceptance/summarize.exs`, so neither is ever formatted.

### KI-10 · Credo strict contradicts house style

- **Where**: `.credo.exs`. Strict reports 1349 issues, **967** of them
  `AliasUsage` and `ParenthesesOnZeroArityDefs`, both of which the codebase
  deliberately does not follow.
- **Blocks**: the credo count cannot be ratcheted until this is decided
  (disable those two checks, or adopt them and fix 967 sites). Area 5a owes the
  decision.

### KI-11 · `mvn verify` fails straight after `build-dist.sh`

- **Where**: the Java build. A plain `verify` after `scripts/build-dist.sh` dies
  at the shade step on a stale `target/`. The working command is `clean verify`.

### KI-12 · Veneer artifacts are debug builds by default

- **Where**: `scripts/build-{node,python,ruby,php}-sdk.sh` — `--release` is not
  the default, so a plain run leaves debug artifacts installed.
- **Impact**: a `--runtime <veneer>` benchmark under `FLEZHA_BENCH_SKIP_BUILD=1`
  then measures a debug veneer and reports it as a number. General to all four
  veneers (an earlier note blaming Python alone was wrong).

### KI-13 · The PHP container arm needs `--network host`, and leaks containers

- **Where**: the `FLEZHA_E2E_RUNTIME=php` shim.
- **What happens**: without `--network host` every `transport=quic` test fails
  with "temporary test runner not connected" while UDS passes. Separately, the
  shim leaks one container per wedge drill: the port kill reaches only the
  `docker run` client, and `wedge_probe`'s handler never returns.

### KI-14 · `flezha-php-zts` has no coverage driver

- **Where**: the PHP image — neither xdebug nor pcov, so `phpunit --coverage-text`
  cannot run in the container. Blocks any PHP coverage diff.

### KI-15 · The Ruby veneer needs an explicit bindgen include path here

- **Where**: this box lacks `libclang-common-*-dev`, so the Ruby build needs
  `BINDGEN_EXTRA_CLANG_ARGS=-I$(gcc -print-file-name=include)`. Handled in
  `scripts/build-ruby-sdk.sh`; recorded because the failure reads as a broken
  Ruby rather than a missing header.

### KI-16 · `mix deps.get` reports packages with security advisories

- Pre-existing, unaudited. Needs someone to read the advisories and decide.

---

## P2 — tests that lie or leak

### KI-17 · UDS socket paths overflow `sun_path`, and can fail a test

- **Where**: the system tier. A stream socket path of 114 bytes
  (`/tmp/flezha_sockets/<node>/secondary_stream_<namespace>_<ids>_wus.sock`)
  exceeds `sun_path`'s 108.
- **What happens**: 120 "Failed to start primary listener" plus 167 "Failed to
  connect runner" error lines per run, while the tests still pass — **and it can
  now fail one**: `schema_evolution_e2e_test.exs:65` on the python arm is
  preceded by `[flz_nif] UDS driver fatal error: path must be shorter than
  libc::sockaddr_un.sun_path`.
- **A fix must**: shorten the generated path, not raise the limit.

### KI-18 · Five load-sensitive tests, each seen once and green alone

Recorded so a future red run is not mistaken for a regression. **Green in
isolation is not evidence** — reproduce under load before concluding anything
(see `docs/refactor/STATE.md` for the stress-regime method).

- `test/flezha_test.exs:591` (runner disconnect, preserve) — 1/6 full runs,
  0/10 alone, 0/3 instrumented. Root cause never established.
- `workflow_crash_resume_e2e_test.exs:249` — **reproduced at
  `refactor-baseline-2`, so provably pre-existing**.
- `effectively_once_e2e_test.exs:34` — a different module from KI-2's second face.
- `test/plugin/kafka/worker_registry_test.exs:303` — a controller unit test with
  no runner involved.
- `flezha_test.exs:383` (live handover) — not the same as `:591`.

### KI-19 · Test hygiene: leaked temp dirs and DLQ leftovers

- 738 leaked `/tmp/flezha_*` directories (state-test and durability-stub roots
  never removed); `app_data/dlq` holds leftovers from tests using the default DLQ
  directory. Owed to the test-consolidation area.

### KI-20 · `sdk_smoke_e2e_test.exs` still carries the old drop workaround

- `send_until_forwarded` is a re-send loop written around KI-2. Fold it onto
  `NodeTestHelper.await_stream_bound/2` once KI-2 is fixed.

### KI-21 · Three uncaptured `[error]` lines in a plain `mix test`

- Two sdk_lang-mismatch restore refusals and one ControlUnit abnormal shutdown.
  Would trip CI's uncaptured-log gate on this suite shape.

### KI-22 · An inert drain in the output-stall drill

- Commit `220ba60c`'s `:sys.get_state(Loop)` drain is a no-op: a 200 000-iteration
  probe showed the `send_after(…, 0)` tick lands after the reply every time.

### KI-23 · `unique/1` suffixes can breach the operative-id cap

- Suffixes reach 7 digits within one BEAM, so any id prefix over 24 characters
  breaches the 32-byte operative-id cap late in a combined run.

### KI-24 · `sdk/rust/tests/ui/*.rs` must not be rustfmt-formatted

- Their line numbers are pinned by the `.stderr` snapshots, and `cargo fmt` never
  reaches them because trybuild compiles them out-of-tree — so a per-file
  `rustfmt` loop *does* reach them and breaks the snapshots. Exclude that
  directory from any formatting loop.

---

## P3 — warts, wording, and measurement notes

### KI-25 · A production `usleep` nothing states

- `sdk/php/src/Executor/SwooleExecutor.php:271` carries `usleep(5_000)`. The sleep
  metric counts test paths only, so no gate sees it. Probably legitimate and
  short and finite — but nothing says so.

### KI-26 · A stale doc block on `channel_metadata`

- `sdk/rust/src/operative/dispatcher/emit_sink.rs:344-350` documents a function
  that no longer exists, names the **retired** `Block` overflow policy, and
  carries a design-decision label the charter forbids.

### KI-27 · Specs and docs name things that do not exist

- `openspec/specs/controller-packaging/spec.md:270,290` still name
  `libflz_transport.{so,dylib,dll}` as committed binaries.
- `flz_jni` — a crate that does not exist — is named in `README.md:48`,
  `docker/dev/README.md:26` and four specs.
- `mix bench.roundtrip`'s moduledoc names `--runtime rust|java|elixir`; the
  manifests give it five more.
- `config/config.exs` documented a request sub-option as `{"name": …}`; it is
  `key`, and that invocation never worked.

### KI-28 · A cross-compile warning only the dist build surfaces

- `flz_core/src/control_unit.rs:19` — `unused_imports` on `socket_dir` when
  cross-compiled to `x86_64-pc-windows-gnu`; used only inside `#[cfg(unix)]`.

### KI-29 · Benchmark rows that cannot resolve what they are asked to

Measured floors on the reference box (`moon`, i7-8650U, `powersave`, on mains):

| Row | Floor | Verdict |
|---|---|---|
| Rust typed dispatch | 0.33 % | gate |
| Rust entity stress | 0.95 % | gate |
| `throughput_effectively_once_tiers` tier_off / tier_on | 9.04 % / 6.01 % | **direction only** |
| `host_loop` (erased) | 4.13 % | guard |
| whole-system round trip | ~18 % | **direction only** |
| veneer round trip (node) | 11.89 % | paired rounds only |

A row whose floor exceeds the effect being claimed is a direction, not a gate.
Do not run one as a gate and do not report its single-run number as a verdict.

### KI-30 · Three environment facts that cost hours if unknown

- `/proc/sys/kernel/perf_event_paranoid` is `4` here, which refuses **both**
  `perf record` and `perf stat`; valgrind is not installed. Instruction-level
  attribution is therefore impossible on this box — say "not attributable here"
  rather than digging.
- **An arm built in a `git worktree` is not comparable to one built in the
  tree.** A Rust binary is not byte-reproducible across build paths; a 60-second
  microbench read +3.5 % against a worktree arm and −3.5 % against a tree-built
  arm of the same source. Every decisive comparison must be between arms built
  the same way.
- The scratchpad is per session, so benchmark arms do not survive a new session
  unless they are rebuilt from a tag.

### KI-31 · Duplication the programme has chosen not to remove yet

- `test/apps/rust_runner/src/main.rs` duplicates the Rust bench's four-stage
  stress pipeline (four jscpd pairs) — owed to the test-apps area.
- The nine `convert_value` copies across SDKs are structural twins a 40-token
  window does not detect.
- Whole-repo `duplicate_pairs` rose 183 → 235 when three test masses became
  fifteen binaries, each repeating one `use` header.

---

## Older defects, found before the refactor programme

These predate `docs/refactor/STATE.md` and were carried only in session memory
until 2026-08-30. Dates are when each was last established, not when it started.

### KI-32 · The merge-blocking bench gate's own command does not complete

- **Severity**: P1 — a required CI gate that cannot pass.
- **Where**: `./scripts/bench.sh -n 100 -m 10000`, the exact command the
  `bench-regression` gate runs.
- **What happens**: it stalls partway and dies with
  `Timeout waiting for message <N>/1000000`; N landed at 647 899 / 675 886 /
  678 448 / 728 176 across four runs. At `-n 100 -m 2000` it still wedges
  (~160 285/200 000). At `-n 10 -m 500` it completes fine (~275 K msg/s).
- **The controller's own verdict**: `data-plane stall localized:
  layer=:driver_loop_frozen`, `%{transport: {:uds_data, "bench_<id>",
  :secondary}, loop_age_ms: :infinity}` — the secondary UDS stream's driver loop.
- **Proven pre-existing**: `lib/mix/tasks/bench.ex` and
  `test/apps/rust_runner/src/main.rs` were reverted to HEAD, the release runner
  rebuilt, and it still failed at 647 899/1 000 000.
- **Consequence**: a green *small* bench proves nothing about this gate, and the
  gate has never actually run to completion here.

### KI-33 · Java workflow resume re-runs committed producers

- **Severity**: P1 · **proposal owed — investigation first**.
- **Where**: `test/system/workflow_crash_resume_e2e_test.exs:147` — "a fan-in
  diamond joins once, and a crash re-runs only the uncommitted producer".
- **What happens**: **fails deterministically on both Java arms (uds and quic),
  3 of 3 runs.** The Rust arms pass. Expected 5 distinct payloads, gets 9:
  `[a, c, c, b, b, j:c, j:c, j:b, j:b]`. `a` runs once, so A's journal skip
  works — but B1/B2, C and the join each run **twice**. The resume re-runs
  committed producers instead of only the uncommitted one.
- **Why it matters**: that is an effectively-once violation on the Java SDK's
  resume path, not a test artifact.
- **Proven pre-existing**: swapping in the `java_runner.jar` blob from the git
  index (the build staged before `unify-plugin-sink-api`) reproduces it, so that
  change did not cause it.
- **Why no proposal yet**: the symptom is established and the mechanism is not.
  A proposal must start by root-causing which journal check the Java driver
  skips on resume, then fix it — writing a fix design now would be guessing.

### KI-34 · Java host dispatch costs 27–55 % more than Rust

- **Severity**: P2 · **proposal owed** (`java-host-dispatch-cost`).
- Measured during the cross-SDK perf-parity campaign (2026-08-22/23). The
  campaign delivered the *proof* — `mix bench.roundtrip --runtime rust|java|elixir`
  through the existing `Fleet` seam — and the gap it exposed was never closed.
- Nothing has been written; the number is the whole of what is known.

### KI-35 · `output_stall_drill_test:108` flakes ~40 % even alone

- **Severity**: P2.
- **Where**: `test/flezha/convergence/output_stall_drill_test.exs:108` —
  "re-flagging an already-recorded stall costs no extra tick".
- **The race**: the test calls `Watchdog.sweep_now()` then
  `refute_receive {:tick, _}, 200`. The convergence `Loop` also ticks on its own
  timer, so where the 200 ms window falls inside that interval decides the
  result. The assertion cannot distinguish a second sweep's tick from the
  periodic one.
- **Note**: it passed 1809/1809 in two full runs minutes before failing 2 of 5
  alone on the same tree. **"Green in isolation" does not apply to this one** —
  it is the counter-example to that heuristic.
- **A fix must**: give the assertion a way to tell the two ticks apart, not widen
  the window.

### KI-36 · The controller OOM fix was never validated under real load

- **Severity**: P1 for production, P2 here.
- **Background**: the 2026-06-19 controller OOM (2 GiB, exit 137, ~30 h outage)
  was reproduced and root-caused on 2026-06-21 — controller write buffers on the
  WAL persistence path with no ceiling. `WalWriter.append_async` is a
  fire-and-forget cast while `FsyncBatcher` coordinates fsync via a blocking
  call, so when the SDK ships state changes faster than the controller can fsync
  (the Pi's SD-card fsync being the bottleneck) the FsyncBatcher and
  per-partition WalWriter mailboxes grow without bound.
- **The gap**: fix `7393b70` has never been validated against the mock Kraken
  load that reproduced the original incident.

### KI-37 · SDK native binaries ship all five triples — provisional

- **Severity**: P2 · a deliberate, time-boxed choice.
- Every SDK ships all five prebuilt `libflz_core_ffi` triples inside its main
  artifact. This is deliberate while flezha is private and every consumer is the
  author — one artifact beats five.
- **The obligation**: before flezha goes public, or when rolling out the Python /
  Node / Ruby SDKs, switch to platform-specific distribution — per-triple
  Composer packages, wheels, `optionalDependencies` per os/cpu (the napi-rs
  convention), Java classifier artifacts. The risk being avoided is five ad-hoc
  packaging schemes invented independently.

### KI-38 · `process_hub` is pinned to a GitHub branch

- **Severity**: P2. The dependency is pinned to GitHub `dev` for the
  declared-children-and-remote-manifest work. Temporary; a hex release (0.6.2) is
  owed, and the pin must never regress to a GPL-licensed revision
  (`make refresh-process-hub` unlocks it).

### KI-39 · Open-source obligations, none met

- **Severity**: P2. Nothing is published yet. Owed before anything is: the
  `process_hub` hex release, a CLA bot, a `security@` contact, and the trademark
  question. Licensing itself is settled (MPL-2.0 controller, Apache-2.0 SDK side,
  recorded in `REUSE.toml`).

### KI-40 · CI does not run three test files or the kernel's own tests

- **Severity**: P2. CI skips `entity_timers`, `sugar_emit` and `simple_procedure`
  (all three need the `testing` feature) and **never runs `cargo test -p
  flz_core`** at all.

---

## Production (state as of June 2026, unverified since)

Recorded here because nothing else tracks them. Every one predates this
programme and none has been re-checked against the current deployment. Append
every new production freeze to `incident_log.md` at the repository root.

- **PROD FREEZE, hub-merge wedge** (2026-06-19) — `/healthz` reported healthy for
  30 hours while the data plane was wedged. Still open: BEAM backtrace capture on
  a wedge, a healthz check that sees a data-plane stall, and OOM-under-churn.
- **PROD OUTAGE, rpi4 node wedge** (2026-06-12) — single point of failure still
  open. Recovery is: power-cycle the rpi4, then `start-asset.sh`.
- **Convergence reliability audit fixes** — on `dev`, **not deployed**.
- **Recurring freeze from a self-inflicted channel kill** — fixed, **not
  deployed**.
- **`unified-convergence-loop` Phase 5** — 5.D zero-loss FAILED; 5.OW left six
  overwatch failures.
- **sol6 recurrence** — per-pod hostPath registry is a structural split-brain.

---

## Owed follow-up changes (big, not yet written)

### `sdk-declaration-refusals-in-the-core`

- **Severity**: P2 · **proposal owed**, scheduled after the Java SDK area.
- The core refuses 19 declaration rules that all four veneer SDKs *also* refuse,
  in different wording. The core's cycle, reachability and single-root checks are
  workflow-only — a stream class's DAG is checked by every SDK and never by the
  core — and two veneer rules have no core counterpart.
- **Why it is big**: moving them changes refusal texts across every SDK, so it is
  a cross-SDK change under `CLAUDE.md`'s one-change-every-SDK rule. It removes
  roughly 19 rules × 4 SDKs of duplicated logic.
- **One rule has already diverged**: correction requires *every* event-time clock
  in the SDK and *at least one* in the core.

### `WorkflowOperative::procedures` accepts a stream builder

- **Severity**: P3 · a latent authoring hole.
- `sdk/rust/src/workflow/mod.rs` hands a workflow class the **stream**
  `ChannelBuilder`, so a class can call `c.effectively_once()` or
  `c.correction_window(…)`. `build_spec` never refuses it, `WorkflowSpec` has no
  field for it, and nothing reads it — silently inert. The core refuses both
  (`flz_core/src/host/register.rs:110,115`).
- Fold into `sdk-declaration-refusals-in-the-core`.

### Macro attribute branches with no coverage anywhere

- **Severity**: P3.
- Five `flezha-macros` attribute branches are exercised by no test in the
  repository: `idle_ttl =`, `max_entities =`, `capacity =`, `on_full =`,
  `clock =` (`procedure.rs` and all three arms of `on_full_call`). A pass fixture
  is owed before anyone refactors those emission paths.

---

## Resolved

| Issue | Closed by |
|---|---|
| `cargo check --workspace --all-targets` red without a feature nobody passes | `a1bdfa34` — three `[[test]]` entries were missing `required-features` |
| `test/apps/node_runner/fixtures.mjs` a `SyntaxError` since `refactor(0a)`, so no Node e2e had run in four areas | `d6ef9947` |
| Node `Source::get` costing four N-API calls per field read (~4 % on the Node dispatch row) | `d6ef9947` |
| `refactor_metrics.py` measuring Rust functions short (`#` stripped as a comment, so `quote!` interpolations ended the brace count) | `5d53f7f5` |
| Three Rust SDK flakes, each with a mechanism rather than a retry | `623ba124` |
| Committed `.pyc` files and a missing `__pycache__` ignore | area 2 |
| `mix test --include system` "always exits 2" | refuted — the `:postgres_integration` modules self-skip now |
