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
- **Triage (2026-08-30)**: **REAL**, reproduces at defaults, and the blind spot
  is wider than filed. `enqueue_write` (`flz_nif/src/uds_driver.rs:474`) still
  drops on `would_overflow_write_buffer` and only bumps `write_buffer_dropped`;
  `send_frame` (`flz_nif/src/stream.rs:25`) still answers `atoms::ok()`
  unconditionally, and does not even check the `cmd_tx.send` result (`let _ =
  …`) — so a dead driver thread is a *second* drop path that is not counted at
  all. Correction to the entry: it is not only the caller that cannot see the
  loss. Nothing in `lib/` reads `write_buffer_dropped`.
  `TransportSource.emit_driver_stats/2`
  (`lib/flezha/observability/otp_backend/transport_source.ex:90`) emits
  `write_buffer_len` — the depth — and never the drop counter; no liveness
  verdict names it; the only reader in the tree is the printout at
  `lib/mix/tasks/bench/roundtrip.ex:698`. A production drop is invisible to the
  caller *and* to the platform.
- **Reproduction** — a UDS driver with a peer that connects and never reads. No
  env tuning: the default 8 MiB cap is enough.

  ```elixir
  # /tmp/ki3.exs
  alias Flezha.Transport.Native
  path = Path.join(System.tmp_dir!(), "ki3_#{System.unique_integer([:positive])}.sock")
  {:ok, server} = Native.start_uds_server(path, self())
  # A peer that connects and NEVER reads.
  {:ok, _s} = :gen_tcp.connect({:local, String.to_charlist(path)}, 0, [:binary, active: false, packet: :raw])
  stream = receive do: ({:flz_connection, s} -> s), after: (5_000 -> raise "no connection")
  payload = :binary.copy("x", 64 * 1024)
  replies = for i <- 1..400, do: Native.send_frame(stream, "p#{i}", payload, %{})
  Process.sleep(500)
  st = hd(Native.driver_stats(server).streams)
  IO.puts("send_frame replies: #{inspect(Enum.uniq(replies))} over #{length(replies)} calls")
  IO.puts("frames_written=#{st.frames_written} write_buffer_dropped=#{st.write_buffer_dropped}")
  Native.stop_server(server)
  ```

  ```
  $ MIX_ENV=test mix run --no-start /tmp/ki3.exs
  send_frame replies: [:ok] over 400 calls
  frames_written=131 write_buffer_dropped=269
  ```

  269 of 400 frames dropped; all 400 calls answered `:ok`. **Control** (proving
  the probe can report a clean result): the same script under
  `FLEZHA_MAX_WRITE_BUFFER_BYTES=1073741824` prints `frames_written=400
  write_buffer_dropped=0`.
- **Worth fixing**: yes. "Investigation first" still stands for the real
  question — what a caster is told — but two things need no design pass and end
  the silence on their own: emit `write_buffer_dropped` from `TransportSource`
  beside `driver_write_buffer_bytes`, and give the stall classifier a clause on
  it. Loud loss is not the fix, but it is strictly better than this.

### KI-41 · A re-entrant outbox window advances the watermark before the park it is dispatching is durable

- **Severity**: P1 · **investigation first**.
- **Where**: `lib/flezha/state/wal_writer.ex` — `take_outbox_window/3` calls
  `await_entry/4` *before* `apply_effect/2` for the same entry.
- **What happens**: an entry with no outputs completes inside `await_entry/4`
  (tally `pending: 0`), so `complete_entry({:cursor, seq})` re-enters
  `try_dispatch_outbox/1` and a nested window dispatches the *next* record and
  reports its consumed-offset — all before the outer entry's own
  `apply_effect/2` runs. A zero-output entry is exactly what a dead-letter park
  produces, and `apply_effect/2`'s own comment states the required order: "the
  watermark move can truncate the poison input out of the input-log, so the DLQ
  store entry must be durable first". Under re-entry that order inverts.
  `drain_confirmed/1` likewise advances the in-memory `outbox_flushed_seq` past
  the park before the park is stored.
- **Evidence**: a probe on `dispatch_outbox_window/2`, `take_outbox_window/3`,
  `apply_effect/2` and `advance_consumed/2`, driven by
  `test/flezha/state/wal_writer_outbox_test.exs` "a nested window's advance
  survives the outer window closing", printed exactly this order:
  `take seq=0` → `complete_entry {:cursor, 0}` → nested `window-open` →
  `take seq=1` → `apply_effect seq=1 offset=2` → `record_consumed 2` →
  `apply_effect seq=0 dlq=true` (the park) → outer `record_consumed`.
- **Impact**: the exposure is the few microseconds between the nested window's
  `record_consumed/3` and the outer entry's park write, inside one `do_fsync/1`
  on one process. A retention sweep landing in that window could truncate the
  poison input while the DLQ entry is not yet stored. A crash in the window is
  safe: the boot re-drive re-reads the WAL and `apply_effect/2` is idempotent.
  `persist_dedup/1` runs at the end of the same `do_fsync/1`, after the park, so
  the *durable* cursor is never inverted.
- **Why not fixed here**: found while instrumenting
  `fix-outbox-watermark-overwrite` (its task 1.2, the design's Open Question).
  That change's own Non-Goals rule the re-entrancy out of scope — reshaping
  `complete_entry/2`'s control flow belongs to `refactor-5a-state` — and this is
  an ordering inversion, not the double-write that change fixes.
- **A fix must**: run an entry's `apply_effect/2` before anything can complete
  it, or stop `complete_entry/2` re-entering dispatch while a window is open.
  Note the two are the same seam `refactor-5a-state` will restructure.
- **Triage (2026-08-30)**: **REAL** — the ordering is unchanged at HEAD and
  reproduces deterministically — **but the impact claim is NOT confirmed, and no
  loss was constructible.** `fix-outbox-watermark-overwrite` landed as
  `9410eea7` ("fix(state): the consumed watermark stops moving backwards", an
  ancestor of HEAD). It clamped `InputLog.record_consumed/3` monotonically and
  did not touch `take_outbox_window/3`, which still calls `await_entry/4` at
  `lib/flezha/state/wal_writer.ex:1058` and `apply_effect/2` at `:1060`. The
  trigger is precisely a **zero-output** entry: `await_entry/4` →
  `register_entry/4` ends in `tally(key, & &1)`, and `complete?/1` holds on
  `pending: 0`, so `complete_entry({:cursor, seq})` fires inside the very call
  that registered the entry.
- **Reproduction** — a local trace over the functions the entry names, driven by
  the two-record batch it describes. Write the probe to the scratchpad and run
  it with `mix test` (it needs `test/helpers`); it is a probe, not a repo test.

  ```elixir
  # /tmp/ki41_probe_test.exs — setup copied from
  # test/flezha/state/wal_writer_outbox_test.exs (tmp_app_data_dir,
  # start_state_tree, a per-test dlq dir, effect_header/effect_body/output).
  :erlang.trace_pattern({Flezha.State.WalWriter, :take_outbox_window, 3}, true, [:local])
  :erlang.trace_pattern({Flezha.State.WalWriter, :apply_effect, 2}, true, [:local])
  :erlang.trace_pattern({Flezha.State.WalWriter, :advance_consumed, 2}, true, [:local])
  :erlang.trace_pattern({Flezha.State.WalWriter, :complete_entry, 2}, true, [:local])
  :erlang.trace_pattern({Flezha.State.WalWriter, :dispatch_outbox_window, 2}, true, [:local])
  :erlang.trace_pattern({Flezha.State.InputLog, :record_consumed, 3}, true, [:local])
  :erlang.trace_pattern({Flezha.Dlq.Store, :append, 1}, true, [:local])
  :erlang.trace(:all, true, [:call, {:tracer, collector}])

  in_one_window(fn ->
    # seq 0: a park — ZERO outputs, consumed_offset 1.
    :ok = WalWriter.append(pid, 0, effect_header(),
            effect_body([], 1, %{channel: "in", error: "poison", attempts: 3, payload: "kp:poison"}))
    # seq 1: a normal record, consumed_offset 2, durable in the same batch.
    :ok = WalWriter.append(pid, 1, effect_header(), effect_body([output("k2", "b")], 2))
  end)

  :ok = WalWriter.fsync(pid)
  ```

  ```
  $ MIX_ENV=test mix test /tmp/ki41_probe_test.exs
  ===== KI-41 OBSERVED ORDER =====
    dispatch_outbox_window
    take_outbox_window(chunks=0, consumed=-1)
    complete_entry({:cursor, 0})
    dispatch_outbox_window
    take_outbox_window(chunks=0, consumed=-1)
    apply_effect(seq=1 offset=2 dlq=false)
    take_outbox_window(chunks=1, consumed=2)
    advance_consumed(2)
    InputLog.record_consumed(2)
    apply_effect(seq=0 offset=1 dlq=true)
    Dlq.Store.append(origin_offset=1)  <-- THE PARK
    take_outbox_window(chunks=1, consumed=1)
    advance_consumed(1)
    InputLog.record_consumed(1)
  ===== watermark = 2 =====
  >>> KI-41 CONFIRMED: the watermark advanced BEFORE the park was stored
  ```

  This is the entry's trace, function for function. **Control**: give the park
  record ONE output and the same probe prints `apply_effect(seq=0 dlq=true)` →
  `Dlq.Store.append(origin_offset=1)` → … → `InputLog.record_consumed(2)`, i.e.
  `>>> KI-41 NOT OBSERVED — the park was stored first, as intended`. Zero
  outputs is the entire trigger, and the probe can report clean.
- **NOT established — no loss follows from the inversion.** Three facts close
  every path this triage could find. (1) `take_outbox_window/3` only admits
  entries at `seq <= outbox_durable_seq`, so the park's **effect record is
  already fsynced** before any of this runs, and the boot re-drive re-appends
  the DLQ entry (`test/flezha/state/wal_writer_outbox_test.exs` "re-drive of a
  park record is idempotent" pins it). (2) `persist_dedup/1` is never reachable
  inside the nesting — every call site (`:798`, `:862`, `:920`) runs it *after*
  `try_dispatch_outbox/1` returns, by which time the park is written — so the
  **durable** re-drive floor is never inverted. (3) The truncation the entry
  fears costs nothing: `Dlq.Store` carries the park's own `payload`
  (`lib/flezha/dlq/store.ex:340`), and a replay never reads the input log, so
  losing the poison input at that offset loses no recoverable data. The exposure
  is therefore a few microseconds of an **in-memory ETS watermark** being ahead
  of a park that is independently recoverable.
- **Worth fixing**: yes, but as an ordering invariant, **not** as silent data
  loss — this should drop out of the P1 band. It stays worth doing because the
  invariant `apply_effect/2` states in its own comment is one a future reader
  will rely on, and re-entrant `complete_entry/2` makes it locally unreadable.
  Fold it into `refactor-5a-state` as the entry already says; it does not
  justify a change of its own.

---

### KI-45 · `PrometheusRegistry` re-encodes a metric label the same way the OTLP codec did

- **Severity**: P2 · **proposal owed** (small).
- **Where**: `lib/flezha/observability/export/prometheus_registry.ex:192` —
  `Enum.map(fn {k, v} -> {to_string(k), to_string(v)} end)` over a metric
  signal's metadata.
- **What happens**: the shape `close-otlp-export-crash` removed from
  `OtlpCodec`, still standing on the other exporter. `to_string/1` raises
  `Protocol.UndefinedError` for a map, a tuple or a pid, which would take
  `PrometheusRegistry` down inside `record/1`; and for a list it returns the
  binary of the elements, so a charlist or byte-list label is silently
  rewritten on the exposition.
- **Why it is not the same severity**: `PrometheusSink.filter/0` is
  `%{kinds: [:metric], sources: :any}`
  (`lib/flezha/observability/export/prometheus_sink.ex:36`), so the Logger
  bridge's map and tuple — which reach *every* OTLP log signal — never arrive
  here. Only a metric emitted with a non-scalar metadata value does, and every
  metric source in the tree emits scalars today. It is a live crash waiting on
  one new source, not one that fires now.
- **Evidence**: measured on `0c4a695a` alongside KI-44's reproduction; the
  filter and the `to_string` pair are read directly off the two files above.
  **Not driven**: no signal was pushed through `PrometheusRegistry.record/1`
  with a map-valued label, so the crash is inferred from `to_string/1`'s
  contract, not observed.
- **Why not fixed with KI-44**: it is not the same fix. Prometheus label
  *names* must match `[a-zA-Z_][a-zA-Z0-9_]*`, so coercing a tuple key to
  `"{:a, :b}"` produces a well-formed string and a malformed exposition — the
  Prometheus side needs a name rule as well as a value rule, and
  `escape_label_value/1` already owns part of that question.
- **A fix must**: decide the label-name rule first (refuse, slugify, or drop
  the label), then reuse `OtlpCodec.attribute_string/1`'s decision for the
  value — a binary unchanged, scalars via `String.Chars`, everything else
  inspected. `lib/flezha/observability/export/otlp_client.ex:147` has the same
  two-`to_string` shape and is **not** part of this: those are operator-supplied
  `:otlp_headers` from config, not telemetry.

---

## P2 — gates that lie, and time sinks

### KI-4 · The format gate aborts before it checks Rust

- **Where**: `scripts/format-apply.sh` — the `need` preflight (line ~33) runs for
  every formatter *before* any formatter runs, and `cargo +nightly fmt` is line ~43.
- **What happens**: on a box missing one veneer formatter (commonly `rubocop`),
  the script fails loudly about that formatter and never reports that the Rust
  tree is dirty.
- **Evidence**: this is exactly how commit `d6f6e16f` landed with six unformatted
  production files. The worker's "format-check.sh exits 0" was taken on trust.
- **A fix should**: warn-and-continue on a missing formatter, or run each
  formatter's check independently and report all failures, so one absent tool
  cannot mask another language's state.
- **Meanwhile**: run `cargo +nightly fmt --all --check` directly; it is read-only
  and takes seconds.
- **Triage (2026-08-30)**: **REAL**, and it reproduces on this box today with no
  setup at all — `rubocop` is simply not on `PATH` here. The ordering is
  unchanged: the `need` preflight is `scripts/format-apply.sh:28-35`, the first
  formatter is line 43. The cited commit `d6f6e16f` still resolves and is
  reachable from `dev` (`refactor(2b): one shape reads every veneer's
  activations and results`), so the history rewrite did not orphan it.
- **Reproduction** — the gate as the charter invokes it, unmodified:

  ```
  $ scripts/format-apply.sh --check
  missing rubocop — install the development dependency in sdk/ruby/flezha.gemspec
  $ echo $?
  1
  ```

  Not one `==> ` line was printed, so no formatter ran. **Control** (proving the
  Rust half has something to say and is simply never asked): run directly,
  `cargo +nightly-2026-05-07 fmt --all -- --check` exits **0** with empty output
  — the tree is clean at HEAD, which the gate above neither knows nor reports.
- **Correction to the entry**: the "Meanwhile" command is not valid. `cargo fmt`
  passes flags after `--`; `cargo +nightly fmt --all --check` is rejected. The
  working form is `cargo +nightly-2026-05-07 fmt --all -- --check`.
- **Worth fixing**: yes, and it is small — run each formatter's check in a
  subshell, collect the failures, and exit non-zero at the end. The preflight can
  stay as a warning. Group with KI-7 and KI-8: all three are one script and one
  workflow.

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
- **Triage (2026-08-30)**: **REAL**; the mechanism is unchanged and the numbers
  are stale. `jscpd_pairs` (`scripts/refactor_metrics.py:397`) is handed only the
  roots of the areas this invocation scans (line 525), so a pair whose two halves
  straddle a scanned and an unscanned area is invisible.
- **Reproduction** — three runs, same tree, same commit:

  ```
  $ python3 scripts/refactor_metrics.py --area sdk/rust --json | jq '.areas["sdk/rust"].duplicate_pairs'
  169
  $ python3 scripts/refactor_metrics.py --area sdk/rust --area sdk/java --area sdk/node --json | ...
  169
  $ python3 scripts/refactor_metrics.py --json | jq '.areas["sdk/rust"].duplicate_pairs'
  215
  ```

  Diffing the two pair LISTS: 46 pairs are in the whole-repo run and not the
  area-only one, **and none the other way** — 42 of the 46 pair `sdk/rust`
  against `test/apps`, the rest against `flz_core`, `flz_core_ffi` and
  `flz_transport`. The entry's "136 with two siblings" does not reproduce with
  *those* siblings: `sdk/java` and `sdk/node` share nothing with `sdk/rust`, so
  they add zero. Which siblings you scan is exactly the point.
- **Correction to the entry**: the numbers are 169 / 169 / 215, not 128 / 136 /
  178, and the relation is **monotone** — a per-area count is always an
  undercount, never an overcount. So a per-area baseline checked against the
  whole-repo run fires falsely (the direction the entry gives), and the reverse
  silently misses a regression. `naming.yml:85` runs `--check` with no `--area`,
  and `baseline.json` was written the same way, so the gate itself is consistent.
- **Worth fixing**: yes, cheaply — record the scan mode in the JSON and refuse to
  compare across modes. The per-area invocation is genuinely useful for a worker
  looking at one area; what must not happen is a per-area number being quoted
  next to a baseline number as though they were the same measurement.

### KI-6 · `ruff check` is red and no gate runs it

- **Where**: `sdk/python`; `scripts/format-check.sh` runs `ruff format` only.
- **What happens**: `ruff check` reports 68 findings at HEAD, 16 of them `I001`
  import ordering. Neither CI nor the local gate runs it.
- **A fix should**: decide whether `ruff check` joins the gate (and fix the 68) or
  is deliberately out of scope, and say which in the script.
- **Triage (2026-08-30)**: **REAL**; the count has drifted up. Still no gate:
  `format-apply.sh:49` runs `ruff format` and nothing runs `ruff check`, in the
  script or in any workflow (see KI-7's grep, which covers ruff too).
- **Reproduction**:

  ```
  $ sdk/python/.venv/bin/ruff check --statistics sdk/python test/apps/python_runner
   17  I001    [*] unsorted-imports
   16  RUF022  [-] unsorted-dunder-all
    9  UP035   [*] deprecated-import
    7  RUF023  [*] unsorted-dunder-slots
    5  B017    [ ] assert-raises-exception
    4  BLE001  [ ] blind-except
    4  UP017   [*] datetime-timezone-utc
    3  DTZ005  [ ] call-datetime-now-without-tzinfo
  … 19 rules in all
  Found 77 errors.
  [*] 55 fixable with the `--fix` option
  $ echo $?
  1
  ```
- **Correction to the entry**: **77** findings, not 68; **17** are `I001`, not 16.
  55 of the 77 are auto-fixable.
- **Worth fixing**: yes — as a decision, not as 77 edits. Four rules
  (`I001`/`RUF022`/`UP035`/`RUF023`, 49 findings) are mechanical and fixable in
  one pass; `DTZ005`/`DTZ001` are the ambient-clock family the Python SDK
  deliberately guards against and are worth reading rather than silencing. Either
  outcome must be written into `format-apply.sh` so the next reader knows `ruff
  check` was considered and not merely forgotten.

### KI-7 · CI does not run the four veneer formatters

- **Where**: `.github/workflows/format.yml`.
- **What happens**: CI and `scripts/format-check.sh` no longer mirror each other
  for Node, Python, Ruby and PHP.
- **Triage (2026-08-30)**: **REAL**, unchanged, and it is total — no workflow
  runs any of the four, not `format.yml` and not the four veneer SDK workflows.
- **Reproduction**:

  ```
  $ grep -rniE "prettier|rubocop|php-cs-fixer|ruff" .github/workflows/
  $ echo $?
  1
  ```

  **Control** (proving the grep can hit): `grep -rl spotless .github/workflows/`
  → `.github/workflows/format.yml`. So the absence is real, not a bad pattern.
  `php-sdk.yml`'s "Lint every source file" step is `php -l` — a syntax check, not
  php-cs-fixer.
- **Worth fixing**: yes. This is the load-bearing half of KI-4: `format-check.sh`
  is the only thing that ever runs these four, and KI-4 is the reason it can
  silently not run them. One job, four steps, each installing that SDK's pinned
  dev dependency — the veneer workflows already install every one of those
  toolchains for their own build steps.

### KI-8 · `cargo +nightly fmt` is unpinned locally

- **Where**: `scripts/format-apply.sh` runs `cargo +nightly fmt`; CI pins
  `nightly-2026-05-07`. A local nightly bump silently diverges from CI.
- **Triage (2026-08-30)**: **REAL but currently inert.** `format-apply.sh:43` is
  still `cargo +nightly fmt`; `format.yml:115` still pins
  `nightly-2026-05-07`. On this box the two are genuinely different builds.
- **Reproduction**:

  ```
  $ rustup run nightly rustc --version
  rustc 1.97.0-nightly (f964de49b 2026-05-07)
  $ rustup run nightly-2026-05-07 rustc --version
  rustc 1.97.0-nightly (365c0e1d7 2026-05-06)
  ```

  But the divergence buys nothing today: both `cargo +nightly fmt --all --
  --check` and `cargo +nightly-2026-05-07 fmt --all -- --check` exit **0** with
  byte-identical (empty) output on this tree. The risk is real, the realisation
  is zero.
- **Worth fixing**: yes, and it is one word — `cargo +nightly-2026-05-07` in the
  script, so the pin lives in one place. Do it inside the KI-4/KI-7 change; it
  does not justify a change of its own.

### KI-9 · `.formatter.exs` misses two scripts

- **Where**: `inputs` omits `scripts/auto_restore_persistence_smoke.exs` and
  `bench/acceptance/summarize.exs`, so neither is ever formatted.
- **Triage (2026-08-30)**: **REAL**, and both named files are in fact unformatted
  right now — so this is not merely "never formatted", it is "and it shows".
- **Reproduction**:

  ```
  $ mix format --check-formatted .credo.exs bench/acceptance/summarize.exs \
      examples/plugins/hello_world/manifest.exs scripts/auto_restore_persistence_smoke.exs
  ** (Mix) mix format failed due to --check-formatted.
  The following files are not formatted:

    /home/moon/Code/flezha/bench/acceptance/summarize.exs
    /home/moon/Code/flezha/scripts/auto_restore_persistence_smoke.exs
  $ echo $?
  1
  ```

  `summarize.exs` alone would take ~150 lines of reflow. **Control**: the same
  invocation reports the other two files as clean, so the check discriminates.
- **Correction to the entry**: there are **four** tracked `.exs` outside
  `inputs`, not two — `.credo.exs` and
  `examples/plugins/hello_world/manifest.exs` are the other two, and both happen
  to be formatted already. `inputs` is `["{mix,.formatter}.exs",
  "{config,lib,test}/**/*.{ex,exs}"]`.
- **Worth fixing**: yes, one line — but note that widening `inputs` makes CI's
  `mix format --check-formatted` go **red** until those two files are formatted,
  so the widening and the reflow are one commit, not two. Landing it in the same
  change as KI-4/KI-7/KI-8 keeps every format-gate edit together.

### KI-10 · Credo strict contradicts house style

- **Where**: `.credo.exs`. Strict reports 1349 issues, **967** of them
  `AliasUsage` and `ParenthesesOnZeroArityDefs`, both of which the codebase
  deliberately does not follow.
- **Blocks**: the credo count cannot be ratcheted until this is decided
  (disable those two checks, or adopt them and fix 967 sites). Area 5a owes the
  decision.
- **Triage (2026-08-30)**: **REAL**, the shape is exactly as filed and the
  numbers have grown. Nothing has decided this; the ratchet is still absent.
- **Reproduction**:

  ```
  $ mix credo --strict
  7156 mods/funs, found 13 warnings, 215 refactoring opportunities,
  569 code readability issues, 664 software design suggestions.
  $ echo $?
  30
  ```

  1461 in total, by message:

  ```
    653  Nested modules could be aliased at the top of the invoking module.   (AliasUsage)
    430  Do not use parentheses when defining a function which has no arguments.
    125  Function body is nested too deep (max depth is 2, was 3).
     37  Prefer using an implicit `try` rather than explicit `try`.
     30  Cond statements should contain at least two conditions besides `true`.
  … 74 distinct messages in all
  ```

  The ratchet claim holds and is checkable: every one of the 36 areas in
  `docs/refactor/baseline.json` carries `"credo_warnings": null`, so no area
  compares this metric at all.
- **Correction to the entry**: **1461** issues, not 1349; the two contested
  checks are **1083** of them (653 + 430), not 967 — 74 % of the count.
- **Worth fixing**: yes, but as a **decision**, and the decision is nearly made
  for it. Adopting either check means editing ~1083 sites against the house
  style the codebase already chose deliberately; disabling both leaves 378
  issues, of which 139 are real nesting depth and the rest are a readable tail.
  Recommend: disable `AliasUsage` and `ParenthesesOnZeroArityDefs` in
  `.credo.exs` with a one-line reason each, then ratchet on the 378. Until then
  the count is not a signal and should not be quoted as one.

### KI-12 · Veneer artifacts are debug builds by default

- **Where**: `scripts/build-{node,python,ruby,php}-sdk.sh` — `--release` is not
  the default, so a plain run leaves debug artifacts installed.
- **Impact**: a `--runtime <veneer>` benchmark under `FLEZHA_BENCH_SKIP_BUILD=1`
  then measures a debug veneer and reports it as a number. General to all four
  veneers (an earlier note blaming Python alone was wrong).
- **Triage (2026-08-30)**: the mechanism is **true** and the impact is **stale**
  — the hole it describes has been closed twice over since it was filed.
  Confirmed still true: all four of `scripts/build-{node,python,ruby,php}-sdk.sh`
  set `profile=debug` and only `--release` changes it. But each script's own
  header now states this as a decision, in the same words in all four — "`--release`
  builds the veneer in release, which is what the bench profile wants; the
  default is a debug veneer, which is what the e2e wants".
- **Reproduction of the mitigation**, which is what actually matters here: every
  veneer's bench build line already passes `--release`.

  ```
  $ for m in test/apps/*/conformance.json; do python3 -c "…print(json.load(open('$m'))['build'])"; done
  node   … 'bench': 'scripts/build-node-sdk.sh --release'
  php    … 'bench': 'scripts/build-php-sdk.sh --release'
  python … 'bench': 'scripts/build-python-sdk.sh --release'
  ruby   … 'bench': 'scripts/build-ruby-sdk.sh --release'
  ```

  And the one path that skips the build now says so in the report:
  `lib/mix/tasks/bench/report.ex:473-478` emits "FLEZHA_BENCH_SKIP_BUILD is set —
  the artifact measured is whatever was on disk, which is not necessarily the
  tree you are in".
- **Worth fixing**: **no.** The debug default is deliberate and documented, the
  bench profile overrides it, and the residual case is announced in the report
  header. The entry's "reports it as a number" is no longer true. Keep the fact
  — it is a real trap for anyone reading a stale artifact — but demote it out of
  the defect list.

### KI-13 · The PHP container arm needs `--network host`, and leaks containers

- **Where**: the `FLEZHA_E2E_RUNTIME=php` shim.
- **What happens**: without `--network host` every `transport=quic` test fails
  with "temporary test runner not connected" while UDS passes. Separately, the
  shim leaks one container per wedge drill: the port kill reaches only the
  `docker run` client, and `wedge_probe`'s handler never returns.
- **Triage (2026-08-30)**: true as recorded, but it is **not a defect in this
  repository** and I could **not reproduce it**. The shim is not tracked: the
  manifest's launch line is a bare `php` (`test/apps/php_runner/conformance.json`
  → `command.test`), and there is no PHP on this box at all (`command -v php` →
  absent), so the container arm exists only as a `php` shim an individual puts on
  `PATH`. Both facts are already written down in
  `docs/dev_notes/refactor-2-ffi-veneers.md:208-230` (with the QUIC signature —
  "seven failures, all QUIC" — and the `-i` flag the entry omits) and in
  `docs/refactor/STATE.md` under 2e.
- **What I tried**: reproducing either half needs the shim built plus a full
  `FLEZHA_E2E_RUNTIME=php transport=quic` conformance arm, and the leak half also
  needs the wedge drill. I did not run it. I did confirm the fallout is not
  present now: `docker ps` lists **no containers**, so nothing is leaked in the
  current tree.
- **Worth fixing**: **not as a defect** — there is no repo code to change. Two
  things are worth doing, both cheap: fold the shim's exact `docker run` line
  (with `--network host` and `-i`) into `docs/sdk/conformance.md` beside the Ruby
  `PATH` note, so it is findable from where someone runs the suite rather than
  from a refactor dev note; and if the shim ever becomes a tracked script, give
  it a `--rm`-plus-name teardown so the wedge drill's container cannot survive
  the run. Until then this is an environment note, like KI-15.

### KI-14 · `flezha-php-zts` has no coverage driver

- **Where**: the PHP image — neither xdebug nor pcov, so `phpunit --coverage-text`
  cannot run in the container. Blocks any PHP coverage diff.
- **Triage (2026-08-30)**: **REAL**, reproduced, and true of both images.
- **Reproduction**:

  ```
  $ docker run --rm -u $(id -u):$(id -g) -v $PWD:/work -w /work/sdk/php \
      flezha-php-zts php vendor/bin/phpunit --coverage-text
  PHPUnit 10.5.64 by Sebastian Bergmann and contributors.
  Runtime:       PHP 8.4.25
  ...............................................................  63 / 129 ( 48%)
  ..........S.................................................... 126 / 129 ( 97%)
  ...                                                             129 / 129 (100%)

  There was 1 PHPUnit test runner warning:

  1) No code coverage driver available

  OK, but there were issues!
  Tests: 129, Assertions: 1284, PHPUnit Warnings: 1, Skipped: 1.
  ```

  `php -m` lists neither `xdebug` nor `pcov` in `flezha-php-zts` **or**
  `flezha-php`. The tests themselves run fine; only coverage is impossible.
- **A dead end worth recording**: `phpdbg` *is* present at
  `/usr/local/bin/phpdbg` in the ZTS image, but it is not a way out —
  `phpunit/php-code-coverage` 10.1.16 (under PHPUnit 10.5.64) dropped phpdbg
  support, so only Xdebug or PCOV will do.
- **Worth fixing**: yes, and it is one line in the image — `pecl install pcov`
  plus `extension=pcov.so`, which costs far less runtime than Xdebug. It matters
  because a coverage diff is how this repo decides test deletions
  (`docs/dev_notes/refactor-1-kernel.md`), and PHP is the one SDK that cannot
  produce one. Small enough to ride along with any other PHP image change.

### KI-15 · The Ruby veneer needs an explicit bindgen include path here

- **Where**: this box lacks `libclang-common-*-dev`, so the Ruby build needs
  `BINDGEN_EXTRA_CLANG_ARGS=-I$(gcc -print-file-name=include)`. Handled in
  `scripts/build-ruby-sdk.sh`; recorded because the failure reads as a broken
  Ruby rather than a missing header.
- **Triage (2026-08-30)**: **REAL**, reproduced exactly — and the entry
  understates it: the guard covers the build script only, so every **direct**
  cargo invocation on a box like this one still dies.
- **Reproduction** — the box's premise first: no `libclang-common-*-dev` is
  installed and `/usr/lib/llvm-*/lib/clang/*/include/stdarg.h` does not exist,
  while `$(gcc -print-file-name=include)/stdarg.h` does. Then, with the SDK's
  own Ruby on `PATH` and `BINDGEN_EXTRA_CLANG_ARGS` unset:

  ```
  $ PATH="$HOME/.rbenv/versions/3.4.10/bin:$PATH" \
    env -u BINDGEN_EXTRA_CLANG_ARGS cargo build -p flz_core_ffi --features ruby
  error: failed to run custom build command for `rb-sys v0.9.130`
    /home/moon/.rbenv/versions/3.4.10/include/ruby-3.4.0/ruby/ruby.h:23:10:
        fatal error: 'stdarg.h' file not found
  exit 101
  ```

  `scripts/build-ruby-sdk.sh:23-33` exports the include path and the same build
  then succeeds — but nothing else does. `cargo test -p flz_core_ffi --features
  ruby-embed`, which `ruby-sdk.yml:121` runs and which the refactor programme
  runs directly, never passes through that script.
- **A trap the entry does not name**: with the *system* Ruby (3.2.3, no
  `ruby-dev`) the build fails earlier and differently — `'ruby.h' file not
  found` — which hides the stdarg.h symptom entirely and sends the reader after
  the wrong header.
- **Worth fixing**: yes, small — move the guard out of the script into
  `flz_core_ffi/build.rs` (or a `.cargo/config.toml` env entry), so any cargo
  invocation of the ruby features gets it. As it stands the mitigation protects
  the one caller that was already known to need it.

### KI-16 · `mix deps.get` reports packages with security advisories

- Pre-existing, unaudited. Needs someone to read the advisories and decide.
- **Triage (2026-08-30)**: **REAL**, and now audited — which is exactly what the
  entry asked for. There is **one** advisory, not several, and this tree is not
  exposed to it.
- **Reproduction**:

  ```
  $ mix hex.audit
  Advisories:
    postgrex 0.22.3 - EEF-CVE-2026-66838 (MEDIUM)
      aka: CVE-2026-66838, GHSA-3gww-3f36-2388
      SQL injection via the :comment option in Postgrex.stream/4
      https://osv.dev/vulnerability/EEF-CVE-2026-66838

  Found packages with security advisories
  ```
- **Exposure: none.** The vulnerable call is `Postgrex.stream/4` with a
  `:comment` option. `lib/` never calls `Postgrex.stream` and never passes
  `:comment` anywhere — the two Postgrex callers use `Postgrex.query/4`
  (`lib/flezha/state/durability_tier/backend/postgres.ex:178`),
  `Postgrex.child_spec/1` (`:48`) and `Postgrex.start_link/1`
  (`lib/flezha/plugin/provider/postgres/sink_worker.ex:126`), plus an
  `after_connect: {Postgrex, :query!, …}` at `:185`.
- **Worth fixing**: yes, and it is a lock bump, not a change. `mix.exs` already
  declares `{:postgrex, "~> 0.22"}`, so a patched 0.22.x is admissible without
  touching the constraint; `mix.lock` pins 0.22.3. Do it the next time deps are
  touched. The entry's "needs someone to read the advisories and decide" is now
  discharged: read, one advisory, not exploitable here, bump when convenient.

### KI-48 · `ControlUnit` exits abnormally whenever a runner tree comes down

- **Severity**: P2 · **proposal owed** — the fix is a production behaviour
  change, so it was diagnosed and left alone by `green-the-suite-log-gate`
  (that change's D2 says a defect landing on the production side is brought back
  rather than folded in).
- **Where**: `lib/flezha/runner/runner_supervisor.ex:18`–`:39` (the child order)
  and `lib/flezha/runner/control_unit.ex` (every `handle_call` that calls
  `ControlChannel.send_control/2` — `:220`, `:271`).
- **What happens**: `RunnerSupervisor` lists `ControlChannel` before
  `ControlUnit`, so its `:one_for_one` terminate sweep — reverse order — stops
  the **channel first**. A `ControlUnit` sitting in
  `GenServer.call(control_channel, {:send_control, …}, 5000)` at that moment
  exits with `{:shutdown, {GenServer, :call, …}}`. OTP's `supervisor:shutdown/2`
  accepts only a bare `shutdown`, so the tuple is `{error, Reason}` and
  `do_terminate/2` reports `shutdown_error` at `[error]`:

  ```
  [error] Child Flezha.Runner.ControlUnit of Supervisor
    {Flezha.Service.Identificator, "test_temp_namespace_37722754971_wus"}
    shut down abnormally
  ** (exit) exited in: GenServer.call({:via, Registry, {…, "…_cc"}},
       {:send_control, <<…>>}, 5000)
      ** (EXIT) shutdown
  ```

- **The in-flight command is a `Ping`** (field 19 of `CommandRequest`), whose
  16-hex nonce is minted only by `ControlUnit.handle_call({:ping_runner, …})`,
  whose only production caller is `Flezha.Operative.Lifecycle` —
  `attempt_ping_and_start/3` before `:start_operative`, and `runner_ready?/1`.
- **Why this is production and not a test artefact.** Everything after the test's
  one call is production code: `NodeTestHelper.stop_runner/2` →
  `FleetManager.disconnect_runner/2` (the same public entry `Convergence.Heal`
  uses) → the runner closes → `FleetManager.runner_disconnected` →
  `do_handle_runner_disconnect/3` → `FleetSupervisor.stop_runner_sup/1` →
  `Supervisor.terminate_child/2`. No test stops any child itself or names an
  order. The ping is issued by production code. The one test-side ingredient —
  `test/flezha_test.exs:592` starting `Convergence.Loop`, which the test config
  leaves off — makes the run **more** production-like, not less, since the Loop
  is on in production.
- **The code already half-knows.** `ControlChannel.send_control/2` guards the
  channel being *gone* (`GenServer.whereis` → `{:error, :no_control_channel}`)
  but not its dying mid-call; and `Lifecycle.timeout_class?/1` names "the
  ControlUnit having exited on such a call" as one of three shapes a busy runner
  takes. The **caller** is defended; the unit itself is left to die.
- **Impact**: one `[error]` supervisor report per teardown that races an
  in-flight control command. Nothing is lost — the tree is going down and every
  caller already tolerates the exit — but it is the last line keeping
  `.github/workflows/elixir.yml`'s uncaptured-log gate red, now intermittently
  (absent from one full `mix test`, present in the next; 2 of 2 runs of
  `test/flezha_test.exs` alone).
- **A fix must** decide between two shapes and not paper over the report:
  guard the call inside `ControlUnit` (catch the exit, answer the waiter an
  error — the shape `ping_runner/2` already uses on its own caller side), or
  stop `ControlUnit` before `ControlChannel` in `RunnerSupervisor`'s child list.
  It must NOT be answered by wrapping the test in `capture_log`: the test drives
  no failure path on purpose, so there would be nothing to assert.

---

### KI-50 · `Extension.PhaseTest` "uncertain health probe" fails about 1 run in 3, in-suite only

- **Severity**: P2 · not in KI-18's population; found 2026-08-30.
- **Where**: `test/plugin/extension/phase_test.exs`, "an uncertain health probe
  is observable / a provider that reports its own uncertainty reads healthy AND
  emits".
- **Evidence**: full `mix test` on the same tree gave **1835/1836 then 1836/1836**
  — intermittent, not deterministic. The module alone passes **3 of 3** (14
  tests each). So it is an interaction, and this repository's own lesson is that
  a test which passes alone and fails in a suite is evidence of a **leak** before
  it is evidence of load (see KI-43's resolution).
- **Not attributable to `adopt-process-hub-hex-release`**: the file carries zero
  references to `ProcessHub` or `:elector`, and that change's own worker measured
  1836 on three consecutive runs before it.
- **A fix must**: find what leaks into it rather than retrying it. The assertion
  is "reads healthy AND emits", so the candidate is an emit from a previous
  module's provider arriving on this one's observation — the same shape as
  KI-49. Do not add a settle: that hides it, and the suite then stops being able
  to report it at all.

---

## P2 — tests that lie or leak

### KI-49 · `overwatch_suspension_test` leaks a retrying task onto the next test's stub

- **Severity**: P2 · found by `green-the-suite-log-gate`'s own gate, and **not
  fixed by it** — it is a fourth uncaptured-error source, outside that change's
  task list.
- **Where**: `test/flezha/operative/overwatch_suspension_test.exs:157`.
- **What happens**: a test's migrate task is still in the retry ladder
  (`Lifecycle.do_start_with_retry/4`, 10 ms backoff, preceded by
  `startup attempt 1 failed: {:ping_failed, :no_control_unit} — retrying`) when
  the test ends. The next test replaces the module's Mimic stub —
  `stub(OperativeManager, :assign_runner, fn _ns, "chat", ^inst -> "ex_2" end)`,
  whose third argument is **pinned to that test's own instance id** — so the
  leftover task arrives carrying the *previous* id and matches no clause:

  ```
  [error] Task #PID<0.426.0> started from #PID<0.415.0> terminating
  ** (FunctionClauseError) no function clause matching in anonymous fn/3 in
     …OverwatchSuspensionTest."test input for a suspended instance is journaled
     and rehydrates it"/1
      overwatch_suspension_test.exs:157: anonymous fn("default", "chat", "susp_451")
      lib/flezha/operative/lifecycle.ex:440: Lifecycle.resolve_runner/4
  [error] Child :undefined of Supervisor Flezha.Operative.MessageHandlerTaskSup terminated
  ```

  Two `[error]` lines, **every test still passing** — which is exactly the shape
  the uncaptured-log gate exists to catch.
- **Reproduction**: the module alone, no other file involved —
  `mix test test/flezha/operative/overwatch_suspension_test.exs`, **1 run in 5**
  (seed 432113) prints the pair while reporting `20 passed`; in a full `mix test`
  it appeared in 1 of 3 runs. Cheap: the module runs in ~0.2 s.
- **Why it was not fixed on the spot**: it is test-side and small, but the fix
  has to decide *why* the task outlives its test before it picks a shape, and a
  catch-all clause on the stub would silence the symptom while weakening the
  assertion the pin exists to make. `green-the-suite-log-gate` was scoped to
  three named lines and one judgement call.
- **A fix must** stop or await the migrate task the test spawned, rather than
  loosening the stub — the pinned `^inst` is what proves the rehydrate resolves
  the *right* instance.

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
- **Triage (2026-08-30)**: **STALE — and stronger than that, the path shape the
  entry quotes never existed in code.** Socket names are composed in exactly two
  places, and neither appends `_wus`: `lib/flezha/socket/support.ex:76`–`:78`
  (`"#{type}_stream_#{runner_id}.sock"`) and
  `flz_transport/src/utility/socket_dir.rs:164`–`:179` (same shape,
  `STREAM_SOCKET_INFIX = "_stream_"`). `_wus` is a Registry **via-name** suffix
  from `Identificator.unit_sup/1` (`lib/flezha/service/identificator.ex:38`),
  never a filename. `git log --all -S '_wus.sock'` returns only commits that
  touch this entry's own prose — the string has never appeared in source.
  The runner id is not `<namespace>_<ids>` either: it is `namespace <> "_" <>`
  four concatenated `Enum.random(1..1000)` (`identificator.ex:20`–`:36`).
- **The fix landed in `ac64206a`** ("feat(discovery): a runner finds its
  controller…"): `config/test.exs:141`–`:149` moved the dir to
  `System.tmp_dir!()/flezha_sockets/<node>` with a comment naming SUN_LEN, and
  `test/helpers/node_test_helper.ex:137`–`:139` keeps peer dirs beside it.
- **Reproduction — refuted, by measuring the production function.** Calling
  `Flezha.Socket.Support.stream_socket_path/2` under `config/test.exs`'s dir:

  ```
  socket_dir = /tmp/flezha_sockets/nonode@nohost
  workflow_loop_stateless/10d + rand16     107 bytes  ok    <- absolute worst case
  typical rand (12 chars):                 101 bytes
  ```

  The 107 row needs a 10-digit unique suffix **and** all four `Enum.random(1..1000)`
  landing on exactly 1000 (p ≈ 1e-12). The longest path ever actually written on
  this box, across every past run's leftovers, is **89 bytes**:
  `/tmp/flezha_sockets/test_agent_node1@127.0.0.1/secondary_stream_default_559954644993.sock`.
  And a plain suite run produces **zero** of the entry's error lines:

  ```
  Failed to start primary listener: 0
  Failed to connect runner        : 0
  sun_path msgs                   : 0
  ```
- **Two corrections of fact**: the ceiling is **107, not 108** (binding real
  sockets: `len=107 OK`, `len=108 ERROR :einval`), and **nothing in this repo
  enforces it** — `grep -rn "sun_path\|sockaddr_un" --include=*.rs` outside
  `target/` returns nothing. The message the entry quotes is Rust std's own,
  surfaced from the kernel.
- **What could NOT be established**: the 120/167 error-line counts. Those are
  system-tier numbers and a plain `mix test` excludes `:system`; re-counting them
  needs a built SDK runner per arm, which is past "cheap and decisive". But they
  cannot have been sun_path overflow, because no path the code can build reaches
  even 108.
- **Worth fixing**: nothing. Close as fixed by `ac64206a` with the numbers
  corrected. **Keep one caveat**: the margin is node-name dependent —
  `socket_dir()` embeds `node()`, which is `nonode@nohost` (13 bytes) under `mix
  test`; a distributed run named e.g. `flezha@127.0.0.1` (16) adds 3 and puts the
  theoretical worst case at 110. CI does not do this
  (`.github/workflows/nightly-e2e.yml:212` is a bare `mix test --include system
  --include multi_node`), so it is a trap for a future change, not a live defect.


### KI-18 · Load-sensitive tests, each seen once and green alone

Recorded so a future red run is not mistaken for a regression. **Green in
isolation is not evidence** — reproduce under load before concluding anything
(see `docs/refactor/STATE.md` for the stress-regime method). Five were filed
together and the dated triage below reasons about those five; anything added
since carries its own date.

- `test/flezha_test.exs:591` (runner disconnect, preserve) — 1/6 full runs,
  0/10 alone, 0/3 instrumented. Root cause never established.
- `workflow_crash_resume_e2e_test.exs:249` — **reproduced at
  `refactor-baseline-2`, so provably pre-existing**.
- `effectively_once_e2e_test.exs:34` — a different module from KI-2's second face.
- `test/plugin/kafka/worker_registry_test.exs:303` — a controller unit test with
  no runner involved.
- `flezha_test.exs:383` (live handover) — not the same as `:591`.
- `test/system/sdk_smoke_e2e_test.exs:55` (`transport=uds` handshake +
  `AssignRunnerId`) — added 2026-08-30 from `java-build-runs-twice`. One red in
  three consecutive `FLEZHA_E2E_RUNTIME=java mix test --include sdk_smoke` runs
  against a `java_runner.jar` whose md5 never changed, so the artifact is not the
  variable. The red run took **133.9 s against 68.3 s and 69.3 s** for the two
  green ones and is the only one of the three carrying `[fleet_manager]
  proceeding past recovery timeout`; the failure itself is
  `command_assign_runner_id` timing out at its 5 s budget, which then took the
  20 s `{:runner_connect, …}` call and the `{Flezha.Transport.Server, :uds}`
  child down with it. The module alone: **23 passed**.
- **Second sightings (2026-08-30, from `green-the-suite-log-gate`'s runs)**: two
  of the five now have a second, and the "exactly one sighting each" below is
  that much weaker. `worker_registry_test.exs:303` failed one plain `mix test`
  (`assert entry.callers == 3`, left `2`) and passes alone, 14 passed;
  `flezha_test.exs:383` and `workflow_crash_resume_e2e_test.exs:249` each failed
  one of two `--include system` runs — **a different one each run**, 1914/1915
  both times, which is the load-sensitive signature rather than a regression.
- **Triage (2026-08-30)**: **the set does not hold together — one defect, one
  stale, three suspicions.** Established by reading the code for each named
  mechanism, not by re-running anything: the entry's own instruction is that
  green-in-isolation proves nothing, and the converse is that a red nobody can
  explain is a suspicion. Four of the five have exactly one sighting each.
- **The evidence anchor is orphaned, and so is every one of its siblings.** The
  entry's only reproduction rests on tag `refactor-baseline-2`. It still
  resolves (`b76b600e`) but this morning's rewrite left it off the mainline:

  ```
  $ git merge-base --is-ancestor refactor-baseline-2 HEAD ; echo $?
  1        # NOT an ancestor of dev
  ```

  All seven `refactor-baseline-*` tags are in this state. They are **recoverable,
  because every one has a tree-identical twin on `dev`** — so the reproduction
  instructions are still valid in content, and re-pointing the tags is mechanical:

  | tag | orphaned commit | `dev` twin (tree identical) |
  |---|---|---|
  | `refactor-baseline` / `-0a` | `31866dc5` | `2a12065d` |
  | `refactor-baseline-0b` | `20b850ca` | `a147111c` |
  | `refactor-baseline-1` | `8cf49fa5` | `df3fc0f4` |
  | `refactor-baseline-1s` | `b94255a2` | `10939a9c` |
  | `refactor-baseline-2` | `b76b600e` | `b0697e49` |
  | `refactor-baseline-3` | `ead07960` | `61bbcb98` |

  Fourteen places in `docs/` and `CLAUDE.md` cite these tags, and
  `docs/refactor/charter.md:135` makes them the A/B method of the whole
  programme. A `git gc --prune` — or any fresh clone — loses all seven. **This is
  the most urgent thing in this entry and it is not about tests at all.**
- **Per test:**
  1. `test/flezha_test.exs:591` — **SUSPICION, and the coordinate is wrong.**
     `:591` is `@tag :runner_disconnect_preserve`; the test is `:592`, so `mix
     test …:591` selects the *previous* test under ExUnit's at-or-before rule.
     Its own record is 1/6 full runs, 0/10 alone, 0/3 instrumented, and it has
     not been seen since. Plenty of candidate mechanisms (a fixed
     `@temp_namespace` and fixed `operative_1..15` shared with three sibling
     tests; `reset_recovery_states/0` on a process-global cache; the singleton
     convergence `Loop` started at a 1 s tick while `:657` also drives
     `tick_now/0`) — **none of them is *the* mechanism.** Weakest of the five.
  2. `workflow_crash_resume_e2e_test.exs:249` — **RESOLVED, and it was never a
     load-sensitive test.** It was a KI-42 site: the module carried its **own
     private** `cleanup/3` whose `File.rm_rf` named `<state_root>/<class>`, so
     each of its **seven** call sites (`554, 696, 741, 794, 975, 1034, 1093`,
     the last being `drive_crash_resume/5`) leaked one class tree per run, and a
     later run whose `unique/1` id collided read the leftovers.
     `assertions-that-cannot-fail` deleted that private copy — the module now
     imports `TierE2EHarness.cleanup/3` — and measured the leak both ways on one
     scenario. **Drop it from the list; four remain.**
  3. `effectively_once_e2e_test.exs:34` — **STALE**, cured today by
     `fix-first-input-drop-on-bind` (`e16260a7`) in both halves: the module lost
     its `await_stream_bound` call, and its `on_exit` rides
     `TierE2EHarness.cleanup/3`, whose state path the same commit corrected. The
     deleted settle only ever covered Phase 0; the SIGKILL-and-rebind face it
     actually failed on is now gated in the product
     (`Lifecycle.complete_init/4`). Drop it from the list.
  4. `worker_registry_test.exs:303` — **SUSPICION, but with a mechanism and a
     one-line fix.** The Mimic hypothesis is *refuted*: the module contains zero
     Mimic references, and the leaked stubs in its converted siblings are on
     arity 1 while this test calls arity 2 (Mimic keys on `{mod, fun, arity}`).
     The real gap is that after `Process.exit(worker_pid, :kill)` at `:364`
     nothing waits for the registry to process the monitor `:DOWN` before three
     callers issue `swap/2` at `:367`; `handle_call({:swap, …})` checks the entry
     first, so a queued DOWN returns the dead pid, the starter is never called,
     and `:373`'s 100 ms `assert_receive {:starter_called, _}` fails. **Its own
     sibling at `:384` has the missing barrier verbatim** (`eventually(fn -> …
     worker_monitors == %{} and pending_refs == %{} end, 1_000)`).
  5. `flezha_test.exs:383` — **SUSPICION, with a mechanism.**
     `:402` asserts `ow_state.arrived_via_hotswap?`, which is
     `serialized_state != <<>>` (`handover.ex:99`), fed by a control-channel
     round trip (`handover.ex:184`) bounded by `ControlUnit`'s
     `@default_reply_timeout_ms 5_000`. A peer slower than 5 s under load falls
     back to `state.snapshot`, which this test never creates — and the failure
     reads verbatim "expected live HotSwap handover, not a WAL cold restore".
     The 5 s bound is unrelated to the drain's own 30 s, so widening the drain
     does nothing. **`flezha_test.exs:466` (test `:413`) carries the identical
     exposure and is not listed here.**
- **What could NOT be established**: which assertion actually failed in any of
  the five original sightings. No captured failure output survives anywhere in
  the repo — `docs/refactor/STATE.md` and the archived `refactor-0-tests`
  design record only `file:line` and a count, and for items 3 and 4 that line is
  the *test* line, so it names a whole scenario rather than an assertion. The
  mechanisms named for 2, 4 and 5 are demonstrably present and would each produce
  a red under load; **none is proven to be the one that fired.**
- **Worth fixing**: re-point the seven tags — that is the real risk here and it
  is minutes of work. Then reshape the entry: delete item 3, correct item 1's
  coordinate to `:592`, and attach the mechanisms to 4 and 5 so they stop
  reading as unexplained. (Item 2 is done — closed with KI-42.) Item 4 can be
  closed without a reproduction, because the missing barrier is visible in the
  code and its own sibling shows the correct form. Item 1 should be demoted to what it is: one
  sighting, no mechanism, not reproduced in 13 attempts.


### KI-19 · Test hygiene: leaked temp dirs and DLQ leftovers

- 738 leaked `/tmp/flezha_*` directories (state-test and durability-stub roots
  never removed); `app_data/dlq` holds leftovers from tests using the default DLQ
  directory. Owed to the test-consolidation area.
- **Triage (2026-08-30)**: **REAL**, reproduced in one plain `mix test`, and now
  attributed to three named modules. Both of the entry's numbers are stale, in
  opposite directions.
- **Reproduction** — count, run, count:

  ```
  $ ls -d /tmp/flezha_* | wc -l
  293
  $ mix test                        # 1830 passed, 0 failures, 56.8 s
  $ ls -d /tmp/flezha_* | wc -l
  303
  ```

  13 created, 3 reaped, **net +10 per plain run**: `flezha_state_test_*` × 10,
  `flezha_durability_stub_*` × 2, `flezha_state_dir_*` × 1.
- **Attribution** — 11 of the 13 belong to three modules, and running only those
  three shows it in 0.8 s:

  ```
  $ mix test test/flezha/operative/restore_refusal_test.exs \
             test/flezha/runner/fleet_manager_sdk_lang_test.exs \
             test/flezha/boot/state_path_coherence_test.exs
  Finished in 0.8 seconds ... 302 -> 313 dirs   (+11)
  ```

  `StateTestHelpers.tmp_app_data_dir/0` (`test/helpers/state_test_helpers.ex:59`)
  mkdirs the root and hands it back; reaping it is each caller's `on_exit`.
  Roughly thirty callers pair it with `File.rm_rf`. **These three never do** —
  their `on_exit` deletes the `:state_root` env var and leaves the tree:
  `restore_refusal_test.exs:30` (8 tests, so 8 dirs a run),
  `fleet_manager_sdk_lang_test.exs:178`, `state_path_coherence_test.exs:29`
  (which also mints its own `flezha_state_dir_*` at `:53`). The remaining two are
  `DurabilityBackendStub`'s isolated store (`durability_backend_stub.ex:58`),
  likewise never reaped.
- **Correction to the entry — the scale**: `/tmp/flezha_*` is **301 dirs / 6.3
  MB**, not 738. But every one of them is dated *today* (oldest `03:36`, newest
  `16:05`): the 738 were cleared at some point and this is **one working day's
  accumulation**, which is the more alarming reading of the same fact.
- **Correction to the entry — the DLQ half is a different leak, and it did not
  grow here**: 57 class dirs in `app_data/dlq` before the run and 57 after (484
  KB). 54 are named `pgrp<N>_p0` — that is `Partition.member_id/2`
  (`lib/flezha/partition.ex:103`), whose `@member_separator` is `"#p"`, passed
  through `Store.safe/1` (`lib/flezha/dlq/store.ex:434`, `[^A-Za-z0-9._-]` → `_`),
  so `pgrp<N>#p0` lands on disk as `pgrp<N>_p0`. They come from the **system tier** —
  `partitioned_operatives_e2e_test.exs`, the module that also held KI-42's third
  site —
  plus 3 `aborted_*`. A plain `mix test` cannot reproduce that half; it needs
  `--include system`.
- **Worth fixing**: yes, and the `/tmp` half is three `on_exit` lines plus one in
  the stub helper — the cheapest item in this band. The DLQ half is its own
  leak: KI-42 is closed and touched only the **state** root, so nothing about
  `app_data/dlq` moved with it.
- **Resolution of the `/tmp` half (2026-08-30, `green-the-suite-log-gate`)**:
  closed at **four** sites, each `on_exit` beside the line that mints the
  directory — `restore_refusal_test.exs`'s setup,
  `fleet_manager_sdk_lang_test.exs:178`, both dirs in
  `state_path_coherence_test.exs` (the `tmp_app_data_dir/0` one and the
  hand-rolled `flezha_state_dir_*`), and `DurabilityBackendStub`'s
  `isolate_store_dirs/0`, which mints one root per `install/0` where a module
  uninstalls once — so `uninstall/0` alone would still have leaked one per test.
  Measured with a timestamp marker rather than a count, because
  `System.unique_integer/1` restarts low in each BEAM and run N+1 re-mints run
  N's names, so a count hides a leak behind a collision: after the fix a run of
  the four sites creates or touches **no** `/tmp/flezha_*` entry at all, and a
  full `mix test` touches only the configured socket root. The three named
  modules were **+11 dirs** before and **0** after; the full run's total fell
  **336 → 323**, the 13 reclaimed being earlier runs' leaks whose names this run
  re-minted and then cleaned.
- **What is left**: the DLQ half, deliberately untouched. Triage established it
  does not grow in a plain run and needs `--include system`. KI-42 is closed and
  did not reach it — that entry was about the state root, and `Store.safe/1`
  writes under `app_data/dlq` on its own path rules.


### KI-21 · Three uncaptured `[error]` lines in a plain `mix test`

- Two sdk_lang-mismatch restore refusals and one ControlUnit abnormal shutdown.
  Would trip CI's uncaptured-log gate on this suite shape.
- **Triage (2026-08-30)**: **REAL, exactly as filed — and the entry understates
  it.** "Would trip CI's uncaptured-log gate" is wrong in tense: the gate exists,
  it is unconditional, and **it fails on this suite at HEAD today**.
  `.github/workflows/elixir.yml:150`–`:163` runs `mix test | tee
  /tmp/mix-test.log` and then greps that log; any hit is `exit 1`. So the
  `elixir.yml` `mix test` job is red on every run, on a suite that is otherwise
  1830/1830 green.
- **Reproduction** — the suite, then CI's own gate command verbatim against its
  log:

  ```
  $ mix test > /tmp/ki21_run.log 2>&1        # 1830 passed, 0 failures, 56.8 s
  $ grep -nE '\[(error|critical)\]' /tmp/ki21_run.log
  69:...[error] [Overwatch] restore REFUSED entity={"tier_refused_48578", "p0", "k"}
       runner=runner_1: "sdk_lang mismatch: this SDK is rust (1), the record was
       written by java (2)"; leaving the operative unbound rather than folding
       new inputs onto empty state
  70:...[error] [Overwatch] restore REFUSED entity={"refused_49122", "p0", "k"}  (same text)
  158:[error] Child Flezha.Runner.ControlUnit of Supervisor
       {Flezha.Service.Identificator, "test_temp_namespace_147568631510_wus"}
       shut down abnormally
  GATE RESULT: exit 1 — FAILS
  ```

  Three lines, matching the entry's count and its split (two sdk_lang refusals,
  one ControlUnit) exactly. The gate can report clean — it is a plain grep over a
  log that is otherwise 1830 dots — so the red is the finding, not the probe.
- **Attribution**: both restore refusals are
  `test/flezha/operative/restore_refusal_test.exs` — `:84` (`entity("tier_refused")`)
  and its sibling in the same `describe`. That module contains **no `capture_log`
  anywhere**, which is precisely what the gate's own message asks for: "a test
  that drives a failure path on purpose must wrap it in `ExUnit.CaptureLog` and
  assert the message". Both tests already *have* the refusal text as a literal in
  the body, so asserting on it costs nothing.
- **What I could NOT pin**: the third line's owning test. It is a
  `Flezha.Runner.ControlUnit` child under `Identificator.gen_via(runner_id,
  "wus")` (`lib/flezha/service/identificator.ex:39`), so the id is a
  `NodeTestHelper.start_runner`-style runner in the `test_temp_namespace` family
  — but `control_channel_disconnect_test.exs` alone emits **zero** `[error]`
  lines, so it is not that one. Naming it needs a `--trace` run I did not spend.
- **Worth fixing**: yes, and it is the highest value-per-line item in this band —
  a merge-blocking gate is currently red for three log lines, two of which are
  one `capture_log` wrapper in a single module. Until it is fixed the gate
  protects nothing, because nobody can tell a new uncaptured error from the three
  standing ones.
- **Resolution (2026-08-30, `green-the-suite-log-gate`)**: **two of the three
  lines closed; the third is a production defect and is re-filed as KI-48.**
  Both refusals in `test/flezha/operative/restore_refusal_test.exs` now run
  inside `ExUnit.CaptureLog.with_log/1` and assert on what was captured — that
  the line says `restore REFUSED`, names the Entity (`inspect(id)`) and carries
  the SDK's own reason — so the diagnostic became the assertion the module was
  missing rather than console noise. `capture_log` without an assertion, and
  `@moduletag capture_log: true`, were both rejected: they make the run quiet by
  making the module deaf. Measured: a plain `mix test` grepped **2** refusal
  lines at HEAD and **0** after, 1836 passed both times.
- **The gate itself was proved, not assumed** — it had never been seen to pass,
  so a green run only showed it was quiet. A scratch test emitting one
  uncaptured `Logger.error` turned CI's grep **RED while `mix test` still exited
  0** (which is the whole point of the gate); the same test wrapped in
  `capture_log` with an assertion turned it **GREEN**. The scratch test was then
  deleted.
- **The attribution the entry could not pin**: `test/flezha_test.exs:592`
  ("runner disconnect preserve"), found with `--trace`. The line is intermittent
  — absent from the baseline full run, present in the post-fix one, present in
  2 of 2 runs of that file alone — so the gate is **intermittently** red on it,
  not permanently. See KI-48 for the mechanism.
- **The entry's "three lines" is one run's sample, not the population.** A third
  full run turned up a **fourth** source the baseline never showed — two
  `[error]` lines from a task `overwatch_suspension_test` leaks into its own
  next test, filed as KI-49 and reproducible in that module alone. So the gate
  cannot be relied on to be green until KI-48 and KI-49 are both closed; what
  this change bought is that a *new* uncaptured error is now visible against a
  known, named set of two instead of an unexplained three.


### KI-22 · An inert drain in the output-stall drill

- Commit `087d0d28`'s `:sys.get_state(Loop)` drain is a no-op: a 200 000-iteration
  probe showed the `send_after(…, 0)` tick lands after the reply every time.
- **Triage (2026-08-30)**: **STALE — the drain the entry describes was deleted
  from the tree yesterday.** `5f381a3e` ("refactor(0b): tests wait for signals,
  not for time") removed it and said so in its own message: *"the output-stall
  drill's boot tick"*. HEAD
  (`test/flezha/convergence/output_stall_drill_test.exs:57`–`:74`) listens
  **before** the Loop starts and consumes the boot tick as a settle signal
  (`assert_receive {:tick, _boot_tick}, @budget_ms`) instead of trying to drain
  it afterwards.
- **The hash**: `087d0d28` is **not dangling — it is stranded**. It resolves as
  an object (`git cat-file -t` → `commit`) but `git merge-base --is-ancestor
  087d0d28 HEAD` is false: this morning's rewrite moved it off `dev`, and it now
  survives only on `origin/RESCUE-area3-45cbd1db` and
  `origin/spike/1s-dispatcher`. Its post-rewrite twin on `dev` is **`710a503e`**,
  same subject and author. Quote that one.
- **Reproduction of the entry's original claim — and a correction to it.** A
  standalone 200 000-iteration probe over both message shapes:

  ```
  $ elixir ki22_repro.exs 200000
  iterations: 200000
  A  send_after(self(), :tick, 0) in init  -> handled before :sys.get_state reply:    274/200000
  B  plain send/2 from a RETURNED sync call -> handled before :sys.get_state reply: 200000/200000
  ```

  The probe separates the arms (274 ≠ 0 ≠ 200 000), so it could have reported
  either way. **The old drain was inert 99.863 % of the time, not "every time"** —
  and those 274 are exactly the flake signature.
- **Do not "clean up" the `:sys.get_state(Loop)` that remains at `:111`.** It is a
  different drain guarding a different message and it has a hard guarantee:
  `Watchdog.sweep_now/0` is a `GenServer.call` (`watchdog.ex:91`) whose handler
  executes `:telemetry.execute([:flezha, :state, :output_stall], …)` inline
  (`watchdog.ex:459`), and `Loop.handle_telemetry/4` runs in the *Watchdog's*
  process and does a plain local `send` (`loop.ex:191`). So by the time
  `sweep_now()` returns, `{:output_stall, _}` is already in the Loop's mailbox
  and the test's `$gen_call` queues behind it. Sound by construction.
- **KI-35 is the same mechanism and the same fix — it needs re-triage.** One
  commit removed the inert drain *and* replaced `refute_receive {:tick, _}, 200`
  with the zero-timeout `refute_received` (`git log -S` on both strings returns
  `5f381a3e` for each). KI-35's stated cause is also wrong on its own terms: it
  blames the Loop's periodic tick, but `@tick_ms` is `3_600_000` — one hour — so
  after the boot tick the next periodic tick was never inside any window. The
  boot tick was always the only intruder. It did not reproduce at HEAD: **15/15
  idle and 14/14 under six busy-loop hogs on eight cores**, against a claimed
  ~40 % rate that should have produced ~6 failures in 15.
- **Worth fixing**: nothing to fix. Close KI-22 as fixed by `5f381a3e`, and
  re-triage KI-35 against the same commit rather than carrying it as a live flake.


### KI-23 · `unique/1` suffixes can breach the operative-id cap

- Suffixes reach 7 digits within one BEAM, so any id prefix over 24 characters
  breaches the 32-byte operative-id cap late in a combined run.
- **Triage (2026-08-30)**: **PARTLY TRUE — the mechanism is real, both numbers
  are off by one digit, and the headroom is two orders of magnitude.** Confirmed:
  `TierE2EHarness.unique/1` (`test/helpers/tier_e2e_harness.ex:182`) is `prefix
  <> "_" <> Integer.to_string(System.unique_integer([:positive]))`, and the cap
  is `@operative_id_max_bytes 32` (`lib/flezha/constant.ex:5`) enforced at
  `lib/flezha/service/validator.ex:23`–`:27`, refusing with `"operative id
  exceeds 32 bytes"`.
- **Correction 1 — the breach starts at 8 digits, not 7, and no prefix at HEAD
  exceeds 24.** Run against the real validator:

  ```
  checkpoint_batched_store   24-wide prefix + 1 + 7 = 32  :ok
  checkpoint_batched_store   24-wide prefix + 1 + 8 = 33  {:error, "operative id exceeds 32 bytes"}
  live unique("checkpoint_batched_store") = checkpoint_batched_store_3206 (29 bytes) -> :ok
  ```

  The two longest prefixes are both exactly 24
  (`checkpoint_batched_outputs_e2e_test.exs:41`, and
  `workflow_crash_resume_e2e_test.exs:1090`'s `String.slice(factory_id, 0, 24)`);
  every other tops out at 18. So the entry's "any prefix over 24 characters" is
  right in form but names a population that is currently empty, and "7 digits"
  should read 8.
- **Correction 2 — `tier_commit_block_workflow_` is no longer a prefix.** It
  survives only as a *factory id* (`test/apps/rust_runner/src/main.rs:335`, the
  Java twin, both `conformance.json`) — the `block` → `drop_nothing` rename never
  reached the fixture name. `workflow_crash_resume_e2e_test.exs:1090` already
  truncates it, with a comment saying why, landed in `5f381a3e`. The mitigation
  exists but budgets exactly 7 digits with **zero margin**.
- **Reproduction — refuted for a plain run, with the generator characterised.** A
  probe attached to a full suite:

  ```
  [UNIQPROBE] start_max=17541 end_max=53893 digits=5
  [UNIQPROBE] worst id: "checkpoint_batched_store_53893" = 30 bytes (cap 32)
  ```

  5 digits, 30 bytes, 2 under the cap. The whole suite advances the stream by
  36,352 — about 1,136 calls at this box's stride of 32 per call (stride is
  scheduler-count dependent: 16 at 2 schedulers, 64 at 16). Only bare
  `:erlang.unique_integer/0` and `[:positive]` share that stream —
  `[:monotonic]` and `make_ref/0` do not, measured at 200k calls each — which is
  why the suite barely moves it. **The arithmetic**: 24 + 1 + d ≤ 32 → d ≤ 7, so
  d = 8 needs a value ≥ 10⁷ ≈ 281,250 calls at stride 32. That is ~167× what the
  full plain suite consumes here, or ~46× on a 32-core box.
- **What could NOT be established**: the combined `--include system --include
  multi_node` consumption, which needs a built SDK runner. It is heavier, but
  plausibly 3–5×, not 46–167× — so "late in a combined run" is very likely wrong,
  though I cannot rule it out on a very large box.
- **Worth recording, and tighter than the filed issue**: the *partitioned* path
  has less headroom. `lib/flezha/operative/partition_group.ex:110` requires
  `member_id(base, N-1)` to fit in 32, so the `#pK` suffix (3–4 bytes) comes out
  of the same budget and a partitioned start with a 32-byte base id is refused
  today by `id_budget_refusal/2` (`:126`–`:131`).
- **Worth fixing**: low priority, and cheap when done — hold prefixes to ≤ 23
  (which leaves 8 digits) or use a fixed-width suffix. Only the two 24-byte
  prefixes sit at the edge, and one of them is already a manual truncation with
  no margin, which is the fragile part worth removing.


### KI-24 · `sdk/rust/tests/ui/*.rs` must not be rustfmt-formatted

- Their line numbers are pinned by the `.stderr` snapshots, and `cargo fmt` never
  reaches them because trybuild compiles them out-of-tree — so a per-file
  `rustfmt` loop *does* reach them and breaks the snapshots. Exclude that
  directory from any formatting loop.

- **Triage (2026-08-30)**: **REAL, live and undefended — but the entry names the
  wrong threat.** The repo's own formatters are safe: `scripts/format-apply.sh:43`
  runs `cargo +nightly fmt --all` and `.github/workflows/format.yml:82`–`:86`
  runs the same under the pinned `nightly-2026-05-07`. Neither is a per-file
  loop. The exposure is the **agent-level convention** — this box's standing rule
  is "never `cargo fmt --all` here; use `rustfmt +nightly --edition 2021 <file>`"
  — so any session formatting a changed-file list that includes
  `sdk/rust/tests/ui/*.rs` breaks the snapshots. That is worth keeping the entry
  for, and it is not what the entry currently says.
- **No defence exists.** `rustfmt.toml:17`–`:20` ignores only
  `flz_transport/src/generated` and `sdk/rust/src/generated`; nothing in
  `scripts/`, `.github/workflows/` or any `.toml` mentions `tests/ui`.
- **Reproduction** — scratchpad copies only, the working tree never touched:

  ```
  $ cp rustfmt.toml sdk/rust/tests/ui/*.rs $SCRATCH/uifmt/
  $ cd $SCRATCH/uifmt && for f in *.rs; do rustfmt +nightly --edition 2021 "$f"; done
  CHANGED: procedure_ctx_with_key.rs        (13 -> 16 lines)
  CHANGED: sealed_disallows_on_full.rs      (37 -> 44 lines)
  CHANGED: sealed_disallows_wiring.rs       (55 -> 69 lines)
  CHANGED: stream_handle_cannot_escape.rs   (34 -> 41 lines)
  CHANGED: wire_wrong_input.rs              (53 -> 67 lines)
  total changed: 5 of 25
  ```

  20 of 25 files are unchanged, so the run can report clean — the 5 are the
  finding, not the method.
- **Every one of the 5 has `.stderr` pins, and every pin moves.** Sample, with
  the line the snapshot names before and after formatting:

  | pin | before | after |
  |---|---|---|
  | `procedure_ctx_with_key.rs:9:1` | `fn p(_input: u64, ctx: &mut …ProcedureCtx) -> …` | `fn p(` |
  | `sealed_disallows_on_full.rs:34:10` | `.on_full(OverflowPolicy::DropNothing);` | `}` |
  | `sealed_disallows_wiring.rs:52:10` | `.into::<Downstream>();` | `&mut self,` |
  | `stream_handle_cannot_escape.rs:27:15` | `static STASH: std::sync::Mutex<…>` | `_: StateHandle<'_, Self>,` |
  | `wire_wrong_input.rs:50:17` | `.into::<ConsumesCandle>();` | `fn init_state(…) {}` |

  And the snapshots embed the **rendered source line verbatim**, not just
  `line:col`, so even a pin whose coordinates survived would still break. 5 of 5.
- **The "`cargo fmt` never reaches them" claim is verified**, without running fmt:
  `cargo metadata --no-deps` on `sdk/rust/Cargo.toml` lists 35 test targets and
  **none** with a `src_path` under `/tests/ui/`. The only ui-related target is the
  trybuild driver `sdk/rust/tests/ui_compile_fail.rs`, which names them as runtime
  **string** paths (`t.compile_fail("tests/ui/wire_wrong_input.rs")`), never as
  modules. Cargo does not auto-discover `tests/<subdir>/*.rs`, and nothing
  `mod`-declares them.
- **Correction to the entry and to `CLAUDE.md`**: the count is **5, not three**.
  `CLAUDE.md:62` and `docs/dev_notes/refactor-3-rust-sdk.md:46`–`:48` both say
  "breaks three at once"; at HEAD it is five.
- **Worth fixing**: yes, and there is a one-line mechanical defence the entry does
  not mention — add `"sdk/rust/tests/ui"` to `rustfmt.toml`'s `ignore` array.
  `ignore` is nightly-only, which is already the pinned toolchain, and it makes a
  stray `rustfmt +nightly --edition 2021 sdk/rust/tests/ui/foo.rs` a no-op. That
  turns a documentation-only defence, which depends on every future session having
  read this entry, into one the tool enforces.

---

## P3 — warts, wording, and measurement notes

### KI-25 · A production `usleep` nothing states

- `sdk/php/src/Executor/SwooleExecutor.php:271` carries `usleep(5_000)`. The sleep
  metric counts test paths only, so no gate sees it. Probably legitimate and
  short and finite — but nothing says so.
- **Triage (2026-08-30)**: **TRUE on both halves, and it is a note, not a
  defect.** The sleep is at `sdk/php/src/Executor/SwooleExecutor.php:271` and it
  is provably short and finite from the three lines around it: `joinAll()`
  (`:265`) opens `$deadline = microtime(true) + self::JOIN_TIMEOUT_S` (`:267`,
  `JOIN_TIMEOUT_S = 5.0` at `:39`) and the poll is
  `while ($thread->isAlive() && microtime(true) < $deadline)`. A 5 ms poll inside
  a 5 s deadline.
- **Reproduction of the metric half — and it generalises further than the entry
  says.** The exemption is the *path* classifier, not the pattern:

  ```
  $ grep -rn 'usleep' sdk/php/src/
  sdk/php/src/Executor/SwooleExecutor.php:271:                    usleep(5_000);
  $ python3 -c "import json;a=json.load(open('docs/refactor/baseline.json'))['areas']['sdk/php'];print(a['prod_loc'],a['sleeps'],a['sleeps_by_file'])"
  4134 0 {}
  ```

  `SLEEP["php"]` is `\b(?:u)?sleep\(` (`scripts/refactor_metrics.py:340`) and
  *does* match this line — but `measure_lines` increments `sleeps` only under
  `if in_test[i]` (`:378`–`:379`), and `is_test_path` (`:102`) is false for
  `sdk/php/src/…`. So the metric is blind to production sleeps **in every
  language, by construction** — it can never be the thing that finds one. That is
  the fact worth carrying forward, not this particular `usleep`.
- **Worth fixing**: no. The code is correct and the `joinAll` docblock already
  explains why the drain is bounded at all. What the entry asks for is one
  comment, and the honest resolution is to record the metric's scope (above) and
  close this.

### KI-26 · A stale doc block on `channel_metadata`

- `sdk/rust/src/operative/dispatcher/emit_sink.rs:344-350` documents a function
  that no longer exists, names the **retired** `Block` overflow policy, and
  carries a design-decision label the charter forbids.
- **Triage (2026-08-30)**: **STALE — closed by `c5bdc26b` (`refactor(3f): area 3
  closes — and prices the seam it was built on`), which landed after this entry
  was filed.** `docs/known_issues.md` was created by `586e6eab`, which is
  *older* than `c5bdc26b` in `dev`'s history, so the whole P3 band predates area
  3's last segment.
- **Reproduction — all three claims checked at HEAD, all three gone:**

  ```
  $ sed -n '344,347p' sdk/rust/src/operative/dispatcher/emit_sink.rs
  /// Build the emit wire metadata: drop any caller-supplied reserved
  /// `channel` key, then stamp the descriptor's channel name.
  fn channel_metadata(
  $ grep -nE 'Decision|\bD[0-9]\b' sdk/rust/src/operative/dispatcher/emit_sink.rs
  $ grep -rn 'OverflowPolicy::Block' sdk/rust/src/
  ```

  The doc block now documents `channel_metadata`, which exists (defined `:346`,
  called `:150`); the design-decision label is gone; and `OverflowPolicy::Block`
  appears nowhere in `sdk/rust/src` at all. `git log -S` on both `"Decision"` and
  `"OverflowPolicy::Block"` over that file returns `c5bdc26b` as the most recent
  commit for each.
- **Worth fixing**: nothing to fix. Close it.

### KI-27 · Specs and docs name things that do not exist

- `openspec/specs/controller-packaging/spec.md:270,290` still name
  `libflz_transport.{so,dylib,dll}` as committed binaries.
- `flz_jni` — a crate that does not exist — is named in `README.md:48`,
  `docker/dev/README.md:26` and four specs.
- `mix bench.roundtrip`'s moduledoc names `--runtime rust|java|elixir`; the
  manifests give it five more.
- `config/config.exs` documented a request sub-option as `{"name": …}`; it is
  `key`, and that invocation never worked.
- **Triage (2026-08-30)**: **THREE OF FOUR ARE STALE; the third is still true
  and moved.** Checked one at a time rather than as one claim, which is what
  separated them.
- **(a) `libflz_transport` in `controller-packaging` — STALE.**
  `grep -n libflz_transport openspec/specs/controller-packaging/spec.md` returns
  nothing; `:270` and `:290` now describe the three *untracked* Java artefact
  paths. Fixed by `24127af0` (`spec: sync the deltas from
  unbundle-committed-build-artifacts`), today. Repo-wide the name survives only
  in `openspec/changes/archive/**`, where it is history and correct.
- **(b) `flz_jni` — STALE.** A repo-wide grep over `*.md`, `*.toml` and `*.rs`
  returns exactly two hits, and both are this file and `docs/refactor/STATE.md`
  recording the issue:

  ```
  $ grep -rn "flz_jni" --include="*.md" --include="*.toml" --include="*.rs" . | grep -v ^./target
  docs/known_issues.md:1388:- `flz_jni` — a crate that does not exist — is named in `README.md:48`,
  docs/refactor/STATE.md:281:  `libflz_transport.{so,dylib,dll}` as committed binaries, and `flz_jni` (a crate
  ```

  `git log -S "flz_jni" -- README.md docker/dev/README.md` names `024efa60`
  (`refactor(1c)`) as the commit that removed both. **The two files still
  carrying the name are the two files that describe the defect** — delete these
  lines and the string leaves the repository.
- **(c) The `bench.roundtrip` moduledoc — STILL TRUE, and it is the one worth
  keeping.** `lib/mix/tasks/bench/roundtrip.ex:24`–`:25` reads
  "`--runtime` rust|java|elixir (default `rust`)", while `parse_runtime/2`
  at `:1114`–`:1130` reads `PortPilot.runtimes()`, which is every
  `test/apps/*_runner/conformance.json` on disk: **elixir java node php python
  ruby rust** — seven, so the doc understates by four, not five. (The entry's
  "five more" counted against a different manifest set.) Note the sibling tasks
  are already correct: `lib/mix/tasks/bench.ex:22`–`:26` is manifest-driven and
  says so, and `lib/mix/tasks/bench/workflow.ex:160`–`:167` hard-codes
  `rust|java` *deliberately*, refusing `elixir` by name because the Elixir SDK
  has no workflow operatives. Only `roundtrip.ex`'s prose is stale.
- **(d) `config/config.exs` `{"name": …}` — STALE.** `grep -n '"name"'
  config/config.exs` returns nothing; `:29` now reads
  `["on_message", {"key": "fetch_init_state"}]`. Closed by
  `fix-extension-subopt-typing`.
- **Worth fixing**: only (c), and it is a two-line doc edit in one file. Trim
  (a), (b) and (d) from this entry when (c) lands, and the entry goes with
  them.

### KI-28 · A cross-compile warning only the dist build surfaces

- `flz_core/src/control_unit.rs:19` — `unused_imports` on `socket_dir` when
  cross-compiled to `x86_64-pc-windows-gnu`; used only inside `#[cfg(unix)]`.
- **Triage (2026-08-30)**: **STALE — this is the exact warning `62d7e918`
  (`fix(core): gate the socket_dir import to the platform that uses it`) closed
  today.** Same file, same line, same import, same target. HEAD
  (`flz_core/src/control_unit.rs:19`–`:23`) now reads `#[cfg(unix)] use
  flz_transport::utility::socket_dir;` with `socket_probe` left ungated because
  `connect_diagnosis/0` is not platform-specific, which matches the
  `#[cfg(unix)]` on `try_open_uds` — the only caller.
- **The commit adds a fact this entry did not have**: CI sets `RUSTFLAGS: -D
  warnings`, so on a Windows target this was an **error**, not a warning. It
  stayed invisible for four months because CI dropped cargo-zigbuild in April and
  every committed artefact was built on Linux, where the import is live. It
  surfaced on the first five-triple cross-build since.
- **NOT REPRODUCIBLE ON THIS BOX, and this is the part to carry forward.** The
  target is installed (`rustup target list --installed` lists
  `x86_64-pc-windows-gnu`) and `cargo-zigbuild` is on `PATH` — but **`zig`
  itself is not installed**, and without it the cross-build dies long before it
  reaches our code:

  ```
  $ RUSTFLAGS="-D warnings" cargo check -q -p flz_core --target x86_64-pc-windows-gnu
  error: failed to run custom build command for `ring v0.17.14`
  ```

  `ring` needs a C cross-compiler, which is what zigbuild supplies. So the
  cross-compile lane cannot be exercised here at all; the fix is verifiable only
  by reading it and by the commit's own recorded `cargo zigbuild` run. Treat "no
  local cross-compile toolchain" as an environment fact alongside KI-30.
- **Worth fixing**: nothing to fix. Close it — but note that the local half of
  the loop that would catch the next one of these does not exist on this box.

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

- **Triage (2026-08-30)**: **TRUE, and correctly filed as a note rather than a
  defect.** The reproduction of a measurement note is the measurement, so this
  triage adds a row measured today rather than re-deriving six floors.
- **A seventh row, measured this session — the merge-gate row.** Five runs of
  `./scripts/bench.sh -n 100 -m 10000` (the `bench-regression` workload), same
  box, same tree, same session:

  ```
  receive window : 518,872  568,312  562,280  581,754  536,635   spread 12.1 %
  end-to-end     : 239,530  250,374  245,423  251,808  243,849   spread  5.1 %
  ```

  Two things follow. **The end-to-end window is less than half as noisy as the
  receive window** on this workload (5.1 % vs 12.1 %) — the opposite of what the
  gate's choice of metric would suggest, and worth knowing before anyone treats a
  single receive-window number as a verdict. And **the gate's own tolerance band
  is 10 % (`REGRESSION_TOLERANCE: "0.9"`, `.github/workflows/bench.yml:29`) while
  the metric it gates spreads 12.1 % run-to-run here.** The gate still passes
  with margin because it compares to a floor of 480,000 and the slowest run was
  518,872 — but the band is narrower than the noise, so a *near-floor* box would
  flip on scheduling alone.
- **What could NOT be established**: none of the six recorded floors was
  re-derived. Each needs its own paired-round campaign, and the `whole-system
  round trip` and `veneer round trip` rows additionally need the Kafka bench
  stack (`bench/flink/compose.yml`), which is not up and is `.git/info/exclude`d.
- **Worth fixing**: nothing here is a defect. The one thing worth *doing* is
  recording the seventh row above beside the other six, so the gate's row has a
  published floor like every other row does.

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
- **Triage (2026-08-30)**: **TRUE. Fact 1 reproduced verbatim; facts 2 and 3 are
  correct but were not re-measured, and one fact is missing.**
- **Fact 1 — reproduced.** `perf` *is* on `PATH` (`/usr/bin/perf`), which is what
  makes this cost hours: it exists and then refuses.

  ```
  $ cat /proc/sys/kernel/perf_event_paranoid
  4
  $ perf stat -e cycles true
  Error:
  No supported events found.
  Access to performance monitoring and observability operations is limited.
  ...
  perf_event_paranoid setting is 4:
  $ perf record -o /tmp/perf.data true
  Error:
  Failure to open event 'cpu/cycles/Pu' on PMU 'cpu' which will be removed.
  $ which valgrind
  (nothing)
  ```

  Both subcommands refused, and valgrind is absent. "Not attributable here" is
  the correct answer, exactly as written.
- **Facts 2 and 3 — not re-measured, and deliberately not.** The worktree-vs-tree
  build asymmetry needs two 60-second microbench arms built two ways to
  re-derive; re-running it would cost more than it could tell, and nothing about
  it has changed. Believe it and build both arms the same way.
- **A fourth environment fact belongs here**: **there is no local cross-compile
  toolchain.** `cargo-zigbuild` is installed but `zig` is not, so any
  `--target x86_64-pc-windows-gnu` build dies in `ring`'s build script. See
  KI-28 — a cross-compile-only warning cannot be reproduced on this box.
- **Worth fixing**: not a defect. Keep the list and add the fourth fact.

### KI-31 · Duplication the programme has chosen not to remove yet

- `test/apps/rust_runner/src/main.rs` duplicates the Rust bench's four-stage
  stress pipeline (four jscpd pairs) — owed to the test-apps area.
- The nine `convert_value` copies across SDKs are structural twins a 40-token
  window does not detect.
- Whole-repo `duplicate_pairs` rose 183 → 235 when three test masses became
  fifteen binaries, each repeating one `use` header.
- **Triage (2026-08-30)**: **TRUE, deliberate, and NOT WORTH FIXING here — but
  the second bullet names the wrong place, and that matters to whoever picks it
  up.**
- **Bullet 1 — confirmed, both halves present.** The four-stage pipeline exists
  twice: `test/apps/rust_runner/src/main.rs` declares `stress_a`/`stress_b`/
  `stress_c`/`stress_d` (`:2392`, `:2422`, `:2452`, `:2482`) plus `entity_stress`
  (`:2505`), and `sdk/rust/benches/dispatch.rs` declares `stress_bench_a`…`_d`
  (`:187`, `:218`, `:249`, `:280`) plus `entity_stress_bench_operative` (`:304`).
  A live scan pairs them across the seam:

  ```
  $ python3 scripts/refactor_metrics.py --area test/apps --area sdk/rust --json
  sdk/rust duplicate_pairs = 210
  ... "sdk/rust/benches/correctness.rs:268-274 <-> test/apps/rust_runner/src/main.rs:163-169 (40 tokens)"
  ```

  Note `sdk/rust` reads **210** today against the baseline's 215, so the number
  moves under ordinary work and is not a fixed target.
- **Bullet 2 — CORRECTION: the nine `convert_value` copies are not "across
  SDKs".** All nine are Elixir controller plugin providers under
  `lib/flezha/plugin/`:

  ```
  provider_behaviour.ex   beam_msg_forward.ex   filter.ex
  operative_forward.ex    kafka.ex              monitored_io_forward.ex
  http_poll.ex            websocket.ex          postgres.ex
  ```

  Nine files, nine definition sites, one language, one directory. That is a
  materially different (and easier) refactor than a cross-SDK one, and it belongs
  to the `lib/flezha/plugin` area — which the baseline records at 16
  `duplicate_pairs` — not to any SDK area. Anyone reading "across SDKs" would go
  looking in six languages for something that lives in one.
- **Bullet 3 — the caveat is the content, and it is right.** `duplicate_pairs` is
  a *pair* count that a file split multiplies for unchanged source, and the
  baseline has no whole-repo figure to compare against (it records per-area
  numbers only), so the 183 → 235 delta is not re-derivable from
  `docs/refactor/baseline.json`. Always name the scan mode; prefer duplicated
  tokens.
- **Worth fixing**: no — this entry exists to say the programme chose to defer
  these, and that choice still holds. Fix the "across SDKs" wording so the
  deferral is at least accurately described.

---

### KI-46 · OTLP attributes are stringified where the protocol has typed values

- **Severity**: P3 · **proposal owed**.
- **Where**: `lib/flezha/observability/export/otlp_codec.ex` — `kv_attribute/2`
  and `any_value_string/1`, which emit `AnyValue.string_value` (field 1) and
  nothing else.
- **What happens**: nothing breaks. Every attribute is present and readable
  after `close-otlp-export-crash`, but a structured value crosses as an Elixir
  rendering — `%{line: 412}`, `{Mod, :fun, 2}`, `[1, 2, 3]` — where OTLP's
  `AnyValue` has `array_value` (5) and `kvlist_value` (6) for exactly these
  shapes. A collector or a Grafana query cannot index into a string.
- **Why it was not folded into `close-otlp-export-crash`**: that change had to
  stop the exporter dying on its own payload, and stringifying is the smaller
  fix. Emitting typed values changes the shape on the wire for attributes that
  already export today, which is a compatibility question with its own answer
  (design `close-otlp-export-crash/design.md`, Open Questions).
- **A fix must**: map Elixir terms onto `AnyValue`'s oneof (`bool_value`,
  `int_value`, `double_value`, `array_value`, `kvlist_value`, `bytes_value`),
  decide what a tuple, a pid and a struct become — none has an OTLP shape, so
  inspection stays their answer — and keep one decision behind all attribute
  sites, as `attribute_string/1` is now.

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
- **Triage (2026-08-30)**: **STALE. The gate's command completes, and it
  passes.** Three consecutive runs of the exact wrapper this entry names, none
  of them wedged, none of them short:

  ```
  $ ./scripts/bench.sh -n 100 -m 10000
  totals
    operatives          100
    inputs dispatched   1,000,000
    responses received  1,000,000
  throughput
    end-to-end (first input → last response)           4174.8 ms  239,530
    receive only (send loop returned → last response)  1927.3 ms  518,872

  run 1 EXIT=0  responses 1,000,000  receive 518,872
  run 2 EXIT=0  responses 1,000,000  receive 568,312
  run 3 EXIT=0  responses 1,000,000  receive 562,280
  ```

  No `Timeout waiting for message <N>/1000000`, no `:driver_loop_frozen`, 3/3
  against a filed 4/4 failure rate. Run 1 included the release build; runs 2 and
  3 used `FLEZHA_BENCH_SKIP_BUILD=1` against the artifact run 1 had just built.
- **And the gate's arithmetic passes, applied by hand from the workflow:**

  ```
  measured=518872 (worst of the three)  floor=480000  threshold=432000  verdict=PASS
  ```

  `480_000` is agreed by both sources the gate's own sync step compares
  (`docs/operations/benchmarking.md:265` and
  `openspec/specs/rust-sdk-dispatch/spec.md`), so that step passes too.
- **Two corrections to the entry's framing.** The gate does **not** run
  `scripts/bench.sh` — `.github/workflows/bench.yml:122` runs `MIX_ENV=prod mix
  bench --runtime rust --shape regression -n 100 -m 10000` directly. The wrapper
  is equivalent (it `exec`s the same task with the same defaults) so the
  reproduction stands, but quote the workflow. And the sync step that could never
  pass is fixed: `:78` now reads `SPEC=openspec/specs/rust-sdk-dispatch/spec.md`
  with a comment saying the change dir it once named is long archived.
- **What could NOT be established: which change fixed it.** The candidates that
  landed since — the non-blocking `DropNothing` backpressure work
  (`a full mailbox withholds work instead of holding a thread`),
  `improve-bench-report-and-signals` (which proved the `:driver_loop_frozen`
  verdict itself fired *vacuously* on a driver observed in its first moments),
  and today's `fix-first-input-drop-on-bind` — are each plausible and I did not
  bisect. **The `:driver_loop_frozen` half of the original diagnosis is now known
  to have been at least partly a false verdict**, which is reason enough not to
  trust the old root-cause note.
- **Worth fixing**: nothing to fix; close it. **Watch the first CI run anyway** —
  this gate has still never executed in CI, so a green local box is not a green
  gate, and CI hardware is slower than this one. Keep KI-29's observation in
  view: the tolerance band is 10 % and this row's own spread here is 12.1 %.

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
- **Triage (2026-08-30)**: **STILL OPEN, but its stated reproduction no longer
  runs — the test is pinned away from Java on purpose. Anyone who follows this
  entry's instructions will get green and wrongly close it.**
- **What happens if you do what the entry says:**

  ```
  $ FLEZHA_E2E_RUNTIME=java mix test test/system/workflow_crash_resume_e2e_test.exs:147 --include system
  [conformance] runtime=java manifest=test/apps/java_runner/conformance.json deferred=[]
  Result: 2 passed, 17 excluded
  ```

  Green. And the identical command with `FLEZHA_E2E_RUNTIME=rust` gives the
  identical result, because **the runtime axis is inert for this test**.
- **Why — and the code says so itself.** The scenario is now at
  `test/system/workflow_crash_resume_e2e_test.exs:135` (`:147` is inside its
  body) and carries `@tag reference_runtime: true` at `:134`, added by `578fb2e9`
  (`feat(test): one end-to-end suite, one SDK per run`). `FlezhaTest.SystemCase`
  resolves that tag to the reference SDK unconditionally
  (`test/helpers/system_case.ex:66`–`:67`:
  `def runtime_for(%{reference_runtime: true}), do: NodeTestHelper.reference_runtime()`),
  and the reference is `:rust`. The comment above the tag (`:130`–`:133`) states
  the reason in as many words: *"Known open on Java: a resumed diamond re-runs a
  committed producer (predates this change; recorded in the dev notes). Pinned to
  the reference until `java-diamond-resume` closes it."*
- **Proved by process, not by inference.** Sampling `ps -eo pid,comm | awk
  '$2=="java"'` twice a second across each run:

  ```
  pinned test  (:135, @tag reference_runtime)  java arm -> 0 distinct java processes
  unpinned test (:72, same file)               java arm -> 4 distinct java processes
  ```

  The probe was validated against a real `java -jar test/apps/java_runner/dist/java_runner.jar`
  first, so a zero is a zero and not a broken probe. The Java e2e arm is alive and
  well — it simply never reaches this scenario.
- **What could NOT be established: whether the defect still exists at HEAD.**
  Confirming or refuting it requires removing `@tag reference_runtime: true`, and
  triage fixes nothing. Everything else is in place — both jars are on disk from
  today (`02:27`) and the only kernel commit since is `62d7e918`, a
  `#[cfg(unix)]` import gate with no behaviour change on Linux — so a single tag
  removal is the whole cost of finding out.
- **Worth fixing**: yes, and it is still P1. **First step is one line**: run that
  scenario un-pinned on the Java arm and see whether `[a, c, c, b, b, j:c, j:c,
  j:b, j:b]` still comes back. Rename the owed change to `java-diamond-resume`,
  which is what the test comment already calls it.

### KI-34 · Java host dispatch costs 27–55 % more than Rust

- **Severity**: P2 · **proposal owed** (`java-host-dispatch-cost`).
- Measured during the cross-SDK perf-parity campaign (2026-08-22/23). The
  campaign delivered the *proof* — `mix bench.roundtrip --runtime rust|java|elixir`
  through the existing `Fleet` seam — and the gap it exposed was never closed.
- Nothing has been written; the number is the whole of what is known.
- **Triage (2026-08-30)**: **OLD AND SOFT. The 27–55 % figure is not
  reproducible today, and it was never a gate-grade number even at source.**
- **The source qualifies itself.** The measurement was
  `mix bench.roundtrip -p 2 -m 3000` through the Kafka bench stack, and the
  campaign's own note records *"Box was loaded; spread is wide (rust keyed/off
  42.0–58.1k), so treat as direction."* A 28 % spread inside one arm cannot
  resolve a 27 % between-arm difference. The honest form of the original claim is
  its other sentence: **Java is under Rust on every tier.**
- **Re-measured today on the neighbouring row** — `mix bench --shape regression
  -n 100 -m 10000`, the same `Fleet` seam, arms interleaved inside each round,
  `FLEZHA_BENCH_SKIP_BUILD=1` against artifacts built today:

  ```
  round=1 rust receive=581,754 end_to_end=251,808
  round=1 java receive=513,686 end_to_end=211,937
  round=2 rust receive=536,635 end_to_end=243,849
  round=2 java receive=482,543 end_to_end=206,290

  java vs rust, mean of two rounds:  receive -10.9 %   end-to-end -15.6 %
  ```

  Java is behind in 2/2 pairs on both windows, so the *direction* holds. The
  *magnitude* does not: 11–16 %, not 27–55 %. And the receive-window gap is
  inside the rust arm's own run-to-run spread on this box (12.1 %, five runs —
  see KI-29), so at this sample size the receive number resolves nothing; only
  the end-to-end window separates the arms.
- **What could NOT be established**: the campaign's actual rows. `mix
  bench.roundtrip` needs the Kafka stack at `bench/flink/compose.yml`, which is
  `.git/info/exclude`d and not running (`docker ps` is empty), and bringing it up
  was out of scope for triage. **The row I measured is not the row the entry
  cites** — treat the two numbers as neighbours, not as a before/after.
- **Worth fixing**: the *entry* is worth rewriting before any proposal is. As it
  stands it advertises a precise-looking range that neither its own source nor a
  re-measurement supports, and a proposal opened against "27–55 %" would be
  chasing a number. Restate it as "Java is measurably behind Rust on every tier;
  magnitude unestablished" and keep `java-host-dispatch-cost` owed on that
  basis.

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
- **Triage (2026-08-30)**: **STALE — fixed by `5f381a3e` (`refactor(0b): tests
  wait for signals, not for time`), the same commit that closed KI-22, and the
  stated race was never the mechanism.**
- **The stated cause was wrong on its own terms.** The entry blames the
  convergence `Loop`'s periodic timer. The test starts the Loop with
  `start_supervised!({Loop, tick_ms: @tick_ms})` where `@tick_ms` is `3_600_000`
  — one hour — so after the boot tick no periodic tick was ever inside any 200 ms
  window. The intruder was always the *boot* tick, which is why one commit that
  taught the test to consume it fixed KI-22 and this at once.
- **The fix, at HEAD.** The scenario is now
  `test/flezha/convergence/output_stall_drill_test.exs:99` (the assertion at
  `:112`, not `:108`) and the timed `refute_receive {:tick, _}, 200` is gone:

  ```elixir
  :ok = Watchdog.sweep_now()
  _ = :sys.get_state(Loop)
  refute_received {:tick, _}
  ```

  Zero timeout, so there is no window to fall inside. The `:sys.get_state(Loop)`
  above it is the *sound* drain, not KI-22's inert one — `Watchdog.sweep_now/0`
  is a `GenServer.call` whose handler executes the telemetry inline, so any tick
  it caused is already in the Loop's mailbox before the call returns. See KI-22's
  triage; do not "clean it up".
- **Reproduction — refuted, 164 executions, idle and under load:**

  ```
  $ mix test test/flezha/convergence/output_stall_drill_test.exs --repeat-until-failure 40
  41 rounds x 2 tests, 0 failures        (idle)
  41 rounds x 2 tests, 0 failures        (with 6 busy-loop hogs on 8 cores)
  ```

  Against a filed ~40 % rate that should have produced roughly 33 failures in 82
  rounds. **The harness can report a failure** — a scratch test asserting
  `:rand.uniform(100) > 40` under the same flag exits 2 with `Result: 0/1
  passed`, so a green run here is a result and not a broken invocation.
- **Worth fixing**: nothing to fix. Close it, and close it *citing `5f381a3e`* so
  the next reader does not re-derive the wrong mechanism from the entry's own
  text.

### KI-36 · The controller OOM fix was never validated under real load

- **Severity**: P1 for production, P2 here.
- **Background**: the 2026-06-19 controller OOM (2 GiB, exit 137, ~30 h outage)
  was reproduced and root-caused on 2026-06-21 — controller write buffers on the
  WAL persistence path with no ceiling. `WalWriter.append_async` is a
  fire-and-forget cast while `FsyncBatcher` coordinates fsync via a blocking
  call, so when the SDK ships state changes faster than the controller can fsync
  (the Pi's SD-card fsync being the bottleneck) the FsyncBatcher and
  per-partition WalWriter mailboxes grow without bound.
- **The gap**: fix `f877ccb` has never been validated against the mock Kraken
  load that reproduced the original incident.
- **Triage (2026-08-30)**: **STILL TRUE. The gap is exactly as described, and
  nothing this session changed it.**
- **The hash resolves and is live**, which is worth stating because most
  pre-rewrite citations in this file do not: `f877ccb5` (`feat(resilience): bound
  controller WAL writer and transport send buffers`) is an ancestor of HEAD and
  sits on `dev`, `origin/dev` and `origin/main`. Its post-rewrite twin `75471751`
  carries the same subject and is **not** an ancestor — cite `f877ccb5`.
- **The load harness that would validate it exists**: `test/apps/testnet` is the
  mock Kraken v2 feed (`test/apps/testnet/README.md:27`), driven by
  `FLEZHA_TESTNET_CONFIG`. So this is a "nobody has run it" gap, not a "the tool
  is gone" gap.
- **What could NOT be established, and deliberately so**: whether the fix holds
  under that load. Reproducing the original incident means an SD-card-class fsync
  bottleneck plus a sustained state-change rate — hours of soak against real
  hardware, which is a change, not a triage step. **A green unit or e2e run
  proves nothing here**: the failure mode is unbounded mailbox growth under
  sustained overload, and every short run is inside the bound by construction.
- **Worth fixing**: the fix is already in. What is owed is the *validation*, and
  it is a soak, not a patch. Keep it filed at P1-for-production and note that it
  is blocked on hardware and time rather than on any unknown.

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
- **Triage (2026-08-30)**: **HALF STALE. "Every SDK ships all five" is false —
  it is true of two SDKs out of six, and the other four already do the right
  thing.** Read at HEAD:

  | SDK | How the native library reaches a consumer | All five bundled? |
  |---|---|---|
  | Java | `flezha-sdk-native/src/main/resources/native/<platform>/` — 5 dirs in one jar | **yes** |
  | PHP | `sdk/php/lib/{linux-x64,linux-arm64,macos-x64,macos-arm64,windows-x64}/` | **yes** |
  | Node | `optionalDependencies` — five `@flezha/sdk-<os>-<arch>` packages (`sdk/node/package.json:47`–`:53`) | no — already per-triple |
  | Ruby | `spec.extensions = ["ext/flz_core_ffi/extconf.rb"]` — compiled at install | no — ships no binary |
  | Python | maturin (`[tool.maturin]`, `sdk/python/pyproject.toml:46`) — per-platform wheels | no |
  | Elixir | its own core; inherits nothing from `flz_core` | n/a |

  So the "risk being avoided" — five ad-hoc schemes invented independently —
  **has already partly happened**, and the conventions chosen are the sensible
  per-ecosystem ones the entry itself recommends. What is left is Java and PHP.
- **A separate thing changed today and must not be confused with this one.**
  `5457bb8f` (`build(java): the seven build artefacts leave git`) removed the five
  triples and both fat jars from **version control** — `git ls-files | grep -E
  '\.(so|dylib|dll|jar)$'` now returns **0**, against 1.70 GB of history across
  163 blobs. That changes where the binaries come from (Maven `--fetch-missing`,
  `sdk/java/flezha-sdk-native/pom.xml:124`, plus a tag-triggered release job that
  cross-builds all five), and `CONTRIBUTING.md` now states "No build artifact is
  committed" with `scripts/check-build-artifacts.sh` enforcing it. **It does not
  change the distribution shape**: a published Java jar still carries five
  triples. Repository weight and artifact weight are different problems.
- **Worth fixing**: yes, eventually, and the scope is now two SDKs, not six.
  Rewrite the entry to name Java and PHP, and record that Node, Ruby and Python
  set the precedent to follow rather than being part of the debt.

### KI-39 · Open-source obligations, none met

- **Severity**: P2. Nothing is published yet. All four things once owed — the
  `process_hub` hex release, a CLA bot, a `security@` contact, and the
  trademark question — are now addressed; what remains is confirming the CLA
  bot is installed on the GitHub repository, and publishing itself. Licensing
  itself is settled (MPL-2.0 controller, Apache-2.0 SDK side, recorded in
  `REUSE.toml`).
- **Triage (2026-08-30)**: **ALL FOUR ARE DONE**, the last of them —
  the hex release — later the same day. Checked one at a time:
  - **`security@` contact — DONE.** `SECURITY.md` exists (written today) with
    `security@flezha.com` at `:15`, plus GitHub private vulnerability reporting,
    a 5-day acknowledgement / 14-day update commitment, a 90-day coordinated
    disclosure window, and an explicit scope list.
  - **CLA bot — DONE as far as this repository can take it.** `CLA.md` exists
    (5,588 bytes) and `CONTRIBUTING.md` names the mechanism: *"The cla-assistant
    bot asks for the signature on your first pull request."*
  - **Trademark — ADDRESSED.** `README.md:329` is a `## License, trademark and
    warranty` section, and `:350` states the position explicitly: neither
    licence grants trademark rights (MPL-2.0 §2.3, Apache-2.0 §6), so a fork,
    derived work or hosted service may not use the name.
  - **`process_hub` hex release — DONE (2026-08-30).** 0.7.0 is published, and
    `adopt-process-hub-hex-release` took it: `mix.exs` declares
    `{:process_hub, "~> 0.7.0"}` and `mix.lock` holds a hex entry, so the
    "a hex-published package cannot depend on a git dependency" obstacle is
    gone. The licence is now in the package metadata (`mix hex.info
    process_hub` → Apache-2.0) rather than needing a read of
    `deps/process_hub/mix.exs` after every bump.
- **What could NOT be established**: whether cla-assistant is actually *installed*
  on the GitHub repository, as opposed to documented. `CONTRIBUTING.md` describes
  the bot; nothing in the tree can prove the app is enabled. Verify that in the
  GitHub UI before treating the CLA obligation as closed.
- **Worth fixing**: narrow the entry to the one remaining item. As written it
  overstates the debt by three items and would send someone to write files that
  already exist.

### KI-40 · CI does not run three test files or the kernel's own tests

- **Severity**: P2. CI skips `entity_timers`, `sugar_emit` and `simple_procedure`
  (all three need the `testing` feature) and **never runs `cargo test -p
  flz_core`** at all.
- **Triage (2026-08-30)**: **STALE on both halves. CI runs `cargo test -p
  flz_core`, and it passes the `testing` feature.** `.github/workflows/elixir.yml`
  has a `Run Rust SDK tests` step whose comment names this very gap — *"The only
  place the flz_core and flezha_runner crates' own unit and integration tests run
  in CI"* — and whose body is:

  ```yaml
  cargo test --quiet -p flz_core                                  # :141
  cargo test --quiet \                                            # :142
    -p flezha_runner --features bench-internals,testing \
    --lib --tests
  ```

  Added by `4fcde3ca` (`test: cut the suite from 150s to 58s and make
  failure-path errors assertions`).
- **Reproduction — CI's exact command builds the gated targets:**

  ```
  $ cargo test -p flezha_runner --features bench-internals,testing --lib --tests --no-run
  36 test binaries, including:
    simple_procedure  sugar_emit  state_evolution
    timer_surface  timer_firing  timer_durability  timer_lifecycle
  ```

  **And the check can go dirty** — drop `testing` and the same command does not
  merely skip them, it fails to compile:

  ```
  $ cargo test -p flezha_runner --features bench-internals --lib --tests --no-run
  sdk/rust/tests/common/timers.rs:17:20: error[E0432]: unresolved import
    `flezha_runner::testing`: could not find `testing` in `flezha_runner`
  error: could not compile `flezha_runner` (test "runner_lifecycle") ... (6 targets)
  ```

  So the feature is load-bearing and demonstrably present in the CI invocation.
- **One correction to carry**: **`entity_timers` is not a test target.** It is a
  capability name; the timer tests are four targets — `timer_surface`,
  `timer_firing`, `timer_durability`, `timer_lifecycle`
  (`sdk/rust/Cargo.toml:169`–`:183`). The CI step's own comment inherits the same
  wrong name, which is cosmetic but will mislead the next reader grepping for it.
  Also note `sugar_emit` needs only `["testing"]` (`:191`), not
  `bench-internals`.
- **Worth fixing**: nothing functional. One word in a CI comment
  (`entity_timers` → the four `timer_*` targets), if anyone is in the file
  anyway. Close the entry.

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
| KI-11 · every second Java build in a checkout failed at shade — `duplicate entry: META-INF/THIRD_PARTY_NOTICES.md`, and the conformance manifest's own build command carries no `clean` | `java-build-runs-twice` — `<forceCreation>true</forceCreation>` on `maven-jar-plugin` in the two modules that shade (`sdk/java/flezha-sdk`, `test/apps/java_runner`), at a **pinned 3.4.1**. The pin is load-bearing, not hygiene: nothing pinned that plugin, so this box's Maven 3.8.7 resolved **2.4** and the release image's Maven 3.9.9 resolves **3.4.1**, and the `jar.forceCreation` user property — the form that avoids declaring the plugin, and so avoids Maven's missing-version warning — exists only up to 2.x. Under a 3.4.1 pin it is silently inert and the duplicate-entry failure reproduces unchanged, which is a fix that would have looked green here and shipped broken. **The filed cause was wrong twice over.** `build-dist.sh` is not involved: `clean verify` → 0 then a bare `verify` → 1 reproduces it with no dist build anywhere, so the condition is a non-empty `target/`, which is every checkout after its first build. And it is not the aggregator's missing sources either — that only makes it total. **Shade replaces the jar plugin's output file in place**, so on the next build the jar plugin's up-to-date check finds the *shaded* jar at its own destination and skips: with no sources it always skips, and with sources it skips whenever nothing is newer than the fat jar. Either way shade re-shades its own 15 MB output; `flezha-sdk` fails loudly only because `IncludeResourceTransformer` then attaches a notices file the jar already carries. **`test/apps/java_runner` had the identical defect and failed silently** — its filter excludes `META-INF/THIRD_PARTY_NOTICES.md` from every artifact including its own, so it re-shaded to a fixpoint and exited 0; measured by `original-java_runner.jar` coming out 16,082,694 bytes (the fat jar) instead of 209,955. Both of the entry's suggested one-line fixes are refuted: `<shadedArtifactAttached>` installs the *empty* jar as `com.flezha:flezha-sdk`, which is what `java_runner` compiles against; moving the notices to `src/main/resources` removes the duplicate-entry symptom and leaves the re-shading, which is the half that can carry a deleted native forward. Proof: the conformance `build.test` command (byte-identical to `build.bench`) three times in one checkout with no `clean`, all exit 0; `unzip -l` on the fat jar → one `META-INF/THIRD_PARTY_NOTICES.md`, exactly the five `native/<os>-<arch>/libflz_core_ffi.*` and no other native library, 1833 entries as before; `mvn -P coverage,native-verify clean verify` green (117 + 17 + 5 tests); `FLEZHA_E2E_RUNTIME=java mix test --include sdk_smoke` 1859 passed |
| KI-38 · `process_hub` pinned to a GitHub `dev` branch, so no version number described what a build contained | `adopt-process-hub-hex-release` (`4735c359`) — `mix.exs` declares `{:process_hub, "~> 0.7.0"}`; `mix.lock` carries the hex entry. **0.7.0 is the pinned code, published**: 78 `.ex` files each side, exactly one differs (`hub.ex`), and that difference is a pure relocation of `for_workers/1` — sorted, the two files are an identical multiset of lines. What the `.ex` comparison could **not** see is the package's own `mix.exs`, which raises `elector` from `~> 0.3.3` to `~> 0.3.4`; resolution fails until `elector` is unlocked, so the lock diff is two lines, not one. elector 0.3.4 is a real behaviour change on a live path (`erpc:multicall` replaces per-node `send_request`/`wait_response`, so the fan-out timeout is per batch rather than per node, and an election survives a candidate vanishing) — `declared_children` and `centralized_load_balancer` are its callers. `mix compile --force --warnings-as-errors` 272 files clean (negative control: a bogus `ProcessHub` call does fail it); `mix test` 1836 passed and `--include system` 1915 passed / 5 skipped, both unchanged from the HEAD baselines recorded before the swap, and `--include multi_node` 1845 passed — added because of the elector finding, since that tag is not covered by `--include system` and is where leader election actually runs |
| KI-42 · six sites named `<state_root>/<class>`, a path the system never writes — five `refute File.exists?` that could not fail, and a `File.rm_rf` that reclaimed nothing | `assertions-that-cannot-fail` (`9708e2e5`) — all six now derive from `Flezha.State.Registry.class_dir/1`, and **every one was proved twice**: with the subject genuinely present at the real path the *old* assertion still passed (the two unit sites **34 passed**; `partitioned_operatives_e2e` **2 passed**; `remote_durability_e2e` under `POSTGRES_INTEGRATION=1` **3 passed**), and the *corrected* one goes red on the same subject and green once it is removed. **No deletion is broken** — all five are green against real behaviour, so `ClassRegistry`, `PartitionGroup.rescale/3`, `TierE2EHarness.destroy_local_state/1` and the rescale's tree removal all keep the promises their test names make; there was nothing to escalate. The `rm_rf` is proved by count, not by an assertion: one scenario (`workflow_crash_resume_e2e_test.exs:692`) left `app_data/state/workflow_loop_boot_2082/chat_loop_workflow_2114` with three class sidecars in it after its own `cleanup/3` ran, and leaves nothing after — the module's private `cleanup/3` is gone entirely, it imports `TierE2EHarness.cleanup/3`, which is the one teardown. **The sweep confirms the triage's number**: 46 `refute File.exists?` / `refute File.dir?` in `test/`, **exactly five vacuous** — the five filed — and 41 sound, each read against the layout it asserts on. **The seed is larger than filed**: not two moduledoc blocks but **seven**, across five modules (`state/registry.ex` twice, `operative/class_registry.ex`, `state/completion.ex`, `state/input_log.ex`, `state/proc_inbox.ex`, `state/proc_journal.ex`) plus four in `docs/`, every one dropping the `<namespace>` segment its own code writes. `Registry.namespace_dir/1` now owns that level and `class_dir/1` composes on it, so the two production sites that rebuilt it (`AuthZ.Quotas.compute_disk_bytes/1`, `Convergence.Loop.namespace_dir_mtimes/0`) and the two test sites (`TierE2EHarness.cleanup/3`, `quic_test.exs`) derive it too. Baselines unchanged: `mix test` **1836 passed**, `--include system` **1915 passed / 5 skipped** |
| KI-47 · `check-overflow-naming.sh` red on its own evidence — a triage entry has to quote the retired name to prove it is gone | the gate now separates a QUOTED mention from a bare use, scoped to `docs/known_issues.md`; drilled both ways (bare use still exits 1) |
| KI-44 · the OTLP codec crashed on any attribute value that is not a string, and silently re-encoded a list | `close-otlp-export-crash` (`ba0d3632`) — one private `attribute_string/1` renders every attribute **key and value**: a binary passes through untouched, an atom, integer or float takes its `String.Chars` rendering, everything else — lists included — goes through `inspect/1` with `limit: :infinity, printable_limit: :infinity`. Both halves were driven red first: a real `LoggerHandler.log/2` event through the real codec raised `Protocol.UndefinedError` on `logger:` (a Map) and on `mfa:` (a Tuple), while `%{l: [1, 2, 3]}` encoded to the bytes `<<1, 2, 3>>` **with no exception at all**. Three corrections to the filed entry, each measured: `:222` is the metric `NumberDataPoint` site, not the resource (`default_resource/0` never sees signal metadata); an attribute **key** can be a non-string too and crashed identically, since `Signal.new/1` validates that metadata is a map and never constrains its keys; and the escalation is no longer inferred — with the coercion reverted, `OtlpSink` dies inside `flush/1` with that exception, which the flush test now pins. `mix test test/flezha/observability/export_test.exs` with `attribute_string/1` reverted to `to_string/1`: **12 of 18**, six red including the list case, which fails without raising; restored: **18 of 18**, full suite **1836 passed, 0 failed**. The sibling site on the Prometheus exporter is KI-45; typed `AnyValue`s are KI-46 |
| KI-43 · 28 test modules set Mimic's global mode without releasing it | `close-mimic-global-leak` (`828a9f50`) — 26 modules moved to `FlezhaTest.Helper.MimicGlobal.set_mimic_global_until_exit/1`, two dropped global mode entirely (they register no stub anywhere reachable), and `scripts/check-mimic-global.sh` refuses the raw call in `test/` with `test/helpers/mimic_global.ex` exempt by path. Measured, not asserted: `mix test test/flezha/service/cli_manager_state_get_test.exs test/flezha/service/cli_manager_schedule_test.exs --seed 0` was **11/20 passed, 9 failed**, every failure `Only the global owner is allowed`, against **10 passed** for the schedule module alone; after, **20 passed** on the same seed. Read directly (a temporary `ExUnit.after_suite` probe on `:ets.lookup(Mimic.Server, :mode)`): at seed 0 exactly one of the 28 left the mode `:global`; after the change all 36 global-mode modules left it `:private`. The entry's own reading was too broad — see `docs/dev_notes/close-mimic-global-leak.md` for which of the 28 could actually leak and why |
| KI-2 · first input after `start_operative/2` journaled and never forwarded (both faces: the start window, and the restore re-forward onto a stream a fresh runner has not bound) | `fix-first-input-drop-on-bind` (`e16260a7`) — the operative bind now waits for its data-stream channel (`StreamChannel.await_bind/2`), and an undelivered payload arms `{:stream_bound, _}`, which re-forwards the journal tail through the replay path a restore already used. Deterministic proof: `test/flezha/operative/bind_reforward_test.exs` — its four delivery cases were run red on the pre-fix tree, in 2.1 s, with no sampling. `effectively_once_checkpoint_e2e_test.exs:37` 40x in isolation: **40/40 pass, 0 `[:flezha, :stream, :drop]`** |
| KI-20 · `sdk_smoke_e2e_test.exs` carried the old re-send workaround | `fix-first-input-drop-on-bind` (`e16260a7`) — `send_until_forwarded` deleted, the single `send_io` stands, and `NodeTestHelper.await_stream_bound/2` is gone from the tree with it (29 call sites across 15 modules) |
| KI-1 · the consumed watermark overwritten back after an effectively-once park (retention floor and credit under-reporting by one offset) | `fix-outbox-watermark-overwrite` (`9410eea7`) — `InputLog.record_consumed/3` now advances the stored offset or leaves it, atomically. `test/system/dlq_e2e_test.exs:121` in isolation: **0 of 6 runs passed before, 5 of 5 after**; the deterministic proof is `wal_writer_outbox_test.exs` "a nested window's advance survives the outer window closing", which fails on `refactor-baseline-2` as well as on the pre-fix HEAD |
| `cargo check --workspace --all-targets` red without a feature nobody passes | `119a20d4` — three `[[test]]` entries were missing `required-features` |
| `test/apps/node_runner/fixtures.mjs` a `SyntaxError` since `refactor(0a)`, so no Node e2e had run in four areas | `d6f6e16f` |
| Node `Source::get` costing four N-API calls per field read (~4 % on the Node dispatch row) | `d6f6e16f` |
| `refactor_metrics.py` measuring Rust functions short (`#` stripped as a comment, so `quote!` interpolations ended the brace count) | `6818eec1` |
| Three Rust SDK flakes, each with a mechanism rather than a retry | `588094e4` |
| Committed `.pyc` files and a missing `__pycache__` ignore | area 2 |
| `mix test --include system` "always exits 2" | refuted — the `:postgres_integration` modules self-skip now |
