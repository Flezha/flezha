# SDK State Persistence

For SDK implementers and contributors: how a Tasklet's per-Cell state becomes durable in
the Rust, Java and Elixir SDKs: the flusher, the restoration fold, compaction, the payload ids
and the record header. What a relay author writes and configures (state, changes, the
`durable` setting, the per-tasklet knobs, Kryo registration, the test harnesses) is in
[`../../public/topics/core/state.md`](../../public/topics/core/state.md). The controller's on-disk layout,
fsync batching, retention and the `flz state` CLI are in
[`../state_persistence.md`](../state_persistence.md); frame layouts in
[`protocol_spec.md`](protocol_spec.md#11-per-cell-state-persistence). The Elixir SDK and the
other FFI veneers carry them through the core ([`elixir_sdk.md`](elixir_sdk.md#persistence)).

## Shared concepts

### Changes and timers in the log

A Tasklet's `Change` folds itself with `apply_to(state)`; the runtime persists each record
through the per-Cell flush buffer. `NoChange` (Rust, uninhabited) and a `Void` state (Java) opt a
Tasklet out: the runtime detects it and registers nothing with the flusher. Rust reads the clock
off the tasklet, a written `event_time` or `logical_clock` on `#[flezha::tasklet]`; Java off the
class, a written `eventTime(input)` or `@Tasklet.Spec(logicalClock = true)`. Coalescing
collapses Changes sharing a `coalesce_key()` within one linger window to the most recent record
at drain time.

Timers are folded state: registrations, cancellations and firings are records in the
Cell's own change log (`format_id = 3` timer-ops records, or the `timers` field of an
effect record); a restore rebuilds the pending set by the same walk that folds Changes, and
a compaction re-states the full set after the snapshot seq so truncation cannot eat it
([`cell_timers.md`](cell_timers.md)).

### The flusher lives in the core

`flz_core` owns one flush thread per process, started lazily on the first registration of a
Tasklet that keeps state. Every linger tick (`FLEZHA_FLUSH_LINGER_MS`, default 5) it drains
each registered Cell's buffer, coalesces, stamps the 16-byte header and ships the frames.
Rust pushes typed Changes into the buffer directly; Java has no flusher of its own — its
Kryo-encoded change bytes ride the activation result into the core, so a long invocation (a
GC pause included) delays its records: the durability bound is a guideline, not a hard cap.
With `durable: false` the drain still runs (buffer emptied, seq advanced) but nothing is sent.
The Elixir SDK works as Java does: its ETF-encoded change bytes ride the activation result
into the core.

### How `durable` reaches the controller

The runner announces the class's `durable` setting (`durable` = `memory`, `local` or `remote`
in the `AddRelay` response, with `sleep_after_ms` when the class sleeps), so the controller
reads the same value: a class kept in memory keeps no WAL or snapshot, is given no restore on a
rebind, and answers a live Cell read with `{:error, :persistence_off}`. No controller setting
overrides it.

### Restoration flow (warm restart)

On a rebind the controller ships `RestoreInitState { cell_id, snapshot_bytes,
change_log_bytes, terminal_seq, has_compensation_intent, has_compensation_done }`:
`snapshot_bytes` is the raw encoded `State` (the controller strips the snapshot file's
header before shipping), `change_log_bytes` the WAL tail in the controller's on-disk record
framing. Under the Cell's exclusivity gate the runtime calls the author's
`init_state(cell_key)` (a Java cell's state comes from its state class or its written
`initState(cellKey)`), replaces the state with the decoded
snapshot when present, folds every record in seq order (plain Changes, the Changes enclosed
in effect records, timer ops into the pending-timer set), re-arms the change seq to
`terminal_seq + 1` (terminal = highest folded seq, never below the command's
`terminal_seq`) and answers `RestoreInitStateResponse { cell_id, terminal_seq }`. It also
discards any open checkpoint interval, half-filled replay slice and queued inputs (the
controller re-forwards them from the durable input log), restores the consumed-offset floor
and event-time high-water from the effect records, and re-arms processing-time timers. A Cell
restored before the runner's secondary stream exists has nothing to arm them on; installing the
stream arms them, once.

The Elixir SDK folds through the core like every SDK on it: the core walks and verifies the
records, and hands each snapshot and change body to the SDK, which decodes it from the External
Term Format, migrates it when its version asks, and applies it with `apply_change/2`.
Rust folds in-process with postcard and verifies each record's header (`sdk_lang`,
`schema_version`, `format_id`) and trailing CRC32, failing loud with
`RestoreError::{LangMismatch, SchemaTooOld, SchemaFromTheFuture, HeaderInvalid,
CrcMismatch, SnapshotDecode, ChangeDecode}`. Java folds through a `RestoreFold`
activation: the core splits the log on `format_id`, folds timer ops and cursors itself and
hands the host only the snapshot and change bodies to Kryo-decode and fold through each
change's `applyTo`; the
host fold performs no header or CRC check. Both folds are one walk,
`flz_core::restore::fold_restore`: snapshot first, then the log, answering the terminal seq; only
the sink differs. A restore naming a Cell with no live slot
validates the log and acks the terminal seq; a dynamic Cell the controller has state for
is materialised so it survives a rebind; an evicted Cell is never restored — eviction
deletes its files.

### Compaction (`CompactCell`)

Compaction is controller-driven and real on all three SDKs. On `CompactCell { cell_id }` the
core, under the Cell's exclusivity gate, drains the pending flush buffer first
(`flush_one`) so in-memory state equals the shipped WAL, then reads
`snapshot_seq = change_seq - 1` — the highest seq the snapshot already folds. The controller
truncates records with `seq > snapshot_seq` and seeds its baseline there, so stamping
`change_seq` (the next seq to assign) would dedup-drop the next real record. The
pending-timer set is re-stated as a fresh timer-ops record after the seq is read, landing at
`snapshot_seq + 1` where truncation keeps it. That sequence is
`flz_core::host::snapshot_frame` for every Cell; only the body differs. `State` is serialised with the change codec —
postcard (Rust, in-process), Kryo (Java) or ETF (Elixir), the latter two via a `SnapshotRequest` activation, never the
JSON migration serializer, or restore cannot decode it — shipped as a
`cell_state_snapshot` data frame, and acknowledged with
`CompactCellResponse { cell_id, snapshot_seq, snapshot_bytes_size }`. A `cell_id`
matching no live Cell is answered as an error with `{0, 0}`; the controller treats
`{0, 0}` against a non-empty WAL as a failed handshake. Size/age triggers, timeout, backoff
and give-up are the controller's.

### Wire payload ids and the record header

| Channel | Name | Carries |
|---|---|---|
| data, SDK → controller | `cell_state_change` | one change record (`format_id` 1 postcard / 4 Kryo / 7 ETF) or timer-ops record (3) |
| data, SDK → controller | `cell_effect_record` | effectively-once effect record (`format_id` 2: changes — enclosed in the writing SDK's own codec, postcard / Kryo / ETF — outputs, consumed offset, timers, dead-letter entry) |
| data, SDK → controller | `cell_state_snapshot` | compaction snapshot body |
| data, SDK → controller | `delete_cell_state` | dynamic-Cell eviction |
| data, SDK → controller | `park_payload`, `compensation_intent`, `compensation_done` | DLQ park; steps saga markers |
| control, controller → SDK | `RestoreInitState` / `RestoreInitStateResponse` | warm restart (reply from the SDK) |
| control, controller → SDK | `CompactCell` / `CompactCellResponse` | compaction (reply from the SDK) |
| control, controller → SDK | `CellMailboxStatus` | legacy `full` / `open` back-pressure (pre-credit sessions) |

Every durable record carries a 16-byte big-endian header stamped by the core's
`HeaderTemplate`; effect records and
timer-ops records use `format_id` 2 and 3 on the SDKs that write them, and every SDK's
restore rejects any `sdk_lang` but its own before it reads the format byte:

| Bytes | Field | Rust | Java | Elixir |
|---|---|---|---|---|
| 0 | `sdk_lang` | 1 | 2 | 3 |
| 1..5 | `sdk_version` | `(major << 16) \| (minor << 8) \| patch` of the writing SDK's own released version | the same packing of the writing SDK's own released version | the same packing of the writing SDK's own released version |
| 5..9 | `schema_version` | the Tasklet's `SCHEMA_VERSION` | the class-wide maximum of its Tasklets' `schemaVersion` | the Tasklet's `schema_version/0` |
| 9 | `format_id` | 1 postcard | 4 Kryo | 7 External Term Format |
| 10..12 | reserved | 0 | 0 | 0 |
| 12..16 | `crc32` | over the body | over the body | over the body |

Every SDK ships the smallest `compact_after_bytes` across the class's Tasklets in
`AddRelay` metadata only when it differs from the default
(`flz_core::persistence::DEFAULT_COMPACT_AFTER_BYTES`, in lockstep with the controller's
`:default_compact_after_bytes`).

## Per SDK

- **Rust.** The buffer behind the context is shared, so an emit drains just-applied Changes
  onto the durable path before the matching output frame (flush-before-emit). `Change<S>` is
  `Serialize + DeserializeOwned + Send + Sync + 'static`, and `State` is
  `Serialize + DeserializeOwned` so the runtime can also serialise it for `CellState` and
  migration.
- **Java.** Built-in Kryo registrations (`RestoreInitPayload`, a reserved slot, primitive arrays,
  boxed numbers, `String`, `Optional`, the common `java.util` collections, `Instant`,
  `ArrayDeque`) take fixed low ids, and discovered user types take name-derived ids in a band
  above them. Java timer coverage lives in `ActivationDispatcherTest`; `HarnessBehaviourTest` is
  the reference case for the direct-write detection.
- **Elixir.** The `state` block generates `state_from_json/1`, which turns a JSON object read
  back from the controller into the state term, and each `defchange` generates an
  `apply_change/2` clause and its recorder, journaling through `Flezha.SDK.Ctx.apply/3`. The change terms ride
  the activation result as ETF (`format_id` 7), and `Flezha.SDK.Ctx.checkpoint/1` closes an
  effectively-once interval from inside `run/3`. The tests are `sdk/elixir/test/flezha/sdk/dispatch_test.exs`
  (the activation kinds a restore and a compaction use) and the cross-SDK end-to-end suites
  run with `FLEZHA_E2E_RUNTIME=elixir`.

## See also

- [`../../public/topics/core/state.md`](../../public/topics/core/state.md): the author's view: state, changes, knobs, harnesses
- [`protocol_spec.md`](protocol_spec.md#11-per-cell-state-persistence) — frame layouts for every payload id and control command
- [`../state_persistence.md`](../state_persistence.md) — controller WAL, snapshots, retention, compaction triggers, live read
- [`poll_core.md`](poll_core.md) — the activation model the Java change bytes ride
- [`cell_timers.md`](cell_timers.md) — timers as folded state
- [`authoring_contract.md`](authoring_contract.md) — tiers, derived Change types, determinism
- [`../resilience/steps_relay.md`](../resilience/steps_relay.md) — durable-by-default steps classes
