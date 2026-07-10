# Controller state persistence

Implementation notes for the per-Segment WAL, snapshot, and
time-travel infrastructure on the Elixir controller. Settles the
details of the per-Segment WAL infrastructure; keep this in sync
with `lib/flezha/state/`.

> Per-Segment WAL persistence (this doc) is independent of and
> complementary to **plugin-subscription persistence** (cspec-driven;
> see `docs/process_hub_architecture.md` "Plugin subscriptions
> persistence" and
> `openspec/specs/plugin-subscription-persistence/spec.md`). The WAL
> persists data-plane progress; the cspec persists which plugins are
> subscribed.

## On-disk layout

```
app_data/
  state/
    {operative_class}/
      {operation_id}/
        {segment_key}.wal
        {segment_key}.snap.{seq}
        {segment_key}.outbox.dedup
```

The path triple `(operative_class, operation_id, segment_key)` uniquely
identifies a Segment's state. Persistence does not include the
`executor_id` — that is an addressing concern at the SDK boundary. When
an executor pod restarts and rotates its `executor_id`, state under the
flat layout is preserved and shipped to the new executor on Segment
attach via `RestoreInitState`. The one-time
`Flezha.Boot.StatePathMigrator` runs at `Application.start/2` to migrate
an older executor-keyed layout into the flat one.

`segment_key` is URL-safe-base64-encoded with a leading `_` when it
contains characters outside `[A-Za-z0-9_.-]`. Round-trippable via
`Flezha.State.Registry.encode_segment_key/1` and `decode_segment_key/1`.

Configurable root via `:flezha, :state_root` (default `app_data/state`).

## Module map

| Module | Role |
|---|---|
| `Flezha.State.Registry` | Path resolution + directory enumeration. No GenServer, no I/O beyond `File.ls/1`. |
| `Flezha.State.Wire` | Wire-frame codec for `segment_state_change` / `segment_state_snapshot` payload-ids. |
| `Flezha.State.Metrics` | Per-Segment `:counters` references; lazy allocation in a `:ets` table. |
| `Flezha.State.Outbox` | `last_persisted_seq` cache file (`<segment_key>.outbox.dedup`). |
| `Flezha.State.PersistenceResolver` | Per-Operative config resolution (`operative_persistence`, `time_travel_retention_hours`, `compact_after_bytes`). |
| `Flezha.State.Supervisor` | `DynamicSupervisor` for per-Segment `WalWriter` GenServers + a process registry for name lookup. |
| `Flezha.State.WalWriter` | One GenServer per Segment. Owns the WAL file handle; opens it in `handle_continue/2` so creation never blocks the caller. |
| `Flezha.State.FsyncBatcher` | Single shared GenServer driving `:file.datasync/1` across dirty writers on a fixed cadence. |
| `Flezha.State.Snapshot` | Snapshot file lifecycle (`.tmp → fsync → rename → truncate`). |
| `Flezha.State.Retention` | Periodic sweeper enforcing `time_travel_retention_hours`. |
| `Flezha.State.Restore` | Assembles `RestoreInitState` payload (snapshot + tail WAL). |
| `Flezha.State.TimeTravel` | Point-in-time query engine backing `flz state at`. |

## WAL record format

```
[seq:           u64 BE]
[len:           u32 BE]    <- = 16 + change_bytes.len()
[header:        16 B  ]    <- HeaderTemplate::stamp output (sdk_lang/version/schema)
[change_bytes]
[crc32:         u32 BE]    <- crc32 over header + change_bytes
```

The trailing `crc32` lets `WalWriter.scan_for_last_valid_seq/2` detect
partial-write tails at boot — the file is forward-scanned (forward, not
backward, because there is no length field at the record tail) and any
bytes past the last valid record are truncated.

## Writer startup

`StreamChannel` resolves a writer with `Supervisor.find_or_start/1` on
every `segment_state_change` frame. To keep the channel's loop off the
disk, `WalWriter.init/1` only builds the struct and returns
`{:continue, :open}`; the process is already Registry-registered, so
`find_or_start` returns in microseconds. All disk work — `mkdir`,
`:file.open`, `scan_for_last_valid_seq`, seek-to-EOF — runs in
`handle_continue(:open, …)`. Since a continue is processed before any
queued message, appends that arrive during the scan wait in the writer's
own mailbox, never the channel's; `last_persisted_seq` recovery is
unchanged.

## fsync batching

`Flezha.State.FsyncBatcher` is one GenServer (`name: __MODULE__`).
`WalWriter.handle_call({:append, ...})` casts `mark_dirty(segment_id)`
after each write; the batcher coalesces casts into a single per-tick
fsync per writer. Tick interval defaults to `5 ms` (≈ 200 Hz),
configurable via `:flezha, :agent_fsync_interval_ms`. Legal range
`[1, 1000]` ms; out-of-range values fail `init/1` loudly.

## Snapshot lifecycle

Compaction is triggered automatically by the `Flezha.State.Watchdog`
sweep on **size or age** (plus manually via `flz state rotate`):

* **Size** — un-snapshotted WAL bytes past the resolved
  `compact_after_bytes` (default **8 MiB**; per-Operation override via
  the SDK's `compact_after_bytes()`, global via
  `:flezha, :default_compact_after_bytes`).
* **Age** — the oldest un-snapshotted WAL record is older than
  `:flezha, :compact_after_age_ms` (default 300 000 ms; `0` disables),
  so low-throughput Operations still get periodic baselines.

Both read the `WalWriter`'s in-memory accounting (no extra disk scan),
share the Watchdog's in-flight `compactions` map as the re-enqueue guard
(size+age can never double-enqueue), and emit
`[:flezha, :state, :compaction_trigger]` with `trigger: :size | :age`.

1. Executor (or compaction trigger) requests a snapshot — controller ships
   `CompactSegment` to the SDK over the control channel.
2. SDK clones `Self::State` under the sticky CAS, postcard-serializes
   it, ships the bytes back via `segment_state_snapshot`.
3. `StreamChannel` decodes the frame and calls
   `Flezha.State.Snapshot.write_snapshot/4`:
   - opens `<key>.snap.{seq}.tmp`
   - writes `<<header::binary, snapshot_bytes::binary, crc32::32-big>>`
   - `:file.sync/1` + `:file.close/1`
   - `:file.rename/2` to `<key>.snap.{seq}`
   - `WalWriter.truncate_up_to/2` removes records `≤ seq`.

Crash safety: the rename is atomic; a crash mid-write leaves a stale
`.tmp` (filtered out by `Registry.scan_dir/1`). A crash between rename
and truncate is also safe — applying the snapshot then folding the
already-folded WAL records is idempotent (umbrella commitment on
`apply_change`).

## Retention sweeper

`Flezha.State.Retention` runs every `:flezha,
:time_travel_retention_check_interval_ms` (default 60 000 ms). For each
Segment under `Registry.all_segments/0`:

1. `retention_hours = Flezha.State.PersistenceResolver.retention_hours(operative_class)`
2. If 0: keep only the latest snapshot, truncate WAL to its seq.
3. Otherwise: find the latest snapshot whose mtime is `≤ now -
   retention_hours * 3600`. That seq is the "oldest snapshot to keep";
   reap older snapshots and rewrite the WAL keeping records with
   `seq > kept_seq`.

WAL rewrite is `segment.wal.rewriting → rename` so a mid-rewrite crash
leaves the original WAL intact.

## Flow control

### Credit-grant (primary)

Executor→controller flow control on the state stream is credit-based:
park, never drop. On the first `segment_state_change` frame of a stream
attach, the `StreamChannel` offers `CreditWindowInit{stream_id,
window_bytes, low_watermark_bytes}` over the control channel (kill
switch: `FLEZHA_CREDIT_FLOW_CONTROL=false` skips the offer). The window
resolves per-Operative via `PersistenceResolver.credit_window_bytes/2`
(`:credit_window_bytes` override map, default `wal_high_watermark_bytes`);
the `stream_id` is an opaque session id minted fresh per attach. The SDK
acks by returning `CreditWindowAck` as the command's response; on ack the
session is *credit-speaking* and the channel enforces a per-stream byte
ledger:

* **Debit on receipt** — every state-change frame's wire size is debited;
  the controller's un-fsynced buffer per stream is bounded by
  `window_bytes` (a frame arriving on an empty buffer is admitted even if
  larger than the window — single-frame overshoot).
* **Fsync mints credit** — `WalWriter` attributes each written frame's
  wire bytes to the debiting channel (`credit_pending`, session-tagged)
  and reports them back on durable drain (`{:credit_drained, session,
  bytes}` on fsync/truncate; immediately for dedup/gap/shed/realign/
  stale-refusal drops that never occupy the buffer). One batched `CreditGrant` goes out when
  the freed credit brings the un-fsynced buffer below
  `low_watermark_bytes` — never per record; a stalled fsync mints nothing.
* **Sender parks on exhaustion** — the SDK (`sdk/rust/src/segment/credit.rs`,
  `sdk/java .../state/CreditWindow.java`) debits each frame before the
  sink write and parks the flush path (under the buffer lock, so
  producers throttle at the buffer boundary) until the next grant. Parks
  are bounded-wait loops; the `credit_parked` counter advances while
  parked and ships in `data_heartbeat` metadata, so a credit-parked
  executor classifies as fed-but-parked, not silently frozen. Grants and
  heartbeats ride threads that never park.
* **Reconnect resets the ledger** — a re-attach re-runs the handshake
  with a fresh session id; drain reports and grants tagged with a dead
  session are discarded on both sides.
* **Overrun is an anomaly** — a frame past the exhausted ledger is a
  protocol violation: shed at the channel (the seq gap drives the
  existing resync repair), counted in `credit_overrun_total`, and
  emitted as `[:flezha, :state, :credit_anomaly]`.

Soak-proven against the 06-21 mock-Kraken load
(`test/load/credit_soak_test.exs`, ≥1100 msg/s with a deliberately
slowed fsync path): flat controller memory, zero sheds, zero loss,
park/resume cycling visible in counters.

### Legacy status flags (pre-credit sessions only)

For sessions that never complete the credit handshake, `WalWriter`
keeps the advisory path byte-for-byte: crossing
`:flezha, :wal_high_watermark_bytes` (default 16 MiB) emits a
`[:flezha, :state, :wal, :backpressure]` telemetry event AND ships
`SegmentMailboxStatus{status: "full"}` to the SDK (routed via the
writer's `owning_executor`); draining below `wal_low_watermark_bytes`
(default `high_watermark / 2`) ships `"open"`. The hysteresis prevents
oscillation. Credit-speaking sessions get the telemetry event only —
no wire signal. SDK-side the flags are tolerated and dropped
(compatibility shims); the send path is bounded by local mailbox
policies plus the credit window.

### Hard in-memory bound (shed — last-resort anomaly path)

A hard bound, strictly above the soft watermark, caps the writer's own
memory: `append_async/6` sheds (drops) the incoming Change when either
`pending_bytes` reaches `:flezha, :wal_hard_watermark_bytes` (default
`4 ×` the soft watermark) **or** the writer's mailbox depth reaches
`:flezha, :wal_pending_msg_cap` (default `5_000` — a cast flood grows
the mailbox even when each record is tiny). Each shed increments
`wal_shed_total` (per-Segment `Metrics` counter); the resulting seq
gap drives the existing `Recovery.resync` handshake (once per shed
episode), so the SDK — authoritative for segment state — re-sends the
missing Changes. Back-pressure sheds, never blocks: the async cast path
stays non-blocking so a slow disk cannot stall the data-plane channel.

For pre-credit sessions this is the safety net against a slow disk or
an SDK ignoring the status flags. For a credit-speaking session the
ledger refuses an overrunning frame before the hard bound is reachable,
so a conformant sender never sheds — any shed recorded against a
credit-speaking session emits `[:flezha, :state, :credit_anomaly]`
(kind `:shed`) and is an alerting condition, never expected
back-pressure.

## Time-travel CLI

```
flz state list <operative_class>
flz state at <operative/operation/segment_key> <seq | iso8601-timestamp>
flz state rotate <operative/operation/segment_key>
flz state retention <operative_class> <hours>
flz state delete <operative/operation/segment_key>
```

Implemented in `cli/src/state.rs` (Rust binary) which RPCs into
`Flezha.Service.CliManager.state_*/N` on the running controller. Path
strings like `MarketOperative/OhlcOperation/M1` are resolved against
`Flezha.State.Registry.all_segments/0` to confirm a Segment exists
on disk; the live executor binding (when needed for `state rotate`) is
looked up from the runtime ProcessHub registry.

`flz state at` returns the assembled snapshot + change-log bytes plus
the resolved terminal seq. The full SDK-side replay (folding bytes into
JSON-serialized terminal state for human display) is wired on top of
this engine.

`flz state retention` writes via `Application.put_env(:flezha,
:time_travel_retention_hours, %{<OperativeClass> => hours}, persistent: true)`;
the next `Retention` tick reads the new value. Persistence across an
Application boot requires writing it to `runtime.exs`.

## Dynamic-segment hard delete

Dynamic segmenting adds a destructive delete path. When a
dynamic-mode Operation opts into an idle TTL (`segment_idle_ttl() ->
Some(ttl)`; default is `None` = no eviction), the SDK reaper evicts an
idle, persistence-enabled Segment by shipping a fire-and-forget
`delete_segment_state` frame on the secondary stream (payload-id
`Flezha.State.Wire.payload_id_delete_segment_state/0`; body is the bare
structured `segment_id`). `Flezha.Executor.StreamChannel` routes it to
`Flezha.State.Supervisor.delete_segment/1`, which stops the per-Segment
`WalWriter`, drops any pending `FsyncBatcher` dirty mark, and unlinks the
`{key}.wal`, `{key}.snap.{seq}`, and `{key}.outbox.dedup` files
(`Flezha.State.Registry.delete_files/1`). The delete is **idempotent** —
deleting a never-seen or already-gone segment succeeds.

A later input for the same key cold-starts a fresh Segment (new WAL from
seq 0); there is **no** restore-from-disk on recreate. The operator-driven
counterpart, `flz state delete <operative/operation/key>`, calls the same
`delete_segment/1` and returns a confirmation (the reaper path is
fire-and-forget and reads no reply).

## Crash-recovery semantics

| Crash window | Recovery action at next boot |
|---|---|
| WAL append, before fsync | Last record may be lost; `scan_for_last_valid_seq/2` truncates partial-write tail. SDK re-ships records with `seq <= last_persisted_seq` which the WalWriter silently drops as duplicates. |
| Snapshot `.tmp` write | Stale `.tmp` ignored by `Registry.scan_dir/1`; overwritten on next compaction. |
| Snapshot rename, before WAL truncate | Snapshot is durable; WAL still holds covered records. Restore applies snapshot then folds the redundant records — `apply_change` is idempotent. |
| WAL `.rewriting` from retention or truncation | `.rewriting` files filtered by `Registry.scan_dir/1`; rewrite re-runs on the next sweep. |

## Configuration knobs

| Key | Default | Meaning |
|---|---|---|
| `:flezha, :state_root` | `"app_data/state"` | Root for per-Segment files. |
| `:flezha, :agent_fsync_interval_ms` | `5` | `FsyncBatcher` tick cadence. Range [1, 1000]. |
| `:flezha, :wal_high_watermark_bytes` | `16 * 1024 * 1024` | Soft back-pressure trigger (signals SDK). |
| `:flezha, :wal_low_watermark_bytes` | `high / 2` | Back-pressure release. |
| `:flezha, :wal_hard_watermark_bytes` | `high * 4` | Hard un-fsynced-bytes bound; past it `append_async` sheds. |
| `:flezha, :wal_pending_msg_cap` | `5_000` | Hard mailbox-depth bound; past it `append_async` sheds. |
| `:flezha, :time_travel_retention_check_interval_ms` | `60_000` | `Retention` tick cadence. |
| `:flezha, :time_travel_retention_hours` | `%{}` (default 6 from PersistenceResolver) | Per-Operative retention overrides. |
| `:flezha, :default_time_travel_retention_hours` | `6` | Global default when no override. |
| `:flezha, :default_compact_after_bytes` | `8 * 1024 * 1024` | Per-Operation compaction size threshold (env `FLEZHA_COMPACT_AFTER_BYTES`). |
| `:flezha, :compact_after_age_ms` | `300_000` | Age floor for compaction; `0` disables (env `FLEZHA_COMPACT_AFTER_AGE_MS`; `0` in `:test`). |
| `:flezha, :operative_persistence` | `%{}` | Per-Operative-class executor override (`true` / `false`). |
| `:flezha, :output_watchdog_interval_ms` | `10_000` | `Watchdog` sweep cadence. Min 100 ms. |
| `:flezha, :output_stall_timeout_ms` | `%{}` (default 60 000) | Per-Operative-class output-staleness window; `0` disables. |
| `:flezha, :default_output_stall_timeout_ms` | `60_000` | Global default when no per-class override. |
| `:flezha, :compaction_timeout_ms` | `30_000` | `CompactSegment` → snapshot handshake bound; `0` disables. |

### Self-healing recovery (`Flezha.State.Watchdog` + `Flezha.State.Recovery`)

A live Segment's persistence/output path must not be able to stall
silently and permanently. Three failure edges now recover via the shared
`RestoreInitState` resync (`Flezha.State.Recovery.resync/2`) and emit
telemetry:

* **Seq gap (populated WAL)** — a Change with `seq > last_persisted_seq
  + 1` while `last_persisted_seq >= 0` is no longer a terminal
  `{:error, {:gap}}`; `Flezha.Executor.StreamChannel` emits
  `[:flezha, :state, :seq_gap]` and resyncs so the SDK rewinds and
  resends from `terminal_seq + 1`.
* **Output staleness** — `Watchdog` flags a Segment that is still
  receiving Changes but whose persisted seq froze past
  `output_stall_timeout_ms`, emits `[:flezha, :state, :output_stall]`,
  and resyncs. A genuinely idle Segment (no Changes arriving) is not
  flagged.
* **Compaction timeout** — a `CompactSegment` whose snapshot does not
  return within `compaction_timeout_ms` emits
  `[:flezha, :state, :compaction_timeout]` and resyncs; compaction stays
  retriable.

### Baseline recovery: push vs pull

The resync direction is chosen by who holds authoritative history
(`Flezha.State.Recovery.dispatch_resync`):

* **Controller holds a baseline** (snapshot and/or WAL records) →
  **push** `RestoreInitState` (the existing handshake).
* **Controller holds no baseline** while the executor is live → **pull**
  a full-state snapshot via `CompactSegment`. The dispatch path never
  ships a `RestoreInitState` whose assembled payload is empty
  (`Restore.has_persisted_state?/1`, which treats a zero-byte WAL as no
  state) — an empty payload would rebuild the live Segment to
  `init_state` and wipe its accumulated state.

The pull is data-plane-triggered on cold takeover: the first
`segment_state_change` arriving with an empty WAL
(`last_persisted_seq == -1`), `seq > 0`, and no executor-restart realign
puts the `WalWriter` into **awaiting-baseline** — the frame is shed
(counted in `wal_shed_total`, not an error, and the SDK is NOT
disconnected), `[:flezha, :state, :baseline_seed]` fires once, and one
`CompactSegment` is dispatched per episode. Further gap frames shed
quietly until the pulled `segment_state_snapshot` lands; its seq becomes
`last_persisted_seq` (the snapshot encodes the shed deltas' effects) and
the executor's next delta appends contiguously. A stuck pull is bounded
by the compaction-timeout sweep. A gap on a *populated* WAL remains a
protocol violation (`{:error, {:gap, ...}}`).

## Executor-restart realign is owner-bound

Within one executor's lifetime the per-Segment seq is monotonic, so a
Change from a *different* session than the one that wrote the current
epoch, at a non-contiguous seq, signals an executor restart: the
`WalWriter` realigns — truncates the WAL, drops snapshots, adopts the
new seq base — and emits `[:flezha, :state, :wal_realign]`. A
same-session low seq stays a legitimate dedup.

The realign is honoured **only from the Segment's current owner
binding**. A newest-wins takeover unbinds the superseded session from
the write path before binding the new one (see
`docs/resilience/executor_identity.md` "The WAL binds to the owner"):
fenced `StreamChannel`s refuse durable-write forwards at the binding,
and the `WalWriter` refuses frames from a session marked superseded
(`FleetManager.superseded_session?/1`) with `{:error, :stale_writer}`
— consulted only when the frame's session differs from the epoch
owner, so the owning session's append path pays no per-record cost.
Refusals emit `[:flezha, :state, :stale_writer_refused]` once per
(Segment, superseded session) episode and count into
`stale_writer_refused_total`; they never spawn a resync (the zombie is
not a party worth repairing toward) and release any credit debit
immediately.

## Persisted `executor_id` is advisory across controller restarts

The `executor_id` field carried in an operative-binding record (replayed
into `Flezha.Operative.Overwatch.init/1` from ProcessHub's durable
registry at boot) is **advisory only**. It identifies the executor the
operative was bound to BEFORE the current controller process started.

On every cold start the Overwatch calls
`OperativeManager.assign_executor/2` inside
`handle_continue({:bind_executor, …}, _)` to re-resolve a live
executor for the operative's `factory_id`. The persisted value is used
for diagnostic logging only (a `rebound from stale executor X to live
Y` line fires when the live executor differs).

Control-plane calls (`AddOperative`, `:ping_executor`, `start_operative_sdk`)
are issued against the re-resolved live executor, never against the
persisted value directly. The bounded-retry governor (see
`docs/operative_architecture.md` "Overwatch lifecycle") re-resolves on
every retry attempt so a fresh executor that comes online during a
backoff window is picked up automatically.

No on-disk format change accompanies this — the field is still
written and read with the same layout. The clarification covers only
the runtime semantics; the source of truth for the live binding is
ProcessHub's registry, not the persisted record.
