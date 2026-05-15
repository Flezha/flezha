# Controller state persistence

Implementation notes for the per-Partition WAL, snapshot, and
time-travel infrastructure on the Elixir controller. Settles details
deferred from `relay-state-persistence/design.md`; keep this in sync
with `lib/flezha/state/`.

> Per-Partition WAL persistence (this doc) is independent of and
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
    {relay_class}/
      {shard_id}/
        {partition_key}.wal
        {partition_key}.snap.{seq}
        {partition_key}.outbox.dedup
```

The path triple `(relay_class, shard_id, partition_key)` uniquely
identifies a Partition's state. Persistence does not include the
`executor_id` — that is an addressing concern at the SDK boundary. When
an executor pod restarts and rotates its `executor_id`, state under the
flat layout is preserved and shipped to the new executor on Partition
attach via `RestoreInitState`. The one-time
`Flezha.Boot.StatePathMigrator` runs at `Application.start/2` to migrate
an older executor-keyed layout into the flat one.

`partition_key` is URL-safe-base64-encoded with a leading `_` when it
contains characters outside `[A-Za-z0-9_.-]`. Round-trippable via
`Flezha.State.Registry.encode_partition_key/1` and `decode_partition_key/1`.

Configurable root via `:flezha, :state_root` (default `app_data/state`).

## Module map

| Module | Role |
|---|---|
| `Flezha.State.Registry` | Path resolution + directory enumeration. No GenServer, no I/O beyond `File.ls/1`. |
| `Flezha.State.Wire` | Wire-frame codec for `partition_state_change` / `partition_state_snapshot` payload-ids. |
| `Flezha.State.Metrics` | Per-Partition `:counters` references; lazy allocation in a `:ets` table. |
| `Flezha.State.Outbox` | `last_persisted_seq` cache file (`<partition_key>.outbox.dedup`). |
| `Flezha.State.PersistenceResolver` | Per-Relay config resolution (`relay_persistence`, `time_travel_retention_hours`, `compact_after_bytes`). |
| `Flezha.State.Supervisor` | `DynamicSupervisor` for per-Partition `WalWriter` GenServers + a process registry for name lookup. |
| `Flezha.State.WalWriter` | One GenServer per Partition. Owns the WAL file handle. |
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

## fsync batching

`Flezha.State.FsyncBatcher` is one GenServer (`name: __MODULE__`).
`WalWriter.handle_call({:append, ...})` casts `mark_dirty(partition_id)`
after each write; the batcher coalesces casts into a single per-tick
fsync per writer. Tick interval defaults to `5 ms` (≈ 200 Hz),
configurable via `:flezha, :agent_fsync_interval_ms`. Legal range
`[1, 1000]` ms; out-of-range values fail `init/1` loudly.

## Snapshot lifecycle

1. Executor (or compaction trigger) requests a snapshot — controller ships
   `CompactPartition` to the SDK over the control channel.
2. SDK clones `Self::State` under the sticky CAS, postcard-serializes
   it, ships the bytes back via `partition_state_snapshot`.
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
Partition under `Registry.all_partitions/0`:

1. `retention_hours = Flezha.State.PersistenceResolver.retention_hours(relay_class)`
2. If 0: keep only the latest snapshot, truncate WAL to its seq.
3. Otherwise: find the latest snapshot whose mtime is `≤ now -
   retention_hours * 3600`. That seq is the "oldest snapshot to keep";
   reap older snapshots and rewrite the WAL keeping records with
   `seq > kept_seq`.

WAL rewrite is `partition.wal.rewriting → rename` so a mid-rewrite crash
leaves the original WAL intact.

## Back-pressure

`WalWriter` tracks `pending_bytes` (queued-but-not-fsynced bytes). When
it crosses `:flezha, :wal_high_watermark_bytes` (default 16 MiB) the
writer emits a `[:flezha, :state, :wal, :backpressure]` telemetry event
AND calls `Flezha.Relay.Overwatch.notify_partition_backpressure/2`
which ships a `PartitionMailboxStatus{status: "full"}` control command
to the SDK. When `pending_bytes` falls below `wal_low_watermark_bytes`
(default `high_watermark / 2`), a corresponding `"open"` notification
fires.

The hysteresis prevents oscillation around the threshold. The SDK's
per-Partition mailbox observer translates the signal into the channel's
declared `OverflowPolicy` (see `relay-state-persistence-rust`).

## Time-travel CLI

```
flz state list <relay_class>
flz state at <relay/shard/partition_key> <seq | iso8601-timestamp>
flz state rotate <relay/shard/partition_key>
flz state retention <relay_class> <hours>
```

Implemented in `cli/src/state.rs` (Rust binary) which RPCs into
`Flezha.Service.CliManager.state_*/N` on the running controller. Path
strings like `MarketRelay/OhlcShard/M1` are resolved against
`Flezha.State.Registry.all_partitions/0` to confirm a Partition exists
on disk; the live executor binding (when needed for `state rotate`) is
looked up from the runtime ProcessHub registry.

`flz state at` returns the assembled snapshot + change-log bytes plus
the resolved terminal seq. The full SDK-side replay (folding bytes into
JSON-serialized terminal state for human display) is wired on top of
this engine.

`flz state retention` writes via `Application.put_env(:flezha,
:time_travel_retention_hours, %{<RelayClass> => hours}, persistent: true)`;
the next `Retention` tick reads the new value. Persistence across an
Application boot requires writing it to `runtime.exs`.

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
| `:flezha, :state_root` | `"app_data/state"` | Root for per-Partition files. |
| `:flezha, :agent_fsync_interval_ms` | `5` | `FsyncBatcher` tick cadence. Range [1, 1000]. |
| `:flezha, :wal_high_watermark_bytes` | `16 * 1024 * 1024` | Back-pressure trigger. |
| `:flezha, :wal_low_watermark_bytes` | `high / 2` | Back-pressure release. |
| `:flezha, :time_travel_retention_check_interval_ms` | `60_000` | `Retention` tick cadence. |
| `:flezha, :time_travel_retention_hours` | `%{}` (default 6 from PersistenceResolver) | Per-Relay retention overrides. |
| `:flezha, :default_time_travel_retention_hours` | `6` | Global default when no override. |
| `:flezha, :default_compact_after_bytes` | `64 * 1024 * 1024` | Per-Shard compaction threshold. |
| `:flezha, :relay_persistence` | `%{}` | Per-Relay-class executor override (`true` / `false`). |

## Persisted `executor_id` is advisory across controller restarts

The `executor_id` field carried in a relay-binding record (replayed
into `Flezha.Relay.Overwatch.init/1` from ProcessHub's durable
registry at boot) is **advisory only**. It identifies the executor the
relay was bound to BEFORE the current controller process started.

On every cold start the Overwatch calls
`RelayManager.assign_executor/2` inside
`handle_continue({:bind_executor, …}, _)` to re-resolve a live
executor for the relay's `factory_id`. The persisted value is used
for diagnostic logging only (a `rebound from stale executor X to live
Y` line fires when the live executor differs).

Control-plane calls (`AddRelay`, `:ping_executor`, `start_relay_sdk`)
are issued against the re-resolved live executor, never against the
persisted value directly. The bounded-retry governor (see
`docs/relay_architecture.md` "Overwatch lifecycle") re-resolves on
every retry attempt so a fresh executor that comes online during a
backoff window is picked up automatically.

No on-disk format change accompanies this — the field is still
written and read with the same layout. The clarification covers only
the runtime semantics; the source of truth for the live binding is
ProcessHub's registry, not the persisted record.
