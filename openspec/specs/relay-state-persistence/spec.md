# relay-state-persistence Specification

## Purpose

The relay-state-persistence capability provides agent-side, per-Partition durability for Relay state. The agent maintains an append-only write-ahead log (WAL) per Partition under `app_data/state/`, fsynced on a configurable cadence, with asynchronous size-triggered compaction into snapshot files. On Partition reconnect, the agent assembles a `RestoreInitState{partition_id, snapshot_bytes, change_log_bytes}` payload and ships it to the SDK over the control channel; the SDK applies the snapshot and folds the WAL tail to recover terminal state. Persistence is per-Relay-class opt-in via runtime config or a Relay-class const, defaulting on. The agent retains pre-compaction WAL records for a configurable window to support time-travel queries via `flz state at`. The capability is additive to the existing `Overwatch` GenServer: persistence frames (`partition_state_change`, `partition_state_snapshot`) flow alongside the existing protocol; no SDK or operator code outside this capability is required.

## Requirements

### Requirement: Per-Partition append-only WAL on the agent

Each Partition whose Relay has `persistence_enabled = true` and whose Shard's `Change` is not `NoChange` SHALL have a per-Partition write-ahead log on the agent at `app_data/state/{compute_engine_id}/{relay_class}/{shard_id}/{partition_key}.wal`. Records SHALL be `[seq: u64 BE][len: u32 BE][header: 16B][change_bytes][crc32: u32 BE]` where `len = 16 + byte_size(change_bytes)` and `crc32 = :erlang.crc32(<<header::binary, change_bytes::binary>>)`. The WAL SHALL be written by the agent's persistence module (`lib/flezha/state/wal_writer.ex`) and SHALL never be written directly by user code or by the SDK.

`seq` SHALL be strictly monotonic per Partition and assigned by the SDK. The agent SHALL accept any incoming Change frame whose `seq == last_persisted_seq + 1` for that Partition; SHALL silently drop (with a `dedup_drops_total` counter increment) any frame whose `seq <= last_persisted_seq`; AND SHALL reject as a protocol violation any frame whose `seq > last_persisted_seq + 1` (returning `{:error, {:gap, expected, got}}` to the SDK).

#### Scenario: WAL allocated lazily on first append

- **WHEN** the agent receives the first `partition_state_change` frame for a Partition that does not yet have a WAL file
- **THEN** the persistence module SHALL create the directory `app_data/state/{ce_id}/{relay}/{shard}/`, open the WAL file at `{partition_key}.wal` with `[:raw, :read, :write, :binary, :append]`, and start a `WalWriter` GenServer for the Partition

#### Scenario: Strictly monotonic seq accepted in order

- **WHEN** the SDK ships frames with `seq = 1, 2, 3` for a fresh Partition
- **THEN** the agent SHALL append all three records to the WAL in order; the file's `last_persisted_seq` SHALL equal 3 after the third append

#### Scenario: Re-shipped seq is silently deduped

- **WHEN** the SDK ships a frame with `seq = 5` for a Partition whose `last_persisted_seq = 7` (re-ship after SDK crash that lost the in-memory cursor)
- **THEN** the agent SHALL NOT append the duplicate; the agent SHALL increment `dedup_drops_total`; the wire reply SHALL be `:ok` (so the SDK does not retry)

#### Scenario: Out-of-order seq is rejected

- **WHEN** the SDK ships a frame with `seq = 10` for a Partition whose `last_persisted_seq = 7`
- **THEN** the agent SHALL refuse the append and reply `{:error, {:gap, expected: 8, got: 10}}`; the SDK SHALL treat this as a protocol violation and disconnect (matching the existing reconnect semantics)

#### Scenario: Crash-safe append via crc

- **WHEN** the agent process crashes mid-record-write, leaving a partial record at the file tail
- **THEN** on next agent boot, `WalWriter::scan_for_last_valid_seq/1` SHALL detect the partial record (crc32 mismatch on the trailing bytes), truncate the file to the last valid record, and resume normal operation; `last_persisted_seq` SHALL equal the seq of the last valid record

### Requirement: Snapshot lifecycle with atomic rename and WAL truncation

When the agent receives a `partition_state_snapshot` frame from the SDK (typically in response to a `CompactPartition` command), the agent SHALL: (1) write the snapshot bytes to `{partition_key}.snap.{seq}.tmp` with `[:raw, :write, :binary]`; (2) `:file.sync/1` the temp file; (3) atomically rename to `{partition_key}.snap.{seq}` via `:file.rename/2`; (4) call `WalWriter.truncate_up_to(seq)` which rewrites the WAL dropping records with `seq' <= seq` and atomically renames the rewritten file into place.

If the agent crashes between steps 3 and 4, the next boot SHALL find both the new snapshot and the un-truncated WAL; `lib/flezha/state/registry.ex::scan_dir/1` SHALL detect the situation, prefer the latest snapshot, and mark the WAL records ≤ `snapshot.seq` as redundant. Replay-from-this-state is correct: applying the snapshot then folding the WAL produces a state where the redundant records' apply_change calls are no-ops (because the snapshot already encodes their effects).

#### Scenario: Snapshot rename atomic

- **WHEN** the agent receives a `partition_state_snapshot` frame
- **THEN** an observer reading the directory SHALL see either the prior snapshot file OR the new snapshot file at any moment, but not a half-written file

#### Scenario: WAL truncation removes pre-snapshot records

- **WHEN** a snapshot at `seq = 100` lands and the WAL contains records seqs 1-150
- **THEN** after `WalWriter.truncate_up_to(100)`, the WAL SHALL contain only records seqs 101-150; the snapshot file SHALL contain the post-fold state at seq 100

#### Scenario: Crash mid-truncate leaves both files; replay is correct

- **WHEN** the agent crashes between snapshot-rename and WAL-truncate, leaving snapshot at seq 100 and full WAL with seqs 1-150
- **THEN** on next agent reconnect, the agent SHALL ship `RestoreInitState` with the snapshot bytes AND the full WAL bytes (records 1-150); the SDK SHALL apply the snapshot (state at seq 100) then fold records 1-150 — records 1-100 are no-ops because the snapshot already encodes their effects; records 101-150 advance the state correctly; terminal state matches the pre-crash state at seq 150

### Requirement: Asynchronous compaction triggered by per-Partition WAL size

When a Partition's WAL exceeds `compact_after_bytes` (per-Shard, default 64 MiB; the SDK ships the per-Shard value to the agent in the `AddProcessor` metadata), the agent SHALL trigger asynchronous compaction by enqueuing `Overwatch.handle_continue(:compact_partition, partition_id)`. The continuation SHALL ship `CompactPartition{partition_id}` to the SDK over the control channel.

The SDK SHALL acquire the Partition's sticky CAS, clone `Self::State`, release the CAS, serialize the clone (postcard), and ship `partition_state_snapshot{partition_id, seq, header, snapshot_bytes, crc32}` back to the agent. The agent SHALL handle the snapshot per the snapshot-lifecycle requirement above. The agent SHALL reply `CompactPartitionResponse{partition_id, snapshot_seq, snapshot_bytes_size}` to confirm.

The hot path SHALL pay only the brief sticky-CAS hold during the state clone — microseconds, not the seconds-to-minutes a synchronous full-state snapshot would take.

#### Scenario: Compaction triggered by size threshold

- **WHEN** a Partition's `wal_size_bytes` grows past 64 MiB (the default `compact_after_bytes`)
- **THEN** the agent SHALL enqueue `:compact_partition` for that Partition without blocking new Change ingestion

#### Scenario: Per-Shard override propagated and honored

- **WHEN** a Shard declares `compact_after_bytes() -> usize { 16 * 1024 * 1024 }` in the SDK
- **THEN** the SDK SHALL ship `compact_after_bytes = 16777216` for this Shard in `AddProcessor` metadata; the agent SHALL trigger compaction for this Shard's Partitions when WAL grows past 16 MiB

#### Scenario: Hot path holds CAS only for state clone

- **WHEN** the SDK receives `CompactPartition` and clones the state
- **THEN** the SDK SHALL acquire the sticky CAS, clone, release the CAS within the same `Shard::run`-equivalent invocation, and serialize the clone in a non-runner context; the partition's input throughput SHALL NOT drop below 90% of pre-compaction throughput during the clone

### Requirement: Restart restores state via RestoreInitState handshake

On reconnect, for each Partition whose Relay has `persistence_enabled = true` AND whose agent-side state is non-empty (snapshot file or WAL records exist), the agent SHALL: (1) call `Restore.assemble_payload(partition_id)` to read the latest snapshot bytes plus the tail-WAL bytes; (2) ship `RestoreInitState{partition_id, snapshot_bytes, change_log_bytes}` to the SDK over the control channel; (3) BLOCK the Partition's input streaming (entry channel and inter-shard inputs targeting this Partition queue at the agent-side outbox); (4) await `RestoreInitStateResponse{partition_id, terminal_seq}` from the SDK; (5) UNBLOCK the Partition's input streaming; subsequent Changes from the SDK carry `seq = terminal_seq + 1`.

For Partitions whose Relay has `persistence_enabled = false` OR whose agent-side state is empty (no snapshot, no WAL records), the agent SHALL fall through to the existing `processor-init-state` capability flow.

`RestoreInitState` and `compute_engine_migrate` SHALL be mutually exclusive per Partition: when `compute_engine_migrate` is invoked for a Partition with persisted state, the migration path's snapshot bytes SHALL be the restoration source and `RestoreInitState` SHALL NOT be issued. The agent SHALL never issue both for the same Partition reconnect.

#### Scenario: Cold start with no persisted state

- **WHEN** a Partition reconnects and the agent has no WAL or snapshot for it AND `persistence_enabled = true`
- **THEN** `RestoreInitState` SHALL NOT be issued; the agent SHALL invoke the existing `processor-init-state` capability flow (init-state plugin chain or inline init_values)

#### Scenario: Restart with snapshot only

- **WHEN** a Partition reconnects with a snapshot at `seq = 100` and an empty WAL (post-compaction)
- **THEN** the agent SHALL ship `RestoreInitState` with the snapshot bytes and empty change_log_bytes; the SDK SHALL apply the snapshot via the runtime auto-restore; `RestoreInitStateResponse` SHALL carry `terminal_seq = 100`

#### Scenario: Restart with snapshot and tail WAL

- **WHEN** a Partition reconnects with a snapshot at `seq = 100` and WAL records seqs 101-150
- **THEN** the agent SHALL ship `RestoreInitState` with the snapshot bytes and the WAL bytes (records 101-150 framed with `[seq][len][header][bytes][crc32]`); the SDK SHALL apply the snapshot then fold records 101-150 in seq order; `RestoreInitStateResponse` SHALL carry `terminal_seq = 150`

#### Scenario: Restart with WAL only (no snapshot)

- **WHEN** a Partition reconnects with WAL records seqs 1-50 and no snapshot
- **THEN** the agent SHALL ship `RestoreInitState` with empty snapshot_bytes and WAL bytes for seqs 1-50; the SDK SHALL fold all 50 records via apply_change; `RestoreInitStateResponse` SHALL carry `terminal_seq = 50`

#### Scenario: Persistence-disabled Relay skips RestoreInitState

- **WHEN** a Partition reconnects under a Relay with `persistence_enabled = false`
- **THEN** the agent SHALL NOT call `Restore.assemble_payload` AND SHALL NOT ship `RestoreInitState`; the existing `processor-init-state` capability flow runs instead

#### Scenario: Migration takes precedence over RestoreInitState

- **WHEN** `compute_engine_migrate` is invoked for a Partition with persisted state
- **THEN** the migration path's snapshot bytes SHALL be used as the restoration source; `RestoreInitState` SHALL NOT be issued for this Partition; the SDK observes a single restoration handshake (the migration one)

#### Scenario: Input streaming blocked during restoration

- **WHEN** an entry-channel frame arrives for a Partition mid-restoration (between agent ship of `RestoreInitState` and receipt of `RestoreInitStateResponse`)
- **THEN** the agent SHALL queue the frame in its per-Partition outbox; the frame SHALL NOT ship to the SDK until restoration completes

### Requirement: Time-travel queries via WAL retention beyond latest snapshot

The agent SHALL retain pre-compaction WAL records for `time_travel_retention_hours` beyond the latest snapshot, configurable per-Relay-class via runtime config `time_travel_retention_hours.<RelayClass>` (default `6` hours globally; setting to `0` disables time travel for that Relay class). The retention sweeper (`Flezha.State.Retention`) SHALL run on `time_travel_retention_check_interval_ms` (default 60_000), iterate Partitions, and rewrite each Partition's WAL dropping records older than the cutoff.

The agent SHALL expose a query interface `Flezha.State.TimeTravel.query(partition_id, target)` where `target = {:seq, u64} | {:timestamp, datetime}`. The query SHALL: (1) locate the latest snapshot at-or-before the target; (2) read the tail-WAL records up to the target; (3) ship `RestoreInitState` to a transient SDK-side replay processor; (4) receive a `partition_state_snapshot` carrying JSON-encoded terminal state; (5) return the JSON to the caller.

The query SHALL be exposed via the `flz state at <relay/shard/partition> <seq | timestamp>` CLI subcommand.

#### Scenario: Default 6-hour retention

- **WHEN** an operator does not configure `time_travel_retention_hours`
- **THEN** the agent SHALL retain WAL records for 6 hours beyond the latest snapshot

#### Scenario: Time-travel disabled (`retention_hours = 0`)

- **WHEN** `time_travel_retention_hours.<RelayClass> = 0`
- **THEN** the retention sweeper SHALL truncate the WAL up to the latest snapshot's seq immediately on each tick; only the latest snapshot is queryable; queries for any seq < latest snapshot return `TimeTravelError{kind: :outside_retention}`

#### Scenario: Query within retention window returns state

- **WHEN** the operator runs `flz state at MarketRelay/btc/OhlcShard/M1 "2026-05-07T10:30:00Z"` AND that timestamp falls within the retention window
- **THEN** the agent SHALL load the latest snapshot at-or-before the timestamp and fold WAL records up to it; the CLI SHALL print the JSON-encoded state

#### Scenario: Query outside retention returns error

- **WHEN** the operator queries a timestamp older than the earliest available WAL record's record-time
- **THEN** the agent SHALL return `TimeTravelError{kind: :outside_retention, earliest_available: <ts>}`; the CLI SHALL print a clear error pointing at the earliest queryable point

#### Scenario: Query against persistence-off Relay errors

- **WHEN** the operator runs `flz state at <relay/shard/partition> <target>` for a Partition whose Relay has `persistence_enabled = false`
- **THEN** the agent SHALL return `TimeTravelError{kind: :partition_not_persistent, relay, shard, partition}`

#### Scenario: Live retention update applies on next sweep

- **WHEN** the operator runs `flz state retention MarketRelay 12` and the prior value was `6`
- **THEN** the runtime config SHALL be updated to 12 hours; the next retention-sweeper tick SHALL apply the new value (records older than 12 hours are deleted; records younger SHALL be retained going forward)

### Requirement: Fsync batching at configurable cadence

The agent SHALL run a single shared `Flezha.State.FsyncBatcher` GenServer per BEAM node. The batcher SHALL maintain a `MapSet` of dirty Partition IDs (Partitions whose `WalWriter` has appended bytes since the last fsync). On each tick at interval `agent_fsync_interval_ms` (default 5; configurable via runtime config in range [1, 1000]), the batcher SHALL: (1) iterate the dirty set; (2) cast `:fsync` to each `WalWriter`; (3) reset the set; (4) schedule the next tick via `Process.send_after/3`.

`WalWriter` SHALL invoke `:file.datasync/1` on its file handle when receiving the `:fsync` message and SHALL update `fsync_durability_lag_ms` to `now - oldest_pending_record_timestamp`.

#### Scenario: Default 5 ms tick = 200 Hz fsync rate

- **WHEN** an operator does not configure `agent_fsync_interval_ms`
- **THEN** the batcher SHALL tick every 5 ms (200 Hz) AND each dirty `WalWriter` SHALL fsync at most once per tick

#### Scenario: 5 ms + 5 ms = 10 ms total durability bound

- **WHEN** a Shard records a Change at time `t` under default flusher and fsync settings (`linger_ms = 5` SDK-side; `agent_fsync_interval_ms = 5` agent-side)
- **THEN** the Change SHALL be durably fsynced to disk on the agent by `t + 10 ms` modulo network round-trip and OS scheduling; `fsync_durability_lag_ms` SHALL stabilize below 10 ms under steady-state load

#### Scenario: Operator-tightened bound

- **WHEN** an operator sets `agent_fsync_interval_ms = 1`
- **THEN** the batcher SHALL tick every 1 ms (1000 Hz); the durability bound SHALL be ~2 ms total at the cost of more frequent fsyncs

### Requirement: Per-Relay persistence toggle resolution at relay startup

The agent SHALL resolve a per-Relay-class boolean `persistence_enabled` at the moment a Relay instance is added (`AddProcessor` command processed in `Overwatch.init/1`) using the following ordered rule:

1. If the agent's runtime config defines `relay_persistence.<RelayClass>` for this Relay class, use that value.
2. Otherwise, if the `AddProcessor` metadata carries a Relay-class const value (`Relay::PERSISTENCE_ENABLED`), use that value.
3. Otherwise, use the built-in default of `true`.

The resolved value SHALL be bound to the Relay instance for its entire lifetime. Subsequent changes to the operator-side config or to the SDK's Relay-class const (via SDK redeploy) SHALL NOT affect already-running instances.

When `persistence_enabled = false`, the agent SHALL NOT allocate WAL or snapshot files for any of that Relay's Partitions, SHALL ignore (drop with a counter increment) any incoming `partition_state_change` or `partition_state_snapshot` frames for those Partitions, and SHALL NOT issue `RestoreInitState` on reconnect.

#### Scenario: Default-on with no override

- **WHEN** an operator does not configure `relay_persistence.MarketRelay` AND `MarketRelay`'s `AddProcessor` metadata does not declare `PERSISTENCE_ENABLED`
- **THEN** the agent SHALL resolve `persistence_enabled = true` for `MarketRelay`

#### Scenario: Operator override takes precedence

- **WHEN** `MarketRelay`'s `AddProcessor` metadata declares `PERSISTENCE_ENABLED = true` AND the operator sets `relay_persistence.MarketRelay = false`
- **THEN** the agent SHALL resolve `persistence_enabled = false` for `MarketRelay`

#### Scenario: Mid-life config change does not affect running instance

- **WHEN** a Relay instance is running with resolved `persistence_enabled = true` AND the operator subsequently runs `flz config set relay_persistence.MarketRelay false`
- **THEN** the running instance SHALL continue with `persistence_enabled = true`; the new value SHALL take effect only after the SDK pod hosting the instance is restarted (the new instance's `AddProcessor` re-runs the resolution)

### Requirement: 16-byte version header on persisted blobs

Every WAL record and snapshot file SHALL carry a 16-byte header preceding the payload bytes:

- bytes 0:1 — `sdk_lang: u8` (1 = Rust, 2 = Java; reserved range 3–254)
- bytes 1:5 — `sdk_version: u32 BE` (semver-encoded)
- bytes 5:9 — `schema_version: u32 BE` (per-Shard, user-controlled)
- bytes 9:10 — `format_id: u8` (1 = postcard, 2 = bincode, 3 = rkyv-zerocopy, 4 = kryo-registered)
- bytes 10:12 — reserved, MUST be zero
- bytes 12:16 — `crc32: u32 BE` over the payload bytes (change_bytes for WAL records; snapshot_bytes for snapshot files)

The agent SHALL stamp the header at write time using the SDK-supplied values (the SDK ships the header pre-stamped in the wire frame; the agent writes it verbatim). The SDK SHALL verify the header at read time; the agent SHALL NOT verify the header except for the leading WAL record-framing crc32 over `header + change_bytes`.

#### Scenario: Header stamped verbatim from SDK

- **WHEN** the SDK ships a `partition_state_change` frame with header `[0x01, ...sdk_version..., ...schema_version..., 0x01, 0x00, 0x00, ...crc32...]`
- **THEN** the agent SHALL append the WAL record byte-for-byte; no agent-side header rewriting SHALL occur

#### Scenario: Cross-language load fails loud at SDK side

- **WHEN** a Java SDK reconnects to a Partition whose WAL was written by a Rust SDK (header `sdk_lang = 1`)
- **THEN** on `RestoreInitState` decode, the Java SDK SHALL fail with a cross-language-mismatch error and SHALL NOT proceed; the agent's role is unchanged (it ships bytes, the SDK enforces the language gate)

### Requirement: Idempotent emits with stable dedup keys

The Rust SDK runtime SHALL automatically attach `flezha.dedup_key = "{relay_class}/{shard_id}/{partition_key}/{change_seq}"` to outgoing plugin-bound emit metadata for any emit issued during a `state.apply(...)`-driven `Shard::run` invocation. The agent's plugin-provider path SHALL NOT alter or strip this metadata key; it SHALL flow through to the sink verbatim.

The Kafka producer plugin (`Flezha.Plugin.Provider.Kafka`) SHALL use the `flezha.dedup_key` value as the Kafka producer key (taking precedence over any other configured keying strategy). Other plugin providers MAY use the key for sink-specific dedup purposes; the contract is that the key is opaque metadata.

User-supplied metadata via `ctx.emit_with_meta(value, meta)` SHALL have its `flezha.dedup_key` entry silently dropped (matching the existing reserved-key behavior for `channel`); the SDK runtime stamps its own value.

#### Scenario: Dedup key attached automatically

- **WHEN** a Shard at `(MarketRelay/btc, OhlcShard, M1)` records a Change at `seq = 12345` AND the same `Shard::run` invocation calls `ctx.emit::<BarClosed>(...)` to a Kafka-bound channel
- **THEN** the outgoing Kafka producer SHALL receive the message with key `"MarketRelay/OhlcShard/M1/12345"`

#### Scenario: Replayed Change preserves dedup key

- **WHEN** a Partition is restored from snapshot + WAL and re-issues an emit for `seq = 12345`
- **THEN** the re-issued Kafka producer message SHALL carry the identical key `"MarketRelay/OhlcShard/M1/12345"` as the original emit; downstream sink dedup (e.g. Kafka log compaction by key) SHALL collapse the duplicates

#### Scenario: Agent-side outbox dedupes by (partition, change_seq)

- **WHEN** the SDK ships `partition_state_change` with `seq = 12345` to the agent twice for the same Partition (once before crash, once after replay)
- **THEN** the agent SHALL persist exactly one record; the duplicate SHALL be detected via `seq <= last_persisted_seq` and discarded silently with `dedup_drops_total += 1`

#### Scenario: User-supplied dedup_key is dropped

- **WHEN** a Shard calls `ctx.emit_with_meta(value, %{"flezha.dedup_key" => "custom"})`
- **THEN** the user's `flezha.dedup_key` entry SHALL be silently dropped; the SDK runtime SHALL stamp `"{relay}/{shard}/{partition}/{seq}"` as the dedup key

### Requirement: Per-Partition persistence metrics

Each Partition's metric set SHALL expose six counters via `:counters` references, accessible from `Flezha.State.Metrics.to_map(partition_id)` and exported through the existing `Flezha.Plugin.Provider.Metrics` path:

- `persisted_changes_total` — monotonic; incremented per Change appended to the WAL (post-coalesce, post-dedup).
- `persisted_bytes_total` — monotonic; incremented by the byte length of each appended record.
- `wal_size_bytes` — gauge; current WAL file size (read from `:file.read_file_info/1` after each append and after each retention-sweep rewrite).
- `snapshot_seq` — gauge; seq of the latest snapshot file (or 0 if none).
- `flush_latency_ms_p99` — gauge; rolling 99th-percentile of `now - record_received_ts` measured per fsync, computed via a 1024-bucket reservoir sampler.
- `fsync_durability_lag_ms` — gauge; `now - oldest_pending_record_ts` at each `FsyncBatcher` tick.

The counters SHALL use `Ordering::Relaxed` (the BEAM `:counters` API does not expose ordering modes; this is satisfied by the underlying implementation). External metric systems (Prometheus, StatsD) integration is out of scope for this requirement; the existing `Flezha.Plugin.Provider.Metrics` path is the export surface.

#### Scenario: Counters initialized at Partition construction

- **WHEN** a Partition is constructed with `persistence_enabled = true`
- **THEN** all six counters SHALL be allocated; `persisted_changes_total = 0`, `persisted_bytes_total = 0`, `wal_size_bytes = 0`, `snapshot_seq = 0` (no snapshot yet), `flush_latency_ms_p99 = 0`, `fsync_durability_lag_ms = 0`

#### Scenario: Counters update after Change append

- **WHEN** the agent appends 10 Changes totaling 500 bytes to a Partition's WAL
- **THEN** `persisted_changes_total` SHALL increase by 10; `persisted_bytes_total` SHALL increase by `>= 500` (record framing overhead included; ≈ `500 + 10 * (8 + 4 + 16 + 4) = 820` bytes in practice); `wal_size_bytes` SHALL equal the file size after the appends

#### Scenario: Counters survive agent restart

- **WHEN** the agent is restarted and reads back the WAL files
- **THEN** `persisted_changes_total` and `persisted_bytes_total` SHALL be recomputed from the WAL records (scanning); `snapshot_seq` SHALL be the highest snapshot file's seq; `wal_size_bytes` SHALL match the file size; `flush_latency_ms_p99` and `fsync_durability_lag_ms` SHALL reset to 0

### Requirement: Back-pressure via OverflowPolicy when WAL outpaces fsync

The agent SHALL monitor each `WalWriter`'s `pending_bytes` (queued-but-not-fsynced bytes). When `pending_bytes >= wal_high_watermark_bytes` (default 16 MiB), the agent SHALL ship `PartitionMailboxStatus{partition_id, status: :full}` over the control channel to the SDK. When `pending_bytes` falls below `wal_low_watermark_bytes` (default `wal_high_watermark_bytes / 2`), the agent SHALL ship `PartitionMailboxStatus{partition_id, status: :open}`.

The SDK's per-Partition mailbox observer SHALL treat `:full` as equivalent to the existing per-Partition mailbox-full condition that drives `OverflowPolicy`; the producing Shard's `ctx.emit(...)` SHALL apply the channel's declared policy (`Block` parks the producer, `DropNewest` drops, `DropOldest` discards-oldest).

The agent SHALL NOT issue `:full` more than once per Partition while the watermark is exceeded (idempotent transitions); SHALL NOT issue `:open` until `:full` was previously sent for that Partition (no spurious `:open` frames).

#### Scenario: High-watermark triggers :full status

- **WHEN** a Partition's `WalWriter` accumulates `pending_bytes = 16 MiB + 1 record`
- **THEN** the agent SHALL ship `PartitionMailboxStatus{partition_id, status: :full}` exactly once

#### Scenario: Low-watermark triggers :open status

- **WHEN** the same Partition's `pending_bytes` drains below 8 MiB after fsyncs
- **THEN** the agent SHALL ship `PartitionMailboxStatus{partition_id, status: :open}` exactly once

#### Scenario: OverflowPolicy::Block parks producer under back-pressure

- **WHEN** a Shard with `OverflowPolicy::Block` on its emit channel calls `ctx.emit(...)` for a Partition under `:full` status
- **THEN** the producer SHALL park its calling thread; on receipt of `:open` status the producer SHALL unpark and the emit SHALL succeed; the producer's `Shard::run` SHALL eventually return `Ok(())`

### Requirement: Crash-safe persistence on agent reboot

On agent boot, `Flezha.State.Supervisor` SHALL call `Registry.scan_dir/1` for each `app_data/state/{ce_id}/{relay}/{shard}/` directory and reconstruct the in-memory persistence state:

- For each `partition_key.wal` file: start a `WalWriter` GenServer; `WalWriter::scan_for_last_valid_seq/1` runs at `init/1` to recover `last_persisted_seq` and truncate any partial-write tail.
- For each `partition_key.snap.{seq}` file (latest): record the `snapshot_seq` counter.
- For each `partition_key.snap.{seq}.tmp` file: unlink (orphaned mid-write).
- For each `partition_key.wal.rewriting` file: unlink (orphaned retention rewrite).
- For each `partition_key.outbox.dedup` file: read the persisted `last_persisted_seq` for the dedup cache (recovered from the WAL scan as a cross-check).

`Flezha.State.Outbox` SHALL persist `last_persisted_seq` per Partition into `partition_key.outbox.dedup` (8-byte u64 BE) on every WAL append via a write-then-rename atomic update; this allows fast dedup-cache priming on boot without re-scanning the entire WAL.

#### Scenario: Boot recovers from clean shutdown

- **WHEN** the agent boots after a clean shutdown
- **THEN** for each Partition's WAL file, `WalWriter::scan_for_last_valid_seq` SHALL run successfully and recover `last_persisted_seq` matching the on-disk state

#### Scenario: Boot recovers from crash during WAL append

- **WHEN** the agent boots after a crash mid-WAL-append (partial record at file tail)
- **THEN** `scan_for_last_valid_seq` SHALL detect the crc-mismatched partial record AND truncate it; `last_persisted_seq` SHALL equal the seq of the last valid (crc-good) record

#### Scenario: Boot recovers from crash during snapshot rename

- **WHEN** the agent boots after a crash between snapshot fsync and rename, leaving `partition_key.snap.{seq}.tmp`
- **THEN** boot SHALL unlink the `.tmp` file; the prior snapshot (if any) and the un-truncated WAL are intact; persistence resumes correctly

#### Scenario: Boot recovers from crash during retention rewrite

- **WHEN** the agent boots after a crash during the retention sweeper's WAL rewrite, leaving `partition_key.wal.rewriting`
- **THEN** boot SHALL unlink the `.rewriting` file; the original WAL is intact; the next sweeper tick re-runs the rewrite idempotently

### Requirement: flz state CLI subcommand surface

The agent SHALL expose a `flz state` CLI subcommand tree:

- `flz state at <relay/shard/partition> <seq | timestamp>` — point-in-time state query per the time-travel requirement above. Returns JSON-encoded state.
- `flz state list <relay>` — for each Partition of the Relay, prints `wal_seq`, `snapshot_seq`, `wal_bytes`, `flush_latency_ms_p99`, `fsync_durability_lag_ms`. Reads `Metrics.to_map/1`.
- `flz state rotate <relay/shard/partition>` — forces compaction by enqueuing `Overwatch.handle_continue(:compact_partition, partition_id)`. Returns immediately ("compaction queued"); the snapshot completes asynchronously.
- `flz state retention <relay> <hours>` — sets `time_travel_retention_hours.<RelayClass>` in runtime config (`Application.put_env(:flezha, ..., persistent: true)`); the next retention-sweeper tick reads the new value.

#### Scenario: flz state list enumerates Partitions

- **WHEN** the operator runs `flz state list MarketRelay`
- **THEN** the CLI SHALL print one line per Partition with the listed fields; output SHALL be tabular (column-aligned)

#### Scenario: flz state rotate triggers async compaction

- **WHEN** the operator runs `flz state rotate MarketRelay/btc/OhlcShard/M1`
- **THEN** the CLI SHALL print "compaction queued" and exit 0; the agent SHALL ship `CompactPartition` to the SDK; the resulting snapshot SHALL appear on disk within seconds

#### Scenario: flz state retention persists across agent restart

- **WHEN** the operator runs `flz state retention MarketRelay 12` and the agent is subsequently restarted
- **THEN** after restart, the resolved `time_travel_retention_hours` for `MarketRelay` SHALL be 12
