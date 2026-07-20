# operative-state-persistence Specification

## Purpose

The operative-state-persistence capability provides agent-side, per-Segment durability for Operative state. The agent maintains an append-only write-ahead log (WAL) per Segment under `app_data/state/`, fsynced on a configurable cadence, with asynchronous size-triggered compaction into snapshot files. On Segment reconnect, the agent assembles a `RestoreInitState{segment_id, snapshot_bytes, change_log_bytes}` payload and ships it to the SDK over the control channel; the SDK applies the snapshot and folds the WAL tail to recover terminal state. Persistence is per-Operative-class opt-in via runtime config or an Operative-class const, defaulting on. The agent retains pre-compaction WAL records for a configurable window to support time-travel queries via `flz state at`. The capability is additive to the existing `Overwatch` GenServer: persistence frames (`segment_state_change`, `segment_state_snapshot`) flow alongside the existing protocol; no SDK or runner code outside this capability is required.
## Requirements
### Requirement: Per-Segment append-only WAL on the agent

Each Segment whose Operative has `persistence_enabled = true` and whose Operation's `Change` is not `NoChange` SHALL have a per-Segment write-ahead log on the Controller at `app_data/state/{operative_class}/{operation_id}/{segment_key}.wal`. The on-disk path SHALL NOT include `runner_id` as a path component. Records SHALL be `[seq: u64 BE][len: u32 BE][header: 16B][change_bytes][crc32: u32 BE]` where `len = 16 + byte_size(change_bytes)` and `crc32 = :erlang.crc32(<<header::binary, change_bytes::binary>>)`. The WAL SHALL be written by the Controller's persistence module (`lib/flezha/state/wal_writer.ex`) and SHALL never be written directly by user code or by the SDK.

`seq` SHALL be strictly monotonic per Segment and assigned by the SDK. The Controller SHALL accept any incoming Change frame whose `seq == last_persisted_seq + 1` for that Segment; SHALL silently drop (with a `dedup_drops_total` counter increment) any frame whose `seq <= last_persisted_seq`. For a frame whose `seq > last_persisted_seq + 1` the Controller SHALL distinguish two cases:

- **Populated WAL** (`last_persisted_seq >= 0`) and not a runner-restart realign: a genuine protocol violation. The Controller SHALL reject the append (returning `{:error, {:gap, expected, got}}`) and the SDK SHALL treat it as a protocol violation and disconnect.
- **Empty WAL** (`last_persisted_seq == -1`) with `seq > 0` and not a runner-restart realign: a cold-takeover where this Controller holds no baseline for a Segment the runner already has state for. The Controller SHALL NOT disconnect. It SHALL enter an **awaiting-baseline** state for the Segment, shed the frame (incrementing a `wal_shed_total`-class counter, not erroring), drive exactly one `CompactSegment` pull (see "Restart restores state via RestoreInitState handshake"), and keep shedding further gap frames without erroring until a `segment_state_snapshot` establishes the baseline.

A one-time on-disk migration `Flezha.Boot.StatePathMigrator.migrate/0` SHALL run at controller boot before `Flezha.State.Supervisor` starts any per-Segment `WalWriter`. The migrator SHALL walk every directory under `app_data/state/` of the form `{runner_id}/{operative_class}/{operation_id}/` and atomically move its contents into `{operative_class}/{operation_id}/`. The migration is idempotent (skips silently if the flat layout already exists) and logs each rename. After all old `{runner_id}/` directories are emptied, they are removed.

#### Scenario: WAL path uses flat layout

- **WHEN** a Segment `p1` with operation `s1` and segment_key `k1` belonging to Operative class `MarketOperative` is running and persistence is enabled
- **THEN** the Controller SHALL write its WAL to `app_data/state/MarketOperative/s1/k1.wal`
- **AND** the path SHALL NOT contain any `runner_id` component regardless of which runner currently hosts the Operative

#### Scenario: WAL allocated lazily on first append

- **WHEN** the agent receives the first `segment_state_change` frame for a Segment that does not yet have a WAL file
- **THEN** the persistence module SHALL create the directory `app_data/state/{operative}/{operation}/`, open the WAL file at `{segment_key}.wal` with `[:raw, :read, :write, :binary, :append]`, and start a `WalWriter` GenServer for the Segment

#### Scenario: Strictly monotonic seq accepted in order

- **WHEN** the SDK ships frames with `seq = 1, 2, 3` for a fresh Segment
- **THEN** the agent SHALL append all three records to the WAL in order; the file's `last_persisted_seq` SHALL equal 3 after the third append

#### Scenario: Re-shipped seq is silently deduped

- **WHEN** the SDK ships a frame with `seq = 5` for a Segment whose `last_persisted_seq = 7` (re-ship after SDK crash that lost the in-memory cursor)
- **THEN** the agent SHALL NOT append the duplicate; the agent SHALL increment `dedup_drops_total`; the wire reply SHALL be `:ok` (so the SDK does not retry)

#### Scenario: Out-of-order seq on a populated WAL is rejected

- **GIVEN** a Segment whose `last_persisted_seq = 7` (populated WAL)
- **WHEN** the SDK ships a frame with `seq = 10`
- **THEN** the agent SHALL refuse the append and reply `{:error, {:gap, expected: 8, got: 10}}`; the SDK SHALL treat this as a protocol violation and disconnect

#### Scenario: Gap on an empty WAL seeds a baseline instead of disconnecting

- **GIVEN** a freshly-assigned Controller with no WAL for a Segment (`last_persisted_seq = -1`) whose live runner has been producing changes and next emits `seq = 5001`, and the frame is not a runner-restart realign
- **WHEN** the agent receives the `segment_state_change` frame
- **THEN** the agent SHALL NOT reply with a fatal gap error and SHALL NOT disconnect the SDK; the agent SHALL shed the frame (incrementing the shed counter), mark the Segment awaiting-baseline, and dispatch exactly one `CompactSegment` to pull a full-state baseline; further gap frames arriving before the snapshot SHALL be shed without error
- **AND** once the pulled `segment_state_snapshot{seq}` lands, `last_persisted_seq` SHALL equal that snapshot's seq and the runner's next new delta SHALL append contiguously

#### Scenario: Crash-safe append via crc

- **WHEN** the agent process crashes mid-record-write, leaving a partial record at the file tail
- **THEN** on next agent boot, `WalWriter::scan_for_last_valid_seq/1` SHALL detect the partial record (crc32 mismatch on the trailing bytes), truncate the file to the last valid record, and resume normal operation; `last_persisted_seq` SHALL equal the seq of the last valid record

#### Scenario: One-time migration from runner_id-keyed layout

- **GIVEN** a controller running pre-change code wrote files under `app_data/state/exec-9/MarketOperative/btc/M1.wal` and `app_data/state/exec-12/PriceOperative/eth/M5.wal`
- **WHEN** the controller is upgraded and `Flezha.Boot.StatePathMigrator.migrate/0` runs at boot
- **THEN** the files SHALL be moved to `app_data/state/MarketOperative/btc/M1.wal` and `app_data/state/PriceOperative/eth/M5.wal`
- **AND** the now-empty `exec-9/` and `exec-12/` directories SHALL be removed
- **AND** if the migrator runs again on a subsequent boot, it SHALL skip silently because the flat layout already exists

#### Scenario: Migration handles runner_id rotation across restarts

- **GIVEN** a runner restart between WAL writes — first write under `exec-9`, restart yields `exec-12` for the same Operative
- **WHEN** the migrator runs (one-time, on first boot under new code)
- **THEN** files from BOTH `exec-9/MarketOperative/btc/` and `exec-12/MarketOperative/btc/` SHALL be merged into the flat path; on collision (same segment_key.wal in both), the migrator SHALL prefer the file with the higher last-record `seq` (read by scanning) and preserve the other as `<key>.wal.preserved-<exec-9>` for forensics

### Requirement: Snapshot lifecycle with atomic rename and WAL truncation

When the agent receives a `segment_state_snapshot` frame from the SDK (typically in response to a `CompactSegment` command), the agent SHALL: (1) write the snapshot bytes to `{segment_key}.snap.{seq}.tmp` with `[:raw, :write, :binary]`; (2) `:file.sync/1` the temp file; (3) atomically rename to `{segment_key}.snap.{seq}` via `:file.rename/2`; (4) call `WalWriter.truncate_up_to(seq)` which rewrites the WAL dropping records with `seq' <= seq` and atomically renames the rewritten file into place.

If the agent crashes between steps 3 and 4, the next boot SHALL find both the new snapshot and the un-truncated WAL; `lib/flezha/state/registry.ex::scan_dir/1` SHALL detect the situation, prefer the latest snapshot, and mark the WAL records ≤ `snapshot.seq` as redundant. Replay-from-this-state is correct: applying the snapshot then folding the WAL produces a state where the redundant records' apply_change calls are no-ops (because the snapshot already encodes their effects).

#### Scenario: Snapshot rename atomic

- **WHEN** the agent receives a `segment_state_snapshot` frame
- **THEN** an observer reading the directory SHALL see either the prior snapshot file OR the new snapshot file at any moment, but not a half-written file

#### Scenario: WAL truncation removes pre-snapshot records

- **WHEN** a snapshot at `seq = 100` lands and the WAL contains records seqs 1-150
- **THEN** after `WalWriter.truncate_up_to(100)`, the WAL SHALL contain only records seqs 101-150; the snapshot file SHALL contain the post-fold state at seq 100

#### Scenario: Crash mid-truncate leaves both files; replay is correct

- **WHEN** the agent crashes between snapshot-rename and WAL-truncate, leaving snapshot at seq 100 and full WAL with seqs 1-150
- **THEN** on next agent reconnect, the agent SHALL ship `RestoreInitState` with the snapshot bytes AND the full WAL bytes (records 1-150); the SDK SHALL apply the snapshot (state at seq 100) then fold records 1-150 — records 1-100 are no-ops because the snapshot already encodes their effects; records 101-150 advance the state correctly; terminal state matches the pre-crash state at seq 150

### Requirement: Asynchronous compaction triggered by per-Segment WAL size

Compaction SHALL be triggered by **WAL size or age**. When a Segment's WAL exceeds `compact_after_bytes` (per-Operation, **default 8 MiB**; the SDK ships the per-Operation value to the agent in the `AddOperative` metadata, and the SDK's own advertised default SHALL be 8 MiB so it does not override the global `:flezha, :default_compact_after_bytes`), OR the Segment's oldest un-snapshotted WAL record is older than `compact_after_age_ms` (global runtime knob, default `300_000`; `0` disables the age trigger), the agent SHALL trigger asynchronous compaction by enqueuing `Overwatch.handle_continue(:compact_segment, segment_id)`. The continuation SHALL ship `CompactSegment{segment_id}` to the SDK over the control channel. The age trigger SHALL be evaluated by the existing `Flezha.State.Watchdog` periodic sweep, reusing the same continuation as the size trigger.

The SDK SHALL acquire the Segment's sticky CAS, clone `Self::State`, release the CAS, serialize the clone (postcard), and ship `segment_state_snapshot{segment_id, seq, header, snapshot_bytes, crc32}` back to the agent. The agent SHALL handle the snapshot per the snapshot-lifecycle requirement above. The agent SHALL reply `CompactSegmentResponse{segment_id, snapshot_seq, snapshot_bytes_size}` to confirm.

The hot path SHALL pay only the brief sticky-CAS hold during the state clone — microseconds, not the seconds-to-minutes a synchronous full-state snapshot would take.

#### Scenario: Compaction triggered by size threshold

- **WHEN** a Segment's `wal_size_bytes` grows past 8 MiB (the default `compact_after_bytes`)
- **THEN** the agent SHALL enqueue `:compact_segment` for that Segment without blocking new Change ingestion

#### Scenario: Compaction triggered by WAL age for a low-throughput Segment

- **GIVEN** `compact_after_age_ms = 300_000` and a Segment whose WAL has not reached 8 MiB but whose oldest un-snapshotted record is older than 5 minutes
- **WHEN** the `Flezha.State.Watchdog` sweep runs
- **THEN** the agent SHALL enqueue `:compact_segment` for that Segment so a fresh baseline is written even though the size threshold was never reached

#### Scenario: Age trigger disabled

- **GIVEN** `compact_after_age_ms = 0`
- **WHEN** the `Flezha.State.Watchdog` sweep runs over a Segment with an old but small WAL
- **THEN** the agent SHALL NOT enqueue compaction on the basis of age; only the size trigger applies

#### Scenario: Per-Operation override propagated and honored

- **WHEN** an Operation declares `compact_after_bytes() -> usize { 16 * 1024 * 1024 }` in the SDK
- **THEN** the SDK SHALL ship `compact_after_bytes = 16777216` for this Operation in `AddOperative` metadata; the agent SHALL trigger compaction for this Operation's Segments when WAL grows past 16 MiB

#### Scenario: Hot path holds CAS only for state clone

- **WHEN** the SDK receives `CompactSegment` and clones the state
- **THEN** the SDK SHALL acquire the sticky CAS, clone, release the CAS within the same `Operation::run`-equivalent invocation, and serialize the clone in a non-runner context; the segment's input throughput SHALL NOT drop below 90% of pre-compaction throughput during the clone

### Requirement: Restart restores state via RestoreInitState handshake

On runner (re)bind, the controller SHALL recover each persisted Segment's state, choosing the direction by which side holds authoritative history:

- **Controller holds a baseline (a snapshot and/or WAL for the Segment) → push.** The controller SHALL drive a `RestoreInitState` handshake and the SDK SHALL rebuild the live Segment slot from the controller's durable WAL — snapshot apply followed by change-log fold — rather than initialising empty.
- **Controller holds no baseline for the Segment (no snapshot and no WAL) while the runner is live → pull.** The controller SHALL request a `CompactSegment` from the runner to obtain a full-state baseline, rather than pushing a `RestoreInitState`. Because a freshly-assigned controller cannot enumerate a live runner's Segments (its own disk is empty), the pull is data-plane-triggered by the first `segment_state_change` frame that arrives with an empty WAL (see "Per-Segment append-only WAL on the agent", empty-WAL gap).

The restore/resync dispatch path SHALL NOT ship a `RestoreInitState` whose assembled payload is empty (no snapshot and no WAL). It SHALL guard with `Flezha.State.Restore.has_persisted_state?/1` — as the bind-time path already does — so an empty payload can never rebuild a live Segment to `init_state` and destroy accumulated state. An empty-WAL condition SHALL route to the pull path above instead.

Restore SHALL be:

1. **Unconditional on every Overwatch bind** — driven by `Flezha.State.Recovery.resync_for_overwatch/2` (not a class→runner lookup that fails on cold boot). When the operative arrived via HotSwap handover, the in-band state is authoritative and the `{:kick_restore}` path SHALL skip to avoid double-applying.
2. **Keyed by operative instance id, not factory id** — the Segment wire identity is `[rc_len:u8][rc][sh_len:u8][sh][pk_len:u16 BE][pk]` keyed by the operative *instance*, so two operatives of the same factory class never collide on one WAL.
3. **Inclusive of seq 0** — an empty WAL initialises `last_persisted_seq = -1` (not `0`) and the "no snapshot" restore sentinel is `-1`, so the 0-based first change is never dedup-dropped. After fold, the SDK re-arms `change_seq = terminal_seq + 1` so the next produced frame is contiguous.

#### Scenario: a stateful operative resumes its accumulated state after both runners restart
- **GIVEN** a `resume_probe` operative has accumulated a running total of 100 in persisted state
- **WHEN** both runners are gracefully stopped and restarted under load and the operative rebinds
- **THEN** the operative's restored state is 100 (the WAL snapshot + change-log), driving one more `+7` input emits 107, and no state is lost.

#### Scenario: the first change of a fresh operative survives restart
- **GIVEN** an operative produced exactly one state change (seq 0) and no snapshot exists
- **WHEN** the runner restarts and restore runs
- **THEN** the seq-0 change is included in the restored change-log (the `-1` empty-WAL/no-snapshot sentinels keep it), not dropped as a dedup floor.

#### Scenario: fresh controller pulls a baseline from a live runner
- **GIVEN** an Operation is reassigned to a controller that has no WAL and no snapshot for its Segment, while the runner stays live holding accumulated state at seq 5000
- **WHEN** the operative binds and the runner's next `segment_state_change` arrives
- **THEN** the controller SHALL request a `CompactSegment` and, on receiving the `segment_state_snapshot`, write it as the Segment's baseline; the controller SHALL NOT push a `RestoreInitState` (which would carry an empty payload and reset the live Segment to `init_state`)

#### Scenario: empty-payload restore is never dispatched
- **GIVEN** a resync is triggered for a Segment whose controller has no snapshot and no WAL
- **WHEN** the recovery path evaluates whether to ship `RestoreInitState`
- **THEN** `has_persisted_state?/1` SHALL be false and no `RestoreInitState` SHALL be sent; the accumulated live state on the runner SHALL be preserved (not rebuilt to `init_state`)

### Requirement: Time-travel queries via WAL retention beyond latest snapshot

The agent SHALL retain pre-compaction WAL records for `time_travel_retention_hours` beyond the latest snapshot, configurable per-Operative-class via runtime config `time_travel_retention_hours.<OperativeClass>` (default `6` hours globally; setting to `0` disables time travel for that Operative class). The retention sweeper (`Flezha.State.Retention`) SHALL run on `time_travel_retention_check_interval_ms` (default 60_000), iterate Segments, and rewrite each Segment's WAL dropping records older than the cutoff.

The agent SHALL expose a query interface `Flezha.State.TimeTravel.query(segment_id, target)` where `target = {:seq, u64} | {:timestamp, datetime}`. The query SHALL: (1) locate the latest snapshot at-or-before the target; (2) read the tail-WAL records up to the target; (3) ship `RestoreInitState` to a transient SDK-side replay operative; (4) receive a `segment_state_snapshot` carrying JSON-encoded terminal state; (5) return the JSON to the caller.

The query SHALL be exposed via the `flz state at <operative/operation/segment> <seq | timestamp>` CLI subcommand.

#### Scenario: Default 6-hour retention

- **WHEN** a runner does not configure `time_travel_retention_hours`
- **THEN** the agent SHALL retain WAL records for 6 hours beyond the latest snapshot

#### Scenario: Time-travel disabled (`retention_hours = 0`)

- **WHEN** `time_travel_retention_hours.<OperativeClass> = 0`
- **THEN** the retention sweeper SHALL truncate the WAL up to the latest snapshot's seq immediately on each tick; only the latest snapshot is queryable; queries for any seq < latest snapshot return `TimeTravelError{kind: :outside_retention}`

#### Scenario: Query within retention window returns state

- **WHEN** the runner runs `flz state at MarketOperative/btc/OhlcOperation/M1 "2026-05-07T10:30:00Z"` AND that timestamp falls within the retention window
- **THEN** the agent SHALL load the latest snapshot at-or-before the timestamp and fold WAL records up to it; the CLI SHALL print the JSON-encoded state

#### Scenario: Query outside retention returns error

- **WHEN** the runner queries a timestamp older than the earliest available WAL record's record-time
- **THEN** the agent SHALL return `TimeTravelError{kind: :outside_retention, earliest_available: <ts>}`; the CLI SHALL print a clear error pointing at the earliest queryable point

#### Scenario: Query against persistence-off Operative errors

- **WHEN** the runner runs `flz state at <operative/operation/segment> <target>` for a Segment whose Operative has `persistence_enabled = false`
- **THEN** the agent SHALL return `TimeTravelError{kind: :segment_not_persistent, operative, operation, segment}`

#### Scenario: Live retention update applies on next sweep

- **WHEN** the runner runs `flz state retention MarketOperative 12` and the prior value was `6`
- **THEN** the runtime config SHALL be updated to 12 hours; the next retention-sweeper tick SHALL apply the new value (records older than 12 hours are deleted; records younger SHALL be retained going forward)

### Requirement: Fsync batching at configurable cadence

The agent SHALL run a single shared `Flezha.State.FsyncBatcher` GenServer per BEAM node. The batcher SHALL maintain a `MapSet` of dirty Segment IDs (Segments whose `WalWriter` has appended bytes since the last fsync). On each tick at interval `agent_fsync_interval_ms` (default 5; configurable via runtime config in range [1, 1000]), the batcher SHALL: (1) iterate the dirty set; (2) cast `:fsync` to each `WalWriter`; (3) reset the set; (4) schedule the next tick via `Process.send_after/3`.

`WalWriter` SHALL invoke `:file.datasync/1` on its file handle when receiving the `:fsync` message and SHALL update `fsync_durability_lag_ms` to `now - oldest_pending_record_timestamp`.

#### Scenario: Default 5 ms tick = 200 Hz fsync rate

- **WHEN** a runner does not configure `agent_fsync_interval_ms`
- **THEN** the batcher SHALL tick every 5 ms (200 Hz) AND each dirty `WalWriter` SHALL fsync at most once per tick

#### Scenario: 5 ms + 5 ms = 10 ms total durability bound

- **WHEN** an Operation records a Change at time `t` under default flusher and fsync settings (`linger_ms = 5` SDK-side; `agent_fsync_interval_ms = 5` agent-side)
- **THEN** the Change SHALL be durably fsynced to disk on the agent by `t + 10 ms` modulo network round-trip and OS scheduling; `fsync_durability_lag_ms` SHALL stabilize below 10 ms under steady-state load

#### Scenario: Runner-tightened bound

- **WHEN** a runner sets `agent_fsync_interval_ms = 1`
- **THEN** the batcher SHALL tick every 1 ms (1000 Hz); the durability bound SHALL be ~2 ms total at the cost of more frequent fsyncs

### Requirement: Per-Operative persistence toggle resolution at operative startup

The agent SHALL resolve a per-Operative-class boolean `persistence_enabled` at the moment an Operative instance is added (`AddOperative` command processed in `Overwatch.init/1`) using the following ordered rule:

1. If the agent's runtime config defines `operative_persistence.<OperativeClass>` for this Operative class, use that value.
2. Otherwise, if the `AddOperative` metadata carries an Operative-class const value (`UnboundedOperative::PERSISTENCE_ENABLED`), use that value.
3. Otherwise, use the built-in default of `true`.

The resolved value SHALL be bound to the Operative instance for its entire lifetime. Subsequent changes to the runner-side config or to the SDK's Operative-class const (via SDK redeploy) SHALL NOT affect already-running instances.

When `persistence_enabled = false`, the agent SHALL NOT allocate WAL or snapshot files for any of that Operative's Segments, SHALL ignore (drop with a counter increment) any incoming `segment_state_change` or `segment_state_snapshot` frames for those Segments, and SHALL NOT issue `RestoreInitState` on reconnect.

#### Scenario: Default-on with no override

- **WHEN** a runner does not configure `operative_persistence.MarketOperative` AND `MarketOperative`'s `AddOperative` metadata does not declare `PERSISTENCE_ENABLED`
- **THEN** the agent SHALL resolve `persistence_enabled = true` for `MarketOperative`

#### Scenario: Runner override takes precedence

- **WHEN** `MarketOperative`'s `AddOperative` metadata declares `PERSISTENCE_ENABLED = true` AND the runner sets `operative_persistence.MarketOperative = false`
- **THEN** the agent SHALL resolve `persistence_enabled = false` for `MarketOperative`

#### Scenario: Mid-life config change does not affect running instance

- **WHEN** an Operative instance is running with resolved `persistence_enabled = true` AND the runner subsequently runs `flz config set operative_persistence.MarketOperative false`
- **THEN** the running instance SHALL continue with `persistence_enabled = true`; the new value SHALL take effect only after the SDK pod hosting the instance is restarted (the new instance's `AddOperative` re-runs the resolution)

### Requirement: 16-byte version header on persisted blobs

Every WAL record and snapshot file SHALL carry a 16-byte header preceding the payload bytes:

- bytes 0:1 — `sdk_lang: u8` (1 = Rust, 2 = Java; reserved range 3–254)
- bytes 1:5 — `sdk_version: u32 BE` (semver-encoded)
- bytes 5:9 — `schema_version: u32 BE` (per-Operation, user-controlled)
- bytes 9:10 — `format_id: u8` (1 = postcard, 2 = bincode, 3 = rkyv-zerocopy, 4 = kryo-registered)
- bytes 10:12 — reserved, MUST be zero
- bytes 12:16 — `crc32: u32 BE` over the payload bytes (change_bytes for WAL records; snapshot_bytes for snapshot files)

The agent SHALL stamp the header at write time using the SDK-supplied values (the SDK ships the header pre-stamped in the wire frame; the agent writes it verbatim). The SDK SHALL verify the header at read time; the agent SHALL NOT verify the header except for the leading WAL record-framing crc32 over `header + change_bytes`.

#### Scenario: Header stamped verbatim from SDK

- **WHEN** the SDK ships a `segment_state_change` frame with header `[0x01, ...sdk_version..., ...schema_version..., 0x01, 0x00, 0x00, ...crc32...]`
- **THEN** the agent SHALL append the WAL record byte-for-byte; no agent-side header rewriting SHALL occur

#### Scenario: Cross-language load fails loud at SDK side

- **WHEN** a Java SDK reconnects to a Segment whose WAL was written by a Rust SDK (header `sdk_lang = 1`)
- **THEN** on `RestoreInitState` decode, the Java SDK SHALL fail with a cross-language-mismatch error and SHALL NOT proceed; the agent's role is unchanged (it ships bytes, the SDK enforces the language gate)

### Requirement: Idempotent emits with stable dedup keys

The Rust SDK runtime SHALL automatically attach `flezha.dedup_key = "{operative_class}/{operation_id}/{segment_key}/{change_seq}"` to outgoing plugin-bound emit metadata for any emit issued during a `state.apply(...)`-driven `Operation::run` invocation. The agent's plugin-provider path SHALL NOT alter or strip this metadata key; it SHALL flow through to the sink verbatim.

The Kafka producer plugin (`Flezha.Plugin.Provider.Kafka`) SHALL use the `flezha.dedup_key` value as the Kafka producer key (taking precedence over any other configured keying strategy). Other plugin providers MAY use the key for sink-specific dedup purposes; the contract is that the key is opaque metadata.

User-supplied metadata via `ctx.emit_with_meta(value, meta)` SHALL have its `flezha.dedup_key` entry silently dropped (matching the existing reserved-key behavior for `channel`); the SDK runtime stamps its own value.

#### Scenario: Dedup key attached automatically

- **WHEN** an Operation at `(MarketOperative/btc, OhlcOperation, M1)` records a Change at `seq = 12345` AND the same `Operation::run` invocation calls `ctx.emit::<BarClosed>(...)` to a Kafka-bound channel
- **THEN** the outgoing Kafka producer SHALL receive the message with key `"MarketOperative/OhlcOperation/M1/12345"`

#### Scenario: Replayed Change preserves dedup key

- **WHEN** a Segment is restored from snapshot + WAL and re-issues an emit for `seq = 12345`
- **THEN** the re-issued Kafka producer message SHALL carry the identical key `"MarketOperative/OhlcOperation/M1/12345"` as the original emit; downstream sink dedup (e.g. Kafka log compaction by key) SHALL collapse the duplicates

#### Scenario: Agent-side outbox dedupes by (segment, change_seq)

- **WHEN** the SDK ships `segment_state_change` with `seq = 12345` to the agent twice for the same Segment (once before crash, once after replay)
- **THEN** the agent SHALL persist exactly one record; the duplicate SHALL be detected via `seq <= last_persisted_seq` and discarded silently with `dedup_drops_total += 1`

#### Scenario: User-supplied dedup_key is dropped

- **WHEN** an Operation calls `ctx.emit_with_meta(value, %{"flezha.dedup_key" => "custom"})`
- **THEN** the user's `flezha.dedup_key` entry SHALL be silently dropped; the SDK runtime SHALL stamp `"{operative}/{operation}/{segment}/{seq}"` as the dedup key

### Requirement: Per-Segment persistence metrics

Each Segment's metric set SHALL expose six counters via `:counters` references, accessible from `Flezha.State.Metrics.to_map(segment_id)` and exported through the existing `Flezha.Plugin.Provider.Metrics` path:

- `persisted_changes_total` — monotonic; incremented per Change appended to the WAL (post-coalesce, post-dedup).
- `persisted_bytes_total` — monotonic; incremented by the byte length of each appended record.
- `wal_size_bytes` — gauge; current WAL file size (read from `:file.read_file_info/1` after each append and after each retention-sweep rewrite).
- `snapshot_seq` — gauge; seq of the latest snapshot file (or 0 if none).
- `flush_latency_ms_p99` — gauge; rolling 99th-percentile of `now - record_received_ts` measured per fsync, computed via a 1024-bucket reservoir sampler.
- `fsync_durability_lag_ms` — gauge; `now - oldest_pending_record_ts` at each `FsyncBatcher` tick.

The counters SHALL use `Ordering::Relaxed` (the BEAM `:counters` API does not expose ordering modes; this is satisfied by the underlying implementation). External metric systems (Prometheus, StatsD) integration is out of scope for this requirement; the existing `Flezha.Plugin.Provider.Metrics` path is the export surface.

#### Scenario: Counters initialized at Segment construction

- **WHEN** a Segment is constructed with `persistence_enabled = true`
- **THEN** all six counters SHALL be allocated; `persisted_changes_total = 0`, `persisted_bytes_total = 0`, `wal_size_bytes = 0`, `snapshot_seq = 0` (no snapshot yet), `flush_latency_ms_p99 = 0`, `fsync_durability_lag_ms = 0`

#### Scenario: Counters update after Change append

- **WHEN** the agent appends 10 Changes totaling 500 bytes to a Segment's WAL
- **THEN** `persisted_changes_total` SHALL increase by 10; `persisted_bytes_total` SHALL increase by `>= 500` (record framing overhead included; ≈ `500 + 10 * (8 + 4 + 16 + 4) = 820` bytes in practice); `wal_size_bytes` SHALL equal the file size after the appends

#### Scenario: Counters survive agent restart

- **WHEN** the agent is restarted and reads back the WAL files
- **THEN** `persisted_changes_total` and `persisted_bytes_total` SHALL be recomputed from the WAL records (scanning); `snapshot_seq` SHALL be the highest snapshot file's seq; `wal_size_bytes` SHALL match the file size; `flush_latency_ms_p99` and `fsync_durability_lag_ms` SHALL reset to 0

### Requirement: Back-pressure via OverflowPolicy when WAL outpaces fsync

For sessions that are NOT credit-speaking (pre-credit SDKs, or the controller running with credit flow control disabled), the agent SHALL monitor each `WalWriter`'s `pending_bytes` (queued-but-not-fsynced bytes). When `pending_bytes >= wal_high_watermark_bytes` (default 16 MiB), the agent SHALL ship `SegmentMailboxStatus{segment_id, status: :full}` over the control channel to the SDK. When `pending_bytes` falls below `wal_low_watermark_bytes` (default `wal_high_watermark_bytes / 2`), the agent SHALL ship `SegmentMailboxStatus{segment_id, status: :open}`.

The SDK's per-Segment mailbox observer SHALL treat `:full` as equivalent to the existing per-Segment mailbox-full condition that drives `OverflowPolicy`; the producing Operation's `ctx.emit(...)` SHALL apply the channel's declared policy (`Block` parks the producer, `DropNewest` drops, `DropOldest` discards-oldest).

The agent SHALL NOT issue `:full` more than once per Segment while the watermark is exceeded (idempotent transitions); SHALL NOT issue `:open` until `:full` was previously sent for that Segment (no spurious `:open` frames).

For credit-speaking sessions the agent SHALL NOT ship `SegmentMailboxStatus` frames: back-pressure for those sessions is carried entirely by the credit-grant protocol (see the `credit-grant-flow-control` capability), which parks the sender on window exhaustion instead of signalling a status flag.

#### Scenario: High-watermark triggers :full status

- **WHEN** a Segment's `WalWriter` accumulates `pending_bytes = 16 MiB + 1 record` on a non-credit-speaking session
- **THEN** the agent SHALL ship `SegmentMailboxStatus{segment_id, status: :full}` exactly once

#### Scenario: Low-watermark triggers :open status

- **WHEN** the same Segment's `pending_bytes` drains below 8 MiB after fsyncs
- **THEN** the agent SHALL ship `SegmentMailboxStatus{segment_id, status: :open}` exactly once

#### Scenario: OverflowPolicy::Block parks producer under back-pressure

- **WHEN** an Operation with `OverflowPolicy::Block` on its emit channel calls `ctx.emit(...)` for a Segment under `:full` status on a non-credit-speaking session
- **THEN** the producer SHALL park its calling thread; on receipt of `:open` status the producer SHALL unpark and the emit SHALL succeed; the producer's `Operation::run` SHALL eventually return `Ok(())`

#### Scenario: Credit-speaking session receives no status flags

- **WHEN** a credit-speaking SDK's Segment drives `pending_bytes` past `wal_high_watermark_bytes`
- **THEN** no `SegmentMailboxStatus` frame is shipped; the sender is throttled by credit exhaustion instead

### Requirement: Hard in-memory bound sheds WAL appends past the watermark

Each `WalWriter` SHALL enforce a hard in-memory bound on un-fsynced data —
covering both queued-but-not-fsynced bytes and pending mailbox depth. When the
hard bound is exceeded, `append_async/5` SHALL shed the incoming Change (drop
it without writing) and increment a shed counter, rather than growing the
writer's mailbox or un-fsynced bytes without bound. The data plane SHALL NOT
block: the shed leaves a sequence gap that the existing resync path repairs.

The hard bound SHALL be strictly above the soft watermark. For non-credit-speaking
sessions, shedding remains the last-resort safety net for a slow disk or an SDK
that does not honour the `SegmentMailboxStatus` wire signal. For credit-speaking
sessions, the credit ledger SHALL refuse an overrunning frame before the hard
bound is reached, so shedding is unreachable for a conformant sender; any shed
recorded against a credit-speaking session SHALL be surfaced as an anomaly
(`credit_overrun_total` / `wal_shed_total`), never treated as expected
back-pressure.

#### Scenario: Appends past the hard bound are shed, not buffered

- **WHEN** a `WalWriter` serving a non-credit-speaking session cannot drain
  (fsync stalled / writer suspended) and `append_async/5` is called repeatedly
  until the un-fsynced bytes or pending mailbox depth exceed the hard bound
- **THEN** further appends are dropped and a shed counter increments, and the
  writer's pending mailbox depth stays bounded instead of growing one-to-one
  with the calls

#### Scenario: Shed-induced gap is repaired by resync

- **WHEN** a Change has been shed, leaving the next received seq non-contiguous
- **THEN** the existing seq-gap resync handshake re-pulls the missing state from
  the SDK (the authoritative source), so no committed data is lost

#### Scenario: Conformant credit-speaking sender never sheds

- **WHEN** a credit-speaking SDK honours its window while the disk drains slowly
- **THEN** the sender parks on credit exhaustion before the hard bound is
  reached and `wal_shed_total` for its Segments does not advance

### Requirement: Crash-safe persistence on agent reboot

On agent boot, after `Flezha.Boot.StatePathMigrator.migrate/0` runs, `Flezha.State.Supervisor` SHALL call `Registry.scan_dir/1` for each `app_data/state/{operative}/{operation}/` directory and reconstruct the in-memory persistence state:

- For each `segment_key.wal` file: start a `WalWriter` GenServer; `WalWriter::scan_for_last_valid_seq/1` runs at `init/1` to recover `last_persisted_seq` and truncate any partial-write tail.
- For each `segment_key.snap.{seq}` file (latest): record the `snapshot_seq` counter.
- For each `segment_key.snap.{seq}.tmp` file: unlink (orphaned mid-write).
- For each `segment_key.wal.rewriting` file: unlink (orphaned retention rewrite).
- For each `segment_key.outbox.dedup` file: read the persisted `last_persisted_seq` for the dedup cache (recovered from the WAL scan as a cross-check).

`Flezha.State.Outbox` SHALL persist `last_persisted_seq` per Segment into `segment_key.outbox.dedup` (8-byte u64 BE) on every WAL append via a write-then-rename atomic update; this allows fast dedup-cache priming on boot without re-scanning the entire WAL.

#### Scenario: Boot recovers from clean shutdown

- **WHEN** the agent boots after a clean shutdown
- **THEN** for each Segment's WAL file, `WalWriter::scan_for_last_valid_seq` SHALL run successfully and recover `last_persisted_seq` matching the on-disk state

#### Scenario: Boot recovers from crash during WAL append

- **WHEN** the agent boots after a crash mid-WAL-append (partial record at file tail)
- **THEN** `scan_for_last_valid_seq` SHALL detect the crc-mismatched partial record AND truncate it; `last_persisted_seq` SHALL equal the seq of the last valid (crc-good) record

#### Scenario: Boot recovers from crash during snapshot rename

- **WHEN** the agent boots after a crash between snapshot fsync and rename, leaving `segment_key.snap.{seq}.tmp`
- **THEN** boot SHALL unlink the `.tmp` file; the prior snapshot (if any) and the un-truncated WAL are intact; persistence resumes correctly

#### Scenario: Boot recovers from crash during retention rewrite

- **WHEN** the agent boots after a crash during the retention sweeper's WAL rewrite, leaving `segment_key.wal.rewriting`
- **THEN** boot SHALL unlink the `.rewriting` file; the original WAL is intact; the next sweeper tick re-runs the rewrite idempotently

### Requirement: flz state CLI subcommand surface

The agent SHALL expose a `flz state` CLI subcommand tree, using operative vocabulary in every path argument:

- `flz state at <operative/operation/segment> <seq | timestamp>` — point-in-time state query per the time-travel requirement. Returns JSON-encoded state.
- `flz state list <operative>` — for each Segment of the Operative, prints `wal_seq`, `snapshot_seq`, `wal_bytes`, `flush_latency_ms_p99`, `fsync_durability_lag_ms`. Reads `Metrics.to_map/1`.
- `flz state rotate <operative/operation/segment>` — forces compaction by enqueuing the per-Segment compaction continuation on the Overwatch. Returns immediately ("compaction queued"); the snapshot completes asynchronously.
- `flz state retention <operative> <hours>` — sets `time_travel_retention_hours.<OperativeClass>` in runtime config (`persistent: true`); the next retention-sweeper tick reads the new value.

#### Scenario: flz state list enumerates Segments

- **WHEN** the runner runs `flz state list MarketOperative`
- **THEN** the CLI SHALL print one line per Segment with the listed fields; output SHALL be tabular (column-aligned)

#### Scenario: flz state rotate triggers async compaction

- **WHEN** the runner runs `flz state rotate MarketOperative/btc/OhlcOperation/M1`
- **THEN** the CLI SHALL print "compaction queued" and exit 0; the agent SHALL ship `CompactSegment` to the SDK; the resulting snapshot SHALL appear on disk within seconds

#### Scenario: flz state retention persists across agent restart

- **WHEN** the runner runs `flz state retention MarketOperative 12` and the agent is subsequently restarted
- **THEN** after restart, the resolved `time_travel_retention_hours` for `MarketOperative` SHALL be 12

### Requirement: Snapshot files use flat path

The Controller SHALL store snapshot files at `app_data/state/{operative_class}/{operation_id}/{segment_key}.snap.{seq}`. The on-disk path SHALL NOT include `runner_id`. The on-the-wire payload-id SHALL be `segment_state_snapshot`.

#### Scenario: Snapshot path is flat

- **WHEN** the Controller compacts the WAL of Segment `p1` (Operative class `MarketOperative`, operation `s1`, segment_key `k1`) and assigns snapshot seq=42
- **THEN** the resulting file SHALL be at `app_data/state/MarketOperative/s1/k1.snap.42`

### Requirement: ProcessorApplyState renamed to SegmentApplyState

The protobuf message carrying inbound state application SHALL be named `SegmentApplyState`. The field naming the owning operative SHALL be `operative_id`; field `segment_id` (string) IS the canonical identifier for the targeted Segment. The on-the-wire payload-id SHALL be `segment_apply_state`. The inverse `SegmentState` message and the `segment_state_change` / `segment_state_snapshot` / `delete_segment_state` payload-ids replace the former `partition_*` names in lockstep across all SDKs.

#### Scenario: Rust SDK emits SegmentApplyState
- **WHEN** the Rust SDK applies a state change to Segment `p1` belonging to Operative `r1`
- **THEN** the SDK SHALL send a `SegmentApplyState { operative_id: "r1", segment_id: "...", change_bytes: ... }` on the control channel; the Controller SHALL accept it and route it to the Segment's WAL writer

#### Scenario: Java SDK emits SegmentApplyState
- **WHEN** the Java SDK applies a state change to a Segment
- **THEN** the SDK SHALL send a `SegmentApplyState` message identical in shape to the Rust SDK's; the Controller SHALL handle both identically

### Requirement: SDK serializes Operative state on `SegmentState` request via the Decision-10 JSON shape

When the SDK receives a `SegmentState{operative_id}` command, it SHALL respond with a JSON object whose top-level keys are the Operative's Operation names and whose values are:

- For single-mode Operations (one Segment per Operation): the serialized `Self::State` of that Segment.
- For static-mode Operations (`operation_keys() = [...]`): an inner JSON object whose keys are the Operation's `operation_key` values and whose values are each Segment's serialized `Self::State`.

The wire payload SHALL be `segment_state` payload-id with the JSON object as the message bytes. Empty Operatives (no live Segments) SHALL produce `{}`.

State serialization SHALL go through a per-Operation fn pointer captured at `register_unbounded::<R>()` time. The fn pointer downcasts the type-erased state held in `SegmentSlot::state` to its statically-known `SegmentState<S>` and emits `serde_json::Value` via the `S::State: Serialize` bound on the trait. No additional user-facing trait machinery is required beyond the existing `Serialize + DeserializeOwned` bounds on `Operation::State`.

#### Scenario: Single-mode Operation returns its state at the operation-name key

- **WHEN** the SDK receives `SegmentState{operative_id}` for an Operative whose only Operation is `"counter"` (single-mode) with state `{"value": 42}`
- **THEN** the wire reply payload SHALL be `{"counter": {"value": 42}}`

#### Scenario: Static-mode Operation returns a key-keyed object

- **WHEN** the SDK receives `SegmentState{operative_id}` for an Operative whose only Operation is `"ohlc"` (static-mode, keys `["1m", "1h"]`) with `1m` state `{"high": 10}` and `1h` state `{"high": 20}`
- **THEN** the wire reply payload SHALL be `{"ohlc": {"1m": {"high": 10}, "1h": {"high": 20}}}`

### Requirement: SDK applies `AddOperative.init_values` JSON to per-Segment state

When the SDK receives an `AddOperative{operative_id, factory_id, init_values}` command and `init_values` is a non-empty JSON object, the SDK SHALL parse the JSON, walk its top-level keys, and for each key that matches a registered Operation name on the Operative's class:

- For single-mode Operations: deserialize the JSON value directly into `S::State` via the per-Operation `deserialize_state` fn pointer (captured at registration time and bounded on `S::State: DeserializeOwned`), then overwrite the freshly-`init_state`d Segment's state with the deserialized value.
- For static-mode Operations: walk the inner object's keys; for each `(key, value)` pair where `key` is a declared `operation_key`, deserialize the inner value into `S::State` and overwrite that Segment's state.

Top-level keys that do not match any Operation name SHALL be ignored (with an `eprintln!` log line at the SDK boundary). Inner-object keys that do not match a declared `operation_key` SHALL also be ignored.

If `init_values` is empty (`""` on the wire), the SDK SHALL leave each Segment's state untouched after the cold-start `Operation::init_state(operation_key, None)` returned by the build path.

`MigrationDeserialization{factory_id, operative_id, serialized_state}` SHALL reuse the same `apply_init_values` path: the `serialized_state` payload is the UTF-8 JSON blob produced by `serialize_operative_state` on the source runner; applying it to a freshly-started instance restores byte-identical state.

#### Scenario: Cold start with init_values seeds single-mode Operation

- **WHEN** the SDK receives `AddOperative{operative_id, factory_id, init_values: "{\"counter\":{\"value\":7}}"}` for an Operative with one single-mode Operation `"counter"`
- **THEN** the SDK SHALL deserialize `{"value":7}` directly into `CounterState` and overwrite the Segment's state with `CounterState { value: 7 }`

#### Scenario: Cold start with init_values seeds static-mode Operation per key

- **WHEN** the SDK receives `AddOperative{...init_values: "{\"ohlc\":{\"1m\":{\"high\":5},\"1h\":{\"high\":10}}}"}` for an Operative with `operation_keys = ["1m", "1h"]`
- **THEN** the SDK SHALL deserialize `{"high":5}` into the `"1m"` Segment's state AND deserialize `{"high":10}` into the `"1h"` Segment's state

#### Scenario: Empty init_values uses the default path

- **WHEN** the SDK receives `AddOperative{...init_values: ""}`
- **THEN** the SDK SHALL leave each Segment's state at the value `Operation::init_state(operation_key, None)` returned at build time

#### Scenario: Round-trip via MigrationSerialization / MigrationDeserialization

- **WHEN** the SDK on the source runner responds to `MigrationSerialization{operative_id}` with payload bytes `B = serialize_operative_state(instance)`, and a second SDK on the destination runner receives `MigrationDeserialization{factory_id, operative_id, serialized_state: B}`
- **THEN** the destination's freshly-started instance SHALL have Segment state byte-identical to the source's at the moment of serialization, as observed by `serialize_operative_state` re-issued against the destination

### Requirement: The persisted `runner_id` in an operative-binding record is advisory across a controller restart

`Flezha.Operative.Overwatch.init/1` accepts a `stale_runner_id` argument that originates from the durable operative-binding record replayed at boot. This value SHALL be treated as advisory only — it identifies the runner the operative was bound to BEFORE the current controller process started. The Overwatch SHALL re-resolve the live `runner_id` via `OperativeManager.assign_runner/2` inside `handle_continue({:bind_runner, …}, _)` and SHALL NOT issue any control-plane calls (`AddOperative`, `:ping_runner`, `start_operative_sdk`) against the persisted value directly.

When the re-resolved `runner_id` differs from `stale_runner_id`, the Overwatch SHALL log an `info`-level rebind line naming both ids and proceed. When the re-resolved `runner_id` equals `stale_runner_id`, the Overwatch SHALL still proceed via the normal startup path — equality with the persisted value does NOT bypass the end-to-end ping (see `runner-controller-connection-healing`).

No on-disk format change accompanies this requirement; the binding record's `runner_id` field SHALL continue to be written and read with the same wire layout. The clarification is purely about the runtime semantics of the field.

#### Scenario: Stale runner_id is re-resolved at boot
- **GIVEN** a durable operative-binding record for operative `r1` with `runner_id = "default_stale"`
- **AND** the controller restarts and replays the record
- **AND** the only live runner in the namespace is `default_fresh`
- **WHEN** `Flezha.Operative.Overwatch` is started by `dist_sup` with `stale_runner_id = "default_stale"`
- **THEN** `handle_continue({:bind_runner, "default_stale", operative}, _)` SHALL invoke `OperativeManager.assign_runner(namespace, operative.factory_id)` and resolve to `"default_fresh"`
- **AND** no control-plane call SHALL be issued against `"default_stale"`
- **AND** an `info`-level line SHALL log `operative r1 rebound from stale runner "default_stale" to live "default_fresh"`

#### Scenario: Re-resolved runner_id equal to stale value still runs the full ping path
- **GIVEN** a durable operative-binding record for operative `r1` with `runner_id = "e1"`
- **AND** the controller restarts and `e1` is still the live runner (e.g. the runner outlived the controller)
- **WHEN** the Overwatch resolves `runner_id == stale_runner_id == "e1"`
- **THEN** the Overwatch SHALL still issue `:ping_runner` before `:start_operative`
- **AND** the rebind-log line SHALL NOT fire (the ids are equal)
- **AND** the bounded-retry governor SHALL still apply on ping/start failure

### Requirement: A persistence seq gap triggers resync, not permanent rejection

When the Controller receives a `segment_state_change` frame whose `seq > last_persisted_seq + 1`, it SHALL treat the gap as a recoverable desync rather than a terminal protocol error. The Controller SHALL emit `[:flezha, :state, :seq_gap]` telemetry with measurements `%{expected: integer(), got: integer()}` and metadata identifying the Segment, and SHALL initiate the `RestoreInitState` resync handshake for that Segment (assemble snapshot + WAL tail per the existing "Restart restores state via RestoreInitState handshake" flow, ship it to the SDK, await `RestoreInitStateResponse{terminal_seq}`, and resume accepting Changes at `terminal_seq + 1`). After a successful resync the Segment's WAL SHALL advance again and its outbox SHALL drain to the output plugins.

The Controller SHALL NOT leave a Segment indefinitely rejecting Changes with `{:error, {:gap, expected, got}}`. The strict acceptance rule for the contiguous case (`seq == last_persisted_seq + 1`) and the silent-dedup rule for the stale case (`seq <= last_persisted_seq`) are unchanged.

#### Scenario: Gap resynchronises the segment instead of wedging it
- **GIVEN** a Segment with `last_persisted_seq = 100`
- **WHEN** the Controller receives a Change with `seq = 137` (a gap)
- **THEN** the Controller SHALL emit `[:flezha, :state, :seq_gap]` with `expected: 101, got: 137`
- **AND** the Controller SHALL ship `RestoreInitState` for the Segment and await `RestoreInitStateResponse`
- **AND** after the handshake the Segment SHALL accept subsequent Changes and its WAL SHALL advance past `last_persisted_seq`
- **AND** the Segment SHALL NOT remain in a permanent gap-rejection state

#### Scenario: Contiguous and stale frames are unaffected
- **WHEN** a Change arrives with `seq == last_persisted_seq + 1`
- **THEN** it SHALL be appended as today (no resync, no telemetry)
- **AND WHEN** a Change arrives with `seq <= last_persisted_seq`
- **THEN** it SHALL be silently deduped with a `dedup_drops_total` increment as today

### Requirement: Per-Segment output-staleness watchdog with self-heal

The Controller SHALL track, per persistence-enabled Segment, the wall-clock time at which `last_persisted_seq` last advanced. A periodic sweep SHALL run on a configurable interval and, for any Segment whose data stream is attached (the SDK is live and applying inputs) but whose `last_persisted_seq` has not advanced within `output_stall_timeout_ms` (configurable per Operative class; default 60_000), SHALL emit `[:flezha, :state, :output_stall]` telemetry (measurements `%{stalled_ms: integer(), last_seq: integer()}`, Segment metadata) and SHALL trigger the `RestoreInitState` resync recovery for that Segment.

This bounds the duration of any silent persistence/output freeze to at most one `output_stall_timeout_ms` window. Because WAL persistence and output-plugin delivery share the per-Segment outbox, recovering WAL advance SHALL also resume output (e.g. Kafka production) for that Segment.

#### Scenario: A live segment whose WAL stops advancing is detected and recovered
- **GIVEN** a persistence-enabled Segment whose data stream is attached and whose SDK continues to apply inputs (in-memory state advancing)
- **WHEN** `last_persisted_seq` does not advance for longer than `output_stall_timeout_ms`
- **THEN** the staleness sweep SHALL emit `[:flezha, :state, :output_stall]` with the stalled duration and last seq
- **AND** the Controller SHALL trigger resync recovery for the Segment
- **AND** after recovery the Segment's WAL SHALL advance and its outbox SHALL drain

#### Scenario: An idle segment is not falsely flagged
- **GIVEN** a persistence-enabled Segment whose SDK is applying no inputs (genuinely idle, no Changes to persist)
- **WHEN** the staleness sweep runs
- **THEN** the Segment SHALL NOT be flagged as stalled and SHALL NOT be resynced (no Change is expected, so a non-advancing `last_persisted_seq` is correct)

### Requirement: Compaction handshake is bounded and cannot wedge persistence

When the Controller ships `CompactSegment{segment_id}` to the SDK, it SHALL bound the wait for the corresponding `segment_state_snapshot` response with a timeout. If the snapshot does not arrive within the bound, the Controller SHALL emit `[:flezha, :state, :compaction_timeout]` telemetry, SHALL NOT leave the Segment's WAL append blocked, and SHALL recover via resync. Compaction SHALL be retriable on a later trigger; a single failed/slow compaction SHALL NOT permanently stop WAL growth or output for the Segment.

#### Scenario: A stalled compaction times out instead of freezing the WAL
- **GIVEN** a Segment for which the Controller has shipped `CompactSegment`
- **WHEN** no `segment_state_snapshot` response arrives within the compaction timeout
- **THEN** the Controller SHALL emit `[:flezha, :state, :compaction_timeout]`
- **AND** WAL append for the Segment SHALL NOT remain blocked (no "large WAL, no snapshot, frozen" state)
- **AND** persistence SHALL recover via resync and compaction SHALL be eligible to retry later

### Requirement: State path resolution SHALL be single-source and coherent

The on-disk location of a Segment's WAL/snapshot/outbox SHALL be computed from a single
authoritative resolver that every reader, writer, and the boot-time migrator agree on, so
that no path divergence can orphan persisted state. If `FLEZHA_STATE_DIR` is set, the
resolver and the migrator SHALL use the same value, and a change of root between boots
SHALL be reconciled — existing state migrated from the old root to the new one — rather
than silently orphaned. The boot migrator SHALL NOT relocate state into a layout (e.g. a
`_system/` namespace prefix) that the reader does not read; layout changes ahead of reader
support are prohibited because they orphan state.

#### Scenario: WAL is not orphaned by a path-resolution change
- **GIVEN** persisted WAL/snapshot files for running Segments
- **WHEN** the Controller restarts after this change
- **THEN** the restore path SHALL find the existing files at the resolver's path
- **AND** no Segment's state SHALL become invisible because the migrator and reader disagree on the path

#### Scenario: Migrator and reader agree on layout
- **WHEN** the boot migrator runs
- **THEN** the directory layout it produces SHALL be exactly the layout `Flezha.State.Registry` reads
- **AND** any layout the reader does not support SHALL NOT be produced by the migrator

### Requirement: Per-Segment WAL SHALL realign to a restarted runner's seq base

The Controller's per-Segment WAL SHALL adopt a restarted runner's sequence base as the
live source of truth rather than freezing. When a runner process restarts and reconnects
under a new runner id, it resumes emitting Changes from a fresh sequence base derived from
its post-restart rebuild (e.g. a Kafka replay), which may diverge from the persisted
`last_persisted_seq`. The WAL SHALL realign its baseline — discarding the superseded
pre-restart WAL/snapshots so a later `RestoreInitState` ships only the new epoch — instead
of dedup-dropping the new runner's Changes as stale. A Change at or below
`last_persisted_seq` from the *same* runner SHALL remain a legitimate dedup. A realignment
SHALL emit `[:flezha, :state, :wal_realign]` telemetry.

A realign SHALL be honoured only when the divergent Changes arrive via the Segment's
current owner binding — the session the takeover logic most recently bound to the
Segment's write path. Changes forwarded by a superseded session SHALL be refused at
the binding (the superseded session's channels are unbound during takeover, before the
new session is bound) and SHALL NOT trigger a realign; the refusal SHALL NOT add any
per-record check to the owning session's append path.

#### Scenario: A restarted runner's diverged seq base realigns the WAL
- **GIVEN** a Segment whose WAL has advanced to seq N under runner A
- **WHEN** runner A restarts and a new runner B, bound as the Segment's owner by takeover, resumes emitting Changes from a seq base diverged from N
- **THEN** the WAL SHALL realign to runner B's seq base and resume advancing rather than freeze at N
- **AND** a `[:flezha, :state, :wal_realign]` event SHALL be emitted

#### Scenario: A same-runner duplicate is still deduped
- **GIVEN** a Segment whose WAL has advanced to seq N under runner A
- **WHEN** runner A re-sends a Change at or below seq N (a retransmit, not a restart)
- **THEN** the Change SHALL be deduped and the WAL SHALL NOT realign

#### Scenario: Output-stall resync resolves the owning runner
- **WHEN** the Watchdog detects an output stall and triggers a resync for a Segment
- **THEN** `Flezha.State.Recovery.resolve_runner/1` SHALL resolve the owning runner id from the durable registry entry's metadata tag, keyed by the operative's operative_class (its `:operative_class` opt, defaulting to its id)

#### Scenario: A superseded session's divergent frames do not realign
- **GIVEN** a Segment whose stable identity was taken over by a new session while the old session's frames are still in flight
- **WHEN** the superseded session's channel forwards a Change with a divergent seq base
- **THEN** the frame SHALL be refused at the binding, the WAL SHALL NOT realign, and a `[:flezha, :state, :stale_writer_refused]` event SHALL be emitted once for the episode

### Requirement: Output is persisted before it is emitted (flush-before-emit)

For a persistence-enabled operative, the SDK SHALL drain a just-applied state change to the durable WAL send path BEFORE writing the corresponding output frame, so durable state never lags emitted output by more than the in-flight network window. This is gated on `persistence_enabled` and `FLEZHA_FLUSH_BEFORE_EMIT` (default on); non-persistent operatives keep the zero-cost inline emit. Each change is moved out of the buffer exactly once and seq order is preserved via the shared `change_seq`.

#### Scenario: durable state does not trail emitted output
- **GIVEN** a persistence-enabled operative applies a state change and then emits output for it
- **WHEN** the dispatch runs with flush-before-emit enabled
- **THEN** the change is shipped to the per-Segment WAL path before the output frame is written
- **AND** on a subsequent hard-kill the restored state is no further behind the emitted output than the single in-flight change.

#### Scenario: a non-persistent operative keeps the inline fast path
- **GIVEN** an operative with persistence disabled
- **WHEN** it emits output
- **THEN** no flush-before-emit drain occurs and the emit path is unchanged.

### Requirement: Per-Segment state is deleted on dynamic-mode eviction

When the SDK evicts a Segment due to dynamic-mode idle-TTL eviction (see `operative-operation-segment-model` "Per-Operation idle-TTL eviction is opt-in"), eviction SHALL be destructive. If persistence is enabled for the Operative class, the SDK SHALL ship a fire-and-forget `delete_segment_state{segment_id}` frame so the agent removes the per-Segment WAL, snapshot, and outbox files (`{key}.wal`, `{key}.snap.{seq}`, `{key}.outbox.dedup` under `app_data/state/{operative}/{operation}/`). A subsequent input for the same key SHALL cold-start a fresh Segment (new WAL from seq 0); the SDK SHALL NOT restore the evicted Segment's state from disk.

#### Scenario: Eviction deletes on-disk state
- **WHEN** a persistence-enabled dynamic-mode Segment is evicted
- **THEN** the SDK SHALL ship `delete_segment_state{segment_id}` and the agent SHALL remove the WAL, snapshot, and outbox files for that segment_id

#### Scenario: Recreate after eviction cold-starts
- **WHEN** a dynamic-mode Segment is evicted and a subsequent input arrives for the same key
- **THEN** the SDK SHALL recreate the Segment via `init_state(Some(key), None)` and a new WAL SHALL start from seq 0; no restore-from-disk SHALL occur

### Requirement: Per-Segment state delete RPC

The agent SHALL accept a `delete_segment_state` data-stream frame whose body is the structured `segment_id`. On receipt the agent SHALL: (1) stop any per-Segment `WalWriter` for that segment_id and drop its pending `FsyncBatcher` dirty mark; (2) remove the `{key}.wal`, `{key}.snap.{seq}` (all sequences), and `{key}.outbox.dedup` files. Delete SHALL be idempotent: a delete for a segment_id with no files SHALL succeed.

The delete is reachable via two paths:

- The SDK reaper, for evicted persistence-enabled Segments (fire-and-forget; the agent writes no response frame back on the data stream).
- The CLI command `flz state delete <operative/operation/key>`, which calls `Flezha.State.Supervisor.delete_segment/1` directly and returns a `{segment_id, status}` confirmation synchronously.

#### Scenario: Delete removes existing files
- **WHEN** the agent receives `delete_segment_state` for a segment_id that has WAL and snapshot files on disk
- **THEN** the agent SHALL stop the per-Segment processes and remove all three file types

#### Scenario: Delete is idempotent for nonexistent segment
- **WHEN** the agent receives `delete_segment_state` for a segment_id with no on-disk state
- **THEN** the agent SHALL succeed without error (the delete is a no-op)

#### Scenario: CLI delete returns confirmation
- **WHEN** an operator runs `flz state delete <operative/operation/key>` against a Segment with on-disk state
- **THEN** `Flezha.State.Supervisor.delete_segment/1` SHALL remove the files and return `{:ok, %{segment_id, status: :ok}}`, and the CLI SHALL exit 0

### Requirement: Atomic effect record type in the per-Segment WAL

The per-Segment WAL SHALL support an additional record type carrying an
invocation's state change(s), its outbox entries (channel, key, user metadata,
payload, emit ordinal), and its consumed input offset as one crc-protected record
appended and fsynced as a unit. Existing change-record and snapshot requirements
are unchanged; at-least-once operatives never produce effect records. Restore and
time-travel readers SHALL fold the state-change portion of effect records exactly
as they fold plain change records.

#### Scenario: Effect record replays as state

- **WHEN** a Segment with effect records in its WAL is restored via the restore
  handshake
- **THEN** the assembled payload folds the change portions in seq order and the
  restored state equals the pre-crash state

#### Scenario: Mixed WAL stays readable

- **WHEN** a WAL contains plain change records followed by effect records (tier
  enabled mid-lifetime)
- **THEN** restore, retention, and time-travel read the mixed sequence correctly

### Requirement: Drain-time coalescing is disabled for effect records

Records carrying outbox entries SHALL NOT be coalesced away or merged at drain
time: every effect record ships and persists individually so no durable output
can be dropped by state-change coalescing. Plain change records (no outputs)
retain existing coalescing behaviour.

#### Scenario: Output-bearing change survives coalescing window

- **WHEN** two changes with the same coalesce key occur within the linger window
  and the first one's invocation emitted an output
- **THEN** both the first record (with its outputs) and the second record are
  shipped and persisted

### Requirement: Dedup-file writes are amortized off the append hot path

The per-record WAL append SHALL NOT perform a separate dedup-file write: the
WAL append is the only per-record durable write. The dedup file
(`last_persisted_seq` and `outbox_flushed_seq`) is a rebuildable cache and
SHALL be advanced at most at fsync-batch cadence, and on snapshot, rotation,
and clean shutdown. On boot, a missing or stale dedup file SHALL be repaired
by recomputing from the WAL itself; a stale `outbox_flushed_seq` follows the
re-drive rule (never skip ahead).

#### Scenario: Appends produce one durable write each

- **WHEN** N changes are appended within one fsync batch
- **THEN** exactly N WAL appends and at most one dedup-file update occur — no
  per-record tmp+rename

#### Scenario: Stale dedup file is rebuilt from the WAL

- **WHEN** the controller crashes after appends that outran the last dedup-file
  update
- **THEN** boot recomputes `last_persisted_seq` from the WAL tail and dedup
  behaviour is identical to a per-record-persisted watermark

### Requirement: Outbox flush cursor lives beside the persistence watermark

The per-Segment dedup file SHALL carry both the existing `last_persisted_seq`
watermark and the new `outbox_flushed_seq` cursor, updated with the same
atomic tmp+rename discipline. On restart, a missing or stale cursor SHALL be
treated as "re-drive from the last known value" (never as "skip ahead").

#### Scenario: Stale cursor re-drives, never skips

- **WHEN** the dedup file's `outbox_flushed_seq` is older than the newest durable
  effect record after a crash
- **THEN** the controller re-drives all effect-record outputs above the cursor
  and never advances the cursor past undelivered entries

