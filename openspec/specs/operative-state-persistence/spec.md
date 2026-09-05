# operative-state-persistence Specification

## Purpose

The operative-state-persistence capability provides agent-side, per-Entity durability for Operative state. The agent maintains an append-only write-ahead log (WAL) per Entity under `app_data/state/`, fsynced on a configurable cadence, with asynchronous size-triggered compaction into snapshot files. On Entity reconnect, the agent assembles a `RestoreInitState{entity_id, snapshot_bytes, change_log_bytes}` payload and ships it to the SDK over the control channel; the SDK applies the snapshot and folds the WAL tail to recover terminal state. Persistence is per-Operative-class opt-in via runtime config or an Operative-class const, defaulting on. The agent retains pre-compaction WAL records for a configurable window to support time-travel queries via `flz state at`. The capability is additive to the existing `Overwatch` GenServer: persistence frames (`entity_state_change`, `entity_state_snapshot`) flow alongside the existing protocol; no SDK or runner code outside this capability is required.
## Requirements
### Requirement: Per-Entity append-only WAL on the agent

Each Entity whose Operative has `persistence_enabled = true` and whose Procedure's `Change` is not `NoChange` SHALL have a per-Entity write-ahead log on the Controller at `app_data/state/{operative_class}/{procedure_id}/{entity_key}.wal`. The on-disk path SHALL NOT include `runner_id` as a path component. Records SHALL be `[seq: u64 BE][len: u32 BE][header: 16B][change_bytes][crc32: u32 BE]` where `len = 16 + byte_size(change_bytes)` and `crc32 = :erlang.crc32(<<header::binary, change_bytes::binary>>)`. The WAL SHALL be written by the Controller's persistence module (`lib/flezha/state/wal_writer.ex`) and SHALL never be written directly by user code or by the SDK.

`seq` SHALL be strictly monotonic per Entity and assigned by the SDK. The Controller SHALL accept any incoming Change frame whose `seq == last_persisted_seq + 1` for that Entity; SHALL silently drop (with a `dedup_drops_total` counter increment) any frame whose `seq <= last_persisted_seq`. For a frame whose `seq > last_persisted_seq + 1` the Controller SHALL distinguish two cases:

- **Populated WAL** (`last_persisted_seq >= 0`) and not a runner-restart realign: a genuine protocol violation. The Controller SHALL reject the append (returning `{:error, {:gap, expected, got}}`) and the SDK SHALL treat it as a protocol violation and disconnect.
- **Empty WAL** (`last_persisted_seq == -1`) with `seq > 0` and not a runner-restart realign: a cold-takeover where this Controller holds no baseline for an Entity the runner already has state for. The Controller SHALL NOT disconnect. It SHALL enter an **awaiting-baseline** state for the Entity, shed the frame (incrementing a `wal_shed_total`-class counter, not erroring), drive a `CompactEntity` pull (see "Restart restores state via RestoreInitState handshake"), and keep shedding further gap frames without erroring until an `entity_state_snapshot` establishes the baseline. The pull SHALL be retried on a bounded interval for as long as the Entity remains awaiting-baseline: a pull that fails to dispatch (for example a torn control channel answering `:no_control_channel`) MUST NOT strand the Entity, because no snapshot will arrive to clear the awaiting-baseline state and every subsequent frame would be shed indefinitely. Retries SHALL be rate-limited so a per-frame gap flood cannot fan out into unbounded concurrent pulls.

A one-time on-disk migration `Flezha.Boot.StatePathMigrator.migrate/0` SHALL run at controller boot before `Flezha.State.Supervisor` starts any per-Entity `WalWriter`. The migrator SHALL walk every directory under `app_data/state/` of the form `{runner_id}/{operative_class}/{procedure_id}/` and atomically move its contents into `{operative_class}/{procedure_id}/`. The migration is idempotent (skips silently if the flat layout already exists) and logs each rename. After all old `{runner_id}/` directories are emptied, they are removed.

#### Scenario: WAL path uses flat layout

- **WHEN** an Entity `p1` with procedure `s1` and entity_key `k1` belonging to Operative class `MarketOperative` is running and persistence is enabled
- **THEN** the Controller SHALL write its WAL to `app_data/state/MarketOperative/s1/k1.wal`
- **AND** the path SHALL NOT contain any `runner_id` component regardless of which runner currently hosts the Operative

#### Scenario: WAL allocated lazily on first append

- **WHEN** the agent receives the first `entity_state_change` frame for an Entity that does not yet have a WAL file
- **THEN** the persistence module SHALL create the directory `app_data/state/{operative}/{procedure}/`, open the WAL file at `{entity_key}.wal` with `[:raw, :read, :write, :binary, :append]`, and start a `WalWriter` GenServer for the Entity

#### Scenario: Strictly monotonic seq accepted in order

- **WHEN** the SDK ships frames with `seq = 1, 2, 3` for a fresh Entity
- **THEN** the agent SHALL append all three records to the WAL in order; the file's `last_persisted_seq` SHALL equal 3 after the third append

#### Scenario: Re-shipped seq is silently deduped

- **WHEN** the SDK ships a frame with `seq = 5` for an Entity whose `last_persisted_seq = 7` (re-ship after SDK crash that lost the in-memory cursor)
- **THEN** the agent SHALL NOT append the duplicate; the agent SHALL increment `dedup_drops_total`; the wire reply SHALL be `:ok` (so the SDK does not retry)

#### Scenario: Out-of-order seq on a populated WAL is rejected

- **GIVEN** an Entity whose `last_persisted_seq = 7` (populated WAL)
- **WHEN** the SDK ships a frame with `seq = 10`
- **THEN** the agent SHALL refuse the append and reply `{:error, {:gap, expected: 8, got: 10}}`; the SDK SHALL treat this as a protocol violation and disconnect

#### Scenario: Gap on an empty WAL seeds a baseline instead of disconnecting

- **GIVEN** a freshly-assigned Controller with no WAL for an Entity (`last_persisted_seq = -1`) whose live runner has been producing changes and next emits `seq = 5001`, and the frame is not a runner-restart realign
- **WHEN** the agent receives the `entity_state_change` frame
- **THEN** the agent SHALL NOT reply with a fatal gap error and SHALL NOT disconnect the SDK; the agent SHALL shed the frame (incrementing the shed counter), mark the Entity awaiting-baseline, and dispatch a `CompactEntity` to pull a full-state baseline; further gap frames arriving before the snapshot SHALL be shed without error and SHALL NOT each dispatch their own pull
- **AND** once the pulled `entity_state_snapshot{seq}` lands, `last_persisted_seq` SHALL equal that snapshot's seq and the runner's next new delta SHALL append contiguously

#### Scenario: Crash-safe append via crc

- **WHEN** the agent process crashes mid-record-write, leaving a partial record at the file tail
- **THEN** on next agent boot, `RecordFrame.scan_for_last_valid_seq/1` SHALL detect the partial record — a read that runs off the end of the file, or a framed record whose crc32 fails and after which no complete record can begin — truncate the file to the last valid record, report the repair, and resume normal procedure; `last_persisted_seq` SHALL equal the seq of the last valid record

#### Scenario: A corrupt record with records behind it is not a torn tail

- **GIVEN** a WAL of three records whose second record's body has one flipped bit, so its framing is intact and the third record still decodes after it
- **WHEN** the agent opens that WAL
- **THEN** the open SHALL NOT truncate, the third record SHALL still be on disk afterwards, and the failure SHALL be reported naming the Entity, the seq and the byte offset

#### Scenario: One-time migration from runner_id-keyed layout

- **GIVEN** a controller running pre-change code wrote files under `app_data/state/exec-9/MarketOperative/btc/M1.wal` and `app_data/state/exec-12/PriceOperative/eth/M5.wal`
- **WHEN** the controller is upgraded and `Flezha.Boot.StatePathMigrator.migrate/0` runs at boot
- **THEN** the files SHALL be moved to `app_data/state/MarketOperative/btc/M1.wal` and `app_data/state/PriceOperative/eth/M5.wal`
- **AND** the now-empty `exec-9/` and `exec-12/` directories SHALL be removed
- **AND** if the migrator runs again on a subsequent boot, it SHALL skip silently because the flat layout already exists

#### Scenario: Migration handles runner_id rotation across restarts

- **GIVEN** a runner restart between WAL writes — first write under `exec-9`, restart yields `exec-12` for the same Operative
- **WHEN** the migrator runs (one-time, on first boot under new code)
- **THEN** files from BOTH `exec-9/MarketOperative/btc/` and `exec-12/MarketOperative/btc/` SHALL be merged into the flat path; on collision (same entity_key.wal in both), the migrator SHALL prefer the file with the higher last-record `seq` (read by scanning) and preserve the other as `<key>.wal.preserved-<exec-9>` for forensics

#### Scenario: A baseline pull that fails to dispatch is retried

- **GIVEN** an Entity marked awaiting-baseline whose `CompactEntity` pull failed to dispatch (the control channel answered `:no_control_channel`)
- **WHEN** further gap frames continue to arrive for that Entity
- **THEN** the agent SHALL retry the pull once the rate-limit interval has elapsed rather than shedding indefinitely without ever retrying
- **AND** the retries SHALL be bounded to at most one in-flight pull per Entity per interval, so a sustained gap flood does not fan out

### Requirement: Snapshot lifecycle with atomic rename and WAL truncation

When the agent receives an `entity_state_snapshot` frame from the SDK (typically in response to a `CompactEntity` command), the agent SHALL: (1) write the snapshot bytes to `{entity_key}.snap.{seq}.tmp` with `[:raw, :write, :binary]`; (2) `:file.sync/1` the temp file; (3) atomically rename to `{entity_key}.snap.{seq}` via `:file.rename/2`; (4) call `WalWriter.truncate_up_to(seq)` which drops the records with `seq' <= seq` and atomically renames the rewritten file into place. The cut SHALL cost the retained tail, not the WAL: the writer keeps a sparse seq-to-position index of its records, finds the boundary within one sample span and copies the tail as bytes, because the cut runs inside the writer's loop. A cut with no record at or below `seq` — a retention tick on a writer compaction already cut — SHALL answer at once without rewriting, because the rewrite runs inside the writer's loop and, under the effectively-once tier, every output rides that loop; only a writer awaiting its baseline runs such a cut, which is what establishes the baseline.

If the agent crashes between steps 3 and 4, the next boot SHALL find both the new snapshot and the un-truncated WAL; `lib/flezha/state/registry.ex::scan_dir/1` SHALL detect the situation, prefer the latest snapshot, and mark the WAL records ≤ `snapshot.seq` as redundant. Replay-from-this-state is correct: applying the snapshot then folding the WAL produces a state where the redundant records' apply_change calls are no-ops (because the snapshot already encodes their effects).

#### Scenario: Snapshot rename atomic

- **WHEN** the agent receives an `entity_state_snapshot` frame
- **THEN** an observer reading the directory SHALL see either the prior snapshot file OR the new snapshot file at any moment, but not a half-written file

#### Scenario: WAL truncation removes pre-snapshot records

- **WHEN** a snapshot at `seq = 100` lands and the WAL contains records seqs 1-150
- **THEN** after `WalWriter.truncate_up_to(100)`, the WAL SHALL contain only records seqs 101-150; the snapshot file SHALL contain the post-fold state at seq 100

#### Scenario: A cut with nothing below it leaves the WAL untouched
- **WHEN** the WAL holds seqs 101-150 after a compaction at 100 and a retention tick calls `WalWriter.truncate_up_to(100)`
- **THEN** the writer answers `:ok` without rewriting or renaming the file, and its un-fsynced bytes, credit and tail age are unchanged

#### Scenario: Crash mid-truncate leaves both files; replay is correct

- **WHEN** the agent crashes between snapshot-rename and WAL-truncate, leaving snapshot at seq 100 and full WAL with seqs 1-150
- **THEN** on next agent reconnect, the agent SHALL ship `RestoreInitState` with the snapshot bytes AND the full WAL bytes (records 1-150); the SDK SHALL apply the snapshot (state at seq 100) then fold records 1-150 — records 1-100 are no-ops because the snapshot already encodes their effects; records 101-150 advance the state correctly; terminal state matches the pre-crash state at seq 150

### Requirement: Asynchronous compaction triggered by per-Entity WAL size

Compaction SHALL be triggered by **WAL size or age**. When an Entity's WAL exceeds `compact_after_bytes` (per-Procedure, **default 8 MiB**; the SDK ships the per-Procedure value to the agent in the `AddOperative` metadata, and the SDK's own advertised default SHALL be 8 MiB so it does not override the global `:flezha, :default_compact_after_bytes`), OR the Entity's oldest un-snapshotted WAL record is older than `compact_after_age_ms` (global runtime knob, default `300_000`; `0` disables the age trigger), the agent SHALL trigger asynchronous compaction by enqueuing `Overwatch.handle_continue(:compact_entity, entity_id)`. The continuation SHALL ship `CompactEntity{entity_id}` to the SDK over the control channel. The age trigger SHALL be evaluated by the existing `Flezha.State.Watchdog` periodic sweep, reusing the same continuation as the size trigger.

The SDK SHALL acquire the Entity's sticky CAS, clone `Self::State`, release the CAS, serialize the clone (postcard), and ship `entity_state_snapshot{entity_id, seq, header, snapshot_bytes, crc32}` back to the agent. The agent SHALL handle the snapshot per the snapshot-lifecycle requirement above. The agent SHALL reply `CompactEntityResponse{entity_id, snapshot_seq, snapshot_bytes_size}` to confirm.

The hot path SHALL pay only the brief sticky-CAS hold during the state clone — microseconds, not the seconds-to-minutes a synchronous full-state snapshot would take.

#### Scenario: Compaction triggered by size threshold

- **WHEN** an Entity's `wal_size_bytes` grows past 8 MiB (the default `compact_after_bytes`)
- **THEN** the agent SHALL enqueue `:compact_entity` for that Entity without blocking new Change ingestion

#### Scenario: Compaction triggered by WAL age for a low-throughput Entity

- **GIVEN** `compact_after_age_ms = 300_000` and an Entity whose WAL has not reached 8 MiB but whose oldest un-snapshotted record is older than 5 minutes
- **WHEN** the `Flezha.State.Watchdog` sweep runs
- **THEN** the agent SHALL enqueue `:compact_entity` for that Entity so a fresh baseline is written even though the size threshold was never reached

#### Scenario: Age trigger disabled

- **GIVEN** `compact_after_age_ms = 0`
- **WHEN** the `Flezha.State.Watchdog` sweep runs over an Entity with an old but small WAL
- **THEN** the agent SHALL NOT enqueue compaction on the basis of age; only the size trigger applies

#### Scenario: Per-Procedure override propagated and honored

- **WHEN** a Procedure declares `compact_after_bytes() -> usize { 16 * 1024 * 1024 }` in the SDK
- **THEN** the SDK SHALL ship `compact_after_bytes = 16777216` for this Procedure in `AddOperative` metadata; the agent SHALL trigger compaction for this Procedure's Entities when WAL grows past 16 MiB

#### Scenario: Hot path holds CAS only for state clone

- **WHEN** the SDK receives `CompactEntity` and clones the state
- **THEN** the SDK SHALL acquire the sticky CAS, clone, release the CAS within the same `Procedure::run`-equivalent invocation, and serialize the clone in a non-runner context; the entity's input throughput SHALL NOT drop below 90% of pre-compaction throughput during the clone

### Requirement: Restart restores state via RestoreInitState handshake

On runner (re)bind, the controller SHALL recover each persisted Entity's state, choosing the direction by which side holds authoritative history:

- **Controller holds a baseline (a snapshot and/or WAL for the Entity) → push.** The controller SHALL drive a `RestoreInitState` handshake and the SDK SHALL rebuild the live Entity slot from the controller's durable WAL — snapshot apply followed by change-log fold — rather than initialising empty.
- **Controller holds no baseline for the Entity (no snapshot and no WAL) while the runner is live → pull.** The controller SHALL request a `CompactEntity` from the runner to obtain a full-state baseline, rather than pushing a `RestoreInitState`. Because a freshly-assigned controller cannot enumerate a live runner's Entities (its own disk is empty), the pull is data-plane-triggered by the first `entity_state_change` frame that arrives with an empty WAL (see "Per-Entity append-only WAL on the agent", empty-WAL gap).

The restore/resync dispatch path SHALL NOT ship a `RestoreInitState` whose assembled payload is empty (no snapshot and no WAL). It SHALL guard with `Flezha.State.Restore.has_persisted_state?/1` — as the bind-time path already does — so an empty payload can never rebuild a live Entity to `init_state` and destroy accumulated state. An empty-WAL condition SHALL route to the pull path above instead.

Restore SHALL be:

1. **Unconditional on every Overwatch bind** — driven by `Flezha.State.Recovery.resync_for_overwatch/2` (not a class→runner lookup that fails on cold boot). When the operative arrived via HotSwap handover, the in-band state is authoritative and the `{:kick_restore}` path SHALL skip to avoid double-applying.
2. **Keyed by operative instance id, not factory id** — the Entity wire identity is `[rc_len:u8][rc][sh_len:u8][sh][pk_len:u16 BE][pk]` keyed by the operative *instance*, so two operatives of the same factory class never collide on one WAL.
3. **Inclusive of seq 0** — an empty WAL initialises `last_persisted_seq = -1` (not `0`) and the "no snapshot" restore sentinel is `-1`, so the 0-based first change is never dedup-dropped. After fold, the SDK re-arms `change_seq = terminal_seq + 1` so the next produced frame is contiguous.
4. **Re-armed from the controller's terminal** — `RestoreInitState.terminal_seq` SHALL carry the controller's `max(snapshot seq, last WAL seq shipped)`, and the SDK SHALL re-arm `change_seq = max(change-log terminal, terminal_seq) + 1`. A snapshot-only restore — the WAL tail is empty after a compaction — carries no change-log record to derive the seq from; re-armed below the snapshot's seq, its next Changes would be dropped by the writer as duplicates.

#### Scenario: a snapshot-only restore keeps the change seq contiguous
- **GIVEN** an Entity compacted at seq 1 (snapshot present, WAL tail empty)
- **WHEN** it is restored on a rebind and produces its next Change
- **THEN** that Change SHALL carry seq 2 and SHALL be persisted, not dedup-dropped

#### Scenario: a stateful operative resumes its accumulated state after both runners restart
- **GIVEN** a `resume_probe` operative has accumulated a running total of 100 in persisted state
- **WHEN** both runners are gracefully stopped and restarted under load and the operative rebinds
- **THEN** the operative's restored state is 100 (the WAL snapshot + change-log), driving one more `+7` input emits 107, and no state is lost.

#### Scenario: the first change of a fresh operative survives restart
- **GIVEN** an operative produced exactly one state change (seq 0) and no snapshot exists
- **WHEN** the runner restarts and restore runs
- **THEN** the seq-0 change is included in the restored change-log (the `-1` empty-WAL/no-snapshot sentinels keep it), not dropped as a dedup floor.

#### Scenario: fresh controller pulls a baseline from a live runner
- **GIVEN** a Procedure is reassigned to a controller that has no WAL and no snapshot for its Entity, while the runner stays live holding accumulated state at seq 5000
- **WHEN** the operative binds and the runner's next `entity_state_change` arrives
- **THEN** the controller SHALL request a `CompactEntity` and, on receiving the `entity_state_snapshot`, write it as the Entity's baseline; the controller SHALL NOT push a `RestoreInitState` (which would carry an empty payload and reset the live Entity to `init_state`)

#### Scenario: empty-payload restore is never dispatched
- **GIVEN** a resync is triggered for an Entity whose controller has no snapshot and no WAL
- **WHEN** the recovery path evaluates whether to ship `RestoreInitState`
- **THEN** `has_persisted_state?/1` SHALL be false and no `RestoreInitState` SHALL be sent; the accumulated live state on the runner SHALL be preserved (not rebuilt to `init_state`)

### Requirement: Time-travel queries via WAL retention beyond latest snapshot

The agent SHALL retain pre-compaction WAL records for `time_travel_retention_hours` beyond the latest snapshot, configurable per-Operative-class via runtime config `time_travel_retention_hours.<OperativeClass>` (default `6` hours globally; setting to `0` disables time travel for that Operative class). The retention sweeper (`Flezha.State.Retention`) SHALL run on `time_travel_retention_check_interval_ms` (default 60_000), iterate Entities, and rewrite each Entity's WAL dropping records older than the cutoff.

The agent SHALL expose a query interface `Flezha.State.TimeTravel.query(entity_id, target)` where `target = {:seq, u64} | {:timestamp, datetime}`. The query SHALL: (1) locate the latest snapshot at-or-before the target; (2) read the tail-WAL records up to the target; (3) ship `RestoreInitState` to a transient SDK-side replay operative; (4) receive an `entity_state_snapshot` carrying JSON-encoded terminal state; (5) return the JSON to the caller.

The query SHALL be exposed via the `flz state at <operative/procedure/entity> <seq | timestamp>` CLI subcommand.

#### Scenario: Default 6-hour retention

- **WHEN** a runner does not configure `time_travel_retention_hours`
- **THEN** the agent SHALL retain WAL records for 6 hours beyond the latest snapshot

#### Scenario: Time-travel disabled (`retention_hours = 0`)

- **WHEN** `time_travel_retention_hours.<OperativeClass> = 0`
- **THEN** the retention sweeper SHALL truncate the WAL up to the latest snapshot's seq immediately on each tick; only the latest snapshot is queryable; queries for any seq < latest snapshot return `TimeTravelError{kind: :outside_retention}`

#### Scenario: Query within retention window returns state

- **WHEN** the runner runs `flz state at MarketOperative/btc/OhlcProcedure/M1 "2026-05-07T10:30:00Z"` AND that timestamp falls within the retention window
- **THEN** the agent SHALL load the latest snapshot at-or-before the timestamp and fold WAL records up to it; the CLI SHALL print the JSON-encoded state

#### Scenario: Query outside retention returns error

- **WHEN** the runner queries a timestamp older than the earliest available WAL record's record-time
- **THEN** the agent SHALL return `TimeTravelError{kind: :outside_retention, earliest_available: <ts>}`; the CLI SHALL print a clear error pointing at the earliest queryable point

#### Scenario: Query against persistence-off Operative errors

- **WHEN** the runner runs `flz state at <operative/procedure/entity> <target>` for an Entity whose Operative has `persistence_enabled = false`
- **THEN** the agent SHALL return `TimeTravelError{kind: :entity_not_persistent, operative, procedure, entity}`

#### Scenario: Live retention update applies on next sweep

- **WHEN** the runner runs `flz state retention MarketOperative 12` and the prior value was `6`
- **THEN** the runtime config SHALL be updated to 12 hours; the next retention-sweeper tick SHALL apply the new value (records older than 12 hours are deleted; records younger SHALL be retained going forward)

### Requirement: Fsync batching at configurable cadence

The agent SHALL run a single shared `Flezha.State.FsyncBatcher` GenServer per BEAM node. The batcher SHALL maintain a `MapSet` of dirty writer IDs (Entities whose `WalWriter`, or input-logs whose log process, has appended bytes since the last fsync). A writer SHALL mark itself dirty once per fsync window — on the first append after its last sync, not on every append. On each tick at interval `agent_fsync_interval_ms` (default 5; configurable via runtime config in range [1, 1000]), the batcher SHALL: (1) iterate the dirty set; (2) cast `:fsync` to each writer, so the writers sync concurrently in their own processes and a writer busy behind a deep mailbox delays no other writer's durability, credit release or outbox dispatch; (3) reset the set; (4) schedule the next tick via `Process.send_after/3`. `flush_now` and `flush_all` SHALL instead call each dirty writer synchronously, so their callers return with every sync done; a writer that does not confirm a synchronous sync SHALL stay in the dirty set for the next drain.

A (re)started batcher SHALL seed its dirty set with every registered writer: with one mark per window, a dirty set lost with a crashed batcher would otherwise never be rebuilt, and a writer's un-synced tail would wait for its next append.

`WalWriter` SHALL invoke `:file.datasync/1` on its file handle when receiving the `:fsync` message and SHALL update `fsync_durability_lag_ms` to `now - oldest_pending_record_timestamp`.

#### Scenario: Default 5 ms tick = 200 Hz fsync rate

- **WHEN** a runner does not configure `agent_fsync_interval_ms`
- **THEN** the batcher SHALL tick every 5 ms (200 Hz) AND each dirty `WalWriter` SHALL fsync at most once per tick

#### Scenario: 5 ms + 5 ms = 10 ms total durability bound

- **WHEN** a Procedure records a Change at time `t` under default flusher and fsync settings (`linger_ms = 5` SDK-side; `agent_fsync_interval_ms = 5` agent-side)
- **THEN** the Change SHALL be durably fsynced to disk on the agent by `t + 10 ms` modulo network round-trip and OS scheduling; `fsync_durability_lag_ms` SHALL stabilize below 10 ms under steady-state load

#### Scenario: Runner-tightened bound

- **WHEN** a runner sets `agent_fsync_interval_ms = 1`
- **THEN** the batcher SHALL tick every 1 ms (1000 Hz); the durability bound SHALL be ~2 ms total at the cost of more frequent fsyncs

#### Scenario: One dirty mark per fsync window

- **WHEN** a `WalWriter` appends N records between two batcher ticks
- **THEN** it marks itself dirty once, on the first of those appends, and marks again only after the tick has synced it

#### Scenario: A slow writer does not delay its siblings

- **WHEN** one dirty writer's mailbox is deep enough that its sync runs late
- **THEN** every other dirty writer syncs on the same tick, because the tick casts instead of waiting on each writer in turn

#### Scenario: A restarted batcher syncs every registered writer

- **WHEN** the batcher crashes with marks in its dirty set and restarts
- **THEN** its first tick syncs every registered writer, so no un-synced tail waits for that writer's next append

### Requirement: Per-Operative persistence toggle resolution at operative startup

The agent SHALL resolve a per-Operative-class boolean `persistence_enabled` at the moment an Operative instance is added (`AddOperative` command processed in `Overwatch.init/1`) using the following ordered rule:

1. If the agent's runtime config defines `operative_persistence.<OperativeClass>` for this Operative class, use that value.
2. Otherwise, if the `AddOperative` metadata carries an Operative-class const value (`StreamOperative::PERSISTENCE_ENABLED`), use that value.
3. Otherwise, use the built-in default of `true`.

The resolved value SHALL be bound to the Operative instance for its entire lifetime. Subsequent changes to the runner-side config or to the SDK's Operative-class const (via SDK redeploy) SHALL NOT affect already-running instances.

When `persistence_enabled = false`, the agent SHALL NOT allocate WAL or snapshot files for any of that Operative's Entities, SHALL ignore (drop with a counter increment) any incoming `entity_state_change` or `entity_state_snapshot` frames for those Entities, and SHALL NOT issue `RestoreInitState` on reconnect.

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

- bytes 0:1 — `sdk_lang: u8`, an **allocated identifier** with one table of record in `flz_core/src/header.rs`: `1` Rust, `2` Java, `3` Elixir, `4` Node, `5` Ruby, `6` PHP, `7` Python. Values `8–254` are allocated to further SDKs **in landing order**, one value per SDK, and a proposal SHALL NOT assert a value it has not been allocated. `0` is reserved and means "unspecified"; `255` is reserved.
- bytes 1:5 — `sdk_version: u32 BE`, the **writing SDK's own** released version packed as `(major << 16) | (minor << 8) | patch` with each component clamped at 255. `0` means the writing SDK did not state one and SHALL NOT be read as version 0.0.0.
- bytes 5:9 — `schema_version: u32 BE` (per-Procedure, user-controlled)
- bytes 9:10 — `format_id: u8` (1 = postcard, 2 = effect record, 3 = timer ops, 4 = kryo-registered, 5 = msgpack, 6 = json, 7 = etf — Erlang External Term Format, the Elixir SDK's codec). A `format_id` names the codec that wrote the body and MAY be shared by SDKs that write the same codec, because the cross-language gate is `sdk_lang` and not this byte; a new codec takes the next free value in the same table of record.
- bytes 10:12 — reserved, MUST be zero
- bytes 12:16 — `crc32: u32 BE` over the payload bytes (change_bytes for WAL records; snapshot_bytes for snapshot files)

The agent SHALL stamp the header at write time using the SDK-supplied values (the SDK ships the header pre-stamped in the wire frame; the agent writes it verbatim). The agent SHALL treat an unknown `sdk_lang` or `format_id` as opaque — it stores and re-ships such records unchanged and SHALL NOT reject them, so allocating a new SDK's value requires no agent change. The agent SHALL, however, **read** byte 0 to resolve a factory's established language for the placement guard; that read is a routing decision, not a validation of the body.

The SDK SHALL verify the header at read time on **every** restore path — the typed path, the host path and an SDK that is its own core alike — before any body reaches a codec. A record whose `sdk_lang` is not the reading SDK's own value SHALL be refused with a language-mismatch error naming both values; the SDK SHALL NOT attempt to decode it, and SHALL NOT fall back to a partial or empty state. The agent SHALL NOT verify the header except for the leading WAL record-framing crc32 over `header + change_bytes`.

**The language is resolved before the format.** A record from another SDK is neither a format question nor a schema one, so the language gate SHALL be applied first and its refusal SHALL name both languages. Reading the format byte first makes a record written by an SDK with a different codec report as a malformed header, naming neither.

**`sdk_version` identifies the SDK that wrote the record, and it is advisory.** Each SDK SHALL stamp its own released version — the version its own package metadata declares, never the version of a component it links — and an SDK that is a host over the shared kernel SHALL carry that value across the declaration surface rather than let the kernel supply one of its own. The core SHALL refuse a class declaration that states no version, so a record written after this rule is in force SHALL NOT carry `0`.

**No restore path SHALL gate on `sdk_version`.** The language byte decides which codec may read a body and the schema version decides which shape it has; those two carry every refusal. A migration MAY consult `sdk_version` to distinguish records that are identical in language and schema but were written by SDK builds known to differ, and SHALL NOT decide a body's shape from it alone. The packing is lossy — a pre-release or build suffix is dropped and a component above 255 clamps — so two distinct SDK builds MAY stamp the same value, and equality of this field SHALL NOT be read as identity of the writer.

#### Scenario: Header stamped verbatim from SDK

- **WHEN** the SDK ships an `entity_state_change` frame with header `[0x01, ...sdk_version..., ...schema_version..., 0x01, 0x00, 0x00, ...crc32...]`
- **THEN** the agent SHALL append the WAL record byte-for-byte; no agent-side header rewriting SHALL occur

#### Scenario: Cross-language load fails loud at SDK side

- **WHEN** a Java SDK reconnects to an Entity whose WAL was written by a Rust SDK (header `sdk_lang = 1`)
- **THEN** on `RestoreInitState` decode, the Java SDK SHALL fail with a cross-language-mismatch error and SHALL NOT proceed; the agent's role is unchanged (it ships bytes, the SDK enforces the language gate)

#### Scenario: The host restore path enforces the gate too

- **WHEN** a host-layer class (a class served through the C ABI, including every JNI- and FFI-hosted SDK) is sent a `RestoreInitState` whose records carry another SDK's `sdk_lang`
- **THEN** the core's host restore fold SHALL refuse before handing any body to the host codec, and SHALL report a language-mismatch error naming the expected and found values

#### Scenario: The agent stores an unknown language opaquely

- **WHEN** the agent receives records stamped with an `sdk_lang` value it has no knowledge of
- **THEN** it SHALL store, truncate, compact and re-ship them exactly as it does any other record, and SHALL NOT reject them

#### Scenario: Two SDKs never share a language value

- **WHEN** the allocation table is read
- **THEN** each shipped SDK SHALL hold exactly one value and no value SHALL be held by two SDKs, including SDKs whose implementation is still only proposed

#### Scenario: A record written by a differently-encoded SDK is refused by language, not by format

- **WHEN** a record stamped with the Node SDK's `sdk_lang` and the MessagePack `format_id` reaches an SDK that writes postcard
- **THEN** the restore SHALL be refused with a language-mismatch error naming both languages, and SHALL NOT be reported as a malformed or unknown-format header

#### Scenario: The MessagePack identifier is shared, not minted

- **WHEN** the Node SDK and the Ruby SDK each write a durable record
- **THEN** both `format_id` bytes SHALL be the one already allocated to MessagePack, and an SDK that writes the same codec SHALL reuse it rather than take a new value

#### Scenario: Two SDKs sharing a codec are still separated by language

- **WHEN** a Ruby SDK writes a change body and a Node SDK later reconnects to that Entity
- **THEN** the header SHALL carry the Ruby SDK's allocated `sdk_lang` and `format_id = 5`, and the restore SHALL be refused with a language-mismatch error naming both SDKs — the shared `format_id` SHALL NOT let one SDK's codec read the other's bytes

#### Scenario: PHP records are self-describing

- **WHEN** a PHP-hosted Entity writes a change record
- **THEN** the header SHALL carry the PHP SDK's allocated `sdk_lang` and the JSON `format_id`, and a later PHP restore of that Entity SHALL decode it

#### Scenario: A PHP SDK refuses another language's records

- **WHEN** a PHP SDK is bound to an Entity whose WAL was written with any other SDK's `sdk_lang`
- **THEN** the core's host restore fold SHALL refuse before any body reaches the PHP codec, and the controller SHALL leave the operative unbound rather than cold-starting it

#### Scenario: A Python-written change is self-describing on restore

- **WHEN** a Python SDK writes a change body and later restores the Entity
- **THEN** the header SHALL carry the Python SDK's allocated `sdk_lang` and `format_id = 5`, and a Rust or Java SDK reconnecting to that Entity SHALL fail with the same cross-language-mismatch error rather than misreading the payload

#### Scenario: A shared codec byte does not weaken the language gate

- **WHEN** three SDKs write MessagePack bodies and therefore all stamp `format_id = 5`
- **THEN** their records SHALL remain distinguishable by `sdk_lang`, and a restore SHALL refuse on that byte rather than on the codec byte

#### Scenario: An Elixir record is self-describing and the Elixir SDK refuses every other language

- **WHEN** an Elixir Entity writes a change record and later restores it, and separately is sent a record stamped with any other SDK's `sdk_lang`
- **THEN** the written header SHALL carry `sdk_lang = 3` and `format_id = 7`, the Elixir restore SHALL decode it, and the foreign record SHALL be refused by the SDK's own fold with the language-mismatch wording naming both languages before its format byte is read

#### Scenario: A record names the SDK that wrote it, not the kernel it linked

- **GIVEN** a host-layer SDK at its own released version, running over a kernel built at a different version
- **WHEN** that SDK writes a change record
- **THEN** bytes 1:5 SHALL carry the SDK's own version, and SHALL NOT carry the kernel's

#### Scenario: A declaration stating no version is refused before any record is written

- **WHEN** a host class is declared with `sdk_version` absent or zero
- **THEN** the declaration SHALL be refused naming the field, and no Entity of that class SHALL reach a state where it could stamp a header

### Requirement: Idempotent emits with stable dedup keys

The Rust SDK runtime SHALL automatically attach `flezha.dedup_key = "{operative_class}/{procedure_id}/{entity_key}/{change_seq}"` to outgoing plugin-bound emit metadata for any emit issued during a `state.apply(...)`-driven `Procedure::run` invocation. The agent's plugin-provider path SHALL NOT alter or strip this metadata key; it SHALL flow through to the sink verbatim.

The Kafka producer plugin (`Flezha.Plugin.Provider.Kafka`) SHALL use the `flezha.dedup_key` value as the Kafka producer key (taking precedence over any other configured keying strategy). Other plugin providers MAY use the key for sink-specific dedup purposes; the contract is that the key is opaque metadata.

User-supplied metadata via `ctx.emit_with_meta(value, meta)` SHALL have its `flezha.dedup_key` entry silently dropped (matching the existing reserved-key behavior for `channel`); the SDK runtime stamps its own value.

#### Scenario: Dedup key attached automatically

- **WHEN** a Procedure at `(MarketOperative/btc, OhlcProcedure, M1)` records a Change at `seq = 12345` AND the same `Procedure::run` invocation calls `ctx.emit::<BarClosed>(...)` to a Kafka-bound channel
- **THEN** the outgoing Kafka producer SHALL receive the message with key `"MarketOperative/OhlcProcedure/M1/12345"`

#### Scenario: Replayed Change preserves dedup key

- **WHEN** an Entity is restored from snapshot + WAL and re-issues an emit for `seq = 12345`
- **THEN** the re-issued Kafka producer message SHALL carry the identical key `"MarketOperative/OhlcProcedure/M1/12345"` as the original emit; downstream sink dedup (e.g. Kafka log compaction by key) SHALL collapse the duplicates

#### Scenario: Agent-side outbox dedupes by (entity, change_seq)

- **WHEN** the SDK ships `entity_state_change` with `seq = 12345` to the agent twice for the same Entity (once before crash, once after replay)
- **THEN** the agent SHALL persist exactly one record; the duplicate SHALL be detected via `seq <= last_persisted_seq` and discarded silently with `dedup_drops_total += 1`

#### Scenario: User-supplied dedup_key is dropped

- **WHEN** a Procedure calls `ctx.emit_with_meta(value, %{"flezha.dedup_key" => "custom"})`
- **THEN** the user's `flezha.dedup_key` entry SHALL be silently dropped; the SDK runtime SHALL stamp `"{operative}/{procedure}/{entity}/{seq}"` as the dedup key

### Requirement: Per-Entity persistence metrics

Each Entity's metric set SHALL expose six counters via `:counters` references, accessible from `Flezha.State.Metrics.to_map(entity_id)` and exported through the existing `Flezha.State.Metrics` path:

- `persisted_changes_total` — monotonic; incremented per Change appended to the WAL (post-coalesce, post-dedup).
- `persisted_bytes_total` — monotonic; incremented by the byte length of each appended record.
- `wal_size_bytes` — gauge; current WAL file size (read from `:file.read_file_info/1` after each append and after each retention-sweep rewrite).
- `snapshot_seq` — gauge; seq of the latest snapshot file (or 0 if none).
- `flush_latency_ms_p99` — gauge; rolling 99th-percentile of `now - record_received_ts` measured per fsync, computed via a 1024-bucket reservoir sampler.
- `fsync_durability_lag_ms` — gauge; `now - oldest_pending_record_ts` at each `FsyncBatcher` tick.

The counters SHALL use `Ordering::Relaxed` (the BEAM `:counters` API does not expose ordering modes; this is satisfied by the underlying implementation). External metric systems (Prometheus, StatsD) integration is out of scope for this requirement; the existing `Flezha.State.Metrics` path is the export surface.

#### Scenario: Counters initialized at Entity construction

- **WHEN** an Entity is constructed with `persistence_enabled = true`
- **THEN** all six counters SHALL be allocated; `persisted_changes_total = 0`, `persisted_bytes_total = 0`, `wal_size_bytes = 0`, `snapshot_seq = 0` (no snapshot yet), `flush_latency_ms_p99 = 0`, `fsync_durability_lag_ms = 0`

#### Scenario: Counters update after Change append

- **WHEN** the agent appends 10 Changes totaling 500 bytes to an Entity's WAL
- **THEN** `persisted_changes_total` SHALL increase by 10; `persisted_bytes_total` SHALL increase by `>= 500` (record framing overhead included; ≈ `500 + 10 * (8 + 4 + 16 + 4) = 820` bytes in practice); `wal_size_bytes` SHALL equal the file size after the appends

#### Scenario: Counters survive agent restart

- **WHEN** the agent is restarted and reads back the WAL files
- **THEN** `persisted_changes_total` and `persisted_bytes_total` SHALL be recomputed from the WAL records (scanning); `snapshot_seq` SHALL be the highest snapshot file's seq; `wal_size_bytes` SHALL match the file size; `flush_latency_ms_p99` and `fsync_durability_lag_ms` SHALL reset to 0

### Requirement: Back-pressure via OverflowPolicy when WAL outpaces fsync

For sessions that are NOT credit-speaking (pre-credit SDKs, or the controller running with credit flow control disabled), the agent SHALL monitor each `WalWriter`'s `pending_bytes` (queued-but-not-fsynced bytes). When `pending_bytes >= wal_high_watermark_bytes` (default 16 MiB), the agent SHALL ship `EntityMailboxStatus{entity_id, status: :full}` over the control channel to the SDK. When `pending_bytes` falls below `wal_low_watermark_bytes` (default `wal_high_watermark_bytes / 2`), the agent SHALL ship `EntityMailboxStatus{entity_id, status: :open}`.

The SDK's per-Entity mailbox observer SHALL treat `:full` as equivalent to the existing per-Entity mailbox-full condition that drives `OverflowPolicy`; the producing Procedure's `ctx.emit(...)` SHALL apply the channel's declared policy (`DropNothing` waits for room, `DropNewest` drops, `DropOldest` discards-oldest).

The agent SHALL NOT issue `:full` more than once per Entity while the watermark is exceeded (idempotent transitions); SHALL NOT issue `:open` until `:full` was previously sent for that Entity (no spurious `:open` frames).

For credit-speaking sessions the agent SHALL NOT ship `EntityMailboxStatus` frames: back-pressure for those sessions is carried entirely by the credit-grant protocol (see the `credit-grant-flow-control` capability), which parks the sender on window exhaustion instead of signalling a status flag.

#### Scenario: High-watermark triggers :full status

- **WHEN** an Entity's `WalWriter` accumulates `pending_bytes = 16 MiB + 1 record` on a non-credit-speaking session
- **THEN** the agent SHALL ship `EntityMailboxStatus{entity_id, status: :full}` exactly once

#### Scenario: Low-watermark triggers :open status

- **WHEN** the same Entity's `pending_bytes` drains below 8 MiB after fsyncs
- **THEN** the agent SHALL ship `EntityMailboxStatus{entity_id, status: :open}` exactly once

#### Scenario: OverflowPolicy::DropNothing parks producer under back-pressure

- **WHEN** a Procedure with `OverflowPolicy::DropNothing` on its emit channel calls `ctx.emit(...)` for an Entity under `:full` status on a non-credit-speaking session
- **THEN** the producer SHALL park its calling thread; on receipt of `:open` status the producer SHALL unpark and the emit SHALL succeed; the producer's `Procedure::run` SHALL eventually return `Ok(())`

#### Scenario: Credit-speaking session receives no status flags

- **WHEN** a credit-speaking SDK's Entity drives `pending_bytes` past `wal_high_watermark_bytes`
- **THEN** no `EntityMailboxStatus` frame is shipped; the sender is throttled by credit exhaustion instead

### Requirement: Hard in-memory bound sheds WAL appends past the watermark

Each `WalWriter` SHALL enforce a hard in-memory bound on un-fsynced data —
covering both queued-but-not-fsynced bytes and pending mailbox depth. When the
hard bound is exceeded, `append_async/5` SHALL shed the incoming Change (drop
it without writing) and increment a shed counter, rather than growing the
writer's mailbox or un-fsynced bytes without bound. The data plane SHALL NOT
block: the shed leaves a sequence gap that the existing resync path repairs.

The hard bound SHALL be strictly above the soft watermark. For non-credit-speaking
sessions, shedding remains the last-resort safety net for a slow disk or an SDK
that does not honour the `EntityMailboxStatus` wire signal. For credit-speaking
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
  reached and `wal_shed_total` for its Entities does not advance

### Requirement: Crash-safe persistence on agent reboot

On agent boot, after `Flezha.Boot.StatePathMigrator.migrate/0` runs, `Flezha.State.Supervisor` SHALL call `Registry.scan_dir/1` for each `app_data/state/{operative}/{procedure}/` directory and reconstruct the in-memory persistence state:

- For each `entity_key.wal` file: start a `WalWriter` GenServer; `WalWriter::scan_for_last_valid_seq/1` runs at `init/1` to recover `last_persisted_seq` and truncate any partial-write tail.
- For each `entity_key.snap.{seq}` file (latest): record the `snapshot_seq` counter.
- For each `entity_key.snap.{seq}.tmp` file: unlink (orphaned mid-write).
- For each `entity_key.wal.rewriting` file: unlink (orphaned retention rewrite).
- For each `entity_key.outbox.dedup` file: read the persisted `last_persisted_seq` for the dedup cache (recovered from the WAL scan as a cross-check).

`Flezha.State.Outbox` SHALL persist `last_persisted_seq` per Entity into `entity_key.outbox.dedup` (8-byte u64 BE) on every WAL append via a write-then-rename atomic update; this allows fast dedup-cache priming on boot without re-scanning the entire WAL.

#### Scenario: Boot recovers from clean shutdown

- **WHEN** the agent boots after a clean shutdown
- **THEN** for each Entity's WAL file, `WalWriter::scan_for_last_valid_seq` SHALL run successfully and recover `last_persisted_seq` matching the on-disk state

#### Scenario: Boot recovers from crash during WAL append

- **WHEN** the agent boots after a crash mid-WAL-append (partial record at file tail)
- **THEN** `scan_for_last_valid_seq` SHALL detect the crc-mismatched partial record AND truncate it; `last_persisted_seq` SHALL equal the seq of the last valid (crc-good) record

#### Scenario: Boot recovers from crash during snapshot rename

- **WHEN** the agent boots after a crash between snapshot fsync and rename, leaving `entity_key.snap.{seq}.tmp`
- **THEN** boot SHALL unlink the `.tmp` file; the prior snapshot (if any) and the un-truncated WAL are intact; persistence resumes correctly

#### Scenario: Boot recovers from crash during retention rewrite

- **WHEN** the agent boots after a crash during the retention sweeper's WAL rewrite, leaving `entity_key.wal.rewriting`
- **THEN** boot SHALL unlink the `.rewriting` file; the original WAL is intact; the next sweeper tick re-runs the rewrite idempotently

### Requirement: flz state CLI subcommand surface

The agent SHALL expose a `flz state` CLI subcommand tree, using operative vocabulary in every path argument:

- `flz state get <operative/procedure/entity>` — current in-memory state of one live Entity, with the `change_seq` it was read at. Refuses a non-resident Entity naming the reason.
- `flz state at <operative/procedure/entity> <seq | timestamp>` — point-in-time state query per the time-travel requirement. Returns JSON-encoded state.
- `flz state list <operative>` — for each Entity of the Operative, prints `wal_seq`, `snapshot_seq`, `wal_bytes`, `flush_latency_ms_p99`, `fsync_durability_lag_ms`. Reads `Metrics.to_map/1`.
- `flz state rotate <operative/procedure/entity>` — forces compaction by enqueuing the per-Entity compaction continuation on the Overwatch. Returns immediately ("compaction queued"); the snapshot completes asynchronously.
- `flz state retention <operative> <hours>` — sets `time_travel_retention_hours.<OperativeClass>` in runtime config (`persistent: true`); the next retention-sweeper tick reads the new value.

#### Scenario: flz state get returns the live value

- **WHEN** the runner runs `flz state get MarketOperative/OhlcProcedure/M1` against a live Entity
- **THEN** the CLI SHALL print that Entity's state as JSON with its `change_seq` and exit 0

#### Scenario: flz state get refuses a non-resident Entity

- **WHEN** the runner runs `flz state get` against an idle-evicted Entity
- **THEN** the CLI SHALL exit non-zero naming eviction and pointing at `flz state at`

#### Scenario: flz state list enumerates Entities

- **WHEN** the runner runs `flz state list MarketOperative`
- **THEN** the CLI SHALL print one line per Entity with the listed fields; output SHALL be tabular (column-aligned)

#### Scenario: flz state rotate triggers async compaction

- **WHEN** the runner runs `flz state rotate MarketOperative/btc/OhlcProcedure/M1`
- **THEN** the CLI SHALL print "compaction queued" and exit 0; the agent SHALL ship `CompactEntity` to the SDK; the resulting snapshot SHALL appear on disk within seconds

#### Scenario: flz state retention persists across agent restart

- **WHEN** the runner runs `flz state retention MarketOperative 12` and the agent is subsequently restarted
- **THEN** after restart, the resolved `time_travel_retention_hours` for `MarketOperative` SHALL be 12

### Requirement: Snapshot files use flat path

The Controller SHALL store snapshot files at `app_data/state/{operative_class}/{procedure_id}/{entity_key}.snap.{seq}`. The on-disk path SHALL NOT include `runner_id`. The on-the-wire payload-id SHALL be `entity_state_snapshot`.

#### Scenario: Snapshot path is flat

- **WHEN** the Controller compacts the WAL of Entity `p1` (Operative class `MarketOperative`, procedure `s1`, entity_key `k1`) and assigns snapshot seq=42
- **THEN** the resulting file SHALL be at `app_data/state/MarketOperative/s1/k1.snap.42`

### Requirement: ProcessorApplyState renamed to EntityApplyState

The protobuf message carrying inbound state application SHALL be named `EntityApplyState`. The field naming the owning operative SHALL be `operative_id`; field `entity_id` (string) IS the canonical identifier for the targeted Entity. The on-the-wire payload-id SHALL be `entity_apply_state`. The inverse `EntityState` message and the `entity_state_change` / `entity_state_snapshot` / `delete_entity_state` payload-ids replace the former `partition_*` names in lockstep across all SDKs.

#### Scenario: Rust SDK emits EntityApplyState
- **WHEN** the Rust SDK applies a state change to Entity `p1` belonging to Operative `r1`
- **THEN** the SDK SHALL send an `EntityApplyState { operative_id: "r1", entity_id: "...", change_bytes: ... }` on the control channel; the Controller SHALL accept it and route it to the Entity's WAL writer

#### Scenario: Java SDK emits EntityApplyState
- **WHEN** the Java SDK applies a state change to an Entity
- **THEN** the SDK SHALL send an `EntityApplyState` message identical in shape to the Rust SDK's; the Controller SHALL handle both identically

### Requirement: SDK serializes Operative state on `EntityState` request via the Decision-10 JSON shape

When the SDK receives an `EntityState{operative_id, procedure, entity_key}` command, it SHALL respond with a JSON object whose top-level keys are the Operative's Procedure names and whose values are:

- For single-mode Procedures (one Entity per Procedure): the serialized `Self::State` of that Entity.
- For static-mode Procedures (`entity_keys() = [...]`): an inner JSON object whose keys are the Procedure's Entity keys and whose values are each Entity's serialized `Self::State`.
- For dynamic-mode Procedures: an inner JSON object of the same shape as the static case, whose keys are the live Entity keys held in the dynamic Entity registry at serialization time.

`procedure` and `entity_key` are optional selectors. When both are absent, the reply SHALL cover every Procedure of the Operative — the pre-existing behaviour, byte-identical. When `procedure` is present, serialization SHALL be restricted to that Procedure's Entities. When `entity_key` is also present, serialization SHALL be restricted to that one Entity. `entity_key` present without `procedure` SHALL be refused, because an Entity key is unique only within its Procedure.

The wire payload SHALL be `entity_state` payload-id with the JSON object as the message bytes. Empty Operatives (no live Entities) SHALL produce `{}`. The reply frame's metadata SHALL carry `flz.change_seq` for the serialized Entity when a selector names exactly one.

Serialization SHALL take the Entity's state lock, SHALL NOT drain pending changes, and SHALL NOT trigger compaction or WAL truncation; the serialized value therefore includes applied-but-unflushed changes.

State serialization SHALL go through a per-Procedure fn pointer captured at `register_stream::<R>()` time. The fn pointer downcasts the type-erased state held in `EntitySlot::state` to its statically-known `EntityState<S>` and emits `serde_json::Value` via the `S::State: Serialize` bound on the trait. No additional user-facing trait machinery is required beyond the existing `Serialize + DeserializeOwned` bounds on `Procedure::State`.

#### Scenario: Single-mode Procedure returns its state at the procedure-name key

- **WHEN** the SDK receives `EntityState{operative_id}` with no selector for an Operative whose only Procedure is `"counter"` (single-mode) with state `{"value": 42}`
- **THEN** the wire reply payload SHALL be `{"counter": {"value": 42}}`

#### Scenario: Static-mode Procedure returns a key-keyed object

- **WHEN** the SDK receives `EntityState{operative_id}` with no selector for an Operative whose only Procedure is `"ohlc"` (static-mode, keys `["1m", "1h"]`) with `1m` state `{"high": 10}` and `1h` state `{"high": 20}`
- **THEN** the wire reply payload SHALL be `{"ohlc": {"1m": {"high": 10}, "1h": {"high": 20}}}`

#### Scenario: Dynamic-mode Procedure returns its live Entities

- **WHEN** the SDK receives `EntityState{operative_id}` for an Operative whose `"candle"` Procedure holds lazily created Entities `BTCUSD@1m` and `ETHUSD@1m`
- **THEN** the reply SHALL be `{"candle": {"BTCUSD@1m": {...}, "ETHUSD@1m": {...}}}`

#### Scenario: A selector narrows the serialization

- **WHEN** the SDK receives `EntityState{operative_id, procedure: "candle", entity_key: "BTCUSD@1m"}` for an Operative holding two Procedures and two thousand Entities
- **THEN** exactly one Entity SHALL be serialized, the reply SHALL be `{"candle": {"BTCUSD@1m": {...}}}`, and the frame metadata SHALL carry that Entity's `flz.change_seq`

#### Scenario: An entity_key without a procedure is refused

- **WHEN** the SDK receives `EntityState{operative_id, entity_key: "BTCUSD@1m"}` with no `procedure`
- **THEN** the SDK SHALL refuse the command rather than searching Procedures for a matching key

### Requirement: SDK applies `AddOperative.init_values` JSON to per-Entity state

When the SDK receives an `AddOperative{operative_id, factory_id, init_values}` command and `init_values` is a non-empty JSON object, the SDK SHALL parse the JSON, walk its top-level keys, and for each key that matches a registered Procedure name on the Operative's class:

- For single-mode Procedures: deserialize the JSON value directly into `S::State` via the per-Procedure `deserialize_state` fn pointer (captured at registration time and workflow on `S::State: DeserializeOwned`), then overwrite the freshly-`init_state`d Entity's state with the deserialized value.
- For static-mode Procedures: walk the inner object's keys; for each `(key, value)` pair where `key` is a declared `entity_key`, deserialize the inner value into `S::State` and overwrite that Entity's state.

Top-level keys that do not match any Procedure name SHALL be ignored (with an `eprintln!` log line at the SDK boundary). Inner-object keys that do not match a declared `entity_key` SHALL also be ignored.

If `init_values` is empty (`""` on the wire), the SDK SHALL leave each Entity's state untouched after the cold-start `Procedure::init_state(entity_key, None)` returned by the build path.

`MigrationDeserialization{factory_id, operative_id, serialized_state}` SHALL reuse the same `apply_init_values` path: the `serialized_state` payload is the UTF-8 JSON blob produced by `serialize_operative_state` on the source runner; applying it to a freshly-started instance restores byte-identical state.

#### Scenario: Cold start with init_values seeds single-mode Procedure

- **WHEN** the SDK receives `AddOperative{operative_id, factory_id, init_values: "{\"counter\":{\"value\":7}}"}` for an Operative with one single-mode Procedure `"counter"`
- **THEN** the SDK SHALL deserialize `{"value":7}` directly into `CounterState` and overwrite the Entity's state with `CounterState { value: 7 }`

#### Scenario: Cold start with init_values seeds static-mode Procedure per key

- **WHEN** the SDK receives `AddOperative{...init_values: "{\"ohlc\":{\"1m\":{\"high\":5},\"1h\":{\"high\":10}}}"}` for an Operative with `entity_keys = ["1m", "1h"]`
- **THEN** the SDK SHALL deserialize `{"high":5}` into the `"1m"` Entity's state AND deserialize `{"high":10}` into the `"1h"` Entity's state

#### Scenario: Empty init_values uses the default path

- **WHEN** the SDK receives `AddOperative{...init_values: ""}`
- **THEN** the SDK SHALL leave each Entity's state at the value `Procedure::init_state(entity_key, None)` returned at build time

#### Scenario: Round-trip via MigrationSerialization / MigrationDeserialization

- **WHEN** the SDK on the source runner responds to `MigrationSerialization{operative_id}` with payload bytes `B = serialize_operative_state(instance)`, and a second SDK on the destination runner receives `MigrationDeserialization{factory_id, operative_id, serialized_state: B}`
- **THEN** the destination's freshly-started instance SHALL have Entity state byte-identical to the source's at the moment of serialization, as observed by `serialize_operative_state` re-issued against the destination

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

When the Controller receives an `entity_state_change` frame whose `seq > last_persisted_seq + 1`, it SHALL treat the gap as a recoverable desync rather than a terminal protocol error. The Controller SHALL emit `[:flezha, :state, :seq_gap]` telemetry with measurements `%{expected: integer(), got: integer()}` and metadata identifying the Entity, and SHALL initiate the `RestoreInitState` resync handshake for that Entity (assemble snapshot + WAL tail per the existing "Restart restores state via RestoreInitState handshake" flow, ship it to the SDK, await `RestoreInitStateResponse{terminal_seq}`, and resume accepting Changes at `terminal_seq + 1`). After a successful resync the Entity's WAL SHALL advance again and its outbox SHALL drain to the output plugins.

The Controller SHALL NOT leave an Entity indefinitely rejecting Changes with `{:error, {:gap, expected, got}}`. The strict acceptance rule for the contiguous case (`seq == last_persisted_seq + 1`) and the silent-dedup rule for the stale case (`seq <= last_persisted_seq`) are unchanged.

#### Scenario: Gap resynchronises the entity instead of wedging it
- **GIVEN** an Entity with `last_persisted_seq = 100`
- **WHEN** the Controller receives a Change with `seq = 137` (a gap)
- **THEN** the Controller SHALL emit `[:flezha, :state, :seq_gap]` with `expected: 101, got: 137`
- **AND** the Controller SHALL ship `RestoreInitState` for the Entity and await `RestoreInitStateResponse`
- **AND** after the handshake the Entity SHALL accept subsequent Changes and its WAL SHALL advance past `last_persisted_seq`
- **AND** the Entity SHALL NOT remain in a permanent gap-rejection state

#### Scenario: Contiguous and stale frames are unaffected
- **WHEN** a Change arrives with `seq == last_persisted_seq + 1`
- **THEN** it SHALL be appended as today (no resync, no telemetry)
- **AND WHEN** a Change arrives with `seq <= last_persisted_seq`
- **THEN** it SHALL be silently deduped with a `dedup_drops_total` increment as today

### Requirement: Per-Entity output-staleness watchdog is emit-only

The Controller SHALL track, per persistence-enabled Entity, the wall-clock time at which `last_persisted_seq` last advanced. A periodic sweep SHALL run on a configurable interval and, for any Entity whose data stream is attached (the SDK is live and applying inputs) but whose `last_persisted_seq` has not advanced within `output_stall_timeout_ms` (configurable per Operative class; default 60_000), SHALL emit `[:flezha, :state, :output_stall]` telemetry (measurements `%{stalled_ms: integer(), last_seq: integer()}`, Entity metadata).

The sweep SHALL do nothing else. It SHALL NOT invoke `Flezha.State.Recovery.resync/2` or any other remedy, and SHALL keep no dedup bookkeeping of its own: the remedy is the convergence Loop's output-stall ladder (`resync_runner`, escalating to `rebind_operative`, under one per-target backoff — see `convergence-self-healing`). A sensor that also heals is a second, uncoordinated actor for one signal.

This bounds the duration of any silent persistence/output freeze to at most one `output_stall_timeout_ms` window plus one convergence tick. Because WAL persistence and output-plugin delivery share the per-Entity outbox, recovering WAL advance SHALL also resume output (e.g. Kafka production) for that Entity.

Each completed sweep SHALL stamp its own freshness into `Flezha.Convergence.Snapshot`, so a sweep that wedges without crashing is visible on `/healthz` rather than silently disabling detection.

#### Scenario: A live entity whose WAL stops advancing is detected and recovered
- **GIVEN** a persistence-enabled Entity whose data stream is attached and whose SDK continues to apply inputs (in-memory state advancing)
- **WHEN** `last_persisted_seq` does not advance for longer than `output_stall_timeout_ms`
- **THEN** the staleness sweep SHALL emit `[:flezha, :state, :output_stall]` with the stalled duration and last seq
- **AND** the sweep SHALL trigger no recovery of its own
- **AND** the convergence Loop SHALL apply the output-stall ladder, after which the Entity's WAL SHALL advance and its outbox SHALL drain

#### Scenario: An idle entity is not falsely flagged
- **GIVEN** a persistence-enabled Entity whose SDK is applying no inputs (genuinely idle, no Changes to persist)
- **WHEN** the staleness sweep runs
- **THEN** the Entity SHALL NOT be flagged as stalled and SHALL NOT be resynced (no Change is expected, so a non-advancing `last_persisted_seq` is correct)

#### Scenario: A wedged sweep is visible within one healthz poll
- **GIVEN** the staleness sweep stops completing passes without the process crashing
- **WHEN** `/healthz` is queried once the stamp exceeds 3× the sweep interval
- **THEN** it SHALL report `200 degraded` with `reason: sensor_stale` naming the watchdog

### Requirement: CompactEntity resolves a workflow Entity

The SDK SHALL resolve the Entity a `CompactEntity` addresses in the same two
places a `RestoreInitState` looks: the stream instance registry and a workflow
instance's stage slots. A workflow Entity SHALL therefore be compactable by the
iteration-boundary snapshot and by the Watchdog's size/age triggers exactly
like a stream Entity.

#### Scenario: A looped workflow Entity gets its boundary snapshot
- **GIVEN** a durable looping workflow instance whose procedure holds Entity state
- **WHEN** an iteration commits and the Controller ships `CompactEntity` for that Entity
- **THEN** the SDK SHALL answer with the Entity's `entity_state_snapshot` (not "no live entity matches the addressed entity_id") and the Controller SHALL write the snapshot and truncate the WAL behind it

### Requirement: A stopped operative's WAL writers are closed

When an operative is stopped, the Controller SHALL close every live `WalWriter`
of its class — whether its state directory is deleted or retained for the
finished-job window. The files decide inspectability; the processes go with
the operative.

#### Scenario: A finished workflow retains its files, not its writers
- **WHEN** a durable workflow completes and its state is retained for `workflow_completed_retention_hours`
- **THEN** its WAL and journal files SHALL stay on disk and no `WalWriter` process SHALL remain registered for its entities

#### Scenario: A deleted state directory has no writer under it
- **WHEN** an operative is stopped and its state is deleted
- **THEN** its writers SHALL be closed before the files are removed

### Requirement: Compaction handshake is bounded and cannot wedge persistence

When the Controller ships `CompactEntity{entity_id}` to the SDK, it SHALL bound the wait for the corresponding `entity_state_snapshot` response with a timeout. If the snapshot does not arrive within the bound, the Controller SHALL emit `[:flezha, :state, :compaction_timeout]` telemetry, SHALL NOT leave the Entity's WAL append blocked, and SHALL recover via resync. Compaction SHALL be retriable on a later trigger; a single failed/slow compaction SHALL NOT permanently stop WAL growth or output for the Entity.

The recovery step SHALL actually execute: a recovery invocation that cannot dispatch — for example because it lacks the `runner_id` needed to address the runner — SHALL NOT be silently discarded by its caller. Retries SHALL be bounded per the strike-and-give-up requirement above rather than repeating at the sweep interval without limit.

#### Scenario: A stalled compaction times out instead of freezing the WAL
- **GIVEN** an Entity for which the Controller has shipped `CompactEntity`
- **WHEN** no `entity_state_snapshot` response arrives within the compaction timeout
- **THEN** the Controller SHALL emit `[:flezha, :state, :compaction_timeout]`
- **AND** WAL append for the Entity SHALL NOT remain blocked (no "large WAL, no snapshot, frozen" state)
- **AND** persistence SHALL recover via resync and compaction SHALL be eligible to retry later

#### Scenario: The recovery step is not a silent no-op
- **GIVEN** a compaction timeout for an Entity whose owning runner is known to the Controller
- **WHEN** the timeout handler runs
- **THEN** the recovery it invokes SHALL either dispatch, or record its inability to dispatch
- **AND** the handler SHALL NOT discard the recovery result

### Requirement: State path resolution SHALL be single-source and coherent

The on-disk location of an Entity's WAL/snapshot/outbox SHALL be computed from a single
authoritative resolver that every reader, writer, and the boot-time migrator agree on, so
that no path divergence can orphan persisted state. If `FLEZHA_STATE_DIR` is set, the
resolver and the migrator SHALL use the same value, and a change of root between boots
SHALL be reconciled — existing state migrated from the old root to the new one — rather
than silently orphaned. The boot migrator SHALL NOT relocate state into a layout (e.g. a
`_system/` namespace prefix) that the reader does not read; layout changes ahead of reader
support are prohibited because they orphan state.

#### Scenario: WAL is not orphaned by a path-resolution change
- **GIVEN** persisted WAL/snapshot files for running Entities
- **WHEN** the Controller restarts after this change
- **THEN** the restore path SHALL find the existing files at the resolver's path
- **AND** no Entity's state SHALL become invisible because the migrator and reader disagree on the path

#### Scenario: Migrator and reader agree on layout
- **WHEN** the boot migrator runs
- **THEN** the directory layout it produces SHALL be exactly the layout `Flezha.State.Paths` reads
- **AND** any layout the reader does not support SHALL NOT be produced by the migrator

### Requirement: Per-Entity WAL SHALL realign to a restarted runner's seq base

The Controller's per-Entity WAL SHALL adopt a restarted runner's sequence base as the
live source of truth rather than freezing. When a runner process restarts and reconnects
under a new runner id, it resumes emitting Changes from a fresh sequence base derived from
its post-restart rebuild (e.g. a Kafka replay), which may diverge from the persisted
`last_persisted_seq`. The WAL SHALL realign its baseline — discarding the superseded
pre-restart WAL/snapshots so a later `RestoreInitState` ships only the new epoch — instead
of dedup-dropping the new runner's Changes as stale. A Change at or below
`last_persisted_seq` from the *same* runner SHALL remain a legitimate dedup. A realignment
SHALL emit `[:flezha, :state, :wal_realign]` telemetry.

A realign SHALL be honoured only when the divergent Changes arrive via the Entity's
current owner binding — the session the takeover logic most recently bound to the
Entity's write path. Changes forwarded by a superseded session SHALL be refused at
the binding (the superseded session's channels are unbound during takeover, before the
new session is bound) and SHALL NOT trigger a realign; the refusal SHALL NOT add any
per-record check to the owning session's append path.

#### Scenario: A restarted runner's diverged seq base realigns the WAL
- **GIVEN** an Entity whose WAL has advanced to seq N under runner A
- **WHEN** runner A restarts and a new runner B, bound as the Entity's owner by takeover, resumes emitting Changes from a seq base diverged from N
- **THEN** the WAL SHALL realign to runner B's seq base and resume advancing rather than freeze at N
- **AND** a `[:flezha, :state, :wal_realign]` event SHALL be emitted

#### Scenario: A same-runner duplicate is still deduped
- **GIVEN** an Entity whose WAL has advanced to seq N under runner A
- **WHEN** runner A re-sends a Change at or below seq N (a retransmit, not a restart)
- **THEN** the Change SHALL be deduped and the WAL SHALL NOT realign

#### Scenario: An operator-triggered resync resolves the owning runner
- **WHEN** a caller with no runner id of its own triggers a resync for an Entity (`flz state rotate`)
- **THEN** `Flezha.State.Recovery.resolve_runner/1` SHALL resolve the owning runner id from the durable registry entry's metadata tag, keyed by the operative's operative_class (its `:operative_class` opt, defaulting to its id)
- **AND** an output stall SHALL NOT be such a caller: the Watchdog emits only, and the convergence Loop's output-stall ladder owns that remedy (see `convergence-self-healing`)

#### Scenario: A superseded session's divergent frames do not realign
- **GIVEN** an Entity whose stable identity was taken over by a new session while the old session's frames are still in flight
- **WHEN** the superseded session's channel forwards a Change with a divergent seq base
- **THEN** the frame SHALL be refused at the binding, the WAL SHALL NOT realign, and a `[:flezha, :state, :stale_writer_refused]` event SHALL be emitted once for the episode

### Requirement: Output is persisted before it is emitted (flush-before-emit)

For a persistence-enabled operative, the SDK SHALL drain a just-applied state change to the durable WAL send path BEFORE writing the corresponding output frame, so durable state never lags emitted output by more than the in-flight network window. This is gated on `persistence_enabled` and `FLEZHA_FLUSH_BEFORE_EMIT` (default on); non-persistent operatives keep the zero-cost inline emit. Each change is moved out of the buffer exactly once and seq order is preserved via the shared `change_seq`.

The guarantee is **per dispatch**, and it SHALL be stated that way wherever it is
relied on. A dispatch's records and the outputs that dispatch produced have two
different writers — the Entity ships its own records to the state stream and
hands its outputs to the Operative, which ships them — so an output is one
process-hop behind its record. Across dispatches the SDK SHALL NOT be taken to
promise any interleaving of the two: a later dispatch's records MAY reach the
stream ahead of an earlier dispatch's output, and this SHALL NOT be read as an
output overtaking a record. A reader — a test, an operator, or a controller-side
consumer — that asserts a fixed alternation of records and outputs across
dispatches is asserting a coincidence of scheduling, not this requirement.

#### Scenario: durable state does not trail emitted output
- **GIVEN** a persistence-enabled operative applies a state change and then emits output for it
- **WHEN** the dispatch runs with flush-before-emit enabled
- **THEN** the change is shipped to the per-Entity WAL path before the output frame is written
- **AND** on a subsequent hard-kill the restored state is no further behind the emitted output than the single in-flight change.

#### Scenario: a non-persistent operative keeps the inline fast path
- **GIVEN** an operative with persistence disabled
- **WHEN** it emits output
- **THEN** no flush-before-emit drain occurs and the emit path is unchanged.

#### Scenario: An output never leaves ahead of the change that produced it
- **GIVEN** a persistence-enabled operative whose Procedure applies a change and emits on every dispatch
- **WHEN** many dispatches run, whether their inputs arrive in a burst or one at a time
- **THEN** for every output, the change record of the dispatch that produced it SHALL already be on the state stream, so a crash in the gap can never leave an output in the world with no record of what produced it
- **AND** the output SHALL carry the seq of that record, so which change produced it is readable rather than inferred from arrival order

#### Scenario: A later dispatch's record may overtake an earlier dispatch's output
- **GIVEN** an Entity that completes a second dispatch while the Operative is still writing the first dispatch's outputs
- **WHEN** both dispatches' frames arrive on the state stream
- **THEN** the second dispatch's records MAY precede the first dispatch's output, and this SHALL NOT be treated as a violation, a loss, or a reordering of any dispatch

### Requirement: Per-Entity state is deleted on dynamic-mode eviction

When the SDK evicts an Entity due to dynamic-mode idle-TTL eviction (see `operative-procedure-entity-model` "Per-Procedure idle-TTL eviction is opt-in"), eviction SHALL be destructive. If persistence is enabled for the Operative class, the SDK SHALL ship a fire-and-forget `delete_entity_state{entity_id}` frame so the agent removes the per-Entity WAL, snapshot, and outbox files (`{key}.wal`, `{key}.snap.{seq}`, `{key}.outbox.dedup` under `app_data/state/{operative}/{procedure}/`). A subsequent input for the same key SHALL cold-start a fresh Entity (new WAL from seq 0); the SDK SHALL NOT restore the evicted Entity's state from disk.

#### Scenario: Eviction deletes on-disk state
- **WHEN** a persistence-enabled dynamic-mode Entity is evicted
- **THEN** the SDK SHALL ship `delete_entity_state{entity_id}` and the agent SHALL remove the WAL, snapshot, and outbox files for that entity_id

#### Scenario: Recreate after eviction cold-starts
- **WHEN** a dynamic-mode Entity is evicted and a subsequent input arrives for the same key
- **THEN** the SDK SHALL recreate the Entity via `init_state(Some(key), None)` and a new WAL SHALL start from seq 0; no restore-from-disk SHALL occur

### Requirement: Per-Entity state delete RPC

The agent SHALL accept a `delete_entity_state` data-stream frame whose body is the structured `entity_id`. On receipt the agent SHALL: (1) stop any per-Entity `WalWriter` for that entity_id and drop its pending `FsyncBatcher` dirty mark; (2) remove the `{key}.wal`, `{key}.snap.{seq}` (all sequences), and `{key}.outbox.dedup` files. Delete SHALL be idempotent: a delete for an entity_id with no files SHALL succeed.

The delete is reachable via two paths:

- The SDK reaper, for evicted persistence-enabled Entities (fire-and-forget; the agent writes no response frame back on the data stream).
- The CLI command `flz state delete <operative/procedure/key>`, which calls `Flezha.State.Supervisor.delete_entity/1` directly and returns an `{entity_id, status}` confirmation synchronously.

#### Scenario: Delete removes existing files
- **WHEN** the agent receives `delete_entity_state` for an entity_id that has WAL and snapshot files on disk
- **THEN** the agent SHALL stop the per-Entity processes and remove all three file types

#### Scenario: Delete is idempotent for nonexistent entity
- **WHEN** the agent receives `delete_entity_state` for an entity_id with no on-disk state
- **THEN** the agent SHALL succeed without error (the delete is a no-op)

#### Scenario: CLI delete returns confirmation
- **WHEN** an operator runs `flz state delete <operative/procedure/key>` against an Entity with on-disk state
- **THEN** `Flezha.State.Supervisor.delete_entity/1` SHALL remove the files and return `{:ok, %{entity_id, status: :ok}}`, and the CLI SHALL exit 0

### Requirement: Atomic effect record type in the per-Entity WAL

The per-Entity WAL SHALL support an additional record type carrying an
invocation's state change(s), its outbox entries (channel, key, user metadata,
payload, emit ordinal), and its consumed input offset as one crc-protected record
appended and fsynced as a unit. Existing change-record and snapshot requirements
are unchanged; at-least-once operatives never produce effect records. Restore and
time-travel readers SHALL fold the state-change portion of effect records exactly
as they fold plain change records.

#### Scenario: Effect record replays as state

- **WHEN** an Entity with effect records in its WAL is restored via the restore
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

The per-Entity dedup file SHALL carry both the existing `last_persisted_seq`
watermark and the new `outbox_flushed_seq` cursor, updated with the same
atomic tmp+rename discipline. On restart, a missing or stale cursor SHALL be
treated as "re-drive from the last known value" (never as "skip ahead").

#### Scenario: Stale cursor re-drives, never skips

- **WHEN** the dedup file's `outbox_flushed_seq` is older than the newest durable
  effect record after a crash
- **THEN** the controller re-drives all effect-record outputs above the cursor
  and never advances the cursor past undelivered entries

### Requirement: The restore contract SHALL state the fate of queued input

The restore contract SHALL state what happens to an Entity's queued input, and
SHALL make the answer depend on whether the Controller will re-forward it.

The `RestoreInitState` requirement describes how an Entity's state is rebuilt
but says nothing about the input a live Entity already holds queued when the
restore lands. The SDKs consequently disagree — the Rust SDK discards it, the
Java SDK preserves it — with neither behaviour documented as intended.

The Controller re-forwards input only for classes whose inputs it journals
before forwarding: the effectively-once tier, whose inputs land in the input
log, and durable workflow instances, whose inputs land in the proc inbox.
`Ingest.journal_input/2` passes every other operative through un-journaled,
including a plain stateful stream operative — the default configuration,
since persistence defaults to enabled and the tier is an explicit opt-in.

A re-forward SHALL NOT be issued onto a data stream that has not yet bound. The
Controller re-forwards after a rebind, and a fresh runner binds its primary data
stream after the control-channel handshake completes; a re-forward issued in
that window is dropped and never re-driven, which loses exactly the input the
restore exists to preserve. The re-forward SHALL therefore be gated on the bind,
using the same mechanism that delivers a journaled input accepted before an
operative's first bind — one re-forward path, not two.

#### Scenario: The contract distinguishes journaled from un-journaled classes

- **WHEN** the restore contract is consulted for the fate of queued input
- **THEN** it SHALL give different answers for a class whose inputs are
  journaled and a class whose inputs are not

#### Scenario: Un-journaled queued input survives a restore

- **GIVEN** a live Entity of a class whose inputs the Controller does not
  journal, holding queued input
- **WHEN** the Controller drives a `RestoreInitState` handshake for that Entity
- **THEN** the queued input SHALL NOT be silently lost

#### Scenario: A restore-time discard is observable to an operator

- **WHEN** a restore discards queued input
- **THEN** the discard SHALL be counted and exposed, naming the Entity, rather
  than leaving no trace

#### Scenario: A re-forward onto an unbound stream is not lost

- **GIVEN** a rebind whose fresh runner has completed the control-channel
  handshake but has not yet bound its primary data stream
- **WHEN** the Controller re-forwards an Entity's journaled offsets
- **THEN** those offsets SHALL reach the procedure once the data stream binds,
  and SHALL NOT be dropped

#### Scenario: The restore and first-bind re-forwards share one path

- **WHEN** the re-forward issued after a restore and the re-forward issued at an
  operative's first data-stream bind are compared
- **THEN** they SHALL be the same code path, differing only in what triggers them

### Requirement: The compaction handshake SHALL be verified end to end against a real SDK

The existing compaction requirements describe an SDK obligation that no SDK has ever implemented, and the test suite has never involved a real SDK on this path — controller-side tests stub `:state_resync_fun` or `ControlChannel.send_control` and assert on telemetry alone. The system SHALL carry end-to-end coverage, on each SDK arm, that fails when the SDK does not produce a snapshot.

The coverage SHALL assert against observable durable state — a snapshot file existing and the WAL shrinking — and SHALL NOT be satisfied by telemetry assertions alone.

#### Scenario: A triggered compaction produces a durable snapshot and truncates the WAL

- **GIVEN** a live runner hosting an Entity whose Procedure has a non-`NoChange` Change type, with persistence enabled and a WAL containing records
- **WHEN** the Controller ships `CompactEntity` for that Entity and the SDK answers
- **THEN** a snapshot file SHALL exist for that Entity under its state directory
- **AND** the Entity's WAL size SHALL be strictly smaller than it was before the handshake
- **AND** `[:flezha, :state, :compaction_completed]` SHALL have been observed for that Entity

#### Scenario: A stub SDK response fails the coverage

- **GIVEN** an SDK build whose `CompactEntity` handler answers `snapshot_seq = 0` without shipping an `entity_state_snapshot` frame
- **WHEN** the end-to-end compaction coverage runs against it
- **THEN** the coverage SHALL fail

#### Scenario: The cold-controller baseline-seed pull becomes reachable

- **GIVEN** the archived `snapshot-baseline-durability` change, whose cold-controller "pull a full-state baseline via `CompactEntity`" path was inert because no SDK ever answered the handshake with a snapshot
- **WHEN** a cold controller with an empty WAL for a live runner's Entity issues `CompactEntity` (via `Recovery.dispatch_resync`'s empty-WAL branch)
- **THEN** the SDK SHALL now answer with a real `entity_state_snapshot`, so the baseline-seed scenario that change asserts becomes reachable for the first time
- **AND** this change's requirements — not an amendment to the archived change — are the current authority for that handshake (the archived change is left as the historical record)

### Requirement: The Controller SHALL inspect CompactEntityResponse

`Flezha.State.Commands.dispatch_compact_entity/2` currently treats a successful `send_control` as compaction having started and never reads the response body. The Controller SHALL inspect `CompactEntityResponse` and SHALL treat a response that reports no snapshot for an Entity with a non-empty WAL as a failure of the handshake, not as its completion.

#### Scenario: Zero snapshot_seq against a non-empty WAL is an error

- **GIVEN** an Entity whose WAL contains at least one record
- **WHEN** the SDK answers `CompactEntityResponse{snapshot_seq: 0, snapshot_bytes_size: 0}`
- **THEN** the Controller SHALL record the handshake as failed, SHALL emit failure telemetry naming the Entity, and SHALL NOT mark the compaction as started or completed

#### Scenario: Zero snapshot_seq against an empty WAL is not an error

- **GIVEN** an Entity whose WAL contains no records
- **WHEN** the SDK answers `CompactEntityResponse{snapshot_seq: 0}`
- **THEN** the Controller SHALL NOT treat the response as a failure

### Requirement: A failing compaction SHALL be bounded by strikes and give up loudly

`Watchdog.flag_compaction_timeout/2` currently calls `Recovery.resync/2` without a `runner_id`, so `do_resync/2` returns `{:error, :no_runner_id}` and the caller discards it — the timeout handler is a silent no-op, and the Entity is dropped from the in-flight map so the next sweep re-triggers it indefinitely. Repeated compaction failure for an Entity SHALL be counted, SHALL back off between attempts, and SHALL escalate to a loud, operator-visible give-up rather than retrying at the sweep interval forever.

#### Scenario: Repeated failures back off rather than retrying at sweep cadence

- **GIVEN** an Entity whose compaction handshake fails or times out
- **WHEN** the failure repeats
- **THEN** the interval between successive `CompactEntity` dispatches for that Entity SHALL increase
- **AND** the Controller SHALL NOT dispatch `CompactEntity` for that Entity once per sweep indefinitely

#### Scenario: Give-up is observable

- **GIVEN** an Entity whose compaction has failed past the strike threshold
- **WHEN** the threshold is crossed
- **THEN** the Controller SHALL emit give-up telemetry naming the Entity and the failure count, at a severity an operator can alert on

#### Scenario: A recovery attempt that cannot dispatch is not silently discarded

- **WHEN** a compaction failure path invokes recovery without the information needed to dispatch it
- **THEN** the outcome SHALL be logged or counted; it SHALL NOT be discarded

### Requirement: The per-Procedure compact_after_bytes threshold SHALL be reachable

`PersistenceResolver.compact_after_bytes/2` names its first parameter `_procedure_id` and discards it, so the per-Procedure threshold the existing compaction requirement mandates cannot be resolved by any caller — the global default always wins. The resolver SHALL consult the per-Procedure value, and every caller SHALL pass the arguments needed to resolve it.

#### Scenario: A declared per-Procedure threshold governs its own Entities

- **GIVEN** a Procedure that declares a `compact_after_bytes` different from the global default, shipped in the `AddOperative` metadata
- **WHEN** the Watchdog evaluates the size trigger for an Entity of that Procedure
- **THEN** the declared value SHALL be used, not the global default

#### Scenario: An undeclared Procedure falls back to the global default

- **GIVEN** a Procedure that declares no `compact_after_bytes`
- **WHEN** the Watchdog evaluates the size trigger
- **THEN** the global `:default_compact_after_bytes` SHALL be used

### Requirement: The persistence contract SHALL have a runnable smoke check

`test/scripts/state_persistence.sh` SHALL run to completion against the dev
compose stack and SHALL assert the data-plane half of this capability: with
an operative running, plugins attached and inputs flowing, WAL files appear
under the namespaced state layout and `flz state list <class>` surfaces the
live entity.

The script SHALL drive the plugin-attach API that currently exists rather
than a removed one, and SHALL start from a clean state on every run, so a
green result is evidence about the contract and not about leftovers from a
previous run.

Where the script cannot run in CI, its manual status and its measured
runtime SHALL be recorded next to it, so a later reader can tell "nobody
ran it" from "it passes".

#### Scenario: The smoke check passes from a clean state

- **WHEN** the script is run twice in succession, each run starting from wiped volumes
- **THEN** both runs complete successfully and assert the WAL layout and the live-entity listing

#### Scenario: A removed API is not driven

- **WHEN** the script attaches a plugin to an operative
- **THEN** it passes only options the provider currently accepts, and the attach succeeds

#### Scenario: Manual status is recorded

- **WHEN** the script is not wired into an automated tier
- **THEN** its manual status and measured runtime are recorded next to the script and in the docs that reference it

### Requirement: A refused restore fails the bind instead of cold-starting the Entity

`RestoreInitState` is an awaited control round trip, and the controller SHALL act on its answer. When the SDK reports a restore failure — a language mismatch, a schema mismatch, a CRC failure, a truncated change log, or an apply error — the controller SHALL NOT proceed as though the Entity had no state. It SHALL leave the operative unbound, SHALL log and emit telemetry naming the entity, the runner and the reported reason, and SHALL surface it to the convergence loop so the operative is visible as unhealthy rather than silently serving from empty state.

A restore that reports no state to restore (an Entity with an empty WAL and no snapshot) is a success, not a failure, and SHALL bind normally.

A round trip that never completed — a timeout, a control channel that is gone, a runner shutting down — is NOT a refusal: the SDK reported nothing, so there is no verdict on the state, and the controller SHALL leave that case to the rebind path that already owns a runner which stopped answering. Only a failure the SDK itself reports parks the operative.

The controller SHALL NOT discard the result of an awaited restore at any call site.

A torn tail the reader repairs under the proof in "A durable record that fails its crc32 SHALL be refused, and a truncation SHALL be proved safe before it happens" is a repair, not a restore refusal: the append protocol's own crash leaves no complete record behind the failure point, so the Entity binds on the records that were fully written. A crc failure with complete records behind it is the refusal this requirement governs, and the controller's own reader SHALL produce that condition rather than deleting the record before the SDK can report it.

#### Scenario: A refused restore does not cold-start

- **GIVEN** an Entity with a populated WAL whose records the bound SDK refuses
- **WHEN** the controller awaits `RestoreInitState` and receives the refusal
- **THEN** the operative SHALL NOT be bound, the failure SHALL be logged with the reported reason, and the Entity SHALL NOT begin folding new inputs on top of empty state

#### Scenario: A genuinely empty Entity still binds

- **GIVEN** an Entity with no snapshot and a zero-length WAL
- **WHEN** the controller awaits `RestoreInitState`
- **THEN** the SDK SHALL answer successfully with a terminal seq of zero and the operative SHALL bind and serve

#### Scenario: An unanswered restore is not a refusal
- **GIVEN** an Entity whose runner's control channel dies before the restore is answered
- **WHEN** the controller awaits `RestoreInitState` and the round trip fails without an SDK reply
- **THEN** the operative SHALL NOT be parked for a restore refusal, because nothing was reported about its state; the runner's disappearance is handled by the rebind path

#### Scenario: A corrupt WAL is loud, not silent

- **GIVEN** an Entity whose WAL holds a record that fails its record framing CRC with complete records behind it
- **WHEN** a restore is attempted
- **THEN** the operative SHALL remain unbound with a reason naming the CRC failure, rather than binding on partial state, and the corrupt record and every record behind it SHALL still be on disk

### Requirement: A durable record that fails its crc32 SHALL be refused, and a truncation SHALL be proved safe before it happens

A durable record whose framing crc32 does not verify SHALL be refused by name, and the refusal SHALL carry the file it was read from, the last seq that verified before it and the byte offset of the failure.

The reader already distinguishes a crc failure from a read that runs off the end of the file and from a length field below the header size. That distinction SHALL be carried to the caller. A reader SHALL NOT collapse the three into one error, and SHALL NOT answer a caller with the records it managed to read before a failure as though the log had ended there.

A reader SHALL truncate a record log only where it can prove that no complete record can begin after the failure point. A crc failure whose framed extent ends before the end of the file has acknowledged records behind it and SHALL NOT be truncated: records the Controller acknowledged as durable SHALL NOT be deleted to make a log parse. Where the proof holds, the truncation is the torn tail the append protocol produces and SHALL still happen — and SHALL be reported rather than performed silently.

Every read of a record log SHALL apply this rule, whichever entry point reaches it: opening a log for appends, folding one for its records, locating a retention or rewind boundary in one, and assembling a restore payload from one.

A refusal SHALL be observable without attaching a tracer: it SHALL emit one telemetry event and one log line at error severity. A durable-record loss reported to nobody is the defect this requirement exists to prevent.

A refusal SHALL leave the bytes on disk untouched and SHALL leave the operative unbound rather than serving from the records that happened to precede the failure, which is the fate "A refused restore fails the bind instead of cold-starting the Entity" already assigns to a CRC failure the SDK reports.

#### Scenario: A corrupt record mid-log does not delete the records after it

- **GIVEN** a WAL holding three records where the second record's body has one flipped bit
- **WHEN** the writer opens the log
- **THEN** the open SHALL refuse and name the seq and offset, the third record SHALL still be present on disk, and the file's byte length SHALL be unchanged

#### Scenario: A torn tail is still repaired, and now says so

- **GIVEN** a WAL whose last record is a short write from a crash mid-append
- **WHEN** the writer opens the log
- **THEN** the partial tail SHALL be truncated, the preceding records SHALL be intact, and the repair SHALL be reported with the byte count discarded

#### Scenario: A restore payload is refused rather than shipped short

- **GIVEN** an Entity whose WAL holds a corrupt record below its terminal seq
- **WHEN** the controller assembles `RestoreInitState` for that Entity
- **THEN** it SHALL refuse rather than ship the records before the corrupt one as the whole change log, and the operative SHALL NOT bind on that truncated history

#### Scenario: A rewind boundary is not located past a corrupt record

- **GIVEN** an Entity whose WAL holds a corrupt record below the seq an epoch realign rewinds to
- **WHEN** the rewind locates its boundary
- **THEN** it SHALL refuse rather than cut at the corrupt record's offset and delete the valid records between it and the intended boundary

#### Scenario: A refusal reaches an operator surface

- **WHEN** any durable read refuses a record for a crc failure
- **THEN** one telemetry event and one error log line SHALL name the file, the seq and the offset, and the refusal SHALL be countable without a tracer

#### Scenario: Every runtime answers a corrupt record the same way

- **WHEN** the Controller's reader, the Elixir SDK's restore fold and the kernel's change-log fold are each given the same corrupt record
- **THEN** all three SHALL refuse by name, and none SHALL return a partial result as success

