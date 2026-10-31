# relay-state-persistence Specification

## Purpose

The relay-state-persistence capability provides agent-side, per-Cell durability for Relay state. The agent maintains an append-only write-ahead log (WAL) per Cell under `storage/app/state/`, fsynced on a configurable cadence, with asynchronous size-triggered compaction into snapshot files. On Cell reconnect, the agent assembles a `RestoreInitState{cell_id, snapshot_bytes, change_log_bytes}` payload and ships it to the SDK over the control channel; the SDK applies the snapshot and folds the WAL tail to recover terminal state. Persistence is per-Relay-class opt-in via runtime config or a Relay-class const, defaulting on. The agent retains pre-compaction WAL records for a configurable window to support time-travel queries via `flz state at`. The capability is additive to the existing `Overwatch` GenServer: persistence frames (`cell_state_change`, `cell_state_snapshot`) flow alongside the existing protocol; no SDK or runner code outside this capability is required.
## Requirements
### Requirement: Per-Cell append-only WAL on the agent

Each Cell whose Relay has `keeps_state = true` and whose Tasklet's `Change` is not `NoChange` SHALL have a per-Cell write-ahead log on the Controller at `storage/app/state/{relay_class}/{tasklet_id}/{cell_key}.wal`. The on-disk path SHALL NOT include `runner_id` as a path component. Records SHALL be `[seq: u64 BE][len: u32 BE][header: 16B][change_bytes][crc32: u32 BE]` where `len = 16 + byte_size(change_bytes)` and `crc32 = :erlang.crc32(<<header::binary, change_bytes::binary>>)`. The WAL SHALL be written by the Controller's persistence module (`lib/flezha/state/wal_writer.ex`) and SHALL never be written directly by user code or by the SDK.

`seq` SHALL be strictly monotonic per Cell and assigned by the SDK. The Controller SHALL accept any incoming Change frame whose `seq == last_persisted_seq + 1` for that Cell; SHALL silently drop (with a `dedup_drops_total` counter increment) any frame whose `seq <= last_persisted_seq`. For a frame whose `seq > last_persisted_seq + 1` the Controller SHALL distinguish two cases:

- **Populated WAL** (`last_persisted_seq >= 0`): a genuine protocol violation. The Controller SHALL reject the append (returning `{:error, {:gap, expected, got}}`) and the SDK SHALL treat it as a protocol violation and disconnect.
- **Empty WAL** (`last_persisted_seq == -1`) with `seq > 0`: a cold-takeover where this Controller holds no baseline for a Cell the runner already has state for. The Controller SHALL NOT disconnect. It SHALL enter an **awaiting-baseline** state for the Cell, shed the frame (incrementing a `wal_shed_total`-class counter, not erroring), drive a `CompactCell` pull (see "Restart restores state via RestoreInitState handshake"), and keep shedding further gap frames without erroring until a `cell_state_snapshot` establishes the baseline. The pull SHALL be retried on a bounded interval for as long as the Cell remains awaiting-baseline: a pull that fails to dispatch (for example a torn control channel answering `:no_control_channel`) MUST NOT strand the Cell, because no snapshot will arrive to clear the awaiting-baseline state and every subsequent frame would be shed indefinitely. Retries SHALL be rate-limited so a per-frame gap flood cannot fan out into unbounded concurrent pulls.

A one-time on-disk migration `Flezha.Boot.StatePathMigrator.migrate/0` SHALL run at controller boot before `Flezha.State.Supervisor` starts any per-Cell `WalWriter`. The migrator SHALL walk every directory under `storage/app/state/` of the form `{runner_id}/{relay_class}/{tasklet_id}/` and atomically move its contents into `{relay_class}/{tasklet_id}/`. The migration is idempotent (skips silently if the flat layout already exists) and logs each rename. After all old `{runner_id}/` directories are emptied, they are removed.

#### Scenario: WAL path uses flat layout

- **WHEN** a Cell `p1` with tasklet `s1` and cell_key `k1` belonging to Relay class `MarketRelay` is running and persistence is enabled
- **THEN** the Controller SHALL write its WAL to `storage/app/state/MarketRelay/s1/k1.wal`
- **AND** the path SHALL NOT contain any `runner_id` component regardless of which runner currently hosts the Relay

#### Scenario: WAL allocated lazily on first append

- **WHEN** the agent receives the first `cell_state_change` frame for a Cell that does not yet have a WAL file
- **THEN** the persistence module SHALL create the directory `storage/app/state/{relay}/{tasklet}/`, open the WAL file at `{cell_key}.wal` with `[:raw, :read, :write, :binary, :append]`, and start a `WalWriter` GenServer for the Cell

#### Scenario: Strictly monotonic seq accepted in order

- **WHEN** the SDK ships frames with `seq = 1, 2, 3` for a fresh Cell
- **THEN** the agent SHALL append all three records to the WAL in order; the file's `last_persisted_seq` SHALL equal 3 after the third append

#### Scenario: Re-shipped seq is silently deduped

- **WHEN** the SDK ships a frame with `seq = 5` for a Cell whose `last_persisted_seq = 7` (re-ship after SDK crash that lost the in-memory cursor)
- **THEN** the agent SHALL NOT append the duplicate; the agent SHALL increment `dedup_drops_total`; the wire reply SHALL be `:ok` (so the SDK does not retry)

#### Scenario: Out-of-order seq on a populated WAL is rejected

- **GIVEN** a Cell whose `last_persisted_seq = 7` (populated WAL)
- **WHEN** the SDK ships a frame with `seq = 10`
- **THEN** the agent SHALL refuse the append and reply `{:error, {:gap, expected: 8, got: 10}}`; the SDK SHALL treat this as a protocol violation and disconnect

#### Scenario: Gap on an empty WAL seeds a baseline instead of disconnecting

- **GIVEN** a freshly-assigned Controller with no WAL for a Cell (`last_persisted_seq = -1`) whose live runner has been producing changes and next emits `seq = 5001`
- **WHEN** the agent receives the `cell_state_change` frame
- **THEN** the agent SHALL NOT reply with a fatal gap error and SHALL NOT disconnect the SDK; the agent SHALL shed the frame (incrementing the shed counter), mark the Cell awaiting-baseline, and dispatch a `CompactCell` to pull a full-state baseline; further gap frames arriving before the snapshot SHALL be shed without error and SHALL NOT each dispatch their own pull
- **AND** once the pulled `cell_state_snapshot{seq}` lands, `last_persisted_seq` SHALL equal that snapshot's seq and the runner's next new delta SHALL append contiguously

#### Scenario: Crash-safe append via crc

- **WHEN** the agent process crashes mid-record-write, leaving a partial record at the file tail
- **THEN** on next agent boot, `RecordFrame.scan_for_last_valid_seq/1` SHALL detect the partial record — a read that runs off the end of the file, or a framed record whose crc32 fails and after which no complete record can begin — truncate the file to the last valid record, report the repair, and resume normal tasklet; `last_persisted_seq` SHALL equal the seq of the last valid record

#### Scenario: A corrupt record with records behind it is not a torn tail

- **GIVEN** a WAL of three records whose second record's body has one flipped bit, so its framing is intact and the third record still decodes after it
- **WHEN** the agent opens that WAL
- **THEN** the open SHALL NOT truncate, the third record SHALL still be on disk afterwards, and the failure SHALL be reported naming the Cell, the seq and the byte offset

#### Scenario: One-time migration from runner_id-keyed layout

- **GIVEN** a controller running pre-change code wrote files under `storage/app/state/exec-9/MarketRelay/btc/M1.wal` and `storage/app/state/exec-12/PriceRelay/eth/M5.wal`
- **WHEN** the controller is upgraded and `Flezha.Boot.StatePathMigrator.migrate/0` runs at boot
- **THEN** the files SHALL be moved to `storage/app/state/MarketRelay/btc/M1.wal` and `storage/app/state/PriceRelay/eth/M5.wal`
- **AND** the now-empty `exec-9/` and `exec-12/` directories SHALL be removed
- **AND** if the migrator runs again on a subsequent boot, it SHALL skip silently because the flat layout already exists

#### Scenario: Migration handles runner_id rotation across restarts

- **GIVEN** a runner restart between WAL writes — first write under `exec-9`, restart yields `exec-12` for the same Relay
- **WHEN** the migrator runs (one-time, on first boot under new code)
- **THEN** files from BOTH `exec-9/MarketRelay/btc/` and `exec-12/MarketRelay/btc/` SHALL be merged into the flat path; on collision (same cell_key.wal in both), the migrator SHALL prefer the file with the higher last-record `seq` (read by scanning) and preserve the other as `<key>.wal.preserved-<exec-9>` for forensics

#### Scenario: A baseline pull that fails to dispatch is retried

- **GIVEN** a Cell marked awaiting-baseline whose `CompactCell` pull failed to dispatch (the control channel answered `:no_control_channel`)
- **WHEN** further gap frames continue to arrive for that Cell
- **THEN** the agent SHALL retry the pull once the rate-limit interval has elapsed rather than shedding indefinitely without ever retrying
- **AND** the retries SHALL be bounded to at most one in-flight pull per Cell per interval, so a sustained gap flood does not fan out

### Requirement: Snapshot lifecycle with atomic rename and WAL truncation

When the agent receives a `cell_state_snapshot` frame from the SDK (typically in response to a `CompactCell` command), the agent SHALL: (1) write the snapshot bytes to `{cell_key}.snap.{seq}.tmp` with `[:raw, :write, :binary]`; (2) `:file.sync/1` the temp file; (3) atomically rename to `{cell_key}.snap.{seq}` via `:file.rename/2`; (4) call `WalWriter.truncate_up_to(seq)` which drops the records with `seq' <= seq` and atomically renames the rewritten file into place. The cut SHALL cost the retained tail, not the WAL: the writer keeps a sparse seq-to-position index of its records, finds the boundary within one sample span and copies the tail as bytes, because the cut runs inside the writer's loop. A cut with no record at or below `seq` — a retention tick on a writer compaction already cut — SHALL answer at once without rewriting, because the rewrite runs inside the writer's loop and, under the effectively-once tier, every output rides that loop; only a writer awaiting its baseline runs such a cut, which is what establishes the baseline.

If the agent crashes between steps 3 and 4, the next boot SHALL find both the new snapshot and the un-truncated WAL; `lib/flezha/state/registry.ex::scan_dir/1` SHALL detect the situation, prefer the latest snapshot, and mark the WAL records ≤ `snapshot.seq` as redundant. Replay-from-this-state is correct: applying the snapshot then folding the WAL produces a state where the redundant records' apply_change calls are no-ops (because the snapshot already encodes their effects).

#### Scenario: Snapshot rename atomic

- **WHEN** the agent receives a `cell_state_snapshot` frame
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

### Requirement: Asynchronous compaction triggered by per-Cell WAL size

Compaction SHALL be triggered by **WAL size or age**. When a Cell's WAL exceeds `compact_after_bytes` (per-Tasklet, **default 8 MiB**; the SDK ships the per-Tasklet value to the agent in the `AddRelay` metadata, and the SDK's own advertised default SHALL be 8 MiB so it does not override the global `:flezha, :default_compact_after_bytes`), OR the Cell's oldest un-snapshotted WAL record is older than `compact_after_age_ms` (global runtime knob, default `300_000`; `0` disables the age trigger), the agent SHALL trigger asynchronous compaction by enqueuing `Overwatch.handle_continue(:compact_cell, cell_id)`. The continuation SHALL ship `CompactCell{cell_id}` to the SDK over the control channel. The age trigger SHALL be evaluated by the existing `Flezha.State.Watchdog` periodic sweep, reusing the same continuation as the size trigger.

The SDK SHALL acquire the Cell's sticky CAS, clone `Self::State`, release the CAS, serialize the clone (postcard), and ship `cell_state_snapshot{cell_id, seq, header, snapshot_bytes, crc32}` back to the agent. The agent SHALL handle the snapshot per the snapshot-lifecycle requirement above. The agent SHALL reply `CompactCellResponse{cell_id, snapshot_seq, snapshot_bytes_size}` to confirm.

The hot path SHALL pay only the brief sticky-CAS hold during the state clone — microseconds, not the seconds-to-minutes a synchronous full-state snapshot would take.

A Cell inside an open checkpoint interval SHALL close it early and ship its record before the snapshot is taken (`effectively-once-checkpoint-granularity`). A Cell whose live state holds the changes of an attempt awaiting retry SHALL refuse the compaction with `state.cell_busy` and ship no snapshot, whoever asked for it. The Watchdog SHALL treat that refusal as retry-later: it SHALL clear the in-flight compaction without a backoff strike, SHALL NOT count it toward the give-up, and SHALL ask again on its next sweep whatever the WAL's size or age.

#### Scenario: Compaction triggered by size threshold

- **WHEN** a Cell's `wal_size_bytes` grows past 8 MiB (the default `compact_after_bytes`)
- **THEN** the agent SHALL enqueue `:compact_cell` for that Cell without blocking new Change ingestion

#### Scenario: Compaction triggered by WAL age for a low-throughput Cell

- **GIVEN** `compact_after_age_ms = 300_000` and a Cell whose WAL has not reached 8 MiB but whose oldest un-snapshotted record is older than 5 minutes
- **WHEN** the `Flezha.State.Watchdog` sweep runs
- **THEN** the agent SHALL enqueue `:compact_cell` for that Cell so a fresh baseline is written even though the size threshold was never reached

#### Scenario: Age trigger disabled

- **GIVEN** `compact_after_age_ms = 0`
- **WHEN** the `Flezha.State.Watchdog` sweep runs over a Cell with an old but small WAL
- **THEN** the agent SHALL NOT enqueue compaction on the basis of age; only the size trigger applies

#### Scenario: Per-Tasklet override propagated and honored

- **WHEN** a Tasklet declares `compact_after_bytes() -> usize { 16 * 1024 * 1024 }` in the SDK
- **THEN** the SDK SHALL ship `compact_after_bytes = 16777216` for this Tasklet in `AddRelay` metadata; the agent SHALL trigger compaction for this Tasklet's Cells when WAL grows past 16 MiB

#### Scenario: Hot path holds CAS only for state clone

- **WHEN** the SDK receives `CompactCell` and clones the state
- **THEN** the SDK SHALL acquire the sticky CAS, clone, release the CAS within the same `Tasklet::run`-equivalent invocation, and serialize the clone in a non-runner context; the cell's input throughput SHALL NOT drop below 90% of pre-compaction throughput during the clone

#### Scenario: A busy refusal is asked again on the next sweep without a strike

- **GIVEN** a `CompactCell` the runner refused with `state.cell_busy`
- **WHEN** the next `Flezha.State.Watchdog` sweep runs
- **THEN** it SHALL ship `CompactCell` for that Cell again with `trigger: :deferred`, and the Cell's `:compaction` backoff row SHALL hold no strike

### Requirement: Restart restores state via RestoreInitState handshake

On runner (re)bind, the controller SHALL recover each persisted Cell's state, choosing the direction by which side holds authoritative history:

- **Controller holds a baseline (a snapshot and/or WAL for the Cell) → push.** The controller SHALL drive a `RestoreInitState` handshake and the SDK SHALL rebuild the live Cell slot from the controller's durable WAL — snapshot apply followed by change-log fold — rather than initialising empty.
- **Controller holds no baseline for the Cell (no snapshot and no WAL) while the runner is live → pull.** The controller SHALL request a `CompactCell` from the runner to obtain a full-state baseline, rather than pushing a `RestoreInitState`. Because a freshly-assigned controller cannot enumerate a live runner's Cells (its own disk is empty), the pull is data-plane-triggered by the first `cell_state_change` frame that arrives with an empty WAL (see "Per-Cell append-only WAL on the agent", empty-WAL gap).

The restore/resync dispatch path SHALL NOT ship a `RestoreInitState` whose assembled payload is empty (no snapshot and no WAL). It SHALL guard with `Flezha.State.Restore.has_persisted_state?/1` — as the bind-time path already does — so an empty payload can never rebuild a live Cell to `init_state` and destroy accumulated state. An empty-WAL condition SHALL route to the pull path above instead.

Restore SHALL be:

1. **Unconditional on every Overwatch bind, and performed by the bind** — `Lifecycle.complete_init/4` restores the persisted Cells of the relay it binds, awaited for the effectively-once tier and dispatched from the bind for every other class, and stamps `AddRelay.resume_cell_count` from the same enumeration so a durable steps driver is held until they land. Every way a relay comes onto a runner is that bind: a fresh bind, an un-park, a rehydrate, a rewind, the convergence loop's rebind after its runner died, and the takeover by a restarted runner presenting the same stable identity. The heal's `rebind_relay` SHALL dispatch no restore of its own after the bind, and no other repair SHALL ship a `RestoreInitState`: `resync_runner` pulls the live Cell's state (`CompactCell`), since a copy behind a live Cell would rewind it. When the relay arrived via HotSwap handover, the in-band state is authoritative and the bind's restore SHALL skip to avoid double-applying.
2. **Keyed by relay instance id, not factory id** — the Cell wire identity is `[rc_len:u8][rc][sh_len:u8][sh][pk_len:u16 BE][pk]` keyed by the relay *instance*, so two relays of the same factory class never collide on one WAL.
3. **Inclusive of seq 0** — an empty WAL initialises `last_persisted_seq = -1` (not `0`) and the "no snapshot" restore sentinel is `-1`, so the 0-based first change is never dedup-dropped. After fold, the SDK re-arms `change_seq = terminal_seq + 1` so the next produced frame is contiguous.
4. **Re-armed from the controller's terminal** — `RestoreInitState.terminal_seq` SHALL carry the controller's `max(snapshot seq, last WAL seq shipped)`, and the SDK SHALL re-arm `change_seq = max(change-log terminal, terminal_seq) + 1`. A snapshot-only restore — the WAL tail is empty after a compaction — carries no change-log record to derive the seq from; re-armed below the snapshot's seq, its next Changes would be dropped by the writer as duplicates.

#### Scenario: a snapshot-only restore keeps the change seq contiguous
- **GIVEN** a Cell compacted at seq 1 (snapshot present, WAL tail empty)
- **WHEN** it is restored on a rebind and produces its next Change
- **THEN** that Change SHALL carry seq 2 and SHALL be persisted, not dedup-dropped

#### Scenario: a stateful relay resumes its accumulated state after both runners restart
- **GIVEN** a `resume_probe` relay has accumulated a running total of 100 in persisted state
- **WHEN** both runners are gracefully stopped and restarted under load and the relay rebinds
- **THEN** the relay's restored state is 100 (the WAL snapshot + change-log), driving one more `+7` input emits 107, and no state is lost.

#### Scenario: a durable steps relay's driver is held until the loop's rebind has restored its Cells
- **GIVEN** a durable steps instance with one persisted Cell whose runner is killed
- **WHEN** the convergence loop rebinds it onto a live runner
- **THEN** the `AddRelay` SHALL carry `resume_cell_count = 1`, the bind SHALL ship that Cell's `RestoreInitState`, the driver SHALL run its first uncommitted tasklet only after the restore landed, and no second `RestoreInitState` for the Cell SHALL follow from the heal

#### Scenario: the first change of a fresh relay survives restart
- **GIVEN** a relay produced exactly one state change (seq 0) and no snapshot exists
- **WHEN** the runner restarts and restore runs
- **THEN** the seq-0 change is included in the restored change-log (the `-1` empty-WAL/no-snapshot sentinels keep it), not dropped as a dedup floor.

#### Scenario: fresh controller pulls a baseline from a live runner
- **GIVEN** a Tasklet is reassigned to a controller that has no WAL and no snapshot for its Cell, while the runner stays live holding accumulated state at seq 5000
- **WHEN** the relay binds and the runner's next `cell_state_change` arrives
- **THEN** the controller SHALL request a `CompactCell` and, on receiving the `cell_state_snapshot`, write it as the Cell's baseline; the controller SHALL NOT push a `RestoreInitState` (which would carry an empty payload and reset the live Cell to `init_state`)

#### Scenario: empty-payload restore is never dispatched
- **GIVEN** a resync is triggered for a Cell whose controller has no snapshot and no WAL
- **WHEN** the recovery path evaluates whether to ship `RestoreInitState`
- **THEN** `has_persisted_state?/1` SHALL be false and no `RestoreInitState` SHALL be sent; the accumulated live state on the runner SHALL be preserved (not rebuilt to `init_state`)

### Requirement: Time-travel queries via WAL retention beyond latest snapshot

The agent SHALL retain pre-compaction WAL records for `time_travel_retention_hours` beyond the latest snapshot, configurable per-Relay-class via runtime config `time_travel_retention_hours.<RelayClass>` (default `6` hours globally; setting to `0` disables time travel for that Relay class). The retention sweeper (`Flezha.State.Retention`) SHALL run on `time_travel_retention_check_interval_ms` (default 60_000), iterate Cells, and rewrite each Cell's WAL dropping records older than the cutoff. A Cell whose writer runs SHALL be cut by that writer; a Cell with none SHALL be cut by the same cut function under a per-Cell lock that a writer starting meanwhile SHALL wait for, so no record that writer appends is lost and no output is dispatched by the sweep. Either cut SHALL first write the consumed offsets of the records it drops into the Cell's lineage record as its folded offset; behind the remote gate the offline cut SHALL count only the records at or below the Cell's persisted flush cursor, never past what the gate has acknowledged.

The agent SHALL expose a query interface `Flezha.State.TimeTravel.query(cell_id, target)` where `target = {:seq, u64} | {:timestamp, datetime}`. The query SHALL: (1) locate the latest snapshot at-or-before the target; (2) read the tail-WAL records above that snapshot and up to the target, through the one payload function the bind's restore uses, so a Cell with no snapshot includes its record 0, and return the snapshot's header with its bytes; (3) ship `RestoreInitState` to a transient SDK-side replay relay; (4) receive a `cell_state_snapshot` carrying JSON-encoded terminal state; (5) return the JSON to the caller.

The query SHALL be exposed via the `flz state at <relay/tasklet/cell> <seq | timestamp>` CLI subcommand.

#### Scenario: Default 6-hour retention

- **WHEN** a runner does not configure `time_travel_retention_hours`
- **THEN** the agent SHALL retain WAL records for 6 hours beyond the latest snapshot

#### Scenario: Time-travel disabled (`retention_hours = 0`)

- **WHEN** `time_travel_retention_hours.<RelayClass> = 0`
- **THEN** the retention sweeper SHALL truncate the WAL up to the latest snapshot's seq immediately on each tick; only the latest snapshot is queryable; queries for any seq < latest snapshot return `TimeTravelError{kind: :outside_retention}`

#### Scenario: Query within retention window returns state

- **WHEN** the runner runs `flz state at MarketRelay/btc/OhlcTasklet/M1 "2026-05-07T10:30:00Z"` AND that timestamp falls within the retention window
- **THEN** the agent SHALL load the latest snapshot at-or-before the timestamp and fold WAL records up to it; the CLI SHALL print the JSON-encoded state

#### Scenario: Query outside retention returns error

- **WHEN** the runner queries a timestamp older than the earliest available WAL record's record-time
- **THEN** the agent SHALL return `TimeTravelError{kind: :outside_retention, earliest_available: <ts>}`; the CLI SHALL print a clear error pointing at the earliest queryable point

#### Scenario: Query against persistence-off Relay errors

- **WHEN** the runner runs `flz state at <relay/tasklet/cell> <target>` for a Cell whose Relay has `keeps_state = false`
- **THEN** the agent SHALL return `TimeTravelError{kind: :cell_not_persistent, relay, tasklet, cell}`

#### Scenario: Live retention update applies on next sweep

- **WHEN** the runner runs `flz state retention MarketRelay 12` and the prior value was `6`
- **THEN** the runtime config SHALL be updated to 12 hours; the next retention-sweeper tick SHALL apply the new value (records older than 12 hours are deleted; records younger SHALL be retained going forward)

#### Scenario: A Cell with no snapshot keeps its first record

- **WHEN** `flz state at <relay/tasklet/cell> 3` targets a Cell whose WAL holds records 0..5 and no snapshot
- **THEN** the change log returned SHALL hold records 0 to 3, the same bytes the bind's restore would ship for them

#### Scenario: A writer started during an offline cut keeps its record

- **WHEN** the sweeper cuts a Cell that has no writer, and a record for that Cell arrives while the cut runs
- **THEN** the writer started for it SHALL open the file only after the cut ends, and the record SHALL be in the WAL afterwards

#### Scenario: An offline cut folds what it drops

- **WHEN** the sweeper cuts records carrying consumed offsets up to 41 from a Cell that has no writer
- **THEN** the Cell's lineage record SHALL hold a folded offset of at least 41 before the cut file is renamed into place, and the Cell's floor read afterwards SHALL be what it was before the cut

#### Scenario: An offline cut behind the remote gate folds only what was acknowledged

- **WHEN** the sweeper cuts a writer-less Cell of a `durable: remote` class whose persisted flush cursor is 5 and whose dropped records 0..7 carry consumed offsets up to 41, the record at 5 carrying 30
- **THEN** the folded offset written SHALL be 30, not 41

### Requirement: Fsync batching at configurable cadence

The agent SHALL run a single shared `Flezha.State.FsyncBatcher` GenServer per BEAM node. The batcher SHALL maintain a `MapSet` of dirty writer IDs (Cells whose `WalWriter`, or input-logs whose log process, has appended bytes since the last fsync). A writer SHALL mark itself dirty once per fsync window — on the first append after its last sync, not on every append. On each tick at interval `agent_fsync_interval_ms` (default 5; configurable via runtime config in range [1, 1000]), the batcher SHALL: (1) iterate the dirty set; (2) cast `:fsync` to each writer, so the writers sync concurrently in their own processes and a writer busy behind a deep mailbox delays no other writer's durability, credit release or outbox dispatch; (3) reset the set. `flush_now` and `flush_all` SHALL instead call each dirty writer synchronously, so their callers return with every sync done; a writer that does not confirm a synchronous sync SHALL stay in the dirty set for the next drain.

The tick SHALL be armed exactly while the dirty set is non-empty, so a batcher with nothing dirty never wakes. A mark that finds no tick armed SHALL arm one for the next point of the rhythm the last tick set: the last tick's time plus a whole number of intervals, and never sooner than one interval after that tick. The tick a mark arms therefore falls due no later than a tick that had kept running would have, and ticks that keep finding work fall due one interval apart. A mark while a tick is armed SHALL leave that tick as it is, so a steady stream of marks cannot push it back. A drain, a `forget` or a flush that leaves the set empty SHALL leave no tick armed; a flush that leaves a writer dirty SHALL leave the tick armed, so that writer is retried on it.

A (re)started batcher SHALL seed its dirty set with every registered writer: with one mark per window, a dirty set lost with a crashed batcher would otherwise never be rebuilt, and a writer's un-synced tail would wait for its next append.

`WalWriter` SHALL invoke `:file.datasync/1` on its file handle when receiving the `:fsync` message and SHALL update `fsync_durability_lag_ms` to `now - oldest_pending_record_timestamp`.

#### Scenario: Default 5 ms tick while a writer is dirty

- **WHEN** a runner does not configure `agent_fsync_interval_ms` and writers keep marking themselves dirty
- **THEN** the batcher SHALL tick at a 5 ms interval AND each dirty `WalWriter` SHALL fsync at most once per tick

#### Scenario: A batcher with nothing dirty does not wake

- **WHEN** a drain, a `forget` or a flush leaves the dirty set empty, as on a controller with no runner
- **THEN** the batcher SHALL have no tick armed, and SHALL receive no tick until a writer marks itself dirty

#### Scenario: A mark after a quiet spell waits for the rhythm, not a whole interval

- **WHEN** a writer marks itself dirty while no tick is armed, some time after the last tick
- **THEN** the batcher SHALL arm the tick for the first point, not before the mark, that lies a whole number of intervals after the last tick and at least one interval after it
- **AND** a second mark before that tick SHALL leave it where it is

#### Scenario: A writer a flush could not reach keeps its retry

- **WHEN** `flush_now` or `flush_all` leaves a writer in the dirty set because it did not confirm its sync
- **THEN** a tick SHALL stay armed, and that writer SHALL be drained on it

#### Scenario: 5 ms + 5 ms = 10 ms total durability bound

- **WHEN** a Tasklet records a Change at time `t` under default flusher and fsync settings (`linger_ms = 5` SDK-side; `agent_fsync_interval_ms = 5` agent-side)
- **THEN** the Change SHALL be durably fsynced to disk on the agent by `t + 10 ms` modulo network round-trip and OS scheduling; `fsync_durability_lag_ms` SHALL stabilize below 10 ms under steady-state load

#### Scenario: Runner-tightened bound

- **WHEN** a runner sets `agent_fsync_interval_ms = 1`
- **THEN** the batcher SHALL tick at a 1 ms interval while a writer is dirty; the durability bound SHALL be ~2 ms total at the cost of more frequent fsyncs

#### Scenario: One dirty mark per fsync window

- **WHEN** a `WalWriter` appends N records between two batcher ticks
- **THEN** it marks itself dirty once, on the first of those appends, and marks again only after the tick has synced it

#### Scenario: A slow writer does not delay its siblings

- **WHEN** one dirty writer's mailbox is deep enough that its sync runs late
- **THEN** every other dirty writer syncs on the same tick, because the tick casts instead of waiting on each writer in turn

#### Scenario: A restarted batcher syncs every registered writer

- **WHEN** the batcher crashes with marks in its dirty set and restarts
- **THEN** its first tick syncs every registered writer, so no un-synced tail waits for that writer's next append

### Requirement: Per-Relay persistence toggle resolution at relay startup

The controller SHALL resolve a per-Relay-class boolean `keeps_state` for an
instance of either run from the class's own `durable` as the runner announced it in the
`AddRelay` response metadata (`durable`, `"memory"` when the class opts out), recorded by the
class registry with the class's other declarations so a process-less read at boot or in a sweep
resolves it too; a class that announced nothing resolves `true`. No controller configuration
SHALL override it.

The run SHALL NOT enter the resolution: a flow and a steps relay are resolved by the same rule
from the same declaration. The resolved value SHALL be bound to the Relay instance for its
entire lifetime. Subsequent changes to the SDK's class setting (via SDK redeploy) SHALL NOT
affect already-running instances.

When `keeps_state = false`, the controller SHALL NOT allocate WAL or snapshot files for
any of that Relay's Cells, SHALL ignore (drop with a counter increment) any incoming
`cell_state_change` or `cell_state_snapshot` frames for those Cells, SHALL NOT issue
`RestoreInitState` on reconnect, and SHALL refuse a live Cell-state read as
the code `state.persistence_off`.

#### Scenario: Default-on

- **WHEN** `MarketRelay`'s `AddRelay` response announces `durable = "local"`
- **THEN** the controller SHALL resolve `keeps_state = true` for `MarketRelay`

#### Scenario: The class's opt-out reaches the controller

- **WHEN** a class of either run declares `durable: false` in its SDK and an instance of it is added
- **THEN** the `AddRelay` response SHALL carry `durable = "memory"`, the controller SHALL resolve `keeps_state = false` for the class, no restore SHALL be shipped on its next bind, and its live Cell-state read SHALL be refused with the code `state.persistence_off`

#### Scenario: A leftover config map does not switch a class's state off

- **WHEN** `MarketRelay` declares `effectively_once` and announces `durable = "local"` AND the controller's application env still carries `relay_persistence: %{"MarketRelay" => false}` from an older release
- **THEN** the controller SHALL resolve `keeps_state = true` for `MarketRelay` and SHALL restore its persisted Cells on bind

### Requirement: 16-byte version header on persisted blobs

Every WAL record and snapshot file SHALL carry a 16-byte header preceding the payload bytes:

- bytes 0:1 — `sdk_lang: u8`, an **allocated identifier** with one table of record in `crates/flz_core/src/header.rs`: `1` Rust, `2` Java, `3` Elixir, `4` Node, `5` Ruby, `6` PHP, `7` Python, `8` Go. Values `9–254` are allocated to further SDKs **in landing order**, one value per SDK, and a proposal SHALL NOT assert a value it has not been allocated. `0` is reserved and means "unspecified"; `255` is reserved.
- bytes 1:5 — `sdk_version: u32 BE`, the **writing SDK's own** released version packed as `(major << 16) | (minor << 8) | patch` with each component clamped at 255. `0` means the writing SDK did not state one and SHALL NOT be read as version 0.0.0.
- bytes 5:9 — `schema_version: u32 BE` (per-Tasklet, user-controlled)
- bytes 9:10 — `format_id: u8` (1 = postcard, 2 = effect record, 3 = timer ops, 4 = kryo-registered, 5 = msgpack, 6 = json, 7 = etf — Erlang External Term Format, the Elixir SDK's codec). A `format_id` names the codec that wrote the body and MAY be shared by SDKs that write the same codec, because the cross-language gate is `sdk_lang` and not this byte; a new codec takes the next free value in the same table of record.
- bytes 10:12 — reserved, MUST be zero
- bytes 12:16 — `crc32: u32 BE` over the payload bytes (change_bytes for WAL records; snapshot_bytes for snapshot files)

The agent SHALL stamp the header at write time using the SDK-supplied values (the SDK ships the header pre-stamped in the wire frame; the agent writes it verbatim). The agent SHALL treat an unknown `sdk_lang` or `format_id` as opaque — it stores and re-ships such records unchanged and SHALL NOT reject them, so allocating a new SDK's value requires no agent change. The agent SHALL, however, **read** byte 0 to resolve a factory's established language for the placement guard; that read is a routing decision, not a validation of the body.

The SDK SHALL verify the header at read time on **every** restore path — the typed path and the host path alike — before any body reaches a codec. A record whose `sdk_lang` is not the reading SDK's own value SHALL be refused with a language-mismatch error naming both values; the SDK SHALL NOT attempt to decode it, and SHALL NOT fall back to a partial or empty state. The agent SHALL NOT verify the header except for the leading WAL record-framing crc32 over `header + change_bytes`.

**The language is resolved before the format.** A record from another SDK is neither a format question nor a schema one, so the language gate SHALL be applied first and its refusal SHALL name both languages. Reading the format byte first makes a record written by an SDK with a different codec report as a malformed header, naming neither.

**`sdk_version` identifies the SDK that wrote the record, and it is advisory.** Each SDK SHALL stamp its own released version — the version its own package metadata declares, never the version of a component it links — and an SDK that is a host over the shared kernel SHALL carry that value across the declaration surface rather than let the kernel supply one of its own. The core SHALL refuse a class declaration that states no version, so a record written after this rule is in force SHALL NOT carry `0`.

**No restore path SHALL gate on `sdk_version`.** The language byte decides which codec may read a body and the schema version decides which shape it has; those two carry every refusal. A migration MAY consult `sdk_version` to distinguish records that are identical in language and schema but were written by SDK builds known to differ, and SHALL NOT decide a body's shape from it alone. The packing is lossy — a pre-release or build suffix is dropped and a component above 255 clamps — so two distinct SDK builds MAY stamp the same value, and equality of this field SHALL NOT be read as identity of the writer.

#### Scenario: Header stamped verbatim from SDK

- **WHEN** the SDK ships a `cell_state_change` frame with header `[0x01, ...sdk_version..., ...schema_version..., 0x01, 0x00, 0x00, ...crc32...]`
- **THEN** the agent SHALL append the WAL record byte-for-byte; no agent-side header rewriting SHALL occur

#### Scenario: Cross-language load fails loud at SDK side

- **WHEN** a Java SDK reconnects to a Cell whose WAL was written by a Rust SDK (header `sdk_lang = 1`)
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

- **WHEN** a Ruby SDK writes a change body and a Node SDK later reconnects to that Cell
- **THEN** the header SHALL carry the Ruby SDK's allocated `sdk_lang` and `format_id = 5`, and the restore SHALL be refused with a language-mismatch error naming both SDKs — the shared `format_id` SHALL NOT let one SDK's codec read the other's bytes

#### Scenario: PHP records are self-describing

- **WHEN** a PHP-hosted Cell writes a change record
- **THEN** the header SHALL carry the PHP SDK's allocated `sdk_lang` and the JSON `format_id`, and a later PHP restore of that Cell SHALL decode it

#### Scenario: A PHP SDK refuses another language's records

- **WHEN** a PHP SDK is bound to a Cell whose WAL was written with any other SDK's `sdk_lang`
- **THEN** the core's host restore fold SHALL refuse before any body reaches the PHP codec, and the controller SHALL leave the relay unbound rather than cold-starting it

#### Scenario: A Python-written change is self-describing on restore

- **WHEN** a Python SDK writes a change body and later restores the Cell
- **THEN** the header SHALL carry the Python SDK's allocated `sdk_lang` and `format_id = 5`, and a Rust or Java SDK reconnecting to that Cell SHALL fail with the same cross-language-mismatch error rather than misreading the payload

#### Scenario: A shared codec byte does not weaken the language gate

- **WHEN** three SDKs write MessagePack bodies and therefore all stamp `format_id = 5`
- **THEN** their records SHALL remain distinguishable by `sdk_lang`, and a restore SHALL refuse on that byte rather than on the codec byte

#### Scenario: An Elixir record is self-describing and the Elixir SDK refuses every other language

- **WHEN** an Elixir Cell writes a change record and later restores it, and separately is sent a record stamped with any other SDK's `sdk_lang`
- **THEN** the written header SHALL carry `sdk_lang = 3` and `format_id = 7`, the Elixir restore SHALL decode it, and the foreign record SHALL be refused by the core's host restore fold with the language-mismatch wording naming both languages before its format byte is read

#### Scenario: A record names the SDK that wrote it, not the kernel it linked

- **GIVEN** a host-layer SDK at its own released version, running over a kernel built at a different version
- **WHEN** that SDK writes a change record
- **THEN** bytes 1:5 SHALL carry the SDK's own version, and SHALL NOT carry the kernel's

#### Scenario: A declaration stating no version is refused before any record is written

- **WHEN** a host class is declared with `sdk_version` absent or zero
- **THEN** the declaration SHALL be refused naming the field, and no Cell of that class SHALL reach a state where it could stamp a header

### Requirement: Idempotent emits with stable dedup keys

The Rust SDK runtime SHALL automatically attach `flezha.dedup_key = "{relay_class}/{tasklet_id}/{cell_key}/{change_seq}"` to outgoing plugin-bound emit metadata for any emit issued during a `Tasklet::run` invocation that records changes through `ctx.change(...)`. The agent's plugin-provider path SHALL NOT alter or strip this metadata key; it SHALL flow through to the output verbatim.

The Kafka producer plugin (`FlezhaKafka.Producer`, from the Kafka plugin package) SHALL use the `flezha.dedup_key` value as the Kafka producer key when the output carries neither a `flz.key` nor the effectively-once `flz.epoch` stamp. Other plugin providers MAY use the key for output-specific dedup purposes; the contract is that the key is opaque metadata.

User-supplied metadata via `ctx.emit_with_meta(value, meta)` SHALL have its `flezha.dedup_key` entry silently dropped (matching the existing reserved-key behavior for `channel`); the SDK runtime stamps its own value.

#### Scenario: Dedup key attached automatically

- **WHEN** a Tasklet at `(MarketRelay/btc, OhlcTasklet, M1)` records a Change at `seq = 12345` AND the same `Tasklet::run` invocation calls `ctx.emit::<BarClosed>(...)` to a Kafka-bound channel
- **THEN** the outgoing Kafka producer SHALL receive the message with key `"MarketRelay/OhlcTasklet/M1/12345"`

#### Scenario: Replayed Change preserves dedup key

- **WHEN** a Cell is restored from snapshot + WAL and re-issues an emit for `seq = 12345`
- **THEN** the re-issued Kafka producer message SHALL carry the identical key `"MarketRelay/OhlcTasklet/M1/12345"` as the original emit; downstream output dedup (e.g. Kafka log compaction by key) SHALL collapse the duplicates

#### Scenario: Agent-side outbox dedupes by (cell, change_seq)

- **WHEN** the SDK ships `cell_state_change` with `seq = 12345` to the agent twice for the same Cell (once before crash, once after replay)
- **THEN** the agent SHALL persist exactly one record; the duplicate SHALL be detected via `seq <= last_persisted_seq` and discarded silently with `dedup_drops_total += 1`

#### Scenario: User-supplied dedup_key is dropped

- **WHEN** a Tasklet calls `ctx.emit_with_meta(value, %{"flezha.dedup_key" => "custom"})`
- **THEN** the user's `flezha.dedup_key` entry SHALL be silently dropped; the SDK runtime SHALL stamp `"{relay}/{tasklet}/{cell}/{seq}"` as the dedup key

### Requirement: Per-Cell persistence metrics

Each Cell's metric set SHALL expose six counters via `:counters` references, accessible from `Flezha.State.Metrics.to_map(cell_id)` and exported through the existing `Flezha.State.Metrics` path:

- `persisted_changes_total` — monotonic; incremented per Change appended to the WAL (post-coalesce, post-dedup).
- `persisted_bytes_total` — monotonic; incremented by the byte length of each appended record.
- `wal_size_bytes` — gauge; current WAL file size (read from `:file.read_file_info/1` after each append and after each retention-sweep rewrite).
- `snapshot_seq` — gauge; seq of the latest snapshot file (or 0 if none).
- `flush_latency_ms_p99` — gauge; rolling 99th-percentile of `now - record_received_ts` measured per fsync, computed via a 1024-bucket reservoir sampler.
- `fsync_durability_lag_ms` — gauge; `now - oldest_pending_record_ts` at each `FsyncBatcher` tick.

The counters SHALL use `Ordering::Relaxed` (the BEAM `:counters` API does not expose ordering modes; this is satisfied by the underlying implementation). External metric systems (Prometheus, StatsD) integration is out of scope for this requirement; the existing `Flezha.State.Metrics` path is the export surface.

#### Scenario: Counters initialized at Cell construction

- **WHEN** a Cell is constructed with `keeps_state = true`
- **THEN** all six counters SHALL be allocated; `persisted_changes_total = 0`, `persisted_bytes_total = 0`, `wal_size_bytes = 0`, `snapshot_seq = 0` (no snapshot yet), `flush_latency_ms_p99 = 0`, `fsync_durability_lag_ms = 0`

#### Scenario: Counters update after Change append

- **WHEN** the agent appends 10 Changes totaling 500 bytes to a Cell's WAL
- **THEN** `persisted_changes_total` SHALL increase by 10; `persisted_bytes_total` SHALL increase by `>= 500` (record framing overhead included; ≈ `500 + 10 * (8 + 4 + 16 + 4) = 820` bytes in practice); `wal_size_bytes` SHALL equal the file size after the appends

#### Scenario: Counters survive agent restart

- **WHEN** the agent is restarted and reads back the WAL files
- **THEN** `persisted_changes_total` and `persisted_bytes_total` SHALL be recomputed from the WAL records (scanning); `snapshot_seq` SHALL be the highest snapshot file's seq; `wal_size_bytes` SHALL match the file size; `flush_latency_ms_p99` and `fsync_durability_lag_ms` SHALL reset to 0

### Requirement: Back-pressure via OverflowPolicy when WAL outpaces fsync

For sessions that are NOT credit-speaking (pre-credit SDKs, or the controller running with credit flow control disabled), the agent SHALL monitor each `WalWriter`'s `pending_bytes` (queued-but-not-fsynced bytes). When `pending_bytes >= wal_high_watermark_bytes` (default 16 MiB), the agent SHALL ship `CellMailboxStatus{cell_id, status: :full}` over the control channel to the SDK. When `pending_bytes` falls below `wal_low_watermark_bytes` (default `wal_high_watermark_bytes / 2`), the agent SHALL ship `CellMailboxStatus{cell_id, status: :open}`.

The SDK's per-Cell mailbox observer SHALL treat `:full` as equivalent to the existing per-Cell mailbox-full condition that drives `OverflowPolicy`; the producing Tasklet's `ctx.emit(...)` SHALL apply the channel's declared policy (`DropNothing` waits for room, `DropNewest` drops, `DropOldest` discards-oldest).

The agent SHALL NOT issue `:full` more than once per Cell while the watermark is exceeded (idempotent transitions); SHALL NOT issue `:open` until `:full` was previously sent for that Cell (no spurious `:open` frames).

For credit-speaking sessions the agent SHALL NOT ship `CellMailboxStatus` frames: back-pressure for those sessions is carried entirely by the credit-grant protocol (see the `credit-grant-flow-control` capability), which parks the sender on window exhaustion instead of signalling a status flag.

#### Scenario: High-watermark triggers :full status

- **WHEN** a Cell's `WalWriter` accumulates `pending_bytes = 16 MiB + 1 record` on a non-credit-speaking session
- **THEN** the agent SHALL ship `CellMailboxStatus{cell_id, status: :full}` exactly once

#### Scenario: Low-watermark triggers :open status

- **WHEN** the same Cell's `pending_bytes` drains below 8 MiB after fsyncs
- **THEN** the agent SHALL ship `CellMailboxStatus{cell_id, status: :open}` exactly once

#### Scenario: OverflowPolicy::DropNothing parks producer under back-pressure

- **WHEN** a Tasklet with `OverflowPolicy::DropNothing` on its emit channel calls `ctx.emit(...)` for a Cell under `:full` status on a non-credit-speaking session
- **THEN** the producer SHALL park its calling thread; on receipt of `:open` status the producer SHALL unpark and the emit SHALL succeed; the producer's `Tasklet::run` SHALL eventually return `Ok(())`

#### Scenario: Credit-speaking session receives no status flags

- **WHEN** a credit-speaking SDK's Cell drives `pending_bytes` past `wal_high_watermark_bytes`
- **THEN** no `CellMailboxStatus` frame is shipped; the sender is throttled by credit exhaustion instead

### Requirement: Hard in-memory bound sheds WAL appends past the watermark

Each `WalWriter` SHALL enforce a hard in-memory bound on un-fsynced data —
covering both queued-but-not-fsynced bytes and pending mailbox depth. When the
hard bound is exceeded, `append_async/5` SHALL shed the incoming Change (drop
it without writing) and increment a shed counter, rather than growing the
writer's mailbox or un-fsynced bytes without bound. The data plane SHALL NOT
block: the shed leaves a sequence gap that the existing resync path repairs.

The hard bound SHALL be strictly above the soft watermark. For non-credit-speaking
sessions, shedding remains the last-resort safety net for a slow disk or an SDK
that does not honour the `CellMailboxStatus` wire signal. For credit-speaking
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
  reached and `wal_shed_total` for its Cells does not advance

### Requirement: Crash-safe persistence on agent reboot

On agent boot, after `Flezha.Boot.StatePathMigrator.migrate/0` runs, `Flezha.State.Supervisor` SHALL call `Registry.scan_dir/1` for each `storage/app/state/{relay}/{tasklet}/` directory and reconstruct the in-memory persistence state:

- For each `cell_key.wal` file: start a `WalWriter` GenServer; `WalWriter::scan_for_last_valid_seq/1` runs at `init/1` to recover `last_persisted_seq` and truncate any partial-write tail.
- For each `cell_key.snap.{seq}` file (latest): record the `snapshot_seq` counter.
- For each `cell_key.snap.{seq}.tmp` file: unlink (orphaned mid-write).
- For each `cell_key.wal.rewriting` file: unlink (orphaned retention rewrite).
- For each Cell's `cell_key.lineage` and `cell_key.outbox.dedup` pair: read both (`last_persisted_seq` recovered from the WAL scan as a cross-check); the epoch SHALL be the higher of the two records'; a dedup file at an older epoch than the lineage record SHALL complete the lineage record's pending cut before the writer serves anything, and a dedup file at an epoch at or above it SHALL make that pending cut void.

`Flezha.State.Lineage` SHALL persist each Cell's dedup file `cell_key.outbox.dedup` in its existing layout (four 8-byte big-endian integers: `last_persisted_seq`, `outbox_flushed_seq`, `epoch`, `folded_offset`), so a build that predates the lineage record reads it unchanged, at the cadence the requirement "Dedup-file writes are amortized off the append hot path" states, via a write-then-rename atomic update, and the Cell's lineage record into `cell_key.lineage` before any dedup file that depends on it, the lineage install followed by an fsync of its directory; this allows fast dedup-cache priming on boot without re-scanning the entire WAL.

#### Scenario: Boot recovers from clean shutdown

- **WHEN** the agent boots after a clean shutdown
- **THEN** for each Cell's WAL file, `WalWriter::scan_for_last_valid_seq` SHALL run successfully and recover `last_persisted_seq` matching the on-disk state

#### Scenario: Boot recovers from crash during WAL append

- **WHEN** the agent boots after a crash mid-WAL-append (partial record at file tail)
- **THEN** `scan_for_last_valid_seq` SHALL detect the crc-mismatched partial record AND truncate it; `last_persisted_seq` SHALL equal the seq of the last valid (crc-good) record

#### Scenario: Boot recovers from crash during snapshot rename

- **WHEN** the agent boots after a crash between snapshot fsync and rename, leaving `cell_key.snap.{seq}.tmp`
- **THEN** boot SHALL unlink the `.tmp` file; the prior snapshot (if any) and the un-truncated WAL are intact; persistence resumes correctly

#### Scenario: Boot recovers from crash during retention rewrite

- **WHEN** the agent boots after a crash during the retention sweeper's WAL rewrite, leaving `cell_key.wal.rewriting`
- **THEN** boot SHALL unlink the `.rewriting` file; the original WAL is intact; the next sweeper tick re-runs the rewrite idempotently

#### Scenario: A dedup file ahead of the lineage record wins the epoch

- **WHEN** the agent boots with a dedup file at epoch 7 and a lineage record at epoch 6 holding a pending cut (a build that predates the lineage record ran in between, or the lineage install was lost)
- **THEN** the writer SHALL serve at epoch 7 with the dedup file's folded offset, SHALL ignore the pending cut, and SHALL write the lineage record at epoch 7 before its next dedup file

#### Scenario: Boot completes a bump a crash interrupted

- **WHEN** the agent boots after a crash between a rewind's lineage write and its WAL cut, leaving a lineage record at epoch 5 with a pending cut above seq S and a dedup file at epoch 4
- **THEN** boot SHALL cut the WAL and the snapshots above S and write a dedup file at epoch 5 before the writer serves, and SHALL re-drive no output of the discarded records

### Requirement: flz state CLI subcommand surface

The agent SHALL expose a `flz state` CLI subcommand tree, using relay vocabulary in every path argument:

- `flz state get <relay/tasklet/cell>` — current in-memory state of one live Cell, with the `change_seq` it was read at. Refuses a non-resident Cell naming the reason.
- `flz state at <relay/tasklet/cell> <seq | timestamp>` — point-in-time state query per the time-travel requirement. Returns JSON-encoded state.
- `flz state list <relay>` — for each Cell of the Relay, prints `wal_seq`, `snapshot_seq`, `wal_bytes`, `flush_latency_ms_p99`, `fsync_durability_lag_ms`. Reads `Metrics.to_map/1`.
- `flz state rotate <relay/tasklet/cell>` — forces compaction by shipping `CompactCell` to the Cell's runner and awaiting its answer: "compaction queued" when the runner took it (the snapshot completes asynchronously), "compaction refused" naming the Cell when the runner refused it as busy (the Watchdog asks again on its next sweep), "compaction timed out" when no answer came within `compaction_timeout_ms`, or "compaction failed" with the reason for a runner error or a stub reply.
- `flz state retention <relay> <hours>` — sets `time_travel_retention_hours.<RelayClass>` in runtime config (`persistent: true`); the next retention-sweeper tick reads the new value.

#### Scenario: flz state get returns the live value

- **WHEN** the runner runs `flz state get MarketRelay/OhlcTasklet/M1` against a live Cell
- **THEN** the CLI SHALL print that Cell's state as JSON with its `change_seq` and exit 0

#### Scenario: flz state get refuses a non-resident Cell

- **WHEN** the runner runs `flz state get` against an idle-evicted Cell
- **THEN** the CLI SHALL exit non-zero naming eviction and pointing at `flz state at`

#### Scenario: flz state list enumerates Cells

- **WHEN** the runner runs `flz state list MarketRelay`
- **THEN** the CLI SHALL print one line per Cell with the listed fields; output SHALL be tabular (column-aligned)

#### Scenario: flz state rotate triggers async compaction

- **WHEN** the runner runs `flz state rotate MarketRelay/btc/OhlcTasklet/M1`
- **THEN** the agent SHALL ship `CompactCell` to the SDK, and the CLI SHALL print "compaction queued" and exit 0; the resulting snapshot SHALL appear on disk within seconds

#### Scenario: flz state rotate names a refused compaction

- **WHEN** the runner runs `flz state rotate` against a Cell whose runner refuses the snapshot with `state.cell_busy`
- **THEN** the CLI SHALL print "compaction refused" naming the Cell, SHALL NOT print "compaction queued", and SHALL exit 0; the snapshot SHALL appear on a later Watchdog sweep

#### Scenario: flz state retention persists across agent restart

- **WHEN** the runner runs `flz state retention MarketRelay 12` and the agent is subsequently restarted
- **THEN** after restart, the resolved `time_travel_retention_hours` for `MarketRelay` SHALL be 12

### Requirement: Snapshot files use flat path

The Controller SHALL store snapshot files at `storage/app/state/{relay_class}/{tasklet_id}/{cell_key}.snap.{seq}`. The on-disk path SHALL NOT include `runner_id`. The on-the-wire payload-id SHALL be `cell_state_snapshot`.

#### Scenario: Snapshot path is flat

- **WHEN** the Controller compacts the WAL of Cell `p1` (Relay class `MarketRelay`, tasklet `s1`, cell_key `k1`) and assigns snapshot seq=42
- **THEN** the resulting file SHALL be at `storage/app/state/MarketRelay/s1/k1.snap.42`

### Requirement: SDK serializes Relay state on `CellState` request via the Decision-10 JSON shape

When the SDK receives a `CellState{relay_id, tasklet, cell_key}` command, it SHALL respond with a JSON object whose top-level keys are the Relay's Tasklet names and whose values are:

- For single-mode Tasklets (one Cell per Tasklet): the serialized `Self::State` of that Cell.
- For static-mode Tasklets (`cell_keys() = [...]`): an inner JSON object whose keys are the Tasklet's Cell keys and whose values are each Cell's serialized `Self::State`.
- For dynamic-mode Tasklets: an inner JSON object of the same shape as the static case, whose keys are the live Cell keys held in the dynamic Cell registry at serialization time.

`tasklet` and `cell_key` are optional selectors. When both are absent, the reply SHALL cover every Tasklet of the Relay — the pre-existing behaviour, byte-identical. When `tasklet` is present, serialization SHALL be restricted to that Tasklet's Cells. When `cell_key` is also present, serialization SHALL be restricted to that one Cell. `cell_key` present without `tasklet` SHALL be refused, because a Cell key is unique only within its Tasklet.

The wire payload SHALL be `cell_state` payload-id with the JSON object as the message bytes. Empty Relays (no live Cells) SHALL produce `{}`. The reply frame's metadata SHALL carry `flz.change_seq` for the serialized Cell when a selector names exactly one.

Serialization SHALL take the Cell's state lock, SHALL NOT drain pending changes, and SHALL NOT trigger compaction or WAL truncation; the serialized value therefore includes applied-but-unflushed changes.

State serialization SHALL go through a per-Tasklet fn pointer captured at `register_stream::<R>()` time. The fn pointer downcasts the type-erased state held in `CellSlot::state` to its statically-known `CellState<S>` and emits `serde_json::Value` via the `S::State: Serialize` bound on the trait. No additional user-facing trait machinery is required beyond the existing `Serialize + DeserializeOwned` bounds on `Tasklet::State`.

#### Scenario: Single-mode Tasklet returns its state at the tasklet-name key

- **WHEN** the SDK receives `CellState{relay_id}` with no selector for a Relay whose only Tasklet is `"counter"` (single-mode) with state `{"value": 42}`
- **THEN** the wire reply payload SHALL be `{"counter": {"value": 42}}`

#### Scenario: Static-mode Tasklet returns a key-keyed object

- **WHEN** the SDK receives `CellState{relay_id}` with no selector for a Relay whose only Tasklet is `"ohlc"` (static-mode, keys `["1m", "1h"]`) with `1m` state `{"high": 10}` and `1h` state `{"high": 20}`
- **THEN** the wire reply payload SHALL be `{"ohlc": {"1m": {"high": 10}, "1h": {"high": 20}}}`

#### Scenario: Dynamic-mode Tasklet returns its live Cells

- **WHEN** the SDK receives `CellState{relay_id}` for a Relay whose `"candle"` Tasklet holds lazily created Cells `BTCUSD@1m` and `ETHUSD@1m`
- **THEN** the reply SHALL be `{"candle": {"BTCUSD@1m": {...}, "ETHUSD@1m": {...}}}`

#### Scenario: A selector narrows the serialization

- **WHEN** the SDK receives `CellState{relay_id, tasklet: "candle", cell_key: "BTCUSD@1m"}` for a Relay holding two Tasklets and two thousand Cells
- **THEN** exactly one Cell SHALL be serialized, the reply SHALL be `{"candle": {"BTCUSD@1m": {...}}}`, and the frame metadata SHALL carry that Cell's `flz.change_seq`

#### Scenario: A cell_key without a tasklet is refused

- **WHEN** the SDK receives `CellState{relay_id, cell_key: "BTCUSD@1m"}` with no `tasklet`
- **THEN** the SDK SHALL refuse the command rather than searching Tasklets for a matching key

### Requirement: SDK applies `AddRelay.init_values` JSON to per-Cell state

When the SDK receives an `AddRelay{relay_id, factory_id, init_values}` command and `init_values` is a non-empty JSON object, the SDK SHALL parse the JSON, walk its top-level keys, and for each key that matches a registered Tasklet name on the Relay's class:

- For single-mode Tasklets: deserialize the JSON value directly into `S::State` via the per-Tasklet `deserialize_state` fn pointer (captured at registration time and steps relay on `S::State: DeserializeOwned`), then overwrite the freshly-`init_state`d Cell's state with the deserialized value.
- For static-mode Tasklets: walk the inner object's keys; for each `(key, value)` pair where `key` is a declared `cell_key`, deserialize the inner value into `S::State` and overwrite that Cell's state.

The rule SHALL be the same for a flow instance and a steps instance: a steps relay's stage Cells are built through the stream `build_cell` and take `init_values` through the same application, before the `AddRelay` acknowledgement and therefore before any message can reach the instance. No kind SHALL read `init_values` as anything but state.

Top-level keys that do not match any Tasklet name SHALL be ignored (with an `eprintln!` log line at the SDK boundary). Inner-object keys that do not match a declared `cell_key` SHALL also be ignored.

If `init_values` is empty (`""` on the wire), the SDK SHALL leave each Cell's state untouched after the cold-start `Tasklet::init_state(cell_key)` returned by the build path.

`MigrationDeserialization{factory_id, relay_id, serialized_state}` SHALL reuse the same `apply_init_values` path: the `serialized_state` payload is the UTF-8 JSON blob produced by `serialize_relay_state` on the source runner; applying it to a freshly-started instance restores byte-identical state.

#### Scenario: Cold start with init_values seeds single-mode Tasklet

- **WHEN** the SDK receives `AddRelay{relay_id, factory_id, init_values: "{\"counter\":{\"value\":7}}"}` for a Relay with one single-mode Tasklet `"counter"`
- **THEN** the SDK SHALL deserialize `{"value":7}` directly into `CounterState` and overwrite the Cell's state with `CounterState { value: 7 }`

#### Scenario: Cold start with init_values seeds static-mode Tasklet per key

- **WHEN** the SDK receives `AddRelay{...init_values: "{\"ohlc\":{\"1m\":{\"high\":5},\"1h\":{\"high\":10}}}"}` for a Relay with `cell_keys = ["1m", "1h"]`
- **THEN** the SDK SHALL deserialize `{"high":5}` into the `"1m"` Cell's state AND deserialize `{"high":10}` into the `"1h"` Cell's state

#### Scenario: A steps instance takes init_values as state

- **WHEN** the SDK receives `AddRelay{..., kind: "steps", init_values: "{\"reserve_funds\":{\"budget\":100}}"}` for a steps class whose tasklet `reserve_funds` holds state
- **THEN** the `reserve_funds` Cell's state SHALL be `{"budget":100}` before the instance's first message is read, and no entry channel SHALL receive the document

#### Scenario: Empty init_values uses the default path

- **WHEN** the SDK receives `AddRelay{...init_values: ""}`
- **THEN** the SDK SHALL leave each Cell's state at the value `Tasklet::init_state(cell_key)` returned at build time

#### Scenario: Round-trip via MigrationSerialization / MigrationDeserialization

- **WHEN** the SDK on the source runner responds to `MigrationSerialization{relay_id}` with payload bytes `B = serialize_relay_state(instance)`, and a second SDK on the destination runner receives `MigrationDeserialization{factory_id, relay_id, serialized_state: B}`
- **THEN** the destination's freshly-started instance SHALL have Cell state byte-identical to the source's at the moment of serialization, as observed by `serialize_relay_state` re-issued against the destination

### Requirement: The persisted `runner_id` in a relay-binding record is advisory across a controller restart

`Flezha.Relay.Overwatch.init/1` accepts a `stale_runner_id` argument that originates from the durable relay-binding record replayed at boot. This value SHALL be treated as advisory only — it identifies the runner the relay was bound to BEFORE the current controller process started. The Overwatch SHALL re-resolve the live `runner_id` via `RelayManager.assign_runner/2` inside `handle_continue({:bind_runner, …}, _)` and SHALL NOT issue any control-plane calls (`AddRelay`, `:ping_runner`, `start_relay_sdk`) against the persisted value directly.

When the re-resolved `runner_id` differs from `stale_runner_id`, the Overwatch SHALL log an `info`-level rebind line naming both ids and proceed. When the re-resolved `runner_id` equals `stale_runner_id`, the Overwatch SHALL still proceed via the normal startup path — equality with the persisted value does NOT bypass the end-to-end ping (see `runner-controller-connection-healing`).

No on-disk format change accompanies this requirement; the binding record's `runner_id` field SHALL continue to be written and read with the same wire layout. The clarification is purely about the runtime semantics of the field.

#### Scenario: Stale runner_id is re-resolved at boot
- **GIVEN** a durable relay-binding record for relay `r1` with `runner_id = "default_stale"`
- **AND** the controller restarts and replays the record
- **AND** the only live runner in the namespace is `default_fresh`
- **WHEN** `Flezha.Relay.Overwatch` is started by `dist_sup` with `stale_runner_id = "default_stale"`
- **THEN** `handle_continue({:bind_runner, "default_stale", relay}, _)` SHALL invoke `RelayManager.assign_runner(namespace, relay.factory_id)` and resolve to `"default_fresh"`
- **AND** no control-plane call SHALL be issued against `"default_stale"`
- **AND** an `info`-level line SHALL log `relay r1 rebound from stale runner "default_stale" to live "default_fresh"`

#### Scenario: Re-resolved runner_id equal to stale value still runs the full ping path
- **GIVEN** a durable relay-binding record for relay `r1` with `runner_id = "e1"`
- **AND** the controller restarts and `e1` is still the live runner (e.g. the runner outlived the controller)
- **WHEN** the Overwatch resolves `runner_id == stale_runner_id == "e1"`
- **THEN** the Overwatch SHALL still issue `:ping_runner` before `:start_relay`
- **AND** the rebind-log line SHALL NOT fire (the ids are equal)
- **AND** the steps-retry governor SHALL still apply on ping/start failure

### Requirement: A persistence seq gap triggers resync, not permanent rejection

When the Controller receives a `cell_state_change` frame whose `seq > last_persisted_seq + 1`, it SHALL treat the gap as a recoverable desync rather than a terminal protocol error. The Controller SHALL emit `[:flezha, :state, :seq_gap]` telemetry with measurements `%{expected: integer(), got: integer()}` and metadata identifying the Cell, and SHALL initiate the `RestoreInitState` resync handshake for that Cell (assemble snapshot + WAL tail per the existing "Restart restores state via RestoreInitState handshake" flow, ship it to the SDK, await `RestoreInitStateResponse{terminal_seq}`, and resume accepting Changes at `terminal_seq + 1`). After a successful resync the Cell's WAL SHALL advance again and its outbox SHALL drain to the output plugins.

The Controller SHALL NOT leave a Cell indefinitely rejecting Changes with `{:error, {:gap, expected, got}}`. The strict acceptance rule for the contiguous case (`seq == last_persisted_seq + 1`) and the silent-dedup rule for the stale case (`seq <= last_persisted_seq`) are unchanged.

#### Scenario: Gap resynchronises the cell instead of wedging it
- **GIVEN** a Cell with `last_persisted_seq = 100`
- **WHEN** the Controller receives a Change with `seq = 137` (a gap)
- **THEN** the Controller SHALL emit `[:flezha, :state, :seq_gap]` with `expected: 101, got: 137`
- **AND** the Controller SHALL ship `RestoreInitState` for the Cell and await `RestoreInitStateResponse`
- **AND** after the handshake the Cell SHALL accept subsequent Changes and its WAL SHALL advance past `last_persisted_seq`
- **AND** the Cell SHALL NOT remain in a permanent gap-rejection state

#### Scenario: Contiguous and stale frames are unaffected
- **WHEN** a Change arrives with `seq == last_persisted_seq + 1`
- **THEN** it SHALL be appended as today (no resync, no telemetry)
- **AND WHEN** a Change arrives with `seq <= last_persisted_seq`
- **THEN** it SHALL be silently deduped with a `dedup_drops_total` increment as today

### Requirement: Per-Cell output-staleness watchdog is emit-only

The Controller SHALL track, per persistence-enabled Cell, the wall-clock time at which `last_persisted_seq` last advanced. A periodic sweep SHALL run on a configurable interval and, for any Cell whose data stream is attached (the SDK is live and applying inputs) but whose `last_persisted_seq` has not advanced within `output_stall_timeout_ms` (one controller setting, the application key `:default_output_stall_timeout_ms`; default 60_000; no per-class map), SHALL emit `[:flezha, :state, :output_stall]` telemetry (measurements `%{stalled_ms: integer(), last_seq: integer()}`, Cell metadata).

The sweep SHALL do nothing else. It SHALL NOT invoke `Flezha.State.Recovery.resync/2` or any other remedy, and SHALL keep no dedup bookkeeping of its own: the remedy is the convergence Loop's output-stall ladder (`rebind_relay`, under its per-target backoff — see `convergence-self-healing`). A sensor that also heals is a second, uncoordinated actor for one signal.

This bounds the duration of any silent persistence/output freeze to at most one `output_stall_timeout_ms` window plus one convergence tick. Because WAL persistence and output-plugin delivery share the per-Cell outbox, recovering WAL advance SHALL also resume output (e.g. Kafka production) for that Cell.

Each completed sweep SHALL stamp its own freshness into `Flezha.Convergence.Snapshot`, so a sweep that wedges without crashing is visible on `/healthz` rather than silently disabling detection.

#### Scenario: A live cell whose WAL stops advancing is detected and recovered
- **GIVEN** a persistence-enabled Cell whose data stream is attached and whose SDK continues to apply inputs (in-memory state advancing)
- **WHEN** `last_persisted_seq` does not advance for longer than `output_stall_timeout_ms`
- **THEN** the staleness sweep SHALL emit `[:flezha, :state, :output_stall]` with the stalled duration and last seq
- **AND** the sweep SHALL trigger no recovery of its own
- **AND** the convergence Loop SHALL apply the output-stall ladder, after which the Cell's WAL SHALL advance and its outbox SHALL drain

#### Scenario: An idle cell is not falsely flagged
- **GIVEN** a persistence-enabled Cell whose SDK is applying no inputs (genuinely idle, no Changes to persist)
- **WHEN** the staleness sweep runs
- **THEN** the Cell SHALL NOT be flagged as stalled and SHALL NOT be resynced (no Change is expected, so a non-advancing `last_persisted_seq` is correct)

#### Scenario: A wedged sweep is visible within one healthz poll
- **GIVEN** the staleness sweep stops completing passes without the process crashing
- **WHEN** `/healthz` is queried once the stamp exceeds 3× the sweep interval
- **THEN** it SHALL report `200 degraded` with `reason: sensor_stale` naming the watchdog

### Requirement: CompactCell resolves a steps relay Cell

The SDK SHALL resolve the Cell a `CompactCell` addresses in the same two
places a `RestoreInitState` looks: the flow instance registry and a steps relay
instance's stage slots. A steps relay Cell SHALL therefore be compactable by the
iteration-boundary snapshot and by the Watchdog's size/age triggers exactly
like a flow relay Cell.

#### Scenario: A looped steps relay Cell gets its boundary snapshot
- **GIVEN** a durable looping steps instance whose tasklet holds Cell state
- **WHEN** an iteration commits and the Controller ships `CompactCell` for that Cell
- **THEN** the SDK SHALL answer with the Cell's `cell_state_snapshot` (not "no live cell matches the addressed cell_id") and the Controller SHALL write the snapshot and truncate the WAL behind it

### Requirement: A stopped relay's WAL writers are closed

When a relay is stopped, the Controller SHALL close every live `WalWriter`
of its class — whether its state directory is deleted or retained for the
finished-job window. The files decide inspectability; the processes go with
the relay.

#### Scenario: A finished steps relay retains its files, not its writers
- **WHEN** a durable steps relay whose class declares `keep_finished` completes and its state is retained for its window
- **THEN** its WAL and journal files SHALL stay on disk and no `WalWriter` process SHALL remain registered for its cells

#### Scenario: A deleted state directory has no writer under it
- **WHEN** a relay is stopped and its state is deleted
- **THEN** its writers SHALL be closed before the files are removed

### Requirement: Compaction handshake is bounded and cannot wedge persistence

When the Controller ships `CompactCell{cell_id}` to the SDK, it SHALL bound the wait for the corresponding `cell_state_snapshot` response with a timeout. If the snapshot does not arrive within the bound, the Controller SHALL emit `[:flezha, :state, :compaction_timeout]` telemetry, SHALL NOT leave the Cell's WAL append blocked, and SHALL recover via resync. Compaction SHALL be retriable on a later trigger; a single failed/slow compaction SHALL NOT permanently stop WAL growth or output for the Cell.

The recovery step SHALL actually execute: a recovery invocation that cannot dispatch — for example because it lacks the `runner_id` needed to address the runner — SHALL NOT be silently discarded by its caller. Retries SHALL be bounded per the strike-and-give-up requirement above rather than repeating at the sweep interval without limit.

#### Scenario: A stalled compaction times out instead of freezing the WAL
- **GIVEN** a Cell for which the Controller has shipped `CompactCell`
- **WHEN** no `cell_state_snapshot` response arrives within the compaction timeout
- **THEN** the Controller SHALL emit `[:flezha, :state, :compaction_timeout]`
- **AND** WAL append for the Cell SHALL NOT remain blocked (no "large WAL, no snapshot, frozen" state)
- **AND** persistence SHALL recover via resync and compaction SHALL be eligible to retry later

#### Scenario: The recovery step is not a silent no-op
- **GIVEN** a compaction timeout for a Cell whose owning runner is known to the Controller
- **WHEN** the timeout handler runs
- **THEN** the recovery it invokes SHALL either dispatch, or record its inability to dispatch
- **AND** the handler SHALL NOT discard the recovery result

### Requirement: State path resolution SHALL be single-source and coherent

The on-disk location of a Cell's WAL/snapshot/outbox SHALL be computed from a single
authoritative resolver that every reader, writer, and the boot-time migrator agree on, so
that no path divergence can orphan persisted state. If `FLEZHA_STATE_DIR` is set, the
resolver and the migrator SHALL use the same value, and a change of root between boots
SHALL be reconciled — existing state migrated from the old root to the new one — rather
than silently orphaned. The boot migrator SHALL NOT relocate state into a layout (e.g. a
`_system/` namespace prefix) that the reader does not read; layout changes ahead of reader
support are prohibited because they orphan state.

#### Scenario: WAL is not orphaned by a path-resolution change
- **GIVEN** persisted WAL/snapshot files for running Cells
- **WHEN** the Controller restarts after this change
- **THEN** the restore path SHALL find the existing files at the resolver's path
- **AND** no Cell's state SHALL become invisible because the migrator and reader disagree on the path

#### Scenario: Migrator and reader agree on layout
- **WHEN** the boot migrator runs
- **THEN** the directory layout it produces SHALL be exactly the layout `Flezha.State.Paths` reads
- **AND** any layout the reader does not support SHALL NOT be produced by the migrator

### Requirement: Per-Cell WAL SHALL take Changes only from its owner

The Controller's per-Cell WAL SHALL take Changes only from its owner: the runner the last
bind re-seated from the log (by a restore, or by a hand-over onto that instance), or, with no
owner yet (a writer just started, or a correction rewind), the runner whose Change it writes
next. The owner SHALL be compared by stable identity, so a reconnect's new session stays the
owner. A Change from any other runner, or from a session a takeover superseded, SHALL be
refused with its credit released and SHALL change neither the WAL, its snapshots nor its
epoch; one `[:flezha, :state, :stale_writer_refused]` event SHALL be emitted per refused
session. A Change at or below `last_persisted_seq` from the owner SHALL remain a legitimate
dedup. A restarted runner continues the log, since its bind restores it before it starts.

Changes forwarded by a superseded session SHALL be refused at the binding (the superseded
session's channels are unbound during takeover, before the new session is bound); a Change
from a session whose identity the relay's ownership record does not name SHALL be refused at
the stream channel before it reaches the writer, counted and reported as
`runner-identity-and-session` prescribes; no refusal SHALL add any per-record check to the
owning session's append path.

#### Scenario: A runner that is not the owner never rewrites the WAL
- **GIVEN** a Cell whose WAL has advanced to seq N under runner A, with a snapshot
- **WHEN** a runner B that no bind seated sends Changes from a lower seq base
- **THEN** each SHALL be refused, the WAL SHALL stay at seq N with its snapshot, and one `[:flezha, :state, :stale_writer_refused]` event SHALL name B's session

#### Scenario: A same-runner duplicate is still deduped
- **GIVEN** a Cell whose WAL has advanced to seq N under runner A
- **WHEN** runner A re-sends a Change at or below seq N (a retransmit, not a restart)
- **THEN** the Change SHALL be deduped and the WAL SHALL NOT change

#### Scenario: A user-triggered resync resolves the owning runner
- **WHEN** a caller with no runner id of its own triggers a resync for a Cell (`flz state rotate`)
- **THEN** `Flezha.State.Recovery.resolve_runner/1` SHALL resolve the owning runner id from the durable registry entry's metadata tag, keyed by the relay's relay_class (its `:relay_class` opt, defaulting to its id)
- **AND** an output stall SHALL NOT be such a caller: the Watchdog emits only, and the convergence Loop's output-stall ladder owns that remedy (see `convergence-self-healing`)

#### Scenario: A superseded session's divergent frames are refused
- **GIVEN** a Cell whose stable identity was taken over by a new session while the old session's frames are still in flight
- **WHEN** the superseded session's channel forwards a Change with a divergent seq base
- **THEN** the frame SHALL be refused at the binding, the WAL SHALL NOT change, and a `[:flezha, :state, :stale_writer_refused]` event SHALL be emitted once for the episode

#### Scenario: A runner that does not own the relay never writes its WAL
- **GIVEN** a Cell of relay `X` whose ownership record names runner B's stable identity, with the WAL at seq N under B
- **WHEN** a session of another identity forwards a Change for that Cell with a divergent seq base
- **THEN** the frame SHALL be refused at the stream channel, the WAL SHALL stay at seq N with its snapshots, and `[:flezha, :runner, :foreign_frame_refused]` SHALL name the session, B and `X`

### Requirement: Output is persisted before it is emitted (flush-before-emit)

For a persistence-enabled relay, the SDK SHALL drain a just-applied state change to the durable WAL send path BEFORE writing the corresponding output frame, so durable state never lags emitted output by more than the in-flight network window. This is gated on `keeps_state` and `FLEZHA_FLUSH_BEFORE_EMIT` (default on); non-persistent relays keep the zero-cost inline emit. Each change is moved out of the buffer exactly once and seq order is preserved via the shared `change_seq`.

The guarantee is **per dispatch**, and it SHALL be stated that way wherever it is
relied on. A dispatch's records and the outputs that dispatch produced have two
different writers — the Cell ships its own records to the state stream and
hands its outputs to the Relay, which ships them — so an output is one
process-hop behind its record. Across dispatches the SDK SHALL NOT be taken to
promise any interleaving of the two: a later dispatch's records MAY reach the
stream ahead of an earlier dispatch's output, and this SHALL NOT be read as an
output overtaking a record. A reader — a test, a user, or a controller-side
consumer — that asserts a fixed alternation of records and outputs across
dispatches is asserting a coincidence of scheduling, not this requirement.

#### Scenario: durable state does not trail emitted output
- **GIVEN** a persistence-enabled relay applies a state change and then emits output for it
- **WHEN** the dispatch runs with flush-before-emit enabled
- **THEN** the change is shipped to the per-Cell WAL path before the output frame is written
- **AND** on a subsequent hard-kill the restored state is no further behind the emitted output than the single in-flight change.

#### Scenario: a non-persistent relay keeps the inline fast path
- **GIVEN** a relay with persistence disabled
- **WHEN** it emits output
- **THEN** no flush-before-emit drain occurs and the emit path is unchanged.

#### Scenario: An output never leaves ahead of the change that produced it
- **GIVEN** a persistence-enabled relay whose Tasklet applies a change and emits on every dispatch
- **WHEN** many dispatches run, whether their inputs arrive in a burst or one at a time
- **THEN** for every output, the change record of the dispatch that produced it SHALL already be on the state stream, so a crash in the gap can never leave an output in the world with no record of what produced it
- **AND** the output SHALL carry the seq of that record, so which change produced it is readable rather than inferred from arrival order

#### Scenario: A later dispatch's record may overtake an earlier dispatch's output
- **GIVEN** a Cell that completes a second dispatch while the Relay is still writing the first dispatch's outputs
- **WHEN** both dispatches' frames arrive on the state stream
- **THEN** the second dispatch's records MAY precede the first dispatch's output, and this SHALL NOT be treated as a violation, a loss, or a reordering of any dispatch

### Requirement: Per-Cell state is deleted on dynamic-mode eviction

When the SDK evicts a Cell due to dynamic-mode idle-TTL eviction (see `relay-tasklet-cell-model` "Per-Tasklet idle-TTL eviction is opt-in"), eviction SHALL be destructive. If persistence is enabled for the Relay class, the SDK SHALL ship a `delete_cell_state{cell_id}` frame under the durable guarantee class, held for the next bound stream when none is bound, so the agent removes the per-Cell WAL, snapshot, and outbox files (`{key}.wal`, `{key}.snap.{seq}`, `{key}.outbox.dedup` under `storage/app/state/{relay}/{tasklet}/`). A subsequent input for the same key SHALL cold-start a fresh Cell (new WAL from seq 0); the SDK SHALL NOT restore the evicted Cell's state from disk.

#### Scenario: Eviction deletes on-disk state
- **WHEN** a persistence-enabled dynamic-mode Cell is evicted
- **THEN** the SDK SHALL ship `delete_cell_state{cell_id}` and the agent SHALL remove the WAL, snapshot, and outbox files for that cell_id

#### Scenario: Recreate after eviction cold-starts
- **WHEN** a dynamic-mode Cell is evicted and a subsequent input arrives for the same key
- **THEN** the SDK SHALL recreate the Cell via `init_state(Some(key), None)` and a new WAL SHALL start from seq 0; no restore-from-disk SHALL occur

### Requirement: Per-Cell state delete RPC

The agent SHALL accept a `delete_cell_state` data-stream frame whose body is the structured `cell_id`. On receipt the agent SHALL: (1) stop any per-Cell `WalWriter` for that cell_id and drop its pending `FsyncBatcher` dirty mark; (2) remove the `{key}.wal`, `{key}.snap.{seq}` (all sequences), and `{key}.outbox.dedup` files. Delete SHALL be idempotent: a delete for a cell_id with no files SHALL succeed.

The delete is reachable via two paths:

- The SDK reaper, for evicted persistence-enabled Cells. The reaper waits for nothing and the agent writes no response frame back on the data stream; a refused write is answered by the frame's durable guarantee class, never at the reaper's write site.
- The CLI command `flz state delete <relay/tasklet/key>`, which calls `Flezha.State.Supervisor.delete_cell/1` directly and returns an `{cell_id, status}` confirmation synchronously.

#### Scenario: Delete removes existing files
- **WHEN** the agent receives `delete_cell_state` for a cell_id that has WAL and snapshot files on disk
- **THEN** the agent SHALL stop the per-Cell processes and remove all three file types

#### Scenario: Delete is idempotent for nonexistent cell
- **WHEN** the agent receives `delete_cell_state` for a cell_id with no on-disk state
- **THEN** the agent SHALL succeed without error (the delete is a no-op)

#### Scenario: CLI delete returns confirmation
- **WHEN** a user runs `flz state delete <relay/tasklet/key>` against a Cell with on-disk state
- **THEN** `Flezha.State.Supervisor.delete_cell/1` SHALL remove the files and return `{:ok, %{cell_id, status: :ok}}`, and the CLI SHALL exit 0

### Requirement: Atomic effect record type in the per-Cell WAL

The per-Cell WAL SHALL support an additional record type carrying an
invocation's state change(s), its outbox entries (channel, key, user metadata,
payload, emit ordinal), and its consumed input offset as one crc-protected record
appended and fsynced as a unit. A Cell of a flow class on the effectively-once tier and a
Cell of a durable steps class SHALL both write it, the steps relay Cell with no consumed offset;
a plain flow relay Cell never produces effect records. Existing change-record and snapshot requirements
are unchanged. Restore and
time-travel readers SHALL fold the state-change portion of effect records exactly
as they fold plain change records, for either kind.

#### Scenario: Effect record replays as state

- **WHEN** a Cell with effect records in its WAL is restored via the restore
  handshake
- **THEN** the assembled payload folds the change portions in seq order and the
  restored state equals the pre-crash state

#### Scenario: A steps relay Cell's WAL holds effect records

- **WHEN** a durable steps relay Cell applies a change and emits in one invocation
- **THEN** its WAL holds one effect record for the invocation, and the restore handshake folds its change portion into the Cell's state exactly as it folds a flow Cell's

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
WAL append is the only per-record durable write, except that the first record
a writer takes for a Cell whose files landed from elsewhere, or whose lineage
record holds no id, SHALL first write that Cell's lineage record once. The dedup file's cursors
(`last_persisted_seq` and `outbox_flushed_seq`) are a rebuildable cache and
SHALL be advanced at most at fsync-batch cadence, and on snapshot, rotation,
and clean shutdown. On boot, a missing or stale dedup file SHALL be repaired
by recomputing from the WAL itself; a stale `outbox_flushed_seq` follows the
re-drive rule (never skip ahead).

#### Scenario: Appends produce one durable write each

- **WHEN** N changes are appended within one fsync batch
- **THEN** exactly N WAL appends and at most one dedup-file update occur — no
  per-record tmp+rename; the first append after a Cell's files landed also
  writes its lineage record, once

#### Scenario: Stale dedup file is rebuilt from the WAL

- **WHEN** the controller crashes after appends that outran the last dedup-file
  update
- **THEN** boot recomputes `last_persisted_seq` from the WAL tail and dedup
  behaviour is identical to a per-record-persisted watermark

### Requirement: Outbox flush cursor lives beside the persistence watermark

The per-Cell dedup file SHALL carry both the existing `last_persisted_seq`
watermark and the new `outbox_flushed_seq` cursor, updated with the same
atomic tmp+rename discipline. Its epoch and folded offset SHALL be copies of
the Cell's lineage record's, kept in their existing positions so a build that
predates the lineage record reads the file unchanged; a reader SHALL take the
higher epoch of the two records. On restart, a missing or stale cursor SHALL be
treated as "re-drive from the last known value" (never as "skip ahead").

#### Scenario: Stale cursor re-drives, never skips

- **WHEN** the dedup file's `outbox_flushed_seq` is older than the newest durable
  effect record after a crash
- **THEN** the controller re-drives all effect-record outputs above the cursor
  and never advances the cursor past undelivered entries

#### Scenario: A Cell with no lineage record yet keeps its dedup facts

- **WHEN** a writer opens a Cell whose only small file is a dedup file holding epoch 2 and folded offset 17
- **THEN** the writer SHALL serve with epoch 2 and folded offset 17, SHALL write the Cell's lineage record before its next dedup file, and that dedup file SHALL keep the same 32-byte layout

#### Scenario: A rolled-back build reads the dedup file

- **WHEN** a build that predates the lineage record opens a Cell this change wrote
- **THEN** it SHALL read the same `last_persisted_seq`, `outbox_flushed_seq`, epoch and folded offset the newer build wrote, and SHALL re-drive only what lies above that flush cursor

### Requirement: The restore contract SHALL state the fate of queued input

The restore contract SHALL state what happens to a Cell's queued input, and
SHALL make the answer depend on whether the Controller will re-forward it.

The `RestoreInitState` requirement describes how a Cell's state is rebuilt
but says nothing about the input a live Cell already holds queued when the
restore lands. The SDKs consequently disagree, the Rust SDK discards it and the
Java SDK preserves it, with neither behaviour documented as intended.

The Controller re-forwards input only for instances whose inputs it journals
before forwarding, into the one durable input log: a flow relay on the
effectively-once tier and a durable steps instance. `Ingest.journal_input/3`
passes every other relay through un-journaled, including a plain stateful
flow relay, the default configuration, since persistence defaults to
enabled and the tier is an explicit opt-in.

A re-forward SHALL NOT be issued onto a data stream that has not yet bound. The
Controller re-forwards after a rebind, and a fresh runner binds its primary data
stream after the control-channel handshake completes; a re-forward issued in
that window is dropped and never re-driven, which loses exactly the input the
restore exists to preserve. The re-forward SHALL therefore be gated on the bind,
using the same mechanism that delivers a journaled input accepted before a
relay's first bind: one re-forward path for both kinds, not one per kind.

#### Scenario: The contract distinguishes journaled from un-journaled classes

- **WHEN** the restore contract is consulted for the fate of queued input
- **THEN** it SHALL give different answers for an instance whose inputs are
  journaled and one whose inputs are not, and the same answer for a journaled
  unbounded and a journaled steps relay

#### Scenario: Un-journaled queued input survives a restore

- **GIVEN** a live Cell of a class whose inputs the Controller does not
  journal, holding queued input
- **WHEN** the Controller drives a `RestoreInitState` handshake for that Cell
- **THEN** the queued input SHALL NOT be silently lost

#### Scenario: A restore-time discard is observable to a user

- **WHEN** a restore discards queued input
- **THEN** the discard SHALL be counted and exposed, naming the Cell, rather
  than leaving no trace

#### Scenario: A re-forward onto an unbound stream is not lost

- **GIVEN** a rebind whose fresh runner has completed the control-channel
  handshake but has not yet bound its primary data stream
- **WHEN** the Controller re-forwards an instance's journaled offsets
- **THEN** those offsets SHALL reach the tasklet once the data stream binds,
  and SHALL NOT be dropped

#### Scenario: The restore and first-bind re-forwards share one path

- **WHEN** the re-forward issued after a restore, the re-forward issued at a
  relay's first data-stream bind, and the re-forward issued when a
  steps relay rehydrates are compared
- **THEN** they SHALL be the same code path, differing only in what triggers them

### Requirement: The compaction handshake SHALL be verified end to end against a real SDK

The existing compaction requirements describe an SDK obligation that no SDK has ever implemented, and the test suite has never involved a real SDK on this path — controller-side tests stub `:state_resync_fun` or `ControlChannel.send_control` and assert on telemetry alone. The system SHALL carry end-to-end coverage, on each SDK arm, that fails when the SDK does not produce a snapshot.

The coverage SHALL assert against observable durable state — a snapshot file existing and the WAL shrinking — and SHALL NOT be satisfied by telemetry assertions alone.

#### Scenario: A triggered compaction produces a durable snapshot and truncates the WAL

- **GIVEN** a live runner hosting a Cell whose Tasklet has a non-`NoChange` Change type, with persistence enabled and a WAL containing records
- **WHEN** the Controller ships `CompactCell` for that Cell and the SDK answers
- **THEN** a snapshot file SHALL exist for that Cell under its state directory
- **AND** the Cell's WAL size SHALL be strictly smaller than it was before the handshake
- **AND** `[:flezha, :state, :compaction_completed]` SHALL have been observed for that Cell

#### Scenario: A stub SDK response fails the coverage

- **GIVEN** an SDK build whose `CompactCell` handler answers `snapshot_seq = 0` without shipping a `cell_state_snapshot` frame
- **WHEN** the end-to-end compaction coverage runs against it
- **THEN** the coverage SHALL fail

#### Scenario: The cold-controller baseline-seed pull becomes reachable

- **GIVEN** the archived `snapshot-baseline-durability` change, whose cold-controller "pull a full-state baseline via `CompactCell`" path was inert because no SDK ever answered the handshake with a snapshot
- **WHEN** a cold controller with an empty WAL for a live runner's Cell issues `CompactCell` (via `Recovery.dispatch_resync`'s empty-WAL branch)
- **THEN** the SDK SHALL now answer with a real `cell_state_snapshot`, so the baseline-seed scenario that change asserts becomes reachable for the first time
- **AND** this change's requirements — not an amendment to the archived change — are the current authority for that handshake (the archived change is left as the historical record)

### Requirement: The Controller SHALL inspect CompactCellResponse

`Flezha.State.Commands.dispatch_compact_cell/2` currently treats a successful `send_control` as compaction having started and never reads the response body. The Controller SHALL inspect `CompactCellResponse` and SHALL treat a response that reports no snapshot for a Cell with a non-empty WAL as a failure of the handshake, not as its completion.

#### Scenario: Zero snapshot_seq against a non-empty WAL is an error

- **GIVEN** a Cell whose WAL contains at least one record
- **WHEN** the SDK answers `CompactCellResponse{snapshot_seq: 0, snapshot_bytes_size: 0}`
- **THEN** the Controller SHALL record the handshake as failed, SHALL emit failure telemetry naming the Cell, and SHALL NOT mark the compaction as started or completed

#### Scenario: Zero snapshot_seq against an empty WAL is not an error

- **GIVEN** a Cell whose WAL contains no records
- **WHEN** the SDK answers `CompactCellResponse{snapshot_seq: 0}`
- **THEN** the Controller SHALL NOT treat the response as a failure

### Requirement: A failing compaction SHALL be bounded by strikes and give up loudly

`Watchdog.flag_compaction_timeout/2` currently calls `Recovery.resync/2` without a `runner_id`, so `do_resync/2` returns `{:error, :no_runner_id}` and the caller discards it — the timeout handler is a silent no-op, and the Cell is dropped from the in-flight map so the next sweep re-triggers it indefinitely. Repeated compaction failure for a Cell SHALL be counted, SHALL back off between attempts, and SHALL escalate to a loud, user-visible give-up rather than retrying at the sweep interval forever.

#### Scenario: Repeated failures back off rather than retrying at sweep cadence

- **GIVEN** a Cell whose compaction handshake fails or times out
- **WHEN** the failure repeats
- **THEN** the interval between successive `CompactCell` dispatches for that Cell SHALL increase
- **AND** the Controller SHALL NOT dispatch `CompactCell` for that Cell once per sweep indefinitely

#### Scenario: Give-up is observable

- **GIVEN** a Cell whose compaction has failed past the strike threshold
- **WHEN** the threshold is crossed
- **THEN** the Controller SHALL emit give-up telemetry naming the Cell and the failure count, at a severity a user can alert on

#### Scenario: A recovery attempt that cannot dispatch is not silently discarded

- **WHEN** a compaction failure path invokes recovery without the information needed to dispatch it
- **THEN** the outcome SHALL be logged or counted; it SHALL NOT be discarded

### Requirement: The per-Tasklet compact_after_bytes threshold SHALL be reachable

`PersistenceResolver.compact_after_bytes/2` names its first parameter `_tasklet_id` and discards it, so the per-Tasklet threshold the existing compaction requirement mandates cannot be resolved by any caller — the global default always wins. The resolver SHALL consult the per-Tasklet value, and every caller SHALL pass the arguments needed to resolve it.

#### Scenario: A declared per-Tasklet threshold governs its own Cells

- **GIVEN** a Tasklet that declares a `compact_after_bytes` different from the global default, shipped in the `AddRelay` metadata
- **WHEN** the Watchdog evaluates the size trigger for a Cell of that Tasklet
- **THEN** the declared value SHALL be used, not the global default

#### Scenario: An undeclared Tasklet falls back to the global default

- **GIVEN** a Tasklet that declares no `compact_after_bytes`
- **WHEN** the Watchdog evaluates the size trigger
- **THEN** the global `:default_compact_after_bytes` SHALL be used

### Requirement: The persistence contract SHALL have a runnable smoke check

`test/scripts/state_persistence.sh` SHALL run to completion against the dev
compose stack and SHALL assert the data-plane half of this capability: with
a relay running, plugins attached and inputs flowing, WAL files appear
under the namespaced state layout and `flz state list <class>` surfaces the
live cell.

The script SHALL drive the plugin-attach API that currently exists rather
than a removed one, and SHALL start from a clean state on every run, so a
green result is evidence about the contract and not about leftovers from a
previous run.

Where the script cannot run in CI, its manual status and its measured
runtime SHALL be recorded next to it, so a later reader can tell "nobody
ran it" from "it passes".

#### Scenario: The smoke check passes from a clean state

- **WHEN** the script is run twice in succession, each run starting from wiped volumes
- **THEN** both runs complete successfully and assert the WAL layout and the live-cell listing

#### Scenario: A removed API is not driven

- **WHEN** the script attaches a plugin to a relay
- **THEN** it passes only options the provider currently accepts, and the attach succeeds

#### Scenario: Manual status is recorded

- **WHEN** the script is not wired into an automated tier
- **THEN** its manual status and measured runtime are recorded next to the script and in the docs that reference it

### Requirement: A refused restore fails the bind instead of cold-starting the Cell

`RestoreInitState` is an awaited control round trip, and the controller SHALL act on its answer. When the SDK reports a restore failure — a language mismatch, a schema mismatch, a CRC failure, a truncated change log, or an apply error — the controller SHALL NOT proceed as though the Cell had no state. It SHALL leave the relay unbound, SHALL log and emit telemetry naming the cell, the runner and the reported reason, and SHALL surface it to the convergence loop so the relay is visible as unhealthy rather than silently serving from empty state.

A restore that reports no state to restore (a Cell with an empty WAL and no snapshot) is a success, not a failure, and SHALL bind normally.

A round trip that never completed — a timeout, a control channel that is gone, a runner shutting down — is NOT a refusal: the SDK reported nothing, so there is no verdict on the state, and the controller SHALL leave that case to the rebind path that already owns a runner which stopped answering. Only a failure the SDK itself reports parks the relay.

The controller SHALL NOT discard the result of an awaited restore at any call site.

A torn tail the reader repairs under the proof in "A durable record that fails its crc32 SHALL be refused, and a truncation SHALL be proved safe before it happens" is a repair, not a restore refusal: the append protocol's own crash leaves no complete record behind the failure point, so the Cell binds on the records that were fully written. A crc failure with complete records behind it is the refusal this requirement governs, and the controller's own reader SHALL produce that condition rather than deleting the record before the SDK can report it.

#### Scenario: A refused restore does not cold-start

- **GIVEN** a Cell with a populated WAL whose records the bound SDK refuses
- **WHEN** the controller awaits `RestoreInitState` and receives the refusal
- **THEN** the relay SHALL NOT be bound, the failure SHALL be logged with the reported reason, and the Cell SHALL NOT begin folding new inputs on top of empty state

#### Scenario: A genuinely empty Cell still binds

- **GIVEN** a Cell with no snapshot and a zero-length WAL
- **WHEN** the controller awaits `RestoreInitState`
- **THEN** the SDK SHALL answer successfully with a terminal seq of zero and the relay SHALL bind and serve

#### Scenario: An unanswered restore is not a refusal
- **GIVEN** a Cell whose runner's control channel dies before the restore is answered
- **WHEN** the controller awaits `RestoreInitState` and the round trip fails without an SDK reply
- **THEN** the relay SHALL NOT be parked for a restore refusal, because nothing was reported about its state; the runner's disappearance is handled by the rebind path

#### Scenario: A corrupt WAL is loud, not silent

- **GIVEN** a Cell whose WAL holds a record that fails its record framing CRC with complete records behind it
- **WHEN** a restore is attempted
- **THEN** the relay SHALL remain unbound with a reason naming the CRC failure, rather than binding on partial state, and the corrupt record and every record behind it SHALL still be on disk

### Requirement: A durable record that fails its crc32 SHALL be refused, and a truncation SHALL be proved safe before it happens

A durable record whose framing crc32 does not verify SHALL be refused by name, and the refusal SHALL carry the file it was read from, the last seq that verified before it and the byte offset of the failure.

The reader already distinguishes a crc failure from a read that runs off the end of the file and from a length field below the header size. That distinction SHALL be carried to the caller. A reader SHALL NOT collapse the three into one error, and SHALL NOT answer a caller with the records it managed to read before a failure as though the log had ended there.

A reader SHALL truncate a record log only where it can prove that no complete record can begin after the failure point. A crc failure whose framed extent ends before the end of the file has acknowledged records behind it and SHALL NOT be truncated: records the Controller acknowledged as durable SHALL NOT be deleted to make a log parse. Where the proof holds, the truncation is the torn tail the append protocol produces and SHALL still happen — and SHALL be reported rather than performed silently.

Every read of a record log SHALL apply this rule, whichever entry point reaches it: opening a log for appends, folding one for its records, locating a retention or rewind boundary in one, and assembling a restore payload from one.

A refusal SHALL be observable without attaching a tracer: it SHALL emit one telemetry event and one log line at error severity. A durable-record loss reported to nobody is the defect this requirement exists to prevent.

A refusal SHALL leave the bytes on disk untouched and SHALL leave the relay unbound rather than serving from the records that happened to precede the failure, which is the fate "A refused restore fails the bind instead of cold-starting the Cell" already assigns to a CRC failure the SDK reports.

#### Scenario: A corrupt record mid-log does not delete the records after it

- **GIVEN** a WAL holding three records where the second record's body has one flipped bit
- **WHEN** the writer opens the log
- **THEN** the open SHALL refuse and name the seq and offset, the third record SHALL still be present on disk, and the file's byte length SHALL be unchanged

#### Scenario: A torn tail is still repaired, and now says so

- **GIVEN** a WAL whose last record is a short write from a crash mid-append
- **WHEN** the writer opens the log
- **THEN** the partial tail SHALL be truncated, the preceding records SHALL be intact, and the repair SHALL be reported with the byte count discarded

#### Scenario: A restore payload is refused rather than shipped short

- **GIVEN** a Cell whose WAL holds a corrupt record below its terminal seq
- **WHEN** the controller assembles `RestoreInitState` for that Cell
- **THEN** it SHALL refuse rather than ship the records before the corrupt one as the whole change log, and the relay SHALL NOT bind on that truncated history

#### Scenario: A rewind boundary is not located past a corrupt record

- **GIVEN** a Cell whose WAL holds a corrupt record below the seq a rewind cuts to
- **WHEN** the rewind locates its boundary
- **THEN** it SHALL refuse rather than cut at the corrupt record's offset and delete the valid records between it and the intended boundary

#### Scenario: A refusal reaches an operations surface

- **WHEN** any durable read refuses a record for a crc failure
- **THEN** one telemetry event and one error log line SHALL name the file, the seq and the offset, and the refusal SHALL be countable without a tracer

#### Scenario: Every runtime answers a corrupt record the same way

- **WHEN** the Controller's reader and the kernel's change-log fold are each given the same corrupt record
- **THEN** both SHALL refuse by name, and none SHALL return a partial result as success

### Requirement: The durable record frame SHALL have exactly one reader and one writer

Every read and every write of the durable record framing `[seq: u64 BE][len: u32 BE][header: 16 B][change_bytes][crc32: u32 BE]` in the Controller SHALL go through `Flezha.State.Frame`, the Controller's frame module, on disk and over a binary alike; `Flezha.State.RecordFrame` walks a record log on a device through it.

A module that holds framed records SHALL NOT decompose the framing itself, SHALL NOT compute the framing crc32 itself, and SHALL NOT restate the header's byte length. This applies to the Controller's restore path.

The crc32 over `header <> change_bytes` SHALL be written and verified in one expression. Two spellings of the same value — `crc32(header <> body)` and `crc32(crc32(header), body)` — are the same crc computed twice, and only one of them SHALL remain.

The kernel's reader (`flz_core::restore::fold_change_log`) is the second runtime and SHALL NOT share code with the Controller's seam. The two SHALL share a byte fixture instead: golden record bytes, stored once, read by both runtimes' tests, so that a framing change on either side fails on both.

#### Scenario: One walk, and the documentation that says so is true

- **WHEN** the readers of the durable record framing are enumerated
- **THEN** exactly one module SHALL decompose it, and the published statement that `RecordFrame.fold_records/3` is the one walk every reader uses SHALL be true of the tree

#### Scenario: A byte-oriented reader uses the same decoder as a device-oriented one

- **GIVEN** a restore that has read a whole log into memory and an input log that reads a file device
- **WHEN** each folds its records
- **THEN** both SHALL reach the same decoder, and a record decoded from bytes and the same record decoded from a device SHALL yield identical `seq`, header and body

#### Scenario: Both runtimes agree on the bytes

- **WHEN** the shared frame fixture is encoded by the Controller's seam
- **THEN** the kernel's reader SHALL fold those exact bytes to the same records, and a change to either encoder SHALL fail the other runtime's test

#### Scenario: A second frame reader cannot be added quietly

- **WHEN** a module outside the seam decomposes the record framing or computes its crc32
- **THEN** the declaration test SHALL fail and name the module

### Requirement: A durable file SHALL be installed by one crash-atomic ceremony and SHALL declare its durability

Every whole-file install of durable data SHALL use the one install function, which writes to an in-progress path, fsyncs the descriptor, renames onto the final path, and removes the in-progress file on any failure.

What varies between callers SHALL be read from the durable file's declaration rather than being a constant inside the function: the in-progress suffix it writes under, whether the install owes an fsync of its own, and whether it creates the directory it writes into. A caller that needs a different suffix uses the ceremony instead of re-implementing it.

A runtime packaged separately from the Controller is out of this ceremony, and the reason SHALL be stated where its install lives: it depends on no module of the Controller, the ceremony's included, so an install of its own is that boundary holding rather than a bypass.

Every module that owns durable bytes on disk SHALL declare, in one table, the frame it uses (`record` or `whole-file`), its durability mode (batched through the shared fsync cadence, or fsynced inline) and its in-progress suffix. The scanners that must ignore in-progress files SHALL derive their exclusions from those declared suffixes rather than listing them, so that a new durable file cannot be surfaced as a phantom Cell by omission.

An ownership marker that shares a directory with durable data — a fencing claim file — is not an in-progress file and SHALL stay excluded by name, with the reason stated.

#### Scenario: Every install is the same ceremony

- **WHEN** the tree's whole-file installs of durable data are enumerated
- **THEN** each SHALL call the one install function, and none SHALL open, write, sync and rename by hand

#### Scenario: A crash mid-install leaves the previous file

- **GIVEN** a durable file being replaced
- **WHEN** the process dies after the in-progress write and before the rename
- **THEN** the final path SHALL still hold the previous contents, and the in-progress file SHALL be ignored by every scanner

#### Scenario: A new durable file is a declaration

- **WHEN** a module gains durable bytes on disk
- **THEN** adding its row to the declaration SHALL be sufficient for the fsync cadence and for the scanners' exclusions, with no second edit in a scanner

#### Scenario: The declaration and the tree cannot disagree

- **WHEN** a module declares itself durable and is absent from the table, or a table row names no module
- **THEN** the declaration test SHALL fail and name the discrepancy

### Requirement: A Cell's durable cursors SHALL be one record with one stated ordering

The four cursors a Cell's writer carries — the last persisted seq, the outbox durable seq, the outbox flush cursor and the epoch they are counted under — SHALL be read through one accessor that returns them together, and a reader SHALL NOT copy the individual fields out of the writer's state. The epoch is the higher of the Cell's lineage record's and its dedup file's; the accessor SHALL read both, from disk when no writer runs.

The ordering the cursors assume SHALL be stated once at the value and asked before the record is written, not restated by each reader: an output is confirmed only after its record is fsynced and a record is fsynced only after it is appended, so the flush cursor SHALL NOT exceed the durable seq and the durable seq SHALL NOT exceed the last persisted seq. Advance SHALL be monotone; a rewind SHALL happen only under an epoch bump.

The snapshot cursor is deliberately outside that chain. It counts how much state a compaction folded away and the flush cursor how much output was confirmed, so a Cell that emits nothing snapshots past a flush cursor that never leaves its empty-log floor; ordering the two against each other would be false of every Cell outside the effectively-once tier.

A cursor's name SHALL mean one thing. Where several names denote "the last durable seq", they SHALL resolve to one field of the record rather than to independent copies that can drift.

#### Scenario: A reader reads the record, not the fields

- **WHEN** an operations surface, a time-travel query, a hydrator or a replay needs a Cell's progress
- **THEN** it SHALL call the one accessor, and the writer's individual cursor fields SHALL NOT appear in its module

#### Scenario: The ordering is enforced where the value lives

- **WHEN** the cursor record is about to be persisted
- **THEN** the ordering SHALL be asked of the record, and a record that does not hold it SHALL be refused and reported rather than written, leaving the older record that does

#### Scenario: A rewind happens only under an epoch bump

- **WHEN** a cursor moves backwards
- **THEN** the move SHALL be attributable to an epoch bump, and any other backwards move SHALL be refused

### Requirement: The input-silence sweep is off by default and reports as a warning

The Controller's input-silence sweep SHALL be disabled by default, SHALL be named for the observation it makes rather than for a conclusion about severity, and SHALL report at warning level when enabled.

The sweep observes one thing: a relay whose `last_persisted_seq` advanced at
some point has received no Change for the whole window. That is silence on the
input side. The relay is silent only when none of its cells that produced
output has received a Change in the window: a cell that produced once and has
nothing new to hear, beside cells that keep hearing, does not make its relay
silent. Whether the silence means a dead upstream, a source that reports hourly,
or a quiet night is precisely what the sweep cannot determine, so it SHALL NOT
report a conclusion its evidence does not support. The convergence judge already holds
this rule for the same observation: a flat marker with no input-side advance is
idle, and idle is healthy.

The sweep SHALL consider an instance of either kind, and SHALL skip an instance that is
sleeping (its park record is present in the class registry), because a sleeper receives no
Change by design; it SHALL also keep skipping a checkpoint-ack class between checkpoints. It
SHALL NOT skip an instance for its kind.

The knob SHALL be `input_silence_timeout_ms`, defaulting to `0`, which the sweep
already reads as "do not run", and settable from the environment as
`FLEZHA_INPUT_SILENCE_TIMEOUT_MS`. The telemetry event SHALL be
`[:flezha, :state, :input_silence]`. Both are plain renames of the previous
`catastrophic_dataflow_*` spellings, with no acceptance of the old names.

#### Scenario: A quiet relay raises nothing by default

- **GIVEN** a flow relay that produced output earlier and has received no input for an hour
- **WHEN** the watchdog sweeps with `input_silence_timeout_ms` unset
- **THEN** no event SHALL be emitted and no line SHALL be logged

#### Scenario: A user can turn it back on

- **GIVEN** `input_silence_timeout_ms` set to a positive value
- **WHEN** a flow relay that produced output earlier receives no input for longer than that value
- **THEN** `[:flezha, :state, :input_silence]` SHALL be emitted once for that relay class, and one line SHALL be logged at warning level

#### Scenario: A quiet cell beside a live one does not make its relay silent

- **GIVEN** `input_silence_timeout_ms` set to a positive value and a relay with two cells that produced output
- **WHEN** one cell receives no Change for longer than the window while the other keeps receiving them
- **THEN** no `input_silence` event SHALL be emitted for the relay, and one SHALL be once the other cell falls silent too

#### Scenario: A container sets it from the environment

- **GIVEN** `FLEZHA_INPUT_SILENCE_TIMEOUT_MS=120000` in the controller's environment
- **WHEN** the configuration is read
- **THEN** `input_silence_timeout_ms` SHALL be `120000`

#### Scenario: The message states what was observed

- **WHEN** the sweep reports a relay
- **THEN** the logged line SHALL state that no input was received within the window and that output was produced earlier, and SHALL NOT assert that data flow has stopped

#### Scenario: Resumed input clears the latch

- **GIVEN** a relay the sweep has reported
- **WHEN** a Change arrives for it and the sweep runs again
- **THEN** the latch SHALL clear so a later silence is reported afresh

#### Scenario: A sleeping instance is never flagged

- **GIVEN** `input_silence_timeout_ms` set to a positive value and an instance of either kind that produced output, then slept
- **WHEN** the watchdog sweeps after the window has elapsed with the instance still asleep
- **THEN** no `input_silence` event SHALL be emitted for it

#### Scenario: An awake silent steps relay is flagged like a flow relay

- **GIVEN** `input_silence_timeout_ms` set to a positive value and an awake `per_input` steps relay whose class does not sleep and which produced output earlier
- **WHEN** it receives no Change for longer than the window
- **THEN** `[:flezha, :state, :input_silence]` SHALL be emitted once for it

### Requirement: Every persistent Cell's records carry the offset it consumed through, and the Cell skips at that floor

A Cell of a persistent class SHALL write the input-log offset it consumed through in the same
durable record as the state that input produced: the effect record under the effectively-once
tier, and outside the tier every change record the Cell drains, carrying the highest offset
folded into that record after coalescing. No other record SHALL hold a Cell's consumed offset. A
controller boot scan, a hydration and a restore SHALL read a Cell's consumed offset from its last
durable record of either kind. A prefix cut SHALL keep the highest offset the records it drops
carried in the Cell's dedup file, and the boot scan, a sleeper's seed, a rewind and a correction's
baseline at or above that cut SHALL take the higher of that and its records'; a Cell whose records and dedup file carry none has consumed
nothing the log still holds.

Every persistent Cell SHALL keep a consumed floor: restored from its durable offset on
`RestoreInitState`, and advanced when the Cell takes an input stamped with a higher `flz.offset`.
A Cell SHALL drop an input stamped at or below its floor without running it, emitting or
changing state. A class whose persistence is off SHALL keep no floor and SHALL be unchanged.

The record that carries a Cell's consumed offset SHALL also keep every inter-tasklet value the
input produced, with that offset as its origin, and a bind SHALL deliver each kept value whose
origin is above the instance's floor to its consumer ahead of the re-forwarded tail. A consumer
SHALL drop a delivered value at or below its own floor.

#### Scenario: A compaction that folded every offset-bearing record keeps the offset

- **WHEN** a Cell's only offset-bearing record is cut by a compaction and the controller restarts
- **THEN** the boot scan and a sleeper's seed SHALL report that record's offset, and a rewind whose surviving records carry none SHALL keep it

#### Scenario: A plain persistent Cell restores its floor

- **WHEN** a Cell of a sleeping class that does not declare the tier has durably folded offsets 0..2 and is restored after a controller restart
- **THEN** its restored floor SHALL be 2, and re-forwarded offsets 0..2 SHALL change nothing and emit nothing

#### Scenario: A torn tail cannot split a change from its offset

- **WHEN** the WAL's last change record is torn by a crash
- **THEN** the Cell's durable offset SHALL be the one carried by the last intact change record, and the torn record's input SHALL be owed again

#### Scenario: A coalesced record carries the highest folded offset

- **WHEN** a Cell folds offsets 10, 11 and 12 into changes the drain coalesces into one record
- **THEN** that record SHALL carry offset 12

#### Scenario: A value lost from a plain consumer's mailbox is delivered again

- **WHEN** a producer of a sleeping class that does not declare the tier has durably recorded offset 7, its edge value was admitted to the consumer's mailbox, and the runner dies before the consumer's record for offset 7 is durable
- **THEN** after the restore the producer SHALL drop the re-forwarded offset 7, the consumer SHALL receive the value from the producer's record, and its state SHALL reflect offset 7 once

#### Scenario: A live floor drops a repeated input

- **WHEN** a bound Cell has taken offset 12 and the same record at offset 12 reaches it again from a second bind
- **THEN** the Cell SHALL drop it, and its state and outputs SHALL be as if it arrived once

### Requirement: A Cell's lineage record SHALL be written before the cursors that count under it

Each persistent Cell SHALL keep a lineage record, `cell_key.lineage`, installed by the
crash-atomic ceremony with an fsync of the file and then of its directory: the Cell's lineage id
(or none), the trail of the lineage ids it continues each with the seq it continued from (at most
16, newest first), the epoch, the folded offset, whether its files landed from another
controller's copy, and the cut a bump has not yet completed. It SHALL be written only by a bump (a
rewind, a correction), a mint, a prefix cut, a landing, or the first write of a Cell
that has none, always before the dedup file or the WAL change that depends on it, and by a
landing only after the Cell's data files landed. It SHALL be written by the Cell's running
writer, or with none running under the Cell's file lock, which the offline cut and a landing
hold and a starting writer waits for. A writer SHALL write
neither record from state it did not load: a writer that refused its file at open SHALL leave
both files as it found them. A crash between the lineage write and the writes after it SHALL
leave a Cell that boots to the state the operation was making, and SHALL NOT re-drive an output
of a record a bump discarded.

#### Scenario: A refused writer leaves both records untouched

- **WHEN** a writer refuses its WAL at open because a record failed its crc with complete records behind it, and is then closed
- **THEN** the Cell's lineage record and dedup file SHALL be byte for byte what they were before the writer started

#### Scenario: A rewind interrupted after its lineage write completes at boot

- **WHEN** the controller crashes after a rewind to seq 3 under epoch 6 wrote its lineage record and before it cut the WAL holding records 0..7
- **THEN** the next boot SHALL cut the records above 3 and the snapshots above 3, lower the instance's input-log floor as the live rewind does, and write a dedup file at epoch 6, and a correction re-run at epoch 6 SHALL change nothing

#### Scenario: A prefix cut interrupted after its lineage write loses nothing

- **WHEN** the controller crashes after a prefix cut wrote the folded offset and before it renamed the cut file
- **THEN** the WAL SHALL hold every record it held before, and the Cell's floor SHALL be what it was before the cut

#### Scenario: A mint interrupted before its record is kept

- **WHEN** the controller crashes after a mint wrote the new lineage id and before the record that caused it was appended
- **THEN** the Cell SHALL boot with the new id and no record past its fork, the record SHALL be appended when it arrives again, and no second id SHALL be minted for it

#### Scenario: A failed mint fails the append

- **WHEN** the lineage install of a mint fails
- **THEN** the record SHALL NOT be appended, the writer SHALL exit under its supervisor, and the record SHALL reach the restarted writer by the path any failed append takes

#### Scenario: A hydration that fails writes no lineage record

- **WHEN** a hydration of a Cell fails to fetch one of its artifacts
- **THEN** no lineage record and no folded offset SHALL be written for that Cell

### Requirement: A Cell's lineage id SHALL be minted at its first record after its files land

A landed Cell SHALL keep the lineage id and trail its files carried, and SHALL mint a new one at
its first record: a Cell whose files landed on a controller from elsewhere (a hydration from the
durability backend of an entry another controller wrote, or a pull from another controller)
SHALL mint a new lineage id, continuing the old one in its trail, when its writer takes its
first record there; a hydration or a pull alone SHALL NOT mint, and a Cell hydrated from an entry
this controller wrote SHALL NOT be marked landed. A Cell whose lineage record holds no id SHALL
mint one at its first record. A restart, a rebind or a rewind on the same
controller SHALL NOT mint. Minting SHALL change neither the epoch nor any output's `flz.epoch` or
`flz.seq`. When a relay arrives and its local copy is compared with the newest copy (the one on
the controller it moves from, else the backend's), on every reference both hold the newest copy
SHALL replace the local one when its lineage continues the local one, or when the two lineages
differ and the local one does not continue the newest copy from a seq at or above that copy's
top, whatever their lengths; two copies of one lineage, or a copy with no id, SHALL be compared
by epoch and then the highest seq, as before.

#### Scenario: A continued history wins over a longer abandoned one

- **WHEN** controller A shipped a Cell through seq 3 and kept records 4..7 unshipped, and controller B hydrated the cut at 3, took records 4 and 5 and shipped them, and the relay returns to A
- **THEN** A SHALL replace its copy with B's, whose WAL holds B's records 4 and 5, and A's records 4..7 SHALL be gone with the set-aside copy

#### Scenario: A hydration that takes no turn does not fork the lineage

- **WHEN** controller B hydrated a Cell's cut at 3 and the relay moved on before B took a record, and the relay returns to A, which kept records 4..7 past that cut
- **THEN** both copies SHALL carry the same lineage id, and A SHALL keep its longer copy

#### Scenario: A node keeps the turns it took past the cut it continued

- **WHEN** controller B hydrated a cut at 3, minted a lineage at its first record, took records 4 and 5, and binds the relay again before any of them was shipped
- **THEN** B SHALL keep its copy over the backend's cut at 3

#### Scenario: A restart of a fully shipped relay mints nothing

- **WHEN** a relay whose Cells this controller shipped whole restarts on it twice, and each bind adopts its own shipped cut
- **THEN** no lineage id SHALL be minted and no trail entry added

#### Scenario: An older peer is compared as before

- **WHEN** a relay moves from a controller whose build predates lineage ids
- **THEN** the arriving controller SHALL compare the two copies by epoch and then the highest seq, and the older controller, adopting from a newer one, SHALL read the newer one's copy as it reads any copy today

#### Scenario: Data from before lineage ids compares as before

- **WHEN** neither copy of a Cell carries a lineage id
- **THEN** the newer copy SHALL be the one with the higher epoch and then the higher seq, a tie going to the incoming copy

