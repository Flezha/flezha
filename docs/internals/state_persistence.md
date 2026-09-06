# Controller state persistence

How the controller keeps an Operative's per-Entity state durable: the on-disk layout, the
`WalWriter` + `FsyncBatcher` substrate, snapshots and retention, runner→controller flow control,
the live and time-travel read paths, and what recovery does at every crash window. Written for
contributors working in `lib/flezha/state/`. The guarantee-level picture (write path, effect
record, push-vs-pull baseline) is [`../architecture/kernel.md`](../architecture/kernel.md) and is
linked, not repeated; the runner-side flush path is
[`../sdk/state_persistence.md`](../sdk/state_persistence.md). Per-Entity WAL persistence is
independent of plugin-subscription persistence (the ProcessHub cspec,
[`process_hub.md`](process_hub.md)): the WAL persists data-plane progress, the cspec which
plugins are subscribed.

## On-disk layout

```text
<state_root>/                        # :flezha, :state_root → FLEZHA_STATE_DIR → app_data/state
  .layout                            # namespaced-layout marker
  {namespace}/                       # `_system` holds pre-namespace state
    {operative_class}/
      kind  effectively_once  checkpoint_ack  compact_after_bytes ...   # class sidecars
      input.log                      # durable input log (effectively-once tier)
      completion.record              # a finished workflow operative
      {procedure_id}/
        {entity_key}.wal
        {entity_key}.snap.{seq}
        {entity_key}.outbox.dedup
        {entity_key}.comp_intent     # saga compensation markers; presence is
        {entity_key}.comp_done       # the signal (empty files)
    {instance_id}/{instance_id}/     # one workflow instance
      proc.journal                   # step progress
      inbox.wal                      # durable inbox
```

The triple `(operative_class, procedure_id, entity_key)` identifies an Entity's state and is all
the wire carries. The namespace level is resolved at ONE seam, `Flezha.State.Paths.class_dir/1`:
a claims ETS table (`:flezha_state_namespaces`, keyed `{state_root, class}`) → the on-disk layout
→ `_system`. An Operative records the claim when it binds; a class owned by another namespace
refuses with `{:namespace_conflict, class, owner}`, which makes cross-namespace state hijack
impossible at the path seam; a class found under `_system` is adopted (one atomic rename) by the
first real claimant; `Paths.delete_files/1` releases the claim once the class directory is
gone. `Flezha.Boot.StatePathMigrator` runs at `Application.start/2` and lifts an older tree under
`_system/`; the `.layout` marker, not the tree shape, makes re-runs no-ops. The `runner_id` is
never part of the layout: a restarted runner's state ships to it on attach via `RestoreInitState`.

`entity_key` is URL-safe base64 with a leading `_` when it contains characters outside
`[A-Za-z0-9_.-]` (`Paths.encode_entity_key/1` / `decode_entity_key/1`). A workflow instance's
`proc.journal` and `inbox.wal` ride the same `WalWriter` + `FsyncBatcher` substrate under two
reserved keys (`_proc_journal`, `_proc_inbox`; `Paths.journal_id/1` / `inbox_id/1`), write no
dedup side-file, and are skipped by `Paths.scan_dir/1` along with `.tmp`, `.rewriting` and
unknown suffixes, so nothing surfaces as a phantom Entity.

## Module map

| Module | Role |
|---|---|
| `Flezha.State.RecordFrame` | The on-disk record frame every durable log shares: `encode_record/3`, `decode_record/2`, `fold_records/3`, `fold_path/3`, `scan_for_last_valid_seq/1`, and the `cut_below/5` / `cut_above/4` byte-range rewrites. Names only `SeqIndex`; holds no handle and no process. |
| `Flezha.State.Paths` | Path resolution, namespace claims, enumeration; no GenServer, I/O limited to `File.ls/1` and `atomic_install/2`. |
| `Flezha.State.Wire` | Codec for the `entity_state_change` / `entity_effect_record` / `entity_state_snapshot` / `delete_entity_state` / `park_payload` / compensation payload-ids, the postcard `EffectRecordBody`, the `flz.entity` / `flz.seq` / `flz.key` output stamps. |
| `Flezha.State.Metrics` | Per-Entity `:counters` (`persisted_changes_total`, `wal_size_bytes`, `snapshot_seq`, `wal_shed_total`, `stale_writer_refused_total`, `fsync_total`, ...) in an ETS table. |
| `Flezha.State.Supervisor` | `DynamicSupervisor` for per-Entity `WalWriter`s plus the `WriterRegistry` process registry: `find_or_start/1`, `delete_entity/1`, `registered_ids/0`. Creates the claims, watermark and `ClassRegistry` tables so they outlive any one process. |
| `Flezha.State.WalWriter` | One GenServer per Entity: WAL handle, seq accounting, dedup, credit attribution, outbox cursors, realign and baseline logic. |
| `Flezha.State.RecordLog` / `SeqIndex` | Append/read for the reserved logs over `WalWriter.append_next/3` (assigns the seq, writes and fsyncs in one call); the sparse seq→position index (one sample per 256 records, pure — no file I/O) shared by the WAL writer and the input log so a prefix cut copies the tail as bytes. |
| `Flezha.State.FsyncBatcher` | One shared GenServer casting `:fsync` to every dirty writer per tick. |
| `Flezha.State.Snapshot` | Snapshot file lifecycle (`.tmp → fsync → rename → truncate`). |
| `Flezha.State.Outbox` | Per-Entity dedup file (`last_persisted_seq`, `outbox_flushed_seq`, realign `epoch`; advanced at fsync cadence, repaired at boot from the WAL scan) and the delivery-confirmation seam (`dispatch/2`, `token/2,3`, `hold/1`, `settled/1`, `confirm/1`, `park/4`). The owner is always a `WalWriter`: the Entity's for a stream, the journal writer for a workflow. |
| `Flezha.State.InputLog` / `CreditLedger` | Effectively-once tier: per-Operative ingest journal (`<class>/input.log`), the min-watermark the log owns (`consumed: %{entity => offset}` in its state; the ETS table is the published view), truncation below `min(watermark, now - hold_window)`, and the pure byte ledger (`admit/2`, `drain/2`) behind both source credit and the state-stream window. |
| `Flezha.State.PersistenceResolver` | Per-class knobs: operator map → instance metadata → SDK declaration → global default. |
| `Flezha.State.Commands` / `Recovery` / `Restore` | Control-channel round-trips (`dispatch_restore_init_state/2`, `dispatch_compact_entity/2`, `notify_entity_backpressure/2`); `resync/2` (push `RestoreInitState` or pull `CompactEntity`); assembly of the restore payload (latest snapshot + WAL tail + compensation markers). |
| `Flezha.State.Watchdog` | Sensor: compaction triggers and timeouts, output stalls, disk-quota and RPO alarms. |
| `Flezha.State.Retention` / `Completion` | Sweeper for time-travel retention, finished workflows (`completion.record`) and input logs. |
| `Flezha.State.TimeTravel` | Point-in-time query behind `flz state at`. |
| `Flezha.State.ProcJournal` / `ProcInbox` | Workflow step journal and durable inbox over the reserved writers. |
| `Flezha.State.WalTransfer` | Destination-pulled copy of one Entity's files between controllers. |
| `Flezha.State.DurabilityTier` (+ `Shipper`, `Hydrator`, `Manifest`, `Backend`) | Opt-in remote artifact shipping and hydration; `backend: :none` wires nothing. See [`../operations/remote_durability_tier.md`](../operations/remote_durability_tier.md). |

## Cross-controller transfer

`state_root` is node-local and never assumed shared, so durable state pins an Operative to the
controller whose disk holds it. A HotSwap handover normally carries state in-band, but
`Flezha.Operative.Handover.prepare/1` (via `Overwatch.prepare_handover_state/1`) sources it from
the runner, so a migration triggered by that runner's death (the loop's `replace_operative`, when
a controller loses its last runner for a namespace) ships an empty payload. The handover stamps `source_node`; on an
empty payload the destination runs `WalTransfer.pull_operative/3` against that node before the
ordinary replay. The transfer is destination-driven — the source only answers reads
(`entities/1`, `manifest/1`, `read_chunk/3` over `:erpc`) — so a source dying mid-transfer fails
the pull instead of stranding a partial file. Each file lands through `Paths.atomic_install/2`
(a `.transferring` sibling, fsynced, renamed; any failure discards it and leaves that Entity
cold). A requested name is resolved against `Paths.entity_files/1`, never joined onto the state
directory, so a peer cannot read arbitrary paths. Telemetry `[:flezha, :state, :wal_pulled]` /
`[:flezha, :state, :wal_pull_failed]`. Source files stay; the Retention sweeper reaps orphans.

**Partition rescale.** `WalTransfer.move_entity/3` is the same machinery pointed at a different
Operative class: each Entity whose owner changes moves into the new member's tree under the same
procedure and key — an atomic rename per file on one controller, the pull with `:as` across
controllers with the source deleted only after every file landed. The move is atomic per Entity,
so re-running `flz operative scale` after a crash moves nothing twice; the rescale stamps
`state_source_node` on the new members' opts so a member placed elsewhere pulls its class tree
first ([`../sdk/partitioned_operatives.md`](../sdk/partitioned_operatives.md)). All of this is
one-shot at migration time, not replication: a controller that dies together with its disk takes
its Operatives' state with it unless the remote durability tier is enabled.

## WAL record format

```text
[seq:    u64 BE]
[len:    u32 BE]      = 16 + byte_size(change_bytes)
[header: 16 B]        sdk_lang / version / schema
[change_bytes]
[crc32:  u32 BE]      :erlang.crc32(header <> change_bytes)
```

`RecordFrame.scan_for_last_valid_seq/2` forward-scans the file at open (there is no length at the
record tail, so a backward scan is impossible). It truncates only where it can prove that no
complete record can begin after the failure point — a read that ran off the end of the file, a
crc failure whose framed extent is the rest of the file, or a length field with less than one
whole record left after it. That is the torn tail a crash mid-append leaves, and the repair is
reported (`[:flezha, :state, :record_frame, :repaired]`). Anything else — most importantly a
record whose crc fails with complete records behind it — is **refused**: the bytes stay on disk,
the writer stays alive and refuses to serve, and one error line and one telemetry event
(`[:flezha, :state, :record_frame, :refused]`) name the file, the last seq that verified and the
byte offset. `Flezha.State.RecordFrame` is the one walk every reader uses and gives the same answer:
`fold_records/4` over an open device, `fold_bytes/4` over bytes already in memory — a restore
reading a whole WAL, or the Elixir SDK folding a change log the Controller shipped — and one
`decode_bytes/1` beneath both, so a record read from a device and the same record read from
bytes decode identically. The two differ in one stated thing: a device log may have a torn tail
an append left, and a payload shipped whole has none, so a crc failure in a payload is refused
however few bytes follow it. `test/flezha/state/durable_frame_conformance_test.exs` folds the
same golden log through every one of them, and `flz_core/src/restore.rs` folds the same bytes in
the kernel from `test/fixtures/durable_frame.txt` — a framing change on either side fails the
other side's suite.

A snapshot file is `<<header::16-bytes, snapshot_bytes, crc32::32-big>>` — a third on-disk
format, not this frame, though its crc is the same expression over the same two ranges
(`RecordFrame.crc32/2`).

## Durability, and how a whole file lands

A durable file is one row of `Flezha.State.Durable`. The row says how the file is framed
(`:record` for a log appended and cut record by record, `:whole` for a file replaced entire),
what makes its bytes durable (`:inline` when the install fsyncs before it renames, `:batched`
when a sync the owner already runs is what makes them durable) and the suffix it wears while it
is being written.

`Paths.atomic_install/3` is the one ceremony: it fills an in-progress sibling under the declared
suffix, fsyncs the descriptor when the declaration says the install owes the sync, renames onto
the final path, and removes the sibling on any failure. Nothing else opens, writes, syncs and
renames a durable file by hand. A caller passes its declared id; the suffix, the sync and
whether the ceremony creates the directory come from the row, so a new durable file is a
declaration rather than a fourth copy of the ceremony.

Two exclusions derive from the same rows. `Paths.scan_dir/1` and the durability tier's local
backend ask `Durable.in_progress?/1` rather than listing suffixes, so a new durable file cannot
be surfaced as a phantom Entity by a scanner nobody edited. A `.claim` sidecar is not
in-progress and is excluded by name where it lives: it is the durability tier's fencing counter
and it outlives every write.

The Elixir SDK's runner-id file is the one whole-file install outside the ceremony, and the
reason is packaging rather than durability: the SDK is its own core and
`scripts/check-sdk-boundary.sh` refuses it `Flezha.State.Paths`.

## Reserved-log record formats

`proc.journal` and `inbox.wal` reuse the WAL record framing above; what differs is what the
16-byte header and the change-bytes carry.

The journal's header is what the writer dedups a commit barrier on: a re-sent
`proc_committed` / `iter_committed` / `compensated` for an identity the journal already holds
is answered from the existing entry, never appended twice. The `iter` field is the enclosing
loop iteration (`0` for a non-looping chain), so redo-vs-skip works at both the procedure and
the iteration granularity from one scan.

```text
proc.journal
header: [epoch: u32 BE][type: u8][proc_idx: u16 BE][iter: u32 BE][0: 40 bits]
body:   [type: u8][proc_idx: u16 BE][iter: u32 BE][payload]

inbox.wal
header: [sha256(dedup_id)[0..16]]
body:   [dedup_len: u16 BE][dedup_id][meta_len: u32 BE][meta][payload]
```

Journal entry types: `1 = proc_committed` (the entities that ran, the serialized inter-proc
handoff and the procedure's staged outbox — ONE durable write), `2 = outbox_flushed` (that
outbox reached the plugins), `3 = iter_committed` (a loop iteration finished: the inbox offset
it consumed plus its terminal handoff), `4 = parked` (the loop is suspended at a committed
boundary, with an optional wake deadline), `5 = compensated` (a compensation hook's undo
outbox), `6 = rewound` (the loop was rewound to the iteration in this entry's `iter` field).

```text
proc_committed: [n_entities: u16 BE] n × ([len: u16 BE][entity_identity])
                [handoff]
                [outbox]
iter_committed: [consumed_offset: u64 BE][handoff]
parked:         [wake_deadline_ms: u64 BE]   # 0 = no deadline
compensated:    [outbox]
outbox_flushed: [for_type: u8]   # which entry type's outbox was flushed
rewound:        <empty>          # the target iteration is the `iter` field
handoff:        [n: u32 BE] n × ([channel_id: u32 BE][len: u32 BE][payload])
outbox:         [n: u32 BE] n × ([len: u16 BE][entity_identity][seq_in_proc: u32 BE]
                                 [len: u16 BE][channel][len: u32 BE][payload])
```

A torn tail is truncated by the `WalWriter` open scan; the read path stops at the first invalid
record.

## Outbox dedup file format

`<key>.outbox.dedup` v2 is three 8-byte big-endian integers —
`last_persisted_seq` (unsigned), `outbox_flushed_seq` (signed, `-1` = nothing flushed yet),
`epoch` (unsigned) — written through `.tmp` + rename. A legacy 8-byte file reports its value as
`last_persisted_seq` with cursor `-1` and epoch `0`; shorter or unreadable files report the
empty triple.

## Schema evolution

The header's `schema_version` (bytes 5..9) is the version of the Procedure that
wrote the record. The controller stores it and ships it back: `RestoreInitState`
carries the change log's headers inside the WAL framing and the snapshot's own
header in `snapshot_header` — `Restore.assemble_payload/1` reads it off disk
with the body rather than discarding it, because a stripped header leaves every
compacted Entity unverifiable.

Resolution is the SDK's, in `flz_core` (one table for every SDK, typed path and
host crossing alike): a record stamped at the running version decodes; one
stamped older but at or above the Procedure's declared readable floor goes to
its migration; anything else is refused — below the floor, or from a version
newer than the one running. Per record, because a WAL written across a deploy
holds both.

When any record was migrated, the SDK sets `migrated` on the
`RestoreInitStateResponse`, and `Commands.await_restore_init_state/2` answers it
with the ordinary `CompactEntity` handshake: the next snapshot is written by the
running version and the migrated records truncate behind it, so an Entity pays
its migration on one restore rather than on every restart. The bind-time restore
awaits its replies (bounded `Task.async_stream`, `:bind_restore_concurrency`)
for exactly this reason — the reply is where that fact lives. Telemetry:
`[:flezha, :state, :schema_migration]`.

## Writer startup and fsync batching

`StreamChannel` resolves a writer with `Supervisor.find_or_start/1` on every `entity_state_change`
frame. `WalWriter.init/1` only builds the struct and returns `{:continue, :open}`; the process is
already registered, so `find_or_start` returns in microseconds, and all disk work (`mkdir`,
`:file.open`, the tail scan, seek-to-EOF) runs in `handle_continue(:open, _)` — processed before
any queued message, so appends arriving during the scan wait in the writer's mailbox, never the
channel's.

A writer casts `FsyncBatcher.mark_dirty/1` once per fsync window (the first append after a drain);
each tick casts `:fsync` to every dirty writer, so writers sync concurrently and one writer behind
a deep mailbox delays no sibling's durability. `flush_now/0` and `flush_all/0` call instead, and a
writer that does not confirm stays dirty. A (re)started batcher seeds its dirty set from
`Supervisor.registered_ids/0`. The tick is `:agent_fsync_interval_ms` (default 5 ms, legal range
`[1, 1000]`; out of range fails `init/1` with `{:interval_out_of_range, ...}`) and emits
`[:flezha, :state, :fsync, :tick]`. The input log syncs on a linked companion handle so the sync
never sits in its append loop.

## Snapshot lifecycle

The `Watchdog` sweep triggers compaction on size or age (`flz state rotate` manually), reading
the writer's in-memory accounting, sharing the in-flight `compactions` map as the re-enqueue
guard, and emitting `[:flezha, :state, :compaction_trigger]` with `trigger: :size | :age`:

- **Size** — un-snapshotted bytes past `PersistenceResolver.compact_after_bytes/2`: an operator
  `:compact_after_bytes => %{class => bytes}` map, else the SDK's declared `compact_after_bytes()`
  (persisted as a class sidecar), else `:default_compact_after_bytes` (8 MiB).
- **Age** — the oldest un-snapshotted record is older than
  `PersistenceResolver.compact_after_age_ms/1` (`:compact_after_age_ms`, default 300 000 ms, `0`
  disables). A declared correction window raises this floor to at least itself, because
  compaction truncates the history a rewind needs.

1. The controller ships `CompactEntity` over the control channel.
2. The SDK locates the live Entity, **drains its pending changes first** (so memory equals the
   shipped WAL), serializes `State` under the state lock — postcard in Rust, Kryo in Java, the
   codec the restore path decodes, never the JSON migration serializer — ships
   `entity_state_snapshot` stamped with the highest seq it folds, and answers
   `CompactEntityResponse{snapshot_seq, snapshot_bytes_size}`.
3. `StreamChannel` calls `Snapshot.write_snapshot/4`: write `<key>.snap.{seq}.tmp`, `:file.sync/1`,
   close, `File.rename/2` to `<key>.snap.{seq}`, then `WalWriter.truncate_up_to/2` drops
   `seq <= snapshot_seq` (boundary via `SeqIndex`, tail copied as bytes by `cut_below`/`cut_above`,
   never record by record). The next append, at `snapshot_seq + 1`, is past the new baseline.

The rename is atomic; a `.tmp` left by a crash is filtered by `scan_dir/1`; a crash between rename
and truncate is safe because folding already-folded records is idempotent.

**Failure path.** `Commands.dispatch_compact_entity/2` treats a
`{snapshot_seq: 0, snapshot_bytes_size: 0}` answer against a non-empty WAL as a failed handshake
(`[:flezha, :state, :compaction_failed]`); a snapshot that never arrives is bounded by
`:compaction_timeout_ms` (default 30 s, `0` disables; `[:flezha, :state, :compaction_timeout]`).
The Watchdog keeps one `Flezha.Config.Backoff` strike per `{:compaction, entity_id}` (5 s →
30 s → 60 s → 300 s); below the give-up threshold a still-correct Entity is not resynced and the
backoff-gated sweep is the retry. At 4 failed attempts it emits
`[:flezha, :state, :compaction_gave_up]` (alertable, naming the Entity and attempt count) and
dispatches a real resync with the `runner_id` the writer's status carries; `compaction_completed`
resets the strike.

## Retention sweeper

`Flezha.State.Retention` ticks every `:time_travel_retention_check_interval_ms` (default
60 000 ms) and sweeps three things:

- **Entities** — per `Paths.all_entities/0`, `PersistenceResolver.retention_hours/1` (default
  6). `0`: keep only the latest snapshot and truncate the WAL to its seq. Otherwise the oldest
  snapshot with mtime `≤ now - retention_hours * 3600` is kept, older ones reaped, and the WAL
  keeps `seq > kept_seq`. The rewrite is `entity.wal.rewriting → rename`, so a mid-rewrite crash
  leaves the original intact; a writer with nothing below the cut answers at once.
- **Finished workflows** — a class whose `completion.record` is older than
  `PersistenceResolver.completed_retention_hours/1` (default 72 h) loses its retained
  state and record.
- **Input logs** — `InputLog.Sweep.run/1` per class: a cast that truncates below
  `min(watermark, now - hold_window)` and emits `[:flezha, :state, :input_log_frozen_watermark]`
  when the log grows while the watermark stays put.

## Flow control

Runner→controller flow control on the state stream is credit-based: park, never drop. On the
first `entity_state_change` of a stream attach the `StreamChannel` offers
`CreditWindowInit{stream_id, window_bytes, low_watermark_bytes}` over the control channel
(`:credit_flow_control_enabled` / `FLEZHA_CREDIT_FLOW_CONTROL=false` is the off switch). The
window is `PersistenceResolver.credit_window_bytes/2` (per-class map, default
`:wal_high_watermark_bytes`, capped by the namespace's `max_mailbox_bytes_total` quota), the low
watermark `:credit_low_watermark_bytes` (default half the window), the `stream_id` an opaque
session minted per attach. The SDK answers `CreditWindowAck`; from then on:

- **Debit on receipt** — every frame's wire size passes `CreditLedger.admit/2`; a frame arriving on
  an empty buffer is always admitted, even past the window (single-frame overshoot).
- **Fsync mints credit** — the writer attributes each written frame to the debiting channel
  (`credit_pending`, session-tagged) and reports `{:credit_drained, session, bytes}` on durable
  drain, or immediately for frames that never occupy the buffer (dedup, gap, shed, realign, stale
  refusal). `CreditLedger.drain/2` returns one batched `CreditGrant` when the outstanding bytes
  fall below the low watermark or a low-watermark's worth has been freed; a stalled fsync mints
  nothing.
- **The sender parks** — `flz_core/src/credit.rs` (one implementation for every host) debits
  before the sink write and parks until the next grant; the `credit_parked` counter rides
  `data_heartbeat` metadata so a parked runner classifies as fed-but-parked, not frozen.
- **Reconnect resets the ledger** — a re-attach re-runs the handshake with a fresh session;
  reports and grants tagged with a dead session are dropped.
- **Overrun is an anomaly** — a frame past the exhausted ledger is shed at the channel (the seq
  gap drives the resync repair), bumps the channel's `credit_overrun_total` liveness counter and
  emits `[:flezha, :state, :credit_anomaly]` (kind `:overrun`).

**Status flags (sessions without the handshake).** Crossing `:wal_high_watermark_bytes` (16 MiB)
emits `[:flezha, :state, :wal, :backpressure]` and ships `EntityMailboxStatus{status: "full"}` to
the owning runner (`Commands.notify_entity_backpressure/2`); draining below
`:wal_low_watermark_bytes` (default `high / 2`) ships `"open"` — hysteresis against oscillation.
Credit-speaking sessions get the telemetry only.

**Hard in-memory bound (shed, last resort).** `append_async/6` sheds the incoming Change when
`pending_bytes` reaches `:wal_hard_watermark_bytes` (default `4 ×` the soft watermark) or the
writer's mailbox depth reaches `:wal_pending_msg_cap` (default 5 000). Each shed counts into
`wal_shed_total`; the seq gap drives one `Recovery.resync` per episode, so the SDK — authoritative
for Entity state — re-sends. The cast path never blocks, so a slow disk cannot stall the data
channel. A conformant credit-speaking sender never reaches this bound; a shed against such a
session emits `[:flezha, :state, :credit_anomaly]` (kind `:shed`) and is an alerting condition.

## Live read (`flz state get`)

```text
flz state get <operative>/<procedure>[/<entity_key>] [--output text|json]
```

The current in-memory value of one live Entity, or of every Entity of one Procedure, as JSON with
the `change_seq` it was read at. An author does nothing extra: the mechanism is the `EntityState`
command every SDK answers for migration and handover, narrowed by two optional selector fields;
the selector-less form is the handover shape and is unchanged byte for byte. A single-mode
Procedure reads as `<operative>/<procedure>`, a static one as `<operative>/<procedure>/<declared key>`,
a dynamic one (`.key_by` / `.keyBy` on the wiring) as `<operative>/<procedure>/<live key>`. Work
is proportional to the selector: a keyed read is one lookup, a Procedure read serializes every
live Entity of it.

`Flezha.Service.CliManager.state_get/3` resolves the namespace holding the Operative
(`OperativeManager.namespace_of/2`), authorizes as `:"state.get"` (viewer level, like `state.list`
and `state.at`) and issues the round-trip. The seq rides the reply frame's `flz.change_seq`
metadata, present only when the selector names exactly one Entity, so the JSON body keeps the
shape the handover caller already decodes. Both SDKs sort object keys, so an equivalent state
serializes identically on either runtime.

**Freshness: the freshest value there is, possibly ahead of durability.** The SDK serializes under
the Entity's state lock without draining pending changes, so the reply includes changes the
flusher has not yet shipped; the value may never become durable, which is why the seq is
reported. **A read is not a compaction**: it triggers no drain, snapshot or truncation. Compaction
must drain because its `snapshot_seq` becomes a truncation point; a read has no such obligation,
and taking one would let `flz state get` rewrite durable history.

**A non-resident Entity is refused, never served from disk.** Dynamic-mode eviction is destructive
(the recreate cold-starts), the controller cannot decode the SDK's WAL bytes (postcard/Kryo,
stored opaquely), and a fallback would bind a runner and restore an Entity as a side effect of a
read. Every refusal exits non-zero with the reason on stderr; the non-resident family names
`flz state at` as the durable path.

| Refusal | Means | Raised by |
|---|---|---|
| `entity_evicted` | idle-evicted; eviction is destructive, so no durable value exists either | SDK |
| `entity_not_found` | that Procedure holds no Entity by that key | SDK |
| `procedure_not_found` | the Operative declares no Procedure by that name | SDK |
| `entity_key_without_procedure` | a key given with no Procedure to scope it (a key is unique only within its Procedure) | SDK |
| `{:not_resident, :suspended}` | the instance is suspended at a committed boundary and holds no runner resources | controller |
| `{:not_resident, :unbound}` | the Operative is not bound to a runner; refused rather than waiting for a bind | controller |
| `{:unsupported_for_kind, :persistent_state}` | the kind has no per-Entity state | controller |
| `{:oversized_state, bytes}` | the reply exceeds `:wal_high_watermark_bytes` | controller |

`entity_evicted` vs `entity_not_found` is a real distinction: eviction unlinks the Entity's files,
so the SDK keeps a bounded FIFO (1024 keys, `flz_core` `EVICTED_TRACE_CAP`) of what its reaper
evicted; past the cap the answer degrades to `entity_not_found`, the safe direction.

**Size bound.** The reply is bounded by `:wal_high_watermark_bytes` (16 MiB), the ceiling the
workflow handoff uses; there is no separate key. Over it the read is refused naming the byte count
and the narrowing selector: `state get: the selected state is 20971520 bytes, over the
:wal_high_watermark_bytes ceiling — narrow the read with <operative>/<procedure>/<entity_key>`.

**Output.** Text prints `change_seq:` and `state:`; `--output json` (or its switch spelling
`--json`) prints
`{"change_seq":412,"path":"ohlc/candle/BTCUSD@1m","state":{...}}`. A whole-Procedure read names
no single version and prints `change_seq: -` (JSON `null`).

**Programmatic seam.** The CLI is one caller; the read is available in-process:

```elixir
# Narrowed read: {:ok, json, change_seq} | {:error, reason}
OperativeManager.get_entity_state("trading", "ohlc", %{procedure: "candle", entity_key: "BTCUSD@1m"})
# Whole Operative, seq-less: {:ok, json} — the migration/handover shape
OperativeManager.get_entity_state("trading", "ohlc")
# The CLI verb itself: path parsing, authz, rendering
CliManager.state_get("ohlc/candle/BTCUSD@1m", "json")
```

**Cost.** One control round-trip plus one state-lock acquisition, bounded by one invocation: a
point read for operators and low-rate readers, not a serving API and not a subscription — a
high-rate path is a sink (`.sink(...)` into Kafka or Postgres). Worked examples:
`sdk/rust/examples/live_state_read.rs`,
`sdk/java/flezha-sdk-examples/src/main/java/com/flezha/examples/E11_LiveStateRead.java`.

| | `flz state get` | `flz state at` |
|---|---|---|
| Question | what is it **now** | what was it **then** |
| Source | the runner's memory | the controller's WAL |
| Freshness | at or ahead of durable | exactly as durable at that seq |
| Needs a bound runner | yes | no |
| Version reported | `change_seq` it was read at | the seq/timestamp asked for |
| Non-resident Entity | refused, naming why | served, if the WAL still holds it |

## Time-travel CLI

```text
flz state list <operative_class>                                         # viewer
flz state at <operative/procedure/entity_key> <seq | iso8601-timestamp>  # viewer
flz state rotate <operative/procedure/entity_key>                        # operator
flz state retention <operative_class> <hours>                            # admin
flz state delete <operative/procedure/entity_key>                        # admin
```

The verbs live in `cli/src/state.rs` and RPC into `Flezha.Service.CliManager.state_*`.
`state list` reports each Entity's live writer counters (`Metrics.snapshot_all/0`: WAL size,
snapshot seq, ...). `state at` runs `Flezha.State.TimeTravel.query/2`, which returns
`%{snapshot_bytes, change_log_bytes, terminal_seq}` — the assembled baseline and change log at the
resolved seq — and refuses a kind without the `:time_travel?` capability. `state rotate` resolves
the runner with `Recovery.resolve_runner/1` and calls `Commands.dispatch_compact_entity/2`.
`state retention` writes `Application.put_env(:flezha, :time_travel_retention_hours,
%{class => hours}, persistent: true)`; the next sweep reads it, and surviving a boot requires
`runtime.exs`.

## Dynamic-entity hard delete

A dynamic-mode Procedure that opts into an idle TTL (`entity_idle_ttl()` returning a duration;
default none) has its idle, persistence-enabled Entities evicted by the SDK reaper, which ships a
fire-and-forget `delete_entity_state` frame (payload-id `Wire.payload_id_delete_entity_state/0`,
body the bare `entity_id`) on the secondary stream. `StreamChannel` honours it only for a class
with the `:dynamic_entities?` capability — any other kind gets a warning and is ignored, so a
durable workflow's state is never dropped out from under it — and routes it to
`Flezha.State.Supervisor.delete_entity/1`, which closes the writer, drops the `FsyncBatcher` dirty
mark, unlinks every path in `Paths.entity_files/1` (`Paths.delete_files/1`) and drops the
Entity's input-log watermark entry (`InputLog.forget_entity/2`). The delete is idempotent. A later
input for the same key cold-starts a fresh Entity from seq 0; there is no restore-from-disk on
recreate. `flz state delete` calls the same function and prints a confirmation.

## Crash-recovery semantics

| Crash window | Recovery at next boot |
|---|---|
| WAL append, before fsync | The last record may be lost; `scan_for_last_valid_seq/2` truncates the partial tail. The SDK re-ships records with `seq <= last_persisted_seq`, which the writer drops as duplicates. |
| Snapshot `.tmp` write | Ignored by `Paths.scan_dir/1`; overwritten on the next compaction. |
| Snapshot renamed, WAL not yet truncated | Snapshot durable, WAL still holds covered records; restore applies the snapshot then folds them — idempotent. |
| `.rewriting` from retention or truncation | Filtered by `scan_dir/1`; the original WAL is intact and the rewrite re-runs on the next sweep. |

## Self-healing sensors: `Watchdog` + `Recovery`

The Watchdog sweeps every `:output_watchdog_interval_ms` (default 10 000 ms; `0` disables the
timer) and is a sensor: it emits, and the convergence Loop owns every operative-level remedy. Each
completed sweep stamps `Flezha.Convergence.Snapshot.observe_sensor(:watchdog, at, period)`; a
wedged Watchdog stops stamping and `/healthz` reports `200 degraded, reason: sensor_stale` within
one poll. The mark is written by the sensor, never polled from it.

- **Seq gap on a populated WAL** — `seq > last_persisted_seq + 1` while `last_persisted_seq >= 0`:
  the writer emits `[:flezha, :state, :seq_gap]` and resyncs so the SDK resends from
  `terminal_seq + 1`.
- **Output staleness** — an Entity still receiving Changes whose persisted seq froze past
  `PersistenceResolver.output_stall_timeout_ms/1` (default 60 000 ms, `0` disables) emits
  `[:flezha, :state, :output_stall]` and nothing else; the remedy is the Loop's ladder
  (`resync_runner` → `rebind_operative`, [`convergence_loop.md`](convergence_loop.md)). An idle
  Entity is not flagged.
- **Compaction timeout** — stays with the Watchdog because it owns the handshake end to end.
- **Disk quota and RPO** — latched `[:flezha, :state, :disk_quota_breach]` (the namespace's
  `max_state_bytes`) and, with a remote backend, `[:flezha, :state, :rpo_exceeded]`; emit-only.

**Push or pull.** `Recovery.resync/2` chooses by who holds history (kernel.md has the sequence): a
controller with a baseline pushes `RestoreInitState`; one with none pulls a snapshot via
`CompactEntity`, and it never ships an empty `RestoreInitState` (`Restore.has_persisted_state?/1`
treats a zero-byte WAL as no state), which would rebuild a live Entity to `init_state`. On cold
takeover the first `entity_state_change` on an empty WAL (`last_persisted_seq == -1`) with
`seq > 0` and no runner-restart realign puts the writer into **awaiting-baseline**: the frame is
shed (counted in `wal_shed_total`; the SDK is not disconnected), `[:flezha, :state, :baseline_seed]`
fires once per episode and a `CompactEntity` is dispatched; further gap frames shed quietly until
the snapshot lands, its seq becomes `last_persisted_seq` and the next delta appends contiguously.
The gap is DECLARED once per episode (`{:seq_gap, entity_id}`, carrying the runner that emitted
it) and the convergence loop plans the `resync_runner` that pulls, retrying it on the loop's own
5 s → 30 s → 60 s → 300 s table while the Entity waits — a pull that failed to dispatch (a torn
control channel answers `:no_control_channel`) would otherwise strand it in permanent shed. The
baseline landing, or any contiguous append, declares `{:seq_gap_closed, entity_id}`, which ends
the episode and releases the ladder.

## What a restore does to queued input

A `RestoreInitState` lands on an Entity that may hold inputs in its mailbox — a rebind does not
stop the runner, and `AddOperative` is idempotent. The queue survives only when the controller
cannot hand it back: classes whose inputs are journaled before forwarding — the effectively-once
tier (input log) and durable workflow instances (proc inbox) — have the mailbox **cleared**,
because the queue is a subset of the log the controller re-forwards and keeping it would
double-apply it and, in a correction, re-order it behind the replayed slice. Every other class,
including a plain stateful stream operative (the default), **keeps** its queue:
`Flezha.Operative.Ingest` forwards those inputs un-journaled
([`../resilience/data_durability.md`](../resilience/data_durability.md)), so the mailbox is their
only copy. The rule lives once, in `EntitySlot::discard_queued_input_on_restore`
(`flz_core/src/entity.rs`); discards ship as `restore_discarded_inputs` in `data_heartbeat`
metadata. The replay buffer, replay flags and any open checkpoint interval are cleared on every
class. A dispatch already in flight when the restore lands has its Change stamped with a seq the
`change_seq` re-arm then re-issues; the WAL dedups the collision (`seq <= last_persisted_seq`) at
the cost of at most that one Change.

## Runner-restart realign is owner-bound

Within one runner session the per-Entity seq is monotonic, so a Change from a different session
than the one that wrote the current epoch, at a non-contiguous seq, signals a runner restart: the
writer realigns — truncates the WAL, removes the snapshots, adopts `seq - 1` as
`last_persisted_seq` and the new session as `owning_runner` — and emits
`[:flezha, :state, :wal_realign]`. A same-session low seq stays a dedup. The realign is honoured
only from the Entity's current owner binding: a newest-wins takeover unbinds the superseded
session before binding the new one ([`runner_identity.md`](runner_identity.md) "The WAL binds to
the owner") — fenced `StreamChannel`s refuse durable-write forwards, and the writer refuses frames
from a session `FleetManager.Marks.superseded_session?/1` marks superseded with `{:error, :stale_writer}`,
consulted only when the frame's session differs from the epoch owner so the owner's append path
pays nothing. Refusals emit `[:flezha, :state, :stale_writer_refused]` once per (Entity, session)
episode, count into `stale_writer_refused_total`, release any credit debit immediately and never
spawn a resync.

## Persisted `runner_id` is advisory

The `runner_id` in an operative-binding record (replayed into `Flezha.Operative.Overwatch.init/1`
from ProcessHub's durable registry at boot) names the runner the Operative was bound to before
this controller process started. On every cold start
`handle_continue({:bind_runner, stale_runner_id, operative}, _)` runs `Lifecycle.bind/3`, which
re-resolves a live runner via `OperativeManager.assign_runner/3`; the persisted value feeds only
the `rebound from stale runner X to live Y` log line. `AddOperative`, `:ping_runner` and
`start_operative_sdk` target the re-resolved runner, and the bounded-retry governor re-resolves on
every attempt. The source of truth for the live binding is ProcessHub's registry.

## Configuration knobs

All under `:flezha`. Per-class maps are `%{<OperativeClass> => value}` and resolve operator map →
instance metadata → SDK declaration → global default.

| Key | Default | Meaning |
|---|---|---|
| `:state_root` | `FLEZHA_STATE_DIR`, else `"app_data/state"` | Root for every durable file. |
| `:agent_fsync_interval_ms` | `5` | `FsyncBatcher` tick; range `[1, 1000]`. |
| `:wal_high_watermark_bytes` | `16 * 1024 * 1024` | Soft watermark: status flags, default credit window, live-read ceiling, journal entry bound. |
| `:wal_low_watermark_bytes` | `high / 2` | Status-flag release. |
| `:wal_hard_watermark_bytes` | `high * 4` | Hard un-fsynced-bytes bound; past it `append_async` sheds. |
| `:wal_pending_msg_cap` | `5_000` | Hard mailbox-depth bound; past it `append_async` sheds. |
| `:credit_flow_control_enabled` | `true` (`FLEZHA_CREDIT_FLOW_CONTROL`) | `false` skips the `CreditWindowInit` offer. |
| `:credit_window_bytes` | `%{}` (default `wal_high_watermark_bytes`, capped by `max_mailbox_bytes_total`) | Per-class state-stream window. |
| `:credit_low_watermark_bytes` | `window / 2` | Grant trigger. |
| `:outbox_inflight_max` | `1_000` | Effect records inside the plugin chain unconfirmed before the flush waits. |
| `:outbox_resettle_ms` | `60_000` | Re-dispatch an entry whose chain run never reported back. |
| `:record_log_cut_after_bytes` | `64 * 1024` | Released bytes before a reserved log (journal/inbox) cuts its prefix. |
| `:default_compact_after_bytes` | `8 * 1024 * 1024` (`FLEZHA_COMPACT_AFTER_BYTES`) | Size trigger; `:compact_after_bytes` map overrides per class. |
| `:compact_after_age_ms` | `300_000` (`FLEZHA_COMPACT_AFTER_AGE_MS`; `0` in `:test`) | Age trigger; `0` disables; raised to a declared correction window. |
| `:compaction_timeout_ms` | `30_000` | `CompactEntity` → snapshot bound; `0` disables. |
| `:output_watchdog_interval_ms` | `10_000` (`FLEZHA_OUTPUT_WATCHDOG_INTERVAL_MS`) | Watchdog sweep; `0` disables. |
| `:output_stall_timeout_ms` / `:default_output_stall_timeout_ms` | `%{}` / `60_000` | Output-staleness window; `0` disables. |
| `:time_travel_retention_check_interval_ms` | `60_000` (`FLEZHA_RETENTION_CHECK_INTERVAL_MS`) | Retention tick. |
| `:time_travel_retention_hours` / `:default_time_travel_retention_hours` | `%{}` / `6` | Snapshot history kept; `0` keeps only the latest. |
| `:workflow_completed_retention_hours` / `:default_workflow_completed_retention_hours` | `%{}` / `72` (`FLEZHA_WORKFLOW_RETENTION_HOURS`) | Finished workflow data kept before the sweeper reclaims it. |
| `:input_log_hold_window_ms` | `0` (`FLEZHA_INPUT_LOG_HOLD_WINDOW_MS`) | Floor on how long a consumed input stays in `input.log`; a declared correction window raises it. |
| `:operative_persistence` | `%{}` (default `true`) | Per-class persistence on/off (workflow kinds; a stream is always persistent). |
| `:sync_durability` | `%{}` (default: the SDK declaration) | Per-class: commit barriers wait for the remote tier. |

## Smoke check (manual)

`test/scripts/state_persistence.sh` is the executable check for the data-plane
half of this capability: it brings up the dev compose stack, starts an
operative on the `store_input` factory, attaches the websocket and Kafka
plugins, lets the testnet drive inputs, then asserts the namespaced WAL layout
(`<state_root>/<namespace>/<operative_class>/<procedure_id>/<key>.wal`) and
that `flz state list <operative_class>` reports the live entity. It wipes
`app_data/state/` and its compose volumes on every run, so a green result is
evidence about the contract and not about leftovers.

**It is NOT wired into CI, and nothing else runs it.** It needs a Docker
daemon, a Kafka broker and a full in-container `cargo build` + `mix release`,
none of which the `elixir`, `naming`, `docker` or `nightly-e2e` workflows
carry. Measured cost: **2.5-4 min** for a green run from wiped volumes (four
consecutive green runs: 2m20s, 3m36s, 3m37s, 4m08s). `cleanup` runs `down -v`,
so the `cargo build` and `mix release` inside the container are paid every
time. Run it by hand after touching
`lib/flezha/state/`, the plugin-attach CLI, or the script itself — a long
green gap in this file's history means nobody ran it, not that it passes.

Note that `flz state at` takes the DECODED entity key: `_.wal` on disk is
`Paths.encode_entity_key/1`'s rendering of the empty key, not a path
fragment.

## See also

- [`../architecture/kernel.md`](../architecture/kernel.md) — the write path, the effect record, push-vs-pull baseline, the minimum watermark.
- [`../sdk/state_persistence.md`](../sdk/state_persistence.md) — the runner-side flush, restore and compaction path in both SDKs.
- [`../sdk/protocol_spec.md`](../sdk/protocol_spec.md) — `EntityState` selector fields, `flz.change_seq`, refusal tokens; `CreditWindowInit` / `CreditGrant`.
- [`runner_identity.md`](runner_identity.md) — the takeover guard the owner-bound realign relies on.
- [`convergence_loop.md`](convergence_loop.md) — the output-stall remediation ladder.
- [`../operations/remote_durability_tier.md`](../operations/remote_durability_tier.md) — surviving the loss of the disk.
- [`../../test/scripts/state_persistence.sh`](../../test/scripts/state_persistence.sh) — the manual smoke check for this capability's data-plane half.
