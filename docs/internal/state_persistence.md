# Controller state persistence

How the controller keeps a Relay's per-Cell state durable: the on-disk layout, the
`WalWriter` + `FsyncBatcher` substrate, snapshots and retention, runner→controller flow control,
the live and time-travel read paths, and what recovery does at every crash window. Written for
contributors working in `lib/flezha/state/`. The guarantee-level picture (write path, effect
record, push-vs-pull baseline) is [`../public/architecture/kernel.md`](../public/architecture/kernel.md) and is
linked, not repeated; the runner-side flush path is
[`sdk/state_persistence.md`](sdk/state_persistence.md). Per-Cell WAL persistence is
independent of plugin persistence (the ProcessHub cspec):
the WAL persists data-plane progress, the cspec which
plugins are attached and on which hooks.

## On-disk layout

The state root is one row of `lib/flezha/layout/table.ex`, which declares every
subtree this product writes, the knob that moves each one and what `flz reset`
does with it. `FLEZHA_STATE_DIR` fills `:state_root` once at boot in
`config/runtime.exs`; nothing reads the variable afterwards. This section
describes what is inside the state root; the table is where the root itself is
decided.

```text
<state_root>/                        # :flezha, :state_root, else storage/app/state
  .layout                            # namespaced-layout marker
  {namespace}/                       # `_system` holds pre-namespace state
    {relay_class}/
      run  effectively_once  ack_checkpoint  compact_after_bytes ...   # class sidecars
      input.log                      # durable input log (effectively-once tier, durable steps relay)
      completion.record              # a finished steps relay
      {tasklet_id}/
        {cell_key}.wal
        {cell_key}.snap.{seq}
        {cell_key}.outbox.dedup
        {cell_key}.lineage           # the epoch and folded offset's home
        {cell_key}.comp_intent       # saga compensation markers; presence is
        {cell_key}.comp_done         # the signal (empty files)
    {instance_id}/{instance_id}/     # one steps instance, whose class dir is its id
      task.journal                   # step progress
```

The triple `(relay_class, tasklet_id, cell_key)` identifies a Cell's state and is all
the wire carries. The namespace level is resolved at ONE seam, `Flezha.State.Paths.class_dir/1`:
a claims ETS table (`:flezha_state_namespaces`, keyed `{state_root, class}`) → the on-disk layout
→ `_system`. A Relay records the claim when it binds; a class owned by another namespace
refuses with `{:namespace_conflict, class, owner}`, which makes cross-namespace state hijack
impossible at the path seam; a class found under `_system` is adopted (one atomic rename) by the
first real claimant; `Paths.delete_durable/1` releases the claim once the class directory is
gone. `Flezha.Boot.StatePathMigrator` runs at `Application.start/2` and lifts an older tree under
`_system/`; the `.layout` marker, not the tree shape, makes re-runs no-ops. The `runner_id` is
never part of the layout: a restarted runner's state ships to it on attach via `RestoreInitState`.

`cell_key` is URL-safe base64 with a leading `_` when it contains characters outside
`[A-Za-z0-9_.-]` (`Paths.encode_cell_key/1` / `decode_cell_key/1`). A steps instance's
`task.journal` rides the same `WalWriter` + `FsyncBatcher` substrate under one reserved key
(`_task_journal`; `Paths.journal_id/1`), writes no dedup side-file, and is skipped by
`Paths.scan_dir/1` along with `.tmp`, `.rewriting` and unknown suffixes, so nothing surfaces as
a phantom Cell. A steps relay's input lives in the class-level `input.log`, the same file a flow relay
on the effectively-once tier writes.

## Module map

| Module | Role |
|---|---|
| `Flezha.State.RecordFrame` | The on-disk record frame every durable log shares, walked on a device: `decode_record/2`, `fold_records/3`, `fold_path/3`, `scan_for_last_valid_seq/1`, and the `cut_below/5` / `cut_above/4` byte-range rewrites. The frame itself, `encode_record/3` and the byte decoder are `Flezha.State.Frame`. Names only `SeqIndex`; holds no handle and no process. |
| `Flezha.State.Paths` | Path resolution, namespace claims, enumeration; no GenServer, I/O limited to `File.ls/1` and `atomic_install/2`. |
| `Flezha.State.Wire` | The codec for the `cell_state_change` / `cell_effect_record` / `cell_state_snapshot` / `delete_cell_state` / `park_payload` / compensation payload-ids and the postcard `EffectRecordBody`. The `flz.cell` / `flz.seq` / `flz.key` output stamps are `Flezha.State.Outbox`'s. |
| `Flezha.State.Metrics` | Per-Cell `:counters` (`persisted_changes_total`, `wal_size_bytes`, `snapshot_seq`, `wal_shed_total`, `stale_writer_refused_total`, `fsync_total`, ...) in an ETS table. |
| `Flezha.State.Supervisor` | `DynamicSupervisor` for per-Cell `WalWriter`s plus the `WriterRegistry` process registry: `find_or_start/1`, `delete_cell/1`, `registered_ids/0`. Creates the claims, watermark and `ClassRegistry` tables so they outlive any one process. |
| `Flezha.State.WalWriter` | One GenServer per Cell: WAL handle, seq accounting, dedup, credit attribution, outbox cursors, realign and baseline logic. |
| `Flezha.State.RecordLog` / `SeqIndex` | Append/read for the reserved journal over `WalWriter.append_next/3` (assigns the seq, writes and fsyncs in one call); the sparse seq→position index (one sample per 256 records, pure — no file I/O) shared by the WAL writer and the input log so a prefix cut copies the tail as bytes. |
| `Flezha.State.FsyncBatcher` | One shared GenServer casting `:fsync` to every dirty writer per tick. |
| `Flezha.State.Snapshot` | Snapshot file lifecycle (`.tmp → fsync → rename → truncate`). |
| `Flezha.State.Lineage` | A Cell's two small records: the dedup file (`last_persisted_seq`, `outbox_flushed_seq`, realign `epoch`, and `folded_offset`, the highest consumed offset of the records a prefix cut dropped; advanced at fsync cadence, the cursors repaired at boot from the WAL scan) and the lineage file, the home of the epoch and the folded offset (`read/1`, `progress/1`, `dedup/1`, `put/2`, `put_dedup/2`). |
| `Flezha.State.Outbox` | The delivery-confirmation seam (`dispatch/2`, `token/2,3`, `hold/1`, `settled/1`, `confirm/1`, `park/4`). The owner is always the Cell's writer, for a flow relay Cell and a steps relay Cell alike: a flow relay's entry is committed by its record's fsync, a steps relay's is held until a durable stage marker names its record (`commit_unit: :stage`, read at boot from the class registry). |
| `Flezha.State.InputLog` / `CreditLedger` | Durable input for either kind (a flow relay on the effectively-once tier, a durable steps relay): the per-instance ingest journal (`<key>/input.log`), answered once synced, with its `flz.msg_id` index; the min-watermark the log owns over its reporters (Cells, or a steps relay's journal identity; `consumed: %{reporter => offset}` in its state, the ETS table the published view); `tail/1` (floor and last offset, no record read); truncation below `min(watermark, now - hold_window)` once the prefix exceeds the cut threshold; and the pure byte ledger (`admit/2`, `drain/2`) behind both source credit and the state-stream window. |
| `Flezha.State.PersistenceResolver` | Per-class knobs: user map → instance metadata → SDK declaration → global default. |
| `Flezha.State.Commands` / `Recovery` / `Restore` | Control-channel round-trips (`dispatch_restore_init_state/2`, `dispatch_compact_cell/2`, `notify_cell_backpressure/2`); `resync/2` (push `RestoreInitState` or pull `CompactCell`); assembly of the restore payload (latest snapshot + WAL tail + compensation markers). |
| `Flezha.State.Watchdog` | Sensor: compaction triggers and timeouts, output stalls, disk-quota and RPO alarms. |
| `Flezha.State.Retention` / `Completion` | Sweeper for time-travel retention, finished steps relays (`completion.record`) and input logs. |
| `Flezha.State.TimeTravel` | Point-in-time query behind `flz state at`. |
| `Flezha.State.TaskJournal` | Steps relay step journal over the reserved writer: the markers `task_committed`, `iter_committed`, `dead_lettered`, `compensated` and `rewound`, the park of a failed turn (`park_turn_async/4`) and the bind's removal of one that never reached its record (`drop_unrecorded_turns/1`), the committed record ranges per Cell a writer and a bind re-derive (`committed_records/1`), the channels the rewind guard reads (`staged_channels_above/2`), and the reporter of each consumed offset into the input log's watermark (`release_input/2`). It holds no output: the Cells' writers own delivery. |
| `Flezha.State.WalTransfer` | Destination-pulled copy of one Cell's files between controllers. |
| `Flezha.State.DurabilityTier` (+ `Shipper`, `Hydrator`, `Manifest`, `Backend`) | Opt-in remote artifact shipping and hydration; `backend: :none` wires nothing. See [`../public/topics/core/durability.md`](../public/topics/core/durability.md). |

## Cross-controller transfer

`state_root` is node-local and never assumed shared, so durable state pins a Relay to the
controller whose disk holds it. A HotSwap handover normally carries state in-band, but
`Flezha.Relay.Handover.prepare/1` (via `Overwatch.prepare_handover_state/1`) sources it from
the runner, so a migration triggered by that runner's death (the hub's own redistribution, after
a controller loses its last runner for a factory and withdraws it) ships an empty payload. The handover stamps `source_node`; on an
empty payload the destination runs `WalTransfer.pull_relay/3` against that node before the
ordinary replay. The pull carries every durable file of the instance, one reference at a time:
each Cell's file set (`{:cell, cell_id}`), a steps relay's journal under its reserved identity, and
the instance's input log (`{:input_log, class}`), because a journal without its log would seed
the floor in an empty offset space. `Flezha.State.Adoption` runs it once, as the relay arrives
and before any of its writers opens, and only when the source's copy supersedes this node's
(`Artifact.copy_supersedes?/4`: by lineage first, a copy continuing ours or unrelated to it
winning whatever its length and one ours continues only past our fork; within one lineage the
WAL-owner epoch, then the highest seq, a tie going to the source). The pull marks each Cell's
lineage record landed under the Cell's file lock, so its next writer mints a new lineage id.
A local copy it replaces, an empty one on a node that held nothing, is first renamed aside whole,
so the pull lands as on a node that holds nothing; once every reference has landed, what only the old copy held (a reference the source
lacks, a class-level sidecar) is copied back and the old copy is removed, and otherwise the old
copy comes back, so a node that held nothing keeps none of a copy that did not land whole. The landing is recorded atomically, with the files to copy back, before any is
copied, so at the next arrival a set-aside copy recorded as landed has its copy-back finished,
and an unrecorded one (a crash before the newer copy landed) comes back. Writers still open on the class are closed before the bind opens its
own, nothing is replaced while another relay bound on the node owns the class, and a source
that cannot be asked is logged and the local copy bound unchecked. The
transfer is destination-driven — the source only answers reads
(`Paths.durable_refs/1`, `manifest/1`, `read_chunk/3` over `:erpc`) — so a source dying mid-transfer fails
the pull instead of stranding a partial file. Each file lands through `Paths.atomic_install/2`
(a `.transferring` sibling, fsynced, renamed; any failure discards it and leaves that Cell
cold). A requested name is resolved against the reference's own file set — `Paths.cell_files/1`
for a Cell, the one `Paths.input_log_path/1` for the log — never joined onto the state
directory, so a peer cannot read arbitrary paths. Telemetry `[:flezha, :state, :wal_pulled]` /
`[:flezha, :state, :wal_pull_failed]`. Source files stay; the Retention sweeper reaps orphans.

**Partition rescale.** `WalTransfer.move_cell/3` is the same machinery pointed at a different
Relay class: each Cell whose owner changes moves into the new member's tree under the same
tasklet and key — an atomic rename per file on one controller, the pull with `:as` across
controllers with the source deleted only after every file landed. A member that keeps its id
but sits on another controller is gathered the same way, and a steps relay member's journal and
input log move with its Cells under its own id. The stop that precedes the moves preserves the
files but closes the member's writers and its input log on the node it ran on, so the restarted
member opens the files where the move left them. A surviving steps relay member's stop first seals
its journal (`TaskJournal.cut/2` with `seal: true`) at its latest committed iteration. The
move is atomic per Cell, so re-running `flz relay scale` after a crash moves nothing twice;
the rescale stamps `state_source_node` on the new members' opts so a member placed elsewhere
pulls its class tree before its bind reads its resume state
([`../public/topics/core/partitioning.md`](../public/topics/core/partitioning.md)). All of this is
one-shot at migration time, not replication: a controller that dies together with its disk takes
its Relays' state with it unless the remote durability tier is enabled.

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
reading a whole WAL — and one
`decode_bytes/1` beneath both, so a record read from a device and the same record read from
bytes decode identically. The two differ in one stated thing: a device log may have a torn tail
an append left, and a payload shipped whole has none, so a crc failure in a payload is refused
however few bytes follow it. `test/flezha/state/durable_frame_conformance_test.exs` folds the
same golden log through every one of them, and `crates/flz_core/src/restore.rs` folds the same bytes in
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
be surfaced as a phantom Cell by a scanner nobody edited. A `.claim` sidecar is not
in-progress and is excluded by name where it lives: it is the durability tier's fencing counter
and it outlives every write.

## Reserved-log record formats

`task.journal` reuses the WAL record framing above; what differs is what the 16-byte header and
the change-bytes carry.

The journal's header is what the writer dedups a commit barrier on: a re-sent
`task_committed` / `iter_committed` / `compensated` for an identity the journal already holds
is answered from the existing entry, never appended twice. The `iter` field is the enclosing
loop iteration (`0` for a non-looping chain), so redo-vs-skip works at both the tasklet and
the iteration granularity from one scan.

```text
task.journal
header: [epoch: u32 BE][type: u8][task_idx: u16 BE][iter: u32 BE][0: 40 bits]
body:   [type: u8][task_idx: u16 BE][iter: u32 BE][payload]
```

Journal entry types: `1 = task_committed` (the Cells the stage ran, each naming the effect
records it wrote and their plugin channels, the serialized inter-task handoff, the completion
and the timer trigger), `3 = iter_committed` (a loop iteration finished: the input-log offset it
consumed, the entry channel whose chain it ran, and its terminal handoff), `5 = compensated`
(the Cells a compensation hook ran on, each naming its undo records), `6 = rewound` (the loop
was rewound to the iteration in this entry's `iter` field), `7 = input_released` (an input
dead-lettered unread), `8 = dead_lettered` (a keep-progress session's turn failed: the offset it
consumed, the entry channel, the failing tasklet, its attempts, the DLQ entry and the running
count of failed turns; every fold reads it as the iteration's commit, its committed tasklets owed
undo until `compensated` covers each). Types 2 and 4 are unused. The
journal holds markers only: a Cell's state changes and plugin outputs live in its own effect
records, which the writer has made durable before the entry naming them is appended.

```text
task_committed: [cells][handoff][completion][trigger]
iter_committed: [offset][len: u16 BE][entry_channel][handoff][completion][failed_turns]
dead_lettered:  [offset][len: u16 BE][entry_channel][len: u16 BE][tasklet]
                [attempts: u32 BE][len: u16 BE][dlq_entry][count: u32 BE]
failed_turns:   <empty>                                   # no turn has failed
                | [3: u8][count: u32 BE][last_iter: u32 BE]
compensated:    [cells]
rewound:        <empty>          # the target iteration is the `iter` field
cells:          [n: u16 BE] n × ([len: u16 BE][cell_identity][records]
                                 [n_chan: u16 BE] n_chan × ([len: u16 BE][channel]))
records:        [0: u8]                                  # the Cell wrote no record
                | [1: u8][first_seq: u64 BE][seq: u64 BE] # its records first_seq..seq
handoff:        [n: u32 BE] n × ([channel_id: u32 BE][len: u32 BE][payload])
completion:     <empty>                      # none; an entry written before completions existed
                | [1: u8][len: u32 BE][result]
trigger:        <empty>                      # the pass was not opened by a timer
                | [2: u8][len: u16 BE][cell_key][kind: u8][scheduled_at: u64 BE]
                                             # kind 0 = processing time, 1 = event time
offset:         [0: u8][0: u64 BE]           # consumed no journaled input
                | [1: u8][consumed_offset: u64 BE]   # 0 is the log's first offset
```

A torn tail is truncated by the `WalWriter` open scan; the read path stops at the first invalid
record.

## Outbox dedup file format

`<key>.outbox.dedup` v3 is four 8-byte big-endian integers —
`last_persisted_seq` (unsigned), `outbox_flushed_seq` (signed, `-1` = nothing flushed yet),
`epoch` (unsigned), `folded_offset` (signed, `-1` = none) — written through `.tmp` + rename.
A v2 file (the first three) reads `folded_offset` as `-1`; a legacy 8-byte file reports its value
as `last_persisted_seq` with cursor `-1` and epoch `0`; shorter or unreadable files report the
empty record. The layout does not change, so a build from before the lineage file reads it after
a rollback.

`<key>.lineage` is the home of the epoch and the folded offset: `"FLZL"`, version byte `1`, a
crc32 of the body, then the term-encoded record (`id`, `trail`, `landed`, `epoch`,
`folded_offset`, `cut`). It is installed through `.tmp`, fsynced, renamed, and its directory
fsynced (durability `:inline_dir`), always before the dedup file carries a new epoch or folded
offset, so a power loss cannot keep the dedup file's copy and lose it. A Cell without one gets it
at its writer's next dedup write; an unreadable one reads as none. The first record a writer
appends to a Cell with no `id`, or to a landed copy, first installs a new random `id` (the mint,
`Epoch.open_window/1`): the old id goes on the `trail` with the seq it forked at (16 kept) and
`landed` is cleared; a failed install fails that append and restarts the writer. A writer that
finds its Cell's directory gone (the relay's state deleted under it) treats the Cell as dropped:
it refuses the record, gives back its stream credit and stops without a restart.
`Lineage.read/1` takes the higher epoch of the two files with that file's folded offset (the
lineage file's on a tie); a
dedup file ahead means a build without lineage records ran since, and the lineage record is
rewritten before the next dedup write. Its pending `cut` counts only while the dedup file's epoch
is below its own, and a dedup file behind it reads with an empty log's cursors
(`last_persisted_seq` 0, `outbox_flushed_seq` -1): a power loss can keep the record whose cut
was cleared and lose the dedup write after it, and the older epoch's flush cursor would count the
new epoch's records as flushed. A realign (`cut: :all`) and a rewind (`{:above, seq}`) write it with the next
epoch before their disk step, then clear the cut in it before the dedup file is written (the dedup
file is not synced, so only the lineage record can end the cut); a writer that boots with the cut
still pending runs that disk step again before it serves. A prefix cut writes its folded offset to
it before the cut file is renamed. A writer whose open refused the WAL never read either file and
writes neither.

`folded_offset` is the highest consumed offset the WAL records a prefix cut dropped carried
(`WalWriter.Epoch.cut_below/3`): a live writer's (`WalWriter.truncate_up_to/3`: compaction, or retention
while the writer runs) folds behind the remote gate only what the gate acknowledged. A retention
cut of a Cell with no writer runs in a task holding the Cell's file lock, so a writer starting
meanwhile waits for it, and folds behind the gate only the records at or below the persisted flush
cursor; it leaves a Cell whose bump is pending to its writer's boot, and one that finds a writer
hands the cut to it once the lock is let go. It is the one
field the WAL cannot rebuild, so every consumed-offset read of a Cell (the boot scan, a sleeper's
seed, a hydration's seed, a rewind, a correction's baseline) starts from it; `WalWriter.Epoch.floor/2` is
the one reader of the floor from disk. A cut is always at a snapshot's seq, so a
rewind below the newest snapshot, or a realign, resets it to `-1`, and a read bounded below the
newest snapshot does not count it.

A stream's first marker fences the remote gate (`WalWriter.RemoteGate.await_held/1`): it lists
from disk the gated Cells of the identity's `durable: remote` classes whose last record lies
above the cut their manifest entry names (`DurabilityTier.Shipper.shipped_through/1`), then the running
gated writers, dropping from the disk set every Cell a writer now holds. Each writer drains a
fence credit behind what it holds; each Cell left in the disk set ships from the fence task,
retried on the `:durability_ship` ladder until it lands, without starting a writer. The flush
cursor is not the test: outside the effectively-once tier it stays `-1`.

## Schema evolution

The header's `schema_version` (bytes 5..9) is the version of the Tasklet that
wrote the record. The controller stores it and ships it back: `RestoreInitState`
carries the change log's headers inside the WAL framing and the snapshot's own
header in `snapshot_header` — `Restore.payload/2` reads it off disk
with the body rather than discarding it, because a stripped header leaves every
compacted Cell unverifiable.

Resolution is the SDK's, in `flz_core` (one table for every SDK, typed path and
host crossing alike): a record stamped at the running version decodes; one
stamped older but at or above the Tasklet's declared readable floor goes to
its migration; anything else is refused — below the floor, or from a version
newer than the one running. Per record, because a WAL written across a deploy
holds both.

When any record was migrated, the SDK sets `migrated` on the
`RestoreInitStateResponse`, and `Commands.await_restore_init_state/2` answers it
with the ordinary `CompactCell` handshake: the next snapshot is written by the
running version and the migrated records truncate behind it, so a Cell pays
its migration on one restore rather than on every restart. The bind-time restore
awaits its replies (steps `Task.async_stream`, `:bind_restore_concurrency`)
for exactly this reason — the reply is where that fact lives. Telemetry:
`[:flezha, :state, :schema_migration]`.

## Writer startup and fsync batching

`StreamChannel` resolves a writer with `Supervisor.find_or_start/1` on every `cell_state_change`
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
`[:flezha, :state, :fsync, :tick]`. It is armed only while a writer is dirty, so an idle batcher
never wakes: a mark that finds none armed arms it for the next point of the last tick's rhythm,
never sooner than one interval after that tick, a mark on an armed tick leaves it alone, and a
drain, flush or `forget` that leaves nothing dirty leaves no tick armed. The input log syncs on a
linked companion handle so the sync never sits in its append loop.

## Snapshot lifecycle

The `Watchdog` sweep triggers compaction on size or age (`flz state rotate` manually), reading
the writer's in-memory accounting, sharing the in-flight `compactions` map as the re-enqueue
guard, and emitting `[:flezha, :state, :compaction_trigger]` with `trigger: :size | :age |
:deferred`:

- **Size** — un-snapshotted bytes past `PersistenceResolver.compact_after_bytes/2`: the SDK's
  declared `compact_after_bytes()` (persisted as a class sidecar), else
  `:default_compact_after_bytes` (8 MiB).
- **Age** — the oldest un-snapshotted record is older than
  `PersistenceResolver.compact_after_age_ms/1` (`:compact_after_age_ms`, default 300 000 ms, `0`
  disables). A declared correction window raises this floor to at least itself, because
  compaction truncates the history a rewind needs.
- **Deferred** — the runner refused the Cell's last compaction, from any caller, with
  `state.cell_busy`: its live state held a retried attempt's changes, which no record can carry
  until the item resolves, or the credit window stayed exhausted for the serializer's gate wait. The refusal clears the in-flight entry without a backoff strike, and
  the next sweep asks again whatever the size or age. `flz state rotate` awaits the answer and
  prints a queued, refused, timed-out or failed compaction apart.

1. The controller ships `CompactCell` over the control channel.
2. The SDK locates the live Cell, **closes an open checkpoint interval early and drains its
   pending changes first** (so memory equals the shipped WAL and the consumed offset moves past
   every input the state holds; a Cell mid-retry is refused, as above), serializes `State` under the state lock — postcard in Rust, Kryo in Java, the
   codec the restore path decodes, never the JSON migration serializer — ships
   `cell_state_snapshot` stamped with the highest seq it folds, and answers
   `CompactCellResponse{snapshot_seq, snapshot_bytes_size}`.
3. `StreamChannel` calls `Snapshot.write_snapshot/4`: write `<key>.snap.{seq}.tmp`, `:file.sync/1`,
   close, `File.rename/2` to `<key>.snap.{seq}`, then `WalWriter.truncate_up_to/2` drops
   `seq <= snapshot_seq` (boundary via `SeqIndex`, tail copied as bytes by `cut_below`/`cut_above`,
   never record by record). The next append, at `snapshot_seq + 1`, is past the new baseline.
   A sample is trusted only while the record at its position still carries its seq; otherwise
   the boundary is found from byte 0, so an index that drifted from its file can never cut
   mid-record. Anything that empties or rewrites the WAL resets the index with it.

The rename is atomic; a `.tmp` left by a crash is filtered by `scan_dir/1`; a crash between rename
and truncate is safe because folding already-folded records is idempotent.

**Failure path.** `Commands.dispatch_compact_cell/2` treats a
`{snapshot_seq: 0, snapshot_bytes_size: 0}` answer against a non-empty WAL as a failed handshake
(`[:flezha, :state, :compaction_failed]`); a snapshot that never arrives is bounded by
`:compaction_timeout_ms` (default 30 s, `0` disables; `[:flezha, :state, :compaction_timeout]`).
The Watchdog keeps one `Flezha.Config.Backoff` strike per `{:compaction, cell_id}` (5 s →
30 s → 60 s → 300 s); below the give-up threshold a still-correct Cell is not resynced and the
backoff-gated sweep is the retry. At 4 failed attempts it emits
`[:flezha, :state, :compaction_gave_up]` (alertable, naming the Cell and attempt count) and
dispatches a real resync with the `runner_id` the writer's status carries; `compaction_completed`
resets the strike.

## Retention sweeper

`Flezha.State.Retention` ticks every `:time_travel_retention_check_interval_ms` (default
60 000 ms) and sweeps three things:

- **Cells** — per `Paths.all_cells/0`, `PersistenceResolver.retention_hours/1` (default
  6). `0`: keep only the latest snapshot and truncate the WAL to its seq. Otherwise the oldest
  snapshot with mtime `≤ now - retention_hours * 3600` is kept, older ones reaped, and the WAL
  keeps `seq > kept_seq`. The rewrite is `cell.wal.rewriting → rename`, so a mid-rewrite crash
  leaves the original intact; a writer with nothing below the cut answers at once.
- **Finished steps relays** — a class whose `completion.record` is older than
  `PersistenceResolver.completed_retention_hours/1` (default 72 h) loses its retained
  state and record.
- **Input logs** — `InputLog.Sweep.run/1` per state key, for either kind: a cast that truncates
  below `min(watermark, now - hold_window)`, rewriting the file only once the prefix below the
  floor exceeds `:record_log_cut_after_bytes`, and emits
  `[:flezha, :state, :input_log_frozen_watermark]` when the log grows while the watermark stays
  put. A `once` steps relay never commits an iteration, so its log goes with its class directory
  under finished-job retention.

## Flow control

Runner→controller flow control on the state stream is credit-based: park, never drop. On the
first `cell_state_change` of a stream attach the `StreamChannel` offers
`CreditWindowInit{stream_id, window_bytes}` over the control channel
(`:credit_flow_control_enabled` / `FLEZHA_CREDIT_FLOW_CONTROL=false` is the off switch). The
window is `PersistenceResolver.credit_window_bytes/2` (`:wal_high_watermark_bytes`, capped by
the namespace's `max_mailbox_bytes_total` quota), the low
watermark `:credit_low_watermark_bytes` (default half the window, kept by the controller and never
sent), the `stream_id` an opaque
session minted per attach. The SDK answers `CreditWindowAck`; from then on:

- **Debit on receipt** — every frame's wire size passes `CreditLedger.admit/2`; a frame arriving on
  an empty buffer is always admitted, even past the window (single-frame overshoot).
- **Fsync mints credit** — the writer attributes each written frame to the debiting channel
  (`credit_pending`, session-tagged) and reports `{:credit_drained, session, bytes}` on durable
  drain, or immediately for frames that never occupy the buffer (dedup, gap, shed, stale refusal).
  Bytes the writer lets go unsynced (a realign, a rewind's cut, a stop whose final sync fails) are
  reported `{:credit_released, session, bytes}`: the window is freed, no marker is confirmed.
  `CreditLedger.drain/2` returns one batched `CreditGrant` when the outstanding bytes
  fall below the low watermark or a low-watermark's worth has been freed; a stalled fsync mints
  nothing.
- **The sender parks** — `crates/flz_core/src/credit.rs` (one implementation for every host) debits
  before the output plugin write and parks until the next grant; the `credit_parked` counter rides
  `data_heartbeat` metadata so a parked runner classifies as fed-but-parked, not frozen.
- **Reconnect resets the ledger** — a re-attach re-runs the handshake with a fresh session;
  reports and grants tagged with a dead session are dropped.
- **Overrun is an anomaly** — a frame past the exhausted ledger is shed at the channel (the seq
  gap drives the resync repair), bumps the channel's `credit_overrun_total` liveness counter and
  emits `[:flezha, :state, :credit_anomaly]` (kind `:overrun`).

**Status flags (sessions without the handshake).** Crossing `:wal_high_watermark_bytes` (16 MiB)
emits `[:flezha, :state, :wal, :backpressure]` and ships `CellMailboxStatus{status: "full"}` to
the owning runner (`Commands.notify_cell_backpressure/2`); draining below
`:wal_low_watermark_bytes` (default `high / 2`) ships `"open"` — hysteresis against oscillation.
Credit-speaking sessions get the telemetry only.

**Hard in-memory bound (shed, last resort).** `append_async/6` sheds the incoming Change when
`pending_bytes` reaches `:wal_hard_watermark_bytes` (default `4 ×` the soft watermark) or the
writer's mailbox depth reaches `:wal_pending_msg_cap` (default 5 000). Each shed counts into
`wal_shed_total`; the seq gap drives one `Recovery.resync` per episode, so the SDK — authoritative
for Cell state — re-sends. The cast path never blocks, so a slow disk cannot stall the data
channel. A conformant credit-speaking sender never reaches this bound; a shed against such a
session emits `[:flezha, :state, :credit_anomaly]` (kind `:shed`) and is an alerting condition.

## Live read (`flz state get`)

```text
flz state get <relay>/<tasklet>[/<cell_key>] [--output text|json]
```

The current in-memory value of one live Cell, or of every Cell of one Tasklet, as JSON with
the `change_seq` it was read at. An author does nothing extra: the mechanism is the `CellState`
command every SDK answers for migration and handover, narrowed by two optional selector fields;
the selector-less form is the handover shape and is unchanged byte for byte. A single-mode
Tasklet reads as `<relay>/<tasklet>`, a static one as `<relay>/<tasklet>/<declared key>`,
a dynamic one (`.key_by` / `.keyBy` on the wiring) as `<relay>/<tasklet>/<live key>`. Work
is proportional to the selector: a keyed read is one lookup, a Tasklet read serializes every
live Cell of it.

`Flezha.Service.Steps.state_get/2` resolves the namespace holding the Relay
(`RelayManager.namespace_of/2`), authorizes as `:"state.get"` (viewer level, like `state.list`
and `state.at`) and issues the round-trip. The seq rides the reply frame's `flz.change_seq`
metadata, present only when the selector names exactly one Cell, so the JSON body keeps the
shape the handover caller already decodes. Both SDKs sort object keys, so an equivalent state
serializes identically on either runtime.

**Freshness: the freshest value there is, possibly ahead of durability.** The SDK serializes under
the Cell's state lock without draining pending changes, so the reply includes changes the
flusher has not yet shipped; the value may never become durable, which is why the seq is
reported. **A read is not a compaction**: it triggers no drain, snapshot or truncation. Compaction
must drain because its `snapshot_seq` becomes a truncation point; a read has no such obligation,
and taking one would let `flz state get` rewrite durable history.

**A non-resident Cell is refused, never served from disk.** Dynamic-mode eviction is destructive
(the recreate cold-starts), the controller cannot decode the SDK's WAL bytes (postcard/Kryo,
stored opaquely), and a fallback would bind a runner and restore a Cell as a side effect of a
read. Every refusal exits non-zero with the reason on stderr; the non-resident family names
`flz state at` as the durable path.

| Refusal | Means | Raised by |
|---|---|---|
| `cell_evicted` | idle-evicted; eviction is destructive, so no durable value exists either | SDK |
| `cell_not_found` | that Tasklet holds no Cell by that key | SDK |
| `tasklet_not_found` | the Relay declares no Tasklet by that name | SDK |
| `cell_key_without_tasklet` | a key given with no Tasklet to scope it (a key is unique only within its Tasklet) | SDK |
| `{:not_resident, :suspended}` | the instance is suspended at a committed boundary and holds no runner resources | controller |
| `{:not_resident, :unbound}` | the Relay is not bound to a runner; refused rather than waiting for a bind | controller |
| `:persistence_off` | the class declares persistence off, so its Cells hold no durable state | controller |
| `{:oversized_state, bytes}` | the reply exceeds `:wal_high_watermark_bytes` | controller |

`cell_evicted` vs `cell_not_found` is a real distinction: eviction unlinks the Cell's files,
so the SDK keeps a bounded FIFO (1024 keys, `flz_core` `EVICTED_TRACE_CAP`) of what its idle sweep
evicted; past the cap the answer degrades to `cell_not_found`, the safe direction.

**Size bound.** The reply is bounded by `:wal_high_watermark_bytes` (16 MiB), the ceiling the
steps relay handoff uses; there is no separate key. Over it the read is refused naming the byte count
and the narrowing selector: `state get: the selected state is 20971520 bytes, over the
:wal_high_watermark_bytes ceiling — narrow the read with <relay>/<tasklet>/<cell_key>`.

**Output.** Text prints `change_seq:` and `state:`; `--output json` (or its switch spelling
`--json`) prints
`{"change_seq":412,"path":"ohlc/candle/BTCUSD@1m","state":{...}}`. A whole-Tasklet read names
no single version and prints `change_seq: -` (JSON `null`).

**Programmatic seam.** The CLI is one caller; the read is available in-process:

```elixir
# Narrowed read: {:ok, json, change_seq} | {:error, reason}
RelayManager.get_cell_state("trading", "ohlc", %{tasklet: "candle", cell_key: "BTCUSD@1m"})
# Whole Relay, seq-less: {:ok, json} — the migration/handover shape
RelayManager.get_cell_state("trading", "ohlc")
# The published operation the CLI calls: path parsing and authz
Flezha.Service.Steps.state_get("ohlc/candle/BTCUSD@1m", caller_context)
```

**Cost.** One control round-trip plus one state-lock acquisition, bounded by one invocation: a
point read for users and low-rate readers, not a serving API and not a subscription — a
high-rate path is an output plugin (`.output(...)` into Kafka or Postgres). Worked examples:
`sdk/rust/examples/live_state_read.rs`,
`sdk/java/flezha-sdk-examples/src/main/java/com/flezha/examples/state/`.

| | `flz state get` | `flz state at` |
|---|---|---|
| Question | what is it **now** | what was it **then** |
| Source | the runner's memory | the controller's WAL |
| Freshness | at or ahead of durable | exactly as durable at that seq |
| Needs a bound runner | yes | no |
| Version reported | `change_seq` it was read at | the seq/timestamp asked for |
| Non-resident Cell | refused, naming why | served, if the WAL still holds it |

## Time-travel CLI

```text
flz state list <relay_class>                                         # viewer
flz state at <relay/tasklet/cell_key> <seq | iso8601-timestamp>      # viewer
flz state rotate <relay/tasklet/cell_key>                            # ops
flz state retention <relay_class> <hours>                            # admin
flz state delete <relay/tasklet/cell_key>                            # admin
```

The verbs live in `crates/cli/src/state.rs` and reach `Flezha.Service.Steps` through the door.
`state list` reports each Cell's live writer counters (`Metrics.snapshot_all/0`: WAL size,
snapshot seq, ...). `state at` runs `Flezha.State.TimeTravel.query/2`, which returns the restore
payload a bind ships, as of the resolved seq (`Restore.payload/2`: `%{snapshot_bytes,
snapshot_header, change_log_bytes, terminal_seq}`; with no snapshot at or before it, the change log
starts at record 0), and refuses a kind without the `:time_travel?` capability. `state rotate` resolves
the runner with `Recovery.resolve_runner/1` and calls `Commands.dispatch_compact_cell/2`.
`state retention` writes `Application.put_env(:flezha, :time_travel_retention_hours,
%{class => hours}, persistent: true)`; the next sweep reads it, and surviving a boot requires
`runtime.exs`.

## Dynamic-cell hard delete

A dynamic-mode Tasklet that opts into an idle TTL (`idle_ttl()` returning a duration;
default none) has its idle, persistence-enabled Cells evicted by the SDK's idle sweep, which ships a
`delete_cell_state` frame (payload-id `Wire.payload_id_delete_cell_state/0`, body the bare
`cell_id`) on the secondary stream under the durable guarantee class, so an eviction with no
stream bound holds the frame for the rebind instead of losing it. The sweep waits for no reply
and the controller writes none. `StreamChannel` honours it only for a class
with the `:dynamic_cells?` capability — any other kind gets a warning and is ignored, so a
durable steps relay's state is never dropped out from under it — and routes it to
`Flezha.State.Supervisor.delete_cell/1`, which closes the writer, drops the `FsyncBatcher` dirty
mark, unlinks every path in `Paths.cell_files/1` (`Paths.delete_durable/1`) and drops the
Cell's input-log watermark entry (`InputLog.forget_cell/2`). The delete is idempotent. A later
input for the same key cold-starts a fresh Cell from seq 0; there is no restore-from-disk on
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
timer) and is a sensor: it emits, and the convergence Loop owns every relay-level remedy. Each
completed sweep stamps `Flezha.Convergence.Snapshot.observe_sensor(:watchdog, at, period)`; a
wedged Watchdog stops stamping and `/healthz` reports `200 degraded, reason: sensor_stale` within
one poll. The mark is written by the sensor, never polled from it.

- **Seq gap on a populated WAL** — `seq > last_persisted_seq + 1` while `last_persisted_seq >= 0`:
  the writer emits `[:flezha, :state, :seq_gap]` and resyncs so the SDK resends from
  `terminal_seq + 1`.
- **Output staleness** — a Cell still receiving Changes whose persisted seq froze past
  `PersistenceResolver.output_stall_timeout_ms/1` (default 60 000 ms, `0` disables) emits
  `[:flezha, :state, :output_stall]` and nothing else; the remedy is the Loop's ladder
  (`resync_runner` → `rebind_relay`). An idle
  Cell is not flagged.
- **Compaction timeout** — stays with the Watchdog because it owns the handshake end to end.
- **Disk quota and RPO** — latched `[:flezha, :state, :disk_quota_breach]` (the namespace's
  `max_state_bytes`) and, with a remote backend, `[:flezha, :state, :rpo_exceeded]`; emit-only.

**Push or pull.** `Recovery.resync/2` chooses by who holds history (kernel.md has the sequence): a
controller with a baseline pushes `RestoreInitState`; one with none pulls a snapshot via
`CompactCell`, and it never ships an empty `RestoreInitState` (`Restore.has_persisted_state?/1`
treats a zero-byte WAL as no state), which would rebuild a live Cell to `init_state`. On cold
takeover the first `cell_state_change` on an empty WAL (`last_persisted_seq == -1`) with
`seq > 0` and no runner-restart realign puts the writer into **awaiting-baseline**: the frame is
shed (counted in `wal_shed_total`; the SDK is not disconnected), `[:flezha, :state, :baseline_seed]`
fires once per episode and a `CompactCell` is dispatched; further gap frames shed quietly until
the snapshot lands, its seq becomes `last_persisted_seq` and the next delta appends contiguously.
The gap is DECLARED once per episode (`{:seq_gap, cell_id}`, carrying the runner that emitted
it) and the convergence loop plans the `resync_runner` that pulls, retrying it on the loop's own
own `:heal` table while the Cell waits — a pull that failed to dispatch (a torn
control channel answers `:no_control_channel`) would otherwise strand it in permanent shed. The
baseline landing, or any contiguous append, declares `{:seq_gap_closed, cell_id}`, which ends
the episode and releases the ladder.

## What a restore does to queued input

A `RestoreInitState` lands on a Cell that may hold inputs in its mailbox — a rebind does not
stop the runner, and `AddRelay` is idempotent. The queue survives only when the controller
cannot hand it back: classes whose inputs are journaled before forwarding — the effectively-once
tier and durable steps instances, both through the one input log — have the mailbox **cleared**,
because the queue is a subset of the log the controller re-forwards and keeping it would
double-apply it and, in a correction, re-order it behind the replayed slice. Every other class,
including a plain stateful flow relay (the default), **keeps** its queue:
`Flezha.Relay.Ingest` forwards those inputs un-journaled
([`../public/topics/core/durability.md`](../public/topics/core/durability.md)), so the mailbox is their
only copy. The rule lives once, in `CellSlot::discard_queued_input_on_restore`
(`crates/flz_core/src/cell.rs`); discards ship as `restore_discarded_inputs` in `data_heartbeat`
metadata. The replay buffer, replay flags and any open checkpoint interval are cleared on every
class. A dispatch already in flight when the restore lands has its Change stamped with a seq the
`change_seq` re-arm then re-issues; the WAL dedups the collision (`seq <= last_persisted_seq`) at
the cost of at most that one Change.

## Runner-restart realign is owner-bound

Within one runner session the per-Cell seq is monotonic, so a Change from a different session
than the one that wrote the current epoch, at a non-contiguous seq, signals a runner restart: the
writer realigns — truncates the WAL, removes the snapshots, adopts `seq - 1` as
`last_persisted_seq` and the new session as `owning_runner` — and emits
`[:flezha, :state, :wal_realign]`. A same-session low seq stays a dedup. The realign is honoured
only from the Cell's current owner binding: a newest-wins takeover unbinds the superseded
session before binding the new one — fenced `StreamChannel`s refuse durable-write forwards, and the writer refuses frames
from a session `FleetManager.Marks.superseded_session?/1` marks superseded with `{:error, :stale_writer}`,
consulted only when the frame's session differs from the epoch owner so the owner's append path
pays nothing. Refusals emit `[:flezha, :state, :stale_writer_refused]` once per (Cell, session)
episode, count into `stale_writer_refused_total`, release any credit debit immediately and never
spawn a resync.

Before any of that, a Change from a runner whose stable identity the relay's ownership record
does not name, as owner or as the target of a running migrate, is refused at the `StreamChannel`
(`Flezha.Runner.StreamChannel.Records.admit/3`) and never reaches the writer, so it cannot realign
it. The refusal counts into the channel's `foreign_frame_refused` slot, emits
`[:flezha, :runner, :foreign_frame_refused]` once per session and relay, and tells that runner
to remove its copy. A Change for a relay with no ownership record on this node reaches the
writer under the session fence alone when the relay's durable state belongs to the session's
namespace (`Flezha.State.Paths.namespace_of_class/1`), and is refused the same way otherwise. A
cell id whose relay class or tasklet id would name another directory (`.`, `..`, or one holding
`/` or NUL) does not decode (`Flezha.State.Wire.decode_cell_id/1`), so no path is built from it.

## Persisted `runner_id` is advisory

The `runner_id` in a relay-binding record (replayed into `Flezha.Relay.Overwatch.init/1`
from ProcessHub's durable registry at boot) names the runner the Relay was bound to before
this controller process started. On every cold start
`handle_continue({:bind_runner, stale_runner_id, relay}, _)` runs `Lifecycle.bind_continue/3`,
whose bind re-resolves a live runner via `RelayManager.assign_runner/3`; the persisted value feeds only
the `rebound from stale runner X to live Y` log line. `AddRelay`, `:ping_runner` and
`start_relay_sdk` target the re-resolved runner, and the steps-retry governor re-resolves on
every attempt. The source of truth for the live binding is ProcessHub's registry.

## Configuration knobs

All under `:flezha`, one layer per setting: a per-relay value resolves instance metadata → SDK
declaration → global row, and no configuration map overrides it per class. The one
`%{<RelayClass> => hours}` store is `:time_travel_retention_hours`, which `flz state retention`
writes. The boot refuses a key no row reads and a value its row's type refuses
(`Flezha.Config.Environment.check!/1`).

| Key | Default | Meaning |
|---|---|---|
| `:state_root` | `FLEZHA_STATE_DIR`, else `"storage/app/state"` | Root for every durable file. |
| `:agent_fsync_interval_ms` | `5` | `FsyncBatcher` tick; range `[1, 1000]`. |
| `:wal_high_watermark_bytes` | `16 * 1024 * 1024` | Soft watermark: status flags, default credit window, live-read ceiling, journal entry bound. |
| `:wal_low_watermark_bytes` | `high / 2` | Status-flag release. |
| `:wal_hard_watermark_bytes` | `high * 4` | Hard un-fsynced-bytes bound; past it `append_async` sheds. |
| `:wal_pending_msg_cap` | `5_000` | Hard mailbox-depth bound; past it `append_async` sheds. |
| `:credit_flow_control_enabled` | `true` (`FLEZHA_CREDIT_FLOW_CONTROL`) | `false` skips the `CreditWindowInit` offer. |
| `:credit_low_watermark_bytes` | `window / 2` | Grant trigger. |
| `:outbox_inflight_max` | `1_000` | Effect records inside the plugin chain unconfirmed, per Cell of either kind, before the flush waits. |
| `:outbox_resettle_ms` | `60_000` | Re-dispatch an entry whose chain run never reported back, or whose relay call or answer its delivery still holds. |
| `:sdk_commit_ack_ms` | `30_000` (`FLEZHA_STEPS_ACK_TIMEOUT_MS`) | The commit worker's deadline for the effect records a steps relay barrier names to become durable; past it the barrier fails with `record_not_durable`. The runner's barrier re-send reads the same variable. |
| `:record_log_cut_after_bytes` | `64 * 1024` | Released bytes before the journal or an input log rewrites to cut its prefix. |
| `:default_compact_after_bytes` | `8 * 1024 * 1024` (`FLEZHA_COMPACT_AFTER_BYTES`) | Size trigger for a class that declares none. |
| `:compact_after_age_ms` | `300_000` (`FLEZHA_COMPACT_AFTER_AGE_MS`; `0` in `:test`) | Age trigger; `0` disables; raised to a declared correction window. |
| `:compaction_timeout_ms` | `30_000` | `CompactCell` → snapshot bound; `0` disables. |
| `:output_watchdog_interval_ms` | `10_000` (`FLEZHA_OUTPUT_WATCHDOG_INTERVAL_MS`) | Watchdog sweep; `0` disables. |
| `:default_output_stall_timeout_ms` | `60_000` | Output-staleness window; `0` disables. |
| `:time_travel_retention_check_interval_ms` | `60_000` (`FLEZHA_RETENTION_CHECK_INTERVAL_MS`) | Retention tick. |
| `:time_travel_retention_hours` / `:default_time_travel_retention_hours` | `%{}` / `6` | Snapshot history kept; `0` keeps only the latest. |
| `:default_relay_completed_retention_hours` | `72` (`FLEZHA_STEPS_RETENTION_HOURS`) | Finished steps relay data kept before the sweeper reclaims it; `flz relay list` keeps the same window. |
| `:source_credit_window_bytes` | `wal_high_watermark_bytes` | Bytes a source's input log admits before its writers wait. |
| `:input_log_hold_window_ms` | `0` (`FLEZHA_INPUT_LOG_HOLD_WINDOW_MS`) | Floor on how long a consumed input stays in `input.log`; a declared correction window raises it. |

## Smoke check (manual)

`test/scripts/state_persistence.sh` is the executable check for the data-plane
half of this capability: it brings up the dev compose stack, starts a
relay on the `store_input` factory, attaches the websocket and Kafka
plugins, lets the testnet drive inputs, then asserts the namespaced WAL layout
(`<state_root>/<namespace>/<relay_class>/<tasklet_id>/<key>.wal`) and
that `flz state list <relay_class>` reports the live cell. It wipes
`storage/app/state/` and its compose volumes on every run, so a green result is
evidence about the contract and not about leftovers.

**It is NOT wired into CI, and nothing else runs it.** It needs a Docker
daemon, a Kafka broker and a full in-container `cargo build` + `mix release`,
none of which the `elixir`, `naming`, `docker` or `nightly-e2e` steps relays
carry. Measured cost: **2.5-4 min** for a green run from wiped volumes (four
consecutive green runs: 2m20s, 3m36s, 3m37s, 4m08s). `cleanup` runs `down -v`,
so the `cargo build` and `mix release` inside the container are paid every
time. Run it by hand after touching
`lib/flezha/state/`, the plugin-attach CLI, or the script itself — a long
green gap in this file's history means nobody ran it, not that it passes.

Note that `flz state at` takes the DECODED cell key: `_.wal` on disk is
`Paths.encode_cell_key/1`'s rendering of the empty key, not a path
fragment.

## See also

- [`../public/architecture/kernel.md`](../public/architecture/kernel.md) — the write path, the effect record, push-vs-pull baseline, the minimum watermark.
- [`sdk/state_persistence.md`](sdk/state_persistence.md) — the runner-side flush, restore and compaction path in both SDKs.
- [`sdk/protocol_spec.md`](sdk/protocol_spec.md) — `CellState` selector fields, `flz.change_seq`, refusal tokens; `CreditWindowInit` / `CreditGrant`.
- [`../public/topics/core/durability.md`](../public/topics/core/durability.md) — surviving the loss of the disk.
- [`../../test/scripts/state_persistence.sh`](../../test/scripts/state_persistence.sh) — the manual smoke check for this capability's data-plane half.
