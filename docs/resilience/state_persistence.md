# Controller state persistence

Implementation notes for the per-Entity WAL, snapshot, and
time-travel infrastructure on the Elixir controller. Settles the
details of the per-Entity WAL infrastructure; keep this in sync
with `lib/flezha/state/`.

> Per-Entity WAL persistence (this doc) is independent of and
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
    .layout                            # namespaced-layout marker (migration discriminator)
    {namespace}/                       # `_system` for pre-namespace state
      {operative_class}/
        input.log                      # durable input log (effectively-once tier)
        effectively_once               # declaration sidecars, written once at bind
        checkpoint_ack
        {procedure_id}/
          {entity_key}.wal
          {entity_key}.snap.{seq}
          {entity_key}.outbox.dedup
      {instance_id}/
        {instance_id}/
        proc.journal                     # workflow step progress
        inbox.wal                      # workflow durable inbox
```

The path triple `(operative_class, procedure_id, entity_key)` uniquely
identifies an Entity's state — the namespace level is resolved by
`Flezha.State.Registry` (never carried on the wire) from the
class→namespace claim an Operative records when it binds, else from
the on-disk layout, else `_system`. A class owned by another
namespace refuses the claim, which is what makes cross-namespace
state hijack impossible at the path seam; a class migrated under
`_system` is adopted (one atomic rename) by the first real namespace
that claims it. Persistence does not include the `runner_id` — that
is an addressing concern at the SDK boundary. When a runner pod
restarts and rotates its `runner_id`, state under this layout is
preserved and shipped to the new runner on Entity attach via
`RestoreInitState`. The one-time `Flezha.Boot.StatePathMigrator` runs
at `Application.start/2` to migrate an older runner-keyed layout into
the flat one and the flat one under `_system/`; the `.layout` marker
is what makes re-runs no-ops (the legacy and namespaced trees are
structurally indistinguishable).

`entity_key` is URL-safe-base64-encoded with a leading `_` when it
contains characters outside `[A-Za-z0-9_.-]`. Round-trippable via
`Flezha.State.Registry.encode_entity_key/1` and `decode_entity_key/1`.

A workflow instance's `proc.journal` and `inbox.wal` ride the **same**
WalWriter + FsyncBatcher substrate as an Entity, under two reserved
entity keys (`_proc_journal`, `_proc_inbox`). They write no dedup side-file,
which is what keeps `Registry.scan_dir/1` from ever surfacing one as a
phantom Entity.

Configurable root via `:flezha, :state_root` (default `app_data/state`).

## Module map

| Module | Role |
|---|---|
| `Flezha.State.Registry` | Path resolution + directory enumeration. No GenServer, no I/O beyond `File.ls/1`. |
| `Flezha.State.Wire` | Wire-frame codec for `entity_state_change` / `entity_effect_record` / `entity_state_snapshot` payload-ids (+ the postcard `EffectRecordBody` codec). |
| `Flezha.State.Metrics` | Per-Entity `:counters` references; lazy allocation in a `:ets` table. |
| `Flezha.State.Outbox` | Per-Entity dedup file (`<entity_key>.outbox.dedup`): `last_persisted_seq` + `outbox_flushed_seq` + realign `epoch`. A rebuildable cache advanced at fsync-batch cadence (never per record); boot repairs it from the WAL scan. Also the delivery-confirmation seam that moves the flush cursor: mints the `flz.ack` token, and routes a sink's hold / confirm / park back to the writer that owns it. |
| `Flezha.State.InputLog` | Effectively-once tier: per-operative durable ingest journal (`<class>/input.log`), min-watermark tracker + truncation, and the source-ingestion credit ledger. |
| `Flezha.State.PersistenceResolver` | Per-Operative config resolution (`operative_persistence`, `time_travel_retention_hours`, `compact_after_bytes`). |
| `Flezha.State.Supervisor` | `DynamicSupervisor` for per-Entity `WalWriter` GenServers + a process registry for name lookup. |
| `Flezha.State.WalWriter` | One GenServer per Entity. Owns the WAL file handle; opens it in `handle_continue/2` so creation never blocks the caller. |
| `Flezha.State.FsyncBatcher` | Single shared GenServer driving `:file.datasync/1` across dirty writers on a fixed cadence. |
| `Flezha.State.Snapshot` | Snapshot file lifecycle (`.tmp → fsync → rename → truncate`). |
| `Flezha.State.Retention` | Periodic sweeper enforcing `time_travel_retention_hours`. |
| `Flezha.State.Restore` | Assembles `RestoreInitState` payload (snapshot + tail WAL). |
| `Flezha.State.TimeTravel` | Point-in-time query engine backing `flz state at`. |
| `Flezha.State.WalTransfer` | Cross-controller, destination-pulled copy of one Entity's durable files at migration time. See below. |
| `Flezha.State.DurabilityTier` (+ `Shipper` / `Hydrator` / `Manifest` / `Backend`) | Opt-in remote artifact shipping and boot/bind-time hydration — off by default (`backend: none`). See [`../remote_durability_tier.md`](../remote_durability_tier.md). |

## Cross-controller transfer

`state_root` is **node-local and never assumed shared**. That makes an
operative's durable state a pin to the controller whose disk holds it, and a
migration to another controller has to move the files or lose them.

A HotSwap handover normally carries the state in-band — but
`Flezha.Operative.Lifecycle.prepare_handover/1` sources it from the **runner**, so the one
migration triggered by a runner's death (`hub_migrate`, when a controller loses
its last runner for a namespace) necessarily ships an empty payload. That is
where `Flezha.State.WalTransfer` runs: the handover stamps `source_node`, and an
empty payload makes the destination pull every Entity of the operative from
that node before driving the ordinary replay.

The transfer is **destination-driven** — the source only answers reads
(`entities/1`, `manifest/1`, `read_chunk/3` over `:erpc`) — so a source that
dies mid-transfer fails the pull rather than stranding a partial file. Each file
is written to a `.transferring` sibling, fsynced, then atomically renamed;
any failure removes the artefact and leaves that Entity cold rather than
half-restored. A requested name is resolved against
`Registry.entity_files/1`, never joined onto the state directory, so a peer
cannot read arbitrary paths off the node. Telemetry:
`[:flezha, :state, :wal_pulled]` / `[:flezha, :state, :wal_pull_failed]`.

Source files are left in place; the Retention sweeper reaps orphans.

### Moving one Entity to another owner (partition rescale)

`WalTransfer.move_entity/3` is the same machinery pointed at a *different
Operative class*: a partition rescale recomputes which member owns each key
group, and every Entity whose owner changes moves into the new member's tree
under the same procedure and key. On one controller that is an atomic rename
per file; across controllers it is the pull above with `:as` naming the
destination Entity, and the source copy is removed only after every file has
landed. Either way the move is atomic per Entity, so re-running
`flz operative scale` after a crash re-derives what is left from what is on
disk and moves nothing twice.

The rescale gathers the group's Entities on the node that ran it and stamps
`state_source_node` on the new members' opts; a member ProcessHub then places
elsewhere pulls its class tree from there before restoring. See
[`../partitioned_operatives.md`](../partitioned_operatives.md).

This is **one-shot at migration time, not replication**. A controller that dies
together with its disk takes its operatives' state with it — unless the
**remote durability tier** is enabled, which ships the same files as immutable
artifacts to a Postgres or S3-compatible backend and hydrates them back into
this layout on a fresh node. Off by default; see
[`../remote_durability_tier.md`](../remote_durability_tier.md).

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
every `entity_state_change` frame. To keep the channel's loop off the
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
A `WalWriter` casts `mark_dirty(entity_id)` on the first write of an
fsync window (not on every write); each tick casts `:fsync` to every
dirty writer, so the writers sync concurrently in their own processes
and a writer stuck behind a deep mailbox delays no sibling's durability.
`flush_now`/`flush_all` call instead, and a writer that does not confirm
stays dirty. A (re)started batcher seeds its dirty set from every
registered writer. Tick interval defaults to `5 ms` (≈ 200 Hz),
configurable via `:flezha, :agent_fsync_interval_ms`. Legal range
`[1, 1000]` ms; out-of-range values fail `init/1` loudly.

## Snapshot lifecycle

Compaction is triggered automatically by the `Flezha.State.Watchdog`
sweep on **size or age** (plus manually via `flz state rotate`):

* **Size** — un-snapshotted WAL bytes past the resolved
  `compact_after_bytes` (default **8 MiB**). The SDK's declared
  `compact_after_bytes()` rides the `AddOperative` metadata (the smallest
  across the operative's procedures, when it differs from the default),
  is persisted in `ClassRegistry`, and is resolved per operative class by
  `PersistenceResolver.compact_after_bytes/2` ahead of the global
  `:flezha, :default_compact_after_bytes`; an operator
  `:compact_after_bytes => %{class => bytes}` map overrides both.
* **Age** — the oldest un-snapshotted WAL record is older than
  `:flezha, :compact_after_age_ms` (default 300 000 ms; `0` disables),
  so low-throughput Procedures still get periodic baselines.

Both read the `WalWriter`'s in-memory accounting (no extra disk scan),
share the Watchdog's in-flight `compactions` map as the re-enqueue guard
(size+age can never double-enqueue), and emit
`[:flezha, :state, :compaction_trigger]` with `trigger: :size | :age`.

1. Runner (or compaction trigger) requests a snapshot — controller ships
   `CompactEntity` to the SDK over the control channel.
2. The SDK locates the addressed live Entity, **drains any pending changes
   first** (so its in-memory state equals the shipped WAL), serializes
   `Self::State` under the state lock — **postcard** in Rust, **Kryo** in Java,
   the same codec the restore path decodes, never the JSON migration
   serializer — and ships the bytes via `entity_state_snapshot` stamped with
   the highest change seq the snapshot already folds. It answers
   `CompactEntityResponse{snapshot_seq, snapshot_bytes_size}`.
3. `StreamChannel` decodes the frame and calls
   `Flezha.State.Snapshot.write_snapshot/4`:
   - opens `<key>.snap.{seq}.tmp`
   - writes `<<header::binary, snapshot_bytes::binary, crc32::32-big>>`
   - `:file.sync/1` + `:file.close/1`
   - `:file.rename/2` to `<key>.snap.{seq}`
   - `WalWriter.truncate_up_to/2` keeps records with `seq > snapshot_seq`
     (drops `≤ snapshot_seq`). Because the SDK drained before capturing the
     seq, no record the snapshot lacks is dropped, and the next append (at
     `snapshot_seq + 1`) is past the re-established baseline — never a
     dedup-drop.

Crash safety: the rename is atomic; a crash mid-write leaves a stale
`.tmp` (filtered out by `Registry.scan_dir/1`). A crash between rename
and truncate is also safe — applying the snapshot then folding the
already-folded WAL records is idempotent (umbrella commitment on
`apply_change`).

### The compaction failure path

`Flezha.State.Commands.dispatch_compact_entity/2` inspects the `CompactEntityResponse`:
a `{snapshot_seq: 0, snapshot_bytes_size: 0}` answer against a **non-empty**
WAL is a failed handshake (never a completion), reported as
`[:flezha, :state, :compaction_failed]`. A snapshot that never arrives is
bounded by `:flezha, :compaction_timeout_ms` (default 30 s).

Repeated failure for an Entity is not retried at the sweep interval forever:
the Watchdog holds one `Flezha.Convergence.Backoff` keyed
`{:compaction, entity_id}` (schedule 5 s → 30 s → 60 s → 300 s). Below the
give-up threshold a still-live, still-correct Entity is **not** resynced (a
re-restore fixes nothing and risks availability); the backoff-gated trigger
sweep is the retry. At the give-up threshold the WAL has grown across every
rung, so the Watchdog emits `[:flezha, :state, :compaction_gave_up]` (an
operator-alertable alarm naming the Entity and attempt count) **and**
dispatches a real resync with the `runner_id` the `WalWriter` status carries.
Completion (`compaction_completed`) resets the strike.

## Retention sweeper

`Flezha.State.Retention` runs every `:flezha,
:time_travel_retention_check_interval_ms` (default 60 000 ms). For each
Entity under `Registry.all_entities/0`:

1. `retention_hours = Flezha.State.PersistenceResolver.retention_hours(operative_class)`
2. If 0: keep only the latest snapshot, truncate WAL to its seq.
3. Otherwise: find the latest snapshot whose mtime is `≤ now -
   retention_hours * 3600`. That seq is the "oldest snapshot to keep";
   reap older snapshots and rewrite the WAL keeping records with
   `seq > kept_seq`.

WAL rewrite is `entity.wal.rewriting → rename` so a mid-rewrite crash
leaves the original WAL intact.

## Flow control

### Credit-grant (primary)

Runner→controller flow control on the state stream is credit-based:
park, never drop. On the first `entity_state_change` frame of a stream
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
* **Sender parks on exhaustion** — the runner core (`flz_core/src/credit.rs`,
  one implementation for every host) debits each frame before the
  sink write and parks the flush path (under the buffer lock, so
  producers throttle at the buffer boundary) until the next grant. Parks
  are bounded-wait loops; the `credit_parked` counter advances while
  parked and ships in `data_heartbeat` metadata, so a credit-parked
  runner classifies as fed-but-parked, not silently frozen. Grants and
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
`EntityMailboxStatus{status: "full"}` to the SDK (routed via the
writer's `owning_runner`); draining below `wal_low_watermark_bytes`
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
`wal_shed_total` (per-Entity `Metrics` counter); the resulting seq
gap drives the existing `Recovery.resync` handshake (once per shed
episode), so the SDK — authoritative for entity state — re-sends the
missing Changes. Back-pressure sheds, never blocks: the async cast path
stays non-blocking so a slow disk cannot stall the data-plane channel.

For pre-credit sessions this is the safety net against a slow disk or
an SDK ignoring the status flags. For a credit-speaking session the
ledger refuses an overrunning frame before the hard bound is reachable,
so a conformant sender never sheds — any shed recorded against a
credit-speaking session emits `[:flezha, :state, :credit_anomaly]`
(kind `:shed`) and is an alerting condition, never expected
back-pressure.

## Live read (`flz state get`)

```
flz state get <operative/procedure[/entity_key]>
```

The current in-memory state of one live Entity (or of one Procedure's
Entities), as JSON, with the `change_seq` it was read at.
`Flezha.Service.CliManager.state_get/3` resolves the namespace holding the
Operative, authorizes it as `:"state.get"`, and issues the existing
`EntityState` control round-trip narrowed by the selector; the seq rides the
reply frame's `flz.change_seq` metadata, so the JSON body keeps the shape the
migration/handover caller already decodes.

**The value is the freshest one there is, and may be ahead of durability.**
The SDK serializes under the Entity's state lock without draining pending
changes, so the reply includes changes not yet flushed to the WAL. That is
strictly better than a durable read provided the caller knows which version it
got — hence the seq. The read triggers no compaction, snapshot, or truncation.

**A non-resident Entity is refused, never served from disk.** The refusal names
which of: idle-evicted, instance suspended, no Entity by that key, Operative
unbound — and points at `flz state at`. There is deliberately no durable
fallback: dynamic-mode eviction is destructive (the recreate cold-starts, so
there is no older value), the controller cannot decode the SDK's WAL bytes
(postcard/Kryo, stored opaquely), and a fallback would mean binding a runner and
restoring an Entity as a side effect of a read.

The reply is bounded by `:wal_high_watermark_bytes` — the same ceiling the
workflow handoff uses. Over it, the read is refused with the byte count and the
selector that would narrow it; no new configuration key.

Full surface, refusal table and worked examples:
[`../entity_state_live_query.md`](../entity_state_live_query.md).

## Time-travel CLI

```
flz state list <operative_class>
flz state at <operative/procedure/entity_key> <seq | iso8601-timestamp>
flz state rotate <operative/procedure/entity_key>
flz state retention <operative_class> <hours>
flz state delete <operative/procedure/entity_key>
```

Implemented in `cli/src/state.rs` (Rust binary) which RPCs into
`Flezha.Service.CliManager.state_*/N` on the running controller. Path
strings like `MarketOperative/OhlcProcedure/M1` are resolved against
`Flezha.State.Registry.all_entities/0` to confirm an Entity exists
on disk; the live runner binding (when needed for `state rotate`) is
looked up from the runtime ProcessHub registry.

`flz state at` returns the assembled snapshot + change-log bytes plus
the resolved terminal seq. The full SDK-side replay (folding bytes into
JSON-serialized terminal state for human display) is wired on top of
this engine.

`flz state retention` writes via `Application.put_env(:flezha,
:time_travel_retention_hours, %{<OperativeClass> => hours}, persistent: true)`;
the next `Retention` tick reads the new value. Persistence across an
Application boot requires writing it to `runtime.exs`.

## Dynamic-entity hard delete

Dynamic entity mode adds a destructive delete path. When a
dynamic-mode Procedure opts into an idle TTL (`entity_idle_ttl() ->
Some(ttl)`; default is `None` = no eviction), the SDK reaper evicts an
idle, persistence-enabled Entity by shipping a fire-and-forget
`delete_entity_state` frame on the secondary stream (payload-id
`Flezha.State.Wire.payload_id_delete_entity_state/0`; body is the bare
structured `entity_id`). `Flezha.Runner.StreamChannel` routes it to
`Flezha.State.Supervisor.delete_entity/1`, which stops the per-Entity
`WalWriter`, drops any pending `FsyncBatcher` dirty mark, and unlinks the
`{key}.wal`, `{key}.snap.{seq}`, and `{key}.outbox.dedup` files
(`Flezha.State.Registry.delete_files/1`). The delete is **idempotent** —
deleting a never-seen or already-gone entity succeeds.

A later input for the same key cold-starts a fresh Entity (new WAL from
seq 0); there is **no** restore-from-disk on recreate. The operator-driven
counterpart, `flz state delete <operative/procedure/key>`, calls the same
`delete_entity/1` and returns a confirmation (the reaper path is
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
| `:flezha, :state_root` | `"app_data/state"` | Root for per-Entity files. |
| `:flezha, :agent_fsync_interval_ms` | `5` | `FsyncBatcher` tick cadence. Range [1, 1000]. |
| `:flezha, :wal_high_watermark_bytes` | `16 * 1024 * 1024` | Soft back-pressure trigger (signals SDK). |
| `:flezha, :wal_low_watermark_bytes` | `high / 2` | Back-pressure release. |
| `:flezha, :wal_hard_watermark_bytes` | `high * 4` | Hard un-fsynced-bytes bound; past it `append_async` sheds. |
| `:flezha, :wal_pending_msg_cap` | `5_000` | Hard mailbox-depth bound; past it `append_async` sheds. |
| `:flezha, :time_travel_retention_check_interval_ms` | `60_000` | `Retention` tick cadence. |
| `:flezha, :time_travel_retention_hours` | `%{}` (default 6 from PersistenceResolver) | Per-Operative retention overrides. |
| `:flezha, :default_time_travel_retention_hours` | `6` | Global default when no override. |
| `:flezha, :default_compact_after_bytes` | `8 * 1024 * 1024` | Per-Procedure compaction size threshold (env `FLEZHA_COMPACT_AFTER_BYTES`). |
| `:flezha, :compact_after_age_ms` | `300_000` | Age floor for compaction; `0` disables (env `FLEZHA_COMPACT_AFTER_AGE_MS`; `0` in `:test`). |
| `:flezha, :operative_persistence` | `%{}` | Per-Operative-class runner override (`true` / `false`). |
| `:flezha, :output_watchdog_interval_ms` | `10_000` | `Watchdog` sweep cadence. Min 100 ms. |
| `:flezha, :output_stall_timeout_ms` | `%{}` (default 60 000) | Per-Operative-class output-staleness window; `0` disables. |
| `:flezha, :default_output_stall_timeout_ms` | `60_000` | Global default when no per-class override. |
| `:flezha, :compaction_timeout_ms` | `30_000` | `CompactEntity` → snapshot handshake bound; `0` disables. |

### Self-healing recovery (`Flezha.State.Watchdog` + `Flezha.State.Recovery`)

A live Entity's persistence/output path must not be able to stall
silently and permanently. Three operative-level failure edges are
detected here; two recover via the shared `RestoreInitState` resync
(`Flezha.State.Recovery.resync/2`), and the third is **emit-only** —
the `Watchdog` is a sensor and the convergence Loop owns every
operative-level remedy. (With a remote durability backend configured,
the sweep additionally latches RPO breaches and emits the alarm-grade
`[:flezha, :state, :rpo_exceeded]` — also emit-only.)

* **Seq gap (populated WAL)** — a Change with `seq > last_persisted_seq
  + 1` while `last_persisted_seq >= 0` is no longer a terminal
  `{:error, {:gap}}`; `Flezha.State.WalWriter` emits
  `[:flezha, :state, :seq_gap]` and resyncs so the SDK rewinds and
  resends from `terminal_seq + 1`.
* **Output staleness** — `Watchdog` flags an Entity that is still
  receiving Changes but whose persisted seq froze past
  `output_stall_timeout_ms` and emits
  `[:flezha, :state, :output_stall]`. It does **nothing else**: the
  remedy is the convergence Loop's output-stall ladder
  (`resync_runner`, escalating to `rebind_operative` — see
  `docs/resilience/convergence_loop.md`), under one per-target backoff.
  The Watchdog's former direct `Recovery.resync/2` call and its
  `last_resync` dedup map are deleted; they were a second, uncoordinated
  actor racing the Loop's own rebind. A genuinely idle Entity (no
  Changes arriving) is not flagged.
* **Compaction timeout** — a `CompactEntity` whose snapshot does not
  return within `compaction_timeout_ms` emits
  `[:flezha, :state, :compaction_timeout]` and resyncs; compaction stays
  retriable. This one stays with the Watchdog because it owns the
  handshake end to end — it dispatched the `CompactEntity`, so it is the
  only thing that can time it out and re-dispatch it.

**Sweep freshness.** Each completed sweep stamps
`Flezha.Convergence.Snapshot.observe_sensor(:watchdog, at, period)`. A
Watchdog that wedges without crashing stops stamping, and `/healthz`
reports `200 degraded, reason: sensor_stale` within one poll instead of
silently losing detection. The mark is written by the sensor, never
polled from it, so reading it cannot block on the wedged process.

### Baseline recovery: push vs pull

The resync direction is chosen by who holds authoritative history
(`Flezha.State.Recovery.dispatch_resync`):

* **Controller holds a baseline** (snapshot and/or WAL records) →
  **push** `RestoreInitState` (the existing handshake).
* **Controller holds no baseline** while the runner is live → **pull**
  a full-state snapshot via `CompactEntity`. The dispatch path never
  ships a `RestoreInitState` whose assembled payload is empty
  (`Restore.has_persisted_state?/1`, which treats a zero-byte WAL as no
  state) — an empty payload would rebuild the live Entity to
  `init_state` and wipe its accumulated state.

The pull is data-plane-triggered on cold takeover: the first
`entity_state_change` arriving with an empty WAL
(`last_persisted_seq == -1`), `seq > 0`, and no runner-restart realign
puts the `WalWriter` into **awaiting-baseline** — the frame is shed
(counted in `wal_shed_total`, not an error, and the SDK is NOT
disconnected), `[:flezha, :state, :baseline_seed]` fires once per
episode, and a `CompactEntity` is dispatched. Further gap frames shed
quietly until the pulled `entity_state_snapshot` lands; its seq becomes
`last_persisted_seq` (the snapshot encodes the shed deltas' effects) and
the runner's next delta appends contiguously. The pull is **retried** on
the resync interval (`:wal_resync_retry_interval_ms`, default 1 s) for as
long as the Entity waits — a pull that failed to dispatch (a torn control
channel answers `:no_control_channel`) would otherwise strand the Entity
in permanent shed, because only the pulled snapshot clears the state.
At most one pull is in flight per Entity per interval, so a per-frame gap
flood cannot fan out. A stuck pull is bounded by the compaction-timeout
sweep. A gap on a *populated* WAL remains a protocol violation
(`{:error, {:gap, ...}}`).

## What a restore does to input already queued

A `RestoreInitState` lands on an Entity that may be holding inputs in its
mailbox — a rebind does not stop the runner, and `AddOperative` is
idempotent, so the live instance and its queue survive into the restore.
What happens to that queue depends on whether the Controller can hand it
back:

* **Classes whose inputs are journaled before forwarding** — the
  effectively-once tier (input log) and durable workflow instances (proc
  inbox) — have their mailbox **cleared**. The queue is a subset of the
  durable log the Controller re-forwards, so keeping it would double-apply
  it and, during a correction, re-order it behind the replayed slice.
* **Every other class** — including a plain stateful stream operative,
  the default configuration — keeps its queue. `Ingest.journal_input/3`
  passes those inputs through un-journaled (see
  `docs/resilience/data_durability.md`), so the mailbox is the only copy
  they have and discarding them is loss with nothing to recover from.

The rule lives once, in the runner core
(`EntitySlot::discard_queued_input_on_restore`), so it cannot drift between
hosts. Discards are counted and shipped as `restore_discarded_inputs` in
`data_heartbeat` metadata, and each one logs the Entity it emptied.

The replay buffer, replay flags, and any open checkpoint interval are
cleared on **every** class: those belong to a past the restore replaced.

Known race, independent of the above: a dispatch already in flight when
the restore lands has its Change stamped with a seq that the
`change_seq` re-arm then re-issues, and the WAL dedups the collision
(`seq <= last_persisted_seq`). It costs at most the one in-flight Change,
and fires with an empty mailbox too.

## Runner-restart realign is owner-bound

Within one runner's lifetime the per-Entity seq is monotonic, so a
Change from a *different* session than the one that wrote the current
epoch, at a non-contiguous seq, signals a runner restart: the
`WalWriter` realigns — truncates the WAL, drops snapshots, adopts the
new seq base — and emits `[:flezha, :state, :wal_realign]`. A
same-session low seq stays a legitimate dedup.

The realign is honoured **only from the Entity's current owner
binding**. A newest-wins takeover unbinds the superseded session from
the write path before binding the new one (see
`docs/resilience/runner_identity.md` "The WAL binds to the owner"):
fenced `StreamChannel`s refuse durable-write forwards at the binding,
and the `WalWriter` refuses frames from a session marked superseded
(`FleetManager.superseded_session?/1`) with `{:error, :stale_writer}`
— consulted only when the frame's session differs from the epoch
owner, so the owning session's append path pays no per-record cost.
Refusals emit `[:flezha, :state, :stale_writer_refused]` once per
(Entity, superseded session) episode and count into
`stale_writer_refused_total`; they never spawn a resync (the zombie is
not a party worth repairing toward) and release any credit debit
immediately.

## Persisted `runner_id` is advisory across controller restarts

The `runner_id` field carried in an operative-binding record (replayed
into `Flezha.Operative.Overwatch.init/1` from ProcessHub's durable
registry at boot) is **advisory only**. It identifies the runner the
operative was bound to BEFORE the current controller process started.

On every cold start the Overwatch calls
`OperativeManager.assign_runner/2` inside
`handle_continue({:bind_runner, …}, _)` to re-resolve a live
runner for the operative's `factory_id`. The persisted value is used
for diagnostic logging only (a `rebound from stale runner X to live
Y` line fires when the live runner differs).

Control-plane calls (`AddOperative`, `:ping_runner`, `start_operative_sdk`)
are issued against the re-resolved live runner, never against the
persisted value directly. The bounded-retry governor (see
`docs/operative_architecture.md` "Overwatch lifecycle") re-resolves on
every retry attempt so a fresh runner that comes online during a
backoff window is picked up automatically.

No on-disk format change accompanies this — the field is still
written and read with the same layout. The clarification covers only
the runtime semantics; the source of truth for the live binding is
ProcessHub's registry, not the persisted record.
