# Data and State Durability

The reference for **where Flezha can lose operator state or output, where it cannot, and how
to operate so a planned action never loses data**, for users and operator authors. It
covers the scenario matrix, the hard-kill gap, the delivery semantics (at-least-once by
default, the effectively-once tier), the durable input log and a steps operator's stage commit, the cross-hop
guarantee, timers, the dead-letter queue, late-input correction, the remote durability tier
and the user playbook. Operator *placement* recovery is
[`../internals/process_hub.md`](../internals/process_hub.md); the WAL substrate itself is
[`../internals/state_persistence.md`](../internals/state_persistence.md).

## The two things that can be lost

1. **Operator state** — the accumulated in-memory state of an operator. The authoritative
   live copy is on the operator's **one bound runner**; it is backed up **asynchronously** to a
   per-Cell **WAL on the controller** (the runner streams `cell_state_change` frames, the
   controller batches the fsync, ~5 ms).
2. **Output** — what an operator emits. Output is routed **through the controller** (the
   `:on_output` plugin chain → Kafka / Postgres / HTTP / another operator). It is
   **at-least-once** by default; a flow class that declares `exactly: effectively_once` gets the
   **effectively-once tier** below.

The core invariant: **on a graceful stop no state is lost; on a hard kill a small best-effort
window can lose the most recent un-persisted state.**

Between the two sits the wire itself, and what a runner does when it refuses a frame is decided
by the frame's declared **guarantee class**, never at the write site. A change record, a
snapshot, an eviction's delete, a saga marker, a park payload and the shutdown state ship are
`durable`: held in a bounded drop-oldest ring, the shed counted, replayed in write order on the
next bound stream. A plugin or steps operator emit is `output`: held already framed, so a keyed
output's channel and identity stamps survive the replay. A control frame is refused to its
caller and never held, because the reconnect re-issues the request it belonged to, and the data
heartbeat is held by nothing at all — it reports a moment that has passed. The seam is
`flz_core::state_sink::write` for every runtime built on the shared core and
`Flezha.SDK.Hold.write/3` for the Elixir SDK, which is its own core; a payload id no class table
names is refused at the call rather than given a default. See
[`../sdk/protocol_spec.md`](../sdk/protocol_spec.md) §7.

A third durable artifact is the operator's **plugin manifest**: it lives in the registry cspec
and is written cluster-wide (`propagate: true`), so an operator restarted from any node
re-attaches its full plugin set and the convergence loop re-adds anything missing.

## Scenario matrix

| Scenario | Operator state | Output | Why / how |
| --- | --- | --- | --- |
| **Graceful runner stop** (SIGTERM / rolling deploy / `disconnect()` / `Drop`) | **No loss** | At-least-once | The SDK (`flz_core/src/shutdown.rs`) serializes each operator's final state, ships it, and waits for `RunnerShutdownAck` before exiting. |
| **Single runner SIGKILL** under load | **Brief best-effort loss** | At-least-once | No drain; the operator rebinds and restores from the controller WAL, which may lag the last few changes. |
| **All runners SIGKILL'd at once** under load | **Brief best-effort loss** | At-least-once | Same WAL-restore path with no live copy anywhere, so the lag is reliably exposed. |
| **Runner OOM / container crash / node death** | **Brief best-effort loss** | At-least-once | Same as SIGKILL — no drain runs. |
| **Controller graceful restart / rolling deploy with a live peer** | **No loss** | At-least-once | The new instance syncs the registry from the surviving peer; runners stay connected. |
| **Controller plain restart, sole instance** | **No loss** | At-least-once | The first reconcile round replays the operator list from the durable registry; `/healthz` reports `recovering` until rebound. No user step. |
| **Whole cluster down → cold start** | Recovered from the durable registry | n/a | No peer survives, so the reconcile's difference is the whole list and disk is the only record. |
| **Operator migrated to a controller with no local WAL** (node loss, redistribution, lost hostPath), runner live | **No loss** | At-least-once | The WAL is node-local (with the remote tier, hydrated from the manifest). Absent both, the first `cell_state_change` into an empty WAL triggers a rate-limited `CompactCell` **pull**: the runner ships a full snapshot that becomes the baseline. The controller never pushes an empty `RestoreInitState`. See [`../internals/state_persistence.md`](../internals/state_persistence.md) "Baseline recovery: push vs pull". |
| **Node rejoins a live cluster** | Synced from peers; local durable copy kept | n/a | The reconcile's difference against live peers is empty, so the node starts nothing. The on-disk copy is never discarded to force that. |
| **Kafka broker down, runners alive** | No loss | Buffered, resumes | The producer reconnects; the queue drains when the broker returns. |
| **Sustained overload** (changes outpace fsync) | **No loss** | At-least-once | Credit-grant flow control: the sender's byte window (`:wal_high_watermark_bytes`, 16 MiB) is minted back only on WAL fsync, so an exhausted window *parks* the SDK's flush path — park, never drop. A shed on a credit session emits `[:flezha, :state, :credit_anomaly]`. |
| **Steps operator runner SIGKILL** (durable, the default) | **No loss** of a committed tasklet; an uncommitted one re-runs | Never lost; released only after its stage commits | Respawns on a new runner, restores committed Cell state behind the restore-before-run gate, and resumes at the **first un-journaled tasklet**: `task.journal` records each completed tasklet, the effect records its Cells wrote and its handoff, and an entry is appended only once every record it names is durable, so committed tasks are skipped, not redone, over state that is on disk. Inspect with `flz state journal`. |
| **Looping steps operator runner SIGKILL** (durable) | **No loss** of a committed tasklet | Never lost; released only after its stage commits | Resumes at the iteration after the last `iter_committed`; the in-flight one re-runs from its own last committed task. Unconsumed input is still in the instance's durable input log. |
| **Input sent to a suspended / crashed / migrating steps instance** (durable) | **No loss** | n/a | Appended to the instance's durable input log (`input.log`) before forwarding, delivered on rehydrate; a suspended instance is woken by the append. |
| **Input sent to a sleeping flow instance** (its class keeps its work and sleeps) | **No loss** | n/a | Appended to the instance's input log before forwarding; the append wakes it, its Cells are restored before the log's tail is re-forwarded, and the message is folded once, also when the controller restarts while the instance is awake. |
| **Controller redeploy while an instance of either run is suspended** | **No loss** | n/a | Suspension is durable state (the park record in the class directory + input log + snapshots): the instance is rediscovered from disk, stays evicted, and rehydrates on the next input or its wake deadline. |
| **Steps operator runner SIGKILL** (`durable: false`) | **Full: re-runs from the top** | At-least-once | No WAL; the operator re-runs from the top on its next message and the messages waiting in its memory are lost. Idempotent by author contract, or keep the work (`local`). |
| **Failed turn of a `per_input` keep-progress session** (durable) | **No loss**: the turn's message is parked | The turn's committed outputs stay out; its `compensate` hooks undo them | The message is parked in the DLQ before the iteration is journaled `dead_lettered`; the turn is undone and the next message runs. A crash after the record re-runs the owed undo; a crash before it re-runs the turn and parks it at most once. |
| **Steps operator SIGKILL mid-compensation** (durable) | Compensation resumes | n/a | Compensation-intent / -done markers ride the restore; the respawn finishes the outstanding undo hooks and skips the done ones. |
| **Elixir operator process dies** (killed, or any crash outside a clean stop) | Whatever its class keeps after any rebind | Whatever its class keeps after any rebind | The runner drops the instance and reports `OperatorExited`; the controller parks it and the convergence loop rebinds it, restoring its Cells before the input log's tail above the floor is re-forwarded. A durable steps run resumes from its journal. |
| **Elixir Cell dies with unfinished input** (killed, or an exit linked into it) | **No loss** of input that has an offset | At-least-once, or effectively-once on the tier | The Operator ends instead of re-minting the Cell, and the rebind above re-forwards the log, so every input the dead Cell held runs again. With no offset held, the Cell is minted again from its slot. An instance that dies on every bind is retried on its backoff rung. |
| **Poison payload** (fails every retry) | **No loss** | Cell continues | Retry-then-park: after `retry_max` attempts the payload is parked durably to the DLQ *before* the Cell advances. Only retention (30 d / 4 GiB, loud) ever discards it. |

## Why output can briefly get ahead of state (the hard-kill gap)

The runner streams **output frames** and **state-change frames** independently, and the
controller emits default-tier output before the matching state is fsynced. On a hard kill,
output can outlive the durable state (Kafka holds `total=135`, the WAL holds `128`) and the
restored operator comes back slightly stale. The default tier does **not** close this with
per-record acking; planned stops drain to zero loss, and an operator that needs the gap closed
declares the effectively-once tier.

**The fsync window.** The WAL writer does not fsync per record: `State.FsyncBatcher` casts one
`:fsync` to every dirty writer per tick (`:agent_fsync_interval_ms`, default 5 ms; the tick runs
only while a writer is dirty) and each
writer runs its own `:file.datasync/1`. Records written inside the last tick can be lost on a
hard kill; lowering the interval narrows the window at the cost of I/O pressure. Per-record WAL
fsync is not supported (the audit log's `:per_record` mode is a separate writer).

**A corrupt record is refused, not repaired.** A record log is truncated only where the reader can
prove that no complete record can begin after the failure point. That proof holds for the torn
tail a crash mid-append leaves, and the repair is now reported instead of performed in silence. It
does not hold for a record whose crc32 fails with complete records behind it: a flipped bit under
a record the controller already acknowledged is not a torn tail, and deleting the acknowledged
records after it to make the log parse would trade data for a file that opens. Such a record is
refused. The bytes are left exactly as they are, the process that owns the file stays alive and
refuses to serve rather than being restarted into the same file, and the refusal is reported once
at `error` severity plus one `[:flezha, :state, :record_frame, :refused]` telemetry event naming
the file, the last seq that verified and the byte offset. The Cell's restore payload is refused
the same way, so the operator is left unbound rather than binding on the records that happened to
precede the failure — the fate "a refused restore" already has. Recovery is a user decision
with the bytes still in hand: hydrate from the remote durability tier, take a peer's copy, or
inspect the file.

## Delivery semantics: at-least-once default, effectively-once tier

### Default: at-least-once with identity

Output leaves as it is produced; nothing at the controller remembers what was delivered, so a
crash-replay can re-emit. A channel declared `.keyed()` stamps every plugin-bound frame with
`flz.cell`, `flz.seq` (`<change_seq>.<emit_ordinal>`, from the durable WAL seq) and `flz.key`
(the Cell's own key, or the key an emit supplied via `ctx.emit_keyed(key, value)`). The stamps
derive only from replay-stable inputs, so a restore replay re-emits **identical** stamps and a
consumer that upserts on `flz.key` absorbs duplicates exactly. The determinism contract — no
ambient time, UUIDs or external reads in `run` / `apply_to` / `compensate`; use `ctx.now()`
and `ctx.stable_id()` — is documented on the `Tasklet` trait and checked by
`TaskletTestHarness::replay_emits_equal`.

Two limits of the default tier: `flz.seq` is not an exact append-only dedup handle (invocations
with no state change can collide on it), and there is no `flz.epoch` — the epoch belongs to the
controller's outbox lineage. A conforming sink reads a keyed default-tier record as epoch `0`
and orders by `flz.seq`; a *regressing* seq from a Cell that cold-started without a baseline
is refused rather than guessed. The epoch, bumped by a WAL realign or a stream correction,
resolves that and exists only where the controller's outbox mints the identity: under the tier
and on a durable steps operator's Cells.

Nothing journals a default-tier input, which also decides the fate of input queued in a
Cell's mailbox when a restore lands: with no log to re-forward from, the queue is the only
copy, so the restore keeps it. Under the tier, and for a class that sleeps, the restore
discards it (`discard_queued_input_on_restore`) because the input log re-forwards it.

**Nothing drops by default.** `DropNothing` is the default overflow policy for every channel
kind in both SDKs and the FFI host; an operator that prefers to shed declares
`.on_full(DropNewest)` or `.on_full(DropOldest)`. The Kafka producer's `:queue_overflow` defaults to
`:drop_nothing` for every record. Non-keyed channels are plain at-least-once.

*What `DropNothing` guarantees, per channel kind.* On an **inter-tasklet** channel the
producing Cell is withheld further work until the push lands — its push holdback holds the
value, its gates stay held, and the flusher holds its durable records with it, so the consumed
offset can never become durable ahead of an edge output derived from it. On an **entry**
channel the shared transport reader stalls instead, which carries the backpressure to the
controller. On a **plugin-bound** channel there is no SDK-side mailbox at all and the policy is
a compile error. Nothing is discarded in any of them.

Once a push lands, the value also rides the producer's record, wherever that record carries the
consumed offset ([the input floor](#the-input-floor-a-processed-prefix)): a value lost with the consumer's mailbox, before the consumer's own
record was durable, is delivered again from there, so the producer's floor skipping the
re-forwarded input strands nothing. One window this does **not** close, named so nobody reads
more into the guarantee: **the controller's send buffer**, which still discards the newest frame
when a runner stops reading (`flz_nif/src/driver/mod.rs`, `StreamState::offer`). The
backpressure chain is complete inside the runner and breaks at its last hop.

### Effectively-once tier (opt-in, `exactly: effectively_once` in the class's run)

The built-in composition *at-least-once delivery + deterministic key + idempotent sink =
effectively-once*. Every plugin-bound channel must be `.keyed()` (refused at build time
otherwise), and the tier is built by the shared runner core, so Rust and Java earn the same
guarantee.

- **Durable input log.** `Flezha.Operator.Ingest.ingest/4` appends every inbound payload to a
  per-instance `input.log` before forwarding, stamped `flz.offset`; the log answers once the
  record is synced. It is the one durable input log, shared with a durable steps operator (below). The
  host never waits on the log (`InputLog.append_async/5`): the input is forwarded when the log
  answers, and a zero-credit park lands on the source's call alone. A repeat of a `flz.msg_id`
  the log still retains is absorbed; a message without an id is always appended. Retention
  truncates only below the instance's floor ([below](#the-input-floor-a-processed-prefix));
  a frozen watermark past the size threshold emits
  `[:flezha, :state, :input_log_frozen_watermark]`.
- **Atomic effect record.** State changes, outputs, consumed offset, dead-letter entry and event
  time form ONE `cell_effect_record` under ONE fsync. No partial combination is ever durable
  and effect records are never coalesced away.
- **An inter-tasklet output's offset is never durable ahead of the output.** The invocation is
  treated as unresolved until every edge push has been admitted to its consumer's mailbox: its
  changes, its plugin outputs and its offset are held out of the flush buffer until then,
  exactly as a failed-but-retryable attempt's changes are. A fan-out invocation holds its record
  until **every** consumer's push has landed; a replay after a partial fan-out re-emits to all
  of them and each consumer's own restored floor decides whether it is a duplicate — which is
  how at-least-once edge delivery composes into effectively-once without a second mechanism.
- **An inter-tasklet value rides its producer's record.** The effect record that consumed the
  input also keeps each edge value, through the channel's codec, with the offset it derives
  from. Between the push landing and the consumer's own record becoming durable the value
  lives in the consumer's mailbox, which a crash or a restore loses; the restore then delivers
  the kept value again to every consumer whose own records do not acknowledge that offset,
  ahead of the re-forwarded tail, while the producer's floor skips the re-forwarded input. A
  consumer that already took it drops it at its floor.
- **Fsync-gated outbox.** Outputs reach the `:on_output` chain only after their record is
  fsynced, stamped `(flz.cell, flz.seq = outbox_seq, flz.epoch, flz.key)` — a sufficient handle
  for exact dedup. On restart the controller re-drives every record above the per-Cell
  `outbox_flushed_seq` cursor in order under identical identity: **never lost, never applied
  twice at a conforming sink**.
- **The cursor advances on confirmation, not on dispatch.** Each flushed output carries an
  opaque `flz.ack` token naming the writer that owns its cursor (`Flezha.State.Outbox`). A
  delivery-guaranteed sink takes a *hold* as it accepts the record and *confirms* when its write
  is durable — Kafka produce acked, Postgres transaction committed, forwarded ingest confirmed.
  A record no sink held completes as soon as the chain has run it, so a chain without a
  guaranteed sink behaves as before. A batch whose retry budget is exhausted **parks** through
  the dead-letter path with its identity attached, and only then may the cursor pass it. A
  delivery whose settlement is lost is re-dispatched by the resettle tick after
  `:outbox_resettle_ms` (60 s); at most `:outbox_inflight_max` (1 000) entries are in flight per
  Cell. Unconfirmed output is visible as `outbox_unconfirmed_ms` in the Cell's stall status
  and reaches the convergence loop as the `outbox_flat` verdict.
- **Reports are batched, never the facts.** One dispatch pass hands its window to the chain in
  one cast and settles it with one report; a sink confirms a completed batch with one report
  naming every entry (a park stays per record). A chain that crashes mid-batch settles nothing,
  and the resettle re-drives every pending entry under identical identity.
- **Conforming sinks out of the box.** The **Postgres** sink (`operator_postgres`,
  `Flezha.Plugin.Provider.Postgres`) upserts every record of a flush batch into
  `flezha_records` (keyed by `flz.key`, else the durable identity) and advances the per-Cell
  `flezha_sink_progress` row in ONE transaction; every upsert is guarded by `(epoch, seq)`
  monotonicity, so re-drives and stale epochs are no-ops, and the commit is what confirms the
  batch. An unreachable database delays the flush (retry with backoff; callers park past
  `queue_max`, 10 000) — delayed, never dropped. The schema is user-owned by default
  (`auto_create: false` fails `health/1` naming the missing tables); a commit stall past
  `unhealthy_after_ms` (30 000) escalates on the plugin-health → convergence path. Unstamped
  records become plain inserts. The **Kafka** producer (`operator_kafka_producer`) uses
  `flz.key` as the message key and carries the durable identity in the `flezha.dedup_key`
  header; see [`../plugins/kafka_producer.md`](../plugins/kafka_producer.md).
- **The websocket sink is not a conforming sink, and does not claim to be.** A plugin bound as
  an output (`operator_websocket`, `Flezha.Plugin.Provider.Websocket`) ships each dispatched
  payload as a frame on the connection it already holds, and answers the hold by its declared
  `sink_ack_mode`: `"none"` (default) confirms when the frame reaches the established socket,
  which is **at-most-once** past that point; `"required"` confirms only when an inbound frame
  echoes the correlation id, which is **at-least-once**. Neither is effectively-once and no
  mode that claims to be is offered — a websocket exposes no transaction to enlist and no key
  to overwrite, so the destination cannot participate. What the sink can do is carry
  `flz.cell` and `flz.seq` in the frame template, and the consumer that tracks the last
  sequence it has applied per cell is the only place the dedup can work. Frames are queued
  while the socket is down (`sink_queue_max`, 10 000; callers park at the bound) and a frame
  whose retry budget is spent parks in the dead-letter store. An enable on a tier operator
  warns rather than refusing: the operator's internal guarantee is unchanged and only its
  websocket hop is best-effort. See
  [`../plugins/websocket_subscription.md`](../plugins/websocket_subscription.md).
- **Source back-pressure.** Ingestion debits a per-operator source credit window
  (`:source_credit_window_bytes`, default = `:wal_high_watermark_bytes`); the min-watermark
  advance mints it back. At zero credit the source adapter's ingest call parks (a websocket
  stops reading its socket), bounding input-log growth against a stalled Cell. A sink outage
  longer than `window / ingest rate` delays ingestion; it never wedges it.
- **Cost-free default.** Operators without the flag keep every path byte-identical. Under the
  tier a slow or unreachable sink backs the outbox up on disk, never in memory.
- **Steps operators** do not declare the tier (it is a member of the flow run only): a durable
  steps operator's Cells write the same effect record as their ordinary durability, released at the
  stage commit below.

Wire shapes: [`../sdk/protocol_spec.md`](../sdk/protocol_spec.md) "Output identity stamps" and
"cell_effect_record".

### Ack granularity: per input, or per checkpoint

The tier acknowledges an input by making its state durable. An operator that folds *many*
inputs into one coherent push — a window that only closes on its boundary — declares
`ack: checkpoint` under `exactly: effectively_once` and calls `ctx.checkpoint()` where its
state is coherent:

- **The consumed offset advances only at the checkpoint.** Changes in between are held in the
  Cell's flush buffer; the checkpoint ships the interval's changes with the closing input's
  offset as one record, one fsync.
- **Emits flush as they happen by default.** A mid-interval emit ships in a record carrying no
  offset and reaches its plugin as soon as that record is fsynced.
- **`outputs: at_checkpoint`** with its `max_delay_ms` holds the interval's outputs
  with its changes and ships them inside the checkpoint record — one WAL append per interval,
  flushed as one batch after the fsync. `max_delay_ms` bounds the latency: the interval closes
  that long after the input that opened it, so a quiet Cell never holds an output
  indefinitely, and an unbounded hold cannot be declared by omission.
- **`every: n`** closes the interval after `n` inputs; an explicit
  `ctx.checkpoint()` still closes it early. The core owns the interval, so every host language
  gets the same rules.
- **A park closes the interval.** An input that exhausts its retries is acknowledged into the
  DLQ together with the interval's state, so a replay starts *after* the poison.
- **Recovery replays the interval.** The Cell is restored to its last checkpoint and the
  controller re-forwards every input after it; the epoch is unchanged and the keyed-sink dedup
  absorbs the re-emitted outputs. A restore discards the open interval.

**The interval is the knob you size the system with**: the input-log retention floor, the
recovery work after a crash, and the cadence at which source credit is minted. An operator
that never checkpoints stops truncating its input log and eventually parks its own source.
A checkpoint cannot be declared without the tier, because `ack` sits under
`effectively_once` in the tree, and the controller's dataflow-stop watchdog skips a
checkpoint class — silence between checkpoints is its steady state.

## Steps operators: one record, released at the stage commit

A **durable structured** steps operator (tasklets declared, the default
`durable: local`) never lets a tasklet's plugin output escape before that tasklet is
durable. The commit path, crash-resume and journal layout are in
[`steps_operator.md`](steps_operator.md); this is the flush contract.

- **One record per invocation.** A steps operator Cell writes the `cell_effect_record` a tier flow operator
  Cell writes: its state changes and its plugin outputs (channel, key, the author's metadata,
  payload) in one record under one fsync, with no consumed offset (the iteration's
  `iter_committed` carries it). The runner sends the record as the item settles, before the
  stage's barrier, never on the flush linger. A steps class does not declare the tier; the
  record is what its durability means.
- **Records, then the marker, then the ack.** The barrier's `TaskCommit` names, per Cell, the
  range of records it wrote in the stage and the plugin channels they wrote on, and carries no
  output. The controller appends the `task_committed` entry only after every named record is
  durable on its Cell's writer, so a durable marker always implies durable records. A record
  that is refused or not durable within `FLEZHA_STEPS_ACK_TIMEOUT_MS` fails the barrier. A
  barrier re-sent under the same reference id is answered from the existing entry, or joins
  the wait of the copy still waiting.
- **One outbox.** The Cell writer's outbox serves both runs: same `{:cursor, seq}` key,
  dedup-file cursor, confirmation tally, resettle and restart re-drive. A flow Cell's entry is
  committed by its own fsync; a steps operator Cell's entry is **held** until a durable stage marker
  names its record, and the commit worker releases the named range after the ack. Compute is
  not coupled to sink health: the next tasklet runs while the flush lands.
- **A failed stage leaks nothing.** A held entry that no marker will name (a stage interrupted
  by a crash and re-run, a failed stage, an iteration a rewind unwound) is dropped without
  reaching the chain: by the Cell's next committed range, which starts above it, or at the
  instance's next bind, which re-derives the committed ranges from the journal. An entry
  already handed to the chain is left to its confirmation.
- **Crash before flush re-drives.** On restart the writer re-queues every record above its flush
  cursor, reads the committed ranges from the journal and releases what a live entry names;
  the bind starts the writer of every persisted Cell, so a Cell the resumed run never writes
  again still re-drives what it owes. The journal's cut keeps any stage entry naming a record
  above its Cell's flush cursor.
- **Branches resume independently.** The restore carries each in-flight commit's handoff under
  its own `task_idx`: a respawn re-runs exactly the uncommitted subtrees, and a committed branch
  neither re-runs nor re-emits. A rollback runs in reverse topological order.
- **Keys.** Each flushed frame carries the stream's identity: `flz.cell`,
  `flz.seq = <record seq>.<emit ordinal>`, `flz.epoch` (the Cell's realign epoch), `channel`,
  `flz.key` when the channel is keyed, and the author's metadata. `emit_keyed` and emit with
  metadata work in a steps tasklet in every SDK.
- **The epoch rule.** A crash re-drive keeps every stamp, so a dedup sink absorbs the repeat. A
  deliberate [rewind](steps_operator.md#rewind-taking-a-loop-back-to-a-committed-iteration)
  leaves `flz.epoch` alone: a re-run's output is a newer record of the same Cell, so its
  `flz.seq` sorts above the unwound run's and an `(epoch, seq)`-guarded sink overwrites.
- **Compensation rides the same path.** A `compensate` hook's output is a newer record of the
  compensating Cell, named by the `compensated` entry and released once that entry is durable;
  its seq is above the forward output's, so an undo never collides with it.
- **Kept in memory (`durable: false`).** Such a class writes no record and no journal; its outputs go
  straight to the wire with channel, key and metadata, as a tier-off flow operator's do.

**Guarantee.** Never lost, never leaked before commit, and **at-least-once with stable keys** on
the re-drive window (between a sink's confirmation and the flush cursor's write); a
dedup-capable sink absorbs that duplicate exactly. Inspect what an instance's stages name with
`flz state journal <operator/instance>` (each stage's `ran:` lines name the Cell's records and
channels) and what a Cell still owes or holds for a stage with
`flz state outbox <operator/tasklet/key>` (`outbox_lag`, `held_for_stage`).

## The input floor: a processed prefix

An instance's floor decides what retention may release from its input log and where a bind's
re-forward starts. It is not what the controller forwarded, and not any one Cell's progress.

- **Every persistent Cell records the offset it consumed through**, in the record of the state
  that input produced: the effect record under the tier, and outside it the change records a
  drain writes, folded into one record that carries the highest offset. A restore, a hydration
  and the controller's boot scan read a Cell's offset from its last record.
- **Every Cell keeps a floor.** A restore seeds it from the Cell's records; it rises as the Cell
  takes each input the controller forwarded. An input at or below it is dropped unrun, so a
  tail re-forwarded twice is taken once.
- **The runner reports a processed prefix.** After each drain pass it sends, for an instance
  that took new input, a `processed_prefix` marker naming `P`: every input at or below `P` has
  been processed by every Cell it reached, downstream Cells included, and every record it
  produced is on the data stream ahead of the marker. An input still queued, in flight, retrying
  or unflushed holds `P` below it; an idle Cell holds nothing. The marker never waits in the
  reconnect ring, and a record the ring evicted is never covered.
- **The controller confirms `P` durable.** It records where each of the instance's Cell writers
  stood on the stream when the marker arrived, and confirms `P` once every one of them has been
  fsynced past that point. A confirmed `P` is the floor. Before one is confirmed, and after a
  controller restart, the floor falls back to the minimum durable consumed offset over the
  instance's reporters, and the re-forward from there is safe because every Cell drops what it
  already consumed.

A plain sleeping stream therefore folds nothing twice across a controller restart while it is
awake, and a stream keyed by one quiet key does not pin the log for everyone else.

## Steps operators: the same durable input log

The input half of the same story. A durable steps instance's input goes through the one
durable input log the tier uses (above), keyed by the instance id: appended **before** it is
forwarded, answered once synced, stamped `flz.offset`, and an append that fails is not
forwarded. A crash after the append re-delivers on rehydrate (at-least-once *into* the
operator, exactly-once *completion* via the consumed offset). A repeat of a `flz.msg_id` the
log still retains is absorbed, so an at-least-once sender cannot produce a duplicate turn,
while a message without an id is always appended.

What differs is the reporter. A steps operator's Cells report no consumed offset; its journal does,
under its own reserved identity: each durable iteration commit reports the offset it consumed
into the log's watermark, and a message dead-lettered unread is journaled as released
(`input_released`) before the instance sleeps. Every bind seeds that row from the highest offset
the journal released, and the driver drops a re-forwarded message at or below it. The watermark
is the instance's floor: retention reclaims below it, the bind re-forwards the retained records
above it, and a rewind never lowers it, so a deferred truncation costs disk, never a re-delivery.

**Source credit applies too.** A durable steps operator's appends debit the same per-instance
`:source_credit_window_bytes` window, drained as iterations commit, so a sender that has the
window's worth of input (16 MiB by default) owed by one instance parks until the instance
commits an iteration, instead of growing the log without bound.

**Guarantee.** Input sent to a suspended, crashed or migrating durable instance is **never
lost**. Inspect with `flz state inbox <operator/instance>`, which renders the log of either
kind. An opted-out class keeps its waiting messages in memory only. Mechanics:
[`steps_operator.md`](steps_operator.md) "Durable loop and suspension".

## Sleeping instances

Sleeping when idle is a setting of every class that keeps its work: `sleep_after` under
`durable: local` or `remote`, by default never for a flow class and six hours for a steps class,
`false` for never; a class kept in memory
has nothing to sleep on. An instance whose Cells hold nothing queued, in flight, retrying,
unflushed or timed, idle for its class's `sleep_after`, ships `IterPark` and is
evicted; the controller writes its park record first. A steps operator sleeps only at a committed
boundary, a flow operator at iteration `0`.

A flow operator that sleeps keeps durable input: every message is appended to its input log before it
is forwarded, one append per message. Its floor is the tier's: the offsets its Cells' records
carry and the processed prefix the controller confirms
([above](#the-input-floor-a-processed-prefix)). A forward alone releases nothing, so an input the
runner took and lost before recording it is re-forwarded. A controller that boots while the
instance sleeps reads the floor from its Cells' records. The bind that wakes it restores every
persisted Cell, awaited, before it re-forwards the tail. A flow operator that does not
declare the setting pays nothing.

**Guarantee.** A message sent to a sleeping instance of either kind wakes it and is delivered
once, across a controller restart while it sleeps. A sleeping steps operator stopped by a user is
woken to end `cancelled`; a sleeping flow operator's stop removes it, as a flow operator's stop always does.

## Operator to operator: the cross-hop delivery guarantee

`Flezha.Plugin.Provider.OperatorForward` delivers one operator's output into another's input
stream with **no** new identity scheme, dedup store, ledger or wire format. Three existing parts
assemble the guarantee: the source's outbox stamps `flz.seq` / `flz.key` / `flz.epoch` and makes
the record durable; the plugin's worker holds the record and retries until the target confirms;
the target's ingest absorbs a repeat of a `flz.msg_id` its durable input log still retains and
is offset-addressed in that log.

**The guarantee is derived, never configured** — there is no delivery-mode option, because a
source with no outbox has nothing to re-send from:

| Source operator | Across the hop |
| --- | --- |
| effectively-once flow operator | effectively-once |
| durable steps operator | effectively-once |
| plain flow operator (no outbox) | at-least-once, best effort — no de-duplication |

**A partitioned target adds a routing precondition.** Forwarding into a partition group routes
by `hash(flz.key)` to the owning member; a message *without* `flz.key` (an emit on an unkeyed
channel, from either kind) is **parked to the source's DLQ** with
`partitioned target requires keyed emit` rather than scattered. A park is a durable outcome the
cursor may pass.

**In-flight items survive a controller restart.** The Cell writer's outbox gates its cursor on
the sink, for a durable steps operator and an effectively-once flow operator alike: it keeps each dispatched
entry in its confirmation tally and re-drives everything above the persisted
`outbox_flushed_seq` on restore. A plain flow operator keeps retry-until-confirmed within the worker's lifetime only. Verdicts,
refusals and telemetry: [`../plugins/operator_forward.md`](../plugins/operator_forward.md).

## Per-Cell timers: folded state, not a scheduler

A timer registration, cancellation and firing are each a durable record in the Cell's own
change log, so the pending set is rebuilt by the ordinary fold on restore — no side-file, no
crash marker, nothing new to make crash-consistent.

| Crash point | Outcome |
| --- | --- |
| before the registration is durable | the timer was never owed |
| after the registration, before firing | pending; fires when its condition is next met |
| after firing | the removal folded in; it does not re-fire |
| mid-firing, before the record is durable (tier) | neither effects nor removal landed — the whole firing repeats |

Under the tier a firing is one effect record (the changes `on_timer` applied, its emits, the
removal) with no `consumed_offset`, so the input-log watermark does not advance on it. Without
the tier a firing is at-least-once, like input; a firing that exhausts its retries parks to the
DLQ, and its removal is durable either way, so a poison timer parks once. On the wire the ops are
a `format_id = 3` record under `cell_state_change` (a postcard `Vec<TimerOp>`) or
`EffectRecordBody.timers` under the tier. See [`../sdk/cell_timers.md`](../sdk/cell_timers.md).

## Dead-letter queue: poison payloads park, never drop

A permanently-failing (poison) payload in a Flow operator is never silently discarded. A
failing invocation — returned error or panic/exception, same policy in both SDKs — is retried
up to the channel's `retry_max` (default 3 attempts, exponential backoff with jitter); authors
mark an error non-retryable with `TaskletError::non_retryable` /
`TaskletException.NonRetryable` to park immediately. An unroutable forward into a partition
group, the SDK's out-of-range key-group guard, and a frame naming a channel that is not one of the
Operator's entry channels park with no invocation failure at all; the last of those parks under
the name the frame asked for, with an error naming the entry channels the Operator does declare,
and after one attempt, because the same declared set answers every re-attempt. After
exhaustion the payload is **durably parked** to the controller's DLQ store
(`app_data/dlq/<operator_class>/<tasklet_id>/`, JSONL index + payload blobs) and only then
does the Cell advance:

- **Tier off:** the SDK ships a `park_payload` frame and holds the Cell until the controller
  fsyncs the entry and acks (`ParkPayloadAck`), re-sending until acked.
- **Tier on:** the park rides the invocation's atomic effect record (`{no state change, dlq
  entry, consumed offset}`), so a crash can never advance the offset while losing the park.
  Parking advances the consumed offset past the poison, so the min-watermark resumes and a
  credit-parked source resumes granting.

Operations surface: `flz dlq list | show | replay | drop`. All four are `ops` verbs and take
`--token` (or `FLEZHA_TOKEN`) — a parked payload is the failed business record held verbatim, so
reading one is as privileged as discarding it. A verb naming an operator class is authorized in
that class's namespace; one that names none reads across every namespace and is authorized in
`_system`. Replay is explicit-only and re-enters the
ordinary input path (fresh output identity — it journals as a new input); a failing replay
re-parks with `generation` incremented; `drop` is audited. Retention bounds the store by age
AND size (30 d / 4 GiB, more restrictive wins); a retention deletion is genuine loss and is
loud: `[:flezha, :dlq, :retention_dropped]`, exported as `dlq_dropped_by_retention_total`.
Every new entry also executes `[:flezha, :dlq, :dead_lettered]`, which names the entry
rather than counting it and reaches only controller plugins
([`hook_webhook.md`](../plugins/hook_webhook.md)), never an exporter.

**Steps operators.** A steps operator Cell runs the same first rung: retried in place,
non-retryable errors skipping the ladder (see
[Cell retry](steps_operator.md#cell-retry-the-rung-before-the-abort)). Its terminal escape is
usually not a park: the tasklet fails, AbortAll fires and saga compensation unwinds. Two cases
park. A message the entry channel cannot read parks under the channel. A failed turn of a
`per_input` loop declaring keep-progress parks under the `_turn` slot: the controller reads the
turn's bytes from the instance's input log and appends the entry (keyed `_turn@<offset>`, so a
re-run that fails again lands on it) before it journals the iteration as `dead_lettered`; the
turn's committed tasklets are then undone and the session takes its next message. A replay sends
the bytes back as a new message; a replay to an ended session is refused. See
[A failed turn of a keep-progress session](steps_operator.md#a-failed-turn-of-a-keep-progress-session).

## Late-input correction: fix the past, and the sinks fix themselves

A trade arrives five seconds late, after the candle it belongs to has closed and published.
Without correction that candle is permanently wrong, and so is every indicator downstream.

Correction is **recovery with a re-ordered input sequence**: rewind the Cell to a baseline
behind the late event, restore the runner there, and re-forward the retained input log ordered
by event time rather than arrival. Outputs republish under a bumped epoch, so a keyed changelog
sink overwrites the stale rows by natural key. **There is nothing to splice** — inputs are
journaled before they are forwarded, so a late input is already in the log at its arrival
offset; only the *order* was ever wrong.

### Declaring it

```rust
fn settings() -> OperatorSettings {
    OperatorSettings {
        run: Run::Flow {
            exactly: Exactly::EffectivelyOnce {
                ack: AckGranularity::PerInput,
                correction_ms: Some(600_000),               // repair up to 10 minutes of past
                beyond_window: BeyondWindowPolicy::Drop,    // optional; default ProcessAsArrival
            },
        },
        ..OperatorSettings::default()
    }
}
```

Three prerequisites are authoring facts the engine cannot repair, and all are hard refusals at
SDK build time:

| Prerequisite | Why |
| --- | --- |
| `exactly: effectively_once` | append-before-forward is what puts the late input in the log |
| an event-time clock | nothing else yields a time to order a replayed slice by |
| every plugin channel `.keyed()` | a bumped epoch overwrites *by key*; an append channel would duplicate (already implied by the tier) |

**Retention is derived from the declaration.** `InputLog.hold_window_ms/1` resolves to
`max(:input_log_hold_window_ms, declared window)`, so truncation never removes an input the
correction would replay; `PersistenceResolver.compact_after_age_ms/1` is never shorter than the
window, so compaction always leaves a baseline to land on (`0`, age-triggered compaction off,
already satisfies any window and is preserved). A floor that had to be raised is logged once and
emitted as `[:flezha, :correction, :retention_derived]` at bind.

### What happens

1. The SDK extracts the event time and compares it to the Cell's event-time high-water.
   Behind it but inside the window → the input is **not dispatched** (it is already durable;
   folding it now would be the mistake the correction undoes) and a `correction_request` goes
   to the controller.
2. Requests coalesce per Cell into the one replay that reaches furthest back.
3. The controller picks the baseline: the last seq whose history entirely precedes the target,
   scanning *forward* to the first record at or after it (a backward scan would stop at an
   out-of-order straggler and correct nothing).
4. `WalWriter.rewind_to/3` keeps the records at or below the baseline and drops only the
   snapshots above it — not the realign primitive, which wipes the WAL. The `persist_dedup` at
   its tail is the commit point: the epoch bump is durable before any output re-emits under it.
5. `RestoreInitState` re-seats the runner on the baseline (blocking, so the control-channel
   restore cannot race the data-channel replay). The Cell clears its mailbox — every queued
   input is already in the slice.
6. The input-log range above the baseline's consumed offset is re-forwarded marked
   `flz.replay`; the Cell holds the slice, orders it by event time (ties by ascending offset),
   then dispatches.
7. Outputs flush through the normal outbox under the new epoch; Kafka key-upserts and the
   Postgres `(epoch, seq)` guard admit the higher epoch and overwrite.

**A crash mid-correction needs no marker.** Before the rewind commits nothing durable changed:
the late input is still unconsumed, the ordinary resume re-forwards it and the correction runs
again. After it commits the on-disk state *is* the baseline, so the ordinary resume replay
**is** the correction. The epoch is passed to `rewind_to` explicitly so a re-run re-asserts it
rather than bumping twice.

**Beyond the window** an input follows the declared policy, decided in the SDK:
`ProcessAsArrival` (the default — declaring a window never silently starts discarding data) or
`Drop`. Neither advances the high-water, so a very old event cannot drag the window backward.

**Manual replay.** `flz operator replay <operator/tasklet/key> --from <seq|timestamp>`
runs the same transaction with the baseline chosen instead of derived: deploy a fix, heal the
recent past. A target older than retention is refused with `outside_retention`, naming the
earliest available point.

**Limits.** A correction overwrites the keys it re-emits; it does not retract keys it no longer
emits (impossible for event-time-bucketed keys, possible for others — treat the sink as
append-with-overwrite). A downstream operator sees corrected outputs as ordinary keyed updates
and does not itself rewind. The window is bounded by retention by construction.

## Remote durability tier: surviving the loss of a controller's disk

Everything above is local. A controller that dies **with its disk** takes its operators' state
and the registry record of their existence with it; `WalTransfer` is a one-shot pull at
migration time, not replication. With `FLEZHA_DURABILITY_BACKEND` unset
a production controller warns on every boot and runs local-only, and `FLEZHA_DURABILITY_BACKEND=none` is byte-identical
to a build without the tier (`durability_posture: local_only`).

When enabled, immutable artifacts — snapshots, WAL record bundles, task-journal suffixes, the
durable input log, the registry cspec, parked DLQ records, the authz and schedule stores —
ship to a Postgres, S3-compatible or file backend at boundaries that already exist (checkpoint
close, compaction, steps operator commit barriers, a park or a drop) plus one backstop timer
(`ship_interval_ms`, 30 s). The manifest is written last, so a torn upload is invisible to
recovery, which hydrates the artifacts into the local layout and runs the ordinary restore path.
A controller that kept a copy of an operator's state when the operator moved away compares it,
when the operator comes back, with the copy on the node it moves from (`WalTransfer`), or else the
cut another controller shipped, by the WAL-owner epoch and then the highest seq, a tie going to the
incoming copy. A newer copy replaces the local one whole: the local copy is set aside, the newer
one is pulled or hydrated, and a reference only the local copy holds is kept, never deleted. A
newer copy that does not land whole, or a crash before it has, leaves the local copy as it was.

What a lost disk costs depends on the class's `durable` level and on its commit point: an
input under `ack: per_input` or `at_least_once`, a checkpoint under `ack: checkpoint`, a stage of
a steps instance.

| `durable` | Flow, an input | Flow, a checkpoint | Steps, a stage |
| --- | --- | --- | --- |
| `false` (memory) | everything, on any crash | not declarable: the tier needs a disk | everything: the run starts over |
| `local` | the work since the last ship, at most the RPO target | the work since the last ship, at most the RPO target | the work since the last ship, at most the RPO target |
| `remote` | nothing acknowledged | nothing acknowledged; the open interval replays | nothing acknowledged |

At `local`, shipping is asynchronous and never touches an acknowledgement: a failing backend
delays no append, fsync or watermark advance. What degrades is the guarantee, loudly: lag past
`rpo_target_ms` (120 s) emits the alarm-grade `[:flezha, :state, :rpo_exceeded]`, surfaced by
`flz controller status` and `/healthz` (`degraded, reason: durability_rpo_exceeded`).

At `remote`, every commit point waits for its ship before it is acknowledged. A steps stage's
`TaskCommitResponse` is held until the manifest naming the barrier is written. A flow Cell's
writer holds what each fsync made durable, the credit it releases to the runner (which also
confirms the instance's processed prefix) and the dispatch of its effect records (their
outputs, their parks, their consumed offsets), until a ship of the Cell and its input log
lands; one ship is in flight per Cell, and the next covers every input synced meanwhile. A
record refused unwritten (a duplicate, a gap, a shed) drains behind the held ones. A writer
that stops, or a lineage cut by a realign or a rewind, frees the runner's credit for what it
held without confirming the prefix: the markers that covered it are dropped unconfirmed, and
later markers wait only on what the writer still owes. One window is
still open: after a writer resets and restarts under the same stream, until its boot ship lands,
a new marker can confirm an input whose record is on local disk but not yet shipped. Under
`ack: checkpoint` only the interval's close waits, so an interval ships once: prefer it for a
`remote` flow class, which otherwise pays a remote write per synced batch. A controller
restored from the manifest resumes each Cell after the last input it acknowledged and never
delivers that input again; inputs of an open interval replay. A backend outage stalls only
`remote` classes: their acknowledgements wait and the RPO alarm fires, while `local` classes on
the same controller go on. A refused ship keeps the reference's RPO clock running while its
commit point waits, also when another controller has taken the lineage. `flz controller status` prints each running class's level as
`durability_levels`, and each remote commit's wait is `[:flezha, :state, :remote_commit]`.

## How to avoid loss: user playbook

- **Deploy and upgrade runners with a graceful stop, never a kill.** Send SIGTERM (or a
  `flz`-driven `disconnect`) and set the orchestrator grace period
  (`terminationGracePeriodSeconds` / `stop_grace_period`) **longer than the drain** (ack budget
  2 s per operator in parallel, serializer cap 10 s). Roll one node at a time so a peer is
  always live.
- **Restart the controller correctly.** Multi-node: bring the new instance up *before* stopping
  the old one. Sole instance: plain restart; the first reconcile round replays the operator
  list from the durable registry once `FLEZHA_PROCESS_HUB_CLUSTER_SETTLE_MS` (2 000) has passed
  with no peer to wait for. `FLEZHA_PROCESS_HUB_RECONCILE_GRACE_MS` (45 000) only caps the wait
  on a connected peer that never answers.
- **Make sinks idempotent where exactly-once matters.** Declare the channel `.keyed()` so
  re-emits carry identical `flz.key` / `flz.seq` stamps to upsert on, or opt the operator into
  `exactly: effectively_once`; the Kafka and Postgres plugins conform out of the box.
- **Never treat the on-disk registry as the live source of truth** in code or ops; it is
  what the orphan reconcile replays on a cold start.
- **Steps operators are durable and at-least-once by default.** Author each tasklet and
  compensation idempotently, or opt the class out of persistence for a fire-and-forget job that
  must re-run from a clean slate.

## Common pitfalls

- `docker kill`, SIGKILL, or a `terminationGracePeriodSeconds` shorter than the drain on a
  runner cuts the drain off → best-effort loss. Use a graceful stop and adequate grace.
- **The runner binary must receive SIGTERM directly.** Run it as PID 1 (exec-form
  `CMD`/entrypoint, or `setpriv`/`gosu` dropping privileges by `exec`). A wrapper that forks the
  binary and force-kills it on SIGTERM — notably `su` ("Session terminated, killing shell") —
  silently degrades a graceful stop to a hard kill.
- Expecting the operator list back instantly after a sole-controller restart: `/healthz`
  reads `recovering` until the first reconcile round, about two seconds on a controller with
  no peers, and then until the replayed operators are bound to runners again. An operator
  whose runner has not reconnected is held unbound and bound when it does; that is the design,
  not a fault.
- Assuming output is exactly-once: it is at-least-once by default. Dedup downstream or opt into
  the `exactly: effectively_once` tier.

## See also

- [`overview.md`](overview.md) — the map of every resilience mechanism and its owning doc
- [`steps_operator.md`](steps_operator.md) — commit path, crash-resume, suspension, retention
- [`../architecture/kernel.md`](../architecture/kernel.md) — the write path, the effect record, the epoch rule, flow control
- [`../internals/state_persistence.md`](../internals/state_persistence.md) — WAL layout, snapshots, retention knobs
