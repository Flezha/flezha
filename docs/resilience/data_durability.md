# Durability mechanism

How the guarantees of [Durability](../topics/core/durability.md) are built: the
guarantee class of a frame, the parts of the effectively-once tier, and the input
floor. This page is for contributors; what survives what, and how to use it, is on the
topic page. The WAL substrate is
[`../internals/state_persistence.md`](../internals/state_persistence.md).

## The guarantee class of a frame

Between the two sits the wire itself, and what a runner does when it refuses a frame is decided
by the frame's declared **guarantee class**, never at the write site. A change record, a
snapshot, an eviction's delete, a saga marker, a park payload and the shutdown state ship are
`durable`: held in a bounded drop-oldest ring, the shed counted, replayed in write order on the
next bound stream. A plugin or steps relay emit is `output`: held already framed, so a keyed
output's channel and identity stamps survive the replay. A control frame is refused to its
caller and never held, because the reconnect re-issues the request it belonged to, and the data
heartbeat is held by nothing at all — it reports a moment that has passed. The seam is
`flz_core::state_sink::write`, shared by every SDK; a payload id no class table
names is refused at the call rather than given a default. See
[`../sdk/protocol_spec.md`](../sdk/protocol_spec.md) §7.

## Effectively-once tier (opt-in, `exactly: effectively_once` in the class's run)

The built-in composition *at-least-once delivery + deterministic key + idempotent output plugin =
effectively-once*. Every plugin-bound channel must be `.keyed()` (refused at build time
otherwise), and the tier is built by the shared runner core, so Rust and Java earn the same
guarantee.

- **Durable input log.** `Flezha.Relay.Ingest.ingest/4` appends every inbound payload to a
  per-instance `input.log` before forwarding, stamped `flz.offset`; the log answers once the
  record is synced. It is the one durable input log, shared with a durable steps relay (below). The
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
  twice at a conforming output plugin**.
- **The cursor advances on confirmation, not on dispatch.** Each flushed output carries an
  opaque `flz.ack` token naming the writer that owns its cursor (`Flezha.State.Outbox`). A
  delivery-guaranteed output plugin takes a *hold* as it accepts the record and *confirms* when its write
  is durable — Kafka produce acked, Postgres transaction committed, forwarded ingest confirmed.
  A record no output plugin held completes as soon as the chain has run it, so a chain without a
  guaranteed output plugin behaves as before. A batch whose retry budget is exhausted **parks** through
  the dead-letter path with its identity attached, and only then may the cursor pass it. A
  delivery whose settlement is lost is re-dispatched by the resettle tick after
  `:outbox_resettle_ms` (60 s); at most `:outbox_inflight_max` (1 000) entries are in flight per
  Cell. Unconfirmed output is visible as `outbox_unconfirmed_ms` in the Cell's stall status
  and reaches the convergence loop as the `outbox_flat` verdict.
- **Reports are batched, never the facts.** One dispatch pass hands its window to the chain in
  one cast and settles it with one report; an output plugin confirms a completed batch with one report
  naming every entry (a park stays per record). A chain that crashes mid-batch settles nothing,
  and the resettle re-drives every pending entry under identical identity.
- **Conforming output plugins out of the box.** The **Postgres** output plugin (`relay_postgres`,
  `Flezha.Plugin.Provider.Postgres`) upserts every record of a flush batch into
  `flezha_records` (keyed by `flz.key`, else the durable identity) and advances the per-Cell
  `flezha_sink_progress` row in ONE transaction; every upsert is guarded by `(epoch, seq)`
  monotonicity, so re-drives and stale epochs are no-ops, and the commit is what confirms the
  batch. An unreachable database delays the flush (retry with backoff; callers park past
  `queue_max`, 10 000) — delayed, never dropped. The schema is user-owned by default
  (`auto_create: false` fails `health/1` naming the missing tables); a commit stall past
  `unhealthy_after_ms` (30 000) escalates on the plugin-health → convergence path. Unstamped
  records become plain inserts. The **Kafka** producer (`kafka_producer`, from the Kafka plugin package) uses
  `flz.key` as the message key and carries the durable identity in the `flezha.dedup_key`
  header; see [`../plugins/kafka_producer.md`](../plugins/kafka_producer.md).
- **The websocket output plugin is not a conforming output plugin, and does not claim to be.** A plugin bound as
  an output (`relay_websocket`, `Flezha.Plugin.Provider.Websocket`) ships each dispatched
  payload as a frame on the connection it already holds, and answers the hold by its declared
  `sink_ack_mode`: `"none"` (default) confirms when the frame reaches the established socket,
  which is **at-most-once** past that point; `"required"` confirms only when an inbound frame
  echoes the correlation id, which is **at-least-once**. Neither is effectively-once and no
  mode that claims to be is offered — a websocket exposes no transaction to enlist and no key
  to overwrite, so the destination cannot participate. What the output plugin can do is carry
  `flz.cell` and `flz.seq` in the frame template, and the consumer that tracks the last
  sequence it has applied per cell is the only place the dedup can work. Frames are queued
  while the socket is down (`sink_queue_max`, 10 000; callers park at the bound) and a frame
  whose retry budget is spent parks in the dead-letter store. An enable on a tier relay
  warns rather than refusing: the relay's internal guarantee is unchanged and only its
  websocket hop is best-effort. See
  [`../plugins/websocket_subscription.md`](../plugins/websocket_subscription.md).
- **Source back-pressure.** Ingestion debits a per-relay source credit window
  (`:source_credit_window_bytes`, default = `:wal_high_watermark_bytes`); the min-watermark
  advance mints it back. At zero credit the source adapter's ingest call parks (a websocket
  stops reading its socket), bounding input-log growth against a stalled Cell. An output plugin outage
  longer than `window / ingest rate` delays ingestion; it never wedges it.
- **Cost-free default.** Relays without the flag keep every path byte-identical. Under the
  tier a slow or unreachable output plugin backs the outbox up on disk, never in memory.
- **Steps relays** do not declare the tier (it is a member of the flow run only): a durable
  steps relay's Cells write the same effect record as their ordinary durability, released at the
  stage commit below.

Wire shapes: [`../sdk/protocol_spec.md`](../sdk/protocol_spec.md) "Output identity stamps" and
"cell_effect_record".

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
