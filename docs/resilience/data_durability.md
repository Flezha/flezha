# Data and State Durability

The reference for **where Flezha can lose operative state or output, where it cannot, and how
to operate so a planned action never loses data**, for operators and operative authors. It
covers the scenario matrix, the hard-kill gap, the delivery semantics (at-least-once by
default, the effectively-once tier), the durable input log and the workflow outbox, the cross-hop
guarantee, timers, the dead-letter queue, late-input correction, the remote durability tier
and the operator playbook. Operative *placement* recovery is
[`../internals/process_hub.md`](../internals/process_hub.md); the WAL substrate itself is
[`../internals/state_persistence.md`](../internals/state_persistence.md).

## The two things that can be lost

1. **Operative state** — the accumulated in-memory state of an operative. The authoritative
   live copy is on the operative's **one bound runner**; it is backed up **asynchronously** to a
   per-Cell **WAL on the controller** (the runner streams `cell_state_change` frames, the
   controller batches the fsync, ~5 ms).
2. **Output** — what an operative emits. Output is routed **through the controller** (the
   `:on_output` plugin chain → Kafka / Postgres / HTTP / another operative). It is
   **at-least-once** by default; an operative that declares `effectively_once()` gets the
   **effectively-once tier** below.

The core invariant: **on a graceful stop no state is lost; on a hard kill a small best-effort
window can lose the most recent un-persisted state.**

Between the two sits the wire itself, and what a runner does when it refuses a frame is decided
by the frame's declared **guarantee class**, never at the write site. A change record, a
snapshot, an eviction's delete, a saga marker, a park payload and the shutdown state ship are
`durable`: held in a bounded drop-oldest ring, the shed counted, replayed in write order on the
next bound stream. A plugin or workflow emit is `output`: held already framed, so a keyed
output's channel and identity stamps survive the replay. A control frame is refused to its
caller and never held, because the reconnect re-issues the request it belonged to, and the data
heartbeat is held by nothing at all — it reports a moment that has passed. The seam is
`flz_core::state_sink::write` for every runtime built on the shared core and
`Flezha.SDK.Hold.write/3` for the Elixir SDK, which is its own core; a payload id no class table
names is refused at the call rather than given a default. See
[`../sdk/protocol_spec.md`](../sdk/protocol_spec.md) §7.

A third durable artifact is the operative's **plugin manifest**: it lives in the registry cspec
and is written cluster-wide (`propagate: true`), so an operative restarted from any node
re-attaches its full plugin set and the convergence loop re-adds anything missing.

## Scenario matrix

| Scenario | Operative state | Output | Why / how |
| --- | --- | --- | --- |
| **Graceful runner stop** (SIGTERM / rolling deploy / `disconnect()` / `Drop`) | **No loss** | At-least-once | The SDK (`flz_core/src/shutdown.rs`) serializes each operative's final state, ships it, and waits for `RunnerShutdownAck` before exiting. |
| **Single runner SIGKILL** under load | **Brief best-effort loss** | At-least-once | No drain; the operative rebinds and restores from the controller WAL, which may lag the last few changes. |
| **All runners SIGKILL'd at once** under load | **Brief best-effort loss** | At-least-once | Same WAL-restore path with no live copy anywhere, so the lag is reliably exposed. |
| **Runner OOM / container crash / node death** | **Brief best-effort loss** | At-least-once | Same as SIGKILL — no drain runs. |
| **Controller graceful restart / rolling deploy with a live peer** | **No loss** | At-least-once | The new instance syncs the registry from the surviving peer; runners stay connected. |
| **Controller plain restart, sole instance** | **No loss** | At-least-once | The first reconcile round replays the operative list from the durable registry; `/healthz` reports `recovering` until rebound. No operator step. |
| **Whole cluster down → cold start** | Recovered from the durable registry | n/a | No peer survives, so the reconcile's difference is the whole list and disk is the only record. |
| **Operative migrated to a controller with no local WAL** (node loss, redistribution, lost hostPath), runner live | **No loss** | At-least-once | The WAL is node-local (with the remote tier, hydrated from the manifest). Absent both, the first `cell_state_change` into an empty WAL triggers a rate-limited `CompactCell` **pull**: the runner ships a full snapshot that becomes the baseline. The controller never pushes an empty `RestoreInitState`. See [`../internals/state_persistence.md`](../internals/state_persistence.md) "Baseline recovery: push vs pull". |
| **Node rejoins a live cluster** | Synced from peers; local durable copy kept | n/a | The reconcile's difference against live peers is empty, so the node starts nothing. The on-disk copy is never discarded to force that. |
| **Kafka broker down, runners alive** | No loss | Buffered, resumes | The producer reconnects; the queue drains when the broker returns. |
| **Sustained overload** (changes outpace fsync) | **No loss** | At-least-once | Credit-grant flow control: the sender's byte window (`:wal_high_watermark_bytes`, 16 MiB) is minted back only on WAL fsync, so an exhausted window *parks* the SDK's flush path — park, never drop. A shed on a credit session emits `[:flezha, :state, :credit_anomaly]`. |
| **Workflow operative runner SIGKILL** (durable, the default) | **Brief best-effort loss** (async WAL lag) | At-least-once | Respawns on a new runner, restores committed Cell state behind the restore-before-run gate, and resumes at the **first un-journaled tasklet**: `task.journal` records each completed tasklet and its handoff, so committed tasks are skipped, not redone. Inspect with `flz state journal`. |
| **Looping workflow operative runner SIGKILL** (durable) | **Brief best-effort loss** | At-least-once | Resumes at the iteration after the last `iter_committed`; the in-flight one re-runs from its own last committed task. Unconsumed input is still in the instance's durable input log. |
| **Input sent to a suspended / crashed / migrating workflow instance** (durable) | **No loss** | n/a | Appended to the instance's durable input log (`input.log`) before forwarding, delivered on rehydrate; a suspended instance is woken by the append. |
| **Controller redeploy while a workflow instance is suspended** | **No loss** | n/a | Suspension is durable state (`parked` journal entry + input log + snapshots): the instance is rediscovered from disk, stays evicted, and rehydrates on the next input or its wake deadline. |
| **Workflow operative runner SIGKILL** (persistence opted out) | **Full: re-runs from the top** | At-least-once | No WAL; the operative re-runs from the top on its next message and the messages waiting in its memory are lost. Idempotent by author contract, or don't opt out. |
| **Workflow operative SIGKILL mid-compensation** (durable) | Compensation resumes | n/a | Compensation-intent / -done markers ride the restore; the respawn finishes the outstanding undo hooks and skips the done ones. |
| **Poison payload** (fails every retry) | **No loss** | Cell continues | Retry-then-park: after `retry_max` attempts the payload is parked durably to the DLQ *before* the Cell advances. Only retention (30 d / 4 GiB, loud) ever discards it. |

## Why output can briefly get ahead of state (the hard-kill gap)

The runner streams **output frames** and **state-change frames** independently, and the
controller emits default-tier output before the matching state is fsynced. On a hard kill,
output can outlive the durable state (Kafka holds `total=135`, the WAL holds `128`) and the
restored operative comes back slightly stale. The default tier does **not** close this with
per-record acking; planned stops drain to zero loss, and an operative that needs the gap closed
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
the same way, so the operative is left unbound rather than binding on the records that happened to
precede the failure — the fate "a refused restore" already has. Recovery is an operator decision
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
ambient time, UUIDs or external reads in `run` / `apply_change` / `compensate`; use `ctx.now()`
and `ctx.stable_id()` — is documented on the `Tasklet` trait and checked by
`TaskletTestHarness::replay_emits_equal`.

Two limits of the default tier: `flz.seq` is not an exact append-only dedup handle (invocations
with no state change can collide on it), and there is no `flz.epoch` — the epoch belongs to the
controller's outbox lineage. A conforming sink reads a keyed default-tier record as epoch `0`
and orders by `flz.seq`; a *regressing* seq from a Cell that cold-started without a baseline
is refused rather than guessed. The epoch, bumped by a WAL realign or a rewind, resolves that
and exists only under the tier.

Nothing journals a default-tier input, which also decides the fate of input queued in a
Cell's mailbox when a restore lands: with no log to re-forward from, the queue is the only
copy, so the restore keeps it. Under the tier the restore discards it
(`discard_queued_input_on_restore`) because the input log re-forwards it.

**Nothing drops by default.** `DropNothing` is the default overflow policy for every channel
kind in both SDKs and the FFI host; an operative that prefers to shed declares
`.on_full(DropNewest)` or `.on_full(DropOldest)`. The Kafka producer's `:queue_overflow` defaults to
`:drop_nothing` for every record. Non-keyed channels are plain at-least-once.

*What `DropNothing` guarantees, per channel kind.* On an **inter-tasklet** channel the
producing Cell is withheld further work until the push lands — its push holdback holds the
value, its gates stay held, and the flusher holds its durable records with it, so the consumed
offset can never become durable ahead of an edge output derived from it. On an **entry**
channel the shared transport reader stalls instead, which carries the backpressure to the
controller. On a **plugin-bound** channel there is no SDK-side mailbox at all and the policy is
a compile error. Nothing is discarded in any of them.

Two windows this does **not** close, both named so nobody reads more into the guarantee:

- **The consumer's volatile mailbox.** Once a push lands, the value lives only there until the
  consumer's own record is durable, and a tier-class restore clears the mailbox
  (`discard_queued_input_on_restore`); the controller re-forwards to the *entry* channel, where
  the producer's own floor skips it. Closing it needs `OutputTarget::Edge` in the transactional
  outbox.
- **The controller's send buffer**, which still discards the newest frame when a runner stops
  reading (`flz_nif/src/driver/mod.rs`, `StreamState::offer`). The backpressure chain is complete inside the runner
  and breaks at its last hop.

### Effectively-once tier (opt-in, `effectively_once()` on the channel builder)

The built-in composition *at-least-once delivery + deterministic key + idempotent sink =
effectively-once*. Every plugin-bound channel must be `.keyed()` (refused at build time
otherwise), and the tier is built by the shared runner core, so Rust and Java earn the same
guarantee.

- **Durable input log.** `Flezha.Operative.Ingest.ingest/4` appends every inbound payload to a
  per-instance `input.log` before forwarding, stamped `flz.offset`; the log answers once the
  record is synced. It is the one durable input log, shared with a durable workflow (below). The
  host never waits on the log (`InputLog.append_async/5`): the input is forwarded when the log
  answers, and a zero-credit park lands on the source's call alone. A repeat of a `flz.msg_id`
  the log still retains is absorbed; a message without an id is always appended. Retention
  truncates only below the minimum durable consumed-offset across the instance's reporters (here
  its Cells); a frozen watermark past the size threshold emits
  `[:flezha, :state, :input_log_frozen_watermark]`.
- **Atomic effect record.** State changes, outputs, consumed offset, dead-letter entry and event
  time form ONE `cell_effect_record` under ONE fsync. No partial combination is ever durable
  and effect records are never coalesced away.
- **An inter-tasklet output's offset is never durable ahead of the output.** An edge value is
  the one output kind with **no** durable copy — `OutputTarget::Edge` is excluded from the
  transactional outbox — so the only thing that can re-produce it is the producer re-running its
  input. The invocation is therefore treated as unresolved until every edge push has been
  admitted to its consumer's mailbox: its changes, its plugin outputs and its offset are held out
  of the flush buffer until then, exactly as a failed-but-retryable attempt's changes are. Fold
  first and a crash inside the flusher's ~5 ms linger leaves the offset durable, the replay floor
  skipping that input, and the consumer never fed. A fan-out invocation holds its record until
  **every** consumer's push has landed; a replay after a partial fan-out re-emits to all of them
  and each consumer's own restored floor decides whether it is a duplicate — which is how
  at-least-once edge delivery composes into effectively-once without a second mechanism.
  *Not closed by this:* between the push landing and the consumer's own record becoming durable
  the value lives only in the consumer's volatile mailbox, which a tier-class restore discards
  (`discard_queued_input_on_restore`) and which the controller re-supplies only at the *entry*
  channel, where the producer's floor skips it. Closing that needs `OutputTarget::Edge` in the
  transactional outbox — a separate change.
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
- **Conforming sinks out of the box.** The **Postgres** sink (`operative_postgres`,
  `Flezha.Plugin.Provider.Postgres`) upserts every record of a flush batch into
  `flezha_records` (keyed by `flz.key`, else the durable identity) and advances the per-Cell
  `flezha_sink_progress` row in ONE transaction; every upsert is guarded by `(epoch, seq)`
  monotonicity, so re-drives and stale epochs are no-ops, and the commit is what confirms the
  batch. An unreachable database delays the flush (retry with backoff; callers park past
  `queue_max`, 10 000) — delayed, never dropped. The schema is operator-owned by default
  (`auto_create: false` fails `health/1` naming the missing tables); a commit stall past
  `unhealthy_after_ms` (30 000) escalates on the plugin-health → convergence path. Unstamped
  records become plain inserts. The **Kafka** producer (`operative_kafka_producer`) uses
  `flz.key` as the message key and carries the durable identity in the `flezha.dedup_key`
  header; see [`../plugins/kafka_producer.md`](../plugins/kafka_producer.md).
- **The websocket sink is not a conforming sink, and does not claim to be.** A plugin bound as
  an output (`operative_websocket`, `Flezha.Plugin.Provider.Websocket`) ships each dispatched
  payload as a frame on the connection it already holds, and answers the hold by its declared
  `sink_ack_mode`: `"none"` (default) confirms when the frame reaches the established socket,
  which is **at-most-once** past that point; `"required"` confirms only when an inbound frame
  echoes the correlation id, which is **at-least-once**. Neither is effectively-once and no
  mode that claims to be is offered — a websocket exposes no transaction to enlist and no key
  to overwrite, so the destination cannot participate. What the sink can do is carry
  `flz.cell` and `flz.seq` in the frame template, and the consumer that tracks the last
  sequence it has applied per cell is the only place the dedup can work. Frames are queued
  while the socket is down (`sink_queue_max`, 10 000; callers park at the bound) and a frame
  whose retry budget is spent parks in the dead-letter store. An enable on a tier operative
  warns rather than refusing: the operative's internal guarantee is unchanged and only its
  websocket hop is best-effort. See
  [`../plugins/websocket_subscription.md`](../plugins/websocket_subscription.md).
- **Source back-pressure.** Ingestion debits a per-operative source credit window
  (`:source_credit_window_bytes`, default = `:wal_high_watermark_bytes`); the min-watermark
  advance mints it back. At zero credit the source adapter's ingest call parks (a websocket
  stops reading its socket), bounding input-log growth against a stalled Cell. A sink outage
  longer than `window / ingest rate` delays ingestion; it never wedges it.
- **Cost-free default.** Operatives without the flag keep every path byte-identical. Under the
  tier a slow or unreachable sink backs the outbox up on disk, never in memory.
- **Workflow operatives** opt in with the same per-record semantics; without the tier a durable
  structured workflow gets the task-boundary guarantee below.

Wire shapes: [`../sdk/protocol_spec.md`](../sdk/protocol_spec.md) "Output identity stamps" and
"cell_effect_record".

### Ack granularity: per input, or per checkpoint

The tier acknowledges an input by making its state durable. An operative that folds *many*
inputs into one coherent push — a window that only closes on its boundary — declares
`checkpoint_ack()` beside `effectively_once()` (`checkpointAck()` in Java) and calls
`ctx.checkpoint()` where its state is coherent:

- **The consumed offset advances only at the checkpoint.** Changes in between are held in the
  Cell's flush buffer; the checkpoint ships the interval's changes with the closing input's
  offset as one record, one fsync.
- **Emits flush as they happen by default.** A mid-interval emit ships in a record carrying no
  offset and reaches its plugin as soon as that record is fsynced.
- **`outputs_at_checkpoint(max_delay_ms)`** (`outputsAtCheckpoint`) holds the interval's outputs
  with its changes and ships them inside the checkpoint record — one WAL append per interval,
  flushed as one batch after the fsync. `max_delay_ms` bounds the latency: the interval closes
  that long after the input that opened it, so a quiet Cell never holds an output
  indefinitely, and an unbounded hold cannot be declared by omission.
- **`checkpoint_every(n)`** (`checkpointEvery`) closes the interval after `n` inputs; an explicit
  `ctx.checkpoint()` still closes it early. The core owns the interval, so every host language
  gets the same rules.
- **A park closes the interval.** An input that exhausts its retries is acknowledged into the
  DLQ together with the interval's state, so a replay starts *after* the poison.
- **Recovery replays the interval.** The Cell is restored to its last checkpoint and the
  controller re-forwards every input after it; the epoch is unchanged and the keyed-sink dedup
  absorbs the re-emitted outputs. A restore discards the open interval.

**The interval is the knob you size the system with**: the input-log retention floor, the
recovery work after a crash, and the cadence at which source credit is minted. An operative
that never checkpoints stops truncating its input log and eventually parks its own source.
`checkpoint_ack()` without `effectively_once()` is refused at build time
(`CheckpointAckWithoutTier` / `checkpointAckWithoutTier()`), and the controller's dataflow-stop
watchdog skips a checkpoint class — silence between checkpoints is its steady state.

## Workflow operatives: staged commit and the transactional outbox

A **durable structured** workflow operative (tasklets declared, the default
`PERSISTENCE_ENABLED`) never lets a tasklet's plugin output escape before that tasklet is
durable. The commit path, crash-resume and journal layout are in
[`workflow_operative.md`](workflow_operative.md); this is the flush contract.

- **Staged, not emitted.** Inside a structured tasklet `ctx.emit` does not reach the wire;
  each Cell buffers its serialized plugin-bound output, so a failed, aborted or cancelled
  tasklet leaks **nothing**. Opted-out classes have no barrier to stage against and keep the
  direct at-least-once path.
- **One durable write.** At the barrier the staged outbox travels inside `TaskCommit` and is
  journaled with the tasklet's completion in ONE `task_committed` record in `task.journal`,
  fsynced by the `FsyncBatcher` tick before the ack. Progress and output cannot tear apart; a
  barrier re-sent under the same reference id is answered from the existing entry.
- **Flush after the durable point.** Only after that fsync does the controller deliver the
  entries, in journal order, through the plugin fan-out live output uses. The `outbox_flushed`
  entry is written when the sinks confirm (`TaskJournal.mark_flushed/5`), not when the chain is
  handed the payload; a re-entered flush does not deliver an entry still with the chain; an
  unbound operative is retried. Compute is not coupled to sink health — the next tasklet runs
  while the flush lands.
- **Crash before flush re-drives.** On restore every `task_committed` (and `compensated`) entry
  with no matching `outbox_flushed` is re-delivered with its original keys before the resumed
  run advances (`TaskJournal.flush_pending/2`). A flushed task is never re-delivered.
- **Branches resume independently.** The restore carries each in-flight commit's handoff under
  its own `task_idx`: a respawn re-runs exactly the uncommitted subtrees, and a committed branch
  neither re-runs nor re-emits. A rollback runs in reverse topological order.
- **Keys.** Each flushed frame carries `flz.cell`, `flz.seq` (`<task_idx>.<seq_in_task>`) and
  `flz.epoch` — the effectively-once identity shape, minted from the journal — and no `flz.key`.
- **The epoch rule.** `0` at first start. A crash re-drive keeps it, so a dedup sink absorbs the
  repeat; a deliberate [rewind](workflow_operative.md#rewind-taking-a-loop-back-to-a-committed-iteration)
  bumps it, so an `(epoch, seq)`-guarded sink overwrites. **Crash keeps the epoch, rewind bumps it.**
- **Compensation rides the same path.** A `compensate` hook's output is staged, journaled in a
  `compensated` entry and flushed after that fsync with a trailing `.1` seq component, so an
  undo never collides with the forward output it reverses.

**Guarantee.** Never lost, never leaked before commit, and **at-least-once with stable keys** on
the re-drive window (between a sink's confirmation and the `outbox_flushed` marker it
triggers); a dedup-capable sink absorbs that duplicate exactly. Inspect what an instance owes
with `flz state journal <operative/instance>`.

## Workflow operatives: the same durable input log

The input half of the same story. A durable workflow instance's input goes through the one
durable input log the tier uses (above), keyed by the instance id: appended **before** it is
forwarded, answered once synced, stamped `flz.offset`, and an append that fails is not
forwarded. A crash after the append re-delivers on rehydrate (at-least-once *into* the
operative, exactly-once *completion* via the consumed offset). A repeat of a `flz.msg_id` the
log still retains is absorbed, so an at-least-once sender cannot produce a duplicate turn,
while a message without an id is always appended.

What differs is the reporter. A workflow's Cells report no consumed offset; its journal does,
under its own reserved identity: each durable iteration commit (and a park that dead-lettered a
message unread) reports the offset it consumed into the log's watermark, and every bind seeds
that row from the highest offset any committed iteration consumed. The watermark is the
instance's floor: retention reclaims below it, the bind re-forwards the retained records above
it, and a rewind never lowers it, so a deferred truncation costs disk, never a re-delivery.

**Source credit applies too.** A durable workflow's appends debit the same per-instance
`:source_credit_window_bytes` window, drained as iterations commit, so a sender that has the
window's worth of input (16 MiB by default) owed by one instance parks until the instance
commits an iteration, instead of growing the log without bound.

**Guarantee.** Input sent to a suspended, crashed or migrating durable instance is **never
lost**. Inspect with `flz state inbox <operative/instance>`, which renders the log of either
kind. An opted-out class keeps its waiting messages in memory only. Mechanics:
[`workflow_operative.md`](workflow_operative.md) "Durable loop and suspension".

## Operative to operative: the cross-hop delivery guarantee

`Flezha.Plugin.Provider.OperativeForward` delivers one operative's output into another's input
stream with **no** new identity scheme, dedup store, ledger or wire format. Three existing parts
assemble the guarantee: the source's outbox stamps `flz.seq` / `flz.key` / `flz.epoch` and makes
the record durable; the plugin's worker holds the record and retries until the target confirms;
the target's ingest absorbs a repeat of a `flz.msg_id` its durable input log still retains and
is offset-addressed in that log.

**The guarantee is derived, never configured** — there is no delivery-mode option, because a
source with no outbox has nothing to re-send from:

| Source operative | Across the hop |
| --- | --- |
| effectively-once stream | effectively-once |
| durable workflow | effectively-once |
| plain stream (no outbox) | at-least-once, best effort — no de-duplication |

**A partitioned target adds a routing precondition.** Forwarding into a partition group routes
by `hash(flz.key)` to the owning member; a message *without* `flz.key` (an unkeyed channel, or
any workflow-outbox output) is **parked to the source's DLQ** with
`partitioned target requires keyed emit` rather than scattered. A park is a durable outcome the
cursor may pass.

**In-flight items survive a controller restart.** Both outbox drivers gate their cursor on the
sink: a durable workflow's entry stays owed until `outbox_flushed` is written and
`flush_pending/2` re-drives it; an effectively-once stream's writer keeps each dispatched entry
in its confirmation tally and re-drives everything above the persisted `outbox_flushed_seq` on
restore. A plain stream keeps retry-until-confirmed within the worker's lifetime only. Verdicts,
refusals and telemetry: [`../plugins/operative_forward.md`](../plugins/operative_forward.md).

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

A permanently-failing (poison) payload in a Stream operative is never silently discarded. A
failing invocation — returned error or panic/exception, same policy in both SDKs — is retried
up to the channel's `retry_max` (default 3 attempts, exponential backoff with jitter); authors
mark an error non-retryable with `TaskletError::non_retryable` /
`TaskletException.NonRetryable` to park immediately. An unroutable forward into a partition
group, the SDK's out-of-range key-group guard, and a frame naming a channel that is not one of the
Operative's entry channels park with no invocation failure at all; the last of those parks under
the name the frame asked for, with an error naming the entry channels the Operative does declare,
and after one attempt, because the same declared set answers every re-attempt. After
exhaustion the payload is **durably parked** to the controller's DLQ store
(`app_data/dlq/<operative_class>/<tasklet_id>/`, JSONL index + payload blobs) and only then
does the Cell advance:

- **Tier off:** the SDK ships a `park_payload` frame and holds the Cell until the controller
  fsyncs the entry and acks (`ParkPayloadAck`), re-sending until acked.
- **Tier on:** the park rides the invocation's atomic effect record (`{no state change, dlq
  entry, consumed offset}`), so a crash can never advance the offset while losing the park.
  Parking advances the consumed offset past the poison, so the min-watermark resumes and a
  credit-parked source resumes granting.

Operator surface: `flz dlq list | show | replay | drop`. All four are `operator` verbs and take
`--token` (or `FLEZHA_TOKEN`) — a parked payload is the failed business record held verbatim, so
reading one is as privileged as discarding it. A verb naming an operative class is authorized in
that class's namespace; one that names none reads across every namespace and is authorized in
`_system`. Replay is explicit-only and re-enters the
ordinary input path (fresh output identity — it journals as a new input); a failing replay
re-parks with `generation` incremented; `drop` is audited. Retention bounds the store by age
AND size (30 d / 4 GiB, more restrictive wins); a retention deletion is genuine loss and is
loud: `[:flezha, :dlq, :retention_dropped]`, exported as `dlq_dropped_by_retention_total`.

**Workflow equivalence.** The DLQ applies to Stream operatives only. A workflow Cell runs the
same first rung — retried in place, non-retryable errors skipping the ladder (see
[Cell retry](workflow_operative.md#cell-retry-the-rung-before-the-abort)) — but its terminal
escape is not a park: the tasklet fails, AbortAll fires and saga compensation unwinds.

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
fn channels(c: &ChannelBuilder) {
    c.effectively_once();
    c.correction_window(600_000);                      // repair up to 10 minutes of past
    c.beyond_window_policy(BeyondWindowPolicy::Drop);  // optional; default ProcessAsArrival
}
```

Three prerequisites are authoring facts the engine cannot repair, and all are hard refusals at
SDK build time:

| Prerequisite | Why |
| --- | --- |
| `effectively_once()` | append-before-forward is what puts the late input in the log |
| an `EventTimeFrom` clock | nothing else yields a time to order a replayed slice by |
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

**Operator replay.** `flz operative replay <operative/tasklet/key> --from <seq|timestamp>`
runs the same transaction with the baseline chosen instead of derived: deploy a fix, heal the
recent past. A target older than retention is refused with `outside_retention`, naming the
earliest available point.

**Limits.** A correction overwrites the keys it re-emits; it does not retract keys it no longer
emits (impossible for event-time-bucketed keys, possible for others — treat the sink as
append-with-overwrite). A downstream operative sees corrected outputs as ordinary keyed updates
and does not itself rewind. The window is bounded by retention by construction.

## Remote durability tier: surviving the loss of a controller's disk

Everything above is local. A controller that dies **with its disk** takes its operatives' state
and the registry record of their existence with it; `WalTransfer` is a one-shot pull at
migration time, not replication. A production boot must choose: with `FLEZHA_DURABILITY_BACKEND`
unset the controller refuses to start, and `FLEZHA_DURABILITY_BACKEND=none` is byte-identical
to a build without the tier (`durability_posture: local_only`).

When enabled, immutable artifacts — snapshots, WAL record bundles, task-journal suffixes, the
durable input log, the registry cspec, parked DLQ records, the authz and schedule stores —
ship to a Postgres, S3-compatible or file backend at boundaries that already exist (checkpoint
close, compaction, workflow commit barriers, a park or a drop) plus one backstop timer
(`ship_interval_ms`, 30 s). The manifest is written last, so a torn upload is invisible to
recovery, which hydrates the artifacts into the local layout and runs the ordinary restore path.

| Kind | Mode | Loss when the disk is gone |
| --- | --- | --- |
| Workflow | declared `SYNC_DURABILITY` | **none** — the barrier is remote before it is acked |
| Workflow | default | at most the work since the last shipped barrier |
| Stream | checkpoint-ack | at most the current, unclosed interval |
| Stream | per-input | at most one `ship_interval_ms` |

Async shipping never touches an ack path: a failing backend delays no append, fsync or
watermark advance. What degrades is the guarantee, loudly — lag past `rpo_target_ms` (120 s)
emits the alarm-grade `[:flezha, :state, :rpo_exceeded]`, surfaced by `flz controller status`
and `/healthz` (`degraded, reason: durability_rpo_exceeded`). Configuration, lineage ownership
across two controllers and the kill-the-disk drill:
[`../operations/remote_durability_tier.md`](../operations/remote_durability_tier.md).

## How to avoid loss: operator playbook

- **Deploy and upgrade runners with a graceful stop, never a kill.** Send SIGTERM (or a
  `flz`-driven `disconnect`) and set the orchestrator grace period
  (`terminationGracePeriodSeconds` / `stop_grace_period`) **longer than the drain** (ack budget
  2 s per operative in parallel, serializer cap 10 s). Roll one node at a time so a peer is
  always live.
- **Restart the controller correctly.** Multi-node: bring the new instance up *before* stopping
  the old one. Sole instance: plain restart; the first reconcile round replays the operative
  list from the durable registry once `FLEZHA_PROCESS_HUB_CLUSTER_SETTLE_MS` (2 000) has passed
  with no peer to wait for. `FLEZHA_PROCESS_HUB_RECONCILE_GRACE_MS` (45 000) only caps the wait
  on a connected peer that never answers.
- **Make sinks idempotent where exactly-once matters.** Declare the channel `.keyed()` so
  re-emits carry identical `flz.key` / `flz.seq` stamps to upsert on, or opt the operative into
  `effectively_once()` — the Kafka and Postgres plugins conform out of the box.
- **Never treat the on-disk registry as the live source of truth** in code or ops; it is
  what the orphan reconcile replays on a cold start.
- **Workflow operatives are durable and at-least-once by default.** Author each tasklet and
  compensation idempotently, or opt the class out of persistence for a fire-and-forget job that
  must re-run from a clean slate.

## Common pitfalls

- `docker kill`, SIGKILL, or a `terminationGracePeriodSeconds` shorter than the drain on a
  runner cuts the drain off → best-effort loss. Use a graceful stop and adequate grace.
- **The runner binary must receive SIGTERM directly.** Run it as PID 1 (exec-form
  `CMD`/entrypoint, or `setpriv`/`gosu` dropping privileges by `exec`). A wrapper that forks the
  binary and force-kills it on SIGTERM — notably `su` ("Session terminated, killing shell") —
  silently degrades a graceful stop to a hard kill.
- Expecting the operative list back instantly after a sole-controller restart: `/healthz`
  reads `recovering` until the first reconcile round, about two seconds on a controller with
  no peers, and then until the replayed operatives are bound to runners again. An operative
  whose runner has not reconnected is held unbound and bound when it does; that is the design,
  not a fault.
- Assuming output is exactly-once: it is at-least-once by default. Dedup downstream or opt into
  the `effectively_once()` tier.

## See also

- [`overview.md`](overview.md) — the map of every resilience mechanism and its owning doc
- [`workflow_operative.md`](workflow_operative.md) — commit path, crash-resume, suspension, retention
- [`../architecture/kernel.md`](../architecture/kernel.md) — the write path, the effect record, the epoch rule, flow control
- [`../internals/state_persistence.md`](../internals/state_persistence.md) — WAL layout, snapshots, retention knobs
- [`../operations/remote_durability_tier.md`](../operations/remote_durability_tier.md) — surviving the loss of a disk
