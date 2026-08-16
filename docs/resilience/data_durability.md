# Data & state durability: what survives, what doesn't, and how to avoid loss

This is the single reference for **where flezha can lose operative state or output, where
it cannot, and how to operate so you never lose data on a planned action.** Read it
with `docs/resilience/process_hub_recovery_model.md` (which covers *operative placement* recovery; this
doc covers *operative state + output* durability).

## The two things that can be "lost"

1. **Operative state** — an operative's accumulated in-memory state (e.g. a running total). The
   authoritative live copy lives on the operative's **one bound runner**; it is backed up
   **asynchronously** to a per-Entity **WAL on the controller** (the runner streams
   `entity_state_change` frames; the controller batches the fsync, ~5 ms).
2. **Output** — what an operative emits on `on_output`. Output is routed **through the
   controller** (the operative sends an output frame; the controller runs the `on_output`
   plugin chain → Kafka/HTTP/operative/etc.). Output is **at-least-once** by default;
   operatives that declare `effectively_once()` get the **effectively-once tier**
   (durable input-log + atomic effect record + fsync-gated flush — see
   "Delivery semantics" below).

The core durability invariant we provide: **on a *graceful* stop, no state is lost; on a
*hard kill*, a small best-effort window can lose the most-recent un-persisted state.**

A third durable artifact is the operative's **plugin manifest** (the `on_output`/feed
plugin chain): it lives in the operative's registry cspec and is written cluster-wide
(`propagate: true`), so an operative restarted from any node re-attaches its full plugin
set and the convergence loop re-adds anything missing. See
`process_hub_recovery_model.md` "Registry writes must be cluster-wide".

## Scenario matrix

| Scenario | Operative state | Output | Why / how |
|---|---|---|---|
| **Graceful runner stop** (SIGTERM / rolling deploy / `disconnect()` / `Drop`) | **No loss** | At-least-once | SDK `shutdown.rs` serializes each operative's *final* state, ships it to the controller, and waits for `RunnerShutdownAck` before exiting. |
| **Single runner SIGKILL** (hard) under load | **Brief best-effort loss** | At-least-once | No graceful drain; the operative rebinds to a peer/itself and restores from the controller WAL, which may lag the last few changes emitted under load. |
| **All runners SIGKILL'd at once** under load | **Brief best-effort loss** | At-least-once | Same WAL-restore path, with no live copy anywhere — reliably exposes the lag (this is what `stateful-resume` provokes). |
| **Runner OOM / container crash / node death** | **Brief best-effort loss** | At-least-once | Same as SIGKILL — no drain runs. |
| **Controller graceful restart / rolling deploy with a live peer** | **No loss** | At-least-once | The new instance syncs the registry from the surviving peer; runners stay connected. |
| **Controller plain restart, sole instance** | **No loss** | At-least-once | The first reconcile round replays the operative list from the durable registry; `/healthz` honestly reports `recovering` until rebound. No operator step, and no variant of this restart discards the list. |
| **Whole cluster down → cold start** | Recovered from the durable registry | n/a | No peer survives, so the reconcile's difference is the whole list and disk is the only record. |
| **Procedure migrated to a controller with no local WAL** (node loss / redistribution / lost hostPath), runner live | **No loss** | At-least-once | The WAL is node-local and unreplicated, so the new controller starts baseline-less. Its first `entity_state_change` (empty WAL, `seq > 0`) triggers a `CompactEntity` **pull** — the runner ships a full-state snapshot that becomes the new baseline; deltas shed while awaiting it are superseded by the snapshot. The controller never pushes an empty `RestoreInitState` (which would reset the live Entity to `init_state`). See [`state_persistence.md`](state_persistence.md) "Baseline recovery: push vs pull". |
| **Node rejoins a live cluster** | Synced from peers; local durable copy kept | n/a | The reconcile's difference against live peers is empty, so the node starts nothing. The on-disk copy is no longer discarded to force that — a peer reporting an empty registry must not be able to erase state cluster-wide. |
| **Kafka/broker down, runners alive** | No loss | Buffered, resumes | Producer reconnects (see kafka producer recovery); offset resumes when the broker returns. |
| **Sustained overload** (state changes outpace fsync — the 06-21 OOM shape) | **No loss** (credit-speaking SDK) | At-least-once | Credit-grant flow control: the sender's byte window (default 16 MiB) is minted back only on WAL fsync, so an exhausted window *parks* the SDK's flush path instead of growing controller memory — park, never drop. Shedding is unreachable for a conformant sender; any shed/overrun on a credit session emits `[:flezha, :state, :credit_anomaly]`. Pre-credit SDKs keep the legacy status-flag + shed/resync path. See [`state_persistence.md`](state_persistence.md) "Flow control". |
| **Workflow operative runner SIGKILL** (durable, the default) | **Brief best-effort loss** (async WAL lag) | At-least-once | Respawns on a new runner, restores committed Entity state via the restore-before-run gate, and resumes **at the first un-journaled procedure**: the proc-journal (`proc.journal`, one gated `ProcCommit` fsync per proc barrier) records each completed procedure + its inter-proc handoff, so committed procs are **skipped, not redone**, and the next proc is seeded from the journaled handoff (Rust SDK; Java keeps the pre-journal full re-run until its parity change). Inspect with `flz state journal`. See [`workflow_operative.md`](workflow_operative.md). |
| **Looping workflow operative runner SIGKILL** (durable) | **Brief best-effort loss** (async WAL lag) | At-least-once | Resumes at the iteration **after** the last journaled `iter_committed` — committed turns never re-run, and the in-flight one re-runs from its own last committed proc. Input it had not consumed is still in the durable inbox. See [`workflow_operative.md`](workflow_operative.md) "Durable loop and suspension". |
| **Input sent to a suspended / crashed / migrating workflow instance** (durable) | **No loss** | n/a | The controller appends it to the instance's `inbox.wal` before forwarding, so it is delivered on rehydrate rather than dropped — and a suspended instance is *woken* by the append. Duplicates inside the undelivered window are absorbed. See "Workflow operatives: the durable inbox". |
| **Controller redeploy while a workflow instance is suspended** | **No loss** | n/a | Suspension is durable state (`parked` journal entry + inbox + snapshots), not a live process: the instance is rediscovered from disk on boot, stays evicted, and rehydrates on the next input or its wake deadline exactly as if nothing happened. |
| **Workflow operative runner SIGKILL** (persistence opted out) | **Full — re-runs from the top** | At-least-once | No WAL to restore from; the operative re-runs its `run`/chain from `args`, and any input in its memory-only inbox is lost. Idempotent by author contract, or don't opt out. |
| **Workflow operative SIGKILL mid-compensation** (durable) | Compensation resumes | n/a | Compensation-intent / -done markers ride the restore; the respawn finishes the outstanding undo hooks and **skips** the already-done ones (no forward re-run). |
| **Poison payload** (procedure fails on every retry) | **No loss** | Entity continues | Retry-then-park: after `retry_max` attempts the payload is durably parked to the DLQ store *before* the entity advances (tier off: fsync-then-ack; tier on: one atomic record with the consumed offset). Inspect/replay/drop via `flz dlq`; only retention (30 d / 4 GiB, loud) ever discards a parked payload. See "Dead-letter queue" below. |

## Why output can briefly get ahead of state (the hard-kill gap)

The runner streams **output frames** and **state-change frames** to the controller
independently, and the controller emits output before the matching state is durably
fsync'd. So on a hard kill, output (e.g. total=135 in Kafka) can outlive the durable
state (WAL=128), and the restored operative comes back slightly stale. We deliberately do
**not** close this with per-record acking (it would add a network round-trip to every
message and cripple throughput). Instead:

- **Planned stops drain gracefully → zero loss** (the SDK ships final state with ack).
- **Hard kills accept the small window** — rare, and full protection requires the
  expensive checkpoint/2-phase-commit machinery (this is what Apache Flink's end-to-end
  exactly-once buys, at a real cost). flezha is "durable state + at-least-once output."

**FsyncBatcher and the lag window.** The WAL writer does not fsync after every record.
`State.FsyncBatcher` coalesces dirty-mark casts from all per-entity `WalWriter`
processes and issues one `:file.datasync/1` call per writer per cadence tick (default
5 ms). On a hard kill, records written within the last ~5 ms may not have been
fsynced and can be lost. This is the mechanical source of the "async WAL lag" in the
scenario matrix above. The fsync cadence is configurable via
`:flezha, :agent_fsync_interval_ms`; lowering it reduces the lag window at the cost
of higher I/O pressure. Compare: the audit-log writer has a separate `:per_record` vs
`:batched` fsync mode (`:per_record` is its default); WAL state persistence always
uses the batched FsyncBatcher — per-record WAL fsync is not supported.

## Delivery semantics: at-least-once default, effectively-once tier

**Default (at-least-once + identity).** Output leaves on the primary
stream as it is produced; nothing at the controller remembers what was
delivered, so a crash-replay can re-emit. Channels declared `.keyed()` in
the Rust SDK stamp every plugin-bound frame with reserved metadata —
`flz.entity` (emitting Entity), `flz.seq` (`<change_seq>.<emit_ordinal>`,
derived from the durable WAL seq), and `flz.key` (the natural key supplied
via `ctx.emit_keyed(key, value)`). Because the stamps derive only from
replay-stable inputs, a crash-restore replay re-emits frames with
**identical** stamps, so a downstream consumer that upserts on `flz.key`
(candles, indicators — any changelog-shaped stream) absorbs duplicates
exactly. The determinism contract this rests on — no ambient time, UUIDs,
or external reads in `run`/`apply_change`/`compensate`; use `ctx.now()`
and `ctx.stable_id()` instead — is documented on the `Procedure` trait and
verified by `ProcedureTestHarness::replay_emits_equal`. In the default
tier `flz.seq` is **not** an exact append-only dedup handle
(no-state-change invocations can collide on it).

Nothing at the controller journals a default-tier input, which is also what
decides the fate of input still queued in an Entity's mailbox when a rebind
restores it: with no log to re-forward from, that queue is the only copy the
input has, so the restore keeps it. Under the tier — where the input log *is*
that copy — the restore clears it instead. See
`docs/resilience/state_persistence.md` "What a restore does to input already
queued".

The SDK cannot mint `flz.epoch` — the epoch belongs to the controller's
durable outbox lineage — so a keyed record without the tier carries
`flz.entity`, `flz.seq` and `flz.key` and no epoch. A conforming sink reads
that as epoch `0` and orders the record by `flz.seq` alone, which is exactly
the changelog upsert those stamps exist to enable. The one thing it cannot do
is survive a **regressing** sequence: an Entity that cold-starts without
pulling a baseline restarts its `change_seq`, and the sink correctly refuses
the lower identity rather than guess. The epoch — which a WAL realign bumps —
is what resolves that, and it exists only under the tier.

**Effectively-once tier (opt-in, `effectively_once()` on the operative
builder).** The built-in upgrade of *at-least-once delivery + deterministic
key + idempotent sink = effectively-once*:

- **Durable input-log.** The controller journals every inbound payload
  (post-`:on_input`) into a per-operative `input.log` before forwarding,
  stamping the offset as `flz.offset`. Retention truncates only below the
  minimum durable consumed-offset across the operative's Entities; a
  frozen watermark past the size threshold emits
  `[:flezha, :state, :input_log_frozen_watermark]` instead of silently
  accumulating.
- **Atomic effect record.** In tier mode plugin emits leave on the
  reliable state sink *inside* the invocation's `entity_effect_record` —
  state change(s), outputs, and consumed offset are ONE WAL record under
  ONE fsync. No partial combination is ever durable, and effect records
  are never coalesced away.
- **Fsync-gated outbox flush.** Outputs reach the `:on_output` plugin
  chain only after their record is fsynced, stamped with the durable
  identity `(flz.entity, outbox_seq = flz.seq, flz.epoch)` — now a
  sufficient handle for exact append-only dedup. On restart the controller
  re-drives every entry above the per-Entity `outbox_flushed_seq` cursor
  in order with identical identity: outputs are **never lost, never
  applied twice at a conforming sink** (Kafka: message key = `flz.key`,
  header `flezha.dedup_key` — see `docs/kafka-producer.md` for the
  changelog-vs-append contract).
- **Postgres sink (built-in conforming sink).** The `operative_postgres`
  `io_binding` plugin (`Flezha.Plugin.Provider.Postgres`) makes the tier
  effectively-once *into the database*: per flush batch ONE transaction
  upserts every record into `flezha_records` (keyed by `flz.key`, falling
  back to the durable identity for keyless tier outputs) and advances the
  per-Entity `flezha_sink_progress` watermark row — data and progress can
  never diverge. Every upsert is guarded by `(epoch, seq)` monotonicity,
  so controller re-drives and stale epochs are byte-exact no-ops. An
  unreachable database delays the flush (retry with backoff; enqueue
  callers park past `queue_max`) — data delayed, never dropped. The schema
  is fixed and operator-owned by default (`auto_create: false` fails
  `health/1` with the missing tables); commit stalls past
  `unhealthy_after_ms` surface on the same plugin-health → convergence
  escalation path as Kafka. Unstamped (at-least-once) records become plain
  inserts and never touch the watermark.
- **Source back-pressure.** Ingestion debits a per-operative source credit
  ledger; the min-watermark advance mints it back. At zero credit the
  source adapter's ingest call parks (the websocket stops reading its
  socket), bounding input-log growth against a stalled Entity.
- **Cost-free default.** Operatives without the flag keep every path
  byte-identical — no input-log writes, no outbox, no gating. Slow or
  unreachable sinks back the outbox up on disk (never memory) and delay
  outputs; data is never dropped — the correct failure mode for the tier.
- **Workflow operatives** opt in with the same per-record semantics: each
  proc's changes + keyed emits form one atomic effect record, and controller
  re-drives carry identical identity. Without the tier, a durable
  structured workflow gets the proc-boundary guarantee below instead.

Non-keyed channels are byte-identical to before and remain plain
at-least-once. See `docs/sdk_protocol_spec.md` "Output identity stamps"
and "entity_effect_record".

### Ack granularity: per input, or per checkpoint

The tier acknowledges an input by making its state durable — that is what
one atomic record per invocation buys. But some operatives fold *many*
inputs into one state push: a window that only closes on its boundary, a
batch that only means something complete. Pushing per input is not merely
expensive for them, it is incoherent — the offset would advance with state
they did not mean to publish, and a restore would resume past inputs the
restored state never saw.

Such an operative declares `checkpoint_ack()` alongside `effectively_once()`
and calls `ctx.checkpoint()` where its state is genuinely coherent:

- **The consumed-offset advances only at the checkpoint.** Changes applied
  in between are held in the Entity's flush buffer and are not durable on
  their own. The checkpoint ships the whole interval's changes together with
  the offset of the input that closed it — one record, one fsync. State and
  offset advance together or not at all.
- **Emits still flush as they happen.** Checkpointing moves the
  *acknowledgement*, not the output: a mid-interval emit ships in a record
  carrying no offset, and reaches its plugin as soon as that record is
  fsynced.
- **A park closes the interval.** An input that exhausts its retries is
  acknowledged into the dead-letter queue along with the interval's state,
  so a later replay starts *after* the poison rather than re-tripping it.
- **Recovery replays the interval.** The Entity is restored to its last
  checkpoint and the controller re-forwards every input after it — the same
  crash-resume re-forward the tier already does, just over a longer tail. The
  epoch is unchanged (only a deliberate rewind bumps it), the interval's
  outputs are emitted again, and the keyed-sink dedup the tier already
  requires absorbs them.

**The interval is the knob you size the system with.** It is simultaneously
the input-log retention floor (truncation never crosses the last checkpoint),
the recovery work after a crash, and the granularity at which source credit is
minted — grants arrive in bursts at checkpoint cadence, and the source parks in
between if it exhausts them. Checkpoint rarely and you pay in disk and recovery
time; checkpoint often and you approach the per-input default. An operative that
*never* checkpoints stops truncating its input log and eventually parks its own
source — the same back-pressure any non-consuming Entity gets.

Per-input remains the default and is byte-identical to a tier operative that
never heard of the mode. `checkpoint_ack()` without `effectively_once()` is
refused at build time (there is no durable offset to hold back, and no durable
input log to replay from); the keyed-channel requirement is already enforced for
the whole tier. Because such an operative deliberately makes nothing durable for
a whole interval, the controller's dataflow-stop watchdog skips it — silence
between checkpoints is its steady state, not a dead feed.

## Workflow operatives: staged commit and the transactional outbox

A **durable structured** workflow operative (procedures declared, the
default `PERSISTENCE_ENABLED`) never lets a procedure's plugin output
escape before that procedure is durable.

- **Staged, not emitted.** Inside a structured procedure `ctx.emit` does
  not reach the wire: each entity buffers its serialized plugin-bound
  output. A failed, aborted, or cancelled procedure therefore leaks
  **nothing** — the staged bytes die with the run. (The `run`-sugar form
  has no procedures, hence no barrier to stage against and no journal
  entry to gate a flush on; it keeps the direct at-least-once wire path,
  as do opted-out classes.)
- **One durable write.** At the barrier the staged outbox travels to the
  controller inside the `ProcCommit` request and is journaled with the
  procedure's completion in ONE `proc_committed` record in the instance's
  `proc.journal` (fsynced before the ack). Progress and output cannot tear
  apart.
- **Flush after the durable point.** Only once that fsync lands does the
  controller deliver the entries — in journal order, through the same
  plugin fan-out live workflow output uses — then record an
  `outbox_flushed` entry. A delivery that cannot happen yet (operative not
  bound) is retried; entries are never silently dropped. Compute progress
  is not coupled to sink health: the next procedure runs while the flush
  lands, because the chain hands data forward through the handoff, never
  through the sink.
- **Crash before flush re-drives.** On restore, every `proc_committed` (and
  `compensated`) entry with no matching `outbox_flushed` is re-delivered
  with its **original keys** before the resumed run advances. A proc whose
  flush IS recorded is never re-delivered.
- **Branches resume independently.** The procedure topology is a tree
  (`workflow-branching`), so the restore state carries each in-flight
  commit's handoff under its own `proc_idx`: a respawn re-runs exactly the
  **uncommitted subtrees**, seeding each from its own parent's edge, and
  a branch that already committed neither re-runs nor re-emits. The
  operative is complete when every **leaf** procedure has committed — a
  fact the controller derives from the journal alone, against the leaf
  set the SDK declares at `AddOperative`. A rollback runs in **reverse
  topological** order: no procedure is undone before its compensated
  descendants (see `docs/resilience/workflow_operative.md`).
- **Keys.** Each flushed frame carries `flz.entity`, `flz.seq`
  (`<proc_idx>.<seq_in_proc>`) and `flz.epoch` — the effectively-once output
  identity shape, minted from the journal, so a keyed sink adapter applies
  unchanged.
- **The epoch rule.** The epoch is `0` at first start. A crash re-drive keeps
  it, so the re-driven output keys identically to its original delivery and a
  dedup sink absorbs it. A **deliberate** rewind bumps it
  ([loop rewind](workflow_operative.md#rewind-taking-a-loop-back-to-a-committed-iteration)),
  so what the resumed loop emits is *newer* than what the unwound iterations
  emitted, and a `(epoch, seq)`-guarded sink overwrites rather than dedups.
  One rule, both kinds: **crash keeps the epoch, rewind bumps it.**
- **Compensation undo-effects ride the same path.** A `compensate` hook's
  plugin output is staged per entity, journaled in a `compensated` entry,
  and flushed after that fsync with a comp-marked key (a trailing `.1` seq
  component), so an undo can never collide with the forward output it
  reverses. The per-entity intent/done markers still decide which hooks
  run on resume.

**Guarantee.** Never lost, never leaked before commit — and **at-least-once
with stable keys** on the re-drive window (the overlap between a delivery
and its unwritten `outbox_flushed` marker). A dedup-capable sink absorbs
that duplicate exactly; without one, a re-driven output can appear twice.
Inspect what an instance owes with `flz state journal <instance>`.

## Workflow operatives: the durable inbox

The **input** half of the same story. A durable workflow instance's post-start
input (a chat turn, an agent message) is appended to a per-instance
`inbox.wal` — the same `WalWriter` + `FsyncBatcher` substrate as `proc.journal`,
living beside it under `<state_root>/<instance>/<instance>/` — **before** it is
forwarded to the runner:

- **Append-before-forward.** A crash after the append but before delivery
  re-delivers on rehydrate: at-least-once *into* the operative, exactly-once
  *completion* via the consumed offset. An append that fails is not forwarded
  either — a frame the sender can retry beats an undurable one it believes was
  accepted.
- **Offsets and dedup.** Offsets are monotonic (1-based; `0` means "nothing
  consumed"). The dedup id is the sender's `flz.msg_id` metadata when present,
  else a payload hash; a duplicate inside the **undelivered window** is absorbed
  at append, so an at-least-once sender cannot produce a duplicate turn. Two
  legitimately identical payloads sent inside that window need a sender id.
- **Truncation.** `iter_committed` records the offset the iteration consumed and
  the inbox releases everything at or below it. Truncation is advisory cleanup:
  delivery is filtered by the *same* consumed offset, so a lost truncate costs
  disk, never correctness. Message payloads live only in `inbox.wal`, never in
  `proc.journal` — the two files have deliberately different retention.

**Guarantee.** Input sent to a suspended, crashed, or migrating durable workflow
instance is **never lost**: it is durable before it is forwarded and delivered
when the instance next runs. Inspect with `flz state inbox <instance>`. An
opted-out class keeps the memory-only inbox (input in flight at a crash is
lost — that is what opting out buys). See
[`workflow_operative.md`](workflow_operative.md) "Durable loop and suspension".

## Operative to operative: the cross-hop delivery guarantee

`Flezha.Plugin.Provider.OperativeForward` delivers one operative's output into
another operative's input stream. It introduces **no** new identity scheme,
dedup store, durable ledger, or wire format: the guarantee is assembled from
three parts that already exist.

1. The source's transactional outbox stamps `flz.seq` / `flz.key` / `flz.epoch`
   and makes the record durable.
2. The plugin's worker retries until the target confirms.
3. The target's `journal_input/2` absorbs a repeat of `flz.msg_id` (else payload
   hash) inside its undelivered window, and is offset-addressed for the
   effectively-once tier.

**The guarantee is derived, never configured.** There is no delivery-mode
option, because what a sender gets follows from what it already declares:

| Source operative | What it gets across the hop |
|---|---|
| effectively-once stream | effectively-once |
| durable workflow | effectively-once |
| plain stream (no outbox) | at-least-once, best effort — no de-duplication |

An acknowledgement is only worth having if the sender can act on it, and acting
means re-sending from a durable record. A source with no outbox has nothing to
re-send from, so an ack mode would sell it a guarantee that cannot be honoured.

**Limitation — in-flight items do not survive a controller restart.** Verified
against both outbox drivers, and the answer is the same for each:

- durable workflow: `ProcJournal.flush_entry/3` casts `{:io_out, ...}` to the
  Overwatch and *then* appends the `outbox_flushed` marker. Once that marker
  lands the entry is no longer owed, so `flush_pending/2` will not re-drive it.
- effectively-once stream: `WalWriter.dispatch_outbox_entries/2` casts, then
  advances `outbox_flushed_seq` and persists it. A restore re-drives only
  entries *above* that persisted seq.

Neither cursor is gated on a sink acknowledging anything, so a payload sitting
in the forward worker's in-memory queue when the controller dies is lost. The
window runs from the marker/cursor advance to the confirming delivery, workflow
by the retry backoff.

This is the same posture as every other sink — the Kafka producer's buffer and
the Postgres sink's buffer are lost identically. Diverging here would mean a
second durable ledger beside the outbox that already exists.

See [`../operative_forward_plugin.md`](../operative_forward_plugin.md) for the
verdicts, refusals, and telemetry.

## Per-Entity timers: folded state, not a scheduler

A timer registration, cancellation and firing are each a durable record in the
Entity's **own change log**, so the pending set is rebuilt by the ordinary fold
on restore. There is no timer side-file, no timer journal entry type, no crash
marker and no completeness oracle — and therefore nothing new to make
crash-consistent.

What falls out of that, rather than being built:

| Crash point | Outcome |
| --- | --- |
| before the registration is durable | the timer was never owed |
| after the registration, before firing | the timer is pending and fires when its condition is next met |
| after firing | the removal folded in; it does not re-fire |
| mid-firing, before the record is durable (tier) | neither the effects nor the removal landed — the whole firing repeats |

The ops ride the wire two ways, both of which the controller already stores
verbatim and never decodes: outside the effectively-once tier as a
`format_id = 3` record under the ordinary `entity_state_change` payload-id
(body: a postcard `Vec<TimerOp>`), and inside the tier as
`EffectRecordBody.timers`.

**Under the tier a firing is atomic with its effects.** One firing writes ONE
effect record carrying the Changes `on_timer` applied, the values it emitted,
and the removal of the fired timer. Its `consumed_offset` is **absent** — a
firing consumes no input offset, so the input-log watermark does not advance on
it, and every reader already tolerates a nil offset.

**Without the tier, firing degrades to at-least-once**, exactly as input
processing does under the same conditions; `on_timer` bodies carry the same
idempotence advice as `run`. A firing that exhausts its retry budget parks to
the dead-letter queue like a poison input — and its removal is durable either
way, so a poison timer parks once instead of firing forever.

Compaction is the one place the fold needs help: a snapshot covers `State`
alone, so the SDK re-states the full pending set as a fresh record at a seq
ABOVE `snapshot_seq`. Truncation keeps it, and a crash between the two leaves
the untruncated log, which still holds the originals. See
[`../entity_timers.md`](../entity_timers.md).

## Dead-letter queue: poison payloads park, never drop

A permanently-failing (poison) payload in a Stream operative is never
silently discarded (`dead-letter-queue`). A failing invocation —
returned error or panic/exception, same policy in both SDKs — is retried
up to the input channel's `retry_max` (default 3 total attempts,
exponential backoff + jitter; authors classify errors non-retryable via
`ProcedureError::non_retryable` / `ProcedureException.NonRetryable` to
park immediately). After exhaustion the payload is **durably parked** to
the controller's DLQ store (`app_data/dlq/<operative_class>/<procedure_id>/`,
JSONL index + payload blobs) and only then does the entity advance:

- **Tier off:** the SDK ships a `park_payload` frame and holds the
  entity until the controller fsyncs the entry and acks
  (`ParkPayloadAck`) — park-then-advance, re-sending until acked.
- **Tier on:** the park rides the invocation's atomic effect record
  (`{no state change, dlq entry, consumed offset}`), so a crash can never
  advance the offset while losing the park; the entry materializes from
  the record after fsync, idempotent on origin identity. Parking advances
  the consumed offset past the poison payload, so the **min-watermark
  resumes** — input-log retention proceeds and a credit-parked source
  loop resumes granting.

Operator surface: `flz dlq list | show | replay | drop`. Replay is
explicit-only and re-enters the ordinary input path (fresh output
identity by construction — the replay journals as a new input); a failing
replay re-parks with `generation` incremented; `drop` is audited.
Retention bounds the store by age AND total size (defaults 30 d / 4 GiB,
more restrictive wins); a retention deletion discards already-parked data
and is deliberately loud (`dlq_dropped_by_retention_total` + a dedicated
event naming the entry).

**Workflow equivalence:** the DLQ applies to Stream operatives only.
A Workflow entity runs the same first rung — its error is retried in
place (default 3 attempts, jittered exponential backoff, non-retryable
errors skipping the ladder; see
[Entity retry](workflow_operative.md#entity-retry-the-rung-before-the-abort))
— but its terminal escape is not a park: the procedure fails, AbortAll
fires, and saga compensation unwinds, so nothing is written to the DLQ
store. Configurable `on_entity_failure` policies (DeadLetter |
MinSuccess) remain deferred.

## Late-input correction: fix the past, and the sinks fix themselves

An exchange sends a trade five seconds late. It lands after the candle it
belongs to has already closed and published. Without correction, that candle
is permanently wrong — and so is every indicator downstream of it.

Correction is **recovery with a re-ordered input sequence**. It reuses the
recovery machinery wholesale: rewind the Entity to a baseline *behind* the
late event, restore the runner there, and re-forward the retained input log —
except ordered by event time rather than by arrival. Outputs republish under a
bumped epoch, so a keyed changelog sink overwrites the stale rows by natural
key. One machine, two triggers: a late input, or an operator.

**There is nothing to splice.** Inputs are journaled *before* they are
forwarded (`Ingest.journal_input/2`), so a late input is already durably in
the input log at its arrival offset by the time anything knows it is late.
Re-reading the range already contains it. Only the *order* was ever wrong.

### Declaring it

```rust
fn channels(c: &ChannelBuilder) {
    c.effectively_once();
    c.correction_window(600_000);          // repair up to 10 minutes of past
    c.beyond_window_policy(BeyondWindowPolicy::Drop);   // optional; default below
    ...
}
```

Three prerequisites are **authoring facts the engine cannot repair**, and all
three are hard refusals at SDK build time — an operative that runs happily and
then cannot correct when asked is the worst outcome:

| Prerequisite | Why |
|---|---|
| `effectively_once()` | append-before-forward is what puts the late input in the log at all |
| an `EventTimeFrom` clock | nothing else yields a time to order a replayed slice by |
| every plugin channel `.keyed()` | a bumped epoch overwrites *by key*; an unkeyed append channel would duplicate, not correct (already implied by the tier) |

**Retention is not a prerequisite — the declaration derives it.** A correction
over a 10-minute window *means* retaining 10 minutes of input log and 10 minutes
of WAL history; that is what the declaration says, not something an operator has
to guess separately. So:

- `InputLog.hold_window_ms/1` resolves to `max(:input_log_hold_window_ms, the
  declared window)` — truncation never removes an input the correction would
  replay.
- `PersistenceResolver.compact_after_age_ms/1` is never shorter than the declared
  window — compaction always leaves a baseline the rewind can land on. This is why
  it is resolved **per operative**: it stopped being a tuning knob and became a
  correctness constraint. An age floor of `0` disables age-triggered compaction
  entirely, which retains WAL history without an age bound and already satisfies
  any window, so `0` is preserved rather than raised.

An operator-configured floor that is already more generous wins. Because a
one-line declaration can commit the controller to real disk, a floor that had to
be raised is logged once and emitted as
`[:flezha, :correction, :retention_derived]` at bind, naming the floor, its
configured value, and its derived value.

### What happens

1. The SDK extracts the event time (only it can — the extractor is user code)
   and compares it to the Entity's event-time high-water. Behind it, but inside
   the window → the input is **not dispatched**. It is already durable in the
   log; running it now would fold it into the state at the wrong position, which
   is the exact mistake the correction undoes. A `correction_request` goes to the
   controller instead.
2. Requests coalesce per Entity (debounced) into the one replay that reaches
   furthest back. Five late trades are one restore-replay cycle, not five.
3. The controller picks the baseline: the last seq whose history entirely
   precedes the target. It scans *forward* to the first record at or after the
   target — scanning backwards for the first earlier event time would stop at an
   out-of-order straggler and leave the records that overtook the late input in
   place, correcting nothing.
4. `WalWriter.rewind_to(seq, epoch)` keeps the records at or below the baseline
   and drops only the snapshots above it. This is **not** the realign primitive,
   which truncates the WAL to zero and deletes every snapshot — that is a
   cold-takeover wipe. The `persist_dedup` at the rewind's tail is the **commit
   point**: the epoch bump is durable before any output can be re-emitted under
   it.
5. `RestoreInitState` re-seats the runner on the baseline (blocking — the
   restore rides the control channel while the replay rides the data channel, and
   without the fence they race). The Entity clears its mailbox: every queued
   input is already a member of the slice by append-before-forward, so keeping it
   would double-apply it *and* apply it after the sorted slice, reintroducing the
   mis-ordering.
6. The input-log range above the baseline's consumed offset is re-forwarded,
   marked `flz.replay`. The Entity holds the whole slice, orders it by extracted
   event time (stably, tie-broken by ascending offset), and only then dispatches.
7. Outputs flush through the normal outbox under the new epoch. Kafka key-upserts
   and the Postgres `(epoch, seq)` guard both admit the higher epoch and overwrite.

### Crash mid-correction needs no marker

That falls out of the design rather than being bolted on:

- **Before the rewind commits**, nothing durable changed. The late input is still
  in the log, still unconsumed. The ordinary resume re-forwards it, the SDK
  re-classifies it as late, and the correction runs again.
- **After the rewind commits**, the on-disk state *is* the baseline and the
  watermark was lowered to match — so the ordinary resume replay **is** the
  correction.

Hence: for a correction-declaring operative, *every* journaled replay is marked
and event-time-ordered. Crash recovery and deliberate correction are one path.
The epoch is passed to `rewind_to` explicitly rather than derived, so a re-run
re-asserts the epoch it already committed to instead of bumping a second time —
a second bump would strand the first attempt's rows at an epoch nothing
overwrites.

### Beyond the window

An input later than the window follows the declared policy:
`process_as_arrival` (the **default** — it is what the engine does today, so
declaring a window never silently starts discarding data) or `drop`. Both emit
`[:flezha, :correction, :beyond_window]` so the lateness is visible and the
operator can widen the window deliberately.

### Operator replay

`flz operative replay <operative/procedure/key> --from <seq|timestamp>` runs the
same transaction with the baseline chosen instead of derived: deploy a fix,
heal the recent past. A target older than retention is refused with
`outside_retention`, naming the earliest available point.

### Limits worth knowing

- **A correction overwrites the keys it re-emits; it does not retract keys it no
  longer emits.** If the stale run published a row under a key the corrected run
  never produces, that row survives. For event-time-bucketed keys (candles,
  windowed aggregates) this cannot happen — the key is a function of the event
  time. For key schemes where it can, treat the sink as append-with-overwrite,
  not as a mirror.
- **No cross-operative cascade.** A downstream operative sees corrected outputs
  as ordinary keyed updates; it does not itself rewind.
- **The window is bounded by retention, by construction.** There is no
  unbounded-history correction.

## Remote durability tier: surviving the loss of a controller's disk

Everything above is local. A controller that dies **with its disk** takes its
operatives' state — and the registry record of which operatives existed — with
it; `WalTransfer` is a one-shot pull at migration time, not replication. The
remote durability tier is the answer, and it is **off by default**
(`FLEZHA_DURABILITY_BACKEND=none`), under which behaviour is byte-identical to a
build without it.

When enabled, it ships immutable artifacts — compaction snapshots,
seq-addressed WAL record bundles, proc-journal and inbox suffixes, the input log,
the registry cspec — to a Postgres or S3-compatible backend at boundaries that
already exist (checkpoint close, compaction, workflow commit barriers), plus one
backstop timer. The per-reference manifest is written last, so a torn upload is
invisible to recovery.

**RPO by kind:**

| Kind | Mode | Loss when the disk is gone |
| --- | --- | --- |
| Workflow | declared `SYNC_DURABILITY` | **none** — the barrier is remote before it is acked |
| Workflow | default | at most the work since the last shipped barrier |
| Stream | checkpoint-ack | at most the current, unclosed interval |
| Stream | per-input | at most one `ship_interval_ms` (default 30 s) |

**Failure posture.** Async shipping never touches an ack path: a failing backend
delays no append, fsync, ack or watermark advance, and retries on the shared
backoff ladder while local operation continues. What degrades is the guarantee,
and it is loud — `[:flezha, :state, :rpo_exceeded]` is alarm-grade, surfaced in
`flz controller status` and `/healthz`. Synchronous mode instead stalls the
opted-in workflow at its barrier: visible, bounded by the existing ack budget,
and correct, because the class asked for RPO 0. Neither mode loses data
silently.

**Fencing.** Every manifest write carries the writer's WAL-owner epoch and is
applied conditionally, which makes the remote store an external arbiter: a stale
claimant's write is refused, it stops shipping that reference and alarms rather
than retrying. This extends the existing local epoch fencing outward — it does
not replace it.

**Recovery** hydrates the manifest-referenced artifacts into the local layout
and then runs the existing restore-and-replay path unchanged. Local state is
never overwritten. Full detail, configuration and the kill-the-disk drill:
[`../remote_durability_tier.md`](../remote_durability_tier.md).

## How to avoid loss — operator playbook

- **Deploy / upgrade runners with a graceful stop, never a kill.** Send SIGTERM (or
  `flz`-driven `disconnect`) and set the orchestrator grace period
  (`terminationGracePeriodSeconds` / `stop_grace_period`) **longer than the drain**
  (serialize + ack; default ack budget 2 s/operative, serialize cap 10 s). Roll one node at
  a time so a peer is always live.
- **Restart the controller correctly:**
  - Multi-node / rolling: bring the new instance up *before* killing the old → peer sync,
    zero loss.
  - Sole instance: plain restart. The first reconcile round replays the operative list
    from the durable registry within `FLEZHA_PROCESS_HUB_RECONCILE_GRACE_MS`.
- **Make sinks idempotent where exactly-once matters.** Output is at-least-once by
  default and can be re-emitted after a rebind/restore — use Kafka keys/transactions,
  HTTP idempotency keys, or DB upserts downstream. Declare the channel `.keyed()` so
  re-emits carry identical `flz.key`/`flz.seq` stamps to upsert on — or opt the
  operative into `effectively_once()` for the built-in tier (see "Delivery semantics"
  above; the built-in Kafka and Postgres sink plugins are conforming out of the box).
- **Never treat DETS as the source of truth** in code or procs — it is a cold-start backup
  only (see `docs/resilience/process_hub_recovery_model.md`).
- **Workflow operatives are durable + at-least-once by default.** A crash re-runs the
  procedure chain over restored state, so author each procedure (and compensation)
  **idempotently**, or **opt the class out of persistence** for a fire-and-forget job that
  must re-run from a clean slate. See [`workflow_operative.md`](workflow_operative.md).

## Common pitfalls

- `docker kill` / `kill -9` / `terminationGracePeriodSeconds` too short on a runner →
  drain is cut off → best-effort loss. Use graceful stop + adequate grace.
- **The runner binary must receive SIGTERM directly.** Run it as PID 1 (exec-form
  `CMD`/entrypoint, or `setpriv`/`gosu` to drop privileges by `exec`). A wrapper that
  forks the binary and force-kills it on SIGTERM — notably `su` ("Session terminated,
  killing shell") — terminates the SDK before its graceful drain ships final state, so
  a "graceful" stop silently degrades to a hard kill. The prod `market-executor` is
  correct (exec-form `CMD ["./market_analyzer"]`, `runAsUser: 1000`, no `su`).
- Plain-restarting the sole controller and expecting the operative list back instantly /
  `/healthz` to read `recovering` → use recovery mode instead.
- Assuming output is exactly-once → it is at-least-once by default; dedup downstream
  or opt the operative into the `effectively_once()` tier.
- Forcing a node to recover from its own DETS while peers are alive → split-brain /
  stale-state crash (see the recovery-model doc).
