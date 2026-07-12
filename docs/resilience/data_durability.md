# Data & state durability: what survives, what doesn't, and how to avoid loss

This is the single reference for **where flezha can lose operative state or output, where
it cannot, and how to operate so you never lose data on a planned action.** Read it
with `docs/resilience/process_hub_recovery_model.md` (which covers *operative placement* recovery; this
doc covers *operative state + output* durability).

## The two things that can be "lost"

1. **Operative state** — an operative's accumulated in-memory state (e.g. a running total). The
   authoritative live copy lives on the operative's **one bound executor**; it is backed up
   **asynchronously** to a per-Segment **WAL on the controller** (the executor streams
   `segment_state_change` frames; the controller batches the fsync, ~5 ms).
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
| **Graceful executor stop** (SIGTERM / rolling deploy / `disconnect()` / `Drop`) | **No loss** | At-least-once | SDK `shutdown.rs` serializes each operative's *final* state, ships it to the controller, and waits for `ExecutorShutdownAck` before exiting. |
| **Single executor SIGKILL** (hard) under load | **Brief best-effort loss** | At-least-once | No graceful drain; the operative rebinds to a peer/itself and restores from the controller WAL, which may lag the last few changes emitted under load. |
| **All executors SIGKILL'd at once** under load | **Brief best-effort loss** | At-least-once | Same WAL-restore path, with no live copy anywhere — reliably exposes the lag (this is what `stateful-resume` provokes). |
| **Executor OOM / container crash / node death** | **Brief best-effort loss** | At-least-once | Same as SIGKILL — no drain runs. |
| **Controller graceful restart / rolling deploy with a live peer** | **No loss** | At-least-once | The new instance syncs the registry from the surviving peer; executors stay connected. |
| **Controller plain restart, sole instance** (marker present) | State in WAL preserved; **operative *list* briefly unknown (~6 s)** | At-least-once | Skip-replay boot wipes local DETS by design; the registry is rebuilt from executors re-announcing. `/healthz` is `healthy` (vacuously) during the gap — safe for kubelet, but not "recovering". Prefer recovery mode (below). |
| **Controller recovery-mode restart, sole instance** (marker removed / `flz cluster prepare-recovery`) | **No loss** | At-least-once | Replays the registry from DETS; `/healthz` honestly reports `recovering` until rebound. The correct way to restart the *only* instance. |
| **Whole cluster down → cold start** | Recovered from DETS | n/a | The ONE case DETS replay is for: no peer survives, disk is the only record. |
| **Operation migrated to a controller with no local WAL** (node loss / redistribution / lost hostPath), executor live | **No loss** | At-least-once | The WAL is node-local and unreplicated, so the new controller starts baseline-less. Its first `segment_state_change` (empty WAL, `seq > 0`) triggers a `CompactSegment` **pull** — the executor ships a full-state snapshot that becomes the new baseline; deltas shed while awaiting it are superseded by the snapshot. The controller never pushes an empty `RestoreInitState` (which would reset the live Segment to `init_state`). See [`state_persistence.md`](state_persistence.md) "Baseline recovery: push vs pull". |
| **Node rejoins a live cluster** | Synced from peers; local DETS discarded | n/a | Correct — local DETS is stale; peers are authoritative. |
| **Kafka/broker down, executors alive** | No loss | Buffered, resumes | Producer reconnects (see kafka producer recovery); offset resumes when the broker returns. |
| **Sustained overload** (state changes outpace fsync — the 06-21 OOM shape) | **No loss** (credit-speaking SDK) | At-least-once | Credit-grant flow control: the sender's byte window (default 16 MiB) is minted back only on WAL fsync, so an exhausted window *parks* the SDK's flush path instead of growing controller memory — park, never drop. Shedding is unreachable for a conformant sender; any shed/overrun on a credit session emits `[:flezha, :state, :credit_anomaly]`. Pre-credit SDKs keep the legacy status-flag + shed/resync path. See [`state_persistence.md`](state_persistence.md) "Flow control". |
| **Bounded operative executor SIGKILL** (durable, the default) | **Brief best-effort loss** (async WAL lag) | At-least-once | Respawns on a new executor, restores committed Segment state via the restore-before-run gate, and resumes **at the first un-journaled operation**: the op-journal (`op.journal`, one gated `OpCommit` fsync per op barrier) records each completed operation + its inter-op handoff, so committed ops are **skipped, not redone**, and the next op is seeded from the journaled handoff (Rust SDK; Java keeps the pre-journal full re-run until its parity change). Inspect with `flz state journal`. See [`bounded_operative.md`](bounded_operative.md). |
| **Looping bounded operative executor SIGKILL** (durable) | **Brief best-effort loss** (async WAL lag) | At-least-once | Resumes at the iteration **after** the last journaled `iter_committed` — committed turns never re-run, and the in-flight one re-runs from its own last committed op. Input it had not consumed is still in the durable inbox. See [`bounded_operative.md`](bounded_operative.md) "Durable loop and suspension". |
| **Input sent to a suspended / crashed / migrating bounded instance** (durable) | **No loss** | n/a | The controller appends it to the instance's `inbox.wal` before forwarding, so it is delivered on rehydrate rather than dropped — and a suspended instance is *woken* by the append. Duplicates inside the undelivered window are absorbed. See "Bounded operatives: the durable inbox". |
| **Controller redeploy while a bounded instance is suspended** | **No loss** | n/a | Suspension is durable state (`parked` journal entry + inbox + snapshots), not a live process: the instance is rediscovered from disk on boot, stays evicted, and rehydrates on the next input or its wake deadline exactly as if nothing happened. |
| **Bounded operative executor SIGKILL** (persistence opted out) | **Full — re-runs from the top** | At-least-once | No WAL to restore from; the operative re-runs its `run`/chain from `args`, and any input in its memory-only inbox is lost. Idempotent by author contract, or don't opt out. |
| **Bounded operative SIGKILL mid-compensation** (durable) | Compensation resumes | n/a | Compensation-intent / -done markers ride the restore; the respawn finishes the outstanding undo hooks and **skips** the already-done ones (no forward re-run). |
| **Poison payload** (operation fails on every retry) | **No loss** | Segment continues | Retry-then-park: after `retry_max` attempts the payload is durably parked to the DLQ store *before* the segment advances (tier off: fsync-then-ack; tier on: one atomic record with the consumed offset). Inspect/replay/drop via `flz dlq`; only retention (30 d / 4 GiB, loud) ever discards a parked payload. See "Dead-letter queue" below. |

## Why output can briefly get ahead of state (the hard-kill gap)

The executor streams **output frames** and **state-change frames** to the controller
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
`State.FsyncBatcher` coalesces dirty-mark casts from all per-segment `WalWriter`
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
`flz.segment` (emitting Segment), `flz.seq` (`<change_seq>.<emit_ordinal>`,
derived from the durable WAL seq), and `flz.key` (the natural key supplied
via `ctx.emit_keyed(key, value)`). Because the stamps derive only from
replay-stable inputs, a crash-restore replay re-emits frames with
**identical** stamps, so a downstream consumer that upserts on `flz.key`
(candles, indicators — any changelog-shaped stream) absorbs duplicates
exactly. The determinism contract this rests on — no ambient time, UUIDs,
or external reads in `run`/`apply_change`/`compensate`; use `ctx.now()`
and `ctx.stable_id()` instead — is documented on the `Operation` trait and
verified by `OperationTestHarness::replay_emits_equal`. In the default
tier `flz.seq` is **not** an exact append-only dedup handle
(no-state-change invocations can collide on it).

**Effectively-once tier (opt-in, `effectively_once()` on the operative
builder).** The built-in upgrade of *at-least-once delivery + deterministic
key + idempotent sink = effectively-once*:

- **Durable input-log.** The controller journals every inbound payload
  (post-`:on_input`) into a per-operative `input.log` before forwarding,
  stamping the offset as `flz.offset`. Retention truncates only below the
  minimum durable consumed-offset across the operative's Segments; a
  frozen watermark past the size threshold emits
  `[:flezha, :state, :input_log_frozen_watermark]` instead of silently
  accumulating.
- **Atomic effect record.** In tier mode plugin emits leave on the
  reliable state sink *inside* the invocation's `segment_effect_record` —
  state change(s), outputs, and consumed offset are ONE WAL record under
  ONE fsync. No partial combination is ever durable, and effect records
  are never coalesced away.
- **Fsync-gated outbox flush.** Outputs reach the `:on_output` plugin
  chain only after their record is fsynced, stamped with the durable
  identity `(flz.segment, outbox_seq = flz.seq, flz.epoch)` — now a
  sufficient handle for exact append-only dedup. On restart the controller
  re-drives every entry above the per-Segment `outbox_flushed_seq` cursor
  in order with identical identity: outputs are **never lost, never
  applied twice at a conforming sink** (Kafka: message key = `flz.key`,
  header `flezha.dedup_key` — see `docs/kafka-producer.md` for the
  changelog-vs-append contract).
- **Postgres sink (built-in conforming sink).** The `operative_postgres`
  `io_binding` plugin (`Flezha.Plugin.Provider.Postgres`) makes the tier
  effectively-once *into the database*: per flush batch ONE transaction
  upserts every record into `flezha_records` (keyed by `flz.key`, falling
  back to the durable identity for keyless tier outputs) and advances the
  per-Segment `flezha_sink_progress` watermark row — data and progress can
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
  socket), bounding input-log growth against a stalled Segment.
- **Cost-free default.** Operatives without the flag keep every path
  byte-identical — no input-log writes, no outbox, no gating. Slow or
  unreachable sinks back the outbox up on disk (never memory) and delay
  outputs; data is never dropped — the correct failure mode for the tier.
- **Bounded operatives** opt in with the same per-record semantics: each
  op's changes + keyed emits form one atomic effect record, and controller
  re-drives carry identical identity. Without the tier, a durable
  structured bounded gets the op-boundary guarantee below instead.

Non-keyed channels are byte-identical to before and remain plain
at-least-once. See `docs/sdk_protocol_spec.md` "Output identity stamps"
and "segment_effect_record".

## Bounded operatives: staged commit and the transactional outbox

A **durable structured** bounded operative (operations declared, the
default `PERSISTENCE_ENABLED`) never lets an operation's plugin output
escape before that operation is durable.

- **Staged, not emitted.** Inside a structured operation `ctx.emit` does
  not reach the wire: each segment buffers its serialized plugin-bound
  output. A failed, aborted, or cancelled operation therefore leaks
  **nothing** — the staged bytes die with the run. (The `run`-sugar form
  has no operations, hence no barrier to stage against and no journal
  entry to gate a flush on; it keeps the direct at-least-once wire path,
  as do opted-out classes.)
- **One durable write.** At the barrier the staged outbox travels to the
  controller inside the `OpCommit` request and is journaled with the
  operation's completion in ONE `op_committed` record in the instance's
  `op.journal` (fsynced before the ack). Progress and output cannot tear
  apart.
- **Flush after the durable point.** Only once that fsync lands does the
  controller deliver the entries — in journal order, through the same
  plugin fan-out live bounded output uses — then record an
  `outbox_flushed` entry. A delivery that cannot happen yet (operative not
  bound) is retried; entries are never silently dropped. Compute progress
  is not coupled to sink health: the next operation runs while the flush
  lands, because the chain hands data forward through the handoff, never
  through the sink.
- **Crash before flush re-drives.** On restore, every `op_committed` (and
  `compensated`) entry with no matching `outbox_flushed` is re-delivered
  with its **original keys** before the resumed run advances. An op whose
  flush IS recorded is never re-delivered.
- **Keys.** Each flushed frame carries `flz.segment`, `flz.seq`
  (`<op_idx>.<seq_in_op>`) and `flz.epoch` — the effectively-once output
  identity shape, minted from the journal, so a keyed sink adapter applies
  unchanged. The epoch is `0` at first start and stable across crash
  re-drives (only a future rewind bumps it).
- **Compensation undo-effects ride the same path.** A `compensate` hook's
  plugin output is staged per segment, journaled in a `compensated` entry,
  and flushed after that fsync with a comp-marked key (a trailing `.1` seq
  component), so an undo can never collide with the forward output it
  reverses. The per-segment intent/done markers still decide which hooks
  run on resume.

**Guarantee.** Never lost, never leaked before commit — and **at-least-once
with stable keys** on the re-drive window (the overlap between a delivery
and its unwritten `outbox_flushed` marker). A dedup-capable sink absorbs
that duplicate exactly; without one, a re-driven output can appear twice.
Inspect what an instance owes with `flz state journal <instance>`.

## Bounded operatives: the durable inbox

The **input** half of the same story. A durable bounded instance's post-start
input (a chat turn, an agent message) is appended to a per-instance
`inbox.wal` — the same `WalWriter` + `FsyncBatcher` substrate as `op.journal`,
living beside it under `<state_root>/<instance>/<instance>/` — **before** it is
forwarded to the executor:

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
  `op.journal` — the two files have deliberately different retention.

**Guarantee.** Input sent to a suspended, crashed, or migrating durable bounded
instance is **never lost**: it is durable before it is forwarded and delivered
when the instance next runs. Inspect with `flz state inbox <instance>`. An
opted-out class keeps the memory-only inbox (input in flight at a crash is
lost — that is what opting out buys). See
[`bounded_operative.md`](bounded_operative.md) "Durable loop and suspension".

## Dead-letter queue: poison payloads park, never drop

A permanently-failing (poison) payload in an Unbounded operative is never
silently discarded (`platform-dead-letter-queue`). A failing invocation —
returned error or panic/exception, same policy in both SDKs — is retried
up to the input channel's `retry_max` (default 3 total attempts,
exponential backoff + jitter; authors classify errors non-retryable via
`OperationError::non_retryable` / `OperationException.NonRetryable` to
park immediately). After exhaustion the payload is **durably parked** to
the controller's DLQ store (`app_data/dlq/<operative_class>/<operation_id>/`,
JSONL index + payload blobs) and only then does the segment advance:

- **Tier off:** the SDK ships a `park_payload` frame and holds the
  segment until the controller fsyncs the entry and acks
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

**Bounded equivalence:** the DLQ applies to Unbounded operatives only. A
permanently-failing Bounded segment already has its escape — the
operation fails, AbortAll fires, and saga compensation unwinds — so
nothing is written to the DLQ store; configurable `on_segment_failure`
policies (DeadLetter | MinSuccess) remain deferred.

## How to avoid loss — operator playbook

- **Deploy / upgrade executors with a graceful stop, never a kill.** Send SIGTERM (or
  `flz`-driven `disconnect`) and set the orchestrator grace period
  (`terminationGracePeriodSeconds` / `stop_grace_period`) **longer than the drain**
  (serialize + ack; default ack budget 2 s/operative, serialize cap 10 s). Roll one node at
  a time so a peer is always live.
- **Restart the controller correctly:**
  - Multi-node / rolling: bring the new instance up *before* killing the old → peer sync,
    zero loss.
  - Sole instance that must keep its operatives: use **recovery mode** (remove the
    `cluster.healthy` marker / `flz cluster prepare-recovery`) → replays from DETS.
  - Plain restart of a sole instance is "accept the operative-list rebuild window" — only do
    it when that's acceptable.
- **Make sinks idempotent where exactly-once matters.** Output is at-least-once by
  default and can be re-emitted after a rebind/restore — use Kafka keys/transactions,
  HTTP idempotency keys, or DB upserts downstream. Declare the channel `.keyed()` so
  re-emits carry identical `flz.key`/`flz.seq` stamps to upsert on — or opt the
  operative into `effectively_once()` for the built-in tier (see "Delivery semantics"
  above; the built-in Kafka and Postgres sink plugins are conforming out of the box).
- **Never treat DETS as the source of truth** in code or ops — it is a cold-start backup
  only (see `docs/resilience/process_hub_recovery_model.md`).
- **Bounded operatives are durable + at-least-once by default.** A crash re-runs the
  operation chain over restored state, so author each operation (and compensation)
  **idempotently**, or **opt the class out of persistence** for a fire-and-forget job that
  must re-run from a clean slate. See [`bounded_operative.md`](bounded_operative.md).

## Common pitfalls

- `docker kill` / `kill -9` / `terminationGracePeriodSeconds` too short on an executor →
  drain is cut off → best-effort loss. Use graceful stop + adequate grace.
- **The executor binary must receive SIGTERM directly.** Run it as PID 1 (exec-form
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
