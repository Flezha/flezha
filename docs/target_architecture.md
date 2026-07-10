# Target architecture — durable-compute kernel

One map from the agreed target-state architecture of the operative model to the
OpenSpec changes that deliver it. Read this before proposing work that touches
durability, output guarantees, flow control, or health: the invariants below are
settled, and each one has exactly one owning change or spec.

Vocabulary: `docs/glossary.md`. Profiles: **Unbounded** (infinite stream,
per-segment recovery, low latency) and **Bounded** (op-chain, journal +
snapshot, saga, durable suspension) — two profiles over one shared kernel.

## Kernel invariants

1. **Source and sink are pluggable edge adapters.** The core assumes no
   concrete external system; Kafka is one optional sink example, not a source
   and not a system component. The internal model (input-log, WAL, outbox,
   watermark) is adapter-independent.

2. **One atomic durable record.** Each processed input produces a single
   crc-protected WAL record containing `{state change, outbox entries
   (channel, key, metadata, payload, emit ordinal), consumed input offset}`,
   appended and fsynced as one unit. State and emitted effect can never tear.
   Sink flush happens only after that record is durable (gated on fsync).

3. **The durable input-log is opt-in** (the effectively-once guarantee tier,
   default OFF). Tier off is byte-identical to today's wire behaviour and
   writes no input-log file. Tier on: every source is normalized into one
   offset-addressable durable log; each Segment recovers from its own consumed
   offset.

4. **Output identity and the epoch rule.** Replay-stable output identity is
   `(flz.segment, flz.seq, flz.key)`; durable outbox identity is
   `(segment, outbox_seq, epoch)`. Crash-retry and takeover KEEP the epoch —
   sink-side dedup absorbs re-emits. Only deliberate rewind / correction /
   replay BUMPS the epoch, so corrected outputs pass dedup and overwrite the
   stale changelog row by natural key.

5. **Fencing lives at the connection change, never per write.** Two-layer
   executor identity with newest-wins takeover; a superseded session is
   refused at the door and its channels closed. No per-write fencing token —
   the WAL binds to the owner the door assigned, and realign is honoured only
   for the current owner.

6. **Flow control is credit-based: park, never drop.** Executor→controller
   byte-credit is granted on WAL *fsync* (durable drain), not append, with
   low/high watermarks; a sender out of credit parks until the next grant.
   The hard-watermark shed path remains only as a last-resort safety net
   against a non-cooperating sender — under credit it is an anomaly signal,
   not a load response. With the effectively-once tier on, a second credit
   loop gates source ingestion on the min-watermark across the operative's
   Segments, so the slowest branch backpressures the source instead of
   growing the input-log to disk-full.

7. **Min-watermark is the retention signal.** The controller tracks
   `min(consumed offset)` across Segments and truncates the input-log below
   it (subject to the correction hold window). A permanently-failing payload
   must never freeze the watermark: after bounded retries the Segment durably
   parks the payload to a dead-letter store and advances its offset — data is
   never lost, and never silently dropped.

8. **Health is producing-liveness, not channel-liveness.** `/healthz` and
   convergence classification answer "is work flowing" — watermark / WAL seq
   advancing, outbox draining — with an "is it being fed?" precondition so
   IDLE (no input, flat is fine) is distinguished from STUCK (fed but not
   advancing). An empty intended set is never vacuously healthy.

## Where each invariant lives

| Invariant | Owning change / spec |
| --- | --- |
| Atomic record, gated flush, outbox cursor | `effectively-once-output` (modifies `operative-state-persistence`) |
| Opt-in durable input-log, min-watermark retention | `effectively-once-output` (`durable-input-log` capability) |
| Deterministic effects, `(flz.segment, flz.seq, flz.key)` identity | `deterministic-effects-and-output-identity` |
| Epoch bump on deliberate rewind/correction only | `operative-rewind-and-correction` |
| Durable snapshot baseline (restore floor) | `snapshot-baseline-durability` |
| Fencing at the door, newest-wins takeover | `executor-identity-and-session` spec |
| Owner-bound WAL realign (stale-writer refusal) | `wal-owner-fencing` |
| Credit-grant flow control | `credit-grant-flow-control` — executor→controller loop **shipped** (handshake, fsync-minted batched grants, park semantics in both SDKs, soak-proven vs the 06-21 load); the source-ingestion loop awaits `effectively-once-output`'s input-log + min-watermark |
| Poison-payload dead-letter (Unbounded) | `platform-dead-letter-queue` (umbrella slot 4) |
| Producing-liveness health | `progress-liveness-health` (modifies `convergence-self-healing`) — **shipped**: WAL-marker IDLE/STUCK classification with the fed precondition, intent-mismatch (durable evidence vs empty intent) degrading `/healthz`, `stuck_operatives`/`intent_mismatch` detail in the body; watermark/outbox marker inputs activate when `effectively-once-output` lands |
| Bounded profile: saga, compensation, crash-resume | `bounded-operative-*` specs (archived, shipped) |
| Controller→executor send-buffer bound | `controller-send-buffer-bound` spec |

Apply order for the effectively-once track:
`deterministic-effects-and-output-identity` → `effectively-once-output` →
`operative-rewind-and-correction`, with `snapshot-baseline-durability` applied
before rewind.

## Incident lineage the gap changes close

- **2026-06-21 controller OOM** (unbounded WAL mailboxes, 290 MB → 1.88 GB →
  OOMKilled at ~1100 msg/s): closed structurally by credit-grant flow control —
  the append buffer stays bounded because credit reflects real fsync
  throughput.
- **~30 h silent outage** (registry wipe → intended==bound==0 → `/healthz`
  reported healthy): closed by producing-liveness health — expected==0 is
  never vacuously healthy, and health tracks watermark/outbox progress.
- **Silent poison drop** (a panicking operation is caught and its payload
  discarded with only a log line): closed by the dead-letter queue — park
  durably, advance the offset, inspect/replay/drop via CLI. Bounded already
  covers this class via AbortAll + saga compensation.

## Deferred (benchmark-gated or later round)

- Rust-side durable I/O and wire coalescing: hypothesis only; adopt solely on
  a proven benchmark win (`docs/benchmarking.md` harness), never for
  reliability — bounding the BEAM path already closes the OOM class.
- Kernel-level hibernation (durable-evict for both profiles), per-operative
  durability-mode knob, incremental snapshots, turnkey idempotent sink
  adapters: future round, opt-in, not drawn into the current change set.
- `effectively-once-checkpoint-granularity` (parking-lot proposal): opt-in
  checkpoint ack-granularity for the effectively-once tier — consumed-offset
  advances only at explicit checkpoint boundaries, recovery replays the interval;
  future round, not in the current apply order.
