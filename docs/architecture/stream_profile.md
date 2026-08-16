# The stream profile

A stream operative is an endless stream: a live market feed, a click stream, a sensor
firehose. It has no beginning and no end, it processes message by message with no barrier, and
each of its entities recovers on its own without disturbing the others.

It runs on the [kernel](kernel.md) unchanged. What is specific to it is the shape of its
recovery, its escape hatch for a poison payload, its ability to correct the past, its ability
to acknowledge in batches rather than one input at a time, and its ability to run one logical
stream across N machines.

- [Happy path](#happy-path)
- [Per-entity recovery](#per-entity-recovery)
- [Dead-letter queue](#dead-letter-queue)
- [Checkpoint ack-granularity](#checkpoint-ack-granularity)
- [Late-input correction](#late-input-correction)
- [Partitioned operatives](#partitioned-operatives)

---

## Happy path

The same input, both modes, side by side.

```mermaid
sequenceDiagram
    participant SRC as 🌐 Source
    participant CTRL as 🧠 Controller
    participant EX as ⚙️ Runner entity (state in memory)
    participant SINK as 📤 Sink

    SRC->>CTRL: event
    alt effectively-once
        CTRL->>CTRL: 💾 append to the input log — offset N
        CTRL->>EX: input, stamped flz.offset = N
        EX->>EX: run, mutate state in memory
        EX->>CTRL: ONE effect record {changes, outputs, consumed_offset = N, event_time}
        CTRL->>CTRL: 💾 ONE append, ONE fsync
        Note over CTRL: DURABLE POINT
        CTRL->>SINK: flush the outputs, stamped (entity, seq, epoch, key)
        SINK->>SINK: dedup by key
        Note over CTRL,SINK: a crash re-emits the SAME identity — the sink absorbs it
    else at-least-once (the default)
        CTRL->>EX: input
        EX->>EX: run, mutate state in memory
        EX->>CTRL: state change (seq++)
        EX->>SINK: output leaves immediately, no fsync wait
        CTRL->>CTRL: 💾 append, fsync in the background
        Note over CTRL,SINK: a crash inside the batch window re-emits a duplicate —<br/>dedup downstream is the user's concern
    end
```

The tier trades one fsync batch of latency (5 ms) for the guarantee that state, output, and
progress move together. The default trades that guarantee for zero added latency. Neither is
"the right answer" — it is a per-operative declaration.

## Per-entity recovery

The blast radius of a runner crash is **one entity**. Each entity carries its own WAL, its
own snapshot lineage, and — under the tier — its own consumed offset, so it resumes on its own
without coordinating with anything.

```mermaid
sequenceDiagram
    participant EX as ⚙️ Runner (old)
    participant CTRL as 🧠 Controller
    participant EX2 as ⚙️ Runner (recovered)

    EX->>CTRL: effect records through offset 5000
    Note over EX: 💥 CRASH
    CTRL->>CTRL: the registry marks the operative unbound
    EX2->>CTRL: dial in — same stable identity, empty state
    Note over CTRL,EX2: takeover: unbind the old session, then bind the new one.<br/>A late frame from the old session is refused at the door AND at the WAL.
    CTRL-->>EX2: 💾 RestoreInitState — snapshot @ 4800 + every WAL record 4801..5000
    EX2->>EX2: load the snapshot, fold the change log — state is back at 5000
    CTRL->>EX2: re-forward the input log from the consumed offset + 1
    Note over EX2: ONLY this entity replays.<br/>Sibling entities and other branches never noticed.
```

Under the tier that re-forward is exact: the controller knows the last offset each entity
durably consumed, so it replays precisely the unconsumed tail. Without the tier there is no
input log, and recovery restores state only — the inputs that were in flight are gone.

That same re-forward path is reused for two other things. It is how a checkpoint interval is
replayed, and it is how a correction replays its rewound slice — there is only one replay
mechanism in the system, driven from three places.

## Dead-letter queue

A payload that makes user code fail *every* time is poison. Left alone it either wedges the
entity forever — freezing the minimum watermark, so retention stops and source credit dries up
— or it gets swallowed by a catch-all and disappears. Both are unacceptable. The rule is:
**retry, then park it durably, then advance.**

```mermaid
sequenceDiagram
    participant CTRL as 🧠 Controller
    participant EX as ⚙️ Runner entity
    participant DLQ as 💾 DLQ store

    CTRL->>EX: input (payload P, offset N)
    loop up to retry_max (default 3 attempts, exponential backoff + jitter, capped at 5 s)
        EX->>EX: procedure fails — an Err, or a panic
    end
    Note over EX: a ProcedureError::NonRetryable skips the ladder and parks at once.<br/>A panic stays retryable — the author never got to classify it.

    alt effectively-once
        EX->>CTRL: the DLQ entry rides INSIDE the effect record,<br/>alongside consumed_offset = N
        CTRL->>CTRL: 💾 ONE append, ONE fsync
        CTRL->>DLQ: materialize the entry (fsynced) — BEFORE the offset advances
        Note over CTRL,DLQ: no ack is sent, and none is needed:<br/>if the record never landed, input-log replay re-parks the payload
    else at-least-once
        EX->>CTRL: park_payload frame (bytes, entity, channel, error, attempts)
        Note over EX: the entity BLOCKS and re-sends every 2 s until acked —<br/>it must not advance past an unparked poison payload
        CTRL->>DLQ: 💾 append entries.jsonl + the payload blob, fsync
        CTRL-->>EX: ParkPayloadAck
    end

    EX->>EX: advance past N — the entity is not stuck
    Note over CTRL,DLQ: the minimum watermark moves → retention resumes, source credit is minted
```

The two paths differ in exactly one way, and it is the point of the tier: with the tier on, the
park and the offset advance are *the same durable record*, so they cannot come apart. With the
tier off they are two procedures, and the ack is what orders them.

Retry lives in the SDK, where the policy is declared. The park is durable on the **controller**,
because runners are disposable and their disks are not part of the durability contract.

**The store.** `app_data/dlq/<operative_class>/<procedure_id>/` holds an `entries.jsonl` plus one
blob per payload, fsynced before the ack. Entries are idempotent on their origin — `entity@offset`
under the tier, a content hash of the payload without it — so a re-park of the same poison is a
no-op rather than a pile of duplicates. Retention is **30 days AND 4 GiB**, whichever bites
first, and every retention deletion logs loudly: that data was never replayed and is now
permanently gone.

**The operator surface** is `flz dlq list | show | replay | drop`. A replay marks the entry
`replaying` and re-injects the payload as an ordinary new input; if no re-park arrives within the
grace window it is treated as a success and the entry is removed, and if it fails again it
re-parks with the generation bumped. Replay is deliberately an *operator* verb — an automatic
replay would simply re-poison the entity. An entry whose input was not byte-representable (an
inter-procedure value) carries no payload and can only be inspected or dropped.

Workflow operatives do not use the DLQ. Their equivalent escape is abort-and-compensate — see
[`workflow_profile.md`](workflow_profile.md#retry-then-abort).

## Checkpoint ack-granularity

The tier acknowledges an input by making its state durable. For most operatives that is exactly
right. But some fold *many* inputs into one meaningful state push — a window that only closes on
its boundary, a batch that only means something complete. For them, per-input acknowledgement is
not merely expensive, it is **incoherent**: the offset would advance with state the operative
never meant to publish, and a restore would resume past inputs the restored state never saw.

Such an operative declares `checkpoint_ack()` and calls `ctx.checkpoint()` where its state is
genuinely coherent.

```mermaid
sequenceDiagram
    participant CTRL as 🧠 Controller
    participant EX as ⚙️ Runner entity
    participant SINK as 📤 Sink

    Note over EX: --- an interval opens ---
    CTRL->>EX: input @ offset 100
    EX->>EX: apply changes → STAGED (held, not flushable on their own)
    CTRL->>EX: input @ offset 101
    EX->>EX: apply changes → STAGED
    EX->>CTRL: an emit ships in a record with NO offset and NO changes
    CTRL->>SINK: the output flushes as soon as THAT record is fsynced
    Note over CTRL: the consumed offset does NOT move. The watermark stands still —<br/>and standing still IS this operative's steady state.

    CTRL->>EX: input @ offset 102 — the window closes
    EX->>EX: ctx.checkpoint()
    EX->>CTRL: ONE record — the whole interval's staged changes + consumed_offset = 102
    CTRL->>CTRL: 💾 ONE append, ONE fsync
    Note over CTRL: state and offset advance together, or not at all

    Note over EX,CTRL: --- a crash mid-interval ---
    Note over EX: the restore DISCARDS the staged changes — they were never acknowledged
    CTRL->>EX: re-forward every input after offset 102 — the ordinary crash-resume path
    Note over EX,SINK: the interval's outputs are emitted again under the SAME epoch —<br/>the keyed sink the tier already requires absorbs them
```

Four consequences fall out of that, and they are the whole design:

- **Emits still flush as they happen.** Checkpointing moves the *acknowledgement*, not the
  output. Staging the outputs too would buy nothing — the interval's inputs are re-forwarded on
  replay either way, so the outputs are re-emitted either way.
- **A park closes the interval.** A poison input about to be acknowledged into the dead-letter
  queue must never be replayed back onto the procedure, so the park ships the staged state along
  with it. It is also the only correct choice: acknowledging the poison while the state before
  it is not durable would tear.
- **A restore discards the open interval.** Those changes belong to a past that no longer
  exists.
- **The dataflow-stop watchdog skips the operative.** "Produced before, silent since" is a dead
  feed for a normal class and a *normal interval* for this one.

**The interval is the knob you size the system with.** It is simultaneously the input-log
retention floor (truncation never crosses the last checkpoint), the recovery work after a crash,
and the granularity at which source credit is minted — grants arrive in bursts at checkpoint
cadence, and the source parks in between if it exhausts them. Checkpoint rarely and you pay in
disk and recovery time; checkpoint often and you converge on the per-input default. An operative
that *never* checkpoints stops truncating its input log and eventually parks its own source —
which is the same back-pressure any non-consuming entity gets.

`checkpoint_ack()` without `effectively_once()` is refused at build time: there is no durable
offset to hold back, and no durable log to replay from.

## Late-input correction

An event arrives late — after the window it belongs to has already been computed and published.
The state is wrong, and so is everything downstream of it.

The insight that makes this cheap: **there is nothing to splice**. The input was journaled at
its arrival offset *before* anything knew it was late, so the durable log already contains it.
Only the *order* was wrong. Correction is therefore the ordinary restore-and-replay path with
two changes — a rewound baseline, and an event-time-sorted slice. No new control message, no
correction journal, no crash marker.

```mermaid
sequenceDiagram
    participant SRC as 🌐 Source
    participant CTRL as 🧠 Controller
    participant EX as ⚙️ Runner entity
    participant SINK as 📤 Sink (changelog)

    SRC->>CTRL: a late event
    CTRL->>CTRL: 💾 input log append (offset N) — before forwarding, as always
    CTRL->>EX: forward, stamped flz.offset = N
    EX->>EX: extract the event time — behind the high-water mark, but inside the window
    EX->>CTRL: correction_request {entity, target_event_time}
    Note over EX: the input is NOT processed here.<br/>It is already durable in the log, and the replay will deliver it in its rightful place.

    Note over CTRL: coalesce per entity (200 ms debounce) — one replay, at the OLDEST target
    CTRL->>CTRL: 1 · baseline — scan the WAL FORWARD to the first record<br/>with event_time >= T, and keep the one before it
    CTRL->>CTRL: 2 · slice = (that record's consumed offset, the log's last offset]
    CTRL->>CTRL: 3 · 💾 rewind_to(baseline_seq, epoch + 1) — keep records at or below the baseline,<br/>drop only the snapshots above it, lower the input watermark. THIS is the commit point.
    CTRL->>EX: 4 · RestoreInitState — a BLOCKING wait. The runner clears its mailbox.
    CTRL->>EX: 5 · re-forward the slice, each marked flz.replay
    CTRL->>EX: 6 · flz.replay_end — a payload-less BROADCAST to every entity
    EX->>EX: buffer until the marker, sort by (event time, then offset), then dispatch
    EX->>CTRL: corrected effect records, under epoch + 1
    CTRL->>SINK: gated flush, epoch + 1
    SINK->>SINK: the higher epoch passes dedup and OVERWRITES the stale rows by natural key
    Note over EX: 7 · live inputs after the slice drain in arrival order again
```

Four details in there are load-bearing, and each was a bug in an earlier design:

- **The baseline scans forward, not backward.** Walking back to "the first record with an
  earlier event time" stops at an out-of-order straggler — a beyond-window input that was
  processed as an arrival — and leaves the records that overtook the late input in place,
  correcting nothing. Forward to the first record at or after the target, then keep the one
  before it, is the only correct baseline.
- **The rewind happens before the restore.** Reversed, the runner would append corrected
  records onto sequences the stale records still hold, and emit under the old epoch before the
  bump was durable.
- **`flz.replay_end` is a broadcast, not a mark on the last input.** An input routes to exactly
  one entity by key, so a marker riding the last one would strand the slice in every *other*
  entity's buffer forever.
- **The epoch is a parameter, not `current + 1` computed inside the writer.** That is what makes
  a crash-resumed correction idempotent instead of double-bumping — and a double bump would
  leave the first attempt's rows behind at an epoch nothing overwrites.

**A crash mid-correction needs no marker.** Before the rewind commits, nothing durable has
changed: the late input is still in the log, still unconsumed, so the ordinary resume re-forwards
it, the SDK re-classifies it as late, and we end up back here. After the rewind commits, the
on-disk state **is** the baseline and the watermark was lowered to match — so the ordinary
resume replay *is* the correction. Hence the rule: for a correction-declaring operative, **every**
journaled replay is marked and sorted. Crash recovery and deliberate correction share one path.

**An input beyond the window** is handled by a declared policy: `process_as_arrival` (the
default — today's behaviour, and declaring a window must never silently start dropping data) or
`drop`. A beyond-window arrival deliberately does **not** advance the event-time high-water mark.

### Declaring it, and what it refuses

Three preconditions are **authoring facts the engine cannot repair**, and all three are refused
at SDK build time:

| Refused | Why |
| --- | --- |
| `correction_window()` without `effectively_once()` | there is no durable input log to replay from |
| a procedure whose clock is not `EventTimeFrom` | "late" is meaningless without an event time carried by the input |
| a plugin channel that is not `.keyed()` | a corrected output must be able to overwrite the stale one by natural key |

**Retention is not one of them — it is derived.** A correction over a 60 s window *means*
holding 60 s of input log and 60 s of WAL history; that is the definition of the declaration,
not a policy to negotiate with an operator. So a declared window raises both retention floors to
meet itself:

- the **input-log hold window** becomes `max(configured, declared window)`, so truncation never
  removes an input the correction would replay;
- the **snapshot age floor** is never shorter than the window, so compaction always leaves a
  baseline the rewind can land on. (An age floor of `0` disables age-triggered compaction
  entirely — WAL history is then stream, which already satisfies any window, so `0` stays
  `0`.)

An operator who configures a *longer* floor keeps it. Because a one-line declaration can commit
the controller to real disk, a floor that had to be raised is logged and emitted as
`[:flezha, :correction, :retention_derived]` at bind, naming the floor and both values.

### Operator replay

The same machinery, with the baseline chosen by a human instead of by a late input:

```
flz operative replay <operative/procedure/key> --from <seq|timestamp>
```

Deploy a fix, then heal the recent past with it. It rewinds to the given point, replays the
retained window through the current code, and republishes under a bumped epoch — the keyed sink
overwrites the stale rows by natural key.

**One limit worth knowing.** A correction *overwrites* the keys it re-emits. It does not
**retract** a key that the stale run emitted and the corrected run does not. That is safe for
event-time-bucketed keys — a candle, an aggregate, anything whose key is derived from the window
it belongs to — because the corrected run emits the same key set. It is not safe for a key set
that shrinks. Details:
[`../resilience/data_durability.md`](../resilience/data_durability.md#late-input-correction-fix-the-past-and-the-sinks-fix-themselves).

## Partitioned operatives

A stream operative binds to exactly one runner, so entities fan out across one machine's cores
and no further. `--partitions N` is the way past that box: one logical stream materializes as
**N ordinary operative instances** (`ohlc#p0..#p2`), each a full, unmodified operative — its own
Overwatch, registry row, runner binding, input log, state tree, plugins, and DLQ identity.
There is **no group record**: each member's durable start opts self-describe (`group`,
`partition`, `partitions`, key-group range), and every group-level view — the single
`flz operative list` row, `info`'s coverage check — is recomputed from the rows at read time,
so nothing exists that could diverge from the registry. Placement, convergence, rebind,
migration and recovery all operate per member, unchanged.

```mermaid
flowchart TB
    SRC["🌐 Source — the ASK is split, never the data:<br/>each member subscribes upstream to only its items"]:::src
    subgraph GROUP["group &quot;ohlc&quot; — a DERIVED view over 3 ordinary instances"]
        direction LR
        P0["ohlc#p0<br/>key groups 0–85"]:::exec
        P1["ohlc#p1<br/>key groups 86–170"]:::exec
        P2["ohlc#p2<br/>key groups 171–255"]:::exec
    end
    HASH{{"group(key) = fnv1a128(key) mod 256<br/>ONE hash, THREE pinned implementations —<br/>Rust · Java · Elixir, one fixture set"}}:::ctrl
    FWD["operative_forward into the group:<br/>route each message by hash(flz.key)<br/>missing key → loud DLQ park, never round-robin"]:::ctrl
    GUARD["SDK guard at dispatch: a key outside the member's<br/>range parks on the FIRST frame, naming the contract —<br/>never silently wrong state"]:::prot
    D0[("💾 own WAL tree")]:::store
    D1[("💾 own WAL tree")]:::store
    D2[("💾 own WAL tree")]:::store

    SRC --> P0
    SRC --> P1
    SRC --> P2
    FWD --> HASH --> GROUP
    P0 --- GUARD
    P0 -.-> D0
    P1 -.-> D1
    P2 -.-> D2

    classDef ctrl fill:#dbeafe,stroke:#1e40af,color:#111
    classDef exec fill:#dcfce7,stroke:#166534,color:#111
    classDef src fill:#ede9fe,stroke:#6b21a8,color:#111
    classDef store fill:#e5e7eb,stroke:#374151,color:#111
    classDef prot fill:#dcfce7,stroke:#166534,color:#111
```

- **Key groups, not `mod N`.** Keys hash into 256 fixed key groups; each partition owns a
  contiguous range, stored explicitly in its opts and shipped on `AddOperative` — never derived
  from `(p, N)` at read time. A rescale therefore moves whole group slices instead of rehoming
  nearly every key, and a later weighted assignment needs no format change.
- **The routing contract is guarded, not trusted.** The item string a source subscribes with,
  and the `flz.key` a forward routes on, must equal what `key_by` extracts. The SDK checks every
  computed entity key against the member's own range and parks the first violation with an error
  naming the contract; a topology with no keyed entry procedure refuses the partitioned start
  outright (`PartitionsWithoutKeyedProcedure`) — N private copies of one unkeyed entity would be
  N divergent states that all look like "the" state.
- **Sources split the ask.** The websocket subscription source filters its desired item set to
  the member's groups before subscribing (server-side split — total bandwidth equals one
  connection's, divided); `http_poll` renders `{{partition_index}}`/`{{partition_count}}` into
  its URL once at init. `flz plugin enable <group>` fans out over the members with the partition
  opts injected.
- **Rescale is quiesce → move → restart.** `flz operative scale <group> M` waits until sources
  are off, every member's watermark is caught up and the group DLQ is empty; recomputes the
  ranges; moves each rehomed entity's durable file set (same-controller rename, cross-controller
  `WalTransfer` pull); starts M members, which restore through the ordinary bind. Entity
  identity contains the key and no runner, so no storage format changes — a crash mid-rescale
  re-derives the remaining moves from what is on disk.

A hot *single* key is still bounded by one machine — inherent to per-key ordering (Flink is
identical); the documented answer is two-stage aggregation. Partition spread across runners is
statistical (HRW) for now. The full contract — preflight, id budget, quota, coverage
validation, `flz state get <group>/...` resolution:
[`../partitioned_operatives.md`](../partitioned_operatives.md).
