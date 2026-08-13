# Flezha Java SDK — worked examples

Eleven examples, in the order worth reading them. Each is a real compiled source
file in this module, not a snippet — the reactor builds them, so an example that
drifts from the SDK **breaks the build** instead of quietly lying to you.

```
mvn -f sdk/java/pom.xml test -pl flezha-sdk-examples -am
```

| # | File | What it teaches |
|---|---|---|
| 01 | [`E01_Echo`](src/main/java/com/flezha/examples/E01_Echo.java) | The three pieces: Procedure, Operative, registration. Stateless. |
| 02 | [`E02_StatefulCounter`](src/main/java/com/flezha/examples/E02_StatefulCounter.java) | Durable state via Changes — and why you never assign to a field. |
| 03 | [`E03_EffectivelyOnce`](src/main/java/com/flezha/examples/E03_EffectivelyOnce.java) | Effectively-once output: keyed emits, checkpointed intervals. |
| 04 | [`E04_LateInputCorrection`](src/main/java/com/flezha/examples/E04_LateInputCorrection.java) | Repairing the past when an input arrives late. |
| 05 | [`E05_WorkflowSaga`](src/main/java/com/flezha/examples/E05_WorkflowSaga.java) | A workflow operative: resumable, compensating transaction. |
| 06 | [`E06_DurableChatLoop`](src/main/java/com/flezha/examples/E06_DurableChatLoop.java) | A long-lived agent: one durable turn per message, suspended in between. |
| 07 | [`E07_TestingYourProcedureTest`](src/test/java/com/flezha/examples/E07_TestingYourProcedureTest.java) | Testing a Procedure — and proving it is replay-safe. |
| 08 | [`E08_SpawnWorkflowWorker`](src/main/java/com/flezha/examples/E08_SpawnWorkflowWorker.java) | Asking the cluster for a workflow worker from inside a Procedure. |
| 09 | [`E09_MergedInput`](src/main/java/com/flezha/examples/E09_MergedInput.java) | One Procedure over two Channels: a data feed plus a live config feed. |
| 10 | [`E10_StreamTransforms`](src/main/java/com/flezha/examples/E10_StreamTransforms.java) | `filter` → `flatMap` → `keyBy` on the wiring: one trade, four timeframes, four Entities, one Procedure. |
| 11 | [`E11_LiveStateRead`](src/main/java/com/flezha/examples/E11_LiveStateRead.java) | Reading one Entity's current value with `flz state get` — and what its `change_seq` promises. |

## The one idea worth internalising first

Everything durable in Flezha rests on **replay**. State is rebuilt by re-applying
a change log; a correction re-runs `run` outright; a crashed workflow step is
re-attempted.

So `run` must be a **pure function of its inputs**. A Procedure that reads
`System.currentTimeMillis()` or `UUID.randomUUID()` will produce a *different*
result when replayed than when it first ran — and nothing will tell you. The
state silently diverges after a crash; the "idempotent" upsert writes a second
row under a fresh key.

The SDK gives you the two escapes, and the two gates:

| Instead of | Use | Why |
|---|---|---|
| `System.currentTimeMillis()` | `ctx.now()` | Evaluated once per invocation; a replay serves the same value. |
| `UUID.randomUUID()` | `ctx.stableId()` | Derived from entity + seq + call ordinal; a replay derives the same id. |

| Gate | Proves |
|---|---|
| `harness.replayEquals()` | Replaying the change log rebuilds the same **state**. |
| `harness.replayEmitsEqual(inputs)` | Re-running produces the same **output**. |

Both. A Procedure can rebuild identical state while emitting a fresh UUID each
run — and a keyed sink's idempotence depends on exactly that not happening.
Example 07 shows both, and they are cheap enough to run on every Procedure you
write.

## Which shape do I want?

**Stream** — a long-running dataflow over a stream. Starts with the runner,
never finishes. (01–04)

**Workflow** — started with arguments, runs to completion, torn down. A checkout,
a job, a conversation. (05–06, and 08 spawns one from inside a stream
Procedure).

Then, for a stream operative, work down this list and stop at the first "no":

| Question | If yes |
|---|---|
| Does it need to survive a restart with its state intact? | Declare a `Change` type → **02** |
| Would a crash between "state written" and "output sent" be a correctness bug? | `c.effectivelyOnce()` → **03** |
| Is the state only coherent at a boundary (a closed window, a finished batch)? | `c.checkpointAck()` → **03** |
| Do inputs arrive out of order, and does that corrupt earlier results? | `c.correctionWindow(ms)` → **04** |
| Does it need a second feed — live config, an enrichment stream, a join? | `c.merge(I.class)` → **09** |
| Does something outside the pipeline need one Entity's current value? | nothing to declare — `flz state get` → **11** |

Each of those is a declaration on the topology, and each is a promise the
controller has to keep — so declare the ones you need, not the ones that sound
reassuring. A `correctionWindow` sizes the controller's input-log retention; a
tier without keyed sinks is refused outright.

## Things the compiler will tell you, so you don't have to remember them

Java's type erasure means the SDK cannot enforce at compile time what the Rust
SDK does. It enforces the same rules at **registration** instead — at process
start, before a single input is served, with the same error names:

- `EventTimeFrom` without an `eventTime(Input)` override → refused.
- `LogicalClock` with `Change = NoChange` → `UnservableClock` (there is no seq to serve).
- `.keyed()` on a channel with no plugin wire → refused (a key has nowhere to ride).
- `effectivelyOnce()` with an unkeyed plugin channel → refused (a replay would duplicate).
- `checkpointAck()` without `effectivelyOnce()` → `CheckpointAckWithoutTier`.
- A durable workflow chain with a codec-less inter-proc channel → `MissingInterProcCodec`.
- A loop on `run`-sugar → `LoopWithoutChain` (there is no chain to iterate).
- A `merge(...)` with fewer than two sources, the same Channel twice, or a source
  re-wiring an already-declared edge → `MergeTooFewSources` /
  `DuplicateMergeSource` / `EdgeAlreadyWired`.
- A **workflow** merge over an entry Channel → `MergeFromEntryChannel` (the
  spawn-args seed names no Channel; merge its procedures' output streams).

If your operative starts, these all held.
