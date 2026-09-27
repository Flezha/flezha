# Multi-input tasklets (merge)

For operator authors who need one Tasklet fed from two or more Channels —
a data stream plus a config stream, a join of two computed streams, an
enrichment path — and for SDK implementers: the `merge` surface every SDK
carries, shown here in Rust and Java, what it does and does not promise,
containment of a failing mapping, the refusals, the bounded operator restriction, and
where the code lives.

A Tasklet consumes one `Input` type, and `.into::<Task>()` type-locks it to
one Channel. A **merge** declares the `Input` as an author-defined sum type
and subscribes two or more source Channels to it, each with a mapping into
that type:

```rust
c.merge::<DetectorInput>()
    .from(c.input::<Reading>("readings"), DetectorInput::Reading)
    .from(c.input::<Threshold>("config"), DetectorInput::Threshold)
    .into::<Detector>();
```

```java
c.merge(DetectorInput.class)
    .from(c.input(Reading.class, "readings"), DetectorInput::ofReading)
    .from(c.input(Threshold.class, "config"), DetectorInput::ofThreshold)
    .into(DetectorTasklet.class);
```

The merge is a **subscription-time** construct: `Tasklet` is untouched, so
dispatch, state, `cells_for`, retry/DLQ and the emit APIs all see one type.
Full examples: `sdk/rust/examples/merged_input.rs`, Java `MergedInputTest` in `flezha-sdk-examples`.

## What a merge promises

- **Sources are entry or inter-tasklet Channels, freely mixed** on an unbounded operator
  operator; a bounded operator merge takes inter-tasklet sources only. Mapping an
  entry Channel directly removes the parser-Tasklet detour for a low-rate
  control channel; the detour is still right for a hot path with its own
  state.
- **A Rust source payload must be `Clone`** (`MergeSource::Payload: Clone`):
  the mapping clones the value, where a plain `.into` forwards the `Arc`. Java
  has no such requirement.
- **Per-source FIFO is preserved; nothing is ordered across sources.** There
  are no windowed joins, buffering or alignment primitives; correlation is the
  Tasklet's own state's job.
- **`cells_for` and the key see the MAPPED value.** Keying is declared
  **once, on the merge** (`Merge::key_by` / `Merge.keyBy`); a key on a source
  handle is refused as `TransformAfterKeyBy`, since that source's value has
  already become the sum type by the time keying runs.
- **A source handle may carry its own transforms**, which run before the
  mapping on the source's payload type; transforms on the merge run after, on
  the sum type. All of it is one composed closure on the edge
  ([`stream_transforms.md`](stream_transforms.md)).
- **Mappings MUST be pure and total.** They run on live delivery and again on
  WAL replay, and the two must agree. A mapping returns `I`, not
  `Result<I, _>` — it has no error channel, so fallible work belongs in the
  Tasklet, on the sum type, where it has somewhere to fail to.

A panicking mapping is contained:

| Where the mapping sits | What a panic does |
| --- | --- |
| stream, either source kind | Caught at the routing seam (`ConsumerEdge::transform_value`); the value climbs the source Channel's retry→DLQ ladder, the one a failing entry parse rides. On Java the mapping runs inside an activation, so a throw is that activation's failure and rides its ladder. |
| bounded operator, forward accumulate | Runs on the worker that produced the value, at the end of the producing item, inside the routing seam (`deliver_edge`); contained there and turned into a non-retryable failure of the producing stage. The producing Cell is recorded as having run, the saga unwinds the stages **recorded as completed**, and the instance still reports `BoundedCompleted` with status `error`. Not retried — a pure mapping would panic identically. |
| bounded operator, crash-resume re-seed | Contained the same way. The resumed iteration seeds its completed list from the journal (`journaled_stages`), so a producer the journal already committed is unwound even though this pass never re-ran it. |

On the forward site the mapping runs after its producer's intent markers,
stage record and task-commit barrier, so **the producer whose edge blew up is
always in the undo plan**. A sibling still in flight in the same level is not:
Rust runs a level in parallel and the abort returns at the first failing
edge, the ordinary mid-level abort shape. Both SDKs run the core's single
driver and unwind the same tasklets in the same order.

## Why replay is free

The mapping is applied at the **routing seam** (`route_to_consumers`
in-process; host-side for Java), the one path live delivery and the
controller's re-forwarded replay slice both travel. Frames are journaled per
source Channel, unchanged, so the replay slice orders and folds exactly as the
live run did and a warm restart restores the same state. No wire change, no
controller change: the controller stamps `metadata["channel"]` on a frame and
has no model of a Channel's consumers, so a merge is invisible to it.

## Refusals

Definition-time, alongside every other topology issue:

| Refusal | Rust | Java |
| --- | --- | --- |
| fewer than two sources | `TopologyIssue::MergeTooFewSources` | `MergeTooFewSources` in the accumulated `UnboundedOperatorConfigurationException` |
| same Channel subscribed twice | `TopologyIssue::DuplicateMergeSource` (names the Channel) | `DuplicateMergeSource` (names the Channel) |
| source re-wiring an edge `.into` or another merge already declared | `TopologyIssue::EdgeAlreadyWired` (names Channel + Tasklet) | `EdgeAlreadyWired` (names both) |
| mapped type ≠ the Tasklet's `Input` | compile error at `.into::<Task>()` (`Tasklet<Input = I>` bound) | registration error naming both types (`wireConsumer` compares the declared merge type) |

A duplicated source reports only `DuplicateMergeSource`: the arity check
counts every `.from` written, not surviving sources.

## Bounded operators

The mapping is applied where the driver accumulates a producer's released
batch into its child and again where a crash-resume re-seeds that child from
the journaled handoff, so a resumed merge sees exactly what the live one did.
An **entry** Channel is a source on a bounded operator as on an unbounded operator: every message
carries the Channel it arrived on, so its mapping is selected the same way. A
merged tasklet fed by an entry Channel is a root of that Channel's chain, and
a merge of two entry Channels runs the merged tasklet once per message, on
the message's mapping, in the pass that message starts; a join where two
chains meet waits only for the producers of the chain that runs.

## Where the code lives

| Piece | Rust | Java |
| --- | --- | --- |
| `ChannelBuilder::merge`, `Merge`, `MergeSource`, the `TopologyIssue` variants; `attach_consumer` is the one place an edge is recorded and a second declaration on a mapped edge is refused | `sdk/rust/src/channel/builder/` | `flezha-sdk-api/.../channel/Merge.java`; `flezha-sdk-runtime/.../operator/builder/UnboundedOperatorTopologyBuilder.java` (`MergeImpl`, `wireConsumer`) |
| `ConsumerEdge` (`tasklet` + composed `transform` + optional `key`); `consumer_ids()` is the id-only view the graph walk and the bounded operator DAG use | `flz_core/src/channel.rs` | `flezha-sdk-runtime/.../operator/ConsumerEdge.java` |
| The one mapping site on the routing path | `flz_core/src/routing.rs` | `flezha-sdk-runtime/.../core/ActivationDispatcher.java` |
| `ChainCtx::edge` + `extend_mapped` at the accumulate and re-seed sites | `flz_core/src/bounded/{driver,spec}.rs` | shared core |

Tests: `sdk/rust/tests/merged_input.rs`; the bounded fixtures
`MergeBounded`, `PanickingMergeBounded` and `PanickingTransformBounded` in
`sdk/rust/src/bounded/mod.rs`, each run forward and resumed from a journal
so both mapping sites are exercised; Java `MergedInputTest`.

## See also

- [`stream_transforms.md`](stream_transforms.md) — the edge closure a merge mapping is one link of.
- [`rust_operator_architecture.md`](rust_operator_architecture.md) — the dispatch path and cell modes a merged Tasklet runs under.
- [`../architecture/bounded_profile.md`](../architecture/bounded_profile.md) — the stage barrier, abort and saga compensation a bounded operator merge rides.
