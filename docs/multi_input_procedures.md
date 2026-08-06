# Multi-input procedures (merged subscriptions)

A Procedure consumes a single `Input` type. Wiring it with `.into::<Proc>()`
type-locks that `Input` to the source Channel's payload, so a single-input
subscription feeds a Procedure from exactly one Channel. That leaves anything
needing two feeds — a data stream plus a runtime-config stream, a join of two
computed streams, an enrichment path — with no wiring of its own: contort the
topology, or restart the Operative with new environment.

A **merge** declares the Procedure's `Input` as one author-defined sum type and
subscribes two or more source Channels to it, each subscription supplying a
mapping from its payload into that type.

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
    .into(DetectorProcedure.class);
```

The merge is purely a **subscription-time** construct: `Procedure` is untouched,
so dispatch, state, `entities_for`, retry/DLQ and the emit APIs all see one type.
Single-input `.into` is unchanged and remains the common case.

Full examples: `sdk/rust/examples/merged_input.rs`,
`sdk/java/flezha-sdk-examples/.../E09_MergedInput.java`.

## What a merge does and does not promise

- **Sources may be entry Channels or inter-procedure Channels**, freely mixed —
  on an unbounded Operative (see [Bounded operatives](#bounded-operatives) for
  the one restriction). An entry Channel maps like any other source, from
  whatever payload it declares (`Fn(T) -> I`, e.g. `Fn(Reading) -> I` in the
  shipped example), which removes the parser-Procedure detour for low-rate
  control channels — that detour is still available, and still right for hot
  paths that need their own state.
- **A merge source's payload must be `Clone` (Rust).** The mapping is handed the
  Channel's value by reference and clones it, so `MergeSource::Payload: Clone`;
  a plain `.into` forwards the existing `Arc` and imposes no such bound. Java
  has no equivalent requirement — its values are already references.
- **Per-source FIFO is preserved. Nothing is ordered across sources.** This is
  the same guarantee two independent consumers of two Channels already had.
  Correlation between feeds is the Procedure's own state's job; there are no
  windowed joins, buffering, or alignment primitives here.
- **`entities_for` and the merge's `key_by` are evaluated on the MAPPED
  value.** One routing function, total over the sum type. Per-source entity
  mode is something you encode in the enum, not a per-edge setting — which is
  why a merge declares its keying **once, on the merge** (`.key_by` /
  `.keyBy`), and declaring it on a source handle instead is refused: by the
  time keying runs, that source's value has already become the sum type.
- **A source handle may carry its own transforms.** `map` / `filter` /
  `flat_map` declared on a source run BEFORE that source's mapping, on the
  source's own payload type; transforms declared on the merge run after, on the
  sum type. Both live on the same edge as one composed closure — a merge
  mapping is simply one link of it (`stream-transforms`).
- **Mappings MUST be pure, and total.** They run on live delivery and again on
  WAL replay, and the two must agree — the purity half is the same contract
  entry parsers and output serializers carry, and a mapping that read the clock
  would restore a state the live run never had.

  The totality half is a mapping's own: it returns `I`, not `Result<I, _>`, so
  it has **no error channel**, where an entry parser returns a `Result` whose
  failure rides the Channel's retry→DLQ ladder. Do the fallible work in the
  Procedure, on the sum type, where it has somewhere to fail to.

  A mapping that *panics* is contained, and where it lands depends on the
  Operative and the source:

  | Where the mapping sits | What a panic does |
  | --- | --- |
  | unbounded, inter-procedure source | Runs under `ctx.emit`, inside the producing Procedure's invocation `catch_unwind`. Caught, attributed to that producer, rides **its** retry→DLQ ladder. |
  | unbounded, entry-Channel source | Runs on the wire-reader path (`runner/fast.rs` → `route_to_consumers`). `stream-transforms` closed the guard gap here: the edge closure is contained at the routing seam and the value climbs the Channel's own retry→DLQ ladder, the same one a failing entry **parse** rides. |
  | bounded, inter-procedure source — forward | Runs on the driver thread in `extend_mapped`, outside any entity invocation. Contained there and turned into a terminal chain error: the saga unwinds the stages **recorded as completed** at that point, and the instance still reports `BoundedCompleted` (status `error`). Both SDKs unwind the same procedures in the same order. |
  | bounded, inter-procedure source — crash-resume re-seed | Contained the same way and the instance still reports `error`. The **undo scope differs by SDK**: Rust seeds the resumed iteration's completed list from the journal, so the journaled producer is unwound; Java's resumed iteration does not, so nothing is unwound. See [Known limitation](#known-limitation-java-resumed-chains-do-not-compensate-journaled-procedures). |

  The bounded rows are a guard this capability had to add: an edge closure is the
  only author closure on a bounded chain that runs on the driver thread rather
  than inside an entity invocation, so without it an unwind would skip
  `emit_bounded_completed` and strand the instance with no terminal frame. It is
  not retried — a mapping is required to be pure, so a re-run would panic
  identically — and bounded has no DLQ; abort-and-compensate is its escape.

  "Recorded as completed" is the exact scope, and it is the ordinary mid-abort
  scope, not something the merge invented. On the **forward** site the mapping
  runs after its producer's intent markers, stage record and proc-commit barrier,
  so **the producer whose edge blew up is always in the undo plan**, on both
  SDKs. What is not in it is a procedure still in flight elsewhere in the same
  level: Rust runs a level in parallel and the abort returns at the first
  failing edge, so a sibling that ran alongside the producer has no stage record
  yet and is not compensated — exactly what a failing proc-commit on that same
  edge has always done. (A Java chain is sequential, so it has no in-flight
  sibling to lose.) The forward panic tests assert the undo list exactly: the
  same procedures in the same order on both arms — `["undo-b:b:a:x",
  "undo-a:a:x"]` in Rust and `[undo-b, undo-a]` in Java, the literals differing
  only because the two diamond fixtures format their log lines differently.

  On the **crash-resume re-seed** site the scope is not symmetric, and the
  asymmetry is not this capability's to fix — see below.

## Why replay is free

The mapping is applied at the **routing** seam
(`dispatcher::route_to_consumers` / `ProcedureDispatcher.routeToConsumers`),
which is the one path both live delivery and the controller's re-forwarded
replay slice travel. Frames are still journaled per source Channel, unchanged,
so:

- on-disk formats are untouched;
- the replay slice orders and folds exactly as the live run did;
- a warm restart of a merged Procedure restores the same state.

There is **no wire change and no controller change**. The controller routes a
frame to an operative and stamps `metadata["channel"]`; it has no model of a
Channel's consumers or their input types, so a merge is invisible to it.

## Validation

Definition-time, alongside every other topology issue:

| Refusal | Rust | Java |
| --- | --- | --- |
| fewer than two sources | `TopologyIssue::MergeTooFewSources` | `MergeTooFewSources` in the accumulated `UnboundedOperativeConfigurationException` |
| same Channel subscribed twice | `TopologyIssue::DuplicateMergeSource` (names the Channel) | `DuplicateMergeSource` (names the Channel) |
| source re-wiring an edge `.into` or another merge already declared | `TopologyIssue::EdgeAlreadyWired` (names Channel + Procedure) | `EdgeAlreadyWired` (names both) |
| an entry Channel as a **bounded** merge source | `BuildBoundedError::MergeFromEntryChannel` | `MergeFromEntryChannel` in the `BoundedOperativeConfigurationException` |
| mapped type ≠ the Procedure's `Input` | compile error at `.into::<Proc>()` (`Procedure<Input = I>` bound) | registration error naming both types |

The type check is the one asymmetry, and it is the established one: what Rust
rejects at compile time, Java's erasure forces to registration time. Equal
safety, different moment.

## Bounded operatives

A merge works in a bounded chain, over **inter-procedure** Channels: the mapping
is applied where the driver **accumulates** a producer's released batch into its
child, and again where a crash-resume re-seeds that child from the journaled
handoff, so a resumed merge sees exactly what the live one did. Both sites run
on the driver thread, so both contain a panicking mapping and turn it into a
terminal chain error and the instance still reports.

What the two sites do **not** share is the undo scope. On the forward site the
saga unwinds the stages recorded as completed — the producer whose edge blew up
among them, on both SDKs. On the crash-resume re-seed the SDKs diverge, for a
pre-existing reason that is not merge-specific: see the next section.

An **entry** Channel cannot be one of a bounded merge's sources, and that is
refused at registration (`BuildBoundedError::MergeFromEntryChannel` /
`MergeFromEntryChannel` in the Java exception). A bounded operative is seeded
from its spawn args — and, for a `PerInput` loop, from its inbox items — and
neither carries the Channel a payload arrived on, so there is nothing to select
a mapping by; a second entry Channel would additionally never be read. Merge the
output streams of two or more of the operative's procedures instead. (The
unbounded case is unaffected: there the controller stamps
`metadata["channel"]` on every frame, which is exactly what the bounded seed
lacks.)

### Known limitation: Java resumed chains do not compensate journaled procedures

`BoundedOperativeRunner.runIteration` declares its `completed` list empty and
`continue`s past every procedure the journal records as committed, so those
procedures are never added to it. Any abnormal exit from a **resumed** Java
bounded chain therefore compensates only the procedures that re-ran in that
pass; the ones the journal had already committed are left un-undone. Rust's
`drive_chain` seeds the same list from the journal and does unwind them.

This predates `multi-input-procedures` and is not specific to a merge — it
applies to a resumed Java chain failing anywhere forward. A merge mapping that
throws at the re-seed site is simply one way to reach it, which is why it is
recorded here. Observable symptom: a Java bounded instance that crashed
mid-chain, resumed, then failed reports `BoundedCompleted` with status `error`
having compensated fewer procedures than its Rust twin would. Documented, not
fixed: closing it changes bounded resume semantics, which this capability has
no mandate to change.

## Implementation map

- `sdk/rust/src/channel/mod.rs` — `ConsumerEdge` (`procedure` + optional
  `MergeMapFn`) replaces the bare consumer id on `ChannelDescriptor`.
- `sdk/rust/src/channel/builder.rs` — `ChannelBuilder::merge`, `Merge`,
  the `MergeSource` trait (implemented for `EntryStream` and `Stream`), and the
  three `TopologyIssue` variants. `attach_consumer` is the one place a
  channel→procedure edge is recorded, so it is also where a second declaration
  on an already-mapped edge is refused.
- `sdk/rust/src/bounded/spec.rs` — the bounded entry-source refusal, beside the
  existing single-root rule.
- `sdk/rust/src/operative/dispatcher.rs` — one mapping site, on the shared
  routing path.
- `sdk/rust/src/bounded/driver.rs` — `ChainCtx::edge_map` + `extend_mapped`, applied
  at the accumulate and journal re-seed sites, and the containment that keeps a
  panicking mapping from taking the driver thread with it.
- Java mirrors: `com.flezha.sdk.channel.Merge`,
  `runtime.operative.ConsumerEdge`, `UnboundedOperativeTopologyBuilder.MergeImpl`,
  `ProcedureDispatcher.routeToConsumers` (the one routing seam, shared by the
  wire reader and an inter-procedure emit) and `BoundedOperativeRunner` (whose
  `extendMapped` is the containment, rethrowing as `BoundedOperativeException` into
  the chain's existing abort path).

## Tests

- `sdk/rust/tests/merged_input.rs`
- `sdk/java/flezha-sdk-runtime/src/test/java/com/flezha/sdk/runtime/worker/MergedInputTest.java`

Mirrored scenario for scenario: merged delivery through typed inter-procedure
Channels (with the merged node's own output still fanning out), entry Channels
as sources, per-source FIFO under interleaving, a replayed slice mapped exactly
as live delivery mapped it, `entities_for` on the mapped value, and the
definition-time refusals.

The bounded halves are the diamond fan-in re-authored as a merge —
`MergeBounded` in the Rust `bounded_driver_scenarios` and
`MergeBoundedOperative` in Java's `BoundedKindTest` — each run forward and again
resumed from a journal, so both the accumulate and the re-seed mapping sites are
exercised. A third bounded fixture per SDK panics/throws from its mapping, and is driven
down **both** mapping sites.

On the **forward** site each asserts the instance still reports `error` and
unwinds the same procedures in the same order on both arms (`["undo-b:b:a:x",
"undo-a:a:x"]` / `[undo-b, undo-a]` — the literals differ only in the fixtures'
log formatting). That exact-list assertion is what pins the containment to
parity rather than to "roughly equivalent".

On the **crash-resume re-seed** site each starts the fixture through the resume
harness with a journal, so the mapping runs inside `resumed_start` /
`bankJournaledHandoffs`. Both assert containment and the terminal `error`
report. Rust additionally asserts the journaled stages were compensated in
reverse — read off the compensation-done markers the sink already captures, NOT
the compensation log, which `diamond_op!` fills once per completed output and a
journal-rebuilt `EntityRun` has none. Java asserts no undo list at all: its
scope there is empty by the known limitation above, and pinning that value would
make closing the gap a test failure.
