# multi-input-procedures Specification

## Purpose
TBD - created by archiving change add-multi-input-procedures. Update Purpose after archive.
## Requirements

### Requirement: A procedure SHALL be authorable over multiple source channels via a mapped sum-type input
Both SDKs (Rust and Java, released together) SHALL provide a merge authoring form in which a procedure declares a single `Input` sum type and subscribes to two or more source channels, each subscription supplying a pure mapping from the source channel's payload into that `Input` type. On a stream operative, entry channels and inter-procedure typed channels SHALL both be valid sources. On a workflow operative, only inter-procedure typed channels SHALL be valid sources: a workflow operative is seeded through one entry channel from its spawn args (and, for a per-input loop, its inbox items), neither of which carries the channel a payload arrived on, so no mapping could be selected — that shape SHALL be refused at definition time rather than accepted. The single-input `into` form SHALL remain unchanged.

#### Scenario: Two typed channels feed one procedure

- **WHEN** an operative merges `ticker_out: TickerStats` and `ohlc_out: Candle` into `DetectorProcedure` with `Input = SignalInput` via two mappings
- **THEN** the procedure SHALL receive `SignalInput::Ticker(..)` and `SignalInput::Candle(..)` values through its standard `run` path, and its hybrid outputs SHALL behave exactly as for single-input procedures

#### Scenario: Entry channel as a merge source on a stream operative

- **WHEN** an entry channel (sealed payload of type `T`) is one of a stream operative's merge sources with mapping `Fn(T) -> I`
- **THEN** frames injected on that entry (including by source providers and `flz operative send`) SHALL be delivered mapped, with no parser procedure required

#### Scenario: Entry channel as a workflow merge source is refused

- **WHEN** a workflow operative declares a merge whose sources include an entry channel
- **THEN** `define_workflow` SHALL return a named definition-time error identifying the entry channel and the merged procedure, and SHALL NOT register the class

#### Scenario: A mapping that panics on a workflow chain does not strand the instance

- **WHEN** a merge mapping on a workflow operative's inter-procedure source panics (Rust) or throws (Java) while releasing a producer's batch to its consumers
- **THEN** the panic SHALL be contained on the driver thread and reported as a terminal chain failure — the saga SHALL compensate the procedures recorded as completed at that point, which SHALL include the producer whose edge the mapping was applied to, and the operative SHALL still emit `WorkflowCompleted` with status `error` naming the mapping. A procedure still in flight elsewhere in the same level is not recorded and is not compensated, as for any other mid-level abort.

#### Scenario: A mapping that panics while re-seeding from the journal is contained, with an SDK-dependent undo scope

- **WHEN** a merge mapping panics (Rust) or throws (Java) while a crash-resumed chain re-seeds a consumer from a journaled handoff
- **THEN** it SHALL be contained and the operative SHALL still emit `WorkflowCompleted` with status `error`, and the saga SHALL compensate whatever that SDK's resumed iteration recorded as completed — the journaled procedures on Rust, and none on Java, whose resumed iteration does not record them. That difference is a pre-existing property of Java's workflow resume, not of the merge, and is documented as a known limitation rather than relied on

#### Scenario: SDK parity

- **WHEN** the capability ships
- **THEN** Rust and Java SDKs SHALL expose equivalent merge authoring with identical semantics and mirrored test scenarios

### Requirement: Merged delivery SHALL preserve per-source FIFO order and replay determinism
Frames SHALL be journaled per source channel unchanged; per-source mappings and any edge transforms and key function SHALL run identically on live delivery and WAL replay, so a warm restart replays a merged procedure to the same state as live processing. Per-source FIFO ordering SHALL be preserved; no ordering SHALL be guaranteed across sources. `entities_for` and the edge key function SHALL be evaluated on the mapped input value; a merge's key function SHALL be declared once on the merge rather than per source, because every source has been mapped into one type by the time keying runs. Mappings, transforms, and key functions SHALL be pure and total; none is offered a retry, and on a workflow chain — where they run on the driver thread rather than inside an entity invocation — an unwinding closure SHALL be contained so the operative still terminates observably. Where a producer's released batch is mapped into its consumers, the mapping SHALL be applied only after that producer's completion has been recorded, so the producer is always within the saga's undo scope, and both SDKs SHALL unwind the same set of procedures in the same order. Where a crash-resumed chain's mapping is applied to a journaled handoff, the undo scope is whatever that SDK's resumed iteration records as completed, which is not the same on both SDKs.

#### Scenario: Replay reproduces merged state

- **WHEN** a merged procedure processes interleaved frames from two sources and the operative warm-restarts
- **THEN** replay SHALL restore the same journaled state as live processing produced

#### Scenario: Per-source order holds under interleaving

- **WHEN** source A emits a1,a2 and source B emits b1 concurrently
- **THEN** the procedure SHALL observe a1 before a2, and b1 at any position relative to them

#### Scenario: A merge keys once, on the mapped value

- **WHEN** a merge declares two sources and one key function reading a field of the merged sum type
- **THEN** every input from either source SHALL be routed by that one function, and the function SHALL receive the mapped value rather than either source's payload

#### Scenario: A throwing key function on a merged workflow chain is contained

- **WHEN** the key function on a merged procedure's edge in a workflow chain throws while a producer's batch is accumulated
- **THEN** the chain SHALL abort through its existing compensate path and emit its terminal frame rather than stranding the instance

### Requirement: Merge definitions SHALL be validated at definition time
A merge with fewer than two sources, duplicate sources, a source re-wiring a channel-to-procedure edge already declared by `into` or by another merge, or a mapping type mismatch SHALL fail operative definition with a named topology error (extending the existing definition-time validation surface), not at runtime. A declared mapping SHALL never be silently discarded; a merge left unclosed by `into` SHALL be flagged by the language's own unused-result lint where one exists.

#### Scenario: Duplicate source rejected

- **WHEN** a merge subscribes the same channel twice
- **THEN** `define_stream`/`define_workflow` SHALL return a topology error naming the duplicated channel

#### Scenario: A mapping is never dropped onto an already-wired edge

- **WHEN** a merge declares a source whose channel already feeds that same procedure (wired by `into`, or by another merge)
- **THEN** operative definition SHALL fail with a topology error naming the channel and the procedure, rather than keeping the first declaration's mapping

### Requirement: A merge SHALL carry transforms on the merged stream, not per source
Value transforms (`map`, `filter`, `flat_map`) declared on a merge SHALL apply to the merged value, after each source's own mapping. Per-source reshaping SHALL remain the job of each source's mapping function, which is what defines the sum type. Declaring a transform directly on a source handle that is subsequently consumed by a merge SHALL apply to that source before its mapping.

#### Scenario: A transform on the merge sees the sum type

- **WHEN** a merge of two sources declares `.filter(...)` before `.into(P)`
- **THEN** the predicate SHALL receive the merged sum-type value and SHALL gate inputs originating from either source

#### Scenario: A transform on a source sees that source's payload

- **WHEN** a source handle declares `.filter(...)` and is then passed to `merge().from(source, Map::new)`
- **THEN** the predicate SHALL receive that source's own payload type and SHALL run before the source's mapping
