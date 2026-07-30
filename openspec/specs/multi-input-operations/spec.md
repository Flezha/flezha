# multi-input-operations Specification

## Purpose
TBD - created by archiving change add-multi-input-operations. Update Purpose after archive.
## Requirements
### Requirement: An operation SHALL be authorable over multiple source channels via a mapped sum-type input

Both SDKs (Rust and Java, released together) SHALL provide a merge authoring form in which an operation declares a single `Input` sum type and subscribes to two or more source channels, each subscription supplying a pure mapping from the source channel's payload into that `Input` type. On an unbounded operative, entry channels and inter-operation typed channels SHALL both be valid sources. On a bounded operative, only inter-operation typed channels SHALL be valid sources: a bounded operative is seeded through one entry channel from its spawn args (and, for a per-input loop, its inbox items), neither of which carries the channel a payload arrived on, so no mapping could be selected — that shape SHALL be refused at definition time rather than accepted. The single-input `into` form SHALL remain unchanged.

#### Scenario: Two typed channels feed one operation

- **WHEN** an operative merges `ticker_out: TickerStats` and `ohlc_out: Candle` into `DetectorOperation` with `Input = SignalInput` via two mappings
- **THEN** the operation SHALL receive `SignalInput::Ticker(..)` and `SignalInput::Candle(..)` values through its standard `run` path, and its hybrid outputs SHALL behave exactly as for single-input operations

#### Scenario: Entry channel as a merge source on an unbounded operative

- **WHEN** an entry channel (sealed payload of type `T`) is one of an unbounded operative's merge sources with mapping `Fn(T) -> I`
- **THEN** frames injected on that entry (including by source providers and `flz operative send`) SHALL be delivered mapped, with no parser operation required

#### Scenario: Entry channel as a bounded merge source is refused

- **WHEN** a bounded operative declares a merge whose sources include an entry channel
- **THEN** `define_bounded` SHALL return a named definition-time error identifying the entry channel and the merged operation, and SHALL NOT register the class

#### Scenario: A mapping that panics on a bounded chain does not strand the instance

- **WHEN** a merge mapping on a bounded operative's inter-operation source panics (Rust) or throws (Java) while releasing a producer's batch to its consumers
- **THEN** the panic SHALL be contained on the driver thread and reported as a terminal chain failure — the saga SHALL compensate the operations recorded as completed at that point, which SHALL include the producer whose edge the mapping was applied to, and the operative SHALL still emit `BoundedCompleted` with status `error` naming the mapping. An operation still in flight elsewhere in the same level is not recorded and is not compensated, as for any other mid-level abort.

#### Scenario: A mapping that panics while re-seeding from the journal is contained, with an SDK-dependent undo scope

- **WHEN** a merge mapping panics (Rust) or throws (Java) while a crash-resumed chain re-seeds a consumer from a journaled handoff
- **THEN** it SHALL be contained and the operative SHALL still emit `BoundedCompleted` with status `error`, and the saga SHALL compensate whatever that SDK's resumed iteration recorded as completed — the journaled operations on Rust, and none on Java, whose resumed iteration does not record them. That difference is a pre-existing property of Java's bounded resume, not of the merge, and is documented as a known limitation rather than relied on

#### Scenario: SDK parity

- **WHEN** the capability ships
- **THEN** Rust and Java SDKs SHALL expose equivalent merge authoring with identical semantics and mirrored test scenarios

### Requirement: Merged delivery SHALL preserve per-source FIFO order and replay determinism

Frames SHALL be journaled per source channel unchanged; mappings SHALL run identically on live delivery and WAL replay, so a warm restart replays a merged operation to the same state as live processing. Per-source FIFO ordering SHALL be preserved; no ordering SHALL be guaranteed across sources. `segment_for` SHALL be evaluated on the mapped input value. Mappings SHALL be pure and total; a mapping is not offered a retry, and on a bounded chain — where it runs on the driver thread rather than inside a segment invocation — an unwinding mapping SHALL be contained so the operative still terminates observably. Where a producer's released batch is mapped into its consumers, the mapping SHALL be applied only after that producer's completion has been recorded, so the producer is always within the saga's undo scope, and both SDKs SHALL unwind the same set of operations in the same order. Where a crash-resumed chain's mapping is applied to a journaled handoff, the undo scope is whatever that SDK's resumed iteration records as completed, which is not the same on both SDKs.

#### Scenario: Replay reproduces merged state

- **WHEN** a merged operation processes interleaved frames from two sources and the operative warm-restarts
- **THEN** replay SHALL restore the same journaled state as live processing produced

#### Scenario: Per-source order holds under interleaving

- **WHEN** source A emits a1,a2 and source B emits b1 concurrently
- **THEN** the operation SHALL observe a1 before a2, and b1 at any position relative to them

### Requirement: Merge definitions SHALL be validated at definition time

A merge with fewer than two sources, duplicate sources, a source re-wiring a channel-to-operation edge already declared by `into` or by another merge, or a mapping type mismatch SHALL fail operative definition with a named topology error (extending the existing definition-time validation surface), not at runtime. A declared mapping SHALL never be silently discarded; a merge left unclosed by `into` SHALL be flagged by the language's own unused-result lint where one exists.

#### Scenario: Duplicate source rejected

- **WHEN** a merge subscribes the same channel twice
- **THEN** `define_unbounded`/`define_bounded` SHALL return a topology error naming the duplicated channel

#### Scenario: A mapping is never dropped onto an already-wired edge

- **WHEN** a merge declares a source whose channel already feeds that same operation (wired by `into`, or by another merge)
- **THEN** operative definition SHALL fail with a topology error naming the channel and the operation, rather than keeping the first declaration's mapping

