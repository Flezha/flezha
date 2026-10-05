# multi-input-tasklets Specification

## Purpose
TBD - created by archiving change add-multi-input-tasklets. Update Purpose after archive.
## Requirements
### Requirement: A tasklet SHALL be authorable over multiple source channels via a mapped sum-type input
Every SDK (Rust, Java, Node, Python, Ruby, PHP and Elixir) SHALL provide a merge authoring form in which a tasklet declares a single `Input` sum type and subscribes to two or more source channels, each subscription supplying a pure mapping from the source channel's payload into that `Input` type. On a flow relay and on a steps relay alike, entry channels and inter-tasklet typed channels SHALL both be valid sources: every message a steps relay receives carries the channel it arrived on, so the mapping of that source is selected as it is on a flow relay. On a steps relay, a merged tasklet fed by an entry channel SHALL be a root of that channel's chain, and a merge of two entry channels SHALL run the merged tasklet once per message, on the message's mapping, in the pass that message starts. The single-input `into` form SHALL remain unchanged.

#### Scenario: Two typed channels feed one tasklet

- **WHEN** a relay merges `ticker_out: TickerStats` and `ohlc_out: Candle` into `DetectorTasklet` with `Input = SignalInput` via two mappings
- **THEN** the tasklet SHALL receive `SignalInput::Ticker(..)` and `SignalInput::Candle(..)` values through its standard `run` path, and its hybrid outputs SHALL behave exactly as for single-input tasklets

#### Scenario: Entry channel as a merge source on a flow relay

- **WHEN** an entry channel (sealed payload of type `T`) is one of a flow relay's merge sources with mapping `Fn(T) -> I`
- **THEN** frames injected on that entry (including by source providers and `flz relay send`) SHALL be delivered mapped, with no parser tasklet required

#### Scenario: Entry channel as a steps relay merge source

- **WHEN** a `per_input` steps relay merges entry channel `orders: Order` and inter-tasklet channel `checked: Checked` into `Book` with two mappings
- **THEN** a message on `orders` SHALL reach `Book` as `Input::Order(..)` through `orders`'s mapping in the pass it starts, a value released on `checked` SHALL reach `Book` as `Input::Checked(..)`, and registration SHALL accept the class on every SDK

#### Scenario: A mapping that panics on a steps chain does not strand the instance

- **WHEN** a merge mapping on a steps relay's inter-tasklet source panics (Rust) or throws (Java) while releasing a producer's batch to its consumers
- **THEN** the panic SHALL be contained on the driver thread and reported as a terminal chain failure — the saga SHALL compensate the tasklets recorded as completed at that point, which SHALL include the producer whose edge the mapping was applied to, and the relay SHALL still emit `RelayCompleted` with status `error` naming the mapping. A tasklet still in flight elsewhere in the same level is not recorded and is not compensated, as for any other mid-level abort.

#### Scenario: A mapping that panics while re-seeding from the journal is contained, with an SDK-dependent undo scope

- **WHEN** a merge mapping panics (Rust) or throws (Java) while a crash-resumed chain re-seeds a consumer from a journaled handoff
- **THEN** it SHALL be contained and the relay SHALL still emit `RelayCompleted` with status `error`, and the saga SHALL compensate whatever that SDK's resumed iteration recorded as completed — the journaled tasklets on Rust, and none on Java, whose resumed iteration does not record them. That difference is a pre-existing property of Java's steps resume, not of the merge, and is documented as a known limitation rather than relied on

#### Scenario: An entry mapping that fails on a steps relay parks the message

- **WHEN** a merge mapping on a steps relay's entry-channel source panics or throws on a message
- **THEN** the message SHALL ride the entry channel's ladder and be parked to the dead-letter queue as a flow relay's would, no pass SHALL start, and the instance SHALL keep waiting

#### Scenario: SDK parity

- **WHEN** the `merged_input` fixture is started on any of the seven runtimes and one frame is injected on each of its two entry channels
- **THEN** the merged tasklet SHALL emit one line per source on the `merged` sink, each tagged by that source's mapping, and every SDK's own suite SHALL carry the mirrored merge tests: two typed sources into one tasklet, an entry channel as a source on both kinds, per-source FIFO under interleaving, the key evaluated on the mapped value, and the definition-time refusals

### Requirement: Merged delivery SHALL preserve per-source FIFO order and replay determinism
Frames SHALL be journaled per source channel unchanged; per-source mappings and any edge transforms and key function SHALL run identically on live delivery and WAL replay, so a warm restart replays a merged tasklet to the same state as live processing. Per-source FIFO ordering SHALL be preserved; no ordering SHALL be guaranteed across sources. `cells_for` and the edge key function SHALL be evaluated on the mapped input value; a merge's key function SHALL be declared once on the merge rather than per source, because every source has been mapped into one type by the time keying runs. Mappings, transforms, and key functions SHALL be pure and total; none is offered a retry, and on a steps chain — where they run on the driver thread rather than inside a cell invocation — an unwinding closure SHALL be contained so the relay still terminates observably. Where a producer's released batch is mapped into its consumers, the mapping SHALL be applied only after that producer's completion has been recorded, so the producer is always within the saga's undo scope, and both SDKs SHALL unwind the same set of tasklets in the same order. Where a crash-resumed chain's mapping is applied to a journaled handoff, the undo scope is whatever that SDK's resumed iteration records as completed, which is not the same on both SDKs.

#### Scenario: Replay reproduces merged state

- **WHEN** a merged tasklet processes interleaved frames from two sources and the relay warm-restarts
- **THEN** replay SHALL restore the same journaled state as live processing produced

#### Scenario: Per-source order holds under interleaving

- **WHEN** source A emits a1,a2 and source B emits b1 concurrently
- **THEN** the tasklet SHALL observe a1 before a2, and b1 at any position relative to them

#### Scenario: A merge keys once, on the mapped value

- **WHEN** a merge declares two sources and one key function reading a field of the merged sum type
- **THEN** every input from either source SHALL be routed by that one function, and the function SHALL receive the mapped value rather than either source's payload

#### Scenario: A throwing key function on a merged steps chain is contained

- **WHEN** the key function on a merged tasklet's edge in a steps chain throws while a producer's batch is accumulated
- **THEN** the chain SHALL abort through its existing compensate path and emit its terminal frame rather than stranding the instance

### Requirement: Merge definitions SHALL be validated at definition time
A merge with fewer than two sources, duplicate sources, a source re-wiring a channel-to-tasklet edge already declared by `into` or by another merge, or a mapping type mismatch SHALL fail relay definition with a named topology error (extending the existing definition-time validation surface), not at runtime. A declared mapping SHALL never be silently discarded; a merge left unclosed by `into` SHALL be flagged by the language's own unused-result lint where one exists.

#### Scenario: Duplicate source rejected

- **WHEN** a merge subscribes the same channel twice
- **THEN** `register_factory` / `registerFactory` SHALL return a topology error naming the duplicated channel

#### Scenario: A mapping is never dropped onto an already-wired edge

- **WHEN** a merge declares a source whose channel already feeds that same tasklet (wired by `into`, or by another merge)
- **THEN** relay definition SHALL fail with a topology error naming the channel and the tasklet, rather than keeping the first declaration's mapping

### Requirement: A merge SHALL carry transforms on the merged stream, not per source
Value transforms (`map`, `filter`, `flat_map`) declared on a merge SHALL apply to the merged value, after each source's own mapping. Per-source reshaping SHALL remain the job of each source's mapping function, which is what defines the sum type. Declaring a transform directly on a source handle that is subsequently consumed by a merge SHALL apply to that source before its mapping.

#### Scenario: A transform on the merge sees the sum type

- **WHEN** a merge of two sources declares `.filter(...)` before `.into(P)`
- **THEN** the predicate SHALL receive the merged sum-type value and SHALL gate inputs originating from either source

#### Scenario: A transform on a source sees that source's payload

- **WHEN** a source handle declares `.filter(...)` and is then passed to `merge().from(source, Map::new)`
- **THEN** the predicate SHALL receive that source's own payload type and SHALL run before the source's mapping

