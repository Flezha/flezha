# stream-transforms Specification

## Purpose
Edge-closure value transforms (`map`, `filter`, `flat_map`) and edge-side Cell
keying (`key_by`) on the graph-authoring surface: their evaluation point, their
composition into a single edge closure, their replay-determinism and purity
contract, and the build-time refusals.
## Requirements
### Requirement: Stream handles SHALL offer value transforms that are not Tasklets

Every SDK SHALL offer `map`, `filter`, and `flat_map` / `flatMap` on the
entry-stream and output-stream handles. Each SHALL take an author closure, SHALL
return a stream handle carrying the post-transform payload type, and SHALL NOT
create a Tasklet, a Cell, a mailbox, or a dispatch hop. A transform SHALL be
stored on the consumer edge that the chain terminates in, and a chain of
transforms not terminated by `.into(<tasklet>)` SHALL fail definition with an
error naming the dangling stream. A plugin sink SHALL NOT terminate a chain: it
serializes the channel's own value and closes no consumer edge, so a chain
followed only by a sink is dangling.

Every SDK SHALL additionally refuse, naming the channel: a second edge key
before one `.into(...)`; a transform declared after an edge key, the key being
evaluated on the final post-transform value; and a transform or key declared on
a handle whose channel already carries a plugin sink.

#### Scenario: A filter costs no Tasklet

- **WHEN** an operative declares `c.input(Trade.class, "trades").filter(t -> t.qty() > 1.0).into(CandleTasklet.class)` (Java) or the equivalent in any other SDK
- **THEN** registration SHALL succeed with a topology containing exactly ONE Tasklet node (`CandleTasklet`), and inputs failing the predicate SHALL NOT reach it

#### Scenario: A transform reshapes the channel's payload type

- **WHEN** a stream of `Trade` declares `.map(Ticker.class, Ticker::from)` and the result is wired into a Tasklet whose `Input` is `Ticker`
- **THEN** registration SHALL succeed and the consumer SHALL observe `Ticker` values

#### Scenario: One input may yield several values

- **WHEN** a stream declares `.flatMap(Slot.class, t -> TIMEFRAMES.stream().map(tf -> new Slot(t, tf)))` over four timeframes
- **THEN** each input SHALL produce four values delivered independently to the consumer

#### Scenario: A dangling transform chain is refused

- **WHEN** a stream declares `.map(...)` and the resulting handle is never wired to a Tasklet, whether it is left unwired or is followed only by a plugin sink
- **THEN** definition SHALL fail with an error naming the dangling stream

#### Scenario: A second edge key is refused rather than replacing the first

- **WHEN** a stream declares two edge keys before one `.into(...)`
- **THEN** definition SHALL fail naming the channel, and SHALL NOT silently keep either key

#### Scenario: A transform after the key it would feed is refused

- **WHEN** a stream declares an edge key and then a transform, and wires the result into a Tasklet
- **THEN** definition SHALL fail naming the channel, because the key is evaluated on the final post-transform value and so must be the last link

#### Scenario: A transform behind an attached plugin sink is refused

- **WHEN** a stream attaches a plugin sink and then declares a transform or an edge key on the returned handle, even where that chain is closed by a later `.into(...)`
- **THEN** definition SHALL fail naming the channel; declaring the transform and its `.into(...)` before attaching the sink SHALL succeed and SHALL produce the same topology

#### Scenario: A fanned value answers every key at the crossing

- **WHEN** a chain declares `flat_map` yielding three values and then `key_by`, and the host answers the core's key request for one payload
- **THEN** the answer SHALL carry the three keys, one per fanned value, a chain whose fan yields nothing SHALL answer no key, exactly as a filtered payload does, and the core SHALL deliver the payload to the Cell of every answered key, where the host runs only the values whose key names that Cell

#### Scenario: The transform chain is proven on every runtime

- **WHEN** the `transform_chain` fixture is started on any of the seven runtimes and the frames `a:1,b:2` and `drop` are injected on `trades`
- **THEN** exactly `A:1` and `B:2` SHALL reach the `transformed` sink, on the keys `A` and `B` (the key is read after the upper-casing `map`), and nothing SHALL reach it for `drop`

### Requirement: Cell keying SHALL be declared on the edge, not on the Tasklet

Both SDKs SHALL offer `key_by` (Rust) / `keyBy` (Java) on the stream handles,
taking a closure from the post-transform value to a `String`. The returned key
SHALL select the Cell of the consumer wired after it. `key_by` SHALL be the only
way to express dynamic Cell keying; a consumer reached by an edge carrying a key
function SHALL be a dynamic-mode Tasklet with lazily created Cells, exactly as
the deleted `cell_key` trait hook produced.

#### Scenario: Keying is visible in the wiring

- **WHEN** an operative declares `c.input(Trade.class, "trades").keyBy(Trade::symbol).into(CandleTasklet.class)`
- **THEN** inputs SHALL be routed to the Cell named by `symbol`, and `CandleTasklet` SHALL declare no keying member of any kind

#### Scenario: Each value of a fan-out is keyed independently

- **WHEN** a `flatMap` yields four `Slot` values from one `Trade` and the edge also declares `.keyBy(s -> s.symbol() + "@" + s.tf())`
- **THEN** the four values SHALL be routed to four distinct Cells

#### Scenario: Cell lifecycle is unchanged

- **WHEN** a Tasklet reached by a keyed edge declares `cell_idle_ttl()`
- **THEN** its lazily created Cells SHALL be evicted on that TTL exactly as dynamic-mode Cells are today

### Requirement: Consecutive transforms SHALL compose into ONE edge closure

Consecutive transforms and the key function declared on one edge SHALL be composed
into a single stored closure per concern (one transform closure, one key closure).
The runtime SHALL apply at most one transform closure and at most one key closure
per consumer edge. Fusion SHALL therefore be structural — there SHALL be no
optimizer pass, no chaining heuristic, and no author-facing directive to disable
chaining.

#### Scenario: Three transforms are one closure

- **WHEN** an edge declares `.filter(f).map(g).map(h)` before `.into(P)`
- **THEN** the topology SHALL store exactly one transform closure on that edge, and one input SHALL traverse the routing seam exactly once

#### Scenario: A Tasklet is the boundary

- **WHEN** a chain declares `.map(f).into(A).map(g).into(B)`
- **THEN** `f` SHALL be stored on the edge into `A` and `g` on the edge into `B`, and the value SHALL cross a mailbox exactly once, at `A`

### Requirement: Transforms and keys SHALL be evaluated at the routing seam

Transform and key closures SHALL be evaluated where the existing merge mapping is
evaluated — during routing to consumers, before delivery — so that live delivery
and the controller's re-forwarded replay slice traverse the identical code path.
The key closure SHALL be evaluated on the post-transform value. Frames SHALL
continue to be journaled per source channel unchanged.

#### Scenario: Replay reproduces transformed state

- **WHEN** a Tasklet behind an edge carrying a transform and a key processes inputs and the operative warm-restarts
- **THEN** replay SHALL restore the same journaled state per Cell as live processing produced

#### Scenario: The key sees the transformed value

- **WHEN** an edge declares `.map(Ticker.class, Ticker::from).keyBy(Ticker::symbol)`
- **THEN** the key closure SHALL receive a `Ticker`, not the pre-transform `Trade`

### Requirement: Transform and key closures SHALL be pure, and an unwinding closure SHALL be contained

Transform and key closures SHALL be pure and total: they re-run on replay, and a
closure that observed ambient state or produced a different value would rebuild a
state the live run never had. A closure that panics or throws SHALL NOT terminate
the routing thread; the failure SHALL ride the channel's existing retry-then-DLQ
ladder — the same ladder a failing entry-channel parse rides — and on a workflow
chain SHALL be contained so the operative still terminates observably.

#### Scenario: A throwing transform does not kill routing

- **WHEN** a `map` closure throws on one input
- **THEN** the routing thread SHALL survive, the input SHALL be retried per the channel's retry policy and parked to the DLQ on exhaustion, and subsequent inputs SHALL continue to be routed

#### Scenario: A throwing transform on a workflow chain terminates observably

- **WHEN** a transform closure on a workflow operative's inter-tasklet edge throws
- **THEN** the chain SHALL abort through its existing compensate path and emit its terminal frame rather than stranding the instance

### Requirement: Transform and key declarations SHALL be validated at definition time

Every SDK SHALL refuse the following at topology build time with the same error
names: `KeyByOnUnkeyableTarget` — a `key_by` whose consumer declares a non-empty
`cell_keys()`; `DuplicateKeyBy` — more than one `key_by` on one consumer edge;
`TransformAfterTerminal` — a transform declared on a handle already closed by a
plugin sink; `EdgeAlreadyWired` — an existing consumer edge re-declared where
either the existing or the new declaration carries a transform or a key, named
by the channel and the tasklet it already feeds. A re-declaration carrying
neither SHALL remain the legal fan-in no-op.

#### Scenario: Keying a static-mode consumer is refused

- **WHEN** a `key_by` edge wires a Tasklet declaring `cell_keys() = ["1m","1h"]`
- **THEN** the build SHALL fail with `KeyByOnUnkeyableTarget` naming the Tasklet

#### Scenario: Two key functions on one edge are refused

- **WHEN** an edge declares `.keyBy(f).keyBy(g)` before `.into(P)`
- **THEN** the build SHALL fail with `DuplicateKeyBy` naming the edge

#### Scenario: Silent re-declaration is refused

- **WHEN** a consumer edge already carrying a key function is declared a second time with a different key function
- **THEN** the build SHALL fail rather than keep either function

#### Scenario: A closure on the second declaration is refused too

- **WHEN** a channel already feeds a Tasklet on a bare edge and the same Tasklet is wired from that channel a second time, this time behind a transform or a key
- **THEN** the build SHALL fail with `EdgeAlreadyWired` naming the channel and the Tasklet, and SHALL NOT append a second consumer edge

#### Scenario: Transform-free re-declaration stays legal

- **WHEN** the same Tasklet is wired as a consumer of the same channel twice with no transform and no key on either declaration
- **THEN** the declaration SHALL remain a no-op producing exactly one consumer edge

### Requirement: Java SHALL take an explicit type token where erasure prevents inference

Java's `map` and `flatMap` SHALL take the post-transform type as an explicit
`Class<?>` first argument, matching the existing `input(Class<T>, String)`
convention, because erasure prevents recovering the type from a lambda and the
runtime compares declared types when wiring a consumer. Java's `filter` and
`keyBy` SHALL take no type token. Rust SHALL take no type token on any of the
four, because `Tasklet<Input = T>` is checked at compile time.

#### Scenario: Java map carries the token

- **WHEN** a Java operative declares `.map(Ticker.class, Ticker::from)`
- **THEN** the resulting stream's declared payload type SHALL be `Ticker` and wiring a consumer whose `Input` is not `Ticker` SHALL fail registration naming both types

#### Scenario: Rust map infers

- **WHEN** a Rust operative declares `.map(Ticker::from).into::<P>()` where `P::Input` is not `Ticker`
- **THEN** the program SHALL fail to compile

### Requirement: The definition-time refusals are rows of the reference fixture

The refusals this capability names SHALL be judged by the reference check over the
ordered links of each chain, never by a builder of its own — `KeyByOnUnkeyableTarget`,
`DuplicateKeyBy`, `TransformAfterTerminal`, `TransformAfterKeyBy`,
`DanglingTransformChain`, `EdgeAlreadyWired` — and each SHALL be one row of
`test/fixtures/reference_topologies.txt` together with the `ok` row that pins the legal
transform-free fan-in as one consumer edge. An SDK SHALL prove the set by its conformance
run over those rows, with no refusal test of its own per name.

#### Scenario: The link order is what the check reads

- **WHEN** a chain records a `key` link followed by a `transform` link before `into`
- **THEN** the check refuses `TransformAfterKeyBy` naming the channel, in every SDK,
  because the record carries the order the builder was called in

#### Scenario: The fan-in no-op derives one edge in every SDK

- **WHEN** the `ok` row wires the same tasklet from one channel twice with no link on either declaration
- **THEN** every SDK's derived declaration lists that consumer once

