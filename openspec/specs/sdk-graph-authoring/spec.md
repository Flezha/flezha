# sdk-graph-authoring Specification

## Purpose
TBD - created by archiving change sdk-graph-authoring-api. Update Purpose after archive.
## Requirements
### Requirement: Producer-first stream-handle graph builder
Both SDKs SHALL author operative topologies through a producer-first fluent builder in which `.into(<procedure>)` wires a consumer onto the current stream and returns the **consumed procedure's own output stream handle**. `input(<type>, "name").into(A)` SHALL wire the entry consumer and return A's output stream; `stream.into(B)` SHALL add B as a consumer of `stream`'s channel and return B's output stream, so a linear pipeline is a single chained expression. The handle SHALL be a lightweight, copyable reference into the operative's topology builder (Java: an object; Rust: a `Copy` value), and per-channel configuration (`codec()`, `keyed()`, `onFull(...)`, `retryMax(...)`, `retryBackoffMs(...)`, `named(...)`) SHALL remain available on the handle, chainable in any order relative to consumer wiring. Handles SHALL be invalidated when the topology freezes at registration; using one afterwards SHALL fail with an error naming the operative.

A chain MAY additionally carry **edge transforms** (`map`, `filter`, `flat_map` / `flatMap`) and **edge keying** (`key_by` / `keyBy`) between one `.into(...)` and the next. These SHALL return a stream handle of the same kind, SHALL NOT introduce a Procedure node into the topology, and SHALL be stored on the consumer edge the chain terminates in. Both the entry-stream handle and the output-stream handle SHALL expose the same transform, keying, and per-channel configuration surface; the two handles SHALL NOT drift.

The payload type of every non-entry channel SHALL be taken from the producing procedure's declared `Output` type, as reshaped by any edge transforms declared after it — the builder SHALL NOT accept a redundant type token for inter-procedure channels, except where Java erasure prevents inferring a transform's result type. Wiring a consumer whose `Input` type differs from the stream's payload type SHALL fail to compile in Rust and SHALL fail registration in Java with an error naming both procedures and both types.

#### Scenario: Linear chain is one expression

- **WHEN** an operative declares `c.input(OrderEvent.class, "orders").into(Validate.class).into(Alert.class).sink("alerts")` (Java) or the equivalent `c.input::<OrderEvent>("orders").into::<Validate>().into::<Alert>().sink("alerts")` (Rust)
- **THEN** registration SHALL succeed with the topology entry→Validate→Alert→sink, and the channel between Validate and Alert SHALL carry Validate's declared `Output` type with no type token in the declaration

#### Scenario: Transforms sit between consumers without adding nodes

- **WHEN** an operative declares `c.input(Trade.class, "trades").filter(t -> t.qty() > 1.0).keyBy(Trade::symbol).into(Candle.class).sink("candles")`
- **THEN** registration SHALL succeed with a topology containing exactly one Procedure node, and the entry→Candle edge SHALL carry the predicate and the key function

#### Scenario: Type-mismatched consumer is refused

- **WHEN** `stream.into(B)` is declared where B's `Input` type differs from the stream's payload type
- **THEN** the program SHALL fail to compile (Rust) or registration SHALL throw naming both procedures and both types (Java)

#### Scenario: Both handles carry the same surface

- **WHEN** a transform or keying method is declared on an entry-stream handle and on an output-stream handle
- **THEN** both SHALL accept it with identical semantics, and neither handle SHALL offer a transform the other lacks

#### Scenario: Handle used after freeze is refused

- **WHEN** user code retains a stream handle and calls `.into(...)` on it after the operative's topology has been frozen at registration
- **THEN** the call SHALL fail with an error naming the operative and stating the topology is frozen

### Requirement: Branching is handle reuse; `branch()` is equivalent sugar
Using one stream handle to wire more than one consumer SHALL declare fan-out: every consumer receives every value released on that stream's channel. The builder SHALL also expose `branch(arm1, arm2, ...)` taking two or more lambdas, each invoked with a copy of the same handle; `branch` SHALL be pure sugar — the resulting topology SHALL be identical to the equivalent handle-reuse declaration, and the two spellings SHALL be freely mixable.

#### Scenario: Handle reuse fans out

- **WHEN** an operative holds `validated` (a stream handle) and declares `validated.into(FraudCheck)` and `validated.into(Receipt)`
- **THEN** both FraudCheck and Receipt SHALL be consumers of the same channel and each SHALL receive every value the producer releases on it

#### Scenario: branch() builds the identical topology

- **WHEN** the same graph is declared once via two `validated.into(...)` statements and once via `validated.branch(b -> b.into(FraudCheck), b -> b.into(Receipt))`
- **THEN** the two frozen topologies SHALL be identical (same channels, consumers, and derived names)

#### Scenario: branch() with fewer than two arms is refused

- **WHEN** `branch(...)` is called with zero or one lambda
- **THEN** registration SHALL fail with an error stating branch requires at least two arms

### Requirement: Channel names auto-derive from the producer node's identity
A channel created by wiring out of a node SHALL be named by that node's identity string (the procedure's `name()`, or the explicit node name per the node-identity requirement) unless `.named("...")` overrides it. Entry channels SHALL keep an explicit, mandatory name (they are the operative's external input address). Derived, overridden, and entry names SHALL all live in the existing per-operative channel namespace with its duplicate check; a collision SHALL fail registration naming both declarations. Because derived names come from hand-written identity strings, renaming a language class SHALL NOT change any on-disk or on-wire name.

#### Scenario: Derived name ships on the wire

- **WHEN** procedure `Validate` (whose `name()` returns `"validate"`) is wired via `.sink("audit_sink")` with no `.named(...)` override
- **THEN** frames delivered to the `audit_sink` plugin SHALL carry `channel = "validate"` in their metadata

#### Scenario: named() overrides the derived name

- **WHEN** the declaration chains `.named("orders.validated")` on Validate's output stream
- **THEN** the channel SHALL be recorded and stamped as `"orders.validated"` everywhere the channel name appears (wire metadata, journals, errors)

#### Scenario: Name collision is refused

- **WHEN** a `.named(...)` override or node name collides with another channel name in the same operative
- **THEN** registration SHALL fail with an error naming both colliding declarations

### Requirement: Node identity permits reusing one Procedure class at several graph positions
A graph node SHALL be identified by its Procedure class unless an explicit node name is supplied at wiring time (`.into(Filter.class, "spam_filter")` in Java, `into_named::<Filter>("spam_filter")` in Rust). Two unnamed mentions of the same class SHALL resolve to the same node. An explicit node name SHALL create a distinct node whose identity string replaces the procedure's `name()` for every purpose identity serves today: per-entity state directories, journal entries, derived channel names, and diagnostics. Node identity strings SHALL be unique per operative; a collision (with another node name or a procedure `name()`) SHALL fail registration.

#### Scenario: Same class reused as two named nodes

- **WHEN** an operative declares `validated.into(Filter.class, "fraud_filter")` and `validated.into(Filter.class, "spam_filter")`
- **THEN** registration SHALL succeed with two distinct nodes, and each node's entities SHALL persist state under its own identity (`fraud_filter` / `spam_filter`)

#### Scenario: Unnamed mentions unify

- **WHEN** two different streams each declare `.into(Merge.class)` with no node name
- **THEN** both SHALL wire to one Merge node (a fan-in), not two

#### Scenario: Node-name collision is refused

- **WHEN** a node name equals another node's identity string in the same operative
- **THEN** registration SHALL fail naming both nodes

### Requirement: Graphs are DAGs; fan-in accumulates in both SDKs
The shared graph-shape rule for both SDKs SHALL be: one **or more** entry channels feeding the graph, every node reachable from some entry channel, at most one output channel per node, any number of incoming channels per node, and no cycles. Multiple entry channels let a single operative ingest several external sources (e.g. a market pipeline consuming a trade feed and an order-book feed on distinct channels); the controller routes each inbound frame to its entry channel by the `metadata["channel"]` name, falling back to the first-declared entry channel when that metadata is absent. Validation SHALL use a topological ordering (all of a node's producers precede it); a cycle SHALL be rejected naming a node on it. A node with several incoming channels (fan-in) SHALL receive the **accumulated** contributions of all its producers — no producer's output may be overwritten or dropped. In workflow operatives a fan-in node SHALL be dispatched only after every one of its producers has completed and committed, and its input batch SHALL be the concatenation of all producers' released outputs. In stream operatives the existing per-channel dispatch SHALL deliver both producers' values to the shared consumer as they arrive.

#### Scenario: Workflow join runs once with both contributions

- **WHEN** a workflow graph A→(B,C)→D runs to completion
- **THEN** D SHALL be dispatched exactly once, after both B and C committed, with an input batch containing both B's and C's released outputs

#### Scenario: Cycle is refused

- **WHEN** a declaration wires a node's output (transitively) back into itself
- **THEN** registration SHALL fail with an error identifying the cycle

#### Scenario: Rust accepts what Java accepts

- **WHEN** the same fan-in topology is declared in the Rust SDK and the Java SDK
- **THEN** both SHALL accept it with identical accumulate semantics — the former Rust fan-in rejection SHALL NOT exist

### Requirement: Typed entry inputs with default and custom deserialization
`input(<type>, "name")` SHALL accept any payload type. `byte[]`/`Vec<u8>` SHALL be delivered unchanged and `String` UTF-8-decoded, byte-identical to current behavior. Any other type SHALL be deserialized at the entry edge from the wire bytes using the SDK's default JSON codec (Jackson in Java; `serde_json` behind a `DeserializeOwned` bound in Rust), with an overload/variant accepting a custom `bytes → T` parser. The parser SHALL run before dispatch on the consumer path: a parse failure SHALL be classified like a failing invocation and ride the entry channel's existing retry→DLQ ladder under the channel's origin identity. The parser SHALL be deterministic (same bytes → same value; no ambient reads), the same contract output serializers carry.

#### Scenario: Typed entry with default JSON

- **WHEN** an operative declares `c.input(OrderEvent.class, "orders")` and a JSON-encoded OrderEvent arrives on the wire
- **THEN** the consuming procedure's `run` SHALL receive the deserialized `OrderEvent`

#### Scenario: Custom parser overload

- **WHEN** an operative declares `c.input(OrderEvent.class, "orders", bytes -> OrderEvent.parse(bytes))`
- **THEN** the supplied parser SHALL produce the value delivered to the consumer, and the default JSON codec SHALL NOT run

#### Scenario: Parse failure parks to the DLQ

- **WHEN** an arriving payload fails the entry parser on every retry attempt of the channel's retry budget
- **THEN** the payload SHALL be parked to the dead-letter queue under the entry channel's origin identity, and the consuming procedure SHALL NOT be invoked with a partial value

#### Scenario: String and bytes entries are byte-identical to today

- **WHEN** an operative declares a `String` or `byte[]` entry channel
- **THEN** delivery behavior (passthrough / UTF-8 decode, invalid-UTF-8 drop-and-log) SHALL be unchanged from the pre-change contract

### Requirement: Single `sink` egress with optional `sealed` terminator

Plugin egress SHALL be declared by exactly one builder method: `sink(name)` / `sink(name, serializer)` in Java and `sink(name)` / `sink_with(name, serializer)` in Rust. `sink` SHALL bind the channel to the named plugin sink and SHALL return the stream handle, so inter-procedure consumers MAY be wired after it — a channel is terminal by having no further wiring, not by method choice. `sink(name)` without a serializer SHALL apply the same default serialization rules as the removed `output` (passthrough for bytes, UTF-8 for strings, default JSON otherwise). The name SHALL be destination-neutral: it identifies the attachment point the controller routes to by channel-name metadata; several channels MAY name the same plugin sink. A separate optional `.sealed()` terminator SHALL return a non-chainable handle and add no topology or runtime difference — it is a compile-time statement that no consumer follows. The former `output` / `alsoOutput` / `also_output` / `also_output_with` spellings SHALL NOT exist in either SDK; there SHALL be no coexistence period. The rename SHALL be scoped to user-called builder methods: wire frames, descriptor fields, plugin bindings, controller configuration, and error identifiers keep their existing names.

#### Scenario: sink is hybrid by default

- **WHEN** a stream declares `.sink("indicators", serializer)` and a consumer is wired from the same handle afterwards
- **THEN** each emitted value SHALL reach the typed consumer and, serialized once, the `indicators` plugin — identical to the former `alsoOutput` behavior

#### Scenario: Terminal by construction

- **WHEN** a stream declares `.sink("pager_sink")` and nothing further is wired from the handle
- **THEN** the resulting topology SHALL be identical to the former `.output("pager_sink")` declaration

#### Scenario: sealed refuses further consumers at compile time

- **WHEN** user code attempts to wire a consumer from the handle returned by `.sink("audit").sealed()`
- **THEN** the code SHALL fail to compile (Rust) / fail to type-check (Java) — and the sealed declaration's runtime topology SHALL be byte-identical to the same declaration without `.sealed()`

#### Scenario: Old spellings do not exist

- **WHEN** user code calls `.output(...)`, `.alsoOutput(...)`, or `.also_output_with(...)` against the new SDK
- **THEN** the code SHALL fail to compile; no transitional alias SHALL exist

#### Scenario: Wire metadata is unchanged

- **WHEN** a channel formerly declared with `alsoOutput("indicators", s)` is redeclared with `sink("indicators", s)`
- **THEN** frames delivered to the plugin SHALL be byte-identical, including the `channel` name metadata and record keying

### Requirement: The channel-declaration API is replaced with no coexistence
`ChannelDecl`, `.from(...)`, and the typed-and-named inter-procedure `channel(<type>, "name")` declaration SHALL be removed from both SDKs' public surface in the same change that introduces the graph builder. There SHALL be no transitional period in which both APIs are usable.

#### Scenario: Old spellings do not compile

- **WHEN** user code declares `c.channel(String.class, "mid").from(Stage1.class)` (Java) or `c.channel::<String>("mid").from::<Stage1>()` (Rust) against the new SDK
- **THEN** the code SHALL fail to compile — the methods do not exist

### Requirement: One contract across SDKs, per-language spelling allowed
The graph builder's semantics — handle model, branching, node identity, DAG rule, fan-in accumulation, derived names, typed inputs, `sink` vocabulary — SHALL be identical in Rust and Java, and the shared e2e matrix SHALL exercise one contract with no per-runtime branching. Per-language surface differences SHALL be limited to spelling (`Proc.class` vs turbofish, lambdas vs closures, `into(Proc.class, "name")` vs `into_named::<Proc>("name")`). Where Rust enforces a rule at compile time, Java SHALL enforce the same rule at registration with an error carrying the same diagnostic content.

#### Scenario: Same graph, both SDKs, same behavior

- **WHEN** the shared e2e branching fixture topology is authored in both SDKs with the new builder
- **THEN** both runtimes SHALL produce the same sink outputs, journal shapes, and compensation order for the same scenario script

### Requirement: Latest-document router building block

The graph builder SHALL offer `route_latest(document_stream, traffic_stream, key_fn)` (Rust) / `routeLatest(documentStream, trafficStream, keyFn)` (Java), expanding to a single-entity SDK-packaged procedure that turns an operative-wide document stream into per-key keyed deliveries. Its journaled state SHALL be `{latest document, version, delivered: key → last delivered version}`. A received document byte-identical to the retained one SHALL be inert; a differing document SHALL replace it and increment the version. On each traffic item, the procedure SHALL emit the retained document keyed under `key_fn(item)` if and only if that key's delivered version is behind the current version, then record the delivery — so each key receives each distinct document version exactly once, always on that key's own traffic, and keys never observed in traffic receive nothing. The expansion SHALL use only existing primitives (merge, single-entity procedure, journaled state, keyed emit): no new runtime semantics, wire format, or replay path. State mutations SHALL ride the standard change journal and satisfy the replay-determinism harness.

#### Scenario: Changed document reaches each key on its own next traffic

- **WHEN** a new document arrives and traffic items for keys `A` and `B` follow
- **THEN** the document SHALL be emitted exactly once keyed `A` (on `A`'s first subsequent item) and exactly once keyed `B` (on `B`'s first subsequent item)

#### Scenario: Repeated identical document is inert

- **WHEN** the same document arrives again (e.g. a re-poll after reconnect) and traffic continues
- **THEN** no delivery SHALL be emitted for any key that already received it

#### Scenario: A first-seen key receives the current document once

- **WHEN** traffic introduces a key never seen before while a document is retained
- **THEN** the retained document SHALL be emitted keyed under the new key on that first item, and not again on the following items

#### Scenario: Only keys proven by traffic are emitted

- **WHEN** a document changes while some key produces no further traffic
- **THEN** no emission SHALL be made for that key — there is no remembered-set fan-out

#### Scenario: Router state replays deterministically

- **WHEN** the packaged procedure's journal is replayed over its inputs by the replay-determinism harness
- **THEN** the rebuilt `{document, version, delivered}` state SHALL equal the live run's state

