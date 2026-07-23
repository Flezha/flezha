# sdk-graph-authoring Specification

## Purpose
TBD - created by archiving change sdk-graph-authoring-api. Update Purpose after archive.
## Requirements
### Requirement: Producer-first stream-handle graph builder

Both SDKs SHALL author operative topologies through a producer-first fluent builder in which `.into(<operation>)` wires a consumer onto the current stream and returns the **consumed operation's own output stream handle**. `input(<type>, "name").into(A)` SHALL wire the entry consumer and return A's output stream; `stream.into(B)` SHALL add B as a consumer of `stream`'s channel and return B's output stream, so a linear pipeline is a single chained expression. The handle SHALL be a lightweight, copyable reference into the operative's topology builder (Java: an object; Rust: a `Copy` value), and per-channel configuration (`codec()`, `keyed()`, `onFull(...)`, `retryMax(...)`, `retryBackoffMs(...)`, `named(...)`) SHALL remain available on the handle, chainable in any order relative to consumer wiring. Handles SHALL be invalidated when the topology freezes at registration; using one afterwards SHALL fail with an error naming the operative.

The payload type of every non-entry channel SHALL be taken from the producing operation's declared `Output` type — the builder SHALL NOT accept a redundant type token for inter-operation channels. Wiring a consumer whose `Input` type differs from the stream's payload type SHALL fail to compile in Rust and SHALL fail registration in Java with an error naming both operations and both types.

#### Scenario: Linear chain is one expression

- **WHEN** an operative declares `c.input(OrderEvent.class, "orders").into(Validate.class).into(Alert.class).output("alerts")` (Java) or the equivalent `c.input::<OrderEvent>("orders").into::<Validate>().into::<Alert>().output("alerts")` (Rust)
- **THEN** registration SHALL succeed with the topology entry→Validate→Alert→sink, and the channel between Validate and Alert SHALL carry Validate's declared `Output` type with no type token in the declaration

#### Scenario: Type-mismatched consumer is refused

- **WHEN** `stream.into(B)` is declared where B's `Input` type differs from the stream's payload type
- **THEN** the program SHALL fail to compile (Rust) or registration SHALL throw naming both operations and both types (Java)

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

A channel created by wiring out of a node SHALL be named by that node's identity string (the operation's `name()`, or the explicit node name per the node-identity requirement) unless `.named("...")` overrides it. Entry channels SHALL keep an explicit, mandatory name (they are the operative's external input address). Derived, overridden, and entry names SHALL all live in the existing per-operative channel namespace with its duplicate check; a collision SHALL fail registration naming both declarations. Because derived names come from hand-written identity strings, renaming a language class SHALL NOT change any on-disk or on-wire name.

#### Scenario: Derived name ships on the wire

- **WHEN** operation `Validate` (whose `name()` returns `"validate"`) is wired via `.alsoOutput("audit_sink")` with no `.named(...)` override
- **THEN** frames delivered to the `audit_sink` plugin SHALL carry `channel = "validate"` in their metadata

#### Scenario: named() overrides the derived name

- **WHEN** the declaration chains `.named("orders.validated")` on Validate's output stream
- **THEN** the channel SHALL be recorded and stamped as `"orders.validated"` everywhere the channel name appears (wire metadata, journals, errors)

#### Scenario: Name collision is refused

- **WHEN** a `.named(...)` override or node name collides with another channel name in the same operative
- **THEN** registration SHALL fail with an error naming both colliding declarations

### Requirement: Node identity permits reusing one Operation class at several graph positions

A graph node SHALL be identified by its Operation class unless an explicit node name is supplied at wiring time (`.into(Filter.class, "spam_filter")` in Java, `into_named::<Filter>("spam_filter")` in Rust). Two unnamed mentions of the same class SHALL resolve to the same node. An explicit node name SHALL create a distinct node whose identity string replaces the operation's `name()` for every purpose identity serves today: per-segment state directories, journal entries, derived channel names, and diagnostics. Node identity strings SHALL be unique per operative; a collision (with another node name or an operation `name()`) SHALL fail registration.

#### Scenario: Same class reused as two named nodes

- **WHEN** an operative declares `validated.into(Filter.class, "fraud_filter")` and `validated.into(Filter.class, "spam_filter")`
- **THEN** registration SHALL succeed with two distinct nodes, and each node's segments SHALL persist state under its own identity (`fraud_filter` / `spam_filter`)

#### Scenario: Unnamed mentions unify

- **WHEN** two different streams each declare `.into(Merge.class)` with no node name
- **THEN** both SHALL wire to one Merge node (a fan-in), not two

#### Scenario: Node-name collision is refused

- **WHEN** a node name equals another node's identity string in the same operative
- **THEN** registration SHALL fail naming both nodes

### Requirement: Graphs are DAGs; fan-in accumulates in both SDKs

The shared graph-shape rule for both SDKs SHALL be: one **or more** entry channels feeding the graph, every node reachable from some entry channel, at most one output channel per node, any number of incoming channels per node, and no cycles. Multiple entry channels let a single operative ingest several external sources (e.g. a market pipeline consuming a trade feed and an order-book feed on distinct channels); the controller routes each inbound frame to its entry channel by the `metadata["channel"]` name, falling back to the first-declared entry channel when that metadata is absent. Validation SHALL use a topological ordering (all of a node's producers precede it); a cycle SHALL be rejected naming a node on it. A node with several incoming channels (fan-in) SHALL receive the **accumulated** contributions of all its producers — no producer's output may be overwritten or dropped. In bounded operatives a fan-in node SHALL be dispatched only after every one of its producers has completed and committed, and its input batch SHALL be the concatenation of all producers' released outputs. In unbounded operatives the existing per-channel dispatch SHALL deliver both producers' values to the shared consumer as they arrive.

#### Scenario: Bounded join runs once with both contributions

- **WHEN** a bounded graph A→(B,C)→D runs to completion
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
- **THEN** the consuming operation's `run` SHALL receive the deserialized `OrderEvent`

#### Scenario: Custom parser overload

- **WHEN** an operative declares `c.input(OrderEvent.class, "orders", bytes -> OrderEvent.parse(bytes))`
- **THEN** the supplied parser SHALL produce the value delivered to the consumer, and the default JSON codec SHALL NOT run

#### Scenario: Parse failure parks to the DLQ

- **WHEN** an arriving payload fails the entry parser on every retry attempt of the channel's retry budget
- **THEN** the payload SHALL be parked to the dead-letter queue under the entry channel's origin identity, and the consuming operation SHALL NOT be invoked with a partial value

#### Scenario: String and bytes entries are byte-identical to today

- **WHEN** an operative declares a `String` or `byte[]` entry channel
- **THEN** delivery behavior (passthrough / UTF-8 decode, invalid-UTF-8 drop-and-log) SHALL be unchanged from the pre-change contract

### Requirement: Destination-neutral `output` / `alsoOutput` vocabulary

The plugin-bound terminal SHALL be `output(name[, serializer])` (Rust: `output`) and the hybrid tap SHALL be `alsoOutput(name[, serializer])` (Rust: `also_output` / `also_output_with`). `alsoOutput(name)` without a serializer SHALL use the same default serialization rules as `output` (passthrough for bytes, UTF-8 for strings, JSON otherwise). The former `toPlugin` / `to_plugin` / `alsoToPluginWith` / `also_to_plugin_with` method names SHALL NOT exist. The rename SHALL be scoped to user-called builder methods: wire frames, descriptor fields, plugin bindings, controller configuration, and error identifiers keep their existing names.

#### Scenario: output replaces toPlugin

- **WHEN** a stream declares `.output("pager_sink")`
- **THEN** the channel SHALL be plugin-bound to `pager_sink` with behavior identical to the former `toPlugin("pager_sink")`, and no `toPlugin`/`to_plugin` method SHALL exist on any builder type

#### Scenario: alsoOutput without a serializer uses defaults

- **WHEN** a stream of a JSON-serializable type declares `.alsoOutput("audit_sink")` with no serializer
- **THEN** the tap SHALL publish the default-JSON bytes of each emitted value to `audit_sink`, and inter-operation consumers SHALL receive the typed values unchanged

### Requirement: The channel-declaration API is replaced with no coexistence

`ChannelDecl`, `.from(...)`, and the typed-and-named inter-operation `channel(<type>, "name")` declaration SHALL be removed from both SDKs' public surface in the same change that introduces the graph builder. There SHALL be no transitional period in which both APIs are usable.

#### Scenario: Old spellings do not compile

- **WHEN** user code declares `c.channel(String.class, "mid").from(Stage1.class)` (Java) or `c.channel::<String>("mid").from::<Stage1>()` (Rust) against the new SDK
- **THEN** the code SHALL fail to compile — the methods do not exist

### Requirement: One contract across SDKs, per-language spelling allowed

The graph builder's semantics — handle model, branching, node identity, DAG rule, fan-in accumulation, derived names, typed inputs, `output` vocabulary — SHALL be identical in Rust and Java, and the shared e2e matrix SHALL exercise one contract with no per-runtime branching. Per-language surface differences SHALL be limited to spelling (`Op.class` vs turbofish, lambdas vs closures, `into(Op.class, "name")` vs `into_named::<Op>("name")`). Where Rust enforces a rule at compile time, Java SHALL enforce the same rule at registration with an error carrying the same diagnostic content.

#### Scenario: Same graph, both SDKs, same behavior

- **WHEN** the shared e2e branching fixture topology is authored in both SDKs with the new builder
- **THEN** both runtimes SHALL produce the same sink outputs, journal shapes, and compensation order for the same scenario script

