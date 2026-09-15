# sdk-graph-authoring Specification

## Purpose
TBD - created by archiving change sdk-graph-authoring-api. Update Purpose after archive.
## Requirements
### Requirement: Producer-first stream-handle graph builder
Every SDK SHALL author operative topologies through a producer-first fluent builder in which `.into(<procedure>)` wires a consumer onto the current stream and returns the **consumed procedure's own output stream handle**. `input(<type>, "name").into(A)` SHALL wire the entry consumer and return A's output stream; `stream.into(B)` SHALL add B as a consumer of `stream`'s channel and return B's output stream, so a linear pipeline is a single chained expression. A procedure node SHALL own **exactly one output channel**, created on first use and reused by every later call on a handle whose source is that node, so wiring a second consumer, attaching a plugin sink or overriding the channel name reaches the same channel rather than declaring another. A declaration in which one procedure produces two channels SHALL NOT be constructible: the runtime resolves a procedure's output channel to a single id, and a second producing channel would be journaled by nothing and re-seeded by nothing on crash-resume. The handle SHALL be a lightweight, copyable reference into the operative's topology builder (Java: an object; Rust: a `Copy` value), and per-channel configuration (`keyed()`, `onFull(...)`, `retryMax(...)`, `retryBackoffMs(...)`, `named(...)`) SHALL remain available on the handle, chainable in any order relative to consumer wiring. Handles SHALL be invalidated when the topology freezes at registration; using one afterwards SHALL fail with an error naming the operative.

The runtime SHALL derive the journal codec for every inter-procedure channel, and no builder in any SDK SHALL offer a method to declare one. An inter-procedure channel of a durable structured workflow class is journalable and re-seedable with no codec declaration and no declaration key.

An SDK whose procedures hand the core **bytes** derives it from the channel's kind, which is what every host over the erased poll core already does. An SDK whose procedures hand the core **typed values** derives it from the producing procedure's declared `Output` type, and SHALL therefore require every `Output` to be writable to bytes and readable back, the way it already requires this of every `State`. That requirement SHALL be unconditional rather than resolved against whether a class journals, because whether a class journals is an operator's setting at deploy time and not a fact available where the code is compiled. A typed SDK SHALL state the requirement in its own vocabulary when a type does not meet it, rather than surfacing the serialization library's message about a trait the author never named.

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

#### Scenario: No SDK declares an inter-procedure codec

- **WHEN** any SDK declares a durable structured workflow class whose procedure A is wired into procedure B
- **THEN** registration SHALL succeed with no codec declared anywhere on the A to B channel, the declaration SHALL carry no codec key, and A's handoff to B SHALL be journaled at the commit barrier and re-seeded on crash-resume from the codec the runtime derived

#### Scenario: A typed SDK refuses an output it cannot write down

- **WHEN** a typed-value SDK declares a procedure whose `Output` type cannot be written to bytes and read back
- **THEN** the program SHALL fail to compile, and the refusal SHALL name the type and state that a procedure's output is journaled at a workflow's commit barrier

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

A channel created by wiring out of a node SHALL be named by that node's identity string (the procedure's `name()`, or the explicit node name per the node-identity requirement) unless a `sink("...")` name or a `.named("...")` override supplies one. Both declarations name the same channel and SHALL be honoured identically wherever the channel name appears: wire metadata, journals, diagnostics, the address a plugin filter matches, and the name a local-mode callback registers under. A channel named by both `sink("...")` and `.named("...")` SHALL fail registration naming both declarations, even when the two strings are equal, so no declaration an author wrote is ever discarded and no precedence rule has to be remembered. `sink()` with no name SHALL leave the channel named exactly as it would be without the sink. Entry channels SHALL keep an explicit, mandatory name (they are the operative's external input address). Derived, sink-supplied, overridden, and entry names SHALL all live in the existing per-operative channel namespace with its duplicate check; a collision SHALL fail registration naming both declarations. Because derived names come from hand-written identity strings, renaming a language class SHALL NOT change any on-disk or on-wire name.

#### Scenario: A sink name is the channel name

- **WHEN** procedure `Validate` (whose `name()` returns `"validate"`) is wired via `.sink("audit_sink")` with no `.named(...)`
- **THEN** the channel SHALL be named `"audit_sink"` and frames delivered to the plugin SHALL carry `channel = "audit_sink"` in their metadata

#### Scenario: An unnamed sink derives as before

- **WHEN** procedure `Validate` (whose `name()` returns `"validate"`) is wired via `.sink()` with no `.named(...)`
- **THEN** the channel SHALL be named `"validate"` and frames delivered to the plugin SHALL carry `channel = "validate"` in their metadata

#### Scenario: named() overrides the derived name

- **WHEN** the declaration chains `.named("orders.validated")` on Validate's output stream
- **THEN** the channel SHALL be recorded and stamped as `"orders.validated"` everywhere the channel name appears (wire metadata, journals, errors)

#### Scenario: named() reaches a plugin terminator

- **WHEN** the declaration chains `.named("input")` and then `.sink()` on one edge
- **THEN** the channel SHALL be named `"input"` and the frame SHALL carry `channel = "input"`, so a forward target resolves its entry channel by the name the author declared

#### Scenario: Naming a channel twice is refused

- **WHEN** the declaration chains `.named("a")` and then `.sink("b")` on one edge
- **THEN** registration SHALL fail with an error naming both declarations
- **AND** the same SHALL hold when both strings are equal

#### Scenario: Name collision is refused

- **WHEN** a `.named(...)` override, a `sink(...)` name, or a node name collides with another channel name in the same operative
- **THEN** registration SHALL fail with an error naming both colliding declarations
- **AND** where the collision is two channels naming one sink, the error SHALL say so rather than reporting a bare duplicate name

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

Plugin egress SHALL be declared by exactly one builder method in every SDK, taking an optional channel name and an optional serializer. Java, Node, Ruby, PHP and Python SHALL spell the four combinations in their own idiom for an optional argument (an overload in Java, an optional parameter in Node, a default argument in Ruby, PHP and Python); Rust, which has neither overloading nor default arguments, SHALL spell them `sink()` / `sink_named(name)` / `sink_with(serializer)` / `sink_named_with(name, serializer)`; Elixir SHALL spell them `sink/1`, `sink/2`, `sink_with/2` and `sink_with/3`, the name defaulting in the middle argument. `sink` SHALL bind the producing procedure's output channel to the plugin egress and SHALL return the stream handle, so inter-procedure consumers MAY be wired after it — a channel is terminal by having no further wiring, not by method choice. An SDK whose `sink` returns nothing SHALL NOT satisfy this requirement, because the hybrid declaration is unwritable in it. A channel carrying a plugin sink and no consumer SHALL be declared to the runtime as plugin-bound; a channel carrying a plugin sink **and** consumers SHALL keep its inter-procedure kind, and the host SHALL release both the consumer edge outputs and one plugin output for each emitted value. A `sink` without a serializer SHALL apply the default serialization rules (passthrough for bytes, UTF-8 for strings, default JSON otherwise). The name a `sink` carries SHALL be the channel's name, which is the metadata the controller routes by; a channel SHALL therefore be named by at most one `sink`, and two channels naming one sink SHALL fail registration as a duplicate channel name. A separate optional `.sealed()` terminator SHALL return a non-chainable handle and add no topology or runtime difference — it is a compile-time statement that no consumer follows, and SHALL exist only in the SDKs whose type system can enforce it. The former `output` / `alsoOutput` / `also_output` / `also_output_with` spellings SHALL NOT exist in any SDK; there SHALL be no coexistence period. Wire frames, descriptor fields, plugin bindings, controller configuration, and error identifiers keep their existing names.

#### Scenario: sink is hybrid by default

- **WHEN** a stream declares `.sink("indicators", serializer)` and a consumer is wired from the same handle afterwards
- **THEN** each emitted value SHALL reach the typed consumer and, serialized once, the plugin layer, stamped `channel = "indicators"`

#### Scenario: Terminal by construction

- **WHEN** a stream declares `.sink("pager_sink")` and nothing further is wired from the handle
- **THEN** the channel SHALL be plugin-bound and named `"pager_sink"`

#### Scenario: The no-name form is available in every SDK

- **WHEN** a stream declares the no-name form in any SDK
- **THEN** the channel SHALL be plugin-bound, its name SHALL be whatever it would have been without the sink, and the declaration SHALL compile or register in every SDK

#### Scenario: sealed refuses further consumers at compile time

- **WHEN** user code attempts to wire a consumer from the handle returned by `.sink("audit").sealed()`
- **THEN** the code SHALL fail to compile (Rust) / fail to type-check (Java) — and the sealed declaration's runtime topology SHALL be byte-identical to the same declaration without `.sealed()`

#### Scenario: Old spellings do not exist

- **WHEN** user code calls `.output(...)`, `.alsoOutput(...)`, or `.also_output_with(...)` against the new SDK
- **THEN** the code SHALL fail to compile; no transitional alias SHALL exist

#### Scenario: Every SDK stamps one name for one declaration

- **WHEN** the same operative declaring `.sink("indicators", s)` is registered from a Rust host and from a host SDK
- **THEN** frames delivered to the plugin SHALL be byte-identical, `channel = "indicators"` included, so the two naming sources cannot answer differently

#### Scenario: A hybrid channel declared in a veneer SDK keeps one channel

- **WHEN** a Node, Ruby, PHP or Python operative declares `.sink("mid", enc).into(Next)` on one handle
- **THEN** the declaration SHALL carry exactly one channel produced by that procedure, named `mid`, of inter-procedure kind, listing `Next` as its consumer, and each emitted value SHALL reach both `Next` and the plugin layer

### Requirement: The channel-declaration API is replaced with no coexistence
`ChannelDecl`, `.from(...)`, and the typed-and-named inter-procedure `channel(<type>, "name")` declaration SHALL be removed from both SDKs' public surface in the same change that introduces the graph builder. There SHALL be no transitional period in which both APIs are usable.

#### Scenario: Old spellings do not compile

- **WHEN** user code declares `c.channel(String.class, "mid").from(Stage1.class)` (Java) or `c.channel::<String>("mid").from::<Stage1>()` (Rust) against the new SDK
- **THEN** the code SHALL fail to compile — the methods do not exist

### Requirement: One contract across SDKs, per-language spelling allowed
The graph builder's semantics — handle model, one output channel per producing procedure, branching, node identity, DAG rule, fan-in accumulation, derived names, typed inputs, `sink` vocabulary — SHALL be identical in every SDK, and the shared e2e matrix SHALL exercise one contract with no per-runtime branching. Per-language surface differences SHALL be limited to spelling (`Proc.class` vs turbofish vs a first-class procedure value, lambdas vs closures vs blocks, `into(Proc.class, "name")` vs `into_named::<Proc>("name")`) and to the one surface a language cannot express, a compile-time `sealed()` terminator. Where Rust enforces a rule at compile time, every other SDK SHALL enforce the same rule at registration with an error carrying the same diagnostic content.

#### Scenario: Same graph, both SDKs, same behavior

- **WHEN** the shared e2e branching fixture topology is authored in both SDKs with the new builder
- **THEN** both runtimes SHALL produce the same sink outputs, journal shapes, and compensation order for the same scenario script

#### Scenario: One fan-out declaration, one topology, every SDK

- **WHEN** the same fan-out is authored in each SDK — a held handle wired into two procedures, and again as the equivalent `branch(...)` sugar
- **THEN** every SDK's declaration SHALL carry one channel produced by that procedure listing both consumers, with the same derived channel name, and the two spellings SHALL produce identical declarations within each SDK

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

### Requirement: Named payload codecs in the SDKs whose channels carry no type
The Ruby, Python, Node and PHP SDKs SHALL accept a named codec on `input` and on `sink`, from the vocabulary `bytes`, `text` and `json`, plus `msgpack` in the Ruby, Python and Node SDKs. `bytes` SHALL deliver and send bytes unchanged; `text` SHALL decode UTF-8 into a string and encode a string as UTF-8; `json` SHALL decode JSON and encode any value as JSON, a string included; `msgpack` SHALL decode and encode with the SDK's own MessagePack codec. Each SDK SHALL spell the codec the way it spells a named policy: a symbol in Ruby (`codec: :text`), a string in Python (`codec="text"`), a string in an options object in Node (`{ codec: 'text' }`), and a case of the backed enum `ChannelCodec` in PHP (`codec: ChannelCodec::Text`), whose cases SHALL be exactly `Bytes`, `Text` and `Json`.

An entry channel declared with no codec and no decoder SHALL decode JSON. A sink declared with no codec and no encoder SHALL apply the default serialization rules of the `sink` requirement: bytes unchanged, a string as UTF-8, any other value as JSON.

An unknown codec name, or a codec given together with a decoder or an encoder, SHALL be refused when the operative is declared, with the SDK's declaration error naming the operative and the channel. Under `text`, a payload that is not valid UTF-8 SHALL be a decode failure riding the entry channel's retry and dead-letter ladder, and a value that is not a string SHALL be an encode failure naming the channel. Under `bytes`, a value that is not bytes SHALL be an encode failure naming the channel.

The Java, Rust and Elixir SDKs carry the same formats without a codec argument: Java and Rust choose `bytes`, `text` or `json` from the declared type, and Elixir's `input/2` delivers the binary unchanged while `input_with/3` takes any other decoder.

#### Scenario: A text channel is one word

- **WHEN** a Ruby operative declares `c.input("in", codec: :text).into(Echo).sink("out")`, the bytes `hello` arrive on `in`, and the procedure returns `"echo: hello"`
- **THEN** the procedure SHALL receive the string `hello`, and the plugin frame SHALL carry the bytes `echo: hello`, identical to what a Java operative declaring `c.input(String.class, "in")` and `.sink("out")` sends for the same output

#### Scenario: A sink call is its name alone in every dynamic SDK

- **WHEN** a Ruby, Python, Node or PHP operative declares its sink as `sink("out")` with no codec and no encoder, and the procedure returns the string `echo: hello`
- **THEN** registration SHALL succeed and the plugin frame SHALL carry the bytes `echo: hello`, with no quotes and no MessagePack header, in each of the four SDKs

#### Scenario: An undeclared entry reads JSON in every dynamic SDK

- **WHEN** a Ruby, Python, Node or PHP operative declares an entry with no codec and no decoder, and the bytes `{"a":1}` arrive on it
- **THEN** the procedure SHALL receive a map whose key `a` holds the number 1

#### Scenario: Plain text into an undeclared entry fails its decode

- **WHEN** the bytes `hello` arrive on an entry declared with no codec and no decoder
- **THEN** the decode SHALL fail and ride the entry channel's retry and dead-letter ladder, and the procedure SHALL NOT be invoked with the payload

#### Scenario: An undeclared sink follows the value

- **WHEN** a procedure emits a string, a byte value and a map to a sink declared with no codec and no encoder
- **THEN** the plugin frames SHALL carry the string's UTF-8 bytes, the byte value unchanged, and the map as JSON, in each of the four SDKs

#### Scenario: A json sink quotes a string

- **WHEN** a procedure emits the string `hi` to a sink declared with codec `json`
- **THEN** the plugin frame SHALL carry the four bytes `"hi"`, quotes included

#### Scenario: MessagePack stays one word away where it ships

- **WHEN** a Ruby, Python or Node operative declares an entry and a sink with codec `msgpack`
- **THEN** the entry SHALL decode and the sink SHALL encode with that SDK's MessagePack codec, byte-identical to that SDK's class codec

#### Scenario: An unknown codec name is refused at declaration

- **WHEN** a Ruby, Python or Node operative declares an entry named `in` with the codec name `yaml`
- **THEN** declaring the operative SHALL raise the SDK's declaration error naming the operative, the channel `in`, the name `yaml` and the accepted names, before any message is processed

#### Scenario: A codec and a callable together are refused

- **WHEN** an operative in any of the four SDKs declares an entry with both a codec and a decoder, or a sink with both a codec and an encoder
- **THEN** declaring the operative SHALL raise the SDK's declaration error naming the operative and the channel

#### Scenario: Invalid UTF-8 under text is a failure, not a replacement

- **WHEN** bytes that are not valid UTF-8 arrive on an entry declared with codec `text`
- **THEN** the decode SHALL fail and ride the entry channel's retry and dead-letter ladder, and no string carrying replacement characters SHALL reach the procedure

#### Scenario: A text sink refuses a value that is not a string

- **WHEN** a procedure emits the number 42 to a sink declared with codec `text`
- **THEN** the invocation SHALL fail with an error naming the channel, and no plugin frame SHALL carry the value

#### Scenario: A bytes sink refuses a value that is not bytes

- **WHEN** a procedure emits the number 42 to a sink declared with codec `bytes`
- **THEN** the invocation SHALL fail with an error naming the channel, and no plugin frame SHALL carry the value; a Ruby or PHP string, which is the byte value in those languages, SHALL pass through unchanged

