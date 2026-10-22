# sdk-graph-authoring Specification

## Purpose
TBD - created by archiving change sdk-graph-authoring-api. Update Purpose after archive.
## Requirements
### Requirement: Producer-first stream-handle graph builder
Every SDK SHALL author relay topologies through a producer-first fluent builder in which `.into(<tasklet>)` wires a consumer onto the current stream and returns the **consumed tasklet's own output stream handle**. `input(<type>, "name").into(A)` SHALL wire the entry consumer and return A's output stream; `stream.into(B)` SHALL add B as a consumer of `stream`'s channel and return B's output stream, so a linear pipeline is a single chained expression. A tasklet node SHALL own **exactly one output channel**, created on first use and reused by every later call on a handle whose source is that node, so wiring a second consumer, attaching a plugin output or overriding the channel name reaches the same channel rather than declaring another. A declaration in which one tasklet produces two channels SHALL NOT be constructible: the runtime resolves a tasklet's output channel to a single id, and a second producing channel would be journaled by nothing and re-seeded by nothing on crash-resume. The handle SHALL be a lightweight, copyable reference into the relay's topology builder (Java: an object; Rust: a `Copy` value), and per-channel configuration (`keyed()`, `onFull(...)`, `retryMax(...)`, `retryBackoffMs(...)`, `named(...)`) SHALL remain available on the handle, chainable in any order relative to consumer wiring. Handles SHALL be invalidated when the topology freezes at registration; using one afterwards SHALL fail with an error naming the relay.

The runtime SHALL derive the journal codec for every inter-tasklet channel, and no builder in any SDK SHALL offer a method to declare one. An inter-tasklet channel of a durable structured steps class is journalable and re-seedable with no codec declaration and no declaration key.

An SDK whose tasklets hand the core **bytes** derives it from the channel's kind, which is what every host over the erased poll core already does. An SDK whose tasklets hand the core **typed values** derives it from the producing tasklet's declared `Output` type, and SHALL therefore require every `Output` to be writable to bytes and readable back, the way it already requires this of every `State`. That requirement SHALL be unconditional rather than resolved against whether a class journals, because whether a class journals is a user's setting at deploy time and not a fact available where the code is compiled. A typed SDK SHALL state the requirement in its own vocabulary when a type does not meet it, rather than surfacing the serialization library's message about a trait the author never named.

A chain MAY additionally carry **edge transforms** (`map`, `filter`, `flat_map` / `flatMap`) and **edge keying** (`key_by` / `keyBy`) between one `.into(...)` and the next. These SHALL return a stream handle of the same kind, SHALL NOT introduce a Tasklet node into the topology, and SHALL be stored on the consumer edge the chain terminates in. Both the entry-stream handle and the output-stream handle SHALL expose the same transform, keying, and per-channel configuration surface; the two handles SHALL NOT drift.

The payload type of every non-entry channel SHALL be taken from the producing tasklet's declared `Output` type, as reshaped by any edge transforms declared after it — the builder SHALL NOT accept a redundant type token for inter-tasklet channels, except where Java erasure prevents inferring a transform's result type. Wiring a consumer whose `Input` type differs from the stream's payload type SHALL fail to compile in Rust and SHALL fail registration in Java with an error naming both tasklets and both types.

#### Scenario: Linear chain is one expression

- **WHEN** a relay declares `c.input(OrderEvent.class, "orders").into(Validate.class).into(Alert.class).output("alerts")` (Java) or the equivalent `c.input::<OrderEvent>("orders").into::<Validate>().into::<Alert>().output("alerts")` (Rust)
- **THEN** registration SHALL succeed with the topology entry→Validate→Alert→output, and the channel between Validate and Alert SHALL carry Validate's declared `Output` type with no type token in the declaration

#### Scenario: Transforms sit between consumers without adding nodes

- **WHEN** a relay declares `c.input(Trade.class, "trades").filter(t -> t.qty() > 1.0).keyBy(Trade::symbol).into(Candle.class).output("candles")`
- **THEN** registration SHALL succeed with a topology containing exactly one Tasklet node, and the entry→Candle edge SHALL carry the predicate and the key function

#### Scenario: Type-mismatched consumer is refused

- **WHEN** `stream.into(B)` is declared where B's `Input` type differs from the stream's payload type
- **THEN** the program SHALL fail to compile (Rust) or registration SHALL throw naming both tasklets and both types (Java)

#### Scenario: Both handles carry the same surface

- **WHEN** a transform or keying method is declared on an entry-stream handle and on an output-stream handle
- **THEN** both SHALL accept it with identical semantics, and neither handle SHALL offer a transform the other lacks

#### Scenario: Handle used after freeze is refused

- **WHEN** user code retains a stream handle and calls `.into(...)` on it after the relay's topology has been frozen at registration
- **THEN** the call SHALL fail with an error naming the relay and stating the topology is frozen

#### Scenario: No SDK declares an inter-tasklet codec

- **WHEN** any SDK declares a durable structured steps class whose tasklet A is wired into tasklet B
- **THEN** registration SHALL succeed with no codec declared anywhere on the A to B channel, the declaration SHALL carry no codec key, and A's handoff to B SHALL be journaled at the commit barrier and re-seeded on crash-resume from the codec the runtime derived

#### Scenario: A typed SDK refuses an output it cannot write down

- **WHEN** a typed-value SDK declares a tasklet whose `Output` type cannot be written to bytes and read back
- **THEN** the program SHALL fail to compile, and the refusal SHALL name the type and state that a tasklet's output is journaled at a steps relay's commit barrier

### Requirement: Branching is handle reuse; `branch()` is equivalent sugar
Using one stream handle to wire more than one consumer SHALL declare fan-out: every consumer receives every value released on that stream's channel. The builder SHALL also expose `branch(arm1, arm2, ...)` taking two or more lambdas, each invoked with a copy of the same handle; `branch` SHALL be pure sugar — the resulting topology SHALL be identical to the equivalent handle-reuse declaration, and the two spellings SHALL be freely mixable.

#### Scenario: Handle reuse fans out

- **WHEN** a relay holds `validated` (a stream handle) and declares `validated.into(FraudCheck)` and `validated.into(Receipt)`
- **THEN** both FraudCheck and Receipt SHALL be consumers of the same channel and each SHALL receive every value the producer releases on it

#### Scenario: branch() builds the identical topology

- **WHEN** the same graph is declared once via two `validated.into(...)` statements and once via `validated.branch(b -> b.into(FraudCheck), b -> b.into(Receipt))`
- **THEN** the two frozen topologies SHALL be identical (same channels, consumers, and derived names)

#### Scenario: branch() with fewer than two arms is refused

- **WHEN** `branch(...)` is called with zero or one lambda
- **THEN** registration SHALL fail with an error stating branch requires at least two arms

### Requirement: Channel names auto-derive from the producer node's identity

A channel created by wiring out of a node SHALL be named by that node's identity string (the tasklet's `name()`, or the explicit node name per the node-identity requirement) unless an `output("...")` name or a `.named("...")` override supplies one. Both declarations name the same channel and SHALL be honoured identically wherever the channel name appears: wire metadata, journals, diagnostics, the address a plugin filter matches, and the name a local-mode callback registers under. A channel named by both `output("...")` and `.named("...")` SHALL fail registration naming both declarations, even when the two strings are equal, so no declaration an author wrote is ever discarded and no precedence rule has to be remembered. `output()` with no name SHALL leave the channel named exactly as it would be without the output. Entry channels SHALL keep an explicit, mandatory name (they are the relay's external input address). Derived, output-supplied, overridden, and entry names SHALL all live in the existing per-relay channel namespace with its duplicate check; a collision SHALL fail registration naming both declarations. Because derived names come from hand-written identity strings, renaming a language class SHALL NOT change any on-disk or on-wire name.

#### Scenario: An output name is the channel name

- **WHEN** tasklet `Validate` (whose `name()` returns `"validate"`) is wired via `.output("audit_sink")` with no `.named(...)`
- **THEN** the channel SHALL be named `"audit_sink"` and frames delivered to the plugin SHALL carry `channel = "audit_sink"` in their metadata

#### Scenario: An unnamed output derives as before

- **WHEN** tasklet `Validate` (whose `name()` returns `"validate"`) is wired via `.output()` with no `.named(...)`
- **THEN** the channel SHALL be named `"validate"` and frames delivered to the plugin SHALL carry `channel = "validate"` in their metadata

#### Scenario: named() overrides the derived name

- **WHEN** the declaration chains `.named("orders.validated")` on Validate's output stream
- **THEN** the channel SHALL be recorded and stamped as `"orders.validated"` everywhere the channel name appears (wire metadata, journals, errors)

#### Scenario: named() reaches a plugin terminator

- **WHEN** the declaration chains `.named("input")` and then `.output()` on one edge
- **THEN** the channel SHALL be named `"input"` and the frame SHALL carry `channel = "input"`, so a forward target resolves its entry channel by the name the author declared

#### Scenario: Naming a channel twice is refused

- **WHEN** the declaration chains `.named("a")` and then `.output("b")` on one edge
- **THEN** registration SHALL fail with an error naming both declarations
- **AND** the same SHALL hold when both strings are equal

#### Scenario: Name collision is refused

- **WHEN** a `.named(...)` override, an `output(...)` name, or a node name collides with another channel name in the same relay
- **THEN** registration SHALL fail with an error naming both colliding declarations
- **AND** where the collision is two channels naming one output, the error SHALL say so rather than reporting a bare duplicate name

### Requirement: Node identity permits reusing one Tasklet class at several graph positions
A graph node SHALL be identified by its Tasklet class unless an explicit node name is supplied at wiring time (`.into(Filter.class, "spam_filter")` in Java, `into_named::<Filter>("spam_filter")` in Rust). Two unnamed mentions of the same class SHALL resolve to the same node. An explicit node name SHALL create a distinct node whose identity string replaces the tasklet's `name()` for every purpose identity serves today: per-cell state directories, journal entries, derived channel names, and diagnostics. Node identity strings SHALL be unique per relay; a collision (with another node name or a tasklet `name()`) SHALL fail registration.

#### Scenario: Same class reused as two named nodes

- **WHEN** a relay declares `validated.into(Filter.class, "fraud_filter")` and `validated.into(Filter.class, "spam_filter")`
- **THEN** registration SHALL succeed with two distinct nodes, and each node's cells SHALL persist state under its own identity (`fraud_filter` / `spam_filter`)

#### Scenario: Unnamed mentions unify

- **WHEN** two different streams each declare `.into(Merge.class)` with no node name
- **THEN** both SHALL wire to one Merge node (a fan-in), not two

#### Scenario: Node-name collision is refused

- **WHEN** a node name equals another node's identity string in the same relay
- **THEN** registration SHALL fail naming both nodes

### Requirement: Graphs are DAGs; fan-in accumulates in both SDKs
The shared graph-shape rule for both kinds SHALL be: one **or more** entry channels feeding the graph, every node reachable from some entry channel, at most one output channel per node, any number of incoming channels per node, and no cycles. Multiple entry channels let a single relay ingest several external sources (e.g. a market pipeline consuming a trade feed and an order-book feed on distinct channels); the controller routes each inbound frame to its entry channel by the `metadata["channel"]` name, falling back to the first-declared entry channel when that metadata is absent. An entry channel MAY feed more than one tasklet. Validation SHALL use a topological ordering (all of a node's producers precede it); a cycle SHALL be rejected naming a node on it. A node with several incoming channels (fan-in) SHALL receive the **accumulated** contributions of all its producers — no producer's output may be overwritten or dropped. In steps relays each entry channel roots its own chain, the tasklets reachable from that channel's consumers; a message runs only the chain of the channel it arrived on, and a fan-in node SHALL be dispatched only after every one of its producers **in that pass's chain** has completed and committed, with an input batch that is the concatenation of those producers' released outputs. A producer of another chain SHALL NOT be waited for, live or on a crash-resume. In flow relays the existing per-channel dispatch SHALL deliver both producers' values to the shared consumer as they arrive. No registration refusal SHALL exist for an entry channel feeding several tasklets, for several entry channels on a steps relay, or for an entry channel as a merge source on a steps relay.

#### Scenario: Steps relay join runs once with both contributions

- **WHEN** a steps graph A→(B,C)→D runs to completion
- **THEN** D SHALL be dispatched exactly once, after both B and C committed, with an input batch containing both B's and C's released outputs

#### Scenario: A join where two chains meet waits only for this pass's producer

- **WHEN** a steps relay declares entry `a` feeding A→J and entry `b` feeding B→J, and a message arrives on `a`
- **THEN** J SHALL be dispatched once A committed, with A's released output only, and B SHALL NOT run in that pass

#### Scenario: An entry channel fans out to two roots

- **WHEN** a steps relay declares entry channel `order` feeding both `reserve` and `audit`
- **THEN** a message on `order` SHALL be delivered to both, each through its own edge's transforms, in the same first level of the pass

#### Scenario: Cycle is refused

- **WHEN** a declaration wires a node's output (transitively) back into itself
- **THEN** registration SHALL fail with an error identifying the cycle

#### Scenario: Rust accepts what Java accepts

- **WHEN** the same fan-in topology is declared in the Rust SDK and the Java SDK
- **THEN** both SHALL accept it with identical accumulate semantics — the former Rust fan-in rejection SHALL NOT exist

### Requirement: Typed entry inputs with default and custom deserialization
`input(<type>, "name")` SHALL accept any payload type. `byte[]`/`Vec<u8>` SHALL be delivered unchanged and `String` UTF-8-decoded, byte-identical to current behavior. Any other type SHALL be deserialized at the entry edge from the wire bytes using the SDK's default JSON codec (Jackson in Java; `serde_json` behind a `DeserializeOwned` bound in Rust), with an overload/variant accepting a custom `bytes → T` parser. The parser SHALL run before dispatch on the consumer path: a parse failure SHALL be classified like a failing invocation and ride the entry channel's existing retry→DLQ ladder under the channel's origin identity. The parser SHALL be deterministic (same bytes → same value; no ambient reads), the same contract output serializers carry.

#### Scenario: Typed entry with default JSON

- **WHEN** a relay declares `c.input(OrderEvent.class, "orders")` and a JSON-encoded OrderEvent arrives on the wire
- **THEN** the consuming tasklet's `run` SHALL receive the deserialized `OrderEvent`

#### Scenario: Custom parser overload

- **WHEN** a relay declares `c.input(OrderEvent.class, "orders", bytes -> OrderEvent.parse(bytes))`
- **THEN** the supplied parser SHALL produce the value delivered to the consumer, and the default JSON codec SHALL NOT run

#### Scenario: Parse failure parks to the DLQ

- **WHEN** an arriving payload fails the entry parser on every retry attempt of the channel's retry budget
- **THEN** the payload SHALL be parked to the dead-letter queue under the entry channel's origin identity, and the consuming tasklet SHALL NOT be invoked with a partial value

#### Scenario: String and bytes entries are byte-identical to today

- **WHEN** a relay declares a `String` or `byte[]` entry channel
- **THEN** delivery behavior (passthrough / UTF-8 decode, invalid-UTF-8 drop-and-log) SHALL be unchanged from the pre-change contract

### Requirement: The channel-declaration API is replaced with no coexistence
`ChannelDecl`, `.from(...)`, and the typed-and-named inter-tasklet `channel(<type>, "name")` declaration SHALL be removed from both SDKs' public surface in the same change that introduces the graph builder. There SHALL be no transitional period in which both APIs are usable.

#### Scenario: Old spellings do not compile

- **WHEN** user code declares `c.channel(String.class, "mid").from(Stage1.class)` (Java) or `c.channel::<String>("mid").from::<Stage1>()` (Rust) against the new SDK
- **THEN** the code SHALL fail to compile — the methods do not exist

### Requirement: One contract across SDKs, per-language spelling allowed
The graph builder's semantics — handle model, one output channel per producing tasklet, branching, node identity, DAG rule, fan-in accumulation, derived names, typed inputs, `output` vocabulary — SHALL be identical in every SDK, and the shared e2e matrix SHALL exercise one contract with no per-runtime branching. Per-language surface differences SHALL be limited to spelling (`Task.class` vs turbofish vs a first-class tasklet value, lambdas vs closures vs blocks, `into(Task.class, "name")` vs `into_named::<Task>("name")`). Where Rust enforces a rule at compile time, every other SDK SHALL enforce the same rule at registration with an error carrying the same diagnostic content.

#### Scenario: Same graph, both SDKs, same behavior

- **WHEN** the shared e2e branching fixture topology is authored in both SDKs with the new builder
- **THEN** both runtimes SHALL produce the same outputs, journal shapes, and compensation order for the same scenario script

#### Scenario: One fan-out declaration, one topology, every SDK

- **WHEN** the same fan-out is authored in each SDK — a held handle wired into two tasklets, and again as the equivalent `branch(...)` sugar
- **THEN** every SDK's declaration SHALL carry one channel produced by that tasklet listing both consumers, with the same derived channel name, and the two spellings SHALL produce identical declarations within each SDK

### Requirement: Latest-document router building block

Every SDK's graph builder SHALL offer the latest-document router — `route_latest(document_stream, traffic_stream, key_fn)` in Rust, Python and Ruby, `route_latest/4` on `Flezha.SDK.ChannelBuilder` in Elixir, `routeLatest(documentStream, trafficStream, keyFn)` in Java, Node and PHP — and a `_named` spelling (`route_latest_named` / `routeLatestNamed` / `route_latest_named/5`) that takes a node name, expanding to a single-cell SDK-packaged tasklet that turns a relay-wide document stream into per-key keyed deliveries. Its journaled state SHALL be `{latest document, version, delivered: key → last delivered version}`. A received document byte-identical to the retained one SHALL be inert; a differing document SHALL replace it and increment the version. On each traffic item, the tasklet SHALL emit the retained document keyed under `key_fn(item)` if and only if that key's delivered version is behind the current version, then record the delivery — so each key receives each distinct document version exactly once, always on that key's own traffic, and keys never observed in traffic receive nothing. The expansion SHALL use only existing primitives (merge, single-cell tasklet, journaled state, keyed emit): no new runtime semantics, wire format, or replay path. State mutations SHALL ride the standard change journal and satisfy the replay-determinism harness.

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

- **WHEN** the packaged tasklet's journal is replayed over its inputs by the replay-determinism harness
- **THEN** the rebuilt `{document, version, delivered}` state SHALL equal the live run's state

#### Scenario: Every SDK packages the same router

- **WHEN** the block is declared in any SDK and its tasklet test harness (or, for Elixir, its unit test) feeds one document, then traffic items for keys `A`, `B` and `A`, then the same document again, then an item for `A`
- **THEN** the router SHALL emit `(A, document)` and `(B, document)` exactly once each and nothing on the fifth item, its node identity SHALL be `route_latest`, and its journaled state SHALL be the reference's `{document, version, delivered}` with `version` at 1

### Requirement: Named payload codecs in the SDKs whose channels carry no type
The Ruby, Python, Node and PHP SDKs SHALL accept a named codec on `input` and on `output`, from the vocabulary `bytes`, `text` and `json`, plus `msgpack` in the Ruby, Python and Node SDKs. `bytes` SHALL deliver and send bytes unchanged; `text` SHALL decode UTF-8 into a string and encode a string as UTF-8; `json` SHALL decode JSON and encode any value as JSON, a string included; `msgpack` SHALL decode and encode with the SDK's own MessagePack codec. Each SDK SHALL spell the codec the way it spells a named policy: a symbol in Ruby (`codec: :text`), a string in Python (`codec="text"`), a string in an options object in Node (`{ codec: 'text' }`), and a case of the backed enum `ChannelCodec` in PHP (`codec: ChannelCodec::Text`), whose cases SHALL be exactly `Bytes`, `Text` and `Json`.

An entry channel declared with no codec and no decoder SHALL decode JSON. An output declared with no codec and no encoder SHALL apply the default serialization rules of the `output` requirement: bytes unchanged, a string as UTF-8, any other value as JSON.

An unknown codec name, or a codec given together with a decoder or an encoder, SHALL be refused when the relay is declared, with the SDK's declaration error naming the relay and the channel. Under `text`, a payload that is not valid UTF-8 SHALL be a decode failure riding the entry channel's retry and dead-letter ladder, and a value that is not a string SHALL be an encode failure naming the channel. Under `bytes`, a value that is not bytes SHALL be an encode failure naming the channel.

The Java, Rust and Elixir SDKs carry the same formats without a codec argument: Java and Rust choose `bytes`, `text` or `json` from the declared type, and Elixir's `input/2` delivers the binary unchanged while `input_with/3` takes any other decoder.

#### Scenario: A text channel is one word

- **WHEN** a Ruby relay declares `c.input("in", codec: :text).into(Echo).output("out")`, the bytes `hello` arrive on `in`, and the tasklet emits `"echo: hello"`
- **THEN** the tasklet SHALL receive the string `hello`, and the plugin frame SHALL carry the bytes `echo: hello`, identical to what a Java relay declaring `c.input(String.class, "in")` and `.output("out")` sends for the same output

#### Scenario: An output call is its name alone in every dynamic SDK

- **WHEN** a Ruby, Python, Node or PHP relay declares its output as `output("out")` with no codec and no encoder, and the tasklet returns the string `echo: hello`
- **THEN** registration SHALL succeed and the plugin frame SHALL carry the bytes `echo: hello`, with no quotes and no MessagePack header, in each of the four SDKs

#### Scenario: A PHP output call is its name alone when the tasklet emits

- **WHEN** a PHP relay declares its output as `output('out')` with no codec and no encoder, and the tasklet's `run`, declared `: void`, calls `$ctx->emit('echo: hello')`
- **THEN** registration SHALL succeed and the plugin frame SHALL carry the bytes `echo: hello`, with no quotes and no MessagePack header

#### Scenario: A Ruby output call is its name alone when the tasklet emits

- **WHEN** a Ruby relay declares its output as `output("out")` with no codec and no encoder, and the tasklet's `run` calls `ctx.emit("echo: hello")`
- **THEN** registration SHALL succeed and the plugin frame SHALL carry the bytes `echo: hello`, with no quotes and no MessagePack header

#### Scenario: A Node output call is its name alone when the tasklet emits

- **WHEN** a Node relay declares its output as `output('out')` with no codec and no encoder, and the tasklet's `run` calls `ctx.emit('echo: hello')`
- **THEN** registration SHALL succeed and the plugin frame SHALL carry the bytes `echo: hello`, with no quotes and no MessagePack header

#### Scenario: An undeclared entry reads JSON in every dynamic SDK

- **WHEN** a Ruby, Python, Node or PHP relay declares an entry with no codec and no decoder, and the bytes `{"a":1}` arrive on it
- **THEN** the tasklet SHALL receive a map whose key `a` holds the number 1

#### Scenario: Plain text into an undeclared entry fails its decode

- **WHEN** the bytes `hello` arrive on an entry declared with no codec and no decoder
- **THEN** the decode SHALL fail and ride the entry channel's retry and dead-letter ladder, and the tasklet SHALL NOT be invoked with the payload

#### Scenario: An undeclared output follows the value

- **WHEN** a tasklet emits a string, a byte value and a map to an output declared with no codec and no encoder
- **THEN** the plugin frames SHALL carry the string's UTF-8 bytes, the byte value unchanged, and the map as JSON, in each of the four SDKs

#### Scenario: A json output quotes a string

- **WHEN** a tasklet emits the string `hi` to an output declared with codec `json`
- **THEN** the plugin frame SHALL carry the four bytes `"hi"`, quotes included

#### Scenario: MessagePack stays one word away where it ships

- **WHEN** a Ruby, Python or Node relay declares an entry and an output with codec `msgpack`
- **THEN** the entry SHALL decode and the output SHALL encode with that SDK's MessagePack codec, byte-identical to that SDK's class codec

#### Scenario: An unknown codec name is refused at declaration

- **WHEN** a Ruby, Python or Node relay declares an entry named `in` with the codec name `yaml`
- **THEN** declaring the relay SHALL raise the SDK's declaration error naming the relay, the channel `in`, the name `yaml` and the accepted names, before any message is processed

#### Scenario: A codec and a callable together are refused

- **WHEN** a relay in any of the four SDKs declares an entry with both a codec and a decoder, or an output with both a codec and an encoder
- **THEN** declaring the relay SHALL raise the SDK's declaration error naming the relay and the channel

#### Scenario: Invalid UTF-8 under text is a failure, not a replacement

- **WHEN** bytes that are not valid UTF-8 arrive on an entry declared with codec `text`
- **THEN** the decode SHALL fail and ride the entry channel's retry and dead-letter ladder, and no string carrying replacement characters SHALL reach the tasklet

#### Scenario: A text output refuses a value that is not a string

- **WHEN** a tasklet emits the number 42 to an output declared with codec `text`
- **THEN** the invocation SHALL fail with an error naming the channel, and no plugin frame SHALL carry the value

#### Scenario: A bytes output refuses a value that is not bytes

- **WHEN** a tasklet emits the number 42 to an output declared with codec `bytes`
- **THEN** the invocation SHALL fail with an error naming the channel, and no plugin frame SHALL carry the value; a Ruby or PHP string, which is the byte value in those languages, SHALL pass through unchanged

### Requirement: A channel SHALL carry at most one plugin end

A channel SHALL end in at most one of `output` and `call(...)`. A channel declaring `output` and a
call end, or two call ends, SHALL fail registration as `ChannelEndsTwice`, code
`topology.channel_ends_twice`, naming the channel, judged by `flz_core::host::register::check` for
the SDKs that share `flz_core` and by the Elixir SDK's re-expression of it. Two `output` ends on
one channel SHALL stay `DuplicatePluginOutput`. `UnkeyedChannelUnderEffectivelyOnce` SHALL apply to
every channel ending in `output`, whether or not its outputs answer calls.
`test/fixtures/reference_topologies.txt` SHALL carry a `ChannelEndsTwice` row for `output` beside a
call end and SHALL carry no `response` step or row.

#### Scenario: An output beside a call end is refused

- **WHEN** a relay declares `.into(Plain).output("out")` and `.call("pricing", deadline_ms: 500)` on the same handle
- **THEN** registration SHALL fail with `topology.channel_ends_twice` naming `Plain`'s channel, in every SDK

#### Scenario: An unkeyed output under the tier is refused even when it answers calls

- **WHEN** a persistent, effectively-once flow relay declares `.into(Plain).output("quotes")` without `keyed` and is meant to answer calls
- **THEN** registration SHALL fail as `UnkeyedChannelUnderEffectivelyOnce`, and the same declaration with `.keyed()` SHALL register

### Requirement: `call(...)` SHALL end a chain on a callee in every SDK

Every SDK's graph builder SHALL offer `call` on a stream handle with a static target or a factory,
a required positive deadline in milliseconds, an optional `channel` naming the callee's entry
channel, an optional `answer_channel` naming the callee's channel whose output answers the call
and, for a factory, an optional `create` defaulting to true, spelled in the SDK's idiom for named
arguments: `call("pricing", deadline_ms: 500)` and
`call(factory: "agent", deadline_ms: 60_000, create: false)` in Elixir and Ruby, keyword arguments
in Python, an options object in Node, named arguments in PHP, and a target value
(`CallTarget::id(..)` / `CallTarget::factory(..)`, `CallTarget.id(..)` / `CallTarget.factory(..)`)
with a deadline in Rust and Java, where `answer_channel` is a method on the target value. It SHALL
take the request's serialization arguments `output` takes and the answer's decoding arguments `input`
takes in that SDK: the answer type in Rust and Java, one named codec for both directions in Node,
Python, Ruby and PHP (JSON by default), and optional `serialize:` and `decode:` functions in Elixir.
It SHALL be a plugin end of the channel, counted by `ChannelEndsTwice` beside an `output`. It SHALL
answer a handle on a new entry channel of the relay, the Response channel, named after the
calling channel with the suffix `.response`, whose values are the SDK's `Response` type; the handle
SHALL carry the whole surface an entry handle carries. A call SHALL NOT be a graph edge, so wiring
the Response channel into the tasklet that made the call SHALL NOT be refused as a cycle. The
builder SHALL record a `call` link carrying the target or factory, the deadline, the callee's entry
channel, the answer channel, `create` and the Response channel's name.

#### Scenario: The same gateway in every SDK

- **WHEN** a gateway that keys a turn by its session, calls the factory `agent` and wires the Response channel back into its accounting tasklet is declared in each SDK
- **THEN** each declaration SHALL register with the same derived channels, and each SDK's runner SHALL deliver the same `Response` for the same answer

#### Scenario: The Response channel is an entry channel

- **WHEN** a relay declares `.into(Turn).call("pricing", deadline_ms: 500).into(Price)`
- **THEN** registration SHALL succeed, the relay SHALL declare the entry channel `<Turn's channel>.response` with `Price` as its consumer, and each `Response` SHALL reach `Price`

#### Scenario: The answer channel is carried on the request

- **WHEN** a relay declares `.into(Turn).call("pricing", deadline_ms: 500, answer_channel: "quotes")` in any SDK
- **THEN** every request frame the end produces SHALL carry `flz.call_answer_channel` = `quotes`

### Requirement: A call end SHALL be refused where it cannot work

Registration SHALL refuse, in `flz_core::host::register::check` and its steps spec check for the
SDKs sharing `flz_core` and in the Elixir SDK's re-expression:

- a `:once` or `:chained` steps class declaring a call end, as `CallOutsidePerInputLoop`
  (`topology.call_outside_per_input_loop`), because such a class never reads the later message a
  `Response` is;
- a factory call with `create: false` and no `key_by` before it, as `CallWithoutKey`
  (`topology.call_without_key`), because nothing names the instance it may not create;
- a call end whose deadline is zero, as `CallDeadlineZero` (`topology.call_deadline_zero`);
- a call end whose Response channel feeds nothing, as `CallResponseUnread`
  (`topology.call_response_unread`), because every outcome would be journaled and delivered to no
  tasklet;
- a call end beside an `output` or another call end on one channel, as `ChannelEndsTwice`.

`test/fixtures/reference_topologies.txt` SHALL carry one row for each, the steps `<x>.call(<target>)`,
`<x>.call_factory(<factory>)`, `<x>.call_factory_nocreate(<factory>)`, `<x>.call_deadline(0)` and `<x>.call_unread(<target>)`,
and an `ok` row wiring a Response channel back into its calling tasklet, with its derived lines.

#### Scenario: A single-pass class that calls is refused

- **WHEN** a `:once` steps class declares `.into(Plain).call("pricing", deadline_ms: 500)`
- **THEN** registration SHALL fail with `topology.call_outside_per_input_loop` in every SDK

#### Scenario: A per-input class that calls registers

- **WHEN** a `:per_input` steps class declares the same call end
- **THEN** registration SHALL succeed

### Requirement: Single `output` egress

Plugin egress SHALL be declared by exactly one builder method in every SDK, taking an optional channel name and an optional serializer. Java, Node, Ruby, PHP and Python SHALL spell the four combinations in their own idiom for an optional argument (an overload in Java, an optional parameter in Node, a default argument in Ruby, PHP and Python); Rust, which has neither overloading nor default arguments, SHALL spell them `output()` / `output_named(name)` / `output_with(serializer)` / `output_named_with(name, serializer)`; Elixir SHALL spell them `output/1`, `output/2`, `output_with/2` and `output_with/3`, the name defaulting in the middle argument. `output` SHALL bind the producing tasklet's output channel to the plugin egress and SHALL return the stream handle, so inter-tasklet consumers MAY be wired after it — a channel is terminal by having no further wiring, not by method choice. An SDK whose `output` returns nothing SHALL NOT satisfy this requirement, because the hybrid declaration is unwritable in it. A channel carrying a plugin output and no consumer SHALL be declared to the runtime as plugin-bound; a channel carrying a plugin output **and** consumers SHALL keep its inter-tasklet kind, and the host SHALL release both the consumer edge outputs and one plugin output for each emitted value. An `output` without a serializer SHALL apply the default serialization rules (passthrough for bytes, UTF-8 for strings, default JSON otherwise). The name an `output` carries SHALL be the channel's name, which is the metadata the controller routes by; a channel SHALL therefore be named by at most one `output`, and two channels naming one output SHALL fail registration as a duplicate channel name. The former `sink` / `sink_named` / `sink_with` / `sink_named_with` spellings, and the short form's `sink` setting, SHALL NOT exist in any SDK; there SHALL be no coexistence period. Wire frames, descriptor fields and internal identifiers keep their existing names. The word "sink" SHALL name no user-visible part of a relay or a plugin role: SDK methods and settings, test and local-mode helpers, provider ids, error codes and texts, CLI help and documentation say "output", and a plugin that takes a relay's outputs is an output plugin, one that feeds an entry channel an input plugin.

#### Scenario: output is hybrid by default

- **WHEN** a flow relay declares `.output("indicators", serializer)` and a consumer is wired from the same handle afterwards
- **THEN** each emitted value SHALL reach the typed consumer and, serialized once, the plugin layer, stamped `channel = "indicators"`

#### Scenario: Terminal by construction

- **WHEN** a flow relay declares `.output("pager")` and nothing further is wired from the handle
- **THEN** the channel SHALL be plugin-bound and named `"pager"`

#### Scenario: The no-name form is available in every SDK

- **WHEN** a flow relay declares the no-name form in any SDK
- **THEN** the channel SHALL be plugin-bound, its name SHALL be whatever it would have been without the output, and the declaration SHALL compile or register in every SDK

#### Scenario: Old spellings do not exist

- **WHEN** user code calls `.sink(...)`, `sink_named(...)` or `sink_with(...)`, or a short form sets `sink`, against the new SDK
- **THEN** the code SHALL fail to compile or to register; no transitional alias SHALL exist

#### Scenario: Every SDK stamps one name for one declaration

- **WHEN** the same relay declaring `.output("indicators", s)` is registered from a Rust host and from a host SDK
- **THEN** frames delivered to the plugin SHALL be byte-identical, `channel = "indicators"` included, so the two naming sources cannot answer differently

#### Scenario: A hybrid channel declared in a veneer SDK keeps one channel

- **WHEN** a Node, Ruby, PHP or Python relay declares `.output("mid", enc).into(Next)` on one handle
- **THEN** the declaration SHALL carry exactly one channel produced by that tasklet, named `mid`, of inter-tasklet kind, listing `Next` as its consumer, and each emitted value SHALL reach both `Next` and the plugin layer

#### Scenario: The file plugin takes the new provider ids

- **WHEN** `flz plugin enable log relay_file_output --Xpath=out.ndjson` is run
- **THEN** the plugin SHALL be enabled, and `flz plugin enable log relay_file_sink ...` SHALL be refused as an unknown provider

#### Scenario: No user-visible sink remains

- **WHEN** the repository's SDK sources, docs, CLI help, error catalogue and specs are searched for `.sink(`, `sink_named`, `sink_with`, "sink plugin", "source plugin" and the renamed error codes and provider ids
- **THEN** only `local_dev/dev_notes/`, `openspec/changes/archive/` and the observability layer's sinks SHALL match

