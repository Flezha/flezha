# ruby-sdk Specification

## Purpose
TBD - created by archiving change ruby-sdk. Update Purpose after archive.
## Requirements
### Requirement: The Ruby SDK is a poll host over `flz_core`

The Ruby SDK SHALL drive `flz_core` through the poll boundary and SHALL contain no kernel code: transport, framing, ordering, per-cell exclusivity, durability, retries, the retry→park ladder, timers, credit, routing, the workflow driver and reconnect SHALL remain in the core. The SDK's responsibilities SHALL be limited to the authoring surface, the codec for user types, the threading host, and idiomatic errors.

#### Scenario: No kernel surface is reimplemented in Ruby
- **WHEN** the gem's source is inspected for a retry ladder, an outbox, a WAL writer, a credit window or a workflow driver
- **THEN** none SHALL exist; every such behaviour SHALL be reached by calling the core

#### Scenario: The runner connects, serves and shuts down through the core
- **WHEN** a Ruby runner starts, binds an operative, processes input, fires a timer and receives a shutdown
- **THEN** every step SHALL be driven by `poll` / `complete` against the core, and the runner SHALL exit with in-flight activations completed or reported

### Requirement: Runner entry point and lifecycle

The SDK SHALL expose one entry point that declares classes, claims a runner slot and serves until shutdown. Declaration SHALL complete before the first `poll`. Shutdown SHALL be observable from a signal, from closed stdin (the e2e teardown contract), and from a controller-requested shutdown, and SHALL drain in-flight activations before the process exits.

#### Scenario: Declaration precedes serving
- **WHEN** the entry point is invoked with two declared operative classes
- **THEN** both class declarations SHALL reach the core before any activation is polled

#### Scenario: Closed stdin ends the runner
- **WHEN** the runner's stdin reaches EOF
- **THEN** the runner SHALL begin shutdown, drain in-flight activations, and exit zero

### Requirement: Class-based authoring surface with explicit registration

Operatives and tasklets SHALL be authored as Ruby classes. The SDK SHALL NOT scan packages, walk load paths, or autoload to discover them; the author SHALL name the classes that participate. A declaration that cannot be resolved to a valid topology SHALL be refused at registration time with an error naming the offending class and method — registration-time refusal replaces the Rust SDK's compile-time refusal. A tasklet's body SHALL be its `run` method, the name every SDK gives a tasklet's body; a tasklet class defining `process` and no `run` SHALL be refused at registration naming `run`.

#### Scenario: An undeclared class never runs
- **WHEN** a tasklet class is defined but not registered
- **THEN** it SHALL NOT be declared to the core, and no activation SHALL reach it

#### Scenario: A bad topology is refused by name
- **WHEN** a registered tasklet emits to a channel no tasklet consumes and the operative is not sealed on that edge
- **THEN** registration SHALL raise naming the class, the method and the channel; no controller connection SHALL be attempted

#### Scenario: A tasklet's body is its run method
- **WHEN** a `Flezha::Tasklet` subclass defines `run(input, ctx)` and an operative wires it
- **THEN** each input SHALL reach `run`, and its return value SHALL be the tasklet's output

#### Scenario: The old body method is refused
- **WHEN** a registered tasklet class defines `process(input, ctx)` and no `run`
- **THEN** registration SHALL raise naming the class and `run`

### Requirement: The authoring surface carries the shared vocabulary at full parity

The Ruby authoring surface SHALL carry both operative kinds — stream and workflow — and the shared vocabulary the reference SDK carries: the graph/wiring DSL with `key_by` and transforms, `ctx` emits, `ctx.now` and `ctx.stable_id`, cell state and change journaling, static and dynamic cell modes, per-cell timers, tiers including effectively-once, multi-input tasklets, tasklet-requested spawn, workflow chains, loops, suspension and rewind. A capability the Ruby SDK does not carry SHALL be named as deferred in its conformance manifest together with the follow-up that owes it; silent absence SHALL be a defect.

#### Scenario: The manifest defers nothing at v1
- **WHEN** `test/apps/ruby_runner/conformance.json` is compared against the Rust reference manifest
- **THEN** its `deferred` list SHALL be empty and its fixture set SHALL equal the reference's

#### Scenario: A workflow class runs a durable chain
- **WHEN** a Ruby workflow operative runs a chained loop with a suspension and is resumed after a runner restart
- **THEN** the journal SHALL drive resume without re-running a committed tasklet

#### Scenario: Two channels merge into one tasklet
- **WHEN** an operative declares `c.merge.from(ticks) { |t| … }.from(quotes) { |q| … }.key_by { |v| v[:symbol] }.into(Detector)`
- **THEN** registration SHALL succeed with one Detector node fed by two consumer edges, each applying its own mapping, a value from either entry SHALL reach `run` as the mapped value keyed by `symbol`, a duplicated source SHALL refuse with `DuplicateMergeSource` and a single source with `MergeTooFewSources`

#### Scenario: One input becomes three deliveries
- **WHEN** an edge declares `.flat_map { |line| line.split(",") }` into a tasklet and the dispatcher is handed `a,b,c`
- **THEN** `run` SHALL run three times, once per element, under one consumed offset, and feeding an empty line SHALL run it zero times

#### Scenario: The latest-document router is offered
- **WHEN** `c.route_latest(config, trades) { |t| t[:symbol] }` is declared and `Flezha::Testing::Harness` feeds a document, trades for `A` and `B`, the same document, then a trade for `A`
- **THEN** exactly one `["A", document]` and one `["B", document]` SHALL be emitted, the repeated document SHALL be inert, and `route_latest_named` SHALL take a node name

### Requirement: State and Change serialize through author-declared `to_h` / `from_h` over MessagePack

State and Change types SHALL serialize through author-declared `to_h` and `from_h` methods encoded as MessagePack, carrying an author-controlled schema version. The SDK SHALL NOT use `Marshal` for any persisted or wire-bound value. The SDK SHALL raise at registration time when a declared State or Change type does not implement both methods.

#### Scenario: `Marshal` is not reachable for persisted state
- **WHEN** the gem's persistence and wire paths are inspected
- **THEN** no call to `Marshal.dump` or `Marshal.load` SHALL appear on them

#### Scenario: A class rename does not break restore
- **WHEN** a State class is renamed and its `to_h` / `from_h` shape is unchanged
- **THEN** a Cell restored from a WAL written before the rename SHALL fold successfully

#### Scenario: A missing codec method is refused at registration
- **WHEN** a State type declares `to_h` but not `from_h`
- **THEN** registration SHALL raise naming the type and the missing method

### Requirement: Ruby integers carry the schema's unsigned 64-bit values without narrowing

Offsets, sequence numbers and timestamps crossing the boundary SHALL be represented as Ruby integers with no width policy, precision loss or float widening.

#### Scenario: A high-bit sequence number round-trips
- **WHEN** an activation carries a sequence number above 2^63
- **THEN** the value observed in Ruby SHALL equal it exactly, and the value returned in the result SHALL round-trip byte-identically

### Requirement: The Ruby builder submits its topology to the reference check

The Ruby SDK's builder SHALL record the authoring calls it receives — channels, the
ordered links of each chain, merges, branches, class flags and tasklet facts — into the
declaration record and SHALL take its verdict from the reference check, so that every name
of the `TopologyIssue` vocabulary is refused with the reference sentence and no rule is
carried in the builder. The class-based surface SHALL stay as it is; only the judgement moves.
The SDK SHALL prove the set by one conformance run over every row of
`test/fixtures/reference_topologies.txt` through the row interpreter in its test app.

#### Scenario: A malformed row refuses with the reference name

- **WHEN** the Ruby interpreter authors a refusal row through the SDK's own builder
- **THEN** registration refuses with the row's name and the reference sentence

#### Scenario: A well-formed row derives to the fixture's bytes

- **WHEN** the Ruby interpreter authors an `ok` row
- **THEN** the derived declaration equals the row's lines

#### Scenario: A merge row runs through the SDK's own merge handle

- **WHEN** the Ruby interpreter reaches `merge_one_source` or `merge_same_twice`
- **THEN** it authors the row with `c.merge.from(...)…into(...)` rather than raising, registration refuses with `MergeTooFewSources` or `DuplicateMergeSource`, and the manifest's `topology_surfaces` lists `merge`

### Requirement: The single-tasklet short form is a class-body declaration
The short form SHALL be a `Flezha::StreamOperative` or `Flezha::WorkflowOperative` subclass, the same base a full-form operative subclasses, and SHALL NOT be a `Flezha::Tasklet` subclass; `Flezha::Tasklet` SHALL lose its `sink` class-body declaration and SHALL raise naming the operative base when one is written. The class body SHALL declare `factory_id "name"`, as a full-form operative already does, and MAY declare `sink "name"`, `input "name"` and `codec :text`, each its own class-body call so that none rides another. The kind is the base class and SHALL NOT be a keyword.

Such a class SHALL be a short form exactly when it defines `run` and not `channels` or `tasklets`; defining both SHALL be refused, and defining neither SHALL be refused naming the missing body. `sink` SHALL be optional. The class SHALL carry a generated operative of its base's kind whose factory id is `factory_id`, wiring an entry channel named `input` (or the `input` value) into a generated tasklet named `factory_id` and, when a `sink` is declared, that tasklet to the `sink` plugin channel; with no `sink`, it SHALL wire the entry channel into the tasklet and declare no plugin channel. A workflow short form SHALL be a `once` workflow with every workflow setting at its default. The entry SHALL be decoded by `codec` where one is declared, whether or not a `sink` is.

`Runner#register_factory` SHALL accept such a class and register the generated operative; the refusals SHALL be raised there, at registration, as the Ruby contract places every refusal: `Flezha::DeclarationError` whose refusals and message begin with `ShortFormWithTopology`. A class with no `factory_id` SHALL be refused naming the required declaration, and SHALL NOT take a factory id from the class constant. The Ruby SDK cannot see a tasklet's output type, owes no `SinkWithoutOutput`, and SHALL NOT list `shorthand_typed_output` in its manifest. A duplicate factory id SHALL raise with a message beginning `DuplicateFactoryId`.

#### Scenario: An operative class with a run body is a whole stream operative

- **WHEN** `class Upcase < Flezha::StreamOperative` declares `factory_id "upcase"`, `sink "out"`, `codec :text` and `run`, and the runner is given `register_factory(Upcase)`
- **THEN** the runner declares a stream operative `upcase` with entry channel `input` decoding text, a tasklet named `upcase` and sink `out`, and no `Flezha::Tasklet` subclass exists for it

#### Scenario: An operative class with a run body is a whole workflow operative

- **WHEN** `class Place < Flezha::WorkflowOperative` declares `factory_id "place"`, `sink "placed"`, `input "order"`, `codec :text` and `run`, the runner is given `register_factory(Place)`, and an instance is started with init values
- **THEN** `run` receives the start values as text, its return value reaches `placed`, and the instance completes

#### Scenario: A short form with no sink is declared

- **WHEN** a class declares `factory_id "counter"`, `input "ticks"`, `codec :text`, `initial_state`, `apply_change` and `run`, with no `sink`
- **THEN** the operative declares `ticks` into the generated tasklet and no plugin channel

#### Scenario: An input and codec need no sink

- **WHEN** a class declares `input "ticks"` and `codec :text` and no `sink`
- **THEN** `register_factory` accepts it and the entry channel takes that name and codec

#### Scenario: A channels body beside a run body is refused

- **WHEN** a `Flezha::StreamOperative` subclass defines both `channels` and `run`
- **THEN** `register_factory` raises `Flezha::DeclarationError` naming `ShortFormWithTopology`

#### Scenario: A sink on a tasklet is refused

- **WHEN** a `Flezha::Tasklet` subclass declares `sink "out"`
- **THEN** loading it raises naming `Flezha::StreamOperative` and `Flezha::WorkflowOperative`

#### Scenario: A derived factory id is refused

- **WHEN** a `Flezha::StreamOperative` subclass declares `sink "out"` and `run` and no `factory_id`
- **THEN** `register_factory` raises `Flezha::DeclarationError` naming the required `factory_id`, and no controller connection is attempted

