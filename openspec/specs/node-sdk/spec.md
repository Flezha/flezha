# node-sdk Specification

## Purpose
TBD - created by archiving change node-sdk. Update Purpose after archive.
## Requirements
### Requirement: A Node Runner Entry Point Over the Poll Core

The SDK SHALL provide a runner entry point that takes a namespace, accepts stream and workflow
operative declarations by module specifier, connects to the controller, and runs until shutdown. It
SHALL drive `flz_core` through the C ABI and SHALL NOT re-express dispatch, ordering, overflow, the
retry ladder, durability or the workflow driver.

Transport configuration SHALL ride the same environment the other poll hosts read.

#### Scenario: A runner connects and serves
- **WHEN** a runner is started with a declared stream operative
- **THEN** it SHALL connect, advertise its factories, receive activations and process inputs in
  per-entity order

#### Scenario: The SDK adds no kernel behaviour
- **WHEN** the package source is inspected for dispatch, retry, overflow or durability logic
- **THEN** none SHALL be found outside calls into the core

### Requirement: Factory-Function Authoring With Type-Driven Wiring

The SDK SHALL provide a factory-function authoring surface — a procedure definition, a stream
operative definition, a workflow operative definition — wired by a fluent channel builder carrying
entry channels, inter-procedure edges, keying and edge transforms. Handler input and output types
SHALL drive graph wiring.

Decorators SHALL NOT be required, and the surface SHALL be usable from plain JavaScript with no
build step.

#### Scenario: A graph is declared and built
- **WHEN** an operative declares an entry channel, a keying function and a procedure
- **THEN** the topology SHALL build, and the procedure's entities SHALL be addressable by the
  declared key

#### Scenario: Plain JavaScript authors without a build step
- **WHEN** an operative is declared from a plain JavaScript module
- **THEN** it SHALL register and run with no transpilation

### Requirement: A Mismatched Edge Is a Compile Error, and the Refusal Is Pinned

The SDK SHALL type the channel builder so that wiring a producer to a consumer whose input type does
not accept the producer's output is rejected by the TypeScript compiler. These refusals SHALL be
pinned by fixtures that are expected to fail compilation, so that a refusal which stops working
fails the build.

This is the SDK's counterpart to the compile-fail tests the Rust SDK pins, and it SHALL NOT be
demoted to a registration-time check.

#### Scenario: A wrong edge type fails to compile
- **WHEN** a procedure is wired to consume a type its declared input does not accept
- **THEN** type checking SHALL fail, and a fixture asserting that failure SHALL be part of the test
  suite

### Requirement: Registration-Time Refusals Name the Defect

The SDK SHALL refuse, at registration, every defect class the authoring contract names that static
typing cannot see: a missing or duplicated entry channel, a cycle, an unreachable procedure, an
identity collision, a missing or unservable clock extractor, a keyed channel without a plugin wire,
a tier channel that is not keyed, checkpoint acknowledgement without the tier, a correction window
without its preconditions, branch arity violations, and a duplicate plugin sink. The verdict SHALL
be the reference check's, reached through `flz_core_check_class` over the record the builder
writes; the builder SHALL carry no rule of its own, and SHALL refuse every name of the reference
vocabulary, not a subset.

Every refusal SHALL name the offending handler and the specific defect.

#### Scenario: A cyclic graph is refused by name
- **WHEN** a declared topology contains a cycle
- **THEN** registration SHALL fail naming the participating procedures

#### Scenario: A reference refusal the builder never carried is refused
- **WHEN** a procedure declares `mailboxCapacity(0)`
- **THEN** registration SHALL fail with `MailboxCapacityZero` naming the procedure, where the
  core accepted the declaration before the check read the record

### Requirement: A Procedure Context at Parity With the Other SDKs

The SDK SHALL provide the invocation context the authoring contract defines: plain, channel-selected
and keyed emits with metadata, the deterministic clock read, the stable identifier, timer
registration and cancellation, workflow spawn, and the workflow inbox. The reserved metadata
namespace SHALL be enforced.

#### Scenario: A reserved metadata key is refused
- **WHEN** a handler emits with a metadata key inside the reserved namespace
- **THEN** the emit SHALL be refused naming the key

### Requirement: Determinism Warnings for Ambient Non-Determinism

The SDK SHALL warn, naming file and line, when a handler reads an ambient clock or generates a
random identifier inside an invocation instead of using the context's deterministic equivalents. The
warning SHALL NOT abort the invocation.

#### Scenario: An ambient clock read is reported
- **WHEN** a handler reads the ambient wall clock during an invocation
- **THEN** a warning SHALL name the file, the line, and the context equivalent

### Requirement: State and Change Serialize as MessagePack With a Specified Type Mapping

Durable state and change bodies SHALL serialize as MessagePack through one codec interface with an
author-controlled schema version, sharing the MessagePack format identifier already allocated rather
than minting a new one.

The JavaScript type mapping SHALL be specified rather than inherited from a library default:
unsigned 64-bit offsets, sequences and timestamps SHALL round-trip through arbitrary-precision
integers with no narrowing; dates SHALL use the MessagePack timestamp extension; byte arrays SHALL
encode as binary and decode to one consistent representation; and absent and null values SHALL
remain distinguishable across a round trip.

A value the codec cannot represent SHALL be refused at registration, naming the class and the field.

#### Scenario: A large unsigned integer survives a round trip
- **WHEN** an offset above the safe integer range is encoded and decoded
- **THEN** the value SHALL be recovered exactly

#### Scenario: An unrepresentable field is refused
- **WHEN** a declared state type holds a value the codec cannot represent
- **THEN** registration SHALL fail naming the type and the field

### Requirement: State Is Mutated Only Through Declared Changes

The SDK SHALL reject direct mutation of a declared state value from within a handler, with an error
naming the field and pointing at the change-based path. The change fold SHALL retain its ability to
mutate, and restore SHALL be unaffected.

#### Scenario: Direct mutation is refused
- **WHEN** a handler assigns to a field of its state value directly
- **THEN** the assignment SHALL be refused naming the field and the change-based path

### Requirement: Host Class Declarations Use the Reference Key Spellings

The SDK SHALL emit host class declaration keys with the reference spellings, and those spellings
SHALL be added to the shared key fixture so that a key present in the core with no Node spelling
fails the detector.

#### Scenario: A missing key spelling fails the detector
- **WHEN** the core declares a host class key for which the Node builder has no spelling
- **THEN** the shared key fixture check SHALL fail

### Requirement: Logging Integrates With the Host Runtime

The SDK SHALL route its diagnostics through a pluggable logger with a sensible default, mirroring
the defaults the other SDKs ship, and SHALL NOT require an observability dependency.

#### Scenario: Diagnostics reach the host's logger
- **WHEN** a logger is supplied at runner construction
- **THEN** SDK diagnostics SHALL be emitted through it

### Requirement: The channel builder carries merge, the latest-document router and flatMap

The Node channel builder SHALL offer `merge()`, returning a merge handle with `from(source, map)`, `keyBy(key)`, `into(procedure)` and `into(procedure, nodeName)`, with the `multi-input-procedures` semantics: a source is an entry stream or an inter-procedure stream declared on the same builder, each source's mapping composes after that source's own transforms, the key is declared once on the merge and applied to the mapped value on every source's edge, and `into` returns the merged procedure's own output stream. The builder SHALL record each source as a chain closed by the core's `merge` link (`{"link": "merge", "procedure", "group"}`) under one group per merge and SHALL take the merge refusals (`MergeTooFewSources`, `DuplicateMergeSource`, `EdgeAlreadyWired`) from the reference check, carrying no rule of its own. The builder SHALL offer `routeLatest(documents, traffic, keyFn)` and `routeLatestNamed(documents, traffic, keyFn, nodeName)` as the `sdk-graph-authoring` router, packaged in the SDK as a single-entity procedure with journaled `{document, version, delivered}` state. Stream handles SHALL offer `flatMap(fn)` beside `map` and `filter`: the closure answers an iterable, each element reaches the consumer on its own, an empty iterable is a drop, and the transform chain is applied as a list so `filter` and `flatMap` are one mechanism; the record carries the same `transform` link as `map`.

#### Scenario: Two channels merge into one procedure
- **WHEN** an operative declares `c.merge().from(ticks, (t) => ({kind: 'tick', ...t})).from(quotes, (q) => ({kind: 'quote', ...q})).keyBy((v) => v.symbol).into(Detector)`
- **THEN** registration SHALL succeed with one Detector node fed by two consumer edges, each edge SHALL apply its own mapping, and a value from either entry SHALL reach Detector's handler as the mapped object keyed by `symbol`

#### Scenario: A duplicated source is refused by the reference name
- **WHEN** a merge declares the same stream twice
- **THEN** registration SHALL refuse with `DuplicateMergeSource` naming the channel, and a merge with one source SHALL refuse with `MergeTooFewSources`

#### Scenario: One input becomes three deliveries
- **WHEN** an edge declares `.flatMap((line) => line.split(',').filter(Boolean))` into a procedure and the dispatcher is handed `a,b,c`
- **THEN** the handler SHALL run three times, once per element, under one consumed offset, and a line the closure answers `[]` for SHALL run it zero times

#### Scenario: The router delivers on a key's own traffic
- **WHEN** `c.routeLatest(config, trades, (t) => t.symbol)` is declared and the harness feeds a document, then trades for `A` and `B`, then the same document, then a trade for `A`
- **THEN** exactly one `{key: 'A', document}` and one `{key: 'B', document}` SHALL be emitted, and the repeated document SHALL be inert

### Requirement: The single-procedure short form is four definition fields
`defineProcedure` SHALL accept `kind` (`'stream'` or `'workflow'`), `sink`, `input` and `codec` beside `name`. A definition carrying `name`, `kind` and `sink` SHALL return a procedure that also carries a generated `ClassSpec` of that kind whose factory id is `name`: a stream, or a `once` workflow with every workflow setting at its default, wiring an entry channel named `input` (or the `input` field's value) into the procedure and the procedure to the `sink` plugin channel. `codec` SHALL take the vocabulary of `c.input(name, { codec })` and SHALL be lowered to that call. The spec SHALL be built at definition time so a refusal is raised while the module loads, as a `dup_node` graph already is. `defineProcedure` SHALL throw `DeclarationError` with the refusal's name in `refusals` and at the start of the message: `SinkWithoutId` for a `sink` with an empty `name`, raised before the plain non-empty-name check; `SinkWithoutKind` for a `sink` with no `kind`; and `ShorthandKeyWithoutSink` for `kind`, `input` or `codec` with no `sink`, naming the fields. The runner's `registerFactory(specifier)` SHALL register the `ClassSpec` of every short-form procedure the module exports, as it registers every exported spec. A duplicate factory id SHALL throw with a message beginning `DuplicateFactoryId`. Whether `SinkWithoutOutput` can be raised at TypeScript's type level without a build step SHALL be decided at implementation and reflected in the manifest's `topology_surfaces`.

#### Scenario: A module exporting one procedure is a whole stream operative

- **WHEN** `./upcase.mjs` default-exports `defineProcedure({ name: 'upcase', kind: 'stream', sink: 'out', codec: 'text', handle })` and the runner is given `registerFactory('./upcase.mjs')`
- **THEN** the runner declares a stream operative `upcase` with entry channel `input` decoding text and sink `out`, and the module calls no `defineOperative`

#### Scenario: A module exporting one procedure is a whole workflow operative

- **WHEN** `./place.mjs` exports `defineProcedure({ name: 'place', kind: 'workflow', sink: 'placed', input: 'order', codec: 'text', handle })`, the runner is given `registerFactory('./place.mjs')`, and an instance is started with init values
- **THEN** `handle` receives the start values as text, its return value reaches `placed`, and the instance completes

#### Scenario: A sink without a name is refused by the short form's name

- **WHEN** a module calls `defineProcedure({ kind: 'stream', sink: 'out', handle })`
- **THEN** loading it throws `DeclarationError` whose refusals contain `SinkWithoutId`

#### Scenario: A sink without a kind is refused

- **WHEN** a module calls `defineProcedure({ name: 'upcase', sink: 'out', handle })`
- **THEN** loading it throws `DeclarationError` whose refusals contain `SinkWithoutKind`

#### Scenario: Plain JavaScript still needs no build step

- **WHEN** the short form is used from a `.mjs` module
- **THEN** it registers and runs with no transpilation

### Requirement: A Node runner registers a factory with one awaited call
The runner SHALL provide `async registerFactory(specifier)` and `async unregisterFactory(specifier)`, and SHALL NOT provide `defineStream`, `defineWorkflow` or `declare`. `registerFactory` SHALL be legal before and after `run()`: it SHALL import the module and read its exported operatives (stream, workflow or short form, the kind read from each), rejecting with an error naming the specifier when the import fails or the module exports no operative; before `run()` it SHALL record the specifier for `run()`, and after `run()` it SHALL also declare every exported operative against the running core and every worker. `unregisterFactory` SHALL drop a specifier recorded before `run()`, rejecting naming the specifier when none was recorded, and after `run()` SHALL reject with an error stating that a running Node runner cannot stop serving a started declaration. The authoring functions `defineProcedure`, `defineOperative` and `defineWorkflow` SHALL keep their names, because they declare a class.

#### Scenario: Registration is awaited before run

- **WHEN** a program runs `const runner = createRunner('default'); await runner.registerFactory('./echo_operative.mjs'); await runner.run();`
- **THEN** the runner declares the module's operative at start

#### Scenario: A broken module rejects where it is registered

- **WHEN** a program awaits `runner.registerFactory('./missing.mjs')` before `run()`
- **THEN** the promise rejects with an error naming `./missing.mjs`, before `run()` is called

#### Scenario: A post-start registration reaches the running core

- **WHEN** `registerFactory('./late.mjs')` is awaited after `run()` has started the runner
- **THEN** the module's operative is declared to the core and to every worker, as `declare` did

#### Scenario: A post-start removal is rejected, not ignored

- **WHEN** `unregisterFactory('./echo_operative.mjs')` is awaited after `run()`
- **THEN** the promise rejects saying a running Node runner cannot stop serving a started declaration, and the operative stays declared
