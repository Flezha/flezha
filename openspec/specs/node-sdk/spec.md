# node-sdk Specification

## Purpose

The Node SDK. Its authoring surface defines tasklets and relays with factory functions whose types
drive the wiring, so a mismatched edge is a compile error; it carries the shared context, settings
tree, short form and channel builder, serializes state and changes as MessagePack, and allows state
to change only through declared changes. Its host never reaches a blocking core call from
JavaScript: fetch threads are its own, a completion pool and starvation supervisor keep activations
moving, dispatch can target one of N event loops, every activation is answered exactly once, and
shutdown drains in-flight work. It ships as a feature of the existing ABI crate with prebuilt
per-platform binaries and negotiates the schema version before connecting, and is proven through the
conformance suite, its own harness and workflow, and Node as a measured runtime.

## Requirements

### Requirement: A Node Runner Entry Point Over the Poll Core

The SDK SHALL provide a runner entry point that takes a namespace, accepts flow and steps
relay declarations by module specifier, connects to the controller, and runs until shutdown. It
SHALL drive `flz_core` through the C ABI and SHALL NOT re-express dispatch, ordering, overflow, the
retry ladder, durability or the steps driver.

Transport configuration SHALL ride the same environment the other poll hosts read.

#### Scenario: A runner connects and serves
- **WHEN** a runner is started with a declared flow relay
- **THEN** it SHALL connect, advertise its factories, receive activations and process inputs in
  per-cell order

#### Scenario: The SDK adds no kernel behaviour
- **WHEN** the package source is inspected for dispatch, retry, overflow or durability logic
- **THEN** none SHALL be found outside calls into the core

### Requirement: Factory-Function Authoring With Type-Driven Wiring

The SDK SHALL provide a factory-function authoring surface (a tasklet definition and a relay
definition taking the settings tree) wired by a fluent channel builder carrying
entry channels, inter-tasklet edges, keying and edge transforms. Handler input and output types
SHALL drive graph wiring: the input type is `run`'s first parameter, and the output type is the one
the definition declares through its type arguments or its context parameter, never one inferred from
a body. A tasklet's body SHALL be the definition's `run` field, the name every SDK gives a tasklet's
body, and it SHALL emit through its context; a definition carrying a `handle` field SHALL throw
`FlezhaError` naming `run`.

Decorators SHALL NOT be required, and the surface SHALL be usable from plain JavaScript with no
build step.

#### Scenario: A graph is declared and built
- **WHEN** a relay declares an entry channel, a keying function and a tasklet
- **THEN** the topology SHALL build, and the tasklet's cells SHALL be addressable by the
  declared key

#### Scenario: Plain JavaScript authors without a build step
- **WHEN** a relay is declared from a plain JavaScript module
- **THEN** it SHALL register and run with no transpilation

#### Scenario: A tasklet's body is its run field
- **WHEN** a module calls `defineTasklet({ name: 'upcase', run: (s, ctx) => ctx.emit(s.toUpperCase()) })` and a relay wires it
- **THEN** each input SHALL reach `run`, and the value it passes to `ctx.emit` SHALL be the tasklet's output

#### Scenario: The old body field is refused
- **WHEN** a module calls `defineTasklet({ name: 'upcase', handle: (s) => s })`
- **THEN** loading it SHALL throw `FlezhaError` naming `run`

### Requirement: A Mismatched Edge Is a Compile Error, and the Refusal Is Pinned

The SDK SHALL type the channel builder so that wiring a producer to a consumer whose input type does
not accept the producer's output is rejected by the TypeScript compiler. These refusals SHALL be
pinned by fixtures that are expected to fail compilation, so that a refusal which stops working
fails the build.

This is the SDK's counterpart to the compile-fail tests the Rust SDK pins, and it SHALL NOT be
demoted to a registration-time check.

#### Scenario: A wrong edge type fails to compile
- **WHEN** a tasklet is wired to consume a type its declared input does not accept
- **THEN** type checking SHALL fail, and a fixture asserting that failure SHALL be part of the test
  suite

### Requirement: Registration-Time Refusals Name the Defect

The SDK SHALL refuse, at registration, every defect class the authoring contract names that static
typing cannot see: a missing or duplicated entry channel, a cycle, an unreachable tasklet, an
identity collision, an unservable clock, a keyed channel without a plugin wire, a tier channel
that is not keyed, checkpoint acknowledgement without the tier, a correction window without its
preconditions, branch arity violations, and a duplicate plugin output. The verdict SHALL be the
reference check's, reached through `flz_core_check_class` over the record the builder writes; the
builder SHALL carry no rule of its own, and SHALL refuse every code of the reference vocabulary,
not a subset. The clock SHALL be read from the fields the tasklet writes, so no clock can be
declared without the function it needs.

Every refusal SHALL name the offending handler and the specific defect.

#### Scenario: A cyclic graph is refused by name
- **WHEN** a declared topology contains a cycle
- **THEN** registration SHALL fail naming the participating tasklets

#### Scenario: A reference refusal the builder never carried is refused
- **WHEN** a tasklet declares `mailboxCapacity(0)`
- **THEN** registration SHALL fail with `topology.mailbox_capacity_zero` naming the tasklet, where the
  core accepted the declaration before the check read the record

### Requirement: A Tasklet Context at Parity With the Other SDKs

The SDK SHALL provide the invocation context the authoring contract defines: plain, channel-selected
and keyed emits with metadata, the deterministic clock read, the stable identifier, timer
registration and cancellation, steps spawn, and the steps relay inbox. The reserved metadata
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

The Node channel builder SHALL offer `merge()`, returning a merge handle with `from(source, map)`, `keyBy(key)`, `into(tasklet)` and `into(tasklet, nodeName)`, with the `relay-graph` semantics: a source is an entry stream or an inter-tasklet stream declared on the same builder, each source's mapping composes after that source's own transforms, the key is declared once on the merge and applied to the mapped value on every source's edge, and `into` returns the merged tasklet's own output stream. The builder SHALL record each source as a chain closed by the core's `merge` link (`{"link": "merge", "tasklet", "group"}`) under one group per merge and SHALL take the merge refusals (`topology.merge_too_few_sources`, `topology.duplicate_merge_source`, `topology.edge_already_wired`) from the reference check, carrying no rule of its own. The builder SHALL offer `routeLatest(documents, traffic, keyFn)` and `routeLatestNamed(documents, traffic, keyFn, nodeName)` as the `relay-graph` router, packaged in the SDK as a single-cell tasklet with journaled `{document, version, delivered}` state. Stream handles SHALL offer `flatMap(fn)` beside `map` and `filter`: the closure answers an iterable, each element reaches the consumer on its own, an empty iterable is a drop, and the transform chain is applied as a list so `filter` and `flatMap` are one mechanism; the record carries the same `transform` link as `map`.

#### Scenario: Two channels merge into one tasklet
- **WHEN** a relay declares `c.merge().from(ticks, (t) => ({kind: 'tick', ...t})).from(quotes, (q) => ({kind: 'quote', ...q})).keyBy((v) => v.symbol).into(Detector)`
- **THEN** registration SHALL succeed with one Detector node fed by two consumer edges, each edge SHALL apply its own mapping, and a value from either entry SHALL reach Detector's handler as the mapped object keyed by `symbol`

#### Scenario: A duplicated source is refused by the reference name
- **WHEN** a merge declares the same stream twice
- **THEN** registration SHALL refuse with `topology.duplicate_merge_source` naming the channel, and a merge with one source SHALL refuse with `topology.merge_too_few_sources`

#### Scenario: One input becomes three deliveries
- **WHEN** an edge declares `.flatMap((line) => line.split(',').filter(Boolean))` into a tasklet and the dispatcher is handed `a,b,c`
- **THEN** the handler SHALL run three times, once per element, under one consumed offset, and a line the closure answers `[]` for SHALL run it zero times

#### Scenario: The router delivers on a key's own traffic
- **WHEN** `c.routeLatest(config, trades, (t) => t.symbol)` is declared and the harness feeds a document, then trades for `A` and `B`, then the same document, then a trade for `A`
- **THEN** exactly one `{key: 'A', document}` and one `{key: 'B', document}` SHALL be emitted, and the repeated document SHALL be inert

### Requirement: The single-tasklet short form is four definition fields
The short form SHALL be declared with `defineRelay`, the same function that declares a full-form relay, and SHALL NOT be declared with `defineTasklet`, which SHALL accept none of `output`, `input` or `codec` and SHALL throw `FlezhaError` naming the field and the relay function when one is given. Each SHALL accept `factoryId`, `output`, `input`, `codec` and `run` beside the tasklet fields it already reads; how it runs is the run branch it writes (`flow` or `steps`), as on the full form.

A definition SHALL be a short form exactly when it carries `run` and not `channels`; carrying both SHALL throw `FlezhaError` whose code is `topology.short_form_with_topology`, and carrying neither SHALL throw naming the missing body. `output` SHALL be optional. With `output`, the definition SHALL return a `ClassSpec` whose factory id is `factoryId`, wiring an entry channel named `input` (or the `input` field's value) into a generated tasklet named `factoryId` and that tasklet to the `output` plugin channel; with no `output`, it SHALL wire the entry channel into the tasklet and declare no plugin channel. A short form SHALL take the class settings as the full form does and SHALL refuse only `name` (`topology.setting_on_short_form`); a steps short form SHALL be a `once` steps relay unless `steps` writes another `each`, fed by its first message on the entry channel and taking `init_values` as the tasklet's initial state. `codec` SHALL take the vocabulary of `c.input(name, { codec })` and SHALL be lowered to that call, whether or not an `output` is written. The spec SHALL be built at definition time so a refusal is raised while the module loads, as a `dup_node` graph already is.

An absent or empty `factoryId` SHALL throw naming the required field. The runner's `registerFactory(specifier)` SHALL register the `ClassSpec` of every short form the module exports, as it registers every exported spec. A duplicate factory id SHALL throw with a message beginning `topology.duplicate_factory_id`. Whether `topology.output_without_type` can be raised at TypeScript's type level without a build step SHALL be decided at implementation and reflected in the manifest's `topology_surfaces`, as it is today.

#### Scenario: A module exporting one definition is a whole flow relay

- **WHEN** `./upcase.mjs` default-exports `defineRelay({ factoryId: 'upcase', output: 'out', codec: 'text', run })` and the runner is given `registerFactory('./upcase.mjs')`
- **THEN** the runner declares a flow relay `upcase` with entry channel `input` decoding text, a tasklet named `upcase` and output `out`, and the module calls no `defineTasklet`

#### Scenario: A module exporting one definition is a whole steps relay

- **WHEN** `./place.mjs` exports `defineRelay({ factoryId: 'place', output: 'placed', input: 'order', codec: 'text', run })`, the runner is given `registerFactory('./place.mjs')`, an instance is started, and one message is sent on `order`
- **THEN** `run` receives the message as text, the value it passes to `ctx.emit` reaches `placed`, and the instance completes

#### Scenario: A short-form steps relay declares its loop mode

- **WHEN** a module exports `defineRelay({ factoryId: 'chat', output: 'replies', codec: 'text', each: 'per_input', run })` and two messages reach its entry channel
- **THEN** the spec's `steps.each` is `per_input`, `run` runs once per message, and the instance does not complete after the first

#### Scenario: A short form with no output is declared

- **WHEN** a module exports `defineRelay({ factoryId: 'counter', input: 'ticks', codec: 'text', initState, applyChange, run })`
- **THEN** the relay declares `ticks` into the generated tasklet and no plugin channel

#### Scenario: A codec needs no output

- **WHEN** a definition carries `codec: 'text'` and no `output`
- **THEN** it is accepted and the entry channel decodes text

#### Scenario: A run body beside a channels body is refused

- **WHEN** a module calls `defineRelay({ factoryId: 'x', run, channels })`
- **THEN** loading it throws `FlezhaError` whose code is `topology.short_form_with_topology`

#### Scenario: A definition with neither body is refused

- **WHEN** a module calls `defineRelay({ factoryId: 'x' })`
- **THEN** loading it throws `FlezhaError` naming the missing `run` or `channels`

#### Scenario: A relay field on a tasklet is refused

- **WHEN** a module calls `defineTasklet({ name: 'upcase', output: 'out', run })`
- **THEN** loading it throws `FlezhaError` naming `output` and `defineRelay`

#### Scenario: A definition with no factory id is refused

- **WHEN** a module calls `defineRelay({ output: 'out', run })`
- **THEN** loading it throws `FlezhaError` naming the required `factoryId`

#### Scenario: One function defines every relay

- **WHEN** the SDK's exports are listed
- **THEN** `defineRelay` is the one relay-defining function, beside `defineTasklet`, and it defines a flow and a steps relay alike

#### Scenario: Plain JavaScript still needs no build step

- **WHEN** the short form is used from a `.mjs` module
- **THEN** it registers and runs with no transpilation

#### Scenario: A short form emits only through its context

- **WHEN** a module exports `defineRelay({ factoryId: 'upcase', output: 'out', codec: 'text', run: (line, ctx) => ctx.emit(line.toUpperCase()) })` and a second exports the same with `run: (line) => line.toUpperCase()`
- **THEN** `hello` sent to the first SHALL reach `out` as `HELLO`, nothing SHALL reach the second's `out`, and the second written in TypeScript SHALL fail to type-check

#### Scenario: A short form grows into its full form unchanged

- **WHEN** a short form's `initState`, `applyChange` and `run` are moved verbatim into `defineTasklet({ name: <the factory id>, ... })` wired by a `channels` body with the same entry and output
- **THEN** the full form SHALL declare the same record and SHALL restore the Cells the short form saved

### Requirement: A Node runner registers a factory with one awaited call
The runner SHALL provide `async registerFactory(specifier)` and `async unregisterFactory(specifier)`, and SHALL NOT provide the authoring functions or a `declare` method as runner methods. `registerFactory` SHALL be legal before and after `run()`: it SHALL import the module and read its exported relays (flow, steps or short form, the run read from each), rejecting with an error naming the specifier when the import fails or the module exports no relay; before `run()` it SHALL record the specifier for `run()`, and after `run()` it SHALL also declare every exported relay against the running core and every worker. `unregisterFactory` SHALL drop a specifier recorded before `run()`, rejecting naming the specifier when none was recorded, and after `run()` SHALL reject with an error stating that a running Node runner cannot stop serving a started declaration. The authoring functions SHALL be `defineTasklet` and `defineRelay`, module-level functions named for what they declare.

#### Scenario: Registration is awaited before run

- **WHEN** a program runs `const runner = createRunner('default'); await runner.registerFactory('./echo_relay.mjs'); await runner.run();`
- **THEN** the runner declares the module's relay at start

#### Scenario: A broken module rejects where it is registered

- **WHEN** a program awaits `runner.registerFactory('./missing.mjs')` before `run()`
- **THEN** the promise rejects with an error naming `./missing.mjs`, before `run()` is called

#### Scenario: A post-start registration reaches the running core

- **WHEN** `registerFactory('./late.mjs')` is awaited after `run()` has started the runner
- **THEN** the module's relay is declared to the core and to every worker, as `declare` did

#### Scenario: A post-start removal is rejected, not ignored

- **WHEN** `unregisterFactory('./echo_relay.mjs')` is awaited after `run()`
- **THEN** the promise rejects saying a running Node runner cannot stop serving a started declaration, and the relay stays declared

### Requirement: A Node relay definition carries the settings tree it wrote

`defineRelay` SHALL take the class settings as the tree, its run written as the branch key (`flow: {...}` or `steps: {...}`) and `durable` beside it, and `toHostClassDecl` SHALL put the tree on the wire as written with every key snake-cased, so a setting under a parent that does not carry it reaches the core and is refused as `topology.unknown_field`, naming the setting and its parent, when the runner declares the class to the shared core (at start, or at the registration itself for a class registered after start), as every other core refusal in this SDK is raised, as a `FlezhaError` naming it. A definition SHALL carry no `retryAttempts`, `retryBackoffMs`, `barrierRetryAttempts` or `wakeAfterMs`, and the declaration SHALL carry no `wake_after_ms`. A tasklet context inside a steps relay SHALL offer `timerAfter`, `timerAtEventTime` and `cancelTimer` exactly as inside a flow relay, and a tasklet's `onTimer` SHALL run on its firing in both runs; only a `compensate` hook's context SHALL throw an error naming `runtime.timers_unavailable` from them.

#### Scenario: A misplaced setting reaches the core

- **WHEN** a module exports `defineRelay({ factoryId: 'w', flow: { each: 'per_input' }, channels })` and the runner declares it
- **THEN** declaring it to the core throws an error naming `topology.unknown_field`, `each` and `flow`, and the relay is not declared

#### Scenario: Remote durability is a durable level

- **WHEN** a module exports `defineRelay({ factoryId: 'sd', steps: {}, durable: 'remote', channels })`
- **THEN** the declaration carries `durable: "remote"`, and the channel builder carries no durability call

#### Scenario: A timer inside a steps tasklet fires

- **WHEN** a tasklet of a running `per_input` steps relay calls `ctx.timerAfter(300)` in `run`
- **THEN** the call returns the scheduled timestamp, the effect carries the timer op, and `onTimer` runs on that Cell at that timestamp, opening a pass at that tasklet

#### Scenario: The retired wake field is refused

- **WHEN** a module exports `defineRelay({ factoryId: 'w', steps: {}, wakeAfterMs: 5000, channels })`
- **THEN** `defineRelay` refuses the unknown field by name (`topology.unknown_field`), and no declaration carries `wake_after_ms`

### Requirement: A value leaves a Node tasklet only through its context
A value SHALL leave a Node tasklet only through its context: `ctx.emit(value, options)`, with the `key`, `channel` and `metadata` options, and by no second spelling (`ctx.emitTo` does not exist). The SDK SHALL NOT emit a value returned from `run`, `onTimer` or `compensate`, under the dispatcher, in local mode and under `TaskletTestHarness` alike.

`TaskletDefinition` SHALL type `run`, `onTimer` and `compensate` to return `void | Promise<void>`, so that TypeScript refuses a hook that returns a value, whether it is written as an expression body, a block with `return value`, an `async` body or a reference to a function returning a value. These refusals SHALL be pinned by `@ts-expect-error` fixtures in `sdk/node/types-tests/`, checked by the existing negative control that strips each directive and requires its line to still produce a diagnostic. A plain JavaScript hook that returns a value SHALL run, and its value SHALL be ignored.

The output type SHALL NOT be inferred from a body. A tasklet that emits SHALL name its output type through the definition's type arguments (`defineTasklet<Trade, Candle>({ ... })`) or through the annotation of its context parameter (`ctx: Ctx<Candle, State, Change>`), and a tasklet that names none SHALL keep the output type `never`, so that TypeScript refuses an `emit` on it.

#### Scenario: A value leaves a tasklet only through the context

- **WHEN** a tasklet's `run` calls `ctx.emit('a')` and `ctx.emit('b', { key: 'k' })` for one input, and for the next input calls neither
- **THEN** the first input SHALL emit `a` and `b` keyed `k`, and the second SHALL emit nothing, under the dispatcher and under `TaskletTestHarness` alike

#### Scenario: A returned value is ignored in plain JavaScript

- **WHEN** a `.mjs` tasklet wired to an output declares `run: (input) => input.toUpperCase()` and receives `hello`
- **THEN** it SHALL run, nothing SHALL reach the output, and no error SHALL be raised

#### Scenario: A returned value does not type-check

- **WHEN** a TypeScript tasklet declares `run: (trade: Trade) => trade`, `run: async (trade: Trade) => trade`, or an `onTimer` that returns a value
- **THEN** `tsc --noEmit` SHALL fail on that line, and the fixture pinning it with `@ts-expect-error` SHALL keep the build green only while the refusal holds

#### Scenario: The output type is declared by type arguments or by the context

- **WHEN** one TypeScript tasklet is written `defineTasklet<Trade, Candle>({ name: 'ohlc', run: (trade, ctx) => ctx.emit(candleOf(trade)) })` and another annotates `run: (trade: Trade, ctx: Ctx<Candle, Window, Move>) => { ... }` beside its `initState` and `applyChange`
- **THEN** both SHALL type-check, `.into(Publish)` behind either SHALL type-check where `Publish` reads `Candle` and SHALL fail where it reads `Trade`

#### Scenario: An emit with no declared output does not type-check

- **WHEN** a TypeScript tasklet that names no output type calls `ctx.emit(value)`
- **THEN** `tsc --noEmit` SHALL fail on that call, because the output type is `never`

### Requirement: A Node tasklet declares its settings as fields of its definition
A Node tasklet's settings SHALL be fields of the object handed to `defineTasklet`, or of a short form's definition.

The idle TTL SHALL be `idleTtlSeconds`, a positive whole number of seconds, and the declaration record SHALL carry it as `idle_ttl_ms`, that number times 1000. Unset SHALL mean no eviction. Any other value SHALL be refused with a `FlezhaError` naming `idleTtlSeconds` and the tasklet. `maxCells` SHALL be a top-level field with its existing meaning.

The clock SHALL follow from what the definition writes: event time when it writes `eventTime`, logical when it writes `logicalClock: true`, and the wall clock otherwise. A definition writing both SHALL be refused with a `FlezhaError` naming `eventTime` and `logicalClock`. A logical clock on a tasklet with no `applyChange` SHALL keep the core's `topology.unservable_clock` refusal. The SDK SHALL NOT have a `clock` field or export a `ClockKind` type.

The initial state SHALL come from `initState(cellKey)`, and its absence SHALL declare a stateless tasklet. A definition writing `applyChange` without `initState` SHALL be refused with a `FlezhaError` naming both. A change SHALL be any value the codec carries, recorded with `ctx.change(value)` and folded by the definition's `applyChange(state, change)`, and no change class SHALL be required; `stateCodec` and `changeCodec` SHALL stay. `migrateState(fromVersion, bytes)` and `migrateChange(fromVersion, bytes)` SHALL keep receiving bytes.

`defineTasklet` SHALL refuse a field it does not read with a `FlezhaError` naming the field and the tasklet, as a relay definition refuses its own unknown fields, whether the definition carries it as its own or inherits it. It SHALL NOT map a retired field (`dynamicCells`, `clock`, `initialState`) to its successor; the refusal SHALL name what replaced it.

#### Scenario: The idle TTL is whole seconds

- **WHEN** a tasklet reached by `keyBy` is defined with `{ name: 'sessions', idleTtlSeconds: 3600, run }`
- **THEN** the declaration record SHALL carry `idle_ttl_ms` `3600000` for that tasklet

#### Scenario: An idle TTL that is not a positive whole number is refused

- **WHEN** a tasklet is defined with `idleTtlSeconds` `0.2`, `0` or `'60'`
- **THEN** `defineTasklet` SHALL throw `FlezhaError` naming `idleTtlSeconds` and the tasklet, while the module loads

#### Scenario: Writing eventTime makes the clock event time

- **WHEN** a tasklet writes `eventTime: (trade) => trade.at` and no clock field, and a trade with `at` `1720000000000` is dispatched
- **THEN** the declaration SHALL carry clock `event_time`, and `ctx.now()` inside `run` SHALL return `1720000000000n`

#### Scenario: logicalClock declares the logical clock

- **WHEN** a tasklet writing `initState` and `applyChange` is defined with `logicalClock: true`
- **THEN** the declaration SHALL carry clock `logical`, and `ctx.now()` SHALL serve the Cell's change sequence

#### Scenario: A tasklet with neither is on the wall clock

- **WHEN** a tasklet writes neither `eventTime` nor `logicalClock`
- **THEN** the declaration SHALL carry clock `wall`

#### Scenario: Event time and the logical clock together are refused

- **WHEN** a tasklet writes `eventTime` and `logicalClock: true`
- **THEN** `defineTasklet` SHALL throw `FlezhaError` naming `eventTime` and `logicalClock`

#### Scenario: A logical clock on a tasklet with no fold is refused

- **WHEN** a tasklet with no `applyChange` writes `logicalClock: true` and its relay is declared
- **THEN** the declaration SHALL be refused with `topology.unservable_clock` naming the tasklet

#### Scenario: The initial state comes from initState

- **WHEN** a tasklet writes `initState: (cellKey) => ({ key: cellKey, total: 0 })` and a Cell is created for key `A`
- **THEN** the Cell's state SHALL start as `{ key: 'A', total: 0 }`, and a single-cell tasklet's as what `initState(undefined)` returns

#### Scenario: A fold with no initial state is refused

- **WHEN** a tasklet writes `applyChange` and no `initState`
- **THEN** `defineTasklet` SHALL throw `FlezhaError` naming `applyChange` and `initState`

#### Scenario: A change needs no class

- **WHEN** a tasklet calls `ctx.change({ kind: 'event', at: 5 })` and its `applyChange` folds that object into its state
- **THEN** the change SHALL be journaled as MessagePack, folded live, and folded again to an equal state on restore, with no class defined for it

#### Scenario: A retired or unknown tasklet field is refused

- **WHEN** a `.mjs` module calls `defineTasklet` with `dynamicCells: {}`, `clock: 'logical'`, `initialState` or a misspelt field beside `name` and `run`
- **THEN** loading it SHALL throw `FlezhaError` naming that field and the tasklet, and the same object literal in TypeScript SHALL fail to type-check

### Requirement: A Node tasklet's cell mode is read from the wiring
The core SHALL derive the cell mode per tasklet node from what is declared, and the SDK SHALL send none: static when the tasklet's definition writes `cells`, dynamic when a chain of the relay records a `key` link (from `keyBy` on a stream or on a merge) before the `into` or `merge` link that names the node, and single otherwise. The SDK SHALL NOT have a `dynamicCells` field or export a `DynamicCells` type. A tasklet writing `cells` and reached by a `keyBy` edge SHALL be refused with the core's `topology.key_by_on_unkeyable_target`, as a `FlezhaError` carrying that name. A dynamic node SHALL honour its `idleTtlSeconds` and `maxCells` through the core's existing eviction.

#### Scenario: keyBy alone makes a tasklet dynamic

- **WHEN** a relay declares `c.input('orders').keyBy((o) => o.customer).into(Totals)` and `Totals` writes only `name`, `initState`, `applyChange` and `run`
- **THEN** the core SHALL derive dynamic mode for `Totals`, and each customer SHALL get its own Cell

#### Scenario: A keyed merge makes its tasklet dynamic

- **WHEN** a relay declares `c.merge().from(a, f).from(b, g).keyBy((v) => v.symbol).into(Detector)`
- **THEN** the core SHALL derive dynamic mode for `Detector`

#### Scenario: One definition takes its mode from each wiring

- **WHEN** one tasklet definition is wired through `keyBy` in one relay and without it in another
- **THEN** the core SHALL derive dynamic mode for it in the first relay and single in the second

#### Scenario: A static key set fed by keyBy is refused

- **WHEN** a tasklet writing `cells: { keys: ['a', 'b'], cellsFor }` is reached by an edge declaring `keyBy`
- **THEN** declaring the relay SHALL throw `FlezhaError` whose refusals contain `topology.key_by_on_unkeyable_target`, naming the tasklet

### Requirement: A Node cell belongs to one relay
The Node runner SHALL keep a Cell's live state by its relay, its tasklet and its cell key, so that two relays of one factory served by the same runner, such as two instances of a steps relay or two partitions of a flow one, never read or write each other's state. Every activation that reads or writes a Cell (work, restore fold, snapshot, live state read, migration serialize, apply state and compensate) SHALL address it this way.

#### Scenario: Two relays of one factory keep their own cell

- **WHEN** one runner answers work for relays `order_a` and `order_b` of one factory, on the same tasklet and cell key
- **THEN** each relay's live state SHALL hold only its own changes, and a restore, an applied state, a snapshot, a migration serialize and a compensation for one SHALL read or write that relay's cell alone

### Requirement: The Node runner SHALL carry the shared lifecycle verbs

The Node `Runner` SHALL offer `start()`, which connects and resolves once the runner has its id,
`awaitTermination()`, which resolves when the runner has shut down, `run()`, exactly `start()`
followed by `awaitTermination()`, and `close()`, which stops fetching, drains and releases the core and
is idempotent; `stop()` SHALL NOT exist. A tasklet's static cells SHALL be declared with `cellKeys`
and `cellsFor` on the definition, not under a `cells` object, and the test harness SHALL drive an
input with `feed`, answer an `Outcome` and expose the Cell's state as `state`.

#### Scenario: start resolves before shutdown

- **WHEN** application code awaits `runner.start()` with a controller running
- **THEN** the promise SHALL resolve once the runner is assigned an id, and `awaitTermination()` SHALL stay pending until `close()` is called

#### Scenario: close is idempotent

- **WHEN** `close()` is awaited twice on a started runner
- **THEN** the second call SHALL resolve without effect

### Requirement: No Blocking Core Call Is Synchronously Reachable From JavaScript

The SDK SHALL NOT expose any blocking core entry point as a synchronous call from JavaScript.
`flz_core_poll` SHALL NOT be exported to JavaScript at all. `flz_core_complete`,
`flz_core_spawn_result` read with a nonzero timeout, and `flz_core_start`
SHALL be reachable only as asynchronous operations whose blocking work executes on a thread that is
not an event-loop thread.

This requirement exists to close a process-wide deadlock. Under the default `DropNothing` overflow
policy a completion's routing push waits until the consumer's mailbox drains, and the consumer
drains only when its handler runs. If the waiting thread is the event loop, the loop is the only
thread that could run that handler, and no other thread can free it.

Non-blocking entry points (abandon, buffer free, monotonic clock, last-error, `flz_core_spawn_request`,
and `flz_core_spawn_result` read with a timeout of zero) MAY be synchronous. `ctx.spawnSteps` SHALL
therefore return its handle synchronously, the handle's `tryResult()` SHALL be synchronous, and only
its `wait()` SHALL be asynchronous.

#### Scenario: A blocking entry point cannot be called from the event loop
- **WHEN** the veneer's exported surface is enumerated in a test
- **THEN** every blocking core entry point SHALL be absent or asynchronous, and the absence SHALL be
  asserted rather than reviewed

#### Scenario: A full no-drop consumer does not wedge the process
- **WHEN** a tasklet emits onto a full consumer mailbox declared `DropNothing`, with the
  consumer's handler runnable only on an event loop
- **THEN** the completion SHALL wait off-loop, the consumer's activation SHALL be dispatched and
  drained, and the completion SHALL then return

#### Scenario: Spawn returns on the event loop and waits off it
- **WHEN** a tasklet calls `ctx.spawnSteps('worker', payload)` on an event loop and then
  awaits `handle.wait()` while the controller is slow to answer
- **THEN** the spawn call SHALL return the handle without blocking the loop, `tryResult()` SHALL
  answer pending without blocking the loop, and the waiting read SHALL run on a thread that is not
  an event-loop thread

### Requirement: Fetch Threads Are the SDK's Own and Never the Runtime's Shared Pool

The SDK SHALL poll on native threads it owns, and SHALL NOT poll on the JavaScript runtime's shared
I/O threadpool. A fetch thread SHALL NOT execute user code, so that it is always available to enter
`poll`. The fetcher count SHALL be configurable and SHALL default to the value the Java host uses.

#### Scenario: Polling does not consume shared I/O capacity
- **WHEN** the runner is polling with every fetcher blocked in the core
- **THEN** the runtime's shared I/O threadpool SHALL remain fully available to unrelated file,
  name-resolution and cryptographic work

### Requirement: The Completion Pool Grows So a Blocked Push Cannot Exhaust It

Completions SHALL execute on a pool of native threads whose size grows when a completion is queued
and every thread is occupied, up to a configured ceiling. The pool SHALL be sized through the
existing worker-pool base and ceiling configuration rather than through new configuration
vocabulary, and threads above the base SHALL retire after an idle streak.

A fixed pool is refused: if every thread parks pushing to one consumer, the result that would drain
that consumer has no thread to run on, and the deadlock re-forms one level out from the one the
event-loop rule closes.

Growth SHALL be driven by the host's own observation of a queued completion with no free thread,
because the core cannot observe the host's completion queue.

#### Scenario: A blocked push does not exhaust the pool
- **WHEN** every completion thread is parked pushing to a full consumer mailbox and a further
  completion is queued
- **THEN** the pool SHALL grow, the queued completion SHALL execute, and the runner SHALL make
  progress

#### Scenario: The pool returns to base
- **WHEN** the condition that grew the pool has cleared and the added threads have been idle for the
  retirement streak
- **THEN** the pool SHALL return to its base size

### Requirement: A Starvation Supervisor Covers Queued Direct Activations

The SDK SHALL park one thread in the core's starvation wait, and SHALL respond to an observation by
making a thread available to enter `poll`. The supervisor SHALL end when the runner shuts down,
observing the same three-way shape as `poll`.

This covers the condition the core can observe — a queued direct activation with no thread inside
`poll` — which is distinct from, and not a substitute for, the completion-pool growth rule.

#### Scenario: A queued direct activation is promoted
- **WHEN** a restore fold, snapshot request or steps relay step is queued with no thread inside `poll`
  for the core's dwell
- **THEN** the observation SHALL result in a thread entering `poll` and the activation SHALL be
  handed out

### Requirement: Dispatch Targets One of N Event Loops Over a Single Code Path

An activation SHALL be handed to one event loop selected by the host, and the selection SHALL be the
only difference between the single-loop and multi-loop configurations. The number of dispatch loops
SHALL be configurable both in code and by environment variable, SHALL default to the single-loop
configuration, and SHALL NOT require a different authoring surface, registration form, or dispatch
path.

The selection SHALL send every activation addressed to a cell to the same loop, and SHALL spread
only the prepare crossing — the one activation that names no cell, because the key is what it is
computing — across loops. A dispatch loop is a separate isolate holding the state its restore fold
materialized, so an activation for that cell sent elsewhere would find nothing to fold into.
Exclusivity makes either selection safe for concurrency; only one of them is safe for state.

A cell that carries no key is still a cell: a single-cell tasklet holds state exactly as a
keyed one does, and an absent key SHALL NOT be read as an absent cell.

A whole-relay leave names every cell of its relay, which sit on whichever loops their keys
selected, so it SHALL reach every loop before any activation handed out after it, and the core
SHALL be answered once.

#### Scenario: A cell's activations reach the loop holding its state
- **WHEN** a cell's restore fold has run on one dispatch loop under a multi-loop configuration
- **THEN** its later activations SHALL be dispatched to that same loop

#### Scenario: A single-cell tasklet is not spread across loops
- **WHEN** successive activations reach a tasklet that declares no keys under a multi-loop
  configuration
- **THEN** they SHALL all be dispatched to the loop holding that tasklet's one cell

#### Scenario: A relay started again after a stop starts its cells from initial state
- **WHEN** a relay whose cells sit on several loops is stopped and started again under its id
- **THEN** every loop SHALL have dropped the stopped relay's cells before the new start's first
  activation reaches it

#### Scenario: The tiers share one dispatch path
- **WHEN** the same handler module is run under the single-loop and multi-loop configurations
- **THEN** dispatch, completion, ordering and results SHALL be identical, and the handler source
  SHALL be unchanged

#### Scenario: Per-cell ordering holds across loops
- **WHEN** successive inputs for one cell are dispatched under a multi-loop configuration
- **THEN** they SHALL be processed in enqueue order regardless of which loop takes each activation

### Requirement: Handlers Are Registered by Module Specifier

The runner entry point SHALL accept handler modules by specifier rather than as already-imported
values, in every dispatch configuration. A dispatch loop SHALL import the module itself.

Accepting an imported value is refused because a live value cannot cross into a separate isolate, so
it would make enabling additional dispatch loops a breaking change to the authoring surface.

#### Scenario: Enabling more loops is a flag, not a rewrite
- **WHEN** a runner written against the single-loop default is reconfigured for multiple loops
- **THEN** no change to the handler module or its registration SHALL be required

#### Scenario: An unimportable module is named
- **WHEN** a registered specifier cannot be imported
- **THEN** startup SHALL fail naming the specifier and chaining the original import error

### Requirement: Module-Scope State Is Per Dispatch Loop, and the SDK Says So

The SDK SHALL document that state held at module scope is instantiated once per dispatch loop, and
SHALL prove both configurations in its test app. Durable state SHALL be unaffected, being owned by
the core through the declared change fold.

#### Scenario: Isolation is observable and specified
- **WHEN** a handler module holds module-scope state and runs under a multi-loop configuration
- **THEN** each loop SHALL hold its own instance, and this SHALL be asserted by a test rather than
  described only in prose

### Requirement: Every Activation Is Answered Exactly Once Through a Structural Guard

Every dispatched activation SHALL be answered exactly once — completed, failed, or abandoned —
through a guard that runs on any exit path, including a thrown error, an unhandled promise
rejection, and the termination of the dispatch loop that held it. The guard SHALL NOT be a
convention applied at each return site.

An unanswered activation retires its cell for the life of the process, so this is a correctness
requirement rather than a hygiene one.

#### Scenario: A throwing handler abandons rather than retiring its cell
- **WHEN** a handler throws, or its returned promise rejects
- **THEN** the activation SHALL be answered, and the cell SHALL receive subsequent activations

#### Scenario: A terminating dispatch loop abandons what it held
- **WHEN** a dispatch loop terminates with activations outstanding
- **THEN** every outstanding activation SHALL be abandoned before the replacement loop begins
  dispatching

### Requirement: A Dispatch Loop That Ends Is Replaced

A dispatch loop that ends for any reason other than shutdown SHALL be replaced without user
action, and its departure SHALL NOT permanently reduce the host's dispatch capacity. A fetch thread
that ends on an error SHALL likewise be replaced.

The single-loop configuration SHALL document that the main event loop cannot be replaced, and that
this is the reason a multi-loop configuration is the recommended production posture.

#### Scenario: A dead worker is replaced
- **WHEN** a dispatch worker terminates unexpectedly
- **THEN** a replacement SHALL start, the configured loop count SHALL be restored, and the failure
  SHALL be reported

### Requirement: Event-Loop Delay Is Observed and Reported

The SDK SHALL observe its dispatch loops' event-loop delay and SHALL report a warning, naming the
tasklet most recently dispatched on that loop, when delay crosses a documented threshold.

A synchronous compute-bound handler is the one failure this SDK cannot prevent, so it SHALL be
diagnosable at its source rather than only as a controller-side stall verdict.

#### Scenario: A blocking handler is diagnosed at the host
- **WHEN** a handler occupies its dispatch loop past the delay threshold
- **THEN** the SDK SHALL warn naming the tasklet and the observed delay, and SHALL point at the
  multi-loop configuration as the remedy

### Requirement: Shutdown Drains In-Flight Activations

Shutdown SHALL be observed from a signal, from end of file on standard input, and from a
controller-requested stop, and SHALL drain in-flight activations before exit. The poll timeout SHALL
bound how quickly a shutdown request is observed, and that bound SHALL be documented.

#### Scenario: Every dispatch loop is drained
- **WHEN** shutdown is requested with activations in flight across several dispatch loops
- **THEN** every loop SHALL be drained or its activations abandoned, and the process SHALL exit
  without requiring a further signal

#### Scenario: A held loop does not prevent the runner from being stopped
- **WHEN** end of file is reached on standard input while a handler is holding its dispatch loop
- **THEN** the end of file SHALL be observed off that loop, the remaining loops SHALL be given the
  documented grace period to drain, and the process SHALL end without a signal

### Requirement: The Parallelism Model Is Stated as Arithmetic

The SDK's documentation SHALL state its concurrency limits as numbers rather than adjectives: that
concurrent activations are bounded by the core's in-flight cap and not by the fetcher or loop count,
that compute parallelism equals the dispatch loop count, and that the single-loop default has a
compute parallelism of one.

#### Scenario: The documentation states the bound
- **WHEN** a user reads the concurrency documentation
- **THEN** it SHALL name the in-flight cap, the fetcher default, the loop count's role, and which of
  those bounds which kind of work

### Requirement: The Node Veneer Is a Feature on the Existing ABI Crate

The Node binding SHALL be built from the existing C ABI crate under its own feature, beside the
Java shim, and SHALL be a one-to-one veneer over the same entry points. It SHALL NOT introduce a
second crate, a second copy of the ABI, or a kernel capability the C ABI does not expose.

Enabling the feature SHALL NOT alter the crate's existing artifacts or the binaries other SDKs ship.

#### Scenario: The veneer adds no ABI surface
- **WHEN** the Node veneer's entry points are compared with the C ABI's
- **THEN** each SHALL correspond to an existing entry point, and no new kernel capability SHALL be
  reachable through it

#### Scenario: The feature is additive
- **WHEN** the crate is built with and without the Node feature
- **THEN** the existing artifacts SHALL be unchanged and the Java shim SHALL be unaffected

### Requirement: Schema Version Is Negotiated Before Any Controller Connection

The binding SHALL pass its compiled-against activation schema version at context creation, and a
mismatch SHALL fail with an error naming both versions before any controller connection is
attempted.

#### Scenario: A skewed binary refuses to start
- **WHEN** the JavaScript package and the native binary were built against different schema versions
- **THEN** startup SHALL fail naming both versions, and no connection SHALL be attempted

### Requirement: Payloads Cross the Boundary Without an Intermediate Copy

User payload bytes SHALL be exposed to JavaScript without an intermediate allocation, and every
buffer the core hands out SHALL be freed through the ABI's free entry point before control returns,
so that no native buffer escapes into the JavaScript heap unmanaged.

#### Scenario: No native buffer escapes
- **WHEN** activations are dispatched and completed under sustained load
- **THEN** native memory SHALL stay bounded and every core-allocated buffer SHALL have been freed

### Requirement: Errors and Panics Map Onto Host Errors

Core errors SHALL surface as host errors preserving the engine's detail, and a panic crossing the
boundary SHALL become a host error rather than terminating the process.

#### Scenario: A core error carries its detail
- **WHEN** an entry point fails
- **THEN** the thrown error SHALL carry the core's recorded reason

### Requirement: Prebuilt Binaries Ship as Per-Platform Optional Dependencies

The package SHALL distribute native binaries as separate per-platform packages resolved through
optional dependencies, with a source build from a checkout as the fallback, so that installing the
SDK requires neither a Rust toolchain nor downloading binaries for platforms the user does not run.
Every per-platform package the SDK declares SHALL be produced by the build and published at the same
version as the parent package, and SHALL be published before it, so the parent never declares a
version that cannot yet resolve. The set of declared packages and the set the loader resolves SHALL derive from
one table, so a platform cannot be added to one and omitted from the other. When no platform
package resolved and no fallback is available, the SDK SHALL refuse at load with a message naming
the detected platform, the package it looked for, and how to build the binary locally.

Bundling every supported triple into one artifact is refused: it is the distribution shape already
recorded as owed a split, and this package has an established convention for doing it correctly from
the start.

#### Scenario: One platform's binary is installed
- **WHEN** the package is installed on a supported platform
- **THEN** only that platform's binary SHALL be fetched, and no toolchain SHALL be required

#### Scenario: An unsupported platform is told what a source build needs
- **WHEN** the package is installed on a platform no per-platform package is published for
- **THEN** the SDK SHALL name that platform as unpublished and state the source build's
  requirements, rather than naming a package that does not exist

#### Scenario: Every declared platform package is produced
- **WHEN** the build's published artefacts are enumerated
- **THEN** every package named in the SDK's optional dependencies SHALL be among them, at the parent's version

#### Scenario: A resolution failure is not silent
- **WHEN** the SDK is loaded on an installation where no platform package resolved and no fallback binary is present
- **THEN** it SHALL raise an error naming the detected platform, the expected package and the local build command, rather than surfacing a dynamic-loader error

#### Scenario: The shipped file list matches the documented fallback
- **WHEN** the package's file list is compared against the fallback the documentation describes
- **THEN** the fallback SHALL be present in the published package, or the documentation SHALL state that it is available only in a source checkout

### Requirement: One Binary Serves Every Supported Host Version

The binding SHALL target the stable ABI its host runtime provides, so that one binary per platform
serves every supported host version with no per-version rebuild. The supported host version window
SHALL be documented, and the package SHALL be consumable from both module systems.

#### Scenario: A host upgrade needs no rebuild
- **WHEN** the host runtime is upgraded within the supported window
- **THEN** the installed binary SHALL continue to load

#### Scenario: Both module systems import the package
- **WHEN** the package is imported from an ECMAScript module and required from a CommonJS module
- **THEN** both SHALL resolve and expose the same surface

### Requirement: The Node Test App Is Admitted by the Conformance Suite

The SDK SHALL ship a Node test app with a conformance manifest declaring its runtime, command
template, test and bench artifact profiles, build command, connect timeout, transports, deferred
capabilities and fixtures, and SHALL be dispatchable as a runtime option in the nightly end-to-end
workflow. The suite SHALL pass for every capability the manifest does not defer, over every
transport the manifest lists.

Admitting the runtime SHALL require no change to the shared test helpers, the system suites or the
test helper configuration — a new SDK adds an app, not harness clauses. The conformance gate SHALL
take one row of data: where that SDK spells its language byte, so the manifest is checked against
the constant rather than trusted.

#### Scenario: The suite admits the runtime
- **WHEN** the end-to-end suite is run against the Node runtime
- **THEN** every non-deferred capability SHALL pass, and the smoke tier's controller-side
  declaration snapshot SHALL match the reference

#### Scenario: The manifest is the scope matrix
- **WHEN** the SDK's documentation names the capabilities it carries
- **THEN** it SHALL point at the manifest rather than restate it, and a discrepancy SHALL fail the
  gate

### Requirement: Both Dispatch Configurations Are an Axis of the Run

The dispatch loop count SHALL be an axis of the end-to-end run rather than a property of a fixture
file. One test app SHALL be run under both the single-loop and multi-loop configurations, and there
SHALL be no per-configuration fixture set.

#### Scenario: One app proves both configurations
- **WHEN** the suite is run under each dispatch configuration
- **THEN** the same fixtures SHALL execute, and results SHALL be identical for every capability that
  does not concern dispatch topology

### Requirement: The Pool-Wedge Capability Is Decided by the Drill

The manifest SHALL claim the pool-wedge capability if the multi-loop configuration produces the
wedge signature under the drill, and SHALL otherwise defer it by name with a recorded follow-up. The
claim SHALL NOT be asserted from the design.

#### Scenario: The drill decides the claim
- **WHEN** the wedge drill is run against the Node runner
- **THEN** the capability SHALL be claimed or deferred according to the drill's result, and the
  manifest SHALL record which

### Requirement: The Node Runner Honours the Harness Teardown Contract

The Node test app SHALL terminate when its standard input reaches end of file, so the harness can
stop runners without signals, and SHALL drain in-flight activations before exiting.

#### Scenario: Closing stdin stops the runner
- **WHEN** the harness closes the runner's standard input
- **THEN** the runner SHALL drain its in-flight activations and exit without requiring a signal

### Requirement: A Tasklet Test Harness at Parity With the Other SDKs

The SDK SHALL ship a tasklet test harness at parity with those the Rust and Java SDKs provide:
construction from an initial state or from a restore payload, application of inputs, timer firing,
event-time advance, inspection of recorded changes and emits, and a replay-equality gate that folds
the recorded change log against a freshly constructed initial state and reports a mismatch with both
states for inspection.

A handler SHALL remain an ordinary function that a plain unit test can invoke with no runtime present
and no controller running.

#### Scenario: Replay equality is gated in a unit test
- **WHEN** a handler whose change log does not reproduce its terminal state is exercised through the
  harness
- **THEN** the replay-equality gate SHALL report a mismatch carrying the expected and actual states

#### Scenario: A handler is testable with no runtime
- **WHEN** a defined handler is called directly from a unit test with no runner started
- **THEN** it SHALL execute as an ordinary function

### Requirement: Node Is a Measured Runtime, Not an Assumed One

The round-trip benchmark SHALL accept the Node runtime as an arm, and the SDK SHALL land with
per-runtime numbers recorded for it rather than inheriting another runtime's. Measurements SHALL
interleave arms inside a round, and SHALL report the dispatch configuration under which they were
taken.

#### Scenario: Numbers are attributed to the runtime and configuration
- **WHEN** the round-trip benchmark is run for the Node runtime
- **THEN** the recorded results SHALL name the runtime and the dispatch configuration, and SHALL NOT
  be reported as another runtime's

### Requirement: The Package Has Its Own Test and Build Workflow

The SDK SHALL ship a continuous-integration workflow running its unit tests, its type-check
fixtures including those expected to fail compilation, and its native binary builds for every
supported platform.

#### Scenario: A broken refusal fails the build
- **WHEN** a compile-time refusal stops working
- **THEN** the fixture asserting that refusal SHALL fail the workflow
