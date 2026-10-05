# node-sdk Specification

## Purpose
TBD - created by archiving change node-sdk. Update Purpose after archive.
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

The Node channel builder SHALL offer `merge()`, returning a merge handle with `from(source, map)`, `keyBy(key)`, `into(tasklet)` and `into(tasklet, nodeName)`, with the `multi-input-tasklets` semantics: a source is an entry stream or an inter-tasklet stream declared on the same builder, each source's mapping composes after that source's own transforms, the key is declared once on the merge and applied to the mapped value on every source's edge, and `into` returns the merged tasklet's own output stream. The builder SHALL record each source as a chain closed by the core's `merge` link (`{"link": "merge", "tasklet", "group"}`) under one group per merge and SHALL take the merge refusals (`topology.merge_too_few_sources`, `topology.duplicate_merge_source`, `topology.edge_already_wired`) from the reference check, carrying no rule of its own. The builder SHALL offer `routeLatest(documents, traffic, keyFn)` and `routeLatestNamed(documents, traffic, keyFn, nodeName)` as the `sdk-graph-authoring` router, packaged in the SDK as a single-cell tasklet with journaled `{document, version, delivered}` state. Stream handles SHALL offer `flatMap(fn)` beside `map` and `filter`: the closure answers an iterable, each element reaches the consumer on its own, an empty iterable is a drop, and the transform chain is applied as a list so `filter` and `flatMap` are one mechanism; the record carries the same `transform` link as `map`.

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
A value SHALL leave a Node tasklet only through its context: `ctx.emit(value, options)`, with the `key`, `channel` and `metadata` options, and `ctx.emitTo(channel, value, options)`. The SDK SHALL NOT emit a value returned from `run`, `onTimer` or `compensate`, under the dispatcher, in local mode and under `TaskletTestHarness` alike.

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
The cell mode SHALL be read per tasklet node from what is declared, never from a flag: static when the tasklet's definition writes `cells`, dynamic when a chain of the relay records a `key` link (from `keyBy` on a stream or on a merge) before the `into` or `merge` link that names the node, and single otherwise. The SDK SHALL NOT have a `dynamicCells` field or export a `DynamicCells` type. A tasklet writing `cells` and reached by a `keyBy` edge SHALL be refused with the core's `topology.key_by_on_unkeyable_target`, as a `FlezhaError` carrying that name. A dynamic node SHALL honour its `idleTtlSeconds` and `maxCells` through the core's existing eviction.

#### Scenario: keyBy alone makes a tasklet dynamic

- **WHEN** a relay declares `c.input('orders').keyBy((o) => o.customer).into(Totals)` and `Totals` writes only `name`, `initState`, `applyChange` and `run`
- **THEN** the declaration record SHALL carry `cell_mode` `dynamic` for `Totals`, and each customer SHALL get its own Cell

#### Scenario: A keyed merge makes its tasklet dynamic

- **WHEN** a relay declares `c.merge().from(a, f).from(b, g).keyBy((v) => v.symbol).into(Detector)`
- **THEN** the declaration record SHALL carry `cell_mode` `dynamic` for `Detector`

#### Scenario: One definition takes its mode from each wiring

- **WHEN** one tasklet definition is wired through `keyBy` in one relay and without it in another
- **THEN** the first relay's record SHALL carry `cell_mode` `dynamic` for it and the second's `single`

#### Scenario: A static key set fed by keyBy is refused

- **WHEN** a tasklet writing `cells: { keys: ['a', 'b'], cellsFor }` is reached by an edge declaring `keyBy`
- **THEN** declaring the relay SHALL throw `FlezhaError` whose refusals contain `topology.key_by_on_unkeyable_target`, naming the tasklet

### Requirement: A Node cell belongs to one relay
The Node runner SHALL keep a Cell's live state by its relay, its tasklet and its cell key, so that two relays of one factory served by the same runner, such as two instances of a steps relay or two partitions of a flow one, never read or write each other's state. Every activation that reads or writes a Cell (work, restore fold, snapshot, live state read, migration serialize, apply state and compensate) SHALL address it this way.

#### Scenario: Two relays of one factory keep their own cell

- **WHEN** one runner answers work for relays `order_a` and `order_b` of one factory, on the same tasklet and cell key
- **THEN** each relay's live state SHALL hold only its own changes, and a restore, an applied state, a snapshot, a migration serialize and a compensation for one SHALL read or write that relay's cell alone

