# php-sdk Specification

## Purpose

The PHP SDK. Its authoring surface carries the full host-class declaration at parity with the other
SDKs: tasklets declared on one attribute, the short form, per-cell dispatch and timers, edge
transforms and keys, tiers, checkpoint intervals and dead letters, steps relays, deterministic
effects and author-declared JSON state, judged against the reference topology check. Concurrency
goes through an Executor seam that owns loop multiplicity but never per-cell correctness:
`SyncExecutor` is the zero-dependency default and the optional `SwooleExecutor` gives one runner
every core, both gated by the same suites. It ships as one Composer package with a declared PHP
floor that obtains the native binary at install, negotiates the schema version, runs one runner per
process, and is developed and tested from its own tree.

## Requirements

### Requirement: PHP SDK entry point and runner lifecycle
The PHP SDK SHALL expose a runner handle created by `Flezha::createRunner(string $namespace)`, carrying declaration and lifecycle on one object, with `registerFactory()` registering a relay class of either run (the run is read from its `#[Relay]`), `unregisterFactory()` as its inverse, `executor()` selecting the concurrency adapter, `start()` connecting, `awaitTermination()` blocking, and `run()` as `start()` + `awaitTermination()`.

The runner SHALL map onto the ABI lifecycle `flz_core_context_new(SCHEMA_VERSION)` → `flz_core_declare_class` per class → `flz_core_start(namespace)` → poll/complete → `flz_core_shutdown` → `flz_core_context_free`. Naming SHALL follow `docs/internal/sdk/naming_conventions.md`, matching the Rust and Java surfaces.

#### Scenario: A declared flow relay connects and is placed
- **WHEN** a script calls `Flezha::createRunner('trading')->registerFactory(OrderTotals::class)->run()` against a live controller
- **THEN** the runner SHALL connect, `AddRelay` SHALL place an instance of that class, and inputs routed to it SHALL reach the declared tasklet

#### Scenario: Zero declared relay types is legal
- **WHEN** a runner is started having declared no relay class
- **THEN** it SHALL connect successfully and remain connected, matching the Rust and Java behaviour for an empty factory list

#### Scenario: Schema mismatch is refused before connecting
- **WHEN** the PHP shim's compiled `SCHEMA_VERSION` differs from the loaded `libflz_core_ffi`
- **THEN** `createRunner` SHALL raise an error naming both versions, and no controller connection SHALL be attempted

#### Scenario: Missing ext-ffi fails with an actionable message
- **WHEN** the SDK starts on a PHP build where the `FFI` class is unavailable or `ffi.enable` forbids the call
- **THEN** it SHALL raise an error naming the required extension and ini setting, rather than a bare "Class FFI not found" fatal

### Requirement: Tasklet authoring surface and per-cell dispatch
The PHP SDK SHALL define an abstract `Flezha\Tasklet` class whose subclasses declare their initial `State` by overriding `initState(?string $cellKey)`, a `run(mixed $input, Ctx $ctx): void` invoked per input, and an `applyChange(mixed &$state, mixed $change)` that folds a Change into State, with a `Ctx` supplying `emit()` (with named `key`, `channel` and `metadata` arguments), `change()`, `now()`, `stableId()`, `checkpoint()` and the timer registrations. Overriding a hook SHALL be what declares it. A value SHALL leave a tasklet only through `$ctx->emit()`: `run` returns nothing, and the SDK SHALL NOT emit a value returned from any hook.

Per-cell ordering and exclusivity SHALL be the core's, not the SDK's: the SDK SHALL NOT implement mailboxes, drain gates, or ordering of its own.

#### Scenario: Different cells of one tasklet run concurrently
- **WHEN** the threaded executor is selected and inputs for two distinct cell keys arrive together
- **THEN** both activations MAY be in flight simultaneously on distinct threads, and the SDK SHALL add no serialization of its own

#### Scenario: One cell never runs concurrently with itself
- **WHEN** many inputs for a single cell key arrive under any executor
- **THEN** the SDK SHALL never have two activations for that cell in flight, because the core does not hand them out

#### Scenario: Per-cell inputs are delivered in arrival order
- **WHEN** inputs for one cell arrive in a known order
- **THEN** `run()` SHALL observe them in that order

#### Scenario: A throwing run climbs the retry ladder
- **WHEN** `run()` throws
- **THEN** the SDK SHALL complete the activation with a retryable verdict and the core SHALL re-hand the batch per the channel's retry policy, parking to the DLQ when attempts are exhausted

#### Scenario: A non-retryable error parks immediately
- **WHEN** `run()` throws the SDK's non-retryable tasklet exception
- **THEN** the activation SHALL park without further attempts

#### Scenario: A value leaves a tasklet only through the context
- **WHEN** a tasklet's `run` calls `$ctx->emit('a')` and `$ctx->emit('b', key: 'k')` for one input, and for the next input calls neither
- **THEN** the first input SHALL emit `a` and `b` keyed `k`, and the second SHALL emit nothing, under the dispatcher and under `Testing\TaskletTestHarness` alike

#### Scenario: The initial state comes from initState
- **WHEN** a tasklet overrides `initState(?string $cellKey)` and a Cell is created for key `A`
- **THEN** the Cell's state SHALL start as what `initState('A')` returns, and a single-cell tasklet's as what `initState(null)` returns

### Requirement: Deterministic effects through Ctx
The PHP SDK SHALL serve time from `Ctx::now()` and identifiers from `Ctx::stableId()`, evaluating the declared clock once per invocation and never reading ambient wall-clock or randomness on the authoring surface.

`stableId()` SHALL be byte-identical to the Rust and Java implementations for the same cell identity, change seq and per-invocation ordinal.

#### Scenario: now() is constant within one invocation
- **WHEN** `Ctx::now()` is called twice inside one `run()`
- **THEN** both calls SHALL return the same value

#### Scenario: stableId() matches the other SDKs
- **WHEN** `stableId()` is computed for a fixed cell identity, change seq and ordinal
- **THEN** it SHALL equal the value the Rust and Java SDKs produce for the same inputs, pinned by a shared fixture

### Requirement: Activation codec pinned to the shared fixtures
The PHP SDK SHALL encode and decode the postcard `Activation` and `ActivationResult` schema in PHP, pinned against the rows of `test/fixtures/postcard_bodies.txt` — the bytes the core encodes from its own structs, which the Java fixture suite and the controller also assert against.

A `u64` field SHALL decode into a PHP integer, and a value whose high bit is set SHALL raise a decode error rather than silently degrade to a float, because `PHP_INT_MAX` is 2^63−1 and a silently widened offset would corrupt comparison rather than fail.

#### Scenario: Every schema fixture round-trips
- **WHEN** each activation row of `test/fixtures/postcard_bodies.txt` is decoded, and the values of each result row are encoded, by the PHP codec
- **THEN** the decoded activation SHALL be the value the core encoded, and the encoded result SHALL be identical to the row

#### Scenario: A u64 above 2^63-1 is rejected
- **WHEN** a schema field carries a `u64` with the high bit set
- **THEN** the decoder SHALL raise an error naming the field, and SHALL NOT return a float

### Requirement: State and Change serialize as author-declared JSON
The PHP SDK SHALL serialize durable State and Change values as JSON produced by author-declared `toArray()` and reconstructed by `fromArray()`, and SHALL NOT use PHP `serialize()`, whose embedded class names and mangled private-property names make a class rename break restore.

Durable records SHALL be stamped `sdk_lang = SDK_LANG_PHP` with the PHP change-format id, declared through `HostClassDecl`'s existing `sdk_lang` / `change_format_id` fields.

#### Scenario: State survives a restart
- **WHEN** a cell accumulates State, the runner is stopped, and a new runner binds the same relay
- **THEN** the restored State SHALL equal the State at shutdown, folded from the change log through `fromArray()`

#### Scenario: A renamed class still restores
- **WHEN** a State class is renamed between two runs and its `fromArray()` is registered under the same tasklet
- **THEN** restore SHALL succeed, because the durable bytes carry no class name

#### Scenario: Records are stamped with the PHP language byte
- **WHEN** a PHP-hosted cell writes a change record
- **THEN** the record header SHALL carry the PHP SDK's allocated `sdk_lang` value and the PHP change-format id

#### Scenario: The advertisement carries the same byte
- **WHEN** the PHP runner connects and advertises its factories
- **THEN** every factory entry — in the connect init frame and in `UpdateFactories` — SHALL carry the language declared for the class registered under that id, so the controller places a PHP-authored factory only on a PHP runner

#### Scenario: A record of another language is refused, not decoded
- **WHEN** a PHP-hosted class is sent a `RestoreInitState` whose records carry another SDK's `sdk_lang`
- **THEN** the core's host restore fold SHALL refuse before any body reaches the PHP codec, and the controller SHALL leave the relay unbound rather than cold-starting it

### Requirement: Full host-class declaration
The PHP SDK SHALL build the complete `HostClassDecl` JSON accepted by `flz_core_declare_class`: the channels graph (entry, inter-tasklet, plugin, with per-channel retry, overflow and plugin keying), cell modes, clock kinds, the effectively-once tier and ack granularity, the correction window, header lang and format bytes, and steps lifecycle facts.

A declaration the core refuses SHALL surface the core's error naming the offending field, and the class SHALL NOT be registered.

#### Scenario: Declared facts reach AddRelayResponse
- **WHEN** a PHP class declares the effectively-once tier, a correction window and checkpoint ack-granularity, and the controller sends `AddRelay`
- **THEN** the response metadata SHALL carry the same declarations the typed Rust path produces for an equivalent class

#### Scenario: An unserviceable declaration is refused at declare time
- **WHEN** a PHP class declares a field combination the host layer cannot serve
- **THEN** `registerFactory()` SHALL raise an error naming the offending field, before the runner starts

### Requirement: Cell modes and the prepare crossing
The PHP SDK SHALL support single, static-keyed and dynamic cell modes, and SHALL answer the core's batched `KeyRequest` prepare crossing with routing keys and extracted event times for the payloads it is given.

The cell mode SHALL be read per tasklet node from what is declared, never from a flag: static when the tasklet's `#[Tasklet\Spec]` carries `cellKeys`, dynamic when a chain of the relay keys its values with `keyBy` before the `into` or merge that reaches the tasklet, and single otherwise. A tasklet carrying `cellKeys` and fed by `keyBy` SHALL be refused at registration with the core's `topology.key_by_on_unkeyable_target`.

Dynamic mode SHALL honour the declared idle TTL and cell cap through the core's existing eviction, without a PHP-side registry.

#### Scenario: A dynamic cell is created on first key
- **WHEN** an input arrives whose key function yields a key with no live cell
- **THEN** the core SHALL create the cell and the SDK SHALL run the tasklet against fresh State

#### Scenario: A failing key function climbs the entry retry ladder
- **WHEN** the key function throws for one payload in a prepare batch
- **THEN** that payload SHALL climb the entry channel's retry→park ladder, and the other payloads in the batch SHALL be unaffected

#### Scenario: keyBy alone makes a tasklet dynamic
- **WHEN** a relay declares `$c->input('orders')->keyBy(fn (array $o): string => $o['customer'])->into(Counter::class)` and `Counter`'s `#[Tasklet\Spec]` carries only its `name`
- **THEN** the declaration record SHALL carry `cell_mode` `dynamic` for `Counter`, and each customer SHALL get its own Cell

#### Scenario: One class takes its mode from each wiring
- **WHEN** one tasklet class is wired through `keyBy` in one relay and without it in another
- **THEN** the first relay's record SHALL carry `cell_mode` `dynamic` for it and the second's `single`

#### Scenario: A static key set fed by keyBy is refused
- **WHEN** a tasklet carrying `#[Tasklet\Spec(name: 'routed', cellKeys: ['a', 'b'])]` is reached by an edge declaring `keyBy`
- **THEN** registration SHALL throw `FlezhaException` carrying `topology.key_by_on_unkeyable_target` and naming the tasklet

### Requirement: Tiers, checkpoint intervals and DLQ park
The PHP SDK SHALL carry tier semantics on activation completion: changes, outputs, timer ops and `checkpoint` ride the `ItemEffect`, the core builds effect records and owns offsets, and a park mints a DLQ entry exactly where the typed layer does.

#### Scenario: An effectively-once class advances the watermark
- **WHEN** a PHP class declares the effectively-once tier and processes an input
- **THEN** the completion SHALL carry the consumed offset and the controller's durable watermark SHALL advance

#### Scenario: Checkpoint ack holds the interval open
- **WHEN** a checkpoint-ack class processes inputs without calling `Ctx::checkpoint()`
- **THEN** the changes SHALL stage without advancing the watermark, and the first `checkpoint()` SHALL close the interval and advance it

#### Scenario: An exhausted retry parks with the payload
- **WHEN** a tasklet fails past its channel's attempt budget
- **THEN** the SDK SHALL park the input to the DLQ store and the offset SHALL advance, so the replay starts after the poison input

### Requirement: Per-cell timers
The PHP SDK SHALL expose event-time and processing-time timer registration and cancellation on `Ctx`, returning the absolute scheduled timestamp as the cancellation handle, and SHALL handle the core's timer firing activations through an `onTimer()` tasklet hook.

#### Scenario: A processing-time timer fires
- **WHEN** a tasklet registers a processing-time timer and its deadline passes
- **THEN** `onTimer()` SHALL be invoked with the scheduled timestamp, and `Ctx::now()` inside it SHALL equal that timestamp

#### Scenario: Pending timers survive a restart
- **WHEN** a runner holding pending timers is restarted and the cell is restored
- **THEN** the restored cell SHALL still hold those timers, rebuilt from its change log

### Requirement: Edge transforms, keys and inter-tasklet routing
The PHP SDK SHALL support the producer-side edge surface — map, filter, flatMap and keyBy on stream handles, and merge with per-source mapping — with transforms composed at declaration time and evaluated where the core routes outputs. `$c->merge()` SHALL return a merge handle with `from($source, callable $map)`, `keyBy(callable $key)`, `into(string $class, ?string $nodeName = null)`, recording each source as a chain closed by the core's `merge` link under one group and taking the merge refusals from the reference check. `$c->routeLatest($documents, $traffic, callable $keyFn)` and `routeLatestNamed(..., string $nodeName)` SHALL expand to the `relay-graph` router, packaged in the SDK as a single-cell tasklet with journaled `{document, version, delivered}` state.

#### Scenario: A chain of transforms is applied once per value
- **WHEN** several transforms are declared on one edge before its terminal `into`
- **THEN** each input SHALL traverse the composed chain exactly once

#### Scenario: A throwing transform is contained
- **WHEN** an edge transform throws
- **THEN** the failure SHALL climb the channel's retry→park ladder rather than terminating the runner

#### Scenario: Two channels merge into one tasklet
- **WHEN** a relay declares `$c->merge()->from($ticks, fn ($t) => [...])->from($quotes, fn ($q) => [...])->keyBy(fn ($v) => $v['symbol'])->into(Detector::class)`
- **THEN** registration SHALL succeed with one Detector node fed by two consumer edges, each applying its own mapping, a value from either entry SHALL reach `run` as the mapped value keyed by `symbol`, a duplicated source SHALL refuse with `topology.duplicate_merge_source` and a single source with `topology.merge_too_few_sources`

#### Scenario: The router delivers on a key's own traffic
- **WHEN** `$c->routeLatest($config, $trades, fn ($t) => $t['symbol'])` is declared and the harness feeds a document, trades for `A` and `B`, the same document, then a trade for `A`
- **THEN** exactly one `['A', $document]` and one `['B', $document]` SHALL be emitted, and the repeated document SHALL be inert

### Requirement: Steps relays at parity
The PHP SDK SHALL support steps relays as structured chains, where each stage partition arrives as a step activation and completion answers the driver's barrier. `Flezha\Relay` SHALL carry no one-function `run`, and `Flezha\StepsCtx` SHALL NOT exist; a tasklet inside a steps relay SHALL end its instance with `$ctx->complete($result)`, carried in its step answer.

Compensation hooks, loop modes, suspension and rewind SHALL be the core driver's existing machinery, reached through step activations; the SDK SHALL NOT implement a driver.

#### Scenario: A structured chain completes in order
- **WHEN** a steps class declaring a multi-stage chain is spawned
- **THEN** each stage SHALL arrive as a step activation, and the instance SHALL report completion after the terminal stage

#### Scenario: An aborted chain compensates in reverse
- **WHEN** a stage fails after earlier stages committed
- **THEN** the core SHALL hand out `CompensateRun` activations for the committed stages in reverse order

#### Scenario: A tasklet ends its steps relay
- **WHEN** a tasklet of a `PerInput` steps relay calls `$ctx->complete(['turns' => 4])` while handling a message
- **THEN** the iteration SHALL commit and the instance SHALL complete with that result instead of waiting for the next message

#### Scenario: A resumed chain skips journaled stages
- **WHEN** a steps instance is resumed after a crash mid-chain
- **THEN** stages recorded as committed in the task-journal SHALL NOT re-run

### Requirement: Clean shutdown on an activation budget
The PHP SDK SHALL accept an optional maximum-activation count after which the runner shuts down cleanly and exits zero, so an external supervisor can recycle a long-lived process without data loss.

The budget SHALL default to unlimited, and a clean shutdown SHALL leave unpolled work journaled controller-side for re-forwarding on the next bind.

#### Scenario: The runner exits cleanly at its budget
- **WHEN** a runner started with an activation budget reaches it
- **THEN** it SHALL stop polling, shut the context down, and exit with status zero

#### Scenario: Recycling loses no work
- **WHEN** a runner exits at its budget while inputs remain unconsumed
- **THEN** a replacement runner binding the same relay SHALL receive those inputs

### Requirement: End-to-end parity is the acceptance gate
The PHP SDK SHALL be gated by a runner application under `test/apps/php_runner`, declaring its launch line and its fixtures in a `conformance.json` manifest exactly as the Rust, Java and Elixir apps do, and driven by `php_sdk`-tagged suites in `test/system` selected by `FLEZHA_E2E_RUNTIME=php`.

Coverage SHALL include timers, dynamic cells, compaction, DLQ park, effectively-once, live state read and steps crash-resume — the same scenarios the Java arm asserts — and the manifest SHALL defer nothing, because this SDK claims full parity.

#### Scenario: The e2e matrix does not branch per runtime
- **WHEN** a system suite asserts a capability on the Rust and Java runners
- **THEN** the PHP runner SHALL assert the same capability without a runtime-specific exemption

#### Scenario: A new runtime needs no harness change
- **WHEN** the PHP test app is added with its manifest
- **THEN** the e2e harness and the bench fleet SHALL render its launch line from that manifest alone, with no PHP-specific clause in either

#### Scenario: The same app serves both executors
- **WHEN** the suites are run against the PHP runtime under each executor
- **THEN** one test app and one manifest SHALL serve both, differing only by the environment variable of the run

### Requirement: The PHP builder submits its topology to the reference check

The PHP SDK's builder SHALL record the authoring calls it receives — channels, the
ordered links of each chain, merges, branches, class flags and tasklet facts — into the
declaration record and SHALL take its verdict from the reference check, so that every name
of the `TopologyIssue` vocabulary is refused with the reference sentence and no rule is
carried in the builder. The attribute-carried surface SHALL stay as it is; only the judgement moves.
The SDK SHALL prove the set by one conformance run over every row of
`test/fixtures/reference_topologies.txt` through the row interpreter in its test app.

#### Scenario: A malformed row refuses with the reference name

- **WHEN** the PHP interpreter authors a refusal row through the SDK's own builder
- **THEN** registration refuses with the row's name and the reference sentence

#### Scenario: A well-formed row derives to the fixture's bytes

- **WHEN** the PHP interpreter authors an `ok` row
- **THEN** the derived declaration equals the row's lines

#### Scenario: A merge row runs through the SDK's own merge handle

- **WHEN** the PHP interpreter reaches `merge_one_source` or `merge_same_twice`
- **THEN** it authors the row with `$c->merge()->from(...)…->into(...)` rather than throwing, registration refuses with `topology.merge_too_few_sources` or `topology.duplicate_merge_source`, and the manifest's `topology_surfaces` lists `merge`

### Requirement: The single-tasklet short form is four attribute arguments
The short form SHALL be declared with the `#[Relay]` attribute a full-form relay already carries, on a class extending `Flezha\Relay`, and SHALL NOT be declared with `#[Tasklet\Spec]`, which SHALL accept none of `run`, `durable`, `output`, `input` or `codec` and SHALL throw naming the argument and `#[Relay]` when one is given. `#[Relay]` SHALL accept `output`, `input` and `codec` (a `ChannelCodec`) beside the `factoryId` and the class settings it already takes; how the class runs is its `run` argument.

A class SHALL be a short form exactly when it defines the tasklet body `run(mixed $input, Ctx $ctx): mixed` and does not override the topology body `channels`, which is the mechanism `Definition` already uses to read which hooks a class overrides; defining `run` and overriding `channels` SHALL throw `FlezhaException` naming `topology.short_form_with_topology`, and doing neither SHALL throw naming the missing body. A short-form class MAY define the tasklet hooks a `Flezha\Tasklet` subclass defines (`state`, `applyChange`), which the generated tasklet takes.

`output` SHALL be optional. The class SHALL carry a generated relay whose factory id is `factoryId`, wiring an entry channel named `input` (or the `input` argument's value) into a generated tasklet named `factoryId` and, when an `output` is written, that tasklet to the `output` plugin channel; with no `output`, it SHALL wire the entry channel into the tasklet and declare no plugin channel. A short form SHALL take the class settings on `#[Relay]` as the full form does; a steps short form SHALL be a `once` steps relay unless its `run` writes another `each`, fed by its first message on the entry channel and taking `init_values` as the tasklet's initial state. The entry SHALL be decoded by `codec` where one is written, whether or not an `output` is. An absent `factoryId` SHALL throw naming the required argument, and the SDK SHALL NOT derive one from the class.

`Runner::registerFactory` SHALL accept such a class string and register the generated relay, throwing `FlezhaException` at registration whose code is the refusal's code. A duplicate factory id SHALL throw `FlezhaException` with the code `topology.duplicate_factory_id`. The PHP SDK owes no `topology.output_without_type`: `Flezha\Tasklet::run` is declared `: mixed` and PHP refuses an override narrowing that to `: void`, so a tasklet declaring no output cannot be written; the manifest SHALL NOT list `shorthand_typed_output`.

#### Scenario: An attributed class is a whole flow relay

- **WHEN** a class extending `Flezha\Relay` is attributed `#[Relay(factoryId: 'upcase', output: 'out', codec: ChannelCodec::Text)]`, defines `run`, and the runner is given `registerFactory(Upcase::class)`
- **THEN** the runner declares a flow relay `upcase` with entry channel `input` decoding text, a tasklet named `upcase` and output `out`, and no `Flezha\Tasklet` subclass exists for it

#### Scenario: An attributed class is a whole steps relay

- **WHEN** a class extending `Flezha\Relay` is attributed `#[Relay(factoryId: 'place', output: 'placed', input: 'order', codec: ChannelCodec::Text)]` and defines `run`, the runner is given `registerFactory(Place::class)`, an instance is started, and one message is sent on `order`
- **THEN** `run` receives the message as text, the value it passes to `$ctx->emit` reaches `placed`, and the instance completes

#### Scenario: A short-form steps relay declares its loop mode

- **WHEN** a class extending `Flezha\Relay` is attributed `#[Relay(factoryId: 'chat', output: 'replies', codec: ChannelCodec::Text)]` and `#[Steps(each: Each::PerInput)]`, defines `run`, is registered, and two messages reach its entry channel
- **THEN** the declaration carries `each` `per_input`, `run` runs once per message, and the instance does not complete after the first

#### Scenario: A short form with no output is declared

- **WHEN** a class extending `Flezha\Relay` is attributed `#[Relay(factoryId: 'counter', input: 'ticks', codec: ChannelCodec::Text)]` and defines `run`, `state` and `applyChange`
- **THEN** the relay declares `ticks` into the generated tasklet and no plugin channel

#### Scenario: An input and codec need no output

- **WHEN** a class extending `Flezha\Relay` is attributed `#[Relay(factoryId: 'counter', input: 'ticks', codec: ChannelCodec::Text)]` with no `output`
- **THEN** `registerFactory` accepts it and the entry channel takes that name and codec

#### Scenario: A run body beside a channels override is refused

- **WHEN** an attributed relay class defines `run` and overrides `channels`
- **THEN** `registerFactory` throws `FlezhaException` whose code is `topology.short_form_with_topology`

#### Scenario: A tasklet cannot declare no output

- **WHEN** a class extending `Flezha\Tasklet` declares `public function run(mixed $input, Ctx $ctx): void`
- **THEN** it SHALL fail to load, because `Tasklet::run` is declared `: mixed`, so no PHP tasklet can declare no output and the SDK owes no `topology.output_without_type`

#### Scenario: An output on a tasklet attribute is refused

- **WHEN** a class is attributed `#[Tasklet\Spec(name: 'upcase', output: 'out')]`
- **THEN** `registerFactory` throws naming `output` and `#[Relay]`

#### Scenario: A derived factory id is refused

- **WHEN** a class extending `Flezha\Relay` defines `run` and carries no `#[Relay]` attribute
- **THEN** `registerFactory` throws naming the required `#[Relay(factoryId: ...)]`

### Requirement: A PHP class is judged on the settings tree it wrote

`#[Relay]` SHALL take the class settings as the tree's own data (`run`, `durable`, `maxDurationMs`, `description`), and `HostClassDecl::of` SHALL put the tree on the wire as written, so a setting under a parent that does not carry it reaches the core and is refused as `topology.unknown_field`, naming the setting and its parent, when the runner declares the class to the shared core (at start, or at the registration itself for a class registered after start), as every other core refusal in this SDK is raised, as a `FlezhaException` naming it. `#[Relay]` SHALL carry no `retryAttempts`, `retryBackoffMs`, `barrierRetryAttempts` or `wakeAfterMs`, and the declaration SHALL carry no `wake_after_ms`. A `Ctx` inside a steps relay SHALL offer `timerAfter`, `timerAtEventTime` and `cancelTimer` exactly as inside a flow relay, and a tasklet's `onTimer` SHALL run on its firing in both runs; only a `compensate` hook's `Ctx` SHALL throw an error naming `runtime.timers_unavailable` from them.

#### Scenario: A misplaced setting reaches the core

- **WHEN** a class declares `#[Relay(factoryId: 'w', run: ['flow' => ['each' => 'per_input']])]`, and the runner declares it
- **THEN** declaring it to the core throws `FlezhaException` whose code is `topology.unknown_field`, naming `each` and `flow`, and the relay is not declared

#### Scenario: The retired wake argument is refused

- **WHEN** a class carries `#[Relay(factoryId: 'w', wakeAfterMs: 5000)]`
- **THEN** PHP refuses the unknown named argument when the attribute is instantiated, and no declaration carries `wake_after_ms`

#### Scenario: A timer inside a steps tasklet fires

- **WHEN** a tasklet of a running `per_input` steps relay calls `$ctx->timerAfter(300)` in `run`
- **THEN** the call returns the scheduled timestamp, the effect carries the timer op, and `onTimer` runs on that Cell at that timestamp, opening a pass at that tasklet

### Requirement: A PHP tasklet names itself and declares its settings on one attribute
A PHP tasklet SHALL be a class extending `Flezha\Tasklet` and carrying `#[Tasklet\Spec(...)]`, and its `#[Tasklet\Spec]` SHALL carry a non-empty `name`. Registration SHALL refuse a tasklet class whose declaration has no `name`, or an empty one, with a `FlezhaException` naming the class, and SHALL NOT derive a name from the class. `name` SHALL NOT be inherited from a parent class, so every concrete subclass writes its own. The short form SHALL keep naming its tasklet after its factory id and SHALL keep refusing a `name`.

The idle TTL SHALL be `idleTtlSeconds`, a whole number of seconds, and the declaration record SHALL carry it as `idle_ttl_ms`, that number times 1000. Unset SHALL mean no eviction.

The clock SHALL follow from what the class writes: event time when it overrides `eventTime()`, logical when its `#[Tasklet\Spec]` carries `logicalClock: true`, and the wall clock otherwise. A class that does both SHALL be refused at registration with a `FlezhaException` naming `eventTime` and `logicalClock`. A logical clock on a tasklet that does not override `initState` SHALL keep the core's `topology.unservable_clock` refusal. The SDK SHALL NOT have a `Flezha\Clock` enum or a `clock` argument.

A change SHALL be any value the codec carries, recorded with `$ctx->change($value)` and folded by the tasklet's `applyChange(mixed &$state, mixed $change)`, and no change class SHALL be required. The optional `stateType` and `changeType` Codable classes SHALL stay. `migrateState(int $fromVersion, string $bytes)` and `migrateChange(int $fromVersion, string $bytes)` SHALL keep receiving bytes.

`#[Tasklet\Spec]` SHALL NOT accept `dynamicCells`, `clock` or `idleTtlMs`.

#### Scenario: A tasklet without a name is refused

- **WHEN** a class extending `Flezha\Tasklet` carries `#[Tasklet\Spec(cellKeys: ['a'])]`, or no `#[Tasklet\Spec]` at all, and a relay wiring it is registered
- **THEN** registration throws `FlezhaException` naming the class and the missing `name`, and nothing is registered

#### Scenario: A renamed class keeps its state

- **WHEN** a tasklet class carrying `#[Tasklet\Spec(name: 'totals')]` is renamed between two runs, keeping the attribute
- **THEN** its Cells restore from the state saved under `totals`

#### Scenario: A subclass writes its own name

- **WHEN** a class extends a named tasklet class and carries `#[Tasklet\Spec(mailboxCapacity: 8)]` with no `name`
- **THEN** registration refuses it naming the subclass, and the parent's other declarations are still merged into a subclass that does write a `name`

#### Scenario: The idle TTL is whole seconds

- **WHEN** a tasklet fed by `keyBy` carries `#[Tasklet\Spec(name: 'sessions', idleTtlSeconds: 3_600)]`
- **THEN** the declaration record carries `idle_ttl_ms` `3600000` for that tasklet

#### Scenario: Overriding eventTime makes the clock event time

- **WHEN** a tasklet overrides `eventTime()` and its `#[Tasklet\Spec]` says nothing about the clock
- **THEN** the declaration carries clock `event_time`, and `$ctx->now()` inside `run` returns what `eventTime()` extracted from that input

#### Scenario: logicalClock declares the logical clock

- **WHEN** a tasklet overriding `initState` and `applyChange` carries `#[Tasklet\Spec(name: 'ledger', logicalClock: true)]`
- **THEN** the declaration carries clock `logical`, and `$ctx->now()` serves the Cell's change sequence

#### Scenario: Event time and the logical clock together are refused

- **WHEN** a tasklet overrides `eventTime()` and carries `logicalClock: true`
- **THEN** registration throws `FlezhaException` naming `eventTime` and `logicalClock`

#### Scenario: A logical clock on a stateless tasklet is refused

- **WHEN** a tasklet that does not override `initState` carries `logicalClock: true`
- **THEN** registration throws `FlezhaException` carrying the core's `topology.unservable_clock`

#### Scenario: A change needs no class

- **WHEN** a tasklet calls `$ctx->change(['kind' => 'event', 'deadline' => $at])` and its `applyChange` folds that array into an array state
- **THEN** the change is journaled as JSON, folded live, and folded again to an equal state on restore, with no class defined for it

#### Scenario: A retired tasklet argument is refused

- **WHEN** a class carries `#[Tasklet\Spec(name: 'x', dynamicCells: true)]`, `#[Tasklet\Spec(name: 'x', clock: ...)]` or `#[Tasklet\Spec(name: 'x', idleTtlMs: 1000)]`
- **THEN** PHP refuses the unknown named argument when the SDK reads the attribute, naming it, and the class is not registered

### Requirement: The four command activation kinds
The PHP SDK SHALL handle every command activation kind the core hands out: `RestoreFold`, `SnapshotRequest`, `LiveStateRead` and `ApplyState`, and the `Leave` that tells it a Cell or a relay left.

#### Scenario: A restore fold rebuilds state before inputs flow
- **WHEN** a cell with a persisted change log is bound and the core hands out a `RestoreFold`
- **THEN** the SDK SHALL fold the change bodies into State and complete the activation, and the cell's next input activation SHALL observe the folded state

#### Scenario: A snapshot request round-trips through restore
- **WHEN** the controller triggers `CompactCell` and the core hands out a `SnapshotRequest`
- **THEN** the SDK SHALL answer with a serialized State that a later `RestoreFold` decodes to an equal State

#### Scenario: A live state read reports the current value
- **WHEN** `flz state get` targets a live PHP-hosted cell
- **THEN** the SDK SHALL answer a `LiveStateRead` with the cell's current State as JSON, stamped with the change seq it was read at

#### Scenario: A migration serializes and applies
- **WHEN** a relay hot-swap migrates a PHP-hosted cell
- **THEN** `LiveStateRead` on the source and `ApplyState` on the destination SHALL round-trip the State

#### Scenario: A leave drops the held State
- **WHEN** the core hands out a `Leave` for a cell, or with `whole_relay` for a relay
- **THEN** the SDK SHALL drop the State it holds for that cell, or for every cell of that relay, and a cell started again under the same name SHALL begin at `initState`

### Requirement: The PHP runner and timer record SHALL use the shared names

`Flezha\Runner::close()` SHALL be the one public shutdown verb (`stop()` does not exist), the
channel builder SHALL be `Flezha\ChannelBuilder` and its stream handle `Flezha\Stream` (no
`Flezha\Graph\Builder` or `Flezha\Graph\Edge`), the timer hook's record SHALL be
`Flezha\TimerFiring` with `kind` and `scheduledAt`, and the runner's host plumbing (`serve`, `core`,
`coreAddress`, `autoloadPath`, `entryScript`, `abandonContext`, `attachInThread`), `Dropped`, `Fanned`
and `Ctx::effectRecord` SHALL be marked `@internal`. The harness SHALL offer one replay gate,
`replayEquals()`, raising on a mismatch, with no static twin.

#### Scenario: The renamed builder declares a relay

- **WHEN** a relay's `channels(ChannelBuilder $c)` wires an input into a tasklet carrying `#[Tasklet\Spec(name: 'x')]`
- **THEN** the relay SHALL register and run exactly as it did under the former names

### Requirement: The Executor seam owns loop multiplicity, not work distribution
The PHP SDK SHALL define an `Executor` interface of exactly two methods — `run(Runner $runner): void`, which drives the runner's poll loop to completion and MAY drive it on more than one thread, and `stop(): void`, which asks every loop to finish its current activation and return.

The seam SHALL NOT be expressed as per-activation submission of a callable, because `Swoole\Thread` bootstraps a fresh interpreter from a script path and scalar arguments and cannot carry a closure, an object or a resource across a thread boundary. Loop multiplicity is expressible under every PHP threading model; work distribution is not.

The loop SHALL take its work from a blocking `flz_core_poll(ctx, timeout_ms)` call and complete through `flz_core_complete`, which the ABI accepts from any thread including one that never polled — so no executor requires a notification descriptor or any other ABI addition.

#### Scenario: The poll loop is executor-agnostic
- **WHEN** the SDK is run under each shipped executor against an identical workload
- **THEN** the same decode, dispatch and completion code paths SHALL execute, differing only in how many threads run them

#### Scenario: An activation completes on the thread that polled it
- **WHEN** any executor runs the loop
- **THEN** each thread SHALL poll, dispatch and complete within itself, so no activation crosses a thread boundary

#### Scenario: Stop does not race in-flight activations
- **WHEN** the runner stops, whether by signal or by reaching its activation budget
- **THEN** `stop()` SHALL return only once every loop has left user code, and `flz_core_shutdown` SHALL NOT be called before it does

### Requirement: Concurrency cannot affect per-cell correctness
An executor SHALL NOT be able to change ordering, exclusivity, or state consistency, because those are held by the core: `PollCore` never hands out two activations for the same cell concurrently, regardless of how many threads poll it.

The SDK SHALL NOT add locking, queueing, or ordering of its own on top of that guarantee.

#### Scenario: Concurrent activations never share a cell
- **WHEN** many threads poll under sustained load
- **THEN** no two in-flight activations SHALL name the same cell

#### Scenario: Switching executors does not change results
- **WHEN** the same deterministic workload is replayed under `SyncExecutor` and under `SwooleExecutor`
- **THEN** the resulting cell states and emitted outputs SHALL be equal

### Requirement: SyncExecutor is the zero-dependency default
The SDK SHALL ship a `SyncExecutor` as the default, requiring no Composer dependency and no PHP extension beyond `ext-ffi`: `run()` drives the loop on the calling thread and `stop()` sets the flag the loop checks between polls.

It SHALL process one activation at a time, which is correct for tasklet bodies that are CPU-bound or use blocking I/O, and it SHALL work on a non-thread-safe (NTS) PHP build, which is what distributions ship by default.

#### Scenario: A runner starts with no dependencies installed
- **WHEN** a project installs only `flezha/sdk` on an NTS PHP build and starts a runner without selecting an executor
- **THEN** the runner SHALL start and process activations under `SyncExecutor`

#### Scenario: A blocking tasklet body is correct, not exceptional
- **WHEN** a tasklet body performs blocking I/O under `SyncExecutor`
- **THEN** the activation SHALL complete normally and the runner SHALL process the next one afterwards, with no warning and no degraded mode

### Requirement: SwooleExecutor gives one runner every core
The SDK SHALL ship a `SwooleExecutor` that runs the poll loop on a pool of `Swoole\Thread`s over one shared `FlzContext`, so a single runner — and therefore a single relay's tasklet class — uses more than one core.

Each thread SHALL bootstrap the runner's entry script, rebuild the PHP-side tasklet registry, and attach to the existing context by its address, without re-declaring classes to the core: `flz_core_declare_class` is per context, not per thread. The entry script is therefore re-entered, and the SDK SHALL document that it must be safe to `require` — in particular that it carries no `#!` line, which is stripped only from the script PHP was invoked with.

The pool SHALL grow on the core's own signal rather than a guess: a supervisor SHALL block in the core pool governor's supervisor entry (`flz_core_pool_supervise`), which waits on the core's starvation signal, and add a thread when it answers so, the governor holding the configured maximum, which is the case the ABI documents as "hosts whose fetch threads can be occupied by user code". A thread the governor started above the base SHALL leave after the governor's idle time, 30 s without work.

`SwooleExecutor` SHALL be selected explicitly and SHALL NOT be the default, because it requires a ZTS PHP build and an extension that most distributions do not ship.

#### Scenario: Threads process activations in parallel
- **WHEN** a CPU-bound workload runs under `SwooleExecutor` with a thread maximum above one
- **THEN** total throughput SHALL exceed the same workload's throughput under `SyncExecutor` on the same host

#### Scenario: The pool grows when user code occupies the fetch threads
- **WHEN** every running thread is inside a tasklet body while further work is queued
- **THEN** the supervisor SHALL observe starvation and start another thread, up to the configured maximum

#### Scenario: One relay uses more than one core
- **WHEN** a single dynamic-cell relay receives inputs across many keys under `SwooleExecutor`
- **THEN** activations for distinct cells SHALL execute simultaneously on distinct threads

#### Scenario: A missing runtime produces an instruction
- **WHEN** `SwooleExecutor` is selected on a non-ZTS build, without the extension that provides `Swoole\Thread`, or on a build whose `ffi.enable` is not `true`
- **THEN** the SDK SHALL raise an error naming what is missing and how to obtain it, before the runner connects

#### Scenario: FFI must be enabled outright, not only for preload
- **WHEN** the threaded executor is selected on a build whose `ffi.enable` is PHP's shipped default `preload`
- **THEN** it SHALL be refused with an instruction, because that setting permits FFI from the CLI SAPI and a worker thread is a fresh interpreter that is not it — every thread would otherwise start and be refused its first core call

#### Scenario: A worker that ends before shutdown is replaced
- **WHEN** a worker thread ends for any reason other than shutdown
- **THEN** the pool SHALL report it and SHALL NOT count it against the maximum, so a pool that lost every worker starts another rather than reporting starvation forever

#### Scenario: Threads exit cleanly at shutdown
- **WHEN** the runner stops while several threads hold in-flight activations
- **THEN** every thread SHALL finish its activation, leave the loop and be joined, and the context SHALL be freed exactly once, by the thread that created it

### Requirement: The FFI binding resolves per thread
The SDK SHALL resolve its FFI handle through a per-thread accessor rather than a process-wide singleton, because `FFI::cdef` is per-interpreter and each `Swoole\Thread` is a separate interpreter.

The `FlzContext` pointer SHALL be carried as an integer, so it crosses a thread boundary as a bootstrap argument with no lifetime of its own. A shim that casts the integer back to a pointer type through its language's FFI SHALL keep whatever that cast borrows alive for as long as the pointer is used; carrying the integer to the ABI directly is the shape with nothing to keep alive.

#### Scenario: Each interpreter gets its own handle
- **WHEN** the binding is accessed from a thread that has not used it before
- **THEN** it SHALL create that thread's handle lazily and reach the same context

#### Scenario: An attached handle answers a call that dereferences the context
- **WHEN** a handle attached by address is used for a call the core answers from the context itself, rather than one that ignores it
- **THEN** the core's own answer SHALL come back — a handle that passes a context-free call and faults on the first real one is the failure this pins

#### Scenario: The context outlives every worker thread
- **WHEN** worker threads attach to a context they did not create
- **THEN** they SHALL NOT free it, and the creating thread SHALL free it only after every worker has been joined

### Requirement: Both executors are gated by the same suites
Every suite that gates the PHP SDK — unit, e2e and benchmark — SHALL be runnable under either executor without editing a test, a fixture or a manifest, selecting it by one environment variable of the run, defaulting to `SyncExecutor`.

The selection SHALL be an axis of the run, never a property of a file, so no test, fixture or benchmark exists in a per-executor variant.

#### Scenario: The e2e suite runs under either executor
- **WHEN** the `php_sdk`-tagged suites are run with the executor variable unset, and again set to the threaded executor
- **THEN** both runs SHALL execute the same tests and both SHALL pass

#### Scenario: The benchmark runs under either executor
- **WHEN** the round-trip benchmark is run against the PHP runtime under each executor
- **THEN** each run SHALL report which executor produced it, so the two numbers are comparable and attributable

#### Scenario: No fixture names an executor
- **WHEN** the PHP test app and the system suites are inspected
- **THEN** no fixture, test name or manifest entry SHALL be specific to one executor

### Requirement: Swoole is optional and never named in shared code
The SDK's Composer manifest SHALL require only `php >= 8.4` and `ext-ffi`, declaring `ext-swoole` under `suggest` with the minimum version that provides `Swoole\Thread`.

No Swoole type SHALL be named outside the threaded adapter's own files and its own tests — not in the `Executor` interface, not as a parameter or return type, and not in a `use` statement in shared code — so that PHP's per-class autoloading leaves the adapter entirely inert when it is not selected. The adapter is more than one file because a `Swoole\Thread` is constructed from a **script path**: its worker bootstrap is necessarily a script, not a class. The check SHALL read the parsed source rather than the raw text, so a comment explaining the seam is not an offence.

Shared code SHALL likewise not name the adapter class itself: the factory that constructs it is the one door, so nothing else autoloads the adapter.

#### Scenario: An unselected adapter is never loaded
- **WHEN** a runner runs under `SyncExecutor` on an NTS build with no Swoole extension present
- **THEN** no error SHALL occur, because the threaded adapter's file is never autoloaded

#### Scenario: Shared code names no runtime
- **WHEN** the SDK's sources outside the adapter file are inspected
- **THEN** no Swoole symbol SHALL appear in any of them

### Requirement: The SDK ships as one Composer package with a declared PHP floor
The PHP SDK SHALL be distributed as a single Composer package named `flezha/sdk`, requiring `php >= 8.4`, `ext-ffi`, `ext-mbstring` and `composer-plugin-api`, and nothing else.

The 8.4 floor is the supported PHP line, not the lowest the code could parse: the authoring surface needs only 8.1 — enums and `never` — and `ext-ffi` itself is stable from 8.0, but 8.4 is the line this project builds, tests and benchmarks the SDK on. The floor SHALL be declared in `require` rather than pinned through `config.platform`, so the manifest states what is supported instead of freezing what resolves. `composer-plugin-api` is the one requirement that is not a runtime need: it is what makes the package a Composer plugin, which is how it obtains its native binary, and like `php` and the extensions it is a platform package that pulls nothing. `ext-swoole` SHALL be the only `suggest` entry, declared at the minimum version providing `Swoole\Thread`, and the threaded executor MAY declare a higher PHP floor than the package as a whole, checked when it is selected rather than at install.

#### Scenario: A minimal install works
- **WHEN** a project runs `composer require flezha/sdk` on PHP 8.4 with `ext-ffi` enabled
- **THEN** installation SHALL pull no further packages, and a runner SHALL start

#### Scenario: The threaded executor checks its own requirements
- **WHEN** `SwooleExecutor` is selected on a build that does not satisfy its PHP or extension floor
- **THEN** the SDK SHALL raise an error naming what is missing, and installation of the package itself SHALL never have been affected by it

#### Scenario: An unsupported PHP version is refused at install
- **WHEN** installation is attempted on PHP below 8.4
- **THEN** Composer SHALL refuse it on the declared platform constraint

### Requirement: One runner per process; the SDK spawns none
The PHP SDK SHALL start exactly one runner per process and SHALL NOT fork, spawn, supervise, or recycle processes.

Process multiplicity SHALL be the orchestrator's concern — Kubernetes replicas, systemd template units, or a supervisor's own process management — so the SDK stays agnostic to which one is in use.

#### Scenario: The entry script starts one runner
- **WHEN** the runner entry point is invoked with a namespace
- **THEN** exactly one process, one `FlzContext` and one controller connection SHALL result

#### Scenario: More processes means more runners
- **WHEN** an orchestrator starts several runner processes for one namespace on one host
- **THEN** each SHALL register as its own runner, and the controller SHALL spread relays across them by its existing placement

#### Scenario: The SDK exposes no worker-count knob
- **WHEN** the SDK's command-line surface is inspected
- **THEN** it SHALL offer no flag that spawns additional processes

### Requirement: Schema version is negotiated, never assumed
The package SHALL carry the `SCHEMA_VERSION` it was built against and pass it to `flz_core_context_new`, so a shim paired with a mismatched core is refused before any controller connection.

A version bump in `crates/flz_core/src/schema.rs` SHALL therefore require the PHP fixtures and constant to move in the same commit, exactly as the Rust and Java fixture suites do.

#### Scenario: A mismatched pair refuses to start
- **WHEN** the package's schema version differs from the loaded core's
- **THEN** context creation SHALL fail with an error naming both versions

#### Scenario: CI catches an unsynchronised bump
- **WHEN** the core's schema changes without the PHP fixtures being updated
- **THEN** the PHP fixture suite SHALL fail

### Requirement: The package obtains the host's native binary at install
The package SHALL make a `libflz_core_ffi` binary available for the host it is installed on, for the same five target triples the Java distribution ships, selected at runtime by detected platform. It SHALL obtain exactly the host's binary rather than carrying every triple, from the release matching the package's own version, and `scripts/build-dist.sh` SHALL produce the artefacts it obtains. A binary obtained at install time SHALL be verified against the checksum manifest published beside it and SHALL be refused on a mismatch. The package SHALL NOT depend on build output being present in version control.

This obtain-one shape is the split that bundling every triple is recorded as owing; it arrives first here because a Composer package is published from a version-control tag and so cannot carry untracked build output at all.

#### Scenario: The right binary is selected at start
- **WHEN** a runner starts on a supported platform
- **THEN** the SDK SHALL load the matching binary without user configuration

#### Scenario: An unsupported platform fails clearly
- **WHEN** a runner starts on a platform with no available binary
- **THEN** it SHALL raise an error naming the detected platform, the location searched and the override environment variable, rather than a dynamic-loader failure

#### Scenario: An external binary can override the bundle
- **WHEN** the native-library directory environment variable names a directory containing a compatible binary
- **THEN** the SDK SHALL load that binary in preference to any other, on any platform

#### Scenario: A populated checkout touches no network
- **WHEN** the package's platform directory already holds a compatible binary
- **THEN** it SHALL be used and no download SHALL be attempted

#### Scenario: An obtained binary is verified before use
- **WHEN** a binary is obtained at install time
- **THEN** its checksum SHALL be compared against the published manifest for the package's own version, and a mismatch SHALL refuse the install, leaving no binary behind

#### Scenario: A package published from a tag still installs
- **WHEN** the package is installed from a registry that serves it from a version-control tag, carrying no build output
- **THEN** the installation SHALL still yield a loadable binary for the host, or SHALL fail naming what to do by hand

#### Scenario: Disabled install scripts do not break the install
- **WHEN** Composer installs the package with scripts disabled
- **THEN** the binary SHALL still be obtained

#### Scenario: An install that skipped the fetch names how to finish it
- **WHEN** the package was installed without its binary being obtained, and a runner starts
- **THEN** the error SHALL name the asset and manifest to download, the directory to place the binary in, the override environment variable, and the Composer command that obtains it

#### Scenario: A platform with no release and a failed fetch refuse differently
- **WHEN** installation meets a platform no release publishes a binary for, or a fetch of a published binary that fails
- **THEN** the first SHALL direct the user to build the binary for that platform and the second to retry or fetch by hand, in messages that do not share wording

### Requirement: The package is developed and tested from its own tree

The PHP SDK SHALL be developed in a repository whose root is the Composer
package, with its test app beside it, and its suite SHALL run there without a
core tree: every test that reads a shared artefact SHALL read the vendored
fixture set, and no test SHALL skip for the absence of this repository.

#### Scenario: The suite runs from the PHP tree alone
- **WHEN** the PHP suite runs in the PHP repository with `contract/` present and no core tree
- **THEN** every test SHALL run and none SHALL be skipped for a missing repository file

#### Scenario: Packagist serves the package root
- **WHEN** the repository is registered on Packagist
- **THEN** `composer.json` SHALL be at the repository root and the package SHALL install from a tag
