# php-sdk Specification

## Purpose
TBD - created by archiving change add-php-sdk. Update Purpose after archive.
## Requirements
### Requirement: PHP SDK entry point and runner lifecycle
The PHP SDK SHALL expose a runner handle created by `Flezha::createRunner(string $namespace)`, carrying declaration and lifecycle on one object, with `registerFactory()` registering an operative class of either kind (the kind is read from the class), `unregisterFactory()` as its inverse, `executor()` selecting the concurrency adapter, `start()` connecting, `awaitTermination()` blocking, and `run()` as `start()` + `awaitTermination()`.

The runner SHALL map onto the ABI lifecycle `flz_core_context_new(SCHEMA_VERSION)` → `flz_core_declare_class` per class → `flz_core_start(namespace)` → poll/complete → `flz_core_shutdown` → `flz_core_context_free`. Naming SHALL follow `docs/sdk/naming_conventions.md`, matching the Rust and Java surfaces.

#### Scenario: A declared stream operative connects and is placed
- **WHEN** a script calls `Flezha::createRunner('trading')->registerFactory(OrderTotals::class)->run()` against a live controller
- **THEN** the runner SHALL connect, `AddOperative` SHALL place an instance of that class, and inputs routed to it SHALL reach the declared tasklet

#### Scenario: Zero declared operative types is legal
- **WHEN** a runner is started having declared no operative class
- **THEN** it SHALL connect successfully and remain connected, matching the Rust and Java behaviour for an empty factory list

#### Scenario: Schema mismatch is refused before connecting
- **WHEN** the PHP shim's compiled `SCHEMA_VERSION` differs from the loaded `libflz_core_ffi`
- **THEN** `createRunner` SHALL raise an error naming both versions, and no controller connection SHALL be attempted

#### Scenario: Missing ext-ffi fails with an actionable message
- **WHEN** the SDK starts on a PHP build where the `FFI` class is unavailable or `ffi.enable` forbids the call
- **THEN** it SHALL raise an error naming the required extension and ini setting, rather than a bare "Class FFI not found" fatal

### Requirement: Tasklet authoring surface and per-cell dispatch
The PHP SDK SHALL define a `Tasklet` interface whose implementations declare their initial `State`, a `run()` invoked per input, and an `applyChange()` that folds a Change into State, with a `Ctx` supplying `emit()`, `emitKeyed()`, `now()`, `stableId()`, `checkpoint()` and the timer registrations.

Per-cell ordering and exclusivity SHALL be the core's, not the SDK's — the SDK SHALL NOT implement mailboxes, drain gates, or ordering of its own.

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
The PHP SDK SHALL encode and decode the postcard `Activation` and `ActivationResult` schema in PHP, pinned against the hex fixtures in `flz_core/src/schema.rs` — the same artefact the Rust and Java fixture suites assert against.

A `u64` field SHALL decode into a PHP integer, and a value whose high bit is set SHALL raise a decode error rather than silently degrade to a float, because `PHP_INT_MAX` is 2^63−1 and a silently widened offset would corrupt comparison rather than fail.

#### Scenario: Every schema fixture round-trips
- **WHEN** each hex fixture in `flz_core/src/schema.rs` is decoded and re-encoded by the PHP codec
- **THEN** the re-encoded bytes SHALL be identical to the fixture

#### Scenario: A u64 above 2^63-1 is rejected
- **WHEN** a schema field carries a `u64` with the high bit set
- **THEN** the decoder SHALL raise an error naming the field, and SHALL NOT return a float

### Requirement: State and Change serialize as author-declared JSON
The PHP SDK SHALL serialize durable State and Change values as JSON produced by author-declared `toArray()` and reconstructed by `fromArray()`, and SHALL NOT use PHP `serialize()`, whose embedded class names and mangled private-property names make a class rename break restore.

Durable records SHALL be stamped `sdk_lang = SDK_LANG_PHP` with the PHP change-format id, declared through `HostClassDecl`'s existing `sdk_lang` / `change_format_id` fields.

#### Scenario: State survives a restart
- **WHEN** a cell accumulates State, the runner is stopped, and a new runner binds the same operative
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
- **THEN** the core's host restore fold SHALL refuse before any body reaches the PHP codec, and the controller SHALL leave the operative unbound rather than cold-starting it

### Requirement: Full host-class declaration
The PHP SDK SHALL build the complete `HostClassDecl` JSON accepted by `flz_core_declare_class`: the channels graph (entry, inter-tasklet, plugin, with per-channel retry, overflow and plugin keying), cell modes, clock kinds, the effectively-once tier and ack granularity, the correction window, header lang and format bytes, and workflow lifecycle facts.

A declaration the core refuses SHALL surface the core's error naming the offending field, and the class SHALL NOT be registered.

#### Scenario: Declared facts reach AddOperativeResponse
- **WHEN** a PHP class declares the effectively-once tier, a correction window and checkpoint ack-granularity, and the controller sends `AddOperative`
- **THEN** the response metadata SHALL carry the same declarations the typed Rust path produces for an equivalent class

#### Scenario: An unserviceable declaration is refused at declare time
- **WHEN** a PHP class declares a field combination the host layer cannot serve
- **THEN** `registerFactory()` SHALL raise an error naming the offending field, before the runner starts

### Requirement: The five command activation kinds
The PHP SDK SHALL handle every command activation kind the core hands out: `RestoreFold`, `SnapshotRequest`, `LiveStateRead`, `ApplyState` and `MigrationSerialize`.

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
- **WHEN** an operative hot-swap migrates a PHP-hosted cell
- **THEN** `MigrationSerialize` on the source and `ApplyState` on the destination SHALL round-trip the State

### Requirement: Cell modes and the prepare crossing
The PHP SDK SHALL support single, static-keyed and dynamic cell modes, and SHALL answer the core's batched `KeyRequest` prepare crossing with routing keys and extracted event times for the payloads it is given.

Dynamic mode SHALL honour the declared idle TTL and cell cap through the core's existing eviction, without a PHP-side registry.

#### Scenario: A dynamic cell is created on first key
- **WHEN** an input arrives whose key function yields a key with no live cell
- **THEN** the core SHALL create the cell and the SDK SHALL run the tasklet against fresh State

#### Scenario: A failing key function climbs the entry retry ladder
- **WHEN** the key function throws for one payload in a prepare batch
- **THEN** that payload SHALL climb the entry channel's retry→park ladder, and the other payloads in the batch SHALL be unaffected

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
The PHP SDK SHALL support the producer-side edge surface — map, filter, flatMap and keyBy on stream handles, and merge with per-source mapping — with transforms composed at declaration time and evaluated where the core routes outputs. `$c->merge()` SHALL return a merge handle with `from($source, callable $map)`, `keyBy(callable $key)`, `into(string $class, ?string $nodeName = null)`, recording each source as a chain closed by the core's `merge` link under one group and taking the merge refusals from the reference check. `$c->routeLatest($documents, $traffic, callable $keyFn)` and `routeLatestNamed(..., string $nodeName)` SHALL expand to the `sdk-graph-authoring` router, packaged in the SDK as a single-cell tasklet with journaled `{document, version, delivered}` state.

#### Scenario: A chain of transforms is applied once per value
- **WHEN** several transforms are declared on one edge before its terminal `into`
- **THEN** each input SHALL traverse the composed chain exactly once

#### Scenario: A throwing transform is contained
- **WHEN** an edge transform throws
- **THEN** the failure SHALL climb the channel's retry→park ladder rather than terminating the runner

#### Scenario: Two channels merge into one tasklet
- **WHEN** an operative declares `$c->merge()->from($ticks, fn ($t) => [...])->from($quotes, fn ($q) => [...])->keyBy(fn ($v) => $v['symbol'])->into(Detector::class)`
- **THEN** registration SHALL succeed with one Detector node fed by two consumer edges, each applying its own mapping, a value from either entry SHALL reach `run` as the mapped value keyed by `symbol`, a duplicated source SHALL refuse with `DuplicateMergeSource` and a single source with `MergeTooFewSources`

#### Scenario: The router delivers on a key's own traffic
- **WHEN** `$c->routeLatest($config, $trades, fn ($t) => $t['symbol'])` is declared and the harness feeds a document, trades for `A` and `B`, the same document, then a trade for `A`
- **THEN** exactly one `['A', $document]` and one `['B', $document]` SHALL be emitted, and the repeated document SHALL be inert

### Requirement: Workflow operatives at parity
The PHP SDK SHALL support workflow operatives as structured chains, where each stage partition arrives as a step activation and completion answers the driver's barrier. `Flezha\WorkflowOperative` SHALL carry no one-function `run`, and `Flezha\WorkflowCtx` SHALL NOT exist; a tasklet inside a workflow SHALL end its instance with `$ctx->complete($result)`, carried in its step answer.

Compensation hooks, loop modes, suspension and rewind SHALL be the core driver's existing machinery, reached through step activations; the SDK SHALL NOT implement a driver.

#### Scenario: A structured chain completes in order
- **WHEN** a workflow class declaring a multi-stage chain is spawned
- **THEN** each stage SHALL arrive as a step activation, and the instance SHALL report completion after the terminal stage

#### Scenario: An aborted chain compensates in reverse
- **WHEN** a stage fails after earlier stages committed
- **THEN** the core SHALL hand out `CompensateRun` activations for the committed stages in reverse order

#### Scenario: A tasklet ends its workflow
- **WHEN** a tasklet of a `PerInput` workflow calls `$ctx->complete(['turns' => 4])` while handling a message
- **THEN** the iteration SHALL commit and the instance SHALL complete with that result instead of waiting for the next message

#### Scenario: A resumed chain skips journaled stages
- **WHEN** a workflow instance is resumed after a crash mid-chain
- **THEN** stages recorded as committed in the task-journal SHALL NOT re-run

### Requirement: Clean shutdown on an activation budget
The PHP SDK SHALL accept an optional maximum-activation count after which the runner shuts down cleanly and exits zero, so an external supervisor can recycle a long-lived process without data loss.

The budget SHALL default to unlimited, and a clean shutdown SHALL leave unpolled work journaled controller-side for re-forwarding on the next bind.

#### Scenario: The runner exits cleanly at its budget
- **WHEN** a runner started with an activation budget reaches it
- **THEN** it SHALL stop polling, shut the context down, and exit with status zero

#### Scenario: Recycling loses no work
- **WHEN** a runner exits at its budget while inputs remain unconsumed
- **THEN** a replacement runner binding the same operative SHALL receive those inputs

### Requirement: End-to-end parity is the acceptance gate
The PHP SDK SHALL be gated by a runner application under `test/apps/php_runner`, declaring its launch line and its fixtures in a `conformance.json` manifest exactly as the Rust, Java and Elixir apps do, and driven by `php_sdk`-tagged suites in `test/system` selected by `FLEZHA_E2E_RUNTIME=php`.

Coverage SHALL include timers, dynamic cells, compaction, DLQ park, effectively-once, live state read and workflow crash-resume — the same scenarios the Java arm asserts — and the manifest SHALL defer nothing, because this SDK claims full parity.

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
- **THEN** it authors the row with `$c->merge()->from(...)…->into(...)` rather than throwing, registration refuses with `MergeTooFewSources` or `DuplicateMergeSource`, and the manifest's `topology_surfaces` lists `merge`

### Requirement: The single-tasklet short form is four attribute arguments
The short form SHALL be declared with the `#[Operative]` attribute a full-form operative already carries, on a class extending `Flezha\StreamOperative` or `Flezha\WorkflowOperative`, and SHALL NOT be declared with `#[Task]`, which SHALL accept none of `kind`, `sink`, `input` or `codec` and SHALL throw naming the argument and `#[Operative]` when one is given. `#[Operative]` SHALL accept `sink`, `input` and `codec` (a `ChannelCodec`) beside the `factoryId` it already requires; the kind is the base class the attributed class extends and SHALL NOT be an argument. `Flezha\OperativeKind` SHALL be deleted, its only readers being the removed `kind`.

A class SHALL be a short form exactly when it defines the tasklet body `run(mixed $input, Ctx $ctx): mixed` and does not override the topology body `channels`, which is the mechanism `Definition` already uses to read which hooks a class overrides; defining `run` and overriding `channels` SHALL throw `DeclarationException` naming `ShortFormWithTopology`, and doing neither SHALL throw naming the missing body. A short-form class MAY define the tasklet hooks a `Flezha\Tasklet` subclass defines (`state`, `applyChange`), which the generated tasklet takes.

`sink` SHALL be optional. The class SHALL carry a generated operative of its base class's kind whose factory id is `factoryId`, wiring an entry channel named `input` (or the `input` argument's value) into a generated tasklet named `factoryId` and, when a `sink` is written, that tasklet to the `sink` plugin channel; with no `sink`, it SHALL wire the entry channel into the tasklet and declare no plugin channel. A workflow short form MAY carry `#[Workflow(loopMode: ...)]` beside `#[Operative]` and SHALL be a `once` workflow when it does not, with every other workflow setting at its default, fed by its first message on the entry channel and taking `init_values` as the tasklet's initial state; a `#[Workflow]` on a short form that sets any argument other than `loopMode`, or `persistent: false` on `#[Operative]`, SHALL throw `DeclarationException` naming the setting and the full form. The entry SHALL be decoded by `codec` where one is written, whether or not a `sink` is. An absent `factoryId` SHALL throw naming the required argument, and the SDK SHALL NOT derive one from the class.

`Runner::registerFactory` SHALL accept such a class string and register the generated operative, throwing `DeclarationException` at registration with the refusal's name in its refusals and at the start of its message. A duplicate factory id SHALL throw with a message beginning `DuplicateFactoryId`. The PHP SDK owes no `SinkWithoutOutput`: `Flezha\Tasklet::run` is declared `: mixed` and PHP refuses an override narrowing that to `: void`, so a tasklet declaring no output cannot be written; the manifest SHALL NOT list `shorthand_typed_output`.

#### Scenario: An attributed class is a whole stream operative

- **WHEN** a class extending `Flezha\StreamOperative` is attributed `#[Operative(factoryId: 'upcase', sink: 'out', codec: ChannelCodec::Text)]`, defines `run`, and the runner is given `registerFactory(Upcase::class)`
- **THEN** the runner declares a stream operative `upcase` with entry channel `input` decoding text, a tasklet named `upcase` and sink `out`, and no `Flezha\Tasklet` subclass exists for it

#### Scenario: An attributed class is a whole workflow operative

- **WHEN** a class extending `Flezha\WorkflowOperative` is attributed `#[Operative(factoryId: 'place', sink: 'placed', input: 'order', codec: ChannelCodec::Text)]` and defines `run`, the runner is given `registerFactory(Place::class)`, an instance is started, and one message is sent on `order`
- **THEN** `run` receives the message as text, its return value reaches `placed`, and the instance completes

#### Scenario: A short-form workflow declares its loop mode

- **WHEN** a class extending `Flezha\WorkflowOperative` is attributed `#[Operative(factoryId: 'chat', sink: 'replies', codec: ChannelCodec::Text)]` and `#[Workflow(loopMode: LoopMode::PerInput)]`, defines `run`, is registered, and two messages reach its entry channel
- **THEN** the declaration carries `loop_mode` `per_input`, `run` runs once per message, and the instance does not complete after the first

#### Scenario: A short form with no sink is declared

- **WHEN** a class extending `Flezha\StreamOperative` is attributed `#[Operative(factoryId: 'counter', input: 'ticks', codec: ChannelCodec::Text)]` and defines `run`, `state` and `applyChange`
- **THEN** the operative declares `ticks` into the generated tasklet and no plugin channel

#### Scenario: An input and codec need no sink

- **WHEN** a class extending `Flezha\StreamOperative` is attributed `#[Operative(factoryId: 'counter', input: 'ticks', codec: ChannelCodec::Text)]` with no `sink`
- **THEN** `registerFactory` accepts it and the entry channel takes that name and codec

#### Scenario: A run body beside a channels override is refused

- **WHEN** an attributed operative class defines `run` and overrides `channels`
- **THEN** `registerFactory` throws `DeclarationException` naming `ShortFormWithTopology`

#### Scenario: A tasklet cannot declare no output

- **WHEN** a class extending `Flezha\Tasklet` declares `public function run(mixed $input, Ctx $ctx): void`
- **THEN** it SHALL fail to load, because `Tasklet::run` is declared `: mixed`, so no PHP tasklet can declare no output and the SDK owes no `SinkWithoutOutput`

#### Scenario: A sink on a tasklet attribute is refused

- **WHEN** a class is attributed `#[Task(name: 'upcase', sink: 'out')]`
- **THEN** `registerFactory` throws naming `sink` and `#[Operative]`

#### Scenario: A derived factory id is refused

- **WHEN** a class extending `Flezha\StreamOperative` defines `run` and carries no `#[Operative]` attribute
- **THEN** `registerFactory` throws naming the required `#[Operative(factoryId: ...)]`

### Requirement: A PHP class of either kind is judged on what it wrote

`HostClassDecl::of` SHALL put every key the builder recorded on the wire for either kind, with no kind gate, so a stream-only builder call on a workflow reaches the core and is refused under its name (`EffectivelyOnceOnWorkflow`, `CheckpointOnWorkflow`, `CorrectionWindowOnWorkflow`) when the runner declares the class to the shared core (at start, or at the registration itself for a class registered after start), as every other core refusal in this SDK is raised, as a `DeclarationException` naming it. `#[Workflow]` on a class extending `Flezha\StreamOperative` SHALL be refused at `registerFactory` naming the attribute and the class, as the attribute's own documentation states. `#[Workflow]` SHALL carry no `retryAttempts`, `retryBackoffMs`, `barrierRetryAttempts` or `wakeAfterMs`, and the declaration SHALL carry no `wake_after_ms`; the builder SHALL carry no `syncDurability()` call, `#[Workflow(syncDurability: true)]` being the one spelling. A `Ctx` inside a workflow SHALL offer `timerAfter`, `timerAtEventTime` and `cancelTimer` exactly as inside a stream, and a tasklet's `onTimer` SHALL run on its firing in both kinds; only a `compensate` hook's `Ctx` SHALL throw an error naming `TimersUnavailable` from them.

#### Scenario: A tier call on a workflow reaches the core

- **WHEN** a class extending `Flezha\WorkflowOperative` calls `$c->effectivelyOnce()` in `channels`, the runner registers it and starts
- **THEN** declaring it to the core throws `DeclarationException` naming `EffectivelyOnceOnWorkflow`, and the operative is not declared

#### Scenario: A workflow attribute on a stream class is refused

- **WHEN** a class extending `Flezha\StreamOperative` carries `#[Workflow(loopMode: LoopMode::PerInput)]`
- **THEN** `registerFactory` throws `DeclarationException` naming `#[Workflow]` and the class

#### Scenario: The retired wake argument is refused

- **WHEN** a class carries `#[Workflow(wakeAfterMs: 5000)]`
- **THEN** PHP refuses the unknown named argument when the attribute is instantiated, and no declaration carries `wake_after_ms`

#### Scenario: A timer inside a workflow tasklet fires

- **WHEN** a tasklet of a running `PerInput` workflow calls `$ctx->timerAfter(300)` in `run`
- **THEN** the call returns the scheduled timestamp, the effect carries the timer op, and `onTimer` runs on that Cell at that timestamp, opening a pass at that tasklet

