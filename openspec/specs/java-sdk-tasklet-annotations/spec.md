# java-sdk-tasklet-annotations Specification

## Purpose

The java-sdk-tasklet-annotations capability is the Java realization of the SDK authoring contract. It provides an annotation processor for `@Tasklet` / `@State` / `@CellsFor` (and `@CellKey` for dynamic mode) that generates the six `*Type()` getters, `name`, `initState`, the `Tasklet` implementation, `applyChange`, and — for a `@Tasklet` declaring a `sink` — the short form's `<Class>StreamOperative` or `<Class>WorkflowOperative`, which the application registers with `FlezhaRunner.registerFactory`. The processor mirrors the Rust SDK's signature-to-types, state, cell mode, and single-tasklet short-form inference rules so an equivalent operative expands to a structurally identical topology across both SDKs. Multi-tasklet topology stays explicitly declared in a `channels()` method, and all generated code targets the existing trait/interface surface with no engine, protocol, or WAL change.
## Requirements
### Requirement: `@Tasklet` annotation processor

The Java SDK SHALL provide an annotation processor that generates a complete `Tasklet` implementation from an `@Tasklet`-annotated class with a `run` method, conforming to the authoring contract. The generated tasklet's `name()` SHALL be the `id` element when one is written, and the class name in snake_case otherwise. The annotation SHALL declare a tasklet and nothing else: it SHALL NOT carry `kind`, `sink` or `input`, which belong to an operative declaration, and the processor SHALL fail compilation naming the member and directing the author to `@StreamOperative` or `@WorkflowOperative` when one is written. The processor SHALL generate no operative class from `@Tasklet` and SHALL NOT generate an aggregate registration class.

#### Scenario: Stateless tasklet generates type getters and impl

- **WHEN** a class `SingleMode` annotated `@Tasklet(id = "single_mode")` declares `String run(String input)`
- **THEN** the processor generates the six `*Type()` getters, `name` returning `single_mode`, `initState`, a `Tasklet` implementation with `Input = String` and `Output = String`, and no operative class

#### Scenario: Stateful tasklet infers State from the run parameter

- **WHEN** an `@Tasklet` class declares `String run(String input, StaticEntryState state)`
- **THEN** the generated `Tasklet` sets the state type to `StaticEntryState` and routes mutations through the existing state-handle path

#### Scenario: Generated tasklet is referenceable in channels

- **WHEN** an `@Tasklet` class `StaticEntry` is processed
- **THEN** its generated `Tasklet` type can be named in a hand-written `channels()` or `tasklets()` body via `from`/`into`

#### Scenario: A written id is the tasklet's name

- **WHEN** a class `SugarStore` is annotated `@Tasklet(id = "sugar_store_input")`
- **THEN** the generated tasklet's `name()` is `sugar_store_input`, and its state is saved under that name

#### Scenario: A tasklet with no id keeps its derived name

- **WHEN** a class `StaticDownstream` is annotated `@Tasklet` with no `id`
- **THEN** the generated tasklet is named `static_downstream`, as before

#### Scenario: An operative member on a tasklet is refused

- **WHEN** a class annotated `@Tasklet(id = "do_nothing", sink = "echo")`, or the same with `kind` or `input`, is compiled
- **THEN** the processor SHALL fail compilation naming the member and the short-form annotation that carries it

#### Scenario: The duplicate-id exception carries the name

- **WHEN** `FlezhaRunner` is asked to define a second class, of either kind, under a factory id it already holds
- **THEN** the exception message begins with `DuplicateFactoryId` and names the id

### Requirement: `@State` change generation

The Java SDK SHALL provide a `@State` annotation whose processor generates a `Change` type and `applyChange` for field-granular mutations, journaled through the existing WAL.

#### Scenario: Field mutation is journaled

- **WHEN** a `@State` class has a list field appended to inside `run`
- **THEN** a generated change records the mutation and it is applied through the generated `applyChange`

#### Scenario: Unsupported mutation requires manual change

- **WHEN** a mutation cannot be expressed as a field-granular change
- **THEN** the processor requires a hand-written change type and does not generate incorrect journaling

### Requirement: `@CellsFor` and cell opt-ins

The Java SDK SHALL support a `@CellsFor` method annotation and `@Tasklet` attributes for static keys, mailbox capacity, and overflow policy.

#### Scenario: Cell method and static keys

- **WHEN** a `@Tasklet(keys = {"a","b","c","d"})` class has a method annotated `@CellsFor`
- **THEN** the generated `Tasklet` sets `cellKeys()` to the listed keys and delegates `cellsFor` to that method

#### Scenario: Mailbox and overflow opt-ins

- **WHEN** `@Tasklet` declares capacity and overflow attributes
- **THEN** the generated `Tasklet` overrides `mailboxCapacity()` and the channel overflow policy accordingly

#### Scenario: Dynamic-mode opt-in

- **WHEN** an `@Tasklet` class marks a method `@CellKey` returning `Optional<String>`, optionally with `idleTtl` / `maxCells` attributes
- **THEN** the generated `Tasklet` sets the dynamic-mode override, delegates `cellKey` to that method, leaves `cellKeys()` empty, and applies the TTL / cap overrides per the dynamic cell-mode rules

#### Scenario: Static and dynamic opt-ins are mutually exclusive

- **WHEN** an `@Tasklet` declares both `@CellsFor` (static) and `@CellKey` (dynamic)
- **THEN** the processor reports a compile-time error that the two modes are mutually exclusive

### Requirement: Mirrors the Rust inference rules

The Java processor SHALL apply the same signature-to-types, state, and auto-wiring inference rules as the Rust SDK, per the authoring contract.

#### Scenario: Equivalent operative matches the Rust expansion

- **WHEN** the reference operative is authored with Java annotations and with Rust macros
- **THEN** both expand to a structurally identical topology and associated-type mapping

### Requirement: Map-aware `@State` `@Journal` mutators

The `@State` annotation processor SHALL generate journaling mutators, a `Change` type, and `applyChange` for map fields annotated `@Journal(MapSet | MapVec | MapDeque)`, mirroring the Rust derive. The generated mutators perform the mutation live and record it; the generated `applyChange` replays it. Flat and map fields on the same class SHALL compose into a single generated `Change`.

- `MapSet` on `Map<K, V>` → `set<Field>(key, V)`.
- `MapVec` on `Map<K, List<U>>` → `push<Field>(key, U)`, `drain<Field>Front(key, n)`, `replaceLast<Field>(key, U)`.
- `MapDeque` on `Map<K, Deque<U>>` → `pushBack<Field>(key, U)`, `popFront<Field>(key, n)`.

#### Scenario: MapVec field generates collection mutators

- **WHEN** a `@State` class has a `@Journal(MapVec) Map<String, List<Double>> closes` field
- **THEN** the processor generates `pushCloses`, `drainClosesFront`, and `replaceLastCloses`, the matching change records, and an `applyChange` that replays each on the keyed list

#### Scenario: MapSet field generates a keyed setter

- **WHEN** a `@State` class has a `@Journal(MapSet) Map<String, ProductState> byProduct` field
- **THEN** the processor generates `setByProduct(key, ProductState)`, the matching change, and an `applyChange` that inserts the value

#### Scenario: Unannotated or mismatched map field errors

- **WHEN** a map field lacks `@Journal`, or its type does not match the declared mode
- **THEN** the processor reports a compilation error directing the author to a hand-written change, and generates no journaling for that field

### Requirement: `Optional<Output>` return in `@Tasklet`

The `@Tasklet` annotation processor SHALL accept a `run` method returning `Optional<Output>`, emitting when present and emitting nothing when absent. The `void` return (no output) and unconditional `Output` return (always emit) SHALL be unchanged.

#### Scenario: Optional return drives conditional emit

- **WHEN** a `@Tasklet` `run` returns `Optional<Output>`
- **THEN** the generated tasklet emits on present and emits nothing on empty, without the author referencing the tasklet context

#### Scenario: Generated operative matches the hand-written twin

- **WHEN** a keyed-collection tasklet authored with `@State` + `@Tasklet` returning `Optional<Output>` is processed
- **THEN** the generated operative processes input identically to a hand-written twin and `replayEquals()` is empty

### Requirement: `@InitState` method on `@Tasklet`

The `@Tasklet` annotation processor SHALL recognise an `@InitState`-annotated method `State name(Optional<String> cellKey)` on the authoring class and generate a `Tasklet.initState(cellKey, payload)` that delegates to it, passing the cell key. When no `@InitState` method is present, the generated `initState` SHALL keep returning `new State()`, unchanged. Declaring `@InitState` on a stateless tasklet (no `@State` handle parameter on `run`) SHALL be a processor error. The restore payload SHALL NOT be passed to the `@InitState` method.

#### Scenario: @InitState delegates initState to the method

- **WHEN** a `@Tasklet` class with a stateful `run` declares an `@InitState State initState(Optional<String> cellKey)` method
- **THEN** the generated tasklet's `initState(cellKey, payload)` returns `impl.initState(cellKey)`, so each static cell starts from key-derived state, and `TaskletTestHarness.replayEquals()` is empty for the tasklet

#### Scenario: @InitState on a stateless tasklet is rejected

- **WHEN** `@InitState` is declared on a `@Tasklet` class whose `run` has no `@State` handle parameter
- **THEN** the processor emits an error stating `@InitState` requires a stateful run, and generates no init delegation

### Requirement: `@StreamOperative` and `@WorkflowOperative` carry the short form

The Java SDK SHALL provide annotations `@StreamOperative` and `@WorkflowOperative` in `com.flezha.sdk.authoring`, applied to a class with exactly one public method named `run`, declaring a whole single-tasklet operative of the kind the annotation names. Each SHALL require a non-empty `id`, SHALL accept an optional `sink` and an optional `input` defaulting to the empty string, which the processor SHALL lower to the entry channel name `input`, and SHALL NOT accept a `kind` member. `com.flezha.sdk.authoring.OperativeKind` SHALL be deleted, its only readers being the removed `kind`.

The processor SHALL generate `<Class>Tasklet`, whose `name()` is the written `id` verbatim, and `<Class>StreamOperative implements StreamOperative` (with `channels()`) or `<Class>WorkflowOperative implements WorkflowOperative` (with `tasklets()`), whose `factoryId()` is that `id`, whose topology body wires the entry channel into `<Class>Tasklet` and, when a `sink` is written, `<Class>Tasklet` to that plugin sink, and whose every other operative method is at its interface default. With no `sink`, the topology body SHALL declare the entry channel and the tasklet and no plugin channel. The processor SHALL register nothing and SHALL NOT generate an aggregate registration class. An empty or absent `id` SHALL fail compilation naming the required member. A `sink` on a class whose `run` returns `void` and declares no `emits` SHALL fail compilation with a message beginning `SinkWithoutOutput`. An annotated class that also implements `StreamOperative` or `WorkflowOperative` SHALL fail compilation with a message beginning `ShortFormWithTopology`.

`FlezhaRunner.registerFactory` and `unregisterFactory` SHALL accept a class annotated `@StreamOperative` or `@WorkflowOperative` and resolve its generated operative by the processor's naming rule, so the author registers the class they wrote. Resolution SHALL be per class, with no aggregate class written in the processor's last round.

#### Scenario: An annotated class is a whole stream operative

- **WHEN** a class `Echo` with `String run(String s)` is annotated `@StreamOperative(id = "echo", sink = "out")` and the runner is given `registerFactory(Echo.class)`
- **THEN** the runner declares a stream operative `echo` with entry channel `input` and sink `out`, backed by a generated tasklet named `echo`

#### Scenario: An annotated class is a whole workflow operative

- **WHEN** a class `Place` is annotated `@WorkflowOperative(id = "place", sink = "placed", input = "order")`, is registered, an instance is started, and one message is sent on `order`
- **THEN** `run` receives the message on `order`, its return value reaches `placed`, and the instance completes as a `once` workflow

#### Scenario: A short form with no sink is declared

- **WHEN** a class is annotated `@StreamOperative(id = "counter", input = "ticks")` and registered
- **THEN** the generated operative declares `ticks` into the generated tasklet and no plugin channel

#### Scenario: The author registers the class they wrote

- **WHEN** `registerFactory(Echo.class)` is called for an annotated class
- **THEN** the generated operative is declared, and the author never names `EchoStreamOperative`

#### Scenario: Nothing registers itself

- **WHEN** an application declares annotated short-form classes and never calls `registerFactory` for them
- **THEN** its runner declares no operative for them, and no `com.flezha.generated.GeneratedStreamOperatives` class exists

#### Scenario: The generated entry channel keeps its default name

- **WHEN** a class annotated `@StreamOperative(id = "echo", sink = "out")` declares no `input` element
- **THEN** the generated operative's entry channel SHALL be named `input`

#### Scenario: The annotation names the generated entry channel

- **WHEN** a class is annotated `@StreamOperative(id = "echo", sink = "out", input = "prices")`
- **THEN** the generated operative's entry channel SHALL be named `prices`

#### Scenario: An annotation with no id is refused

- **WHEN** a class `Upcase` is annotated `@StreamOperative(sink = "out")` with no `id`
- **THEN** the processor SHALL fail compilation naming the required `id`, and SHALL NOT derive a factory id from the class name

#### Scenario: An annotated class implementing an operative interface is refused

- **WHEN** a class annotated `@StreamOperative` also implements `StreamOperative`
- **THEN** the processor SHALL fail compilation with a message beginning `ShortFormWithTopology`

#### Scenario: A sink on a void run is refused

- **WHEN** a class annotated `@StreamOperative(id = "drain", sink = "out")` declares `void run(String s)` with no `emits`
- **THEN** the processor SHALL fail compilation with a message beginning `SinkWithoutOutput`

