# java-sdk-procedure-annotations Specification

## Purpose

The java-sdk-procedure-annotations capability is the Java realization of the SDK authoring contract. It provides an annotation processor for `@Procedure` / `@State` / `@EntitiesFor` (and `@EntityKey` for dynamic mode) that generates the six `*Type()` getters, `name`, `initState`, the `Procedure` implementation, `applyChange`, and — for a `@Procedure` declaring a `sink` — the short form's `<Class>StreamOperative` or `<Class>WorkflowOperative`, which the application registers with `FlezhaRunner.registerFactory`. The processor mirrors the Rust SDK's signature-to-types, state, entity mode, and single-procedure short-form inference rules so an equivalent operative expands to a structurally identical topology across both SDKs. Multi-procedure topology stays explicitly declared in a `channels()` method, and all generated code targets the existing trait/interface surface with no engine, protocol, or WAL change.
## Requirements
### Requirement: `@Procedure` annotation processor

The Java SDK SHALL provide an annotation processor that generates a complete `Procedure` implementation from an `@Procedure`-annotated class with a `run` method, conforming to the authoring contract. The generated procedure's `name()` SHALL be the `id` element when one is written, and the class name in snake_case otherwise. The annotation SHALL carry a `kind` element declared `OperativeKind[] kind() default {}` over `enum OperativeKind { STREAM, WORKFLOW }`, and an optional `input` element naming the generated operative's entry channel, defaulting to the empty string, which the processor SHALL lower to the entry channel name `input`. A class declaring a `sink` SHALL be a short form: the processor SHALL generate `<Class>StreamOperative implements StreamOperative` (for `STREAM`, with `channels()`) or `<Class>WorkflowOperative implements WorkflowOperative` (for `WORKFLOW`, with `procedures()`) wiring the entry channel into `<Class>Procedure` and `<Class>Procedure` to the named plugin sink, with `factoryId()` equal to the `id` and every other operative method at its interface default, and SHALL register nothing; the application registers the generated class with `registerFactory`. The processor SHALL NOT generate an aggregate registration class. Each short-form refusal SHALL fail compilation with a message beginning with its name: `SinkWithoutId` for a `sink` with an empty `id`, `SinkWithoutKind` for a `sink` with no `kind`, `SinkWithoutOutput` for a `sink` on a class whose `run` returns `void` and declares no `emits`, and `ShorthandKeyWithoutSink` for an `input` or `kind` element on a class with no `sink`, naming the elements. A `kind` element holding more than one value SHALL fail compilation.

#### Scenario: Stateless procedure generates type getters and impl

- **WHEN** a class `SingleMode` annotated `@Procedure(id = "single_mode", kind = OperativeKind.STREAM, sink = "sink")` declares `String run(String input)`
- **THEN** the processor generates the six `*Type()` getters, `name` returning `single_mode`, `initState`, a `Procedure` implementation with `Input = String` and `Output = String`, and `SingleModeStreamOperative`

#### Scenario: Stateful procedure infers State from the run parameter

- **WHEN** an `@Procedure` class declares `String run(String input, StaticEntryState state)`
- **THEN** the generated `Procedure` sets the state type to `StaticEntryState` and routes mutations through the existing state-handle path

#### Scenario: Generated procedure is referenceable in channels

- **WHEN** an `@Procedure` class `StaticEntry` is processed
- **THEN** its generated `Procedure` type can be named in a hand-written `channels()` or `procedures()` body via `from`/`into`

#### Scenario: A written id is the procedure's name

- **WHEN** a class `SugarStore` is annotated `@Procedure(id = "sugar_store_input", kind = OperativeKind.STREAM, sink = "sugar_store_sink")`
- **THEN** the generated procedure's `name()` is `sugar_store_input`, and its state is saved under that name

#### Scenario: A workflow short form generates a workflow operative

- **WHEN** a class `Place` is annotated `@Procedure(id = "place", kind = OperativeKind.WORKFLOW, sink = "placed", input = "order")`
- **THEN** the processor generates `PlaceWorkflowOperative`, and `Flezha.createRunner(ns).registerFactory(PlaceWorkflowOperative.class)` registers a `once` workflow `place` wiring `order` into the procedure and the procedure to `placed`

#### Scenario: Nothing registers itself

- **WHEN** an application declares short-form classes and never calls `registerFactory` for their generated operatives
- **THEN** its runner declares no operative for them, and no `com.flezha.generated.GeneratedStreamOperatives` class exists

#### Scenario: The generated entry channel keeps its default name

- **WHEN** a class annotated `@Procedure(id = "single_mode", kind = OperativeKind.STREAM, sink = "sink")` declares no `input` element
- **THEN** the generated operative's entry channel SHALL be named `input`

#### Scenario: The annotation names the generated entry channel

- **WHEN** a class is annotated `@Procedure(id = "single_mode", kind = OperativeKind.STREAM, sink = "sink", input = "prices")`
- **THEN** the generated operative's entry channel SHALL be named `prices`

#### Scenario: A short-form element without a `sink` is refused

- **WHEN** a class is annotated `@Procedure(id = "single_mode", input = "prices")` with no `sink`
- **THEN** the processor SHALL fail compilation with a message beginning `ShorthandKeyWithoutSink`, naming `input` and the missing `sink`

#### Scenario: A `sink` without an `id` is refused rather than derived

- **WHEN** a class `Upcase` is annotated `@Procedure(kind = OperativeKind.STREAM, sink = "out")` with no `id`
- **THEN** the processor SHALL fail compilation with a message beginning `SinkWithoutId`, and SHALL NOT generate an operative under `upcase`

#### Scenario: A `sink` without a `kind` is refused

- **WHEN** a class is annotated `@Procedure(id = "upcase", sink = "out")`
- **THEN** the processor SHALL fail compilation with a message beginning `SinkWithoutKind`

#### Scenario: A procedure with no `sink` keeps its derived id

- **WHEN** a class `StaticDownstream` is annotated `@Procedure` with neither `id` nor `sink`
- **THEN** the generated procedure is named `static_downstream`, as before

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

### Requirement: `@EntitiesFor` and entity opt-ins

The Java SDK SHALL support a `@EntitiesFor` method annotation and `@Procedure` attributes for static keys, mailbox capacity, and overflow policy.

#### Scenario: Entity method and static keys

- **WHEN** a `@Procedure(keys = {"a","b","c","d"})` class has a method annotated `@EntitiesFor`
- **THEN** the generated `Procedure` sets `entityKeys()` to the listed keys and delegates `entitiesFor` to that method

#### Scenario: Mailbox and overflow opt-ins

- **WHEN** `@Procedure` declares capacity and overflow attributes
- **THEN** the generated `Procedure` overrides `mailboxCapacity()` and the channel overflow policy accordingly

#### Scenario: Dynamic-mode opt-in

- **WHEN** an `@Procedure` class marks a method `@EntityKey` returning `Optional<String>`, optionally with `idleTtl` / `maxEntities` attributes
- **THEN** the generated `Procedure` sets the dynamic-mode override, delegates `entityKey` to that method, leaves `entityKeys()` empty, and applies the TTL / cap overrides per the dynamic entity-mode rules

#### Scenario: Static and dynamic opt-ins are mutually exclusive

- **WHEN** an `@Procedure` declares both `@EntitiesFor` (static) and `@EntityKey` (dynamic)
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

### Requirement: `Optional<Output>` return in `@Procedure`

The `@Procedure` annotation processor SHALL accept a `run` method returning `Optional<Output>`, emitting when present and emitting nothing when absent. The `void` return (no output) and unconditional `Output` return (always emit) SHALL be unchanged.

#### Scenario: Optional return drives conditional emit

- **WHEN** a `@Procedure` `run` returns `Optional<Output>`
- **THEN** the generated procedure emits on present and emits nothing on empty, without the author referencing the procedure context

#### Scenario: Generated operative matches the hand-written twin

- **WHEN** a keyed-collection procedure authored with `@State` + `@Procedure` returning `Optional<Output>` is processed
- **THEN** the generated operative processes input identically to a hand-written twin and `replayEquals()` is empty

### Requirement: `@InitState` method on `@Procedure`

The `@Procedure` annotation processor SHALL recognise an `@InitState`-annotated method `State name(Optional<String> entityKey)` on the authoring class and generate a `Procedure.initState(entityKey, payload)` that delegates to it, passing the procedure key. When no `@InitState` method is present, the generated `initState` SHALL keep returning `new State()`, unchanged. Declaring `@InitState` on a stateless procedure (no `@State` handle parameter on `run`) SHALL be a processor error. The restore payload SHALL NOT be passed to the `@InitState` method.

#### Scenario: @InitState delegates initState to the method

- **WHEN** a `@Procedure` class with a stateful `run` declares an `@InitState State initState(Optional<String> entityKey)` method
- **THEN** the generated procedure's `initState(entityKey, payload)` returns `impl.initState(entityKey)`, so each static entity starts from key-derived state, and `ProcedureTestHarness.replayEquals()` is empty for the procedure

#### Scenario: @InitState on a stateless procedure is rejected

- **WHEN** `@InitState` is declared on a `@Procedure` class whose `run` has no `@State` handle parameter
- **THEN** the processor emits an error stating `@InitState` requires a stateful run, and generates no init delegation
