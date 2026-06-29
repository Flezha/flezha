# java-sdk-operation-annotations Specification

## Purpose

The java-sdk-operation-annotations capability is the Java realization of the SDK authoring contract. It provides an annotation processor for `@Operation` / `@State` / `@ContinuousOperative` / `@SegmentFor` (and `@OperationKey` for dynamic mode) that generates the six `*Type()` getters, `name`, `initState`, the `Operation` implementation, `applyChange`, and `OperativeRegistry.registerContinuous`. The processor mirrors the Rust SDK's signature-to-types, state, segmentation-mode, and single-operation auto-wiring inference rules so an equivalent operative expands to a structurally identical topology across both SDKs. Multi-operation topology stays explicitly declared in a `channels()` method, and all generated code targets the existing trait/interface surface with no engine, protocol, or WAL change.

## Requirements

### Requirement: `@Operation` annotation processor

The Java SDK SHALL provide an annotation processor that generates a complete `Operation` implementation from an `@Operation`-annotated class with a `run` method, conforming to the authoring contract.

#### Scenario: Stateless operation generates type getters and impl

- **WHEN** a class annotated `@Operation(id = "single_mode", sink = "sink")` declares `String run(String input)`
- **THEN** the processor generates the six `*Type()` getters, `name`, `initState`, and an `Operation` implementation with `Input = String` and `Output = String`

#### Scenario: Stateful operation infers State from the run parameter

- **WHEN** an `@Operation` class declares `String run(String input, StaticEntryState state)`
- **THEN** the generated `Operation` sets the state type to `StaticEntryState` and routes mutations through the existing state-handle path

#### Scenario: Generated operation is referenceable in channels

- **WHEN** an `@Operation` class `StaticEntry` is processed
- **THEN** its generated `Operation` type can be named in a `@ContinuousOperative` `channels()` body via `from`/`into`

### Requirement: `@State` change generation

The Java SDK SHALL provide a `@State` annotation whose processor generates a `Change` type and `applyChange` for field-granular mutations, journaled through the existing WAL.

#### Scenario: Field mutation is journaled

- **WHEN** a `@State` class has a list field appended to inside `run`
- **THEN** a generated change records the mutation and it is applied through the generated `applyChange`

#### Scenario: Unsupported mutation requires manual change

- **WHEN** a mutation cannot be expressed as a field-granular change
- **THEN** the processor requires a hand-written change type and does not generate incorrect journaling

### Requirement: `@SegmentFor` and segment opt-ins

The Java SDK SHALL support a `@SegmentFor` method annotation and `@Operation` attributes for static keys, mailbox capacity, and overflow policy.

#### Scenario: Segment method and static keys

- **WHEN** a `@Operation(keys = {"a","b","c","d"})` class has a method annotated `@SegmentFor`
- **THEN** the generated `Operation` sets `operationKeys()` to the listed keys and delegates `segmentFor` to that method

#### Scenario: Mailbox and overflow opt-ins

- **WHEN** `@Operation` declares capacity and overflow attributes
- **THEN** the generated `Operation` overrides `mailboxCapacity()` and the channel overflow policy accordingly

#### Scenario: Dynamic-mode opt-in

- **WHEN** an `@Operation` class marks a method `@OperationKey` returning `Optional<String>`, optionally with `idleTtl` / `maxSegments` attributes
- **THEN** the generated `Operation` sets the dynamic-mode override, delegates `operationKey` to that method, leaves `operationKeys()` empty, and applies the TTL / cap overrides per `relay-dynamic-partitioning`

#### Scenario: Static and dynamic opt-ins are mutually exclusive

- **WHEN** an `@Operation` declares both `@SegmentFor` (static) and `@OperationKey` (dynamic)
- **THEN** the processor reports a compile-time error that the two modes are mutually exclusive

### Requirement: `@ContinuousOperative` registration and explicit topology

The Java SDK SHALL support a `@ContinuousOperative` annotation that generates `OperativeRegistry.registerContinuous`, while multi-operation topology stays explicitly declared in a `channels()` method.

#### Scenario: Single-operation operative is auto-wired and registered

- **WHEN** a single `@Operation` declares an `id` and `sink`
- **THEN** the processor generates and registers an operative wiring input → operation → sink with no explicit `registerContinuous` call

#### Scenario: Multi-operation operative keeps explicit channels

- **WHEN** a `@ContinuousOperative`-annotated class declares a `channels()` method wiring multiple operations
- **THEN** the processor generates registration but preserves the author's explicit topology

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

### Requirement: `Optional<Output>` return in `@Operation`

The `@Operation` annotation processor SHALL accept a `run` method returning `Optional<Output>`, emitting when present and emitting nothing when absent. The `void` return (no output) and unconditional `Output` return (always emit) SHALL be unchanged.

#### Scenario: Optional return drives conditional emit

- **WHEN** a `@Operation` `run` returns `Optional<Output>`
- **THEN** the generated operation emits on present and emits nothing on empty, without the author referencing the operation context

#### Scenario: Generated operative matches the hand-written twin

- **WHEN** a keyed-collection operation authored with `@State` + `@Operation` returning `Optional<Output>` is processed
- **THEN** the generated operative processes input identically to a hand-written twin and `replayEquals()` is empty

### Requirement: `@InitState` method on `@Operation`

The `@Operation` annotation processor SHALL recognise an `@InitState`-annotated method `State name(Optional<String> operationKey)` on the authoring class and generate an `Operation.initState(operationKey, payload)` that delegates to it, passing the operation key. When no `@InitState` method is present, the generated `initState` SHALL keep returning `new State()`, unchanged. Declaring `@InitState` on a stateless operation (no `@State` handle parameter on `run`) SHALL be a processor error. The restore payload SHALL NOT be passed to the `@InitState` method.

#### Scenario: @InitState delegates initState to the method

- **WHEN** a `@Operation` class with a stateful `run` declares an `@InitState State initState(Optional<String> operationKey)` method
- **THEN** the generated operation's `initState(operationKey, payload)` returns `impl.initState(operationKey)`, so each static segment starts from key-derived state, and `OperationTestHarness.replayEquals()` is empty for the operation

#### Scenario: @InitState on a stateless operation is rejected

- **WHEN** `@InitState` is declared on a `@Operation` class whose `run` has no `@State` handle parameter
- **THEN** the processor emits an error stating `@InitState` requires a stateful run, and generates no init delegation
