# java-sdk-procedure-annotations Specification

## Purpose

The java-sdk-procedure-annotations capability is the Java realization of the SDK authoring contract. It provides an annotation processor for `@Procedure` / `@State` / `@StreamOperative` / `@EntitiesFor` (and `@EntityKey` for dynamic mode) that generates the six `*Type()` getters, `name`, `initState`, the `Procedure` implementation, `applyChange`, and `OperativeRegistry.registerStream`. The processor mirrors the Rust SDK's signature-to-types, state, entity mode, and single-procedure auto-wiring inference rules so an equivalent operative expands to a structurally identical topology across both SDKs. Multi-procedure topology stays explicitly declared in a `channels()` method, and all generated code targets the existing trait/interface surface with no engine, protocol, or WAL change.

## Requirements

### Requirement: `@Procedure` annotation processor

The Java SDK SHALL provide an annotation processor that generates a complete `Procedure` implementation from an `@Procedure`-annotated class with a `run` method, conforming to the authoring contract.

#### Scenario: Stateless procedure generates type getters and impl

- **WHEN** a class annotated `@Procedure(id = "single_mode", sink = "sink")` declares `String run(String input)`
- **THEN** the processor generates the six `*Type()` getters, `name`, `initState`, and a `Procedure` implementation with `Input = String` and `Output = String`

#### Scenario: Stateful procedure infers State from the run parameter

- **WHEN** an `@Procedure` class declares `String run(String input, StaticEntryState state)`
- **THEN** the generated `Procedure` sets the state type to `StaticEntryState` and routes mutations through the existing state-handle path

#### Scenario: Generated procedure is referenceable in channels

- **WHEN** an `@Procedure` class `StaticEntry` is processed
- **THEN** its generated `Procedure` type can be named in a `@StreamOperative` `channels()` body via `from`/`into`

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

### Requirement: `@StreamOperative` registration and explicit topology

The Java SDK SHALL support a `@StreamOperative` annotation that generates `OperativeRegistry.registerStream`, while multi-procedure topology stays explicitly declared in a `channels()` method.

#### Scenario: Single-procedure operative is auto-wired and registered

- **WHEN** a single `@Procedure` declares an `id` and `sink`
- **THEN** the processor generates and registers an operative wiring input → procedure → sink with no explicit `registerStream` call

#### Scenario: Multi-procedure operative keeps explicit channels

- **WHEN** a `@StreamOperative`-annotated class declares a `channels()` method wiring multiple procedures
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
