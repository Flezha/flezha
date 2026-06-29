# java-sdk-shard-annotations Specification

## Purpose

The java-sdk-shard-annotations capability is the Java realization of the SDK authoring contract. It provides an annotation processor for `@Shard` / `@State` / `@Relay` / `@PartitionFor` (and `@ShardKey` for dynamic mode) that generates the six `*Type()` getters, `name`, `initState`, the `Shard` implementation, `applyChange`, and `RelayRegistry.registerRelay`. The processor mirrors the Rust SDK's signature-to-types, state, partitioning-mode, and single-shard auto-wiring inference rules so an equivalent relay expands to a structurally identical topology across both SDKs. Multishard topology stays explicitly declared in a `channels()` method, and all generated code targets the existing trait/interface surface with no engine, protocol, or WAL change.

## Requirements

### Requirement: `@Shard` annotation processor

The Java SDK SHALL provide an annotation processor that generates a complete `Shard` implementation from a `@Shard`-annotated class with a `run` method, conforming to the authoring contract.

#### Scenario: Stateless shard generates type getters and impl

- **WHEN** a class annotated `@Shard(id = "single_mode", sink = "sink")` declares `String run(String input)`
- **THEN** the processor generates the six `*Type()` getters, `name`, `initState`, and a `Shard` implementation with `Input = String` and `Output = String`

#### Scenario: Stateful shard infers State from the run parameter

- **WHEN** a `@Shard` class declares `String run(String input, StaticEntryState state)`
- **THEN** the generated `Shard` sets the state type to `StaticEntryState` and routes mutations through the existing state-handle path

#### Scenario: Generated shard is referenceable in channels

- **WHEN** a `@Shard` class `StaticEntry` is processed
- **THEN** its generated `Shard` type can be named in a `@Relay` `channels()` body via `from`/`into`

### Requirement: `@State` change generation

The Java SDK SHALL provide a `@State` annotation whose processor generates a `Change` type and `applyChange` for field-granular mutations, journaled through the existing WAL.

#### Scenario: Field mutation is journaled

- **WHEN** a `@State` class has a list field appended to inside `run`
- **THEN** a generated change records the mutation and it is applied through the generated `applyChange`

#### Scenario: Unsupported mutation requires manual change

- **WHEN** a mutation cannot be expressed as a field-granular change
- **THEN** the processor requires a hand-written change type and does not generate incorrect journaling

### Requirement: `@PartitionFor` and partition opt-ins

The Java SDK SHALL support a `@PartitionFor` method annotation and `@Shard` attributes for static keys, mailbox capacity, and overflow policy.

#### Scenario: Partition method and static keys

- **WHEN** a `@Shard(keys = {"a","b","c","d"})` class has a method annotated `@PartitionFor`
- **THEN** the generated `Shard` sets `shardKeys()` to the listed keys and delegates `partitionFor` to that method

#### Scenario: Mailbox and overflow opt-ins

- **WHEN** `@Shard` declares capacity and overflow attributes
- **THEN** the generated `Shard` overrides `mailboxCapacity()` and the channel overflow policy accordingly

#### Scenario: Dynamic-mode opt-in

- **WHEN** a `@Shard` class marks a method `@ShardKey` returning `Optional<String>`, optionally with `idleTtl` / `maxPartitions` attributes
- **THEN** the generated `Shard` sets the dynamic-mode override, delegates `shardKey` to that method, leaves `shardKeys()` empty, and applies the TTL / cap overrides per `relay-dynamic-partitioning`

#### Scenario: Static and dynamic opt-ins are mutually exclusive

- **WHEN** a `@Shard` declares both `@PartitionFor` (static) and `@ShardKey` (dynamic)
- **THEN** the processor reports a compile-time error that the two modes are mutually exclusive

### Requirement: `@Relay` registration and explicit topology

The Java SDK SHALL support a `@Relay` annotation that generates `RelayRegistry.registerRelay`, while multishard topology stays explicitly declared in a `channels()` method.

#### Scenario: Single-shard relay is auto-wired and registered

- **WHEN** a single `@Shard` declares an `id` and `sink`
- **THEN** the processor generates and registers a relay wiring input → shard → sink with no explicit `registerRelay` call

#### Scenario: Multishard relay keeps explicit channels

- **WHEN** a `@Relay`-annotated class declares a `channels()` method wiring multiple shards
- **THEN** the processor generates registration but preserves the author's explicit topology

### Requirement: Mirrors the Rust inference rules

The Java processor SHALL apply the same signature-to-types, state, and auto-wiring inference rules as the Rust SDK, per the authoring contract.

#### Scenario: Equivalent relay matches the Rust expansion

- **WHEN** the reference relay is authored with Java annotations and with Rust macros
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

### Requirement: `Optional<Output>` return in `@Shard`

The `@Shard` annotation processor SHALL accept a `run` method returning `Optional<Output>`, emitting when present and emitting nothing when absent. The `void` return (no output) and unconditional `Output` return (always emit) SHALL be unchanged.

#### Scenario: Optional return drives conditional emit

- **WHEN** a `@Shard` `run` returns `Optional<Output>`
- **THEN** the generated shard emits on present and emits nothing on empty, without the author referencing the shard context

#### Scenario: Generated relay matches the hand-written twin

- **WHEN** a keyed-collection shard authored with `@State` + `@Shard` returning `Optional<Output>` is processed
- **THEN** the generated relay processes input identically to a hand-written twin and `replayEquals()` is empty

### Requirement: `@InitState` method on `@Shard`

The `@Shard` annotation processor SHALL recognise an `@InitState`-annotated method `State name(Optional<String> shardKey)` on the authoring class and generate a `Shard.initState(shardKey, payload)` that delegates to it, passing the shard key. When no `@InitState` method is present, the generated `initState` SHALL keep returning `new State()`, unchanged. Declaring `@InitState` on a stateless shard (no `@State` handle parameter on `run`) SHALL be a processor error. The restore payload SHALL NOT be passed to the `@InitState` method.

#### Scenario: @InitState delegates initState to the method

- **WHEN** a `@Shard` class with a stateful `run` declares an `@InitState State initState(Optional<String> shardKey)` method
- **THEN** the generated shard's `initState(shardKey, payload)` returns `impl.initState(shardKey)`, so each static partition starts from key-derived state, and `ShardTestHarness.replayEquals()` is empty for the shard

#### Scenario: @InitState on a stateless shard is rejected

- **WHEN** `@InitState` is declared on a `@Shard` class whose `run` has no `@State` handle parameter
- **THEN** the processor emits an error stating `@InitState` requires a stateful run, and generates no init delegation
