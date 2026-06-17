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
