# rust-sdk-shard-macros Specification

## Purpose

The rust-sdk-shard-macros capability is the Rust realization of the SDK authoring contract. It provides the `#[flezha::shard]` attribute macro (signature-to-types inference, `&mut` state inference, partition/mailbox/clock opt-ins, static and dynamic modes), the `#[derive(flezha::State)]` derive (field-granular `Change` enum + `apply_change` journaled through the existing WAL), and the `flezha::relay!` declarative macro for linear pipelines, plus inventory-based auto-registration. All macros expand to the existing `Relay`/`Shard`/`Change`/`Channel` traits with zero engine, protocol, or WAL change; macro-generated and hand-written shards compose in the same relay.

## Requirements

### Requirement: `#[flezha::shard]` attribute macro

The Rust SDK SHALL provide a `#[flezha::shard]` attribute macro applied to a single `run` function that generates a complete `Shard` implementation conforming to the authoring contract.

#### Scenario: Stateless shard collapses to a function

- **WHEN** `#[flezha::shard(id = "do_nothing", sink = "echo")]` annotates `fn do_nothing(input: String) -> Vec<u8>`
- **THEN** the macro generates a `Default` shard type, a `Shard` impl with `Input = String`, `Output = Vec<u8>`, `State = ()`, `Change = NoChange`, `Clock = WallClock`, `Config = ()`, `name`, `init_state`, and a `run` wrapper that emits the returned value

#### Scenario: Stateful shard infers State from the mutable parameter

- **WHEN** `#[flezha::shard(id = "store_input", sink = "sink")]` annotates `fn store_input(input: String, state: &mut StoreInputState) -> String`
- **THEN** the generated `Shard` impl sets `State = StoreInputState` and routes mutations through the existing state-handle path

#### Scenario: Generated shard type is referenceable in channels

- **WHEN** a shard is generated from `fn static_entry(..)`
- **THEN** a PascalCase shard type (e.g. `StaticEntry`) is produced that can be named in an explicit `channels()` body via `.from`/`.into`

### Requirement: Attribute opt-ins for partition and mailbox behavior

The `#[flezha::shard]` macro SHALL accept attribute opt-ins that map to the corresponding `Shard` overrides without dropping to a raw impl.

#### Scenario: Static partition keys and partition_for

- **WHEN** `#[flezha::shard(id = "static_entry_shard", keys = ["a","b","c","d"], partition_for = part)]` is used with a free function `part`
- **THEN** the generated impl sets `shard_keys()` to the listed keys, marks partitioning overridden, and delegates `partition_for` to `part`

#### Scenario: Mailbox capacity and overflow policy

- **WHEN** `capacity` and `on_full` attributes are supplied
- **THEN** the generated impl overrides `mailbox_capacity()` and the channel's overflow policy accordingly

#### Scenario: Clock override

- **WHEN** a `clock` attribute names a supported clock
- **THEN** the generated `Shard` uses that clock instead of `WallClock`

#### Scenario: Dynamic-mode opt-in

- **WHEN** `#[flezha::shard(id = "rsi", shard_key = coin_of)]` is used with a free function `coin_of(&Trade) -> Option<String>`, optionally with `idle_ttl` and `max_partitions`
- **THEN** the generated impl sets `SHARD_KEY_OVERRIDDEN = true`, delegates `shard_key` to `coin_of`, leaves `shard_keys()` empty, and applies the `idle_ttl` / `max_partitions` overrides — producing a valid dynamic-mode shard per `relay-dynamic-partitioning`

#### Scenario: Static and dynamic opt-ins are mutually exclusive

- **WHEN** a `#[flezha::shard]` declares both `keys`/`partition_for` (static) and `shard_key` (dynamic)
- **THEN** the macro fails to compile with a message that the two modes are mutually exclusive (composite mode is not supported)

### Requirement: `#[derive(flezha::State)]` change derivation

The Rust SDK SHALL provide a `#[derive(flezha::State)]` derive that generates a `Change` enum and `apply_change` for field-granular mutations, journaled through the existing WAL.

#### Scenario: Field mutations are journaled

- **WHEN** a state struct derives `flezha::State` and a `Vec` field is pushed to inside `run`
- **THEN** a generated change variant records the mutation and it is applied via the generated `apply_change`

#### Scenario: Unsupported mutation reports an error

- **WHEN** a mutation cannot be expressed as a field-granular change
- **THEN** the SDK requires a hand-written `Change` and the derive does not silently produce incorrect journaling

### Requirement: `flezha::relay!` linear-pipeline macro

The Rust SDK SHALL provide a `flezha::relay!` declarative macro that generates a relay for a strictly linear pipeline and lowers to a normal `channels()` body.

#### Scenario: Linear pipeline removes the channels block

- **WHEN** `flezha::relay!{ id: "static_partition", pipeline: input::<String> -> StaticEntry -> StaticDownstream -> plugin::<Vec<u8>>("test_kafka") }` is written
- **THEN** a registered relay is generated wiring input → `StaticEntry` → `StaticDownstream` → the `test_kafka` plugin

#### Scenario: Non-linear graph is rejected

- **WHEN** a `relay!` pipeline expresses fan-out or multi-input
- **THEN** the macro fails to compile with a message directing the author to an explicit `channels()` body

### Requirement: Auto-registration without an explicit call

The Rust SDK SHALL auto-register relays produced by the single-shard auto-wire path and the `relay!` macro, while keeping manual `register_relay` valid.

#### Scenario: Auto-wired relay needs no register_relay

- **WHEN** a single-shard auto-wired relay is defined
- **THEN** it is discovered and registered at startup without a `register_relay` call

#### Scenario: Manual registration still works

- **WHEN** a hand-written relay calls `register_relay::<R>()`
- **THEN** it registers exactly as before

### Requirement: Sugared and raw shards compose

A relay SHALL be able to mix macro-generated shards and hand-written shards.

#### Scenario: Mixed relay builds and runs

- **WHEN** a relay wires a `#[flezha::shard]`-generated shard to a hand-written `impl Shard` shard
- **THEN** the relay compiles, registers, and processes input through both shards correctly
