# sdk-authoring-contract Specification

## Purpose

The sdk-authoring-contract capability is the single, language-agnostic source of truth for SDK authoring sugar. It defines the canonical mapping from a sugared shard/relay declaration to the existing `Relay`/`Shard`/`Change`/`Channel` trait (or interface) surface: signature → associated types, mutable-state parameter → `State`, `State` → `Change` derivation with an explicit hand-written boundary, explicit partitioning-mode opt-ins (single/static/dynamic), single-shard input-to-sink auto-wiring, and auto-registration. All sugar is pure expansion — it introduces no new runtime path, protocol message, or WAL change, and sugared shards compose with hand-written ones. The contract binds Rust, Java, and the forward-looking Python and Elixir SDKs so the same boilerplate collapses identically across every binding.

## Requirements

### Requirement: Sugar expands to existing trait surface

The authoring sugar in every SDK SHALL be pure expansion over the existing `Relay`/`Shard`/`Change`/`Channel` trait (or interface) surface. The generated code MUST be equivalent to a valid hand-written implementation, and MUST NOT introduce any new runtime path, protocol message, or WAL change.

#### Scenario: Generated code equals hand-written impl

- **WHEN** a sugared shard is expanded by any SDK's macro or processor
- **THEN** the result is a `Shard`/`Relay` implementation that compiles and runs identically to the equivalent hand-written one
- **AND** no protobuf, controller, or WAL-format change is required for it to run

#### Scenario: Sugared and raw shards interoperate

- **WHEN** a relay contains one sugared shard and one hand-written shard wired together
- **THEN** the relay builds and runs correctly with both shards participating

#### Scenario: Eject to full impl is a paste

- **WHEN** a user replaces a sugared shard with the code its sugar expands to
- **THEN** the relay behaves identically with no other change required

### Requirement: Canonical signature-to-types inference

The contract SHALL define one canonical mapping from a sugared `run` declaration to the shard's associated types, and every SDK MUST implement it identically.

#### Scenario: Input and Output inferred from signature

- **WHEN** a `run` declaration takes a first value parameter of type `I` and returns `O` (or `Result<O, _>`)
- **THEN** the shard's `Input` is `I` and `Output` is `O`

#### Scenario: Unit return means no output

- **WHEN** a `run` declaration returns no value (unit)
- **THEN** the shard declares no output channel

#### Scenario: Defaults for unspecified types

- **WHEN** a sugared shard does not specify clock or config
- **THEN** `Clock` defaults to the wall clock and `Config` defaults to the unit type

### Requirement: State parameter drives stateful inference

A mutable-state parameter on the `run` declaration SHALL be the single signal that a shard is stateful, and its type SHALL become the shard `State`.

#### Scenario: Mutable state parameter sets State type

- **WHEN** a `run` declaration includes a mutable reference parameter of type `S`
- **THEN** the shard's `State` is `S` and the shard is treated as stateful

#### Scenario: No state parameter means stateless

- **WHEN** a `run` declaration has no mutable-state parameter
- **THEN** `State` is the unit type and `Change` is the no-op change type

### Requirement: State-to-Change derivation with explicit boundary

The contract SHALL define how a state type's field mutations derive a `Change` type and `apply_change`, and SHALL define the explicit boundary beyond which a hand-written `Change` is required.

#### Scenario: Field mutation auto-derives a change

- **WHEN** a state type opts into change derivation and a field is appended-to or set
- **THEN** a corresponding change variant is generated and the mutation is journaled to the existing WAL

#### Scenario: Beyond the boundary requires manual Change

- **WHEN** a mutation is conditional, spans multiple fields atomically, or is computed
- **THEN** the SDK requires a hand-written `Change` and reports a clear error rather than journaling incorrectly

### Requirement: Partitioning mode is an explicit opt-in, never inferred

The contract SHALL express a shard's partitioning mode (single, static, dynamic) through explicit attribute/annotation opt-ins that map 1:1 to the existing trait surface, and SHALL NOT infer the mode from the `run` body. Static and dynamic opt-ins are mutually exclusive (composite mode is out of scope).

#### Scenario: Dynamic opt-in maps to the existing trait surface

- **WHEN** a shard declares a dynamic key opt-in (a `shard_key`-style hook plus optional idle-TTL / max-partitions)
- **THEN** the SDK generates a dynamic-mode shard against the existing trait surface (dynamic-key override set, static key list empty) with no new runtime path

#### Scenario: Static and dynamic opt-ins together are rejected

- **WHEN** a shard declares both a static key opt-in and a dynamic key opt-in
- **THEN** the SDK reports a compile-time error that the modes are mutually exclusive

### Requirement: Single-shard auto-wiring and registration

The contract SHALL define exactly one topology that may be auto-wired — a single shard from a controller input to one sink — and SHALL define auto-registration so the user writes no explicit registration call for that case.

#### Scenario: Single-shard input-to-sink is auto-wired

- **WHEN** a sugared shard declares an `id` and a `sink` and is the sole shard
- **THEN** the SDK generates a relay wiring controller input → shard → named sink and registers it automatically

#### Scenario: Multi-shard topology stays explicit

- **WHEN** a relay has more than one shard, or any fan-out or multi-input edge
- **THEN** the SDK requires explicit topology declaration and does not auto-wire

#### Scenario: Duplicate factory id fails fast

- **WHEN** auto-registration and manual registration would register two relays with the same factory id
- **THEN** startup fails with an error naming the colliding id

### Requirement: Contract binds all current and future SDKs

The contract SHALL be the single source of truth for authoring sugar across Rust, Java, and future Python and Elixir SDKs, including non-normative target sketches for the not-yet-built bindings.

#### Scenario: Reference relay expands identically across SDKs

- **WHEN** the same reference relay is authored with each SDK's sugar
- **THEN** each expansion produces a structurally identical topology and associated-type mapping per this contract

#### Scenario: Future bindings have a documented target

- **WHEN** the Python or Elixir SDK is later built
- **THEN** it follows the contract's documented decorator/macro sketch and inference rules without changing the contract

### Requirement: Keyed and collection state journaling

The authoring contract SHALL define journaling mutators for per-key map state so a stateful shard whose state is a `HashMap`-of-values or `HashMap`-of-collections can be authored without a hand-written `Change` or `apply_change`. Every SDK realization SHALL generate, from a journal-annotated map field, named mutators whose calls are the journaled changes, and a generated change-application that replays them.

The defined modes are:

- **map-set** — `HashMap<K, V>`: a `set` mutator per field; the change records `(key, value)`; application inserts the value at the key.
- **map-vec** — `HashMap<K, Vec<U>>`: `push`, `drain-front(n)`, and `replace-last` mutators; the change records the operation and its argument(s); application performs it on the keyed vector (creating it if absent).
- **map-deque** — `HashMap<K, Deque<U>>`: `push-back` and `pop-front(n)` mutators; application performs them on the keyed deque.

#### Scenario: Map-of-collection field generates per-key mutators

- **WHEN** a state type declares a `HashMap<String, Vec<f64>>` field annotated for map-vec journaling
- **THEN** the SDK generates `push`, `drain-front`, and `replace-last` mutators keyed by the map key, plus the matching change variants and the change-application that replays them

#### Scenario: Map-set is the universal fallback

- **WHEN** a shard computes a new per-key value and calls the generated `set` mutator
- **THEN** the change records the per-key value, and replay re-applies it by insertion — with no hand-written `Change` or `apply_change`

#### Scenario: Inexpressible mutation reports an error

- **WHEN** a map field's per-entry mutation cannot be expressed by any defined mode (or the field type does not match the declared mode)
- **THEN** the SDK reports a clear authoring error directing the author to a hand-written `Change`, and SHALL NOT generate silent or incorrect journaling

### Requirement: Emit-or-skip output return

The authoring contract SHALL define an optional-output return shape for the logic function: a `run` returning an optional `Output` emits when present and emits nothing when absent. The unit return (no output channel) and the unconditional `Output` return (always emit) are unchanged.

#### Scenario: Optional output emits on present, skips on absent

- **WHEN** a `@Shard`/`#[flezha::shard]` `run` returns an optional `Output`
- **THEN** the generated shard emits the value when present and emits nothing when absent, without the author touching the shard context

### Requirement: Single hand-written path determinism

The authoring contract SHALL state that a shard mutating state only through generated journaling mutators has exactly one hand-written path (`run`) and one generated replay path (the generated change-application); the two cannot diverge because the replay path is generated from the same mutator definitions. The replay-determinism conformance test remains the gate for all shards (sugared and hand-written).

#### Scenario: Sugared shard cannot drift between live and replay

- **WHEN** a shard mutates state solely through generated journaling mutators
- **THEN** its replay path is generated (not hand-written), so a live-vs-replay mutation divergence is unrepresentable, and the replay-determinism harness passes

#### Scenario: Replay-safety guards are unnecessary for idempotent set journaling

- **WHEN** a shard that previously hand-wrote an idempotency guard for replay safety migrates to map-set journaling
- **THEN** the guard is unnecessary because replay re-assigns the computed value rather than re-folding an input, and removing it does not change replay equality
