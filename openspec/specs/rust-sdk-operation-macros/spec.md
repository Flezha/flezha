# rust-sdk-operation-macros Specification

## Purpose

The rust-sdk-operation-macros capability is the Rust realization of the SDK authoring contract. It provides the `#[flezha::operation]` attribute macro (signature-to-types inference, `&mut` state inference, segment/mailbox/clock opt-ins, static and dynamic modes), the `#[derive(flezha::State)]` derive (field-granular `Change` enum + `apply_change` journaled through the existing WAL), and the `flezha::operative!` declarative macro for linear pipelines, plus inventory-based auto-registration. All macros expand to the existing `UnboundedOperative`/`Operation`/`Change`/`Channel` traits with zero engine, protocol, or WAL change; macro-generated and hand-written operations compose in the same operative.

## Requirements

### Requirement: `#[flezha::operation]` attribute macro

The Rust SDK SHALL provide a `#[flezha::operation]` attribute macro applied to a single `run` function that generates a complete `Operation` implementation conforming to the authoring contract.

#### Scenario: Stateless operation collapses to a function

- **WHEN** `#[flezha::operation(id = "do_nothing", sink = "echo")]` annotates `fn do_nothing(input: String) -> Vec<u8>`
- **THEN** the macro generates a `Default` operation type, an `Operation` impl with `Input = String`, `Output = Vec<u8>`, `State = ()`, `Change = NoChange`, `Clock = WallClock`, `Config = ()`, `name`, `init_state`, and a `run` wrapper that emits the returned value

#### Scenario: Stateful operation infers State from the mutable parameter

- **WHEN** `#[flezha::operation(id = "store_input", sink = "sink")]` annotates `fn store_input(input: String, state: &mut StoreInputState) -> String`
- **THEN** the generated `Operation` impl sets `State = StoreInputState` and routes mutations through the existing state-handle path

#### Scenario: Generated operation type is referenceable in channels

- **WHEN** an operation is generated from `fn static_entry(..)`
- **THEN** a PascalCase operation type (e.g. `StaticEntry`) is produced that can be named in an explicit `channels()` body via `.from`/`.into`

### Requirement: Attribute opt-ins for segment and mailbox behavior

The `#[flezha::operation]` macro SHALL accept attribute opt-ins that map to the corresponding `Operation` overrides without dropping to a raw impl.

#### Scenario: Static segment keys and segment_for

- **WHEN** `#[flezha::operation(id = "static_entry_operation", keys = ["a","b","c","d"], segment_for = part)]` is used with a free function `part`
- **THEN** the generated impl sets `operation_keys()` to the listed keys, marks segmenting overridden, and delegates `segment_for` to `part`

#### Scenario: Mailbox capacity and overflow policy

- **WHEN** `capacity` and `on_full` attributes are supplied
- **THEN** the generated impl overrides `mailbox_capacity()` and the channel's overflow policy accordingly

#### Scenario: Clock override

- **WHEN** a `clock` attribute names a supported clock
- **THEN** the generated `Operation` uses that clock instead of `WallClock`

#### Scenario: Dynamic-mode opt-in

- **WHEN** `#[flezha::operation(id = "rsi", operation_key = coin_of)]` is used with a free function `coin_of(&Trade) -> Option<String>`, optionally with `idle_ttl` and `max_segments`
- **THEN** the generated impl sets `OPERATION_KEY_OVERRIDDEN = true`, delegates `operation_key` to `coin_of`, leaves `operation_keys()` empty, and applies the `idle_ttl` / `max_segments` overrides — producing a valid dynamic-mode operation per `operative-dynamic-segmenting`

#### Scenario: Static and dynamic opt-ins are mutually exclusive

- **WHEN** a `#[flezha::operation]` declares both `keys`/`segment_for` (static) and `operation_key` (dynamic)
- **THEN** the macro fails to compile with a message that the two modes are mutually exclusive (composite mode is not supported)

### Requirement: `#[derive(flezha::State)]` change derivation

The Rust SDK SHALL provide a `#[derive(flezha::State)]` derive that generates a `Change` enum and `apply_change` for field-granular mutations, journaled through the existing WAL.

#### Scenario: Field mutations are journaled

- **WHEN** a state struct derives `flezha::State` and a `Vec` field is pushed to inside `run`
- **THEN** a generated change variant records the mutation and it is applied via the generated `apply_change`

#### Scenario: Unsupported mutation reports an error

- **WHEN** a mutation cannot be expressed as a field-granular change
- **THEN** the SDK requires a hand-written `Change` and the derive does not silently produce incorrect journaling

### Requirement: `flezha::operative!` linear-pipeline macro

The Rust SDK SHALL provide a `flezha::operative!` declarative macro that generates an operative for a strictly linear pipeline and lowers to a normal `channels()` body.

#### Scenario: Linear pipeline removes the channels block

- **WHEN** `flezha::operative!{ id: "static_segment", pipeline: input::<String> -> StaticEntry -> StaticDownstream -> plugin::<Vec<u8>>("test_kafka") }` is written
- **THEN** a registered operative is generated wiring input → `StaticEntry` → `StaticDownstream` → the `test_kafka` plugin

#### Scenario: Non-linear graph is rejected

- **WHEN** an `operative!` pipeline expresses fan-out or multi-input
- **THEN** the macro fails to compile with a message directing the author to an explicit `channels()` body

### Requirement: Auto-registration without an explicit call

The Rust SDK SHALL auto-register operatives produced by the single-operation auto-wire path and the `operative!` macro, while keeping manual `register_unbounded` valid.

#### Scenario: Auto-wired operative needs no register_unbounded

- **WHEN** a single-operation auto-wired operative is defined
- **THEN** it is discovered and registered at startup without a `register_unbounded` call

#### Scenario: Manual registration still works

- **WHEN** a hand-written operative calls `register_unbounded::<R>()`
- **THEN** it registers exactly as before

### Requirement: Sugared and raw operations compose

An operative SHALL be able to mix macro-generated operations and hand-written operations.

#### Scenario: Mixed operative builds and runs

- **WHEN** an operative wires a `#[flezha::operation]`-generated operation to a hand-written `impl Operation` operation
- **THEN** the operative compiles, registers, and processes input through both operations correctly

### Requirement: Map-aware `#[derive(flezha::State)]` journaling mutators

`#[derive(flezha::State)]` SHALL generate journaling mutators, `Change` variants, and `apply_change` arms for map fields annotated `#[journal(map_set | map_vec | map_deque)]`, in addition to the existing flat-field derivation. The generated mutators perform the mutation live and record it as a `Change`; the generated `apply_change` replays it. Flat-field and map fields on the same struct SHALL compose into a single `Change` enum.

- `map_set` on `HashMap<K, V>` → `set_<field>(key, V)`, variant `Set<Field>(K, V)`.
- `map_vec` on `HashMap<K, Vec<U>>` → `push_<field>(key, U)`, `drain_<field>_front(key, usize)`, `replace_last_<field>(key, U)`.
- `map_deque` on `HashMap<K, VecDeque<U>>` → `push_back_<field>(key, U)`, `pop_front_<field>(key, usize)`.

#### Scenario: map_vec field generates collection mutators

- **WHEN** a `#[derive(flezha::State)]` struct has a `#[journal(map_vec)] closes: HashMap<String, Vec<f64>>` field
- **THEN** the derive generates `push_closes`, `drain_closes_front`, and `replace_last_closes` mutators, the matching `Change` variants, and an `apply_change` that replays each on the keyed vector

#### Scenario: map_set field generates a keyed setter

- **WHEN** a `#[derive(flezha::State)]` struct has a `#[journal(map_set)] by_product: HashMap<String, ProductState>` field
- **THEN** the derive generates `set_by_product(key, ProductState)`, a `SetByProduct(String, ProductState)` variant, and an `apply_change` arm that inserts the value

#### Scenario: Mixed flat and map fields compose

- **WHEN** a struct mixes a flat field and a `#[journal(...)]` map field
- **THEN** both contribute variants to a single generated `Change` enum and arms to a single `apply_change`

#### Scenario: Unannotated or mismatched map field errors

- **WHEN** a map field lacks a `#[journal(...)]` annotation, or its type does not match the declared mode
- **THEN** the derive emits a `compile_error!` directing the author to a hand-written `Change`, and generates no journaling for that field

### Requirement: `Option<Output>` return in `#[flezha::operation]`

`#[flezha::operation]` SHALL accept a `run` function returning `Option<Output>` (or `Result<Option<Output>, OperationError>`), emitting on `Some` and emitting nothing on `None`. The existing unit return (no output) and unconditional `Output` return (always emit) SHALL be unchanged.

#### Scenario: Optional return drives conditional emit

- **WHEN** a `#[flezha::operation]` `run` returns `Option<Output>`
- **THEN** the generated `run` emits via the operation context on `Some` and emits nothing on `None`, without the author referencing `OperationCtx`

#### Scenario: Generated behaviour equals hand-written

- **WHEN** an operation authored with map-aware `#[derive(flezha::State)]` + `#[flezha::operation]` returning `Option<Output>` is expanded
- **THEN** its behaviour is identical to the hand-written `Operation`/`Change` impl, and `OperationTestHarness::replay_equals` passes

### Requirement: `init` opt-in on `#[flezha::operation]`

`#[flezha::operation]` SHALL accept an `init = fn` argument naming a function `fn(Option<&str>) -> State`. When present, the generated `Operation::init_state` SHALL delegate to it, passing the operation key. When absent, `init_state` SHALL keep returning `State::default()` (stateful) or unit (stateless), unchanged. Declaring `init` on a stateless operation (no `&mut State` parameter) SHALL be a `compile_error!`. The restore payload SHALL NOT be passed to the init function.

#### Scenario: init delegates init_state to the named function

- **WHEN** a `#[flezha::operation(init = my_init)]` stateful operation is expanded
- **THEN** its `init_state(operation_key, _)` returns `my_init(operation_key)`, so each static segment starts from key-derived state, and `OperationTestHarness::replay_equals` passes for the operation

#### Scenario: init on a stateless operation fails to compile

- **WHEN** `#[flezha::operation(init = …)]` is applied to a `run` with no `&mut State` parameter
- **THEN** expansion emits a `compile_error!` stating `init` requires a state parameter
