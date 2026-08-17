# sdk-authoring-contract Specification

## Purpose

The sdk-authoring-contract capability is the single, language-agnostic source of truth for SDK authoring sugar. It defines the canonical mapping from a sugared procedure/operative declaration to the existing `StreamOperative`/`Procedure`/`Change`/`Channel` trait (or interface) surface: signature → associated types, mutable-state parameter → `State`, `State` → `Change` derivation with an explicit hand-written boundary, explicit entity-mode opt-ins (single/static/dynamic), single-procedure input-to-sink auto-wiring, and auto-registration. All sugar is pure expansion — it introduces no new runtime path, protocol message, or WAL change, and sugared procedures compose with hand-written ones. The contract binds Rust, Java, and the forward-looking Python and Elixir SDKs so the same boilerplate collapses identically across every binding.
## Requirements

### Requirement: Sugar expands to existing trait surface
The authoring sugar in every SDK SHALL be pure expansion over the existing `StreamOperative`/`Procedure`/`Change`/`Channel` trait (or interface) surface. The generated code MUST be equivalent to a valid hand-written implementation, and MUST NOT introduce any new runtime path, protocol message, or WAL change.

#### Scenario: Generated code equals hand-written impl

- **WHEN** a sugared procedure is expanded by any SDK's macro or processor
- **THEN** the result is a `Procedure`/`StreamOperative` implementation that compiles and runs identically to the equivalent hand-written one
- **AND** no protobuf, controller, or WAL-format change is required for it to run

#### Scenario: Sugared and raw procedures interoperate

- **WHEN** a stream operative contains one sugared procedure and one hand-written procedure wired together
- **THEN** the operative builds and runs correctly with both procedures participating

#### Scenario: Eject to full impl is a paste

- **WHEN** a user replaces a sugared procedure with the code its sugar expands to
- **THEN** the operative behaves identically with no other change required

### Requirement: Canonical signature-to-types inference
The contract SHALL define one canonical mapping from a sugared `run` declaration to the procedure's associated types, and every SDK MUST implement it identically.

#### Scenario: Input and Output inferred from signature

- **WHEN** a `run` declaration takes a first value parameter of type `I` and returns `O` (or `Result<O, _>`)
- **THEN** the procedure's `Input` is `I` and `Output` is `O`

#### Scenario: Unit return means no output

- **WHEN** a `run` declaration returns no value (unit)
- **THEN** the procedure declares no output channel

#### Scenario: Defaults for unspecified types

- **WHEN** a sugared procedure does not specify clock or config
- **THEN** `Clock` defaults to the wall clock and `Config` defaults to the unit type

### Requirement: State parameter drives stateful inference
A mutable-state parameter on the `run` declaration SHALL be the single signal that a procedure is stateful, and its type SHALL become the procedure `State`.

#### Scenario: Mutable state parameter sets State type

- **WHEN** a `run` declaration includes a mutable reference parameter of type `S`
- **THEN** the procedure's `State` is `S` and the procedure is treated as stateful

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

### Requirement: Entity mode is an explicit opt-in, never inferred
The contract SHALL express a procedure's entity mode (single, static, dynamic) through explicit declarations that map 1:1 to the existing runtime surface, and SHALL NOT infer the mode from the `run` body. Static and dynamic declarations are mutually exclusive (composite mode is out of scope).

The declarations are named for the Entity, not the Procedure. Static mode is declared **on the procedure**: the keyset is `entity_keys`, its routing is `entities_for`. Dynamic mode is declared **on the wiring**: the key function is `key_by` / `keyBy` on the consumer edge, and the eviction window remains `entity_idle_ttl` on the procedure. A procedure declaring neither a static keyset nor being reached by a keyed edge is single-mode.

#### Scenario: Dynamic keying is declared on the wiring

- **WHEN** an operative wires a procedure through an edge declaring a key function, and the procedure declares optional idle-TTL / max-entities
- **THEN** the SDK SHALL produce a dynamic-mode procedure against the existing runtime surface (lazily created Entities, empty static key list) with no new runtime path and no keying member on the procedure itself

#### Scenario: Static and dynamic declarations together are rejected

- **WHEN** a procedure declares a static keyset and is also wired through an edge declaring a key function
- **THEN** the SDK SHALL report a build-time error that the modes are mutually exclusive, naming the procedure

#### Scenario: Static routing may name several entities

- **WHEN** a static-mode procedure's `entities_for` declaration returns more than one key for one input
- **THEN** the input MUST be routed to the Entity for each returned key

#### Scenario: The authoring sugar offers no dynamic key hook

- **WHEN** the attribute/annotation sugar is generated for a procedure
- **THEN** it SHALL offer no `entity_key`-style hook, and declaring one SHALL fail at compile time (Rust) or annotation processing (Java) with an error directing the author to `key_by` on the wiring

### Requirement: Single-procedure auto-wiring and registration
The contract SHALL define exactly one topology that may be auto-wired — a single procedure from a controller input to one sink — and SHALL define auto-registration so the user writes no explicit registration call for that case.

#### Scenario: Single-procedure input-to-sink is auto-wired

- **WHEN** a sugared procedure declares an `id` and a `sink` and is the sole procedure
- **THEN** the SDK generates an operative wiring controller input → procedure → named sink and registers it automatically

#### Scenario: Multi-procedure topology stays explicit

- **WHEN** an operative has more than one procedure, or any fan-out or multi-input edge
- **THEN** the SDK requires explicit topology declaration and does not auto-wire

#### Scenario: Duplicate factory id fails fast

- **WHEN** auto-registration and manual registration would register two operatives with the same factory id
- **THEN** startup fails with an error naming the colliding id

### Requirement: Contract binds all current and future SDKs
The contract SHALL be the single source of truth for authoring sugar across Rust, Java, and future Python and Elixir SDKs, including non-normative target sketches for the not-yet-built bindings. Every SDK's stream and workflow authoring surfaces SHALL be rooted in the shared `Computation` base (see below).

#### Scenario: Reference operative expands identically across SDKs

- **WHEN** the same reference stream operative is authored with each SDK's sugar
- **THEN** each expansion produces a structurally identical topology and associated-type mapping per this contract

#### Scenario: Future bindings have a documented target

- **WHEN** the Python or Elixir SDK is later built
- **THEN** it follows the contract's documented decorator/macro sketch and inference rules without changing the contract

### Requirement: Keyed and collection state journaling
The authoring contract SHALL define journaling mutators for per-key map state so a stateful procedure whose state is a `HashMap`-of-values or `HashMap`-of-collections can be authored without a hand-written `Change` or `apply_change`. Every SDK realization SHALL generate, from a journal-annotated map field, named mutators whose calls are the journaled changes, and a generated change-application that replays them.

The defined modes are:

- **map-set** — `HashMap<K, V>`: a `set` mutator per field; the change records `(key, value)`; application inserts the value at the key.
- **map-vec** — `HashMap<K, Vec<U>>`: `push`, `drain-front(n)`, and `replace-last` mutators; the change records the procedure and its argument(s); application performs it on the keyed vector (creating it if absent).
- **map-deque** — `HashMap<K, Deque<U>>`: `push-back` and `pop-front(n)` mutators; application performs them on the keyed deque.

#### Scenario: Map-of-collection field generates per-key mutators

- **WHEN** a state type declares a `HashMap<String, Vec<f64>>` field annotated for map-vec journaling
- **THEN** the SDK generates `push`, `drain-front`, and `replace-last` mutators keyed by the map key, plus the matching change variants and the change-application that replays them

#### Scenario: Map-set is the universal fallback

- **WHEN** a procedure computes a new per-key value and calls the generated `set` mutator
- **THEN** the change records the per-key value, and replay re-applies it by insertion — with no hand-written `Change` or `apply_change`

#### Scenario: Inexpressible mutation reports an error

- **WHEN** a map field's per-entry mutation cannot be expressed by any defined mode (or the field type does not match the declared mode)
- **THEN** the SDK reports a clear authoring error directing the author to a hand-written `Change`, and SHALL NOT generate silent or incorrect journaling

### Requirement: Emit-or-skip output return
The authoring contract SHALL define an optional-output return shape for the logic function: a `run` returning an optional `Output` emits when present and emits nothing when absent. The unit return (no output channel) and the unconditional `Output` return (always emit) are unchanged.

#### Scenario: Optional output emits on present, skips on absent

- **WHEN** a `@Procedure`/`#[flezha::procedure]` `run` returns an optional `Output`
- **THEN** the generated procedure emits the value when present and emits nothing when absent, without the author touching the procedure context

### Requirement: Single hand-written path determinism
The authoring contract SHALL state that a procedure mutating state only through generated journaling mutators has exactly one hand-written path (`run`) and one generated replay path (the generated change-application); the two cannot diverge because the replay path is generated from the same mutator definitions. The replay-determinism conformance test remains the gate for all procedures (sugared and hand-written).

#### Scenario: Sugared procedure cannot drift between live and replay

- **WHEN** a procedure mutates state solely through generated journaling mutators
- **THEN** its replay path is generated (not hand-written), so a live-vs-replay mutation divergence is unrepresentable, and the replay-determinism harness passes

#### Scenario: Replay-safety guards are unnecessary for idempotent set journaling

- **WHEN** a procedure that previously hand-wrote an idempotency guard for replay safety migrates to map-set journaling
- **THEN** the guard is unnecessary because replay re-assigns the computed value rather than re-folding an input, and removing it does not change replay equality

### Requirement: Per-entity state init opt-in
The procedure authoring sugar SHALL offer an opt-in that maps an entity's key to its initial state, replacing the default `init_state` that returns the state's default value. The opt-in names a function of the entity key returning the state (`fn(Option<&str>) -> State` in Rust; an `@InitState`-annotated `State name(Optional<String> entityKey)` method in Java). The generated `init_state` SHALL delegate to it, passing the entity key — absent in single-entity mode and in the test-harness default construction. The opt-in is pure expansion onto the existing `Procedure::init_state` trait method; it SHALL NOT receive the restore payload, which the runtime continues to apply after `init_state` returns. The opt-in SHALL require a stateful procedure; declaring it on a stateless procedure SHALL be a compile-time error (never silently ignored).

#### Scenario: Entity key maps to per-entity state

- **WHEN** a static-mode sugared procedure declares the init opt-in and an Entity is created for a given entity key
- **THEN** the generated `init_state` returns the state produced by the opt-in function for that key, rather than the state default

#### Scenario: Key absent in single-entity and harness-default construction

- **WHEN** the init opt-in function is invoked in single-entity mode or by the test harness's default construction
- **THEN** it receives an absent entity key and returns its key-absent state

#### Scenario: Init opt-in on a stateless procedure is rejected

- **WHEN** the init opt-in is declared on a procedure with no state
- **THEN** the SDK reports a compile-time error directing the author to remove it, and generates no init delegation

### Requirement: Stream and workflow share a `Computation` authoring base
Both authoring surfaces SHALL be rooted in a shared `Computation` base — a Rust trait / Java interface that `StreamOperative` and `WorkflowOperative` both implement at the **authoring-trait layer**. The base's shared member SHALL be `factory_id()` (the one method both authoring traits already share); the kind-divergent surface (`channels` / persistence opt-in vs `run`) SHALL remain on the per-kind traits. The declaration siblings (`defineStream`/`defineWorkflow` in Java, `define_stream`/`define_workflow` in Rust) and their removal inverses (`removeStream`/`removeWorkflow`, `remove_stream`/`remove_workflow`) SHALL be methods on the concrete `FlezhaRunner` handle — not free functions, static methods, or methods on a builder type (there is no builder) — and SHALL be generic over `Computation`. The base SHALL NOT require unifying the sealed operative *instance* enum, which stays asymmetric. Adding a future operative kind SHALL require implementing `Computation` and adding a corresponding `define_*`/`defineX` (and `remove_*`/`removeX`) runner method, without editing the existing two authoring surfaces.

#### Scenario: Both kinds implement the base

- **WHEN** a stream operative class and a workflow operative class are declared on the same `FlezhaRunner`
- **THEN** both SHALL be `Computation` implementations resolvable through their respective `define_*`/`defineX` runner method, sharing `factory_id()`

#### Scenario: A future kind plugs into the base without touching existing kinds

- **WHEN** a new operative kind is added by implementing `Computation` and adding a new `define_*`/`defineX` runner method for it
- **THEN** the `StreamOperative` and `WorkflowOperative` authoring surfaces SHALL be unchanged

#### Scenario: The instance enum is not forced symmetric

- **WHEN** the sealed operative instance enum is renamed
- **THEN** its variants MAY remain asymmetric (a rich stream instance vs an id-only workflow instance), and the `Computation` base SHALL NOT require inventing a common instance type

### Requirement: Each SDK SHALL name the state-mutation enforcement level it actually provides
The authoring contract SHALL record, per SDK, the state-mutation enforcement level that SDK actually provides, so that an asymmetry between SDKs is a deliberate documented choice rather than an accident of each language's ergonomics.

The contract is written once and implemented per language, but the languages do not offer equal enforcement. Rust forecloses direct State mutation at compile time by withholding `DerefMut`. Java cannot — its handle must return a readable object — and today provides only convention plus opt-in test-time detection, while documenting the Rust guarantee. Python is closing the same hole by rejecting direct attribute assignment. Three different enforcement levels honestly stated is correct; three identical claims of which one is true is not.

#### Scenario: The contract records a level per SDK

- **WHEN** the authoring contract describes how a State mutation outside the declared Change path is prevented
- **THEN** it SHALL name the enforcement level for each SDK separately
- **AND** SHALL NOT state a single guarantee that only some SDKs meet

#### Scenario: A weaker level names its available detection

- **GIVEN** an SDK whose enforcement is weaker than compile-time refusal
- **WHEN** the contract records that SDK's level
- **THEN** it SHALL name the detection available in place of enforcement, and whether that detection is opt-in

#### Scenario: A new SDK must declare its level

- **WHEN** a new SDK is added to the contract
- **THEN** it SHALL declare its state-mutation enforcement level rather than inheriting the strongest one by default

### Requirement: Sugared run may declare a context parameter for multi-emit

The authoring sugar (`#[procedure]` in Rust, `@Procedure` in Java) SHALL accept a `run` signature with a trailing procedure-context parameter and a unit/void return: `fn run(input: I, state: …, ctx: &mut ProcedureCtx)` / `void run(I input, StateHandle<S, C> state, ProcedureCtx ctx)` (the state parameter remains optional, driving stateful inference exactly as today; a `Result<(), _>` return is a unit return for this purpose). The generated implementation SHALL pass the invocation's context through, so the body performs any number of `emit` / `emit_keyed` calls itself. Inference of Input, State, and Change from the remaining signature SHALL be unchanged, and the expansion SHALL remain the same code a hand-written implementation would contain.

Because the body owns every emit, the return type no longer declares the output type, and an `emits = Type` (Rust) / `emits = Type.class` (Java) attribute SHALL declare it instead; absent, the procedure declares no output channel. Three conflicts SHALL each be rejected with an error naming them: `key = …` together with a context parameter (the body keys its own emits), a non-unit return together with a context parameter (one procedure, one emit path), and `emits` without a context parameter (there the return type already answers).

#### Scenario: Multi-emit under sugar

- **WHEN** a sugared procedure declares `run(input, state, ctx)` returning unit and its body calls `ctx.emit_keyed` three times for one input
- **THEN** three keyed values SHALL be emitted for that invocation, identically to a hand-written implementation with the same body

#### Scenario: key attribute is rejected on the context shape

- **WHEN** a sugared procedure declares both a context parameter and a `key = …` attribute
- **THEN** macro expansion / annotation processing SHALL fail with an error naming the conflict

#### Scenario: the output type of a context-parameter run

- **WHEN** a sugared procedure declares a context parameter and `emits = Candle`
- **THEN** the generated implementation SHALL declare `Output = Candle`, so the wiring types that procedure's output channel exactly as a return-typed one would

#### Scenario: emits without a context parameter is rejected

- **WHEN** a sugared procedure declares `emits` but no context parameter
- **THEN** macro expansion / annotation processing SHALL fail with an error naming the conflict, since the return type already declares the output type

### Requirement: Hand-written implementations receive inferable defaults

Hand-written procedure implementations SHALL NOT be charged ceremony for information the declaration already states, in each SDK's idiom:

- **Java**: the five type-token methods (`inputType`, `outputType`, `changeType`, `clockType`, `configType`) SHALL have default implementations resolving the type arguments from the implementing class's generic supertype chain (direct `implements Procedure<…>` with concrete arguments, or a concrete subclass of a generic intermediate class). When a token cannot be resolved, registration SHALL fail with an error naming the class and the unresolved type parameter — never guess. `kryoTypes()` SHALL default to the resolved change and state classes when both are resolvable and the change type is not `NoChange` — a `NoChange` procedure persists nothing, so it keeps the empty-list default, which is also what keeps every existing declaration's meaning unchanged. An explicit override SHALL always win.
- **Rust**: a `SimpleProcedure` trait SHALL declare only `Input`, `Output`, `State` plus `name`, `init_state`, and `run`; a blanket implementation SHALL lift every `SimpleProcedure` to `Procedure` with `Clock = WallClock`, `Config = ()`, and the `State` type's derived change type. Implementing `Procedure` directly SHALL remain fully supported, and a type SHALL NOT implement both.

The mandatory authoring surface for the common hand-written case therefore reduces to name, state initialization, and `run` in both SDKs.

#### Scenario: Java tokens resolved from the implements clause

- **WHEN** a class declares `implements Procedure<Trade, EntityState, Candle, EntityStateChange, WallClock, Void>` and overrides none of the token methods
- **THEN** registration SHALL succeed with the six types resolved exactly as written in the clause

#### Scenario: Unresolvable token fails registration by name

- **WHEN** a class implements `Procedure` with a type argument still generic at the concrete class and does not override the corresponding token method
- **THEN** registration SHALL fail with an error naming the class and the unresolved type parameter, and nothing SHALL be registered

#### Scenario: Rust simplified tier defaults the tail types

- **WHEN** a type implements `SimpleProcedure` with `Input = Trade`, `Output = Candle`, `State = EntityState`
- **THEN** it SHALL be registrable wherever a `Procedure` is accepted, with `Clock = WallClock`, `Config = ()`, and the derived change type of `EntityState`

#### Scenario: Explicit override wins

- **WHEN** a Java implementation overrides `clockType()` (or a Rust implementation implements `Procedure` directly with a non-default `Clock`)
- **THEN** the explicit declaration SHALL be used, defaults notwithstanding
