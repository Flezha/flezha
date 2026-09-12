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
The contract SHALL define exactly one topology that may be auto-wired — a single procedure from a controller input to one sink — and SHALL define auto-registration so the user writes no explicit registration call for that case. The auto-wired entry channel SHALL take a name the author may supply, and SHALL be named `input` when the author supplies none, so the generated address is choosable without becoming mandatory.

An entry channel name supplied on a procedure that declares no `sink` SHALL be refused, naming both the supplied name and the missing `sink`, because no operative is generated for it to name a channel on; a spelling that cannot omit the sink owes no refusal.

#### Scenario: Single-procedure input-to-sink is auto-wired

- **WHEN** a sugared procedure declares an `id` and a `sink` and is the sole procedure
- **THEN** the SDK generates an operative wiring controller input → procedure → named sink and registers it automatically

#### Scenario: The auto-wired entry channel keeps its default name

- **WHEN** a sugared procedure declares an `id` and a `sink` and no entry channel name
- **THEN** the generated operative's entry channel SHALL be named `input`

#### Scenario: The author names the auto-wired entry channel

- **WHEN** a sugared procedure declares an `id`, a `sink` and the entry channel name `prices`
- **THEN** the generated operative's entry channel SHALL be named `prices`, and a frame whose metadata names `prices` SHALL reach it

#### Scenario: A name without a generated operative is refused

- **WHEN** a sugared procedure declares an entry channel name but no `sink`, in a spelling that can omit one
- **THEN** the SDK SHALL refuse at compile time, naming both the supplied name and the missing `sink`

#### Scenario: Multi-procedure topology stays explicit

- **WHEN** an operative has more than one procedure, or any fan-out or multi-input edge
- **THEN** the SDK requires explicit topology declaration and does not auto-wire

#### Scenario: Duplicate factory id fails fast

- **WHEN** auto-registration and manual registration would register two operatives with the same factory id
- **THEN** startup fails with an error naming the colliding id

### Requirement: Contract binds all current and future SDKs
The contract SHALL be the single source of truth for authoring sugar across the Rust, Java, Elixir, Node, Ruby, PHP and Python SDKs and any future binding, including non-normative target sketches for bindings not yet built. Every SDK's stream and workflow authoring surfaces SHALL be rooted in the shared `Operative` base (see below). Beyond the sugar, every surface that more than one SDK carries SHALL change in lockstep: a change to the authoring or declaration surface (builder, ctx, the `HostClassDecl` keys), to the wire protocol or its metadata keys, to a default (overflow policy, retry ladder, mailbox capacity, heartbeat keys), or to a fixture that proves one of these (the regression and round-trip bench factories, the end-to-end runtime matrix) SHALL land in every SDK in the same change, or SHALL name in its spec delta the SDK it defers and the follow-up change that owes it. An SDK that carries a capability another SDK neither carries nor names as deferred is a defect of the change, not a backlog item. A performance change SHALL state in its notes which layer it changed (controller, shared kernel, or host SDK); a measurement taken with one runtime SHALL be reported as that runtime's number, and a claim that holds for every SDK SHALL be backed by the same bench run on each runtime that carries the tier.

An SDK whose authoring surface is a set of factory functions, or a set of classes, rather than macros or annotations SHALL be bound by the same contract: the sketch names the mechanism, and the inference rules, the refusal set and the invocation context are the same.

#### Scenario: Reference operative expands identically across SDKs

- **WHEN** the same reference stream operative is authored with each SDK's sugar
- **THEN** each expansion produces a structurally identical topology and associated-type mapping per this contract

#### Scenario: Future bindings have a documented target

- **WHEN** a new SDK binding is later built
- **THEN** it follows the contract's documented decorator/macro sketch and inference rules without changing the contract

#### Scenario: A declaration key lands in every SDK or is deferred by name

- **WHEN** a change adds a key to `HostClassDecl` (for example a checkpoint option)
- **THEN** the same change adds its spelling to the Rust, Java, Node, Ruby, PHP and Python builders, and its spec delta records the Elixir SDK as either carrying it or deferring it to a named follow-up

#### Scenario: A performance claim is measured per runtime

- **WHEN** a change reports a throughput or latency result for a tier the Rust, Java, Elixir, Ruby, PHP and Python runners all carry
- **THEN** its notes report a number per runtime from the same bench protocol, and a runtime without a number is named as unmeasured rather than covered by the Rust number

#### Scenario: The Ruby surface is bound by the contract, not beside it

- **WHEN** the Ruby SDK's authoring surface is compared against the contract's inference rules
- **THEN** its class-based sugar SHALL expand to the same topology and associated-type mapping as the other SDKs', and a divergence SHALL be a defect of the Ruby SDK rather than a contract exception

#### Scenario: The PHP binding is rooted in the shared base

- **WHEN** a PHP stream or workflow operative is authored
- **THEN** its declaration SHALL expand to the same topology and entity-mode mapping the contract defines for Rust and Java, with no PHP-specific runtime path, protocol message, or WAL change

#### Scenario: An attribute-carried declaration is bound by the same rules

- **WHEN** the PHP SDK's `#[Proc]` attribute and override-detected hooks are compared against the contract's inference rules
- **THEN** they SHALL yield the same declaration a Rust macro or a Java annotation yields for the equivalent class, and a divergence SHALL be a defect of the PHP SDK rather than a contract exception

#### Scenario: The Python surface is bound by the contract, not beside it

- **WHEN** the Python SDK's authoring surface is compared against the contract's inference rules
- **THEN** its decorator-based sugar SHALL expand to the same topology and associated-type mapping as the other SDKs', and a divergence SHALL be a defect of the Python SDK rather than a contract exception

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

### Requirement: Each SDK SHALL name the state-mutation enforcement level it actually provides
The authoring contract SHALL record, per SDK, the state-mutation enforcement level that SDK actually provides, so that an asymmetry between SDKs is a deliberate documented choice rather than an accident of each language's ergonomics.

The contract is written once and implemented per language, but the languages do not offer equal enforcement. Rust forecloses direct State mutation at compile time by withholding `DerefMut`. Java cannot — its handle must return a readable object — and today provides only convention plus opt-in test-time detection, while documenting the Rust guarantee. Elixir's state is immutable, so an unrecorded mutation cannot exist. Node and Python reject direct assignment at runtime, naming the field and the change-based path. Different enforcement levels honestly stated is correct; identical claims of which only some are true is not.

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

#### Scenario: The Node SDK's level is runtime rejection

- **WHEN** the contract records the Node SDK's level
- **THEN** it SHALL state that assigning to a declared state value from a handler is rejected at runtime, naming the field and the change-based path, while the change fold and the restore retain their ability to mutate

#### Scenario: Python's level is recorded as supplied, not forthcoming

- **WHEN** the contract's per-SDK table is read after the Python SDK lands
- **THEN** Python's level SHALL read as runtime rejection of direct attribute assignment, with the reserved runtime path for applying changes and restoring state named as the exception

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

### Requirement: Every SDK binding passes the shared conformance suite

Every SDK binding SHALL ship a test app under `test/apps/<sdk>_runner/` with a conformance manifest (`sdk-conformance`), and SHALL pass the shared end-to-end suite under `FLEZHA_E2E_RUNTIME=<sdk>` for every capability its manifest does not defer. A capability listed under `deferred` SHALL match the SDK's documented scope matrix, and the documented matrix SHALL point at the manifest rather than restate it. The contract's reference-operative equivalence (§6 `static_entity`, §6.1 `rolling_window_twin`) SHALL be asserted against the controller-side golden declaration snapshot in the smoke tier, in addition to any SDK-local expansion test; an SDK whose expansion differs from the golden fails conformance, whatever its local test says.

#### Scenario: A new binding is admitted by the suite, not by review
- **WHEN** a new SDK's test app and manifest are added and `FLEZHA_E2E_RUNTIME=<sdk> mix test --include system` passes with every non-deferred capability green
- **THEN** the binding satisfies this contract for the capabilities it carries, and its deferred list is its scope matrix

#### Scenario: A binding cannot land half-wired
- **WHEN** a pull request adds a new SDK's test app without its manifest, or with a manifest that defers a required capability, omits a vocabulary capability, or is not dispatchable from the nightly
- **THEN** `mix test` fails in the conformance gate in that pull request, naming what is missing and the "Adding an SDK to the conformance suite" section

#### Scenario: A deferred list that drifts from the documented scope fails the gate
- **WHEN** an SDK's documentation names a capability as carried while its manifest lists it under `deferred`
- **THEN** the discrepancy is a defect of the change that introduced it; the manifest is the one place the scope is declared

### Requirement: The authoring contract documents schema evolution

The authoring contract SHALL document, in lockstep across every SDK that
persists state, that:

- `SCHEMA_VERSION` is bumped on any non-additive edit to the Procedure's `State`
  **or** its `Change` type — not to the `Change` type alone;
- the codecs are positional and not self-describing, so an unbumped edit that
  changes field order or field count is a silent corruption, not a compatibility
  question;
- `MIN_READABLE_SCHEMA_VERSION` plus the migration entry points are how an
  author reads what an earlier version wrote, and their default is to refuse.

The authoring test harness SHALL be able to restore a Procedure from bytes
stamped at an older schema version, so a migration is testable without a running
controller.

#### Scenario: a migration is proven at the authoring seam

- **GIVEN** a Procedure at `SCHEMA_VERSION = 2` with a declared migration from version 1
- **WHEN** the author restores a harness from a payload stamped version 1
- **THEN** the harness SHALL apply the migration and expose the migrated state for assertion

### Requirement: An SDK Whose Language Can Refuse a Wrong Edge at Compile Time SHALL Do So

Where an SDK's language can reject a mismatched channel wiring before the program runs, the SDK SHALL
implement that refusal and SHALL pin it with fixtures that are expected to fail compilation, so a
refusal that stops working fails the build. Registration-time refusals remain the backstop for
defects static typing cannot see.

This records as a contract property what the Rust SDK already does with compile-fail tests, so a
statically typed SDK cannot demote it to a runtime check by omission.

#### Scenario: The refusal is pinned as a test

- **WHEN** an SDK claims compile-time edge type checking
- **THEN** a fixture asserting the compilation failure SHALL exist in its test suite

#### Scenario: The pinned fixture cannot pass for the wrong reason

- **WHEN** a fixture asserts that a wiring does not compile
- **THEN** the suite SHALL also establish that the line fails for the declared reason rather than for any error at all

### Requirement: Every topology is judged once, by the reference check, from the reference fixture

Every SDK's builder SHALL submit the topology it was given, as a record of the authoring
calls (the channels, the ordered links of each chain — transform, key, sink, name,
consumer — the merges and branches, the class flags and the procedure facts), to one
reference check that holds every named refusal once: `flz_core::host::register::check`
for the Rust, Java, Node, Python, Ruby and PHP SDKs, and the Elixir SDK's re-expression of
the same check, written from the same fixture, since it inherits nothing from the core.
The refusal vocabulary SHALL be the `TopologyIssue` set — every name and sentence the
reference builder carries — and SHALL live in the core, with a builder carrying no rule of
its own beside it. `test/fixtures/reference_topologies.txt` SHALL be the contract: one row
per refusal naming the authoring steps that produce it and the surface it needs, and one
row per well-formed shape whose derived declaration the row states; an SDK passes this
requirement by one conformance run over every row, not by a set of tests of its own.

#### Scenario: A refusal absent from one SDK is impossible

- **WHEN** a topology that one SDK refuses by a reference name is authored in any other SDK
- **THEN** registration SHALL refuse it with the same name and sentence, because the
  verdict is the core's, or the Elixir SDK's re-expression proven by the same fixture row

#### Scenario: A new refusal is one variant and one row

- **WHEN** a refusal is added to the vocabulary
- **THEN** the change adds one variant, one fixture row and no builder code, and every
  SDK's conformance run covers the row with no test edit

#### Scenario: A surface an SDK does not carry is skipped by name

- **WHEN** a fixture row needs `merge` or `branch` and an SDK's documented subset carries neither
- **THEN** that SDK's run SHALL report the row as skipped for that surface, and the seam
  test SHALL count the skip against the documented subset rather than let the row drop

#### Scenario: The checkpoint flags imply the granularity in one place

- **WHEN** a class declares `checkpoint_ack`, `checkpoint_every`, `outputs_at_checkpoint`
  or `checkpoint_max_delay_ms` in any SDK
- **THEN** the record carries the flags as written and the reference check derives the
  acknowledgement granularity and judges its preconditions, with no per-SDK helper doing so

### Requirement: Stream and workflow share an `Operative` authoring base

Both authoring surfaces SHALL be rooted in a shared `Operative` base, a Rust trait or
Java interface that `StreamOperative` and `WorkflowOperative` both implement at the
**authoring-trait layer**. The base's shared member SHALL be `factory_id()`, the one
method both authoring traits already share; the kind-divergent surface (`channels` and
the persistence opt-in on one side, `run` on the other) SHALL remain on the per-kind
traits. The declaration siblings (`defineStream`/`defineWorkflow` in Java,
`define_stream`/`define_workflow` in Rust) and their removal inverses
(`removeStream`/`removeWorkflow`, `remove_stream`/`remove_workflow`) SHALL be methods
on the concrete `FlezhaRunner` handle, not free functions, static methods, or methods
on a builder type, and SHALL be generic over the shared base where the host language
expresses such a bound. The base SHALL NOT require unifying the sealed operative
*instance* enum, which stays asymmetric. Adding a future operative kind SHALL require
implementing the base and adding a corresponding `define_*`/`defineX` and
`remove_*`/`removeX` runner method, without editing the existing two authoring
surfaces.

Where a host language groups types into namespaces or modules, both operative kinds
and the shared base SHALL be grouped together rather than split across namespaces, so
that the grouping states the taxonomy the vocabulary already uses.

#### Scenario: Both kinds resolve through the shared base

- **WHEN** a stream operative and a workflow operative are each declared on a runner
- **THEN** both SHALL be `Operative` implementations resolvable through their respective
  `define_*`/`defineX` runner method, sharing `factory_id()`

#### Scenario: A new kind is added without editing the existing surfaces

- **WHEN** a new operative kind is added by implementing `Operative` and adding a new
  `define_*`/`defineX` runner method for it
- **THEN** neither existing authoring surface SHALL require modification

#### Scenario: The instance enum stays asymmetric

- **WHEN** the sealed operative instance representation is considered
- **THEN** its variants MAY remain asymmetric, a rich stream instance against an id-only
  workflow instance, and the `Operative` base SHALL NOT require inventing a common
  instance type

#### Scenario: Both kinds are grouped in one namespace

- **WHEN** a host language groups authoring types into namespaces, modules or packages
- **THEN** the shared base and both operative kinds SHALL be reachable from one such
  grouping, and neither kind SHALL be the only one placed outside it

