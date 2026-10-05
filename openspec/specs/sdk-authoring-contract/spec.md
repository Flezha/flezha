# sdk-authoring-contract Specification

## Purpose

The sdk-authoring-contract capability is the single, language-agnostic source of truth for SDK authoring sugar. It defines the canonical mapping from a sugared tasklet/relay declaration to the existing `Relay`/`Tasklet`/`Change`/`Channel` trait (or interface) surface: signature → associated types, mutable-state parameter → `State`, `State` → `Change` derivation with an explicit hand-written boundary, explicit cell-mode opt-ins (single/static/dynamic), single-tasklet input-to-output auto-wiring, and auto-registration. All sugar is pure expansion — it introduces no new runtime path, protocol message, or WAL change, and sugared tasklets compose with hand-written ones. The contract binds Rust, Java, and the forward-looking Python and Elixir SDKs so the same boilerplate collapses identically across every binding.
## Requirements
### Requirement: Sugar expands to existing trait surface
The authoring sugar in every SDK SHALL be pure expansion over the existing `Relay`/`Tasklet`/`Change`/`Channel` trait (or interface) surface. The generated code MUST be equivalent to a valid hand-written implementation, and MUST NOT introduce any new runtime path, protocol message, or WAL change.

#### Scenario: Generated code equals hand-written impl

- **WHEN** a sugared tasklet is expanded by any SDK's macro or processor
- **THEN** the result is a `Tasklet`/`Relay` implementation that compiles and runs identically to the equivalent hand-written one
- **AND** no protobuf, controller, or WAL-format change is required for it to run

#### Scenario: Sugared and raw tasklets interoperate

- **WHEN** a flow relay contains one sugared tasklet and one hand-written tasklet wired together
- **THEN** the relay builds and runs correctly with both tasklets participating

#### Scenario: Eject to full impl is a paste

- **WHEN** a user replaces a sugared tasklet with the code its sugar expands to
- **THEN** the relay behaves identically with no other change required

### Requirement: Canonical signature-to-types inference
The contract SHALL define one canonical mapping from a tasklet declared as a function to the tasklet's types, and every SDK that declares a tasklet as a function (Rust's `#[flezha::relay]` / `#[flezha::relay]` on a function, Python's decorated function) MUST implement it identically. A Rust or Java tasklet written as a type names its types on that type instead: `type Input` / `type Output` in the Rust `impl Tasklet` block, the `Tasklet<Input, State, Output>` arguments in Java.

#### Scenario: Input and Output inferred from signature

- **WHEN** a `run` declaration takes a first value parameter of type `I` and returns `O` (or `Result<O, _>`)
- **THEN** the tasklet's `Input` is `I` and `Output` is `O`

#### Scenario: Unit return means no output

- **WHEN** a `run` declaration returns no value (unit)
- **THEN** the tasklet declares no output channel

#### Scenario: Defaults for unspecified types

- **WHEN** a tasklet declared as a function specifies no clock
- **THEN** it runs on the wall clock

### Requirement: State parameter drives stateful inference
A function-declared tasklet SHALL keep no state in Rust: `#[flezha::relay]` on a function with a `&mut` parameter SHALL fail to compile with a message beginning `ShortFormStateOnFunction`, and a stateful Rust tasklet SHALL name `type State` on its `impl Tasklet` block. In an SDK that infers a function tasklet's state from a parameter, a mutable-state parameter SHALL be the single signal that the tasklet is stateful, and its type SHALL become the tasklet `State`.

#### Scenario: Mutable state parameter sets State type

- **WHEN**, in an SDK that infers state from a parameter, a `run` declaration includes a mutable reference parameter of type `S`
- **THEN** the tasklet's `State` is `S` and the tasklet is treated as stateful

#### Scenario: No state parameter means stateless

- **WHEN** a `run` declaration has no mutable-state parameter
- **THEN** `State` is the unit type and `Change` is the no-op change type

#### Scenario: A Rust function with a state parameter is refused

- **WHEN** `#[flezha::relay(id = "store_input")]` annotates `fn store_input(input: String, state: &mut S) -> String`
- **THEN** the macro fails to compile with a message beginning `ShortFormStateOnFunction`

### Requirement: State-to-Change derivation with explicit boundary
The contract SHALL define how a state type's field mutations derive a `Change` type and `apply_change`, and SHALL define the explicit boundary beyond which a hand-written `Change` is required.

#### Scenario: Field mutation auto-derives a change

- **WHEN** a state type opts into change derivation and a field is appended-to or set
- **THEN** a corresponding change variant is generated and the mutation is journaled to the existing WAL

#### Scenario: Beyond the boundary requires manual Change

- **WHEN** a mutation is conditional, spans multiple fields atomically, or is computed
- **THEN** the SDK requires a hand-written `Change` and reports a clear error rather than journaling incorrectly

### Requirement: Cell mode is an explicit opt-in, never inferred
The contract SHALL express a tasklet's cell mode (single, static, dynamic) through explicit declarations that map 1:1 to the existing runtime surface, and SHALL NOT infer the mode from the `run` body. Static and dynamic declarations are mutually exclusive (composite mode is out of scope).

The declarations are named for the Cell, not the Tasklet. Static mode is declared **on the tasklet**: the keyset is `cell_keys`, its routing is `cells_for`. Dynamic mode is declared **on the wiring**: the key function is `key_by` / `keyBy` on the consumer edge, and the eviction window remains `cell_idle_ttl` on the tasklet. A tasklet declaring neither a static keyset nor being reached by a keyed edge is single-mode.

#### Scenario: Dynamic keying is declared on the wiring

- **WHEN** a relay wires a tasklet through an edge declaring a key function, and the tasklet declares optional idle-TTL / max-cells
- **THEN** the SDK SHALL produce a dynamic-mode tasklet against the existing runtime surface (lazily created Cells, empty static key list) with no new runtime path and no keying member on the tasklet itself

#### Scenario: Static and dynamic declarations together are rejected

- **WHEN** a tasklet declares a static keyset and is also wired through an edge declaring a key function
- **THEN** the SDK SHALL report a build-time error that the modes are mutually exclusive, naming the tasklet

#### Scenario: Static routing may name several cells

- **WHEN** a static-mode tasklet's `cells_for` declaration returns more than one key for one input
- **THEN** the input MUST be routed to the Cell for each returned key

#### Scenario: The authoring sugar offers no dynamic key hook

- **WHEN** the attribute/annotation sugar is generated for a tasklet
- **THEN** it SHALL offer no `cell_key`-style hook, and declaring one SHALL fail at compile time (Rust) or annotation processing (Java) with an error directing the author to `key_by` on the wiring

### Requirement: Single-tasklet auto-wiring and registration
The contract SHALL define exactly one topology that may be written short, a single tasklet from a controller input to at most one output, and every SDK SHALL carry it for both relay kinds: Rust, Java, Python, Node, Ruby, PHP and Elixir. **The short form SHALL be a relay declaration**, written at the SDK's kind-named relay entry point, and SHALL NOT be a tasklet declaration that becomes a relay: `#[flezha::relay]` / `#[flezha::relay]` in Rust, `@Relay` / `@Relay` in Java, `@flezha.relay` / `@flezha.relay` in Python, `defineRelay` / `defineRelay` in Node, a `Flezha::Relay` / `Flezha::Relay` subclass in Ruby, `#[Relay]` on a class extending `Flezha\Relay` / `Flezha\Relay` in PHP, `use Flezha.SDK.Relay` / `use Flezha.SDK.Relay` in Elixir.

A relay declaration SHALL be a short form exactly when it carries its one tasklet's body **instead of** a topology body, a topology body being `channels`, the one name both kinds give it in every SDK. The body SHALL be named `run`, as every SDK names a tasklet's body. The kind SHALL be the entry point's own name (in PHP, the base class its `#[Relay]` class extends) and SHALL NOT be a key on the declaration. The identity SHALL be the relay's factory id, spelled as that SDK's full-form relay already spells it (`id` in Rust, Java and Python, `factoryId` in Node and PHP, `factory_id` in Ruby and Elixir), and SHALL be written by the author; an SDK that otherwise derives a name SHALL NOT derive a factory id. The declaration MAY carry an `output`, an entry channel name `input`, and, in an SDK whose channel builder names an entry codec (Python, Node, Ruby, PHP), that SDK's `codec` with the builder's vocabulary.

`output` SHALL be optional. With an `output`, the generated relay SHALL declare an entry channel, the tasklet, and a plugin output of that name; with no `output`, it SHALL declare the entry channel and the tasklet and no plugin channel, which is the topology a full-form relay already declares when its channel ends at a tasklet. `input` and `codec` SHALL be accepted whether or not an `output` is written, because an entry channel exists either way.

The generated relay SHALL otherwise be the relay an author would hand-write with that one tasklet: an entry channel named by `input`, or `input` when none is written, decoded by `codec` where one is written; the tasklet; a factory id equal to the written identity; and every class-level setting at the SDK's default for its kind. A steps short form SHALL therefore be a `once` steps relay unless it declares its loop mode, its entry channel receiving its first message, from a spawn, `flz relay send` or `flz relay start --text`, and its `init_values` being the tasklet's initial state as on a flow relay: the loop mode SHALL be accepted on a steps short form in every SDK, in the spelling that SDK's full form gives the setting (`each` on the Rust attribute, `each()` on `@Relay`, `each` on the Node definition, `each=` on the Python decorator, `each` in the Ruby class body, `#[Steps(each: ...)]` beside `#[Relay]` in PHP, `each:` on the Elixir `use`), and SHALL reach the generated relay as the full form's loop mode does. Every other class-level setting (the retry ladder is no longer one, being an edge's; a duration, persistence, the effectively-once tier, a correction window, rollback mode, synchronous durability) SHALL NOT be expressible on the short form; it is the full form, and where an SDK's spelling lets an author write one on a short form it SHALL be refused naming the setting. A loop mode or rollback mode outside the declared set (`once`, `per_input`, `chained`; `undo_all`, `keep_progress`) SHALL be refused at declaration naming the value, in every SDK whose spelling can write one. The tasklet's own settings (cell keys and `cells_for`, mailbox capacity, clock, schema version, the state and change declarations) SHALL be accepted on the short form and SHALL reach the generated tasklet. A short-form key (`output`, `input`, `codec`) or a tasklet setting written on a declaration that carries a topology body SHALL be refused rather than ignored.

The expansion SHALL produce an ordinary tasklet and an ordinary relay, and the generated tasklet's node name SHALL be the factory id verbatim, with no suffix, case change or decoration, because a tasklet's name is a durable component of every cell's state path. Where an SDK generates code, the two SHALL be separate types, and the author-facing name SHALL be the relay's, because that is what the declaration declares.

Registration SHALL be explicit in every SDK: the author hands the declaration to the runner's `register_factory`, which reads its kind as it reads any relay's, and nothing is harvested, scanned or collected. No SDK SHALL require the author to name a generated artefact.

A relay declaration carrying both a tasklet body and a topology body SHALL be refused. A written `output` on a body the SDK can see declares no output SHALL be refused; an SDK whose tasklets declare no output type owes no such refusal and says so in its conformance manifest.

#### Scenario: A flow short form is the hand-written single-tasklet stream

- **WHEN** a flow relay declaration carries its identity, an `output` and a tasklet body, and the author registers it
- **THEN** the runner declares a flow relay wiring the controller input to the tasklet to the named output, with every class setting at its default, and the declaration equals the one a hand-written single-tasklet stream produces

#### Scenario: A steps short form runs once on its first message

- **WHEN** a steps relay declaration carries its identity, an `output` and a tasklet body, is registered, an instance is started, and one message is sent to it
- **THEN** the entry channel receives the message, the tasklet runs once, its output reaches the output, and the instance completes

#### Scenario: A short form with no output declares an entry channel and a tasklet

- **WHEN** a flow relay declaration carries its identity and a tasklet body and no `output`
- **THEN** the generated relay SHALL declare the entry channel and the tasklet and no plugin channel, and the declaration SHALL be accepted, as a full-form relay whose channel ends at a tasklet is accepted

#### Scenario: An entry channel name and codec need no output

- **WHEN** a short form declares `input` or a `codec` and no `output`
- **THEN** the declaration SHALL be accepted and the entry channel SHALL take that name and codec

#### Scenario: The generated tasklet is named after the factory id

- **WHEN** a short form declares the factory id `echo`
- **THEN** the generated tasklet's name SHALL be `echo`, so its cells live at the same state path a hand-written tasklet named `echo` uses, and ejecting to the full form moves no cell state

#### Scenario: A tasklet setting reaches the generated tasklet

- **WHEN** a short form declares a tasklet setting such as a mailbox capacity or cell keys
- **THEN** the generated tasklet SHALL carry it, as a hand-written tasklet declaring it does

#### Scenario: A class setting on a short form is refused

- **WHEN** a short form, in an SDK whose spelling can write one, declares a class-level setting other than the loop mode, such as persistence off or a maximum duration
- **THEN** the SDK SHALL refuse the declaration naming the setting

#### Scenario: A steps short form declares its loop mode

- **WHEN** a steps short form in any of the seven SDKs declares the `per_input` loop mode in that SDK's spelling, is registered, and two messages reach its entry channel
- **THEN** the generated relay SHALL declare `each` `per_input`, the tasklet SHALL run once per message, and the instance SHALL not complete after the first

#### Scenario: An unknown loop mode is refused at declaration

- **WHEN** a Python steps relay is decorated with `each="forever"`, or a steps relay in any SDK whose spelling takes a string names a rollback mode outside the declared set
- **THEN** the declaration SHALL be refused naming the value, and nothing SHALL reach the core

#### Scenario: A short-form key beside a topology body is refused

- **WHEN** a relay declaration carries a `channels` body and also an `output`, an `input` or a `codec`
- **THEN** the SDK SHALL refuse the declaration rather than ignore the key

#### Scenario: The kind is read from the entry point, not from a key

- **WHEN** a short form is written at the flow entry point and another at the steps entry point
- **THEN** each SHALL be a relay of that kind, no declaration SHALL carry a `kind` key, and no declaration SHALL be able to name a kind that disagrees with its entry point

#### Scenario: The auto-wired entry channel keeps its default name

- **WHEN** a short form declares no entry channel name
- **THEN** the generated relay's entry channel SHALL be named `input`

#### Scenario: The author names the auto-wired entry channel

- **WHEN** a short form declares the entry channel name `prices`
- **THEN** the generated relay's entry channel SHALL be named `prices`, and a frame whose metadata names `prices` SHALL reach it

#### Scenario: A short form reads text where the builder names codecs

- **WHEN** a Python, Node, Ruby or PHP short form declares the text codec and a frame is sent with `flz relay send --text`
- **THEN** the tasklet receives the text as a string, as the same codec on the builder's `input` delivers it

#### Scenario: Registration is written by the author

- **WHEN** a program declares a short form and never hands it to its runner
- **THEN** the runner declares no relay for it

#### Scenario: The author registers what the author wrote

- **WHEN** a short form is registered in any of the seven SDKs
- **THEN** the argument SHALL be the declaration the author wrote, and no SDK SHALL require the author to name a type, class or module the SDK generated

#### Scenario: A tasklet body beside a topology body is refused

- **WHEN** a relay declaration carries a tasklet body and also a `channels` body
- **THEN** the SDK SHALL refuse naming `topology.short_form_with_topology`, at compile time where the language has one and at declaration or registration otherwise

#### Scenario: An output on a body with no output is refused

- **WHEN** a short form declares an `output` and its body is one the SDK can see declares no output
- **THEN** the SDK SHALL refuse naming `topology.output_without_type`

#### Scenario: Multi-tasklet topology stays explicit

- **WHEN** a relay has more than one tasklet, any fan-out or multi-input edge, a keyed edge, or any class-level setting other than a steps relay's loop mode
- **THEN** the SDK requires the full form and does not auto-wire

#### Scenario: Duplicate factory id fails fast across kinds

- **WHEN** a runner is handed an unbounded and a steps relay, short or full, under one factory id
- **THEN** the second definition fails with an error naming `topology.duplicate_factory_id` and the colliding id

#### Scenario: The same declaration in every SDK

- **WHEN** the reference short-form fixtures `sugar_do_nothing`, `sugar_store_input`, `sugar_named_input`, `sugar_steps` and `sugar_state_only` are authored in each SDK's test app
- **THEN** each is one relay declaration carrying its factory id and a tasklet body (an `output` on all but `sugar_state_only`, and `prices` or `order` for the named entries), no separate tasklet declaration and one registration call, and every runtime's declaration equals the reference golden

### Requirement: Contract binds all current and future SDKs
The contract SHALL be the single source of truth for authoring sugar across the Rust, Java, Elixir, Node, Ruby, PHP and Python SDKs and any future binding, including non-normative target sketches for bindings not yet built. Every SDK's flow and steps authoring surfaces SHALL be rooted in the shared `Relay` base (see below). Beyond the sugar, every surface that more than one SDK carries SHALL change in lockstep: a change to the authoring or declaration surface (builder, ctx, the `HostClassDecl` keys), to the wire protocol or its metadata keys, to a default (overflow policy, retry ladder, mailbox capacity, heartbeat keys), or to a fixture that proves one of these (the regression and round-trip bench factories, the end-to-end runtime matrix) SHALL land in every SDK in the same change, or SHALL name in its spec delta the SDK it defers and the follow-up change that owes it. An SDK that carries a capability another SDK neither carries nor names as deferred is a defect of the change, not a backlog item. A performance change SHALL state in its notes which layer it changed (controller, shared kernel, or host SDK); a measurement taken with one runtime SHALL be reported as that runtime's number, and a claim that holds for every SDK SHALL be backed by the same bench run on each runtime that carries the tier.

An SDK whose authoring surface is a set of factory functions, or a set of classes, rather than macros or annotations SHALL be bound by the same contract: the sketch names the mechanism, and the inference rules, the refusal set and the invocation context are the same.

#### Scenario: Reference relay expands identically across SDKs

- **WHEN** the same reference flow relay is authored with each SDK's sugar
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

- **WHEN** a PHP flow or steps relay is authored
- **THEN** its declaration SHALL expand to the same topology and cell-mode mapping the contract defines for Rust and Java, with no PHP-specific runtime path, protocol message, or WAL change

#### Scenario: An attribute-carried declaration is bound by the same rules

- **WHEN** the PHP SDK's `#[Task]` attribute and override-detected hooks are compared against the contract's inference rules
- **THEN** they SHALL yield the same declaration a Rust macro or a Java annotation yields for the equivalent class, and a divergence SHALL be a defect of the PHP SDK rather than a contract exception

#### Scenario: The Python surface is bound by the contract, not beside it

- **WHEN** the Python SDK's authoring surface is compared against the contract's inference rules
- **THEN** its decorator-based sugar SHALL expand to the same topology and associated-type mapping as the other SDKs', and a divergence SHALL be a defect of the Python SDK rather than a contract exception

### Requirement: Keyed and collection state journaling
The authoring contract SHALL define journaling mutators for per-key map state so a stateful tasklet whose state is a `HashMap`-of-values or `HashMap`-of-collections can be authored without a hand-written `Change` or `apply_change`. Every SDK realization SHALL generate, from a journal-annotated map field, named mutators whose calls are the journaled changes, and a generated change-application that replays them.

The defined modes are:

- **map-set** — `HashMap<K, V>`: a `set` mutator per field; the change records `(key, value)`; application inserts the value at the key.
- **map-vec** — `HashMap<K, Vec<U>>`: `push`, `drain-front(n)`, and `replace-last` mutators; the change records the tasklet and its argument(s); application performs it on the keyed vector (creating it if absent).
- **map-deque** — `HashMap<K, Deque<U>>`: `push-back` and `pop-front(n)` mutators; application performs them on the keyed deque.

#### Scenario: Map-of-collection field generates per-key mutators

- **WHEN** a state type declares a `HashMap<String, Vec<f64>>` field annotated for map-vec journaling
- **THEN** the SDK generates `push`, `drain-front`, and `replace-last` mutators keyed by the map key, plus the matching change variants and the change-application that replays them

#### Scenario: Map-set is the universal fallback

- **WHEN** a tasklet computes a new per-key value and calls the generated `set` mutator
- **THEN** the change records the per-key value, and replay re-applies it by insertion — with no hand-written `Change` or `apply_change`

#### Scenario: Inexpressible mutation reports an error

- **WHEN** a map field's per-entry mutation cannot be expressed by any defined mode (or the field type does not match the declared mode)
- **THEN** the SDK reports a clear authoring error directing the author to a hand-written `Change`, and SHALL NOT generate silent or incorrect journaling

### Requirement: Emit-or-skip output return
The authoring contract SHALL define an optional-output return shape for a short form declared on a function (Rust `#[flezha::relay]` / `#[flezha::relay]` on a `fn`): a function returning an optional `Output` emits when present and emits nothing when absent. The unit return (no output channel) and the unconditional `Output` return (always emit) are unchanged. A Java, Rust or PHP tasklet's `run` returns no output and emits through its context, typed in Java and Rust, so the shape does not apply there: emitting nothing is not calling `emit`. A PHP short form is a class whose `run` is a tasklet's, so it emits through its context too. A Python short form declared on a function (`@flezha.relay` / `@flezha.relay`) takes the same shape, `None` being its absent value; a Python tasklet class's `run`, and a Python short form declared on a class, returns `None` and emits through its typed context, so the shape does not apply there. A Ruby tasklet's `run`, and a Ruby short form's, emits through its context as well, so the shape does not apply there; a Ruby method answers its last expression, so a Ruby `run` may answer a value, and the SDK ignores it. A Node tasklet's `run`, and a Node short form's, a definition object carrying a tasklet's fields and `run`, emits through its typed context as well, so the shape does not apply there; a Node `run` is typed to return nothing, so TypeScript refuses one that returns a value, and the SDK ignores a value a plain JavaScript `run` returns.

#### Scenario: A function short form emits on present and skips on absent

- **WHEN** a Rust function short form returns an optional `Output`
- **THEN** the relay emits the value when present and emits nothing when absent, without the author touching the context

#### Scenario: A Python function short form emits on a value and skips on None

- **WHEN** a Python function short form returns `None` for one input and a value for the next
- **THEN** the relay emits nothing for the first input and emits the value for the second, without the author touching the context

#### Scenario: A Java tasklet that emits nothing

- **WHEN** a Java tasklet's `run` returns without calling `ctx.emit` for an input
- **THEN** nothing SHALL be emitted for that input

#### Scenario: A Rust tasklet that emits nothing

- **WHEN** a Rust tasklet's `run` returns `Ok(())` without calling `ctx.emit` for an input
- **THEN** nothing SHALL be emitted for that input

#### Scenario: A PHP tasklet that emits nothing

- **WHEN** a PHP tasklet's or short form's `run` returns without calling `$ctx->emit` or `$ctx->emitKeyed` for an input
- **THEN** nothing SHALL be emitted for that input, and PHP SHALL refuse to load a `run` that returns a value

#### Scenario: A Python tasklet that emits nothing

- **WHEN** a Python tasklet class's `run`, or a Python short form declared on a class, returns `None` without calling `ctx.emit` for an input
- **THEN** nothing SHALL be emitted for that input, and a `run` that returns a value SHALL be refused

#### Scenario: A Ruby tasklet that emits nothing

- **WHEN** a Ruby tasklet's or short form's `run` answers a value without calling `ctx.emit` or `ctx.emit_to` for an input
- **THEN** nothing SHALL be emitted for that input, and the answered value SHALL be ignored rather than refused

#### Scenario: A Node tasklet that emits nothing

- **WHEN** a plain JavaScript tasklet's or short form's `run` returns a value without calling `ctx.emit` or `ctx.emitTo` for an input
- **THEN** nothing SHALL be emitted for that input, the returned value SHALL be ignored, and the same `run` written in TypeScript SHALL fail to type-check

### Requirement: Single hand-written path determinism
The authoring contract SHALL state how each SDK keeps a tasklet's live and replay paths from diverging. A tasklet mutating state only through generated journaling recorders (Elixir's `state` block) has exactly one hand-written path (`run`) and one generated replay path (the generated change-application); the two cannot diverge because the replay path is generated from the same recorder definitions. A Rust or Java tasklet's change applies itself (`apply_to` / `applyTo`), and that one fold is what `ctx.change` runs live and what a restore runs on replay. The replay-determinism conformance test remains the gate for all tasklets.

#### Scenario: Sugared tasklet cannot drift between live and replay

- **WHEN** an Elixir tasklet mutates state solely through its `state` block's generated recorders
- **THEN** its replay path is generated (not hand-written), so a live-vs-replay mutation divergence is unrepresentable, and the replay-determinism harness passes

#### Scenario: Replay-safety guards are unnecessary for idempotent set journaling

- **WHEN** a tasklet that previously hand-wrote an idempotency guard for replay safety moves to a change that records the computed value (a set)
- **THEN** the guard is unnecessary because replay re-assigns the computed value rather than re-folding an input, and removing it does not change replay equality

#### Scenario: A Rust change folds the same live and on replay

- **WHEN** a Rust tasklet records its changes through `ctx.change` and the harness runs `replay_equals()`
- **THEN** the replay folds each recorded change, encoded and decoded as the runtime persists it, through the same `apply_to` the live run used, and the result equals the live state

### Requirement: Per-cell state init opt-in
The tasklet authoring sugar SHALL offer an opt-in that maps a cell's key to its initial state, replacing the default `init_state` that returns the state's default value. The opt-in names a function of the cell key returning the state (`fn(Option<&str>) -> State` in Rust; an `@InitState`-annotated `State name(Optional<String> cellKey)` method in Java). The generated `init_state` SHALL delegate to it, passing the cell key — absent in single-cell mode and in the test-harness default construction. The opt-in is pure expansion onto the existing `Tasklet::init_state` trait method; it SHALL NOT receive the restore payload, which the runtime continues to apply after `init_state` returns. The opt-in SHALL require a stateful tasklet; declaring it on a stateless tasklet SHALL be a compile-time error (never silently ignored).

#### Scenario: Cell key maps to per-cell state

- **WHEN** a static-mode sugared tasklet declares the init opt-in and a Cell is created for a given cell key
- **THEN** the generated `init_state` returns the state produced by the opt-in function for that key, rather than the state default

#### Scenario: Key absent in single-cell and harness-default construction

- **WHEN** the init opt-in function is invoked in single-cell mode or by the test harness's default construction
- **THEN** it receives an absent cell key and returns its key-absent state

#### Scenario: Init opt-in on a stateless tasklet is rejected

- **WHEN** the init opt-in is declared on a tasklet with no state
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

### Requirement: Hand-written implementations receive inferable defaults

Tasklet authors SHALL NOT be charged ceremony for information the declaration already states, in each SDK's idiom:

- **Java**: the author's class is the one tasklet form. Its `Input`, `State` and `Output` SHALL be read from the implementing class's generic supertype chain (direct `implements Tasklet<…>` with concrete arguments, or a concrete subclass of a generic intermediate class). When one cannot be resolved, registration SHALL fail with an error naming the class and the unresolved type parameter, never guessing. The name and scalar settings SHALL be read from `@Tasklet.Spec`, the initial state from the state class's no-arg constructor, and the Kryo user types from discovery, each unless the class overrides the matching hook (`initState`, `kryoTypes`), and an override SHALL always win. Whether an **optional hook** is declared SHALL be derived from the implementing class rather than from a companion boolean: the interface SHALL carry no `*Overridden()` method, and registration SHALL read `onTimer`, `eventTime`, `compensate`, `cellsFor`, `initState`, `migrateState` and `migrateChange` by comparing each hook's declaring class against the interface's own. This keeps Java level with the Ruby and PHP bindings, where a declaration and its implementation cannot disagree.
- **Rust**: the author's type is the one tasklet form, `impl Tasklet for T` under `#[flezha::tasklet]`. The attribute SHALL write every item the block does not: `NAME`, the settings given as keys, the `*_OVERRIDDEN` flags derived from the hooks the block writes, `CLOCK`, `init_state` as `Default::default()`, and `State = ()`, `Change = NoChange` and `Output = ()`. It SHALL generate no type. A block that writes every item itself SHALL remain fully supported without the attribute. There SHALL be no `SimpleTasklet` trait.

The mandatory authoring surface for the common case therefore reduces to a name and `run` in both SDKs.

#### Scenario: Java types resolved from the implements clause

- **WHEN** a class declares `implements Tasklet<Trade, PriceWindow, Move>` and carries `@Tasklet.Spec(name = "track_prices")`
- **THEN** registration SHALL succeed with the three types resolved exactly as written in the clause

#### Scenario: Unresolvable type fails registration by name

- **WHEN** a class implements `Tasklet` with a type argument still generic at the concrete class
- **THEN** registration SHALL fail with an error naming the class and the unresolved type parameter, and nothing SHALL be registered

#### Scenario: The Rust attribute writes what the block leaves out

- **WHEN** a `#[flezha::tasklet(name = "echo")]` block writes only `Input`, `Output` and `run`
- **THEN** it SHALL be registrable wherever a `Tasklet` is accepted, with `State = ()`, `Change = NoChange`, the wall clock and `NAME` `echo`

#### Scenario: Explicit override wins

- **WHEN** a Java class overrides `kryoTypes()`, or a Rust `#[flezha::tasklet]` block writes `init_state`
- **THEN** the explicit declaration SHALL be used, defaults notwithstanding

#### Scenario: A hook cannot be written and left unclaimed

- **WHEN** a Java class declares one of the optional hooks, or a Rust `#[flezha::tasklet]` block writes `compensate` or `cells_for`, and writes nothing else about it
- **THEN** registration SHALL treat the hook as declared, and the binding SHALL offer no way to write the hook while leaving it inactive

### Requirement: Every SDK binding passes the shared conformance suite

Every SDK binding SHALL ship a test app under `test/apps/<sdk>_runner/` with a conformance manifest (`sdk-conformance`), and SHALL pass the shared end-to-end suite under `FLEZHA_E2E_RUNTIME=<sdk>` for every capability its manifest does not defer. A capability listed under `deferred` SHALL match the SDK's documented scope matrix, and the documented matrix SHALL point at the manifest rather than restate it. The contract's reference-relay equivalence (§6 `static_cell`, §6.1 `rolling_window_twin`) SHALL be asserted against the controller-side golden declaration snapshot in the smoke tier, in addition to any SDK-local expansion test; an SDK whose expansion differs from the golden fails conformance, whatever its local test says.

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

- `SCHEMA_VERSION` is bumped on any non-additive edit to the Tasklet's `State`
  **or** its `Change` type — not to the `Change` type alone;
- the codecs are positional and not self-describing, so an unbumped edit that
  changes field order or field count is a silent corruption, not a compatibility
  question;
- `MIN_READABLE_SCHEMA_VERSION` plus the migration entry points are how an
  author reads what an earlier version wrote, and their default is to refuse.

The authoring test harness SHALL be able to restore a Tasklet from bytes
stamped at an older schema version, so a migration is testable without a running
controller.

#### Scenario: a migration is proven at the authoring seam

- **GIVEN** a Tasklet at `SCHEMA_VERSION = 2` with a declared migration from version 1
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
calls (the channels, the ordered links of each chain — transform, key, output, name,
consumer — the merges and branches, the class flags and the tasklet facts), to one
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

#### Scenario: The checkpoint members are judged in one place

- **WHEN** a class declares `ack: checkpoint` with `every`, `outputs` or `max_delay_ms` in any SDK
- **THEN** the record carries the tree as written and the reference check reads the
  acknowledgement granularity and judges its preconditions, with no per-SDK helper doing so

### Requirement: Short-form refusals carry one name per refusal in every SDK
The three short-form refusals SHALL be named `topology.short_form_with_topology`, `topology.output_without_type` and `topology.duplicate_factory_id` in every SDK, and the name SHALL be the code the author reads: in the compile error text in Rust, Java and an Elixir `use` declaration, and as the `code` of the SDK's one error type in Python, Node, Ruby, PHP and an Elixir registration. The names SHALL be listed in `docs/sdk/authoring_contract.md` §5, which the conformance check reads as the source of the row names.

`SinkWithoutId`, `SinkWithoutKind` and `ShorthandKeyWithoutSink` SHALL NOT exist. A short form's identity is a relay's factory id, which every relay has always been required to carry, so the first is the full form's own rule; the kind is the entry point's name, so the second is unrepresentable; and `input` and `codec` are meaningful with no `output`, so the third refuses nothing.

#### Scenario: A refusal is recognisable across languages

- **WHEN** an author writes a tasklet body beside a topology body in any of the seven SDKs
- **THEN** the error they see contains `topology.short_form_with_topology`, whatever else that SDK's error carries

#### Scenario: The duplicate-id error is named, not only described

- **WHEN** a runner is asked to define a second relay with a factory id it already holds
- **THEN** the error contains `topology.duplicate_factory_id` and the id, in every SDK

#### Scenario: A retired refusal name is gone from every SDK

- **WHEN** the seven SDK repositories are searched for `SinkWithoutId`, `SinkWithoutKind` or `ShorthandKeyWithoutSink`
- **THEN** no source, test, fixture or document names any of the three

### Requirement: The timer contract holds in flow and in steps and no class-level wake exists

The `on_timer` contract of section 3.4 of the authoring contract SHALL hold for a tasklet of a steps relay exactly as for a tasklet of a flow relay, in every SDK: the four timer calls SHALL be available on the tasklet context in both kinds under the same names, `fired_at` SHALL be the scheduled timestamp and equal `ctx.now()` inside the hook, `timer_after` SHALL anchor on `ctx.now()`, and a registration made during a replay or a re-run SHALL be registered. What differs in a steps relay SHALL be only what follows from the steps relay's peculiarities, and the contract SHALL state it: a firing starts a pass at the tasklet that set the timer, or joins that tasklet's stage while it runs, or waits its turn behind the pass in flight; a `ONCE` instance ends with its one pass and a timer due after that never fires; a firing's removal is durable at its stage's commit. No SDK SHALL declare a class-level wake setting (`wake_after`, `wakeAfter`, `wake_after_ms`, `wakeAfterMs`): a steps relay that must wake itself writes a timer in a tasklet. The `compensate` hook SHALL keep no timer session in either kind.

#### Scenario: The same tasklet code arms a timer in both kinds

- **WHEN** a tasklet whose `run` calls `timer_after` and whose `on_timer` emits is wired into a flow relay and, unchanged, into a `per_input` steps relay
- **THEN** both SHALL register the timer, both SHALL run `on_timer` at its scheduled time, and the steps relay's firing SHALL open a pass at that tasklet

#### Scenario: The retired wake setting is refused as an unknown setting

- **WHEN** a steps class in any SDK writes the wake setting in that SDK's old spelling
- **THEN** the SDK SHALL refuse it as it refuses any unknown class setting (a compile error, a `TypeError`, a `NoMethodError`, an unknown attribute argument, a `CompileError`), and no declaration SHALL carry `wake_after_ms`

### Requirement: Every SDK offers one relay base

Every SDK SHALL offer one relay authoring base that takes the settings tree and the
graph, and SHALL offer the short form on that same base. The short form SHALL take the
class settings the full form takes. No SDK SHALL offer a second base, a kind-specific
factory function or a kind-specific registry.

#### Scenario: The seven bases

- **WHEN** each SDK's public surface is listed (`public-api.txt`, the Java API module, the Node type declarations, the Python, Ruby and PHP packages, the Elixir `use` macros)
- **THEN** each SHALL name one relay base and no retired class, and the conformance surface snapshot SHALL match

### Requirement: A setting under another setting cannot be written

Each SDK SHALL model every "one of" of the tree as a sum type of its language and every
nested setting as a member of its parent only, so that a setting written under the wrong
parent is a compile error where the language has one and `topology.unknown_field` at
registration otherwise. No SDK SHALL accept and ignore a misplaced setting.

#### Scenario: A misplaced setting in each SDK

- **WHEN** the reference fixture writes `each: per_input` under `run: flow` in each of the seven SDKs
- **THEN** Rust and Java SHALL fail to compile, and Elixir, Node, Python, Ruby and PHP SHALL refuse registration with `topology.unknown_field` naming `each` and `flow`

### Requirement: The class declares its run and its durability and the runner announces the run

The class SHALL carry `run` and `durable`; the runner SHALL announce the run with the
factory id in `UpdateFactories` and the run and the durability in each instance's
registration; a start SHALL not be able to change either.

#### Scenario: The announcement matches the class

- **WHEN** a class declares `run: {steps, each: chained, on_failure: undo_all}` and `durable: {local, sleep_after: false}` in any SDK
- **THEN** the runner's announcement SHALL carry exactly those values, and `flz relay info` on an instance SHALL print them

