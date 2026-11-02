# python-sdk Specification

## Purpose

The Python SDK. Its authoring surface uses decorators whose type hints drive a statically checkable
graph, explicit module registration, versioned state and change encoding without pickle, state
changes only through the change mechanism, deterministic time and identity with warnings on ambient
use, the shared settings tree, short form and channel builder, and standard-library diagnostics. Its
default host is a fetch-and-run thread pool with a required starvation supervisor; every blocking
core call releases the GIL, synchronous and asynchronous handlers are both first class, it refuses
use after a fork, and a free-threaded build tier ships. It is a feature of the ABI crate built into
two wheel tiers, negotiates the schema version first, and is proven through the conformance suite,
the harness teardown contract, a tasklet test harness at parity, a pinned coverage plugin and Python
as a measured runtime.

## Requirements

### Requirement: Python SDK Entry Point and Runner Lifecycle

The Python SDK SHALL provide `flezha.create_runner(namespace)` as the single entry point, returning a concrete runner. Creating a runner SHALL NOT connect, claim the process singleton slot, or spawn any thread. `runner.start()` MUST connect to the controller's control socket, send the handshake init message per `docs/internal/sdk/protocol_spec.md` carrying the runner's currently-declared factory ids (possibly empty), wait for `AssignRunnerId`, and return the same runner. `runner.run()` SHALL be sugar for `start()` followed by `await_termination()`, never an independent implementation. The runner SHALL expose `namespace` and `relays()`, and SHALL support the context-manager protocol so `with flezha.create_runner(ns) as r:` performs graceful shutdown on exit.

At most one **started** runner SHALL exist per process; the slot is claimed at `start()`, not at create. Any number of unstarted runners MAY exist. A second `start()` on the same runner SHALL raise.

The runner SHALL report, in its startup line, the wheel build tier, the active executor and the pool sizing, so a benchmark or bug report is never ambiguous about the conditions it was produced under.

#### Scenario: Successful connection on a created runner
- **WHEN** a user calls `flezha.create_runner("my_app").register_factory(MarketRelay).start()` with the controller running
- **THEN** the call blocks until the controller responds with `AssignRunnerId`, returns the same runner, and `runner.namespace` equals `"my_app"`

#### Scenario: Controller unavailable triggers exponential backoff retries
- **WHEN** a user calls `start()` and the controller's control socket is not yet listening
- **THEN** the SDK SHALL retry the connect with exponential backoff and succeed once the controller becomes available, on the same ladder the other SDKs use

#### Scenario: Zero declared relay types is legal
- **WHEN** `start()` is called with no `register_factory` calls
- **THEN** the runner SHALL connect successfully with an empty factory id list, and types MAY be defined later on the live runner

#### Scenario: Second start on the same runner is refused
- **WHEN** `start()` is called twice on one runner
- **THEN** the second call SHALL raise and SHALL NOT dial the controller

### Requirement: Decorator-Based Authoring Surface

The Python SDK SHALL expose its authoring surface as decorators: `@flezha.state`, `@flezha.tasklet` and `@flezha.relay`. Decorators SHALL register the decorated object into a module-level registry at import time and SHALL NOT connect, spawn threads, or perform I/O.

`@flezha.tasklet` SHALL decorate a class and nothing else. A tasklet class's body SHALL be its `run` method, the name every SDK gives a tasklet's body; a decorated class defining `process` and no `run` SHALL raise `FlezhaError` naming `run`. A function passed to `flezha.tasklet`, as a decorator or as its first argument, SHALL raise `FlezhaError` naming the function, stating that a tasklet is a class whose `run` takes the value and the context, and directing the author to `@flezha.relay` / `@flezha.relay` for a whole one-tasklet relay. `name` SHALL be required: a tasklet declared with no `name`, or an empty one, SHALL raise `FlezhaError` naming the class, and the SDK SHALL NOT derive a tasklet's name from `__name__`. The hooks SHALL be the class's methods of their names (`init_state`, `apply_change`, `cells_for`, `event_time`, `on_timer`, `compensate`, `migrate_state`, `migrate_change`), so writing a hook is what declares it.

A tasklet SHALL be testable with no controller and no runner, through `flezha.testing.TaskletTestHarness`. A function short form's body SHALL remain an ordinary callable, reachable as the relay's generated tasklet (`op.tasklet(value)`), which forwards to the function.

Declaration SHALL be a pure expansion onto the shared authoring contract's surface, introducing no new runtime path, protocol message or WAL change, and decorated tasklets SHALL compose with one another in a single relay graph.

#### Scenario: A function tasklet is refused
- **WHEN** a module declares `@flezha.tasklet(name="price") def price(tick: Tick) -> Quote: ...`
- **THEN** importing it SHALL raise `FlezhaError` naming `price`, stating that a tasklet is a class whose `run` takes the value and the context, and naming `@flezha.relay`

#### Scenario: Decorating performs no I/O
- **WHEN** a module containing `@flezha.tasklet` and `@flezha.relay` declarations is imported with no controller running
- **THEN** the import SHALL succeed, open no socket, and start no thread

#### Scenario: A class tasklet's body is its run method
- **WHEN** a class decorated `@flezha.tasklet(name="upcase")` defines `run(self, value: str, ctx: flezha.Ctx[None, str]) -> None` calling `ctx.emit(value.upper())`, and a relay wires it
- **THEN** each input SHALL reach `run`, and the value passed to `ctx.emit` SHALL be the tasklet's output

#### Scenario: The old body method is refused
- **WHEN** a class decorated `@flezha.tasklet` defines `process` and no `run`
- **THEN** decorating it SHALL raise `FlezhaError` naming `run`

#### Scenario: A tasklet without a name is refused
- **WHEN** a module declares `@flezha.tasklet` or `flezha.tasklet(Upcase)` over a class with no `name`
- **THEN** importing it SHALL raise `FlezhaError` naming `Upcase` and the required `name`

#### Scenario: A renamed class keeps its state
- **WHEN** a tasklet class declared `@flezha.tasklet(name="tally")` is renamed from `Tally` to `WordTally` and its relay is restarted over saved state
- **THEN** the tasklet SHALL still be named `tally`, and its cells SHALL restore the state saved under that name

#### Scenario: A function short form's body stays callable
- **WHEN** a module declares `@flezha.relay(factory_id="upcase", output="out") def upcase(value: str) -> str` and a plain unit test calls `upcase.tasklet("a")` with no runner started
- **THEN** the call SHALL run the function as an ordinary Python function and return `"A"`

### Requirement: Type Hints Drive Graph Wiring, And The Graph Is Statically Checkable

The Python SDK SHALL derive a tasklet's input type from the annotation on `run`'s value parameter, and its output type from the output argument of the `Ctx[State, Output]` annotation on `run`'s context parameter (for a function short form, from the function's return annotation), and SHALL use them to wire the relay graph. The package SHALL ship `py.typed` and a fully annotated public surface, such that a wrong edge type is reported by a standard type checker before the program runs. The decorator SHALL be typed so that `@flezha.tasklet(name=...)` over a class answers `Tasklet[I, O]` with `I` and `O` read from that class's `run`, so a named tasklet is checked at the edge like any other.

A handler whose value parameter annotation is missing SHALL raise `FlezhaError` where the decorator runs, and one whose annotated types cannot be connected into a valid graph SHALL raise at the `register_factory` call site, both before any input is served, with an error naming the offending handler and the specific defect. Registration-time refusals SHALL cover the defect classes Rust catches at compile time: wrong input type on a wired edge, missing entry channel, multiple entry channels, graph cycle, unreachable tasklet, node identity collision, two clocks declared on one tasklet, unservable clock, keyed channel without a plugin wire, tier channel not keyed, checkpoint ack without the effectively-once tier, correction window without its preconditions, branch with too few arms, and duplicate plugin output.

#### Scenario: A wrong edge type is a type error before it is a runtime error
- **WHEN** a user wires a tasklet whose context is `Ctx[None, Invoice]` into a tasklet whose `run` accepts `Shipment`, and runs a standard type checker
- **THEN** the type checker SHALL report the mismatch, naming both types

#### Scenario: Mismatched types are refused at registration
- **WHEN** the same graph is registered without a prior type-check pass
- **THEN** `register_factory` SHALL raise a topology error naming both handlers and the incompatible types, and the runner SHALL NOT connect

#### Scenario: Missing annotation is refused at declaration
- **WHEN** a user declares `@flezha.tasklet(name="handle")` over a class whose `run(self, order, ctx)` has no annotation on `order`
- **THEN** declaring it SHALL raise `FlezhaError` naming the handler and the unannotated parameter

#### Scenario: Refusal happens before any input is served
- **WHEN** a relay graph contains a cycle
- **THEN** the error SHALL be raised from the `register_factory` call site, and no cell, mailbox or connection SHALL have been created

#### Scenario: A named tasklet is checked at the edge
- **WHEN** `tests/typing_fixtures/wrong_edge.py` declares both tasklets as classes with `@flezha.tasklet(name=...)` and wires `Invoice` into `Shipment`, and `tests/typing_fixtures/well_typed.py` declares its tasklets the same way with matching types
- **THEN** pyright SHALL report exactly one error for `wrong_edge.py`, naming `Invoice` and `Shipment`, and none for `well_typed.py`

### Requirement: Durable State Mutation Is Rejected Outside the Change Mechanism

A class decorated with `@flezha.state` SHALL reject direct attribute assignment on its instances from user code, raising an error that names the attribute and directs the user to the change-based mutation path. This supplies, at runtime, the enforcement level the authoring contract records for Python, closing the hole Rust closes structurally via the absent `DerefMut` on its state handle.

The runtime itself SHALL retain the ability to apply changes and to restore state from a snapshot plus change log; the guard SHALL constrain user code only.

#### Scenario: Direct assignment is refused
- **WHEN** user code inside a handler executes `state.count += 1` on a `@flezha.state` instance
- **THEN** the SDK SHALL raise an error naming `count` and stating that the mutation must be expressed as a change

#### Scenario: The change path succeeds
- **WHEN** user code applies the same mutation through the declared change mechanism
- **THEN** the mutation SHALL be applied to the live state, recorded for replay, and SHALL NOT raise

#### Scenario: Recorded changes reproduce state on replay
- **WHEN** a Cell's state is rebuilt from its snapshot and recorded change log
- **THEN** the resulting state SHALL equal the live state at the moment of the last recorded change

### Requirement: Deterministic Time and Identity With Ambient-Use Warnings

`ctx.now()` SHALL return the invocation's time value, computed once by the runtime from the tasklet's declared clock before the handler executes, returning that same value for every call within the invocation. `ctx.stable_id()` SHALL return a deterministic, replay-stable 128-bit identifier derived from the cell identity, the invocation's durable change seq, and a per-invocation call ordinal; repeated calls within one invocation SHALL differ, and the n-th call of a replayed invocation SHALL equal the n-th call of the original. Both SHALL be computed by the engine, so Python values are bit-identical to the other SDKs' without a Python-side implementation.

The SDK SHALL detect ambient non-deterministic calls — including `time.time()`, `datetime.now()` and `uuid.uuid4()` — made inside a handler invocation, and SHALL emit a warning naming the source file and line and directing the user to `ctx.now()` or `ctx.stable_id()`. Detection SHALL warn and SHALL NOT raise or abort the invocation.

#### Scenario: Time is stable within an invocation
- **WHEN** a handler calls `ctx.now()` three times in one invocation
- **THEN** all three calls SHALL return the same value

#### Scenario: Stable ids reproduce across replay
- **WHEN** an invocation calling `ctx.stable_id()` twice is replayed from the durable log
- **THEN** the first and second replayed ids SHALL equal the first and second original ids respectively

#### Scenario: Ambient clock use warns
- **WHEN** a handler calls `time.time()` during an invocation
- **THEN** the SDK SHALL emit a warning naming the file and line and pointing at `ctx.now()`, and the invocation SHALL complete normally

### Requirement: Emit Semantics and Reserved Metadata

`ctx.emit(value)` SHALL route to the tasklet's declared output channel; `ctx.emit(channel, value)` SHALL select a channel explicitly. Emitting SHALL respect the keyed-channel rules: a channel declared keyed SHALL require a key, a channel not declared keyed SHALL refuse one, and both refusals SHALL name the channel. Metadata on a pure inter-tasklet channel with no plugin output SHALL be refused.

User-supplied metadata keys in the reserved `flz.` namespace SHALL be rejected with an error naming the key, so identity stamps can neither be spoofed nor clobbered. The runtime SHALL stamp `flz.cell`, `flz.seq`, and on keyed emits `flz.key`, onto plugin-bound frames, applied after user metadata.

#### Scenario: Keyed channel requires a key
- **WHEN** a handler emits without a key on a channel declared keyed
- **THEN** the SDK SHALL raise an error naming the channel

#### Scenario: Reserved metadata is refused
- **WHEN** a handler emits with the metadata key `flz.seq`
- **THEN** the SDK SHALL raise an error naming `flz.seq`

### Requirement: Explicit Module Registration

The Python SDK SHALL require users to name the modules that declare relays, and SHALL NOT perform implicit package scanning or filesystem discovery. A named module that cannot be imported SHALL raise an error naming the module and chaining the underlying import error.

#### Scenario: Named modules are imported and registered
- **WHEN** a user names two modules containing `@flezha.relay` declarations
- **THEN** both SHALL be imported, their declarations registered, and both factory ids SHALL appear in the handshake

#### Scenario: An unimportable module fails loudly
- **WHEN** a named module raises `ImportError` on import
- **THEN** the SDK SHALL raise an error naming the module and chaining the original `ImportError`

### Requirement: Versioned State and Change Encoding, Never Pickle

Change and state bytes produced by the Python SDK SHALL use a schema-aware, versioned MessagePack encoding supporting additive field evolution: state written by an older declaration SHALL remain loadable after new fields with defaults are added. The encoding SHALL be author-declared through one codec protocol, with built-in adapters for Pydantic models, dataclasses and attrs classes, and a hand-written escape for anything else.

The SDK SHALL NOT use `pickle` for durable state or change bytes, on either the persistence or the wire path, because loading a pickle executes code and its payloads embed class paths that a refactor invalidates. A test SHALL assert no `pickle` use survives on those paths.

Encoding SHALL be validated at registration: a `@flezha.state` class containing a field the codec cannot represent SHALL raise at the `register_factory` call site naming the class and field. Integer offsets, seqs and timestamps SHALL round-trip without narrowing, including values above 2^63.

#### Scenario: Added field remains loadable
- **WHEN** state is persisted, a new field with a default is added to the `@flezha.state` class, and the Cell is restored
- **THEN** restore SHALL succeed and the new field SHALL take its default

#### Scenario: Unrepresentable field is refused at registration
- **WHEN** a `@flezha.state` class declares a field holding an open socket
- **THEN** `register_factory` SHALL raise naming the class and the field

#### Scenario: Pickle cannot be reintroduced
- **WHEN** the persistence and wire paths are inspected by the SDK's own test suite
- **THEN** no `pickle` use SHALL be present, and a change that introduces one SHALL fail that test

### Requirement: Dynamic Factory Declaration

`register_factory` and its inverse `unregister_factory` SHALL be legal both before and after `start()`, each taking a relay of either run, the run read from the relay. The declared factory set SHALL be per-runner and SHALL start empty each generation. A post-start declaration or removal SHALL reach the core immediately, and the core SHALL send the controller the runner's full factory set as `UpdateFactories`; the reconnect init frame stays the resync of record. Removing a type with live instances on the runner SHALL raise the core's `runtime.relay_type_in_use`; removing an undeclared type SHALL raise naming the id.

#### Scenario: Post-start define reaches the core and the next bind
- **WHEN** `register_factory` is called on a started runner
- **THEN** the class SHALL be declared to the core immediately, SHALL appear in the runner's declared set, and SHALL NOT block on an acknowledgement

#### Scenario: Removing an in-use type is refused
- **WHEN** removal targets a type with a live instance
- **THEN** the SDK SHALL raise `runtime.relay_type_in_use` and the type SHALL remain declared

### Requirement: Diagnostics Use the Standard Library, Not a Tracing Vendor

The SDK SHALL emit its diagnostics through the standard library `logging` module under a named logger hierarchy. When a runner starts and the application has configured no handler for that hierarchy or the root logger, the SDK SHALL install one handler on its own logger writing to standard error at the level `FLEZHA_LOG_LEVEL` names, `warn` when unset — the level every other SDK starts at; an application's own logging configuration SHALL win. The SDK SHALL keep the one helper that configures this default explicitly, and core notices SHALL reach the same logger. The SDK SHALL NOT depend on or emit OpenTelemetry spans while no other SDK does; adding them is a cross-SDK change.

#### Scenario: A runner starts at warn on standard error
- **WHEN** a user starts a runner without configuring logging and with `FLEZHA_LOG_LEVEL` unset
- **THEN** the SDK's warnings SHALL reach standard error and its info lines SHALL NOT

#### Scenario: The application's logging wins
- **WHEN** the application configured a handler on the root logger before starting a runner
- **THEN** the SDK SHALL install no handler of its own and its records SHALL reach the application's handler

#### Scenario: One call turns diagnostics on
- **WHEN** a user calls the opt-in diagnostics helper
- **THEN** the SDK's loggers SHALL emit at a documented default level to a documented default destination

### Requirement: A stopped runner exits cleanly

A runner stopped by a signal, by end of file on standard input, or by the controller's own close SHALL let the process exit with status 0 once its drain completes, whether standard input is a pipe or a terminal. The standard-input watcher SHALL take no interpreter-level lock across its blocking read — it reads the descriptor unbuffered — so the interpreter's finalisation never waits on, or aborts over, a thread parked in that read.

#### Scenario: SIGTERM with a piped standard input
- **GIVEN** a started runner whose standard input is a pipe the other end keeps open
- **WHEN** the process receives SIGTERM
- **THEN** the runner SHALL drain and the process SHALL exit with status 0, with no finalisation abort on standard error

#### Scenario: End of file still stops the runner
- **WHEN** the other end of standard input closes
- **THEN** the watcher SHALL observe end of file, the runner SHALL drain, and the process SHALL exit with status 0

### Requirement: The Python SDK SHALL resolve type hints through one fallback

Resolving a callable's or a class's type hints, with a fallback for annotations that cannot be resolved, SHALL be written once in the Python SDK and called from every site that needs it.

The fallback is a judgement call — which exceptions an unresolvable forward reference may raise, and what an unresolvable annotation degrades to — and writing it at three sites means the judgement is taken three times and must be revised three times. It is also what makes a linter report one decision as three findings, each needing its own suppression.

Where a caller needs a different degraded value, that SHALL be a parameter of the shared helper rather than a reason to copy it.

#### Scenario: One definition, called from every site

- **WHEN** the Python SDK's modules that resolve type hints are read
- **THEN** exactly one definition of the resolution-and-fallback SHALL exist, and the others SHALL call it

#### Scenario: The differing fallback value is a parameter

- **WHEN** a caller needs an unresolvable annotation to degrade to something other than the raw annotations
- **THEN** it SHALL pass that value to the shared helper, and SHALL NOT define its own copy

#### Scenario: The suppression is written once

- **WHEN** the broad-exception check is run over the Python SDK
- **THEN** the fallback SHALL account for one finding and one suppression, not three

### Requirement: The state guard SHALL delegate to the class it wraps inside the runtime window

Inside `runtime_mutation()` the guard on a declared state class SHALL hand
assignment to the wrapped class's own `__setattr__`, so a class that refuses
assignment on its own terms — a frozen dataclass — keeps that refusal rather than
gaining a permission the runtime never meant to grant. The delegation SHALL be
covered by a test that fails when it is removed.

#### Scenario: A frozen state class refuses inside the runtime window

- **WHEN** a frozen dataclass declared with `@flezha.state` is assigned to inside `runtime_mutation()`
- **THEN** the dataclass's own `FrozenInstanceError` SHALL be raised, not `FlezhaError` and not silence

### Requirement: The Python builder submits its topology to the reference check

The Python SDK's builder SHALL record the authoring calls it receives — channels, the
ordered links of each chain, merges, branches, class flags and tasklet facts — into the
declaration record and SHALL take its verdict from the reference check, so that every name
of the `TopologyIssue` vocabulary is refused with the reference sentence and no rule is
carried in the builder. The decorator-based surface SHALL stay as it is; only the judgement moves.
The SDK SHALL prove the set by one conformance run over every row of
`test/fixtures/reference_topologies.txt` through the row interpreter in its test app.

#### Scenario: A malformed row refuses with the reference name

- **WHEN** the Python interpreter authors a refusal row through the SDK's own builder
- **THEN** registration refuses with the row's name and the reference sentence

#### Scenario: A well-formed row derives to the fixture's bytes

- **WHEN** the Python interpreter authors an `ok` row
- **THEN** the derived declaration equals the row's lines

#### Scenario: A merge row runs through the SDK's own merge handle

- **WHEN** the Python interpreter reaches `merge_one_source` or `merge_same_twice`
- **THEN** it authors the row with `c.merge().from_(...)…into(...)` rather than skipping it, registration refuses with `topology.merge_too_few_sources` or `topology.duplicate_merge_source`, and the manifest's `topology_surfaces` lists `merge`

### Requirement: The single-tasklet short form is four decorator keywords
The short form SHALL be declared with `@flezha.relay`, the same decorator that declares a full-form relay, and SHALL NOT be declared with `flezha.tasklet`, which SHALL accept none of `output`, `input` or `codec` and SHALL raise `FlezhaError` naming the keyword and the relay decorator when one is passed. Each decorator SHALL accept `factory_id`, `output`, `input` and `codec`, and the keywords `flezha.tasklet` takes but `name`, which it hands to the generated tasklet; how it runs is the `run` keyword, as on the full form.

A decorated object SHALL be a short form when it is a tasklet body rather than a `ChannelBuilder` body: a class, which is never a channels body, or any function but a channels body, which is a function whose one parameter is annotated `flezha.ChannelBuilder` or not annotated at all, the shape the SDK already reads as a builder body. `output` SHALL be optional. With `output`, the decorator SHALL build a relay whose factory id is `factory_id`, wiring an entry channel named `input` (or the `input` keyword's value) into a generated tasklet named `factory_id` and that tasklet to the `output` plugin channel; with no `output`, it SHALL wire the entry channel into the tasklet and declare no plugin channel. A short form SHALL take the class settings as the full form does; a steps short form SHALL be a `once` steps relay unless its `run` writes another `each`, fed by its first message on the entry channel and taking `init_values` as the tasklet's initial state. A value outside its choice (`each` other than `"once"`, `"per_input"` and `"chained"`, `on_failure` other than `"undo_all"` and `"keep_progress"`) SHALL be refused naming the setting and the value, on the short form and the full form alike. `codec` SHALL take the vocabulary of `Channels.input(name, codec=)` and SHALL be lowered to that call, whether or not an `output` is written. Declaring SHALL still perform no I/O.

At decoration the SDK SHALL raise `FlezhaError` whose code is the refusal's code: `topology.output_without_type` for an `output` on a body whose return annotation is absent or `None`, and `topology.short_form_with_topology` for a decorated object carrying both a tasklet body and a `ChannelBuilder` body. An absent `factory_id` SHALL be refused naming the required keyword, and the SDK SHALL NOT derive a factory id from `__name__`. `Runner.register_factory` SHALL accept a short form and register its relay, and `Runner.register_modules` SHALL register the relays a named module carries, short or full. A duplicate factory id SHALL raise `FlezhaError` with the code `topology.duplicate_factory_id`.

#### Scenario: A decorated function is a whole flow relay

- **WHEN** a module declares `@flezha.relay(factory_id="upcase", output="out", codec="text") def upcase(s: str) -> str` and the runner is given `register_factory(upcase)`
- **THEN** the runner declares a flow relay `upcase` with entry channel `input` decoding text, a tasklet named `upcase` and output `out`, and the module declares no `flezha.tasklet`

#### Scenario: A decorated function is a whole steps relay

- **WHEN** a module declares `@flezha.relay(factory_id="place", output="placed", input="order", codec="text") def place(s: str) -> str`, the runner is given `register_factory(place)`, an instance is started, and one message is sent on `order`
- **THEN** the tasklet receives the message as text on `order`, its return value reaches `placed`, and the instance completes

#### Scenario: A short-form steps relay declares its loop mode

- **WHEN** a module declares `@flezha.relay(factory_id="chat", output="replies", codec="text", each="per_input") def chat(s: str) -> str` and two messages reach its entry channel
- **THEN** the declaration carries `each` `per_input`, the tasklet runs once per message, and the instance does not complete after the first

#### Scenario: An unknown loop mode is refused at decoration

- **WHEN** a module declares `@flezha.relay(factory_id="w", each="forever")` over any body, or `on_failure="best_effort"`
- **THEN** importing it raises `FlezhaError` naming the keyword and the value, and nothing is declared

#### Scenario: A short form with no output is declared

- **WHEN** a module declares `@flezha.relay(factory_id="counter", input="ticks", codec="text")` over a class with `run` and `apply_change` and the runner is given `register_factory`
- **THEN** the relay declares `ticks` into the tasklet and no plugin channel, and the tasklet's changes are journaled as any tasklet's are

#### Scenario: A codec needs no output

- **WHEN** a short form declares `codec="text"` and no `output`
- **THEN** the declaration is accepted and the entry channel decodes text

#### Scenario: register_modules collects a short form

- **WHEN** a module declares a short form and the runner is given `register_modules("that.module")`
- **THEN** the runner declares the short form's relay

#### Scenario: A builder body is still the full form

- **WHEN** `@flezha.relay(factory_id="static_cell")` decorates `def f(c: flezha.ChannelBuilder) -> None`
- **THEN** it declares a full-form relay from that body, and no tasklet is generated

#### Scenario: A tasklet body beside a channels body is refused

- **WHEN** a decorated class declares both a `run` body and a `channels` body
- **THEN** importing it raises `FlezhaError` whose code is `topology.short_form_with_topology`

#### Scenario: A relay keyword on a tasklet is refused

- **WHEN** a module declares `@flezha.tasklet(name="upcase", output="out")`
- **THEN** importing it raises `FlezhaError` naming `output` and `flezha.relay`

#### Scenario: A derived id is refused

- **WHEN** a module declares `@flezha.relay(output="out") def upcase(s: str) -> str`
- **THEN** importing it raises `FlezhaError` naming the required `factory_id`, and no socket is opened

#### Scenario: An output on a body with no output is refused

- **WHEN** a module declares `@flezha.relay(factory_id="drain", output="out") def drain(s: str) -> None`
- **THEN** importing it raises `FlezhaError` whose code is `topology.output_without_type`

#### Scenario: The generated tasklet composes in another topology

- **WHEN** a short form's generated tasklet is named in another relay's `ChannelBuilder` body
- **THEN** it wires as any tasklet does, and the short form's own relay is declared only if the runner is also handed it or its module

#### Scenario: A function short form that returns None emits nothing

- **WHEN** a function short form with an `output` returns `None` for one input and `"b"` for the next
- **THEN** nothing SHALL be emitted for the first input, and `"b"` SHALL reach the output for the second

#### Scenario: A function short form taking a context is refused

- **WHEN** a module declares `@flezha.relay(factory_id="stamp", output="out") def stamp(value: str, ctx: flezha.Ctx[None, str]) -> str`
- **THEN** importing it raises `FlezhaError` naming the parameter `ctx` and directing the author to a class body

#### Scenario: A class short form emits through its context

- **WHEN** `@flezha.relay(factory_id="store_input", output="sink")` decorates a class whose `run(self, value: str, ctx: flezha.Ctx[Stored, str]) -> None` calls `ctx.change(...)` and `ctx.emit(value)`, and the runner is given `register_factory(store_input)`
- **THEN** the relay declares a tasklet named `store_input`, its changes are journaled, and the emitted value reaches `output`

### Requirement: A Python class carries the settings tree it wrote

`@flezha.relay` SHALL take the class settings as the tree's own data (`run`, `durable`, `max_duration_ms`, `description`), and `to_host_class_decl` SHALL put the tree on the wire as written, so a setting under a parent that does not carry it reaches the core and is refused as `topology.unknown_field`, naming the setting and its parent, when the runner declares the class to the shared core (at start, or at the registration itself for a class registered after start), as every other core refusal in this SDK is raised, as a `FlezhaError` naming it. `relay` SHALL take no `retry_attempts`, `retry_backoff_ms`, `barrier_retry_attempts` or `wake_after_ms`, and the declaration SHALL carry no `wake_after_ms`. A `Ctx` inside a steps relay SHALL offer `timer_after`, `timer_at_event_time` and `cancel_timer` exactly as inside a flow relay, and a tasklet's `on_timer` SHALL run on its firing in both runs; only a `compensate` hook's `Ctx` SHALL raise an error naming `runtime.timers_unavailable` from them.

#### Scenario: A misplaced setting reaches the core

- **WHEN** a module declares `@flezha.relay("w", run={"flow": {"each": "per_input"}})` and the runner declares it
- **THEN** declaring it to the core raises naming `topology.unknown_field`, `each` and `flow`, and the relay is not declared

#### Scenario: A retired keyword is refused

- **WHEN** a module declares `@flezha.relay(factory_id="w", retry_attempts=5)` or `@flezha.relay(factory_id="w", wake_after_ms=5000)`
- **THEN** importing it raises `TypeError` for the unknown keyword, the decorator declaring neither

#### Scenario: A timer inside a steps tasklet fires

- **WHEN** a tasklet of a running `per_input` steps relay calls `ctx.timer_after(300)` in `run`
- **THEN** the call returns the scheduled timestamp, the effect carries the timer op, and `on_timer` runs on that Cell at that timestamp, opening a pass at that tasklet

### Requirement: A Python tasklet's value leaves only through its context

A tasklet class's `run`, `on_timer` and `compensate` SHALL each take the context as their last positional parameter (`run(self, value, ctx)`, `on_timer(self, timer, ctx)`, `compensate(self, outputs, ctx)`), plain or `async def`; a hook written with fewer positional parameters SHALL raise `FlezhaError` naming the hook and the context parameter. A value SHALL leave a tasklet only through `ctx.emit`, keyed or not, any number of times per invocation. A non-`None` return from any of the three hooks SHALL be refused: at declaration, when the hook's return annotation names a type other than `None`, with a `FlezhaError` naming the hook and `ctx.emit`; and at run time, when the hook returns a value anyway, by failing that invocation as non-retryable with a message naming the tasklet, the hook and `ctx.emit`. The dispatcher and `TaskletTestHarness` SHALL apply the one rule through one shared call.

`flezha.Ctx` SHALL be generic in the state and the output, `Ctx[State, Output]`, with `None` in either position meaning no state or no output, and `ctx.emit` SHALL take a value of the `Output` type, so a type checker reports an emit of the wrong type.

#### Scenario: An annotated return is refused at declaration
- **WHEN** a tasklet class's `run` is annotated `-> str`
- **THEN** declaring it SHALL raise `FlezhaError` naming `run` and `ctx.emit`

#### Scenario: A value returned at run time fails the input
- **WHEN** an unannotated `run` returns `"x"` for an input
- **THEN** that input SHALL fail as non-retryable with a message naming the tasklet, `run` and `ctx.emit`, and nothing SHALL be emitted for it

#### Scenario: A timer firing emits through its context
- **WHEN** a tasklet's `on_timer(self, timer, ctx)` calls `ctx.emit("fired")` and a registered timer fires
- **THEN** `"fired"` SHALL be emitted on the tasklet's output, and a returned value from `on_timer` SHALL fail the firing as non-retryable

#### Scenario: A hook without a context is refused
- **WHEN** a tasklet class writes `run(self, value: str)` or `on_timer(self, timer)`
- **THEN** declaring it SHALL raise `FlezhaError` naming the hook and the context parameter

#### Scenario: A tasklet that emits nothing
- **WHEN** a tasklet's `run` returns without calling `ctx.emit` for an input
- **THEN** nothing SHALL be emitted for that input

#### Scenario: An emit of the wrong type is a type error
- **WHEN** a tasklet whose context is `Ctx[None, str]` calls `ctx.emit(42)` and a standard type checker runs
- **THEN** the type checker SHALL report the call

### Requirement: A Python tasklet's clock follows from what it writes

A tasklet's clock SHALL be event time when its class writes `event_time`, logical when it is declared with `logical_clock=True`, and the wall clock otherwise, and the SDK SHALL write that clock into the tasklet's declaration record as `event_time`, `logical` or `wall`. There SHALL be no `clock` setting. A tasklet that writes `event_time` and is declared `logical_clock=True` SHALL raise `FlezhaError` naming both. A logical clock on a tasklet that records no changes SHALL keep the core's `topology.unservable_clock` refusal.

#### Scenario: The default clock is the wall clock
- **WHEN** a tasklet writes no `event_time` and is not declared `logical_clock=True`
- **THEN** its declaration SHALL carry the wall clock, and `ctx.now()` SHALL be the wall-clock time read once at dispatch

#### Scenario: Writing event_time makes the clock event time
- **WHEN** a tasklet writes `event_time(self, trade) -> int` returning `trade.at`, and a trade with `at = 1720000000000` is dispatched
- **THEN** its declaration SHALL carry the event-time clock, `ctx.now()` inside that invocation SHALL return `1720000000000`, and an event-time timer SHALL fire against it

#### Scenario: A logical clock is declared by its setting
- **WHEN** a tasklet with `apply_change` is declared `@flezha.tasklet(name="seq", logical_clock=True)`
- **THEN** its declaration SHALL carry the logical clock

#### Scenario: Two clocks are refused
- **WHEN** a tasklet declared `logical_clock=True` also writes `event_time`
- **THEN** declaring it SHALL raise `FlezhaError` naming `logical_clock` and `event_time`

#### Scenario: A logical clock with no changes is still unservable
- **WHEN** a tasklet with no `apply_change` is declared `logical_clock=True` and its relay is registered
- **THEN** registration SHALL raise naming `topology.unservable_clock`

#### Scenario: The clock setting is gone
- **WHEN** a module declares `@flezha.tasklet(name="t", clock="event_time")`
- **THEN** importing it SHALL raise `TypeError` for the unknown keyword `clock`

### Requirement: A Python tasklet's cell mode is read from the wiring

A tasklet's cell mode SHALL be derived per node of each relay, from the chains its builder recorded: static when the tasklet declares `cell_keys`, dynamic when some channel's chain has a `key` link before the `into` or `merge` link that names the node, and single otherwise. The SDK SHALL write that mode into the declaration record. There SHALL be no `dynamic_cells` setting. A tasklet that declares `cell_keys` and is fed through `key_by` SHALL be refused by the core as `topology.key_by_on_unkeyable_target`, raised as a `FlezhaError` carrying that name. An empty `cell_keys` SHALL still be refused at declaration.

#### Scenario: key_by alone makes the consumer dynamic
- **WHEN** a relay declares `c.input("in", codec="text").key_by(prefix).into(tally)` and `tally` declares no cell setting
- **THEN** `tally`'s declaration SHALL carry the dynamic cell mode, and inputs with two different keys SHALL reach two different cells

#### Scenario: A keyed merge makes the merged tasklet dynamic
- **WHEN** a merge declares `key_by(...)` and closes on a tasklet with `into`
- **THEN** that tasklet's declaration SHALL carry the dynamic cell mode

#### Scenario: One tasklet wired two ways
- **WHEN** one tasklet is fed through `key_by` in one relay and without a key in another
- **THEN** its node SHALL be dynamic in the first relay's declaration and single in the second's

#### Scenario: A static key set fed by key_by is refused
- **WHEN** a tasklet declared with `cell_keys=["a", "b"]` is fed through `key_by`
- **THEN** registration SHALL raise `FlezhaError` naming `topology.key_by_on_unkeyable_target`

#### Scenario: The dynamic_cells setting is gone
- **WHEN** a module declares `@flezha.tasklet(name="t", dynamic_cells=True)`
- **THEN** importing it SHALL raise `TypeError` for the unknown keyword `dynamic_cells`

### Requirement: A Python tasklet's state starts from `init_state` or its state class

A tasklet's state class SHALL be resolved once at declaration: the `state=` setting, else the state argument of the `Ctx[State, Output]` annotation on `run`'s context parameter, else the return annotation of `init_state`, with `None`, `Any` and an absent annotation meaning no state. A tasklet SHALL be stateful when it writes `init_state(self, cell_key)` or a state class is resolved. For every new cell the runtime SHALL call `init_state` with the cell key (`None` in single-cell mode) when it is written, and otherwise SHALL call the state class with no arguments, taking the origin of a parameterised builtin such as `dict[str, Any]`; the restore fold SHALL start from the same state. A state class that cannot be called with no arguments, on a tasklet that writes no `init_state`, SHALL raise `FlezhaError` at declaration naming the class and `init_state`. A tasklet that writes `apply_change` and is not stateful SHALL raise `FlezhaError` naming `init_state` and the state declaration. There SHALL be no `initial_state` hook.

#### Scenario: A new cell's state is the state class
- **WHEN** a tasklet whose `run` context is `Ctx[Seen, str]` writes no `init_state` and a cell is created
- **THEN** that cell's `ctx.state` SHALL equal `Seen()`

#### Scenario: A written init_state receives the cell key
- **WHEN** a static tasklet with cells `a` and `b` writes `init_state(self, cell_key)` and cell `b` is created
- **THEN** `init_state` SHALL be called with `"b"`, and the cell's state SHALL be what it returned

#### Scenario: A parameterised builtin starts empty
- **WHEN** a tasklet whose `run` context is `Ctx[dict[str, int], None]` writes `apply_change` and no `init_state`, and a cell is created
- **THEN** that cell's `ctx.state` SHALL be `{}`

#### Scenario: An unbuildable state class is refused
- **WHEN** a tasklet's state class requires a constructor argument and the tasklet writes no `init_state`
- **THEN** declaring it SHALL raise `FlezhaError` naming the class and `init_state`

#### Scenario: A restore starts from the same state
- **WHEN** a cell of a tasklet with no `init_state` is restored from a change log of three changes
- **THEN** the fold SHALL start from the state class called with no arguments, and the result SHALL equal the state before the restart

### Requirement: A Python tasklet's idle TTL is whole seconds

A tasklet's idle TTL SHALL be declared as `idle_ttl_seconds`, a positive `int`, on `flezha.tasklet` and on a short form. A `bool`, a float, zero or a negative number SHALL raise `FlezhaError` naming `idle_ttl_seconds`. Unset SHALL mean no idle eviction. The SDK SHALL write the declared seconds times 1 000 into the declaration record's `idle_ttl_ms`. There SHALL be no `idle_ttl_ms` setting.

#### Scenario: Seconds reach the record in milliseconds
- **WHEN** a tasklet fed through `key_by` is declared `idle_ttl_seconds=60`
- **THEN** its declaration record SHALL carry `idle_ttl_ms` `60000`, and an idle cell SHALL be evicted after 60 seconds

#### Scenario: A fractional TTL is refused
- **WHEN** a tasklet is declared `idle_ttl_seconds=0.2` or `idle_ttl_seconds=0`
- **THEN** declaring it SHALL raise `FlezhaError` naming `idle_ttl_seconds`

#### Scenario: The millisecond setting is gone
- **WHEN** a module declares `@flezha.tasklet(name="t", idle_ttl_ms=60_000)`
- **THEN** importing it SHALL raise `TypeError` for the unknown keyword `idle_ttl_ms`

### Requirement: The ChannelBuilder carries merge, the latest-document router and flat_map

`ChannelBuilder.merge()` SHALL return a merge handle with `from_(source, map)` (`from` being a keyword), `key_by(key)`, `into(tasklet)` and `into(tasklet, name)`, with the `relay-graph` semantics: a source is an entry edge or an inter-tasklet edge declared on the same builder, each source's mapping composes after that source's own transforms, the key is declared once on the merge and applied to the mapped value on every source's edge, and `into` returns the merged tasklet's own output edge. The builder SHALL record each source as a chain closed by the core's `merge` link under one group per merge and SHALL take the merge refusals from the reference check. `ChannelBuilder.route_latest(documents, traffic, key_fn)` and `route_latest_named(documents, traffic, key_fn, name)` SHALL expand to the `relay-graph` router, packaged in the SDK as a single-cell tasklet with journaled `{document, version, delivered}` state. `Edge.flat_map(fn)` SHALL exist beside `map` and `filter`: the callable answers an iterable, each element reaches the consumer on its own, an empty iterable is a drop, the chain is applied as a list so `filter` and `flat_map` are one mechanism, and the record carries the same `transform` link as `map`.

#### Scenario: Two channels merge into one tasklet
- **WHEN** a relay declares `c.merge().from_(ticks, tick_input).from_(quotes, quote_input).key_by(lambda v: v["symbol"]).into(detector)`
- **THEN** registration SHALL succeed with one detector node fed by two consumer edges, each applying its own mapping, and a value from either entry SHALL reach the handler as the mapped value keyed by `symbol`

#### Scenario: A duplicated source is refused by the reference name
- **WHEN** a merge declares the same edge twice
- **THEN** registration SHALL refuse with `topology.duplicate_merge_source` naming the channel, and a merge with one source SHALL refuse with `topology.merge_too_few_sources`

#### Scenario: One input becomes three deliveries
- **WHEN** an edge declares `.flat_map(lambda line: [part for part in line.split(",") if part])` into a tasklet and the dispatcher is handed `a,b,c`
- **THEN** the handler SHALL run three times, once per element, under one consumed offset, and a line the callable answers `[]` for SHALL run it zero times

#### Scenario: The router delivers on a key's own traffic
- **WHEN** `c.route_latest(config, trades, lambda t: t["symbol"])` is declared and `TaskletTestHarness` feeds a document, trades for `A` and `B`, the same document, then a trade for `A`
- **THEN** exactly one `("A", document)` and one `("B", document)` SHALL be emitted, and the repeated document SHALL be inert

### Requirement: The Python runner SHALL close with the shared verb and hide its internals

`Runner.close()` SHALL be the one public shutdown verb: it stops fetching, drains and releases the
core, and is idempotent. `Runner.drain` SHALL NOT exist, and the signal-safe trigger the core's
shutdown callback and `__exit__` call SHALL be private. `tasklet()` SHALL take `state_type=` and
`change_type=`, the stream handle SHALL be `flezha.Stream`, `compact_after_bytes` SHALL be a tasklet
setting, and `Ctx.settle_return`, `Ctx.effect_record` and `DROPPED` SHALL NOT be public. The harness
SHALL offer one replay gate, the `replay_equals()` method, with no module-level twin, and its bridge
to the fake core SHALL be private.

#### Scenario: close drains and is idempotent

- **WHEN** application code calls `runner.close()` twice on a started runner
- **THEN** the first SHALL drain and release the core and the second SHALL return without effect

### Requirement: The default host is a fetch-and-run thread pool with a starvation supervisor

The SDK's default executor SHALL run a base pool of Python threads, each looping `poll → dispatch → complete` on itself, and one supervisor thread parked in the core pool governor's supervisor entry (`flz_core_pool_supervise`), which waits on the core's starvation signal and answers what to spawn. Each starvation observation SHALL start one additional worker, up to a ceiling; workers above the base SHALL retire after the governor's idle time, 30 s without work. The base SHALL default to the machine's processor count, passed to the governor as this SDK's default, and SHALL be configured by `FLEZHA_WORKER_POOL_SIZE`; the ceiling SHALL default to ten times the base, never below it, and SHALL be configured by `FLEZHA_WORKER_POOL_MAX`; the core reads both. The supervisor SHALL drain the core's notice queue on every answer. The SDK SHALL introduce no additional pool knob.

The process's main thread SHALL NOT be a poll worker, because CPython runs signal handlers only on the main thread between bytecodes and a main thread inside a handler would delay them by the length of that handler.

#### Scenario: A workload that never starves runs on the base pool
- **WHEN** handlers return promptly and the runner is saturated with work
- **THEN** the pool SHALL remain at the base size and no worker SHALL be started by the supervisor

#### Scenario: Blocked handlers buy threads
- **WHEN** every worker is inside a handler that blocks for seconds and work is queued in the core
- **THEN** the supervisor SHALL observe starvation and start workers up to the ceiling, and the queued work SHALL be served

#### Scenario: Grown workers retire
- **WHEN** the blocking workload ends and the grown workers idle past the retirement streak
- **THEN** the pool SHALL return to the base size

#### Scenario: Concurrency is bounded by the core, not the pool
- **WHEN** the pool is larger than `FLEZHA_MAX_INFLIGHT_ACTIVATIONS`
- **THEN** work activations underway SHALL NOT exceed the core's cap

#### Scenario: The main thread stays available to signal handling
- **WHEN** the runner is serving under full load
- **THEN** no activation SHALL be dispatched on the process's main thread

### Requirement: The starvation supervisor is required, not an optimisation

The SDK SHALL NOT offer a pool configuration that omits the starvation supervisor, including a single-threaded mode. A completion can block: an emit onto a full `DropNothing`-policy consumer mailbox parks the completing thread, and the inline-drain hook that rescues the in-process Rust host is not reachable through the C ABI. A host whose only thread is parked in a completion is therefore not entering `poll`, which is what the core reports as starvation, and the worker the supervisor starts is what drains the consumer and releases the producer.

#### Scenario: A DropNothing-policy wedge recovers on a base pool of one
- **WHEN** the pool base is configured to one worker and a handler emits onto a full `DropNothing`-policy consumer mailbox
- **THEN** the supervisor SHALL observe starvation, start a worker, and that worker SHALL drain the consumer so the blocked completion returns

#### Scenario: The supervisor cannot be configured away
- **WHEN** a configuration attempts to run the pool without the supervisor
- **THEN** the SDK SHALL refuse it rather than start a host that can wedge

### Requirement: Every blocking core call releases the GIL

The binding SHALL release the global interpreter lock for the duration of every blocking core call — `poll`, the pool supervisor entry `pool_supervise`, `complete` and the steps relay inbox receive — so that no blocking core call prevents another Python thread from running. The binding SHALL NOT hold a Rust lock across arbitrary Python execution.

#### Scenario: A blocked steps relay receive does not freeze the interpreter
- **WHEN** a steps stage waits on the inbox for several seconds
- **THEN** other Python threads, including fetchers, SHALL continue to execute Python code throughout the wait

#### Scenario: A blocked poll does not freeze the interpreter
- **WHEN** every worker is parked in `poll` with no work available
- **THEN** an unrelated Python thread SHALL run at full speed

#### Scenario: A blocked completion does not freeze the interpreter
- **WHEN** a completion parks behind a full `DropNothing`-policy consumer mailbox
- **THEN** other Python threads SHALL continue to execute, including the worker that drains that consumer

### Requirement: The poll timeout is the documented interrupt granularity

Because shutdown wakes every poller and the ABI exposes no per-thread wake, worker threads SHALL bound each `poll` by a timeout, and that timeout SHALL be the documented upper bound on how quickly `KeyboardInterrupt`, a termination signal, and runner close are observed. The value SHALL be documented alongside the guarantee.

#### Scenario: Interrupt is observed within the bound
- **WHEN** the process receives an interrupt while all workers are parked in `poll` with no work
- **THEN** shutdown SHALL begin within the documented poll timeout rather than waiting for work to arrive

### Requirement: The executor seam may never affect per-cell ordering

The SDK SHALL expose an executor seam with at least a thread executor (default) and an asyncio executor. Under every executor the thread that fetches SHALL be a real Python thread — never a coroutine, and never borrowed from an event loop's default executor or any other pool the application also uses — and an activation SHALL be completed only after its dispatch has returned. An executor SHALL NOT observe, reorder, batch or coalesce activations.

The executor SHALL be selected by `FLEZHA_PYTHON_EXECUTOR` (default the thread executor) as an axis of the run, never encoded per test, per fixture or per manifest entry.

#### Scenario: Ordering holds under either executor
- **WHEN** the conformance suite runs under the thread executor and again under the asyncio executor
- **THEN** per-cell input ordering and output identity SHALL be identical in both runs

#### Scenario: The asyncio executor does not fetch on the event loop
- **WHEN** the asyncio executor is active
- **THEN** `poll` SHALL be called from a real thread and activations SHALL reach the loop through a thread-safe hand-off

#### Scenario: Completing from a different thread than the one that polled is supported
- **WHEN** an executor dispatches on a thread or loop other than the fetcher's and completes there
- **THEN** the completion SHALL be accepted and the cell released identically to same-thread completion

#### Scenario: One test app serves both executors
- **WHEN** the suite is run under each executor
- **THEN** the same test app and manifest SHALL serve both, with no per-executor fixture, test name or manifest entry

### Requirement: Synchronous and asynchronous handlers are both first class

Handlers SHALL be ordinary synchronous callables by default. The SDK SHALL detect a coroutine-function handler and drive it to completion under whichever executor is active, without requiring the user to declare which style they are using. Both styles SHALL observe identical ordering, retry and durability semantics, and a synchronous user SHALL NOT be required to install or import an event loop library.

#### Scenario: A synchronous handler runs unchanged
- **WHEN** a handler performs a blocking HTTP call
- **THEN** the SDK SHALL invoke it directly and SHALL NOT require any async adaptation from the user

#### Scenario: An async handler is driven under both executors
- **WHEN** a coroutine-function handler is served under the thread executor and again under the asyncio executor
- **THEN** it SHALL be driven to completion in both, with identical ordering and durability semantics

### Requirement: The SDK spawns no processes and refuses use after a fork

The SDK SHALL NOT fork, spawn or supervise processes; process count SHALL be the orchestrator's decision, and each process SHALL be one runner. The SDK SHALL record the process that started the core and SHALL raise, naming the rule, on any core call made from a different process. The documented entrypoint order SHALL be: declare classes, fork if the user forks at all, then start in the child.

#### Scenario: A core call after a fork is refused loudly
- **WHEN** a process forks after the core has been started and the child calls into the core
- **THEN** the SDK SHALL raise an error naming the fork rule, rather than wedging or silently failing to serve

#### Scenario: Fork-then-start is supported
- **WHEN** a user declares classes in the parent, forks, and starts the core in each child
- **THEN** each child SHALL serve as an independent runner

### Requirement: A free-threaded build tier ships, and per-interpreter state keeps sub-interpreters reachable

The SDK SHALL ship a free-threaded wheel tier alongside the GIL tier from its first release, with one source and one API; the free-threaded tier SHALL be documented as the build under which handler bodies execute in parallel. The runner SHALL report which tier it is running in its startup line.

All per-interpreter SDK state — the binding handle, the class registry view and the active executor — SHALL be resolved per interpreter rather than through a process-wide module global, so that a sub-interpreter executor can be added without restructuring. The sub-interpreter executor SHALL be recorded as deferred by name, with the follow-up change that owes it, in the SDK's documentation.

#### Scenario: Handler bodies run in parallel on the free-threaded tier
- **WHEN** two CPU-bound handlers of different Cells are served concurrently under the free-threaded wheel
- **THEN** their bodies SHALL execute in parallel, and the run SHALL be observably faster than the same workload on the GIL tier

#### Scenario: The build tier is never ambiguous
- **WHEN** a benchmark or bug report includes the runner's startup line
- **THEN** that line SHALL name the build tier, the active executor and the pool sizing

#### Scenario: No process-wide global holds the binding
- **WHEN** the SDK's state accessors are inspected
- **THEN** each SHALL resolve per interpreter, and none SHALL be a bare module-level mutable handle

#### Scenario: The deferral is named, not silent
- **WHEN** the SDK's documentation page is read
- **THEN** it SHALL name the sub-interpreter executor as deferred and name the follow-up that owes it

### Requirement: The parallelism model is documented as arithmetic

The SDK documentation SHALL state, without hedging: on the GIL tier, Python-level handler execution is bounded to one core per process; on the free-threaded tier it is not; everything below the poll seam runs with the GIL released and is genuinely parallel under either tier; work underway is capped by `FLEZHA_MAX_INFLIGHT_ACTIVATIONS` regardless of pool size; throughput per loop is approximately the reciprocal of handler latency; and the levers for CPU-bound handlers are, in order, the free-threaded tier, process count, and native libraries that release the GIL.

#### Scenario: The ceiling is stated, not implied
- **WHEN** a reader consults the Python SDK page for how to scale CPU-bound handlers
- **THEN** it SHALL name the free-threaded tier and process count as the levers and SHALL state the one-core-per-process bound of the GIL tier explicitly

#### Scenario: A worked sizing example accompanies the model
- **WHEN** the page states the model
- **THEN** it SHALL work one example through from handler latency and desired in-flight concurrency to a loop and process count

### Requirement: The Python Binding Is a Feature of the ABI Crate, Not a New One

The PyO3 binding SHALL be built from `flz_core_ffi` under a `python` feature, as a one-to-one veneer over the same C entry points in the same binary, mirroring the existing `jni` feature. It SHALL NOT be a separate crate, SHALL NOT reimplement schema negotiation, buffer ownership or error mapping, and SHALL NOT introduce a transport client, framing or reconnect implementation of its own.

#### Scenario: The veneer is a consumer, not a fork
- **WHEN** the Python veneer's exported functions are compared against the ABI's exported symbol set
- **THEN** each SHALL map onto a `flz_core_ffi` entry point, and any Python-reachable kernel capability that bypasses the ABI SHALL be a defect

#### Scenario: The veneer is provable without Python
- **WHEN** the crate's own test suite runs with the `python` feature enabled
- **THEN** a full scenario SHALL be driven through the veneer with no external Python process involved

### Requirement: Schema Version Negotiation Precedes Any Connection

The binding SHALL negotiate the core's schema version at context creation, and a mismatch SHALL raise an error naming both versions before any controller connection is attempted. A wheel built against a different core schema SHALL NOT be able to reach the wire.

#### Scenario: A mismatched build fails before connecting
- **WHEN** a wheel whose embedded core reports a different schema version is loaded
- **THEN** context creation SHALL raise naming both versions, and no socket SHALL be opened

### Requirement: Two Wheel Tiers, One Source and One API

The SDK SHALL declare a supported Python floor no lower than 3.11, and SHALL publish a GIL tier as one `abi3` wheel per supported platform targeting that floor, plus a free-threaded tier as per-version wheels for each supported free-threaded interpreter. Both tiers SHALL be built from one source with one public API and one set of semantics; they SHALL differ only in the interpreter build they target. Prebuilt wheels SHALL be published for the supported platform triples so that installation requires no Rust toolchain; a source distribution SHALL remain as the fallback for platforms without one.

The free-threaded tier SHALL be published from the SDK's first release rather than deferred, because it is the only change that lifts the SDK's single documented concurrency ceiling.

#### Scenario: The floor is what the compatibility wheel targets
- **WHEN** the `abi3` wheel is inspected
- **THEN** it SHALL target the declared floor, so one wheel serves every supported interpreter at or above it

#### Scenario: Installation needs no toolchain
- **WHEN** the SDK is installed on a supported platform in an image with no Rust toolchain, under either tier
- **THEN** installation SHALL succeed from a prebuilt wheel and the runner SHALL start

#### Scenario: The source distribution still builds
- **WHEN** the SDK is installed on a platform with no prebuilt wheel, in an image with a Rust toolchain
- **THEN** the source distribution SHALL build and the runner SHALL start

#### Scenario: Both tiers pass the same suite
- **WHEN** the SDK's unit suite runs under the GIL tier and again under a free-threaded interpreter
- **THEN** both SHALL pass with identical semantics

### Requirement: Payloads Cross the Boundary Without an Intermediate Copy

The binding SHALL expose an activation's user payload to the codec as a borrowed view over the core's buffer, so that the common decode path allocates no intermediate byte object. A user who requests raw bytes SHALL receive a copy. The borrowed view's validity SHALL end with the invocation, and every core buffer SHALL be released through the ABI's free entry point before the binding returns, so no native buffer escapes into Python.

#### Scenario: The common path copies nothing
- **WHEN** an activation is decoded through a built-in codec adapter
- **THEN** no intermediate byte object SHALL be allocated for the payload

#### Scenario: No native buffer escapes
- **WHEN** an activation completes, including one whose handler raised
- **THEN** every core buffer handed out for it SHALL have been freed through the ABI

### Requirement: Errors and Panics Cross the Boundary Intact

An exception raised by a Python handler SHALL be surfaced to the engine as a tasklet error carrying the exception type, message and traceback, and SHALL be subject to the same retry policy as a Rust handler error; an exception marked non-retryable SHALL skip the remaining retry budget and park exactly as its Rust counterpart does. An error originating in the engine and surfaced to Python SHALL preserve the engine's specific detail rather than a generic failure. The boundary SHALL NOT allow a Python exception to unwind into Rust as a panic, nor a Rust panic to reach Python as an opaque failure.

#### Scenario: A handler exception is retried then parked
- **WHEN** a Python handler raises on every attempt and the channel's retry budget is three
- **THEN** the SDK SHALL invoke it three times and then park the input to the DLQ with the Python traceback recorded in the entry

#### Scenario: A non-retryable exception parks immediately
- **WHEN** a Python handler raises an exception marked non-retryable
- **THEN** the SDK SHALL park after a single attempt without consuming the remaining budget

#### Scenario: An engine error reaches Python intact
- **WHEN** a topology build fails inside the engine
- **THEN** the Python-side exception SHALL carry the engine's specific issue, not a generic failure

### Requirement: The wheels are built from the wrapper crate

maturin SHALL build both wheel tiers from the wrapper crate in the Python
tree, whose one dependency is `flz_core_ffi` at the pinned core tag with the
`python-extension` feature and, for the compatibility tier, `python-abi3`.
`pyproject.toml` SHALL name no path outside the Python tree.

#### Scenario: A wheel builds from the Python tree alone
- **WHEN** `maturin build --release` runs in the Python repository with no core tree beside it
- **THEN** it SHALL fetch the pinned tag and produce a wheel whose module exports `PyInit_flz_core_ffi`

#### Scenario: Both tiers ship
- **WHEN** a release completes
- **THEN** the assets SHALL include one `abi3` wheel per triple and one free-threaded wheel per triple per supported interpreter

### Requirement: The Python Test App Is Admitted by the Conformance Suite

The SDK SHALL ship `test/apps/python_runner/` with a conformance manifest declaring its runtime, command template, `test` and `bench` artifact profiles, build command, connect timeout, transports, deferred capabilities and fixtures, and SHALL be dispatchable as a runtime option in the nightly end-to-end steps relay. The suite SHALL pass under `FLEZHA_E2E_RUNTIME=python` for every capability the manifest does not defer, over every transport the manifest lists.

Admitting the runtime SHALL require no change to the shared test helpers, the system suites, the test helper configuration or the conformance gate — a new SDK adds an app, not harness clauses.

#### Scenario: The suite admits the runtime
- **WHEN** `FLEZHA_E2E_RUNTIME=python mix test --include system` is run
- **THEN** every non-deferred capability SHALL pass, and the smoke tier's controller-side declaration snapshot SHALL match the reference

#### Scenario: The manifest is the scope matrix
- **WHEN** the SDK's documentation names the capabilities it carries
- **THEN** it SHALL point at the manifest rather than restate it, and a discrepancy SHALL fail the gate

#### Scenario: The pool-wedge capability is decided by the suite, not by argument
- **WHEN** the wedge drill is run against the Python runner
- **THEN** the capability SHALL be claimed if the fetch-and-run pool produces the wedge signature, and otherwise deferred by name in the manifest with a follow-up recorded

### Requirement: The Python Runner Honours the Harness Teardown Contract

The Python test app SHALL terminate when its standard input reaches end of file, so the harness can stop runners without signals, and SHALL drain in-flight activations before exiting.

#### Scenario: Closing stdin stops the runner
- **WHEN** the harness closes the runner's standard input
- **THEN** the runner SHALL drain its in-flight activations and exit without requiring a signal

### Requirement: A Tasklet Test Harness at Parity With the Other SDKs

The SDK SHALL ship a tasklet test harness at parity with those the Rust and Java SDKs already provide: construction from an initial state or from a restore payload, application of inputs, timer firing, event-time advance, inspection of recorded changes and emits, and a replay-equality gate that folds the recorded change log against a freshly constructed initial state and reports a mismatch with both states for inspection.

The harness SHALL construct a cell's initial state as the runtime does, from `init_state` when the tasklet writes it and otherwise from its state class, and SHALL apply the runtime's rule for what a hook returns through the same shared call: a function short form's return value is emitted, and a tasklet class hook that returns a value fails the invocation. A tasklet SHALL be testable through the harness with no runner started and no controller running, and a function short form's body SHALL remain an ordinary callable that a plain unit test can invoke directly.

#### Scenario: Replay equality is gated in a unit test
- **WHEN** a handler whose change log does not reproduce its terminal state is exercised through the harness
- **THEN** the replay-equality gate SHALL report a mismatch carrying the expected and actual states

#### Scenario: A handler is testable with no runtime
- **WHEN** a tasklet class is driven through `TaskletTestHarness` with `feed`, once per input, and a function short form's generated tasklet is called directly, both from a unit test with no runner started
- **THEN** the harness SHALL report the values the tasklet passed to `ctx.emit` and the changes it recorded, and the function SHALL execute as an ordinary function

#### Scenario: The harness refuses a returned value as the runner does
- **WHEN** the harness feeds an input to a tasklet class whose unannotated `run` returns a value
- **THEN** the invocation SHALL fail with the same non-retryable message the runner produces, naming the tasklet, `run` and `ctx.emit`

### Requirement: Python Is a Measured Runtime, Not an Assumed One

The round-trip benchmark SHALL accept the Python runtime as an arm, and the SDK SHALL land with per-runtime numbers recorded for it rather than inheriting another runtime's. Measurements SHALL interleave arms inside a round, and SHALL report the wheel tier and executor under which they were taken.

#### Scenario: The bench carries the runtime
- **WHEN** the round-trip benchmark is run with the Python runtime selected
- **THEN** it SHALL launch the Python runner from its manifest and report that runtime's numbers

#### Scenario: A number names its conditions
- **WHEN** a Python performance number is recorded
- **THEN** it SHALL name the wheel tier and the executor, and SHALL NOT be reported as another runtime's number

### Requirement: The Python SDK SHALL declare a pinned coverage plugin

The Python SDK's `pyproject.toml` SHALL declare a coverage plugin for `pytest` in the same `dev` extra that pins the rest of the test toolchain, at an exact version, so `pytest` can report coverage from the declared dependencies alone.

The extra currently pins `pytest`, `pyright`, `msgpack` and `ruff` and no coverage tool, so the SDK's suite runs and cannot be measured. Continuous integration SHALL install the plugin from that extra rather than naming a version of its own, on the same terms as every other entry.

The report SHALL measure the package's import-time modules. A test configuration that imports the package before coverage begins tracing — a warning filter resolved at ini time, an early plugin — leaves the modules executed at import wholly unmeasured and prints a warning that reads like a broken tool; the filter SHALL be applied where tracing has already started.

#### Scenario: Coverage runs from the declared extra

- **WHEN** the `dev` extra is installed into a clean checkout and the Python SDK's suite is run in coverage mode
- **THEN** a coverage report SHALL be produced without installing a tool the manifest does not name

#### Scenario: The coverage plugin is pinned like its neighbours

- **WHEN** the `dev` extra is read
- **THEN** the coverage plugin SHALL name an exact version, not a lower bound

#### Scenario: The package's own import is measured

- **WHEN** the suite is run in coverage mode
- **THEN** `flezha/__init__.py` SHALL report the lines the import executed as covered, and the run SHALL NOT warn that the package was imported before it was measured
