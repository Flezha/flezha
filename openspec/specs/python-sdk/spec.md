# python-sdk Specification

## Purpose
TBD - created by archiving change python-sdk. Update Purpose after archive.
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
- **WHEN** a module declares `@flezha.relay(id="upcase", output="out") def upcase(value: str) -> str` and a plain unit test calls `upcase.tasklet("a")` with no runner started
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

`register_factory` and its inverse `unregister_factory` SHALL be legal both before and after `start()`, each taking a relay of either run, the run read from the relay. The declared factory set SHALL be per-runner and SHALL start empty each generation. A post-start declaration SHALL reach the core immediately, so the set the reconnect init frame is serialized from at send time carries it; that frame is the resync of record. Removing a type with live instances SHALL raise; removing an undeclared type SHALL raise naming the id.

Shipping an `UpdateFactories` frame to a *live* controller is out of reach for a veneer host: the ABI exposes no entry point that emits one, and none that retracts a declaration, so only the in-process Rust host — which owns the runner channel directly — can notify a live controller of a post-start change. The Java SDK carries the identical gap. Closing it is a change to `sdk-core-abi`, owed to the follow-up `sdk-abi-factory-resync`; until it lands, a veneer host's post-start change reaches the controller at the next bind.

#### Scenario: Post-start define reaches the core and the next bind
- **WHEN** `register_factory` is called on a started runner
- **THEN** the class SHALL be declared to the core immediately, SHALL appear in the runner's declared set, and SHALL NOT block on an acknowledgement

#### Scenario: The live-controller resync is deferred by name
- **WHEN** the SDK's documentation describes a post-start declaration
- **THEN** it SHALL name the ABI gap and the follow-up that owes it, rather than claiming a frame the host cannot send

#### Scenario: Removing an in-use type is refused
- **WHEN** removal targets a type with a live instance
- **THEN** the SDK SHALL raise and the type SHALL remain declared

### Requirement: Diagnostics Use the Standard Library, Not a Tracing Vendor

The SDK SHALL emit its diagnostics through the standard library `logging` module under a named logger hierarchy, SHALL install no handler by default, and SHALL provide one opt-in helper that configures a sensible default — the same posture the Rust SDK's diagnostics defaults and the Elixir SDK's `Logger` use provide. The SDK SHALL NOT depend on or emit OpenTelemetry spans while no other SDK does; adding them is a cross-SDK change.

#### Scenario: A quiet library by default
- **WHEN** a user imports the SDK and starts a runner without configuring logging
- **THEN** the SDK SHALL install no handler and SHALL NOT write to the root logger's stream on its own initiative

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

### Requirement: The Channels builder carries merge, the latest-document router and flat_map

`Channels.merge()` SHALL return a merge handle with `from_(source, map)` (`from` being a keyword), `key_by(key)`, `into(tasklet)` and `into(tasklet, name)`, with the `multi-input-tasklets` semantics: a source is an entry edge or an inter-tasklet edge declared on the same builder, each source's mapping composes after that source's own transforms, the key is declared once on the merge and applied to the mapped value on every source's edge, and `into` returns the merged tasklet's own output edge. The builder SHALL record each source as a chain closed by the core's `merge` link under one group per merge and SHALL take the merge refusals from the reference check. `Channels.route_latest(documents, traffic, key_fn)` and `route_latest_named(documents, traffic, key_fn, name)` SHALL expand to the `sdk-graph-authoring` router, packaged in the SDK as a single-cell tasklet with journaled `{document, version, delivered}` state. `Edge.flat_map(fn)` SHALL exist beside `map` and `filter`: the callable answers an iterable, each element reaches the consumer on its own, an empty iterable is a drop, the chain is applied as a list so `filter` and `flat_map` are one mechanism, and the record carries the same `transform` link as `map`.

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

### Requirement: The single-tasklet short form is four decorator keywords
The short form SHALL be declared with `@flezha.relay`, the same decorator that declares a full-form relay, and SHALL NOT be declared with `flezha.tasklet`, which SHALL accept none of `output`, `input` or `codec` and SHALL raise `FlezhaError` naming the keyword and the relay decorator when one is passed. Each decorator SHALL accept `id`, `output`, `input` and `codec`, and the keywords `flezha.tasklet` takes but `name`, which it hands to the generated tasklet; how it runs is the `run` keyword, as on the full form.

A decorated object SHALL be a short form when it is a tasklet body rather than a `Channels` body: a class, which is never a channels body, or any function but a channels body, which is a function whose one parameter is annotated `flezha.Channels` or not annotated at all, the shape the SDK already reads as a builder body. `output` SHALL be optional. With `output`, the decorator SHALL build a relay whose factory id is `id`, wiring an entry channel named `input` (or the `input` keyword's value) into a generated tasklet named `id` and that tasklet to the `output` plugin channel; with no `output`, it SHALL wire the entry channel into the tasklet and declare no plugin channel. A short form SHALL take the class settings as the full form does; a steps short form SHALL be a `once` steps relay unless its `run` writes another `each`, fed by its first message on the entry channel and taking `init_values` as the tasklet's initial state. A value outside its choice (`each` other than `"once"`, `"per_input"` and `"chained"`, `on_failure` other than `"undo_all"` and `"keep_progress"`) SHALL be refused naming the setting and the value, on the short form and the full form alike. `codec` SHALL take the vocabulary of `Channels.input(name, codec=)` and SHALL be lowered to that call, whether or not an `output` is written. Declaring SHALL still perform no I/O.

At decoration the SDK SHALL raise `FlezhaError` whose code is the refusal's code: `topology.output_without_type` for an `output` on a body whose return annotation is absent or `None`, and `topology.short_form_with_topology` for a decorated object carrying both a tasklet body and a `Channels` body. An absent `id` SHALL be refused naming the required keyword, and the SDK SHALL NOT derive a factory id from `__name__`. `Runner.register_factory` SHALL accept a short form and register its relay, and `Runner.register_modules` SHALL register the relays a named module carries, short or full. A duplicate factory id SHALL raise `FlezhaError` with the code `topology.duplicate_factory_id`.

#### Scenario: A decorated function is a whole flow relay

- **WHEN** a module declares `@flezha.relay(id="upcase", output="out", codec="text") def upcase(s: str) -> str` and the runner is given `register_factory(upcase)`
- **THEN** the runner declares a flow relay `upcase` with entry channel `input` decoding text, a tasklet named `upcase` and output `out`, and the module declares no `flezha.tasklet`

#### Scenario: A decorated function is a whole steps relay

- **WHEN** a module declares `@flezha.relay(id="place", output="placed", input="order", codec="text") def place(s: str) -> str`, the runner is given `register_factory(place)`, an instance is started, and one message is sent on `order`
- **THEN** the tasklet receives the message as text on `order`, its return value reaches `placed`, and the instance completes

#### Scenario: A short-form steps relay declares its loop mode

- **WHEN** a module declares `@flezha.relay(id="chat", output="replies", codec="text", each="per_input") def chat(s: str) -> str` and two messages reach its entry channel
- **THEN** the declaration carries `each` `per_input`, the tasklet runs once per message, and the instance does not complete after the first

#### Scenario: An unknown loop mode is refused at decoration

- **WHEN** a module declares `@flezha.relay(id="w", each="forever")` over any body, or `on_failure="best_effort"`
- **THEN** importing it raises `FlezhaError` naming the keyword and the value, and nothing is declared

#### Scenario: A short form with no output is declared

- **WHEN** a module declares `@flezha.relay(id="counter", input="ticks", codec="text")` over a class with `run` and `apply_change` and the runner is given `register_factory`
- **THEN** the relay declares `ticks` into the tasklet and no plugin channel, and the tasklet's changes are journaled as any tasklet's are

#### Scenario: A codec needs no output

- **WHEN** a short form declares `codec="text"` and no `output`
- **THEN** the declaration is accepted and the entry channel decodes text

#### Scenario: register_modules collects a short form

- **WHEN** a module declares a short form and the runner is given `register_modules("that.module")`
- **THEN** the runner declares the short form's relay

#### Scenario: A builder body is still the full form

- **WHEN** `@flezha.relay(id="static_cell")` decorates `def f(c: flezha.Channels) -> None`
- **THEN** it declares a full-form relay from that body, and no tasklet is generated

#### Scenario: A tasklet body beside a channels body is refused

- **WHEN** a decorated class declares both a `run` body and a `channels` body
- **THEN** importing it raises `FlezhaError` whose code is `topology.short_form_with_topology`

#### Scenario: A relay keyword on a tasklet is refused

- **WHEN** a module declares `@flezha.tasklet(name="upcase", output="out")`
- **THEN** importing it raises `FlezhaError` naming `output` and `flezha.relay`

#### Scenario: A derived id is refused

- **WHEN** a module declares `@flezha.relay(output="out") def upcase(s: str) -> str`
- **THEN** importing it raises `FlezhaError` naming the required `id`, and no socket is opened

#### Scenario: An output on a body with no output is refused

- **WHEN** a module declares `@flezha.relay(id="drain", output="out") def drain(s: str) -> None`
- **THEN** importing it raises `FlezhaError` whose code is `topology.output_without_type`

#### Scenario: The generated tasklet composes in another topology

- **WHEN** a short form's generated tasklet is named in another relay's `Channels` body
- **THEN** it wires as any tasklet does, and the short form's own relay is declared only if the runner is also handed it or its module

#### Scenario: A function short form that returns None emits nothing

- **WHEN** a function short form with an `output` returns `None` for one input and `"b"` for the next
- **THEN** nothing SHALL be emitted for the first input, and `"b"` SHALL reach the output for the second

#### Scenario: A function short form taking a context is refused

- **WHEN** a module declares `@flezha.relay(id="stamp", output="out") def stamp(value: str, ctx: flezha.Ctx[None, str]) -> str`
- **THEN** importing it raises `FlezhaError` naming the parameter `ctx` and directing the author to a class body

#### Scenario: A class short form emits through its context

- **WHEN** `@flezha.relay(id="store_input", output="sink")` decorates a class whose `run(self, value: str, ctx: flezha.Ctx[Stored, str]) -> None` calls `ctx.change(...)` and `ctx.emit(value)`, and the runner is given `register_factory(store_input)`
- **THEN** the relay declares a tasklet named `store_input`, its changes are journaled, and the emitted value reaches `output`

### Requirement: A Python class carries the settings tree it wrote

`@flezha.relay` SHALL take the class settings as the tree's own data (`run`, `durable`, `max_duration_ms`, `description`), and `to_host_class_decl` SHALL put the tree on the wire as written, so a setting under a parent that does not carry it reaches the core and is refused as `topology.unknown_field`, naming the setting and its parent, when the runner declares the class to the shared core (at start, or at the registration itself for a class registered after start), as every other core refusal in this SDK is raised, as a `FlezhaError` naming it. `relay` SHALL take no `retry_attempts`, `retry_backoff_ms`, `barrier_retry_attempts` or `wake_after_ms`, and the declaration SHALL carry no `wake_after_ms`. A `Ctx` inside a steps relay SHALL offer `timer_after`, `timer_at_event_time` and `cancel_timer` exactly as inside a flow relay, and a tasklet's `on_timer` SHALL run on its firing in both runs; only a `compensate` hook's `Ctx` SHALL raise an error naming `runtime.timers_unavailable` from them.

#### Scenario: A misplaced setting reaches the core

- **WHEN** a module declares `@flezha.relay("w", run={"flow": {"each": "per_input"}})` and the runner declares it
- **THEN** declaring it to the core raises naming `topology.unknown_field`, `each` and `flow`, and the relay is not declared

#### Scenario: A retired keyword is refused

- **WHEN** a module declares `@flezha.relay(id="w", retry_attempts=5)` or `@flezha.relay(id="w", wake_after_ms=5000)`
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

