# python-sdk Specification

## Purpose
TBD - created by archiving change python-sdk. Update Purpose after archive.
## Requirements
### Requirement: Python SDK Entry Point and Runner Lifecycle

The Python SDK SHALL provide `flezha.create_runner(namespace)` as the single entry point, returning a concrete runner. Creating a runner SHALL NOT connect, claim the process singleton slot, or spawn any thread. `runner.start()` MUST connect to the controller's control socket, send the handshake init message per `docs/sdk/protocol_spec.md` carrying the runner's currently-declared factory ids (possibly empty), wait for `AssignRunnerId`, and return the same runner. `runner.run()` SHALL be sugar for `start()` followed by `await_termination()`, never an independent implementation. The runner SHALL expose `namespace` and `operatives()`, and SHALL support the context-manager protocol so `with flezha.create_runner(ns) as r:` performs graceful shutdown on exit.

At most one **started** runner SHALL exist per process; the slot is claimed at `start()`, not at create. Any number of unstarted runners MAY exist. A second `start()` on the same runner SHALL raise.

The runner SHALL report, in its startup line, the wheel build tier, the active executor and the pool sizing, so a benchmark or bug report is never ambiguous about the conditions it was produced under.

#### Scenario: Successful connection on a created runner
- **WHEN** a user calls `flezha.create_runner("my_app").define_stream(MarketOperative).start()` with the controller running
- **THEN** the call blocks until the controller responds with `AssignRunnerId`, returns the same runner, and `runner.namespace` equals `"my_app"`

#### Scenario: Controller unavailable triggers exponential backoff retries
- **WHEN** a user calls `start()` and the controller's control socket is not yet listening
- **THEN** the SDK SHALL retry the connect with exponential backoff and succeed once the controller becomes available, on the same ladder the other SDKs use

#### Scenario: Zero declared operative types is legal
- **WHEN** `start()` is called with no `define_stream` / `define_workflow` calls
- **THEN** the runner SHALL connect successfully with an empty factory id list, and types MAY be defined later on the live runner

#### Scenario: Second start on the same runner is refused
- **WHEN** `start()` is called twice on one runner
- **THEN** the second call SHALL raise and SHALL NOT dial the controller

### Requirement: Decorator-Based Authoring Surface

The Python SDK SHALL expose its authoring surface as decorators: `@flezha.state`, `@flezha.procedure`, `@flezha.stream_operative` and `@flezha.workflow_operative`. A decorated handler SHALL be an ordinary Python callable, callable directly in a unit test without any Flezha runtime present. Decorators SHALL register the decorated object into a module-level registry at import time and SHALL NOT connect, spawn threads, or perform I/O.

Declaration SHALL be a pure expansion onto the shared authoring contract's surface — introducing no new runtime path, protocol message or WAL change — and decorated procedures SHALL compose with one another in a single operative graph.

#### Scenario: A decorated handler is directly callable
- **WHEN** a user defines `@flezha.procedure def price(tick: Tick) -> Quote: ...` and calls `price(some_tick)` from a plain unit test with no runner started
- **THEN** the function executes as an ordinary Python function and returns a `Quote`

#### Scenario: Decorating performs no I/O
- **WHEN** a module containing `@flezha.procedure` and `@flezha.stream_operative` declarations is imported with no controller running
- **THEN** the import SHALL succeed, open no socket, and start no thread

### Requirement: Type Hints Drive Graph Wiring, And The Graph Is Statically Checkable

The Python SDK SHALL derive procedure input and output types from the type hints on the decorated handler's signature and SHALL use them to wire the operative graph. The package SHALL ship `py.typed` and a fully annotated public surface, such that a wrong edge type is reported by a standard type checker before the program runs.

A handler whose parameter or return annotation is missing, or whose annotated types cannot be connected into a valid graph, SHALL raise at the `define_stream` / `define_workflow` call site — before any input is served — with an error naming the offending handler and the specific defect. Registration-time refusals SHALL cover the defect classes Rust catches at compile time: wrong input type on a wired edge, missing entry channel, multiple entry channels, graph cycle, unreachable procedure, node identity collision, missing event-time extractor when an event-time clock is declared, unservable clock, keyed channel without a plugin wire, tier channel not keyed, checkpoint ack without the effectively-once tier, correction window without its preconditions, branch with too few arms, and duplicate plugin sink.

#### Scenario: A wrong edge type is a type error before it is a runtime error
- **WHEN** a user wires a handler returning `Invoice` into a handler accepting `Shipment` and runs a standard type checker
- **THEN** the type checker SHALL report the mismatch, naming both handlers

#### Scenario: Mismatched types are refused at registration
- **WHEN** the same graph is registered without a prior type-check pass
- **THEN** `define_stream` SHALL raise a topology error naming both handlers and the incompatible types, and the runner SHALL NOT connect

#### Scenario: Missing annotation is refused at registration
- **WHEN** a user declares `@flezha.procedure def handle(order): ...` with no parameter annotation
- **THEN** `define_stream` SHALL raise an error naming the handler and the unannotated parameter

#### Scenario: Refusal happens before any input is served
- **WHEN** an operative graph contains a cycle
- **THEN** the error SHALL be raised from the `define_stream` call site, and no entity, mailbox or connection SHALL have been created

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
- **WHEN** an Entity's state is rebuilt from its snapshot and recorded change log
- **THEN** the resulting state SHALL equal the live state at the moment of the last recorded change

### Requirement: Deterministic Time and Identity With Ambient-Use Warnings

`ctx.now()` SHALL return the invocation's time value, computed once by the runtime from the procedure's declared clock before the handler executes, returning that same value for every call within the invocation. `ctx.stable_id()` SHALL return a deterministic, replay-stable 128-bit identifier derived from the entity identity, the invocation's durable change seq, and a per-invocation call ordinal; repeated calls within one invocation SHALL differ, and the n-th call of a replayed invocation SHALL equal the n-th call of the original. Both SHALL be computed by the engine, so Python values are bit-identical to the other SDKs' without a Python-side implementation.

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

`ctx.emit(value)` SHALL route to the procedure's declared output channel; `ctx.emit(channel, value)` SHALL select a channel explicitly. Emitting SHALL respect the keyed-channel rules: a channel declared keyed SHALL require a key, a channel not declared keyed SHALL refuse one, and both refusals SHALL name the channel. Metadata on a pure inter-procedure channel with no plugin sink SHALL be refused.

User-supplied metadata keys in the reserved `flz.` namespace SHALL be rejected with an error naming the key, so identity stamps can neither be spoofed nor clobbered. The runtime SHALL stamp `flz.entity`, `flz.seq`, and on keyed emits `flz.key`, onto plugin-bound frames, applied after user metadata.

#### Scenario: Keyed channel requires a key
- **WHEN** a handler emits without a key on a channel declared keyed
- **THEN** the SDK SHALL raise an error naming the channel

#### Scenario: Reserved metadata is refused
- **WHEN** a handler emits with the metadata key `flz.seq`
- **THEN** the SDK SHALL raise an error naming `flz.seq`

### Requirement: Explicit Module Registration

The Python SDK SHALL require users to name the modules that declare operatives, and SHALL NOT perform implicit package scanning or filesystem discovery. A named module that cannot be imported SHALL raise an error naming the module and chaining the underlying import error.

#### Scenario: Named modules are imported and registered
- **WHEN** a user names two modules containing `@flezha.stream_operative` declarations
- **THEN** both SHALL be imported, their declarations registered, and both factory ids SHALL appear in the handshake

#### Scenario: An unimportable module fails loudly
- **WHEN** a named module raises `ImportError` on import
- **THEN** the SDK SHALL raise an error naming the module and chaining the original `ImportError`

### Requirement: Versioned State and Change Encoding, Never Pickle

Change and state bytes produced by the Python SDK SHALL use a schema-aware, versioned MessagePack encoding supporting additive field evolution: state written by an older declaration SHALL remain loadable after new fields with defaults are added. The encoding SHALL be author-declared through one codec protocol, with built-in adapters for Pydantic models, dataclasses and attrs classes, and a hand-written escape for anything else.

The SDK SHALL NOT use `pickle` for durable state or change bytes, on either the persistence or the wire path, because loading a pickle executes code and its payloads embed class paths that a refactor invalidates. A test SHALL assert no `pickle` use survives on those paths.

Encoding SHALL be validated at registration: a `@flezha.state` class containing a field the codec cannot represent SHALL raise at the `define_stream` call site naming the class and field. Integer offsets, seqs and timestamps SHALL round-trip without narrowing, including values above 2^63.

#### Scenario: Added field remains loadable
- **WHEN** state is persisted, a new field with a default is added to the `@flezha.state` class, and the Entity is restored
- **THEN** restore SHALL succeed and the new field SHALL take its default

#### Scenario: Unrepresentable field is refused at registration
- **WHEN** a `@flezha.state` class declares a field holding an open socket
- **THEN** `define_stream` SHALL raise naming the class and the field

#### Scenario: Pickle cannot be reintroduced
- **WHEN** the persistence and wire paths are inspected by the SDK's own test suite
- **THEN** no `pickle` use SHALL be present, and a change that introduces one SHALL fail that test

### Requirement: Dynamic Factory Declaration

`define_stream` / `define_workflow` and their removal inverses SHALL be legal both before and after `start()`. The declared factory set SHALL be per-runner and SHALL start empty each generation. A post-start declaration SHALL reach the core immediately, so the set the reconnect init frame is serialized from at send time carries it; that frame is the resync of record. Removing a type with live instances SHALL raise; removing an undeclared type SHALL raise naming the id.

Shipping an `UpdateFactories` frame to a *live* controller is out of reach for a veneer host: the ABI exposes no entry point that emits one, and none that retracts a declaration, so only the in-process Rust host — which owns the runner channel directly — can notify a live controller of a post-start change. The Java SDK carries the identical gap. Closing it is a change to `sdk-core-abi`, owed to the follow-up `sdk-abi-factory-resync`; until it lands, a veneer host's post-start change reaches the controller at the next bind.

#### Scenario: Post-start define reaches the core and the next bind
- **WHEN** `define_stream` is called on a started runner
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
ordered links of each chain, merges, branches, class flags and procedure facts — into the
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

