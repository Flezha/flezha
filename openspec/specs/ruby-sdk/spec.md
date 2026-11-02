# ruby-sdk Specification

## Purpose

The Ruby SDK, a poll host over `flz_core`. Its authoring surface is class based with explicit
registration and carries the shared vocabulary at full parity, with state and changes serialized
through author-declared `to_h`/`from_h` over MessagePack and unsigned 64-bit values kept intact. Its
default host is a fetch-and-run thread pool with a starvation supervisor; every blocking core call
releases the GVL, it refuses use after a fork, and per-VM state is Ractor-local. It is a feature of
`flz_core_ffi` shipped as precompiled platform gems or a source gem built at the pinned core tag,
enables YJIT and `Process.warmup`, negotiates the schema version before connecting, and is proven
through the shared conformance and end-to-end suites, its own unit-test workflow and a pinned
coverage library.

## Requirements

### Requirement: The Ruby SDK is a poll host over `flz_core`

The Ruby SDK SHALL drive `flz_core` through the poll boundary and SHALL contain no kernel code: transport, framing, ordering, per-cell exclusivity, durability, retries, the retry→park ladder, timers, credit, routing, the steps driver and reconnect SHALL remain in the core. The SDK's responsibilities SHALL be limited to the authoring surface, the codec for user types, the threading host, and idiomatic errors.

#### Scenario: No kernel surface is reimplemented in Ruby
- **WHEN** the gem's source is inspected for a retry ladder, an outbox, a WAL writer, a credit window or a steps driver
- **THEN** none SHALL exist; every such behaviour SHALL be reached by calling the core

#### Scenario: The runner connects, serves and shuts down through the core
- **WHEN** a Ruby runner starts, binds a relay, processes input, fires a timer and receives a shutdown
- **THEN** every step SHALL be driven by `poll` / `complete` against the core, and the runner SHALL exit with in-flight activations completed or reported

### Requirement: Runner entry point and lifecycle

The SDK SHALL expose one entry point that declares classes, claims a runner slot and serves until shutdown. Declaration SHALL complete before the first `poll`. Shutdown SHALL be observable from a signal, from closed stdin (the e2e teardown contract), and from a controller-requested shutdown, and SHALL drain in-flight activations before the process exits.

#### Scenario: Declaration precedes serving
- **WHEN** the entry point is invoked with two declared relay classes
- **THEN** both class declarations SHALL reach the core before any activation is polled

#### Scenario: Closed stdin ends the runner
- **WHEN** the runner's stdin reaches EOF
- **THEN** the runner SHALL begin shutdown, drain in-flight activations, and exit zero

### Requirement: Class-based authoring surface with explicit registration

Relays and tasklets SHALL be authored as Ruby classes. The SDK SHALL NOT scan packages, walk load paths, or autoload to discover them; the author SHALL name the classes that participate. A declaration that cannot be resolved to a valid topology SHALL be refused at registration time with an error naming the offending class and method; registration-time refusal replaces the Rust SDK's compile-time refusal. A tasklet's body SHALL be its `run` method, the name every SDK gives a tasklet's body; a tasklet class defining `process` and no `run` SHALL be refused at registration naming `run`.

A value SHALL leave a tasklet only through its context: `ctx.emit` (with its `key:`, `channel:` and `metadata:` keywords) and by no second spelling (`ctx.emit_to` does not exist). The SDK SHALL ignore what `run`, `on_timer` and `compensate` answer and SHALL NOT emit it, and SHALL NOT refuse a non-nil answer, since every Ruby method answers its last expression. `Flezha::Ctx` SHALL NOT offer `emit_returned`, and `Flezha::Testing::TaskletTestHarness` SHALL report what a tasklet emitted and SHALL NOT report what it returned.

#### Scenario: An undeclared class never runs
- **WHEN** a tasklet class is defined but not registered
- **THEN** it SHALL NOT be declared to the core, and no activation SHALL reach it

#### Scenario: A bad topology is refused by name
- **WHEN** a registered tasklet emits to a channel no tasklet consumes
- **THEN** registration SHALL raise naming the class, the method and the channel; no controller connection SHALL be attempted

#### Scenario: A tasklet's body is its run method
- **WHEN** a `Flezha::Tasklet` subclass defines `run(input, ctx)` and a relay wires it
- **THEN** each input SHALL reach `run`, and the values `run` passes to `ctx.emit` SHALL be the tasklet's output

#### Scenario: The old body method is refused
- **WHEN** a registered tasklet class defines `process(input, ctx)` and no `run`
- **THEN** registration SHALL raise naming the class and `run`

#### Scenario: A returned value is not emitted
- **WHEN** a tasklet's `run` answers `"echo: hi"` without calling `ctx.emit`, and another tasklet's `run` ends in `ctx.change({ seen: 1 })`, each wired to an output
- **THEN** neither SHALL emit anything for that input, under the dispatcher and under `Flezha::Testing::TaskletTestHarness` alike

#### Scenario: A value emitted through the context leaves once
- **WHEN** a tasklet's `run` calls `ctx.emit("a")` and `ctx.emit("b", key: "k")` for one input and answers `"c"`
- **THEN** the input SHALL emit `a` and `b` keyed `k`, and SHALL NOT emit `c`

### Requirement: The authoring surface carries the shared vocabulary at full parity

The Ruby authoring surface SHALL carry both runs — flow and steps — on one `Flezha::Relay` base, and the shared vocabulary the reference SDK carries: the graph/wiring DSL with `key_by` and transforms, `ctx` emits, `ctx.now` and `ctx.stable_id`, cell state and change journaling, static and dynamic cell modes, per-cell timers, tiers including effectively-once, multi-input tasklets, tasklet-requested spawn, steps relay chains, loops, suspension and rewind. A capability the Ruby SDK does not carry SHALL be named as deferred in its conformance manifest together with the follow-up that owes it; silent absence SHALL be a defect.

#### Scenario: The manifest defers nothing at v1
- **WHEN** `test/apps/ruby_runner/conformance.json` is compared against the Rust reference manifest
- **THEN** its `deferred` list SHALL be empty and its fixture set SHALL equal the reference's

#### Scenario: A steps class runs a durable chain
- **WHEN** a Ruby steps relay runs a chained loop with a suspension and is resumed after a runner restart
- **THEN** the journal SHALL drive resume without re-running a committed tasklet

#### Scenario: Two channels merge into one tasklet
- **WHEN** a relay declares `c.merge.from(ticks) { |t| … }.from(quotes) { |q| … }.key_by { |v| v[:symbol] }.into(Detector)`
- **THEN** registration SHALL succeed with one Detector node fed by two consumer edges, each applying its own mapping, a value from either entry SHALL reach `run` as the mapped value keyed by `symbol`, a duplicated source SHALL refuse with `topology.duplicate_merge_source` and a single source with `topology.merge_too_few_sources`

#### Scenario: One input becomes three deliveries
- **WHEN** an edge declares `.flat_map { |line| line.split(",") }` into a tasklet and the dispatcher is handed `a,b,c`
- **THEN** `run` SHALL run three times, once per element, under one consumed offset, and feeding an empty line SHALL run it zero times

#### Scenario: The latest-document router is offered
- **WHEN** `c.route_latest(config, trades) { |t| t[:symbol] }` is declared and `Flezha::Testing::TaskletTestHarness` feeds a document, trades for `A` and `B`, the same document, then a trade for `A`
- **THEN** exactly one `["A", document]` and one `["B", document]` SHALL be emitted, the repeated document SHALL be inert, and `route_latest_named` SHALL take a node name

### Requirement: State and Change serialize through author-declared `to_h` / `from_h` over MessagePack

State and Change types SHALL serialize through author-declared `to_h` and `from_h` methods encoded as MessagePack, carrying an author-controlled schema version. The SDK SHALL NOT use `Marshal` for any persisted or wire-bound value. The SDK SHALL raise at registration time when a declared State or Change type does not implement both methods.

#### Scenario: `Marshal` is not reachable for persisted state
- **WHEN** the gem's persistence and wire paths are inspected
- **THEN** no call to `Marshal.dump` or `Marshal.load` SHALL appear on them

#### Scenario: A class rename does not break restore
- **WHEN** a State class is renamed and its `to_h` / `from_h` shape is unchanged
- **THEN** a Cell restored from a WAL written before the rename SHALL fold successfully

#### Scenario: A missing codec method is refused at registration
- **WHEN** a State type declares `to_h` but not `from_h`
- **THEN** registration SHALL raise naming the type and the missing method

### Requirement: Ruby integers carry the schema's unsigned 64-bit values without narrowing

Offsets, sequence numbers and timestamps crossing the boundary SHALL be represented as Ruby integers with no width policy, precision loss or float widening.

#### Scenario: A high-bit sequence number round-trips
- **WHEN** an activation carries a sequence number above 2^63
- **THEN** the value observed in Ruby SHALL equal it exactly, and the value returned in the result SHALL round-trip byte-identically

### Requirement: The Ruby builder submits its topology to the reference check

The Ruby SDK's builder SHALL record the authoring calls it receives — channels, the
ordered links of each chain, merges, branches, class flags and tasklet facts — into the
declaration record and SHALL take its verdict from the reference check, so that every name
of the `TopologyIssue` vocabulary is refused with the reference sentence and no rule is
carried in the builder. The class-based surface SHALL stay as it is; only the judgement moves.
The SDK SHALL prove the set by one conformance run over every row of
`test/fixtures/reference_topologies.txt` through the row interpreter in its test app.

#### Scenario: A malformed row refuses with the reference name

- **WHEN** the Ruby interpreter authors a refusal row through the SDK's own builder
- **THEN** registration refuses with the row's name and the reference sentence

#### Scenario: A well-formed row derives to the fixture's bytes

- **WHEN** the Ruby interpreter authors an `ok` row
- **THEN** the derived declaration equals the row's lines

#### Scenario: A merge row runs through the SDK's own merge handle

- **WHEN** the Ruby interpreter reaches `merge_one_source` or `merge_same_twice`
- **THEN** it authors the row with `c.merge.from(...)…into(...)` rather than raising, registration refuses with `topology.merge_too_few_sources` or `topology.duplicate_merge_source`, and the manifest's `topology_surfaces` lists `merge`

### Requirement: The single-tasklet short form is a class-body declaration
The short form SHALL be a `Flezha::Relay` subclass, the same base a full-form relay subclasses, and SHALL NOT be a `Flezha::Tasklet` subclass; `Flezha::Tasklet` SHALL lose its `output` class-body declaration and SHALL raise naming the relay base when one is written. The class body SHALL declare `factory_id "name"`, as a full-form relay already does, and MAY declare `sink "name"`, `input "name"` and `codec :text`, each its own class-body call so that none rides another. How it runs is the `run` class setting, written as on the full form.

Such a class SHALL be a short form exactly when it defines `run` and not `channels`; defining both SHALL be refused, and defining neither SHALL be refused naming the missing body. `output` SHALL be optional. The class SHALL carry a generated relay whose factory id is `factory_id`, wiring an entry channel named `input` (or the `input` value) into a generated tasklet named `factory_id` and, when an `output` is declared, that tasklet to the `output` plugin channel; with no `output`, it SHALL wire the entry channel into the tasklet and declare no plugin channel. A short form SHALL take the class settings as the full form does; a steps short form SHALL be a `once` steps relay unless its `run` writes another `each`, fed by its first message on the entry channel and taking `init_values` as the tasklet's initial state. The entry SHALL be decoded by `codec` where one is declared, whether or not an `output` is.

`Runner#register_factory` SHALL accept such a class and register the generated relay; the refusals SHALL be raised there, at registration, as the Ruby contract places every refusal: `Flezha::Error` whose code is `topology.short_form_with_topology`. A class with no `factory_id` SHALL be refused naming the required declaration, and SHALL NOT take a factory id from the class constant. The Ruby SDK cannot see a tasklet's output type, owes no `topology.output_without_type`, and SHALL NOT list `shorthand_typed_output` in its manifest. A duplicate factory id SHALL raise `Flezha::Error` with the code `topology.duplicate_factory_id`.

#### Scenario: A relay class with a run body is a whole flow relay

- **WHEN** `class Upcase < Flezha::Relay` declares `factory_id "upcase"`, `sink "out"`, `codec :text` and `run`, and the runner is given `register_factory(Upcase)`
- **THEN** the runner declares a flow relay `upcase` with entry channel `input` decoding text, a tasklet named `upcase` and output `out`, and no `Flezha::Tasklet` subclass exists for it

#### Scenario: A relay class with a run body is a whole steps relay

- **WHEN** `class Place < Flezha::Relay` declares `factory_id "place"`, `sink "placed"`, `input "order"`, `codec :text` and `run`, the runner is given `register_factory(Place)`, an instance is started, and one message is sent on `order`
- **THEN** `run` receives the message as text, the value it passes to `ctx.emit` reaches `placed`, and the instance completes

#### Scenario: A short-form steps relay declares its loop mode

- **WHEN** `class Chat < Flezha::Relay` declares `factory_id "chat"`, `sink "replies"`, `codec :text`, `each :per_input` and `run`, is registered, and two messages reach its entry channel
- **THEN** the declaration carries `each` `per_input`, `run` runs once per message, and the instance does not complete after the first

#### Scenario: A short form with no output is declared

- **WHEN** a class declares `factory_id "counter"`, `input "ticks"`, `codec :text`, `initial_state`, `apply_change` and `run`, with no `output`
- **THEN** the relay declares `ticks` into the generated tasklet and no plugin channel

#### Scenario: An input and codec need no output

- **WHEN** a class declares `input "ticks"` and `codec :text` and no `output`
- **THEN** `register_factory` accepts it and the entry channel takes that name and codec

#### Scenario: A short form's returned value is not emitted

- **WHEN** a short form declaring `sink "out"` defines `run(value, _ctx) = value.upcase` and a message reaches it
- **THEN** nothing reaches `out`, and a short form whose `run` calls `ctx.emit(value.upcase)` sends the upper-cased value to `out`

#### Scenario: A channels body beside a run body is refused

- **WHEN** a `Flezha::Relay` subclass defines both `channels` and `run`
- **THEN** `register_factory` raises `Flezha::Error` whose code is `topology.short_form_with_topology`

#### Scenario: An output on a tasklet is refused

- **WHEN** a `Flezha::Tasklet` subclass declares `sink "out"`
- **THEN** loading it raises naming `Flezha::Relay`

#### Scenario: A derived factory id is refused

- **WHEN** a `Flezha::Relay` subclass declares `sink "out"` and `run` and no `factory_id`
- **THEN** `register_factory` raises `Flezha::Error` naming the required `factory_id`, and no controller connection is attempted

### Requirement: A Ruby class carries the settings tree it wrote

`Flezha::Relay`'s class body SHALL take the class settings as the tree's own data (`run`, `durable`, `max_duration_ms`, `description`), and `to_host_class_decl` SHALL put the tree on the wire as written, so a setting under a parent that does not carry it reaches the core and is refused as `topology.unknown_field`, naming the setting and its parent, when the runner declares the class to the shared core (at start, or at the registration itself for a class registered after start), as every other core refusal in this SDK is raised, as a `Flezha::Error` naming it. `Flezha::Relay` SHALL declare no `retry_attempts`, `retry_backoff_ms`, `barrier_retry_attempts` or `wake_after_ms` setting, and the declaration SHALL carry no `wake_after_ms`. A `Ctx` inside a steps relay SHALL offer `timer_after`, `timer_at_event_time` and `cancel_timer` exactly as inside a flow relay, and a tasklet's `on_timer` SHALL run on its firing in both runs; only a `compensate` hook's `Ctx` SHALL raise an error naming `runtime.timers_unavailable` from them.

#### Scenario: A misplaced setting reaches the core

- **WHEN** a `Flezha::Relay` subclass writes `run flow: { each: :per_input }` and the runner declares it
- **THEN** declaring it to the core raises naming `topology.unknown_field`, `each` and `flow`, and the relay is not declared

#### Scenario: A retired setting is refused

- **WHEN** a `Flezha::Relay` subclass writes `retry_attempts 5` or `wake_after_ms 5000` in its class body
- **THEN** loading it raises `NoMethodError`, the setting no longer being declared

#### Scenario: A timer inside a steps tasklet fires

- **WHEN** a tasklet of a running `per_input` steps relay calls `ctx.timer_after(300)` in `run`
- **THEN** the call returns the scheduled timestamp, the effect carries the timer op, and `on_timer` runs on that Cell at that timestamp, opening a pass at that tasklet

### Requirement: A Ruby tasklet names itself and declares its settings in its class body
A Ruby tasklet SHALL be a class `< Flezha::Tasklet` whose class body writes `tasklet_name "..."`, a non-empty name. Declaring a relay that wires a tasklet class with no `tasklet_name`, or an empty one, SHALL raise `Flezha::Error` naming the class, and the SDK SHALL NOT derive a name from the class. `tasklet_name` SHALL NOT be inherited, so every concrete subclass writes its own. The declaration SHALL keep the name `tasklet_name` because `name` is `Class#name`. The short form SHALL keep naming its tasklet after its factory id.

The idle TTL SHALL be `idle_ttl_seconds`, a positive whole number of seconds written in the class body, and the declaration record SHALL carry it as `idle_ttl_ms`, that number times 1000; any other value SHALL be refused with `Flezha::Error` naming `idle_ttl_seconds` where the class body writes it. Unset SHALL mean no eviction. `max_cells` SHALL be a class-body setting of its own.

The clock SHALL follow from what the class writes: event time when it defines `event_time`, logical when its class body writes the `logical_clock` flag, and the wall clock otherwise. A class that does both SHALL be refused at registration with `Flezha::Error` naming `event_time` and `logical_clock`. A logical clock on a tasklet that has no state (it defines no `init_state` and declares no `state_type`) SHALL keep the core's `topology.unservable_clock` refusal.

A Cell's initial state SHALL be what `init_state(cell_key)` answers when the class defines it, else `state_type.new` when the class declares `state_type`, else `nil`. With `state_type` and no `init_state`, registration SHALL build one state and SHALL raise `Flezha::Error` naming the class, `state_type` and `init_state` when that raises. A class defining `apply_change` with neither `init_state` nor `state_type` SHALL be refused at registration, naming `apply_change`, `init_state` and `state_type`.

A change SHALL be any value the codec carries, recorded with `ctx.change(value)` and folded by the tasklet's `apply_change(state, change)`, and no change class SHALL be required. The optional `state_type` and `change_type` declarations SHALL stay. Schema migrations SHALL be the instance methods `migrate_state(from_version, bytes)` and `migrate_change(from_version, bytes)`, declared by defining them like every other hook, and SHALL keep receiving the stored bytes.

A tasklet class body SHALL NOT offer `dynamic_cells`, `clock`, `idle_ttl_ms`, or a `migrate_state` / `migrate_change` block. A class that defines the retired `initial_state` method SHALL be refused at registration with `Flezha::Error` naming `init_state`.

#### Scenario: A tasklet without a name is refused

- **WHEN** a class `< Flezha::Tasklet` writes `cell_keys "a"` and no `tasklet_name`, or is written with `Class.new(Flezha::Tasklet)` and no `tasklet_name`, and a relay wiring it is declared
- **THEN** declaring it raises `Flezha::Error` naming the class (or saying it is anonymous) and the missing `tasklet_name`, and nothing is declared

#### Scenario: A renamed class keeps its state

- **WHEN** a tasklet class writing `tasklet_name "totals"` is renamed between two runs, keeping the declaration
- **THEN** its Cells restore from the state saved under `totals`

#### Scenario: A subclass writes its own name

- **WHEN** a class inherits from a named tasklet class and writes `mailbox_capacity 8` and no `tasklet_name`
- **THEN** declaring it raises naming the subclass, and the parent's other declarations are still inherited by a subclass that does write a `tasklet_name`

#### Scenario: The idle TTL is whole seconds

- **WHEN** a tasklet fed by `key_by` writes `tasklet_name "sessions"` and `idle_ttl_seconds 3_600`
- **THEN** the declaration record carries `idle_ttl_ms` `3600000` for that tasklet

#### Scenario: A fractional idle TTL is refused

- **WHEN** a tasklet class body writes `idle_ttl_seconds 0.2`
- **THEN** loading it raises `Flezha::Error` naming `idle_ttl_seconds` and the value

#### Scenario: Defining event_time makes the clock event time

- **WHEN** a tasklet defines `event_time(input)` and its class body says nothing about the clock
- **THEN** the declaration carries clock `event_time`, and `ctx.now` inside `run` answers what `event_time` extracted from that input

#### Scenario: logical_clock declares the logical clock

- **WHEN** a tasklet defining `init_state` and `apply_change` writes `tasklet_name "ledger"` and `logical_clock`
- **THEN** the declaration carries clock `logical`, and `ctx.now` serves the Cell's change sequence

#### Scenario: Event time and the logical clock together are refused

- **WHEN** a tasklet defines `event_time` and writes `logical_clock`
- **THEN** registration raises `Flezha::Error` naming `event_time` and `logical_clock`

#### Scenario: A logical clock on a stateless tasklet is refused

- **WHEN** a tasklet that defines no `init_state` and declares no `state_type` writes `logical_clock`
- **THEN** registration raises `Flezha::Error` carrying the core's `topology.unservable_clock`

#### Scenario: The initial state comes from init_state

- **WHEN** a tasklet defines `init_state(cell_key)` and a Cell is created for key `A`
- **THEN** the Cell's state starts as what `init_state("A")` answers, and a single-cell tasklet's as what `init_state(nil)` answers

#### Scenario: state_type alone gives the initial state

- **WHEN** a tasklet declares `state_type Cart`, where `Cart.new` takes no argument, and defines no `init_state`
- **THEN** each new Cell's state starts as `Cart.new`, and a tasklet that also defines `init_state` starts from what `init_state` answers

#### Scenario: A state_type that cannot be built is refused

- **WHEN** a tasklet declares `state_type` naming a class whose `new` requires an argument, and defines no `init_state`
- **THEN** registration raises `Flezha::Error` naming the class, `state_type` and `init_state`

#### Scenario: A fold with no state is refused

- **WHEN** a tasklet defines `apply_change` and neither `init_state` nor `state_type`
- **THEN** registration raises `Flezha::Error` naming `apply_change`, `init_state` and `state_type`

#### Scenario: A change needs no class

- **WHEN** a tasklet calls `ctx.change({ kind: "event", deadline: at })` and its `apply_change` folds that Hash into a Hash state
- **THEN** the change is journaled as MessagePack, folded live, and folded again to an equal state on restore, with no class defined for it

#### Scenario: Migrations are methods that receive bytes

- **WHEN** a tasklet writing `schema_version 2` and `min_readable_schema_version 1` defines `migrate_state(from_version, bytes)`, and a Cell restores from a snapshot written at version 1
- **THEN** `migrate_state` is called with `1` and the stored bytes, its answer is the restored state, and a tasklet with that floor defining neither method is refused at registration

#### Scenario: A retired initial_state method is refused

- **WHEN** a tasklet class defines `initial_state(cell_key)` and a relay wiring it is declared
- **THEN** declaring it raises `Flezha::Error` naming `init_state`, rather than running the tasklet with no state

#### Scenario: A retired declaration is refused

- **WHEN** a tasklet class body writes `dynamic_cells`, `clock :event_time`, `idle_ttl_ms 1000`, or `migrate_state { |from, bytes| ... }`
- **THEN** loading it raises `NoMethodError` naming the declaration, the declaration no longer existing

### Requirement: A Ruby tasklet's cell mode is read from the wiring
The core SHALL derive each tasklet node's cell mode from what is written, and the Ruby SDK SHALL send none: static when the tasklet's class body writes `cell_keys`, dynamic when a chain of the relay keys its values with `key_by` before the `into` or merge that reaches the node, and single otherwise. The mode SHALL belong to the node, so one class MAY take a different mode in each relay that wires it. A tasklet writing `cell_keys` and fed by `key_by` SHALL be refused at registration with the core's `topology.key_by_on_unkeyable_target`. The record SHALL carry the tasklet's `idle_ttl_seconds` and `max_cells`, which a dynamic node honours.

#### Scenario: key_by alone makes a tasklet dynamic

- **WHEN** a relay declares `c.input("orders").key_by { |o| o[:customer] }.into(Counter)` and `Counter`'s class body writes only `tasklet_name "counter"`
- **THEN** the core SHALL derive dynamic mode for `counter`, and each customer SHALL get its own Cell whose key is the customer

#### Scenario: A merge keyed by key_by makes its tasklet dynamic

- **WHEN** a relay declares `c.merge.from(ticks) { |t| t }.from(quotes) { |q| q }.key_by { |v| v[:symbol] }.into(Detector)` and `Detector` writes no cell declaration
- **THEN** the core SHALL derive dynamic mode for `Detector`'s node

#### Scenario: One class takes its mode from each wiring

- **WHEN** one tasklet class is wired through `key_by` in one relay and without it in another
- **THEN** the core SHALL derive dynamic mode for it in the first relay and single in the second

#### Scenario: A static key set fed by key_by is refused

- **WHEN** a tasklet writing `tasklet_name "routed"` and `cell_keys "a", "b"` is reached by an edge declaring `key_by`
- **THEN** registration raises `Flezha::Error` carrying `topology.key_by_on_unkeyable_target` and naming the tasklet

### Requirement: The Ruby runner SHALL start and close with the shared verbs

`Runner#start` SHALL connect (`boot` does not exist) and `Runner#close` SHALL be the one public
shutdown verb (`drain` does not exist); the signal-safe trigger SHALL be private. The channel
builder SHALL be `Flezha::ChannelBuilder` and its stream handle `Flezha::Stream`;
`compact_after_bytes` SHALL be a tasklet setting; `Ctx#effect_record` and the readers of the
accumulated changes, outputs and timers SHALL NOT be public; and the harness SHALL offer one replay
gate, `replay_equals`, raising on a mismatch, with no `replay_equals!` or `replay_equals?`.

#### Scenario: A runner starts and closes

- **WHEN** application code calls `runner.start` then `runner.close` twice
- **THEN** the runner SHALL connect, then drain and release the core once

### Requirement: The default host is a fetch-and-run thread pool with a starvation supervisor

The SDK's default executor SHALL run a base pool of Ruby threads, each looping `poll → dispatch → complete` on itself, and one supervisor thread parked in the core pool governor's supervisor entry (`flz_core_pool_supervise`), which waits on the core's starvation signal and answers what to spawn. Each starvation observation SHALL start one additional worker, up to a ceiling; workers above the base SHALL retire after the governor's idle time, 30 s without work. The base SHALL default to the machine's processor count, passed to the governor as this SDK's default, and SHALL be configured by `FLEZHA_WORKER_POOL_SIZE`; the ceiling SHALL default to ten times the base, never below it, and SHALL be configured by `FLEZHA_WORKER_POOL_MAX`; the core reads both. The supervisor SHALL drain the core's notice queue on every answer. The SDK SHALL introduce no additional pool knob.

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

### Requirement: Every blocking core call releases the GVL

The binding SHALL release the global VM lock for the duration of every blocking core call — `poll`, the pool supervisor entry `pool_supervise`, `complete` and the steps relay inbox receive — so that no blocking core call prevents another Ruby thread from running.

#### Scenario: A blocked steps relay receive does not freeze the VM
- **WHEN** a steps stage waits on the inbox for several seconds
- **THEN** other Ruby threads, including fetchers, SHALL continue to execute Ruby code throughout the wait

#### Scenario: A blocked poll does not freeze the VM
- **WHEN** every worker is parked in `poll` with no work available
- **THEN** an unrelated Ruby thread SHALL run at full speed

### Requirement: The poll timeout is the documented interrupt granularity

Because shutdown wakes every poller and the ABI exposes no per-thread wake, worker threads SHALL bound each `poll` by a timeout, and that timeout SHALL be the documented upper bound on how quickly thread interruption, `Interrupt`, and runner close are observed. The value SHALL be documented alongside the guarantee.

#### Scenario: Interrupt is observed within the bound
- **WHEN** the process receives an interrupt while all workers are parked in `poll` with no work
- **THEN** shutdown SHALL begin within the documented poll timeout rather than waiting for work to arrive

### Requirement: The executor seam may never affect per-cell ordering

The SDK SHALL expose an executor seam with at least a thread executor (default) and a `Fiber::Scheduler` executor. Under every executor the thread that fetches SHALL be a real Ruby thread — never a fiber, and never borrowed from a shared I/O pool the application also uses — and an activation SHALL be completed only after its dispatch has returned. An executor SHALL NOT observe, reorder, batch or coalesce activations.

#### Scenario: Ordering holds under either executor
- **WHEN** the conformance suite runs under the thread executor and again under the fiber executor
- **THEN** per-cell input ordering and output identity SHALL be identical in both runs

#### Scenario: The fiber executor does not fetch on a fiber
- **WHEN** the fiber executor is active
- **THEN** `poll` SHALL be called from a real thread and activations SHALL reach fibers through a hand-off queue

#### Scenario: Completing from a different thread than the one that polled is supported
- **WHEN** an executor dispatches on a thread or fiber other than the fetcher and completes there
- **THEN** the completion SHALL be accepted and the cell released identically to same-thread completion

### Requirement: The SDK spawns no processes and refuses use after a fork

The SDK SHALL NOT fork, spawn or supervise processes; process count SHALL be the orchestrator's decision, and each process SHALL be one runner. The SDK SHALL record the process that started the core and SHALL raise, naming the rule, on any core call made from a different process. The documented entrypoint order SHALL be: declare classes, warm up, fork if the user forks at all, then start in the child.

#### Scenario: A core call after a fork is refused loudly
- **WHEN** a process forks after the core has been started and the child calls into the core
- **THEN** the SDK SHALL raise an error naming the fork rule, rather than wedging or silently failing to serve

#### Scenario: Fork-then-start is supported
- **WHEN** a user declares classes in the parent, forks, and starts the core in each child
- **THEN** each child SHALL serve as an independent runner

### Requirement: Per-VM SDK state is Ractor-local, and the Ractor executor is deferred by name

All per-VM SDK state — the binding handle, the class registry view and the active executor — SHALL be resolved per Ractor rather than through a process-wide singleton, so that a Ractor executor can be added without restructuring. The Ractor executor SHALL be recorded as deferred by name, with the follow-up change that owes it, in the SDK's documentation.

#### Scenario: No process-wide singleton holds the binding
- **WHEN** the SDK's state accessors are inspected
- **THEN** each SHALL resolve per Ractor, and none SHALL be a bare constant or global holding a mutable handle

#### Scenario: The deferral is named, not silent
- **WHEN** the SDK's documentation page is read
- **THEN** it SHALL name the Ractor executor as deferred and name the follow-up that owes it

### Requirement: The parallelism model is documented as arithmetic

The SDK documentation SHALL state, without hedging: Ruby-level handler execution is bounded to one core per process; everything below the poll seam runs with the GVL released and is genuinely parallel; work underway is capped by `FLEZHA_MAX_INFLIGHT_ACTIVATIONS` regardless of pool size; and the lever for CPU-bound handlers is process count, not thread count.

#### Scenario: The ceiling is stated, not implied
- **WHEN** a reader consults the Ruby SDK page for how to scale CPU-bound handlers
- **THEN** it SHALL name process count as the lever and SHALL state the one-core-per-process bound explicitly

### Requirement: The Ruby veneer is a feature of `flz_core_ffi`, not a separate crate

The Ruby binding SHALL be built from `flz_core_ffi` under a `ruby` cargo feature, beside the existing `jni` feature, as a one-to-one veneer over the same C entry points in the same crate. A second crate, a second ABI surface, or a Ruby-only reimplementation of an entry point SHALL be a defect.

#### Scenario: One crate exports both veneers
- **WHEN** `flz_core_ffi` is built with `--features ruby` and with `--features jni`
- **THEN** both SHALL produce the same C ABI plus their language veneer, from the same source crate

#### Scenario: The veneer adds no entry point of its own
- **WHEN** the Ruby veneer's exported functions are compared against the C entry points
- **THEN** each SHALL map one-to-one onto a C entry point, with no Ruby-only kernel path

### Requirement: Precompiled platform gems install without a Rust toolchain

The SDK SHALL publish a precompiled native gem per supported platform, each embedding the built native library, so that installation on a supported platform requires no Rust toolchain, no cargo invocation and no network fetch of Rust dependencies. A source gem SHALL remain available as the fallback and SHALL state its toolchain requirement. Cross-built gems SHALL be produced by CI on every change to the SDK or the core, and the gem CI installs SHALL be the gem the end-to-end arm runs.

#### Scenario: Installation on a supported platform needs no Rust
- **WHEN** the gem is installed on a supported platform in an image with no Rust toolchain
- **THEN** installation SHALL succeed and the runner SHALL start

#### Scenario: The published gem is the tested gem
- **WHEN** the end-to-end Ruby arm runs in CI
- **THEN** it SHALL run against the cross-built gem produced by the same steps relay, not against an ad-hoc local build

### Requirement: Schema version is negotiated before any controller connection

Context creation SHALL negotiate the activation schema version between the gem's veneer and the core and SHALL fail with an error naming both versions when they are incompatible, before any controller connection is attempted.

#### Scenario: A stale gem against a newer core fails at boot
- **WHEN** a gem built against schema version N loads a core speaking an incompatible N+1
- **THEN** startup SHALL fail naming both versions, and no connection SHALL be attempted

### Requirement: YJIT is enabled by default and reported at startup

The runner entry point SHALL enable YJIT when the running Ruby supports it and the user has not already decided, SHALL allow the user to override that decision, and SHALL report the resulting JIT state in the runner's startup line so a measurement can never be ambiguous about it.

#### Scenario: Default-on and reported
- **WHEN** the runner starts on a Ruby with YJIT available and no user override
- **THEN** YJIT SHALL be enabled and the startup line SHALL say so

#### Scenario: The user's decision wins
- **WHEN** the user has explicitly disabled YJIT
- **THEN** the SDK SHALL NOT enable it, and the startup line SHALL report it disabled

### Requirement: `Process.warmup` runs after declaration and before serving

The runner entry point SHALL call `Process.warmup` after class declaration completes and before the first `poll`, and the SDK documentation SHALL state that ordering together with the rule that the core must not be started before a fork.

#### Scenario: Warmup precedes the first poll
- **WHEN** the entry point runs
- **THEN** `Process.warmup` SHALL be called after the last class declaration and before any activation is polled

#### Scenario: The prefork ordering is documented in one place
- **WHEN** the SDK page's startup section is read
- **THEN** it SHALL give the order declare → warm up → fork → start, and SHALL state that starting the core before a fork is unsupported

### Requirement: The reference runner image documents jemalloc

The reference runner image SHALL use jemalloc and the SDK documentation SHALL name heap fragmentation under long-lived workers as the reason. jemalloc SHALL NOT be a hard runtime requirement of the gem.

#### Scenario: The image ships it and the gem does not require it
- **WHEN** the reference image is built and, separately, the gem is installed on a host without jemalloc
- **THEN** the image SHALL run with jemalloc preloaded, and the gem SHALL install and run without it

### Requirement: The gem is licensed and marked as SDK-side

The gem SHALL carry the SDK-side licence the other SDKs carry, SHALL be recorded in the repository's licensing manifest, and SHALL ship the third-party notices for the native code it embeds.

#### Scenario: Licensing is declared, not implied
- **WHEN** the licensing manifest and the built gem are inspected
- **THEN** the gem SHALL be recorded SDK-side, and the notices for the embedded native library SHALL travel inside the gem

### Requirement: The source gem builds from the wrapper crate at the pinned core tag

The gem's `extconf.rb` SHALL build the wrapper crate in the gem's `ext/`
directory, whose one dependency is `flz_core_ffi` at the pinned core tag with
the `ruby` feature, after trying the precompiled library first. It SHALL name
no path outside the gem. When neither the precompiled library nor `cargo` and
`git` are available it SHALL refuse naming both and the precompiled gem as
the alternative. The precompiled platform gems the packaging requirement
already mandates SHALL be produced by the core release job for every
supported triple and Ruby minor.

#### Scenario: A source install with a toolchain builds
- **GIVEN** cargo and git on the host and no precompiled library
- **WHEN** the source gem installs
- **THEN** `extconf.rb` SHALL build the wrapper at the pinned tag and the runner SHALL start

#### Scenario: A source install without a toolchain refuses usefully
- **GIVEN** no cargo on the host and no precompiled library
- **WHEN** the source gem installs
- **THEN** it SHALL refuse naming cargo, git and the precompiled gem for the platform

### Requirement: A Ruby test app joins the shared conformance suite without changing the harness

The change SHALL add `test/apps/ruby_runner/` with a `conformance.json` declaring every field the manifest requirement names, and SHALL add `ruby` as a runtime option of the nightly end-to-end steps relay. The suite SHALL admit the SDK with no change to `test/helpers/`, `test/system/`, `test/test_helper.exs` or the formatter, and with no new clause in the static gate: the gate's per-SDK tables — where an SDK spells its language byte, and which of its sources may name a connect variable — SHALL each gain one row, which is what admission means, and no rule SHALL be added, relaxed or special-cased for `ruby`.

#### Scenario: The app is admitted by manifest and one gate row
- **WHEN** the Ruby test app and its manifest are added
- **THEN** `mix test` SHALL pass with no edit to the harness, the system tests or the formatter, and the gate's diff SHALL be one row per per-SDK table

#### Scenario: A missing manifest field fails the gate
- **WHEN** the manifest omits a required field or names a capability outside the vocabulary
- **THEN** the static gate SHALL fail naming `ruby` and the offending field

### Requirement: The Ruby manifest matches the reference for what it carries and names what it defers

For every capability the Ruby manifest does not defer, its fixture set SHALL equal the Rust reference manifest's. Fixture ids SHALL be the reference's ids, unprefixed, and each SHALL implement the same scenario with the same output channel names, output bytes and declared options. Every deferred capability SHALL be named in `deferred`, and the SDK's documentation SHALL name the follow-up change that owes it.

#### Scenario: Fixture parity with the reference
- **WHEN** the gate compares `test/apps/ruby_runner/conformance.json` against `test/apps/rust_runner/conformance.json`
- **THEN** every non-deferred capability's fixture set SHALL be equal, and no Ruby-prefixed fixture id SHALL exist

#### Scenario: A deferral is named, not silent
- **WHEN** the Ruby manifest defers a capability the reference carries
- **THEN** it SHALL appear in `deferred` and the follow-up that owes it SHALL be named in the SDK's documentation page

#### Scenario: A capability added later fails Ruby red until carried
- **WHEN** a later change adds a vocabulary capability or a reference fixture
- **THEN** the gate SHALL fail for `ruby` until the Ruby app carries it or defers it by name

### Requirement: The end-to-end suite runs green under the Ruby runtime

`FLEZHA_E2E_RUNTIME=ruby` SHALL run the system suite against the Ruby runner over every transport the manifest lists, and every non-deferred capability's tests SHALL pass. The runner SHALL honour the stdin-EOF teardown contract the harness uses to stop runners.

#### Scenario: The full arm is green
- **WHEN** `FLEZHA_E2E_RUNTIME=ruby mix test --include system` is run
- **THEN** every test whose capability the manifest does not defer SHALL run and pass

#### Scenario: The declaration snapshot pins the Ruby declaration keys
- **WHEN** the smoke tier runs under `ruby`
- **THEN** the decoded class declaration SHALL match the golden snapshot, and an unspelled or misspelled declaration key SHALL fail it

### Requirement: The gem carries its own unit-test workflow

The SDK SHALL have a CI workflow running the gem's unit tests on every supported Ruby version, running the concurrency requirements' scenarios (pool growth and retirement, GVL release, fork refusal, executor ordering) as real tests rather than documentation, and cross-building the platform gems.

#### Scenario: Concurrency behaviour is tested, not asserted in prose
- **WHEN** the gem's unit suite runs
- **THEN** it SHALL include tests that observe pool growth under blocked handlers, retirement afterwards, a refused post-fork core call, and identical ordering under both executors

### Requirement: The Ruby SDK SHALL declare a pinned coverage library

The Ruby SDK's `flezha.gemspec` SHALL declare a coverage library as a development dependency at an exact version, alongside the `async`, `minitest`, `rake` and `rubocop` pins it already carries, so the gem's suite can report coverage from its own manifest.

The suite SHALL load it through the `Rakefile`'s existing test task rather than through a second task added beside it, because a coverage run and a plain run SHALL exercise the same suite definition.

#### Scenario: Coverage runs from the gemspec's development dependencies

- **WHEN** the gem's development dependencies are installed into a clean checkout and the suite is run in coverage mode
- **THEN** a coverage report SHALL be produced without installing a gem the gemspec does not name

#### Scenario: One test task, not two

- **WHEN** the `Rakefile` is read
- **THEN** the coverage run SHALL use the existing test task's suite definition, and SHALL NOT duplicate it
