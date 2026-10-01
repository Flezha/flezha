# rust-sdk-tasklet-macros Specification

## Purpose

The rust-sdk-tasklet-macros capability is the Rust realization of the SDK authoring contract. It provides the `#[flezha::tasklet]` attribute macro, which completes the author's `impl Tasklet` block with the items it leaves out (name, settings, hook flags, clock, default state and types) and generates no type, the `#[flezha::operator]` / `#[flezha::operator]` short forms on a stateless function or an `impl Tasklet` block, and the `flezha::operator!` declarative macro for linear pipelines, each naming the type the author registers explicitly. All macros expand to the existing `Operator`/`Tasklet`/`Change`/`Channel` traits with zero engine, protocol, or WAL change; macro-generated and hand-written tasklets compose in the same operator.
## Requirements
### Requirement: `#[flezha::tasklet]` attribute macro

The Rust SDK SHALL provide a `#[flezha::tasklet]` attribute macro applied to an `impl Tasklet for T` block, where `T` is the author's own type. It SHALL generate no type: it SHALL only add to that block the items the author did not write, namely the `NAME` const from the required `name` key, the setting items for the keys written, the `COMPENSATE_OVERRIDDEN`, `CELLS_FOR_OVERRIDDEN`, `MAX_CELLS_OVERRIDDEN` and `MAILBOX_CAPACITY_OVERRIDDEN` flags from the matching `fn` the block writes (`compensate`, `cells_for`, `max_cells`, `mailbox_capacity`) or the matching key (`max_cells`, `capacity`), the `CLOCK` const, `init_state` returning `Default::default()` when none is written, and `State = ()`, `Change = NoChange` and `Output = ()` when not written. An item the block writes that the attribute would also add, and an unknown key, SHALL fail to compile naming it. The attribute SHALL NOT accept `kind`, `sink`, `input` or `on_full`, which describe an operator and its entry channel, and SHALL fail to compile naming the key and directing the author to `#[flezha::operator]` when one is written. The attribute on a function SHALL fail to compile, directing the author to the `impl` block.

#### Scenario: A stateless tasklet writes only its types and run

- **WHEN** `#[flezha::tasklet(name = "echo")]` annotates `impl Tasklet for Echo` writing `type Input = String`, `type Output = String` and `run`
- **THEN** the block compiles with `State = ()`, `Change = NoChange`, `NAME = "echo"`, a default `init_state` and the wall clock, and no type other than `Echo` exists for it

#### Scenario: A stateful tasklet names its state and change

- **WHEN** a `#[flezha::tasklet(name = "quiet_symbol")]` block writes `type State = LastTrade` and `type Change = Traded`
- **THEN** `ctx.state()` reads a `LastTrade` and `ctx.change` takes a `Traded`, and `init_state` returns `LastTrade::default()`

#### Scenario: The author's type is referenceable in channels

- **WHEN** a tasklet is declared on `struct StaticEntry` with `#[flezha::tasklet]`
- **THEN** `StaticEntry` itself can be named in a `channels()` body of an operator of either kind via `.from`/`.into`

#### Scenario: A written hook sets its flag

- **WHEN** a `#[flezha::tasklet]` block writes `fn compensate`
- **THEN** `COMPENSATE_OVERRIDDEN` SHALL be `true` for the tasklet, and the runtime SHALL run it on a rollback

#### Scenario: An operator key on a tasklet is refused

- **WHEN** `#[flezha::tasklet(name = "do_nothing", sink = "echo")]`, or the same with `kind`, `input` or `on_full`, annotates an `impl Tasklet` block
- **THEN** the macro fails to compile naming the key and the short-form attribute that carries it

#### Scenario: The attribute on a function is refused

- **WHEN** `#[flezha::tasklet(name = "echo")]` annotates `fn echo(input: String) -> String`
- **THEN** the macro fails to compile, directing the author to `impl Tasklet` or to the short-form attributes

### Requirement: Attribute opt-ins for cell and mailbox behavior

The `#[flezha::tasklet]` macro SHALL accept keys that set the corresponding `Tasklet` items without the author writing them: `keys`, `capacity`, `max_cells`, `idle_ttl_seconds`, `schema_version`, `min_readable_schema_version`, `compact_after_bytes`, `max_timers_per_cell` and `logical_clock`. `cells_for` SHALL be a hook the block writes, not a key.

#### Scenario: Static cell keys and cells_for

- **WHEN** `#[flezha::tasklet(name = "static_entry_tasklet", keys = ["a","b","c","d"])]` annotates a block that writes `fn cells_for`
- **THEN** the tasklet's `cell_keys()` are the listed keys, `CELLS_FOR_OVERRIDDEN` is `true`, and inputs are routed by the written `cells_for`

#### Scenario: Mailbox capacity

- **WHEN** a `capacity` key is supplied
- **THEN** the tasklet's `mailbox_capacity()` returns it and `MAILBOX_CAPACITY_OVERRIDDEN` is `true`

#### Scenario: Logical clock

- **WHEN** `#[flezha::tasklet(name = "seq", logical_clock)]` annotates a block with a non-`NoChange` change
- **THEN** the tasklet's `CLOCK` is `Logical`

#### Scenario: Dynamic-mode knobs

- **WHEN** `#[flezha::tasklet(name = "rsi", idle_ttl_seconds = 5, max_cells = 10)]` is used
- **THEN** the tasklet's `cell_idle_ttl()` and `max_cells()` return those values, `cell_keys()` is empty, and the tasklet declares no key of its own, because the key is `key_by` on the wiring

#### Scenario: The attribute offers no dynamic key hook

- **WHEN** a `#[flezha::tasklet]` declares a `cell_key` key
- **THEN** the macro fails to compile with a message directing the author to declare `.key_by(f)` on the stream that feeds the tasklet

### Requirement: `flezha::operator!` linear-pipeline macro

The Rust SDK SHALL provide a `flezha::operator!` declarative macro that declares a named operator type for a strictly linear pipeline and lowers to a normal `channels()` body. The macro SHALL begin with the type it declares, `pub struct Name;` (any visibility), and that type SHALL implement `Operator` so it is registered with `runner.register_factory(Name)`. The pipeline's `input` head SHALL accept an optional channel name in the same position the `plugin` terminator takes one, and SHALL lower to the entry channel name `input` when none is written.

#### Scenario: Linear pipeline removes the channels block

- **WHEN** `flezha::operator!{ pub struct StaticCell; id: "static_cell", pipeline: input::<String> -> StaticEntry -> StaticDownstream -> plugin::<Vec<u8>>("test_kafka") }` is written
- **THEN** `StaticCell` implements `Operator` wiring input → `StaticEntry` → `StaticDownstream` → the `test_kafka` plugin, whose entry channel is named `input`, and `runner.register_factory(StaticCell)` registers it

#### Scenario: The pipeline head names its entry channel

- **WHEN** a pipeline head is written `input::<String>("prices")`
- **THEN** the generated operator's entry channel SHALL be named `prices`

#### Scenario: Non-linear graph is rejected

- **WHEN** an `operator!` pipeline expresses fan-out or multi-input
- **THEN** the macro fails to compile with a message directing the author to an explicit `channels()` body

#### Scenario: An empty entry channel name is rejected

- **WHEN** a pipeline head is written `input::<String>("")`
- **THEN** the macro fails to compile, stating that an entry channel name may not be empty

### Requirement: Sugared and raw tasklets compose

An operator SHALL be able to mix tasklets declared with `#[flezha::tasklet]` and tasklets whose `impl Tasklet` block states every item by hand.

#### Scenario: Mixed operator builds and runs

- **WHEN** an operator wires a `#[flezha::tasklet]` tasklet to a tasklet whose `impl Tasklet` writes `NAME`, `CLOCK`, its flags and `init_state` by hand
- **THEN** the operator compiles, registers, and processes input through both tasklets correctly

### Requirement: `#[flezha::operator]` short-form attribute

The Rust SDK SHALL provide one attribute macro applied to a single `run` function that declares a whole single-tasklet operator, running as its `run` key says (`flow` when unwritten): `#[flezha::operator]`. It SHALL require an `id`, SHALL accept an optional `sink` naming a plugin channel, an optional `input` naming the entry channel, naming it `input` when the key is absent, and an optional `on_full` setting that entry channel's overflow policy. `#[flezha::operator]` SHALL additionally accept an optional `each` key taking `"once"`, `"per_input"` or `"chained"`, which the generated operator SHALL return from `each()`, and SHALL fail to compile naming any other value; `#[flezha::operator]` SHALL refuse the key naming the steps attribute. No other class-level setting SHALL be a key of either attribute. The kind SHALL NOT be expressible as a key. Two attributes are taken rather than one `#[flezha::operator]` because `flezha::operator!` is already a `#[proc_macro]` of that name in the same module (`sdk/rust/macros/src/lib.rs:42`).

Each attribute SHALL take one of two targets:

With a `sink`, the generated topology SHALL wire the entry channel into the tasklet and the tasklet to that plugin sink. With no `sink`, it SHALL wire the entry channel into the tasklet and declare no plugin channel. A `sink` on a run function with a unit return and no `emits` SHALL fail to compile with a message beginning `topology.sink_without_output`.

#### Scenario: A flow short form emits an operator and a tasklet

- **WHEN** `#[flezha::operator(id = "do_nothing", sink = "echo")]` annotates `fn do_nothing(input: String) -> Vec<u8>`
- **THEN** the macro generates `DoNothing` implementing `Operator` with `factory_id()` `do_nothing` and a `channels()` body wiring `input` into `DoNothingTasklet` and `DoNothingTasklet` to the `echo` plugin sink, and `DoNothingTasklet` implementing `Tasklet` with `Input = String`, `Output = Vec<u8>`, `State = ()`, `Change = NoChange` and `NAME` `do_nothing`

#### Scenario: The operator takes the author's name

- **WHEN** a function short form is registered
- **THEN** `runner.register_factory(DoNothing)` SHALL register the operator, and `DoNothing` SHALL NOT implement `Tasklet`

#### Scenario: A steps short form implements the steps operator trait

- **WHEN** `#[flezha::operator(id = "place", sink = "placed", input = "order")]` annotates `fn place(order: String) -> String`
- **THEN** `Place` implements `Operator` with `factory_id()` `place` and a `channels()` body wiring `order` into `PlaceTasklet` and `PlaceTasklet` to the `placed` plugin sink, every other steps item at its default, and `runner.register_factory(Place)` registers it

#### Scenario: A steps short form declares its loop mode

- **WHEN** `#[flezha::operator(id = "chat", sink = "replies", each = "per_input")]` annotates `fn chat(message: String) -> String`, `Chat` is registered, and two messages reach its entry channel
- **THEN** `Chat::each()` is `Each::PerInput`, the tasklet runs once per message, and the instance does not complete after the first

#### Scenario: An unknown loop mode does not compile

- **WHEN** `#[flezha::operator(id = "chat", each = "forever")]` annotates a run function
- **THEN** the macro fails to compile naming `each` and the three accepted values

#### Scenario: A short form with no sink wires only the entry channel

- **WHEN** `#[flezha::operator(id = "counter", input = "ticks")]` annotates `impl Tasklet for Counter` with `type Input = String`
- **THEN** `Counter`'s `channels()` body wires `ticks` into `Counter` and declares no plugin sink, and the operator registers and runs

#### Scenario: A stateful short form sits on the impl block

- **WHEN** `#[flezha::operator(id = "store_input", sink = "sink", max_cells = 10)]` annotates `impl Tasklet for StoreInput` with `type State = StoreInputState` and `type Change = Stored`
- **THEN** `StoreInput` implements both `Tasklet` and `Operator`, its tasklet is named `store_input` with cells capped at 10, and `runner.register_factory(StoreInput)` registers it with no generated type

#### Scenario: A function short form with state is refused

- **WHEN** `#[flezha::operator(id = "store_input")]` annotates `fn store_input(input: String, state: &mut S) -> String`
- **THEN** the macro fails to compile with a message beginning `ShortFormStateOnFunction`

#### Scenario: The generated tasklet is nameable in another topology

- **WHEN** `EchoTasklet` is named in a hand-written `channels()` body of a different operator
- **THEN** it wires as any tasklet does, and `Echo`'s own operator is declared only if `Echo` is also registered

#### Scenario: The attribute keeps the default entry channel name

- **WHEN** `#[flezha::operator(id = "do_nothing", sink = "echo")]` declares no `input` key
- **THEN** the operator's entry channel SHALL be named `input`

#### Scenario: The attribute names the generated entry channel

- **WHEN** `#[flezha::operator(id = "do_nothing", sink = "echo", input = "prices")]` annotates a run function
- **THEN** the operator's entry channel SHALL be named `prices`

#### Scenario: A sink on a unit return is refused

- **WHEN** `#[flezha::operator(id = "drain", sink = "out")]` annotates a run function with a unit return and no `emits`
- **THEN** the macro fails to compile with a message beginning `topology.sink_without_output`

#### Scenario: An empty entry channel name is rejected

- **WHEN** `#[flezha::operator(id = "do_nothing", sink = "echo", input = "")]` annotates a run function
- **THEN** the macro fails to compile, stating that an entry channel name may not be empty

#### Scenario: A topology body beside a short form does not compile

- **WHEN** a type carrying a short-form attribute also has a hand-written `Operator` impl
- **THEN** compilation fails, and the compile-fail fixture pinning it cites `topology.short_form_with_topology`

### Requirement: The Rust operator trait carries one body and one settings tree

`Operator` SHALL declare `fn channels(c: &ChannelBuilder);`, required, and SHALL declare no `tasklets`, `retry_attempts`, `retry_backoff`, `barrier_retry_attempts` or `wake_after` item; its items SHALL be `factory_id`, `channels` and `settings` (an `OperatorSettings` holding `run`, `durable`, `max_duration` and `description`). The tier, its checkpoint and its correction window SHALL be members of `Run::Flow`, so a steps run cannot carry them, and `ZeroRetryBackoff` SHALL NOT exist. A steps cell SHALL write no effect record: the collecting path SHALL carry no effectively-once branch, and `steps_metadata` in the shared core SHALL advertise no `effectively_once` for a steps class. The step ladder SHALL take the consuming edge's `RetryPolicy` from the partition's dispatcher, as the stream ladders do, and the barrier re-send count SHALL be a constant beside the acknowledgement timeout. The `TaskletCtx` a steps cell's `run` and `on_timer` receive SHALL carry the same timer session a flow cell's does, so `timer_after`, `timer_at_event_time`, `cancel_processing_time_timer` and `cancel_event_time_timer` answer as in a flow operator; only a `compensate` hook's context SHALL answer `TaskletError::TimersUnavailable`. `sdk/rust/public-api.txt` SHALL record the new signature and none of the removed items.

#### Scenario: A steps operator writes channels and no retry knob

- **WHEN** a type implements `Operator` with `fn channels(c: &ChannelBuilder)` wiring `c.input::<String>("in").retry_max(2).into(Flaky)` and `Flaky` fails on every attempt
- **THEN** the class registers, the cell runs exactly twice before the instance aborts and compensates, and a type writing `fn retry_attempts() -> u32` in the same impl fails to compile

#### Scenario: The tier on a steps operator does not compile

- **WHEN** an `Operator` writes `Run::Steps { .. }` with an `exactly` member, or calls `c.effectively_once()` in `channels`
- **THEN** compilation fails, neither member nor builder call existing, and no `AddOperatorResponse` for a steps class could carry `exactly`

#### Scenario: A stale body does not compile

- **WHEN** a type implements `Operator` with `fn tasklets(c: &ChannelBuilder)` and no `channels`
- **THEN** compilation fails naming the missing `channels` and the unknown `tasklets`

#### Scenario: The retired wake item does not compile

- **WHEN** a type implements `Operator` with `fn wake_after() -> Option<Duration>`
- **THEN** compilation fails naming `wake_after` as not a member of the trait

#### Scenario: A timer inside a steps tasklet fires

- **WHEN** a tasklet of a running `PerInput` steps operator calls `ctx.timer_after(Duration::from_millis(300))` in `run`
- **THEN** the call answers `Ok(at)`, and `on_timer` runs on that cell with `fired_at == at`, opening a pass at that tasklet

