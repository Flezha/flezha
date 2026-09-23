# rust-sdk-tasklet-macros Specification

## Purpose

The rust-sdk-tasklet-macros capability is the Rust realization of the SDK authoring contract. It provides the `#[flezha::tasklet]` attribute macro (signature-to-types inference, `&mut` state inference, cell/mailbox/clock opt-ins, static and dynamic modes), the `#[derive(flezha::State)]` derive (field-granular `Change` enum + `apply_change` journaled through the existing WAL), and the `flezha::operative!` declarative macro for linear pipelines, each naming the type the author registers explicitly. All macros expand to the existing `StreamOperative`/`Tasklet`/`Change`/`Channel` traits with zero engine, protocol, or WAL change; macro-generated and hand-written tasklets compose in the same operative.
## Requirements
### Requirement: `#[flezha::tasklet]` attribute macro

The Rust SDK SHALL provide a `#[flezha::tasklet]` attribute macro applied to a single `run` function that generates a complete `Tasklet` implementation conforming to the authoring contract. The attribute SHALL declare a tasklet and nothing else: it SHALL NOT accept `kind`, `sink`, `input` or `on_full`, which describe an operative and its entry channel, and SHALL fail to compile naming the key and directing the author to `#[flezha::stream_operative]` or `#[flezha::workflow_operative]` when one is written. The generated tasklet's `name()` SHALL be the `id` when one is written.

#### Scenario: Stateless tasklet collapses to a function

- **WHEN** `#[flezha::tasklet(id = "do_nothing")]` annotates `fn do_nothing(input: String) -> Vec<u8>`
- **THEN** the macro generates a `Default` tasklet type `DoNothing`, a `Tasklet` impl with `Input = String`, `Output = Vec<u8>`, `State = ()`, `Change = NoChange`, `Clock = WallClock`, `Config = ()`, `name`, `init_state`, and a `run` wrapper that emits the returned value, and no operative impl

#### Scenario: Stateful tasklet infers State from the mutable parameter

- **WHEN** `#[flezha::tasklet(id = "store_input")]` annotates `fn store_input(input: String, state: &mut StoreInputState) -> String`
- **THEN** the generated `Tasklet` impl sets `State = StoreInputState` and routes mutations through the existing state-handle path

#### Scenario: Generated tasklet type is referenceable in channels

- **WHEN** a tasklet is generated from `fn static_entry(..)`
- **THEN** a PascalCase tasklet type (e.g. `StaticEntry`) is produced that can be named in an explicit `channels()` body of an operative of either kind via `.from`/`.into`

#### Scenario: An operative key on a tasklet is refused

- **WHEN** `#[flezha::tasklet(id = "do_nothing", sink = "echo")]`, or the same with `kind`, `input` or `on_full`, annotates a run function
- **THEN** the macro fails to compile naming the key and the short-form attribute that carries it

### Requirement: Attribute opt-ins for cell and mailbox behavior

The `#[flezha::tasklet]` macro SHALL accept attribute opt-ins that map to the corresponding `Tasklet` overrides without dropping to a raw impl.

#### Scenario: Static cell keys and cells_for

- **WHEN** `#[flezha::tasklet(id = "static_entry_tasklet", keys = ["a","b","c","d"], cells_for = part)]` is used with a free function `part`
- **THEN** the generated impl sets `cell_keys()` to the listed keys, marks cell overridden, and delegates `cells_for` to `part`

#### Scenario: Mailbox capacity and overflow policy

- **WHEN** `capacity` and `on_full` attributes are supplied
- **THEN** the generated impl overrides `mailbox_capacity()` and the channel's overflow policy accordingly

#### Scenario: Clock override

- **WHEN** a `clock` attribute names a supported clock
- **THEN** the generated `Tasklet` uses that clock instead of `WallClock`

#### Scenario: Dynamic-mode knobs

- **WHEN** `#[flezha::tasklet(id = "rsi", idle_ttl = 5000, max_cells = 10)]` is used
- **THEN** the generated impl applies the `cell_idle_ttl()` / `max_cells()` overrides, leaves `cell_keys()` empty, and declares no key of its own — the key is `key_by` on the wiring

#### Scenario: The sugar offers no dynamic key hook

- **WHEN** a `#[flezha::tasklet]` declares a `cell_key` attribute
- **THEN** the macro fails to compile with a message directing the author to declare `.key_by(f)` on the stream that feeds the tasklet

### Requirement: `#[derive(flezha::State)]` change derivation

The Rust SDK SHALL provide a `#[derive(flezha::State)]` derive that generates a `Change` enum and `apply_change` for field-granular mutations, journaled through the existing WAL.

#### Scenario: Field mutations are journaled

- **WHEN** a state struct derives `flezha::State` and a `Vec` field is pushed to inside `run`
- **THEN** a generated change variant records the mutation and it is applied via the generated `apply_change`

#### Scenario: Unsupported mutation reports an error

- **WHEN** a mutation cannot be expressed as a field-granular change
- **THEN** the SDK requires a hand-written `Change` and the derive does not silently produce incorrect journaling

### Requirement: `flezha::operative!` linear-pipeline macro

The Rust SDK SHALL provide a `flezha::operative!` declarative macro that declares a named operative type for a strictly linear pipeline and lowers to a normal `channels()` body. The macro SHALL begin with the type it declares, `pub struct Name;` (any visibility), and that type SHALL implement `StreamOperative` so it is registered with `runner.register_factory(Name)`. The pipeline's `input` head SHALL accept an optional channel name in the same position the `plugin` terminator takes one, and SHALL lower to the entry channel name `input` when none is written.

#### Scenario: Linear pipeline removes the channels block

- **WHEN** `flezha::operative!{ pub struct StaticCell; id: "static_cell", pipeline: input::<String> -> StaticEntry -> StaticDownstream -> plugin::<Vec<u8>>("test_kafka") }` is written
- **THEN** `StaticCell` implements `StreamOperative` wiring input → `StaticEntry` → `StaticDownstream` → the `test_kafka` plugin, whose entry channel is named `input`, and `runner.register_factory(StaticCell)` registers it

#### Scenario: The pipeline head names its entry channel

- **WHEN** a pipeline head is written `input::<String>("prices")`
- **THEN** the generated operative's entry channel SHALL be named `prices`

#### Scenario: Non-linear graph is rejected

- **WHEN** an `operative!` pipeline expresses fan-out or multi-input
- **THEN** the macro fails to compile with a message directing the author to an explicit `channels()` body

#### Scenario: An empty entry channel name is rejected

- **WHEN** a pipeline head is written `input::<String>("")`
- **THEN** the macro fails to compile, stating that an entry channel name may not be empty

### Requirement: Sugared and raw tasklets compose

An operative SHALL be able to mix macro-generated tasklets and hand-written tasklets.

#### Scenario: Mixed operative builds and runs

- **WHEN** an operative wires a `#[flezha::tasklet]`-generated tasklet to a hand-written `impl Tasklet` tasklet
- **THEN** the operative compiles, registers, and processes input through both tasklets correctly

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

### Requirement: `Option<Output>` return in `#[flezha::tasklet]`

`#[flezha::tasklet]` SHALL accept a `run` function returning `Option<Output>` (or `Result<Option<Output>, TaskletError>`), emitting on `Some` and emitting nothing on `None`. The existing unit return (no output) and unconditional `Output` return (always emit) SHALL be unchanged.

#### Scenario: Optional return drives conditional emit

- **WHEN** a `#[flezha::tasklet]` `run` returns `Option<Output>`
- **THEN** the generated `run` emits via the tasklet context on `Some` and emits nothing on `None`, without the author referencing `TaskletCtx`

#### Scenario: Generated behaviour equals hand-written

- **WHEN** a tasklet authored with map-aware `#[derive(flezha::State)]` + `#[flezha::tasklet]` returning `Option<Output>` is expanded
- **THEN** its behaviour is identical to the hand-written `Tasklet`/`Change` impl, and `TaskletTestHarness::replay_equals` passes

### Requirement: `init` opt-in on `#[flezha::tasklet]`

`#[flezha::tasklet]` SHALL accept an `init = fn` argument naming a function `fn(Option<&str>) -> State`. When present, the generated `Tasklet::init_state` SHALL delegate to it, passing the cell key. When absent, `init_state` SHALL keep returning `State::default()` (stateful) or unit (stateless), unchanged. Declaring `init` on a stateless tasklet (no `&mut State` parameter) SHALL be a `compile_error!`. The restore payload SHALL NOT be passed to the init function.

#### Scenario: init delegates init_state to the named function

- **WHEN** a `#[flezha::tasklet(init = my_init)]` stateful tasklet is expanded
- **THEN** its `init_state(cell_key, _)` returns `my_init(cell_key)`, so each static cell starts from key-derived state, and `TaskletTestHarness::replay_equals` passes for the tasklet

#### Scenario: init on a stateless tasklet fails to compile

- **WHEN** `#[flezha::tasklet(init = …)]` is applied to a `run` with no `&mut State` parameter
- **THEN** expansion emits a `compile_error!` stating `init` requires a state parameter

### Requirement: `#[flezha::stream_operative]` and `#[flezha::workflow_operative]` short-form attributes

The Rust SDK SHALL provide two attribute macros applied to a single `run` function that declare a whole single-tasklet operative of the kind the attribute names: `#[flezha::stream_operative]` and `#[flezha::workflow_operative]`. Each SHALL require an `id`, SHALL accept an optional `sink` naming a plugin channel, an optional `input` naming the entry channel, naming it `input` when the key is absent, and an optional `on_full` setting that entry channel's overflow policy. `#[flezha::workflow_operative]` SHALL additionally accept an optional `loop_mode` key taking `"once"`, `"per_input"` or `"chained"`, which the generated operative SHALL return from `loop_mode()`, and SHALL fail to compile naming any other value; `#[flezha::stream_operative]` SHALL refuse the key naming the workflow attribute. No other class-level setting SHALL be a key of either attribute. The kind SHALL NOT be expressible as a key. Two attributes are taken rather than one `#[flezha::operative]` because `flezha::operative!` is already a `#[proc_macro]` of that name in the same module (`sdk/rust/macros/src/lib.rs:42`).

Each attribute SHALL emit **two** types: the operative type under the author's PascalCase name (`Echo` for `fn echo`), implementing `StreamOperative` or `WorkflowOperative`, each with `channels()`, with `factory_id()` equal to the `id` and every other operative item at its trait default; and the tasklet type beside it under that name suffixed `Tasklet` (`EchoTasklet`), carrying the `Tasklet` impl the `#[flezha::tasklet]` attribute would generate for the same function. The generated tasklet's `name()` SHALL be the `id` verbatim. `EchoTasklet` SHALL be nameable in another operative's `channels()` body, of either kind.

With a `sink`, the generated topology SHALL wire the entry channel into the tasklet and the tasklet to that plugin sink. With no `sink`, it SHALL wire the entry channel into the tasklet and declare no plugin channel. A `sink` on a run function with a unit return and no `emits` SHALL fail to compile with a message beginning `SinkWithoutOutput`.

#### Scenario: A stream short form emits an operative and a tasklet

- **WHEN** `#[flezha::stream_operative(id = "do_nothing", sink = "echo")]` annotates `fn do_nothing(input: String) -> Vec<u8>`
- **THEN** the macro generates `DoNothing` implementing `StreamOperative` with `factory_id()` `do_nothing` and a `channels()` body wiring `input` into `DoNothingTasklet` and `DoNothingTasklet` to the `echo` plugin sink, and `DoNothingTasklet` implementing `Tasklet` with `Input = String`, `Output = Vec<u8>`, `State = ()`, `Change = NoChange`, `Clock = WallClock`, `Config = ()` and `name()` `do_nothing`

#### Scenario: The operative takes the author's name

- **WHEN** a short form is registered
- **THEN** `runner.register_factory(DoNothing)` SHALL register the operative, and `DoNothing` SHALL NOT implement `Tasklet`

#### Scenario: A workflow short form implements the workflow trait

- **WHEN** `#[flezha::workflow_operative(id = "place", sink = "placed", input = "order")]` annotates `fn place(order: String) -> String`
- **THEN** `Place` implements `WorkflowOperative` with `factory_id()` `place` and a `channels()` body wiring `order` into `PlaceTasklet` and `PlaceTasklet` to the `placed` plugin sink, every other workflow item at its default, and `runner.register_factory(Place)` registers it

#### Scenario: A workflow short form declares its loop mode

- **WHEN** `#[flezha::workflow_operative(id = "chat", sink = "replies", loop_mode = "per_input")]` annotates `fn chat(message: String) -> String`, `Chat` is registered, and two messages reach its entry channel
- **THEN** `Chat::loop_mode()` is `LoopMode::PerInput`, the tasklet runs once per message, and the instance does not complete after the first

#### Scenario: An unknown loop mode does not compile

- **WHEN** `#[flezha::workflow_operative(id = "chat", loop_mode = "forever")]` annotates a run function
- **THEN** the macro fails to compile naming `loop_mode` and the three accepted values

#### Scenario: A short form with no sink wires only the entry channel

- **WHEN** `#[flezha::stream_operative(id = "counter", input = "ticks")]` annotates `fn counter(input: String, state: &mut CounterState)`
- **THEN** `Counter`'s `channels()` body wires `ticks` into `CounterTasklet` and declares no plugin sink, and the operative registers and runs

#### Scenario: A stateful short form infers State from the mutable parameter

- **WHEN** `#[flezha::stream_operative(id = "store_input", sink = "sink")]` annotates `fn store_input(input: String, state: &mut StoreInputState) -> String`
- **THEN** `StoreInputTasklet`'s `Tasklet` impl sets `State = StoreInputState` and routes mutations through the existing state-handle path

#### Scenario: The generated tasklet is nameable in another topology

- **WHEN** `EchoTasklet` is named in a hand-written `channels()` body of a different operative
- **THEN** it wires as any tasklet does, and `Echo`'s own operative is declared only if `Echo` is also registered

#### Scenario: The attribute keeps the default entry channel name

- **WHEN** `#[flezha::stream_operative(id = "do_nothing", sink = "echo")]` declares no `input` key
- **THEN** the generated operative's entry channel SHALL be named `input`

#### Scenario: The attribute names the generated entry channel

- **WHEN** `#[flezha::stream_operative(id = "do_nothing", sink = "echo", input = "prices")]` annotates a run function
- **THEN** the generated operative's entry channel SHALL be named `prices`

#### Scenario: A sink on a unit return is refused

- **WHEN** `#[flezha::stream_operative(id = "drain", sink = "out")]` annotates a run function with a unit return and no `emits`
- **THEN** the macro fails to compile with a message beginning `SinkWithoutOutput`

#### Scenario: An empty entry channel name is rejected

- **WHEN** `#[flezha::stream_operative(id = "do_nothing", sink = "echo", input = "")]` annotates a run function
- **THEN** the macro fails to compile, stating that an entry channel name may not be empty

#### Scenario: A topology body beside a short form does not compile

- **WHEN** a type carrying a short-form attribute also has a hand-written `StreamOperative` or `WorkflowOperative` impl
- **THEN** compilation fails, and the compile-fail fixture pinning it cites `ShortFormWithTopology`

### Requirement: The Rust workflow trait carries the stream's body and none of the stream's settings

`WorkflowOperative` SHALL declare `fn channels(c: &ChannelBuilder);`, required as `StreamOperative::channels` is, and SHALL declare no `tasklets`, `retry_attempts`, `retry_backoff`, `barrier_retry_attempts` or `wake_after` item; its items SHALL be `factory_id`, `channels`, `max_duration`, `self_terminating`, `loop_mode`, `rollback_mode`, `PERSISTENCE_ENABLED` and `SYNC_DURABILITY`. `build_spec` SHALL refuse a workflow whose `channels` set the builder's `effectively_once`, any checkpoint flag or the correction window, with `BuildWorkflowError` variants named `EffectivelyOnceOnWorkflow`, `CheckpointOnWorkflow` and `CorrectionWindowOnWorkflow`, and `ZeroRetryBackoff` SHALL NOT exist. A workflow cell SHALL write no effect record: the collecting path SHALL carry no effectively-once branch, and `workflow_metadata` in the shared core SHALL advertise no `effectively_once` for a workflow class. The step ladder SHALL take the consuming edge's `RetryPolicy` from the partition's dispatcher, as the stream ladders do, and the barrier re-send count SHALL be a constant beside the acknowledgement timeout. The `TaskletCtx` a workflow cell's `run` and `on_timer` receive SHALL carry the same timer session a stream cell's does, so `timer_after`, `timer_at_event_time`, `cancel_processing_time_timer` and `cancel_event_time_timer` answer as in a stream; only a `compensate` hook's context SHALL answer `TaskletError::TimersUnavailable`. `sdk/rust/public-api.txt` SHALL record the new signature and none of the removed items.

#### Scenario: A workflow writes channels and no retry knob

- **WHEN** a type implements `WorkflowOperative` with `fn channels(c: &ChannelBuilder)` wiring `c.input::<String>("in").retry_max(2).into(Flaky)` and `Flaky` fails on every attempt
- **THEN** the class registers, the cell runs exactly twice before the instance aborts and compensates, and a type writing `fn retry_attempts() -> u32` in the same impl fails to compile

#### Scenario: The tier on a workflow is refused by the SDK

- **WHEN** a `WorkflowOperative`'s `channels` calls `c.effectively_once()`
- **THEN** `register_factory` answers `BuildWorkflowError::EffectivelyOnceOnWorkflow`, nothing is registered, and no `AddOperativeResponse` for that class could carry `effectively_once`

#### Scenario: A stale body does not compile

- **WHEN** a type implements `WorkflowOperative` with `fn tasklets(c: &ChannelBuilder)` and no `channels`
- **THEN** compilation fails naming the missing `channels` and the unknown `tasklets`

#### Scenario: The retired wake item does not compile

- **WHEN** a type implements `WorkflowOperative` with `fn wake_after() -> Option<Duration>`
- **THEN** compilation fails naming `wake_after` as not a member of the trait

#### Scenario: A timer inside a workflow tasklet fires

- **WHEN** a tasklet of a running `PerInput` workflow calls `ctx.timer_after(Duration::from_millis(300))` in `run`
- **THEN** the call answers `Ok(at)`, and `on_timer` runs on that cell with `fired_at == at`, opening a pass at that tasklet

