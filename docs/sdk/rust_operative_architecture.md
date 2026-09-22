# Rust SDK Operative Architecture

For operative authors and SDK implementers: the Rust SDK's runtime model
(Stream Operative class, instance, Tasklet, Cell, Channel, Runner), the
path a message takes from the wire to `Tasklet::run` and out to a plugin,
the backpressure and emit contracts, and the limits. The authoring surface is
specified in [`authoring_contract.md`](authoring_contract.md); the kernel
underneath in [`poll_core.md`](poll_core.md) and
[`../architecture/runner.md`](../architecture/runner.md). The rustdoc on
`sdk/rust/src/lib.rs` is the authoritative surface; nothing doctests this file.

## Concepts at a glance

```text
                        Wire (UDS / QUIC)
                                │
                                ▼
                  ┌──────────────────────────┐
                  │ OperativeDispatchHandle  │  metadata["channel"] →
                  │ (per process)            │  topology lookup
                  └──────────────────────────┘
                                │
                                ▼
            ┌─────────────────────────────────────────┐
            │  OperativeInstance (one per start;      │
            │  `--partitions N` starts N of these)    │
            │   ┌────────────┐   ┌──────────────┐     │
            │   │ Tasklet A│ ->│ Tasklet B  │ -> ... -> plugin
            │   │ (cell 0) │   │ (cells    │     │
            │   │            │   │  "1m", "1h") │     │
            │   └────────────┘   └──────────────┘     │
            └─────────────────────────────────────────┘
```

- **Stream Operative class** — the type-level declaration (`StreamOperative`
  trait: `factory_id()` + `channels(&ChannelBuilder)`). Declared once per
  runner via `FlezhaRunner::register_factory(R)`; `unregister_factory(R)` is
  its inverse. The topology is built and validated at the declaring call site
  and shared (`Arc<OperativeTopology>`) by every instance of the class.
- **Operative instance** (`OperativeInstance`) — the live object spawned on
  `AddOperative`. Each instance owns its per-Cell state and mailboxes;
  instances are independent and routed by `operative_id` (the SDK calls it
  `instance_id`).
- **Tasklet** — the unit of compute: typed `Input` / `Output` / `State` /
  `Change` / `Clock` / `Config`, a sync `run`, and a cell-mode declaration.
- **Cell** — one lane of a Tasklet with its own state and mailbox. Modes
  (`CellMode`): `Single` (default, one Cell), `Static` (fixed
  `cell_keys()`, routed by `cells_for`), `Dynamic` (declared on the
  wiring by `.key_by(f)`, Cells created per key at runtime). At most one
  drain runs per Cell at any time (sticky ordering).
- **Channel** — the typed wire between producers and consumers (`ChannelKind`):
  `Entry` (controller → SDK), `InterTasklet` (`Arc<T>` zero-copy between
  Tasklets), `PluginBound` (serialized to bytes and sent to a named plugin
  through the controller).
- **Runner** — the poll-worker pool that drains mailboxes, one per process
  (`FlezhaRunner::start()`); see "Worker pool" below.

### Operative kinds (controller side)

Every operative carries a **kind** (`:stream` or `:workflow`), the one its
class declares and its runner announces with the factory id, resolved at start
and frozen. `Flezha.Operative.Kind` is the one capability table: a kind
declares only the capabilities it has, and the control plane reads every flag
through `Kind.capable?/2` with a `false` default. `:stream` declares
`persistent_state?`, `time_travel?`, `dynamic_cells?` and
`output_stall_watch?`; `:workflow` declares none. A gate reads the flag instead
of probing the compute unit — `flz state get` on a workflow operative answers
`{:error, {:unsupported_for_kind, :persistent_state}}`.

### The `:workflow` kind

A workflow operative is a job that ends itself: `flz operative start <factory>
[--id <id>]` names only the factory, and the controller puts the kind the
runner announced on `AddOperative`. Both kinds are one instance type in one
registry, resolved once per frame at the dispatch boundary; a workflow instance
is an `OperativeInstance` that also carries its pass sheet, and its class is
told apart by the `workflow` field on its topology. A workflow class declares
its graph in `channels`, on the same builder a stream uses:

```rust
struct Greet;
impl flezha_runner::WorkflowOperative for Greet {
    fn factory_id() -> &'static str { "greet" }
    fn channels(c: &flezha_runner::ChannelBuilder) {
        c.input::<String>("in").into::<SayHello>().sink_named("out");
    }
}
flezha_runner::Flezha::create_runner("my_app")?.register_factory(Greet)?.run()?;
```

`SayHello` is an ordinary `Tasklet` whose `run` emits `format!("hello {input}")`.
The entry channel takes the instance's first message, from a spawn,
`flz operative send` or `flz operative start --text`; `init_values` is the
tasklets' initial state, as on a stream. A class declaring no tasklet is
refused at registration, and so is one whose `channels` set the stream's
`effectively_once`, a checkpoint flag or `correction_window`
(`EffectivelyOnceOnWorkflow`, `CheckpointOnWorkflow`,
`CorrectionWindowOnWorkflow`). A tasklet's retry ladder is its input edge's
`retry_max` / `retry_backoff_ms`, as on a stream, and a timer registered inside
a workflow tasklet fails with `TimersUnavailable`. A tasklet ends a
looping instance with `ctx.complete(result)`. When the job ends, the SDK emits
`WorkflowCompleted` on the control stream and the controller drives the one
teardown path (`Flezha.Service.OperativeManager.stop_operative/3`). A tasklet
that fails past its retry ladder is terminal: compensation, then teardown
plus `[:flezha, :workflow, :failed]`, no re-run. A job that
completes before the start RPC confirms its binding is reported as a successful
start with no live `runner_id`. Lifecycle, durable loop, suspension, saga
compensation, cell retry and finished-job retention are in
[`../resilience/workflow_operative.md`](../resilience/workflow_operative.md).

## Public API

```rust
use flezha_runner::{
    Flezha, ChannelBuilder, StreamOperative, Tasklet, TaskletCtx, TaskletError,
};
```

`Flezha::create_runner(namespace)` returns the process's `FlezhaRunner`;
`register_factory(R)` / `register_factory(T)` declare classes on it;
`start()` connects (retried with backoff until the controller is reachable)
and `run()` is `start()` + `await_termination()`. The controller spawns
instances later with `flz operative start`. `Flezha::current_runner()` reaches
the started runner from code that does not control startup order. Every
fallible call returns `FlezhaError`:

| Refusal | When |
|---|---|
| `DuplicateFactoryId` | a second `register_factory` of the same factory id on one runner — the declared set is per runner and starts empty, even though the class registry is process-global |
| `NotDefined` | `unregister_factory` names a factory this runner never declared |
| `OperativeTypeInUse { live_instances }` | `unregister_factory` while live instances exist; the type stays defined |
| `AlreadyRunning` | a second `start()` in the process — at most one started runner per process |
| `InvalidTopology` / `InvalidWorkflow` | reported at the declaring call site, never a panic |
| `NotStarted` | `operatives()` / `await_termination()` on a never-started runner |

Starting with zero declared classes is legal, and `register_factory` /
`unregister_factory` stay callable after `start()`; a post-start change ships
the runner's full factory set as an `UpdateFactories` control request —
fire-and-forget, no ack; the reconnect init frame is the resync of record. To
call them after `start()`, bind the runner: a chained
`Flezha::create_runner(ns)?.start()?` drops the temporary at statement end and
shuts the runner down; write `let runner = Flezha::create_runner(ns)?;` and
then `runner.start()?;` (`run()` is immune, because it blocks until shutdown).

## Authoring sugar (default path)

`#[flezha_runner::tasklet]`, `#[derive(flezha_runner::State)]` and
`flezha_runner::operative!` (crate `flezha-macros`) are pure expansion over
the `StreamOperative` / `Tasklet` / `Change` traits: a sugared tasklet and
a hand-written one compose in one operative, and ejecting a sugared tasklet
is a paste of its expansion. The contract binding every SDK is
[`authoring_contract.md`](authoring_contract.md).

```rust
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Default, flezha_runner::State)]
struct OhlcState { candles: Vec<f64> }

#[flezha_runner::tasklet(id = "parse_op")]
fn parse(input: String) -> f64 {
    input.parse().unwrap_or(0.0)
}

// `&mut State` ⇒ stateful; the return value is auto-emitted.
#[flezha_runner::tasklet(id = "ohlc_op", keys = ["1m", "1h", "1d", "1w"], cells_for = all_tfs)]
fn ohlc(price: f64, state: &mut OhlcState) -> Vec<u8> {
    state.push_candles(price);            // journaled via #[derive(State)]
    format!("{price}").into_bytes()
}

fn all_tfs(_: &f64) -> Vec<&'static str> { vec!["1m", "1h", "1d", "1w"] }

// A linear pipeline needs no `channels()` body. The macro begins with the type
// it declares, which is what the author registers.
flezha_runner::operative! {
    pub struct Market;
    id: "market",
    pipeline: input::<String> -> Parse -> Ohlc -> plugin::<Vec<u8>>("ohlc_kafka")
}
```

A single-tasklet operative needs no `operative!`: its `run` function takes
`#[flezha_runner::stream_operative]` or `#[flezha_runner::workflow_operative]`
instead (the short form, below). Attribute opt-ins: `id`, `emits`, `keys` +
`cells_for` (static mode), `key`, `idle_ttl` + `max_cells` (dynamic-mode
knobs; the key itself is declared on the wiring with `.key_by(f)`), `capacity`,
`clock`, and on the short-form attributes `sink`, `input` and `on_full`, which
describe the operative's entry channel.
Nothing is harvested: every operative a runner declares is one the author
registered. Fan-out and multi-input stay on an explicit `channels()` body.

Both shorthands name their entry channel `input` unless the author says
otherwise: `input = "prices"` on the attribute, `input::<String>("prices")` on
the `operative!` pipeline head — the same position the `plugin` terminator
takes its name in.

### The short form

`#[flezha_runner::stream_operative]` or `#[flezha_runner::workflow_operative]`
on a `run` function declares a whole operative of the kind the attribute names.
It emits two types: the operative under the function's PascalCase name, which
is what you register, and the tasklet beside it, suffixed `Tasklet`:

```rust
#[flezha_runner::stream_operative(id = "upcase", sink = "out")]
fn upcase(value: String) -> String {
    value.to_uppercase()
}

#[flezha_runner::workflow_operative(id = "place", sink = "placed", input = "order")]
fn place(order: String) -> String {
    order
}

Flezha::create_runner("default")?
    .register_factory(Upcase)?
    .register_factory(Place)?
    .run()?;
```

The generated operative is the one you would have written by hand with that
single tasklet: an entry channel named `input` (or the `input` key's value),
the tasklet, a plugin sink named by `sink` when one is written, a factory id
equal to `id`, and every class-level setting at its trait default. The
tasklet (`UpcaseTasklet`) is named `id` verbatim, so its cells keep their
state path when the operative grows into the full form, and it wires into
another operative's `channels()` body like any tasklet. A workflow short form
is a `once` workflow, run once over its first message: `flz operative start
<id> --text '<payload>'`, or a start followed by `flz operative send`; a second
message to a `once` instance is accepted and never read. It may declare
another loop mode, `#[flezha_runner::workflow_operative(id = "chat", sink =
"replies", loop_mode = "per_input")]`, and then runs once per message.

`sink` is optional. With none, the entry channel feeds the tasklet and no
plugin channel is declared: the operative that only keeps state.

```rust
#[derive(Serialize, Deserialize, Clone, Default, flezha_runner::State)]
struct CounterState { seen: Vec<String> }

#[flezha_runner::stream_operative(id = "counter", input = "ticks")]
fn counter(tick: String, state: &mut CounterState) {
    state.push_seen(tick);
}
```

Anything more is the full form: a second tasklet, a fan-out, a merge, a keyed
edge, a loop mode, a retry ladder, the effectively-once tier, a correction
window. Rust names no entry codec — an entry decodes by its declared type — so
the short form takes no `codec` key.

The refusals are compile errors. A `sink` on a unit return with no `emits`
fails with a message beginning `SinkWithoutOutput`, and a missing `id` names the
attribute. A hand-written `StreamOperative` or `WorkflowOperative` impl beside a
short form is rustc's conflicting-implementation error, which the contract calls
`ShortFormWithTopology`. `#[flezha_runner::tasklet]` declares a tasklet
only and refuses `sink`, `input`, `kind` and `on_full`.

## Escape hatch: the raw traits

The sugar expands to this. Reach for it when the topology is not a single
tasklet or a linear chain.

```rust
use flezha_runner::{ChannelBuilder, Flezha, Tasklet, TaskletCtx, TaskletError, StreamOperative};
use flezha_runner::state::{NoChange, RestoreInitPayload, StateHandle, WallClock};

#[derive(Default)]
struct ParseTask;
impl Tasklet for ParseTask {
    type Input = String;
    type Output = f64;
    type State = ();
    type Change = NoChange;   // opts out of persistence
    type Clock = WallClock;
    type Config = ();

    fn name() -> &'static str { "parse_op" }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}

    fn run(&mut self, input: String, _: StateHandle<'_, Self>, ctx: &mut TaskletCtx<'_>)
        -> Result<(), TaskletError>
    {
        let price: f64 = input.parse().map_err(|e: std::num::ParseFloatError| {
            TaskletError::Custom(e.to_string())
        })?;
        ctx.emit::<f64>(price)
    }
}

struct MarketOperative;
impl StreamOperative for MarketOperative {
    fn factory_id() -> &'static str { "market" }

    fn channels(c: &ChannelBuilder) {
        c.input::<String>("trade.raw")
            .into::<ParseTask>()          // ParseTask's output stream handle
            .named("trade.parsed")
            .into::<OhlcTask>()           // static-mode consumer, see below
            .sink_named("ohlc.candles");  // plugin-bound terminal, named here
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    Flezha::create_runner("market_ns")?.register_factory(MarketOperative)?.run()?;
    Ok(())
}
```

A static-mode Tasklet (`OhlcTask`) also declares `cell_keys()`, overrides
`cells_for(&Input) -> Vec<&'static str>` and sets
`const CELLS_FOR_OVERRIDDEN: bool = true` (a keyset without the override is
`TopologyIssue::CellsForMissing`). A stateful Tasklet declares a real
`Change` and overrides `apply_change(&mut State, &Change)`; state is read via
the `StateHandle`'s `Deref` and mutated with `state.apply(change)`. `run`,
`apply_change` and `compensate` must be deterministic — time via `ctx.now()`,
ids via `ctx.stable_id()`. `SimpleTasklet` is the tier between the attribute
and the full trait. Other hooks: `from_config`, `on_timer`,
`max_timers_per_cell` (10 000), `compact_after_bytes` (8 MiB), `compensate`
+ `COMPENSATE_OVERRIDDEN` (workflow saga), `SCHEMA_VERSION`.

### Java SDK parity

The Java SDK is the same model: `Tasklet<Input, State, Output, ChangeT,
ClockT, ConfigT>` is a parameterized interface with `inputType()` /
`outputType()` accessors, so bounds Rust checks at compile time become
`StreamOperativeConfigurationException`s from `registerFactory(...)`;
a written `cellsFor`, read off the class at registration, replaces the
`CELLS_FOR_OVERRIDDEN` const;
dynamic mode is read off the wiring on both sides (the annotation processor
refuses `@CellKey`) with `cellIdleTtl()` as the only knob; zero-copy
passes the value by reference, so the producer must treat an emitted value as
immutable. `@Tasklet` / `@State` / `@CellsFor` (`flezha-sdk-processor`)
generate the impls, and a `@StreamOperative` or `@WorkflowOperative` class
generates the short form's `<Class>StreamOperative` or
`<Class>WorkflowOperative`, which the application registers with
`registerFactory`. Persisted
Changes declare `sdk_lang = 2`, `change_format_id = 4` (Kryo). A Java
`WorkflowOperative` emits `WorkflowCompleted` byte-identically to Rust.

## Hot-path dispatch

1. **Wire frame → entry channel** — `metadata["channel"]` is resolved through
   `OperativeTopology::name_to_id` (`HashMap<&'static str, ChannelId>`), then
   a `Vec` index into `topology.channels`. A frame with no `channel` key goes
   to the class's first declared `Entry` channel; a name that is not one of the
   class's `Entry` channels — undeclared, or declared as a sink or an
   inter-tasklet channel — is parked to the dead-letter store under the name
   it asked for, with one attempt and no retry wait. The channel's `wrap_input` turns the bytes into
   `Arc<dyn Any + Send + Sync>` (see "Entry decoding").
2. **Entry channel → consumer Cell** — per consumer Tasklet: `Single`
   jumps to cell 0; `Static` does one HashMap lookup per key returned by
   `cells_for(&input)`; `Dynamic` calls the edge's `key_by` closure and
   does one `DashMap` lookup on the Tasklet's `DynamicCellRegistry`,
   creating the Cell on a miss. The key function is total — an input is
   discarded by a `filter` declared before it, never by a keyless return.
3. **Mailbox push** — one `ArrayQueue::try_push`, one Relaxed `pushes_total`
   increment, one `last_activity_at` store (the reaper's idle stamp), one
   inflight compare-exchange. A winning CAS schedules the Cell's round on
   the poll core; a losing one means an in-progress drain picks the item up
   through the missed-wake re-check. A full mailbox applies the channel's
   `OverflowPolicy` ("Backpressure" below).
4. **Drain → `Tasklet::run`** — the dispatcher downcasts to `Arc<S::Input>`,
   takes the value out with `Arc::unwrap_or_clone` (a clone only when fan-out
   left the Arc shared) and calls the typed `run`. A returned `Err` or panic
   climbs the channel's `RetryPolicy` (default 3 attempts, 50 ms base,
   exponential with ±50 % jitter, capped at 5 s) and then parks the payload to
   the dead-letter queue; `TaskletError::NonRetryable` parks immediately.

Bench shapes and floors: [`../operations/benchmarking.md`](../operations/benchmarking.md).

### Entry decoding

`c.input::<T>("name")` accepts any payload type: `Vec<u8>` is delivered
unchanged, `String` is UTF-8-decoded (invalid UTF-8 is dropped and logged),
anything else is deserialized with the SDK's default JSON codec. A failing
typed parse rides the channel's retry→DLQ ladder. `input_with(name, parser)`
replaces the codec with a deterministic `Fn(Vec<u8>) -> Result<T, String>`.
Producer-side transforms (`map`, `filter`, `flat_map`), `key_by`, `branch`
and `merge` are in [`stream_transforms.md`](stream_transforms.md) and
[`multi_input_tasklets.md`](multi_input_tasklets.md).

## Dynamic cell mode

Dynamic mode is declared on the **wiring** — an edge carrying `.key_by(f)`
makes its consumer dynamic — so a reader of `channels()` sees where work fans
out. A dynamic Tasklet owns zero Cells at startup; the first input whose
key function returns `key` creates a `CellSlot` (cold
`init_state(Some(key), None)`) in the Tasklet's `DynamicCellRegistry`.
Concurrent producers for a fresh key share one slot. Declaring `.key_by` into
a Tasklet with a non-empty `cell_keys()` is refused
(`TopologyIssue::KeyByOnUnkeyableTarget`). Two per-Tasklet knobs:

- `cell_idle_ttl() -> Option<Duration>` — default `None`, no eviction and no
  reaper. `Some(ttl)` spawns one `flezha-reaper` thread per Tasklet that
  sweeps every `min(ttl / 4, 30 s)` (floored at 1 ms; `FLEZHA_REAP_INTERVAL_MS`
  overrides) and evicts any Cell idle longer than `ttl`. A sweep takes the
  Cell's drain gate and confirms an empty mailbox first, so it never
  interrupts a running `run`. **Eviction is destructive**: the slot, its
  pending timers and (when persistent) its on-disk WAL/snapshot go — a
  `delete_cell_state` to the controller, durable-class like every other
  record, so an eviction inside a reconnect window holds the frame for the
  rebind rather than losing it — and a later input cold-starts fresh. The
  reaper waits for no reply and the controller writes none. The registry
  keeps the last 1024 evicted keys so a live read can say "idle-evicted"
  rather than "never existed".
- `max_cells() -> usize` — default `100_000`, a soft cap: at the cap
  new-key inputs are dropped (`inputs_dropped_capped_total`), existing keys
  are unaffected. It bounds memory against a runaway `.key_by`, not
  admission; concurrent inserts may overshoot slightly.

A **bind-time restore** is the other direction and does materialize a slot:
the controller ships one `RestoreInitState` per Cell with durable state and
the SDK creates the dynamic Cell to fold it into. Eviction deletes the
files, so no restore is ever shipped for an evicted Cell. This is also what
carries a key across a partition rescale.

Per-Tasklet counters (`DynamicCellCounters`, Relaxed):
`cells_created_total`, `cells_evicted_total`,
`inputs_dropped_capped_total`.

## Partitioning

An instance binds to one runner, so the machine is the ceiling for one
Operative. `--partitions N` starts N ordinary instances (`<id>#p0` …
`<id>#p{N-1}`), each owning a contiguous range of the 256 key groups a
routing key hashes into (`KeyGroupRange` on the `OperativeInstance`). Nothing in
this doc changes for a member; the one dispatch addition is a range check on
each computed cell key that parks a mis-route before any state for that key
exists. A class with no keyed entry-fed tasklet refuses to be partitioned.
Group model, sources, forwarding, rescale: [`partitioned_operatives.md`](partitioned_operatives.md).

## Per-Cell timers

A Tasklet registers deadlines on its own Cell (`ctx.timer_at_event_time`,
`ctx.timer_after`) and handles them in `on_timer`. Event-time timers fire on
the dispatch path when an input advances the Cell's event-time high-water,
before that input; processing-time timers ride the poll core's deferred-wake
list and are promoted onto the Cell's existing mailbox, so a firing obeys
the same ordering, overflow policy and single-drainer exclusion an input does.
The pending set is folded state. Contract: [`cell_timers.md`](cell_timers.md).

## Backpressure and bounded mailboxes

Each Cell's mailbox is a fixed-size lock-free `crossbeam` `ArrayQueue`
allocated at operative startup. Capacity is declared per Tasklet; the
policy applied when it is full is declared per channel.

### Capacity (consumer side)

```rust
impl Tasklet for OhlcTask {
    fn mailbox_capacity() -> usize { 4096 }
    const MAILBOX_CAPACITY_OVERRIDDEN: bool = true;
    // ...
}
```

Resolution at `register_factory(R)`: the Tasklet's value when
`MAILBOX_CAPACITY_OVERRIDDEN = true`, else `FLEZHA_MAILBOX_CAPACITY`, else
1024. Capacity must be in `1..=1_048_576`; out-of-range values fail
registration naming the Tasklet. Static-mode Tasklets allocate one
mailbox per declared key, all at the same capacity.

### Policy (producer / wiring side)

```rust
c.input::<Vec<u8>>("trade.raw")
    .on_full(OverflowPolicy::DropNewest)   // entry-side policy
    .into::<ParseTask>()
    .into::<OhlcTask>()
    .named("ohlc.candles")
    .on_full(OverflowPolicy::DropNothing)  // inter-tasklet policy
    .into::<IndicatorTask>();
```

| `OverflowPolicy` | Behaviour |
|---|---|
| `DropNothing` (default, every kind) | Nothing is discarded, and **how** that is realized depends on who produced the value. A producing Cell's `ctx.emit` never parks: the push is held in that Cell's push holdback, the Cell is handed no further activation until it drains, and the consumer's next free slot drains it. The wire reader, which has no producing Cell, retries after a bounded 50 ms park on the consumer's `Waiters`, woken per pop — the one park left, and what carries ingress backpressure to the controller. Preserves ordering either way, loses nothing, never stalls indefinitely; a producer blocked longer than 5 s logs one `flezha::mailbox` warning naming the consumer. |
| `DropNewest` | The incoming item is discarded; `drops_newest_total` += 1. |
| `DropOldest` | The mailbox pops its oldest item and accepts the new one (`force_push`); `drops_oldest_total` += 1 **and `pushes_total` += 1** — the force-push admitted, so it is a successful enqueue by that counter's definition. Which item is evicted is best-effort under MPMC contention; the evicted one is named in a `flezha::mailbox` debug line. |

A **replay-slice member ignores the declared policy and takes the `DropNothing`
path**, whatever the channel says: a correction's answer may not depend on
arrival timing, and a dropped member would leave the slice open, short, and
folded anyway — a wrong answer with nothing reported.

Declaring a dropping policy on a class that also declares `effectively_once()`
is **refused at build time** (`TopologyIssue::DroppingPolicyUnderEffectivelyOnce`,
and its C-ABI twin at declare time): a shed input's offset is still advanced
past by the tier's watermark, so the loss is permanent under a contract that
promises nothing is lost. A correction window requires the tier, so it is
covered by the same refusal.

A `PluginBound` channel has no SDK-side mailbox between the emit and the
wire, so a policy declared on it is never consulted; backpressure there is the
socket buffer plus transport flow control. `.sealed()` returns a finalized
`Channel<T>` with no builder methods, which is how an author states
terminality in the type — nothing, `.on_full(...)` included, chains after it.

The entry default has a co-tenancy cost: the wire reader services every
Operative on one physical connection, and under `DropNothing` it parks on the
saturated Operative's mailbox until the pool pops a slot — siblings wait with
it. An Operative that prefers to shed (a live ticker where a stale price is
worthless) declares `DropNewest` / `DropOldest` on its entry channel; the
reader stays free and every drop is counted at the declaration site.

### Per-Cell counters

`CellCounters` on every mailbox, four `AtomicU64`s bumped `Relaxed`, plus a
process-wide aggregate of each overflow outcome beside `DISPATCHES_COMPLETED`:

| Counter | Bumped on | Readable |
|---|---|---|
| `pushes_total` | every admission, `DropOldest`'s force-push included | `slot.counters()` |
| `pushes_blocked_total` | every `DropNothing` admission that found the mailbox full — a wait iteration on the entry path, an emit deferred or re-attempted on the producer path | `slot.counters()`; process-wide via `cell::pushes_blocked_total()` → heartbeat `blocked_parks` |
| pending pushes | Cells currently holding an undelivered inter-tasklet output, and the age of the oldest | `poll_core::global().pending_push_count()` / `pending_push_oldest_age_ms()` → heartbeat `pending_pushes`, `pending_push_oldest_ms` |
| `drops_newest_total` | every drop under `DropNewest` | `slot.counters()`; process-wide via `cell::drops_newest_total()` → heartbeat `drops_newest` |
| `drops_oldest_total` | every eviction under `DropOldest` | `slot.counters()`; process-wide via `cell::drops_oldest_total()` → heartbeat `drops_oldest` |

The per-Cell set is the breakdown a holder of the slot reads; what leaves the
process is the aggregate, carried in the data-channel heartbeat's metadata
beside `dispatches`, `credit_parked` and `inflight`. A per-Cell breakdown on
that path would need a registry walk per heartbeat.

`blocked_parks` counts **every full-mailbox admission, not episodes**. A runner
held in one long block episode would otherwise show a frozen dispatch count
beside a frozen block count — byte for byte the picture a wedged runner shows —
and that distinction is exactly what the controller's `runner_pool_wedged`
verdict hangs off: dispatches frozen with blocks climbing is backpressure,
dispatches frozen with blocks flat is a wedge. The entry path pays those as
wait iterations of a parked thread; a producing Cell pays them as a deferral
and its re-attempts, and reports the standing condition separately through
`pending_pushes` / `pending_push_oldest_ms` — it holds no handout, so
`inflight` is blind to it.

### Worker pool

`FLEZHA_WORKER_POOL_SIZE` sets the base worker count (default: the host's
available parallelism, fallback 2); a starvation supervisor grows the pool up
to `FLEZHA_WORKER_POOL_MAX` (default `10 × base`, never below base), and a
grown worker retires after 30 s idle. A `DropNothing` producer parks the
worker it ran on, so a pipeline of depth D under `DropNothing` wants `≥ 2 × D + 1`
threads — but a parked producer is not inside `poll`, so the work queued
behind it is exactly the starvation the pool grows on; raising the base only
removes the growth latency. Single-stage pipelines are unaffected: the wire
reader is the only producer and its park costs no pool thread.

## Emit paths

### Inter-tasklet zero-copy

`ctx.emit::<C>(value)` on an `InterTasklet` channel wraps the value in one
`Arc`, clones the `Arc` per consumer Cell, and each drain takes it out with
`Arc::unwrap_or_clone`: a single consumer gets the inner `T` with no clone;
fan-out pays one value clone per consumer.

### Plugin-bound emit

On a channel with a plugin sink the dispatcher serializes the value once
(`sink()` / `sink_named(name)` use the default rules — passthrough for
`Vec<u8>`, UTF-8 for `String`, JSON otherwise; `sink_with(serializer)` /
`sink_named_with(name, serializer)` supply a `Fn(&T) -> Vec<u8>`), builds a frame with `payloader::build_from_binary`
carrying `channel = <channel name>` in the metadata, and writes it on the
`FlezhaStream` the originating input arrived on. The controller's plugin
layer (for example `Flezha.Plugin.Provider.Kafka` with its `:channel` option)
gates on `metadata["channel"]`.

`ctx.emit_with_meta::<C>(value, meta)` appends user metadata to the plugin
frame only; it returns `MetadataNotAllowedOnInterTaskletChannel` on an
inter-tasklet channel without a sink, `ReservedMetadataKey` for any key
under the reserved `flz.` prefix, and silently replaces a user-supplied
`channel` entry with the real channel name. A channel declared `.keyed()`
(requires a sink) stamps `flz.cell` / `flz.seq` / `flz.key` on every plugin
frame: `emit` carries the Cell's own key (a single-cell Tasklet fails
with `KeyRequiredOnKeyedChannel`), `emit_keyed(key, value)` carries a
replay-stable natural key (`KeyNotAllowedOnNonKeyedChannel` elsewhere).

### Hybrid and terminal: one `sink` method

`sink` / `sink_named` / `sink_with` / `sink_named_with` on a producer's output
handle attach the channel's `plugin_sink: Option<PluginSink>` — the one
representation of plugin egress. The `_named` half names the channel, which is
what rides the frame; naming one channel with both a sink and `.named(...)` is
`TopologyIssue::ChannelNamedTwice`. `kind` is derived at build time: `InterTasklet` when consumers are wired
after the sink (hybrid — one typed value reaches the consumer mailboxes and,
serialized, the plugin), `PluginBound` when nothing consumes the channel
(terminal). Order of `sink_with` and `.into::<S>()` is irrelevant; a second
sink on one channel is `TopologyIssue::DuplicatePluginSink`; wiring a
consumer after `.sealed()` is a compile error.

```rust
sma_stream                       // SmaTask's output handle, from `.into::<SmaTask>()`
    .sink_named_with("sma_out", sma_to_pb_bytes)
    .into::<BollingerTask>();
```

Per emit on a hybrid channel the typed `Arc<T>` fans out to every consumer
mailbox under the channel's `OverflowPolicy` first; then the serializer runs
once inside `catch_unwind` and the bytes take the plugin-wire path, identical
to a pure `PluginBound` frame. A panicking serializer never prevents
inter-tasklet delivery: the producer gets
`TaskletError::PluginSerializerPanic { channel, message }` and the dispatcher
stays alive. Channels naming one plugin fan in at the controller, told apart
by the `channel` metadata key; Rust and Java frames are byte-identical there.

## Per-Cell state and persistence

Each `CellSlot` holds `CellState<S> { tasklet: S, state: S::State }`
behind a `Mutex` the inflight CAS keeps uncontended: the Tasklet value is a
per-Cell scratch buffer, `S::State` is the domain state, `init_state` runs
once per Cell and later runs mutate in place through the `StateHandle`.
State migration (`MigrationSerialization` / `CellState`) walks the Cells
per Tasklet and produces the JSON shape in
[`protocol_spec.md`](protocol_spec.md) "Operative state JSON shape".

Persistence rides the `Change` trait: `state.apply(change)` is a `Vec::push`
on the hot path; serialization, coalescing and the wire send happen on one
per-process flusher thread on a 5 ms linger (`FLEZHA_FLUSH_LINGER_MS`).
`type Change = NoChange` opts a Tasklet out. The class toggle
`StreamOperative::PERSISTENCE_ENABLED` (default `true`) is frozen at instance
start; the runner overrides it per instance with the operative-start metadata
key `operative_persistence.<OperativeClass>`. Trait surface, wire format,
`TaskletTestHarness` (`replay_equals`, `replay_emits_equal`) and the Java
port: [`state_persistence.md`](state_persistence.md).

## Limits

- **Per-Tasklet `handle_message` is not served by the SDK**: the
  `OperativeMessage` command is answered `operative does not support
  handle_message` (`flz_core/src/command/operative_message.rs`); controller-side
  handlers are in [`../plugins/message_handlers.md`](../plugins/message_handlers.md).
- **One started runner per process** (`FlezhaError::AlreadyRunning`).
- **`UpdateFactories` has no ack**; the reconnect init frame is the resync.
- **`unregister_factory` cannot see a suspended durable workflow instance**:
  the removal succeeds and rehydration fails placement.
- **Only the overflow aggregates leave the process** — `blocked_parks`,
  `drops_newest`, `drops_oldest`, `pending_pushes` and `pending_push_oldest_ms`
  ride the data heartbeat; the per-Cell breakdown and the dynamic-cell
  counters stay in-process, unexported to a metrics backend. Which Cell is
  behind is named only in the runner's own `flezha::mailbox` log — the
  heartbeat carries counters, not identities.
- **`max_cells` is a soft cap** and `DropOldest` is best-effort under
  contention; the Java SDK has no `maxCells` knob at all.

## Where to look in the code

| Concern | File |
|---|---|
| `StreamOperative` trait; `Tasklet`, `TaskletCtx`, `TaskletError`, `SimpleTasklet` | `sdk/rust/src/operative/mod.rs`; `sdk/rust/src/tasklet/{mod,ctx,error,simple}.rs` |
| `ChannelBuilder`, `Stream<T>`, `TopologyIssue`, `Channel<T>` | `sdk/rust/src/channel/builder/`, `sdk/rust/src/channel/mod.rs` (kernel types: `flz_core/src/channel.rs`) |
| Topology builder, mailbox-capacity resolution | `sdk/rust/src/operative/builder.rs` |
| Authoring macros | `sdk/rust/macros/src/{tasklet,state,operative}.rs` |
| Typed dispatcher and plugin-sink tail; poll-worker pool | `sdk/rust/src/operative/dispatcher.rs`, `poll_pool.rs` |
| Poll engine (handout, admission, ladder, deferred wakes) | `flz_core/src/poll_core/` |
| `OperativeInstance`, `KeyGroupRange`; `OperativeTopology`, `CellMode`; registries | `flz_core/src/operative/{instance,topology,registry}.rs` |
| `CellSlot`, `CellCounters`; `Mailbox`, `Waiters`; dynamic registry and reaper | `flz_core/src/cell.rs`, `mailbox.rs`, `dynamic.rs` |
| Wire dispatch (`OperativeDispatchHandle`); wire input routing | `flz_core/src/routing.rs`, `flz_core/src/command/streams_connect.rs` |
| Enqueue, overflow policies, `send_plugin_frame`; Change flusher | `flz_core/src/routing.rs`, `flz_core/src/flusher.rs` |
| Operative state JSON shape, `apply_init_values` | `flz_core/src/command/cell_state.rs` |
| `WorkflowOperative`; `TaskletTestHarness` | `sdk/rust/src/workflow/mod.rs`, `flz_core/src/workflow/mod.rs`; `sdk/rust/src/testing/harness.rs` |

## See also

- [`authoring_contract.md`](authoring_contract.md) — the cross-SDK authoring contract
- [`poll_core.md`](poll_core.md) — the kernel boundary the SDK is hosted on
- [`state_persistence.md`](state_persistence.md) — `Change`, WAL wire format, restore
- [`../architecture/stream_profile.md`](../architecture/stream_profile.md) — the stream profile end to end
- [`../internals/overwatch_lifecycle.md`](../internals/overwatch_lifecycle.md) — the controller-side Overwatch that starts and feeds an instance
