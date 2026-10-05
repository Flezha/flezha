# Rust SDK Relay Architecture

For relay authors and SDK implementers: the Rust SDK's runtime model
(Flow Relay class, instance, Tasklet, Cell, Channel, Runner), the
path a message takes from the wire to `Tasklet::run` and out to a plugin,
the backpressure and emit contracts, and the limits. The authoring surface is
specified in [`authoring_contract.md`](authoring_contract.md); the kernel
underneath in [`poll_core.md`](poll_core.md) and
[`../architecture/runner.md`](../architecture/runner.md). The rustdoc on
`sdk/rust/src/lib.rs` is the authoritative surface; nothing doctests this file.

**Support tier:** [tier 1](../publishing/maturity.md#sdk-rust), run by the maintainer in a
live deployment, passes the full conformance run in CI and is permanently supported.

## Concepts at a glance

```text
                        Wire (UDS / QUIC)
                                │
                                ▼
                  ┌──────────────────────────┐
                  │ RelayDispatchHandle  │  metadata["channel"] →
                  │ (per process)            │  topology lookup
                  └──────────────────────────┘
                                │
                                ▼
            ┌─────────────────────────────────────────┐
            │  RelayInstance (one per start;      │
            │  `--partitions N` starts N of these)    │
            │   ┌────────────┐   ┌──────────────┐     │
            │   │ Tasklet A│ ->│ Tasklet B  │ -> ... -> plugin
            │   │ (cell 0) │   │ (cells    │     │
            │   │            │   │  "1m", "1h") │     │
            │   └────────────┘   └──────────────┘     │
            └─────────────────────────────────────────┘
```

- **Relay class** — the type-level declaration (`Relay`
  trait: `factory_id()` + `channels(&ChannelBuilder)` + `settings()`). Declared once per
  runner via `FlezhaRunner::register_factory(R)`; `unregister_factory(R)` is
  its inverse. The topology is built and validated at the declaring call site
  and shared (`Arc<RelayTopology>`) by every instance of the class.
- **Relay instance** (`RelayInstance`) — the live object spawned on
  `AddRelay`. Each instance owns its per-Cell state and mailboxes;
  instances are independent and routed by `relay_id` (the SDK calls it
  `instance_id`).
- **Tasklet** — the unit of compute: the author's own type, with typed
  `Input` / `Output` / `State` / `Change`, a `NAME`, a clock, a sync `run`,
  and a cell-mode declaration.
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

### How a relay runs (controller side)

Every relay carries a **run** (`:flow` or `:steps`), the one its
class declares and its runner announces with the factory id, resolved at start
and frozen. What an instance can do follows from its class's settings tree (`run`,
`durable` with its `sleep_after`) and from its own state. `flz state get` on an
instance of either run whose class keeps nothing (`durable: false`) answers
`{:error, :persistence_off}`.

### The `:steps` kind

A steps relay is a job that ends itself: `flz relay start <factory>
[--id <id>]` names only the factory, and the controller puts the kind the
runner announced on `AddRelay`. Both kinds are one instance type in one
registry, resolved once per frame at the dispatch boundary; a steps instance
is a `RelayInstance` that also carries its pass sheet, and its class is
told apart by the `steps` field on its topology. A steps class declares
its graph in `channels`, on the same builder a flow relay uses:

```rust
struct Greet;
impl flezha_runner::Relay for Greet {
    fn factory_id() -> &'static str { "greet" }
    fn channels(c: &flezha_runner::ChannelBuilder) {
        c.input::<String>("in").into::<SayHello>().output_named("out");
    }
}
flezha_runner::Flezha::create_runner("my_app")?.register_factory(Greet)?.run()?;
```

`SayHello` is an ordinary `Tasklet` whose `run` emits `format!("hello {input}")`.
The entry channel takes the instance's first message, from a spawn,
`flz relay send` or `flz relay start --text`; `init_values` is the
tasklets' initial state, as on a flow relay. A class declaring no tasklet is
refused at registration; the effectively-once tier, its checkpoint and its
correction window are members of `Run::Flow`, so a steps class cannot write
them. A tasklet's retry ladder is its input edge's
`retry_max` / `retry_backoff_ms`, as on a flow relay, and its timers fire as passes
of their own ([`cell_timers.md`](cell_timers.md#timers-in-a-steps-relay)). A tasklet ends a
looping instance with `ctx.complete(result)`. When the job ends, the SDK emits
`RelayCompleted` on the control stream and the controller drives the one
teardown path (`Flezha.Service.RelayManager.stop_relay/3`). A tasklet
that fails past its retry ladder is terminal: compensation, then teardown
plus `[:flezha, :steps, :failed]`, no re-run. A job that
completes before the start RPC confirms its binding is reported as a successful
start with no live `runner_id`. Lifecycle, durable loop, suspension, saga
compensation, cell retry and finished-job retention are in
[`../resilience/steps_relay.md`](../resilience/steps_relay.md).

Every class carries `durable` in its `RelaySettings`: `Durable::Memory`,
`Durable::Local` (the default) or `Durable::Remote`, the last two with a
`sleep_after`: `SleepAfter::RunDefault` (the default: a flow class never sleeps,
a steps class after six hours), `SleepAfter::Never` or `SleepAfter::After(d)`. An instance whose
class sleeps is evicted after that much idleness and wakes on its next message
with its Cells restored.

## Public API

```rust
use flezha_runner::{
    tasklet, ChannelBuilder, Ctx, Flezha, Tasklet, TaskletError, Relay,
};
```

`Flezha::create_runner(namespace)` returns the process's `FlezhaRunner`;
`register_factory(R)` / `register_factory(T)` declare classes on it;
`start()` connects (retried with backoff until the controller is reachable)
and `run()` is `start()` + `await_termination()`. The controller spawns
instances later with `flz relay start`. `Flezha::current_runner()` reaches
the started runner from code that does not control startup order. Every
fallible call returns `FlezhaError`, whose `code()` is one of these:

| Refusal | When |
|---|---|
| `topology.duplicate_factory_id` | a second `register_factory` of the same factory id on one runner — the declared set is per runner and starts empty, even though the class registry is process-global |
| `runtime.class_not_registered` | `unregister_factory` names a factory this runner never declared |
| `runtime.relay_type_in_use` | `unregister_factory` while live instances exist; the type stays defined |
| `runtime.runner_already_running` | a second `start()` in the process — at most one started runner per process |
| `topology.*` | reported at the declaring call site, never a panic |
| `runtime.runner_not_started` | `relays()` / `await_termination()` on a never-started runner |

Starting with zero declared classes is legal, and `register_factory` /
`unregister_factory` stay callable after `start()`; a post-start change ships
the runner's full factory set as an `UpdateFactories` control request —
fire-and-forget, no ack; the reconnect init frame is the resync of record. To
call them after `start()`, bind the runner: a chained
`Flezha::create_runner(ns)?.start()?` drops the temporary at statement end and
shuts the runner down; write `let runner = Flezha::create_runner(ns)?;` and
then `runner.start()?;` (`run()` is immune, because it blocks until shutdown).

## Writing a tasklet

A tasklet is your own type. `#[flezha_runner::tasklet(name = "...")]` sits on its
`impl Tasklet` block and generates no type: it writes the items the block leaves
out, which are `NAME`, the settings given as keys, the `*_OVERRIDDEN` flags from
the hooks the block writes, `CLOCK`, a default `init_state`, and `State = ()`,
`Change = NoChange`, `Output = ()`. The contract binding every SDK is
[`authoring_contract.md`](authoring_contract.md).

```rust
use flezha_runner::{tasklet, Change, Ctx, Tasklet, TaskletError};
use serde::{Deserialize, Serialize};

#[derive(Default)]
pub struct Parse;

#[tasklet(name = "parse_op")]
impl Tasklet for Parse {
    type Input = String;
    type Output = f64;

    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.emit(input.parse().unwrap_or(0.0))
    }
}

#[derive(Serialize, Deserialize, Default)]
pub struct Candles { closes: Vec<f64> }

#[derive(Serialize, Deserialize)]
pub struct Closed(f64);

impl Change<Candles> for Closed {
    fn apply_to(&self, state: &mut Candles) { state.closes.push(self.0); }
}

#[derive(Default)]
pub struct Ohlc;

#[tasklet(name = "ohlc_op", keys = ["1m", "1h", "1d", "1w"])]
impl Tasklet for Ohlc {
    type Input = f64;
    type Output = Vec<u8>;
    type State = Candles;
    type Change = Closed;

    fn cells_for(_: &f64) -> Vec<&'static str> { vec!["1m", "1h", "1d", "1w"] }

    fn run(&mut self, price: f64, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.change(Closed(price));               // applied and journaled
        ctx.emit(format!("{price}").into_bytes())
    }
}

// A linear pipeline needs no `channels()` body. The macro begins with the type
// it declares, which is what the author registers.
flezha_runner::relay! {
    pub struct Market;
    id: "market",
    pipeline: input::<String> -> Parse -> Ohlc -> plugin::<Vec<u8>>("ohlc_kafka")
}
```

`ctx.state()` reads the Cell's state and `ctx.change(c)` is the only way to
change it: it runs `c.apply_to(state)` and records `c` for the flusher. There is
no `&mut State` anywhere, so a mutation the runtime did not record does not
compile. Emits are typed as the tasklet's `Output`.

Hooks are declared by writing them: `event_time` (which makes the clock event
time), `on_timer`, `compensate` (steps saga), `cells_for`, `init_state`,
`migrate_state`, `migrate_change`. Settings are keys on the attribute:
`keys` (static mode, with a written `cells_for`), `max_cells` and
`idle_ttl_seconds` (dynamic-mode knobs; the key itself is declared on the
wiring with `.key_by(f)`), `capacity`, `schema_version`,
`min_readable_schema_version`, `compact_after_bytes`, `max_timers_per_cell`,
and `logical_clock`. Nothing is harvested: every relay a runner declares is
one the author registered. Fan-out and multi-input stay on an explicit
`channels()` body.

Both shorthands name their entry channel `input` unless the author says
otherwise: `input = "prices"` on the attribute, `input::<String>("prices")` on
the `relay!` pipeline head — the same position the `plugin` terminator
takes its name in.

### The short form

`#[flezha_runner::relay]`
declares a whole one-tasklet relay of the kind the attribute names. On a
function with no state, it emits two types: the relay under the function's
PascalCase name, which is what you register, and the tasklet beside it,
suffixed `Tasklet`. The function returns its output: a value emits, an
`Option` emits when present, `()` declares no output.

```rust
#[flezha_runner::relay(id = "upcase", output = "out")]
fn upcase(value: String) -> String {
    value.to_uppercase()
}

#[flezha_runner::relay(id = "place", output = "placed", input = "order")]
fn place(order: String) -> String {
    order
}

Flezha::create_runner("default")?
    .register_factory(Upcase)?
    .register_factory(Place)?
    .run()?;
```

The generated relay is the one you would have written by hand with that
single tasklet: an entry channel named `input` (or the `input` key's value),
the tasklet, a plugin output named by `output` when one is written, a factory id
equal to `id`, and every class-level setting at its trait default. The
tasklet (`UpcaseTasklet`) is named `id` verbatim, so its cells keep their
state path when the relay grows into the full form, and it wires into
another relay's `channels()` body like any tasklet. A steps short form
is a `once` steps relay, run once over its first message: `flz relay start
<id> --text '<payload>'`, or a start followed by `flz relay send`; a second
message to a `once` instance is accepted and never read. It may declare
another loop mode, `#[flezha_runner::relay(id = "chat", output =
"replies", each = "per_input")]`, and then runs once per message.

A one-tasklet relay with state or hooks puts the same attribute on its
`impl Tasklet` block. The attribute completes the block as `#[tasklet]` does,
names the tasklet after the `id`, takes the tasklet setting keys beside the
relay keys, and implements the relay trait on your own type, which is
what you register. `output` is optional; with none, the entry channel feeds the
tasklet and no plugin channel is declared: the relay that only keeps state.

```rust
#[derive(Serialize, Deserialize, Default)]
pub struct Seen { ticks: Vec<String> }

#[derive(Serialize, Deserialize)]
pub struct Ticked(String);

impl Change<Seen> for Ticked {
    fn apply_to(&self, state: &mut Seen) { state.ticks.push(self.0.clone()); }
}

#[derive(Default)]
pub struct Counter;

#[flezha_runner::relay(id = "counter", input = "ticks")]
impl Tasklet for Counter {
    type Input = String;
    type State = Seen;
    type Change = Ticked;

    fn run(&mut self, tick: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.change(Ticked(tick));
        Ok(())
    }
}

runner.register_factory(Counter)?;
```

Anything more is the full form: a second tasklet, a fan-out, a merge, a keyed
edge, a retry ladder, the effectively-once tier, a correction
window. Rust names no entry codec — an entry decodes by its declared type — so
the short form takes no `codec` key.

The refusals are compile errors. An `output` on a unit return, or on a block
whose `Output` is `()`, fails with a message beginning `topology.output_without_type`; a
function with a `&mut` parameter fails with one beginning
`ShortFormStateOnFunction`; a missing `id` names the attribute. A hand-written
`Relay` impl beside a short form is rustc's
conflicting-implementation error, which the contract calls
`topology.short_form_with_topology`. `#[flezha_runner::tasklet]` declares a tasklet only:
it refuses `output`, `input`, `kind` and `on_full`, a function target, and a
block that also carries a short-form attribute.

## The reference form

An `impl Tasklet` block without the attribute writes every item the attribute
would add. It is what the attribute expands to, it composes with the everyday
form in one relay, and it is shown here only: the guides always use the
attribute.

```rust
use flezha_runner::{ChannelBuilder, ClockKind, Ctx, Flezha, NoChange, Tasklet, TaskletError, Relay};

#[derive(Default)]
struct ParseTask;

impl Tasklet for ParseTask {
    type Input = String;
    type Output = f64;
    type State = ();
    type Change = NoChange;   // opts out of persistence
    const NAME: &'static str = "parse_op";
    const CLOCK: ClockKind = ClockKind::Wall;

    fn init_state(_: Option<&str>) {}

    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        let price: f64 = input.parse().map_err(|e: std::num::ParseFloatError| {
            TaskletError::Custom(e.to_string())
        })?;
        ctx.emit(price)
    }
}

struct MarketRelay;
impl Relay for MarketRelay {
    fn factory_id() -> &'static str { "market" }

    fn channels(c: &ChannelBuilder) {
        c.input::<String>("trade.raw")
            .into::<ParseTask>()          // ParseTask's output stream handle
            .named("trade.parsed")
            .into::<OhlcTask>()           // static-mode consumer, see below
            .output_named("ohlc.candles");  // plugin-bound terminal, named here
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    Flezha::create_runner("market_ns")?.register_factory(MarketRelay)?.run()?;
    Ok(())
}
```

In this form nothing is derived, so the author also writes each flag the
attribute would set: a static-mode Tasklet (`OhlcTask`) declares `cell_keys()`,
writes `cells_for(&Input) -> Vec<&'static str>` and sets
`const CELLS_FOR_OVERRIDDEN: bool = true` (a keyset without it is
`TopologyIssue::CellsForMissing`); a written `compensate` needs
`COMPENSATE_OVERRIDDEN = true` to run on a rollback, and a written `max_cells`
or `mailbox_capacity` its `*_OVERRIDDEN` flag. A forgotten flag silently keeps
the hook off, which is the reason the everyday form derives them. `CLOCK` is
`ClockKind::EventTime` for a tasklet that writes `event_time`, whose default
panics naming the tasklet; `ClockKind::Logical` needs a real `Change`, or
registration fails with `topology.unservable_clock`.

A stateful Tasklet names its `Change`, a type implementing `Change<State>` whose
`apply_to` performs the mutation. A change enum is encoded by variant index,
so **a new variant goes at the end**: inserting one in the middle moves the
index every later variant was saved under. Reordering, removing or retyping a
variant or a field is a non-additive edit that needs a `SCHEMA_VERSION` bump
([`authoring_contract.md` §3.0](authoring_contract.md#30-changing-a-state-or-change-type-after-it-has-been-deployed)).
`run`, `apply_to` and `compensate` must be deterministic — time via
`ctx.now()`, ids via `ctx.stable_id()`. Other items: `on_timer`,
`max_timers_per_cell` (10 000), `compact_after_bytes` (8 MiB),
`SCHEMA_VERSION`. Saved state is keyed by `NAME`, never by the Rust type, so a
type can be renamed freely; renaming `NAME` declares a different tasklet.

### Java SDK parity

The Java SDK is the same model with one authoring form: the author's class
implements `Tasklet<Input, State, Output>` and carries `@Tasklet.Spec` for its
name and settings, and the three types are read off its `implements` clause,
so bounds Rust checks at compile time become
`FlezhaException`s from `registerFactory(...)`, each carrying its `topology.*` code;
a written `cellsFor`, read off the class at registration, replaces the
`CELLS_FOR_OVERRIDDEN` const;
dynamic mode is read off the wiring on both sides, with
`@Tasklet.Spec(idleTtlSeconds, maxCells)` as its knobs; zero-copy
passes the value by reference, so the producer must treat an emitted value as
immutable. Nothing is generated: a change is a record the author writes, and a
`@Relay` tasklet class is a whole short-form
relay, built when the application registers it with
`registerFactory`. Persisted
Changes declare `sdk_lang = 2`, `change_format_id = 4` (Kryo). A Java
`Relay` emits `RelayCompleted` byte-identically to Rust.

## Hot-path dispatch

1. **Wire frame → entry channel** — `metadata["channel"]` is resolved through
   `RelayTopology::name_to_id` (`HashMap<&'static str, ChannelId>`), then
   a `Vec` index into `topology.channels`. A frame with no `channel` key goes
   to the class's first declared `Entry` channel; a name that is not one of the
   class's `Entry` channels — undeclared, or declared as an output or an
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
`init_state(Some(key))`) in the Tasklet's `DynamicCellRegistry`.
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
Relay. `--partitions N` starts N ordinary instances (`<id>#p0` …
`<id>#p{N-1}`), each owning a contiguous range of the 256 key groups a
routing key hashes into (`KeyGroupRange` on the `RelayInstance`). Nothing in
this doc changes for a member; the one dispatch addition is a range check on
each computed cell key that parks a mis-route before any state for that key
exists. A class with no keyed entry-fed tasklet refuses to be partitioned.
Group model, sources, forwarding, rescale: [`partitioned_relays.md`](partitioned_relays.md).

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
allocated at relay startup. Capacity is declared per Tasklet; the
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
| `DropNothing` (default, either run) | Nothing is discarded, and **how** that is realized depends on who produced the value. A producing Cell's `ctx.emit` never parks: the push is held in that Cell's push holdback, the Cell is handed no further activation until it drains, and the consumer's next free slot drains it. The wire reader, which has no producing Cell, retries after a bounded 50 ms park on the consumer's `Waiters`, woken per pop — the one park left, and what carries ingress backpressure to the controller. Preserves ordering either way, loses nothing, never stalls indefinitely; a producer blocked longer than 5 s logs one `flezha::mailbox` warning naming the consumer. |
| `DropNewest` | The incoming item is discarded; `drops_newest_total` += 1. |
| `DropOldest` | The mailbox pops its oldest item and accepts the new one (`force_push`); `drops_oldest_total` += 1 **and `pushes_total` += 1** — the force-push admitted, so it is a successful enqueue by that counter's definition. Which item is evicted is best-effort under MPMC contention; the evicted one is named in a `flezha::mailbox` debug line. |

A **replay-slice member ignores the declared policy and takes the `DropNothing`
path**, whatever the channel says: a correction's answer may not depend on
arrival timing, and a dropped member would leave the slice open, short, and
folded anyway — a wrong answer with nothing reported.

Declaring a dropping policy on a class that also declares `exactly: effectively_once`
is **refused at build time** (`TopologyIssue::DroppingPolicyUnderEffectivelyOnce`,
and its C-ABI twin at declare time): a shed input's offset is still advanced
past by the tier's watermark, so the loss is permanent under a contract that
promises nothing is lost. A correction window requires the tier, so it is
covered by the same refusal.

A `PluginBound` channel has no SDK-side mailbox between the emit and the
wire, so a policy declared on it is never consulted; backpressure there is the
socket buffer plus transport flow control.

The entry default has a co-tenancy cost: the wire reader services every
Relay on one physical connection, and under `DropNothing` it parks on the
saturated Relay's mailbox until the pool pops a slot — siblings wait with
it. A Relay that prefers to shed (a live ticker where a stale price is
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

On a channel with a plugin output the dispatcher serializes the value once
(`output()` / `output_named(name)` use the default rules — passthrough for
`Vec<u8>`, UTF-8 for `String`, JSON otherwise; `output_with(serializer)` /
`output_named_with(name, serializer)` supply a `Fn(&T) -> Vec<u8>`), builds a frame with `payloader::build_from_binary`
carrying `channel = <channel name>` in the metadata, and writes it on the
`FlezhaStream` the originating input arrived on. The controller's plugin
layer (for example the Kafka plugin package's producer with its `:channel` option)
gates on `metadata["channel"]`.

`ctx.emit_with_meta::<C>(value, meta)` appends user metadata to the plugin
frame only; it returns `runtime.metadata_on_inter_tasklet_channel` on an
inter-tasklet channel without an output, `runtime.reserved_metadata_key` for any key
under the reserved `flz.` prefix, and silently replaces a user-supplied
`channel` entry with the real channel name. A channel declared `.keyed()`
(requires an output) stamps `flz.cell` / `flz.seq` / `flz.key` on every plugin
frame: `emit` carries the Cell's own key (a single-cell Tasklet fails
with `runtime.key_required_on_keyed_channel`), `emit_keyed(key, value)` carries a
replay-stable natural key (`runtime.key_not_allowed_on_non_keyed_channel` elsewhere).

### Parts

`ctx.part(value)` sends a preview of the output the invocation is producing
while it runs, from `run` or `on_timer`, in a flow relay and in a
steps one's stage alike. It takes no channel: it previews the tasklet's own
output channel, and the value is serialized as an emit of it on that channel
would be. The context calls `flz_core::part::send`, which frames the part under
the payload id `output_part` with `channel`, `flz.part` (from 0, per attempt),
`flz.part_attempt`, `flz.cell`, `flz.relay` and, for a keyed Cell,
`flz.key`, and writes it without waiting. The relay's listeners receive it;
an output never does, and it is never durable, replayed or counted as an output.

```rust
fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
    for partial in ["he", "hel", "hell"] {
        ctx.part(partial.to_string())?;
    }
    ctx.emit("hello".to_string())
}
```

A part that cannot leave at once (no stream, a local run, a stream that would
block, a frame over the limit) is dropped and counted, and a replayed input
sends none. A channel that does not end in an output (it only feeds another
tasklet, or ends in `call(...)`) answers `TaskletError::Part` with the code
`part.no_output`. A `compensate` hook previews no work item and answers
`part.outside_invocation`; the testing harness sends nothing.

### Hybrid and terminal: one `output` method

`output` / `output_named` / `output_with` / `output_named_with` on a producer's output
handle attach the channel's `plugin_sink: Option<PluginSink>` — the one
representation of plugin egress. The `_named` half names the channel, which is
what rides the frame; naming one channel with both an output and `.named(...)` is
`TopologyIssue::ChannelNamedTwice`. `kind` is derived at build time: `InterTasklet` when consumers are wired
after the output (hybrid — one typed value reaches the consumer mailboxes and,
serialized, the plugin), `PluginBound` when nothing consumes the channel
(terminal). Order of `output_with` and `.into::<S>()` is irrelevant; a second
output on one channel is `TopologyIssue::DuplicatePluginOutput`.

```rust
sma_stream                       // SmaTask's output handle, from `.into::<SmaTask>()`
    .output_named_with("sma_out", sma_to_pb_bytes)
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

`.call::<A>(target, deadline_ms)` is the channel's other plugin end, one per
channel. A call end keeps the transform and key of the chain
that closed on it, as an edge does: each value emitted on the channel becomes the
requests that chain yields, each serialized by the output rule and stamped with the
call's target, deadline, Response channel and key before it leaves as a plugin
output. `call` answers an `EntryStream<Response<T, A>>` on the channel
`<channel>.response` ([`calls.md`](calls.md#relay-calls-call)).

## Per-Cell state and persistence

Each `CellSlot` holds `CellState<S> { tasklet: S, state: S::State }`
behind a `Mutex` the inflight CAS keeps uncontended: the Tasklet value is a
per-Cell scratch buffer, `S::State` is the domain state, `init_state` runs
once per Cell and later runs change it through `Ctx::change`.
State migration (`MigrationSerialization` / `CellState`) walks the Cells
per Tasklet and produces the JSON shape in
[`protocol_spec.md`](protocol_spec.md) "Relay state JSON shape".

Persistence rides the `Change` trait: `ctx.change(change)` is a `Vec::push`
on the hot path; serialization, coalescing and the wire send happen on one
per-process flusher thread on a 5 ms linger (`FLEZHA_FLUSH_LINGER_MS`).
A tasklet that leaves `Change` at `NoChange` opts out. The class's `durable`
(default `Durable::Local`) is frozen at instance start and announced to the controller, which a user can override
per class with `:relay_persistence`. Trait surface, wire format,
`TaskletTestHarness` (`replay_equals`, `replay_emits_equal`) and the Java
port: [`state_persistence.md`](state_persistence.md).

## Limits

- **Per-Tasklet `handle_message` is not served by the SDK**: the
  `RelayMessage` command is answered `relay does not support
  handle_message` (`flz_core/src/command/relay_message.rs`); controller-side
  handlers are in [`../plugins/message_handlers.md`](../plugins/message_handlers.md).
- **One started runner per process** (`FlezhaError::AlreadyRunning`).
- **`UpdateFactories` has no ack**; the reconnect init frame is the resync.
- **`unregister_factory` cannot see a suspended durable steps instance**:
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
| `Relay` trait; `Tasklet`, `Ctx`, `Change`, `ClockKind`, `TaskletError` | `sdk/rust/src/relay/mod.rs`; `sdk/rust/src/tasklet/{mod,ctx,change,clock,error}.rs` |
| `ChannelBuilder`, `Stream<T>`, `TopologyIssue` | `sdk/rust/src/channel/builder/`, `sdk/rust/src/channel/mod.rs` (kernel types: `flz_core/src/channel.rs`) |
| Topology builder, mailbox-capacity resolution | `sdk/rust/src/relay/builder.rs` |
| Authoring macros | `sdk/rust/macros/src/{tasklet,short_form,relay}.rs` |
| Typed dispatcher and plugin-output tail; poll-worker pool | `sdk/rust/src/relay/dispatcher.rs`, `poll_pool.rs` |
| Poll engine (handout, admission, ladder, deferred wakes) | `flz_core/src/poll_core/` |
| `RelayInstance`, `KeyGroupRange`; `RelayTopology`, `CellMode`; registries | `flz_core/src/relay/{instance,topology,registry}.rs` |
| `CellSlot`, `CellCounters`; `Mailbox`, `Waiters`; dynamic registry and reaper | `flz_core/src/cell.rs`, `mailbox.rs`, `dynamic.rs` |
| Wire dispatch (`RelayDispatchHandle`); wire input routing | `flz_core/src/routing.rs`, `flz_core/src/command/streams_connect.rs` |
| Enqueue, overflow policies, `send_plugin_frame`; Change flusher | `flz_core/src/routing.rs`, `flz_core/src/flusher.rs` |
| Relay state JSON shape, `apply_init_values` | `flz_core/src/command/cell_state.rs` |
| `Relay`; `TaskletTestHarness` | `sdk/rust/src/steps/mod.rs`, `flz_core/src/steps/mod.rs`; `sdk/rust/src/testing/harness.rs` |

## See also

- [`authoring_contract.md`](authoring_contract.md) — the cross-SDK authoring contract
- [`poll_core.md`](poll_core.md) — the kernel boundary the SDK is hosted on
- [`state_persistence.md`](state_persistence.md) — `Change`, WAL wire format, restore
- [`../architecture/flow_profile.md`](../architecture/flow_profile.md): a flow relay end to end
- [`../internals/overwatch_lifecycle.md`](../internals/overwatch_lifecycle.md) — the controller-side Overwatch that starts and feeds an instance
