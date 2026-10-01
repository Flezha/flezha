# Elixir SDK

For Elixir developers authoring operators and for SDK implementers. The Elixir runner SDK
gives an Elixir application the Operator/Tasklet/Cell model of the Rust and Java SDKs,
deployable two ways from one core: **in-node** (`:native`) inside the Controller's own BEAM
node over message passing, or **standalone** (`:uds` / `:quic`) as its own BEAM node speaking
the binary protocol, indistinguishable from a Rust runner to the Controller. Everything above
the transport seam is shared code with identical semantics in both modes. The SDK inherits
nothing from `flz_core` ([`../architecture/runner.md`](../architecture/runner.md#one-change-every-sdk)).

**Support tier:** [tier 1](../publishing/maturity.md#sdk-elixir), run by the maintainer in
a live deployment, passes the full conformance run in CI and is permanently supported.

## Local mode: run a graph with no controller

The shortest way to see an operator work, and a third way to run one beside
the two modes below: **no controller at all**. `Flezha.SDK.Local.start/2` takes
the `Flezha.SDK.Operator` module you deploy, compiles the topology its
`channels/1` already declares, and hands sink output to a function:

```elixir
{:ok, run} = Flezha.SDK.Local.start(WordCount)
:ok = Flezha.SDK.Local.on_sink(run, "counts", &IO.puts(&1.payload))
:ok = Flezha.SDK.Local.feed(run, "lines", "the quick brown fox")
:ok = Flezha.SDK.Local.run_until_idle(run, 5_000)
:ok = Flezha.SDK.Local.stop(run)
```

`on_sink/3` registers against the sink's **channel** name, which is the
producing tasklet's name unless the declaration `named/2` gave it one.
`feed/3` enters through the same `Flezha.SDK.Operator.Inbox.input/4` a frame
off a bound data stream enters, so the channel's declared decode and admission
run as deployed. A worked two-tasklet example is in
[`../quickstart.md`](../quickstart.md).

**State is held in memory and nothing is durable** — no write-ahead log, no
snapshots, no dead-letter queue. Every run says so on stderr and it is not
suppressible. Durability, effectively-once delivery, plugins, placement and
migration, the dead-letter queue, observability export, authz and multi-runner
scale need a controller; a declaration asking for one of those is refused at
startup, naming it. This SDK expresses local mode in its own core, as it does
everything else. Local mode ships in every other SDK too. Walk-through:
[`../quickstart.md`](../quickstart.md).

## Choosing a mode

| | In-node (`:native`) | Standalone (`:uds` / `:quic`) |
|---|---|---|
| Where | Controller's BEAM | Own BEAM node |
| Isolation | crashes contained per process; **memory/scheduler shared** with the Controller | full OS-process isolation |
| Use when | trusted, latency-sensitive, co-located operators | untrusted or resource-unpredictable operators, cross-node deployment |
| Security | a function call; a connect from another BEAM node is refused (`:remote_node_rejected`) | UDS filesystem perms / QUIC mutual TLS |

`:native` is the default and is **not** env-derived: a controller sets `FLEZHA_TRANSPORT=uds`
for its *server*, and an env-following in-node SDK would dial its own node over a socket.
Standalone deployments pass `:uds` / `:quic` explicitly; connection parameters come from the
env the Rust client reads (`FLEZHA_SOCKET_FILES_PATH`, `FLEZHA_QUIC_ADDRESSES`,
`FLEZHA_CA_CERT_PATH`).

## Registration

Runner-as-handle; errors are `{:error, %Flezha.SDK.Error{}}` returned at the call site (never
a raise) and propagate through a pipe. The error's `code` is one of
[the error codes](../errors.md); this page names each refusal by its code.

```elixir
Flezha.SDK.create_runner("my_namespace", transport: :native)
|> Flezha.SDK.register_factory(MyOperator)
|> Flezha.SDK.run()   # start/1 then await_termination/1

# or under the host supervisor — same sequence, reconnects with the same stable id:
children = [{Flezha.SDK, namespace: "my_namespace", operators: [MyOperator], transport: :native}]
```

- `create_runner/2` returns a handle — no connection, no processes, no slot claim.
- `register_factory/2` takes a module of the `Flezha.SDK.Operator` behaviour, which the `use`
  macro sets; any other module answers
  `{:error, %Flezha.SDK.Error{code: "topology.no_operator_exported"}}`. It reads and checks the
  class's settings tree, so how the class runs is read from the class, never named by the call.
  The runner announces each factory's run (`flow`, or `steps` with its `each` and `on_failure`)
  with its id, in the handshake and in every `UpdateFactories`, so a start names the factory
  and nothing else.
- `register_factory/2` / `unregister_factory/2` work before and after `start/1`; a post-start change
  ships `UpdateFactories`; removing a type with live instances refuses
  `runtime.operator_type_in_use`, naming the factory id and the live count.
- `start/1` claims the process-wide runner slot (loser: `runtime.runner_already_running`)
  and connects; `run/1` = `start/1` + `await_termination/1`; `close/1` disconnects.

## Authoring

Two `use` macros write the behaviours below. Each takes the scalar settings
as options named after the callback they define, and **defines only the options
written**, so every default stays exactly where it was — including the ones an
environment knob decides.

```elixir
defmodule MyOperator do
  use Flezha.SDK.Operator, factory_id: "my_op"

  @impl true
  def channels(c), do: c |> input("in") |> into(MyTask) |> sink("out")
end

defmodule MyTask do
  use Flezha.SDK.Tasklet, name: "my_task"

  @impl true
  def run(input, state, ctx) do
    emit(ctx, transform(input))
    {:ok, state}
  end
end
```

The operator macro imports `Flezha.SDK.Channels`, so a whole pipe — a merge
included — is written with no module prefix. `use Flezha.SDK.Tasklet` imports
`Flezha.SDK.Ctx`'s authoring functions (every one but `apply/3`, which would
shadow `Kernel.apply/3`) and leaves `init_state/1` answering `nil` unless the
module writes one, so a stateless tasklet writes only `run/3`. A tasklet ends
its instance with `complete(ctx, result)`: in a class that runs in steps once its
iteration commits, in a flow class once the instance's Cells hold nothing.

An unknown option is a `CompileError` naming it, and so is a missing required
one (`name` for a tasklet, `factory_id` for an operator). `idle_ttl_seconds`
takes a positive integer and `logical_clock` a boolean; any other value is a
`CompileError` naming the option. The retired `cell_idle_ttl_ms` and `clock`
options, and a `clock/0`, `event_time_of/1` or `cell_idle_ttl_ms/0` written in
the module with `def` or `defp`, are `CompileError`s naming what replaced them.
A hand-written `apply_change/2` is one too: in the `use` form a change is
written with `defchange` ([Persistence](#persistence)).

### The settings tree

One settings tree says how a class runs and where its work is kept. Every
setting is optional:

```elixir
defmodule Checkout do
  use Flezha.SDK.Operator,
    factory_id: "checkout",
    run: {:steps, each: :chained, on_failure: :keep_progress},
    durable: {:local, sleep_after: 60_000},
    description: "reserve, charge, ship",
    max_duration_ms: 600_000

  @impl true
  def channels(c), do: c |> input("in") |> into(Reserve) |> into(Charge)
end
```

- `run` is `:flow` (the default) or `:steps`. `{:flow, exactly: exactly}` takes
  `:at_least_once` (the default) or `:effectively_once`, written with members as
  `{:effectively_once, ack: ack, correction_ms: ms, beyond_window: :drop}`; `ack` is
  `:per_input` (the default), `:checkpoint` or
  `{:checkpoint, every: n, max_delay_ms: ms, outputs: :at_checkpoint}`.
  `{:steps, each: each, on_failure: on_failure}` takes `each` `:once` (the default),
  `:per_input` or `:chained`, and `on_failure` `:undo_all` (the default) or `:keep_progress`.
- `durable` is `false` (in memory: a crash starts over), `:local` (the default) or `:remote`,
  the last two optionally written `{:local, sleep_after: ms}`: an idle instance sleeps after
  that long (by default never for a flow class and six hours for a steps class, `false` never)
  and wakes on its next message with its
  Cells restored. `:remote` is for a class that runs in steps; a flow class declaring it is
  refused `topology.remote_on_flow`.
- `max_duration_ms` is a wall-clock stop for either run: a steps instance unwinds and ends
  `cancelled`, a flow instance ends `cancelled` once its Cells hold nothing.

A setting written under a parent that does not carry it (`each` under `:flow`, `ack` under
`:steps`, `sleep_after` under `false`) is a `CompileError` naming the setting and the
parent, and so is a value outside its choices. A hand-written `run/0` or `durable/0`
answering the same tree meets the same check at `register_factory/2`, as
`topology.unknown_field` or `topology.invalid_setting`. The one combination refused is
`:effectively_once` with `durable: false`, `topology.effectively_once_without_persistence`:
the tier is durable state.

> A tasklet nested inside an operator module inherits that module's imports
> lexically, so `use Flezha.SDK.Tasklet` takes the builder's back — a nested
> tasklet's own `input/0` would otherwise clash with `Channels.input/2`. A
> nested tasklet must be written **before** the `channels/1` that names it,
> which is how Elixir aliases resolve.

### The short form

An operator module that writes its one tasklet's `run/3` instead of
`channels/1` is a whole single-tasklet operator, registered as any operator is.

```elixir
defmodule Upcase do
  use Flezha.SDK.Operator, factory_id: "upcase", sink: "out"

  def run(input, state, ctx) do
    emit(ctx, String.upcase(input))
    {:ok, state}
  end
end

defmodule Place do
  use Flezha.SDK.Operator, factory_id: "place", sink: "placed", input: "order", run: :steps

  def run(input, state, ctx) do
    emit(ctx, input)
    {:ok, state}
  end
end

children = [{Flezha.SDK, namespace: "default", operators: [Upcase, Place]}]
```

The module generates the nested tasklet module `Upcase.Tasklet`, whose
`name/0` is the factory id verbatim and which delegates to the tasklet
callbacks the module defines, and a `channels/1` wiring an entry channel
named `"input"` (or the `input` option's value) into it and, with `sink`, it
to that plugin sink. The class settings it takes are the full form's, and every
one it does not write keeps its default. Because the tasklet is named after the
factory id, its cells keep their state path when the module grows into the full
form. `Place` runs in steps with `each: :once`, run once over its first message:
`flz operator start --text '<payload>'`, or a start followed by
`flz operator send`; a second message to a `once` instance is accepted and
never read.

```elixir
defmodule Chat do
  use Flezha.SDK.Operator, factory_id: "chat", sink: "replies", run: {:steps, each: :per_input}

  def run(turn, state, ctx) do
    emit(ctx, turn)
    {:ok, state}
  end
end
```

`Chat` runs its tasklet once per message and does not complete after the first.

`run/3` is written as in a tasklet: the ctx functions are imported, a `state`
block and `defchange` work, and the options `use Flezha.SDK.Tasklet` takes
(`max_cells:`, `logical_clock:`, ...) apply to the generated tasklet. `sink` is
optional; with none, nothing leaves the tasklet but its state:

```elixir
defmodule Counter do
  use Flezha.SDK.Operator, factory_id: "counter", input: "ticks", max_cells: 10

  state do
    field(:seen, :list, default: [])
  end

  defchange seen(state, item) do
    %{state | seen: state.seen ++ [item]}
  end

  def run(input, state, ctx), do: {:ok, seen(state, ctx, input)}
end
```

Anything more is the full form: a second tasklet, a fan-out, a merge, a keyed
edge, an edge's retry ladder.
Elixir names no entry codec — binaries pass through and `input_with/3` takes a
parser — so the short form takes no `codec` option.

The refusals are `CompileError`s. `run/3` beside `channels/1` begins
`topology.short_form_with_topology`, and writing neither names both. A
short-form option on a full form names the option, and `use Flezha.SDK.Tasklet`
refuses `sink` and `input`, naming the operator macro. `event_time/1` beside
`logical_clock: true` names both and the module the author wrote. This SDK cannot see a tasklet's
output type, so it owes no `topology.sink_without_output` and its manifest does not list
`shorthand_typed_output`.

### The behaviours beneath

The plain `@behaviour` form compiles as well, and is what the macros expand
to. It is the reference form, shown on this page and in no guide: every
callback is written by hand, and a change is recorded through the module's own
`apply_change/2` and `Flezha.SDK.Ctx.apply/3`.

```elixir
defmodule HandWritten do
  @moduledoc false
  @behaviour Flezha.SDK.Tasklet

  @impl true
  def name, do: "hand_written"
  @impl true
  def init_state(_key), do: 0
  @impl true
  def idle_ttl_seconds, do: 1
  @impl true
  def event_time(input), do: String.to_integer(input)
  @impl true
  def apply_change(count, {:add, n}), do: count + n

  @impl true
  def run(input, count, ctx) do
    Ctx.emit(ctx, Integer.to_string(Ctx.now(ctx)))
    {:ok, Ctx.apply(ctx, count, {:add, String.to_integer(input)})}
  end
end

defmodule HandWrittenOperator do
  @moduledoc false
  @behaviour Flezha.SDK.Operator

  @impl true
  def factory_id, do: "hand_written_operator"

  @impl true
  def channels(c) do
    c |> Channels.input("in") |> Channels.into(HandWritten) |> Channels.sink("out")
  end
end
```

`Ctx` and `Channels` are aliases of `Flezha.SDK.Ctx` and `Flezha.SDK.Channels`.
No macro sees this form, so what the `use` form refuses at compile time is
refused at registration instead: both clocks as `topology.two_clocks`, and a
retired `clock/0`, `event_time_of/1` or `cell_idle_ttl_ms/0` as
`topology.retired_callback`, naming the callback and its replacement.

`run/3` returns `{:ok, new_state} | {:error, reason} | {:error, {:non_retryable, reason}}`;
emits flush only on `{:ok, _}`. Optional `Tasklet` callbacks: `cell_keys/0` (static
keyset), `cells_for/1`, `mailbox_capacity/0` (1024; env `FLEZHA_MAILBOX_CAPACITY`, range
`1..1_048_576`), `max_cells/0` (100_000 — the cap on a dynamic node's live Cells: at it
a new key's input is dropped and counted, an existing key's is served),
`idle_ttl_seconds/0` (nil; see [Dynamic cells](#dynamic-cells)), the persistence
three — `apply_change/2` (declares a changeful, persisted tasklet; see
[Persistence](#persistence)), `compact_after_bytes/0` (8 MiB) and `state_from_json/1` — and
the schema four — `schema_version/0` (1), `min_readable_schema_version/0` (the version),
`migrate_state/2`, `migrate_change/2` (see [Schema evolution](#schema-evolution)) — and the
timer four — `logical_clock/0` (`false`), `event_time/1`, `on_timer/4`, `max_timers_per_cell/0`
(10 000; see [Timers and clocks](#timers-and-clocks)).
`Operator` takes the optional `description/0`, `max_duration_ms/0`, `run/0` and `durable/0`
([The settings tree](#the-settings-tree)); a class that runs in steps writes the same
`channels/1` and has no retry setting of its own, because a stage's ladder is the edge's
`retry_max` / `retry_backoff_ms`, as a flow operator's is.
`Flezha.SDK.Ctx`: `emit/2`, `emit_with_meta/3` (a reserved `flz.` key answers
`runtime.reserved_metadata_key`), `emit_keyed/3` and `emit_keyed_with_meta/4` (the
output's natural key on a sink declared `keyed/1`; on a node with no keyed sink they answer
`runtime.key_not_allowed_on_non_keyed_channel`, and a keyless emit from a
Single Cell on a keyed sink answers `runtime.key_required_on_keyed_channel`),
`part/2` (a preview of the invocation's output, sent at once rather than when `run/3`
returns: encoded as an emit of the value would be, numbered `flz.part` from `0` within the
attempt and stamped with `flz.part_attempt`, `flz.cell` and `flz.key`; it goes to the
operator's listeners and to nothing else, never waits, is dropped and counted when the wire
cannot take it at once, and is sent by no replayed input; it answers `part.no_sink` when the
node's channel ends in no sink, a `response/1` or `call/3` end included, and `part.outside_invocation`
through a ctx whose invocation returned),
`apply/3` (fold a change and record it; a `defchange` recorder calls it),
`complete/2` (end the instance with a result, encoded as `spawn_steps` encodes its
payload), `now/1` (the declared clock's value, once per invocation; the scheduled timestamp
inside `on_timer/4`), `cell_key/1`, and the timer four
— `timer_after/2` (milliseconds after `now/1`), `timer_at_event_time/2`,
`cancel_processing_time_timer/2`, `cancel_event_time_timer/2` — the registrations answering
`{:ok, at}`, the absolute timestamp a cancel takes, or
`runtime.event_time_timer_without_event_time_clock` /
`runtime.timer_cap_exceeded`; inside a steps operator's tasklet all four answer
`runtime.timers_unavailable` (see [Timers and clocks](#timers-and-clocks)).
`spawn_steps(ctx, factory_id, payload, opts)` requests spawn-or-deliver of a steps operator
instance, `id:` and `if_running:` optional, with `payload` sent as a sink with no codec sends a
value (bytes as they are, anything else as JSON; a term with no JSON form raises), and answers
a `Flezha.SDK.Spawn` handle at once that resolves to `{:ok, %{id: id, outcome: outcome}}`; the contract is
`docs/sdk/tasklet_requested_spawn.md`. A spawn without `id:` always starts a new
instance, starts a second one when reissued after a lost answer, and answers an empty id when
skipped on replay; pass `id:` when that matters.

## Graph builder (v1 subset)

`input`/`input_with` → `into`/`into_named` → `keyed` → `sink`/`sink_with`, with
`map`/`filter`/`flat_map`/`key_by` as edge transforms (fused into one closure per consumer
edge), plus `named`, `on_full`, `retry_max`, `retry_backoff_ms`, and `merge/1` with
`route_latest/4` ([`multi_input_tasklets.md`](multi_input_tasklets.md)). Fan-out is handle reuse,
or `branch/2`, which calls each arm of a list with the same stream and answers it;
unnamed mentions of one module unify (fan-in). A sink names the channel it terminates:
`sink("input")` makes the frame `channel = "input"`, which is what reaches a forward
target's `input` entry, and `sink/1` leaves the name derived — `named/2`'s name, else the
tasklet's `name/0`. Naming one channel twice is refused. `response/1`, or `response_with/2`
with a serializer, ends a channel on the answer to an outside call instead of a sink
([`calls.md`](calls.md)). `call(handle, "pricing", deadline_ms: 500)`, or
`call(handle, factory: "agent", deadline_ms: ..., create: false)` after a `key_by/2`, ends a
chain on a call to another operator and answers a handle on its Response channel, whose
values are `%Flezha.SDK.Response{}` ([`calls.md`](calls.md#operator-calls-call)). The user's metadata rides the frame. Cell mode is derived: non-empty `cell_keys/0` → static; a keyed edge into a
keyless tasklet → dynamic; else single. Refusals accumulate into one
`{:error, %Flezha.SDK.Error{}}` carrying the first issue's code and every issue in
`details.issues` — e.g. `topology.transform_after_key_by`, `topology.retry_max_zero`,
`topology.channel_named_twice` (`named/2` beside a `sink` that names),
`topology.keyed_channel_without_plugin_wire` (`keyed/1` on a channel with no sink),
`topology.deferred_builder_surface` (names the operator module).

The names are the reference vocabulary every SDK answers with
(`flz_core/src/host/issue.rs`), and this SDK is the one that re-expresses rather
than submits: it is its own core and inherits nothing from `flz_core`, so what
it shares is the fixture — `test/fixtures/reference_topologies.txt`, one row per
refusal, driven against every SDK by the same conformance run
([`conformance.md`](conformance.md) § The refusal table).

The effectively-once tier is a class setting, not a builder call
([The settings tree](#the-settings-tree)). A `beyond_window` written with no `correction_ms`
is a zero window, refused, because a fate for a window nobody declared is an authoring
error. The tier's refusals are `topology.checkpoint_every_zero`,
`topology.at_checkpoint_without_max_delay`, `topology.unkeyed_channel_under_effectively_once`
(naming the sink's channel), `topology.dropping_policy_under_effectively_once` (naming the node's inbound
channel and the policy), `topology.correction_without_event_time` (naming a
Tasklet and the clock it declares) and `topology.correction_window_zero`.

## Keyed channels

`keyed/1` on the handle before `sink/2` declares output identity on that sink — the
reference's `.keyed()`. Every frame it produces carries `flz.cell`
(`<operator id>/<tasklet name>/<cell key or empty>`), `flz.seq`
(`<change_seq at handout>.<emit ordinal>` — the seq the invocation started at, before its own
records advanced it, and the ordinal counting the invocation's plugin outputs from zero,
stamped or not) and `flz.key`, appended after the user's metadata so a stamp cannot be
clobbered. The key is the emit's own (`emit_keyed/3`), else the sink route's `key_by`, else
the emitting Cell's; a Single Cell with none is refused at the emit. A sink not declared
keyed carries no `flz.` key. The stamps are minted at the routing seam in the Operator, this
SDK's own core. Under the effectively-once tier the Cell captures the same output through
the same seam into its effect record and stamps nothing: the controller derives
`flz.cell`, `flz.seq` and `flz.epoch` at flush, so a host that also stamped would produce a
second answer.

## Overflow, retry, park

The overflow default is `:drop_nothing` for **every** edge kind, in lockstep with the
Rust/Java SDKs and the FFI host: it waits for room and never drops, at the cost that a delayed
primary-stream router delays every operator fed by that stream. `on_full(:drop_newest | :drop_oldest)` sheds
instead, and every shed item is counted: `drops_newest` / `drops_oldest` ride the heartbeat
beside `dispatches` and `blocked_parks`, the same keys the other SDKs report, and the
`[:flezha, :sdk, :mailbox]` telemetry event stays the in-node handle on the same drop.

**This SDK was the reference design for how `flz_core` now waits.** A full mailbox never blocks
a scheduler here: `handle_call({:enqueue, …})` DEFERS its reply and holds the value in a
`waiters` FIFO, and each dequeue hands its freed slot to the oldest waiter. A push that finds a
free slot in the Cell's `Flezha.SDK.Cell.Room` is a cast instead, so the router never waits on
a Cell that is only busy running a tasklet. `flz_core` held the producing OS thread
instead until `nonblocking-drop-nothing-backpressure` gave it the same shape (a push holdback on
the producing Cell). So this SDK needed no mechanism change — only the reporting half, because
heartbeat keys are a cross-SDK contract: `pending_pushes` / `pending_push_oldest_ms` count the
Cells holding an unqueued push and the age of the oldest.
In native mode the Cell bound is a dispatch-concurrency bound, not a memory bound;
a frozen `dispatches` with `blocked_parks` climbing is backpressure, and frozen with it flat is
a stall (`data_heartbeat` every ~10 s per data stream, `control_ping` every 15 s, echoed by the
controller).

A shedding policy on a node of a class on the effectively-once tier is refused at registration
(`topology.dropping_policy_under_effectively_once`, naming the channel and the policy): a durable-input-log
class that shed at its mailbox would hold the min-watermark on the shed offset for ever while
the log behind it kept growing.

A failing `run/3` (error tuple, raise, or exit) retries up to `retry_max` (default 3 total)
with exponential backoff and ±50 % jitter from `retry_backoff_ms` (default 50 ms, cap 5 s);
`{:error, {:non_retryable, _}}` parks immediately. On exhaustion the SDK ships a
byte-compatible `park_payload` and holds the Cell until the controller's `ParkPayloadAck`
(sent after the DLQ store fsync). Identical in both modes. A steps stage's Cells climb the
same ladder, read off the edge that feeds them; an exhausted one fails the tasklet, and the
instance unwinds, instead of parking.

## Partitioning

With `partition_count >= 2` on `AddOperator`, a dynamic key is admitted against the owned
range **before** any Cell state exists (a miss parks with the contract error naming the key
and its group); static keys outside the range idle silently; a keyless topology refuses the
bind (`PartitionsWithoutKeyedTasklet`). Keyless means, as in the other SDKs, that no tasklet an
entry channel feeds is keyed: a graph whose only keyed tasklet sits behind an inter-task edge is
refused a partitioned start. The hash is `Flezha.SDK.KeyGroup.group_of/1`, the
one the controller's range assignment calls ([`partitioned_operators.md`](partitioned_operators.md)).

## Dynamic cells

A dynamic node mints one Cell per key up to its `max_cells/0`; at the cap a new key's
input is dropped and counted (`[:flezha, :sdk, :dynamic]` telemetry, `event: :capped`), an
existing key's is served, and a restore that would materialise a key past the cap is refused.
With `idle_ttl_seconds/0` declared, a positive whole number of seconds, the
Operator runs one `:reap` tick at `FLEZHA_REAP_INTERVAL_MS`, else
`min(ttl / 4, 30 s)` and at least 1 ms. The sweep runs in
the Operator's own process — while it runs no push is routed, which is the drain gate — and
asks each Cell whether it has been idle past the TTL holding no work (a bounded probe; a
busy one is skipped until the next sweep), then evicts an idle one synchronously: the Cell
ships `delete_cell_state` on its own record stream when the class persists (behind its
records by construction), clears its slot so a successor is a cold start from `init_state/1`,
and stops. When no stream will take that delete, it and any records the Cell was still
holding pass to the Operator before the process ends, which replays them on the next bound
stream — ahead of anything a replacement Cell for the same key writes there. At most 256 evictions happen per sweep; the rest wait for the next tick. Evicted
keys are remembered (1 024 at most), so a live read of one answers `cell_evicted` rather
than `cell_not_found`. Without a TTL nothing is swept and the Cells live for the
operator's lifetime.

## Timers and clocks

The clock `Ctx.now/1` serves, read once per invocation, follows from what the tasklet
writes: event time when it defines `event_time/1` (what that answers for the input), the
logical clock when it declares `logical_clock: true` (the Cell's durable change seq —
`terminal + 1` after a restore — refused as `topology.unservable_clock` on a tasklet with no
`defchange`), and otherwise the wall clock, the epoch millisecond at dispatch entry. Both
at once is a `CompileError` naming `event_time/1` and `logical_clock`. Inside
`on_timer/4`, `now/1` is the
scheduled timestamp under every clock, so a chained `timer_after/2` schedules off the timer
that woke it and a replay registers the same instant.

Timers work in a steps operator's tasklets as in a flow operator's, and a firing runs as a pass of its own
([`cell_timers.md`](cell_timers.md#timers-in-a-steps-operator)). Only a `compensate` hook runs with
timers unavailable: there each of the four ctx timer calls answers
`runtime.timers_unavailable` and arms nothing.

A Tasklet registers a deadline from `run/3` or from a firing and handles it in
`on_timer(fired_at, kind, state, ctx)`, which answers what `run/3` answers. Pending timers
are one set per kind, bounded by `max_timers_per_cell/0`: a duplicate registration is a
no-op, a cancel of nothing answers `false`. An invocation's registrations and cancellations
accumulate on a session over a copy of the set — opened on the first timer call, so an
invocation that registers nothing pays nothing — and commit only when it completes; a
firing's session opens on its own removal, committed even when the firing parks, so a poison
timer parks once. A firing on a Tasklet that exports no `on_timer/4` parks non-retryably
naming the tasklet and the callback.

**Which kind fires when.** A **processing-time** timer (`timer_after/2`) fires when the
wall clock reaches it: the Cell process arms one `Process.send_after/3` per pending
timer — the BEAM's own timer service is the wake list no thread of this SDK owns — and the
due message folds every timer due by then onto the Cell's own input queue, ascending, so
the drain that serialises inputs serialises firings, whatever order two same-millisecond
messages arrive in; a due message that finds no room comes back 5 ms later, never drops; a
firing whose timer has left the pending set fires nothing. An **event-time** timer
(`timer_at_event_time/2`, event-time clock only) fires at admission: when an input's
`event_time/1` advances the Cell's high-water past it, every due timer is queued ahead
of that input, ascending — a Cell nothing arrives at never fires one. The order on the
wire is the same for both: the dispatch's change records, then its timer ops, then — once
the timers are armed and counted — its outputs.

**Durability.** The committed ops ship after the dispatch's change records as one
`format_id = 3` record (the postcard `Vec<TimerOp>` every SDK writes, stored by the
controller as any change record), so the restore fold rebuilds the pending set from the
same log the state comes back through and re-arms it, overdue timers firing at once,
ascending; a compaction re-states the whole set above the snapshot's seq; the slot a
replacement seeds from carries it. A Tasklet without `apply_change/2` holds its timers in
memory only — the shared core's host path buffers timer ops only for a slot with a flusher
entry, and this SDK ships nothing for a `NoChange` tasklet. Eviction discards a Cell's
timers with its state. A Single or Static Cell that crashes outside the ladder is minted
again at once, as it was at start, so the timers its slot holds are re-armed before any
input; it cannot loop, because everything a Tasklet raises rides the ladder and parks —
only a crash outside it re-mints, and a fresh Cell dispatches nothing until an input or a
due timer reaches it. That holds while the instance has no unfinished input. A Cell that
dies outside the ladder while the instance holds an input with an offset (one it took or
queued and has not finished) ends its Operator instead: the runner reports the instance,
and the controller rebinds it, restores its Cells and re-forwards the input log above the
confirmed floor, so those inputs run again rather than behind a replacement's floor.

**Observability.** Registrations, firings, cancellations and refusals are one
`[:flezha, :sdk, :timers]` telemetry event carrying `event:` and `tasklet:` — this SDK's
per-Tasklet counters. The heartbeat carries `timers_armed` (the processing-time timers
armed on the BEAM's timer service, released on firing, disarm and the Cell's end; a
Cell killed untrappably releases nothing, so the gauge over-states by its count until the
runner restarts) and `timers_fired`.

## Persistence

A tasklet that exports `apply_change(state, change)` is changeful and persisted; one
without it is the reference's `NoChange` — its state is a plain term nothing ships. In the
`use` form a change is written once with `defchange`, which generates both halves: the
`apply_change/2` clause that folds it and a recorder that records it through
`Flezha.SDK.Ctx.apply/3` and answers the new state, which `run/3` returns — one fold, one
push, so a replay of the recorded changes is the state.

```elixir
defmodule Tally do
  @moduledoc false
  use Flezha.SDK.Tasklet, name: "counter"

  @impl true
  def init_state(_key), do: 0

  defchange add(count, n) do
    count + n
  end

  @impl true
  def run(_input, count, ctx), do: {:ok, add(count, ctx, 1)}
end
```

Each input records the change `{:add, 1}`, and a restore folds the log back to the count.

### The `state` block, and `defchange`

`use Flezha.SDK.Tasklet` makes a `state` block available, which declares the
fields a map state carries and their defaults — §3's derivation:

```elixir
defmodule Position do
  @moduledoc false
  use Flezha.SDK.Tasklet, name: "position_state"

  state do
    field(:qty, :integer, default: 0)
    field(:fill_ids, :list, default: [])
  end

  defchange record_fill(state, qty, id) do
    %{state | qty: state.qty + qty, fill_ids: state.fill_ids ++ [id]}
  end

  @impl true
  def run({:fill, qty, id}, state, ctx), do: {:ok, record_fill(state, ctx, qty, id)}
end
```

A `field` takes a name, a type (`:integer`, `:float`, `:string`, `:boolean`,
`:list`, `:map`, `:term`) and a `default:`. The block generates `init_state/1`
returning the map of defaults (overridable, for a state that depends on the
cell key) and `state_from_json/1` reading each field by its string key. It
generates no change: a block with no `defchange` is a changeless tasklet.

`defchange name(state, args…)` is the one way to change state, with or without
a block. It generates the `apply_change(state, {:name, args…})` clause and the
recorder `name(state, ctx, args…)`, so a mutation spanning several fields is
**one** journal record rather than several: `record_fill(state, ctx, 3, "f1")`
records `{:record_fill, 3, "f1"}`. An argument the fold ignores is written
`_note`, as in any function.

A module that `use`s `Flezha.SDK.Tasklet` or an operator macro and writes
`apply_change/2` by hand fails to compile, naming `apply_change/2` and
`defchange`, and so does a block beside a hand-written `state_from_json/1`.

Renaming or retyping a field is a non-additive state edit and takes a
schema-version bump, exactly as [§3.0 of the authoring
contract](authoring_contract.md) says for every SDK — the change terms are
persisted, and a restore folds what was written.

§3.1's keyed-map and deque modes are deferred for Elixir; the block carries the
field and `:list` modes only.

After a successful dispatch the Cell ships one `cell_state_change` frame per recorded
change — the shared 16-byte header (`sdk_lang` 3, the SDK's version, the schema version, the
Erlang External Term Format id `7` allocated in `flz_core::header`), the record framed
through `Flezha.SDK.Wire.encode_frame/4` under the instance-keyed identity, at
`seq = change_seq` from `0`, in dispatch order on one secondary stream, before the dispatch's
own output frame, each under the credit window. The body is `:erlang.term_to_binary/2`,
deterministic. With the class's `durable: false` the seq advances and nothing
ships. A changeful Cell also writes `{state, seq}` into its Operator's slot table after
its records are on the wire, so a Cell killed untrappably comes back on the same seq line
rather than at seq 0, where the controller's writer would drop every later record as a
duplicate.

Every frame this SDK writes to a stream goes through one seam, `Flezha.SDK.Hold.write/3`, and
the frame's payload id declares its **guarantee class** — the writer names the frame it is
writing and never what happens when the wire refuses it. A `:durable` frame (a change record,
an effect record, a compaction snapshot, an eviction's delete, a saga marker, a park payload,
the shutdown state ship) is held and replayed onto the stream the next rebind wires, in the
order it was written, so a `change_seq` is never inverted. An `:output` is held already framed,
so a keyed emit's channel and its `flz.*` stamps survive the replay. A `:control` frame is
refused to its caller and never held, because the reconnect re-issues the request it belonged
to. The `data_heartbeat` is `:probe`: it is owed no delivery, and its misses feed the same
reconnect ladder the shared core runs — `FLEZHA_BROKEN_PIPE_RECONNECT_THRESHOLD` misses inside
`FLEZHA_BROKEN_PIPE_WINDOW_MS`, re-armed after `FLEZHA_RECONNECT_REQUEST_RETRY_MS` — with a
heartbeat counted delivered when any bound secondary took it. A payload id the table does not
name is refused at the call rather than given a default.

The hold is bounded by `FLEZHA_RECONNECT_RING_CAPACITY` (default 1024), drops the oldest past
that and counts the shed as `drops_oldest`. There is one hold per wire a process writes to: the
Cell's records, the Operator's plugin-bound outputs, the frames a Cell hands on when it
stops, and the runner's own two — the secondaries the steps driver's writes and parks go on,
and the control channel. The holds are ordered against each other: on a rebind the Operator
replays what stopped Cells handed it, then rebinds every live Cell and waits, then replays
its own outputs — so a dispatch's records reach the wire ahead of its output frame on a replay
exactly as they do live, and a dead Cell's delete ahead of both.

The controller's commands are answered by `Flezha.SDK.Commands`, off the runner's loop:
`RestoreInitState` folds the snapshot then the change log in the controller's on-disk framing
(`Flezha.SDK.Record.fold/6` — CRC over header and body, language before format before schema,
the shared core's refusal wordings), re-arms `change_seq` to `max(folded, terminal_seq) + 1`
and replies `terminal_seq`; the walk has one arm per format, so an effect record
(`format_id = 2`) is unwrapped in place — its enclosed change bodies fold in seq order under
the enclosing record's version decision, its timer ops join the pending set, its consumed
offset becomes the Cell's floor and its event time the high-water — and a snapshot header
carrying format 2 is malformed; `CompactCell` ships `cell_state_snapshot` at `change_seq - 1`
behind the Cell's records and replies the seq and size; `CellState` answers the
Decision-10 JSON (`%{tasklet => state}` for a Single node, `%{tasklet => %{key => state}}`
for a Static or Dynamic one, narrowed by the selector) with `flz.change_seq` on the reply
frame for one Cell and the refusals `cell_key_without_tasklet`, `tasklet_not_found`,
`cell_not_found`, `cell_evicted`, `cell_busy` by name; `CellApplyState`, `MigrationDeserialization` and
a non-empty `AddOperator.init_values` hand each tasklet's JSON to its `state_from_json/1`
(the object itself by default — declare it when the state's keys are atoms);
`CreditWindowInit` and `CreditGrant` drive `Flezha.SDK.Credit`, one window per runner that
parks the shipping Cell — never drops — and counts `credit_parked` on the heartbeat. A
class whose smallest `compact_after_bytes/0` differs from the default declares it. Two nodes
of one changeful module in a class would share a WAL identity and are refused at registration
(`DuplicatePersistentTasklet`); the reference gives a named node its own identity, but here
`named/2` also names the channel and the tasklet name is the WAL segment.

### Schema evolution

`schema_version/0` (1) is stamped on every record and resolved per record on restore against
`min_readable_schema_version/0` (the version itself unless declared; a floor above the
version reads the version only): equal decodes, between the floor and the version migrates,
above the version or below the floor refuses by name with the shared core's wordings. A
migrating record is decoded and handed to `migrate_state/2` (a snapshot) or
`migrate_change/2` (a change) with the stamped version and the DECODED term — the body codec
is `:erlang.term_to_binary/2`, so there is no second codec for an author to choose —
answering `{:ok, term}` at the running version or `{:error, reason}`. Both default to
refusing (`no migration from schema_version N: the tasklet declares none`), and a failing
one refuses naming its reason and the version, reseating nothing. A restore that migrated
answers `migrated`, and the controller compacts behind it so the migration is paid once.

A graceful stop — SIGTERM (the BEAM's own `init:stop`, in a runner-only boot), `close/1`, a
terminal `RunnerDisconnect` — runs `terminate/2`: every live operator is serialised, one
`RunnerShutdownNotification` ships per operator, and the acks are awaited at most 2 s in
parallel before the connection closes. `child_spec/1` sets a 10 s shutdown budget for it. A
non-terminal `RunnerDisconnect` stops the runner in place — the reference's `Stopped`: the
ack goes out, this side closes its streams, nothing reconnects and the process stays, so a
`:permanent` supervisor has nothing to restart into a second registration.

## Effectively-once tier

`run: {:flow, exactly: :effectively_once}` puts a class on the tier. Its `AddOperator`
reply declares `exactly: "effectively_once"` (and `ack: "checkpoint"` when its `ack` is
`:checkpoint`), which is what makes the controller stamp `flz.offset` on every forwarded
input, re-forward from `watermark + 1` on every bind, and derive the output identity at
flush. The tier sits under `run: :flow` only: a steps operator commits stage by stage, and a
durable one writes its effect records without declaring a tier, so `exactly` under `:steps`
is a misplaced setting.

**One record per effectful invocation.** In place of the change records and the trailing
timer-ops record, the Cell ships one `cell_effect_record` (`format_id = 2`, the postcard
body `Flezha.SDK.Wire.encode_effect_body/1` writes byte-exactly) carrying the invocation's
changes, its captured plugin outputs, the consumed offset, the event time, the timer ops and,
on a park, the dead-letter entry. State, outputs and the offset cannot tear because they are
one record at one seq.

**The offset and the floor.** The Operator reads `flz.offset` off the frame metadata for a
tier class only and enqueues `%Cell.Input{value, offset}`; an emit's origin carries the
offset of the input that produced it, so an inter-tasklet push inherits it. An input at or
below the floor a restore rebuilt is a completed dispatch: the counter advances and no record
ships.

**The interval and its four closes.** `ack: :checkpoint`, or `{:checkpoint, ...}` with any
of `every`, `max_delay_ms` and `outputs`, turns checkpoint granularity on. `Flezha.SDK.Cell.Interval`
then stages the changes, the timer ops and the last offset across invocations, and one record
closes the whole interval — on `Ctx.checkpoint/1`, on a park, on `every` inputs, or on
`max_delay_ms` after the interval opened. The delay is one `Process.send_after/3` whose
message enqueues a close on the Cell's own queue, so it lands behind the inputs that
preceded it and runs no author code; a stale one finds nothing open.

**Held outputs.** Under `outputs: :at_checkpoint`, which needs a `max_delay_ms`, the interval's outputs are held with its
changes and ride the closing record in production order under one seq. Under the default
they flush as they happen, so a sink sees an interval's outputs long before its offsets are
acknowledged.

**The park holds nothing.** A poison input parks inside the record as its `dlq` entry — with
the error, the attempts, the payload bytes and the offset — and closes the interval. There is
no `park_payload` frame and no wait for `ParkPayloadAck`: the queued input dispatches
immediately. A parked firing's removal rides the same record's `timers`.

**The edge hold.** An invocation whose outputs include an inter-tasklet push holds its
record in a FIFO until the router reports every push admitted, so the offset is never durable
ahead of the output. The Cell never gates its own queue on that report: on a single-process
router a gated Cell and a router blocked on that Cell's full queue would wait on each
other.

**`Ctx.stable_id/1`** answers the frozen 32-hex FNV-1a-128 over the Cell's wire identity,
the seq the invocation started at and a per-invocation ordinal — the same value every other
SDK derives for the same three facts, so an effect re-driven after a crash carries the id it
carried before.

**The restore and the slot.** A restore discards the open interval, cancels its deadline and
sets the floor and the event-time high-water from the log. The Operator's slot table carries
the staged interval and the floor beside `{state, seq}`, so a Cell killed untrappably comes
back holding what it had staged; records already owed to an edge report die with the process,
and the seq gap that leaves is what makes the controller resync the Cell and re-forward
those offsets.

A class with `durable: false` is refused the tier (`topology.effectively_once_without_persistence`):
the tier is durable state.

## Runner-only boot (standalone)

A standalone node depends on the `flezha_sdk` package, which starts no controller, and runs
SDK processes via `Flezha.SDK.child_spec/1`. `test/apps/elixir_runner` is a working example
(path dependency, stdin-EOF watcher). An application that depends on `flezha` itself and wants
no controller tree sets `config :flezha, controller_enabled: false` (env
`FLEZHA_CONTROLLER_ENABLED=false|0|no|off`; default true): `Flezha.Application.start/2` then
starts an empty supervision tree — no transport servers, HubManager, convergence, state-path
migration or boot-ready gate.

## Late-input correction

`correction_ms` under `:effectively_once` declares how far behind a Cell's event-time
high-water a late input may land and still be corrected; `beyond_window` declares what happens to
one that lands further back — `:process_as_arrival`, the default and the behaviour of a class
with no window at all, or `:drop`. Both sit under the tier (only the tier journals inputs
before forwarding them, so only the tier has a durable input to replay), and the window needs
at least one Tasklet that writes `event_time/1` (there is nothing else to order a replay by).
The class declares `correction_ms` and `beyond_window`, which is what makes the controller
derive the input-log hold window and the snapshot age floor from the declaration rather than
from a user setting.

**There is no splice.** An input is journaled before it is forwarded, so a late one is already
in the log at its arrival offset; the correction is the tier's ordinary restore-and-replay with
a rewound baseline and an event-time-ordered slice. The SDK's half is four things:

- **Classify, before advancing.** `Flezha.SDK.Cell.Clock` tracks the high-water and
  `Flezha.SDK.Cell.Tier.classify/3` places every input against it. At or ahead of it — or
  with no high-water yet — the input dispatches and the high-water rises.
- **Ask, and withhold.** Behind the high-water but inside the window, the input is *not*
  dispatched: one `correction_request` frame goes out on the Cell's own state stream carrying
  the target event time alone, and the replayed slice brings the input back in place. With no
  state stream there is nobody to ask, so it falls back to processing as an arrival.
- **Never drag the window backwards.** A beyond-window arrival is dispatched under the default
  policy but does **not** advance the high-water — otherwise one hour-old straggler would make
  every input after it look late.
- **Hold the slice, then order it.** Inputs the controller marks `flz.replay` are buffered as
  the Cell pops them; the payload-less `flz.replay_end` broadcast reaches every Cell of the
  operator and takes its place on each one's queue behind the members it closes. Popping it
  dispatches the slice by ascending event time, ties broken by ascending input-log offset,
  ahead of anything enqueued behind the broadcast. A restore discards a buffered slice, because
  the controller re-forwards it from the rewound watermark.

The fixture is `candle` in `test/apps/elixir_runner`, the same id and the same knobs every
other SDK registers, and the scenarios are `test/system/late_input_correction_e2e_test.exs`.

## Scope

The manifest at [`test/apps/elixir_runner/conformance.json`](../../test/apps/elixir_runner/conformance.json)
defers nothing: every capability is carried with the reference's fixtures. A call into a
builder function this SDK does not carry fails registration naming the operator module.

## The package

The SDK is the `flezha_sdk` mix package, developed in a repository of its own and carried
here as a submodule at `sdk/elixir`. It is not published to Hex yet. It depends on no controller module. The controller
depends on it and imports the pure modules both need — the record frame, the postcard codec,
the key-group hash, the bounded call, the counter-table macro and the in-node stream — so
each exists once. The compiler holds that direction: the package compiles with
`--warnings-as-errors`, where a reference to a controller module is an undefined-module
warning.
## Calls into the runner's own processes

A runner lives inside your application, so a wedged Cell or Operator must not take one of
your processes with it. Every call the SDK makes into one of its own processes goes through
`Flezha.SDK.Call`, waits a bound the configuration table names, and answers a target that
does not reply with a refusal instead of exiting the caller: `{:error, :operator_busy}` from
`Flezha.SDK.Operator.Server.lookup/4` and `cells/1`, `{:error, :runner_busy}` from the
`Flezha.SDK.RunnerServer` mutations, `{:error, "cell_busy: …"}` from the Cell commands,
`nil` where the answer is a value the caller already reads as absent.

A refusal is not a cancellation: the request is already in the target's mailbox and the work
still runs. So the bound follows what the call does. A call that answers from the queue in
front of it takes a flat bound — `sdk_control_call_ms` for the RunnerServer, an Operator and a
local run, `sdk_cell_call_ms` for a Cell. A call whose duration grows with your state takes
one derived from the controller bound that awaits it: `sdk_cell_restore_ms` inside
`restore_await_timeout_ms`, `sdk_cell_snapshot_ms` inside `compaction_timeout_ms`,
`sdk_cell_apply_ms` inside `control_unit_reply_timeout_ms`. The SDK never reports a failure
while the controller is still waiting for the answer.

Three calls wait with no bound, because there the wait is the protocol and the caller is one
of the runner's own processes, never one of yours: `Flezha.SDK.Cell.enqueue_block/2` under
`:drop_nothing`, whose guarantee is that an input is never dropped and never refused;
`Flezha.SDK.Credit.acquire/2`, which parks a sender until a grant covers its frame; and
`Flezha.SDK.Cell.rebind/2`, whose caller replays that Cell's outputs after it returns and
so must not proceed before its records have reached the wire.

## See also

- [`authoring_contract.md`](authoring_contract.md) — the cross-SDK authoring contract
- [`conformance.md`](conformance.md) — the shared conformance suite and this SDK's manifest
- [`protocol_spec.md`](protocol_spec.md) — the binary protocol a standalone runner speaks
- [`../internals/nif_transport.md`](../internals/nif_transport.md) — the NIF client behind the
  socket modes
