# Elixir SDK

For Elixir developers authoring relays and for SDK implementers. The Elixir runner SDK
gives an Elixir application the Relay/Tasklet/Cell model of the other SDKs, and runs on the
same runtime they do: `flz_core`, bound through a native library the package carries
([`../../public/architecture/runner.md`](../../public/architecture/runner.md#one-change-every-sdk)). A runner
reaches its controller one way, over UDS or QUIC, wherever that controller runs: in the same
BEAM, on the same machine or on another.

**Support tier:** [tier 1](../../public/publishing/maturity.md#sdk-elixir), run by the maintainer in
a live deployment, passes the full conformance run in CI and is permanently supported.

Installing, the supported Elixir versions and how the native library reaches the host are in
the package's [`README.md`](../../../sdk/elixir/README.md); local mode in
[`running_and_testing.md`](../../public/topics/core/running_and_testing.md#local-mode).

## Where a runner runs

A runner is a child of your application's supervision tree, and the controller sees it as
it sees a runner in any language, apart from its language byte. The same module runs in
three places:

| | Beside the controller, in its BEAM | Its own BEAM on the same machine | Another machine |
|---|---|---|---|
| Transport | UDS | UDS | QUIC |
| Isolation | none: a crash in the native library ends the BEAM, controller included | full OS-process isolation | full |
| Use when | trusted relays that share the controller's release | relays you want isolated from the controller | relays near their data, or across a fleet |

`transport:` takes `:uds` (the default) or `:quic`; anything else is refused as
`runtime.invalid_transport`. The connection settings are read through `Flezha.SDK.Knob`: the
OS environment first, then the application env (`config :flezha_sdk, socket_files_path:
"/run/flezha"`), and the SDK hands them to the core when the runner starts. The BEAM keeps its
own copy of the environment, so a value set with `System.put_env/2` does reach the SDK, and
reaches the core through it. A runner in the controller's BEAM finds the controller's socket
when both resolve the same directory: set `FLEZHA_SOCKET_FILES_PATH` (or `socket_files_path`)
for both, or let both walk the default ladder.

A controller that enforces authorization asks every runner for a credential, the one in its
own BEAM included. Issue it with `flz token issue runner@<ns> <ns> runner` and pass it as
`namespace_secret:`, or set `FLEZHA_NAMESPACE_SECRET`.

## Registration

Runner-as-handle; errors are `{:error, %Flezha.SDK.Error{}}` returned at the call site (never
a raise) and propagate through a pipe. The error's `code` is one of
[the error codes](../../public/errors.md); this page names each refusal by its code.

```elixir
Flezha.SDK.create_runner("my_namespace", namespace_secret: secret)
|> Flezha.SDK.register_factory(MyRelay)
|> Flezha.SDK.run()   # start/1 then await_termination/1

# or under the host supervisor, the same sequence:
children = [{Flezha.SDK, namespace: "my_namespace", relays: [MyRelay]}]
```

- `create_runner/2` returns a handle — no connection, no processes, no slot claim. It takes
  `transport:` and `namespace_secret:`.
- `register_factory/2` takes a module of the `Flezha.SDK.Relay` behaviour, which the `use`
  macro sets; any other module answers
  `{:error, %Flezha.SDK.Error{code: "topology.no_relay_exported"}}`. The class's declaration
  is judged by `flz_core` there and then, the check every SDK's builder meets, so a refusal
  carries the core's `topology.*` code with every issue in `details.issues`.
- `register_factory/2` / `unregister_factory/2` work before and after `start/1`; after it,
  the core sends the controller the full factory set, and refuses to remove a type with live
  instances on this runner with `runtime.relay_type_in_use`, naming the factory id and the
  live count.
- `start/1` claims the process-wide runner slot (loser: `runtime.runner_already_running`)
  and connects; a refused connect is answered at the call with the controller's code, such as
  `runner.namespace_not_proven`, and leaves nothing behind, so a supervisor may start the
  runner again; so does a connect no controller confirmed within the start wait
  (`runtime.connect_unconfirmed`), `FLEZHA_RUNNER_START_WAIT_MS` or
  `config :flezha_sdk, runner_start_wait_ms: ...`, which the SDK hands to the core. `run/1` = `start/1` + `await_termination/1`.
- A runner runs **once per BEAM**, because `flz_core` is process-wide. `close/1` ships each
  relay's state and ends the session; a later start in the node, a local run's included, is
  refused as `runtime.core_shut_down`. So is the restart after a runner crash: the failure reaches the
  node, which is the unit to restart, and the controller moves the relays meanwhile. One
  runner or one local run is live in a BEAM at a time; a second is refused as
  `runtime.core_in_use`.

## Authoring

Two `use` macros write the behaviours below. Each takes the scalar settings
as options named after the callback they define, and **defines only the options
written**, so every default stays exactly where it was — including the ones an
environment knob decides.

```elixir
defmodule MyRelay do
  use Flezha.SDK.Relay, factory_id: "my_op"

  @impl true
  def channels(c), do: c |> input("in") |> into(MyTask) |> output("out")
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

The relay macro imports `Flezha.SDK.Channels`, so a whole pipe — a merge
included — is written with no module prefix. `use Flezha.SDK.Tasklet` imports
`Flezha.SDK.Ctx`'s authoring functions (every one but `apply/3`, which would
shadow `Kernel.apply/3`) and leaves `init_state/1` answering `nil` unless the
module writes one, so a stateless tasklet writes only `run/3`. A tasklet ends
its instance with `complete(ctx, result)`: in a class that runs in steps once its
iteration commits, in a flow class once the instance's Cells hold nothing.

An unknown option is a `CompileError` naming it, and so is a missing required
one (`name` for a tasklet, `factory_id` for a relay). `idle_ttl_seconds`
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
  use Flezha.SDK.Relay,
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
  Cells restored. `:remote` acknowledges each commit point (a stage, an input, a checkpoint)
  only once the durability backend holds it.
- `max_duration_ms` is a wall-clock stop for either run, counted from the run's first bind:
  a steps instance unwinds and ends `cancelled`, a flow instance ends `cancelled` once its
  Cells hold nothing.

The SDK forwards the tree as written and the core judges it at `register_factory/2`: a
setting written under a parent that does not carry it (`each` under `:flow`, `ack` under
`:steps`, `sleep_after` under `false`) is refused as `topology.unknown_field` naming the
setting and the parent, and a value outside its choices as `topology.invalid_setting`,
whether `use` or a hand-written `run/0` or `durable/0` answers the tree. The one combination refused is
`:effectively_once` with `durable: false`, `topology.effectively_once_without_persistence`:
the tier is durable state.

> A tasklet nested inside a relay module inherits that module's imports
> lexically, so `use Flezha.SDK.Tasklet` takes the builder's back — a nested
> tasklet's own `input/0` would otherwise clash with `Channels.input/2`. A
> nested tasklet must be written **before** the `channels/1` that names it,
> which is how Elixir aliases resolve.

### The short form

A relay module that writes its one tasklet's `run/3` instead of
`channels/1` is a whole single-tasklet relay, registered as any relay is.

```elixir
defmodule Upcase do
  use Flezha.SDK.Relay, factory_id: "upcase", output: "out"

  def run(input, state, ctx) do
    emit(ctx, String.upcase(input))
    {:ok, state}
  end
end

defmodule Place do
  use Flezha.SDK.Relay, factory_id: "place", output: "placed", input: "order", run: :steps

  def run(input, state, ctx) do
    emit(ctx, input)
    {:ok, state}
  end
end

children = [{Flezha.SDK, namespace: "default", relays: [Upcase, Place]}]
```

The module generates the nested tasklet module `Upcase.Tasklet`, whose
`name/0` is the factory id verbatim and which delegates to the tasklet
callbacks the module defines, and a `channels/1` wiring an entry channel
named `"input"` (or the `input` option's value) into it and, with `output`, it
to that plugin output. The class settings it takes are the full form's, and every
one it does not write keeps its default. Because the tasklet is named after the
factory id, its cells keep their state path when the module grows into the full
form. `Place` runs in steps with `each: :once`, run once over its first message:
`flz relay start --text '<payload>'`, or a start followed by
`flz relay send`; a second message to a `once` instance is accepted and
never read.

```elixir
defmodule Chat do
  use Flezha.SDK.Relay, factory_id: "chat", output: "replies", run: {:steps, each: :per_input}

  def run(turn, state, ctx) do
    emit(ctx, turn)
    {:ok, state}
  end
end
```

`Chat` runs its tasklet once per message and does not complete after the first.

`run/3` is written as in a tasklet: the ctx functions are imported, a `state`
block and `defchange` work, and the options `use Flezha.SDK.Tasklet` takes
(`max_cells:`, `logical_clock:`, ...) apply to the generated tasklet. `output` is
optional; with none, nothing leaves the tasklet but its state:

```elixir
defmodule Counter do
  use Flezha.SDK.Relay, factory_id: "counter", input: "ticks", max_cells: 10

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
`codec:` names the entry channel's codec as `input/3`'s does, JSON when
absent.

The refusals are `CompileError`s. `run/3` beside `channels/1` begins
`topology.short_form_with_topology`, and writing neither names both. A
short-form option on a full form names the option, and `use Flezha.SDK.Tasklet`
refuses `output` and `input`, naming the relay macro. `event_time/1` beside
`logical_clock: true` names both and the module the author wrote. This SDK cannot see a tasklet's
output type, so it owes no `topology.output_without_type` and its manifest does not list
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

defmodule HandWrittenRelay do
  @moduledoc false
  @behaviour Flezha.SDK.Relay

  @impl true
  def factory_id, do: "hand_written_relay"

  @impl true
  def channels(c) do
    c |> Channels.input("in", codec: :text) |> Channels.into(HandWritten) |> Channels.output("out")
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
keyset, which needs `cells_for/1` naming the keys an input routes to, or the class is
refused as `topology.cells_for_missing`), `mailbox_capacity/0` (1024; env `FLEZHA_MAILBOX_CAPACITY`, range
`1..1_048_576`), `max_cells/0` (100_000 — the cap on a dynamic node's live Cells: at it
a new key's input is dropped and counted, an existing key's is served),
`idle_ttl_seconds/0` (nil — a dynamic Cell idle past that many whole seconds is evicted, and its key starts again from `init_state/1`), the persistence
three — `apply_change/2` (declares a changeful, persisted tasklet; see
[Persistence](#persistence)), `compact_after_bytes/0` (8 MiB) and `state_from_json/1` — and
the schema four — `schema_version/0` (1), `min_readable_schema_version/0` (the version),
`migrate_state/2`, `migrate_change/2` (see [Schema evolution](#schema-evolution)) — and the
timer four — `logical_clock/0` (`false`), `event_time/1`, `on_timer/4`, `max_timers_per_cell/0`
(10 000; see [Timers and clocks](#timers-and-clocks)).
`Relay` takes the optional `description/0`, `max_duration_ms/0`, `run/0` and `durable/0`
([The settings tree](#the-settings-tree)); a class that runs in steps writes the same
`channels/1` and has no retry setting of its own, because a stage's ladder is the edge's
`retry_max` / `retry_backoff_ms`, as a flow relay's is.
`Flezha.SDK.Ctx`: `emit/2`, `emit_with_meta/3` (a reserved `flz.` key answers
`runtime.reserved_metadata_key`), `emit_keyed/3` and `emit_keyed_with_meta/4` (the
output's natural key on an output declared `keyed/1`; on a node with no keyed output they answer
`runtime.key_not_allowed_on_non_keyed_channel`, and a keyless emit from a
Single Cell on a keyed output answers `runtime.key_required_on_keyed_channel`),
`part/2` (a preview of the invocation's output, sent at once rather than when `run/3`
returns: encoded as an emit of the value would be, numbered `flz.part` from `0` within the
attempt and stamped with `flz.part_attempt`, `flz.cell` and `flz.key`; it goes to the
relay's listeners and to nothing else, never waits, is dropped and counted when the wire
cannot take it at once, and is sent by no replayed input; it answers `part.no_output` when the
node's channel ends in no output, a `call/3` end included, and `part.outside_invocation`
through a ctx whose invocation returned),
`apply/3` (fold a change and record it; a `defchange` recorder calls it),
`complete/2` (end the instance with a result, encoded as `spawn_steps` encodes its
payload), `now/1` (the declared clock's value, once per invocation; the scheduled timestamp
inside `on_timer/4`), `cell_key/1`, and the timer four
— `timer_after/2` (milliseconds after `now/1`), `timer_at_event_time/2`,
`cancel_processing_time_timer/2`, `cancel_event_time_timer/2` — the registrations answering
`{:ok, at}`, the absolute timestamp a cancel takes, and a cancel `:ok`; inside a
`compensate/3` hook all four answer
`runtime.timers_unavailable` (see [Timers and clocks](#timers-and-clocks)).
`spawn_steps(ctx, factory_id, payload, opts)` requests spawn-or-deliver of a steps relay
instance, `id:` and `if_running:` optional, with `payload` sent as an output with no codec sends a
value (bytes as they are, anything else as JSON; a term with no JSON form raises), and answers
a `Flezha.SDK.Spawn` handle at once that resolves to `{:ok, %{id: id, outcome: outcome}}`; the contract is
`docs/public/topics/core/workflows.md`. A spawn without `id:` always starts a new
instance, starts a second one when reissued after a lost answer, and answers an empty id when
skipped on replay; pass `id:` when that matters.

## Graph builder (v1 subset)

`input`/`input_with` → `into`/`into_named` → `keyed` → `output`/`output_with`, with
`map`/`filter`/`flat_map`/`key_by` as edge transforms (fused into one closure per consumer
edge), plus `named`, `on_full`, `retry_max`, `retry_backoff_ms`, and `merge/1` with
`route_latest/4` ([`streams.md`](../../public/topics/core/streams.md#merged-inputs)). Fan-out is handle reuse,
or `branch/2`, which calls each arm of a list with the same stream and answers it;
unnamed mentions of one module unify (fan-in). An output names the channel it terminates:
`output("input")` makes the frame `channel = "input"`, which is what reaches a forward
target's `input` entry, and `output/1` leaves the name derived — `named/2`'s name, else the
tasklet's `name/0`. Naming one channel twice is refused. A called input's first output
answers its caller and still reaches the output's plugins
([`sending_and_calling.md`](../../public/topics/core/sending_and_calling.md#calls)). `call(handle, "pricing", deadline_ms: 500)`, or
`call(handle, factory: "agent", deadline_ms: ..., create: false)` after a `key_by/2`, with
`answer_channel:` to name the callee's answering channel, ends a
chain on a call to another relay and answers a handle on its Response channel, whose
values are `%Flezha.SDK.Response{}` ([`sending_and_calling.md`](../../public/topics/core/sending_and_calling.md#calls)). The user's metadata rides the frame. Cell mode is derived: non-empty `cell_keys/0` → static; a keyed edge into a
keyless tasklet → dynamic; else single. Refusals accumulate into one
`{:error, %Flezha.SDK.Error{}}` carrying the first issue's code and every issue in
`details.issues` — e.g. `topology.transform_after_key_by`, `topology.retry_max_zero`,
`topology.channel_named_twice` (`named/2` beside an `output` that names),
`topology.keyed_channel_without_plugin_wire` (`keyed/1` on a channel with no output),
`topology.deferred_builder_surface` (names the relay module).

The SDK builds the declaration and `flz_core` judges it, so the names are the reference
vocabulary every SDK answers with (`flz_core/src/host/issue.rs`). A few refusals are about
the Elixir surface itself and are judged before the declaration is submitted:
`topology.channel_named_twice`, `topology.deferred_builder_surface`, a `keyed/1`, `named/2` or
`output` on an entry, and a tasklet that declares two clocks. The fixture every SDK is driven
against is `test/fixtures/reference_topologies.txt`, one row per refusal
([`conformance.md`](conformance.md) § The refusal table).

The effectively-once tier is a class setting, not a builder call
([The settings tree](#the-settings-tree)). A `beyond_window` written with no `correction_ms`
is a zero window, refused, because a fate for a window nobody declared is an authoring
error. The tier's refusals are `topology.checkpoint_every_zero`,
`topology.at_checkpoint_without_max_delay`, `topology.unkeyed_channel_under_effectively_once`
(naming the output's channel), `topology.dropping_policy_under_effectively_once` (naming the node's inbound
channel and the policy), `topology.correction_without_event_time` (naming a
Tasklet and the clock it declares) and `topology.correction_window_zero`.

## Keyed channels

`keyed/1` on the handle before `output/2` declares output identity on that output — the
reference's `.keyed()`. The SDK hands each output its key: the emit's own (`emit_keyed/3`), else
the output route's `key_by`, else the emitting Cell's; a Single Cell with none is refused at the
emit. The core stamps the frame at settle with `flz.cell`
(`<relay id>/<tasklet name>/<cell key or empty>`), `flz.seq` (`<change_seq at handout>.<emit ordinal>`, the
ordinal counting the item's plugin outputs from zero, stamped or not) and `flz.key`, after the
user's metadata so a stamp cannot be clobbered. An output not declared keyed carries no `flz.`
key. Under the effectively-once tier, and in a durable steps relay, the controller derives
`flz.cell`, `flz.seq` and `flz.epoch` at flush from the effect record instead.

## The runtime underneath

Everything below the tasklet call is `flz_core`'s, the same code every other SDK runs on:
mailboxes and overflow, the retry ladder and the dead-letter queue, partitioning, dynamic
Cells and their eviction, timers, persistence and restore, the effectively-once tier and
late-input correction, credit, reconnects and the heartbeat. The topic pages describe them
once for every SDK: [`streams.md`](../../public/topics/core/streams.md),
[`partitioning.md`](../../public/topics/core/partitioning.md), [`timers.md`](../../public/topics/core/timers.md),
[`state.md`](../../public/topics/core/state.md), [`durability.md`](../../public/topics/core/durability.md) and
[`effectively_once.md`](../../public/topics/core/effectively_once.md). What the Elixir SDK adds is how
your code is called.

**Workers.** A pool of BEAM processes answers the core's activations. Each worker is paired
with a thread the native library owns: the thread waits in the core's poll and hands the
activation over as a message, the worker runs your tasklet and completes it, and only then
does the thread poll again, so no wait for an activation holds a BEAM scheduler. The core
never hands out two activations for one Cell at once, however many workers there are.

How many workers is the core's pool governor's
([`poll_core.md`](poll_core.md#the-core-decides-the-sdk-brings-the-threads)), over the
schedulers online as this runtime's base: its supervisor, a thread of the native library, asks
the pool process for each worker, one more when the core reports every worker held, and a
grown worker's thread stops it after the governor's idle time. `FLEZHA_WORKER_POOL_SIZE` and
`FLEZHA_WORKER_POOL_MAX` size it; their types and defaults are in
[Runner environment variables](../../public/configuration/runner_environment.md#every-variable).

A worker that crashes is replaced, and the activation it held is failed back to the core's
retry ladder. A tasklet that raises, throws or answers `{:error, _}` retries up to
`retry_max` with the backoff `retry_backoff_ms` declares; `{:error, {:non_retryable, _}}`
parks at once.

**Payloads and state.** Snapshots, change records and values passed between tasklets are
the Erlang External Term Format (`:erlang.term_to_binary/1`, header format id `7`), read back
with `binary_to_term(_, [:safe])`. A plugin output is bytes, written by the output's
serializer.

## Timers and clocks

The clock `Ctx.now/1` serves, read once per invocation, follows from what the tasklet
writes: event time when it defines `event_time/1` (what that answers for the input), the
logical clock when it declares `logical_clock: true` (the Cell's durable change seq —
refused as `topology.unservable_clock` on a tasklet with no `defchange`), and otherwise the
wall clock. Both at once is a `CompileError` naming `event_time/1` and `logical_clock`.
Inside `on_timer/4`, `now/1` is the scheduled timestamp under every clock, so a chained
`timer_after/2` schedules off the timer that woke it and a replay registers the same instant.

A Tasklet registers a deadline from `run/3` or from a firing and handles it in
`on_timer(fired_at, kind, state, ctx)`, which answers what `run/3` answers. Pending timers
are one set per kind, kept by the core and bounded by `max_timers_per_cell/0`: a duplicate
registration is a no-op, and so is a cancel of nothing. An invocation's registrations and
cancellations reach the core only when it completes, which judges them as the item settles:
past the cap, or an event-time timer under another clock, the item fails, not retried, with
`runtime.timer_cap_exceeded` / `runtime.event_time_timer_without_event_time_clock`. A firing on a Tasklet that exports no `on_timer/4` parks
non-retryably naming the tasklet and the callback. Only a `compensate` hook runs with
timers unavailable: there each of the four ctx timer calls answers
`runtime.timers_unavailable` and arms nothing. When each kind fires, and how pending timers
survive a restart, is the core's and the same in every SDK
([`timers.md`](../../public/topics/core/timers.md)).

## Persistence

A tasklet that exports `apply_change(state, change)` is changeful and persisted; one
without it is the reference's `NoChange` — its state is a plain term nothing ships. In the
`use` form a change is written once with `defchange`, which generates both halves: the
`apply_change/2` clause that folds it and a recorder that records it through
`Flezha.SDK.Ctx.apply/3` and answers the new state, which `run/3` returns — one fold, one
record, so a replay of the recorded changes is the state.

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

A module that `use`s `Flezha.SDK.Tasklet` or a relay macro and writes
`apply_change/2` by hand fails to compile, naming `apply_change/2` and
`defchange`, and so does a block beside a hand-written `state_from_json/1`.

Renaming or retyping a field is a non-additive state edit and takes a
schema-version bump, exactly as [§3.0 of the authoring
contract](authoring_contract.md) says for every SDK — the change terms are
persisted, and a restore folds what was written.

§3.1's keyed-map and deque modes are deferred for Elixir; the block carries the
field and `:list` modes only.

A live state read, an applied state and a migration hand each tasklet's JSON to its
`state_from_json/1` (the object itself by default — declare it when the state's keys are
atoms).

### Schema evolution

`schema_version/0` (1) is stamped on every record and resolved per record on restore against
`min_readable_schema_version/0` (the version itself unless declared): equal decodes, between
the floor and the version migrates, above the version or below the floor refuses by name with
the shared core's wordings. A migrating record is decoded and handed to `migrate_state/2` (a
snapshot) or `migrate_change/2` (a change) with the stamped version and the decoded term,
answering `{:ok, term}` at the running version or `{:error, reason}`. Both default to refusing
(`no migration from schema_version N: the tasklet declares none`), and a failing one refuses
naming its reason and the version.

`close/1`, or the supervisor stopping the runner, shuts the core down as every SDK does: each
relay's state is shipped, served by the workers, and the session ends. The workers still
running after that are stopped before the native context is freed. `child_spec/1` sets a 10 s
shutdown budget for it.

## Effectively-once tier

`run: {:flow, exactly: :effectively_once}` puts a class on the tier
([`effectively_once.md`](../../public/topics/core/effectively_once.md)). The tier sits under
`run: :flow` only: a steps relay commits stage by stage, and a durable one writes its effect
records without declaring a tier, so `exactly` under `:steps` is a misplaced setting. A class
with `durable: false` is refused the tier (`topology.effectively_once_without_persistence`):
the tier is durable state.

**`Ctx.stable_id/1`** answers the frozen 32-hex FNV-1a-128 over the Cell's identity, the seq
the invocation started at and a per-invocation ordinal — the same value every other SDK
derives for the same three facts, so an effect re-driven after a crash carries the id it
carried before.

**`Ctx.effect/4`** (`effect(ctx, name, fun)` in a `use` tasklet) runs an outside call once per
re-run of the invocation and answers `{:ok, result}`, or an error `run/3` returns as it is
([`effectively_once.md`](../../public/topics/core/effectively_once.md#recorded-effects)).

`correction_ms` under the tier declares how far behind a Cell's event-time high-water a late
input may land and still be corrected, and `beyond_window` what happens to one further back:
`:process_as_arrival` (the default) or `:drop`. The window needs at least one Tasklet that
writes `event_time/1`. The fixture is `candle` in `test/apps/elixir_runner`, the same id and
knobs every other SDK registers, and the scenarios are
`test/system/late_input_correction_e2e_test.exs`.

## Runner-only boot (standalone)

A standalone node depends on the `flezha_sdk` package, which starts no controller, and runs
the runner via `Flezha.SDK.child_spec/1`. `test/apps/elixir_runner` is a working example
(path dependency, a stdin-EOF watcher). An application that depends on `flezha`
itself and wants no controller tree sets `config :flezha, controller_enabled: false` (env
`FLEZHA_CONTROLLER_ENABLED=false|0|no|off`; default true): `Flezha.Application.start/2` then
starts an empty supervision tree.

## Scope

The manifest at [`test/apps/elixir_runner/conformance.json`](../../../test/apps/elixir_runner/conformance.json)
defers nothing: every capability is carried with the reference's fixtures. A call into a
builder function this SDK does not carry fails registration naming the relay module.

## The package

The SDK is the `flezha_sdk` mix package, developed in a repository of its own and carried
here as a submodule at `sdk/elixir`. It carries the native library `flz_core_beam`, built over
`flz_core`'s ABI (its [`README.md`](../../../sdk/elixir/README.md) says how it reaches the
host). It depends on no controller module, and the controller uses none of its runtime:
the controller depends on it only so a runner can be started in the controller's own BEAM.
The package compiles with `--warnings-as-errors`, where a reference to a controller module is
an undefined-module warning.

## Calls into the runner's own processes

Every call the SDK makes into one of its own processes goes through `Flezha.SDK.Call`, waits a
bound the configuration table names, and answers a target that does not reply with a refusal
instead of exiting the caller. `sdk_control_call_ms` bounds the calls into the runner and a
local run; `sdk_spawn_reply_ms` bounds the wait for a spawned relay's answer. A refusal is not
a cancellation: the request is already in the target's mailbox and the work still runs.

## Diagnostics

The SDK logs through `Logger`. The core's notices (a retry, a reconnect, a
refusal, the local-mode banner) reach it through one process the SDK starts on
first use and keeps for the life of the VM, so a notice outlives the run that
wrote it. The native library sends each from a thread of its own, never from
a scheduler. That process logs them under the domain `[:flezha]` from the
level `FLEZHA_LOG_LEVEL` names (`warn` when unset), and the local-mode banner
at `:error`, which no level hides.

## See also

- [`authoring_contract.md`](authoring_contract.md) — the cross-SDK authoring contract
- [`conformance.md`](conformance.md) — the shared conformance suite and this SDK's manifest
- [`protocol_spec.md`](protocol_spec.md) — the binary protocol the core speaks to a controller
- [`poll_core.md`](poll_core.md) — the activation model every host on `flz_core` follows
