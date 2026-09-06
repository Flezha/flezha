# Elixir SDK

For Elixir developers authoring operatives and for SDK implementers. The Elixir runner SDK
gives an Elixir application the Operative/Procedure/Entity model of the Rust and Java SDKs,
deployable two ways from one core: **in-node** (`:native`) inside the Controller's own BEAM
node over message passing, or **standalone** (`:uds` / `:quic`) as its own BEAM node speaking
the binary protocol, indistinguishable from a Rust runner to the Controller. Everything above
the transport seam is shared code with identical semantics in both modes. The SDK inherits
nothing from `flz_core` ([`../architecture/runner.md`](../architecture/runner.md#one-change-every-sdk)).

## Local mode: run a graph with no controller

The shortest way to see an operative work, and a third way to run one beside
the two modes below: **no controller at all**. `Flezha.SDK.Local.start/2` takes
the `Flezha.SDK.StreamOperative` module you deploy, compiles the topology its
`channels/1` already declares, and hands sink output to a function:

```elixir
{:ok, run} = Flezha.SDK.Local.start(WordCount)
:ok = Flezha.SDK.Local.on_sink(run, "counts", &IO.puts(&1.payload))
:ok = Flezha.SDK.Local.feed(run, "lines", "the quick brown fox")
:ok = Flezha.SDK.Local.run_until_idle(run, 5_000)
:ok = Flezha.SDK.Local.stop(run)
```

`on_sink/3` registers against the sink's **channel** name, which is the
producing procedure's name unless the declaration `named/2` gave it one.
`feed/3` enters through the same `Flezha.SDK.Operative.Inbox.input/4` a frame
off a bound data stream enters, so the channel's declared decode and admission
run as deployed. A worked two-procedure example is in
[`../quickstart.md`](../quickstart.md).

**State is held in memory and nothing is durable** — no write-ahead log, no
snapshots, no dead-letter queue. Every run says so on stderr and it is not
suppressible. Durability, effectively-once delivery, plugins, placement and
migration, the dead-letter queue, observability export, authz and multi-runner
scale need a controller; a declaration asking for one of those is refused at
startup, naming it. This SDK expresses local mode in its own core, as it does
everything else. Local mode also ships in the Rust and Java SDKs; Node, Python,
Ruby and PHP do not carry it yet. Walk-through:
[`../quickstart.md`](../quickstart.md).

## Choosing a mode

| | In-node (`:native`) | Standalone (`:uds` / `:quic`) |
|---|---|---|
| Where | Controller's BEAM | Own BEAM node |
| Isolation | crashes contained per process; **memory/scheduler shared** with the Controller | full OS-process isolation |
| Use when | trusted, latency-sensitive, co-located operatives | untrusted or resource-unpredictable operatives, cross-node deployment |
| Security | a function call; a connect from another BEAM node is refused (`:remote_node_rejected`) | UDS filesystem perms / QUIC mutual TLS |

`:native` is the default and is **not** env-derived: a controller sets `FLEZHA_TRANSPORT=uds`
for its *server*, and an env-following in-node SDK would dial its own node over a socket.
Standalone deployments pass `:uds` / `:quic` explicitly; connection parameters come from the
env the Rust client reads (`FLEZHA_SOCKET_FILES_PATH`, `FLEZHA_QUIC_ADDRESSES`,
`FLEZHA_CA_CERT_PATH`).

## Registration

Runner-as-handle; errors are tuples returned at the call site (never a raise) and propagate
through a pipe.

```elixir
Flezha.SDK.create_runner("my_namespace", transport: :native)
|> Flezha.SDK.define_stream(MyOperative)
|> Flezha.SDK.run()   # start/1 then await_termination/1

# or under the host supervisor — same sequence, reconnects with the same stable id:
children = [{Flezha.SDK, namespace: "my_namespace", streams: [MyOperative], transport: :native}]
```

- `create_runner/2` returns a handle — no connection, no processes, no slot claim.
- `define_stream/2` / `remove_stream/2` work before and after `start/1`; a post-start change
  ships `UpdateFactories`; removing a type with live instances refuses
  `{:error, {:operative_type_in_use, factory_id, count}}`.
- `start/1` claims the process-wide runner slot (loser: `{:error, {:already_running, ns}}`)
  and connects; `run/1` = `start/1` + `await_termination/1`; `close/1` disconnects.

## Behaviours

```elixir
defmodule MyOperative do
  @behaviour Flezha.SDK.StreamOperative     # optional: description/0
  def factory_id, do: "my_op"

  def channels(c) do
    import Flezha.SDK.Channels
    c |> input("in") |> into(MyProc) |> named("proc") |> sink("out")
  end
end

defmodule MyProc do
  @behaviour Flezha.SDK.Procedure
  def name, do: "my_proc"
  def init_state(_entity_key), do: %{}
  def run(input, state, ctx) do
    Flezha.SDK.Ctx.emit(ctx, transform(input))
    {:ok, state}
  end
end
```

`run/3` returns `{:ok, new_state} | {:error, reason} | {:error, {:non_retryable, reason}}`;
emits flush only on `{:ok, _}`. Optional `Procedure` callbacks: `entity_keys/0` (static
keyset), `entities_for/1`, `mailbox_capacity/0` (1024; env `FLEZHA_MAILBOX_CAPACITY`, range
`1..1_048_576`), `max_entities/0` (100_000 — the cap on a dynamic node's live Entities: at it
a new key's input is dropped and counted, an existing key's is served),
`entity_idle_ttl_ms/0` (nil; see [Dynamic entities](#dynamic-entities)), the persistence
three — `apply_change/2` (declares a changeful, persisted procedure; see
[Persistence](#persistence)), `compact_after_bytes/0` (8 MiB) and `state_from_json/1` — and
the schema four — `schema_version/0` (1), `min_readable_schema_version/0` (the version),
`migrate_state/2`, `migrate_change/2` (see [Schema evolution](#schema-evolution)) — and the
timer four — `clock/0` (`:wall`), `event_time_of/1`, `on_timer/4`, `max_timers_per_entity/0`
(10 000; see [Timers and clocks](#timers-and-clocks)).
`StreamOperative` takes an optional `persistence_enabled/0` (default `true`).
`Flezha.SDK.Ctx`: `emit/2`, `emit_with_meta/3` (a reserved `flz.` key answers
`{:error, {:reserved_metadata_key, k}}`), `emit_keyed/3` and `emit_keyed_with_meta/4` (the
output's natural key on a sink declared `keyed/1`; on a node with no keyed sink they answer
`{:error, {:key_not_allowed_on_non_keyed_channel, channel}}`, and a keyless emit from a
Single Entity on a keyed sink answers `{:error, {:key_required_on_keyed_channel, channel}}`),
`apply/3` (fold a change and record it), `now/1` (the declared clock's value, once per
invocation; the scheduled timestamp inside `on_timer/4`), `entity_key/1`, and the timer four
— `timer_after/2` (milliseconds after `now/1`), `timer_at_event_time/2`,
`cancel_processing_time_timer/2`, `cancel_event_time_timer/2` — the registrations answering
`{:ok, at}`, the absolute timestamp a cancel takes, or
`{:error, {:event_time_timer_without_event_time_clock, procedure, clock}}` /
`{:error, {:timer_cap_exceeded, procedure, max}}`.

Entry-input idle rides the handle: `Flezha.SDK.last_entry_delivery(handle, channel)` answers
when an entry channel last received a frame (`nil` before its first — the handle is free, so
the read is legal before `start/1`), and `Flezha.SDK.on_entry_idle(handle, channel,
threshold_ms, observer)` reports silence past the threshold as episodes to a module
implementing `Flezha.SDK.IdleObserver` (`idle/1`, optional `resumed/1`) or a fun of arity one;
`child_spec/1` takes the same registrations as `idle_observers: [{channel, threshold_ms,
observer}]`. Both layers are this SDK's own: `Flezha.SDK.EntryInput` keeps one `:atomics` cell
per entry-channel name and `Flezha.SDK.EntryIdle` — one process per runner — the episode
latch and the tick. Contract: [`entry_input_idle.md`](entry_input_idle.md).

## Graph builder (v1 subset)

`input`/`input_with` → `into`/`into_named` → `keyed` → `sink`/`sink_with`/`sealed`, with
`map`/`filter`/`flat_map`/`key_by` as edge transforms (fused into one closure per consumer
edge), plus `named`, `on_full`, `retry_max`, `retry_backoff_ms`. Fan-out is handle reuse;
unnamed mentions of one module unify (fan-in). A sink frame is named by its producing node —
`named/2`'s name, else the procedure's `name/0`, never the plugin — so `named("input") |>
sink("relay_out")` reaches a forward target's `input` entry; the user's metadata rides the
frame. Entity mode is derived: non-empty `entity_keys/0` → static; a keyed edge into a
keyless procedure → dynamic; else single. Refusals accumulate into one
`{:error, {:invalid_topology, issues}}` — e.g. `TransformAfterKeyBy`, `RetryMaxZero`,
`KeyedChannelWithoutPluginWire` (`keyed/1` on a channel with no sink),
`DeferredBuilderSurface` (names the operative module).

The names are the reference vocabulary every SDK answers with
(`flz_core/src/host/issue.rs`), and this SDK is the one that re-expresses rather
than submits: it is its own core and inherits nothing from `flz_core`, so what
it shares is the fixture — `test/fixtures/reference_topologies.txt`, one row per
refusal, driven against every SDK by the same conformance run
([`conformance.md`](conformance.md) § The refusal table). Three names are absent
here and the fixture's `needs` column says why: `BranchTooFewArms`,
`MergeTooFewSources` and `DuplicateMergeSource` need a `branch` or a `merge`,
and the v1 subset carries neither.

Six calls declare the class rather than an edge and answer the token so they pipe in any
order: `effectively_once/1`, `checkpoint_ack/1` / `checkpoint_every/2` /
`outputs_at_checkpoint/2`, each of which turns the checkpoint granularity on by itself, and
`correction_window/2` / `beyond_window_policy/2`, which amend one correction policy so that
declaring a policy alone leaves a zero window — refused, because a fate for a window nobody
declared is an authoring error. Their refusals are `CheckpointAckWithoutTier`,
`CheckpointEveryZero`, `AtCheckpointWithoutMaxDelay`, `UnkeyedChannelUnderEffectivelyOnce`
(naming the sink's channel), `DroppingPolicyUnderEffectivelyOnce` (naming the node's inbound
channel and the policy), `CorrectionWithoutTier`, `CorrectionWithoutEventTime` (naming a
Procedure and the clock it declares) and `CorrectionWindowZero`.

## Keyed channels

`keyed/1` on the handle before `sink/2` declares output identity on that sink — the
reference's `.keyed()`. Every frame it produces carries `flz.entity`
(`<operative id>/<procedure name>/<entity key or empty>`), `flz.seq`
(`<change_seq at handout>.<emit ordinal>` — the seq the invocation started at, before its own
records advanced it, and the ordinal counting the invocation's plugin outputs from zero,
stamped or not) and `flz.key`, appended after the user's metadata so a stamp cannot be
clobbered. The key is the emit's own (`emit_keyed/3`), else the sink route's `key_by`, else
the emitting Entity's; a Single Entity with none is refused at the emit. A sink not declared
keyed carries no `flz.` key. The stamps are minted at the routing seam in the Operative, this
SDK's own core. Under the effectively-once tier the Entity captures the same output through
the same seam into its effect record and stamps nothing: the controller derives
`flz.entity`, `flz.seq` and `flz.epoch` at flush, so a host that also stamped would produce a
second answer.

## Overflow, retry, park

The overflow default is `:drop_nothing` for **every** edge kind, in lockstep with the
Rust/Java SDKs and the FFI host: it waits for room and never drops, at the cost that a delayed
primary-stream router delays every operative fed by that stream. `on_full(:drop_newest | :drop_oldest)` sheds
instead, and every shed item is counted: `drops_newest` / `drops_oldest` ride the heartbeat
beside `dispatches` and `blocked_parks`, the same keys the other SDKs report, and the
`[:flezha, :sdk, :mailbox]` telemetry event stays the in-node handle on the same drop.

**This SDK was the reference design for how `flz_core` now waits.** A full mailbox never blocks
a scheduler here: `handle_call({:enqueue, …})` DEFERS its reply and holds the value in a
`waiters` FIFO that `admit_waiter/1` drains in order. `flz_core` held the producing OS thread
instead until `nonblocking-drop-nothing-backpressure` gave it the same shape (a push holdback on
the producing Entity). So this SDK needed no mechanism change — only the reporting half, because
heartbeat keys are a cross-SDK contract: `pending_pushes` / `pending_push_oldest_ms` count the
Entities holding an unqueued push and the age of the oldest.
In native mode the Entity bound is a dispatch-concurrency bound, not a memory bound;
a frozen `dispatches` with `blocked_parks` climbing is backpressure, and frozen with it flat is
a stall (`data_heartbeat` every ~10 s per data stream, `control_ping` every 15 s, echoed by the
controller).

A shedding policy on a node fed under `effectively_once/1` is refused at registration
(`DroppingPolicyUnderEffectivelyOnce`, naming the channel and the policy): a durable-input-log
class that shed at its mailbox would hold the min-watermark on the shed offset for ever while
the log behind it kept growing.

A failing `run/3` (error tuple, raise, or exit) retries up to `retry_max` (default 3 total)
with exponential backoff and ±50 % jitter from `retry_backoff_ms` (default 50 ms, cap 5 s);
`{:error, {:non_retryable, _}}` parks immediately. On exhaustion the SDK ships a
byte-compatible `park_payload` and holds the Entity until the controller's `ParkPayloadAck`
(sent after the DLQ store fsync). Identical in both modes.

## Partitioning

With `partition_count >= 2` on `AddOperative`, a dynamic key is admitted against the owned
range **before** any Entity state exists (a miss parks with the contract error naming the key
and its group); static keys outside the range idle silently; a keyless topology refuses the
bind (`PartitionsWithoutKeyedProcedure`). The hash is the controller's own
`Flezha.Partition.group_of/1` ([`partitioned_operatives.md`](partitioned_operatives.md)).

## Dynamic entities

A dynamic node mints one Entity per key up to its `max_entities/0`; at the cap a new key's
input is dropped and counted (`[:flezha, :sdk, :dynamic]` telemetry, `event: :capped`), an
existing key's is served, and a restore that would materialise a key past the cap is refused.
With `entity_idle_ttl_ms/0` declared, the Operative runs one `:reap` tick at
`FLEZHA_REAP_INTERVAL_MS`, else `min(ttl / 4, 30 s)` and at least 1 ms. The sweep runs in
the Operative's own process — while it runs no push is routed, which is the drain gate — and
asks each Entity whether it has been idle past the TTL holding no work (a bounded probe; a
busy one is skipped until the next sweep), then evicts an idle one synchronously: the Entity
ships `delete_entity_state` on its own record stream when the class persists (behind its
records by construction), clears its slot so a successor is a cold start from `init_state/1`,
and stops. When no stream will take that delete, it and any records the Entity was still
holding pass to the Operative before the process ends, which replays them on the next bound
stream — ahead of anything a replacement Entity for the same key writes there. At most 256 evictions happen per sweep; the rest wait for the next tick. Evicted
keys are remembered (1 024 at most), so a live read of one answers `entity_evicted` rather
than `entity_not_found`. Without a TTL nothing is swept and the Entities live for the
operative's lifetime.

## Timers and clocks

`clock/0` names what `Ctx.now/1` serves, read once per invocation: `:wall` (the epoch
millisecond at dispatch entry, the default), `:logical` (the Entity's durable change seq —
`terminal + 1` after a restore — refused as `UnservableClock` on a procedure without
`apply_change/2`) or `:event_time` (what `event_time_of/1` extracts from the input; refused
as `EventTimeClockWithoutExtractor` without it). Inside `on_timer/4`, `now/1` is the
scheduled timestamp under every clock, so a chained `timer_after/2` schedules off the timer
that woke it and a replay registers the same instant.

A Procedure registers a deadline from `run/3` or from a firing and handles it in
`on_timer(fired_at, kind, state, ctx)`, which answers what `run/3` answers. Pending timers
are one set per kind, bounded by `max_timers_per_entity/0`: a duplicate registration is a
no-op, a cancel of nothing answers `false`. An invocation's registrations and cancellations
accumulate on a session over a copy of the set — opened on the first timer call, so an
invocation that registers nothing pays nothing — and commit only when it completes; a
firing's session opens on its own removal, committed even when the firing parks, so a poison
timer parks once. A firing on a Procedure that exports no `on_timer/4` parks non-retryably
naming the procedure and the callback.

**Which kind fires when.** A **processing-time** timer (`timer_after/2`) fires when the
wall clock reaches it: the Entity process arms one `Process.send_after/3` per pending
timer — the BEAM's own timer service is the wake list no thread of this SDK owns — and the
due message folds every timer due by then onto the Entity's own input queue, ascending, so
the drain that serialises inputs serialises firings, whatever order two same-millisecond
messages arrive in; a due message that finds no room comes back 5 ms later, never drops; a
firing whose timer has left the pending set fires nothing. An **event-time** timer
(`timer_at_event_time/2`, `:event_time` clock only) fires at admission: when an input's
`event_time_of/1` advances the Entity's high-water past it, every due timer is queued ahead
of that input, ascending — an Entity nothing arrives at never fires one. The order on the
wire is the same for both: the dispatch's change records, then its timer ops, then — once
the timers are armed and counted — its outputs.

**Durability.** The committed ops ship after the dispatch's change records as one
`format_id = 3` record (the postcard `Vec<TimerOp>` every SDK writes, stored by the
controller as any change record), so the restore fold rebuilds the pending set from the
same log the state comes back through and re-arms it, overdue timers firing at once,
ascending; a compaction re-states the whole set above the snapshot's seq; the slot a
replacement seeds from carries it. A Procedure without `apply_change/2` holds its timers in
memory only — the shared core's host path buffers timer ops only for a slot with a flusher
entry, and this SDK ships nothing for a `NoChange` procedure. Eviction discards an Entity's
timers with its state. A Single or Static Entity that crashes outside the ladder is minted
again at once, as it was at start, so the timers its slot holds are re-armed before any
input; it cannot loop, because everything a Procedure raises rides the ladder and parks —
only a crash outside it re-mints, and a fresh Entity dispatches nothing until an input or a
due timer reaches it.

**Observability.** Registrations, firings, cancellations and refusals are one
`[:flezha, :sdk, :timers]` telemetry event carrying `event:` and `procedure:` — this SDK's
per-Procedure counters. The heartbeat carries `timers_armed` (the processing-time timers
armed on the BEAM's timer service, released on firing, disarm and the Entity's end; an
Entity killed untrappably releases nothing, so the gauge over-states by its count until the
runner restarts) and `timers_fired`.

## Persistence

A procedure that exports `apply_change(state, change)` is changeful and persisted; one
without it is the reference's `NoChange` — its state is a plain term nothing ships.
`Flezha.SDK.Ctx.apply(ctx, state, change)` is the only recorded mutation path: it returns
`apply_change(state, change)` and records the change, and `run/3` returns the state it
produced — one fold, one push, so a replay of the recorded changes is the state.

```elixir
defmodule Counter do
  @behaviour Flezha.SDK.Procedure
  def name, do: "counter"
  def init_state(_key), do: %{value: 0}
  def apply_change(s, {:delta, n}), do: %{s | value: s.value + n}
  def state_from_json(%{"value" => v}), do: %{value: v}
  def run(n, s, ctx), do: {:ok, Flezha.SDK.Ctx.apply(ctx, s, {:delta, n})}
end
```

After a successful dispatch the Entity ships one `entity_state_change` frame per recorded
change — the shared 16-byte header (`sdk_lang` 3, the SDK's version, the schema version, the
Erlang External Term Format id `7` allocated in `flz_core::header`), the record framed
through `Flezha.State.Wire.encode_frame/4` under the instance-keyed identity, at
`seq = change_seq` from `0`, in dispatch order on one secondary stream, before the dispatch's
own output frame, each under the credit window. The body is `:erlang.term_to_binary/2`,
deterministic. With the class's `persistence_enabled/0` false the seq advances and nothing
ships. A changeful Entity also writes `{state, seq}` into its Operative's slot table after
its records are on the wire, so an Entity killed untrappably comes back on the same seq line
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
Entity's records, the Operative's plugin-bound outputs, the frames an Entity hands on when it
stops, and the runner's own two — the secondaries the workflow driver's writes and parks go on,
and the control channel. The holds are ordered against each other: on a rebind the Operative
replays what stopped Entities handed it, then rebinds every live Entity and waits, then replays
its own outputs — so a dispatch's records reach the wire ahead of its output frame on a replay
exactly as they do live, and a dead Entity's delete ahead of both.

The controller's commands are answered by `Flezha.SDK.Commands`, off the runner's loop:
`RestoreInitState` folds the snapshot then the change log in the controller's on-disk framing
(`Flezha.SDK.Record.fold/6` — CRC over header and body, language before format before schema,
the shared core's refusal wordings), re-arms `change_seq` to `max(folded, terminal_seq) + 1`
and replies `terminal_seq`; the walk has one arm per format, so an effect record
(`format_id = 2`) is unwrapped in place — its enclosed change bodies fold in seq order under
the enclosing record's version decision, its timer ops join the pending set, its consumed
offset becomes the Entity's floor and its event time the high-water — and a snapshot header
carrying format 2 is malformed; `CompactEntity` ships `entity_state_snapshot` at `change_seq - 1`
behind the Entity's records and replies the seq and size; `EntityState` answers the
Decision-10 JSON (`%{procedure => state}` for a Single node, `%{procedure => %{key => state}}`
for a Static or Dynamic one, narrowed by the selector) with `flz.change_seq` on the reply
frame for one Entity and the refusals `entity_key_without_procedure`, `procedure_not_found`,
`entity_not_found`, `entity_evicted`, `entity_busy` by name; `EntityApplyState`, `MigrationDeserialization` and
a non-empty `AddOperative.init_values` hand each procedure's JSON to its `state_from_json/1`
(the object itself by default — declare it when the state's keys are atoms);
`CreditWindowInit` and `CreditGrant` drive `Flezha.SDK.Credit`, one window per runner that
parks the shipping Entity — never drops — and counts `credit_parked` on the heartbeat. A
class whose smallest `compact_after_bytes/0` differs from the default declares it. Two nodes
of one changeful module in a class would share a WAL identity and are refused at registration
(`DuplicatePersistentProcedure`); the reference gives a named node its own identity, but here
`named/2` also names the channel and the procedure name is the WAL segment.

### Schema evolution

`schema_version/0` (1) is stamped on every record and resolved per record on restore against
`min_readable_schema_version/0` (the version itself unless declared; a floor above the
version reads the version only): equal decodes, between the floor and the version migrates,
above the version or below the floor refuses by name with the shared core's wordings. A
migrating record is decoded and handed to `migrate_state/2` (a snapshot) or
`migrate_change/2` (a change) with the stamped version and the DECODED term — the body codec
is `:erlang.term_to_binary/2`, so there is no second codec for an author to choose —
answering `{:ok, term}` at the running version or `{:error, reason}`. Both default to
refusing (`no migration from schema_version N: the procedure declares none`), and a failing
one refuses naming its reason and the version, reseating nothing. A restore that migrated
answers `migrated`, and the controller compacts behind it so the migration is paid once.

A graceful stop — SIGTERM (the BEAM's own `init:stop`, in a runner-only boot), `close/1`, a
terminal `RunnerDisconnect` — runs `terminate/2`: every live operative is serialised, one
`RunnerShutdownNotification` ships per operative, and the acks are awaited at most 2 s in
parallel before the connection closes. `child_spec/1` sets a 10 s shutdown budget for it. A
non-terminal `RunnerDisconnect` stops the runner in place — the reference's `Stopped`: the
ack goes out, this side closes its streams, nothing reconnects and the process stays, so a
`:permanent` supervisor has nothing to restart into a second registration.

## Effectively-once tier

`Channels.effectively_once/1` puts a class on the tier. The declaration ships
`effectively_once: "true"` (and `ack_granularity: "checkpoint"` when one of the three
checkpoint options is declared), which is what makes the controller stamp `flz.offset` on
every forwarded input, re-forward from `watermark + 1` on every bind, and derive the output
identity at flush.

**One record per effectful invocation.** In place of the change records and the trailing
timer-ops record, the Entity ships one `entity_effect_record` (`format_id = 2`, the postcard
body `Flezha.State.Wire.encode_effect_body/1` writes byte-exactly) carrying the invocation's
changes, its captured plugin outputs, the consumed offset, the event time, the timer ops and,
on a park, the dead-letter entry. State, outputs and the offset cannot tear because they are
one record at one seq.

**The offset and the floor.** The Operative reads `flz.offset` off the frame metadata for a
tier class only and enqueues `%Entity.Input{value, offset}`; an emit's origin carries the
offset of the input that produced it, so an inter-procedure push inherits it. An input at or
below the floor a restore rebuilt is a completed dispatch: the counter advances and no record
ships.

**The interval and its four closes.** `checkpoint_ack/1`, `checkpoint_every/2` and
`outputs_at_checkpoint/2` each turn checkpoint granularity on. `Flezha.SDK.Entity.Interval`
then stages the changes, the timer ops and the last offset across invocations, and one record
closes the whole interval — on `Ctx.checkpoint/1`, on a park, on `every` inputs, or on
`max_delay_ms` after the interval opened. The delay is one `Process.send_after/3` whose
message enqueues a close on the Entity's own queue, so it lands behind the inputs that
preceded it and runs no author code; a stale one finds nothing open.

**Held outputs.** Under `outputs_at_checkpoint/2` the interval's outputs are held with its
changes and ride the closing record in production order under one seq. Under the default
they flush as they happen, so a sink sees an interval's outputs long before its offsets are
acknowledged.

**The park holds nothing.** A poison input parks inside the record as its `dlq` entry — with
the error, the attempts, the payload bytes and the offset — and closes the interval. There is
no `park_payload` frame and no wait for `ParkPayloadAck`: the queued input dispatches
immediately. A parked firing's removal rides the same record's `timers`.

**The edge hold.** An invocation whose outputs include an inter-procedure push holds its
record in a FIFO until the router reports every push admitted, so the offset is never durable
ahead of the output. The Entity never gates its own queue on that report: on a single-process
router a gated Entity and a router blocked on that Entity's full queue would wait on each
other.

**`Ctx.stable_id/1`** answers the frozen 32-hex FNV-1a-128 over the Entity's wire identity,
the seq the invocation started at and a per-invocation ordinal — the same value every other
SDK derives for the same three facts, so an effect re-driven after a crash carries the id it
carried before.

**The restore and the slot.** A restore discards the open interval, cancels its deadline and
sets the floor and the event-time high-water from the log. The Operative's slot table carries
the staged interval and the floor beside `{state, seq}`, so an Entity killed untrappably comes
back holding what it had staged; records already owed to an edge report die with the process,
and the seq gap that leaves is what makes the controller resync the Entity and re-forward
those offsets.

A class with `persistence_enabled/0` false advances the seq and ships nothing under the tier,
as it does outside it.

## Runner-only boot (standalone)

A standalone node depends on flezha as a library without becoming a controller:
`config :flezha, controller_enabled: false` (env `FLEZHA_CONTROLLER_ENABLED=false|0|no|off`;
default true). `Flezha.Application.start/2` then starts an empty supervision tree — no
transport servers, HubManager, convergence, state-path migration or boot-ready gate — and the
host app runs SDK processes via `Flezha.SDK.child_spec/1`. `test/apps/elixir_runner` is a
working example (path dependency, runner-only, stdin-EOF watcher).

## Late-input correction

`Channels.correction_window/2` declares how far behind an Entity's event-time high-water a
late input may land and still be corrected; `beyond_window_policy/2` declares what happens to
one that lands further back — `:process_as_arrival`, the default and the behaviour of a class
with no window at all, or `:drop`. The declaration requires `effectively_once/1` (only the
tier journals inputs before forwarding them, so only the tier has a durable input to replay)
and at least one `:event_time`-clocked Procedure (there is nothing else to order a replay by).
It ships `correction_window_ms` and `beyond_window_policy`, which is what makes the controller
derive the input-log hold window and the snapshot age floor from the declaration rather than
from an operator setting.

**There is no splice.** An input is journaled before it is forwarded, so a late one is already
in the log at its arrival offset; the correction is the tier's ordinary restore-and-replay with
a rewound baseline and an event-time-ordered slice. The SDK's half is four things:

- **Classify, before advancing.** `Flezha.SDK.Entity.Clock` tracks the high-water and
  `Flezha.SDK.Entity.Tier.classify/3` places every input against it. At or ahead of it — or
  with no high-water yet — the input dispatches and the high-water rises.
- **Ask, and withhold.** Behind the high-water but inside the window, the input is *not*
  dispatched: one `correction_request` frame goes out on the Entity's own state stream carrying
  the target event time alone, and the replayed slice brings the input back in place. With no
  state stream there is nobody to ask, so it falls back to processing as an arrival.
- **Never drag the window backwards.** A beyond-window arrival is dispatched under the default
  policy but does **not** advance the high-water — otherwise one hour-old straggler would make
  every input after it look late.
- **Hold the slice, then order it.** Inputs the controller marks `flz.replay` are buffered as
  the Entity pops them; the payload-less `flz.replay_end` broadcast reaches every Entity of the
  operative and takes its place on each one's queue behind the members it closes. Popping it
  dispatches the slice by ascending event time, ties broken by ascending input-log offset,
  ahead of anything enqueued behind the broadcast. A restore discards a buffered slice, because
  the controller re-forwards it from the rewound watermark.

The fixture is `candle` in `test/apps/elixir_runner`, the same id and the same knobs every
other SDK registers, and the scenarios are `test/system/late_input_correction_e2e_test.exs`.

## Deferred capabilities (v1 scope)

The list is [`test/apps/elixir_runner/conformance.json`](../../test/apps/elixir_runner/conformance.json)'s
`deferred` — the one place this SDK's scope is declared, read by the e2e harness, the bench and
the conformance gate ([`conformance.md`](conformance.md)). In prose: the workflow kind (`kind: workflow` on `AddOperative` is
refused: `workflow kind is not supported by the Elixir stream SDK`); `merge`, `route_latest`,
`branch`, `codec`. A builder/ctx call into a deferred surface fails registration loudly,
naming the operative module.

One more, outside the vocabulary because no capability names it: the hex package
(`package-elixir-sdk`).

Each deferred capability's follow-up re-expresses the kernel's rules in Elixir under the
"one change, every SDK" rule — this SDK is not a poll host and inherits nothing from `flz_core`, as its
persistence already shows.

## Packaging boundary

SDK modules (`lib/flezha/sdk/`) touch the controller application only through named seams:
`Flezha.SDK.Codec` (the only module that may reference `Flezha.Runner.CommandsProto` /
`RunnerCommands`), `Flezha.State.Wire` (the frame and entity-id codec, the postcard park body),
`Flezha.Partition.group_of/1` (key-group hash), `Flezha.Transport.Stream`,
`Flezha.Transport.Local(.Stream)` and `Flezha.Transport.Server` (native),
`Flezha.Transport.Native` (the `flz_nif` client entry points, socket modes), and
`Flezha.SDK.Knob` (the only module that may reference `Flezha.Config` and the tables under
it — a runner is a different process and reads the environment, never the controller's
application env, so every other SDK module asks `Knob.get/1` for a value and
`Knob.arm/4`/`Knob.wait/4` for a wait).
`scripts/check-sdk-boundary.sh` enforces it, so the hex extraction (`package-elixir-sdk`) is a
swap of seam internals, not a rewrite.

## See also

- [`authoring_contract.md`](authoring_contract.md) — the cross-SDK authoring contract
- [`conformance.md`](conformance.md) — the shared conformance suite and this SDK's manifest
- [`protocol_spec.md`](protocol_spec.md) — the binary protocol a standalone runner speaks
- [`../internals/nif_transport.md`](../internals/nif_transport.md) — the NIF client behind the
  socket modes
