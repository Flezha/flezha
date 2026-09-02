# Elixir SDK

For Elixir developers authoring operatives and for SDK implementers. The Elixir runner SDK
gives an Elixir application the Operative/Procedure/Entity model of the Rust and Java SDKs,
deployable two ways from one core: **in-node** (`:native`) inside the Controller's own BEAM
node over message passing, or **standalone** (`:uds` / `:quic`) as its own BEAM node speaking
the binary protocol, indistinguishable from a Rust runner to the Controller. Everything above
the transport seam is shared code with identical semantics in both modes. The SDK inherits
nothing from `flz_core` ([`../architecture/runner.md`](../architecture/runner.md#one-change-every-sdk)).

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
`migrate_state/2`, `migrate_change/2` (see [Schema evolution](#schema-evolution)).
`StreamOperative` takes an optional `persistence_enabled/0` (default `true`).
`Flezha.SDK.Ctx`: `emit/2`, `emit_with_meta/3` (a reserved `flz.` key answers
`{:error, {:reserved_metadata_key, k}}`), `emit_keyed/3` and `emit_keyed_with_meta/4` (the
output's natural key on a sink declared `keyed/1`; on a node with no keyed sink they answer
`{:error, {:key_not_allowed_on_non_keyed_channel, channel}}`, and a keyless emit from a
Single Entity on a keyed sink answers `{:error, {:key_required_on_keyed_channel, channel}}`),
`apply/3` (fold a change and record it), `now/1` (wall ms, once per invocation),
`entity_key/1`.

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
`KeyedInterProcedureChannel` (`keyed/1` reaching a procedure before any sink),
`DeferredBuilderSurface` (names the operative module).

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
SDK's own core; there is no tier here yet, so every keyed emit is stamped.

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

There is no declare-time refusal of a shedding policy here, because this SDK declares no
durability tier and no correction window for one to contradict — the rule the other SDKs
enforce ships with `effectively_once` if this SDK ever gains it.

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
and stops. At most 256 evictions happen per sweep; the rest wait for the next tick. Evicted
keys are remembered (1 024 at most), so a live read of one answers `entity_evicted` rather
than `entity_not_found`. Without a TTL nothing is swept and the Entities live for the
operative's lifetime.

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

The controller's commands are answered by `Flezha.SDK.Commands`, off the runner's loop:
`RestoreInitState` folds the snapshot then the change log in the controller's on-disk framing
(`Flezha.SDK.Record.fold/6` — CRC over header and body, language before format before schema,
the shared core's refusal wordings), re-arms `change_seq` to `max(folded, terminal_seq) + 1`
and replies `terminal_seq`; `CompactEntity` ships `entity_state_snapshot` at `change_seq - 1`
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

## Runner-only boot (standalone)

A standalone node depends on flezha as a library without becoming a controller:
`config :flezha, controller_enabled: false` (env `FLEZHA_CONTROLLER_ENABLED=false|0|no|off`;
default true). `Flezha.Application.start/2` then starts an empty supervision tree — no
transport servers, HubManager, convergence, state-path migration or boot-ready gate — and the
host app runs SDK processes via `Flezha.SDK.child_spec/1`. `test/apps/elixir_runner` is a
working example (path dependency, runner-only, stdin-EOF watcher).

## Deferred capabilities (v1 scope)

The list is [`test/apps/elixir_runner/conformance.json`](../../test/apps/elixir_runner/conformance.json)'s
`deferred` — the one place this SDK's scope is declared, read by the e2e harness, the bench and
the conformance gate ([`conformance.md`](conformance.md)). In prose: the effectively-once
tier and everything on it (effect records, checkpoints, `ctx.stable_id`); correction windows
and the clock kinds; entity timers; the workflow kind (`kind: workflow` on `AddOperative` is
refused: `workflow kind is not supported by the Elixir stream SDK`); `merge`, `route_latest`,
`branch`, `codec`. A builder/ctx call into a deferred surface fails registration loudly,
naming the operative module.

One more, outside the vocabulary because no capability names it: the hex package
(`package-elixir-sdk`).

Each deferred capability's follow-up re-expresses the kernel's rules
(`StagedInterval::fold`, the timer set, the replay slice) in Elixir under the "one change,
every SDK" rule — this SDK is not a poll host and inherits nothing from `flz_core`, as its
persistence already shows.

## Packaging boundary

SDK modules (`lib/flezha/sdk/`) touch the controller application only through named seams:
`Flezha.SDK.Codec` (the only module that may reference `Flezha.Runner.CommandsProto` /
`RunnerCommands`), `Flezha.State.Wire` (the frame and entity-id codec, the postcard park body),
`Flezha.Partition.group_of/1` (key-group hash), `Flezha.Transport.Stream`,
`Flezha.Transport.Local(.Stream)` and `Flezha.Transport.Server` (native), and
`Flezha.Transport.Native` (the `flz_nif` client entry points, socket modes).
`scripts/check-sdk-boundary.sh` enforces it, so the hex extraction (`package-elixir-sdk`) is a
swap of seam internals, not a rewrite.

## See also

- [`authoring_contract.md`](authoring_contract.md) — the cross-SDK authoring contract
- [`conformance.md`](conformance.md) — the shared conformance suite and this SDK's manifest
- [`protocol_spec.md`](protocol_spec.md) — the binary protocol a standalone runner speaks
- [`../internals/nif_transport.md`](../internals/nif_transport.md) — the NIF client behind the
  socket modes
