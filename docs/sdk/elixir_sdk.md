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
env the Rust client reads (`FLEZHA_SOCKET_FILES_PATH`, `FLEZHA_QUIC_ADDRESS`,
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
`1..1_048_576`), `max_entities/0` (100_000). `Flezha.SDK.Ctx`: `emit/2`, `emit_with_meta/3`
(a reserved `flz.` key answers `{:error, {:reserved_metadata_key, k}}`), `now/1` (wall ms,
once per invocation), `entity_key/1`.

## Graph builder (v1 subset)

`input`/`input_with` → `into`/`into_named` → `sink`/`sink_with`/`sealed`, with
`map`/`filter`/`flat_map`/`key_by` as edge transforms (fused into one closure per consumer
edge), plus `named`, `on_full`, `retry_max`, `retry_backoff_ms`. Fan-out is handle reuse;
unnamed mentions of one module unify (fan-in). Entity mode is derived: non-empty
`entity_keys/0` → static; a keyed edge into a keyless procedure → dynamic; else single.
Refusals accumulate into one `{:error, {:invalid_topology, issues}}` — e.g.
`TransformAfterKeyBy`, `RetryMaxZero`, `DeferredBuilderSurface` (names the operative module).

## Overflow, retry, park

The overflow default is `:drop_nothing` for **every** edge kind, in lockstep with the
Rust/Java SDKs and the FFI host: it waits for room and never drops, at the cost that a delayed
primary-stream router delays every operative fed by that stream. `on_full(:drop_newest | :drop_oldest)` sheds
instead. In native mode the Entity bound is a dispatch-concurrency bound, not a memory bound;
the heartbeat's frozen `dispatches` / `blocked_parks` counters make a stall visible
(`data_heartbeat` every ~10 s per data stream, `control_ping` every 15 s, echoed by the
controller).

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
the conformance gate ([`conformance.md`](conformance.md)). In prose: state persistence and
everything gated on it (entity state shipping, restore, compaction, credit windows, the
effectively-once tier, checkpoints, correction windows, entity timers, `ctx.stable_id`,
clocks); the workflow kind (`kind: workflow` on `AddOperative` is refused: `workflow kind is
not supported by the Elixir stream SDK`); keyed channels (`keyed`, `emit_keyed`, output
identity stamps); `merge`, `route_latest`, `branch`, `codec`; dynamic-entity eviction (dynamic
Entities live for the operative's lifetime); and the entry-idle observer. A builder/ctx call
into a deferred surface fails registration loudly, naming the operative module; a persistence
frame from the controller is ignored with a warning.

Two more, outside the vocabulary because no capability names them:
graceful-shutdown final-state, and the hex package (`package-elixir-sdk`).

Each deferred capability's follow-up re-expresses the kernel's rules
(`StagedInterval::fold`, the credit window, restore) in Elixir under the "one change, every
SDK" rule — this SDK is not a poll host and inherits nothing from `flz_core`.

## Packaging boundary

SDK modules (`lib/flezha/sdk/`) touch the controller application only through named seams:
`Flezha.SDK.Codec` (the only module that may reference `Flezha.Runner.CommandsProto` /
`RunnerCommands`), `Flezha.State.Wire.encode_park_body/1` (postcard),
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
