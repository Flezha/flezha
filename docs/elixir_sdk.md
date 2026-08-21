# Elixir SDK

The Elixir runner SDK gives Elixir applications the same Operative/Procedure/Entity
programming model the Rust and Java SDKs have, deployable **two ways from one
core**:

- **In-node (`native` transport)** — the runner runs in the Controller's own BEAM
  node. The data path is plain message passing: zero framing, zero syscalls,
  binaries by refcount. Same-node only, opt-in.
- **Standalone (`uds` / `quic` transport)** — the runner is its own BEAM node
  (same machine over UDS, a different machine over QUIC), speaking the existing
  binary protocol. It is **indistinguishable from a Rust runner** to the
  Controller, with zero controller-side changes.

Everything above the transport seam — behaviours, the graph builder, dispatch,
the retry→park ladder, the partition guard, heartbeats, stable identity — is
shared code with identical semantics in both modes.

## Choosing a mode

| | In-node (`:native`) | Standalone (`:uds` / `:quic`) |
|---|---|---|
| Where | Controller's BEAM | Own BEAM node |
| Data path | message passing | UDS / QUIC binary protocol |
| Isolation | per-process crashes contained; **memory/scheduler shared** with the Controller | full OS-process isolation |
| Use when | trusted, latency-sensitive, co-located operatives | untrusted or resource-unpredictable operatives, or cross-node deployment |
| Security | unreachable from outside the node (a function call); same-node only | UDS filesystem perms / QUIC mutual TLS — the Rust/Java runner surface |

The `:native` default is deliberate and **not** env-derived: a controller node
legitimately sets `FLEZHA_TRANSPORT=uds` for its *server*, and an env-following
in-node SDK would then dial its own node over a socket by accident. Standalone
deployments pass `:uds`/`:quic` explicitly; the connection parameters then come
from the same env the Rust client reads (`FLEZHA_SOCKET_FILES_PATH`,
`FLEZHA_QUIC_ADDRESS`, `FLEZHA_CA_CERT_PATH`).

## Registration

Runner-as-handle, mirroring the cross-SDK surface. Errors are named tuples
returned at the call site (never a raise) and propagate through a pipe.

```elixir
Flezha.SDK.create_runner("my_namespace", transport: :native)
|> Flezha.SDK.define_stream(MyOperative)
|> Flezha.SDK.run()   # start/1 then await_termination/1
```

Or under a host application's supervision tree:

```elixir
children = [
  {Flezha.SDK, namespace: "my_namespace", streams: [MyOperative], transport: :native}
]
Supervisor.start_link(children, strategy: :one_for_one)
```

- `create_runner/2` returns a handle — no connection, no processes, no slot claim.
- `define_stream/2` / `remove_stream/2` are legal before and after `start/1`; a
  post-start change ships `UpdateFactories`. Removing a type with a live instance
  refuses `{:error, {:operative_type_in_use, factory_id, count}}`.
- `start/1` claims the process-wide runner slot (the loser gets
  `{:error, {:already_running, ns}}`) and connects.
- `run/1` = `start/1` then `await_termination/1`; `close/1` disconnects.

## Behaviours

```elixir
defmodule MyOperative do
  @behaviour Flezha.SDK.StreamOperative
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

`run/3` returns `{:ok, new_state} | {:error, reason} | {:error, {:non_retryable, reason}}`.
Optional `Procedure` callbacks: `entity_keys/0` (static keyset), `entities_for/1`,
`mailbox_capacity/0` (1024; env `FLEZHA_MAILBOX_CAPACITY`, range `1..1_048_576`),
`max_entities/0` (100_000). `Ctx`: `emit/2`, `emit_with_meta/3` (reserved `flz.`
keys refused), `now/1` (wall ms, once per invocation), `entity_key/1`. Emits flush
only on `{:ok, _}` — a failing invocation discards its partial emits.

## Graph builder (v1 subset)

`input`/`input_with` → `into`/`into_named` → `sink`/`sink_with`/`sealed`, with
`map`/`filter`/`flat_map`/`key_by` as edge transforms (fused into one closure per
consumer edge), `named`, `on_full`, `retry_max`, `retry_backoff_ms`. Fan-out is
handle reuse; unnamed mentions of one module unify (fan-in). Entity mode is
derived: non-empty `entity_keys/0` → Static; a keyed edge into a keyless
procedure → Dynamic; else Single. Registration refusals accumulate into a single
`{:error, {:invalid_topology, issues}}`.

## Overflow policies

The default is `:block` for **every** edge kind (in lockstep with the Rust/Java
SDKs and the FFI host). `:block` waits for room and never drops — the documented
co-tenancy cost is that a blocked primary-stream router delays every operative fed
by that stream, exactly as the Rust reader parks. Declare `on_full(:drop_newest)`
(or `:drop_oldest`) to shed instead. In the native mode the Entity bound is a
dispatch-concurrency bound, not a memory bound (backpressure lands in the router's
own mailbox), which the heartbeat's frozen `dispatches`/`blocked_parks` makes
visible.

## DLQ ladder

A failing `run/3` (error tuple, raise, or exit) retries up to `retry_max` (default
3 total) with exponential backoff + jitter from `retry_backoff_ms` (default 50 ms,
cap 5 s); `{:error, {:non_retryable, _}}` parks immediately. On exhaustion the SDK
ships a byte-compatible `park_payload` and holds the Entity until the controller's
`ParkPayloadAck` (sent after the DLQ store fsync). Identical in both modes.

## Partitioning

When `AddOperative` carries `partition_count >= 2`: a dynamic key is admitted
against the owned range **before** any Entity state exists (a miss parks with the
contract error naming the key and its group); static keys outside the range idle
silently; a keyless topology refuses the bind (`PartitionsWithoutKeyedProcedure`).
The key-group hash is the controller's own pinned FNV-1a-128.

## Runner-only boot (standalone)

A standalone node depends on flezha as a library without becoming a controller:
set `config :flezha, controller_enabled: false` (or `FLEZHA_CONTROLLER_ENABLED=false`).
`Flezha.Application.start/2` then starts an empty tree — no transport servers, no
HubManager, no convergence, no state migration — leaving the host app to run only
SDK processes via `Flezha.SDK.child_spec/1`. `test/apps/elixir_runner` is a working
example (path dependency, runner-only, stdin-EOF watcher).

## Deferred capabilities (v1 scope)

State persistence and everything gated on it (entity state shipping, restore,
compaction, credit windows, the effectively-once tier, checkpoints, correction
windows, entity timers, `ctx.stable_id`, clocks); the workflow kind; keyed
channels (`keyed`, `emit_keyed`, output identity stamps); `merge`, `route_latest`,
`branch`, `codec`; dynamic-entity eviction (v1 dynamic Entities live for the
operative's lifetime); the entry-idle observer; graceful-shutdown final-state; and
the hex package (`package-elixir-sdk`). A builder/ctx call into a deferred surface
fails registration loudly, naming the operative module.

## Packaging boundary

SDK modules (`lib/flezha/sdk/`) touch the controller application only through named
seams — `Flezha.SDK.Codec` (protobuf), `Flezha.State.Wire.encode_park_body/1`
(postcard), `Flezha.Partition.group_of/1` (key-group hash), `Transport.Server` /
`Transport.Local.Stream` / `Transport.Stream` (native), and the `flz_nif` client
entry points (socket). `scripts/check-sdk-boundary.sh` enforces it from this change
onward, so the later hex extraction (`package-elixir-sdk`) is a swap of seam
internals, not a rewrite.
