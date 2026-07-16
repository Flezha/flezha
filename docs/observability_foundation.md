# Observability foundation

This document describes the `Flezha.Observability` subsystem — the
contracts, default sinks, and operator surface that the rest of the
productionization umbrella (export plugins, web console, audit log,
authn/authz) builds on top of.

Source code lives under `lib/flezha/observability/` and the operator
surface under `lib/flezha/service/observability.ex`. The subsystem is
inserted into the application supervision tree by
`Flezha.Observability.Supervisor`, started right after
`Flezha.Service.HubManager` in `Flezha.Application.start/2`.

## Signal taxonomy

Every producer and consumer agrees on one envelope shape:

```elixir
%Flezha.Observability.Signal{
  kind: :metric | :event | :log | :trace_span,
  source: atom(),       # registered source name
  name: atom(),         # signal-name-within-source
  measurements: map(),  # numeric fields
  metadata: map(),      # categorical fields / labels
  ts: integer()         # System.monotonic_time(:microsecond)
}
```

Construct via `Flezha.Observability.Signal.new/1`. The constructor
validates that `kind` is one of the four atoms, that `source` and
`name` are atoms, and that `measurements` / `metadata` are plain
maps (keyword lists are rejected). `ts` is auto-filled from
`System.monotonic_time(:microsecond)` when not supplied.

## Source contract

Producers register either as a **declarative source** (emit-driven
application code) or as a **process-backed source** (a long-lived
GenServer implementing `Flezha.Observability.Source`). Both flavours
register through `Flezha.Observability.Registry.register_source/1`.

```elixir
# Declarative
:ok = Flezha.Observability.Registry.register_source(%{
  name: :my_source,
  kinds: [:metric, :event],
  flavour: :declarative,
  cardinality_budget: 1_000
})

Flezha.Observability.Bus.emit(
  Flezha.Observability.Signal.new(%{
    kind: :metric,
    source: :my_source,
    name: :requests_total,
    measurements: %{count: 1}
  })
)

# Process-backed
defmodule MyPolledSource do
  @behaviour Flezha.Observability.Source

  @impl true
  def init(_), do: {:ok, %{}}

  @impl true
  def handle_poll(_now, state), do: {:ok, state}

  @impl true
  def config_schema, do: %{poll_interval_ms: :pos_integer}
end
```

## Sink contract

Sinks implement `Flezha.Observability.Sink`. Each Sink is a
GenServer started under
`Flezha.Observability.SinkSupervisor` (`:one_for_one`) so one Sink
crashing or stalling never affects peer Sinks or producers. The
Sink declares the kinds and source globs it wants via `filter/0`:

```elixir
%{kinds: [:metric, :event], sources: :any}
%{kinds: [:log], sources: [:logger]}
```

`config_schema/0` is consulted at registration time. An invalid
config rejects the registration before the Sink process is started.

## Registry

`Flezha.Observability.Registry` owns two named ETS tables —
`:flezha_observability_sources` and `:flezha_observability_sinks`,
both `:protected, :set`. Read paths (`list_sources/0`,
`list_sinks/0`, `lookup_*`, source-status lookups) read directly
from ETS without serialising through the GenServer. Writes
(register, unregister, status transitions, cardinality
mutations) go through the GenServer so the tables stay consistent.

### Cardinality budget

Each source declares a `cardinality_budget` (default `1_000`). The
Registry tracks distinct *metadata-key shapes* per source (the
sorted list of metadata keys, hashed via `:erlang.phash2/1` — the
values are not part of the shape so cardinality is bounded by
*label keys*, not label values).

On a `:metric` emit, the bus calls `Registry.check_cardinality/2`:

* If the shape is already known → `:ok` (fast path; direct ETS read).
* If the shape is new and within budget → `:ok` with the shape
  added to the source's tracked set (GenServer call).
* If the shape is new and at/over budget → `{:dropped,
  :cardinality_budget_exceeded}`. The first time this happens for
  a given source, a single `:cardinality_budget_exceeded` event is
  emitted on the `:observability_registry` source carrying only
  the offending source name (never the offending metadata).

The budget does NOT apply to `:event`, `:log`, or `:trace_span`
signals — those are rate-limited by the overflow policy instead.

### Source statuses

Sources move between `:active` and `:disabled` via
`enable_source/1` / `disable_source/1`. Disabled-source emits are
dropped at the bus with no sink work.

## Bus overflow policy

`Flezha.Observability.Bus.emit/1` runs in the calling process. It:

1. Looks up the source's registration row via direct ETS read.
2. Drops the signal if the source is `:disabled`.
3. Validates the signal's `kind` against the source's declared
   kinds — a mismatch is dropped and a `:source_kind_violation`
   event is emitted on `:observability_registry`.
4. For `:metric` signals, calls `Registry.check_cardinality/2` and
   drops if the budget is exceeded.
5. Reads the sink registry via direct ETS read and selects the
   sinks whose `filter/0` accepts the signal.
6. For each interested sink under the default **`:drop` overflow
   policy**, checks the sink's mailbox via
   `Process.info(pid, :message_queue_len)`. If at or above the
   `mailbox_high_watermark` (default `10_000`), the per-sink
   `:dropped` counter increments and the signal is skipped.
   Otherwise the bus posts `{:signal, signal}` to the sink mailbox
   via `Process.send/3`.
7. For sinks registered with **`:block` overflow policy**, the bus
   uses `GenServer.call(sink_pid, {:signal, signal}, block_timeout_ms)`
   — synchronous, intentional back-pressure on the producer (used
   by the future audit-log sink).

The entire dispatch body is wrapped in `try/rescue/catch`. On any
error the `:dispatcher_failure_count` counter (see
`Flezha.Observability.Counters`) increments and the error is logged
at `:error` once per unique error class. A broken observability
subsystem must never crash a producer.

## Default sinks

### Ring-buffer sink (`Flezha.Observability.RingBufferSink`)

Owns one ETS `:ordered_set` table per registered source —
`:protected` (read from any process, written only by the sink).
Entries are `{{ts, monotonic_seq}, signal}`. Two retention layers:

* Opportunistic cap (`:ring_buffer_max_entries_per_source`, default
  `10_000`): on insert, drop the oldest entry when the cap is
  exceeded.
* Periodic age sweep (every `retention_check_interval_ms`,
  default `5_000`): `:ets.select_delete/2` deletes entries older
  than `:ring_buffer_retention_ms` (default `900_000` — 15 min).

The sink subscribes to Registry registration events via
`Phoenix.PubSub` so per-source tables are created / dropped on the
fly.

Range queries via
`Flezha.Observability.RingBufferSink.query(source, from_ts, opts)`
run in the **caller process** via direct ETS access — they never
block producers. Pass `from_ts: 0` for "no lower bound" (the BEAM
monotonic-time origin is typically negative, so the bare guard
`ts >= 0` would otherwise filter everything out on a freshly-booted
node).

### Phoenix.PubSub sink (`Flezha.Observability.PubSubSink`)

Broadcasts every accepted signal on
`Flezha.Observability.PubSub` under topic
`flezha.observability.source.<source-atom>`. Subscribers receive
`{:flezha_signal, %Flezha.Observability.Signal{}}`.

Helpers `Flezha.Observability.subscribe/1` and
`Flezha.Observability.unsubscribe/1` wrap the topic computation.

The sink is gated by the `:pubsub_sink_enabled` knob. In test
config it defaults to `false`; subscribe returns
`{:error, :pubsub_sink_disabled}` when the sink is not running.

## Logger handler bridge

`Flezha.Observability.LoggerHandler` installs an OTP `:logger`
handler (the native handler-module API — *not* the deprecated
`Logger.Backends`). Every log record becomes a `:log` signal under
the `:logger` source, with `name` set to the log level
(`:debug` / `:info` / `:notice` / `:warning` / `:error` /
`:critical` / `:alert` / `:emergency`). Metadata carries
`level`, rendered `message`, `mfa`, `file`, `line`, and preserves
the original `Logger` metadata under `metadata.logger`.

A broken bus must never lose application logs — the handler wraps
each emit in `try`. On failure it increments
`:logger_bridge_failures` (see `Flezha.Observability.Counters`)
and returns normally so the `:logger` pipeline continues.

## Telemetry bridge mapping table

`Flezha.Observability.TelemetryBridge` attaches a single
`:telemetry.attach_many/4` handler. The mapping table inside the
module pairs each known `[:flezha, ...]` event with
`{source_atom, name_atom, kind}`:

| `:telemetry` event                                | source                     | name                  | kind     |
| ------------------------------------------------- | -------------------------- | --------------------- | -------- |
| `[:flezha, :state, :wal, :backpressure]`          | `:flezha_state`            | `:wal_backpressure`   | `:metric` |
| `[:flezha, :state, :fsync, :tick]`                | `:flezha_state`            | `:fsync_tick`         | `:event` |
| `[:flezha, :boot, :recovery_complete]`            | `:flezha_boot`             | `:recovery_complete`  | `:event` |
| `[:flezha, :shutdown, :requested/...]`            | `:flezha_shutdown`         | per phase             | `:event` |
| `[:flezha, :plugin_installer, ...]`               | `:flezha_plugin_installer` | per event             | `:event` |
| `[:flezha, :ws, :idle_reconnect]`                 | `:flezha_ws`               | `:idle_reconnect`     | `:event` |

The producer call-sites are not modified. To add a new bridged
event, append one entry to the `@mapping` map and re-register the
source kinds on next boot.

## Service-API surface

`Flezha.Service.Observability` is the service module callable from
both the CLI (via thin wrappers in `Flezha.Service.CliManager`) and
the future Phoenix `flezha_ui` directly. Public functions:

* `subscribe(source, caller_context)`
* `query(query_params, caller_context)`
* `list_sources(caller_context)` / `list_sinks(caller_context)`
* `enable_source(name, caller_context)` /
  `disable_source(name, caller_context)`

Every call accepts a `caller_context` map of the shape:

```elixir
%{source: :cli | :ui | :sdk | :internal,
  principal: term() | nil,
  request_id: binary() | nil}
```

Missing or invalid `:source` returns `{:error, :invalid_caller_context}`.
When the master flag (`:observability` → `enabled: false`) is set,
every call returns `{:error, :observability_disabled}`.

`subscribe/2`, `enable_source/2`, and `disable_source/2` emit an
audit `:event` signal on the `:observability_service` source
recording the action, the caller `:source`, and the optional
`request_id`. This is the seam that the future `platform-audit-log`
change consumes.

## Configuration knobs

The runtime config map `:observability` (set in
`config/runtime.exs`) accepts:

| Knob                                | Default     | Env var                          |
| ----------------------------------- | ----------- | -------------------------------- |
| `:enabled`                          | `true`      | `FLEZHA_OBSERVABILITY_ENABLED`   |
| `:ring_buffer_retention_ms`         | `900_000`   | —                                |
| `:ring_buffer_max_entries_per_source` | `10_000`  | —                                |
| `:overflow_policy`                  | `:drop`     | —                                |
| `:cardinality_budget_per_source`    | `1_000`     | —                                |
| `:pubsub_sink_enabled`              | `true` / `false` in test | —                   |

`config/test.exs` disables the PubSub sink by default so cross-test
subscribers don't accidentally receive each other's signals.

## Hot-path benchmark

```
mix flezha.bench.observability [iterations]
```

Defaults to `1_000_000`. The task brings up only the observability
supervisor (no Controller, no runners, no transports) so the
measurement isolates `Bus.emit/1`. Default sinks are unregistered
before the bench so it measures the zero-interested-sinks
fast path. The result is informational; the documented threshold
is `< 1_000 ns/emit` on a typical developer box.

## Counters

`Flezha.Observability.Counters` exposes:

* `Counters.get(:dispatcher_failure_count)` — bumped on any bus
  dispatch exception.
* `Counters.get(:logger_bridge_failures)` — bumped when the
  `:logger` handler caught a bus exception.
* `Counters.get(:telemetry_bridge_failures)` — bumped when the
  `:telemetry` bridge caught a bus exception.

`Counters.first_seen?(error_class)` is the dedup mechanism used to
log dispatch errors once per unique error class.
