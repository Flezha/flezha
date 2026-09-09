# Observability

For operators running a controller and for contributors adding sources or sinks. Covers the
`Flezha.Observability` subsystem: the signal envelope, the built-in pollers and bridges, the
default in-memory sinks, the `flz observe` CLI, the Prometheus and OTLP exporters, every
configuration knob, and the contracts for extending it. The durable audit log that consumes
this bus has its own document: [audit_log.md](audit_log.md).

`Flezha.Observability.Supervisor` starts after `Flezha.Service.HubManager` and before
`Flezha.Shutdown.Coordinator`; children in order: `Registry`, `Phoenix.PubSub` (`Flezha.Observability.PubSub`),
`SinkSupervisor`, `SourceSupervisor`, `LoggerHandler`, `TelemetryBridge`, `DefaultSinks`,
`OtpBackend.Supervisor`, `Export.Supervisor`, `AuditLog.Supervisor`. With the master flag off
(`:observability` → `enabled: false`) it starts no children; every service-API call returns `{:error, :observability_disabled}`.

## What the operator gets

### Signal taxonomy

One envelope, `%Flezha.Observability.Signal{kind, source, name, measurements, metadata, ts}`:
`kind` is `:metric | :event | :log | :trace_span`; `source` (registered source name) and `name`
(signal name within the source) are atoms; `measurements` is the numeric map, `metadata` the
categorical label map; `ts` is `System.monotonic_time(:microsecond)`. `Signal.new/1` (map or
keyword list) validates `kind`, rejects non-atom `source`/`name` and non-plain-map
`measurements`/`metadata` (structs and keyword lists raise `ArgumentError`), and fills `ts`.

### Shipped sources

`Flezha.Observability.OtpBackend.Supervisor` (`:one_for_one`) runs five pollers, listed as
`name (kinds, default poll, cardinality budget)`; `:otp_backend_enabled` `false` starts none.

- `:beam_vm` (`[:metric]`, 1_000 ms, 50) — gauges (`value`) `memory_total|processes|atom|binary|ets`
  (bytes), `run_queue_total`, `process_count`, `port_count`; counters (`count`) `gc_count`, `gc_words_reclaimed`;
  gauge `scheduler_utilisation` in `[0.0, 1.0]` from consecutive `:scheduler_wall_time` samples (second poll on).
- `:host` (`[:metric, :event]`, 5_000 ms, 50) — Linux `/proc` reader: gauges `rss_bytes`, `open_fds`,
  `load_avg_1m|5m|15m`, `cpu_utilisation` (second poll on); counters `cpu_user_us`, `cpu_system_us`
  (jiffies × 10_000). An unreadable `/proc/self/stat` yields one `:host_source_degraded` event
  (`metadata.reason: :proc_self_stat_unreadable`), then no-ops.
- `:processhub` (`[:metric]`, 1_000 ms, 200) — per running namespace hub, metadata `%{hub: hub}`:
  `registered_processes` (`ProcessHub.process_list(hub, :local)` length) and `pending_recoveries`
  (`1` when `ProcessHub.Service.Recovery.recovery_state/1` is `:recovering` or `:recovery_pending`).
- `:flezha_transport` (`[:metric]`, 1_000 ms, 40) — per active transport
  (`Flezha.Transport.Server.list_active_transports/0` + `get_stats/1`), metadata
  `%{transport: :uds | :quic}`: gauge `open_connections` (pending streams). With
  `:data_plane_liveness` on (default): the driver self-report, from the UDS and the QUIC driver
  alike (counter `driver_loop_iterations`; gauges `driver_loop_age_ms`, `driver_cmd_queue_depth`,
  `driver_write_buffer_bytes`, `driver_streams`) and BEAM aggregates under `%{scope: :data_plane}` — counters
  `channel_io_in_received`, `channel_send_frame_calls`, `channel_nil_drops`,
  `channel_flz_data_received`, `operative_io_in_received`, `operative_forwarded`,
  `operative_filtered_nil`, `operative_parked_drops` (ingress casts a parked operative dropped —
  the confirming entry point refuses instead, so this counts only what a cast-based source lost),
  `credit_active_sessions`, `credit_overrun_total`, `credit_parked`,
  `mailbox_blocked`, `mailbox_drops_newest`, `mailbox_drops_oldest`, `mailbox_pending_pushes`
  (the runner's overflow self-report: producers held on full entity mailboxes, Entities
  withheld work because a consumer will not take their output, and what a shedding operative
  discarded — the only handle on that loss from outside the runner),
  `restore_discarded_inputs` (inputs a rebind threw away, correct only where the controller
  re-forwards from a durable log, so a climbing value sizes that loss), `timers_armed` and
  `timers_fired` (the wake list's occupancy and its firings: armed climbing while fired stays
  flat is promotion wedged rather than an idle Operative), `heartbeat_frames` and `dispatches`
  (heartbeats arriving while dispatches stand still is a wedged pool with a live runner);
  gauges `credit_ledger_level`, `inflight`, `inflight_oldest_ms` and `pending_push_oldest_ms`
  (activations underway and the age of the oldest, and the age of the oldest push a full
  consumer is withholding — what makes ONE wedged Entity visible while the runner as a whole
  keeps completing work). No bytes in/out.

  **This list is derived, not maintained.** Every metric here is a row of
  `Flezha.Transport.LivenessCounters` whose `surface:` names it, and the source emits one
  signal per such row; the authoritative list is the rows themselves. A counter the runner
  reports in its heartbeat is counted once per runner however many secondary streams mirror
  it, and then reduced across runners the way its row declares — counts add up, an age gauge
  takes the larger. A per-channel counter sums across channels. Reading them:
  [../internals/transport_liveness.md](../internals/transport_liveness.md).
  **The self-report needs a NIF driver.** The `:native` in-node transport has none, so a
  native-only controller emits no `driver_*` metric — including `driver_write_buffer_dropped`, the
  send-buffer loss counter — and its data-plane evidence is the BEAM aggregates alone.
- `:flezha_bounded_mailbox` (`[:metric, :event]`, 1_000 ms, 5_000) — registered so it shows in
  `flz observe sources`; emits one `:bounded_mailbox_unavailable` event (`metadata.reason:
  :nif_surface_pending`) on its first poll and nothing else — its poll body (`poll_mailboxes/0`)
  is a stub awaiting a NIF surface.

Also present at boot: `:observability_registry` and `:observability_service` (events, pre-registered by
the Registry), `:observability_export` (exporter events, budget 200), the telemetry-bridged sources under
[Telemetry bridge](#telemetry-bridge), and `:logger` — never registered, so it rides the
unregistered-source path (no cardinality check, not disable-able).

### Default sinks

`DefaultSinks` registers two sinks (filter `:any`/`:any`, overflow `:drop`, watermark 10_000):

- `:ring_buffer` (`Flezha.Observability.RingBufferSink`) — one `:protected` ETS `:ordered_set`
  per source, entries `{{ts, seq}, signal}`; drop-oldest above `:ring_buffer_max_entries_per_source`
  (10_000) and a sweep every `:retention_check_interval_ms` (5_000) deleting entries older than
  `:ring_buffer_retention_ms` (900_000). `query(source, from_ts, limit: n)` and `count/1` read ETS
  in the caller; `from_ts: 0` means no lower bound. Tables follow Registry events.
- `:pubsub` (`Flezha.Observability.PubSubSink`) — broadcasts every accepted signal on
  `Flezha.Observability.PubSub`, topic `flezha.observability.source.<source>`, as `{:flezha_signal, %Signal{}}`;
  `Flezha.Observability.subscribe/1` / `unsubscribe/1` wrap the topic and return
  `{:error, :pubsub_sink_disabled}` when the sink is not running (`:pubsub_sink_enabled`, off in test).

### `flz observe` CLI

Every verb rides the sibling `flzvm rpc` into `Flezha.Service.CliManager.observability_render_*` with a
`caller_context` of `source: :cli`, so the service API's authz gates apply. `--json`,
`--output json`, `--output=json`, `-o json` and `-o=json` are one request read by one
reader; every verb fails with `observability is disabled` under the master flag.
A failure is written to stderr, whatever the verb, so a run piped into `jq` or a
file collects the answer and never the error.

| Verb                                                   | Effect                                                                                   |
| ------------------------------------------------------ | ---------------------------------------------------------------------------------------- |
| `flz observe sources [--json]`                         | one row per registered source: name, status, kinds, cardinality budget                   |
| `flz observe sinks [--json]`                           | one row per registered sink: name, module, overflow, watermark                           |
| `flz observe tail <source> [--json] [--duration-ms N]` | streams live signals through the PubSub sink for N ms (default 30000); `--json` is NDJSON; fails with `pubsub sink is disabled` when `:pubsub_sink_enabled` is false |
| `flz observe enable <source>`                          | sets the source `:active`                                                                |
| `flz observe disable <source>`                         | sets the source `:disabled`; the bus drops its emits                                     |

## Exporting

Both exporters are off by default and run under `Flezha.Observability.Export.Supervisor`
(`:one_for_one`, every child `:permanent`), which registers the `:observability_export` source.

### Prometheus

`:prometheus_enabled` starts `PrometheusRegistry`, `PrometheusSink`, `PrometheusHttp`. The sink
(`:prometheus`) takes `:metric` only, overflow `:drop`, watermark 50_000 — the next scrape recovers a
missed sample. The endpoint is `Flezha.Http.Listener` (`:gen_tcp`, HTTP/1.1, no Cowboy or Plug) on
`:prometheus_bind` (`127.0.0.1`) : `:prometheus_port` (`9568`): `GET /metrics` returns
`text/plain; version=0.0.4; charset=utf-8`, `GET /` a one-link HTML page; other paths `404`, other
methods `405`, malformed requests `400`; idle connections close after 5 s. There is no
authentication — keep it on loopback or front it with a reverse proxy.

Series names are `<source>_<name>` (`beam_vm_memory_total`, `flezha_dlq_dlq_parked_total`). Labels are
the metadata keys and values, sorted; `prometheus_type` and `buckets` never become labels.
The type is inferred from the measurement keys (first match wins); `metadata.prometheus_type` overrides.

**Labels are made well-formed at record time, whatever a producer emitted.** A metadata map holds
arbitrary Elixir terms — `TelemetryBridge` forwards a third-party `:telemetry` event's metadata
verbatim — and a malformed exposition is worse than a missing one: Prometheus fails the *whole
scrape*, so one bad key blinds every metric the controller has.

*Values* take `Flezha.Observability.Signal.metadata_text/1`: a UTF-8 binary passes through, an atom
(`nil` reads `"nil"`), integer or float takes its `String.Chars` rendering, and everything else — a
map, a tuple, a pid, a list, a binary that is not valid UTF-8 — is inspected in full. So a charlist
value reads `~c"lib/x.ex"` rather than `lib/x.ex`: at the source you know a charlist is text, at the
exporter you do not.

The audit log's JSON lines and `flz observe tail --json` take the companion rule,
`Signal.metadata_json/1`, which keeps what JSON can hold typed — a null, an array, an object — and
asks `metadata_text/1` for everything it cannot, **binaries included**. JSON has no bytes type, so a
binary that is not valid UTF-8 is inspected rather than carried: that decision is written once, and
the two renderers cannot answer differently for the same term.

*Names* are then sanitized into Prometheus's `[a-zA-Z_][a-zA-Z0-9_]*`:

- every character outside `[a-zA-Z0-9_]` becomes `_`, so `http.status_code` becomes
  `http_status_code` — the published OpenTelemetry-to-Prometheus mapping;
- a name that would start with a digit gains a leading `_`, and an empty name becomes `_`;
- a **leading run of `_` collapses to one**, so no producer label can enter Prometheus's reserved
  `__` namespace or forge the registry's own `__truncated__` marker;
- on a **histogram** a resulting name of `le` is dropped, because the renderer appends its own
  `le` to every bucket line and a duplicate label name invalidates the exposition. On a counter or
  gauge `le` is legal and is kept;
- if two keys in one signal sanitize to the same name, the pairs are sorted and the first wins;
  the rest are dropped rather than emitted as a duplicate label.

**Sanitizing can merge two series.** Two raw label sets that sanitize alike become one ETS key:
counters add, gauges last-write-win, histograms merge. This is deliberate — sanitizing at render
time instead would emit two rows with identical series lines, which Prometheus rejects as hard as a
duplicate label — but it does mean a metric's cardinality is budgeted on the names an operator
actually sees.

Every name rewritten or dropped increments a per-source counter, readable with
`PrometheusRegistry.coercion_count/1` beside the existing `truncation_count/1`. There is no event
per coercion: the rewrite is self-evident in the scrape, because the sanitized name is what the
operator reads.

| Measurements contain                 | Type      | Stored as                             |
| ------------------------------------ | --------- | ------------------------------------- |
| `:duration_us`, `:bytes`, `:buckets` | histogram | sample into `_bucket`/`_sum`/`_count` |
| `:count`                             | counter   | running sum of `count` deltas         |
| `:gauge`, `:value`, anything else    | gauge     | last value                            |

Default buckets — duration (µs): `5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 500_000,
1_000_000, 2_500_000, 5_000_000`; bytes: `1024, 4096, 16_384, 65_536, 262_144, 1_048_576,
4_194_304, 16_777_216`; `measurements.buckets` overrides (`value` is then the sample).
Cardinality: per `{source, name}` the registry tracks distinct label-value sets against the
source's declared budget (1_000 for an unknown source); an emit that would exceed it is folded
into a synthetic `{__truncated__="true"}` series, `PrometheusRegistry.truncation_count/1`
increments, and a `:prometheus_label_truncated` event (`metadata: %{source, metric}`) is
emitted. Every scrape emits `:prometheus_scrape` (`%{duration_us, series_count}`).

### OTLP/HTTP

`:otlp_enabled` with an `:otlp_endpoint` starts `OtlpClient` and `OtlpSink`. The sink (`:otlp`) takes
every kind, overflow **`:block`**, watermark 10_000, `block_timeout_ms` 30_000, and ignores signals from
`:observability_export` so it cannot feed back on its own events. It flushes at `:otlp_batch_max` (512)
signals or every `:otlp_flush_interval_ms` (10_000), whichever first.

`OtlpCodec` hand-encodes the OTLP v1 protobuf bodies (`Content-Type: application/x-protobuf`): `:metric`
→ `ExportMetricsServiceRequest` (Sum or Gauge); `:log` and `:event` → `ExportLogsServiceRequest` (events
at severity INFO); `:trace_span` → `ExportTraceServiceRequest` with random 16-byte trace and 8-byte span
ids (no propagation). The resource carries `service.name=flezha` and `service.version`; timestamps are
Unix nanoseconds (`(ts + System.time_offset(:microsecond)) * 1_000`). The client POSTs to the endpoint
plus `/v1/metrics`, `/v1/logs`, `/v1/traces` (a suffix already present is not doubled), so
`:otlp_endpoint` is the collector **base** URL.

Attribute keys and values cross as OTLP strings, and a signal's metadata holds arbitrary terms — the
Logger bridge attaches a map (`logger:`) and an `{m, f, a}` tuple to every log event. Every term is
representable: a binary is used unchanged, an atom, integer or float takes its `String.Chars` rendering,
and anything else — a map, a tuple, a pid, **a list** — is inspected in full (`limit: :infinity`, so
nothing is elided into `...`). So an attribute may reach the collector as an Elixir rendering
(`%{line: 412}`, `[1, 2, 3]`) rather than a JSON or typed one; encoding never raises on an attribute's
shape, and never silently re-encodes one. Note that a charlist reads as `~c"..."`, not as its text: the
codec cannot tell a charlist meant as text from a byte list meant as data, and saying which it is beats
guessing. `service.version` is the exception, converted at its source because it is known to be one.

Retry: up to 5 attempts per POST with backoff `250, 500, 1_000, 2_000, 4_000` ms on `5xx`, `:closed`,
`:timeout` or `:econnrefused`; any other error gives up at once; per-request timeout 10 s. Success
emits `:otlp_batch_flushed` (`%{batch_size, duration_us}`); exhaustion discards the batch and emits
`:otlp_batch_dropped` (`%{batch_size, attempts}`, `metadata.failures`). Under `:block` a producer waits
in `GenServer.call` for at most the sink's `block_timeout_ms` (30 s) per emit; past that the emit is
abandoned and `:dispatcher_failure_count` increments — an unreachable collector slows producers, it
cannot wedge them. `:otlp_headers` is a map injected into every request (typically
`%{"authorization" => "Bearer <token>"}`); `https` endpoints use `:mint` with its default peer verification.

### Running a Grafana / OpenTelemetry Collector stack

```bash
FLEZHA_PROMETHEUS_ENABLED=true FLEZHA_PROMETHEUS_BIND=0.0.0.0   # collector scrapes http://HOST:9568/metrics
FLEZHA_OTLP_ENABLED=true FLEZHA_OTLP_ENDPOINT=http://collector:4318   # base URL, no /v1/... suffix
```

Give the collector a `prometheus` receiver on `HOST:9568` and/or an `otlp` receiver on `4318`; Grafana reads its exporters.

## Configuration knobs

Environment booleans accept `1`, `true`, `yes`, `on` (case-insensitive). `config/runtime.exs` sets
the OTP-backend keys and the `:observability_export` block only outside `:test`, where
`config/test.exs` pins the PubSub sink, the pollers and both exporters off.

| Config / key                                              | Default                                                                                   | Env var                        |
| --------------------------------------------------------- | ----------------------------------------------------------------------------------------- | ------------------------------ |
| `:observability` / `:enabled`                             | `true`                                                                                    | `FLEZHA_OBSERVABILITY_ENABLED` |
| `:observability` / `:ring_buffer_retention_ms`            | `900_000`                                                                                 | —                              |
| `:observability` / `:ring_buffer_max_entries_per_source`  | `10_000`                                                                                  | —                              |
| `:observability` / `:retention_check_interval_ms`         | `5_000` (code default; not set in `runtime.exs`)                                          | —                              |
| `:observability` / `:cardinality_budget_per_source`       | `1_000` — budget for a source registering without one                                     | —                              |
| `:observability` / `:overflow_policy`                     | `:drop` — set in `runtime.exs` but read by no module; each sink's own `:overflow` applies | —                              |
| `:observability` / `:pubsub_sink_enabled`                 | `true` (`false` in test)                                                                  | —                              |
| `:observability` / `:data_plane_liveness`                 | `true`                                                                                    | `FLEZHA_DATA_PLANE_LIVENESS`   |
| `:observability` / `:otp_backend_enabled`                 | `true` (`false` in test)                                                                  | —                              |
| `:observability` / `:otp_backend` → `:poll_intervals`     | `%{beam_vm: 1_000, host: 5_000, processhub: 1_000, flezha_transport: 1_000, flezha_bounded_mailbox: 1_000}` | —            |
| `:observability_export` / `:prometheus_enabled`           | `false`                                                                                   | `FLEZHA_PROMETHEUS_ENABLED`    |
| `:observability_export` / `:prometheus_port`              | `9568`                                                                                    | `FLEZHA_PROMETHEUS_PORT`       |
| `:observability_export` / `:prometheus_bind`              | `"127.0.0.1"`                                                                             | `FLEZHA_PROMETHEUS_BIND`       |
| `:observability_export` / `:otlp_enabled`                 | `false`                                                                                   | `FLEZHA_OTLP_ENABLED`          |
| `:observability_export` / `:otlp_endpoint`                | `nil`                                                                                     | `FLEZHA_OTLP_ENDPOINT`         |
| `:observability_export` / `:otlp_headers`                 | `%{}`                                                                                     | —                              |
| `:observability_export` / `:otlp_flush_interval_ms`       | `10_000`                                                                                  | —                              |
| `:observability_export` / `:otlp_batch_max`               | `512`                                                                                     | —                              |

## Extending

### Source contract

A source is **declarative** (application code that emits) or **process-backed** (a GenServer
emitting on its own schedule). Both register through `Flezha.Observability.Registry.register_source/1`
with `name` (atom), `kinds` (subset of the four), `flavour` (`:declarative | :process_backed`) and
an optional `cardinality_budget`, then emit through `Flezha.Observability.Bus.emit/1`:

```elixir
:ok = Registry.register_source(%{name: :my_source, kinds: [:metric], flavour: :declarative})
Bus.emit(Signal.new(%{kind: :metric, source: :my_source, name: :requests, measurements: %{count: 1}}))
```

Registration is what makes a source visible, disable-able and budgeted — an unregistered source
still fans out, unchecked. A `kind` outside the registered `kinds` is dropped and reported as a
`:source_kind_violation` event on `:observability_registry` (`metadata: %{source, attempted_kind}`).

The shipped pollers are GenServers built with `use Flezha.Observability.OtpBackend.Poller,
source: :name, default_poll_ms: n` (injects `start_link/1` and `poll_interval_ms/0`
honouring `:otp_backend` → `:poll_intervals`); each registers itself in `init/1` and schedules its own
`:poll`. The built-in list is `@sources` in `OtpBackend.Supervisor`; a third-party poller runs under its
own supervisor or via `DynamicSupervisor.start_child/2` on `Flezha.Observability.SourceSupervisor`.
The macro also declares `@behaviour Flezha.Observability.Source` and supplies an overridable
`config_schema/0`, so every shipped poller implements the registration contract. That behaviour
declares `config_schema/0` and nothing else: no foundation driver calls a source back — a poller
drives itself, and a process-backed source's `init/1` is GenServer's.

A process-backed source in full — it registers itself, arms its own tick, and emits from it:

```elixir
defmodule MyPolledSource do
  use GenServer

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @impl true
  def init(_opts) do
    Registry.register_source(%{name: :mine, kinds: [:metric], flavour: :process_backed})
    Process.send_after(self(), :poll, 1_000)
    {:ok, %{count: 0}}
  end

  @impl true
  def handle_info(:poll, state) do
    Bus.emit(Signal.new(%{kind: :metric, source: :mine, name: :ticks}))
    Process.send_after(self(), :poll, 1_000)
    {:noreply, %{state | count: state.count + 1}}
  end
end
```

### Sink contract

A sink implements `Flezha.Observability.Sink` — `config_schema/0` and `filter/0`
returning `%{kinds: :any | [kind], sources: :any | [atom]}` — and is registered with
`Registry.register_sink(%{name, module, config, overflow, mailbox_high_watermark, block_timeout_ms})`,
which checks that the module is loaded and exports `config_schema/0` and `filter/0`, that every schema
key is present in `config` (`{:error, {:missing_config_keys, keys}}` otherwise), then starts
`module.start_link(%{name: name, config: config})` under `SinkSupervisor` (`:one_for_one`, restart
`:transient`) and returns `{:ok, pid}`. Defaults: `overflow: :drop`, `mailbox_high_watermark: 10_000`, `block_timeout_ms: 5_000`.

Delivery is a raw message, not a callback: under `:drop` the bus sends `{:signal, signal}`
(handle it in `handle_info/2`); under `:block` it issues
`GenServer.call(pid, {:signal, signal}, block_timeout_ms)` (handle it in `handle_call/3`).
`Registry.register_external_sink/1` inserts a row for a sink whose process another supervisor
owns (pass `pid`; this is how the exporters register); `unregister_sink/1` terminates a
`SinkSupervisor` child and removes the row. `Registry.sink_counter(name, :dropped | :crashes)`
reads the per-sink counters; `:dropped` increments on every watermark drop.

`filter/0` is consulted before the bus sends, so a sink never sees a signal it did not ask for.
`:kinds` is `:any` or a list of `Flezha.Observability.Signal.kinds/0`; `:sources` is `:any` or a
list of source atoms — `%{kinds: [:metric, :event], sources: :any}`, `%{kinds: [:log], sources: [:logger]}`.

A sink in full:

```elixir
defmodule MyCounterSink do
  use GenServer
  @behaviour Flezha.Observability.Sink

  @impl Flezha.Observability.Sink
  def config_schema, do: %{}

  @impl Flezha.Observability.Sink
  def filter, do: %{kinds: :any, sources: :any}

  @impl GenServer
  def init(_config), do: {:ok, %{count: 0}}

  @impl GenServer
  def handle_info({:signal, _signal}, %{count: c} = state) do
    {:noreply, %{state | count: c + 1}}
  end
end
```

### Registry

`Flezha.Observability.Registry` owns two `:protected` `:set` ETS tables,
`:flezha_observability_sources` and `:flezha_observability_sinks`. Reads (`list_sources/0`,
`list_sinks/0`, `lookup_source/1`, `lookup_sink/1`, counters) hit ETS directly; writes go through
the GenServer and are broadcast on PubSub topic `flezha.observability.registry` as
`{:flezha_observability_registry, {:source_registered, name} | {:source_unregistered, name} |
{:source_status, name, status}}` (`subscribe_registry_events/0` subscribes the caller).
`enable_source/1` / `disable_source/1` flip the status (`{:error, :not_found}` when unknown).

Cardinality budget: per source the Registry keeps the set of *metadata-key shapes* (`:erlang.phash2/1`
of the sorted key list — keys, not values). On every `:metric` emit the bus calls `check_cardinality/2`:
a known shape is `:ok` from ETS; a new shape under budget is added through the GenServer; a new shape
at or over budget returns `{:dropped, :cardinality_budget_exceeded}`, bumps
`source_counter(name, :budget_exceeded)` and, once per source lifetime, emits a
`:cardinality_budget_exceeded` event on `:observability_registry` carrying only `%{source: name}`.
`:event`, `:log` and `:trace_span` are never budget-checked.

### Bus overflow policy

`Flezha.Observability.Bus.emit/1` runs in the caller, always returns `:ok` and never raises. It drops
a `:disabled` source, fans an unregistered source out unchecked, drops a `kind` outside the source's
`kinds` (emitting `:source_kind_violation`), runs `check_cardinality/2` for `:metric`, then selects
every sink whose `filter` accepts the signal (one scan of the sinks table) and delivers per sink:

- `:drop` — if `Process.info(pid, :message_queue_len)` is at or above `mailbox_high_watermark`,
  bump the sink's `:dropped` counter and skip; else send `{:signal, signal}`.
- `:block` — `GenServer.call(pid, {:signal, signal}, block_timeout_ms)`; a sink emitting a signal
  of its own gets a plain send so it cannot deadlock on itself.

The body is wrapped in `try/rescue/catch`: any failure bumps `:dispatcher_failure_count` and logs
once per error class; a sink exiting with `:noproc`, `:normal` or `:shutdown` is counted, not logged.

### Logger handler bridge

`Flezha.Observability.LoggerHandler` installs a native OTP `:logger` handler (id
`:flezha_observability`, level `:all`; not `Logger.Backends`). Each record becomes a `:log` signal
on `:logger` with `name` = level, `measurements: %{count: 1}` and metadata `level`, rendered
`message`, `mfa`, `file`, `line`, plus the original metadata under `logger`. A failing emit bumps
`:logger_bridge_failures` and returns normally — the `:logger` pipeline never loses a record.

### Telemetry bridge

`Flezha.Observability.TelemetryBridge` attaches one `:telemetry.attach_many/4` handler
(`:flezha_observability_telemetry_bridge`) over the keys of its `@mapping` and registers every mapped
source (declarative, default budget) on start; measurements and metadata pass through unchanged. Adding
a bridged event is one `@mapping` entry. Events are under `[:flezha, ...]`; the signal name is the last
event segment unless shown as `→ name`.

A **provider**'s events are not written by hand: the prefix is derived from the provider's own
identifier and the verb comes from one declared vocabulary — `shipped`, `retried`, `failed`,
`overflowed` — so `[:flezha, :plugin, <provider>, <verb>]` reads the same for every provider
(`Flezha.Plugin.ProviderBehaviour.emit/3`). Facts that belong to one provider alone — a broker
metadata refresh, a topic creation, a websocket re-subscribe — keep their own verb under the same
prefix. The websocket provider's old `[:flezha, :ws, …]` root is gone; its events now sit with
every other provider's, and the source atom `:flezha_ws` is unchanged so the exposed series are
too.

An event the tree writes and the bridge does not map reaches no exporter at all, so
`TelemetryBridge.unexported/0` names every one, grouped under the reason it has none:
`commit_path` (one signal per durable workflow commit, at the data plane's rate),
`counted_elsewhere` (the same fact is already an exported `LivenessCounters` slot:
`[:flezha, :sdk, :mailbox]` is `hb_drops_newest`/`hb_drops_oldest` and
`[:flezha, :sdk, :timers]` is `hb_timers_armed`/`hb_timers_fired`), `control_input`
(the convergence loop attaches to it itself, so it is a control input rather than a sink) and
`lifecycle` (an action the controller took, never a fault it detected, and already answered by
an operator surface that reports the resulting state). Every reason is a PROPERTY of the event:
a reason that named the change owing it a sink expired the moment that change landed, which is
how the previous list came to state nothing while still passing. The suite pins the total, so an
event can leave the list and none may join. It judges the event NAME wherever the tree writes it
rather than only at the `:telemetry.execute` call, because three names are assembled into a term
in one module and executed in another. `unwritten/0` is the other direction: mapped events
nothing writes any more, marked `(dead: nothing emits it)` in the table below, each kept for the
reason `unwritten/0` records.

| Source | Events (kind) |
| --- | --- |
| `:flezha_boot` | event: `[:boot, :pre_recovery_replay]`; `[:boot, :reconcile_duplicate]`; `[:boot, :recovery_complete]`; `[:hub, :start_failed]` → `:hub_start_failed`. |
| `:flezha_convergence` | metric: `[:backoff, :reset]` → `:backoff_reset`; `[:backoff, :wait]` → `:backoff_wait`. event: `[:convergence, :intent_mismatch]`. |
| `:flezha_correction` | event: `[:correction, :refused]`. |
| `:flezha_dlq` | metric: `[:dlq, :parked]` → `:dlq_parked_total`; `[:dlq, :retention_dropped]` → `:dlq_dropped_by_retention_total`; `[:dlq, :usage]` → `:dlq_usage`. |
| `:flezha_durability` | metric: `[:state, :artifact_shipped]`; `[:state, :durability_gc]`. event: `[:state, :artifact_missing]`; `[:state, :claim_superseded]`; `[:state, :hydrated]`; `[:state, :hydration_failed]`; `[:state, :rpo_exceeded]`; `[:state, :ship_failed]`; `[:state, :ship_fenced]` (dead: nothing emits it). |
| `:flezha_health` | event: `[:convergence, :heal]`; `[:convergence, :operative_stuck]`; `[:convergence, :safe_call]`; `[:data_plane, :stall_localized]`; `[:health, :verdict]`; `[:hub, :unavailable]`; `[:plugin, :health, :fail_open]`; `[:runner, :evicted]`; `[:runner, :liveness_timeout]`; `[:runner, :reconnected]`; `[:runner, :reconnected_reassigned]`; `[:runner, :restarted]`; `[:sdk, :parked]`; `[:state, :input_silence]`; `[:transport, :connect_rejected]`; `[:workflow, :failed]`; `[:workflow, :parked]`; `[:workflow, :suspended]`. |
| `:flezha_http_poll` | metric: `[:plugin, :http_poll, :shipped]` → `:injected`; `[:plugin, :http_poll, :unchanged]`. event: `[:plugin, :http_poll, :failed]`. |
| `:flezha_kafka` | metric: `[:plugin, :kafka, :metadata_refresh]`; `[:plugin, :kafka, :overflowed]`; `[:plugin, :kafka, :retried]`; `[:plugin, :kafka, :shipped]`. event: `[:plugin, :kafka, :failed]`; `[:plugin, :kafka, :topic_created]`; `[:plugin, :kafka, :worker_swapped]`. |
| `:flezha_leadership` | event: `[:leadership, :transition]` (dead: nothing emits it). |
| `:flezha_operative` | metric: `[:operative, :parked]` → `:operative_parked_total`. Metadata `namespace`, `operative_id`, `reason`, and `plugin_id` — the plugin that caused the park, or `nil` when no plugin did. |
| `:flezha_operative_forward` | metric: `[:plugin, :operative_forward, :queue_depth]`; `[:plugin, :operative_forward, :retried]` → `:retry`; `[:plugin, :operative_forward, :shipped]` → `:delivered`. event: `[:plugin, :operative_forward, :overflowed]` → `:saturated`. |
| `:flezha_plugin` | event: `[:plugin, :phase]`. |
| `:flezha_plugin_installer` | event: `[:plugin_installer, :load_failed]`; `[:plugin_installer, :store_opened]`; `[:plugin_installer, :unsigned_install]`. |
| `:flezha_reconcile` | event: `[:reconcile, :action]` (dead: nothing emits it); `[:reconcile, :health]` (dead: nothing emits it). |
| `:flezha_runner` | event: `[:runner, :factory_language_conflict]`; `[:runner, :quota_refused]`. |
| `:flezha_schedule` | event: `[:schedule, :spawn_failed]`. |
| `:flezha_sdk` | event: `[:sdk, :dynamic]`; `[:sdk, :retry]`. |
| `:flezha_shutdown` | event: `[:shutdown, :complete]`; `[:shutdown, :drain_progress]`; `[:shutdown, :phase_completed]`; `[:shutdown, :phase_started]`; `[:shutdown, :requested]`; `[:shutdown, :timeout]`. |
| `:flezha_state` | metric: `[:state, :wal, :backpressure]` → `:wal_backpressure`. event: `[:state, :baseline_seed]`; `[:state, :compaction_failed]`; `[:state, :compaction_gave_up]`; `[:state, :compaction_timeout]`; `[:state, :compaction_trigger]`; `[:state, :credit_anomaly]`; `[:state, :disk_quota_breach]`; `[:state, :fsync, :tick]` → `:fsync_tick`; `[:state, :inbox_duplicate]`; `[:state, :input_log_frozen_watermark]`; `[:state, :input_log_syncer_died]`; `[:state, :output_stall]`; `[:state, :record_frame, :refused]` → `:record_frame_refused`; `[:state, :record_frame, :repaired]` → `:record_frame_repaired`; `[:state, :restore_refused]`; `[:state, :seq_gap]`; `[:state, :stale_writer_refused]`; `[:state, :wal_pull_failed]`; `[:state, :wal_realign]`; `[:state, :wal_rewind]`. |
| `:flezha_stream` | metric: `[:stream, :drop]`. event: `[:stream, :fanout_warning]`. |
| `:flezha_ws` | metric: `[:plugin, :websocket, :shipped]`. event: `[:plugin, :websocket, :failed]`; `[:plugin, :websocket, :idle_reconnect]`; `[:plugin, :websocket, :overflowed]`; `[:plugin, :websocket, :reconcile]`; `[:plugin, :websocket, :resubscribe]`; `[:plugin, :websocket, :retried]`. |

### Service API

`Flezha.Service.Observability` is the operator-facing surface behind the `Flezha.Service.CliManager`
`observability_*` wrappers: `subscribe/2`, `tail/2`, `query/2`, `list_sources/1`, `list_sinks/1`,
`enable_source/2`, `disable_source/2`. Every call takes a `caller_context` last — `%{source: :cli | :ui |
:sdk | :internal, principal: term() | nil, request_id: binary() | nil, namespace: binary() | nil}`; a
missing or invalid `:source` returns `{:error, :invalid_caller_context}`. Each call then passes
`Flezha.AuthZ.require_bound_principal/1` (`:internal` always passes; otherwise a non-empty `:principal`
is required while authn/authz is enabled) and `Flezha.AuthZ.authorize/3` on the caller's bound
namespace (`_system` when unbound): `observability.query` and `observability.subscribe` are viewer
actions, `observability.enable_source` and `observability.disable_source` operator actions. `query/2`
takes `%{source, from_ts?, limit?}` and filters results to the caller's namespace (signals without
`metadata.namespace` pass) unless the caller is `admin@_system`, `:internal`, or authz is off; `tail/2`
also requires the PubSub sink. `subscribe`, `tail`, `enable_source` and `disable_source` each emit an
audit `:event` on `:observability_service` named after the action, with metadata `action`,
`caller_source`, `source` and, when supplied, `request_id` — the seam the audit log's allow-list consumes.

### Counters

`Flezha.Observability.Counters` (lock-free `:counters`) exposes `get/1` for `:dispatcher_failure_count`,
`:logger_bridge_failures` and `:telemetry_bridge_failures`; `first_seen?/1` is the once-per-error-class
log dedup. `mix flezha.bench.observability [iterations]` measures `Bus.emit/1` against zero sinks — see
[benchmarking.md](benchmarking.md).

## See also

- [audit_log.md](audit_log.md) — the durable sink on this bus and its default allow-list
- [authn_authz.md](authn_authz.md) — roles behind the `observability.*` actions
- [../configuration.md](../configuration.md) — the full env-var and application-env index
