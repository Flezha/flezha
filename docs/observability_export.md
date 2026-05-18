# Observability export

Built-in Sinks that ship observability signals out of the BEAM:

- **`Flezha.Observability.Export.PrometheusSink`** — aggregates
  `:metric` signals into in-memory counter / gauge / histogram
  registers and serves the Prometheus text exposition format on a
  configurable HTTP port.
- **`Flezha.Observability.Export.OtlpSink`** — batches `:metric`,
  `:event`, `:log`, and `:trace_span` signals and POSTs them over
  OTLP/HTTP (protobuf body) to a configured collector endpoint.

Both exporters are OFF by default and require explicit config opt-in.
They run under `Flezha.Observability.Export.Supervisor` as a child
of `Flezha.Observability.Supervisor`, started AFTER `OtpBackend.Supervisor`.

## Prometheus

### Wire format

Plain text per the Prometheus exposition format (`text/plain;
version=0.0.4`). The aggregation register infers the Prometheus type
from `Signal.measurements` keys:

| Measurements                                   | Prometheus type |
| ---------------------------------------------- | --------------- |
| `%{count: integer}`                            | counter         |
| `%{value: number}` / `%{gauge: number}`        | gauge           |
| `%{duration_us: int}` / `%{bytes: int}`        | histogram       |
| `%{buckets: [...], values: [...]}`             | histogram       |

Producers can override the inferred type by setting
`metadata.prometheus_type` to `:counter | :gauge | :histogram`.

Histograms use these default buckets:

- Duration (microseconds): `5_000, 10_000, 25_000, 50_000, 100_000,
  250_000, 500_000, 1_000_000, 2_500_000, 5_000_000`.
- Bytes: `1024, 4096, 16384, 65536, 262_144, 1_048_576, 4_194_304,
  16_777_216`.

A producer can override via `metadata.buckets`.

### Cardinality enforcement

The Prometheus sink tracks distinct label-value combinations
per-`{source, metric_name}`. When new labels would push the metric
over the source's declared `cardinality_budget`, the offending emit
is aggregated into a synthetic `{__truncated__="true"}` series and
the source's `:prometheus_label_truncations` counter is incremented.
Truncations also emit a `:prometheus_label_truncated` event on the
`:observability_export` source so they are observable.

### HTTP endpoint

The hand-rolled `:gen_tcp` listener accepts exactly two request
shapes:

- `GET /metrics` — returns the exposition body.
- `GET /` — returns a small HTML link page.

Everything else returns `404`. `POST /metrics` returns `405`.
Malformed requests return `400`. Per-connection idle timeout default
is `5_000` ms.

The listener binds to `127.0.0.1` by default. Set
`:prometheus_bind` to expose on a non-loopback interface. The
endpoint does not enforce authentication — front it with a reverse
proxy (nginx, envoy, ALB) or rely on network-level controls if you
expose it outside `127.0.0.1`.

### Operational events

- `:prometheus_scrape` — emitted on every successful `GET /metrics`,
  carrying `measurements: %{duration_us, series_count}`.
- `:prometheus_label_truncated` — emitted when the cardinality cap
  forces a truncation.

## OTLP

### Wire format

OTLP/HTTP with `Content-Type: application/x-protobuf`. The encoder
hand-builds the message bodies for the OTLP service requests:

- `metrics` → `ExportMetricsServiceRequest` (Sum or Gauge per signal).
- `logs` → `ExportLogsServiceRequest` (severity `INFO`; `:event`
  signals are routed here too).
- `traces` → `ExportTraceServiceRequest` (each `:trace_span` is one
  internal Span; `trace_id` / `span_id` are 16-byte / 8-byte randoms
  until cross-process propagation lands).

Timestamps are absolute Unix nanoseconds derived from the signal's
monotonic `ts` field plus the BEAM's wall-time offset.

### Batching + back-pressure

The sink accumulates signals until **`:otlp_batch_max`** (default
`512`) or **`:otlp_flush_interval_ms`** (default `10_000`) — whichever
fires first. The overflow policy is **`:block`**: a producer emitting
into a full sink mailbox waits on `GenServer.call` until the sink
accepts. This is the right default for an audit-grade exporter
where a dropped batch is a permanent gap.

The HTTP POST itself applies bounded exponential backoff: 250 ms,
500, 1_000, 2_000, 4_000 (cap), max 5 attempts. After 5 failures the
batch is dropped, the per-sink `:dropped` counter increments, and
an `:otlp_batch_dropped` event is emitted with `batch_size` and
`attempts`. **Producer back-pressure is bounded** by
`mailbox_watermark × sum-of-backoffs` — an unreachable endpoint
cannot wedge producers indefinitely.

### Authentication / TLS

Set `:otlp_headers` to inject arbitrary request headers (typically
`%{"authorization" => "Bearer <token>"}`). HTTPS endpoints are
verified against the system CA store via `:mint`. No additional
config required for one-way TLS.

### Operational events

- `:otlp_batch_flushed` — emitted on every successful POST,
  carrying `measurements: %{batch_size, duration_us}`.
- `:otlp_batch_dropped` — emitted when retries are exhausted,
  carrying `measurements: %{batch_size, attempts}`.

## Configuration knobs

```elixir
config :flezha, :observability_export,
  prometheus_enabled: false,
  prometheus_port: 9568,
  prometheus_bind: "127.0.0.1",
  otlp_enabled: false,
  otlp_endpoint: "https://otlp-collector.example.com",
  otlp_headers: %{"authorization" => "Bearer xyz"},
  otlp_flush_interval_ms: 10_000,
  otlp_batch_max: 512
```

Env-var overrides:

- `FLEZHA_PROMETHEUS_ENABLED`
- `FLEZHA_PROMETHEUS_PORT`
- `FLEZHA_OTLP_ENABLED`
- `FLEZHA_OTLP_ENDPOINT`

## Operating a Grafana / OpenTelemetry Collector stack

A typical deployment:

1. Run an [OpenTelemetry Collector](https://opentelemetry.io/docs/collector/)
   alongside the Flezha controller.
2. Configure the collector with a Prometheus scrape job pointing at
   `http://flezha-host:9568/metrics`, and forward to your metrics
   backend (Mimir, Tempo, Honeycomb, etc.).
3. Alternatively (or additionally) enable the OTLP sink with
   `FLEZHA_OTLP_ENABLED=true FLEZHA_OTLP_ENDPOINT=http://collector:4318/v1/metrics`
   and let the collector route metrics / logs / traces to your
   backends.

The Prometheus metric names follow the convention
`<source>_<name>` (e.g. `beam_vm_memory_total`, `host_rss_bytes`).
