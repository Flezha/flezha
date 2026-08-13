# observability-export Specification

## Purpose

The observability-export capability provides built-in `Sink` implementations that fan observability signals out of the BEAM to external collectors. It ships two opt-in exporters: a hand-rolled `:gen_tcp`-based Prometheus text-exposition endpoint (`Flezha.Observability.Export.PrometheusSink` + `PrometheusHttp`) bound to `127.0.0.1` by default and accepting only `GET /metrics` and `GET /`, and an OTLP/HTTP protobuf exporter (`Flezha.Observability.Export.OtlpSink` + `OtlpCodec` + `OtlpClient`) that batches `:metric`, `:event`, `:log`, and `:trace_span` signals to a configured endpoint with bounded exponential backoff. Both sinks run under a dedicated `Flezha.Observability.Export.Supervisor` (`:one_for_one`) added as the last child of `Flezha.Observability.Supervisor`, honour per-exporter enable flags, and remain OFF by default. Operational events (`:otlp_batch_flushed`, `:otlp_batch_dropped`, `:prometheus_scrape`, `:prometheus_label_truncated`) are emitted onto a declarative `:observability_export` source so they participate in the foundation's bus / sink fan-out. The Prometheus sink uses the foundation's `:drop` overflow policy; the OTLP sink uses `:block` so operators turning it on accept workflow producer back-pressure rather than silent loss.

## Requirements

### Requirement: Prometheus exporter sink

The system SHALL provide a built-in Sink `Flezha.Observability.Export.PrometheusSink` that aggregates `:metric` signals into in-memory counter / gauge / histogram registers and serves the Prometheus text exposition format on a configurable HTTP port. The sink SHALL implement the foundation's `Flezha.Observability.Sink` behaviour and SHALL register with the foundation Registry when the `:prometheus_enabled` config flag is `true`. The sink SHALL use the foundation's `:drop` overflow policy.

#### Scenario: Sink off by default
- **WHEN** the application boots with default `:observability_export` configuration
- **THEN** `Flezha.Observability.Registry.list_sinks/0` SHALL NOT include a sink named `:prometheus` and no HTTP port SHALL be bound

#### Scenario: Sink registers when enabled
- **WHEN** the application boots with `:observability_export, :prometheus_enabled: true`
- **THEN** `Flezha.Observability.Registry.list_sinks/0` SHALL include a sink named `:prometheus` and an HTTP listener SHALL be bound on the configured port (default `9568`)

#### Scenario: Counter metric is exposed
- **WHEN** the sink has received `5` `:metric` signals on source `:bench`, name `:requests`, with `measurements: %{count: 1}` each and metadata `%{path: "/x"}`
- **THEN** a `GET /metrics` to the configured port SHALL return a body containing the line `bench_requests_total{path="/x"} 5` (or equivalent canonical Prometheus naming and labelling)

#### Scenario: Gauge metric is exposed
- **WHEN** the sink has received a `:metric` signal with `measurements: %{value: 42.5}` on source `:beam_vm`, name `:rss_bytes`
- **THEN** a `GET /metrics` SHALL return a line of type gauge with value `42.5` for the `beam_vm_rss_bytes` metric

#### Scenario: Histogram metric is exposed
- **WHEN** the sink has received `:metric` signals with `measurements: %{duration_us: 100}`, `%{duration_us: 500}`, `%{duration_us: 1500}` on source `:flezha_state`, name `:fsync_tick`
- **THEN** a `GET /metrics` SHALL return Prometheus histogram lines (`_bucket`, `_sum`, `_count`) for `flezha_state_fsync_tick_microseconds` reflecting the three samples

#### Scenario: Producer-set type override wins over inference
- **WHEN** a producer emits `:metric` with `measurements: %{count: 1}` and `metadata: %{prometheus_type: :gauge, ...}`
- **THEN** the sink SHALL render the metric as a gauge regardless of the `:count` key default

#### Scenario: Cardinality truncation visibility
- **WHEN** a source with cardinality budget `100` emits `:metric` signals carrying `101` distinct label combinations on a single metric name
- **THEN** the `/metrics` output SHALL include at most `100` distinct labelled series for that metric, PLUS one synthetic series with `__truncated__="true"` aggregating the over-budget emits, AND the foundation Registry counter `:prometheus_label_truncations` for the source SHALL be greater than `0`

### Requirement: Prometheus HTTP endpoint is restrictive

The Prometheus HTTP listener SHALL accept exactly two request shapes: `GET /metrics` (returns the exposition body) and `GET /` (returns a small HTML link to `/metrics`). All other paths, methods, or malformed requests SHALL return `404 Not Found` or `400 Bad Request` and SHALL NOT leak state. The listener SHALL bind to `127.0.0.1` by default and SHALL require explicit configuration (`:prometheus_bind`) to listen on a non-loopback interface.

#### Scenario: Unknown path returns 404
- **WHEN** an HTTP client sends `GET /admin` to the configured port
- **THEN** the server SHALL respond with status `404` and a brief plaintext body

#### Scenario: Disallowed method returns 405
- **WHEN** an HTTP client sends `POST /metrics` to the configured port
- **THEN** the server SHALL respond with status `405` and SHALL NOT mutate any state

#### Scenario: Slow request is timed out
- **WHEN** an HTTP client opens a connection and sends one byte every `5` seconds for `60` seconds
- **THEN** the server SHALL close the connection after the per-connection idle timeout (default `5_000` ms) without producing a response and without affecting other concurrent scrapes

#### Scenario: Default bind is loopback
- **WHEN** the application boots with `:prometheus_enabled: true` and no explicit `:prometheus_bind`
- **THEN** the listener SHALL be bound on `127.0.0.1` only and NOT on `0.0.0.0`

### Requirement: OTLP exporter sink

The system SHALL provide a built-in Sink `Flezha.Observability.Export.OtlpSink` that exports `:metric`, `:event`, `:log`, and `:trace_span` signals over OTLP/HTTP (protobuf body) to a configured endpoint. The sink SHALL batch signals up to `:otlp_batch_max` (default `512`) or `:otlp_flush_interval_ms` (default `10_000`), whichever fires first. The sink SHALL use the foundation's `:block` overflow policy and SHALL be OFF by default.

#### Scenario: Sink off by default
- **WHEN** the application boots with default `:observability_export` configuration
- **THEN** `Flezha.Observability.Registry.list_sinks/0` SHALL NOT include a sink named `:otlp`

#### Scenario: Sink registers when enabled
- **WHEN** the application boots with `:observability_export, :otlp_enabled: true` and an `:otlp_endpoint` set
- **THEN** `Flezha.Observability.Registry.list_sinks/0` SHALL include a sink named `:otlp` with overflow policy `:block`

#### Scenario: Batched export on size trigger
- **WHEN** the sink has received `:otlp_batch_max` signals since its last flush
- **THEN** the sink SHALL POST a single OTLP/HTTP request carrying every batched signal as protobuf within one second of the threshold being crossed, and an `:otlp_batch_flushed` event SHALL be emitted on source `:observability_export`

#### Scenario: Batched export on time trigger
- **WHEN** at least one signal is in the buffer and `:otlp_flush_interval_ms` elapses since the last flush
- **THEN** the sink SHALL POST a single OTLP/HTTP request carrying every buffered signal, even if fewer than `:otlp_batch_max`

#### Scenario: Configured headers are sent
- **WHEN** the sink is configured with `otlp_headers: %{"authorization" => "Bearer xyz"}`
- **THEN** every outgoing OTLP/HTTP POST SHALL include the header `Authorization: Bearer xyz`

#### Scenario: TLS endpoint is honoured
- **WHEN** the sink is configured with `otlp_endpoint: "https://otlp.example.com/v1/metrics"`
- **THEN** the underlying HTTP client SHALL establish a TLS connection and SHALL verify the server certificate against the system CA store

### Requirement: OTLP retry and back-pressure semantics

The OTLP sink SHALL apply bounded exponential backoff on transient HTTP failures (`5xx`, connection refused, timeout) with at most `5` attempts and an interval cap of `4_000` ms. After `5` failed attempts the batch SHALL be dropped, the per-sink `:dropped` counter SHALL increment, and an `:otlp_batch_dropped` event SHALL be emitted on source `:observability_export` carrying the batch size and attempt count. The sink's `:block` overflow policy SHALL bound producer-side back-pressure to the configured retry window so an unreachable endpoint SHALL NOT wedge producers indefinitely.

#### Scenario: Transient failure retries with backoff
- **WHEN** the OTLP endpoint returns `503` on the first attempt
- **THEN** the sink SHALL retry after `250` ms (backoff), then `500` ms, `1_000` ms, `2_000` ms, `4_000` ms (capped) — at most `5` attempts total

#### Scenario: Permanent failure drops the batch
- **WHEN** the OTLP endpoint refuses connection on all `5` attempts
- **THEN** the batch SHALL be discarded, the sink's `:dropped` counter SHALL increment by the batch size, and an `:otlp_batch_dropped` `:event` signal SHALL be emitted with `measurements: %{batch_size: N, attempts: 5}`

#### Scenario: Producer back-pressure is bounded
- **WHEN** the OTLP endpoint is unreachable for `60` seconds and a producer continuously emits signals
- **THEN** the producer's `Bus.emit/1` calls SHALL never block for longer than `mailbox_high_watermark × max_backoff` and SHALL eventually return as batches drop

### Requirement: Export supervisor and enable flags

Both exporters SHALL run under a dedicated supervisor `Flezha.Observability.Export.Supervisor` (`:one_for_one`) added as a child of `Flezha.Observability.Supervisor` AFTER the foundation's `DefaultSinks` worker. The supervisor SHALL honour the per-exporter enable flags (`:prometheus_enabled`, `:otlp_enabled`) and SHALL start each exporter as a child only when its flag is `true`. When both flags are `false`, the supervisor SHALL run with zero children.

#### Scenario: Only Prometheus enabled
- **WHEN** the application boots with `:prometheus_enabled: true` and `:otlp_enabled: false`
- **THEN** `Flezha.Observability.Export.Supervisor` SHALL list exactly one child (the Prometheus subtree) and the OTLP sink SHALL NOT be registered

#### Scenario: Master observability flag disables exports
- **WHEN** the application boots with `:observability, :enabled: false`
- **THEN** `Flezha.Observability.Export.Supervisor` SHALL NOT run (transitively suppressed by the master flag) and `Flezha.Observability.Registry.list_sinks/0` SHALL return an empty list

### Requirement: Export-internal events on `:observability_export` source

The export subsystem SHALL register a declarative source `:observability_export` with kind `[:event]` at supervisor start. Operational events SHALL be emitted onto this source so they participate in the foundation's bus / sink fan-out. At minimum the events `:otlp_batch_flushed`, `:otlp_batch_dropped`, `:prometheus_scrape`, and `:prometheus_label_truncated` SHALL be emitted.

#### Scenario: Prometheus scrape emits a `:prometheus_scrape` event
- **WHEN** an HTTP client successfully scrapes `/metrics`
- **THEN** an `:event` signal on source `:observability_export`, name `:prometheus_scrape`, with `measurements: %{duration_us: pos_integer(), series_count: non_neg_integer()}` SHALL be emitted on the bus

#### Scenario: OTLP flush emits a `:otlp_batch_flushed` event
- **WHEN** the OTLP sink successfully POSTs a batch of `N` signals
- **THEN** an `:event` signal on source `:observability_export`, name `:otlp_batch_flushed`, with `measurements: %{batch_size: N, duration_us: pos_integer()}` SHALL be emitted on the bus

### Requirement: Runtime configuration knobs and env-var overrides

The system SHALL expose a runtime configuration map `:observability_export` with the keys `:prometheus_enabled` (bool), `:prometheus_port` (integer, default `9568`), `:prometheus_bind` (binary, default `"127.0.0.1"`), `:otlp_enabled` (bool), `:otlp_endpoint` (URL binary), `:otlp_headers` (map of binary keys to binary values), `:otlp_flush_interval_ms` (positive integer), `:otlp_batch_max` (positive integer). Env-var overrides `FLEZHA_PROMETHEUS_ENABLED`, `FLEZHA_PROMETHEUS_PORT`, `FLEZHA_OTLP_ENABLED`, `FLEZHA_OTLP_ENDPOINT` SHALL set the corresponding keys at runtime-config evaluation time. The `:observability` map SHALL additionally gain `:otp_backend_enabled` (bool, default `true`).

#### Scenario: Env var enables Prometheus exporter
- **WHEN** the application is booted with `FLEZHA_PROMETHEUS_ENABLED=true` and no other observability_export config
- **THEN** `Flezha.Observability.Registry.list_sinks/0` SHALL include `:prometheus` and the listener SHALL bind on port `9568`

#### Scenario: Env var sets a non-default port
- **WHEN** the application is booted with `FLEZHA_PROMETHEUS_ENABLED=true` and `FLEZHA_PROMETHEUS_PORT=19568`
- **THEN** the Prometheus listener SHALL bind on `127.0.0.1:19568`

#### Scenario: Test config disables both exporters by default
- **WHEN** tests run via `mix test`
- **THEN** both `:prometheus_enabled` and `:otlp_enabled` SHALL be `false` so no port is bound and no network call is attempted unless a test explicitly opts in
