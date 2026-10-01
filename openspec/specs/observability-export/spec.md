# observability-export Specification

## Purpose

The observability-export capability provides built-in `Sink` implementations that fan observability signals out of the BEAM to external collectors. It ships two opt-in exporters: a hand-rolled `:gen_tcp`-based Prometheus text-exposition endpoint (`Flezha.Observability.Export.PrometheusSink` + `PrometheusHttp`) bound to `127.0.0.1` by default and accepting only `GET /metrics` and `GET /`, and an OTLP/HTTP protobuf exporter (`Flezha.Observability.Export.OtlpSink` + `OtlpCodec` + `OtlpClient`) that batches `:metric`, `:event`, `:log`, and `:trace_span` signals to a configured endpoint with bounded exponential backoff. Both sinks run under a dedicated `Flezha.Observability.Export.Supervisor` (`:one_for_one`) added as the last child of `Flezha.Observability.Supervisor`, honour per-exporter enable flags, and remain OFF by default. Operational events (`:otlp_batch_flushed`, `:otlp_batch_dropped`, `:prometheus_scrape`, `:prometheus_label_truncated`) are emitted onto a declarative `:observability_export` source so they participate in the foundation's bus / sink fan-out. The Prometheus sink uses the foundation's `:drop` overflow policy; the OTLP sink uses `:block` so users turning it on accept steps producer back-pressure rather than silent loss.
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

The OTLP sink SHALL apply bounded exponential backoff on transient HTTP failures (`5xx`, connection refused, timeout) with at most `5` attempts and an interval cap of `4_000` ms; that schedule SHALL be the `:otlp_post` row of the backoff table (`250, 500, 1_000, 2_000, 4_000` ms, no jitter, five attempts), read from the table and emitted on `[:flezha, :backoff, :wait]` per attempt. After `5` failed attempts the batch SHALL be dropped, the per-sink `:dropped` counter SHALL increment, and an `:otlp_batch_dropped` event SHALL be emitted on source `:observability_export` carrying the batch size and attempt count. The sink's `:block` overflow policy SHALL bound producer-side back-pressure to the configured retry window so an unreachable endpoint SHALL NOT wedge producers indefinitely.

#### Scenario: Transient failure retries with backoff
- **WHEN** the OTLP endpoint returns `503` on the first attempt
- **THEN** the sink SHALL retry after `250` ms (backoff), then `500` ms, `1_000` ms, `2_000` ms, `4_000` ms (capped) — at most `5` attempts total

#### Scenario: Permanent failure drops the batch
- **WHEN** the OTLP endpoint refuses connection on all `5` attempts
- **THEN** the batch SHALL be discarded, the sink's `:dropped` counter SHALL increment by the batch size, and an `:otlp_batch_dropped` `:event` signal SHALL be emitted with `measurements: %{batch_size: N, attempts: 5}`

#### Scenario: Producer back-pressure is bounded
- **WHEN** the OTLP endpoint is unreachable for `60` seconds and a producer continuously emits signals
- **THEN** the producer's `Bus.emit/1` calls SHALL never block for longer than `mailbox_high_watermark × max_backoff` and SHALL eventually return as batches drop

#### Scenario: The schedule is read from the table, not the client
- **WHEN** the `:otlp_post` row is read beside the sink's five waits on `[:flezha, :backoff, :wait]`
- **THEN** the emitted delays SHALL equal the row's rungs exactly, and the client module SHALL hold no rung list of its own

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

### Requirement: Every attribute key and value SHALL be encodable on the OTLP wire

The OTLP encoder SHALL represent any attribute key as a string and any attribute
value as the `AnyValue` member that carries its type, whatever the term. Encoding
SHALL NOT raise, and SHALL NOT be able to take the export process down, because
the terms it receives are ordinary telemetry: the Logger bridge attaches a map
and a tuple to every log signal it forwards, and a signal's metadata is validated
as a map without any constraint on its keys, so a key is as free in its shape as
a value.

**A key takes the text rule.** A term that is already a binary SHALL be used
unchanged **where that binary is valid UTF-8**. A binary that is not SHALL be
rendered by inspection: protobuf defines a `string` field as UTF-8, so passing
arbitrary bytes through produces a message a strict collector rejects. Any term
that is not a binary, atom, integer or float SHALL be rendered by inspection,
**lists included** — a list SHALL NOT be converted with `to_string/1`, which
returns the binary of its elements and silently changes what the attribute says.
A `nil` key SHALL render as `nil`: it is the one atom whose `String.Chars`
rendering is not its own name but the empty string, which as a key names nothing
at all and collides with a genuine empty key.

**A value takes the typed rule**, and `AnyValue`'s oneof SHALL carry it: a
boolean as `bool_value`, an integer as `int_value`, a float as `double_value`, a
list as `array_value`, a map as `kvlist_value` whose entries are `KeyValue`
messages built exactly as a top-level attribute is, and a binary or an atom as
`string_value`. A `nil` value SHALL cross as an `AnyValue` with **no member of
the oneof set** — the protocol's own representation of a value with no type —
and SHALL NOT cross as the text `nil`, because a consumer cannot tell a real null
from the word.

A term for which OTLP has no shape — a tuple, a pid, a reference, a function, a
struct — SHALL cross as its inspection in `string_value`. An integer that does
not fit `int_value`'s `int64` SHALL cross as its decimal rendering in
`string_value` rather than as a varint no decoder can read.

The rendering SHALL NOT elide. Where a limit applies it SHALL be stated
explicitly, so a truncated attribute is never mistaken for a complete one.

This rule SHALL be stated in exactly one place and reached by every renderer of
a signal's metadata — the OTLP encoder, the Prometheus registry, the audit log
and the `flz observe tail` renderer — so they cannot drift into different
answers to one question. It is the rule for a wire that can carry only text, and
it SHALL be the only such rule.

A renderer whose output format carries typed values SHALL use one shared rule for
*values* too, and SHALL NOT keep its own. There SHALL therefore be exactly two
rules for a term the system did not choose the type of: the text one above, and a
typed one for a format with a null, an array and an object. The typed rule SHALL
pass a binary, a number and a boolean through unchanged, SHALL render an atom by
its own name, SHALL carry `nil` as the format's null rather than as the text
`nil`, and SHALL recurse into a list and into a map — whose keys take the text
rule, because a key is text in every one of these formats. Any other term SHALL be
rendered by inspection, exactly as the text rule renders it.

Where a format has a type the typed rule's own output cannot express, the encoder
SHALL choose the field that carries it and SHALL NOT re-decide what the term
means. The one such term is a binary that is not valid UTF-8: it SHALL take the
text rule's answer and cross inspected, so that the raw bytes appear nowhere in
the encoded message.

Two renderers of one format answering differently is the failure this states away:
the audit log and `flz observe tail --json` are both JSON, a user may read
them side by side, and a list rendered as an array in one and as the string
`"[1, 2, 3]"` in the other is one question answered twice.

#### Scenario: A bridged log signal encodes

- **GIVEN** a log signal carrying the metadata the Logger bridge attaches, including a map and an `{module, function, arity}` tuple
- **WHEN** it is encoded for OTLP
- **THEN** encoding SHALL succeed, the map SHALL be a `kvlist_value` and the tuple SHALL be a readable `string_value`

#### Scenario: A list attribute is not re-encoded as its bytes

- **GIVEN** an attribute whose value is the list `[1, 2, 3]`
- **WHEN** it is encoded
- **THEN** the attribute SHALL be an `array_value` of three `int_value` elements, and SHALL NOT be the three-byte binary of its elements nor a string

#### Scenario: A typed value crosses as its own `AnyValue` member

- **GIVEN** attributes whose values are an integer, a negative integer, a boolean, a float and a map
- **WHEN** they are encoded
- **THEN** each SHALL set `int_value`, `int_value`, `bool_value`, `double_value` and `kvlist_value` respectively, and none SHALL set `string_value`

#### Scenario: A term with no OTLP shape crosses as its inspection

- **GIVEN** attributes whose values are a tuple, a pid and a struct
- **WHEN** they are encoded
- **THEN** each SHALL set `string_value`, and its content SHALL be the term's inspection

#### Scenario: A non-string attribute key is coerced like a value

- **GIVEN** a signal whose metadata is keyed by a tuple and by a list
- **WHEN** it is encoded
- **THEN** encoding SHALL succeed, and neither key SHALL be the binary of its elements

#### Scenario: A string attribute is unchanged

- **GIVEN** an attribute whose value is already a binary and is valid UTF-8
- **WHEN** it is encoded
- **THEN** the attribute SHALL be that binary, unmodified, in `string_value`

#### Scenario: A non-UTF-8 binary attribute does not reach the wire raw

- **GIVEN** an attribute whose value is a binary that is not valid UTF-8
- **WHEN** it is encoded
- **THEN** the attribute's encoded `string` field SHALL be valid UTF-8, and the raw bytes SHALL NOT appear anywhere in the encoded message

#### Scenario: A `nil` key names itself and a `nil` value carries no type

- **GIVEN** an attribute whose key is `nil` and an attribute whose value is `nil`
- **WHEN** they are encoded
- **THEN** the key SHALL render `nil` and SHALL NOT render as the empty string, and the value's `AnyValue` SHALL set no member of the oneof

#### Scenario: The exporter survives what it cannot represent typefully

- **GIVEN** a stream of signals carrying values of any term type
- **WHEN** they are exported
- **THEN** the export process SHALL remain alive, and no signal SHALL be dropped for the shape of its attribute values

#### Scenario: A list, a map and a null reach a typed format as themselves

- **GIVEN** a signal whose metadata carries a list, a map and a `nil` value
- **WHEN** it is rendered into a format that carries typed values
- **THEN** the list SHALL be an array, the map SHALL be an object whose keys took the
  text rule, and the `nil` SHALL be the format's null — none of the three a string

#### Scenario: Two renderers of one format answer identically

- **GIVEN** one signal's metadata rendered by the audit log and by `flz observe tail --json`
- **WHEN** both renderings are decoded
- **THEN** the two metadata objects SHALL be equal

### Requirement: The Prometheus exposition SHALL be well-formed whatever a metric signal's metadata carries

The Prometheus registry SHALL render a valid text exposition for any `:metric`
signal, whatever the types and spellings in its metadata map. Recording a signal
SHALL NOT raise, because the registry owns the exporter's aggregation tables and
its death discards every counter, gauge and histogram accumulated since boot —
a loss a user reads as a counter reset, and one that arrives precisely when
the system is under the load that makes export worth having.

The terms it receives are ordinary telemetry, not misuse. `Signal.new/1`
validates that metadata is a map and constrains neither its keys nor its values,
and the telemetry bridge forwards a `:telemetry` event's metadata map verbatim,
so a producer's tuple, map, pid, list or arbitrarily-spelled key reaches the
exporter unchanged.

**Label values.** A value SHALL be rendered by the same rule the OTLP encoder
uses for an attribute: a valid-UTF-8 binary unchanged, an atom (`nil` as `nil`),
integer or float by its `String.Chars` rendering, and every other term — lists
included — by inspection. One rule SHALL serve both exporters; two would be the
same question answered twice, and the answers would drift.

**Label names.** A rendered label name SHALL match `[a-zA-Z_][a-zA-Z0-9_]*`. A
name that does not SHALL be sanitized into that grammar rather than refused, so
that a key following a foreign convention — dotted OpenTelemetry attribute
names reach the bridge unchanged — keeps its label instead of disappearing with
nothing in the output to say so. A malformed name is worse than a rewritten one:
Prometheus fails the entire scrape on a parse error, so one bad key hides every
metric the controller exports.

A sanitized name SHALL NOT enter Prometheus's reserved `__` namespace, and SHALL
NOT be able to forge the registry's own `__truncated__` series.

**Reserved and colliding names.** A rendered series SHALL NOT carry the same
label name twice. Where a producer's label would collide with one the renderer
adds itself — `le` on a histogram's bucket lines — the producer's label SHALL be
dropped. Where two of a signal's own keys sanitize to one name, one SHALL be
kept deterministically and the rest dropped.

**Visibility.** Every dropped or rewritten label SHALL increment a per-source
counter readable from the registry. It SHALL NOT emit an event per occurrence:
the sites that would emit are on the recording path, and telemetry that storms
under the condition it reports is not observability.

#### Scenario: A metric signal carrying a tuple does not kill the registry

- **GIVEN** the `[:flezha, :state, :wal, :backpressure]` telemetry event, whose metadata carries `cell_id` as an `{operator_class, tasklet_id, cell_key}` tuple
- **WHEN** it reaches the Prometheus registry through the telemetry bridge
- **THEN** the registry process SHALL remain alive, its aggregation tables SHALL survive, and the metric SHALL appear in the exposition with a readable rendering of the tuple

#### Scenario: Series recorded before an unrepresentable one survive it

- **GIVEN** a counter series already recorded and rendering
- **WHEN** a later signal carries a value or key of any other term type
- **THEN** the earlier series SHALL still render with its accumulated value

#### Scenario: A list label value is not re-encoded as its bytes

- **GIVEN** a metric signal whose metadata is `%{l: [1, 2, 3]}`
- **WHEN** the exposition is rendered
- **THEN** the label SHALL read as the list, and SHALL NOT be the three-byte binary of its elements

#### Scenario: A label name outside the Prometheus grammar is rewritten, not emitted raw

- **GIVEN** a metric signal whose metadata is keyed by `"http.status_code"`
- **WHEN** the exposition is rendered
- **THEN** the rendered label name SHALL match `[a-zA-Z_][a-zA-Z0-9_]*`, and the label's value SHALL still be present

#### Scenario: A producer cannot enter the reserved namespace

- **GIVEN** a metric signal whose metadata key sanitizes to a name beginning with two underscores
- **WHEN** the exposition is rendered
- **THEN** the rendered name SHALL carry at most one leading underscore, and SHALL NOT be `__truncated__`

#### Scenario: A histogram never carries two `le` labels

- **GIVEN** a metric signal with `measurements: %{duration_us: 100}` and a metadata key `le`
- **WHEN** the exposition is rendered
- **THEN** each bucket line SHALL carry exactly one `le` label, the one the renderer adds

#### Scenario: The exposition body is valid UTF-8

- **GIVEN** a metric signal whose metadata value is a binary that is not valid UTF-8
- **WHEN** the exposition is rendered
- **THEN** the whole body SHALL be valid UTF-8, as the `charset=utf-8` content type it is served with declares

#### Scenario: The exporter survives a flapping producer

- **GIVEN** a producer emitting signals the exporter cannot represent typefully, faster than the export supervisor's restart intensity
- **WHEN** they are recorded
- **THEN** the export supervisor and all its children SHALL remain alive

### Requirement: One exposition name SHALL be produced by one function

A metric's exposition name SHALL be produced by a single function shared by every
exporter, with the joining character a parameter of the exporter rather than a
second copy of the rule. Two exporters SHALL NOT be able to disagree about the name
of the same metric.

#### Scenario: The same source and name reach two exporters

- **WHEN** the OTLP and Prometheus exporters render the same `{source, name}` pair
- **THEN** both SHALL derive it from the one shared function, each applying its own
  joining character

#### Scenario: A refusal sentence is written once

- **WHEN** a provider value is refused by a filter and by the provider behaviour
- **THEN** both SHALL produce the refusal through `refuse_value/2` rather than one
  restating the other's sentence

