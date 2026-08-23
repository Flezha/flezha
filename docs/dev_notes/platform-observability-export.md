# Observability export (`platform-observability-export`)

- **Modules**: `lib/flezha/observability/export/` — `Supervisor`,
  `PrometheusRegistry`, `PrometheusSink`, `PrometheusHttp`,
  `OtlpCodec`, `OtlpClient`, `OtlpSink`.
- **Supervision tree**: `Flezha.Observability.Export.Supervisor` starts
  AFTER the OTP backend in `Flezha.Observability.Supervisor`
  (`AuditLog.Supervisor` is the last child). Registers the declarative
  `:observability_export` source on init so the exporters'
  operational events participate in bus fan-out.
- **Prometheus**: hand-rolled `:gen_tcp` HTTP/1.1 listener (~250
  LOC, no Cowboy/Plug) at default `127.0.0.1:9568/metrics`. Accepts
  only `GET /metrics` and `GET /`; everything else returns 404 / 405
  / 400. Type inferred from `Signal.measurements` keys (`:count` →
  counter, `:value`/`:gauge` → gauge, `:duration_us`/`:bytes` →
  histogram); `metadata.prometheus_type` overrides. Cardinality
  enforcement aggregates over-budget label values into a synthetic
  `{__truncated__="true"}` series and emits
  `:prometheus_label_truncated` events.
- **OTLP**: OTLP/HTTP via `:mint` (top-level dep, was transitive
  via `:mint_web_socket`). Hand-rolled protobuf encoder in
  `OtlpCodec` matches OpenTelemetry v1 schema for metrics / logs /
  traces. Batched with size + time triggers (`:otlp_batch_max` 512,
  `:otlp_flush_interval_ms` 10s). Overflow policy `:block` —
  producer back-pressure during OTLP outages, bounded by
  `mailbox_watermark × sum-of-backoffs`. Backoff: 250/500/1000/2000/4000
  ms, max 5 attempts.
- **Runtime config knobs**: `:observability_export` map with
  `:prometheus_enabled` (default `false`), `:prometheus_port`
  (`9568`), `:prometheus_bind` (`"127.0.0.1"`), `:otlp_enabled`
  (`false`), `:otlp_endpoint`, `:otlp_headers` (map for bearer
  tokens), `:otlp_flush_interval_ms`, `:otlp_batch_max`. Env-var
  overrides: `FLEZHA_PROMETHEUS_ENABLED`, `FLEZHA_PROMETHEUS_PORT`,
  `FLEZHA_OTLP_ENABLED`, `FLEZHA_OTLP_ENDPOINT`.
- **Registry extension**: `Flezha.Observability.Registry.register_external_sink/1`
  lets exporters register a sink whose pid is owned by a different
  supervisor — required because `Export.Supervisor` owns the
  Prometheus and OTLP sinks, not the foundation `SinkSupervisor`.
- **Detailed notes**: `docs/operations/observability.md`.

