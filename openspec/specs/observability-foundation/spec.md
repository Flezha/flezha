# observability-foundation Specification

## Purpose

The observability-foundation capability provides the Flezha Controller's first-class, pluggable subsystem that unifies metrics, events, logs, and in-process trace spans behind one contract. It defines a single signal envelope (`kind` ∈ `:metric | :event | :log | :trace_span`), a `Source` behaviour for producers and a `Sink` behaviour for consumers, an ETS-backed `Registry` enumerating live sources and sinks with per-source cardinality budgets, and a fan-out bus that is non-blocking on the producer side (default policy: drop-with-counter — observability never back-pressures the hot path). The capability ships two default sinks (an in-memory per-source ring buffer queryable via the service API, and a `Phoenix.PubSub` fan-out for a future LiveView UI), a `:logger` handler that bridges Elixir log records onto the bus, and a single dispatching `:telemetry` handler that lifts the existing `[:flezha, ...]` events onto the bus without modifying any call-site. The service-API module `Flezha.Service.Observability` is callable from both the CLI (via `Flezha.Service.CliManager`) and the planned Phoenix UI directly, with a `caller_context` argument on every public function so a future authorization capability can plug in without a breaking API change. External exporters, built-in VM/host probes, and cross-transport trace-context propagation are explicitly out of scope and deferred to follow-up capabilities.

## Requirements

### Requirement: Unified signal taxonomy

The observability subsystem SHALL define exactly one signal envelope structure used by all producers and all consumers. The structure SHALL include the fields `kind`, `source`, `name`, `measurements`, `metadata`, and `ts`. The field `kind` SHALL be one of the atoms `:metric`, `:event`, `:log`, or `:trace_span`. The structure SHALL be exposed as a module struct under the foundation namespace and SHALL be the only on-bus signal shape.

#### Scenario: Constructing a metric signal
- **WHEN** a producer constructs a signal with `kind: :metric`, `source: :flezha_state`, `name: :wal_backpressure`, `measurements: %{count: 1, bytes: 16_384}`, `metadata: %{operative: "orders", operation: 3}`
- **THEN** the resulting struct SHALL be accepted by the bus without error and SHALL carry a monotonic-time `ts` field set automatically when not supplied

#### Scenario: Rejecting an unknown signal kind
- **WHEN** a producer attempts to construct a signal with `kind: :unknown_kind`
- **THEN** the construction SHALL fail with an `ArgumentError` before the signal reaches the bus

#### Scenario: All four kinds are accepted
- **WHEN** signals of each kind `:metric`, `:event`, `:log`, `:trace_span` are emitted in turn
- **THEN** each SHALL be delivered to attached sinks with its `kind` field preserved exactly

### Requirement: Source contract

The observability subsystem SHALL define a `Source` contract under which any producer of signals can register itself. The contract SHALL support two registration flavours: a **declarative** flavour in which the producer calls a public emit function directly, and a **process-backed** flavour in which the producer implements a behaviour and is started under the source supervisor. Every registered source SHALL declare its name, the set of signal kinds it emits, and a per-source cardinality budget.

#### Scenario: Declarative source registration
- **WHEN** code registers a declarative source with name `:my_source`, kinds `[:metric, :event]`, and cardinality budget `1_000`
- **THEN** the source SHALL appear in `list_sources/0` and emits from `:my_source` SHALL be accepted by the bus

#### Scenario: Process-backed source registration
- **WHEN** code registers a module implementing the process-backed `Source` behaviour
- **THEN** the source supervisor SHALL start the module as a child under a one-for-one strategy, and the source SHALL appear in `list_sources/0` with its status

#### Scenario: Source emits a signal kind it did not declare
- **WHEN** a source registered with kinds `[:metric]` attempts to emit a `:log` signal
- **THEN** the emit SHALL be dropped, a `:source_kind_violation` event SHALL be emitted on the bus with the source name, and a per-source violation counter SHALL increment

### Requirement: Sink contract

The observability subsystem SHALL define a `Sink` contract under which any consumer of signals can be registered. Each Sink SHALL be implemented as a behaviour with at minimum an `init/1` callback and a `handle_signal/2` callback. Each registered Sink SHALL run as a supervised process under a dedicated DynamicSupervisor using a one-for-one strategy so that one Sink crash does not affect peer Sinks or producers.

#### Scenario: Sink receives every signal that passes its filter
- **WHEN** a sink is registered with filter `kinds: [:metric]` and a `:metric` signal is emitted
- **THEN** the sink's `handle_signal/2` SHALL be invoked exactly once with that signal

#### Scenario: Sink filter excludes non-matching signal
- **WHEN** a sink is registered with filter `kinds: [:metric]` and a `:log` signal is emitted
- **THEN** the sink's `handle_signal/2` SHALL NOT be invoked

#### Scenario: Crashing sink does not affect peer sinks
- **WHEN** sink A is registered to crash on every signal, sink B is registered as well, and a signal is emitted
- **THEN** sink A SHALL crash and be restarted by its supervisor, and sink B SHALL receive the signal exactly once

#### Scenario: Crashing sink does not affect producers
- **WHEN** a sink crashes during dispatch of a signal
- **THEN** the producer's emit call SHALL return normally and SHALL NOT raise

### Requirement: Registry tracks live sources and sinks

The observability subsystem SHALL maintain a registry, backed by ETS, that enumerates every currently registered source and sink with its name, status (`:active` / `:disabled` / `:crashed`), and registration metadata. The registry SHALL be queryable read-only by consumers without serializing through a single process.

#### Scenario: Listing live sources
- **WHEN** two sources `:a` and `:b` are registered and the caller invokes `list_sources/0`
- **THEN** the result SHALL include both sources with their kinds, cardinality budgets, and status `:active`

#### Scenario: Disabled source surfaces correctly
- **WHEN** source `:a` is disabled via `disable_source/1`
- **THEN** `list_sources/0` SHALL show source `:a` with status `:disabled` and emits from `:a` SHALL be dropped without invoking any sink

### Requirement: Per-source cardinality budget for metric signals

The Registry SHALL enforce a per-source cardinality budget on `:metric` signals based on the shape of `metadata` keys (the label-shape, not the label-values). When a source's distinct label-shape count exceeds its budget, subsequent emits of new shapes SHALL be dropped, a `:cardinality_budget_exceeded` event SHALL be emitted on the bus carrying only the source name, and a per-source counter SHALL increment. The budget SHALL NOT apply to `:event`, `:log`, or `:trace_span` signals.

#### Scenario: Source under budget
- **WHEN** a source with budget `100` emits `:metric` signals with `50` distinct label shapes
- **THEN** all emits SHALL be delivered to attached sinks

#### Scenario: Source exceeds budget
- **WHEN** a source with budget `100` emits `:metric` signals with `101` distinct label shapes
- **THEN** the 101st new label shape SHALL be dropped, a single `:cardinality_budget_exceeded` event SHALL be emitted on the bus, and the source's `:budget_exceeded_count` counter SHALL be `1`

#### Scenario: Budget does not apply to events
- **WHEN** a source with budget `100` emits `200` distinct `:event` signals with `200` distinct metadata shapes
- **THEN** all `200` events SHALL be delivered to attached sinks

#### Scenario: Budget exceeded event does not propagate cardinality
- **WHEN** the bus emits a `:cardinality_budget_exceeded` event
- **THEN** the event's metadata SHALL contain only the offending source name and no label-shape data from the dropped signal

### Requirement: Non-blocking producer fan-out with drop-with-counter overflow

The bus SHALL dispatch signals from producers to sinks without blocking the producer process when a sink's mailbox is over its watermark. The default per-sink overflow policy SHALL be `:drop`, and dropped signals SHALL be counted in a per-sink atomic counter exposed via the registry. A per-sink alternate policy `:block` SHALL be available at sink registration; sinks using `:block` SHALL use a synchronous call instead of an asynchronous send.

#### Scenario: Sink at watermark drops signals
- **WHEN** a sink with mailbox watermark `100` already has `100` messages queued and a producer emits a signal targeted at that sink
- **THEN** the signal SHALL NOT be delivered to that sink, the sink's `:dropped_count` SHALL increment by `1`, and the producer SHALL return immediately

#### Scenario: Producer is never blocked by a slow sink under default policy
- **WHEN** a sink is intentionally slow (sleeps in `handle_signal/2`) and a producer emits 1,000 signals in rapid succession
- **THEN** the producer SHALL complete all 1,000 emits in bounded time and SHALL NOT exceed the time taken by 1,000 emits when the slow sink is absent by more than a fixed small constant (registry lookup + `Process.info/2` check)

#### Scenario: Block policy is opt-in
- **WHEN** a sink is registered with `overflow: :block` and its mailbox is at watermark
- **THEN** producer emits targeted at the sink SHALL synchronously wait until the sink accepts the message; no drop counter SHALL increment for that sink

### Requirement: Default ring-buffer sink

The foundation SHALL ship a built-in ring-buffer sink that retains, in-memory, the last N signals per source (bounded by a maximum entry count and a maximum age). The ring buffer SHALL store signals in per-source ETS `:ordered_set` tables keyed by monotonic timestamp. The buffer's contents SHALL be queryable via the service API by source name and time range without serializing through a GenServer.

#### Scenario: Ring buffer retains recent signals
- **WHEN** source `:s` emits `100` signals and the caller queries the ring buffer for `:s` with `from_ts: 0`
- **THEN** the result SHALL contain `100` signals in monotonic-time order

#### Scenario: Ring buffer enforces max-entries cap
- **WHEN** the cap is `1_000` entries per source and a source emits `1_500` signals in quick succession
- **THEN** the ring buffer SHALL contain exactly the `1_000` most-recent signals for that source

#### Scenario: Ring buffer enforces age-based retention
- **WHEN** the retention window is `60_000` ms and `300` ms have passed beyond an entry's timestamp
- **THEN** the entry SHALL no longer be returned by a query whose `from_ts` is after the entry's timestamp; a periodic sweep SHALL delete the entry from ETS within one sweep interval after expiry

#### Scenario: Ring buffer queries do not block producers
- **WHEN** a producer is emitting `1_000` signals per second and a caller issues a query against the ring buffer for the same source
- **THEN** the producer's emit latency distribution SHALL NOT change measurably during the query

### Requirement: Default Phoenix.PubSub sink

The foundation SHALL ship a built-in sink that fans signals out via `Phoenix.PubSub` on a topic-per-source basis so that a future LiveView UI can subscribe once and receive pushed updates. The sink SHALL be optional and SHALL be enableable or disableable via runtime configuration; in test config it SHALL default to disabled.

#### Scenario: Subscriber receives signal
- **WHEN** a subscriber subscribes to topic `flezha.observability.source.:s` and source `:s` emits a signal
- **THEN** the subscriber's mailbox SHALL receive a message containing the signal envelope

#### Scenario: PubSub sink disabled in test
- **WHEN** the application boots with the runtime configuration `pubsub_sink_enabled: false`
- **THEN** the PubSub sink SHALL NOT be started under the sink supervisor, and a subscriber SHALL NOT receive any signal

### Requirement: Logger handler bridges Elixir log records onto the bus

The foundation SHALL install a `:logger` handler at supervisor start that converts every `Logger` event into a `:log` signal on the bus. The handler SHALL preserve at minimum the level, message, and `mfa`/`file`/`line` metadata of the original `Logger` event. The handler SHALL NOT cause log records to be lost in `Logger` if the bus dispatch fails — if dispatching raises, the handler SHALL catch the error, increment a `:logger_bridge_failures` counter, and return normally so the `Logger` pipeline continues.

#### Scenario: Log record becomes a signal
- **WHEN** application code calls `Logger.warning("hello")`
- **THEN** within the same scheduler tick a `:log` signal with `name: :warning`, `metadata.level: :warning`, and `metadata.message: "hello"` SHALL appear on the bus

#### Scenario: Bus failure does not break logging
- **WHEN** the bus dispatch raises during a `:log` signal emit
- **THEN** the original `Logger` call SHALL return normally, the `:logger_bridge_failures` counter SHALL increment by `1`, and subsequent `Logger` calls SHALL continue to function

### Requirement: Telemetry bridge lifts existing `:telemetry` events onto the bus

The foundation SHALL attach a single dispatching `:telemetry` handler at supervisor start that maps each known `[:flezha, ...]` event onto an observability signal. The mapping table SHALL be defined inside the bridge module and SHALL include at minimum the events `[:flezha, :state, :wal, :backpressure]`, `[:flezha, :state, :fsync, :tick]`, `[:flezha, :boot, :recovery_complete]`, `[:flezha, :shutdown, :requested]`, `[:flezha, :plugin_installer, :load_failed]`, `[:flezha, :plugin_installer, :store_opened]`, `[:flezha, :plugin_installer, :unsigned_install]`, `[:flezha, :ws, :idle_reconnect]`, `[:flezha, :ws, :resubscribe]`, `[:flezha, :state, :seq_gap]`, `[:flezha, :state, :output_stall]`, `[:flezha, :state, :compaction_timeout]`, `[:flezha, :state, :wal_realign]`, `[:flezha, :reconcile, :action]`, `[:flezha, :reconcile, :health]`, `[:flezha, :leadership, :transition]`, and `[:flezha, :stream, :drop]`. Existing call-sites SHALL NOT be modified.

#### Scenario: Bridged event becomes a signal
- **WHEN** code calls `:telemetry.execute([:flezha, :state, :wal, :backpressure], %{bytes: 1024}, %{operative: "r1"})`
- **THEN** a signal with `kind: :event`, `source: :flezha_state`, `name: :wal_backpressure`, `measurements.bytes: 1024`, `metadata.operative: "r1"` SHALL appear on the bus

#### Scenario: Websocket re-subscribe event is bridged
- **WHEN** code calls `:telemetry.execute([:flezha, :ws, :resubscribe], %{data_silence_ms: 120_000}, %{plugin_id: "wss://ws.kraken.com"})`
- **THEN** a signal sourced from the websocket provider carrying `measurements.data_silence_ms: 120_000` and `metadata.plugin_id: "wss://ws.kraken.com"` SHALL appear on the bus

#### Scenario: Persistence stall/gap events are bridged
- **WHEN** code calls `:telemetry.execute([:flezha, :state, :output_stall], %{stalled_ms: 60_000, last_seq: 100}, %{operative: "btc_kraken", operation: "ohlc", key: "1m"})`
- **THEN** a signal sourced from the state subsystem carrying the stalled duration, last seq, and Segment metadata SHALL appear on the bus
- **AND** the same bridging SHALL hold for `[:flezha, :state, :seq_gap]` and `[:flezha, :state, :compaction_timeout]`

#### Scenario: Bridge failure does not crash producer
- **WHEN** the bridge's mapping function raises for a specific event
- **THEN** the original `:telemetry.execute` call SHALL return normally, a `:dispatcher_failure_count` counter SHALL increment, and an error SHALL be logged once with the offending event name

### Requirement: Service-API surface

The foundation SHALL provide a service-API module `Flezha.Service.Observability` callable from both the CLI (via `Flezha.Service.CliManager`) and the future Phoenix UI directly, without routing through `CliManager`. The module SHALL expose at minimum `subscribe/2`, `query/2`, `list_sources/0`, `list_sinks/0`, `enable_source/1`, `disable_source/1`. Every public function SHALL accept a `caller_context` argument as its first or last positional argument AND SHALL be authorized through `Flezha.AuthZ.authorize/3` when `:authn_authz, :enabled` is `true`. Read operations (`list_sources`, `list_sinks`, `query`, `subscribe`) SHALL require at least the `viewer` role on the caller's bound namespace; mutate operations (`enable_source`, `disable_source`) SHALL require at least `operator`. Read operations SHALL filter results to the caller's bound namespace; results for other namespaces SHALL NOT be returned.

#### Scenario: CLI calls service API through CliManager
- **WHEN** the CLI invokes a `list_sources` request that reaches `Flezha.Service.CliManager`
- **THEN** `CliManager` SHALL call `Flezha.Service.Observability.list_sources/1` passing a `caller_context` with `source: :cli` AND a verified `principal`/`namespace`/`role` from the handshake, and the result SHALL flow back to the CLI

#### Scenario: UI calls service API directly
- **WHEN** Phoenix UI controller code (in a future change) invokes `Flezha.Service.Observability.query/2` with a `caller_context` of `source: :ui` carrying a verified principal
- **THEN** the call SHALL succeed without routing through `CliManager`

#### Scenario: Service API records caller context on audit-relevant calls
- **WHEN** any of `subscribe/2`, `enable_source/1`, `disable_source/1` is invoked
- **THEN** an `:event` signal with the called function name, the `caller_context.source`, the verified `caller_context.principal`, the `caller_context.namespace`, and the `caller_context.request_id` (if present) SHALL be emitted on the bus

#### Scenario: Viewer cannot disable a source
- **WHEN** `:authn_authz, :enabled` is `true` AND a caller with role `viewer` invokes `disable_source/2`
- **THEN** the call SHALL return `{:error, {:forbidden, :role_insufficient}}` AND no source SHALL be disabled

#### Scenario: Cross-namespace query is filtered
- **WHEN** `:authn_authz, :enabled` is `true` AND a caller bound to namespace `team-orders` invokes `query/2` against a source carrying signals from both `team-orders` and `team-payments`
- **THEN** the returned signal list SHALL contain only signals whose `metadata.namespace` is `team-orders`

### Requirement: Authorization seam

The service API SHALL accept a `caller_context` argument on every public function and the context SHALL include at minimum a `source` field whose value is one of `:cli`, `:ui`, `:sdk`, or `:internal`. When `:authn_authz, :enabled` is `true`, the `caller_context` from a `:cli`, `:ui`, or `:sdk` source SHALL also include a verified `:principal` bound by the authentication layer at the connection boundary; service implementations SHALL call `Flezha.AuthZ.authorize/3` at the top of every public function before any side effect. Service callers MUST NOT forge a `:principal`; calls whose presented `:principal` was not bound to the caller's connection by the authentication layer SHALL be rejected with `{:error, {:forbidden, :principal_not_bound}}`. `:internal` callers SHALL pass `source: :internal` and SHALL NOT carry a `:principal`. When `:authn_authz, :enabled` is `false` (the upgrade-compatibility mode), the prior behaviour SHALL be preserved: no `:principal` is required and `Flezha.AuthZ.authorize/3` returns `:ok` unconditionally.

#### Scenario: Caller context with verified principal succeeds
- **WHEN** `:authn_authz, :enabled` is `true` AND a `:cli` caller invokes `query/2` with `caller_context: %{source: :cli, principal: "alice@example.com", namespace: "_system", role: :viewer}` AND the principal was bound by the CLI handshake
- **THEN** the call SHALL succeed if `Flezha.AuthZ.authorize/3` returns `:ok` for action `:observability.query` on scope `%{namespace: "_system"}`

#### Scenario: Forged principal is rejected
- **WHEN** `:authn_authz, :enabled` is `true` AND a caller invokes `query/2` with a `:principal` that the authentication layer did not bind to this caller's connection
- **THEN** the call SHALL return `{:error, {:forbidden, :principal_not_bound}}`

#### Scenario: Missing required field is rejected
- **WHEN** a caller invokes `query/2` with `caller_context: %{}` (missing `source`)
- **THEN** the call SHALL fail with a clear error indicating the required field

#### Scenario: Authn-disabled mode preserves placeholder behaviour
- **WHEN** `:authn_authz, :enabled` is `false` AND a caller invokes `query/2` with `caller_context: %{source: :ui, principal: nil}`
- **THEN** the call SHALL succeed AND no authorization rejection SHALL occur

### Requirement: Sink configuration validation

Every Sink SHALL declare a `config_schema/0` callback. At sink registration time the foundation SHALL validate the provided configuration against the schema before starting the sink process. Registration SHALL fail with a descriptive error if validation fails; an invalid Sink SHALL NOT be started.

#### Scenario: Valid sink config registers
- **WHEN** a sink is registered with configuration that matches its declared schema
- **THEN** the sink SHALL be started under the sink supervisor and SHALL appear in `list_sinks/0`

#### Scenario: Invalid sink config is rejected before start
- **WHEN** a sink is registered with configuration that fails its declared schema
- **THEN** registration SHALL fail with a descriptive error and SHALL NOT start any process under the sink supervisor

### Requirement: In-process span recording for `trace_span` signals

The foundation SHALL accept `:trace_span` signals carrying `trace_id`, `span_id`, and `parent_span_id` fields and SHALL deliver them to attached sinks. The foundation SHALL NOT propagate trace context across the binary transport in this change; cross-transport propagation is deferred to a future change. The schema for `:trace_span` signals SHALL include the propagation fields so that future change does not require a schema migration.

#### Scenario: In-process span is recorded
- **WHEN** code emits a `:trace_span` signal with `measurements: %{duration_us: 1234}`, `metadata: %{trace_id: "abcd", span_id: "ef01", parent_span_id: nil, operation: "wal.append"}`
- **THEN** the signal SHALL be delivered to attached sinks with all fields preserved

#### Scenario: Cross-transport propagation is explicitly out of scope
- **WHEN** an executor on a remote node produces work for the controller
- **THEN** in this change the controller-side span SHALL NOT automatically share a `trace_id` with any executor-side span; this remains an out-of-scope behaviour

### Requirement: Observability subsystem is configurable and disengageable

The foundation SHALL expose a runtime configuration key `:observability` with sub-keys controlling: ring-buffer retention duration, per-source maximum entry count, default overflow policy, default cardinality budget, PubSub-sink enable flag, and a master enable flag. When the master enable flag is `false`, the foundation supervisor SHALL NOT start sinks or attach the telemetry bridge or install the logger handler; service-API calls SHALL return a defined `:disabled` result.

#### Scenario: Default configuration starts every subsystem
- **WHEN** the application boots with default `:observability` configuration
- **THEN** the supervisor SHALL start with the registry, telemetry bridge, logger handler, ring-buffer sink, and PubSub sink (or test default) running

#### Scenario: Master-disable suppresses the subsystem
- **WHEN** the application boots with `:observability` configuration including `enabled: false`
- **THEN** no sinks SHALL be started, the telemetry bridge SHALL NOT be attached, the logger handler SHALL NOT be installed, and service-API calls SHALL return `{:error, :observability_disabled}`

### Requirement: Hot-path safety of producer emit

The producer-side emit function SHALL be allocation-light: it SHALL accept `measurements` and `metadata` as maps and SHALL NOT require the caller to construct keyword lists or proplists. When zero sinks are interested in a signal (no matching filters), the emit SHALL return without scheduling any process message or ETS write.

#### Scenario: Emit with no interested sinks is a no-op
- **WHEN** a signal is emitted on a source that no registered sink filters for
- **THEN** the emit SHALL not enqueue any sink mailbox message and SHALL not insert into any ETS table

#### Scenario: Emit accepts map measurements without conversion
- **WHEN** a producer emits with `measurements: %{count: 1}` and `metadata: %{operative: "r"}`
- **THEN** the signal SHALL be delivered without the producer being required to convert the maps to lists or keyword lists
