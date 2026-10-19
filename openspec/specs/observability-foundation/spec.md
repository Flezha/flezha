# observability-foundation Specification

## Purpose

The observability-foundation capability provides the Flezha Controller's first-class, pluggable subsystem that unifies metrics, events, logs, and in-process trace spans behind one contract. It defines a single signal envelope (`kind` ∈ `:metric | :event | :log | :trace_span`), a `Source` behaviour for producers and a `Sink` behaviour for consumers, an ETS-backed `Registry` enumerating live sources and sinks with per-source cardinality budgets, and a fan-out bus that is non-blocking on the producer side (default policy: drop-with-counter — observability never back-pressures the hot path). The capability ships two default sinks (an in-memory per-source ring buffer queryable via the service API, and a `Phoenix.PubSub` fan-out for a future LiveView UI), a `:logger` handler that bridges Elixir log records onto the bus, and a single dispatching `:telemetry` handler that lifts the existing `[:flezha, ...]` events onto the bus without modifying any call-site. The service-API module `Flezha.Service.Observability` is callable from both the CLI (via `Flezha.Service.CliManager`) and the planned Phoenix UI directly, with a `caller_context` argument on every public function so a future authorization capability can plug in without a breaking API change. External exporters, built-in VM/host probes, and cross-transport trace-context propagation are explicitly out of scope and deferred to follow-up capabilities.
## Requirements
### Requirement: Unified signal taxonomy

The observability subsystem SHALL define exactly one signal envelope structure used by all producers and all consumers. The structure SHALL include the fields `kind`, `source`, `name`, `measurements`, `metadata`, and `ts`. The field `kind` SHALL be one of the atoms `:metric`, `:event`, `:log`, or `:trace_span`. The structure SHALL be exposed as a module struct under the foundation namespace and SHALL be the only on-bus signal shape.

#### Scenario: Constructing a metric signal
- **WHEN** a producer constructs a signal with `kind: :metric`, `source: :flezha_state`, `name: :wal_backpressure`, `measurements: %{count: 1, bytes: 16_384}`, `metadata: %{relay: "orders", tasklet: 3}`
- **THEN** the resulting struct SHALL be accepted by the bus without error and SHALL carry a monotonic-time `ts` field set automatically when not supplied

#### Scenario: Rejecting an unknown signal kind
- **WHEN** a producer attempts to construct a signal with `kind: :unknown_kind`
- **THEN** the construction SHALL fail with an `ArgumentError` before the signal reaches the bus

#### Scenario: All four kinds are accepted
- **WHEN** signals of each kind `:metric`, `:event`, `:log`, `:trace_span` are emitted in turn
- **THEN** each SHALL be delivered to attached sinks with its `kind` field preserved exactly

### Requirement: Source contract

The observability subsystem SHALL define a `Source` contract under which any producer of signals can register itself. The contract SHALL support two registration flavours: a **declarative** flavour in which the producer calls a public emit function directly, and a **process-backed** flavour in which the producer is a GenServer started under the source supervisor. Every registered source SHALL declare its name, the set of signal kinds it emits, and a per-source cardinality budget. A registration MAY also declare a plain `title`, a one-line `description`, an `area` (one of `:relays`, `:storage`, `:plugins`, `:controller`, `:machine`) and `signals`, a list of `%{name, kind, description}` naming each signal the source can emit, where each signal MAY also carry a plain `label`, an `outcome` of `:normal` or `:failure`, and a `sentence` template. A template is text whose placeholders are `{key}`, interpolating a label or measurement, and `{key:noun}`, interpolating a count with a noun; a key is a lowercase identifier, and any other `{` or `}` makes the template malformed. The registry SHALL refuse a malformed value for any of these keys, a malformed title as `{:invalid_title, title}` and a malformed signal key as `{:invalid_signals, signals}`, SHALL keep them, and `list_sources/0` SHALL answer them. `Flezha.Observability.Source.signals/1` SHALL build the declarations from `name: {kind, meaning}`, whose label is the humanized name and whose outcome is `:normal`, or from `name: {kind, words}`, the words being `meaning:` and optionally `label:`, `outcome:` and `sentence:`.

The `Flezha.Observability.Source` behaviour SHALL declare `config_schema/0` and no other callback. It SHALL NOT declare `init/1`: a declarative source owns no process and has no `init/1` to offer, and a process-backed source is a GenServer whose `init/1` is GenServer's — a module declaring both behaviours would name that callback twice, which the compiler refuses as conflicting behaviours. A behaviour a whole flavour cannot declare is not a contract that flavour can be held to.

#### Scenario: Declarative source registration
- **WHEN** code registers a declarative source with name `:my_source`, kinds `[:metric, :event]`, and cardinality budget `1_000`
- **THEN** the source SHALL appear in `list_sources/0` and emits from `:my_source` SHALL be accepted by the bus

#### Scenario: Process-backed source registration
- **WHEN** code registers a module implementing the process-backed `Source` behaviour
- **THEN** the source supervisor SHALL start the module as a child under a one-for-one strategy, and the source SHALL appear in `list_sources/0` with its status

#### Scenario: Source emits a signal kind it did not declare
- **WHEN** a source registered with kinds `[:metric]` attempts to emit a `:log` signal
- **THEN** the emit SHALL be dropped, a `:source_kind_violation` event SHALL be emitted on the bus with the source name, and a per-source violation counter SHALL increment

#### Scenario: The contract is declarable beside GenServer
- **WHEN** a process-backed source declares both `GenServer` and `Flezha.Observability.Source`
- **THEN** the module SHALL compile with no warning, and `mix compile --warnings-as-errors` SHALL succeed

#### Scenario: A source registers its description, area and signals
- **WHEN** code registers `:my_source` with `description: "Orders received"`, `area: :relays` and `signals: [%{name: :received, kind: :metric, description: "An order arrived"}]`
- **THEN** `list_sources/0` SHALL answer the entry with those three values

#### Scenario: A malformed descriptive key is refused
- **WHEN** code registers a source with `area: :elsewhere`, or a `signals` entry without a description, or a non-binary `description`
- **THEN** `register_source/1` SHALL answer an error naming the key and no source SHALL be registered

#### Scenario: A source registers a title and a signal's label, outcome and sentence
- **WHEN** code registers `:my_source` with `title: "Orders"` and `signals: Source.signals(received: {:metric, label: "Received", outcome: :normal, meaning: "An order arrived.", sentence: "{shop} received {count:order}"})`
- **THEN** `list_sources/0` SHALL answer the title, and the signal with that label, outcome, sentence and its meaning as `description`

#### Scenario: The short form takes defaults
- **WHEN** code declares `Source.signals(order_refused: {:event, "An order was refused."})`
- **THEN** the declaration SHALL carry the label `"Order refused"`, the outcome `:normal` and no sentence

#### Scenario: A malformed title, label, outcome or sentence is refused
- **WHEN** code registers a source with a blank `title`, or a signal with a blank `label`, an `outcome` of `:warning`, or a `sentence` of `"{shop received"`
- **THEN** `register_source/1` SHALL answer an error naming the key and no source SHALL be registered

### Requirement: Sink contract

The observability subsystem SHALL define a `Sink` contract under which any consumer of signals can be registered. Each Sink SHALL be a process that receives each accepted signal as the message `{:signal, signal}` and SHALL export `config_schema/0` and `filter/0`, which the registry consults before the sink process is started. Each registered Sink SHALL run as a supervised process under a dedicated DynamicSupervisor using a one-for-one strategy so that one Sink crash does not affect peer Sinks or producers.

The contract names the message rather than a callback because that is the mechanism the bus uses: the bus sends `{:signal, signal}` to the sink's pid, asynchronously under the `:drop` overflow policy and as a synchronous call under `:block`. A behaviour callback in place of the message would be a second delivery mechanism, and a sink that has to serve a mailbox anyway cannot be reached by one.

#### Scenario: Sink receives every signal that passes its filter
- **WHEN** a sink is registered with filter `kinds: [:metric]` and a `:metric` signal is emitted
- **THEN** the sink process SHALL receive `{:signal, signal}` exactly once with that signal

#### Scenario: Sink filter excludes non-matching signal
- **WHEN** a sink is registered with filter `kinds: [:metric]` and a `:log` signal is emitted
- **THEN** the sink process SHALL NOT receive that signal

#### Scenario: Crashing sink does not affect peer sinks
- **WHEN** sink A is registered to crash on every signal, sink B is registered as well, and a signal is emitted
- **THEN** sink A SHALL crash and be restarted by its supervisor, and sink B SHALL receive the signal exactly once

#### Scenario: Crashing sink does not affect producers
- **WHEN** a sink crashes during dispatch of a signal
- **THEN** the producer's emit call SHALL return normally and SHALL NOT raise

#### Scenario: A declared contract has no implementation behind it
- **WHEN** the `Sink` behaviour declares a callback that no module in the tree invokes
- **THEN** that callback SHALL be removed rather than left declared, because a contract nothing enforces teaches a mechanism that does not exist and every implementer writes a stub for it

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
- **WHEN** a sink is intentionally slow (it parks on receiving `{:signal, signal}`) and a producer emits 1,000 signals in rapid succession
- **THEN** the producer SHALL complete all 1,000 emits in bounded time and SHALL NOT exceed the time taken by 1,000 emits when the slow sink is absent by more than a fixed small constant (registry lookup + `Process.info/2` check)

#### Scenario: Block policy is opt-in
- **WHEN** a sink is registered with `overflow: :block` and its mailbox is at watermark
- **THEN** producer emits targeted at the sink SHALL synchronously wait until the sink accepts the message; no drop counter SHALL increment for that sink

#### Scenario: The registry itself is the producer
- **WHEN** the observability registry needs a signal emitted as a consequence of a call it is handling
- **THEN** the emit SHALL happen in the calling producer's process and not inside the registry's own `handle_call/3`, because a sink registered with `overflow: :block` would otherwise hold the registry for its whole `block_timeout_ms` and every source registration, sink registration and status change would queue behind it

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

The foundation SHALL attach a single dispatching `:telemetry` handler at supervisor start that maps each known `[:flezha, ...]` event onto an observability signal. The mapping SHALL have three halves: a hand-kept lifecycle half defined inside the bridge module, which SHALL include at minimum the events `[:flezha, :state, :wal, :backpressure]`, `[:flezha, :state, :fsync, :tick]`, `[:flezha, :boot, :recovery_complete]`, `[:flezha, :shutdown, :requested]`, `[:flezha, :plugin_package, :loaded]`, `[:flezha, :plugin_package, :refused]`, `[:flezha, :plugin, :websocket, :idle_reconnect]`, `[:flezha, :plugin, :websocket, :resubscribe]`, `[:flezha, :state, :seq_gap]`, `[:flezha, :state, :output_stall]`, `[:flezha, :state, :compaction_timeout]`, `[:flezha, :state, :wal_realign]`, and `[:flezha, :stream, :drop]`; and a half DERIVED from the convergence loop's detector table, mapping every event a detector entry declares (including `[:flezha, :health, :verdict]`, executed on every verdict-state transition with metadata `kind`, `id`, `state` and `reason`) so a detector's event is bridged by its declaration and never by a bridge edit; and a third half DERIVED at bridge start from the providers installed packages add to the provider table: for every such provider, `[:flezha, :plugin, <provider name>, verb]` for each of the declared verbs (`shipped`, `retried` and `overflowed` as metrics, `failed` as an event) and for each verb the provider declares as its own, sourced `flezha_<provider name>` and named by the verb, where the provider name is the one its declaration answers and its events are written under. A first-party provider's events SHALL be hand-kept lines, so a core with no package derives nothing and registers no source for it. The third half SHALL cost an event no more than the other two: it is built once at bridge start, each of its events is handled with its own line, the bridge's handler of the other halves carries nothing more per event than before, and it is not part of the merged mapping the tree walk below judges. A hand-kept line SHALL win over a derived one of either kind. The two package events SHALL map to the source `:flezha_plugin_packages` with the names `:loaded` and `:refused`. A relay's deliberate start and its stop or completion, `[:flezha, :relay, :started]` and `[:flezha, :relay, :stopped]`, SHALL map to the source `:flezha_relay` with the names `:relay_started` and `:relay_stopped`, and each SHALL reach every sink that accepts it, the controller hooks' sink among them, from its one `:telemetry.execute`. Existing call-sites SHALL NOT be modified. Every hand-kept line SHALL carry a one-line meaning for its signal, and every bridged source SHALL have an area and a description in the bridge, so a line or a source cannot be added without one; the bridge SHALL register each bridged source with its area, its description and the signals its lines name, each with its meaning. A detector table entry SHALL carry each declared event's meaning, and the shared provider verbs SHALL carry one meaning each; a provider's own verb MAY carry one, and one without is registered with no meaning of its own.

Every `[:flezha, ...]` event NAME written anywhere in the controller SHALL be a key of the merged mapping or a member of an explicit unexported list inside the bridge module. The judged unit SHALL be the name wherever it is written, not only the `:telemetry.execute` call, because an event name may be assembled into a term in one module and executed in another, or assembled from a provider's own identifier. Each unexported entry SHALL name a reason that is a PROPERTY of the event rather than the change that owes it a sink: a reason naming a change expires when that change lands, and the list then passes while stating nothing. The unexported list is a ratchet: its length SHALL be pinned by the suite so it never grows, and an event name that is neither mapped nor unexported SHALL fail the suite naming where it is written and the event. A `:telemetry.execute` whose event argument the walk cannot resolve SHALL be a named entry in a pinned forward list, so a name the walk cannot read is refused rather than silently omitted.

The mapping SHALL also name, in the other direction, every mapped event nothing in the controller writes any more, each with the reason its line stays, and the suite SHALL assert that set exactly so a dead key can neither appear nor be resurrected unnoticed.

#### Scenario: Bridged event becomes a signal
- **WHEN** code calls `:telemetry.execute([:flezha, :state, :wal, :backpressure], %{bytes: 1024}, %{relay: "r1"})`
- **THEN** a signal with `kind: :event`, `source: :flezha_state`, `name: :wal_backpressure`, `measurements.bytes: 1024`, `metadata.relay: "r1"` SHALL appear on the bus

#### Scenario: A package the boot refused is bridged
- **WHEN** the controller executes `[:flezha, :plugin_package, :refused]` with metadata `id: "acme"` and `code: "plugin.library_conflict"`
- **THEN** a signal with `kind: :event`, `source: :flezha_plugin_packages`, `name: :refused` carrying that metadata SHALL appear on the bus

#### Scenario: A relay's start and stop reach every sink
- **GIVEN** a controller plugin taking `on_relay_started` and `on_relay_stopped`
- **WHEN** a relay is started and then stopped
- **THEN** signals with `source: :flezha_relay` and the names `:relay_started` and `:relay_stopped` SHALL appear in the ring buffer, and the plugin SHALL receive each hook event exactly once

#### Scenario: Websocket re-subscribe event is bridged
- **WHEN** code calls `:telemetry.execute([:flezha, :plugin, :websocket, :resubscribe], %{data_silence_ms: 120_000}, %{plugin_id: "wss://feed.example.com"})`
- **THEN** a signal sourced from the websocket provider carrying `measurements.data_silence_ms: 120_000` and `metadata.plugin_id: "wss://feed.example.com"` SHALL appear on the bus

#### Scenario: Persistence stall/gap events are bridged
- **WHEN** code calls `:telemetry.execute([:flezha, :state, :output_stall], %{stalled_ms: 60_000, last_seq: 100}, %{relay: "btc_feed", tasklet: "ohlc", key: "1m"})`
- **THEN** a signal sourced from the state subsystem carrying the stalled duration, last seq, and Cell metadata SHALL appear on the bus
- **AND** the same bridging SHALL hold for `[:flezha, :state, :seq_gap]` and `[:flezha, :state, :compaction_timeout]`

#### Scenario: Bridge failure does not crash producer
- **WHEN** the bridge's mapping function raises for a specific event
- **THEN** the original `:telemetry.execute` call SHALL return normally, a `:dispatcher_failure_count` counter SHALL increment, and an error SHALL be logged once with the offending event name

#### Scenario: A detector's declared event is bridged with no bridge edit
- **WHEN** the control-plane watchdog executes `[:flezha, :runner, :liveness_timeout]`, or the plugin fact seam executes `[:flezha, :plugin, :health, :fail_open]`, or the watchdog executes `[:flezha, :state, :input_silence]`
- **THEN** a signal for it SHALL appear on the bus, mapped from the detector table's declaration, and the bridge module's hand-kept half SHALL hold no line for it

#### Scenario: A verdict transition is bridged
- **WHEN** a relay's verdict record changes state from `healthy` to `halted`
- **THEN** a signal named `verdict` sourced from the health seam SHALL appear on the bus with metadata `kind: :relay`, the relay id, `state: :halted` and the halt reason, and no signal SHALL appear while the state holds

#### Scenario: An event nobody bridges fails the suite
- **WHEN** a `:telemetry.execute` of a `[:flezha, ...]` event is added to the controller and the event is neither a key of the merged mapping nor a member of the unexported list
- **THEN** the suite SHALL fail naming the file, the line and the event, and adding the event to the unexported list past its pinned length SHALL fail the suite too

#### Scenario: An event name assembled away from its execute call is still judged
- **WHEN** an event name is written as a literal in one module, carried in a term, and executed by a `:telemetry.execute` in another, or is assembled from a provider's identifier and a declared verb
- **THEN** the suite SHALL judge that name against the merged mapping and the unexported list exactly as if it were written at the execute call, and a segment the walk cannot resolve to a literal SHALL fail by name rather than being dropped from the set

#### Scenario: An unexported entry naming a change instead of a property
- **WHEN** an event is added to the unexported list whose reason is the change that owes it a sink, rather than a property of the event
- **THEN** the reason SHALL be rejected: an entry SHALL carry one of the bridge's declared reason classes, and the suite SHALL fail for an entry carrying none

#### Scenario: A mapped event nothing writes any more
- **WHEN** the last producer of a mapped event is deleted, leaving its mapping line in place
- **THEN** the suite SHALL fail unless that event is named in the bridge's unwritten list with the reason its line stays, and SHALL fail again if a producer for a listed event reappears

#### Scenario: A package provider's event is exported with no bridge edit
- **GIVEN** a package whose provider `acme_sink` declares `telemetry: [rebalanced: :event]`, loaded at boot
- **WHEN** its worker executes `[:flezha, :plugin, :acme_sink, :shipped]` and `[:flezha, :plugin, :acme_sink, :rebalanced]`
- **THEN** a metric signal named `shipped` and an event signal named `rebalanced`, both sourced `flezha_acme_sink`, SHALL appear on the bus, and the bridge module SHALL hold no line naming `acme_sink`

### Requirement: Service-API surface

The foundation SHALL provide a service-API module `Flezha.Service.Observability` callable from both the CLI (via `Flezha.Service.CliManager`) and the future Phoenix UI directly, without routing through `CliManager`. The module SHALL expose at minimum `subscribe/2`, `query/2`, `list_sources/0`, `list_sinks/0`, `enable_source/1`, `disable_source/1`. Every public function SHALL accept a `caller_context` argument as its first or last positional argument AND SHALL be authorized through `Flezha.AuthZ.authorize/3` when `:authn_authz, :enabled` is `true`. Read tasklets (`list_sources`, `list_sinks`, `query`, `subscribe`) SHALL require at least the `viewer` role on the caller's bound namespace; mutate tasklets (`enable_source`, `disable_source`) SHALL require at least `ops`. Read tasklets SHALL filter results to the caller's bound namespace; results for other namespaces SHALL NOT be returned.

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
- **THEN** the call SHALL return `{:error, %Flezha.Error{code: "authz.role_insufficient"}}` AND no source SHALL be disabled

#### Scenario: Cross-namespace query is filtered
- **WHEN** `:authn_authz, :enabled` is `true` AND a caller bound to namespace `team-orders` invokes `query/2` against a source carrying signals from both `team-orders` and `team-payments`
- **THEN** the returned signal list SHALL contain only signals whose `metadata.namespace` is `team-orders`

### Requirement: Authorization seam

The service API SHALL accept a `caller_context` argument on every public function and the context SHALL include at minimum a `source` field whose value is one of `:cli`, `:ui`, `:sdk`, or `:internal`. When `:authn_authz, :enabled` is `true`, the `caller_context` from a `:cli`, `:ui`, or `:sdk` source SHALL also include a verified `:principal` bound by the authentication layer at the connection boundary; service implementations SHALL call `Flezha.AuthZ.authorize/3` at the top of every public function before any side effect. Service callers MUST NOT forge a `:principal`; calls whose presented `:principal` was not bound to the caller's connection by the authentication layer SHALL be rejected with `{:error, %Flezha.Error{code: "authz.principal_not_bound"}}` (the policy point itself answers `{:forbidden, :principal_not_bound}`). `:internal` callers SHALL pass `source: :internal` and SHALL NOT carry a `:principal`. When `:authn_authz, :enabled` is `false` (the upgrade-compatibility mode), the prior behaviour SHALL be preserved: no `:principal` is required and `Flezha.AuthZ.authorize/3` returns `:ok` unconditionally.

#### Scenario: Caller context with verified principal succeeds
- **WHEN** `:authn_authz, :enabled` is `true` AND a `:cli` caller invokes `query/2` with `caller_context: %{source: :cli, principal: "alice@example.com", namespace: "_system", role: :viewer}` AND the principal was bound by the CLI handshake
- **THEN** the call SHALL succeed if `Flezha.AuthZ.authorize/3` returns `:ok` for action `:observability.query` on scope `%{namespace: "_system"}`

#### Scenario: Forged principal is rejected
- **WHEN** `:authn_authz, :enabled` is `true` AND a caller invokes `query/2` with a `:principal` that the authentication layer did not bind to this caller's connection
- **THEN** the call SHALL return `{:error, %Flezha.Error{code: "authz.principal_not_bound"}}`

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
- **WHEN** code emits a `:trace_span` signal with `measurements: %{duration_us: 1234}`, `metadata: %{trace_id: "abcd", span_id: "ef01", parent_span_id: nil, tasklet: "wal.append"}`
- **THEN** the signal SHALL be delivered to attached sinks with all fields preserved

#### Scenario: Cross-transport propagation is explicitly out of scope
- **WHEN** a runner on a remote node produces work for the controller
- **THEN** in this change the controller-side span SHALL NOT automatically share a `trace_id` with any runner-side span; this remains an out-of-scope behaviour

### Requirement: Observability subsystem is configurable and disengageable

The foundation SHALL expose a runtime configuration key `:observability` with sub-keys controlling: ring-buffer retention duration, per-source maximum entry count, default overflow policy, default cardinality budget, PubSub-sink enable flag, and a master enable flag. When the master enable flag is `false`, the foundation supervisor SHALL NOT start sinks or attach the telemetry bridge or install the logger handler; service-API calls SHALL return a defined `:disabled` result.

#### Scenario: Default configuration starts every subsystem
- **WHEN** the application boots with default `:observability` configuration
- **THEN** the supervisor SHALL start with the registry, telemetry bridge, logger handler, ring-buffer sink, and PubSub sink (or test default) running

#### Scenario: Master-disable suppresses the subsystem
- **WHEN** the application boots with `:observability` configuration including `enabled: false`
- **THEN** no sinks SHALL be started, the telemetry bridge SHALL NOT be attached, the logger handler SHALL NOT be installed, and service-API calls SHALL return `{:error, %Flezha.Error{code: "observability.disabled"}}`

### Requirement: Hot-path safety of producer emit

The producer-side emit function SHALL be allocation-light: it SHALL accept `measurements` and `metadata` as maps and SHALL NOT require the caller to construct keyword lists or proplists. When zero sinks are interested in a signal (no matching filters), the emit SHALL return without scheduling any process message or ETS write.

#### Scenario: Emit with no interested sinks is a no-op
- **WHEN** a signal is emitted on a source that no registered sink filters for
- **THEN** the emit SHALL not enqueue any sink mailbox message and SHALL not insert into any ETS table

#### Scenario: Emit accepts map measurements without conversion
- **WHEN** a producer emits with `measurements: %{count: 1}` and `metadata: %{relay: "r"}`
- **THEN** the signal SHALL be delivered without the producer being required to convert the maps to lists or keyword lists

### Requirement: A name that cannot be rendered in the exposition SHALL be refused where it is declared

A source name and a `:metric` signal's name SHALL match `[a-zA-Z_][a-zA-Z0-9_]*`
and SHALL NOT begin with two underscores, and a name that does not SHALL be
refused at the point it is declared — never rewritten, and never discovered at
scrape time.

These two names are joined into the metric name an exporter renders, and a metric
name outside that grammar fails the **whole** Prometheus scrape rather than one
series: a single space, dot or leading digit hides every metric the controller
exports. The grammar SHALL be stated in exactly one place and read by the
registration path and by the exporter alike, so that what is refused and what is
rendered cannot disagree.

A source name SHALL be refused by `Registry.register_source/1`, answering an error
that names the offending name, so a source with an unrenderable name never becomes
registered. A `:metric` signal's name SHALL be refused by the signal constructor,
raising before the signal reaches the bus, for the same reason an unknown `kind`
does — both are developer-chosen atoms, wrong at the call site, and the call site
is where the author can see it. A signal of any other kind SHALL be unaffected:
only a `:metric` name is rendered as a metric name.

This SHALL NOT extend to a signal's metadata keys. A metadata key is a runtime term
forwarded verbatim from third-party telemetry, and an exporter SHALL keep
sanitizing it rather than refusing it, because refusing there would drop a caller's
dimension with nothing in the output to say so.

#### Scenario: A source name outside the grammar is refused at registration

- **WHEN** a source is registered under a name containing a space or a dot
- **THEN** `register_source/1` SHALL answer an error naming that name, and the source SHALL NOT appear in `list_sources/0`

#### Scenario: A metric name outside the grammar is refused at construction

- **WHEN** a producer constructs a signal with `kind: :metric` and a name beginning with a digit
- **THEN** the construction SHALL fail with an `ArgumentError` before the signal reaches the bus

#### Scenario: The grammar binds metric names only

- **WHEN** a producer constructs an `:event`, `:log` or `:trace_span` signal with a name outside the grammar
- **THEN** the construction SHALL succeed, because that name is never rendered as a metric name

#### Scenario: A legal name is untouched

- **WHEN** a source `:flezha_state` is registered and a signal `kind: :metric, name: :wal_backpressure` is constructed
- **THEN** both SHALL succeed, and the exposition SHALL render `flezha_state_wal_backpressure`

#### Scenario: A metadata key is still sanitized rather than refused

- **WHEN** a metric signal carries a metadata key outside the grammar, such as `"http.status_code"`
- **THEN** the emit SHALL succeed and the exporter SHALL render a sanitized label name, exactly as before

### Requirement: A counted fact SHALL be declared once and every store derived from the declaration

A process-local counter table SHALL be declared as a list of named slots, and its indices, accessors, snapshot and collapse rules SHALL be generated from that list rather than written beside it.

Hand-numbering a slot is forbidden: a module SHALL NOT carry an attribute or a
function clause that assigns an integer position to a slot name, because the
number and the name then have to be kept in step by hand and the compiler cannot
see the drift. A slot list that already derives its indices satisfies this
requirement; what changes is that one mechanism serves every store instead of
each store carrying its own copy.

A row SHALL carry the slot's kind — a monotonic count, a set gauge, or an instant
— because the kind is what decides how the slot is reduced across the processes
that hold it, and a reduction inferred from a slot's spelling is a guess. A
counter that is a durable ledger read by policy, rather than instrumentation
that changes no behaviour, is not a counter table under this requirement and
SHALL NOT be folded into one.

A single-counter reference — a sequence, an index, an occupancy flag — has no layout
to declare and is exempt; the exemption SHALL be written down beside the
reference rather than left to be inferred.

#### Scenario: A store declares slots, not indices

- **GIVEN** a module owning a process-local counter table
- **WHEN** it declares its slots
- **THEN** its indices, `bump`, `set`, `get` and `snapshot` SHALL be generated from the declaration
- **AND** the module SHALL carry no integer position for any slot name

#### Scenario: A new slot costs one row

- **WHEN** a slot is appended to a declared table
- **THEN** it SHALL be readable through the table's snapshot with no other edit to the module

#### Scenario: A durable ledger is not folded

- **WHEN** a counter's values are read as an authorization input and persisted per mutation
- **THEN** it SHALL remain its own module and SHALL NOT be expressed as an instrumentation table

### Requirement: Runner sessions SHALL be published as observability signals

The controller SHALL emit `[:flezha, :runner, :connected]` when a runner session is admitted and
`[:flezha, :runner, :disconnected]` when one ends, each with measurements `%{count: 1}` and
metadata `%{namespace:, runner:}`, beside the event bus dispatches of the same two facts. The
telemetry bridge SHALL lift them onto the `:flezha_runner` source as the events
`runner_connected` and `runner_disconnected`, so a consumer on another node can follow runner
sessions through `Observability.tail/2`.

#### Scenario: A runner connects

- **WHEN** a runner session is admitted in namespace `default`
- **THEN** a `:flezha_runner` signal named `runner_connected` with metadata namespace `default`
  and the session's runner id reaches the ring buffer

#### Scenario: A runner disconnects

- **WHEN** that session ends
- **THEN** a `:flezha_runner` signal named `runner_disconnected` naming the same namespace and
  runner reaches the ring buffer

### Requirement: The controller's status SHALL carry the clock signals are stamped with

`Flezha.Service.Controller.status/1` SHALL answer `monotonic_us`, the node's
`System.monotonic_time(:microsecond)`, so a consumer on another node can age a signal's `ts`
against the controller's own clock.

#### Scenario: Ageing a signal

- **WHEN** a consumer reads `status/1` after reading a signal
- **THEN** `monotonic_us` minus the signal's `ts` is the signal's age in microseconds

### Requirement: Every source core registers is described

Every observability source the controller registers itself SHALL declare a non-empty title, a non-empty description, an area, and every signal it can emit with its kind, a non-empty label, an outcome and a non-empty meaning. A signal that reports something that did not happen as asked (a refusal, a failure or timeout, an overflow or drop, a damaged record, a stall, or a health detector's finding) SHALL declare the outcome `:failure`; retries, waits and resets SHALL declare `:normal`. The shared provider verbs `failed` and `overflowed` SHALL be failures and `shipped` and `retried` normal; a provider's own verb MAY declare its label, outcome and sentence, and one that does not SHALL be normal. Every sentence template a core source declares SHALL parse, and a bridged line's template SHALL name only keys that the line's emit sites write, which the suite SHALL check wherever it can read a site's keys. A source that nothing in the controller can make emit SHALL NOT be registered. The suite SHALL assert both against every source the booted registry holds, and against the declared registration of every source a test boot does not start (the built-in pollers, the audit log, export and authorization).

#### Scenario: A booted controller's sources are described
- **WHEN** the controller has booted with default configuration and `Registry.list_sources/0` is read
- **THEN** every entry SHALL carry a non-empty title, a non-empty description and an area, and every signal it declares SHALL carry a kind, a non-empty label, an outcome and a non-empty meaning

#### Scenario: Sources nothing writes are gone
- **WHEN** `Registry.list_sources/0` is read after boot
- **THEN** it SHALL NOT include `:flezha_reconcile`, `:flezha_leadership` or `:flezha_bounded_mailbox`

#### Scenario: A template names a key its emit site does not write
- **WHEN** a bridged line declares the sentence `"{runner} connected to {region}"` and its emit site writes `namespace` and `runner` but no `region`
- **THEN** the suite SHALL fail naming the line and the missing key

### Requirement: The source listing carries recent activity

`Flezha.Service.Observability.list_sources/1` SHALL answer, for each source, `recent: %{window_ms:, signals: %{name => %{count:, last_age_ms:}}}`, computed from the ring buffer over its retention window: per signal name, how many signals the buffer holds and how long ago, by the controller's monotonic clock, the newest one was emitted. The counts SHALL include only signals the caller could read through `query/2`. `recent` SHALL be `nil` when the ring-buffer sink is not running.

#### Scenario: A source emitted recently
- **WHEN** `:flezha_file_sink` emitted `flushed` three times in the last minute and an admin caller lists sources
- **THEN** its entry SHALL carry `recent.signals.flushed.count == 3` and a `last_age_ms` no larger than the time since the last emit, and `recent.window_ms` SHALL equal the ring buffer's retention

#### Scenario: Counts honour the caller's namespace
- **WHEN** a caller bound to namespace `team-a` lists sources and the buffer holds signals of `team-a`, of `team-b` and without a namespace
- **THEN** the counts SHALL include the `team-a` signals and those without a namespace, and SHALL NOT include the `team-b` signals

#### Scenario: No ring buffer
- **WHEN** the ring-buffer sink is not running
- **THEN** every entry SHALL carry `recent: nil`

