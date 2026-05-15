# relay-extensions Specification

## Purpose

The relay-extensions capability is the typed extension-point system exposed by relay hosts (e.g. `Flezha.Relay.Overwatch`). Each host declares its extension points once in a module-level catalog; plugins are represented as a unified `%Flezha.Plugin{}` struct carrying a `subscriptions` list and attach to those extensions as data, and the engine dispatches each extension through `Flezha.Plugin.Extension.Table` — a per-host registry that holds subscriptions, pre-composed dispatch chains keyed by extension kind (`:fold`, `:transform`, `:fanout`, `:request`, `:timer`), an active-extensions bitset, and all subscriber lifecycle state (monitor refs, timer refs, restart bookkeeping, `PluginManager` registrations). The capability defines how plugins declare subscriptions, how the table compiles dispatch chains per extension kind, how the active-bitset fast path makes unused extensions zero-cost, how lifecycle (subscribe / unsubscribe / teardown / restart / handover) is handled uniformly, and what guarantees hold across the extension points a host publishes. Adding a new extension point becomes a catalog entry plus a single `Extension.run/3` call at the dispatch site.

## Requirements

### Requirement: Extension-point catalog

The system SHALL declare every extension point exposed by a host module (e.g. `Flezha.Relay.Overwatch`) in a single module-level catalog. Each catalog entry MUST specify the extension name (atom), its dispatch kind (one of `:fold`, `:transform`, `:fanout`, `:request`, `:timer`), and its context-struct module. The catalog SHALL be the sole source of truth for which extension points exist on that host; no runtime registration of new extension names is permitted.

#### Scenario: Overwatch declares its catalog
- **WHEN** `Flezha.Relay.Overwatch` is compiled
- **THEN** the module SHALL declare an `@extensions` attribute containing at least these entries: `{:on_relay_init, :fold, Flezha.Plugin.Extension.Ctx.Init}`, `{:on_input, :transform, Flezha.Plugin.Extension.Ctx.IO}`, `{:on_output, :transform, Flezha.Plugin.Extension.Ctx.IO}`, `{:on_interval, :timer, Flezha.Plugin.Extension.Ctx.Tick}`, `{:on_message, :request, Flezha.Plugin.Extension.Ctx.Msg}`, `{:on_snapshot, :fanout, Flezha.Plugin.Extension.Ctx.Snapshot}`, `{:on_terminate, :fanout, Flezha.Plugin.Extension.Ctx.Terminate}`

#### Scenario: Subscribing to an undeclared extension fails
- **WHEN** a subscription attempts to attach to an extension name not present in the host's catalog
- **THEN** `Flezha.Plugin.Extension.Table.subscribe/3` MUST return `{:error, {:unknown_extension, name}}` and state MUST NOT be mutated

### Requirement: Unified plugin struct with subscriptions

The system SHALL represent a plugin as a single `%Flezha.Plugin{}` struct with fields `id` (string), `provider` (a module implementing the relevant extension callbacks), `opts` (a map of provider configuration), and `subscriptions` (a list of `{extension_name, subscription_opts}` tuples). A single `%Flezha.Plugin{}` MAY subscribe to multiple extension points.

#### Scenario: Plugin subscribes to multiple extensions
- **WHEN** a plugin is constructed with `subscriptions: [{:on_input, [position: 0]}, {:on_interval, [interval_ms: 5_000]}, {:on_terminate, []}]` and enabled via `Extension.Table.subscribe/3`
- **THEN** the provider module's `on_input/2`, `on_interval/2`, and `on_terminate/2` callbacks MUST each be invoked on the corresponding dispatch event
- **AND** a single `unsubscribe/2` MUST remove all three subscriptions atomically

#### Scenario: Provider missing a subscribed callback
- **WHEN** a plugin subscribes to `{:on_input, _}` but its `provider` module does not export `on_input/2`
- **THEN** `Extension.Table.subscribe/3` MUST return `{:error, {:missing_callback, :on_input}}` and state MUST NOT be mutated

### Requirement: Fold-kind dispatch

The system SHALL dispatch `:fold`-kind extensions by invoking each subscriber's callback in position order, threading the accumulator returned by each call into the next. A callback MAY return `{:ok, ctx}` to continue the fold or `{:error, reason}` to halt the chain. A halt MUST return `{:error, reason}` from `Extension.run/3` without invoking further subscribers.

#### Scenario: Ordered fold with multiple subscribers
- **WHEN** three subscribers at positions 0, 1, 2 return `{:ok, ctx_a}`, `{:ok, ctx_b}`, `{:ok, ctx_c}` respectively
- **THEN** `Extension.run/3` MUST invoke them in order 0 → 1 → 2 passing each preceding result as the ctx input to the next, and MUST return `{:ok, ctx_c}`

#### Scenario: Fold halts on error
- **WHEN** a subscriber at position 1 returns `{:error, :db_down}` and a third subscriber exists at position 2
- **THEN** the position-2 subscriber MUST NOT be invoked and `Extension.run/3` MUST return `{:error, :db_down}`

### Requirement: Transform-kind dispatch

The system SHALL dispatch `:transform`-kind extensions by invoking each subscriber in position order. A callback returning `{:ok, ctx}` continues the chain; `{:skip, reason}` halts and returns `{:skip, reason}` from `Extension.run/3`; `{:error, reason}` logs the error and continues with the PRIOR ctx (preserving the existing IO-binding behaviour where errors do not corrupt the in-flight payload).

Transform subscribers SHALL be implemented such that their callbacks return in O(1) wall-clock time relative to any external resource (broker round-trips, database queries, downstream HTTP calls, etc.). The chain runner is invoked inline from the host process's message handler — synchronous waits on external resources within a transform subscriber inflate the host's `:message_queue_len` under sustained input above the resource's drain rate. Subscribers that need to perform external I/O SHALL admit work into a dedicated worker (per the `kafka-producer-plugin`'s `cast_enqueue` pattern) and return immediately, surfacing failures through telemetry rather than the chain's return value.

#### Scenario: Transform chain with skip
- **WHEN** a subscriber at position 1 returns `{:skip, :rate_limited}`
- **THEN** subsequent subscribers MUST NOT be invoked and `Extension.run/3` MUST return `{:skip, :rate_limited}`

#### Scenario: Transform chain tolerates subscriber errors
- **WHEN** a subscriber at position 1 returns `{:error, :backend_unavailable}` and another subscriber exists at position 2
- **THEN** the error MUST be logged at `:error` level, the position-2 subscriber MUST be invoked with the ctx as it was after position 0, and the final result MUST be that of position 2

#### Scenario: Sustained input does not accumulate the host's mailbox when transforms are non-blocking
- **GIVEN** a host (e.g. `Flezha.Relay.Overwatch`) with a transform chain whose every subscriber is non-blocking per the requirement above
- **AND** an upstream that emits 80 events/sec for 10 minutes
- **WHEN** sampled at any time during that window via `:erlang.process_info(host_pid, :message_queue_len)`
- **THEN** the host's `:message_queue_len` SHALL be at most 100
- **AND** zero OOM-kill events SHALL occur on the BEAM under the configured cgroup memory limit

#### Scenario: Misbehaving transform inflates host mailbox under sustained input
- **GIVEN** a transform subscriber whose callback synchronously waits 50 ms on an external resource (regression of the non-blocking contract)
- **AND** an upstream that emits 80 events/sec
- **WHEN** the chain runs for 10 minutes
- **THEN** the host's `:message_queue_len` SHALL grow without bound (this scenario fails the non-blocking-contract assertion above)
- **AND** this scenario serves as the canonical regression test for any future first-party transform subscriber whose chain-facing callback is tempted to block — its presence in the test suite SHALL fail the build if a producer-style plugin is added without a `cast_enqueue`-equivalent admission path

### Requirement: Fanout-kind dispatch

The system SHALL dispatch `:fanout`-kind extensions by invoking every subscriber independently. Subscribers MUST NOT observe each other's return values. A subscriber that raises or returns an error MUST be logged and MUST NOT prevent other subscribers from running. `Extension.run/3` for `:fanout` extensions always returns `:ok`.

#### Scenario: Fanout runs every subscriber
- **WHEN** three subscribers are attached to a `:fanout` extension
- **THEN** all three subscribers MUST be invoked regardless of return values or raised exceptions, and `Extension.run/3` MUST return `:ok`

#### Scenario: Fanout subscriber raise is isolated
- **WHEN** one fanout subscriber raises an exception during its callback
- **THEN** the exception MUST be captured and logged, subsequent subscribers MUST still run, and `Extension.run/3` MUST return `:ok`

### Requirement: Request-kind dispatch with keyed uniqueness

The system SHALL dispatch `:request`-kind extensions by looking up exactly one subscriber by a request key supplied in the ctx. Multiple plugins MUST NOT be permitted to subscribe to the same `:request`-kind extension with the same key on the same host. The subscriber's callback MUST be invoked in a short-lived Task supervised by a host-owned supervisor so the host's mailbox is not blocked by the callback.

#### Scenario: Request routes to the keyed subscriber
- **WHEN** `Extension.run/3` is called for a `:request` extension with a ctx whose key matches a registered subscriber
- **THEN** that subscriber's callback MUST be invoked with the ctx inside a Task, and the reply MUST be sent via the ctx's reply mechanism

#### Scenario: Duplicate key subscription rejected
- **WHEN** a plugin attempts to subscribe to a `:request` extension with a key already in use by another plugin on the same host
- **THEN** `Extension.Table.subscribe/3` MUST return `{:error, {:key_conflict, extension_name, key}}` and the existing subscriber MUST remain registered unchanged

#### Scenario: Unknown request key
- **WHEN** `Extension.run/3` is called for a `:request` extension with a key not registered
- **THEN** the reply mechanism MUST deliver `{:error, {:unknown_key, extension_name, key}}` to the caller and the host MUST NOT crash

### Requirement: Timer-kind dispatch

The system SHALL dispatch `:timer`-kind extensions via scheduled invocations. When a plugin subscribes with `{:on_interval, [interval_ms: N]}`, the extension table SHALL schedule a timer that sends `{:extension_timer, extension_name, plugin_id}` to the host process every `N` milliseconds. The host SHALL forward these timer messages into `Extension.Table.fire_timer/3`, which invokes the subscriber and reschedules the next tick.

#### Scenario: Timer subscription schedules periodic invocation
- **WHEN** a plugin subscribes to `{:on_interval, [interval_ms: 1_000]}`
- **THEN** the subscriber's `on_interval/2` callback MUST be invoked approximately every 1_000 ms until the plugin is unsubscribed

#### Scenario: Unsubscribe cancels timer
- **WHEN** a plugin with an active `:on_interval` subscription is unsubscribed
- **THEN** all pending timer messages for that plugin MUST be cancelled and no further invocations MUST occur

### Requirement: Active-extensions fast path

The system SHALL maintain an `active_mask` integer in `Extension.Table` in which bit N is set if and only if extension at catalog index N has at least one subscriber. `Extension.run/3` MUST check this mask before invoking any dispatch logic; when the mask bit is clear the call MUST return the ctx unchanged (for `:fold` / `:transform`), `:ok` (for `:fanout`), or an empty-key outcome (for `:request`) without invoking user code or allocating list cells.

#### Scenario: Extension with no subscribers is zero-cost
- **WHEN** `Extension.run/3` is called for an extension with zero subscribers
- **THEN** no provider callback MUST be invoked and no subscriber-specific data structure MUST be traversed

#### Scenario: Bit flips on first and last subscriber
- **WHEN** the first subscriber is added to an extension, its mask bit MUST be set; when the last subscriber for that extension is removed, its mask bit MUST be cleared

### Requirement: Dispatch chains are pre-composed at subscribe time

For `:fold`, `:transform`, and `:fanout` extensions, the system SHALL build the dispatch function once when subscriptions change (on subscribe, unsubscribe, or reorder), and SHALL NOT traverse the subscriber list on each `Extension.run/3` call. The dispatch path MUST NOT allocate per-subscriber control-flow tuples (e.g. `{:cont, _}` / `{:halt, _}`) on each invocation.

#### Scenario: Chain rebuilt on subscribe
- **WHEN** a new subscriber is added to a `:fold` or `:transform` extension that already has subscribers
- **THEN** the pre-composed dispatch function for that extension MUST be rebuilt so subsequent `Extension.run/3` calls include the new subscriber in position order

#### Scenario: Dispatch path does not traverse subscriber list
- **WHEN** `Extension.run/3` is called with N subscribers attached (N ≥ 1)
- **THEN** the call MUST invoke the pre-composed dispatch function and MUST NOT call `Enum.reduce/3`, `Enum.reduce_while/3`, or any list-iteration primitive over the subscriber list

### Requirement: Subscriber lifecycle owned by Extension.Table

The system SHALL place all subscriber-owned runtime state — monitor refs for spawned subscriber pids, timer refs for `:timer` subscriptions, retry-backoff counters, and `PluginManager` registrations — inside `Flezha.Plugin.Extension.Table`. The host process SHALL forward `{:DOWN, ref, :process, pid, reason}`, `{:extension_timer, name, plugin_id}`, and `{:retry_plugin_restart, plugin_id}` messages into the table, which MUST resolve them to the owning subscriber and apply the extension-kind-appropriate policy.

#### Scenario: DOWN for subscriber-owned pid
- **WHEN** the host process receives `{:DOWN, ref, :process, pid, reason}` where `ref` belongs to a subscriber
- **THEN** `Extension.Table.handle_down/5` MUST resolve `ref` to the owning plugin, apply its restart policy, and return updated table state

#### Scenario: DOWN for unrelated pid
- **WHEN** the host process receives `{:DOWN, ref, :process, pid, reason}` where `ref` does not belong to any subscriber
- **THEN** `Extension.Table.handle_down/5` MUST return `:ignore` and state MUST NOT be mutated

#### Scenario: Teardown releases every resource
- **WHEN** `Extension.Table.teardown_all/1` is invoked
- **THEN** every subscriber-owned monitor MUST be demonitored, every timer ref MUST be cancelled, every subscriber-owned pid MUST be signalled to stop, and every `PluginManager` registration MUST be released

### Requirement: Subscription ordering for ordered kinds

The system SHALL honor a `position` option in subscription opts for `:fold` and `:transform` extensions. Subscribers MUST be invoked in ascending position order. When a new subscriber's position collides with an existing one, subsequent subscribers MUST be shifted upward and renumbered contiguously. `:fanout`, `:request`, and `:timer` kinds MUST reject `position` in subscription opts with `{:error, :position_not_applicable}`.

#### Scenario: Position-based insertion shifts followers
- **WHEN** three subscribers exist at positions 0, 1, 2 and a fourth is added with `position: 1`
- **THEN** the new subscriber occupies position 1, the previous position-1 subscriber is renumbered to 2, and the previous position-2 subscriber is renumbered to 3

#### Scenario: Position on fanout-kind rejected
- **WHEN** a plugin subscribes to a `:fanout` extension with `position: 0` in subscription opts
- **THEN** `Extension.Table.subscribe/3` MUST return `{:error, :position_not_applicable}`

### Requirement: Subscription preserved across relay handover

The system SHALL serialize the extension-table subscription list during `Flezha.Relay.Overwatch.prepare_handover_state/1` and SHALL rehydrate it on the destination node via `Extension.Table.rehydrate/2` during `alter_handover_state/2`. Runtime state that is not transportable (monitor refs, timer refs, subscriber-owned pids) MUST be re-established on the destination node during rehydration. After rehydration the dispatch behaviour MUST match the pre-migration state.

#### Scenario: Subscribed plugins survive migration
- **WHEN** a relay with subscribers on `:on_input` and `:on_message` is migrated to a new compute engine
- **THEN** after migration `Extension.run(:on_input, ctx)` MUST invoke the same set of subscribers as before migration, and `Extension.run(:on_message, ctx_with_key)` MUST route to the same handler

#### Scenario: Subscriber-owned pids are restarted on destination
- **WHEN** a subscriber with a monitored pid is migrated
- **THEN** the destination node MUST spawn a new process for that subscriber and establish a fresh monitor; the source node's pid and monitor ref MUST NOT be transported

### Requirement: Built-in Kafka producer extension conforms to the kafka-producer-plugin contract

The first-party `Flezha.Plugin.Provider.Kafka.Producer` SHALL implement the full `kafka-producer-plugin` capability. Specifically: retries with bounded backoff, idempotent producer semantics, batching with `:linger_ms`, bounded queue with `:queue_overflow`, single shared worker per cluster, active metadata refresh on errors, auto-create topic on first publish, and `:telemetry` events for every state transition.

A Produce attempt that ultimately fails after exhausting `:delivery_timeout_ms` SHALL return `{:error, {:delivery_failed, last_reason}}` to the chain. A passthrough of any raw `:no_broker | :not_leader_for_partition | :unknown_topic_or_partition | :network_exception | :request_timed_out | :not_enough_replicas` to the chain SHALL be considered a regression and rejected at code review.

#### Scenario: No raw transient errors surface to the chain
- **GIVEN** the production-grade Kafka producer extension is loaded
- **WHEN** any test or integration scenario induces a 60-second window of `:not_leader_for_partition` responses
- **THEN** the chain caller (`Flezha.Plugin.Extension.Table.compile_chain/3`) SHALL never observe `{:error, :not_leader_for_partition}` for any message that ultimately succeeds
- **AND** SHALL never observe `{:error, :no_broker}` either
- **AND** for messages that genuinely fail, the chain SHALL receive `{:error, {:delivery_failed, _}}` only after the configured timeout

#### Scenario: Default behaviour is "just works" for the common case
- **GIVEN** a downstream consumer enables a `relay_kafka_producer` with no parameter overrides beyond `:hosts` and `:topic`
- **WHEN** the consumer publishes a stream of messages through `:on_output` to a non-existent topic
- **THEN** the topic SHALL be auto-created on first publish (default `:auto_create_topic = true`)
- **AND** messages SHALL be batched with default `:linger_ms = 10` ms
- **AND** retries SHALL handle transient broker unavailability with default `:retries = 5`
- **AND** durability defaults SHALL be `acks=-1` with idempotence enabled
- **AND** no custom configuration is required for the producer to operate correctly under normal cluster conditions including rolling broker restarts and KRaft leader elections
</content>
</invoke>


### Requirement: Two-phase plugin lifecycle gated on SDK readiness

The system SHALL support a two-phase plugin lifecycle for relay-host plugins: `prepared` (the GenServer/struct is registered, config is validated, no upstream resources are acquired) and `running` (upstream connections opened, subscribe handshakes issued, input emission permitted). The transition from `prepared` to `running` SHALL be driven by `Flezha.Plugin.Extension.Table.bulk_activate/1`, invoked by the relay host ONLY after `start_relay_sdk/2` returns `{:ok, _}` (i.e. the SDK-side relay is bound and ready to accept input).

The `Flezha.Plugin.Host` behaviour SHALL declare `activate/1` as an `@optional_callback`. A plugin provider that implements `activate/1` SHALL use it for upstream I/O acquisition (TLS connect, subscribe send, downstream consumer attach). A provider without `activate/1` SHALL be transitioned to `running` with a no-op (its meaningful work happens on `:on_input` / `:on_output` and is naturally quiescent until input arrives).

Plugins in the `prepared` state SHALL NOT emit `:io_in` to the relay host. The `Extension.Table` SHALL record each plugin's lifecycle phase per binding, and `Table.teardown_all/1` SHALL handle both `prepared` and `running` plugins uniformly (no preconditions on phase for cleanup).

#### Scenario: Plugin lifecycle progresses prepared → running on SDK ready
- **GIVEN** a relay `r1` with one `Plugin.Provider.Websocket` plugin in its initial plugin list
- **WHEN** `Flezha.Relay.Overwatch.handle_continue({:bind_executor, …}, _)` runs `prepare_initial_plugins/2`
- **THEN** the plugin's `init_provider/1` SHALL run (registers state, validates config) but SHALL NOT open a TLS connection or issue a subscribe frame
- **AND** the plugin's lifecycle phase in `Extension.Table` SHALL be `:prepared`
- **WHEN** the Overwatch's `start_relay_sdk(executor_id, relay)` returns `{:ok, _}`
- **AND** the Overwatch invokes `Extension.Table.bulk_activate(table)`
- **THEN** the plugin's `activate/1` callback SHALL fire exactly once
- **AND** the plugin's lifecycle phase SHALL transition to `:running`
- **AND** the upstream TLS connection SHALL open at this point and the subscribe handshake SHALL issue

#### Scenario: Plugins stay in prepared state when SDK never starts
- **GIVEN** a relay `r1` whose Overwatch enters the parked-error state because `:start_relay` exhausts its bounded-retry budget
- **WHEN** the Overwatch parks with `state.init_error != nil`
- **THEN** every plugin in `state.extensions` SHALL be in the `:prepared` phase
- **AND** no plugin SHALL have invoked `activate/1`
- **AND** no plugin SHALL have opened an upstream connection or issued a subscribe handshake
- **AND** no `:io_in` cast SHALL have arrived from any plugin
- **AND** the controller process memory SHALL hold steady at the idle baseline (no per-frame amplification)

#### Scenario: Teardown handles prepared and running plugins uniformly
- **GIVEN** a relay whose extension table contains one plugin in `:prepared` and one plugin in `:running`
- **WHEN** the Overwatch terminates (parked, manual stop, or HotSwap source-side)
- **AND** `Extension.Table.teardown_all/1` runs
- **THEN** both plugins SHALL have their owned pids killed, timers cancelled, monitors demonitored, and `PluginManager` registrations released
- **AND** no error SHALL be logged about either plugin's phase
- **AND** the cleanup SHALL NOT distinguish phase at the call site

### Requirement: Plugin providers SHALL NOT perform upstream I/O in `init_provider/1`

A plugin provider's `init_provider/1` callback SHALL be restricted to local, fast, deterministic work: config validation, GenServer registration, struct construction, supervisor wiring. The callback SHALL NOT:

- open a TLS or plain TCP connection to an upstream broker or websocket endpoint,
- issue a protocol-level handshake (websocket subscribe, Kafka API version negotiate, HTTP TLS handshake against a remote host),
- block on the SDK's data plane being bound,
- acquire any resource whose health depends on the relay's SDK side being ready.

Providers that need to do any of the above SHALL implement the optional `activate/1` callback and perform the work there. `Plugin.Provider.Websocket` is the in-tree exemplar.

Providers whose meaningful work is triggered by `:on_input` / `:on_output` (`Filter`, `Kafka.Producer`, indicator plugins, `BeamMsgForward`, `MessageForward`, `MonitoredIoForward`, `StdOut`, `Metrics`) MAY keep their existing `init_provider/1` and SHALL NOT need an `activate/1` implementation — their work is naturally quiescent until input arrives at the relay host, which only happens after the running phase.

#### Scenario: Websocket provider defers TLS connect to activate/1
- **WHEN** `Plugin.Provider.Websocket.init_provider/1` is called with valid opts
- **THEN** the call SHALL return `{:ok, opts}` (or `{:ok, opts, nil}` for the owned-pid arity) without opening a TLS connection to the configured `:uri`
- **AND** no `Fresh.start_link` call SHALL have occurred during `init_provider/1`
- **AND** the eventual TLS handshake SHALL only happen when `Plugin.Provider.Websocket.activate/1` is invoked
