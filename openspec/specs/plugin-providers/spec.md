# plugin-providers Specification

## Purpose

How a plugin provider declares itself and how the extension table runs it. A provider declares its
whole lifecycle, its options and their defaults, its telemetry vocabulary, whether it feeds the
relay, whether it can be shared, and whether it is a relay or a controller plugin; the table drives
it from that declaration and owns every subscriber's lifecycle. Bringing a plugin up waits for its
relay's SDK, does no upstream I/O in `init_provider/1`, treats a crash there as a refusal, and is
idempotent; a failed activation reaches the caller, health is a declared contract, and a recovered
relay never reuses a dead worker. This capability also covers channel options, enabling and
disabling by target, sources and their batches, bounded sink queues, and the modules a plugin
package may reach. How hooks dispatch is `plugin-relay-hooks`.
## Requirements
### Requirement: Subscriber lifecycle owned by Extension.Table

The system SHALL place all subscriber-owned runtime state — monitor refs for spawned subscriber pids, timer refs for `:timer` subscriptions, each subscriber's lifecycle phase with its `started_at`, and `PluginManager` registrations — inside `Flezha.Plugin.Extension.Table`. The host process SHALL forward `{:DOWN, ref, :process, pid, reason}` and `{:extension_timer, name, plugin_id}` messages into the table, which MUST resolve them to the owning subscriber. The table SHALL hold no retry counter, retry timer or restart policy: a subscriber's `DOWN` is recorded as the phase `{:down, reason}` and declared to the convergence loop as `{:plugin_down, namespace, relay_id, plugin_id}`, and the restart is the loop's `restart_plugin` primitive, executed through `Extension.Table.attempt_restart/3` and keyed per plugin. The stable-uptime rule survives as that primitive's evidence: its key resets only on `:running` observed for the stable window, read from the table's `started_at`.

#### Scenario: DOWN for subscriber-owned pid
- **WHEN** the host process receives `{:DOWN, ref, :process, pid, reason}` where `ref` belongs to a subscriber
- **THEN** `Extension.Table.handle_down/5` MUST resolve `ref` to the owning plugin, record its phase as `{:down, reason}`, declare `{:plugin_down, …}` and return updated table state; it MUST NOT spawn a restart or arm a timer

#### Scenario: DOWN for unrelated pid
- **WHEN** the host process receives `{:DOWN, ref, :process, pid, reason}` where `ref` does not belong to any subscriber
- **THEN** `Extension.Table.handle_down/5` MUST return `:ignore` and state MUST NOT be mutated

#### Scenario: Teardown releases every resource
- **WHEN** `Extension.Table.teardown_all/1` is invoked
- **THEN** every subscriber-owned monitor MUST be demonitored, every timer ref MUST be cancelled, every subscriber-owned pid MUST be signalled to stop, and every `PluginManager` registration MUST be released

#### Scenario: A crashing plugin throttles no sibling
- **GIVEN** a relay with two declared plugins, one crashing on every start
- **WHEN** the loop plans across ticks
- **THEN** the crashing plugin's `{restart_plugin, {id, plugin_id}}` key walks the table while the sibling's key is untouched, and the sibling's start is never delayed by the crash

### Requirement: Two-phase plugin lifecycle gated on SDK readiness

The system SHALL support a two-phase plugin lifecycle for relay-host plugins: `prepared` (the GenServer/struct is registered, config is validated, no upstream resources are acquired) and `running` (upstream connections opened, subscribe handshakes issued, input emission permitted). The transition from `prepared` to `running` SHALL be driven by `Flezha.Plugin.Extension.Table.bulk_activate/1`, invoked by the relay host ONLY after `start_relay_sdk/2` returns `{:ok, _}` (i.e. the SDK-side relay is bound and ready to accept input).

`Flezha.Plugin.ProviderBehaviour` SHALL declare `activate/1` as an optional callback with a default body that completes activation without upstream work, so that the table invokes it for every provider and never discovers it by probing. A plugin provider that overrides `activate/1` SHALL use it for upstream I/O acquisition (TLS connect, subscribe send, downstream consumer attach). A provider that does not override it SHALL be transitioned to `running` by the default (its meaningful work happens on `:on_input` / `:on_output` and is naturally quiescent until input arrives); a provider that declares a worker SHALL have that worker started by the table at this transition, for every provider alike.

Plugins in the `prepared` state SHALL NOT emit `:io_in` to the relay host. The `Extension.Table` SHALL record each plugin's lifecycle phase per binding, and `Table.teardown_all/1` SHALL handle both `prepared` and `running` plugins uniformly (no preconditions on phase for cleanup).

#### Scenario: Plugin lifecycle progresses prepared → running on SDK ready
- **GIVEN** a relay `r1` with one `Plugin.Provider.Websocket` plugin in its initial plugin list
- **WHEN** `Flezha.Relay.Overwatch.handle_continue({:bind_runner, …}, _)` runs `prepare_initial_plugins/2`
- **THEN** the plugin's `init_provider/1` SHALL run (registers state, validates config) but SHALL NOT open a TLS connection or issue a subscribe frame
- **AND** the plugin's lifecycle phase in `Extension.Table` SHALL be `:prepared`
- **WHEN** the Overwatch's `start_relay_sdk(runner_id, relay)` returns `{:ok, _}`
- **AND** the Overwatch invokes `Extension.Table.bulk_activate(table)`
- **THEN** the plugin's `activate/1` callback SHALL fire exactly once
- **AND** the plugin's lifecycle phase SHALL transition to `:running`
- **AND** the upstream TLS connection SHALL open at this point and the subscribe handshake SHALL issue

#### Scenario: Plugins stay in prepared state when SDK never starts
- **GIVEN** a relay `r1` whose Overwatch enters the parked-error state because `:start_relay` exhausts its steps-retry budget
- **WHEN** the Overwatch parks with `state.halt_reason != nil`
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

### Requirement: Plugin providers SHALL expose a health contract

`Flezha.Plugin.ProviderBehaviour` SHALL define an optional `health/1` callback returning
a health verdict for a running provider (default: process alive ⇒ healthy). The WebSocket
provider's verdict SHALL **reuse the existing canonical liveness timestamps** rather than
introduce new ones — healthy means its connection-liveness `last_frame_at` (and, when
`resubscribe_after_ms` is configured, its data-liveness `last_data_frame_at`) are within
their windows, per the "Websocket connection-liveness watchdog" / "Websocket data-liveness
watchdog" requirements already in this capability. The Kafka producer provider SHALL
report healthy only when its worker is alive, not permanently parked, and its queue is
draining. The provider's answer is a FACT: `ProviderBehaviour.worker_health/3` SHALL be
the one place it is normalised into the probe-result shape (`:healthy`,
`{:unhealthy, reason}` or `{:fail_open, reason}`), keeping each provider's reason words,
and no second normaliser SHALL exist. The reconciler / `Extension.Table` SHALL query
`health/1` through that seam and hand the result, unjudged, to the convergence loop's
probe table, whose judge mints the plugin's one verdict record — `healthy`, `degraded` or
`unknown` — and escalates, restarting an unhealthy provider under a bounded governor, only
when the provider's own fast in-process recovery has repeatedly failed to restore health.
The providers' in-process reconnect/retry detectors are retained; the health contract
adds escalation, it does not replace them.

A plugin in the `:prepared` phase SHALL be judged `healthy` with reason `prepared` — it
has acquired no upstream resources, so it has nothing to be unhealthy about — and that
verdict SHALL NOT be read anywhere as evidence that the plugin is running: the record's
`phase` is `prepared`, and the phase, not the health state, is the authority on whether
a plugin is doing its work. A consumer that needs "is this plugin running" SHALL read the
phase (see "A declared plugin that is not running is a divergence" in
`cluster-convergence`).

#### Scenario: Unhealthy provider is restarted
- **GIVEN** a relay whose Kafka producer worker is alive but permanently parked (not draining)
- **WHEN** the reconciler queries the provider's `health/1`
- **THEN** the provider SHALL report unhealthy
- **AND** the reconciler SHALL restart it under the bounded governor so production resumes

#### Scenario: Healthy provider is left alone
- **GIVEN** a WebSocket provider that is connected and receiving frames
- **WHEN** `health/1` is queried
- **THEN** it SHALL report healthy and the reconciler SHALL take no action

#### Scenario: A prepared plugin is healthy but is not running
- **GIVEN** a declared plugin whose `activate/1` failed, leaving it in the `:prepared` phase
- **WHEN** its verdict record is read
- **THEN** the state SHALL be `healthy` with reason `prepared` and the phase SHALL be `prepared`
- **AND** the plugin SHALL NOT be reported as running to any consumer that asks which plugins are running

#### Scenario: One normaliser, one record
- **GIVEN** a provider whose `health/1` returns an unrecognised value
- **WHEN** the reconciler queries it
- **THEN** `ProviderBehaviour.worker_health/3` SHALL be the only function that turns the answer into a probe result, the probe table SHALL be the only place that turns the probe result into a state, and the plugin SHALL have exactly one verdict record on the snapshot

### Requirement: Recovered relays SHALL NOT reuse dead or parked plugin workers

When a relay is recovered or respawned, plugin re-application SHALL NOT reuse a
stale provider process. `Flezha.Plugin.Provider.WorkerSup.ensure_worker/5` SHALL verify
the looked-up pid passes the provider's declared reusability check before reuse, and
reap a dead or parked worker so a fresh one is started.

Reaping SHALL NOT wait on the process registry. A registry entry that still names
the reaped worker SHALL NOT delay, and SHALL NOT fail, the start of the fresh one:
the supervisor's own termination already returns after the child has exited, and a
registration over an entry naming a dead pid replaces it. A bounded poll in place
of that is a fixed cost on every swap for a failure that cannot occur, and it is
invisible to the sleep gate because that gate counts test files only.

#### Scenario: Stale producer pid is reaped on recovery
- **GIVEN** a recovered relay whose previous Kafka producer worker is dead or parked but still in the registry
- **WHEN** `ensure_worker/5` runs during plugin re-application
- **THEN** it SHALL reap the stale entry and start a fresh, healthy producer worker
- **AND** the recovered relay SHALL produce rather than re-attach to the dead worker

#### Scenario: A stale registry entry does not delay the fresh worker
- **GIVEN** a reaped worker whose registry entry has not yet been cleared and still names it
- **WHEN** `ensure_worker/5` starts the replacement
- **THEN** a fresh, live worker SHALL be started and named by the registry
- **AND** the swap SHALL complete without sleeping

### Requirement: Plugin enable SHALL be idempotent for an id already registered against the same target

Enabling a plugin name that is already enabled in the **same** namespace with the **same** provider SHALL succeed as an explicit no-op: the CLI exits 0 stating the plugin is already enabled and the existing plugin was left untouched, its running copies and their workers observe no restart, and no declared-set write occurs. Opts SHALL NOT be compared; a re-run is a presence check, not config convergence, and `flz plugin update` is the verb that changes options. The manager SHALL answer a verdict distinguishable from a fresh enable (`{:ok, :already_enabled}`) so callers can render it. Enabling a name already enabled in the namespace with a **different** provider SHALL remain an error naming the conflict (`plugin.id_taken`). A one-step enable that also names a relay SHALL, on an already-enabled plugin, go on to the bind, which is itself a no-op for a relay already bound.

#### Scenario: Re-running an enable is quiet

- **WHEN** `flz plugin enable` is re-run with a plugin name already enabled with the same provider in the same namespace
- **THEN** the command exits 0 stating already-enabled, and the running plugin and its workers are untouched

#### Scenario: A provider conflict still fails

- **WHEN** the plugin name is enabled in the namespace with another provider
- **THEN** the enable fails with `plugin.id_taken` naming the namespace

#### Scenario: A re-run one-step enable binds once

- **GIVEN** `trades` enabled and bound to `a`
- **WHEN** `flz plugin enable trades websocket --relay_id a` is re-run
- **THEN** the command exits 0 and `a` holds one binding to `trades`

### Requirement: A failed activation SHALL be reported to the caller, not only logged

`Flezha.Plugin.Extension.Table.bulk_activate/2` SHALL report which plugins did
NOT reach `:running`. A provider's `activate/1` returning `{:error, reason}`,
returning an invalid value, or raising SHALL leave the plugin in `:prepared`
AND SHALL surface that outcome to the caller, so the relay host can answer
its own caller honestly instead of recording a success it did not achieve.

`Flezha.Relay.Plugins.subscribe/3` and `Flezha.Relay.Plugins.restart/3`
SHALL return an error when the plugin they were asked to bring up did not reach
`:running`, even when `Table.subscribe/3` itself succeeded. Returning `{:ok, _}`
for a plugin that never activated makes the convergence loop's `start_plugin` and
`restart_plugin` heals report success for a plugin that acquired no upstream
resources, which advances the backoff ladder as though the divergence were fixed.

#### Scenario: activate returning an error is reported, not swallowed
- **GIVEN** a declared plugin whose provider's `activate/1` returns `{:error, :upstream_down}`
- **WHEN** the relay host runs `bulk_activate/2`
- **THEN** the plugin's phase SHALL remain `:prepared`
- **AND** `bulk_activate/2` SHALL identify that plugin as not activated
- **AND** `Plugins.subscribe/3` SHALL return an error naming the plugin and the reason

#### Scenario: activate raising is reported, not swallowed
- **GIVEN** a declared plugin whose provider's `activate/1` raises
- **WHEN** the relay host runs `bulk_activate/2`
- **THEN** the plugin's phase SHALL remain `:prepared`
- **AND** `Plugins.subscribe/3` SHALL return an error rather than `{:ok, state}`

#### Scenario: a restart whose activation fails does not report success
- **GIVEN** a declared plugin that self-reports `{:unhealthy, :upstream_down}`
- **AND** its upstream is still down when the restart fires
- **WHEN** `Plugins.restart/3` unsubscribes, re-subscribes, and activates it
- **THEN** the plugin's phase SHALL be `:prepared`
- **AND** `Plugins.restart/3` SHALL return an error so the heal is recorded as failed

#### Scenario: a successful activation still reports success
- **GIVEN** a declared plugin whose provider's `activate/1` returns `{:ok, pid}`
- **WHEN** the relay host runs `bulk_activate/2`
- **THEN** the plugin's phase SHALL be `:running`
- **AND** `Plugins.subscribe/3` SHALL return `{:ok, state}`

### Requirement: Bringing up a plugin SHALL be idempotent on one already subscribed

`Flezha.Relay.Plugins.subscribe/3` SHALL re-ACTIVATE a plugin that is already
present on the extension table, rather than refusing it as a duplicate id.

This is load-bearing, not a convenience. `Table.subscribe/3` rejects a duplicate
plugin id, and a plugin whose `activate/1` failed remains SUBSCRIBED in the
`:prepared` phase — so the `start_plugin` heal that "A declared plugin that is
not running is a divergence" relies on would fail at the subscribe step on every
tick, forever. The divergence would be visible, the op would be planned, the
heal would be attempted, and nothing would ever be repaired. Re-activating is
what makes the heal reach the provider's `activate/1` at all.

A plugin already in the `:running` phase is skipped by `bulk_activate/2`, so the
same call is a no-op for one that needs nothing.

#### Scenario: a heal re-activates rather than refusing a duplicate
- **GIVEN** a declared plugin left `:prepared` by a failed `activate/1`, still subscribed on the table
- **WHEN** the convergence loop heals it with `start_plugin`
- **THEN** `Plugins.subscribe/3` SHALL re-activate it rather than returning a duplicate-id error
- **AND** once upstream recovers the plugin SHALL reach `:running` and the primitive SHALL report `:ok`

#### Scenario: bringing up an already-running plugin changes nothing
- **GIVEN** a declared plugin already in the `:running` phase
- **WHEN** `Plugins.subscribe/3` is called for it
- **THEN** it SHALL remain `:running`, its provider's `activate/1` SHALL NOT be invoked again, and the call SHALL return `{:ok, state}`

### Requirement: An uncertain health probe SHALL be observable

The plugin fact seam SHALL answer `{:fail_open, reason}` when a provider's `health/1`
raises, times out, or returns an unrecognised value, and SHALL emit
`[:flezha, :plugin, :health, :fail_open]` once, where the fact was read, naming the
plugin id, the provider and the reason it fell back. The convergence loop's judge SHALL
mint that plugin's record `healthy` with reason `{:fail_open, reason}` — an uncertain
probe MUST never drive a heal — and a probe that could not run at all SHALL be judged
`unknown`. The fail-open event SHALL reach the observability bus by the probe table's
own declaration of it, so it is never invisible: a worker that is alive but no longer
answering is otherwise indistinguishable from a healthy one at every layer of the
system.

#### Scenario: a wedged worker's fail-open verdict is emitted
- **GIVEN** a running plugin whose provider `health/1` does not answer within the probe budget
- **WHEN** the plugin's health is probed
- **THEN** the verdict record SHALL be `healthy` with a fail-open reason
- **AND** telemetry SHALL be emitted naming the plugin id, the provider and the fallback reason

#### Scenario: a definite verdict emits nothing
- **GIVEN** a running plugin whose provider `health/1` returns `:healthy` or `{:unhealthy, reason}`
- **WHEN** the plugin's health is probed
- **THEN** that fact SHALL reach the judge unchanged and the record SHALL be `healthy` or `degraded` accordingly
- **AND** no fail-open telemetry SHALL be emitted

#### Scenario: the fail-open event reaches an output plugin
- **GIVEN** the observability subsystem is running with its default output plugins
- **WHEN** a provider's `health/1` times out and the fail-open event is emitted
- **THEN** a signal carrying the plugin id, the provider and the reason SHALL appear on the bus with no hand-written bridge line for the event

### Requirement: A provider's conversion table SHALL refuse a value it cannot interpret

A plugin provider's option conversion SHALL be total for every option it declares: a value the option's declared type or bounds do not accept SHALL be answered `{:error, binary()}` naming the option and the offered value, never passed through unchanged.

The type is declared by the option's row, and the generated fall-through clause SHALL apply it to every value the provider's own `convert_value/2` clauses did not match. An option declared with no interpretable type keeps the permissive default: a provider deliberately does not convert its string and host-injected options, and refusing those would refuse every working configuration.

A provider that declares an option type the contract does not offer SHALL NOT compile, and the compile error SHALL name the module and the option. Discipline is not a mechanism: a table whose totality depends on each author remembering to write a terminal clause regenerates this defect on the next option added.

The shared conversion helpers `ProviderBehaviour.parse_int/2` and `ProviderBehaviour.parse_bool/2` SHALL be total, accepting both the CLI string form and the native form of their type and refusing everything else. The system SHALL NOT offer a partial or raising counterpart for the `init_provider/1` path: an option that reaches `init_provider/1` is user-supplied and unvalidated, so a helper that raises on a shape it does not expect turns a bad value into a crash rather than a refusal.

An option a provider interprets SHALL be converted before the provider takes any side effect, and preparation SHALL take none: no worker is started, no connection pool is opened and no process is linked until the plugin is activated.

#### Scenario: An unmatched value for a declared option is refused

- **GIVEN** a provider option whose declared type does not accept the value supplied, and whose own clauses do not match it
- **WHEN** the value is converted
- **THEN** the result SHALL be `{:error, message}` naming the option and the value
- **AND** the value SHALL NOT reach the provider's struct, its worker options, or its stored opts

#### Scenario: An option the declaration does not interpret is unaffected

- **GIVEN** a provider option declared with no interpretable type, or a host-injected key the declaration does not name
- **WHEN** the value is converted
- **THEN** it SHALL be accepted unchanged

#### Scenario: A provider that declares an unknown option type is a compile error

- **GIVEN** a provider module that `use`s `Flezha.Plugin.ProviderBehaviour` and declares an option whose type the contract does not offer
- **WHEN** the module is compiled
- **THEN** compilation SHALL fail with an error naming the module and the option

#### Scenario: The shared integer helper refuses a shape it cannot parse

- **GIVEN** a value that is neither an integer nor a string naming one
- **WHEN** it is converted through `ProviderBehaviour.parse_int/2`
- **THEN** the result SHALL be `{:error, message}` naming the field and the value
- **AND** it SHALL NOT raise

#### Scenario: An invalid tunable is refused before any worker is started

- **GIVEN** a plugin whose configuration carries a value its provider cannot interpret
- **WHEN** the plugin is enabled
- **THEN** preparation SHALL answer `{:error, message}`
- **AND** no worker, pool or linked process SHALL have been started for that plugin

### Requirement: A crashing `init_provider/1` SHALL be a refusal, not an escape

`Flezha.Plugin.Extension.Table` SHALL turn an exception, a throw or an exit raised inside a provider's `init_provider/1` into `{:error, reason}` at the point the callback is invoked, so that the plugin fails to enable rather than the failure escaping into the caller. The reason SHALL name the provider and carry the kind and payload of the failure.

The conversion SHALL apply to both call sites of the callback — the enable path and the plugin-heal restart path — because a provider that crashes on a stored value crashes identically on every heal that re-prepares it.

The failure SHALL be logged at `:error` with the formatted exception and its stacktrace. Nothing is hidden by this requirement: an escaping exception today kills the relay host process that invoked the enable, taking every other plugin on that host with it, and a refusal plus a full stacktrace in the log is strictly more legible than a crash.

A refusal produced this way SHALL travel the path a refused enable already travels, and SHALL NOT be reported to the user as a success.

#### Scenario: A raise inside `init_provider/1` refuses the enable

- **GIVEN** a plugin whose provider raises inside `init_provider/1` — for example a boolean option whose value the provider's helpers cannot match
- **WHEN** the plugin is subscribed
- **THEN** `Extension.Table.subscribe/3` SHALL return `{:error, reason}`
- **AND** the reason SHALL name the provider
- **AND** the caller SHALL remain alive with the table unchanged

#### Scenario: A throw that is not the internal sentinel refuses the enable

- **GIVEN** a plugin whose provider throws a plain value inside `init_provider/1`
- **WHEN** the plugin is subscribed
- **THEN** `Extension.Table.subscribe/3` SHALL return `{:error, reason}` rather than letting the throw escape

#### Scenario: The plugin-heal restart path refuses the same way

- **GIVEN** a subscribed plugin whose provider raises when `init_provider/1` is re-run
- **WHEN** the table attempts a provider restart
- **THEN** the restart SHALL be recorded as failed and the failure logged
- **AND** the exception SHALL NOT escape into the host process

### Requirement: A provider worker handed to a caller SHALL have passed the reusability check

`ensure_worker/4` SHALL NOT answer a worker that its own reusability check has
rejected. Every arm that returns an existing worker SHALL be gated on the same
check, so the rule that a stale provider process is never reused holds on all of
them and not only on the arm that happens to consult it.

#### Scenario: A cached worker fails the reusability check

- **WHEN** a caller asks for a worker whose cached entry no longer satisfies
  `reusable?`
- **THEN** a fresh worker SHALL be started and the stale one SHALL NOT be returned

#### Scenario: Every returning arm asks the same question

- **WHEN** any arm of `ensure_worker/4` answers with an existing worker
- **THEN** that answer SHALL have passed the same reusability check as every other arm

### Requirement: A provider SHALL declare its whole lifecycle, and the extension table SHALL drive from the declaration

`Flezha.Plugin.ProviderBehaviour` SHALL declare every callback the host can
invoke on a provider (`init_provider/1`, `activate/1`, `health/1`,
`inspect_state/1`, `terminate_provider/1` and the extension callbacks
`on_relay_init/2`, `on_input/2`, `on_output/2`, `on_interval/2`,
`on_message/2`, `on_snapshot/2`, `on_terminate/2`, `on_preview/2`, `on_call/2`) as optional callbacks whose
lifecycle members carry a default body a provider may override, and SHALL
derive at compile time one declaration record per provider naming its option
rows, the extension callbacks it implements, the lifecycle callbacks it
overrides and its worker, if it has one. `Flezha.Plugin.Extension.Table` SHALL
invoke every callback through the declaration and SHALL NOT discover a
provider's shape by probing exports at run time.

The table SHALL keep one lifecycle record per plugin (its phase, its worker
and monitor, when the worker started, its retry count and its timers), and the
provider-owned process SHALL be a field of that record, injected into
`health/1`, `inspect_state/1` and `terminate_provider/1` from one site. The
phase SHALL be one declared type, `:prepared | :running`, read outside the table
only through the table's predicates, and every phase transition SHALL emit one
event naming the plugin, the provider, the phases and the reason.

A provider that declares a worker SHALL have it started by the table at
activation through the one worker supervisor shape, under a family named after
the provider, keyed and restarted as the declaration says, tracked, probed and
stopped by the table one way; no provider SHALL start, register or stop a
lifecycle process of its own. A plugin in the `:prepared` phase SHALL hold no
process.

Every provider SHALL be a row of one provider table, from which the enabled
providers are read and to which an installed package adds rows; the
configuration file SHALL NOT restate the list. One conformance test SHALL run
the same drill over every row (prepare, activate with one failure, activate,
probe, report, disable, tear down) and SHALL fail when a provider implements
an extension callback its declaration does not name, when a probe is written
in the plugin directory, when a phase literal is read outside the table, when
a provider starts a process outside the table, or when a documented option is
not a declared row.

#### Scenario: A callback the provider does not override takes the default

- **WHEN** a provider that overrides neither `activate/1` nor `health/1` is
  activated and probed
- **THEN** the table SHALL call the default bodies (activation completes
  without upstream work and the verdict is the declared worker alive or
  absent), and no export SHALL be probed to decide so

#### Scenario: A declared worker is started at activation and holds nothing before it

- **GIVEN** a provider declaring a worker module, key and restart value
- **WHEN** its plugin is prepared on a relay whose SDK side never binds
- **THEN** no process SHALL exist for the plugin, the phase SHALL be
  `:prepared`, and a later activation SHALL start the worker under the
  provider's family with the declared key and restart value, tracked by the
  table as the plugin's owned process

#### Scenario: A disabled plugin leaves no worker behind

- **WHEN** a plugin whose provider declares a `:permanent` worker is disabled
- **THEN** the table SHALL stop the worker through the supervisor, the family
  registry SHALL hold no worker for the plugin id afterwards, and
  `terminate_provider/1` SHALL have been called once with the owned process

#### Scenario: Every transition is one event

- **WHEN** a plugin moves between `:prepared` and `:running`, or enters or
  leaves the table, for any reason
- **THEN** exactly one `[:flezha, :plugin, :phase]` event SHALL be executed
  from the table's one transition function, carrying the plugin id, the
  provider, the phase before and after, and the reason

#### Scenario: A probe or a literal beside the seam fails the suite

- **WHEN** a `function_exported?` call on a provider is written under the
  plugin directory, a phase atom is compared outside the extension table, a
  process is started inside a provider's `init_provider/1` or `activate/1`, or a
  provider defines an `on_*/2` function its declaration does not name
- **THEN** the conformance test SHALL fail naming the file and line, or the
  module and function

#### Scenario: A provider is one row

- **WHEN** a provider module is added under the plugin directory with its
  declaration and one row in the provider table
- **THEN** it SHALL be enabled by id, listed by `flz plugin list`, described by
  `flz plugin help`, driven by the conformance test and rendered into the docs
  gate with no other file edited

### Requirement: A provider's options SHALL be declared once, and every default SHALL come from the declaration

A provider SHALL declare its options as rows — name, type, default, whether
required, bounds, whether internal, documentation — from which its struct, its
strict allow-list, its conversion table, its help screen, its `provider_info/0`,
its documented option table and its published option description derive. A worker SHALL receive the converted
struct and read its fields; no worker SHALL re-read an option from a keyword
list with a default of its own, and no default SHALL exist outside the rows.
The "missing required", "below the minimum", "above the maximum" and "invalid
value" refusals SHALL be rendered by one builder from the rows, and a
provider's cross-field rule SHALL answer the same `{:error, binary()}` shape.
A test SHALL render every provider's option table from its declaration and
assert the provider's page under `docs/public/plugins/` carries it, so a documented
key that is not a row, a row that is not documented, and a default that differs
between the two each fail the suite.

A row SHALL be described by one function, `Flezha.Plugin.ProviderBehaviour.Options.describe_row/1`,
which the documented option table, the published option description
(`api-operations`) and the catalogue's description of an operation's arguments all
read, so a row's type, its required flag and its default are read one way.

A first-party provider's public row SHALL declare the narrowest row type that describes every
value the row accepts, and no public row of a first-party provider SHALL be typed `:any`.
`:any` stays available for an internal row and for a provider outside the release. A row whose
value a prepared plugin holds in another shape SHALL accept that shape again, so preparing the
plugin from its stored options converts them again.

#### Scenario: A default has one home

- **WHEN** an option's default is changed in its row
- **THEN** the struct default, the help screen, the worker's effective value
  and the documented default SHALL all reflect it, with no other edit

#### Scenario: A worker that re-reads an option fails the suite

- **WHEN** a worker module under a provider's directory reads an option from
  its opts with a literal default
- **THEN** the conformance test SHALL fail naming the file and the key

#### Scenario: The documented table equals the declared one

- **WHEN** the docs gate renders a provider's rows and compares them with its
  page
- **THEN** a key present on one side only, or a default that differs, SHALL
  fail the suite naming the provider and the key

#### Scenario: A required option missing is refused by the one builder

- **WHEN** a plugin is enabled without an option its provider declares
  `required: true`
- **THEN** the refusal SHALL be `{:error, sentence}` naming the option, in the
  same sentence shape every provider answers, on both the CLI and the
  `init_provider/1` path

#### Scenario: The catalogue reads the same description

- **WHEN** `mix flezha.catalogue` writes `crates/cli/catalogue.json`
- **THEN** every argument it lists SHALL be the description `describe_row/1` gives that
  argument's row, with its type and its values written as JSON

#### Scenario: No first-party public row is untyped

- **WHEN** the rows of every provider in `Flezha.Plugin.Providers.all/0`, and of the Kafka and
  RabbitMQ plugins in their own suites, are read
- **THEN** no row that is not internal SHALL be typed `:any`, and a test SHALL fail naming
  each one that is

#### Scenario: Kafka hosts in every spelling

- **WHEN** a Kafka plugin is enabled with `hosts` written `kafka1:9092,kafka2:9093`, as the
  list `["kafka1:9092", "kafka2:9093"]`, or as `[{"kafka1", 9092}, {"kafka2", 9093}]`
- **THEN** each SHALL prepare to the same `{host, port}` list, and the options the prepared
  plugin holds SHALL prepare again to the same list

#### Scenario: A host without a port is refused

- **WHEN** a Kafka plugin is enabled with `hosts` written `kafka1`
- **THEN** the enable SHALL be refused naming the option `hosts`

### Requirement: A provider's telemetry names SHALL come from one declared vocabulary

A provider SHALL emit telemetry through one function on the provider behaviour, and its event prefix SHALL be derived from the provider's own identifier rather than written as a module attribute.

The vocabulary SHALL declare the verbs that mean the same thing for every
provider — a record shipped, a delivery retried, a delivery failed, and an
overflow shed — so that one dashboard reads every provider and a new provider
cannot invent a third spelling for a fact two providers already report. Facts
that belong to one provider alone, such as a broker metadata refresh or a topic
creation, SHALL remain that provider's own verbs beside the declared ones. An own verb SHALL be
declared as `verb: kind`, `verb: {kind, meaning}` or `verb: {kind, meaning, level}`; a provider SHALL list the shared verbs it writes,
each with an optional log level, under its declaration's `emits:` key, which a first-party
provider's exported telemetry is derived from. A level names the `Logger` level the bridge writes
the event at, so the provider writes no `Logger` line of its own for that fact.

Every provider's events SHALL sit under one root. A provider that emits under a
root of its own is unreachable by any consumer that subscribes to the family,
and the divergence is invisible until someone greps for it.

#### Scenario: The prefix is derived from the provider id

- **WHEN** a provider emits a declared verb
- **THEN** the event name SHALL be the family root, the provider's identifier and the verb, with no prefix attribute in the provider's module

#### Scenario: A second spelling of a shared fact is refused

- **WHEN** a provider emits an event meaning "a record was shipped" under a name outside the declared vocabulary
- **THEN** the vocabulary test SHALL fail and name the event

#### Scenario: A provider-specific fact keeps its own verb

- **WHEN** a provider reports a fact no other provider has
- **THEN** it SHALL emit that verb under the same derived prefix, and the vocabulary test SHALL accept it

#### Scenario: A declared level replaces the provider's own log line

- **GIVEN** a provider declaring `emits: [failed: :warning]`
- **WHEN** its worker executes its `failed` event
- **THEN** the bridge SHALL write one `Logger` line at `:warning` for it, and the provider's module SHALL write none

### Requirement: A provider SHALL declare whether it feeds the relay

A provider declaration SHALL carry whether the provider injects payloads into the relay, defaulting to false. Only a provider whose own worker calls `Host.inject/4` declares it true.

This cannot be inferred from the callback list. A hook on `on_input` means "act on a payload entering the relay", not "inject one": the Kafka producer hooked on `on_input` publishes what the relay ingested and feeds nothing. Feeding and acting on the chain are two unrelated jobs, and only the declaration tells them apart.

#### Scenario: An injecting provider declares it

- **WHEN** the `websocket` or `http_poll` provider declaration is read
- **THEN** it SHALL report that the provider feeds the relay

#### Scenario: A chain-only provider does not

- **WHEN** the Kafka producer declaration is read
- **THEN** it SHALL report that the provider does not feed the relay, whatever hooks a plugin using it has

### Requirement: A channel option SHALL name the channel the plugin is attached to

A plugin's `channel` option SHALL name the channel the plugin is attached to in both directions at once: the entry channel it injects on when its provider feeds the relay, and the filter over records passing through the relay when the plugin has a chain hook. A plugin that does both jobs SHALL read both from that one name. The resolution SHALL be performed once, where the plugin's options are normalised, and SHALL produce the two concrete values the injection sites and `ProviderBehaviour.in_channel?/2` read, so no provider re-derives the meaning and dispatch is unchanged. It SHALL read only the options a user wrote, so re-preparing a plugin from the options it already holds — which every plugin restart and every handover does — resolves the same answer again rather than refusing it.

Whether a plugin feeds the relay comes from its provider declaration. Whether it acts on the chain comes from its hooks, which the host already merges into the options map.

`channel_in` SHALL name the entry channel alone and `channel_out` the record filter alone, for a plugin whose two names differ. Within one relay an entry channel and an output channel never share a name, so a plugin that injects and has an `on_output` hook is such a plugin; the documentation SHALL say so beside the example.

#### Scenario: A feeding plugin reads `channel` as its entry channel

- **GIVEN** a plugin whose provider feeds the relay and which has no chain hook
- **WHEN** it is enabled with `channel: "trades"`
- **THEN** every payload it injects SHALL carry `"trades"` as its channel

#### Scenario: A chain plugin reads `channel` as its record filter

- **GIVEN** a plugin with an `on_output` hook whose provider does not feed the relay
- **WHEN** it is enabled with `channel: "prices"`
- **THEN** it SHALL act only on records whose metadata names `prices`, and other records SHALL pass through the chain unchanged

#### Scenario: A plugin doing both jobs reads both from one name

- **GIVEN** a plugin whose provider feeds the relay and which has an `on_input` hook
- **WHEN** it is enabled with `channel: "requests"`
- **THEN** it SHALL inject on `requests` and SHALL act only on records whose metadata names `requests`

#### Scenario: A plugin doing both jobs names each explicitly

- **GIVEN** a plugin whose provider feeds the relay and which has an `on_output` hook
- **WHEN** it is enabled with `channel_in: "requests"` and `channel_out: "responses"`
- **THEN** it SHALL inject on `requests` and act only on records naming `responses`

### Requirement: An unresolvable channel option SHALL be refused when the plugin is enabled

Enabling a plugin SHALL be refused when its channel options cannot take effect or contradict each other, and the refusal SHALL name both halves of the conflict. The refusal happens when the plugin is enabled, not when a frame moves, because the provider declaration and the hook list answer every case before any payload exists.

Three shapes are refused: `channel` supplied together with `channel_in` or `channel_out`, because resolving it by precedence would hide the mistake; an explicit key naming a job the plugin does not do, `channel_in` on a provider that does not feed the relay or `channel_out` on a plugin with no chain hook, because the option could never take effect; and `channel` on a plugin that neither feeds the relay nor has a chain hook, because nothing could read it.

A provider SHALL declare only the spellings it can honour. A spelling for a direction the provider never works in — `channel_out` on a source that only feeds, `channel_in` on a provider that only acts on the chain — is therefore not one of its options and is refused as an option that provider does not offer, by the same rule that refuses every unrecognised key.

A `"channel"` key inside a provider's general metadata bag SHALL continue to select the entry channel on its own, and SHALL be refused when supplied together with any of the three channel options, because both would otherwise be live and the winner would be an implementation detail.

#### Scenario: `channel` alongside an explicit key is refused

- **WHEN** a plugin is enabled with both `channel` and `channel_out`
- **THEN** the enable SHALL be refused, naming both keys

#### Scenario: A key for a job the plugin does not do is refused

- **GIVEN** a plugin whose provider declares `channel_out` and which has no chain hook
- **WHEN** it is enabled with `channel_out`
- **THEN** the enable SHALL be refused, stating the job the plugin does not do

#### Scenario: A spelling for a direction the provider never works in is not one of its options

- **WHEN** a plugin of the `http_poll` provider is enabled with `channel_out`, or a plugin of the Kafka producer is enabled with `channel_in`
- **THEN** the enable SHALL be refused, naming the key as an option that provider does not offer

#### Scenario: `channel` on a plugin doing neither job is refused

- **GIVEN** a plugin whose provider does not feed the relay and which has no chain hook
- **WHEN** it is enabled with `channel: "x"`
- **THEN** the enable SHALL be refused, stating that nothing reads the option

#### Scenario: The metadata bag alone still selects the entry channel

- **WHEN** a `websocket` plugin is enabled with a metadata bag carrying `"channel" => "us"` and no channel option
- **THEN** the enable SHALL succeed and every injected payload SHALL carry `"us"` as its channel

#### Scenario: The metadata bag together with a channel option is refused

- **WHEN** a `websocket` plugin is enabled with both a metadata bag carrying `"channel"` and a `channel` option
- **THEN** the enable SHALL be refused, naming both

### Requirement: A channel filter SHALL match any of the names it carries

A plugin's channel filter SHALL accept one or more channel names and SHALL act on a record whose channel metadata matches any of them. A filter carrying no name SHALL continue to match every record, which is what makes an unfiltered plugin receive everything.

The names SHALL be written as a comma-separated list in the user-facing spelling, parsed by the option-row vocabulary rather than by any provider, so a provider reads a resolved set and cannot observe whether the user wrote one name or a list of one. A name that is empty after trimming SHALL be refused rather than dropped, because an empty segment is a typo and accepting it would filter on a name nobody wrote.

The names form a set. Order SHALL NOT be significant and a repeated name SHALL NOT be refused, since neither can change which records match.

This filter selects records; it does not merge channels. Each record SHALL keep its own channel metadata, so a plugin receiving two channels can still tell them apart and a downstream consumer is unaffected.

The entry channel a plugin injects on SHALL remain a single name. A payload is injected on exactly one channel, so a list has no meaning there and SHALL be refused: on `channel_in`, and on `channel` wherever the plugin's provider feeds the relay, because `channel` names the entry channel there too. A feeding plugin filters several channels with `channel_out`.

#### Scenario: Two names select both channels

- **GIVEN** a plugin configured with the filter `alerts,audit` on a relay that also produces `metrics`
- **WHEN** one record is released on each of the three channels
- **THEN** the plugin SHALL act on the `alerts` and the `audit` record, and the `metrics` record SHALL pass through the chain unchanged

#### Scenario: One name behaves as before

- **WHEN** a plugin is configured with the filter `alerts`
- **THEN** it SHALL act on exactly the records it acts on when that filter is a single name today

#### Scenario: No filter still matches everything

- **WHEN** a plugin is configured with no channel filter
- **THEN** it SHALL act on every record, whatever channel each names

#### Scenario: Each record keeps its own channel

- **GIVEN** a plugin configured with the filter `alerts,audit`
- **WHEN** it acts on a record released on `audit`
- **THEN** that record's channel metadata SHALL still read `audit`

#### Scenario: An empty segment is refused

- **WHEN** a plugin is enabled with the filter `alerts,,audit`
- **THEN** the enable SHALL be refused, naming the option

#### Scenario: A repeated name is accepted

- **WHEN** a plugin is enabled with the filter `alerts,alerts`
- **THEN** the enable SHALL succeed and the plugin SHALL act on exactly the `alerts` records

#### Scenario: A list on the entry side is refused

- **WHEN** a plugin that feeds the relay is enabled with more than one name in `channel_in`, or in `channel`
- **THEN** the enable SHALL be refused, stating that a payload is injected on exactly one channel

### Requirement: A plugin enable SHALL refuse a plain option it does not read

`flz plugin enable` SHALL refuse, naming it, any plain option it does not read, and SHALL write nothing for that enable.

The enable verb is open: the CLI hands a plain `--key value` it does not declare
to the controller instead of refusing it, because the words a provider owns must
reach the settings document. The controller reads `namespace` and
`relay_id` from the plain options and dropped every other key, so a
mistyped option, or one a help screen advertised, enabled a plugin configured
with less than the user wrote and reported success. The
`cli-command-surface` capability already requires an option a user typed
to be read or refused with the flag named; this is that rule at the one place
the enable's plain options are read. Provider settings are written `--X<key>`
and hooks `--hook`, so neither is affected.

#### Scenario: A removed flag is refused
- **WHEN** `flz plugin enable p relay_file_output --relay_id op --trigger on_output --Xpath=out.ndjson` is issued
- **THEN** the command MUST fail naming `trigger`
- **AND** no plugin entry MUST be written to the relay's declared plugin manifest

#### Scenario: An option nothing reads is refused
- **WHEN** an enable is issued with `--position 0`
- **THEN** the command MUST fail naming `position` and saying that provider settings are written `--X<key>`

#### Scenario: The options the enable reads are accepted
- **WHEN** an enable is issued with `--namespace`, `--relay_id`, a `--hook` with its sub-options and `--X` settings
- **THEN** the enable MUST NOT be refused for any of them

### Requirement: A bind SHALL bring up every declared plugin its table does not hold

Every bind of an instance SHALL bring up each binding of the instance's declared entry whose
plugin its extension table does not hold (a first bind, a wake, an un-park, a rebind to another
runner alike), and SHALL leave every binding the table already holds as it is, whatever phase
it is in. A table that is not empty SHALL NOT make the bind skip the declared bindings. A
binding of a non-shareable plugin SHALL be brought up in the instance's own table; a binding
of a shareable plugin SHALL subscribe the instance to the plugin's running copy on its
controller (or to the `single` copy), starting that copy if it is not running. A binding that
cannot be brought up (its provider not loaded, its plugin not enabled) SHALL be recorded as not
started with its reason and SHALL NOT stop the bind.

#### Scenario: A runtime plugin on a sleeper found at boot

- **GIVEN** a relay bound to a file output plugin that was asleep when the controller
  restarted, so its table is empty, and a listener then attached to it
- **WHEN** an input wakes it
- **THEN** its table SHALL hold both the file output plugin and the listener, running, before its
  first output is dispatched, and the file SHALL hold that output

#### Scenario: A rebind re-initialises nothing

- **WHEN** a running relay whose table holds all its declared bindings is rebound to
  another runner
- **THEN** no plugin SHALL be subscribed again and no provider's `init_provider/1` SHALL be
  called

#### Scenario: A missing provider does not stop the bind

- **WHEN** a relay binds on a controller lacking the provider of one of its two bindings
- **THEN** the relay SHALL run, the other binding SHALL be running, and the missing one
  SHALL be recorded as not started with `plugin.provider_not_loaded`

### Requirement: A plugin disable SHALL be addressed by its target

`flz plugin disable <name>` and every other disable of a plugin SHALL find the plugin by its
name in its namespace's declared set and remove it there, and every controller running a copy
of it SHALL stop that copy. Every binding to it SHALL stay on its relay. `flz plugin unbind
<name> --relay_id <op>` SHALL find the relay, or the partition group, from the declared
sets of the cluster's namespaces and remove the binding there, on every current member of a
group. Neither SHALL depend on which controller node ran the enable or the bind, or on any
node-local table: a plugin bound to a relay that moved to another controller node, or to a
member a rescale added, SHALL be disabled or unbound from any node, and the verb SHALL be
authorized in the namespace the resolution finds.

#### Scenario: Unbinding a plugin from a moved relay

- **GIVEN** a file output plugin bound through controller node A to a relay that A hosted
- **WHEN** the relay migrates to node B and `flz plugin unbind <name> --relay_id <op>`
  is then run through A (and, in a second run of the same setup, through a third node C that
  never held it)
- **THEN** the unbind SHALL succeed, the relay's declared bindings and B's extension table
  SHALL hold no entry for the plugin, and the convergence loop SHALL NOT start it again

#### Scenario: Disabling a plugin that runs on two controllers

- **GIVEN** a shareable plugin running on controllers A and B
- **WHEN** `flz plugin disable <name>` is run through a third controller C
- **THEN** no copy SHALL run on A or B, and the bound relays SHALL keep their bindings

### Requirement: A provider SHALL declare whether it can be shared and which options are live

`use Flezha.Plugin.ProviderBehaviour` SHALL take `shareable:` (default `false`) and an option
row SHALL take `live:` (default `false`); both SHALL be part of the provider's `Decl`, read by
the extension table and the plugin manager from the declaration only. A provider with a live
option SHALL implement `reconfigure/2`; a declaration with a live option and no
`reconfigure/2` SHALL fail to compile. The first-party providers `websocket`, `http_poll`,
`http_push` and `stdout` SHALL declare `shareable: true`; every other first-party provider
SHALL declare `false`. The Kafka package's `kafka_producer` SHALL declare `true`.

#### Scenario: The declaration answers sharing

- **WHEN** `Flezha.Plugin.Provider.Websocket.__provider__()` and
  `Flezha.Plugin.Provider.File.Source.__provider__()` are read
- **THEN** the first SHALL answer `shareable: true` and the second `shareable: false`

#### Scenario: A live option without its callback

- **WHEN** a provider declares an option `live: true` and does not define `reconfigure/2`
- **THEN** its module SHALL fail to compile with a message naming the option

### Requirement: A provider SHALL declare whether it is a relay plugin or a controller plugin

`use Flezha.Plugin.ProviderBehaviour` SHALL take `type: :relay` (the default) or
`type: :controller`; no other value SHALL compile. A controller provider SHALL define none of
the relay hook callbacks (`on_relay_init/2`, `on_input/2`, `on_output/2`,
`on_interval/2`, `on_message/2`, `on_snapshot/2`, `on_terminate/2`, `on_preview/2`, `on_call/2`) and a
relay provider none of the controller hook callbacks (`on_relay_started/2`,
`on_relay_stopped/2`, `on_relay_failed/2`, `on_dead_letter/2`,
`on_plugin_unhealthy/2`); a module that does SHALL fail to compile naming the callback.
`flz plugin help <provider>` SHALL print the type as `relay` or `controller`.

#### Scenario: Every first-party relay provider

- **WHEN** the `plugin_type/0` of every first-party provider but `hook_webhook` is read
- **THEN** each SHALL answer `:relay`, and `hook_webhook`'s SHALL answer `:controller`

#### Scenario: A controller provider with a relay hook

- **WHEN** a module declaring `type: :controller` defines `on_output/2`
- **THEN** it SHALL fail to compile with a message naming `on_output/2`

#### Scenario: The old type is gone

- **WHEN** a module declares `type: :io_binding`
- **THEN** it SHALL fail to compile naming the two allowed types

#### Scenario: A controller provider with the calls hook

- **WHEN** a module declaring `type: :controller` defines `on_call/2`
- **THEN** it SHALL fail to compile with a message naming `on_call/2`

### Requirement: A source SHALL be told whether its relay's input is durable

Every provider's options SHALL carry the host option `durable_input` once its plugin is
activated, true exactly when the relay journals its input (a durable steps relay, or a
flow relay with `effectively_once` or that sleeps), merged over the stored options so a
stored option cannot set it. A shared copy of a shareable plugin answers each injection
itself, so its options SHALL carry `durable_input: false`. A source that moves an upstream position (an offset, a cursor, an acknowledgement)
SHALL move it only past records whose verdict was `:ok`, after every confirmed batch when
`durable_input` is true.

#### Scenario: A flow relay with effectively-once

- **WHEN** a plugin is brought up on a flow relay whose class sets `effectively_once`
- **THEN** its options SHALL carry `durable_input: true`

#### Scenario: A plain flow relay on local disk

- **WHEN** a plugin is brought up on a flow relay with `durable: local`, no
  `effectively_once` and no `sleep_after`
- **THEN** its options SHALL carry `durable_input: false`

#### Scenario: A stored option cannot claim durability

- **WHEN** a plugin is enabled with the option `durable_input` set to true on a plain flow
  relay
- **THEN** the stored value SHALL be overwritten by the relay's setting, as
  `effectively_once` is, and the activated options SHALL carry `durable_input: false`

#### Scenario: A shared copy

- **WHEN** a shareable plugin's shared copy activates it
- **THEN** its options SHALL carry `durable_input: false`

### Requirement: A source SHALL be able to hand over a batch and wait once

`Flezha.Plugin.Host.inject_many/4` SHALL take a relay, a list of `{message, metadata}`, a
mode (`:verdict` or `:fire_and_forget`) and a timeout. With `:verdict` it SHALL keep up to
`plugin_ingest_window` records waiting for their verdicts at once, sending the next as each
answer arrives, and SHALL answer one verdict per record in the records' order,
`{:error, {:unreachable, :timeout}}` for each record unanswered at the deadline, the shape
`inject/5` answers a timeout with. The relay SHALL ingest the records in the order given. With `:fire_and_forget` it SHALL hand every record over without
waiting and answer `:ok`.

#### Scenario: A batch confirmed with one wait

- **GIVEN** a flow relay with `effectively_once`
- **WHEN** a source calls `inject_many/4` with 500 records in `:verdict` mode
- **THEN** it SHALL answer 500 verdicts in order, the input log SHALL hold the 500 records in
  that order, and the number of log syncs SHALL be lower than 500

#### Scenario: A duplicate in the batch

- **GIVEN** a relay that journals its input
- **WHEN** two records of one batch carry the same `flz.msg_id`
- **THEN** both verdicts SHALL be `:ok` and the relay SHALL receive the message once

#### Scenario: A deadline cuts the batch

- **WHEN** the relay has not answered every record of a batch by the deadline
- **THEN** every answered record SHALL keep its verdict, every other record SHALL answer
  `{:error, {:unreachable, :timeout}}`, and no reply SHALL reach the source after the call
  returns

#### Scenario: A batch larger than the window

- **GIVEN** `plugin_ingest_window` set to 4
- **WHEN** a source calls `inject_many/4` with 20 records in `:verdict` mode
- **THEN** at most 4 of them SHALL wait for a verdict at any moment, and the 20 verdicts
  SHALL answer in order

### Requirement: A provider MAY declare telemetry verbs of its own

`use Flezha.Plugin.ProviderBehaviour` SHALL accept `telemetry: [verb: :metric | :event]`, the
verbs a provider emits beyond the declared vocabulary, each optionally with its meaning,
`verb: {kind, "what it says"}`, or its meaning and a log level, `verb: {kind, "what it says",
level}`, and `__provider__/0` SHALL answer them.
A declaration whose own verb repeats a vocabulary verb, whose kind is neither `:metric` nor
`:event`, whose meaning is blank, or whose level is not one of `Logger`'s, SHALL fail the
provider's compilation naming the verb. A declaration MAY also carry `description:`, one line
saying what the plugin does, which the telemetry bridge gives the provider's observability
source; a non-binary or blank description SHALL fail the provider's compilation. An own verb a provider emits
without declaring it SHALL reach handlers attached to its event and no exporter; a first-party
provider's emitted verbs SHALL stay judged by the telemetry bridge's tree walk.

#### Scenario: An own verb

- **WHEN** a provider declares `telemetry: [rebalanced: :event]` and emits `:rebalanced`
- **THEN** the bridge SHALL map it as an event

#### Scenario: An own verb that repeats the vocabulary

- **WHEN** a provider declares `telemetry: [shipped: :metric]`
- **THEN** its compilation SHALL fail naming `shipped`

#### Scenario: An own verb with a meaning and a level

- **WHEN** a provider declares `telemetry: [rebalanced: {:event, "The sink moved its partitions.", :warning}]` and emits `:rebalanced`
- **THEN** the bridge SHALL map it as an event carrying that meaning and write one `Logger` line at `:warning` for it

#### Scenario: A level that is not a Logger level

- **WHEN** a provider declares `telemetry: [rebalanced: {:event, "The sink moved its partitions.", :loud}]`
- **THEN** its compilation SHALL fail naming `rebalanced`

#### Scenario: A declared description reaches the source listing

- **WHEN** a provider declaring `description: "Reads orders from the acme queue."` is loaded and its events are derived
- **THEN** its observability source SHALL be listed with that description

### Requirement: A source that cannot route by key SHALL refuse a partitioned relay one way

A source that cannot tell which member a record's key belongs to SHALL declare
`partitioned: :refuse` in its source declaration. The source kit SHALL then refuse, when the
plugin is brought up on a member at bind, options that carry the partition options a group
member's plugin is given, with an error carrying the provider's name and the member that SHALL
render as `plugin.partitioned_host`, and SHALL accept every other plugin. No provider SHALL write
this check itself, and the package API SHALL carry no function for it.

#### Scenario: A table reader on a partitioned relay

- **WHEN** a source declaring `partitioned: :refuse` is enabled on a member of a partition group
- **THEN** the enable SHALL be refused with `plugin.partitioned_host` naming the provider and
  the member

#### Scenario: A relay that is not partitioned

- **WHEN** the same source is enabled on a relay outside any partition group
- **THEN** the enable SHALL succeed

### Requirement: A pull source SHALL be one declaration, its own verdicts and its own read loop

A provider SHALL declare a source with the `source:` key of its declaration, naming its worker
module, `partitioned: :refuse | :own_copy` and the facts its worker reports. From that declaration
the core SHALL derive, for every source alike: the refusal of a plugin that names no entry
channel, the partitioned refusal when declared, the worker specs, the health verdict (a worker
that does not answer its probe inside twice the probe budget reads `{:fail_open,
:probe_timeout}`; `failure_threshold` consecutive failures read `{:unhealthy, {reason, error}}`, `reason` being
the declaration's `failure_reason`, `:read_failed` unless declared; a worker idle past its window
reads the shared idle verdict), and that the provider feeds the relay. The entry-channel check
SHALL come before the partitioned refusal. The provider SHALL write verdict clauses only for facts
of its own. Every provider declaration, a source's or not, SHALL define a no-op `on_terminate/2`
unless the provider writes one, so a stored binding naming that hook binds. A provider worker SHALL be built on the one worker kit, which answers the health
probe from the declared facts, stops when its host stops, and keeps the shared failure counters.
The package API SHALL NOT carry `require_entry_channel/2`, `refuse_partitioned/2`,
`park_or_reply/2`, `release_waiters/1` or `saturated?/1`.

#### Scenario: A source with no entry channel is refused by the kit

- **WHEN** a plugin of any declared source is enabled naming neither `channel` nor `channel_in`
- **THEN** the enable SHALL be refused with `plugin.entry_channel_required` naming both spellings, and the provider's own module SHALL hold no code for that check

#### Scenario: Every source reports repeated failures alike

- **GIVEN** the file source, the Postgres poll source and a package source, each with `failure_threshold: 2`, none declaring a `failure_reason`
- **WHEN** two consecutive reads fail on each
- **THEN** each SHALL report `{:unhealthy, {:read_failed, error}}`, while the HTTP poll source, declaring `:poll_failing`, SHALL report `{:unhealthy, {:poll_failing, error}}`

#### Scenario: A stored binding that names on_terminate still binds

- **WHEN** a controller boots holding a source binding, and a websocket binding, each stored with the hook `on_terminate`
- **THEN** both bindings SHALL be brought up and the source SHALL feed its relay

### Requirement: Every bounded sink queue SHALL be admitted in one place

A sink that queues outputs SHALL admit them through the controller's one sink admission, which
reads the queue's `queue_overflow` word (`drop_nothing`, the default for every sink,
`drop_oldest`, `drop_newest`). Under `drop_nothing` a dispatch arriving at a full queue SHALL wait
for room; a sink that declares `queue_block_timeout_ms` SHALL overflow the output when that time
passes, counting a plain output and parking a guaranteed one, and a sink that declares none SHALL
hold the caller until room is made. A dispatch on its way when the queue filled MAY be admitted
past the bound, and no other output SHALL be. A sink whose every enqueue is a call (the "delay,
never loss" sinks) SHALL hold its caller under a hard bound; a sink whose dispatch casts while its
full flag is down (`http_push`, Kafka, RabbitMQ) SHALL keep one flag per worker, up while any of
its queues is full. A synchronous caller of a sink's enqueue SHALL be answered
`{:error, {:queue_full, :timeout}}` at the block timeout, while a waiting dispatch's output
overflows. The file and Postgres sinks SHALL batch through one
loop whose provider callback commits one batch.

#### Scenario: One admission bounds every policy sink

- **GIVEN** `http_push`, `kafka_producer` and `rabbitmq_producer`, each with `queue_max: 10`, `queue_block_timeout_ms: 500` and the default policy, against an endpoint that is down
- **WHEN** each receives 12 guaranteed outputs
- **THEN** each queue SHALL hold at most 10 plus the outputs already on their way when it filled, and every output past the bound SHALL be parked within 500 ms of its dispatch

#### Scenario: A delay-never-loss sink holds its caller

- **WHEN** the file sink's queue reaches its `queue_max`
- **THEN** the next dispatch SHALL wait until the queue drains below the bound, and no output SHALL be dropped

### Requirement: An option shared by providers SHALL be declared once

Each option several providers take SHALL be declared once (`channel`, `channel_in`,
`channel_out`, `plugin_id`, `failure_threshold`, `idle_after_ms`, `queue_max`, `queue_overflow`,
`queue_max_bytes`, `queue_block_timeout_ms`, `retry_backoff_ms`, `retry_max_backoff_ms`), as a fragment a provider's
declaration takes, so their type, default, conversion and help text are one. A credential SHALL
be one row kind: an inline value, an environment variable named by `<name>_env` read again at
every client start, and a transient copy shown as `**redacted**`; the Postgres, RabbitMQ and Kafka
providers SHALL use it, and the Kafka option is `password_env` (`sasl_password_env` is gone). A
header list SHALL be one row type validated by the option table, not by another connector.

#### Scenario: The Kafka password variable

- **WHEN** a Kafka producer is enabled with `password_env: KAFKA_PASS` and `KAFKA_PASS` is set
- **THEN** the client SHALL authenticate with that password, `flz plugin info` SHALL show `**redacted**` for it, and an enable naming `sasl_password_env` SHALL be refused as an option the provider does not read

#### Scenario: One help text for a shared option

- **WHEN** `flz plugin help` is read for the file source and the Kafka consumer
- **THEN** their `failure_threshold` rows SHALL carry the same type, default and text except the noun each provider passes

### Requirement: A plugin package SHALL reach the core only through declared modules

A plugin package SHALL name only the core modules declared as the package surface. The Postgres
CDC and RabbitMQ packages SHALL reach Postgres connection handling and SQL helpers only through
the declared shared connection module, never through the poll source's own modules; the Postgres
sink and poll source SHALL stay in the core.

#### Scenario: A package naming a connector's internals

- **WHEN** a package module aliases `Flezha.Plugin.Provider.PostgresPoll.Query`
- **THEN** the package-surface test SHALL fail naming the package file and the module

### Requirement: A pid-typed provider option SHALL accept only a pid

A provider option declared with type `pid` SHALL accept only a process identifier given in the
controller's own VM. A string, an atom or any other value, including the name of a registered
process, SHALL be refused naming the option, so no caller can direct a plugin's messages to a
process of the controller by name.

#### Scenario: A registered name is refused

- **WHEN** `relay_beam_msg` is enabled with `recv_pid: "Elixir.Flezha.AuthZ.TokenStore"`
- **THEN** the enable SHALL be refused naming `recv_pid`, and no message SHALL be sent to that
  process

#### Scenario: A pid is accepted

- **WHEN** code in the controller's VM enables `relay_beam_msg` with `recv_pid` set to its own pid
- **THEN** the enable SHALL succeed, and each record SHALL reach that pid
