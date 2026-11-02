# plugin-relay-hooks Specification

## Purpose

How a relay plugin attaches to a relay through hooks. A relay host declares its extension points
once in a catalog; a plugin is one `%Flezha.Plugin{}` struct whose hooks attach to those points as
data, and `Flezha.Plugin.Extension.Table` dispatches each point by its kind (`:fold`, `:transform`,
`:fanout`, `:request`, `:timer`) through a chain composed when the hook subscribes, with an
active-extensions bitset so a point no plugin uses costs nothing. Hooks run in a declared order,
their options are typed per kind, an invalid hook is refused before anything durable is written,
user text never creates atoms, a plugin may have no hooks, and hooks survive a relay handover. The
Overwatch publishes `on_preview` and `on_call`. The hooks a controller plugin hears are
`plugin-controller-hooks`.

## Requirements

### Requirement: Extension-point catalog

The system SHALL declare every extension point exposed by a host module (e.g. `Flezha.Relay.Overwatch`) in a single module-level catalog. Each catalog entry MUST specify the extension name (atom), its dispatch kind (one of `:fold`, `:transform`, `:fanout`, `:request`, `:timer`), and its context-struct module. The catalog SHALL be the sole source of truth for which extension points exist on that host; no runtime registration of new extension names is permitted. A user and a plugin author call these points hooks, and every surface they type or read uses that word.

#### Scenario: Overwatch declares its catalog
- **WHEN** `Flezha.Relay.Overwatch` is compiled
- **THEN** the module SHALL declare an `@extensions` attribute containing at least these entries: `{:on_relay_init, :fold, Flezha.Plugin.Extension.Ctx.Init}`, `{:on_input, :transform, Flezha.Plugin.Extension.Ctx.IO}`, `{:on_output, :transform, Flezha.Plugin.Extension.Ctx.IO}`, `{:on_interval, :timer, Flezha.Plugin.Extension.Ctx.Tick}`, `{:on_message, :request, Flezha.Plugin.Extension.Ctx.Msg}`, `{:on_snapshot, :fanout, Flezha.Plugin.Extension.Ctx.Snapshot}`, `{:on_terminate, :fanout, Flezha.Plugin.Extension.Ctx.Terminate}`

#### Scenario: A hook on an undeclared point fails
- **WHEN** a plugin names a hook absent from the host's catalog
- **THEN** `Flezha.Plugin.Extension.Table.bulk_subscribe/2` MUST return `{:error, {plugin_id, {:unknown_hook, name}}}` and state MUST NOT be mutated

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

Transform subscribers SHALL be implemented such that their callbacks return in O(1) wall-clock time relative to any external resource (broker round-trips, database queries, downstream HTTP calls, etc.). The chain runner is invoked inline from the host process's message handler — synchronous waits on external resources within a transform subscriber inflate the host's `:message_queue_len` under sustained input above the resource's drain rate. Subscribers that need to perform external I/O SHALL admit work into a dedicated worker (per the `plugin-kafka`'s `cast_enqueue` pattern) and return immediately, surfacing failures through telemetry rather than the chain's return value.

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

The system SHALL dispatch `:request`-kind extensions by looking up exactly one subscriber by a request key supplied in the ctx. The request key SHALL be a string, both in the hook's `key` sub-option and in the dispatch ctx, so a user-supplied key is never interned as an atom. Multiple plugins MUST NOT be permitted to hook the same `:request`-kind point with the same key on the same host. The subscriber's callback MUST be invoked in a short-lived Task supervised by a host-owned supervisor so the host's mailbox is not blocked by the callback.

#### Scenario: Request routes to the keyed subscriber
- **WHEN** `Extension.run/3` is called for a `:request` extension with a ctx whose key matches a registered subscriber
- **THEN** that subscriber's callback MUST be invoked with the ctx inside a Task, and the reply MUST be sent via the ctx's reply mechanism

#### Scenario: A key declared on the CLI is reachable at dispatch
- **WHEN** a plugin is enabled with `--hook=on_message --on_message.key=orders` and a request is dispatched with key `"orders"`
- **THEN** that subscriber's callback MUST be invoked

#### Scenario: Duplicate key subscription rejected
- **WHEN** a plugin attempts to hook a `:request` point with a key already in use by another plugin on the same host
- **THEN** `Extension.Table.subscribe/3` MUST return `{:error, {:key_conflict, extension_name, key}}` and the existing subscriber MUST remain registered unchanged

#### Scenario: Unknown request key
- **WHEN** `Extension.run/3` is called for a `:request` extension with a key not registered
- **THEN** the reply mechanism MUST deliver `{:error, {:unknown_key, extension_name, key}}` to the caller and the host MUST NOT crash

### Requirement: Timer-kind dispatch

The system SHALL dispatch `:timer`-kind extensions via scheduled invocations. When a plugin has the hook `{:on_interval, [interval_ms: N]}`, the extension table SHALL schedule a timer that sends `{:extension_timer, extension_name, plugin_id}` to the host process every `N` milliseconds. The host SHALL forward these timer messages into `Extension.Table.fire_timer/3`, which invokes the subscriber and reschedules the next tick.

#### Scenario: A timer hook schedules periodic invocation
- **WHEN** a plugin has the hook `{:on_interval, [interval_ms: 1_000]}`
- **THEN** the subscriber's `on_interval/2` callback MUST be invoked approximately every 1_000 ms until the plugin is unsubscribed

#### Scenario: Unsubscribe cancels timer
- **WHEN** a plugin with an active `:on_interval` hook is unsubscribed
- **THEN** all pending timer messages for that plugin MUST be cancelled and no further invocations MUST occur

### Requirement: Active-extensions fast path

The system SHALL maintain an `active_mask` integer in `Extension.Table` in which bit N is set if and only if extension at catalog index N has at least one subscriber. `Extension.run/3` MUST check this mask before invoking any dispatch logic; when the mask bit is clear the call MUST return the ctx unchanged (for `:fold` / `:transform`), `:ok` (for `:fanout`), or an empty-key outcome (for `:request`) without invoking user code or allocating a list. A host's hot-path transform dispatch (the Overwatch's `:on_input` and `:on_output`) SHALL ask `Extension.active?/2` before building the chain's context: an unsubscribed chain is a passthrough answered before its `Ctx.IO` exists, so the fast path costs no allocation either.

#### Scenario: Extension with no subscribers is zero-cost
- **WHEN** `Extension.run/3` is called for an extension with zero subscribers
- **THEN** no provider callback MUST be invoked and no subscriber-specific data structure MUST be traversed

#### Scenario: Bit flips on first and last subscriber
- **WHEN** the first subscriber is added to an extension, its mask bit MUST be set; when the last subscriber for that extension is removed, its mask bit MUST be cleared

#### Scenario: Unsubscribed transform chain builds no context
- **WHEN** a relay with no `:on_output` subscriber flushes an output
- **THEN** the output passes through unchanged without a `Ctx.IO` being built or `Extension.run/3` being entered

### Requirement: Dispatch chains are pre-composed at subscribe time

For `:fold`, `:transform`, and `:fanout` extensions, the system SHALL build the dispatch function once when subscriptions change (on subscribe, unsubscribe, or reorder), and SHALL NOT traverse the subscriber list on each `Extension.run/3` call. The dispatch path MUST NOT allocate per-subscriber control-flow tuples (e.g. `{:cont, _}` / `{:halt, _}`) on each invocation.

#### Scenario: Chain rebuilt on subscribe
- **WHEN** a new subscriber is added to a `:fold` or `:transform` extension that already has subscribers
- **THEN** the pre-composed dispatch function for that extension MUST be rebuilt so subsequent `Extension.run/3` calls include the new subscriber in position order

#### Scenario: Dispatch path does not traverse subscriber list
- **WHEN** `Extension.run/3` is called with N subscribers attached (N ≥ 1)
- **THEN** the call MUST invoke the pre-composed dispatch function and MUST NOT call `Enum.reduce/3`, `Enum.reduce_while/3`, or any list-iteration primitive over the subscriber list

### Requirement: Unified plugin struct with hooks

The system SHALL represent a plugin as a single `%Flezha.Plugin{}` struct with fields `id` (string), `provider` (a module implementing the relevant hook callbacks), `opts` (a map of provider configuration), and `hooks` (a list of `{hook_name, hook_opts}` tuples). A single `%Flezha.Plugin{}` MAY have hooks on several points.

At the boundary, a hook name MAY be supplied as text or as an atom and hook
opts MAY be supplied as a map or a keyword list: the shapes the CLI, the `hooks`
key of a `--plugins` spec or of an admin `enable_plugin` call, an installed
package manifest and direct construction produce. `Extension.Table.subscribe/3`
SHALL canonicalise all of them to `{atom, keyword}` with typed values; the
canonical form is what the table stores and what every later reader observes.

#### Scenario: Plugin hooks several points
- **WHEN** a plugin is constructed with `hooks: [{:on_input, [position: 0]}, {:on_interval, [interval_ms: 5_000]}, {:on_terminate, []}]` and enabled via `Extension.Table.subscribe/3`
- **THEN** the provider module's `on_input/2`, `on_interval/2`, and `on_terminate/2` callbacks MUST each be invoked on the corresponding dispatch event
- **AND** a single `unsubscribe/2` MUST remove all three hooks atomically

#### Scenario: Provider missing a hooked callback
- **WHEN** a plugin has the hook `{:on_input, _}` but its `provider` module does not export `on_input/2`
- **THEN** `Extension.Table.subscribe/3` MUST return `{:error, {:missing_callback, :on_input}}` and state MUST NOT be mutated

#### Scenario: A boundary shape is canonicalised
- **WHEN** a plugin is constructed with `hooks: [{"on_interval", %{"interval_ms" => "5000"}}]`
- **THEN** the table MUST store `{:on_interval, [interval_ms: 5000]}`

#### Scenario: Hooks written under another key are refused
- **WHEN** a `--plugins` spec, an admin `enable_plugin` call or a direct construction carries its hooks under a `subscriptions` key and has no `hooks` key
- **THEN** the plugin MUST be refused with an error the caller receives
- **AND** no plugin entry MUST be written to the relay's declared plugin manifest

### Requirement: Hook ordering for ordered kinds

The system SHALL honor a `position` option in hook opts for `:fold` and `:transform` extensions. `position` SHALL be an integer once canonicalised; a value that cannot be coerced to one MUST be refused with an error naming `position`, and MUST NOT reach chain insertion. Subscribers MUST be invoked in ascending position order. When a new subscriber's position collides with an existing one, subsequent subscribers MUST be shifted upward and renumbered contiguously. `:fanout`, `:request`, and `:timer` kinds MUST reject `position` in hook opts with `{:error, :position_not_applicable}`.

#### Scenario: Position-based insertion shifts followers
- **WHEN** three subscribers exist at positions 0, 1, 2 and a fourth is added with `position: 1`
- **THEN** the new subscriber occupies position 1, the previous position-1 subscriber is renumbered to 2, and the previous position-2 subscriber is renumbered to 3

#### Scenario: A position declared on the CLI orders the chain
- **WHEN** a plugin is enabled with `--hook=on_output --on_output.position=0`
- **THEN** the hook is inserted at position 0 of that point's chain

#### Scenario: Position on fanout-kind rejected
- **WHEN** a plugin hooks a `:fanout` point with `position: 0` in its hook opts
- **THEN** `Extension.Table.subscribe/3` MUST return `{:error, :position_not_applicable}`

### Requirement: Hook sub-options SHALL have a declared type per dispatch kind

The system SHALL declare, in a single per-kind table owned by
`Flezha.Plugin.Extension.Table`, the name and type of every hook sub-option each
dispatch kind accepts: `:timer` accepts `interval_ms` (integer), which has no
default and is required; `:fold` accepts `position` (integer), `timeout_ms`
(integer) and `on_error` (one of `"fail"`, `"empty"`); `:transform` accepts
`position` (integer); `:request` accepts `key` (string); `:fanout` accepts none.
That declaration SHALL be the sole authority on which sub-options exist and what
type each carries.

`Extension.Table.subscribe/3` SHALL canonicalise a `%Flezha.Plugin{}` against
that declaration before validating it, coercing each value to its declared type
and rejecting a value that cannot be coerced with an error naming the
sub-option. The canonical struct SHALL be what the table stores, so a provider
restart, an unsubscribe and a handover all observe typed values. Canonicalising
an already-typed hook SHALL leave it unchanged.

A sub-option not present in the declaration for that kind SHALL be refused with
`{:error, {:unknown_hook_opt, kind, name}}`. A kind that carries a
sub-option other kinds accept SHALL declare it refused, naming its own error, so
the declaration answers every name for every kind: `position` on `:fanout`,
`:request` and `:timer` answers `{:error, :position_not_applicable}`.

#### Scenario: A sub-option declared as text reaches the table as its declared type
- **WHEN** a plugin is enabled with `--hook=on_interval --on_interval.interval_ms=5000`
- **THEN** the plugin has the hook `{:on_interval, [interval_ms: 5000]}` and the enable succeeds

#### Scenario: The same declaration works from the relay-start spec
- **WHEN** a relay is started with a `--plugins` spec whose hook opts are `{"interval_ms": "5000"}`
- **THEN** the plugin has the hook `{:on_interval, [interval_ms: 5000]}`

#### Scenario: A timer hook without an interval is refused
- **WHEN** a plugin is enabled with `--hook=on_interval` and no `--on_interval.interval_ms`
- **THEN** the enable MUST be refused with an error naming `interval_ms`
- **AND** no plugin entry MUST be written to the relay's declared plugin manifest

#### Scenario: An ill-typed sub-option is refused, never raised
- **WHEN** a plugin hooks a `:fold` or `:transform` point with `position: "first"`
- **THEN** `Extension.Table.subscribe/3` MUST return an error naming `position`, MUST NOT raise, and state MUST NOT be mutated

#### Scenario: An undeclared sub-option is refused
- **WHEN** a plugin hooks a `:timer` point with a sub-option the declaration does not list
- **THEN** `Extension.Table.subscribe/3` MUST return `{:error, {:unknown_hook_opt, :timer, name}}` and state MUST NOT be mutated

#### Scenario: Canonicalisation is idempotent
- **WHEN** a hook whose values are already of their declared types is canonicalised
- **THEN** the resulting hook MUST be identical to the input

### Requirement: User-supplied hook text SHALL NOT create atoms

The system SHALL resolve a hook name supplied by a user against the
host catalog as text, and a sub-option name against the declaration for that
kind, without interning either. No sub-option value supplied by a user
SHALL be converted to an atom. A name that resolves to nothing SHALL be refused
rather than created.

#### Scenario: An unknown hook name is refused without being interned
- **WHEN** a plugin is enabled naming a hook absent from the host catalog
- **THEN** the enable MUST be refused with `{:error, {:unknown_hook, name}}` and no atom MUST be created for that name

#### Scenario: A closed-set sub-option is matched as text
- **WHEN** a plugin hooks a `:fold` point with `on_error` set to `"empty"`
- **THEN** the hook MUST be accepted and the fold chain MUST apply the empty-on-error behaviour

### Requirement: A user SHALL be able to enable a plugin with no hooks

`flz plugin enable` SHALL treat an enable that names no `--hook` as a plugin with an empty hook list, and SHALL offer no second spelling of that declaration.

An empty hook list is a supported state everywhere below the CLI:
canonicalisation folds it to an empty canonical list, the io-bound predicate
answers false for it, and no output plugin is built.

The inbound half of a relay plugin does not depend on any hook. A
provider's declared worker holds the connection and injects what arrives
through the host's one inject, so a plugin with no hooks still feeds its
relay. The hook list governs only what the plugin sends. A controller plugin
with no hooks receives nothing.

#### Scenario: An enable naming no hook has no hooks
- **WHEN** `flz plugin enable p relay_websocket --relay_id op --Xuri=ws://host/in` is issued with no `--hook`
- **THEN** the plugin's stored hook list MUST be empty
- **AND** no outbound queue MUST be allocated for it
- **AND** frames arriving on its connection MUST still reach the relay's input

#### Scenario: An echo is still available by name
- **WHEN** an enable is issued with `--hook=on_input`
- **THEN** the plugin MUST have the hook `on_input` and every payload the relay ingests MUST be dispatched to the provider's `on_input/2`

### Requirement: Hooks preserved across relay handover

The system SHALL carry no hook list in a relay's handover state. The destination
Overwatch SHALL build its extension table from the relay's declared bindings, resolved
against their plugins' entries, exactly as a bind does, so the hooks after a handover are the
ones the relay's declared entry holds. Runtime state that is not transportable (monitor
refs, timer refs, subscriber-owned pids) MUST be established afresh on the destination node.
After the handover the dispatch behaviour MUST match the pre-migration state.

#### Scenario: Hooked plugins survive migration
- **WHEN** a relay with bindings on `:on_input` and `:on_message` is migrated to a new compute engine
- **THEN** after migration `Extension.run(:on_input, ctx)` MUST invoke the same set of subscribers as before migration, and `Extension.run(:on_message, ctx_with_key)` MUST route to the same handler

#### Scenario: Subscriber-owned pids are restarted on destination
- **WHEN** a subscriber with a monitored pid is migrated
- **THEN** the destination node MUST spawn a new process for that subscriber and establish a fresh monitor; the source node's pid and monitor ref MUST NOT be transported

### Requirement: An invalid hook SHALL be refused before any durable write

The system SHALL canonicalise and type-check a plugin's hooks against
the host catalog before writing declared intent for that plugin. An enable
refused for an ill-typed, undeclared or unknown hook SHALL leave no
durable record, so the convergence loop is never handed intent that can never be
satisfied. The declared intent that is written SHALL hold canonical, typed
values.

#### Scenario: A refused enable persists nothing
- **WHEN** `flz plugin enable` is issued with a sub-option that cannot be coerced to its declared type
- **THEN** the command MUST fail with an error naming the sub-option
- **AND** no plugin entry MUST be written to the relay's declared plugin manifest

#### Scenario: A stored declaration holds typed values
- **WHEN** an enable succeeds with a sub-option supplied as text
- **THEN** the durable declared intent for that plugin MUST hold the coerced value, not the text

### Requirement: Surfaces that report a plugin's hooks SHALL call them hooks

`flz plugin help <provider_id>` and `flz relay info` SHALL name a plugin's hooks as hooks, in text and in JSON.

#### Scenario: Plugin help lists hooks as the flag a user types
- **WHEN** `flz plugin help relay_file_output` is run
- **THEN** it SHALL list `SUPPORTED HOOKS:` with one `--hook=<name>` line per hook callback the provider declares
- **AND** its common options SHALL name neither `--trigger` nor `--position`

#### Scenario: Relay info prints hooks
- **WHEN** `flz relay info` reports a relay with a plugin hooked on `on_output` at position 0
- **THEN** the plugin's text output SHALL carry a `hook: on_output` line with its opts

#### Scenario: Relay info JSON uses the spec shape
- **WHEN** `flz relay info` answers in JSON for the same relay
- **THEN** the plugin SHALL carry `hooks` as a list of `{"name": ..., "opts": {...}}` objects, the shape a `--plugins` spec accepts

### Requirement: The Overwatch SHALL publish an `on_preview` hook that carries no output

`Flezha.Relay.Overwatch` SHALL declare the catalogue entry
`{:on_preview, :fanout, Flezha.Plugin.Extension.Ctx.IO}`. The hook SHALL carry frames meant
for listeners only: a relay's parts and its dead-letter notices. No output, input or
durable record SHALL ever be dispatched on it, and a frame dispatched on it SHALL NOT be
dispatched on `on_output` or `on_input`. Because it is a `:fanout` hook, a subscriber SHALL
see every frame, no subscriber's answer SHALL change what another sees, and a raising
subscriber SHALL be logged and SHALL NOT stop the others. `MonitoredIOForward` SHALL be the
one first-party provider implementing `on_preview/2`, and a listener attached for outputs
SHALL subscribe it to both `on_output` and `on_preview`. No first-party output plugin provider SHALL
implement `on_preview/2`.

#### Scenario: An output plugin cannot subscribe to previews

- **WHEN** `flz plugin enable` is asked to attach the Kafka producer with the hook
  `on_preview`
- **THEN** the enable SHALL be refused naming the missing `on_preview` callback, as any
  hook a provider does not implement is refused, and no plugin row SHALL remain

#### Scenario: A preview reaches every listener and nothing else

- **GIVEN** a relay with a file output plugin and two listeners
- **WHEN** it produces one part
- **THEN** both listeners SHALL receive it and the file output plugin's callbacks SHALL NOT be
  called

### Requirement: The Overwatch SHALL publish an `on_call` hook that carries a relay's calls

`Flezha.Relay.Overwatch` SHALL declare the catalogue entry
`{:on_call, :fanout, Flezha.Plugin.Extension.Ctx.IO}`. The hook SHALL carry, for listeners only,
each call the relay takes and each call's outcome (`relay-listener`). No output, input, part
or dead-letter notice SHALL be dispatched on it, and nothing dispatched on it SHALL be
dispatched on `on_output`, `on_input` or `on_preview`. Because it is a `:fanout` hook, a
subscriber SHALL see every event, no subscriber's answer SHALL change what another sees, and a
raising subscriber SHALL be logged and SHALL NOT stop the others. The Overwatch SHALL build no
event while no plugin of the relay is subscribed to it. `MonitoredIOForward` SHALL be the one
first-party provider implementing `on_call/2`, and SHALL send an event as it sends an output.

#### Scenario: An output plugin cannot subscribe to calls

- **WHEN** `flz plugin enable` is asked to attach the Kafka producer with the hook `on_call`
- **THEN** the enable SHALL be refused naming the missing `on_call` callback, and no plugin
  row SHALL remain

#### Scenario: A call reaches a calls listener and nothing else

- **GIVEN** a relay with a file output plugin, an output listener and a calls listener
- **WHEN** it is called and answers
- **THEN** the calls listener SHALL receive the call and its outcome, the output listener and
  the file output plugin SHALL receive the answer as an output, and neither SHALL receive a
  call event
