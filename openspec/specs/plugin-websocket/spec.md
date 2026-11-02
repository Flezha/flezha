# plugin-websocket Specification

## Purpose

The websocket plugin: a connection a relay reads from and writes to. It names its entry channel with
an option, and can change what it is subscribed to without reconnecting, from a polled source or a
static item list, committing a delta only on its acknowledgement and restoring the durable applied
set on every connect. As an output it sends relay outputs on the connection it holds, queuing them
while the socket is down and confirming delivery by a declared acknowledgement mode, without
blocking the socket on the relay. Two watchdogs keep the connection alive: one reconnects only a
dead socket, the other re-subscribes in place when data stops on a live one, with O(1) tracking per
frame.

## Requirements

### Requirement: A websocket plugin SHALL accept an optional polled subscription source

`Plugin.Provider.Websocket` SHALL accept the optional opts `subscription_url`,
`subscription_interval_ms` (minimum `1000`, default `30000`), `subscription_headers`,
`subscribe_template` and `unsubscribe_template`, normalised from string or integer
identically to the existing `idle_timeout_ms`. A plugin configured with
`subscription_url` SHALL poll that URL on the configured interval and treat the
response as the desired subscription set.

The response SHALL be `{"version": <integer>, "items": [<string>, …]}`. Item strings
SHALL be treated as opaque: the provider SHALL NOT parse, validate or interpret them.
Attach-time validation SHALL reject a scheme other than http/https, a URL with no
host, an interval below the minimum, malformed headers, a `subscription_url` without
both templates, and any template lacking the `{{items}}` placeholder.

#### Scenario: Attach with a valid subscription source
- **WHEN** a plugin is enabled with `subscription_url`, `subscription_interval_ms: 30000`, and both templates
- **THEN** the plugin SHALL start and SHALL poll the URL every 30 s

#### Scenario: Attach with a template missing the items placeholder is rejected
- **WHEN** a plugin is enabled with a `subscribe_template` that contains no `{{items}}` placeholder
- **THEN** the enable SHALL be rejected with a validation error
- **AND** no connection SHALL be opened

#### Scenario: Attach with an interval below the minimum is rejected
- **WHEN** a plugin is enabled with `subscription_interval_ms: 100`
- **THEN** the enable SHALL be rejected with a validation error

#### Scenario: Item strings are not interpreted
- **WHEN** the source returns items that are not valid symbols for the upstream protocol
- **THEN** the provider SHALL still render them into the subscribe template unchanged
- **AND** SHALL NOT reject them locally

### Requirement: Reconcile SHALL send delta frames on the established connection

On each poll cycle the provider SHALL compute `added = desired − applied` and
`removed = applied − desired`. When `added` is non-empty it SHALL render
`subscribe_template` with `{{items}}` substituted by `added` as a JSON array and
`{{req_id}}` by a fresh monotonically increasing integer, and send it on the
**existing** connection. When `removed` is non-empty it SHALL do the same with
`unsubscribe_template`.

The provider SHALL NOT exit its process, tear down the socket, or reconnect in order
to apply a subscription change. When both sets are empty the provider SHALL send
nothing. A send attempted while the underlying websocket is transiently unestablished
SHALL be skipped with a `:warning` log and retried on the next cycle, never raising.

#### Scenario: Added item is subscribed in place
- **GIVEN** a connected plugin whose applied set is `["a"]`
- **WHEN** the source begins returning `["a", "b"]`
- **THEN** the provider SHALL send a rendered subscribe frame naming only `b`
- **AND** the Fresh connection process SHALL be the same process before and after

#### Scenario: Removed item is unsubscribed in place
- **GIVEN** a connected plugin whose applied set is `["a", "b"]`
- **WHEN** the source begins returning `["a"]`
- **THEN** the provider SHALL send a rendered unsubscribe frame naming only `b`
- **AND** the Fresh connection process SHALL be the same process before and after

#### Scenario: Simultaneous addition and removal sends both frames
- **GIVEN** a connected plugin whose applied set is `["a", "b"]`
- **WHEN** the source begins returning `["a", "c"]`
- **THEN** the provider SHALL send a subscribe frame naming `c` and an unsubscribe frame naming `b`

#### Scenario: Unchanged source sends nothing
- **GIVEN** a connected plugin whose applied set equals the source's set
- **WHEN** three poll cycles elapse
- **THEN** the provider SHALL send no subscribe or unsubscribe frame

#### Scenario: Send against an unestablished socket is non-fatal
- **WHEN** a reconcile attempts to send while the underlying websocket is transiently `nil`
- **THEN** the send SHALL be skipped with a `:warning` log
- **AND** the provider process SHALL stay alive and retry on the next cycle

### Requirement: A delta SHALL be committed only on a successful acknowledgement

The provider SHALL correlate each rendered frame with its `req_id` and SHALL fold its
delta into the applied set only when an inbound frame reports success for that
`req_id`. A failed, malformed, or unanswered request SHALL leave the applied set
unchanged, so the next cycle re-derives and re-sends an identical delta.

When the upstream protocol echoes no correlation id, the provider SHALL support an
explicit ack-free mode in which a delta commits on successful send; drift SHALL still
be corrected by the next reconcile cycle.

#### Scenario: Acknowledged subscribe commits
- **WHEN** a subscribe frame with `req_id` N is sent and a success acknowledgement for `req_id` N arrives
- **THEN** the item SHALL be part of the applied set
- **AND** the next cycle SHALL send no frame for it

#### Scenario: Failed subscribe is retried, not assumed
- **GIVEN** the upstream answers `req_id` N with a failure
- **WHEN** the next poll cycle runs
- **THEN** the item SHALL still be absent from the applied set
- **AND** the provider SHALL send the subscribe frame again

#### Scenario: Unanswered subscribe is retried
- **GIVEN** no acknowledgement for `req_id` N arrives before the next cycle
- **WHEN** the next poll cycle runs
- **THEN** the provider SHALL send the subscribe frame again

#### Scenario: Acknowledgement frames do not count as data liveness
- **WHEN** the only inbound traffic is subscription acknowledgements
- **THEN** `last_data_frame_at` SHALL be governed by the existing `ignore_pattern` classification
- **AND** acknowledgements SHALL NOT by themselves suppress the data-liveness watchdog when configured to be ignored

### Requirement: The applied set SHALL be durable across restarts

The provider SHALL persist its applied set to the plugin cspec after each successful
commit, and SHALL restore it when the plugin is restarted by its manager or
re-applied after a controller restart. A restored plugin SHALL NOT re-send subscribe
frames for items already in its restored applied set beyond the frame issued on
connect.

A failed poll SHALL NOT be interpreted as an empty desired set, and SHALL NOT cause
any unsubscribe.

#### Scenario: Applied set survives a plugin restart
- **GIVEN** a plugin whose applied set is `["a", "b"]`
- **WHEN** `Extension.Table.handle_down/5` restarts the plugin
- **THEN** the restored applied set SHALL contain `a` and `b`

#### Scenario: Applied set survives a controller restart
- **GIVEN** a plugin whose applied set is `["a", "b"]`
- **WHEN** the controller restarts and re-applies plugins from the cspec
- **THEN** the plugin SHALL resume with `a` and `b` applied

#### Scenario: Source outage does not unsubscribe anything
- **GIVEN** a connected plugin with a non-empty applied set
- **WHEN** three consecutive polls fail with a network error
- **THEN** the provider SHALL send no unsubscribe frame
- **AND** the applied set SHALL be unchanged

### Requirement: Connect SHALL restore the current applied set, not the boot-time payload

When a subscription source is configured, `handle_connect/3` SHALL issue the rendered
`subscribe_template` over the **current applied set** through Fresh's reply form,
rather than the `post_connect` payload. When no subscription source is configured the
provider SHALL issue `post_connect` exactly as before.

#### Scenario: Reconnect restores the current set
- **GIVEN** a plugin with a subscription source that booted with `["a"]` and has since applied `["a", "b"]`
- **WHEN** the connection is re-established
- **THEN** the frame issued on connect SHALL name both `a` and `b`

#### Scenario: No subscription source keeps post_connect behaviour
- **GIVEN** a plugin with no `subscription_url`
- **WHEN** the connection is established
- **THEN** the provider SHALL issue the `post_connect` payload unchanged

### Requirement: Reconcile activity SHALL be observable

The provider SHALL report its applied set, the outcome of its last reconcile, and its
last error, and SHALL emit a telemetry event per reconcile that sent at least one
frame, carrying the counts of added and removed items with `%{plugin_id: binary()}`
metadata.

#### Scenario: Applied set is reportable
- **WHEN** a user inspects a plugin configured with a subscription source
- **THEN** the applied set and the last reconcile outcome SHALL be reported

#### Scenario: Reconcile emits telemetry
- **WHEN** a reconcile cycle sends a subscribe frame for two items
- **THEN** a telemetry event SHALL be emitted carrying the added count
- **AND** its metadata SHALL carry the `plugin_id`

#### Scenario: Last error is retained after a failed poll
- **GIVEN** a poll that fails with a network error
- **WHEN** a user inspects the plugin
- **THEN** the last error SHALL name the failure

### Requirement: The desired item set SHALL be filtered to the partition's key groups
When a websocket plugin's opts carry `partition_index`/`partition_count` (with a key-group range), the desired set derived from a subscription poll SHALL be filtered — between decode and reconcile — to items whose `fnv1a128(item) mod 256` falls in the partition's owned range. Everything downstream (applied set, delta frames, durable `applied_subscriptions`, connect-time re-assert) derives from the filtered desired set. Without partition opts, behavior is unchanged.

#### Scenario: Each partition subscribes only to its share
- **WHEN** the items endpoint publishes 9 items and a member of a 3-partition group polls it
- **THEN** the member's desired set contains only the items hashing into its range, its subscribe deltas name only those items, and its durable applied set never contains another member's item

#### Scenario: Item churn stays partition-correct
- **WHEN** a new item appears in the polled list
- **THEN** exactly the member owning that item's key group subscribes to it, and no other member does

#### Scenario: Unpartitioned plugins are unchanged
- **WHEN** a plugin has no partition opts
- **THEN** the desired set is the full decoded item list, exactly as before

### Requirement: A static `subscription_items` opt SHALL provide the item list without a poll
The provider SHALL accept `subscription_items` — a literal item list feeding the same desired/reconcile machinery as a poll result — mutually exclusive with `subscription_url` (declaring both SHALL be refused at build/init). The static list SHALL pass through the same partition filter. `post_connect` remains an opaque boot frame and is never filtered.

#### Scenario: Static list drives reconcile
- **WHEN** a plugin declares `subscription_items` and connects
- **THEN** subscribe deltas for (the partition-filtered subset of) those items are sent and ack-gated exactly as for a polled list

#### Scenario: Both sources declared is refused
- **WHEN** opts declare both `subscription_url` and `subscription_items`
- **THEN** the plugin is refused at init with an error naming the conflict

### Requirement: A websocket plugin SHALL name its entry channel with an option

`Plugin.Provider.Websocket` SHALL accept the entry channel it injects on as a declared option, resolved by the rule in `plugin-providers`: `channel` names it on every plugin of this provider, together with the record filter when the plugin also has a chain hook, and `channel_in` names it alone in every case.

Before this, the provider had no option naming its entry channel and a user had to write the `"channel"` key into `payload_metadata`, the general label bag, which is neither discoverable in the provider's help screen nor distinguishable from the provider's own `channel` option pointing the other way.

`payload_metadata` SHALL remain the general label bag and a `"channel"` key inside it SHALL continue to select the entry channel when no channel option is supplied, so a configuration that names nothing else keeps working unchanged.

#### Scenario: A receive-only plugin names its entry channel

- **GIVEN** a websocket plugin with no chain hook
- **WHEN** it is enabled with `channel: "us"` and a frame arrives on its socket
- **THEN** the frame SHALL be injected on the entry channel named `us`

#### Scenario: The explicit spelling names the same channel

- **WHEN** the same plugin is enabled with `channel_in: "us"` instead
- **THEN** an arriving frame SHALL be injected on the entry channel named `us`

#### Scenario: The metadata bag still works on its own

- **WHEN** a websocket plugin is enabled with `payload_metadata` carrying `"channel" => "us"` and no channel option
- **THEN** an arriving frame SHALL be injected on the entry channel named `us`

#### Scenario: A plugin doing both jobs names each end

- **GIVEN** a websocket plugin with an `on_output` hook
- **WHEN** it is enabled with `channel_in: "requests"` and `channel_out: "responses"`
- **THEN** an arriving frame SHALL be injected on `requests`
- **AND** only outputs whose metadata names `responses` SHALL be sent on the socket

### Requirement: `subscription_items` SHALL be a live option

The websocket provider SHALL declare `subscription_items` live. Its `reconfigure/2` SHALL give
the running plugin the new list as its desired item set and run a reconcile at once, which
sends the subscribe and unsubscribe frames on the established connection exactly as a polled
change does. No other websocket option SHALL be live.

#### Scenario: Adding and removing pairs on a shared connection

- **GIVEN** a websocket plugin bound to two relays, connected, with `subscription_items`
  `["a", "b"]` applied
- **WHEN** `flz plugin update` sets `subscription_items` to `["a", "c"]`
- **THEN** a subscribe frame naming `c` and an unsubscribe frame naming `b` SHALL be sent on
  the same connection, the connection process SHALL be the same before and after, and both
  relays SHALL keep ingesting

### Requirement: A websocket plugin SHALL send relay outputs on the connection it holds

`Plugin.Provider.Websocket` SHALL implement `on_input/2` and `on_output/2` and SHALL
send the dispatched payload upstream as a frame on the socket it already holds, without
opening a second connection. A plugin with a hook in neither direction SHALL behave
exactly as it does today and SHALL start no queue, no timer and no additional process.

The provider SHALL accept `sink_frame_type` (`"text"` or `"binary"`, default `"text"`),
an optional `sink_template` whose `{{payload}}` placeholder is replaced by the dispatched
payload and which SHALL be refused at enable time without that placeholder, and an optional
`channel_out` restricting the plugin to outputs whose metadata names that channel, with the
same meaning the Kafka producer's own restriction has. With no `sink_template` the payload
SHALL be sent verbatim as the frame body.

Because the websocket provider also feeds the relay, `channel` on it names both
directions by the rule in `plugin-providers`: the outputs it sends and the entry channel
its inbound frames are injected on. A plugin whose two names differ SHALL write `channel_out`
for the restriction and `channel_in` for the entry channel, and `channel_out` SHALL be
accepted as the explicit spelling of the restriction in every case.

Outbound frames SHALL be handed to the connection only while it is established. The
provider SHALL NOT use the connection library's public send function, whose disconnected
path discards a frame without an error.

#### Scenario: An output is sent on the established socket
- **WHEN** a relay with a websocket plugin hooked on `on_output` produces an output
- **THEN** the payload SHALL be sent as one frame of the configured type on the connection the plugin already holds
- **AND** no second connection SHALL be opened

#### Scenario: Both directions share one connection
- **WHEN** a plugin has an `on_output` hook and the upstream sends an inbound frame
- **THEN** the inbound frame SHALL be injected into the relay as it is for a receive-only plugin
- **AND** both directions SHALL use the same socket

#### Scenario: A channel-restricted output plugin ignores other channels
- **GIVEN** a plugin configured with `channel: "prices"`
- **WHEN** an output whose metadata names a different channel is dispatched
- **THEN** no frame SHALL be sent, and the output SHALL pass through the chain unchanged

#### Scenario: The explicit spelling restricts the same way
- **GIVEN** a plugin configured with `channel_out: "prices"`
- **WHEN** an output whose metadata names a different channel is dispatched
- **THEN** no frame SHALL be sent, and the output SHALL pass through the chain unchanged

#### Scenario: One `channel` names both directions
- **GIVEN** a plugin with an `on_output` hook and configured with `channel: "prices"`, on a relay declaring an entry channel `prices`
- **WHEN** an inbound frame arrives and an output naming `alerts` is dispatched
- **THEN** the frame SHALL be injected on `prices`, and no frame SHALL be sent for the `alerts` output

#### Scenario: A plugin with no hook costs nothing
- **WHEN** a plugin is enabled with no `on_input` or `on_output` hook
- **THEN** no outbound queue SHALL be allocated and no outbound timer SHALL be armed

### Requirement: An outbound frame SHALL be queued while the socket is down, never dropped

The provider SHALL hold outbound frames in a bounded queue whenever the connection is not
established, and SHALL flush that queue on connect. No outbound frame SHALL be discarded
without either being sent, being parked in the dead-letter store, or refusing the caller.

The queue SHALL be bounded by `sink_queue_max` (default `10000`). When the queue is at its
bound the enqueueing caller SHALL be parked until the queue drains, applying back-pressure
rather than dropping, which is the policy the other bounded output plugins share. A frame whose
retry budget is permanently exhausted SHALL be parked through the dead-letter store with the
catalogue error `websocket.unacknowledged` (its `attempts` detail the attempts made), attributed
to the output's own cell and channel, and SHALL NOT be silently lost.

On connect, subscription frames SHALL be sent before queued outbound frames, so a server
that requires a subscription handshake receives it first.

#### Scenario: Frames sent while disconnected are held and flushed
- **GIVEN** a plugin whose socket has dropped and is reconnecting
- **WHEN** outputs are dispatched to it
- **THEN** each frame SHALL be held in the queue rather than handed to the connection
- **AND** on reconnect they SHALL be sent in dispatch order

#### Scenario: Subscription frames precede queued output on connect
- **GIVEN** a plugin with a subscription source and queued outbound frames
- **WHEN** the socket connects
- **THEN** the subscription frames SHALL be sent before any queued outbound frame

#### Scenario: A full queue parks the caller
- **WHEN** the outbound queue reaches `sink_queue_max`
- **THEN** the enqueueing caller SHALL be parked until the queue drains below the bound
- **AND** no frame SHALL be discarded to make room

#### Scenario: An exhausted frame is parked, not dropped
- **WHEN** a frame's retry budget is permanently exhausted
- **THEN** it SHALL be written to the dead-letter store carrying its cell and channel and the error `websocket.unacknowledged`
- **AND** the outbox hold SHALL be answered so the cursor advances past exactly the parked range

### Requirement: Delivery SHALL be confirmed by a declared acknowledgement mode

The provider SHALL accept `sink_ack_mode` with the values `"none"` (default) and
`"required"`, and SHALL answer the outbox hold for an output according to it. The two modes
SHALL carry different, separately stated guarantees, and neither SHALL be described as
exactly-once.

Under `"none"` the hold SHALL be answered when the frame is handed to the established
connection. The guarantee SHALL be stated as at-most-once beyond that point: a frame
accepted by the local socket and lost to a reconnect is not recoverable and is not
retried.

Under `"required"` the hold SHALL be answered only when an inbound frame carries the
correlation id the outbound frame was sent with, and an unanswered frame SHALL be re-sent
on the next cycle rather than assumed delivered. The guarantee SHALL be stated as
at-least-once. The correlation rule SHALL be the one the subscription delta commit already
uses, and SHALL NOT be a second mechanism.

`"required"` SHALL be refused at enable time without a `sink_template` carrying the
correlation placeholder. A websocket frame has no header, so a verbatim payload has nowhere
to put an id the upstream can echo, and an acknowledgement a frame cannot ask for is not a
guarantee.

A frame SHALL be able to carry the output's durable identity stamps, through the same
template: `{{cell}}` and `{{seq}}` SHALL be substituted with the output's `flz.cell` and
`flz.seq`. Because a repeat is a normal event under `"required"` and impossible under
`"none"`, the mode that can repeat is exactly the mode a template is mandatory for, so a
repeated frame always carries the identity a consumer needs. The specification SHALL state
that deduplication is the consumer's, because a websocket exposes no transaction to enlist
and no key to overwrite.

#### Scenario: Confirm on handoff under the default mode
- **GIVEN** a plugin with `sink_ack_mode: "none"`
- **WHEN** an output with a delivery-guaranteed hold is sent on an established socket
- **THEN** the hold SHALL be answered at handoff
- **AND** no acknowledgement SHALL be awaited

#### Scenario: Confirm on acknowledgement under the required mode
- **GIVEN** a plugin with `sink_ack_mode: "required"`
- **WHEN** an output is sent and the upstream echoes its correlation id
- **THEN** the hold SHALL be answered on that inbound frame and not before

#### Scenario: An unacknowledged frame is re-sent, not assumed
- **GIVEN** a plugin with `sink_ack_mode: "required"`
- **WHEN** a sent frame receives no acknowledgement within its window
- **THEN** the frame SHALL be re-sent
- **AND** the hold SHALL remain unanswered until an acknowledgement arrives or the retry budget is exhausted

#### Scenario: A mode that can repeat carries the identity a consumer dedups on
- **GIVEN** a plugin with `sink_ack_mode: "required"`
- **WHEN** the enable supplies no `sink_template`, or one without the correlation placeholder
- **THEN** the enable SHALL be refused, naming the placeholder

#### Scenario: Outputs carry their identity stamps
- **WHEN** an output is sent through a template carrying the cell and sequence placeholders
- **THEN** the frame SHALL carry the output's `flz.cell` and `flz.seq` stamps
- **AND** the published documentation SHALL state that the consumer deduplicates on them

### Requirement: A websocket output plugin on an effectively-once relay SHALL warn when its relay brings it up

A websocket output binding brought up on an effectively-once relay SHALL warn and SHALL
NOT be refused. The warning SHALL name the plugin and the acknowledgement mode, and SHALL
state that the relay's internal guarantee is unchanged while its websocket output hop
is not effectively-once.

The provider SHALL read the tier from the `:effectively_once` fact the relay hands it at
activation rather than from any state of its own.

#### Scenario: Bringing up on a tier relay warns and proceeds
- **GIVEN** a relay running with effectively-once delivery
- **WHEN** the relay brings up a websocket plugin with an `on_output` hook
- **THEN** a warning naming the plugin and the acknowledgement mode SHALL be emitted
- **AND** the plugin SHALL reach running

#### Scenario: Bringing up on a plain relay is silent
- **GIVEN** a relay not running with effectively-once delivery
- **WHEN** the relay brings up the same plugin
- **THEN** no such warning SHALL be emitted

### Requirement: The outbound handoff SHALL NOT block the socket process on the relay

The handoff from the output dispatch chain to the socket process SHALL be a correlated
asynchronous exchange, and SHALL NOT be a synchronous call in either direction while the
opposite direction can also block. The socket process SHALL NOT wait on the relay's
Overwatch while the Overwatch is waiting on it.

This is a deadlock refusal, not a performance preference: on the effectively-once tier the
socket process already calls the Overwatch without a deadline to confirm an injected input,
and the Overwatch is the process that runs the output chain. A synchronous outbound send
closes that cycle and hangs the relay permanently.

#### Scenario: Simultaneous ingest and output do not deadlock
- **GIVEN** a relay running with effectively-once delivery and a websocket plugin bound in both directions
- **WHEN** an inbound frame is being confirmed while an output is dispatched to the same plugin
- **THEN** neither process SHALL block on the other
- **AND** both the injection and the outbound frame SHALL complete

#### Scenario: The caller learns the outcome without holding the socket
- **WHEN** an output is handed to the socket process
- **THEN** the caller SHALL receive a correlated result within a bounded budget
- **AND** the socket process SHALL remain able to read inbound frames throughout

### Requirement: Websocket connection-liveness watchdog reconnects only on a dead socket

`Plugin.Provider.Websocket` SHALL maintain a connection-liveness timestamp `last_frame_at` that is refreshed by **every** inbound frame regardless of kind: data frames delivered to `handle_in/2`, frames matching `ignore_pattern` (e.g. heartbeats), and control frames delivered to `handle_control/2` (ping/pong). The idle watchdog SHALL compare `now - last_frame_at` against `idle_timeout_ms`; when the threshold is met or exceeded the connection SHALL be treated as dead and the provider process SHALL exit with `{:shutdown, :idle_timeout}`, driving the existing `Extension.Table.handle_down/5` reconnect path and emitting `[:flezha, :plugin, :websocket, :idle_reconnect]`.

`idle_timeout_ms` SHALL retain its name, its `30_000` default, and its `0`-disables-the-watchdog semantics. Because any inbound frame refreshes `last_frame_at`, a connection that carries a periodic heartbeat (or any control traffic) SHALL NOT be reconnected merely because no market-data frame has arrived.

The effective idle window SHALL be no shorter than the interval of the keepalive ping that refreshes it. On a socket carrying no inbound data the only refresh is the pong answering the connection library's own ping, so an idle window equal to or shorter than that ping interval reaches its threshold at the moment the next pong is due and tears down a healthy connection on timer jitter. A configured `idle_timeout_ms` at or below the effective ping interval SHALL be raised to a multiple of it, and the applied value SHALL be reported to the user rather than silently substituted. A plugin bound as an output makes a socket with no inbound data an ordinary configuration, so this floor SHALL apply to every websocket plugin and not only to those bound in both directions.

Outbound activity SHALL NOT refresh `last_frame_at`. A write proves only that the local socket accepted bytes, so counting it as connection liveness would let a send-only socket to a dead peer refresh itself for ever and never reconnect.

The socket-level reconnect — the path a refused upgrade or a dropped connection takes without the process exiting — SHALL ride the `:ws_reconnect` row of the backoff table: the connection library's initial and maximum backoff SHALL be set from that row (a user's explicit `ws_opts` still winning), and every reconnect wait SHALL be emitted on `[:flezha, :backoff, :wait]` for ladder `:ws_reconnect`, so an upstream that answers `429` is retried on a declared, observable schedule rather than on one the tree neither configures nor sees.

#### Scenario: Heartbeat-only traffic keeps the connection open past the data-silence window
- **GIVEN** a websocket plugin with `idle_timeout_ms: 30_000` and `ignore_pattern: "heartbeat"`
- **WHEN** the upstream sends only heartbeat frames (matching `ignore_pattern`) for 5 minutes and no data frame
- **THEN** `last_frame_at` SHALL be refreshed by each heartbeat
- **AND** the watchdog SHALL NOT fire and the provider process SHALL NOT exit with `{:shutdown, :idle_timeout}`

#### Scenario: A truly silent socket still reconnects
- **WHEN** no inbound frame of any kind (data, heartbeat, or control) arrives for `idle_timeout_ms`
- **THEN** the watchdog SHALL exit the process with `{:shutdown, :idle_timeout}` and emit `[:flezha, :plugin, :websocket, :idle_reconnect]` with `silence_ms` measured from `last_frame_at`

#### Scenario: Control frames count as liveness
- **WHEN** the only inbound traffic for a period is websocket control frames (ping/pong) handled by `handle_control/2`
- **THEN** each control frame SHALL refresh `last_frame_at`
- **AND** the watchdog SHALL NOT reconnect

#### Scenario: A refused upgrade is retried on the declared row
- **GIVEN** a websocket plugin whose upstream answers every upgrade with `429`
- **WHEN** the socket reconnects for 20 seconds
- **THEN** each reconnect SHALL be preceded by one `[:flezha, :backoff, :wait]` for ladder `:ws_reconnect` carrying the row's delay for that attempt, and the connect timestamps SHALL follow those delays rather than a library default

#### Scenario: An idle window at the ping interval is raised
- **GIVEN** a websocket plugin whose only inbound traffic is the pong answering the keepalive ping
- **WHEN** `idle_timeout_ms` is configured equal to or below the effective ping interval
- **THEN** the effective idle window SHALL be raised above that interval
- **AND** the applied value SHALL be reported to the user
- **AND** the connection SHALL NOT be torn down while the pongs keep arriving

#### Scenario: A send-only socket to a dead peer still reconnects
- **GIVEN** a websocket plugin bound as an output whose peer has stopped answering
- **WHEN** outputs continue to be written to the socket and no inbound frame arrives for the effective idle window
- **THEN** the watchdog SHALL treat the connection as dead and reconnect
- **AND** the outbound writes SHALL NOT have refreshed `last_frame_at`

### Requirement: Websocket data-liveness watchdog re-subscribes in place on a live socket

`Plugin.Provider.Websocket` SHALL maintain a data-liveness timestamp `last_data_frame_at` refreshed only by data frames that do NOT match `ignore_pattern` (the existing classification). The provider SHALL accept an optional `resubscribe_after_ms` opt (default `0`, meaning disabled), normalised from string or integer identically to `idle_timeout_ms`. When `resubscribe_after_ms > 0`, the connection is alive (the connection-liveness watchdog has not fired), and `now - last_data_frame_at >= resubscribe_after_ms`, the provider SHALL resend its subscription on the **existing** connection — without exiting the process, tearing down the socket, or reconnecting — and SHALL emit `[:flezha, :plugin, :websocket, :resubscribe]` with measurements `%{data_silence_ms: integer()}` and metadata `%{plugin_id: binary()}`.

What is resent SHALL be the `post_connect` payload when no subscription source is configured, and the rendered `subscribe_template` over the **current applied set** when one is (see `plugin-websocket`). The watchdog SHALL NOT resend a boot-time payload that a runtime subscription change has since superseded.

The re-subscribe SHALL be rate-limited by a `last_resubscribe_at` guard so that no more than one re-subscribe is issued per `resubscribe_after_ms` window while data remains stale. When `resubscribe_after_ms` is `0`, `last_data_frame_at` SHALL NOT trigger any action and the provider SHALL behave as connection-liveness-only.

#### Scenario: Live connection with stale data re-subscribes without reconnecting
- **GIVEN** a websocket plugin with `idle_timeout_ms: 30_000`, `ignore_pattern: "heartbeat"`, and `resubscribe_after_ms: 120_000`
- **WHEN** heartbeats keep arriving (connection alive) but no non-heartbeat data frame arrives for 120 s
- **THEN** the provider SHALL resend the `post_connect` payload on the existing connection
- **AND** the provider process SHALL NOT exit and the socket SHALL NOT be reconnected
- **AND** a `[:flezha, :plugin, :websocket, :resubscribe]` event SHALL be emitted

#### Scenario: Re-subscribe is rate-limited while data stays stale
- **GIVEN** the conditions above with `resubscribe_after_ms: 120_000`
- **WHEN** data remains stale for 10 minutes while heartbeats continue
- **THEN** the provider SHALL resend the subscribe at most once per 120 s window (not on every idle-check tick)

#### Scenario: Re-subscribe disabled by default
- **GIVEN** a websocket plugin started without `resubscribe_after_ms` (or with `resubscribe_after_ms: 0`)
- **WHEN** data goes stale on an otherwise-alive connection
- **THEN** the provider SHALL NOT resend the subscribe and SHALL NOT emit `[:flezha, :plugin, :websocket, :resubscribe]`
- **AND** only the connection-liveness watchdog SHALL govern reconnect

#### Scenario: Re-subscribe with a subscription source resends the current applied set
- **GIVEN** a websocket plugin with `resubscribe_after_ms: 120_000` and a subscription source, booted with `["a"]` and having since applied `["a", "b"]`
- **WHEN** the data-liveness watchdog fires
- **THEN** the resent frame SHALL name both `a` and `b`
- **AND** it SHALL NOT resend the boot-time `post_connect` payload

### Requirement: Websocket subscribe handshake is issued without a re-entrant send

`Plugin.Provider.Websocket.handle_connect/3` SHALL issue its subscribe frame by returning it through Fresh's reply form (`{:reply, [{type, message}], state}`) so the frame is sent after the connection's websocket is established. The frame SHALL be the `post_connect` payload when no subscription source is configured, and the rendered `subscribe_template` over the current applied set when one is. The provider SHALL NOT issue the subscribe by calling `Fresh.send/2` re-entrantly from within `handle_connect/3`. Any send issued outside a Fresh callback (the data-liveness re-subscribe and the reconcile path) SHALL be guarded so that sending while the underlying websocket is `nil`/unestablished logs at `:warning` and is skipped rather than raising; the subscribe is re-issued on the next successful `handle_connect/3` or the next reconcile cycle. Under no inbound condition SHALL the provider terminate with `{:badkey, :extensions, nil}`.

#### Scenario: Subscribe on connect does not crash on the upgrade race
- **WHEN** `handle_connect/3` runs and a `post_connect` payload is configured
- **THEN** the subscribe frame SHALL be returned via `{:reply, [{type, message}], state}`
- **AND** no `Fresh.send/2` SHALL be invoked from within `handle_connect/3`
- **AND** the provider SHALL NOT raise `{:badkey, :extensions, nil}` regardless of upgrade timing

#### Scenario: Re-subscribe against a momentarily-unestablished socket is non-fatal
- **WHEN** the data-liveness re-subscribe attempts to send while the underlying websocket is transiently `nil`
- **THEN** the send SHALL be skipped with a `:warning` log and the provider process SHALL stay alive

#### Scenario: Reconcile send against a momentarily-unestablished socket is non-fatal
- **WHEN** a reconcile cycle attempts to send a delta frame while the underlying websocket is transiently `nil`
- **THEN** the send SHALL be skipped with a `:warning` log
- **AND** the delta SHALL remain uncommitted and be retried on the next cycle

### Requirement: Websocket liveness tracking is O(1) and allocation-free per frame

The per-frame work added for liveness tracking SHALL be O(1) in the frame size beyond the unavoidable payload handoff, and SHALL NOT allocate per frame or spawn per-frame processes. Each inbound frame SHALL be classified as data-versus-ignored at most once, with both `last_frame_at` and (for data frames) `last_data_frame_at` derived from that single classification and a single monotonic-clock read. The `ignore_pattern` SHALL be compiled once at provider start (not per frame), and matched with a binary-search primitive. No code path SHALL scan a frame payload more than once per frame.

#### Scenario: Each frame is classified and timestamped once
- **WHEN** a data frame is processed by `handle_in/2`
- **THEN** the payload SHALL be tested against `ignore_pattern` at most once
- **AND** the monotonic clock SHALL be read at most once for that frame
- **AND** no per-frame heap allocation SHALL be required to update the liveness timestamps

#### Scenario: ignore_pattern is compiled once
- **WHEN** a websocket plugin is started with a non-empty `ignore_pattern`
- **THEN** the pattern SHALL be compiled to a reusable matcher at start and reused for every frame
- **AND** the pattern SHALL NOT be recompiled on the per-frame path
