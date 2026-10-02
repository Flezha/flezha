# websocket-subscription-source Specification

## Purpose
Change what a live websocket is subscribed to without reconnecting. A plugin
pointed at a URL publishing the desired item set polls it, diffs it against the
set it has applied, and sends user-supplied subscribe/unsubscribe templates
for the difference on the connection it already holds. Item strings are opaque,
so all exchange vocabulary stays in configuration; a delta commits only on an
acknowledgement bearing its `req_id`, so retry is nothing but re-deriving the
same diff next cycle. The applied set is durable, so connect and both watchdog
tiers re-assert what the socket actually holds rather than its boot-time payload.
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

`Plugin.Provider.Websocket` SHALL accept the entry channel it injects on as a declared option, resolved by the rule in `operator-extensions`: `channel` names it on every plugin of this provider, together with the record filter when the plugin also has a chain hook, and `channel_in` names it alone in every case.

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

- **GIVEN** a websocket plugin bound to two operators, connected, with `subscription_items`
  `["a", "b"]` applied
- **WHEN** `flz plugin update` sets `subscription_items` to `["a", "c"]`
- **THEN** a subscribe frame naming `c` and an unsubscribe frame naming `b` SHALL be sent on
  the same connection, the connection process SHALL be the same before and after, and both
  operators SHALL keep ingesting

