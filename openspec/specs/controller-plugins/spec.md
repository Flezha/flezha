# controller-plugins Specification

## Purpose
A controller plugin is bound to no relay and hears what happens in a namespace (a relay started, stopped or failed, an input dead-lettered, a plugin unhealthy) through five hooks, from a copy on each controller that holds the namespace.
## Requirements
### Requirement: A controller plugin SHALL be enabled with its hooks and no relay

A plugin whose provider declares `type: :controller` SHALL be enabled by name with
`flz plugin enable <name> <provider> [--single] [options] --hook <hook>`, one `--hook` per
hook, and its hooks SHALL be stored on the plugin. Each hook SHALL be one of the controller hooks and SHALL be
implemented by the provider; otherwise the enable SHALL be refused with `plugin.unknown_hook`
or `plugin.hook_not_implemented`. `flz plugin bind` of a controller plugin, and an enable of
one naming `--relay_id`, SHALL be refused with `plugin.not_bindable`. `flz plugin update`
SHALL be able to change its hooks.

A controller plugin whose provider runs a worker and implements no controller hook MAY be
enabled with no hook; any other controller plugin SHALL still be refused with
`plugin.hooks_missing` when it names no hook. A provider MAY declare the one namespace it may be enabled in, and an enable in any
other namespace SHALL be refused with a code naming the namespace.

#### Scenario: Enabling a controller plugin

- **WHEN** `flz plugin enable notify hook_webhook --Xurl=https://hooks.test/x --hook on_relay_failed --namespace n` runs
- **THEN** `flz plugin list --namespace n` SHALL show `notify` with the hook
  `on_relay_failed` and no bound relay

#### Scenario: A relay hook on a controller plugin

- **WHEN** a controller plugin is enabled with `--hook on_output`
- **THEN** the enable SHALL be refused with `plugin.unknown_hook`, naming the hooks a
  controller plugin may take

#### Scenario: Binding a controller plugin

- **GIVEN** `notify` enabled as a controller plugin in `n`
- **WHEN** `flz plugin bind notify --relay_id a --namespace n` runs
- **THEN** it SHALL be refused with `plugin.not_bindable`

#### Scenario: A worker-only controller plugin

- **WHEN** a controller plugin whose provider declares a worker and implements no controller
  hook is enabled in its namespace with no `--hook`
- **THEN** the enable SHALL succeed and the worker SHALL run on every controller holding the
  namespace

#### Scenario: A provider bound to one namespace

- **WHEN** a provider that declares `_system` as its namespace is enabled in `n`
- **THEN** the enable SHALL be refused with a code naming `_system`

### Requirement: A controller plugin SHALL run on every controller of its namespace, or once

A controller plugin without `single` SHALL run one copy on every controller that holds a hub
for its namespace and has its provider, from its enable, a hub start for the namespace on a
controller, or a controller boot, until it is disabled or the hub leaves that controller. It
SHALL NOT stop for want of relays. With `single` it SHALL run exactly one copy in the
cluster, moved as a `single` relay plugin is moved.

#### Scenario: Copies on every controller of the namespace

- **GIVEN** three controllers, each holding a hub for `n`, all with the provider
- **WHEN** a controller plugin is enabled in `n` without `single`
- **THEN** `flz plugin list --namespace n` SHALL show it running on all three

#### Scenario: Running with no relay

- **GIVEN** a controller plugin enabled in `n` and no relay in `n`
- **WHEN** `FLEZHA_PLUGIN_IDLE_STOP_MS` passes twice
- **THEN** the plugin SHALL still run on every controller of `n`

#### Scenario: A copy whose worker cannot start

- **GIVEN** a controller plugin whose worker cannot start on a controller, as an HTTP API
  on a port in use cannot
- **WHEN** the cause goes away
- **THEN** its copy there SHALL start its worker on a later convergence tick, with no
  disable and enable
- **AND** until then `flz plugin list` SHALL NOT list that controller under `runs on`, and
  each refused attempt SHALL log the worker's error-level crash report and one warning
  naming the refusal

### Requirement: Five hooks SHALL tell a controller plugin what happened

A controller plugin SHALL be able to take the hooks `on_relay_started`,
`on_relay_stopped`, `on_relay_failed`, `on_dead_letter` and `on_plugin_unhealthy`. Every
event SHALL carry `hook`, `namespace`, `controller`, `at` (UTC, ISO 8601) and `text` (one
line a person can read), and:

- `on_relay_started`, after a deliberate start (by a user, a schedule, a tasklet or the
  admin SDK) has declared the relay and it runs, and not on a restart, a move, a wake or
  a partition rescale: `relay`, `factory`;
- `on_relay_stopped`, after a deliberate stop or a completion, and not for a partition
  rescale: `relay`, `reason` (`stopped` or `completed`);
- `on_relay_failed`, when the relay's health reads parked or failed (its runner is
  lost), once until it reads healthy, degraded or suspended again or is stopped, so neither
  a park retry nor an unknown or unbound read between two failed ones repeats it:
  `relay`, `error` (the error code of the park's reason);
- `on_dead_letter`, when an input is newly dead-lettered: `relay`, `entry` (the DLQ id),
  `channel`, `error`; never the input itself;
- `on_plugin_unhealthy`, when a plugin bound to a relay turns from healthy to
  unhealthy: `plugin`, `relay`, `error`.

A hook event SHALL be delivered to a plugin at most once per occurrence.

#### Scenario: A started relay

- **GIVEN** a controller plugin in `n` taking `on_relay_started`
- **WHEN** `flz relay start echo Echo --namespace n` succeeds
- **THEN** the plugin SHALL receive one event with `hook` `on_relay_started`,
  `relay` `echo`, the factory, the controller it started on and a `text`

#### Scenario: No start event on a restart

- **GIVEN** that plugin and the running relay `echo`
- **WHEN** `echo`'s Overwatch is killed and the convergence loop starts it again
- **THEN** the plugin SHALL receive no further `on_relay_started` event

#### Scenario: A failed relay

- **GIVEN** a controller plugin in `n` taking `on_relay_failed`
- **WHEN** a relay in `n` parks, and its park is retried on later ticks
- **THEN** the plugin SHALL receive one event naming the relay and the park's error code

#### Scenario: A dead-lettered input carries no data

- **GIVEN** a controller plugin in `n` taking `on_dead_letter`
- **WHEN** an input to a relay in `n` is dead-lettered
- **THEN** the plugin SHALL receive one event naming the relay, the DLQ entry, the channel
  and the error code, and the event SHALL NOT contain the input's payload

#### Scenario: A plugin turning unhealthy

- **GIVEN** a controller plugin in `n` taking `on_plugin_unhealthy` and a websocket plugin
  bound to a relay in `n`
- **WHEN** the websocket plugin's health turns unhealthy and stays so for three probe windows
- **THEN** the controller plugin SHALL receive exactly one event naming the websocket plugin

### Requirement: A controller plugin SHALL hear only its own namespace

A controller plugin enabled in a namespace SHALL receive only events whose `namespace` is
that namespace, and enabling it SHALL be authorized in that namespace like any plugin.

#### Scenario: Two namespaces

- **GIVEN** a controller plugin in `n` taking `on_relay_started`
- **WHEN** a relay starts in `m`
- **THEN** the plugin SHALL receive nothing

### Requirement: An event SHALL be delivered where it happened

A controller plugin without `single` SHALL receive an event from its copy on the controller
where the event happened, and from no other copy; with `single` it SHALL receive it at its
one copy. An event that happens on a controller without a copy of the plugin SHALL NOT be
delivered to it.

#### Scenario: One notification, not one per controller

- **GIVEN** a controller plugin taking `on_relay_failed` running on three controllers
- **WHEN** one relay parks
- **THEN** exactly one copy, the one on the relay's controller, SHALL receive the event

#### Scenario: A single plugin hears every controller

- **GIVEN** a controller plugin with `single`, running on controller 1 of three
- **WHEN** a relay parks on controller 3
- **THEN** the copy on controller 1 SHALL receive the event

### Requirement: Hooks SHALL NOT slow Flezha down

An emit of a hook event SHALL NOT wait for any plugin. On a controller where nothing wants
hook events, an emit SHALL send no message and write nothing. Each copy of a controller plugin
SHALL hold its waiting events in a queue bounded by `FLEZHA_PLUGIN_BACKLOG_BYTES` and
`FLEZHA_PLUGIN_BACKLOG_FRAMES`, SHALL drop the oldest on overflow, and the copy's health
SHALL report `plugin_lagging` with the count dropped, which `flz plugin list` shows as the
controllers the plugin lags on. Delivery SHALL be best effort: an event
waiting in a copy whose controller dies SHALL be lost.

#### Scenario: Nothing listens

- **GIVEN** a controller with no controller plugin
- **WHEN** a relay starts and parks there
- **THEN** no message SHALL be sent to any hook output plugin, and the observability registry SHALL hold
  no hook output plugin on that controller

#### Scenario: A stuck plugin

- **GIVEN** a controller plugin whose hook callback blocks, with a queue of 10 frames
- **WHEN** 100 relays park
- **THEN** every park SHALL complete without waiting on the plugin, the plugin SHALL report
  `plugin_lagging`, and its queue SHALL hold at most 10 events

