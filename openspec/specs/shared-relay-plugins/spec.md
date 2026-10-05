# shared-relay-plugins Specification

## Purpose
A plugin is enabled once by name and relays bind to it. A shareable provider runs one copy per
controller for every unpartitioned relay bound to it there, or one copy in the cluster with
`single`; a partitioned relay's members each run their own copy; a provider that is not
shareable serves one relay.
## Requirements
### Requirement: A plugin SHALL be enabled by name, without a relay

`flz plugin enable <name> <provider> [options]` SHALL validate the options with the
provider's declaration and record the plugin under its name in the namespace for the whole
cluster, with no relay named and nothing started. A name SHALL be unique in its
namespace: enabling a name that is enabled with the same provider SHALL answer
`{:ok, :already_enabled}` and change nothing, and with another provider SHALL be refused
with `plugin.id_taken`. The plugin SHALL survive a controller restart and a whole-cluster
cold start.

#### Scenario: Enabling records the plugin and starts nothing

- **WHEN** `flz plugin enable trades websocket --uri wss://example.test --namespace n` runs
  on a cluster of two controllers with no relay bound to `trades`
- **THEN** `flz plugin list --namespace n` on either controller SHALL show `trades` as
  `idle`, and no websocket worker SHALL run on either controller

#### Scenario: A name is unique in its namespace

- **GIVEN** `trades` enabled in `n` with the websocket provider
- **WHEN** `trades` is enabled in `n` with the HTTP poll provider
- **THEN** the enable SHALL be refused with `plugin.id_taken` and `trades` SHALL be unchanged

#### Scenario: The plugin survives a cold start

- **GIVEN** `trades` enabled in `n`
- **WHEN** every controller of the cluster is stopped and started again
- **THEN** `flz plugin list --namespace n` SHALL show `trades` with its options

### Requirement: A relay SHALL be bound to a plugin by name

`flz plugin bind <name> --relay_id <op> [--hooks ...] [--channel ...]` SHALL record a
binding on the relay, in the plugin's namespace, and bring it up where the relay runs.
`flz plugin unbind <name> --relay_id <op>` SHALL remove it and leave the plugin enabled.
A bind to a name that is not enabled SHALL be refused with `plugin.not_found`; an unbind of a
binding the relay does not have SHALL be refused with `plugin.not_bound`. A binding SHALL
travel with its relay through a move, a restart and a cold start.
`flz plugin enable <name> <provider> --relay_id <op>` SHALL enable and then bind in one
command; a bind that fails there SHALL leave the plugin enabled and say so.

#### Scenario: Two relays bound to one plugin

- **GIVEN** `trades` enabled in `n`
- **WHEN** `flz plugin bind trades --relay_id a --namespace n` and the same for `b` run
- **THEN** `flz plugin list --namespace n` SHALL show `trades` bound to `a` and `b`

#### Scenario: Binding to a plugin that is not enabled

- **WHEN** `flz plugin bind nope --relay_id a --namespace n` runs
- **THEN** it SHALL be refused with `plugin.not_found` and `a`'s entry SHALL be unchanged

#### Scenario: Unbind keeps the plugin

- **GIVEN** `trades` bound to `a` only
- **WHEN** `flz plugin unbind trades --relay_id a --namespace n` runs
- **THEN** `trades` SHALL be listed as enabled, `idle`, with no binding

### Requirement: A shareable plugin SHALL refuse what it cannot serve

A binding to a plugin whose provider declares itself not shareable SHALL be refused with
`plugin.not_shareable`, naming the relay that holds it, when another relay is already
bound. A plugin bound to a partitioned relay, shareable or not and `single` or not, SHALL
run one copy on every member, as a relay's plugin does today, and a member's copy SHALL
NOT be shared with any other relay.

#### Scenario: A file reader is not shared

- **GIVEN** a file source plugin `in` bound to `a`
- **WHEN** `in` is bound to `b`
- **THEN** the bind SHALL be refused with `plugin.not_shareable` naming `a`

#### Scenario: A shared plugin on a partitioned relay

- **GIVEN** `trades` (websocket) enabled, `p` started with four partitions, and `a`
  unpartitioned and bound to `trades` on the same controller
- **WHEN** `trades` is bound to `p`
- **THEN** each of the four members SHALL run its own copy, with its own connection,
  subscribing only the items its partition owns
- **AND** no member SHALL join the controller's shared copy, which keeps serving `a` alone
- **AND** unbinding `p` SHALL stop the four members' copies and leave the shared one running

### Requirement: A shareable plugin SHALL run once on each controller its relays run on

A shareable plugin without `single` SHALL run one copy on each controller where at least one
unpartitioned relay bound to it runs, and SHALL NOT run on any other controller. Every
unpartitioned bound relay on that controller SHALL be served by that one copy: one
upstream connection for a source, one worker for a sink.

#### Scenario: Two relays on one controller share one connection

- **GIVEN** a test websocket server and `trades` bound to `a` and `b`, both on controller 1
- **WHEN** the server sends one frame
- **THEN** the server SHALL have seen exactly one connection, and `a` and `b` SHALL each
  ingest the frame once

#### Scenario: Relays on two controllers

- **GIVEN** a cluster of three controllers and `trades` bound to `a` on controller 1 and `b`
  on controller 3
- **THEN** `trades` SHALL run on controllers 1 and 3 and not on controller 2, and the test
  server SHALL see two connections

### Requirement: A plugin SHALL keep its connection open across a relay's move

When a relay bound to a shareable plugin moves to another controller, the plugin SHALL
start on the new controller when the relay brings its plugins up there, and the copy on
the old controller SHALL NOT stop until the relay has left it and the idle stop has
passed, unless another bound relay keeps it running.

#### Scenario: Move without a reconnect gap

- **GIVEN** `trades` bound only to `a`, on controller 1 of two, and a test websocket server
- **WHEN** controller 1 is drained and `a` moves to controller 2
- **THEN** the server SHALL see the connection from controller 2 open before the one from
  controller 1 closes

### Requirement: A `single` plugin SHALL run once in the cluster

A plugin enabled with `--single` SHALL run exactly one copy in the cluster while any
unpartitioned relay is bound to it, on a controller the cluster chooses, and SHALL deliver what it reads to every
bound relay on any controller. When its controller leaves gracefully, the plugin SHALL
move with a hand-over that opens the new connection before the old one closes and keeps its
bound relays. When its controller fails, it SHALL start on another and its relays SHALL
be served again without being bound again. A binding with an `on_output` hook to a `single`
plugin SHALL be refused with `plugin.single_sink`.

#### Scenario: One connection for relays on three controllers

- **GIVEN** `trades` enabled with `--single`, bound to relays on each of three controllers
- **WHEN** the test server sends one frame
- **THEN** the server SHALL have seen exactly one connection and each relay SHALL ingest
  the frame once

#### Scenario: The single copy moves with its controller

- **GIVEN** `trades` with `--single` running on controller 1, bound to relays on 2 and 3
- **WHEN** controller 1 is drained
- **THEN** `trades` SHALL run on another controller, the server SHALL see the new connection
  open before the old one closes, and the relays on 2 and 3 SHALL ingest the next frame

#### Scenario: A sink hook on a single plugin

- **WHEN** a binding with the `on_output` hook is made to a `single` plugin
- **THEN** it SHALL be refused with `plugin.single_sink`

### Requirement: Every bound relay SHALL get every record, and a slow one SHALL fall behind alone

A shareable source plugin SHALL hand every record to every relay that shares it, straight
from the worker that reads it, with no process between the worker and the relay and without
waiting on any relay: a slow one SHALL fall behind in its own input queue and lose nothing,
and the other relays' delivery SHALL NOT slow down. A relay whose input is durable (an
effectively-once relay, a sleeping one, a durable steps relay) SHALL NOT share the
source: it SHALL run the source on its own, as a relay's own plugin runs, so every record
is confirmed to it, a slow one holds back only its own upstream connection (TCP back-pressure
on a socket), and its HTTP poller advances the content only once it took it and offers it
again until it does. A shared HTTP poller SHALL offer each relay the content it does not
hold yet, so a newly bound relay gets the current content at the next poll. The relays
sharing a source SHALL be told to its worker when they bind and unbind, and read again by the
worker each time it connects, never looked up per record.

#### Scenario: One stalled relay

- **GIVEN** `trades` bound to `a` and `b`, and `a`'s Overwatch suspended
- **WHEN** the server sends 100 frames
- **THEN** `b` SHALL ingest all 100 while `a` is suspended, and when `a` resumes it SHALL
  ingest all 100 in order

#### Scenario: An effectively-once relay runs its own connection

- **GIVEN** `trades` bound to a plain relay `a` and to an effectively-once relay `b` that
  confirms nothing
- **WHEN** the server sends 5 frames
- **THEN** the server SHALL have seen two connections, and `a` SHALL ingest all 5

#### Scenario: Relays bound while the connection is down

- **GIVEN** `trades` bound to `a` and `b` while its upstream refuses connections
- **WHEN** the upstream accepts the connection and sends a frame
- **THEN** `a` and `b` SHALL each ingest it

### Requirement: An unused plugin SHALL stop and stay enabled

A running copy of a plugin whose last bound relay on that controller leaves SHALL stop at
once when the leave was deliberate (an unbind, a relay stop, a relay that ended), and
otherwise after `FLEZHA_PLUGIN_IDLE_STOP_MS` (default 60000) unless a bound relay arrives
first. A stopped plugin SHALL stay enabled and SHALL start again when a bound relay runs.
A plugin with no running bound relay SHALL be listed `idle`.

#### Scenario: A crash does not close the connection

- **GIVEN** `trades` bound only to `a`, and `FLEZHA_PLUGIN_IDLE_STOP_MS` at 5000
- **WHEN** `a`'s Overwatch is killed and the convergence loop starts it again within 5 s
- **THEN** the test server SHALL have seen one connection throughout

#### Scenario: A stop closes it at once

- **GIVEN** `trades` bound only to `a`
- **WHEN** `flz relay stop a` runs
- **THEN** the connection SHALL close without waiting for the idle stop, and `trades` SHALL
  be listed `idle`

#### Scenario: Idle, then used again

- **GIVEN** `trades` listed `idle`
- **WHEN** a relay bound to it starts
- **THEN** `trades` SHALL run on that relay's controller

### Requirement: Only `disable` SHALL turn a plugin off

`flz plugin disable <name>` SHALL remove the plugin, stop every running copy on every
controller, and leave every binding on its relay. A binding whose plugin is not enabled
SHALL make the relay's health report `plugin_unhealthy` with `plugin.not_found`, and SHALL
come up when a plugin of that name is enabled. Nothing else SHALL remove a plugin.

#### Scenario: Disable with bound relays

- **GIVEN** `trades` bound to `a` and `b`
- **WHEN** `flz plugin disable trades --namespace n` runs
- **THEN** no copy of `trades` SHALL run, `a` and `b` SHALL keep running, each SHALL report
  `plugin.not_found` for `trades`, and enabling `trades` again SHALL bring both bindings up

### Requirement: A missing provider SHALL leave the relay running

A relay SHALL bind and run when it brings up a binding whose provider is not in the
controller's provider table; the binding SHALL NOT start, the relay's health
SHALL report `plugin_unhealthy` with `plugin.provider_not_loaded` naming the controller, and
one warning SHALL be logged. The binding SHALL be tried again at the relay's next bring-up.
While the binding is not started, a guaranteed output it would receive SHALL wait in the
outbox and be delivered when the binding starts; a plain output to it SHALL be dropped and
counted.

#### Scenario: Placed on a controller without the package

- **GIVEN** two controllers, only the first with the `example_stamp` package, and plugin `s`
  of that provider bound to `a`
- **WHEN** `a` is placed on the second controller
- **THEN** `a` SHALL run and process its inputs, its health SHALL report
  `plugin.provider_not_loaded` naming the second controller, and the log SHALL hold one
  warning

#### Scenario: A guaranteed output waits for its sink

- **GIVEN** a relay with guaranteed outputs bound to a sink whose provider is missing on
  its controller
- **WHEN** it emits an output, and the relay then moves to a controller that has the
  provider
- **THEN** the sink SHALL receive the output there

### Requirement: A running plugin's options SHALL be changeable

`flz plugin update <name> [options]` SHALL validate the new options with the provider's
declaration and record them. When every changed option is declared live, every running copy
SHALL apply them in place through the provider's `reconfigure/2` without restarting; otherwise
every running copy SHALL restart with the new options, and the command SHALL print a warning
naming the bound relays.

#### Scenario: A live option is applied in place

- **GIVEN** `trades` running with `subscription_items` `["a"]`
- **WHEN** `flz plugin update trades --subscription_items '["a","b"]'` runs
- **THEN** a subscribe frame naming `b` SHALL be sent on the same connection, and the
  command SHALL print no restart warning

#### Scenario: Any other option restarts with a warning

- **GIVEN** `trades` bound to `a` and `b`
- **WHEN** `flz plugin update trades --uri wss://other.test` runs
- **THEN** the old connection SHALL close, a connection to the new address SHALL open, and the
  command SHALL print a warning naming `a` and `b`

### Requirement: `flz plugin list` SHALL show each plugin's bindings and where it runs

`flz plugin list` SHALL show, for each plugin in the namespace: its name, its provider, whether
it is `single`, its bound relays, and the controllers it runs on, or `idle`.

#### Scenario: A plugin running on two controllers

- **GIVEN** `trades` bound to `a` on controller 1 and `b` on controller 3
- **WHEN** `flz plugin list --namespace n` runs
- **THEN** the `trades` row SHALL name the websocket provider, `a` and `b`, and controllers 1
  and 3

### Requirement: A relay entry with old plugin rows SHALL start without them

A relay's declared entry carrying plugin rows of the earlier form SHALL start without
those plugins, and one warning SHALL name the relay and the old plugin ids and say to
enable and bind them again. Nothing SHALL convert them.

#### Scenario: An old entry

- **GIVEN** a declared entry for `a` carrying an old `plugins` row with id `w`
- **WHEN** `a` starts
- **THEN** `a` SHALL run with no plugin and the log SHALL hold a warning naming `a` and `w`

