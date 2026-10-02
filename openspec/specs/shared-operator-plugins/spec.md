# shared-operator-plugins Specification

## Purpose
A plugin is enabled once by name and operators bind to it. A shareable provider runs one copy per
controller for every unpartitioned operator bound to it there, or one copy in the cluster with
`single`; a partitioned operator's members each run their own copy; a provider that is not
shareable serves one operator.
## Requirements
### Requirement: A plugin SHALL be enabled by name, without an operator

`flz plugin enable <name> <provider> [options]` SHALL validate the options with the
provider's declaration and record the plugin under its name in the namespace for the whole
cluster, with no operator named and nothing started. A name SHALL be unique in its
namespace: enabling a name that is enabled with the same provider SHALL answer
`{:ok, :already_enabled}` and change nothing, and with another provider SHALL be refused
with `plugin.id_taken`. The plugin SHALL survive a controller restart and a whole-cluster
cold start.

#### Scenario: Enabling records the plugin and starts nothing

- **WHEN** `flz plugin enable trades websocket --uri wss://example.test --namespace n` runs
  on a cluster of two controllers with no operator bound to `trades`
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

### Requirement: An operator SHALL be bound to a plugin by name

`flz plugin bind <name> --operator_id <op> [--hooks ...] [--channel ...]` SHALL record a
binding on the operator, in the plugin's namespace, and bring it up where the operator runs.
`flz plugin unbind <name> --operator_id <op>` SHALL remove it and leave the plugin enabled.
A bind to a name that is not enabled SHALL be refused with `plugin.not_found`; an unbind of a
binding the operator does not have SHALL be refused with `plugin.not_bound`. A binding SHALL
travel with its operator through a move, a restart and a cold start.
`flz plugin enable <name> <provider> --operator_id <op>` SHALL enable and then bind in one
command; a bind that fails there SHALL leave the plugin enabled and say so.

#### Scenario: Two operators bound to one plugin

- **GIVEN** `trades` enabled in `n`
- **WHEN** `flz plugin bind trades --operator_id a --namespace n` and the same for `b` run
- **THEN** `flz plugin list --namespace n` SHALL show `trades` bound to `a` and `b`

#### Scenario: Binding to a plugin that is not enabled

- **WHEN** `flz plugin bind nope --operator_id a --namespace n` runs
- **THEN** it SHALL be refused with `plugin.not_found` and `a`'s entry SHALL be unchanged

#### Scenario: Unbind keeps the plugin

- **GIVEN** `trades` bound to `a` only
- **WHEN** `flz plugin unbind trades --operator_id a --namespace n` runs
- **THEN** `trades` SHALL be listed as enabled, `idle`, with no binding

### Requirement: A shareable plugin SHALL refuse what it cannot serve

A binding to a plugin whose provider declares itself not shareable SHALL be refused with
`plugin.not_shareable`, naming the operator that holds it, when another operator is already
bound. A plugin bound to a partitioned operator, shareable or not and `single` or not, SHALL
run one copy on every member, as an operator's plugin does today, and a member's copy SHALL
NOT be shared with any other operator.

#### Scenario: A file reader is not shared

- **GIVEN** a file source plugin `in` bound to `a`
- **WHEN** `in` is bound to `b`
- **THEN** the bind SHALL be refused with `plugin.not_shareable` naming `a`

#### Scenario: A shared plugin on a partitioned operator

- **GIVEN** `trades` (websocket) enabled, `p` started with four partitions, and `a`
  unpartitioned and bound to `trades` on the same controller
- **WHEN** `trades` is bound to `p`
- **THEN** each of the four members SHALL run its own copy, with its own connection,
  subscribing only the items its partition owns
- **AND** no member SHALL join the controller's shared copy, which keeps serving `a` alone
- **AND** unbinding `p` SHALL stop the four members' copies and leave the shared one running

### Requirement: A shareable plugin SHALL run once on each controller its operators run on

A shareable plugin without `single` SHALL run one copy on each controller where at least one
unpartitioned operator bound to it runs, and SHALL NOT run on any other controller. Every
unpartitioned bound operator on that controller SHALL be served by that one copy: one
upstream connection for a source, one worker for a sink.

#### Scenario: Two operators on one controller share one connection

- **GIVEN** a test websocket server and `trades` bound to `a` and `b`, both on controller 1
- **WHEN** the server sends one frame
- **THEN** the server SHALL have seen exactly one connection, and `a` and `b` SHALL each
  ingest the frame once

#### Scenario: Operators on two controllers

- **GIVEN** a cluster of three controllers and `trades` bound to `a` on controller 1 and `b`
  on controller 3
- **THEN** `trades` SHALL run on controllers 1 and 3 and not on controller 2, and the test
  server SHALL see two connections

### Requirement: A plugin SHALL keep its connection open across an operator's move

When an operator bound to a shareable plugin moves to another controller, the plugin SHALL
start on the new controller when the operator brings its plugins up there, and the copy on
the old controller SHALL NOT stop until the operator has left it and the idle stop has
passed, unless another bound operator keeps it running.

#### Scenario: Move without a reconnect gap

- **GIVEN** `trades` bound only to `a`, on controller 1 of two, and a test websocket server
- **WHEN** controller 1 is drained and `a` moves to controller 2
- **THEN** the server SHALL see the connection from controller 2 open before the one from
  controller 1 closes

### Requirement: A `single` plugin SHALL run once in the cluster

A plugin enabled with `--single` SHALL run exactly one copy in the cluster while any
unpartitioned operator is bound to it, on a controller the cluster chooses, and SHALL deliver what it reads to every
bound operator on any controller. When its controller leaves gracefully, the plugin SHALL
move with a hand-over that opens the new connection before the old one closes and keeps its
bound operators. When its controller fails, it SHALL start on another and its operators SHALL
be served again without being bound again. A binding with an `on_output` hook to a `single`
plugin SHALL be refused with `plugin.single_sink`.

#### Scenario: One connection for operators on three controllers

- **GIVEN** `trades` enabled with `--single`, bound to operators on each of three controllers
- **WHEN** the test server sends one frame
- **THEN** the server SHALL have seen exactly one connection and each operator SHALL ingest
  the frame once

#### Scenario: The single copy moves with its controller

- **GIVEN** `trades` with `--single` running on controller 1, bound to operators on 2 and 3
- **WHEN** controller 1 is drained
- **THEN** `trades` SHALL run on another controller, the server SHALL see the new connection
  open before the old one closes, and the operators on 2 and 3 SHALL ingest the next frame

#### Scenario: A sink hook on a single plugin

- **WHEN** a binding with the `on_output` hook is made to a `single` plugin
- **THEN** it SHALL be refused with `plugin.single_sink`

### Requirement: Every bound operator SHALL get every record, and a slow one SHALL fall behind alone

A shareable source plugin SHALL hand every record to every bound operator. Each binding SHALL
hold the records its operator has not taken in a backlog bounded by
`FLEZHA_PLUGIN_BACKLOG_BYTES` and `FLEZHA_PLUGIN_BACKLOG_FRAMES`; a record that does not fit
SHALL drop that binding's oldest waiting records. The plugin SHALL NOT wait for a slow
operator, and the other operators' delivery SHALL NOT slow down. A binding that dropped
records within the last probe window SHALL make the operator's health report
`plugin_lagging` with the count dropped.

#### Scenario: One stalled operator

- **GIVEN** `trades` bound to `a` and `b`, a backlog of 10 frames, and `a`'s runner stalled
- **WHEN** the server sends 100 frames
- **THEN** `b` SHALL ingest all 100, `a`'s health SHALL report `plugin_lagging`, and when
  `a`'s runner resumes it SHALL ingest the last 10 frames in order, after at most the one
  frame it held when it stalled

### Requirement: An unused plugin SHALL stop and stay enabled

A running copy of a plugin whose last bound operator on that controller leaves SHALL stop at
once when the leave was deliberate (an unbind, an operator stop, an operator that ended), and
otherwise after `FLEZHA_PLUGIN_IDLE_STOP_MS` (default 60000) unless a bound operator arrives
first. A stopped plugin SHALL stay enabled and SHALL start again when a bound operator runs.
A plugin with no running bound operator SHALL be listed `idle`.

#### Scenario: A crash does not close the connection

- **GIVEN** `trades` bound only to `a`, and `FLEZHA_PLUGIN_IDLE_STOP_MS` at 5000
- **WHEN** `a`'s Overwatch is killed and the convergence loop starts it again within 5 s
- **THEN** the test server SHALL have seen one connection throughout

#### Scenario: A stop closes it at once

- **GIVEN** `trades` bound only to `a`
- **WHEN** `flz operator stop a` runs
- **THEN** the connection SHALL close without waiting for the idle stop, and `trades` SHALL
  be listed `idle`

#### Scenario: Idle, then used again

- **GIVEN** `trades` listed `idle`
- **WHEN** an operator bound to it starts
- **THEN** `trades` SHALL run on that operator's controller

### Requirement: Only `disable` SHALL turn a plugin off

`flz plugin disable <name>` SHALL remove the plugin, stop every running copy on every
controller, and leave every binding on its operator. A binding whose plugin is not enabled
SHALL make the operator's health report `plugin_unhealthy` with `plugin.not_found`, and SHALL
come up when a plugin of that name is enabled. Nothing else SHALL remove a plugin.

#### Scenario: Disable with bound operators

- **GIVEN** `trades` bound to `a` and `b`
- **WHEN** `flz plugin disable trades --namespace n` runs
- **THEN** no copy of `trades` SHALL run, `a` and `b` SHALL keep running, each SHALL report
  `plugin.not_found` for `trades`, and enabling `trades` again SHALL bring both bindings up

### Requirement: A missing provider SHALL leave the operator running

An operator SHALL bind and run when it brings up a binding whose provider is not in the
controller's provider table; the binding SHALL NOT start, the operator's health
SHALL report `plugin_unhealthy` with `plugin.provider_not_loaded` naming the controller, and
one warning SHALL be logged. The binding SHALL be tried again at the operator's next bring-up.
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

- **GIVEN** an operator with guaranteed outputs bound to a sink whose provider is missing on
  its controller
- **WHEN** it emits an output, and the operator then moves to a controller that has the
  provider
- **THEN** the sink SHALL receive the output there

### Requirement: A running plugin's options SHALL be changeable

`flz plugin update <name> [options]` SHALL validate the new options with the provider's
declaration and record them. When every changed option is declared live, every running copy
SHALL apply them in place through the provider's `reconfigure/2` without restarting; otherwise
every running copy SHALL restart with the new options, and the command SHALL print a warning
naming the bound operators.

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
it is `single`, its bound operators, and the controllers it runs on, or `idle`.

#### Scenario: A plugin running on two controllers

- **GIVEN** `trades` bound to `a` on controller 1 and `b` on controller 3
- **WHEN** `flz plugin list --namespace n` runs
- **THEN** the `trades` row SHALL name the websocket provider, `a` and `b`, and controllers 1
  and 3

### Requirement: An operator entry with old plugin rows SHALL start without them

An operator's declared entry carrying plugin rows of the earlier form SHALL start without
those plugins, and one warning SHALL name the operator and the old plugin ids and say to
enable and bind them again. Nothing SHALL convert them.

#### Scenario: An old entry

- **GIVEN** a declared entry for `a` carrying an old `plugins` row with id `w`
- **WHEN** `a` starts
- **THEN** `a` SHALL run with no plugin and the log SHALL hold a warning naming `a` and `w`

