# factory-description Specification

## Purpose
Describe a factory with no instance running: what its class declares, its graph with the tasklets that compensate, and its runners and controllers, through the factory rows, `Fleet.factory` and `flz relay factory`.
## Requirements
### Requirement: A factory row SHALL carry what its class declares

Each row `Flezha.Service.Fleet.factories(namespace)` answers SHALL carry, beside its id,
description, run and runners:

- `each` and `on_failure`: the steps loop shape and failure mode the factory is established
  at, nil for a flow factory;
- `declarations`: the class declarations of the announcement the row reads, decoded as the
  controller decodes an `AddRelay` answer's metadata: `durable` (`memory`, `local` or
  `remote`), `sleep_after_ms`, `max_duration_ms`, `effectively_once`, `ack` (`per_input` or
  `checkpoint`), `correction_ms` and `keyed`, a key the class did not declare being nil, and
  the flow-only `effectively_once`, `ack`, `correction_ms` and `keyed` being nil for a steps
  factory. `declarations` SHALL be nil when the announcement carries none, which is what a
  runner older than this change sends;
- `declarations_disagree`: true when the factory's runners, on any controller, announce
  different declarations;
- `nodes`: the controllers whose connected runners announce the factory.

The row SHALL read the announcement of the factory's newest advertising runner, as the graph
read does, and the cluster-wide answer SHALL merge rows by factory id as the cluster read core
merges any row, reporting a difference in declarations between controllers as it reports one
in the API.

#### Scenario: A steps factory with no instance

- **GIVEN** a runner that announces steps factory `fan_trade` with `each: once`,
  `on_failure: undo_all` and `durable: memory`, and no `fan_trade` instance runs
- **WHEN** a caller lists the namespace's factories
- **THEN** the `fan_trade` row carries `each: once`, `on_failure: undo_all` and declarations
  with `durable: memory`

#### Scenario: A runner older than the declarations

- **WHEN** the only runner of factory `legacy` announces no declarations
- **THEN** the `legacy` row carries `declarations: nil`, and the factory is placed as before

#### Scenario: Runners on two controllers

- **WHEN** runners announcing factory `ma_fan` are connected to controllers `a` and `b`
- **THEN** the namespace's factory list has one `ma_fan` row whose `nodes` are `a` and `b`

#### Scenario: Two controllers announce different settings

- **WHEN** a runner on controller `a` announces factory `probe` with `durable: memory` and a
  runner on controller `b` announces it with `durable: remote`
- **THEN** the one `probe` row carries `declarations_disagree: true`, and `Fleet.factory` for it
  answers `disagree: true`

### Requirement: One factory SHALL be describable without a running instance

The controller SHALL publish `Flezha.Service.Fleet.factory(namespace, factory_id, caller)`,
bound to the verb `Fleet.factories` is bound to. It SHALL answer the factory's row as
`Fleet.factories` answers it, plus its announced `graph` and `disagree`: true when its
advertising runners do not all announce one class, that is different graphs on the controller
that read the graph, or different APIs or declarations on any controller. The graph SHALL be
read on a controller whose runner announces the factory, wherever the caller is connected. A
factory with no announced graph SHALL be answered with `graph: nil`, not an error. A factory no
connected runner announces SHALL be refused as `relay.factory_unknown`, naming the factory. The
CLI SHALL expose the function as `flz relay factory <factory_id>`, render the graph as it
renders `Relay.graph`, and also print the run, the steps loop shape and failure mode, the
declarations, the runners, and the tasklets that compensate.

#### Scenario: Describing a spawned factory

- **GIVEN** factory `fan_trade` is announced and no instance of it runs
- **WHEN** a caller holding the factories verb reads `Fleet.factory("trader", "fan_trade")`
- **THEN** the answer carries the `fan_trade` row, its graph with every tasklet and channel, and
  `disagree: false`

#### Scenario: Read through a controller without the factory's runner

- **GIVEN** the only `fan_trade` runner is connected to controller `b`
- **WHEN** a caller connected to controller `a` reads `Fleet.factory("trader", "fan_trade")`
- **THEN** the answer is the one controller `b` holds

#### Scenario: An unknown factory

- **WHEN** a caller reads a factory no connected runner announces
- **THEN** the call is refused naming that factory, and no row is answered

#### Scenario: A caller without the verb

- **WHEN** a caller whose role does not grant the factories verb reads a factory
- **THEN** the authorization seam refuses the call

### Requirement: The description SHALL say which tasklets compensate

In the graph `Fleet.factory` answers, each tasklet SHALL carry `compensates`: true exactly when
the tasklet belongs to a steps class and declares a `compensate` hook, false for every other
tasklet, and nil when the announcement predates the field. The answer SHALL name tasklets, never
chain positions.

#### Scenario: A saga's compensating tasklets

- **GIVEN** steps factory `fan_trade` whose tasklets `reserve` and `place` declare `compensate`
  and whose tasklet `notify` does not
- **WHEN** a caller reads the factory
- **THEN** `reserve` and `place` carry `compensates: true` and `notify` carries
  `compensates: false`

#### Scenario: A flow class

- **WHEN** a caller reads a flow factory
- **THEN** every tasklet carries `compensates: false`

