# cluster-wide-reads Specification

## Purpose
An operator read answers for the whole cluster, whichever controller received it: the shape of a cluster answer (`unreachable`, a row's `node`), where its controllers come from, and the relay, factory, namespace, status, steps and diagnosis reads that follow it.
## Requirements
### Requirement: A cluster read SHALL name the controllers that did not answer

A read that gathers its answer from several controllers SHALL report, in a field named
`unreachable`, the node of every controller it asked that did not answer within the read's
bound. The field SHALL be an empty list when every controller answered. A list read SHALL
answer `%{rows: rows, unreachable: nodes}`. A read that answers a map SHALL carry
`unreachable` as one more key of that map. Every row that describes something living on one
controller SHALL carry that controller's node in a field named `node`.

A controller that does not answer SHALL cost the read one bound, not one bound per controller,
and SHALL NOT fail the read: the rows of the controllers that answered are still returned.

#### Scenario: Every controller answers

- **GIVEN** a cluster of two controllers that both answer
- **WHEN** a caller reads the relay list
- **THEN** the answer carries `unreachable: []` and every row carries the `node` that hosts it

#### Scenario: A controller does not answer in time

- **GIVEN** a cluster of three controllers where one is wedged
- **WHEN** a caller reads the factory list
- **THEN** the answer arrives within one bound, holds the rows of the two answering
  controllers, and names the wedged controller in `unreachable`

### Requirement: The controllers of a cluster read SHALL come from the cluster directory

A cluster read SHALL ask the controllers that the cluster directory lists, and SHALL NOT take
its peers from `Node.list()`. Every controller SHALL advertise itself in the directory while it
runs, and stop advertising when it stops. A node connected to the cluster that is not a
controller, such as a console, SHALL therefore never be asked, never be listed as a controller
and never be reported unreachable.

#### Scenario: A console is connected

- **GIVEN** two controllers and a console node connected to both as a visible node
- **WHEN** a caller lists the connected controllers
- **THEN** exactly the two controllers are listed and `unreachable` is empty

### Requirement: Relay health SHALL be the hosting controller's verdict

Every surface that answers a relay's health SHALL answer the verdict of the controller that
hosts that relay. This covers the relay list, relay rows, partition-group rows, `Relay.info`,
`Controller.cluster_status` and every health count derived from them. A relay whose hosting
controller does not answer SHALL read `unknown`, and that controller SHALL be named in the
read's `unreachable`. A relay that no controller hosts SHALL keep the health its registry row
gives it today.

#### Scenario: A healthy relay on another controller

- **GIVEN** controllers A and B, and a healthy relay `orders` hosted on B
- **WHEN** a caller connected to A lists the namespace's relays
- **THEN** `orders` reads `healthy` with `node` B, not `unknown`

#### Scenario: The hosting controller does not answer

- **GIVEN** a relay hosted on controller B, which does not answer the health read
- **WHEN** a caller connected to A lists the relays
- **THEN** the relay reads `unknown` and B is named in `unreachable`

### Requirement: A registry read SHALL answer on a controller outside the namespace's hub

Every read of a namespace's relay registry SHALL answer the same rows on every controller. The
registry reads are the relay rows, the relay bindings, the registry dump and the relay ids. A controller that is not a live
member of the namespace's hub SHALL route the read to a member instead of answering an empty
list or a `hub_down` refusal. Only when no controller runs the hub SHALL the read answer as it
does today when the hub is down.

#### Scenario: The answering controller left the hub

- **GIVEN** controller A left namespace `trader`'s hub and controller B is its member with three
  relays
- **WHEN** a caller connected to A lists `trader`'s relays
- **THEN** the three relays are listed

### Requirement: The factory list SHALL hold every factory the cluster's runners advertise

`Fleet.factories/2` SHALL answer one row per factory that any controller's runners advertise in
the namespace. The row SHALL list the `nodes` whose runners advertise it and the union of the
`runners` that can place it. `Fleet.runners_for_factory/3` SHALL answer the runners of every
controller that advertises the factory. A factory advertised only on a controller that does not
answer SHALL be missing, and that controller SHALL be named in `unreachable`.

#### Scenario: A factory served only through another controller

- **GIVEN** factory `fan_trade` whose only runner is connected to controller B
- **WHEN** a caller connected to controller A lists the factories
- **THEN** `fan_trade` is listed with `nodes: [B]` and its runner

#### Scenario: Two controllers serve one factory

- **GIVEN** factory `ma_fan` advertised by one runner on A and one on B
- **WHEN** a caller lists the factories
- **THEN** `ma_fan` is listed once, with both nodes and both runners

### Requirement: The cluster status SHALL not name a leader the cluster does not elect

`Controller.cluster_status/2` SHALL NOT answer a `leader` or `is_leader`, because the cluster
elects none. It SHALL answer the controller that answered as `answered_by`, the hub's nodes with
their relay counts and recovery state, the unplaced relays, the cluster-wide relay health, and
`unreachable`.

#### Scenario: Two controllers host relays

- **GIVEN** controllers A and B, each hosting two healthy relays of the namespace
- **WHEN** a caller connected to A reads the cluster status
- **THEN** the relay health counts four healthy relays, `answered_by` is A, and no leader is
  named

### Requirement: An instance's steps reads SHALL run where its state is

Every steps read of one instance SHALL run on the controller that holds the instance's state:
the controller hosting the instance while it runs, otherwise the controller whose state root
holds its directory. The steps reads are its completion, journal, committed iterations, consumed
offset, undelivered input, inbox, last input offset, Cells, a Cell's outbox and a Cell's state at
an earlier point. A caller connected to any controller SHALL get the same answer. An instance whose
state no answering controller holds SHALL be refused as not found, and the controllers that did
not answer SHALL be named in the refusal.

#### Scenario: A running instance on another controller

- **GIVEN** a steps instance running on controller B
- **WHEN** a caller connected to controller A reads its journal
- **THEN** the journal B holds is answered

#### Scenario: A retained finished instance on another controller

- **GIVEN** a finished steps instance whose state its class keeps, retained on controller B
- **WHEN** a caller connected to controller A reads its completion
- **THEN** the completion record B holds is answered

### Requirement: A relay's diagnosis SHALL be made on the controller that hosts it

`Relay.diagnose/3` SHALL build its report on the controller that hosts the relay, so the
verdict, the runner and plugin records, the dead letters and the input-log findings are that
controller's. A relay that no controller hosts SHALL be diagnosed where its state is, as the
steps reads are.

#### Scenario: A relay with dead letters on another controller

- **GIVEN** a relay hosted on controller B with two dead letters parked on B
- **WHEN** a caller connected to controller A diagnoses it
- **THEN** the report names the two dead letters and B's verdict for the relay

### Requirement: Relay info SHALL list the finished relays of every controller

`Relay.info/2` SHALL list the retained finished relays of every controller in the namespace,
each with its `node`, beside the live relays. A read of one relay SHALL find a retained
finished relay on any controller.

#### Scenario: A relay finished on another controller

- **GIVEN** a steps relay that finished on controller B and is still inside its retention window
- **WHEN** a caller connected to controller A reads the namespace's relay info
- **THEN** the finished relay is listed with `node` B

### Requirement: Dead letters SHALL be read and acted on across the cluster

`Dlq.list/2` and `Dlq.stats/1` SHALL answer the parked entries of every controller in the
cluster answer shape, each row carrying the `node` whose store holds it.
`Dlq.list/2` SHALL keep its order, nearest-to-expiry first, across controllers. `Dlq.get/2`,
`Dlq.drop/2` and `Dlq.replay/2` SHALL find the entry on whichever controller holds it, judge the
caller against the entry's namespace as they do today, and act on that controller. An entry no
answering controller holds SHALL be refused as not found, naming the controllers that did not
answer.

#### Scenario: Dead letters parked on two controllers

- **GIVEN** one entry parked on controller A and two on controller B
- **WHEN** a caller connected to A lists the dead letters
- **THEN** all three are listed, each with its node

#### Scenario: Replaying an entry held by another controller

- **GIVEN** an entry parked on controller B
- **WHEN** a caller connected to A replays it
- **THEN** B re-dispatches it through the ordinary input path and the entry is removed from B

### Requirement: Schedules SHALL be read and acted on across the cluster

`Schedule.list/1` SHALL answer the schedule definitions of every controller in the cluster
answer shape, each row carrying the `node` that holds the definition and `fires_on`, the node
whose job fires it. `Schedule.run/2` and
`Schedule.remove/2` SHALL act on the controller that holds the named definition. `Schedule.add/2`
SHALL refuse a name that any answering controller already holds, and SHALL refuse while a
controller that might hold it does not answer, naming that controller.

#### Scenario: A schedule added through another controller

- **GIVEN** schedule `nightly` added through controller B
- **WHEN** a caller connected to A lists the schedules
- **THEN** `nightly` is listed with node B, the node firing it, its next fire and its last result

#### Scenario: Removing a schedule held by another controller

- **GIVEN** schedule `nightly` held by controller B
- **WHEN** a caller connected to A removes it
- **THEN** B forgets it and it stops firing

#### Scenario: A duplicate name is refused

- **GIVEN** schedule `nightly` held by controller B
- **WHEN** a caller connected to A adds another schedule named `nightly`
- **THEN** the add is refused naming the existing schedule and B, and nothing is stored on A

### Requirement: An observability signal SHALL say where and when it was recorded

Every signal answered by `Observability.query/2` or handed on by `Observability.tail/2` SHALL carry
`node`, the controller that recorded it, and `at_us`, the wall-clock time in microseconds at which
it was recorded, computed on that controller from its own clock. A consumer SHALL be able to order
signals from several controllers by `at_us` without knowing any controller's monotonic clock.

#### Scenario: Two controllers record signals

- **GIVEN** controller A records a signal at 12:00:01 and controller B one at 12:00:02, wall clock
- **WHEN** a caller queries the source
- **THEN** both signals are answered, A's first, each carrying its node and its `at_us`

### Requirement: Observability reads and switches SHALL cover every controller

`Observability.query/2` SHALL read the ring buffer of every controller, filter each by the
caller's namespace before the limit, merge the signals by `at_us` and answer the newest up to the
limit, in the cluster answer shape. It SHALL take its lower bound as `since_us`, a wall-clock time
each controller converts to its own clock. `Observability.list_sources/1` SHALL answer one row per
source with its status on each controller and its recent activity summed across controllers.
`Observability.list_sinks/1` SHALL answer each controller's sinks with their `node`.
`Observability.enable_source/2` and `Observability.disable_source/2` SHALL apply the change on
every controller and answer the controllers that applied it (`applied`) and `unreachable`.
`Observability.tail/2` SHALL hand on the signals of every controller.

#### Scenario: A source disabled across the cluster

- **GIVEN** two controllers with source `flezha_relay` enabled
- **WHEN** a caller connected to A disables it
- **THEN** both controllers stop emitting it and the answer names both as applied

#### Scenario: A tail sees another controller's signals

- **GIVEN** a tail opened through controller A on source `flezha_relay`
- **WHEN** a relay hosted on controller B starts
- **THEN** the tail hands on B's `relay_started` signal carrying node B

### Requirement: Quota usage SHALL be the cluster's total

`Namespace.quota/3` SHALL answer the namespace's usage of a quota kind across the cluster: the
`total`, the usage per controller in `by_node`, and `unreachable`. The relay count SHALL be read
once from the namespace's relay registry and not summed per controller. Connected runners, Cells,
mailbox bytes and disk bytes SHALL be summed across controllers.

#### Scenario: Runners on two controllers

- **GIVEN** namespace `trader` with two runners connected to A and one to B
- **WHEN** a caller reads `trader`'s runner quota usage
- **THEN** the total is 3, with A at 2 and B at 1

### Requirement: Per-controller facts SHALL be answered for every controller

`Controller.status/1`, `Controller.boot_status/1` and `Controller.mem_top/1` SHALL answer one row
per controller, each with its `node`, in the cluster answer shape. `Controller.durability_breaches/1`,
`Controller.durability_status/1` and `Fleet.installed_plugins/1` SHALL answer the rows of every
controller, each with the `node` that holds it, in the cluster answer shape.
`Fleet.providers/1` SHALL answer one row per provider with the `nodes` that can enable it, and the
count of refused packages per controller (`refused`). `Fleet.provider/2` SHALL answer from a
controller that holds the provider.

#### Scenario: Plugin packages differ between controllers

- **GIVEN** the `rabbitmq` package installed on controller A only
- **WHEN** a caller connected to B lists the providers
- **THEN** the `rabbitmq` providers are listed with `nodes: [A]`

#### Scenario: Status of a two-controller cluster

- **WHEN** a caller reads the controller status of a two-controller cluster
- **THEN** two rows are answered, one per controller, each with its node

