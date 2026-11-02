# cluster-placement Specification

## Purpose

Where a relay runs. A controller declares the factories it can run as served groups, and a grouped
relay is placed only among the nodes that serve its group, held where it runs when nobody does.
Within a node, one runner is chosen from the candidates by load-aware rendezvous hashing under a
user-configurable fairness cap; a count never refuses a placement or moves a healthy relay, and the
resulting spread is observable.

## Requirements

### Requirement: One runner is selected from the candidate set by steps-load rendezvous hashing

The controller SHALL select a relay's runner from the candidate set by
rendezvous ranking narrowed by a fairness cap. The candidate set SHALL be
obtained from the single placement filter that `runner-factory-sync` requires,
and this selection SHALL NOT construct a candidate set of its own. Candidates
SHALL be ranked by a rendezvous hash of the relay id against the runner id,
and the selected runner SHALL be the highest-ranked candidate whose count of
bound relays is below the cap. When every candidate is at or above the cap,
the highest-ranked candidate SHALL be selected, so a cap never refuses a
placement the filter allowed.

The count is a fairness input and not a measure of work. Two relays are not
equal load, so an even count SHALL NOT be read as an even machine, and no
throughput guarantee is made here.

#### Scenario: A namespace with fewer relays than runners leaves no runner idle

- **WHEN** eight active runners advertise factory `F` at the established language and eight relays of factory `F` are started
- **THEN** each of the eight runners SHALL be bound exactly one of them

#### Scenario: Spread holds when relays outnumber runners

- **WHEN** five active runners advertise factory `F` and twenty-five relays of factory `F` are started
- **THEN** each runner SHALL be bound exactly five of them

#### Scenario: Every candidate at cap still places

- **WHEN** a placement is resolved and every candidate's bound count is at or above the cap
- **THEN** the highest-ranked candidate SHALL be selected and the placement SHALL succeed

#### Scenario: The filter still decides eligibility

- **WHEN** a runner advertising factory `F` at a language other than the established one holds no relays at all
- **THEN** it SHALL NOT be selected for `F`, whatever its count

### Requirement: The fairness cap is user-configurable and spans the whole range

The fairness cap SHALL be derived from a declared configuration knob expressed
as an integer percentage of the fair share, defaulting to `100`. The cap for a
placement SHALL be `max(1, ceil(percent * (bound + 1) / (100 * candidates)))`,
where `bound` is the number of relays already counted against the candidate
set and `candidates` is its size. A percentage large enough that the cap always
exceeds every candidate's count SHALL reproduce selection by rendezvous hash
alone, so a user can return to unweighted placement without a code change.

#### Scenario: The default is exactly the fair share

- **WHEN** the knob is left unset and relays are started across a fixed runner set
- **THEN** the difference between the busiest and idlest runner's bound count SHALL be at most one

#### Scenario: A high percentage restores plain rendezvous selection

- **WHEN** the knob is set high enough that no candidate ever reaches the cap
- **THEN** every relay SHALL be placed on the candidate its rendezvous rank puts first

### Requirement: Node selection is not load-aware

Selection of a controller node SHALL be made by the namespace hub's
distribution strategy, `Flezha.Cluster.GroupedHashing`, by consistent hashing
of the relay id over the controllers that serve the relay's factory,
with no load input. A relay with no factory SHALL be hashed over every
controller in the hub. Every node SHALL compute the same controller for the
same relay from the same group table, so concurrent decisions on different
nodes agree. Runner counts SHALL NOT be consulted for a node decision, and no
path outside the hub SHALL choose a destination node for a running relay.

#### Scenario: Two controllers agree on the hosting node

- **WHEN** two controllers holding the same group table are asked where the same relay belongs
- **THEN** both SHALL name the same controller, whatever the runner counts on either node

#### Scenario: A factory on one controller only

- **GIVEN** controllers A and B in one namespace hub, where only B has a runner advertising `report`
- **WHEN** twenty `report` relays are started from A
- **THEN** every one of them SHALL be hosted on B and none SHALL ever be started on A

### Requirement: A count never refuses a placement or moves a healthy relay

A stale or inaccurate count SHALL NOT cause a placement to fail. Whenever the
filter returns a non-empty candidate set, selection SHALL return one of its
members. A relay already bound to a runner that is active and in the
candidate set for its factory SHALL NOT be re-placed on account of any count, so
that a load reading cannot move work that is running correctly.

#### Scenario: An inflated count still places

- **WHEN** every candidate's recorded count is higher than the relays actually bound to it
- **THEN** the placement SHALL still return a candidate from the filter's set

#### Scenario: A healthy binding survives a convergence tick

- **WHEN** convergence evaluates a relay whose bound runner is active and advertises its factory at the established language
- **THEN** the relay SHALL remain on that runner and no rebind SHALL be issued

### Requirement: The spread is observable

The controller SHALL expose the number of relays bound to each runner as a
declared measurement, through the same declaration mechanism every other
controller counter uses. The measurement SHALL carry the runner identity, so a
user can see the distribution the cap governs rather than infer it.

#### Scenario: A user reads the distribution

- **WHEN** relays are bound across several runners in a namespace and the controller's measurements are scraped
- **THEN** the exposition SHALL report a per-runner bound-relay count for each of them

### Requirement: A controller declares the factories it can run as served groups

`Flezha.Runner.FleetManager` SHALL declare factory `F` of namespace `N` as a
served group on `N`'s hub while `Factories.placeable_runners/3` is non-empty
for it on this node, deriving the desired set in `Session.sync_directory/1`
beside the `:pg` groups and announcing the whole change of one sync in one
call. A factory advertised here only at a language other than the established
one SHALL NOT be declared. The current set SHALL be announced again after every
start of the namespace's hub on this node.

A relay SHALL be started with its factory id as its group, under the
`:group` key of its registry metadata, and a relay with no factory SHALL
carry no group. No handler SHALL rewrite or drop the key, and a registry row
restored without it SHALL be given it from the relay's factory when the
child is next started.

A factory that stops being placeable here SHALL be withdrawn after
`group_withdraw_grace_ms`, and SHALL NOT be withdrawn if it becomes placeable
again first. When the runner whose loss made it unplaceable carries the
retiring mark, the withdrawal SHALL be announced at once. The pending
withdrawals SHALL live in the fleet manager's state under its existing timer
seam, and the call handling a disconnect SHALL NOT block on any of it.

#### Scenario: A rolling deploy adds a factory

- **GIVEN** controllers A and B, each with one runner advertising `echo`
- **WHEN** B's runner is replaced by one advertising `echo` and `report`
- **THEN** a `report` relay started from either controller SHALL be hosted on B
- **AND** the `echo` relays SHALL keep their controllers

#### Scenario: A runner back within the grace moves nothing

- **GIVEN** `report` relays on B, whose only `report` runner disconnects without a retiring mark
- **WHEN** a runner advertising `report` connects to B before `group_withdraw_grace_ms` elapses
- **THEN** no relay SHALL have changed controller, and each SHALL be rebound to the new runner

#### Scenario: A retired runner withdraws at once

- **GIVEN** `report` relays on B and a second controller A serving `report`
- **WHEN** a user runs `flz runner disconnect` against B's only `report` runner
- **THEN** B SHALL withdraw `report` without waiting for the grace and the relays SHALL move to A with their state

#### Scenario: A wrong-language runner does not make its controller eligible

- **GIVEN** `report` established at one SDK language by a runner on B
- **WHEN** a runner on A advertises `report` at another language
- **THEN** A SHALL NOT serve `report` and no `report` relay SHALL be hosted on A

### Requirement: A grouped child is placed only among the nodes that serve its group

`Flezha.Cluster.GroupedHashing` SHALL implement ProcessHub's distribution strategy protocol
without naming a namespace, a runner or a factory, and SHALL read a child's group from the
child's caller metadata under the strategy's `group_key` (default `:group`). Any term
SHALL be accepted as a group, and the strategy SHALL NOT convert a group to an atom.

For a child with a group, `belongs_to/4` SHALL answer nodes taken by consistent hashing
from the nodes currently serving that group, and SHALL NOT answer a node that does not
serve it. When fewer nodes serve the group than the replication factor asks for, it SHALL
answer every serving node and no other.

For a child whose metadata carries no `group_key`, `belongs_to/4` SHALL answer what
`ConsistentHashing` answers for the same child id over the same hub nodes.

The answer SHALL be the same on the node that originates a start, on the node that
receives a forwarded start, and on any node after the child's row is registered.

#### Scenario: A grouped child starts on the only node that serves its group

- **GIVEN** a hub on nodes `a` and `b` using `GroupedHashing`
- **AND** `b` has called `serve_groups(hub, ["report"])` and `a` has not
- **WHEN** twenty children are started from `a` with `metadata: %{group: "report"}`
- **THEN** every one of them runs on `b`

#### Scenario: A child with no group is placed as ConsistentHashing places it

- **GIVEN** the same two-node hub
- **WHEN** children are started with no `:group` in their metadata
- **THEN** each runs on the node `ConsistentHashing` assigns that child id over `[a, b]`

#### Scenario: A forwarded start is validated with the child's group

- **GIVEN** a start of a `"report"` child forwarded from `a` to `b`
- **AND** the distribution signatures of `a` and `b` differ, so `b` takes the full
  validation path
- **WHEN** `b` validates the request
- **THEN** `b` accepts the child as its own and starts it

### Requirement: A node declares and withdraws the groups it serves

`GroupedHashing.serve_groups(hub_id, groups)` SHALL add `groups` to the set the local node
serves and `GroupedHashing.unserve_groups(hub_id, groups)` SHALL remove them. Both SHALL
be idempotent, and both SHALL answer `{:error, :not_grouped_hashing}` on a hub that uses
another distribution strategy.

One call SHALL make one announcement to the peer hubs, carrying the local node's full
served set, and SHALL trigger at most one redistribution pass on each node, however many
groups it names. A call that changes no group's member set SHALL trigger none.

A node joining the hub SHALL be sent the served set of every node already in it. A node
that leaves the hub, and a node whose hub stops while the node keeps running, SHALL be
removed from every group on every remaining node.

A node whose hub coordinator is restarted inside a running hub SHALL start from an empty
table and never from a saved one, and the controller SHALL declare the factories its
connected runners can run again. Once that declaration is made, or after at most five
seconds, the node SHALL send its row to every peer; each peer SHALL replace the row it held
for that node with it and SHALL send the node its own row.

A node whose hub started afresh, a crashed node included, SHALL send no empty row to a
joining peer until its owner declares a set or `reconcile_interval_ms` passes, so each peer
SHALL keep the row it held for that node and SHALL place that node's children on it rather
than on another node. A start that node can place on no node SHALL be refused at once with
`:no_target_nodes`, and the controller SHALL answer a relay start refused that way with
`relay.factory_unserved`. A row still empty at the bound SHALL then be sent, so each peer
drops the row it kept and places that node's children as without it.

`distribution_signature/2` SHALL differ between two nodes whose group tables differ.

#### Scenario: Serving a group moves its children to the new node with their state

- **GIVEN** children of group `"report"` running on `b`, the only node serving it, under a
  `HotSwap` migration strategy with handover
- **WHEN** `a` calls `serve_groups(hub, ["report"])`
- **THEN** the children the ring over `[a, b]` assigns to `a` migrate to `a`
- **AND** each arrives with the state it handed over
- **AND** the children the ring still assigns to `b` do not restart

#### Scenario: One call naming many groups runs one pass

- **WHEN** a node calls `serve_groups(hub, groups)` with one hundred groups
- **THEN** each node dispatches `pre_redistribution` once

#### Scenario: A stopped hub leaves every group

- **GIVEN** `a` and `b` both serve `"echo"`
- **WHEN** the hub on `b` is stopped while node `b` keeps running
- **THEN** `a`'s table lists `a` alone for `"echo"`
- **AND** every `"echo"` child runs on `a`

#### Scenario: A restarted coordinator rebuilds the table

- **GIVEN** `a` serves `"local"` and `b` serves `"report"`
- **WHEN** the coordinator of `b`'s hub is killed and its hub restarts it
- **THEN** `a`'s table lists no node for `"report"`
- **AND** `b`'s table lists `a` for `"local"`
- **WHEN** `b` serves `"report"` again
- **THEN** a `"report"` child started from `a` runs on `b`

#### Scenario: A survivor keeps a crashed node's children for it

- **GIVEN** `a` and `b` serve `"report"` and declared children of `"report"` run on `b`
- **WHEN** `b`'s hub dies without a leave and starts afresh, serving nothing yet
- **THEN** `a`'s reconcile rounds start none of those children on `a`
- **WHEN** `b` serves `"report"` again
- **THEN** each of those children runs on `b`

#### Scenario: A crashed node that never serves again gives its children up

- **GIVEN** `a` and `b` serve `"report"` and declared children of `"report"` run on `b`
- **WHEN** `b`'s hub dies without a leave, starts afresh and never serves `"report"`
- **THEN** once `reconcile_interval_ms` has passed, those children run on `a`

#### Scenario: A joining node learns the table

- **GIVEN** `b` serves `"report"`
- **WHEN** `c` joins the hub and serves nothing
- **THEN** a `"report"` child started from `c` runs on `b`

### Requirement: A child whose group nobody serves is held where it runs

For a child whose group has no serving node, `belongs_to/4` SHALL answer the nodes the
child is registered on. No migration strategy SHALL move or stop such a child, and no
redundancy strategy SHALL start or stop a replica of it, for as long as the group stays
unserved. When a node serves the group again the child SHALL be placed as any other
grouped child, through the hub's migration strategy.

A start naming a group with no serving node SHALL be refused with
`{:error, {:no_group_members, groups}}` and SHALL start none of the request's children.
The controller SHALL answer such a relay start with `relay.factory_unserved`, of class
`unavailable`, so the caller may retry.
A declared child in that position SHALL remain declared. A node that starts serving a
group SHALL ask its hub for a reconcile round, which waits for neither
`reconcile_interval_ms` nor the sync tick, and that round SHALL start the child.

#### Scenario: The last serving node withdraws and the children keep running

- **GIVEN** children of group `"report"` running on `b`, the only node serving it
- **WHEN** `b` calls `unserve_groups(hub, ["report"])`
- **THEN** every child keeps its pid on `b`

#### Scenario: A start into an unserved group is refused

- **GIVEN** no node serves `"report"`
- **WHEN** a child is started with `metadata: %{group: "report"}`
- **THEN** the start answers `{:error, {:no_group_members, ["report"]}}`
- **AND** the registry holds no row for the child

#### Scenario: A declared child waits for its group

- **GIVEN** a durable child of group `"report"` with no registry row, whose only serving
  node `b` has left the hub
- **WHEN** reconcile rounds run on `a`, which does not serve `"report"`
- **THEN** the child is started nowhere and stays in the declared list
- **WHEN** `a` calls `serve_groups(hub, ["report"])`
- **THEN** a reconcile round runs within a few seconds, without waiting for the reconcile
  interval or the sync tick
- **AND** the child runs on `a` with the metadata it was declared with

### Requirement: A returning node's first reconcile round waits until the node serves a group

A node's first reconcile round after its coordinator starts SHALL start no declared child,
while a peer still holds a row for the node, until this node serves a group, waiting at most
the round's `pre_recovery_replay` budget (`reconcile_interval_ms`) less one poll. Past that
bound it SHALL place the children as it would have without the wait. A node no peer holds a
row for, alone or on a cold start of the whole cluster, SHALL NOT wait. The wait SHALL run
outside the hub's coordinator, so the node's own declaration lands while it waits.

#### Scenario: A restarted node's declared child comes back to it

- **GIVEN** a durable child of group `"report"` with no registry row and running nowhere,
  in a hub of `a` and `b` where `b` still holds a row for `a` and neither serves `"report"`
- **WHEN** the coordinator of `a`'s hub restarts and its first round reaches its starts
- **THEN** the round starts nothing while `a` serves nothing
- **WHEN** `a` calls `serve_groups(hub, ["report"])`
- **THEN** that first round starts the child, and it runs on `a`

### Requirement: A strategy learns from a start request before the request is validated

The ProcessHub that Flezha builds against SHALL dispatch `pre_children_start` in
`StartChildrenRequest.execute/2` before it determines which of the request's children
belong to the local node, with the payload unchanged at `%{request: request, hub: hub}`.
`GroupedHashing` SHALL fill its group stash from that hook, so a receiving node that takes
the full validation path answers `belongs_to/4` with each child's group.

#### Scenario: A handler runs before validation

- **GIVEN** a `pre_children_start` handler that records when it ran
- **AND** a distribution strategy whose `belongs_to/4` records when it was asked
- **WHEN** a node receives a start request that takes the full validation path
- **THEN** the handler's record precedes the strategy's
