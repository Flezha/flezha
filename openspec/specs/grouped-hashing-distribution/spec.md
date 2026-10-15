# grouped-hashing-distribution Specification

## Purpose
TBD - created by archiving change place-a-relay-among-the-controllers-that-run-its-factory. Update Purpose after archive.
## Requirements
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

