# cluster-split-behaviour Specification

## Purpose
What a Flezha cluster does in a network split: how a user picks the behaviour, which side keeps running relays under each choice, how a planned scale-down and a restart are treated, and what a controller without quorum does and reports.
## Requirements
### Requirement: A user SHALL choose the split behaviour of every hub through one setting

The controller SHALL read `FLEZHA_SPLIT_BEHAVIOUR` (`divergence`, `static`, `majority` or `dynamic`) and build every namespace hub and the schedule hub with the matching ProcessHub partition tolerance strategy. Unset, it SHALL be `divergence`. The parameters SHALL be `FLEZHA_QUORUM_NODES`, `FLEZHA_QUORUM_PERCENT` and `FLEZHA_QUORUM_WINDOW_MS`, each a row of the configuration table.

#### Scenario: Nothing set keeps today's behaviour
- **WHEN** a controller boots with no split variable set
- **THEN** every hub runs `Divergence`, and both sides of a split keep their relays

#### Scenario: The schedule hub follows the setting
- **WHEN** a controller boots with `FLEZHA_SPLIT_BEHAVIOUR=majority`
- **THEN** the namespace hubs and the schedule hub all run `MajorityQuorum`

#### Scenario: An incomplete setting refuses the boot
- **WHEN** a controller boots with `FLEZHA_SPLIT_BEHAVIOUR=static` and no `FLEZHA_QUORUM_NODES`, or with `dynamic` and `FLEZHA_QUORUM_PERCENT` outside 51..100
- **THEN** the boot fails with an error naming the variable, and no hub starts

### Requirement: A node that loses quorum SHALL stop its relays before it takes over anyone else's

When a node's strategy decides it has lost quorum, the hub SHALL lock before it redistributes the children of the nodes it lost. Every relay on the locked node SHALL stop through its normal teardown, so its runner drops it and its writers flush.

#### Scenario: Minority side of a split
- **WHEN** a five-node cluster under `majority` splits into three and two
- **THEN** the two-node side stops its relays, never starts a relay of the three-node side, and its runners drop the relays they held
- **AND** the three-node side keeps its relays and takes over the two-node side's relays

#### Scenario: Heal
- **WHEN** the split heals
- **THEN** the locked nodes unlock, and once the cluster has settled every relay runs on exactly one node

### Requirement: A planned departure SHALL shrink the membership; a crash or split SHALL NOT

Under `majority`, a node that drains or announces its departure SHALL leave the member list on every node; a node that disappears without either SHALL stay on it and count as missing. Under `dynamic`, only a node that disappears SHALL count as a sudden loss. A node that died for good SHALL be removable with one call that reaches every node.

#### Scenario: Scale down one node at a time
- **WHEN** a ten-node cluster under `majority` is drained down to two nodes, one or several at a time
- **THEN** the cluster keeps running at every step

#### Scenario: Fast scale-down under dynamic
- **WHEN** five of ten nodes under `dynamic` drain within the window
- **THEN** the remaining five keep running

#### Scenario: Crash still counts
- **WHEN** a six-node cluster under `majority` loses three nodes without a drain or announcement
- **THEN** the remaining three stop their relays

#### Scenario: Forget a dead node
- **WHEN** an operator forgets a node that died for good
- **THEN** every node removes it from its member list, and the quorum is computed without it

### Requirement: A node SHALL never run as a cluster of one because it forgot the cluster

Under `majority`, the member list SHALL survive a restart wherever the hub's registry survives it. A node with no stored list, and any node under `dynamic` or `static` at start, SHALL wait for `FLEZHA_QUORUM_NODES` nodes before it runs relays when that variable is set.

#### Scenario: Restart alone with the volume
- **WHEN** one node of a five-node `majority` cluster restarts and can reach no peer
- **THEN** it reads its stored list of five and runs no relays

#### Scenario: Fresh node waits for the first-start count
- **WHEN** a node with no stored list starts under `majority` with `FLEZHA_QUORUM_NODES=3` and sees two nodes
- **THEN** it runs no relays until a third node joins

### Requirement: A dynamic quorum SHALL never let two equal halves both run

The `dynamic` percentage SHALL be more than 50, so a split into two equal halves stops both.

#### Scenario: Even split
- **WHEN** a four-node cluster under `dynamic` with the default percentage splits two and two within the window
- **THEN** both sides stop their relays

### Requirement: A start or stop on a node without quorum SHALL answer with a clear error

A relay or schedule start or stop issued on a node whose hub has no quorum SHALL fail with `no_quorum`, and the CLI and API SHALL say that the controller is on the minority side of a network split.

#### Scenario: Starting a relay on the minority side
- **WHEN** `flz relay start` reaches a controller whose namespace hub has no quorum
- **THEN** the command fails with the `no_quorum` message and nothing is started

