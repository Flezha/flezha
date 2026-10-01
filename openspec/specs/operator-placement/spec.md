# operator-placement Specification

## Purpose
How the controller selects one runner from the candidate set for an operator:
the rendezvous ranking, the fairness cap and its knob, the fallback when every
candidate is at cap, the scope boundary against node selection, and the
observability of the resulting spread.
## Requirements
### Requirement: One runner is selected from the candidate set by steps-load rendezvous hashing

The controller SHALL select an operator's runner from the candidate set by
rendezvous ranking narrowed by a fairness cap. The candidate set SHALL be
obtained from the single placement filter that `runner-factory-sync` requires,
and this selection SHALL NOT construct a candidate set of its own. Candidates
SHALL be ranked by a rendezvous hash of the operator id against the runner id,
and the selected runner SHALL be the highest-ranked candidate whose count of
bound operators is below the cap. When every candidate is at or above the cap,
the highest-ranked candidate SHALL be selected, so a cap never refuses a
placement the filter allowed.

The count is a fairness input and not a measure of work. Two operators are not
equal load, so an even count SHALL NOT be read as an even machine, and no
throughput guarantee is made here.

#### Scenario: A namespace with fewer operators than runners leaves no runner idle

- **WHEN** eight active runners advertise factory `F` at the established language and eight operators of factory `F` are started
- **THEN** each of the eight runners SHALL be bound exactly one of them

#### Scenario: Spread holds when operators outnumber runners

- **WHEN** five active runners advertise factory `F` and twenty-five operators of factory `F` are started
- **THEN** each runner SHALL be bound exactly five of them

#### Scenario: Every candidate at cap still places

- **WHEN** a placement is resolved and every candidate's bound count is at or above the cap
- **THEN** the highest-ranked candidate SHALL be selected and the placement SHALL succeed

#### Scenario: The filter still decides eligibility

- **WHEN** a runner advertising factory `F` at a language other than the established one holds no operators at all
- **THEN** it SHALL NOT be selected for `F`, whatever its count

### Requirement: The fairness cap is user-configurable and spans the whole range

The fairness cap SHALL be derived from a declared configuration knob expressed
as an integer percentage of the fair share, defaulting to `100`. The cap for a
placement SHALL be `max(1, ceil(percent * (bound + 1) / (100 * candidates)))`,
where `bound` is the number of operators already counted against the candidate
set and `candidates` is its size. A percentage large enough that the cap always
exceeds every candidate's count SHALL reproduce selection by rendezvous hash
alone, so a user can return to unweighted placement without a code change.

#### Scenario: The default is exactly the fair share

- **WHEN** the knob is left unset and operators are started across a fixed runner set
- **THEN** the difference between the busiest and idlest runner's bound count SHALL be at most one

#### Scenario: A high percentage restores plain rendezvous selection

- **WHEN** the knob is set high enough that no candidate ever reaches the cap
- **THEN** every operator SHALL be placed on the candidate its rendezvous rank puts first

### Requirement: Node selection is not load-aware

Selection of a controller node SHALL be made by the namespace hub's
distribution strategy, `Flezha.Cluster.GroupedHashing`, by consistent hashing
of the operator id over the controllers that serve the operator's factory,
with no load input. An operator with no factory SHALL be hashed over every
controller in the hub. Every node SHALL compute the same controller for the
same operator from the same group table, so concurrent decisions on different
nodes agree. Runner counts SHALL NOT be consulted for a node decision, and no
path outside the hub SHALL choose a destination node for a running operator.

#### Scenario: Two controllers agree on the hosting node

- **WHEN** two controllers holding the same group table are asked where the same operator belongs
- **THEN** both SHALL name the same controller, whatever the runner counts on either node

#### Scenario: A factory on one controller only

- **GIVEN** controllers A and B in one namespace hub, where only B has a runner advertising `report`
- **WHEN** twenty `report` operators are started from A
- **THEN** every one of them SHALL be hosted on B and none SHALL ever be started on A

### Requirement: A count never refuses a placement or moves a healthy operator

A stale or inaccurate count SHALL NOT cause a placement to fail. Whenever the
filter returns a non-empty candidate set, selection SHALL return one of its
members. An operator already bound to a runner that is active and in the
candidate set for its factory SHALL NOT be re-placed on account of any count, so
that a load reading cannot move work that is running correctly.

#### Scenario: An inflated count still places

- **WHEN** every candidate's recorded count is higher than the operators actually bound to it
- **THEN** the placement SHALL still return a candidate from the filter's set

#### Scenario: A healthy binding survives a convergence tick

- **WHEN** convergence evaluates an operator whose bound runner is active and advertises its factory at the established language
- **THEN** the operator SHALL remain on that runner and no rebind SHALL be issued

### Requirement: The spread is observable

The controller SHALL expose the number of operators bound to each runner as a
declared measurement, through the same declaration mechanism every other
controller counter uses. The measurement SHALL carry the runner identity, so a
user can see the distribution the cap governs rather than infer it.

#### Scenario: A user reads the distribution

- **WHEN** operators are bound across several runners in a namespace and the controller's measurements are scraped
- **THEN** the exposition SHALL report a per-runner bound-operator count for each of them

### Requirement: A controller declares the factories it can run as served groups

`Flezha.Runner.FleetManager` SHALL declare factory `F` of namespace `N` as a
served group on `N`'s hub while `Factories.placeable_runners/3` is non-empty
for it on this node, deriving the desired set in `Session.sync_directory/1`
beside the `:pg` groups and announcing the whole change of one sync in one
call. A factory advertised here only at a language other than the established
one SHALL NOT be declared. The current set SHALL be announced again after every
start of the namespace's hub on this node.

An operator SHALL be started with its factory id as its group, under the
`:group` key of its registry metadata, and an operator with no factory SHALL
carry no group. No handler SHALL rewrite or drop the key, and a registry row
restored without it SHALL be given it from the operator's factory when the
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
- **THEN** a `report` operator started from either controller SHALL be hosted on B
- **AND** the `echo` operators SHALL keep their controllers

#### Scenario: A runner back within the grace moves nothing

- **GIVEN** `report` operators on B, whose only `report` runner disconnects without a retiring mark
- **WHEN** a runner advertising `report` connects to B before `group_withdraw_grace_ms` elapses
- **THEN** no operator SHALL have changed controller, and each SHALL be rebound to the new runner

#### Scenario: A retired runner withdraws at once

- **GIVEN** `report` operators on B and a second controller A serving `report`
- **WHEN** a user runs `flz runner disconnect` against B's only `report` runner
- **THEN** B SHALL withdraw `report` without waiting for the grace and the operators SHALL move to A with their state

#### Scenario: A wrong-language runner does not make its controller eligible

- **GIVEN** `report` established at one SDK language by a runner on B
- **WHEN** a runner on A advertises `report` at another language
- **THEN** A SHALL NOT serve `report` and no `report` operator SHALL be hosted on A

