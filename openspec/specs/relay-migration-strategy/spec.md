# relay-migration-strategy Specification

## Purpose
TBD - created by archiving change move-relays-by-cold-swap. Update Purpose after archive.
## Requirements
### Requirement: A namespace moves its relays by cold swap unless it chooses hot swap

Every namespace record SHALL carry a `migration` setting, `cold` or `hot`. A record without
the setting, and a namespace with no record, SHALL read `cold`. The verb
`namespace_set_migration` (`flz namespace set-migration <name> cold|hot`) SHALL set it on an
existing record, refuse any other value with the verb's argument error, and be authorised as
`namespace_set_quota` is. `flz namespace show` SHALL report it.

When a namespace's hub starts on a controller, it SHALL take the migration strategy the
namespace's setting names: `ProcessHub.Strategy.Migration.ColdSwap` for `cold`,
`ProcessHub.Strategy.Migration.HotSwap` for `hot`, both with handover enabled, the configured
state query timeout and the consent settings. A running hub SHALL keep the strategy it started
with; a changed setting SHALL take effect at that hub's next start. Relay Overwatches and
plugin homes SHALL accept the handover of either strategy, so moves between hubs started under
different settings deliver their state.

#### Scenario: A new namespace moves cold

- **GIVEN** a namespace created with no `migration` setting
- **WHEN** its hub starts
- **THEN** the hub's migration strategy is `ColdSwap` with handover and consent settings

#### Scenario: A namespace that chooses hot moves hot

- **GIVEN** `flz namespace set-migration orders hot` ran before the `orders` hub started
- **WHEN** the hub starts
- **THEN** its migration strategy is `HotSwap` with handover and consent settings
- **AND** `flz namespace show orders` reports `migration: hot`

#### Scenario: A changed setting waits for the hub's next start

- **GIVEN** the `orders` hub running cold
- **WHEN** `flz namespace set-migration orders hot` runs
- **THEN** the running hub still moves cold
- **AND** after the controller restarts, its `orders` hub moves hot

#### Scenario: An unknown value is refused

- **WHEN** `flz namespace set-migration orders warm` runs
- **THEN** it is refused and the record keeps its setting

### Requirement: A cold move runs at most one copy of a relay

In a cold move the source Overwatch SHALL run its whole `:process` teardown (the runner told
to drop the relay after its Cells' last changes shipped, the Cell writers closed flushed, the
lineage released) before the destination's Overwatch starts. At no instant SHALL two
Overwatches of one relay run on any controllers.

#### Scenario: The destination starts after the source stopped

- **GIVEN** a relay in a cold namespace on controller A and a node B joining that the hub assigns it to
- **WHEN** the hub moves it
- **THEN** A's Overwatch has terminated and its Cell writers are closed before B's Overwatch starts
- **AND** no runner holds the relay from both controllers at once

### Requirement: The hub hands a moved relay its source node

A migrating child's start data SHALL name the node it left (`migrated_from`), for both
strategies. On the destination, the hub's child-data alteration SHALL write that node into the
relay's opts as `:state_source_node`, and the relay's arrival SHALL pull its durable files from
that node through the existing foreign-state pull before its bind. The destination SHALL NOT
ask the hub where the relay runs to find its source.

#### Scenario: A cold move brings the durable state over

- **GIVEN** a persistent relay in a cold namespace with Cell state on controller A's disk only
- **WHEN** it moves to controller B
- **THEN** B pulls its Cells, journal and input log from A before it binds
- **AND** it resumes on B with every change A held

#### Scenario: A hot move finds its source the same way

- **GIVEN** a persistent relay in a hot namespace on controller A
- **WHEN** it moves to controller B
- **THEN** B learns A from the start data and pulls from A before it binds

### Requirement: A cold move of a persistent class restores from disk, not from in-band state

The source's `prepare_handover_state/1` SHALL ship no serialized state, and SHALL NOT ask the
runner for one, when the hub moving the relay runs `ColdSwap` and the relay's class is persistent.
The destination SHALL restore the relay from the copy it pulled, as for an empty handover, and
SHALL NOT mark it as arrived by handover. When the class is not persistent, the source SHALL
ship the runner's serialized state as before, and the relay SHALL arrive with the state the
runner had when the state was taken. The source SHALL learn the strategy from its running hub,
not from the namespace record.

#### Scenario: Input taken between the state query and the stop survives

- **GIVEN** a persistent relay in a cold namespace that takes a confirmed input after its state was queried and before it stopped
- **WHEN** it arrives on the destination
- **THEN** its state there includes that input's change

#### Scenario: A class without state on disk carries its state in-band

- **GIVEN** a relay whose class is not persistent, in a cold namespace, holding in-memory state
- **WHEN** it moves to another controller
- **THEN** it arrives with the state its runner serialized when the move took it

### Requirement: Input sent while no copy runs is not buffered

No controller SHALL hold or forward input addressed to a relay between its source's stop and
its destination's start. An input injected with `:confirming` or `:verdict` to a relay with no
running Overwatch SHALL fail without being counted as taken, so a source that confirms each
record does not move its position past it; a `:call` SHALL answer `call.unavailable`; a
`:fire_and_forget` input SHALL be dropped. An effectively-once relay SHALL be injected
confirming, so a cold move loses none of its input. An Overwatch that ends while its input log
is still journaling a confirming input SHALL answer that input's sender, within its stop
budget: taken when the log took it, which the relay's next bind re-forwards, refused
otherwise, so the sender never sends again an input the relay will apply.

#### Scenario: A confirming source loses nothing across a cold move

- **GIVEN** an effectively-once relay in a cold namespace fed by a confirming source at a steady rate
- **WHEN** the relay moves to another controller
- **THEN** every record the source produced is applied exactly once on the destination

#### Scenario: An input journaled as the source stops is answered taken

- **GIVEN** an effectively-once relay whose source Overwatch stops for a cold move while its input log journals a confirming input
- **WHEN** the log takes the input
- **THEN** the sender is answered taken and does not send it again
- **AND** the destination applies it once, from the log

#### Scenario: A call during the gap is refused as unavailable

- **WHEN** a call reaches a relay whose source stopped and whose destination has not started
- **THEN** it answers `call.unavailable`

### Requirement: A single plugin opens its new connection before the old one closes in a cold move

The old copy of a `single` plugin SHALL keep running until the new copy reports its upstream
open, then stop, when the plugin's home moves cold while relays are still subscribed to the copy. A copy with no
subscribers left SHALL stop at once.

#### Scenario: The upstream connection overlaps across a cold home move

- **GIVEN** a `single` websocket plugin in a cold namespace whose home runs on controller A, feeding a relay on controller B
- **WHEN** the home moves to controller C
- **THEN** C's copy opens its connection before A's copy closes its own

