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
state query timeout, the configured state TTL and the consent settings. A running hub SHALL keep the strategy it started
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

### Requirement: A relay whose move is never finished takes itself back

An Overwatch whose handover state the hub took SHALL wait one state TTL for the hub to end it.
Past that, when another running copy of the relay is registered, it SHALL stop as moved; when
none is, it SHALL bind again to its runner, which dropped the relay's instance when the state
was read, restoring its Cells and forwarding again the input they have not consumed; when the
hub cannot be asked, it SHALL ask again one state TTL later.

#### Scenario: A move the hub never finishes is undone

- **GIVEN** a relay whose handover state the hub took
- **WHEN** the hub goes down before starting a replacement, and the state TTL passes
- **THEN** the relay binds again to its runner and processes input again

#### Scenario: A started replacement keeps the relay

- **GIVEN** a relay whose handover state the hub took
- **WHEN** the hub starts the replacement and goes down before stopping the old copy, and the state TTL passes
- **THEN** the old copy stops and the replacement keeps running

### Requirement: A single plugin opens its new connection before the old one closes in a cold move

The old copy of a `single` plugin SHALL keep running until the new copy reports its upstream
open, then stop, when the plugin's home moves cold while relays are still subscribed to the copy. A copy with no
subscribers left SHALL stop at once.

#### Scenario: The upstream connection overlaps across a cold home move

- **GIVEN** a `single` websocket plugin in a cold namespace whose home runs on controller A, feeding a relay on controller B
- **WHEN** the home moves to controller C
- **THEN** C's copy opens its connection before A's copy closes its own

### Requirement: A relay without an input log stops behind every input it confirmed

A relay whose input is not journaled SHALL stop behind every input its Overwatch confirmed.
When that Overwatch stops while bound to a runner, every input it delivered to the runner before
the stop SHALL be applied by the runner, and every record those inputs produced SHALL be handed
to the relay's Cell writers before they close. The Overwatch SHALL send a stop marker on the relay's data stream behind its last
delivery, then tell the runner to drop the relay with a time limit. The runner SHALL drop it only
after it has read the marker and the instance is quiet: no Cell holds an input queued, in a
batch at a host, held for a retry or owed to another Cell, no input is crossing to the host or
on a retry ladder, and every Cell's records are flushed. The runner SHALL then answer with a
final processed-prefix marker on the state sink, behind those records, and the controller SHALL
close the writers only after it has that answer. The runner SHALL wait for this off its command
thread, so no other relay's command waits behind it. When the time limit passes first, the stop
SHALL finish as it does without the answer. Nothing SHALL be added to an input's own path. A
relay whose input is journaled SHALL stop as before.

#### Scenario: An input on the wire as the stop begins is applied and moved

- **GIVEN** a persistent relay without an input log in a cold namespace, fed by a confirming source
- **WHEN** the source controller confirms an input and the relay's cold move begins before the runner has read that input from its data stream
- **THEN** the runner applies the input before it drops the relay
- **AND** the destination's state includes that input's change

#### Scenario: Inputs queued in a busy Cell are applied before the last flush

- **GIVEN** a relay whose Cell is still applying an earlier input while confirmed inputs wait in its mailbox
- **WHEN** the relay is stopped for a cold move
- **THEN** every waiting input is applied and its record flushed before the relay is dropped
- **AND** those records are in the copy the destination pulls

#### Scenario: An input on a retry is applied before the relay is dropped

- **GIVEN** a relay whose Cell failed an input with a retryable error and holds it for its backoff
- **WHEN** the relay is stopped within the time limit
- **THEN** the retry runs before the relay is dropped, and its record is flushed

#### Scenario: Work passed between a relay's tasklets is finished too

- **GIVEN** a relay whose entry tasklet's output feeds a second tasklet of the same relay
- **WHEN** it is stopped right after an input was confirmed
- **THEN** both tasklets' changes from that input are in the copy the destination pulls

#### Scenario: The last records reach the writers before they close

- **GIVEN** a relay whose runner flushes its Cells' last records as it stops
- **WHEN** the source closes the relay's writers
- **THEN** every one of those records has been handed to the writers first

#### Scenario: A busy relay does not hold up the runner's other relays

- **GIVEN** two relays on one runner, the first with a Cell busy in author code
- **WHEN** the first is stopped and the second is sent a command
- **THEN** the second's command is answered while the first still waits

#### Scenario: A runner that does not answer does not hold the move

- **GIVEN** a relay whose Cell never finishes, or whose runner is wedged or gone
- **WHEN** the relay is stopped
- **THEN** the stop finishes within its existing budget, as it does today

