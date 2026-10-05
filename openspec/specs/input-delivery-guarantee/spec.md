# input-delivery-guarantee Specification

## Purpose
TBD - created by archiving change fix-first-input-drop-on-bind. Update Purpose after archive.
## Requirements
### Requirement: An accepted input SHALL be delivered or refused, never silently dropped

An input the Controller reports as accepted SHALL either reach the relay's
tasklet or be refused to the sender. It SHALL NOT be possible for the
Controller to answer `:ok`, record the input as accepted, and then deliver it to
nothing.

An unbound data stream SHALL NOT by itself make an input undeliverable. Where
the Controller journals the input, the bind SHALL deliver it; where it does not
journal the input, the Controller SHALL NOT accept the input while the stream is
unbound.

A relay SHALL NOT be brought up on a data stream that has not bound: its
bind SHALL wait for the stream, so nothing issued by the bind — a durable
restore's re-forward included — is sent onto a stream that would drop it. A
relay whose stream does not bind SHALL be parked rather than run.

#### Scenario: A journaled input arriving before the stream binds is delivered

- **GIVEN** a relay of a journaled class whose data stream has not yet bound
- **WHEN** an input is sent and the Controller reports it accepted
- **THEN** the tasklet SHALL receive that input once the stream binds

#### Scenario: An un-journaled input is not accepted while the stream is unbound

- **GIVEN** a relay of a class the Controller does not journal, whose data stream has not yet bound
- **WHEN** an input is sent
- **THEN** the Controller SHALL NOT report the input accepted

#### Scenario: The journal cannot claim an input the system dropped

- **GIVEN** an input written to the journal during an unbound-stream window
- **WHEN** the relay's stream binds and the Cell's replay range is computed
- **THEN** that input SHALL fall inside the range and be forwarded

### Requirement: A data-stream bind SHALL re-forward the journal's undelivered tail

A journaled input that an unbound data stream could not carry SHALL be forwarded
by that stream's next bind, in offset order, using the same replay path a
restore uses. This applies to relays of a journaled class. A bind SHALL NOT
introduce a second re-forward mechanism beside the restore's.

The re-forward SHALL be bounded by the Cell's consumed offset, so an input the
Cell has already consumed is not re-delivered, and SHALL be idempotent across
repeated binds: a flow relay that binds, drops and rebinds without the Cell making
progress SHALL forward the same tail rather than an accumulating one.

A bind that owes nothing — every input so far having been delivered — SHALL
forward nothing, so an ordinary runner reconnect does not re-deliver.

#### Scenario: The bind forwards what the Cell has not consumed

- **GIVEN** a Cell whose consumed offset is 3 and whose journal holds offsets up to 6
- **WHEN** offsets 4, 5 and 6 could not be delivered and the relay's data stream then binds
- **THEN** offsets 4, 5 and 6 SHALL be forwarded in order, and offsets at or below 3 SHALL NOT

#### Scenario: A flapping stream does not multiply deliveries

- **GIVEN** a Cell with an undelivered journal tail whose stream binds, drops and rebinds three times without the Cell consuming anything
- **THEN** the number of distinct inputs the tasklet receives SHALL equal the size of the tail

#### Scenario: The bind uses the restore's replay path

- **WHEN** the bind re-forward is implemented
- **THEN** it SHALL call the same replay-slice function the restore path calls, rather than a second forwarding implementation

### Requirement: The unbound-stream drop counter SHALL mean a genuine fault

`[:flezha, :stream, :drop]` and the `nil_drops` counter SHALL NOT fire during
ordinary relay startup or rebind. A drop SHALL indicate an input that could
be neither delivered nor accounted for, so a non-zero count is actionable.

#### Scenario: A clean run drops nothing

- **WHEN** the end-to-end suite runs to completion with the drop probe attached
- **THEN** no `[:flezha, :stream, :drop]` event SHALL be emitted on a start or rebind path

### Requirement: The test suite SHALL NOT compensate for undelivered input

No test helper SHALL exist whose purpose is to wait for a data stream to bind
before sending, and no test SHALL re-send an input until it is forwarded. The
suite SHALL fail on a delivery regression rather than settling around it.

#### Scenario: The bind settle helper is gone

- **WHEN** the test tree is searched for a helper that polls stream-bind status before sending
- **THEN** there SHALL be none, and the end-to-end suite SHALL be green without one

#### Scenario: No test re-sends until forwarded

- **WHEN** the test tree is searched for a loop that re-sends an input until it is observed forwarded
- **THEN** there SHALL be none

### Requirement: An entry frame naming an undeclared channel SHALL be parked, never dropped or rerouted

A frame whose metadata names an entry channel SHALL be delivered to that channel, or parked when the
relay declares no ENTRY channel of that name. A name the relay declares as a channel of
another kind — a output plugin, an inter-tasklet channel — is not an entry channel and SHALL be
parked the same way, because channel names are one namespace across every kind. The frame SHALL NOT
be discarded, and it SHALL NOT be delivered to a different channel. Every SDK SHALL answer the same
way: the six hosted on `flz_core` through the core's one resolution, and the Elixir SDK, which is
its own core, by re-expressing it.

The park SHALL record the requested name in the entry's channel field and nowhere else. A name off
the wire SHALL NOT reach the identity the dead-letter store keys its row by, because the store makes
that identity's tasklet component a directory; the SDK SHALL supply a fixed component of its own
for a park that resolved no channel.

The park SHALL be immediate rather than laddered. The name is a fact of the frame, so a re-attempt
resolves against the same declared set and fails identically; walking the channel's retry budget
would delay the report without changing it.

A frame carrying no channel in its metadata SHALL continue to resolve to the first declared entry
channel. That is the default a input plugin relies on and it is not an unresolved name.

A relay that declares no entry channel at all SHALL park such a frame with a reason naming that
condition, rather than reporting an unresolved name against an empty set.

#### Scenario: A named channel the relay does not declare is parked

- **GIVEN** a relay declaring one entry channel `prices`
- **WHEN** a frame arrives whose metadata carries `channel = "not_prices"`
- **THEN** the frame SHALL be parked in the dead-letter store with its payload bytes, and SHALL NOT reach any tasklet

#### Scenario: The Elixir SDK stops rerouting an unresolved name

- **GIVEN** an Elixir relay declaring entry channels `prices` and `config`
- **WHEN** a frame arrives whose metadata carries `channel = "quotes"`
- **THEN** the frame SHALL be parked, and neither `prices` nor `config` SHALL receive it

#### Scenario: A frame with no channel still reaches the first entry channel

- **GIVEN** a relay declaring one entry channel `prices`
- **WHEN** a frame arrives whose metadata carries no `channel` key
- **THEN** the frame SHALL be delivered to `prices`

#### Scenario: The default is the first declared entry channel, whatever its name sorts as

- **GIVEN** a relay declaring entry channels `volumes` and then `prices`, in that order
- **WHEN** a frame arrives whose metadata carries no `channel` key
- **THEN** the frame SHALL be delivered to `volumes`, on every SDK

#### Scenario: A name the relay declares as another kind of channel is parked

- **GIVEN** a relay declaring one entry channel `prices` and one output plugin `out`
- **WHEN** a frame arrives whose metadata carries `channel = "out"`
- **THEN** the frame SHALL be parked under the name `out`, and SHALL NOT reach any tasklet

#### Scenario: A requested name never becomes a path component

- **GIVEN** a relay declaring one entry channel `prices`
- **WHEN** a frame arrives whose metadata carries a `channel` naming a parent directory
- **THEN** the parked entry SHALL record that name as its channel, and the identity it is stored under SHALL carry the SDK's own fixed component instead

#### Scenario: The park is immediate

- **GIVEN** a relay whose entry channel declares a retry budget above one
- **WHEN** a frame naming an undeclared channel arrives
- **THEN** the parked entry SHALL record one attempt, and no retry delay SHALL be waited

#### Scenario: A relay with no entry channel at all

- **GIVEN** a relay declaring no entry channel
- **WHEN** any frame arrives for it
- **THEN** the frame SHALL be parked with a reason naming that the relay declares no entry channel

