# input-delivery-guarantee Specification

## Purpose
TBD - created by archiving change fix-first-input-drop-on-bind. Update Purpose after archive.
## Requirements
### Requirement: An accepted input SHALL be delivered or refused, never silently dropped

An input the Controller reports as accepted SHALL either reach the operative's
procedure or be refused to the sender. It SHALL NOT be possible for the
Controller to answer `:ok`, record the input as accepted, and then deliver it to
nothing.

An unbound data stream SHALL NOT by itself make an input undeliverable. Where
the Controller journals the input, the bind SHALL deliver it; where it does not
journal the input, the Controller SHALL NOT accept the input while the stream is
unbound.

An operative SHALL NOT be brought up on a data stream that has not bound: its
bind SHALL wait for the stream, so nothing issued by the bind — a durable
restore's re-forward included — is sent onto a stream that would drop it. An
operative whose stream does not bind SHALL be parked rather than run.

#### Scenario: A journaled input arriving before the stream binds is delivered

- **GIVEN** an operative of a journaled class whose data stream has not yet bound
- **WHEN** an input is sent and the Controller reports it accepted
- **THEN** the procedure SHALL receive that input once the stream binds

#### Scenario: An un-journaled input is not accepted while the stream is unbound

- **GIVEN** an operative of a class the Controller does not journal, whose data stream has not yet bound
- **WHEN** an input is sent
- **THEN** the Controller SHALL NOT report the input accepted

#### Scenario: The journal cannot claim an input the system dropped

- **GIVEN** an input written to the journal during an unbound-stream window
- **WHEN** the operative's stream binds and the Entity's replay range is computed
- **THEN** that input SHALL fall inside the range and be forwarded

### Requirement: A data-stream bind SHALL re-forward the journal's undelivered tail

A journaled input that an unbound data stream could not carry SHALL be forwarded
by that stream's next bind, in offset order, using the same replay path a
restore uses. This applies to operatives of a journaled class. A bind SHALL NOT
introduce a second re-forward mechanism beside the restore's.

The re-forward SHALL be bounded by the Entity's consumed offset, so an input the
Entity has already consumed is not re-delivered, and SHALL be idempotent across
repeated binds: a stream that binds, drops and rebinds without the Entity making
progress SHALL forward the same tail rather than an accumulating one.

A bind that owes nothing — every input so far having been delivered — SHALL
forward nothing, so an ordinary runner reconnect does not re-deliver.

#### Scenario: The bind forwards what the Entity has not consumed

- **GIVEN** an Entity whose consumed offset is 3 and whose journal holds offsets up to 6
- **WHEN** offsets 4, 5 and 6 could not be delivered and the operative's data stream then binds
- **THEN** offsets 4, 5 and 6 SHALL be forwarded in order, and offsets at or below 3 SHALL NOT

#### Scenario: A flapping stream does not multiply deliveries

- **GIVEN** an Entity with an undelivered journal tail whose stream binds, drops and rebinds three times without the Entity consuming anything
- **THEN** the number of distinct inputs the procedure receives SHALL equal the size of the tail

#### Scenario: The bind uses the restore's replay path

- **WHEN** the bind re-forward is implemented
- **THEN** it SHALL call the same replay-slice function the restore path calls, rather than a second forwarding implementation

### Requirement: The unbound-stream drop counter SHALL mean a genuine fault

`[:flezha, :stream, :drop]` and the `nil_drops` counter SHALL NOT fire during
ordinary operative startup or rebind. A drop SHALL indicate an input that could
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

