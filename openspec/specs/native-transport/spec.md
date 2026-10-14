# native-transport Specification

## Purpose
TBD - created by archiving change elixir-sdk-native-transport. Update Purpose after archive.
## Requirements
### Requirement: One driver loop serves every server transport

The transport server driver SHALL be one loop implemented once, and a transport SHALL
be one implementation of the loop's transport contract. `flz_nif` SHALL hold exactly
one generic driver loop (`DriverLoop<T: Transport>`) owning the iteration clock, the
liveness heartbeat, the command drain and every `ServerCommand` arm, send-side framing,
the send-gate ledger, the gate observation and the per-iteration stats snapshot. A
transport implementation SHALL supply only wire operations — bind and accept, read,
write and flush, close, shutdown, and its own poll timeout — and SHALL NOT restate any
job the loop owns. A transport SHALL NOT carry its own copy of a stat, a command arm
or a clock.

A capability the loop provides SHALL be provided to every transport at once. A
capability one transport has and another lacks is a defect of the loop, not a backlog
item for the transport that lacks it.

#### Scenario: A transport is one implementation

- **WHEN** the `flz_nif` source is audited for driver loops
- **THEN** exactly one loop exists, and each supported server transport appears as one
  implementation of the transport contract with no loop of its own

#### Scenario: One conformance table covers both transports

- **WHEN** the driver conformance test runs
- **THEN** the same table of cases SHALL run against every transport implementation,
  asserting for each case that the command is handled, the send-gate ledger balances,
  the stats snapshot carries the field the case moves, and the close reason is the
  same word

#### Scenario: A capability added to the loop reaches every transport

- **WHEN** a job is added to the driver loop
- **THEN** it SHALL be reachable by every transport implementation without editing
  any of them, and no implementation SHALL be able to opt out by omission

### Requirement: A stream close is reported with one word for every transport

The driver SHALL report a stream close with one vocabulary shared by every transport.
The close reason carried in `{:flz_closed, stream_id, reason}` SHALL come from a single
declared set of reasons, and the same wire event SHALL produce the same reason word
regardless of which transport observed it. A reason SHALL be declared once and rendered
once; a transport SHALL NOT spell a shared event with a word of its own.

A reason that only one transport can produce, because the wire only offers it there,
SHALL still be declared in the one set rather than written as a literal at the site
that produces it.

The NIF's client read path SHALL report a close for a stream whose peer ended it even
while the rest of the connection lives on: one stream ending is that stream's close, and
its reader SHALL NOT wait for the whole connection to die before it hears it.

#### Scenario: A peer ending the stream reads the same over every transport

- **WHEN** a connected peer ends a stream cleanly over UDS and, separately, over QUIC
- **THEN** both closes SHALL deliver the same reason word to the owning process

#### Scenario: A transport-specific reason is still declared

- **WHEN** a transport reports an event only its wire can produce, such as a stream
  reset or a lost connection
- **THEN** the reason SHALL be a member of the one declared set, and the set SHALL name
  which transports can produce it

#### Scenario: The client agrees with the server drivers

- **WHEN** the NIF's client read path reports a close
- **THEN** it SHALL use the same declared reason set as the server drivers, so a reason
  read from a log does not depend on which side of the connection produced it

#### Scenario: One stream of a live QUIC client connection ends

- **WHEN** the peer ends one stream of a QUIC client connection, by a clean finish or by
  a reset, while the connection itself stays up
- **THEN** the client's reader of that stream SHALL read it as ended, as it reads a UDS
  stream whose peer ended it, and the NIF's client read path SHALL report the close to
  the stream's owner
- **AND** the client SHALL NOT take the end of that one stream as the loss of the
  connection

