# transport-driver Specification

## Purpose

The shared transport layer between controller and runners. One driver loop serves every server
transport and reports a stream close with one word. Every outbound send buffer is bounded and
reached through one send gate that holds the sender rather than discarding frames, and a write never
reports bytes it did not accept. Every decode path enforces a maximum frame length before allocating
from an untrusted length prefix, a frame too large to describe is refused at encode, and a frame
that cannot be read ends the stream on both sides, counted and logged.

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

### Requirement: Controller-to-runner transport send buffer is bounded

The controller transport driver SHALL bound each per-stream outbound send buffer
(UDS `uds_driver.rs` `write_buffer` and QUIC `quic_driver.rs` `write_buffer`) by
a configurable `max_write_buffer_bytes` cap, **on every driver** — not on one.

When the socket cannot accept more bytes (e.g. `WouldBlock` on UDS,
`WriteError::Blocked` on QUIC) and the buffered bytes cross a configurable
high-water mark, the driver SHALL raise a congestion signal on that stream and
SHALL NOT discard the frame. It SHALL lower the signal and wake the sender once
the buffered bytes fall below a configurable low-water mark. The driver SHALL
NOT block its command loop on a slow or non-reading peer.

A sender observing a raised signal SHALL hold back rather than drop, and SHALL
NOT block a scheduler thread while doing so.

#### Scenario: Crossing the high-water mark gates the sender instead of dropping

- **WHEN** a stream's peer is not reading and frames are enqueued until the
  buffered bytes cross the high-water mark
- **THEN** the stream's congestion signal is raised, the sender holds back, and
  no frame is discarded

#### Scenario: Buffer drains and accepts new frames again

- **WHEN** the peer resumes reading and the buffered bytes fall back below the cap
- **THEN** subsequently enqueued frames are buffered and flushed normally

#### Scenario: The bound applies to QUIC as well as UDS

- **WHEN** a QUIC stream's peer completes the handshake and never reads, and the
  connection's flow-control window fills
- **THEN** the buffered bytes stay at or below `max_write_buffer_bytes` and the
  congestion signal is raised, rather than the buffer growing without bound

#### Scenario: A raised signal is observable as held, not wedged

- **WHEN** a sender is holding back on a raised congestion signal
- **THEN** it is reported as held rather than as a stalled or wedged driver

### Requirement: Send-buffer frame loss is counted and reported

The driver SHALL expose a `write_buffer_dropped` counter on `DriverStats` that
increments once per frame lost, and the platform SHALL read it: the counter is
emitted with the driver's other statistics and named by a liveness verdict, so a
non-zero value is reported rather than merely recorded.

Under the bound above no frame is discarded for congestion, so this counter is
an invariant: any increment is a defect to be surfaced, not a routine metric.

**Every server driver reports it, and the reporting is part of the
requirement.** Both the UDS and the QUIC driver produce a `DriverStats`, so for
a controller on either transport the counter is emitted with the rest of the
driver's statistics and the liveness verdict that names it can fire. Each keeps
its own per-stream send buffer and its own send gate, so the counter is a real
per-stream refusal count on both rather than an aggregate.

#### Scenario: Congestion increments nothing

- **WHEN** a peer stops reading and the send buffer fills to its cap
- **THEN** `write_buffer_dropped` stays at zero, because the sender was gated
  rather than the frame discarded

#### Scenario: A counted loss reaches the platform

- **WHEN** `write_buffer_dropped` is non-zero for a stream
- **THEN** the value is emitted with the driver's statistics and a liveness
  verdict names it, rather than being visible only to a benchmark printout

#### Scenario: A QUIC controller is observed on the same counter

- **WHEN** the controller's transport is QUIC and a stream's send buffer
  refuses a frame at its ceiling
- **THEN** the refusal is counted on that stream's own line of the driver's
  statistics, emitted like any other, and named by the same liveness verdict a
  UDS controller would raise

### Requirement: A transport write never reports bytes it did not accept

Every send buffer in the tree SHALL answer its caller truthfully. A `Write`
implementation returning `Ok(n)` asserts that `n` bytes were accepted, so it
SHALL NOT be the answer to a frame the buffer discarded; a frame refused at the
ceiling SHALL be reported as an error the caller can act on.

A send buffer shared by several writers on one stream SHALL append what it
buffers rather than replace it, and SHALL answer every writer whose bytes it
holds. It SHALL NOT write a later frame onto the stream ahead of an earlier
frame's buffered remainder.

#### Scenario: A frame refused at the ceiling is answered as refused

- **WHEN** a QUIC client stream's peer has stopped reading, its send buffer is
  at `max_write_buffer_bytes`, and a further frame is offered
- **THEN** the writer receives an error naming the condition, and the buffer is
  left unchanged — never `Ok(len)` for a frame that was discarded

#### Scenario: A second concurrent writer queues behind the first

- **WHEN** one writer's partial frame is buffered on a blocked stream and a
  second writer writes on the same stream
- **THEN** the second frame is appended behind the first's remainder, both
  writers are still owed a reply, and both are answered once the buffer drains

### Requirement: Bounded frame allocation

Every transport decode path SHALL enforce a configured maximum on the frame,
payload, and message length prefixes, and SHALL reject a frame whose declared length
exceeds the maximum BEFORE allocating a buffer for it. The check SHALL apply on both
the runner reader (`flz_transport` payloader) and the controller NIF frame buffer.
Inner length fields (e.g. `message_length`) SHALL additionally be bounded by their
enclosing payload length.

#### Scenario: Oversized length prefix is rejected before allocation

- **WHEN** a frame declares a payload or message length greater than the configured
  maximum
- **THEN** the decoder returns a framing error without allocating a buffer sized to
  the declared length, and neither the runner reader thread nor the controller VM is
  brought down

#### Scenario: Inner length cannot exceed the outer payload

- **WHEN** a frame's inner `message_length` claims more bytes than the enclosing
  payload contains
- **THEN** the decoder rejects the frame rather than allocating from the inner value

#### Scenario: Well-formed frames within the limit are unaffected

- **WHEN** a frame's lengths are within the configured maximum
- **THEN** it decodes exactly as before, with no behavioural change

### Requirement: Rejection terminates the stream, loudly

The decoder's owner SHALL treat a length-limit violation as stream-fatal, because
the byte stream is left unrecoverable: the connection or stream carrying the
offending frame SHALL be closed through the existing error path, and the event
SHALL be observable (logged with the declared size, and surfaced to the
controller as a distinct close reason on the NIF side). The limit SHALL be
configurable via `FLEZHA_MAX_FRAME_BYTES` with a safe default, defined once and
shared by both decode paths.

#### Scenario: Controller closes the offending stream

- **WHEN** the NIF frame buffer reports an over-limit declared length
- **THEN** the driver closes that stream via its existing close path with a
  distinct close reason, and other streams are unaffected

#### Scenario: Runner reader reconnects or exits via existing error handling

- **WHEN** the runner-side payloader rejects an over-limit frame
- **THEN** the error is classified as stream-fatal by the existing reader error
  handling (control channel reconnects; a data-stream reader exits and logs)

#### Scenario: One configured limit governs both sides

- **WHEN** a user sets `FLEZHA_MAX_FRAME_BYTES`
- **THEN** both the runner payloader and the controller frame buffer enforce
  that same value, and with the variable unset both enforce the same default

### Requirement: A frame is refused at encode rather than written with a truncated length

Every length prefix this system writes SHALL be refused when the value does not fit the field, rather than written as a truncated cast.

The metadata region carries a one-byte length for each key and each value and a
two-byte length for the table, so a key or value over 255 bytes, or a table over
65535 bytes, cannot be represented. Writing the low bits of the true length
produces a frame that parses without error and yields wrong data, because the
reader resumes at the wrong offset and every remaining pair in the frame is
misread. That is strictly worse than a refusal: it is undetectable downstream,
and the decode-side limits this capability already sets cannot catch it, since
the frame is small and well formed by every check they apply.

One encoder serves both directions, so the refusal SHALL cover frames the runner
builds and frames the controller builds alike.

#### Scenario: An oversized metadata value is refused, not truncated

- **WHEN** a frame is built with a metadata value longer than the one-byte length field can express
- **THEN** the encoder SHALL refuse the frame with an error naming the offending key and the length, and SHALL NOT emit bytes

#### Scenario: An oversized metadata key is refused

- **WHEN** a frame is built with a metadata key longer than the one-byte length field can express
- **THEN** the encoder SHALL refuse the frame with an error naming the length, and SHALL NOT emit bytes

#### Scenario: An oversized metadata table is refused

- **WHEN** the serialized metadata table exceeds the two-byte length field
- **THEN** the encoder SHALL refuse the frame with an error naming the size, and SHALL NOT emit bytes

#### Scenario: Both directions are covered by one check

- **WHEN** the controller builds a frame carrying an oversized metadata value
- **THEN** it SHALL be refused by the same check that refuses one the runner builds

#### Scenario: Frames within the limits are byte-identical

- **WHEN** a frame's metadata keys, values and table are all within their length fields
- **THEN** it SHALL encode to exactly the bytes it encoded to before, with no behavioural change

### Requirement: A name that cannot ride a frame is refused where it is declared

A declared channel name SHALL be refused at registration when it is longer than the metadata field that carries it on the wire.

The encode-time refusal makes a malformed frame impossible, but it is a poor
place to learn: it fires at emit time on a value fixed when the topology was
written. The name rides every frame the channel produces, as the `channel`
metadata value under a one-byte length prefix, so registration is where the
author can still be told, with the declaration in front of them.

The refusal SHALL be a name of the reference vocabulary with a fixture row, so
every SDK renders it identically and none carries a rule of its own beside it.

#### Scenario: An over-long channel name is refused at registration

- **WHEN** a class declares a channel whose name exceeds the metadata field's one-byte length
- **THEN** registration SHALL refuse the class naming the channel and its length, and the runner SHALL NOT start

#### Scenario: The declaration-time bound and the encode-time bound agree

- **WHEN** a channel name is at the limit exactly
- **THEN** registration SHALL accept it and every frame the channel produces SHALL encode

### Requirement: A frame body that cannot be read SHALL end the stream on both sides, counted and logged

The frame body and its metadata table SHALL be decoded by one decoder, `flz_transport`'s
payloader, on the runner reader and in the controller NIF frame buffer alike; the NIF SHALL keep
only its incremental buffering. A body or metadata table that decoder cannot read, including a
truncated metadata pair, SHALL be stream-fatal on the controller as it is on the runner: the
driver SHALL close the stream through its existing close path with the distinct close reason
`malformed_frame`, SHALL count it in the driver's stats, and the close SHALL be logged by the
stream's owner. No frame SHALL be skipped silently, and no pair of a truncated metadata table
SHALL be delivered.

#### Scenario: The controller closes a stream whose frame body it cannot read

- **WHEN** a runner writes a frame whose header is within the limit and whose body's inner
  lengths do not fit the payload
- **THEN** the controller's driver closes that stream with reason `malformed_frame`, its
  malformed-frame count rises by one, and other streams are unaffected, on UDS and on QUIC

#### Scenario: A truncated metadata table is refused, not shortened

- **WHEN** a frame's metadata table ends inside a key or value
- **THEN** both decoders refuse the frame, and the controller delivers none of the table's pairs

#### Scenario: Well-formed frames decode as before

- **WHEN** frames within the limit, with and without metadata, arrive split across reads
- **THEN** the controller delivers each frame whole and unchanged, exactly as before the decoder
  was shared
