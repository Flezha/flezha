# controller-send-buffer-bound Specification

## Purpose

The controller-send-buffer-bound capability bounds every per-stream outbound send buffer in the tree — both server drivers and the QUIC client — so a slow or non-reading peer can neither grow memory without limit nor have its sender's frames quietly discarded. Each buffer is bounded by a configurable `max_write_buffer_bytes` and reaches it through one send gate: crossing a high-water mark raises a congestion signal the sender reads and holds on, and draining below a low-water mark lowers it and wakes the sender. Nothing is discarded for congestion, so `write_buffer_dropped` is an invariant rather than a metric — a non-zero value is a defect, emitted with the driver's statistics and named by a liveness verdict. Both server drivers produce that reporting, so a healthy `/healthz` is evidence about the transport on a UDS and on a QUIC controller alike.
## Requirements
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

