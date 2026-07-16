# controller-send-buffer-bound Specification

## Purpose

The controller-send-buffer-bound capability bounds the controller transport driver's per-stream outbound send buffers so a slow or non-reading runner peer cannot grow controller memory without limit. Each per-stream write buffer (UDS `uds_driver.rs` `write_buffer` and QUIC `quic.rs` `write_buf`) is capped by a configurable `max_write_buffer_bytes`; enqueues past the cap drop the newest frame (`DropNewest`, mirroring the SDK bounded mailbox) rather than blocking the driver's command loop, and each drop is counted on `DriverStats` so it is observable.

## Requirements

### Requirement: Controller-to-runner transport send buffer is bounded

The controller transport driver SHALL bound each per-stream outbound send buffer
(UDS `uds_driver.rs` `write_buffer` and QUIC `quic.rs` `write_buf`) by a
configurable `max_write_buffer_bytes` cap. When the socket cannot accept more
bytes (e.g. `WouldBlock`) and an enqueue would push the buffer past the cap, the
driver SHALL drop the incoming frame rather than append it, applying a
`DropNewest` policy that mirrors the SDK bounded mailbox. The driver SHALL NOT
block its command loop on a slow or non-reading peer.

#### Scenario: Enqueue past the cap drops the newest frame

- **WHEN** a stream's peer is not reading (socket writes return `WouldBlock`) and
  frames are enqueued until the buffered bytes reach `max_write_buffer_bytes`
- **THEN** further enqueued frames are dropped and the send buffer stays at or
  below `max_write_buffer_bytes` instead of growing without bound

#### Scenario: Buffer drains and accepts new frames again

- **WHEN** the peer resumes reading and the buffered bytes fall back below the cap
- **THEN** subsequently enqueued frames are buffered and flushed normally

### Requirement: Dropped send-buffer frames are counted

The driver SHALL expose a `write_buffer_dropped` counter on `DriverStats` that
increments once per frame dropped due to the send-buffer cap, so the drop is
observable rather than silent.

#### Scenario: Drop increments the counter

- **WHEN** a frame is dropped because the send buffer is at the cap
- **THEN** the stream's `write_buffer_dropped` count in `DriverStats` increases
  by one
