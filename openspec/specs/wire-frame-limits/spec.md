# wire-frame-limits Specification

## Purpose

The wire-frame-limits capability bounds every transport decode path: a
configured maximum frame/payload/message length is enforced BEFORE any buffer
is allocated from an untrusted length prefix, on both the runner reader
(`flz_transport` payloader) and the controller NIF frame buffer. A single
malformed header can therefore no longer force a multi-GiB allocation that
kills a runner reader thread or the controller VM.

## Requirements

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

- **WHEN** an operator sets `FLEZHA_MAX_FRAME_BYTES`
- **THEN** both the runner payloader and the controller frame buffer enforce
  that same value, and with the variable unset both enforce the same default
