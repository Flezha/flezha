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

