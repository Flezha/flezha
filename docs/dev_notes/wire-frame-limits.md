# wire-frame-limits

One malformed 36-byte header could force a ~4 GiB allocation in either frame
parser (runner reader thread death; whole-BEAM death inside the NIF). Both
decode paths now reject a declared length above `FLEZHA_MAX_FRAME_BYTES`
(default 64 MiB) BEFORE allocating.

## Load-bearing decisions

- **The parsers run on raw bytes before any authentication verdict.** Transport
  authn does not cover this; the limit is the only pre-identity defence on an
  exposed port.
- **One limit, defined once**: `max_frame_bytes()` in
  `flz_transport/src/transport/config.rs`, used by the payloader directly and
  by `FrameBuffer::new()` (flz_nif depends on flz_transport). It is
  `OnceLock`-cached — boot-time knob, one atomic load on the per-frame hot
  path. `max_write_buffer_bytes()` moved here too (was duplicated verbatim in
  `uds_driver.rs` and `transport/quic.rs`); it stays per-call-read, so its
  env-var semantics are unchanged.
- **The inner `message_length` allocates BEFORE `read_exact` can fail.** The
  slice read would error naturally, but only after `vec![0; message_length]`
  already allocated from the hostile value — hence the explicit
  `message_length <= payload_len - 6` check in `parse_payload_body` (shared by
  both extract variants). The NIF-side `parse_payload` was already
  slice-bounded; only the outer prefix needed the check there.
- **A rejected header poisons the stream — there is no resync.** The byte
  stream has no recoverable frame boundary after a bad header.
  `FrameBuffer::feed` returns `Result`; a violation moves it to
  `BufferState::Poisoned` and every later feed repeats the error. Drivers close
  the stream via their existing close path with reason `frame_too_large`.
  Runner side rejects with `io::ErrorKind::InvalidData`, which
  `ControlLiveness::classify_error` already treats as stream-fatal (only
  `Interrupted` retries) — no new error machinery.
- **`FrameBuffer::feed` fast path must not regress**: a full header available
  in the `Empty` state is `split_at` on the input slice — never accumulate into
  a header Vec on the hot path (~115K msg/s).
- Wire-format geometry (`PAYLOAD_ID_LEN` = 32, `HEADER_LEN` = 36) is exported
  by the payloader; `frame_buffer`'s `HEADER_SIZE` aliases it.

## Test map

- `flz_transport` `service::payloader::tests` — oversized outer prefix →
  `InvalidData` (not `UnexpectedEof`, which is what the unfixed path yields
  under overcommit); inner > outer; in-limit round-trip both variants;
  reject-classifies-as-`Reconnect`.
- `flz_transport` `transport::config::tests` — knob parse override/default/
  garbage/zero. Tests the pure parser, NOT the accessor: the `OnceLock` cache
  is process-global, an env-mutating test would race the payloader tests.
- `flz_nif` `frame_buffer::tests` — oversized header (whole and split across
  feeds); poisoned buffer keeps erroring on later valid bytes;
  exact-limit frame still decodes; pre-existing partial/multi-frame tests
  unchanged (`FrameBuffer::with_limit` keeps them env-independent).
