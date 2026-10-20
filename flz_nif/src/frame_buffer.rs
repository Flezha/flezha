use flz_transport::service::payloader;
use flz_transport::transport::close_reason::CloseReason;
use rustler::LocalPid;

use crate::driver_msg::send_data_msg;

/// Frame header width, owned by the wire format's payloader module.
const HEADER_SIZE: usize = payloader::HEADER_LEN;

/// A completely parsed frame ready to deliver to Elixir.
#[derive(Debug)]
pub struct ParsedFrame {
    pub payload_id: String,
    pub message: Vec<u8>,
    pub metadata: payloader::PayloadMetadata,
}

/// Where one stream's parsed frames go: to the owner as `{:flz_data, …}`
/// while the stream is active, into a queue while it is passive — flushed in
/// order the moment it is activated, so no frame is delivered out of turn.
/// Activated once, it hands over one frame and is passive again, so the frames
/// behind that one wait for whoever activates the stream next.
pub struct Inbox {
    active: bool,
    once: bool,
    passive_queue: Vec<ParsedFrame>,
}

impl Inbox {
    /// A stream starts passive: its first owner activates it.
    pub fn new() -> Self {
        Inbox {
            active: false,
            once: false,
            passive_queue: Vec::new(),
        }
    }

    /// Hand `frames` to `owner` while the stream is active, and hold the rest.
    pub fn deliver(&mut self, owner: LocalPid, stream_id: u64, frames: Vec<ParsedFrame>) {
        let mut frames = frames.into_iter();
        while self.active {
            let Some(frame) = frames.next() else {
                return;
            };
            self.active = !self.once;
            send_data_msg(owner, stream_id, frame);
        }
        self.passive_queue.extend(frames);
    }

    /// Active from now on, or for one frame when `once`: everything held while
    /// passive goes first, in order.
    pub fn activate(&mut self, owner: LocalPid, stream_id: u64, once: bool) {
        self.active = true;
        self.once = once;
        let queued = std::mem::take(&mut self.passive_queue);
        self.deliver(owner, stream_id, queued);
    }

    pub fn deactivate(&mut self) {
        self.active = false;
    }
}

impl Default for Inbox {
    fn default() -> Self {
        Self::new()
    }
}

/// Bytes that cannot be framed. The byte stream cannot resync past either, so
/// the owner must close the stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameError {
    /// A header declared a payload length above the configured maximum.
    TooLarge,
    /// A payload the wire format's decoder refused: an inner length overran
    /// it, or its metadata table was cut inside a pair.
    Malformed,
}

impl FrameError {
    pub fn close_reason(self) -> CloseReason {
        match self {
            FrameError::TooLarge => CloseReason::FrameTooLarge,
            FrameError::Malformed => CloseReason::MalformedFrame,
        }
    }
}

/// Stateful frame parser that accumulates bytes and yields complete frames.
///
/// States:
///   Empty          — waiting for the next frame's header
///   PartialHeader  — have < HEADER_SIZE bytes of header
///   PartialPayload — have header, waiting for payload_size bytes
///   Poisoned       — a frame could not be read; feeds keep failing
pub struct FrameBuffer {
    state: BufferState,
    max_frame_bytes: usize,
}

enum BufferState {
    Empty,
    PartialHeader {
        buf: Vec<u8>,
    },
    PartialPayload {
        payload_id: String,
        payload_size: usize,
        buf: Vec<u8>,
    },
    /// A frame could not be read; every further feed repeats the error.
    Poisoned(FrameError),
}

impl FrameBuffer {
    pub fn new() -> Self {
        Self::with_limit(flz_transport::transport::config::max_frame_bytes())
    }

    pub fn with_limit(max_frame_bytes: usize) -> Self {
        FrameBuffer {
            state: BufferState::Empty,
            max_frame_bytes,
        }
    }

    /// Feed incoming bytes into the parser. Returns 0 or more complete frames,
    /// or a stream-fatal `FrameError` if a header declares an over-limit
    /// payload length (checked BEFORE allocating for it) or a payload does
    /// not decode.
    pub fn feed(&mut self, data: &[u8]) -> Result<Vec<ParsedFrame>, FrameError> {
        let mut frames = Vec::new();
        let mut remaining = data;

        loop {
            if let BufferState::Poisoned(e) = self.state {
                return Err(e);
            }
            if remaining.is_empty() {
                break;
            }
            let state = std::mem::replace(&mut self.state, BufferState::Empty);
            self.state = self
                .step(state, &mut remaining, &mut frames)
                .unwrap_or_else(BufferState::Poisoned);
        }

        Ok(frames)
    }

    /// One transition: consume what `remaining` offers the current state and
    /// answer the next one.
    fn step(
        &self,
        state: BufferState,
        remaining: &mut &[u8],
        frames: &mut Vec<ParsedFrame>,
    ) -> Result<BufferState, FrameError> {
        match state {
            BufferState::Empty => {
                if remaining.len() >= HEADER_SIZE {
                    // Fast path: header available in-place, no accumulation.
                    let (header, rest) = remaining.split_at(HEADER_SIZE);
                    *remaining = rest;
                    self.start_frame(header, remaining, frames)
                } else {
                    let mut buf = Vec::with_capacity(HEADER_SIZE);
                    fill(&mut buf, HEADER_SIZE, remaining);
                    Ok(BufferState::PartialHeader { buf })
                }
            }
            BufferState::PartialHeader { mut buf } => {
                if fill(&mut buf, HEADER_SIZE, remaining) {
                    self.start_frame(&buf, remaining, frames)
                } else {
                    Ok(BufferState::PartialHeader { buf })
                }
            }
            BufferState::PartialPayload {
                payload_id,
                payload_size,
                mut buf,
            } => {
                if fill(&mut buf, payload_size, remaining) {
                    frames.push(decode(payload_id, &buf)?);
                    Ok(BufferState::Empty)
                } else {
                    Ok(BufferState::PartialPayload {
                        payload_id,
                        payload_size,
                        buf,
                    })
                }
            }
            BufferState::Poisoned(_) => unreachable!("poison returns before a step"),
        }
    }

    /// A complete 36-byte header: enforce the limit BEFORE any payload
    /// allocation, then consume as much of the payload as is available.
    fn start_frame(
        &self,
        header: &[u8],
        remaining: &mut &[u8],
        frames: &mut Vec<ParsedFrame>,
    ) -> Result<BufferState, FrameError> {
        let (payload_id, payload_size) = parse_header(&header[..HEADER_SIZE]);
        if payload_size > self.max_frame_bytes {
            return Err(FrameError::TooLarge);
        }

        let cur = *remaining;
        Ok(if cur.len() >= payload_size {
            frames.push(decode(payload_id, &cur[..payload_size])?);
            *remaining = &cur[payload_size..];
            BufferState::Empty
        } else {
            let mut buf = Vec::with_capacity(payload_size);
            buf.extend_from_slice(cur);
            *remaining = &[];
            BufferState::PartialPayload {
                payload_id,
                payload_size,
                buf,
            }
        })
    }
}

/// Append bytes from `remaining` into `buf` until it reaches `want`; advances
/// `remaining` and reports whether `buf` is complete.
fn fill(buf: &mut Vec<u8>, want: usize, remaining: &mut &[u8]) -> bool {
    let cur = *remaining;
    let take = std::cmp::min(want - buf.len(), cur.len());
    buf.extend_from_slice(&cur[..take]);
    *remaining = &cur[take..];
    buf.len() >= want
}

/// Parse the fixed-width header into (payload_id, payload_size).
fn parse_header(header: &[u8]) -> (String, usize) {
    let payload_id = payloader::parse_payload_id(&header[..payloader::PAYLOAD_ID_LEN]);
    let payload_size =
        u32::from_be_bytes([header[32], header[33], header[34], header[35]]) as usize;
    (payload_id, payload_size)
}

/// One whole payload through the wire format's own decoder.
fn decode(payload_id: String, body: &[u8]) -> Result<ParsedFrame, FrameError> {
    let (message, metadata) =
        payloader::parse_payload_body(body).map_err(|_| FrameError::Malformed)?;
    Ok(ParsedFrame {
        payload_id,
        message,
        metadata,
    })
}

#[cfg(test)]
mod tests {
    use flz_transport::service::payloader;

    use super::*;

    #[test]
    fn test_partial_header() {
        let frame_bytes = payloader::build_without_metadata("my_id", b"data");

        let mut fb = FrameBuffer::new();

        // Feed only 20 bytes (partial header)
        let frames = fb.feed(&frame_bytes[..20]).unwrap();
        assert_eq!(frames.len(), 0);

        // Feed the rest
        let frames = fb.feed(&frame_bytes[20..]).unwrap();
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].payload_id, "my_id");
        assert_eq!(frames[0].message, b"data");
    }

    #[test]
    fn test_partial_payload() {
        let frame_bytes = payloader::build_without_metadata("pid", b"a]longer message here");

        let mut fb = FrameBuffer::new();

        // Feed header + partial payload
        let frames = fb.feed(&frame_bytes[..40]).unwrap();
        assert_eq!(frames.len(), 0);

        // Feed rest
        let frames = fb.feed(&frame_bytes[40..]).unwrap();
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].payload_id, "pid");
    }

    #[test]
    fn test_multiple_frames_in_one_feed() {
        let f1 = payloader::build_without_metadata("id_a", b"msg1");
        let f2 = payloader::build_without_metadata("id_b", b"msg2");

        let mut combined = Vec::new();
        combined.extend_from_slice(&f1);
        combined.extend_from_slice(&f2);

        let mut fb = FrameBuffer::new();
        let frames = fb.feed(&combined).unwrap();

        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].payload_id, "id_a");
        assert_eq!(frames[1].payload_id, "id_b");
    }

    #[test]
    fn test_empty_feed() {
        let mut fb = FrameBuffer::new();
        let frames = fb.feed(&[]).unwrap();
        assert_eq!(frames.len(), 0);
    }

    fn header(payload_size: u32) -> Vec<u8> {
        let mut bytes = vec![b' '; 32];
        bytes.extend_from_slice(&payload_size.to_be_bytes());
        bytes
    }

    /// A frame whose header is honest about `body`, whatever `body` says inside.
    fn frame_around(body: &[u8]) -> Vec<u8> {
        let mut bytes = header(body.len() as u32);
        bytes.extend_from_slice(body);
        bytes
    }

    /// Fed whole and split inside the payload, so both the in-place and the
    /// accumulated path reach the decoder.
    fn refused_whole_and_split(bytes: &[u8]) {
        for split_at in [bytes.len(), 38] {
            let mut fb = FrameBuffer::new();
            let first = fb.feed(&bytes[..split_at]);
            let err = if split_at == bytes.len() {
                first.unwrap_err()
            } else {
                assert_eq!(first.unwrap().len(), 0);
                fb.feed(&bytes[split_at..]).unwrap_err()
            };
            assert_eq!(err, FrameError::Malformed);
            assert_eq!(err.close_reason().to_string(), "malformed_frame");

            let good = payloader::build_without_metadata("id", b"data");
            assert_eq!(fb.feed(&good).unwrap_err(), FrameError::Malformed);
        }
    }

    #[test]
    fn a_body_whose_message_length_overruns_its_payload_is_refused() {
        let mut body = 100u32.to_be_bytes().to_vec();
        body.extend_from_slice(b"short\0\0");
        refused_whole_and_split(&frame_around(&body));
    }

    #[test]
    fn a_metadata_table_cut_inside_a_value_is_refused() {
        let table = [1, 5, b'k', b'v', b'a'];
        let mut body = 1u32.to_be_bytes().to_vec();
        body.push(b'm');
        body.extend_from_slice(&(table.len() as u16).to_be_bytes());
        body.extend_from_slice(&table);
        refused_whole_and_split(&frame_around(&body));
    }

    /// An over-limit header is refused the moment it completes — arriving
    /// whole or split across feeds — before any payload is allocated.
    #[test]
    fn an_oversized_header_is_refused_whole_or_split() {
        for (declared, split_at) in [(u32::MAX, None), (2048, Some(20))] {
            let mut fb = FrameBuffer::with_limit(1024);
            let bytes = header(declared);
            let err = match split_at {
                None => fb.feed(&bytes).unwrap_err(),
                Some(at) => {
                    assert_eq!(fb.feed(&bytes[..at]).unwrap().len(), 0);
                    fb.feed(&bytes[at..]).unwrap_err()
                }
            };
            assert_eq!(err, FrameError::TooLarge);
        }
    }

    #[test]
    fn poisoned_buffer_keeps_erroring() {
        let mut fb = FrameBuffer::with_limit(1024);
        assert!(fb.feed(&header(4096)).is_err());

        // A later feed of otherwise-valid bytes must not resync.
        let good = payloader::build_without_metadata("id", b"data");
        assert_eq!(fb.feed(&good).unwrap_err(), FrameError::TooLarge);
    }

    #[test]
    fn in_limit_frame_at_exact_limit_still_decodes() {
        let frame_bytes = payloader::build_without_metadata("edge", b"payload");
        // payload_size = total - 36-byte header; a limit of exactly that passes.
        let payload_size = frame_bytes.len() - 36;

        let mut fb = FrameBuffer::with_limit(payload_size);
        let frames = fb.feed(&frame_bytes).unwrap();
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].message, b"payload");
    }
}
