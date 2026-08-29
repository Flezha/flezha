use std::collections::HashMap;

use flz_transport::service::payloader;
use rustler::LocalPid;

use crate::driver_msg::send_data_msg;

/// Frame header width, owned by the wire format's payloader module.
const HEADER_SIZE: usize = payloader::HEADER_LEN;

/// A completely parsed frame ready to deliver to Elixir.
#[derive(Debug)]
pub struct ParsedFrame {
    pub payload_id: String,
    pub message: Vec<u8>,
    pub metadata: HashMap<String, String>,
}

/// Where one stream's parsed frames go: to the owner as `{:flz_data, …}`
/// while the stream is active, into a queue while it is passive — flushed in
/// order the moment it is activated, so no frame is delivered out of turn.
pub struct Inbox {
    active: bool,
    passive_queue: Vec<ParsedFrame>,
}

impl Inbox {
    /// A stream starts passive: its first owner activates it.
    pub fn new() -> Self {
        Inbox {
            active: false,
            passive_queue: Vec::new(),
        }
    }

    /// Hand `frames` to `owner` now, or hold them until the stream is active.
    pub fn deliver(&mut self, owner: LocalPid, stream_id: u64, frames: Vec<ParsedFrame>) {
        if self.active {
            for frame in frames {
                send_data_msg(owner, stream_id, frame);
            }
        } else {
            self.passive_queue.extend(frames);
        }
    }

    /// Active from now on: everything held while passive goes first, in order.
    pub fn activate(&mut self, owner: LocalPid, stream_id: u64) {
        self.active = true;
        let queued: Vec<_> = self.passive_queue.drain(..).collect();
        for frame in queued {
            send_data_msg(owner, stream_id, frame);
        }
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

/// A header declared a payload length above the configured maximum. The byte
/// stream cannot resync past it, so the owner must close the stream.
#[derive(Debug)]
pub struct FrameError {
    pub declared: usize,
    pub max: usize,
}

impl std::fmt::Display for FrameError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "declared {} > max {}", self.declared, self.max)
    }
}

impl std::error::Error for FrameError {}

/// Stateful frame parser that accumulates bytes and yields complete frames.
///
/// States:
///   Empty          — waiting for the next frame's header
///   PartialHeader  — have < HEADER_SIZE bytes of header
///   PartialPayload — have header, waiting for payload_size bytes
///   Poisoned       — an over-limit header was seen; feeds keep failing
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
    /// An over-limit header was seen; every further feed repeats the error.
    Poisoned {
        declared: usize,
    },
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
    /// payload length (checked BEFORE allocating for it).
    pub fn feed(&mut self, data: &[u8]) -> Result<Vec<ParsedFrame>, FrameError> {
        let mut frames = Vec::new();
        let mut remaining = data;

        loop {
            if let BufferState::Poisoned { declared } = &self.state {
                return Err(FrameError {
                    declared: *declared,
                    max: self.max_frame_bytes,
                });
            }
            if remaining.is_empty() {
                break;
            }
            let state = std::mem::replace(&mut self.state, BufferState::Empty);
            match self.step(state, &mut remaining, &mut frames) {
                Ok(state) => self.state = state,
                Err(e) => {
                    self.state = BufferState::Poisoned {
                        declared: e.declared,
                    };
                    return Err(e);
                }
            }
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
                    if let Some(frame) = parse_payload(&payload_id, &buf) {
                        frames.push(frame);
                    }
                    Ok(BufferState::Empty)
                } else {
                    Ok(BufferState::PartialPayload {
                        payload_id,
                        payload_size,
                        buf,
                    })
                }
            }
            BufferState::Poisoned { .. } => unreachable!("poison returns before a step"),
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
            return Err(FrameError {
                declared: payload_size,
                max: self.max_frame_bytes,
            });
        }

        let cur = *remaining;
        Ok(if cur.len() >= payload_size {
            if let Some(frame) = parse_payload(&payload_id, &cur[..payload_size]) {
                frames.push(frame);
            }
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

/// Parse payload body: message_size(4) + message + metadata_size(2) + metadata.
fn parse_payload(payload_id: &str, data: &[u8]) -> Option<ParsedFrame> {
    if data.len() < 6 {
        return None; // Need at least message_size(4) + metadata_size(2)
    }

    let message_size = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;

    if data.len() < 4 + message_size + 2 {
        return None;
    }

    let message = data[4..4 + message_size].to_vec();

    let meta_offset = 4 + message_size;
    let metadata_size = u16::from_be_bytes([data[meta_offset], data[meta_offset + 1]]) as usize;

    let meta_start = meta_offset + 2;
    if data.len() < meta_start + metadata_size {
        return None;
    }

    let metadata = deserialize_metadata(&data[meta_start..meta_start + metadata_size]);

    Some(ParsedFrame {
        payload_id: payload_id.to_string(),
        message,
        metadata,
    })
}

/// Deserialize flat metadata format: [key_len(1)] [val_len(1)] [key] [value] ...
fn deserialize_metadata(data: &[u8]) -> HashMap<String, String> {
    let mut metadata = HashMap::new();
    let mut offset = 0;

    while offset + 2 <= data.len() {
        let key_len = data[offset] as usize;
        let val_len = data[offset + 1] as usize;
        offset += 2;

        if offset + key_len + val_len > data.len() {
            break;
        }

        let key = String::from_utf8_lossy(&data[offset..offset + key_len]).to_string();
        offset += key_len;
        let value = String::from_utf8_lossy(&data[offset..offset + val_len]).to_string();
        offset += val_len;

        metadata.insert(key, value);
    }

    metadata
}

#[cfg(test)]
mod tests {
    use flz_transport::service::payloader;

    use super::*;

    #[test]
    fn test_single_complete_frame() {
        let mut metadata = HashMap::new();
        metadata.insert("key1".to_string(), "val1".to_string());

        let frame_bytes =
            payloader::build_from_binary("test_payload", b"hello world", metadata.clone());

        let mut fb = FrameBuffer::new();
        let frames = fb.feed(&frame_bytes).unwrap();

        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].payload_id, "test_payload");
        assert_eq!(frames[0].message, b"hello world");
        assert_eq!(frames[0].metadata.get("key1").unwrap(), "val1");
    }

    #[test]
    fn test_partial_header() {
        let frame_bytes = payloader::build_from_binary("my_id", b"data", HashMap::new());

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
        let frame_bytes =
            payloader::build_from_binary("pid", b"a]longer message here", HashMap::new());

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
        let f1 = payloader::build_from_binary("id_a", b"msg1", HashMap::new());
        let f2 = payloader::build_from_binary("id_b", b"msg2", HashMap::new());

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

    fn oversized_header(payload_size: u32) -> Vec<u8> {
        let mut bytes = vec![b' '; 32];
        bytes.extend_from_slice(&payload_size.to_be_bytes());
        bytes
    }

    /// An over-limit header is refused the moment it completes — arriving
    /// whole or split across feeds — before any payload is allocated.
    #[test]
    fn an_oversized_header_is_refused_whole_or_split() {
        for (declared, split_at) in [(u32::MAX, None), (2048, Some(20))] {
            let mut fb = FrameBuffer::with_limit(1024);
            let header = oversized_header(declared);
            let err = match split_at {
                None => fb.feed(&header).unwrap_err(),
                Some(at) => {
                    assert_eq!(fb.feed(&header[..at]).unwrap().len(), 0);
                    fb.feed(&header[at..]).unwrap_err()
                }
            };
            assert_eq!(err.declared, declared as usize);
            assert_eq!(err.max, 1024);
        }
    }

    #[test]
    fn poisoned_buffer_keeps_erroring() {
        let mut fb = FrameBuffer::with_limit(1024);
        assert!(fb.feed(&oversized_header(4096)).is_err());

        // A later feed of otherwise-valid bytes must not resync.
        let good = payloader::build_from_binary("id", b"data", HashMap::new());
        let err = fb.feed(&good).unwrap_err();
        assert_eq!(err.declared, 4096);
    }

    #[test]
    fn a_frame_error_renders_the_declared_length_against_the_limit() {
        let err = FrameError {
            declared: 4096,
            max: 1024,
        };
        assert_eq!(err.to_string(), "declared 4096 > max 1024");
    }

    #[test]
    fn in_limit_frame_at_exact_limit_still_decodes() {
        let frame_bytes = payloader::build_from_binary("edge", b"payload", HashMap::new());
        // payload_size = total - 36-byte header; a limit of exactly that passes.
        let payload_size = frame_bytes.len() - 36;

        let mut fb = FrameBuffer::with_limit(payload_size);
        let frames = fb.feed(&frame_bytes).unwrap();
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].message, b"payload");
    }

    #[test]
    fn test_frame_with_metadata() {
        let mut metadata = HashMap::new();
        metadata.insert("foo".to_string(), "bar".to_string());
        metadata.insert("baz".to_string(), "qux".to_string());

        let frame_bytes = payloader::build_from_binary("meta_test", b"body", metadata);

        let mut fb = FrameBuffer::new();
        let frames = fb.feed(&frame_bytes).unwrap();

        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].metadata.get("foo").unwrap(), "bar");
        assert_eq!(frames[0].metadata.get("baz").unwrap(), "qux");
    }
}
