use std::collections::HashMap;

/// Header size: payload_id (32) + payload_size (4) = 36 bytes.
const HEADER_SIZE: usize = 36;

/// A completely parsed frame ready to deliver to Elixir.
#[derive(Debug)]
pub struct ParsedFrame {
    pub payload_id: String,
    pub message: Vec<u8>,
    pub metadata: HashMap<String, String>,
}

/// Stateful frame parser that accumulates bytes and yields complete frames.
///
/// Port of Elixir `Payloader.parse_frame/3` state machine.
/// States:
///   Empty          — waiting for first byte
///   PartialHeader  — have < 36 bytes of header
///   PartialPayload — have header, waiting for payload_size bytes
pub struct FrameBuffer {
    state: BufferState,
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
}

impl FrameBuffer {
    pub fn new() -> Self {
        FrameBuffer {
            state: BufferState::Empty,
        }
    }

    /// Feed incoming bytes into the parser. Returns 0 or more complete frames.
    pub fn feed(&mut self, data: &[u8]) -> Vec<ParsedFrame> {
        let mut frames = Vec::new();
        let mut remaining = data;

        loop {
            let consumed;
            match &mut self.state {
                BufferState::Empty => {
                    if remaining.is_empty() {
                        break;
                    }
                    if remaining.len() >= HEADER_SIZE {
                        // Parse header directly
                        let (payload_id, payload_size) = parse_header(&remaining[..HEADER_SIZE]);
                        consumed = HEADER_SIZE;
                        remaining = &remaining[consumed..];
                        // Try to complete payload immediately
                        if remaining.len() >= payload_size {
                            if let Some(frame) =
                                parse_payload(&payload_id, &remaining[..payload_size])
                            {
                                frames.push(frame);
                            }
                            remaining = &remaining[payload_size..];
                            // Stay in Empty state for next frame
                        } else {
                            // Partial payload
                            let mut buf = Vec::with_capacity(payload_size);
                            buf.extend_from_slice(remaining);
                            self.state = BufferState::PartialPayload {
                                payload_id,
                                payload_size,
                                buf,
                            };
                            break;
                        }
                    } else {
                        // Partial header
                        let mut buf = Vec::with_capacity(HEADER_SIZE);
                        buf.extend_from_slice(remaining);
                        self.state = BufferState::PartialHeader { buf };
                        break;
                    }
                }
                BufferState::PartialHeader { buf } => {
                    if remaining.is_empty() {
                        break;
                    }
                    let need = HEADER_SIZE - buf.len();
                    let take = std::cmp::min(need, remaining.len());
                    buf.extend_from_slice(&remaining[..take]);
                    remaining = &remaining[take..];

                    if buf.len() >= HEADER_SIZE {
                        let (payload_id, payload_size) = parse_header(&buf[..HEADER_SIZE]);
                        // Transition to payload collection
                        if remaining.len() >= payload_size {
                            if let Some(frame) =
                                parse_payload(&payload_id, &remaining[..payload_size])
                            {
                                frames.push(frame);
                            }
                            remaining = &remaining[payload_size..];
                            self.state = BufferState::Empty;
                        } else {
                            let mut payload_buf = Vec::with_capacity(payload_size);
                            payload_buf.extend_from_slice(remaining);
                            self.state = BufferState::PartialPayload {
                                payload_id,
                                payload_size,
                                buf: payload_buf,
                            };
                            break;
                        }
                    } else {
                        break;
                    }
                }
                BufferState::PartialPayload {
                    payload_id,
                    payload_size,
                    buf,
                } => {
                    if remaining.is_empty() {
                        break;
                    }
                    let need = *payload_size - buf.len();
                    let take = std::cmp::min(need, remaining.len());
                    buf.extend_from_slice(&remaining[..take]);
                    remaining = &remaining[take..];

                    if buf.len() >= *payload_size {
                        let id = payload_id.clone();
                        let payload_data: Vec<u8> = buf.clone();
                        if let Some(frame) = parse_payload(&id, &payload_data) {
                            frames.push(frame);
                        }
                        self.state = BufferState::Empty;
                    } else {
                        break;
                    }
                }
            }
        }

        frames
    }
}

/// Parse the 36-byte header into (payload_id, payload_size).
fn parse_header(header: &[u8]) -> (String, usize) {
    let payload_id = String::from_utf8_lossy(&header[..32])
        .trim_end()
        .to_string();
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
        let frames = fb.feed(&frame_bytes);

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
        let frames = fb.feed(&frame_bytes[..20]);
        assert_eq!(frames.len(), 0);

        // Feed the rest
        let frames = fb.feed(&frame_bytes[20..]);
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
        let frames = fb.feed(&frame_bytes[..40]);
        assert_eq!(frames.len(), 0);

        // Feed rest
        let frames = fb.feed(&frame_bytes[40..]);
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
        let frames = fb.feed(&combined);

        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].payload_id, "id_a");
        assert_eq!(frames[1].payload_id, "id_b");
    }

    #[test]
    fn test_empty_feed() {
        let mut fb = FrameBuffer::new();
        let frames = fb.feed(&[]);
        assert_eq!(frames.len(), 0);
    }

    #[test]
    fn test_frame_with_metadata() {
        let mut metadata = HashMap::new();
        metadata.insert("foo".to_string(), "bar".to_string());
        metadata.insert("baz".to_string(), "qux".to_string());

        let frame_bytes = payloader::build_from_binary("meta_test", b"body", metadata);

        let mut fb = FrameBuffer::new();
        let frames = fb.feed(&frame_bytes);

        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].metadata.get("foo").unwrap(), "bar");
        assert_eq!(frames[0].metadata.get("baz").unwrap(), "qux");
    }
}
