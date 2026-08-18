use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::str;

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

/// Metadata key-value pairs attached to a payload frame.
pub type PayloadMetadata = HashMap<String, String>;

/// Fixed width of the space-padded payload-id header field.
pub const PAYLOAD_ID_LEN: usize = 32;

/// Frame header width: payload_id + payload_size (4-byte BE).
pub const HEADER_LEN: usize = PAYLOAD_ID_LEN + 4;

/// Decode the fixed-width, space-padded payload-id header field.
pub fn parse_payload_id(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).trim_end().to_string()
}

/// A declared length prefix an untrusted peer controls exceeded its bound.
/// Raised BEFORE allocating for the declared length; the stream cannot resync
/// past it, so callers treat it as stream-fatal.
fn reject_oversized(what: &str, declared: usize, max: usize) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("frame {what} length {declared} exceeds maximum {max}"),
    )
}

fn check_payload_length(payload_length: usize) -> io::Result<()> {
    let max = crate::transport::config::max_frame_bytes();
    if payload_length > max {
        return Err(reject_oversized("payload", payload_length, max));
    }
    Ok(())
}

/// Parse a payload body: message_size (4) + message + metadata_size (2) + metadata.
fn parse_payload_body(payload_buf: &[u8]) -> io::Result<(Vec<u8>, PayloadMetadata)> {
    let mut payload_reader = payload_buf;

    // Read the message size (4 bytes), bounded by the enclosing payload so a
    // hostile inner prefix cannot force an allocation the payload can't fill.
    let message_length = payload_reader.read_u32::<BigEndian>()? as usize;
    let max_message = payload_buf.len().saturating_sub(4 + 2);
    if message_length > max_message {
        return Err(reject_oversized("message", message_length, max_message));
    }

    // Read the actual message
    let mut message_buf = vec![0; message_length];
    payload_reader.read_exact(&mut message_buf)?;

    // Read the metadata size (2 bytes)
    let metadata_length = payload_reader.read_u16::<BigEndian>()? as usize;

    // Read the metadata
    let mut metadata_buf = vec![0; metadata_length];
    payload_reader.read_exact(&mut metadata_buf)?;

    // Deserialize metadata
    let metadata = deserialize_metadata(&metadata_buf)?;

    Ok((message_buf, metadata))
}

pub fn extract_payload<R: Read>(
    mut reader: R,
) -> io::Result<(String, Vec<u8>, HashMap<String, String>)> {
    // Read the payload ID
    let mut id_buf = [0; PAYLOAD_ID_LEN];
    reader.read_exact(&mut id_buf)?;
    let payload_id = parse_payload_id(&id_buf);

    // Read the payload size (4 bytes)
    let payload_length = reader.read_u32::<BigEndian>()? as usize;
    check_payload_length(payload_length)?;

    // Read the entire payload
    let mut payload_buf = vec![0; payload_length];
    reader.read_exact(&mut payload_buf)?;

    let (message_buf, metadata) = parse_payload_body(&payload_buf)?;

    Ok((payload_id, message_buf, metadata))
}

// reason: tuple shape is the wire-extraction return type; refactoring would change the public signature
#[allow(clippy::type_complexity)]
pub fn extract_payload_non_blocking<R: Read>(
    mut reader: R,
) -> io::Result<Option<(String, Vec<u8>, HashMap<String, String>)>> {
    let mut header_buf = [0; HEADER_LEN];
    match reader.read(&mut header_buf) {
        Ok(0) => {
            // End of stream
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "Connection closed",
            ));
        }
        Ok(n) if n < HEADER_LEN => {
            // This should not happen with UnixStream, but as a precaution.
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Incomplete header",
            ));
        }
        Ok(_) => {
            // Header read successfully
        }
        Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
            return Ok(None);
        }
        Err(e) => {
            return Err(e);
        }
    }

    let payload_id = parse_payload_id(&header_buf[..PAYLOAD_ID_LEN]);
    let payload_length = (&header_buf[PAYLOAD_ID_LEN..]).read_u32::<BigEndian>()? as usize;
    check_payload_length(payload_length)?;

    // Read the entire payload
    let mut payload_buf = vec![0; payload_length];
    if payload_length > 0 {
        match reader.read_exact(&mut payload_buf) {
            Ok(_) => {}
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                // This is a protocol error. We got a header but can't get the body.
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Partial payload read",
                ));
            }
            Err(e) => {
                return Err(e);
            }
        }
    }

    let (message_buf, metadata) = parse_payload_body(&payload_buf)?;

    Ok(Some((payload_id, message_buf, metadata)))
}

pub fn build_from_binary(payload_id: &str, message: &[u8], metadata: PayloadMetadata) -> Vec<u8> {
    build_payload(payload_id, message, metadata)
}

pub fn build_from_str(payload_id: &str, message: &str, metadata: PayloadMetadata) -> Vec<u8> {
    build_payload(payload_id, message.as_bytes(), metadata)
}

/// Zero-alloc variant of `build_from_binary`: appends the framed payload
/// bytes to `out` in-place. Used by the hot path to batch many outputs
/// into a single write syscall.
pub fn build_from_binary_into(
    out: &mut Vec<u8>,
    payload_id: &str,
    message: &[u8],
    metadata: &PayloadMetadata,
) {
    append_payload(out, payload_id, message, metadata);
}

pub fn build_from_str_into(
    out: &mut Vec<u8>,
    payload_id: &str,
    message: &str,
    metadata: &PayloadMetadata,
) {
    append_payload(out, payload_id, message.as_bytes(), metadata);
}

fn append_payload(out: &mut Vec<u8>, payload_id: &str, message: &[u8], metadata: &PayloadMetadata) {
    // Serialize metadata directly into the metadata region after we write
    // the metadata_size. We can't do that cleanly because metadata_size
    // precedes the bytes, so serialize to a scratch Vec first.
    let metadata_binary = serialize_metadata(metadata);
    let metadata_size = metadata_binary.len() as u16;
    let message_size = message.len() as u32;
    let payload_size = 4 + message.len() + 2 + metadata_binary.len();

    // Reserve in-place to avoid resizes during the appends below.
    out.reserve(32 + 4 + payload_size);

    // Write the payload ID (32 bytes, space-padded).
    let id_bytes = payload_id.as_bytes();
    if id_bytes.len() >= 32 {
        out.extend_from_slice(&id_bytes[..32]);
    } else {
        out.extend_from_slice(id_bytes);
        for _ in id_bytes.len()..32 {
            out.push(b' ');
        }
    }

    // Payload size (4 BE).
    out.extend_from_slice(&(payload_size as u32).to_be_bytes());

    // Message size (4 BE) + message.
    out.extend_from_slice(&message_size.to_be_bytes());
    out.extend_from_slice(message);

    // Metadata size (2 BE) + metadata.
    out.extend_from_slice(&metadata_size.to_be_bytes());
    out.extend_from_slice(&metadata_binary);
}

fn build_payload(payload_id: &str, message: &[u8], metadata: PayloadMetadata) -> Vec<u8> {
    // Serialize metadata
    let metadata_binary = serialize_metadata(&metadata);
    let metadata_size = metadata_binary.len() as u16;
    let message_size = message.len() as u32;

    // Calculate payload size: message_size (4) + message + metadata_size (2) + metadata
    let payload_size = 4 + message.len() + 2 + metadata_binary.len();

    // Ensure the payload ID is 32 bytes
    let mut payload_id_padded = payload_id.to_string();
    if payload_id_padded.len() > 32 {
        payload_id_padded.truncate(32);
    } else {
        payload_id_padded.push_str(&" ".repeat(32 - payload_id_padded.len()));
    }

    // Calculate total capacity: payload_id (32) + payload_size (4) + payload_size
    let total_capacity = 32 + 4 + payload_size;
    let mut result = Vec::with_capacity(total_capacity);

    // Write the payload ID (32 bytes)
    result.write_all(payload_id_padded.as_bytes()).unwrap();

    // Write the payload size (4 bytes)
    result.write_u32::<BigEndian>(payload_size as u32).unwrap();

    // Write the message size (4 bytes)
    result.write_u32::<BigEndian>(message_size).unwrap();

    // Write the message
    result.write_all(message).unwrap();

    // Write the metadata size (2 bytes)
    result.write_u16::<BigEndian>(metadata_size).unwrap();

    // Write the metadata
    result.write_all(&metadata_binary).unwrap();

    result
}

fn serialize_metadata(metadata: &HashMap<String, String>) -> Vec<u8> {
    let mut result = Vec::new();

    for (key, value) in metadata {
        let key_bytes = key.as_bytes();
        let value_bytes = value.as_bytes();

        // Write key size (1 byte)
        result.push(key_bytes.len() as u8);
        // Write value size (1 byte)
        result.push(value_bytes.len() as u8);
        // Write key
        result.extend_from_slice(key_bytes);
        // Write value
        result.extend_from_slice(value_bytes);
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header(payload_length: u32) -> Vec<u8> {
        let mut bytes = vec![b' '; 32];
        bytes.extend_from_slice(&payload_length.to_be_bytes());
        bytes
    }

    // Declared 4 GiB payload on a 36-byte input: without the pre-allocation
    // check this reads as UnexpectedEof AFTER allocating the declared size.
    #[test]
    fn oversized_payload_prefix_rejected_before_allocation() {
        let bytes = header(u32::MAX);
        let err = extract_payload(&bytes[..]).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("payload"), "{err}");

        let err = extract_payload_non_blocking(&bytes[..]).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn inner_message_length_bounded_by_enclosing_payload() {
        // payload of 10 bytes whose inner message_size claims 100.
        let mut bytes = header(10);
        bytes.extend_from_slice(&100u32.to_be_bytes());
        bytes.extend_from_slice(&[0; 6]);

        let err = extract_payload(&bytes[..]).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("message"), "{err}");

        let err = extract_payload_non_blocking(&bytes[..]).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn in_limit_frames_decode_unchanged() {
        let mut metadata = PayloadMetadata::new();
        metadata.insert("k".to_string(), "v".to_string());
        let bytes = build_from_binary("round_trip", b"hello", metadata.clone());

        let (id, message, meta) = extract_payload(&bytes[..]).unwrap();
        assert_eq!(
            (id.as_str(), message.as_slice()),
            ("round_trip", &b"hello"[..])
        );
        assert_eq!(meta, metadata);

        let (id, message, meta) = extract_payload_non_blocking(&bytes[..]).unwrap().unwrap();
        assert_eq!(
            (id.as_str(), message.as_slice()),
            ("round_trip", &b"hello"[..])
        );
        assert_eq!(meta, metadata);
    }

    // The reject must be stream-fatal through the existing reader error
    // handling: readers reconnect/exit on anything but Interrupted.
    #[test]
    fn oversized_reject_classifies_as_reconnect() {
        use crate::transport::liveness::{ControlLiveness, ReadAction};
        let err = reject_oversized("payload", usize::MAX, 1);
        assert!(matches!(
            ControlLiveness::new().classify_error(&err),
            ReadAction::Reconnect
        ));
    }
}

fn deserialize_metadata(metadata_bytes: &[u8]) -> io::Result<HashMap<String, String>> {
    let mut metadata = HashMap::new();
    let mut offset = 0;

    while offset < metadata_bytes.len() {
        if offset + 2 > metadata_bytes.len() {
            break; // Not enough bytes for key and value sizes
        }

        let key_size = metadata_bytes[offset] as usize;
        let value_size = metadata_bytes[offset + 1] as usize;
        offset += 2;

        if offset + key_size + value_size > metadata_bytes.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid metadata format: insufficient bytes for key/value",
            ));
        }

        let key = String::from_utf8_lossy(&metadata_bytes[offset..offset + key_size]).to_string();
        offset += key_size;

        let value =
            String::from_utf8_lossy(&metadata_bytes[offset..offset + value_size]).to_string();
        offset += value_size;

        metadata.insert(key, value);
    }

    Ok(metadata)
}
