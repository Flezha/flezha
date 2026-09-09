use std::collections::HashMap;
use std::io::{self, Read};
use std::str;

use byteorder::{BigEndian, ReadBytesExt};

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

/// Fixed width of a metadata key's and a metadata value's length prefix.
pub const MAX_METADATA_FIELD_LEN: usize = u8::MAX as usize;

/// Fixed width of the metadata table's length prefix.
pub const MAX_METADATA_TABLE_LEN: usize = u16::MAX as usize;

/// A length prefix exceeded the field that has to carry it.
///
/// On decode the length is one an untrusted peer declared, raised BEFORE
/// allocating for it; the stream cannot resync past it, so callers treat it as
/// stream-fatal. On encode it is a length this side cannot represent, refused
/// rather than written as a truncated cast.
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
    let Some(header_buf) = read_header_or_none(&mut reader)? else {
        return Ok(None);
    };

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

/// One non-blocking read of the frame header: the header, `None` when nothing
/// is readable yet, an error at end of stream or on a short read.
fn read_header_or_none<R: Read>(reader: &mut R) -> io::Result<Option<[u8; HEADER_LEN]>> {
    let mut header_buf = [0; HEADER_LEN];
    match reader.read(&mut header_buf) {
        Ok(0) => Err(io::Error::new(
            io::ErrorKind::BrokenPipe,
            "Connection closed",
        )),
        // A short header cannot happen on a UnixStream; refused as a precaution.
        Ok(n) if n < HEADER_LEN => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Incomplete header",
        )),
        Ok(_) => Ok(Some(header_buf)),
        Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => Ok(None),
        Err(e) => Err(e),
    }
}

/// Refused when a metadata key, value or table is longer than the length
/// prefix the wire format gives it.
pub fn build_from_binary(
    payload_id: &str,
    message: &[u8],
    metadata: PayloadMetadata,
) -> io::Result<Vec<u8>> {
    let mut out = Vec::new();
    append_payload(&mut out, payload_id, message, &metadata)?;
    Ok(out)
}

/// A frame carrying no metadata. An empty table fits every length prefix, so
/// there is nothing here to refuse and callers get bytes, not a result.
pub fn build_without_metadata(payload_id: &str, message: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    append_checked(&mut out, payload_id, message, &[], 0);
    out
}

/// Zero-alloc variant of `build_from_binary`: appends the framed payload
/// bytes to `out` in-place. Used by the hot path to batch many outputs
/// into a single write syscall.
///
/// On refusal `out` is left exactly as it was found, so a rejected frame
/// cannot half-append into a batch.
pub fn build_from_binary_into(
    out: &mut Vec<u8>,
    payload_id: &str,
    message: &[u8],
    metadata: &PayloadMetadata,
) -> io::Result<()> {
    append_payload(out, payload_id, message, metadata)
}

fn append_payload(
    out: &mut Vec<u8>,
    payload_id: &str,
    message: &[u8],
    metadata: &PayloadMetadata,
) -> io::Result<()> {
    // Serialize metadata directly into the metadata region after we write
    // the metadata_size. We can't do that cleanly because metadata_size
    // precedes the bytes, so serialize to a scratch Vec first.
    let metadata_binary = serialize_metadata(metadata)?;
    let metadata_size = check_metadata_table(&metadata_binary)?;
    append_checked(out, payload_id, message, &metadata_binary, metadata_size);
    Ok(())
}

/// Write the frame from a metadata table already proven to fit `metadata_size`.
/// The only place the frame layout is written down.
fn append_checked(
    out: &mut Vec<u8>,
    payload_id: &str,
    message: &[u8],
    metadata_binary: &[u8],
    metadata_size: u16,
) {
    let message_size = message.len() as u32;
    let payload_size = 4 + message.len() + 2 + metadata_binary.len();

    // Reserve in-place to avoid resizes during the appends below.
    out.reserve(HEADER_LEN + payload_size);

    // Write the payload ID (32 bytes, space-padded).
    let id_bytes = payload_id.as_bytes();
    if id_bytes.len() >= PAYLOAD_ID_LEN {
        out.extend_from_slice(&id_bytes[..PAYLOAD_ID_LEN]);
    } else {
        out.extend_from_slice(id_bytes);
        for _ in id_bytes.len()..PAYLOAD_ID_LEN {
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
    out.extend_from_slice(metadata_binary);
}

/// The serialized table has to fit its two-byte length prefix.
fn check_metadata_table(metadata_binary: &[u8]) -> io::Result<u16> {
    u16::try_from(metadata_binary.len()).map_err(|_| {
        reject_oversized(
            "metadata table",
            metadata_binary.len(),
            MAX_METADATA_TABLE_LEN,
        )
    })
}

/// Each key and each value has to fit its one-byte length prefix. Writing the
/// low bits instead would produce a frame that parses cleanly and carries
/// wrong pairs, which no decode-side limit can catch.
fn serialize_metadata(metadata: &HashMap<String, String>) -> io::Result<Vec<u8>> {
    let mut result = Vec::new();

    for (key, value) in metadata {
        let key_bytes = key.as_bytes();
        let value_bytes = value.as_bytes();

        let key_size = field_len(key_bytes, || format!("metadata key {key:?}"))?;
        let value_size = field_len(value_bytes, || format!("metadata value for key {key:?}"))?;

        result.push(key_size);
        result.push(value_size);
        result.extend_from_slice(key_bytes);
        result.extend_from_slice(value_bytes);
    }

    Ok(result)
}

/// One field's length prefix. `what` names the field and is called only on
/// refusal, so the emit path formats nothing for a field that fits.
fn field_len(field: &[u8], what: impl FnOnce() -> String) -> io::Result<u8> {
    u8::try_from(field.len())
        .map_err(|_| reject_oversized(&what(), field.len(), MAX_METADATA_FIELD_LEN))
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
        let bytes = build_from_binary("round_trip", b"hello", metadata.clone()).unwrap();

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

    fn oversized(len: usize) -> String {
        "v".repeat(len)
    }

    // A 256-byte value writes `256 as u8` == 0, so a reader takes no value and
    // resumes INSIDE it. Truncating produces a frame that parses cleanly and
    // carries pairs the sender never wrote, which no decode-side limit can
    // catch; the encoder refuses it instead.
    #[test]
    fn oversized_metadata_value_refused() {
        let mut metadata = PayloadMetadata::new();
        metadata.insert("flz.key".to_string(), oversized(MAX_METADATA_FIELD_LEN + 1));

        let err = build_from_binary("emit", b"body", metadata.clone()).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("flz.key"), "{err}");
        assert!(err.to_string().contains("256"), "{err}");

        // ... and no bytes reach the caller's buffer.
        let mut out = vec![0xAA];
        assert!(build_from_binary_into(&mut out, "emit", b"body", &metadata).is_err());
        assert_eq!(out, vec![0xAA]);
    }

    #[test]
    fn oversized_metadata_key_refused() {
        let mut metadata = PayloadMetadata::new();
        metadata.insert(oversized(MAX_METADATA_FIELD_LEN + 1), "v".to_string());

        let err = build_from_binary("emit", b"body", metadata).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("metadata key"), "{err}");
        assert!(err.to_string().contains("256"), "{err}");
    }

    #[test]
    fn oversized_metadata_table_refused() {
        // Every pair is within its one-byte prefixes; the table is not.
        let mut metadata = PayloadMetadata::new();
        for i in 0..512 {
            metadata.insert(format!("key{i:03}"), oversized(MAX_METADATA_FIELD_LEN));
        }

        let err = build_from_binary("emit", b"body", metadata).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("metadata table"), "{err}");
    }

    // The boundary is inclusive on both fields: the largest representable
    // key and value still round-trip.
    #[test]
    fn metadata_at_the_limit_round_trips() {
        let mut metadata = PayloadMetadata::new();
        metadata.insert(
            oversized(MAX_METADATA_FIELD_LEN),
            oversized(MAX_METADATA_FIELD_LEN),
        );
        let bytes = build_from_binary("emit", b"body", metadata.clone()).unwrap();

        let (_, _, decoded) = extract_payload(&bytes[..]).unwrap();
        assert_eq!(decoded, metadata);
    }

    // The no-metadata builder has nothing to refuse, and writes the same
    // frame the fallible one writes for an empty table.
    #[test]
    fn build_without_metadata_matches_the_empty_table() {
        assert_eq!(
            build_without_metadata("probe", b"body"),
            build_from_binary("probe", b"body", PayloadMetadata::new()).unwrap()
        );
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
