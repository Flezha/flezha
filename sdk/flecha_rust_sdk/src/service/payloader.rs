use std::str;
use std::io::{self, Read, Write};
use byteorder::{BigEndian, WriteBytesExt, ReadBytesExt};

pub fn extract_payload<R: Read>(mut reader: R) -> io::Result<(String, String)> {
    // Read the length prefix (8 bytes).
    let mut length_buf = [0; 8];
    reader.read_exact(&mut length_buf)?;
    let message_length = (&length_buf[..]).read_u64::<BigEndian>()? as usize;

    // Read the payload ID (32 bytes).
    let mut id_buf = [0; 32];
    reader.read_exact(&mut id_buf)?;
    let payload_id = String::from_utf8_lossy(&id_buf).trim_end().to_string();

    // Read the actual message.
    let mut message_buf = vec![0; message_length];
    reader.read_exact(&mut message_buf)?;
    let payload_message = String::from_utf8_lossy(&message_buf).to_string();

    Ok((payload_id, payload_message))
}

pub fn build_payload(payload_id: &str, message: &str) -> Vec<u8> {
    // Ensure the payload ID is 32 bytes.
    let mut payload_id_padded = payload_id.to_string();
    if payload_id_padded.len() > 32 {
        payload_id_padded.truncate(32);
    } else {
        payload_id_padded.push_str(&" ".repeat(32 - payload_id_padded.len()));
    }

    // Calculate the message size (8 bytes integer).
    let message_size = message.len() as u64;

    // Create a buffer to hold the payload.
    let mut payload = Vec::with_capacity(8 + 32 + message.len());

    // Write the message size (8 bytes) to the payload.
    let _ = payload.write_u64::<BigEndian>(message_size);

    // Write the payload ID (32 bytes) to the payload.
    payload.write_all(payload_id_padded.as_bytes()).unwrap();

    // Write the message to the payload.
    payload.write_all(message.as_bytes()).unwrap();

    payload
}
