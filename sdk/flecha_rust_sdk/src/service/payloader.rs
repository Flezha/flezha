use std::str;
use std::io::{Read, Write};
use byteorder::{BigEndian, WriteBytesExt, ReadBytesExt};
use std::os::unix::net::UnixStream;

pub fn read_message(stream: &mut UnixStream) -> std::io::Result<(String, String)> {
    // Read the length prefix (8 bytes).
    let mut length_buf = [0; 8];
    stream.read_exact(&mut length_buf)?;
    let message_length = (&length_buf[..]).read_u64::<BigEndian>()? as usize;

    // Read the computation ID (32 bytes).
    let mut id_buf = [0; 32];
    stream.read_exact(&mut id_buf)?;
    let computation_id = String::from_utf8_lossy(&id_buf).trim_end().to_string();

    // Read the actual message.
    let mut message_buf = vec![0; message_length];
    stream.read_exact(&mut message_buf)?;
    let payload_message = String::from_utf8_lossy(&message_buf).to_string();

    Ok((computation_id, payload_message))
}


pub fn build_payload(computation_id: &str, message: &str) -> Vec<u8> {
    // Ensure the computation ID is 32 bytes.
    let mut computation_id_padded = computation_id.to_string();
    if computation_id_padded.len() > 32 {
        computation_id_padded.truncate(32);
    } else {
        computation_id_padded.push_str(&" ".repeat(32 - computation_id_padded.len()));
    }

    // Calculate the message size (8 bytes integer).
    let message_size = message.len() as u64;

    // Create a buffer to hold the payload.
    let mut payload = Vec::with_capacity(8 + 32 + message.len());

    // Write the message size (8 bytes) to the payload.
    let _ = payload.write_u64::<BigEndian>(message_size);

    // Write the computation ID (32 bytes) to the payload.
    payload.write_all(computation_id_padded.as_bytes()).unwrap();

    // Write the message to the payload.
    payload.write_all(message.as_bytes()).unwrap();

    payload
}

// use std::io::{Cursor, Read, Write};
// pub fn parse_payload(payload: &[u8]) -> (String, String) {
//     let mut cursor = Cursor::new(payload);

//     // Read the first 8 bytes for the message size
//     let message_size = cursor.read_u64::<BigEndian>().unwrap();

//     // Read the next 32 bytes for the computation_id
//     let mut computation_id_bytes = vec![0u8; 32];
//     cursor.read_exact(&mut computation_id_bytes).unwrap();
//     let computation_id = str::from_utf8(&computation_id_bytes).unwrap().trim_end().to_string();

//     // Read the message based on the message size
//     let mut message_bytes = vec![0u8; message_size as usize];
//     cursor.read_exact(&mut message_bytes).unwrap();
//     let message = str::from_utf8(&message_bytes).unwrap().to_string();

//     (computation_id, message)
// }