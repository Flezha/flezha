use std::io::{self, Write};

pub fn write_bytes(payload: Vec<u8>) {
    let stdout = io::stdout();
    let mut handle = stdout.lock();
    handle.write_all(&payload).unwrap();
    handle.flush().unwrap();
}