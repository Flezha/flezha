use std::io::{self, Write};

pub fn write_stdout(code: u8, payload: &str) {
    let code_str = code.to_string();
    let message = &format!("CODE:{}{}", code_str, payload);
    let stdout = io::stdout();
    let mut handle = stdout.lock();
    handle.write_all(message.as_bytes()).unwrap();
    handle.flush().unwrap();
}