use std::io::{self, Write};

pub fn write_w_code(code: u8, payload: &str) {
    let code_str = code.to_string();
    let message = &format!("CODE:{}{}", code_str, payload);
    write(message);
}

pub fn reply_ref(ref_id: &str) {
    let message = &format!("COMMAND_REF:{}", ref_id);
    write(message);
}

fn write(message: &str) {
    let stdout = io::stdout();
    let mut handle = stdout.lock();
    handle.write_all(message.as_bytes()).unwrap();
    handle.flush().unwrap();
}