use serde::Deserialize;
use std::path::Path;
use std::os::unix::net::{UnixListener, UnixStream};
use std::io::Read;
use std::sync::{RwLock, Arc, Mutex};
use std::collections::HashMap;
use std::thread;
use std::str;
use byteorder::{BigEndian, ReadBytesExt};

use crate::engine::worker::EngineWorker;
use crate::computation::Computation;
use crate::service::std_out;

use super::CommandAction;

#[derive(Deserialize)]
pub struct OpenSocketCommand {
    socket_id: String,
}

impl CommandAction for OpenSocketCommand {
    fn handle(
        &self,
        engine_worker: &mut EngineWorker,
        _message: &String,
        reference_id: String,
    ) {
        let data_value = self.socket_id.clone();
        let socket_id = data_value.clone();
        let computations = engine_worker.get_computations();
        let reference_id = reference_id.clone();

        let handle = thread::spawn(move || {
            start_socket_listener(
                socket_id, 
                computations, 
                reference_id
            );
        });

        engine_worker.get_thread_map().lock().unwrap().insert(data_value, handle);
    }
}

fn start_socket_listener(id: String, computations: Arc<RwLock<HashMap<String, Arc<Mutex<Computation>>>>>, reference_id: String) {
    let socket_path = format!("app_data/sockets/{}.sock", id);

    let path = Path::new(&socket_path);
    if path.exists() {
        std::fs::remove_file(path).expect("Failed to remove existing socket file");
    }

    let listener = UnixListener::bind(socket_path).expect("Failed to bind socket");
    
    std_out::write_stdout(19, &format!("COMMAND_REF:{}", reference_id));

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let computations_clone = Arc::clone(&computations);
                thread::spawn(move || handle_client(stream, computations_clone));
            }
            Err(e) => eprintln!("Failed to accept connection: {}", e),
        }
    }
}

fn handle_client(mut stream: UnixStream, computations: Arc<RwLock<HashMap<String, Arc<Mutex<Computation>>>>>) {
    loop {
        match read_message(&mut stream) {
            Ok(message) => {
                if let Some((computation_id, text)) = parse_message(&message) {
                    // Acquire the read lock and hold it in a variable
                    let computations_read = computations.read().unwrap();
                    
                    if let Some(c) = computations_read.get(&computation_id.to_string()) {
                        // TODO: I think computation does not need to be under mutex.
                        let computation = c.lock().unwrap();
                        
                        computation.template.compute(text.to_string());
                    } else {
                        eprintln!("Computation with given ID not found");
                    }
                }
            }
            Err(e) => {
                eprintln!("Failed to read message: {}", e);
                break;
            }
        }
    }
}

fn read_message(stream: &mut UnixStream) -> std::io::Result<Vec<u8>> {
    // Read the length prefix
    let mut length_buf = [0; 4];
    stream.read_exact(&mut length_buf)?;

    let message_length = (&length_buf[..]).read_u32::<BigEndian>()? as usize;

    // Read the actual message
    let mut message_buf = vec![0; message_length];
    stream.read_exact(&mut message_buf)?;

    Ok(message_buf)
}

fn parse_message(buffer: &[u8]) -> Option<(&str, &str)> {
    let message = std::str::from_utf8(buffer).ok()?;
    let parts: Vec<&str> = message.splitn(2, '\0').collect();

    if parts.len() == 2 {      
        Some((parts[0], parts[1]))
    } else {
        None
    }
}

// TODO: to send message back add a function to the computation.
// use std::sync::mpsc::Sender;
// use std::os::unix::net::{UnixListener, UnixStream};
// use std::io::{Read, Write};
// use std::sync::{RwLock, Arc, Mutex};
// use std::collections::HashMap;
// use std::thread;
// use std::str;
// use std::io::Cursor;
// use byteorder::{LittleEndian, BigEndian, ReadBytesExt, WriteBytesExt};

// // Prepare message in the format: 
// let mut buffer = Cursor::new(Vec::new());

// // Convert pipeline_id to bytes and write length and bytes
// let computation_str = computation_id.to_string();
// let computation_id_bytes = computation_str.as_bytes().to_vec();
// buffer.write_u32::<LittleEndian>(computation_id_bytes.len() as u32).unwrap();
// buffer.write_all(&computation_id_bytes).unwrap();

// // Convert result to bytes and write length and bytes
// let result_bytes = result.as_bytes();
// buffer.write_u32::<LittleEndian>(result_bytes.len() as u32).unwrap();
// buffer.write_all(result_bytes).unwrap();

// let message = buffer.into_inner();

// if let Err(err) = stream.write_all(&message) {
//     eprintln!("Error writing to stream: {}", err);
//     break;
// }