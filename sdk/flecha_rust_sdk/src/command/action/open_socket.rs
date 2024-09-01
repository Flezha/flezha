use serde::Deserialize;
use std::path::Path;
use std::os::unix::net::{UnixListener, UnixStream};
use std::io::Write;
use std::sync::{RwLock, Arc, Mutex};
use std::collections::HashMap;
use std::thread;
use std::str;

use crate::engine::worker::EngineWorker;
use crate::computation::{Computation, ComputationResult};
use crate::service::std_out;
use crate::service::payloader;

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
    
    std_out::reply_ref(&reference_id);

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
        match payloader::read_message(&mut stream) {
            Ok((computation_id, message)) => {
                let computations_read = computations.read().unwrap();
                
                if let Some(c) = computations_read.get(&computation_id) {
                    let computation = c.lock().unwrap();
                    let result = computation.template.compute(message);

                    match result {
                        ComputationResult::NoReply => {}
                        ComputationResult::Reply(reply) => {
                            send_result(&mut stream, &computation_id, &reply);
                        }
                    }
                } else {
                    eprintln!("Computation with given ID not found {}", computation_id);
                }
            }
            Err(e) => {
                eprintln!("Failed to read message: {}", e);
                break;
            }
        }
    }
}

fn send_result(stream: &mut UnixStream, computation_id: &String, result: &str) {
    let payload = payloader::build_payload(&computation_id, result);

    if let Err(err) = stream.write_all(&payload) {
        eprintln!("Error writing to stream: {}", err);
    }
}
