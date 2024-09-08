use serde::Deserialize;
use std::os::unix::net::UnixStream;
use std::io::Write;
use std::sync::{RwLock, Arc, Mutex};
use std::collections::HashMap;
use std::thread;
use std::str;

use crate::engine::worker::EngineWorker;
use crate::computation::{Computation, ComputationResult};
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
        let command_stream = engine_worker.get_command_stream();

        let handle = thread::spawn(move || {
            start_socket_listener(
                socket_id, 
                computations, 
                reference_id,
                command_stream
            );
        });

        engine_worker.get_thread_map().lock().unwrap().insert(data_value, handle);
    }
}

fn start_socket_listener(
    id: String,
    computations: Arc<RwLock<HashMap<String, Arc<Mutex<Computation>>>>>, 
    reference_id: String,
    command_stream: Arc<UnixStream>
) {
    let socket_path = format!("app_data/sockets/{}.sock", id);
    let stream = UnixStream::connect(socket_path).expect("Failed to connect to the socket");
    let payload = payloader::build_payload(&"30".to_string(), &reference_id);

    command_stream.try_clone().unwrap().write_all(&payload).unwrap();

    handle_client(stream, computations)
}

fn handle_client(mut stream: UnixStream, computations: Arc<RwLock<HashMap<String, Arc<Mutex<Computation>>>>>) {
    loop {
        match payloader::extract_payload(&mut stream) {
            Ok((computation_id, message)) => {
                let computations_read = computations.read().unwrap();
                
                if let Some(c) = computations_read.get(&computation_id) {
                    let computation = c.lock().unwrap();
                    let result = computation.template.run(message);

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
            Err(_e) => {
                // TODO: log error
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
