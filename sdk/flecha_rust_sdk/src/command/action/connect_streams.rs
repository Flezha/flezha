use serde::Deserialize;
use serde_json::Value;
use std::os::unix::net::UnixStream;
use std::io::Write;
use std::sync::{RwLock, Arc, Mutex};
use std::collections::HashMap;
use std::thread;
use std::str;

use crate::computation::{self, ComputationResult, ComputationType};
use crate::engine::ComputationEngine;
use crate::service::payloader;

use super::CommandAction;

use crate::utility::path_helper;

#[derive(Deserialize)]
pub struct ConnectSocketCommand {
    sockets_count: u8
}

impl CommandAction for ConnectSocketCommand {
    fn handle(
        &self,
        engine: &mut ComputationEngine,
        _message: &String,
        reference_id: String,
    ) {
        let computations = engine.get_computations();
        let reference_id = reference_id.clone();
        let control_stream = engine.get_control_stream();

        for i in 0..self.sockets_count {
            let computations = Arc::clone(&computations);
            let engine_id = engine.get_engine_id();
        
            let handle = thread::spawn(move || {
                start_socket_listener(&engine_id, &computations);
            });
        
            engine.get_stream_threads().lock().unwrap().insert(i, handle);
        }

        let mut hashmap: HashMap<String, Value> = HashMap::new();
        hashmap.insert("reference_id".to_string(), Value::String(reference_id));

        let json_string = serde_json::to_string(&hashmap).unwrap();
        let payload = payloader::build_payload(&"connect_sockets".to_string(), &json_string);
        control_stream.try_clone().unwrap().write_all(&payload).unwrap();
    }
}

fn start_socket_listener(
    engine_id: &String,
    computations: &Arc<RwLock<HashMap<String, Arc<Mutex<ComputationType>>>>>,
) {
    let socket_path = path_helper::stream_socket_path(engine_id);
    let stream = UnixStream::connect(socket_path).expect("Failed to connect to the socket");

    handle_client(stream, computations)
}

fn handle_client(mut stream: UnixStream, computations: &Arc<RwLock<HashMap<String, Arc<Mutex<ComputationType>>>>>) {
    loop {
        match payloader::extract_payload(&mut stream) {
            Ok((computation_id, payload)) => {
                let computations_read = computations.read().unwrap();
                
                if let Some(c) = computations_read.get(&computation_id) {
                    let mut comp = c.lock().unwrap();

                    // TODO: reduce duplication.
                    match &mut *comp {
                        ComputationType::TextComputation(comp) => {
                            let payload_message: String = String::from_utf8_lossy(&payload).to_string();
                            let result = comp.run(payload_message);

                            match result {
                                ComputationResult::NoReply => {}
                                ComputationResult::Reply(reply) => {
                                    computation::send_result(&mut stream, &computation_id, &reply);
                                }
                            }
                        }
                        ComputationType::BinaryComputation(comp) => {
                            let result = comp.run(payload);

                            match result {
                                ComputationResult::NoReply => {}
                                ComputationResult::Reply(reply) => {
                                    computation::send_result(&mut stream, &computation_id, &reply);
                                }
                            }
                        }
                    };              
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