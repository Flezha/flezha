use serde::Deserialize;
use serde_json::Value;
use std::os::unix::net::UnixStream;
use std::io::Write;
use std::sync::{RwLock, Arc, Mutex};
use std::collections::HashMap;
use std::thread;
use std::str;

use crate::computation::{self, ComputationType};
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
        engine: &Arc<Mutex<ComputationEngine>>,
        _message: &String,
        reference_id: String,
    ) {
        let engine_guard = engine.lock().unwrap();
        let computations = engine_guard.get_computations();
        let control_stream = engine_guard.get_control_stream();
        let operator_id = engine_guard.get_engine_id();
        let io_streams = engine_guard.get_io_streams();
        let reference_id = reference_id.clone();

        drop(engine_guard);

        for key in 0..self.sockets_count {
            let computations_clone = Arc::clone(&computations);
            let stream = connect_unix(&operator_id, "primary".to_string());
            let shared_stream = connect_unix(&operator_id, "secondary".to_string());

            io_streams.write().unwrap().insert(key, shared_stream.try_clone().unwrap());

            let _handle = thread::spawn(move || {
                listen_incoming(stream, &computations_clone)
            });
        }

        let mut hashmap: HashMap<String, Value> = HashMap::new();
        hashmap.insert("reference_id".to_string(), Value::String(reference_id));

        let json_string = serde_json::to_string(&hashmap).unwrap();
        let payload = payloader::build_from_str(&"connect_sockets".to_string(), &json_string);
        control_stream.try_clone().unwrap().write_all(&payload).unwrap();
    }
}

fn connect_unix(operator_id: &String, stream_type: String) -> UnixStream {
    let socket_path = path_helper::stream_socket_path(operator_id, stream_type);
    UnixStream::connect(socket_path).expect("Failed to connect to the socket")
}

fn listen_incoming(mut stream: UnixStream, computations: &Arc<RwLock<HashMap<String, Arc<Mutex<ComputationType>>>>>) {
    loop {
        match payloader::extract_payload(&mut stream) {
            Ok((computation_id, payload)) => {
                computation::async_exec(computation_id, computations, &mut stream, payload);
            }
            Err(_e) => {
                // TODO: log error
                break;
            }
        }
    }
}