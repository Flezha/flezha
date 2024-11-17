use serde::Deserialize;
use serde_json::Value;
use std::os::unix::net::UnixStream;
use std::io::Write;
use std::sync::{RwLock, Arc, Mutex};
use std::collections::HashMap;
use std::thread;
use std::str;

use crate::computation::{self, ComputationType};
use crate::operator::ComputationOperator;
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
        operator: &Arc<Mutex<ComputationOperator>>,
        _message: &String,
        reference_id: String,
    ) {
        let operator_guard = operator.lock().unwrap();
        let computations = operator_guard.get_computations();
        let control_stream = operator_guard.get_control_stream();
        let operator_id = operator_guard.get_operator_id();
        let io_streams = operator_guard.get_io_streams();
        let reference_id = reference_id.clone();

        drop(operator_guard);

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

fn listen_incoming(mut stream: UnixStream, computations: &Arc<RwLock<HashMap<String, Arc<Mutex<(ComputationType, HashMap<String, String>)>>>>>) {
    loop {
        match payloader::extract_payload(&mut stream) {
            Ok((computation_id, payload)) => {
                computation::exec(
                    &computation_id,
                    computations,
                    &mut stream, 
                    &payload
                );
            }
            Err(_e) => {
                // TODO: log error
                break;
            }
        }
    }
}