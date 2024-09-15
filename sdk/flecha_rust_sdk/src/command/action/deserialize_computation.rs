use std::collections::HashMap;
use std::io::Write;

use super::CommandAction;

use serde::Deserialize;
use serde_json::Value;

use crate::service::payloader;
use crate::engine::worker::EngineWorker;

#[derive(Deserialize)]
pub struct DeSerializeComputationCommand {
    computation_id: String,
    serialized_data: Vec<u8>
}

impl CommandAction for DeSerializeComputationCommand {
    fn handle(
        &self,
        engine_worker: &mut EngineWorker,
        _message: &String,
        reference_id: String,
    ) {
        let control_stream = engine_worker.get_control_stream();
        let computations_lock = engine_worker.get_computations();
        let computations = computations_lock.read().unwrap();
        
        match computations.get(&self.computation_id) {
            Some(computation) => {
                computation.lock().unwrap().template.deserialize_template(&self.serialized_data);
            }
            None => (),
        };

        // TODO: the same code is all over the place, refactor it.
        let mut hashmap: HashMap<String, Value> = HashMap::new();
        hashmap.insert("reference_id".to_string(), Value::String(reference_id));

        let json_string = serde_json::to_string(&hashmap).unwrap();
        let payload = payloader::build_payload(&"43".to_string(), &json_string);
        control_stream.try_clone().unwrap().write_all(&payload).unwrap();
    }
}