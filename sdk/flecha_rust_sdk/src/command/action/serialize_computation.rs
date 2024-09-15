use std::collections::HashMap;
use std::io::Write;

use super::CommandAction;

use serde::Deserialize;
use serde_json::Value;

use crate::service::payloader;
use crate::engine::worker::EngineWorker;

#[derive(Deserialize)]
pub struct SerializeComputationCommand {
    computation_id: String
}

impl CommandAction for SerializeComputationCommand {
    fn handle(
        &self,
        engine_worker: &mut EngineWorker,
        _message: &String,
        reference_id: String,
    ) {
        println!("Serializing computation with id: {}", self.computation_id);

        let control_stream = engine_worker.get_control_stream();
        let computations_lock = engine_worker.get_computations();
        let computations = computations_lock.read().unwrap();  // Acquire read lock
        let serialized_json = match computations.get(&self.computation_id) {
            Some(computation) => {
                let serialized_data = computation.lock().unwrap().template.serialize_template();
                let data_as_values = serialized_data.iter()
                    .map(|&byte| Value::Number(serde_json::Number::from(byte)))
                    .collect();

                Value::Array(data_as_values)
            }
            None => Value::Null,
        };

        let mut hashmap: HashMap<String, Value> = HashMap::new();
        hashmap.insert("reference_id".to_string(), Value::String(reference_id));
        hashmap.insert("data".to_string(), serialized_json);

        let json_string = serde_json::to_string(&hashmap).unwrap();
        let payload = payloader::build_payload(&"42".to_string(), &json_string);
        control_stream.try_clone().unwrap().write_all(&payload).unwrap();
    }
}