use std::collections::HashMap;
use std::io::Write;
use std::sync::{Arc, Mutex};

use super::CommandAction;

use serde::Deserialize;
use serde_json::Value;

use crate::computation::ComputationType;
use crate::engine::ComputationEngine;
use crate::service::payloader;

#[derive(Deserialize)]
pub struct DeSerializeComputationCommand {
    computation_id: String,
    serialized_data: Vec<u8>
}

impl CommandAction for DeSerializeComputationCommand {
    fn handle(
        &self,
        engine: &Arc<Mutex<ComputationEngine>>,
        _message: &String,
        reference_id: String,
    ) {
        let engine_guard = engine.lock().unwrap();
        let control_stream = engine_guard.get_control_stream();
        let computations_lock = engine_guard.get_computations();
        let computations = computations_lock.read().unwrap();
        
        drop(engine_guard);

        match computations.get(&self.computation_id) {
            Some(comp_arc) => {
                let c_enum = comp_arc.lock().unwrap();

                match &*c_enum {
                    ComputationType::TextComputation(computation)  => {
                        computation.deserialize_computation(&self.serialized_data);
                    }
                    ComputationType::BinaryComputation(computation) => {
                        computation.deserialize_computation(&self.serialized_data);
                    }
                }
            }
            None => (),
        };

        // TODO: the same code is all over the place, refactor it.
        let mut hashmap: HashMap<String, Value> = HashMap::new();
        hashmap.insert("reference_id".to_string(), Value::String(reference_id));

        let json_string = serde_json::to_string(&hashmap).unwrap();
        let payload = payloader::build_payload_str(&"deserialize_computation".to_string(), &json_string);
        control_stream.try_clone().unwrap().write_all(&payload).unwrap();
    }
}