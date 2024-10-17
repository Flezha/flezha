use super::CommandAction;

use std::collections::HashMap;
use std::io::Write;
use std::sync::{Arc, Mutex};
use serde_json::Value;
use serde::Deserialize;

use crate::engine::ComputationEngine;
use crate::service::payloader;

#[derive(Deserialize, Debug)]
pub struct RemComputationCommand {
    computation_id: String
}

impl CommandAction for RemComputationCommand {
    fn handle(
        &self,
        engine: &Arc<Mutex<ComputationEngine>>,
        _message: &String,
        reference_id: String,
    ) {                
        let engine_guard = engine.lock().unwrap();
        engine_guard.rem_computation(self.computation_id.clone());        
        
        let mut hashmap: HashMap<String, Value> = HashMap::new();
        hashmap.insert("reference_id".to_string(), Value::String(reference_id));

        let json_string = serde_json::to_string(&hashmap).unwrap();
        let payload = payloader::build_from_str(&"rem_computation".to_string(), &json_string);

        engine_guard.get_control_stream().try_clone().unwrap().write_all(&payload).unwrap();
    }
}
