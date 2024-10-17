use super::CommandAction;

use std::collections::HashMap;
use std::io::Write;
use std::sync::{Arc, Mutex};
use serde_json::Value;
use serde::Deserialize;

use crate::computation::ComputationType;
use crate::engine::ComputationEngine;
use crate::service::payloader;

#[derive(Deserialize, Debug)]
pub struct AddComputationCommand {
    computation_id: String,
    factory_id: String,
    variables: Value,
}

impl CommandAction for AddComputationCommand {
    fn handle(
        &self,
        engine: &Arc<Mutex<ComputationEngine>>,
        _message: &String,
        reference_id: String,
    ) {
        let engine_guard = engine.lock().unwrap();
        let factory = engine_guard.get_factory(self.factory_id.clone()).unwrap();
        let variables = value_to_hashmap(&self.variables).unwrap();
        
        let mut computation_enum = factory(); // Mutable to allow mutable borrowing
        
        match &mut computation_enum {  // Mutably borrow the computation_enum
            ComputationType::TextComputation(c) => c.update_computation(&variables),
            ComputationType::BinaryComputation(c) => c.update_computation(&variables),
        };
                
        engine_guard.add_computation(self.computation_id.clone(), computation_enum);        
        
        let mut hashmap: HashMap<String, Value> = HashMap::new();
        hashmap.insert("reference_id".to_string(), Value::String(reference_id));

        let json_string = serde_json::to_string(&hashmap).unwrap();
        let payload = payloader::build_from_str(&"add_computation".to_string(), &json_string);

        engine_guard.get_control_stream().try_clone().unwrap().write_all(&payload).unwrap();
    }
}

fn value_to_hashmap(value: &Value) -> Option<HashMap<String, Value>> {
    value.as_object().map(|map| {
        map.iter()
           .map(|(k, v)| (k.clone(), v.clone()))
           .collect()
    })
}