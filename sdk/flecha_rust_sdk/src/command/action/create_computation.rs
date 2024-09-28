use super::CommandAction;

use std::collections::HashMap;
use std::io::Write;
use serde_json::Value;
use serde::Deserialize;

use crate::service::payloader;
use crate::engine::worker::EngineWorker;

#[derive(Deserialize)]
pub struct CreateComputationCommand {
    computation_id: String,
    factory_id: String,
    variables: Value,
}

impl CommandAction for CreateComputationCommand {
    fn handle(
        &self,
        engine_worker: &mut EngineWorker,
        _message: &String,
        reference_id: String,
    ) {
        let factory = engine_worker.get_factory(self.factory_id.clone()).unwrap();
        let variables = value_to_hashmap(&self.variables).unwrap();
        
        let mut computation = factory();
        computation.update_computation(&variables);
    
        engine_worker.register_computation(self.computation_id.clone(), computation);

        let mut hashmap: HashMap<String, Value> = HashMap::new();
        hashmap.insert("reference_id".to_string(), Value::String(reference_id));

        let json_string = serde_json::to_string(&hashmap).unwrap();
        let payload = payloader::build_payload(&"add_computation".to_string(), &json_string);

        engine_worker.get_control_stream().try_clone().unwrap().write_all(&payload).unwrap();
    }
}

fn value_to_hashmap(value: &Value) -> Option<HashMap<String, Value>> {
    value.as_object().map(|map| {
        map.iter()
           .map(|(k, v)| (k.clone(), v.clone()))
           .collect()
    })
}