use super::CommandAction;

use std::collections::HashMap;
use std::io::Write;
use std::sync::{Arc, Mutex};
use serde_json::Value;
use serde::Deserialize;

use crate::computation::ComputationType;
use crate::operator::ComputationOperator;
use crate::service::payloader;

#[derive(Deserialize, Debug)]
pub struct AddComputationCommand {
    computation_id: String,
    factory_id: String,
    computation_type: String,
    variables: Value,
}

impl CommandAction for AddComputationCommand {
    fn handle(
        &self,
        operator: &Arc<Mutex<ComputationOperator>>,
        _message: &String,
        reference_id: String,
    ) {
        let operator_guard = operator.lock().unwrap();
        let factory = operator_guard.get_factory(self.factory_id.clone()).unwrap();
        let variables = value_to_hashmap(&self.variables).unwrap();
        let mut metadata: HashMap<String, String> = HashMap::new();
        let mut computation_enum = factory();

        metadata.insert("computation_type".to_string(), self.computation_type.clone());
        
        match &mut computation_enum { 
            ComputationType::TextComputation(c) => c.update_computation(&variables),
            ComputationType::BinaryComputation(c) => c.update_computation(&variables),
        };
                
        operator_guard.add_computation(self.computation_id.clone(), computation_enum, metadata);        
        
        let mut hashmap: HashMap<String, Value> = HashMap::new();
        hashmap.insert("reference_id".to_string(), Value::String(reference_id));

        let json_string = serde_json::to_string(&hashmap).unwrap();
        let payload = payloader::build_from_str(&"add_computation".to_string(), &json_string);

        operator_guard.get_control_stream().try_clone().unwrap().write_all(&payload).unwrap();
    }
}

fn value_to_hashmap(value: &Value) -> Option<HashMap<String, Value>> {
    value.as_object().map(|map| {
        map.iter()
           .map(|(k, v)| (k.clone(), v.clone()))
           .collect()
    })
}