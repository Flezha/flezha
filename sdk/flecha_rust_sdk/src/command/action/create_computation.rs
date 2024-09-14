use super::CommandAction;

use std::collections::HashMap;
use std::io::Write;
use serde_json::Value;
use serde::Deserialize;

use crate::service::payloader;
use crate::computation::Computation;
use crate::engine::worker::EngineWorker;

#[derive(Deserialize)]
pub struct CreateComputationCommand {
    computation_id: String,
    template_id: String,
    variables: Value,
}

impl CommandAction for CreateComputationCommand {
    fn handle(
        &self,
        engine_worker: &mut EngineWorker,
        _message: &String,
        reference_id: String,
    ) {
        let template_factory = engine_worker.get_template_factory(self.template_id.clone()).unwrap();
        let variables = value_to_hashmap(&self.variables).unwrap();
        
        let mut template = template_factory();
        template.update_template(&variables);

        let computation = Computation {
            id: self.computation_id.clone(),
            template,
        };
    
        engine_worker.register_computation(computation);

        let mut hashmap: HashMap<String, Value> = HashMap::new();
        hashmap.insert("reference_id".to_string(), Value::String(reference_id));

        let json_string = serde_json::to_string(&hashmap).unwrap();
        let payload = payloader::build_payload(&"41".to_string(), &json_string);

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