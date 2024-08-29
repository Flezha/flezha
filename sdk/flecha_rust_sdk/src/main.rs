mod computation;
mod engine;
mod service;
mod command;

use engine::ComputationEngine;
use computation::ComputationTemplate;

fn main() {
    let mut engine = ComputationEngine::new();

    engine.register_template("do_nothing".to_string(), || Box::new(DoNothingComputation{}));

    // TODO: remove later.
    std::thread::sleep(std::time::Duration::from_secs(5));
}

struct DoNothingComputation {}

impl ComputationTemplate for DoNothingComputation {
    fn compute(&self, input: String) {
        println!("Do nothing with input: {}", input);
    }

    fn get_schema(&self) -> std::collections::HashMap<String, String> {
        let mut schema = std::collections::HashMap::new();
        schema.insert("input".to_string(), "string".to_string());
        schema
    }
}