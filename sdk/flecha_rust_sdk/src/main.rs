mod computation;
mod engine;
mod service;
mod command;

use engine::ComputationEngine;
use computation::{ComputationTemplate, ComputationResult};

fn main() {
    let mut engine = ComputationEngine::new();

    engine.register_template("do_nothing".to_string(), || Box::new(DoNothingComputation{}));
    engine.register_template("compare_filter".to_string(), || Box::new(CompareFilterComputation{}));


    // TODO: remove later.
    std::thread::sleep(std::time::Duration::from_secs(5));
}


struct CompareFilterComputation {}

impl ComputationTemplate for CompareFilterComputation {
    fn compute(&self, input: String) -> ComputationResult {
        ComputationResult::Reply(input)
    }

    fn get_schema(&self) -> std::collections::HashMap<String, String> {
        std::collections::HashMap::new()
    }
}

struct DoNothingComputation {}

impl ComputationTemplate for DoNothingComputation {
    fn compute(&self, _input: String) -> ComputationResult {
        ComputationResult::NoReply
    }

    fn get_schema(&self) -> std::collections::HashMap<String, String> {
        let mut schema = std::collections::HashMap::new();
        schema.insert("input".to_string(), "string".to_string());
        schema
    }
}