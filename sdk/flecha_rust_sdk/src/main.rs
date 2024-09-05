mod computation;
mod engine;
mod service;
mod command;

use engine::ComputationEngine;
use computation::{Template, TemplateManager, ComputationResult};
use serde::{Serialize, Deserialize};

fn main() {
    let mut engine = ComputationEngine::new();

    engine.register_template(
        "do_nothing".to_string(),
        || Box::new(TemplateManager{
            template: DoNothingComputation{}
        })
    );

    engine.register_template(
        "compare_filter".to_string(),
         || Box::new(TemplateManager{
            template: CompareFilterComputation{ string: "hellos".to_string() }
         })
    );

    // TODO: remove later.
    std::thread::sleep(std::time::Duration::from_secs(5));
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct CompareFilterComputation {
    string: String,
}

impl Template for CompareFilterComputation {
    fn run(&self, input: String) -> ComputationResult {
        // TODO: only for testing: println!("Compare data: {}", &self.string);
        ComputationResult::Reply(input)
    }

    fn get_schema(&self) -> std::collections::HashMap<String, String> {
        let mut schema = std::collections::HashMap::new();
        // TODO: Sending empty schema will throw an error on elixir side.
        schema.insert("input".to_string(), "string".to_string());
        schema
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct DoNothingComputation {}

impl Template for DoNothingComputation {
    fn run(&self, _input: String) -> ComputationResult {
        ComputationResult::NoReply
    }

    fn get_schema(&self) -> std::collections::HashMap<String, String> {
        let mut schema = std::collections::HashMap::new();
        // TODO: Sending empty schema will throw an error on elixir side.
        schema.insert("input".to_string(), "string".to_string());
        schema
    }
}