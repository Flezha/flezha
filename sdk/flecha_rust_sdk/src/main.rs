mod computation;
mod engine;
mod service;
mod command;
mod utility;

use engine::ComputationEngine;
use computation::{Template, TemplateManager, ComputationResult};
use serde::{Serialize, Deserialize};

fn main() {
    let mut engine = ComputationEngine::new("default".to_string());

    engine.register_template(
        "do_nothing".to_string(),
        || Box::new(TemplateManager{
            template: DoNothingComputation{}
        })
    );

    engine.register_template(
        "compare_filter".to_string(),
         || Box::new(TemplateManager{
            template: CompareFilterComputation{ comparable: "".to_string(), inverted: false }
         })
    );

    engine.register_template(
        "increment_state".to_string(), 
        || Box::new(TemplateManager {
            template: IncrementStateComputation { value: 5000}
        })
    );

    // TODO: remove later.
    std::thread::sleep(std::time::Duration::from_secs(15));
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct CompareFilterComputation {
    comparable: String,
    inverted: bool,
}

impl Template for CompareFilterComputation {
    fn run(&self, input: String) -> ComputationResult {
        if self.inverted {
            if self.comparable == input {
                ComputationResult::NoReply
            } else {
                ComputationResult::Reply(input)
            }
        } else {
            if self.comparable == input {
                ComputationResult::Reply(input)
            } else {
                ComputationResult::NoReply
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct DoNothingComputation {}

impl Template for DoNothingComputation {
    fn run(&self, _input: String) -> ComputationResult {
        ComputationResult::NoReply
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct IncrementStateComputation {
    value: u32
}

impl Template for IncrementStateComputation {
    fn run(&self, _input: String) -> ComputationResult {
        ComputationResult::NoReply
    }
}