mod computation;
mod engine;
mod service;
mod command;
mod utility;

use engine::ComputationEngine;
use computation::{Blueprint, BlueprintManager, ComputationResult};
use serde::{Serialize, Deserialize};

fn main() {
    let mut engine = ComputationEngine::new("default".to_string());

    engine.register_blueprint(
        "do_nothing".to_string(),
        || Box::new(BlueprintManager{
            blueprint: DoNothingComputation{}
        })
    );

    engine.register_blueprint(
        "compare_filter".to_string(),
         || Box::new(BlueprintManager{
            blueprint: CompareFilterComputation{ comparable: "".to_string(), inverted: false }
         })
    );

    engine.register_blueprint(
        "increment_state".to_string(), 
        || Box::new(BlueprintManager {
            blueprint: IncrementStateComputation { value: 5000}
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

impl Blueprint for CompareFilterComputation {
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

impl Blueprint for DoNothingComputation {
    fn run(&self, _input: String) -> ComputationResult {
        ComputationResult::NoReply
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct IncrementStateComputation {
    value: u32
}

impl Blueprint for IncrementStateComputation {
    fn run(&self, _input: String) -> ComputationResult {
        ComputationResult::NoReply
    }
}