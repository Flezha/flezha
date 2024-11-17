use aflecha_sdk::{
    ComputationDataType, 
    TextComputation, 
    BinaryComputation, 
    ComputationResult,
    ComputationOutput,
};

use serde::{Serialize, Deserialize};

fn main() {
    let mut processor = aflecha_sdk::new("default".to_string());

    processor.register_factory(
        "do_nothing".to_string(),
        || ComputationDataType::Text(Box::new(DoNothingComputation{}))
    );

    processor.register_factory(
        "compare_filter".to_string(),
         || ComputationDataType::Text(Box::new(CompareFilterComputation{ 
            comparable: "".to_string(), inverted: false 
        }))
    );

    processor.register_factory(
        "increment_state".to_string(), 
        || ComputationDataType::Binary(Box::new(IncrementStateComputation {
            value: 5000
        }))
    );

    // TODO: remove later.
    std::thread::sleep(std::time::Duration::from_secs(120));
}

#[derive(Serialize, Deserialize, Clone)]
struct CompareFilterComputation {
    comparable: String,
    inverted: bool,
}

impl TextComputation for CompareFilterComputation {
    fn run(&mut self, input: String) -> ComputationResult {
        if self.inverted {
            if self.comparable == input {
                ComputationResult::None
            } else {
                ComputationResult::Forward(ComputationOutput::Text(input))
            }
        } else {
            if self.comparable == input {
                ComputationResult::Forward(ComputationOutput::Text(input))
            } else {
                ComputationResult::None
            }
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
struct DoNothingComputation {}

impl TextComputation for DoNothingComputation {
    fn run(&mut self, _input: String) -> ComputationResult {
        ComputationResult::None
    }
}

#[derive(Serialize, Deserialize, Clone)]
struct IncrementStateComputation {
    value: u32
}

impl BinaryComputation for IncrementStateComputation {
    fn run(&mut self, _input: &Vec<u8>) -> ComputationResult {
        self.value += 1;
        ComputationResult::None
    }
}