use aflecha_sdk::{
    ComputationDataType, 
    TextComputation, 
    BinaryComputation, 
    ComputationOutput,
    ComputationResult,
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
    fn run(&mut self, input: String) -> ComputationOutput {
        if self.inverted {
            if self.comparable == input {
                ComputationOutput::None
            } else {
                ComputationOutput::Forward(ComputationResult::Text(input))
            }
        } else {
            if self.comparable == input {
                ComputationOutput::Forward(ComputationResult::Text(input))
            } else {
                ComputationOutput::None
            }
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
struct DoNothingComputation {}

impl TextComputation for DoNothingComputation {
    fn run(&mut self, _input: String) -> ComputationOutput {
        ComputationOutput::None
    }
}

#[derive(Serialize, Deserialize, Clone)]
struct IncrementStateComputation {
    value: u32
}

impl BinaryComputation for IncrementStateComputation {
    fn run(&mut self, _input: &Vec<u8>) -> ComputationOutput {
        self.value += 1;
        ComputationOutput::None
    }
}