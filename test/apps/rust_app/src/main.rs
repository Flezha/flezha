use aflecha_serializer::SerializableComputation;
use aflecha_sdk::SerializableComputation;

use aflecha_sdk::{
    Computation, ComputationInput, ComputationOutput, ComputationResult
};

use serde::{Serialize, Deserialize};

fn main() {
    let mut processor = aflecha_sdk::new("default".to_string());

    processor.register_factory(
        "do_nothing".to_string(),
        || Box::new(DoNothingComputation{})
    );

    processor.register_factory(
        "compare_filter".to_string(),
         || Box::new(CompareFilterComputation{ 
            comparable: "".to_string(), inverted: false 
        })
    );

    processor.register_factory(
        "increment_state".to_string(), 
        || Box::new(IncrementStateComputation {
            value: 5000
        })
    );

    // TODO: remove later.
    std::thread::sleep(std::time::Duration::from_secs(120));
}

#[derive(Serialize, Deserialize, Clone)]
#[derive(SerializableComputation)]
struct CompareFilterComputation {
    comparable: String,
    inverted: bool,
}

impl Computation for CompareFilterComputation {
    fn run(&mut self, input: ComputationInput) -> ComputationOutput {
        match input {
            ComputationInput::Text(text) => {
                if self.inverted {
                    if self.comparable == text {
                        ComputationOutput::None
                    } else {
                        ComputationOutput::Forward(ComputationResult::Text(text))
                    }
                } else {
                    if self.comparable == text {
                        ComputationOutput::Forward(ComputationResult::Text(text))
                    } else {
                        ComputationOutput::None
                    }
                }
            }
            ComputationInput::Binary(_) => ComputationOutput::None
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
#[derive(SerializableComputation)]
struct DoNothingComputation {}

impl Computation for DoNothingComputation {
    fn run(&mut self, _input: ComputationInput) -> ComputationOutput {
        ComputationOutput::None
    }
}

#[derive(Serialize, Deserialize, Clone)]
#[derive(SerializableComputation)]
struct IncrementStateComputation {
    value: u32
}

impl Computation for IncrementStateComputation {
    fn run(&mut self, _input: ComputationInput) -> ComputationOutput {
        self.value += 1;
        ComputationOutput::None
    }
}