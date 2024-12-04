use std::vec;

use aflecha_derive::SerializableComputation;

use aflecha_operator::{
    Computation, ComputationInput, ComputationOutput, ComputationResult, SerializableComputation
};

use serde::{Serialize, Deserialize};

fn main() {
    let mut processor = aflecha_operator::new("default".to_string());

    processor.register_factory(
        "store_input".to_string(),
        || Box::new(StoreInputComputation{
            text_inputs: vec![],
            binary_inputs: vec![]
        })
    );

    processor.register_factory(
        "forward_orig".to_string(),
        || Box::new(ForwardOrigComputation{})
    );

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
    std::thread::sleep(std::time::Duration::from_secs(360));
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
struct ForwardOrigComputation {}

impl Computation for ForwardOrigComputation {
    fn run(&mut self, input: ComputationInput) -> ComputationOutput {
        match input {
            ComputationInput::Text(text) => {
                ComputationOutput::Forward(ComputationResult::Text(text))
            }
            ComputationInput::Binary(binary) => {
                ComputationOutput::Forward(ComputationResult::Binary(binary))
            }
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
#[derive(SerializableComputation)]
struct StoreInputComputation {
    binary_inputs: Vec<Vec<u8>>,
    text_inputs: Vec<String>
}

impl Computation for StoreInputComputation {
    fn run(&mut self, input: ComputationInput) -> ComputationOutput {
        match input {
            ComputationInput::Text(text) => {
                self.text_inputs.push(text);
            }
            ComputationInput::Binary(binary) => {
                self.binary_inputs.push(binary);
            }
        }

        ComputationOutput::None
    }
}

#[derive(SerializableComputation, Serialize, Deserialize, Clone)]
struct IncrementStateComputation {
    value: u32
}

impl Computation for IncrementStateComputation {
    fn run(&mut self, _input: ComputationInput) -> ComputationOutput {
        self.value += 1;
        ComputationOutput::None
    }
}