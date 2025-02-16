use std::vec;

use aflecha_derive::SerializableWorker;

use aflecha_operator::{
    Worker, WorkerInput, WorkerOutput, WorkerResult, SerializableWorker
};

use serde::{Serialize, Deserialize};

fn main() {
    let mut operator = aflecha_operator::new("default".to_string());

    operator.register_factory(
        "store_input".to_string(),
        || Box::new(StoreInputWorker{
            text_inputs: vec![],
            binary_inputs: vec![]
        })
    );

    operator.register_factory(
        "forward_orig".to_string(),
        || Box::new(ForwardOrigWorker{})
    );

    operator.register_factory(
        "do_nothing".to_string(),
        || Box::new(DoNothingWorker{})
    );

    operator.register_factory(
        "compare_filter".to_string(),
         || Box::new(CompareFilterWorker{ 
            comparable: "".to_string(), inverted: false 
        })
    );

    operator.register_factory(
        "increment_state".to_string(), 
        || Box::new(IncrementStateWorker {
            value: 5000
        })
    );

    // TODO: remove later.
    std::thread::sleep(std::time::Duration::from_secs(36));
}

#[derive(Serialize, Deserialize, Clone)]
#[derive(SerializableWorker)]
struct CompareFilterWorker {
    comparable: String,
    inverted: bool,
}

impl Worker for CompareFilterWorker {
    fn run(&mut self, input: WorkerInput) -> WorkerOutput {
        let text = input.as_string();

        if self.inverted {
            if self.comparable == text {
                WorkerOutput::None
            } else {
                WorkerOutput::Forward(WorkerResult::Text(text))
            }
        } else {
            if self.comparable == text {
                WorkerOutput::Forward(WorkerResult::Text(text))
            } else {
                WorkerOutput::None
            }
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
#[derive(SerializableWorker)]
struct DoNothingWorker {}

impl Worker for DoNothingWorker {
    fn run(&mut self, _input: WorkerInput) -> WorkerOutput {
        WorkerOutput::None
    }
}

#[derive(Serialize, Deserialize, Clone)]
#[derive(SerializableWorker)]
struct ForwardOrigWorker {}

impl Worker for ForwardOrigWorker {
    fn run(&mut self, input: WorkerInput) -> WorkerOutput {
        match input {
            WorkerInput::Text(text) => {
                WorkerOutput::Forward(WorkerResult::Text(text))
            }
            WorkerInput::Binary(binary) => {
                WorkerOutput::Forward(WorkerResult::Binary(binary))
            }
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
#[derive(SerializableWorker)]
struct StoreInputWorker {
    binary_inputs: Vec<Vec<u8>>,
    text_inputs: Vec<String>
}

impl Worker for StoreInputWorker {
    fn run(&mut self, input: WorkerInput) -> WorkerOutput {
        match input {
            WorkerInput::Text(text) => {
                self.text_inputs.push(text);
            }
            WorkerInput::Binary(binary) => {
                self.binary_inputs.push(binary);
            }
        }

        WorkerOutput::None
    }
}

#[derive(SerializableWorker, Serialize, Deserialize, Clone)]
struct IncrementStateWorker {
    value: u32
}

impl Worker for IncrementStateWorker {
    fn run(&mut self, _input: WorkerInput) -> WorkerOutput {
        self.value += 1;
        WorkerOutput::None
    }
}