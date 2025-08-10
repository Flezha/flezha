use std::env;
use std::io::{self, Read};
use std::thread::{self};
use std::time::Duration;

use flezha_operator::computation::ComputationError;
use flezha_operator::{register_factory, ComputationHandler, ComputationSettings};

use serde::{Deserialize, Serialize};
fn main() {
    define_factories();

    let _operator = flezha_operator::new(namespace());

    // To keep the process alive while the agent is running.
    std_keeper();
}

fn namespace() -> String {
    let args: Vec<String> = env::args().collect();

    if args.len() > 1 {
        return args[1].clone();
    }

    return "default".to_string();
}

fn std_keeper() -> thread::JoinHandle<()> {
    let mut buf = [0u8; 1];

    loop {
        let result = io::stdin().read(&mut buf);
        match result {
            Ok(0) => {
                std::process::exit(0);
            }
            Ok(_) => {
                // Data available; continue monitoring
                println!("Data available: {:?}", buf);
            }
            Err(_e) => {}
        }

        thread::sleep(Duration::from_secs(1));
    }
}

// STEP DEFINITIONS --------------------------------------------------------------------
struct DoNothingComputation;
impl ComputationHandler<DoNothingState, String, String> for DoNothingComputation {
    fn process(
        &mut self,
        input: String,
        _state: &mut DoNothingState,
    ) -> Result<String, ComputationError> {
        Ok(input)
    }
}

struct StoreInputComputation;
impl ComputationHandler<StoreInputState, String, String> for StoreInputComputation {
    fn process(
        &mut self,
        input: String,
        state: &mut StoreInputState,
    ) -> Result<String, ComputationError> {
        state.text_inputs.push(input.clone());
        Ok(input)
    }
}

struct CompareFilterComputation;
impl ComputationHandler<CompareFilterState, String, String> for CompareFilterComputation {
    fn process(
        &mut self,
        input: String,
        state: &mut CompareFilterState,
    ) -> Result<String, ComputationError> {
        if state.inverted {
            if state.comparable == input {
                Ok(input)
            } else {
                Ok("Not matched".to_string())
            }
        } else {
            if state.comparable == input {
                Ok(input)
            } else {
                Ok("Not matched".to_string())
            }
        }
    }

    fn post_create(&mut self, _state: &mut CompareFilterState) {}
}

// STATE DEFINITIONS -------------------------------------------------------------------
#[derive(Serialize, Deserialize, Clone)]
struct DoNothingState {}

#[derive(Serialize, Deserialize, Clone)]
struct StoreInputState {
    binary_inputs: Vec<Vec<u8>>,
    text_inputs: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone)]
struct CompareFilterState {
    comparable: String,
    inverted: bool,
}

// FACTORY DEFINITIONS -----------------------------------------------------------------
fn define_factories() {
    register_factory(
        "store_input".to_string(),
        ComputationSettings::<StoreInputState, String, String>::default(),
        StoreInputState {
            binary_inputs: vec![],
            text_inputs: vec![],
        },
        || Box::new(StoreInputComputation {}),
    );

    register_factory(
        "compare_filter".to_string(),
        ComputationSettings::<CompareFilterState, String, String>::default(),
        CompareFilterState {
            comparable: "default".to_string(),
            inverted: false,
        },
        || Box::new(CompareFilterComputation {}),
    );

    register_factory(
        "do_nothing".to_string(),
        ComputationSettings::<DoNothingState, String, String>::default(),
        DoNothingState {},
        || Box::new(DoNothingComputation {}),
    );
}
