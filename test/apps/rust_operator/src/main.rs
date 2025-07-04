use std::thread::{self};
use std::io::{self, Read};
use std::time::Duration;
use std::env;

use flezha_operator::{
    PipelineSettings,
    PipelineStep,
    create_pipeline_factory
};

use serde::{Serialize, Deserialize};

fn main() {
    define_factories();

    let _operator = flezha_operator::new(group());

    // To keep the process alive while the agent is running.
    std_keeper();
}

fn group() -> String {
    let args: Vec<String> = env::args().collect();

    if args.len() > 1 {
        return args[1].clone();
    }

    return "default".to_string()
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
struct DoNothingStep;
impl PipelineStep<DoNothingState, String> for DoNothingStep {
    fn process(&mut self, input: String, _state: &mut DoNothingState) -> io::Result<String> {
        Ok(input)
    }
}

struct StoreInputStep;
impl PipelineStep<StoreInputState, String> for StoreInputStep {
    fn process(&mut self, input: String, state: &mut StoreInputState) -> io::Result<String> {
        state.text_inputs.push(input.clone());
        Ok(input)
    }
}

struct CompareFilterStep;
impl PipelineStep<CompareFilterState, String> for CompareFilterStep {
    fn process(&mut self, input: String, state: &mut CompareFilterState) -> io::Result<String> {
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

    fn post_create(&mut self, _state: &mut CompareFilterState) {

    }
}

// STATE DEFINITIONS -------------------------------------------------------------------
#[derive(Serialize, Deserialize, Clone)]
struct DoNothingState {}

#[derive(Serialize, Deserialize, Clone)]
struct StoreInputState {
    binary_inputs: Vec<Vec<u8>>,
    text_inputs: Vec<String>
}

#[derive(Serialize, Deserialize, Clone)]
struct CompareFilterState {
    comparable: String,
    inverted: bool,
}

// FACTORY DEFINITIONS -----------------------------------------------------------------
fn define_factories() {
    create_pipeline_factory(
        "store_input".to_string(),
        PipelineSettings::<String>::default(),
        StoreInputState{
            binary_inputs: vec![],
            text_inputs: vec![]
        },
         || vec![Box::new(StoreInputStep{})]
    );

    create_pipeline_factory(
        "compare_filter".to_string(),
        PipelineSettings::<String>::default(),
        CompareFilterState{
            comparable: "default".to_string(),
            inverted: false
        },
        || vec![Box::new(CompareFilterStep{})]
    );

    create_pipeline_factory(
        "do_nothing".to_string(),
        PipelineSettings::<String>::default(),
        DoNothingState{},
        || vec![Box::new(DoNothingStep{})]
    ); 
}
