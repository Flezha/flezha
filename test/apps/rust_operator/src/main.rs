use std::thread::{self};
use std::io::{self, Read};
use std::time::Duration;
use std::env;

use flezha_operator::{
    PipelineResultType,
    PipelineStep,
    define_pipeline_factory
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
impl PipelineStep<DoNothingState> for DoNothingStep {
    fn run(&mut self, input: flezha_operator::PipelineInput, _state: &mut DoNothingState) -> io::Result<flezha_operator::PipelineOutput> {
        Ok(input.into())
    }
}

struct StoreInputStep;
impl PipelineStep<StoreInputState> for StoreInputStep {
    fn run(&mut self, input: flezha_operator::PipelineInput, state: &mut StoreInputState) -> io::Result<flezha_operator::PipelineOutput> {
        match input {
            flezha_operator::PipelineInput::Text(text) => {
                state.text_inputs.push(text.clone());
                Ok(flezha_operator::PipelineOutput::Text(text))
            }
            flezha_operator::PipelineInput::Binary(binary) => {
                state.binary_inputs.push(binary.clone());
                Ok(flezha_operator::PipelineOutput::Binary(binary))
            }
        }
    }
}

struct CompareFilterStep;
impl PipelineStep<CompareFilterState> for CompareFilterStep {
    fn run(&mut self, input: flezha_operator::PipelineInput, state: &mut CompareFilterState) -> io::Result<flezha_operator::PipelineOutput> {
        let text = match input {
            flezha_operator::PipelineInput::Text(text) => text,
            flezha_operator::PipelineInput::Binary(binary) => String::from_utf8(binary).unwrap()
        };

        if state.inverted {
            if state.comparable == text {
                Ok(flezha_operator::PipelineOutput::Text("Matched".to_string()))
            } else {
                Ok(flezha_operator::PipelineOutput::Text(text))
            }
        } else {
            if state.comparable == text {
                Ok(flezha_operator::PipelineOutput::Text(text))
            } else {
                Ok(flezha_operator::PipelineOutput::Text("Not matched".to_string()))
            }
        }
    }

    fn post_create(&mut self, _state: &mut CompareFilterState) {

    }
}

// STATE DEFINITIONS -------------------------------------------------------------------
#[derive(Serialize, Deserialize)]
struct DoNothingState {}

#[derive(Serialize, Deserialize)]
struct StoreInputState {
    binary_inputs: Vec<Vec<u8>>,
    text_inputs: Vec<String>
}

#[derive(Serialize, Deserialize)]
struct CompareFilterState {
    comparable: String,
    inverted: bool,
}

// FACTORY DEFINITIONS -----------------------------------------------------------------
fn define_factories() {
    define_pipeline_factory! {
        factory_id: "store_input".to_string(),
        result_type: PipelineResultType::Forward,
        state: StoreInputState{
            binary_inputs: vec![],
            text_inputs: vec![]
        },
        steps: [
            StoreInputStep
        ]
    }

    define_pipeline_factory! {
        factory_id: "compare_filter".to_string(),
        result_type: PipelineResultType::Forward,
        state: CompareFilterState{
            comparable: "default".to_string(),
            inverted: false
        },
        steps: [
            CompareFilterStep
        ]
    }

    define_pipeline_factory! {
        factory_id: "do_nothing".to_string(),
        result_type: PipelineResultType::Forward,
        state: DoNothingState{},
        steps: [
            DoNothingStep
        ]
    }
}
