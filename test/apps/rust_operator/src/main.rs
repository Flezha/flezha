use aflecha_operator::{
    PipelineResultType,
    define_pipeline_factory
};

use serde::{Serialize, Deserialize};

fn main() {
    define_factories();

    let _operator = aflecha_operator::new("default".to_string());

    // TODO: remove later.
    std::thread::sleep(std::time::Duration::from_secs(36));
}


#[derive(Default, Serialize, Deserialize)]
struct StoreInputState {
    binary_inputs: Vec<Vec<u8>>,
    text_inputs: Vec<String>
}

#[derive(Default, Serialize, Deserialize)]
struct CompareFilterState {
    comparable: String,
    inverted: bool,
}

#[derive(Default, Serialize, Deserialize)]
struct DoNothingState {}

fn define_factories() {
    define_pipeline_factory! {
        factory_id: "store_input".to_string(),
        result_type: PipelineResultType::Forward,
        state: StoreInputState,
        steps: [
            |input: PipelineInput, state: &mut StoreInputState| -> io::Result<PipelineOutput> {
                match input {
                    PipelineInput::Text(text) => {
                        state.text_inputs.push(text.clone());
                        Ok(PipelineOutput::Text(text))
                    }
                    PipelineInput::Binary(binary) => {
                        state.binary_inputs.push(binary.clone());
                        Ok(PipelineOutput::Binary(binary))
                    }
                }

                // Ok(input.into())
            }
        ]
    }

    define_pipeline_factory! {
        factory_id: "compare_filter".to_string(),
        result_type: PipelineResultType::Forward,
        state: CompareFilterState,
        steps: [
            |input: PipelineInput, state: &mut CompareFilterState| -> io::Result<PipelineOutput> {
                
                let text = match input {
                    PipelineInput::Text(text) => text,
                    PipelineInput::Binary(binary) => String::from_utf8(binary).unwrap()
                };

                if state.inverted {
                    if state.comparable == text {
                        Ok(PipelineOutput::Text("Matched".to_string()))
                    } else {
                        Ok(PipelineOutput::Text(text))
                    }
                } else {
                    if state.comparable == text {
                        Ok(PipelineOutput::Text(text))
                    } else {
                        Ok(PipelineOutput::Text("Not matched".to_string()))
                    }
                }
            }
        ]
    }

    define_pipeline_factory! {
        factory_id: "do_nothing".to_string(),
        result_type: PipelineResultType::Forward,
        state: DoNothingState,
        steps: [
            |input: PipelineInput, _state: &mut DoNothingState| -> io::Result<PipelineOutput> {
                Ok(input.into())
            }
        ]
    }
}
