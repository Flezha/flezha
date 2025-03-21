use aflecha_operator::{
    PipelineResultType,
    define_pipeline_factory
};

use serde::{Serialize, Deserialize};

fn main() {
    define_factories();

    let _operator = aflecha_operator::new("default".to_string());

    // TODO: 
    // operator.register_factory(
    //     "store_input".to_string(),
    //     || Box::new(StoreInputWorker{
    //         text_inputs: vec![],
    //         binary_inputs: vec![]
    //     })
    // );

    // operator.register_factory(
    //     "forward_orig".to_string(),
    //     || Box::new(ForwardOrigWorker{})
    // );

    // operator.register_factory(
    //     "do_nothing".to_string(),
    //     || Box::new(DoNothingWorker{})
    // );

    // operator.register_factory(
    //     "compare_filter".to_string(),
    //      || Box::new(CompareFilterWorker{ 
    //         comparable: "".to_string(), inverted: false 
    //     })
    // );

    // operator.register_factory(
    //     "increment_state".to_string(), 
    //     || Box::new(IncrementStateWorker {
    //         value: 5000
    //     })
    // );

    // TODO: remove later.
    std::thread::sleep(std::time::Duration::from_secs(36));
}


#[derive(Default, Serialize, Deserialize)]
struct StoreInputState {
    binary_inputs: Vec<Vec<u8>>,
    text_inputs: Vec<String>
}

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
}



// TODO:
// #[derive(Serialize, Deserialize, Clone)]
// #[derive(SerializableWorker)]
// struct CompareFilterWorker {
//     comparable: String,
//     inverted: bool,
// }

// impl Worker for CompareFilterWorker {
//     fn run(&mut self, input: PipelineInput) -> PipelineOutput {
//         let text = input.as_string();

//         if self.inverted {
//             if self.comparable == text {
//                 PipelineOutput::None
//             } else {
//                 PipelineOutput::Forward(PipelineResult::Text(text))
//             }
//         } else {
//             if self.comparable == text {
//                 PipelineOutput::Forward(PipelineResult::Text(text))
//             } else {
//                 PipelineOutput::None
//             }
//         }
//     }
// }

// #[derive(Serialize, Deserialize, Clone)]
// #[derive(SerializableWorker)]
// struct DoNothingWorker {}

// impl Worker for DoNothingWorker {
//     fn run(&mut self, _input: PipelineInput) -> PipelineOutput {
//         PipelineOutput::None
//     }
// }

// #[derive(Serialize, Deserialize, Clone)]
// #[derive(SerializableWorker)]
// struct ForwardOrigWorker {}

// impl Worker for ForwardOrigWorker {
//     fn run(&mut self, input: PipelineInput) -> PipelineOutput {
//         match input {
//             PipelineInput::Text(text) => {
//                 PipelineOutput::Forward(PipelineResult::Text(text))
//             }
//             PipelineInput::Binary(binary) => {
//                 PipelineOutput::Forward(PipelineResult::Binary(binary))
//             }
//         }
//     }
// }



// #[derive(SerializableWorker, Serialize, Deserialize, Clone)]
// struct IncrementStateWorker {
//     value: u32
// }

// impl Worker for IncrementStateWorker {
//     fn run(&mut self, _input: PipelineInput) -> PipelineOutput {
//         self.value += 1;
//         PipelineOutput::None
//     }
// }