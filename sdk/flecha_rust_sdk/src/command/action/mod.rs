mod create_computation;
mod connect_streams;
mod serialize_computation;
mod deserialize_computation;

use connect_streams::ConnectSocketCommand;
use serialize_computation::SerializeComputationCommand;
use deserialize_computation::DeSerializeComputationCommand;

use crate::engine::worker::EngineWorker;

use self::create_computation::CreateComputationCommand;

use serde::Deserialize;
use serde_json::Value;

pub trait CommandAction {
    fn handle(
        &self,
        engine_worker: &mut EngineWorker,
        command_struct: &String,
        reference_id: String,
    );
}

#[derive(Deserialize)]
struct Command {
    reference_id: String,
    data: Value,
}

pub fn handle_action(
    engine_worker: &mut EngineWorker,
    payload_id: &String,
    message: &String
) {
    // Deserialize the command from the JSON message
    let command: Command = serde_json::from_str(message).expect("Failed to parse command");
    let reference_id = command.reference_id;

    let command_action: Box<dyn CommandAction> = match payload_id.as_str() {
        "40" => {
            let command: ConnectSocketCommand = serde_json::from_value(command.data)
            .expect("Failed to deserialize OpenSocketCommand");

            Box::new(command)
        }
        "41" => {
            let command: CreateComputationCommand = serde_json::from_value(command.data)
                .expect("Failed to deserialize CreateComputationCommand");
            Box::new(command)
        }
        "42" => {
            let command: SerializeComputationCommand = serde_json::from_value(command.data)
                .expect("Failed to deserialize SerializeComputationCommand");
            Box::new(command)
        }
        "43" => {
            let command: DeSerializeComputationCommand = serde_json::from_value(command.data)
                .expect("Failed to deserialize DeSerializeComputationCommand");
            Box::new(command)
        }
        _ => {
            panic!("Unknown command payload_id: {}", payload_id);
        }
    };

    command_action.handle(engine_worker, message, reference_id);
}