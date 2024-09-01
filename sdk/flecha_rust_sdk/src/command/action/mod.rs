mod create_computation;
mod open_socket;

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
            let open_socket_command: open_socket::OpenSocketCommand = serde_json::from_value(command.data)
                .expect("Failed to deserialize OpenSocketCommand");
            Box::new(open_socket_command)
        }
        "41" => {
            let create_pipeline_command: CreateComputationCommand = serde_json::from_value(command.data)
                .expect("Failed to deserialize CreateComputationCommand");
            Box::new(create_pipeline_command)
        }
        _ => {
            panic!("Unknown command payload_id: {}", payload_id);
        }
    };

    command_action.handle(engine_worker, message, reference_id);
}