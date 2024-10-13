mod create_computation;
mod rem_computation;
mod connect_streams;
mod serialize_computation;
mod deserialize_computation;

use std::sync::{Arc, Mutex};

use connect_streams::ConnectSocketCommand;
use serialize_computation::SerializeComputationCommand;
use deserialize_computation::DeSerializeComputationCommand;


use crate::engine::ComputationEngine;

use self::create_computation::CreateComputationCommand;
use self::rem_computation::RemComputationCommand;

use serde::Deserialize;
use serde_json::Value;

pub trait CommandAction {
    fn handle(
        &self,
        engine: &Arc<Mutex<ComputationEngine>>,
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
    engine: &Arc<Mutex<ComputationEngine>>,
    payload_id: &String,
    message: &String
) {
    // Deserialize the command from the JSON message
    let command: Command = serde_json::from_str(message).expect("Failed to parse command");
    let reference_id = command.reference_id;

    let command_action: Box<dyn CommandAction> = match payload_id.as_str() {
        "connect_sockets" => {
            let command: ConnectSocketCommand = serde_json::from_value(command.data)
                .expect("Failed to deserialize OpenSocketCommand");
            Box::new(command)
        }
        "add_computation" => {
            let command: CreateComputationCommand = serde_json::from_value(command.data)
                .expect("Failed to deserialize CreateComputationCommand");
            Box::new(command)
        }
        "rem_computation" => {
            let command: RemComputationCommand = serde_json::from_value(command.data)
                .expect("Failed to deserialize CreateComputationCommand");
            Box::new(command)
        }
        "serialize_computation" => {
            let command: SerializeComputationCommand = serde_json::from_value(command.data)
                .expect("Failed to deserialize SerializeComputationCommand");
            Box::new(command)
        }
        "deserialize_computation" => {
            let command: DeSerializeComputationCommand = serde_json::from_value(command.data)
                .expect("Failed to deserialize DeSerializeComputationCommand");
            Box::new(command)
        }
        _ => {
            panic!("Unknown command payload_id: {}", payload_id);
        }
    };

    command_action.handle(engine, message, reference_id);
}