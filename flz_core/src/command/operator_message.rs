//! `OperatorMessage`: every request is answered with the `unsupported` error.

use prost::Message;

use super::{CommandAction, Reply};
use crate::pb::{OperatorMessage, OperatorMessageResponse};
use crate::runner::RunnerNode;

const UNSUPPORTED_ERROR: &str = "operator does not support handle_message";

impl CommandAction for OperatorMessage {
    fn handle(&self, _runner: &mut RunnerNode, _message: &str, _errors: &mut Vec<String>) -> Reply {
        let response = OperatorMessageResponse {
            payload: Vec::new(),
            error: UNSUPPORTED_ERROR.to_string(),
        };
        Reply {
            payload_id: "operator_message",
            bytes: response.encode_to_vec(),
        }
    }
}
