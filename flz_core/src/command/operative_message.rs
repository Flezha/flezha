//! `OperativeMessage`: every request is answered with the `unsupported` error.

use prost::Message;

use super::{CommandAction, Reply};
use crate::pb::{OperativeMessage, OperativeMessageResponse};
use crate::runner::RunnerNode;

const UNSUPPORTED_ERROR: &str = "operative does not support handle_message";

impl CommandAction for OperativeMessage {
    fn handle(&self, _runner: &mut RunnerNode, _message: &str, _errors: &mut Vec<String>) -> Reply {
        let response = OperativeMessageResponse {
            payload: Vec::new(),
            error: UNSUPPORTED_ERROR.to_string(),
        };
        Reply {
            payload_id: "operative_message",
            bytes: response.encode_to_vec(),
        }
    }
}
