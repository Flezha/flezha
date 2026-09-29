//! `OperatorMessage`: every request is answered with
//! `runtime.operator_message_unsupported`.

use prost::Message;

use super::{CommandAction, Reply};
use crate::pb::{self, OperatorMessage, OperatorMessageResponse};
use crate::runner::RunnerNode;

impl CommandAction for OperatorMessage {
    fn handle(
        &self,
        _runner: &mut RunnerNode,
        _message: &str,
        _errors: &mut Vec<crate::Error>,
    ) -> Reply {
        let response = OperatorMessageResponse {
            payload: Vec::new(),
            error: Some(pb::Error::from(
                &crate::Code::OperatorMessageUnsupported
                    .with([("operator", self.operator_id.as_str().into())]),
            )),
        };
        Reply {
            payload_id: "operator_message",
            bytes: response.encode_to_vec(),
        }
    }
}
