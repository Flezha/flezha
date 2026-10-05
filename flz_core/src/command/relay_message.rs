//! `RelayMessage`: every request is answered with
//! `runtime.relay_message_unsupported`.

use prost::Message;

use super::{CommandAction, Reply};
use crate::pb::{self, RelayMessage, RelayMessageResponse};
use crate::runner::RunnerNode;

impl CommandAction for RelayMessage {
    fn handle(
        &self,
        _runner: &mut RunnerNode,
        _message: &str,
        _errors: &mut Vec<crate::Error>,
    ) -> Reply {
        let response = RelayMessageResponse {
            payload: Vec::new(),
            error: Some(pb::Error::from(
                &crate::Code::RelayMessageUnsupported
                    .with([("relay", self.relay_id.as_str().into())]),
            )),
        };
        Reply {
            payload_id: "relay_message",
            bytes: response.encode_to_vec(),
        }
    }
}
