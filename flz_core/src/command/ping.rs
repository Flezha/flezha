use prost::Message;

use super::{CommandAction, Reply};
use crate::pb::{Ping, PingResponse};
use crate::runner::RunnerNode;

/// Cheap end-to-end control-channel health probe.
///
/// The Controller issues `Ping` immediately before each `AddOperative` during
/// the Overwatch's cold-restart startup sequence (see
/// `runner-controller-connection-healing`). A successful round-trip
/// proves the channel is bound and the SDK's reply path is intact, so a
/// subsequent `AddOperative` is safe to issue. The handler echoes the request
/// `nonce` so the caller can correlate replies if multiple pings are in
/// flight on the same channel.
impl CommandAction for Ping {
    fn handle(&self, _runner: &mut RunnerNode, _message: &str, _errors: &mut Vec<String>) -> Reply {
        let response = PingResponse {
            nonce: self.nonce.clone(),
        };

        Reply {
            payload_id: "ping",
            bytes: response.encode_to_vec(),
        }
    }
}
