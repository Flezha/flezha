//! Inbound credit-grant flow control commands (controller → SDK).
//!
//! `CreditWindowInit` arms the process-global sender window
//! (`crate::credit`) and acks by returning the encoded
//! `CreditWindowAck` as this command's `CommandResponse` payload — the
//! controller marks the session credit-speaking on that ack.
//! `CreditGrant` replenishes the window. Both run on the thread that reads
//! control frames, never queued behind the coordinator: a coordinator command
//! can wait on a Cell's buffer lock whose holder is parked on exhausted
//! credit, and only a grant frees it.

use std::collections::HashMap;

use flz_transport::transport::FlezhaStream;
use prost::Message;

use crate::credit;
use crate::pb::command_request::Command;
use crate::pb::{CommandRequest, CreditWindowAck};

/// Apply and answer `message` when it is a credit command; `false` for any
/// other frame, which the coordinator runs.
pub(crate) fn answer_credit(stream: &FlezhaStream, payload_id: &str, message: &[u8]) -> bool {
    let Ok(CommandRequest {
        reference_id,
        command,
    }) = CommandRequest::decode(message)
    else {
        return false;
    };
    let reply = match command {
        Some(Command::CreditWindowInit(init)) => {
            credit::global().arm(init.stream_id.clone(), init.window_bytes);
            CreditWindowAck {
                stream_id: init.stream_id,
            }
            .encode_to_vec()
        }
        Some(Command::CreditGrant(grant)) => {
            credit::global().grant(&grant.stream_id, grant.bytes);
            Vec::new()
        }
        _ => return false,
    };
    super::respond_with(
        stream,
        super::encode_response(payload_id, reference_id, reply, &[], HashMap::new()),
    );
    true
}
