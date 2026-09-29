//! Inbound credit-grant flow control commands (controller → SDK).
//!
//! `CreditWindowInit` arms the process-global sender window
//! (`crate::credit`) and acks by returning the encoded
//! `CreditWindowAck` as this command's `CommandResponse` payload — the
//! controller marks the session credit-speaking on that ack.
//! `CreditGrant` replenishes the window; both run on the control
//! coordinator and never block, so grants keep flowing while data-path
//! threads are parked on exhausted credit.

use prost::Message;

use super::{CommandAction, Reply};
use crate::credit;
use crate::pb::{CreditGrant, CreditWindowAck, CreditWindowInit};
use crate::runner::RunnerNode;

impl CommandAction for CreditWindowInit {
    fn handle(
        &self,
        _runner: &mut RunnerNode,
        _message: &str,
        _errors: &mut Vec<crate::Error>,
    ) -> Reply {
        credit::global().arm(self.stream_id.clone(), self.window_bytes);

        let ack = CreditWindowAck {
            stream_id: self.stream_id.clone(),
        };

        Reply {
            payload_id: "credit_window_init",
            bytes: ack.encode_to_vec(),
        }
    }
}

impl CommandAction for CreditGrant {
    fn handle(
        &self,
        _runner: &mut RunnerNode,
        _message: &str,
        _errors: &mut Vec<crate::Error>,
    ) -> Reply {
        credit::global().grant(&self.stream_id, self.bytes);

        Reply {
            payload_id: "credit_grant",
            bytes: Vec::new(),
        }
    }
}
