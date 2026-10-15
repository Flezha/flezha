//! Inbound `ParkPayloadAck` (controller → SDK): the dead-letter entry
//! identified by `park_id` is durable in the controller's DLQ store. Lets
//! go of the park: a Cell blocked in `park::park_tier_off` drops its poison
//! payload and advances, and the park owner frees the slot it held. Runs on
//! the control coordinator and never blocks.

use super::{CommandAction, Reply};
use crate::park;
use crate::pb::ParkPayloadAck;
use crate::runner::RunnerNode;

impl CommandAction for ParkPayloadAck {
    fn handle(
        &self,
        _runner: &mut RunnerNode,
        _message: &str,
        _errors: &mut Vec<crate::Error>,
    ) -> Reply {
        park::complete_ack(&self.park_id);
        Reply {
            payload_id: "park_payload_ack",
            bytes: Vec::new(),
        }
    }
}
