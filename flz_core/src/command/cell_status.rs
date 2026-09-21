//! Inbound `CellMailboxStatus`: legacy controller → SDK back-pressure
//! signal, kept only for compatibility with pre-credit controllers.
//!
//! A credit-speaking controller never sends this (back-pressure rides the
//! credit-grant protocol — see `crate::credit`); a pre-credit
//! controller's `"full"`/`"open"` flags are tolerated and dropped, since
//! this SDK's send path is already bounded by its local mailbox policies.
//!
//! No `CommandResponse` payload — this is a one-way notification.

use super::{CommandAction, Reply};
use crate::pb::CellMailboxStatus;
use crate::runner::RunnerNode;

impl CommandAction for CellMailboxStatus {
    fn handle(&self, _runner: &mut RunnerNode, _message: &str, _errors: &mut Vec<String>) -> Reply {
        tracing::debug!(
            target: "flezha::backpressure",
            "legacy cell mailbox status from a pre-credit controller: {}",
            self.status,
        );

        Reply {
            payload_id: "cell_mailbox_status",
            bytes: Vec::new(),
        }
    }
}
