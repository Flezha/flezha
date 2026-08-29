//! Inverse of `entity_state`. Section 4 routes the state binary
//! through the dispatcher's per-Procedure / per-Entity apply path.
//! Section 3 ships the wiring stub: any `bin_apply_state` request
//! returns the canonical `apply_state_not_supported` token until the
//! dispatcher exposes its apply hook.

use super::{CommandAction, Reply};
use crate::pb::EntityApplyState;
use crate::runner::RunnerNode;

impl CommandAction for EntityApplyState {
    fn handle(&self, runner: &mut RunnerNode, _message: &str, errors: &mut Vec<String>) -> Reply {
        if !runner.has_operative(&self.operative_id) {
            errors.push(format!(
                "StreamOperative instance with id {} not found",
                self.operative_id
            ));
            return Reply {
                payload_id: "entity_apply_state",
                bytes: Vec::new(),
            };
        }

        // Section 4: invoke the dispatcher's apply entry point.
        errors.push("apply_state_not_supported".to_string());
        Reply {
            payload_id: "entity_apply_state",
            bytes: Vec::new(),
        }
    }
}
