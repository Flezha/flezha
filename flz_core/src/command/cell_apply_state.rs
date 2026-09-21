//! `CellApplyState`: every request is answered with `apply_state_not_supported`.

use super::{CommandAction, Reply};
use crate::pb::CellApplyState;
use crate::runner::RunnerNode;

impl CommandAction for CellApplyState {
    fn handle(&self, runner: &mut RunnerNode, _message: &str, errors: &mut Vec<String>) -> Reply {
        if !runner.has_operative(&self.operative_id) {
            errors.push(format!(
                "StreamOperative instance with id {} not found",
                self.operative_id
            ));
            return Reply {
                payload_id: "cell_apply_state",
                bytes: Vec::new(),
            };
        }

        // Section 4: invoke the dispatcher's apply entry point.
        errors.push("apply_state_not_supported".to_string());
        Reply {
            payload_id: "cell_apply_state",
            bytes: Vec::new(),
        }
    }
}
