//! `CellApplyState`: every request is answered with `state.apply_not_supported`.

use super::{CommandAction, Reply};
use crate::pb::CellApplyState;
use crate::runner::RunnerNode;

impl CommandAction for CellApplyState {
    fn handle(
        &self,
        runner: &mut RunnerNode,
        _message: &str,
        errors: &mut Vec<crate::Error>,
    ) -> Reply {
        if !runner.has_relay(&self.relay_id) {
            errors.push(super::cell_state::relay_not_found(&self.relay_id));
            return Reply {
                payload_id: "cell_apply_state",
                bytes: Vec::new(),
            };
        }

        // Section 4: invoke the dispatcher's apply entry point.
        errors.push(crate::Error::State(crate::Code::ApplyNotSupported.with([])));
        Reply {
            payload_id: "cell_apply_state",
            bytes: Vec::new(),
        }
    }
}
