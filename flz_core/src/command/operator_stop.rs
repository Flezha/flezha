use super::{CommandAction, Reply};
use crate::pb::RemOperator;
use crate::runner::RunnerNode;

impl CommandAction for RemOperator {
    fn handle(
        &self,
        runner: &mut RunnerNode,
        _message: &str,
        errors: &mut Vec<crate::Error>,
    ) -> Reply {
        if runner.has_operator(&self.operator_id) {
            runner.stop_operator(&self.operator_id);
            Reply {
                payload_id: "stop_operator",
                bytes: Vec::new(),
            }
        } else {
            errors.push(super::cell_state::operator_not_found(&self.operator_id));
            Reply {
                payload_id: "stop_operator",
                bytes: Vec::new(),
            }
        }
    }
}
