use super::{CommandAction, Reply};
use crate::pb::RemOperator;
use crate::runner::RunnerNode;

impl CommandAction for RemOperator {
    fn handle(&self, runner: &mut RunnerNode, _message: &str, errors: &mut Vec<String>) -> Reply {
        if runner.has_operator(&self.operator_id) {
            runner.stop_operator(&self.operator_id);
            Reply {
                payload_id: "stop_operator",
                bytes: Vec::new(),
            }
        } else {
            errors.push("UnboundedOperator instance not found".to_string());
            Reply {
                payload_id: "stop_operator",
                bytes: Vec::new(),
            }
        }
    }
}
