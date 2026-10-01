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
        let instance_id = match &self.scope {
            Some(scope) => {
                crate::operator::scope::released(
                    runner.get_operator(&self.operator_id).as_deref(),
                    scope,
                );
                crate::operator::scope::scope_id(
                    &self.operator_id,
                    &scope.inner,
                    scope.key.as_deref(),
                )
            }
            None => self.operator_id.clone(),
        };
        if runner.has_operator(&instance_id) {
            runner.stop_operator(&instance_id);
            Reply {
                payload_id: "stop_operator",
                bytes: Vec::new(),
            }
        } else {
            errors.push(super::cell_state::operator_not_found(&instance_id));
            Reply {
                payload_id: "stop_operator",
                bytes: Vec::new(),
            }
        }
    }
}
