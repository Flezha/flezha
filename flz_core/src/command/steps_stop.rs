use super::{CommandAction, Reply};
use crate::pb::StepsStop;
use crate::runner::RunnerNode;

impl CommandAction for StepsStop {
    fn handle(
        &self,
        runner: &mut RunnerNode,
        _message: &str,
        _errors: &mut Vec<crate::Error>,
    ) -> Reply {
        // Cooperative external stop (`flz operator stop`): the run unwinds
        // in-flight work to its one teardown and emits
        // OperatorCompleted(status="cancelled"). A no-op for an instance that
        // is not running, distinct from the hard `RemOperator` removal.
        if let Some(instance) = runner.get_operator(&self.operator_id) {
            crate::steps::stop(&instance);
        }
        Reply {
            payload_id: "steps_stop",
            bytes: Vec::new(),
        }
    }
}
