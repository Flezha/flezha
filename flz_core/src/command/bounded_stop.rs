use super::{CommandAction, Reply};
use crate::pb::BoundedStop;
use crate::runner::RunnerNode;

impl CommandAction for BoundedStop {
    fn handle(
        &self,
        runner: &mut RunnerNode,
        _message: &str,
        _errors: &mut Vec<crate::Error>,
    ) -> Reply {
        // Cooperative external stop (`flz operator stop`): the run unwinds
        // in-flight work to its one teardown and emits
        // BoundedCompleted(status="cancelled"). A no-op for an instance that
        // is not running, distinct from the hard `RemOperator` removal.
        if let Some(instance) = runner.get_operator(&self.operator_id) {
            crate::bounded::stop(&instance);
        }
        Reply {
            payload_id: "bounded_stop",
            bytes: Vec::new(),
        }
    }
}
