use super::{CommandAction, Reply};
use crate::pb::WorkflowStop;
use crate::runner::RunnerNode;

impl CommandAction for WorkflowStop {
    fn handle(&self, runner: &mut RunnerNode, _message: &str, _errors: &mut Vec<String>) -> Reply {
        // Cooperative external stop (`flz operative stop`): the run unwinds
        // in-flight work to its one teardown and emits
        // WorkflowCompleted(status="cancelled"). A no-op for an instance that
        // is not running, distinct from the hard `RemOperative` removal.
        if let Some(instance) = runner.get_operative(&self.operative_id) {
            crate::workflow::stop(&instance);
        }
        Reply {
            payload_id: "workflow_stop",
            bytes: Vec::new(),
        }
    }
}
