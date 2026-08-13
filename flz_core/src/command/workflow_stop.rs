use super::CommandAction;
use crate::pb::WorkflowStop;
use crate::runner::RunnerNode;

impl CommandAction for WorkflowStop {
    fn handle(
        &self,
        runner: &mut RunnerNode,
        _message: &str,
        _errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        // Cooperative external stop (`flz operative stop`): trip the workflow
        // driver's cancellation token. The driver unwinds in-flight work to the
        // single teardown and emits WorkflowCompleted(status="cancelled"). A
        // no-op if the instance is not running (already finished / never
        // started), distinct from the hard `RemOperative` removal.
        if let Some(instance) = runner.get_workflow(&self.operative_id) {
            instance.request_cancel();
        }
        ("workflow_stop".to_string(), Vec::new())
    }
}
