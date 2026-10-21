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
        // Cooperative external stop (`flz relay stop`): the run unwinds
        // in-flight work to its one teardown and emits
        // RelayCompleted(status="cancelled"). A no-op for an instance that
        // is not running, distinct from the hard `RemRelay` removal.
        if let Some(instance) = runner.get_relay(&self.relay_id) {
            crate::steps::stop(&instance);
        }
        Reply {
            payload_id: "steps_stop",
            bytes: Vec::new(),
        }
    }
}
