use super::{CommandAction, Reply};
use crate::pb::RemRelay;
use crate::runner::RunnerNode;

impl CommandAction for RemRelay {
    fn handle(
        &self,
        runner: &mut RunnerNode,
        _message: &str,
        errors: &mut Vec<crate::Error>,
    ) -> Reply {
        if runner.has_relay(&self.relay_id) {
            runner.stop_relay(&self.relay_id);
            Reply {
                payload_id: "stop_relay",
                bytes: Vec::new(),
            }
        } else {
            errors.push(super::cell_state::relay_not_found(&self.relay_id));
            Reply {
                payload_id: "stop_relay",
                bytes: Vec::new(),
            }
        }
    }
}
