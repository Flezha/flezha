use super::{CommandAction, Reply};
use crate::pb::RemOperative;
use crate::runner::RunnerNode;

impl CommandAction for RemOperative {
    fn handle(&self, runner: &mut RunnerNode, _message: &str, errors: &mut Vec<String>) -> Reply {
        if runner.has_operative(&self.operative_id) {
            runner.stop_operative(&self.operative_id);
            Reply {
                payload_id: "stop_operative",
                bytes: Vec::new(),
            }
        } else {
            errors.push("StreamOperative instance not found".to_string());
            Reply {
                payload_id: "stop_operative",
                bytes: Vec::new(),
            }
        }
    }
}
