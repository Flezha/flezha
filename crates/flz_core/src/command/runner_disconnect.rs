use super::{CommandAction, Reply};
use crate::pb::RunnerDisconnect;
use crate::runner::{RunnerNode, RunnerStatus};

impl CommandAction for RunnerDisconnect {
    fn handle(
        &self,
        runner: &mut RunnerNode,
        _message: &str,
        _errors: &mut Vec<crate::Error>,
    ) -> Reply {
        runner.set_status(RunnerStatus::Stopped);
        if self.terminal {
            // Acted on by the disconnect arm of `command::handle_command`,
            // once this reply is on the wire — the controller must see its ack
            // before the process goes away.
            crate::shutdown::request_terminal_exit(self.reason.clone());
        }

        Reply {
            payload_id: "runner_disconnect",
            bytes: Vec::new(),
        }
    }
}
