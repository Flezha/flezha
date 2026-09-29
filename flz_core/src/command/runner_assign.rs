use super::{CommandAction, Reply};
use crate::pb::AssignRunnerId;
use crate::runner::{RunnerNode, RunnerStatus};

impl CommandAction for AssignRunnerId {
    fn handle(
        &self,
        runner: &mut RunnerNode,
        _message: &str,
        _errors: &mut Vec<crate::Error>,
    ) -> Reply {
        // The controller-minted value is this connection's **session token**: it
        // keys the per-connection data-stream sockets and the controller-side
        // Registry/StreamChannel names. It is NOT persisted — the durable
        // identity is the self-owned stable id (see `persistent_id`), which the
        // SDK already presented in the connect handshake.
        runner.assign_runner_id(self.runner_id.clone());
        runner.set_status(RunnerStatus::Running);

        Reply {
            payload_id: "assign_runner_id",
            bytes: Vec::new(),
        }
    }
}
