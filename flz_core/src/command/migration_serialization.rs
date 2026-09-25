//! `MigrationSerialization` — produces the per-Tasklet / per-Cell
//! state blob per `operator-mvp-rust/design.md` Decision 10, then removes
//! the UnboundedOperator instance from the local registry (the controller is migrating
//! it elsewhere).

use super::{CommandAction, Reply};
use crate::command::cell_state::serialize_operator_state;
use crate::pb::MigrationSerialization;
use crate::runner::RunnerNode;

impl CommandAction for MigrationSerialization {
    fn handle(&self, runner: &mut RunnerNode, _message: &str, _errors: &mut Vec<String>) -> Reply {
        let serialized_state = match runner.get_operator(&self.operator_id) {
            Some(instance) => serialize_operator_state(&instance),
            None => Vec::new(),
        };

        runner.stop_operator(&self.operator_id);

        Reply {
            payload_id: "migration_serialization",
            bytes: serialized_state,
        }
    }
}
