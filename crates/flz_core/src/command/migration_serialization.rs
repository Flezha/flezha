//! `MigrationSerialization` — produces the per-Tasklet / per-Cell
//! state blob per `relay-mvp-rust/design.md` Decision 10, then removes
//! the Relay instance from the local registry (the controller is migrating
//! it elsewhere).

use super::{CommandAction, Reply};
use crate::command::cell_state::serialize_relay_state;
use crate::pb::MigrationSerialization;
use crate::runner::RunnerNode;

impl CommandAction for MigrationSerialization {
    fn handle(
        &self,
        runner: &mut RunnerNode,
        _message: &str,
        _errors: &mut Vec<crate::Error>,
    ) -> Reply {
        let serialized_state = match runner.get_relay(&self.relay_id) {
            Some(instance) => serialize_relay_state(&instance),
            None => Vec::new(),
        };

        runner.stop_relay(&self.relay_id);

        Reply {
            payload_id: "migration_serialization",
            bytes: serialized_state,
        }
    }
}
