//! `MigrationSerialization` — produces the per-Tasklet / per-Cell
//! state blob, with each Cell's change seq and pending timers
//! (`serialize_moved`), then removes the Relay instance from the local
//! registry (the controller is migrating it elsewhere). One that drains
//! first does so as a draining stop does ([`super::relay_stop`]).

use super::relay_stop::{drain_then, remove};
use super::{CommandAction, OffReaderAction, OffReaderBuild, Reply};
use crate::command::cell_state::serialize_moved;
use crate::pb::command_request::Command;
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
            Some(instance) => serialize_moved(&instance),
            None => Vec::new(),
        };

        remove(runner, &self.relay_id);

        Reply {
            payload_id: "migration_serialization",
            bytes: serialized_state,
        }
    }
}

impl OffReaderAction for MigrationSerialization {
    fn prepare(&self, runner: &mut RunnerNode) -> Result<OffReaderBuild, crate::Error> {
        let then = Command::MigrationSerialization(MigrationSerialization {
            relay_id: self.relay_id.clone(),
            drain_within_ms: 0,
        });
        drain_then(runner, &self.relay_id, self.drain_within_ms, then)
    }
}
