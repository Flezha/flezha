//! `MigrationDeserialization` — restore a Relay instance's per-Tasklet /
//! per-Cell state from a snapshot blob and register it locally.
//! `apply_moved` takes what `serialize_moved` shipped: the Cell state
//! through `apply_init_values`, so a migrated instance ends up byte-identical
//! to its source, and each Cell's change seq and pending timers.

use super::{CommandAction, Reply};
use crate::command::cell_state::apply_moved;
use crate::pb::MigrationDeserialization;
use crate::runner::RunnerNode;

impl CommandAction for MigrationDeserialization {
    fn handle(
        &self,
        runner: &mut RunnerNode,
        _message: &str,
        errors: &mut Vec<crate::Error>,
    ) -> Reply {
        // A hot-swap handover carries no partition identity: the destination's
        // `AddRelay` ran first and already registered the instance with
        // its range, so this call resolves to that instance.
        match runner.start_relay(
            self.factory_id.clone(),
            self.relay_id.clone(),
            crate::relay::instance::KeyGroupRange::UNPARTITIONED,
            crate::steps::Resume::default(),
        ) {
            Some(instance) => {
                if !self.serialized_state.is_empty() {
                    let json_str = match std::str::from_utf8(&self.serialized_state) {
                        Ok(s) => s,
                        Err(e) => {
                            errors.push(crate::Error::State(
                                crate::Code::MigrationUnreadable
                                    .with([("reason", e.to_string().into())]),
                            ));
                            return Reply {
                                payload_id: "migration_deserialization",
                                bytes: Vec::new(),
                            };
                        }
                    };
                    if let Err(e) = apply_moved(runner, &instance, json_str) {
                        errors.push(e);
                    }
                }
                Reply {
                    payload_id: "migration_deserialization",
                    bytes: Vec::new(),
                }
            }
            None => {
                errors.push(super::relay_start::class_not_registered(&self.factory_id));
                Reply {
                    payload_id: "migration_deserialization",
                    bytes: Vec::new(),
                }
            }
        }
    }
}
