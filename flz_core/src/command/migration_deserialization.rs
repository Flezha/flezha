//! `MigrationDeserialization` — restore a StreamOperative instance's per-Tasklet /
//! per-Cell state from a snapshot blob and register it locally.
//! Reuses `apply_init_values` for the state-application path so a
//! migrated instance ends up with byte-identical Cell state to the
//! source instance produced by `serialize_operative_state`.

use super::{CommandAction, Reply};
use crate::command::cell_state::apply_init_values;
use crate::pb::MigrationDeserialization;
use crate::runner::RunnerNode;

impl CommandAction for MigrationDeserialization {
    fn handle(&self, runner: &mut RunnerNode, _message: &str, errors: &mut Vec<String>) -> Reply {
        // A hot-swap handover carries no partition identity: the destination's
        // `AddOperative` ran first and already registered the instance with
        // its range, so this call resolves to that instance.
        match runner.start_operative(
            self.factory_id.clone(),
            self.operative_id.clone(),
            crate::operative::instance::KeyGroupRange::UNPARTITIONED,
        ) {
            Some(instance) => {
                if !self.serialized_state.is_empty() {
                    let json_str = match std::str::from_utf8(&self.serialized_state) {
                        Ok(s) => s,
                        Err(e) => {
                            errors.push(format!(
                                "MigrationDeserialization: serialized_state is not UTF-8: {}",
                                e
                            ));
                            return Reply {
                                payload_id: "migration_deserialization",
                                bytes: Vec::new(),
                            };
                        }
                    };
                    if let Err(e) = apply_init_values(&*instance, json_str) {
                        errors.push(e.to_string());
                    }
                }
                Reply {
                    payload_id: "migration_deserialization",
                    bytes: Vec::new(),
                }
            }
            None => {
                errors.push(format!(
                    "StreamOperative class with factory_id {} is not registered",
                    self.factory_id
                ));
                Reply {
                    payload_id: "migration_deserialization",
                    bytes: Vec::new(),
                }
            }
        }
    }
}
