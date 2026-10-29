use std::time::Duration;

use super::cell_state::relay_not_found;
use super::{CommandAction, OffReaderAction, OffReaderAnswer, OffReaderBuild, Reply};
use crate::pb::command_request::Command;
use crate::pb::RemRelay;
use crate::relay::instance::RelayInstance;
use crate::runner::RunnerNode;

impl CommandAction for RemRelay {
    fn handle(
        &self,
        runner: &mut RunnerNode,
        _message: &str,
        errors: &mut Vec<crate::Error>,
    ) -> Reply {
        if !runner.stop_relay(&self.relay_id) {
            // A draining stop's leave, begun for a relay removed meanwhile.
            crate::host::end_leave(&self.relay_id);
            errors.push(relay_not_found(&self.relay_id));
        }
        Reply {
            payload_id: "stop_relay",
            bytes: Vec::new(),
        }
    }
}

/// A stop that drains first runs off the control reader; any other stops at
/// once.
pub(super) fn classify(stop: RemRelay) -> super::CommandActionType {
    if stop.drain_within_ms > 0 {
        super::CommandActionType::OffReader(Box::new(stop))
    } else {
        super::CommandActionType::Regular(Box::new(stop))
    }
}

/// A stop that drains first, for a relay whose input is not journaled: a
/// start of the same id is held from here, the wait runs on the worker, and
/// the stop itself goes back to the coordinator as one that does not drain.
impl OffReaderAction for RemRelay {
    fn prepare(&self, runner: &mut RunnerNode) -> Result<OffReaderBuild, crate::Error> {
        let instance = runner
            .get_relay(&self.relay_id)
            .ok_or_else(|| relay_not_found(&self.relay_id))?;
        crate::host::begin_leave(&self.relay_id);
        let within = Duration::from_millis(self.drain_within_ms);
        let relay_id = self.relay_id.clone();
        Ok(Box::new(move || {
            drain(&instance, within);
            OffReaderAnswer::Coordinator(Box::new(Command::RemRelay(RemRelay {
                relay_id,
                drain_within_ms: 0,
            })))
        }))
    }
}

/// Wait until the instance has read the stop marker, holds no input it was
/// sent and has flushed every record, then send the final marker behind
/// them. Out of time, the stop goes on without it.
fn drain(instance: &RelayInstance, within: Duration) {
    let drained = || {
        instance
            .stop_marker
            .load(std::sync::atomic::Ordering::Acquire)
            && instance.quiet_for_stop()
            && crate::prefix::send_final(&instance.instance_id)
    };
    if !crate::util::wait_until(within, Duration::from_millis(1), drained) {
        flz_transport::notice!(
            Warn,
            "relay {}: not drained within {} ms; stopping without it",
            instance.instance_id,
            within.as_millis()
        );
    }
}
