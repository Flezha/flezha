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
        if !remove(runner, &self.relay_id) {
            errors.push(relay_not_found(&self.relay_id));
        }
        Reply {
            payload_id: "stop_relay",
            bytes: Vec::new(),
        }
    }
}

/// Remove `relay_id`; false when it is not here.
pub(super) fn remove(runner: &mut RunnerNode, relay_id: &str) -> bool {
    let removed = runner.stop_relay(relay_id);
    if !removed {
        // A draining command's leave, begun for a relay removed meanwhile.
        crate::host::end_leave(relay_id);
    }
    removed
}

/// A command that drains its relay first runs off the control reader; any
/// other runs at once.
pub(super) fn classify<C>(drain_within_ms: u64, command: C) -> super::CommandActionType
where
    C: CommandAction + OffReaderAction + 'static,
{
    if drain_within_ms > 0 {
        super::CommandActionType::OffReader(Box::new(command))
    } else {
        super::CommandActionType::Regular(Box::new(command))
    }
}

impl OffReaderAction for RemRelay {
    fn prepare(&self, runner: &mut RunnerNode) -> Result<OffReaderBuild, crate::Error> {
        let then = Command::RemRelay(RemRelay {
            relay_id: self.relay_id.clone(),
            drain_within_ms: 0,
        });
        drain_then(runner, &self.relay_id, self.drain_within_ms, then)
    }
}

/// A command that drains first, for a relay whose input is not journaled: a
/// start of the same id is held from here, the wait runs on the worker, and
/// `then`, the command as one that does not drain, goes back to the
/// coordinator.
pub(super) fn drain_then(
    runner: &mut RunnerNode,
    relay_id: &str,
    drain_within_ms: u64,
    then: Command,
) -> Result<OffReaderBuild, crate::Error> {
    let instance = runner
        .get_relay(relay_id)
        .ok_or_else(|| relay_not_found(relay_id))?;
    crate::host::begin_leave(relay_id);
    let within = Duration::from_millis(drain_within_ms);
    Ok(Box::new(move || {
        drain(&instance, within);
        OffReaderAnswer::Coordinator(Box::new(then))
    }))
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
