//! The relay kernel: topology, instance, registry.

pub mod completion;
pub mod instance;
pub mod registry;
pub mod settings;
pub mod sleep;
#[cfg(all(test, unix))]
mod sleep_tests;
pub mod topology;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use instance::RelayInstance;

/// An instance's deadline on the poll core's deferred-wake list: `ParkIdle`
/// for either kind, the rest a steps run's.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Deadline {
    /// Waiting at a committed boundary long enough to suspend.
    ParkIdle,
    MaxDuration,
    /// A barrier's ack is overdue: re-send it, or give up on it.
    Ack(u64),
    /// Cell restores that never landed: the run ends `error`.
    RestoreGate,
    /// An undo found its Cell still handed out: try again.
    UndoGate,
    /// A message's item found its Cell's mailbox full: try again.
    Admit,
    /// A flow relay asked to end: report it once its Cells hold nothing.
    Complete,
}

/// Arm `deadline` `after` from now in place of the due `held` keeps (`0`
/// when none is armed), so a re-arm leaves one entry.
pub(crate) fn rearm(
    inst: &Arc<RelayInstance>,
    deadline: Deadline,
    held: &AtomicU64,
    after: Duration,
) {
    let due = crate::util::wall_now_ms().saturating_add(after.as_millis() as u64);
    let old = held.swap(due, Ordering::AcqRel);
    crate::poll_core::global().rearm_instance(
        Arc::downgrade(inst),
        inst.seq,
        deadline,
        (old != 0).then_some(old),
        due,
    );
}
