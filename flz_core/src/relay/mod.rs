//! The relay kernel: topology, instance, registry.

pub mod completion;
pub mod instance;
pub mod registry;
pub mod settings;
pub mod sleep;
#[cfg(all(test, unix))]
mod sleep_tests;
pub mod topology;

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
