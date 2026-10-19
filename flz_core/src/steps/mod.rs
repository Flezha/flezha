//! A relay that runs in steps (`Run::Steps`): spawned with `init_values`
//! as initial state per tasklet, exactly as a flow relay is, fed by
//! messages on its entry channels through the same entrance, run to an exit
//! trigger, torn down. Its stages are Cells fed through the flow path's
//! routing and mailboxes; what is its own is a pass sheet on the instance,
//! advanced by events: a tasklet DAG with one chain per entry channel, a
//! commit per stage, and exit triggers (the chain finishing, a tasklet's
//! `complete`, a loop running out of work, `max_duration`, an external stop)
//! that all converge on one `RelayCompleted`.

mod commit;
pub mod compensation;
#[cfg(all(test, unix))]
mod driver_tests;
pub mod instance;
mod resume;
mod run;
pub mod spec;

use std::fmt;

pub use instance::{Deadline, Resume, StepsRun};
pub use run::{
    attempt_failed, complete, entry_parked, hold_emit, item_settled, message, note_failure, open,
    pass_call, pass_offset, removed, restore_landed, results_landed, ship_stage_record, stop,
    unwired_stream,
};
pub(crate) use run::{
    committed_firings, crossed, deadline_due, firing_assembled, firing_promoted, firings_joined,
    held_removals, hold_arm, hold_removal, items_dropped, pass_dead, reap_holds,
};

pub use crate::relay::settings::{Each, OnFailure};

/// A steps relay failure, retryable by default: inside a structured tasklet
/// it climbs the per-cell ladder first.
#[derive(Debug, Clone)]
pub struct StepsError {
    pub message: String,
    pub non_retryable: bool,
    /// A host's entry decoder could not read the message: once the ladder
    /// exhausts, the message parks and the run goes on waiting rather than
    /// ending.
    pub unreadable: bool,
}

impl StepsError {
    /// A permanent failure: no retry, the tasklet fails at once.
    pub fn non_retryable(message: impl Into<String>) -> Self {
        Self::new(message, true)
    }

    /// A message the entry decoder rejected.
    pub fn unreadable(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            non_retryable: false,
            unreadable: true,
        }
    }

    pub fn new(message: impl Into<String>, non_retryable: bool) -> Self {
        Self {
            message: message.into(),
            non_retryable,
            unreadable: false,
        }
    }
}

impl fmt::Display for StepsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl From<String> for StepsError {
    fn from(s: String) -> Self {
        StepsError::new(s, false)
    }
}

impl From<&str> for StepsError {
    fn from(s: &str) -> Self {
        StepsError::new(s, false)
    }
}
