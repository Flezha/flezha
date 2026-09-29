//! The **bounded operator** operator kind: spawned with `init_values` as initial
//! state per tasklet, exactly as an unbounded operator is, fed by messages on its entry
//! channels through the unbounded operator's entrance, run to an exit trigger, torn
//! down. Its stages are unbounded Cells fed through the unbounded operator's routing and
//! mailboxes; what is its own is a pass sheet on the instance, advanced by
//! events: a tasklet DAG with one chain per entry channel, a commit per
//! stage, and exit triggers (the chain finishing, a tasklet's `complete`, a
//! loop running out of work, `max_duration`, an external stop) that all
//! converge on one `BoundedCompleted`.

mod commit;
pub mod compensation;
#[cfg(all(test, unix))]
mod driver_tests;
pub mod instance;
mod resume;
mod run;
pub mod spec;

use std::fmt;

pub use instance::{BoundedRun, Deadline, Resume};
pub use run::{
    attempt_failed, complete, entry_parked, hold_emit, item_settled, message, note_failure, open,
    removed, restore_landed, ship_stage_record, stop, unwired_stream,
};
pub(crate) use run::{
    committed_firings, crossed, deadline_due, firing_assembled, firing_promoted, firings_joined,
    held_removals, hold_removal, items_dropped, pass_dead, reap_holds,
};

/// How a structured chain repeats. Every message-fed pass waits for its
/// message the same way: a durable instance idle past the threshold at a
/// committed boundary, the one before its first pass included, parks until
/// the next message or its wake deadline. Every iteration ends with a
/// durable commit, so a crash resumes after the last committed one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoopMode {
    /// One pass over the first message, then done.
    Once,
    /// One iteration per message.
    PerInput,
    /// The first pass over the first message; every later one seeded by the
    /// previous terminal handoff into the chain that message picked, ending
    /// on an empty batch.
    Chained,
}

impl LoopMode {
    /// The name a class declares its loop mode by, on the wire and in a
    /// refusal.
    pub fn as_str(self) -> &'static str {
        match self {
            LoopMode::Once => "once",
            LoopMode::PerInput => "per_input",
            LoopMode::Chained => "chained",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RollbackMode {
    /// Any failure undoes every completed tasklet in reverse. The default.
    AllOrNothing,
    /// Committed iterations are kept; a failure undoes only the in-flight one.
    /// In a `PerInput` loop a failed turn is dead-lettered and undone, and the
    /// loop takes its next message.
    KeepProgress,
}

/// The refusal a tasklet outside a bounded operator earns by calling
/// `complete`. It fails that input as the author's own non-retryable error
/// would, so it travels as the error's text, which names the code.
pub fn complete_outside_bounded() -> flz_transport::errors::FlezhaError {
    crate::Code::CompleteOutsideBounded.with([])
}

/// A bounded operator failure, retryable by default: inside a structured tasklet
/// it climbs the per-cell ladder first.
#[derive(Debug, Clone)]
pub struct BoundedError {
    pub message: String,
    pub non_retryable: bool,
    /// A host's entry decoder could not read the message: once the ladder
    /// exhausts, the message parks and the run goes on waiting rather than
    /// ending.
    pub unreadable: bool,
}

impl BoundedError {
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

impl fmt::Display for BoundedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl From<String> for BoundedError {
    fn from(s: String) -> Self {
        BoundedError::new(s, false)
    }
}

impl From<&str> for BoundedError {
    fn from(s: &str) -> Self {
        BoundedError::new(s, false)
    }
}
