//! The **workflow** operative kind: spawned with `init_values` as initial
//! state per tasklet, exactly as a stream is, fed by messages on its entry
//! channels through the stream's entrance, run to an exit trigger, torn
//! down. It drives the stream building blocks (tasklets, channels, cells,
//! `ctx.emit`) under a lifecycle: a tasklet DAG with one chain per entry
//! channel, an inbox the entrance fills, and exit triggers (the chain
//! finishing, a tasklet's `complete`, a loop running out of work,
//! `max_duration`, an external stop) that all converge on one
//! `WorkflowCompleted`.

mod commit;
pub mod compensation;
pub mod driver;
#[cfg(all(test, unix))]
mod driver_tests;
pub mod instance;
pub mod registry;
mod resume;
pub mod spec;
pub mod step;

use std::any::Any;
use std::fmt;
use std::sync::Arc;

/// Frame-metadata key of a journaled input's durable-inbox offset, stamped
/// by the controller before forwarding.
pub const INBOX_OFFSET_KEY: &str = "flz.inbox_offset";

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RollbackMode {
    /// Any failure undoes every completed tasklet in reverse. The default.
    AllOrNothing,
    /// Committed iterations are kept; a failure undoes only the in-flight one.
    KeepProgress,
}

/// The refusal a tasklet outside a workflow earns by calling `complete`.
pub const COMPLETE_OUTSIDE_WORKFLOW: &str =
    "CompleteOutsideWorkflow: only a tasklet running inside a workflow operative can complete it; a stream operative never completes";

/// A workflow failure, retryable by default: inside a structured tasklet
/// it climbs the per-cell ladder first.
#[derive(Debug, Clone)]
pub struct WorkflowError {
    pub message: String,
    pub non_retryable: bool,
    /// A host's entry decoder could not read the message: once the ladder
    /// exhausts, the message parks and the run goes on waiting rather than
    /// ending.
    pub unreadable: bool,
}

impl WorkflowError {
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

impl fmt::Display for WorkflowError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl From<String> for WorkflowError {
    fn from(s: String) -> Self {
        WorkflowError::new(s, false)
    }
}

impl From<&str> for WorkflowError {
    fn from(s: &str) -> Self {
        WorkflowError::new(s, false)
    }
}

/// The sink [`crate::cell::TaskletDispatcher::run_collecting`] drives:
/// inter-tasklet output is collected for the next stage, plugin-bound
/// output staged for the tasklet's outbox.
pub trait WorkflowCollectSink {
    fn collect(&mut self, value: Arc<dyn Any + Send + Sync>);

    /// A serialized plugin-bound output on `channel`: staged until the
    /// barrier for a journaling run, straight to the wire otherwise.
    fn emit_plugin_bytes(&mut self, channel: &str, bytes: Vec<u8>);

    /// The tasklet asked to end its instance with `result` once the
    /// iteration commits; a later call in the same step replaces it.
    fn complete(&mut self, result: Vec<u8>);
}
