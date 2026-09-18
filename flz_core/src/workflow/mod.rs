//! The **workflow** operative kind: spawned with `args`, run to an exit
//! trigger, torn down. It drives the stream building blocks (procedures,
//! channels, entities, `ctx.emit`) under a lifecycle: a procedure DAG, an
//! inbox for post-start input, and exit triggers (the chain finishing, a
//! procedure's `complete`, a loop running out of work, `max_duration`, an
//! external stop) that all converge on one `WorkflowCompleted`.

mod commit;
pub mod compensation;
pub mod driver;
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

/// How a structured chain repeats. Every iteration ends with a durable
/// commit, so a crash resumes after the last committed one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoopMode {
    /// One pass, seeded from the spawn args.
    Once,
    /// One iteration per inbox item. A durable instance idle past the
    /// threshold at a committed boundary parks until the next input or its
    /// wake deadline.
    PerInput,
    /// One iteration per pass, each seeded by the previous terminal handoff;
    /// ends on an empty batch. Never parks.
    Chained,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RollbackMode {
    /// Any failure undoes every completed procedure in reverse. The default.
    AllOrNothing,
    /// Committed iterations are kept; a failure undoes only the in-flight one.
    KeepProgress,
}

/// The refusal a procedure outside a workflow earns by calling `complete`.
pub const COMPLETE_OUTSIDE_WORKFLOW: &str =
    "CompleteOutsideWorkflow: only a procedure running inside a workflow operative can complete it; a stream operative never completes";

/// A workflow failure, retryable by default: inside a structured procedure
/// it climbs the per-entity ladder first.
#[derive(Debug, Clone)]
pub struct WorkflowError {
    pub message: String,
    pub non_retryable: bool,
}

impl WorkflowError {
    /// A permanent failure: no retry, the procedure fails at once.
    pub fn non_retryable(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            non_retryable: true,
        }
    }

    pub fn new(message: impl Into<String>, non_retryable: bool) -> Self {
        Self {
            message: message.into(),
            non_retryable,
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

/// The sink [`crate::entity::ProcedureDispatcher::run_collecting`] drives:
/// inter-procedure output is collected for the next stage, plugin-bound
/// output staged for the procedure's outbox.
pub trait WorkflowCollectSink {
    fn collect(&mut self, value: Arc<dyn Any + Send + Sync>);

    /// A serialized plugin-bound output on `channel`: staged until the
    /// barrier for a journaling run, straight to the wire otherwise.
    fn emit_plugin_bytes(&mut self, channel: &str, bytes: Vec<u8>);

    /// The procedure asked to end its instance with `result` once the
    /// iteration commits; a later call in the same step replaces it.
    fn complete(&mut self, result: Vec<u8>);
}
