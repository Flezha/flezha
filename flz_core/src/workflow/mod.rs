//! The **workflow** operative kind: spawned with `args`, run to an exit
//! trigger, torn down. It drives the stream building blocks (procedures,
//! channels, entities, `ctx.emit`) under a lifecycle — a `run`-sugar body or
//! a structured procedure DAG, an inbox for post-start input, and exit
//! triggers (quiescence, `complete`, a loop's terminal predicate,
//! `max_duration`, an external stop) that all converge on one
//! `WorkflowCompleted`.

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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crossbeam::channel::{Receiver, RecvTimeoutError};

use self::instance::InboxItem;

/// Frame-metadata key of a journaled input's durable-inbox offset, stamped
/// by the controller before forwarding.
pub const INBOX_OFFSET_KEY: &str = "flz.inbox_offset";

/// How a structured chain repeats. Every iteration ends with a durable
/// commit, so a crash resumes after the last committed one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoopMode {
    /// One pass, seeded from the spawn args; the only mode a `run`-sugar
    /// class may declare.
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

/// The run-sugar entrypoint as the class spec stores it.
pub type WorkflowRunFn = fn(&mut WorkflowCtx, &str) -> Result<(), WorkflowError>;

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
}

/// The context a `run`-sugar body receives.
pub struct WorkflowCtx {
    instance_id: Arc<str>,
    cancel: Arc<AtomicBool>,
    completed: bool,
    result: Vec<u8>,
    inbox: Receiver<InboxItem>,
}

impl WorkflowCtx {
    pub fn new(instance_id: Arc<str>, cancel: Arc<AtomicBool>, inbox: Receiver<InboxItem>) -> Self {
        Self {
            instance_id,
            cancel,
            completed: false,
            result: Vec::new(),
            inbox,
        }
    }

    /// Emit one output to the bound plugins, at-least-once and straight to the
    /// wire: a `run`-sugar body has no barrier to stage against.
    pub fn emit(&mut self, bytes: impl Into<Vec<u8>>) -> Result<(), WorkflowError> {
        crate::runner::emit_workflow_output(self.instance_id.to_string(), bytes.into());
        Ok(())
    }

    /// Request terminal completion with `result`; loops observe it.
    pub fn complete(&mut self, result: impl Into<Vec<u8>>) {
        self.result = result.into();
        self.completed = true;
    }

    /// True once an external stop or `max_duration` tripped the token.
    pub fn is_cancelled(&self) -> bool {
        self.cancel.load(Ordering::Acquire)
    }

    /// Block for the next inbox item; `None` on cancellation or a closed
    /// inbox.
    pub fn recv(&mut self) -> Option<Vec<u8>> {
        loop {
            if self.is_cancelled() || self.completed {
                return None;
            }
            match self.inbox.recv_timeout(Duration::from_millis(200)) {
                Ok(item) => return Some(item.payload),
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => return None,
            }
        }
    }

    /// Repeat `body` until `predicate` holds, the token trips, or
    /// [`complete`](Self::complete) is called.
    pub fn loop_until<P, B>(&mut self, mut predicate: P, mut body: B) -> Result<(), WorkflowError>
    where
        P: FnMut() -> bool,
        B: FnMut(&mut WorkflowCtx) -> Result<(), WorkflowError>,
    {
        while !self.is_cancelled() && !self.completed && !predicate() {
            body(self)?;
        }
        Ok(())
    }

    /// Whether `complete` was called, and the result. Driver-only.
    pub fn into_result(self) -> (bool, Vec<u8>) {
        (self.completed, self.result)
    }
}
