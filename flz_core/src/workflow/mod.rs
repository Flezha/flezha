//! The **workflow** compute-unit kind: an on-demand, terminating,
//! possibly-interactive process.
//!
//! A workflow operative is spawned with `args`, runs to an exit trigger, and is
//! then torn down. It reuses the stream **building blocks** (Procedures,
//! Channels, Entities, per-Entity state, `ctx.emit`) but drives them under a
//! workflow **lifecycle**:
//!
//! - **`run` sugar** — the trivial case is a bare
//!   [`run`](WorkflowOperative::run) body: a single implicit Procedure/Entity.
//! - **Structured** — override [`procedures`](WorkflowOperative::procedures) to
//!   author Procedures/Channels/Entities on the same `ChannelBuilder` a
//!   stream operative uses; the workflow driver runs them **sequentially in
//!   a linear chain** with a per-procedure **count-based barrier** (entities
//!   within one procedure still run in parallel) — see [`driver`].
//! - **Exit triggers** — the operative terminates on the first of: the chain
//!   running to quiescence, [`WorkflowCtx::complete`], a loop's terminal
//!   predicate, a declared [`max_duration`](WorkflowOperative::max_duration), or
//!   external `flz operative stop` ([`WorkflowStop`](crate::pb::WorkflowStop)).
//!   Every trigger converges on the single `WorkflowCompleted` teardown.
//! - **Inbox** — a running operative receives input **after it starts** via
//!   [`WorkflowCtx::recv`] (today's drop-on-arrival is gone).
//!
//! ```ignore
//! struct Greet;
//! impl flezha_runner::WorkflowOperative for Greet {
//!     fn factory_id() -> &'static str { "greet" }
//!     fn run(ctx: &mut flezha_runner::WorkflowCtx, args: &str) -> Result<(), flezha_runner::WorkflowError> {
//!         ctx.complete(format!("hello {args}").into_bytes());
//!         Ok(())
//!     }
//! }
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! flezha_runner::Flezha::create_runner("my_app")?.define_workflow::<Greet>()?.run()?;
//! # Ok(())
//! # }
//! ```

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

/// Frame-metadata key carrying a journaled input's durable-inbox offset
/// (`workflow-durable-suspension`). Stamped by the controller when it appends
/// the input to the instance's `inbox.wal`, before forwarding it.
pub const INBOX_OFFSET_KEY: &str = "flz.inbox_offset";

/// How a structured workflow chain repeats (`workflow-durable-loop`). Every
/// iteration ends with a durable iteration commit — journal, boundary snapshot,
/// outbox flush — so a crash resumes at the iteration **after** the last
/// committed one, never at the loop start.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoopMode {
    /// Not a loop: one pass of the chain, seeded from the spawn args. The
    /// default, and the only mode a `run`-sugar class may declare.
    Once,
    /// One iteration per inbox item (the chat-turn shape): each iteration is
    /// seeded by the next post-start input through the entry channel. A
    /// durable instance whose inbox stays empty past the idle threshold at a
    /// committed boundary **parks** — it is evicted from the runner and
    /// rehydrated by the next input or its wake deadline
    /// (`workflow-durable-suspension`).
    PerInput,
    /// One iteration per pass of the chain, each seeded by the previous
    /// iteration's terminal handoff (the refinement-loop shape). Iteration 0 is
    /// seeded from the spawn args; the loop ends when an iteration releases an
    /// empty batch. Never parks — it always has its own next input.
    Chained,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RollbackMode {
    /// Any failure undoes **all** completed procedures in reverse, back to the
    /// start (the classic saga). The default.
    AllOrNothing,
    /// Each finished loop iteration commits (is kept); a failure undoes only the
    /// in-flight iteration's completed procedures. Within a single iteration —
    /// or a no-loop pass — rollback is all-or-nothing.
    KeepProgress,
}

/// Type-erased run-sugar entrypoint stored in the class spec. `T::run` coerces
/// to this `fn` pointer because it takes no `self`.
pub type WorkflowRunFn = fn(&mut WorkflowCtx, &str) -> Result<(), WorkflowError>;

/// A workflow failure. Retryable by default: inside a structured procedure it
/// first passes through the class's per-entity retry ladder
/// (`workflow-entity-retry`), and only a `non_retryable` error — or one that
/// exhausted the ladder — fails the procedure.
#[derive(Debug, Clone)]
pub struct WorkflowError {
    pub message: String,
    /// Skip the retry ladder: this failure can never succeed on a re-run.
    pub non_retryable: bool,
}

impl WorkflowError {
    /// Classify a failure as permanent (validation error, 4xx-class response):
    /// the entity is not retried and the procedure fails immediately. Mirrors
    /// [`ProcedureError::non_retryable`](crate::procedure::ProcedureError::non_retryable),
    /// which a procedure body inside an entity returns to the same effect.
    pub fn non_retryable(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            non_retryable: true,
        }
    }

    /// Build with an explicit classification (dispatcher seam: carries an
    /// procedure's own `NonRetryable` marker through to the workflow ladder).
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

/// Collecting sink driven by [`crate::entity::ProcedureDispatcher::run_collecting`]:
/// inter-procedure output is **collected** for handoff to the next stage at the
/// barrier; plugin-bound output is **staged** (serialized) for the procedure's
/// transactional outbox. The workflow driver implements this per running entity.
pub trait WorkflowCollectSink {
    /// Collect an inter-procedure output value for the next stage's input batch.
    fn collect(&mut self, value: Arc<dyn Any + Send + Sync>);

    /// Take an already-serialized plugin-bound output produced on `channel`.
    /// For a durable structured operative the bytes are **staged** until the
    /// procedure's barrier and travel to the controller inside `ProcCommit`, so
    /// nothing reaches a plugin before the proc's journal entry is durable
    /// (`workflow-transactional-outbox`); an opted-out class forwards straight
    /// to the wire (at-least-once, as before).
    fn emit_plugin_bytes(&mut self, channel: &str, bytes: Vec<u8>);
}

/// Lifecycle-aware context handed to [`WorkflowOperative::run`] (the run-sugar
/// path). `emit` writes a frame tagged with the instance id on a secondary
/// stream that the controller fans out to the unit's plugins; `recv` drains
/// the inbox (input delivered after start); `loop_until` repeats a body until a
/// terminal predicate or cancellation (duration / external stop); `complete`
/// sets the terminal result.
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

    /// Emit one output payload to the unit's bound plugins. Best-effort and
    /// at-least-once, like operative output. Routed through the coordinator so
    /// the stream write stays single-threaded.
    ///
    /// This is the `run`-sugar path only: with no procedures there is no
    /// barrier to stage against and no journal entry to gate a flush on, so the
    /// emit goes straight to the wire. A structured procedure's `ctx.emit`
    /// instead stages into the procedure's transactional outbox
    /// (`workflow-transactional-outbox`).
    pub fn emit(&mut self, bytes: impl Into<Vec<u8>>) -> Result<(), WorkflowError> {
        crate::runner::emit_workflow_output(self.instance_id.to_string(), bytes.into());
        Ok(())
    }

    /// Request terminal completion with `result`. The body should return
    /// shortly after; the driver tears down with status `ok` and `result` in
    /// the `WorkflowCompleted` payload. Loops observe completion automatically.
    pub fn complete(&mut self, result: impl Into<Vec<u8>>) {
        self.result = result.into();
        self.completed = true;
    }

    /// True once an external stop / `max_duration` has tripped the
    /// cancellation token. Long-running bodies should check this and unwind.
    pub fn is_cancelled(&self) -> bool {
        self.cancel.load(Ordering::Acquire)
    }

    /// Block for the next inbox item (input delivered after start). Returns
    /// `None` when cancellation trips or the inbox is permanently closed.
    /// Sleeps while idle (≤200 ms wake granularity to honour cancellation) —
    /// never busy-spins.
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

    /// Repeat `body` until `predicate` returns `true` OR the cancellation
    /// token trips (duration / external stop) OR [`complete`](Self::complete)
    /// is called. The construct observes cancellation automatically and blocks
    /// while idle (a body that waits on [`recv`](Self::recv) sleeps rather than
    /// spinning). `predicate` provides the required non-timeout exit, so a loop
    /// authored through this construct can always terminate without an external
    /// kill.
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

    /// Consume the context, returning whether `complete` was called and the
    /// result payload. Driver-only.
    pub fn into_result(self) -> (bool, Vec<u8>) {
        (self.completed, self.result)
    }
}
