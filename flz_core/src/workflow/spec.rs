//! The frozen registration-time result of authoring a workflow class: a
//! run-sugar body or a validated procedure DAG (single-rooted, acyclic, no
//! dynamic entities), plus the class's loop, rollback and durability facts.

use std::sync::Arc;
use std::time::Duration;

use super::{LoopMode, RollbackMode, WorkflowRunFn};
use crate::channel::{ProcedureId, RetryPolicy};
use crate::operative::topology::OperativeTopology;

/// The procedure DAG in Kahn order from the root, so every producer sits at
/// a lower index than its consumer. A node's index is its `stage_idx` and
/// its wire `proc_idx`, and reversing the order is the compensation order.
#[derive(Debug, Default)]
pub struct ProcDag {
    pub order: Vec<ProcedureId>,
    pub parents: Vec<Vec<usize>>,
    pub children: Vec<Vec<usize>>,
}

impl ProcDag {
    pub fn len(&self) -> usize {
        self.order.len()
    }

    pub fn is_empty(&self) -> bool {
        self.order.is_empty()
    }

    /// The nodes with no children.
    pub fn leaves(&self) -> impl Iterator<Item = usize> + '_ {
        (0..self.len()).filter(|&i| self.children[i].is_empty())
    }

    /// The single leaf, when there is exactly one.
    pub fn single_leaf(&self) -> Option<usize> {
        let mut leaves = self.leaves();
        let first = leaves.next()?;
        leaves.next().is_none().then_some(first)
    }
}

/// A workflow class as the registry holds it; `topology` is `None` for the
/// run-sugar form.
pub struct WorkflowSpec {
    /// The SDK language; a run-sugar class has no topology to carry it.
    pub sdk_lang: u8,
    pub run: WorkflowRunFn,
    pub topology: Option<Arc<OperativeTopology>>,
    pub dag: ProcDag,
    pub max_duration: Option<Duration>,
    pub rollback_mode: RollbackMode,
    pub loop_mode: LoopMode,
    pub wake_after: Option<Duration>,
    /// The per-entity ladder inside a stage.
    pub retry: RetryPolicy,
    /// Sends of one barrier before an unacked commit fails the run.
    pub barrier_retry_attempts: u32,
    pub persistence_enabled: bool,
    /// The controller ships a barrier to its durability backend before acking
    /// it; inert without a backend.
    pub sync_durability: bool,
    /// A host class: every body crosses through the poll surface, and the
    /// driver never touches `run`.
    pub host: bool,
}

// Manual: `OperativeTopology` holds closures and is not `Debug`. A summary is
// enough for test assertions (`expect`/`expect_err`) and diagnostics.
impl std::fmt::Debug for WorkflowSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkflowSpec")
            .field("structured", &self.topology.is_some())
            .field("dag_len", &self.dag.len())
            .field("max_duration", &self.max_duration)
            .field("rollback_mode", &self.rollback_mode)
            .field("loop_mode", &self.loop_mode)
            .field("retry", &self.retry)
            .field("barrier_retry_attempts", &self.barrier_retry_attempts)
            .field("persistence_enabled", &self.persistence_enabled)
            .field("sync_durability", &self.sync_durability)
            .finish()
    }
}

/// Why a workflow class is refused at registration.
#[derive(Debug, thiserror::Error)]
pub enum BuildWorkflowError {
    #[error("workflow operative `{factory_id}` has an invalid topology: {detail}")]
    Topology {
        factory_id: &'static str,
        detail: String,
    },

    #[error(
        "workflow operative `{factory_id}` declares procedures but no entry channel; a structured \
         workflow must declare one input channel (c.input::<T>(...)) to seed its first procedure"
    )]
    NoEntryChannel { factory_id: &'static str },

    #[error(
        "workflow operative `{factory_id}` declares procedure `{procedure}` on a cycle; a \
         bounded topology is a DAG — a procedure's output may never reach its own input"
    )]
    Cycle {
        factory_id: &'static str,
        procedure: String,
    },

    #[error(
        "workflow operative `{factory_id}` declares procedure `{procedure}` as the producer of \
         more than one channel; a procedure emits on exactly one output channel (its `Output` \
         type), so a second one could never be routed. Fan out by declaring several consumers on \
         the one channel"
    )]
    MultipleOutputChannels {
        factory_id: &'static str,
        procedure: String,
    },

    #[error(
        "workflow operative `{factory_id}` declares entry channel `{channel}` feeding {consumers} \
         procedures; a bounded topology has a single root — the entry channel must feed exactly \
         one procedure, which then fans out"
    )]
    EntryRoot {
        factory_id: &'static str,
        channel: String,
        consumers: usize,
    },

    #[error(
        "workflow operative `{factory_id}` merges entry channel `{channel}` into procedure \
         `{procedure}`; a workflow operative is seeded from its spawn args (and, for a `PerInput` \
         loop, its inbox items) through ONE entry channel, and neither carries the channel a \
         payload arrived on — so an entry channel cannot be one merge source among several. \
         Merge the output streams of two or more of this operative's procedures instead"
    )]
    MergeFromEntryChannel {
        factory_id: &'static str,
        channel: String,
        procedure: String,
    },

    #[error(
        "workflow operative `{factory_id}` declares a handoff-chained loop over a branching \
         topology ({leaves} terminal procedures); iteration N+1 is seeded from iteration N's \
         terminal batch, which a graph with several leaves does not define. Use \
         `LoopMode::PerInput`, or keep the chained loop converging on one terminal"
    )]
    BranchingChainedLoop {
        factory_id: &'static str,
        leaves: usize,
    },

    #[error(
        "workflow operative `{factory_id}` declares a dynamic-entity procedure `{procedure}`; \
         workflow operatives do not support dynamic entity mode"
    )]
    DynamicEntities {
        factory_id: &'static str,
        procedure: String,
    },

    #[error(
        "workflow operative `{factory_id}` is not terminable: its only possible exit is an external \
         stop. Add a terminal condition — let run()/the chain complete, give the loop a terminal \
         predicate, or declare max_duration()."
    )]
    ExternalStopOnly { factory_id: &'static str },

    #[error(
        "workflow operative `{factory_id}` declares procedure `{procedure}` that is not reachable \
         from the entry channel; every procedure must hang off the one graph rooted there"
    )]
    Disconnected {
        factory_id: &'static str,
        procedure: String,
    },

    #[error(
        "workflow operative `{factory_id}` declares a retry ladder ({attempts} attempts) with a \
         zero backoff base; retries would re-run the failing entity in a hot loop. Give \
         retry_backoff() a non-zero base, or set retry_attempts() to 0 for fail-fast"
    )]
    ZeroRetryBackoff {
        factory_id: &'static str,
        attempts: u32,
    },

    #[error(
        "workflow operative `{factory_id}` declares a chain loop but no procedures; a loop iterates \
         a structured chain — declare procedures() (c.input::<T>(...) + procedures), or drop \
         loop_mode() and let the run() body drive its own loop_until"
    )]
    LoopWithoutChain { factory_id: &'static str },

    #[error(
        "workflow operative `{factory_id}` is durable but its inter-procedure channel `{channel}` \
         has no byte codec; declare `.codec()` (standard serde payloads) or `.codec_with(...)` \
         so the proc-journal can persist and re-seed the stage handoff, or opt the class out with \
         `PERSISTENCE_ENABLED = false`"
    )]
    MissingInterProcCodec {
        factory_id: &'static str,
        channel: String,
    },

    #[error(
        "workflow operative `{factory_id}` is a durable handoff-chained loop, but its terminal \
         procedure `{procedure}` releases no codec'd output channel; iteration N+1 is seeded from \
         iteration N's terminal batch, which the iteration commit must be able to journal. Give \
         the terminal procedure an output channel with `.codec()`, or switch to \
         `LoopMode::PerInput`"
    )]
    LoopTerminalWithoutCodec {
        factory_id: &'static str,
        procedure: String,
    },
}
