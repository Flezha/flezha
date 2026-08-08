//! Bounded class spec: the frozen authoring result produced at registration.
//!
//! A bounded operative is authored either as bare [`run`](super::BoundedOperative::run)
//! sugar (no declared procedures) or as a structured **DAG** of
//! Procedures/Channels/Entities via
//! [`procedures`](super::BoundedOperative::procedures). `build_spec` runs the
//! declaration once, validates it (single-rooted acyclic graph — fan-out and
//! fan-in both legal, no dynamic entities, self-terminating), and freezes the
//! result for every spawn of the class.

use std::sync::Arc;
use std::time::Duration;

use super::{BoundedRunFn, LoopMode, RollbackMode};
use crate::channel::{ProcedureId, RetryPolicy};
use crate::operative::topology::OperativeTopology;

/// The static procedure DAG of a structured bounded class (`bounded-branching`).
///
/// `order` holds every procedure in **Kahn topological order from the root**,
/// so every producer of a node — a fan-in has several — sits at a lower index
/// than the node itself. A node's index is also its `stage_idx` and its wire
/// `proc_idx`. That numbering is load-bearing: completion order is topological,
/// and reversing it (what `plan_compensation` already does) is
/// reverse-topological compensation — a join is undone before any of its
/// producers. A linear chain is the single-branch case.
#[derive(Debug, Default)]
pub struct ProcDag {
    pub order: Vec<ProcedureId>,
    /// Indexes of each node's producers (empty for the root), positionally.
    pub parents: Vec<Vec<usize>>,
    /// Indexes of each node's children — the consumers of its output channel.
    pub children: Vec<Vec<usize>>,
}

impl ProcDag {
    pub fn len(&self) -> usize {
        self.order.len()
    }

    /// The nodes with no children — the operative is done when all of them
    /// have committed.
    pub fn leaves(&self) -> impl Iterator<Item = usize> + '_ {
        (0..self.len()).filter(|&i| self.children[i].is_empty())
    }

    /// The single leaf, when the graph has exactly one — the terminal procedure
    /// whose batch a handoff-chained loop re-seeds from, and whose output
    /// channel an iteration commit journals.
    pub fn single_leaf(&self) -> Option<usize> {
        let mut leaves = self.leaves();
        let first = leaves.next()?;
        leaves.next().is_none().then_some(first)
    }
}

/// Frozen per-class authoring result stored in the registry. `topology` is
/// `None` for the run-sugar form; `Some` (with the resolved `tree`) for the
/// structured form.
pub struct BoundedSpec {
    /// Run-sugar body. Invoked only when `topology` is `None`.
    pub run: BoundedRunFn,
    /// Structured topology, or `None` for run-sugar.
    pub topology: Option<Arc<OperativeTopology>>,
    /// Procedures as a static DAG rooted at the entry channel's consumer.
    /// Empty for run-sugar.
    pub dag: ProcDag,
    /// Optional wall-clock backstop applied by the driver.
    pub max_duration: Option<Duration>,
    /// Saga rollback scope applied on abnormal termination.
    pub rollback_mode: RollbackMode,
    /// Loop shape of the structured chain (`LoopMode::Once` for run-sugar).
    pub loop_mode: LoopMode,
    /// Durable wake deadline armed when a `PerInput` loop parks.
    pub wake_after: Option<Duration>,
    /// Per-entity retry ladder applied inside a structured stage
    /// (`bounded-entity-retry`), on the same policy type the unbounded
    /// retry-then-park loop uses.
    pub retry: RetryPolicy,
    /// Class-level persistence toggle; when true, a structured bounded's
    /// per-Entity Changes are flushed to the WAL as they apply.
    pub persistence_enabled: bool,
}

// Manual: `OperativeTopology` holds closures and is not `Debug`. A summary is
// enough for test assertions (`expect`/`expect_err`) and diagnostics.
impl std::fmt::Debug for BoundedSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BoundedSpec")
            .field("structured", &self.topology.is_some())
            .field("dag_len", &self.dag.len())
            .field("max_duration", &self.max_duration)
            .field("rollback_mode", &self.rollback_mode)
            .field("loop_mode", &self.loop_mode)
            .field("retry", &self.retry)
            .field("persistence_enabled", &self.persistence_enabled)
            .finish()
    }
}

/// Registration-time rejections for a bounded operative authoring.
#[derive(Debug, thiserror::Error)]
pub enum BuildBoundedError {
    #[error("bounded operative `{factory_id}` has an invalid topology: {detail}")]
    Topology {
        factory_id: &'static str,
        detail: String,
    },

    #[error(
        "bounded operative `{factory_id}` declares procedures but no entry channel; a structured \
         bounded must declare one input channel (c.input::<T>(...)) to seed its first procedure"
    )]
    NoEntryChannel { factory_id: &'static str },

    #[error(
        "bounded operative `{factory_id}` declares procedure `{procedure}` on a cycle; a \
         bounded topology is a DAG — a procedure's output may never reach its own input"
    )]
    Cycle {
        factory_id: &'static str,
        procedure: String,
    },

    #[error(
        "bounded operative `{factory_id}` declares procedure `{procedure}` as the producer of \
         more than one channel; a procedure emits on exactly one output channel (its `Output` \
         type), so a second one could never be routed. Fan out by declaring several consumers on \
         the one channel"
    )]
    MultipleOutputChannels {
        factory_id: &'static str,
        procedure: String,
    },

    #[error(
        "bounded operative `{factory_id}` declares entry channel `{channel}` feeding {consumers} \
         procedures; a bounded topology has a single root — the entry channel must feed exactly \
         one procedure, which then fans out"
    )]
    EntryRoot {
        factory_id: &'static str,
        channel: String,
        consumers: usize,
    },

    #[error(
        "bounded operative `{factory_id}` merges entry channel `{channel}` into procedure \
         `{procedure}`; a bounded operative is seeded from its spawn args (and, for a `PerInput` \
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
        "bounded operative `{factory_id}` declares a handoff-chained loop over a branching \
         topology ({leaves} terminal procedures); iteration N+1 is seeded from iteration N's \
         terminal batch, which a graph with several leaves does not define. Use \
         `LoopMode::PerInput`, or keep the chained loop converging on one terminal"
    )]
    BranchingChainedLoop {
        factory_id: &'static str,
        leaves: usize,
    },

    #[error(
        "bounded operative `{factory_id}` declares a dynamic-entity procedure `{procedure}`; \
         bounded operatives do not support dynamic entity mode"
    )]
    DynamicEntities {
        factory_id: &'static str,
        procedure: String,
    },

    #[error(
        "bounded operative `{factory_id}` is not terminable: its only possible exit is an external \
         stop. Add a terminal condition — let run()/the chain complete, give the loop a terminal \
         predicate, or declare max_duration()."
    )]
    ExternalStopOnly { factory_id: &'static str },

    #[error(
        "bounded operative `{factory_id}` declares procedure `{procedure}` that is not reachable \
         from the entry channel; every procedure must hang off the one graph rooted there"
    )]
    Disconnected {
        factory_id: &'static str,
        procedure: String,
    },

    #[error(
        "bounded operative `{factory_id}` declares a retry ladder ({attempts} attempts) with a \
         zero backoff base; retries would re-run the failing entity in a hot loop. Give \
         retry_backoff() a non-zero base, or set retry_attempts() to 0 for fail-fast"
    )]
    ZeroRetryBackoff {
        factory_id: &'static str,
        attempts: u32,
    },

    #[error(
        "bounded operative `{factory_id}` declares a chain loop but no procedures; a loop iterates \
         a structured chain — declare procedures() (c.input::<T>(...) + procedures), or drop \
         loop_mode() and let the run() body drive its own loop_until"
    )]
    LoopWithoutChain { factory_id: &'static str },

    #[error(
        "bounded operative `{factory_id}` is durable but its inter-procedure channel `{channel}` \
         has no byte codec; declare `.codec()` (standard serde payloads) or `.codec_with(...)` \
         so the proc-journal can persist and re-seed the stage handoff, or opt the class out with \
         `PERSISTENCE_ENABLED = false`"
    )]
    MissingInterProcCodec {
        factory_id: &'static str,
        channel: String,
    },

    #[error(
        "bounded operative `{factory_id}` is a durable handoff-chained loop, but its terminal \
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
