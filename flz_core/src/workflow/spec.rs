//! The frozen registration-time result of authoring a workflow class: a
//! validated tasklet DAG (single-rooted, acyclic, no dynamic cells),
//! plus the class's loop, rollback and durability facts.

use std::sync::Arc;
use std::time::Duration;

use super::{LoopMode, RollbackMode};
use crate::channel::{RetryPolicy, TaskletId};
use crate::operative::topology::OperativeTopology;

/// The tasklet DAG in Kahn order from the root, so every producer sits at
/// a lower index than its consumer. A node's index is its `stage_idx` and
/// its wire `task_idx`, and reversing the order is the compensation order.
#[derive(Debug, Default)]
pub struct TaskDag {
    pub order: Vec<TaskletId>,
    pub parents: Vec<Vec<usize>>,
    pub children: Vec<Vec<usize>>,
}

impl TaskDag {
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

/// Where a graph walk starts. `Roots` is the single-rooted shape a structured
/// workflow declares, so a node the roots do not reach is left out of the
/// order; `EveryIndegreeZero` is the stream shape, where a disconnected node
/// still takes a position and unreachability is reported separately.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum GraphSeed {
    Roots,
    EveryIndegreeZero,
}

/// One graph-shape walk over the tasklet nodes: Kahn topological order plus
/// reachability from the roots. Both declaration surfaces read their shape
/// refusals off this — the rule is one rule, so its mechanism is one
/// mechanism, and each caller keeps its own refusal wording.
pub struct GraphOrder {
    /// Node ids in Kahn topological order (FIFO — for a tree this is exactly
    /// BFS from the root, so workflow `task_idx` numbering stays stable).
    pub order: Vec<TaskletId>,
    /// Node id → position in `order`; `usize::MAX` for a node the walk left
    /// out.
    pub pos: Vec<usize>,
    /// Incoming edges per node, indexed by node id.
    pub parents: Vec<Vec<usize>>,
    /// Outgoing edges per node, indexed by node id.
    pub children: Vec<Vec<usize>>,
    /// First node unreachable from the roots, if any (meaningful only when
    /// the graph is acyclic — a cycle already explains unreachability).
    pub unreachable: Option<TaskletId>,
    /// First node the walk left over — on (or strictly downstream of) a cycle.
    pub on_cycle: Option<TaskletId>,
}

/// The edge tables one walk needs: outgoing, incoming, and the incoming count.
fn adjacency(
    n: usize,
    edges: &[(TaskletId, TaskletId)],
) -> (Vec<Vec<usize>>, Vec<Vec<usize>>, Vec<usize>) {
    let mut parents: Vec<Vec<usize>> = vec![Vec::new(); n];
    let mut children: Vec<Vec<usize>> = vec![Vec::new(); n];
    let mut indegree = vec![0usize; n];
    for &(producer, consumer) in edges {
        children[producer as usize].push(consumer as usize);
        parents[consumer as usize].push(producer as usize);
        indegree[consumer as usize] += 1;
    }
    (parents, children, indegree)
}

/// Which nodes `roots` reach, following `children`.
fn reachable(children: &[Vec<usize>], roots: &[TaskletId]) -> Vec<bool> {
    let mut reached = vec![false; children.len()];
    let mut stack: Vec<usize> = roots.iter().map(|&r| r as usize).collect();
    while let Some(node) = stack.pop() {
        if std::mem::replace(&mut reached[node], true) {
            continue;
        }
        stack.extend(children[node].iter().copied());
    }
    reached
}

/// Walk `n` nodes over `edges` (producer → consumer pairs), reachable from
/// `roots`. Merge mappings are irrelevant to shape, so an edge is a pair of
/// tasklet ids.
pub fn graph_order(
    n: usize,
    edges: &[(TaskletId, TaskletId)],
    roots: &[TaskletId],
    seed: GraphSeed,
) -> GraphOrder {
    let (parents, children, mut indegree) = adjacency(n, edges);
    let reached = reachable(&children, roots);
    let unreachable = (0..n).find(|&i| !reached[i]).map(|i| i as TaskletId);

    let mut queued = vec![false; n];
    let mut queue: std::collections::VecDeque<usize> = match seed {
        GraphSeed::Roots => roots.iter().map(|&r| r as usize).collect(),
        GraphSeed::EveryIndegreeZero => (0..n).filter(|&i| indegree[i] == 0).collect(),
    };
    for &node in &queue {
        queued[node] = true;
    }
    let mut order = Vec::with_capacity(n);
    let mut pos = vec![usize::MAX; n];
    while let Some(node) = queue.pop_front() {
        pos[node] = order.len();
        order.push(node as TaskletId);
        for &child in &children[node] {
            indegree[child] = indegree[child].saturating_sub(1);
            if indegree[child] == 0 && !queued[child] {
                queued[child] = true;
                queue.push_back(child);
            }
        }
    }
    let on_cycle = (0..n)
        .find(|&i| pos[i] == usize::MAX)
        .map(|i| i as TaskletId);

    GraphOrder {
        order,
        pos,
        parents,
        children,
        unreachable,
        on_cycle,
    }
}

/// A workflow class as the registry holds it.
pub struct WorkflowSpec {
    pub topology: Arc<OperativeTopology>,
    pub dag: TaskDag,
    pub max_duration: Option<Duration>,
    pub rollback_mode: RollbackMode,
    pub loop_mode: LoopMode,
    pub wake_after: Option<Duration>,
    /// The per-cell ladder inside a stage.
    pub retry: RetryPolicy,
    /// Sends of one barrier before an unacked commit fails the run.
    pub barrier_retry_attempts: u32,
    pub persistence_enabled: bool,
    /// The controller ships a barrier to its durability backend before acking
    /// it; inert without a backend.
    pub sync_durability: bool,
}

// Manual: `OperativeTopology` holds closures and is not `Debug`. A summary is
// enough for test assertions (`expect`/`expect_err`) and diagnostics.
impl std::fmt::Debug for WorkflowSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkflowSpec")
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
        "workflow operative `{factory_id}` declares tasklets but no entry channel; a structured \
         workflow must declare one input channel (c.input::<T>(...)) to seed its first tasklet"
    )]
    NoEntryChannel { factory_id: &'static str },

    #[error(
        "workflow operative `{factory_id}` declares tasklet `{tasklet}` on a cycle; a \
         bounded topology is a DAG — a tasklet's output may never reach its own input"
    )]
    Cycle {
        factory_id: &'static str,
        tasklet: String,
    },

    #[error(
        "workflow operative `{factory_id}` declares tasklet `{tasklet}` as the producer of \
         more than one channel; a tasklet emits on exactly one output channel (its `Output` \
         type), so a second one could never be routed. Fan out by declaring several consumers on \
         the one channel"
    )]
    MultipleOutputChannels {
        factory_id: &'static str,
        tasklet: String,
    },

    #[error(
        "workflow operative `{factory_id}` declares entry channel `{channel}` feeding {consumers} \
         tasklets; a bounded topology has a single root — the entry channel must feed exactly \
         one tasklet, which then fans out"
    )]
    EntryRoot {
        factory_id: &'static str,
        channel: String,
        consumers: usize,
    },

    #[error(
        "workflow operative `{factory_id}` merges entry channel `{channel}` into tasklet \
         `{tasklet}`; a workflow operative is seeded from its spawn args (and, for a `PerInput` \
         loop, its inbox items) through ONE entry channel, and neither carries the channel a \
         payload arrived on — so an entry channel cannot be one merge source among several. \
         Merge the output streams of two or more of this operative's tasklets instead"
    )]
    MergeFromEntryChannel {
        factory_id: &'static str,
        channel: String,
        tasklet: String,
    },

    #[error(
        "workflow operative `{factory_id}` declares a handoff-chained loop over a branching \
         topology ({leaves} terminal tasklets); iteration N+1 is seeded from iteration N's \
         terminal batch, which a graph with several leaves does not define. Use \
         `LoopMode::PerInput`, or keep the chained loop converging on one terminal"
    )]
    BranchingChainedLoop {
        factory_id: &'static str,
        leaves: usize,
    },

    #[error(
        "workflow operative `{factory_id}` declares a dynamic-cell tasklet `{tasklet}`; \
         workflow operatives do not support dynamic cell mode"
    )]
    DynamicCells {
        factory_id: &'static str,
        tasklet: String,
    },

    #[error(
        "workflow operative `{factory_id}` is not terminable: its only possible exit is an external \
         stop. Add a terminal condition: let the chain complete, have a tasklet call \
         ctx.complete, or declare max_duration()."
    )]
    ExternalStopOnly { factory_id: &'static str },

    #[error(
        "workflow operative `{factory_id}` declares tasklet `{tasklet}` that is not reachable \
         from the entry channel; every tasklet must hang off the one graph rooted there"
    )]
    Disconnected {
        factory_id: &'static str,
        tasklet: String,
    },

    #[error(
        "workflow operative `{factory_id}` declares a retry ladder ({attempts} attempts) with a \
         zero backoff base; retries would re-run the failing cell in a hot loop. Give \
         retry_backoff() a non-zero base, or set retry_attempts() to 0 for fail-fast"
    )]
    ZeroRetryBackoff {
        factory_id: &'static str,
        attempts: u32,
    },

    #[error(
        "workflow operative `{factory_id}` declares no tasklets; a workflow runs a graph of \
         tasklets from one entry channel, declared in tasklets() on the builder a stream uses"
    )]
    NoTasklets { factory_id: &'static str },

    #[error(
        "workflow operative `{factory_id}` is a durable handoff-chained loop, but its terminal \
         tasklet `{tasklet}` releases no inter-tasklet handoff; iteration N+1 is seeded \
         from iteration N's terminal batch, and a tasklet that owns no output channel, or whose \
         only output is plugin-bound, has nothing the journal carries. Give the terminal \
         tasklet an output channel that is not plugin-bound — taking the sink off its own \
         channel is enough — or switch to `LoopMode::PerInput`"
    )]
    LoopTerminalWithoutHandoff {
        factory_id: &'static str,
        tasklet: String,
    },
}
