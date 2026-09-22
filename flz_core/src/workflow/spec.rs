//! The frozen registration-time result of authoring a workflow class: a
//! validated tasklet DAG (acyclic, every node reachable from an entry
//! channel) with one chain per entry channel, plus the
//! class's loop, rollback and durability facts.

use std::time::Duration;

use super::{LoopMode, RollbackMode};
use crate::channel::{ChannelId, ChannelKind, TaskletId};
use crate::operative::topology::{OperativeTopology, TaskletDescriptor};

/// The tasklet DAG in Kahn order from the entry channels' consumers, so
/// every producer sits at a lower index than its consumer. A node's index is
/// its `stage_idx` and its wire `task_idx`, and reversing the order is the
/// compensation order. Each entry channel roots a chain: a message runs only
/// the chain of the channel it arrived on.
#[derive(Debug, Default)]
pub struct TaskDag {
    pub order: Vec<TaskletId>,
    pub parents: Vec<Vec<usize>>,
    pub children: Vec<Vec<usize>>,
    pub chains: Vec<EntryChain>,
}

/// One entry channel's chain: the positions it feeds, everything reachable
/// from them, and each member's count of parents inside the chain, which is
/// what a join waits for in a pass on this channel.
#[derive(Debug, Clone)]
pub struct EntryChain {
    pub channel_id: ChannelId,
    pub roots: Vec<usize>,
    members: Vec<bool>,
    parents_in_chain: Vec<usize>,
}

impl EntryChain {
    pub fn contains(&self, node: usize) -> bool {
        self.members.get(node).copied().unwrap_or(false)
    }

    /// The producers of `node` that run in a pass on this channel.
    pub fn parents_in_chain(&self, node: usize) -> usize {
        self.parents_in_chain.get(node).copied().unwrap_or(0)
    }

    /// The chain's nodes with no children.
    pub fn leaves<'a>(&'a self, dag: &'a TaskDag) -> impl Iterator<Item = usize> + 'a {
        dag.leaves().filter(|&leaf| self.contains(leaf))
    }

    /// The chain's single leaf, when there is exactly one.
    pub fn single_leaf(&self, dag: &TaskDag) -> Option<usize> {
        let mut leaves = self.leaves(dag);
        let first = leaves.next()?;
        leaves.next().is_none().then_some(first)
    }
}

impl TaskDag {
    /// The DAG over positions, with one chain per entry channel computed by
    /// reachability from the positions that channel feeds. `entries` pairs
    /// each entry channel with the positions of its consumers.
    pub fn new(
        order: Vec<TaskletId>,
        parents: Vec<Vec<usize>>,
        children: Vec<Vec<usize>>,
        entries: impl IntoIterator<Item = (ChannelId, Vec<usize>)>,
    ) -> Self {
        let chains = entries
            .into_iter()
            .map(|(channel_id, roots)| {
                let members = reachable(&children, roots.iter().copied());
                let parents_in_chain = parents
                    .iter()
                    .map(|ps| ps.iter().filter(|&&p| members[p]).count())
                    .collect();
                EntryChain {
                    channel_id,
                    roots,
                    members,
                    parents_in_chain,
                }
            })
            .collect();
        Self {
            order,
            parents,
            children,
            chains,
        }
    }

    /// The DAG a walk over every tasklet found, re-indexed positionally so
    /// stage `i`'s parents and children are stage indices too, each list
    /// ascending as a level's nodes commit, with one chain per entry channel
    /// from the positions `entries` feed.
    pub fn from_walk(walk: GraphOrder, entries: Vec<(ChannelId, Vec<TaskletId>)>) -> Self {
        let positions = |nodes: &[usize]| {
            let mut at: Vec<usize> = nodes.iter().map(|&node| walk.pos[node]).collect();
            at.sort_unstable();
            at
        };
        let parents = walk
            .order
            .iter()
            .map(|&id| positions(&walk.parents[id as usize]))
            .collect();
        let children = walk
            .order
            .iter()
            .map(|&id| positions(&walk.children[id as usize]))
            .collect();
        let entries = entries.into_iter().map(|(channel_id, consumers)| {
            let roots = consumers.iter().map(|&id| walk.pos[id as usize]).collect();
            (channel_id, roots)
        });
        Self::new(walk.order, parents, children, entries)
    }

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

    /// The chain the entry channel `channel_id` roots.
    pub fn chain(&self, channel_id: ChannelId) -> Option<&EntryChain> {
        self.chains.iter().find(|c| c.channel_id == channel_id)
    }

    /// The position tasklet `tasklet_id` holds, its `stage_idx`.
    pub fn node_of(&self, tasklet_id: TaskletId) -> Option<usize> {
        self.order.iter().position(|&id| id == tasklet_id)
    }

    /// Whether `node` is fed by an entry channel.
    pub fn is_root(&self, node: usize) -> bool {
        self.chains.iter().any(|c| c.roots.contains(&node))
    }

    /// The first chain a handoff-chained loop cannot carry forward: one with
    /// several leaves, so no single terminal batch.
    pub fn branching_chain(&self) -> Option<&EntryChain> {
        self.chains
            .iter()
            .find(|chain| chain.single_leaf(self).is_none())
    }

    /// The first chain's terminal whose output is not an inter-tasklet
    /// channel, so it releases nothing a handoff-chained loop's journal
    /// carries into the next iteration.
    pub fn terminal_without_handoff<'t>(
        &self,
        topology: &'t OperativeTopology,
    ) -> Option<&'t TaskletDescriptor> {
        self.chains
            .iter()
            .filter_map(|chain| chain.single_leaf(self))
            .filter_map(|leaf| topology.tasklet(self.order[leaf]))
            .find(|terminal| {
                !terminal
                    .output_channel_id
                    .and_then(|id| topology.channel(id))
                    .is_some_and(|channel| channel.kind == ChannelKind::InterTasklet)
            })
    }
}

/// Where a graph walk starts. `Roots` is the entry-rooted shape a structured
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
fn reachable(children: &[Vec<usize>], roots: impl Iterator<Item = usize>) -> Vec<bool> {
    let mut reached = vec![false; children.len()];
    let mut stack: Vec<usize> = roots.collect();
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
    let reached = reachable(&children, roots.iter().map(|&r| r as usize));
    let unreachable = (0..n).find(|&i| !reached[i]).map(|i| i as TaskletId);

    // A tasklet several entry channels feed is one root, queued once.
    let mut queued = vec![false; n];
    let mut queue = std::collections::VecDeque::new();
    let seeds: Vec<usize> = match seed {
        GraphSeed::Roots => roots.iter().map(|&r| r as usize).collect(),
        GraphSeed::EveryIndegreeZero => (0..n).filter(|&i| indegree[i] == 0).collect(),
    };
    for node in seeds {
        if !std::mem::replace(&mut queued[node], true) {
            queue.push_back(node);
        }
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

/// What a workflow class declares beyond its topology, carried on it as
/// [`OperativeTopology::workflow`]: the one fact a class registry reads the
/// kind from.
#[derive(Debug)]
pub struct WorkflowSpec {
    pub dag: TaskDag,
    pub max_duration: Option<Duration>,
    pub rollback_mode: RollbackMode,
    pub loop_mode: LoopMode,
    pub wake_after: Option<Duration>,
    /// The controller ships a barrier to its durability backend before acking
    /// it; inert without a backend.
    pub sync_durability: bool,
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
         workflow must declare an input channel (c.input::<T>(...)) its messages arrive on"
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
        "workflow operative `{factory_id}` declares a handoff-chained loop over a branching \
         chain ({leaves} terminal tasklets); iteration N+1 is seeded from iteration N's \
         terminal batch, which a chain with several leaves does not define. Use \
         `LoopMode::PerInput`, or keep every chain of the loop converging on one terminal"
    )]
    BranchingChainedLoop {
        factory_id: &'static str,
        leaves: usize,
    },

    #[error(
        "workflow operative `{factory_id}` is not terminable: its only possible exit is an external \
         stop. Add a terminal condition: let the chain complete, have a tasklet call \
         ctx.complete, or declare max_duration()."
    )]
    ExternalStopOnly { factory_id: &'static str },

    #[error(
        "workflow operative `{factory_id}` declares tasklet `{tasklet}` that is not reachable \
         from any entry channel; every tasklet must hang off a chain rooted at one"
    )]
    Disconnected {
        factory_id: &'static str,
        tasklet: String,
    },

    #[error(
        "EffectivelyOnceOnWorkflow: workflow operative `{factory_id}` declares \
         effectively_once(); a workflow commits at its stage boundary and carries no tier"
    )]
    EffectivelyOnceOnWorkflow { factory_id: &'static str },

    #[error(
        "CheckpointOnWorkflow: workflow operative `{factory_id}` declares a checkpoint \
         (checkpoint_ack, checkpoint_every, outputs_at_checkpoint); a workflow commits at its \
         stage boundary"
    )]
    CheckpointOnWorkflow { factory_id: &'static str },

    #[error(
        "CorrectionWindowOnWorkflow: workflow operative `{factory_id}` declares \
         correction_window(); correction replays a stream's input log, which a workflow has not"
    )]
    CorrectionWindowOnWorkflow { factory_id: &'static str },

    #[error(
        "workflow operative `{factory_id}` declares no tasklets; a workflow runs a graph of \
         tasklets from its entry channels, declared in channels() on the builder a stream uses"
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
