//! The frozen registration-time result of authoring a bounded class: a
//! validated tasklet DAG (acyclic, every node reachable from an entry
//! channel) with one chain per entry channel, plus the
//! class's loop, rollback and durability facts.

use std::time::Duration;

use flz_transport::errors::{ErrorModel, FlezhaError};
use serde_json::{Map, Value};

use super::{LoopMode, RollbackMode};
use crate::channel::{ChannelId, ChannelKind, TaskletId};
use crate::operator::topology::{OperatorTopology, TaskletDescriptor};
use crate::Code;

/// The tasklet DAG in Kahn order from the entry channels' consumers, so
/// every producer sits at a lower index than its consumer. A node's index is
/// its `stage_idx` and its wire `task_idx`, and reversing the order is the
/// compensation order. A pass runs one chain: an entry channel's, for a
/// message that arrived on it, or a tasklet's, for a timer firing it set.
#[derive(Debug, Default)]
pub struct TaskDag {
    pub order: Vec<TaskletId>,
    pub parents: Vec<Vec<usize>>,
    pub children: Vec<Vec<usize>>,
    /// One chain per entry channel.
    pub entries: Vec<(ChannelId, Chain)>,
    /// One chain per tasklet, rooted at it, indexed by position.
    pub from_node: Vec<Chain>,
}

/// What one pass runs: its roots, everything reachable from them, and each
/// member's count of parents inside the chain, which is what a join waits
/// for in that pass.
#[derive(Debug, Clone)]
pub struct Chain {
    pub roots: Vec<usize>,
    members: Vec<bool>,
    parents_in_chain: Vec<usize>,
}

impl Chain {
    fn reaching(parents: &[Vec<usize>], children: &[Vec<usize>], roots: Vec<usize>) -> Self {
        let members = reachable(children, roots.iter().copied());
        let parents_in_chain = parents
            .iter()
            .map(|ps| ps.iter().filter(|&&p| members[p]).count())
            .collect();
        Self {
            roots,
            members,
            parents_in_chain,
        }
    }

    pub fn contains(&self, node: usize) -> bool {
        self.members.get(node).copied().unwrap_or(false)
    }

    /// The producers of `node` that run in a pass on this chain.
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
    /// The DAG over positions, with one chain per entry channel and one per
    /// tasklet, each computed by reachability from its roots. `entries`
    /// pairs each entry channel with the positions of its consumers.
    pub fn new(
        order: Vec<TaskletId>,
        parents: Vec<Vec<usize>>,
        children: Vec<Vec<usize>>,
        entries: impl IntoIterator<Item = (ChannelId, Vec<usize>)>,
    ) -> Self {
        let entries = entries
            .into_iter()
            .map(|(channel_id, roots)| (channel_id, Chain::reaching(&parents, &children, roots)))
            .collect();
        let from_node = (0..order.len())
            .map(|node| Chain::reaching(&parents, &children, vec![node]))
            .collect();
        Self {
            order,
            parents,
            children,
            entries,
            from_node,
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
    pub fn chain(&self, channel_id: ChannelId) -> Option<&Chain> {
        self.entries
            .iter()
            .find(|(id, _)| *id == channel_id)
            .map(|(_, chain)| chain)
    }

    /// The position tasklet `tasklet_id` holds, its `stage_idx`.
    pub fn node_of(&self, tasklet_id: TaskletId) -> Option<usize> {
        self.order.iter().position(|&id| id == tasklet_id)
    }

    /// Whether `node` is fed by an entry channel.
    pub fn is_root(&self, node: usize) -> bool {
        self.entries.iter().any(|(_, c)| c.roots.contains(&node))
    }

    /// The first chain a handoff-chained loop cannot carry forward: one with
    /// several leaves, so no single terminal batch.
    pub fn branching_chain(&self) -> Option<(ChannelId, &Chain)> {
        self.entries
            .iter()
            .find(|(_, chain)| chain.single_leaf(self).is_none())
            .map(|(channel_id, chain)| (*channel_id, chain))
    }

    /// The first chain's terminal whose output is not an inter-tasklet
    /// channel, so it releases nothing a handoff-chained loop's journal
    /// carries into the next iteration.
    pub fn terminal_without_handoff<'t>(
        &self,
        topology: &'t OperatorTopology,
    ) -> Option<&'t TaskletDescriptor> {
        self.entries
            .iter()
            .filter_map(|(_, chain)| chain.single_leaf(self))
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
/// bounded operator declares, so a node the roots do not reach is left out of the
/// order; `EveryIndegreeZero` is the unbounded shape, where a disconnected node
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
    /// BFS from the root, so bounded operator `task_idx` numbering stays stable).
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

/// What a bounded class declares beyond its topology, carried on it as
/// [`OperatorTopology::bounded`]: the one fact a class registry reads the
/// kind from.
#[derive(Debug)]
pub struct BoundedSpec {
    pub dag: TaskDag,
    pub max_duration: Option<Duration>,
    pub rollback_mode: RollbackMode,
    pub loop_mode: LoopMode,
    /// The controller ships a barrier to its durability backend before acking
    /// it; inert without a backend.
    pub sync_durability: bool,
}

/// Why a bounded class is refused at registration.
#[derive(Debug)]
pub enum BuildBoundedError {
    /// The shared topology's own refusal, carried as the core rendered it.
    Topology {
        factory_id: &'static str,
        error: FlezhaError,
    },
    NoEntryChannel {
        factory_id: &'static str,
    },
    Cycle {
        factory_id: &'static str,
        tasklet: String,
    },
    MultipleOutputChannels {
        factory_id: &'static str,
        tasklet: String,
    },
    BranchingChainedLoop {
        factory_id: &'static str,
        channel: String,
        leaves: usize,
    },
    /// Not self-terminating and no `max_duration`: only an external stop ends it.
    ExternalStopOnly {
        factory_id: &'static str,
    },
    Disconnected {
        factory_id: &'static str,
        tasklet: String,
    },
    EffectivelyOnceOnBounded {
        factory_id: &'static str,
    },
    CheckpointOnBounded {
        factory_id: &'static str,
    },
    CorrectionWindowOnBounded {
        factory_id: &'static str,
    },
    NoTasklets {
        factory_id: &'static str,
    },
    LoopTerminalWithoutHandoff {
        factory_id: &'static str,
        tasklet: String,
    },
    /// A call end on a class that is not `:per_input`, which never reads the
    /// later message its `Response` is.
    CallOutsidePerInputLoop {
        factory_id: &'static str,
        channel: String,
    },
}

impl ErrorModel for BuildBoundedError {
    /// The refusal in the model's one shape, naming the factory.
    fn to_error(&self) -> FlezhaError {
        match self.judged() {
            Ok((code, details)) => code.error(details),
            Err(error) => error.clone(),
        }
    }
}

impl BuildBoundedError {
    /// This refusal's code and details, or the shared topology's own error.
    fn judged(&self) -> Result<(Code, Map<String, Value>), &FlezhaError> {
        let tasklet = |tasklet: &String| vec![("tasklet", Value::from(tasklet.as_str()))];
        let (factory_id, code, pairs) = match self {
            Self::Topology { error, .. } => return Err(error),
            Self::NoEntryChannel { factory_id } => (factory_id, Code::NoEntryChannel, vec![]),
            Self::ExternalStopOnly { factory_id } => (factory_id, Code::ExternalStopOnly, vec![]),
            Self::EffectivelyOnceOnBounded { factory_id } => {
                (factory_id, Code::EffectivelyOnceOnBounded, vec![])
            }
            Self::CheckpointOnBounded { factory_id } => {
                (factory_id, Code::CheckpointOnBounded, vec![])
            }
            Self::CorrectionWindowOnBounded { factory_id } => {
                (factory_id, Code::CorrectionWindowOnBounded, vec![])
            }
            Self::NoTasklets { factory_id } => (factory_id, Code::NoTasklets, vec![]),
            Self::Cycle {
                factory_id,
                tasklet: t,
            } => (factory_id, Code::Cycle, tasklet(t)),
            Self::MultipleOutputChannels {
                factory_id,
                tasklet: t,
            } => (factory_id, Code::MultipleOutputChannels, tasklet(t)),
            Self::Disconnected {
                factory_id,
                tasklet: t,
            } => (factory_id, Code::UnreachableTasklet, tasklet(t)),
            Self::LoopTerminalWithoutHandoff {
                factory_id,
                tasklet: t,
            } => (
                factory_id,
                Code::ChainedLoopTerminalWithoutHandoff,
                tasklet(t),
            ),
            Self::CallOutsidePerInputLoop {
                factory_id,
                channel,
            } => (
                factory_id,
                Code::CallOutsidePerInputLoop,
                vec![("channel", channel.as_str().into())],
            ),
            Self::BranchingChainedLoop {
                factory_id,
                channel,
                leaves,
            } => (
                factory_id,
                Code::BranchingChainedLoop,
                vec![
                    ("channel", channel.as_str().into()),
                    ("leaves", (*leaves).into()),
                ],
            ),
        };
        let mut details: Map<String, Value> = pairs
            .into_iter()
            .map(|(key, value)| (key.to_string(), value))
            .collect();
        details.insert("factory_id".into(), (*factory_id).into());
        Ok((code, details))
    }
}

impl std::fmt::Display for BuildBoundedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_error().message)
    }
}

impl std::error::Error for BuildBoundedError {}
