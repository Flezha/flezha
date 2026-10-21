//! The frozen registration-time result of authoring a steps class: a
//! validated tasklet DAG (acyclic, every node reachable from an entry
//! channel) with one chain per entry channel, plus the class's loop shape and
//! failure mode.

use super::{Each, OnFailure};
use crate::channel::{ChannelId, TaskletId};
use crate::relay::settings::Run;
use crate::relay::topology::{graph_order, reachable, GraphOrder, RelayTopology};

/// The tasklet DAG in Kahn order, so every producer sits at a lower index
/// than its consumer. A node's index is its `stage_idx` and its wire
/// `task_idx`, and reversing the order is the compensation order. A pass
/// runs one chain: an entry channel's, for a message that arrived on it, or
/// a tasklet's, for a timer firing it set.
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

    /// The DAG of a class `check` accepted: one walk over every
    /// producer-to-consumer edge, with one chain per entry channel.
    pub fn of(topology: &RelayTopology) -> Self {
        let entries = topology.entry_consumers();
        let edges: Vec<_> = topology
            .channels
            .iter()
            .filter_map(|c| c.producer.map(|p| (p, c)))
            .flat_map(|(p, c)| c.consumer_ids().map(move |id| (p, id)))
            .collect();
        Self::from_walk(
            graph_order(topology.tasklets.len(), &edges, &entries),
            entries,
        )
    }

    /// The DAG a walk over every tasklet found, re-indexed positionally so
    /// stage `i`'s parents and children are stage indices too, each list
    /// ascending as a level's nodes commit, with one chain per entry channel
    /// from the positions `entries` feed.
    pub(crate) fn from_walk(walk: GraphOrder, entries: Vec<(ChannelId, Vec<TaskletId>)>) -> Self {
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

    /// The first chain's terminal that `hands_off` says releases nothing a
    /// handoff-chained loop's journal carries into the next iteration.
    pub fn terminal_without_handoff(
        &self,
        hands_off: impl Fn(TaskletId) -> bool,
    ) -> Option<TaskletId> {
        self.entries
            .iter()
            .filter_map(|(_, chain)| chain.single_leaf(self))
            .map(|leaf| self.order[leaf])
            .find(|&terminal| !hands_off(terminal))
    }
}

/// What a steps class declares beyond its topology, carried on it as
/// `RelayTopology::steps`: the one fact a class registry reads the kind from.
#[derive(Debug)]
pub struct StepsSpec {
    pub dag: TaskDag,
    pub on_failure: OnFailure,
    pub each: Each,
}

impl StepsSpec {
    /// What a class running `run` carries beyond its topology: nothing for a
    /// flow class.
    pub fn of(topology: &RelayTopology, run: &Run) -> Option<Self> {
        let Run::Steps { each, on_failure } = *run else {
            return None;
        };
        Some(Self {
            dag: TaskDag::of(topology),
            on_failure,
            each,
        })
    }
}

#[cfg(test)]
#[path = "spec_tests.rs"]
mod tests;
