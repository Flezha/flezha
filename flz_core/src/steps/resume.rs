//! What the journal says about where a run resumes: its committed work as
//! stage records and the handoffs a resumed pass re-seeds its children from.
//! The invariant: a resumed chain sees exactly what the live one did: a
//! re-seeded value goes through the same edge, and a journaled iteration is
//! either wholly committed or wholly in flight.

use std::collections::{BTreeMap, HashMap};

use super::compensation::{CellRun, StageRecord};
use super::instance::{CommittedTask, Completion, JournalResume, MidResume, Release, Value};
use super::run::{unwired_stream, Run};
use super::spec::Chain;
use super::{StepsError, OnFailure};
use crate::channel::{ChannelId, ChannelKind};
use crate::operator::topology::OperatorTopology;

/// The chain a carry-fed pass re-enters: the one the loop's first message
/// picked, from the journaled entry channel.
pub(super) fn carried_channel(r: &Run<'_>, resume: &JournalResume) -> ChannelId {
    let topology = &r.inst.topology;
    topology
        .lookup_channel_id(&resume.entry_channel)
        .or_else(|| topology.first_of_kind(ChannelKind::Entry).map(|(id, _)| id))
        .expect("a structured steps declares an entry channel (validated at registration)")
}

/// A journaled handoff back into values through each item's producing
/// channel's codec; `only` restricts it to one edge.
pub(super) fn seed_from_journal(
    topology: &OperatorTopology,
    handoff: &[(u32, Vec<u8>)],
    only: Option<ChannelId>,
) -> Result<Vec<Value>, StepsError> {
    let mut seeded: Vec<Value> = Vec::with_capacity(handoff.len());
    for (channel_id, bytes) in handoff {
        if only.is_some_and(|edge| edge != *channel_id) {
            continue;
        }
        let codec = topology
            .channel(*channel_id)
            .and_then(|c| c.codec.as_ref())
            .ok_or_else(|| {
                StepsError::from(format!(
                    "journaled handoff references channel id {channel_id} without an inter-task \
                     codec; cannot re-seed the resumed tasklet"
                ))
            })?;
        let value = (codec.decode)(bytes.clone()).map_err(|e| {
            StepsError::from(format!(
                "journaled handoff decode failed on channel {channel_id}: {e}"
            ))
        })?;
        seeded.push(value);
    }
    Ok(seeded)
}

/// What a resumed pass starts from besides its roots: the tasklets that
/// committed before the crash, what they asked to complete with, and each
/// uncommitted child of a committed producer seeded from its journaled
/// handoff — ready, or banked while a producer in the chain is still owed.
#[derive(Default)]
pub(crate) struct Seed {
    pub(crate) committed_nodes: Vec<usize>,
    pub(crate) completion: Completion,
    pub(crate) ready: BTreeMap<usize, Vec<Release>>,
    pub(crate) banked: HashMap<usize, BTreeMap<usize, Vec<Release>>>,
    pub(crate) waiting: HashMap<usize, usize>,
}

/// A mid-iteration resume of a pass over `chain`: every uncommitted node of
/// it with a committed producer re-seeds from that producer's journaled
/// handoff on the producer's own channel, through the edge between them; a
/// join with a producer still uncommitted banks it and waits, counting only
/// the producers in this chain.
pub(super) fn resumed_seed(
    r: &Run<'_>,
    mid: &MidResume,
    chain: &Chain,
) -> Result<Seed, StepsError> {
    let dag = &r.spec.dag;
    let committed = &mid.committed_nodes;
    let journal = &mid.journal;
    let mut seed = Seed {
        committed_nodes: committed.clone(),
        ..Seed::default()
    };
    for task in &journal.committed_tasks {
        let node = task.task_idx as usize;
        if task.iter == journal.iter && committed.contains(&node) {
            seed.completion.note(node, task.completion.clone());
        }
    }
    let stream = unwired_stream();
    for node in 0..dag.len() {
        if !chain.contains(node) || committed.contains(&node) || chain.roots.contains(&node) {
            continue;
        }
        let parents: Vec<usize> = dag.parents[node]
            .iter()
            .copied()
            .filter(|p| chain.contains(*p))
            .collect();
        let committed_parents: Vec<usize> = parents
            .iter()
            .copied()
            .filter(|p| committed.contains(p))
            .collect();
        if committed_parents.is_empty() {
            // A node with no committed parent becomes runnable as its
            // producers commit.
            continue;
        }
        let mut by_parent: BTreeMap<usize, Vec<Release>> = BTreeMap::new();
        for &parent in &committed_parents {
            let handoff = journal
                .task_handoffs
                .iter()
                .find(|edge| edge.task_idx as usize == parent)
                .map(|edge| edge.handoff.as_slice())
                .unwrap_or_default();
            let producer_channel = r.task(parent).output_channel_id;
            let values = seed_from_journal(&r.inst.topology, handoff, producer_channel)?;
            let (Some(channel_id), target) = (producer_channel, dag.order[node]) else {
                continue;
            };
            let Some(descriptor) = r.inst.topology.channel(channel_id) else {
                continue;
            };
            let Some(edge) = descriptor.consumers.iter().find(|e| e.tasklet == target) else {
                continue;
            };
            let mut items = Vec::new();
            for value in &values {
                items.extend(r.address(descriptor, [edge], value, &stream, None)?);
            }
            by_parent.insert(parent, items);
        }
        let outstanding = parents.len() - committed_parents.len();
        if outstanding == 0 {
            seed.ready
                .insert(node, by_parent.into_values().flatten().collect());
        } else {
            seed.banked.insert(node, by_parent);
            seed.waiting.insert(node, outstanding);
        }
    }
    Ok(seed)
}

/// The journal's committed work as stage records: the fully-committed
/// iterations an all-or-nothing rollback would undo, and the in-flight
/// iteration's stages (skipped on resume, still rolled back). Outputs are
/// empty, as on any crash-resume.
pub(super) fn journaled_stages(
    r: &Run<'_>,
    resume: &JournalResume,
) -> (Vec<Vec<StageRecord>>, Vec<StageRecord>) {
    let undoable: Vec<CommittedTask> = resume
        .committed_tasks
        .iter()
        .filter(|task| {
            r.spec.on_failure == OnFailure::UndoAll || task.iter >= resume.iter
        })
        .cloned()
        .collect();
    let mut by_iter = stages_by_iter(r, &undoable);
    // The resume point splits the two: everything below it is committed progress.
    let in_flight = by_iter
        .split_off(&resume.iter)
        .remove(&resume.iter)
        .unwrap_or_default();
    (by_iter.into_values().collect(), in_flight)
}

/// Journaled commits as stage records, grouped by iteration and ordered by
/// chain position; shared by the forward resume and the rewind undo.
pub(super) fn stages_by_iter(
    r: &Run<'_>,
    tasks: &[CommittedTask],
) -> BTreeMap<u32, Vec<StageRecord>> {
    let mut by_iter: BTreeMap<u32, Vec<StageRecord>> = BTreeMap::new();
    for task in tasks {
        if let Some(record) = stage_record(r, task.task_idx, task.iter, &task.cells_ran) {
            by_iter.entry(task.iter).or_default().push(record);
        }
    }
    for stages in by_iter.values_mut() {
        // Completion order within an iteration is chain order.
        stages.sort_by_key(|s| s.stage_idx);
    }
    by_iter
}

/// One journaled commit as a `StageRecord`, each identity looked up on the
/// instance as a restore looks it up, a keyed one minted; `None` when the
/// tasklet is not in this build's chain.
fn stage_record(
    r: &Run<'_>,
    task_idx: u32,
    iter: u32,
    cells_ran: &[Vec<u8>],
) -> Option<StageRecord> {
    let stage_idx = task_idx as usize;
    let tasklet = *r.spec.dag.order.get(stage_idx)?;
    let cells = cells_ran
        .iter()
        .filter_map(|id| r.inst.slot_by_identity(id))
        .filter(|slot| slot.tasklet_id == tasklet)
        .map(CellRun::new)
        .collect();
    Some(StageRecord {
        stage_idx,
        iter,
        cells,
    })
}
