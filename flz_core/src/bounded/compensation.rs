//! Saga compensation, planned: the completed-stage records turned into the
//! rollback order (completed tasklets in reverse, each Cell that ran once),
//! and the undo steps a run takes, one event at a time, to carry it out.

use std::collections::{BTreeMap, HashSet, VecDeque};
use std::sync::Arc;

use super::instance::{CellPos, CommittedTask, UndoStep, Value};
use super::resume::stages_by_iter;
use super::run::Run;
use super::RollbackMode;
use crate::cell::CellSlot;
use crate::timer::TimerOp;

/// The effect records one Cell wrote in a stage or an undo: its first and last
/// seq, and the plugin channels they wrote on.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RecordSpan {
    pub first: u64,
    pub last: u64,
    pub channels: Vec<String>,
}

impl RecordSpan {
    /// Widen `span` by one more record, or by another span, naming each
    /// channel once; a Cell's records only grow.
    pub fn absorb(span: &mut Option<RecordSpan>, other: RecordSpan) {
        let span = span.get_or_insert_with(|| RecordSpan {
            first: other.first,
            last: other.last,
            channels: Vec::new(),
        });
        span.first = span.first.min(other.first);
        span.last = span.last.max(other.last);
        for channel in other.channels {
            if !span.channels.contains(&channel) {
                span.channels.push(channel);
            }
        }
    }

    /// The Cell as a barrier names it: its identity and, when it wrote any,
    /// its records.
    pub fn committed_cell(cell: &CellSlot, span: Option<&RecordSpan>) -> crate::pb::CommittedCell {
        crate::pb::CommittedCell {
            cell_id: cell.cell_identity.as_ref().clone(),
            seq: span.map(|s| s.last),
            first_seq: span.map(|s| s.first),
            channels: span.map(|s| s.channels.clone()).unwrap_or_default(),
        }
    }
}

/// One Cell's contribution to a completed stage; a Cell that received no
/// item never ran and is not recorded.
#[derive(Clone)]
pub struct CellRun {
    pub cell: Arc<CellSlot>,
    pub outputs: Vec<Value>,
    pub record: Option<RecordSpan>,
    /// The result this Cell's `complete` asked the instance to end with.
    pub completion: Option<Vec<u8>>,
    /// The firings this Cell served in the stage, whose removal from its
    /// pending set is written when the stage commits.
    pub removals: Vec<TimerOp>,
}

impl CellRun {
    /// A Cell that ran and recorded nothing yet.
    pub fn new(cell: Arc<CellSlot>) -> Self {
        Self {
            cell,
            outputs: Vec::new(),
            record: None,
            completion: None,
            removals: Vec::new(),
        }
    }
}

/// A completed tasklet, recorded so it can be rolled back; `iter` keys its
/// `compensated` journal entry (0 for a non-looping chain and for a record
/// rebuilt from intent markers).
#[derive(Clone)]
pub struct StageRecord {
    pub stage_idx: usize,
    pub iter: u32,
    pub cells: Vec<CellRun>,
}

/// The rollback order: `in_flight` (the interrupted iteration, in completion
/// order) is always undone in reverse; all-or-nothing then undoes each
/// committed iteration too, keep-progress keeps them. Completion order is
/// topological, so its reverse undoes no tasklet before its descendants.
pub fn plan_compensation<'a>(
    mode: RollbackMode,
    committed: &'a [Vec<StageRecord>],
    in_flight: &'a [StageRecord],
) -> Vec<&'a StageRecord> {
    let mut plan: Vec<&StageRecord> = in_flight.iter().rev().collect();
    if mode == RollbackMode::AllOrNothing {
        for iteration in committed.iter().rev() {
            plan.extend(iteration.iter().rev());
        }
    }
    plan
}

/// The saga's undo per [`plan_compensation`], once per Cell that ran;
/// hookless tasklets take no part, and a Cell whose compensation-done marker
/// was shipped back already ran its undo before the crash.
pub(super) fn saga_steps(
    r: &Run<'_>,
    committed: &[Vec<StageRecord>],
    in_flight: &[StageRecord],
    undone: &HashSet<Vec<u8>>,
) -> VecDeque<UndoStep> {
    let mut steps = VecDeque::new();
    for record in plan_compensation(r.spec.rollback_mode, committed, in_flight) {
        if !r.task(record.stage_idx).compensate_overridden {
            continue;
        }
        for run in &record.cells {
            if undone.contains(run.cell.cell_identity.as_slice()) {
                continue;
            }
            steps.push_back(UndoStep::Cell {
                iter: record.iter,
                cell: Arc::clone(&run.cell),
                outputs: run.outputs.iter().map(Arc::clone).collect(),
                marks: true,
            });
        }
    }
    steps
}

/// Undo the tasklets a rewind unwound, as [`unwind_steps`] undoes them.
pub(super) fn rewind_steps(r: &Run<'_>, unwound: &[CommittedTask]) -> VecDeque<UndoStep> {
    let by_iter: Vec<Vec<StageRecord>> = stages_by_iter(r, unwound).into_values().collect();
    unwind_steps(r, &by_iter)
}

/// Undo whole iterations (a rewind's, a failed turn's) in reverse iteration
/// and chain order. It ignores the per-Cell done markers (a Cell in two
/// unwound iterations is undone twice) and journals one `compensated` entry
/// per tasklet, even an empty one, so the journal alone says what is still
/// outstanding.
pub(super) fn unwind_steps(r: &Run<'_>, iterations: &[Vec<StageRecord>]) -> VecDeque<UndoStep> {
    let mut steps = VecDeque::new();
    for record in plan_compensation(RollbackMode::AllOrNothing, iterations, &[]) {
        if r.task(record.stage_idx).compensate_overridden {
            for run in &record.cells {
                steps.push_back(UndoStep::Cell {
                    iter: record.iter,
                    cell: Arc::clone(&run.cell),
                    outputs: run.outputs.iter().map(Arc::clone).collect(),
                    marks: false,
                });
            }
        }
        steps.push_back(UndoStep::Record {
            node: record.stage_idx,
            iter: record.iter,
            cells_ran: record
                .cells
                .iter()
                .map(|run| run.cell.cell_identity.as_ref().clone())
                .collect(),
        });
    }
    steps
}

/// The completed-stage records a crash-resume compensates, from the
/// shipped-back intent markers, each looked up on the instance as a restore
/// looks it up. Outputs are empty — they were never durable, so a
/// compensable tasklet persists its undo handle in State.
pub(super) fn stages_from_intents(r: &Run<'_>, intents: &HashSet<Vec<u8>>) -> Vec<StageRecord> {
    let mut by_node: BTreeMap<usize, BTreeMap<CellPos, CellRun>> = BTreeMap::new();
    for identity in intents {
        let Some(cell) = r.inst.slot_by_identity(identity) else {
            continue;
        };
        if let Some((node, pos)) = r.locate(&cell) {
            by_node
                .entry(node)
                .or_default()
                .insert(pos, CellRun::new(cell));
        }
    }
    // Intent markers carry no iteration; a pre-journal saga resume has only
    // one pass to undo anyway.
    by_node
        .into_iter()
        .map(|(stage_idx, cells)| StageRecord {
            stage_idx,
            iter: 0,
            cells: cells.into_values().collect(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stage(stage_idx: usize) -> StageRecord {
        StageRecord {
            stage_idx,
            iter: 0,
            cells: Vec::new(),
        }
    }

    fn order(plan: &[&StageRecord]) -> Vec<usize> {
        plan.iter().map(|s| s.stage_idx).collect()
    }

    fn span(first: u64, last: u64, channels: &[&str]) -> RecordSpan {
        RecordSpan {
            first,
            last,
            channels: channels.iter().map(|c| c.to_string()).collect(),
        }
    }

    /// A Cell's records in a stage widen into one range, each channel named
    /// once, in first-written order.
    #[test]
    fn a_cells_records_widen_into_one_span() {
        let mut record = None;
        RecordSpan::absorb(&mut record, span(4, 4, &["mid", "mid"]));
        RecordSpan::absorb(&mut record, span(5, 5, &[]));
        RecordSpan::absorb(&mut record, span(6, 6, &["out", "mid"]));
        assert_eq!(record, Some(span(4, 6, &["mid", "out"])));
    }

    #[test]
    fn no_progress_compensates_nothing() {
        let plan = plan_compensation(RollbackMode::AllOrNothing, &[], &[]);
        assert!(plan.is_empty());
        let plan = plan_compensation(RollbackMode::KeepProgress, &[], &[]);
        assert!(plan.is_empty());
    }

    #[test]
    fn single_pass_is_reverse_order_in_both_modes() {
        // A no-loop pass (committed empty): both modes undo the whole pass in
        // reverse — Op0, Op1, Op2 completed ⇒ compensate Op2, Op1, Op0.
        let in_flight = vec![stage(0), stage(1), stage(2)];
        for mode in [RollbackMode::AllOrNothing, RollbackMode::KeepProgress] {
            let plan = plan_compensation(mode, &[], &in_flight);
            assert_eq!(order(&plan), vec![2, 1, 0], "mode {mode:?}");
        }
    }

    /// A tree's completion order is topological, so plain reversal undoes
    /// descendants before ancestors.
    #[test]
    fn a_tree_pass_unwinds_descendants_before_ancestors() {
        // Root 0 → (1, 2); 1 → 3. Completion order is the frontier order:
        // level 0 = [0], level 1 = [1, 2], level 2 = [3].
        let parent = [None, Some(0), Some(0), Some(1)];
        let in_flight = vec![stage(0), stage(1), stage(2), stage(3)];
        let plan = plan_compensation(RollbackMode::AllOrNothing, &[], &in_flight);
        assert_eq!(order(&plan), vec![3, 2, 1, 0]);

        let position = |node: usize| order(&plan).iter().position(|&s| s == node).unwrap();
        for (node, p) in parent.iter().enumerate() {
            if let Some(p) = p {
                assert!(
                    position(*p) > position(node),
                    "tasklet {p} must be compensated after its descendant {node}"
                );
            }
        }
    }

    #[test]
    fn all_or_nothing_undoes_committed_iterations_too() {
        // Two committed iterations (each a 2-stage pass) plus a partial third.
        let committed = vec![vec![stage(0), stage(1)], vec![stage(0), stage(1)]];
        let in_flight = vec![stage(0)];
        let plan = plan_compensation(RollbackMode::AllOrNothing, &committed, &in_flight);
        // Reverse completion order across every iteration, back to the start:
        // in-flight Op0, then iteration 2 (Op1, Op0), then iteration 1 (Op1, Op0).
        assert_eq!(order(&plan), vec![0, 1, 0, 1, 0]);
    }

    #[test]
    fn keep_progress_keeps_committed_iterations() {
        // Same shape, keep-progress: only the in-flight iteration unwinds; the
        // two finished iterations are kept.
        let committed = vec![vec![stage(0), stage(1)], vec![stage(0), stage(1)]];
        let in_flight = vec![stage(0)];
        let plan = plan_compensation(RollbackMode::KeepProgress, &committed, &in_flight);
        assert_eq!(order(&plan), vec![0]);
    }
}
