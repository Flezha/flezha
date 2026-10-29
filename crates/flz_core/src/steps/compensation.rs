//! Saga compensation, planned: the completed-stage records turned into the
//! rollback order (completed tasklets in reverse, each Cell that ran once),
//! and the undo steps a run takes, one event at a time, to carry it out.

use std::collections::{BTreeMap, HashSet, VecDeque};
use std::sync::atomic::Ordering;
use std::sync::Arc;

use super::commit::undo_frame;
use super::effects::Frame;
use super::instance::{
    AfterUndo, BarrierFor, CellPos, CommittedTask, Phase, Undo, UndoStep, Value,
};
use super::resume::stages_by_iter;
use super::run::{Decide, Run};
use super::{OnFailure, StepsError};
use crate::cell::CellSlot;
use crate::state_sink::COMPENSATION_DONE_PAYLOAD_ID;
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
    mode: OnFailure,
    committed: &'a [Vec<StageRecord>],
    in_flight: &'a [StageRecord],
) -> Vec<&'a StageRecord> {
    let mut plan: Vec<&StageRecord> = in_flight.iter().rev().collect();
    if !mode.keeps_committed() {
        plan.extend(reversed(committed));
    }
    plan
}

/// Every record of `iterations`, the last iteration first and each in
/// reverse completion order.
fn reversed(iterations: &[Vec<StageRecord>]) -> impl Iterator<Item = &StageRecord> {
    iterations
        .iter()
        .rev()
        .flat_map(|iteration| iteration.iter().rev())
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
    for record in plan_compensation(r.spec.on_failure, committed, in_flight) {
        if !r.task(record.stage_idx).compensate_overridden {
            continue;
        }
        for run in &record.cells {
            if undone.contains(run.cell.cell_identity.as_slice()) {
                continue;
            }
            steps.push_back(UndoStep::cell(record.iter, run, true));
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
    for record in reversed(iterations) {
        if r.task(record.stage_idx).compensate_overridden {
            for run in &record.cells {
                steps.push_back(UndoStep::cell(record.iter, run, false));
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

impl Decide<'_, '_, '_> {
    // -- compensation ------------------------------------------------------------

    /// Undo what the run completed, in reverse completion order, then `then`.
    pub(super) fn compensate(&mut self, then: AfterUndo) {
        self.r.run.pass_dead.store(true, Ordering::Release);
        self.s.pass = None;
        let steps = saga_steps(
            self.r,
            &self.s.committed,
            &self.s.done,
            &self.s.compensation_done,
        );
        self.begin_undo(steps, then);
    }

    /// Carry out `steps`, one event at a time, then `then`.
    pub(super) fn begin_undo(&mut self, steps: VecDeque<UndoStep>, then: AfterUndo) {
        self.s.phase = Phase::Compensating;
        let planned = steps
            .iter()
            .filter(|step| matches!(step, UndoStep::Cell { .. }))
            .count();
        self.s.undo = Some(Undo {
            steps,
            cells: Vec::new(),
            then,
            error: None,
            planned,
            partial: false,
        });
        self.undo_step();
    }

    /// The run cannot go on: undo what is outstanding, then end `cancelled`
    /// when a stop tripped the token, else `error`.
    pub(super) fn compensate_on(&mut self, error: StepsError) {
        let then = if self.cancelled() {
            AfterUndo::cancelled()
        } else {
            AfterUndo::Terminal("error".to_string(), error.message)
        };
        self.compensate(then);
    }

    /// The next step of the undo plan, or what follows its end.
    pub(super) fn undo_step(&mut self) {
        let r = self.r;
        loop {
            let Some(undo) = self.s.undo.as_mut() else {
                return;
            };
            let next = match undo.steps.front() {
                None => None,
                Some(UndoStep::Cell { cell, outputs, .. }) => {
                    let offset = match &self.s.pass {
                        Some(pass) => pass.effect_offset(),
                        None => self.s.entry.and_then(|(_, offset)| offset),
                    };
                    self.fx.run_undo = Some((Arc::clone(cell), outputs.clone(), offset));
                    return;
                }
                Some(UndoStep::Record {
                    node,
                    iter,
                    cells_ran,
                }) => Some((*node, *iter, cells_ran.clone())),
                Some(UndoStep::Mark { identity }) => {
                    if r.inst.keeps_state() {
                        self.fx
                            .markers
                            .push((COMPENSATION_DONE_PAYLOAD_ID, Arc::clone(identity)));
                    }
                    undo.steps.pop_front();
                    continue;
                }
            };
            let Some((node, iter, cells_ran)) = next else {
                return self.undo_finished();
            };
            if !r.journaling() {
                undo.steps.pop_front();
                continue;
            }
            let written = std::mem::take(&mut undo.cells);
            let command = undo_frame(r, node, iter, cells_ran, written);
            self.barrier(BarrierFor::Undo, Frame::Built(Box::new(command)));
            return;
        }
    }

    /// The undo plan ran out: what it planned, or `error` when an undo's
    /// entry was never journaled. A run that ends here reports whether its
    /// rollback completed: `complete`, `partial`, or nothing when it planned
    /// no hook.
    fn undo_finished(&mut self) {
        let finished = self.s.undo.take().expect("undo checked above");
        let undo = match (
            finished.planned,
            finished.partial || finished.error.is_some(),
        ) {
            (0, _) => "",
            (_, true) => "partial",
            (_, false) => "complete",
        };
        match (finished.then, finished.error) {
            (AfterUndo::Terminal(status, error), None) => self.ended(&status, error, undo),
            (AfterUndo::Resume, None) => self.resume(),
            (AfterUndo::Terminal(_, planned), Some(error)) if !planned.is_empty() => {
                self.ended("error", format!("{planned}; {error}"), undo)
            }
            (_, Some(error)) => self.ended("error", error, undo),
        }
    }

    /// The controller's last answer to an undo's `compensated` entry. Only an
    /// ack lets the Cell's done marker follow; an entry never journaled drops
    /// the marker too, so a resume runs the (idempotent) hook again, and the
    /// rest of the plan still runs.
    pub(super) fn undo_journaled(&mut self, result: Result<(), String>) {
        let r = self.r;
        let Some(undo) = self.s.undo.as_mut() else {
            return;
        };
        let recorded = undo.steps.pop_front();
        if let Err(error) = result {
            undo.partial = true;
            let id = &r.inst.instance_id;
            flz_transport::notice!(Error, "steps {id} compensation journaling failed: {error}");
            if matches!(undo.steps.front(), Some(UndoStep::Mark { .. })) {
                undo.steps.pop_front();
            }
            if let Some(UndoStep::Record { node, .. }) = recorded {
                let tasklet = &r.task(node).name;
                undo.error.get_or_insert_with(|| {
                    format!("compensation of tasklet `{tasklet}` was not journaled: {error}")
                });
            }
        }
        self.undo_step();
    }

    /// One Cell's undo answered. A saga journals the record the hook wrote
    /// before the done marker: the marker stops the hook re-running on
    /// resume, so a crash between the two must fall on the side that re-runs
    /// the (idempotent) hook. A rewind collects it into its tasklet's entry.
    pub(super) fn undone(&mut self, node: usize, cell: CellPos, slot: &Arc<CellSlot>) {
        let attempt = self.s.attempts.remove(&(node, cell)).unwrap_or_default();
        let failed = attempt.failure.is_some();
        if let Some(failure) = attempt.failure {
            let (id, error) = (&self.r.inst.instance_id, failure.error);
            flz_transport::notice!(Error, "steps {id} compensation error: {error}");
        }
        let Some(undo) = self.s.undo.as_mut() else {
            return;
        };
        undo.partial |= failed;
        let Some(UndoStep::Cell { iter, marks, .. }) = undo.steps.pop_front() else {
            return;
        };
        let written = attempt
            .record
            .map(|span| RecordSpan::committed_cell(slot, Some(&span)));
        if !marks {
            undo.cells.extend(written);
            return self.undo_step();
        }
        let identity = Arc::clone(&slot.cell_identity);
        undo.steps.push_front(UndoStep::Mark {
            identity: Arc::clone(&identity),
        });
        if let Some(written) = written {
            undo.cells = vec![written];
            undo.steps.push_front(UndoStep::Record {
                node,
                iter,
                cells_ran: vec![identity.as_ref().clone()],
            });
        }
        self.undo_step();
    }
}

/// The completion one tasklet asked for: its greatest cell key's.
pub(super) fn node_completion(runs: &[CellRun]) -> Option<Vec<u8>> {
    runs.iter()
        .filter_map(|run| Some((&run.cell.cell_key, run.completion.as_ref()?)))
        .max_by(|a, b| a.0.cmp(b.0))
        .map(|(_, result)| result.clone())
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
        let plan = plan_compensation(OnFailure::UndoAll, &[], &[]);
        assert!(plan.is_empty());
        let plan = plan_compensation(OnFailure::KeepProgress, &[], &[]);
        assert!(plan.is_empty());
    }

    #[test]
    fn single_pass_is_reverse_order_in_both_modes() {
        // A no-loop pass (committed empty): both modes undo the whole pass in
        // reverse — Op0, Op1, Op2 completed ⇒ compensate Op2, Op1, Op0.
        let in_flight = vec![stage(0), stage(1), stage(2)];
        for mode in [OnFailure::UndoAll, OnFailure::KeepProgress] {
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
        let plan = plan_compensation(OnFailure::UndoAll, &[], &in_flight);
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
    fn undo_all_undoes_committed_iterations_too() {
        // Two committed iterations (each a 2-stage pass) plus a partial third.
        let committed = vec![vec![stage(0), stage(1)], vec![stage(0), stage(1)]];
        let in_flight = vec![stage(0)];
        let plan = plan_compensation(OnFailure::UndoAll, &committed, &in_flight);
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
        let plan = plan_compensation(OnFailure::KeepProgress, &committed, &in_flight);
        assert_eq!(order(&plan), vec![0]);
    }
}
