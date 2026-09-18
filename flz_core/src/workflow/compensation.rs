//! Saga compensation: the pure planner that turns the completed-stage records
//! into the rollback order (completed procedures in reverse, each entity that
//! ran once), and the executors the driver calls with the plan — the
//! crash-resume pass, the rewind undo and the journaled undo record.

use std::any::Any;
use std::sync::Arc;

use super::commit::{outbox_items, ship_and_await};
use super::driver::ChainCtx;
use super::instance::CommittedProc;
use super::resume::stages_by_iter;
use super::RollbackMode;
use crate::state_sink::COMPENSATION_DONE_PAYLOAD_ID;

/// A value an entity collected forward, retained for compensation.
pub type Value = Arc<dyn Any + Send + Sync>;

/// One staged plugin-bound output: `(plugin channel, payload)`.
pub type StagedOutput = (String, Vec<u8>);

/// One entity's contribution to a completed stage; an empty-partition entity
/// never ran and is not recorded.
pub struct EntityRun {
    pub entity_index: usize,
    pub outputs: Vec<Value>,
    pub staged: Vec<StagedOutput>,
    /// The result this entity's `complete` asked the instance to end with.
    pub completion: Option<Vec<u8>>,
}

/// A completed procedure, recorded so it can be rolled back; `iter` keys its
/// `compensated` journal entry (0 for a non-looping chain and for a record
/// rebuilt from intent markers).
pub struct StageRecord {
    pub stage_idx: usize,
    pub iter: u32,
    pub entities: Vec<EntityRun>,
}

/// The rollback order: `in_flight` (the interrupted iteration, in completion
/// order) is always undone in reverse; all-or-nothing then undoes each
/// committed iteration too, keep-progress keeps them. Completion order is
/// topological, so its reverse undoes no procedure before its descendants.
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

/// Roll back an abnormally-terminated chain per [`plan_compensation`], once
/// per entity that ran; hookless procedures are skipped. Best-effort: a
/// failing hook is logged and the rest still runs.
pub(super) fn run_compensation(
    ctx: &ChainCtx,
    committed: &[Vec<StageRecord>],
    in_flight: &[StageRecord],
) {
    let plan = plan_compensation(ctx.rollback_mode, committed, in_flight);
    if plan.is_empty() {
        return;
    }
    for record in plan {
        let proc = ctx.proc(record.stage_idx);
        // Hookless procedures take no part in rollback.
        if !proc.compensate_overridden {
            continue;
        }
        let slots = &ctx.stage_entities[record.stage_idx];
        for seg_run in &record.entities {
            let slot = &slots[seg_run.entity_index];
            // Crash-resume: an entity whose compensation-done marker was shipped
            // back already ran its undo — skip it (idempotent by author
            // contract, but re-running wastes work / double-emits records).
            if ctx.instance.has_compensation_done(&slot.entity_identity) {
                continue;
            }
            let staged = super::step::run_compensation_step(ctx, record.stage_idx, seg_run);
            // Journal this entity's undo output BEFORE the done marker: the
            // marker is what stops the hook re-running on resume, so a crash
            // between the two must fall on the side that re-runs the (idempotent)
            // hook rather than the side that loses its undo output. A hook that
            // emitted nothing needs no entry.
            if !staged.is_empty() {
                journal_undo(
                    ctx,
                    record,
                    "compensation",
                    vec![slot.entity_identity.as_ref().clone()],
                    outbox_items(slot, &staged),
                );
            }
            // Durable saga (§4.3): record this compensation's completion so a
            // later crash resumes the remainder and skips this entity.
            emit_compensation_record(
                COMPENSATION_DONE_PAYLOAD_ID,
                &slot.entity_identity,
                ctx.persistence_enabled,
            );
        }
    }
}

/// Undo the procedures a rewind unwound, in reverse iteration and chain
/// order — always all-or-nothing. It ignores the per-entity done markers (an
/// entity in two rewound iterations is undone twice) and journals one
/// `compensated` entry per procedure, even an empty one, so the journal alone
/// says what is still outstanding.
pub(super) fn run_rewind_undo(ctx: &ChainCtx, unwound: &[CommittedProc]) {
    let by_iter: Vec<Vec<StageRecord>> = stages_by_iter(ctx, unwound).into_values().collect();

    for record in plan_compensation(RollbackMode::AllOrNothing, &by_iter, &[]) {
        let proc = ctx.proc(record.stage_idx);
        let slots = &ctx.stage_entities[record.stage_idx];

        let mut entities_ran = Vec::new();
        let mut outbox = Vec::new();
        for seg_run in &record.entities {
            let slot = &slots[seg_run.entity_index];
            entities_ran.push(slot.entity_identity.as_ref().clone());
            if proc.compensate_overridden {
                let staged = super::step::run_compensation_step(ctx, record.stage_idx, seg_run);
                outbox.extend(outbox_items(slot, &staged));
            }
        }

        journal_undo(ctx, record, "rewind-undo", entities_ran, outbox);
    }
}

/// Journal one procedure's undo, carrying what its hook staged. The cancel
/// token is ignored: rollback IS the response to that cancel.
fn journal_undo(
    ctx: &ChainCtx,
    record: &StageRecord,
    what: &str,
    entities_ran: Vec<Vec<u8>>,
    outbox: Vec<crate::pb::ProcOutboxItem>,
) {
    let commit = crate::pb::command_request::Command::ProcCommit(crate::pb::ProcCommit {
        operative_id: ctx.instance_id.to_string(),
        proc_idx: record.stage_idx as u32,
        iter: record.iter,
        entities_ran,
        handoff: Vec::new(),
        outbox,
        compensating: true,
        completes: false,
        result: Vec::new(),
    });

    if let Err(e) = ship_and_await(ctx, record.stage_idx, what, false, commit) {
        eprintln!(
            "[flezha] workflow {} {what} journaling failed: {e}",
            ctx.instance_id
        );
    }
}

/// The completed-stage records a crash-resume compensates, from the
/// shipped-back intent markers. Outputs are empty — they were never durable,
/// so a compensable procedure persists its undo handle in State.
pub(super) fn stages_from_intents(ctx: &ChainCtx) -> Vec<StageRecord> {
    let mut completed: Vec<StageRecord> = Vec::new();
    for (stage_idx, slots) in ctx.stage_entities.iter().enumerate() {
        let entities: Vec<EntityRun> = slots
            .iter()
            .enumerate()
            .filter(|(_, slot)| ctx.instance.has_compensation_intent(&slot.entity_identity))
            .map(|(entity_index, _)| EntityRun {
                entity_index,
                outputs: Vec::new(),
                staged: Vec::new(),
                completion: None,
            })
            .collect();
        if !entities.is_empty() {
            // Intent markers carry no iteration; a pre-journal saga resume has
            // only one pass to undo anyway.
            completed.push(StageRecord {
                stage_idx,
                iter: 0,
                entities,
            });
        }
    }
    completed
}

/// Ship one saga marker for an Entity; a no-op for a non-durable class.
pub(super) fn emit_compensation_record(
    payload_id: &str,
    entity_identity: &[u8],
    persistence_enabled: bool,
) {
    if persistence_enabled {
        crate::state_sink::send_compensation_record(payload_id, entity_identity);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stage(stage_idx: usize) -> StageRecord {
        StageRecord {
            stage_idx,
            iter: 0,
            entities: Vec::new(),
        }
    }

    fn order(plan: &[&StageRecord]) -> Vec<usize> {
        plan.iter().map(|s| s.stage_idx).collect()
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
                    "procedure {p} must be compensated after its descendant {node}"
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
