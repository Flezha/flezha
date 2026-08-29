//! Saga compensation planning for the workflow driver.
//!
//! The driver records each **completed** stage (the procedure + the entities
//! that ran, with their collected outputs) as it advances. On abnormal
//! termination — a procedure errors, or an external stop trips after partial
//! progress — [`plan_compensation`] turns those records into the exact rollback
//! order: completed procedures are undone in **reverse**, and within an
//! procedure each entity that ran is compensated once (entity order is
//! irrelevant — independent keys). A clean `ok` produces no records, so nothing
//! is compensated.
//!
//! The planner is **pure** so both rollback modes are testable without driving
//! the runner; the executors below it — the crash-resume pass, the rewind undo
//! and the journaled undo record — are what the driver calls with the plan.

use std::any::Any;
use std::sync::Arc;

use super::commit::{outbox_items, ship_and_await};
use super::driver::ChainCtx;
use super::instance::CommittedProc;
use super::resume::stages_by_iter;
use super::RollbackMode;
use crate::state_sink::COMPENSATION_DONE_PAYLOAD_ID;

/// Type-erased value an entity collected on its forward run, retained for a
/// possible compensation. Downcast back to `Arc<S::Output>` by
/// `ProcedureDispatcher::compensate_collecting`.
pub type Value = Arc<dyn Any + Send + Sync>;

/// One serialized plugin-bound output staged by an entity, in the order the
/// entity produced it: `(plugin channel name, payload bytes)`.
pub type StagedOutput = (String, Vec<u8>);

/// One entity's contribution to a completed stage: which entity ran, the
/// inter-procedure outputs it collected, and the plugin-bound output it staged
/// for the procedure's transactional outbox. Only entities that actually
/// processed input are recorded ("an effect"); empty-partition entities never
/// ran and are skipped.
pub struct EntityRun {
    pub entity_index: usize,
    pub outputs: Vec<Value>,
    pub staged: Vec<StagedOutput>,
}

/// A completed procedure in the chain, recorded so it can be rolled back.
/// `stage_idx` indexes both the chain and the driver's pre-built stage-entity
/// table, so the compensator resolves the procedure descriptor and the entity
/// slots from it. `iter` is the loop iteration it ran in, which its `compensated`
/// journal entry is keyed by (0 for a non-looping chain, and for a record
/// rebuilt from compensation-intent markers, which carry no iteration).
pub struct StageRecord {
    pub stage_idx: usize,
    pub iter: u32,
    pub entities: Vec<EntityRun>,
}

/// Resolve the rollback order for an abnormal termination.
///
/// `in_flight` is the interrupted iteration's completed stages, in completion
/// order; it is **always** undone (reverse order). Completion order is
/// **topological** over the procedure tree — the driver records a level of the
/// frontier before releasing its children, and the journal-rebuilt path sorts by
/// `stage_idx`, which is BFS-numbered — so reversing it is exactly the
/// reverse-topological order a tree saga requires: no procedure is undone before
/// its compensated descendants, and independent sibling subtrees unwind in an
/// unspecified relative order. For a linear chain that degenerates to the exact
/// reverse of the chain (`workflow-branching` D4a).
///
/// `committed` holds fully finished iterations, in completion order:
///
/// - **all-or-nothing** undoes everything — the in-flight iteration first, then
///   each committed iteration, all in reverse completion order, back to the
///   start.
/// - **keep-progress** undoes only the in-flight iteration; every committed
///   iteration is kept.
///
/// Within a single iteration (or a no-loop pass) the two modes coincide:
/// `committed` is empty, so both return the in-flight stages reversed.
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

/// Roll back an abnormally-terminated chain. The in-flight iteration's completed
/// procedures are always compensated, in reverse order (per
/// [`plan_compensation`]) and, within a procedure, once per entity that ran;
/// `committed` holds the iterations the journal records as finished, which
/// keep-progress keeps and all-or-nothing unwinds after it. Procedures without a
/// `compensate` hook are skipped. Best-effort: a failing hook is logged and the
/// remaining steps still run, so one broken undo cannot strand the rest.
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

/// Undo the procedures a rewind unwound, in reverse iteration and reverse chain
/// order (`workflow-loop-rewind`). Forced all-or-nothing: a rewind unwinds
/// committed iterations by definition, whatever the class's rollback mode says
/// about crashes.
///
/// Two things differ from the crash-resume pass. It does not consult the
/// per-entity compensation-done markers — those are per *entity*, and an entity
/// that ran in two rewound iterations must be undone once per iteration. And it
/// journals exactly one `compensated` entry per procedure, **even an empty one**
/// for a hookless procedure: that entry is the durable proof the procedure is
/// undone, and without it the journal would re-plan it on every restore. The
/// journal is therefore the sole authority on what is still outstanding, which is
/// what makes a crash mid-rewind resume with exactly the remaining work.
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

/// Journal one procedure's undo — the `compensated` entry that records it as
/// reversed, carrying whatever output its hook staged. Shared by the crash-resume
/// pass (one entry per compensated entity) and the rewind pass (one per
/// procedure, even an empty one). Best-effort: the cancel token is ignored,
/// because rollback IS the response to that cancel.
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
    });

    if let Err(e) = ship_and_await(ctx, record.stage_idx, what, false, commit) {
        eprintln!(
            "[flezha] workflow {} {what} journaling failed: {e}",
            ctx.instance_id
        );
    }
}

/// Rebuild the completed-stage records to compensate on a crash-resume from the
/// shipped-back compensation-intent markers: an Entity slot whose identity was
/// marked "intent" completed forward and needs undo. Outputs are empty — an
/// procedure's collected inter-procedure output was never durable, so a durable
/// compensable procedure must persist its undo handle in State (which IS
/// restored), per the at-least-once author contract. Stages are gathered in
/// chain order; [`plan_compensation`] reverses them for the undo pass.
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

/// Ship one saga compensation marker for an Entity through the reliable sink —
/// a no-op for an opted-out (non-durable) class, which never crash-resumes.
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

    /// Build a `StageRecord` with no retained outputs (the plan only orders
    /// stages; per-entity output handling is the dispatcher's job).
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

    /// `workflow-branching`: a tree's completion order is topological (a parent's
    /// level completes before its children's), so the planner's plain reversal
    /// IS reverse-topological — descendants before ancestors — with no
    /// parent-link bookkeeping of its own.
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
