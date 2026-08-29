//! The workflow driver: one dedicated thread per instance that runs the body
//! — `run`-sugar or the procedure DAG, where a child runs only after every
//! producer feeding it commits — and converges every exit trigger on one
//! `WorkflowCompleted`. The invariant: the driver only ever blocks (inbox,
//! barrier, commit ack) and is never a thread its own stages need; stage
//! work leaves as [`WorkflowStep`] activations the poll core hands out.

use std::any::Any;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crossbeam::channel::Receiver;

use super::commit::{commit_iteration, commit_proc_boundary};
use super::compensation::{
    emit_compensation_record, run_compensation, run_rewind_undo, stages_from_intents, StageRecord,
};
use super::instance::{InboxItem, JournalResume, WorkflowInstance};
use super::resume::{journaled_stages, next_iteration_seed, park, IterationSeed, IterationStart};
use super::spec::{ProcDag, WorkflowSpec};
use super::{LoopMode, RollbackMode, WorkflowCtx, WorkflowError, WorkflowRunFn};
use crate::channel::{BuildEntityArgs, ConsumerEdge, ProcedureId, RetryPolicy};
use crate::entity::EntitySlot;
use crate::flusher::EntityFlusher;
use crate::operative::topology::{EntityMode, OperativeTopology, ProcedureDescriptor};
use crate::state_sink::COMPENSATION_INTENT_PAYLOAD_ID;

pub(super) type Value = Arc<dyn Any + Send + Sync>;

/// How often a blocked driver re-checks for a cooperative stop.
pub(super) const CANCEL_POLL_CHUNK: Duration = Duration::from_millis(200);

/// Driver thread stack in KiB; unset keeps the platform default. An edge
/// transform is author code on this thread, and a stack overflow aborts.
const ENV_DRIVER_STACK_KIB: &str = "FLEZHA_WORKFLOW_DRIVER_STACK_KIB";

pub(super) enum RunOutcome {
    /// `WorkflowCompleted` with this `(status, error, result)`.
    Terminal(String, String, Vec<u8>),
    /// Suspended at a committed boundary: `IterPark` is on the wire and the
    /// run tears down without a completion.
    Parked,
}

/// Read-only context of one structured chain run.
pub(super) struct ChainCtx<'a> {
    pub(super) topology: &'a Arc<OperativeTopology>,
    pub(super) dag: &'a ProcDag,
    pub(super) instance: &'a WorkflowInstance,
    pub(super) instance_id: &'a Arc<str>,
    pub(super) stage_entities: &'a [Vec<Arc<EntitySlot>>],
    pub(super) rollback_mode: RollbackMode,
    pub(super) loop_mode: LoopMode,
    pub(super) wake_after: Option<Duration>,
    pub(super) inbox: &'a Receiver<InboxItem>,
    pub(super) retry: RetryPolicy,
    pub(super) barrier_retry_attempts: u32,
    pub(super) persistence_enabled: bool,
    /// Inbox items the controller still owes this bind; the park-idle clock
    /// starts at zero.
    pub(super) owed_inbox: std::cell::Cell<u32>,
}

impl ChainCtx<'_> {
    /// Whether this run journals its proc boundaries — the one gate on both
    /// the `ProcCommit` handshake and output staging. A transport-less run
    /// has no journal, so its plugin output goes straight to the wire.
    pub(super) fn journaling(&self) -> bool {
        self.persistence_enabled && crate::runner::coordinator_installed()
    }

    pub(super) fn proc(&self, node: usize) -> &ProcedureDescriptor {
        self.topology
            .procedure(self.dag.order[node])
            .expect("dag procedure present in topology")
    }

    /// The edge from `producer`'s output channel into `child`; `None` when
    /// the producer's `Output` already IS the consumer's `Input`.
    pub(super) fn edge(&self, producer: usize, child: usize) -> Option<&ConsumerEdge> {
        let channel_id = self.proc(producer).output_channel_id?;
        let target = self.dag.order[child];
        self.topology
            .channel(channel_id)?
            .consumers
            .iter()
            .find(|e| e.procedure == target)
    }
}

/// Extend `dest` with a released batch through the edge's transform. One
/// body for the forward accumulate and the journal re-seed, so a resumed
/// chain sees what the live one did; a contained unwind aborts the chain
/// rather than killing the driver, and is not retried.
pub(super) fn extend_mapped(
    dest: &mut Vec<Value>,
    edge: Option<&ConsumerEdge>,
    batch: impl Iterator<Item = Value>,
) -> Result<(), WorkflowError> {
    let Some(edge) = edge.filter(|e| e.transform.is_some()) else {
        dest.extend(batch);
        return Ok(());
    };
    for value in batch {
        match edge.transform_value(&value)? {
            None => dest.push(value),
            Some(yielded) => dest.extend(yielded),
        }
    }
    Ok(())
}

/// Spawn the driver for an instance the caller already registered — before
/// the `AddOperative` ack, so nothing arriving right after is lost.
pub fn spawn(
    spec: Arc<WorkflowSpec>,
    instance: Arc<WorkflowInstance>,
    inbox_rx: Receiver<InboxItem>,
    args: String,
) {
    let name = format!("flezha-workflow-{}", instance.instance_id);
    let thread_instance = Arc::clone(&instance);
    let mut builder = thread::Builder::new().name(name);
    // `0` — unset, unparseable, or an explicit zero — keeps the platform default.
    match crate::util::env_parse_positive(ENV_DRIVER_STACK_KIB, 0) {
        0 => {}
        kib => builder = builder.stack_size(kib * 1024),
    }
    let spawned = builder.spawn(move || run(spec, thread_instance, inbox_rx, args));

    if let Err(e) = spawned {
        // Spawn failed: deregister this run's flusher entries and report a
        // terminal failure so the controller does not wait forever. The registry
        // entry is dropped by the controller's stop_operative on the completion.
        deregister_flushers(&instance.stage_entities);
        crate::runner::emit_workflow_completed(
            instance.instance_id.to_string(),
            "error".to_string(),
            format!("failed to spawn workflow driver thread: {e}"),
            Vec::new(),
        );
    }
}

fn run(
    spec: Arc<WorkflowSpec>,
    instance: Arc<WorkflowInstance>,
    inbox_rx: Receiver<InboxItem>,
    args: String,
) {
    let finished = Arc::new(AtomicBool::new(false));

    // max_duration backstop: trip `cancel` once it elapses, unless the body
    // finished first.
    let watcher = spec
        .max_duration
        .map(|d| spawn_duration_watcher(d, Arc::clone(&instance.cancel), Arc::clone(&finished)));

    let outcome = run_body(&spec, &instance, inbox_rx, &args);

    finished.store(true, Ordering::Release);
    if let Some(w) = watcher {
        let _ = w.join();
    }
    // Deregister this run's per-Entity flusher entries. The registry entry
    // itself stays until the controller's stop_operative teardown, so a
    // duplicate AddOperative before then is idempotent (no re-run).
    deregister_flushers(&instance.stage_entities);

    match outcome {
        RunOutcome::Terminal(status, error, result) => {
            crate::runner::emit_workflow_completed(
                instance.instance_id.to_string(),
                status,
                error,
                result,
            );
        }
        // Suspension is not a terminal outcome: the controller evicts the
        // instance on the already-shipped `IterPark` and keeps it resumable.
        RunOutcome::Parked => {}
    }
}

/// The body by class shape: a structured chain, a host run-sugar body crossed
/// through the poll surface, or the typed in-process `run`.
fn run_body(
    spec: &WorkflowSpec,
    instance: &Arc<WorkflowInstance>,
    inbox_rx: Receiver<InboxItem>,
    args: &str,
) -> RunOutcome {
    match spec.topology.as_ref() {
        // Structured: hold on the crash-resume gate until the Controller's
        // Entity restores land, then drive the chain over the restored state.
        Some(_) => {
            instance.wait_for_restore();
            run_structured(spec, instance, args, &inbox_rx)
        }
        // Run-sugar: no Entities, no restore — a crash re-runs from the top.
        None if spec.host => {
            let (outcome, result) = crate::host::run_workflow_sugar(
                &instance.instance_id,
                &instance.factory_id,
                args,
                &instance.cancel,
                inbox_rx,
            );
            classify(outcome, &instance.cancel, result)
        }
        None => run_sugar(
            spec.run,
            &instance.instance_id,
            args,
            &instance.cancel,
            inbox_rx,
        ),
    }
}

fn run_sugar(
    run: WorkflowRunFn,
    instance_id: &Arc<str>,
    args: &str,
    cancel: &Arc<AtomicBool>,
    inbox_rx: Receiver<InboxItem>,
) -> RunOutcome {
    let mut ctx = WorkflowCtx::new(Arc::clone(instance_id), Arc::clone(cancel), inbox_rx);
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run(&mut ctx, args)));
    let (_completed, result) = ctx.into_result();
    classify(outcome, cancel, result)
}

/// The terminal tuple: a failure or panic is `error`, else a tripped token
/// is `cancelled`, else `ok`.
fn classify(
    outcome: std::thread::Result<Result<(), WorkflowError>>,
    cancel: &AtomicBool,
    result: Vec<u8>,
) -> RunOutcome {
    let (status, error) = match outcome {
        Ok(Ok(())) => {
            if cancel.load(Ordering::Acquire) {
                ("cancelled".to_string(), String::new())
            } else {
                ("ok".to_string(), String::new())
            }
        }
        Ok(Err(e)) => ("error".to_string(), e.to_string()),
        Err(panic) => ("error".to_string(), crate::util::format_panic(panic)),
    };
    RunOutcome::Terminal(status, error, result)
}

/// Structured path: walk the DAG; sibling branches and entities interleave.
fn run_structured(
    spec: &WorkflowSpec,
    instance: &WorkflowInstance,
    args: &str,
    inbox: &Receiver<InboxItem>,
) -> RunOutcome {
    let topology = spec
        .topology
        .as_ref()
        .expect("run_structured invoked only for a structured spec");

    // Entities were built + registered up front by `start_workflow_resumable`
    // (so restore could reach them and the resume gate could hold); a durable
    // class has them already restored by the Controller's `RestoreInitState`
    // frames. The run's flusher entries are deregistered in `run`'s teardown.
    let ctx = ChainCtx {
        topology,
        dag: &spec.dag,
        instance,
        instance_id: &instance.instance_id,
        stage_entities: &instance.stage_entities,
        rollback_mode: spec.rollback_mode,
        loop_mode: spec.loop_mode,
        wake_after: spec.wake_after,
        inbox,
        retry: spec.retry,
        barrier_retry_attempts: spec.barrier_retry_attempts,
        persistence_enabled: spec.persistence_enabled,
        owed_inbox: std::cell::Cell::new(instance.owed_inbox),
    };
    drive_chain(&ctx, args, &instance.cancel)
}

/// Every stage's Entities in DAG order, built before the driver thread so a
/// restore frame always finds them registered. Each slot's `instance` is a
/// null `Weak`: the collecting path never routes through a `StreamInstance`.
pub fn build_all_stage_entities(
    spec: &WorkflowSpec,
    instance_id: &Arc<str>,
) -> Vec<Vec<Arc<EntitySlot>>> {
    let topology = spec
        .topology
        .as_ref()
        .expect("build_all_stage_entities invoked only for a structured spec");
    let mut stage_entities: Vec<Vec<Arc<EntitySlot>>> = Vec::with_capacity(spec.dag.len());
    for &proc_id in &spec.dag.order {
        let proc = topology
            .procedure(proc_id)
            .expect("chain procedure present in topology");
        stage_entities.push(build_stage_entities(
            topology,
            instance_id,
            proc_id,
            proc,
            spec.persistence_enabled,
        ));
    }
    stage_entities
}

fn deregister_flushers(stage_entities: &[Vec<Arc<EntitySlot>>]) {
    for slots in stage_entities {
        for slot in slots {
            if let Some(id) = slot.flusher_entry_id {
                EntityFlusher::instance().deregister(id);
            }
        }
    }
}

/// Where the journal says a chain resumes: the committed iterations, the
/// in-flight iteration's stages, and its nodes already committed.
struct ResumePlan {
    resume: JournalResume,
    committed: Vec<Vec<StageRecord>>,
    in_flight: Vec<StageRecord>,
    committed_nodes: Vec<usize>,
}

/// The chain's prologue: finish an in-flight compensation (`Err` ends the
/// run), undo what a rewind unwound, read the journal's committed work. With
/// a journal only a compensation-done marker proves an undo was in flight —
/// intent markers accumulate during forward progress too.
fn resume_plan(ctx: &ChainCtx) -> Result<ResumePlan, RunOutcome> {
    let resumed = stages_from_intents(ctx);
    let journal = ctx.instance.proc_journal_resume();
    if !resumed.is_empty() && (journal.is_none() || ctx.instance.any_compensation_done()) {
        run_compensation(ctx, &[], &resumed);
        return Err(RunOutcome::Terminal(
            "error".to_string(),
            "resumed compensation after crash".to_string(),
            Vec::new(),
        ));
    }

    let resume = journal.unwrap_or_default();

    // The journal's committed work, in the two granularities the driver needs:
    // the fully-committed iterations (`plan_compensation`'s `committed` input, so
    // keep-progress keeps them and all-or-nothing unwinds them —
    // the loop's rollback scope) and the stages of the still-in-flight iteration.
    // Both are empty for a fresh run and for a non-looping chain.
    let (committed, in_flight) = journaled_stages(ctx, &resume);

    // A rewind unwinds the iterations above its target before the loop resumes at
    // it (workflow-loop-rewind). The journal has already folded the forward state
    // back — `resume.iter` IS the rewound-to iteration + 1 — so all that is left
    // is the undo, and the journal is authoritative about how much of it is still
    // outstanding.
    if !resume.rewind_undo.is_empty() {
        run_rewind_undo(ctx, &resume.rewind_undo);
    }

    // The root is index 0, so "the root already committed" IS "this is a
    // mid-iteration resume" — and a journal that somehow records a child without
    // its parent re-runs the whole iteration rather than trusting the fragment.
    let committed_nodes: Vec<usize> = if resume.committed_proc_idxs.contains(&0) {
        resume
            .committed_proc_idxs
            .iter()
            .map(|&i| i as usize)
            .collect()
    } else {
        Vec::new()
    };
    Ok(ResumePlan {
        resume,
        committed,
        in_flight,
        committed_nodes,
    })
}

fn drive_chain(ctx: &ChainCtx, args: &str, cancel: &AtomicBool) -> RunOutcome {
    let ResumePlan {
        resume,
        mut committed,
        in_flight,
        mut committed_nodes,
    } = match resume_plan(ctx) {
        Ok(plan) => plan,
        Err(outcome) => return outcome,
    };
    let mut iter = resume.iter;
    let mut consumed_offset = resume.consumed_offset;
    // Terminal batch of the previous iteration, carried in memory by a
    // handoff-chained loop; on resume it comes back from the journal instead.
    let mut carry: Option<Vec<Value>> = None;
    // Consumed by the first pass; every later iteration runs the whole graph.
    let mut resumed_stages = Some(in_flight);

    loop {
        // Completed stages of this iteration, in completion order (parents
        // before children), so an abnormal exit rolls them back in reverse.
        let mut completed: Vec<StageRecord> = resumed_stages.take().unwrap_or_default();
        let seed = next_iteration_seed(
            ctx,
            args,
            iter,
            &resume,
            &committed_nodes,
            carry.take(),
            cancel,
        );
        let (start, consumed) = match seed_frontier(ctx, seed, iter, cancel, &committed, &completed)
        {
            Ok(seeded) => seeded,
            Err(outcome) => return outcome,
        };
        if let Some(offset) = consumed {
            consumed_offset = offset;
        }
        let terminal_batch = match run_iteration(ctx, iter, start, cancel, &mut completed) {
            Ok(batch) => batch,
            Err((status, error)) => {
                run_compensation(ctx, &committed, &completed);
                return RunOutcome::Terminal(status, error, Vec::new());
            }
        };
        if let Err(outcome) = seal_iteration(
            ctx,
            iter,
            consumed_offset,
            &terminal_batch,
            cancel,
            &committed,
            &completed,
        ) {
            return outcome;
        }
        committed.push(completed);
        carry = Some(terminal_batch);
        iter += 1;
        committed_nodes.clear();
    }
}

/// The iteration boundary: commit it durably, then the loop enters the next
/// iteration. A non-looping chain finishes here; a refused commit undoes what
/// is outstanding. `Err` ends the run.
fn seal_iteration(
    ctx: &ChainCtx,
    iter: u32,
    consumed_offset: u64,
    terminal_batch: &[Value],
    cancel: &AtomicBool,
    committed: &[Vec<StageRecord>],
    completed: &[StageRecord],
) -> Result<(), RunOutcome> {
    if ctx.loop_mode == LoopMode::Once {
        return Err(RunOutcome::Terminal(
            "ok".to_string(),
            String::new(),
            Vec::new(),
        ));
    }
    if let Err(e) = commit_iteration(ctx, iter, consumed_offset, terminal_batch) {
        let (status, error) = abnormal_status(cancel, e);
        run_compensation(ctx, committed, completed);
        return Err(RunOutcome::Terminal(status, error, Vec::new()));
    }
    Ok(())
}

/// The frontier a seed opens, or the outcome that ends the run instead.
fn seed_frontier(
    ctx: &ChainCtx,
    seed: IterationSeed,
    iter: u32,
    cancel: &AtomicBool,
    committed: &[Vec<StageRecord>],
    completed: &[StageRecord],
) -> Result<(IterationStart, Option<u64>), RunOutcome> {
    match seed {
        IterationSeed::Frontier(start, consumed) => Ok((start, consumed)),
        // The loop ran out of work (single pass done, chained loop released
        // an empty batch, inbox closed): a clean terminal.
        IterationSeed::Done => Err(terminal_or_cancelled(cancel)),
        // The park record names the last COMMITTED iteration — `iter` is the
        // one it would have run next — so `parked(N)` always follows
        // `iter_committed(N)`, and a resume enters N+1 either way.
        IterationSeed::Park => Err(park(ctx, iter.saturating_sub(1))),
        IterationSeed::Cancelled => Err(RunOutcome::Terminal(
            "cancelled".to_string(),
            String::new(),
            Vec::new(),
        )),
        // The seed cannot be built (an unre-seedable journaled handoff, an
        // entry payload the channel rejects) — terminate abnormally and undo
        // what is outstanding.
        IterationSeed::Failed(e) => {
            run_compensation(ctx, committed, completed);
            Err(RunOutcome::Terminal(
                "error".to_string(),
                e.message,
                Vec::new(),
            ))
        }
    }
}

pub(super) type Runnable = (usize, Vec<Value>);

/// One iteration of the DAG from `start`, frontier by frontier: a node runs,
/// commits, and accumulates its batch into each child, which runs once every
/// producer committed — so a join is dispatched exactly once. `completed`
/// records stages in topological order. `Ok` is the batch the leaves
/// released; `Err` the terminal status of an abnormal exit.
fn run_iteration(
    ctx: &ChainCtx,
    iter: u32,
    start: IterationStart,
    cancel: &AtomicBool,
    completed: &mut Vec<StageRecord>,
) -> Result<Vec<Value>, (String, String)> {
    let IterationStart {
        ready: mut frontier,
        mut pending,
        mut waiting,
    } = start;
    let mut children = Children {
        pending: &mut pending,
        waiting: &mut waiting,
        leaf_batch: Vec::new(),
    };

    while !frontier.is_empty() {
        // A cooperative stop before a level starts is partial progress: stop
        // advancing and roll back what completed.
        if cancel.load(Ordering::Acquire) {
            return Err(("cancelled".to_string(), String::new()));
        }

        let (level, failure) = super::step::run_stage(ctx, frontier);
        let mut next: Vec<Runnable> = Vec::new();

        for (node, out) in level {
            let batch = advance_node(ctx, iter, node, out, failure.is_some(), cancel, completed)?;
            if failure.is_none() {
                children.accumulate(ctx, node, batch, &mut next, cancel)?;
            }
        }

        if let Some(e) = failure {
            return Err(abnormal_status(cancel, e));
        }
        frontier = next;
    }

    Ok(children.leaf_batch)
}

/// Each child's input batch accumulating until every producer committed, and
/// what the leaves released.
struct Children<'a> {
    pending: &'a mut HashMap<usize, Vec<Value>>,
    waiting: &'a mut HashMap<usize, usize>,
    leaf_batch: Vec<Value>,
}

impl Children<'_> {
    /// Accumulate — never overwrite — this producer's output into each child
    /// through that edge's mapping; a leaf's batch is the iteration's output.
    fn accumulate(
        &mut self,
        ctx: &ChainCtx,
        node: usize,
        batch: Vec<Value>,
        next: &mut Vec<Runnable>,
        cancel: &AtomicBool,
    ) -> Result<(), (String, String)> {
        let children = &ctx.dag.children[node];
        if children.is_empty() {
            self.leaf_batch.extend(batch);
            return Ok(());
        }
        for &child in children {
            extend_mapped(
                self.pending.entry(child).or_default(),
                ctx.edge(node, child),
                batch.iter().map(Arc::clone),
            )
            .map_err(|e| abnormal_status(cancel, e))?;
            let outstanding = self
                .waiting
                .entry(child)
                .or_insert_with(|| ctx.dag.parents[child].len());
            *outstanding -= 1;
            if *outstanding == 0 {
                self.waiting.remove(&child);
                next.push((child, self.pending.remove(&child).unwrap_or_default()));
            }
        }
        Ok(())
    }
}

/// Record one committed node — intent markers first, then the proc-boundary
/// commit, then its stage record — and answer its released batch. The order
/// is the at-least-once contract: a crash between marker and ack resolves as
/// "not committed → re-run". A node beside a failed sibling still records
/// (it ran and needs undo) but does not commit.
fn advance_node(
    ctx: &ChainCtx,
    iter: u32,
    node: usize,
    out: super::step::StageOut,
    sibling_failed: bool,
    cancel: &AtomicBool,
    completed: &mut Vec<StageRecord>,
) -> Result<Vec<Value>, (String, String)> {
    let proc = ctx.proc(node);
    let slots = &ctx.stage_entities[node];
    if proc.compensate_overridden {
        for seg_run in &out.entities {
            emit_compensation_record(
                COMPENSATION_INTENT_PAYLOAD_ID,
                &slots[seg_run.entity_index].entity_identity,
                ctx.persistence_enabled,
            );
        }
    }
    let commit = if sibling_failed {
        Ok(())
    } else {
        commit_proc_boundary(ctx, node, iter, proc, &out.entities, slots, &out.batch)
    };
    completed.push(StageRecord {
        stage_idx: node,
        iter,
        entities: out.entities,
    });
    if let Err(e) = commit {
        return Err(abnormal_status(cancel, e));
    }
    Ok(out.batch)
}

/// `cancelled` when a stop tripped the token, else `ok`.
fn terminal_or_cancelled(cancel: &AtomicBool) -> RunOutcome {
    let status = if cancel.load(Ordering::Acquire) {
        "cancelled"
    } else {
        "ok"
    };
    RunOutcome::Terminal(status.to_string(), String::new(), Vec::new())
}

/// `cancelled` when the token tripped (the stop interrupted the wait), else
/// the error.
pub(super) fn abnormal_status(cancel: &AtomicBool, error: WorkflowError) -> (String, String) {
    if cancel.load(Ordering::Acquire) {
        ("cancelled".to_string(), String::new())
    } else {
        ("error".to_string(), error.message)
    }
}

/// One stage's slots through the stream `build_entity`, so state and identity
/// are what a stream operative's would be.
fn build_stage_entities(
    topology: &Arc<OperativeTopology>,
    instance_id: &Arc<str>,
    proc_id: ProcedureId,
    proc: &ProcedureDescriptor,
    persistence_enabled: bool,
) -> Vec<Arc<EntitySlot>> {
    let build = |entity_key: Option<&str>, entity_index: usize| {
        (proc.build_entity)(BuildEntityArgs {
            topology: Arc::clone(topology),
            instance: Weak::new(),
            instance_id: Arc::clone(instance_id),
            procedure_id: proc_id,
            entity_key,
            entity_index,
            persistence_enabled,
        })
    };
    match proc.mode {
        EntityMode::Single => vec![build(None, 0)],
        EntityMode::Static => proc
            .entity_keys
            .iter()
            .enumerate()
            .map(|(idx, key)| build(Some(key), idx))
            .collect(),
        // Rejected at registration; defensively yield no entities.
        EntityMode::Dynamic => Vec::new(),
    }
}

/// Trips `cancel` once `max_duration` elapses, unless the body finished first.
fn spawn_duration_watcher(
    duration: Duration,
    cancel: Arc<AtomicBool>,
    finished: Arc<AtomicBool>,
) -> JoinHandle<()> {
    thread::Builder::new()
        .name("flezha-workflow-timer".to_string())
        .spawn(move || {
            if crate::util::sleep_interruptible(duration, Some(&finished)) {
                cancel.store(true, Ordering::Release);
            }
        })
        .expect("spawning flezha-workflow-timer thread should not fail")
}
