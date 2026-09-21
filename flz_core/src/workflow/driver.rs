//! The workflow driver: one dedicated thread per instance that runs the
//! tasklet DAG, where a child runs only after every producer feeding it
//! commits, and converges every exit trigger on one
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

use super::commit::{commit_iteration, commit_task_boundary};
use super::compensation::{
    emit_compensation_record, run_compensation, run_rewind_undo, stages_from_intents, CellRun,
    StageRecord,
};
use super::instance::{InboxItem, JournalResume, WorkflowInstance};
use super::resume::{journaled_stages, next_iteration_seed, park, IterationSeed, IterationStart};
use super::spec::{TaskDag, WorkflowSpec};
use super::{LoopMode, RollbackMode, WorkflowError};
use crate::cell::CellSlot;
use crate::channel::{BuildCellArgs, ConsumerEdge, RetryPolicy, TaskletId};
use crate::flusher::CellFlusher;
use crate::operative::topology::{CellMode, OperativeTopology, TaskletDescriptor};
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
    pub(super) dag: &'a TaskDag,
    pub(super) instance: &'a WorkflowInstance,
    pub(super) instance_id: &'a Arc<str>,
    pub(super) stage_cells: &'a [Vec<Arc<CellSlot>>],
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
    /// Whether this run journals its task boundaries — the one gate on both
    /// the `TaskCommit` handshake and output staging. A transport-less run
    /// has no journal, so its plugin output goes straight to the wire.
    pub(super) fn journaling(&self) -> bool {
        self.persistence_enabled && crate::runner::coordinator_installed()
    }

    pub(super) fn task(&self, node: usize) -> &TaskletDescriptor {
        self.topology
            .tasklet(self.dag.order[node])
            .expect("dag tasklet present in topology")
    }

    /// The edge from `producer`'s output channel into `child`; `None` when
    /// the producer's `Output` already IS the consumer's `Input`.
    pub(super) fn edge(&self, producer: usize, child: usize) -> Option<&ConsumerEdge> {
        let channel_id = self.task(producer).output_channel_id?;
        let target = self.dag.order[child];
        self.topology
            .channel(channel_id)?
            .consumers
            .iter()
            .find(|e| e.tasklet == target)
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
        deregister_flushers(&instance.stage_cells);
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

    // Hold on the crash-resume gate until the Controller's Cell restores
    // land, then drive the chain over the restored state.
    instance.wait_for_restore();
    let outcome = run_structured(&spec, &instance, &args, &inbox_rx);

    finished.store(true, Ordering::Release);
    if let Some(w) = watcher {
        let _ = w.join();
    }
    // Deregister this run's per-Cell flusher entries. The registry entry
    // itself stays until the controller's stop_operative teardown, so a
    // duplicate AddOperative before then is idempotent (no re-run).
    deregister_flushers(&instance.stage_cells);

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

/// Structured path: walk the DAG; sibling branches and cells interleave.
fn run_structured(
    spec: &WorkflowSpec,
    instance: &WorkflowInstance,
    args: &str,
    inbox: &Receiver<InboxItem>,
) -> RunOutcome {
    let topology = &spec.topology;

    // Cells were built + registered up front by `start_workflow_resumable`
    // (so restore could reach them and the resume gate could hold); a durable
    // class has them already restored by the Controller's `RestoreInitState`
    // frames. The run's flusher entries are deregistered in `run`'s teardown.
    let ctx = ChainCtx {
        topology,
        dag: &spec.dag,
        instance,
        instance_id: &instance.instance_id,
        stage_cells: &instance.stage_cells,
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

/// Every stage's Cells in DAG order, built before the driver thread so a
/// restore frame always finds them registered. Each slot's `instance` is a
/// null `Weak`: the collecting path never routes through a `StreamInstance`.
pub fn build_all_stage_cells(
    spec: &WorkflowSpec,
    instance_id: &Arc<str>,
) -> Vec<Vec<Arc<CellSlot>>> {
    let topology = &spec.topology;
    let mut stage_cells: Vec<Vec<Arc<CellSlot>>> = Vec::with_capacity(spec.dag.len());
    for &task_id in &spec.dag.order {
        let task = topology
            .tasklet(task_id)
            .expect("chain tasklet present in topology");
        stage_cells.push(build_stage_cells(
            topology,
            instance_id,
            task_id,
            task,
            spec.persistence_enabled,
        ));
    }
    stage_cells
}

fn deregister_flushers(stage_cells: &[Vec<Arc<CellSlot>>]) {
    for slots in stage_cells {
        for slot in slots {
            if let Some(id) = slot.flusher_entry_id {
                CellFlusher::instance().deregister(id);
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
    let journal = ctx.instance.task_journal_resume();
    if !resumed.is_empty() && (journal.is_none() || ctx.instance.any_compensation_done()) {
        run_compensation(ctx, &[], &resumed);
        return Err(RunOutcome::Terminal(
            "error".to_string(),
            "resumed compensation after crash".to_string(),
            Vec::new(),
        ));
    }

    let resume = journal.unwrap_or_default();

    // The last committed iteration asked to end the instance: its commit is
    // durable, so the run ends as it would have before the crash.
    if let Some(result) = &resume.completion {
        return Err(RunOutcome::Terminal(
            "ok".to_string(),
            String::new(),
            result.clone(),
        ));
    }

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
    let committed_nodes: Vec<usize> = if resume.committed_task_idxs.contains(&0) {
        resume
            .committed_task_idxs
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
        let (terminal_batch, completion) =
            match run_iteration(ctx, iter, start, cancel, &mut completed) {
                Ok(released) => released,
                Err((status, error)) => {
                    run_compensation(ctx, &committed, &completed);
                    return RunOutcome::Terminal(status, error, Vec::new());
                }
            };
        if let Err(outcome) = seal_iteration(
            ctx,
            Seal {
                iter,
                consumed_offset,
                terminal_batch: &terminal_batch,
                completion,
            },
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

/// What an iteration hands its boundary.
struct Seal<'a> {
    iter: u32,
    consumed_offset: u64,
    terminal_batch: &'a [Value],
    /// A tasklet's `complete`: the run ends once the commit is durable.
    completion: Option<Vec<u8>>,
}

/// The iteration boundary: commit it durably, then the loop enters the next
/// iteration. A non-looping chain, or one a tasklet completed, finishes
/// here; a refused commit undoes what is outstanding. `Err` ends the run.
fn seal_iteration(
    ctx: &ChainCtx,
    seal: Seal,
    cancel: &AtomicBool,
    committed: &[Vec<StageRecord>],
    completed: &[StageRecord],
) -> Result<(), RunOutcome> {
    if ctx.loop_mode == LoopMode::Once {
        return Err(RunOutcome::Terminal(
            "ok".to_string(),
            String::new(),
            seal.completion.unwrap_or_default(),
        ));
    }
    if let Err(e) = commit_iteration(
        ctx,
        seal.iter,
        seal.consumed_offset,
        seal.terminal_batch,
        seal.completion.as_deref(),
    ) {
        let (status, error) = abnormal_status(cancel, e);
        run_compensation(ctx, committed, completed);
        return Err(RunOutcome::Terminal(status, error, Vec::new()));
    }
    match seal.completion {
        Some(result) => Err(RunOutcome::Terminal(
            "ok".to_string(),
            String::new(),
            result,
        )),
        None => Ok(()),
    }
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

/// What a node or a whole iteration releases: its batch, and the completion
/// a tasklet asked for.
type Released = (Vec<Value>, Option<Vec<u8>>);

/// One iteration of the DAG from `start`, frontier by frontier: a node runs,
/// commits, and accumulates its batch into each child, which runs once every
/// producer committed — so a join is dispatched exactly once. `completed`
/// records stages in topological order. `Ok` is the batch the leaves
/// released and the completion a tasklet asked for; `Err` the terminal
/// status of an abnormal exit.
fn run_iteration(
    ctx: &ChainCtx,
    iter: u32,
    start: IterationStart,
    cancel: &AtomicBool,
    completed: &mut Vec<StageRecord>,
) -> Result<Released, (String, String)> {
    let IterationStart {
        ready: mut frontier,
        mut pending,
        mut waiting,
        mut completion,
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
            let (batch, asked) =
                advance_node(ctx, iter, node, out, failure.is_some(), cancel, completed)?;
            completion.note(node, asked);
            if failure.is_none() {
                children.accumulate(ctx, node, batch, &mut next, cancel)?;
            }
        }

        if let Some(e) = failure {
            return Err(abnormal_status(cancel, e));
        }
        frontier = next;
    }

    Ok((children.leaf_batch, completion.result()))
}

/// The completion an iteration's tasklets asked for, resolved so it never
/// depends on which step answered first: the tasklet latest in the chain's
/// commit order wins, and within one tasklet the greatest cell key.
#[derive(Default)]
pub(super) struct Completion(Option<(usize, Vec<u8>)>);

impl Completion {
    pub(super) fn note(&mut self, node: usize, asked: Option<Vec<u8>>) {
        let Some(result) = asked else {
            return;
        };
        if self.0.as_ref().is_none_or(|(held, _)| node > *held) {
            self.0 = Some((node, result));
        }
    }

    fn result(self) -> Option<Vec<u8>> {
        self.0.map(|(_, result)| result)
    }
}

/// The completion one tasklet asked for: its greatest cell key's.
fn node_completion(ctx: &ChainCtx, node: usize, runs: &[CellRun]) -> Option<Vec<u8>> {
    let slots = &ctx.stage_cells[node];
    runs.iter()
        .filter_map(|run| Some((&slots[run.cell_index].cell_key, run.completion.as_ref()?)))
        .max_by(|a, b| a.0.cmp(b.0))
        .map(|(_, result)| result.clone())
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

/// Record one committed node — intent markers first, then the task-boundary
/// commit, then its stage record — and answer its released batch and the
/// completion it asked for. The order is the at-least-once contract: a crash
/// between marker and ack resolves as "not committed → re-run". A node beside
/// a failed sibling still records (it ran and needs undo) but does not
/// commit.
fn advance_node(
    ctx: &ChainCtx,
    iter: u32,
    node: usize,
    out: super::step::StageOut,
    sibling_failed: bool,
    cancel: &AtomicBool,
    completed: &mut Vec<StageRecord>,
) -> Result<Released, (String, String)> {
    if ctx.task(node).compensate_overridden {
        let slots = &ctx.stage_cells[node];
        for seg_run in &out.cells {
            emit_compensation_record(
                COMPENSATION_INTENT_PAYLOAD_ID,
                &slots[seg_run.cell_index].cell_identity,
                ctx.persistence_enabled,
            );
        }
    }
    let asked = node_completion(ctx, node, &out.cells);
    let commit = if sibling_failed {
        Ok(())
    } else {
        commit_task_boundary(ctx, node, iter, &out.cells, &out.batch, asked.as_deref())
    };
    completed.push(StageRecord {
        stage_idx: node,
        iter,
        cells: out.cells,
    });
    if let Err(e) = commit {
        return Err(abnormal_status(cancel, e));
    }
    Ok((out.batch, asked))
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

/// One stage's slots through the stream `build_cell`, so state and identity
/// are what a stream operative's would be.
fn build_stage_cells(
    topology: &Arc<OperativeTopology>,
    instance_id: &Arc<str>,
    task_id: TaskletId,
    task: &TaskletDescriptor,
    persistence_enabled: bool,
) -> Vec<Arc<CellSlot>> {
    let build = |cell_key: Option<&str>, cell_index: usize| {
        (task.build_cell)(BuildCellArgs {
            topology: Arc::clone(topology),
            instance: Weak::new(),
            instance_id: Arc::clone(instance_id),
            tasklet_id: task_id,
            cell_key,
            cell_index,
            persistence_enabled,
        })
    };
    match task.mode {
        CellMode::Single => vec![build(None, 0)],
        CellMode::Static => task
            .cell_keys
            .iter()
            .enumerate()
            .map(|(idx, key)| build(Some(key), idx))
            .collect(),
        // Rejected at registration; defensively yield no cells.
        CellMode::Dynamic => Vec::new(),
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
