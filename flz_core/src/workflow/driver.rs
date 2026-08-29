//! Workflow driver: one per started workflow instance, on a **dedicated
//! thread** that spends its life blocked — on its inbox, on a stage barrier, on
//! a commit ack — and must never be one of the threads its own stages need to
//! run. Stage work leaves as activations ([`WorkflowStep`]) the SDK's workers
//! pick up off the poll core's direct queue; the driver's count-based barrier
//! is the only wait. It runs the body — `run`-sugar ([`run_sugar`]) or the
//! structured procedure DAG ([`run_structured`]: a child procedure runs only
//! after every producer feeding it commits, sibling branches and the entities
//! inside each procedure run in parallel) — and converges every exit trigger
//! on the single `WorkflowCompleted` teardown, emitting status
//! (`ok | error | cancelled`) plus the result payload. The instance it drives
//! was registered before the driver started, so a `WorkflowStop`, an inbox
//! frame or a restore arriving right after is never lost to a startup race.

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

/// Type-erased value moving between workflow stages.
pub(super) type Value = Arc<dyn Any + Send + Sync>;

/// How often a blocked driver re-checks for a cooperative stop: the commit-ack
/// wait, the inbox wait, and a stage's retry backoff all wake at this cadence
/// (mirrors the restore gate's `RESUME_WAIT_CHUNK`).
pub(super) const CANCEL_POLL_CHUNK: Duration = Duration::from_millis(200);

/// Stack size for a driver thread, in KiB; unset keeps the platform default.
/// An address-space lever only (a blocked driver's stack is not resident), and
/// not free: an edge transform (`extend_mapped`) is author code on this thread,
/// and a stack overflow aborts the process rather than unwinding.
const ENV_DRIVER_STACK_KIB: &str = "FLEZHA_WORKFLOW_DRIVER_STACK_KIB";

/// How a workflow run ended, as seen by the teardown.
pub(super) enum RunOutcome {
    /// Terminal: emit `WorkflowCompleted` with this `(status, error, result)`.
    Terminal(String, String, Vec<u8>),
    /// Suspended at a committed iteration boundary. `IterPark` is already on the
    /// wire; the run tears down WITHOUT a terminal completion, and the
    /// controller keeps the instance `suspended` until an input (or the wake
    /// deadline) rehydrates it.
    Parked,
}

/// Read-only context threaded through one structured chain run: the topology,
/// the resolved procedure DAG, the running instance (its id + saga
/// crash-resume markers), the pre-built per-stage entities and the class's
/// loop, rollback and durability facts.
pub(super) struct ChainCtx<'a> {
    pub(super) topology: &'a Arc<OperativeTopology>,
    pub(super) dag: &'a ProcDag,
    pub(super) instance: &'a WorkflowInstance,
    pub(super) instance_id: &'a Arc<str>,
    pub(super) stage_entities: &'a [Vec<Arc<EntitySlot>>],
    pub(super) rollback_mode: RollbackMode,
    /// Loop shape: `Once` is the single pass, the others iterate the chain with
    /// a durable commit per iteration (`workflow-durable-loop`).
    pub(super) loop_mode: LoopMode,
    /// Wake deadline armed when a `PerInput` loop parks.
    pub(super) wake_after: Option<Duration>,
    /// Post-start input, fed from the wire dispatch path. A `PerInput` loop
    /// seeds each iteration from it.
    pub(super) inbox: &'a Receiver<InboxItem>,
    /// Per-entity retry ladder applied inside a stage (`workflow-entity-retry`).
    pub(super) retry: RetryPolicy,
    /// Sends of one barrier before an unacked commit fails the run.
    pub(super) barrier_retry_attempts: u32,
    pub(super) persistence_enabled: bool,
    /// Inbox items still owed by the controller for this bind, counted down
    /// as they arrive; the park-idle clock starts at zero.
    pub(super) owed_inbox: std::cell::Cell<u32>,
}

impl ChainCtx<'_> {
    /// Whether this run journals its proc boundaries — the single gate on both
    /// the `ProcCommit` handshake and output staging. An opted-out class or a
    /// transport-less run (unit test / standalone bench, no coordinator) has no
    /// journal to gate a flush on, so its plugin output must keep going
    /// straight to the wire rather than being staged into an outbox nobody
    /// would ever flush.
    pub(super) fn journaling(&self) -> bool {
        self.persistence_enabled && crate::runner::coordinator_installed()
    }

    /// The procedure descriptor at a DAG node (= its `stage_idx` / wire
    /// `proc_idx`).
    pub(super) fn proc(&self, node: usize) -> &ProcedureDescriptor {
        self.topology
            .procedure(self.dag.order[node])
            .expect("dag procedure present in topology")
    }

    /// The consumer edge from `producer`'s output channel into `child`, whose
    /// transform is every `map`/`filter`/`flat_map` and merge mapping declared
    /// on it, or `None` for the bare single-input case where the producer's
    /// `Output` already IS the consumer's `Input`.
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

/// Extend `dest` with a producer's released batch, put through the edge's
/// transform when it declares one. Shared by the forward accumulate and the
/// journal re-seed so a resumed chain sees exactly what the live one did; a
/// bare edge — every single-input chain — moves its values straight in.
///
/// The transform is author code running on the driver thread, so
/// `ConsumerEdge`'s containment is what turns an unwind into an error the
/// chain aborts and compensates on rather than a dead driver with no
/// `WorkflowCompleted`; a pure closure is not retried. An edge key never
/// reaches here — `build_dag` refuses a dynamic workflow procedure.
pub(super) fn extend_mapped(
    dest: &mut Vec<Value>,
    edge: Option<&ConsumerEdge>,
    batch: impl Iterator<Item = Value>,
) -> Result<(), String> {
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

/// Spawn the workflow driver for one already-registered instance on a dedicated
/// thread. Fire-and-forget: the thread exits at terminal teardown. The caller
/// ([`RunnerNode::start_workflow_resumable`](crate::runner::RunnerNode))
/// builds + registers the [`WorkflowInstance`] **synchronously**, before the
/// `AddOperative` frame is ACKed, so an inbox frame / `WorkflowStop` / restore
/// arriving right after is never lost to a startup race.
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

/// The body by class shape: a structured chain over restored Entities, a host
/// run-sugar body crossed through the poll surface, or the typed in-process
/// `run` fn.
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

/// Run-sugar path: a single implicit Procedure/Entity whose body is `run`.
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

/// Map a body outcome + cancellation state to the terminal completion tuple.
/// Precedence: a real failure (`Err`/panic) → `error`; otherwise a tripped
/// cancellation token → `cancelled`; otherwise `ok`.
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

/// Structured path: walk the procedure DAG. A child procedure starts only when
/// every producer feeding it has finished and had its commit acked
/// (count-based barrier); entities — and independent sibling branches — run in
/// parallel on the SDK's workers.
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

/// Build every stage's Entities for a structured spec, in DAG order. Called
/// synchronously by `start_workflow_resumable` so the Entities are registered
/// for restore and the resume gate is armed before the driver thread — or any
/// restore frame — runs. `instance` on each slot is a null `Weak` — the workflow
/// collecting path never routes through an `StreamInstance`. When the class
/// is durable, each Entity's applied Changes are flushed to the WAL as they run.
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

/// Deregister every per-Entity flusher entry created for a run so the
/// process-global flusher does not accumulate dead entries across jobs.
fn deregister_flushers(stage_entities: &[Vec<Arc<EntitySlot>>]) {
    for slots in stage_entities {
        for slot in slots {
            if let Some(id) = slot.flusher_entry_id {
                EntityFlusher::instance().deregister(id);
            }
        }
    }
}

/// What the journal says about where a chain resumes: the committed iterations
/// (keep-progress keeps them, all-or-nothing unwinds them), the stages of the
/// in-flight iteration, and the nodes of it already committed.
struct ResumePlan {
    resume: JournalResume,
    committed: Vec<Vec<StageRecord>>,
    in_flight: Vec<StageRecord>,
    committed_nodes: Vec<usize>,
}

/// The chain's prologue on every start: finish an in-flight compensation
/// (`Err` — the run ends there), undo what a rewind unwound, and read the
/// journal's committed work.
///
/// Crash-resume into an in-flight compensation (saga, §4.3): a non-empty
/// reconstruction means the Controller shipped back compensation-intent
/// markers. Without a proc-journal that always means the operative had
/// terminated abnormally with (some of) its undo outstanding. WITH a journal,
/// intent markers also accumulate during normal forward progress, so only a
/// shipped compensation-done marker — written exclusively by an undo pass —
/// still proves compensation was in flight; otherwise the journal decides
/// where forward execution resumes (workflow-proc-journal).
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
    // `workflow-durable-loop` D2) and the stages of the still-in-flight iteration.
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

/// The iteration boundary (`workflow-durable-loop`): journal `iter_committed`,
/// snapshot the entities, flush the iteration's staged outbox, release the
/// consumed inbox prefix — then the loop enters the next iteration. A
/// non-looping chain has no boundary and finishes here; a refused commit undoes
/// what is outstanding. `Err` is the outcome that ends the run.
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

/// The frontier a seed opens, or the outcome that ends the run instead: the
/// loop ran out of work (a clean terminal), parks at a committed boundary,
/// was cancelled while waiting for input, or could not build its seed (an
/// abnormal terminal, after undoing what is outstanding).
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
            Err(RunOutcome::Terminal("error".to_string(), e, Vec::new()))
        }
    }
}

/// One DAG node scheduled to run, with the batch it consumes.
pub(super) type Runnable = (usize, Vec<Value>);

/// Run one iteration of the procedure DAG, starting from `start` (the root on
/// a fresh pass; the uncommitted subgraphs on a mid-iteration resume) and
/// advancing frontier by frontier: every runnable node runs its entities on
/// the pool together — sibling branches are independent, so they interleave —
/// joins on one barrier, commits, and **accumulates** its released batch into
/// each child. A child becomes runnable only once every one of its producers
/// has committed (`workflow-branching` fan-in), so a join is dispatched exactly
/// once, with the concatenation of all its producers' outputs. Each completed
/// stage is recorded into `completed` in completion order, which is
/// topological (every producer precedes its consumers).
///
/// `Ok` carries the batch the leaves released — the operative is complete
/// exactly when the frontier empties, i.e. when every leaf has committed. `Err`
/// carries the terminal status of an abnormal exit (a failing procedure, a
/// cooperative stop, a refused proc commit).
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
            return Err(abnormal_status(cancel, e.to_string()));
        }
        frontier = next;
    }

    Ok(children.leaf_batch)
}

/// The children of a level's committed nodes: each child's input batch
/// accumulating until every producer has committed, and the output the leaves
/// released.
struct Children<'a> {
    pending: &'a mut HashMap<usize, Vec<Value>>,
    waiting: &'a mut HashMap<usize, usize>,
    leaf_batch: Vec<Value>,
}

impl Children<'_> {
    /// Accumulate — never overwrite — this producer's released output into
    /// each child's input batch; the child runs once all its producers have
    /// committed. A merged child receives each producer's contribution through
    /// that edge's mapping. A leaf's batch is the iteration's own output.
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

/// Record one committed node: its saga intent markers and its proc-boundary
/// commit, then its completed-stage record. Returns the node's released batch
/// for the accumulate.
///
/// Durable saga (§4.3): one compensation-intent marker per entity that ran, so
/// a crash mid-compensation knows what still needs undo — only for compensable
/// procedures (a hookless proc is never rolled back). The proc-boundary commit
/// (workflow-proc-journal) journals the stage's completion + handoff on the
/// controller and blocks until the durable ack — a child procedure must not
/// race ahead of the committed record — ordered AFTER the intent markers so a
/// crash between marker and ack resolves as "proc not committed → re-run"
/// (at-least-once). A sibling in the same level failed ⇒ the run is already
/// unwinding: this node's work still needs undo (it ran), but committing it —
/// and flushing the outbox that commit carries — would only have to be
/// compensated straight back.
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

/// Terminal status of a loop that ran out of work: `cancelled` when a stop
/// tripped the token, else a clean `ok`.
fn terminal_or_cancelled(cancel: &AtomicBool) -> RunOutcome {
    let status = if cancel.load(Ordering::Acquire) {
        "cancelled"
    } else {
        "ok"
    };
    RunOutcome::Terminal(status.to_string(), String::new(), Vec::new())
}

/// A commit failure is `cancelled` when the token tripped (the stop is what
/// interrupted the ack wait), else a terminal error.
pub(super) fn abnormal_status(cancel: &AtomicBool, error: String) -> (String, String) {
    if cancel.load(Ordering::Acquire) {
        ("cancelled".to_string(), String::new())
    } else {
        ("error".to_string(), error)
    }
}

/// Build the EntitySlots for one stage: a single slot in Single mode, one per
/// declared key in Static mode. Reuses the stream `build_entity` closure
/// so per-Entity state + identity are identical to a stream operative.
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

/// Watcher thread for `max_duration`: trips `cancel` once the duration elapses,
/// unless the body finished first (the sleep wakes every ≤200 ms to honour an
/// early finish promptly).
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
