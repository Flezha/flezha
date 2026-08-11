//! Bounded driver.
//!
//! One driver runs per started bounded instance on a **dedicated thread**: it
//! spends its life blocked — on its inbox, on a stage barrier, on a commit ack
//! — and it must never be one of the threads its own stages need to run. Stage
//! work leaves as activations ([`BoundedStep`]) the SDK's workers pick up off
//! the poll core's direct queue, in both languages; the driver's count-based
//! barrier is the only wait. The driver:
//!
//! - reads the [`BoundedInstance`](super::instance::BoundedInstance) that
//!   `start_bounded_resumable` registered on the shared `OperativeRegistry`
//!   before the driver started — its cancel token + inbox (so `BoundedStop` /
//!   `RemOperative` / `max_duration` cancel it and the wire-dispatch path
//!   delivers post-start input) and, for a structured durable class, the
//!   crash-resume gate held until the Controller's Entity restores land;
//! - runs the body — `run`-sugar ([`run_sugar`]) or the structured procedure
//!   DAG ([`run_structured`], a child procedure runs only after every producer
//!   feeding it commits, sibling branches and the entities inside each
//!   procedure run in parallel on the SDK's workers, count-based barrier
//!   between levels);
//! - converges every exit trigger on the single `BoundedCompleted` teardown,
//!   emitting status (`ok | error | cancelled`) plus the result payload.

use std::any::Any;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Weak};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crossbeam::channel::{Receiver, RecvTimeoutError};

use super::compensation::{plan_compensation, EntityRun, StageRecord, StagedOutput};
use super::instance::{BoundedInstance, CommittedProc, InboxItem, JournalResume};
use super::spec::{BoundedSpec, ProcDag};
use super::{BoundedCtx, BoundedError, BoundedRunFn, LoopMode, RollbackMode};
use crate::channel::{
    BuildEntityArgs, ChannelId, ChannelKind, ConsumerEdge, ProcedureId, RetryPolicy,
};
use crate::entity::EntitySlot;
use crate::flusher::EntityFlusher;
use crate::operative::topology::{EntityMode, OperativeTopology, ProcedureDescriptor};
use crate::state_sink::{COMPENSATION_DONE_PAYLOAD_ID, COMPENSATION_INTENT_PAYLOAD_ID};

/// Type-erased value moving between bounded stages.
pub(super) type Value = Arc<dyn Any + Send + Sync>;

/// Upper bound on the wait for the controller's `ProcCommitResponse` /
/// `IterCommitResponse` before the run terminates abnormally — the same posture
/// as any mid-run channel loss (the un-acked proc/iteration re-runs on resume,
/// at-least-once).
const PROC_COMMIT_ACK_TIMEOUT: Duration = Duration::from_secs(30);
/// How often a blocked driver re-checks for a cooperative stop: the commit-ack
/// wait, the inbox wait, and a stage's retry backoff all wake at this cadence
/// (mirrors the restore gate's `RESUME_WAIT_CHUNK`).
pub(super) const CANCEL_POLL_CHUNK: Duration = Duration::from_millis(200);

/// How long an inbox-driven loop waits at a committed iteration boundary before
/// it parks (eviction hysteresis, `bounded-durable-suspension`): a message
/// arriving inside the window continues the loop in place instead of paying a
/// park/rehydrate cycle. Overridable with `FLEZHA_BOUNDED_PARK_IDLE_MS`.
const PARK_IDLE_DEFAULT_MS: u64 = 5_000;

/// How a bounded run ended, as seen by the teardown.
enum RunOutcome {
    /// Terminal: emit `BoundedCompleted` with this `(status, error, result)`.
    Terminal(String, String, Vec<u8>),
    /// Suspended at a committed iteration boundary. `IterPark` is already on the
    /// wire; the run tears down WITHOUT a terminal completion, and the
    /// controller keeps the instance `suspended` until an input (or the wake
    /// deadline) rehydrates it.
    Parked,
}

/// Read-only context threaded through one structured chain run: the topology,
/// the resolved procedure DAG, the running instance (its id + saga
/// crash-resume markers), the pre-built per-stage entities, the
/// rollback mode, and whether the class is durable (gates the saga compensation
/// records). Bundled so the chain driver and the compensator take it by
/// reference instead of a long parameter list.
pub(super) struct ChainCtx<'a> {
    pub(super) topology: &'a Arc<OperativeTopology>,
    dag: &'a ProcDag,
    pub(super) instance: &'a BoundedInstance,
    pub(super) instance_id: &'a Arc<str>,
    pub(super) stage_entities: &'a [Vec<Arc<EntitySlot>>],
    rollback_mode: RollbackMode,
    /// Loop shape: `Once` is the single pass, the others iterate the chain with
    /// a durable commit per iteration (`bounded-durable-loop`).
    loop_mode: LoopMode,
    /// Wake deadline armed when a `PerInput` loop parks.
    wake_after: Option<Duration>,
    /// Post-start input, fed from the wire dispatch path. A `PerInput` loop
    /// seeds each iteration from it.
    inbox: &'a Receiver<InboxItem>,
    /// Per-entity retry ladder applied inside a stage (`bounded-entity-retry`).
    pub(super) retry: RetryPolicy,
    persistence_enabled: bool,
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
    fn edge(&self, producer: usize, child: usize) -> Option<&ConsumerEdge> {
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
/// The transform is author code, and unlike every other author closure on a
/// bounded chain it runs on the driver thread rather than inside an entity
/// invocation — so `ConsumerEdge`'s containment matters here the way
/// `run_collecting`'s matters for a procedure body: an unwind becomes an error
/// the chain aborts and compensates on, instead of killing the driver thread
/// and leaving the instance with no `BoundedCompleted`. Retrying is not
/// offered: an edge closure is required to be pure, so a re-run would panic
/// identically. An edge key never reaches here — a keyed edge makes its
/// consumer dynamic, and `build_dag` refuses a dynamic bounded procedure.
fn extend_mapped(
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

/// Spawn the bounded driver for one already-registered instance on a dedicated
/// thread. Fire-and-forget: the thread exits at terminal teardown. The caller
/// ([`RunnerNode::start_bounded_resumable`](crate::runner::RunnerNode))
/// builds + registers the [`BoundedInstance`] **synchronously**, before the
/// `AddOperative` frame is ACKed, so an inbox frame / `BoundedStop` / restore
/// arriving right after is never lost to a startup race.
pub fn spawn(
    spec: Arc<BoundedSpec>,
    instance: Arc<BoundedInstance>,
    inbox_rx: Receiver<InboxItem>,
    args: String,
) {
    let name = format!("flezha-bounded-{}", instance.instance_id);
    let thread_instance = Arc::clone(&instance);
    let spawned = thread::Builder::new()
        .name(name)
        .spawn(move || run(spec, thread_instance, inbox_rx, args));

    if let Err(e) = spawned {
        // Spawn failed: deregister this run's flusher entries and report a
        // terminal failure so the controller does not wait forever. The registry
        // entry is dropped by the controller's stop_operative on the completion.
        deregister_flushers(&instance.stage_entities);
        crate::runner::emit_bounded_completed(
            instance.instance_id.to_string(),
            "error".to_string(),
            format!("failed to spawn bounded driver thread: {e}"),
            Vec::new(),
        );
    }
}

fn run(
    spec: Arc<BoundedSpec>,
    instance: Arc<BoundedInstance>,
    inbox_rx: Receiver<InboxItem>,
    args: String,
) {
    let finished = Arc::new(AtomicBool::new(false));

    // max_duration backstop: trip `cancel` once it elapses, unless the body
    // finished first.
    let watcher = spec
        .max_duration
        .map(|d| spawn_duration_watcher(d, Arc::clone(&instance.cancel), Arc::clone(&finished)));

    let outcome = match spec.topology.as_ref() {
        // Structured: hold on the crash-resume gate until the Controller's
        // Entity restores land, then drive the chain over the restored state.
        Some(_) => {
            instance.wait_for_restore();
            run_structured(&spec, &instance, &args, &inbox_rx)
        }
        // Run-sugar: no Entities, no restore — a crash re-runs from the top.
        // A host class's body runs host-side, crossed through the poll
        // surface; a typed one is the in-process `run` fn.
        None if spec.host => {
            let (outcome, result) = crate::host::run_bounded_sugar(
                &instance.instance_id,
                &instance.factory_id,
                &args,
                &instance.cancel,
                inbox_rx,
            );
            classify(outcome, &instance.cancel, result)
        }
        None => run_sugar(
            spec.run,
            &instance.instance_id,
            &args,
            &instance.cancel,
            inbox_rx,
        ),
    };

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
            crate::runner::emit_bounded_completed(
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

/// Run-sugar path: a single implicit Procedure/Entity whose body is `run`.
fn run_sugar(
    run: BoundedRunFn,
    instance_id: &Arc<str>,
    args: &str,
    cancel: &Arc<AtomicBool>,
    inbox_rx: Receiver<InboxItem>,
) -> RunOutcome {
    let mut ctx = BoundedCtx::new(Arc::clone(instance_id), Arc::clone(cancel), inbox_rx);
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run(&mut ctx, args)));
    let (_completed, result) = ctx.into_result();
    classify(outcome, cancel, result)
}

/// Map a body outcome + cancellation state to the terminal completion tuple.
/// Precedence: a real failure (`Err`/panic) → `error`; otherwise a tripped
/// cancellation token → `cancelled`; otherwise `ok`.
fn classify(
    outcome: std::thread::Result<Result<(), BoundedError>>,
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
    spec: &BoundedSpec,
    instance: &BoundedInstance,
    args: &str,
    inbox: &Receiver<InboxItem>,
) -> RunOutcome {
    let topology = spec
        .topology
        .as_ref()
        .expect("run_structured invoked only for a structured spec");

    // Entities were built + registered up front by `start_bounded_resumable`
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
        persistence_enabled: spec.persistence_enabled,
    };
    drive_chain(&ctx, args, &instance.cancel)
}

/// Build every stage's Entities for a structured spec, in DAG order. Called
/// synchronously by `start_bounded_resumable` so the Entities are registered
/// for restore and the resume gate is armed before the driver thread — or any
/// restore frame — runs. `instance` on each slot is a null `Weak` — the bounded
/// collecting path never routes through an `UnboundedInstance`. When the class
/// is durable, each Entity's applied Changes are flushed to the WAL as they run.
pub fn build_all_stage_entities(
    spec: &BoundedSpec,
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

fn drive_chain(ctx: &ChainCtx, args: &str, cancel: &AtomicBool) -> RunOutcome {
    // Crash-resume into an in-flight compensation (saga, §4.3): a non-empty
    // reconstruction means the Controller shipped back compensation-intent
    // markers. Without a proc-journal that always means the operative had
    // terminated abnormally with (some of) its undo outstanding. WITH a
    // journal, intent markers also accumulate during normal forward progress,
    // so only a shipped compensation-done marker — written exclusively by an
    // undo pass — still proves compensation was in flight; otherwise the
    // journal decides where forward execution resumes (bounded-proc-journal).
    let resumed = reconstruct_completed_from_intents(ctx);
    let journal = ctx.instance.proc_journal_resume();
    if !resumed.is_empty() && (journal.is_none() || ctx.instance.any_compensation_done()) {
        run_compensation(ctx, &[], &resumed);
        return RunOutcome::Terminal(
            "error".to_string(),
            "resumed compensation after crash".to_string(),
            Vec::new(),
        );
    }

    let resume = journal.unwrap_or_default();

    // The journal's committed work, in the two granularities the driver needs:
    // the fully-committed iterations (`plan_compensation`'s `committed` input, so
    // keep-progress keeps them and all-or-nothing unwinds them —
    // `bounded-durable-loop` D2) and the stages of the still-in-flight iteration.
    // Both are empty for a fresh run and for a non-looping chain.
    let (mut committed, in_flight) = journaled_stages(ctx, &resume);

    // A rewind unwinds the iterations above its target before the loop resumes at
    // it (bounded-loop-rewind). The journal has already folded the forward state
    // back — `resume.iter` IS the rewound-to iteration + 1 — so all that is left
    // is the undo, and the journal is authoritative about how much of it is still
    // outstanding.
    if !resume.rewind_undo.is_empty() {
        run_rewind_undo(ctx, &resume.rewind_undo);
    }

    let mut iter = resume.iter;
    let mut consumed_offset = resume.consumed_offset;
    // Terminal batch of the previous iteration, carried in memory by a
    // handoff-chained loop; on resume it comes back from the journal instead.
    let mut carry: Option<Vec<Value>> = None;

    // Resume state, consumed by the first pass: the procedures of the in-flight
    // iteration the journal already records committed are skipped but still
    // recorded as completed, so a later failure rolls them back too. Their
    // uncommitted children are the pass's starting frontier, each re-seeded from
    // its own parent's journaled handoff.
    let mut resumed_stages = Some(in_flight);
    // The root is index 0, so "the root already committed" IS "this is a
    // mid-iteration resume" — and a journal that somehow records a child without
    // its parent re-runs the whole iteration rather than trusting the fragment.
    let mut committed_nodes: Vec<usize> = if resume.committed_proc_idxs.contains(&0) {
        resume
            .committed_proc_idxs
            .iter()
            .map(|&i| i as usize)
            .collect()
    } else {
        Vec::new()
    };

    loop {
        // Completed stages of this iteration, in completion order (parents before
        // children), so an abnormal exit rolls them back in reverse — which for a
        // DAG is reverse-topological.
        let mut completed: Vec<StageRecord> = resumed_stages.take().unwrap_or_default();

        let start = match next_iteration_seed(
            ctx,
            args,
            iter,
            &resume,
            &committed_nodes,
            carry.take(),
            cancel,
        ) {
            IterationSeed::Frontier(start, consumed) => {
                if let Some(offset) = consumed {
                    consumed_offset = offset;
                }
                start
            }
            // The loop ran out of work (single pass done, chained loop released
            // an empty batch, inbox closed): a clean terminal.
            IterationSeed::Done => return terminal_or_cancelled(cancel),
            // The park record names the last COMMITTED iteration — `iter` is the
            // one it would have run next — so `parked(N)` always follows
            // `iter_committed(N)`, and a resume enters N+1 either way.
            IterationSeed::Park => return park(ctx, iter.saturating_sub(1)),
            IterationSeed::Cancelled => {
                return RunOutcome::Terminal("cancelled".to_string(), String::new(), Vec::new());
            }
            // The seed cannot be built (an unre-seedable journaled handoff, an
            // entry payload the channel rejects) — terminate abnormally and undo
            // what is outstanding.
            IterationSeed::Failed(e) => {
                run_compensation(ctx, &committed, &completed);
                return RunOutcome::Terminal("error".to_string(), e, Vec::new());
            }
        };

        match run_iteration(ctx, iter, start, cancel, &mut completed) {
            Ok(terminal_batch) => {
                // Iteration boundary (`bounded-durable-loop`): journal
                // `iter_committed`, snapshot the entities, flush the iteration's
                // staged outbox, release the consumed inbox prefix — then enter
                // the next iteration. A non-looping chain has no boundary and
                // finishes here, exactly as before.
                if ctx.loop_mode == LoopMode::Once {
                    return RunOutcome::Terminal("ok".to_string(), String::new(), Vec::new());
                }
                if let Err(e) = commit_iteration(ctx, iter, consumed_offset, &terminal_batch) {
                    let (status, error) = abnormal_status(cancel, e);
                    run_compensation(ctx, &committed, &completed);
                    return RunOutcome::Terminal(status, error, Vec::new());
                }
                committed.push(completed);
                carry = Some(terminal_batch);
                iter += 1;
                // Every iteration after the resumed one runs the whole graph.
                committed_nodes.clear();
            }
            Err((status, error)) => {
                run_compensation(ctx, &committed, &completed);
                return RunOutcome::Terminal(status, error, Vec::new());
            }
        }
    }
}

/// One DAG node scheduled to run, with the batch it consumes.
pub(super) type Runnable = (usize, Vec<Value>);

/// Where an iteration starts: the immediately runnable nodes plus, for a
/// mid-iteration resume over a join, the contributions already banked for
/// nodes still waiting on an uncommitted producer.
#[derive(Default)]
struct IterationStart {
    /// Nodes whose every producer is satisfied, with their input batches.
    ready: Vec<Runnable>,
    /// Accumulated input batches of nodes with producers still outstanding
    /// (journaled handoffs of committed producers, on a resume).
    pending: HashMap<usize, Vec<Value>>,
    /// Producers still outstanding per waiting node.
    waiting: HashMap<usize, usize>,
}

impl IterationStart {
    /// A fresh pass: the DAG root over the seed batch.
    fn fresh(batch: Vec<Value>) -> Self {
        Self {
            ready: vec![(0, batch)],
            ..Default::default()
        }
    }
}

/// Run one iteration of the procedure DAG, starting from `start` (the root on
/// a fresh pass; the uncommitted subgraphs on a mid-iteration resume) and
/// advancing frontier by frontier: every runnable node runs its entities on
/// the pool together — sibling branches are independent, so they interleave —
/// joins on one barrier, commits, and **accumulates** its released batch into
/// each child. A child becomes runnable only once every one of its producers
/// has committed (`bounded-branching` fan-in), so a join is dispatched exactly
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
    let mut leaf_batch: Vec<Value> = Vec::new();
    let IterationStart {
        ready: mut frontier,
        mut pending,
        mut waiting,
    } = start;

    while !frontier.is_empty() {
        // A cooperative stop before a level starts is partial progress: stop
        // advancing and roll back what completed.
        if cancel.load(Ordering::Acquire) {
            return Err(("cancelled".to_string(), String::new()));
        }

        let (level, failure) = super::step::run_stage(ctx, frontier);
        let mut next: Vec<Runnable> = Vec::new();

        for (node, out) in level {
            let proc = ctx.proc(node);
            let slots = &ctx.stage_entities[node];

            // Durable saga (§4.3): record this procedure's completion — one
            // compensation-intent marker per entity that ran — so a crash
            // mid-compensation knows what still needs undo. Only for compensable
            // procedures (a hookless proc is never rolled back).
            if proc.compensate_overridden {
                for seg_run in &out.entities {
                    emit_compensation_record(
                        COMPENSATION_INTENT_PAYLOAD_ID,
                        &slots[seg_run.entity_index].entity_identity,
                        ctx.persistence_enabled,
                    );
                }
            }

            // Proc-boundary commit (bounded-proc-journal): journal the stage's
            // completion + handoff on the controller and block until the durable
            // ack — a child procedure must not race ahead of the committed
            // record. Ordered AFTER the intent markers so a crash between marker
            // and ack resolves as "proc not committed → re-run" (at-least-once).
            //
            // A sibling in the same level failed ⇒ the run is already unwinding:
            // this node's work still needs undo (it ran), but committing it —
            // and flushing the outbox that commit carries — would only have to be
            // compensated straight back.
            let commit = match failure {
                None => {
                    commit_proc_boundary(ctx, node, iter, proc, &out.entities, slots, &out.batch)
                }
                Some(_) => Ok(()),
            };
            completed.push(StageRecord {
                stage_idx: node,
                iter,
                entities: out.entities,
            });
            if let Err(e) = commit {
                return Err(abnormal_status(cancel, e));
            }
            if failure.is_some() {
                continue;
            }

            let children = &ctx.dag.children[node];
            if children.is_empty() {
                leaf_batch.extend(out.batch);
            } else {
                for &child in children {
                    // Accumulate — never overwrite — this producer's released
                    // output into the child's input batch; the child runs once
                    // all its producers have committed. A merged child receives
                    // each producer's contribution through that edge's mapping.
                    if let Err(e) = extend_mapped(
                        pending.entry(child).or_default(),
                        ctx.edge(node, child),
                        out.batch.iter().map(Arc::clone),
                    ) {
                        return Err(abnormal_status(cancel, e));
                    }
                    let outstanding = waiting
                        .entry(child)
                        .or_insert_with(|| ctx.dag.parents[child].len());
                    *outstanding -= 1;
                    if *outstanding == 0 {
                        waiting.remove(&child);
                        next.push((child, pending.remove(&child).unwrap_or_default()));
                    }
                }
            }
        }

        if let Some(e) = failure {
            return Err(abnormal_status(cancel, e.to_string()));
        }
        frontier = next;
    }

    Ok(leaf_batch)
}

/// The start of a mid-iteration resume: every uncommitted node with at least
/// one committed producer gets that producer's journaled handoff — filtered to
/// the producer's own output channel, so a branch only ever re-seeds from its
/// own edge. A node whose producers all committed is immediately runnable; a
/// join with one committed and one uncommitted producer banks the journaled
/// contribution and waits for the fresh one (`bounded-branching`
/// crash-resume).
fn resumed_start(
    ctx: &ChainCtx,
    resume: &JournalResume,
    committed: &[usize],
) -> Result<IterationStart, String> {
    let mut start = IterationStart::default();
    for node in 0..ctx.dag.len() {
        if committed.contains(&node) {
            continue;
        }
        let parents = &ctx.dag.parents[node];
        let committed_parents: Vec<usize> = parents
            .iter()
            .copied()
            .filter(|p| committed.contains(p))
            .collect();
        if committed_parents.is_empty() {
            // The root (no parents) is never here — a mid-iteration resume
            // implies the root committed — and a deeper node with no committed
            // parent becomes runnable through the normal accumulate flow.
            continue;
        }
        let mut batch: Vec<Value> = Vec::new();
        for parent in &committed_parents {
            let handoff = resume
                .proc_handoffs
                .iter()
                .find(|edge| edge.proc_idx as usize == *parent)
                .map(|edge| edge.handoff.as_slice())
                .unwrap_or_default();
            let seeded = seed_from_journal(ctx, handoff, ctx.proc(*parent).output_channel_id)?;
            extend_mapped(&mut batch, ctx.edge(*parent, node), seeded.into_iter())?;
        }
        let outstanding = parents.len() - committed_parents.len();
        if outstanding == 0 {
            start.ready.push((node, batch));
        } else {
            start.pending.insert(node, batch);
            start.waiting.insert(node, outstanding);
        }
    }
    Ok(start)
}

/// How the next iteration is seeded.
enum IterationSeed {
    /// Start the iteration here — the root alone on a fresh pass, the
    /// uncommitted subgraphs on a mid-iteration resume.
    /// `Some(offset)` when an inbox item supplied the seed (the offset the
    /// iteration commit records as consumed).
    Frontier(IterationStart, Option<u64>),
    /// The loop has no more work: terminate cleanly.
    Done,
    /// The inbox is empty at a committed boundary past the idle threshold:
    /// suspend (`bounded-durable-suspension`).
    Park,
    /// A cooperative stop arrived while waiting for input — nothing is in
    /// flight, so there is nothing to roll back.
    Cancelled,
    /// The seed could not be built; terminate abnormally.
    Failed(String),
}

/// Resolve the frontier this iteration starts from.
///
/// A mid-iteration resume (`committed` non-empty — the journal already records
/// procedures of this iteration committed) starts at exactly the uncommitted
/// subgraphs whose producers did commit. Otherwise the iteration starts at the
/// graph root, seeded by the loop's shape: the spawn args (single pass / first
/// iteration), the next inbox item (`PerInput`), or the previous iteration's
/// terminal handoff (`Chained`).
fn next_iteration_seed(
    ctx: &ChainCtx,
    args: &str,
    iter: u32,
    resume: &JournalResume,
    committed: &[usize],
    carry: Option<Vec<Value>>,
    cancel: &AtomicBool,
) -> IterationSeed {
    if !committed.is_empty() {
        return match resumed_start(ctx, resume, committed) {
            Ok(start) => IterationSeed::Frontier(start, None),
            Err(e) => IterationSeed::Failed(e),
        };
    }

    match ctx.loop_mode {
        // Single pass, and the first iteration of a chained loop: the spawn args
        // through the entry channel.
        LoopMode::Once | LoopMode::Chained if iter == 0 => {
            seed_or_fail(seed_bytes(ctx.topology, args.as_bytes().to_vec()), None)
        }
        // A single pass has no second iteration.
        LoopMode::Once => IterationSeed::Done,

        // Handoff-chained: the previous iteration's terminal batch, in memory or
        // (after a crash) re-decoded from the journaled iteration commit. An
        // empty batch means the loop has converged.
        LoopMode::Chained => {
            let batch = match carry {
                Some(batch) => batch,
                None => match seed_from_journal(ctx, &resume.handoff, None) {
                    Ok(batch) => batch,
                    Err(e) => return IterationSeed::Failed(e),
                },
            };
            if batch.is_empty() {
                IterationSeed::Done
            } else {
                IterationSeed::Frontier(IterationStart::fresh(batch), None)
            }
        }

        // Inbox-driven: one iteration per post-start input. An empty inbox past
        // the idle threshold parks the instance (durable classes only — an
        // opted-out one has no journal to park into and blocks in memory, as
        // before).
        LoopMode::PerInput => match wait_for_input(ctx, cancel) {
            Some(item) => seed_or_fail(
                seed_bytes(ctx.topology, item.payload),
                // Offset 0 = unjournaled input (opted-out class): keep the
                // recorded consumed offset where it is.
                (item.offset > 0).then_some(item.offset),
            ),
            None if cancel.load(Ordering::Acquire) => IterationSeed::Cancelled,
            None if ctx.journaling() => IterationSeed::Park,
            // Transport-less / opted-out: the inbox closed for good.
            None => IterationSeed::Done,
        },
    }
}

fn seed_or_fail(seeded: Result<Vec<Value>, String>, consumed: Option<u64>) -> IterationSeed {
    match seeded {
        Ok(batch) => IterationSeed::Frontier(IterationStart::fresh(batch), consumed),
        Err(e) => IterationSeed::Failed(e),
    }
}

/// Block for the next inbox item. `None` on a cooperative stop, on a closed
/// inbox, or — for a journaling instance — once the inbox has been idle past
/// the hysteresis window (the caller then parks). Sleeps while idle; the
/// ≤200 ms wake granularity observes cancellation promptly.
fn wait_for_input(ctx: &ChainCtx, cancel: &AtomicBool) -> Option<InboxItem> {
    let deadline = Instant::now() + park_idle();
    loop {
        if cancel.load(Ordering::Acquire) {
            return None;
        }
        match ctx.inbox.recv_timeout(CANCEL_POLL_CHUNK) {
            Ok(item) => return Some(item),
            Err(RecvTimeoutError::Timeout) => {
                if ctx.journaling() && Instant::now() >= deadline {
                    return None;
                }
            }
            Err(RecvTimeoutError::Disconnected) => return None,
        }
    }
}

/// Eviction hysteresis for an idle inbox-driven loop.
fn park_idle() -> Duration {
    let ms = std::env::var("FLEZHA_BOUNDED_PARK_IDLE_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(PARK_IDLE_DEFAULT_MS);
    Duration::from_millis(ms)
}

/// Suspend at the committed boundary of iteration `iter`: ship `IterPark` (with
/// the class's durable wake deadline, if any) and tear the run down without a
/// terminal completion. The controller journals `parked`, evicts the instance,
/// and rehydrates it on the next inbox append or at the deadline.
fn park(ctx: &ChainCtx, iter: u32) -> RunOutcome {
    let wake_deadline_ms = ctx.wake_after.map_or(0, |after| {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        (now + after).as_millis() as u64
    });
    crate::runner::emit_control_request(
        format!(
            "iter-park-{}-{}-{}",
            ctx.instance_id,
            iter,
            crate::util::nonce_suffix()
        ),
        crate::pb::command_request::Command::IterPark(crate::pb::IterPark {
            operative_id: ctx.instance_id.to_string(),
            iter,
            wake_deadline_ms,
        }),
    );
    RunOutcome::Parked
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
fn abnormal_status(cancel: &AtomicBool, error: String) -> (String, String) {
    if cancel.load(Ordering::Acquire) {
        ("cancelled".to_string(), String::new())
    } else {
        ("error".to_string(), error)
    }
}

/// Wrap one entry payload — the spawn args, or an inbox item's bytes — into the
/// entry channel's typed value.
fn seed_bytes(topology: &OperativeTopology, bytes: Vec<u8>) -> Result<Vec<Value>, String> {
    let entry = topology
        .channels
        .iter()
        .find(|c| c.kind == ChannelKind::Entry)
        .expect("structured bounded has an entry channel (validated at registration)");
    let wrap = entry
        .wrap_input
        .as_ref()
        .expect("entry channel carries a wrap_input");
    wrap(bytes)
        .map(|v| vec![v])
        .map_err(|e| format!("seeding entry channel failed: {e}"))
}

/// Decode a journaled handoff batch back into typed values via each item's
/// producing-channel codec (`bounded-proc-journal` crash-resume seeding).
///
/// `only` restricts the batch to one edge: a resumed child branch consumes just
/// the values its parent released on the channel that child reads. `None` takes
/// the whole batch — the handoff-chained loop's terminal re-seed, whose values
/// were released on the terminal channel and enter through the entry one.
fn seed_from_journal(
    ctx: &ChainCtx,
    handoff: &[(u32, Vec<u8>)],
    only: Option<ChannelId>,
) -> Result<Vec<Value>, String> {
    let mut seeded: Vec<Value> = Vec::with_capacity(handoff.len());
    for (channel_id, bytes) in handoff {
        if only.is_some_and(|edge| edge != *channel_id) {
            continue;
        }
        let codec = ctx
            .topology
            .channel(*channel_id)
            .and_then(|c| c.codec.as_ref())
            .ok_or_else(|| {
                format!(
                    "journaled handoff references channel id {channel_id} without an inter-proc \
                     codec; cannot re-seed the resumed procedure"
                )
            })?;
        let value = (codec.decode)(bytes.clone())
            .map_err(|e| format!("journaled handoff decode failed on channel {channel_id}: {e}"))?;
        seeded.push(value);
    }
    Ok(seeded)
}

/// Rebuild the journal's committed work as stage records, in the two
/// granularities the driver needs: the **fully-committed iterations** (below the
/// resume point — `plan_compensation`'s `committed` input, `bounded-durable-loop`
/// D2) and the stages of the **still-in-flight iteration**, which a resumed pass
/// skips but must still be able to roll back. One traversal, one grouping.
///
/// Entity outputs are empty, as on any crash-resume: a procedure's collected
/// inter-procedure output was never durable, so a durable compensable procedure
/// persists its undo handle in Entity state (which IS restored), per the
/// at-least-once author contract.
fn journaled_stages(
    ctx: &ChainCtx,
    resume: &JournalResume,
) -> (Vec<Vec<StageRecord>>, Vec<StageRecord>) {
    let mut by_iter = stages_by_iter(ctx, &resume.committed_procs);
    // The resume point splits the two: everything below it is committed progress.
    let in_flight = by_iter
        .split_off(&resume.iter)
        .remove(&resume.iter)
        .unwrap_or_default();
    (by_iter.into_values().collect(), in_flight)
}

/// Journaled proc commits as stage records, grouped by the iteration they ran in
/// and ordered within it by chain position (= completion order). Shared by the
/// forward resume and the rewind undo, which need the same grouping over
/// different sets of commits.
fn stages_by_iter(ctx: &ChainCtx, procs: &[CommittedProc]) -> BTreeMap<u32, Vec<StageRecord>> {
    let mut by_iter: BTreeMap<u32, Vec<StageRecord>> = BTreeMap::new();
    for proc in procs {
        if let Some(record) = stage_record(ctx, proc.proc_idx, proc.iter, &proc.entities_ran) {
            by_iter.entry(proc.iter).or_default().push(record);
        }
    }
    for stages in by_iter.values_mut() {
        // Completion order within an iteration is chain order.
        stages.sort_by_key(|s| s.stage_idx);
    }
    by_iter
}

/// One journaled proc commit as a `StageRecord`, resolving each shipped-back wire
/// entity identity to its slot index. `None` when the procedure is not in this
/// build's chain (a topology that changed under a journaled instance).
fn stage_record(
    ctx: &ChainCtx,
    proc_idx: u32,
    iter: u32,
    entities_ran: &[Vec<u8>],
) -> Option<StageRecord> {
    let stage_idx = proc_idx as usize;
    let slots = ctx.stage_entities.get(stage_idx)?;
    let entities = entities_ran
        .iter()
        .filter_map(|id| {
            slots
                .iter()
                .position(|slot| slot.entity_identity.as_slice() == id.as_slice())
        })
        .map(|entity_index| EntityRun {
            entity_index,
            outputs: Vec::new(),
            staged: Vec::new(),
        })
        .collect();
    Some(StageRecord {
        stage_idx,
        iter,
        entities,
    })
}

/// Ship `ProcCommit` for a completed stage and block until the controller's
/// durable `ProcCommitResponse` (bounded-proc-journal D3). The handoff batch is
/// serialized through the procedure's output-channel codec (validated present
/// at registration; a terminal or plugin-bound output has no codec and ships
/// an empty handoff), and the stage's staged plugin output rides along as the
/// procedure's outbox (`bounded-transactional-outbox`) — journaled with the
/// commit in one durable write, flushed by the controller only afterwards.
/// Skipped for opted-out classes and transport-less runs (unit tests /
/// standalone, where no coordinator is installed — mirroring the DLQ park's
/// sink gate); those never stage, so nothing is stranded. A failed or timed-out
/// ack is a terminal run error; the un-acked proc re-runs on resume, and its
/// staged output — never journaled — is simply dropped (at-least-once).
fn commit_proc_boundary(
    ctx: &ChainCtx,
    stage_idx: usize,
    iter: u32,
    proc: &ProcedureDescriptor,
    entities: &[EntityRun],
    slots: &[Arc<EntitySlot>],
    batch: &[Value],
) -> Result<(), String> {
    if !ctx.journaling() {
        return Ok(());
    }

    let handoff = encode_handoff(ctx, proc.output_channel_id, batch)
        .map_err(|e| format!("proc {stage_idx} {e}"))?;

    let entities_ran: Vec<Vec<u8>> = entities
        .iter()
        .map(|seg_run| slots[seg_run.entity_index].entity_identity.as_ref().clone())
        .collect();

    ship_and_await(
        ctx,
        stage_idx,
        "commit",
        true,
        crate::pb::command_request::Command::ProcCommit(crate::pb::ProcCommit {
            operative_id: ctx.instance_id.to_string(),
            proc_idx: stage_idx as u32,
            iter,
            entities_ran,
            handoff,
            outbox: entities
                .iter()
                .flat_map(|seg_run| outbox_items(&slots[seg_run.entity_index], &seg_run.staged))
                .collect(),
            compensating: false,
        }),
    )
}

/// Ship `IterCommit` for a finished loop iteration and block until the
/// controller's durable `IterCommitResponse` (`bounded-durable-loop` D1): the
/// journal records `iter_committed(iter, consumed_offset)` with the iteration's
/// terminal handoff, the instance's entities are snapshotted at the boundary,
/// the iteration's staged outbox is flushed, and the consumed inbox prefix is
/// released. The next iteration starts only on the successful reply, so a crash
/// re-runs at most the whole in-flight iteration — never a committed one.
fn commit_iteration(
    ctx: &ChainCtx,
    iter: u32,
    consumed_offset: u64,
    batch: &[Value],
) -> Result<(), String> {
    if !ctx.journaling() {
        return Ok(());
    }

    // The terminal handoff re-seeds iteration N+1 of a handoff-chained loop
    // after a crash; an inbox-driven loop re-seeds from its inbox instead and
    // ships whatever the terminal procedure released (empty when it has no
    // codec'd output channel, and for a branching graph, which has no single
    // terminal procedure — registration refuses to chain a loop over one).
    let terminal_channel = ctx
        .dag
        .single_leaf()
        .and_then(|leaf| ctx.proc(leaf).output_channel_id);
    let handoff = encode_handoff(ctx, terminal_channel, batch)
        .map_err(|e| format!("iteration {iter} {e}"))?;

    ship_and_await(
        ctx,
        iter as usize,
        "iteration commit",
        true,
        crate::pb::command_request::Command::IterCommit(crate::pb::IterCommit {
            operative_id: ctx.instance_id.to_string(),
            iter,
            consumed_offset,
            handoff,
        }),
    )
}

/// Serialize a released batch through its producing channel's inter-proc codec.
/// Empty when the procedure has no output channel (a terminal stage) or the
/// channel carries no codec (a plugin-bound output).
fn encode_handoff(
    ctx: &ChainCtx,
    channel_id: Option<u32>,
    batch: &[Value],
) -> Result<Vec<crate::pb::ProcHandoffItem>, String> {
    let Some(channel_id) = channel_id else {
        return Ok(Vec::new());
    };
    let Some(codec) = ctx
        .topology
        .channel(channel_id)
        .and_then(|c| c.codec.as_ref())
    else {
        return Ok(Vec::new());
    };

    batch
        .iter()
        .map(|value| {
            (codec.encode)(value.as_ref())
                .map(|payload| crate::pb::ProcHandoffItem {
                    channel_id,
                    payload,
                })
                .map_err(|e| format!("handoff serialization failed on channel {channel_id}: {e}"))
        })
        .collect()
}

/// One entity's staged plugin output as outbox entries, tagged with the
/// producing entity's wire identity and the entity-local emit order
/// (`seq_in_proc`) — the controller mints the flush key from that tuple.
fn outbox_items(slot: &Arc<EntitySlot>, staged: &[StagedOutput]) -> Vec<crate::pb::ProcOutboxItem> {
    staged
        .iter()
        .enumerate()
        .map(
            |(seq_in_proc, (channel, payload))| crate::pb::ProcOutboxItem {
                entity_id: slot.entity_identity.as_ref().clone(),
                seq_in_proc: seq_in_proc as u32,
                channel: channel.clone(),
                payload: payload.clone(),
            },
        )
        .collect()
}

/// Emit one durability handshake — `ProcCommit` (forward proc boundary or
/// compensation undo record) or `IterCommit` (iteration boundary) — and block
/// until the controller's durable ack. All three are "journal this, then tell me
/// it is durable" round-trips over the same reference-id-keyed reply path.
///
/// `abort_on_cancel` aborts the wait early on a cooperative cancel, which the
/// caller maps to the `cancelled` terminal status. The compensation undo record
/// passes `false`: rollback IS the response to that cancel, and abandoning the
/// record because the token is set would drop the undo output it carries — it
/// waits out the full ack deadline instead.
fn ship_and_await(
    ctx: &ChainCtx,
    idx: usize,
    what: &str,
    abort_on_cancel: bool,
    command: crate::pb::command_request::Command,
) -> Result<(), String> {
    let reference_id = format!(
        "proc-commit-{}-{}-{}",
        ctx.instance_id,
        idx,
        crate::util::nonce_suffix()
    );
    let rx = crate::shutdown::register_pending_ack(reference_id.clone());
    crate::runner::emit_control_request(reference_id.clone(), command);

    // Chunked wait so a cooperative cancel is observed promptly.
    let deadline = Instant::now() + PROC_COMMIT_ACK_TIMEOUT;
    loop {
        match rx.recv_timeout(CANCEL_POLL_CHUNK) {
            Ok(Ok(_)) => return Ok(()),
            Ok(Err(e)) => {
                return Err(format!("{what} {idx} refused by controller: {e}"));
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                if abort_on_cancel && ctx.instance.cancel.load(Ordering::Acquire) {
                    crate::shutdown::drop_pending_ack(&reference_id);
                    return Err(format!("{what} {idx} interrupted by stop"));
                }
                if Instant::now() >= deadline {
                    crate::shutdown::drop_pending_ack(&reference_id);
                    return Err(format!(
                        "{what} {idx} ack timed out after {}s",
                        PROC_COMMIT_ACK_TIMEOUT.as_secs()
                    ));
                }
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                crate::shutdown::drop_pending_ack(&reference_id);
                return Err(format!("{what} {idx} waiter dropped"));
            }
        }
    }
}

/// Roll back an abnormally-terminated chain. The in-flight iteration's completed
/// procedures are always compensated, in reverse order (per
/// [`plan_compensation`]) and, within a procedure, once per entity that ran;
/// `committed` holds the iterations the journal records as finished, which
/// keep-progress keeps and all-or-nothing unwinds after it. Procedures without a
/// `compensate` hook are skipped. Best-effort: a failing hook is logged and the
/// remaining steps still run, so one broken undo cannot strand the rest.
fn run_compensation(ctx: &ChainCtx, committed: &[Vec<StageRecord>], in_flight: &[StageRecord]) {
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
/// order (`bounded-loop-rewind`). Forced all-or-nothing: a rewind unwinds
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
fn run_rewind_undo(ctx: &ChainCtx, unwound: &[CommittedProc]) {
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
            "[flezha] bounded {} {what} journaling failed: {e}",
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
fn reconstruct_completed_from_intents(ctx: &ChainCtx) -> Vec<StageRecord> {
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
fn emit_compensation_record(payload_id: &str, entity_identity: &[u8], persistence_enabled: bool) {
    if persistence_enabled {
        crate::state_sink::send_compensation_record(payload_id, entity_identity);
    }
}

/// Build the EntitySlots for one stage: a single slot in Single mode, one per
/// declared key in Static mode. Reuses the unbounded `build_entity` closure
/// so per-Entity state + identity are identical to an unbounded operative.
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
        .name("flezha-bounded-timer".to_string())
        .spawn(move || {
            if crate::util::sleep_interruptible(duration, Some(&finished)) {
                cancel.store(true, Ordering::Release);
            }
        })
        .expect("spawning flezha-bounded-timer thread should not fail")
}
