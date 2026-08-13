//! One stage of a workflow chain: who runs it, and how its driver waits.
//!
//! A stage's work leaves the driver as **step activations** — one per entity
//! partition — on the poll core's direct queue. A typed class runs a step in
//! this process ([`run_workflow_step`]); a C-ABI host receives it as an ordinary
//! `Work` / `CompensateRun` activation and its completion folds through
//! [`complete_workflow_step`]. Either way the thread that runs user code belongs
//! to the SDK, and the only thread waiting is the driver, parked at
//! [`StageBarrier`].
//!
//! The barrier also owns the retry ladder (`workflow-entity-retry`): a step runs
//! ONE attempt and releases its worker, and the re-submission is timed here, at
//! a barrier this thread is blocked on anyway — so a backoff window is an entry
//! in a map, never a sleeping worker.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use crossbeam::channel::{unbounded, Receiver, Sender};

use super::compensation::{EntityRun, StagedOutput};
use super::driver::{ChainCtx, Runnable, Value, CANCEL_POLL_CHUNK};
use super::{WorkflowCollectSink, WorkflowError};
use crate::channel::RetryPolicy;
use crate::entity::EntitySlot;
use crate::operative::topology::{EntityMode, ProcedureDescriptor};
use crate::poll_core::EnginePayload;
use crate::util::LockRecover;

/// One stage's parallel-join result: the inter-procedure `batch` released to the
/// next stage, plus the per-entity record (which entities ran, with their
/// collected outputs) retained for a possible saga rollback.
pub(super) struct StageOut {
    pub(super) batch: Vec<Value>,
    pub(super) entities: Vec<EntityRun>,
}

/// One entity's stage result: the inter-procedure output collected for the
/// next stage plus the plugin-bound output staged for the procedure's outbox.
type EntityOut = (Vec<Value>, Vec<StagedOutput>);

/// Which entity partition a step belongs to — the barrier's bookkeeping key and
/// the address its answer comes back under.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct StepKey {
    node: usize,
    entity_index: usize,
}

/// Run one frontier — every currently-runnable DAG node — as step activations
/// and join on a single count-based barrier. All of a node's entities and all
/// the nodes of the frontier are submitted together: the nodes are independent,
/// so sibling branches genuinely interleave on the SDK's workers. A failing
/// entity climbs its retry ladder in place; its siblings keep their completed
/// results buffered and are never re-run.
///
/// Returns the `StageOut` of every node that completed **whole** (ascending by
/// node, so parents keep preceding children) plus the first error seen. A node
/// with a failing entity yields nothing: its staged plugin output is dropped
/// with the error, exactly as a failing stage's always was — a failed procedure
/// leaks nothing. A node whose entities all got an empty partition still yields
/// an empty `StageOut`: a procedure with nothing to do is done.
pub(super) fn run_stage(
    ctx: &ChainCtx,
    frontier: Vec<Runnable>,
) -> (Vec<(usize, StageOut)>, Option<WorkflowError>) {
    let mut barrier = StageBarrier::new(frontier.iter().map(|(node, _)| *node), ctx.retry);
    for (node, batch) in &frontier {
        let slots = &ctx.stage_entities[*node];
        for (entity_index, items) in partition(ctx.proc(*node), slots.len(), batch)
            .into_iter()
            .enumerate()
        {
            if !items.is_empty() {
                barrier.submit(
                    ctx,
                    StepKey {
                        node: *node,
                        entity_index,
                    },
                    items,
                );
            }
        }
    }
    barrier.join(ctx)
}

/// Run one entity's `compensate` hook — as its own step activation, so undo
/// crosses to the SDK exactly as forward work does — and hand back whatever
/// undo output it staged (under that entity's identity, for the `compensated`
/// journal entry — `workflow-transactional-outbox` D5). Best-effort: a failing
/// hook is logged where it runs, so one broken undo cannot strand the rest of
/// the plan.
pub(super) fn run_compensation_step(
    ctx: &ChainCtx,
    node: usize,
    seg_run: &EntityRun,
) -> Vec<StagedOutput> {
    let (tx, rx) = unbounded::<StepDone>();
    let key = StepKey {
        node,
        entity_index: seg_run.entity_index,
    };
    let outputs: Vec<Value> = seg_run.outputs.iter().map(Arc::clone).collect();
    submit_step(ctx, key, StepWork::Compensate(outputs), &tx);
    match rx.recv() {
        Ok(StepDone {
            result: Ok((_, staged)),
            ..
        }) => staged,
        _ => Vec::new(),
    }
}

/// Partition an input batch across a stage's entities. Single mode routes every
/// item to entity 0; Static mode routes by the procedure's `entities_for` keys.
fn partition(proc: &ProcedureDescriptor, entity_count: usize, batch: &[Value]) -> Vec<Vec<Value>> {
    let mut buckets: Vec<Vec<Value>> = vec![Vec::new(); entity_count];
    match proc.mode {
        EntityMode::Single => {
            if let Some(bucket) = buckets.first_mut() {
                bucket.extend(batch.iter().map(Arc::clone));
            }
        }
        EntityMode::Static => {
            let key_to_idx: HashMap<&str, usize> = proc
                .entity_keys
                .iter()
                .enumerate()
                .map(|(i, k)| (*k, i))
                .collect();
            for item in batch {
                for key in (proc.entities_for_erased)(item.as_ref()) {
                    if let Some(&idx) = key_to_idx.get(key) {
                        buckets[idx].push(Arc::clone(item));
                    }
                }
            }
        }
        EntityMode::Dynamic => {}
    }
    buckets
}

/// The driver's stage barrier: every entity partition of a frontier, its place
/// on the retry ladder, and the answers collected so far.
struct StageBarrier {
    tx: Sender<StepDone>,
    rx: Receiver<StepDone>,
    /// One ladder per submitted partition, alive until that partition resolves.
    ladders: HashMap<StepKey, Ladder>,
    runs: BTreeMap<usize, Vec<EntityRun>>,
    /// Nodes an entity failed for: they yield nothing to the next stage.
    failed: HashSet<usize>,
    first_err: Option<WorkflowError>,
    max_attempts: u32,
}

impl StageBarrier {
    fn new(nodes: impl Iterator<Item = usize>, retry: RetryPolicy) -> Self {
        let (tx, rx) = unbounded::<StepDone>();
        Self {
            tx,
            rx,
            ladders: HashMap::new(),
            runs: nodes.map(|node| (node, Vec::new())).collect(),
            failed: HashSet::new(),
            first_err: None,
            max_attempts: retry.max_attempts.max(1),
        }
    }

    /// Hand one entity's first attempt out and open its ladder.
    fn submit(&mut self, ctx: &ChainCtx, key: StepKey, items: Vec<Value>) {
        submit_step(ctx, key, StepWork::Items(items.clone()), &self.tx);
        self.ladders.insert(
            key,
            Ladder {
                items,
                attempt: 1,
                retry: None,
            },
        );
    }

    /// Wait for every submitted partition — even on failure, so nothing is
    /// mid-run when the level completes — timing the retry ladder here, then
    /// assemble what completed whole.
    fn join(mut self, ctx: &ChainCtx) -> (Vec<(usize, StageOut)>, Option<WorkflowError>) {
        while !self.ladders.is_empty() {
            match self.next_answer(ctx) {
                Some(StepDone {
                    key,
                    result: Ok(out),
                }) => self.bank(key, out),
                Some(StepDone {
                    key,
                    result: Err(error),
                }) => self.fail_or_retry(ctx, key, error),
                // The wait ended with no answer: a due attempt was re-submitted,
                // or a stop arrived — which abandons every waiting ladder on the
                // failure it was climbing away from, exactly as the in-line
                // backoff did.
                None => {
                    if ctx.instance.cancel.load(Ordering::Acquire) {
                        self.abandon_backoffs();
                    }
                }
            }
        }
        self.into_level()
    }

    /// Re-submit everything whose backoff has elapsed, then block for the next
    /// answer — no longer than the nearest attempt still owes, and never past
    /// the cancellation granularity the in-line backoff had.
    fn next_answer(&mut self, ctx: &ChainCtx) -> Option<StepDone> {
        let now = Instant::now();
        let due: Vec<StepKey> = self
            .ladders
            .iter()
            .filter(|(_, ladder)| ladder.retry.as_ref().is_some_and(|(at, _)| *at <= now))
            .map(|(key, _)| *key)
            .collect();
        for key in due {
            let Some(ladder) = self.ladders.get_mut(&key) else {
                continue;
            };
            ladder.retry = None;
            ladder.attempt += 1;
            let items = ladder.items.clone();
            submit_step(ctx, key, StepWork::Items(items), &self.tx);
        }

        match self.nearest_due() {
            Some(due) => self
                .rx
                .recv_timeout(due.saturating_duration_since(now).min(CANCEL_POLL_CHUNK))
                .ok(),
            // Nothing is backing off, so every unresolved partition has an
            // activation in flight whose reply guard owes an answer: block for
            // it, exactly as the barrier always did. A disconnect is impossible
            // while this barrier holds its own sender, but must not hang the
            // level on it either.
            None => match self.rx.recv() {
                Ok(done) => Some(done),
                Err(_) => {
                    self.poison_remaining("workflow step activation dropped");
                    None
                }
            },
        }
    }

    /// When the earliest pending attempt comes due, if any is pending.
    fn nearest_due(&self) -> Option<Instant> {
        self.ladders
            .values()
            .filter_map(|ladder| ladder.retry.as_ref().map(|(at, _)| *at))
            .min()
    }

    /// A completed partition joins its node's runs and closes its ladder.
    fn bank(&mut self, key: StepKey, (outputs, staged): EntityOut) {
        self.ladders.remove(&key);
        if let Some(runs) = self.runs.get_mut(&key.node) {
            runs.push(EntityRun {
                entity_index: key.entity_index,
                outputs,
                staged,
            });
        }
    }

    /// A retryable failure with budget left starts a backoff; anything else
    /// fails the node — its whole stage output is dropped with the error and
    /// its siblings are never re-run.
    fn fail_or_retry(&mut self, ctx: &ChainCtx, key: StepKey, error: WorkflowError) {
        let Some(ladder) = self.ladders.get_mut(&key) else {
            return;
        };
        if error.non_retryable || ladder.attempt >= self.max_attempts {
            self.ladders.remove(&key);
            self.fail(key.node, error);
            return;
        }
        let delay = crate::retry::note_failed_attempt(
            &step_label(ctx, key),
            ladder.attempt,
            ctx.retry,
            &error,
        );
        ladder.retry = Some((Instant::now() + delay, error));
    }

    /// Record a node as failed on the first error that reached it.
    fn fail(&mut self, node: usize, error: WorkflowError) {
        self.failed.insert(node);
        self.first_err.get_or_insert(error);
    }

    /// Give up on every unresolved partition — the level must not hang on a
    /// step nothing will answer.
    fn poison_remaining(&mut self, message: &'static str) {
        for (key, _) in std::mem::take(&mut self.ladders) {
            self.fail(key.node, WorkflowError::from(message));
        }
    }

    /// A cooperative stop cuts every pending backoff short: each abandoned
    /// ladder fails its node on the error it was climbing away from.
    fn abandon_backoffs(&mut self) {
        let abandoned: Vec<(StepKey, WorkflowError)> = self
            .ladders
            .extract_if(|_, ladder| ladder.retry.is_some())
            .filter_map(|(key, ladder)| ladder.retry.map(|(_, error)| (key, error)))
            .collect();
        for (key, error) in abandoned {
            self.fail(key.node, error);
        }
    }

    /// The nodes that completed whole, ascending by node so parents keep
    /// preceding children, plus the first error the level saw.
    fn into_level(self) -> (Vec<(usize, StageOut)>, Option<WorkflowError>) {
        let level = self
            .runs
            .into_iter()
            .filter(|(node, _)| !self.failed.contains(node))
            .map(|(node, mut entities)| {
                // Deterministic downstream batch: entities in index order (each
                // entity's own item order is preserved by its step).
                entities.sort_by_key(|s| s.entity_index);
                let batch: Vec<Value> = entities
                    .iter()
                    .flat_map(|s| s.outputs.iter().map(Arc::clone))
                    .collect();
                (node, StageOut { batch, entities })
            })
            .collect();
        (level, self.first_err)
    }
}

/// One entity partition's place on the retry ladder (`workflow-entity-retry`),
/// kept driver-side: the batch to re-run from the top (at-least-once, the same
/// idempotency contract crash-resume imposes), the attempt it is on, and —
/// while it backs off — when the next attempt is due plus the failure it is
/// climbing away from. A backoff is an entry in this map, never a sleeping
/// thread: the worker that ran the failed attempt was released when it
/// answered, and each attempt collects into a fresh sink, so a failed
/// attempt's output is discarded rather than double-counted.
struct Ladder {
    items: Vec<Value>,
    attempt: u32,
    retry: Option<(Instant, WorkflowError)>,
}

/// The failing work, as the retry log names it.
fn step_label(ctx: &ChainCtx, key: StepKey) -> String {
    format!(
        "workflow {} procedure {} entity {}",
        ctx.instance_id,
        ctx.proc(key.node).name,
        ctx.stage_entities[key.node][key.entity_index]
            .entity_key
            .as_deref()
            .unwrap_or("-")
    )
}

/// Hand one entity's step to the SDK's workers: an activation on the poll
/// core's direct queue — the queue every host crossing already rides, served
/// ahead of entity work and exempt from the jobs-underway cap, so the work a
/// driver is blocked on is never queued behind work that blocks.
fn submit_step(ctx: &ChainCtx, key: StepKey, work: StepWork, tx: &Sender<StepDone>) {
    crate::poll_core::global().submit_direct(EnginePayload::WorkflowStep(WorkflowStep {
        slot: Arc::clone(&ctx.stage_entities[key.node][key.entity_index]),
        work,
        instance_id: Arc::clone(ctx.instance_id),
        staging: ctx.journaling(),
        reply: StepReply::new(key, tx.clone()),
    }));
}

/// One entity partition's share of a stage, handed out as an activation: the
/// unit of workflow work an SDK worker runs.
pub struct WorkflowStep {
    pub(crate) slot: Arc<EntitySlot>,
    pub(crate) work: StepWork,
    instance_id: Arc<str>,
    staging: bool,
    reply: StepReply,
}

/// What a step runs for its entity: a stage batch, or the saga undo over the
/// outputs that stage recorded.
pub(crate) enum StepWork {
    Items(Vec<Value>),
    Compensate(Vec<Value>),
}

/// One step's answer to the driver's barrier.
struct StepDone {
    key: StepKey,
    result: Result<EntityOut, WorkflowError>,
}

/// The answer a step owes its driver, delivered exactly once: on completion, on
/// a panic inside it, or — if the activation is dropped unserved — from this
/// guard's `Drop`. A driver parked at its barrier therefore never waits on a
/// step that will never answer.
struct StepReply {
    key: StepKey,
    tx: Sender<StepDone>,
    answered: bool,
}

impl StepReply {
    fn new(key: StepKey, tx: Sender<StepDone>) -> Self {
        Self {
            key,
            tx,
            answered: false,
        }
    }

    fn send(&mut self, result: Result<EntityOut, WorkflowError>) {
        self.answered = true;
        let _ = self.tx.send(StepDone {
            key: self.key,
            result,
        });
    }
}

impl Drop for StepReply {
    fn drop(&mut self) {
        if !self.answered {
            self.send(Err(WorkflowError::from("workflow step activation dropped")));
        }
    }
}

/// Run one workflow step on the calling host thread and answer its driver. The
/// typed layer's poll workers reach this through their activation arm; the
/// panic guard is what keeps a worker alive through a step that unwinds past
/// the invocation's own containment.
pub fn run_workflow_step(step: WorkflowStep) {
    let mut sink = WorkflowEntitySink::new(&step.instance_id, step.staging);
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        step.run_in_process(&mut sink)
    }));
    step.finish(
        outcome.unwrap_or_else(|panic| Err(WorkflowError::from(crate::util::format_panic(panic)))),
        sink,
    );
}

/// Fold a host's answer to a step into its driver's barrier — the C-ABI half of
/// [`run_workflow_step`], run by whichever host thread called `complete`.
pub(crate) fn complete_workflow_step(
    step: WorkflowStep,
    outcome: crate::schema::ActivationOutcome,
) {
    let mut sink = WorkflowEntitySink::new(&step.instance_id, step.staging);
    let folded = crate::host::fold_workflow_outcome(&step.slot.flush_buffer, outcome, &mut sink);
    step.finish(folded, sink);
}

impl WorkflowStep {
    /// One attempt, in this process: the entity's items run sequentially
    /// (per-entity ordering preserved) through the monomorphized dispatcher's
    /// collecting path, or its `compensate` hook over the recorded outputs.
    fn run_in_process(&self, sink: &mut WorkflowEntitySink) -> Result<(), WorkflowError> {
        let mut state = self.slot.state.lock_recover();
        let dispatcher = &self.slot.dispatcher;
        match &self.work {
            StepWork::Items(items) => items.iter().try_for_each(|item| {
                dispatcher.run_collecting(Arc::clone(item), &mut state, &self.slot.topology, sink)
            }),
            StepWork::Compensate(outputs) => dispatcher.compensate_collecting(
                outputs.clone(),
                &mut state,
                &self.slot.topology,
                sink,
            ),
        }
    }

    /// Answer the driver. A stage failure travels to the barrier; a
    /// compensation's does not — undo is best-effort, so its error is logged
    /// and whatever the hook staged is still handed back for the `compensated`
    /// journal entry.
    fn finish(mut self, outcome: Result<(), WorkflowError>, sink: WorkflowEntitySink) {
        let compensating = matches!(self.work, StepWork::Compensate(_));
        let result = match outcome {
            Ok(()) => Ok((sink.collected, sink.staged)),
            Err(error) if compensating => {
                eprintln!(
                    "[flezha] workflow {} compensation error: {error}",
                    self.instance_id
                );
                Ok((sink.collected, sink.staged))
            }
            Err(error) => Err(error),
        };
        self.reply.send(result);
    }
}

/// Per-entity collecting sink for the structured driver. Inter-procedure
/// output is buffered for the next stage. Plugin-bound output is **staged** for
/// the procedure's transactional outbox when the run commits its proc boundaries
/// (`staging`), so nothing reaches a plugin before the proc's journal entry is
/// durable; otherwise — an opted-out class or a transport-less run, neither of
/// which journals — it is forwarded straight to the wire as before.
struct WorkflowEntitySink {
    instance_id: Arc<str>,
    collected: Vec<Value>,
    staged: Vec<StagedOutput>,
    staging: bool,
}

impl WorkflowEntitySink {
    fn new(instance_id: &Arc<str>, staging: bool) -> Self {
        Self {
            instance_id: Arc::clone(instance_id),
            collected: Vec::new(),
            staged: Vec::new(),
            staging,
        }
    }
}

impl WorkflowCollectSink for WorkflowEntitySink {
    fn collect(&mut self, value: Value) {
        self.collected.push(value);
    }

    fn emit_plugin_bytes(&mut self, channel: &str, bytes: Vec<u8>) {
        if self.staging {
            self.staged.push((channel.to_string(), bytes));
        } else {
            crate::runner::emit_workflow_output(self.instance_id.to_string(), bytes);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A step activation dropped without ever being served still answers its
    /// driver. That guarantee is what lets the barrier block on a plain `recv`
    /// while nothing is backing off: every unresolved partition owes exactly
    /// one answer, so the level can never hang on a step nobody ran.
    #[test]
    fn a_dropped_step_activation_still_answers_its_barrier() {
        let (tx, rx) = unbounded::<StepDone>();
        let key = StepKey {
            node: 2,
            entity_index: 1,
        };
        drop(StepReply::new(key, tx));

        let done = rx.try_recv().expect("the dropped reply answered");
        assert_eq!(done.key, key);
        let error = done.result.expect_err("a dropped step is a failure");
        assert!(
            error.to_string().contains("dropped"),
            "the answer names the drop: {error}"
        );
        assert!(
            !error.non_retryable,
            "a drop is not the author's classification, so the ladder may still climb"
        );
    }

    /// The guard fires once. A step that answered — the ordinary path — must
    /// not have a second answer land behind it when it is dropped.
    #[test]
    fn an_answered_step_does_not_answer_again_when_dropped() {
        let (tx, rx) = unbounded::<StepDone>();
        let mut reply = StepReply::new(
            StepKey {
                node: 0,
                entity_index: 0,
            },
            tx,
        );
        reply.send(Ok((Vec::new(), Vec::new())));
        drop(reply);

        assert!(rx.try_recv().expect("the answer").result.is_ok());
        assert!(rx.try_recv().is_err(), "the guard answered a second time");
    }
}
