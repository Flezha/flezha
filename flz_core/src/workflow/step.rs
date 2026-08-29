//! One stage of a workflow chain: its step activations (one per entity
//! partition, on the poll core's direct queue) and the barrier its driver
//! waits at. The invariant: a step runs ONE attempt and releases its worker;
//! the retry ladder is timed at the barrier, so a backoff is an entry in a
//! map, never a sleeping worker.

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

/// One stage's join result: the batch released to the next stage, plus the
/// per-entity record kept for a possible rollback.
pub(super) struct StageOut {
    pub(super) batch: Vec<Value>,
    pub(super) entities: Vec<EntityRun>,
}

/// One entity's stage result: collected output plus staged plugin output.
type EntityOut = (Vec<Value>, Vec<StagedOutput>);

/// Which entity partition a step belongs to.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct StepKey {
    node: usize,
    entity_index: usize,
}

/// Run one frontier as step activations and join on one barrier. A failing
/// entity climbs its ladder in place; its siblings are never re-run. Answers
/// every node that completed whole (ascending, so parents precede children)
/// plus the first error; a node with a failing entity yields nothing, one with
/// nothing to do yields an empty `StageOut`.
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

/// Run one entity's `compensate` hook as its own step, handing back the undo
/// output it staged. Best-effort: a failing hook is logged, never stranding
/// the rest of the plan.
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

/// Partition a batch across a stage's entities: everything to entity 0 in
/// single mode, by `entities_for` keys in static mode.
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

/// The driver's stage barrier: every partition of a frontier, its place on
/// the ladder, and the answers so far.
struct StageBarrier {
    tx: Sender<StepDone>,
    rx: Receiver<StepDone>,
    ladders: HashMap<StepKey, Ladder>,
    runs: BTreeMap<usize, Vec<EntityRun>>,
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
    /// mid-run when the level completes — timing the ladder here.
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

    /// Re-submit everything whose backoff elapsed, then block for the next
    /// answer, at most until the nearest attempt is due.
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

    fn nearest_due(&self) -> Option<Instant> {
        self.ladders
            .values()
            .filter_map(|ladder| ladder.retry.as_ref().map(|(at, _)| *at))
            .min()
    }

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

    /// A retryable failure with budget left backs off; anything else fails
    /// the node, dropping its whole stage output.
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

    fn fail(&mut self, node: usize, error: WorkflowError) {
        self.failed.insert(node);
        self.first_err.get_or_insert(error);
    }

    /// Give up on every unresolved partition rather than hang the level.
    fn poison_remaining(&mut self, message: &'static str) {
        for (key, _) in std::mem::take(&mut self.ladders) {
            self.fail(key.node, WorkflowError::from(message));
        }
    }

    /// A cooperative stop cuts every backoff short, failing each node on the
    /// error it was climbing away from.
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

/// One partition's place on the retry ladder: the batch to re-run from the
/// top (at-least-once), the attempt, and while it backs off the due time and
/// the failure. Each attempt collects into a fresh sink, so a failed
/// attempt's output is never double-counted.
struct Ladder {
    items: Vec<Value>,
    attempt: u32,
    retry: Option<(Instant, WorkflowError)>,
}

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

/// Hand one entity's step to the SDK's workers on the poll core's direct
/// queue — ahead of entity work and exempt from the cap, so what a driver
/// waits on is never queued behind work that blocks.
fn submit_step(ctx: &ChainCtx, key: StepKey, work: StepWork, tx: &Sender<StepDone>) {
    crate::poll_core::global().submit_direct(EnginePayload::WorkflowStep(WorkflowStep {
        slot: Arc::clone(&ctx.stage_entities[key.node][key.entity_index]),
        work,
        instance_id: Arc::clone(ctx.instance_id),
        staging: ctx.journaling(),
        reply: StepReply::new(key, tx.clone()),
    }));
}

/// One partition's share of a stage, as an SDK worker receives it.
pub struct WorkflowStep {
    pub(crate) slot: Arc<EntitySlot>,
    pub(crate) work: StepWork,
    instance_id: Arc<str>,
    staging: bool,
    reply: StepReply,
}

/// A stage batch, or the undo over the outputs that stage recorded.
pub(crate) enum StepWork {
    Items(Vec<Value>),
    Compensate(Vec<Value>),
}

struct StepDone {
    key: StepKey,
    result: Result<EntityOut, WorkflowError>,
}

/// The answer a step owes its driver, delivered exactly once — on completion,
/// on a panic, or from `Drop` if the activation is never served.
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

/// Run one step on the calling host thread and answer its driver; the guard
/// keeps the worker alive through a step that unwinds.
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

/// Fold a host's answer to a step into its barrier — the C-ABI half of
/// [`run_workflow_step`].
pub(crate) fn complete_workflow_step(
    step: WorkflowStep,
    outcome: crate::schema::ActivationOutcome,
) {
    let mut sink = WorkflowEntitySink::new(&step.instance_id, step.staging);
    let folded = crate::host::fold_workflow_outcome(&step.slot.flush_buffer, outcome, &mut sink);
    step.finish(folded, sink);
}

impl WorkflowStep {
    /// One attempt in this process: the items sequentially through the
    /// collecting path, or the `compensate` hook over the recorded outputs.
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
    /// compensation's is logged and what the hook staged is still handed back.
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

/// Per-entity collecting sink: inter-procedure output buffered for the next
/// stage, plugin-bound output staged when the run journals (`staging`) and
/// forwarded straight to the wire otherwise.
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

    /// A step dropped unserved still answers its driver, so the barrier can
    /// block on a plain `recv`: every partition owes exactly one answer.
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

    /// The guard fires once: a step that answered gets no second answer.
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
