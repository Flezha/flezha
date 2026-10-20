//! The pass decisions, under the lock: messages and passes, items, stages.

use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use super::compensation::{node_completion, unwind_steps, CellRun, RecordSpan};
use super::effects::{Frame, MessagePark};
use super::instance::{
    AfterUndo, Attempt, BarrierFor, Carry, CellPos, Deadline, Failure, Firing, FiringKey, Message,
    Pass, PassOrigin, Phase, Release, Stage, StageState, Trigger,
};
use super::resume::Seed;
use super::run::{Decide, Source};
use super::StepsError;
use crate::cell::CellSlot;
use crate::channel::ChannelId;
use crate::relay::sleep::OWED_INBOX_WAIT;
use crate::state_sink::COMPENSATION_INTENT_PAYLOAD_ID;
use crate::timer::TimerKind;

impl Decide<'_, '_, '_> {
    // -- messages and passes -----------------------------------------------------

    /// Back at a committed boundary: the next queued trigger starts its pass,
    /// then a chained loop's carry, or the instance waits, and one whose
    /// class sleeps arms its park.
    pub(super) fn enter_waiting(&mut self) {
        self.s.pass = None;
        if self.cancelled() {
            return self.terminal("cancelled", String::new(), Vec::new());
        }
        if self.r.spec.each.ends_after_first() && self.s.iter > 0 {
            return self.finish_clean();
        }
        let source = match self.next_trigger() {
            Some(Trigger::Message(message)) => Some(Source::Message(message)),
            Some(Trigger::Firing(firing)) => Some(Source::Firing(firing)),
            None => self.s.carry.take().map(Source::Carry),
        };
        if let Some(source) = source {
            self.s.phase = Phase::Starting;
            self.fx.start = Some(source);
            return;
        }
        self.s.phase = Phase::Waiting;
        self.rearm_sleep();
    }

    /// The first queued trigger this boundary may start, in arrival order: a
    /// firing always, a message only where it seeds a pass. An interrupted
    /// pass waiting for its message's re-forward runs before anything else.
    fn next_trigger(&mut self) -> Option<Trigger> {
        let fed = self.message_fed();
        let mid = self.s.resume_mid.is_some();
        let at = self.s.queue.iter().position(|trigger| match trigger {
            Trigger::Message(_) => fed,
            Trigger::Firing(_) => !mid,
        })?;
        self.s.queue.remove(at)
    }

    /// The entry channel a timer-fed pass's commit names, with the consumed
    /// offset it carries: the last message-fed pass's.
    pub(super) fn entry_point(&self) -> (ChannelId, Option<u64>) {
        self.s.entry.unwrap_or_else(|| {
            let first = self.r.spec.dag.entries.first().map(|(channel, _)| *channel);
            (
                first.expect("registration refuses a steps without an entry channel"),
                None,
            )
        })
    }

    /// `node`'s stage, while it is running in a live pass.
    pub(super) fn running_stage(&mut self, node: usize) -> Option<&mut Stage> {
        if self.r.run.pass_dead.load(Ordering::Acquire) {
            return None;
        }
        self.s.running(node)
    }

    /// Where a due firing goes: back to the caller when it runs now, owed by
    /// a stage, a new pass's or the running one of its own tasklet; otherwise
    /// it waits its turn in the queue.
    pub(super) fn admit_firing(&mut self, firing: Firing) -> Option<Firing> {
        let key = firing.key();
        if let Some(stage) = self.running_stage(firing.node) {
            if stage.claim.as_ref() == Some(&key) {
                stage.claim = None;
            } else {
                stage.owed += 1;
            }
            return Some(firing);
        }
        let queued = self.s.queue.iter().any(|trigger| {
            matches!(trigger, Trigger::Firing(f) if Arc::ptr_eq(&f.slot, &firing.slot) && f.key() == key)
        });
        if queued || self.s.phase == Phase::Done {
            return None;
        }
        if self.s.phase == Phase::Waiting && self.s.queue.is_empty() {
            self.open_timer_pass(firing.node, key);
            return Some(firing);
        }
        self.s.queue.push_back(Trigger::Firing(firing));
        None
    }

    /// A pass at the tasklet whose timer fired, its stage owing the firing.
    fn open_timer_pass(&mut self, node: usize, firing: FiringKey) -> Option<&mut Stage> {
        self.disarm(Deadline::ParkIdle);
        let (channel, offset) = self.entry_point();
        let mut pass = Pass::new(PassOrigin::Timer { node, firing }, channel, offset, None);
        pass.stages.insert(node, Stage::owing(1));
        self.s.pass = Some(pass);
        self.s.phase = Phase::Running;
        self.r.run.pass_dead.store(false, Ordering::Release);
        self.s.pass.as_mut()?.stages.get_mut(&node)
    }

    /// A queued firing's turn: its pass opens when the timer is still
    /// pending. A processing-time firing is admitted into its Cell; an
    /// event-time one is claimed by the stage and generated by the Cell's
    /// next assembly, which is readied here.
    pub(super) fn start_firing(&mut self, firing: Firing, pending: bool) {
        if self.s.phase != Phase::Starting {
            return;
        }
        if !pending {
            return self.enter_waiting();
        }
        if self.cancelled() {
            return self.compensate(AfterUndo::Terminal("cancelled".to_string(), String::new()));
        }
        let key = firing.key();
        let Some(stage) = self.open_timer_pass(firing.node, key.clone()) else {
            return;
        };
        if firing.fire.kind == TimerKind::EventTime {
            stage.claim = Some(key);
            self.fx.ready.push(firing.slot);
            return;
        }
        self.fx.admit.push(firing.into_release());
    }

    /// The idle clock restarts: the owed inbox tail's bound while any of it
    /// is still owed (a park then would drop it at the socket), else the park
    /// idle.
    pub(super) fn rearm_sleep(&mut self) {
        if self.s.phase != Phase::Waiting || !self.r.inst.sleeps() {
            return;
        }
        let idle = if self.r.inst.owed_inbox() > 0 {
            OWED_INBOX_WAIT
        } else {
            self.r.inst.sleep_after()
        };
        self.arm(Deadline::ParkIdle, idle);
    }

    pub(super) fn arrived(&mut self, message: Message) {
        if self.s.phase == Phase::Waiting && self.message_fed() {
            self.disarm(Deadline::ParkIdle);
            self.s.phase = Phase::Starting;
            self.fx.start = Some(Source::Message(message));
        } else {
            self.s.queue.push_back(Trigger::Message(message));
        }
    }

    pub(super) fn start_pass(&mut self, origin: PassOrigin, message: Message, seed: Seed) {
        let Message {
            channel,
            offset,
            call,
            roots,
        } = message;
        if self.s.phase != Phase::Starting {
            return;
        }
        if self.cancelled() {
            return self.compensate(AfterUndo::Terminal("cancelled".to_string(), String::new()));
        }
        if matches!(origin, PassOrigin::Message { .. }) {
            self.s.entry = Some((channel, offset));
        }
        let mut pass = Pass::new(origin, channel, offset, call);
        pass.completion = seed.completion;
        for node in seed.committed_nodes {
            let mut stage = Stage::owing(0);
            stage.state = StageState::Done;
            pass.stages.insert(node, stage);
        }
        pass.banked = seed.banked;
        pass.waiting = seed.waiting;
        let mut ready: BTreeMap<usize, Vec<Release>> = seed.ready;
        for release in roots {
            ready.entry(release.node).or_default().push(release);
        }
        self.s.pass = Some(pass);
        self.s.phase = Phase::Running;
        self.r.run.pass_dead.store(false, Ordering::Release);
        for (node, items) in ready {
            self.start_stage(node, items);
        }
        self.settle_pass();
    }

    fn start_stage(&mut self, node: usize, items: Vec<Release>) {
        let Some(pass) = self.s.pass.as_mut() else {
            return;
        };
        let empty = items.is_empty();
        pass.stages.insert(node, Stage::owing(items.len()));
        self.fx.admit.extend(items);
        if empty {
            self.stage_done(node);
        }
    }

    // -- items -------------------------------------------------------------------

    pub(super) fn settled(&mut self, node: usize, cell: CellPos, slot: &Arc<CellSlot>) {
        let attempt = self
            .s
            .attempts
            .remove(&(node, cell.clone()))
            .unwrap_or_default();
        let Attempt {
            outputs,
            held,
            record,
            completion,
            removals,
            failure,
        } = attempt;
        let Some(stage) = self.s.running(node) else {
            return;
        };
        // A Cell whose tasklet ran to its end is recorded, so it is undone
        // with the rest, even when an output's edge then failed.
        if failure.as_ref().is_none_or(|f| f.ran) {
            let run = stage
                .cells
                .entry(cell.clone())
                .or_insert_with(|| CellRun::new(Arc::clone(slot)));
            run.outputs.extend(outputs);
            if let Some(record) = record {
                RecordSpan::absorb(&mut run.record, record);
            }
            run.removals.extend(removals);
            if completion.is_some() {
                run.completion = completion;
            }
            stage.held.entry(cell).or_default().extend(held);
        }
        if let Some(failure) = failure {
            self.fail(failure);
        }
        self.owe_less(node, 1);
    }

    pub(super) fn crossed(&mut self, node: usize, items: Result<Vec<Release>, String>) {
        let Some(stage) = self.s.running(node) else {
            return;
        };
        match items {
            Ok(items) => {
                stage.owed += items.len();
                self.fx.admit.extend(items);
            }
            Err(error) => self.fail(Failure::new(node, None, StepsError::non_retryable(error))),
        }
        self.owe_less(node, 1);
    }

    pub(super) fn owe_less(&mut self, node: usize, count: usize) {
        let Some(stage) = self.s.running(node) else {
            return;
        };
        stage.owed = stage.owed.saturating_sub(count);
        if stage.owed == 0 {
            self.fx.arms.append(&mut stage.arms);
            self.stage_done(node);
        }
    }

    /// The pass cannot commit: the first failure is the one it ends on, and
    /// every Cell waiting out a retry is disarmed rather than re-run.
    fn fail(&mut self, failure: Failure) {
        if let Some(pass) = self.s.pass.as_mut() {
            pass.failure.get_or_insert(failure);
        }
        self.kill_pass();
    }

    /// A stage's barrier failed: the pass fails on it and the stage owes
    /// nothing more.
    pub(super) fn stage_failed(&mut self, node: usize, error: String) {
        self.fail(Failure {
            refused: true,
            ..Failure::new(node, None, StepsError::from(error))
        });
        if let Some(stage) = self.s.pass.as_mut().and_then(|p| p.stages.get_mut(&node)) {
            stage.state = StageState::Done;
        }
        self.settle_pass();
    }

    /// No Cell of the pass runs again: what its Cells still have queued is
    /// consumed unrun, and every one waiting out a retry is disarmed.
    pub(super) fn kill_pass(&mut self) {
        self.r.run.pass_dead.store(true, Ordering::Release);
        self.disarm_retries();
    }

    fn disarm_retries(&mut self) {
        let Some(pass) = self.s.pass.as_ref() else {
            return;
        };
        let running: Vec<usize> = pass
            .stages
            .iter()
            .filter(|(_, stage)| stage.state == StageState::Running)
            .map(|(&node, _)| node)
            .collect();
        for node in running {
            self.fx
                .cancel_retries
                .extend(self.r.inst.slots_of(self.r.spec.dag.order[node]));
        }
    }

    // -- stages ------------------------------------------------------------------

    /// Every item the stage owed settled. A healthy pass records the stage
    /// and commits it; a failed or cancelled one records what ran, for undo,
    /// and commits nothing.
    fn stage_done(&mut self, node: usize) {
        let iter = self.s.iter;
        let dead = self.cancelled()
            || self
                .s
                .pass
                .as_ref()
                .is_some_and(|pass| pass.failure.is_some());
        let Some(pass) = self.s.pass.as_mut() else {
            return;
        };
        let Some(stage) = pass.stages.get_mut(&node) else {
            return;
        };
        let cells: Vec<CellRun> = stage.cells.values().cloned().collect();
        let asked = node_completion(&cells);
        let trigger = match &pass.origin {
            PassOrigin::Timer { node: root, firing } if *root == node => Some(firing.clone()),
            _ => None,
        };
        if dead {
            stage.state = StageState::Done;
        } else {
            pass.completion.note(node, asked.clone());
        }
        if !dead || !cells.is_empty() {
            self.s.done.push(super::compensation::StageRecord {
                stage_idx: node,
                iter,
                cells: cells.clone(),
            });
        }
        if dead {
            return self.settle_pass();
        }
        self.commit_stage(node, iter, cells, asked, trigger);
    }

    /// A recorded stage's commit: its saga intent markers first, then its
    /// barrier, or the commit at once when the run journals nothing. Inlined:
    /// it runs per stage.
    #[inline(always)]
    fn commit_stage(
        &mut self,
        node: usize,
        iter: u32,
        cells: Vec<CellRun>,
        asked: Option<Vec<u8>>,
        trigger: Option<FiringKey>,
    ) {
        if self.r.task(node).compensate_overridden && self.r.inst.keeps_state() {
            for run in &cells {
                self.fx.markers.push((
                    COMPENSATION_INTENT_PAYLOAD_ID,
                    Arc::clone(&run.cell.cell_identity),
                ));
            }
        }
        if !self.r.journaling() {
            return self.stage_committed(node);
        }
        let id = self.barrier(BarrierFor::Stage(node));
        if let Some(stage) = self.s.pass.as_mut().and_then(|p| p.stages.get_mut(&node)) {
            stage.state = StageState::Committing(id);
        }
        self.fx.frames.push((
            id,
            Frame::Task {
                node,
                iter,
                cells,
                completion: asked,
                trigger,
            },
        ));
    }

    /// The stage's commit is durable: its held items go to its children,
    /// and a child whose producers in this chain have all committed runs. A
    /// leaf's output is the iteration's terminal batch.
    fn stage_committed(&mut self, node: usize) {
        let dead = self.cancelled();
        let dag = &self.r.spec.dag;
        let Some(pass) = self.s.pass.as_mut() else {
            return;
        };
        let Some(stage) = pass.stages.get_mut(&node) else {
            return;
        };
        stage.state = StageState::Done;
        let held: Vec<Release> = std::mem::take(&mut stage.held)
            .into_values()
            .flatten()
            .collect();
        if dead || pass.failure.is_some() {
            return self.settle_pass();
        }
        for run in stage.cells.values_mut() {
            let removals = std::mem::take(&mut run.removals);
            if !removals.is_empty() {
                self.fx.removals.push((Arc::clone(&run.cell), removals));
            }
        }
        if dag.children[node].is_empty() {
            let values = stage
                .cells
                .values()
                .flat_map(|run| run.outputs.iter().map(Arc::clone));
            pass.leaf_values.extend(values);
            pass.carry.extend(held);
            return self.settle_pass();
        }
        let chain = self.r.origin_chain(&pass.origin);
        let ready = pass.bank(node, held, &dag.children[node], chain);
        for (child, items) in ready {
            self.start_stage(child, items);
        }
        self.settle_pass();
    }

    /// Nothing owed: a failed or cancelled pass is resolved, a healthy one
    /// sealed once every commit is durable.
    pub(super) fn settle_pass(&mut self) {
        if self.s.phase != Phase::Running {
            return;
        }
        let Some(pass) = self.s.pass.as_ref() else {
            return;
        };
        if pass
            .stages
            .values()
            .any(|stage| stage.state == StageState::Running)
        {
            return;
        }
        if pass.failure.is_some() || self.cancelled() {
            return self.resolve_failure();
        }
        if pass
            .stages
            .values()
            .any(|stage| matches!(stage.state, StageState::Committing(_)))
        {
            return;
        }
        self.seal();
    }

    fn resolve_failure(&mut self) {
        let Some(pass) = self.s.pass.as_ref() else {
            return;
        };
        let owed_acks: Vec<u64> = pass
            .stages
            .values()
            .filter_map(|stage| match stage.state {
                StageState::Committing(id) => Some(id),
                _ => None,
            })
            .collect();
        let channel = pass.channel;
        let offset = pass.consumed_offset;
        let call = pass.call.clone();
        let failure = pass.failure.clone();
        let message_pass = matches!(pass.origin, PassOrigin::Message { .. });
        self.drop_barriers(owed_acks);
        // A root whose message was unreadable, with nothing of the pass done:
        // the message parks under its entry channel and the pass never
        // happened.
        let message_fed = message_pass && self.message_fed();
        let unread = failure.as_ref().filter(|f| {
            f.error.unreadable
                && message_fed
                && self.s.done.is_empty()
                && self
                    .r
                    .chain(channel)
                    .is_some_and(|c| c.roots.contains(&f.node))
        });
        if let Some(unread) = unread {
            self.fx.parked = Some(MessagePark::unread(channel, offset, call, unread));
            self.s.parks_in_flight += 1;
            self.r.run.pass_dead.store(false, Ordering::Release);
            return self.enter_waiting();
        }
        if let Some(failure) = failure
            .clone()
            .filter(|f| message_pass && self.fails_turn(f))
        {
            return self.dead_letter(channel, offset, call, failure);
        }
        let error = failure
            .map(|f| f.error)
            .unwrap_or_else(|| StepsError::from("cancelled"));
        let (status, error) = self.abnormal(&error);
        self.compensate(AfterUndo::Terminal(status, error));
    }

    /// Whether `failure` ends only its turn: a tasklet's failure, not its
    /// commit's, in a `PerInput` keep-progress loop with no stop asked.
    fn fails_turn(&self, failure: &Failure) -> bool {
        self.r.spec.on_failure.fails_turn(self.r.spec.each) && !failure.refused && !self.cancelled()
    }

    /// A failed turn: its iteration commits as dead-lettered, the controller
    /// parking its message first, and on the ack the turn is undone. A run
    /// that journals nothing keeps no input log, so it parks the turn itself,
    /// with neither bytes nor offset but with its call, and undoes it at once.
    fn dead_letter(
        &mut self,
        channel: ChannelId,
        offset: Option<u64>,
        call: Option<Arc<str>>,
        failure: Failure,
    ) {
        let dead_letter = crate::pb::DeadLetter {
            tasklet: self.r.task(failure.node).name.to_string(),
            attempts: failure.attempts,
            error: failure.error.message,
            cell_key: failure.cell_key.as_deref().unwrap_or_default().to_string(),
        };
        if !self.r.journaling() {
            self.fx.parked = Some(MessagePark::turn(channel, call, &dead_letter));
            return self.undo_turn();
        }
        let frame = Frame::Iteration {
            iter: self.s.iter,
            offset,
            channel,
            values: Vec::new(),
            completion: None,
            dead_letter: Some(dead_letter),
        };
        self.s.phase = Phase::Sealing;
        let id = self.barrier(BarrierFor::DeadLetter);
        self.fx.frames.push((id, frame));
    }

    /// The failed turn's iteration is committed: what it ran is undone as a
    /// rewind undoes an iteration, and the loop waits for its next message.
    fn undo_turn(&mut self) {
        let done = std::mem::take(&mut self.s.done);
        self.s.pass = None;
        self.s.iter += 1;
        self.begin_undo(unwind_steps(self.r, &[done]), AfterUndo::Resume);
    }

    /// The iteration boundary: a `Once` pass ends here; a loop commits the
    /// iteration and enters the next one on its ack.
    fn seal(&mut self) {
        let iter = self.s.iter;
        let Some(pass) = self.s.pass.as_ref() else {
            return;
        };
        let completion = pass.completion.result();
        if self.r.spec.each.ends_after_first() {
            return self.terminal("ok", String::new(), completion.unwrap_or_default());
        }
        if !self.r.journaling() {
            return self.iter_acked();
        }
        // A timer-fed pass that released nothing at the leaf commits the
        // carry as it stood, so a chained loop does not end on it.
        let values = match (&pass.origin, &self.s.carry) {
            (PassOrigin::Timer { .. }, Some(carry)) if pass.leaf_values.is_empty() => {
                carry.values.clone()
            }
            _ => pass.leaf_values.clone(),
        };
        let frame = Frame::Iteration {
            iter,
            offset: pass.consumed_offset,
            channel: pass.channel,
            values,
            completion,
            dead_letter: None,
        };
        self.s.phase = Phase::Sealing;
        let id = self.barrier(BarrierFor::Iteration);
        self.fx.frames.push((id, frame));
    }

    fn iter_acked(&mut self) {
        let Some(pass) = self.s.pass.take() else {
            return;
        };
        let done = std::mem::take(&mut self.s.done);
        // Keep-progress never undoes a committed iteration, so it keeps no
        // record of one: nothing then holds its Cells past their eviction.
        if !self.r.spec.on_failure.keeps_committed() {
            self.s.committed.push(done);
        }
        self.s.iter += 1;
        if let Some(result) = pass.completion.result() {
            return self.terminal("ok", String::new(), result);
        }
        // A chained loop carries its leaf batch into the next pass and ends
        // on an empty one; a timer-fed pass that released nothing at the
        // leaf leaves the carry as it stood.
        if self.r.spec.each.carries_leaf() {
            if !pass.leaf_values.is_empty() {
                self.s.carry = Some(Carry {
                    channel: pass.channel,
                    values: pass.leaf_values,
                    items: pass.carry,
                });
            } else if matches!(pass.origin, PassOrigin::Message { .. }) {
                return self.finish_clean();
            }
        }
        self.enter_waiting();
    }

    pub(super) fn barrier_answered(&mut self, target: BarrierFor, result: Result<(), String>) {
        match (target, result) {
            (BarrierFor::Stage(node), Ok(())) => self.stage_committed(node),
            (BarrierFor::Stage(node), Err(error)) => self.stage_failed(
                node,
                format!("commit {node} refused by controller: {error}"),
            ),
            (BarrierFor::Iteration, Ok(())) => {
                if self.s.phase == Phase::Sealing {
                    self.iter_acked();
                }
            }
            (BarrierFor::DeadLetter, Ok(())) => {
                if self.s.phase == Phase::Sealing {
                    self.undo_turn();
                }
            }
            (BarrierFor::Iteration | BarrierFor::DeadLetter, Err(error)) => {
                let error = StepsError::from(format!(
                    "iteration commit {} refused by controller: {error}",
                    self.s.iter
                ));
                let (status, error) = self.abnormal(&error);
                self.compensate(AfterUndo::Terminal(status, error));
            }
            (BarrierFor::Undo, result) => {
                if let Err(error) = result {
                    let id = &self.r.inst.instance_id;
                    flz_transport::notice!(
                        Error,
                        "steps {id} compensation journaling failed: {error}"
                    );
                }
                if let Some(undo) = self.s.undo.as_mut() {
                    undo.steps.pop_front();
                }
                self.undo_step();
            }
        }
    }
}
