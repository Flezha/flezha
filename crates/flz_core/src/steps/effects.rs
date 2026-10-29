//! The effects, after the lock: frames sent, items admitted, author code run.

use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use super::commit::{ack_timeout, iteration_frame, task_frame};
use super::compensation::CellRun;
use super::events::{barrier_acked, crossed, items_dropped, undo_answered};
use super::instance::{Failure, FiringKey, HeldArm, Release, Value};
use super::run::{Run, Source, ADMIT_RETRY, UNDO_GATE_RETRY};
use super::StepsError;
use crate::cell::{CellItem, CellSlot};
use crate::channel::{ChannelId, OverflowPolicy};
use crate::host::HostPayload;
use crate::pb::command_request::Command;
use crate::relay::sleep::iter_park;
use crate::relay::Deadline;
use crate::timer::TimerOp;

// ---- effects, after the lock --------------------------------------------------

/// A message on its way into the dead-letter queue: unread, or its turn's.
pub(super) struct MessagePark {
    channel: ChannelId,
    turn: bool,
    offset: Option<u64>,
    call: Option<Arc<str>>,
    payload: Option<Vec<u8>>,
    error: String,
    attempts: u32,
}

impl MessagePark {
    /// A message the entry channel could not read, with its bytes, offset and
    /// call.
    pub(super) fn unread(
        channel: ChannelId,
        offset: Option<u64>,
        call: Option<Arc<str>>,
        failure: &Failure,
    ) -> Self {
        Self {
            channel,
            turn: false,
            offset,
            call,
            payload: failure.payload.clone(),
            error: failure.error.message.clone(),
            attempts: failure.attempts,
        }
    }

    /// A failed turn of a run that keeps no input log: neither bytes nor
    /// offset, only the call the pass carried.
    pub(super) fn turn(
        channel: ChannelId,
        call: Option<Arc<str>>,
        dead_letter: &crate::pb::DeadLetter,
    ) -> Self {
        Self {
            channel,
            turn: true,
            offset: None,
            call,
            payload: None,
            error: format!(
                "tasklet `{}` failed after {} attempt(s): {}",
                dead_letter.tasklet, dead_letter.attempts, dead_letter.error
            ),
            attempts: dead_letter.attempts,
        }
    }
}

/// A barrier frame to build and send.
pub(super) enum Frame {
    Task {
        node: usize,
        iter: u32,
        cells: Vec<CellRun>,
        completion: Option<Vec<u8>>,
        trigger: Option<FiringKey>,
    },
    Iteration {
        iter: u32,
        offset: Option<u64>,
        channel: ChannelId,
        values: Vec<Value>,
        completion: Option<Vec<u8>>,
        dead_letter: Option<crate::pb::DeadLetter>,
    },
    Built(Box<Command>),
}

impl Frame {
    /// The command, through the journal codecs. Inlined: it runs per stage.
    #[inline(always)]
    fn build(self, r: &Run<'_>) -> Result<Command, StepsError> {
        match self {
            Frame::Task {
                node,
                iter,
                cells,
                completion,
                trigger,
            } => task_frame(r, node, iter, &cells, completion, trigger),
            Frame::Iteration {
                iter,
                offset,
                channel,
                values,
                completion,
                dead_letter,
            } => iteration_frame(r, iter, offset, channel, &values, completion, dead_letter),
            Frame::Built(command) => Ok(*command),
        }
    }
}

#[derive(Default)]
pub(super) struct Effects {
    pub(super) markers: Vec<(&'static str, Arc<Vec<u8>>)>,
    pub(super) frames: Vec<(u64, Frame)>,
    pub(super) drop_acks: Vec<String>,
    pub(super) disarm: Vec<(Deadline, u64)>,
    /// A deadline's re-arm: the due it replaces, then its new one.
    pub(super) arm: Vec<(Deadline, Option<u64>, u64)>,
    pub(super) admit: Vec<Release>,
    pub(super) cancel_retries: Vec<Arc<CellSlot>>,
    pub(super) parked: Option<MessagePark>,
    /// The run's end: status, error, result and how its rollback went.
    pub(super) completed: Option<(String, String, Vec<u8>, String)>,
    pub(super) run_undo: Option<(Arc<CellSlot>, Vec<Value>, Option<u64>)>,
    pub(super) start: Option<Source>,
    /// Firing removals a stage's commit made durable, per Cell.
    pub(super) removals: Vec<(Arc<CellSlot>, Vec<TimerOp>)>,
    /// Timers a stage that is done held off the wake list.
    pub(super) arms: Vec<HeldArm>,
    /// Cells whose next assembly generates a firing their stage claimed.
    pub(super) ready: Vec<Arc<CellSlot>>,
    /// Park at this iteration, naming this offset parked unread.
    pub(super) park: Option<(u32, Option<u64>)>,
}

impl Run<'_> {
    pub(super) fn act(&self, mut fx: Effects) {
        self.send(&mut fx);
        self.schedule(&mut fx);
        if let Some(parked) = fx.parked {
            self.park_message(parked);
        }
        if let Some((status, error, result, undo)) = fx.completed {
            self.emit_completed(status, error, result, undo);
        }
        if let Some((slot, outputs, offset)) = fx.run_undo {
            self.run_undo(slot, outputs, offset);
        }
        if let Some(source) = fx.start {
            self.begin_pass(source);
        }
    }

    /// What goes to the controller, in `act`'s order. Inlined: it runs per
    /// event.
    #[inline(always)]
    fn send(&self, fx: &mut Effects) {
        // Intent markers precede the commit: a crash between marker and ack
        // resolves as "not committed, re-run".
        for (payload_id, identity) in fx.markers.drain(..) {
            crate::state_sink::send_compensation_record(payload_id, &identity);
        }
        for (id, frame) in fx.frames.drain(..) {
            self.send_barrier(id, frame);
        }
        for (slot, ops) in fx.removals.drain(..) {
            slot.dispatcher.write_timer_ops(&slot, ops);
        }
        if let Some((iter, offset)) = fx.park.take() {
            let wake = self.inst.earliest_processing_timer();
            let (reference_id, command) = iter_park(&self.inst.instance_id, iter, wake, offset);
            crate::runner::emit_control_request(reference_id, command);
        }
        for reference_id in fx.drop_acks.drain(..) {
            crate::shutdown::drop_pending_ack(&reference_id);
        }
    }

    /// What goes to the poller, in `act`'s order. Inlined: it runs per event.
    #[inline(always)]
    fn schedule(&self, fx: &mut Effects) {
        let core = crate::poll_core::global();
        for (deadline, due) in fx.disarm.drain(..) {
            core.disarm_instance(self.inst.seq, deadline, due);
        }
        for (deadline, old, due) in fx.arm.drain(..) {
            core.rearm_instance(self.weak(), self.inst.seq, deadline, old, due);
        }
        for (slot, at, stream) in fx.arms.drain(..) {
            core.arm_timer(&slot, at, stream);
        }
        self.admit(std::mem::take(&mut fx.admit));
        for slot in fx.ready.drain(..) {
            if slot.mailbox.claim_inflight() {
                core.schedule(&slot);
            }
        }
        for slot in fx.cancel_retries.drain(..) {
            items_dropped(&slot, core.cancel_retry(&slot));
        }
    }

    /// The run's completion. An ended instance's pending timers go with its
    /// Cells, as a flow relay's do on removal; the record says nothing of them.
    fn emit_completed(&self, status: String, error: String, result: Vec<u8>, undo: String) {
        let pending: usize = self
            .inst
            .all_slots()
            .iter()
            .map(|slot| slot.timers.try_lock().map_or(0, |pending| pending.len()))
            .sum();
        if pending > 0 {
            tracing::debug!(
                target: "flezha::steps",
                instance = %self.inst.instance_id,
                pending,
                "steps ended; its pending timers are dropped with its cells"
            );
        }
        let id = self.inst.instance_id.to_string();
        crate::runner::emit_relay_completed(id, status, error, result, undo);
    }

    /// Build a barrier's frame (the journal codecs run here), register its
    /// ack as an event, send it, and keep it for a re-send.
    fn send_barrier(&self, id: u64, frame: Frame) {
        let built = frame.build(self);
        let reference_id = {
            let sheet = self.run.sheet();
            match sheet.barriers.get(&id) {
                Some(barrier) => barrier.reference_id.clone(),
                None => return,
            }
        };
        let command = match built {
            Ok(command) => command,
            Err(error) => {
                return self.decide(|d| {
                    if let Some(barrier) = d.s.barriers.remove(&id) {
                        barrier.target.failed(d, error.message);
                    }
                });
            }
        };
        // Kept before the send: a refusal can answer before this thread
        // takes the sheet again, and its re-send needs the command.
        self.decide(|d| {
            if let Some(barrier) = d.s.barriers.get_mut(&id) {
                barrier.command = Some(command.clone());
                d.arm(Deadline::Ack(id), ack_timeout());
            }
        });
        let weak = self.weak();
        crate::shutdown::register_pending_ack(
            reference_id.clone(),
            Box::new(move |result| barrier_acked(&weak, id, result.map(|_| ()))),
        );
        crate::runner::emit_control_request(reference_id, command);
    }

    /// Items into their Cells without waiting: a producing Cell's holdback
    /// takes what a full mailbox refuses; a message's item, which has no
    /// producer, waits on the sheet for a short retry. An item with no Cell
    /// yet crosses to its host for its keys.
    fn admit(&self, releases: Vec<Release>) {
        if releases.is_empty() {
            return;
        }
        let mut backlog = !self.run.sheet().unadmitted.is_empty();
        let mut refused = Vec::new();
        let mut keyless = Vec::new();
        for release in releases {
            let Release {
                node,
                cell,
                item,
                producer,
            } = release;
            let Some(cell) = cell else {
                keyless.push((node, item));
                continue;
            };
            let item = match producer {
                Some(producer) => {
                    let policy = OverflowPolicy::DropNothing;
                    crate::admission::admit_from_cell(&producer, &cell, item, policy, true);
                    continue;
                }
                None if backlog => item,
                None => match crate::admission::try_admit(&cell, item) {
                    Ok(()) => continue,
                    Err(item) => {
                        backlog = true;
                        item
                    }
                },
            };
            refused.push(Release {
                node,
                cell: Some(cell),
                item,
                producer: None,
            });
        }
        if !refused.is_empty() {
            self.decide(|d| {
                d.s.unadmitted.extend(refused);
                d.arm(Deadline::Admit, ADMIT_RETRY);
            });
        }
        self.cross(keyless);
    }

    /// Keyless host values into one prepare crossing per channel they were
    /// released on; each stage owes its values until the answer addresses
    /// them.
    fn cross(&self, keyless: Vec<(usize, CellItem)>) {
        if keyless.is_empty() {
            return;
        }
        let Some(owner) = self.run.me.upgrade() else {
            return;
        };
        let mut by_channel: BTreeMap<ChannelId, Vec<crate::host::StagedEntry>> = BTreeMap::new();
        for (node, item) in keyless {
            let payload = match item {
                CellItem::Input(payload) => payload,
                CellItem::Timer(_) | CellItem::CloseInterval => continue,
            };
            let Some(value) = payload.value.downcast_ref::<HostPayload>() else {
                continue;
            };
            by_channel
                .entry(value.channel)
                .or_default()
                .push(crate::host::StagedEntry {
                    bytes: value.bytes.clone(),
                    consumed_offset: None,
                    call: None,
                    replay: false,
                    stream: Arc::clone(&payload.stream),
                    stage: Some(node),
                });
        }
        for (channel_id, entries) in by_channel {
            let Some(channel) = self.inst.topology.channel(channel_id) else {
                continue;
            };
            for unasked in crate::host::stage_prepare(&owner, channel_id, channel, entries) {
                let node = unasked.stage.expect("staged for a stage");
                crossed(
                    self.inst,
                    node,
                    Err(format!(
                        "no consumer of channel `{}` asks its host for keys",
                        channel.name
                    )),
                );
            }
        }
    }

    /// Submit one Cell's undo as a `CompensateRun`, holding the Cell's gate
    /// from here to its answer; a Cell still handed out is tried again.
    fn run_undo(&self, slot: Arc<CellSlot>, outputs: Vec<Value>, offset: Option<u64>) {
        if slot.drain_active.swap(true, Ordering::AcqRel) {
            return self.decide(|d| d.arm(Deadline::UndoGate, UNDO_GATE_RETRY));
        }
        let (weak, answered) = (self.weak(), Arc::clone(&slot));
        // The seq the undo runs at, and its outputs are stamped at.
        let seq = slot.change_seq.load(Ordering::Acquire);
        let answer = move |outcome| undo_answered(&weak, &answered, outcome, seq);
        let reply = crate::poll_core::CommandReply::new(answer);
        crate::poll_core::global().submit_undo(&slot, outputs, offset, seq, reply);
    }

    /// A message into the dead-letter queue, held by the park owner as every
    /// park is: an unreadable one under its entry channel, which the run
    /// counts consumed once the park is let go of; a failed turn's under
    /// `_turn`.
    fn park_message(&self, park: MessagePark) {
        let Some(owner) = self.run.me.upgrade() else {
            return;
        };
        let (held, offset) = (Arc::clone(&owner), park.offset);
        let counted = move || {
            if let Some(r) = Run::of(&held) {
                r.decide(|d| {
                    d.s.parks_in_flight = d.s.parks_in_flight.saturating_sub(1);
                    d.s.parked_offset = d.s.parked_offset.max(offset);
                });
            }
        };
        let then = (!park.turn).then(|| Box::new(counted) as crate::park::Then);
        let Some(descriptor) = owner.topology.channel(park.channel) else {
            return then.into_iter().for_each(|then| then());
        };
        let (label, target) = match park.turn {
            true => (
                "turn on entry channel",
                crate::retry::ParkTarget::turn(descriptor),
            ),
            false => ("entry channel", crate::retry::ParkTarget::entry(descriptor)),
        };
        let park = crate::retry::park_exhausted(
            &format!("{label} `{}`", descriptor.name),
            target,
            &owner.instance_id,
            (offset, park.call, park.payload),
            park.error,
            park.attempts,
            then,
        );
        crate::park::hand_off(&owner, crate::park::Work::Parking(park));
    }
}
