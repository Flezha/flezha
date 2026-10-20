//! The events: each entry point a thread calls with what it brings.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Weak};

use flz_transport::transport::FlezhaStream;

use super::compensation::{plan_compensation, RecordSpan};
use super::instance::{CellPos, Failure, Firing, Pass, Phase, Value};
use super::run::{Run, RESTORE_GATE};
use super::StepsError;
use crate::cell::CellSlot;
use crate::channel::{ChannelDescriptor, ChannelId, ChannelKind};
use crate::flusher::FlushableBuffer;
use crate::header::HeaderTemplate;
use crate::host::Crossed;
use crate::relay::instance::RelayInstance;
use crate::relay::Deadline;
use crate::routing::Entering;
use crate::timer::{TimerFire, TimerKind, TimerOp};

// ---- the events ------------------------------------------------------------

/// The bind is complete: `init_values` are in place. Arms `max_duration` and
/// opens the restore gate once every restore the bind announced has landed.
pub fn open(inst: &RelayInstance) {
    let Some(r) = Run::of(inst) else { return };
    r.decide(|d| {
        if let Some(limit) = d.r.inst.max_duration() {
            d.arm(Deadline::MaxDuration, limit);
        }
        d.s.bound = true;
        if d.s.restored >= d.s.expected {
            d.open_gate();
        } else {
            d.arm(Deadline::RestoreGate, RESTORE_GATE);
        }
    });
}

/// One Cell restore landed, with the saga markers the controller persisted.
pub fn restore_landed(inst: &RelayInstance, cell_id: &[u8], intent: bool, done: bool) {
    let Some(r) = Run::of(inst) else { return };
    r.decide(|d| {
        if intent {
            d.s.compensation_intents.insert(cell_id.to_vec());
        }
        if done {
            d.s.compensation_done.insert(cell_id.to_vec());
        }
        d.landed(1);
    });
}

/// `count` effect results the bind shipped landed; the gate counts them with
/// the Cell restores.
pub fn results_landed(inst: &RelayInstance, count: usize) {
    let Some(r) = Run::of(inst) else { return };
    r.decide(|d| d.landed(count));
}

/// A cooperative stop (`StepsStop`, or `max_duration`).
pub fn stop(inst: &RelayInstance) {
    if let Some(r) = Run::of(inst) {
        r.decide(|d| d.stop());
    }
}

/// The registry dropped the instance: every later event finds it done.
pub fn removed(inst: &RelayInstance) {
    let Some(r) = Run::of(inst) else { return };
    // One step under the lock: an event racing the removal sees it whole.
    r.decide(|d| {
        d.s.phase = Phase::Done;
        d.r.run.cancel.store(true, Ordering::Release);
        d.r.run.pass_dead.store(true, Ordering::Release);
        d.clear_waits();
    });
}

/// A message through the entrance: its items for the chain's roots are
/// addressed here, on the reader, and the pass starts at once or waits in the
/// queue. A message this instance already took is dropped. An entry edge
/// that unwinds, or a key this partition member does not own, rides the
/// channel's ladder off the reader and opens no pass.
pub(crate) fn message<V: Entering>(
    inst: &RelayInstance,
    channel: &ChannelDescriptor,
    value: &V,
    d: &crate::routing::Delivery<'_>,
) {
    let (stream, offset, call) = (d.stream, d.consumed_offset, d.call);
    let Some(r) = Run::of(inst) else { return };
    let Some(channel_id) = inst.topology.lookup_channel_id(&channel.name) else {
        return;
    };
    if !r.run.take(offset) {
        return;
    }
    if let Err(error) = r.arrive(channel, channel_id, value, stream, offset, call) {
        entry_ladder(inst, channel_id, value, stream, offset, call, error);
    }
}

/// The outside call the pass `slot` is running in carries, if any: what a
/// sink output of the pass answers.
pub fn pass_call(slot: &CellSlot) -> Option<Arc<str>> {
    read_pass(slot, |pass| pass.call.clone())
}

/// The input-log offset the pass `slot` is running in took, if any: a stage's
/// items carry none of their own, and its effects are named by it.
pub fn pass_offset(slot: &CellSlot) -> Option<u64> {
    read_pass(slot, Pass::effect_offset)
}

fn read_pass<T>(slot: &CellSlot, read: impl FnOnce(&Pass) -> Option<T>) -> Option<T> {
    let inst = slot.instance.upgrade()?;
    let r = Run::of(&inst)?;
    let sheet = r.run.sheet();
    read(sheet.pass.as_ref()?)
}

/// A message on its way into the dead-letter queue instead of a pass; the
/// run counts it consumed.
pub fn entry_parked(inst: &RelayInstance, offset: Option<u64>) {
    if let Some(r) = Run::of(inst) {
        r.decide(|d| {
            d.s.parked_offset = d.s.parked_offset.max(offset);
            d.rearm_sleep();
        });
    }
}

/// A stage's output value, on the worker that emitted it: the edge chain and
/// `cells_for` run here, and the addressed items wait on the sheet for the
/// stage's commit. A chained loop's leaf addresses its value to the roots.
pub fn hold_emit(
    inst: &RelayInstance,
    channel: Option<&ChannelDescriptor>,
    value: &Value,
    stream: &Arc<FlezhaStream>,
    producer: &Arc<CellSlot>,
) {
    let Some(r) = Run::of(inst) else { return };
    let Some((node, cell)) = r.locate(producer) else {
        return;
    };
    let plugin_bound = channel.is_some_and(|c| c.kind == ChannelKind::PluginBound);
    let addressed = match channel {
        _ if plugin_bound => Ok(Vec::new()),
        _ if r.spec.dag.children[node].is_empty() => Ok(r.carry_of(value, stream, producer)),
        Some(channel) => r.address(channel, value.each(channel), stream, Some(producer)),
        None => Ok(Vec::new()),
    };
    r.decide(|d| {
        let cell_key = cell.1.clone();
        let attempt = d.s.attempts.entry((node, cell)).or_default();
        if !plugin_bound {
            attempt.outputs.push(Arc::clone(value));
        }
        match addressed {
            Ok(held) => attempt.held.extend(held),
            Err(error) => {
                attempt.failure.get_or_insert(Failure {
                    ran: true,
                    ..Failure::new(node, cell_key, StepsError::non_retryable(error))
                });
            }
        }
    });
}

/// Ship a steps relay Cell's just-settled effect record at once, ahead of the
/// stage's barrier, and name it on the pass sheet with the plugin `channels`
/// its outputs were written on, for the barrier to name in turn. The one step
/// both settle sites take, under the buffer lock they settled under, or a drain
/// between ships it unnamed; a settle leaving nothing durable ships and names nothing.
pub fn ship_stage_record(
    slot: &Arc<CellSlot>,
    buffer: &mut dyn FlushableBuffer,
    header: &HeaderTemplate,
    channels: Vec<String>,
) {
    let Some(seq) = crate::flusher::flush_one(
        buffer,
        &slot.cell_identity,
        &slot.change_seq,
        header,
        slot.keeps_state,
    ) else {
        return;
    };
    let record = RecordSpan {
        first: seq,
        last: seq,
        channels,
    };
    with_cell(slot, |r, node, cell| {
        r.decide(|d| {
            let attempt = d.s.attempts.entry((node, cell)).or_default();
            RecordSpan::absorb(&mut attempt.record, record);
        });
    });
}

/// A tasklet's `complete`: the run ends with `result` once the iteration
/// commits; a later call in the same attempt replaces it.
pub fn complete(slot: &Arc<CellSlot>, result: Vec<u8>) {
    with_cell(slot, |r, node, cell| {
        r.decide(|d| {
            d.s.attempts.entry((node, cell)).or_default().completion = Some(result);
        });
    });
}

/// An item's final or non-retryable failure, where a flow relay would park it.
/// `payload` is the item's wire bytes, what an unreadable message parks.
pub fn note_failure(
    slot: &Arc<CellSlot>,
    error: StepsError,
    payload: Option<Vec<u8>>,
    attempts: u32,
) {
    with_cell(slot, |r, node, cell| {
        r.decide(|d| {
            let cell_key = cell.1.clone();
            let attempt = d.s.attempts.entry((node, cell)).or_default();
            attempt.failure.get_or_insert(Failure {
                payload,
                attempts,
                ..Failure::new(node, cell_key, error)
            });
        });
    });
}

/// A retryable attempt failed: what it produced never counts.
pub fn attempt_failed(slot: &Arc<CellSlot>) {
    with_cell(slot, |r, node, cell| {
        r.decide(|d| {
            d.s.attempts.remove(&(node, cell));
        });
    });
}

/// One of a stage's items settled, its outputs already held.
pub fn item_settled(slot: &Arc<CellSlot>) {
    with_cell(slot, |r, node, cell| {
        r.decide(|d| d.settled(node, cell, slot))
    });
}

/// Items consumed without running: a dead pass's queue, a disarmed retry.
pub(crate) fn items_dropped(slot: &Arc<CellSlot>, count: usize) {
    if count == 0 {
        return;
    }
    with_cell(slot, |r, node, cell| {
        r.decide(|d| {
            d.s.attempts.remove(&(node, cell));
            d.owe_less(node, count);
        })
    });
}

/// A processing-time timer of one of the instance's Cells came due on the
/// wake list: it opens a pass, joins its own stage or waits its turn, and
/// reaches its Cell through the run's admission, as a message's item does.
pub(crate) fn firing_promoted(slot: &Arc<CellSlot>, fire: TimerFire) {
    with_cell(slot, |r, node, _| {
        r.decide(|d| {
            if let Some(firing) = d.admit_firing(Firing::new(node, slot, fire)) {
                d.fx.admit.push(firing.into_release());
            }
        })
    });
}

/// An event-time firing the Cell's assembly found due at the top of a
/// batch: whether it runs in this batch.
pub(crate) fn firing_assembled(slot: &Arc<CellSlot>, fire: &TimerFire) -> bool {
    with_cell(slot, |r, node, _| {
        r.decide(|d| {
            d.admit_firing(Firing::new(node, slot, fire.clone()))
                .is_some()
        })
    })
    .unwrap_or(false)
}

/// Event-time firings the assembly staged ahead of an input join the stage
/// that delivered the input.
pub(crate) fn firings_joined(slot: &Arc<CellSlot>, count: usize) {
    if count == 0 {
        return;
    }
    with_cell(slot, |r, node, _| {
        r.decide(|d| {
            if let Some(stage) = d.running_stage(node) {
                stage.owed += count;
            }
        })
    });
}

/// Hold a served firing's removal on its attempt, to be written when the
/// stage commits; `false` when the run journals nothing, where it commits at
/// settle as a flow relay's does.
pub(crate) fn hold_removal(slot: &Arc<CellSlot>, op: TimerOp) -> bool {
    with_cell(slot, |r, node, cell| {
        if !r.journaling() {
            return false;
        }
        r.decide(|d| {
            d.s.attempts
                .entry((node, cell))
                .or_default()
                .removals
                .push(op)
        });
        true
    })
    .unwrap_or(false)
}

/// Hand a steps relay Cell's processing-time op to its tasklet's running
/// stage ([`Stage::hold`](super::instance::Stage::hold)); `false` when none runs.
pub(crate) fn hold_arm(slot: &Arc<CellSlot>, op: &TimerOp, stream: &Arc<FlezhaStream>) -> bool {
    with_cell(slot, |r, node, _| {
        r.decide(|d| {
            d.running_stage(node)
                .is_some_and(|s| s.hold(slot, op, stream))
        })
    })
    .unwrap_or(false)
}

/// The removals the running pass holds for `slot`, not yet durable.
pub(crate) fn held_removals(slot: &Arc<CellSlot>) -> Vec<TimerOp> {
    with_cell(slot, |r, node, cell| {
        let sheet = r.run.sheet();
        let stage = sheet.pass.as_ref().and_then(|pass| pass.stages.get(&node));
        let recorded = stage.and_then(|stage| stage.cells.get(&cell));
        let attempt = sheet.attempts.get(&(node, cell));
        recorded
            .iter()
            .flat_map(|run| run.removals.iter().copied())
            .chain(attempt.iter().flat_map(|a| a.removals.iter().copied()))
            .collect()
    })
    .unwrap_or_default()
}

/// The firings of `slot` whose stage the bind's journal records committed.
pub(crate) fn committed_firings(slot: &Arc<CellSlot>) -> Vec<(TimerKind, u64)> {
    with_cell(slot, |r, node, _| {
        let sheet = r.run.sheet();
        sheet
            .journal
            .iter()
            .flat_map(|journal| journal.firings())
            .filter(|&(_, at, key)| at == node && key.cell_key == slot.cell_key)
            .map(|(_, _, key)| (key.kind, key.scheduled_at))
            .collect()
    })
    .unwrap_or_default()
}

/// The identities an idle sweep keeps, and the record references per Cell.
pub(crate) type ReapHolds = (HashSet<Arc<Vec<u8>>>, HashMap<*const CellSlot, usize>);

/// The Cells an idle sweep of `inst` must keep: `None` — every one — unless
/// the run waits between passes, else those a rollback of the declared mode
/// could still reach through a compensation hook; with them, how many
/// references the run's records keep to each Cell, none of which pushes to
/// it. A flow relay keeps none.
pub(crate) fn reap_holds(inst: &RelayInstance) -> Option<ReapHolds> {
    let Some(r) = Run::of(inst) else {
        return Some(Default::default());
    };
    let sheet = r.run.sheet();
    if sheet.phase != Phase::Waiting {
        return None;
    }
    let reach = plan_compensation(r.spec.on_failure, &sheet.committed, &sheet.done);
    let mut recorded = HashMap::new();
    for record in sheet.committed.iter().flatten().chain(&sheet.done) {
        for run in &record.cells {
            *recorded.entry(Arc::as_ptr(&run.cell)).or_default() += 1;
        }
    }
    let kept = reach
        .into_iter()
        .filter(|record| r.task(record.stage_idx).compensate_overridden)
        .flat_map(|record| &record.cells)
        .map(|run| Arc::clone(&run.cell.cell_identity))
        .collect();
    Some((kept, recorded))
}

/// A stage's keyless host value back from the prepare crossing: addressed
/// to the keyed Cells its answer names, which the stage then owes in its
/// place — or, when the crossing's ladder gave up, the stage fails where a
/// stream would park the value.
pub(crate) fn crossed(
    inst: &RelayInstance,
    node: usize,
    answer: Result<(ChannelId, Crossed, Arc<FlezhaStream>), String>,
) {
    let Some(r) = Run::of(inst) else { return };
    let items = answer
        .and_then(|(channel, crossed, stream)| r.address_crossed(node, channel, &crossed, &stream));
    r.decide(|d| d.crossed(node, items));
}

/// Whether `slot` belongs to a steps relay pass that failed or was cancelled.
pub(crate) fn pass_dead(slot: &CellSlot) -> bool {
    slot.instance
        .upgrade()
        .and_then(|inst| {
            inst.steps
                .as_ref()
                .map(|run| run.pass_dead.load(Ordering::Acquire))
        })
        .unwrap_or(false)
}

/// A deadline the deferred-wake list promoted; stale when it was re-armed or
/// disarmed since.
pub(crate) fn deadline_due(inst: &RelayInstance, deadline: Deadline, due: u64) {
    if let Some(r) = Run::of(inst) {
        r.decide(|d| {
            if d.s.armed.get(&deadline) != Some(&due) {
                return;
            }
            d.s.armed.remove(&deadline);
            d.deadline(deadline);
        });
    }
}

/// `f` over `slot`'s run, its stage and its place there; `None` when the
/// Cell belongs to no live steps run.
pub(super) fn with_cell<T>(
    slot: &Arc<CellSlot>,
    f: impl FnOnce(&Run<'_>, usize, CellPos) -> T,
) -> Option<T> {
    let inst = slot.instance.upgrade()?;
    let r = Run::of(&inst)?;
    let (node, cell) = r.locate(slot)?;
    Some(f(&r, node, cell))
}

/// A barrier's controller reply, on the control reader.
pub(super) fn barrier_acked(inst: &Weak<RelayInstance>, id: u64, result: Result<(), String>) {
    let Some(inst) = inst.upgrade() else { return };
    let Some(r) = Run::of(&inst) else { return };
    r.decide(|d| {
        let Some(target) = d.s.barriers.get(&id).map(|barrier| barrier.target) else {
            return;
        };
        match result {
            Ok(()) => {
                d.disarm(Deadline::Ack(id));
                d.s.barriers.remove(&id);
                target.on_ack(d);
            }
            Err(error) => target.on_refusal(d, id, error),
        }
    });
}

/// A Cell's undo answered, on the thread that completed it.
pub(super) fn undo_answered(
    inst: &Weak<RelayInstance>,
    slot: &Arc<CellSlot>,
    outcome: crate::schema::ActivationOutcome,
    change_seq: u64,
) {
    crate::poll_core::global().release_and_rearm(slot);
    let Some(inst) = inst.upgrade() else { return };
    let Some(r) = Run::of(&inst) else { return };
    crate::host::settle_steps_undo(slot, outcome, change_seq);
    let Some((node, cell)) = r.locate(slot) else {
        return;
    };
    r.decide(|d| d.undone(node, cell, slot));
}

/// A message whose entry edge unwound, re-attempted off the reader as a
/// stream's edge is; parked under the channel when it never enters.
pub(super) fn entry_ladder<V: Entering>(
    inst: &RelayInstance,
    channel_id: ChannelId,
    value: &V,
    stream: &Arc<FlezhaStream>,
    offset: Option<u64>,
    call: Option<&Arc<str>>,
    error: String,
) {
    let Some(owner) = inst.steps.as_ref().and_then(|run| run.me.upgrade()) else {
        return;
    };
    let Some(channel) = owner.topology.channel(channel_id) else {
        return;
    };
    let label = format!("entry channel `{}` into its chain", channel.name);
    let target = crate::retry::ParkTarget::edge(channel);
    let held = Arc::clone(&owner);
    let (value, stream, call) = (value.clone(), Arc::clone(stream), call.cloned());
    let origin = (offset, call.clone(), None);
    let step = move || {
        let (Some(r), Some(channel)) = (Run::of(&held), held.topology.channel(channel_id)) else {
            return crate::retry::Attempt::Delivered(Vec::new());
        };
        match r.arrive(channel, channel_id, &value, &stream, offset, call.as_ref()) {
            Ok(()) => crate::retry::Attempt::Delivered(Vec::new()),
            Err(e) => crate::retry::Attempt::Failed(e),
        }
    };
    crate::retry::hand_ladder(&owner, label, target, origin, error, step, None);
}
