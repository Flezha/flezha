//! Batch assembly: everything the drain pop-loop decided, minus user code.
//! Frees one blocked producer per pop; skips inputs at or below the durably
//! consumed floor; buffers replay-slice members until the slice closes;
//! admits (or drops, or asks the controller for a correction of) late
//! inputs; generates due event-time firings ahead of the input that advanced
//! the high-water; validates timer firings against the pending set. The
//! invariant: an event-time class is handed ONE admitted input per batch.

use std::collections::HashSet;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use super::ItemAttempt;
use crate::cell::{CellItem, CellPayload, CellSlot};
use crate::channel::RetryPolicy;
use crate::correction::Lateness;
use crate::timer::{TimerFire, TimerKind};
use crate::util::LockRecover;

pub(super) fn closed_slice_pending(slot: &Arc<CellSlot>) -> bool {
    slot.replay_depth.load(Ordering::Relaxed) != 0 && !slot.replay_open.load(Ordering::Acquire)
}

pub(super) fn event_time_due_pending(slot: &Arc<CellSlot>) -> bool {
    if !slot.dispatcher.is_event_time() {
        return false;
    }
    let high_water = slot.event_time_high_water.load(Ordering::Acquire);
    if high_water < 0 {
        return false;
    }
    !slot
        .timers
        .lock_recover()
        .due(TimerKind::EventTime, high_water as u64)
        .is_empty()
}

/// One batch: the re-handed head of line, then what an earlier round left,
/// then the mailbox.
pub(super) fn assemble_batch(slot: &Arc<CellSlot>) -> Vec<ItemAttempt> {
    let policy = slot.dispatcher.retry_policy();
    let event_time_class = slot.dispatcher.is_event_time();
    let mut items: Vec<ItemAttempt> = Vec::new();
    // Event-time deadlines already included in this batch — an input's
    // advance must not re-fire what an earlier line of the same batch fires.
    let mut fired: HashSet<u64> = HashSet::new();

    {
        let mut held = slot.pending_retry.lock_recover();
        if !held.is_empty() {
            items.extend(held.drain(..));
            if event_time_class {
                // The held items already contain this round's one admitted
                // input (an event-time batch ends with it); admit no more.
                return items;
            }
        }
    }

    if event_time_class && !closed_slice_pending(slot) {
        // Deadlines a previous batch's completion chained behind the
        // high-water. No admitting input to inherit a connection from, so
        // use the live secondary stream (the `rearm_restored` precedent);
        // without one the firing waits for the next input.
        if let Some(stream) = crate::state_sink::secondary_stream() {
            push_due_event_timers(slot, &stream, &mut items, &mut fired, policy);
        }
    }

    if !closed_slice_pending(slot)
        && stage_deferred_live(slot, &mut items, &mut fired, policy, event_time_class)
    {
        return items;
    }

    if drain_mailbox(slot, &mut items, &mut fired, policy, event_time_class) {
        return items;
    }
    // Mailbox dry (or an event-time input closed the batch): a CLOSED slice
    // buffered behind it flushes now, under the same gate; an open one keeps
    // waiting for its broadcast.
    if closed_slice_pending(slot)
        && !(event_time_class && items.iter().any(|w| matches!(w.item, CellItem::Input(_))))
    {
        flush_closed_slice(slot, &mut items, &mut fired, policy, event_time_class);
    }
    items
}

/// Live items an earlier round deferred behind a draining slice: older than
/// the mailbox, so they go first. `true` when one ended the batch.
fn stage_deferred_live(
    slot: &Arc<CellSlot>,
    items: &mut Vec<ItemAttempt>,
    fired: &mut HashSet<u64>,
    policy: RetryPolicy,
    event_time_class: bool,
) -> bool {
    let deferred: Vec<CellItem> = {
        let mut held = slot.deferred_live.lock_recover();
        held.drain(..).collect()
    };
    for item in deferred {
        match stage_popped_item(slot, item, items, fired, policy, event_time_class) {
            Staged::AdmittedInput if event_time_class => return true,
            Staged::BatchClosed => return true,
            _ => {}
        }
    }
    false
}

/// Pop the mailbox until it runs dry or an item closes the batch, serving
/// one waiter per pop; `true` when a live item behind a closed slice was
/// deferred.
fn drain_mailbox(
    slot: &Arc<CellSlot>,
    items: &mut Vec<ItemAttempt>,
    fired: &mut HashSet<u64>,
    policy: RetryPolicy,
    event_time_class: bool,
) -> bool {
    while let Some(item) = slot.mailbox.pop() {
        slot.waiters.serve_one();
        if closed_slice_pending(slot) {
            if let CellItem::Input(payload) = &item {
                if payload.replay {
                    // A member that raced the broadcast: it extends the
                    // closed slice, never splits it.
                    let CellItem::Input(payload) = item else {
                        unreachable!()
                    };
                    slot.replay_buffer.lock_recover().push(payload);
                    slot.replay_depth.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
            }
            // A non-replay item behind a closed slice: the slice runs first —
            // whole where it can, one member per round where pending
            // event-time timers force member-granular determinism. The item
            // waits its turn behind the remainder.
            flush_closed_slice(slot, items, fired, policy, event_time_class);
            if closed_slice_pending(slot) {
                slot.deferred_live.lock_recover().push_back(item);
                return true;
            }
        }
        match stage_popped_item(slot, item, items, fired, policy, event_time_class) {
            Staged::AdmittedInput if event_time_class => return false,
            Staged::BatchClosed => return false,
            _ => continue,
        }
    }
    false
}

enum Staged {
    /// A live input was admitted; an event-time batch ends here.
    AdmittedInput,
    /// Buffered, skipped, dropped, or a timer — the batch stays open.
    Other,
    /// Deferred to the next round and ends this batch: a checkpoint close
    /// behind live items must neither overtake them nor let later pops fold
    /// into an interval that ended.
    BatchClosed,
}

fn stage_popped_item(
    slot: &Arc<CellSlot>,
    item: CellItem,
    items: &mut Vec<ItemAttempt>,
    fired: &mut HashSet<u64>,
    policy: RetryPolicy,
    event_time_class: bool,
) -> Staged {
    let payload = match item {
        CellItem::CloseInterval => return stage_close(slot, items),
        CellItem::Timer(fire) => {
            stage_timer(slot, fire, items, policy);
            return Staged::Other;
        }
        CellItem::Input(payload) => payload,
    };
    // Effectively-once crash-replay: the controller re-forwards the
    // journaled input tail; offsets this Cell already durably consumed
    // (restored effect records) are skipped, never re-run.
    if let Some(offset) = payload.consumed_offset {
        if (offset as i64) <= slot.consumed_floor.load(Ordering::Acquire) {
            crate::cell::DISPATCHES_COMPLETED.fetch_add(1, Ordering::Relaxed);
            return Staged::Other;
        }
    }
    // A replayed slice is held whole, ordered by event time, and only
    // then dispatched. Re-running it in arrival order would put the late
    // input back after the inputs that overtook it and recompute the
    // same wrong answer.
    if payload.replay {
        slot.replay_buffer.lock_recover().push(payload);
        slot.replay_depth.fetch_add(1, Ordering::Relaxed);
        return Staged::Other;
    }
    if stage_input(slot, payload, items, fired, policy, event_time_class) {
        Staged::AdmittedInput
    } else {
        Staged::Other
    }
}

/// A close lands in mailbox order: at the head of a batch close now; behind
/// live items defer it so they run first.
fn stage_close(slot: &Arc<CellSlot>, items: &[ItemAttempt]) -> Staged {
    if items.is_empty() {
        close_due_interval(slot);
        return Staged::Other;
    }
    slot.deferred_live
        .lock_recover()
        .push_back(CellItem::CloseInterval);
    Staged::BatchClosed
}

/// Stage a firing the pending set still holds; the set is the authority and
/// a stale wake-list entry fires nothing.
fn stage_timer(
    slot: &Arc<CellSlot>,
    fire: TimerFire,
    items: &mut Vec<ItemAttempt>,
    policy: RetryPolicy,
) {
    if slot
        .timers
        .lock_recover()
        .contains(fire.kind, fire.scheduled_at)
    {
        items.push(ItemAttempt {
            now: Some(fire.scheduled_at),
            attempt: 1,
            is_final: 1 >= policy.max_attempts,
            item: CellItem::Timer(fire),
        });
    } else {
        crate::cell::DISPATCHES_COMPLETED.fetch_add(1, Ordering::Relaxed);
    }
}

/// Ship an open interval's accumulation on its `max_delay_ms`; a no-op when
/// an input already closed it.
fn close_due_interval(slot: &Arc<CellSlot>) {
    let mut buffer = slot.flush_buffer.lock_recover();
    slot.dispatcher.close_interval(&mut buffer);
}

/// Admit and stage one input, preceded by the event-time deadlines its
/// high-water advance made due; whether it was admitted.
fn stage_input(
    slot: &Arc<CellSlot>,
    payload: CellPayload,
    items: &mut Vec<ItemAttempt>,
    fired: &mut HashSet<u64>,
    policy: RetryPolicy,
    event_time_class: bool,
) -> bool {
    let is_final = 1 >= policy.max_attempts;
    if !event_time_class {
        items.push(ItemAttempt {
            now: None,
            attempt: 1,
            is_final,
            item: CellItem::Input(payload),
        });
        return true;
    }
    let Some(event_time) = slot.dispatcher.event_time_of(&payload) else {
        // Payload type mismatch — hand it through so the typed layer reports
        // it on the dispatch path, as it always has.
        items.push(ItemAttempt {
            now: None,
            attempt: 1,
            is_final,
            item: CellItem::Input(payload),
        });
        return true;
    };
    if !admit_and_track(slot, event_time, payload.replay) {
        crate::cell::DISPATCHES_COMPLETED.fetch_add(1, Ordering::Relaxed);
        return false;
    }
    // Event-time timers fire on the high-water this input just advanced,
    // ascending, and BEFORE the input itself — otherwise a window would
    // close after the record that belongs to the next one.
    push_due_event_timers(slot, &payload.stream, items, fired, policy);
    items.push(ItemAttempt {
        now: Some(event_time),
        attempt: 1,
        is_final,
        item: CellItem::Input(payload),
    });
    true
}

/// Whether an input is dispatched at all, and the high-water it advances. A
/// late input inside the correction window is dropped here and asked for
/// back — the replayed slice re-delivers it in place. A `replayed` input is
/// in order by construction and still advances the high-water.
fn admit_and_track(slot: &Arc<CellSlot>, event_time: u64, replayed: bool) -> bool {
    let advance = || {
        slot.event_time_high_water
            .fetch_max(event_time as i64, Ordering::AcqRel);
    };
    let Some(policy) = slot.dispatcher.correction_policy() else {
        advance();
        return true;
    };
    if replayed {
        advance();
        return true;
    }
    let high_water = slot.event_time_high_water.load(Ordering::Acquire);
    match crate::correction::classify(event_time, high_water, policy.window_ms) {
        Lateness::OnTime => {
            advance();
            true
        }
        // Requested: drop it here, the slice brings it back. Not requested
        // (transport-less run, nobody to ask): fall back to the
        // no-correction behaviour and process it as an arrival.
        Lateness::Correctable => !crate::correction::request(&slot.cell_identity, event_time),
        // The high-water deliberately does NOT advance for a beyond-window
        // arrival: a very old event must not drag the window backward and
        // make the inputs after it look late.
        Lateness::BeyondWindow => match policy.beyond_window {
            crate::operative::topology::BeyondWindowPolicy::ProcessAsArrival => true,
            crate::operative::topology::BeyondWindowPolicy::Drop => false,
        },
    }
}

/// Every pending event-time deadline at or below the high-water, ascending,
/// deduplicated against the batch.
fn push_due_event_timers(
    slot: &Arc<CellSlot>,
    stream: &Arc<flz_transport::transport::FlezhaStream>,
    items: &mut Vec<ItemAttempt>,
    fired: &mut HashSet<u64>,
    policy: RetryPolicy,
) {
    let high_water = slot.event_time_high_water.load(Ordering::Acquire);
    if high_water < 0 {
        return;
    }
    for scheduled_at in slot
        .timers
        .lock_recover()
        .due(TimerKind::EventTime, high_water as u64)
    {
        if fired.insert(scheduled_at) {
            items.push(ItemAttempt {
                now: Some(scheduled_at),
                attempt: 1,
                is_final: 1 >= policy.max_attempts,
                item: CellItem::Timer(TimerFire {
                    kind: TimerKind::EventTime,
                    scheduled_at,
                    stream: Arc::clone(stream),
                }),
            });
        }
    }
}

/// The slice closed: order it by event time (stable, then by offset, so a
/// re-run folds the same order) and stage ONE member; the rest re-buffers
/// for the next round. The high-water is never reset — the restore rebuilt
/// it.
fn flush_closed_slice(
    slot: &Arc<CellSlot>,
    items: &mut Vec<ItemAttempt>,
    fired: &mut HashSet<u64>,
    policy: RetryPolicy,
    event_time_class: bool,
) {
    let mut slice = std::mem::take(&mut *slot.replay_buffer.lock_recover());
    slot.replay_depth.store(0, Ordering::Relaxed);
    slice.sort_by_key(|p| {
        (
            slot.dispatcher.event_time_of(p).unwrap_or(0),
            p.consumed_offset.unwrap_or(0),
        )
    });
    // Member-granular draining is only needed when pending event-time
    // deadlines could interleave with the members (a chained registration
    // lands at completion, and a crash-resumed correction must fold the same
    // interleave). With none pending — the overwhelmingly common case — the
    // whole ordered slice is one batch, exactly the pre-poll drain.
    let member_granular = event_time_class
        && !slot
            .timers
            .lock_recover()
            .due(TimerKind::EventTime, u64::MAX)
            .is_empty();
    if member_granular {
        let mut members = slice.into_iter();
        if let Some(first) = members.next() {
            let rest: Vec<CellPayload> = members.collect();
            if !rest.is_empty() {
                slot.replay_depth
                    .store(rest.len() as u64, Ordering::Relaxed);
                *slot.replay_buffer.lock_recover() = rest;
            }
            stage_input(slot, first, items, fired, policy, true);
        }
    } else {
        for payload in slice {
            stage_input(slot, payload, items, fired, policy, event_time_class);
        }
    }
}
