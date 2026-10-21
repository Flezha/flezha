//! Mailbox admission under the channel's overflow policy: the entry path's
//! form, which parks the transport reader while `DropNothing` has nowhere to
//! put an item, and the producing Cell's form, which holds the push in
//! that Cell's holdback and withholds work from it instead. The invariant:
//! a producer's pushes land in emit order, whichever form admitted them.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::cell::{CellItem, CellSlot};
use crate::channel::OverflowPolicy;
use crate::util::LockRecover;

/// Steps park per `DropNothing` retry on the entry path, so a lost wake
/// degrades to a retry loop.
const ROOM_WAIT_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(50);

/// One warning per blocked episode past this: a consumer holding a producer
/// this long is wedged or far behind.
const BLOCK_WARN_AFTER: std::time::Duration = std::time::Duration::from_secs(5);

/// Admit one item without waiting for room; `Err` hands it back as
/// [`crate::mailbox::Mailbox::try_push`] does. Shared with the wake list's
/// promotion pass, which must never hold a poller.
pub(crate) fn try_admit(slot: &Arc<CellSlot>, item: CellItem) -> Result<(), CellItem> {
    slot.mailbox.try_push(item)?;
    note_admitted(slot);
    Ok(())
}

/// An item addressed for the entry path and not yet admitted, with its
/// channel's overflow policy.
pub(crate) type Held = (Arc<CellSlot>, CellItem, OverflowPolicy);

/// Admit `held` in order as [`admit_from_reader`] would, but without
/// waiting: stops at the first item `DropNothing` has no room for, counted
/// once as a blocked push however often it is tried (`blocked` carries that
/// across calls), and answers whether everything is in. For the park
/// owner's ladder lane, whose wait would hold up every other relay's ladder.
pub(crate) fn admit_in_order(
    held: &mut std::collections::VecDeque<Held>,
    blocked: &mut bool,
) -> bool {
    while let Some((slot, item, policy)) = held.pop_front() {
        let replay = matches!(&item, CellItem::Input(p) if p.replay);
        if replay {
            slot.replay_open.store(true, Ordering::Release);
        }
        match try_admit(&slot, item) {
            Err(item) if replay || policy == OverflowPolicy::DropNothing => {
                if !std::mem::replace(blocked, true) {
                    slot.note_blocked();
                }
                held.push_front((slot, item, policy));
                return false;
            }
            Err(item) => admit_from_reader(&slot, item, policy),
            Ok(()) => {}
        }
        *blocked = false;
    }
    true
}

/// What every admission owes: the count, the idle sweep's activity stamp, and
/// the round claim that schedules the cell when nobody is draining it.
fn note_admitted(slot: &Arc<CellSlot>) {
    slot.mailbox
        .counters()
        .pushes_total
        .fetch_add(1, Ordering::Relaxed);
    // Activity stamp for the dynamic-mode idle sweep and an instance's sleep (one
    // atomic store).
    slot.last_activity_ms
        .store(crate::util::monotonic_ms(), Ordering::Relaxed);
    if slot.mailbox.claim_inflight() {
        crate::poll_core::global().schedule(slot);
    }
}

/// Admit one item under the channel's policy, PARKING the calling thread
/// while `DropNothing` has nowhere to put it. The entry path's form only: the
/// transport reader has no Cell to withhold work from, and its stalling is
/// the ingress backpressure. A producing Cell takes [`admit_from_cell`].
pub fn admit_from_reader(slot: &Arc<CellSlot>, item: CellItem, policy: OverflowPolicy) {
    // Open the slice here, on the frame-handling thread, rather than in the
    // drain: the controller's end-of-slice broadcast arrives on this same thread
    // after every slice frame, so opening here guarantees the close can never
    // overtake a member it was meant to include. Opening in the drain would race
    // — a slow drain could see the close first and then buffer members nobody
    // would ever flush.
    let replay = matches!(&item, CellItem::Input(p) if p.replay);
    if replay {
        slot.replay_open.store(true, Ordering::Release);
    }
    // A correction's answer may not depend on arrival timing, so a replay-slice
    // member is never shed whatever the channel declares: a dropped member
    // leaves `replay_open` set and `replay_depth` short, the slice still closes
    // on the end-of-slice broadcast, and `flush_closed_slice` folds whatever
    // arrived — a wrong answer with no error anywhere.
    let policy = if replay {
        OverflowPolicy::DropNothing
    } else {
        policy
    };
    let mut next = item;
    let mut blocked_since: Option<std::time::Instant> = None;
    let mut block_warned = false;
    loop {
        match try_admit(slot, next) {
            Ok(()) => return,
            Err(returned) => match policy {
                OverflowPolicy::DropNothing => {
                    next = returned;
                    let since = *blocked_since.get_or_insert_with(std::time::Instant::now);
                    wait_for_room(slot, since, &mut block_warned);
                }
                OverflowPolicy::DropNewest => {
                    slot.note_shed(policy, Some(&returned));
                    return;
                }
                OverflowPolicy::DropOldest => {
                    let evicted = slot.mailbox.force_push(returned);
                    slot.note_shed(policy, evicted.as_ref());
                    // A force-push that admits IS a successful enqueue by
                    // `pushes_total`'s own definition; not counting it made
                    // `processed` exceed `admitted` for this policy.
                    note_admitted(slot);
                    return;
                }
            },
        }
    }
}

/// One `DropNothing` wait on the entry path, warned once per episode past
/// [`BLOCK_WARN_AFTER`].
fn wait_for_room(slot: &Arc<CellSlot>, since: std::time::Instant, warned: &mut bool) {
    slot.note_blocked();
    if slot.mailbox.is_full() {
        // A live drainer on another thread owns progress;
        // steps park so even a lost wake degrades to a
        // retry loop instead of a permanent freeze.
        slot.waiters
            .wait_for_room_timeout(&slot.mailbox, ROOM_WAIT_TIMEOUT);
    }
    if !*warned && since.elapsed() >= BLOCK_WARN_AFTER {
        *warned = true;
        flz_transport::notice!(
            Warn,
            "DropNothing-policy producer stalled >{:?} on a full cell mailbox (tasklet {}, key \
             {:?}); consumer tasklet wedged or far behind",
            BLOCK_WARN_AFTER,
            slot.tasklet_id,
            slot.cell_key
        );
    }
}

/// Whether the non-parking realization of `DropNothing` is on. A rollout
/// switch, not a fourth policy: the guarantee is identical either way.
const ENV_NONPARKING: &str = "FLEZHA_DROP_NOTHING_NONPARKING";

/// How many undelivered pushes one Cell may hold before its emit parks.
/// Only the in-process typed host can reach it: an FFI host's holdback is
/// bounded by one completion's output count.
const ENV_HOLDBACK_MAX: &str = "FLEZHA_PUSH_HOLDBACK_MAX";
const HOLDBACK_MAX_DEFAULT: usize = 4096;

/// Both knobs are read once and cached: they sit on the push path. `0` means
/// "not read yet".
static NONPARKING: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(0);
static HOLDBACK_MAX: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

pub(crate) fn nonparking_enabled() -> bool {
    match NONPARKING.load(Ordering::Relaxed) {
        0 => {
            let on = crate::util::env_bool(ENV_NONPARKING, false);
            NONPARKING.store(1 + u8::from(on), Ordering::Relaxed);
            on
        }
        n => n == 2,
    }
}

fn holdback_max() -> usize {
    match HOLDBACK_MAX.load(Ordering::Relaxed) {
        0 => {
            let max = crate::util::env_parse_positive(ENV_HOLDBACK_MAX, HOLDBACK_MAX_DEFAULT);
            HOLDBACK_MAX.store(max, Ordering::Relaxed);
            max
        }
        n => n,
    }
}

/// Drive both settings from a test without a process per combination.
#[doc(hidden)]
pub fn set_nonparking_for_tests(on: bool) {
    NONPARKING.store(1 + u8::from(on), Ordering::Relaxed);
}

#[doc(hidden)]
pub fn set_holdback_max_for_tests(max: usize) {
    HOLDBACK_MAX.store(max.max(1), Ordering::Relaxed);
}

/// Admit one item produced BY a Cell, withholding work from it instead of
/// holding its thread. Two rules keep emit order total: a push made while the
/// producer already owes one appends behind it, and a Cell owing a push is
/// handed no further activation. `nonparking` is the caller's: a live emit
/// passes [`nonparking_enabled`], a thread that must never park passes
/// `true`. `true` when the push was deferred, which tells a typed batch to
/// hand its tail back.
pub(crate) fn admit_from_cell(
    producer: &Arc<CellSlot>,
    consumer: &Arc<CellSlot>,
    item: CellItem,
    policy: OverflowPolicy,
    nonparking: bool,
) -> bool {
    // A shedding policy has nothing to hold — it admits or discards on the
    // first pass, holding no thread either way. A self-emit into a Cell's
    // OWN full mailbox has nothing to withhold work from: gating it would gate
    // the only round that could drain it, so it keeps the pre-existing
    // behaviour (which wedges the same way the park did, and is why a topology
    // should not do it).
    if !matches!(policy, OverflowPolicy::DropNothing)
        || !nonparking
        || Arc::ptr_eq(producer, consumer)
    {
        admit_from_reader(consumer, item, policy);
        return false;
    }
    // Rule 1. One atomic load on the path every successful push takes; the
    // holdback's lock is touched only once this says it must be.
    while producer.push_deferred.load(Ordering::Acquire) {
        let mut held = producer.push_holdback.lock_recover();
        // Re-read under the lock: the flag is cleared only by a drainer
        // holding it, so a false reading here means the list is empty and a
        // direct attempt cannot overtake anything.
        if !producer.push_deferred.load(Ordering::Acquire) {
            break;
        }
        if held.len() < holdback_max() {
            held.push_back(crate::cell::PendingPush {
                consumer: Arc::clone(consumer),
                item,
            });
            return true;
        }
        drop(held);
        // The bound. Only an in-process typed emit can loop far enough to
        // reach it — an FFI host's outputs are materialized before the core
        // sees them — and that host is the one with a supervised pool behind
        // it. Waiting for THIS producer's own earlier pushes to land, rather
        // than pushing past them, is what keeps the bound from reordering the
        // sequence it exists to bound.
        wait_for_holdback_room(producer, consumer);
    }
    let Err(returned) = try_admit(consumer, item) else {
        return false;
    };
    consumer.note_blocked();
    defer_push(producer, consumer, returned);
    true
}

/// Park until this producer's holdback has room again — the one park an emit
/// path keeps, reached only by an invocation that emitted past the bound.
fn wait_for_holdback_room(producer: &Arc<CellSlot>, consumer: &Arc<CellSlot>) {
    report_holdback_full(producer, consumer);
    producer.note_blocked();
    if consumer.mailbox.is_full() {
        consumer
            .waiters
            .wait_for_room_timeout(&consumer.mailbox, ROOM_WAIT_TIMEOUT);
    }
    crate::poll_core::global().rearm_deferred_pushes(producer);
}

/// Register `producer` as owed `consumer`'s next free slot, and whether room
/// appeared meanwhile — the recheck a parking waiter makes, for the same
/// reason. Called with no holdback lock held: the consumer's drain takes its
/// wait list on every pop, and the two must never nest.
fn owe_room(producer: &Arc<CellSlot>, consumer: &Arc<CellSlot>) -> bool {
    consumer.waiters.wait_producer(producer);
    !consumer.mailbox.is_full()
}

/// Hold one undelivered push and stand the producer down until room appears.
fn defer_push(producer: &Arc<CellSlot>, consumer: &Arc<CellSlot>, item: CellItem) {
    {
        let mut held = producer.push_holdback.lock_recover();
        held.push_back(crate::cell::PendingPush {
            consumer: Arc::clone(consumer),
            item,
        });
        producer.push_deferred.store(true, Ordering::Release);
    }
    if owe_room(producer, consumer) {
        crate::poll_core::global().rearm_deferred_pushes(producer);
    }
}

/// Admit as much of `producer`'s holdback as the consumers take, oldest
/// first, stopping at the first refusal so a partial drain never reorders.
/// Says nothing about the gates, so any thread may call it any number of
/// times.
pub(crate) fn drain_push_holdback(producer: &Arc<CellSlot>) {
    // At most one retry: the recheck answers "room appeared while I was
    // registering", and anything past that is another producer winning the
    // slot — which is the consumer's next pop to resolve, not a spin here.
    for _ in 0..2 {
        let Some(consumer) = admit_held(producer) else {
            return;
        };
        if !owe_room(producer, &consumer) {
            return;
        }
    }
}

/// One drain pass: `Some(consumer)` refused and the rest waits behind it;
/// `None` emptied the holdback and cleared the flag.
fn admit_held(producer: &Arc<CellSlot>) -> Option<Arc<CellSlot>> {
    let mut held = producer.push_holdback.lock_recover();
    while let Some(pending) = held.pop_front() {
        let consumer = Arc::clone(&pending.consumer);
        if let Err(returned) = try_admit(&consumer, pending.item) {
            consumer.note_blocked();
            held.push_front(crate::cell::PendingPush {
                consumer: Arc::clone(&consumer),
                item: returned,
            });
            return Some(consumer);
        }
    }
    // Cleared under the lock so a push that observed the flag set cannot
    // append behind a list nobody will drain again.
    producer.push_deferred.store(false, Ordering::Release);
    None
}

/// One line per [`HOLDBACK_WARN_EVERY_MS`] however many producers are at
/// the bound, naming both ends: the producer looks stuck, the consumer is the
/// one behind, and the heartbeat counters cannot carry that identity.
fn report_holdback_full(producer: &Arc<CellSlot>, consumer: &Arc<CellSlot>) {
    const HOLDBACK_WARN_EVERY_MS: u64 = 5_000;
    static LAST_WARN_MS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let now = crate::util::wall_now_ms();
    let last = LAST_WARN_MS.load(Ordering::Relaxed);
    if now < last + HOLDBACK_WARN_EVERY_MS
        || LAST_WARN_MS
            .compare_exchange(last, now, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
    {
        return;
    }
    flz_transport::notice!(
        Warn,
        "producer {} filled its push holdback of {} for {}; falling back to parking until the \
         consumer catches up",
        producer.dispatcher.cell_label(),
        holdback_max(),
        consumer.dispatcher.cell_label()
    );
}
