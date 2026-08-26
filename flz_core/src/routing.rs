//! The routing kernel: mailbox admission (`enqueue` + overflow policies),
//! consumer-edge delivery, the entry-parse and edge retry→park ladders, and
//! the end-of-slice broadcast. Live delivery and the controller's replay
//! slice both travel these seams.

use std::any::Any;
use std::collections::HashMap;
use std::io::Write;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use flz_transport::service::payloader;
use flz_transport::transport::FlezhaStream;

use crate::channel::OverflowPolicy;
use crate::entity::{make_entity_payload, EntityItem, EntitySlot};
use crate::operative::instance::StreamInstance;
use crate::util::LockRecover;

/// Bounded park per DropNothing-policy retry on the entry path; progress is
/// re-checked on every wakeup so even a lost wake degrades to a retry loop.
const BLOCK_PARK_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(50);

/// One throttled warning per blocked episode once it exceeds this — a full
/// mailbox holding a producer this long means a consumer procedure is wedged or
/// severely behind, and silence here is how the 2026-06-10 freeze hid.
const BLOCK_WARN_AFTER: std::time::Duration = std::time::Duration::from_secs(5);

/// Admit one item without ever waiting for room: the success half of
/// [`enqueue`], shared with the wake list's promotion pass, which must return
/// the item rather than hold the poller that is servicing it.
///
/// `Err` hands the item back exactly as [`crate::mailbox::Mailbox::try_push`]
/// does, so the caller decides what a full mailbox means.
pub(crate) fn try_admit(slot: &Arc<EntitySlot>, item: EntityItem) -> Result<(), EntityItem> {
    slot.mailbox.try_push(item)?;
    note_admitted(slot);
    Ok(())
}

/// What every admission owes, whichever path admitted the item — a plain
/// `try_push` or a `DropOldest` force-push: the count, the reaper's activity
/// stamp, and the round claim that schedules the entity when nobody is
/// draining it.
fn note_admitted(slot: &Arc<EntitySlot>) {
    slot.mailbox
        .counters()
        .pushes_total
        .fetch_add(1, Ordering::Relaxed);
    // Activity stamp for the dynamic-mode reaper (one atomic store; inert for
    // single/static Entities that are never reaped).
    slot.last_activity_at
        .store(crate::dynamic::now_millis(), Ordering::Relaxed);
    if slot.mailbox.try_mark_inflight() {
        crate::poll_core::global().schedule(slot);
    }
}

/// Admit one item under the channel's policy, PARKING the calling thread while
/// `DropNothing` has nowhere to put it.
///
/// The entry path's form, and only the entry path's: the shared transport
/// reader has no producing Entity to withhold work from, its stalling is what
/// carries ingress backpressure to the controller, and it is a core-owned
/// thread no host language ever observes. Every producing Entity takes
/// [`admit_from_entity`] instead, which holds no thread at all.
pub fn enqueue(slot: &Arc<EntitySlot>, item: EntityItem, policy: OverflowPolicy) {
    // Open the slice here, on the frame-handling thread, rather than in the
    // drain: the controller's end-of-slice broadcast arrives on this same thread
    // after every slice frame, so opening here guarantees the close can never
    // overtake a member it was meant to include. Opening in the drain would race
    // — a slow drain could see the close first and then buffer members nobody
    // would ever flush.
    let replay = matches!(&item, EntityItem::Input(p) if p.replay);
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
                    slot.note_blocked();
                    next = returned;
                    let since = *blocked_since.get_or_insert_with(std::time::Instant::now);
                    if slot.mailbox.is_full() {
                        // A live drainer on another thread owns progress;
                        // bounded park so even a lost wake degrades to a
                        // retry loop instead of a permanent freeze.
                        slot.block_parker
                            .wait_one_timeout(&slot.mailbox, BLOCK_PARK_TIMEOUT);
                    }
                    if !block_warned && since.elapsed() >= BLOCK_WARN_AFTER {
                        block_warned = true;
                        tracing::warn!(
                            target: "flezha::mailbox",
                            procedure_id = slot.procedure_id,
                            entity_key = ?slot.entity_key,
                            "DropNothing-policy producer stalled >{:?} on a full entity mailbox; consumer procedure wedged or far behind",
                            BLOCK_WARN_AFTER
                        );
                    }
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

/// Whether the non-parking realization of `DropNothing` is enabled. A rollout
/// switch, not a fourth policy: the guarantee an author declared — nothing
/// discarded, order preserved — is identical either way, and only the
/// deployment learns which mechanism carried it.
const ENV_NONPARKING: &str = "FLEZHA_DROP_NOTHING_NONPARKING";

/// How many undelivered pushes one producing Entity may hold before its emit
/// falls back to parking. Only the in-process typed host can reach it — an FFI
/// host's outputs are materialized as bytes before the core sees them, so its
/// holdback is bounded by one completion's output count — and that host is the
/// one with a supervised worker pool behind it.
const ENV_HOLDBACK_MAX: &str = "FLEZHA_PUSH_HOLDBACK_MAX";
const HOLDBACK_MAX_DEFAULT: usize = 4096;

/// Both knobs are read once and cached in an atomic: they are consulted on the
/// push path, where an `std::env::var` per call would cost more than the whole
/// mechanism. `0` means "not read yet"; the test setters below are what let one
/// binary exercise both settings.
static NONPARKING: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(0);
static HOLDBACK_MAX: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

pub(crate) fn nonparking_enabled() -> bool {
    match NONPARKING.load(Ordering::Relaxed) {
        0 => {
            let on = crate::util::env_flag(ENV_NONPARKING);
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

/// Drive both settings from a test without a process per combination — the one
/// flag whose whole point is that a deployment can flip it.
#[doc(hidden)]
pub fn set_nonparking_for_tests(on: bool) {
    NONPARKING.store(1 + u8::from(on), Ordering::Relaxed);
}

#[doc(hidden)]
pub fn set_holdback_max_for_tests(max: usize) {
    HOLDBACK_MAX.store(max.max(1), Ordering::Relaxed);
}

/// Admit one item produced BY an Entity, withholding work from that Entity
/// instead of holding the thread that produced it.
///
/// Two rules make the ordering a parked producer achieved total, and both live
/// here:
///
/// 1. a push made while `producer` already owes one appends behind it rather
///    than attempting — otherwise item 2 of a completion's batch could land
///    while item 1 is still held;
/// 2. an Entity owing a push is handed no further activation
///    ([`crate::poll_core::PollCore::release_and_rearm`]).
///
/// Returns `true` when the push was deferred, which is what tells a typed
/// batch to stop dispatching and hand its tail back.
pub(crate) fn admit_from_entity(
    producer: &Arc<EntitySlot>,
    consumer: &Arc<EntitySlot>,
    item: EntityItem,
    policy: OverflowPolicy,
) -> bool {
    // A shedding policy has nothing to hold — it admits or discards on the
    // first pass, holding no thread either way. A self-emit into an Entity's
    // OWN full mailbox has nothing to withhold work from: gating it would gate
    // the only round that could drain it, so it keeps the pre-existing
    // behaviour (which wedges the same way the park did, and is why a topology
    // should not do it).
    if !matches!(policy, OverflowPolicy::DropNothing)
        || !nonparking_enabled()
        || Arc::ptr_eq(producer, consumer)
    {
        enqueue(consumer, item, policy);
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
            held.push_back(crate::entity::PendingPush {
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

/// Park the calling thread until this producer's holdback has room again.
/// The one remaining park on an emit path, and the only one a host thread can
/// still see — reached solely by an invocation that emitted past the bound.
fn wait_for_holdback_room(producer: &Arc<EntitySlot>, consumer: &Arc<EntitySlot>) {
    report_holdback_full(producer, consumer);
    producer.note_blocked();
    if consumer.mailbox.is_full() {
        consumer
            .block_parker
            .wait_one_timeout(&consumer.mailbox, BLOCK_PARK_TIMEOUT);
    }
    crate::poll_core::global().rearm_deferred_pushes(producer);
}

/// Register `producer` as owed `consumer`'s next free slot, and say whether
/// room appeared while we were doing it.
///
/// The recheck is the one a parking waiter makes and is owed for the same
/// reason: a drain that emptied the mailbox between the failed admission and
/// the registration would leave nobody to wake it. Both places a producer
/// stands down go through here, so the two cannot drift — the first draft had
/// the recheck on the deferral and not on the re-drain, which is a lost wake
/// costing a full fallback window.
///
/// Called with no holdback lock held: the consumer's drain takes its wait list
/// on every pop, and the two must never nest.
fn owe_room(producer: &Arc<EntitySlot>, consumer: &Arc<EntitySlot>) -> bool {
    consumer.block_parker.wait_producer(producer);
    !consumer.mailbox.is_full()
}

/// Hold one undelivered push and stand the producer down until room appears.
fn defer_push(producer: &Arc<EntitySlot>, consumer: &Arc<EntitySlot>, item: EntityItem) {
    {
        let mut held = producer.push_holdback.lock_recover();
        held.push_back(crate::entity::PendingPush {
            consumer: Arc::clone(consumer),
            item,
        });
        producer.push_deferred.store(true, Ordering::Release);
    }
    if owe_room(producer, consumer) {
        crate::poll_core::global().rearm_deferred_pushes(producer);
    }
}

/// Admit as much of `producer`'s holdback as the consumers will take, oldest
/// first. Stops at the first refusal and leaves the rest in order, so a
/// per-producer emit sequence can never be reordered by a partial drain.
///
/// Says nothing about the Entity's gates: `push_gated` is the single claim on
/// releasing those, so this may be called by any thread, any number of times.
pub(crate) fn drain_push_holdback(producer: &Arc<EntitySlot>) {
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

/// One drain pass. `Some(consumer)` names the one that refused and left the
/// rest of the holdback behind it; `None` means the holdback emptied and the
/// flag is cleared.
fn admit_held(producer: &Arc<EntitySlot>) -> Option<Arc<EntitySlot>> {
    let mut held = producer.push_holdback.lock_recover();
    while let Some(pending) = held.pop_front() {
        let consumer = Arc::clone(&pending.consumer);
        if let Err(returned) = try_admit(&consumer, pending.item) {
            consumer.note_blocked();
            held.push_front(crate::entity::PendingPush {
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

/// One line per [`HOLDBACK_WARN_EVERY_MS`], however many producers are at the
/// bound: a filled holdback is a pathology, and a pathology that logs per push
/// is its own outage. It names both ends because the producer is the Entity
/// that looks stuck and the consumer is the one actually behind — the identity
/// the process-wide heartbeat counters cannot carry.
fn report_holdback_full(producer: &Arc<EntitySlot>, consumer: &Arc<EntitySlot>) {
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
    tracing::warn!(
        target: "flezha::mailbox",
        producer = producer.dispatcher.entity_label(),
        consumer = consumer.dispatcher.entity_label(),
        holdback_max = holdback_max(),
        "producer filled its push holdback; falling back to parking until the consumer catches up"
    );
}

/// Fan a typed value out to every consumer Procedure's entity mailbox per
/// the channel's `OverflowPolicy`. Shared by the inter-procedure emit path
/// (`EntityEmitSink`) and the inbound wire-dispatch path
/// (`OperativeDispatchHandle`). Single-mode procedures route to entity 0;
/// static-mode procedures route by `entities_for_erased` key lookup;
/// dynamic-mode procedures derive a key via the consumer edge's `key_by` and
/// lazily create the Entity through the per-Procedure `DynamicEntityRegistry`.
/// A poison entry payload climbing its channel's retry→DLQ ladder off the
/// wire-reader thread (`sdk-graph-authoring` typed entries): the failing parse
/// is re-attempted per the channel's retry budget — the parser contract says
/// deterministic, so this is expected to fail identically — and then parked to
/// the dead-letter queue under the entry channel's identity, exactly like a
/// retry-exhausted invocation.
pub struct EntryParseLadder {
    pub instance: Arc<StreamInstance>,
    pub channel_id: u32,
    pub bytes: Vec<u8>,
    pub first_error: String,
    pub stream: Arc<flz_transport::transport::FlezhaStream>,
    pub consumed_offset: Option<u64>,
    pub replay: bool,
}

pub fn spawn_entry_parse_ladder(ladder: EntryParseLadder) {
    spawn_park_thread("flezha-entry-parse-park", move || {
        run_entry_parse_ladder(ladder)
    });
}

fn run_entry_parse_ladder(ladder: EntryParseLadder) {
    let channel = ladder
        .instance
        .topology
        .channel(ladder.channel_id)
        .expect("entry channel present in topology");
    let wrap = channel
        .wrap_input
        .as_ref()
        .expect("parsed entry channel carries a wrap_input");
    run_retry_park_ladder(
        &format!("entry channel `{}`", channel.name),
        channel,
        &ladder.instance.instance_id,
        "_entry",
        ladder.consumed_offset,
        Some(ladder.bytes.clone()),
        ladder.first_error.clone(),
        || {
            // A parser violating the determinism contract may succeed on a
            // re-attempt; deliver rather than drop.
            let typed = wrap(ladder.bytes.clone())?;
            route_to_consumers(
                &ladder.instance,
                channel,
                &typed,
                &ladder.stream,
                ladder.consumed_offset,
                ladder.replay,
                None,
            );
            Ok(())
        },
    );
}

/// Re-attempt one failed step off the routing thread per the channel's retry
/// budget, then park what still fails to the dead-letter queue under the
/// channel's identity. The one ladder both poison entry payloads and unwinding
/// edge closures climb — `attempt` re-runs the step AND delivers on success,
/// so the two differ only in what that step is.
#[allow(clippy::too_many_arguments)]
pub(crate) fn run_retry_park_ladder(
    label: &str,
    channel: &crate::channel::ChannelDescriptor,
    instance_id: &str,
    entity_suffix: &str,
    consumed_offset: Option<u64>,
    payload: Option<Vec<u8>>,
    first_error: String,
    mut attempt_step: impl FnMut() -> Result<(), String>,
) {
    let mut attempt: u32 = 1; // the routing-thread run was attempt 1
    let mut error = first_error;
    while attempt < channel.retry.max_attempts {
        let delay = crate::retry::note_failed_attempt(label, attempt, channel.retry, &error);
        crate::util::sleep_interruptible(delay, None);
        attempt += 1;
        match attempt_step() {
            Ok(()) => return,
            Err(e) => error = e,
        }
    }

    crate::park::PARKED_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    eprintln!(
        "[flezha] {} parking payload to DLQ after {} attempt(s): {}",
        label, attempt, error
    );
    let identity =
        crate::flusher::encode_entity_identity(instance_id, &channel.name, entity_suffix);
    let body = crate::park::ParkBody {
        park_id: crate::park::mint_park_id(&format!(
            "{}/{}/{}",
            instance_id, channel.name, entity_suffix
        )),
        origin_offset: consumed_offset,
        entry: crate::flusher::DlqEntryBody {
            channel: channel.name.to_string(),
            error,
            attempts: attempt,
            payload,
        },
    };
    crate::park::park_blocking(&identity, &body);
}

/// Route one channel value to every subscribed consumer, each through its own
/// edge's transform and key. The one seam a `ConsumerEdge`'s closures are
/// applied on: the wire-reader path (live delivery and the controller's
/// re-forwarded replay slice) and an inter-procedure emit both come through
/// here, so the two can never drift.
///
/// A closure that unwinds does not take the routing thread with it — the value
/// climbs this channel's retry→DLQ ladder, the same one a failing entry parse
/// rides, and the remaining consumers are still served.
#[allow(clippy::too_many_arguments)]
pub fn route_to_consumers(
    instance: &Arc<StreamInstance>,
    channel: &crate::channel::ChannelDescriptor,
    value: &Arc<dyn Any + Send + Sync>,
    stream: &Arc<FlezhaStream>,
    consumed_offset: Option<u64>,
    replay: bool,
    producer: Option<&Arc<EntitySlot>>,
) -> bool {
    let mut deferred = false;
    for edge in &channel.consumers {
        match deliver_edge(
            instance,
            channel,
            edge,
            value,
            stream,
            consumed_offset,
            replay,
            producer,
        ) {
            Ok(held) => deferred |= held,
            Err(e) => spawn_edge_ladder(
                instance,
                channel,
                edge,
                value,
                stream,
                consumed_offset,
                replay,
                e,
            ),
        }
    }
    deferred
}

/// Put one value through one edge's transform and deliver everything it
/// yields. The ONE place a `ConsumerEdge`'s closures become mailbox pushes:
/// live routing and the retry ladder's re-attempt both call it, so a
/// re-attempt cannot deliver differently from the attempt it retries.
///
/// A transform-free edge — every pre-existing topology — takes the `None` arm
/// and pays nothing.
#[allow(clippy::too_many_arguments)]
fn deliver_edge(
    instance: &Arc<StreamInstance>,
    channel: &crate::channel::ChannelDescriptor,
    edge: &crate::channel::ConsumerEdge,
    value: &Arc<dyn Any + Send + Sync>,
    stream: &Arc<FlezhaStream>,
    consumed_offset: Option<u64>,
    replay: bool,
    producer: Option<&Arc<EntitySlot>>,
) -> Result<bool, String> {
    match edge.transform_value(value)? {
        None => deliver_to_consumer(
            instance,
            channel,
            edge,
            value,
            stream,
            consumed_offset,
            replay,
            producer,
        ),
        Some(yielded) => yielded.into_iter().try_fold(false, |held, v| {
            deliver_to_consumer(
                instance,
                channel,
                edge,
                &v,
                stream,
                consumed_offset,
                replay,
                producer,
            )
            .map(|d| held | d)
        }),
    }
}

/// Deliver one host completion's edge value to its named consumer — the
/// single-edge form of [`route_to_consumers`] for producer-side-evaluated
/// outputs (the host already ran the edge's transforms and key).
#[allow(clippy::too_many_arguments)]
pub(crate) fn deliver_host_output(
    instance: &Arc<StreamInstance>,
    channel_id: u32,
    consumer: u32,
    value: &Arc<dyn Any + Send + Sync>,
    stream: &Arc<FlezhaStream>,
    consumed_offset: Option<u64>,
    producer: &Arc<EntitySlot>,
) -> Result<bool, String> {
    let channel = instance
        .topology
        .channel(channel_id)
        .ok_or_else(|| format!("edge output names unknown channel {channel_id}"))?;
    let edge = channel
        .consumers
        .iter()
        .find(|e| e.procedure == consumer)
        .ok_or_else(|| {
            format!(
                "edge output names procedure {consumer}, not a consumer of channel `{}`",
                channel.name
            )
        })?;
    // An inter-procedure value is never a replay member: the downstream
    // Procedure receives corrected outputs already in order.
    deliver_to_consumer(
        instance,
        channel,
        edge,
        value,
        stream,
        consumed_offset,
        false,
        Some(producer),
    )
}

/// Deliver one post-transform value to one consumer's Entity. `Err` carries an
/// unwinding key closure up to the ladder; every other outcome — an absent
/// registry, an unmapped static key — is the pre-existing silent skip.
#[allow(clippy::too_many_arguments)]
fn deliver_to_consumer(
    instance: &StreamInstance,
    channel: &crate::channel::ChannelDescriptor,
    edge: &crate::channel::ConsumerEdge,
    value: &Arc<dyn Any + Send + Sync>,
    stream: &Arc<FlezhaStream>,
    consumed_offset: Option<u64>,
    replay: bool,
    producer: Option<&Arc<EntitySlot>>,
) -> Result<bool, String> {
    use crate::operative::topology::EntityMode;
    let policy = channel.overflow_policy;
    let mut deferred = false;
    // The one place the two admission forms are chosen between: a value with a
    // producing Entity withholds work from it, a value from the entry path (or
    // a replay member, which only the entry path produces) parks the reader.
    let mut admit = |consumer: &Arc<EntitySlot>, item| match producer {
        Some(p) if !replay => deferred |= admit_from_entity(p, consumer, item, policy),
        _ => enqueue(consumer, item, policy),
    };
    let consumer_procedure_id = edge.procedure;
    let consumer = match instance.topology.procedure(consumer_procedure_id) {
        Some(s) => s,
        None => return Ok(false),
    };
    match consumer.mode {
        EntityMode::Dynamic => {
            let registry = match instance
                .dynamic_registries
                .get(consumer_procedure_id as usize)
                .and_then(|r| r.as_ref())
            {
                Some(r) => r,
                None => return Ok(false),
            };
            // The key is total for typed classes — `build_topology` refuses
            // an unkeyed edge into a dynamic consumer. A host class's key
            // rides the payload itself (prepare crossing / producer-side
            // evaluation), so it is consulted when no edge closure exists.
            let key = match edge.key_of(value.as_ref())? {
                Some(k) => Some(k),
                None => value
                    .downcast_ref::<crate::host::HostPayload>()
                    .and_then(|p| p.keys.first().cloned()),
            };
            let Some(key) = key else {
                return Ok(false);
            };
            // A key this member does not own means the source and the topology
            // disagree about what a key is. Refuse on the first frame — before
            // `get_or_create` mints an Entity — so a misconfigured group parks
            // its traffic instead of folding half of it into private state.
            if instance.partition.partitioned() {
                instance.partition.admit(&key)?;
            }
            if let Ok(slot) = registry.get_or_create(&key) {
                admit(
                    &slot,
                    EntityItem::Input(make_entity_payload(value, stream, consumed_offset, replay)),
                );
            }
        }
        EntityMode::Single => {
            if let Some(slot) = instance
                .entities
                .get(consumer_procedure_id as usize)
                .and_then(|p| p.first())
            {
                admit(
                    slot,
                    EntityItem::Input(make_entity_payload(value, stream, consumed_offset, replay)),
                );
            }
        }
        EntityMode::Static => {
            let entities = match instance.entities.get(consumer_procedure_id as usize) {
                Some(p) => p,
                None => return Ok(false),
            };
            let key_to_idx = instance
                .entity_key_to_idx
                .get(consumer_procedure_id as usize);
            let partition = instance.partition;
            let mut deliver_idx = |idx: usize| {
                // A statically keyed class partitions by leaving its
                // out-of-range declared Entities idle: one filter here covers
                // the named-key routes and the broadcast alike. Unlike a
                // dynamic key this is never a park — a declared entity the
                // member does not own is not a mis-route, it is somebody
                // else's entity.
                if partition.partitioned()
                    && !consumer
                        .entity_keys
                        .get(idx)
                        .is_some_and(|key| partition.owns(key))
                {
                    return;
                }
                if let Some(slot) = entities.get(idx) {
                    admit(
                        slot,
                        EntityItem::Input(make_entity_payload(
                            value,
                            stream,
                            consumed_offset,
                            replay,
                        )),
                    );
                }
            };
            if let Some(host_payload) = value.downcast_ref::<crate::host::HostPayload>() {
                // Host fan-out keys ride the payload (prepare crossing /
                // producer-side evaluation); an empty set means every
                // declared entity — the entities-for default.
                if host_payload.keys.is_empty() {
                    for idx in 0..entities.len() {
                        deliver_idx(idx);
                    }
                } else {
                    for key in &host_payload.keys {
                        if let Some(&idx) = key_to_idx.and_then(|m| m.get(key.as_str())) {
                            deliver_idx(idx);
                        }
                    }
                }
            } else {
                for key in (consumer.entities_for_erased)(value.as_ref()) {
                    if let Some(&idx) = key_to_idx.and_then(|m| m.get(key)) {
                        deliver_idx(idx);
                    }
                }
            }
        }
    }
    // The closure's borrow of `deferred` ends at its last use above.
    Ok(deferred)
}

/// Climb this channel's retry→DLQ ladder for one edge whose transform or key
/// unwound. The closures are pure by contract, so a re-attempt is expected to
/// fail identically — the ladder is there so the failure is parked and
/// observable rather than silently taking a thread down, exactly as it is for
/// a poison entry payload.
#[allow(clippy::too_many_arguments)]
fn spawn_edge_ladder(
    instance: &Arc<StreamInstance>,
    channel: &crate::channel::ChannelDescriptor,
    edge: &crate::channel::ConsumerEdge,
    value: &Arc<dyn Any + Send + Sync>,
    stream: &Arc<FlezhaStream>,
    consumed_offset: Option<u64>,
    replay: bool,
    error: String,
) {
    let Some(channel_id) = instance.topology.lookup_channel_id(&channel.name) else {
        eprintln!("[flezha] edge transform failed on an unregistered channel: {error}");
        return;
    };
    let ladder = EdgeLadder {
        instance: Arc::clone(instance),
        channel_id,
        edge: edge.clone(),
        value: Arc::clone(value),
        first_error: error,
        stream: Arc::clone(stream),
        consumed_offset,
        replay,
    };
    spawn_park_thread("flezha-edge-transform-park", move || {
        run_edge_ladder(ladder)
    });
}

/// Run a retry→DLQ ladder off the routing thread. Both ladders spawn here:
/// the wait between attempts must not stall the wire reader, and a failure to
/// spawn is reported rather than silently dropping the payload's last chance.
pub(crate) fn spawn_park_thread(name: &str, run: impl FnOnce() + Send + 'static) {
    let spawned = std::thread::Builder::new()
        .name(name.to_string())
        .spawn(run);
    if let Err(e) = spawned {
        eprintln!("[flezha] failed to spawn {name} thread: {e}");
    }
}

struct EdgeLadder {
    instance: Arc<StreamInstance>,
    channel_id: u32,
    edge: crate::channel::ConsumerEdge,
    value: Arc<dyn Any + Send + Sync>,
    first_error: String,
    stream: Arc<FlezhaStream>,
    consumed_offset: Option<u64>,
    replay: bool,
}

fn run_edge_ladder(ladder: EdgeLadder) {
    let channel = ladder
        .instance
        .topology
        .channel(ladder.channel_id)
        .expect("channel present in topology");
    let procedure = ladder
        .instance
        .topology
        .procedure(ladder.edge.procedure)
        .map(|p| p.name.to_string())
        .unwrap_or_default();
    run_retry_park_ladder(
        &format!("channel `{}` edge into `{}`", channel.name, procedure),
        channel,
        &ladder.instance.instance_id,
        "_edge",
        ladder.consumed_offset,
        None,
        ladder.first_error,
        || {
            // The ladder re-attempt runs off the routing thread with no
            // producing Entity in hand, so it takes the entry form: the
            // producer that emitted this value is long since released.
            deliver_edge(
                &ladder.instance,
                channel,
                &ladder.edge,
                &ladder.value,
                &ladder.stream,
                ladder.consumed_offset,
                ladder.replay,
                None,
            )
            .map(|_| ())
        },
    );
}

/// Close every Entity's replayed slice and wake their drains to run it.
///
/// A broadcast rather than a marker riding the slice's last input, because it
/// has to be: an input routes to exactly one Entity by key, so a marker on the
/// last one would reach one Entity and leave every other Entity that buffered
/// slice members holding them forever.
///
/// Arrives on the frame-handling thread after every frame of the slice, so a
/// Entity cannot still be receiving members when its slice is closed.
pub fn end_replay_slice(instance: &StreamInstance) {
    let statics = instance.entities.iter().flatten().cloned();
    let dynamics = instance.dynamic_registries.iter().flatten().flat_map(|r| {
        r.entities
            .iter()
            .map(|e| Arc::clone(e.value()))
            .collect::<Vec<_>>()
    });

    for slot in statics.chain(dynamics) {
        slot.replay_open.store(false, Ordering::Release);
        // The drain may already have run dry and exited with the slice still
        // buffered — it had no way to know more was coming. Wake it so it can
        // see the closed slice and flush it.
        if slot.mailbox.try_mark_inflight() {
            crate::poll_core::global().schedule(&slot);
        }
    }
}

pub fn send_plugin_frame(
    stream: &Arc<FlezhaStream>,
    instance_id: &str,
    bytes: &[u8],
    metadata: &HashMap<String, String>,
) {
    let frame = payloader::build_from_binary(instance_id, bytes, metadata.clone());
    if let Err(e) = (&**stream).write_all(&frame) {
        eprintln!(
            "[flezha] plugin emit: failed to write {} bytes for {}: {}",
            bytes.len(),
            instance_id,
            e
        );
    }
}
