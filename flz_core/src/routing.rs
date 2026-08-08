//! The routing kernel: mailbox admission (`enqueue` + overflow policies),
//! consumer-edge delivery, the entry-parse and edge retry→park ladders, and
//! the end-of-slice broadcast. Live delivery and the controller's replay
//! slice both travel these seams.

use std::any::Any;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use std::io::Write;

use flz_transport::service::payloader;
use flz_transport::transport::FlezhaStream;

use crate::channel::OverflowPolicy;
use crate::entity::{make_entity_payload, EntityItem, EntitySlot};
use crate::operative::instance::UnboundedInstance;

/// The Block-policy inline serve, installed by the in-process host: a full
/// consumer is drained on the producing thread because a parked pool worker
/// can be the only thread able to run the slot's queued round (one-worker
/// self-deadlock; prod 2026-06-10). A host that only polls (the C ABI)
/// installs nothing and Block degrades to the bounded park-retry loop.
static INLINE_SERVE: std::sync::OnceLock<fn(&Arc<EntitySlot>)> = std::sync::OnceLock::new();

/// Install the inline-serve hook (idempotent; first install wins).
pub fn install_inline_serve(hook: fn(&Arc<EntitySlot>)) {
    let _ = INLINE_SERVE.set(hook);
}

fn serve_inline(slot: &Arc<EntitySlot>) {
    if let Some(serve) = INLINE_SERVE.get() {
        serve(slot);
    }
}

/// Bounded park per Block-policy retry; progress is re-checked (and the
/// inline drain re-attempted) on every wakeup.
const BLOCK_PARK_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(50);

/// One throttled warning per blocked episode once it exceeds this — a full
/// mailbox holding a producer this long means a consumer procedure is wedged or
/// severely behind, and silence here is how the 2026-06-10 freeze hid.
const BLOCK_WARN_AFTER: std::time::Duration = std::time::Duration::from_secs(5);

pub fn enqueue(slot: &Arc<EntitySlot>, item: EntityItem, policy: OverflowPolicy) {
    // Open the slice here, on the frame-handling thread, rather than in the
    // drain: the controller's end-of-slice broadcast arrives on this same thread
    // after every slice frame, so opening here guarantees the close can never
    // overtake a member it was meant to include. Opening in the drain would race
    // — a slow drain could see the close first and then buffer members nobody
    // would ever flush.
    if matches!(&item, EntityItem::Input(p) if p.replay) {
        slot.replay_open.store(true, Ordering::Release);
    }
    let mut next = item;
    let mut blocked_since: Option<std::time::Instant> = None;
    let mut block_warned = false;
    loop {
        match slot.mailbox.try_push(next) {
            Ok(()) => {
                slot.mailbox
                    .counters()
                    .pushes_total
                    .fetch_add(1, Ordering::Relaxed);
                // Activity stamp for the dynamic-mode reaper (one atomic
                // store; inert for single/static Entities that are never
                // reaped).
                slot.last_activity_at
                    .store(crate::dynamic::now_millis(), Ordering::Relaxed);
                if slot.mailbox.try_mark_inflight() {
                    crate::poll_core::global().schedule(slot);
                }
                return;
            }
            Err(returned) => match policy {
                OverflowPolicy::Block => {
                    slot.mailbox
                        .counters()
                        .pushes_blocked_total
                        .fetch_add(1, Ordering::Relaxed);
                    next = returned;
                    if blocked_since.is_none() {
                        blocked_since = Some(std::time::Instant::now());
                        crate::entity::BLOCKED_PARK_EPISODES.fetch_add(1, Ordering::Relaxed);
                    }
                    // Serve the full slot inline rather than parking on it:
                    // the slot's round may exist only as a queued pool task,
                    // and a parked pool worker can be the lone thread able to
                    // run it (a one-worker pool self-deadlocks; prod
                    // 2026-06-10). The drain_active gate in try_assemble
                    // keeps this race-safe and order-preserving.
                    serve_inline(slot);
                    if slot.mailbox.is_full() {
                        // A live drainer on another thread owns progress;
                        // bounded park so even a lost wake degrades to a
                        // retry loop instead of a permanent freeze.
                        slot.block_parker
                            .wait_one_timeout(&slot.mailbox, BLOCK_PARK_TIMEOUT);
                    }
                    if !block_warned
                        && blocked_since.is_some_and(|t| t.elapsed() >= BLOCK_WARN_AFTER)
                    {
                        block_warned = true;
                        tracing::warn!(
                            target: "flezha::mailbox",
                            procedure_id = slot.procedure_id,
                            entity_key = ?slot.entity_key,
                            "Block-policy producer stalled >{:?} on a full entity mailbox; consumer procedure wedged or far behind",
                            BLOCK_WARN_AFTER
                        );
                    }
                }
                OverflowPolicy::DropNewest => {
                    slot.mailbox
                        .counters()
                        .drops_newest_total
                        .fetch_add(1, Ordering::Relaxed);
                    drop(returned);
                    return;
                }
                OverflowPolicy::DropOldest => {
                    let _dropped = slot.mailbox.force_push(returned);
                    slot.mailbox
                        .counters()
                        .drops_oldest_total
                        .fetch_add(1, Ordering::Relaxed);
                    if slot.mailbox.try_mark_inflight() {
                        crate::poll_core::global().schedule(slot);
                    }
                    return;
                }
            },
        }
    }
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
    pub instance: Arc<UnboundedInstance>,
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
fn run_retry_park_ladder(
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
pub fn route_to_consumers(
    instance: &Arc<UnboundedInstance>,
    channel: &crate::channel::ChannelDescriptor,
    value: &Arc<dyn Any + Send + Sync>,
    stream: &Arc<FlezhaStream>,
    consumed_offset: Option<u64>,
    replay: bool,
) {
    for edge in &channel.consumers {
        if let Err(e) = deliver_edge(
            instance,
            channel,
            edge,
            value,
            stream,
            consumed_offset,
            replay,
        ) {
            spawn_edge_ladder(
                instance,
                channel,
                edge,
                value,
                stream,
                consumed_offset,
                replay,
                e,
            );
        }
    }
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
    instance: &Arc<UnboundedInstance>,
    channel: &crate::channel::ChannelDescriptor,
    edge: &crate::channel::ConsumerEdge,
    value: &Arc<dyn Any + Send + Sync>,
    stream: &Arc<FlezhaStream>,
    consumed_offset: Option<u64>,
    replay: bool,
) -> Result<(), String> {
    match edge.transform_value(value)? {
        None => deliver_to_consumer(
            instance,
            channel,
            edge,
            value,
            stream,
            consumed_offset,
            replay,
        ),
        Some(yielded) => yielded.into_iter().try_for_each(|v| {
            deliver_to_consumer(instance, channel, edge, &v, stream, consumed_offset, replay)
        }),
    }
}

/// Deliver one post-transform value to one consumer's Entity. `Err` carries an
/// unwinding key closure up to the ladder; every other outcome — an absent
/// registry, an unmapped static key — is the pre-existing silent skip.
#[allow(clippy::too_many_arguments)]
fn deliver_to_consumer(
    instance: &UnboundedInstance,
    channel: &crate::channel::ChannelDescriptor,
    edge: &crate::channel::ConsumerEdge,
    value: &Arc<dyn Any + Send + Sync>,
    stream: &Arc<FlezhaStream>,
    consumed_offset: Option<u64>,
    replay: bool,
) -> Result<(), String> {
    use crate::operative::topology::EntityMode;
    let policy = channel.overflow_policy;
    let consumer_procedure_id = edge.procedure;
    let consumer = match instance.topology.procedure(consumer_procedure_id) {
        Some(s) => s,
        None => return Ok(()),
    };
    match consumer.mode {
        EntityMode::Dynamic => {
            let registry = match instance
                .dynamic_registries
                .get(consumer_procedure_id as usize)
                .and_then(|r| r.as_ref())
            {
                Some(r) => r,
                None => return Ok(()),
            };
            // The key is total — `build_topology` refuses an unkeyed edge
            // into a dynamic consumer — so an absent one cannot happen.
            let Some(key) = edge.key_of(value.as_ref())? else {
                return Ok(());
            };
            if let Ok(slot) = registry.get_or_create(&key) {
                enqueue(
                    &slot,
                    EntityItem::Input(make_entity_payload(value, stream, consumed_offset, replay)),
                    policy,
                );
            }
        }
        EntityMode::Single => {
            if let Some(slot) = instance
                .entities
                .get(consumer_procedure_id as usize)
                .and_then(|p| p.first())
            {
                enqueue(
                    slot,
                    EntityItem::Input(make_entity_payload(value, stream, consumed_offset, replay)),
                    policy,
                );
            }
        }
        EntityMode::Static => {
            let entities = match instance.entities.get(consumer_procedure_id as usize) {
                Some(p) => p,
                None => return Ok(()),
            };
            let key_to_idx = instance
                .entity_key_to_idx
                .get(consumer_procedure_id as usize);
            for key in (consumer.entities_for_erased)(value.as_ref()) {
                if let Some(&idx) = key_to_idx.and_then(|m| m.get(key)) {
                    if let Some(slot) = entities.get(idx) {
                        enqueue(
                            slot,
                            EntityItem::Input(make_entity_payload(
                                value,
                                stream,
                                consumed_offset,
                                replay,
                            )),
                            policy,
                        );
                    }
                }
            }
        }
    }
    Ok(())
}

/// Climb this channel's retry→DLQ ladder for one edge whose transform or key
/// unwound. The closures are pure by contract, so a re-attempt is expected to
/// fail identically — the ladder is there so the failure is parked and
/// observable rather than silently taking a thread down, exactly as it is for
/// a poison entry payload.
#[allow(clippy::too_many_arguments)]
fn spawn_edge_ladder(
    instance: &Arc<UnboundedInstance>,
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
fn spawn_park_thread(name: &str, run: impl FnOnce() + Send + 'static) {
    let spawned = std::thread::Builder::new()
        .name(name.to_string())
        .spawn(run);
    if let Err(e) = spawned {
        eprintln!("[flezha] failed to spawn {name} thread: {e}");
    }
}

struct EdgeLadder {
    instance: Arc<UnboundedInstance>,
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
            deliver_edge(
                &ladder.instance,
                channel,
                &ladder.edge,
                &ladder.value,
                &ladder.stream,
                ladder.consumed_offset,
                ladder.replay,
            )
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
pub fn end_replay_slice(instance: &UnboundedInstance) {
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
