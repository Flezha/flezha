//! The retry ladder both operative profiles climb before their terminal escape.
//!
//! An invocation that fails retryably is re-run on its [`RetryPolicy`] — the
//! input channel's for a stream procedure (`dead-letter-queue`),
//! the class's for a workflow entity (`workflow-entity-retry`) — and only an
//! exhausted (or author-classified non-retryable) failure escapes: the stream
//! profile parks the payload to the dead-letter queue and advances, the workflow
//! profile fails the procedure into AbortAll + saga.
//!
//! What is shared is the **policy**: how long attempt `n` waits, that the wait
//! is counted and logged, and that a caller with a cancellation token may cut it
//! short. What each profile does per attempt — re-clone the input and extract
//! park bytes, or re-run an entity's batch into a fresh sink — stays with the
//! profile. The two off-thread ladders the routing seam climbs — a poison entry
//! parse and an unwinding edge closure — live here too.

use std::any::Any;
use std::fmt::Display;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use flz_transport::transport::FlezhaStream;
use flz_transport::utility::noise::{noise, xorshift64};

use crate::channel::RetryPolicy;
use crate::operative::instance::StreamInstance;
use crate::park::RETRIES_TOTAL;
use crate::routing::{deliver_edge, route_to_consumers};

/// Record a failed-but-retryable attempt — count it (the `data_heartbeat`
/// retry metric) and log it — and return how long to wait before the next one.
/// `label` names the failing work (procedure, or workflow instance/procedure/
/// entity).
pub fn note_failed_attempt(
    label: &str,
    attempt: u32,
    policy: RetryPolicy,
    error: &dyn Display,
) -> Duration {
    let delay = backoff(policy.backoff_base_ms, attempt);
    RETRIES_TOTAL.fetch_add(1, Ordering::Relaxed);
    eprintln!(
        "[flezha] {label} attempt {attempt}/{} failed (retrying in {} ms): {error}",
        policy.max_attempts,
        delay.as_millis()
    );
    delay
}

/// Exponential backoff with ±50% jitter: `base * 2^(attempt-1)`, jitter
/// included, capped at [`crate::channel::RETRY_BACKOFF_MAX_MS`]. The jitter
/// draws from a cheap xorshift over the wall clock — backoff timing is runtime
/// scheduling, outside the procedure determinism contract.
fn backoff(base_ms: u64, attempt: u32) -> Duration {
    let exp = base_ms
        .max(1)
        .saturating_mul(1u64 << (attempt.saturating_sub(1)).min(16))
        .min(crate::channel::RETRY_BACKOFF_MAX_MS);
    let mut x = noise() | 1;
    xorshift64(&mut x);
    // Uniform in [exp/2, exp*1.5), then re-capped.
    let jittered = exp / 2 + x % exp;
    Duration::from_millis(jittered.min(crate::channel::RETRY_BACKOFF_MAX_MS))
}

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
    pub stream: Arc<FlezhaStream>,
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
        let delay = note_failed_attempt(label, attempt, channel.retry, &error);
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

/// Climb this channel's retry→DLQ ladder for one edge whose transform or key
/// unwound. The closures are pure by contract, so a re-attempt is expected to
/// fail identically — the ladder is there so the failure is parked and
/// observable rather than silently taking a thread down, exactly as it is for
/// a poison entry payload.
#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_edge_ladder(
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
