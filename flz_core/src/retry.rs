//! The retry policy every ladder climbs — how long attempt `n` waits,
//! counted and logged — and the two off-thread ladders the routing seam
//! climbs: a poison entry parse and an unwinding edge closure, each parked to
//! the dead-letter queue once exhausted. A third caller, the frame naming a
//! channel the operator does not declare, walks the same ladder with a
//! budget of one.

use std::any::Any;
use std::fmt::Display;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use flz_transport::transport::FlezhaStream;
use flz_transport::utility::noise::{noise, xorshift64};

use crate::channel::{ChannelDescriptor, RetryPolicy};
use crate::operator::instance::OperatorInstance;
use crate::park::RETRIES_TOTAL;
use crate::routing::{deliver_edge, EntryView};

/// What a park is attributed to, and how hard the ladder tries first.
///
/// `channel` is the row's attribution and may be a name off the wire, while
/// `tasklet_id` is the identity slot the controller turns into a DIRECTORY
/// and is always a value this side chose. They are one string for every
/// declared channel and differ only where there is no channel, which is why
/// the pairing is decided here rather than at each of the four call sites.
#[derive(Clone, Copy)]
pub(crate) struct ParkTarget<'a> {
    channel: &'a str,
    tasklet_id: &'a str,
    cell_suffix: &'a str,
    retry: RetryPolicy,
}

impl<'a> ParkTarget<'a> {
    /// A declared entry channel: its own name on both slots, its own budget.
    pub(crate) fn entry(channel: &'a ChannelDescriptor) -> Self {
        Self::declared(channel, "_entry")
    }

    /// A declared channel's consumer edge.
    pub(crate) fn edge(channel: &'a ChannelDescriptor) -> Self {
        Self::declared(channel, "_edge")
    }

    /// A steps operator's failed turn on its entry channel, under the one
    /// `_turn` slot whichever tasklet failed.
    pub(crate) fn turn(channel: &'a ChannelDescriptor) -> Self {
        Self {
            channel: &channel.name,
            tasklet_id: "_turn",
            cell_suffix: "_turn",
            retry: channel.retry,
        }
    }

    /// A name off the wire the operator does not declare: parked at a budget
    /// of one, because the same declared set answers every re-attempt, and
    /// under this side's own slot, because a frame-controlled name never
    /// becomes a path component.
    pub(crate) fn unresolved(requested: &'a str) -> Self {
        Self {
            channel: requested,
            tasklet_id: "_unresolved",
            cell_suffix: "_entry",
            retry: RetryPolicy {
                max_attempts: 1,
                backoff_base_ms: 0,
            },
        }
    }

    fn declared(channel: &'a ChannelDescriptor, cell_suffix: &'a str) -> Self {
        Self {
            channel: &channel.name,
            tasklet_id: &channel.name,
            cell_suffix,
            retry: channel.retry,
        }
    }
}

/// Count and log a retryable failure; how long to wait before the next
/// attempt.
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

/// `base * 2^(attempt-1)` ±50 %, capped at
/// [`crate::channel::RETRY_BACKOFF_MAX_MS`]; the jitter is runtime
/// scheduling, outside the determinism contract.
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

/// Where a re-parsed entry value goes: a flow operator's consumers, a steps operator's
/// inbox. The one place the two kinds differ on the entry ladder.
pub type EntryDeliver = Box<dyn Fn(Arc<dyn Any + Send + Sync>) -> Result<(), String> + Send + Sync>;

/// A poison entry payload on its channel's ladder, off the reader thread;
/// parked under the entry channel's identity when exhausted.
pub struct EntryParseLadder<V: EntryView> {
    pub instance: Arc<V>,
    pub channel_id: u32,
    pub bytes: Vec<u8>,
    pub first_error: String,
    pub consumed_offset: Option<u64>,
    pub call: Option<Arc<str>>,
    pub deliver: EntryDeliver,
}

pub fn spawn_entry_parse_ladder<V: EntryView>(ladder: EntryParseLadder<V>) {
    let instance = Arc::clone(&ladder.instance);
    spawn_park_thread(&instance, "flezha-entry-parse-park", move || {
        run_entry_parse_ladder(ladder)
    });
}

fn run_entry_parse_ladder<V: EntryView>(ladder: EntryParseLadder<V>) {
    let channel = ladder
        .instance
        .topology()
        .channel(ladder.channel_id)
        .expect("entry channel present in topology");
    let wrap = channel
        .wrap_input
        .as_ref()
        .expect("parsed entry channel carries a wrap_input");
    run_entrance_ladder(
        &*ladder.instance,
        &format!("entry channel `{}`", channel.name),
        ParkTarget::entry(channel),
        ladder.consumed_offset,
        ladder.call.as_ref(),
        Some(ladder.bytes.clone()),
        ladder.first_error.clone(),
        || {
            // A parser violating the determinism contract may succeed on a
            // re-attempt; deliver rather than drop.
            let typed = wrap(ladder.bytes.clone())?;
            (ladder.deliver)(typed)
        },
    );
}

/// The ladder for a payload that has not reached `instance` yet: climbed as
/// [`run_retry_park_ladder`] climbs, and `instance` told when it parked.
#[allow(clippy::too_many_arguments)]
pub(crate) fn run_entrance_ladder<V: EntryView>(
    instance: &V,
    label: &str,
    target: ParkTarget<'_>,
    consumed_offset: Option<u64>,
    call: Option<&Arc<str>>,
    payload: Option<Vec<u8>>,
    first_error: String,
    attempt_step: impl FnMut() -> Result<(), String>,
) {
    let parked = run_retry_park_ladder(
        label,
        target,
        instance.instance_id(),
        consumed_offset,
        call,
        payload,
        first_error,
        attempt_step,
    );
    if parked {
        instance.entry_parked(consumed_offset);
    }
}

/// Re-attempt one step per the target's budget, then park what still fails;
/// `attempt_step` re-runs AND delivers, so the ladders differ only in the
/// step. A [`ParkTarget`] travels rather than a `ChannelDescriptor`, because a
/// frame naming a channel the operator does not declare has no descriptor to
/// park under and must park all the same. `true` when the ladder parked.
#[allow(clippy::too_many_arguments)]
pub(crate) fn run_retry_park_ladder(
    label: &str,
    target: ParkTarget<'_>,
    instance_id: &str,
    consumed_offset: Option<u64>,
    call: Option<&Arc<str>>,
    payload: Option<Vec<u8>>,
    first_error: String,
    attempt_step: impl FnMut() -> Result<(), String>,
) -> bool {
    let Err((error, attempts)) = climb_ladder(label, target.retry, first_error, attempt_step)
    else {
        return false;
    };
    park_exhausted(
        label,
        target,
        instance_id,
        consumed_offset,
        call,
        payload,
        error,
        attempts,
    );
    true
}

/// Re-attempt one step per `retry`'s budget; `Err` carries the last error
/// and the attempts spent once the budget is gone, for the caller's own
/// terminal: a park, or a steps stage's failure.
pub(crate) fn climb_ladder(
    label: &str,
    retry: RetryPolicy,
    first_error: String,
    mut attempt_step: impl FnMut() -> Result<(), String>,
) -> Result<(), (String, u32)> {
    let mut attempt: u32 = 1; // the routing-thread run was attempt 1
    let mut error = first_error;
    while attempt < retry.max_attempts {
        let delay = note_failed_attempt(label, attempt, retry, &error);
        crate::util::sleep_interruptible(delay, None);
        attempt += 1;
        match attempt_step() {
            Ok(()) => return Ok(()),
            Err(e) => error = e,
        }
    }
    Err((error, attempt))
}

/// Park what a ladder could not deliver after `attempts`, under the
/// target's identity; the tail every exhausted ladder ends in, whether it
/// climbed here or at a steps stage's barrier. `call` rides the park frame
/// so the controller can answer a caller waiting on the parked input.
#[allow(clippy::too_many_arguments)]
pub(crate) fn park_exhausted(
    label: &str,
    target: ParkTarget<'_>,
    instance_id: &str,
    consumed_offset: Option<u64>,
    call: Option<&Arc<str>>,
    payload: Option<Vec<u8>>,
    error: String,
    attempts: u32,
) {
    let ParkTarget {
        channel,
        tasklet_id,
        cell_suffix,
        ..
    } = target;
    crate::park::PARKED_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    eprintln!(
        "[flezha] {} parking payload to DLQ after {} attempt(s): {}",
        label, attempts, error
    );
    let identity = crate::flusher::encode_cell_identity(instance_id, tasklet_id, cell_suffix);
    let body = crate::park::ParkBody {
        park_id: crate::park::mint_park_id(&format!(
            "{}/{}/{}",
            instance_id, tasklet_id, cell_suffix
        )),
        origin_offset: consumed_offset,
        entry: crate::flusher::DlqEntryBody {
            channel: channel.to_string(),
            error,
            attempts,
            payload,
        },
        reply_to: call.map(|token| token.to_string()),
    };
    crate::park::park_blocking(&identity, &body);
}

/// The ladder for an edge whose transform or key unwound, so the failure is
/// parked and observable rather than a dead thread.
#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_edge_ladder(
    instance: &Arc<OperatorInstance>,
    channel: &crate::channel::ChannelDescriptor,
    edge: &crate::channel::ConsumerEdge,
    value: &Arc<dyn Any + Send + Sync>,
    stream: &Arc<FlezhaStream>,
    consumed_offset: Option<u64>,
    call: Option<&Arc<str>>,
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
        call: call.cloned(),
        replay,
    };
    spawn_park_thread(instance, "flezha-edge-transform-park", move || {
        run_edge_ladder(ladder)
    });
}

/// A ladder off the routing thread; a failure to spawn is reported. The ladder
/// counts as running for `instance` until its closure is dropped, whether it
/// ran or its thread never started.
pub(crate) fn spawn_park_thread<V: EntryView>(
    instance: &Arc<V>,
    name: &str,
    run: impl FnOnce() + Send + 'static,
) {
    let running = LadderRunning::hold(instance);
    let spawned = std::thread::Builder::new()
        .name(name.to_string())
        .spawn(move || {
            let _running = running;
            run()
        });
    if let Err(e) = spawned {
        eprintln!("[flezha] failed to spawn {name} thread: {e}");
    }
}

/// One count in an instance's running ladders, given back on drop.
struct LadderRunning<V: EntryView>(Arc<V>);

impl<V: EntryView> LadderRunning<V> {
    fn hold(instance: &Arc<V>) -> LadderRunning<V> {
        instance.ladders().fetch_add(1, Ordering::AcqRel);
        LadderRunning(Arc::clone(instance))
    }
}

impl<V: EntryView> Drop for LadderRunning<V> {
    fn drop(&mut self) {
        self.0.ladders().fetch_sub(1, Ordering::AcqRel);
    }
}

struct EdgeLadder {
    instance: Arc<OperatorInstance>,
    channel_id: u32,
    edge: crate::channel::ConsumerEdge,
    value: Arc<dyn Any + Send + Sync>,
    first_error: String,
    stream: Arc<FlezhaStream>,
    consumed_offset: Option<u64>,
    call: Option<Arc<str>>,
    replay: bool,
}

fn run_edge_ladder(ladder: EdgeLadder) {
    let channel = ladder
        .instance
        .topology
        .channel(ladder.channel_id)
        .expect("channel present in topology");
    let tasklet = ladder
        .instance
        .topology
        .tasklet(ladder.edge.tasklet)
        .map(|p| p.name.to_string())
        .unwrap_or_default();
    run_retry_park_ladder(
        &format!("channel `{}` edge into `{}`", channel.name, tasklet),
        ParkTarget::edge(channel),
        &ladder.instance.instance_id,
        ladder.consumed_offset,
        ladder.call.as_ref(),
        None,
        ladder.first_error,
        || {
            // The ladder re-attempt runs off the routing thread with no
            // producing Cell in hand, so it takes the entry form: the
            // producer that emitted this value is long since released.
            deliver_edge(
                &ladder.instance,
                channel,
                &ladder.edge,
                &ladder.value,
                &ladder.stream,
                ladder.consumed_offset,
                ladder.call.as_ref(),
                ladder.replay,
                &mut crate::routing::Destination::Mailbox(None),
            )
            .map(|_| ())
            .map_err(|e| e.to_string())
        },
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state_sink;
    use crate::test_support::capture_parks;

    /// The ladder every park path submits to, on the arm the unresolved-channel
    /// caller never walks: a budget above one re-attempts and then parks under
    /// the name and the policy it was handed, which is all it now reads.
    #[test]
    fn a_budget_above_one_retries_then_parks_under_the_name_it_was_given() {
        let _guard = state_sink::test_guard();
        state_sink::reset_for_test();
        let parks = capture_parks();
        let mut attempts = 0;

        run_retry_park_ladder(
            "entry channel `late`",
            // Spelled out rather than built from a descriptor: this asserts
            // what the ladder reads, which is the target and nothing else.
            ParkTarget {
                channel: "late",
                tasklet_id: "late",
                cell_suffix: "_entry",
                retry: RetryPolicy {
                    max_attempts: 3,
                    backoff_base_ms: 1,
                },
            },
            "op-1",
            Some(9),
            Some(&Arc::from("w:tok")),
            Some(b"poison".to_vec()),
            "the routing thread's failure".to_string(),
            || {
                attempts += 1;
                Err(format!("failure {attempts}"))
            },
        );

        assert_eq!(
            attempts, 2,
            "attempt 1 was the caller's, so the ladder runs the other two"
        );
        let body = parks
            .lock()
            .expect("park capture poisoned")
            .pop()
            .expect("an exhausted ladder parks")
            .body;
        assert_eq!(body.entry.channel, "late");
        assert_eq!(body.entry.attempts, 3);
        assert_eq!(
            body.entry.error, "failure 2",
            "the last failure is the reason"
        );
        assert_eq!(body.origin_offset, Some(9));
        assert_eq!(
            body.reply_to.as_deref(),
            Some("w:tok"),
            "the park names its call"
        );
        assert_eq!(body.entry.payload.as_deref(), Some(&b"poison"[..]));
        state_sink::reset_for_test();
    }
}
