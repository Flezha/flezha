//! The retry policy every ladder climbs — how long attempt `n` waits,
//! counted and logged — and the ladders the routing seam hands to the park
//! owner ([`crate::park`]): a poison entry parse and an unwinding edge
//! closure, each parked to the dead-letter queue once exhausted. A third
//! caller, the frame naming a channel the relay does not declare, walks the
//! same ladder with a budget of one.

use std::any::Any;
use std::fmt::Display;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use flz_transport::transport::FlezhaStream;
use flz_transport::utility::noise::{noise, xorshift64};

use crate::admission::Held;
use crate::cell::{CellItem, CellSlot};
use crate::channel::{ChannelDescriptor, RetryPolicy};
use crate::park::{Park, Then, Work, RETRIES_TOTAL};
use crate::relay::instance::RelayInstance;
use crate::routing::{deliver_edge, EntryView};

/// What a park is attributed to, and how hard the ladder tries first.
///
/// `channel` is the row's attribution and may be a name off the wire, while
/// `tasklet_id` is the identity slot the controller turns into a DIRECTORY
/// and is always a value this side chose. They are one string for every
/// declared channel and differ only where there is no channel, which is why
/// the pairing is decided here rather than at each of the call sites.
#[derive(Clone)]
pub(crate) struct ParkTarget {
    channel: Arc<str>,
    tasklet_id: Arc<str>,
    cell_suffix: &'static str,
    retry: RetryPolicy,
}

impl ParkTarget {
    /// A declared entry channel: its own name on both slots, its own budget.
    pub(crate) fn entry(channel: &ChannelDescriptor) -> Self {
        Self::declared(channel, "_entry")
    }

    /// A declared channel's consumer edge.
    pub(crate) fn edge(channel: &ChannelDescriptor) -> Self {
        Self::declared(channel, "_edge")
    }

    /// A steps relay's failed turn on its entry channel, under the one
    /// `_turn` slot whichever tasklet failed.
    pub(crate) fn turn(channel: &ChannelDescriptor) -> Self {
        Self {
            channel: Arc::clone(&channel.name),
            tasklet_id: Arc::from("_turn"),
            cell_suffix: "_turn",
            retry: channel.retry,
        }
    }

    /// A name off the wire the relay does not declare: parked at a budget
    /// of one, because the same declared set answers every re-attempt, and
    /// under this side's own slot, because a frame-controlled name never
    /// becomes a path component.
    pub(crate) fn unresolved(requested: &str) -> Self {
        Self {
            channel: Arc::from(requested),
            tasklet_id: Arc::from("_unresolved"),
            cell_suffix: "_entry",
            retry: RetryPolicy {
                max_attempts: 1,
                backoff_base_ms: 0,
            },
        }
    }

    fn declared(channel: &ChannelDescriptor, cell_suffix: &'static str) -> Self {
        Self {
            channel: Arc::clone(&channel.name),
            tasklet_id: Arc::clone(&channel.name),
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

/// What one ladder attempt came to.
pub(crate) enum Attempt {
    /// Through, with the items it addressed still to be admitted.
    Delivered(Vec<Held>),
    Failed(String),
    /// Not answered yet: looked at again after a backoff, no attempt spent.
    Pending,
}

impl From<Result<Vec<Held>, String>> for Attempt {
    fn from(result: Result<Vec<Held>, String>) -> Self {
        result.map_or_else(Attempt::Failed, Attempt::Delivered)
    }
}

type Step = Box<dyn FnMut() -> Attempt + Send>;
pub(crate) type End = Box<dyn FnOnce(String, u32) -> Option<Park> + Send>;

/// A payload's retry ladder, climbed by the park owner rather than a thread
/// of its own: attempt 1 was the caller's, each further attempt runs once
/// its backoff is due, and the spent budget hands the last error and the
/// attempts to the ladder's end. Nothing it does waits: what has no room
/// yet, or no answer yet, is looked at again after a backoff.
pub(crate) struct Ladder {
    label: String,
    retry: RetryPolicy,
    attempt: u32,
    error: String,
    retrying: bool,
    step: Step,
    end: Option<End>,
    /// Set once delivered: the addressed items not yet admitted, and whether
    /// the first of them was already counted blocked.
    admitting: Option<(std::collections::VecDeque<Held>, bool)>,
}

/// Where one turn of a ladder left it.
pub(crate) enum Climb {
    /// Attempt again at this instant.
    Again(Instant),
    /// Look again at this instant, waiting on a consumer's room or a host's
    /// answer rather than on time alone.
    Waiting(Instant),
    /// Delivered, or exhausted into its end: a park to hold, or nothing.
    Over(Option<Box<Park>>),
}

impl Ladder {
    /// The attempt that is due, if one is, then the next one scheduled or
    /// the end reached.
    pub(crate) fn advance(&mut self) -> Climb {
        let pause = Instant::now() + backoff(self.retry.backoff_base_ms, self.attempt);
        if let Some((admitting, blocked)) = &mut self.admitting {
            return match crate::admission::admit_in_order(admitting, blocked) {
                true => Climb::Over(None),
                false => Climb::Waiting(pause),
            };
        }
        if self.retrying {
            match (self.step)() {
                Attempt::Delivered(items) => {
                    self.admitting = Some((items.into(), false));
                    return self.advance();
                }
                Attempt::Pending => return Climb::Waiting(pause),
                Attempt::Failed(e) => (self.attempt, self.error) = (self.attempt + 1, e),
            }
        }
        self.retrying = self.attempt < self.retry.max_attempts;
        if self.retrying {
            let delay = note_failed_attempt(&self.label, self.attempt, self.retry, &self.error);
            return Climb::Again(Instant::now() + delay);
        }
        let end = self.end.take().expect("a ladder reaches its end once");
        Climb::Over(end(std::mem::take(&mut self.error), self.attempt).map(Box::new))
    }
}

/// Hand a payload whose first attempt failed to the park owner, re-attempted
/// per `target`'s budget. Once the budget is spent, `end` takes the last
/// error and the attempts when given; otherwise the payload parks under
/// `target`, and `instance` is told once the park is let go of (a flow
/// relay takes no notice).
#[allow(clippy::too_many_arguments)]
pub(crate) fn hand_ladder<V: EntryView>(
    instance: &Arc<V>,
    label: String,
    target: ParkTarget,
    consumed_offset: Option<u64>,
    call: Option<Arc<str>>,
    payload: Option<Vec<u8>>,
    first_error: String,
    step: impl FnMut() -> Attempt + Send + 'static,
    end: Option<End>,
) {
    let retry = target.retry;
    let end = end.unwrap_or_else(|| {
        let (told, park_label) = (Arc::clone(instance), label.clone());
        Box::new(move |error, attempts| {
            let instance_id = Arc::clone(told.instance_id());
            let then: Option<Then> = Some(Box::new(move || told.entry_parked(consumed_offset)));
            let origin = (consumed_offset, call, payload);
            let park = park_exhausted(
                &park_label,
                target,
                &instance_id,
                origin,
                error,
                attempts,
                then,
            );
            Some(park)
        })
    });
    let ladder = Ladder {
        label,
        retry,
        attempt: 1,
        error: first_error,
        retrying: false,
        step: Box::new(step),
        end: Some(end),
        admitting: None,
    };
    crate::park::hand_off(instance, Work::Climbing(ladder));
}

/// The park of what could not be delivered after `attempts`, under the
/// target's identity: the tail every exhausted ladder ends in, whether it
/// climbed on the park owner or at a steps stage's barrier. `origin` is the
/// offset the controller stamped, the outside call waiting on the input,
/// which the controller can then answer, and the input's bytes.
pub(crate) fn park_exhausted(
    label: &str,
    target: ParkTarget,
    instance_id: &str,
    (consumed_offset, call, payload): (Option<u64>, Option<Arc<str>>, Option<Vec<u8>>),
    error: String,
    attempts: u32,
    then: Option<Then>,
) -> Park {
    let ParkTarget {
        channel,
        tasklet_id,
        cell_suffix,
        ..
    } = target;
    crate::park::PARKED_TOTAL.fetch_add(1, Ordering::Relaxed);
    eprintln!("[flezha] {label} parking payload to DLQ after {attempts} attempt(s): {error}");
    Park::new(
        crate::flusher::encode_cell_identity(instance_id, &tasklet_id, cell_suffix),
        &format!("{instance_id}/{tasklet_id}/{cell_suffix}"),
        consumed_offset,
        call.as_ref(),
        crate::flusher::DlqEntryBody {
            channel: channel.to_string(),
            error,
            attempts,
            payload,
        },
        then,
    )
}

/// Where a re-parsed entry value goes: a flow relay's consumers, a steps relay's
/// inbox. The one place the two kinds differ on the entry ladder.
pub type EntryDeliver =
    Box<dyn Fn(Arc<dyn Any + Send + Sync>) -> Result<Vec<Held>, String> + Send + Sync>;

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

pub fn hand_entry_parse_ladder<V: EntryView>(ladder: EntryParseLadder<V>) {
    let EntryParseLadder {
        instance,
        channel_id,
        bytes,
        first_error,
        consumed_offset,
        call,
        deliver,
    } = ladder;
    let channel =
        (instance.topology().channel(channel_id)).expect("entry channel present in topology");
    let wrap = Arc::clone(
        (channel.wrap_input.as_ref()).expect("parsed entry channel carries a wrap_input"),
    );
    let label = format!("entry channel `{}`", channel.name);
    let retried = bytes.clone();
    // A parser violating the determinism contract may succeed on a
    // re-attempt; deliver rather than drop.
    let step = move || wrap(retried.clone()).and_then(&deliver).into();
    let (target, payload) = (ParkTarget::entry(channel), Some(bytes));
    hand_ladder(
        &instance,
        label,
        target,
        consumed_offset,
        call,
        payload,
        first_error,
        step,
        None,
    );
}

/// A value onto `channel`'s consumers from the park owner's ladder lane,
/// which never waits for room: a flow relay's addressed items come back in
/// the entry form, for the lane to admit as room appears, and an edge that
/// unwinds climbs its own ladder; a steps relay's value enters its run,
/// which never waits either.
#[allow(clippy::too_many_arguments)]
pub(crate) fn route_held(
    instance: &Arc<RelayInstance>,
    channel: &ChannelDescriptor,
    value: &Arc<dyn Any + Send + Sync>,
    stream: &Arc<FlezhaStream>,
    consumed_offset: Option<u64>,
    call: Option<&Arc<str>>,
    replay: bool,
) -> Vec<Held> {
    if instance.steps.is_some() {
        crate::routing::route_to_consumers(
            instance,
            channel,
            value,
            stream,
            consumed_offset,
            call,
            replay,
            None,
        );
        return Vec::new();
    }
    let mut held = Vec::new();
    for edge in &channel.consumers {
        let mut to = crate::routing::Destination::Held(&mut held);
        let delivered = deliver_edge(
            instance,
            channel,
            edge,
            value,
            stream,
            consumed_offset,
            call,
            replay,
            &mut to,
        );
        if let Err(e) = delivered {
            let error = e.to_string();
            hand_edge_ladder(
                instance,
                channel,
                edge,
                value,
                stream,
                consumed_offset,
                call,
                replay,
                error,
            );
        }
    }
    entry_form(held, channel)
}

/// Items addressed through a held destination, as the entry path admits
/// them: forwarded, under their channel's policy.
fn entry_form(held: Vec<(Arc<CellSlot>, CellItem)>, channel: &ChannelDescriptor) -> Vec<Held> {
    let policy = channel.overflow_policy;
    (held.into_iter())
        .map(|(slot, mut item)| {
            if let CellItem::Input(payload) = &mut item {
                payload.forwarded = true;
            }
            (slot, item, policy)
        })
        .collect()
}

/// The ladder for an edge whose transform or key unwound, so the failure is
/// parked and observable rather than a dead thread.
#[allow(clippy::too_many_arguments)]
pub(crate) fn hand_edge_ladder(
    instance: &Arc<RelayInstance>,
    channel: &ChannelDescriptor,
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
    let tasklet = (instance.topology.tasklet(edge.tasklet))
        .map(|p| p.name.to_string())
        .unwrap_or_default();
    let label = format!("channel `{}` edge into `{}`", channel.name, tasklet);
    let (held, edge, value) = (Arc::clone(instance), edge.clone(), Arc::clone(value));
    let (stream, retried_call) = (Arc::clone(stream), call.cloned());
    let step = move || {
        let channel = (held.topology.channel(channel_id)).expect("channel present in topology");
        let mut items = Vec::new();
        let mut to = crate::routing::Destination::Held(&mut items);
        let call = retried_call.as_ref();
        match deliver_edge(
            &held,
            channel,
            &edge,
            &value,
            &stream,
            consumed_offset,
            call,
            replay,
            &mut to,
        ) {
            Ok(_) => Attempt::Delivered(entry_form(items, channel)),
            Err(e) => Attempt::Failed(e.to_string()),
        }
    };
    let target = ParkTarget::edge(channel);
    hand_ladder(
        instance,
        label,
        target,
        consumed_offset,
        call.cloned(),
        None,
        error,
        step,
        None,
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state_sink;
    use crate::test_support::capture_parks;

    /// A target spelled out rather than built from a descriptor: the test
    /// asserts what the ladder reads, which is the target and nothing else.
    fn late() -> ParkTarget {
        ParkTarget {
            channel: Arc::from("late"),
            tasklet_id: Arc::from("late"),
            cell_suffix: "_entry",
            retry: RetryPolicy {
                max_attempts: 3,
                backoff_base_ms: 1,
            },
        }
    }

    /// The ladder every park path hands off, on the arm the unresolved-channel
    /// caller never walks: a budget above one re-attempts and then parks under
    /// the name and the policy it was handed.
    #[test]
    fn a_budget_above_one_retries_then_parks_under_the_name_it_was_given() {
        let _guard = state_sink::test_guard();
        state_sink::reset_for_test();
        let parks = capture_parks();
        let tries = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let counted = Arc::clone(&tries);
        let instance = crate::test_support::host_class::instance("op-1", &["in"], &[], &[]);

        let label = "entry channel `late`".to_string();
        let first = "the routing thread's failure".to_string();
        let step = move || {
            Attempt::Failed(format!(
                "failure {}",
                counted.fetch_add(1, Ordering::SeqCst) + 1
            ))
        };
        let (call, payload) = (Some(Arc::from("w:tok")), Some(b"poison".to_vec()));
        hand_ladder(
            &instance,
            label,
            late(),
            Some(9),
            call,
            payload,
            first,
            step,
            None,
        );
        crate::test_support::spin_until("the ladder to finish", || !instance.ladders_running());

        assert_eq!(
            tries.load(Ordering::SeqCst),
            2,
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
