//! The prepare crossing: a payload whose consumers need keys or an event time
//! the core cannot derive from bytes is staged and crossed to the host as a
//! [`schema::ActivationKind::KeyRequest`] before it is enqueued — an entry
//! payload, or a steps relay's released value on its way into a keyed tasklet.
//! The invariant: an end-of-slice broadcast never overtakes a slice member
//! still waiting on its answer.

use std::any::Any;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use super::cell::HostPayload;
use super::decl::HostClock;
use crate::channel::ChannelDescriptor;
use crate::poll_core::EnginePayload;
use crate::relay::instance::RelayInstance;
use crate::relay::topology::{CellMode, RelayTopology};
use crate::schema;
use crate::util::LockRecover;

/// Per-instance staging for the prepare crossing, plus the deferred
/// end-of-slice broadcast.
#[derive(Default)]
pub struct HostPrepareState {
    queues: HashMap<u32, HostPrepareQueue>,
    /// Slice members staged or in a crossing; the broadcast waits for zero.
    replay_staged: usize,
    pending_replay_end: bool,
}

impl HostPrepareState {
    /// Whether an entry is queued for a crossing or at the host in one. Neither
    /// is in a mailbox or among the poll core's outstanding activations. A
    /// crossing that failed is on a retry ladder, which the instance counts.
    pub fn in_flight(&self) -> bool {
        self.queues
            .values()
            .any(|queue| queue.inflight || !queue.queued.is_empty())
    }
}

#[derive(Default)]
struct HostPrepareQueue {
    queued: VecDeque<StagedEntry>,
    inflight: bool,
}

#[derive(Clone)]
pub struct StagedEntry {
    pub bytes: Vec<u8>,
    pub consumed_offset: Option<u64>,
    pub call: Option<Arc<str>>,
    pub replay: bool,
    pub stream: Arc<flz_transport::transport::FlezhaStream>,
    /// The steps stage that owes this value until its answer addresses
    /// it; `None` for an entry payload.
    pub stage: Option<usize>,
}

impl StagedEntry {
    fn delivery(&self) -> crate::routing::Delivery<'_> {
        crate::routing::Delivery {
            stream: &self.stream,
            consumed_offset: self.consumed_offset,
            call: self.call.as_ref(),
            replay: self.replay,
        }
    }
}

/// A batched prepare crossing, handed to the host as a
/// [`schema::ActivationKind::KeyRequest`].
pub struct PrepareCrossing {
    pub instance: Arc<RelayInstance>,
    pub channel: crate::channel::ChannelId,
    pub want_keys: bool,
    pub want_event_time: bool,
    pub entries: Vec<StagedEntry>,
    /// A ladder's single-payload re-crossing awaits here.
    pub reply:
        Option<crossbeam::channel::Sender<Vec<Result<schema::PrepareAnswer, schema::WorkError>>>>,
}

/// Cap per crossing.
const PREPARE_BATCH_MAX: usize = 256;

/// `(want_keys, want_event_time)`; `(false, false)` is no crossing.
pub fn prepare_wants(topology: &RelayTopology, channel: &ChannelDescriptor) -> (bool, bool) {
    let mut want_keys = false;
    let mut want_event_time = false;
    for edge in &channel.consumers {
        let Some(desc) = topology.tasklet(edge.tasklet) else {
            continue;
        };
        let Some(facts) = desc.host_facts.as_ref() else {
            continue;
        };
        match desc.mode {
            CellMode::Dynamic => want_keys = true,
            CellMode::Static if desc.cells_for_overridden => want_keys = true,
            _ => {}
        }
        if facts.clock == HostClock::EventTime {
            want_event_time = true;
        }
    }
    (want_keys, want_event_time)
}

/// Stage payloads for one crossing on `channel`, or hand them all back when
/// none of its consumers needs one.
pub fn stage_prepare(
    instance: &Arc<RelayInstance>,
    channel_id: crate::channel::ChannelId,
    channel: &ChannelDescriptor,
    entries: Vec<StagedEntry>,
) -> Vec<StagedEntry> {
    let (want_keys, want_event_time) = prepare_wants(&instance.topology, channel);
    if !want_keys && !want_event_time {
        return entries;
    }
    {
        let mut state = instance.host_prepare.lock_recover();
        state.replay_staged += entries.iter().filter(|entry| entry.replay).count();
        state
            .queues
            .entry(channel_id)
            .or_default()
            .queued
            .extend(entries);
    }
    submit_next_crossing(instance, channel_id, want_keys, want_event_time);
    Vec::new()
}

/// The end-of-slice broadcast, deferred behind slice members still in the
/// crossing.
pub fn deferred_end_replay_slice(instance: &Arc<RelayInstance>) {
    {
        let mut state = instance.host_prepare.lock_recover();
        if state.replay_staged > 0 {
            state.pending_replay_end = true;
            return;
        }
    }
    crate::routing::end_replay_slice(instance);
}

/// Submit the channel's next crossing unless one is in flight.
fn submit_next_crossing(
    instance: &Arc<RelayInstance>,
    channel_id: u32,
    want_keys: bool,
    want_event_time: bool,
) {
    let entries = {
        let mut state = instance.host_prepare.lock_recover();
        let Some(queue) = state.queues.get_mut(&channel_id) else {
            return;
        };
        if queue.inflight || queue.queued.is_empty() {
            return;
        }
        queue.inflight = true;
        let take = queue.queued.len().min(PREPARE_BATCH_MAX);
        queue.queued.drain(..take).collect::<Vec<_>>()
    };
    crate::poll_core::global().submit_direct(EnginePayload::Prepare(PrepareCrossing {
        instance: Arc::clone(instance),
        channel: channel_id,
        want_keys,
        want_event_time,
        entries,
        reply: None,
    }));
}

/// A completed crossing: deliver the answered, ladder the failed, re-arm
/// the next batch.
pub(super) fn apply_prepare_outcome(
    crossing: PrepareCrossing,
    outcome: schema::ActivationOutcome,
) -> Result<(), crate::error::Error> {
    let answers = answers_for(outcome, crossing.entries.len());
    let PrepareCrossing {
        instance,
        channel,
        want_keys,
        want_event_time,
        entries,
        reply,
    } = crossing;
    if let Some(reply) = reply {
        let _ = reply.send(answers);
        return Ok(());
    }
    let Some(channel_desc) = instance.topology.channel(channel) else {
        return Err(crate::error::Error::Answer(
            crate::Code::PrepareUnknownChannel.with([]),
        ));
    };
    let mut replay_resolved = 0usize;
    for (entry, answer) in entries.into_iter().zip(answers) {
        if entry.replay {
            replay_resolved += 1;
        }
        match answer {
            Ok(ans) => deliver_prepared(&instance, channel, channel_desc, entry, ans),
            Err(e) => hand_prepare_ladder(
                Arc::clone(&instance),
                channel,
                want_keys,
                want_event_time,
                entry,
                e,
            ),
        }
    }
    finish_crossing(
        &instance,
        channel,
        want_keys,
        want_event_time,
        replay_resolved,
    );
    Ok(())
}

/// One answer per entry; a short answer fails what it left unanswered.
fn answers_for(
    outcome: schema::ActivationOutcome,
    entries: usize,
) -> Vec<Result<schema::PrepareAnswer, schema::WorkError>> {
    let mut answers = match outcome {
        schema::ActivationOutcome::Prepared(answers) => answers,
        schema::ActivationOutcome::Failed(err) => {
            vec![Err(err); entries]
        }
        _ => {
            vec![
                Err(schema::WorkError {
                    message: "prepare crossing answered with a non-prepare outcome".to_string(),
                    non_retryable: false,
                });
                entries
            ]
        }
    };
    if answers.len() < entries {
        answers.resize(
            entries,
            Err(schema::WorkError {
                message: "prepare crossing answered fewer payloads than asked".to_string(),
                non_retryable: false,
            }),
        );
    }
    answers
}

fn deliver_prepared(
    instance: &Arc<RelayInstance>,
    channel_id: u32,
    channel: &ChannelDescriptor,
    mut entry: StagedEntry,
    answer: schema::PrepareAnswer,
) {
    let value: Arc<dyn Any + Send + Sync> = Arc::new(HostPayload {
        bytes: std::mem::take(&mut entry.bytes),
        channel: channel_id,
        keys: answer.keys,
        event_time: answer.event_time,
        metadata: Vec::new(),
    });
    if let Some(node) = entry.stage {
        return crate::steps::crossed(instance, node, Ok((value, entry.stream)));
    }
    crate::routing::route_to_consumers(instance, channel, &value, &entry.delivery(), None);
}

/// A failed prepare climbs its channel's ladder on the park owner, one
/// re-crossing per attempt. Where the ladder would park an entry payload, a
/// steps relay stage's value fails its stage instead: nothing is parked.
fn hand_prepare_ladder(
    instance: Arc<RelayInstance>,
    channel_id: u32,
    want_keys: bool,
    want_event_time: bool,
    entry: StagedEntry,
    first_error: schema::WorkError,
) {
    let channel =
        (instance.topology.channel(channel_id)).expect("prepare channel present in topology");
    let label = format!("prepare for channel `{}`", channel.name);
    let (held, staged, mut asked) = (Arc::clone(&instance), entry.clone(), None);
    let wants = (want_keys, want_event_time);
    let attempt = move || re_cross(&held, channel_id, wants, &staged, &mut asked);
    let end = entry.stage.map(|node| {
        let failed = Arc::clone(&instance);
        Box::new(move |error, _attempts| {
            crate::steps::crossed(&failed, node, Err(error));
            None
        }) as crate::retry::End
    });
    let origin = (
        entry.consumed_offset,
        entry.call.clone(),
        Some(entry.bytes.clone()),
    );
    crate::retry::hand_ladder(
        &instance,
        label,
        crate::retry::ParkTarget::entry(channel),
        origin,
        first_error.message,
        attempt,
        end,
    );
}

type Answers = Vec<Result<schema::PrepareAnswer, schema::WorkError>>;

/// How long a re-crossing waits for the host's answer before it counts as
/// a failed attempt.
const RE_CROSS_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// One look at a failed prepare's re-crossing, which never waits: the first
/// asks the host, a later one takes its answer, delivered for the ladder to
/// admit.
fn re_cross(
    instance: &Arc<RelayInstance>,
    channel_id: u32,
    (want_keys, want_event_time): (bool, bool),
    staged: &StagedEntry,
    asked: &mut Option<(crossbeam::channel::Receiver<Answers>, std::time::Instant)>,
) -> crate::retry::Attempt {
    use crate::retry::Attempt;
    let Some((rx, since)) = asked else {
        let (tx, rx) = crossbeam::channel::bounded(1);
        crate::poll_core::global().submit_direct(EnginePayload::Prepare(PrepareCrossing {
            instance: Arc::clone(instance),
            channel: channel_id,
            want_keys,
            want_event_time,
            entries: vec![staged.clone()],
            reply: Some(tx),
        }));
        *asked = Some((rx, std::time::Instant::now()));
        return Attempt::Pending;
    };
    let answer = match rx.try_recv() {
        Err(crossbeam::channel::TryRecvError::Empty) if since.elapsed() < RE_CROSS_TIMEOUT => {
            return Attempt::Pending;
        }
        Ok(mut answers) => answers.pop(),
        Err(_) => Some(Err(schema::WorkError {
            message: "prepare re-attempt not answered".to_string(),
            non_retryable: false,
        })),
    };
    *asked = None;
    let value: Arc<dyn Any + Send + Sync> = match answer {
        Some(Ok(answer)) => Arc::new(HostPayload {
            bytes: staged.bytes.clone(),
            channel: channel_id,
            keys: answer.keys,
            event_time: answer.event_time,
            metadata: Vec::new(),
        }),
        Some(Err(e)) => return Attempt::Failed(e.message),
        None => return Attempt::Failed("prepare re-attempt answered nothing".to_string()),
    };
    if let Some(node) = staged.stage {
        crate::steps::crossed(instance, node, Ok((value, Arc::clone(&staged.stream))));
        return Attempt::Delivered(Vec::new());
    }
    let channel = (instance.topology.channel(channel_id)).expect("prepare channel present");
    let d = staged.delivery();
    Attempt::Delivered(crate::retry::route_held(instance, channel, &value, &d))
}

/// Release the inflight gate, re-arm the next batch, fire a deferred
/// broadcast once the last slice member resolved.
fn finish_crossing(
    instance: &Arc<RelayInstance>,
    channel_id: u32,
    want_keys: bool,
    want_event_time: bool,
    replay_resolved: usize,
) {
    let fire_broadcast = {
        let mut state = instance.host_prepare.lock_recover();
        state.replay_staged = state.replay_staged.saturating_sub(replay_resolved);
        if let Some(queue) = state.queues.get_mut(&channel_id) {
            queue.inflight = false;
        }
        state.replay_staged == 0
            && state.pending_replay_end
            && std::mem::take(&mut state.pending_replay_end)
    };
    submit_next_crossing(instance, channel_id, want_keys, want_event_time);
    if fire_broadcast {
        crate::routing::end_replay_slice(instance);
    }
}
