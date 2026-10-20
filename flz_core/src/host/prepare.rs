//! The prepare crossing: a payload whose consumers need keys or an event time
//! the core cannot derive from bytes is staged and crossed to the host as a
//! [`schema::ActivationKind::KeyRequest`] before it is enqueued — an entry
//! payload, or a steps relay's released value on its way into a keyed tasklet.
//! The invariant: an end-of-slice broadcast never overtakes a slice member
//! still waiting on its answer.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;

use super::cell::HostPayload;
use super::decl::HostClock;
use crate::channel::ChannelDescriptor;
use crate::poll_core::EnginePayload;
use crate::relay::instance::RelayInstance;
use crate::relay::topology::{CellMode, TaskletDescriptor};
use crate::routing::{Entering, Routed};
use crate::schema::{self, InputForm};
use crate::steps::instance::Value;
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
pub fn prepare_wants(tasklets: &[TaskletDescriptor], channel: &ChannelDescriptor) -> (bool, bool) {
    let mut want_keys = false;
    let mut want_event_time = false;
    for edge in &channel.consumers {
        let Some(desc) = tasklets.get(edge.tasklet as usize) else {
            continue;
        };
        let Some(facts) = desc.host_facts.as_ref() else {
            continue;
        };
        want_keys |= keyed_by_host(desc);
        want_event_time |= facts.clock == HostClock::EventTime;
    }
    (want_keys, want_event_time)
}

/// Whether a host tasklet's Cells are named by its host at the crossing.
fn keyed_by_host(desc: &TaskletDescriptor) -> bool {
    desc.mode == CellMode::Dynamic || (desc.mode == CellMode::Static && desc.cells_for_overridden)
}

/// Stage payloads for one crossing on `channel`, or hand them all back when
/// none of its consumers needs one.
pub fn stage_prepare(
    instance: &Arc<RelayInstance>,
    channel_id: crate::channel::ChannelId,
    channel: &ChannelDescriptor,
    entries: Vec<StagedEntry>,
) -> Vec<StagedEntry> {
    if channel.prepare_wants == (false, false) {
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
    submit_next_crossing(instance, channel_id, channel.prepare_wants);
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
    (want_keys, want_event_time): (bool, bool),
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
        entries,
        reply,
        ..
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
    for (mut entry, answer) in entries.into_iter().zip(answers) {
        if entry.replay {
            replay_resolved += 1;
        }
        match answer {
            Ok(ans) => {
                let bytes = std::mem::take(&mut entry.bytes);
                for (slot, item, policy) in
                    deliver_answered(&instance, channel, channel_desc, &entry, bytes, ans)
                {
                    crate::admission::admit_from_reader(&slot, item, policy);
                }
            }
            Err(e) => hand_prepare_ladder(Arc::clone(&instance), channel, entry, e),
        }
    }
    finish_crossing(
        &instance,
        channel,
        channel_desc.prepare_wants,
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

/// An answered payload onto its consumers: a steps stage's value into the
/// stage that owes it, anything else through every edge, its items held for
/// the caller to admit.
fn deliver_answered(
    instance: &Arc<RelayInstance>,
    channel_id: u32,
    channel: &ChannelDescriptor,
    entry: &StagedEntry,
    bytes: Vec<u8>,
    answer: schema::PrepareAnswer,
) -> Vec<crate::admission::Held> {
    let crossed = answered(instance, channel_id, channel, bytes, answer);
    if let Some(node) = entry.stage {
        let stream = Arc::clone(&entry.stream);
        crate::steps::crossed(instance, node, Ok((channel_id, crossed, stream)));
        return Vec::new();
    }
    crate::retry::route_held(instance, channel, &crossed, &entry.delivery())
}

/// A crossed payload as each consumer edge of its channel receives it, by edge
/// index: a keyed consumer one payload per Cell its host named, carrying that
/// Cell's values in answer order, and any other consumer the payload
/// untransformed.
pub(crate) type Crossed = Vec<(usize, Value)>;

impl Entering for Crossed {
    fn each<'a>(&'a self, channel: &'a ChannelDescriptor) -> impl Iterator<Item = Routed<'a>> {
        (self.iter()).filter_map(|(index, value)| Some((channel.consumers.get(*index)?, value)))
    }
}

/// What a crossing answered for one payload, as each consumer receives it. A
/// plain consumer's item ships the payload; prepared items ship none and share
/// one copy for a park to record, a copy only when a plain consumer owns it.
fn answered(
    instance: &RelayInstance,
    channel_id: u32,
    channel: &ChannelDescriptor,
    mut bytes: Vec<u8>,
    mut answer: schema::PrepareAnswer,
) -> Crossed {
    let event_time = answer.event_time;
    let host = |bytes, keys, form, entry| -> Value {
        Arc::new(HostPayload {
            bytes,
            channel: channel_id,
            keys,
            event_time,
            metadata: Vec::new(),
            form,
            entry,
        })
    };
    let keyed = |tasklet| {
        instance
            .topology
            .tasklet(tasklet)
            .is_some_and(keyed_by_host)
    };
    let any_plain = channel.consumers.iter().any(|edge| !keyed(edge.tasklet));
    let any_keyed = channel.consumers.iter().any(|edge| keyed(edge.tasklet));
    let entry = any_keyed.then(|| match any_plain {
        true => Arc::new(bytes.clone()),
        false => Arc::new(std::mem::take(&mut bytes)),
    });
    let plain = any_plain.then(|| host(bytes, Vec::new(), InputForm::Untransformed, None));
    let mut crossed = Vec::with_capacity(channel.consumers.len());
    for (index, edge) in channel.consumers.iter().enumerate() {
        if let (Some(value), false) = (&plain, keyed(edge.tasklet)) {
            crossed.push((index, Arc::clone(value)));
            continue;
        }
        let mine = answer
            .values
            .extract_if(.., |value| value.consumer == edge.tasklet);
        for (key, values) in per_cell(mine) {
            let form = InputForm::Prepared(values);
            crossed.push((index, host(Vec::new(), vec![key], form, entry.clone())));
        }
    }
    crossed
}

/// One consumer's values grouped per Cell key in key order, each Cell's in
/// answer order; a value naming no key reaches no Cell.
fn per_cell(values: impl Iterator<Item = schema::PreparedValue>) -> BTreeMap<String, Vec<Vec<u8>>> {
    let mut cells: BTreeMap<String, Vec<Vec<u8>>> = BTreeMap::new();
    for mut value in values {
        let last = value.keys.len().saturating_sub(1);
        for (i, key) in value.keys.drain(..).enumerate() {
            let bytes = match i == last {
                true => std::mem::take(&mut value.bytes),
                false => value.bytes.clone(),
            };
            cells.entry(key).or_default().push(bytes);
        }
    }
    cells
}

/// A failed prepare climbs its channel's ladder on the park owner, one
/// re-crossing per attempt. Where the ladder would park an entry payload, a
/// steps relay stage's value fails its stage instead: nothing is parked.
fn hand_prepare_ladder(
    instance: Arc<RelayInstance>,
    channel_id: u32,
    entry: StagedEntry,
    first_error: schema::WorkError,
) {
    let channel =
        (instance.topology.channel(channel_id)).expect("prepare channel present in topology");
    let label = format!("prepare for channel `{}`", channel.name);
    let (held, staged, mut asked) = (Arc::clone(&instance), entry.clone(), None);
    let wants = channel.prepare_wants;
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
    let channel = (instance.topology.channel(channel_id)).expect("prepare channel present");
    match answer {
        Some(Ok(answer)) => Attempt::Delivered(deliver_answered(
            instance,
            channel_id,
            channel,
            staged,
            staged.bytes.clone(),
            answer,
        )),
        Some(Err(e)) => Attempt::Failed(e.message),
        None => Attempt::Failed("prepare re-attempt answered nothing".to_string()),
    }
}

/// Release the inflight gate, re-arm the next batch, fire a deferred
/// broadcast once the last slice member resolved.
fn finish_crossing(
    instance: &Arc<RelayInstance>,
    channel_id: u32,
    wants: (bool, bool),
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
    submit_next_crossing(instance, channel_id, wants);
    if fire_broadcast {
        crate::routing::end_replay_slice(instance);
    }
}
